import type {
  AgentInboxMessage,
  AgentInboxStore,
  AgentInboxSubscription,
  AgentSchedule,
} from "./types";
export type AgentInboxSqlResult<Row> = { rows: Row[] };
export type AgentInboxSqlTransaction = {
  query<Row = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ): Promise<AgentInboxSqlResult<Row>>;
};
export type AgentInboxSqlClient = AgentInboxSqlTransaction & {
  transaction<Value>(
    work: (tx: AgentInboxSqlTransaction) => Promise<Value>,
  ): Promise<Value>;
};
const safe = (value: string) => {
  if (!/^[a-z_][a-z0-9_]*$/i.test(value))
    throw new Error("Invalid SQL namespace");
  return value;
};
export const agentInboxPostgresSchemaSql = (schema = "agent_inbox") => {
  const ns = safe(schema);
  return `CREATE SCHEMA IF NOT EXISTS ${ns}; CREATE TABLE IF NOT EXISTS ${ns}.subscriptions (id text PRIMARY KEY, tenant_id text NOT NULL, source text NOT NULL, kinds text[] NOT NULL, enabled boolean NOT NULL, document jsonb NOT NULL); CREATE INDEX IF NOT EXISTS agent_inbox_subscription_idx ON ${ns}.subscriptions (tenant_id,source) WHERE enabled; CREATE TABLE IF NOT EXISTS ${ns}.messages (id text PRIMARY KEY, subscription_id text NOT NULL, source text NOT NULL, source_event_id text NOT NULL, status text NOT NULL, not_before timestamptz NOT NULL, expires_at timestamptz, lease_owner text, lease_expires_at timestamptz, document jsonb NOT NULL, UNIQUE(subscription_id,source,source_event_id)); CREATE INDEX IF NOT EXISTS agent_inbox_due_idx ON ${ns}.messages (status,not_before,lease_expires_at); CREATE TABLE IF NOT EXISTS ${ns}.schedules (id text PRIMARY KEY, enabled boolean NOT NULL, next_at timestamptz NOT NULL, document jsonb NOT NULL); CREATE INDEX IF NOT EXISTS agent_inbox_schedules_due_idx ON ${ns}.schedules (next_at) WHERE enabled;`;
};
const row = <Value>(
  value: { document: Value | string } | undefined,
): Value | undefined =>
  value
    ? typeof value.document === "string"
      ? JSON.parse(value.document)
      : value.document
    : undefined;
export const createPostgresAgentInboxStore = ({
  client,
  schema = "agent_inbox",
}: {
  client: AgentInboxSqlClient;
  schema?: string;
}): AgentInboxStore => {
  const ns = safe(schema);
  return {
    saveSubscription: async (v) => {
      await client.query(
        `INSERT INTO ${ns}.subscriptions (id,tenant_id,source,kinds,enabled,document) VALUES ($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT (id) DO UPDATE SET tenant_id=$2,source=$3,kinds=$4,enabled=$5,document=$6::jsonb`,
        [
          v.id,
          v.target.tenantId,
          v.source,
          v.kinds,
          v.enabled,
          JSON.stringify(v),
        ],
      );
    },
    listSubscriptions: async (v) =>
      (
        await client.query<{ document: AgentInboxSubscription | string }>(
          `SELECT document FROM ${ns}.subscriptions WHERE tenant_id=$1 AND source=$2 AND enabled AND $3=ANY(kinds)`,
          [v.tenantId, v.source, v.kind],
        )
      ).rows.map((v) => row<AgentInboxSubscription>(v)!),
    listSubscriptionInventory: async (v) => {
      const limit = Math.max(1, Math.min(v.limit, 200));
      const result = v.tenantId
        ? await client.query<{ document: AgentInboxSubscription | string }>(
            `SELECT document FROM ${ns}.subscriptions WHERE tenant_id=$1 ORDER BY document->>'createdAt' DESC LIMIT $2`,
            [v.tenantId, limit],
          )
        : await client.query<{ document: AgentInboxSubscription | string }>(
            `SELECT document FROM ${ns}.subscriptions ORDER BY document->>'createdAt' DESC LIMIT $1`,
            [limit],
          );
      return result.rows.map((value) => row<AgentInboxSubscription>(value)!);
    },
    setSubscriptionEnabled: async (v) =>
      (
        await client.query(
          `UPDATE ${ns}.subscriptions SET enabled=$3,document=jsonb_set(document,'{enabled}',to_jsonb($3::boolean)) WHERE id=$1 AND tenant_id=$2 RETURNING id`,
          [v.id, v.tenantId, v.enabled],
        )
      ).rows.length === 1,
    enqueue: async (v) => {
      const result = await client.query<{
        document: AgentInboxMessage | string;
      }>(
        `INSERT INTO ${ns}.messages (id,subscription_id,source,source_event_id,status,not_before,expires_at,document) VALUES ($1,$2,$3,$4,$5,$6::timestamptz,$7::timestamptz,$8::jsonb) ON CONFLICT (subscription_id,source,source_event_id) DO UPDATE SET source_event_id=EXCLUDED.source_event_id RETURNING document`,
        [
          v.id,
          v.subscriptionId,
          v.source,
          v.sourceEventId,
          v.status,
          v.notBefore,
          v.expiresAt ?? null,
          JSON.stringify(v),
        ],
      );
      return row<AgentInboxMessage>(result.rows[0])!;
    },
    listMessages: async (v) => {
      const params: unknown[] = [];
      const filters: string[] = [];
      if (v.tenantId) {
        params.push(v.tenantId);
        filters.push(`document->'target'->>'tenantId'=$${params.length}`);
      }
      if (v.status) {
        params.push(v.status);
        filters.push(`status=$${params.length}`);
      }
      params.push(Math.max(1, Math.min(v.limit, 200)));
      const where = filters.length ? ` WHERE ${filters.join(" AND ")}` : "";
      return (
        await client.query<{ document: AgentInboxMessage | string }>(
          `SELECT document FROM ${ns}.messages${where} ORDER BY document->>'updatedAt' DESC LIMIT $${params.length}`,
          params,
        )
      ).rows.map((value) => row<AgentInboxMessage>(value)!);
    },
    cancelMessage: async (v) =>
      (
        await client.query(
          `UPDATE ${ns}.messages SET status='cancelled',document=document || $3::jsonb WHERE id=$1 AND document->'target'->>'tenantId'=$2 AND status='pending' RETURNING id`,
          [
            v.id,
            v.tenantId,
            JSON.stringify({ status: "cancelled", updatedAt: v.now }),
          ],
        )
      ).rows.length === 1,
    claim: (v) =>
      client.transaction(async (tx) => {
        const found = row<AgentInboxMessage>(
          (
            await tx.query<{ document: AgentInboxMessage | string }>(
              `SELECT document FROM ${ns}.messages WHERE ((status='pending' AND not_before <= $1::timestamptz) OR (status='leased' AND lease_expires_at <= $1::timestamptz)) AND (expires_at IS NULL OR expires_at > $1::timestamptz) ORDER BY not_before FOR UPDATE SKIP LOCKED LIMIT 1`,
              [v.now],
            )
          ).rows[0],
        );
        if (!found) return undefined;
        const next = {
          ...found,
          status: "leased" as const,
          leaseOwner: v.workerId,
          leaseExpiresAt: v.leaseExpiresAt,
          attempts: found.attempts + 1,
          updatedAt: v.now,
        };
        return row<AgentInboxMessage>(
          (
            await tx.query<{ document: AgentInboxMessage | string }>(
              `UPDATE ${ns}.messages SET status='leased',lease_owner=$2,lease_expires_at=$3::timestamptz,document=$4::jsonb WHERE id=$1 RETURNING document`,
              [found.id, v.workerId, v.leaseExpiresAt, JSON.stringify(next)],
            )
          ).rows[0],
        );
      }),
    complete: async (v) =>
      (
        await client.query(
          `UPDATE ${ns}.messages SET status='completed',lease_owner=NULL,lease_expires_at=NULL,document=jsonb_set(jsonb_set(document,'{status}','"completed"'),'{updatedAt}',to_jsonb($3::text)) WHERE id=$1 AND lease_owner=$2 AND status='leased' RETURNING id`,
          [v.id, v.workerId, v.now],
        )
      ).rows.length === 1,
    retry: async (v) => {
      const status = v.deadLetter ? "dead_letter" : "pending";
      return (
        (
          await client.query(
            `UPDATE ${ns}.messages SET status=$3,not_before=$4::timestamptz,lease_owner=NULL,lease_expires_at=NULL,document=document || $5::jsonb WHERE id=$1 AND lease_owner=$2 AND status='leased' RETURNING id`,
            [
              v.id,
              v.workerId,
              status,
              v.notBefore,
              JSON.stringify({
                status,
                notBefore: v.notBefore,
                lastError: v.error,
                updatedAt: v.now,
              }),
            ],
          )
        ).rows.length === 1
      );
    },
    saveSchedule: async (v) => {
      await client.query(
        `INSERT INTO ${ns}.schedules (id,enabled,next_at,document) VALUES ($1,$2,$3::timestamptz,$4::jsonb) ON CONFLICT(id) DO UPDATE SET enabled=$2,next_at=$3::timestamptz,document=$4::jsonb`,
        [v.id, v.enabled, v.nextAt, JSON.stringify(v)],
      );
    },
    listSchedules: async (v) => {
      const limit = Math.max(1, Math.min(v.limit, 200));
      const result = v.tenantId
        ? await client.query<{ document: AgentSchedule | string }>(
            `SELECT document FROM ${ns}.schedules WHERE document->'target'->>'tenantId'=$1 ORDER BY document->>'createdAt' DESC LIMIT $2`,
            [v.tenantId, limit],
          )
        : await client.query<{ document: AgentSchedule | string }>(
            `SELECT document FROM ${ns}.schedules ORDER BY document->>'createdAt' DESC LIMIT $1`,
            [limit],
          );
      return result.rows.map((value) => row<AgentSchedule>(value)!);
    },
    setScheduleEnabled: async (v) =>
      (
        await client.query(
          `UPDATE ${ns}.schedules SET enabled=$3,document=jsonb_set(document,'{enabled}',to_jsonb($3::boolean)) WHERE id=$1 AND document->'target'->>'tenantId'=$2 RETURNING id`,
          [v.id, v.tenantId, v.enabled],
        )
      ).rows.length === 1,
    claimDueSchedule: async (v) =>
      row<AgentSchedule>(
        (
          await client.query<{ document: AgentSchedule | string }>(
            `SELECT document FROM ${ns}.schedules WHERE enabled AND next_at <= $1::timestamptz ORDER BY next_at LIMIT 1`,
            [v.now],
          )
        ).rows[0],
      ),
    advanceSchedule: async (v) =>
      (
        await client.query(
          `UPDATE ${ns}.schedules SET next_at=$3::timestamptz,document=jsonb_set(document,'{nextAt}',to_jsonb($3::text)) WHERE id=$1 AND next_at=$2::timestamptz RETURNING id`,
          [v.id, v.expectedNextAt, v.nextAt],
        )
      ).rows.length === 1,
  };
};
