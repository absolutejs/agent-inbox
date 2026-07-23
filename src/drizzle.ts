import {
  and,
  arrayContains,
  asc,
  desc,
  eq,
  gt,
  isNull,
  lte,
  or,
  sql,
  type SQL,
  type SQLWrapper,
} from "drizzle-orm";
import {
  boolean,
  customType,
  index,
  pgSchema,
  text,
  timestamp,
  uniqueIndex,
  type PgAsyncDatabase,
} from "drizzle-orm/pg-core";
import type {
  AgentInboxMessage,
  AgentInboxStore,
  AgentInboxSubscription,
  AgentSchedule,
} from "./types";

type AnyPgDatabase = PgAsyncDatabase<any, any>;
const portableJsonb = customType<{ data: unknown; driverData: unknown }>({
  dataType: () => "jsonb",
  fromDriver: (value) =>
    typeof value === "string" ? JSON.parse(value) : value,
  toDriver: (value) => JSON.stringify(value),
});
const encodedJsonb = <Value>(value: Value) =>
  sql<Value>`${JSON.stringify(value)}::text::jsonb`;

const namespaceOf = (value: string) => {
  if (!/^[a-z_][a-z0-9_]*$/i.test(value))
    throw new Error("Invalid SQL namespace");
  return value;
};
const bounded = (limit: number) => Math.max(1, Math.min(limit, 200));
const tenantDocument = (column: SQLWrapper) =>
  sql<string>`${column}->'target'->>'tenantId'`;
const textDocument = (column: SQLWrapper, key: "createdAt" | "updatedAt") =>
  sql<string>`${column}->>${key}`;

export const agentInboxDrizzleSchema = (namespace = "agent_inbox") => {
  const schema = pgSchema(namespaceOf(namespace));
  const subscriptions = schema.table(
    "subscriptions",
    {
      document: portableJsonb().$type<AgentInboxSubscription>().notNull(),
      enabled: boolean().notNull(),
      id: text().primaryKey(),
      kinds: text().array().notNull(),
      source: text().notNull(),
      tenant_id: text().notNull(),
    },
    (table) => [
      index("agent_inbox_subscription_idx")
        .on(table.tenant_id, table.source)
        .where(eq(table.enabled, true)),
    ],
  );
  const messages = schema.table(
    "messages",
    {
      document: portableJsonb().$type<AgentInboxMessage>().notNull(),
      expires_at: timestamp({ mode: "date", withTimezone: true }),
      id: text().primaryKey(),
      lease_expires_at: timestamp({ mode: "date", withTimezone: true }),
      lease_owner: text(),
      not_before: timestamp({ mode: "date", withTimezone: true }).notNull(),
      source: text().notNull(),
      source_event_id: text().notNull(),
      status: text().$type<AgentInboxMessage["status"]>().notNull(),
      subscription_id: text().notNull(),
    },
    (table) => [
      uniqueIndex("agent_inbox_message_source_event_idx").on(
        table.subscription_id,
        table.source,
        table.source_event_id,
      ),
      index("agent_inbox_due_idx").on(
        table.status,
        table.not_before,
        table.lease_expires_at,
      ),
    ],
  );
  const schedules = schema.table(
    "schedules",
    {
      document: portableJsonb().$type<AgentSchedule>().notNull(),
      enabled: boolean().notNull(),
      id: text().primaryKey(),
      next_at: timestamp({ mode: "date", withTimezone: true }).notNull(),
    },
    (table) => [
      index("agent_inbox_schedules_due_idx")
        .on(table.next_at)
        .where(eq(table.enabled, true)),
    ],
  );

  return { messages, schedules, subscriptions };
};

export const createDrizzleAgentInboxStore = <DB extends AnyPgDatabase>(
  db: DB,
  options: { namespace?: string } = {},
): AgentInboxStore => {
  const { messages, schedules, subscriptions } = agentInboxDrizzleSchema(
    options.namespace,
  );
  const messageTenant = tenantDocument(messages.document);
  const scheduleTenant = tenantDocument(schedules.document);

  return {
    advanceSchedule: async (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: schedules.document })
          .from(schedules)
          .where(
            and(
              eq(schedules.id, value.id),
              eq(schedules.next_at, new Date(value.expectedNextAt)),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        return (
          (
            await transaction
              .update(schedules)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  nextAt: value.nextAt,
                }),
                next_at: new Date(value.nextAt),
              })
              .where(
                and(
                  eq(schedules.id, value.id),
                  eq(schedules.next_at, new Date(value.expectedNextAt)),
                ),
              )
              .returning({ id: schedules.id })
          ).length === 1
        );
      }),
    cancelMessage: async (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: messages.document })
          .from(messages)
          .where(
            and(
              eq(messages.id, value.id),
              eq(messageTenant, value.tenantId),
              eq(messages.status, "pending"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        return (
          (
            await transaction
              .update(messages)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  status: "cancelled",
                  updatedAt: value.now,
                }),
                status: "cancelled",
              })
              .where(
                and(eq(messages.id, value.id), eq(messages.status, "pending")),
              )
              .returning({ id: messages.id })
          ).length === 1
        );
      }),
    claim: (value) =>
      db.transaction(async (transaction) => {
        const now = new Date(value.now);
        const [row] = await transaction
          .select({ document: messages.document })
          .from(messages)
          .where(
            and(
              or(
                and(
                  eq(messages.status, "pending"),
                  lte(messages.not_before, now),
                ),
                and(
                  eq(messages.status, "leased"),
                  lte(messages.lease_expires_at, now),
                ),
              ),
              or(isNull(messages.expires_at), gt(messages.expires_at, now)),
            ),
          )
          .orderBy(asc(messages.not_before))
          .for("update", { skipLocked: true })
          .limit(1);
        if (!row) return undefined;
        const next: AgentInboxMessage = {
          ...row.document,
          attempts: row.document.attempts + 1,
          leaseExpiresAt: value.leaseExpiresAt,
          leaseOwner: value.workerId,
          status: "leased",
          updatedAt: value.now,
        };
        const [updated] = await transaction
          .update(messages)
          .set({
            document: encodedJsonb(next),
            lease_expires_at: new Date(value.leaseExpiresAt),
            lease_owner: value.workerId,
            status: "leased",
          })
          .where(eq(messages.id, next.id))
          .returning({ document: messages.document });
        return updated?.document;
      }),
    claimDueSchedule: async (value) =>
      (
        await db
          .select({ document: schedules.document })
          .from(schedules)
          .where(
            and(
              eq(schedules.enabled, true),
              lte(schedules.next_at, new Date(value.now)),
            ),
          )
          .orderBy(asc(schedules.next_at))
          .limit(1)
      )[0]?.document,
    complete: (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: messages.document })
          .from(messages)
          .where(
            and(
              eq(messages.id, value.id),
              eq(messages.lease_owner, value.workerId),
              eq(messages.status, "leased"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        return (
          (
            await transaction
              .update(messages)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  leaseExpiresAt: undefined,
                  leaseOwner: undefined,
                  status: "completed",
                  updatedAt: value.now,
                }),
                lease_expires_at: null,
                lease_owner: null,
                status: "completed",
              })
              .where(
                and(
                  eq(messages.id, value.id),
                  eq(messages.lease_owner, value.workerId),
                  eq(messages.status, "leased"),
                ),
              )
              .returning({ id: messages.id })
          ).length === 1
        );
      }),
    enqueue: async (value) =>
      (
        await db
          .insert(messages)
          .values({
            document: encodedJsonb(value),
            expires_at: value.expiresAt ? new Date(value.expiresAt) : null,
            id: value.id,
            lease_expires_at: value.leaseExpiresAt
              ? new Date(value.leaseExpiresAt)
              : null,
            lease_owner: value.leaseOwner ?? null,
            not_before: new Date(value.notBefore),
            source: value.source,
            source_event_id: value.sourceEventId,
            status: value.status,
            subscription_id: value.subscriptionId,
          })
          .onConflictDoUpdate({
            set: { source_event_id: value.sourceEventId },
            target: [
              messages.subscription_id,
              messages.source,
              messages.source_event_id,
            ],
          })
          .returning({ document: messages.document })
      )[0]!.document,
    listMessages: async (value) => {
      const filters: SQL[] = [];
      if (value.tenantId) filters.push(eq(messageTenant, value.tenantId));
      if (value.status) filters.push(eq(messages.status, value.status));
      return (
        await db
          .select({ document: messages.document })
          .from(messages)
          .where(and(...filters))
          .orderBy(desc(textDocument(messages.document, "updatedAt")))
          .limit(bounded(value.limit))
      ).map(({ document }) => document);
    },
    listSchedules: async (value) =>
      (
        await db
          .select({ document: schedules.document })
          .from(schedules)
          .where(
            value.tenantId ? eq(scheduleTenant, value.tenantId) : undefined,
          )
          .orderBy(desc(textDocument(schedules.document, "createdAt")))
          .limit(bounded(value.limit))
      ).map(({ document }) => document),
    listSubscriptionInventory: async (value) =>
      (
        await db
          .select({ document: subscriptions.document })
          .from(subscriptions)
          .where(
            value.tenantId
              ? eq(subscriptions.tenant_id, value.tenantId)
              : undefined,
          )
          .orderBy(desc(textDocument(subscriptions.document, "createdAt")))
          .limit(bounded(value.limit))
      ).map(({ document }) => document),
    listSubscriptions: async (value) =>
      (
        await db
          .select({ document: subscriptions.document })
          .from(subscriptions)
          .where(
            and(
              eq(subscriptions.tenant_id, value.tenantId),
              eq(subscriptions.source, value.source),
              eq(subscriptions.enabled, true),
              arrayContains(subscriptions.kinds, [value.kind]),
            ),
          )
      ).map(({ document }) => document),
    retry: (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: messages.document })
          .from(messages)
          .where(
            and(
              eq(messages.id, value.id),
              eq(messages.lease_owner, value.workerId),
              eq(messages.status, "leased"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const status = value.deadLetter ? "dead_letter" : "pending";
        return (
          (
            await transaction
              .update(messages)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  lastError: value.error,
                  leaseExpiresAt: undefined,
                  leaseOwner: undefined,
                  notBefore: value.notBefore,
                  status,
                  updatedAt: value.now,
                }),
                lease_expires_at: null,
                lease_owner: null,
                not_before: new Date(value.notBefore),
                status,
              })
              .where(
                and(
                  eq(messages.id, value.id),
                  eq(messages.lease_owner, value.workerId),
                  eq(messages.status, "leased"),
                ),
              )
              .returning({ id: messages.id })
          ).length === 1
        );
      }),
    saveSchedule: async (value) => {
      await db
        .insert(schedules)
        .values({
          document: encodedJsonb(value),
          enabled: value.enabled,
          id: value.id,
          next_at: new Date(value.nextAt),
        })
        .onConflictDoUpdate({
          set: {
            document: encodedJsonb(value),
            enabled: value.enabled,
            next_at: new Date(value.nextAt),
          },
          target: schedules.id,
        });
    },
    saveSubscription: async (value) => {
      await db
        .insert(subscriptions)
        .values({
          document: encodedJsonb(value),
          enabled: value.enabled,
          id: value.id,
          kinds: value.kinds,
          source: value.source,
          tenant_id: value.target.tenantId,
        })
        .onConflictDoUpdate({
          set: {
            document: encodedJsonb(value),
            enabled: value.enabled,
            kinds: value.kinds,
            source: value.source,
            tenant_id: value.target.tenantId,
          },
          target: subscriptions.id,
        });
    },
    setScheduleEnabled: (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: schedules.document })
          .from(schedules)
          .where(
            and(eq(schedules.id, value.id), eq(scheduleTenant, value.tenantId)),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        return (
          (
            await transaction
              .update(schedules)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  enabled: value.enabled,
                }),
                enabled: value.enabled,
              })
              .where(eq(schedules.id, value.id))
              .returning({ id: schedules.id })
          ).length === 1
        );
      }),
    setSubscriptionEnabled: (value) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select({ document: subscriptions.document })
          .from(subscriptions)
          .where(
            and(
              eq(subscriptions.id, value.id),
              eq(subscriptions.tenant_id, value.tenantId),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        return (
          (
            await transaction
              .update(subscriptions)
              .set({
                document: encodedJsonb({
                  ...row.document,
                  enabled: value.enabled,
                }),
                enabled: value.enabled,
              })
              .where(
                and(
                  eq(subscriptions.id, value.id),
                  eq(subscriptions.tenant_id, value.tenantId),
                ),
              )
              .returning({ id: subscriptions.id })
          ).length === 1
        );
      }),
  };
};
