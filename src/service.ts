import type {
  AgentInboxCodec,
  AgentInboxMessage,
  AgentInboxStore,
  AgentInboxSubscription,
  AgentInboxVerifier,
  AgentSchedule,
} from "./types";
const identity: AgentInboxCodec = {
  encode: async (value) => structuredClone(value),
  decode: async (value) => structuredClone(value),
};
export class AgentInboxVerificationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AgentInboxVerificationError";
  }
}
export const createAgentInbox = ({
  store,
  verifiers,
  codec = identity,
  now = Date.now,
  id = () => crypto.randomUUID(),
  maxBodyBytes = 1_048_576,
}: {
  store: AgentInboxStore;
  verifiers: Record<string, AgentInboxVerifier>;
  codec?: AgentInboxCodec;
  now?: () => number;
  id?: () => string;
  maxBodyBytes?: number;
}) => ({
  subscribe: (value: AgentInboxSubscription) => store.saveSubscription(value),
  schedule: (value: AgentSchedule) => {
    if (!Number.isSafeInteger(value.intervalMs) || value.intervalMs < 1000)
      throw new Error("Schedule interval must be at least one second");
    return store.saveSchedule(value);
  },
  ingest: async ({
    source,
    eventId,
    kind,
    body,
    headers,
  }: {
    source: string;
    eventId: string;
    kind: string;
    body: Uint8Array;
    headers: Headers;
  }) => {
    if (body.byteLength > maxBodyBytes)
      throw new Error("Agent inbox event body is too large");
    const verifier = verifiers[source];
    if (!verifier) throw new Error("No verifier for agent inbox source");
    const checked = await verifier.verify({
      source,
      eventId,
      kind,
      body,
      headers,
    });
    if (!checked.valid || !checked.tenantId)
      throw new AgentInboxVerificationError(
        "Agent inbox event verification failed",
      );
    const subscriptions = await store.listSubscriptions({
      tenantId: checked.tenantId,
      source,
      kind,
    });
    const timestamp = new Date(now()).toISOString();
    const output: AgentInboxMessage[] = [];
    for (const subscription of subscriptions) {
      const messageId = id();
      const message: AgentInboxMessage = {
        id: messageId,
        subscriptionId: subscription.id,
        target: subscription.target,
        source,
        sourceEventId: eventId,
        kind,
        encodedPayload: await codec.encode(checked.payload, {
          tenantId: checked.tenantId,
          messageId,
        }),
        provenance: {
          verifiedBy: verifier.id,
          verifiedAt: timestamp,
          ...(checked.proof !== undefined ? { proof: checked.proof } : {}),
        },
        status: "pending",
        attempts: 0,
        maxAttempts: 5,
        notBefore: timestamp,
        createdAt: timestamp,
        updatedAt: timestamp,
      };
      output.push(await store.enqueue(message));
    }
    return output;
  },
  tickSchedule: async () => {
    const timestamp = new Date(now()).toISOString();
    const schedule = await store.claimDueSchedule({ now: timestamp });
    if (!schedule) return undefined;
    const occurrence = schedule.nextAt;
    const nextAt = new Date(
      Date.parse(occurrence) + schedule.intervalMs,
    ).toISOString();
    const messageId = id();
    const message = await store.enqueue({
      id: messageId,
      subscriptionId: `schedule:${schedule.id}`,
      target: schedule.target,
      source: schedule.source,
      sourceEventId: `${schedule.id}:${occurrence}`,
      kind: schedule.kind,
      encodedPayload: await codec.encode(schedule.payload, {
        tenantId: schedule.target.tenantId,
        messageId,
      }),
      provenance: { verifiedBy: "agent-inbox:schedule", verifiedAt: timestamp },
      status: "pending",
      attempts: 0,
      maxAttempts: schedule.maxAttempts,
      notBefore: occurrence,
      createdAt: timestamp,
      updatedAt: timestamp,
    });
    if (
      !(await store.advanceSchedule({
        id: schedule.id,
        expectedNextAt: occurrence,
        nextAt,
      }))
    )
      return undefined;
    return message;
  },
  workOne: async (
    workerId: string,
    handler: (input: {
      message: AgentInboxMessage;
      payload: unknown;
    }) => Promise<void>,
    options: {
      leaseMs?: number;
      retryDelayMs?: (attempt: number) => number;
    } = {},
  ) => {
    const claimedAt = now();
    const message = await store.claim({
      workerId,
      now: new Date(claimedAt).toISOString(),
      leaseExpiresAt: new Date(
        claimedAt + (options.leaseMs ?? 30_000),
      ).toISOString(),
    });
    if (!message) return undefined;
    try {
      const payload = await codec.decode(message.encodedPayload, {
        tenantId: message.target.tenantId,
        messageId: message.id,
      });
      await handler({ message, payload });
      if (
        !(await store.complete({
          id: message.id,
          workerId,
          now: new Date(now()).toISOString(),
        }))
      )
        throw new Error("Agent inbox lease was lost");
      return { ...message, status: "completed" as const };
    } catch (error) {
      const deadLetter = message.attempts >= message.maxAttempts;
      const current = now();
      await store.retry({
        id: message.id,
        workerId,
        now: new Date(current).toISOString(),
        notBefore: new Date(
          current +
            (options.retryDelayMs?.(message.attempts) ??
              Math.min(300_000, 1000 * 2 ** message.attempts)),
        ).toISOString(),
        error:
          error instanceof Error ? error.message : "Agent inbox handler failed",
        deadLetter,
      });
      throw error;
    }
  },
});

export const createAgentRuntimeInboxHandler =
  (runtime: {
    start(input: {
      actor: { tenantId: string; userId: string; agentId: string };
      agent: AgentInboxMessage["target"]["agent"];
      goal: string;
      input: unknown;
      idempotencyKey: string;
    }): Promise<unknown>;
  }) =>
  async ({
    message,
    payload,
  }: {
    message: AgentInboxMessage;
    payload: unknown;
  }) =>
    runtime.start({
      actor: {
        tenantId: message.target.tenantId,
        userId: message.target.userId,
        agentId: message.target.agentId,
      },
      agent: message.target.agent,
      goal: message.target.goal,
      input: {
        event: {
          source: message.source,
          kind: message.kind,
          provenance: message.provenance,
        },
        payload,
      },
      idempotencyKey: `inbox:${message.id}`,
    });
