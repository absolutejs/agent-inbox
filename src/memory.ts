import type {
  AgentInboxMessage,
  AgentInboxStore,
  AgentInboxSubscription,
  AgentSchedule,
} from "./types";
export const createMemoryAgentInboxStore = (): AgentInboxStore => {
  const subscriptions = new Map<string, AgentInboxSubscription>();
  const messages = new Map<string, AgentInboxMessage>();
  const unique = new Map<string, string>();
  const schedules = new Map<string, AgentSchedule>();
  return {
    saveSubscription: async (value) => {
      subscriptions.set(value.id, structuredClone(value));
    },
    listSubscriptions: async ({ tenantId, source, kind }) =>
      [...subscriptions.values()]
        .filter(
          (item) =>
            item.enabled &&
            item.target.tenantId === tenantId &&
            item.source === source &&
            item.kinds.includes(kind),
        )
        .map((item) => structuredClone(item)),
    enqueue: async (value) => {
      const key = `${value.subscriptionId}:${value.source}:${value.sourceEventId}`;
      const previous = unique.get(key);
      if (previous) return structuredClone(messages.get(previous)!);
      unique.set(key, value.id);
      messages.set(value.id, structuredClone(value));
      return structuredClone(value);
    },
    claim: async ({ workerId, now, leaseExpiresAt }) => {
      const row = [...messages.values()]
        .filter(
          (item) =>
            ((item.status === "pending" &&
              Date.parse(item.notBefore) <= Date.parse(now)) ||
              (item.status === "leased" &&
                Date.parse(item.leaseExpiresAt ?? "") <= Date.parse(now))) &&
            (!item.expiresAt || Date.parse(item.expiresAt) > Date.parse(now)),
        )
        .sort((a, b) => a.notBefore.localeCompare(b.notBefore))[0];
      if (!row) return undefined;
      const next = {
        ...row,
        status: "leased" as const,
        leaseOwner: workerId,
        leaseExpiresAt,
        attempts: row.attempts + 1,
        updatedAt: now,
      };
      messages.set(row.id, next);
      return structuredClone(next);
    },
    complete: async ({ id, workerId, now }) => {
      const row = messages.get(id);
      if (!row || row.status !== "leased" || row.leaseOwner !== workerId)
        return false;
      messages.set(id, {
        ...row,
        status: "completed",
        leaseOwner: undefined,
        leaseExpiresAt: undefined,
        updatedAt: now,
      });
      return true;
    },
    retry: async ({ id, workerId, now, notBefore, error, deadLetter }) => {
      const row = messages.get(id);
      if (!row || row.status !== "leased" || row.leaseOwner !== workerId)
        return false;
      messages.set(id, {
        ...row,
        status: deadLetter ? "dead_letter" : "pending",
        leaseOwner: undefined,
        leaseExpiresAt: undefined,
        notBefore,
        lastError: error,
        updatedAt: now,
      });
      return true;
    },
    saveSchedule: async (value) => {
      schedules.set(value.id, structuredClone(value));
    },
    claimDueSchedule: async ({ now }) =>
      structuredClone(
        [...schedules.values()]
          .filter(
            (item) =>
              item.enabled && Date.parse(item.nextAt) <= Date.parse(now),
          )
          .sort((a, b) => a.nextAt.localeCompare(b.nextAt))[0],
      ),
    advanceSchedule: async ({ id, expectedNextAt, nextAt }) => {
      const row = schedules.get(id);
      if (!row || row.nextAt !== expectedNextAt) return false;
      schedules.set(id, { ...row, nextAt });
      return true;
    },
  };
};
