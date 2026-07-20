export type AgentIdentityPin = {
  descriptorId: string;
  descriptorVersion: string;
  descriptorDigest: string;
};
export type AgentInboxBudget = {
  actions: number;
  costMicros: number;
  inputTokens: number;
  outputTokens: number;
  spendMinor: number;
  wallTimeMs: number;
};
export type AgentInboxTarget = {
  tenantId: string;
  userId: string;
  agentId: string;
  agent: AgentIdentityPin;
  budget: AgentInboxBudget;
  goal: string;
};
export type AgentInboxSubscription = {
  id: string;
  target: AgentInboxTarget;
  source: string;
  kinds: string[];
  maxAttempts: number;
  messageTtlMs: number;
  enabled: boolean;
  createdAt: string;
};
export type AgentInboxMessage = {
  id: string;
  subscriptionId: string;
  target: AgentInboxTarget;
  source: string;
  sourceEventId: string;
  kind: string;
  encodedPayload: unknown;
  provenance: { verifiedBy: string; verifiedAt: string; proof?: unknown };
  status: "pending" | "leased" | "completed" | "dead_letter" | "cancelled";
  attempts: number;
  maxAttempts: number;
  notBefore: string;
  expiresAt?: string;
  leaseOwner?: string;
  leaseExpiresAt?: string;
  lastError?: string;
  createdAt: string;
  updatedAt: string;
};
export type AgentSchedule = {
  id: string;
  target: AgentInboxTarget;
  source: string;
  kind: string;
  payload: unknown;
  intervalMs: number;
  nextAt: string;
  enabled: boolean;
  maxAttempts: number;
  messageTtlMs: number;
  createdAt: string;
};
export type AgentInboxStore = {
  saveSubscription(value: AgentInboxSubscription): Promise<void>;
  listSubscriptions(input: {
    tenantId: string;
    source: string;
    kind: string;
  }): Promise<AgentInboxSubscription[]>;
  listSubscriptionInventory(input: {
    tenantId?: string;
    limit: number;
  }): Promise<AgentInboxSubscription[]>;
  setSubscriptionEnabled(input: {
    id: string;
    tenantId: string;
    enabled: boolean;
  }): Promise<boolean>;
  enqueue(value: AgentInboxMessage): Promise<AgentInboxMessage>;
  listMessages(input: {
    tenantId?: string;
    status?: AgentInboxMessage["status"];
    limit: number;
  }): Promise<AgentInboxMessage[]>;
  cancelMessage(input: {
    id: string;
    tenantId: string;
    now: string;
  }): Promise<boolean>;
  claim(input: {
    workerId: string;
    now: string;
    leaseExpiresAt: string;
  }): Promise<AgentInboxMessage | undefined>;
  complete(input: {
    id: string;
    workerId: string;
    now: string;
  }): Promise<boolean>;
  retry(input: {
    id: string;
    workerId: string;
    now: string;
    notBefore: string;
    error: string;
    deadLetter: boolean;
  }): Promise<boolean>;
  saveSchedule(value: AgentSchedule): Promise<void>;
  listSchedules(input: {
    tenantId?: string;
    limit: number;
  }): Promise<AgentSchedule[]>;
  setScheduleEnabled(input: {
    id: string;
    tenantId: string;
    enabled: boolean;
  }): Promise<boolean>;
  claimDueSchedule(input: { now: string }): Promise<AgentSchedule | undefined>;
  advanceSchedule(input: {
    id: string;
    expectedNextAt: string;
    nextAt: string;
  }): Promise<boolean>;
};
export type AgentInboxVerifier = {
  id: string;
  verify(input: {
    source: string;
    eventId: string;
    kind: string;
    body: Uint8Array;
    headers: Headers;
  }): Promise<{
    valid: boolean;
    tenantId?: string;
    payload?: unknown;
    proof?: unknown;
  }>;
};
export type AgentInboxCodec = {
  encode(
    value: unknown,
    context: { tenantId: string; messageId: string },
  ): Promise<unknown>;
  decode(
    value: unknown,
    context: { tenantId: string; messageId: string },
  ): Promise<unknown>;
};
