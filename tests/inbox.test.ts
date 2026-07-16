import { describe, expect, test } from "bun:test";
import { createAgentInbox, createMemoryAgentInboxStore } from "../src";
const target = {
  tenantId: "tenant",
  userId: "user",
  agentId: "agent",
  agent: {
    descriptorId: "https://agent.example",
    descriptorVersion: "1",
    descriptorDigest: "sha256:abc",
  },
  goal: "Handle verified event",
};
describe("agent inbox", () => {
  test("verifies, deduplicates, leases, and delivers subscribed events", async () => {
    const store = createMemoryAgentInboxStore();
    let handled = 0;
    const inbox = createAgentInbox({
      store,
      verifiers: {
        github: {
          id: "github-sha256",
          verify: async ({ eventId }) => ({
            valid: true,
            tenantId: "tenant",
            payload: { eventId },
            proof: { signature: "ok" },
          }),
        },
      },
      now: () => Date.parse("2026-07-15T00:00:00Z"),
      id: (() => {
        let n = 0;
        return () => `id-${++n}`;
      })(),
    });
    await inbox.subscribe({
      id: "sub",
      target,
      source: "github",
      kinds: ["push"],
      enabled: true,
      createdAt: "2026-07-15T00:00:00Z",
    });
    const input = {
      source: "github",
      eventId: "evt",
      kind: "push",
      body: new TextEncoder().encode("{}"),
      headers: new Headers(),
    };
    expect(await inbox.ingest(input)).toHaveLength(1);
    expect((await inbox.ingest(input))[0]?.id).toBe("id-1");
    await inbox.workOne("worker", async ({ payload }) => {
      handled++;
      expect(payload).toEqual({ eventId: "evt" });
    });
    expect(handled).toBe(1);
  });
  test("materializes deterministic schedule occurrences once", async () => {
    const store = createMemoryAgentInboxStore();
    const inbox = createAgentInbox({
      store,
      verifiers: {},
      now: () => Date.parse("2026-07-15T00:00:00Z"),
      id: () => "message",
    });
    await inbox.schedule({
      id: "daily",
      target,
      source: "schedule",
      kind: "tick",
      payload: {},
      intervalMs: 60_000,
      nextAt: "2026-07-15T00:00:00Z",
      enabled: true,
      maxAttempts: 3,
      createdAt: "2026-07-14T00:00:00Z",
    });
    expect((await inbox.tickSchedule())?.sourceEventId).toBe(
      "daily:2026-07-15T00:00:00Z",
    );
    expect(await inbox.tickSchedule()).toBeUndefined();
  });
});
