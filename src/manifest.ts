import { defineManifest } from "@absolutejs/manifest";
import { Type } from "@sinclair/typebox";
export const manifest = defineManifest()({
  contract: 2,
  discovery: {
    audiences: ["agent-hosts", "automation-builders"],
    intents: [
      "trigger agents from webhooks",
      "schedule agent work",
      "retry agent events",
    ],
    keywords: [
      "agents",
      "webhooks",
      "schedules",
      "triggers",
      "retries",
      "dead-letters",
    ],
    protocols: ["HTTP Webhooks", "AbsoluteJS Agent Runtime"],
  },
  identity: {
    name: "@absolutejs/agent-inbox",
    category: "automation",
    tagline: "Wake agents from verified events, not ambient polling.",
    description:
      "Durable verified webhooks, event subscriptions, interval schedules, encrypted payloads, leases, retries, dead letters, and agent-runtime handoff.",
    docsUrl: "https://github.com/absolutejs/agent-inbox",
    accent: "#06b6d4",
  },
  settings: Type.Object({}),
  wiring: [],
});
