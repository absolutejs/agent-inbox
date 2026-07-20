# @absolutejs/agent-inbox

Durable inbound work for agents. Verified webhooks and event subscriptions are
deduplicated by source event, encrypted through an injected codec, leased to one
worker, retried with backoff, and dead-lettered after a bounded attempt count.
Interval schedules materialize deterministic occurrences without Redis.
Occurrences are inserted before a compare-and-swap advances the schedule, so a
crash cannot lose a tick and retries collapse on the deterministic source ID.

Every subscription and schedule carries a complete runtime budget, bounded
delivery attempts, and a message TTL. Tenant-scoped inventories and explicit
subscription, schedule, and pending-message controls let owner and operator
interfaces manage retained triggers without querying the package schema.

Every message pins the target agent's signed discovery identity and preserves
verification provenance. The runtime adapter starts a durable
`@absolutejs/agent-runtime` run with the inbox message ID as its idempotency key.
Webhook bodies are byte-limited and are rejected unless a source-specific
signature/claim verifier succeeds. The Postgres store uses `SKIP LOCKED`; the
memory store is intended for tests.
