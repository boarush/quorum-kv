quorum-kv is a small binary KV prototype backed by three SQLite Durable Objects per shard. It implements ABD multi-writer registers, with no leader or CAS. Two reachable replicas serve reads and writes while one fails or stalls.

A SHA-256 key hash selects one of 1024 fixed shards. Replica names are `shard-<n>-r<i>`. Distinct placement hints default to apac, me, and weur; hints apply only when objects are first located and do not guarantee separate regions.

Writes query three versions, take the first two successful replies, choose a higher counter with a random writer ID as a tie-breaker, then write to all three and wait for two acknowledgments. Deletes write a null tombstone. Reads query three full records and take two replies. Matching versions return immediately; differing versions require a quorum write-back of the newest record first. Each phase uses independent per-replica timeouts, defaulting to 1500 ms, and returns HTTP 503 without a quorum. Late requests continue in the background.

A normal read costs three DO RPCs and about one RTT to the second-fastest replica. Writes and deletes take six RPCs across two quorum RTTs; repaired reads also take six. This excludes Worker entry, object startup, storage, alarms, and queueing costs. There is no local-read lease.

`GET /kv/:key` returns raw bytes with 200, or 404 when absent, deleted, or expired. `PUT /kv/:key?ttl=seconds` accepts a raw binary body and returns 204. Positive fractional TTLs are supported. `DELETE /kv/:key` returns 204 even if already absent. Keys are URL-encoded UTF-8, up to 1024 bytes; values are limited to 1 MiB. Empty values are distinct from missing values. Set `X-KV-Debug: 1` to receive `X-KV-Quorum` with answering replica names and latency for each phase.

Run `npm ci`, then `npm run check` for TypeScript, ESLint, and the Vitest integration suite. Tests use actual local workerd SQLite Durable Objects through `@cloudflare/vitest-plugin`, the current successor to `@cloudflare/vitest-pool-workers`. Test-only RPC fault injection is enabled only by the Vitest binding, never by the HTTP API.

Run `npm run dev` for Alchemy local development at http://localhost:31463. Override the local port with `PORT`. Alchemy uses local stack state and needs no Cloudflare credentials for local execution. `npm run deploy` requires a configured Alchemy Cloudflare profile; deployment is not part of the local test suite. Keep the local `.alchemy` state for subsequent deploys. `npx wrangler dev --port 31463` is an alternative local runner using the included equivalent Wrangler binding and SQLite migration.

Runtime bindings `SHARDS`, `LOCATION_HINTS` (three comma-separated hints), `REPLICA_TIMEOUT_MS`, and `GC_GRACE_MS` override defaults. Set them consistently in the Alchemy Worker environment, or in Wrangler vars when using Wrangler. Defaults live in `src/config.ts`. Do not change the shard count, hash, replica names, or hint ordering for an existing deployment without a migration.

TTL deadlines travel with each versioned record. Reads hide expired records without generating writes. Alarms discard expired payloads, retain their version metadata for the GC grace period, and eventually remove expired metadata and tombstones. The default grace is one day. Duplicate writes do not extend tombstone grace.

Linearizable registers assume durable replicas, intersecting quorums, and retained versions. Finite garbage collection deliberately weakens this guarantee. A stale replica or delayed write can resurrect a deleted or expired value after its version metadata is collected, especially after an outage longer than the grace period. There is no anti-entropy scan, replica rebuild, or permanent deletion watermark; read repair only runs when a stale record participates in a mismatched read quorum. Do not treat this prototype's finite GC as production-safe deletion.

TTL uses wall clocks and assumes sufficiently synchronized Workers and replicas. Clock skew can change the visibility boundary around expiration. There are no multi-key transactions, snapshots, list operations, or exactly-once writes. A timed-out write can still commit, and concurrent writes select a single version rather than preserving every value. Requests are crash-fault tolerant, not Byzantine tolerant.

The HTTP endpoint has no authentication. Keep it local or add access control before exposing a deployment. Local tests exercise failure and ordering behavior, not actual regional placement, network partitions, clock synchronization, or production latency.
