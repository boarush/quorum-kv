# quorum-kv

A small binary key-value store where two of three replicas is always enough to read or write —
no leader, no compare-and-swap, no extra coordination service.

quorum-kv is a prototype. Each key lives in exactly one shard, and each shard is three
SQLite-backed Durable Objects, so the data is ordinary SQL until you ask it for quorum
semantics. The consistency model is an ABD multi-writer register: every replica can accept a
write, and version numbers let anyone figure out which write won.

The trade-off is deliberate. You get read and write availability while one replica is down or
slow, and you pay for it with two round trips per write, no local-read lease, and a per-key
throughput ceiling of roughly one Durable Object.

## How the pieces fit together

A SHA-256 hash of the key picks one of 1024 fixed shards. Replica names follow the pattern
`shard-<n>-r<i>`, and each replica can derive its siblings from its own name. The current
runtime preserves `ctx.id.name` for named RPCs and alarms.

```mermaid
flowchart LR
    C[Caller Worker] -->|"api.get / put / delete"| W["Api<br/>WorkerEntrypoint"]
    W -->|"SHA-256(key) mod 1024"| S["shard n"]
    S --> R0["shard-n-r0<br/>apac"]
    S --> R1["shard-n-r1<br/>me"]
    S --> R2["shard-n-r2<br/>weur"]
    R0 <-.->|"anti-entropy pull"| R1
    R1 <-.->|"anti-entropy pull"| R2
    R0 <-.->|"anti-entropy pull"| R2
```

The default placement hints are `apac`, `me`, and `weur`. They are only hints: they apply when
an object is first located and do not guarantee that the three replicas live in separate
regions. You can change them, but don't change the shard count, hash, replica names, or hint
ordering of an existing deployment without a migration.

## Request flows

### Writing a value

A write is two quorum phases. First the coordinator asks all three replicas for their current
version and waits for two answers. Then it computes a new version, sends it to all three, and
waits for two acknowledgments.

```mermaid
sequenceDiagram
    autonumber
    participant C as Caller
    participant W as Coordinator
    participant R0 as r0
    participant R1 as r1
    participant R2 as r2

    C->>W: api.put("my-key", value)
    W->>R0: readVersion
    W->>R1: readVersion
    W->>R2: readVersion
    Note over W: keep the first 2 replies
    R0-->>W: counter
    R1-->>W: counter
    Note over W: counter = max(now, highest seen + 1)<br/>writer = random UUID
    W->>R0: write(record)
    W->>R1: write(record)
    W->>R2: write(record)
    Note over W: keep the first 2 acks
    R0-->>W: ok
    R2-->>W: ok
    W-->>C: resolves
    Note over R1: the third request keeps<br/>running in the background
```

The version counter is wall-clock milliseconds, but always at least one above the highest
counter observed. A random writer ID breaks ties within the same millisecond. Deletes use the
same path with a null tombstone as the value.

If two of the three replicas don't answer within the phase timeout, the request throws
`QuorumUnavailable`. Requests that were already dispatched are not cancelled — they finish in
the background, which is why a timed-out write can still commit.

### Reading a value

A read is usually one quorum round. If the two replies disagree on the version, the coordinator
repairs the lagging replicas first, then answers.

```mermaid
flowchart TD
    A["api.get(\"my-key\")"] --> B["Ask all 3 replicas,<br/>keep the first 2 full records"]
    B --> C{"Versions match?"}
    C -->|yes| D{"Expired?"}
    C -->|no| E["Write the newest record back<br/>to all 3 replicas"]
    E --> D
    D -->|"yes"| G["null"]
    D -->|"no"| V{"Value is null?<br/>(tombstone)"}
    V -->|"yes"| G
    V -->|"no"| F["ArrayBuffer"]
    B -.->|"fewer than 2 replies"| H["QuorumUnavailable"]
```

Repairs reuse the version from the read instead of minting a new one, and they carry the
read's _start_ time. That stamp matters: a stalled read can't come back later and pass old data
off as fresh.

Reads also hide expired records, including when only a timeout expired them. Expiration on its
own never generates a write, but a read still repairs if the two replies disagree.

### What a request costs

| Operation              | DO RPCs | Quorum round trips                 |
| ---------------------- | ------- | ---------------------------------- |
| Normal read            | 3       | ~1 (to the second-fastest replica) |
| Write / delete         | 6       | 2                                  |
| Read that needs repair | 6       | 2                                  |

Those counts exclude the Worker entrypoint, object startup, storage work, alarms, and queueing.
There is no local-read lease, and because every key is pinned to a single three-replica shard,
per-key throughput is bounded by one Durable Object rather than scaling with the shard count.

## RPC API

`Api` is a `WorkerEntrypoint` exported from `src/index.ts`. Bind it from another Worker with
`Cloudflare.WorkerEntrypoint<Api>(worker, "Api")`; callers then use normal async methods.

| Method   | Signature                          | Result                                                    |
| -------- | ---------------------------------- | --------------------------------------------------------- |
| `get`    | `get(key)`                         | `ArrayBuffer`, or `null` when absent, deleted, or expired |
| `put`    | `put(key, value, { ttlSeconds? })` | Resolves after the write quorum                           |
| `delete` | `delete(key)`                      | Resolves after the delete quorum; absent keys are fine    |

Keys are UTF-8 strings capped at 1024 bytes. Values default to a 1 MiB limit and can be raised
with `MAX_VALUE_BYTES`. `put` throws `RangeError` for an invalid key, invalid TTL, or oversized
value before any replica RPC happens.

The default value limit leaves room for key and version metadata under the
[8 MB SQLite row limit](https://developers.cloudflare.com/durable-objects/platform/limits/index.md).
If you raise it, leave that overhead in place. An empty value is a real value and is distinct
from a missing key.

All methods fail with `QuorumUnavailable` when fewer than two replicas answer within the phase
timeout. RPC arguments arrive as structured-clone values, so the value is already in memory;
there is no streamed-body limit at the entrypoint.

Across the RPC boundary, `RangeError` arrives as a real `RangeError`. Custom error classes do
not survive intact, so `QuorumUnavailable` arrives as a plain `Error` with
`name: "QuorumUnavailable"` and `code: "QUORUM_UNAVAILABLE"`. Check those properties rather than
using `instanceof QuorumUnavailable`.

## Configuration

All of these are Worker environment bindings. Defaults live in `src/config.ts`, and overriding
them in the Alchemy Worker environment keeps local and deployed behavior in sync.

| Binding              | Default            | What it controls                                               |
| -------------------- | ------------------ | -------------------------------------------------------------- |
| `SHARDS`             | `1024`             | Number of key shards                                           |
| `LOCATION_HINTS`     | `apac,me,weur`     | Three distinct placement hints, in replica order               |
| `REPLICA_TIMEOUT_MS` | `1500`             | Per-phase, per-replica timeout; also the shared sync deadline  |
| `GC_GRACE_MS`        | `86400000` (1 day) | Minimum age before a tombstone or expired row can be collected |
| `SYNC_MS`            | `30000`            | Anti-entropy period, and how often the cleanup alarm runs      |
| `SYNC_BATCH`         | `256`              | Rows per sync pull                                             |
| `MAX_VALUE_BYTES`    | `1048576` (1 MiB)  | Largest accepted value                                         |

## Expiration and garbage collection

TTL deadlines travel with each versioned record, so every replica can hide expired data on its
own. Expiring a value does not write anything; the row keeps its version metadata and simply
loses its payload. A partial expiration index handles both deadline scheduling and payload
cleanup without scanning non-TTL rows, and a partial `gc_at` index keeps collection off a full
table scan.

Payloads are cleared at their deadline or at the next periodic alarm, whichever comes first.
Tombstone collection rides the same `SYNC_MS` alarm, so a larger `SYNC_MS` delays both.

Rows are only physically deleted after the GC grace period _and_ acknowledgment from both peers
that they have seen the row's local sequence. Duplicate writes don't extend tombstone grace. A
peer that is down forever blocks collection for the rows it hasn't acknowledged; replacing or
wiping a replica needs a rebuild protocol that doesn't exist yet.

## Background sync (anti-entropy)

Each replica pulls changed rows from its two siblings every `SYNC_MS` (30 seconds by default),
including right after a write. Clients never have to trigger this.

A pull returns at most `SYNC_BATCH` rows, assembled incrementally under a 4 MiB budget that
accounts for payload copies, strings, and metadata — headroom for RPC serialization and for
pulls running against both peers at once. Each peer's sync stops after eight pulls, 16 MiB of
budgeted bytes, or one shared `REPLICA_TIMEOUT_MS` deadline, whichever comes first. A final
batch may exceed the byte quota, and a single row bigger than the budget is returned alone so
recovery still advances; its size is bounded by the SQLite row limit.

Every applied version gets a monotonically increasing durable local sequence number. Applying
a batch and advancing its cursor happen atomically, and the next pull acknowledges the
previously applied batch. If a pull fails or times out, that batch stays unapplied, while
earlier completed batches keep their durable progress for later alarms.

Those per-alarm quotas bound backlog work so cleanup and rescheduling can continue, but
sustained writes can still outrun anti-entropy. Shards that have been touched keep scheduling
periodic alarms even when empty, so a stale peer can catch up without any client traffic.

```mermaid
flowchart LR
    A["Alarm fires<br/>every SYNC_MS or at next TTL"] --> B["Pull from each peer<br/>≤ 8 pulls, 16 MiB, 1 timeout"]
    B --> C["Drop payloads past their deadline"]
    C --> D["Collect tombstones and<br/>expired rows past grace + peer acks"]
    D --> E["Schedule the next alarm"]
```

## What the consistency guarantee actually covers

The register is linearizable as long as you keep these assumptions:

- Replicas are durable, quorums intersect, and versions are retained.
- Clocks are roughly synchronized and end-to-end operation plus sync delays are bounded.
- The GC grace is much larger than replica timeouts and those bounds.

Versions are wall-clock milliseconds bumped above anything already seen, so an earlier partial
write can't outrank a later completed one — except within clock skew or within the same clock
tick. Coordinator writes and repairs stamp `writtenAt` when the operation starts, and replicas
reject a direct write older than their GC grace, even after GC. Repairs carry the read's start
time and never mint a new version. Rows applied by sync skip the age check because they already
live on a durable replica.

The main thing to watch is that an old read result must never be re-stamped by a repair after
the grace period has passed. A coordinator timeout doesn't cancel an RPC that already left, so
arbitrarily stalled operations, unbounded clock skew, or replica data loss are outside the
guarantee. There is also no permanent per-key deletion watermark.

And the usual small-store caveats: no multi-key transactions, snapshots, list operations, or
exactly-once writes. A timed-out write can still commit, and concurrent writes pick a single
winner rather than preserving every value. Requests tolerate crash faults, not Byzantine ones.

TTL is wall-clock based too, so clock skew between Workers and replicas can move the visibility
boundary around an expiration.

## Development

```sh
npm ci
npm run check        # TypeScript, Oxlint, Oxfmt check, and the Vitest suite
npm run format       # apply Oxfmt
```

`npm test` runs the suite locally by default, without Cloudflare credentials or cloud
state-store bootstrap.

### Testing Durable Objects

`npm run test:workers` runs the KV suite using Cloudflare's official
[Workers Vitest integration](https://developers.cloudflare.com/workers/testing/vitest-integration/)
(`@cloudflare/vitest-plugin`). Tests and the real SQLite Durable Objects run inside workerd.
Tests access bindings through `cloudflare:workers` and use `cloudflare:test` for instance and
storage inspection, alarm delivery, and eviction.

Fault injection uses Vitest prototype spies scoped to selected instances, entirely in
`test/fixtures.ts`. After each test, background contexts drain, spies are restored, and binding
storage is reset. Only this project's Vitest configuration sets `SYNC_BATCH=4` for pagination
coverage; the runtime has no test flag, debug bridge, or fault-injection RPCs.

## Running and deploying

`npm run dev` starts Alchemy local development. The Worker has no HTTP handler; it exposes
`Api` over a service binding and hosts the `Replica` Durable Objects. Use the Vitest suite, or
bind the entrypoint from another Worker, to exercise it. Override the local port with `PORT` if
needed.

Alchemy keeps deployment state in the Cloudflare state store, so both local development and
`npm run deploy` need a configured Cloudflare profile. On first use, `Cloudflare.state()`
deploys or adopts the account-level `alchemy-state-store` Worker. This production/dev state
backend is separate from the test stack's local state; local tests never deploy cloud
resources.

## Please don't ship this as-is

The `Api` entrypoint has no per-caller authorization. Service bindings keep it off the public
internet, but any Worker with the binding can read and write every key. Restrict who can bind
it, or add access control in front of it, before treating it as multi-tenant.

The local tests exercise failure and ordering behavior — not real regional placement, network
partitions, clock synchronization, or production latency.
