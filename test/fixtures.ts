import { env } from "cloudflare:workers";
import {
  createExecutionContext,
  waitOnExecutionContext,
  runInDurableObject,
  runDurableObjectAlarm,
  evictDurableObject,
  reset,
} from "cloudflare:test";
import { afterEach, beforeEach, vi } from "vitest";
import { QuorumKV, type QuorumKVEnv, type Phase } from "quorum-kv";
import { Replica } from "quorum-kv/workers";
import { config } from "../src/config";
import { COLLECT_GARBAGE, EXPIRE_RECORDS, SCHEDULE_EXPIRATION } from "../src/statements";

export { runDurableObjectAlarm as alarm, evictDurableObject as evict };
export const drain = waitOnExecutionContext;
const contexts: ExecutionContext[] = [];
const usedShards = new Set<string>();

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(drain));
  vi.restoreAllMocks();
  await reset();
  usedShards.clear();
});

export type TestReplicas = { name: string; stub: DurableObjectStub<Replica> }[];

// Test-only knowledge of the persisted routing scheme, independent of coordinator internals.
export async function replicasFor(key: string, bindings: QuorumKVEnv): Promise<TestReplicas> {
  const options = config(bindings);
  const hash = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  const shard = new DataView(hash).getUint32(0) % options.shards;
  return options.hints.map((locationHint, i) => {
    const name = `shard-${shard}-r${i}`;
    return {
      name,
      stub: bindings.REPLICAS.get(bindings.REPLICAS.idFromName(name), { locationHint }),
    };
  });
}

export async function setup(overrides: Partial<QuorumKVEnv> = {}) {
  const bindings = { ...env, ...overrides };
  const ctx = createExecutionContext();
  contexts.push(ctx);
  const phases: Phase[] = [];
  const kv = new QuorumKV(bindings, ctx, { onPhase: phase => phases.push(phase) });
  let key: string;
  let replicas: TestReplicas;
  do {
    key = crypto.randomUUID();
    replicas = await replicasFor(key, bindings);
  } while (usedShards.has(replicas[0]!.name));
  usedShards.add(replicas[0]!.name);
  return {
    kv,
    phases,
    key,
    ctx,
    bindings,
    replicas,
  };
}

// RPC methods must stay on the prototype. Select faults by the real instance,
// without adding fields or methods to it or changing production bindings.
type Fault = { delayMs?: number; fail?: boolean };
let faults = new WeakMap<Replica, Fault>();
beforeEach(() => {
  faults = new WeakMap();
  const { read, readVersion, write, pull, alarm } = Replica.prototype;
  const run = async <T>(instance: Replica, call: () => Promise<T>) => {
    const fault = faults.get(instance);
    if (fault?.delayMs) await new Promise(resolve => setTimeout(resolve, fault.delayMs));
    if (fault?.fail) throw new Error("Injected replica failure");
    return call();
  };
  vi.spyOn(Replica.prototype, "read").mockImplementation(function (this: Replica, key) {
    return run(this, () => read.call(this, key));
  });
  vi.spyOn(Replica.prototype, "readVersion").mockImplementation(function (this: Replica, key) {
    return run(this, () => readVersion.call(this, key));
  });
  vi.spyOn(Replica.prototype, "write").mockImplementation(function (this: Replica, key, record) {
    return run(this, () => write.call(this, key, record));
  });
  vi.spyOn(Replica.prototype, "pull").mockImplementation(function (this: Replica, peer, cursor) {
    return run(this, () => pull.call(this, peer, cursor));
  });
  vi.spyOn(Replica.prototype, "alarm").mockImplementation(function (this: Replica) {
    return run(this, () => alarm.call(this));
  });
});

export const setFault = (stub: DurableObjectStub<Replica>, fault: Fault) =>
  runInDurableObject(stub, instance => {
    faults.set(instance, fault);
  });

export const age = (
  stub: DurableObjectStub<Replica>,
  values: { gcAt?: number; expiresAt?: number },
) =>
  runInDurableObject(stub, (_instance, state) => {
    if (values.expiresAt !== undefined)
      state.storage.sql.exec("UPDATE records SET expires_at = ?", values.expiresAt);
    if (values.gcAt !== undefined)
      state.storage.sql.exec("UPDATE records SET gc_at = ? WHERE gc_at IS NOT NULL", values.gcAt);
  });

export const inspect = (stub: DurableObjectStub<Replica>) =>
  runInDurableObject(stub, async (_instance, state) => ({
    rows: state.storage.sql
      .exec<{ key: string; gc_at: number | null; seq: number; counter: number }>(
        "SELECT key, gc_at, seq, counter FROM records",
      )
      .toArray(),
    peers: state.storage.sql
      .exec<{ name: string; cursor: number; ack: number }>("SELECT * FROM peers")
      .toArray(),
    alarm: await state.storage.getAlarm(),
  }));

export const expirationPlans = (stub: DurableObjectStub<Replica>) =>
  runInDurableObject(stub, (_instance, state) => {
    const now = Date.now();
    const query = state.storage.sql.exec<{ next: number | null }>(SCHEDULE_EXPIRATION, now);
    return {
      next: query.one().next,
      rowsRead: query.rowsRead,
      schedule: state.storage.sql
        .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${SCHEDULE_EXPIRATION}`, now)
        .toArray(),
      cleanup: state.storage.sql
        .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${EXPIRE_RECORDS}`, now)
        .toArray(),
      collect: state.storage.sql
        .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${COLLECT_GARBAGE}`, now)
        .toArray(),
    };
  });
