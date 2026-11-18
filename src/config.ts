import type { Replica } from "./replica";

/** String bindings shared by the coordinator and its replicas. */
export interface ReplicaConfig {
  QKV_SHARDS?: string;
  QKV_LOCATION_HINTS?: string;
  QKV_REPLICA_TIMEOUT_MS?: string;
  QKV_GC_GRACE_MS?: string;
  QKV_SYNC_MS?: string;
  QKV_SYNC_BATCH?: string;
  QKV_MAX_VALUE_BYTES?: string;
}

/** Bind REPLICAS to the SQLite-backed Replica Durable Object class. */
export interface QuorumKVEnv extends ReplicaConfig {
  REPLICAS: DurableObjectNamespace<Replica>;
}

export const DEFAULTS = {
  shards: 128,
  hints: Object.freeze(["apac", "enam", "weur"] as DurableObjectLocationHint[]),
  timeoutMs: 1500,
  gcGraceMs: 86_400_000,
  syncMs: 600_000,
  syncBatch: 256,
  maxValueBytes: 4 * 1024 * 1024,
};

const MAX_TIMER_MS = 2 ** 31 - 1;

export function positiveInteger(
  value: string | undefined,
  fallback: number,
  max = Number.MAX_SAFE_INTEGER,
): number {
  const n = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(n) || n <= 0 || n > max) {
    throw new Error(
      max === Number.MAX_SAFE_INTEGER
        ? "Expected a positive safe integer"
        : `Expected a positive safe integer no greater than ${max}`,
    );
  }
  return n;
}

export function config(env: ReplicaConfig) {
  const hints =
    env.QKV_LOCATION_HINTS === undefined
      ? [...DEFAULTS.hints]
      : env.QKV_LOCATION_HINTS.split(",").map(hint => hint.trim());
  const allowed = new Set(["wnam", "enam", "sam", "weur", "eeur", "apac", "oc", "afr", "me"]);
  if (hints.length !== 3 || new Set(hints).size !== 3 || hints.some(hint => !allowed.has(hint))) {
    throw new Error("QKV_LOCATION_HINTS must contain three distinct supported location hints");
  }
  return {
    shards: positiveInteger(env.QKV_SHARDS, DEFAULTS.shards),
    hints: hints as DurableObjectLocationHint[],
    timeoutMs: positiveInteger(env.QKV_REPLICA_TIMEOUT_MS, DEFAULTS.timeoutMs, MAX_TIMER_MS),
    gcGraceMs: positiveInteger(env.QKV_GC_GRACE_MS, DEFAULTS.gcGraceMs),
    syncMs: positiveInteger(env.QKV_SYNC_MS, DEFAULTS.syncMs),
    syncBatch: positiveInteger(env.QKV_SYNC_BATCH, DEFAULTS.syncBatch),
    maxValueBytes: positiveInteger(env.QKV_MAX_VALUE_BYTES, DEFAULTS.maxValueBytes),
  };
}
