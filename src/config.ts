import type { Replica } from "./replica";

export interface Env {
  REPLICAS: DurableObjectNamespace<Replica>;
  SHARDS?: string;
  LOCATION_HINTS?: string;
  REPLICA_TIMEOUT_MS?: string;
  GC_GRACE_MS?: string;
  SYNC_MS?: string;
  TESTING?: string;
}

export const DEFAULTS = {
  shards: 1024,
  hints: ["apac", "me", "weur"] as DurableObjectLocationHint[],
  timeoutMs: 1500,
  gcGraceMs: 86_400_000,
  syncMs: 30_000,
};

export function positiveInteger(value: string | undefined, fallback: number): number {
  const n = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new Error("Expected a positive safe integer");
  return n;
}

export function config(env: Env) {
  const hints = env.LOCATION_HINTS === undefined ? DEFAULTS.hints : env.LOCATION_HINTS.split(",");
  const allowed = new Set(["wnam", "enam", "sam", "weur", "eeur", "apac", "oc", "afr", "me"]);
  if (hints.length !== 3 || new Set(hints).size !== 3 || hints.some(hint => !allowed.has(hint))) {
    throw new Error("LOCATION_HINTS must contain three distinct supported location hints");
  }
  return {
    shards: positiveInteger(env.SHARDS, DEFAULTS.shards),
    hints: hints as DurableObjectLocationHint[],
    timeoutMs: positiveInteger(env.REPLICA_TIMEOUT_MS, DEFAULTS.timeoutMs),
    gcGraceMs: positiveInteger(env.GC_GRACE_MS, DEFAULTS.gcGraceMs),
    syncMs: positiveInteger(env.SYNC_MS, DEFAULTS.syncMs),
  };
}
