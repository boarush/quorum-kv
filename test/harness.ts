import { randomInt, randomUUID } from "node:crypto";
import * as Test from "@alchemy.run/test/Vitest";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { expect } from "@effect/vitest";
import { makeStack } from "../alchemy.run";
import type { Env } from "../src/config";
import { QuorumKV } from "../src/coordinator";
import type { ReplicaCommand, DebugRequest } from "../src/testing";
import { stringify, parse } from "../src/testing";
import type { RecordValue, Version } from "../src/model";
import type { Replica } from "../src/replica";

const { test, beforeAll, afterAll, afterEach, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  dev: true,
});
const stage = process.env.ALCHEMY_TEST_STAGE ?? `test-${randomUUID().slice(0, 12)}`;
const Stack = makeStack({ testing: true, port: randomInt(20_000, 60_000) });
const GuardStack = makeStack({ port: randomInt(20_000, 60_000) });
const deployed = beforeAll(deploy(Stack, { stage }));
const guard = beforeAll(deploy(GuardStack, { stage: `${stage}-guard` }));
let base: string;
export let productionUrl: string;
beforeAll(
  Effect.gen(function* () {
    const api = (yield* deployed).url;
    const production = (yield* guard).url;
    if (!api || !production) throw new Error("Stack has no Worker URL");
    base = api;
    productionUrl = production;
  }),
);
afterAll(destroy(GuardStack, { stage: `${stage}-guard` }));
afterAll(destroy(Stack, { stage }));

export const it = (name: string, run: () => Promise<void>) => test(name, Effect.promise(run));
export { expect };

async function command(name: string, input: ReplicaCommand) {
  const response = await fetch(`${base}/_debug/replica/${name}`, {
    method: "POST",
    body: stringify(input),
  });
  if (!response.ok) throw new Error(await response.text());
  return parse(await response.text());
}

function remoteReplica(name: string) {
  return {
    read: (key: string): Promise<RecordValue> => command(name, { action: "read", key }),
    readVersion: (key: string): Promise<Version> => command(name, { action: "readVersion", key }),
    write: (key: string, record: RecordValue & { writtenAt: number }): Promise<void> =>
      command(name, { action: "write", key, record }),
    setFault: (fault: { delayMs?: number; fail?: boolean }): Promise<void> =>
      command(name, { action: "fault", fault }),
    debug: (input: ReplicaCommand) => command(name, input),
  };
}

type Context = { pending: Promise<unknown>[]; waitUntil(promise: Promise<unknown>): void };
const contexts: Context[] = [];
export async function drain(ctx: Context) {
  await Promise.allSettled(ctx.pending.splice(0));
}
afterEach(
  Effect.promise(async () => {
    await Promise.all(contexts.splice(0).map(drain));
  }),
);

// Isolate each fixture by shard, without deleting a live DO's backing database.
const usedShards = new Set<string>();
export async function setup(overrides: Partial<Env> = {}) {
  const bindings = {
    REPLICAS: {
      idFromName: (name: string) => ({ name }),
      get: (id: { name: string }) => remoteReplica(id.name),
    },
    ...overrides,
  } as Env;
  const ctx: Context = {
    pending: [],
    waitUntil(promise) {
      this.pending.push(promise);
    },
  };
  contexts.push(ctx);
  const kv = new QuorumKV(bindings, ctx);
  let key: string;
  let replicas: Awaited<ReturnType<QuorumKV["replicas"]>>;
  do {
    key = randomUUID();
    replicas = await kv.replicas(key);
  } while (usedShards.has(replicas[0]!.name));
  usedShards.add(replicas[0]!.name);
  return {
    kv,
    key,
    ctx,
    bindings,
    replicas,
    request: (method = "GET", body?: BodyInit, suffix = "", debug = false) =>
      request(
        `/kv/${key}${suffix}`,
        { method, body, headers: debug ? { "X-KV-Debug": "1" } : {} },
        overrides,
      ),
  };
}

export async function request(path: string, init: RequestInit = {}, overrides: Partial<Env> = {}) {
  const chunks: number[][] | undefined =
    init.body === undefined || init.body === null ? undefined : [];
  if (chunks) {
    if (init.body instanceof ReadableStream) {
      const reader = init.body.getReader();
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        chunks.push(Array.from(value));
      }
    } else {
      chunks.push(Array.from(new Uint8Array(await new Response(init.body).arrayBuffer())));
    }
  }
  const input: DebugRequest = {
    path,
    method: init.method ?? "GET",
    chunks,
    headers: Object.fromEntries(new Headers(init.headers)),
    overrides,
    // Timing scenarios measure the actual fast-quorum response, not background draining.
    drain: overrides.REPLICA_TIMEOUT_MS === undefined,
  };
  return fetch(`${base}/_debug/request`, { method: "POST", body: JSON.stringify(input) });
}

export const deployedRequest = (path: string, init?: RequestInit) => fetch(`${base}${path}`, init);
export const alarm = (stub: DurableObjectStub<Replica>): Promise<boolean> =>
  stub.debug({ action: "alarm" }) as Promise<boolean>;
export const age = (
  stub: DurableObjectStub<Replica>,
  values: { gcAt?: number; expiresAt?: number },
) => stub.debug({ action: "age", ...values });
export const inspect = (stub: DurableObjectStub<Replica>) =>
  stub.debug({ action: "inspect" }) as Promise<{
    rows: { key: string; gc_at: number | null; seq: number; counter: number }[];
    peers: { name: string; cursor: number; ack: number }[];
    alarm: number | null;
  }>;
export async function evict(stub: DurableObjectStub<Replica>) {
  // ctx.abort() deliberately rejects the in-flight RPC after storage.sync().
  await expect(stub.debug({ action: "evict" })).rejects.toThrow("Test eviction");
}
