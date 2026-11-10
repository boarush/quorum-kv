import { env } from "cloudflare:workers";
import {
  SELF, createExecutionContext, evictDurableObject, reset, runDurableObjectAlarm,
  runInDurableObject, waitOnExecutionContext,
} from "cloudflare:test";
import { afterEach, expect, it } from "vitest";
import worker from "../src/index";
import type { Env } from "../src/config";
import { config } from "../src/config";
import { QuorumKV, QuorumUnavailable } from "../src/coordinator";
import { compare, type RecordValue } from "../src/model";

const contexts: ExecutionContext[] = [];
const bytes = (value: string) => new TextEncoder().encode(value).buffer as ArrayBuffer;
const text = (value: ArrayBuffer | null) => value === null ? null : new TextDecoder().decode(value);
const stamped = (record: RecordValue) => ({ ...record, writtenAt: Date.now() });
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function setup(overrides: Partial<Env> = {}) {
  const bindings = { ...env, ...overrides } as Env;
  const ctx = createExecutionContext();
  contexts.push(ctx);
  const kv = new QuorumKV(bindings, ctx);
  const key = crypto.randomUUID();
  const replicas = await kv.replicas(key);
  return {
    kv, key, ctx, bindings, replicas,
    request: (method = "GET", body?: BodyInit, suffix = "", debug = false) =>
      worker.fetch(new Request(`https://test/kv/${key}${suffix}`, {
        method, body, headers: debug ? { "X-KV-Debug": "1" } : {},
      }), bindings, ctx),
  };
}

function repairFailingEnv(bindings: Env, failing: Set<string>): Env {
  const namespace = bindings.REPLICAS;
  return {
    ...bindings,
    REPLICAS: {
      idFromName: (name: string) => namespace.idFromName(name),
      get: (...args: Parameters<Env["REPLICAS"]["get"]>) => {
        const [id, options] = args;
        const stub = namespace.get(id, options);
        if (!id.name || !failing.has(id.name)) return stub;
        return {
          read: (key: string) => stub.read(key),
          readVersion: (key: string) => stub.readVersion(key),
          write: async () => { throw new Error("Injected repair failure"); },
        } as unknown as ReturnType<Env["REPLICAS"]["get"]>;
      },
    } as unknown as Env["REPLICAS"],
  } as Env;
}

afterEach(async () => {
  await Promise.all(contexts.splice(0).map(ctx => waitOnExecutionContext(ctx)));
  await reset();
});

it("roundtrips raw bytes and empty values through the Worker and real SQLite DOs", async () => {
  const h = await setup();
  const raw = new Uint8Array([0, 255, 128, 13, 10]);
  expect((await h.request()).status).toBe(404);
  expect((await h.request("PUT", raw)).status).toBe(204);
  const get = await h.request();
  expect(get.status).toBe(200);
  expect(new Uint8Array(await get.arrayBuffer())).toEqual(raw);
  expect(get.headers.get("Cache-Control")).toBe("no-store");
  expect((await h.request("PUT", "")).status).toBe(204);
  expect((await h.request()).status).toBe(200);
  expect((await h.request()).headers.get("Content-Type")).toBe("application/octet-stream");
});

it("routes an end-to-end request through the deployed module export", async () => {
  const key = crypto.randomUUID();
  const put = await SELF.fetch(`https://test/kv/${key}`, { method: "PUT", body: "edge" });
  expect(put.status).toBe(204);
  const get = await SELF.fetch(`https://test/kv/${key}`);
  expect(text(await get.arrayBuffer())).toBe("edge");
});

it("reads its writes, with strictly increasing versions on sequential overwrites", async () => {
  const h = await setup();
  let previous = 0;
  for (let i = 1; i <= 5; i++) {
    await h.kv.put(h.key, bytes(String(i)));
    expect(text(await h.kv.get(h.key))).toBe(String(i));
    await waitOnExecutionContext(h.ctx);
    const version = await h.replicas[0]!.stub.readVersion(h.key);
    expect(version.counter).toBeGreaterThan(previous);
    previous = version.counter;
  }
});

it("orders equal-counter versions by writer and ignores stale or duplicate writes", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  const high: RecordValue = { value: bytes("high"), version: { counter: 4, writer: "z" }, expiresAt: null };
  await stub.write(h.key, stamped(high));
  await stub.write(h.key, stamped({ ...high, value: bytes("low"), version: { counter: 4, writer: "a" } }));
  await stub.write(h.key, stamped({ ...high, value: bytes("older"), version: { counter: 3, writer: "zz" } }));
  await stub.write(h.key, stamped({ ...high, value: bytes("duplicate") }));
  expect(text((await stub.read(h.key)).value)).toBe("high");
});

it("deletes with a versioned tombstone and permits recreation", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("old"));
  await waitOnExecutionContext(h.ctx);
  const written = await h.replicas[0]!.stub.readVersion(h.key);
  await h.kv.delete(h.key);
  expect(await h.kv.get(h.key)).toBeNull();
  expect((await h.request()).status).toBe(404);
  await waitOnExecutionContext(h.ctx);
  const deleted = await h.replicas[0]!.stub.read(h.key);
  expect(deleted.value).toBeNull();
  expect(compare(deleted.version, written)).toBeGreaterThan(0);
  await h.kv.put(h.key, bytes("new"));
  expect(text(await h.kv.get(h.key))).toBe("new");
});

it("expires a value from its replicated deadline without a coordinator write", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("short-lived"), { ttlSeconds: 0.05 });
  await waitOnExecutionContext(h.ctx);
  const before = await h.replicas[0]!.stub.read(h.key);
  expect(before.expiresAt).not.toBeNull();
  await pause(100);
  expect(await h.kv.get(h.key)).toBeNull();
  const after = await h.replicas[0]!.stub.read(h.key);
  expect(after.version).toEqual(before.version);
  expect(after.expiresAt).toBe(before.expiresAt);
});

it("serves reads, writes, and deletes when any one replica is down", async () => {
  for (let down = 0; down < 3; down++) {
    const h = await setup();
    await h.replicas[down]!.stub.setFault({ fail: true });
    expect((await h.request("PUT", "available")).status).toBe(204);
    expect(text(await (await h.request()).arrayBuffer())).toBe("available");
    expect((await h.request("DELETE")).status).toBe(204);
    expect((await h.request()).status).toBe(404);
    await h.replicas[down]!.stub.setFault({});
  }
});

it("takes the fastest two replicas without waiting for a slow replica or its timeout", async () => {
  const h = await setup({ REPLICA_TIMEOUT_MS: "250" });
  await h.replicas[2]!.stub.setFault({ delayMs: 600 });
  const start = Date.now();
  expect((await h.request("PUT", "fast")).status).toBe(204);
  expect(text(await (await h.request()).arrayBuffer())).toBe("fast");
  expect(Date.now() - start).toBeLessThan(250);
  await pause(650); // Drain late RPCs before resetting their storage.
});

it("takes the fastest two replicas without waiting for a slow replica within its timeout", async () => {
  const h = await setup({ REPLICA_TIMEOUT_MS: "1000" });
  await h.replicas[2]!.stub.setFault({ delayMs: 500 });
  const start = Date.now();
  expect((await h.request("PUT", "fast")).status).toBe(204);
  expect(text(await (await h.request()).arrayBuffer())).toBe("fast");
  expect(Date.now() - start).toBeLessThan(250);
  await pause(550); // Drain late RPCs before resetting their storage.
});

it("returns 503 without quorum instead of serving a lone replica", async () => {
  const h = await setup();
  await Promise.all(h.replicas.slice(0, 2).map(({ stub }) => stub.setFault({ fail: true })));
  for (const method of ["GET", "PUT", "DELETE"]) {
    expect((await h.request(method, method === "PUT" ? "no-quorum" : undefined)).status).toBe(503);
  }
});

it("rejects a read when the repair quorum is unavailable", async () => {
  const h = await setup();
  const stale: RecordValue = { value: bytes("stale"), version: { counter: 1, writer: "a" }, expiresAt: null };
  const fresh: RecordValue = { value: bytes("fresh"), version: { counter: 2, writer: "b" }, expiresAt: null };
  await h.replicas[0]!.stub.write(h.key, stamped(stale));
  await Promise.all(h.replicas.slice(1).map(({ stub }) => stub.write(h.key, stamped(fresh))));
  await h.replicas[2]!.stub.setFault({ delayMs: 100 });
  const kv = new QuorumKV(
    repairFailingEnv(h.bindings, new Set([h.replicas[0]!.name, h.replicas[1]!.name])),
    h.ctx,
  );
  await expect(kv.get(h.key)).rejects.toBeInstanceOf(QuorumUnavailable);
  expect(kv.phases.map(phase => phase.name)).toEqual(["read", "repair"]);
  await waitOnExecutionContext(h.ctx);
  await h.replicas[2]!.stub.setFault({});
});

it("bounds unresponsive quorum attempts with per-replica timeouts", async () => {
  const h = await setup({ REPLICA_TIMEOUT_MS: "60" });
  await Promise.all(h.replicas.slice(0, 2).map(({ stub }) => stub.setFault({ delayMs: 300 })));
  const start = Date.now();
  expect((await h.request()).status).toBe(503);
  expect(Date.now() - start).toBeLessThan(250);
  await pause(320);
});

it("repairs a stale third replica when it participates in a read quorum", async () => {
  const h = await setup();
  await h.replicas[2]!.stub.setFault({ fail: true });
  await h.kv.put(h.key, bytes("repaired"));
  await waitOnExecutionContext(h.ctx);
  await h.replicas[2]!.stub.setFault({});
  expect((await h.replicas[2]!.stub.readVersion(h.key)).counter).toBe(0);
  await h.replicas[1]!.stub.setFault({ delayMs: 100 });
  expect(text(await h.kv.get(h.key))).toBe("repaired");
  expect(h.kv.phases.at(-1)!.name).toBe("repair");
  await waitOnExecutionContext(h.ctx);
  const records = await Promise.all(h.replicas.map(({ stub }) => stub.read(h.key)));
  expect(records.map(record => text(record.value))).toEqual(["repaired", "repaired", "repaired"]);
  expect(records.every(record => compare(record.version, records[0]!.version) === 0)).toBe(true);
});

it("lets a later completed write outrank an earlier partial write", async () => {
  const h = await setup();
  // A write that reached only one replica has a tag from before the later write.
  await h.replicas[2]!.stub.write(h.key, {
    value: bytes("partial"),
    version: { counter: Date.now() - 60_000, writer: "z" },
    expiresAt: null,
    writtenAt: Date.now(),
  });
  await h.replicas[2]!.stub.setFault({ fail: true });
  await h.kv.put(h.key, bytes("completed"));
  await waitOnExecutionContext(h.ctx);
  await h.replicas[2]!.stub.setFault({});
  // Delay one replica so the read quorum includes the completed and partial copies.
  await h.replicas[1]!.stub.setFault({ delayMs: 100 });
  expect(text(await h.kv.get(h.key))).toBe("completed");
  await waitOnExecutionContext(h.ctx);
  await h.replicas[1]!.stub.setFault({});
  const records = await Promise.all(h.replicas.map(({ stub }) => stub.read(h.key)));
  expect(records.map(record => text(record.value))).toEqual(["completed", "completed", "completed"]);
});

it("repairs tombstones and expired records rather than resurrecting stale values", async () => {
  for (const expired of [false, true]) {
    const h = await setup();
    const old: RecordValue = { value: bytes("old"), version: { counter: 1, writer: "a" }, expiresAt: null };
    const latest: RecordValue = {
      value: expired ? bytes("expired") : null,
      version: { counter: 2, writer: "b" },
      expiresAt: expired ? Date.now() - 100 : null,
    };
    await Promise.all(h.replicas.map(({ stub }) => stub.write(h.key, stamped(old))));
    await Promise.all(h.replicas.slice(0, 2).map(({ stub }) => stub.write(h.key, stamped(latest))));
    await h.replicas[1]!.stub.setFault({ fail: true });
    expect(await h.kv.get(h.key)).toBeNull();
    expect((await h.replicas[2]!.stub.readVersion(h.key)).counter).toBe(2);
  }
});

it("concurrent writers converge to the maximum version on all three replicas", async () => {
  const h = await setup();
  await Promise.all(Array.from({ length: 12 }, (_, i) => h.kv.put(h.key, bytes(`writer-${i}`))));
  await waitOnExecutionContext(h.ctx);
  const records = await Promise.all(h.replicas.map(({ stub }) => stub.read(h.key)));
  expect(records.every(record => compare(record.version, records[0]!.version) === 0)).toBe(true);
  expect(records.every(record => text(record.value) === text(records[0]!.value))).toBe(true);
  expect(text(await h.kv.get(h.key))).toBe(text(records[0]!.value));
  await h.kv.put(h.key, bytes("after-all-writers"));
  expect(text(await h.kv.get(h.key))).toBe("after-all-writers");
});

it("keeps SQL records across eviction and rejects late writes after restart", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  const record: RecordValue = { value: bytes("durable"), version: { counter: 2, writer: "z" }, expiresAt: null };
  await stub.write(h.key, stamped(record));
  await evictDurableObject(stub);
  await stub.write(h.key, stamped({ ...record, version: { counter: 1, writer: "z" }, value: bytes("late") }));
  expect(text((await stub.read(h.key)).value)).toBe("durable");
});

it("alarms remove expired payloads but retain versions until grace and both peer acknowledgments", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  await stub.write(h.key, stamped({ value: bytes("expires"), version: { counter: 7, writer: "a" }, expiresAt: Date.now() + 60_000 }));
  await runInDurableObject(stub, async (instance, state) => {
    state.storage.sql.exec("UPDATE records SET expires_at = ?, gc_at = ?", Date.now() - 1, Date.now() + 60_000);
    await instance.alarm();
    expect((await instance.read(h.key)).value).toBeNull();
    expect((await instance.readVersion(h.key)).counter).toBe(7);
    expect(await state.storage.getAlarm()).not.toBeNull();
    state.storage.sql.exec("UPDATE records SET gc_at = ?", Date.now() - 1);
  });
  expect(await runDurableObjectAlarm(stub)).toBe(true);
  expect((await stub.readVersion(h.key)).counter).toBe(7);
  for (let round = 0; round < 2; round++) {
    for (const peer of h.replicas.slice(1)) await runDurableObjectAlarm(peer.stub);
  }
  await runDurableObjectAlarm(stub);
  expect((await stub.readVersion(h.key)).counter).toBe(0);
  await runInDurableObject(stub, async (_, state) => {
    expect(await state.storage.getAlarm()).not.toBeNull();
  });
});

it("garbage-collects tombstones without extending grace on duplicate write-back", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  const tombstone: RecordValue = { value: null, version: { counter: 2, writer: "d" }, expiresAt: null };
  await stub.write(h.key, stamped(tombstone));
  const readGc = () => runInDurableObject(stub, (_, state) =>
    state.storage.sql.exec<{ gc_at: number }>("SELECT gc_at FROM records").one().gc_at);
  const first = await readGc();
  await pause(5);
  await stub.write(h.key, stamped(tombstone));
  expect(await readGc()).toBe(first);
  await runInDurableObject(stub, (_, state) => { state.storage.sql.exec("UPDATE records SET gc_at = ?", Date.now() - 1); });
  await runDurableObjectAlarm(stub);
  expect((await stub.readVersion(h.key)).counter).toBe(2);
  for (let round = 0; round < 2; round++) {
    for (const peer of h.replicas.slice(1)) await runDurableObjectAlarm(peer.stub);
  }
  await runDurableObjectAlarm(stub);
  expect((await stub.readVersion(h.key)).counter).toBe(0);
});

it("reports answering replicas and per-phase latency only when requested", async () => {
  const h = await setup();
  expect((await h.request("PUT", "debug", "", true)).headers.get("X-KV-Quorum")).not.toBeNull();
  const result = await h.request("GET", undefined, "", true);
  const phases = JSON.parse(result.headers.get("X-KV-Quorum")!);
  expect(phases).toHaveLength(1);
  expect(phases[0].answers).toHaveLength(2);
  expect(phases[0].answers[0].replica).toMatch(/^shard-\d+-r[012]$/);
  expect(phases[0].answers[0].latencyMs).toBeGreaterThanOrEqual(0);
  expect((await h.request()).headers.get("X-KV-Quorum")).toBeNull();
});

it("rejects malformed input, unsupported methods, and oversized values", async () => {
  const h = await setup();
  for (const ttl of ["0", "-1", "NaN", "Infinity", "1e300", ""]) {
    expect((await h.request("PUT", "bad", `?ttl=${ttl}`)).status).toBe(400);
  }
  expect((await h.request("POST")).status).toBe(405);
  expect((await h.request("PUT", new Uint8Array(1024 * 1024 + 1))).status).toBe(413);
  for (const path of ["/kv/", "/kv/%XX", `/kv/${"k".repeat(1025)}`]) {
    expect((await worker.fetch(new Request(`https://test${path}`), h.bindings, h.ctx)).status).toBe(400);
  }
  expect((await worker.fetch(new Request("https://test/elsewhere"), h.bindings, h.ctx)).status).toBe(404);
});

it("uses stable fixed shards, distinct hints, and validates topology configuration", async () => {
  const h = await setup();
  expect((await h.kv.replicas(h.key)).map(r => r.name)).toEqual(h.replicas.map(r => r.name));
  expect(config(h.bindings).shards).toBe(1024);
  expect(config(h.bindings).hints).toEqual(["apac", "me", "weur"]);
  expect(() => config({ ...h.bindings, LOCATION_HINTS: "apac,apac,weur" })).toThrow();
  expect(() => config({ ...h.bindings, SHARDS: "0" })).toThrow();
  expect(() => config({ ...h.bindings, REPLICA_TIMEOUT_MS: "-1" })).toThrow();
});

it("never exposes fault injection over HTTP", async () => {
  const h = await setup();
  expect((await worker.fetch(new Request("https://test/debug/fault"), h.bindings, h.ctx)).status).toBe(404);
  await runInDurableObject(h.replicas[0]!.stub, async instance => {
    await expect(instance.setFault({ delayMs: -1 })).rejects.toThrow();
  });
});

async function ageGc(replicas: Awaited<ReturnType<QuorumKV["replicas"]>>) {
  for (const { stub } of replicas) {
    await runInDurableObject(stub, (_, state) => {
      state.storage.sql.exec("UPDATE records SET gc_at = ? WHERE gc_at IS NOT NULL", Date.now() - 1);
    });
  }
}

async function syncRounds(replicas: Awaited<ReturnType<QuorumKV["replicas"]>>, rounds = 3) {
  for (let round = 0; round < rounds; round++) {
    for (const { stub } of replicas) await runDurableObjectAlarm(stub);
  }
}

it("keeps deleted and expired versions beyond grace while a replica is offline", async () => {
  for (const expired of [false, true]) {
    const h = await setup();
    await h.kv.put(h.key, bytes("old"));
    await waitOnExecutionContext(h.ctx);
    const oldVersion = await h.replicas[0]!.stub.readVersion(h.key);
    const missing = h.replicas[2]!.stub;
    await missing.setFault({ fail: true });
    if (expired) {
      await h.kv.put(h.key, bytes("expires"), { ttlSeconds: 0.001 });
      await pause(10);
    } else {
      await h.kv.delete(h.key);
    }
    await waitOnExecutionContext(h.ctx);
    await ageGc(h.replicas.slice(0, 2));
    await syncRounds(h.replicas.slice(0, 2));
    for (const { stub } of h.replicas.slice(0, 2)) {
      expect(compare(await stub.readVersion(h.key), oldVersion)).toBeGreaterThan(0);
      expect((await stub.read(h.key)).value).toBeNull();
    }
    await missing.setFault({});
    expect(text((await missing.read(h.key)).value)).toBe("old");
    await syncRounds(h.replicas);
    expect((await missing.read(h.key)).value).toBeNull();
    expect(await h.kv.get(h.key)).toBeNull();
    await ageGc(h.replicas);
    await syncRounds(h.replicas);
    expect(await h.kv.get(h.key)).toBeNull();
  }
});

it("converges without reads using bounded batches and durable peer cursors", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  const target = h.replicas[2]!.stub;
  for (let i = 0; i < 11; i++) {
    await source.write(`key-${i}`, stamped({
      value: bytes(String(i)), version: { counter: 1, writer: "batch" }, expiresAt: null,
    }));
  }
  await evictDurableObject(source);
  await runDurableObjectAlarm(target);
  await runInDurableObject(target, (_, state) => {
    expect(state.storage.sql.exec<{ count: number }>("SELECT COUNT(*) AS count FROM records").one().count).toBe(4);
  });
  await evictDurableObject(target);
  await runDurableObjectAlarm(target);
  await runDurableObjectAlarm(target);
  for (let i = 0; i < 11; i++) expect(text((await target.read(`key-${i}`)).value)).toBe(String(i));
  await runInDurableObject(target, (_, state) => {
    expect(state.storage.sql.exec<{ cursor: number }>("SELECT cursor FROM peers WHERE name = ?", h.replicas[0]!.name).one().cursor).toBe(11);
  });
});

it("acknowledges only applied batches on the following pull and keeps sequences after GC", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  await source.write(h.key, stamped({ value: null, version: { counter: 1, writer: "delete" }, expiresAt: null }));
  await ageGc(h.replicas.slice(0, 1));
  for (const { stub } of h.replicas.slice(1)) await runDurableObjectAlarm(stub);
  await runDurableObjectAlarm(source);
  expect((await source.readVersion(h.key)).counter).toBe(1);
  for (const { stub } of h.replicas.slice(1)) await runDurableObjectAlarm(stub);
  await runDurableObjectAlarm(source);
  expect((await source.readVersion(h.key)).counter).toBe(0);
  await evictDurableObject(source);
  await source.write(h.key, stamped({ value: bytes("new"), version: { counter: 2, writer: "new" }, expiresAt: null }));
  await runInDurableObject(source, (_, state) => {
    expect(state.storage.sql.exec<{ seq: number }>("SELECT seq FROM records").one().seq).toBe(2);
  });
});

it("rejects an old direct write after acknowledged GC", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("old"));
  await waitOnExecutionContext(h.ctx);
  const stale = {
    ...(await h.replicas[0]!.stub.read(h.key)),
    writtenAt: Date.now() - config(h.bindings).gcGraceMs - 1000,
  };
  await h.kv.delete(h.key);
  await waitOnExecutionContext(h.ctx);
  await syncRounds(h.replicas);
  await ageGc(h.replicas);
  await syncRounds(h.replicas);
  for (const { stub } of h.replicas) {
    expect((await stub.readVersion(h.key)).counter).toBe(0);
    await runInDurableObject(stub, async instance => {
      await expect(instance.write(h.key, stale)).rejects.toThrow("Write exceeds GC grace");
    });
    expect((await stub.read(h.key)).value).toBeNull();
  }
  expect(await h.kv.get(h.key)).toBeNull();
});

it("retries peer sync failures on later alarms without losing cursors", async () => {
  const h = await setup();
  await h.replicas[0]!.stub.write(h.key, stamped({
    value: bytes("retry"), version: { counter: 1, writer: "retry" }, expiresAt: null,
  }));
  await h.replicas[0]!.stub.setFault({ fail: true });
  await runDurableObjectAlarm(h.replicas[2]!.stub);
  expect((await h.replicas[2]!.stub.readVersion(h.key)).counter).toBe(0);
  await h.replicas[0]!.stub.setFault({});
  await runDurableObjectAlarm(h.replicas[2]!.stub);
  expect(text((await h.replicas[2]!.stub.read(h.key)).value)).toBe("retry");
});

it("syncs retained expired versions even when their deadline predates grace", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  await source.write(h.key, stamped({
    value: bytes("expired"), version: { counter: 8, writer: "retained" },
    expiresAt: Date.now() - 2 * config(h.bindings).gcGraceMs,
  }));
  await runDurableObjectAlarm(source);
  await runDurableObjectAlarm(h.replicas[2]!.stub);
  const synced = await h.replicas[2]!.stub.read(h.key);
  expect(synced.version.counter).toBe(8);
  expect(synced.value).toBeNull();
});

it("enforces the configurable byte limit for HTTP and the programmatic API before RPCs", async () => {
  const h = await setup({ MAX_VALUE_BYTES: "4" });
  expect((await h.request("PUT", new Uint8Array(4))).status).toBe(204);
  expect((await h.request("PUT", new Uint8Array(5))).status).toBe(413);
  expect((await h.request("PUT", "ééé")).status).toBe(413);
  const phases = h.kv.phases.length;
  await expect(h.kv.put(h.key, new ArrayBuffer(5))).rejects.toBeInstanceOf(RangeError);
  expect(h.kv.phases).toHaveLength(phases);
  await h.kv.put(h.key, new ArrayBuffer(4));
  expect((await h.kv.get(h.key))!.byteLength).toBe(4);
  expect(config(h.bindings).maxValueBytes).toBe(4);
  expect(() => config({ ...h.bindings, MAX_VALUE_BYTES: "0" })).toThrow();
});

it("accepts the default 1 MiB boundary and rejects a larger programmatic value", async () => {
  const h = await setup();
  const size = config(h.bindings).maxValueBytes;
  expect(size).toBe(1024 * 1024);
  await h.kv.put(h.key, new ArrayBuffer(size));
  expect((await h.kv.get(h.key))!.byteLength).toBe(size);
  await expect(h.kv.put(h.key, new ArrayBuffer(size + 1))).rejects.toBeInstanceOf(RangeError);
});

it("bounds streamed bodies by actual bytes instead of trusting Content-Length", async () => {
  const h = await setup({ MAX_VALUE_BYTES: "4" });
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(3));
      controller.enqueue(new Uint8Array(2));
      controller.close();
    },
  });
  const response = await worker.fetch(new Request(`https://test/kv/${h.key}`, {
    method: "PUT", body, headers: { "Content-Length": "1" },
  }), h.bindings, h.ctx);
  expect(response.status).toBe(413);
});
