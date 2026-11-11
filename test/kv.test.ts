import {
  setup,
  it,
  expect,
  drain,
  deployedRequest,
  request,
  alarm,
  age,
  inspect,
  expirationPlans,
  evict,
  productionUrl,
} from "./harness";
import { config } from "../src/config";
import type { Env } from "../src/config";
import { QuorumKV, QuorumUnavailable } from "../src/coordinator";
import { compare, type RecordValue } from "../src/model";
import { SYNC_ALARM_PULLS, SYNC_BATCH_BYTES } from "../src/sync";

const bytes = (value: string) => new TextEncoder().encode(value).buffer as ArrayBuffer;
const text = (value: ArrayBuffer | null) =>
  value === null ? null : new TextDecoder().decode(value);
const stamped = (record: RecordValue) => ({ ...record, writtenAt: Date.now() });
const pause = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

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
          write: async () => {
            throw new Error("Injected repair failure");
          },
        } as unknown as ReturnType<Env["REPLICAS"]["get"]>;
      },
    } as unknown as Env["REPLICAS"],
  } as Env;
}

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
  const { key } = await setup();
  const put = await deployedRequest(`/kv/${key}`, { method: "PUT", body: "edge" });
  expect(put.status).toBe(204);
  const get = await deployedRequest(`/kv/${key}`);
  expect(text(await get.arrayBuffer())).toBe("edge");
});

it("reads its writes, with strictly increasing versions on sequential overwrites", async () => {
  const h = await setup();
  let previous = 0;
  for (let i = 1; i <= 5; i++) {
    await h.kv.put(h.key, bytes(String(i)));
    expect(text(await h.kv.get(h.key))).toBe(String(i));
    await drain(h.ctx);
    const version = await h.replicas[0]!.stub.readVersion(h.key);
    expect(version.counter).toBeGreaterThan(previous);
    previous = version.counter;
  }
});

it("orders equal-counter versions by writer and ignores stale or duplicate writes", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  const high: RecordValue = {
    value: bytes("high"),
    version: { counter: 4, writer: "z" },
    expiresAt: null,
  };
  await stub.write(h.key, stamped(high));
  await stub.write(
    h.key,
    stamped({ ...high, value: bytes("low"), version: { counter: 4, writer: "a" } }),
  );
  await stub.write(
    h.key,
    stamped({ ...high, value: bytes("older"), version: { counter: 3, writer: "zz" } }),
  );
  await stub.write(h.key, stamped({ ...high, value: bytes("duplicate") }));
  expect(text((await stub.read(h.key)).value)).toBe("high");
});

it("deletes with a versioned tombstone and permits recreation", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("old"));
  await drain(h.ctx);
  const written = await h.replicas[0]!.stub.readVersion(h.key);
  await h.kv.delete(h.key);
  expect(await h.kv.get(h.key)).toBeNull();
  expect((await h.request()).status).toBe(404);
  await drain(h.ctx);
  const deleted = await h.replicas[0]!.stub.read(h.key);
  expect(deleted.value).toBeNull();
  expect(compare(deleted.version, written)).toBeGreaterThan(0);
  await h.kv.put(h.key, bytes("new"));
  expect(text(await h.kv.get(h.key))).toBe("new");
});

it("expires a value from its replicated deadline without a coordinator write", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("short-lived"), { ttlSeconds: 0.05 });
  await drain(h.ctx);
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
  const stale: RecordValue = {
    value: bytes("stale"),
    version: { counter: 1, writer: "a" },
    expiresAt: null,
  };
  const fresh: RecordValue = {
    value: bytes("fresh"),
    version: { counter: 2, writer: "b" },
    expiresAt: null,
  };
  await h.replicas[0]!.stub.write(h.key, stamped(stale));
  await Promise.all(h.replicas.slice(1).map(({ stub }) => stub.write(h.key, stamped(fresh))));
  await h.replicas[2]!.stub.setFault({ delayMs: 100 });
  const kv = new QuorumKV(
    repairFailingEnv(h.bindings, new Set([h.replicas[0]!.name, h.replicas[1]!.name])),
    h.ctx,
  );
  await expect(kv.get(h.key)).rejects.toBeInstanceOf(QuorumUnavailable);
  expect(kv.phases.map(phase => phase.name)).toEqual(["read", "repair"]);
  await drain(h.ctx);
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
  await drain(h.ctx);
  await h.replicas[2]!.stub.setFault({});
  expect((await h.replicas[2]!.stub.readVersion(h.key)).counter).toBe(0);
  await h.replicas[1]!.stub.setFault({ delayMs: 100 });
  expect(text(await h.kv.get(h.key))).toBe("repaired");
  expect(h.kv.phases.at(-1)!.name).toBe("repair");
  await drain(h.ctx);
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
  await drain(h.ctx);
  await h.replicas[2]!.stub.setFault({});
  // Delay one replica so the read quorum includes the completed and partial copies.
  await h.replicas[1]!.stub.setFault({ delayMs: 100 });
  expect(text(await h.kv.get(h.key))).toBe("completed");
  await drain(h.ctx);
  await h.replicas[1]!.stub.setFault({});
  const records = await Promise.all(h.replicas.map(({ stub }) => stub.read(h.key)));
  expect(records.map(record => text(record.value))).toEqual([
    "completed",
    "completed",
    "completed",
  ]);
});

it("repairs tombstones and expired records rather than resurrecting stale values", async () => {
  for (const expired of [false, true]) {
    const h = await setup();
    const old: RecordValue = {
      value: bytes("old"),
      version: { counter: 1, writer: "a" },
      expiresAt: null,
    };
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
  await drain(h.ctx);
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
  const record: RecordValue = {
    value: bytes("durable"),
    version: { counter: 2, writer: "z" },
    expiresAt: null,
  };
  await stub.write(h.key, stamped(record));
  await evict(stub);
  await stub.write(
    h.key,
    stamped({ ...record, version: { counter: 1, writer: "z" }, value: bytes("late") }),
  );
  expect(text((await stub.read(h.key)).value)).toBe("durable");
});

it("alarms remove expired payloads but retain versions until grace and both peer acknowledgments", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  await stub.write(
    h.key,
    stamped({
      value: bytes("expires"),
      version: { counter: 7, writer: "a" },
      expiresAt: Date.now() + 60_000,
    }),
  );
  await age(stub, { expiresAt: Date.now() - 1, gcAt: Date.now() + 60_000 });
  await alarm(stub);
  expect((await stub.read(h.key)).value).toBeNull();
  expect((await stub.readVersion(h.key)).counter).toBe(7);
  expect((await inspect(stub)).alarm).not.toBeNull();
  await age(stub, { gcAt: Date.now() - 1 });
  expect(await alarm(stub)).toBe(true);
  expect((await stub.readVersion(h.key)).counter).toBe(7);
  for (let round = 0; round < 2; round++) {
    for (const peer of h.replicas.slice(1)) await alarm(peer.stub);
  }
  await alarm(stub);
  expect((await stub.readVersion(h.key)).counter).toBe(0);
  expect((await inspect(stub)).alarm).not.toBeNull();
});

it("garbage-collects tombstones without extending grace on duplicate write-back", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  const tombstone: RecordValue = {
    value: null,
    version: { counter: 2, writer: "d" },
    expiresAt: null,
  };
  await stub.write(h.key, stamped(tombstone));
  const readGc = async () => (await inspect(stub)).rows[0]!.gc_at;
  const first = await readGc();
  await pause(5);
  await stub.write(h.key, stamped(tombstone));
  expect(await readGc()).toBe(first);
  await age(stub, { gcAt: Date.now() - 1 });
  await alarm(stub);
  expect((await stub.readVersion(h.key)).counter).toBe(2);
  for (let round = 0; round < 2; round++) {
    for (const peer of h.replicas.slice(1)) await alarm(peer.stub);
  }
  await alarm(stub);
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
    expect((await request(path)).status).toBe(400);
  }
  expect((await request("/elsewhere")).status).toBe(404);
});

it("sets no-store on error responses", async () => {
  const h = await setup();
  const ttl = await h.request("PUT", "bad", "?ttl=0");
  expect(ttl.status).toBe(400);
  expect(ttl.headers.get("Cache-Control")).toBe("no-store");
  const method = await h.request("POST");
  expect(method.status).toBe(405);
  expect(method.headers.get("Cache-Control")).toBe("no-store");
  const oversized = await h.request("PUT", new Uint8Array(1024 * 1024 + 1));
  expect(oversized.status).toBe(413);
  expect(oversized.headers.get("Cache-Control")).toBe("no-store");
  await Promise.all(h.replicas.slice(0, 2).map(({ stub }) => stub.setFault({ fail: true })));
  const unavailable = await h.request();
  expect(unavailable.status).toBe(503);
  expect(unavailable.headers.get("Cache-Control")).toBe("no-store");
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

it("bounds timer configuration and isolates the default hints", async () => {
  const h = await setup();
  expect(config({ ...h.bindings, LOCATION_HINTS: " apac , me , weur " }).hints).toEqual([
    "apac",
    "me",
    "weur",
  ]);
  expect(config({ ...h.bindings, REPLICA_TIMEOUT_MS: "2147483647" }).timeoutMs).toBe(2 ** 31 - 1);
  expect(() => config({ ...h.bindings, REPLICA_TIMEOUT_MS: "2147483648" })).toThrow();
  const hints = config(h.bindings).hints;
  hints[0] = "wnam";
  expect(config(h.bindings).hints).toEqual(["apac", "me", "weur"]);
});

it("gates debug routes on TESTING and validates fault injection", async () => {
  const h = await setup();
  expect((await request("/debug/fault")).status).toBe(404);
  for (const path of ["/_debug/request", `/_debug/replica/${h.replicas[0]!.name}`]) {
    expect((await fetch(`${productionUrl}${path}`, { method: "POST", body: "{}" })).status).toBe(
      404,
    );
  }
  await expect(h.replicas[0]!.stub.setFault({ delayMs: -1 })).rejects.toThrow();
});

async function ageGc(replicas: Awaited<ReturnType<QuorumKV["replicas"]>>) {
  for (const { stub } of replicas) {
    await age(stub, { gcAt: Date.now() - 1 });
  }
}

async function syncRounds(replicas: Awaited<ReturnType<QuorumKV["replicas"]>>, rounds = 3) {
  for (let round = 0; round < rounds; round++) {
    for (const { stub } of replicas) await alarm(stub);
  }
}

it("keeps deleted and expired versions beyond grace while a replica is offline", async () => {
  for (const expired of [false, true]) {
    const h = await setup();
    await h.kv.put(h.key, bytes("old"));
    await drain(h.ctx);
    const oldVersion = await h.replicas[0]!.stub.readVersion(h.key);
    const missing = h.replicas[2]!.stub;
    await missing.setFault({ fail: true });
    if (expired) {
      await h.kv.put(h.key, bytes("expires"), { ttlSeconds: 0.001 });
      await pause(10);
    } else {
      await h.kv.delete(h.key);
    }
    await drain(h.ctx);
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
    await source.write(
      `key-${i}`,
      stamped({
        value: bytes(String(i)),
        version: { counter: 1, writer: "batch" },
        expiresAt: null,
      }),
    );
  }
  await evict(source);
  // A pull that fills the row limit reports more, so one alarm drains every batch.
  // Pull as the other peer so the target's own cursor stays untouched.
  const first = await source.pull(h.replicas[1]!.name, 0);
  expect(first.rows).toHaveLength(4);
  expect(first.more).toBe(true);
  await alarm(target);
  expect((await inspect(target)).rows).toHaveLength(11);
  await evict(target);
  await alarm(target);
  for (let i = 0; i < 11; i++) expect(text((await target.read(`key-${i}`)).value)).toBe(String(i));
  expect(
    (await inspect(target)).peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor,
  ).toBe(11);
});

it("caps small-record pulls per alarm and resumes from the durable cursor", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  const target = h.replicas[2]!.stub;
  const batchRows = 4; // The test stack's SYNC_BATCH.
  const count = batchRows * (SYNC_ALARM_PULLS + 3);
  for (let i = 0; i < count; i++) {
    await source.write(
      `small-${i}`,
      stamped({
        value: new ArrayBuffer(0),
        version: { counter: 1, writer: "quota" },
        expiresAt: null,
      }),
    );
  }
  await alarm(target);
  const first = await inspect(target);
  const cursor = first.peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor;
  expect(cursor).toBe(batchRows * SYNC_ALARM_PULLS);
  expect(first.rows).toHaveLength(cursor);
  expect(first.alarm).not.toBeNull();
  await evict(target);
  await alarm(target);
  const recovered = await inspect(target);
  expect(recovered.peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor).toBe(count);
  expect(recovered.rows).toHaveLength(count);
});

it("shares one sync deadline across slow pulls without delaying cleanup for the backlog", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  const target = h.replicas[2]!.stub;
  const count = 16;
  for (let i = 0; i < count; i++) {
    await source.write(
      `slow-${i}`,
      stamped({
        value: new ArrayBuffer(0),
        version: { counter: 1, writer: "deadline" },
        expiresAt: null,
      }),
    );
  }
  await target.write(
    "expired-during-sync",
    stamped({
      value: bytes("expired"),
      version: { counter: 1, writer: "deadline" },
      expiresAt: Date.now() - 1,
    }),
  );
  const { timeoutMs } = config(h.bindings);
  const delayMs = timeoutMs * 0.6;
  await source.setFault({ delayMs });
  const start = Date.now();
  await alarm(target);
  expect(Date.now() - start).toBeLessThan(timeoutMs + 1000);
  const first = await inspect(target);
  const cursor = first.peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor;
  expect(cursor).toBe(4); // The second pull exceeds the shared deadline, not its own timeout.
  expect(first.rows).toHaveLength(cursor + 1);
  expect(first.alarm).not.toBeNull();
  expect((await target.read("expired-during-sync")).value).toBeNull();

  await source.setFault({});
  // A timed-out RPC cannot be cancelled. Its late response must not advance the cursor.
  await pause(delayMs);
  expect(
    (await inspect(target)).peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor,
  ).toBe(cursor);
  await evict(target);
  await alarm(target);
  const recovered = await inspect(target);
  expect(recovered.peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor).toBe(count);
  expect(recovered.rows).toHaveLength(count + 1);
});

it("recovers a large-value backlog across byte-bounded batches and durable cursors", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  const target = h.replicas[2]!.stub;
  // Include a supported SQLite row larger than the batch budget after copy overhead,
  // plus empty values and tombstones. None may stall or be skipped by pagination.
  const sizes = [1024 * 1024, 1024 * 1024, 3 * 1024 * 1024, 1024 * 1024, 0, null];
  for (const [i, size] of sizes.entries()) {
    await source.write(
      `large-${i}`,
      stamped({
        value: size === null ? null : new Uint8Array(size).fill(i).buffer,
        version: { counter: 1, writer: "large" },
        expiresAt: null,
      }),
    );
  }
  await evict(source);

  // Pagination is monotonic, byte-capped, and yields every record exactly once.
  let cursor = 0;
  let batches = 0;
  const keys: string[] = [];
  for (;;) {
    const batch = await source.pull(h.replicas[2]!.name, cursor);
    if (batch.rows.length === 0) {
      expect(batch).toEqual({ rows: [], cursor, more: false, bytes: 0 });
      break;
    }
    if (batch.bytes > SYNC_BATCH_BYTES) expect(batch.rows).toHaveLength(1);
    expect(batch.cursor).toBeGreaterThan(cursor);
    keys.push(...batch.rows.map(row => row.key));
    cursor = batch.cursor;
    batches += 1;
    if (batches === 1) expect(cursor).toBe(1); // Byte budget, not the test stack's four-row limit.
  }
  expect(cursor).toBe(sizes.length);
  expect(batches).toBeGreaterThan(2);
  expect(keys).toEqual(sizes.map((_, i) => `large-${i}`));

  // One alarm drains several byte-capped batches instead of one per sync interval.
  await evict(target);
  await alarm(target);
  const applied = await inspect(target);
  expect(applied.peers.find(peer => peer.name === h.replicas[0]!.name)!.cursor).toBe(sizes.length);
  expect(applied.rows).toHaveLength(sizes.length);

  // The durable cursor prevents the recovered prefix from being pulled again.
  await evict(target);
  await alarm(target);
  expect((await inspect(target)).rows).toHaveLength(sizes.length);

  for (const [i, size] of sizes.entries()) {
    const record = await target.read(`large-${i}`);
    expect(record.version.counter).toBe(1);
    if (size === null) expect(record.value).toBeNull();
    else {
      expect(record.value!.byteLength).toBe(size);
      expect(new Uint8Array(record.value!).every(byte => byte === i)).toBe(true);
    }
  }
});

it("indexes expiration scheduling and cleanup among many records without TTLs", async () => {
  const h = await setup();
  const stub = h.replicas[0]!.stub;
  for (let i = 0; i < 256; i++) {
    await stub.write(
      `persistent-${i}`,
      stamped({
        value: bytes("persistent"),
        version: { counter: 1, writer: "expiration" },
        expiresAt: null,
      }),
    );
  }
  await evict(stub);
  const plans = await expirationPlans(stub);
  expect(plans.next).toBeNull();
  expect(plans.rowsRead).toBeLessThanOrEqual(1);
  for (const plan of [plans.schedule, plans.cleanup]) {
    expect(plan.some(row => /USING (COVERING )?INDEX expirations\b/.test(row.detail))).toBe(true);
  }
  expect(plans.collect.some(row => /USING (COVERING )?INDEX collections\b/.test(row.detail))).toBe(
    true,
  );
  const expiresAt = Date.now() + 5000;
  await stub.write(
    "expiring",
    stamped({
      value: bytes("temporary"),
      version: { counter: 1, writer: "expiration" },
      expiresAt,
    }),
  );
  expect((await inspect(stub)).alarm).toBe(expiresAt);
  const scheduled = await expirationPlans(stub);
  expect(scheduled.next).toBe(expiresAt);
  expect(scheduled.rowsRead).toBeLessThanOrEqual(1);
  await stub.write(
    "expiring",
    stamped({
      value: bytes("expired"),
      version: { counter: 2, writer: "expiration" },
      expiresAt: Date.now() - 1,
    }),
  );
  await alarm(stub);
  expect((await stub.read("expiring")).value).toBeNull();
  expect((await stub.readVersion("expiring")).counter).toBe(2);
  expect((await inspect(stub)).rows).toHaveLength(257);
  expect(text((await stub.read("persistent-255")).value)).toBe("persistent");
  expect((await expirationPlans(stub)).next).toBeNull();
});

it("acknowledges only applied batches on the following pull and keeps sequences after GC", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  await source.write(
    h.key,
    stamped({ value: null, version: { counter: 1, writer: "delete" }, expiresAt: null }),
  );
  await ageGc(h.replicas.slice(0, 1));
  for (const { stub } of h.replicas.slice(1)) await alarm(stub);
  await alarm(source);
  expect((await source.readVersion(h.key)).counter).toBe(1);
  for (const { stub } of h.replicas.slice(1)) await alarm(stub);
  await alarm(source);
  expect((await source.readVersion(h.key)).counter).toBe(0);
  await evict(source);
  await source.write(
    h.key,
    stamped({ value: bytes("new"), version: { counter: 2, writer: "new" }, expiresAt: null }),
  );
  expect((await inspect(source)).rows[0]!.seq).toBe(2);
});

it("rejects an old direct write after acknowledged GC", async () => {
  const h = await setup();
  await h.kv.put(h.key, bytes("old"));
  await drain(h.ctx);
  const stale = {
    ...(await h.replicas[0]!.stub.read(h.key)),
    writtenAt: Date.now() - config(h.bindings).gcGraceMs - 1000,
  };
  await h.kv.delete(h.key);
  await drain(h.ctx);
  await syncRounds(h.replicas);
  await ageGc(h.replicas);
  await syncRounds(h.replicas);
  for (const { stub } of h.replicas) {
    expect((await stub.readVersion(h.key)).counter).toBe(0);
    await expect(stub.write(h.key, stale)).rejects.toThrow("Write exceeds GC grace");
    expect((await stub.read(h.key)).value).toBeNull();
  }
  expect(await h.kv.get(h.key)).toBeNull();
});

it("retries peer sync failures on later alarms without losing cursors", async () => {
  const h = await setup();
  await h.replicas[0]!.stub.write(
    h.key,
    stamped({
      value: bytes("retry"),
      version: { counter: 1, writer: "retry" },
      expiresAt: null,
    }),
  );
  await h.replicas[0]!.stub.setFault({ fail: true });
  await alarm(h.replicas[2]!.stub);
  expect((await h.replicas[2]!.stub.readVersion(h.key)).counter).toBe(0);
  await h.replicas[0]!.stub.setFault({});
  await alarm(h.replicas[2]!.stub);
  expect(text((await h.replicas[2]!.stub.read(h.key)).value)).toBe("retry");
});

it("syncs retained expired versions even when their deadline predates grace", async () => {
  const h = await setup();
  const source = h.replicas[0]!.stub;
  await source.write(
    h.key,
    stamped({
      value: bytes("expired"),
      version: { counter: 8, writer: "retained" },
      expiresAt: Date.now() - 2 * config(h.bindings).gcGraceMs,
    }),
  );
  await alarm(source);
  await alarm(h.replicas[2]!.stub);
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
  const response = await request(
    `/kv/${h.key}`,
    {
      method: "PUT",
      body,
      headers: { "Content-Length": "1" },
    },
    h.bindings,
  );
  expect(response.status).toBe(413);
});
