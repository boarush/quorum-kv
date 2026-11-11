import { DurableObject } from "cloudflare:workers";
import type { Env } from "./config";
import { config } from "./config";
import { replicaDebug, type ReplicaCommand } from "./testing";
import { compare, EMPTY, type RecordValue, type Version } from "./model";

type Row = {
  key: string;
  value: ArrayBuffer | null;
  counter: number;
  writer: string;
  expires_at: number | null;
  gc_at: number | null;
  seq: number;
};

// Leave ample headroom for concurrent pulls, RPC serialization, and SQLite buffers.
const SYNC_BATCH_BYTES = 4 * 1024 * 1024;

const recordValue = (row: Row): RecordValue => ({
  value: row.value,
  version: { counter: row.counter, writer: row.writer },
  expiresAt: row.expires_at,
});

export class Replica extends DurableObject<Env> {
  private fault = { delayMs: 0, fail: false };
  private readonly options;
  private readonly name: string;
  private readonly peers: { name: string; stub: DurableObjectStub<Replica> }[];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.options = config(env);
    // Named RPCs and their alarms preserve ctx.id.name in the current runtime.
    const name = ctx.id.name;
    if (!name || !/^shard-\d+-r[012]$/.test(name))
      throw new Error("Replica must be accessed by shard name");
    this.name = name;
    this.peers = this.options.hints.flatMap((locationHint, i) => {
      const peer = name.replace(/r[012]$/, `r${i}`);
      return peer === name
        ? []
        : [
            {
              name: peer,
              stub: env.REPLICAS.get(env.REPLICAS.idFromName(peer), { locationHint }),
            },
          ];
    });
    const sql = ctx.storage.sql;
    sql.exec(`CREATE TABLE IF NOT EXISTS records (
      key TEXT PRIMARY KEY, value BLOB, counter INTEGER NOT NULL,
      writer TEXT NOT NULL, expires_at REAL, gc_at REAL, seq INTEGER NOT NULL
    )`);
    if (
      !sql
        .exec<{ name: string }>("PRAGMA table_info(records)")
        .toArray()
        .some(column => column.name === "seq")
    ) {
      sql.exec("ALTER TABLE records ADD COLUMN seq INTEGER NOT NULL DEFAULT 0");
      sql.exec("UPDATE records SET seq = rowid");
    }
    sql.exec("CREATE INDEX IF NOT EXISTS changes ON records(seq)");
    sql.exec(
      "CREATE INDEX IF NOT EXISTS expirations ON records(expires_at) WHERE value IS NOT NULL",
    );
    sql.exec(
      "CREATE TABLE IF NOT EXISTS sequence (id INTEGER PRIMARY KEY CHECK(id = 1), seq INTEGER NOT NULL)",
    );
    sql.exec("INSERT OR IGNORE INTO sequence SELECT 1, COALESCE(MAX(seq), 0) FROM records");
    sql.exec(
      "CREATE TABLE IF NOT EXISTS peers (name TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0, ack INTEGER NOT NULL DEFAULT 0)",
    );
    for (const peer of this.peers)
      sql.exec("INSERT OR IGNORE INTO peers(name) VALUES (?)", peer.name);
    ctx.blockConcurrencyWhile(() => this.schedule());
  }

  private async before() {
    const { delayMs, fail } = this.fault;
    if (delayMs) await new Promise(resolve => setTimeout(resolve, delayMs));
    if (fail) throw new Error("Injected replica failure");
  }

  private stored(key: string): RecordValue {
    const row = this.ctx.storage.sql
      .exec<Row>("SELECT * FROM records WHERE key = ?", key)
      .toArray()[0];
    return row ? recordValue(row) : EMPTY;
  }

  async readVersion(key: string): Promise<Version> {
    await this.before();
    return this.stored(key).version;
  }

  async read(key: string): Promise<RecordValue> {
    await this.before();
    return this.stored(key);
  }

  private apply(key: string, record: RecordValue): void {
    if (compare(record.version, this.stored(key).version) <= 0) return;
    const gcAt =
      record.expiresAt !== null
        ? record.expiresAt + this.options.gcGraceMs
        : record.value === null
          ? Date.now() + this.options.gcGraceMs
          : null;
    const seq = this.ctx.storage.sql
      .exec<{ seq: number }>("UPDATE sequence SET seq = seq + 1 WHERE id = 1 RETURNING seq")
      .one().seq;
    this.ctx.storage.sql.exec(
      `INSERT INTO records (key, value, counter, writer, expires_at, gc_at, seq) VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value, counter=excluded.counter,
       writer=excluded.writer, expires_at=excluded.expires_at, gc_at=excluded.gc_at, seq=excluded.seq`,
      key,
      record.value,
      record.version.counter,
      record.version.writer,
      record.expiresAt,
      gcAt,
      seq,
    );
  }

  async write(key: string, record: RecordValue & { writtenAt: number }): Promise<void> {
    await this.before();
    if (
      !Number.isFinite(record.writtenAt) ||
      record.writtenAt < Date.now() - this.options.gcGraceMs
    ) {
      throw new RangeError("Write exceeds GC grace");
    }
    // Sequence allocation and visibility are atomic; sync uses the same version guard.
    this.ctx.storage.transactionSync(() => this.apply(key, record));
    await this.schedule();
  }

  async pull(
    fromPeer: string,
    cursor: number,
  ): Promise<{ rows: { key: string; record: RecordValue }[]; cursor: number }> {
    await this.before();
    if (
      !this.peers.some(peer => peer.name === fromPeer) ||
      !Number.isSafeInteger(cursor) ||
      cursor < 0
    ) {
      throw new Error("Invalid peer cursor");
    }
    this.ctx.storage.sql.exec(
      "UPDATE peers SET ack = MAX(ack, ?) WHERE name = ?",
      cursor,
      fromPeer,
    );
    const rows: { key: string; record: RecordValue }[] = [];
    let bytes = 0;
    // Iterate lazily: materializing the row limit first can exhaust DO memory.
    for (const row of this.ctx.storage.sql.exec<Row>(
      "SELECT * FROM records WHERE seq > ? ORDER BY seq LIMIT ?",
      cursor,
      this.options.syncBatch,
    )) {
      // Budget for a payload copy, UTF-16 strings, and per-record RPC metadata.
      const size =
        2 * (row.value?.byteLength ?? 0) + 2 * (row.key.length + row.writer.length) + 256;
      // Always return at least one row, even if a configured value limit permits
      // a row larger than the budget. A single row is bounded by SQLite's row limit.
      if (rows.length > 0 && bytes + size > SYNC_BATCH_BYTES) break;
      rows.push({ key: row.key, record: recordValue(row) });
      bytes += size;
      cursor = row.seq;
    }
    return { rows, cursor };
  }

  private async sync(peer: { name: string; stub: DurableObjectStub<Replica> }) {
    const cursor = this.ctx.storage.sql
      .exec<{ cursor: number }>("SELECT cursor FROM peers WHERE name = ?", peer.name)
      .one().cursor;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const batch = await Promise.race([
        peer.stub.pull(this.name, cursor),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("Sync timeout")), this.options.timeoutMs);
        }),
      ]);
      this.ctx.storage.transactionSync(() => {
        // Held versions can be arbitrarily old. Only direct coordinator writes expire.
        for (const { key, record } of batch.rows) this.apply(key, record);
        this.ctx.storage.sql.exec(
          "UPDATE peers SET cursor = ? WHERE name = ?",
          batch.cursor,
          peer.name,
        );
      });
    } finally {
      clearTimeout(timer);
    }
  }

  private async schedule() {
    const now = Date.now();
    const row = this.ctx.storage.sql
      .exec<{ next: number | null }>(
        "SELECT MIN(expires_at) AS next FROM records WHERE value IS NOT NULL AND expires_at > ?",
        now,
      )
      .one();
    const next = Math.min(now + this.options.syncMs, row.next ?? Infinity);
    const alarm = await this.ctx.storage.getAlarm();
    if (alarm === null || alarm > next) await this.ctx.storage.setAlarm(next);
  }

  async alarm(): Promise<void> {
    try {
      await this.before();
      await Promise.allSettled(this.peers.map(peer => this.sync(peer)));
      const now = Date.now();
      this.ctx.storage.sql.exec(
        "UPDATE records SET value = NULL WHERE expires_at <= ? AND value IS NOT NULL",
        now,
      );
      this.ctx.storage.sql.exec(
        "DELETE FROM records WHERE gc_at <= ? AND seq <= (SELECT MIN(ack) FROM peers)",
        now,
      );
    } finally {
      await this.schedule();
    }
  }

  async debug(command: ReplicaCommand) {
    if (this.env.TESTING !== "true") throw new Error("Debug RPC is disabled");
    return replicaDebug(this, this.ctx, command);
  }

  async setFault(fault: { delayMs?: number; fail?: boolean }): Promise<void> {
    if (this.env.TESTING !== "true") throw new Error("Fault injection is disabled");
    const delayMs = fault.delayMs ?? 0;
    if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > 60_000)
      throw new Error("Invalid delay");
    this.fault = { delayMs, fail: fault.fail ?? false };
  }
}
