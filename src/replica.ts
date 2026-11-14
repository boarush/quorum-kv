import { DurableObject } from "cloudflare:workers";
import type { Env } from "./config";
import { config } from "./config";
import { compare, EMPTY, type RecordValue, type Version } from "./model";
import { COLLECT_GARBAGE, EXPIRE_RECORDS, SCHEDULE_EXPIRATION } from "./statements";
import { SYNC_ALARM_BYTES, SYNC_ALARM_PULLS, SYNC_BATCH_BYTES, syncRowBytes } from "./sync";

type Row = {
  key: string;
  value: ArrayBuffer | null;
  counter: number;
  writer: string;
  expires_at: number | null;
  gc_at: number | null;
  seq: number;
};

const recordValue = (row: Row): RecordValue => ({
  value: row.value,
  version: { counter: row.counter, writer: row.writer },
  expiresAt: row.expires_at,
});

export class Replica extends DurableObject<Env> {
  private readonly options;
  private readonly name: string;
  private readonly peers: { name: string; locationHint: DurableObjectLocationHint }[];

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
      return peer === name ? [] : [{ name: peer, locationHint }];
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
    sql.exec("CREATE INDEX IF NOT EXISTS collections ON records(gc_at) WHERE gc_at IS NOT NULL");
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

  private stored(key: string): RecordValue {
    const row = this.ctx.storage.sql
      .exec<Row>("SELECT * FROM records WHERE key = ?", key)
      .toArray()[0];
    return row ? recordValue(row) : EMPTY;
  }

  async readVersion(key: string): Promise<Version> {
    return this.stored(key).version;
  }

  async read(key: string): Promise<RecordValue> {
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
  ): Promise<{
    rows: { key: string; record: RecordValue }[];
    cursor: number;
    more: boolean;
    bytes: number;
  }> {
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
    let more = false;
    let read = 0;
    // Iterate lazily: materializing the row limit first can exhaust DO memory.
    for (const row of this.ctx.storage.sql.exec<Row>(
      "SELECT * FROM records WHERE seq > ? ORDER BY seq LIMIT ?",
      cursor,
      this.options.syncBatch,
    )) {
      const size = syncRowBytes(row.key, row.writer, row.value);
      // Always return at least one row, even if a configured value limit permits
      // a row larger than the budget. A single row is bounded by SQLite's row limit.
      if (rows.length > 0 && bytes + size > SYNC_BATCH_BYTES) {
        more = true;
        break;
      }
      rows.push({ key: row.key, record: recordValue(row) });
      bytes += size;
      cursor = row.seq;
      read++;
    }
    // A full row limit may also leave a remainder; an extra empty pull is cheaper than a stall.
    more ||= read === this.options.syncBatch;
    // more signals that the byte budget or row limit truncated the batch.
    return { rows, cursor, more, bytes };
  }

  private async sync(peer: { name: string; locationHint: DurableObjectLocationHint }) {
    // A stub that threw may stay broken, so mint a fresh one per sync instead of caching.
    const stub = this.env.REPLICAS.get(this.env.REPLICAS.idFromName(peer.name), {
      locationHint: peer.locationHint,
    });
    let cursor = this.ctx.storage.sql
      .exec<{ cursor: number }>("SELECT cursor FROM peers WHERE name = ?", peer.name)
      .one().cursor;
    // Bound both fast tiny-row backlogs and slow responsive peers. Every pull
    // shares one timeout budget; completed batches keep their durable progress.
    const deadline = performance.now() + this.options.timeoutMs;
    let drained = 0;
    for (let pulls = 0; pulls < SYNC_ALARM_PULLS; pulls++) {
      const remaining = deadline - performance.now();
      if (remaining <= 0) break;
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const batch = await Promise.race([
          stub.pull(this.name, cursor),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error("Sync timeout")), remaining);
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
        cursor = batch.cursor;
        drained += batch.bytes;
        if (!batch.more || drained >= SYNC_ALARM_BYTES) break;
      } finally {
        clearTimeout(timer);
      }
    }
  }

  private async schedule() {
    const now = Date.now();
    const row = this.ctx.storage.sql.exec<{ next: number | null }>(SCHEDULE_EXPIRATION, now).one();
    const next = Math.min(now + this.options.syncMs, row.next ?? Infinity);
    const alarm = await this.ctx.storage.getAlarm();
    if (alarm === null || alarm > next) await this.ctx.storage.setAlarm(next);
  }

  async alarm(): Promise<void> {
    try {
      await Promise.allSettled(this.peers.map(peer => this.sync(peer)));
      const now = Date.now();
      this.ctx.storage.sql.exec(EXPIRE_RECORDS, now);
      this.ctx.storage.sql.exec(COLLECT_GARBAGE, now);
    } finally {
      await this.schedule();
    }
  }
}
