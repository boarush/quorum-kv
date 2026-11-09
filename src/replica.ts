import { DurableObject } from "cloudflare:workers";
import type { Env } from "./config";
import { DEFAULTS, positiveInteger } from "./config";
import { compare, EMPTY, type RecordValue, type Version } from "./model";

type Row = {
  value: ArrayBuffer | null;
  counter: number;
  writer: string;
  expires_at: number | null;
  gc_at: number | null;
}

export class Replica extends DurableObject<Env> {
  private fault = { delayMs: 0, fail: false };
  private readonly grace: number;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.grace = positiveInteger(env.GC_GRACE_MS, DEFAULTS.gcGraceMs);
    ctx.storage.sql.exec(`CREATE TABLE IF NOT EXISTS records (
      key TEXT PRIMARY KEY, value BLOB, counter INTEGER NOT NULL,
      writer TEXT NOT NULL, expires_at REAL, gc_at REAL
    )`);
    ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS expiry ON records(expires_at)");
    ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS gc ON records(gc_at)");
  }

  private async before() {
    const { delayMs, fail } = this.fault;
    if (delayMs) await new Promise(resolve => setTimeout(resolve, delayMs));
    if (fail) throw new Error("Injected replica failure");
  }

  private stored(key: string): RecordValue {
    const row = this.ctx.storage.sql.exec<Row>(
      "SELECT value, counter, writer, expires_at, gc_at FROM records WHERE key = ?", key,
    ).toArray()[0];
    return row
      ? { value: row.value, version: { counter: row.counter, writer: row.writer }, expiresAt: row.expires_at }
      : EMPTY;
  }

  async readVersion(key: string): Promise<Version> {
    await this.before();
    return this.stored(key).version;
  }

  async read(key: string): Promise<RecordValue> {
    await this.before();
    return this.stored(key);
  }

  async write(key: string, record: RecordValue): Promise<void> {
    await this.before();
    // No await between compare and upsert: concurrent RPCs cannot regress a version.
    if (compare(record.version, this.stored(key).version) <= 0) return;
    const gcAt = record.expiresAt !== null
      ? record.expiresAt + this.grace
      : record.value === null ? Date.now() + this.grace : null;
    this.ctx.storage.sql.exec(
      `INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(key) DO UPDATE SET value=excluded.value, counter=excluded.counter,
       writer=excluded.writer, expires_at=excluded.expires_at, gc_at=excluded.gc_at`,
      key, record.value, record.version.counter, record.version.writer, record.expiresAt, gcAt,
    );
    await this.schedule();
  }

  private async schedule() {
    const row = this.ctx.storage.sql.exec<{ next: number | null }>(
      `SELECT MIN(deadline) AS next FROM (
         SELECT expires_at AS deadline FROM records WHERE value IS NOT NULL AND expires_at IS NOT NULL
         UNION ALL SELECT gc_at AS deadline FROM records WHERE gc_at IS NOT NULL
       )`,
    ).one();
    if (row.next === null) await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(Math.max(Date.now() + 1, row.next));
  }

  async alarm(): Promise<void> {
    const now = Date.now();
    // Drop expired payloads, but keep their versions until grace elapses.
    this.ctx.storage.sql.exec(
      "UPDATE records SET value = NULL WHERE expires_at <= ? AND value IS NOT NULL", now,
    );
    this.ctx.storage.sql.exec("DELETE FROM records WHERE gc_at <= ?", now);
    await this.schedule();
  }

  async setFault(fault: { delayMs?: number; fail?: boolean }): Promise<void> {
    if (this.env.TESTING !== "true") throw new Error("Fault injection is disabled");
    const delayMs = fault.delayMs ?? 0;
    if (!Number.isFinite(delayMs) || delayMs < 0 || delayMs > 60_000) throw new Error("Invalid delay");
    this.fault = { delayMs, fail: fault.fail ?? false };
  }
}
