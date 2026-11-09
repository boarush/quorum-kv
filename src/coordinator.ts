import type { Env } from "./config";
import { config } from "./config";
import { compare, type RecordValue } from "./model";
import type { Replica } from "./replica";

export class QuorumUnavailable extends Error {
  constructor() { super("Replica quorum unavailable"); }
}

export interface Answer {
  replica: string;
  latencyMs: number;
}

export interface Phase {
  name: string;
  answers: Answer[];
}

export async function shardFor(key: string, shards: number): Promise<number> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key));
  return new DataView(digest).getUint32(0) % shards;
}

export class QuorumKV {
  private readonly options;
  readonly phases: Phase[] = [];

  constructor(
    private readonly env: Env,
    private readonly ctx: Pick<ExecutionContext, "waitUntil">,
  ) {
    this.options = config(env);
  }

  async replicas(key: string) {
    const shard = await shardFor(key, this.options.shards);
    return this.options.hints.map((locationHint, i) => {
      const name = `shard-${shard}-r${i}`;
      return {
        name,
        stub: this.env.REPLICAS.get(this.env.REPLICAS.idFromName(name), { locationHint }),
      };
    });
  }

  private async quorum<T>(
    key: string, name: string, call: (stub: DurableObjectStub<Replica>) => Promise<T>,
  ): Promise<T[]> {
    const replicas = await this.replicas(key);
    const answers: Answer[] = [];
    const values: T[] = [];
    this.phases.push({ name, answers });
    let failures = 0;

    const attempts = replicas.map(async ({ name, stub }) => {
      const start = Date.now();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const value = await Promise.race([
          Promise.resolve().then(() => call(stub)),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error("Replica timeout")), this.options.timeoutMs);
          }),
        ]);
        return { value, answer: { replica: name, latencyMs: Date.now() - start } };
      } finally {
        clearTimeout(timer);
      }
    });
    // Keep the third request alive after returning a quorum, without delaying the response.
    this.ctx.waitUntil(Promise.allSettled(attempts).then(() => undefined));
    return new Promise<T[]>((resolve, reject) => {
      for (const attempt of attempts) {
        void attempt.then(result => {
          if (values.length === 2 || failures === 2) return;
          values.push(result.value);
          answers.push(result.answer);
          if (values.length === 2) resolve(values);
        }, () => {
          if (++failures === 2 && values.length < 2) reject(new QuorumUnavailable());
        });
      }
    });
  }

  async get(key: string): Promise<ArrayBuffer | null> {
    const records = await this.quorum(key, "read", stub => stub.read(key));
    const first = records[0]!;
    const second = records[1]!;
    const order = compare(first.version, second.version);
    const newest = order >= 0 ? first : second;
    if (order !== 0) {
      await this.quorum(key, "repair", stub => stub.write(key, { ...newest, writtenAt: Date.now() }));
    }
    return newest.expiresAt !== null && newest.expiresAt <= Date.now() ? null : newest.value;
  }

  async put(key: string, value: ArrayBuffer, options: { ttlSeconds?: number } = {}): Promise<void> {
    const ttl = options.ttlSeconds;
    if (ttl !== undefined && (!Number.isFinite(ttl) || ttl <= 0 ||
      !Number.isSafeInteger(Math.ceil(Date.now() + ttl * 1000 + this.options.gcGraceMs)))) {
      throw new RangeError("ttlSeconds must be a positive finite duration");
    }
    await this.store(key, value, ttl === undefined ? null : Date.now() + ttl * 1000);
  }

  async delete(key: string): Promise<void> {
    await this.store(key, null, null);
  }

  private async store(key: string, value: ArrayBuffer | null, expiresAt: number | null) {
    const versions = await this.quorum(key, "version", stub => stub.readVersion(key));
    const counter = Math.max(...versions.map(version => version.counter)) + 1;
    if (!Number.isSafeInteger(counter)) throw new QuorumUnavailable();
    const record: RecordValue = { value, version: { counter, writer: crypto.randomUUID() }, expiresAt };
    await this.quorum(key, "write", stub => stub.write(key, { ...record, writtenAt: Date.now() }));
  }
}
