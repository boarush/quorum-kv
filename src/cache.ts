import type { RecordValue } from "./model";

interface CacheEntry {
  record: RecordValue;
  bytes: number;
}

const ENTRY_OVERHEAD_BYTES = 64;

export class RecordCache {
  private readonly entries = new Map<string, CacheEntry>();
  private usedBytes = 0;
  private hits = 0;
  private misses = 0;

  constructor(private readonly maxBytes: number) {}

  get(key: string): RecordValue | undefined {
    if (this.maxBytes <= 0) return undefined;
    const entry = this.entries.get(key);
    if (entry === undefined) {
      this.misses++;
      return undefined;
    }
    this.hits++;
    // Map preserves insertion order; re-insert to mark this entry as most recently used.
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.record;
  }

  set(key: string, record: RecordValue): void {
    if (this.maxBytes <= 0) return;
    const bytes = ENTRY_OVERHEAD_BYTES + key.length * 2 + (record.value?.byteLength ?? 0);
    if (bytes > this.maxBytes) {
      // An updated value may no longer fit; never leave the previous value behind.
      this.delete(key);
      return;
    }
    const previous = this.entries.get(key);
    if (previous !== undefined) {
      this.usedBytes -= previous.bytes;
      this.entries.delete(key);
    }
    this.entries.set(key, { record, bytes });
    this.usedBytes += bytes;
    while (this.usedBytes > this.maxBytes) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      const entry = this.entries.get(oldest);
      if (entry === undefined) break;
      this.entries.delete(oldest);
      this.usedBytes -= entry.bytes;
    }
  }

  delete(key: string): void {
    const entry = this.entries.get(key);
    if (entry === undefined) return;
    this.entries.delete(key);
    this.usedBytes -= entry.bytes;
  }

  clear(): void {
    this.entries.clear();
    this.usedBytes = 0;
  }

  stats() {
    return {
      hits: this.hits,
      misses: this.misses,
      entries: this.entries.size,
      bytes: this.usedBytes,
    };
  }
}
