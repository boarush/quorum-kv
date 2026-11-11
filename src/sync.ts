// Leave ample headroom for concurrent pulls, RPC serialization, and SQLite buffers.
export const SYNC_BATCH_BYTES = 4 * 1024 * 1024;

// Bound on byte-capped batches drained per peer per alarm. Large-value backlogs
// advance many rows per interval instead of one, without unbounded alarm work.
export const SYNC_ALARM_BYTES = 16 * 1024 * 1024;

// Budget for a payload copy, UTF-16 strings, and per-record RPC metadata.
export const syncRowBytes = (key: string, writer: string, value: ArrayBuffer | null) =>
  2 * (value?.byteLength ?? 0) + 2 * (key.length + writer.length) + 256;
