// Expiration statements shared by the replica and the query-plan debug harness.
// The partial index on records(expires_at) only serves these when filtered by value.
export const SCHEDULE_EXPIRATION =
  "SELECT MIN(expires_at) AS next FROM records WHERE value IS NOT NULL AND expires_at > ?";

export const EXPIRE_RECORDS =
  "UPDATE records SET value = NULL WHERE expires_at <= ? AND value IS NOT NULL";

// The partial index on records(gc_at) keeps alarm GC off a full table scan.
export const COLLECT_GARBAGE =
  "DELETE FROM records WHERE gc_at <= ? AND seq <= (SELECT MIN(ack) FROM peers)";
