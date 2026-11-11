import { Buffer } from "node:buffer";
import { config, type Env } from "./config";
import type { Replica } from "./replica";
import type { RecordValue } from "./model";
import { COLLECT_GARBAGE, EXPIRE_RECORDS, SCHEDULE_EXPIRATION } from "./statements";

export type ReplicaCommand =
  | { action: "read" | "readVersion"; key: string }
  | { action: "write"; key: string; record: RecordValue & { writtenAt: number } }
  | { action: "fault"; fault: { delayMs?: number; fail?: boolean } }
  | { action: "alarm" | "inspect" | "evict" | "expirationPlans" }
  | { action: "pull"; fromPeer: string; cursor: number }
  | { action: "age"; expiresAt?: number; gcAt?: number };

// Preserve binary records across the test-only JSON bridge.
export const stringify = (value: unknown) =>
  JSON.stringify(value, (_, v) =>
    v instanceof ArrayBuffer ? { $bytes: Buffer.from(v).toString("base64") } : v,
  );
export const parse = (value: string) =>
  JSON.parse(value, (_, v) =>
    v && typeof v === "object" && "$bytes" in v
      ? Uint8Array.from(Buffer.from(v.$bytes, "base64")).buffer
      : v,
  );

export async function replicaDebug(
  instance: Replica,
  state: DurableObjectState,
  command: ReplicaCommand,
) {
  switch (command.action) {
    case "read":
      return instance.read(command.key);
    case "readVersion":
      return instance.readVersion(command.key);
    case "write":
      return instance.write(command.key, command.record);
    case "fault":
      return instance.setFault(command.fault);
    case "pull":
      return instance.pull(command.fromPeer, command.cursor);
    case "expirationPlans": {
      const now = Date.now();
      // Exercise the exact statements used by Replica.
      const query = state.storage.sql.exec<{ next: number | null }>(SCHEDULE_EXPIRATION, now);
      return {
        next: query.one().next,
        rowsRead: query.rowsRead,
        schedule: state.storage.sql
          .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${SCHEDULE_EXPIRATION}`, now)
          .toArray(),
        cleanup: state.storage.sql
          .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${EXPIRE_RECORDS}`, now)
          .toArray(),
        collect: state.storage.sql
          .exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${COLLECT_GARBAGE}`, now)
          .toArray(),
      };
    }
    case "alarm": {
      const alarm = await state.storage.getAlarm();
      if (alarm === null) return false;
      // Match delivery: consume the pending alarm before invoking the handler.
      await state.storage.deleteAlarm();
      await instance.alarm();
      return true;
    }
    case "inspect":
      return {
        rows: state.storage.sql
          .exec<{
            key: string;
            gc_at: number | null;
            seq: number;
            counter: number;
          }>("SELECT key, gc_at, seq, counter FROM records")
          .toArray(),
        peers: state.storage.sql
          .exec<{ name: string; cursor: number; ack: number }>("SELECT * FROM peers")
          .toArray(),
        alarm: await state.storage.getAlarm(),
      };
    case "age":
      if (command.expiresAt !== undefined)
        state.storage.sql.exec("UPDATE records SET expires_at = ?", command.expiresAt);
      if (command.gcAt !== undefined)
        state.storage.sql.exec(
          "UPDATE records SET gc_at = ? WHERE gc_at IS NOT NULL",
          command.gcAt,
        );
      return;
    case "evict":
      await state.storage.sync();
      state.abort("Test eviction");
  }
}

export interface DebugRequest {
  path: string;
  drain?: boolean;
  method: string;
  chunks?: number[][];
  headers?: Record<string, string>;
  overrides?: Pick<Env, "REPLICA_TIMEOUT_MS" | "MAX_VALUE_BYTES">;
}

function noStore(body: BodyInit | null, init: ResponseInit = {}): Response {
  const headers = new Headers(init.headers);
  headers.set("Cache-Control", "no-store");
  return new Response(body, { ...init, headers });
}

export async function debugFetch(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  fetch: (request: Request, env: Env, ctx: ExecutionContext) => Promise<Response>,
): Promise<Response> {
  if (env.TESTING !== "true") return noStore("Not found", { status: 404 });
  const path = new URL(request.url).pathname;
  if (request.method !== "POST") return noStore("Not found", { status: 404 });
  try {
    if (path === "/_debug/request") {
      const input = await request.json<DebugRequest>();
      // Reconstruct streamed requests inside workerd, including a deliberately false Content-Length.
      const body =
        input.chunks === undefined
          ? undefined
          : new ReadableStream<Uint8Array>({
              start(controller) {
                for (const chunk of input.chunks!) controller.enqueue(new Uint8Array(chunk));
                controller.close();
              },
            });
      // Only coordinator settings are overridable, never TESTING or the DO namespace.
      const bindings = {
        ...env,
        REPLICA_TIMEOUT_MS: input.overrides?.REPLICA_TIMEOUT_MS ?? env.REPLICA_TIMEOUT_MS,
        MAX_VALUE_BYTES: input.overrides?.MAX_VALUE_BYTES ?? env.MAX_VALUE_BYTES,
      };
      if (input.path.startsWith("/_debug/")) {
        return noStore("Not found", { status: 404 });
      }
      const pending: Promise<unknown>[] = [];
      const response = await fetch(
        new Request(new URL(input.path, request.url), {
          method: input.method,
          headers: input.headers,
          body,
        }),
        bindings,
        {
          waitUntil(promise) {
            pending.push(promise);
            ctx.waitUntil(promise);
          },
        } as ExecutionContext,
      );
      if (input.drain !== false) await Promise.allSettled(pending);
      return response;
    }
    const match = /^\/_debug\/replica\/(shard-\d+-r([012]))$/.exec(path);
    if (!match) return noStore("Not found", { status: 404 });
    const stub = env.REPLICAS.get(env.REPLICAS.idFromName(match[1]!), {
      locationHint: config(env).hints[Number(match[2])],
    });
    const result = await stub.debug(parse(await request.text()) as ReplicaCommand);
    return noStore(stringify(result ?? null), { headers: { "Content-Type": "application/json" } });
  } catch (error) {
    // Test-only HTTP boundary: malformed input and injected RPC failures must
    // produce a non-success response, never fallback data or a successful parse.
    return noStore(error instanceof Error ? error.message : "Debug request failed", {
      status: 500,
    });
  }
}
