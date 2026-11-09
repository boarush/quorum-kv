import type { Env } from "./config";
import { config } from "./config";
import { QuorumKV, QuorumUnavailable } from "./coordinator";
export { Replica } from "./replica";

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    if (!url.pathname.startsWith("/kv/")) return new Response("Not found", { status: 404 });
    let key: string;
    try {
      key = decodeURIComponent(url.pathname.slice(4));
    } catch {
      return new Response("Invalid key encoding", { status: 400 });
    }
    if (!key || new TextEncoder().encode(key).byteLength > 1024) {
      return new Response("Key must contain 1–1024 UTF-8 bytes", { status: 400 });
    }
    if (!["GET", "PUT", "DELETE"].includes(request.method)) {
      return new Response("Method not allowed", { status: 405, headers: { Allow: "GET, PUT, DELETE" } });
    }
    const kv = new QuorumKV(env, ctx);
    try {
      let response: Response;
      switch (request.method) {
        case "GET": {
          const value = await kv.get(key);
          response = value === null
            ? new Response("Not found", { status: 404 })
            : new Response(value, { headers: { "Content-Type": "application/octet-stream" } });
          break;
        }
        case "PUT": {
          // Bound buffered values before sending three copies across regions.
          const maxValueBytes = config(env).maxValueBytes;
          const reader = request.body?.getReader();
          const chunks: Uint8Array[] = [];
          let length = 0;
          if (reader) {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              length += value.byteLength;
              if (length > maxValueBytes) {
                await reader.cancel();
                return new Response(`Value exceeds ${maxValueBytes} bytes`, { status: 413 });
              }
              chunks.push(value);
            }
          }
          const value = new Uint8Array(length);
          let offset = 0;
          for (const chunk of chunks) { value.set(chunk, offset); offset += chunk.byteLength; }
          const ttl = url.searchParams.get("ttl");
          await kv.put(key, value.buffer, { ttlSeconds: ttl === null ? undefined : Number(ttl) });
          response = new Response(null, { status: 204 });
          break;
        }
        default:
          await kv.delete(key);
          response = new Response(null, { status: 204 });
      }
      response.headers.set("Cache-Control", "no-store");
      if (request.headers.get("X-KV-Debug") === "1") {
        response.headers.set("X-KV-Quorum", JSON.stringify(kv.phases));
      }
      return response;
    } catch (error) {
      if (error instanceof RangeError) return new Response(error.message, { status: 400 });
      if (error instanceof QuorumUnavailable) return new Response(error.message, { status: 503 });
      throw error;
    }
  },
} satisfies ExportedHandler<Env>;
