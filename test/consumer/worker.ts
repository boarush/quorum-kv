// Copied outside the checkout and bundled against an installed tarball by package.test.ts.
import { QuorumKV, QuorumUnavailable, type QuorumKVEnv, type PutOptions } from "quorum-kv";

export { Replica } from "quorum-kv/workers";

export default {
  async fetch(request, env, ctx) {
    const kv = new QuorumKV(env, ctx);
    const key = decodeURIComponent(new URL(request.url).pathname.slice(1));
    try {
      switch (request.method) {
        case "GET": {
          const value = await kv.get(key);
          return value === null ? new Response(null, { status: 404 }) : new Response(value);
        }
        case "PUT": {
          const ttl = request.headers.get("x-ttl-seconds");
          const options: PutOptions = ttl === null ? {} : { ttlSeconds: Number(ttl) };
          await kv.put(key, await request.arrayBuffer(), options);
          return new Response(null, { status: 204 });
        }
        case "DELETE":
          await kv.delete(key);
          return new Response(null, { status: 204 });
        default:
          return new Response(null, { status: 405 });
      }
    } catch (error) {
      if (error instanceof RangeError) return new Response(error.message, { status: 400 });
      if (error instanceof QuorumUnavailable) return new Response(error.code, { status: 503 });
      throw error;
    }
  },
} satisfies ExportedHandler<QuorumKVEnv>;
