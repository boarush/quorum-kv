import { QuorumKV, type QuorumKVEnv } from "quorum-kv";

export { Replica } from "quorum-kv/workers";

export default {
  async fetch(_request: Request, env: QuorumKVEnv, ctx: ExecutionContext): Promise<Response> {
    // Construct per request so background work uses the current execution context.
    // Add your application's authentication/authorization before exposing stored data.
    const kv = new QuorumKV(env, ctx);
    const value = await kv.get("hello");
    return value === null ? new Response(null, { status: 404 }) : new Response(value);
  },
} satisfies ExportedHandler<QuorumKVEnv>;
