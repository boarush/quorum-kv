import { WorkerEntrypoint } from "cloudflare:workers";
import type { Env } from "./config";
import { QuorumKV } from "./coordinator";
export { Replica } from "./replica";

/**
 * Service-binding entrypoint for quorum-kv.
 *
 * Bind it from another Worker with
 * `Cloudflare.WorkerEntrypoint<Api>(worker, "Api")`, then call the methods
 * directly. There is no public HTTP surface.
 */
export class Api extends WorkerEntrypoint<Env> {
  get(key: string): Promise<ArrayBuffer | null> {
    return new QuorumKV(this.env, this.ctx).get(key);
  }

  put(key: string, value: ArrayBuffer, options: { ttlSeconds?: number } = {}): Promise<void> {
    return new QuorumKV(this.env, this.ctx).put(key, value, options);
  }

  delete(key: string): Promise<void> {
    return new QuorumKV(this.env, this.ctx).delete(key);
  }
}
