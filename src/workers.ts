import { WorkerEntrypoint } from "cloudflare:workers";
import type { QuorumKVEnv } from "./config";
import { QuorumKV, type PutOptions } from "./coordinator";

export { Replica } from "./replica";

/** Optional service-binding adapter. Embedded consumers only need Replica. */
export class Api extends WorkerEntrypoint<QuorumKVEnv> {
  get(key: string): Promise<ArrayBuffer | null> {
    return new QuorumKV(this.env, this.ctx).get(key);
  }

  put(key: string, value: ArrayBuffer, options: PutOptions = {}): Promise<void> {
    return new QuorumKV(this.env, this.ctx).put(key, value, options);
  }

  delete(key: string): Promise<void> {
    return new QuorumKV(this.env, this.ctx).delete(key);
  }
}
