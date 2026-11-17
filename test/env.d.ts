import type { QuorumKVEnv as WorkerEnv } from "quorum-kv";

declare global {
  namespace Cloudflare {
    interface Env extends WorkerEnv {}
    interface GlobalProps {
      mainModule: typeof import("../examples/standalone/worker");
      durableNamespaces: "Replica";
    }
  }
}
