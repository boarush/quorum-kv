import { fileURLToPath, URL } from "node:url";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import type { Replica } from "quorum-kv/workers";

export const makeWorker = (name: string, options: { port?: number } = {}) =>
  Cloudflare.Worker(name, {
    main: fileURLToPath(new URL("./worker.ts", import.meta.url)),
    dev: { host: "127.0.0.1", port: options.port ?? Number(process.env.PORT ?? 31463) },
    compatibility: { date: "2026-11-01", flags: ["nodejs_compat"] },
    env: {
      REPLICAS: Cloudflare.DurableObject<Replica>("Replica"),
    },
  });

export default Alchemy.Stack(
  "quorum-kv",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const worker = yield* makeWorker("Api");
    return { worker };
  }),
);
