import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import type { Replica } from "./src/replica";

export const makeWorker = (name: string, options: { port?: number } = {}) =>
  Cloudflare.Worker(name, {
    main: "./src/index.ts",
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
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const worker = yield* makeWorker("Api");
    return { url: worker.url };
  }),
);
