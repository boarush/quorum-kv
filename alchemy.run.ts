import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import type { Replica } from "./src/replica";

export default Alchemy.Stack("quorum-kv", {
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
}, Effect.gen(function* () {
  const worker = yield* Cloudflare.Worker("Api", {
    main: "./src/index.ts",
    compatibility: { date: "2026-11-09", flags: ["nodejs_compat"] },
    env: { REPLICAS: Cloudflare.DurableObject<Replica>("Replica") },
  });
  return { url: worker.url };
}));
