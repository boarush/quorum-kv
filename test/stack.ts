import { randomInt } from "node:crypto";
import * as Alchemy from "alchemy";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { makeWorker } from "../alchemy.run";

// A separate stack name and local state keep tests away from production resources.
export default Alchemy.Stack(
  "quorum-kv-test",
  {
    providers: Cloudflare.providers(),
    state: Alchemy.localState(),
  },
  Effect.gen(function* () {
    const api = yield* makeWorker("Api", { port: randomInt(20_000, 60_000) });
    return { url: api.url };
  }),
);
