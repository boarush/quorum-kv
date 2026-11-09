import { env } from "cloudflare:workers";
import { createExecutionContext, waitOnExecutionContext } from "cloudflare:test";
import { expect, it } from "vitest";
import worker from "../src/index";
import type { Env } from "../src/config";

it("stores raw values in actual SQLite-backed Durable Objects", async () => {
  const ctx = createExecutionContext();
  const key = crypto.randomUUID();
  const put = await worker.fetch(new Request(`https://test/kv/${key}`, { method: "PUT", body: "hello" }), env as Env, ctx);
  expect(put.status).toBe(204);
  const get = await worker.fetch(new Request(`https://test/kv/${key}`), env as Env, ctx);
  expect(await get.text()).toBe("hello");
  await waitOnExecutionContext(ctx);
});
