import { randomUUID } from "node:crypto";
import * as Test from "@alchemy.run/test/Vitest";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import { executeWhenReady } from "@alchemy.run/test/Http";
import { expect } from "vitest";
import Stack from "./stack";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  stage: process.env.ALCHEMY_TEST_STAGE ?? `test-${randomUUID().slice(0, 12)}`,
  dev: true,
});
const deployed = beforeAll(
  deploy(Stack).pipe(
    Effect.tap(({ url }) => {
      if (!url) return Effect.die(new Error("Test stack has no Worker URL"));
      // Retry only this startup probe, not requests whose failure statuses we assert.
      return executeWhenReady(
        HttpClientRequest.put(`${url}/kv/${randomUUID()}?ttl=60`).pipe(
          HttpClientRequest.bodyText("ready"),
        ),
      );
    }),
  ),
);
afterAll(destroy(Stack));
const request = (path: string, init?: RequestInit) =>
  fetch(`${Effect.runSync(deployed).url}${path}`, init);

test(
  "roundtrips bytes, empty values, deletion, and recreation through the deployed Worker",
  Effect.promise(async () => {
    const path = `/kv/${randomUUID()}`;
    const bytes = new Uint8Array([0, 255, 128, 13, 10]);
    expect((await request(path)).status).toBe(404);
    expect((await request(path, { method: "PUT", body: bytes })).status).toBe(204);
    const get = await request(path);
    expect(get.status).toBe(200);
    expect(new Uint8Array(await get.arrayBuffer())).toEqual(bytes);
    expect(get.headers.get("Cache-Control")).toBe("no-store");
    expect(get.headers.get("Content-Type")).toBe("application/octet-stream");
    expect((await request(path, { method: "PUT", body: "" })).status).toBe(204);
    expect(await (await request(path)).text()).toBe("");
    expect((await request(path, { method: "DELETE" })).status).toBe(204);
    expect((await request(path)).status).toBe(404);
    expect((await request(path, { method: "PUT", body: "recreated" })).status).toBe(204);
    expect(await (await request(path)).text()).toBe("recreated");
  }),
);

test(
  "validates public HTTP requests without any test bindings",
  Effect.promise(async () => {
    const path = `/kv/${randomUUID()}`;
    for (const ttl of ["0", "-1", "NaN", "Infinity", ""]) {
      expect((await request(`${path}?ttl=${ttl}`, { method: "PUT", body: "bad" })).status).toBe(
        400,
      );
    }
    expect((await request(path, { method: "POST" })).status).toBe(405);
    expect(
      (await request(path, { method: "PUT", body: new Uint8Array(1024 * 1024 + 1) })).status,
    ).toBe(413);
    expect((await request("/elsewhere")).status).toBe(404);
  }),
);

test(
  "has no debug or maintenance HTTP endpoints",
  Effect.promise(async () => {
    for (const path of ["/_debug/request", "/_debug/replica/shard-0-r0", "/debug/fault"]) {
      const response = await request(path, { method: "POST", body: "{}" });
      expect(response.status).toBe(404);
      expect(response.headers.get("Cache-Control")).toBe("no-store");
    }
  }),
);
