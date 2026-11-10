import { expect, it } from "vitest";
import { RecordCache } from "../src/cache";
import type { RecordValue } from "../src/model";

const record = (size = 10): RecordValue => ({
  value: new ArrayBuffer(size),
  version: { counter: 1, writer: "test" },
  expiresAt: null,
});

it("evicts the least recently used entry", () => {
  const cache = new RecordCache(200);
  cache.set("a", record());
  cache.set("b", record());
  expect(cache.get("a")).toBeDefined();
  cache.set("c", record());
  expect(cache.get("b")).toBeUndefined();
  expect(cache.get("a")).toBeDefined();
  expect(cache.get("c")).toBeDefined();
});

it("does not cache entries larger than the byte budget", () => {
  const cache = new RecordCache(200);
  cache.set("small", record(10));
  cache.set("large", record(200));
  expect(cache.get("small")).toBeDefined();
  expect(cache.get("large")).toBeUndefined();
  expect(cache.stats().bytes).toBe(84);
});

it("drops a previous entry when its replacement exceeds the byte budget", () => {
  const cache = new RecordCache(200);
  cache.set("a", record(10));
  cache.set("a", record(200));
  expect(cache.get("a")).toBeUndefined();
  expect(cache.stats().entries).toBe(0);
});

it("disables itself at zero bytes and clears entries", () => {
  const disabled = new RecordCache(0);
  disabled.set("a", record());
  expect(disabled.get("a")).toBeUndefined();
  expect(disabled.stats().entries).toBe(0);

  const cache = new RecordCache(200);
  cache.set("a", record());
  cache.clear();
  expect(cache.get("a")).toBeUndefined();
  expect(cache.stats().entries).toBe(0);
});
