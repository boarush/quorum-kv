import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      {
        plugins: [
          cloudflareTest({
            main: "./src/index.ts",
            miniflare: {
              compatibilityDate: "2026-11-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: { REPLICAS: { className: "Replica", useSQLite: true } },
              bindings: { SYNC_BATCH: "4" },
            },
          }),
        ],
        test: { name: "workers", include: ["test/kv.test.ts"], testTimeout: 30_000 },
      },
    ],
  },
});
