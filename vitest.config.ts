import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      main: "./src/index.ts",
      miniflare: {
        compatibilityDate: "2026-11-01",
        compatibilityFlags: ["nodejs_compat"],
        durableObjects: { REPLICAS: { className: "Replica", useSQLite: true } },
        bindings: { TESTING: "true", SYNC_BATCH: "4" },
      },
    }),
  ],
});
