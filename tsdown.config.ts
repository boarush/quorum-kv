import { defineConfig } from "tsdown";

export default defineConfig({
  entry: { index: "src/index.ts", workers: "src/workers.ts" },
  format: "esm",
  platform: "neutral",
  target: "es2022",
  tsconfig: "tsconfig.build.json",
  dts: true,
  fixedExtension: false,
  deps: { neverBundle: ["cloudflare:workers"] },
});
