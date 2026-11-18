import { execFileSync, spawnSync } from "node:child_process";
import { copyFileSync, mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, sep } from "node:path";
import { fileURLToPath, URL } from "node:url";
import { build } from "esbuild";
import { Miniflare } from "miniflare";
import { it, expect } from "vitest";
import * as library from "quorum-kv";

const root = fileURLToPath(new URL("../", import.meta.url));

it("loads the coordinator without importing the Cloudflare runtime", () => {
  expect(new Set(Object.keys(library))).toEqual(new Set(["QuorumKV", "QuorumUnavailable"]));
  const error = new library.QuorumUnavailable();
  expect(error).toBeInstanceOf(Error);
  expect(error.code).toBe("QUORUM_UNAVAILABLE");
});

it("runs an installed tarball consumer with checked declarations and real SQLite DOs", async () => {
  const directory = mkdtempSync(join(tmpdir(), "quorum-kv-package-"));
  let runtime: Miniflare | undefined;
  try {
    const [packed] = JSON.parse(
      execFileSync("npm", ["pack", "--json", "--ignore-scripts", "--pack-destination", directory], {
        cwd: root,
        encoding: "utf8",
      }),
    ) as {
      filename: string;
      files: { path: string }[];
    }[];
    const files = packed!.files.map(file => file.path);
    expect(files).toContain("dist/index.js");
    expect(files).toContain("dist/index.d.ts");
    expect(files).toContain("dist/workers.js");
    expect(files).toContain("dist/workers.d.ts");
    expect(
      files.every(path => path.startsWith("dist/") || ["package.json", "README.md"].includes(path)),
    ).toBe(true);

    const consumer = join(directory, "consumer");
    mkdirSync(consumer);
    writeFileSync(
      join(consumer, "package.json"),
      JSON.stringify({ private: true, type: "module" }),
    );
    execFileSync(
      "npm",
      [
        "install",
        "--offline",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--package-lock=false",
        join(directory, packed!.filename),
      ],
      { cwd: consumer, stdio: "pipe" },
    );

    const output = execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `
      import { QuorumKV, QuorumUnavailable } from "quorum-kv";
      console.log(typeof QuorumKV, new QuorumUnavailable().code);
    `,
      ],
      { cwd: consumer, encoding: "utf8" },
    );
    expect(output.trim()).toBe("function QUORUM_UNAVAILABLE");

    writeFileSync(
      join(consumer, "consumer.ts"),
      `
      import { QuorumKV, QuorumUnavailable, type QuorumKVEnv, type ReplicaConfig,
        type PutOptions, type QuorumKVOptions, type Phase } from "quorum-kv";
      import { Api, Replica } from "quorum-kv/workers";
      // @ts-expect-error The file exists in the tarball but is not a public export path.
      import type { QuorumKV as DeepQuorumKV } from "quorum-kv/dist/index.js";
      export { Api, Replica };
      export async function use(env: QuorumKVEnv, ctx: ExecutionContext) {
        const settings: ReplicaConfig = { QKV_SHARDS: "1024" };
        const options: QuorumKVOptions = { onPhase: (phase: Phase) => console.log(phase.name) };
        const kv = new QuorumKV({ ...env, ...settings }, ctx, options);
        const methods: Record<keyof QuorumKV, true> = { get: true, put: true, delete: true };
        // @ts-expect-error Replica routing is not consumer API.
        kv.replicas("key");
        const put: PutOptions = { ttlSeconds: 60 };
        await kv.put("key", new ArrayBuffer(0), put);
        const value: ArrayBuffer | null = await kv.get("key");
        await kv.delete("key");
        return { value, methods, error: new QuorumUnavailable() };
      }
    `,
    );
    copyFileSync(join(root, "test/consumer/worker.ts"), join(consumer, "worker.ts"));
    writeFileSync(
      join(consumer, "tsconfig.json"),
      JSON.stringify({
        compilerOptions: {
          target: "ES2022",
          lib: ["ES2022"],
          module: "NodeNext",
          moduleResolution: "NodeNext",
          strict: true,
          noEmit: true,
          types: [],
          skipLibCheck: false,
        },
        files: [
          "consumer.ts",
          "worker.ts",
          join(root, "node_modules/@cloudflare/workers-types/index.d.ts"),
        ],
      }),
    );
    const typecheck = spawnSync(
      process.execPath,
      [join(root, "node_modules/typescript/bin/tsc"), "-p", consumer],
      {
        cwd: consumer,
        encoding: "utf8",
      },
    );
    expect(typecheck.status, typecheck.stdout + typecheck.stderr).toBe(0);

    const bundle = await build({
      absWorkingDir: consumer,
      entryPoints: ["worker.ts"],
      bundle: true,
      write: false,
      metafile: true,
      format: "esm",
      platform: "browser",
      target: "es2022",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*"],
    });
    // No checkout aliases or source fallback: all bundled inputs must come from the consumer.
    const inputs = Object.keys(bundle.metafile!.inputs).map(path => resolve(consumer, path));
    expect(inputs.every(path => path.startsWith(consumer + sep))).toBe(true);
    expect(inputs).toContain(join(consumer, "node_modules/quorum-kv/dist/index.js"));
    expect(inputs).toContain(join(consumer, "node_modules/quorum-kv/dist/workers.js"));

    runtime = new Miniflare({
      port: 0,
      cf: false,
      enableDevRegistry: false,
      telemetry: { enabled: false },
      logRequests: false,
      workers: [
        {
          config: {
            name: "consumer",
            compatibilityDate: "2026-11-01",
            manifest: {
              mainModule: "worker.js",
              modules: { "worker.js": { type: "esm", contents: bundle.outputFiles![0]!.text } },
            },
            env: {
              REPLICAS: { type: "durable-object", worker: "consumer", exportName: "Replica" },
              QKV_MAX_VALUE_BYTES: { type: "text", value: "8" },
            },
            exports: { Replica: { type: "durable-object", storage: "sqlite" } },
          },
        },
      ],
    });
    const url = "https://consumer.test/key";
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
    const raw = new Uint8Array([0, 255, 128, 13, 10]);
    expect((await runtime.dispatchFetch(url, { method: "PUT", body: raw })).status).toBe(204);
    const response = await runtime.dispatchFetch(url);
    expect(response.status).toBe(200);
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(raw);

    expect(
      (await runtime.dispatchFetch(url, { method: "PUT", body: new Uint8Array(8) })).status,
    ).toBe(204);
    expect((await (await runtime.dispatchFetch(url)).arrayBuffer()).byteLength).toBe(8);
    expect(
      (await runtime.dispatchFetch(url, { method: "PUT", body: new Uint8Array(9) })).status,
    ).toBe(400);
    expect(
      (await runtime.dispatchFetch(url, { method: "PUT", headers: { "x-ttl-seconds": "0" } }))
        .status,
    ).toBe(400);
    expect((await runtime.dispatchFetch("https://consumer.test/")).status).toBe(400);

    expect(
      (await runtime.dispatchFetch(url, { method: "PUT", body: new Uint8Array(0) })).status,
    ).toBe(204);
    const empty = await runtime.dispatchFetch(url);
    expect(empty.status).toBe(200);
    expect((await empty.arrayBuffer()).byteLength).toBe(0);
    expect((await runtime.dispatchFetch(url, { method: "DELETE" })).status).toBe(204);
    expect((await runtime.dispatchFetch(url)).status).toBe(404);
    expect((await runtime.dispatchFetch(url, { method: "DELETE" })).status).toBe(204);
  } finally {
    try {
      await runtime?.dispose();
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  }
}, 30_000);
