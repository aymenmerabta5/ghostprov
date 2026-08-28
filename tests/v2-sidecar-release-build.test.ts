import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import {
  V2_R7_SIDECAR_BUILD_RECIPE,
  buildV2SidecarRelease,
  canonicalV2SidecarBundle,
  checkV2SidecarRelease,
  main,
  v2SidecarReleaseMode,
} from "../scripts/build_v2_sidecar_release";

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

describe("V2-R7 sidecar release build", () => {
  it("pins the browser ESM recipe without chunks, source maps, or identifier mangling", () => {
    expect(V2_R7_SIDECAR_BUILD_RECIPE).toEqual({
      target: "browser",
      format: "esm",
      splitting: false,
      sourcemap: "none",
      external: ["node:*"],
      minify: {
        syntax: true,
        whitespace: true,
        identifiers: false,
      },
    });
    expect(Object.isFrozen(V2_R7_SIDECAR_BUILD_RECIPE)).toBe(true);
    expect(Object.isFrozen(V2_R7_SIDECAR_BUILD_RECIPE.minify)).toBe(true);
    expect(Object.isFrozen(V2_R7_SIDECAR_BUILD_RECIPE.external)).toBe(true);
  });

  it("canonicalizes platform line endings to exactly one trailing LF", () => {
    expect(canonicalV2SidecarBundle("one\r\ntwo\r\n\r\n"))
      .toBe("one\ntwo\n");
    expect(canonicalV2SidecarBundle("one\ntwo")).toBe("one\ntwo\n");
  });

  it("builds twice to identical safe bytes while preserving identifiers", async () => {
    const built = await buildV2SidecarRelease();
    expect(built.revision).toBe("r7");
    expect(built.protocolRevision).toBe("r2");
    expect(built.bytes.byteLength).toBeGreaterThan(1_000);
    expect(built.content).toContain("DenoJournalStorage");
    expect(built.content).toContain("Deno.");
    expect(built.content).toContain('from"node:crypto"');
    expect(built.content).not.toContain("crypto-browserify");
    expect(built.content).not.toContain("\r");
    expect(built.content.endsWith("\n")).toBe(true);
    expect(built.content.endsWith("\n\n")).toBe(false);
    expect(built.content).not.toContain("sourceMappingURL=");
    expect(built.bundleSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(built.buildHash).toMatch(/^[a-f0-9]{64}$/);
    expect(built.schemaHash).toMatch(/^[a-f0-9]{64}$/);
  });

  it("makes --check fail closed for missing or corrupt release bytes", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-r7-release-"));
    roots.push(root);
    const missing = path.join(root, "missing.bundle.js");
    await expect(checkV2SidecarRelease(missing)).rejects.toThrow(/artifact is missing/i);

    const corrupt = path.join(root, "corrupt.bundle.js");
    await writeFile(corrupt, "not the deterministic R7 bundle\n");
    await expect(checkV2SidecarRelease(corrupt)).rejects.toThrow(/differs/i);
    await expect(main(["--unknown"])).rejects.toThrow(/usage/i);
  });

  it("uses source-vs-artifact verification as the safe default command", () => {
    expect(v2SidecarReleaseMode([])).toBe("check");
    expect(v2SidecarReleaseMode(["--check"])).toBe("check");
    expect(v2SidecarReleaseMode(["--write"])).toBe("write");
    expect(() => v2SidecarReleaseMode(["--write", "extra"])).toThrow(/usage/i);
  });

  it("publishes separate safe-default release and frozen-bundle Deno gates", async () => {
    const manifest = JSON.parse(await readFile(
      path.resolve(import.meta.dir, "../package.json"),
      "utf8",
    ));
    expect(manifest.scripts["check:v2-sidecar-release"])
      .toBe("bun scripts/build_v2_sidecar_release.ts");
    expect(manifest.scripts["build:v2-sidecar-release"])
      .toContain("--write");
    expect(manifest.scripts["check:v2-sidecar-deno"])
      .toContain("deno@2.9.5 check --no-config --no-remote --no-npm --no-lock");
  });

  it(
    "matches the checked-in frozen R7 artifact and pinned identity to current source",
    async () => {
      await expect(checkV2SidecarRelease()).resolves.toMatchObject({
        revision: "r7",
        protocolRevision: "r2",
      });
    },
  );
});
