import { describe, expect, it } from "bun:test";
import {
  assertV2SidecarAssets,
  assertV2SidecarReleaseSchema,
  DEFAULT_V2_SIDECAR_REVISION,
  hashV2SidecarFiles,
  loadV2SidecarAssets,
  resolveV2SidecarRevision,
  V2_R1_RELEASE,
  V2_R2_RELEASE,
  V2_R3_RELEASE,
  V2_R4_RELEASE,
  V2_R5_RELEASE,
  V2_R6_RELEASE,
  V2_R7_RELEASE,
} from "../src/v2/sidecar/assets";
import { protocolSchemaHash } from "../src/v2/protocol/shared";

describe("V2 sidecar assets", () => {
  it("produces a stable sorted build with the frozen R1 identity", async () => {
    const first = await loadV2SidecarAssets({ revision: "r1" });
    const second = await loadV2SidecarAssets({ revision: "r1" });

    expect(first).toEqual(second);
    expect(first.revision).toBe("r1");
    expect(DEFAULT_V2_SIDECAR_REVISION).toBe("r7");
    expect(first.buildHash).toBe(V2_R1_RELEASE.buildHash);
    expect(first.schemaHash).toBe(V2_R1_RELEASE.schemaHash);
    expect(first.files[0]!.sha256).toBe(V2_R1_RELEASE.bundleSha256);
    expect(first.files.map(file => file.path)).toEqual(
      [...first.files.map(file => file.path)].sort(),
    );
    expect(first.files.map(file => file.logicalPath)).toEqual(["sidecar.bundle.js"]);
    expect(first.entrypointPath).toBe(first.files[0]!.path);
    expect(first.entrypointPath).toBe(
      `.ghostprov-v2/revisions/r1/${first.schemaHash}/${first.buildHash}/sidecar.bundle.js`,
    );
    expect(first.manifest.path).toBe(
      `.ghostprov-v2/revisions/r1/${first.schemaHash}/${first.buildHash}/manifest.json`,
    );
    expect(JSON.parse(first.manifest.content)).toMatchObject({
      format: "ghostprov-v2-sidecar-assets",
      version: 1,
      revision: "r1",
      buildHash: first.buildHash,
      schemaHash: first.schemaHash,
      entrypoint: "sidecar.bundle.js",
    });
    expect((first as any).obsoletePaths).toEqual([
      ".ghostprov-v2/bootstrap.tex",
      ".ghostprov-v2/deno.json",
      ".ghostprov-v2/sandbox",
      ".ghostprov-v2/sidecar.bundle.js",
      ".ghostprov-v2/src",
    ]);
    await expect(assertV2SidecarAssets(first, "r1")).resolves.toBeUndefined();
  });

  it("selects frozen R7 bulk transport while preserving prior releases", async () => {
    const assets = await loadV2SidecarAssets();
    expect(assets.revision).toBe("r7");
    expect(assets.protocolRevision).toBe("r2");
    expect(assets.transportProfile).toBe("bulk-v1");
    expect(assets.buildHash).toBe(V2_R7_RELEASE.buildHash);
    expect(assets.schemaHash).toBe(V2_R7_RELEASE.schemaHash);
    expect(assets.files[0]!.sha256).toBe(V2_R7_RELEASE.bundleSha256);
    await expect(assertV2SidecarAssets(assets, "r7")).resolves.toBeUndefined();
    await expect(loadV2SidecarAssets({ revision: "r6" }))
      .resolves.toMatchObject({
        revision: "r6",
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        buildHash: V2_R6_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r5" }))
      .resolves.toMatchObject({
        revision: "r5",
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        buildHash: V2_R5_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r4" }))
      .resolves.toMatchObject({
        revision: "r4",
        protocolRevision: "r2",
        transportProfile: "standard",
        buildHash: V2_R4_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r3" }))
      .resolves.toMatchObject({
        revision: "r3",
        protocolRevision: "r2",
        buildHash: V2_R3_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r2" }))
      .resolves.toMatchObject({
        revision: "r2",
        protocolRevision: "r2",
        buildHash: V2_R2_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r1" }))
      .resolves.toMatchObject({ revision: "r1", buildHash: V2_R1_RELEASE.buildHash });
  });

  it("fails closed when either pinned release schema differs from its live protocol", async () => {
    expect(() => assertV2SidecarReleaseSchema(
      V2_R1_RELEASE,
      "f".repeat(64),
    )).toThrow(/R1 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R2_RELEASE,
      "f".repeat(64),
    )).toThrow(/R2 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R3_RELEASE,
      "f".repeat(64),
    )).toThrow(/R3 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R4_RELEASE,
      "f".repeat(64),
    )).toThrow(/R4 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R5_RELEASE,
      "f".repeat(64),
    )).toThrow(/R5 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R6_RELEASE,
      "f".repeat(64),
    )).toThrow(/R6 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R7_RELEASE,
      "f".repeat(64),
    )).toThrow(/R7 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R1_RELEASE,
      V2_R1_RELEASE.schemaHash,
    )).not.toThrow();
    expect(V2_R1_RELEASE.schemaHash).toBe(await protocolSchemaHash("r1"));
    expect(V2_R2_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2"));
    expect(V2_R3_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2"));
    expect(V2_R4_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2"));
    expect(V2_R5_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2", "bulk-v1"));
    expect(V2_R6_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2", "bulk-v1"));
    expect(V2_R7_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2", "bulk-v1"));
  });

  it("produces one executable bundle with no runtime package resolution", async () => {
    const assets = await loadV2SidecarAssets({ revision: "r1" });
    const bundle = assets.files[0]!.content;

    expect(bundle.length).toBeGreaterThan(1_000);
    expect(bundle).toContain("Deno.");
    expect(bundle).not.toMatch(/\b(?:from|import)\s*\(?["'](?:npm:|https:)/);
    expect(bundle).not.toContain("registry.npmjs.org");
  });

  it("loads R1 only from its checked-in release artifact, never mutable current sources", async () => {
    const loaderSource = await Bun.file(
      `${import.meta.dir}/../src/v2/sidecar/assets.ts`,
    ).text();
    const releaseArtifact = await Bun.file(
      `${import.meta.dir}/../src/v2/sidecar/releases/r1/sidecar.bundle.js`,
    ).text();
    const before = await loadV2SidecarAssets({ revision: "r1" });

    // Structural guard: later R2/current-source work cannot silently become R1.
    expect(loaderSource).not.toContain("Bun.build");
    expect(loaderSource).not.toContain("sandbox/v2/main.ts");
    expect(before.files[0]!.content).toBe(releaseArtifact);

    // An arbitrary alternate current-source result has a different identity,
    // while a fresh R1 load remains pinned to the checked-in release.
    const alternate = [{
      ...before.files[0]!,
      content: `${before.files[0]!.content}\n// alternate current-source build\n`,
    }];
    expect(await hashV2SidecarFiles(alternate)).not.toBe(V2_R1_RELEASE.buildHash);
    await expect(loadV2SidecarAssets({ revision: "r1" })).resolves.toEqual(before);
  });

  it("contains no credential literals or ZIP transport", async () => {
    const assets = await loadV2SidecarAssets({ revision: "r1" });
    const serialized = JSON.stringify(assets);

    expect(serialized).not.toMatch(
      /sk-[A-Za-z0-9_-]{24,}|gAAAA[A-Za-z0-9_-]{24,}|eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}/,
    );
    expect(serialized).not.toContain("/zip");
  });

  it("detects source changes in the aggregate build hash", async () => {
    const assets = await loadV2SidecarAssets({ revision: "r1" });
    const changed = assets.files.map((file, index) => (
      index === 0 ? { ...file, content: `${file.content}\n` } : file
    ));
    expect(await hashV2SidecarFiles(changed)).not.toBe(assets.buildHash);
    await expect(assertV2SidecarAssets({ ...assets, files: changed }, "r1"))
      .rejects.toThrow(/content hash mismatch|aggregate build hash mismatch/i);
  });

  it("fails closed on unsupported or mismatched revision selection", async () => {
    expect(resolveV2SidecarRevision()).toBe("r7");
    expect(resolveV2SidecarRevision("r2")).toBe("r2");
    expect(resolveV2SidecarRevision("r3")).toBe("r3");
    expect(resolveV2SidecarRevision("r4")).toBe("r4");
    expect(resolveV2SidecarRevision("r5")).toBe("r5");
    expect(resolveV2SidecarRevision("r6")).toBe("r6");
    expect(resolveV2SidecarRevision("r7")).toBe("r7");
    expect(() => resolveV2SidecarRevision("r8")).toThrow(/unsupported.*r8/i);
    await expect(loadV2SidecarAssets({ revision: "r2" }))
      .resolves.toMatchObject({ revision: "r2", buildHash: V2_R2_RELEASE.buildHash });

    const assets = await loadV2SidecarAssets({ revision: "r1" });
    await expect(assertV2SidecarAssets({
      ...assets,
      revision: "r2",
    }, "r1")).rejects.toThrow(/revision mismatch/i);
  });
});
