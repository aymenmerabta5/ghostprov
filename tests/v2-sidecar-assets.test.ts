import { describe, expect, it } from "bun:test";
import {
  assertV2SidecarAssets,
  assertV2SidecarReleaseSchema,
  DEFAULT_V2_SIDECAR_REVISION,
  hashV2SidecarFiles,
  loadV2SidecarAssets,
  resolveV2SidecarRevision,
  V2_R1_RELEASE,
  V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY,
  V2_R2_RELEASE,
  V2_R3_RELEASE,
  V2_R4_RELEASE,
  V2_R5_RELEASE,
  V2_R6_RELEASE,
  V2_R7_RELEASE,
  V2_R8_RELEASE,
  V2_R9_RELEASE,
  V2_RETIREMENT_RELEASE_IDENTITIES,
  V2_SELECTABLE_RELEASE_IDENTITIES,
} from "../src/v2/sidecar/assets";
import { protocolSchemaHash } from "../src/v2/protocol/shared";

describe("V2 sidecar assets", () => {
  it("produces a stable sorted build with the frozen R1 identity", async () => {
    const first = await loadV2SidecarAssets({ revision: "r1" });
    const second = await loadV2SidecarAssets({ revision: "r1" });

    expect(first).toEqual(second);
    expect(first.revision).toBe("r1");
    expect(DEFAULT_V2_SIDECAR_REVISION).toBe("r9");
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

  it("selects sandbox-relay R9 bulk transport while preserving prior releases", async () => {
    const assets = await loadV2SidecarAssets();
    expect(assets.revision).toBe("r9");
    expect(assets.protocolRevision).toBe("r2");
    expect(assets.transportProfile).toBe("bulk-v1");
    expect(assets.buildHash).toBe(V2_R9_RELEASE.buildHash);
    expect(assets.schemaHash).toBe(V2_R9_RELEASE.schemaHash);
    expect(assets.files[0]!.sha256).toBe(V2_R9_RELEASE.bundleSha256);
    await expect(assertV2SidecarAssets(assets, "r9")).resolves.toBeUndefined();
    await expect(loadV2SidecarAssets({ revision: "r8" }))
      .resolves.toMatchObject({
        revision: "r8",
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        buildHash: V2_R8_RELEASE.buildHash,
      });
    await expect(loadV2SidecarAssets({ revision: "r7" }))
      .resolves.toMatchObject({
        revision: "r7",
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        buildHash: V2_R7_RELEASE.buildHash,
      });
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
      V2_R8_RELEASE,
      "f".repeat(64),
    )).toThrow(/R8 schema hash.*live protocol/i);
    expect(() => assertV2SidecarReleaseSchema(
      V2_R9_RELEASE,
      "f".repeat(64),
    )).toThrow(/R9 schema hash.*live protocol/i);
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
    expect(V2_R8_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2", "bulk-v1"));
    expect(V2_R9_RELEASE.schemaHash).toBe(await protocolSchemaHash("r2", "bulk-v1"));
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
    expect(resolveV2SidecarRevision()).toBe("r9");
    expect(resolveV2SidecarRevision("r2")).toBe("r2");
    expect(resolveV2SidecarRevision("r3")).toBe("r3");
    expect(resolveV2SidecarRevision("r4")).toBe("r4");
    expect(resolveV2SidecarRevision("r5")).toBe("r5");
    expect(resolveV2SidecarRevision("r6")).toBe("r6");
    expect(resolveV2SidecarRevision("r7")).toBe("r7");
    expect(resolveV2SidecarRevision("r8")).toBe("r8");
    expect(resolveV2SidecarRevision("r9")).toBe("r9");
    expect(() => resolveV2SidecarRevision("r10")).toThrow(/unsupported.*r10/i);
    await expect(loadV2SidecarAssets({ revision: "r2" }))
      .resolves.toMatchObject({ revision: "r2", buildHash: V2_R2_RELEASE.buildHash });

    const assets = await loadV2SidecarAssets({ revision: "r1" });
    await expect(assertV2SidecarAssets({
      ...assets,
      revision: "r2",
    }, "r1")).rejects.toThrow(/revision mismatch/i);
  });

  it("keeps the original R2 identity retirement-only", async () => {
    expect(V2_RETIREMENT_RELEASE_IDENTITIES).toContain(
      V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY,
    );
    expect(V2_SELECTABLE_RELEASE_IDENTITIES).not.toContain(
      V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY,
    );
    expect(V2_R2_INITIAL_RELEASE_RETIREMENT_IDENTITY).toMatchObject({
      revision: "r2",
      protocolRevision: "r2",
      buildHash: "21457e48716e99e82d48521ca43c60335ffd9933f4e1e258e252709c9525da6a",
      bundleSha256: "fd76939a0e53638737aa60e4ea13ba27a870f8938ff20bf74354bd8efe87ef73",
    });
    await expect(loadV2SidecarAssets({ revision: "r2" }))
      .resolves.toMatchObject({ buildHash: V2_R2_RELEASE.buildHash });
  });

  it("exposes the authorized R7 to R8 markers across exact frozen identities", async () => {
    const [r7, r8] = await Promise.all([
      loadV2SidecarAssets({ revision: "r7" }),
      loadV2SidecarAssets({ revision: "r8" }),
    ]);
    const r7Bundle = r7.files[0]!.content;
    const r8Bundle = r8.files[0]!.content;

    // loadV2SidecarAssets has already enforced each complete immutable bundle
    // hash. These assertions name the reviewed behavioral markers; they are not
    // a substitute for the exact full-artifact identity check above.
    expect(r7Bundle).toContain('failure.code==="invalid_json"');
    expect(r7Bundle).toContain("validatedImmutableJsonText(input.body)");
    expect(r8Bundle).not.toContain('failure.code==="invalid_json"');
    expect(r8Bundle).not.toContain("validatedImmutableJsonText(input.body)");
    expect(r7Bundle).not.toContain("function normalizeUpstreamResponsesRequest");
    expect(r8Bundle).toContain("function normalizeUpstreamResponsesRequest");
    expect(r8Bundle).toContain("metadata?.stream===!0&&metadata.store===!0");
  });

  it("ships the R9 sandbox-relay credential window on the R8 wire protocol", async () => {
    const [r8, r9] = await Promise.all([
      loadV2SidecarAssets({ revision: "r8" }),
      loadV2SidecarAssets({ revision: "r9" }),
    ]);
    expect(r9.revision).toBe("r9");
    expect(r9.protocolRevision).toBe("r2");
    expect(r9.transportProfile).toBe("bulk-v1");
    // Same wire schema as R8: only the host-configured bootstrap window differs.
    expect(r9.schemaHash).toBe(r8.schemaHash);
    expect(r9.buildHash).not.toBe(r8.buildHash);
    expect(r9.entrypointPath).toBe(
      `.ghostprov-v2/revisions/r9/${r9.schemaHash}/${r9.buildHash}/sidecar.bundle.js`,
    );
    const r9Bundle = r9.files[0]!.content;
    expect(r9Bundle).toContain("function normalizeUpstreamResponsesRequest");
    expect(r9Bundle).not.toContain('failure.code==="invalid_json"');
    expect(r9Bundle).toContain("bootstrapTimeoutMs");
  });
});
