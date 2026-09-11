import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { buildR10Sidecar, checkR10Sidecar } from "../scripts/build_v2_sidecar_r10";
import { V2_R9_RELEASE, V2_R10_RELEASE, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY, V2_R10_C1091742_RETIREMENT_IDENTITY, loadV2SidecarAssets, loadV2SidecarRecoveryAssets } from "../src/v2/sidecar/assets";
import { isRegisteredR10Entrypoint, directSupervisorLaunchFiles, supervisorCleanupFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { safeV2ReleaseIdentity } from "../src/v2/release-observability";

const roots: string[] = [];
afterEach(async () => {
  for (const root of roots.splice(0)) {
    if (!path.resolve(root).startsWith(path.resolve(os.tmpdir()) + path.sep) || !path.basename(root).startsWith("ghostprov-r10-release-")) throw new Error("Unexpected release-test directory");
    await rm(root, { recursive: true, force: true });
  }
});

describe("R10 immutable release", () => {
  it("rebuilds the registered bundle and retains the prior R9 identity", async () => {
    const [built, assets, r9] = await Promise.all([buildR10Sidecar(), loadV2SidecarAssets({ revision: "r10" }), loadV2SidecarAssets({ revision: "r9" })]);
    expect(V2_R10_RELEASE).toEqual(built.identity);
    expect(assets.files[0]!.sha256).toBe(built.identity.bundleSha256);
    expect(r9.files[0]!.sha256).toBe(V2_R9_RELEASE.bundleSha256);
    const source = new TextDecoder().decode(built.bytes);
    expect(source).not.toContain("diagnostic_injected_failure");
    expect(source).not.toContain("diagnosticFailure");
    expect(source).not.toMatch(/(?:from|import)\s*\(?["'](?:https?:|npm:)/);
  });

  it("creates only the exact registered bytes and refuses a missing or modified artifact", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-r10-release-")); roots.push(root);
    const artifactPath = path.join(root, "sidecar.bundle.js");
    await expect(checkR10Sidecar({ artifactPath })).rejects.toThrow(/missing/);
    const created = await checkR10Sidecar({ write: true, artifactPath });
    expect((await readFile(artifactPath)).equals(Buffer.from(created.bytes))).toBe(true);
    await writeFile(artifactPath, "changed");
    await expect(checkR10Sidecar({ write: true, artifactPath })).rejects.toThrow(/immutable/);
    expect(await readFile(artifactPath, "utf8")).toBe("changed");
  });

  it("accepts only the registered entrypoint and exact release tuple", async () => {
    const assets = await loadV2SidecarAssets({ revision: "r10" });
    const good = { entrypointPath: assets.entrypointPath, supervisorNonce: "known-nonce" };
    expect(isRegisteredR10Entrypoint(good.entrypointPath)).toBe(true);
    expect(directSupervisorLaunchFiles(good).launcher).toContain("exec sh");
    for (const entrypointPath of [assets.entrypointPath + "\n", assets.entrypointPath + ".old", "prefix" + assets.entrypointPath,
      assets.entrypointPath.replace(assets.buildHash, V2_R9_RELEASE.buildHash), assets.entrypointPath.replace(assets.buildHash, "0".repeat(64))]) {
      expect(isRegisteredR10Entrypoint(entrypointPath)).toBe(false);
      expect(() => directSupervisorLaunchFiles({ ...good, entrypointPath })).toThrow();
    }
    const identity = { releaseRevision: "r10", protocolRevision: "r2", buildHash: assets.buildHash, schemaHash: assets.schemaHash } as const;
    expect(safeV2ReleaseIdentity(identity)).toEqual(identity);
    expect(safeV2ReleaseIdentity({ ...identity, buildHash: V2_R9_RELEASE.buildHash })).toBeNull();
  });

  it.each([
    ["initial", V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY],
    ["c1091742", V2_R10_C1091742_RETIREMENT_IDENTITY],
  ] as const)("allows archived R10 %s files only for exact cleanup reconstruction", async (_name, identity) => {
    const old = await loadV2SidecarRecoveryAssets(identity);
    const input = { entrypointPath: old.entrypointPath, supervisorNonce: "prior-nonce" };
    expect(supervisorCleanupFiles(input).installer).toContain(old.entrypointPath);
    if (old.buildHash !== V2_R10_RELEASE.buildHash) {
      expect(isRegisteredR10Entrypoint(old.entrypointPath)).toBe(false);
      expect(() => directSupervisorLaunchFiles(input)).toThrow();
    }
    for (const invalid of [old.entrypointPath + "\n", old.entrypointPath + ".old", "prefix" + old.entrypointPath,
      old.entrypointPath.replace(old.buildHash, "0".repeat(64)), old.entrypointPath.replace(old.schemaHash, V2_R9_RELEASE.buildHash)]) {
      expect(() => supervisorCleanupFiles({ ...input, entrypointPath: invalid })).toThrow();
    }
  });
});
