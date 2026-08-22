import { describe, expect, it } from "bun:test";
import {
  hashV2SidecarFiles,
  loadV2SidecarAssets,
} from "../src/v2/sidecar/assets";
import { protocolSchemaHash } from "../src/v2/protocol/shared";

describe("V2 sidecar assets", () => {
  it("produces a stable sorted build with the shared schema hash", async () => {
    const first = await loadV2SidecarAssets();
    const second = await loadV2SidecarAssets();

    expect(first).toEqual(second);
    expect(first.buildHash).toMatch(/^[a-f0-9]{64}$/);
    expect(first.schemaHash).toBe(await protocolSchemaHash());
    expect(first.files.map(file => file.path)).toEqual(
      [...first.files.map(file => file.path)].sort(),
    );
    expect(first.files.map(file => file.path)).toEqual([
      ".ghostprov-v2/sidecar.bundle.js",
    ]);
    expect((first as any).obsoletePaths).toEqual([
      ".ghostprov-v2/bootstrap.tex",
      ".ghostprov-v2/deno.json",
      ".ghostprov-v2/sandbox",
      ".ghostprov-v2/src",
    ]);
  });

  it("produces one executable bundle with no runtime package resolution", async () => {
    const assets = await loadV2SidecarAssets();
    const bundle = assets.files[0]!.content;

    expect(bundle.length).toBeGreaterThan(1_000);
    expect(bundle).toContain("Deno.");
    expect(bundle).not.toMatch(/\b(?:from|import)\s*\(?["'](?:npm:|https:)/);
    expect(bundle).not.toContain("registry.npmjs.org");
  });

  it("contains no credential literals or ZIP transport", async () => {
    const assets = await loadV2SidecarAssets();
    const serialized = JSON.stringify(assets);

    expect(serialized).not.toMatch(
      /sk-[A-Za-z0-9_-]{24,}|gAAAA[A-Za-z0-9_-]{24,}|eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}/,
    );
    expect(serialized).not.toContain("/zip");
  });

  it("detects source changes in the aggregate build hash", async () => {
    const assets = await loadV2SidecarAssets();
    const changed = assets.files.map((file, index) => (
      index === 0 ? { ...file, content: `${file.content}\n` } : file
    ));
    expect(await hashV2SidecarFiles(changed)).not.toBe(assets.buildHash);
  });
});
