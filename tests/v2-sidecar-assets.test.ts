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
    expect(first.files.map(file => file.path)).toContain(
      ".ghostprov-v2/sandbox/v2/main.ts",
    );
    expect(first.files.map(file => file.path)).toContain(
      ".ghostprov-v2/deno.json",
    );
  });

  it("maps every relative import to another tracked asset", async () => {
    const assets = await loadV2SidecarAssets();
    const paths = new Set(assets.files.map(file => file.path));
    for (const asset of assets.files.filter(file => file.path.endsWith(".ts"))) {
      const imports = [...asset.content.matchAll(/from\s+["'](\.{1,2}\/[^"']+)["']/g)];
      for (const match of imports) {
        const resolved = new URL(match[1], `file:///${asset.path}`).pathname.slice(1);
        const candidates = [resolved, `${resolved}.ts`, `${resolved}/index.ts`];
        expect(candidates.some(candidate => paths.has(candidate))).toBe(true);
      }
    }
  });

  it("pins offline dependencies and contains no credential literals", async () => {
    const assets = await loadV2SidecarAssets();
    const serialized = JSON.stringify(assets);
    const denoConfig = assets.files.find(file => file.path.endsWith("deno.json"))!;
    const main = assets.files.find(file => file.path.endsWith("sandbox/v2/main.ts"))!;

    expect(denoConfig.content).toContain('"fflate": "npm:fflate@0.8.2"');
    expect(main.content).toContain('npm:@y-sweet/client@0.9.1');
    expect(main.content).toContain('npm:yjs@13.6.27');
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
