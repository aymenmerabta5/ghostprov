import { mkdir, readFile, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { assertReleaseBundle, canonicalV2SidecarBundle, V2_R8_SIDECAR_BUILD_RECIPE } from "./build_v2_sidecar_release";
import { protocolSchemaHash, sha256Hex, utf8Bytes } from "../src/v2/protocol/shared";
import { loadV2SidecarAssets, r10SidecarArtifactPath } from "../src/v2/sidecar/assets";

const ROOT = path.resolve(import.meta.dir, "..");
export const R10_ARTIFACT = r10SidecarArtifactPath();

/** Build without writing or changing any immutable release registration. */
export async function buildR10Sidecar() {
  const once = async () => {
    const result = await Bun.build({ entrypoints: [path.join(ROOT, "sandbox/v2/r10-main.ts")],
      ...V2_R8_SIDECAR_BUILD_RECIPE, external: [...V2_R8_SIDECAR_BUILD_RECIPE.external] });
    if (!result.success || result.outputs.length !== 1 || result.logs.length) throw new Error("R10 sidecar build failed");
    const content = canonicalV2SidecarBundle(await result.outputs[0]!.text());
    assertReleaseBundle(content, "V2-R10");
    return content;
  };
  const content = await once();
  if (content !== await once()) throw new Error("R10 sidecar build is not deterministic");
  const bytes = utf8Bytes(content);
  const identity = Object.freeze({ revision: "r10" as const, protocolRevision: "r2" as const, transportProfile: "bulk-v1" as const,
    buildHash: await sha256Hex(utf8Bytes(`sidecar.bundle.js\0${content}`)),
    schemaHash: await protocolSchemaHash("r2", "bulk-v1"), bundleSha256: await sha256Hex(bytes) });
  return { bytes, identity };
}

export async function checkR10Sidecar(options: { write?: boolean; artifactPath?: string } = {}) {
  const built = await buildR10Sidecar();
  const { V2_R10_RELEASE } = await import("../src/v2/sidecar/assets");
  for (const key of Object.keys(built.identity) as Array<keyof typeof built.identity>) {
    if (built.identity[key] !== V2_R10_RELEASE[key]) throw new Error(`Frozen R10 identity differs from the rebuilt source: ${JSON.stringify(built.identity)}`);
  }
  const artifact = options.artifactPath ?? R10_ARTIFACT;
  const expected = Buffer.from(built.bytes);
  let existing: Buffer | null = null;
  try { existing = await readFile(artifact); }
  catch (error) { if (!error || typeof error !== "object" || !("code" in error) || error.code !== "ENOENT") throw error; }
  if (existing) {
    if (!existing.equals(expected)) throw new Error("Refusing to replace immutable R10 artifact bytes");
  } else if (options.write) {
    await mkdir(path.dirname(artifact), { recursive: true });
    try { await writeFile(artifact, expected, { flag: "wx" }); }
    catch (error) {
      if (!error || typeof error !== "object" || !("code" in error) || error.code !== "EEXIST" || !(await readFile(artifact)).equals(expected)) throw error;
    }
  } else throw new Error("R10 artifact is missing; run its explicit build command");
  return built;
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length > 1 || (args.length === 1 && args[0] !== "--write" && args[0] !== "--check" && args[0] !== "--check-deno")) throw new Error("Usage: bun scripts/build_v2_sidecar_r10.ts [--check|--write|--check-deno]");
  if (args[0] === "--check-deno") {
    await loadV2SidecarAssets({ revision: "r10" });
    const check = Bun.spawn([process.execPath, "--no-env-file", "x", "--no-install", "deno@2.9.5", "check",
      "--no-config", "--no-remote", "--no-npm", "--no-lock", R10_ARTIFACT], { cwd: ROOT, stdout: "inherit", stderr: "inherit" });
    process.exit(await check.exited);
  }
  const built = await checkR10Sidecar({ write: args[0] === "--write" });
  console.log(JSON.stringify({ ...built.identity, bytes: built.bytes.length, mode: args[0] === "--write" ? "write" : "check" }, null, 2));
}
