import {
  mkdir,
  readFile,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { protocolSchemaHash, sha256Hex, utf8Bytes } from "../src/v2/protocol/shared";
import { V2_R8_RELEASE, V2_R9_RELEASE, type V2SidecarReleaseIdentity } from "../src/v2/sidecar/assets";

const REPOSITORY_ROOT = path.resolve(import.meta.dir, "..");
export const V2_R8_SIDECAR_ENTRYPOINT = path.join(
  REPOSITORY_ROOT,
  "sandbox/v2/main.ts",
);
export const V2_R9_SIDECAR_ENTRYPOINT = path.join(
  REPOSITORY_ROOT,
  "sandbox/v2/r9-main.ts",
);
export const V2_R8_SIDECAR_ARTIFACT = path.join(
  REPOSITORY_ROOT,
  "src/v2/sidecar/releases/r8/sidecar.bundle.js",
);
export const V2_R9_SIDECAR_ARTIFACT = path.join(
  REPOSITORY_ROOT,
  "src/v2/sidecar/releases/r9/sidecar.bundle.js",
);
const LOGICAL_BUNDLE_PATH = "sidecar.bundle.js";

export const V2_R8_SIDECAR_BUILD_RECIPE = Object.freeze({
  target: "browser" as const,
  format: "esm" as const,
  splitting: false,
  sourcemap: "none" as const,
  // Keep Deno's built-in native incremental hash implementation. Bundling
  // node:crypto for a browser target would substitute a large JS polyfill.
  external: Object.freeze(["node:*"]),
  minify: Object.freeze({
    syntax: true,
    whitespace: true,
    identifiers: false,
  }),
});

const CREDENTIAL_PATTERNS = Object.freeze([
  /sk-[A-Za-z0-9_-]{24,}/,
  /gAAAA[A-Za-z0-9_-]{24,}/,
  /eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}(?:\.[A-Za-z0-9_-]{16,})?/,
]);
const RUNTIME_NETWORK_IMPORT = /\b(?:from|import)\s*\(?["'](?:npm:|https:)/;
const RUNTIME_IMPORT_SPECIFIER = /\b(?:from|import)\s*\(?\s*["']([^"']+)["']/g;
const ALLOWED_RUNTIME_IMPORTS = new Set(["node:crypto"]);

export interface V2SidecarReleaseBuild {
  readonly revision: "r8" | "r9";
  readonly protocolRevision: "r2";
  readonly transportProfile: "bulk-v1";
  readonly content: string;
  readonly bytes: Uint8Array;
  readonly bundleSha256: string;
  readonly buildHash: string;
  readonly schemaHash: string;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index++) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

/** Canonical release text has platform-independent LF endings and one final LF. */
export function canonicalV2SidecarBundle(value: string): string {
  return `${value.replace(/\r\n?/g, "\n").replace(/\n+$/g, "")}\n`;
}

function assertReleaseBundle(content: string, label = "V2-R8"): void {
  if (!content.endsWith("\n") || content.endsWith("\n\n") || content.includes("\r")) {
    throw new Error(`${label} sidecar bundle must use LF and end in exactly one LF`);
  }
  if (RUNTIME_NETWORK_IMPORT.test(content) || content.includes("registry.npmjs.org")) {
    throw new Error(`${label} sidecar bundle contains a runtime network import`);
  }
  for (const match of content.matchAll(RUNTIME_IMPORT_SPECIFIER)) {
    if (!ALLOWED_RUNTIME_IMPORTS.has(match[1]!)) {
      throw new Error(`${label} sidecar bundle contains an unsupported runtime import: ${match[1]}`);
    }
  }
  if (![...content.matchAll(RUNTIME_IMPORT_SPECIFIER)].some(match => match[1] === "node:crypto")) {
    throw new Error(`${label} sidecar bundle is missing its native node:crypto import`);
  }
  if (CREDENTIAL_PATTERNS.some(pattern => pattern.test(content))) {
    throw new Error(`${label} sidecar bundle contains a credential-shaped literal`);
  }
  if (/sourceMappingURL=/.test(content)) {
    throw new Error(`${label} sidecar bundle contains a source map reference`);
  }
  if (utf8Bytes(content).byteLength < 1_000 || !content.includes("Deno.")) {
    throw new Error(`${label} sidecar bundle is not a complete Deno entrypoint`);
  }
}

async function buildOnce(entrypoint = V2_R8_SIDECAR_ENTRYPOINT): Promise<Uint8Array> {
  const result = await Bun.build({
    entrypoints: [entrypoint],
    ...V2_R8_SIDECAR_BUILD_RECIPE,
    external: [...V2_R8_SIDECAR_BUILD_RECIPE.external],
  });
  if (!result.success || result.outputs.length !== 1 || result.logs.length !== 0) {
    const diagnostics = result.logs.map(log => log.message).join("\n");
    const label = entrypoint.includes("r9-main") ? "V2-R9" : "V2-R8";
    throw new Error(
      `${label} sidecar build failed, warned, or emitted multiple files${diagnostics ? `:\n${diagnostics}` : ""}`,
    );
  }
  const content = canonicalV2SidecarBundle(await result.outputs[0]!.text());
  assertReleaseBundle(content, entrypoint.includes("r9-main") ? "V2-R9" : "V2-R8");
  return utf8Bytes(content);
}

/**
 * Builds twice in fresh Bun build invocations and refuses nondeterministic bytes.
 * No release file is read or written by this function.
 */
export async function buildV2SidecarRelease(
  revision: "r8" | "r9" = "r8",
): Promise<V2SidecarReleaseBuild> {
  const entrypoint = revision === "r9" ? V2_R9_SIDECAR_ENTRYPOINT : V2_R8_SIDECAR_ENTRYPOINT;
  const first = await buildOnce(entrypoint);
  const second = await buildOnce(entrypoint);
  if (!equalBytes(first, second)) {
    throw new Error(`V2-${revision.toUpperCase()} sidecar build is nondeterministic: consecutive bytes differ`);
  }
  const content = new TextDecoder("utf-8", { fatal: true }).decode(first);
  const [bundleSha256, buildHash, schemaHash] = await Promise.all([
    sha256Hex(first),
    sha256Hex(utf8Bytes(`${LOGICAL_BUNDLE_PATH}\0${content}`)),
    protocolSchemaHash("r2", "bulk-v1"),
  ]);
  return Object.freeze({
    revision,
    protocolRevision: "r2",
    transportProfile: "bulk-v1",
    content,
    bytes: first,
    bundleSha256,
    buildHash,
    schemaHash,
  });
}

function assertV2SidecarReleaseIdentity(
  built: V2SidecarReleaseBuild,
  expected: V2SidecarReleaseIdentity = V2_R8_RELEASE,
): void {
  if (
    expected.revision !== built.revision ||
    expected.protocolRevision !== built.protocolRevision ||
    expected.transportProfile !== built.transportProfile ||
    expected.bundleSha256 !== built.bundleSha256 ||
    expected.buildHash !== built.buildHash ||
    expected.schemaHash !== built.schemaHash
  ) {
    throw new Error(
      `Frozen V2-${expected.revision.toUpperCase()} sidecar release identity differs from deterministic identity ${JSON.stringify(releaseIdentity(built))}`,
    );
  }
}

export async function checkV2SidecarRelease(
  artifactPath = V2_R8_SIDECAR_ARTIFACT,
): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease();
  let checkedIn: Uint8Array;
  try {
    checkedIn = new Uint8Array(await readFile(artifactPath));
  } catch (cause) {
    throw new Error(
      `Frozen V2-R8 sidecar artifact is missing; deterministic identity ${JSON.stringify(releaseIdentity(built))}`,
      { cause },
    );
  }
  if (!equalBytes(checkedIn, built.bytes)) {
    throw new Error("Frozen V2-R8 sidecar artifact differs from the deterministic build");
  }
  assertV2SidecarReleaseIdentity(built);
  return built;
}

async function writeV2SidecarRelease(): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease();
  assertV2SidecarReleaseIdentity(built);
  await mkdir(path.dirname(V2_R8_SIDECAR_ARTIFACT), { recursive: true });
  try {
    const existing = new Uint8Array(await readFile(V2_R8_SIDECAR_ARTIFACT));
    if (!equalBytes(existing, built.bytes)) {
      throw new Error(
        "Refusing to overwrite an immutable V2-R8 sidecar artifact with different bytes",
      );
    }
    return built;
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }

  try {
    await writeFile(V2_R8_SIDECAR_ARTIFACT, built.bytes, { flag: "wx" });
  } catch (error) {
    // An independent writer may have won the create-only race. It is safe only
    // if it published the exact same deterministic bytes.
    if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
    const existing = new Uint8Array(await readFile(V2_R8_SIDECAR_ARTIFACT));
    if (!equalBytes(existing, built.bytes)) {
      throw new Error(
        "Refusing to overwrite an immutable V2-R8 sidecar artifact with different bytes",
      );
    }
  }
  return built;
}

/**
 * R9 sandbox-relay release. Unlike R8 this artifact is owned by the current
 * source tree: writing refuses to change existing bytes, so a rebuilt bundle
 * always means a source change the operator explicitly staged.
 */
export async function checkV2SidecarR9Release(
  artifactPath = V2_R9_SIDECAR_ARTIFACT,
): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease("r9");
  let checkedIn: Uint8Array;
  try {
    checkedIn = new Uint8Array(await readFile(artifactPath));
  } catch (cause) {
    throw new Error(
      `V2-R9 sidecar artifact is missing; deterministic identity ${JSON.stringify(releaseIdentity(built))}`,
      { cause },
    );
  }
  if (!equalBytes(checkedIn, built.bytes)) {
    throw new Error("V2-R9 sidecar artifact differs from the deterministic build");
  }
  assertV2SidecarReleaseIdentity(built, V2_R9_RELEASE);
  return built;
}

export async function writeV2SidecarR9Release(
  artifactPath = V2_R9_SIDECAR_ARTIFACT,
): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease("r9");
  assertV2SidecarReleaseIdentity(built, V2_R9_RELEASE);
  await mkdir(path.dirname(artifactPath), { recursive: true });
  try {
    const existing = new Uint8Array(await readFile(artifactPath));
    if (!equalBytes(existing, built.bytes)) {
      throw new Error(
        "Refusing to overwrite the checked-in V2-R9 sidecar artifact with different bytes; stage the change explicitly",
      );
    }
    return built;
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }
  await writeFile(artifactPath, built.bytes, { flag: "wx" });
  return built;
}

function releaseIdentity(build: V2SidecarReleaseBuild): Record<string, string> {
  return {
    revision: build.revision,
    protocolRevision: build.protocolRevision,
    transportProfile: build.transportProfile,
    buildHash: build.buildHash,
    schemaHash: build.schemaHash,
    bundleSha256: build.bundleSha256,
  };
}

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<void> {
  const mode = v2SidecarReleaseMode(args);
  const isR9 = mode === "check-r9" || mode === "write-r9";
  const checked = mode === "check" || mode === "check-r9";
  const build = isR9
    ? checked
      ? await checkV2SidecarR9Release()
      : await writeV2SidecarR9Release()
    : checked
      ? await checkV2SidecarRelease()
      : await writeV2SidecarRelease();
  console.log(JSON.stringify({
    mode,
    artifact: path.relative(
      REPOSITORY_ROOT,
      isR9 ? V2_R9_SIDECAR_ARTIFACT : V2_R8_SIDECAR_ARTIFACT,
    ).replaceAll("\\", "/"),
    bytes: build.bytes.byteLength,
    ...releaseIdentity(build),
  }, null, 2));
}

export function v2SidecarReleaseMode(
  args: readonly string[],
): "check" | "write" | "check-r9" | "write-r9" {
  if (args.length === 0 || (args.length === 1 && args[0] === "--check")) return "check";
  if (args.length === 1 && args[0] === "--write") return "write";
  if (args.length === 1 && args[0] === "--check-r9") return "check-r9";
  if (args.length === 1 && args[0] === "--write-r9") return "write-r9";
  throw new Error("Usage: bun scripts/build_v2_sidecar_release.ts [--check|--write|--check-r9|--write-r9]");
}

if (import.meta.main) await main();
