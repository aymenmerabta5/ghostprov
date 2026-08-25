import {
  mkdir,
  readFile,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import { protocolSchemaHash, sha256Hex, utf8Bytes } from "../src/v2/protocol/shared";
import { V2_R3_RELEASE } from "../src/v2/sidecar/assets";

const REPOSITORY_ROOT = path.resolve(import.meta.dir, "..");
export const V2_R3_SIDECAR_ENTRYPOINT = path.join(
  REPOSITORY_ROOT,
  "sandbox/v2/main.ts",
);
export const V2_R3_SIDECAR_ARTIFACT = path.join(
  REPOSITORY_ROOT,
  "src/v2/sidecar/releases/r3/sidecar.bundle.js",
);
const LOGICAL_BUNDLE_PATH = "sidecar.bundle.js";

export const V2_R3_SIDECAR_BUILD_RECIPE = Object.freeze({
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
  readonly revision: "r3";
  readonly protocolRevision: "r2";
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

function assertReleaseBundle(content: string): void {
  if (!content.endsWith("\n") || content.endsWith("\n\n") || content.includes("\r")) {
    throw new Error("V2-R3 sidecar bundle must use LF and end in exactly one LF");
  }
  if (RUNTIME_NETWORK_IMPORT.test(content) || content.includes("registry.npmjs.org")) {
    throw new Error("V2-R3 sidecar bundle contains a runtime network import");
  }
  for (const match of content.matchAll(RUNTIME_IMPORT_SPECIFIER)) {
    if (!ALLOWED_RUNTIME_IMPORTS.has(match[1]!)) {
      throw new Error(`V2-R3 sidecar bundle contains an unsupported runtime import: ${match[1]}`);
    }
  }
  if (![...content.matchAll(RUNTIME_IMPORT_SPECIFIER)].some(match => match[1] === "node:crypto")) {
    throw new Error("V2-R3 sidecar bundle is missing its native node:crypto import");
  }
  if (CREDENTIAL_PATTERNS.some(pattern => pattern.test(content))) {
    throw new Error("V2-R3 sidecar bundle contains a credential-shaped literal");
  }
  if (/sourceMappingURL=/.test(content)) {
    throw new Error("V2-R3 sidecar bundle contains a source map reference");
  }
  if (utf8Bytes(content).byteLength < 1_000 || !content.includes("Deno.")) {
    throw new Error("V2-R3 sidecar bundle is not a complete Deno entrypoint");
  }
}

async function buildOnce(): Promise<Uint8Array> {
  const result = await Bun.build({
    entrypoints: [V2_R3_SIDECAR_ENTRYPOINT],
    ...V2_R3_SIDECAR_BUILD_RECIPE,
    external: [...V2_R3_SIDECAR_BUILD_RECIPE.external],
  });
  if (!result.success || result.outputs.length !== 1 || result.logs.length !== 0) {
    const diagnostics = result.logs.map(log => log.message).join("\n");
    throw new Error(
      `V2-R3 sidecar build failed, warned, or emitted multiple files${diagnostics ? `:\n${diagnostics}` : ""}`,
    );
  }
  const content = canonicalV2SidecarBundle(await result.outputs[0]!.text());
  assertReleaseBundle(content);
  return utf8Bytes(content);
}

/**
 * Builds twice in fresh Bun build invocations and refuses nondeterministic bytes.
 * No release file is read or written by this function.
 */
export async function buildV2SidecarRelease(): Promise<V2SidecarReleaseBuild> {
  const first = await buildOnce();
  const second = await buildOnce();
  if (!equalBytes(first, second)) {
    throw new Error("V2-R3 sidecar build is nondeterministic: consecutive bytes differ");
  }
  const content = new TextDecoder("utf-8", { fatal: true }).decode(first);
  const [bundleSha256, buildHash, schemaHash] = await Promise.all([
    sha256Hex(first),
    sha256Hex(utf8Bytes(`${LOGICAL_BUNDLE_PATH}\0${content}`)),
    protocolSchemaHash("r2"),
  ]);
  return Object.freeze({
    revision: "r3",
    protocolRevision: "r2",
    content,
    bytes: first,
    bundleSha256,
    buildHash,
    schemaHash,
  });
}

export async function checkV2SidecarRelease(
  artifactPath = V2_R3_SIDECAR_ARTIFACT,
): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease();
  let checkedIn: Uint8Array;
  try {
    checkedIn = new Uint8Array(await readFile(artifactPath));
  } catch (cause) {
    throw new Error("Frozen V2-R3 sidecar artifact is missing", { cause });
  }
  if (!equalBytes(checkedIn, built.bytes)) {
    throw new Error("Frozen V2-R3 sidecar artifact differs from the deterministic build");
  }
  if (
    V2_R3_RELEASE.revision !== built.revision ||
    V2_R3_RELEASE.protocolRevision !== built.protocolRevision ||
    V2_R3_RELEASE.bundleSha256 !== built.bundleSha256 ||
    V2_R3_RELEASE.buildHash !== built.buildHash ||
    V2_R3_RELEASE.schemaHash !== built.schemaHash
  ) {
    throw new Error("Frozen V2-R3 sidecar release identity differs from the deterministic build");
  }
  return built;
}

async function writeV2SidecarRelease(): Promise<V2SidecarReleaseBuild> {
  const built = await buildV2SidecarRelease();
  await mkdir(path.dirname(V2_R3_SIDECAR_ARTIFACT), { recursive: true });
  try {
    const existing = new Uint8Array(await readFile(V2_R3_SIDECAR_ARTIFACT));
    if (!equalBytes(existing, built.bytes)) {
      throw new Error(
        "Refusing to overwrite an immutable V2-R3 sidecar artifact with different bytes",
      );
    }
    return built;
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }

  try {
    await writeFile(V2_R3_SIDECAR_ARTIFACT, built.bytes, { flag: "wx" });
  } catch (error) {
    // An independent writer may have won the create-only race. It is safe only
    // if it published the exact same deterministic bytes.
    if (!(error instanceof Error && "code" in error && error.code === "EEXIST")) throw error;
    const existing = new Uint8Array(await readFile(V2_R3_SIDECAR_ARTIFACT));
    if (!equalBytes(existing, built.bytes)) {
      throw new Error(
        "Refusing to overwrite an immutable V2-R3 sidecar artifact with different bytes",
      );
    }
  }
  return built;
}

function releaseIdentity(build: V2SidecarReleaseBuild): Record<string, string> {
  return {
    revision: build.revision,
    protocolRevision: build.protocolRevision,
    buildHash: build.buildHash,
    schemaHash: build.schemaHash,
    bundleSha256: build.bundleSha256,
  };
}

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<void> {
  const mode = v2SidecarReleaseMode(args);
  const checked = mode === "check";
  const build = checked
    ? await checkV2SidecarRelease()
    : await writeV2SidecarRelease();
  console.log(JSON.stringify({
    mode: checked ? "check" : "write",
    artifact: path.relative(REPOSITORY_ROOT, V2_R3_SIDECAR_ARTIFACT).replaceAll("\\", "/"),
    bytes: build.bytes.byteLength,
    ...releaseIdentity(build),
  }, null, 2));
}

export function v2SidecarReleaseMode(args: readonly string[]): "check" | "write" {
  if (args.length === 0 || (args.length === 1 && args[0] === "--check")) return "check";
  if (args.length === 1 && args[0] === "--write") return "write";
  throw new Error("Usage: bun scripts/build_v2_sidecar_release.ts [--check|--write]");
}

if (import.meta.main) await main();
