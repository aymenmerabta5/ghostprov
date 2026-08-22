import path from "node:path";
import {
  protocolSchemaHash,
  sha256Hex,
  utf8Bytes,
} from "../protocol/shared";

export interface V2SidecarAsset {
  readonly path: string;
  readonly content: string;
  readonly sha256: string;
}

export interface V2SidecarAssets {
  readonly buildHash: string;
  readonly schemaHash: string;
  readonly files: readonly V2SidecarAsset[];
  readonly obsoletePaths: readonly string[];
}

const ASSET_PREFIX = ".ghostprov-v2";
const BUNDLE_PATH = `${ASSET_PREFIX}/sidecar.bundle.js`;
const OBSOLETE_PATHS = Object.freeze([
  `${ASSET_PREFIX}/bootstrap.tex`,
  `${ASSET_PREFIX}/deno.json`,
  `${ASSET_PREFIX}/sandbox`,
  `${ASSET_PREFIX}/src`,
]);
const ENTRYPOINT = path.resolve(import.meta.dir, "../../../sandbox/v2/main.ts");
const CREDENTIAL_PATTERNS = Object.freeze([
  /sk-[A-Za-z0-9_-]{24,}/,
  /gAAAA[A-Za-z0-9_-]{24,}/,
  /eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}(?:\.[A-Za-z0-9_-]{16,})?/,
]);
const RUNTIME_IMPORT_PATTERN = /\b(?:from|import)\s*\(?["'](?:npm:|https:)/;

function assertOfflineBundle(content: string): void {
  if (RUNTIME_IMPORT_PATTERN.test(content) || content.includes("registry.npmjs.org")) {
    throw new Error("V2 sidecar bundle contains a runtime network import");
  }
  if (CREDENTIAL_PATTERNS.some(pattern => pattern.test(content))) {
    throw new Error("V2 sidecar bundle contains a credential-shaped literal");
  }
}

async function buildOfflineBundle(): Promise<string> {
  const result = await Bun.build({
    entrypoints: [ENTRYPOINT],
    target: "browser",
    format: "esm",
    splitting: false,
    sourcemap: "none",
    minify: {
      syntax: true,
      whitespace: true,
      identifiers: false,
    },
    define: {
      "process.env.NODE_ENV": '"production"',
    },
  });
  if (!result.success || result.outputs.length !== 1) {
    const detail = result.logs.map(log => log.message).join("; ").slice(0, 2_048);
    throw new Error(`Unable to build the offline V2 sidecar bundle: ${detail || "unknown build failure"}`);
  }
  const content = `${(await result.outputs[0]!.text()).trimEnd()}\n`;
  assertOfflineBundle(content);
  return content;
}

export async function hashV2SidecarFiles(
  files: readonly Pick<V2SidecarAsset, "path" | "content">[],
): Promise<string> {
  const sorted = [...files].sort((left, right) => left.path.localeCompare(right.path));
  const seen = new Set<string>();
  let aggregate = "";
  for (const file of sorted) {
    if (seen.has(file.path)) throw new Error(`Duplicate V2 sidecar asset: ${file.path}`);
    seen.add(file.path);
    aggregate += `${file.path}\0${file.content}`;
  }
  return sha256Hex(utf8Bytes(aggregate));
}

export async function loadV2SidecarAssets(): Promise<V2SidecarAssets> {
  const content = await buildOfflineBundle();
  const files = Object.freeze([{
    path: BUNDLE_PATH,
    content,
    sha256: await sha256Hex(utf8Bytes(content)),
  }]);
  return {
    buildHash: await hashV2SidecarFiles(files),
    schemaHash: await protocolSchemaHash(),
    files,
    obsoletePaths: OBSOLETE_PATHS,
  };
}

export function sidecarSourcePath(asset: V2SidecarAsset): string {
  const prefix = `${ASSET_PREFIX}/`;
  if (!asset.path.startsWith(prefix)) {
    throw new Error(`V2 sidecar asset is outside ${ASSET_PREFIX}: ${asset.path}`);
  }
  return asset.path.slice(prefix.length);
}
