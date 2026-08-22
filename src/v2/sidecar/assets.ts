import { readFile } from "node:fs/promises";
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
}

const ASSET_PREFIX = ".ghostprov-v2";
const ENTRYPOINTS = Object.freeze(["sandbox/v2/main.ts"]);
const ALLOWED_PACKAGE_IMPORTS = new Set([
  "fflate",
  "npm:@y-sweet/client@0.9.1",
  "npm:yjs@13.6.27",
]);
const CREDENTIAL_PATTERNS = Object.freeze([
  /sk-[A-Za-z0-9_-]{24,}/,
  /gAAAA[A-Za-z0-9_-]{24,}/,
  /eyJ[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{24,}(?:\.[A-Za-z0-9_-]{16,})?/,
]);
const IMPORT_PATTERN = /\b(?:from\s*|import\s*(?:\(\s*)?)(["'])([^"']+)\1/g;
const HIGH_ENTROPY_LITERAL_PATTERN = /(["'`])([A-Za-z0-9_+/=-]{48,})\1/g;

const DENO_CONFIG = `${JSON.stringify({
  compilerOptions: {
    lib: ["deno.window", "dom", "dom.iterable"],
    strict: true,
  },
  imports: {
    fflate: "npm:fflate@0.8.2",
  },
}, null, 2)}\n`;

function portablePath(value: string): string {
  return value.split(path.sep).join("/");
}

function sourcePath(assetPath: string): string {
  const prefix = `${ASSET_PREFIX}/`;
  if (!assetPath.startsWith(prefix)) {
    throw new Error(`V2 sidecar asset is outside ${ASSET_PREFIX}: ${assetPath}`);
  }
  return assetPath.slice(prefix.length);
}

function extractImports(content: string): readonly string[] {
  return [...content.matchAll(IMPORT_PATTERN)].map(match => match[2]!);
}

function shannonEntropy(value: string): number {
  const counts = new Map<string, number>();
  for (const character of value) {
    counts.set(character, (counts.get(character) ?? 0) + 1);
  }
  let entropy = 0;
  for (const count of counts.values()) {
    const probability = count / value.length;
    entropy -= probability * Math.log2(probability);
  }
  return entropy;
}

function assertNoCredentialLiteral(assetPath: string, content: string): void {
  if (CREDENTIAL_PATTERNS.some(pattern => pattern.test(content))) {
    throw new Error(`V2 sidecar asset contains a credential literal: ${assetPath}`);
  }
  for (const match of content.matchAll(HIGH_ENTROPY_LITERAL_PATTERN)) {
    const literal = match[2]!;
    if (shannonEntropy(literal) >= 4.5) {
      throw new Error(`V2 sidecar asset contains a high-entropy literal: ${assetPath}`);
    }
  }
}

function resolveRelativeImport(
  repositoryRoot: string,
  importer: string,
  specifier: string,
): readonly string[] {
  const importerDirectory = path.dirname(path.join(repositoryRoot, importer));
  const unresolved = path.resolve(importerDirectory, specifier);
  const relative = path.relative(repositoryRoot, unresolved);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`V2 sidecar import escapes the repository: ${importer} -> ${specifier}`);
  }
  if (path.extname(unresolved)) return [unresolved];
  return [`${unresolved}.ts`, path.join(unresolved, "index.ts")];
}

async function readPortableSources(repositoryRoot: string): Promise<Map<string, string>> {
  const pending = [...ENTRYPOINTS];
  const sources = new Map<string, string>();

  while (pending.length > 0) {
    const relativePath = pending.pop()!;
    if (sources.has(relativePath)) continue;
    const absolutePath = path.join(repositoryRoot, relativePath);
    let content: string;
    try {
      content = await readFile(absolutePath, "utf8");
    } catch (cause) {
      throw new Error(`Unable to read V2 sidecar source: ${relativePath}`, { cause });
    }
    const assetPath = `${ASSET_PREFIX}/${portablePath(relativePath)}`;
    assertNoCredentialLiteral(assetPath, content);
    sources.set(portablePath(relativePath), content);

    for (const specifier of extractImports(content)) {
      if (!specifier.startsWith(".")) {
        if (!ALLOWED_PACKAGE_IMPORTS.has(specifier)) {
          throw new Error(`V2 sidecar source imports an unpinned package: ${specifier}`);
        }
        continue;
      }
      const candidates = resolveRelativeImport(repositoryRoot, relativePath, specifier);
      let resolved: string | null = null;
      for (const candidate of candidates) {
        try {
          await readFile(candidate, "utf8");
          resolved = portablePath(path.relative(repositoryRoot, candidate));
          break;
        } catch {
          // Try the next TypeScript resolution candidate.
        }
      }
      if (!resolved) {
        throw new Error(`V2 sidecar relative import is missing: ${relativePath} -> ${specifier}`);
      }
      pending.push(resolved);
    }
  }

  return sources;
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
  const repositoryRoot = path.resolve(import.meta.dir, "../../..");
  const sources = await readPortableSources(repositoryRoot);
  sources.set("deno.json", DENO_CONFIG);

  const files = await Promise.all(
    [...sources.entries()]
      .map(([relativePath, content]) => ({
        path: `${ASSET_PREFIX}/${relativePath}`,
        content,
      }))
      .sort((left, right) => left.path.localeCompare(right.path))
      .map(async file => ({
        ...file,
        sha256: await sha256Hex(utf8Bytes(file.content)),
      })),
  );

  for (const file of files) assertNoCredentialLiteral(file.path, file.content);
  return {
    buildHash: await hashV2SidecarFiles(files),
    schemaHash: await protocolSchemaHash(),
    files,
  };
}

export function sidecarSourcePath(asset: V2SidecarAsset): string {
  return sourcePath(asset.path);
}
