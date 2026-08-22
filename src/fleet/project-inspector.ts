import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import type { YSweetEnvelope } from "../prism/protocol";
import type { ProjectId } from "../sandbox/types";
import type {
  ManagedProjectInspection,
  ManagedProjectInspector,
} from "./types";

export interface ProjectInspectorProviderLike {
  on(event: string, listener: (...args: any[]) => void): void;
  destroy(): void;
}

export interface ProjectInspectorProviderFactory {
  create(envelope: YSweetEnvelope, doc: Y.Doc): ProjectInspectorProviderLike;
}

export type ProjectInspectionErrorKind = "auth" | "sync" | "document" | "marker";

export class ProjectInspectionError extends Error {
  readonly name = "ProjectInspectionError";

  constructor(
    message: string,
    readonly kind: ProjectInspectionErrorKind,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface YDocProjectInspectorOptions {
  mintYSweet: (
    projectId: ProjectId,
    signal?: AbortSignal,
  ) => Promise<YSweetEnvelope>;
  providerFactory?: ProjectInspectorProviderFactory;
  syncTimeoutMs?: number;
}

const DEFAULT_PROVIDER_FACTORY: ProjectInspectorProviderFactory = {
  create(envelope, doc) {
    const token = envelope.token.reveal();
    class AuthenticatedWebSocket extends WebSocket {
      constructor(url: string | URL, protocols?: string | string[]) {
        const raw = String(url);
        super(
          `${raw}${raw.includes("?") ? "&" : "?"}token=${encodeURIComponent(token)}`,
          protocols,
        );
      }
    }
    return new WebsocketProvider(
      envelope.url,
      envelope.docId,
      doc,
      {
        connect: true,
        WebSocketPolyfill: AuthenticatedWebSocket,
      } as any,
    ) as unknown as ProjectInspectorProviderLike;
  },
};

interface ProjectNode {
  readonly uuid: string;
  readonly id: string;
  readonly filename: string;
  readonly parentId: string | null;
  readonly type: string;
  readonly map: Y.Map<unknown>;
}

function abortError(): DOMException {
  return new DOMException("Project inspection aborted", "AbortError");
}

function readText(map: Y.Map<unknown>): string | null {
  const value = map.get("content");
  if (value == null) return null;
  if (value instanceof Y.Text) return value.toString();
  return typeof value === "string" ? value : null;
}

function validComponent(value: string): boolean {
  return value.length > 0 &&
    value.length <= 255 &&
    value !== "." &&
    value !== ".." &&
    !value.includes("/") &&
    !value.includes("\\") &&
    !value.includes("\0");
}

function liveProjectNodes(doc: Y.Doc): readonly ProjectNode[] {
  const shared = doc.getMap("content");
  const nodes: ProjectNode[] = [];
  const ids = new Set<string>();
  for (const [uuid, raw] of shared.entries()) {
    if (!(raw instanceof Y.Map)) {
      throw new ProjectInspectionError("Project contains a malformed node", "document");
    }
    if (raw.get("deleted") === true) continue;
    const idValue = raw.get("id");
    const id = typeof idValue === "string" && idValue ? idValue : uuid;
    if (ids.has(id)) {
      throw new ProjectInspectionError("Project contains duplicate node IDs", "document");
    }
    ids.add(id);
    const filename = raw.get("filename");
    const type = raw.get("type");
    const parent = raw.get("inFolder");
    if (typeof filename !== "string" || typeof type !== "string") {
      throw new ProjectInspectionError("Project node metadata is malformed", "document");
    }
    if (parent != null && typeof parent !== "string") {
      throw new ProjectInspectionError("Project node parent is malformed", "document");
    }
    nodes.push({
      uuid,
      id,
      filename,
      parentId: parent ?? null,
      type,
      map: raw,
    });
  }
  return nodes;
}

function projectPaths(nodes: readonly ProjectNode[]): ReadonlyMap<string, ProjectNode> {
  if (nodes.length === 0) return new Map();
  const roots = nodes.filter(node => (
    node.filename === "root" && node.type === "folder" && node.parentId === null
  ));
  if (roots.length !== 1) {
    throw new ProjectInspectionError("Project must contain exactly one live root", "document");
  }
  const root = roots[0]!;
  const byId = new Map(nodes.map(node => [node.id, node]));
  const resolved = new Map<string, string>([[root.id, ""]]);
  const resolving = new Set<string>();

  const resolveNode = (node: ProjectNode): string => {
    const cached = resolved.get(node.id);
    if (cached !== undefined) return cached;
    if (resolving.has(node.id)) {
      throw new ProjectInspectionError("Project folder graph contains a cycle", "document");
    }
    if (!validComponent(node.filename) || !node.parentId) {
      throw new ProjectInspectionError("Project node path is malformed", "document");
    }
    const parent = byId.get(node.parentId);
    if (!parent || parent.type !== "folder") {
      throw new ProjectInspectionError("Project node parent is missing", "document");
    }
    resolving.add(node.id);
    const parentPath = resolveNode(parent);
    resolving.delete(node.id);
    const result = parentPath ? `${parentPath}/${node.filename}` : node.filename;
    resolved.set(node.id, result);
    return result;
  };

  const paths = new Map<string, ProjectNode>();
  for (const node of nodes) {
    const resolvedPath = resolveNode(node);
    if (paths.has(resolvedPath)) {
      throw new ProjectInspectionError("Project contains duplicate live paths", "document");
    }
    paths.set(resolvedPath, node);
  }
  return paths;
}

function isGhostProvPath(path: string): boolean {
  if (path === "" || path === "main.tex" || path === ".latexmkrc") return true;
  if (path === ".ghostprov-v2") return true;
  if (/^\.ghostprov-v2\/(?:src|sandbox)(?:\/.*)?$/.test(path)) return true;
  return [
    ".ghostprov-v2/deno.json",
    ".ghostprov-v2/static.json",
    ".ghostprov-v2/worker.json",
    ".ghostprov-v2/bootstrap.enc.json",
  ].includes(path);
}

function parseWorkerMarker(content: string): ManagedProjectInspection["marker"] {
  let value: unknown;
  try {
    value = JSON.parse(content);
  } catch (cause) {
    throw new ProjectInspectionError("GhostProv worker marker is not valid JSON", "marker", { cause });
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ProjectInspectionError("GhostProv worker marker must be an object", "marker");
  }
  const raw = value as Record<string, unknown>;
  const keys = Object.keys(raw).sort();
  const expected = ["buildHash", "createdAtMs", "protocol", "workerIndex"];
  if (keys.length !== expected.length || keys.some((key, index) => key !== expected[index])) {
    throw new ProjectInspectionError("GhostProv worker marker fields are invalid", "marker");
  }
  if (
    raw.protocol !== "gp-awareness-v2" ||
    !Number.isSafeInteger(raw.workerIndex) ||
    (raw.workerIndex as number) < 1 ||
    typeof raw.buildHash !== "string" ||
    !/^[a-f0-9]{64}$/.test(raw.buildHash) ||
    !Number.isSafeInteger(raw.createdAtMs) ||
    (raw.createdAtMs as number) < 0
  ) {
    throw new ProjectInspectionError("GhostProv worker marker values are invalid", "marker");
  }
  return {
    protocol: "gp-awareness-v2",
    workerIndex: raw.workerIndex as number,
  };
}

async function waitForSync(
  provider: ProjectInspectorProviderLike,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted) throw abortError();
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(() => finish(new ProjectInspectionError(
      `Project Y-Sweet sync timed out after ${timeoutMs}ms`,
      "sync",
    )), timeoutMs);
    const onAbort = () => finish(abortError());
    signal?.addEventListener("abort", onAbort, { once: true });
    provider.on("sync", value => {
      if (value === true) finish();
    });
    provider.on("connectionClose", () => finish(new ProjectInspectionError(
      "Project Y-Sweet connection closed before sync",
      "sync",
    )));
  });
}

export class YDocProjectInspector implements ManagedProjectInspector {
  readonly #mintYSweet: YDocProjectInspectorOptions["mintYSweet"];
  readonly #providerFactory: ProjectInspectorProviderFactory;
  readonly #syncTimeoutMs: number;

  constructor(options: YDocProjectInspectorOptions) {
    if (!Number.isInteger(options.syncTimeoutMs ?? 15_000) || (options.syncTimeoutMs ?? 15_000) < 1) {
      throw new RangeError("syncTimeoutMs must be a positive integer");
    }
    this.#mintYSweet = options.mintYSweet;
    this.#providerFactory = options.providerFactory ?? DEFAULT_PROVIDER_FACTORY;
    this.#syncTimeoutMs = options.syncTimeoutMs ?? 15_000;
  }

  async inspect(
    projectId: ProjectId,
    signal?: AbortSignal,
  ): Promise<ManagedProjectInspection> {
    if (signal?.aborted) throw abortError();
    let envelope: YSweetEnvelope;
    try {
      envelope = await this.#mintYSweet(projectId, signal);
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      throw new ProjectInspectionError("Unable to mint project Y-Sweet credential", "auth", { cause });
    }
    if (envelope.docId !== projectId) {
      throw new ProjectInspectionError("Y-Sweet credential belongs to another project", "auth");
    }

    const doc = new Y.Doc({ guid: projectId });
    let provider: ProjectInspectorProviderLike | null = null;
    try {
      provider = this.#providerFactory.create(envelope, doc);
      await waitForSync(provider, this.#syncTimeoutMs, signal);
      const paths = projectPaths(liveProjectNodes(doc));
      const markerNode = paths.get(".ghostprov-v2/worker.json");
      const markerContent = markerNode ? readText(markerNode.map) : null;
      if (markerNode && markerContent === null) {
        throw new ProjectInspectionError("GhostProv worker marker content is invalid", "marker");
      }
      return {
        writable: envelope.authorization === "full",
        empty: [...paths.keys()].every(isGhostProvPath),
        marker: markerContent === null ? null : parseWorkerMarker(markerContent),
      };
    } finally {
      try { provider?.destroy(); } catch {}
      doc.destroy();
    }
  }
}
