import * as crypto from "node:crypto";
import { Buffer } from "node:buffer";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { YSweetEnvelope } from "../prism/protocol";
import type { ProjectId } from "./types";

export type YProviderEvent = "sync" | "connection-close";

export interface YProviderLike {
  on(event: YProviderEvent, listener: (...args: any[]) => void): void;
  destroy(): void;
}

export interface YProviderFactory {
  create(envelope: YSweetEnvelope, doc: Y.Doc): YProviderLike;
}

export interface SandboxYDocOptions {
  projectId: ProjectId;
  mintYSweet: () => Promise<YSweetEnvelope>;
  providerFactory?: YProviderFactory;
  clock?: Clock;
  syncTimeoutMs?: number;
  appendDelayMs?: number;
}

export interface YDocFileMeta {
  uuid: string;
  filename: string;
  deleted: boolean;
}

export interface YDocSnapshot {
  projectId: ProjectId;
  connected: boolean;
  generation: number;
  entries: number;
  connectedAtMs: number | null;
  stopped: boolean;
}

export interface RenderSyncState {
  clientStateVector: string;
  clientDeleteSetUpdate: string;
}

export type GuardedYDocPathMutation =
  | { readonly kind: "write"; readonly path: string; readonly content: string }
  | { readonly kind: "tombstone"; readonly path: string };

export interface GuardedYDocMutation {
  readonly guards: readonly {
    readonly path: string;
    readonly equals: string | null;
  }[];
  readonly mutations: readonly GuardedYDocPathMutation[];
}

export type YDocSessionErrorKind = "auth" | "sync" | "missing" | "stopped";

export class YDocSessionError extends Error {
  constructor(message: string, readonly kind: YDocSessionErrorKind = "sync") {
    super(message);
    this.name = "YDocSessionError";
  }
}

const DEFAULT_PROVIDER_FACTORY: YProviderFactory = {
  create(envelope, doc) {
    const token = envelope.token.reveal();
    class AuthenticatedWebSocket extends WebSocket {
      constructor(url: string | URL, protocols?: string | string[]) {
        const raw = String(url);
        super(`${raw}${raw.includes("?") ? "&" : "?"}token=${encodeURIComponent(token)}`, protocols);
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
    ) as unknown as YProviderLike;
  },
};

const MAX_GUARDED_PATH_OPERATIONS = 128;
const MAX_GUARDED_PATH_CONTENT_CHARS = 2 * 1024 * 1024;

interface Connection {
  doc: Y.Doc;
  provider: YProviderLike;
  content: Y.Map<any>;
  index: Map<string, { uuid: string; map: Y.Map<any> }>;
  generation: number;
  connectedAtMs: number;
  disconnected: boolean;
  destroyed: boolean;
}

function readContent(map: Y.Map<any>): string | null {
  const value = map.get("content");
  if (value == null) return null;
  if (value instanceof Y.Text) return value.toString();
  if (typeof value === "string") return value;
  return typeof value?.toString === "function" ? value.toString() : null;
}

function writeContent(doc: Y.Doc, map: Y.Map<any>, content: string): void {
  doc.transact(() => {
    const current = map.get("content");
    if (current instanceof Y.Text) {
      current.delete(0, current.length);
      current.insert(0, content);
      return;
    }
    const text = new Y.Text();
    text.insert(0, content);
    map.set("content", text);
  });
}

function deterministicRootId(projectId: ProjectId): string {
  const bytes = crypto.createHash("sha256")
    .update(`ghostprov:root:${projectId}`)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function normalizedPath(value: string): string[] {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 1024 ||
    value.startsWith("/") ||
    value.includes("\\") ||
    value.includes("\0")
  ) {
    throw new YDocSessionError("Project path is invalid", "missing");
  }
  const parts = value.split("/");
  if (
    parts.some(part => (
      part.length < 1 ||
      part.length > 255 ||
      part === "." ||
      part === ".."
    ))
  ) throw new YDocSessionError("Project path is invalid", "missing");
  return parts;
}

function ensureProjectStructure(
  doc: Y.Doc,
  content: Y.Map<any>,
  settings: Y.Map<any>,
  projectId: ProjectId,
): string {
  let rootId: string | null = null;
  for (const [uuid, value] of content.entries()) {
    const map = value as Y.Map<any>;
    if (map.get("filename") !== "root" || map.get("type") !== "folder" || map.get("deleted") === true) continue;
    const id = map.get("id");
    rootId = typeof id === "string" && id ? id : uuid;
    break;
  }
  const uuid = rootId ?? deterministicRootId(projectId);
  doc.transact(() => {
    if (!rootId) {
      const map = new Y.Map();
      map.set("id", uuid);
      map.set("filename", "root");
      map.set("deleted", false);
      map.set("type", "folder");
      content.set(uuid, map);
    }
    if (settings.get("init") !== true) {
      settings.set("init", true);
      settings.set("deleted", false);
    }
  });
  return uuid;
}

export function tombstoneYDocEntry(doc: Y.Doc, map: Y.Map<any>): void {
  doc.transact(() => {
    const content = map.get("content");
    if (content instanceof Y.Text) content.delete(0, content.length);
    else if (content != null) map.set("content", "");
    map.set("deleted", true);
  });
}

function collectYDocTreeTargets(
  content: Y.Map<any>,
  uuid: string,
  map: Y.Map<any>,
): Y.Map<any>[] {
  const targets: Y.Map<any>[] = [];
  const seen = new Set<string>();
  const visit = (entryUuid: string, entry: Y.Map<any>): void => {
    if (seen.has(entryUuid)) return;
    seen.add(entryUuid);
    if (entry.get("type") === "folder") {
      const folderId = String(entry.get("id") ?? entryUuid);
      for (const [childUuid, value] of content.entries()) {
        const child = value as Y.Map<any>;
        if (child.get("deleted") !== true && child.get("inFolder") === folderId) {
          visit(childUuid, child);
        }
      }
    }
    targets.push(entry);
  };
  visit(uuid, map);
  return targets;
}

function tombstoneYDocTree(
  doc: Y.Doc,
  content: Y.Map<any>,
  uuid: string,
  map: Y.Map<any>,
): Y.Map<any>[] {
  const targets = collectYDocTreeTargets(content, uuid, map);
  doc.transact(() => {
    for (const target of targets) {
      const value = target.get("content");
      if (value instanceof Y.Text) value.delete(0, value.length);
      else if (value != null) target.set("content", "");
      target.set("deleted", true);
    }
  });
  return targets;
}

export class SandboxYDoc {
  readonly projectId: ProjectId;
  readonly #mintYSweet: () => Promise<YSweetEnvelope>;
  readonly #providerFactory: YProviderFactory;
  readonly #clock: Clock;
  readonly #syncTimeoutMs: number;
  readonly #appendDelayMs: number;
  #connection: Connection | null = null;
  #connecting: Promise<Connection> | null = null;
  #generation = 0;
  #stopped = false;

  constructor(options: SandboxYDocOptions) {
    this.projectId = options.projectId;
    this.#mintYSweet = options.mintYSweet;
    this.#providerFactory = options.providerFactory ?? DEFAULT_PROVIDER_FACTORY;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#syncTimeoutMs = options.syncTimeoutMs ?? 15_000;
    this.#appendDelayMs = options.appendDelayMs ?? 20;
  }

  async connect(): Promise<void> {
    await this.#getConnection();
  }

  async listFiles(): Promise<YDocFileMeta[]> {
    return this.#withConnection(connection => {
      const files: YDocFileMeta[] = [];
      for (const [uuid, value] of connection.content.entries()) {
        const map = value as Y.Map<any>;
        files.push({
          uuid,
          filename: String(map.get("filename") ?? ""),
          deleted: map.get("deleted") === true,
        });
      }
      return files;
    });
  }

  async ensurePath(path: string): Promise<{ fileId: string; folderId: string }> {
    const parts = normalizedPath(path);
    return this.#withConnection(connection => {
      const root = this.#rootFolderId(connection);
      if (!root) throw new YDocSessionError("root folder not found", "missing");
      let result: { fileId: string; folderId: string } | null = null;
      connection.doc.transact(() => {
        let parentId = root;
        for (const folderName of parts.slice(0, -1)) {
          let folder = this.#findChild(connection, parentId, folderName);
          if (folder && folder.map.get("type") !== "folder") {
            throw new YDocSessionError(`Path component is not a folder: ${folderName}`, "missing");
          }
          if (!folder) {
            const id = crypto.randomUUID();
            const map = new Y.Map();
            map.set("id", id);
            map.set("filename", folderName);
            map.set("deleted", false);
            map.set("inFolder", parentId);
            map.set("type", "folder");
            connection.content.set(id, map);
            folder = { uuid: id, map };
          }
          parentId = String(folder.map.get("id") ?? folder.uuid);
        }

        const filename = parts.at(-1)!;
        const existing = this.#findChild(connection, parentId, filename);
        if (existing?.map.get("type") === "folder") {
          throw new YDocSessionError(`Project path is an existing folder: ${path}`, "missing");
        }
        if (existing) {
          const id = String(existing.map.get("id") ?? existing.uuid);
          const current = readContent(existing.map) ?? "";
          if (!(existing.map.get("content") instanceof Y.Text)) {
            const text = new Y.Text();
            if (current) text.insert(0, current);
            existing.map.set("content", text);
          }
          existing.map.set("type", "text");
          existing.map.set("inFolder", parentId);
          result = { fileId: id, folderId: parentId };
          return;
        }

        const id = crypto.randomUUID();
        const map = new Y.Map();
        map.set("id", id);
        map.set("filename", filename);
        map.set("deleted", false);
        map.set("inFolder", parentId);
        map.set("type", "text");
        map.set("content", new Y.Text());
        connection.content.set(id, map);
        result = { fileId: id, folderId: parentId };
      });
      if (!result) throw new YDocSessionError(`Failed to ensure project path: ${path}`, "missing");
      return result;
    });
  }

  async writePath(path: string, content: string): Promise<void> {
    const ensured = await this.ensurePath(path);
    await this.#withConnection(connection => {
      const map = connection.content.get(ensured.fileId) as Y.Map<any> | undefined;
      if (!map || map.get("deleted") === true) {
        throw new YDocSessionError(`Project path disappeared: ${path}`, "missing");
      }
      writeContent(connection.doc, map, content);
    });
  }

  /**
   * Compare and mutate project paths in one local Y.Doc transaction. Guards
   * are evaluated before any mutation, so a superseded bootstrap generation
   * cannot partially delete or overwrite the current generation's files.
   */
  async compareAndMutatePaths(input: GuardedYDocMutation): Promise<boolean> {
    if (
      !input || !Array.isArray(input.guards) || !Array.isArray(input.mutations) ||
      input.guards.length < 1 || input.guards.length > MAX_GUARDED_PATH_OPERATIONS ||
      input.mutations.length < 1 || input.mutations.length > MAX_GUARDED_PATH_OPERATIONS
    ) throw new YDocSessionError("Guarded project mutation is invalid", "missing");
    const guardPaths = new Set<string>();
    const mutationPaths = new Set<string>();
    let contentChars = 0;
    const guards = input.guards.map(guard => {
      if (
        !guard || typeof guard.path !== "string" ||
        !(guard.equals === null || typeof guard.equals === "string") ||
        guardPaths.has(guard.path)
      ) throw new YDocSessionError("Guarded project mutation guard is invalid", "missing");
      guardPaths.add(guard.path);
      contentChars += guard.equals?.length ?? 0;
      if (!Number.isSafeInteger(contentChars) || contentChars > MAX_GUARDED_PATH_CONTENT_CHARS) {
        throw new YDocSessionError("Guarded project mutation content is too large", "missing");
      }
      return { ...guard, parts: normalizedPath(guard.path) };
    });
    const mutations = input.mutations.map(mutation => {
      if (
        !mutation || typeof mutation.path !== "string" ||
        (mutation.kind !== "write" && mutation.kind !== "tombstone") ||
        mutationPaths.has(mutation.path)
      ) throw new YDocSessionError("Guarded project path mutation is invalid", "missing");
      mutationPaths.add(mutation.path);
      if (mutation.kind === "write") {
        if (typeof mutation.content !== "string") {
          throw new YDocSessionError("Guarded project write content is invalid", "missing");
        }
        contentChars += mutation.content.length;
        if (!Number.isSafeInteger(contentChars) || contentChars > MAX_GUARDED_PATH_CONTENT_CHARS) {
          throw new YDocSessionError("Guarded project mutation content is too large", "missing");
        }
      }
      return { ...mutation, parts: normalizedPath(mutation.path) };
    });
    for (let left = 0; left < mutations.length; left++) {
      for (let right = left + 1; right < mutations.length; right++) {
        const a = mutations[left]!.parts;
        const b = mutations[right]!.parts;
        const prefix = (shorter: readonly string[], longer: readonly string[]) =>
          shorter.length < longer.length &&
          shorter.every((part, index) => longer[index] === part);
        if (prefix(a, b) || prefix(b, a)) {
          throw new YDocSessionError(
            "Guarded project mutations contain overlapping paths",
            "missing",
          );
        }
      }
    }

    return this.#withConnection(connection => {
      for (const guard of guards) {
        const entry = this.#findPathNode(connection, guard.parts);
        if (entry?.map.get("type") === "folder") return false;
        const current = entry ? readContent(entry.map) : null;
        if (current !== guard.equals) return false;
      }

      type PlannedEntry = {
        uuid: string;
        id: string;
        type: "folder" | "text";
        map: Y.Map<any>;
      };
      const root = this.#rootFolderId(connection);
      if (!root) throw new YDocSessionError("root folder not found", "missing");
      const plannedChildren = new Map<string, PlannedEntry>();
      const plannedEntries: PlannedEntry[] = [];
      const existingWrites: Array<{
        entry: PlannedEntry;
        parentId: string;
        content: string;
        integratedText: Y.Text | null;
      }> = [];
      const tombstones = new Set<Y.Map<any>>();
      const childKey = (parentId: string, filename: string) => `${parentId}\0${filename}`;
      const child = (parentId: string, filename: string): PlannedEntry | null => {
        const planned = plannedChildren.get(childKey(parentId, filename));
        if (planned) return planned;
        const existing = this.#findChild(connection, parentId, filename);
        if (!existing) return null;
        return {
          ...existing,
          id: String(existing.map.get("id") ?? existing.uuid),
          type: existing.map.get("type") === "folder" ? "folder" : "text",
        };
      };
      const detachedText = (content: string): Y.Text => {
        const text = new Y.Text();
        if (content) text.insert(0, content);
        return text;
      };
      const plannedEntry = (
        parentId: string,
        filename: string,
        type: "folder" | "text",
        content?: string,
      ): PlannedEntry => {
        const uuid = crypto.randomUUID();
        const map = new Y.Map();
        map.set("id", uuid);
        map.set("filename", filename);
        map.set("deleted", false);
        map.set("inFolder", parentId);
        map.set("type", type);
        if (type === "text") map.set("content", detachedText(content ?? ""));
        const entry = { uuid, id: uuid, type, map };
        plannedChildren.set(childKey(parentId, filename), entry);
        plannedEntries.push(entry);
        return entry;
      };

      // Build the complete semantic plan without touching the connected doc.
      // Any later collision therefore fails before an earlier write is visible.
      for (const mutation of mutations) {
        if (mutation.kind === "tombstone") {
          const entry = this.#findPathNode(connection, mutation.parts);
          if (entry) {
            for (const target of collectYDocTreeTargets(
              connection.content,
              entry.uuid,
              entry.map,
            )) tombstones.add(target);
          }
          continue;
        }

        let parentId = root;
        for (const folderName of mutation.parts.slice(0, -1)) {
          let folder = child(parentId, folderName);
          if (folder && folder.type !== "folder") {
            throw new YDocSessionError(
              `Path component is not a folder: ${folderName}`,
              "missing",
            );
          }
          folder ??= plannedEntry(parentId, folderName, "folder");
          parentId = folder.id;
        }

        const filename = mutation.parts.at(-1)!;
        const existing = child(parentId, filename);
        if (existing?.type === "folder") {
          throw new YDocSessionError(
            `Project path is an existing folder: ${mutation.path}`,
            "missing",
          );
        }
        if (existing) {
          const currentContent = existing.map.get("content");
          existingWrites.push({
            entry: existing,
            parentId,
            content: mutation.content,
            integratedText: currentContent instanceof Y.Text ? currentContent : null,
          });
        } else {
          plannedEntry(parentId, filename, "text", mutation.content);
        }
      }

      connection.doc.transact(() => {
        for (const entry of plannedEntries) {
          connection.content.set(entry.uuid, entry.map);
        }
        for (const write of existingWrites) {
          if (typeof write.entry.map.get("id") !== "string") {
            write.entry.map.set("id", write.entry.uuid);
          }
          write.entry.map.set("deleted", false);
          write.entry.map.set("inFolder", write.parentId);
          write.entry.map.set("type", "text");
          if (write.integratedText) {
            write.integratedText.delete(0, write.integratedText.length);
            if (write.content) write.integratedText.insert(0, write.content);
          } else {
            // Legacy scalar content has no shared-type identity to preserve.
            // New Y.Text instances are reserved for newly planned files.
            write.entry.map.set("content", write.content);
          }
        }
        for (const target of tombstones) {
          const content = target.get("content");
          if (content instanceof Y.Text) content.delete(0, content.length);
          else if (content != null) target.set("content", "");
          target.set("deleted", true);
          const filename = target.get("filename");
          if (typeof filename === "string") connection.index.delete(filename);
        }
      });
      return true;
    });
  }

  async readPath(path: string): Promise<string | null> {
    const parts = normalizedPath(path);
    return this.#withConnection(connection => {
      const root = this.#rootFolderId(connection);
      if (!root) return null;
      let parentId = root;
      for (let index = 0; index < parts.length; index++) {
        const component = parts[index];
        const child = this.#findChild(connection, parentId, component);
        if (!child) return null;
        if (index < parts.length - 1) {
          if (child.map.get("type") !== "folder") return null;
          parentId = String(child.map.get("id") ?? child.uuid);
          continue;
        }
        if (child.map.get("type") === "folder") return null;
        return readContent(child.map);
      }
      return null;
    });
  }

  async tombstonePath(path: string): Promise<boolean> {
    const parts = normalizedPath(path);
    return this.#withConnection(connection => {
      const root = this.#rootFolderId(connection);
      if (!root) return false;
      let parentId = root;
      for (let index = 0; index < parts.length; index++) {
        const component = parts[index];
        const child = this.#findChild(connection, parentId, component);
        if (!child) return false;
        if (index < parts.length - 1) {
          if (child.map.get("type") !== "folder") return false;
          parentId = String(child.map.get("id") ?? child.uuid);
          continue;
        }
        const targets = tombstoneYDocTree(
          connection.doc,
          connection.content,
          child.uuid,
          child.map,
        );
        for (const target of targets) {
          const filename = target.get("filename");
          if (typeof filename === "string") connection.index.delete(filename);
        }
        return true;
      }
      return false;
    });
  }

  async readFile(filename: string): Promise<string | null> {
    return this.#withConnection(connection => {
      const entry = this.#findEntry(connection, filename);
      return entry ? readContent(entry.map) : null;
    });
  }

  async writeFile(
    filename: string,
    content: string,
    options?: { folderId?: string },
  ): Promise<void> {
    await this.#withConnection(connection => {
      const existing = this.#findEntry(connection, filename);
      if (existing) {
        writeContent(connection.doc, existing.map, content);
        return;
      }
      const folderId = options?.folderId ?? this.#rootFolderId(connection);
      if (!folderId) throw new YDocSessionError("root folder not found", "missing");
      const uuid = crypto.randomUUID();
      const map = new Y.Map();
      connection.doc.transact(() => {
        map.set("id", uuid);
        map.set("filename", filename);
        map.set("deleted", false);
        map.set("inFolder", folderId);
        map.set("type", "text");
        const text = new Y.Text();
        text.insert(0, content);
        map.set("content", text);
        connection.content.set(uuid, map);
      });
      connection.index.set(filename, { uuid, map });
    });
  }

  async appendFile(filename: string, content: string, chunkBytes = 256 * 1024): Promise<void> {
    if (!Number.isInteger(chunkBytes) || chunkBytes < 1) throw new RangeError("chunkBytes must be positive");
    const connection = await this.#getConnection();
    this.#assertConnectionActive(connection);
    let entry = this.#findEntry(connection, filename);
    if (!entry) {
      await this.writeFile(filename, "");
      this.#assertConnectionActive(connection);
      entry = this.#findEntry(connection, filename);
    }
    if (!entry) throw new YDocSessionError(`entry vanished after create: ${filename}`);
    let text = entry.map.get("content");
    if (!(text instanceof Y.Text)) {
      text = new Y.Text();
      entry.map.set("content", text);
    }
    connection.doc.transact(() => text.delete(0, text.length));
    for (let offset = 0; offset < content.length; offset += chunkBytes) {
      this.#assertConnectionActive(connection);
      const chunk = content.slice(offset, offset + chunkBytes);
      connection.doc.transact(() => text.insert(text.length, chunk));
      if (this.#appendDelayMs > 0) await this.#clock.sleep(this.#appendDelayMs);
    }
    this.#assertConnectionActive(connection);
  }

  async deleteFile(filename: string): Promise<boolean> {
    return this.#withConnection(connection => {
      const entry = this.#findEntry(connection, filename);
      if (!entry) return false;
      tombstoneYDocEntry(connection.doc, entry.map);
      connection.index.delete(filename);
      return true;
    });
  }

  async ensureFiles(filenames: readonly string[]): Promise<void> {
    const names = [...new Set(filenames)];
    if (names.some(name => typeof name !== "string" || !name.trim())) {
      throw new TypeError("Tracked filenames must be non-empty strings");
    }
    await this.#withConnection(connection => {
      const folderId = this.#rootFolderId(connection);
      if (!folderId) throw new YDocSessionError("root folder not found", "missing");
      const creations: Array<{ uuid: string; filename: string; map: Y.Map<any> }> = [];
      const normalizations: Array<{ uuid: string; map: Y.Map<any>; content: string }> = [];
      for (const filename of names) {
        const existing = this.#findEntry(connection, filename);
        if (existing) {
          if (
            existing.map.get("type") !== "text" ||
            existing.map.get("inFolder") !== folderId ||
            !(existing.map.get("content") instanceof Y.Text)
          ) {
            normalizations.push({
              uuid: existing.uuid,
              map: existing.map,
              content: readContent(existing.map) ?? "",
            });
          }
          continue;
        }
        creations.push({ uuid: crypto.randomUUID(), filename, map: new Y.Map() });
      }
      if (!creations.length && !normalizations.length) return;
      connection.doc.transact(() => {
        for (const { uuid, map, content } of normalizations) {
          if (typeof map.get("id") !== "string" || !map.get("id")) map.set("id", uuid);
          map.set("inFolder", folderId);
          map.set("type", "text");
          if (!(map.get("content") instanceof Y.Text)) {
            const text = new Y.Text();
            if (content) text.insert(0, content);
            map.set("content", text);
          }
        }
        for (const { uuid, filename, map } of creations) {
          map.set("id", uuid);
          map.set("filename", filename);
          map.set("deleted", false);
          map.set("inFolder", folderId);
          map.set("type", "text");
          map.set("content", new Y.Text());
          connection.content.set(uuid, map);
        }
      });
      for (const { uuid, filename, map } of creations) {
        connection.index.set(filename, { uuid, map });
      }
    });
  }

  async renderSyncState(): Promise<RenderSyncState> {
    return this.#withConnection(connection => {
      const stateVector = Y.encodeStateVector(connection.doc);
      const deleteSetUpdate = Y.encodeStateAsUpdate(connection.doc, stateVector);
      return {
        clientStateVector: Buffer.from(stateVector).toString("base64"),
        clientDeleteSetUpdate: Buffer.from(deleteSetUpdate).toString("base64"),
      };
    });
  }

  async ensureFolder(name: string): Promise<string> {
    return this.#withConnection(connection => {
      for (const [uuid, value] of connection.content.entries()) {
        const map = value as Y.Map<any>;
        if (map.get("type") === "folder" && map.get("filename") === name && map.get("deleted") !== true) {
          const id = map.get("id");
          if (typeof id === "string" && id) return id;
          return uuid;
        }
      }
      const root = this.#rootFolderId(connection);
      if (!root) throw new YDocSessionError("root folder not found", "missing");
      const uuid = crypto.randomUUID();
      const map = new Y.Map();
      connection.doc.transact(() => {
        map.set("id", uuid);
        map.set("filename", name);
        map.set("deleted", false);
        map.set("inFolder", root);
        map.set("type", "folder");
        connection.content.set(uuid, map);
      });
      connection.index.set(name, { uuid, map });
      return uuid;
    });
  }

  async waitForFile(
    filename: string,
    predicate: (content: string | null) => boolean,
    timeoutMs: number,
  ): Promise<string> {
    const deadline = this.#clock.now() + timeoutMs;
    for (;;) {
      const content = await this.readFile(filename).catch(() => null);
      if (predicate(content)) return content as string;
      if (this.#clock.now() >= deadline) {
        throw new YDocSessionError(`waitForFile timeout on ${filename}`, "sync");
      }
      await this.#clock.sleep(Math.min(120, deadline - this.#clock.now()));
    }
  }

  snapshot(): YDocSnapshot {
    return {
      projectId: this.projectId,
      connected: this.#connection !== null,
      generation: this.#generation,
      entries: this.#connection?.content.size ?? 0,
      connectedAtMs: this.#connection?.connectedAtMs ?? null,
      stopped: this.#stopped,
    };
  }

  async destroy(): Promise<void> {
    this.#stopped = true;
    this.#generation++;
    const connection = this.#connection;
    this.#connection = null;
    this.#connecting = null;
    if (connection) this.#destroyConnection(connection);
  }

  async #withConnection<T>(operation: (connection: Connection) => T | Promise<T>): Promise<T> {
    const connection = await this.#getConnection();
    this.#assertConnectionActive(connection);
    const result = await operation(connection);
    this.#assertConnectionActive(connection);
    return result;
  }

  #assertConnectionActive(connection: Connection): void {
    if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
    if (
      connection.disconnected ||
      connection.destroyed ||
      connection.generation !== this.#generation ||
      this.#connection !== connection
    ) {
      throw new YDocSessionError("Yjs connection closed during operation", "sync");
    }
  }

  async #getConnection(): Promise<Connection> {
    if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
    if (this.#connection) return this.#connection;
    if (!this.#connecting) {
      this.#connecting = this.#connectGeneration(++this.#generation)
        .then(connection => {
          if (connection.disconnected || connection.generation !== this.#generation || this.#stopped) {
            this.#destroyConnection(connection);
            if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
            throw new YDocSessionError("Yjs connection closed before activation", "sync");
          }
          this.#connection = connection;
          return connection;
        })
        .finally(() => { this.#connecting = null; });
    }
    return this.#connecting;
  }

  async #connectGeneration(generation: number): Promise<Connection> {
    let envelope: YSweetEnvelope;
    try {
      envelope = await this.#mintYSweet();
    } catch (error) {
      throw new YDocSessionError(`Y-Sweet mint failed: ${error instanceof Error ? error.message : String(error)}`, "auth");
    }
    if (envelope.docId !== this.projectId) {
      throw new YDocSessionError("Y-Sweet envelope belongs to another project", "auth");
    }
    const doc = new Y.Doc({ guid: this.projectId });
    const provider = this.#providerFactory.create(envelope, doc);
    let providerClosed = false;
    const synced = await new Promise<boolean>(resolve => {
      const timer = setTimeout(() => resolve(false), this.#syncTimeoutMs);
      let settled = false;
      const finish = (value: boolean) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(value);
      };
      provider.on("sync", value => { if (value === true) finish(true); });
      provider.on("connection-close", () => {
        // Promise resolution resumes on a later microtask. Preserve a close
        // emitted synchronously after sync so a dead provider is never
        // installed as the active generation in that gap.
        providerClosed = true;
        finish(false);
      });
    });
    if (!synced || providerClosed || generation !== this.#generation || this.#stopped) {
      try { provider.destroy(); } catch {}
      try { doc.destroy(); } catch {}
      if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
      if (generation !== this.#generation) throw new YDocSessionError("Yjs connection superseded", "sync");
      if (providerClosed) throw new YDocSessionError("Yjs connection closed before synchronization", "sync");
      throw new YDocSessionError(`Yjs sync failed within ${this.#syncTimeoutMs}ms`, "sync");
    }
    const content = doc.getMap("content");
    const settings = doc.getMap("settings");
    ensureProjectStructure(doc, content, settings, this.projectId);
    const connection: Connection = {
      doc,
      provider,
      content,
      index: new Map(),
      generation,
      connectedAtMs: this.#clock.now(),
      disconnected: false,
      destroyed: false,
    };
    provider.on("connection-close", () => {
      connection.disconnected = true;
      if (this.#connection === connection) this.#connection = null;
      // y-websocket emits connection-close before it clears its current socket.
      // Destroying it inline would recursively re-enter that same close path.
      queueMicrotask(() => this.#destroyConnection(connection));
    });
    return connection;
  }

  #findEntry(connection: Connection, filename: string): { uuid: string; map: Y.Map<any> } | null {
    const cached = connection.index.get(filename);
    if (cached && cached.map.get("deleted") !== true) return cached;
    for (const [uuid, value] of connection.content.entries()) {
      const map = value as Y.Map<any>;
      if (map.get("filename") === filename && map.get("deleted") !== true) {
        const entry = { uuid, map };
        connection.index.set(filename, entry);
        return entry;
      }
    }
    return null;
  }

  #findPathNode(
    connection: Connection,
    parts: readonly string[],
  ): { uuid: string; map: Y.Map<any> } | null {
    const root = this.#rootFolderId(connection);
    if (!root) return null;
    let parentId = root;
    for (let index = 0; index < parts.length; index++) {
      const child = this.#findChild(connection, parentId, parts[index]!);
      if (!child) return null;
      if (index === parts.length - 1) return child;
      if (child.map.get("type") !== "folder") return null;
      parentId = String(child.map.get("id") ?? child.uuid);
    }
    return null;
  }

  #findChild(
    connection: Connection,
    parentId: string,
    filename: string,
  ): { uuid: string; map: Y.Map<any> } | null {
    for (const [uuid, value] of connection.content.entries()) {
      const map = value as Y.Map<any>;
      if (
        map.get("deleted") !== true &&
        map.get("inFolder") === parentId &&
        map.get("filename") === filename
      ) return { uuid, map };
    }
    return null;
  }

  #rootFolderId(connection: Connection): string | null {
    for (const [uuid, value] of connection.content.entries()) {
      const map = value as Y.Map<any>;
      if (map.get("filename") === "root" && map.get("type") === "folder" && map.get("deleted") !== true) {
        const id = map.get("id");
        return typeof id === "string" && id ? id : uuid;
      }
    }
    return null;
  }

  #destroyConnection(connection: Connection): void {
    if (connection.destroyed) return;
    connection.destroyed = true;
    connection.disconnected = true;
    try { connection.provider.destroy(); } catch {}
    try { connection.doc.destroy(); } catch {}
  }
}
