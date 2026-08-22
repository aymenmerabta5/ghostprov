import * as crypto from "node:crypto";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { YSweetEnvelope } from "../prism/protocol";
import type { ProjectId } from "./types";

export interface YProviderLike {
  on(event: string, listener: (...args: any[]) => void): void;
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

interface Connection {
  doc: Y.Doc;
  provider: YProviderLike;
  content: Y.Map<any>;
  index: Map<string, { uuid: string; map: Y.Map<any> }>;
  generation: number;
  connectedAtMs: number;
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

export function tombstoneYDocEntry(doc: Y.Doc, map: Y.Map<any>): void {
  doc.transact(() => {
    const content = map.get("content");
    if (content instanceof Y.Text) content.delete(0, content.length);
    else if (content != null) map.set("content", "");
    map.set("deleted", true);
  });
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
    let entry = this.#findEntry(connection, filename);
    if (!entry) {
      await this.writeFile(filename, "");
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
      const chunk = content.slice(offset, offset + chunkBytes);
      connection.doc.transact(() => text.insert(text.length, chunk));
      if (this.#appendDelayMs > 0) await this.#clock.sleep(this.#appendDelayMs);
    }
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
    return operation(connection);
  }

  async #getConnection(): Promise<Connection> {
    if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
    if (this.#connection) return this.#connection;
    if (!this.#connecting) {
      this.#connecting = this.#connectGeneration(++this.#generation)
        .then(connection => {
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
      provider.on("connectionClose", () => finish(false));
    });
    if (!synced || generation !== this.#generation || this.#stopped) {
      try { provider.destroy(); } catch {}
      try { doc.destroy(); } catch {}
      if (this.#stopped) throw new YDocSessionError("Yjs session is stopped", "stopped");
      if (generation !== this.#generation) throw new YDocSessionError("Yjs connection superseded", "sync");
      throw new YDocSessionError(`Yjs sync failed within ${this.#syncTimeoutMs}ms`, "sync");
    }
    const connection: Connection = {
      doc,
      provider,
      content: doc.getMap("content"),
      index: new Map(),
      generation,
      connectedAtMs: this.#clock.now(),
    };
    provider.on("connectionClose", () => {
      if (this.#connection !== connection) return;
      this.#connection = null;
      this.#destroyConnection(connection);
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

  #rootFolderId(connection: Connection): string | null {
    for (const [uuid, value] of connection.content.entries()) {
      const map = value as Y.Map<any>;
      if (map.get("filename") === "root" && map.get("type") === "folder") {
        const id = map.get("id");
        return typeof id === "string" && id ? id : uuid;
      }
    }
    return null;
  }

  #destroyConnection(connection: Connection): void {
    try { connection.provider.destroy(); } catch {}
    try { connection.doc.destroy(); } catch {}
  }
}
