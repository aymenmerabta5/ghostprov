/**
 * Persistent Node-side Yjs doc client.
 *
 * The Prism doc (wss://prism.openai.com/y/d/<docId>/ws) is the durable
 * server-side store: every entry survives sandbox restarts/reprovisions and
 * the sandbox provider maps tracked entries to /workspace disk files
 * bidirectionally (doc→disk on write, disk→doc when the sandbox writes).
 *
 * One multiplexed WebSocket connection serves ALL slots — no per-request
 * connections, no zip polling, real-time observers for response streams.
 *
 * Doc layout ("content" Y.Map keyed by UUID → Y.Map):
 *   { id, filename, type:"text", deleted:false, inFolder:<root-folder-uuid>,
 *     content: Y.Text }
 *
 * Auth: ySweet token minted via POST /api/y through the live bridge page
 * (Cloudflare blocks raw fetch). Tokens expire — re-mint + reconnect on any
 * sync failure/close. Sandbox tokens are unrelated to this path.
 */
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import ws from "ws";
import * as crypto from "node:crypto";
import * as fs from "node:fs";

const TOKEN_FILE = "D:/MyWork/AI/ghostprov/.pw/tokens.json";

if (!(globalThis as any).WebSocket) (globalThis as any).WebSocket = ws;

export interface DocFileMeta {
  uuid: string;
  filename: string;
  deleted: boolean;
}

type ProviderLike = {
  on: (ev: string, cb: (...a: any[]) => void) => void;
  destroy: () => void;
  wsconnected?: boolean;
};

export class YDocError extends Error {
  constructor(message: string, readonly kind: "auth" | "sync" | "missing" = "sync") {
    super(message);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Mint a fresh ySweet token through the bridge page; fall back to cache. */
async function mintYSweetToken(): Promise<string> {
  // 1. live mint via in-page fetch (bypasses Cloudflare JA3 binding)
  try {
    const gateway: any = await import("./gateway");
    if (gateway.getBridgePage?.()) {
      const res = await gateway.pageFetch("/api/y", {
        method: "POST",
        json: { docId: docId(), requestContext: { source: "ghostprov-ydoc" } },
      });
      if (res.status === 200) {
        const j = JSON.parse(res.text);
        if (typeof j?.token === "string" && j.token.length > 20) {
          rememberToken(j.token);
          return j.token;
        }
      }
      console.warn(`[ydoc] POST /api/y -> ${res.status}; falling back to cached token`);
    }
  } catch {}
  // 2. bridge memory
  try {
    const bridge: any = await import("../bridge/playwright");
    const t = bridge.getYSweetToken?.() ?? bridge.getBridgeTokens?.()?.ySweetToken;
    if (typeof t === "string" && t.length > 20 && !t.includes("mock")) return t;
  } catch {}
  // 3. cached file
  try {
    const j = JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8"));
    if (typeof j?.ySweetToken === "string" && j.ySweetToken.length > 20) return j.ySweetToken;
  } catch {}
  throw new YDocError("no ySweet token available (bridge down, no cache)", "auth");
}

function docId(): string {
  const id = (process.env.PRISM_DOC_ID ?? "").trim();
  if (!id) throw new YDocError("PRISM_DOC_ID not set", "auth");
  return id;
}

function rememberToken(token: string): void {
  (globalThis as any).__GP_YSWEET__ = { token, at: Date.now() };
  try {
    let j: any = {};
    try { j = JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8")); } catch {}
    j.ySweetToken = token;
    j.updatedAt = Date.now();
    fs.mkdirSync("D:/MyWork/AI/ghostprov/.pw", { recursive: true });
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(j, null, 2));
  } catch {}
}

interface Conn {
  doc: Y.Doc;
  provider: ProviderLike;
  contentMap: Y.Map<any>;
  connectedAt: number;
  generation: number;
}

let conn: Conn | null = null;
let connecting: Promise<Conn> | null = null;
let generation = 0;

async function connect(): Promise<Conn> {
  const myGen = ++generation;
  const token = await mintYSweetToken();
  const doc = new Y.Doc();
  const id = docId();
  const provider = new WebsocketProvider(
    `wss://prism.openai.com/y/d/${id}/ws`,
    id,
    doc,
    {
      connect: true,
      WebSocketPolyfill: class extends ws {
        constructor(url: any, protocols?: any) {
          super(`${url}?token=${token}`, protocols);
        }
      },
    } as any,
  ) as unknown as ProviderLike;

  const ok = await new Promise<boolean>((resolve) => {
    const t = setTimeout(() => resolve(false), 15_000);
    provider.on("sync", (s: boolean) => {
      if (s) { clearTimeout(t); resolve(true); }
    });
    provider.on("connectionClose", () => { /* handled by reconnect logic */ });
  });
  if (!ok) {
    try { provider.destroy(); } catch {}
    try { doc.destroy(); } catch {}
    throw new YDocError("yjs ws sync failed within 15s", "sync");
  }
  if (myGen !== generation) {
    // superseded by a newer connect attempt
    try { provider.destroy(); } catch {}
    try { doc.destroy(); } catch {}
    throw new YDocError("connection superseded", "sync");
  }
  const c: Conn = {
    doc,
    provider,
    contentMap: doc.getMap("content"),
    connectedAt: Date.now(),
    generation: myGen,
  };
  // watch for disconnects → invalidate so next call reconnects with fresh token
  provider.on("connectionClose", () => {
    if (conn === c) conn = null;
    try { c.doc.destroy(); } catch {}
  });
  console.log(`[ydoc] connected (gen ${myGen}, entries=${c.contentMap.size})`);
  return c;
}

/** Get the live connection, (re)connecting when missing/dead. */
async function getConn(forceNew = false): Promise<Conn> {
  if (forceNew) { generation++; conn = null; }
  if (conn) return conn;
  if (!connecting) {
    connecting = connect()
      .then((c) => { conn = c; return c; })
      .finally(() => { connecting = null; });
  }
  return connecting;
}

/**
 * Run `fn` against a healthy connection. On auth/sync failure retries once
 * with a forced reconnect (fresh ySweet token — tokens expire frequently).
 */
async function withConn<T>(fn: (c: Conn) => Promise<T>): Promise<T> {
  let lastErr: unknown = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    let c: Conn;
    try {
      c = await getConn(attempt > 0);
    } catch (e) {
      lastErr = e;
      await sleep(1000 * (attempt + 1));
      continue;
    }
    try {
      return await fn(c);
    } catch (e: any) {
      lastErr = e;
      if (e instanceof YDocError && e.kind === "auth") {
        console.warn("[ydoc] auth failure — forcing reconnect with fresh token…");
        continue;
      }
      throw e;
    }
  }
  throw lastErr instanceof Error ? lastErr : new YDocError(String(lastErr));
}

function findEntry(c: Conn, filename: string): { uuid: string; ymap: Y.Map<any> } | null {
  // O(1) name index per connection (rebuilt on reconnect; absorbs externally
  // created entries via fallback rescan so we never double-create).
  let ix = connIndex.get(c.doc);
  if (!ix) {
    ix = new Map();
    connIndex.set(c.doc, ix);
    for (const [uuid, val] of c.contentMap.entries()) {
      const m = val as Y.Map<any>;
      const fn = m.get("filename");
      if (typeof fn === "string" && m.get("deleted") !== true) ix.set(fn, { uuid, ymap: m });
    }
  }
  const hit = ix.get(filename);
  if (hit) return hit;
  for (const [uuid, val] of c.contentMap.entries()) {
    const m = val as Y.Map<any>;
    if (m.get("filename") === filename && m.get("deleted") !== true) {
      const found = { uuid, ymap: m };
      ix.set(filename, found);
      return found;
    }
  }
  return null;
}

/** Register a newly-created entry so subsequent lookups are O(1). */
function indexEntry(c: Conn, filename: string, ref: { uuid: string; ymap: Y.Map<any> }): void {
  let ix = connIndex.get(c.doc);
  if (!ix) { ix = new Map(); connIndex.set(c.doc, ix); }
  ix.set(filename, ref);
}

const connIndex = new WeakMap<Y.Doc, Map<string, { uuid: string; ymap: Y.Map<any> }>>();

function readContent(ymap: Y.Map<any>): string | null {
  const v = ymap.get("content");
  if (v == null) return null;
  if (Y.Text && v instanceof Y.Text) return v.toString();
  if (typeof v === "string") return v;
  if (typeof v?.toString === "function") return v.toString();
  return null;
}

/** Set content handling both Y.Text values and plain strings. */
function writeContent(doc: Y.Doc, ymap: Y.Map<any>, content: string): void {
  doc.transact(() => {
    const cur = ymap.get("content");
    if (cur instanceof Y.Text) {
      cur.delete(0, cur.length);
      cur.insert(0, content);
    } else {
      const t = new Y.Text();
      t.insert(0, content);
      ymap.set("content", t);
    }
  });
}

function rootFolderId(c: Conn): string | null {
  for (const [, val] of c.contentMap.entries()) {
    const m = val as Y.Map<any>;
    if (m.get("filename") === "root" && m.get("type") === "folder") return m.get("id");
  }
  return null;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export async function listDocFiles(): Promise<DocFileMeta[]> {
  return withConn(async (c) => {
    const out: DocFileMeta[] = [];
    for (const [uuid, val] of c.contentMap.entries()) {
      const m = val as Y.Map<any>;
      out.push({ uuid, filename: String(m.get("filename") ?? ""), deleted: m.get("deleted") === true });
    }
    return out;
  });
}

export async function readDocFile(filename: string): Promise<string | null> {
  return withConn(async (c) => {
    const e = findEntry(c, filename);
    return e ? readContent(e.ymap) : null;
  });
}

/** Create-or-update a tracked file's content in the doc.
 * opts.folderId places NEW entries inside a folder (bare basename filename). */
export async function writeDocFile(
  filename: string,
  content: string,
  opts?: { folderId?: string },
): Promise<void> {
  await withConn(async (c) => {
    const e = findEntry(c, filename);
    if (e) {
      writeContent(c.doc, e.ymap, content);
      return;
    }
    const folder = opts?.folderId ?? rootFolderId(c);
    if (!folder) throw new YDocError("root folder not found in doc", "sync");
    const uuid = crypto.randomUUID();
    const ymap = new Y.Map();
    c.doc.transact(() => {
      ymap.set("id", uuid);
      ymap.set("filename", filename);
      ymap.set("deleted", false);
      ymap.set("inFolder", folder);
      ymap.set("type", "text");
      const t = new Y.Text();
      t.insert(0, content);
      ymap.set("content", t);
    });
    c.contentMap.set(uuid, ymap);
    indexEntry(c, filename, { uuid, ymap });
  });
}

/** Find or create a folder entry by name (child of root). Returns its uuid. */
export async function ensureDocFolder(name: string): Promise<string> {
  return withConn(async (c) => {
    for (const [, val] of c.contentMap.entries()) {
      const m = val as Y.Map<any>;
      if (m.get("type") === "folder" && m.get("filename") === name && m.get("deleted") !== true) {
        const id = m.get("id");
        if (typeof id === "string" && id) return id;
      }
    }
    const root = rootFolderId(c);
    if (!root) throw new YDocError("root folder not found in doc", "sync");
    const uuid = crypto.randomUUID();
    const ymap = new Y.Map();
    c.doc.transact(() => {
      ymap.set("id", uuid);
      ymap.set("filename", name);
      ymap.set("deleted", false);
      ymap.set("inFolder", root);
      ymap.set("type", "folder");
    });
    c.contentMap.set(uuid, ymap);
    indexEntry(c, name, { uuid, ymap });
    console.log(`[ydoc] created folder "${name}"`);
    return uuid;
  });
}

/**
 * Large-content write: clear once, then append in small slices as SEPARATE
 * transactions. A monolithic multi-MB insert goes out as one giant Yjs
 * update which the Prism backend silently truncates/drops past a moving
 * internal limit (~2.4-3.5MB observed) — the doc then converges to a
 * PERMANENTLY truncated file (deterministic, unfixable by retries).
 * Small sequential updates have never failed; this keeps every update tiny.
 */
export async function writeDocFileAppended(
  filename: string,
  content: string,
  chunkSize = Number(process.env.GP_APPEND_CHUNK ?? 256 * 1024),
): Promise<void> {
  await withConn(async (c) => {
    let e = findEntry(c, filename);
    if (!e) {
      const folder = rootFolderId(c);
      if (!folder) throw new YDocError("root folder not found in doc", "sync");
      const uuid = crypto.randomUUID();
      const ymap = new Y.Map();
      c.doc.transact(() => {
        ymap.set("id", uuid);
        ymap.set("filename", filename);
        ymap.set("deleted", false);
        ymap.set("inFolder", folder);
        ymap.set("type", "text");
        ymap.set("content", new Y.Text());
      });
      c.contentMap.set(uuid, ymap);
      e = findEntry(c, filename);
      if (!e) throw new YDocError(`entry vanished after create: ${filename}`, "sync");
    }
    const cur = e.ymap.get("content");
    const ytext = cur instanceof Y.Text ? cur : new Y.Text();
    if (!(cur instanceof Y.Text)) e.ymap.set("content", ytext);
    // phase 1: clear (one small update regardless of previous size)
    c.doc.transact(() => { ytext.delete(0, ytext.length); });
    await sleep(40);
    // phase 2: append slices — each its own transaction => small updates
    for (let off = 0; off < content.length; off += chunkSize) {
      const slice = content.slice(off, off + chunkSize);
      if (!slice) break;
      c.doc.transact(() => { ytext.insert(ytext.length, slice); });
      await sleep(20); // let the provider drain between updates
    }
  });
}

/**
 * Wait until the local doc view of `filename` satisfies `pred`.
 * Local echo is instant for our own writes; for daemon-written files the
 * update arrives via WS push — no polling round-trips.
 */
export async function waitForDoc(
  filename: string,
  pred: (content: string | null) => boolean,
  timeoutMs: number,
  onPoll?: (content: string | null) => void,
): Promise<string> {
  const t0 = Date.now();
  for (;;) {
    const content = await readDocFile(filename).catch(() => null);
    onPoll?.(content);
    if (pred(content)) return content as string;
    if (Date.now() - t0 > timeoutMs) throw new YDocError(`waitForDoc timeout on ${filename} after ${timeoutMs}ms`, "sync");
    await sleep(120);
  }
}

export async function isDocConnected(): Promise<boolean> {
  try {
    await getConn();
    return true;
  } catch {
    return false;
  }
}

/** Test hook + shutdown. */
export async function destroyDocConn(): Promise<void> {
  const c = conn;
  conn = null;
  if (c) {
    try { c.provider.destroy(); } catch {}
    try { c.doc.destroy(); } catch {}
  }
}
