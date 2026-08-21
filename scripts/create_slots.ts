/**
 * Create slot files for parallel agent support.
 * Creates job_s1..job_sN and resp_s1..resp_sN in the Yjs doc.
 */
import "dotenv/config";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const token = process.env.GP_FRESH_TOKEN ?? "";
const NUM_SLOTS = parseInt(process.env.GP_SLOTS ?? "12");

function uuid(): string {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

const doc = new Y.Doc();
const provider = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`,
  docId,
  doc,
  {
    connect: true,
    WebSocketPolyfill: class extends (globalThis as any).WebSocket {
      constructor(url: any, protocols?: any) { super(`${url}?token=${token}`, protocols); }
    },
  } as any,
);

await new Promise<boolean>((resolve) => {
  const t = setTimeout(() => resolve(false), 20000);
  provider.on("sync", (s: boolean) => { if (s) { clearTimeout(t); resolve(true); } });
});
console.log("synced ✓");

const contentType = doc.share.get("content") as any;
if (!contentType?._map) { console.error("no content map"); process.exit(1); }

// find root folder id
let rootFolderId = "";
for (const [, item] of contentType._map.entries()) {
  const inner = item?.content?.type;
  if (inner && typeof inner.get === "function") {
    if (inner.get("filename") === "root" && inner.get("type") === "folder") {
      rootFolderId = inner.get("id");
      break;
    }
  }
}
console.log("root folder:", rootFolderId);

// find existing slot files to avoid duplicates
const existing = new Set<string>();
for (const [, item] of contentType._map.entries()) {
  const inner = item?.content?.type;
  if (inner && typeof inner.get === "function") {
    existing.add(inner.get("filename"));
  }
}

// create slots
let created = 0;
const contentMap = doc.getMap("content"); // proper Y.Map wrapper with .set()
for (let i = 1; i <= NUM_SLOTS; i++) {
  for (const prefix of ["job", "resp"]) {
    const filename = `${prefix}_s${i}.json`;
    if (existing.has(filename)) { console.log(`skip ${filename} (exists)`); continue; }
    const id = uuid();
    const ymap = new Y.Map();
    doc.transact(() => {
      ymap.set("id", id);
      ymap.set("filename", filename);
      ymap.set("deleted", false);
      ymap.set("inFolder", rootFolderId);
      ymap.set("type", "text");
      ymap.set("content", "");
    });
    contentMap.set(id, ymap);
    created++;
    console.log(`created ${filename} (${id})`);
  }
}

console.log(`\ncreated ${created} files total`);

// broadcast update
const update = Y.encodeStateAsUpdate(doc);
const enc = (await import("lib0/encoding")).createEncoder();
(await import("lib0/encoding")).writeVarUint(enc, 0);
(await import("lib0/encoding")).writeVarUint(enc, 1);
(await import("lib0/encoding")).writeVarUint8Array(enc, update);

// access the underlying ws from provider
const ws = (provider as any).ws;
if (ws && ws.readyState === 1) {
  ws.send(encoding.toUint8Array(enc));
  console.log("update broadcast to server");
} else {
  console.warn("ws not ready — relying on provider auto-sync");
}

// wait for propagation then verify via /zip
console.log("\nwaiting 5s for sandbox sync…");
await new Promise(r => setTimeout(r, 5000));

try {
  const r = await fetch(`https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${Date.now()}`, {
    headers: { "x-crixet-sandbox-token": process.env.GP_SANDBOX_TOKEN ?? "" },
    credentials: "include",
  });
  console.log("zip status:", r.status);
  if (r.ok) {
    const buf = Buffer.from(await r.arrayBuffer());
    // list entry names
    const names: string[] = [];
    let q = 0;
    while (q < buf.length - 4) {
      if (buf[q] === 0x50 && buf[q+1] === 0x4b && buf[q+2] === 0x03 && buf[q+3] === 0x04) {
        const nlen = buf.readUInt16LE(q+26);
        const elen = buf.readUInt16LE(q+28);
        const csize = buf.readUInt32LE(q+18);
        names.push(buf.slice(q+30, q+30+nlen).toString());
        q += 30 + nlen + elen + csize;
      } else q++;
    }
    console.log("zip entries:", names.join(", "));
  }
} catch (e: any) { console.log("zip check err:", e.message); }

provider.destroy();
process.exit(0);
