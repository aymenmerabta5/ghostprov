/**
 * Deep probe v2: access Yjs internal _map to extract file contents.
 */
import "dotenv/config";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const token = process.env.GP_FRESH_TOKEN ?? "";

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
console.log("synced ✓\n");

function dumpItemContent(key: string, item: any, prefix: string): void {
  const content = item?.content;
  if (!content) { console.log(`${prefix}${key} → no content`); return; }
  const cType = content.constructor.name;

  if (cType === "ContentType") {
    // This wraps another Y type
    const inner = content.type;
    console.log(`${prefix}${key} → ContentType wrapping ${inner.constructor.name}`);
    if (inner instanceof Y.Text || (inner && typeof inner.toString === "function" && typeof inner.insert === "function")) {
      const s = inner.toString();
      console.log(`  text(${s.length}): ${JSON.stringify(s.slice(0, 120))}`);
    } else if (inner instanceof Y.Map || (inner && typeof inner.get === "function")) {
      console.log(`  inner map size: ${inner.size ?? "?"}`);
      try {
        for (const [k, v] of Object.entries(inner.toJSON?.() ?? {})) {
          console.log(`    ${k}: ${JSON.stringify(v)?.slice(0, 100)}`);
        }
      } catch (e: any) { console.log(`    err: ${e.message}`); }
    }
  } else if (cType === "ContentString") {
    console.log(`${prefix}${key} → string(${content.str.length}) ${JSON.stringify(content.str.slice(0, 100))}`);
  } else if (cType === "ContentAny") {
    console.log(`${prefix}${key} → any: ${JSON.stringify(content.content)?.slice(0, 100)}`);
  } else if (cType === "ContentFormat") {
    console.log(`${prefix}${key} → format`);
  } else {
    console.log(`${prefix}${key} → ${cType}`);
    try {
      const vals = content.getContent?.() ?? null;
      if (vals) console.log(`  getContent: ${JSON.stringify(vals).slice(0, 150)}`);
    } catch {}
  }
}

console.log("=== content map ===");
const contentType = doc.share.get("content");
if (contentType && (contentType as any)._map) {
  for (const [key, item] of (contentType as any)._map.entries()) {
    dumpItemContent(key, item, "  ");
  }
}

console.log("\n=== settings map ===");
const settingsType = doc.share.get("settings");
if (settingsType && (settingsType as any)._map) {
  for (const [key, item] of (settingsType as any)._map.entries()) {
    dumpItemContent(key, item, "  ");
  }
}

// Now try creating a new entry
console.log("\n=== creating probe_test.txt on content map ===");
try {
  const ytext = new Y.Text();
  ytext.insert(0, "PROBE_CREATED_VIA_WS");
  (contentType as any)._map.set("probe_test.txt", ytext);
  // Actually we need to use proper Yjs API
  // contentType.set("probe_test.txt", ytext);
  console.log("set on _map directly");
  
  // Broadcast the change
  const update = Y.encodeStateAsUpdate(doc);
  const enc = (await import("lib0/encoding")).createEncoder();
  (await import("lib0/encoding")).writeVarUint(enc, 0);
  (await import("lib0/encoding")).writeVarUint(enc, 1);
  (await import("lib0/encoding")).writeVarUint8Array(enc, update);
  // ws.send(...)
  console.log("update encoded but need ws reference");
} catch (e: any) {
  console.log("create err:", e.message);
}

provider.destroy();
process.exit(0);
