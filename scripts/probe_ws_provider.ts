/**
 * Probe using WebsocketProvider (handles Yjs protocol natively).
 * URL: wss://host/y/d/{docId}/ws/{docId}?token={token}
 */
import "dotenv/config";
import * as fs from "node:fs";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const token = process.env.GP_FRESH_TOKEN ?? "";
if (!token) { console.error("set GP_FRESH_TOKEN"); process.exit(1); }

// Build the URL exactly as the user showed
const wsUrl = `wss://prism.openai.com/y/d/${docId}/ws/${docId}?token=${token}`;
console.log("url:", wsUrl.slice(0, 80) + "…");

const doc = new Y.Doc();

// WebsocketProvider constructs: {serverUrl}/{roomname}
// We need the final URL to be: wss://.../ws/{docId}?token=...
// So: serverUrl = wss://.../ws?token=..., roomname = docId
// → produces: wss://.../ws?token=.../{docId} ← WRONG ORDER
//
// Instead: hook into the provider's ws construction
// OR: use serverUrl without roomname trick

// Actually y-websocket builds: `${serverUrl}/${roomname}`
// So set serverUrl = `wss://prism.openai.com/y/d/${docId}/ws`
// And roomname = docId
// → gives us: wss://prism.openai.com/y/d/{docId}/ws/{docId}
// Then we need ?token= appended. y-websocket doesn't do this natively.
// Workaround: monkey-patch to append token after construction.

const provider = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`,
  docId,
  doc,
  { connect: true } as any,
);

// Patch the URL to add token — intercept the WebSocket before it connects
const origWs = (provider as any).ws;
if (origWs) {
  // already connected without token — need to destroy and reconnect
}

// Better approach: destroy and re-create with patched URL
provider.destroy();

// Monkey-patch WebSocket to inject token
const OrigWS = globalThis.WebSocket;
class PatchedWS extends OrigWS {
  constructor(url: any, protocols?: any) {
    // Append token if not already present
    const u = typeof url === "string" ? url : url.toString();
    const finalUrl = u.includes("token=") ? u : `${u}?token=${token}`;
    super(finalUrl, protocols);
  }
}
globalThis.WebSocket = PatchedWS as any;

console.log("connecting with patched WS…");
const provider2 = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`,
  docId,
  doc,
  {
    connect: true,
    WebSocketPolyfill: PatchedWS,
  } as any,
);

provider2.on("status", (s: any) => console.log("status:", s.status));
provider2.on("connection-error", (e: any) => console.log("conn-err:", String(e).slice(0, 150)));

const synced = await new Promise<boolean>((resolve) => {
  const t = setTimeout(() => resolve(false), 20000);
  provider2.on("sync", (isSynced: boolean) => {
    if (isSynced) { clearTimeout(t); resolve(true); }
  });
});

console.log("\nsynced:", synced);

if (synced) {
  console.log("\n=== doc structure ===");
  for (const [name, type] of doc.share.entries()) {
    console.log(`  ${name}: ${type.constructor.name}`);
    if (type instanceof Y.Map) {
      let count = 0;
      for (const [key, value] of (type as any)._map.entries()) {
        if (count >= 15) { console.log("    … truncated"); break; }
        const v = value.value;
        const kind = v?.constructor?.name ?? typeof v;
        let preview = "";
        if (v instanceof Y.Text) preview = `Y.Text(${v.length}) head=${JSON.stringify(v.toString().slice(0, 80))}`;
        else if (v instanceof Y.Map) preview = `Y.Map(${v.size})`;
        else if (typeof v === "string") preview = `string(${v.length}) head=${JSON.stringify(v.slice(0, 80))}`;
        else preview = JSON.stringify(v)?.slice(0, 100) ?? String(v);
        console.log(`    ${JSON.stringify(key)} → ${kind} ${preview}`);
        count++;
      }
    }
  }

  // Now create a test file
  console.log("\n=== creating probe_test.txt ===");
  for (const [name, type] of doc.share.entries()) {
    if (!(type instanceof Y.Map)) continue;
    // find the map that contains job.json
    let hasJob = false;
    for (const [key] of (type as any)._map.entries()) {
      if (key === "job.json") { hasJob = true; break; }
    }
    if (hasJob) {
      console.log(`found files map: "${name}"`);
      const text = new Y.Text();
      text.insert(0, "PROBE_CREATED_VIA_WS");
      type.set("probe_test.txt", text);
      console.log("probe_test.txt created!");
      break;
    }
  }

  // wait for it to propagate
  await new Promise(r => setTimeout(r, 3000));
  console.log("done — check /zip for probe_test.txt");
}

provider2.destroy();
doc.destroy();
process.exit(0);
