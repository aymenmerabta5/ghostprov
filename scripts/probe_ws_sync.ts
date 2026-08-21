/**
 * Probe: connect, sync, dump structure, CREATE a file, verify real-time.
 * Uses the y-sweet URL format: wss://host/y/d/{docId}/ws/{docId}?token={token}
 */
import "dotenv/config";
import * as Y from "yjs";
import * as decoding from "lib0/decoding";
import * as encoding from "lib0/encoding";
import WebSocket from "ws";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const token = process.env.GP_FRESH_TOKEN ?? "";
if (!token) { console.error("set GP_FRESH_TOKEN"); process.exit(1); }

const url = `wss://prism.openai.com/y/d/${docId}/ws/${docId}?token=${token}`;
console.log("connecting…");

const ws = new (WebSocket as any)(url);
const doc = new Y.Doc();

function sendSync(update?: Uint8Array) {
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, 0); // msgType sync
  if (update) {
    encoding.writeVarUint(enc, 1); // sync step 2 (update)
    encoding.writeVarUint8Array(enc, update);
  } else {
    encoding.writeVarUint(enc, 0); // sync step 1 (request state)
    encoding.writeVarUint8Array(enc, Y.encodeStateVector(doc));
  }
  ws.send(encoding.toUint8Array(enc));
}

// observe all changes on the doc
doc.on("afterTransaction", (tr: any) => {
  console.log("  [transaction] changed keys:", [...tr.changed.keys?.() ?? []].slice(0, 10));
});

ws.on("open", () => {
  console.log("OPEN ✓");
  sendSync(); // request full state
});

ws.on("message", (data: Buffer, isBinary: boolean) => {
  if (!isBinary) return;
  const buf = new Uint8Array(data);
  const dec = decoding.createDecoder(buf);
  const msgType = decoding.readVarUint(dec);

  if (msgType === 0) {
    const syncType = decoding.readVarUint(dec);
    if (syncType === 0) {
      // sync step 2: full state
      const update = decoding.readVarUint8Array(dec);
      Y.applyUpdate(doc, update);
      console.log("\n=== SYNCED ===");
      dumpStructure();
      // now create a test file
      setTimeout(() => createTestFile(), 500);
    } else if (syncType === 1) {
      // incremental update
      const update = decoding.readVarUint8Array(dec);
      Y.applyUpdate(doc, update);
    } else if (syncType === 2) {
      // update broadcast
      const update = decoding.readVarUint8Array(dec);
      Y.applyUpdate(doc, update);
    }
  }
});

function dumpStructure() {
  console.log("=== top-level types ===");
  for (const [name, type] of doc.share.entries()) {
    console.log(`  ${name}: ${type.constructor.name}`);
  }
  for (const [name, type] of doc.share.entries()) {
    if (!(type instanceof Y.Map)) continue;
    console.log(`\n=== map "${name}" (${type.size} entries) ===`);
    let count = 0;
    for (const [key, value] of (type as any)._map.entries()) {
      if (count >= 20) { console.log("  … truncated"); break; }
      const v = value.value;
      const kind = v?.constructor?.name ?? typeof v;
      let preview = "";
      if (v instanceof Y.Text) preview = `Y.Text(${v.length}) head=${JSON.stringify(v.toString().slice(0, 80))}`;
      else if (typeof v === "string") preview = `string(${v.length}) head=${JSON.stringify(v.slice(0, 80))}`;
      else preview = JSON.stringify(v)?.slice(0, 120) ?? String(v);
      console.log(`  ${JSON.stringify(key)} → ${kind} ${preview}`);
      count++;
    }
  }
}

function createTestFile() {
  console.log("\n=== CREATING probe_test.txt ===");
  // find the files map (the biggest Y.Map)
  let filesMap: Y.Map<any> | null = null;
  for (const [, type] of doc.share.entries()) {
    if (type instanceof Y.Map && type.size > 0) {
      if (!filesMap || type.size > filesMap.size) filesMap = type;
    }
  }
  if (!filesMap) {
    // try creating entries in each map
    console.log("no populated map found, trying first map…");
    for (const [name, type] of doc.share.entries()) {
      if (type instanceof Y.Map) {
        console.log(`setting on map "${name}"`);
        const text = new Y.Text();
        text.insert(0, "PROBE_CREATED_BY_GHOSTPROV_WS");
        (type as any).set("probe_test.txt", text);
        console.log("created probe_test.txt as Y.Text on", name);
        break;
      }
    }
  } else {
    console.log(`using map with ${filesMap.size} entries`);
    const text = new Y.Text();
    text.insert(0, "PROBE_CREATED_BY_GHOSTPROV_WS");
    filesMap.set("probe_test.txt", text);
    console.log("created probe_test.txt as Y.Text");
  }

  // send the update to server
  const update = Y.encodeStateAsUpdate(doc);
  const enc = encoding.createEncoder();
  encoding.writeVarUint(enc, 0);
  encoding.writeVarUint(enc, 1);
  encoding.writeVarUint8Array(enc, update);
  ws.send(encoding.toUint8Array(enc));
  console.log("update sent to server");

  setTimeout(() => {
    console.log("\n=== final structure ===");
    dumpStructure();
    console.log("\ndone — check /zip to see if probe_test.txt materialized on disk");
    ws.close();
    process.exit(0);
  }, 3000);
}

ws.on("error", (e: Error) => { console.error("ws error:", e.message); process.exit(1); });
ws.on("unexpected-response", (_req: any, res: any) => {
  console.error("HTTP", res.statusCode, res.statusMessage);
  process.exit(1);
});
