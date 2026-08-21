/**
 * Probe: connect Node-side to the live Yjs doc, dump file structure.
 */
import "dotenv/config";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import ws from "ws";

if (!(globalThis as any).WebSocket) (globalThis as any).WebSocket = ws;

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const tokens = JSON.parse((await import("node:fs")).readFileSync("D:/MyWork/AI/ghostprov/.pw/tokens.json", "utf8"));
const token = process.env.GP_FRESH_TOKEN || tokens.ySweetToken;
if (!token) { console.error("no ySweet token"); process.exit(1); }

const doc = new Y.Doc();
const provider = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`,
  docId,
  doc,
  {
    connect: true,
    WebSocketPolyfill: class extends ws {
      constructor(url: any, protocols?: any) { super(`${url}?token=${token}`, protocols); }
    },
  } as any,
);

const synced = await new Promise<boolean>((resolve) => {
  const t = setTimeout(() => resolve(false), 15000);
  provider.on("sync", (s: boolean) => { if (s) { clearTimeout(t); resolve(true); } });
});
console.log("synced:", synced);
if (!synced) process.exit(1);

const contentMap = doc.getMap("content");
console.log("entries:", contentMap.size);
for (const [id, val] of contentMap.entries()) {
  const m = val as Y.Map<any>;
  const filename = m.get("filename");
  const type = m.get("type");
  const deleted = m.get("deleted");
  const inFolder = m.get("inFolder");
  const content = m.get("content");
  const cType = content?.constructor?.name ?? typeof content;
  const cLen = typeof content === "string" ? content.length : content?.length?._length ?? content?.length ?? 0;
  console.log(`- ${filename} | type=${type} deleted=${deleted} folder=${String(inFolder).slice(0, 8)} | id=${id.slice(0, 8)} | content=${cType} len=${cLen}`);
}

// also check other shared types
console.log("\nshare keys:", Array.from(doc.share.keys()));

provider.destroy();
doc.destroy();
process.exit(0);
