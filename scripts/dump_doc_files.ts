/** Dump file contents from the live doc. */
import "dotenv/config";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import ws from "ws";
import * as fs from "node:fs";

if (!(globalThis as any).WebSocket) (globalThis as any).WebSocket = ws;

const docId = (process.env.PRISM_DOC_ID ?? "").trim();
const tokens = JSON.parse(fs.readFileSync("D:/MyWork/AI/ghostprov/.pw/tokens.json", "utf8"));
const token = process.env.GP_FRESH_TOKEN || tokens.ySweetToken;

const doc = new Y.Doc();
const provider = new WebsocketProvider(
  `wss://prism.openai.com/y/d/${docId}/ws`, docId, doc,
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
if (!synced) { console.error("sync failed"); process.exit(1); }

const want = process.argv.slice(2);
const contentMap = doc.getMap("content");
for (const [, val] of contentMap.entries()) {
  const m = val as Y.Map<any>;
  const fn = m.get("filename");
  if ((m.get("deleted") === true) || !want.includes(fn)) continue;
  const c = m.get("content");
  const text = c?.toString?.() ?? String(c ?? "");
  console.log(`===== ${fn} (${text.length} chars) =====`);
  console.log(text);
}

fs.mkdirSync("D:/MyWork/AI/ghostprov/.pw/sandbox", { recursive: true });
provider.destroy();
doc.destroy();
process.exit(0);
