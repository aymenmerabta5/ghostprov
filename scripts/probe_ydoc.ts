/**
 * Probe: connect to Prism Yjs doc using the y-sweet style URL with docId twice.
 * Dumps the files map structure read-only.
 */
import "dotenv/config";
import * as fs from "node:fs";
import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import WebSocket from "ws";

if (!(globalThis as any).WebSocket) (globalThis as any).WebSocket = WebSocket;

const docId = (process.env.PRISM_DOC_ID ?? "").trim();

async function liveToken(): Promise<string | null> {
  try {
    const r = await fetch("http://localhost:8787/__bridge/token");
    const j = await r.json();
    if (j?.ySweetToken && !String(j.ySweetToken).includes("mock")) return j.ySweetToken;
  } catch {}
  try {
    const j = JSON.parse(fs.readFileSync("D:/MyWork/AI/ghostprov/.pw/tokens.json", "utf8"));
    return j.ySweetToken ?? null;
  } catch {}
  return null;
}

async function main() {
  const token = await liveToken();
  if (!token) { console.error("no ysweet token"); process.exit(1); }
  console.log("token:", token.slice(0, 16) + "… len=" + token.length);

  // The user-provided URL format: wss://host/y/d/<docId>/ws/<docId>?token=<token>
  const url = `wss://prism.openai.com/y/d/${docId}/ws/${docId}?token=${token}`;
  console.log("connecting:", url.slice(0, 80) + "…");

  const doc = new Y.Doc();

  // Use raw WebSocket + manual Yjs protocol via provider with custom URL
  const provider = new WebsocketProvider(
    url,
    docId,
    doc,
    { connect: true } as any,
  );
  provider.on("status", (s: any) => console.log("status:", s.status));
  provider.on("connection-error", (e: any) => {
    const detail = e?.error?.message || e?.message || String(e);
    console.log("conn-error:", String(detail).slice(0, 200));
  });

  const synced = await new Promise<boolean>((resolve) => {
    const t = setTimeout(() => resolve(false), 20000);
    provider.on("sync", (isSynced: boolean) => {
      if (isSynced) { clearTimeout(t); resolve(true); }
    });
  });
  console.log("\nsynced:", synced);
  if (!synced) {
    // dump what we have anyway
    console.log("share keys:", [...doc.share.keys()]);
    process.exit(2);
  }

  console.log("\n=== top-level types ===");
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

  for (const [name, type] of doc.share.entries()) {
    if (type instanceof Y.Array) {
      console.log(`\n=== array "${name}" len=${type.length} ===`);
      for (let i = 0; i < Math.min(type.length, 10); i++) {
        const v = type.get(i);
        console.log(`  [${i}] ${v?.constructor?.name}: ${JSON.stringify(v)?.slice(0, 100)}`);
      }
    }
  }

  provider.destroy();
  doc.destroy();
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
