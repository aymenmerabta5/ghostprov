/**
 * Raw WebSocket handshake probe for the Prism Yjs endpoint.
 * Tests URL/auth/Origin combinations and prints the actual failure.
 */
import "dotenv/config";
import * as fs from "node:fs";
import WebSocket from "ws";

const docId = (process.env.PRISM_DOC_ID ?? "").trim();

function loadTokens(): { ysweet?: string } {
  try {
    const j = JSON.parse(fs.readFileSync("D:/MyWork/AI/ghostprov/.pw/tokens.json", "utf8"));
    return { ysweet: j.ySweetToken };
  } catch {}
  return {};
}

async function liveToken(): Promise<string | null> {
  try {
    const r = await fetch("http://localhost:8787/__bridge/token");
    const j = await r.json();
    if (j?.ySweetToken && !String(j.ySweetToken).includes("mock")) return j.ySweetToken;
  } catch {}
  return loadTokens().ysweet ?? null;
}

function tryWs(url: string, headers: Record<string, string>, label: string, timeoutMs = 6000): Promise<string> {
  return new Promise((resolve) => {
    const ws = new WebSocket(url, { headers });
    let done = false;
    const finish = (msg: string) => { if (!done) { done = true; try { ws.close(); } catch {} resolve(`${label}: ${msg}`); } };
    const t = setTimeout(() => finish("timeout (no response)"), timeoutMs);
    ws.on("unexpected-response", (_req, res: any) => { clearTimeout(t); finish(`HTTP ${res.statusCode} ${res.statusMessage}`); });
    ws.on("error", (e: any) => { clearTimeout(t); finish(`error: ${e.message}`); });
    ws.on("open", () => { clearTimeout(t); finish("OPEN ✓"); });
    ws.on("upgrade", (res: any) => console.log(`  [${label}] upgrade status ${res.statusCode}`));
  });
}

async function main() {
  const token = await liveToken();
  if (!token) { console.error("no token"); process.exit(1); }
  const base = `wss://prism.openai.com/y/d/${docId}/ws`;
  const results: string[] = [];
  results.push(await tryWs(`${base}?token=${encodeURIComponent(token)}`, { Origin: "https://prism.openai.com" }, "token+origin"));
  results.push(await tryWs(base, { Origin: "https://prism.openai.com" }, "noauth+origin"));
  results.push(await tryWs(`${base}?token=${encodeURIComponent(token)}`, {}, "token only"));
  results.push(await tryWs(`${base}?token=${encodeURIComponent(token)}`, { Origin: "https://prism.openai.com", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/151.0.0.0" }, "token+origin+ua"));
  console.log("\n=== results ===");
  for (const r of results) console.log(r);
  process.exit(0);
}

main();
