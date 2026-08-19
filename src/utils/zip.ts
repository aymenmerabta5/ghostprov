import { unzipSync } from "fflate";

async function getSandboxToken(): Promise<string> {
  try {
    const bridge: any = await import("../bridge/playwright");
    const t = bridge.getSandboxToken?.();
    if (t && t !== "mock-token") return t;
    const b = bridge.getBridgeTokens?.();
    if (b?.sandboxToken && b.sandboxToken !== "mock-token") return b.sandboxToken;
  } catch {}
  try {
    const g = (globalThis as any).__BRIDGE_TOKENS__?.sandboxToken ?? (globalThis as any).__BRIDGE_SANDBOX_TOKEN__;
    if (g && typeof g === "string" && g.length > 10) return g;
  } catch {}
  try {
    const fs: any = await import("node:fs");
    const path: any = await import("node:path");
    const file = path.join("D:/MyWork/AI/ghostprov/.pw", "tokens.json");
    if (fs.existsSync(file)) {
      const j = JSON.parse(fs.readFileSync(file, "utf8"));
      if (j?.sandboxToken) return j.sandboxToken;
    }
  } catch {}
  return process.env.PRISM_SANDBOX_TOKEN ?? "mock-token";
}

async function maybeRefreshOnFailure(status: number, bodyText?: string): Promise<string | null> {
  try {
    const bridge: any = await import("../bridge/playwright");
    if (bridge.shouldRefreshOnStatus?.(status, bodyText)) {
      await bridge.handleAuthFailure?.(status, bodyText);
      const t = bridge.getSandboxToken?.();
      if (t && t !== "mock-token") return t;
    }
  } catch {}
  return null;
}

export async function fetchZip(): Promise<Map<string, Uint8Array>> {
  let tok = await getSandboxToken();
  const bust = Math.floor(Math.random() * 1e15);
  let r = await fetch(
    `https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${bust}`,
    { headers: { "x-crixet-sandbox-token": tok } },
  );
  if (!r.ok) {
    const txt = await r.clone().text().catch(() => "");
    const refreshed = await maybeRefreshOnFailure(r.status, txt);
    if (refreshed) {
      tok = refreshed;
      r = await fetch(
        `https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${bust}`,
        { headers: { "x-crixet-sandbox-token": tok } },
      );
    }
    if (!r.ok) {
      // include syncing hint for better error
      if (txt.toLowerCase().includes("syncing")) {
        throw new Error(`fetchZip ${r.status} syncing — bridge refreshing`);
      }
      throw new Error(`fetchZip ${r.status} ${r.statusText} ${txt}`);
    }
  }
  const buf = new Uint8Array(await r.arrayBuffer());
  const unzipped = unzipSync(buf);
  return new Map(Object.entries(unzipped));
}

export async function extractResponse(): Promise<unknown> {
  const files = await fetchZip();
  const data = files.get("response.json");
  if (!data) throw new Error("response.json not found in zip");
  return JSON.parse(new TextDecoder().decode(data));
}
