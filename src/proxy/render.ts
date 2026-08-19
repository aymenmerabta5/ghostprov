import "dotenv/config";
import { queueRender } from "../utils/queue";

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
  // WSL fallback: also try linux absolute path when Windows path not found (e.g. running via WSL node)
  try {
    const altFile2 = "/mnt/d/MyWork/AI/ghostprov/.pw/tokens.json";
    const fs2: any = await import("node:fs");
    if (fs2.existsSync(altFile2)) {
      const j2 = JSON.parse(fs2.readFileSync(altFile2, "utf8"));
      if (j2?.sandboxToken && typeof j2.sandboxToken === "string" && j2.sandboxToken.length > 5) {
        return j2.sandboxToken;
      }
    }
  } catch {}
  const envTok = (process.env.PRISM_SANDBOX_TOKEN ?? "").trim();
  return envTok.length > 5 ? envTok : "mock-token";
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

async function fetchViaBridge(url: string, opts: RequestInit): Promise<Response> {
  try {
    const bridge: any = await import("../bridge/playwright");
    // Use page.evaluate fetch when bridge is active (has valid browser context) to bypass Cloudflare JA3
    if (bridge.isBridgeActive?.()) {
      const page: any = (bridge as any).getBridgePage?.() ?? (await import("../bridge/playwright").then((m: any) => m.getBridgePage?.())) ?? null;
      // Fallback: try to get page via global
      const p = page ?? (globalThis as any).__BRIDGE_PAGE__ ?? null;
      if (p && typeof p.evaluate === "function") {
        const headersObj: Record<string, string> = {};
        const h: any = (opts.headers as any) ?? {};
        for (const [k, v] of Object.entries(h)) headersObj[k] = String(v);
        const bodyStr = typeof opts.body === "string" ? opts.body : opts.body ? JSON.stringify(opts.body) : undefined;
        const method = opts.method ?? "GET";
        const result: { status: number; headers: Record<string, string>; body: string } = await p.evaluate(
          async ({ url, method, headers, body }: any) => {
            const r = await fetch(url, { method, headers, body, credentials: "include" });
            const txt = await r.text();
            const hdrs: Record<string, string> = {};
            r.headers.forEach((v: string, k: string) => (hdrs[k] = v));
            return { status: r.status, headers: hdrs, body: txt };
          },
          { url, method, headers: headersObj, body: bodyStr },
        );
        return new Response(result.body, { status: result.status, headers: result.headers });
      }
    }
  } catch {}
  // Fallback to direct fetch (for CI mocks and when bridge not active)
  return fetch(url, opts);
}

export async function triggerRenderDirect(): Promise<void> {
  // direct render without queue - used by relay's closed-loop queue to avoid nested deadlock
  let tok = await getSandboxToken();
  const bust = Math.floor(Math.random() * 1e15);
  let r = await fetchViaBridge(
    `https://prism.openai.com/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust}`,
    {
      method: "POST",
      headers: { "x-crixet-sandbox-token": tok, "content-type": "application/json" },
      body: JSON.stringify({
        mainDocument: "main.tex",
        clientStateVector: "AerAuOEF8wI=",
        clientDeleteSetUpdate: "AAA=",
      }),
    },
  );
  if (r.status === 401 || r.status === 403) {
    const txt = await r.clone().text().catch(() => "");
    const refreshed = await maybeRefreshOnFailure(r.status, txt);
    if (refreshed) {
      tok = refreshed;
      r = await fetchViaBridge(
        `https://prism.openai.com/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust}`,
        {
          method: "POST",
          headers: { "x-crixet-sandbox-token": tok, "content-type": "application/json" },
          body: JSON.stringify({
            mainDocument: "main.tex",
            clientStateVector: "AerAuOEF8wI=",
            clientDeleteSetUpdate: "AAA=",
          }),
        },
      );
    }
  }
  if (r.status !== 202) {
    const txt = await r.clone().text().catch(() => "");
    if (txt.toLowerCase().includes("syncing")) {
      const refreshed = await maybeRefreshOnFailure(401, txt);
      if (refreshed) {
        tok = refreshed;
        const r2 = await fetchViaBridge(
          `https://prism.openai.com/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust}`,
          {
            method: "POST",
            headers: { "x-crixet-sandbox-token": tok, "content-type": "application/json" },
            body: JSON.stringify({
              mainDocument: "main.tex",
              clientStateVector: "AerAuOEF8wI=",
              clientDeleteSetUpdate: "AAA=",
            }),
          },
        );
        if (r2.status === 202) r = r2;
        else throw new Error(`render ${r2.status} ${txt}`);
      }
    }
    if (r.status !== 202) throw new Error(`render ${r.status} ${txt}`);
  }
  for (let i = 0; i < 120; i++) {
    await new Promise((res) => setTimeout(res, 1000));
    let sRes = await fetchViaBridge(
      `https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`,
      { headers: { "x-crixet-sandbox-token": tok } },
    );
    if (sRes.status === 401 || sRes.status === 403) {
      const txt = await sRes.clone().text().catch(() => "");
      const refreshed = await maybeRefreshOnFailure(sRes.status, txt);
      if (refreshed) {
        tok = refreshed;
        sRes = await fetchViaBridge(
          `https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`,
          { headers: { "x-crixet-sandbox-token": tok } },
        );
      }
    }
    const s = (await sRes.json()) as { status: string };
    if (s.status === "complete") return;
    if (s.status === "failed") throw new Error("render failed");
    if ((s.status as string).toLowerCase().includes("sync")) {
      await maybeRefreshOnFailure(401, s.status);
    }
  }
  throw new Error("render timeout");
}

export async function triggerRender(): Promise<void> {
  return queueRender(() => triggerRenderDirect());
}
