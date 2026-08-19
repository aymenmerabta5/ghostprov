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

export async function triggerRender(): Promise<void> {
  return queueRender(async () => {
    let tok = await getSandboxToken();
    const bust = Math.floor(Math.random() * 1e15);
    let r = await fetch(
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
    // auto-refresh on 401 / syncing (bridge token may have expired)
    if (r.status === 401 || r.status === 403) {
      const txt = await r.clone().text().catch(() => "");
      const refreshed = await maybeRefreshOnFailure(r.status, txt);
      if (refreshed) {
        tok = refreshed;
        r = await fetch(
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
          const r2 = await fetch(
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
    for (let i = 0; i < 20; i++) {
      await new Promise((res) => setTimeout(res, 1000));
      let sRes = await fetch(
        `https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`,
        { headers: { "x-crixet-sandbox-token": tok } },
      );
      // refresh token if status check is 401
      if (sRes.status === 401 || sRes.status === 403) {
        const txt = await sRes.clone().text().catch(() => "");
        const refreshed = await maybeRefreshOnFailure(sRes.status, txt);
        if (refreshed) {
          tok = refreshed;
          sRes = await fetch(
            `https://prism.openai.com/s/sandboxes/proxy/render-status?renderStatusMode=json&prism_cache_bust=${bust}`,
            { headers: { "x-crixet-sandbox-token": tok } },
          );
        }
      }
      const s = (await sRes.json()) as { status: string };
      if (s.status === "complete") return;
      if (s.status === "failed") throw new Error("render failed");
      // syncing status triggers refresh
      if ((s.status as string).toLowerCase().includes("sync")) {
        await maybeRefreshOnFailure(401, s.status);
      }
    }
    throw new Error("render timeout");
  });
}
