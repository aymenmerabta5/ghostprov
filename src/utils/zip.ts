import "dotenv/config";
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
    if (bridge.isBridgeActive?.()) {
      const page: any = (bridge as any).getBridgePage?.() ?? null;
      if (page && typeof page.evaluate === "function") {
        const headersObj: Record<string, string> = {};
        const h: any = (opts.headers as any) ?? {};
        for (const [k, v] of Object.entries(h)) headersObj[k] = String(v);
        const method = (opts.method ?? "GET") as string;
        const isZip = url.includes("/zip");
        if (isZip) {
          // binary: return base64 via evaluate to avoid text corruption
          const result: { status: number; headers: Record<string, string>; b64: string } = await page.evaluate(
            async ({ url, method, headers }: any) => {
              const r = await fetch(url, { method, headers, credentials: "include" });
              const ab = await r.arrayBuffer();
              const bytes = new Uint8Array(ab);
              let binary = "";
              for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
              const b64 = btoa(binary);
              const hdrs: Record<string, string> = {};
              r.headers.forEach((v: string, k: string) => (hdrs[k] = v));
              return { status: r.status, headers: hdrs, b64 };
            },
            { url, method, headers: headersObj },
          );
          const binary = atob(result.b64);
          const bytes = new Uint8Array(binary.length);
          for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
          return new Response(bytes, { status: result.status, headers: result.headers });
        }
        const bodyStr = typeof opts.body === "string" ? opts.body : opts.body ? JSON.stringify(opts.body) : undefined;
        const result: { status: number; headers: Record<string, string>; body: string } = await page.evaluate(
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
  return fetch(url, opts);
}

export async function fetchZip(): Promise<Map<string, Uint8Array>> {
  let tok = await getSandboxToken();
  const bust = Math.floor(Math.random() * 1e15);
  let r = await fetchViaBridge(
    `https://prism.openai.com/s/sandboxes/proxy/zip?prism_cache_bust=${bust}`,
    { headers: { "x-crixet-sandbox-token": tok } },
  );
  if (!r.ok) {
    const txt = await r.clone().text().catch(() => "");
    const refreshed = await maybeRefreshOnFailure(r.status, txt);
    if (refreshed) {
      tok = refreshed;
      r = await fetchViaBridge(
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
