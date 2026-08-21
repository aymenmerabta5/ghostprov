/**
 * Gateway client — every Prism gateway call flows through the live page
 * (page.evaluate + same-origin fetch) to bypass Cloudflare JA3 binding.
 *
 * CRITICAL: evaluate bodies are passed as RAW STRINGS. Passing functions breaks
 * under tsx/esbuild because the transform injects `__name(...)` helpers that
 * don't exist in the browser (ReferenceError inside evaluate).
 */
import { config } from "../config";
import { ZipPollCoordinator } from "./zip-coordinator";
import * as fs from "node:fs";
import { resolveTokenFile } from "../bridge/config";

type PageLike = { evaluate: (fn: any, ...args: any[]) => Promise<any> };

async function getPage(): Promise<PageLike | null> {
  try {
    const bridge: any = await import("../bridge/playwright");
    const p = bridge.getBridgePage?.();
    if (p && typeof p.evaluate === "function") return p;
  } catch {}
  return null;
}

/**
 * Evaluate a raw-JS async function with one JSON-serializable argument inside
 * the Prism tab. Transform-proof: `fnSource` is a string literal, so bundlers
 * never touch it.
 */
export async function evalInPage<T>(fnSource: string, arg: unknown): Promise<T> {
  const page = await getPage();
  if (!page) throw new Error("prism bridge not active (no page)");
  const expr = `(async () => { const __arg = ${JSON.stringify(arg ?? null)}; return (${fnSource})(__arg); })()`;
  return page.evaluate(expr);
}

export function getSandboxToken(): string {
  try {
    const g = (globalThis as any).__GP_BRIDGE_TOKENS__?.sandboxToken ?? (globalThis as any).__GP_TOK__;
    if (typeof g === "string" && g.length > 10 && !g.includes("mock")) return g;
  } catch {}
  try {
    const file = resolveTokenFile();
    if (fs.existsSync(file)) {
      const j = JSON.parse(fs.readFileSync(file, "utf8"));
      if (j?.sandboxToken && String(j.sandboxToken).length > 10) return j.sandboxToken;
    }
  } catch {}
  const env = (process.env.PRISM_SANDBOX_TOKEN ?? "").trim();
  return env.length > 10 ? env : "";
}

let bustCounter = 0;
export function bust(): string {
  return `gp${Date.now().toString(36)}${(bustCounter++).toString(36)}`;
}

export class GatewayAuthError extends Error {
  constructor(readonly status: number) {
    super(`gateway auth failure ${status}`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Sync token from the live bridge instance (ESM dynamic import — no require). */
export async function syncTokenFromBridge(): Promise<string | null> {
  try {
    const bridge: any = await import("../bridge/playwright");
    const t = bridge.getSandboxToken?.() ?? bridge.getBridgeTokens?.()?.sandboxToken ?? null;
    if (typeof t === "string" && t.length > 10 && !t.includes("mock")) {
      (globalThis as any).__GP_BRIDGE_TOKENS__ = { sandboxToken: t };
      return t;
    }
  } catch {}
  return null;
}

export async function getSandboxTokenAsync(): Promise<string> {
  const t = await syncTokenFromBridge();
  if (t) return t;
  const local = getSandboxToken();
  if (local) return local;
  throw new GatewayAuthError(401);
}

/**
 * Token invalidation recovery: the sandbox token rotates whenever Prism
 * reprovisions/restarts the sandbox. Ask the bridge to refresh (page reload →
 * app re-bootstraps → fresh token captured), then wait until the token differs.
 */
export async function refreshSandboxToken(staleToken: string, timeoutMs = 90_000): Promise<string> {
  try {
    const bridge: any = await import("../bridge/playwright");
    await bridge.refreshBridge?.();
  } catch {}
  const t0 = Date.now();
  for (;;) {
    const t = await syncTokenFromBridge();
    if (t && t !== staleToken) {
      console.log(`[gateway] sandbox token refreshed (${t.slice(0, 12)}…)`);
      return t;
    }
    if (Date.now() - t0 > timeoutMs) throw new GatewayAuthError(401);
    await sleep(1500);
  }
}

// Raw-string evaluate sources (never transformed by bundlers)
const FETCH_TEXT_SRC = `
async (arg) => {
  const r = await fetch(arg.url, {
    method: arg.method,
    headers: arg.headers,
    body: arg.body,
    credentials: "include",
  });
  return { status: r.status, body: await r.text() };
}
`;

const FETCH_ZIP_SRC = `
async (arg) => {
  const r = await fetch(arg.url, {
    headers: { "x-crixet-sandbox-token": arg.token },
    credentials: "include",
  });
  if (!r.ok) return { status: r.status, files: {} };
  const bytes = new Uint8Array(await r.arrayBuffer());
  const dec = new TextDecoder();
  const files = {};
  const b64of = (u8) => {
    let s = "";
    const CH = 0x8000;
    for (let i = 0; i < u8.length; i += CH) {
      s += String.fromCharCode.apply(null, Array.from(u8.subarray(i, i + CH)));
    }
    return btoa(s);
  };
  let q = 0;
  while (q < bytes.length - 4) {
    if (bytes[q] === 0x50 && bytes[q+1] === 0x4b && bytes[q+2] === 0x03 && bytes[q+3] === 0x04) {
      const nlen = bytes[q+26] | (bytes[q+27] << 8);
      const elen = bytes[q+28] | (bytes[q+29] << 8);
      const csize = bytes[q+18] | (bytes[q+19] << 8) | (bytes[q+20] << 16) | (bytes[q+21] << 24);
      const method = bytes[q+8] | (bytes[q+9] << 8);
      const name = dec.decode(bytes.slice(q + 30, q + 30 + nlen));
      const data = bytes.slice(q + 30 + nlen + elen, q + 30 + nlen + elen + csize);
      if (!arg.want || arg.want.includes(name)) {
        if (method === 0) files[name] = b64of(data);
        else {
          try {
            const ds = new DecompressionStream("deflate-raw");
            const ab = await new Response(new Blob([data]).stream().pipeThrough(ds)).arrayBuffer();
            files[name] = b64of(new Uint8Array(ab));
          } catch {}
        }
      }
      q += 30 + nlen + elen + csize;
    } else q++;
  }
  return { status: r.status, files };
}
`;

/** Run a same-origin fetch inside the Prism tab. Auto-recovers from token
 *  invalidation (401/403 → refresh → retry) and transient restarts (502/503). */
export async function pageFetch(
  path: string,
  init?: { method?: string; json?: unknown },
): Promise<{ status: number; text: string }> {
  const url = `${config.prismOrigin}${path}`;
  for (let attempt = 0; attempt < 4; attempt++) {
    const token = await getSandboxTokenAsync();
    let body: string | undefined;
    const headers: Record<string, string> = { "x-crixet-sandbox-token": token };
    if (init?.json !== undefined) {
      headers["content-type"] = "application/json";
      body = JSON.stringify(init.json);
    }
    const res = await evalInPage<{ status: number; body: string }>(FETCH_TEXT_SRC, {
      url, method: init?.method ?? "GET", headers, body,
    });
    if (res.status !== 200 && res.status !== 202) {
      console.warn(`[gw] ${init?.method ?? "GET"} ${path.slice(0, 60)} -> ${res.status} ${res.body.slice(0, 120)}`);
    }
    if ((res.status === 401 || res.status === 403) && attempt < 3) {
      console.warn(`[gw] auth failure ${res.status} — refreshing sandbox token from bridge…`);
      await refreshSandboxToken(token).catch(() => undefined);
      continue;
    }
    if ((res.status === 502 || res.status === 503) && attempt < 3) {
      await sleep(3000);
      continue;
    }
    return { status: res.status, text: res.body };
  }
  throw new GatewayAuthError(401);
}

/** Read /zip, inflate entries in-page, return {name → bytes}. */
async function fetchZipFilesDirect(names?: string[]): Promise<Map<string, Uint8Array>> {
  for (let attempt = 0; attempt < 4; attempt++) {
    const token = await getSandboxTokenAsync();
    const result = await evalInPage<{ status: number; files: Record<string, string> }>(FETCH_ZIP_SRC, {
      url: `${config.prismOrigin}${config.gatewayBase}/zip?prism_cache_bust=${bust()}`,
      token,
      want: names ?? null,
    });
    if (result.status !== 200) {
      console.warn(`[gw] GET /zip -> ${result.status} (attempt ${attempt + 1}/4)`);
    }
    if (result.status === 200) {
      const out = new Map<string, Uint8Array>();
      for (const [name, b64] of Object.entries(result.files)) {
        out.set(name, Uint8Array.from(atob(b64), (c) => c.charCodeAt(0)));
      }
      return out;
    }
    if ((result.status === 401 || result.status === 403) && attempt < 3) {
      await refreshSandboxToken(token).catch(() => undefined);
      continue;
    }
    if ((result.status === 502 || result.status === 503) && attempt < 3) {
      await sleep(3000);
      continue;
    }
    throw new GatewayAuthError(result.status || 500);
  }
  throw new GatewayAuthError(401);
}

const configuredZipBatchMs = Number(process.env.GP_ZIP_BATCH_MS ?? 10);
export const zipPollCoordinator = new ZipPollCoordinator(
  names => fetchZipFilesDirect(names),
  Number.isFinite(configuredZipBatchMs) && configuredZipBatchMs >= 0 ? configuredZipBatchMs : 10,
);

/** Coalesced named reads; unfiltered archive reads remain direct. */
export function fetchZipFiles(names?: string[]): Promise<Map<string, Uint8Array>> {
  return names ? zipPollCoordinator.read(names) : fetchZipFilesDirect();
}

export async function readTrackedFile(name: string): Promise<string | null> {
  const files = await fetchZipFiles([name]);
  const data = files.get(name);
  return data ? new TextDecoder().decode(data) : null;
}

/** Trigger one render cycle. Resolves when latexmk has been kicked off (202). */
export async function triggerRender(): Promise<void> {
  const res = await pageFetch(
    `/s/sandboxes/proxy/render?renderMode=async&renderStatusMode=json&prism_cache_bust=${bust()}`,
    {
      method: "POST",
      json: { mainDocument: "main.tex", clientStateVector: "AerAuOEF8wI=", clientDeleteSetUpdate: "AAA=" },
    },
  );
  if (res.status !== 202) throw new Error(`render ${res.status}: ${res.text.slice(0, 200)}`);
}

/** Remove a workspace file (allowlisted route). Best-effort. */
export async function deleteFile(name: string): Promise<void> {
  await pageFetch(`/s/sandboxes/proxy/delete-file?prism_cache_bust=${bust()}`, {
    method: "POST",
    json: { filename: name },
  }).catch(() => undefined);
}
