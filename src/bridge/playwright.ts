/**
 * Playwright persistent bridge for Prism token minting (Task Bridge)
 *
 * - Launches chromium with userDataDir D:/MyWork/AI/ghostprov/.pw
 * - Navigates to https://prism.openai.com/?u=2a153042-dc25-4ca5-b290-260eed5e8705&pg=1&m=main.tex
 * - Waits for heartbeat (waitForResponse heartbeat)
 * - Captures x-crixet-sandbox-token via page.on('request') header
 * - Captures Y Sweet ASQ token via POST /api/y response {token}
 * - Exposes tokens to Hono via GET /__bridge/token and via file D:/MyWork/AI/ghostprov/.pw/tokens.json + globalThis.__BRIDGE_TOKENS__
 * - Auto-refreshes on 401 / syncing errors (page reload + re-capture)
 *
 * CI fallback: when playwright not installed or bridge not started, getSandboxToken()/getYSweetToken()
 * fall back to process.env.PRISM_SANDBOX_TOKEN / PRISM_Y_TOKEN / PRISM_DOC_ID flow (mock-token/mock_y_token)
 * so unit tests remain hermetic (mocked fetch).
 *
 * No side effects on import: persistent context only starts when startPlaywrightBridge() is called
 * (e.g. `bun src/bridge/playwright.ts` or server startup when PRISM_DOC_ID set and playwright available).
 * Tests mock this module via vi.mock, so live chromium never launches in CI.
 */

import * as fs from "node:fs";
import * as path from "node:path";

// ---------------------------------------------------------------------------
// Constants (task spec)
// ---------------------------------------------------------------------------
export const PRISM_URL =
  "https://prism.openai.com/?u=2a153042-dc25-4ca5-b290-260eed5e8705&pg=1&m=main.tex";
export const USER_DATA_DIR = "D:/MyWork/AI/ghostprov/.pw";
// Use forward slashes explicitly (task spec uses D:/MyWork/AI/ghostprov/.pw, not Windows backslashes)
// path.join on Windows would produce backslashes which breaks cross-platform tests
export const TOKEN_FILE = `${USER_DATA_DIR}/tokens.json`;
// legacy aliases for consumers that expect file at .pw/.tokens.json or .bridge
export const BRIDGE_TOKEN_FILE = TOKEN_FILE;

export interface BridgeTokens {
  sandboxToken?: string | null;
  ySweetToken?: string | null;
  updatedAt?: number;
}

// ---------------------------------------------------------------------------
// In-memory state + global + file persistence
// ---------------------------------------------------------------------------
let sandboxToken: string | null = null;
let ySweetToken: string | null = null;
let updatedAt: number | null = null;

// playwright handles (any to avoid hard dependency on @types/playwright)
let pwContext: any = null;
let pwPage: any = null;
let pwBrowser: any = null;
let isRefreshing = false;
let heartbeatSeen = false;

// Load from file synchronously on import (best-effort, never throws)
function loadTokensFromFileSync(): BridgeTokens {
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const raw = fs.readFileSync(TOKEN_FILE, "utf8");
      const j = JSON.parse(raw) as BridgeTokens;
      if (j.sandboxToken) sandboxToken = j.sandboxToken;
      if (j.ySweetToken) ySweetToken = j.ySweetToken;
      if (j.updatedAt) updatedAt = j.updatedAt;
      // also hydrate global
      (globalThis as any).__BRIDGE_TOKENS__ = {
        sandboxToken,
        ySweetToken,
        updatedAt,
      };
      return j;
    }
  } catch {}
  return {};
}
try {
  loadTokensFromFileSync();
} catch {}

// Also hydrate from env on first load for CI fallback
if (!sandboxToken && process.env.PRISM_SANDBOX_TOKEN) {
  sandboxToken = process.env.PRISM_SANDBOX_TOKEN;
}
if (!ySweetToken && process.env.PRISM_Y_TOKEN) {
  ySweetToken = process.env.PRISM_Y_TOKEN;
}

function persistTokensSync(): void {
  try {
    fs.mkdirSync(USER_DATA_DIR, { recursive: true });
    const payload: BridgeTokens = {
      sandboxToken,
      ySweetToken,
      updatedAt: updatedAt ?? Date.now(),
    };
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(payload, null, 2), "utf8");
    // also write to alternative path for Hono file fallback (tests check both)
    (globalThis as any).__BRIDGE_TOKENS__ = payload;
    // expose individual globals for legacy consumers
    if (sandboxToken) (globalThis as any).__BRIDGE_SANDBOX_TOKEN__ = sandboxToken;
    if (ySweetToken) (globalThis as any).__BRIDGE_Y_TOKEN__ = ySweetToken;
  } catch {}
}

export function getTokenFilePath(): string {
  return TOKEN_FILE;
}

export function getBridgeTokens(): BridgeTokens {
  // Pure memory only — does NOT include file/global/env, so that GET /__bridge/token can
  // distinguish source "bridge" (memory) vs "global" vs "file" vs "env".
  // This keeps tests deterministic: file tokens are exposed via file fallback, env via env fallback.
  // Consumers that want file/global/env fallback should use getSandboxToken()/getYSweetToken().
  return {
    sandboxToken: sandboxToken ?? null,
    ySweetToken: ySweetToken ?? null,
    updatedAt: updatedAt ?? undefined,
  };
}

export function getSandboxToken(): string {
  // 1. memory (bridge)
  if (sandboxToken && sandboxToken !== "mock-token") return sandboxToken;
  const t = getBridgeTokens().sandboxToken;
  if (t && t !== "mock-token" && t !== null) return t;
  // 2. global fallback (set by page.on or tests)
  try {
    const g = (globalThis as any).__BRIDGE_TOKENS__?.sandboxToken ?? (globalThis as any).__BRIDGE_SANDBOX_TOKEN__;
    if (g && typeof g === "string" && g.length > 5) return g;
  } catch {}
  // 3. file fallback (tokens.json written by bridge)
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const j = JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8")) as BridgeTokens;
      if (j?.sandboxToken && typeof j.sandboxToken === "string" && j.sandboxToken.length > 5) {
        // hydrate memory for next call
        sandboxToken = j.sandboxToken;
        if (j.updatedAt) updatedAt = j.updatedAt;
        return j.sandboxToken;
      }
    }
  } catch {}
  return process.env.PRISM_SANDBOX_TOKEN ?? "mock-token";
}

export function getYSweetToken(): string {
  if (ySweetToken && ySweetToken !== "mock_y_token") return ySweetToken;
  const t = getBridgeTokens().ySweetToken;
  if (t && t !== "mock_y_token" && t !== null) return t;
  try {
    const g = (globalThis as any).__BRIDGE_TOKENS__?.ySweetToken ?? (globalThis as any).__BRIDGE_Y_TOKEN__;
    if (g && typeof g === "string" && g.length > 5) return g;
  } catch {}
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const j = JSON.parse(fs.readFileSync(TOKEN_FILE, "utf8")) as BridgeTokens;
      if (j?.ySweetToken && typeof j.ySweetToken === "string" && j.ySweetToken.length > 5) {
        ySweetToken = j.ySweetToken;
        if (j.updatedAt) updatedAt = j.updatedAt;
        return j.ySweetToken;
      }
    }
  } catch {}
  // NOTE: ySweet is per-doc, minted via POST /api/y; fallback is mock_y_token for tests
  return (process.env.PRISM_Y_TOKEN as string | undefined) ?? "mock_y_token";
}

export function setSandboxToken(token: string): void {
  sandboxToken = token;
  updatedAt = Date.now();
  persistTokensSync();
}

export function setYSweetToken(token: string): void {
  ySweetToken = token;
  updatedAt = Date.now();
  persistTokensSync();
}

export function setBridgeTokens(tokens: BridgeTokens): void {
  if (tokens.sandboxToken !== undefined) sandboxToken = tokens.sandboxToken ?? null;
  if (tokens.ySweetToken !== undefined) ySweetToken = tokens.ySweetToken ?? null;
  updatedAt = tokens.updatedAt ?? Date.now();
  persistTokensSync();
}

export function clearBridgeTokens(): void {
  sandboxToken = null;
  ySweetToken = null;
  updatedAt = null;
  try {
    if (fs.existsSync(TOKEN_FILE)) fs.unlinkSync(TOKEN_FILE);
  } catch {}
  delete (globalThis as any).__BRIDGE_TOKENS__;
  delete (globalThis as any).__BRIDGE_SANDBOX_TOKEN__;
  delete (globalThis as any).__BRIDGE_Y_TOKEN__;
}

export function isBridgeActive(): boolean {
  return !!pwPage && !!pwContext;
}

export function wasHeartbeatSeen(): boolean {
  return heartbeatSeen;
}

// ---------------------------------------------------------------------------
// Token capture helpers (used by page.on handlers)
// ---------------------------------------------------------------------------
function captureSandboxTokenFromHeaders(headers: Record<string, string>): boolean {
  // header names are lowercased by playwright
  const tok = (headers as any)["x-crixet-sandbox-token"] ?? (headers as any)["x-crixet-sandbox-token".toLowerCase()];
  if (tok && typeof tok === "string" && tok.length > 10) {
    if (tok !== sandboxToken) {
      setSandboxToken(tok);
    }
    return true;
  }
  return false;
}

async function captureYSweetFromResponse(response: any): Promise<boolean> {
  try {
    const url: string = response.url?.() ?? response.url ?? "";
    const req = response.request?.() ?? null;
    const method: string = req?.method?.() ?? req?.method ?? "";
    if (url.includes("/api/y") && method.toUpperCase() === "POST") {
      // try JSON body
      let json: any = null;
      try {
        json = await response.json();
      } catch {
        try {
          const txt = await response.text();
          json = JSON.parse(txt);
        } catch {}
      }
      if (json?.token && typeof json.token === "string") {
        if (json.token !== ySweetToken) setYSweetToken(json.token);
        return true;
      }
      // also check header fallback (some Prism variants return token in header)
      const headers = response.headers?.() ?? {};
      const htok = (headers as any)["x-y-token"] ?? (headers as any)["y-token"];
      if (htok) {
        setYSweetToken(htok);
        return true;
      }
    }
  } catch {}
  return false;
}

// ---------------------------------------------------------------------------
// Heartbeat wait
// ---------------------------------------------------------------------------
export async function waitForHeartbeat(page: any, timeoutMs: number = 30000): Promise<void> {
  // Prism heartbeat: typically POST https://prism.openai.com/api/heartbeat or GET /heartbeat
  // We wait for any response whose URL contains "heartbeat" with 2xx
  // Also accept "sync" or "y" as alternative readiness signal for older Prism
  await page.waitForResponse(
    (res: any) => {
      try {
        const url: string = res.url?.() ?? String(res.url);
        const status: number = res.status?.() ?? res.status;
        if (url.includes("heartbeat") && status >= 200 && status < 300) {
          heartbeatSeen = true;
          return true;
        }
        // fallback: any successful api/y or render heartbeat indicates doc loaded
        if (url.includes("/api/y") && status === 200) {
          heartbeatSeen = true;
          // not strictly heartbeat, but indicates readiness, treat as pass after 1 heartbeat hasn't arrived within timeout
          // we don't return true here to avoid false positive, but we mark seen
        }
      } catch {}
      return false;
    },
    { timeout: timeoutMs },
  );
}

// ---------------------------------------------------------------------------
// Auto-refresh on 401 / syncing
// ---------------------------------------------------------------------------
export function shouldRefreshOnStatus(status: number, bodyText?: string): boolean {
  if (status === 401) return true;
  if (typeof bodyText === "string") {
    const lower = bodyText.toLowerCase();
    if (lower.includes("syncing") || lower.includes("sync") || lower.includes("unauthorized") || lower.includes("forbidden")) {
      return true;
    }
  }
  return false;
}

export async function handleAuthFailure(status: number, bodyText?: string): Promise<boolean> {
  if (!shouldRefreshOnStatus(status, bodyText)) return false;
  // Use dynamic indirection so vi.spyOn(bridge, "refreshBridge") can intercept in tests
  // (direct internal call would bypass the spied exported binding)
  try {
    const mod: any = await import("./playwright");
    if (mod?.refreshBridge && typeof mod.refreshBridge === "function") {
      await mod.refreshBridge();
      return true;
    }
  } catch {}
  await refreshBridge();
  return true;
}

export async function refreshBridge(): Promise<void> {
  if (isRefreshing) return;
  isRefreshing = true;
  try {
    if (pwPage) {
      try {
        await pwPage.reload({ waitUntil: "domcontentloaded", timeout: 15000 });
      } catch {}
      try {
        await waitForHeartbeat(pwPage, 20000);
      } catch {}
      // after reload, headers will be captured again via page.on handlers
      // also proactively re-mint Y token via fetch if needed
      try {
        await mintYSweetTokenViaFetch();
      } catch {}
    } else {
      // no page: try to re-mint via fetch fallback (for CI or when browser not running)
      try {
        await mintYSweetTokenViaFetch();
      } catch {}
      // also try to refresh sandbox token via fetch? sandbox token is only via header capture, so we reload if possible
      // if no page, attempt to restart bridge if env indicates live mode
      if (process.env.BRIDGE_AUTO_START === "1") {
        try {
          await startPlaywrightBridge({ headless: true });
        } catch {}
      }
    }
  } finally {
    isRefreshing = false;
  }
}

async function mintYSweetTokenViaFetch(docId?: string): Promise<string> {
  const id = docId ?? process.env.PRISM_DOC_ID ?? "mock-doc";
  // Prefer bridge token if we have one, but mint is independent
  try {
    const r = await fetch("https://prism.openai.com/api/y", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ docId: id, requestContext: { source: "initial-bootstrap" } }),
    });
    const j = (await r.json()) as { token?: string };
    if (j.token) {
      setYSweetToken(j.token);
      return j.token;
    }
  } catch {}
  return getYSweetToken();
}

// ---------------------------------------------------------------------------
// Main launch
// ---------------------------------------------------------------------------
export async function startPlaywrightBridge(opts?: { headless?: boolean; timeoutMs?: number }): Promise<{ context: any; page: any }> {
  const headless = opts?.headless ?? false;
  const timeoutMs = opts?.timeoutMs ?? 30000;

  // Ensure userDataDir exists
  try {
    fs.mkdirSync(USER_DATA_DIR, { recursive: true });
  } catch {}

  // Dynamic import so CI without playwright still passes tsc/vitest (mocked bridge)
  let chromium: any;
  try {
    // @ts-ignore - playwright is optional, only needed for live bridge (not CI)
    const mod: any = await import("playwright");
    chromium = mod.chromium ?? mod.default?.chromium ?? mod;
    if (!chromium || !chromium.launchPersistentContext) {
      // try playwright-core fallback
      // @ts-ignore - optional fallback
      const mod2: any = await import("playwright-core").catch(() => null);
      chromium = mod2?.chromium ?? chromium;
    }
    if (!chromium) throw new Error("playwright chromium not found");
  } catch (e: any) {
    throw new Error(`playwright not installed — run \`bun add -d playwright && bunx playwright install chromium\`. Original: ${e?.message ?? String(e)}`);
  }

  // If already running, return existing
  if (pwContext && pwPage) {
    try {
      // verify still connected
      if (typeof pwPage.isClosed === "function" && !pwPage.isClosed()) {
        return { context: pwContext, page: pwPage };
      }
    } catch {}
    // cleanup stale
    try {
      await stopPlaywrightBridge();
    } catch {}
  }

  // Launch persistent context (preserves login / cookies across restarts)
  pwContext = await chromium.launchPersistentContext(USER_DATA_DIR, {
    headless,
    viewport: { width: 1280, height: 900 },
    // args that help with Prism's sandbox iframe
    args: ["--disable-blink-features=AutomationControlled"],
  });

  // Ensure we have a page
  let pages: any[] = [];
  try {
    pages = pwContext.pages?.() ?? [];
  } catch {}
  pwPage = pages[0] ?? (await pwContext.newPage());

  // Attach token capture handlers BEFORE goto so early requests are not missed
  // Capture sandbox token via request headers
  pwPage.on("request", (req: any) => {
    try {
      const headers: Record<string, string> = req.headers?.() ?? {};
      captureSandboxTokenFromHeaders(headers);
      // also capture Y token request header if present (some Prism versions send ASQ in header)
      const yHeader = (headers as any)["authorization"] ?? (headers as any)["x-y-token"];
      if (yHeader && typeof yHeader === "string" && yHeader.startsWith("ASQ")) {
        // Y token sometimes prefixed with Bearer
        const tok = yHeader.replace(/^Bearer\s+/i, "").trim();
        if (tok.length > 20) setYSweetToken(tok);
      }
    } catch {}
  });

  pwPage.on("response", async (res: any) => {
    try {
      await captureYSweetFromResponse(res);
      // also capture sandbox token from response headers if present (some endpoints echo it)
      const headers = res.headers?.() ?? {};
      captureSandboxTokenFromHeaders(headers as Record<string, string>);
      // heartbeat detection
      try {
        const url: string = res.url?.() ?? "";
        if (url.includes("heartbeat") && res.status() >= 200 && res.status() < 300) {
          heartbeatSeen = true;
        }
      } catch {}
    } catch {}
  });

  // Also capture via requestfinished for tokens that are in request headers but not response
  // (already handled above, but keep for completeness)

  // Navigate to Prism doc
  await pwPage.goto(PRISM_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs });

  // Wait for heartbeat (readiness)
  try {
    await waitForHeartbeat(pwPage, timeoutMs);
  } catch (e) {
    // fallback: wait a bit for any network idle, then consider doc loaded if tokens already captured
    try {
      await pwPage.waitForLoadState?.("networkidle", { timeout: 5000 });
    } catch {}
    // if still no heartbeat but tokens captured, allow continue (Prism sometimes uses websocket heartbeat)
    if (!heartbeatSeen && !sandboxToken) {
      // not fatal in mocked CI, but in live mode we at least wait a bit more
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  // After navigation, also try to read tokens from page's localStorage/cookies if needed
  // (Prism sometimes stores sandbox token in localStorage)
  try {
    const lsToken: string | null = await pwPage.evaluate(() => {
      try {
        // common keys Prism uses
        const keys = ["crixet-sandbox-token", "sandbox-token", "x-crixet-sandbox-token", "prism-sandbox-token"];
        for (const k of keys) {
          const v = (globalThis as any).localStorage?.getItem(k);
          if (v) return v;
        }
        // also check window global
        const g: any = globalThis as any;
        if (g.__CRIXET_SANDBOX_TOKEN__) return g.__CRIXET_SANDBOX_TOKEN__;
        if (g.__SANDBOX_TOKEN__) return g.__SANDBOX_TOKEN__;
      } catch {}
      return null;
    });
    if (lsToken && typeof lsToken === "string" && lsToken.length > 10) {
      setSandboxToken(lsToken);
    }
  } catch {}

  // Ensure tokens are persisted and exposed
  persistTokensSync();

  // Set up auto-refresh on page close / crash
  try {
    pwPage.on("close", async () => {
      // attempt auto-restart after short delay if not explicitly stopped
      await new Promise((r) => setTimeout(r, 1000));
      if (!isRefreshing && pwContext) {
        try {
          await refreshBridge();
        } catch {}
      }
    });
  } catch {}

  return { context: pwContext, page: pwPage };
}

export async function stopPlaywrightBridge(): Promise<void> {
  try {
    if (pwPage) {
      try {
        pwPage.removeAllListeners?.("request");
        pwPage.removeAllListeners?.("response");
        pwPage.removeAllListeners?.("close");
      } catch {}
    }
  } catch {}
  try {
    if (pwContext) await pwContext.close();
  } catch {}
  try {
    if (pwBrowser) await pwBrowser.close();
  } catch {}
  pwContext = null;
  pwPage = null;
  pwBrowser = null;
  heartbeatSeen = false;
}

// ---------------------------------------------------------------------------
// CLI entry: `bun src/bridge/playwright.ts` starts the bridge
// ---------------------------------------------------------------------------
if (import.meta.main) {
  const headless = process.argv.includes("--headless");
  console.log(`[bridge] starting persistent context at ${USER_DATA_DIR}`);
  console.log(`[bridge] prism url: ${PRISM_URL}`);
  startPlaywrightBridge({ headless })
    .then(({ page }) => {
      console.log(`[bridge] launched — heartbeatSeen=${heartbeatSeen}, sandboxToken=${sandboxToken ? sandboxToken.slice(0, 12) + "..." : "none"}, yToken=${ySweetToken ? ySweetToken.slice(0, 12) + "..." : "none"}`);
      console.log(`[bridge] tokens file: ${TOKEN_FILE}`);
      console.log(`[bridge] GET http://localhost:8787/__bridge/token to inspect (when server running)`);
      // keep process alive
      setInterval(() => {
        const t = getBridgeTokens();
        console.log(`[bridge] heartbeatSeen=${heartbeatSeen} sandbox=${t.sandboxToken ? t.sandboxToken.slice(0, 8) + "..." : "none"} y=${t.ySweetToken ? t.ySweetToken.slice(0, 8) + "..." : "none"}`);
      }, 30000);
    })
    .catch((e) => {
      console.error("[bridge] failed to start:", e);
      process.exit(1);
    });
}

// Re-export for consumers that need to check file
export { persistTokensSync as _persistTokensSync, loadTokensFromFileSync as _loadTokensFromFileSync };
