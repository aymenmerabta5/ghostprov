/**
 * Playwright persistent bridge for Prism token minting (Task Bridge)
 *
 * - Launches Chromium with the configured persistent GP_USER_DATA_DIR
 * - Navigates to PRISM_PROJECT_URL (or the URL derived from PRISM_DOC_ID)
 * - Waits for heartbeat (waitForResponse heartbeat)
 * - Captures x-crixet-sandbox-token via page.on('request') header
 * - Captures Y Sweet ASQ token via POST /api/y response {token}
 * - Exposes tokens to Hono via GET /__bridge/token, GP_TOKEN_FILE, and globalThis.__BRIDGE_TOKENS__
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

import "dotenv/config";
// Bun -> tsx re-exec for direct `bun src/bridge/playwright.ts` (same hang as server) — only when run as main, not when imported by server
if (typeof process !== "undefined" && (process as any).versions?.bun && !process.env.GHOSTPROV_VIA_TSX && !process.env.VITEST && (import.meta as any).main) {
  try {
    const { spawnSync } = await import("node:child_process");
    const bunBin = (process as any).execPath;
    const args = process.argv.slice(1);
    const script = args.find((a: string) => a.includes("playwright.ts")) ?? process.argv[1];
    const extra = args.filter((a: string) => a !== script && !a.includes("playwright.ts") && !a.endsWith("bun.exe"));
    console.log(`[bridge] bun detected — re-exec via bun x tsx ${script} ${extra.join(" ")}`);
    const res = spawnSync(bunBin, ["x", "tsx", script, ...extra], { stdio: "inherit", env: { ...process.env, GHOSTPROV_VIA_TSX: "1" } });
    process.exit(res.status ?? 0);
  } catch {}
}

import * as fs from "node:fs";
import * as path from "node:path";
import { launchBridgeContext, resolveBridgeConfig } from "./config";

// ---------------------------------------------------------------------------
// Constants (task spec)
// ---------------------------------------------------------------------------
// Project is env-driven so ghostprov can move between Prism projects without
// code changes (the sandbox allocation of an exhausted project may be blocked).
export const BRIDGE_CONFIG = resolveBridgeConfig();
export const PRISM_URL = BRIDGE_CONFIG.prismUrl;
export const USER_DATA_DIR = BRIDGE_CONFIG.userDataDir;
export const TOKEN_FILE = BRIDGE_CONFIG.tokenFile;
// legacy aliases for consumers that expect file at .pw/.tokens.json or .bridge
export const BRIDGE_TOKEN_FILE = TOKEN_FILE;
export const ENV_FILE = BRIDGE_CONFIG.envFile;

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

// Also hydrate from env on first load for CI fallback — treat empty string as unset to allow auto-mint
if (!sandboxToken && process.env.PRISM_SANDBOX_TOKEN && process.env.PRISM_SANDBOX_TOKEN.trim().length > 5) {
  sandboxToken = process.env.PRISM_SANDBOX_TOKEN.trim();
}
if (!ySweetToken && process.env.PRISM_Y_TOKEN && process.env.PRISM_Y_TOKEN.trim().length > 5) {
  ySweetToken = process.env.PRISM_Y_TOKEN.trim();
}

function updateEnvFileIfNeeded(): void {
  try {
    // Skip .env update during vitest to avoid polluting .env with test mock tokens (e.g. bridge-sandbox-xyz)
    if (process.env.VITEST || process.env.NODE_ENV === "test") return;
    // Only update .env when we have a real sandbox token (length > 10) to avoid writing mock
    if (!sandboxToken || sandboxToken === "mock-token" || sandboxToken.length < 10) return;
    // Never write obvious mock tokens from tests
    if (sandboxToken.includes("mock") || sandboxToken.includes("bridge-sandbox") || sandboxToken === "mock_y_token") return;
    if (!fs.existsSync(ENV_FILE)) return;
    let content = fs.readFileSync(ENV_FILE, "utf8");
    const hasKey = /^PRISM_SANDBOX_TOKEN=.*$/m.test(content);
    if (hasKey) {
      const m = content.match(/^PRISM_SANDBOX_TOKEN=(.*)$/m);
      const current = m ? m[1].trim().replace(/^["']|["']$/g, "") : "";
      // Update when empty OR when bridge has newer valid token (keep .env in sync)
      if (!current || current.length < 10 || current !== sandboxToken) {
        // only overwrite if bridge token is clearly real (gAAAA, >50 chars)
        if (sandboxToken && sandboxToken.startsWith("gAAAA") && sandboxToken.length > 50) {
          content = content.replace(/^PRISM_SANDBOX_TOKEN=.*$/m, `PRISM_SANDBOX_TOKEN=${sandboxToken}`);
          fs.writeFileSync(ENV_FILE, content, "utf8");
        } else if (!current || current.length < 5) {
          content = content.replace(/^PRISM_SANDBOX_TOKEN=.*$/m, `PRISM_SANDBOX_TOKEN=${sandboxToken}`);
          fs.writeFileSync(ENV_FILE, content, "utf8");
        }
      }
    } else {
      // append if missing
      if (!content.endsWith("\n")) content += "\n";
      content += `PRISM_SANDBOX_TOKEN=${sandboxToken}\n`;
      fs.writeFileSync(ENV_FILE, content, "utf8");
    }
  } catch {}
}

function persistTokensSync(): void {
  try {
    fs.mkdirSync(path.dirname(TOKEN_FILE), { recursive: true });
    const payload: BridgeTokens = {
      sandboxToken,
      ySweetToken,
      updatedAt: updatedAt ?? Date.now(),
    };
    fs.writeFileSync(TOKEN_FILE, JSON.stringify(payload, null, 2), "utf8");
    (globalThis as any).__BRIDGE_TOKENS__ = payload;
    // expose individual globals for legacy consumers
    if (sandboxToken) (globalThis as any).__BRIDGE_SANDBOX_TOKEN__ = sandboxToken;
    if (ySweetToken) (globalThis as any).__BRIDGE_Y_TOKEN__ = ySweetToken;
  } catch {}
  // also update .env when sandbox token minted (auto-mint bridge)
  updateEnvFileIfNeeded();
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
  const envTok = (process.env.PRISM_SANDBOX_TOKEN ?? "").trim();
  return envTok.length > 5 ? envTok : "mock-token";
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
  const envY = (process.env.PRISM_Y_TOKEN ?? "").trim();
  return envY.length > 5 ? envY : "mock_y_token";
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

export function getBridgePage(): any | null {
  return pwPage ?? null;
}

export function getBridgeContext(): any | null {
  return pwContext ?? null;
}

export function wasHeartbeatSeen(): boolean {
  return heartbeatSeen;
}

// ---------------------------------------------------------------------------
// Token capture helpers (used by page.on handlers)
// ---------------------------------------------------------------------------
function captureSandboxTokenFromHeaders(headers: Record<string, string>): boolean {
  // header names may be any case; playwright lowercases but we handle both
  let tok: string | undefined;
  for (const [k, v] of Object.entries(headers ?? {})) {
    if (k.toLowerCase() === "x-crixet-sandbox-token" && typeof v === "string" && v.length > 10) {
      tok = v;
      break;
    }
  }
  // also check direct access for speed
  if (!tok) {
    tok = (headers as any)["x-crixet-sandbox-token"] ?? (headers as any)["X-Crixet-Sandbox-Token"] ?? (headers as any)["x-crixet-sandbox-token".toLowerCase()];
  }
  if (tok && typeof tok === "string" && tok.length > 10) {
    // gAAAA tokens are ~100+ chars; also handle other prefixes
    if (tok !== sandboxToken) {
      setSandboxToken(tok);
      console.log(`[bridge] captured sandbox token ${tok.slice(0,12)}... via headers`);
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
  // Prism heartbeat: POST https://prism.openai.com/api/heartbeat or GET /heartbeat
  // Also treat successful /api/y or /s/sandboxes/proxy/* as readiness (Prism sometimes uses Yjs ws only)
  await page.waitForResponse(
    (res: any) => {
      try {
        const url: string = res.url?.() ?? String(res.url);
        const status: number = res.status?.() ?? res.status;
        if (url.includes("heartbeat") && status >= 200 && status < 300) {
          heartbeatSeen = true;
          return true;
        }
        if (url.includes("/api/y") && status === 200 && url.includes("prism.openai.com")) {
          heartbeatSeen = true;
          return true;
        }
        if (url.includes("/s/sandboxes/proxy/") && status >= 200 && status < 300) {
          heartbeatSeen = true;
          return true;
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
      // keep browser open: actively re-trigger heartbeat via console command to re-mint token
      try {
        await pwPage.evaluate(async (docId: string) => {
          try {
            await fetch("/s/sandboxes/proxy/heartbeat?prism_cache_bust=" + Date.now()).catch(() => {});
            await fetch("/api/y", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ docId, requestContext: { source: "initial-bootstrap" } }) }).catch(() => {});
          } catch {}
        }, BRIDGE_CONFIG.docId);
        await new Promise((r) => setTimeout(r, 1500));
      } catch {}
      // after reload, headers will be captured again via context.on handlers
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

  // Ensure the configured persistent profile exists.
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

  // Pre-clean session restore that causes "Chromium didn't shut down correctly. Restore pages?"
  // Fix Preferences exit_type and remove Sessions that trigger bubble
  // Also clean lock files left by killed chromium (SingleLock)
  try {
    for (const lock of ["SingletonLock", "SingletonSocket", "SingletonCookie", "lockfile"]) {
      const lp = path.join(USER_DATA_DIR, lock);
      if (fs.existsSync(lp)) { try { fs.unlinkSync(lp); console.log(`[bridge] removed lock ${lock}`); } catch {} }
    }
  } catch {}

  try {
    const prefPath = path.join(USER_DATA_DIR, "Default", "Preferences");
    if (fs.existsSync(prefPath)) {
      try {
        const raw = fs.readFileSync(prefPath, "utf8");
        const j = JSON.parse(raw);
        let changed = false;
        if (j.profile) {
          if (j.profile.exit_type !== "Normal") { j.profile.exit_type = "Normal"; changed = true; }
          if (j.profile.exited !== true) { j.profile.exited = true; changed = true; }
        }
        // also ensure top-level exit_type
        if ((j as any).profile) {}
        if (changed) fs.writeFileSync(prefPath, JSON.stringify(j));
        console.log("[bridge] cleaned Preferences exit_type to Normal");
      } catch (e) { console.warn("[bridge] pref clean failed", (e as any)?.message); }
    }
  } catch {}
  try {
    // Remove Sessions/Tabs that trigger restore bubble (safe, cookies preserved in Cookies DB)
    const sessDir = path.join(USER_DATA_DIR, "Default", "Sessions");
    if (fs.existsSync(sessDir)) {
      fs.rmSync(sessDir, { recursive: true, force: true });
      console.log("[bridge] removed Sessions dir to suppress restore bubble");
    }
    // Also remove Tabs files at top of Sessions backup
    const tabs1 = path.join(USER_DATA_DIR, "Default", "Tabs");
    const tabs2 = path.join(USER_DATA_DIR, "Default", "Session Storage");
    // don't delete Session Storage, just log
  } catch {}

  console.log(`[bridge] launching persistent context headless=${headless} dir=${USER_DATA_DIR} ...`);
  try {
    pwContext = await launchBridgeContext(chromium, BRIDGE_CONFIG, headless);
    console.log("[bridge] persistent context launched");
  } catch (e: any) {
    console.warn(`[bridge] launch failed: ${e?.message}; cleaning only this bridge profile before retry`);
    try { await stopPlaywrightBridge(); } catch {}
    const profileRoot = path.resolve(USER_DATA_DIR) + path.sep;
    for (const relative of ["Default/Sessions", "Default/shared_proto_db", "Default/Cache"]) {
      const target = path.resolve(USER_DATA_DIR, relative);
      if (!target.startsWith(profileRoot)) throw new Error(`refusing out-of-profile cleanup: ${target}`);
      try { if (fs.existsSync(target)) fs.rmSync(target, { recursive: true, force: true }); } catch {}
    }
    pwContext = await launchBridgeContext(chromium, BRIDGE_CONFIG, headless);
    console.log("[bridge] persistent context launched on retry");
  }

  // Robust page handling: Chromium after restore may have about:blank + bubble.
  // Close all existing pages and start fresh to guarantee navigation.
  let pages: any[] = [];
  try {
    pages = pwContext.pages?.() ?? [];
  } catch {}
  console.log(`[bridge] initial pages: ${pages.length} urls: ${pages.map((p:any)=>{try{return p.url()}catch{return "?"}}).join(", ")}`);
  try {
    // Close any about:blank or chrome:// pages; keep at most one prism tab
    for (const p of pages) {
      try {
        const u = p.url?.() ?? "";
        if (u === "about:blank" || u.startsWith("chrome://") || u.startsWith("chrome-error://")) {
          // keep one blank to reuse, close extras
        }
      } catch {}
    }
    // If we have >1 pages, close extras to avoid confusion
    if (pages.length > 1) {
      for (let i = 1; i < pages.length; i++) {
        try { await pages[i].close(); } catch {}
      }
      pages = pwContext.pages?.() ?? [];
    }
  } catch {}
  // Prefer existing prism tab; otherwise create FRESH page (about:blank pages can be black/corrupt)
  let prismFound: any = null;
  for (const p of pages) {
    try { if ((p.url?.() ?? "").includes("prism.openai.com")) { prismFound = p; break; } } catch {}
  }
  if (prismFound) {
    pwPage = prismFound;
    console.log(`[bridge] found existing prism tab, reusing`);
  } else {
    // Close the stale about:blank and create fresh to fix black screen
    try {
      if (pages[0]) {
        const u0 = pages[0].url?.() ?? "";
        if (u0 === "about:blank" || u0.startsWith("chrome://")) {
          // For persistent context, closing the last page closes the browser — reuse it
          if (!headless && pages.length === 1) {
            pwPage = pages[0];
            console.log("[bridge] reusing blank page (persistent, keep last page open)");
          } else {
            try { await pages[0].close(); console.log("[bridge] closed stale blank page"); } catch {}
            pwPage = await pwContext.newPage();
            console.log(`[bridge] created fresh page`);
          }
        } else {
          pwPage = pages[0];
          console.log(`[bridge] using page url: ${pwPage.url?.()}`);
        }
      } else {
        pwPage = await pwContext.newPage();
        console.log(`[bridge] created fresh page (no pages)`);
      }
    } catch {
      pwPage = await pwContext.newPage();
      console.log(`[bridge] created fresh page fallback`);
    }
  }

  // Attach token capture handlers BEFORE goto so early requests are not missed
  // Use context-level listeners to capture from any page/tab (Prism opens in second tab, first is about:blank)
  const attachRequestHandler = (req: any) => {
    try {
      const headers: Record<string, string> = req.headers?.() ?? {};
      captureSandboxTokenFromHeaders(headers);
      // also check all header keys case-insensitive for ASQ token
      for (const [k,v] of Object.entries(headers)) {
        if (typeof v === "string" && v.startsWith("ASQ") && v.length > 20) {
          const tok = v.replace(/^Bearer\s+/i, "").trim();
          if (tok.length > 20) setYSweetToken(tok);
        }
      }
      const yHeader = (headers as any)["authorization"] ?? (headers as any)["x-y-token"] ?? (headers as any)["Authorization"];
      if (yHeader && typeof yHeader === "string" && yHeader.startsWith("ASQ")) {
        const tok = yHeader.replace(/^Bearer\s+/i, "").trim();
        if (tok.length > 20) setYSweetToken(tok);
      }
      // also check URL for token in query (some Prism versions pass ?token=ASQ...)
      try {
        const url: string = req.url?.() ?? "";
        const m = url.match(/[?&]token=(ASQ[^&]+)/);
        if (m && m[1] && m[1].length > 20) setYSweetToken(decodeURIComponent(m[1]));
      } catch {}
    } catch {}
  };
  const attachResponseHandler = async (res: any) => {
    try {
      await captureYSweetFromResponse(res);
      const headers = res.headers?.() ?? {};
      captureSandboxTokenFromHeaders(headers as Record<string, string>);
      try {
        const url: string = res.url?.() ?? "";
        if (url.includes("heartbeat") && res.status() >= 200 && res.status() < 300) {
          heartbeatSeen = true;
        }
      } catch {}
    } catch {}
  };
  // Listen on both context (all pages) and page (fallback)
  try {
    pwContext.on("request", attachRequestHandler);
    pwContext.on("response", attachResponseHandler);
  } catch {}
  pwPage.on("request", attachRequestHandler);
  pwPage.on("response", attachResponseHandler);

  // Also capture via requestfinished for tokens that are in request headers but not response
  // (already handled above, but keep for completeness)

  // Dismiss restore bubble if still present after launch (keyboard Esc, close infobar)
  try {
    // Native bubble is not in DOM, try pressing Esc and clicking via CDP
    try { await pwPage.keyboard.press("Escape").catch(()=>{}); } catch {}
    await new Promise((r)=>setTimeout(r, 400));
    // Also try DOM button if any
    try {
      const restoreBtn = pwPage.locator('button:has-text("Restore")').first();
      if (await restoreBtn.isVisible({ timeout: 800 }).catch(() => false)) {
        await restoreBtn.click().catch(() => {});
        await new Promise((r) => setTimeout(r, 400));
      }
    } catch {}
    // Try via evaluate
    try {
      await pwPage.evaluate(() => {
        document.querySelectorAll('button').forEach((b: any) => {
          if (b.textContent?.includes('Restore')) (b as any).click();
        });
      }).catch(() => {});
    } catch {}
  } catch {}
  // Force navigation to Prism - black about:blank means goto didn't actually run
  // Make visible and always reload
  let navigated = false;
  try {
    // Ensure page is foreground
    try { await pwPage.bringToFront().catch(()=>{}); } catch {}
    await new Promise(r=>setTimeout(r, 500));
    const curUrl: string = pwPage.url?.() ?? "";
    console.log(`[bridge] current url before nav: "${curUrl}"`);
    const needsNav = true; // always force reload to fix black about:blank
    // Only skip if already exactly on PRISM_URL and not black
    const alreadyCorrect = curUrl === PRISM_URL && !curUrl.includes("about:blank");
    if (alreadyCorrect) {
      console.log(`[bridge] already on correct prism doc`);
    } else {
      console.log(`[bridge] navigating to ${PRISM_URL} ...`);
      // Clear black about:blank by forcing about:blank -> prism via evaluate location
      try {
        // First try direct goto with load (more reliable than domcontentloaded for prism)
        await pwPage.goto(PRISM_URL, { waitUntil: "load", timeout: timeoutMs }).catch(async (e:any)=>{
          console.warn(`[bridge] goto load failed ${e?.message}, trying domcontentloaded`);
          await pwPage.goto(PRISM_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs });
        });
      } catch (e:any) {
        console.warn(`[bridge] goto attempt 1 failed ${e?.message}, retry via evaluate`);
        try {
          await pwPage.evaluate((url:string)=>{ location.href = url; }, PRISM_URL);
          await pwPage.waitForLoadState?.("domcontentloaded", {timeout: 15000}).catch(()=>{});
        } catch {}
        // final fallback
        await pwPage.goto(PRISM_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs }).catch(()=>{});
      }
      navigated = true;
      await new Promise(r=>setTimeout(r, 1500));
      const after = pwPage.url?.() ?? "";
      console.log(`[bridge] goto done, now at "${after}" title="${await pwPage.title().catch(()=>"?")}"`);
      // If still about:blank or black, try reload
      if (after === "about:blank" || after.startsWith("chrome://") || after === "") {
        console.warn(`[bridge] still blank after goto, forcing reload`);
        await pwPage.reload({waitUntil:"domcontentloaded", timeout:15000}).catch(()=>{});
        await new Promise(r=>setTimeout(r, 1000));
        await pwPage.goto(PRISM_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs }).catch(()=>{});
        console.log(`[bridge] after retry url="${pwPage.url?.()}"`);
      }
    }
  } catch (e: any) {
    console.warn(`[bridge] goto failed (${e?.message}), retrying ...`);
    try { await pwPage.goto(PRISM_URL, { waitUntil: "domcontentloaded", timeout: timeoutMs }); } catch {}
    navigated = true;
    console.log(`[bridge] retry goto done at "${pwPage.url?.()}"`);
  }
  // Give page a moment to initiate heartbeat/Y requests after nav
  if (navigated) await new Promise((r) => setTimeout(r, 2000));

  // --- Login detection (robust) ---
  let isLoggedIn: boolean | null = null;
  try {
    isLoggedIn = await pwPage.evaluate(async (docId: string) => {
      const body = document.body.innerText || "";
      // Expanded DOM signals
      if (body.includes("Personal workspace") || body.includes("Aimen Merabta") || body.includes("New Project") || body.includes("main.tex")) return true;
      const loginEl = document.querySelector('a[href*="auth"], [data-testid*="login"]');
      const loginButton = [...document.querySelectorAll("button")].find(button => button.textContent?.toLowerCase().includes("log in"));
      if (loginEl || loginButton) return false;
      if (body.toLowerCase().includes("sign in") && body.toLowerCase().includes("prism")) return false;
      try {
        const r = await fetch(`/api/project-access?d=${encodeURIComponent(docId)}`, { credentials: "include" });
        if (r.status === 401 || r.status === 403) return false;
        if (r.ok) return true;
      } catch {}
      // also check if we are on login/openai subdomain redirect
      if (location.hostname.includes("auth.openai.com") || location.pathname.includes("/login")) return false;
      return null;
    }, BRIDGE_CONFIG.docId);
  } catch {}
  console.log(`[bridge] login check: ${isLoggedIn}`);
  if (isLoggedIn === false) {
    console.log("[bridge] not logged in — keeping browser open, waiting for login to prism.openai.com (5min)...");
    try {
      await pwPage.waitForFunction((docId: string) => {
        const b = document.body.innerText || "";
        return b.includes("Personal workspace") || b.includes("New Project") || b.includes("main.tex") || document.body.innerHTML.includes(docId);
      }, BRIDGE_CONFIG.docId, { timeout: 300000 });
      console.log("[bridge] login detected — continuing");
      isLoggedIn = true;
      await new Promise((r) => setTimeout(r, 2000));
    } catch {
      console.warn("[bridge] login wait timeout — continuing anyway");
    }
  } else if (isLoggedIn === true) {
    console.log("[bridge] already logged in");
  } else {
    console.log("[bridge] login unknown — proceeding to heartbeat anyway");
  }

  // Wait for heartbeat (readiness) - also poll for manual navigation
  console.log(`[bridge] waiting for heartbeat (or manual nav) ${timeoutMs}ms ...`);
  try {
    await waitForHeartbeat(pwPage, timeoutMs);
    console.log(`[bridge] heartbeat seen=${heartbeatSeen}`);
  } catch (e) {
    console.warn(`[bridge] heartbeat wait timeout: ${(e as any)?.message}, trying fallback`);
    try {
      await pwPage.waitForLoadState?.("networkidle", { timeout: 5000 });
    } catch {}
    console.log(`[bridge] after fallback heartbeatSeen=${heartbeatSeen} sandboxToken=${sandboxToken ? sandboxToken.slice(0,10)+"..." : "none"}`);
    if (!heartbeatSeen && !sandboxToken) {
      await new Promise((r) => setTimeout(r, 2000));
      // Also check if user handly navigated to prism during wait
      try {
        const cur = pwPage.url?.() ?? "";
        console.log(`[bridge] post-wait url: ${cur}, checking manual nav`);
        if (cur.includes("prism.openai.com") && !sandboxToken) {
          console.log("[bridge] user may have handly navigated, keeping browser open for token capture...");
        }
      } catch {}
    }
  }

  // Active heartbeat trigger via console command (user request: automatically do heartbeat and keep browser open)
  // If no sandbox token captured yet, force a heartbeat fetch from the page's console to trigger token capture
  if (!sandboxToken) {
    try {
      await pwPage.evaluate(async (docId: string) => {
        try {
          await fetch("/s/sandboxes/proxy/heartbeat?prism_cache_bust=" + Date.now(), { headers: { "x-crixet-sandbox-token": "dummy" } }).catch(() => {});
          // also trigger a Y token mint via console
          await fetch("/api/y", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ docId, requestContext: { source: "initial-bootstrap" } }) }).catch(() => {});
        } catch {}
      }, BRIDGE_CONFIG.docId);
      // wait a bit for the request/response handlers to capture
      await new Promise((r) => setTimeout(r, 2000));
    } catch {}
  }

  // Start continuous polling for handly navigation (user goes to project manually)
  // Poll every 3s for 2min to catch token even if goto didn't trigger it
  (async () => {
    for (let i=0; i<40; i++) {
      await new Promise(r=>setTimeout(r, 3000));
      try {
        if (sandboxToken && ySweetToken) break;
        const cur = pwPage.url?.() ?? "";
        if (cur.includes("prism.openai.com")) {
          // try to trigger fetches that carry token
          try { await pwPage.evaluate(async(docId: string)=>{ try{await fetch("/api/y",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({docId,requestContext:{source:"poll"}})}).catch(()=>{});}catch{} }, BRIDGE_CONFIG.docId); } catch {}
          // also scan storage
          try {
            const tok = await pwPage.evaluate(()=>{ try{ for(let i=0;i<localStorage.length;i++){ const k=localStorage.key(i); const v=k?localStorage.getItem(k):null; if(v&&v.startsWith("gAAAA")&&v.length>50) return v; } }catch{} return null; });
            if (tok && tok.length>10) { setSandboxToken(tok as string); console.log(`[bridge] poll captured sandbox ${String(tok).slice(0,12)}...`); }
          } catch {}
        }
        if (sandboxToken) { console.log(`[bridge] poll: sandbox ready ${sandboxToken.slice(0,12)}...`); break; }
      } catch {}
    }
  })().catch(()=>{});

  // After navigation, also try to read tokens from page's localStorage/cookies/IndexedDB/global
  try {
    const lsToken: string | null = await pwPage.evaluate(() => {
      try {
        const keys = ["crixet-sandbox-token", "sandbox-token", "x-crixet-sandbox-token", "prism-sandbox-token", "x-crixet-sandbox-token", "PRISM_SANDBOX_TOKEN", "sandboxToken"];
        for (const k of keys) {
          const v = (globalThis as any).localStorage?.getItem(k);
          if (v && v.length > 10) return v;
        }
        // scan all localStorage for gAAAA… token
        try {
          for (let i = 0; i < localStorage.length; i++) {
            const k = localStorage.key(i);
            const v = k ? localStorage.getItem(k) : null;
            if (v && v.startsWith("gAAAA") && v.length > 50) return v;
          }
        } catch {}
        // check globals
        const g: any = globalThis as any;
        if (g.__CRIXET_SANDBOX_TOKEN__ && typeof g.__CRIXET_SANDBOX_TOKEN__ === "string" && g.__CRIXET_SANDBOX_TOKEN__.length > 10) return g.__CRIXET_SANDBOX_TOKEN__;
        if (g.__SANDBOX_TOKEN__ && typeof g.__SANDBOX_TOKEN__ === "string" && g.__SANDBOX_TOKEN__.length > 10) return g.__SANDBOX_TOKEN__;
        if (g.__PRISM_SANDBOX_TOKEN__ && typeof g.__PRISM_SANDBOX_TOKEN__ === "string" && g.__PRISM_SANDBOX_TOKEN__.length > 10) return g.__PRISM_SANDBOX_TOKEN__;
        // cookies
        try {
          const m = document.cookie.match(/crixet-sandbox-token=([^;]+)/);
          if (m && m[1] && m[1].length > 10) return decodeURIComponent(m[1]);
        } catch {}
      } catch {}
      return null;
    });
    if (lsToken && typeof lsToken === "string" && lsToken.length > 10) {
      console.log(`[bridge] found sandbox token via storage scan ${lsToken.slice(0,12)}...`);
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
    // If headless ephemeral, close underlying browser
    const b = (pwContext as any)?.__browser;
    if (b) try { await b.close(); } catch {}
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
