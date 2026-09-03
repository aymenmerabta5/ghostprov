import { createHash } from "node:crypto";
import { chmod, copyFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import WebSocket from "ws";
import { assertCandidatePort, DEFAULT_CANDIDATE_PORT } from "../src/core/ports";
import { FleetManifestStore } from "../src/fleet/manifest";
import { resolveGhostProvApiKey } from "../src/http/api-key";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import type { LoggerLike, LoggerSnapshot } from "../src/observability/logger";

const LIVE_GUARD = "GP_LIVE_MEMORY_ACCOUNTING";
const MUTATION_GUARD = "GP_LIVE_ALLOW_TEST_PROJECT_MUTATION";
const MiB = 1024 ** 2;
const GiB = 1024 ** 3;

class QuietProofLogger implements LoggerLike {
  emit(): void {}
  snapshot(): LoggerSnapshot {
    return {
      queuedEvents: 0,
      queuedBytes: 0,
      droppedSamples: 0,
      droppedProgress: 0,
      droppedTerminals: 0,
      sinkErrors: 0,
    };
  }
  async close(): Promise<void> {}
}

export interface LiveMemoryAccountingOptions {
  readonly port: 18_888;
  readonly cookieFile: string;
  readonly origin: string;
  readonly model: string;
  readonly timeoutMs: number;
}

interface DedicatedProjectLedger {
  id: ProjectId | null;
  title: string | null;
  intended: boolean;
}

export function resolveLiveMemoryAccountingOptions(
  env: Record<string, string | undefined>,
): LiveMemoryAccountingOptions {
  if (env[LIVE_GUARD] !== "1") {
    throw new Error(`Set ${LIVE_GUARD}=1 to run the live memory-accounting proof`);
  }
  if (env[MUTATION_GUARD] !== "1") {
    throw new Error(`Set ${MUTATION_GUARD}=1 to create and delete one dedicated test project`);
  }
  resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("PRISM_COOKIE_FILE is required");
  const port = Number(env.PORT ?? DEFAULT_CANDIDATE_PORT);
  assertCandidatePort(port, "Live memory-accounting proof");
  if (port !== DEFAULT_CANDIDATE_PORT) {
    throw new Error(`Live memory-accounting proof is hard-fenced to ${DEFAULT_CANDIDATE_PORT}`);
  }
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 10 * 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 30_000 || timeoutMs > 30 * 60_000) {
    throw new Error("GP_LIVE_TIMEOUT_MS must be an integer from 30000 through 1800000");
  }
  const model = env.GP_LIVE_MODEL?.trim() || "gpt-5.6-sol";
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(model)) {
    throw new Error("GP_LIVE_MODEL is invalid");
  }
  return Object.freeze({
    port: DEFAULT_CANDIDATE_PORT,
    cookieFile: path.resolve(cookieFile),
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    model,
    timeoutMs,
  });
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    timer.unref?.();
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}

async function waitUntil(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error(`${label} did not settle`);
}

function assertTemporaryRoot(root: string): void {
  const relative = path.relative(path.resolve(tmpdir()), path.resolve(root));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("Live memory proof temporary root escaped the OS temp directory");
  }
}

function hashProject(id: ProjectId): string {
  return createHash("sha256").update(id).digest("hex").slice(0, 16);
}

async function createDedicatedProject(
  env: Record<string, string | undefined>,
  options: LiveMemoryAccountingOptions,
  ledger: DedicatedProjectLedger,
): Promise<string> {
  const session = await PrismSessionManager.create({
    origin: options.origin,
    mode: "authenticated",
    allowAnonymous: false,
    cookieFile: options.cookieFile,
    initialCookieHeader: env.PRISM_COOKIE?.trim(),
  });
  try {
    const identity = await withTimeout(session.resolve(), 60_000, "Prism identity resolution");
    const projects = new PrismProjectsClient(identity.account);
    const before = new Set((await withTimeout(projects.list(), 60_000, "Prism project listing"))
      .map(value => value.projectId));
    let id: ProjectId;
    do id = projectId(crypto.randomUUID()); while (before.has(id));
    const title = `GhostProv Memory Test ${new Date().toISOString()}`;
    ledger.id = id;
    ledger.title = title;
    ledger.intended = true;
    const created = await withTimeout(projects.create({ projectId: id, title }), 60_000, "test project creation");
    if (created.projectId !== id || created.title !== title || created.deleted) {
      throw new Error("Dedicated live test project identity did not round-trip");
    }
    return identity.fingerprint;
  } finally {
    await session.close();
  }
}

async function deleteDedicatedProject(
  env: Record<string, string | undefined>,
  options: LiveMemoryAccountingOptions,
  dedicated: DedicatedProjectLedger,
): Promise<void> {
  if (!dedicated.intended || !dedicated.id || !dedicated.title) return;
  if (env[MUTATION_GUARD] !== "1") throw new Error("Dedicated cleanup lost its mutation guard");
  const session = await PrismSessionManager.create({
    origin: options.origin,
    mode: "authenticated",
    allowAnonymous: false,
    cookieFile: options.cookieFile,
    initialCookieHeader: env.PRISM_COOKIE?.trim(),
  });
  try {
    const identity = await withTimeout(session.resolve(), 60_000, "cleanup identity resolution");
    const projects = new PrismProjectsClient(identity.account);
    let current;
    try {
      current = await withTimeout(projects.get(dedicated.id), 60_000, "cleanup metadata read");
    } catch (error) {
      const status = error && typeof error === "object" && "status" in error
        ? (error as { status?: unknown }).status
        : null;
      if (status === 404 || status === 410) return;
      throw error;
    }
    if (current.projectId !== dedicated.id || current.title !== dedicated.title) {
      throw new Error("Cleanup refused a project whose identity or title changed");
    }
    if (!current.deleted) {
      await withTimeout(projects.deleteManaged(dedicated.id), 60_000, "dedicated project cleanup");
    }
  } finally {
    await session.close();
  }
}

async function verifyHttp(base: string, apiKey: string, model: string): Promise<{
  firstEventMs: number;
  totalMs: number;
}> {
  const started = performance.now();
  const response = await fetch(`${base}/v1/responses`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      stream: true,
      store: true,
      max_output_tokens: 16,
      reasoning: { effort: "low" },
      input: "Reply with exactly MEMORY_HTTP_OK",
    }),
  });
  if (!response.ok || !response.body) throw new Error(`Memory HTTP canary failed (${response.status})`);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let firstEventMs: number | null = null;
  let body = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    firstEventMs ??= performance.now() - started;
    body += decoder.decode(value, { stream: true });
  }
  if (firstEventMs === null || !body.includes("response.completed")) {
    throw new Error("Memory HTTP canary did not complete");
  }
  return { firstEventMs, totalMs: performance.now() - started };
}

async function verifyWebSocket(
  base: string,
  apiKey: string,
  model: string,
  system: BrowserlessSystem,
  timeoutMs: number,
): Promise<{
  firstEventMs: number;
  totalMs: number;
  peakReservedBytes: number;
  peakRssBytes: number;
}> {
  const url = new URL("/v1/responses", base);
  url.protocol = "ws:";
  const socket = new WebSocket(url, { headers: { authorization: `Bearer ${apiKey}` } });
  await withTimeout(new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  }), 30_000, "candidate WebSocket open");
  let monitoring = true;
  let peakReservedBytes = system.governor.snapshot().reservedBytes;
  let peakRssBytes = process.memoryUsage().rss;
  const monitor = (async () => {
    while (monitoring) {
      peakReservedBytes = Math.max(peakReservedBytes, system.governor.snapshot().reservedBytes);
      peakRssBytes = Math.max(peakRssBytes, process.memoryUsage().rss);
      await Bun.sleep(5);
    }
  })();
  const started = performance.now();
  let firstEventMs: number | null = null;
  try {
    const completed = new Promise<void>((resolve, reject) => {
      socket.on("message", raw => {
        const event = JSON.parse(raw.toString());
        if (event.stream_id !== "memory-live") return;
        firstEventMs ??= performance.now() - started;
        if (event.type === "response.completed") resolve();
        if (event.type === "error" || event.type === "response.failed") {
          reject(new Error("Memory WebSocket canary failed"));
        }
      });
      socket.once("error", reject);
    });
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "memory-live",
      model,
      store: true,
      max_output_tokens: 16,
      reasoning: { effort: "low" },
      input: "Reply with exactly MEMORY_WS_OK",
    }));
    await withTimeout(completed, timeoutMs, "candidate WebSocket response");
    return {
      firstEventMs: firstEventMs ?? performance.now() - started,
      totalMs: performance.now() - started,
      peakReservedBytes,
      peakRssBytes,
    };
  } finally {
    monitoring = false;
    await monitor;
    socket.close();
    await withTimeout(new Promise<void>(resolve => socket.once("close", () => resolve())), 10_000, "candidate WebSocket close")
      .catch(() => undefined);
  }
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = resolveLiveMemoryAccountingOptions(env);
  const apiKey = resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  const root = await mkdtemp(path.join(tmpdir(), "ghostprov-memory-live-"));
  assertTemporaryRoot(root);
  const isolatedCookieFile = path.join(root, "candidate-cookies.json");
  let liveOptions = options;
  const dedicated: DedicatedProjectLedger = { id: null, title: null, intended: false };
  let system: BrowserlessSystem | null = null;
  let failure: unknown = null;
  let result: Record<string, unknown> | null = null;
  try {
    await copyFile(options.cookieFile, isolatedCookieFile);
    if (process.platform !== "win32") await chmod(isolatedCookieFile, 0o600);
    liveOptions = Object.freeze({ ...options, cookieFile: isolatedCookieFile });
    const identityFingerprint = await createDedicatedProject(env, liveOptions, dedicated);
    if (!dedicated.id || !dedicated.title) throw new Error("Dedicated project ledger is incomplete");
    const manifest = new FleetManifestStore({
      file: path.join(root, "fleet.json"),
      identityFingerprint,
    });
    system = await createBrowserlessSystem({
      ...env,
      GP_CANDIDATE: "1",
      PORT: String(options.port),
      GP_TARGET_SANDBOXES: "1",
      GP_MAX_PROJECTS: "1",
      GP_PROVISION_CONCURRENCY: "1",
      GP_COLD_START_CONCURRENCY: "1",
      GP_AUTO_CREATE_PROJECTS: "0",
      GP_FLEET_MANIFEST_FILE: path.join(root, "fleet.json"),
      GP_LOG_DIR: path.join(root, "logs"),
      PRISM_COOKIE_FILE: isolatedCookieFile,
      PRISM_PROJECT_IDS: dedicated.id,
    }, { manifest, logger: new QuietProofLogger() });
    await withTimeout(system.start(), options.timeoutMs, "candidate startup");
    await withTimeout(system.fleet.waitForReady(1), options.timeoutMs, "candidate readiness");

    const idleReservedBytes = system.governor.snapshot().reservedBytes;
    const idleRssBytes = process.memoryUsage().rss;
    if (idleReservedBytes !== system.hostEventMemory.reservedBytes || idleReservedBytes >= GiB) {
      throw new Error("Idle candidate reservation is not the fixed event pool below one GiB");
    }
    if (idleRssBytes >= GiB) throw new Error("Idle candidate RSS exceeded one GiB");
    const base = `http://127.0.0.1:${options.port}`;
    const http = await withTimeout(verifyHttp(base, apiKey, options.model), options.timeoutMs, "HTTP proof");
    await waitUntil(
      () => system!.router.snapshot().active === 0 &&
        system!.governor.snapshot().reservedBytes === system!.hostEventMemory.reservedBytes,
      30_000,
      "HTTP memory release",
    );
    const websocket = await verifyWebSocket(base, apiKey, options.model, system, options.timeoutMs);
    if (websocket.peakReservedBytes >= GiB) {
      throw new Error("Normal WebSocket request exceeded one GiB of governor reservation");
    }
    if (websocket.peakRssBytes >= GiB) {
      throw new Error("Normal WebSocket request exceeded one GiB of process RSS");
    }
    await waitUntil(
      () => system!.webSocketIngressMemory.reservedBytes === 0 &&
        system!.governor.snapshot().reservedBytes === system!.hostEventMemory.reservedBytes,
      30_000,
      "WebSocket memory release",
    );
    result = {
      live: true,
      candidatePort: options.port,
      protectedPortTouched: false,
      projectHash: hashProject(dedicated.id),
      idleReservedMiB: Math.round(idleReservedBytes / MiB),
      idleRssMiB: Math.round(idleRssBytes / MiB),
      websocketPeakReservedMiB: Math.round(websocket.peakReservedBytes / MiB),
      websocketPeakRssMiB: Math.round(websocket.peakRssBytes / MiB),
      httpFirstEventMs: Math.round(http.firstEventMs),
      httpTotalMs: Math.round(http.totalMs),
      websocketFirstEventMs: Math.round(websocket.firstEventMs),
      websocketTotalMs: Math.round(websocket.totalMs),
    };
  } catch (error) {
    failure = error;
  } finally {
    if (system) await system.close().catch(error => { failure ??= error; });
    await deleteDedicatedProject(env, liveOptions, dedicated).catch(error => { failure ??= error; });
    assertTemporaryRoot(root);
    await rm(root, { recursive: true, force: true }).catch(error => { failure ??= error; });
  }
  if (failure) throw failure;
  console.log(JSON.stringify({ ...result, projectDeleted: true }, null, 2));
}

if (import.meta.main) await main();
