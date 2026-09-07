import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { FleetManifestStore } from "../src/fleet/manifest";
import { ObservabilityLogger } from "../src/observability/logger";
import { RotatingJsonlSink } from "../src/observability/rotating-jsonl";
import { resolveObservabilityConfig } from "../src/observability/config";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { parseFrames } from "../src/relay/sse";
import { projectId } from "../src/sandbox/types";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import { V2SandboxSupervisor, type V2SandboxSupervisorOptions } from "../src/v2/supervisor/controller";
import type { V2BootstrapGateway } from "../src/v2/bootstrap/controller";
import { V2_R10_RELEASE } from "../src/v2/sidecar/assets";
import { parseGitFsmonitorResult, type GitFsmonitorResult } from "../src/v2/supervisor/git-fsmonitor";
import { proveSignedShutdown } from "./lib/git-fsmonitor-proof";
import { R9ReconnectProof } from "./lib/r9-reconnect-proof";
import { checkR10Sidecar } from "./build_v2_sidecar_r10";

if (
  process.env.GP_LIVE_GIT_FSMONITOR !== "1" ||
  process.env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1" ||
  process.env.PORT !== "18888"
) throw new Error("Explicit disposable-project guards and candidate port 18888 are required");

const sourceCookie = process.env.PRISM_COOKIE_FILE;
if (!sourceCookie) throw new Error("The authorized Prism cookie source is unavailable");
await checkR10Sidecar();

const model = process.env.GP_LIVE_MODEL ?? "gpt-6-astra";
if (!/^[A-Za-z0-9._-]{1,128}$/.test(model)) throw new Error("Invalid live model identifier");

type FsGateway = V2BootstrapGateway & {
  postJson(suffix: string, json: unknown, signal?: AbortSignal): Promise<Response>;
};

const root = path.resolve(".ghostprov/git-fsmonitor");
await mkdir(root, { recursive: true });
const directory = await mkdtemp(path.join(root, "r10-"));
const cookieFile = path.join(directory, "candidate-cookies.json");
const id = projectId(crypto.randomUUID());
const title = `GhostProv Git fsmonitor R10 verification ${path.basename(directory)}`;
const key = `third-method-${crypto.randomUUID()}`;
const logConfig = resolveObservabilityConfig({
  GP_LOG_LEVEL: "error",
  GP_LOG_DIR: path.join(directory, "host"),
  GP_LOG_ROTATE_BYTES: String(256 * 1024),
  GP_LOG_RETAIN_FILES: "1",
  GP_LOG_QUEUE_MAX_EVENTS: "256",
  GP_LOG_QUEUE_MAX_BYTES: String(1024 * 1024),
  GP_LOG_FLUSH_MS: "1000",
  GP_METRICS_INTERVAL_MS: "60000",
});
const logger = new ObservabilityLogger({
  config: logConfig,
  sinks: [new RotatingJsonlSink({
    directory: logConfig.directory,
    prefix: "third-method-host",
    rotateBytes: logConfig.rotateBytes,
    retainFiles: logConfig.retainFiles,
  })],
});

let session: PrismSessionManager | null = null;
let system: BrowserlessSystem | null = null;
let supervisor: V2SandboxSupervisor | null = null;
let gitReceipt: GitFsmonitorResult | null = null;
let gitSessionId: string | null = null;
let workspaceRemoved = false;
const receipt = (): GitFsmonitorResult | null => gitReceipt;
const reconnectProof = new R9ReconnectProof(1, { dropChallenge: true, dropReply: true });
let created = false;
let deleted = false;
let shutdownProved = false;
let remoteCleaned = false;
let responseCompleted = false;
let sidecarLaunches = 0;
let stable = false;
const blocked = { llm: 0, codexExec: 0, render: 0 };
let bridgeStarts = 0;
let hookFired = false;
let childPid: number | null = null;
let supervisorInstance: string | null = null;
let failure: unknown = null;
let latestNonce: string | null = null;
let responsesCompleted = 0;
let anthropicCompleted = 0;
let completedReconnects = 0;
let recoveryProved = false;
let recoveryMs = 0;
const responseIds = new Set<string>();
const SOAK_MS = 120_000;
function selectedSupervisor(): V2SandboxSupervisor | null { return supervisor; }
function selectedSystem(): BrowserlessSystem { if (!system) throw new Error("Missing system"); return system; }


function safeError(error: unknown) {
  const record = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const safe = (value: unknown) => typeof value === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(value) ? value : null;
  return {
    name: safe(record.name),
    kind: safe(record.kind),
    code: safe(record.code),
    status: typeof record.status === "number" ? record.status : null,
  };
}

function usableSnapshot(snapshot: ReturnType<NonNullable<ReturnType<BrowserlessSystem["registry"]["get"]>>["snapshot"]>): boolean {
  return snapshot.state === "ready" && snapshot.capacity > 0 && snapshot.available > 0 &&
    snapshot.transportConnected === true && snapshot.quarantined === 0 && snapshot.ambiguousQuarantined === 0;
}

async function sendResponse(previousResponseId?: string): Promise<string> {
  let currentCompleted = 0;
  let terminalId: string | null = null;
  const start = performance.now();
  const response = await fetch("http://127.0.0.1:18888/v1/responses", {
    method: "POST",
    signal: AbortSignal.timeout(180_000),
    headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream: true,
      store: true,
      max_output_tokens: 128,
      reasoning: { effort: "low" },
      input: "Reply with the single word Hiii.",
      ...(previousResponseId ? { previous_response_id: previousResponseId } : {}),
    }),
  });
  if (!response.ok || !response.body) throw new Error("The Git-hook sidecar did not accept a Responses request");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let events = 0;
  let buffer = "";
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
      bytes += next.value.byteLength;
      if (bytes > 2 * 1024 * 1024 || buffer.length + next.value.byteLength > 256 * 1024) {
        throw new Error("The test Responses stream exceeded its bound");
      }
      buffer += decoder.decode(next.value, { stream: true });
      const parsed = parseFrames(buffer, 0);
      buffer = buffer.slice(parsed.nextOffset);
      for (const frame of parsed.frames) {
        if (++events > 4096) throw new Error("The test Responses stream exceeded its event bound");
        if (frame.event === "response.completed") {
          currentCompleted++;
          const parsed = JSON.parse(frame.data) as { response?: { id?: unknown } };
          if (typeof parsed.response?.id !== "string") throw new Error("Missing provider response identity");
          terminalId = parsed.response.id;
        }
        if (frame.event === "response.failed" || frame.event === "response.incomplete" || frame.event === "error") {
          throw new Error("The Git-hook sidecar returned a terminal failure");
        }
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  if (currentCompleted !== 1 || !terminalId || responseIds.has(terminalId) || responseIds.size >= 16) throw new Error("Missing or duplicate completed provider response");
  responseIds.add(terminalId);
  responseCompleted = true;
  responsesCompleted++;
  console.log(JSON.stringify({ event: "git_fsmonitor.response_completed", events, bytes, elapsedMs: Math.round(performance.now() - start), responsesCompleted }));
  return terminalId;
}

async function sendAnthropic(): Promise<void> {
  const response = await fetch("http://127.0.0.1:18888/v1/messages", {
    method: "POST", signal: AbortSignal.timeout(180_000),
    headers: { "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json" },
    body: JSON.stringify({ model, stream: false, max_tokens: 128,
      messages: [{ role: "user", content: "Reply with the single word Hiii." }] }),
  });
  const text = await response.text();
  if (!response.ok || text.length > 256 * 1024) throw new Error("Anthropic proof failed");
  const value = JSON.parse(text) as { type?: unknown; stop_reason?: unknown; content?: unknown };
  if (value.type !== "message" || value.stop_reason !== "end_turn" || !Array.isArray(value.content)) throw new Error("Anthropic terminal proof failed");
  anthropicCompleted++;
  console.log(JSON.stringify({ event: "git_fsmonitor.anthropic_completed", anthropicCompleted }));
}

async function waitIdle(): Promise<void> {
  const deadline = performance.now() + 30_000;
  while (performance.now() < deadline) {
    const snapshot = selectedSystem().registry.get(id)?.snapshot();
    if (snapshot && usableSnapshot(snapshot) && snapshot.active === 0 && snapshot.queueDepth === 0) return;
    await Bun.sleep(250);
  }
  throw new Error("Owned Git proof did not return to idle");
}

async function recoverChild(): Promise<void> {
  await waitIdle();
  const selected = selectedSupervisor();
  if (!selected || !latestNonce || !childPid || sidecarLaunches !== 1) throw new Error("Missing original child identity");
  const originalNonce = latestNonce;
  const originalPid = childPid;
  const originalInstance = selected.instanceId;
  const started = performance.now();
  const stopped = await selected.stop({ launchNonces: [originalNonce], reason: "git_fsmonitor_controlled_recovery" }, AbortSignal.timeout(45_000));
  if (stopped !== originalNonce) throw new Error("Controlled stop lacked exact identity proof");
  const deadline = performance.now() + 180_000;
  let nextProgress = performance.now() + 15_000;
  while (performance.now() < deadline) {
    if (selectedSupervisor() !== selected || selected.instanceId !== originalInstance || sidecarLaunches > 2) throw new Error("Recovery changed supervisor identity");
    const snapshot = selectedSystem().registry.get(id)?.snapshot();
    if (Number(sidecarLaunches) === 2 && latestNonce !== originalNonce && childPid !== originalPid && snapshot && usableSnapshot(snapshot)) {
      recoveryProved = true;
      recoveryMs = Math.round(performance.now() - started);
      console.log(JSON.stringify({ event: "git_fsmonitor.recovered", recoveryMs, sameSupervisor: true, bridgeStarts, sidecarLaunches }));
      return;
    }
    if (performance.now() >= nextProgress) {
      console.log(JSON.stringify({ event: "git_fsmonitor.recovery_wait", sidecarLaunches, elapsedMs: Math.round(performance.now() - started) }));
      nextProgress += 15_000;
    }
    await Bun.sleep(250);
  }
  throw new Error("Controlled recovery did not restore ready capacity");
}

async function verifyStability(): Promise<void> {
  await waitIdle();
  const selected = selectedSupervisor();
  if (!selected || !supervisorInstance) throw new Error("Missing signed stability identity");
  const deadline = performance.now() + SOAK_MS;
  let nextProgress = performance.now() + 15_000;
  while (performance.now() < deadline) {
    await Bun.sleep(1_000);
    const snapshot = selectedSystem().registry.get(id)?.snapshot();
    if (!snapshot || !usableSnapshot(snapshot) || selected.instanceId !== supervisorInstance || sidecarLaunches !== 2 || bridgeStarts !== 1) throw new Error("The owned bootstrap did not retain ready capacity");
    if (performance.now() >= nextProgress) {
      console.log(JSON.stringify({ event: "git_fsmonitor.stability_sample", elapsedMs: Math.round(performance.now() - (deadline - SOAK_MS)) }));
      nextProgress += 15_000;
    }
  }
  stable = true;
}

async function cleanupRemote(): Promise<void> {
  if (remoteCleaned) return;
  if (!shutdownProved || !workspaceRemoved || receipt()?.configRestored !== true || receipt()?.lockReleased !== true) throw new Error("Integrated controller did not prove workspace cleanup");
  remoteCleaned = true;
}

try {
  await copyFile(sourceCookie, cookieFile);
  session = await PrismSessionManager.create({
    origin: process.env.PRISM_ORIGIN ?? "https://prism.openai.com",
    mode: "authenticated",
    allowAnonymous: false,
    cookieFile,
  });
  const identity = await session.resolve(AbortSignal.timeout(60_000));
  const accountRequest = identity.account.request.bind(identity.account);
  identity.account.request = async (url, init) => {
    const pathname = new URL(url, process.env.PRISM_ORIGIN ?? "https://prism.openai.com").pathname;
    if (pathname.startsWith("/api/llm/")) { blocked.llm++; throw new Error("LLM launch is blocked during Git proof"); }
    if (pathname.endsWith("/codex/codex_exec_async")) { blocked.codexExec++; throw new Error("Codex execution launch is blocked during Git proof"); }
    if (pathname.endsWith("/render")) { blocked.render++; throw new Error("Render launch is blocked during Git proof"); }
    return accountRequest(url, init);
  };
  const projects = new PrismProjectsClient(identity.account);
  await writeFile(path.join(directory, "owned-project.json"), JSON.stringify({ id, title }));
  created = true;
  await projects.create({ projectId: id, title }, AbortSignal.timeout(60_000));
  console.log(JSON.stringify({ event: "git_fsmonitor.project_created" }));
  system = await createBrowserlessSystem({
    ...process.env,
    PORT: "18888",
    GP_CANDIDATE: "1",
    GHOSTPROV_API_KEY: key,
    PRISM_COOKIE_FILE: cookieFile,
    PRISM_COOKIE: undefined,
    GP_PRISM_SESSION: "authenticated",
    GP_ALLOW_ANONYMOUS: "0",
    PRISM_PROJECT_IDS: id,
    PRISM_DOC_ID: "",
    GP_TARGET_SANDBOXES: "1",
    GP_MAX_PROJECTS: "1",
    GP_AUTO_CREATE_PROJECTS: "0",
    GP_PROJECT_REUSE: "managed",
    GP_GUEST_MAX_PROJECTS: "1",
    GP_FLEET_MANIFEST_FILE: path.join(directory, "fleet.json"),
    GP_PROVISION_CONCURRENCY: "1",
    GP_COLD_START_CONCURRENCY: "1",
    GP_V2_SIDECAR_REVISION: "r10",
    GP_V2_SIDECAR_PROJECT_REVISIONS: "",
    GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay",
    GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "git-fsmonitor",
    GP_V2_DIAGNOSTICS: "0",
  }, {
    sessionManager: session,
    projects,
    logger,
    manifest: new FleetManifestStore({ file: path.join(directory, "fleet.json"), identityFingerprint: identity.fingerprint }),
    sandboxSupervisorFactory: input => {
      if (input.binding.projectId !== id || supervisor) throw new Error("The Git proof permits only its original supervisor");
      const adaptedGateway = new Proxy(input.gateway, {
        get(target, property, receiver) {
          // Observe production calls without replacing the selected launcher.
          if (property === "postJson") return async (suffix: string, json: unknown, signal?: AbortSignal) => {
            if (suffix === "token") bridgeStarts++;
            const response = await (target as FsGateway).postJson(suffix, json, signal);
            if (suffix === "delete-file" && response.status === 200 && gitSessionId &&
                json && typeof json === "object" && "filename" in json && json.filename === `../codex_workspace/${gitSessionId}`) workspaceRemoved = true;
            return response;
          };
          if (property === "fetchText") return async (suffix: string, init?: RequestInit) => {
            const response = await target.fetchText(suffix, init);
            if (suffix.startsWith("codex/read_codex_workspace_file?") && suffix.includes("result.json") && response.status === 200) {
              const sessionId = new URL(suffix, "https://offline.invalid/").searchParams.get("session_id");
              const wrapper = JSON.parse(response.text) as { body_b64?: unknown };
              if (!sessionId || typeof wrapper.body_b64 !== "string" || wrapper.body_b64.length > 4096) throw new Error("Invalid observed hook receipt");
              gitReceipt = parseGitFsmonitorResult(JSON.parse(Buffer.from(wrapper.body_b64, "base64").toString("utf8")), sessionId);
              gitSessionId = sessionId;
              hookFired = gitReceipt.outcome === "submitted";
              console.log(JSON.stringify({ event: "git_fsmonitor.config_restored", configRestored: gitReceipt.configRestored,
                lockReleased: gitReceipt.lockReleased, hookFired }));
            }
            return response;
          };
          const value = Reflect.get(target, property, receiver);
          return typeof value === "function" ? value.bind(target) : value;
        },
      }) as V2BootstrapGateway;
      const value = new V2SandboxSupervisor({ ...input, gateway: adaptedGateway, hostFactory: options => reconnectProof.createHost(options, 1, 1) });
      supervisor = value;
      const launch = value.launch.bind(value);
      value.launch = async (request, signal) => {
        const result = await launch(request, signal);
        sidecarLaunches++;
        childPid = result.process.pid;
        latestNonce = request.sidecarBinding.nonce;
        return result;
      };
      return value;
    },
  }, AbortSignal.timeout(150_000));
  console.log(JSON.stringify({ event: "git_fsmonitor.starting" }));
  await system.start();
  const startup = system.registry.get(id)?.snapshot();
  const runningSupervisor = selectedSupervisor();
  if (!startup || !usableSnapshot(startup) || !runningSupervisor || sidecarLaunches !== 1 || childPid === null || bridgeStarts !== 1) {
    throw new Error("Git-hook R10 startup did not reach usable signed readiness");
  }
  supervisorInstance = runningSupervisor.instanceId;
  if (blocked.llm !== 0 || blocked.codexExec !== 0 || blocked.render !== 0) {
    throw new Error("A blocked legacy launcher was attempted during Git proof");
  }
  console.log(JSON.stringify({ event: "git_fsmonitor.signed_ready", bridgeStarts, sidecarLaunches }));
  const rootResponse = await sendResponse();
  const wave = await Promise.allSettled([sendResponse(), sendResponse(), sendAnthropic(), sendAnthropic()]);
  const waveFailure = wave.find(result => result.status === "rejected");
  if (waveFailure?.status === "rejected") throw waveFailure.reason;
  for (let round = 0; round < 2; round++) {
    await waitIdle();
    await reconnectProof.forceReconnect();
    if (runningSupervisor.instanceId !== supervisorInstance || Number(sidecarLaunches) !== 1 || bridgeStarts !== 1) throw new Error("Reconnect changed launch identity");
    completedReconnects++;
    console.log(JSON.stringify({ event: "git_fsmonitor.reconnected", completedReconnects, supervisors: reconnectProof.snapshot() }));
    await sendResponse();
  }
  await recoverChild();
  await sendResponse(rootResponse);
  await verifyStability();
  shutdownProved = await proveSignedShutdown(runningSupervisor, supervisorInstance, "git_fsmonitor_proof_complete", AbortSignal.timeout(45_000));
  await cleanupRemote();
  if (blocked.llm !== 0 || blocked.codexExec !== 0 || blocked.render !== 0) {
    throw new Error("A blocked legacy launcher was attempted after readiness");
  }
} catch (error) {
  failure = error;
  console.log(JSON.stringify({ event: "git_fsmonitor.failure", ...safeError(error) }));
} finally {
  const closingSupervisor = selectedSupervisor();
  if (!shutdownProved && closingSupervisor) {
    try {
      shutdownProved = await proveSignedShutdown(closingSupervisor, supervisorInstance, "git_fsmonitor_proof_cleanup", AbortSignal.timeout(45_000));
    } catch (error) { failure ??= error; }
  }
  if (shutdownProved) {
    try { await cleanupRemote(); } catch (error) { failure ??= error; }
  }
  await system?.close().catch(error => { failure ??= error; });
  await session?.close().catch(error => { failure ??= error; });
  let cleanup: PrismSessionManager | null = null;
  try {
    if (created) {
      cleanup = await PrismSessionManager.create({
        origin: process.env.PRISM_ORIGIN ?? "https://prism.openai.com",
        mode: "authenticated",
        allowAnonymous: false,
        cookieFile,
      });
      const identity = await cleanup.resolve(AbortSignal.timeout(60_000));
      const projects = new PrismProjectsClient(identity.account);
      const current = await projects.get(id, AbortSignal.timeout(30_000));
      if (current.title !== title) throw new Error("Disposable project ownership changed before cleanup");
      if (!current.deleted) await projects.deleteManaged(id, AbortSignal.timeout(60_000));
      const after = await projects.get(id, AbortSignal.timeout(30_000)).catch(error => {
        if (error && typeof error === "object" && "status" in error && (error.status === 404 || error.status === 410)) return null;
        throw error;
      });
      deleted = after === null || after.deleted;
    }
  } catch (error) { failure ??= error; }
  await cleanup?.close().catch(error => { failure ??= error; });
  for (const file of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`]) {
    if (!path.resolve(file).startsWith(directory + path.sep)) throw new Error("Test cleanup escaped its own directory");
    await rm(file, { force: true });
  }
  await logger.close();
  const report = {
    live: true,
    method: "git_fsmonitor_workspace_initialization",
    revision: "r10",
    buildHash: V2_R10_RELEASE.buildHash,
    bundleSha256: V2_R10_RELEASE.bundleSha256,
    candidatePort: 18888,
    projectCreated: created,
    projectDeleted: deleted,
    bridgeStarts,
    hookFired,
    sidecarLaunches,
    responseCompleted,
    responsesCompleted,
    anthropicCompleted,
    completedReconnects,
    reconnects: reconnectProof.snapshot(),
    recoveryProved,
    recoveryMs,
    configRestored: receipt()?.configRestored ?? false,
    configLockReleased: receipt()?.lockReleased ?? false,
    stableForMs: stable ? SOAK_MS : 0,
    signedShutdown: shutdownProved,
    remoteArtifactsRemoved: remoteCleaned,
    legacyLaunchAttempts: blocked,
    success: failure === null && created && deleted && bridgeStarts === 1 && hookFired && sidecarLaunches === 2 && responsesCompleted === 6 && anthropicCompleted === 2 && completedReconnects === 2 && recoveryProved && stable && shutdownProved && remoteCleaned && receipt()?.configRestored === true && receipt()?.lockReleased === true &&
      blocked.llm === 0 && blocked.codexExec === 0 && blocked.render === 0,
    failure: failure ? safeError(failure) : null,
  };
  await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ event: "git_fsmonitor.result", ...report, directory }));
  if (!report.success) process.exitCode = 1;
}
