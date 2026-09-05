import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import type { LoggerLike, LoggerSnapshot } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { FleetManifestStore } from "../src/fleet/manifest";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { RoutableSandboxSnapshot } from "../src/sandbox/registry";
import type { RunHandle } from "../src/relay/run-handle";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import { V2SandboxSupervisor, type SupervisorBootstrapMode } from "../src/v2/supervisor/controller";
import type { V2SupervisorLaunchResult } from "../src/v2/supervisor/host";
import { V2_R9_RELEASE, V2_R10_RELEASE } from "../src/v2/sidecar/assets";
import { SANDBOX_LOG_FILES, parseDiagnosticLine } from "../src/v2/diagnostics/sandbox-log-schema";
import { checkR10Sidecar } from "./build_v2_sidecar_r10";
import { V2BootstrapController } from "../src/v2/bootstrap/controller";
import { V2TransportSession } from "../src/v2/transport/session";
import { R9ReconnectProof } from "./lib/r9-reconnect-proof";
import { observeYSweetProofConnections } from "./lib/y-sweet-proof-observer";

const PORT = 18_888;
const MAX_BODY_BYTES = 256 * 1024;
const MAX_RESPONSES = 256;

/** Isolated-verifier instrumentation. Records codes only, never exception text. */
export function observeR9RecoveryFailures(onFailure?: () => Promise<void>) {
  const failures: Array<{ phase: string; causes: Array<Record<string, string | null>> }> = [];
  const restores: Array<() => void> = [];
  const label = (value: unknown) => typeof value === "string" && /^[A-Za-z][A-Za-z0-9_]{0,63}$/.test(value) ? value : null;
  const record = (phase: string, error: unknown): void => {
    if (failures.length >= 32) return;
    const causes: Array<Record<string, string | null>> = [];
    let current = error;
    while (current && typeof current === "object" && causes.length < 6) {
      const value = current as Record<string, unknown>;
      causes.push({ name: label(value.name), kind: label(value.kind), code: label(value.remoteCode ?? value.code) });
      current = value.cause;
    }
    failures.push({ phase, causes });
    console.log(JSON.stringify({ event: "r9.live.failure_detail", phase, causes }));
  };
  const observe = (target: object, key: string, phase: string): void => {
    const methods = target as Record<string, unknown>;
    const original = methods[key];
    if (typeof original !== "function") throw new Error("Missing recovery instrumentation method");
    const wrapped = function(this: object, ...args: unknown[]) {
      try {
        return Promise.resolve(Reflect.apply(original, this, args)).catch(async error => {
          record(phase, error);
          await onFailure?.().catch(() => undefined);
          throw error;
        });
      } catch (error) { record(phase, error); throw error; }
    };
    methods[key] = wrapped;
    restores.push(() => { if (methods[key] === wrapped) methods[key] = original; });
  };
  observe(V2BootstrapController.prototype, "ensureReady", "sidecar_bootstrap");
  observe(V2TransportSession.prototype, "connect", "transport_connect");
  observe(V2TransportSession.prototype, "adoptRecoveredSidecar", "recovery_barrier");
  observe(V2SandboxSupervisor.prototype, "launch", "supervisor_launch");
  observe(V2SandboxSupervisor.prototype, "deliverCredential", "supervisor_credential");
  return { failures, restore: () => { for (const restore of restores.reverse()) restore(); } };
}

export function r9RuntimeIsReady(snapshot: Pick<RoutableSandboxSnapshot,
  "state" | "capacity" | "available" | "transportConnected" | "quarantined" | "ambiguousQuarantined"
>): boolean {
  return snapshot.state === "ready" && snapshot.capacity > 0 && snapshot.available > 0 &&
    snapshot.transportConnected === true && snapshot.quarantined === 0 && (snapshot.ambiguousQuarantined ?? 0) === 0;
}

export interface R9RecoveryProofOptions {
  readonly target: 1 | 8;
  readonly port: 18_888;
  readonly timeoutMs: number;
  readonly model: string;
  readonly cookieFile: string;
  readonly origin: string;
  readonly bootstrapMode: SupervisorBootstrapMode;
  readonly benchmarkConcurrency: boolean;
  readonly recoveryCycles: number;
  readonly connectionDropRounds: number;
  readonly reconnectSoakMs: number;
  readonly coldStartConcurrency: 1 | 2;
  readonly sidecarRevision: "r9" | "r10";
  readonly dropChallenge: boolean;
  readonly dropReply: boolean;
}

export function resolveR9RecoveryProofOptions(env: Record<string, string | undefined>): R9RecoveryProofOptions {
  if (env.GP_LIVE_R9_SUPERVISOR_RECOVERY !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new Error("R9 live recovery requires both explicit live and dedicated-project mutation guards");
  }
  const target = Number(env.GP_LIVE_PROJECT_COUNT ?? 1);
  const sidecarRevision = env.GP_LIVE_SIDECAR_REVISION ?? "r9";
  if (sidecarRevision !== "r9" && sidecarRevision !== "r10") throw new Error("Live supervisor release must be R9 or R10");
  if (sidecarRevision === "r10" && env.GP_LIVE_R10_DIAGNOSTICS !== "1") throw new Error("R10 live proof requires its explicit diagnostics guard");
  if (target !== 1 && target !== 8) throw new Error("R9 proof requires exactly one or eight dedicated projects");
  const requestedColdStartConcurrency = Number(env.GP_LIVE_COLD_START_CONCURRENCY ?? 2);
  if (requestedColdStartConcurrency !== 1 && requestedColdStartConcurrency !== 2) {
    throw new Error("Live cold-start concurrency must be one or two");
  }
  const coldStartConcurrency = Math.min(target, requestedColdStartConcurrency) as 1 | 2;
  if (Number(env.PORT ?? PORT) !== PORT) throw new Error("R9 proof is restricted to candidate port 18888");
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 60_000 || timeoutMs > 60 * 60_000) {
    throw new Error("R9 proof deadline must be from one to sixty minutes");
  }
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("R9 proof requires PRISM_COOKIE_FILE");
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/.test(model)) throw new Error("Invalid live model");
  const bootstrapMode = env.GP_V2_SUPERVISOR_BOOTSTRAP_MODE ?? "direct";
  if (bootstrapMode !== "direct" && bootstrapMode !== "llm") throw new Error("Invalid supervisor bootstrap mode");
  const benchmark = env.GP_LIVE_BENCHMARK_CONCURRENCY ?? "0";
  if (benchmark !== "0" && benchmark !== "1") throw new Error("Concurrency benchmark guard must be 0 or 1");
  const recoveryCycles = Number(env.GP_LIVE_RECOVERY_CYCLES ?? 1);
  if (!Number.isSafeInteger(recoveryCycles) || recoveryCycles < 1 || recoveryCycles > 3) {
    throw new Error("Recovery cycles must be from one through three");
  }
  const connectionDropRounds = Number(env.GP_LIVE_CONNECTION_DROP_ROUNDS ?? 0);
  if (!Number.isSafeInteger(connectionDropRounds) || connectionDropRounds < 0 || connectionDropRounds > 2) {
    throw new Error("Connection drop rounds must be from zero through two");
  }
  const faultFlag = (name: string) => {
    const value = env[name] ?? "0";
    if (value !== "0" && value !== "1") throw new Error("Supervisor loss guard must be 0 or 1");
    return value === "1";
  };
  const dropChallenge = faultFlag("GP_LIVE_SUPERVISOR_DROP_CHALLENGE");
  const dropReply = faultFlag("GP_LIVE_SUPERVISOR_DROP_REPLY");
  if ((dropChallenge || dropReply) && connectionDropRounds < 2) throw new Error("Controlled supervisor loss requires two reconnect rounds");
  const reconnectSoakMs = Number(env.GP_LIVE_RECONNECT_SOAK_MS ?? 0);
  if (!Number.isSafeInteger(reconnectSoakMs) || reconnectSoakMs < 0 || reconnectSoakMs > 150 * 60_000 ||
    (reconnectSoakMs > 0 && reconnectSoakMs < 60_000)) {
    throw new Error("Reconnect soak must be disabled or from one through 150 minutes");
  }
  return Object.freeze({ target, port: PORT, timeoutMs, model, cookieFile: path.resolve(cookieFile),
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""), bootstrapMode,
    benchmarkConcurrency: benchmark === "1", recoveryCycles, connectionDropRounds, reconnectSoakMs, coldStartConcurrency, sidecarRevision, dropChallenge, dropReply });
}

function bounded<T>(operation: Promise<T>, timeoutMs: number, phase: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${phase} timed out`)), timeoutMs);
    operation.then(value => { clearTimeout(timer); resolve(value); }, error => { clearTimeout(timer); reject(error); });
  });
}

function assertContained(root: string, target: string): void {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new Error("Proof path escaped its directory");
}

class ProofLogger implements LoggerLike {
  readonly counts = new Map<string, number>();
  readonly transitions: Array<Record<string, unknown>> = [];
  invalidEvents = 0;
  emit(event: ObservabilityEvent): void {
    try { assertSafeObservabilityEvent(event); } catch { this.invalidEvents++; return; }
    if (this.counts.size < 128 || this.counts.has(event.event)) {
      this.counts.set(event.event, (this.counts.get(event.event) ?? 0) + 1);
    }
    // Never print request/response data, project identifiers, or error messages.
    if (event.event === "sandbox.recovery.retry_scheduled") {
      console.log(JSON.stringify({ event: "r9.live.recovery_retry", worker: event.worker_index,
        errorName: event.error_name, errorKind: event.error_kind }));
    }
    if (event.event === "sandbox.liveness") {
      const transition = { worker: event.worker_index, signal: event.liveness_signal, outcome: event.liveness_outcome,
        kind: event.error_kind, errorName: event.error_name, durationMs: event.duration_ms, sidecarAgeMs: event.sidecar_peer_age_ms,
        connected: event.transport_connected, reliableStall: event.reliable_stall, lane: event.awareness_lane,
        reconnectPhase: event.reconnect_phase, supervisorPhase: event.supervisor_phase };
      if (this.transitions.length === 128) this.transitions.shift();
      this.transitions.push(transition);
      console.log(JSON.stringify({ event: "r9.live.liveness", ...transition }));
    }
  }
  snapshot(): LoggerSnapshot { return { queuedEvents: 0, queuedBytes: 0, droppedSamples: 0,
    droppedProgress: 0, droppedTerminals: 0, sinkErrors: this.invalidEvents }; }
  async close(): Promise<void> {}
}

interface TrackedSupervisor {
  readonly supervisor: V2SandboxSupervisor;
  readonly generation: number;
  launches: number;
  discoveryTurns: number;
  launchTurns: number;
  directExecs: number;
  bootstrapMs: number | null;
  latest: (V2SupervisorLaunchResult & { nonce: string }) | null;
}

function outputText(value: Record<string, unknown>): string {
  const parts: string[] = [];
  if (Array.isArray(value.content)) {
    for (const part of value.content) if (part?.type === "text" && typeof part.text === "string") parts.push(part.text);
  }
  if (Array.isArray(value.output)) {
    for (const item of value.output) {
      if (!Array.isArray(item?.content)) continue;
      for (const part of item.content) if (part?.type === "output_text" && typeof part.text === "string") parts.push(part.text);
    }
  }
  return parts.join("\n");
}

async function responseJson(response: Response): Promise<Record<string, unknown>> {
  if (!response.body) throw new Error("Live HTTP response had no body");
  const reader = response.body.getReader();
  const bytes = new Uint8Array(MAX_BODY_BYTES);
  let length = 0;
  try {
    for (;;) {
      const item = await reader.read();
      if (item.done) break;
      if (length + item.value.byteLength > bytes.byteLength) throw new Error("Live HTTP response exceeded its byte bound");
      bytes.set(item.value, length);
      length += item.value.byteLength;
    }
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length)));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("Invalid live JSON response");
    if (!response.ok) throw new Error(`Live HTTP request failed with status ${response.status}`);
    return parsed as Record<string, unknown>;
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

async function consumeHandle(handle: RunHandle): Promise<Record<string, unknown>> {
  let final: Record<string, unknown> | null = null;
  try {
    const results = await Promise.all([
      handle.final.then(value => { final = value as Record<string, unknown>; return final; }),
      (async () => { for await (const _event of handle.events()) { /* Drain permits without retaining output. */ } })(),
    ]);
    return results[0];
  } finally {
    handle.discardUnconsumedEvents?.();
    releaseAtomicBytePermit(final);
  }
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = resolveR9RecoveryProofOptions(env);
  if (options.sidecarRevision === "r10") await checkR10Sidecar();
  const release = options.sidecarRevision === "r10" ? V2_R10_RELEASE : V2_R9_RELEASE;
  const proofRoot = path.resolve(".ghostprov", `live-${options.sidecarRevision}`);
  await mkdir(proofRoot, { recursive: true });
  const directory = await mkdtemp(path.join(proofRoot, "recovery-"));
  assertContained(proofRoot, directory);
  const cookieFile = path.join(directory, "candidate-cookies.json");
  await copyFile(options.cookieFile, cookieFile);
  const logger = new ProofLogger();
  const intended = Array.from({ length: options.target }, () => projectId(crypto.randomUUID()));
  const connectionObserver = observeYSweetProofConnections(intended);
  let connectionReport: ReturnType<typeof connectionObserver.snapshot> | null = null;
  const cleanupEligible = new Set<ProjectId>();
  const deleted = new Set<ProjectId>();
  const titles = new Map(intended.map((id, index) => [id, `GhostProv R9 Recovery ${path.basename(directory)} ${index + 1}`]));
  const tracked = new Map<ProjectId, TrackedSupervisor>();
  const retiredGenerations: Array<Record<string, number | null>> = [];
  let failureLogCapture: Promise<void> | null = null;
  const recoveryFailures = observeR9RecoveryFailures(async () => {
    if (options.sidecarRevision !== "r10") return;
    // Capture the bounded files before the failing runtime retires its source.
    failureLogCapture ??= collectSandboxLogs(false);
    await failureLogCapture;
  });
  const reconnectProof = options.connectionDropRounds || options.reconnectSoakMs ? new R9ReconnectProof(options.target, options) : null;
  let reconnectReport: ReturnType<R9ReconnectProof["snapshot"]> = [];
  let longSoakElapsedMs = 0;
  let longSoakMaxUnavailableMs = 0;
  let longSoakProbeCount = 0;
  const responseIds = new Set<string>();
  const waves: Array<{ phase: string; count: number; durationMs: number }> = [];
  const requestTimings: Array<{ lane: string; durationMs: number }> = [];
  const concurrencyBenchmarks: Array<Record<string, number>> = [];
  const recoveryTimings: Array<{ cycle: number; durationMs: number }> = [];
  const sandboxLogs: Array<{ worker: number; file: string; records: number; bytes: number; dropped: number; fatals: number }> = [];
  let directExecRequests = 0;
  let llmBootstrapEndpointRequests = 0;
  let llmBootstrapStarts = 0;
  const apiKey = `r9-proof-${crypto.randomUUID()}`;
  let session: PrismSessionManager | null = null;
  let system: BrowserlessSystem | null = null;
  let projects: PrismProjectsClient | null = null;
  let phase = "create";
  let failure: unknown = null;
  let recovered = 0;
  let stabilitySamples = 0;
  let startupMs: number | null = null;
  let recoveryMs: number | null = null;
  let failureSnapshots: Array<Record<string, unknown>> = [];
  const collectSandboxLogs = async (strict: boolean): Promise<void> => {
    if (options.sidecarRevision !== "r10" || !system) return;
    const root = path.join(directory, "sandbox-logs");
    await mkdir(root, { recursive: true });
    const deadline = performance.now() + 30000;
    for (let index = 0; index < intended.length; index++) {
      let usable = false;
      for (const file of SANDBOX_LOG_FILES) {
        if (performance.now() >= deadline) { if (strict) throw new Error("Sandbox log collection deadline expired"); return; }
        try {
          const response = await fetch(`http://127.0.0.1:${PORT}/v1/diagnostics/sandboxes/${intended[index]}/logs/${file}`, {
            headers: { authorization: `Bearer ${apiKey}` }, signal: AbortSignal.timeout(Math.max(1, Math.min(5000, Math.floor(deadline - performance.now())))),
          });
          if (response.status === 404) { await response.body?.cancel(); continue; }
          if (response.status !== 200) { await response.body?.cancel(); throw new Error("Sandbox log endpoint failed"); }
          const content = await response.text();
          if (Buffer.byteLength(content) > 128 * 1024) throw new Error("Sandbox log download exceeds its bound");
          const records = content.split("\n").filter(Boolean).map(parseDiagnosticLine);
          if (records.length > 1024) throw new Error("Sandbox log record count exceeds its bound");
          const dropped = Math.max(0, ...records.filter(row => row.event === "logger_stats").map(row => Number(row.dropped)));
          const fatals = records.filter(row => row.event === "fatal").length;
          if (strict && (dropped > 0 || fatals > 0)) throw new Error("Sandbox logs contain drops or an unexpected fatal");
          if (file === "sidecar.jsonl" && records.some(row => row.event === "upstream_terminal")) usable = true;
          await writeFile(path.join(root, `worker-${index + 1}-${file}`), content);
          if (sandboxLogs.length < 48) sandboxLogs.push({ worker: index + 1, file, records: records.length, bytes: Buffer.byteLength(content), dropped, fatals });
        } catch (error) { if (strict) throw error; }
      }
      if (strict && !usable) throw new Error("Current child terminal logs were not retrievable");
    }
  };
  const readyRuntimes = () => system?.registry.readyRuntimes().filter(runtime => r9RuntimeIsReady(runtime.snapshot())) ?? [];
  const runtimeSnapshots = () => intended.map((id, index) => {
    const snapshot = system?.registry.get(id)?.snapshot();
    const host = snapshot?.diagnostics?.host;
    return { worker: index + 1, state: snapshot?.state, available: snapshot?.available, capacity: snapshot?.capacity,
      active: snapshot?.active, reason: snapshot?.availabilityReason, quarantine: snapshot?.quarantined,
      heartbeatAgeMs: snapshot?.heartbeatAgeMs, sidecarAgeMs: snapshot?.sidecarPeerAgeMs,
      connected: snapshot?.transportConnected, launches: tracked.get(id)?.launches,
      windowFrames: host?.windowFrames, schedulerFrames: host?.schedulerFrames,
      ackAgeMs: host?.ackProgressAgeMs, retransmissions: host?.retransmissionsTotal };
  });
  const reportPhase = (next: string) => { phase = next; console.log(JSON.stringify({ event: "r9.live.phase", phase, target: options.target })); };
  const accept = (value: Record<string, unknown>, expected: string): string => {
    if (typeof value.id !== "string" || value.id.length > 256 || !value.id || responseIds.has(value.id)) {
      throw new Error("Live request lacked a unique provider terminal identity");
    }
    if (value.status !== undefined && value.status !== "completed") throw new Error("Live response did not complete");
    if (!outputText(value).includes(expected)) throw new Error("Live response did not contain its expected marker");
    if (responseIds.size >= MAX_RESPONSES) throw new Error("Live response ledger exceeded its bound");
    responseIds.add(value.id);
    return value.id;
  };
  const post = async (route: string, body: Record<string, unknown>, marker: string): Promise<string> => {
    const started = performance.now();
    const response = await fetch(`http://127.0.0.1:${PORT}${route}`, {
      method: "POST", headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json", "anthropic-version": "2023-06-01" },
      body: JSON.stringify(body), signal: AbortSignal.timeout(180_000),
    });
    const id = accept(await responseJson(response), marker);
    requestTimings.push({ lane: route === "/v1/messages" ? "anthropic" : "responses", durationMs: performance.now() - started });
    return id;
  };
  const nativeBody = (input: string) => ({ model: options.model, stream: false, store: true,
    reasoning: { effort: "low" }, max_output_tokens: 256, input });
  const publicWave = async (label: string): Promise<void> => {
    const count = Math.max(2, options.target);
    const started = performance.now();
    const results = await Promise.allSettled(Array.from({ length: count }, async (_, index) => {
      const marker = `R9_${label}_${index}`;
      if (index % 2 === 0) return post("/v1/responses", nativeBody(`Reply with exactly ${marker}`), marker);
      return post("/v1/messages", { model: options.model, stream: false, max_tokens: 256,
        messages: [{ role: "user", content: `Reply with exactly ${marker}` }] }, marker);
    }));
    const rejected = results.find(result => result.status === "rejected");
    if (rejected?.status === "rejected") throw rejected.reason;
    waves.push({ phase: label, count, durationMs: Math.round(performance.now() - started) });
    console.log(JSON.stringify({ event: "r9.live.wave", ...waves[waves.length - 1] }));
  };
  const everySandbox = async (label: string): Promise<void> => {
    const runtimes = readyRuntimes();
    if (runtimes.length !== options.target) throw new Error("Every-sandbox proof lacked all runtimes");
    const started = performance.now();
    const results = await Promise.allSettled(runtimes.map(async (runtime, index) => {
      const marker = `R9_${label}_${index}`;
      const requestStarted = performance.now();
      const handle = await runtime.run({ ...nativeBody(`Reply with exactly ${marker}`), stream: true }, AbortSignal.timeout(180_000));
      accept(await consumeHandle(handle), marker);
      requestTimings.push({ lane: "runtime", durationMs: performance.now() - requestStarted });
    }));
    const rejected = results.find(result => result.status === "rejected");
    if (rejected?.status === "rejected") throw rejected.reason;
    waves.push({ phase: label, count: runtimes.length, durationMs: Math.round(performance.now() - started) });
    console.log(JSON.stringify({ event: "r9.live.wave", ...waves[waves.length - 1] }));
  };
  const waitFor = async (predicate: () => boolean, timeoutMs: number): Promise<void> => {
    const deadline = Date.now() + timeoutMs;
    let nextSample = Date.now() + 30_000;
    while (!predicate()) {
      if (Date.now() >= deadline) throw new Error(`${phase} did not converge before its deadline`);
      if (Date.now() >= nextSample) {
        console.log(JSON.stringify({ event: "r9.live.wait", phase, ready: readyRuntimes().length, runtimes: runtimeSnapshots() }));
        nextSample += 30_000;
      }
      await Bun.sleep(250);
    }
  };
  try {
    session = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await session.resolve(AbortSignal.timeout(60_000));
    const originalRequest = identity.account.request.bind(identity.account);
    identity.account.request = async (url, init) => {
      const pathname = new URL(url, options.origin).pathname;
      if (pathname.startsWith("/api/llm/")) {
        llmBootstrapEndpointRequests++;
        if (options.bootstrapMode === "direct") throw new Error("Direct proof forbids LLM bootstrap endpoints");
        if (pathname === "/api/llm/response_with_tools_start") llmBootstrapStarts++;
      }
      if (pathname.endsWith("/codex/codex_exec_async")) {
        const body: unknown = typeof init?.body === "string" ? JSON.parse(init.body) : null;
        if (!body || typeof body !== "object" || Array.isArray(body) ||
          (body as Record<string, unknown>).codex_path !== "/bin/sh" ||
          (body as Record<string, unknown>).prompt !== "Hiii") throw new Error("Unexpected direct execution request");
        directExecRequests++;
      }
      return originalRequest(url, init);
    };
    projects = new PrismProjectsClient(identity.account);
    const preexisting = new Set((await projects.list(AbortSignal.timeout(60_000))).map(item => item.projectId));
    for (const id of intended) {
      if (preexisting.has(id)) throw new Error("Dedicated project collided with a pre-existing project");
      cleanupEligible.add(id);
      await projects.create({ projectId: id, title: titles.get(id)! }, AbortSignal.timeout(60_000));
    }
    reportPhase("startup");
    const startupStarted = performance.now();
    system = await createBrowserlessSystem({
      ...env, PORT: String(PORT), GP_CANDIDATE: "1", GHOSTPROV_API_KEY: apiKey,
      PRISM_COOKIE_FILE: cookieFile, PRISM_COOKIE: undefined, GP_PRISM_SESSION: "authenticated", GP_ALLOW_ANONYMOUS: "0",
      PRISM_PROJECT_IDS: intended.join(","), PRISM_DOC_ID: "", GP_TARGET_SANDBOXES: String(options.target),
      GP_MAX_PROJECTS: String(options.target), GP_AUTO_CREATE_PROJECTS: "0", GP_PROJECT_REUSE: "managed",
      GP_GUEST_MAX_PROJECTS: "1", GP_FLEET_MANIFEST_FILE: path.join(directory, "fleet.json"),
      GP_PROVISION_CONCURRENCY: "4", GP_COLD_START_CONCURRENCY: String(options.coldStartConcurrency),
      GP_V2_SIDECAR_REVISION: options.sidecarRevision, GP_V2_SIDECAR_PROJECT_REVISIONS: "", GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay",
      GP_V2_SUPERVISOR_BOOTSTRAP_MODE: options.bootstrapMode,
    }, {
      sessionManager: session, projects, logger,
      manifest: new FleetManifestStore({ file: path.join(directory, "fleet.json"), identityFingerprint: identity.fingerprint }),
      sandboxSupervisorFactory: input => {
        const id = projectId(input.binding.projectId);
        if (!intended.includes(id) || (!tracked.has(id) && tracked.size >= options.target)) throw new Error("Unexpected supervisor project");
        const generation = (tracked.get(id)?.generation ?? 0) + 1;
        if (generation > 2) throw new Error("Live supervisor generation bound exceeded");
        const previous = tracked.get(id);
        if (previous && retiredGenerations.length < options.target * 2) {
          retiredGenerations.push({ worker: intended.indexOf(id) + 1, generation: previous.generation,
            launches: previous.launches, directExecs: previous.directExecs, bootstrapMs: previous.bootstrapMs });
        }
        const supervisor = new V2SandboxSupervisor({ ...input,
          ...(reconnectProof ? { hostFactory: options => reconnectProof.createHost(options, intended.indexOf(id) + 1, generation) } : {}),
        });
        const state: TrackedSupervisor = { supervisor, generation, launches: 0, discoveryTurns: 0, launchTurns: 0,
          directExecs: 0, bootstrapMs: null, latest: null };
        tracked.set(id, state);
        const prepare = input.gateway.prepareRelayExecSession?.bind(input.gateway);
        const exec = input.gateway.runRelayExec?.bind(input.gateway);
        if (!prepare || !exec) throw new Error("Live gateway lacks supervisor bootstrap");
        input.gateway.prepareRelayExecSession = signal => { state.discoveryTurns++; return prepare(signal); };
        input.gateway.runRelayExec = (request, continuation, signal) => { state.launchTurns++; return exec(request, continuation, signal); };
        const direct = input.gateway.startSupervisorExec?.bind(input.gateway);
        if (!direct) throw new Error("Live gateway lacks direct supervisor bootstrap");
        input.gateway.startSupervisorExec = (request, signal) => { state.directExecs++; return direct(request, signal); };
        const ensureReady = supervisor.ensureReady.bind(supervisor);
        supervisor.ensureReady = async signal => {
          const startedAt = performance.now();
          await ensureReady(signal);
          state.bootstrapMs ??= Math.round(performance.now() - startedAt);
        };
        const launch = supervisor.launch.bind(supervisor);
        supervisor.launch = async (request, signal) => {
          const result = await launch(request, signal);
          state.launches++;
          state.latest = { ...result, nonce: request.sidecarBinding.nonce };
          return result;
        };
        return supervisor;
      },
    });
    await bounded(system.start(), options.timeoutMs, "Candidate startup");
    await waitFor(() => readyRuntimes().length === options.target, options.timeoutMs);
    startupMs = Math.round(performance.now() - startupStarted);
    if (system.fleet.snapshot().projects.length !== options.target || tracked.size !== options.target) throw new Error("Wrong live fleet size");
    reportPhase("requests_before_recovery");
    await everySandbox("before_each");
    await publicWave("before_http");
    for (let round = 1; round <= options.connectionDropRounds; round++) {
      reportPhase(`supervisor_reconnect_${round}`);
      const before = new Map([...tracked].map(([id, state]) => [id, {
        state, instanceId: state.supervisor.instanceId, launches: state.launches,
      }]));
      await reconnectProof!.forceReconnect();
      for (const [id, original] of before) {
        const current = tracked.get(id);
        if (current !== original.state || current.generation !== 1 || current.launches !== original.launches ||
          current.supervisor.instanceId !== original.instanceId) throw new Error("Reconnect replaced the supervisor or child");
      }
      await publicWave(`reconnect_${round}`);
      console.log(JSON.stringify({ event: "r9.live.reconnect", round, supervisors: reconnectProof!.snapshot() }));
    }
    for (let cycle = 1; cycle <= options.recoveryCycles; cycle++) {
      const root = await post("/v1/responses", nativeBody("Remember codeword R9_AMBER_409 and reply with exactly ROOT_OK."), "ROOT_OK");
      reportPhase(`stop_sidecars_${cycle}`);
      const recoveryStarted = performance.now();
      if (system.registry.readyRuntimes().some(runtime => {
        const state = runtime.snapshot();
        return state.active !== 0 || state.quarantined !== 0 || state.queueDepth !== 0;
      })) throw new Error("Controlled recovery requires idle, unambiguous runtimes");
      const before = new Map([...tracked].map(([id, state]) => [id, { state, latest: state.latest, instance: state.supervisor.instanceId }]));
      const stops = await Promise.allSettled([...before.values()].map(async value => {
        if (!value.latest || value.state.launches !== cycle || value.state.generation !== 1) throw new Error("Startup did not have one exact sidecar launch");
        const stopped = await value.state.supervisor.stop({ launchNonces: [value.latest.nonce], reason: "dedicated_live_recovery" }, AbortSignal.timeout(45_000));
        if (stopped !== value.latest.nonce) throw new Error("Controlled stop returned the wrong launch proof");
      }));
      const stopFailure = stops.find(result => result.status === "rejected");
      if (stopFailure?.status === "rejected") throw stopFailure.reason;
      reportPhase(`recover_same_supervisors_${cycle}`);
      await waitFor(() => [...before].every(([id, previous]) => {
        const current = tracked.get(id);
        if (current !== previous.state || current?.generation !== 1 || current.launches > cycle + 1) throw new Error("Recovery replaced or relaunched the supervisor");
        return current.launches === cycle + 1 && current.latest?.nonce !== previous.latest?.nonce;
      }) && readyRuntimes().length === options.target, 180_000);
      for (const [id, previous] of before) {
        const state = tracked.get(id)!;
        const llm = options.bootstrapMode === "llm" ? 1 : 0;
        const direct = options.bootstrapMode === "direct" ? 1 : 0;
        if (state.supervisor.instanceId !== previous.instance || state.discoveryTurns !== llm || state.launchTurns !== llm || state.directExecs !== direct ||
          state.latest?.process.pid === previous.latest?.process.pid) throw new Error("Recovery did not retain its original supervisor");
        recovered++;
      }
      const cycleMs = Math.round(performance.now() - recoveryStarted);
      recoveryTimings.push({ cycle, durationMs: cycleMs });
      recoveryMs = (recoveryMs ?? 0) + cycleMs;
      reportPhase(`requests_after_recovery_${cycle}`);
      await post("/v1/responses", { ...nativeBody("Reply with only the codeword I asked you to remember."), previous_response_id: root }, "R9_AMBER_409");
      await everySandbox(`after_each_${cycle}`);
      await publicWave(`after_http_${cycle}`);
    }
    reportPhase("concurrency_soak");
    const soakDeadline = Date.now() + (options.target === 8 ? 90_000 : 30_000);
    for (let wave = 0; wave < (options.target === 8 ? 3 : 1); wave++) await publicWave(`soak_${wave}`);
    while (Date.now() < soakDeadline) {
      if (readyRuntimes().length !== options.target) throw new Error("Soak lost full readiness");
      stabilitySamples++;
      await Bun.sleep(1_000);
    }
    if (options.benchmarkConcurrency) {
      for (const concurrency of options.target === 8 ? [1, 8, 16, 32] : [1, 4, 8, 16]) {
        reportPhase(`concurrency_${concurrency}`);
        const beforeTimings = requestTimings.length;
        const startedAt = performance.now();
        const outcomes = await Promise.allSettled(Array.from({ length: concurrency }, (_, index) => {
          const marker = `R9_LOAD_${concurrency}_${index}`;
          return index % 2 === 0
            ? post("/v1/responses", nativeBody(`Reply with exactly ${marker}`), marker)
            : post("/v1/messages", { model: options.model, stream: false, max_tokens: 256,
                messages: [{ role: "user", content: `Reply with exactly ${marker}` }] }, marker);
        }));
        const rejected = outcomes.find(value => value.status === "rejected");
        if (rejected?.status === "rejected") throw rejected.reason;
        const elapsedMs = performance.now() - startedAt;
        const timings = requestTimings.slice(beforeTimings).map(value => value.durationMs).sort((a, b) => a - b);
        if (timings.length !== concurrency || readyRuntimes().length !== options.target) throw new Error("Concurrency proof lost capacity or responses");
        const result = { concurrency, completed: timings.length, durationMs: Math.round(elapsedMs),
          p50Ms: Math.round(timings[Math.ceil(timings.length * 0.5) - 1]!),
          p95Ms: Math.round(timings[Math.ceil(timings.length * 0.95) - 1]!),
          maxMs: Math.round(timings[timings.length - 1]!),
          requestsPerSecond: Math.round(concurrency / elapsedMs * 1_000_000) / 1000 };
        concurrencyBenchmarks.push(result);
        console.log(JSON.stringify({ event: "r9.live.concurrency", ...result }));
      }
    }
    if (options.reconnectSoakMs > 0) {
      reportPhase("long_reconnect_soak");
      const originals = new Map([...tracked].map(([id, state]) => [id, {
        state, instanceId: state.supervisor.instanceId, launches: state.launches,
      }]));
      let continuation = await post("/v1/responses", nativeBody("Reply with exactly RECONNECT_ROOT_OK."), "RECONNECT_ROOT_OK");
      const startedAt = performance.now();
      const deadline = startedAt + options.reconnectSoakMs;
      let unavailableSince: number | null = null;
      let nextProgressAt = startedAt;
      let nextProbeAt = startedAt;
      let pendingProbe: Promise<void> | null = null;
      let probeFailure: unknown = null;
      reconnectProof!.observeNatural(true);
      while (performance.now() < deadline) {
        if (probeFailure) throw probeFailure;
        for (const [id, original] of originals) {
          const current = tracked.get(id);
          if (current !== original.state || current.generation !== 1 || current.launches !== original.launches ||
            current.supervisor.instanceId !== original.instanceId) throw new Error("Long reconnect soak replaced an original supervisor or child");
        }
        const now = performance.now();
        longSoakElapsedMs = Math.round(now - startedAt);
        if (readyRuntimes().length !== options.target) {
          unavailableSince ??= now;
          longSoakMaxUnavailableMs = Math.max(longSoakMaxUnavailableMs, Math.round(now - unavailableSince));
          if (now - unavailableSince > 30_000) throw new Error("Live reconnect failed to restore usable capacity within 30 seconds");
        } else unavailableSince = null;
        if (now >= nextProbeAt && pendingProbe === null && readyRuntimes().length === options.target) {
          if (longSoakProbeCount >= 31) throw new Error("Long-soak request count bound exceeded");
          nextProbeAt = now + 5 * 60_000;
          const round = ++longSoakProbeCount;
          pendingProbe = (async () => {
            const marker = `RECONNECT_SOAK_${round}`;
            continuation = await post("/v1/responses", { ...nativeBody(`Reply with exactly ${marker}.`), previous_response_id: continuation }, marker);
            await post("/v1/messages", { model: options.model, stream: false, max_tokens: 256,
              messages: [{ role: "user", content: `Reply with exactly ${marker}.` }] }, marker);
            console.log(JSON.stringify({ event: "r9.live.long_soak_probe", round, completed: responseIds.size }));
          })().catch(error => { probeFailure = error; }).finally(() => { pendingProbe = null; });
        }
        if (now >= nextProgressAt) {
          console.log(JSON.stringify({ event: "r9.live.long_soak_progress", elapsedMs: longSoakElapsedMs,
            remainingMs: Math.max(0, Math.round(deadline - now)), ready: readyRuntimes().length,
            maxUnavailableMs: longSoakMaxUnavailableMs, completed: responseIds.size, supervisors: reconnectProof!.snapshot() }));
          nextProgressAt = now + 60_000;
        }
        await Bun.sleep(1_000);
      }
      await pendingProbe;
      if (probeFailure) throw probeFailure;
      await waitFor(() => readyRuntimes().length === options.target, 30_000);
      for (const [id, original] of originals) {
        const current = tracked.get(id);
        if (current !== original.state || current.generation !== 1 || current.launches !== original.launches ||
          current.supervisor.instanceId !== original.instanceId) throw new Error("Final reconnect-soak identity changed");
      }
      longSoakElapsedMs = Math.round(performance.now() - startedAt);
      reconnectProof!.observeNatural(false);
    }
    if (options.bootstrapMode === "direct"
      ? directExecRequests !== options.target || llmBootstrapEndpointRequests !== 0
      : directExecRequests !== 0 || llmBootstrapStarts !== options.target * 2) throw new Error("Unexpected bootstrap execution count");
    if (logger.invalidEvents !== 0) throw new Error("Live observability validation failed");
    await collectSandboxLogs(true);
    const proof = reconnectProof?.snapshot() ?? [];
    if (options.dropChallenge && proof.reduce((sum, item) => sum + item.injectedChallengeDrops, 0) !== 1) throw new Error("Challenge loss was not exercised exactly once");
    if (options.dropReply && proof.reduce((sum, item) => sum + item.injectedReplyDrops, 0) !== 1) throw new Error("Reply loss was not exercised exactly once");
    reportPhase("complete");
  } catch (error) {
    failure = error;
    failureSnapshots = runtimeSnapshots();
  } finally {
    connectionReport = connectionObserver.snapshot();
    reconnectProof?.observeNatural(false);
    reconnectReport = reconnectProof?.snapshot() ?? [];
    if (failure) await collectSandboxLogs(false).catch(() => undefined);
    await system?.close().catch(error => { failure ??= error; });
    // system.close() owns the session, so cleanup opens another isolated session.
    await session?.close().catch(error => { failure ??= error; });
    let cleanupSession: PrismSessionManager | null = null;
    try {
      if (cleanupEligible.size > 0) {
        cleanupSession = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
        const account = await cleanupSession.resolve(AbortSignal.timeout(60_000));
        const cleanupProjects = new PrismProjectsClient(account.account);
        for (const id of cleanupEligible) {
          let success = false;
          for (let attempt = 0; attempt < 4 && !success; attempt++) {
            try {
              const summary = await cleanupProjects.get(id, AbortSignal.timeout(30_000));
              if (!summary.deleted && summary.title !== titles.get(id)) throw new Error("Dedicated cleanup title changed");
              if (!summary.deleted) await cleanupProjects.deleteManaged(id, AbortSignal.timeout(60_000));
              const check = await cleanupProjects.get(id, AbortSignal.timeout(30_000));
              if (!check.deleted) throw new Error("Deleted project remained active");
              success = true;
            } catch (error) {
              const status = error && typeof error === "object" && "status" in error ? error.status : null;
              if (status === 404 || status === 410) success = true;
              else if (attempt === 3) failure ??= error;
              else await Bun.sleep(1_000);
            }
          }
          if (success) deleted.add(id);
        }
      }
    } catch (error) { failure ??= error; }
    await cleanupSession?.close().catch(error => { failure ??= error; });
    for (const file of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`]) {
      assertContained(directory, file);
      await rm(file, { force: true, recursive: file.endsWith(".lock") }).catch(error => { failure ??= error; });
    }
    recoveryFailures.restore();
    connectionObserver.restore();
  }
  if (deleted.size !== cleanupEligible.size) failure ??= new Error("Dedicated projects were not fully deleted");
  const safeName = (value: unknown) => typeof value === "string" && /^[A-Za-z][A-Za-z0-9_:.-]{0,63}$/.test(value) ? value : null;
  const causes: Array<{ name: string | null; kind: string | null; code: string | null }> = [];
  let cause: unknown = failure;
  while (cause && typeof cause === "object" && causes.length < 6) {
    const value = cause as Record<string, unknown>;
    causes.push({ name: safeName(value.name), kind: safeName(value.kind), code: safeName(value.remoteCode ?? value.code) });
    cause = value.cause;
  }
  const report = { live: true, success: failure === null, phase, candidatePort: PORT,
    bootstrapMode: options.bootstrapMode, directExecRequests, llmBootstrapEndpointRequests, llmBootstrapStarts,
    terminalReceiptCoalescing: system?.config.v2TerminalReceiptCoalescing ?? null,
    recoveryFailures: recoveryFailures.failures, retiredGenerations,
    supervisorBootstrapMs: [...tracked.values()].map(value => value.bootstrapMs), concurrencyBenchmarks,
    recoveryCycles: options.recoveryCycles, recoveryTimings,
    connectionDropRounds: options.connectionDropRounds, reconnectSoakMs: options.reconnectSoakMs,
    dropChallenge: options.dropChallenge, dropReply: options.dropReply,
    coldStartConcurrency: options.coldStartConcurrency,
    longSoakElapsedMs, longSoakMaxUnavailableMs, longSoakProbeCount, supervisorReconnects: reconnectReport,
    ySweetConnections: connectionReport,
    target: options.target, sidecarRevision: options.sidecarRevision, buildHash: release.buildHash, bundleSha256: release.bundleSha256, sandboxLogs,
    completedResponses: responseIds.size, recoveredSameSupervisor: recovered, startupMs, recoveryMs, stabilitySamples, waves,
    latencyMs: Object.fromEntries(["responses", "anthropic", "runtime"].map(lane => {
      const values = requestTimings.filter(value => value.lane === lane).map(value => value.durationMs).sort((a, b) => a - b);
      const percentile = (fraction: number) => values.length ? Math.round(values[Math.ceil(values.length * fraction) - 1]!) : null;
      return [lane, { count: values.length, p50: percentile(0.5), p95: percentile(0.95), max: percentile(1) }];
    })),
    projectsCreatedOrAttempted: cleanupEligible.size, projectsDeleted: deleted.size, causes,
    failureSnapshots, livenessTransitions: logger.transitions };
  const reportFile = path.join(directory, "report.json");
  await writeFile(reportFile, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ...report, reportFile }));
  if (failure) process.exitCode = 1;
}

if (import.meta.main) await main();
