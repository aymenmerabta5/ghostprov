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
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import type { V2SupervisorLaunchResult } from "../src/v2/supervisor/host";
import { V2_R9_RELEASE } from "../src/v2/sidecar/assets";

const PORT = 18_888;
const MAX_BODY_BYTES = 256 * 1024;
const MAX_RESPONSES = 128;

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
}

export function resolveR9RecoveryProofOptions(env: Record<string, string | undefined>): R9RecoveryProofOptions {
  if (env.GP_LIVE_R9_SUPERVISOR_RECOVERY !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new Error("R9 live recovery requires both explicit live and dedicated-project mutation guards");
  }
  const target = Number(env.GP_LIVE_PROJECT_COUNT ?? 1);
  if (target !== 1 && target !== 8) throw new Error("R9 proof requires exactly one or eight dedicated projects");
  if (Number(env.PORT ?? PORT) !== PORT) throw new Error("R9 proof is restricted to candidate port 18888");
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 60_000 || timeoutMs > 60 * 60_000) {
    throw new Error("R9 proof deadline must be from one to sixty minutes");
  }
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("R9 proof requires PRISM_COOKIE_FILE");
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/.test(model)) throw new Error("Invalid live model");
  return Object.freeze({ target, port: PORT, timeoutMs, model, cookieFile: path.resolve(cookieFile),
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, "") });
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
        kind: event.error_kind, durationMs: event.duration_ms, sidecarAgeMs: event.sidecar_peer_age_ms,
        connected: event.transport_connected, reliableStall: event.reliable_stall, lane: event.awareness_lane };
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
  const proofRoot = path.resolve(".ghostprov", "live-r9");
  await mkdir(proofRoot, { recursive: true });
  const directory = await mkdtemp(path.join(proofRoot, "recovery-"));
  assertContained(proofRoot, directory);
  const cookieFile = path.join(directory, "candidate-cookies.json");
  await copyFile(options.cookieFile, cookieFile);
  const logger = new ProofLogger();
  const intended = Array.from({ length: options.target }, () => projectId(crypto.randomUUID()));
  const cleanupEligible = new Set<ProjectId>();
  const deleted = new Set<ProjectId>();
  const titles = new Map(intended.map((id, index) => [id, `GhostProv R9 Recovery ${path.basename(directory)} ${index + 1}`]));
  const tracked = new Map<ProjectId, TrackedSupervisor>();
  const responseIds = new Set<string>();
  const waves: Array<{ phase: string; count: number; durationMs: number }> = [];
  const requestTimings: Array<{ lane: string; durationMs: number }> = [];
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
      GP_PROVISION_CONCURRENCY: "4", GP_COLD_START_CONCURRENCY: String(Math.min(2, options.target)),
      GP_V2_SIDECAR_REVISION: "r9", GP_V2_SIDECAR_PROJECT_REVISIONS: "", GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay",
    }, {
      sessionManager: session, projects, logger,
      manifest: new FleetManifestStore({ file: path.join(directory, "fleet.json"), identityFingerprint: identity.fingerprint }),
      sandboxSupervisorFactory: input => {
        const id = projectId(input.binding.projectId);
        if (!intended.includes(id) || (!tracked.has(id) && tracked.size >= options.target)) throw new Error("Unexpected supervisor project");
        const generation = (tracked.get(id)?.generation ?? 0) + 1;
        if (generation > 2) throw new Error("Live supervisor generation bound exceeded");
        const supervisor = new V2SandboxSupervisor(input);
        const state: TrackedSupervisor = { supervisor, generation, launches: 0, discoveryTurns: 0, launchTurns: 0, latest: null };
        tracked.set(id, state);
        const prepare = input.gateway.prepareRelayExecSession?.bind(input.gateway);
        const exec = input.gateway.runRelayExec?.bind(input.gateway);
        if (!prepare || !exec) throw new Error("Live gateway lacks supervisor bootstrap");
        input.gateway.prepareRelayExecSession = signal => { state.discoveryTurns++; return prepare(signal); };
        input.gateway.runRelayExec = (request, continuation, signal) => { state.launchTurns++; return exec(request, continuation, signal); };
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
    const root = await post("/v1/responses", nativeBody("Remember codeword R9_AMBER_409 and reply with exactly ROOT_OK."), "ROOT_OK");
    reportPhase("stop_sidecars");
    const recoveryStarted = performance.now();
    if (system.registry.readyRuntimes().some(runtime => {
      const state = runtime.snapshot();
      return state.active !== 0 || state.quarantined !== 0 || state.queueDepth !== 0;
    })) throw new Error("Controlled recovery requires idle, unambiguous runtimes");
    const before = new Map([...tracked].map(([id, state]) => [id, { state, latest: state.latest, instance: state.supervisor.instanceId }]));
    const stops = await Promise.allSettled([...before.values()].map(async value => {
      if (!value.latest || value.state.launches !== 1 || value.state.generation !== 1) throw new Error("Startup did not have one exact sidecar launch");
      const stopped = await value.state.supervisor.stop({ launchNonces: [value.latest.nonce], reason: "dedicated_live_recovery" }, AbortSignal.timeout(45_000));
      if (stopped !== value.latest.nonce) throw new Error("Controlled stop returned the wrong launch proof");
    }));
    const stopFailure = stops.find(result => result.status === "rejected");
    if (stopFailure?.status === "rejected") throw stopFailure.reason;
    reportPhase("recover_same_supervisors");
    await waitFor(() => [...before].every(([id, previous]) => {
      const current = tracked.get(id);
      if (current !== previous.state || current?.generation !== 1 || current.launches > 2) throw new Error("Recovery replaced or relaunched the supervisor");
      return current.launches === 2 && current.latest?.nonce !== previous.latest?.nonce;
    }) && readyRuntimes().length === options.target, 180_000);
    for (const [id, previous] of before) {
      const state = tracked.get(id)!;
      if (state.supervisor.instanceId !== previous.instance || state.discoveryTurns !== 1 || state.launchTurns !== 1 ||
        state.latest?.process.pid === previous.latest?.process.pid) throw new Error("Recovery did not retain its original supervisor");
      recovered++;
    }
    recoveryMs = Math.round(performance.now() - recoveryStarted);
    reportPhase("requests_after_recovery");
    await post("/v1/responses", { ...nativeBody("Reply with only the codeword I asked you to remember."), previous_response_id: root }, "R9_AMBER_409");
    await everySandbox("after_each");
    await publicWave("after_http");
    reportPhase("concurrency_soak");
    const soakDeadline = Date.now() + (options.target === 8 ? 90_000 : 30_000);
    for (let wave = 0; wave < (options.target === 8 ? 3 : 1); wave++) await publicWave(`soak_${wave}`);
    while (Date.now() < soakDeadline) {
      if (readyRuntimes().length !== options.target) throw new Error("Soak lost full readiness");
      stabilitySamples++;
      await Bun.sleep(1_000);
    }
    if (logger.invalidEvents !== 0) throw new Error("Live observability validation failed");
    reportPhase("complete");
  } catch (error) {
    failure = error;
    failureSnapshots = runtimeSnapshots();
  } finally {
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
    target: options.target, buildHash: V2_R9_RELEASE.buildHash, bundleSha256: V2_R9_RELEASE.bundleSha256,
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
