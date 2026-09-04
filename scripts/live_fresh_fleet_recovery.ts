import { createHash } from "node:crypto";
import { chmod, copyFile, mkdir, rm, unlink, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { resolveGhostProvApiKey } from "../src/http/api-key";
import type { LoggerLike, LoggerSnapshot } from "../src/observability/logger";
import type {
  ObservabilityEvent,
  ObservabilityPriority,
} from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { FleetProjectsClient } from "../src/fleet/manager";
import { PrismProjectsClient, type PrismProjectSummary } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import type { RunHandle } from "../src/relay/run-handle";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import type { BrowserlessSystem } from "../src/server/system";
import { createBrowserlessSystem } from "../src/server/system";
import type { ProjectId } from "../src/sandbox/types";

const LIVE_GUARD = "GP_LIVE_FRESH_FLEET_RECOVERY";
const MUTATION_GUARD = "GP_LIVE_ALLOW_TEST_PROJECT_MUTATION";
const CANDIDATE_PORT = 18_888;
const TARGET = 8;
const CAPTURE_MAX_EVENTS = 4_096;
const PROJECT_TITLE = /^GhostProv Worker [1-9][0-9]*$/;

export interface FreshFleetRecoveryOptions {
  readonly port: 18_888;
  readonly timeoutMs: number;
  readonly stabilityMs: number;
  readonly model: string;
  readonly cookieFile: string;
  readonly origin: string;
}

export function resolveFreshFleetRecoveryOptions(
  env: Record<string, string | undefined>,
): FreshFleetRecoveryOptions {
  if (env[LIVE_GUARD] !== "1") {
    throw new Error(`Set ${LIVE_GUARD}=1 to run the dedicated live recovery proof`);
  }
  if (env[MUTATION_GUARD] !== "1") {
    throw new Error(`Set ${MUTATION_GUARD}=1 to create and delete dedicated test projects`);
  }
  resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("PRISM_COOKIE_FILE is required");
  const port = Number(env.PORT ?? CANDIDATE_PORT);
  if (port === 18_787) throw new Error("Live recovery proof refuses protected service port 18787");
  if (port !== CANDIDATE_PORT) {
    throw new Error(`Live recovery proof is hard-fenced to candidate port ${CANDIDATE_PORT}`);
  }
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 60_000 || timeoutMs > 60 * 60_000) {
    throw new Error("GP_LIVE_TIMEOUT_MS must be an integer from 60000 through 3600000");
  }
  const stabilityMs = Number(env.GP_LIVE_STABILITY_MS ?? 35_000);
  if (!Number.isSafeInteger(stabilityMs) || stabilityMs < 30_000 || stabilityMs > 10 * 60_000) {
    throw new Error("GP_LIVE_STABILITY_MS must be an integer from 30000 through 600000");
  }
  const model = env.GP_LIVE_MODEL?.trim() || "gpt-5.6-sol";
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(model)) {
    throw new Error("GP_LIVE_MODEL is invalid");
  }
  return Object.freeze({
    port: CANDIDATE_PORT,
    timeoutMs,
    stabilityMs,
    model,
    cookieFile: path.resolve(cookieFile),
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
  });
}

class ProofLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  readonly validationErrors: string[] = [];
  #dropped = 0;

  emit(event: ObservabilityEvent, _priority?: ObservabilityPriority): void {
    try {
      assertSafeObservabilityEvent(event);
    } catch (error) {
      if (this.validationErrors.length < 16) {
        this.validationErrors.push(error instanceof Error ? error.name : "Error");
      }
      return;
    }
    const visible = event.event.startsWith("fleet.") ||
      event.event.startsWith("sandbox.recovery.") ||
      event.event === "prism.circuit.changed" ||
      event.event === "server.started" ||
      event.event === "server.stopping";
    if (!visible) return;
    if (this.events.length >= CAPTURE_MAX_EVENTS) {
      this.#dropped++;
      return;
    }
    this.events.push(event);
    // Full fleet samples stay available to the proof assertions without
    // flooding stdout with per-runtime diagnostics every five seconds.
    if (event.event !== "fleet.sample") console.log(JSON.stringify(event));
  }

  snapshot(): LoggerSnapshot {
    return {
      queuedEvents: 0,
      queuedBytes: 0,
      droppedSamples: this.#dropped,
      droppedProgress: 0,
      droppedTerminals: 0,
      sinkErrors: this.validationErrors.length,
    };
  }

  async close(): Promise<void> {}
}

interface ProjectLedger {
  readonly preexisting: Set<ProjectId>;
  readonly intended: Set<ProjectId>;
  readonly created: Set<ProjectId>;
  readonly deleted: Set<ProjectId>;
}

class TrackedProjectsClient implements FleetProjectsClient {
  constructor(
    readonly source: PrismProjectsClient,
    readonly ledger: ProjectLedger,
  ) {}

  get(id: ProjectId, signal?: AbortSignal): Promise<PrismProjectSummary> {
    return this.source.get(id, signal);
  }

  async create(
    input: { projectId: ProjectId; title: string },
    signal?: AbortSignal,
  ): Promise<PrismProjectSummary> {
    if (this.ledger.preexisting.has(input.projectId)) {
      throw new Error("Generated test project collides with a pre-existing project");
    }
    this.ledger.intended.add(input.projectId);
    const created = await this.source.create(input, signal);
    this.ledger.created.add(created.projectId);
    return created;
  }

  async deleteManaged(id: ProjectId, signal?: AbortSignal): Promise<void> {
    if (!this.ledger.intended.has(id)) {
      throw new Error("Live recovery proof refused deletion outside its project ledger");
    }
    await this.source.deleteManaged(id, signal);
    this.ledger.deleted.add(id);
  }
}

interface LiveRun {
  readonly system: BrowserlessSystem;
  readonly manifest: FleetManifestStore;
  readonly sessionManager: PrismSessionManager;
  readonly projects: TrackedProjectsClient;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    timer.unref?.();
    promise.then(
      value => {
        clearTimeout(timer);
        resolve(value);
      },
      error => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function hashProject(id: ProjectId): string {
  return createHash("sha256").update(id).digest("hex").slice(0, 16);
}

function proofEnvironment(
  env: Record<string, string | undefined>,
  options: FreshFleetRecoveryOptions,
  manifestFile: string,
): Record<string, string | undefined> {
  return {
    ...env,
    GP_CANDIDATE: "1",
    PORT: String(options.port),
    GP_PRISM_SESSION: "authenticated",
    GP_ALLOW_ANONYMOUS: "0",
    GP_TARGET_SANDBOXES: String(TARGET),
    GP_MAX_PROJECTS: "16",
    GP_PROVISION_CONCURRENCY: "4",
    GP_COLD_START_CONCURRENCY: "2",
    GP_PROJECT_REPLACEMENT_FAILURES: "3",
    GP_FLEET_MANIFEST_FILE: manifestFile,
    PRISM_COOKIE_FILE: options.cookieFile,
    GP_V2_SIDECAR_REVISION: "r9",
    GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay",
    GP_V2_SIDECAR_PROJECT_REVISIONS: "",
    PRISM_PROJECT_IDS: "",
    PRISM_DOC_ID: "",
    GP_AUTO_CREATE_PROJECTS: "1",
    GP_PROJECT_REUSE: "managed",
  };
}

async function createRun(
  env: Record<string, string | undefined>,
  options: FreshFleetRecoveryOptions,
  manifestFile: string,
  ledger: ProjectLedger,
  logger: ProofLogger,
): Promise<LiveRun> {
  const sessionManager = await PrismSessionManager.create({
    origin: options.origin,
    mode: "authenticated",
    allowAnonymous: false,
    cookieFile: options.cookieFile,
    initialCookieHeader: env.PRISM_COOKIE?.trim(),
  });
  try {
    const identity = await withTimeout(
      sessionManager.resolve(),
      Math.min(options.timeoutMs, 60_000),
      "Prism identity resolution",
    );
    const source = new PrismProjectsClient(identity.account);
    if (ledger.preexisting.size === 0 && ledger.intended.size === 0) {
      for (const item of await withTimeout(source.list(), 60_000, "Prism project listing")) {
        ledger.preexisting.add(item.projectId);
      }
    }
    const projects = new TrackedProjectsClient(source, ledger);
    const manifest = new FleetManifestStore({
      file: manifestFile,
      identityFingerprint: identity.fingerprint,
    });
    const system = await createBrowserlessSystem(
      proofEnvironment(env, options, manifestFile),
      { sessionManager, projects, manifest, logger },
    );
    return { system, manifest, sessionManager, projects };
  } catch (error) {
    await sessionManager.close().catch(() => undefined);
    throw error;
  }
}

async function startEight(run: LiveRun, timeoutMs: number): Promise<void> {
  await withTimeout(run.system.start(), timeoutMs, "candidate startup");
  await withTimeout(run.system.fleet.waitForReady(TARGET), timeoutMs, "8/8 fleet readiness");
  const fleet = run.system.fleet.snapshot();
  if (fleet.ready !== TARGET || fleet.unavailable !== 0 || fleet.projects.length !== TARGET) {
    throw new Error("Candidate did not reach an exact healthy 8/8 fleet");
  }
}

function outputText(final: Record<string, unknown>): string {
  const output = Array.isArray(final.output) ? final.output : [];
  const values: string[] = [];
  for (const item of output) {
    if (!item || typeof item !== "object" || Array.isArray(item)) continue;
    const content = Array.isArray((item as Record<string, unknown>).content)
      ? (item as Record<string, unknown>).content as unknown[]
      : [];
    for (const part of content) {
      if (!part || typeof part !== "object" || Array.isArray(part)) continue;
      const text = (part as Record<string, unknown>).text;
      if (typeof text === "string") values.push(text);
    }
  }
  return values.join("\n");
}

async function executeHandle(handle: RunHandle, expectedText?: string): Promise<string> {
  let eventCount = 0;
  let final: any = null;
  const consuming = (async () => {
    for await (const _event of handle.events()) eventCount++;
  })();
  const finalizing = handle.final.then(value => {
    final = value;
    return value;
  });
  try {
    await Promise.all([finalizing, consuming]);
    if (typeof final?.id !== "string" || final.id.length === 0 || eventCount === 0) {
      throw new Error("Live response did not provide a streamed terminal identity");
    }
    if (expectedText && !outputText(final).toUpperCase().includes(expectedText.toUpperCase())) {
      throw new Error("Live response did not contain the expected continuation marker");
    }
    return final.id;
  } finally {
    handle.discardUnconsumedEvents?.();
    releaseAtomicBytePermit(final);
  }
}

async function runRouterWave(
  system: BrowserlessSystem,
  model: string,
  wave: number,
  timeoutMs: number,
): Promise<{ durationMs: number; responseIds: string[] }> {
  const started = performance.now();
  const responseIds = await withTimeout(Promise.all(Array.from({ length: 6 }, async (_, index) => {
    const marker = `GHOSTPROV_ROUTER_${wave}_${index}`;
    const handle = await system.router.startRelay({
      model,
      stream: true,
      store: true,
      max_output_tokens: 32,
      reasoning: { effort: "low" },
      input: `Reply with exactly ${marker}`,
    }, { source: "internal" });
    return executeHandle(handle, marker);
  })), timeoutMs, `six-agent router wave ${wave}`);
  if (new Set(responseIds).size !== 6) {
    throw new Error(`Router wave ${wave} returned duplicate provider response IDs`);
  }
  return { durationMs: Math.round(performance.now() - started), responseIds };
}

async function startStoredContinuationRoot(
  system: BrowserlessSystem,
  model: string,
  timeoutMs: number,
): Promise<string> {
  const handle = await withTimeout(system.router.startRelay({
    model,
    stream: true,
    store: true,
    max_output_tokens: 48,
    reasoning: { effort: "low" },
    input: "Remember codeword GHOSTPROV_AMBER_409 and reply with exactly ROOT_OK.",
  }, { source: "internal" }), timeoutMs, "stored continuation root admission");
  return withTimeout(executeHandle(handle, "ROOT_OK"), timeoutMs, "stored continuation root");
}

async function continueStoredResponse(
  system: BrowserlessSystem,
  model: string,
  previousResponseId: string,
  expected: string,
  input: string,
  timeoutMs: number,
): Promise<string> {
  const handle = await withTimeout(system.router.startRelay({
    model,
    stream: true,
    store: true,
    previous_response_id: previousResponseId,
    max_output_tokens: 48,
    reasoning: { effort: "low" },
    input,
  }, { source: "internal" }), timeoutMs, "stored continuation admission");
  return withTimeout(executeHandle(handle, expected), timeoutMs, "stored continuation response");
}

async function runEverySandboxWave(
  system: BrowserlessSystem,
  model: string,
  wave: number,
  timeoutMs: number,
): Promise<{ durationMs: number; responseIds: string[] }> {
  const runtimes = system.registry.readyRuntimes()
    .slice()
    .sort((left, right) => left.projectId.localeCompare(right.projectId));
  if (runtimes.length !== TARGET) throw new Error(`Wave ${wave} did not have eight routable runtimes`);
  const started = performance.now();
  const responseIds = await withTimeout(Promise.all(runtimes.map(async (runtime, index) => {
    const marker = `GHOSTPROV_RECOVERY_${wave}_${index}`;
    const handle = await runtime.run({
      model,
      stream: true,
      store: true,
      max_output_tokens: 32,
      reasoning: { effort: "low" },
      input: `Reply with exactly ${marker}`,
    });
    return executeHandle(handle);
  })), timeoutMs, `live wave ${wave}`);
  if (new Set(responseIds).size !== TARGET) {
    throw new Error(`Wave ${wave} returned duplicate provider response IDs`);
  }
  return { durationMs: Math.round(performance.now() - started), responseIds };
}

async function assertStable(
  system: BrowserlessSystem,
  durationMs: number,
  label: string,
): Promise<number> {
  const deadline = Date.now() + durationMs;
  let samples = 0;
  while (Date.now() < deadline) {
    const fleet = system.fleet.snapshot();
    if (fleet.ready !== TARGET || fleet.unavailable !== 0 ||
      system.registry.readyRuntimes().length !== TARGET) {
      throw new Error(`${label} lost 8/8 readiness`);
    }
    samples++;
    await Bun.sleep(Math.min(1_000, Math.max(1, deadline - Date.now())));
  }
  return samples;
}

async function reconcileUntil(
  run: LiveRun,
  predicate: () => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = null;
  while (Date.now() < deadline) {
    try {
      await withTimeout(run.system.fleet.reconcile(), Math.min(120_000, timeoutMs), label);
      lastError = null;
    } catch (error) {
      lastError = error;
    }
    if (predicate()) return;
    await Bun.sleep(1_000);
  }
  if (lastError instanceof Error) throw lastError;
  throw new Error(`${label} did not converge`);
}

async function replaceOne(
  run: LiveRun,
  logger: ProofLogger,
  targetProject: ProjectId,
  timeoutMs: number,
): Promise<{ oldProject: ProjectId; successor: ProjectId; eventNames: string[] }> {
  const runtime = run.system.registry.readyRuntimes()
    .find(value => value.projectId === targetProject);
  if (!runtime) throw new Error("No healthy dedicated runtime is available for replacement proof");
  const before = new Set(run.system.fleet.snapshot().projects);
  const worker = (await run.manifest.load()).workers.find(
    value => value.projectId === runtime.projectId,
  );
  if (!worker?.managed || !worker.hasReachedReady) {
    throw new Error("Replacement target lacks managed readiness proof");
  }
  const eventOffset = logger.events.length;
  await withTimeout(runtime.stop(), 120_000, "controlled runtime stop");
  for (let attempt = 1; attempt <= 3; attempt++) {
    run.system.fleet.observeRecoveryEvent({
      type: "retry_scheduled",
      projectId: worker.projectId,
      workerIndex: worker.index,
      transport: "v2",
      reason: "runtime",
      attempt,
      incarnation: attempt,
      leaderGeneration: worker.leaderGeneration,
      consecutiveFailures: attempt,
      retryDelayMs: 1,
      attemptDurationMs: 1,
      totalDurationMs: attempt,
      failure: {
        name: "V2BootstrapError",
        kind: "readiness",
        status: null,
        operation: "bootstrap",
        message: "Controlled dedicated-project readiness fault",
      },
    });
  }
  await reconcileUntil(
    run,
    () => {
      const snapshot = run.system.fleet.snapshot();
      return snapshot.ready === TARGET && snapshot.unavailable === 0 &&
        !snapshot.projects.includes(worker.projectId) &&
        run.projects.ledger.deleted.has(worker.projectId);
    },
    timeoutMs,
    "guarded project replacement",
  );
  const after = run.system.fleet.snapshot().projects;
  const successors = after.filter(value => !before.has(value));
  if (successors.length !== 1) throw new Error("Replacement did not install exactly one successor");
  const successor = successors[0]!;
  const events = logger.events.slice(eventOffset);
  const eventNames = events.map(value => value.event);
  const required: ObservabilityEvent["event"][] = [
    "fleet.replacement.qualified",
    "fleet.replacement.started",
    "fleet.replacement.successor_reserved",
    "fleet.replacement.successor_created",
    "fleet.replacement.successor_activated",
    "fleet.replacement.old_retired",
  ];
  let cursor = -1;
  for (const name of required) {
    const next = eventNames.indexOf(name, cursor + 1);
    if (next < 0) throw new Error(`Replacement lifecycle event is missing: ${name}`);
    cursor = next;
  }
  const successorRuntime = run.system.registry.get(successor);
  if (!successorRuntime || successorRuntime.snapshot().state !== "ready") {
    throw new Error("Replacement successor was removed after reporting readiness");
  }
  return { oldProject: worker.projectId, successor, eventNames };
}

async function closeRun(run: LiveRun | null): Promise<void> {
  if (!run) return;
  await run.system.close();
}

async function cleanupDedicatedProjects(
  env: Record<string, string | undefined>,
  options: FreshFleetRecoveryOptions,
  ledger: ProjectLedger,
): Promise<void> {
  if (ledger.intended.size === 0) return;
  if (env[MUTATION_GUARD] !== "1") {
    throw new Error("Dedicated-project cleanup lost its explicit mutation guard");
  }
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
    for (const id of ledger.intended) {
      if (ledger.preexisting.has(id)) {
        throw new Error("Cleanup refused a project present before the live proof");
      }
      let summary: PrismProjectSummary;
      try {
        summary = await withTimeout(projects.get(id), 60_000, "cleanup metadata read");
      } catch (error) {
        const status = error && typeof error === "object" && "status" in error
          ? (error as { status?: unknown }).status
          : null;
        if (status === 404 || status === 410) {
          ledger.deleted.add(id);
          continue;
        }
        throw error;
      }
      if (summary.deleted) {
        ledger.deleted.add(id);
        continue;
      }
      if (!PROJECT_TITLE.test(summary.title)) {
        throw new Error("Cleanup refused a dedicated project with an unexpected title");
      }
      await withTimeout(projects.deleteManaged(id), 60_000, "dedicated project cleanup");
      ledger.deleted.add(id);
      await Bun.sleep(100);
    }
  } finally {
    await session.close();
  }
}

function assertContained(root: string, target: string): void {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("Live proof secret cleanup path escaped its isolated directory");
  }
}

async function removeIsolatedCredentialFiles(root: string, cookieFile: string): Promise<void> {
  const targets = [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`];
  for (const target of targets) assertContained(root, target);
  await unlink(cookieFile).catch(() => undefined);
  await unlink(`${cookieFile}.identity.json`).catch(() => undefined);
  await rm(`${cookieFile}.lock`, { recursive: true, force: true });
}

function countEvents(events: readonly ObservabilityEvent[]): Record<string, number> {
  const result: Record<string, number> = {};
  for (const event of events) result[event.event] = (result[event.event] ?? 0) + 1;
  return result;
}

function permitProof(events: readonly ObservabilityEvent[], offset = 0): {
  coldAcquired: number;
  warmAcquired: number;
  maxColdInUse: number;
  maxTotalInUse: number;
} {
  let coldAcquired = 0;
  let warmAcquired = 0;
  let maxColdInUse = 0;
  let maxTotalInUse = 0;
  for (const event of events.slice(offset)) {
    if (event.event !== "fleet.startup_permits.changed") continue;
    if (event.permit_action === "acquired") {
      if (event.cold_start === true) coldAcquired++;
      else warmAcquired++;
    }
    if (typeof event.cold_in_use === "number") maxColdInUse = Math.max(maxColdInUse, event.cold_in_use);
    if (typeof event.total_in_use === "number") maxTotalInUse = Math.max(maxTotalInUse, event.total_in_use);
  }
  return { coldAcquired, warmAcquired, maxColdInUse, maxTotalInUse };
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolveFreshFleetRecoveryOptions(env);
  const runId = `${new Date().toISOString().replace(/[:.]/g, "-")}-${crypto.randomUUID()}`;
  const proofDirectory = path.resolve(".ghostprov", "live-proof", runId);
  const manifestFile = path.join(proofDirectory, "fleet.json");
  await mkdir(proofDirectory, { recursive: true });
  const isolatedCookieFile = path.join(proofDirectory, "candidate-cookies.json");
  assertContained(proofDirectory, isolatedCookieFile);
  await copyFile(options.cookieFile, isolatedCookieFile);
  if (process.platform !== "win32") await chmod(isolatedCookieFile, 0o600);
  const liveOptions: FreshFleetRecoveryOptions = Object.freeze({
    ...options,
    cookieFile: isolatedCookieFile,
  });
  const ledger: ProjectLedger = {
    preexisting: new Set(),
    intended: new Set(),
    created: new Set(),
    deleted: new Set(),
  };
  const logger = new ProofLogger();
  const allResponseIds = new Set<string>();
  const waves: Array<{ wave: number; durationMs: number }> = [];
  const routerWaves: Array<{ wave: number; durationMs: number }> = [];
  const replacements: Array<{ old: string; successor: string; lifecycleEvents: number }> = [];
  let active: LiveRun | null = null;
  let firstPermitProof: ReturnType<typeof permitProof> | null = null;
  let restartPermitProof: ReturnType<typeof permitProof> | null = null;
  let stabilitySamples = 0;
  let continuationRootId: string | null = null;
  let continuationTurns = 0;
  let primaryError: unknown = null;
  try {
    active = await createRun(env, liveOptions, manifestFile, ledger, logger);
    await startEight(active, liveOptions.timeoutMs);
    firstPermitProof = permitProof(logger.events);
    if (firstPermitProof.coldAcquired < TARGET || firstPermitProof.maxColdInUse > 2) {
      throw new Error("Cold-start permit proof did not cover all eight projects within the cap");
    }
    const initial = await runEverySandboxWave(
      active.system,
      liveOptions.model,
      1,
      liveOptions.timeoutMs,
    );
    initial.responseIds.forEach(id => allResponseIds.add(id));
    waves.push({ wave: 1, durationMs: initial.durationMs });

    const replacementTargets = active.system.fleet.snapshot().projects
      .slice()
      .sort((left, right) => left.localeCompare(right))
      .slice(0, 2);
    if (replacementTargets.length !== 2) throw new Error("Two replacement targets were not available");
    for (let index = 0; index < replacementTargets.length; index++) {
      const result = await replaceOne(
        active,
        logger,
        replacementTargets[index]!,
        liveOptions.timeoutMs,
      );
      replacements.push({
        old: hashProject(result.oldProject),
        successor: hashProject(result.successor),
        lifecycleEvents: result.eventNames.length,
      });
      stabilitySamples += await assertStable(active.system, 15_000, `replacement ${index + 1}`);
      const wave = await runEverySandboxWave(
        active.system,
        liveOptions.model,
        index + 2,
        liveOptions.timeoutMs,
      );
      wave.responseIds.forEach(id => {
        if (allResponseIds.has(id)) throw new Error("Provider response ID repeated across live waves");
        allResponseIds.add(id);
      });
      waves.push({ wave: index + 2, durationMs: wave.durationMs });
    }

    continuationRootId = await startStoredContinuationRoot(
      active.system,
      liveOptions.model,
      liveOptions.timeoutMs,
    );
    if (allResponseIds.has(continuationRootId)) {
      throw new Error("Stored continuation root reused a prior response ID");
    }
    allResponseIds.add(continuationRootId);

    await closeRun(active);
    active = null;
    const restartOffset = logger.events.length;
    active = await createRun(env, liveOptions, manifestFile, ledger, logger);
    await startEight(active, liveOptions.timeoutMs);
    restartPermitProof = permitProof(logger.events, restartOffset);
    if (restartPermitProof.coldAcquired !== 0 || restartPermitProof.warmAcquired < TARGET) {
      throw new Error("Persisted readiness did not keep all restart work on the warm lane");
    }
    const firstContinuation = await continueStoredResponse(
      active.system,
      liveOptions.model,
      continuationRootId,
      "GHOSTPROV_AMBER_409",
      "Reply with only the codeword I asked you to remember.",
      liveOptions.timeoutMs,
    );
    if (allResponseIds.has(firstContinuation)) {
      throw new Error("First continuation reused a prior response ID");
    }
    allResponseIds.add(firstContinuation);
    continuationTurns++;
    const secondContinuation = await continueStoredResponse(
      active.system,
      liveOptions.model,
      firstContinuation,
      "CHAIN_OK",
      "Reply with exactly CHAIN_OK.",
      liveOptions.timeoutMs,
    );
    if (allResponseIds.has(secondContinuation)) {
      throw new Error("Second continuation reused a prior response ID");
    }
    allResponseIds.add(secondContinuation);
    continuationTurns++;
    for (let routerWave = 1; routerWave <= 3; routerWave++) {
      const result = await runRouterWave(
        active.system,
        liveOptions.model,
        routerWave,
        liveOptions.timeoutMs,
      );
      for (const id of result.responseIds) {
        if (allResponseIds.has(id)) throw new Error("Router response ID repeated across live waves");
        allResponseIds.add(id);
      }
      routerWaves.push({ wave: routerWave, durationMs: result.durationMs });
      await Bun.sleep(1_000);
    }
    stabilitySamples += await assertStable(active.system, liveOptions.stabilityMs, "restart soak");
    const finalWave = await runEverySandboxWave(
      active.system,
      liveOptions.model,
      4,
      liveOptions.timeoutMs,
    );
    finalWave.responseIds.forEach(id => {
      if (allResponseIds.has(id)) throw new Error("Provider response ID repeated across live waves");
      allResponseIds.add(id);
    });
    waves.push({ wave: 4, durationMs: finalWave.durationMs });
    const expectedResponses = TARGET * waves.length + 1 + continuationTurns + 6 * routerWaves.length;
    if (allResponseIds.size !== expectedResponses) {
      throw new Error("Live waves did not complete once on every sandbox");
    }
    if (logger.validationErrors.length > 0 || logger.snapshot().droppedSamples > 0) {
      throw new Error("Live proof observability validation was incomplete");
    }
  } catch (error) {
    primaryError = error;
  } finally {
    try {
      await closeRun(active);
    } catch (error) {
      primaryError ??= error;
    }
    try {
      await cleanupDedicatedProjects(env, liveOptions, ledger);
    } catch (error) {
      primaryError ??= error;
    }
    try {
      await removeIsolatedCredentialFiles(proofDirectory, isolatedCookieFile);
    } catch (error) {
      primaryError ??= error;
    }
  }

  const undeleted = [...ledger.intended].filter(id => !ledger.deleted.has(id));
  if (undeleted.length > 0) {
    primaryError ??= new Error("Dedicated live projects were not fully cleaned up");
  }
  if (primaryError) throw primaryError;
  const report = {
    schema: 1,
    live: true,
    candidatePort: options.port,
    protectedPortTouched: false,
    target: TARGET,
    finalReady: TARGET,
    coldStart: firstPermitProof,
    restart: restartPermitProof,
    replacements,
    waves,
    routerWaves,
    completedResponses: allResponseIds.size,
    crossIncarnationContinuation: {
      rootCreatedBeforeRestart: true,
      successfulTurnsAfterRestart: continuationTurns,
      conflict409Observed: false,
    },
    stabilitySamples,
    projectLedger: {
      intended: ledger.intended.size,
      created: ledger.created.size,
      deleted: ledger.deleted.size,
      undeleted: 0,
      hashes: [...ledger.intended].map(hashProject).sort(),
    },
    lifecycleEventCounts: countEvents(logger.events),
  };
  const reportFile = path.join(proofDirectory, "report.json");
  await writeFile(reportFile, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ...report, reportFile }, null, 2));
}

if (import.meta.main) await main();
