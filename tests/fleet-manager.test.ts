import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import { DEFAULT_V2_SIDECAR_REVISION, loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import type { Clock, MachineSample } from "../src/core/ports";
import { MAX_PENDING_FLEET_RECOVERY_EVENTS, ProjectFleetManager, type ProjectFleetManagerOptions } from "../src/fleet/manager";
import { FleetManifestStore } from "../src/fleet/manifest";
import { FileBootstrapHoldMigration, prepareBootstrapHoldMigrationLedger } from "../src/fleet/bootstrap-migration";
import { FileSupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { V2SupervisorBootstrapError, finalizeHeldSupervisorBootstrapCleanup } from "../src/v2/supervisor/controller";
import { V2SupervisorError } from "../src/v2/supervisor/host";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY } from "../src/v2/sidecar/retirement-identities";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type { FleetLifecycleEvent, ManagedRuntimeFactory } from "../src/fleet/types";
import type { RunHandle } from "../src/relay/run-handle";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { PrismProjectSummary } from "../src/prism/projects-client";
import type { FleetRecoverySnapshot } from "../src/prism/recovery-coordinator";
import { SupervisedSandboxRuntime, type SandboxSupervisorEvent } from "../src/sandbox/supervised-runtime";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");
const C = projectId("11111111-1111-4111-8111-111111111111");
const D = projectId("22222222-2222-4222-8222-222222222222");
const E = projectId("33333333-3333-4333-8333-333333333333");
const F = projectId("44444444-4444-4444-8444-444444444444");
const G = projectId("55555555-5555-4555-8555-555555555555");
const H = projectId("66666666-6666-4666-8666-666666666666");
const FINGERPRINT = "a".repeat(64);
const roots: string[] = [];

const MACHINE: MachineSample = {
  totalBytes: 32 * 1024 ** 3,
  freeBytes: 19 * 1024 ** 3,
  rssBytes: 320 * 1024 ** 2,
  heapBytes: 100 * 1024 ** 2,
  externalBytes: 20 * 1024 ** 2,
  logicalProcessors: 12,
};

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

function project(id: ProjectId, title = `GhostProv Worker ${id === A ? 1 : id === B ? 2 : 3}`): PrismProjectSummary {
  return { projectId: id, title, owner: "owner", deleted: false, createdAt: "2026-08-22T10:00:00.000Z" };
}

function uuidSequence(...values: readonly ProjectId[]): () => string {
  let index = 0;
  return () => {
    const value = values[index++];
    if (!value) throw new Error("test UUID sequence exhausted");
    return value;
  };
}

function recoveryRetry(
  projectId: ProjectId,
  consecutiveFailures: number,
  kind = "timeout",
  leaderGeneration = consecutiveFailures,
): SandboxSupervisorEvent {
  return {
    type: "retry_scheduled",
    projectId,
    workerIndex: 1,
    transport: "v2",
    reason: "start",
    attempt: consecutiveFailures,
    incarnation: consecutiveFailures,
    leaderGeneration,
    consecutiveFailures,
    retryDelayMs: 1_000,
    attemptDurationMs: 70_000,
    totalDurationMs: consecutiveFailures * 71_000,
    failure: {
      name: "V2BootstrapError",
      kind,
      status: null,
      operation: "bootstrap",
      message: `Sidecar bootstrap ${kind} failure`,
    },
  };
}

function recoveryCompleted(projectId: ProjectId): SandboxSupervisorEvent {
  return {
    type: "completed",
    projectId,
    workerIndex: 1,
    transport: "v2",
    reason: "start",
    attempt: 1,
    incarnation: 1,
    consecutiveFailures: 0,
    attemptDurationMs: 1,
    totalDurationMs: 1,
    failure: null,
  };
}

function blockedBootstrap(id: ProjectId, count: number): SandboxSupervisorEvent {
  const event = recoveryRetry(id, count, "deploy");
  return { ...event, failure: { ...event.failure!, bootstrapPhase: "deploy", bootstrapCode: "bootstrap_cleanup_unproven" } };
}

function recoveryCircuit(
  state: FleetRecoverySnapshot["state"],
  scope: FleetRecoverySnapshot["scope"],
): Readonly<FleetRecoverySnapshot> {
  return {
    state,
    scope,
    consecutiveFailures: state === "closed" ? 0 : 2,
    queued: 0,
    probeInFlight: false,
    revalidationInFlight: false,
    nextProbeAtMs: state === "closed" ? null : Date.now() + 60_000,
    lastFailureKind: state === "closed" ? null : "account_transport",
  };
}

class FakeProjects {
  readonly created: Array<{ projectId: ProjectId; title: string }> = [];
  readonly deleted: ProjectId[] = [];
  readonly getCalls: ProjectId[] = [];

  constructor(readonly remote: PrismProjectSummary[]) {}

  async list(): Promise<readonly PrismProjectSummary[]> { return [...this.remote]; }

  async get(id: ProjectId): Promise<PrismProjectSummary> {
    this.getCalls.push(id);
    const found = this.remote.find(value => value.projectId === id && !value.deleted);
    if (!found) {
      const error = new Error(`missing project ${id}`) as Error & {
        status: number;
        kind: string;
      };
      error.name = "PrismProjectError";
      error.status = 404;
      error.kind = "http";
      throw error;
    }
    return found;
  }

  async create(input: { projectId: ProjectId; title: string }): Promise<PrismProjectSummary> {
    this.created.push(input);
    const value = project(input.projectId, input.title);
    this.remote.push(value);
    return value;
  }

  async deleteManaged(id: ProjectId): Promise<void> {
    this.deleted.push(id);
    const found = this.remote.find(value => value.projectId === id);
    if (found) found.deleted = true;
  }
}

class InvalidMetadataProjects extends FakeProjects {
  constructor(
    remote: PrismProjectSummary[],
    readonly invalidId: ProjectId,
    readonly invalidResult: PrismProjectSummary,
  ) {
    super(remote);
  }

  override async get(id: ProjectId): Promise<PrismProjectSummary> {
    if (id !== this.invalidId) return super.get(id);
    this.getCalls.push(id);
    return this.invalidResult;
  }
}

class SelectivelyHungProjects extends FakeProjects {
  constructor(remote: PrismProjectSummary[], readonly hungId: ProjectId) {
    super(remote);
  }

  override async get(id: ProjectId): Promise<PrismProjectSummary> {
    if (id !== this.hungId) return super.get(id);
    this.getCalls.push(id);
    return new Promise<PrismProjectSummary>(() => undefined);
  }
}

class TransportMissingProjects extends FakeProjects {
  override async get(id: ProjectId): Promise<PrismProjectSummary> {
    const found = this.remote.find(value => value.projectId === id && !value.deleted);
    if (found) return super.get(id);
    this.getCalls.push(id);
    const error = new TypeError("project control plane unavailable") as TypeError & {
      kind: string;
    };
    error.name = "AccountTransportError";
    error.kind = "transport";
    throw error;
  }
}

class FailingCreateProjects extends FakeProjects {
  createCalls = 0;

  override async create(
    input: { projectId: ProjectId; title: string },
  ): Promise<PrismProjectSummary> {
    this.createCalls++;
    const error = new TypeError("project creation transport unavailable") as TypeError & {
      kind: string;
    };
    error.name = "AccountTransportError";
    error.kind = "transport";
    void input;
    throw error;
  }
}

class DeferredMissingProjects extends FakeProjects {
  readonly missingStarted: Promise<void>;
  #markMissingStarted!: () => void;
  #releaseMissing!: () => void;
  readonly #missingGate: Promise<void>;

  constructor(remote: PrismProjectSummary[], readonly missingId: ProjectId) {
    super(remote);
    this.missingStarted = new Promise(resolve => { this.#markMissingStarted = resolve; });
    this.#missingGate = new Promise(resolve => { this.#releaseMissing = resolve; });
  }

  releaseMissing(): void { this.#releaseMissing(); }

  override async get(id: ProjectId, _signal?: AbortSignal): Promise<PrismProjectSummary> {
    if (id !== this.missingId) return super.get(id);
    this.getCalls.push(id);
    this.#markMissingStarted();
    await this.#missingGate;
    return super.get(id);
  }
}

class ReverseMetadataProjects extends FakeProjects {
  override async get(id: ProjectId): Promise<PrismProjectSummary> {
    if (id === A) await Bun.sleep(20);
    return super.get(id);
  }
}

class FakeRuntime implements RoutableSandboxRuntime {
  state = "configured";
  starts = 0;
  drains = 0;
  heartbeatAgeMs: number | null = 0;
  readonly listeners = new Set<() => void>();

  constructor(readonly projectId: ProjectId) {}

  async start(): Promise<void> { this.starts++; this.state = "ready"; this.emit(); }
  async drain(): Promise<void> { this.drains++; this.state = "stopped"; }
  async stop(): Promise<void> { this.state = "stopped"; }
  async run(): Promise<RunHandle> { throw new Error("not used"); }
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  emit(): void { for (const listener of this.listeners) listener(); }

  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: this.projectId,
      releaseRevision: DEFAULT_V2_SIDECAR_REVISION,
      state: this.state,
      capacity: this.state === "ready" ? 128 : 0,
      active: 0,
      available: this.state === "ready" ? 128 : 0,
      quarantined: 0,
      heartbeatAgeMs: this.heartbeatAgeMs,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 0,
      completionEwmaMs: 0,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
}

class FakeFactory implements ManagedRuntimeFactory {
  readonly created: Array<{
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
    previouslyReady?: boolean;
  }> = [];
  readonly runtimes = new Map<ProjectId, FakeRuntime>();

  create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
    previouslyReady?: boolean;
  }): FakeRuntime {
    this.created.push(input);
    const runtime = new FakeRuntime(input.projectId);
    this.runtimes.set(input.projectId, runtime);
    return runtime;
  }
}

class CompletionEventRuntime extends FakeRuntime {
  stops = 0;

  constructor(project: ProjectId, readonly onReady: () => void) {
    super(project);
  }

  override async start(): Promise<void> {
    await super.start();
    this.onReady();
  }

  override async stop(): Promise<void> {
    this.stops++;
    await super.stop();
  }
}

class CompletionEventFactory extends FakeFactory {
  onReady: () => void = () => {};

  override create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
  }): CompletionEventRuntime {
    this.created.push(input);
    const runtime = new CompletionEventRuntime(input.projectId, () => this.onReady());
    this.runtimes.set(input.projectId, runtime);
    return runtime;
  }
}

class WrongProjectFactory extends FakeFactory {
  override create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
  }): FakeRuntime {
    if (input.projectId !== A) return super.create(input);
    this.created.push(input);
    const runtime = new FakeRuntime(C);
    this.runtimes.set(C, runtime);
    return runtime;
  }
}

class BlockingStartRuntime extends FakeRuntime {
  readonly started: Promise<void>;
  #markStarted!: () => void;
  readonly #gate: Promise<void>;
  #release!: () => void;

  constructor(project: ProjectId) {
    super(project);
    this.started = new Promise(resolve => { this.#markStarted = resolve; });
    this.#gate = new Promise(resolve => { this.#release = resolve; });
  }

  override async start(): Promise<void> {
    this.starts++;
    this.state = "provisioning";
    this.#markStarted();
    this.emit();
    await this.#gate;
    if (this.state === "stopped") return;
    this.state = "ready";
    this.emit();
  }

  override async stop(): Promise<void> {
    this.state = "stopped";
    this.#release();
    this.emit();
  }
}

class BlockingStartFactory extends FakeFactory {
  readonly blocked = new BlockingStartRuntime(A);

  override create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
  }): FakeRuntime {
    if (input.projectId !== A) return super.create(input);
    this.created.push(input);
    this.runtimes.set(A, this.blocked);
    return this.blocked;
  }
}

class LateStartRuntime extends FakeRuntime {
  readonly started: Promise<void>;
  #markStarted!: () => void;
  readonly #gate: Promise<void>;
  #release!: () => void;

  constructor(project: ProjectId) {
    super(project);
    this.started = new Promise(resolve => { this.#markStarted = resolve; });
    this.#gate = new Promise(resolve => { this.#release = resolve; });
  }

  override async start(): Promise<void> {
    this.starts++;
    this.state = "provisioning";
    this.#markStarted();
    this.emit();
    await this.#gate;
    this.state = "ready";
    this.emit();
  }

  releaseLateStart(): void { this.#release(); }

  override async stop(): Promise<void> {
    this.state = "stopped";
    this.emit();
  }
}

class LateThenHealthyFactory extends FakeFactory {
  readonly old = new LateStartRuntime(A);
  #returnedOld = false;

  override create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
  }): FakeRuntime {
    if (input.projectId !== A || this.#returnedOld) return super.create(input);
    this.#returnedOld = true;
    this.created.push(input);
    this.runtimes.set(A, this.old);
    return this.old;
  }
}

class FailingDrainRuntime extends FakeRuntime {
  override async drain(): Promise<void> {
    this.drains++;
    throw new Error("graceful drain failed");
  }
}

class UnstoppableRuntime extends FailingDrainRuntime {
  override async stop(): Promise<void> {
    throw new Error("forced stop failed");
  }
}

class HungDrainRuntime extends FakeRuntime {
  override async drain(): Promise<void> {
    this.drains++;
    return new Promise<void>(() => undefined);
  }
}

class TrackingStartRuntime extends FakeRuntime {
  #release: (() => void) | null = null;

  constructor(
    project: ProjectId,
    readonly owner: TrackingStartFactory,
  ) {
    super(project);
  }

  override async start(): Promise<void> {
    this.starts++;
    this.state = "provisioning";
    this.owner.active++;
    this.owner.peak = Math.max(this.owner.peak, this.owner.active);
    this.owner.started.push(this.projectId);
    this.emit();
    await new Promise<void>(resolve => { this.#release = resolve; });
    this.owner.active--;
    if (this.state === "stopped") return;
    this.state = "ready";
    this.emit();
  }

  release(): void {
    const release = this.#release;
    this.#release = null;
    release?.();
  }

  override async stop(): Promise<void> {
    this.state = "stopped";
    this.release();
    this.emit();
  }
}

class TrackingStartFactory extends FakeFactory {
  active = 0;
  peak = 0;
  readonly started: ProjectId[] = [];

  override create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
  }): FakeRuntime {
    this.created.push(input);
    const runtime = new TrackingStartRuntime(input.projectId, this);
    this.runtimes.set(input.projectId, runtime);
    return runtime;
  }

  release(project: ProjectId): void {
    (this.runtimes.get(project) as TrackingStartRuntime | undefined)?.release();
  }
}

class BlockingClock implements Clock {
  sleeps = 0;
  aborts = 0;

  now(): number { return 1_000; }

  sleep(_ms: number, signal?: AbortSignal): Promise<void> {
    this.sleeps++;
    if (signal?.aborted) {
      this.aborts++;
      return Promise.reject(new DOMException("aborted", "AbortError"));
    }
    return new Promise<void>((_resolve, reject) => {
      const onAbort = () => {
        this.aborts++;
        signal?.removeEventListener("abort", onAbort);
        reject(new DOMException("aborted", "AbortError"));
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void> {
    return this.sleep(Math.max(0, deadlineMs - this.now()), signal);
  }
}

interface StepWaiter {
  ms: number;
  resolve(): void;
  reject(error: unknown): void;
  signal?: AbortSignal;
  onAbort(): void;
}

class StepClock implements Clock {
  nowMs = 1_000;
  readonly delays: number[] = [];
  maxPending = 0;
  readonly #pending: StepWaiter[] = [];

  now(): number { return this.nowMs; }

  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    this.delays.push(ms);
    return new Promise<void>((resolve, reject) => {
      const waiter: StepWaiter = {
        ms,
        resolve,
        reject,
        signal,
        onAbort: () => {
          const index = this.#pending.indexOf(waiter);
          if (index >= 0) this.#pending.splice(index, 1);
          reject(new DOMException("aborted", "AbortError"));
        },
      };
      if (signal?.aborted) {
        waiter.onAbort();
        return;
      }
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#pending.push(waiter);
      this.maxPending = Math.max(this.maxPending, this.#pending.length);
    });
  }

  waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void> {
    return this.sleep(Math.max(0, deadlineMs - this.nowMs), signal);
  }

  releaseNext(): void {
    const waiter = this.#pending.shift();
    if (!waiter) throw new Error("no clock sleep is pending");
    waiter.signal?.removeEventListener("abort", waiter.onAbort);
    this.nowMs += waiter.ms;
    waiter.resolve();
  }
}

async function state(): Promise<{ root: string; cookieFile: string; manifest: FleetManifestStore }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-fleet-manager-"));
  roots.push(root);
  return {
    root,
    cookieFile: path.join(root, "cookies.json"),
    manifest: new FleetManifestStore({
      file: path.join(root, "fleet.json"),
      identityFingerprint: FINGERPRINT,
    }),
  };
}

function config(cookieFile: string, input: {
  explicit: readonly ProjectId[];
  sessionMode?: "authenticated" | "auto" | "anonymous";
  guestMax?: number;
  provisionConcurrency?: number;
  target?: number;
}) {
  const explicit = input.explicit;
  return resolveMultiSandboxConfig({
    GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef",
    PRISM_COOKIE_FILE: cookieFile,
    PRISM_PROJECT_IDS: explicit.join(","),
    GP_TARGET_SANDBOXES: String(input.target ?? explicit.length),
    GP_PRISM_SESSION: input.sessionMode ?? "authenticated",
    GP_ALLOW_ANONYMOUS: input.sessionMode === "auto" ? "1" : "0",
    GP_GUEST_MAX_PROJECTS: String(input.guestMax ?? 4),
    GP_MAX_PROJECTS: "8",
    GP_PROVISION_CONCURRENCY: String(input.provisionConcurrency ?? 2),
    GP_CANDIDATE: "1",
  }, MACHINE);
}

function manager(input: {
  config: ReturnType<typeof config>;
  manifest: FleetManifestStore;
  projects: FakeProjects;
  registry?: SandboxRegistry;
  factory?: FakeFactory;
  identityKind?: "authenticated" | "anonymous";
  clock?: Clock;
  retryInitialDelayMs?: number;
  retryMaxDelayMs?: number;
  random?: () => number;
  metadataAttemptTimeoutMs?: number;
  runtimeCleanupTimeoutMs?: number;
  randomUuid?: () => string;
  recoveryCircuitSnapshot?: () => Readonly<FleetRecoverySnapshot>;
  canRetireProject?: (projectId: ProjectId) => boolean;
  bootstrapMigration?: FileBootstrapHoldMigration;
  cleanupHeldBootstrap?: ProjectFleetManagerOptions["cleanupHeldBootstrap"];
  finalizeHeldBootstrapCleanup?: ProjectFleetManagerOptions["finalizeHeldBootstrapCleanup"];
  bootstrapCleanupTimeoutMs?: number;
  onLifecycleEvent?: (event: FleetLifecycleEvent) => void;
}) {
  const factory = input.factory ?? new FakeFactory();
  const registry = input.registry ?? new SandboxRegistry();
  return {
    factory,
    registry,
    value: new ProjectFleetManager({
      config: input.config,
      identity: { kind: input.identityKind ?? "authenticated", fingerprint: FINGERPRINT },
      projects: input.projects,
      manifest: input.manifest,
      registry,
      runtimeFactory: factory,
      clock: input.clock,
      retryInitialDelayMs: input.retryInitialDelayMs,
      retryMaxDelayMs: input.retryMaxDelayMs,
      random: input.random ?? (() => 0.5),
      randomUuid: input.randomUuid,
      metadataAttemptTimeoutMs: input.metadataAttemptTimeoutMs,
      runtimeCleanupTimeoutMs: input.runtimeCleanupTimeoutMs,
      recoveryCircuitSnapshot: input.recoveryCircuitSnapshot,
      canRetireProject: input.canRetireProject,
      bootstrapMigration: input.bootstrapMigration,
      cleanupHeldBootstrap: input.cleanupHeldBootstrap,
      finalizeHeldBootstrapCleanup: input.finalizeHeldBootstrapCleanup,
      bootstrapCleanupTimeoutMs: input.bootstrapCleanupTimeoutMs,
      onLifecycleEvent: input.onLifecycleEvent,
    }),
  };
}

function waitForRegistryReady(
  registry: SandboxRegistry,
  count: number,
  timeoutMs = 5_000,
): Promise<void> {
  if (registry.readyRuntimes().length >= count) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(new Error(`registry did not reach ${count} ready runtimes`));
    }, timeoutMs);
    const unsubscribe = registry.subscribe(() => {
      if (registry.readyRuntimes().length < count) return;
      clearTimeout(timeout);
      unsubscribe();
      resolve();
    });
  });
}

async function waitForCondition(
  condition: () => boolean,
  label: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`);
    await Bun.sleep(5);
  }
}

describe("ProjectFleetManager", () => {
  it("releases migration journal leases before cleaning a failed hold persistence attempt", async () => {
    const files = await state();
    const manifestFile = path.join(files.root, "fleet.json");
    const migration = new FileBootstrapHoldMigration({ manifestFile, identityFingerprint: FINGERPRINT });
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects: new FakeProjects([]), clock: new BlockingClock(),
      bootstrapMigration: migration, randomUuid: uuidSequence(A, B, C, D), runtimeCleanupTimeoutMs: 500 });
    await harness.value.reconcile();
    const release = V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY;
    const binding = { projectId: A, sandboxSessionId: "offline-old-sandbox", leaderGeneration: 1,
      buildHash: release.buildHash, schemaHash: release.schemaHash, nonce: "offline-old-bootstrap",
      protocolRevision: "r2" as const, schemaVersion: 2 as const };
    const sessionId = crypto.randomUUID();
    const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: {
      instanceId: "offline-old-instance", credential: { url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/",
        docId: A, token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() },
    } });
    const journal = new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, A);
    await journal.withLease(undefined, entry => entry.save({ version: 1, phase: "submitted", sessionId, binding, connectTimeoutMs: 1_000, sealed }));
    await prepareBootstrapHoldMigrationLedger({ manifestFile, identityFingerprint: FINGERPRINT, projectIds: [A] });
    const runtime = harness.factory.runtimes.get(A)!;
    runtime.state = "recovering";
    let stopping: Promise<void> | undefined;
    runtime.stop = () => {
      runtime.state = "stopped";
      return stopping ??= journal.withLease(undefined, async () => undefined);
    };
    const markDraining = files.manifest.markDraining.bind(files.manifest);
    files.manifest.markDraining = async () => { throw new Error("offline hold persistence failure"); };
    try {
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
      await expect(harness.value.reconcile()).rejects.toThrow("offline hold persistence failure");
      expect(harness.registry.get(A)).toBeUndefined();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)?.retirementReason).toBeNull();
    } finally {
      files.manifest.markDraining = markDraining;
      await stopping;
      await harness.value.shutdown("test-complete");
    }
  });

  it.each([2, 3])("restores %i exact legacy migration projects beyond four holds and never refills their grants", async grantCount => {
    const files = await state();
    const manifestFile = path.join(files.root, "fleet.json");
    const ids = Array.from({ length: 12 + grantCount }, (_, index) =>
      projectId(`00000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`));
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (let index = 0; index < 12; index++) {
      const id = ids[index]!;
      await files.manifest.reserveWorker({ index: index + 1, projectId: id, title: `fixture ${index}`, nowMs: 1 });
      await files.manifest.markCreated(id, 2);
      if (index < 4) await files.manifest.markDraining(id, "bootstrap_hold", 3);
    }
    for (const id of ids.slice(12 - grantCount, 12)) {
      const identity = V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY;
      const binding = { projectId: id, sandboxSessionId: "offline-old-sandbox", leaderGeneration: 1,
        buildHash: identity.buildHash, schemaHash: identity.schemaHash, nonce: "offline-old-bootstrap",
        protocolRevision: "r2" as const, schemaVersion: 2 as const };
      const sessionId = crypto.randomUUID();
      const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: {
        instanceId: "offline-old-instance", credential: { url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/",
          docId: id, token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() },
      } });
      await new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id).withLease(undefined, entry =>
        entry.save({ version: 1, phase: "submitted", sessionId, binding, connectTimeoutMs: 1_000, sealed }));
    }
    await prepareBootstrapHoldMigrationLedger({ manifestFile, identityFingerprint: FINGERPRINT, projectIds: ids.slice(12 - grantCount, 12) });
    const projects = new FakeProjects(ids.slice(0, 12).map(id => project(id)));
    const migration = new FileBootstrapHoldMigration({ manifestFile, identityFingerprint: FINGERPRINT });
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, clock: new BlockingClock(), bootstrapMigration: migration,
      randomUuid: uuidSequence(...ids.slice(12)), canRetireProject: () => false });
    try {
      const firstJournal = new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, ids[12 - grantCount]!);
      const original = await firstJournal.withLease(undefined, entry => entry.load());
      await firstJournal.withLease(undefined, entry => entry.save({ ...original!, connectTimeoutMs: 2_000 }));
      // A stale grant denies extra holds, but must not prevent ordinary workers
      // from starting or reconciling after that project recovers independently.
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 8 });
      await firstJournal.withLease(undefined, entry => entry.save(original!));
      for (const id of ids.slice(12 - grantCount, 12)) {
        harness.factory.runtimes.get(id)!.state = "recovering";
        for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      }
      for (let replacement = 1; replacement <= grantCount; replacement++) {
        await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 8 - grantCount + replacement });
      }
      expect(harness.value.snapshot()).toMatchObject({ ready: 8, unavailable: 0 });
      expect(harness.value.snapshot().heldProjects).toHaveLength(4 + grantCount);
      expect(projects.created.map(worker => worker.projectId)).toEqual(ids.slice(12));
      expect(projects.deleted).toEqual([]);
      for (const id of [ids[4]!, ids[12]!]) {
        harness.factory.runtimes.get(id)!.state = "recovering";
        for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      }
      await harness.value.reconcile();
      expect(projects.created).toHaveLength(grantCount);
      expect(harness.value.snapshot().heldProjects).toHaveLength(4 + grantCount);
    } finally { await harness.value.shutdown("migration-restart"); }
    for (const id of ids.slice(12 - grantCount, 12)) {
      await new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id).withLease(undefined, entry => entry.clear());
    }
    const restarted = manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, clock: new BlockingClock(), bootstrapMigration: migration,
      randomUuid: () => { throw new Error("consumed migration grants must not create projects"); } });
    try {
      await expect(restarted.value.reconcile()).resolves.toMatchObject({ ready: 8, unavailable: 0 });
      await restarted.value.reconcile();
      expect(restarted.value.snapshot().heldProjects).toHaveLength(4 + grantCount);
      expect(projects.created).toHaveLength(grantCount);
      expect(projects.deleted).toEqual([]);
    } finally { await restarted.value.shutdown("test-complete"); }
  });

  it("processes another worker's recovery evidence while an unrelated startup remains pending", async () => {
    const files = await state();
    const factory = new BlockingStartFactory();
    const lifecycle: FleetLifecycleEvent[] = [];
    const projects = new FakeProjects([A, B, C, D, E].map(id => project(id)));
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B, C, D, E], provisionConcurrency: 5 }),
      manifest: files.manifest,
      projects,
      factory,
      clock: new BlockingClock(),
      randomUuid: uuidSequence(F),
      onLifecycleEvent: event => lifecycle.push(event),
    });
    const reconciling = harness.value.reconcile().catch(error => error);
    try {
      await Promise.all([factory.blocked.started, waitForRegistryReady(harness.registry, 4)]);
      factory.runtimes.get(B)!.state = "recovering";
      for (let count = 1; count <= 3; count++) {
        harness.value.observeRecoveryEvent(recoveryRetry(B, count));
      }
      await waitForCondition(() => lifecycle.some(event =>
        event.type === "replacement_qualified" && event.projectId === B),
      "recovery qualification while another startup is pending", 1_000);
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === B)
        ?.replacementEvidence?.count).toBe(3);
      await harness.value.reconcile();
      expect(projects.created.map(worker => worker.projectId)).toEqual([F]);
      expect(factory.runtimes.get(F)?.state).toBe("ready");
      expect(factory.blocked.starts).toBe(1);
      expect(factory.blocked.state).toBe("provisioning");
      expect(factory.created.filter(worker => worker.projectId === A)).toHaveLength(1);
    } finally {
      await harness.value.shutdown("test-complete");
      await reconciling;
    }
  });

  it("counts one submitted cleanup debt across changing supervisor failure phases", async () => {
    const files = await state();
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      clock: new BlockingClock(),
      onLifecycleEvent: event => lifecycle.push(event),
    });
    try {
      await harness.value.reconcile();
      harness.factory.runtimes.get(A)!.state = "recovering";
      for (const [index, bootstrapReason] of ([undefined, "transport", "challenge"] as const).entries()) {
        const event = blockedBootstrap(A, index + 1);
        harness.value.observeRecoveryEvent({
          ...event,
          failure: { ...event.failure!, bootstrapReason },
        });
      }
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)
        ?.replacementEvidence?.count).toBe(3);
      expect(lifecycle.some(event => event.type === "replacement_qualified" && event.projectId === A)).toBe(true);
    } finally { await harness.value.shutdown("test-complete"); }
  });

  it("keeps different bootstrap ownership conflicts as distinct replacement evidence", async () => {
    const files = await state();
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      clock: new BlockingClock(),
      onLifecycleEvent: event => lifecycle.push(event),
    });
    try {
      await harness.value.reconcile();
      harness.factory.runtimes.get(A)!.state = "recovering";
      for (const [index, bootstrapReason] of (["file_mismatch", "generation_conflict"] as const).entries()) {
        const event = recoveryRetry(A, index + 1, "deploy");
        harness.value.observeRecoveryEvent({ ...event, failure: { ...event.failure!, bootstrapPhase: "deploy",
          bootstrapCode: "bootstrap_ownership_conflict", bootstrapReason } });
      }
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)
        ?.replacementEvidence?.count).toBe(1);
      expect(lifecycle.some(event => event.type === "replacement_qualified")).toBe(false);
    } finally { await harness.value.shutdown("test-complete"); }
  });

  it("invalidates replacement evidence across a bounded recovery-event overflow", async () => {
    const files = await state();
    const projects = new FakeProjects([A, B, C, D].map(id => project(id)));
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B, C, D] }),
      manifest: files.manifest,
      projects,
      clock: new BlockingClock(),
    });
    try {
      await harness.value.reconcile();
      harness.factory.runtimes.get(A)!.state = "recovering";
      harness.value.observeRecoveryEvent(recoveryRetry(A, 1));
      harness.value.observeRecoveryEvent(recoveryRetry(A, 2));
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)
        ?.replacementEvidence?.count).toBe(2);
      for (let index = 0; index <= MAX_PENDING_FLEET_RECOVERY_EVENTS; index++) {
        harness.value.observeRecoveryEvent(recoveryRetry(A, 3));
      }
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)
        ?.replacementEvidence?.count).toBe(1);
      expect(projects.created).toEqual([]);
      expect(projects.deleted).toEqual([]);
    } finally {
      await harness.value.shutdown("test-complete");
    }
  });

  it("keeps startup permits and exact starts across yielded reconciliations", async () => {
    const files = await state();
    const factory = new TrackingStartFactory();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B, C], provisionConcurrency: 1 }),
      manifest: files.manifest,
      projects: new FakeProjects([A, B, C].map(id => project(id))),
      factory,
      clock: new BlockingClock(),
    });
    const reconciling = harness.value.reconcile();
    try {
      await waitForCondition(() => factory.started.length === 1, "first pending start");
      harness.value.observeRecoveryEvent(recoveryRetry(A, 1, "deploy"));
      await reconciling;
      await harness.value.reconcile();
      expect(factory.created).toHaveLength(3);
      expect(factory.started).toHaveLength(1);
      expect(factory.peak).toBe(1);
      factory.release(factory.started[0]!);
      await waitForCondition(() => factory.started.length === 2, "second pending start");
      expect(factory.peak).toBe(1);
    } finally {
      await harness.value.shutdown("test-complete");
    }
  });

  it("invalidates replacement evidence when a completed recovery reset cannot be read durably", async () => {
    const files = await state();
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      clock: new BlockingClock(),
      onLifecycleEvent: event => lifecycle.push(event),
    });
    try {
      await harness.value.reconcile();
      harness.factory.runtimes.get(A)!.state = "recovering";
      harness.value.observeRecoveryEvent(recoveryRetry(A, 1));
      harness.value.observeRecoveryEvent(recoveryRetry(A, 2));
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers[0]?.replacementEvidence?.count).toBe(2);
      const load = files.manifest.load.bind(files.manifest);
      let readFailed = false;
      files.manifest.load = () => {
        if (!readFailed) {
          readFailed = true;
          return Promise.reject(new Error("temporary manifest read failure"));
        }
        return load();
      };
      harness.value.observeRecoveryEvent(recoveryCompleted(A));
      await harness.value.reconcile();
      harness.value.observeRecoveryEvent(recoveryRetry(A, 3));
      await harness.value.reconcile();
      expect(readFailed).toBe(true);
      expect((await files.manifest.load()).workers[0]?.replacementEvidence?.count).toBe(1);
      expect(lifecycle.some(event => event.type === "replacement_qualified")).toBe(false);
    } finally {
      await harness.value.shutdown("test-complete");
    }
  });

  it("never reactivates a bootstrap hold when its old startup completes late", async () => {
    const files = await state();
    const factory = new LateThenHealthyFactory();
    const projects = new FakeProjects([]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      factory,
      clock: new BlockingClock(),
      randomUuid: uuidSequence(A, B, C, D, E),
    });
    const reconciling = harness.value.reconcile();
    try {
      await Promise.all([factory.old.started, waitForRegistryReady(harness.registry, 3)]);
      for (let count = 1; count <= 3; count++) {
        harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
      }
      await reconciling;
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, heldProjects: [A] });
      factory.old.releaseLateStart();
      await waitForCondition(() => factory.old.state === "stopped", "late held-start cleanup");
      await harness.value.reconcile();
      expect(harness.registry.get(A)).toBeUndefined();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A))
        .toMatchObject({ state: "draining", retirementReason: "bootstrap_hold" });
      expect(projects.deleted).toEqual([]);
    } finally {
      factory.old.releaseLateStart();
      await harness.value.shutdown("test-complete");
    }
  });

  it("waits for stop completion before restarting a failed runtime already reporting stopped", async () => {
    const files = await state();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      clock: new BlockingClock(),
      runtimeCleanupTimeoutMs: 5,
    });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)!;
    let finishStop!: () => void;
    let stopCalls = 0;
    const stopping = new Promise<void>(resolve => { finishStop = resolve; });
    old.state = "failed";
    old.stop = () => { stopCalls++; old.state = "stopped"; return stopping; };
    try {
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(harness.registry.get(A)).toBe(old);
      expect(harness.factory.created).toHaveLength(1);
      expect(stopCalls).toBe(1);
      finishStop();
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 1 });
      expect(harness.registry.get(A)).not.toBe(old);
      expect(harness.factory.created).toHaveLength(2);
    } finally {
      finishStop();
      await harness.value.shutdown("test-complete");
    }
  });

  it("restarts an empty failed runtime on its exact project and worker generation", async () => {
    const files = await state();
    const projects = new FakeProjects([project(A)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects,
      canRetireProject: () => false,
    });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)!;
    old.state = "failed";
    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 1 });
    expect(old.state).toBe("stopped");
    expect(harness.factory.created.map(worker => [worker.projectId, worker.workerIndex, worker.leaderGeneration]))
      .toEqual([[A, 1, 1], [A, 1, 2]]);
    expect(harness.factory.created[1]?.previouslyReady).toBe(true);
    expect(projects.created).toEqual([]);
    expect(projects.deleted).toEqual([]);
    await harness.value.shutdown("test-complete");
  });

  it.each(["active", "quarantined", "ambiguousQuarantined"] as const)(
    "retains a failed runtime that still owns %s work", async field => {
      const files = await state();
      const harness = manager({
        config: config(files.cookieFile, { explicit: [A] }),
        manifest: files.manifest,
        projects: new FakeProjects([project(A)]),
        clock: new BlockingClock(),
      });
      await harness.value.reconcile();
      const old = harness.factory.runtimes.get(A)!;
      old.state = "failed";
      const snapshot = old.snapshot.bind(old);
      old.snapshot = () => ({ ...snapshot(), [field]: 1 });
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 0, failed: 1 });
      expect(old.state).toBe("failed");
      expect(harness.registry.get(A)).toBe(old);
      expect(harness.factory.created).toHaveLength(1);
      old.snapshot = snapshot;
      await harness.value.shutdown("test-complete");
    },
  );

  it("does not let a completed recovery wake cancel its own active reconciliation", async () => {
    const files = await state();
    const projects = new TransportMissingProjects([]);
    const factory = new CompletionEventFactory();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A], target: 1 }),
      manifest: files.manifest,
      projects,
      factory,
      retryInitialDelayMs: 1,
      retryMaxDelayMs: 4,
    });
    factory.onReady = () => harness.value.observeRecoveryEvent(recoveryCompleted(A));

    await harness.value.reconcile();
    projects.remote.push(project(A));
    await waitForCondition(
      () => harness.value.snapshot().ready === 1 &&
        (factory.runtimes.get(A) as CompletionEventRuntime | undefined)?.stops === 0,
      "stable completed retry",
      1_000,
    );
    await Bun.sleep(20);
    expect(harness.value.snapshot()).toMatchObject({ ready: 1, unavailable: 0 });
    expect((factory.runtimes.get(A) as CompletionEventRuntime).stops).toBe(0);
    await harness.value.shutdown("test-complete");
  });

  it("keeps a previously ready failed project on the warm recovery lane", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({
      index: 1,
      projectId: A,
      title: "worker 1",
      nowMs: 1,
    });
    await files.manifest.markCreated(A, 2);
    await files.manifest.markState(A, "ready", 3);
    await files.manifest.markState(A, "failed", 4);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 1 }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
    });

    await harness.value.reconcile();
    expect(harness.factory.created).toHaveLength(1);
    expect(harness.factory.created[0]?.previouslyReady).toBe(true);
    await harness.value.shutdown("test-complete");
  });

  it("creates a project-less fleet once and restarts from the manifest without environment IDs", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    const first = manager({
      config: config(files.cookieFile, { explicit: [], target: 3, provisionConcurrency: 3 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(A, B, C),
    });

    await expect(first.value.reconcile()).resolves.toMatchObject({
      desired: 3,
      ready: 3,
      unavailable: 0,
    });
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C]);
    const persisted = await files.manifest.load();
    expect(persisted.legacyMigrationCompleted).toBe(true);
    expect(persisted.workers.every(worker => worker.managed)).toBe(true);
    await first.value.shutdown("restart");

    const second = manager({
      config: config(files.cookieFile, { explicit: [], target: 3, provisionConcurrency: 3 }),
      manifest: files.manifest,
      projects,
      randomUuid: () => { throw new Error("restart must not allocate a project"); },
    });
    await expect(second.value.reconcile()).resolves.toMatchObject({ ready: 3 });
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C]);
    await second.value.shutdown("test-complete");
  });

  it("replaces a conclusively failed managed project only after identical local failures", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(A, B, C, D, E),
      canRetireProject: () => true,
      onLifecycleEvent: event => lifecycle.push(event),
    });
    await harness.value.reconcile();
    const failed = harness.factory.runtimes.get(A)!;
    failed.state = "recovering";

    harness.value.observeRecoveryEvent(recoveryRetry(A, 1, "timeout"));
    harness.value.observeRecoveryEvent(recoveryRetry(A, 2, "timeout"));
    harness.value.observeRecoveryEvent(recoveryRetry(A, 3, "hello"));
    await harness.value.reconcile();
    expect(projects.created).toHaveLength(4);
    expect(projects.deleted).toEqual([]);

    harness.value.observeRecoveryEvent(recoveryRetry(A, 2, "hello", 4));
    harness.value.observeRecoveryEvent(recoveryRetry(A, 3, "hello", 5));
    await expect(harness.value.reconcile()).resolves.toMatchObject({
      desired: 4,
      ready: 4,
      unavailable: 0,
    });
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C, D, E]);
    expect(projects.deleted).toEqual([A]);
    expect((await files.manifest.load()).workers.map(worker => worker.projectId).sort())
      .toEqual([B, C, D, E].sort());
    expect(lifecycle.map(event => event.type)).toEqual([
      "replacement_qualified",
      "replacement_started",
      "successor_reserved",
      "successor_created",
      "successor_activated",
      "old_retired",
    ]);
    await harness.value.shutdown("test-complete");
  });

  it("automatically restores blocked bootstrap capacity while preserving the old project and continuation bindings", async () => {
    const files = await state(), projects = new FakeProjects([]);
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, randomUuid: uuidSequence(A, B, C, D, E), canRetireProject: () => false });
    await harness.value.reconcile(); harness.factory.runtimes.get(A)!.state = "recovering";
    const markDraining = files.manifest.markDraining.bind(files.manifest);
    files.manifest.markDraining = async (id, reason, now) => {
      if (reason === "bootstrap_hold") expect(harness.factory.runtimes.get(id)!.state).toBe("stopped");
      return markDraining(id, reason, now);
    };
    for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, unavailable: 0, heldProjects: [A], pendingRetirements: 1 });
    expect(harness.registry.get(A)).toBeUndefined(); expect(projects.deleted).toEqual([]);
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C, D, E]);
    expect((await files.manifest.load()).workers.find(worker => worker.projectId === A))
      .toMatchObject({ state: "draining", retirementReason: "bootstrap_hold" });
    harness.value.observeRecoveryEvent(recoveryCompleted(A));
    await harness.value.reconcile(); expect(projects.deleted).toEqual([]); expect(projects.created).toHaveLength(5);
    await harness.value.shutdown("restart");
    const restarted = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, randomUuid: () => { throw new Error("must not allocate again"); } });
    await expect(restarted.value.reconcile()).resolves.toMatchObject({ ready: 4, heldProjects: [A] });
    expect(restarted.registry.get(A)).toBeUndefined(); expect(projects.deleted).toEqual([]);
    await restarted.value.shutdown("test-complete");
  });

  it("does not hold generic deployment errors or turn duplicate observations into replacement proof", async () => {
    const files = await state(), projects = new FakeProjects([]);
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4 }), manifest: files.manifest,
      projects, randomUuid: uuidSequence(A, B, C, D, E) });
    await harness.value.reconcile(); harness.factory.runtimes.get(A)!.state = "recovering";
    for (let count = 1; count <= 4; count++) harness.value.observeRecoveryEvent(recoveryRetry(A, count, "deploy"));
    await harness.value.reconcile(); expect(projects.created).toHaveLength(4);
    for (let count = 1; count <= 4; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, 1));
    await harness.value.reconcile(); expect(projects.created).toHaveLength(4);
    expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)?.replacementEvidence?.count).toBe(1);
    await harness.value.shutdown("test-complete");
  });

  it.each(["rejected", "pending"] as const)("keeps a %s held cleanup owner without blocking unrelated fleet capacity", async outcome => {
    const files = await state(), projects = new FakeProjects([]);
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock: new BlockingClock(), runtimeCleanupTimeoutMs: 5,
      randomUuid: uuidSequence(A, B, C, D, E) });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)!;
    old.state = "recovering";
    let finish!: () => void;
    let calls = 0;
    const pending = new Promise<void>(resolve => { finish = resolve; });
    const stopping = outcome === "pending" ? pending : Promise.reject(new Error("offline exact cleanup unproven"));
    void stopping.catch(() => undefined);
    old.stop = () => { calls++; old.state = "stopped"; return stopping; };
    for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
    try {
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, heldProjects: [A] });
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, heldProjects: [A] });
      expect(harness.registry.get(A)).toBe(old);
      expect(old.snapshot()).toMatchObject({ state: "stopped", active: 0, quarantined: 0 });
      expect(calls).toBe(1);
      expect(projects.created.map(value => value.projectId)).toEqual([A, B, C, D, E]);
      expect(projects.deleted).toEqual([]);
      if (outcome === "pending") {
        finish();
        await harness.value.reconcile();
        expect(harness.registry.get(A)).toBeUndefined();
      }
    } finally {
      finish();
      if (outcome === "rejected") await expect(harness.value.shutdown("test-complete")).rejects.toThrow("incomplete");
      else await harness.value.shutdown("test-complete");
    }
  });

  it("does not let a retained held runtime starve another held project's cleanup", async () => {
    const files = await state(), projects = new FakeProjects([]), cleaned: ProjectId[] = [];
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock: new BlockingClock(), runtimeCleanupTimeoutMs: 5,
      randomUuid: uuidSequence(A, B, C, D, E),
      cleanupHeldBootstrap: async worker => { cleaned.push(worker.projectId); return false; },
      finalizeHeldBootstrapCleanup: async () => {} });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)!;
    old.state = "recovering";
    old.stop = async () => { old.state = "stopped"; throw new Error("offline cleanup unproven"); };
    await files.manifest.reserveWorker({ index: 6, projectId: F, title: "independent hold", nowMs: 1_000 });
    await files.manifest.markDraining(F, "bootstrap_hold", 1_000);
    try {
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
      await harness.value.reconcile();
      expect(cleaned).toEqual([F]);
      expect(harness.registry.get(A)).toBe(old);
      expect(projects.deleted).toEqual([]);
    } finally { await expect(harness.value.shutdown("test-complete")).rejects.toThrow("incomplete"); }
  });

  it("retries exact held cleanup after the failed owner's real local cleanup finishes", async () => {
    const files = await state(), projects = new FakeProjects([]);
    const clock = new BlockingClock();
    let now = 1000;
    clock.now = () => now;
    let localComplete = false, proof = false, probes = 0, finalized = 0, stopCalls = 0;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock, runtimeCleanupTimeoutMs: 5,
      randomUuid: uuidSequence(A, B, C, D, E),
      cleanupHeldBootstrap: async worker => { expect(worker.projectId).toBe(A); probes++; return proof; },
      finalizeHeldBootstrapCleanup: async worker => { expect(worker.projectId).toBe(A); finalized++; } });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)! as FakeRuntime & { localCleanupComplete(): boolean };
    old.state = "recovering";
    old.localCleanupComplete = () => localComplete;
    old.stop = async () => { stopCalls++; old.state = "stopped"; throw new Error("offline remote cleanup unproven"); };
    try {
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
      await harness.value.reconcile();
      expect(harness.value.snapshot().ready).toBe(4);
      expect(probes).toBe(0);
      expect(harness.registry.get(A)).toBe(old);
      // A public rejection does not authorize touching an internally pending owner.
      await harness.value.reconcile();
      expect(probes).toBe(0);
      localComplete = true;
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(probes).toBe(1);
      expect(harness.registry.get(A)).toBe(old);
      expect(projects.deleted).toEqual([]);
      // The exact supervisor becomes reachable after the bounded retry delay.
      proof = true;
      now += 30_001;
      await harness.value.reconcile();
      await harness.value.reconcile();
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(probes).toBe(2);
      expect(finalized).toBe(1);
      expect(stopCalls).toBe(1);
      expect(harness.registry.get(A)).toBeUndefined();
      expect(projects.deleted).toEqual([A]);
      expect((await files.manifest.load()).workers.some(worker => worker.projectId === A)).toBe(false);
    } finally {
      if (harness.registry.get(A)) await expect(harness.value.shutdown("test-complete")).rejects.toThrow("incomplete");
      else await harness.value.shutdown("test-complete");
    }
  });

  it("requires exact remote proof when cleanup debt arrives after a cached successful stop", async () => {
    const files = await state(), projects = new FakeProjects([]), clock = new BlockingClock();
    let now = 1000, localComplete = false, needsProof = false, proof = false, probes = 0, stops = 0;
    clock.now = () => now;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock, runtimeCleanupTimeoutMs: 5, randomUuid: uuidSequence(A, B, C, D, E),
      cleanupHeldBootstrap: async () => { probes++; return proof; }, finalizeHeldBootstrapCleanup: async () => {} });
    await harness.value.reconcile();
    const old = harness.factory.runtimes.get(A)! as FakeRuntime & { localCleanupComplete(): boolean; cleanupProofRequired(): boolean };
    old.localCleanupComplete = () => localComplete;
    old.cleanupProofRequired = () => needsProof;
    old.state = "recovering";
    old.stop = async () => { stops++; old.state = "stopped"; };
    try {
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
      await harness.value.reconcile();
      expect(harness.registry.get(A)).toBe(old);
      expect(probes).toBe(0);
      // A late child physically closes but reports missing remote cleanup proof.
      localComplete = true;
      needsProof = true;
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(harness.registry.get(A)).toBe(old);
      expect(probes).toBe(1);
      expect(projects.deleted).toEqual([]);
      proof = true; now += 30_001;
      for (let turn = 0; turn < 4; turn++) await harness.value.reconcile();
      expect(probes).toBe(2);
      expect(harness.registry.get(A)).toBeUndefined();
      expect(projects.deleted).toEqual([A]);
      expect(stops).toBe(1);
    } finally {
      if (harness.registry.get(A)) await expect(harness.value.shutdown("test-complete")).rejects.toThrow("incomplete");
      else await harness.value.shutdown("test-complete");
    }
  });

  it("recovers a real supervisor's late factory cleanup debt through exact held proof without restarting", async () => {
    const files = await state(), clock = new BlockingClock();
    let now = 1000;
    clock.now = () => now;
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (const [offset, id] of [A, B, C, D].entries()) {
      await files.manifest.reserveWorker({ index: offset + 1, projectId: id, title: "offline managed worker", nowMs: now });
      await files.manifest.nextLeaderGeneration(id, now);
      await files.manifest.markState(id, "ready", now);
    }
    const initial = new FakeRuntime(A);
    let deliver!: (child: RoutableSandboxRuntime) => void;
    const pendingChild = new Promise<RoutableSandboxRuntime>(resolve => { deliver = resolve; });
    let entered!: () => void;
    const factoryEntered = new Promise<void>(resolve => { entered = resolve; });
    const supervisor = new SupervisedSandboxRuntime({ projectId: A, initialChild: initial, clock,
      createChild: () => { entered(); return pendingChild; } });
    await supervisor.start();
    const peers = [B, C, D].map(id => new FakeRuntime(id));
    await Promise.all(peers.map(peer => peer.start()));
    const registry = new SandboxRegistry([supervisor, ...peers]);
    const projects = new FakeProjects([A, B, C, D].map(id => project(id)));
    let reachable = false, proofCommitted = false, proofCalls = 0, lateStops = 0;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, registry, clock, runtimeCleanupTimeoutMs: 5, randomUuid: uuidSequence(E),
      cleanupHeldBootstrap: async worker => {
        expect(worker.projectId).toBe(A);
        expect(supervisor.localCleanupComplete()).toBe(true);
        expect(supervisor.cleanupProofRequired()).toBe(true);
        proofCalls++;
        if (reachable) proofCommitted = true;
        return reachable;
      },
      finalizeHeldBootstrapCleanup: async () => {
        expect(proofCommitted).toBe(true);
        expect(registry.get(A)).toBeUndefined();
      },
    });
    const late = new FakeRuntime(A) as FakeRuntime & { localCleanupComplete(): boolean };
    late.localCleanupComplete = () => late.state === "stopped";
    late.stop = async () => { lateStops++; late.state = "stopped"; throw new Error("offline late remote cleanup unproven"); };
    try {
      await harness.value.reconcile();
      initial.state = "failed"; initial.emit();
      await factoryEntered;
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent({ ...blockedBootstrap(A, count), leaderGeneration: 1 });
      await harness.value.reconcile();
      await expect(supervisor.stop()).resolves.toBeUndefined();
      expect(supervisor.localCleanupComplete()).toBe(false);
      expect(registry.get(A)).toBe(supervisor);
      expect(harness.value.snapshot().ready).toBe(4);
      expect(proofCalls).toBe(0);
      deliver(late);
      for (let turn = 0; turn < 50; turn++) await Promise.resolve();
      expect(supervisor.localCleanupComplete()).toBe(true);
      await expect(supervisor.stop()).rejects.toThrow("cleanup was incomplete");
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(registry.get(A)).toBe(supervisor);
      expect(proofCalls).toBe(1);
      expect(projects.deleted).toEqual([]);
      reachable = true; now += 30_001;
      for (let turn = 0; turn < 4; turn++) await harness.value.reconcile();
      expect(proofCalls).toBe(2);
      expect(proofCommitted).toBe(true);
      expect(registry.get(A)).toBeUndefined();
      expect(projects.deleted).toEqual([A]);
      expect(projects.created.map(value => value.projectId)).toEqual([E]);
      expect(lateStops).toBe(1);
    } finally {
      deliver(late);
      if (registry.get(A)) await expect(harness.value.shutdown("test-complete")).rejects.toThrow("incomplete");
      else await harness.value.shutdown("test-complete");
    }
  });

  it("freezes bootstrap holds during a common-mode outage and while requests are active", async () => {
    const files = await state(), projects = new FakeProjects([]);
    let circuit = recoveryCircuit("closed", null);
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4 }), manifest: files.manifest,
      projects, randomUuid: uuidSequence(A, B, C, D, E), recoveryCircuitSnapshot: () => circuit });
    await harness.value.reconcile(); const failed = harness.factory.runtimes.get(A)!; failed.state = "recovering";
    for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(A, count));
    circuit = recoveryCircuit("open", "outage"); await harness.value.reconcile();
    expect(projects.created).toHaveLength(4);
    circuit = recoveryCircuit("closed", null);
    const snapshot = failed.snapshot.bind(failed);
    failed.snapshot = () => ({ ...snapshot(), active: 1 });
    await harness.value.reconcile(); expect(projects.created).toHaveLength(4);
    failed.snapshot = snapshot;
    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, heldProjects: [A] });
    expect(projects.deleted).toEqual([]); await harness.value.shutdown("test-complete");
  });

  it("caps retained bootstrap projects instead of growing the account fleet indefinitely", async () => {
    const files = await state(), projects = new FakeProjects([]);
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4 }), manifest: files.manifest,
      projects, randomUuid: uuidSequence(A, B, C, D, E, F, G, H), onLifecycleEvent: event => lifecycle.push(event) });
    await harness.value.reconcile();
    for (const id of [A, B, C, D]) {
      harness.factory.runtimes.get(id)!.state = "recovering";
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      await harness.value.reconcile();
    }
    expect(harness.value.snapshot().heldProjects).toEqual([A, B, C, D]);
    harness.factory.runtimes.get(E)!.state = "recovering";
    for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(E, count));
    await harness.value.reconcile(); expect(projects.created).toHaveLength(8); expect(projects.deleted).toEqual([]);
    expect(lifecycle).toContainEqual(expect.objectContaining({ type: "mutation_blocked", reason: "bootstrap_hold_limit" }));
    await harness.value.shutdown("test-complete");
  });

  it("restores seven of eight workers after an exact old hold cleanup frees the exhausted hold budget", async () => {
    const files = await state();
    const ids = Array.from({ length: 13 }, (_, index) =>
      projectId(`10000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`));
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (let index = 0; index < 12; index++) {
      await files.manifest.reserveWorker({ index: index + 1, projectId: ids[index]!, title: `fixture ${index}`, nowMs: 1 });
      await files.manifest.markCreated(ids[index]!, 2);
      if (index < 4) await files.manifest.markDraining(ids[index]!, "bootstrap_hold", 3);
    }
    const projects = new FakeProjects(ids.slice(0, 12).map(id => project(id)));
    const finalized: ProjectId[] = [];
    const proven: ProjectId[] = [];
    const clock = new StepClock();
    let proofAvailable = false;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, clock, randomUuid: uuidSequence(ids[12]!),
      cleanupHeldBootstrap: async worker => { proven.push(worker.projectId); return proofAvailable; },
      finalizeHeldBootstrapCleanup: async worker => { finalized.push(worker.projectId); },
    });
    try {
      await harness.value.reconcile();
      const failed = ids[10]!;
      harness.factory.runtimes.get(failed)!.state = "recovering";
      for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(failed, count));
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(proven).toContain(ids[0]!);
      expect(harness.value.snapshot()).toMatchObject({ ready: 7, unavailable: 1, heldProjects: ids.slice(0, 4) });
      expect(projects.created).toEqual([]);
      proofAvailable = true;
      clock.nowMs += 30_000;
      await harness.value.reconcile();
      await harness.value.reconcile();
      proofAvailable = false;
      expect(harness.value.snapshot()).toMatchObject({ ready: 8, unavailable: 0 });
      expect(projects.created.map(worker => worker.projectId)).toEqual([ids[12]!]);
      expect(harness.value.snapshot().heldProjects).toHaveLength(4);
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(finalized).toContain(ids[1]!);
      expect(projects.deleted).toEqual([ids[1]!]);
      expect(harness.factory.created.some(worker => ids.slice(0, 4).includes(worker.projectId))).toBe(false);
    } finally { await harness.value.shutdown("test-complete"); }
  });

  it("proves held cleanup with three of eight workers ready while replacement and deletion remain fenced", async () => {
    const files = await state(), clock = new BlockingClock();
    const ids = Array.from({ length: 12 }, (_, index) =>
      projectId(`20000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`));
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (let index = 0; index < ids.length; index++) {
      await files.manifest.reserveWorker({ index: index + 1, projectId: ids[index]!, title: `fixture ${index}`, nowMs: 1 });
      await files.manifest.markCreated(ids[index]!, 2);
      if (index < 4) await files.manifest.markDraining(ids[index]!, "bootstrap_hold", 3);
    }
    const failed = new Set(ids.slice(7));
    const factory = new FakeFactory(), create = factory.create.bind(factory);
    factory.create = input => {
      const runtime = create(input);
      const start = runtime.start.bind(runtime);
      runtime.start = async () => { await start(); if (failed.has(input.projectId)) runtime.state = "recovering"; };
      return runtime;
    };
    const projects = new FakeProjects(ids.map(id => project(id)));
    const proven: ProjectId[] = [], lifecycle: FleetLifecycleEvent[] = [];
    let settleProof: ((proven: boolean) => void) | undefined, finalizations = 0;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, clock, factory, onLifecycleEvent: event => lifecycle.push(event),
      cleanupHeldBootstrap: async worker => {
        proven.push(worker.projectId);
        if (worker.projectId !== ids[0]) return false;
        return new Promise<boolean>(resolve => { settleProof = resolve; });
      },
      finalizeHeldBootstrapCleanup: async () => { finalizations++; },
    });
    try {
      await harness.value.reconcile();
      expect(harness.value.snapshot()).toMatchObject({ ready: 3, unavailable: 5, heldProjects: ids.slice(0, 4) });
      expect(proven).toEqual([ids[0]!]);
      for (const id of failed) {
        for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      }
      await harness.value.reconcile();
      expect(harness.value.snapshot().heldProjects).toHaveLength(4);
      expect(projects.created).toEqual([]); expect(projects.deleted).toEqual([]);
      settleProof!(true);
      await harness.value.reconcile();
      await harness.value.reconcile();
      const manifest = await files.manifest.load();
      expect(manifest.workers.find(worker => worker.projectId === ids[0])?.retirementReason).toBe("replacement");
      expect(harness.value.snapshot()).toMatchObject({ ready: 3, unavailable: 5, heldProjects: ids.slice(1, 4) });
      expect(projects.created).toEqual([]); expect(projects.deleted).toEqual([]);
      expect(finalizations).toBe(0);
      expect(factory.created).toHaveLength(8);
      expect(lifecycle).toContainEqual(expect.objectContaining({ type: "mutation_blocked", reason: "healthy_majority" }));
      expect(lifecycle.some(event => event.type === "replacement_started")).toBe(false);
      expect(lifecycle).toContainEqual(expect.objectContaining({ type: "bootstrap_cleanup_succeeded", projectId: ids[0] }));
    } finally { settleProof?.(false); await harness.value.shutdown("test-complete"); }
  });

  it.each([false, true])("restores three of eight workers through five exact v2 grants (early cleanup and restart: %s)", async earlyCleanup => {
    const files = await state(), manifestFile = path.join(files.root, "fleet.json");
    const ids = Array.from({ length: 17 }, (_, index) =>
      projectId(`30000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`));
    const granted = ids.slice(7, 12), successors = ids.slice(12), blocked = new Set(granted);
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (let index = 0; index < 12; index++) {
      await files.manifest.reserveWorker({ index: index + 1, projectId: ids[index]!, title: `fixture ${index}`, nowMs: 1 });
      await files.manifest.markCreated(ids[index]!, 2);
      if (index < 4) await files.manifest.markDraining(ids[index]!, "bootstrap_hold", 3);
    }
    const originalJournals = new Map<ProjectId, string>();
    for (const id of granted) {
      const identity = V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY;
      const binding = { projectId: id, sandboxSessionId: "offline-old-sandbox", leaderGeneration: 1,
        buildHash: identity.buildHash, schemaHash: identity.schemaHash, nonce: "offline-old-bootstrap",
        protocolRevision: "r2" as const, schemaVersion: 2 as const };
      const sessionId = crypto.randomUUID();
      const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: {
        instanceId: "offline-old-instance", credential: { url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/",
          docId: id, token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() },
      } });
      const attempt = { version: 1 as const, phase: "submitted" as const, sessionId, binding, connectTimeoutMs: 1000, sealed };
      await new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id).withLease(undefined, entry => entry.save(attempt));
      originalJournals.set(id, await Bun.file(path.join(`${manifestFile}.bootstrap`, `${id}.json`)).text());
    }
    await prepareBootstrapHoldMigrationLedger({ manifestFile, identityFingerprint: FINGERPRINT, projectIds: granted, operatorRecovery: true });
    const migration = new FileBootstrapHoldMigration({ manifestFile, identityFingerprint: FINGERPRINT });
    const factory = new FakeFactory(), create = factory.create.bind(factory);
    let work: "active" | "quarantined" | "ambiguousQuarantined" | null = null;
    factory.create = input => {
      const runtime = create(input), start = runtime.start.bind(runtime), snapshot = runtime.snapshot.bind(runtime);
      runtime.start = async () => { await start(); if (blocked.has(input.projectId)) runtime.state = "recovering"; };
      runtime.snapshot = () => ({ ...snapshot(), ...(blocked.has(input.projectId) && work ? { [work]: 1 } : {}) });
      return runtime;
    };
    const projects = new FakeProjects(ids.slice(0, 12).map(id => project(id)));
    const clock = new StepClock();
    let circuit = recoveryCircuit("closed", null);
    let proofEnabled = false;
    const cleanupPorts = earlyCleanup ? {
      cleanupHeldBootstrap: async (worker: Readonly<import("../src/fleet/types").FleetWorkerRecord>, signal: AbortSignal) => {
        if (!proofEnabled || worker.projectId !== granted[0]) return false;
        await new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, worker.projectId).withLease(signal, async entry => {
          const current = await entry.load();
          await entry.save({ ...current!, phase: "cleanup_proven" });
        });
        return true;
      },
      finalizeHeldBootstrapCleanup: async () => { throw new Error("Project deletion must stay blocked"); },
    } : {};
    const construct = (remaining: readonly ProjectId[]) => manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, factory, clock, bootstrapMigration: migration, ...cleanupPorts,
      randomUuid: uuidSequence(...remaining), canRetireProject: () => false, recoveryCircuitSnapshot: () => circuit });
    let harness = construct(successors);
    try {
      await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 3, unavailable: 5 });
      for (const id of granted) for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      circuit = recoveryCircuit("open", "outage");
      await harness.value.reconcile();
      expect(projects.created).toHaveLength(0); expect(harness.value.snapshot().heldProjects).toHaveLength(4);
      circuit = recoveryCircuit("closed", null);
      for (const pending of ["active", "quarantined", "ambiguousQuarantined"] as const) {
        work = pending;
        await harness.value.reconcile();
        expect(projects.created).toHaveLength(0); expect(harness.value.snapshot().heldProjects).toHaveLength(4);
      }
      work = null;
      for (let replacement = 1; replacement <= 5; replacement++) {
        await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 3 + replacement, unavailable: 5 - replacement });
        if (earlyCleanup && replacement === 1) {
          // Keep unused grants fenced while the real manager commits exact cleanup of the first hold.
          work = "active"; proofEnabled = true;
          for (let rotation = 0; rotation < 6; rotation++) {
            clock.nowMs += 30_001;
            await harness.value.reconcile(); await harness.value.reconcile();
            const current = (await files.manifest.load()).workers.find(worker => worker.projectId === granted[0])!;
            if (current.retirementReason === "replacement") break;
          }
          const retired = (await files.manifest.load()).workers.find(worker => worker.projectId === granted[0])!;
          expect(retired.retirementReason).toBe("replacement");
          expect(projects.created).toHaveLength(1);
          const journal = new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, retired.projectId);
          await finalizeHeldSupervisorBootstrapCleanup({ worker: retired, assets: await loadV2SidecarAssets({ revision: "r10" }),
            bootstrapJournal: journal,
            getYSweet: async () => { throw new Error("Cleanup proof must already be durable"); },
            getArtifacts: async () => ({
              ydoc: { async readPath() { return null; }, async writePath() {}, async tombstonePath() { return false; },
                async compareAndMutatePaths() { return true; }, async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; } },
              gateway: { async syncWorkspace() {} }, async close() {},
            }),
          }, new AbortController().signal);
          await journal.withLease(undefined, async entry => { expect(await entry.load()).toBeNull(); });
          work = null; proofEnabled = false;
          await harness.value.shutdown("restart-after-early-grant-finalization");
          harness = construct(successors.slice(1));
          await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, unavailable: 4 });
        }
      }
      expect(projects.created.map(worker => worker.projectId)).toEqual(successors);
      expect(new Set(projects.created.map(worker => worker.projectId)).size).toBe(5);
      expect(harness.value.snapshot().heldProjects).toHaveLength(earlyCleanup ? 8 : 9);
      expect(projects.deleted).toEqual([]);
      expect((await files.manifest.load()).workers).toHaveLength(17);
      for (const id of granted) {
        if (earlyCleanup && id === granted[0]) expect(await Bun.file(path.join(`${manifestFile}.bootstrap`, `${id}.json`)).exists()).toBe(false);
        else expect(await Bun.file(path.join(`${manifestFile}.bootstrap`, `${id}.json`)).text()).toBe(originalJournals.get(id)!);
        expect(factory.created.filter(worker => worker.projectId === id)).toHaveLength(earlyCleanup && id !== granted[0] ? 2 : 1);
      }
      // Neither an ordinary worker nor a successor inherits a consumed operator grant.
      for (const id of [ids[4]!, successors[0]!]) {
        factory.runtimes.get(id)!.state = "recovering";
        for (let count = 1; count <= 3; count++) harness.value.observeRecoveryEvent(blockedBootstrap(id, count));
      }
      await harness.value.reconcile();
      expect(projects.created).toHaveLength(5); expect(harness.value.snapshot().heldProjects).toHaveLength(earlyCleanup ? 8 : 9);
      expect(projects.deleted).toEqual([]);
    } finally { await harness.value.shutdown("operator-recovery-complete"); }
    const restarted = manager({ config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 8 }),
      manifest: files.manifest, projects, clock, bootstrapMigration: migration,
      canRetireProject: () => false,
      randomUuid: () => { throw new Error("consumed operator grants cannot refill"); } });
    try {
      await expect(restarted.value.reconcile()).resolves.toMatchObject({ ready: 8, unavailable: 0 });
      expect(restarted.factory.created.some(worker => granted.includes(worker.projectId))).toBe(false);
      expect(projects.created).toHaveLength(5); expect(projects.deleted).toEqual([]);
      expect(restarted.value.snapshot().heldProjects).toHaveLength(earlyCleanup ? 8 : 9);
    } finally { await restarted.value.shutdown("operator-recovery-restart-complete"); }
  }, 30_000);

  it.each(["proof_returned_false", "proof_threw", "finalize_threw"] as const)("reports %s cleanup evidence without changing hold or retry policy", async cleanupStage => {
    const files = await state(), clock = new BlockingClock();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "held", nowMs: 1 });
    await files.manifest.markDraining(A, "bootstrap_hold", 2);
    const lifecycle: FleetLifecycleEvent[] = [];
    const projects = new FakeProjects([project(A)]);
    const failure = new V2SupervisorBootstrapError("PRIVATE_CLEANUP_CANARY", "deploy", {
      remoteCode: "bootstrap_cleanup_unproven", reason: "challenge",
      cause: new V2SupervisorError("PRIVATE_CLEANUP_CANARY", "timeout", undefined, "challenge"),
    });
    let proofs = 0, finalizations = 0;
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock, randomUuid: uuidSequence(B, C, D, E),
      cleanupHeldBootstrap: async () => {
        proofs++;
        if (cleanupStage === "proof_threw") throw failure;
        return cleanupStage === "finalize_threw";
      },
      finalizeHeldBootstrapCleanup: async () => { finalizations++; throw failure; },
      onLifecycleEvent: event => lifecycle.push(event),
    });
    try {
      for (let index = 0; index < 4; index++) await harness.value.reconcile();
      const reason = cleanupStage === "finalize_threw" ? "cleanup_failed" : "proof_unavailable";
      const failures = lifecycle.filter(event => event.type === "bootstrap_cleanup_deferred" && event.reason === reason);
      expect(failures).toHaveLength(1);
      expect(failures[0]).toMatchObject({ cleanupStage, reason });
      if (cleanupStage === "proof_returned_false") expect(failures[0]).not.toHaveProperty("errorName");
      else expect(failures[0]).toMatchObject({ errorName: "V2SupervisorError", bootstrapPhase: "deploy",
        bootstrapCode: "bootstrap_cleanup_unproven", bootstrapReason: "challenge", supervisorPhase: "challenge" });
      expect(JSON.stringify(failures)).not.toContain("PRIVATE");
      expect(proofs).toBe(1);
      expect(finalizations).toBe(cleanupStage === "finalize_threw" ? 1 : 0);
      expect(projects.deleted).toEqual([]);
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)?.retirementReason)
        .toBe(cleanupStage === "finalize_threw" ? "replacement" : "bootstrap_hold");
    } finally { await harness.value.shutdown("test-complete"); }
  });

  it("keeps timed out cleanup ownership bounded and rejects its late proof", async () => {
    const files = await state(), clock = new StepClock();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "held", nowMs: 1 });
    await files.manifest.markDraining(A, "bootstrap_hold", 2);
    let settle!: (value: boolean) => void;
    let calls = 0;
    let attemptSignal: AbortSignal | undefined;
    const lifecycle: FleetLifecycleEvent[] = [];
    const projects = new FakeProjects([project(A)]);
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects, clock, randomUuid: uuidSequence(B, C, D, E),
      bootstrapCleanupTimeoutMs: 100,
      cleanupHeldBootstrap: async (_worker, signal) => {
        calls++; attemptSignal = signal; return new Promise<boolean>(resolve => { settle = resolve; });
      }, finalizeHeldBootstrapCleanup: async () => {}, onLifecycleEvent: event => lifecycle.push(event) });
    try {
      await harness.value.reconcile();
      expect(calls).toBe(1);
      // The operation timer is installed before the background retry timer.
      clock.releaseNext();
      await Promise.resolve();
      expect(attemptSignal?.aborted).toBe(true);
      for (let count = 0; count < 4; count++) await harness.value.reconcile();
      expect(calls).toBe(1);
      settle(true);
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)?.retirementReason).toBe("bootstrap_hold");
      expect(projects.deleted).toEqual([]);
      expect(lifecycle).toContainEqual(expect.objectContaining({ type: "bootstrap_cleanup_deferred", reason: "timeout" }));
    } finally { settle(false); await harness.value.shutdown("test-complete"); }
  });

  it("fences shutdown and changed held records before committing delayed cleanup proof", async () => {
    const files = await state(), clock = new BlockingClock();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "held", nowMs: 1 });
    await files.manifest.markDraining(A, "bootstrap_hold", 2);
    let settle!: (value: boolean) => void;
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects: new FakeProjects([project(A)]), clock, randomUuid: uuidSequence(B, C, D, E),
      cleanupHeldBootstrap: async () => new Promise<boolean>(resolve => { settle = resolve; }),
      finalizeHeldBootstrapCleanup: async () => {}, onLifecycleEvent: event => lifecycle.push(event) });
    try {
      await harness.value.reconcile();
      await files.manifest.markDraining(A, "bootstrap_hold", 3);
      settle(true);
      await harness.value.reconcile();
      await harness.value.reconcile();
      expect(lifecycle).toContainEqual(expect.objectContaining({ type: "bootstrap_cleanup_deferred", reason: "worker_changed" }));
      const closing = harness.value.shutdown("test-complete");
      settle(true);
      await closing;
      expect((await files.manifest.load()).workers.find(worker => worker.projectId === A)?.retirementReason).toBe("bootstrap_hold");
    } finally { settle(false); await harness.value.shutdown("test-complete"); }
  });

  it("requires closed circuits before held cleanup", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "held", nowMs: 1 });
    await files.manifest.markDraining(A, "bootstrap_hold", 2);
    let calls = 0;
    let circuit = recoveryCircuit("open", "outage");
    const harness = manager({ config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest, projects: new FakeProjects([project(A)]), clock: new BlockingClock(), randomUuid: uuidSequence(B, C, D, E),
      recoveryCircuitSnapshot: () => circuit,
      cleanupHeldBootstrap: async () => { calls++; return false; }, finalizeHeldBootstrapCleanup: async () => {} });
    try {
      await harness.value.reconcile();
      expect(calls).toBe(0);
      circuit = recoveryCircuit("closed", null);
      await harness.value.reconcile();
      expect(calls).toBe(1);
    } finally { await harness.value.shutdown("test-complete"); }
  });

  it("fences replacement while a continuation binding or fleet outage exists", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    let continuationBound = true;
    let circuit = recoveryCircuit("open", "outage");
    const lifecycle: FleetLifecycleEvent[] = [];
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(A, B, C, D, E),
      canRetireProject: id => id !== A || !continuationBound,
      recoveryCircuitSnapshot: () => circuit,
      onLifecycleEvent: event => lifecycle.push(event),
    });

    // The open common-mode circuit permits local intent persistence but no
    // remote create storm.
    const first = await harness.value.reconcile();
    expect(first.projectMutation).toMatchObject({ state: "open" });
    expect(projects.created).toEqual([]);

    circuit = recoveryCircuit("closed", null);
    await harness.value.reconcile();
    const failed = harness.factory.runtimes.get(A)!;
    failed.state = "recovering";
    for (let count = 1; count <= 3; count++) {
      harness.value.observeRecoveryEvent(recoveryRetry(A, count));
    }
    await harness.value.reconcile();
    expect(projects.created).toHaveLength(4);
    expect(projects.deleted).toEqual([]);
    expect(lifecycle).toContainEqual(expect.objectContaining({
      type: "mutation_blocked",
      projectId: A,
      operation: "replace",
      reason: "continuation_binding",
    }));

    continuationBound = false;
    await harness.value.reconcile();
    expect(projects.created).toHaveLength(5);
    expect(projects.deleted).toEqual([A]);
    await harness.value.shutdown("test-complete");
  });

  it("opens a local project-mutation circuit and suppresses create storms", async () => {
    const files = await state();
    const projects = new FailingCreateProjects([]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 8, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(A, B, C, D, E, F, G, H),
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({
      desired: 8,
      ready: 0,
      unavailable: 8,
      projectMutation: {
        state: "open",
        consecutiveFailures: 1,
      },
    });
    expect(projects.createCalls).toBe(4);
    await harness.value.reconcile();
    expect(projects.createCalls).toBe(4);
    expect((await files.manifest.load()).workers).toHaveLength(8);
    await harness.value.shutdown("test-complete");
  });

  it("does not replace a whole persisted fleet when project metadata all returns 404", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (const [index, id] of [A, B, C, D].entries()) {
      await files.manifest.reserveWorker({
        index: index + 1,
        projectId: id,
        title: `GhostProv Worker ${index + 1}`,
        nowMs: index + 1,
      });
      await files.manifest.markState(id, "ready", index + 10);
    }
    const projects = new FakeProjects([]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(E),
      canRetireProject: () => true,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 0, unavailable: 4 });
    expect(projects.created).toEqual([]);
    expect((await files.manifest.load()).workers.every(worker => worker.state !== "draining"))
      .toBe(true);

    projects.remote.push(project(B), project(C), project(D));
    await harness.value.reconcile();
    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4, unavailable: 0 });
    expect(projects.created.map(value => value.projectId)).toEqual([E]);
    expect(projects.deleted).toEqual([A]);
    await harness.value.shutdown("test-complete");
  });

  it("resumes a crash-interrupted replacement and deletes only its managed old project", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    for (const [index, id] of [A, B, C, D, E].entries()) {
      await files.manifest.reserveWorker({
        index: index + 1,
        projectId: id,
        title: `GhostProv Worker ${index + 1}`,
        nowMs: index + 1,
      });
      await files.manifest.markCreated(id, index + 10);
    }
    await files.manifest.markDraining(A, "replacement", 100);
    const projects = new FakeProjects([A, B, C, D, E].map(id => project(id)));
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 4, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: () => { throw new Error("persisted replacement must not allocate"); },
      canRetireProject: () => true,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 4 });
    expect(projects.deleted).toEqual([A]);
    expect((await files.manifest.load()).workers.map(worker => worker.projectId).sort())
      .toEqual([B, C, D, E].sort());
    await harness.value.shutdown("test-complete");
  });

  it("cleans an automatic replacement even when older manual surplus records exist", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.completeLegacyMigration();
    await files.manifest.reserveWorker({
      index: 4,
      projectId: D,
      title: "manual surplus",
      nowMs: 1,
      managed: false,
    });
    await files.manifest.markCreated(D, 2);
    await files.manifest.markDraining(D, "surplus", 3);
    for (const [index, id] of [A, B, C].entries()) {
      await files.manifest.reserveWorker({
        index: index + 1,
        projectId: id,
        title: `worker ${index + 1}`,
        nowMs: 10 + index,
      });
      await files.manifest.markCreated(id, 20 + index);
    }
    await files.manifest.markDraining(A, "replacement", 30);
    const projects = new FakeProjects([A, B, C, D].map(id => project(id)));
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 2 }),
      manifest: files.manifest,
      projects,
      canRetireProject: () => true,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 2 });
    expect(projects.deleted).toEqual([A]);
    const remaining = await files.manifest.load();
    expect(remaining.workers.find(worker => worker.projectId === D)).toMatchObject({
      state: "draining",
      retirementReason: "surplus",
    });
    expect(remaining.workers.some(worker => worker.projectId === A)).toBe(false);
    await harness.value.shutdown("test-complete");
  });

  it("can replace every failed minority without deadlocking behind the first successor", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    const clock = new BlockingClock();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [], target: 5, provisionConcurrency: 4 }),
      manifest: files.manifest,
      projects,
      randomUuid: uuidSequence(A, B, C, D, E, F, G),
      canRetireProject: () => true,
      clock,
    });
    await harness.value.reconcile();
    harness.factory.runtimes.get(A)!.state = "recovering";
    harness.factory.runtimes.get(B)!.state = "recovering";
    for (let count = 1; count <= 3; count++) {
      harness.value.observeRecoveryEvent(recoveryRetry(A, count));
      harness.value.observeRecoveryEvent(recoveryRetry(B, count));
    }

    await harness.value.reconcile();
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C, D, E, F]);
    await harness.value.reconcile();
    expect(projects.created.map(value => value.projectId)).toEqual([A, B, C, D, E, F, G]);
    expect(harness.value.snapshot()).toMatchObject({ desired: 5, ready: 5 });
    await harness.value.shutdown("test-complete");
  });

  it("keeps fresh worker indexes in configured order when metadata completes in reverse", async () => {
    const files = await state();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B], provisionConcurrency: 2 }),
      manifest: files.manifest,
      projects: new ReverseMetadataProjects([project(A), project(B)]),
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 2 });
    const manifest = await files.manifest.load();
    expect(manifest.workers.map(worker => ({ index: worker.index, projectId: worker.projectId })))
      .toEqual([
        { index: 1, projectId: A },
        { index: 2, projectId: B },
      ]);
    expect(harness.factory.created.map(value => ({
      index: value.workerIndex,
      projectId: value.projectId,
    })).sort((left, right) => left.index - right.index)).toEqual([
      { index: 1, projectId: A },
      { index: 2, projectId: B },
    ]);
    await harness.value.shutdown("test-complete");
  });

  it("replaces missing legacy migration seeds without deleting user projects", async () => {
    const files = await state();
    const projects = new FakeProjects([project(B, "Unlisted project")]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, C] }),
      manifest: files.manifest,
      projects,
    });

    const snapshot = await harness.value.reconcile();
    expect(snapshot).toMatchObject({
      desired: 2,
      ready: 2,
      unavailable: 0,
    });
    expect(projects.created).toHaveLength(2);
    expect(harness.factory.created.map(value => value.projectId).sort())
      .toEqual(projects.created.map(value => value.projectId).sort());
    expect(projects.deleted).toEqual([]);
    expect((await files.manifest.load()).legacyMigrationCompleted).toBe(true);
    await harness.value.shutdown("test-complete");
  });

  it("keeps an available legacy project while filling a missing slot", async () => {
    const files = await state();
    const projects = new FakeProjects([project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects,
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot).toMatchObject({
      desired: 2,
      ready: 2,
      failed: 0,
      unavailable: 0,
    });
    expect(harness.factory.created.map(value => value.projectId)).toContain(B);
    expect(projects.created).toHaveLength(1);
    expect(projects.deleted).toEqual([]);
    await harness.value.shutdown("test-complete");
  });

  it("reports late supervisor health transitions without another reconciliation", async () => {
    const files = await state();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A), project(B)]),
    });
    await expect(harness.value.reconcile()).resolves.toMatchObject({ ready: 2 });

    const recovering = harness.factory.runtimes.get(B)!;
    recovering.state = "recovering";
    expect(harness.value.snapshot()).toMatchObject({
      desired: 2,
      ready: 1,
      provisioning: 1,
      unavailable: 1,
      unavailableProjects: [B],
    });

    // A supervised runtime may recover entirely in the background after the
    // manager's original reconcile promise has settled.
    recovering.state = "ready";
    recovering.heartbeatAgeMs = 0;
    expect(harness.value.snapshot()).toMatchObject({
      desired: 2,
      ready: 2,
      provisioning: 0,
      unavailable: 0,
    });
    await harness.value.shutdown("test-complete");
  });

  it("rejects deleted and wrong-project metadata and creates a managed substitute", async () => {
    for (const invalid of [
      { ...project(A), deleted: true },
      project(C, "Wrong returned project"),
    ]) {
      const files = await state();
      const projects = new InvalidMetadataProjects([project(B)], A, invalid);
      const harness = manager({
        config: config(files.cookieFile, { explicit: [A, B] }),
        manifest: files.manifest,
        projects,
      });

      await expect(harness.value.reconcile()).resolves.toMatchObject({
        ready: 2,
        unavailable: 0,
      });
      expect(harness.registry.readyRuntimes().map(runtime => runtime.projectId)).toContain(B);
      expect(harness.registry.get(C)).toBeUndefined();
      expect(projects.created).toHaveLength(1);
      expect(projects.deleted).toEqual([]);
      await harness.value.shutdown("test-complete");
    }
  });

  it("fences a runtime factory that returns a different project ID", async () => {
    const files = await state();
    const factory = new WrongProjectFactory();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A), project(B)]),
      factory,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({
      ready: 1,
      unavailable: 1,
      unavailableProjects: [A],
    });
    expect(harness.registry.readyRuntimes().map(runtime => runtime.projectId)).toEqual([B]);
    expect(harness.registry.get(C)).toBeUndefined();
    expect(factory.runtimes.get(C)?.state).toBe("stopped");
    await harness.value.shutdown("test-complete");
  });

  it("registers a healthy project before another metadata read settles", async () => {
    const files = await state();
    const projects = new DeferredMissingProjects([project(B)], A);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B], provisionConcurrency: 1 }),
      manifest: files.manifest,
      projects,
    });

    const healthyReady = waitForRegistryReady(harness.registry, 1);
    const reconciling = harness.value.reconcile();
    try {
      await projects.missingStarted;
      await healthyReady;
      expect(harness.registry.readyRuntimes().map(runtime => runtime.projectId)).toEqual([B]);
    } finally {
      projects.releaseMissing();
    }
    await expect(reconciling).resolves.toMatchObject({ ready: 2, unavailable: 0 });
    await harness.value.shutdown("test-complete");
  });

  it("never exceeds provisioning concurrency while metadata completes independently", async () => {
    const files = await state();
    const factory = new TrackingStartFactory();
    const harness = manager({
      config: config(files.cookieFile, {
        explicit: [A, B, C],
        provisionConcurrency: 1,
      }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A), project(B), project(C)]),
      factory,
    });

    const reconciling = harness.value.reconcile();
    await waitForCondition(() => factory.started.length === 1, "first runtime start");
    expect(factory.peak).toBe(1);
    factory.release(factory.started[0]);
    await waitForCondition(() => factory.started.length === 2, "second runtime start");
    expect(factory.peak).toBe(1);
    factory.release(factory.started[1]);
    await waitForCondition(() => factory.started.length === 3, "third runtime start");
    expect(factory.peak).toBe(1);
    factory.release(factory.started[2]);

    await expect(reconciling).resolves.toMatchObject({ ready: 3 });
    expect(factory.peak).toBe(1);
    await harness.value.shutdown("test-complete");
  });

  it("aborts an isolated in-flight metadata read during shutdown", async () => {
    const files = await state();
    const projects = new DeferredMissingProjects([project(B)], A);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects,
    });

    const healthyReady = waitForRegistryReady(harness.registry, 1);
    const reconciling = harness.value.reconcile().catch(error => error);
    await projects.missingStarted;
    await healthyReady;
    expect(harness.registry.readyRuntimes()).toHaveLength(1);

    await harness.value.shutdown("test-shutdown");

    expect(await reconciling).toMatchObject({ name: "AbortError" });
    expect(harness.registry.runtimes()).toEqual([]);
  });

  it("interrupts a hung runtime start and completes shutdown", async () => {
    const files = await state();
    const factory = new BlockingStartFactory();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A), project(B)]),
      factory,
    });
    const healthyReady = waitForRegistryReady(harness.registry, 1);
    const reconciling = harness.value.reconcile().catch(error => error);
    await Promise.all([factory.blocked.started, healthyReady]);

    await expect(Promise.race([
      harness.value.shutdown("test-shutdown"),
      Bun.sleep(2_000).then(() => { throw new Error("fleet shutdown hung on runtime.start"); }),
    ])).resolves.toBeUndefined();

    expect(await reconciling).toMatchObject({ name: "AbortError" });
    expect(factory.blocked.state).toBe("stopped");
    expect(harness.registry.runtimes()).toEqual([]);
  });

  it("never lets late old-start cleanup remove a newer exact generation", async () => {
    const files = await state();
    const factory = new LateThenHealthyFactory();
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A), project(B)]),
      factory,
    });
    const controller = new AbortController();
    const first = harness.value.reconcile(controller.signal).catch(error => error);
    await Promise.all([factory.old.started, waitForRegistryReady(harness.registry, 1)]);
    controller.abort();
    expect(await first).toMatchObject({ name: "AbortError" });
    expect(harness.registry.get(A)).toBeUndefined();

    await harness.value.reconcile();
    const replacement = harness.registry.get(A);
    expect(replacement).toBeDefined();
    expect(replacement).not.toBe(factory.old);
    factory.old.releaseLateStart();
    await Bun.sleep(0);
    await Bun.sleep(0);

    expect(harness.registry.get(A)).toBe(replacement);
    await harness.value.shutdown("test-complete");
  });

  it("recovers unavailable metadata in the background without another caller reconcile", async () => {
    const files = await state();
    const projects = new TransportMissingProjects([project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects,
      retryInitialDelayMs: 1,
      retryMaxDelayMs: 8,
    });
    await harness.value.reconcile();

    projects.remote.push(project(A));
    const recovered = harness.value.waitForReady(2);
    await expect(Promise.race([
      recovered,
      Bun.sleep(2_000).then(() => { throw new Error("metadata retry did not recover"); }),
    ])).resolves.toMatchObject({
      ready: 2,
      failed: 0,
      unavailable: 0,
      unavailableProjects: [],
    });

    expect(projects.getCalls.filter(id => id === A).length).toBeGreaterThanOrEqual(2);
    expect(projects.getCalls.filter(id => id === B)).toHaveLength(1);
    expect(harness.factory.created.map(value => value.projectId).sort()).toEqual([A, B].sort());
    expect(projects.created).toEqual([]);
    await harness.value.shutdown("test-complete");
  });

  it("times out one hung metadata read while other missing projects keep recovering", async () => {
    const files = await state();
    const projects = new SelectivelyHungProjects([project(C)], B);
    const harness = manager({
      config: config(files.cookieFile, {
        explicit: [A, B, C],
        provisionConcurrency: 1,
      }),
      manifest: files.manifest,
      projects,
      retryInitialDelayMs: 1,
      retryMaxDelayMs: 8,
      metadataAttemptTimeoutMs: 20,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({
      ready: 1,
      unavailable: 2,
    });
    expect(harness.registry.readyRuntimes().map(runtime => runtime.projectId)).toEqual([C]);
    expect(projects.created).toEqual([]);

    await expect(Promise.race([
      harness.value.waitForReady(2),
      Bun.sleep(2_000).then(() => { throw new Error("independent metadata recovery stalled"); }),
    ])).resolves.toMatchObject({ ready: 2, unavailable: 1 });
    expect(harness.registry.get(B)).toBeUndefined();
    expect(projects.created).toHaveLength(1);
    await harness.value.shutdown("test-complete");
  });

  it("uses one jittered bounded retry loop and resets backoff after progress", async () => {
    const files = await state();
    const clock = new StepClock();
    const projects = new TransportMissingProjects([project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B, C] }),
      manifest: files.manifest,
      projects,
      clock,
      retryInitialDelayMs: 10,
      retryMaxDelayMs: 25,
      random: () => 1,
    });
    await harness.value.reconcile();
    await waitForCondition(() => clock.delays.length === 1, "first metadata retry");
    expect(clock.delays).toEqual([12]);
    expect(clock.maxPending).toBe(1);

    clock.releaseNext();
    await waitForCondition(() => clock.delays.length === 2, "second metadata retry");
    expect(clock.delays).toEqual([12, 24]);
    expect(clock.maxPending).toBe(1);

    clock.releaseNext();
    await waitForCondition(() => clock.delays.length === 3, "bounded metadata retry");
    expect(clock.delays).toEqual([12, 24, 40]);
    expect(clock.maxPending).toBe(1);

    projects.remote.push(project(A));
    clock.releaseNext();
    await waitForCondition(
      () => harness.registry.readyRuntimes().length === 2 && clock.delays.length === 4,
      "partial retry progress",
    );
    expect(clock.delays).toEqual([12, 24, 40, 12]);
    expect(clock.maxPending).toBe(1);
    expect(harness.value.snapshot()).toMatchObject({ ready: 2, unavailable: 1 });
    expect(projects.getCalls.filter(id => id === B)).toHaveLength(1);

    await harness.value.shutdown("test-complete");
  });

  it("aborts the single metadata retry task during shutdown", async () => {
    const files = await state();
    const clock = new BlockingClock();
    const projects = new TransportMissingProjects([project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B] }),
      manifest: files.manifest,
      projects,
      clock,
      retryInitialDelayMs: 10,
      retryMaxDelayMs: 100,
    });
    await harness.value.reconcile();
    for (let turn = 0; clock.sleeps === 0 && turn < 10; turn++) await Promise.resolve();
    expect(clock.sleeps).toBe(1);
    const callsBeforeShutdown = [...projects.getCalls];

    await harness.value.shutdown("test-shutdown");

    expect(clock.aborts).toBe(1);
    expect(projects.getCalls).toEqual(callsBeforeShutdown);
    expect(harness.registry.runtimes()).toEqual([]);
    expect(harness.value.snapshot()).toMatchObject({ desired: 0, unavailable: 0 });
  });

  it("routes only exact configured projects and ignores unlisted manifest workers", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 1 });
    await files.manifest.markCreated(B, 2);
    const projects = new FakeProjects([
      project(A, "User explicit project"),
      project(B, "GhostProv Worker 2"),
      project(C, "GhostProv Worker 3"),
    ]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects,
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.projects).toEqual([A]);
    expect(projects.created).toEqual([]);
    expect(harness.registry.runtimes()).toHaveLength(1);
    const records = (await files.manifest.load()).workers;
    expect(records.find(worker => worker.projectId === A)?.managed).toBe(false);
    expect(harness.factory.created.map(value => value.projectId)).toEqual([A]);
    expect(projects.getCalls).toEqual([A]);
  });

  it("is restart-idempotent after a persisted creating intent", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
    const projects = new FakeProjects([project(A, "GhostProv Worker 1")]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects,
    });

    await harness.value.reconcile();

    expect(projects.created).toEqual([]);
    expect((await files.manifest.load()).workers[0].state).toBe("ready");
  });

  it("keeps the manifest authoritative after a later environment seed changes", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({
      index: 1,
      projectId: A,
      title: "User explicit project",
      nowMs: 1,
      managed: false,
    });
    await files.manifest.markCreated(A, 2);
    const projects = new FakeProjects([
      project(A, "User explicit project"),
      project(B, "Replacement explicit project"),
    ]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [B] }),
      manifest: files.manifest,
      projects,
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.projects).toEqual([A]);
    expect(projects.created).toEqual([]);
  });

  it("never auto-deletes healthy surplus capacity without manual decommission", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
    await files.manifest.markCreated(A, 2);
    await files.manifest.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 3 });
    await files.manifest.markCreated(B, 4);
    const projects = new FakeProjects([project(A), project(B)]);
    const left = new FakeRuntime(A);
    const right = new FakeRuntime(B);
    await left.start();
    await right.start();
    const registry = new SandboxRegistry([left, right]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects,
      registry,
    });

    await harness.value.reconcile();

    expect(registry.get(A)).toBe(left);
    expect(registry.get(B)).toBe(right);
    expect(right.drains).toBe(0);
    expect(projects.deleted).toEqual([]);
    await harness.value.shutdown("test-complete");
  });

  it("force-stops an excess runtime before publishing configured readiness", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({
      index: 1,
      projectId: A,
      title: "Current worker",
      nowMs: 1,
      managed: false,
    });
    await files.manifest.markCreated(A, 2);
    const excess = new FailingDrainRuntime(C);
    await excess.start();
    const registry = new SandboxRegistry([excess]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      registry,
    });

    const snapshot = await harness.value.reconcile();

    expect(excess.drains).toBe(1);
    expect(excess.state).toBe("stopped");
    expect(registry.get(C)).toBeUndefined();
    expect(snapshot).toMatchObject({ ready: 1, unavailable: 0, projects: [A] });
    expect(registry.readyRuntimes().map(runtime => runtime.projectId)).toEqual([A]);
    await harness.value.shutdown("test-complete");
  });

  it("reconciles only explicitly configured guest projects", async () => {
    const files = await state();
    const projects = new FakeProjects([project(A), project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A], sessionMode: "auto", guestMax: 1 }),
      identityKind: "anonymous",
      manifest: files.manifest,
      projects,
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.desired).toBe(1);
    expect(projects.getCalls).toEqual([A]);
    expect(harness.registry.runtimes()).toHaveLength(1);
  });

  it("caps an auto-resolved anonymous fleet without creating excess projects", async () => {
    const files = await state();
    const projects = new FakeProjects([project(A), project(B)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A, B], sessionMode: "auto", guestMax: 1 }),
      identityKind: "anonymous",
      manifest: files.manifest,
      projects,
    });

    await expect(harness.value.reconcile()).resolves.toMatchObject({ desired: 1, ready: 1 });
    expect(projects.getCalls).toEqual([A]);
    expect(harness.factory.created.map(value => value.projectId)).toEqual([A]);
  });

  it("rejects reconciliation after shutdown without creating projects", async () => {
    const files = await state();
    const projects = new FakeProjects([project(A)]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects,
    });

    await harness.value.shutdown("test-shutdown");

    await expect(harness.value.reconcile()).rejects.toThrow("shutting down");
    expect(projects.created).toEqual([]);
    expect(harness.registry.runtimes()).toEqual([]);
  });

  it("reports an incomplete shutdown and retains truthful runtime state", async () => {
    const files = await state();
    const runtime = new UnstoppableRuntime(A);
    await runtime.start();
    const registry = new SandboxRegistry([runtime]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      registry,
    });

    const failure = await harness.value.shutdown("test-shutdown").catch(error => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure.message).toBe(
      `Fleet shutdown was incomplete (failed runtimes: 1; remaining runtimes: 1; projects: ${A})`,
    );
    expect(failure.message).not.toContain("forced stop failed");

    expect(registry.get(A)).toBe(runtime);
    expect(harness.value.snapshot()).toMatchObject({ desired: 1, ready: 1 });
  });

  it("bounds a non-cooperative graceful drain before forcing shutdown", async () => {
    const files = await state();
    const runtime = new HungDrainRuntime(A);
    await runtime.start();
    const registry = new SandboxRegistry([runtime]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      registry,
      runtimeCleanupTimeoutMs: 10,
    });

    await expect(Promise.race([
      harness.value.shutdown("test-shutdown"),
      Bun.sleep(2_000).then(() => { throw new Error("fleet shutdown hung on drain"); }),
    ])).resolves.toBeUndefined();

    expect(runtime.drains).toBe(1);
    expect(runtime.state).toBe("stopped");
    expect(registry.runtimes()).toEqual([]);
    expect(harness.value.snapshot()).toMatchObject({ desired: 0, ready: 0 });
  });

  it("does not report ready until a runtime has a successful fresh heartbeat", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
    await files.manifest.markCreated(A, 2);
    const runtime = new FakeRuntime(A);
    await runtime.start();
    runtime.heartbeatAgeMs = null;
    const registry = new SandboxRegistry([runtime]);
    const harness = manager({
      config: config(files.cookieFile, { explicit: [A] }),
      manifest: files.manifest,
      projects: new FakeProjects([project(A)]),
      registry,
    });

    const snapshot = await harness.value.reconcile();
    expect(snapshot.ready).toBe(0);
    const waiting = harness.value.waitForReady(1);
    runtime.heartbeatAgeMs = 0;
    runtime.emit();
    await expect(waiting).resolves.toMatchObject({ ready: 1 });
    await harness.value.shutdown("test-complete");
  });
});
