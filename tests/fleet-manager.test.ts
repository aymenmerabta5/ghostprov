import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import { DEFAULT_V2_SIDECAR_REVISION } from "../src/v2/sidecar/assets";
import type { Clock, MachineSample } from "../src/core/ports";
import { ProjectFleetManager } from "../src/fleet/manager";
import { FleetManifestStore } from "../src/fleet/manifest";
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
import type { SandboxSupervisorEvent } from "../src/sandbox/supervised-runtime";

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

    await expect(harness.value.shutdown("test-shutdown"))
      .rejects.toThrow("Fleet shutdown was incomplete");

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
