import { createHash, randomUUID } from "node:crypto";
import type { MultiSandboxConfig } from "../config/multi-sandbox";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { FleetRecoverySnapshot } from "../prism/recovery-coordinator";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { PrismProjectSummary } from "../prism/projects-client";
import { isRuntimeHealthy } from "../router/policy";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../sandbox/registry";
import type { SandboxSupervisorEvent } from "../sandbox/supervised-runtime";
import { projectId, type ProjectId } from "../sandbox/types";
import { FleetManifestStore } from "./manifest";
import type {
  FleetLifecycleEvent,
  FleetManifest,
  FleetMutationBlockReason,
  FleetMutationOperation,
  FleetReplacementEvidence,
  FleetReplacementFailureKind,
  FleetSnapshot,
  FleetWorkerRecord,
  ManagedRuntimeFactory,
} from "./types";

export interface FleetProjectsClient {
  get(id: ProjectId, signal?: AbortSignal): Promise<PrismProjectSummary>;
  create?(
    input: { projectId: ProjectId; title: string },
    signal?: AbortSignal,
  ): Promise<PrismProjectSummary>;
  deleteManaged?(id: ProjectId, signal?: AbortSignal): Promise<void>;
}

export interface FleetIdentity {
  kind: PrismIdentityKind;
  fingerprint: string;
}

export type FleetReconciliationErrorKind = "insufficient_projects" | "runtime";

export class FleetReconciliationError extends Error {
  readonly name = "FleetReconciliationError";

  constructor(
    message: string,
    readonly kind: FleetReconciliationErrorKind,
  ) {
    super(message);
  }
}

interface ControlPlaneEvidence {
  successes: number;
  globalFailures: number;
  freezeMutations: boolean;
  managedMissingClaimed: boolean;
}

const DEFAULT_RETRY_INITIAL_DELAY_MS = 500;
const DEFAULT_RETRY_MAX_DELAY_MS = 30_000;
const DEFAULT_METADATA_ATTEMPT_TIMEOUT_MS = 15_000;
const DEFAULT_MUTATION_MAX_BACKOFF_MS = 300_000;
const RETRY_JITTER_RATIO = 0.2;
const REPLACEABLE_BOOTSTRAP_KINDS = new Set([
  "hello",
  "readiness",
  "render",
  "timeout",
]);

function emptySnapshot(): FleetSnapshot {
  return {
    desired: 0,
    ready: 0,
    provisioning: 0,
    draining: 0,
    failed: 0,
    unavailable: 0,
    unavailableProjects: Object.freeze([]),
    projects: Object.freeze([]),
    pendingRetirements: 0,
    projectMutation: Object.freeze({
      state: "closed",
      consecutiveFailures: 0,
      retryInMs: null,
    }),
  };
}

function nextWorkerIndex(workers: readonly FleetWorkerRecord[]): number {
  const used = new Set(workers.map(worker => worker.index));
  for (let index = 1; index <= Number.MAX_SAFE_INTEGER; index++) {
    if (!used.has(index)) return index;
  }
  throw new FleetReconciliationError("No stable worker index is available", "runtime");
}

function concurrencyGate(
  limit: number,
): <T>(operation: () => Promise<T>) => Promise<T> {
  let active = 0;
  const waiting: Array<() => void> = [];
  return async <T>(operation: () => Promise<T>): Promise<T> => {
    if (active < limit) active++;
    else await new Promise<void>(resolve => waiting.push(resolve));
    try {
      return await operation();
    } finally {
      const next = waiting.shift();
      if (next) next();
      else active--;
    }
  };
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function statusOf(error: unknown): number | null {
  const status = record(error)?.status;
  return typeof status === "number" && Number.isInteger(status) && status >= 100 && status <= 599
    ? status
    : null;
}

function isAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError" ||
    record(error)?.name === "AbortError";
}

function isDefinitiveMissingProject(error: unknown): boolean {
  const status = statusOf(error);
  return status === 404 || status === 410;
}

function isGlobalControlPlaneFailure(error: unknown): boolean {
  if (isAbort(error) || isDefinitiveMissingProject(error)) return false;
  const value = record(error);
  const status = statusOf(error);
  if (status === 401 || status === 403 || status === 429 || (status !== null && status >= 500)) {
    return true;
  }
  if (status !== null) return false;
  const name = typeof value?.name === "string" ? value.name.toLowerCase() : "";
  const kind = typeof value?.kind === "string" ? value.kind.toLowerCase() : "";
  return error instanceof TypeError ||
    name.includes("transport") ||
    name.includes("timeout") ||
    name.includes("fetch") ||
    kind.includes("transport") ||
    kind.includes("timeout") ||
    kind === "auth" ||
    kind === "rate_limit";
}

function validProjectSummary(expected: ProjectId, value: PrismProjectSummary): boolean {
  return value.projectId === expected && value.deleted === false;
}

function replacementFailureSignature(
  failure: SandboxSupervisorEvent["failure"],
): { signatureHash: string; kind: FleetReplacementFailureKind } | null {
  if (!failure || failure.name !== "V2BootstrapError") return null;
  if (!failure.kind || !REPLACEABLE_BOOTSTRAP_KINDS.has(failure.kind)) return null;
  if (failure.operation !== "bootstrap" && failure.operation !== "render") return null;
  const signature = [
    failure.name,
    failure.kind,
    failure.status ?? "",
    failure.operation,
    failure.sidecarErrorName ?? "",
    failure.sidecarErrorCode ?? "",
    failure.sidecarCauseName ?? "",
    failure.sidecarPhase ?? "",
    failure.sidecarPermission ?? "",
  ].join("|");
  return Object.freeze({
    signatureHash: createHash("sha256").update(signature).digest("hex"),
    kind: failure.kind as FleetReplacementFailureKind,
  });
}

function missingSnapshot(project: ProjectId): RoutableSandboxSnapshot {
  return {
    projectId: project,
    state: "missing",
    capacity: 0,
    active: 0,
    available: 0,
    quarantined: 0,
    heartbeatAgeMs: null,
    credentialExpiresInMs: null,
    ackEwmaMs: 0,
    completionEwmaMs: 0,
    recentFailureRate: 0,
    queueDepth: 0,
    memoryPressure: 0,
    failure: null,
  };
}

export interface ProjectFleetManagerOptions {
  config: MultiSandboxConfig;
  identity: FleetIdentity;
  projects: FleetProjectsClient;
  manifest: FleetManifestStore;
  registry: SandboxRegistry;
  runtimeFactory: ManagedRuntimeFactory;
  clock?: Clock;
  retryInitialDelayMs?: number;
  retryMaxDelayMs?: number;
  random?: () => number;
  randomUuid?: () => string;
  metadataAttemptTimeoutMs?: number;
  runtimeCleanupTimeoutMs?: number;
  recoveryCircuitSnapshot?: () => Readonly<FleetRecoverySnapshot>;
  canRetireProject?: (projectId: ProjectId) => boolean;
  onLifecycleEvent?: (event: FleetLifecycleEvent) => void;
}

/**
 * Identity-scoped managed Prism fleet.
 *
 * Environment IDs are copied once into the manifest as unmanaged migration
 * records. Every subsequent selection comes exclusively from that manifest.
 * Managed create intent is durable before the remote POST, and managed removal
 * happens only after a replacement is healthy, the old runtime has drained,
 * continuation routing hints are gone, and the remote DELETE succeeds.
 */
export class ProjectFleetManager {
  readonly #config: MultiSandboxConfig;
  readonly #desiredSandboxes: number;
  readonly #identity: FleetIdentity;
  readonly #projects: FleetProjectsClient;
  readonly #manifest: FleetManifestStore;
  readonly #registry: SandboxRegistry;
  readonly #runtimeFactory: ManagedRuntimeFactory;
  readonly #clock: Clock;
  readonly #retryInitialDelayMs: number;
  readonly #retryMaxDelayMs: number;
  readonly #random: () => number;
  readonly #randomUuid: () => string;
  readonly #metadataAttemptTimeoutMs: number;
  readonly #runtimeCleanupTimeoutMs: number;
  readonly #recoveryCircuitSnapshot?: () => Readonly<FleetRecoverySnapshot>;
  readonly #canRetireProject: (projectId: ProjectId) => boolean;
  readonly #onLifecycleEvent?: (event: FleetLifecycleEvent) => void;
  readonly #unavailableProjects = new Set<ProjectId>();
  readonly #replacementCandidates = new Set<ProjectId>();
  readonly #replacementSuccessors = new Map<ProjectId, { created: boolean }>();
  readonly #mutationBlocks = new Map<string, FleetMutationBlockReason>();
  #selectedProjects: readonly ProjectId[] = Object.freeze([]);
  #pendingRetirements = 0;
  #pendingMaintenance = false;
  #mutationFailureCount = 0;
  #mutationsFrozenUntilMs: number | null = null;
  #snapshot: FleetSnapshot = emptySnapshot();
  #chain: Promise<void> = Promise.resolve();
  #activeReconcile: AbortController | null = null;
  #retryController: AbortController | null = null;
  #retryTask: Promise<void> | null = null;
  #retryAttempt = 0;
  #shuttingDown = false;

  constructor(options: ProjectFleetManagerOptions) {
    this.#config = options.config;
    this.#identity = options.identity;
    this.#desiredSandboxes = options.identity.kind === "anonymous"
      ? Math.min(options.config.desiredSandboxes, options.config.guestMaxProjects)
      : options.config.desiredSandboxes;
    this.#projects = options.projects;
    this.#manifest = options.manifest;
    this.#registry = options.registry;
    this.#runtimeFactory = options.runtimeFactory;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#retryInitialDelayMs = options.retryInitialDelayMs ?? DEFAULT_RETRY_INITIAL_DELAY_MS;
    this.#retryMaxDelayMs = options.retryMaxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS;
    this.#random = options.random ?? Math.random;
    this.#randomUuid = options.randomUuid ?? randomUUID;
    this.#metadataAttemptTimeoutMs = options.metadataAttemptTimeoutMs ??
      DEFAULT_METADATA_ATTEMPT_TIMEOUT_MS;
    this.#runtimeCleanupTimeoutMs = options.runtimeCleanupTimeoutMs ??
      this.#config.shutdownDrainMs;
    this.#recoveryCircuitSnapshot = options.recoveryCircuitSnapshot;
    this.#canRetireProject = options.canRetireProject ?? (() => true);
    this.#onLifecycleEvent = options.onLifecycleEvent;
    if (
      !Number.isSafeInteger(this.#retryInitialDelayMs) ||
      this.#retryInitialDelayMs < 1 ||
      !Number.isSafeInteger(this.#retryMaxDelayMs) ||
      this.#retryMaxDelayMs < this.#retryInitialDelayMs ||
      !Number.isSafeInteger(this.#metadataAttemptTimeoutMs) ||
      this.#metadataAttemptTimeoutMs < 1 ||
      !Number.isSafeInteger(this.#runtimeCleanupTimeoutMs) ||
      this.#runtimeCleanupTimeoutMs < 1
    ) {
      throw new RangeError("Fleet retry delays are invalid");
    }
    this.#snapshot = Object.freeze({
      ...emptySnapshot(),
      desired: this.#desiredSandboxes,
    });
  }

  reconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
    return this.#enqueueReconcile(signal);
  }

  /** Receives the supervisor's already-sanitized, bounded recovery record. */
  observeRecoveryEvent(event: SandboxSupervisorEvent): void {
    if (this.#shuttingDown) return;
    const operation = () => this.#recordRecoveryEvent(event);
    const pending = this.#chain.then(operation, operation);
    this.#chain = pending.then(() => undefined, () => undefined);
    void pending.catch(() => undefined);
  }

  /** Wakes project maintenance when the shared outage circuit closes early. */
  observeRecoveryCircuit(snapshot: Readonly<FleetRecoverySnapshot>): void {
    if (this.#shuttingDown || snapshot.state !== "closed" || snapshot.scope !== null) return;
    this.#wakeRetry();
  }

  async #recordRecoveryEvent(event: SandboxSupervisorEvent): Promise<void> {
    if (this.#shuttingDown || event.type === "started") return;
    const current = await this.#manifest.load().catch(() => null);
    const currentWorker = current?.workers.find(worker => worker.projectId === event.projectId);
    if (!currentWorker) return;
    if (event.type === "completed" || event.type === "aborted") {
      await this.#manifest.clearReplacementFailure(event.projectId, this.#clock.now());
      this.#replacementCandidates.delete(event.projectId);
      this.#clearMutationBlock(event.projectId);
      this.#pendingMaintenance = this.#pendingRetirements > 0 ||
        this.#unavailableProjects.size > 0;
      if (event.type === "completed") this.#wakeRetry();
      return;
    }
    if (event.type !== "retry_scheduled") return;
    const failure = replacementFailureSignature(event.failure);
    if (!failure) {
      await this.#manifest.clearReplacementFailure(event.projectId, this.#clock.now());
      this.#replacementCandidates.delete(event.projectId);
      this.#clearMutationBlock(event.projectId, "replace");
      return;
    }
    const manifest = await this.#manifest.recordReplacementFailure({
      id: event.projectId,
      signatureHash: failure.signatureHash,
      kind: failure.kind,
      observedAtMs: this.#clock.now(),
      attempt: event.attempt,
      incarnation: event.incarnation,
      leaderGeneration: event.leaderGeneration ?? currentWorker.leaderGeneration,
      consecutiveFailures: event.consecutiveFailures,
      totalDurationMs: event.totalDurationMs,
    });
    const evidence = manifest.workers.find(
      worker => worker.projectId === event.projectId,
    )?.replacementEvidence;
    if (!evidence || !this.#replacementEvidenceQualifies(evidence)) return;
    if (this.#replacementCandidates.has(event.projectId)) return;
    if (this.#replacementCandidates.size >= this.#config.maxProjects) return;
    this.#replacementCandidates.add(event.projectId);
    this.#emitLifecycle({
      type: "replacement_qualified",
      projectId: event.projectId,
      workerIndex: currentWorker.index,
      leaderGeneration: evidence.lastLeaderGeneration,
      failureKind: evidence.kind,
      failureCount: evidence.count,
    });
    this.#pendingMaintenance = true;
    this.#scheduleRetry();
  }

  #replacementEvidenceQualifies(evidence: FleetReplacementEvidence): boolean {
    return evidence.count >= this.#config.projectReplacementFailureThreshold;
  }

  #hydrateReplacementCandidates(manifest: FleetManifest): void {
    for (const worker of manifest.workers) {
      if (worker.state === "draining" || !worker.replacementEvidence ||
        !this.#replacementEvidenceQualifies(worker.replacementEvidence)) continue;
      if (this.#replacementCandidates.has(worker.projectId)) continue;
      if (this.#replacementCandidates.size >= this.#config.maxProjects) return;
      this.#replacementCandidates.add(worker.projectId);
      this.#emitLifecycle({
        type: "replacement_qualified",
        projectId: worker.projectId,
        workerIndex: worker.index,
        leaderGeneration: worker.replacementEvidence.lastLeaderGeneration,
        failureKind: worker.replacementEvidence.kind,
        failureCount: worker.replacementEvidence.count,
      });
    }
  }

  #hydrateReplacementSuccessors(manifest: FleetManifest): void {
    const retirements = manifest.workers
      .filter(worker => worker.state === "draining" &&
        (worker.retirementReason === "replacement" || worker.retirementReason === "missing"))
      .sort((left, right) => left.updatedAtMs - right.updatedAtMs || left.index - right.index);
    if (retirements.length === 0) return;
    const firstRetirementAtMs = retirements[0]!.updatedAtMs;
    const possible = manifest.workers
      .filter(worker => worker.state !== "draining" && worker.state !== "ready" && worker.managed &&
        worker.createdAtMs >= firstRetirementAtMs)
      .sort((left, right) => left.createdAtMs - right.createdAtMs || left.index - right.index)
      .slice(0, retirements.length);
    for (const worker of possible) {
      if (this.#replacementSuccessors.size >= this.#config.maxProjects) break;
      if (!this.#replacementSuccessors.has(worker.projectId)) {
        this.#replacementSuccessors.set(worker.projectId, {
          created: worker.state !== "creating",
        });
      }
    }
  }

  async waitForReady(minimum: number, signal?: AbortSignal): Promise<FleetSnapshot> {
    if (!Number.isInteger(minimum) || minimum < 1) {
      throw new RangeError("minimum ready runtimes must be positive");
    }
    this.#refreshSnapshot();
    if (this.#snapshot.ready >= minimum) return this.#snapshot;
    if (signal?.aborted) throw new DOMException("Fleet readiness wait aborted", "AbortError");
    return new Promise<FleetSnapshot>((resolve, reject) => {
      const unsubscribe = this.#registry.subscribe(() => {
        this.#refreshSnapshot();
        if (this.#snapshot.ready < minimum) return;
        cleanup();
        resolve(this.#snapshot);
      });
      const onAbort = () => {
        cleanup();
        reject(new DOMException("Fleet readiness wait aborted", "AbortError"));
      };
      const cleanup = () => {
        unsubscribe();
        signal?.removeEventListener("abort", onAbort);
      };
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  async shutdown(reason: string): Promise<void> {
    this.#shuttingDown = true;
    this.#retryController?.abort();
    this.#activeReconcile?.abort();
    await Promise.allSettled([this.#chain, this.#retryTask ?? Promise.resolve()]);
    const runtimes = [...this.#registry.runtimes()];
    const stopped = await Promise.allSettled(runtimes.map(async runtime => {
      await this.#settleWithin(
        () => this.#registry.drain(runtime.projectId, reason),
        this.#runtimeCleanupTimeoutMs,
      );
      if (!await this.#stopAndRemoveRuntime(runtime, this.#runtimeCleanupTimeoutMs)) {
        throw new FleetReconciliationError(
          `Runtime did not stop during fleet shutdown: ${runtime.projectId}`,
          "runtime",
        );
      }
    }));
    const failures = stopped.flatMap(result =>
      result.status === "rejected" ? [result.reason] : []
    );
    if (this.#registry.runtimes().length === 0) {
      this.#unavailableProjects.clear();
      this.#selectedProjects = Object.freeze([]);
      this.#pendingRetirements = 0;
      this.#pendingMaintenance = false;
      this.#refreshSnapshot(0);
    } else {
      this.#selectedProjects = Object.freeze(
        this.#registry.runtimes()
          .map(runtime => runtime.projectId)
          .slice(0, this.#desiredSandboxes),
      );
      this.#refreshSnapshot();
      failures.push(new FleetReconciliationError(
        "Fleet shutdown left registered runtimes",
        "runtime",
      ));
    }
    if (failures.length) throw new AggregateError(failures, "Fleet shutdown was incomplete");
  }

  snapshot(): FleetSnapshot {
    this.#refreshSnapshot();
    return this.#snapshot;
  }

  #enqueueReconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
    if (this.#shuttingDown) {
      return Promise.reject(new FleetReconciliationError(
        "Fleet manager is shutting down",
        "runtime",
      ));
    }
    const pending = this.#chain.then(
      () => this.#runReconcile(signal),
      () => this.#runReconcile(signal),
    );
    this.#chain = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async #runReconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
    if (this.#shuttingDown) {
      throw new FleetReconciliationError("Fleet manager is shutting down", "runtime");
    }
    const controller = new AbortController();
    const onAbort = () => controller.abort();
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) controller.abort();
    this.#activeReconcile = controller;
    try {
      return await this.#reconcile(controller.signal);
    } finally {
      signal?.removeEventListener("abort", onAbort);
      if (this.#activeReconcile === controller) this.#activeReconcile = null;
    }
  }

  async #reconcile(signal: AbortSignal): Promise<FleetSnapshot> {
    this.#throwIfAborted(signal);
    let manifest = await this.#manifest.initialize(this.#identity.kind);
    manifest = await this.#importLegacyProjects(manifest, signal);
    this.#hydrateReplacementCandidates(manifest);
    this.#hydrateReplacementSuccessors(manifest);
    manifest = await this.#applyReplacementCandidates(manifest, signal);
    manifest = await this.#retireSurplusIntents(manifest, signal);
    this.#throwIfAborted(signal);

    const evidence: ControlPlaneEvidence = {
      successes: 0,
      globalFailures: 0,
      freezeMutations: false,
      managedMissingClaimed: false,
    };
    const firstWorkers = manifest.workers
      .filter(worker => worker.state !== "draining")
      .sort((left, right) => left.index - right.index)
      .slice(0, this.#desiredSandboxes);
    this.#selectedProjects = Object.freeze(firstWorkers.map(worker => worker.projectId));
    this.#refreshSnapshot();
    const withProvisionPermit = concurrencyGate(this.#config.provisionConcurrency);
    const withMutationPermit = concurrencyGate(this.#config.provisionConcurrency);
    await Promise.all(firstWorkers.map(worker =>
      this.#reconcileWorker(
        worker,
        evidence,
        signal,
        withProvisionPermit,
        withMutationPermit,
      )
    ));

    manifest = await this.#manifest.load();
    manifest = await this.#reserveManagedCapacity(manifest, signal);
    const firstIds = new Set(firstWorkers.map(worker => worker.projectId));
    const addedWorkers = manifest.workers
      .filter(worker => worker.state !== "draining" && !firstIds.has(worker.projectId))
      .sort((left, right) => left.index - right.index)
      .slice(0, this.#desiredSandboxes);
    this.#selectedProjects = Object.freeze(
      manifest.workers
        .filter(worker => worker.state !== "draining")
        .sort((left, right) => left.index - right.index)
        .slice(0, this.#desiredSandboxes)
        .map(worker => worker.projectId),
    );
    this.#refreshSnapshot();
    await Promise.all(addedWorkers.map(worker =>
      this.#reconcileWorker(
        worker,
        evidence,
        signal,
        withProvisionPermit,
        withMutationPermit,
      )
    ));

    this.#finishControlPlaneEvidence(evidence);
    manifest = await this.#manifest.load();
    const active = manifest.workers
      .filter(worker => worker.state !== "draining")
      .sort((left, right) => left.index - right.index)
      .slice(0, this.#desiredSandboxes);
    this.#selectedProjects = Object.freeze(active.map(worker => worker.projectId));
    await this.#drainOrphanRuntimes(new Set(manifest.workers.map(worker => worker.projectId)), signal);
    this.#refreshSnapshot();

    if (this.#snapshot.ready >= this.#desiredSandboxes) {
      manifest = await this.#cleanupOneRetirement(manifest, signal);
    }
    this.#pendingRetirements = manifest.workers.filter(
      worker => worker.state === "draining",
    ).length;
    const automaticRetirements = manifest.workers.filter(worker =>
      worker.state === "draining" &&
      (worker.retirementReason === "replacement" || worker.retirementReason === "missing")
    ).length;
    const activeCount = manifest.workers.filter(worker => worker.state !== "draining").length;
    this.#pendingMaintenance = this.#unavailableProjects.size > 0 ||
      activeCount < this.#desiredSandboxes ||
      automaticRetirements > 0 ||
      this.#replacementCandidates.size > 0 ||
      this.#mutationRetryInMs() !== null;
    this.#refreshSnapshot();
    this.#scheduleRetry();
    return this.#snapshot;
  }

  async #importLegacyProjects(
    manifest: FleetManifest,
    signal: AbortSignal,
  ): Promise<FleetManifest> {
    if (manifest.legacyMigrationCompleted) return manifest;
    let current = manifest;
    for (let offset = 0; offset < this.#config.projectIds.length; offset++) {
      this.#throwIfAborted(signal);
      const id = this.#config.projectIds[offset]!;
      if (current.workers.some(worker => worker.projectId === id)) continue;
      current = await this.#manifest.reserveWorker({
        index: nextWorkerIndex(current.workers),
        projectId: id,
        title: `Imported Prism project ${offset + 1}`,
        nowMs: this.#clock.now(),
        managed: false,
      });
    }
    return this.#manifest.completeLegacyMigration();
  }

  async #applyReplacementCandidates(
    manifest: FleetManifest,
    signal: AbortSignal,
  ): Promise<FleetManifest> {
    let current = manifest;
    for (const id of [...this.#replacementCandidates]) {
      this.#throwIfAborted(signal);
      const worker = current.workers.find(value => value.projectId === id);
      if (!worker || worker.state === "draining") {
        this.#replacementCandidates.delete(id);
        continue;
      }
      if (!this.#replacementEligible(worker, current)) continue;
      const evidence = worker.replacementEvidence;
      current = await this.#manifest.markDraining(id, "replacement", this.#clock.now());
      this.#replacementCandidates.delete(id);
      this.#clearMutationBlock(id, "replace");
      if (evidence) {
        this.#emitLifecycle({
          type: "replacement_started",
          projectId: id,
          workerIndex: worker.index,
          leaderGeneration: evidence.lastLeaderGeneration,
          failureKind: evidence.kind,
          failureCount: evidence.count,
        });
      }
      // Persist at most one new replacement intent per reconciliation. A
      // healthy successor may unblock another failed minority even when an
      // older remote deletion remains pending.
      return current;
    }
    return current;
  }

  #replacementEligible(worker: FleetWorkerRecord, manifest: FleetManifest): boolean {
    if ((this.#config.v2SidecarRevision !== "r9" && this.#config.v2SidecarRevision !== "r10") ||
      Object.hasOwn(this.#config.v2SidecarProjectRevisions, worker.projectId)) {
      return this.#blockMutation(worker, "replace", "release_policy");
    }
    const gate = this.#mutationGateBlockReason();
    if (gate) return this.#blockMutation(worker, "replace", gate);
    const runtime = this.#registry.get(worker.projectId);
    if (!runtime) return this.#blockMutation(worker, "replace", "runtime_missing");
    const snapshot = runtime.snapshot();
    if (isRuntimeHealthy(snapshot)) {
      return this.#blockMutation(worker, "replace", "runtime_healthy");
    }
    if (snapshot.active !== 0 || snapshot.quarantined !== 0 ||
      (snapshot.ambiguousQuarantined ?? 0) !== 0) {
      return this.#blockMutation(worker, "replace", "work_in_flight");
    }
    try {
      if (!this.#canRetireProject(worker.projectId)) {
        return this.#blockMutation(worker, "replace", "continuation_binding");
      }
    } catch {
      return this.#blockMutation(worker, "replace", "continuation_binding");
    }
    const selected = new Set(
      manifest.workers
        .filter(value => value.state !== "draining")
        .map(value => value.projectId),
    );
    const healthyPeers = this.#registry.snapshots().filter(value =>
      value.projectId !== worker.projectId &&
      selected.has(value.projectId) &&
      isRuntimeHealthy(value) &&
      value.releaseRevision === this.#config.v2SidecarRevision
    ).length;
    if (healthyPeers < Math.floor(this.#desiredSandboxes / 2) + 1) {
      return this.#blockMutation(worker, "replace", "healthy_majority");
    }
    this.#clearMutationBlock(worker.projectId, "replace");
    return true;
  }

  async #retireSurplusIntents(
    manifest: FleetManifest,
    signal: AbortSignal,
  ): Promise<FleetManifest> {
    const active = manifest.workers.filter(worker => worker.state !== "draining");
    if (active.length <= this.#desiredSandboxes) return manifest;
    const ranked = [...active].sort((left, right) => {
      const leftHealthy = isRuntimeHealthy(
        this.#registry.get(left.projectId)?.snapshot() ?? missingSnapshot(left.projectId),
      );
      const rightHealthy = isRuntimeHealthy(
        this.#registry.get(right.projectId)?.snapshot() ?? missingSnapshot(right.projectId),
      );
      if (leftHealthy !== rightHealthy) return leftHealthy ? -1 : 1;
      return left.index - right.index;
    });
    const keep = new Set(
      ranked.slice(0, this.#desiredSandboxes).map(worker => worker.projectId),
    );
    let current = manifest;
    for (const worker of active) {
      this.#throwIfAborted(signal);
      if (!keep.has(worker.projectId)) {
        current = await this.#manifest.markDraining(
          worker.projectId,
          "surplus",
          this.#clock.now(),
        );
      }
    }
    return current;
  }

  async #reserveManagedCapacity(
    manifest: FleetManifest,
    signal: AbortSignal,
  ): Promise<FleetManifest> {
    let current = manifest;
    let active = current.workers.filter(worker => worker.state !== "draining").length;
    while (active < this.#desiredSandboxes) {
      this.#throwIfAborted(signal);
      if (active >= this.#config.maxProjects) {
        throw new FleetReconciliationError(
          `Only ${active} of ${this.#desiredSandboxes} managed projects fit the fleet limit`,
          "insufficient_projects",
        );
      }
      const index = nextWorkerIndex(current.workers);
      const id = this.#newProjectId(current.workers);
      const automaticRetirements = current.workers.filter(worker =>
        worker.state === "draining" &&
        (worker.retirementReason === "replacement" || worker.retirementReason === "missing")
      ).length;
      const trackedSuccessors = [...this.#replacementSuccessors.keys()].filter(project =>
        current.workers.some(worker => worker.projectId === project && worker.state !== "draining")
      ).length;
      const replacementSuccessor = trackedSuccessors < automaticRetirements;
      current = await this.#manifest.reserveWorker({
        index,
        projectId: id,
        title: `GhostProv Worker ${index}`,
        nowMs: this.#clock.now(),
        managed: true,
      });
      if (replacementSuccessor && this.#replacementSuccessors.size < this.#config.maxProjects) {
        this.#replacementSuccessors.set(id, { created: false });
        this.#emitLifecycle({
          type: "successor_reserved",
          projectId: id,
          workerIndex: index,
        });
      }
      active++;
    }
    return current;
  }

  #newProjectId(workers: readonly FleetWorkerRecord[]): ProjectId {
    const existing = new Set(workers.map(worker => worker.projectId));
    for (let attempt = 0; attempt < 32; attempt++) {
      const id = projectId(this.#randomUuid());
      if (!existing.has(id)) return id;
    }
    throw new FleetReconciliationError("Could not allocate a unique managed project ID", "runtime");
  }

  async #reconcileWorker(
    worker: FleetWorkerRecord,
    evidence: ControlPlaneEvidence,
    signal: AbortSignal,
    withProvisionPermit: <T>(operation: () => Promise<T>) => Promise<T>,
    withMutationPermit: <T>(operation: () => Promise<T>) => Promise<T>,
  ): Promise<void> {
    this.#throwIfAborted(signal);
    const existing = this.#registry.get(worker.projectId);
    if (existing) {
      const snapshot = existing.snapshot();
      if (isRuntimeHealthy(snapshot)) {
        this.#unavailableProjects.delete(worker.projectId);
        await this.#manifest.markState(worker.projectId, "ready", this.#clock.now());
        this.#activateSuccessor(worker, worker.leaderGeneration);
      } else {
        this.#unavailableProjects.add(worker.projectId);
      }
      return;
    }

    const project = await this.#resolveProject(
      worker,
      evidence,
      signal,
      withMutationPermit,
    );
    if (!project) return;
    const latest = (await this.#manifest.load()).workers.find(
      value => value.projectId === worker.projectId,
    );
    if (!latest || latest.state === "draining") return;
    await withProvisionPermit(async () => {
      const generation = await this.#manifest.nextLeaderGeneration(
        worker.projectId,
        this.#clock.now(),
      );
      this.#throwIfAborted(signal);
      const runtime = this.#runtimeFactory.create({
        projectId: worker.projectId,
        workerIndex: worker.index,
        leaderGeneration: generation,
        previouslyReady: latest.hasReachedReady,
      });
      if (runtime.projectId !== worker.projectId) {
        await this.#stopAndRemoveRuntime(runtime);
        this.#unavailableProjects.add(worker.projectId);
        await this.#manifest.markState(worker.projectId, "failed", this.#clock.now());
        return;
      }
      this.#registry.add(runtime);
      try {
        await this.#startRuntime(runtime, signal);
        await this.#manifest.markState(worker.projectId, "ready", this.#clock.now());
        this.#unavailableProjects.delete(worker.projectId);
        this.#activateSuccessor(worker, generation);
      } catch (error) {
        if (signal.aborted || this.#shuttingDown) throw error;
        await this.#manifest.markState(worker.projectId, "failed", this.#clock.now());
        this.#unavailableProjects.add(worker.projectId);
      }
    });
  }

  async #resolveProject(
    worker: FleetWorkerRecord,
    evidence: ControlPlaneEvidence,
    signal: AbortSignal,
    withMutationPermit: <T>(operation: () => Promise<T>) => Promise<T>,
  ): Promise<PrismProjectSummary | null> {
    try {
      let project: PrismProjectSummary;
      if (worker.state === "creating" && worker.managed) {
        const mutationBlock = evidence.freezeMutations
          ? "control_plane" as const
          : this.#mutationGateBlockReason();
        if (mutationBlock) {
          this.#blockMutation(worker, "create", mutationBlock);
          this.#unavailableProjects.add(worker.projectId);
          return null;
        }
        if (!this.#projects.create) {
          this.#blockMutation(worker, "create", "project_client");
          throw new FleetReconciliationError(
            "Fleet project client cannot create managed projects",
            "runtime",
          );
        }
        try {
          project = await this.#getProject(worker.projectId, signal);
        } catch (readError) {
          if (!isDefinitiveMissingProject(readError)) throw readError;
          const created = await withMutationPermit(async () => {
            const block = evidence.freezeMutations
              ? "control_plane" as const
              : this.#mutationGateBlockReason();
            if (block) {
              this.#blockMutation(worker, "create", block);
              return null;
            }
            try {
              return await this.#withMetadataTimeout(
                attempt => this.#projects.create!({
                  projectId: worker.projectId,
                  title: worker.title,
                }, attempt),
                signal,
                "creation",
                worker.projectId,
              );
            } catch (createError) {
              if (statusOf(createError) === 409) {
                return this.#getProject(worker.projectId, signal);
              }
              if (isGlobalControlPlaneFailure(createError)) {
                evidence.globalFailures++;
                evidence.freezeMutations = true;
                this.#blockMutation(worker, "create", "control_plane");
                return null;
              }
              throw createError;
            }
          });
          if (!created) {
            this.#unavailableProjects.add(worker.projectId);
            return null;
          }
          project = created;
        }
      } else {
        project = await this.#getProject(worker.projectId, signal);
      }
      if (!validProjectSummary(worker.projectId, project)) {
        if (this.#canReplaceMissingWorker(worker, evidence)) {
          await this.#manifest.markDraining(worker.projectId, "missing", this.#clock.now());
          this.#unavailableProjects.delete(worker.projectId);
        } else {
          this.#unavailableProjects.add(worker.projectId);
        }
        return null;
      }
      evidence.successes++;
      if (worker.state === "creating") {
        await this.#manifest.markCreated(worker.projectId, this.#clock.now());
        this.#clearMutationBlock(worker.projectId, "create");
        const successor = this.#replacementSuccessors.get(worker.projectId);
        if (successor && !successor.created) {
          successor.created = true;
          this.#emitLifecycle({
            type: "successor_created",
            projectId: worker.projectId,
            workerIndex: worker.index,
          });
        }
      }
      return project;
    } catch (error) {
      if (signal.aborted || this.#shuttingDown || isAbort(error)) {
        throw new DOMException("Fleet reconciliation aborted", "AbortError");
      }
      if (error instanceof FleetReconciliationError) throw error;
      if (isDefinitiveMissingProject(error) &&
        !(worker.state === "creating" && worker.managed)) {
        if (this.#canReplaceMissingWorker(worker, evidence)) {
          await this.#manifest.markDraining(worker.projectId, "missing", this.#clock.now());
          this.#unavailableProjects.delete(worker.projectId);
        } else {
          this.#unavailableProjects.add(worker.projectId);
        }
        return null;
      }
      if (isGlobalControlPlaneFailure(error)) {
        evidence.globalFailures++;
        evidence.freezeMutations = true;
      }
      this.#unavailableProjects.add(worker.projectId);
      return null;
    }
  }

  #canReplaceMissingWorker(
    worker: FleetWorkerRecord,
    evidence: ControlPlaneEvidence,
  ): boolean {
    if (!worker.managed) return true;
    if (
      (this.#config.v2SidecarRevision !== "r9" && this.#config.v2SidecarRevision !== "r10") ||
      Object.hasOwn(this.#config.v2SidecarProjectRevisions, worker.projectId) ||
      evidence.managedMissingClaimed ||
      !this.#mutationsAllowed()
    ) return false;
    try {
      if (!this.#canRetireProject(worker.projectId)) return false;
    } catch {
      return false;
    }
    const healthyPeers = this.#registry.snapshots().filter(snapshot =>
      snapshot.projectId !== worker.projectId &&
      isRuntimeHealthy(snapshot) &&
      snapshot.releaseRevision === this.#config.v2SidecarRevision
    ).length;
    if (healthyPeers < Math.floor(this.#desiredSandboxes / 2) + 1) return false;
    evidence.managedMissingClaimed = true;
    return true;
  }

  async #getProject(id: ProjectId, signal: AbortSignal): Promise<PrismProjectSummary> {
    return this.#withMetadataTimeout(
      attempt => this.#projects.get(id, attempt),
      signal,
      "metadata read",
      id,
    );
  }

  async #withMetadataTimeout<T>(
    operation: (signal: AbortSignal) => Promise<T>,
    signal: AbortSignal,
    operationName: string,
    id: ProjectId,
  ): Promise<T> {
    this.#throwIfAborted(signal);
    const attempt = new AbortController();
    let rejectAbort!: (error: DOMException) => void;
    const aborted = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const onAbort = () => {
      attempt.abort();
      rejectAbort(new DOMException("Fleet reconciliation aborted", "AbortError"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<never>((_resolve, reject) => {
      timeout = setTimeout(() => {
        attempt.abort();
        const error = new Error(`Prism project ${operationName} timed out: ${id}`) as Error & {
          kind: string;
          operation: string;
        };
        error.name = "FleetProjectTimeoutError";
        error.kind = "timeout";
        error.operation = "project_control_plane";
        reject(error);
      }, this.#metadataAttemptTimeoutMs);
      timeout.unref?.();
    });
    const pending = Promise.resolve().then(() => operation(attempt.signal));
    try {
      return await Promise.race([pending, aborted, timedOut]);
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
      signal.removeEventListener("abort", onAbort);
      attempt.abort();
    }
  }

  #finishControlPlaneEvidence(evidence: ControlPlaneEvidence): void {
    if (evidence.globalFailures > 0 &&
      (evidence.successes === 0 || evidence.globalFailures >= 2)) {
      this.#mutationFailureCount = Math.min(30, this.#mutationFailureCount + 1);
      this.#mutationsFrozenUntilMs = this.#clock.now() + Math.min(
        DEFAULT_MUTATION_MAX_BACKOFF_MS,
        this.#retryInitialDelayMs * (2 ** Math.min(20, this.#mutationFailureCount - 1)),
      );
      return;
    }
    if (evidence.successes > 0 && evidence.globalFailures === 0) {
      this.#mutationFailureCount = 0;
      this.#mutationsFrozenUntilMs = null;
    }
  }

  #mutationsAllowed(): boolean {
    return this.#mutationGateBlockReason() === null;
  }

  #mutationGateBlockReason(): "recovery_circuit" | "mutation_backoff" | null {
    const circuit = this.#recoveryCircuitSnapshot?.();
    if (circuit && (circuit.state !== "closed" || circuit.scope !== null)) {
      return "recovery_circuit";
    }
    return this.#mutationRetryInMs() === null ? null : "mutation_backoff";
  }

  #mutationRetryInMs(): number | null {
    if (this.#mutationsFrozenUntilMs === null) return null;
    const remaining = this.#mutationsFrozenUntilMs - this.#clock.now();
    if (remaining <= 0) {
      this.#mutationsFrozenUntilMs = null;
      return null;
    }
    return Math.ceil(remaining);
  }

  #blockMutation(
    worker: Pick<FleetWorkerRecord, "projectId" | "index">,
    operation: FleetMutationOperation,
    reason: FleetMutationBlockReason,
  ): false {
    const key = `${operation}:${worker.projectId}`;
    if (this.#mutationBlocks.get(key) === reason) return false;
    if (!this.#mutationBlocks.has(key) &&
      this.#mutationBlocks.size >= this.#config.maxProjects * 3) {
      const oldest = this.#mutationBlocks.keys().next().value;
      if (oldest !== undefined) this.#mutationBlocks.delete(oldest);
    }
    this.#mutationBlocks.set(key, reason);
    this.#emitLifecycle({
      type: "mutation_blocked",
      projectId: worker.projectId,
      workerIndex: worker.index,
      operation,
      reason,
    });
    return false;
  }

  #clearMutationBlock(project: ProjectId, operation?: FleetMutationOperation): void {
    if (operation) {
      this.#mutationBlocks.delete(`${operation}:${project}`);
      return;
    }
    for (const value of ["replace", "create", "delete"] as const) {
      this.#mutationBlocks.delete(`${value}:${project}`);
    }
  }

  #activateSuccessor(
    worker: Pick<FleetWorkerRecord, "projectId" | "index">,
    leaderGeneration: number,
  ): void {
    if (!this.#replacementSuccessors.delete(worker.projectId)) return;
    this.#clearMutationBlock(worker.projectId);
    this.#emitLifecycle({
      type: "successor_activated",
      projectId: worker.projectId,
      workerIndex: worker.index,
      leaderGeneration,
    });
  }

  #emitLifecycle(event: FleetLifecycleEvent): void {
    try { this.#onLifecycleEvent?.(Object.freeze(event)); } catch {}
  }

  async #cleanupOneRetirement(
    manifest: FleetManifest,
    signal: AbortSignal,
  ): Promise<FleetManifest> {
    const worker = manifest.workers
      .filter(value => value.state === "draining" &&
        (value.retirementReason === "replacement" ||
          value.retirementReason === "missing"))
      .sort((left, right) => left.updatedAtMs - right.updatedAtMs || left.index - right.index)[0];
    if (!worker) return manifest;
    const gate = this.#mutationGateBlockReason();
    if (gate) {
      this.#blockMutation(worker, "delete", gate);
      return manifest;
    }
    try {
      if (!this.#canRetireProject(worker.projectId)) {
        this.#blockMutation(worker, "delete", "continuation_binding");
        return manifest;
      }
    } catch {
      this.#blockMutation(worker, "delete", "continuation_binding");
      return manifest;
    }
    const runtime = this.#registry.get(worker.projectId);
    if (runtime) {
      const snapshot = runtime.snapshot();
      // Healthy capacity is never destructively shrunk in-band. Operators can
      // use the digest-confirmed decommission command; automatic deletion is
      // reserved for an already-unhealthy replacement target.
      if (isRuntimeHealthy(snapshot)) {
        this.#blockMutation(worker, "delete", "runtime_healthy");
        return manifest;
      }
      if (snapshot.active !== 0 || snapshot.quarantined !== 0 ||
        (snapshot.ambiguousQuarantined ?? 0) !== 0) {
        this.#blockMutation(worker, "delete", "work_in_flight");
        return manifest;
      }
      await this.#drainRuntime(runtime, signal);
      try {
        if (!this.#canRetireProject(worker.projectId)) {
          this.#blockMutation(worker, "delete", "continuation_binding");
          return manifest;
        }
      } catch {
        this.#blockMutation(worker, "delete", "continuation_binding");
        return manifest;
      }
    }
    if (!worker.managed) {
      const next = await this.#manifest.removeImported(worker.projectId);
      this.#clearMutationBlock(worker.projectId);
      this.#emitLifecycle({
        type: "old_retired",
        projectId: worker.projectId,
        workerIndex: worker.index,
        retirementReason: worker.retirementReason as "replacement" | "missing",
        managed: false,
      });
      this.#replacementSuccessors.delete(worker.projectId);
      return next;
    }
    if (!this.#projects.deleteManaged) {
      this.#blockMutation(worker, "delete", "project_client");
      return manifest;
    }
    try {
      await this.#withMetadataTimeout(
        attempt => this.#projects.deleteManaged!(worker.projectId, attempt),
        signal,
        "deletion",
        worker.projectId,
      );
    } catch (error) {
      if (signal.aborted || this.#shuttingDown || isAbort(error)) throw error;
      if (!isDefinitiveMissingProject(error)) {
        if (isGlobalControlPlaneFailure(error)) {
          this.#openMutationCircuit();
        }
        this.#blockMutation(worker, "delete", "control_plane");
        return manifest;
      }
    }
    this.#mutationFailureCount = 0;
    this.#mutationsFrozenUntilMs = null;
    const next = await this.#manifest.removeManaged(worker.projectId);
    this.#clearMutationBlock(worker.projectId);
    this.#emitLifecycle({
      type: "old_retired",
      projectId: worker.projectId,
      workerIndex: worker.index,
      retirementReason: worker.retirementReason as "replacement" | "missing",
      managed: true,
    });
    this.#replacementSuccessors.delete(worker.projectId);
    return next;
  }

  #openMutationCircuit(): void {
    this.#mutationFailureCount = Math.min(30, this.#mutationFailureCount + 1);
    this.#mutationsFrozenUntilMs = this.#clock.now() + Math.min(
      DEFAULT_MUTATION_MAX_BACKOFF_MS,
      this.#retryInitialDelayMs * (2 ** Math.min(20, this.#mutationFailureCount - 1)),
    );
  }

  async #drainOrphanRuntimes(
    manifestProjects: ReadonlySet<ProjectId>,
    signal: AbortSignal,
  ): Promise<void> {
    for (const runtime of [...this.#registry.runtimes()]) {
      if (manifestProjects.has(runtime.projectId)) continue;
      await this.#drainRuntime(runtime, signal);
    }
  }

  async #drainRuntime(runtime: RoutableSandboxRuntime, signal: AbortSignal): Promise<void> {
    this.#throwIfAborted(signal);
    const draining = Promise.resolve().then(() =>
      this.#registry.drain(runtime.projectId, "fleet-retirement")
    );
    let rejectAbort!: (error: DOMException) => void;
    const aborted = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const onAbort = () => rejectAbort(
      new DOMException("Fleet reconciliation aborted", "AbortError"),
    );
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
    try {
      await Promise.race([draining, aborted]);
    } catch (error) {
      const stopped = await this.#stopAndRemoveRuntime(runtime);
      if (signal.aborted) {
        void draining.finally(() => this.#stopAndRemoveRuntime(runtime)).catch(() => undefined);
        throw error;
      }
      if (!stopped) throw error;
      return;
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
    if (!await this.#stopAndRemoveRuntime(runtime)) {
      throw new FleetReconciliationError(
        `Retired runtime could not be stopped: ${runtime.projectId}`,
        "runtime",
      );
    }
  }

  async #startRuntime(runtime: RoutableSandboxRuntime, signal: AbortSignal): Promise<void> {
    this.#throwIfAborted(signal);
    const starting = Promise.resolve().then(() => runtime.start());
    let rejectAbort!: (error: DOMException) => void;
    const aborted = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const onAbort = () => rejectAbort(
      new DOMException("Fleet reconciliation aborted", "AbortError"),
    );
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
    try {
      await Promise.race([starting, aborted]);
      this.#throwIfAborted(signal);
    } catch (error) {
      if (!signal.aborted) throw error;
      await this.#stopAndRemoveRuntime(runtime);
      void starting.finally(() => this.#stopAndRemoveRuntime(runtime)).catch(() => undefined);
      throw error;
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  async #stopAndRemoveRuntime(
    runtime: RoutableSandboxRuntime,
    timeoutMs = this.#runtimeCleanupTimeoutMs,
  ): Promise<boolean> {
    if (runtime.snapshot().state !== "stopped") {
      const stopped = await this.#settleWithin(() => runtime.stop(), timeoutMs);
      if (!stopped) return false;
    }
    if (runtime.snapshot().state !== "stopped") return false;
    if (this.#registry.get(runtime.projectId) === runtime) {
      await this.#registry.removeStopped(runtime.projectId).catch(() => undefined);
    }
    return this.#registry.get(runtime.projectId) !== runtime;
  }

  async #settleWithin(operation: () => Promise<void>, timeoutMs: number): Promise<boolean> {
    const observed = Promise.resolve().then(operation).then(
      () => true,
      () => false,
    );
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        observed,
        new Promise<false>(resolve => {
          timeout = setTimeout(() => resolve(false), timeoutMs);
          timeout.unref?.();
        }),
      ]);
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
    }
  }

  #scheduleRetry(): void {
    if (this.#shuttingDown || this.#retryTask || !this.#pendingMaintenance) return;
    const controller = new AbortController();
    this.#retryController = controller;
    const task = this.#runRetryLoop(controller.signal);
    this.#retryTask = task;
    void task.catch(() => undefined).finally(() => {
      if (this.#retryTask === task) this.#retryTask = null;
      if (this.#retryController === controller) this.#retryController = null;
      if (!this.#shuttingDown && this.#pendingMaintenance) this.#scheduleRetry();
    });
  }

  #wakeRetry(): void {
    if (!this.#pendingMaintenance || this.#shuttingDown) return;
    // A supervisor can report `completed` synchronously inside the retry
    // reconciliation which is starting it. That completion is already the
    // progress the loop was waiting for. Aborting the shared retry controller
    // here would abort #startRuntime immediately after readiness and destroy
    // the healthy replacement that just recovered.
    if (this.#activeReconcile !== null) return;
    if (this.#retryController) {
      this.#retryController.abort();
      return;
    }
    this.#scheduleRetry();
  }

  async #runRetryLoop(signal: AbortSignal): Promise<void> {
    while (!this.#shuttingDown && this.#pendingMaintenance) {
      await this.#clock.sleep(this.#retryDelayMs(), signal);
      if (signal.aborted || this.#shuttingDown) return;
      const before = `${this.#snapshot.ready}:${this.#snapshot.unavailable}:` +
        `${this.#pendingRetirements}:${this.#replacementCandidates.size}`;
      try {
        await this.#enqueueReconcile(signal);
      } catch (error) {
        if (signal.aborted || this.#shuttingDown) return;
        void error;
      }
      const after = `${this.#snapshot.ready}:${this.#snapshot.unavailable}:` +
        `${this.#pendingRetirements}:${this.#replacementCandidates.size}`;
      this.#retryAttempt = before === after ? Math.min(this.#retryAttempt + 1, 30) : 0;
    }
    this.#retryAttempt = 0;
  }

  #retryDelayMs(): number {
    const exponential = Math.min(
      this.#retryMaxDelayMs,
      this.#retryInitialDelayMs * (2 ** Math.min(this.#retryAttempt, 30)),
    );
    const sampled = this.#random();
    const random = Number.isFinite(sampled)
      ? Math.min(1, Math.max(0, sampled))
      : 0.5;
    const jitter = 1 - RETRY_JITTER_RATIO + (2 * RETRY_JITTER_RATIO * random);
    const retry = Math.max(1, Math.min(this.#retryMaxDelayMs, Math.round(exponential * jitter)));
    const circuit = this.#recoveryCircuitSnapshot?.();
    const circuitRetry = circuit?.nextProbeAtMs === null || circuit?.nextProbeAtMs === undefined
      ? 0
      : Math.ceil(Math.max(0, circuit.nextProbeAtMs - this.#clock.now()));
    return Math.max(retry, this.#mutationRetryInMs() ?? 0, circuitRetry);
  }

  #refreshSnapshot(
    desired = this.#shuttingDown && this.#registry.runtimes().length === 0
      ? 0
      : this.#desiredSandboxes,
  ): void {
    const selectedProjects = new Set(this.#selectedProjects);
    const allSnapshots = this.#registry.snapshots();
    const snapshots = allSnapshots.filter(snapshot => selectedProjects.has(snapshot.projectId));
    const snapshotsByProject = new Map(
      snapshots.map(snapshot => [snapshot.projectId, snapshot] as const),
    );
    const unavailableProjects = this.#selectedProjects.filter(id => {
      const snapshot = snapshotsByProject.get(id);
      return !snapshot || !isRuntimeHealthy(snapshot);
    });
    const missingRuntimeCount = this.#selectedProjects.filter(
      id => !snapshotsByProject.has(id),
    ).length;
    const localRetryInMs = this.#mutationRetryInMs();
    const circuit = this.#recoveryCircuitSnapshot?.();
    const circuitOpen = circuit !== undefined &&
      (circuit.state !== "closed" || circuit.scope !== null);
    const circuitRetryInMs = circuit?.nextProbeAtMs === null || circuit?.nextProbeAtMs === undefined
      ? null
      : Math.ceil(Math.max(0, circuit.nextProbeAtMs - this.#clock.now()));
    const retryInMs = localRetryInMs === null
      ? circuitRetryInMs
      : circuitRetryInMs === null
        ? localRetryInMs
        : Math.max(localRetryInMs, circuitRetryInMs);
    this.#snapshot = Object.freeze({
      desired,
      ready: snapshots.filter(isRuntimeHealthy).length,
      provisioning: snapshots.filter(value => [
        "configured",
        "provisioning",
        "syncing_credentials",
        "syncing_workspace",
        "bootstrapping",
        "connecting_transport",
        "recovering",
      ].includes(value.state)).length,
      draining: allSnapshots.filter(value => value.state === "draining").length,
      failed: snapshots.filter(value => value.state === "failed" || value.state === "auth_required").length +
        missingRuntimeCount,
      unavailable: unavailableProjects.length,
      unavailableProjects: Object.freeze([...unavailableProjects]),
      projects: Object.freeze([...this.#selectedProjects]),
      pendingRetirements: this.#pendingRetirements,
      projectMutation: Object.freeze({
        state: localRetryInMs === null && !circuitOpen ? "closed" : "open",
        consecutiveFailures: Math.max(
          this.#mutationFailureCount,
          circuit?.consecutiveFailures ?? 0,
        ),
        retryInMs,
      }),
    });
  }

  #throwIfAborted(signal: AbortSignal): void {
    if (signal.aborted || this.#shuttingDown) {
      throw new DOMException("Fleet reconciliation aborted", "AbortError");
    }
  }
}
