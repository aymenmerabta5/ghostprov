import { createHash, randomUUID } from "node:crypto";
import type { MultiSandboxConfig } from "../config/multi-sandbox";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import { FLEET_BOOTSTRAP_CLEANUP_ERROR_NAMES, SANDBOX_SUPERVISOR_PHASES } from "../observability/events";
import { requestFailureFields } from "../observability/request-error";
import type { FleetRecoverySnapshot } from "../prism/recovery-coordinator";
import type { RecoveryConnectivity } from "../prism/connectivity";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { PrismProjectSummary } from "../prism/projects-client";
import { isRuntimeHealthy } from "../router/policy";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../sandbox/registry";
import type { SandboxSupervisorEvent } from "../sandbox/supervised-runtime";
import { BOOTSTRAP_FAILURE_CODES, BOOTSTRAP_FAILURE_PHASES, BOOTSTRAP_FAILURE_REASONS, projectId, type ProjectId } from "../sandbox/types";
import { FleetManifestStore, MAX_MANIFEST_WORKERS, sameFleetWorkerRecord } from "./manifest";
import { FileBootstrapHoldMigration } from "./bootstrap-migration";
import type {
  FleetLifecycleEvent,
  FleetBootstrapCleanupDiagnostic,
  FleetBootstrapCleanupReason,
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

interface RuntimeStart {
  readonly runtime: RoutableSandboxRuntime;
  readonly worker: FleetWorkerRecord;
  readonly generation: number;
  readonly controller: AbortController;
  readonly unlinkAbort: () => void;
  completion: Promise<void>;
  settled: boolean;
  succeeded: boolean;
}

interface RuntimeStop {
  state: "pending" | "fulfilled" | "rejected";
  observation: Promise<boolean>;
}

interface BootstrapCleanupOperation {
  readonly kind: "prove" | "finalize";
  readonly worker: Readonly<FleetWorkerRecord>;
  readonly runtimeOwner: RoutableSandboxRuntime | null;
  readonly controller: AbortController;
  readonly deadlineMs: number;
  completion: Promise<void>;
  settled: boolean;
  succeeded: boolean;
  deferredReason: FleetBootstrapCleanupReason | null;
}

const DEFAULT_RETRY_INITIAL_DELAY_MS = 500;
const DEFAULT_RETRY_MAX_DELAY_MS = 30_000;
const DEFAULT_METADATA_ATTEMPT_TIMEOUT_MS = 15_000;
const DEFAULT_MUTATION_MAX_BACKOFF_MS = 300_000;
const RETRY_JITTER_RATIO = 0.2;
export const MAX_PENDING_FLEET_RECOVERY_EVENTS = MAX_MANIFEST_WORKERS * 4;
const MAX_RUNTIME_STOP_OBSERVATIONS = MAX_MANIFEST_WORKERS * 2;
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

function cleanupOwnData(value: object, key: string): unknown {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && "value" in descriptor ? descriptor.value : undefined;
  } catch { return undefined; }
}

function cleanupAtom<T extends string>(value: unknown, allowed: readonly T[]): T | undefined {
  return typeof value === "string" && allowed.includes(value as T) ? value as T : undefined;
}

/** At most six own-data cause records; getters, messages and arbitrary payloads are never read. */
export function bootstrapCleanupFailureDiagnostic(error: unknown): FleetBootstrapCleanupDiagnostic {
  const diagnostic: FleetBootstrapCleanupDiagnostic = { errorName: "Error" };
  const seen = new Set<object>();
  let value = error;
  for (let depth = 0; depth < 6 && value !== null && typeof value === "object" && !seen.has(value); depth++) {
    seen.add(value);
    const name = cleanupOwnData(value, "name");
    diagnostic.errorName = cleanupAtom(name, FLEET_BOOTSTRAP_CLEANUP_ERROR_NAMES) ??
      cleanupAtom(requestFailureFields(value).error_name, FLEET_BOOTSTRAP_CLEANUP_ERROR_NAMES) ?? "Error";
    if (name === "V2SupervisorBootstrapError") {
      diagnostic.bootstrapPhase ??= cleanupAtom(cleanupOwnData(value, "kind"), BOOTSTRAP_FAILURE_PHASES);
      diagnostic.bootstrapCode ??= cleanupAtom(cleanupOwnData(value, "remoteCode"), BOOTSTRAP_FAILURE_CODES);
      diagnostic.bootstrapReason ??= cleanupAtom(cleanupOwnData(value, "reason"), BOOTSTRAP_FAILURE_REASONS);
    }
    if (name === "V2SupervisorError") {
      diagnostic.supervisorPhase ??= cleanupAtom(cleanupOwnData(value, "phase"), SANDBOX_SUPERVISOR_PHASES);
    }
    value = cleanupOwnData(value, "cause");
  }
  return diagnostic;
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
  const cleanupBlocked = failure.kind === "deploy" &&
    failure.bootstrapCode === "bootstrap_cleanup_unproven";
  const bootstrapBlocked = failure.kind === "deploy" &&
    (failure.bootstrapCode === "bootstrap_cleanup_unproven" || failure.bootstrapCode === "bootstrap_ownership_conflict");
  if (!bootstrapBlocked && (!failure.kind || !REPLACEABLE_BOOTSTRAP_KINDS.has(failure.kind))) return null;
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
    ...(bootstrapBlocked ? [failure.bootstrapPhase ?? "", failure.bootstrapCode!,
      ...(cleanupBlocked ? [] : [failure.bootstrapReason ?? ""])] : []),
  ].join("|");
  return Object.freeze({
    signatureHash: createHash("sha256").update(signature).digest("hex"),
    kind: bootstrapBlocked ? "bootstrap_blocked" : failure.kind as FleetReplacementFailureKind,
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
  connectivity?: RecoveryConnectivity;
  canRetireProject?: (projectId: ProjectId) => boolean;
  bootstrapMigration?: FileBootstrapHoldMigration;
  /** Cleanup-only trusted composition port: true requires durable exact cleanup_proven, retained until finalization. */
  cleanupHeldBootstrap?: (worker: Readonly<FleetWorkerRecord>, signal: AbortSignal) => Promise<boolean>;
  /** Finalizes proven bootstrap artifacts before any managed retirement deletes its project or manifest record. */
  finalizeHeldBootstrapCleanup?: (worker: Readonly<FleetWorkerRecord>, signal: AbortSignal) => Promise<void>;
  bootstrapCleanupTimeoutMs?: number;
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
  readonly #connectivity?: RecoveryConnectivity;
  readonly #canRetireProject: (projectId: ProjectId) => boolean;
  readonly #bootstrapMigration?: FileBootstrapHoldMigration;
  readonly #cleanupHeldBootstrap?: ProjectFleetManagerOptions["cleanupHeldBootstrap"];
  readonly #finalizeHeldBootstrapCleanup?: ProjectFleetManagerOptions["finalizeHeldBootstrapCleanup"];
  readonly #bootstrapCleanupTimeoutMs: number;
  #bootstrapCleanup: BootstrapCleanupOperation | null = null;
  #finalizedBootstrapRetirement: Readonly<FleetWorkerRecord> | null = null;
  #bootstrapCleanupRetryAtMs = 0;
  #lastHeldCleanupIndex = 0;
  #lastCleanupDeferral: string | null = null;
  #operatorRecoveryProjects: ReadonlySet<ProjectId> = new Set();
  readonly #onLifecycleEvent?: (event: FleetLifecycleEvent) => void;
  readonly #unavailableProjects = new Set<ProjectId>();
  readonly #replacementCandidates = new Set<ProjectId>();
  readonly #replacementSuccessors = new Map<ProjectId, {
    created: boolean;
    replacementForProjectId: ProjectId;
  }>();
  readonly #mutationBlocks = new Map<string, FleetMutationBlockReason>();
  readonly #runtimeStarts = new Map<RoutableSandboxRuntime, RuntimeStart>();
  readonly #runtimeStops = new Map<RoutableSandboxRuntime, RuntimeStop>();
  readonly #withProvisionPermit: <T>(operation: () => Promise<T>) => Promise<T>;
  readonly #pendingRecoveryEvents: SandboxSupervisorEvent[] = [];
  #recoveryEvidenceGap = false;
  #bookkeepingQueued = false;
  #yieldStartupWait: (() => void) | null = null;
  #startupWaitYielded: Promise<void> | null = null;
  #selectedProjects: readonly ProjectId[] = Object.freeze([]);
  #heldProjects: readonly ProjectId[] = Object.freeze([]);
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
    this.#connectivity = options.connectivity;
    this.#canRetireProject = options.canRetireProject ?? (() => true);
    this.#bootstrapMigration = options.bootstrapMigration;
    this.#cleanupHeldBootstrap = options.cleanupHeldBootstrap;
    this.#finalizeHeldBootstrapCleanup = options.finalizeHeldBootstrapCleanup;
    this.#bootstrapCleanupTimeoutMs = options.bootstrapCleanupTimeoutMs ?? this.#runtimeCleanupTimeoutMs;
    if (this.#cleanupHeldBootstrap && !this.#finalizeHeldBootstrapCleanup) {
      throw new TypeError("Held bootstrap recovery requires retirement cleanup finalization");
    }
    this.#onLifecycleEvent = options.onLifecycleEvent;
    this.#withProvisionPermit = concurrencyGate(this.#config.provisionConcurrency);
    if (
      !Number.isSafeInteger(this.#retryInitialDelayMs) ||
      this.#retryInitialDelayMs < 1 ||
      !Number.isSafeInteger(this.#retryMaxDelayMs) ||
      this.#retryMaxDelayMs < this.#retryInitialDelayMs ||
      !Number.isSafeInteger(this.#metadataAttemptTimeoutMs) ||
      this.#metadataAttemptTimeoutMs < 1 ||
      !Number.isSafeInteger(this.#runtimeCleanupTimeoutMs) ||
      this.#runtimeCleanupTimeoutMs < 1 ||
      !Number.isSafeInteger(this.#bootstrapCleanupTimeoutMs) || this.#bootstrapCleanupTimeoutMs < 1
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
    if (this.#shuttingDown || event.type === "started") return;
    if (this.#pendingRecoveryEvents.length >= MAX_PENDING_FLEET_RECOVERY_EVENTS) {
      // A gap cannot count as repeated identical failure proof. Retain only a
      // new bounded suffix and clear old replacement evidence before reading it.
      // Provider continuation metadata and bootstrap holds are not changed.
      this.#pendingRecoveryEvents.length = 0;
      this.#recoveryEvidenceGap = true;
    }
    this.#pendingRecoveryEvents.push(event);
    this.#yieldStartupWait?.();
    this.#queueBookkeeping();
  }

  #queueBookkeeping(): void {
    if (this.#shuttingDown || this.#bookkeepingQueued) return;
    this.#bookkeepingQueued = true;
    const operation = async () => {
      let failed = false;
      try {
        await this.#commitRuntimeStarts();
        await this.#drainRecoveryEvents();
        this.#pendingMaintenance ||= this.#unavailableProjects.size > 0;
        this.#scheduleRetry();
      } catch {
        failed = true;
        this.#pendingMaintenance = true;
        this.#scheduleRetry();
      } finally {
        this.#bookkeepingQueued = false;
        if (!failed && (this.#pendingRecoveryEvents.length > 0 || this.#recoveryEvidenceGap ||
          [...this.#runtimeStarts.values()].some(start => start.settled))) {
          this.#queueBookkeeping();
        }
      }
    };
    const pending = this.#chain.then(operation, operation);
    this.#chain = pending.then(() => undefined, () => undefined);
    void pending.catch(() => undefined);
  }

  async #drainRecoveryEvents(): Promise<void> {
    const gap = this.#recoveryEvidenceGap;
    this.#recoveryEvidenceGap = false;
    // At most one bounded batch is held while another bounded batch fills.
    const events = this.#pendingRecoveryEvents.splice(0);
    try {
      if (gap) {
        this.#replacementCandidates.clear();
        const manifest = await this.#manifest.load();
        for (const worker of manifest.workers) {
          if (worker.replacementEvidence) {
            await this.#manifest.clearReplacementFailure(worker.projectId, this.#clock.now());
          }
        }
      }
      for (const event of events) await this.#recordRecoveryEvent(event);
    } catch (error) {
      // A failed durable update also leaves a gap in the observed sequence.
      this.#recoveryEvidenceGap = true;
      throw error;
    }
  }

  /** Wakes project maintenance when the shared outage circuit closes early. */
  observeRecoveryCircuit(snapshot: Readonly<FleetRecoverySnapshot>): void {
    if (this.#shuttingDown || snapshot.state !== "closed" || snapshot.scope !== null) return;
    this.#wakeRetry();
  }

  observeConnectivity(): void {
    if (this.#shuttingDown) return;
    this.#refreshSnapshot();
    if (!this.#connectivity?.isRecoveryBlocked()) this.#wakeRetry();
  }

  async #recordRecoveryEvent(event: SandboxSupervisorEvent): Promise<void> {
    if (this.#shuttingDown || event.type === "started") return;
    const current = await this.#manifest.load();
    const currentWorker = current.workers.find(worker => worker.projectId === event.projectId);
    if (!currentWorker) return;
    if (currentWorker.retirementReason === "bootstrap_hold") return;
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
    const possible = manifest.workers
      .filter(worker => worker.state !== "draining" && worker.state !== "ready" && worker.managed &&
        worker.replacementForProjectId != null);
    const possibleIds = new Set(possible.map(worker => worker.projectId));
    for (const id of this.#replacementSuccessors.keys()) {
      if (!possibleIds.has(id)) this.#replacementSuccessors.delete(id);
    }
    for (const worker of possible) {
      if (this.#replacementSuccessors.size >= this.#config.maxProjects) break;
      if (!this.#replacementSuccessors.has(worker.projectId)) {
        this.#replacementSuccessors.set(worker.projectId, {
          created: worker.state !== "creating",
          replacementForProjectId: worker.replacementForProjectId!,
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
    const bootstrapCleanup = this.#bootstrapCleanup;
    if (bootstrapCleanup && !bootstrapCleanup.settled) {
      this.#deferBootstrapCleanup(bootstrapCleanup, "aborted");
      bootstrapCleanup.controller.abort(new DOMException("Fleet shutdown", "AbortError"));
    }
    this.#retryController?.abort();
    this.#activeReconcile?.abort();
    for (const start of this.#runtimeStarts.values()) start.controller.abort();
    this.#pendingRecoveryEvents.length = 0;
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
    if (bootstrapCleanup && !bootstrapCleanup.settled &&
      !await this.#settleWithin(() => bootstrapCleanup.completion, this.#runtimeCleanupTimeoutMs)) {
      failures.push(new FleetReconciliationError("Bootstrap cleanup operation did not stop", "runtime"));
    }
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
    if (failures.length) {
      // The server prints only the outer error. Keep the failed ownership visible
      // without copying nested exception messages or an unbounded fleet listing.
      const failedProjects = runtimes.flatMap((runtime, index) =>
        stopped[index]?.status === "rejected" ? [runtime.projectId] : []
      );
      const shownProjects = failedProjects.slice(0, 8);
      throw new AggregateError(failures,
        `Fleet shutdown was incomplete (failed runtimes: ${failedProjects.length}; ` +
        `remaining runtimes: ${this.#registry.runtimes().length}; ` +
        `projects: ${shownProjects.join(", ") || "none"}${failedProjects.length > shownProjects.length ? ", ..." : ""})`,
      );
    }
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
    this.#yieldStartupWait?.();
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
    this.#startupWaitYielded = new Promise(resolve => { this.#yieldStartupWait = resolve; });
    try {
      await this.#commitRuntimeStarts();
      await this.#drainRecoveryEvents();
      return await this.#reconcile(controller.signal);
    } finally {
      this.#yieldStartupWait = null;
      this.#startupWaitYielded = null;
      signal?.removeEventListener("abort", onAbort);
      if (this.#activeReconcile === controller) this.#activeReconcile = null;
    }
  }

  async #reconcile(signal: AbortSignal): Promise<FleetSnapshot> {
    this.#throwIfAborted(signal);
    let manifest = await this.#manifest.initialize(this.#identity.kind);
    manifest = await this.#importLegacyProjects(manifest, signal);
    manifest = await this.#commitHeldBootstrapCleanup(manifest, signal);
    const needsReplacementSlot = manifest.workers.some(worker =>
      worker.state !== "draining" && worker.replacementEvidence !== null &&
      this.#replacementEvidenceQualifies(worker.replacementEvidence)
    );
    if (manifest.workers.length >= this.#config.maxProjects && needsReplacementSlot) {
      // Exact cleanup has already changed a bootstrap hold to `replacement`.
      // At the total project bound, finalize and retire that proven-dead old
      // project before reserving its successor; waiting for full readiness
      // would deadlock because no free successor slot exists.
      manifest = await this.#cleanupOneRetirement(manifest, signal, true);
    }
    this.#hydrateReplacementCandidates(manifest);
    this.#hydrateReplacementSuccessors(manifest);
    const failedHold: { runtime: RoutableSandboxRuntime | null } = { runtime: null };
    const apply = () => this.#applyReplacementCandidates(manifest, signal, runtime => { failedHold.runtime = runtime; });
    try {
      if (this.#bootstrapMigration) {
        let applying = false;
        try {
          manifest = await this.#bootstrapMigration.withValidatedAuthority(manifest, async authority => {
            applying = true;
            this.#operatorRecoveryProjects = authority.operatorRecoveryProjects;
            return apply();
          });
        } catch (error) {
          if (applying || signal.aborted) throw error;
          // A stale or unavailable private grant removes only migration authority.
          // Ordinary fleet work continues; an entered mutation is never replayed.
          this.#operatorRecoveryProjects = new Set();
          manifest = await apply();
        }
      } else {
        manifest = await apply();
      }
    } finally {
      // A runtime stop can need its own bootstrap journal lease. Migration
      // validation must release that lease before failed-persistence cleanup.
      if (failedHold.runtime) await this.#stopAndRemoveRuntime(failedHold.runtime);
    }
    await this.#fenceHeldProjects(manifest, signal);
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
    const withMutationPermit = concurrencyGate(this.#config.provisionConcurrency);
    await Promise.all(firstWorkers.map(worker =>
      this.#reconcileWorker(
        worker,
        evidence,
        signal,
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
        withMutationPermit,
      )
    ));

    await this.#commitRuntimeStarts();
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
    this.#startHeldBootstrapCleanup(manifest, signal);
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
      !!this.#bootstrapCleanup ||
      (!!this.#cleanupHeldBootstrap && manifest.workers.some(worker => worker.retirementReason === "bootstrap_hold")) ||
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
    deferFailedHoldCleanup: (runtime: RoutableSandboxRuntime) => void,
  ): Promise<FleetManifest> {
    // Failure signatures or a completion may still be queued. Do not act on
    // older evidence until the serialized bookkeeping has observed that suffix.
    if (this.#recoveryEvidenceGap || this.#pendingRecoveryEvents.length > 0) return manifest;
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
      const hold = evidence?.kind === "bootstrap_blocked";
      const runtime = hold ? this.#registry.get(id) : undefined;
      // Fence admission synchronously after the zero-work check. A recovered
      // heartbeat must not make the old project routable while the hold is saved.
      if (runtime) this.#observeRuntimeStop(runtime, this.#runtimeCleanupTimeoutMs);
      try {
        current = await this.#manifest.markDraining(id,
          hold ? "bootstrap_hold" : "replacement", this.#clock.now());
      } catch (error) {
        if (runtime) deferFailedHoldCleanup(runtime);
        throw error;
      }
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
    const hold = worker.replacementEvidence?.kind === "bootstrap_blocked";
    if (hold && (!worker.managed || this.#config.v2SidecarRevision !== "r10")) {
      return this.#blockMutation(worker, "replace", "release_policy");
    }
    if (hold) {
      // Holds retain the exact old project, journal and continuation bindings.
      // Account growth is bounded by the configured total project budget,
      // including every active worker and every retained hold.
      if (manifest.workers.length >= Math.min(this.#config.maxProjects, MAX_MANIFEST_WORKERS)) {
        return this.#blockMutation(worker, "replace", "bootstrap_hold_limit");
      }
    }
    // A hold never retires the project or its continuation metadata. Destructive
    // replacement retains the stricter continuation-retirement gate.
    if (!hold) {
      try {
        if (!this.#canRetireProject(worker.projectId)) {
          return this.#blockMutation(worker, "replace", "continuation_binding");
        }
      } catch {
        return this.#blockMutation(worker, "replace", "continuation_binding");
      }
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
    // A bootstrap hold is non-destructive. Below majority, admit only one
    // successor canary at a time; another hold requires that canary to reach
    // verified readiness first. This restores a fleet after a common outage
    // without deleting, reusing or replaying the old project generation.
    const sequentialBootstrapHold = hold && this.#replacementSuccessors.size === 0;
    if (healthyPeers < Math.floor(this.#desiredSandboxes / 2) + 1 &&
      !(hold && this.#operatorRecoveryProjects.has(worker.projectId)) &&
      !sequentialBootstrapHold) {
      return this.#blockMutation(worker, "replace", "healthy_majority");
    }
    this.#clearMutationBlock(worker.projectId, "replace");
    return true;
  }

  async #fenceHeldProjects(manifest: FleetManifest, signal: AbortSignal): Promise<void> {
    const held = manifest.workers.filter(worker => worker.retirementReason === "bootstrap_hold");
    this.#heldProjects = Object.freeze(held.map(worker => worker.projectId));
    for (const worker of held) {
      this.#throwIfAborted(signal);
      this.#unavailableProjects.delete(worker.projectId);
      // Stop only local routing/recovery ownership. The original project and
      // private bootstrap receipt remain intact even when remote cleanup fails.
      const runtime = this.#registry.get(worker.projectId);
      if (runtime) {
        const stop = this.#observeRuntimeStop(runtime, this.#runtimeCleanupTimeoutMs);
        const snapshot = runtime.snapshot();
        const locallyFenced = stop !== undefined && snapshot.state === "stopped" && snapshot.capacity === 0 && snapshot.available === 0 &&
          snapshot.active === 0 && snapshot.quarantined === 0 && (snapshot.ambiguousQuarantined ?? 0) === 0 &&
          !this.#runtimeStarts.has(runtime);
        // Remote cleanup can remain unresolved after the permanent local stop.
        // Keep that exact owner registered, and keep its hold/journal, while
        // independent workers and a separate successor recover fleet capacity.
        const physicallyComplete = this.#runtimeLocalCleanupComplete(runtime);
        if ((stop?.state === "fulfilled" && physicallyComplete && !this.#runtimeCleanupProofRequired(runtime)) || !locallyFenced) {
          if (!await this.#stopAndRemoveRuntime(runtime)) {
            throw new FleetReconciliationError("Held project could not be fenced locally", "runtime");
          }
        }
      }
      this.#blockMutation(worker, "delete", "bootstrap_cleanup");
    }
  }

  #reportBootstrapCleanupDeferred(worker: Readonly<FleetWorkerRecord>, reason: FleetBootstrapCleanupReason,
    diagnostic?: FleetBootstrapCleanupDiagnostic): void {
    const key = `${worker.projectId}:${worker.leaderGeneration}:${reason}`;
    if (this.#lastCleanupDeferral === key) return;
    this.#lastCleanupDeferral = key;
    this.#emitLifecycle({ type: "bootstrap_cleanup_deferred", projectId: worker.projectId,
      workerIndex: worker.index, leaderGeneration: worker.leaderGeneration, reason, ...diagnostic });
  }

  #deferBootstrapCleanup(operation: BootstrapCleanupOperation, reason: FleetBootstrapCleanupReason,
    diagnostic?: FleetBootstrapCleanupDiagnostic): void {
    if (operation.deferredReason !== null) return;
    operation.deferredReason = reason;
    this.#reportBootstrapCleanupDeferred(operation.worker, reason, diagnostic);
  }

  #beginBootstrapCleanup(kind: BootstrapCleanupOperation["kind"], worker: Readonly<FleetWorkerRecord>): void {
    const controller = new AbortController();
    const timeoutController = new AbortController();
    const operation: BootstrapCleanupOperation = { kind, worker, controller,
      runtimeOwner: this.#registry.get(worker.projectId) ?? null,
      deadlineMs: this.#clock.now() + this.#bootstrapCleanupTimeoutMs,
      completion: Promise.resolve(), settled: false, succeeded: false, deferredReason: null };
    this.#bootstrapCleanup = operation;
    this.#lastCleanupDeferral = null;
    this.#emitLifecycle({ type: "bootstrap_cleanup_started", projectId: worker.projectId,
      workerIndex: worker.index, leaderGeneration: worker.leaderGeneration });
    void this.#clock.waitUntil(operation.deadlineMs, timeoutController.signal).then(() => {
      if (operation.settled) return;
      this.#deferBootstrapCleanup(operation, "timeout");
      controller.abort(new DOMException("Bootstrap cleanup deadline expired", "TimeoutError"));
    }, () => undefined);
    // Retain this one owner after timeout until its actual operation settles.
    // A disconnected or uncooperative cleanup cannot spawn accumulating retries.
    operation.completion = Promise.resolve().then(async () => {
      controller.signal.throwIfAborted();
      if (kind === "prove") return this.#cleanupHeldBootstrap!(worker, controller.signal);
      await this.#finalizeHeldBootstrapCleanup!(worker, controller.signal);
      return true;
    }).then(proven => {
      if (this.#clock.now() >= operation.deadlineMs) this.#deferBootstrapCleanup(operation, "timeout");
      if (this.#shuttingDown || controller.signal.aborted) this.#deferBootstrapCleanup(operation, "aborted");
      if (proven !== true) this.#deferBootstrapCleanup(operation, "proof_unavailable", { cleanupStage: "proof_returned_false" });
      operation.succeeded = proven === true && operation.deferredReason === null;
    }, error => {
      this.#deferBootstrapCleanup(operation, controller.signal.aborted
        ? "aborted" : kind === "prove" ? "proof_unavailable" : "cleanup_failed", {
        cleanupStage: kind === "prove" ? "proof_threw" : "finalize_threw",
        ...bootstrapCleanupFailureDiagnostic(error),
      });
    }).finally(() => {
      operation.settled = true;
      timeoutController.abort();
      this.#bootstrapCleanupRetryAtMs = this.#clock.now() + (operation.succeeded ? 0 : this.#retryMaxDelayMs);
      if (!this.#shuttingDown) {
        this.#pendingMaintenance = true;
        this.#wakeRetry();
        this.#scheduleRetry();
      }
    });
  }

  async #commitHeldBootstrapCleanup(manifest: FleetManifest, signal: AbortSignal): Promise<FleetManifest> {
    const operation = this.#bootstrapCleanup;
    if (!operation?.settled) return manifest;
    this.#bootstrapCleanup = null;
    if (!operation.succeeded) return manifest;
    this.#throwIfAborted(signal);
    const worker = manifest.workers.find(value => value.projectId === operation.worker.projectId);
    const runtime = this.#registry.get(operation.worker.projectId) ?? null;
    if (!worker || !sameFleetWorkerRecord(worker, operation.worker) || runtime !== operation.runtimeOwner ||
      (runtime && !this.#heldRuntimeCleanupReady(runtime))) {
      this.#reportBootstrapCleanupDeferred(operation.worker, "worker_changed");
      return manifest;
    }
    if (operation.kind === "finalize") {
      this.#finalizedBootstrapRetirement = operation.worker;
      return manifest;
    }
    try {
      // Exact proof has settled, but the original hold still witnesses one-shot
      // grant consumption. Persist that fact before retirement can clear it.
      await this.#bootstrapMigration?.recordConsumedHold(operation.worker, signal);
      if (runtime) {
        // The port has persisted the exact journal's remote cleanup proof.
        // Release this registration only after its independent physical-local
        // witness, never merely because its bounded public stop rejected.
        await this.#registry.removeStopped(runtime.projectId);
        this.#runtimeStops.delete(runtime);
      }
      this.#throwIfAborted(signal);
      const next = await this.#manifest.completeBootstrapHoldCleanup(operation.worker, this.#clock.now());
      this.#clearMutationBlock(worker.projectId);
      this.#emitLifecycle({ type: "bootstrap_cleanup_succeeded", projectId: worker.projectId,
        workerIndex: worker.index, leaderGeneration: worker.leaderGeneration });
      return next;
    } catch (error) {
      this.#reportBootstrapCleanupDeferred(operation.worker, "worker_changed");
      throw error;
    }
  }

  #startHeldBootstrapCleanup(manifest: FleetManifest, signal: AbortSignal): void {
    if (!this.#cleanupHeldBootstrap || this.#bootstrapCleanup || this.#clock.now() < this.#bootstrapCleanupRetryAtMs) return;
    this.#throwIfAborted(signal);
    const held = manifest.workers.filter(worker => worker.managed && worker.retirementReason === "bootstrap_hold")
      .sort((left, right) => left.index - right.index);
    const after = held.findIndex(value => value.index > this.#lastHeldCleanupIndex);
    const ordered = after < 1 ? held : [...held.slice(after), ...held.slice(0, after)];
    // An exact runtime owner may remain after remote cleanup failed. Preserve
    // it, but do not let it starve cleanup proof for independent held projects.
    const worker = ordered.find(value => {
      const runtime = this.#registry.get(value.projectId);
      return !runtime || this.#heldRuntimeCleanupReady(runtime);
    });
    if (!worker && held[0]) this.#reportBootstrapCleanupDeferred(held[0], "work_in_flight");
    if (!worker) return;
    const gate = this.#mutationGateBlockReason();
    if (gate) { this.#reportBootstrapCleanupDeferred(worker, gate); return; }
    if (this.#config.v2SidecarRevision !== "r10") {
      this.#reportBootstrapCleanupDeferred(worker, "release_policy"); return;
    }
    // This only proves the already-recorded supervisor's exact cleanup. It
    // cannot launch, create, or delete a project. Requiring a healthy majority
    // here deadlocks a degraded fleet with a full hold budget before any proof
    // can be obtained. Replacement and project deletion retain their own gates.
    this.#lastHeldCleanupIndex = worker.index;
    this.#beginBootstrapCleanup("prove", worker);
  }

  #heldRuntimeCleanupReady(runtime: RoutableSandboxRuntime): boolean {
    const state = this.#runtimeStops.get(runtime)?.state;
    if ((state !== "rejected" && !(state === "fulfilled" && this.#runtimeCleanupProofRequired(runtime))) ||
      this.#runtimeStarts.has(runtime)) return false;
    try {
      const snapshot = runtime.snapshot();
      return snapshot.state === "stopped" && snapshot.capacity === 0 && snapshot.available === 0 &&
        snapshot.active === 0 && snapshot.quarantined === 0 && (snapshot.ambiguousQuarantined ?? 0) === 0 &&
        runtime.localCleanupComplete?.() === true;
    } catch { return false; }
  }

  #runtimeLocalCleanupComplete(runtime: RoutableSandboxRuntime): boolean {
    try { return runtime.localCleanupComplete ? runtime.localCleanupComplete() : true; }
    catch { return false; }
  }

  #runtimeCleanupProofRequired(runtime: RoutableSandboxRuntime): boolean {
    try { return runtime.cleanupProofRequired?.() === true; }
    catch { return true; }
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
      if (active >= this.#config.maxProjects || current.workers.length >= this.#config.maxProjects) {
        const source = current.workers
          .filter(worker => worker.state === "draining" &&
            (worker.retirementReason === "bootstrap_hold" || worker.retirementReason === "replacement" ||
              worker.retirementReason === "missing"))
          .sort((left, right) => right.updatedAtMs - left.updatedAtMs || right.index - left.index)[0];
        if (source) this.#blockMutation(source, "create", "bootstrap_hold_limit");
        break;
      }
      const index = nextWorkerIndex(current.workers);
      const id = this.#newProjectId(current.workers);
      const automaticRetirements = current.workers.filter(worker =>
        worker.state === "draining" &&
        (worker.retirementReason === "replacement" || worker.retirementReason === "missing" || worker.retirementReason === "bootstrap_hold")
      ).sort((left, right) => right.updatedAtMs - left.updatedAtMs || right.index - left.index);
      const trackedSources = new Set(
        [...this.#replacementSuccessors.values()].map(value => value.replacementForProjectId),
      );
      const replacementSource = automaticRetirements.find(worker =>
        !trackedSources.has(worker.projectId)
      ) ?? null;
      current = await this.#manifest.reserveWorker({
        index,
        projectId: id,
        title: `GhostProv Worker ${index}`,
        nowMs: this.#clock.now(),
        managed: true,
        ...(replacementSource ? { replacementForProjectId: replacementSource.projectId } : {}),
      });
      if (replacementSource && this.#replacementSuccessors.size < this.#config.maxProjects) {
        this.#replacementSuccessors.set(id, {
          created: false,
          replacementForProjectId: replacementSource.projectId,
        });
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
        return;
      }
      // A terminally failed bootstrap owns no provider execution and no
      // transport work. Ordinary recovery retries the same registered project
      // with a new generation; the supervisor journal still proves cleanup of
      // any ambiguous prior submission before a new launch. Holds stay fenced
      // for successor restoration and never restart in place.
      if (worker.state !== "draining" && worker.retirementReason !== "bootstrap_hold" &&
        !this.#runtimeStarts.has(existing) &&
        (snapshot.state === "failed" || snapshot.state === "stopped") &&
        snapshot.active === 0 && snapshot.quarantined === 0 &&
        (snapshot.ambiguousQuarantined ?? 0) === 0) {
        if (await this.#stopAndRemoveRuntime(existing)) {
          this.#unavailableProjects.add(worker.projectId);
        } else {
          this.#unavailableProjects.add(worker.projectId);
          return;
        }
      } else {
        this.#unavailableProjects.add(worker.projectId);
        return;
      }
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
    if (this.#runtimeStarts.size >= MAX_MANIFEST_WORKERS ||
      this.#runtimeStops.size >= MAX_MANIFEST_WORKERS) {
      this.#unavailableProjects.add(worker.projectId);
      return;
    }
    const generation = await this.#manifest.nextLeaderGeneration(worker.projectId, this.#clock.now());
    this.#throwIfAborted(signal);
    if (this.#runtimeStarts.size >= MAX_MANIFEST_WORKERS ||
      this.#runtimeStops.size >= MAX_MANIFEST_WORKERS) {
      this.#unavailableProjects.add(worker.projectId);
      return;
    }
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
    this.#unavailableProjects.add(worker.projectId);
    await this.#startRuntime(runtime, worker, generation, signal);
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
      this.#connectivity?.requestCheck();
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
    if (this.#connectivity?.isRecoveryBlocked()) return "recovery_circuit";
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
    provenBootstrapOnly = false,
  ): Promise<FleetManifest> {
    const worker = manifest.workers
      .filter(value => value.state === "draining" &&
        (value.retirementReason === "replacement" ||
          value.retirementReason === "missing") &&
        (!provenBootstrapOnly ||
          (value.retirementReason === "replacement" &&
            value.replacementEvidence?.kind === "bootstrap_blocked")))
      .sort((left, right) => left.updatedAtMs - right.updatedAtMs || left.index - right.index)[0];
    if (!worker) return manifest;
    if (manifest.workers.some(candidate =>
      candidate.replacementForProjectId === worker.projectId
    )) {
      this.#blockMutation(worker, "delete", "work_in_flight");
      return manifest;
    }
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
    if (this.#finalizeHeldBootstrapCleanup) {
      const finalized = this.#finalizedBootstrapRetirement;
      if (!finalized || !sameFleetWorkerRecord(worker, finalized)) {
        if (this.#bootstrapCleanup) return manifest;
        if (this.#clock.now() < this.#bootstrapCleanupRetryAtMs) return manifest;
        this.#beginBootstrapCleanup("finalize", worker);
        return manifest;
      }
      this.#finalizedBootstrapRetirement = null;
      this.#throwIfAborted(signal);
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

  async #startRuntime(
    runtime: RoutableSandboxRuntime,
    worker: FleetWorkerRecord,
    generation: number,
    signal: AbortSignal,
  ): Promise<void> {
    this.#throwIfAborted(signal);
    const controller = new AbortController();
    const cancelStart = () => controller.abort();
    signal.addEventListener("abort", cancelStart, { once: true });
    const start: RuntimeStart = {
      runtime,
      worker,
      generation,
      controller,
      unlinkAbort: () => signal.removeEventListener("abort", cancelStart),
      completion: Promise.resolve(),
      settled: false,
      succeeded: false,
    };
    this.#runtimeStarts.set(runtime, start);
    // The permit belongs to the actual start, including a late non-cooperative
    // completion. Yielding the reconciliation does not release it or replay it.
    start.completion = this.#withProvisionPermit(async () => {
      if (controller.signal.aborted) return;
      try {
        await runtime.start();
      } finally {
        if (controller.signal.aborted) await this.#stopAndRemoveRuntime(runtime);
      }
    }).then(() => { start.succeeded = true; }, () => undefined).finally(() => {
      start.settled = true;
      if (this.#shuttingDown) {
        start.unlinkAbort();
        this.#runtimeStarts.delete(runtime);
      } else {
        this.#queueBookkeeping();
      }
    });
    let rejectAbort!: (error: DOMException) => void;
    const aborted = new Promise<never>((_resolve, reject) => { rejectAbort = reject; });
    const onAbort = () => rejectAbort(
      new DOMException("Fleet reconciliation aborted", "AbortError"),
    );
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
    try {
      await Promise.race([
        start.completion,
        aborted,
        ...(this.#startupWaitYielded ? [this.#startupWaitYielded] : []),
      ]);
      this.#throwIfAborted(signal);
    } catch (error) {
      if (!signal.aborted) throw error;
      controller.abort();
      await this.#stopAndRemoveRuntime(runtime);
      throw error;
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  async #commitRuntimeStarts(): Promise<void> {
    for (const [runtime, start] of this.#runtimeStarts) {
      if (!start.settled) continue;
      this.#runtimeStarts.delete(runtime);
      start.unlinkAbort();
      if (this.#shuttingDown || start.controller.signal.aborted ||
        this.#registry.get(runtime.projectId) !== runtime) continue;
      const current = (await this.#manifest.load()).workers.find(worker =>
        worker.projectId === runtime.projectId);
      if (!current || current.state === "draining" ||
        current.leaderGeneration !== start.generation) continue;
      if (start.succeeded && isRuntimeHealthy(runtime.snapshot())) {
        await this.#manifest.markState(runtime.projectId, "ready", this.#clock.now());
        this.#unavailableProjects.delete(runtime.projectId);
        this.#activateSuccessor(start.worker, start.generation);
      } else {
        await this.#manifest.markState(runtime.projectId, "failed", this.#clock.now());
        this.#unavailableProjects.add(runtime.projectId);
      }
    }
  }

  async #stopAndRemoveRuntime(
    runtime: RoutableSandboxRuntime,
    timeoutMs = this.#runtimeCleanupTimeoutMs,
  ): Promise<boolean> {
    this.#runtimeStarts.get(runtime)?.controller.abort();
    // A supervisor fences routing by reporting stopped before its asynchronous
    // cleanup finishes. Await the idempotent stop operation as cleanup proof.
    const stop = this.#observeRuntimeStop(runtime, timeoutMs);
    if (!stop) return false;
    if (stop.state === "pending" && !await stop.observation) return false;
    if (stop.state !== "fulfilled" || runtime.snapshot().state !== "stopped" ||
      !this.#runtimeLocalCleanupComplete(runtime) || this.#runtimeCleanupProofRequired(runtime)) return false;
    if (this.#registry.get(runtime.projectId) === runtime) {
      await this.#registry.removeStopped(runtime.projectId).catch(() => undefined);
    }
    const removed = this.#registry.get(runtime.projectId) !== runtime;
    if (removed) this.#runtimeStops.delete(runtime);
    return removed;
  }

  #observeRuntimeStop(runtime: RoutableSandboxRuntime, timeoutMs: number): RuntimeStop | undefined {
    this.#runtimeStarts.get(runtime)?.controller.abort();
    let stop = this.#runtimeStops.get(runtime);
    if (!stop) {
      // New starts stop reserving at half this bound, leaving room to fence all
      // currently registered/starting runtimes even if older cleanup is stuck.
      if (this.#runtimeStops.size >= MAX_RUNTIME_STOP_OBSERVATIONS) return undefined;
      stop = { state: "pending", observation: Promise.resolve(false) };
      this.#runtimeStops.set(runtime, stop);
      const observation = stop;
      // stop() synchronously fences admission before a durable hold is saved.
      // Observe it once, including terminal rejection, rather than calling an
      // already-failed owner again on every reconciliation.
      let operation: Promise<void>;
      try { operation = Promise.resolve(runtime.stop()); }
      catch (error) { operation = Promise.reject(error); }
      const stopping = operation.then(
        () => { observation.state = "fulfilled"; },
        error => { observation.state = "rejected"; throw error; },
      ).finally(() => {
        if (!this.#shuttingDown) {
          this.#pendingMaintenance = true;
          this.#scheduleRetry();
        }
      });
      // Install the completion handler and timeout only once. Reconciliation
      // reuses this settled timeout while the real cleanup remains pending.
      stop.observation = this.#settleWithin(() => stopping, timeoutMs);
    }
    return stop;
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
    const circuitOpen = this.#connectivity?.isRecoveryBlocked() || (circuit !== undefined &&
      (circuit.state !== "closed" || circuit.scope !== null));
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
      ...(this.#heldProjects.length > 0 ? { heldProjects: this.#heldProjects } : {}),
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
