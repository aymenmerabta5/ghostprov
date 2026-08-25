import type { MultiSandboxConfig } from "../config/multi-sandbox";
import {
  SYSTEM_CLOCK,
  type Clock,
} from "../core/ports";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { PrismProjectSummary } from "../prism/projects-client";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
} from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";
import { isRuntimeHealthy } from "../router/policy";
import { FleetManifestStore } from "./manifest";
import type {
  FleetSnapshot,
  FleetWorkerRecord,
  ManagedRuntimeFactory,
} from "./types";

export interface FleetProjectsClient {
  get(id: ProjectId, signal?: AbortSignal): Promise<PrismProjectSummary>;
}

export interface FleetIdentity {
  kind: PrismIdentityKind;
  fingerprint: string;
}

export type FleetReconciliationErrorKind =
  | "explicit_project_missing"
  | "runtime";

export class FleetReconciliationError extends Error {
  readonly name = "FleetReconciliationError";

  constructor(
    message: string,
    readonly kind: FleetReconciliationErrorKind,
  ) {
    super(message);
  }
}

type ProjectReconcileOutcome =
  | { kind: "registered"; projectId: ProjectId }
  | { kind: "metadata_unavailable"; projectId: ProjectId }
  | { kind: "runtime_unavailable"; projectId: ProjectId; error: unknown };

const DEFAULT_RETRY_INITIAL_DELAY_MS = 500;
const DEFAULT_RETRY_MAX_DELAY_MS = 30_000;
const DEFAULT_METADATA_ATTEMPT_TIMEOUT_MS = 15_000;
const RETRY_JITTER_RATIO = 0.2;

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

async function allOrThrow<T>(operations: readonly Promise<T>[]): Promise<T[]> {
  const settled = await Promise.allSettled(operations);
  const failed = settled.find(
    (value): value is PromiseRejectedResult => value.status === "rejected",
  );
  if (failed) throw failed.reason;
  return settled.map(value => (value as PromiseFulfilledResult<T>).value);
}

export interface ProjectFleetManagerOptions {
  config: MultiSandboxConfig;
  identity: FleetIdentity;
  projects: FleetProjectsClient;
  manifest: FleetManifestStore;
  registry: SandboxRegistry;
  runtimeFactory: ManagedRuntimeFactory;
  clock?: Clock;
  /** Test seam; production uses a 500 ms through 30 s bounded backoff. */
  retryInitialDelayMs?: number;
  /** Test seam; must be at least retryInitialDelayMs. */
  retryMaxDelayMs?: number;
  /** Test seam for retry jitter. The returned value is clamped to [0, 1]. */
  random?: () => number;
  /** Bounds one project metadata attempt, including a non-cooperative client. */
  metadataAttemptTimeoutMs?: number;
  /** Bounds graceful drain and forced-stop operations during cleanup. */
  runtimeCleanupTimeoutMs?: number;
}

export class ProjectFleetManager {
  readonly #config: MultiSandboxConfig;
  readonly #identity: FleetIdentity;
  readonly #projects: FleetProjectsClient;
  readonly #manifest: FleetManifestStore;
  readonly #registry: SandboxRegistry;
  readonly #runtimeFactory: ManagedRuntimeFactory;
  readonly #clock: Clock;
  readonly #retryInitialDelayMs: number;
  readonly #retryMaxDelayMs: number;
  readonly #random: () => number;
  readonly #metadataAttemptTimeoutMs: number;
  readonly #runtimeCleanupTimeoutMs: number;
  readonly #unavailableProjects = new Set<ProjectId>();
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
    this.#projects = options.projects;
    this.#manifest = options.manifest;
    this.#registry = options.registry;
    this.#runtimeFactory = options.runtimeFactory;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#retryInitialDelayMs = options.retryInitialDelayMs ?? DEFAULT_RETRY_INITIAL_DELAY_MS;
    this.#retryMaxDelayMs = options.retryMaxDelayMs ?? DEFAULT_RETRY_MAX_DELAY_MS;
    this.#random = options.random ?? Math.random;
    this.#metadataAttemptTimeoutMs = options.metadataAttemptTimeoutMs ??
      DEFAULT_METADATA_ATTEMPT_TIMEOUT_MS;
    this.#runtimeCleanupTimeoutMs = options.runtimeCleanupTimeoutMs ??
      this.#config.shutdownDrainMs;
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
      desired: this.#config.projectIds.length,
      projects: Object.freeze([...this.#config.projectIds]),
    });
  }

  reconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
    return this.#enqueueReconcile(this.#config.projectIds, signal);
  }

  #enqueueReconcile(
    projectIds: readonly ProjectId[],
    signal?: AbortSignal,
  ): Promise<FleetSnapshot> {
    if (this.#shuttingDown) {
      return Promise.reject(new FleetReconciliationError(
        "Fleet manager is shutting down",
        "runtime",
      ));
    }
    const pending = this.#chain.then(
      () => this.#runReconcile(projectIds, signal),
      () => this.#runReconcile(projectIds, signal),
    );
    this.#chain = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async waitForReady(
    minimum: number,
    signal?: AbortSignal,
  ): Promise<FleetSnapshot> {
    if (!Number.isInteger(minimum) || minimum < 1) {
      throw new RangeError("minimum ready runtimes must be positive");
    }
    this.#refreshSnapshot(this.#snapshot.desired, this.#snapshot.projects);
    if (this.#snapshot.ready >= minimum) return this.#snapshot;
    if (signal?.aborted) throw new DOMException("Fleet readiness wait aborted", "AbortError");
    return new Promise<FleetSnapshot>((resolve, reject) => {
      const unsubscribe = this.#registry.subscribe(() => {
        this.#refreshSnapshot(this.#snapshot.desired, this.#snapshot.projects);
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
    await Promise.allSettled([
      this.#chain,
      this.#retryTask ?? Promise.resolve(),
    ]);
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
      this.#refreshSnapshot(0, []);
    } else {
      this.#refreshSnapshot(this.#config.projectIds.length, this.#config.projectIds);
      failures.push(new FleetReconciliationError(
        "Fleet shutdown left registered runtimes",
        "runtime",
      ));
    }
    if (failures.length) throw new AggregateError(failures, "Fleet shutdown was incomplete");
  }

  snapshot(): FleetSnapshot {
    return this.#snapshot;
  }

  async #reconcile(
    projectIds: readonly ProjectId[],
    signal: AbortSignal,
  ): Promise<FleetSnapshot> {
    if (signal?.aborted) throw new DOMException("Fleet reconciliation aborted", "AbortError");
    let manifest = await this.#manifest.initialize(this.#identity.kind);
    if (signal?.aborted || this.#shuttingDown) {
      throw new DOMException("Fleet reconciliation aborted", "AbortError");
    }
    const desired = this.#config.projectIds.length;
    if (this.#identity.kind === "anonymous" && desired > this.#config.guestMaxProjects) {
      throw new FleetReconciliationError(
        `Configured ${desired} projects exceeds anonymous fleet limit ${this.#config.guestMaxProjects}`,
        "runtime",
      );
    }
    this.#refreshSnapshot(desired, this.#config.projectIds);
    const configuredIds = new Set(this.#config.projectIds);
    // The router reads this registry directly. Retire every non-allowlisted
    // runtime before any configured runtime can publish readiness; otherwise a
    // fast configured start could bind the server while an excess runtime is
    // still routable.
    await this.#drainExcess(configuredIds, signal);
    if (signal?.aborted || this.#shuttingDown) {
      throw new DOMException("Fleet reconciliation aborted", "AbortError");
    }
    let reservationChain: Promise<void> = Promise.resolve();
    const ensureWorker = (project: PrismProjectSummary): Promise<FleetWorkerRecord> => {
      let selected!: FleetWorkerRecord;
      const reserve = async (): Promise<void> => {
        if (signal?.aborted || this.#shuttingDown) {
          throw new DOMException("Fleet reconciliation aborted", "AbortError");
        }
        manifest = await this.#manifest.load();
        let worker = manifest.workers.find(value => value.projectId === project.projectId);
        if (!worker) {
          const index = nextWorkerIndex(manifest.workers);
          manifest = await this.#manifest.reserveWorker({
            index,
            projectId: project.projectId,
            title: project.title,
            nowMs: this.#clock.now(),
            managed: false,
          });
          manifest = await this.#manifest.markCreated(project.projectId, this.#clock.now());
          worker = manifest.workers.find(value => value.projectId === project.projectId)!;
        }
        selected = worker;
      };
      const pending = reservationChain.then(reserve, reserve);
      reservationChain = pending.then(() => undefined, () => undefined);
      return pending.then(() => selected);
    };

    const withProvisionPermit = concurrencyGate(this.#config.provisionConcurrency);
    // Start every bounded, explicitly configured metadata read immediately.
    // Provisioning/startup remains separately limited, so slow leading reads
    // cannot occupy every provisioning slot and hide later healthy projects.
    const projectReconciliation = allOrThrow(projectIds.map(
      async (id): Promise<ProjectReconcileOutcome> => {
        let project: PrismProjectSummary;
        try {
          project = await this.#getProject(id, signal);
        } catch (error) {
          if (signal?.aborted || this.#shuttingDown) {
            throw new DOMException("Fleet reconciliation aborted", "AbortError");
          }
          this.#markUnavailable(id);
          return { kind: "metadata_unavailable", projectId: id };
        }
        if (project.deleted || project.projectId !== id) {
          this.#markUnavailable(id);
          return { kind: "metadata_unavailable", projectId: id };
        }
        return withProvisionPermit(async () => {
          try {
            const worker = await ensureWorker(project);
            if (signal?.aborted || this.#shuttingDown) {
              throw new DOMException("Fleet reconciliation aborted", "AbortError");
            }
            const existing = this.#registry.get(worker.projectId);
            if (existing) {
              this.#unavailableProjects.delete(id);
              return { kind: "registered", projectId: id };
            }
            const generation = await this.#manifest.nextLeaderGeneration(
              worker.projectId,
              this.#clock.now(),
            );
            if (signal?.aborted || this.#shuttingDown) {
              throw new DOMException("Fleet reconciliation aborted", "AbortError");
            }
            const runtime = this.#runtimeFactory.create({
              projectId: worker.projectId,
              workerIndex: worker.index,
              leaderGeneration: generation,
            });
            if (runtime.projectId !== worker.projectId) {
              await this.#stopAndRemoveRuntime(runtime);
              throw new FleetReconciliationError(
                `Runtime factory returned project ${runtime.projectId} for ${worker.projectId}`,
                "runtime",
              );
            }
            this.#registry.add(runtime);
            try {
              await this.#startRuntime(runtime, signal);
              await this.#manifest.markState(runtime.projectId, "ready", this.#clock.now());
            } catch (error) {
              if (signal?.aborted || this.#shuttingDown) throw error;
              await this.#manifest.markState(runtime.projectId, "failed", this.#clock.now());
            }
            this.#unavailableProjects.delete(id);
            return { kind: "registered", projectId: id };
          } catch (error) {
            if (signal?.aborted || this.#shuttingDown) {
              if (!this.#shuttingDown && !this.#registry.get(id)) {
                this.#unavailableProjects.add(id);
              }
              throw error;
            }
            // A verified explicit project whose local setup failed remains an
            // exact pending project. The retry loop never substitutes or creates
            // a Prism project.
            this.#unavailableProjects.add(id);
            return { kind: "runtime_unavailable", projectId: id, error };
          }
        });
      },
    ));
    const outcomes = await projectReconciliation;

    this.#refreshSnapshot(
      desired,
      this.#config.projectIds,
    );
    const hasConfiguredRuntime = this.#registry.runtimes().some(
      runtime => configuredIds.has(runtime.projectId),
    );
    if (!hasConfiguredRuntime) {
      const runtimeFailure = outcomes.find(
        (outcome): outcome is Extract<ProjectReconcileOutcome, { kind: "runtime_unavailable" }> =>
          outcome.kind === "runtime_unavailable",
      );
      if (runtimeFailure) {
        throw new FleetReconciliationError(
          `Explicit Prism project runtime is unavailable: ${runtimeFailure.projectId}`,
          "runtime",
        );
      }
      throw new FleetReconciliationError(
        `Explicit Prism projects are unavailable: ${this.#config.projectIds.join(", ")}`,
        "explicit_project_missing",
      );
    }
    this.#scheduleRetry();
    return this.#snapshot;
  }

  async #runReconcile(
    projectIds: readonly ProjectId[],
    signal?: AbortSignal,
  ): Promise<FleetSnapshot> {
    if (this.#shuttingDown) {
      throw new FleetReconciliationError("Fleet manager is shutting down", "runtime");
    }
    const controller = new AbortController();
    const onAbort = () => controller.abort();
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) controller.abort();
    this.#activeReconcile = controller;
    try {
      return await this.#reconcile(projectIds, controller.signal);
    } finally {
      signal?.removeEventListener("abort", onAbort);
      if (this.#activeReconcile === controller) this.#activeReconcile = null;
    }
  }

  async #drainExcess(
    selected: ReadonlySet<ProjectId>,
    signal: AbortSignal,
  ): Promise<void> {
    const excess = [...this.#registry.runtimes()]
      .filter(runtime => !selected.has(runtime.projectId))
      .sort((left, right) => right.projectId.localeCompare(left.projectId));
    for (const runtime of excess) {
      const record = (await this.#manifest.load()).workers.find(
        worker => worker.projectId === runtime.projectId,
      );
      if (record) {
        await this.#manifest.markState(runtime.projectId, "draining", this.#clock.now());
      }
      await this.#drainExcessRuntime(runtime, signal);
      if (record) {
        await this.#manifest.markState(runtime.projectId, "created", this.#clock.now());
      }
    }
  }

  async #getProject(
    id: ProjectId,
    signal: AbortSignal,
  ): Promise<PrismProjectSummary> {
    if (signal.aborted) throw new DOMException("Fleet reconciliation aborted", "AbortError");
    const attempt = new AbortController();
    const reading = Promise.resolve().then(() => this.#projects.get(id, attempt.signal));
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
        reject(new FleetReconciliationError(
          `Explicit Prism project metadata timed out: ${id}`,
          "explicit_project_missing",
        ));
      }, this.#metadataAttemptTimeoutMs);
      timeout.unref?.();
    });
    try {
      return await Promise.race([reading, aborted, timedOut]);
    } finally {
      if (timeout !== undefined) clearTimeout(timeout);
      signal.removeEventListener("abort", onAbort);
      attempt.abort();
    }
  }

  async #drainExcessRuntime(
    runtime: RoutableSandboxRuntime,
    signal: AbortSignal,
  ): Promise<void> {
    if (signal.aborted) throw new DOMException("Fleet reconciliation aborted", "AbortError");
    const draining = Promise.resolve().then(() =>
      this.#registry.drain(runtime.projectId, "fleet-reconcile")
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
        `Excess runtime could not be retired: ${runtime.projectId}`,
        "runtime",
      );
    }
  }

  async #startRuntime(
    runtime: RoutableSandboxRuntime,
    signal: AbortSignal,
  ): Promise<void> {
    if (signal.aborted) throw new DOMException("Fleet reconciliation aborted", "AbortError");
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
      if (signal.aborted) {
        throw new DOMException("Fleet reconciliation aborted", "AbortError");
      }
    } catch (error) {
      if (!signal.aborted) throw error;
      await this.#stopAndRemoveRuntime(runtime);
      // A non-cooperative start may settle after stop. Fence that late
      // settlement with another idempotent stop/removal without blocking the
      // bounded shutdown path on the original start promise.
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

  async #settleWithin(
    operation: () => Promise<void>,
    timeoutMs: number,
  ): Promise<boolean> {
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

  #markUnavailable(id: ProjectId): void {
    // An already registered exact runtime remains authoritative during a
    // transient metadata outage. Never take healthy traffic offline merely
    // because the control-plane read failed.
    if (this.#registry.get(id)) this.#unavailableProjects.delete(id);
    else this.#unavailableProjects.add(id);
  }

  #scheduleRetry(): void {
    if (
      this.#shuttingDown ||
      this.#retryTask ||
      this.#unavailableProjects.size === 0 ||
      !this.#registry.runtimes().some(runtime =>
        this.#config.projectIds.includes(runtime.projectId)
      )
    ) return;
    const controller = new AbortController();
    this.#retryController = controller;
    const task = this.#runRetryLoop(controller.signal);
    this.#retryTask = task;
    void task.catch(() => undefined).finally(() => {
      if (this.#retryTask === task) this.#retryTask = null;
      if (this.#retryController === controller) this.#retryController = null;
      if (!this.#shuttingDown && this.#unavailableProjects.size > 0) {
        this.#scheduleRetry();
      }
    });
  }

  async #runRetryLoop(signal: AbortSignal): Promise<void> {
    while (!this.#shuttingDown && this.#unavailableProjects.size > 0) {
      await this.#clock.sleep(this.#retryDelayMs(), signal);
      if (signal.aborted || this.#shuttingDown) return;
      const before = new Set(this.#unavailableProjects);
      try {
        await this.#enqueueReconcile([...before], signal);
      } catch (error) {
        if (signal.aborted || this.#shuttingDown) return;
        void error;
      }
      const madeProgress = [...before].some(id => !this.#unavailableProjects.has(id));
      this.#retryAttempt = madeProgress
        ? 0
        : Math.min(this.#retryAttempt + 1, 30);
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
    return Math.max(1, Math.min(this.#retryMaxDelayMs, Math.round(exponential * jitter)));
  }

  #refreshSnapshot(
    desired: number,
    projects: readonly ProjectId[],
  ): void {
    const selectedProjects = new Set(projects);
    const snapshots = this.#registry.snapshots().filter(
      snapshot => selectedProjects.has(snapshot.projectId),
    );
    const unavailableProjects = projects.filter(
      id => this.#unavailableProjects.has(id) && !this.#registry.get(id),
    );
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
      draining: snapshots.filter(value => value.state === "draining").length,
      failed: snapshots.filter(value => value.state === "failed" || value.state === "auth_required").length +
        unavailableProjects.length,
      unavailable: unavailableProjects.length,
      unavailableProjects: Object.freeze([...unavailableProjects]),
      projects: Object.freeze([...projects]),
    });
  }
}
