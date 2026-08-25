import type { MultiSandboxConfig } from "../config/multi-sandbox";
import {
  SYSTEM_CLOCK,
  type Clock,
} from "../core/ports";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { PrismProjectSummary } from "../prism/projects-client";
import { SandboxRegistry } from "../sandbox/registry";
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

interface SelectedWorker {
  record: FleetWorkerRecord;
  project: PrismProjectSummary;
}

function emptySnapshot(): FleetSnapshot {
  return {
    desired: 0,
    ready: 0,
    provisioning: 0,
    draining: 0,
    failed: 0,
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

async function mapLimit<T>(
  values: readonly T[],
  limit: number,
  operation: (value: T) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const workers = Array.from(
    { length: Math.min(limit, values.length) },
    async () => {
      for (;;) {
        const index = cursor++;
        if (index >= values.length) return;
        await operation(values[index]);
      }
    },
  );
  await Promise.all(workers);
}

export interface ProjectFleetManagerOptions {
  config: MultiSandboxConfig;
  identity: FleetIdentity;
  projects: FleetProjectsClient;
  manifest: FleetManifestStore;
  registry: SandboxRegistry;
  runtimeFactory: ManagedRuntimeFactory;
  clock?: Clock;
}

export class ProjectFleetManager {
  readonly #config: MultiSandboxConfig;
  readonly #identity: FleetIdentity;
  readonly #projects: FleetProjectsClient;
  readonly #manifest: FleetManifestStore;
  readonly #registry: SandboxRegistry;
  readonly #runtimeFactory: ManagedRuntimeFactory;
  readonly #clock: Clock;
  #snapshot: FleetSnapshot = emptySnapshot();
  #chain: Promise<void> = Promise.resolve();
  #activeReconcile: AbortController | null = null;
  #shuttingDown = false;

  constructor(options: ProjectFleetManagerOptions) {
    this.#config = options.config;
    this.#identity = options.identity;
    this.#projects = options.projects;
    this.#manifest = options.manifest;
    this.#registry = options.registry;
    this.#runtimeFactory = options.runtimeFactory;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
  }

  reconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
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
    this.#activeReconcile?.abort();
    await this.#chain;
    const runtimes = [...this.#registry.runtimes()];
    await Promise.allSettled(runtimes.map(async runtime => {
      await this.#registry.drain(runtime.projectId, reason);
      if (runtime.snapshot().state !== "stopped") await runtime.stop();
      await this.#registry.removeStopped(runtime.projectId);
    }));
    this.#refreshSnapshot(0, []);
  }

  snapshot(): FleetSnapshot {
    return this.#snapshot;
  }

  async #reconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
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
    const configuredProjects = new Map<ProjectId, PrismProjectSummary>();
    await mapLimit(
      this.#config.projectIds,
      this.#config.provisionConcurrency,
      async id => {
        let project: PrismProjectSummary;
        try {
          project = await this.#projects.get(id, signal);
        } catch {
          throw new FleetReconciliationError(
            `Explicit Prism project is unavailable: ${id}`,
            "explicit_project_missing",
          );
        }
        if (project.deleted || project.projectId !== id) {
          throw new FleetReconciliationError(
            `Explicit Prism project is unavailable: ${id}`,
            "explicit_project_missing",
          );
        }
        configuredProjects.set(id, project);
      },
    );
    if (signal?.aborted || this.#shuttingDown) {
      throw new DOMException("Fleet reconciliation aborted", "AbortError");
    }
    const selected: SelectedWorker[] = [];
    const selectedIds = new Set<ProjectId>();

    const select = (worker: FleetWorkerRecord, project: PrismProjectSummary): void => {
      if (selectedIds.has(worker.projectId)) return;
      selected.push({ record: worker, project });
      selectedIds.add(worker.projectId);
    };

    for (const id of this.#config.projectIds) {
      if (signal?.aborted || this.#shuttingDown) {
        throw new DOMException("Fleet reconciliation aborted", "AbortError");
      }
      const project = configuredProjects.get(id)!;
      let worker = manifest.workers.find(value => value.projectId === id);
      if (!worker) {
        const index = nextWorkerIndex(manifest.workers);
        manifest = await this.#manifest.reserveWorker({
          index,
          projectId: id,
          title: project.title,
          nowMs: this.#clock.now(),
          managed: false,
        });
        manifest = await this.#manifest.markCreated(id, this.#clock.now());
        worker = manifest.workers.find(value => value.projectId === id)!;
      }
      select(worker, project);
    }

    await this.#drainExcess(selectedIds);
    const missing = selected.filter(worker => !this.#registry.get(worker.record.projectId));
    await mapLimit(missing, this.#config.provisionConcurrency, async selectedWorker => {
      if (signal?.aborted || this.#shuttingDown) {
        throw new DOMException("Fleet reconciliation aborted", "AbortError");
      }
      const generation = await this.#manifest.nextLeaderGeneration(
        selectedWorker.record.projectId,
        this.#clock.now(),
      );
      const runtime = this.#runtimeFactory.create({
        projectId: selectedWorker.record.projectId,
        workerIndex: selectedWorker.record.index,
        leaderGeneration: generation,
      });
      this.#registry.add(runtime);
      try {
        await runtime.start();
        await this.#manifest.markState(
          runtime.projectId,
          "ready",
          this.#clock.now(),
        );
      } catch (error) {
        await this.#manifest.markState(
          runtime.projectId,
          "failed",
          this.#clock.now(),
        );
        void error;
      }
    });

    this.#refreshSnapshot(
      desired,
      selected.map(worker => worker.record.projectId),
    );
    return this.#snapshot;
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

  async #drainExcess(selected: ReadonlySet<ProjectId>): Promise<void> {
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
      await this.#registry.drain(runtime.projectId, "fleet-reconcile");
      if (runtime.snapshot().state !== "stopped") await runtime.stop();
      await this.#registry.removeStopped(runtime.projectId);
      if (record) {
        await this.#manifest.markState(runtime.projectId, "created", this.#clock.now());
      }
    }
  }

  #refreshSnapshot(
    desired: number,
    projects: readonly ProjectId[],
  ): void {
    const snapshots = this.#registry.snapshots();
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
      failed: snapshots.filter(value => value.state === "failed" || value.state === "auth_required").length,
      projects: Object.freeze([...projects]),
    });
  }
}
