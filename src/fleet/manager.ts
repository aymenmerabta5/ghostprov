import type { MultiSandboxConfig } from "../config/multi-sandbox";
import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "../core/ports";
import type { PrismIdentityKind } from "../prism/session-manager";
import type { PrismProjectSummary } from "../prism/projects-client";
import { SandboxRegistry } from "../sandbox/registry";
import { projectId, type ProjectId } from "../sandbox/types";
import { FleetManifestStore } from "./manifest";
import type {
  FleetManifest,
  FleetSnapshot,
  FleetWorkerRecord,
  ManagedProjectInspector,
  ManagedRuntimeFactory,
} from "./types";

export interface FleetProjectsClient {
  list(signal?: AbortSignal): Promise<readonly PrismProjectSummary[]>;
  create(
    input: { projectId: ProjectId; title: string },
    signal?: AbortSignal,
  ): Promise<PrismProjectSummary>;
}

export interface FleetIdentity {
  kind: PrismIdentityKind;
  fingerprint: string;
}

export type FleetReconciliationErrorKind =
  | "explicit_project_missing"
  | "insufficient_projects"
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
  inspector: ManagedProjectInspector;
  manifest: FleetManifestStore;
  registry: SandboxRegistry;
  runtimeFactory: ManagedRuntimeFactory;
  clock?: Clock;
  random?: RandomSource;
}

export class ProjectFleetManager {
  readonly #config: MultiSandboxConfig;
  readonly #identity: FleetIdentity;
  readonly #projects: FleetProjectsClient;
  readonly #inspector: ManagedProjectInspector;
  readonly #manifest: FleetManifestStore;
  readonly #registry: SandboxRegistry;
  readonly #runtimeFactory: ManagedRuntimeFactory;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  #snapshot: FleetSnapshot = emptySnapshot();
  #chain: Promise<void> = Promise.resolve();

  constructor(options: ProjectFleetManagerOptions) {
    this.#config = options.config;
    this.#identity = options.identity;
    this.#projects = options.projects;
    this.#inspector = options.inspector;
    this.#manifest = options.manifest;
    this.#registry = options.registry;
    this.#runtimeFactory = options.runtimeFactory;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
  }

  reconcile(signal?: AbortSignal): Promise<FleetSnapshot> {
    const pending = this.#chain.then(
      () => this.#reconcile(signal),
      () => this.#reconcile(signal),
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
    const remote = (await this.#projects.list(signal)).filter(project => !project.deleted);
    const remoteById = new Map(remote.map(project => [project.projectId, project]));
    const desired = this.#identity.kind === "anonymous"
      ? Math.min(this.#config.desiredSandboxes, this.#config.guestMaxProjects)
      : this.#config.desiredSandboxes;
    const selected: SelectedWorker[] = [];
    const selectedIds = new Set<ProjectId>();

    const select = (worker: FleetWorkerRecord, project: PrismProjectSummary): void => {
      if (selected.length >= desired || selectedIds.has(worker.projectId)) return;
      selected.push({ record: worker, project });
      selectedIds.add(worker.projectId);
    };

    for (const id of this.#config.projectIds) {
      if (selected.length >= desired) break;
      const project = remoteById.get(id);
      if (!project) {
        throw new FleetReconciliationError(
          `Explicit Prism project is unavailable: ${id}`,
          "explicit_project_missing",
        );
      }
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

    for (const current of [...manifest.workers].sort((left, right) => left.index - right.index)) {
      if (selected.length >= desired || selectedIds.has(current.projectId)) continue;
      if (!current.managed) continue;
      let project = remoteById.get(current.projectId);
      if (!project && current.state === "creating" && current.managed) {
        project = await this.#projects.create({
          projectId: current.projectId,
          title: current.title,
        }, signal);
        remoteById.set(project.projectId, project);
      }
      if (!project) {
        await this.#manifest.markState(current.projectId, "failed", this.#clock.now());
        continue;
      }
      if (current.state === "creating") {
        manifest = await this.#manifest.markCreated(current.projectId, this.#clock.now());
      }
      select(
        manifest.workers.find(value => value.projectId === current.projectId) ?? current,
        project,
      );
    }

    for (const project of remote) {
      if (selected.length >= desired) break;
      if (selectedIds.has(project.projectId)) continue;
      const titleMatch = /^GhostProv Worker (\d+)$/.exec(project.title);
      if (!titleMatch) continue;
      const inspection = await this.#inspector.inspect(project.projectId, signal);
      const markerIndex = inspection.marker?.workerIndex;
      if (
        !inspection.writable ||
        inspection.marker?.protocol !== "gp-awareness-v2" ||
        markerIndex !== Number(titleMatch[1]) ||
        !Number.isSafeInteger(markerIndex) ||
        markerIndex < 1 ||
        manifest.workers.some(worker => worker.index === markerIndex)
      ) continue;
      manifest = await this.#manifest.reserveWorker({
        index: markerIndex,
        projectId: project.projectId,
        title: project.title,
        nowMs: this.#clock.now(),
      });
      manifest = await this.#manifest.markCreated(project.projectId, this.#clock.now());
      select(manifest.workers.find(worker => worker.projectId === project.projectId)!, project);
    }

    if (this.#config.projectReuse === "eligible-owned") {
      for (const project of remote) {
        if (selected.length >= desired) break;
        if (selectedIds.has(project.projectId)) continue;
        const inspection = await this.#inspector.inspect(project.projectId, signal);
        if (!inspection.writable || !inspection.empty || inspection.marker) continue;
        const index = nextWorkerIndex(manifest.workers);
        manifest = await this.#manifest.reserveWorker({
          index,
          projectId: project.projectId,
          title: project.title,
          nowMs: this.#clock.now(),
        });
        manifest = await this.#manifest.markCreated(project.projectId, this.#clock.now());
        select(manifest.workers.find(worker => worker.projectId === project.projectId)!, project);
      }
    }

    while (selected.length < desired && this.#config.autoCreateProjects) {
      const index = nextWorkerIndex(manifest.workers);
      const id = projectId(this.#random.uuid());
      const title = `GhostProv Worker ${index}`;
      manifest = await this.#manifest.reserveWorker({
        index,
        projectId: id,
        title,
        nowMs: this.#clock.now(),
      });
      const project = await this.#projects.create({ projectId: id, title }, signal);
      manifest = await this.#manifest.markCreated(id, this.#clock.now());
      select(manifest.workers.find(worker => worker.projectId === id)!, project);
    }

    if (selected.length < desired) {
      throw new FleetReconciliationError(
        `Only ${selected.length} of ${desired} Prism projects are available`,
        "insufficient_projects",
      );
    }

    await this.#drainExcess(selectedIds);
    const missing = selected.filter(worker => !this.#registry.get(worker.record.projectId));
    await mapLimit(missing, this.#config.provisionConcurrency, async selectedWorker => {
      const generation = await this.#manifest.nextLeaderGeneration(
        selectedWorker.record.projectId,
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
      ready: snapshots.filter(value => value.state === "ready").length,
      provisioning: snapshots.filter(value => [
        "configured",
        "provisioning",
        "syncing_credentials",
        "syncing_workspace",
        "deploying_daemon",
      ].includes(value.state)).length,
      draining: snapshots.filter(value => value.state === "draining").length,
      failed: snapshots.filter(value => value.state === "failed" || value.state === "auth_required").length,
      projects: Object.freeze([...projects]),
    });
  }
}
