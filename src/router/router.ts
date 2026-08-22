import { SYSTEM_CLOCK, SYSTEM_RANDOM, type Clock, type RandomSource } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";
import { chooseRuntime, type RouteCandidate } from "./policy";
import { ResourceGovernor } from "./resource-governor";
import type { ReleasableLease } from "./types";
import { classifyWorkload, type WorkloadDescriptor } from "./workload";

export type RelaySource = "http" | "websocket" | "image" | "internal";

export interface StartRelayOptions {
  clientId?: string;
  source?: RelaySource;
  signal?: AbortSignal;
}

interface QueuedJob {
  body: Record<string, unknown>;
  workload: WorkloadDescriptor;
  clientId: string;
  source: RelaySource;
  signal?: AbortSignal;
  memoryLease: ReleasableLease;
  attempts: number;
  excluded: Set<ProjectId>;
  resolve: (handle: RunHandle) => void;
  reject: (error: unknown) => void;
  onAbort?: () => void;
}

export interface MultiSandboxRouterSnapshot {
  capacity: number;
  active: number;
  available: number;
  queued: number;
  runtimes: ReturnType<SandboxRegistry["snapshots"]>;
  resources: ReturnType<ResourceGovernor["snapshot"]>;
}

function abortError(): DOMException {
  return new DOMException("Relay request aborted while queued", "AbortError");
}

export class MultiSandboxRouter {
  readonly #registry: SandboxRegistry;
  readonly #governor: ResourceGovernor;
  readonly #random: RandomSource;
  readonly #clock: Clock;
  readonly #reserved = new Map<ProjectId, number>();
  readonly #queues = new Map<string, QueuedJob[]>();
  readonly #clientOrder: string[] = [];
  #clientCursor = 0;
  #drainScheduled = false;

  constructor(options: {
    registry: SandboxRegistry;
    governor: ResourceGovernor;
    random?: RandomSource;
    clock?: Clock;
  }) {
    this.#registry = options.registry;
    this.#governor = options.governor;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
  }

  async startRelay(
    body: Record<string, unknown>,
    options: StartRelayOptions = {},
  ): Promise<RunHandle> {
    const workload = classifyWorkload(body);
    const memoryLease = await this.#governor.acquire(workload, options.signal);
    if (options.signal?.aborted) {
      memoryLease.release();
      throw abortError();
    }
    return new Promise<RunHandle>((resolve, reject) => {
      const clientId = options.clientId ?? "default";
      const job: QueuedJob = {
        body,
        workload,
        clientId,
        source: options.source ?? "internal",
        signal: options.signal,
        memoryLease,
        attempts: 0,
        excluded: new Set(),
        resolve,
        reject,
      };
      job.onAbort = () => {
        if (!this.#removeJob(job)) return;
        memoryLease.release();
        reject(abortError());
      };
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      this.#enqueue(job);
      this.#scheduleDrain();
    });
  }

  acquireIncomingBody(bytes: number, signal?: AbortSignal): Promise<ReleasableLease> {
    if (!Number.isFinite(bytes) || bytes < 0) {
      return Promise.reject(new RangeError("Incoming body reservation must be non-negative"));
    }
    const MiB = 1024 ** 2;
    const workload: WorkloadDescriptor = {
      class: bytes > 512 * 1024 ? "text-large" : "text-small",
      requestBytes: bytes,
      reservedBytes: Math.max(8 * MiB, bytes * 2 + 2 * MiB),
      cpuPermits: 0,
      stream: false,
      serializedBody: "",
    };
    return this.#governor.acquire(workload, signal);
  }

  snapshot(): MultiSandboxRouterSnapshot {
    const runtimes = this.#registry.snapshots();
    const capacity = runtimes
      .filter(runtime => runtime.state === "ready")
      .reduce((sum, runtime) => sum + Math.max(0, runtime.capacity - runtime.quarantined), 0);
    const active = runtimes.reduce((sum, runtime) => sum + runtime.active, 0) +
      [...this.#reserved.values()].reduce((sum, value) => sum + value, 0);
    return {
      capacity,
      active,
      available: Math.max(0, capacity - active),
      queued: [...this.#queues.values()].reduce((sum, queue) => sum + queue.length, 0),
      runtimes,
      resources: this.#governor.snapshot(),
    };
  }

  #enqueue(job: QueuedJob, front = false): void {
    let queue = this.#queues.get(job.clientId);
    if (!queue) {
      queue = [];
      this.#queues.set(job.clientId, queue);
      this.#clientOrder.push(job.clientId);
    }
    if (front) queue.unshift(job);
    else queue.push(job);
  }

  #removeJob(job: QueuedJob): boolean {
    const queue = this.#queues.get(job.clientId);
    if (!queue) return false;
    const index = queue.indexOf(job);
    if (index < 0) return false;
    queue.splice(index, 1);
    this.#removeEmptyClient(job.clientId);
    return true;
  }

  #removeEmptyClient(clientId: string): void {
    const queue = this.#queues.get(clientId);
    if (queue?.length) return;
    this.#queues.delete(clientId);
    const index = this.#clientOrder.indexOf(clientId);
    if (index >= 0) this.#clientOrder.splice(index, 1);
    if (this.#clientOrder.length) this.#clientCursor %= this.#clientOrder.length;
    else this.#clientCursor = 0;
  }

  #nextJob(): QueuedJob | null {
    if (!this.#clientOrder.length) return null;
    for (let offset = 0; offset < this.#clientOrder.length; offset++) {
      const index = (this.#clientCursor + offset) % this.#clientOrder.length;
      const clientId = this.#clientOrder[index];
      const queue = this.#queues.get(clientId);
      if (!queue?.length) continue;
      const job = queue.shift()!;
      this.#clientCursor = this.#clientOrder.length ? (index + 1) % this.#clientOrder.length : 0;
      this.#removeEmptyClient(clientId);
      return job;
    }
    return null;
  }

  #scheduleDrain(): void {
    if (this.#drainScheduled) return;
    this.#drainScheduled = true;
    queueMicrotask(() => {
      this.#drainScheduled = false;
      this.#drain();
    });
  }

  #drain(): void {
    for (;;) {
      const job = this.#nextJob();
      if (!job) return;
      if (job.signal?.aborted) {
        job.memoryLease.release();
        job.reject(abortError());
        continue;
      }
      const runtime = this.#choose(job);
      if (!runtime) {
        this.#enqueue(job, true);
        return;
      }
      const reserved = (this.#reserved.get(runtime.projectId) ?? 0) + 1;
      this.#reserved.set(runtime.projectId, reserved);
      void this.#dispatch(job, runtime);
    }
  }

  #choose(job: QueuedJob): RoutableSandboxRuntime | null {
    const candidates: RouteCandidate[] = this.#registry.runtimes()
      .filter(runtime => !job.excluded.has(runtime.projectId))
      .map(runtime => {
        const snapshot = runtime.snapshot();
        return {
          ...snapshot,
          reserved: this.#reserved.get(runtime.projectId) ?? 0,
        };
      });
    const decision = chooseRuntime(job.workload, candidates, this.#random);
    return decision ? this.#registry.get(decision.projectId) ?? null : null;
  }

  async #dispatch(job: QueuedJob, runtime: RoutableSandboxRuntime): Promise<void> {
    const started = this.#clock.now();
    try {
      const handle = await runtime.run(job.body, job.signal);
      this.#decrementReserved(runtime.projectId);
      job.signal?.removeEventListener("abort", job.onAbort!);
      job.resolve(handle);
      void handle.final.then(
        () => undefined,
        () => undefined,
      ).finally(() => {
        job.memoryLease.release();
        void started;
        this.#scheduleDrain();
      });
      this.#scheduleDrain();
    } catch (error) {
      this.#decrementReserved(runtime.projectId);
      if (job.attempts < 1 && !job.signal?.aborted) {
        job.attempts++;
        job.excluded.add(runtime.projectId);
        this.#enqueue(job, true);
      } else {
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        job.reject(error);
      }
      this.#scheduleDrain();
    }
  }

  #decrementReserved(projectId: ProjectId): void {
    const next = Math.max(0, (this.#reserved.get(projectId) ?? 1) - 1);
    if (next) this.#reserved.set(projectId, next);
    else this.#reserved.delete(projectId);
  }
}
