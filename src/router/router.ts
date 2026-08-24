import { SYSTEM_CLOCK, SYSTEM_RANDOM, type Clock, type RandomSource } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";
import { chooseRuntime, type RouteCandidate } from "./policy";
import { ResourceGovernor } from "./resource-governor";
import type { ReleasableLease } from "./types";
import { classifyWorkload, type WorkloadDescriptor } from "./workload";
import type { LoggerLike } from "../observability/logger";
import { observabilityEvent, type JsonValue } from "../observability/events";
import { sanitizeDiagnosticMessage } from "../observability/redaction";
import { traceRunHandle } from "../observability/traced-run-handle";

export type RelaySource = "http" | "websocket" | "image" | "anthropic" | "internal";

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
  traceId: string;
  acceptedAtMs: number;
  admittedAtMs: number;
  queuedAtMs: number;
  queueWaitMs: number;
  queueDeadlineMs: number;
  queueTimer?: ReturnType<typeof setTimeout>;
}

interface RouteSelection {
  runtime: RoutableSandboxRuntime;
  candidate: RouteCandidate;
  eligibleCandidates: number;
}

export interface MultiSandboxRouterSnapshot {
  capacity: number;
  active: number;
  available: number;
  queued: number;
  runtimes: ReturnType<SandboxRegistry["snapshots"]>;
  resources: ReturnType<ResourceGovernor["snapshot"]>;
}

export class RouterShutdownError extends Error {
  readonly name = "RouterShutdownError";
  readonly status = 503;
  readonly code = "router_shutting_down";

  constructor(message = "GhostProv is shutting down") {
    super(message);
  }
}

export class RouterCapacityTimeoutError extends Error {
  readonly name = "RouterCapacityTimeoutError";
  readonly status = 503;
  readonly code = "router_capacity_timeout";

  constructor(
    message = "No sandbox capacity became available before the routing deadline",
  ) {
    super(message);
  }
}

function abortError(): DOMException {
  return new DOMException("Relay request aborted while queued", "AbortError");
}

export class MultiSandboxRouter {
  readonly #registry: SandboxRegistry;
  readonly #governor: ResourceGovernor;
  readonly #random: RandomSource;
  readonly #clock: Clock;
  readonly #logger?: LoggerLike;
  readonly #slowRequestMs: number;
  readonly #transport: string;
  readonly #queueTimeoutMs: number;
  readonly #registryUnsubscribe: () => void;
  readonly #reserved = new Map<ProjectId, number>();
  readonly #queues = new Map<string, QueuedJob[]>();
  readonly #clientOrder: string[] = [];
  #clientCursor = 0;
  #drainScheduled = false;
  #shuttingDown = false;
  #closed = false;

  constructor(options: {
    registry: SandboxRegistry;
    governor: ResourceGovernor;
    random?: RandomSource;
    clock?: Clock;
    logger?: LoggerLike;
    queueTimeoutMs?: number;
    observability?: { slowRequestMs?: number; transport?: string };
  }) {
    this.#registry = options.registry;
    this.#governor = options.governor;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#logger = options.logger;
    this.#slowRequestMs = options.observability?.slowRequestMs ?? 10_000;
    this.#transport = options.observability?.transport ?? "unknown";
    this.#queueTimeoutMs = options.queueTimeoutMs ?? 30_000;
    if (!Number.isSafeInteger(this.#queueTimeoutMs) || this.#queueTimeoutMs < 1) {
      throw new RangeError("queueTimeoutMs must be a positive safe integer");
    }
    this.#registryUnsubscribe = this.#registry.subscribe(() => this.#scheduleDrain());
  }

  async startRelay(
    body: Record<string, unknown>,
    options: StartRelayOptions = {},
  ): Promise<RunHandle> {
    if (this.#closed || this.#shuttingDown) throw new RouterShutdownError();
    const acceptedAtMs = this.#clock.now();
    const traceId = this.#random.uuid();
    const relayBody = body.service_tier === undefined
      ? { ...body, service_tier: "fast" }
      : body;
    const workload = classifyWorkload(relayBody);
    const initial = this.snapshot();
    this.#emit("request.accepted", {
      trace_id: traceId,
      source: options.source ?? "internal",
      model: workload.model ?? "unknown",
      service_tier: typeof relayBody.service_tier === "string" ? relayBody.service_tier : "unknown",
      stream: workload.stream,
      workload_class: workload.class,
      request_bytes: workload.requestBytes,
      reserved_bytes: workload.reservedBytes,
      capacity: initial.capacity,
      active: initial.active,
      available: initial.available,
      queued: initial.queued,
    });
    let memoryLease: ReleasableLease;
    try {
      memoryLease = await this.#governor.acquire(workload, options.signal);
    } catch (error) {
      this.#failure(traceId, acceptedAtMs, "admission", error, 0);
      throw error;
    }
    const admittedAtMs = this.#clock.now();
    const resources = this.#governor.snapshot();
    this.#emit("admission.acquired", {
      trace_id: traceId,
      admission_wait_ms: Math.max(0, admittedAtMs - acceptedAtMs),
      reserved_bytes: workload.reservedBytes,
      reserved_total_bytes: resources.reservedBytes,
      reservation_capacity_bytes: resources.reservationCapacityBytes,
      memory_paused: resources.paused,
    });
    if (options.signal?.aborted || this.#closed || this.#shuttingDown) {
      memoryLease.release();
      if (options.signal?.aborted) throw abortError();
      throw new RouterShutdownError();
    }
    return new Promise<RunHandle>((resolve, reject) => {
      const clientId = options.clientId ?? "default";
      const job: QueuedJob = {
        body: relayBody,
        workload,
        clientId,
        source: options.source ?? "internal",
        signal: options.signal,
        memoryLease,
        attempts: 0,
        excluded: new Set(),
        resolve,
        reject,
        traceId,
        acceptedAtMs,
        admittedAtMs,
        queuedAtMs: this.#clock.now(),
        queueWaitMs: 0,
        queueDeadlineMs: this.#clock.now() + this.#queueTimeoutMs,
      };
      job.onAbort = () => {
        if (!this.#removeJob(job)) return;
        memoryLease.release();
        const error = abortError();
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        reject(error);
      };
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      this.#enqueue(job);
      const queued = this.snapshot();
      this.#emit("request.queued", {
        trace_id: job.traceId,
        queued: queued.queued,
        client_queue_depth: this.#queues.get(clientId)?.length ?? 0,
        active: queued.active,
        available: queued.available,
      });
      this.#scheduleDrain();
    });
  }

  acquireIncomingBody(bytes: number, signal?: AbortSignal): Promise<ReleasableLease> {
    if (this.#closed || this.#shuttingDown) {
      return Promise.reject(new RouterShutdownError());
    }
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

  beginShutdown(): void {
    if (this.#shuttingDown) return;
    this.#shuttingDown = true;
    const error = new RouterShutdownError();
    for (const queue of this.#queues.values()) {
      for (const job of queue) {
        this.#clearQueueTimer(job);
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        job.reject(error);
      }
    }
    this.#queues.clear();
    this.#clientOrder.length = 0;
    this.#clientCursor = 0;
  }

  async awaitIdle(timeoutMs: number, signal?: AbortSignal): Promise<void> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      throw new RangeError("Router idle timeout must be positive");
    }
    const deadline = this.#clock.now() + timeoutMs;
    for (;;) {
      if (signal?.aborted) throw new DOMException("Router idle wait aborted", "AbortError");
      const snapshot = this.snapshot();
      if (snapshot.active === 0 && snapshot.queued === 0) return;
      if (this.#clock.now() >= deadline) {
        throw new RouterShutdownError("Router idle wait timed out with active work");
      }
      await this.#clock.sleep(Math.min(10, deadline - this.#clock.now()), signal);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.beginShutdown();
    this.#closed = true;
    this.#registryUnsubscribe();
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
    this.#armQueueTimer(job);
  }

  #removeJob(job: QueuedJob): boolean {
    const queue = this.#queues.get(job.clientId);
    if (!queue) return false;
    const index = queue.indexOf(job);
    if (index < 0) return false;
    queue.splice(index, 1);
    this.#clearQueueTimer(job);
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
      this.#clearQueueTimer(job);
      this.#clientCursor = this.#clientOrder.length ? (index + 1) % this.#clientOrder.length : 0;
      this.#removeEmptyClient(clientId);
      return job;
    }
    return null;
  }

  #armQueueTimer(job: QueuedJob): void {
    this.#clearQueueTimer(job);
    const remaining = Math.max(0, job.queueDeadlineMs - this.#clock.now());
    job.queueTimer = setTimeout(() => {
      job.queueTimer = undefined;
      if (!this.#removeJob(job)) return;
      this.#expireJob(job);
      this.#scheduleDrain();
    }, remaining);
    job.queueTimer.unref?.();
  }

  #clearQueueTimer(job: QueuedJob): void {
    if (job.queueTimer) clearTimeout(job.queueTimer);
    job.queueTimer = undefined;
  }

  #scheduleDrain(): void {
    if (this.#closed || this.#shuttingDown || this.#drainScheduled) return;
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
      if (this.#clock.now() >= job.queueDeadlineMs) {
        this.#expireJob(job);
        continue;
      }
      if (job.signal?.aborted) {
        job.memoryLease.release();
        job.reject(abortError());
        continue;
      }
      const routeStarted = this.#clock.now();
      const selection = this.#choose(job);
      if (!selection) {
        this.#enqueue(job, true);
        return;
      }
      const runtime = selection.runtime;
      const selectedAtMs = this.#clock.now();
      job.queueWaitMs = Math.max(0, selectedAtMs - job.queuedAtMs);
      const candidate = selection.candidate;
      this.#emit("route.selected", {
        trace_id: job.traceId,
        project_id: runtime.projectId,
        route_ms: Math.max(0, selectedAtMs - routeStarted),
        queue_wait_ms: job.queueWaitMs,
        sandbox_capacity: candidate.capacity,
        sandbox_active: candidate.active,
        sandbox_reserved: candidate.reserved,
        sandbox_queued: candidate.queueDepth,
        sandbox_quarantined: candidate.quarantined,
        awareness_rtt_ms: candidate.ackEwmaMs,
        completion_ewma_ms: candidate.completionEwmaMs,
        recent_failure_rate: candidate.recentFailureRate,
        heartbeat_age_ms: candidate.heartbeatAgeMs,
        credential_expires_in_ms: candidate.credentialExpiresInMs,
        memory_pressure: candidate.memoryPressure,
        eligible_candidates: selection.eligibleCandidates,
      });
      const reserved = (this.#reserved.get(runtime.projectId) ?? 0) + 1;
      this.#reserved.set(runtime.projectId, reserved);
      void this.#dispatch(job, runtime);
    }
  }

  #expireJob(job: QueuedJob): void {
    job.signal?.removeEventListener("abort", job.onAbort!);
    job.memoryLease.release();
    const error = new RouterCapacityTimeoutError();
    this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
    job.reject(error);
  }

  #choose(job: QueuedJob): RouteSelection | null {
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
    if (!decision) return null;
    const runtime = this.#registry.get(decision.projectId);
    const candidate = candidates.find(value => value.projectId === decision.projectId);
    return runtime && candidate
      ? { runtime, candidate, eligibleCandidates: candidates.length }
      : null;
  }

  async #dispatch(job: QueuedJob, runtime: RoutableSandboxRuntime): Promise<void> {
    const started = this.#clock.now();
    try {
      const handle = await runtime.run(job.body, job.signal);
      const committedAtMs = this.#clock.now();
      const commitAckMs = Math.max(0, committedAtMs - started);
      this.#decrementReserved(runtime.projectId);
      job.signal?.removeEventListener("abort", job.onAbort!);
      this.#emit("transport.committed", {
        trace_id: job.traceId,
        project_id: runtime.projectId,
        transport: this.#transport,
        queue_wait_ms: job.queueWaitMs,
        commit_ack_ms: commitAckMs,
        retry_count: job.attempts,
        ...(handle.stats.diagnostics ? {
          host_prepare_ms: handle.stats.diagnostics.hostPrepareMs,
          host_scheduler_wait_ms: handle.stats.diagnostics.hostSchedulerWaitMs,
          host_publish_to_ack_ms: handle.stats.diagnostics.hostPublishToAckMs,
          sidecar_first_frame_to_commit_ms: handle.stats.diagnostics.sidecarFirstFrameToCommitMs,
          sidecar_claim_sync_ms: handle.stats.diagnostics.sidecarClaimSyncMs,
          sidecar_admission_wait_ms: handle.stats.diagnostics.sidecarAdmissionWaitMs,
          sidecar_commit_processing_ms: handle.stats.diagnostics.sidecarCommitProcessingMs,
          transport_commit_residual_ms: handle.stats.diagnostics.transportCommitResidualMs,
        } : {}),
      });
      const traced = this.#logger
        ? traceRunHandle(handle, {
          logger: this.#logger,
          clock: this.#clock,
          traceId: job.traceId,
          projectId: runtime.projectId,
          acceptedAtMs: job.acceptedAtMs,
          queueWaitMs: job.queueWaitMs,
          commitAckMs,
          committedAtMs,
          retryCount: job.attempts,
          slowRequestMs: this.#slowRequestMs,
        })
        : handle;
      job.resolve(traced);
      void traced.final.then(
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
      if (job.attempts < 1 && !job.signal?.aborted && !this.#shuttingDown) {
        job.attempts++;
        job.excluded.add(runtime.projectId);
        this.#enqueue(job, true);
      } else {
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#failure(job.traceId, job.acceptedAtMs, "pre-commit", error, job.attempts);
        job.reject(error);
      }
      this.#scheduleDrain();
    }
  }

  #emit(event: Parameters<typeof observabilityEvent>[0], fields: Record<string, JsonValue>): void {
    this.#logger?.emit(observabilityEvent(event, fields), event === "request.failed" ? "terminal" : "progress");
  }

  #failure(
    traceId: string,
    acceptedAtMs: number,
    stage: string,
    error: unknown,
    retryCount: number,
  ): void {
    const value = error && typeof error === "object"
      ? error as Record<string, unknown>
      : {};
    this.#emit("request.failed", {
      trace_id: traceId,
      project_id: null,
      total_ms: Math.max(0, this.#clock.now() - acceptedAtMs),
      stage,
      error_name: error instanceof Error ? error.name : "Error",
      error_kind: typeof value.kind === "string" ? value.kind : null,
      status: typeof value.status === "number" ? value.status : null,
      error_message: sanitizeDiagnosticMessage(error instanceof Error ? error.message : error),
      committed: false,
      quarantined: false,
      retry_count: retryCount,
    });
  }

  #decrementReserved(projectId: ProjectId): void {
    const next = Math.max(0, (this.#reserved.get(projectId) ?? 1) - 1);
    if (next) this.#reserved.set(projectId, next);
    else this.#reserved.delete(projectId);
  }
}
