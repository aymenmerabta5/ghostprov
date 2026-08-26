import { SYSTEM_CLOCK, SYSTEM_RANDOM, type Clock, type RandomSource } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";
import {
  chooseRuntime,
  isRuntimeEligible,
  isRuntimeHealthy,
  type RouteCandidate,
} from "./policy";
import { ResourceGovernor, type ResourceLease } from "./resource-governor";
import type { ReleasableLease } from "./types";
import { RouterTrafficTelemetry, type RouterTrafficSnapshot } from "./telemetry";
import { classifyWorkload, type WorkloadDescriptor } from "./workload";
import type { LoggerLike } from "../observability/logger";
import { observabilityEvent, type JsonValue } from "../observability/events";
import { sanitizeDiagnosticMessage } from "../observability/redaction";
import { traceRunHandle } from "../observability/traced-run-handle";
import { validateResponsesInputJson } from "../relay/responses-validation";
import {
  rateLimitHintFromError,
  rateLimitProblemFromError,
  type RateLimitHint,
  type RateLimitProblem,
} from "../v2/rate-limit";

export type RelaySource = "http" | "websocket" | "image" | "anthropic" | "internal";

type RouterQueueClass = "latency" | "bulk";

const KiB = 1024;
const MiB = KiB ** 2;
const POST_COMMIT_RESERVATION_BYTES = 4 * MiB;
const MIN_FAIR_COST_BYTES = 4 * KiB;
const MAX_FAIR_COST_BYTES = 1 * MiB;
const CLIENT_QUANTUM_BYTES = 256 * KiB;
const CLASS_QUANTUM_BYTES: Readonly<Record<RouterQueueClass, number>> = Object.freeze({
  // Interactive work receives a latency preference, while the byte deficit
  // still guarantees a continuously backlogged bulk lane service.
  latency: 512 * KiB,
  bulk: 256 * KiB,
});
const DEFAULT_QUEUE_CAPACITY = 4_096;
const DEFAULT_QUEUE_BYTES = 1024 * MiB;
const PRECOMMIT_RETRY_BASE_MS = 25;
const PRECOMMIT_RETRY_MAX_MS = 250;
const DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS = 15_000;

interface ClientLane {
  readonly key: string;
  readonly clientId: string;
  readonly queueClass: RouterQueueClass;
  head: QueuedJob | null;
  tail: QueuedJob | null;
  size: number;
  deficitBytes: number;
  previous: ClientLane;
  next: ClientLane;
}

interface FairClassQueue {
  readonly queueClass: RouterQueueClass;
  readonly quantumBytes: number;
  cursor: ClientLane | null;
  lanes: number;
  deficitBytes: number;
  turnActive: boolean;
}

/**
 * A reservation made while the Node HTTP adapter is still buffering a body.
 * Production leases support `replace()` so ownership can move to the parsed
 * workload without briefly reserving both phases. `replace` stays optional on
 * the public router seam so lightweight adapters which only pre-reserve input
 * bytes remain compatible and continue through normal admission.
 */
export interface IncomingResourceLease extends ReleasableLease {
  readonly reservedBytes?: number;
  /** Actual encoded request bytes observed by the bounded body reader. */
  readonly bodyBytes?: number;
  /** Ingress acceptance time, shared with routing/transport telemetry. */
  readonly acceptedAtMs?: number;
  /** Absolute ingress-to-transport-admission deadline. */
  readonly deadlineMs?: number;
  /** Aborts on caller disconnect, deadline expiry, or router shutdown. */
  readonly signal?: AbortSignal;
  /** Grows an unknown-length request reservation before retaining more bytes. */
  ensureBytes?(bytes: number): Promise<void>;
  replace?(
    workload: WorkloadDescriptor,
    signal?: AbortSignal,
    options?: { deadlineMs?: number },
  ): Promise<ResourceLease>;
}

export interface IncomingBodyAcquireOptions {
  /** Structural route limit used only to cap geometric growth. */
  maxBytes?: number;
}

export interface StartRelayOptions {
  clientId?: string;
  source?: RelaySource;
  signal?: AbortSignal;
  incomingLease?: IncomingResourceLease;
  /** Known wire bytes, avoiding a second full-body serialization for sizing. */
  encodedBodyBytes?: number;
}

interface QueuedJob {
  body: Record<string, unknown>;
  workload: WorkloadDescriptor;
  clientId: string;
  source: RelaySource;
  signal?: AbortSignal;
  memoryLease: ResourceLease;
  retainFullLeaseAfterCommit: boolean;
  attempts: number;
  excluded: Map<string, {
    retryEpoch: number;
    retryAtMs: number;
    failures: number;
  }>;
  resolve: (handle: RunHandle) => void;
  reject: (error: unknown) => void;
  onAbort?: () => void;
  traceId: string;
  acceptedAtMs: number;
  admittedAtMs: number;
  queuedAtMs: number;
  queueWaitMs: number;
  queueDeadlineMs: number;
  queueClass: RouterQueueClass;
  fairCostBytes: number;
  fairnessCharged: boolean;
  queueLane: ClientLane | null;
  queuePrevious: QueuedJob | null;
  queueNext: QueuedJob | null;
  deadlineIndex: number;
  retryWakeTimer?: ReturnType<typeof setTimeout>;
  retryWakeAtMs: number | null;
}

interface RouteSelection {
  runtime: RoutableSandboxRuntime;
  candidate: RouteCandidate;
  incarnationKey: string;
  eligibleCandidates: number;
  retryEpoch: number;
}

interface CachedRuntime {
  runtime: RoutableSandboxRuntime;
  snapshot: RoutableSandboxSnapshot;
  candidate: RouteCandidate;
  observedAtMs: number;
  retryEpoch: number;
}

interface IncomingRequestTracker {
  controller: AbortController;
}

function usesImageGeneration(body: Record<string, unknown>): boolean {
  if (typeof body.model === "string" && body.model.startsWith("gpt-image-")) return true;
  if (!Array.isArray(body.tools)) return false;
  return body.tools.some(tool => (
    tool !== null &&
    typeof tool === "object" &&
    !Array.isArray(tool) &&
    (tool as Record<string, unknown>).type === "image_generation"
  ));
}

export interface MultiSandboxRouterSnapshot {
  capacity: number;
  active: number;
  available: number;
  queued: number;
  queuedBytes?: number;
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

class RouterRouteAttemptTimeoutError extends Error {
  readonly name = "RouterRouteAttemptTimeoutError";
  readonly code = "router_route_attempt_timeout";
  readonly committed = false;

  constructor() {
    super("Selected sandbox did not acquire transport admission in time");
  }
}

export class RouterQueueFullError extends Error {
  readonly name = "RouterQueueFullError";
  readonly status = 503;
  readonly code = "router_queue_full";

  constructor(message = "The bounded sandbox routing queue is full") {
    super(message);
  }
}

function abortError(): DOMException {
  return new DOMException("Relay request aborted while queued", "AbortError");
}

function isLocalCpuTransformFailure(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const value = error as { name?: unknown; code?: unknown };
  return value.name === "CpuTransformQueueFullError" ||
    value.name === "CpuTransformTimeoutError" ||
    value.code === "cpu_transform_overloaded" ||
    value.code === "cpu_transform_timeout";
}

function isSafePreCommitFailure(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const value = error as {
    name?: unknown;
    committed?: unknown;
    ambiguous?: unknown;
    providerError?: unknown;
    provider_error?: unknown;
  };
  if (
    value.committed === true ||
    value.ambiguous === true ||
    value.providerError !== undefined ||
    value.provider_error !== undefined
  ) return false;
  // V2 owns this contract: only an explicit pre-commit proof is safe to move
  // to another sandbox. A generic rejected Promise is not such a proof.
  return value.committed === false && value.name === "V2PreCommitError";
}

function isDeterministicRequestTooLarge(error: unknown): boolean {
  if (!isSafePreCommitFailure(error)) return false;
  const code = (error as { code?: unknown }).code;
  return code === "request_too_large";
}

function isRetryablePreCommitFailure(error: unknown): boolean {
  return isSafePreCommitFailure(error) && !isDeterministicRequestTooLarge(error);
}

function queueClassFor(workload: WorkloadDescriptor): RouterQueueClass {
  return workload.class === "text-small" ? "latency" : "bulk";
}

function fairCostBytes(workload: WorkloadDescriptor): number {
  return Math.max(
    MIN_FAIR_COST_BYTES,
    Math.min(MAX_FAIR_COST_BYTES, Math.max(1, workload.requestBytes)),
  );
}

function boundedClientId(value: string | undefined, source: RelaySource): string {
  const normalized = value?.trim();
  if (!normalized) return `${source}:anonymous`;
  if (/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(normalized)) return normalized;
  // Client identity never enters logs, but it is part of queue-map keys. Keep
  // adversarial headers bounded without collapsing every long ID to one lane.
  let left = 0x811c9dc5;
  let right = 0x9e3779b9;
  for (let index = 0; index < normalized.length; index++) {
    const code = normalized.charCodeAt(index);
    left = Math.imul(left ^ code, 0x01000193) >>> 0;
    right = Math.imul(right ^ (code + index), 0x85ebca6b) >>> 0;
  }
  return `${source}:id-${left.toString(16).padStart(8, "0")}${right.toString(16).padStart(8, "0")}`;
}

function runtimeIncarnationKey(value: {
  projectId: ProjectId;
  incarnation?: number;
}): string {
  return `${value.projectId}:${value.incarnation ?? 0}`;
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
  readonly #routeAttemptTimeoutMs: number;
  readonly #queueCapacity: number;
  readonly #queueByteCapacity: number;
  readonly #registryUnsubscribe: () => void;
  readonly #traffic = new RouterTrafficTelemetry();
  readonly #runtimeCache = new Map<ProjectId, CachedRuntime>();
  readonly #incomingRequests = new Set<IncomingRequestTracker>();
  readonly #pendingAdmissions = new Set<IncomingRequestTracker>();
  /**
   * Host run calls not yet handed off to the runtime's own admission-credit
   * accounting. The per-job callback below is the only valid handoff proof.
   */
  readonly #reserved = new Map<ProjectId, Set<QueuedJob>>();
  readonly #rateLimitObservations = new Map<ProjectId, Map<string, {
    hint: RateLimitHint;
    problem: RateLimitProblem | null;
    requestedModel: string | null;
    observedAtMs: number;
    incarnationKey: string;
  }>>();
  readonly #lanes = new Map<string, ClientLane>();
  /** Bounded LRU debt for clients whose one outstanding lane became empty. */
  readonly #inactiveLaneDeficits = new Map<string, number>();
  readonly #clientDepth = new Map<string, number>();
  readonly #fairClasses: Record<RouterQueueClass, FairClassQueue> = {
    latency: {
      queueClass: "latency",
      quantumBytes: CLASS_QUANTUM_BYTES.latency,
      cursor: null,
      lanes: 0,
      deficitBytes: 0,
      turnActive: false,
    },
    bulk: {
      queueClass: "bulk",
      quantumBytes: CLASS_QUANTUM_BYTES.bulk,
      cursor: null,
      lanes: 0,
      deficitBytes: 0,
      turnActive: false,
    },
  };
  readonly #deadlines: QueuedJob[] = [];
  #classCursor: RouterQueueClass = "latency";
  #queuedJobs = 0;
  #queuedBytes = 0;
  #deadlineTimer?: ReturnType<typeof setTimeout>;
  #deadlineTimerAtMs: number | null = null;
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
    routeAttemptTimeoutMs?: number;
    queueCapacity?: number;
    queueByteCapacity?: number;
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
    this.#routeAttemptTimeoutMs = options.routeAttemptTimeoutMs ?? Math.min(
      DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS,
      this.#queueTimeoutMs,
    );
    if (
      !Number.isSafeInteger(this.#routeAttemptTimeoutMs) ||
      this.#routeAttemptTimeoutMs < 1 ||
      this.#routeAttemptTimeoutMs > this.#queueTimeoutMs
    ) {
      throw new RangeError(
        "routeAttemptTimeoutMs must be a positive safe integer no greater than queueTimeoutMs",
      );
    }
    this.#queueCapacity = options.queueCapacity ?? DEFAULT_QUEUE_CAPACITY;
    this.#queueByteCapacity = options.queueByteCapacity ?? DEFAULT_QUEUE_BYTES;
    if (!Number.isSafeInteger(this.#queueCapacity) || this.#queueCapacity < 1) {
      throw new RangeError("queueCapacity must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.#queueByteCapacity) || this.#queueByteCapacity < 1) {
      throw new RangeError("queueByteCapacity must be a positive safe integer");
    }
    for (const runtime of this.#registry.runtimes()) this.#refreshRuntime(runtime);
    this.#registryUnsubscribe = this.#registry.subscribe(event => {
      if (event.type === "removed") {
        this.#runtimeCache.delete(event.projectId);
        this.#rateLimitObservations.delete(event.projectId);
        return;
      }
      const runtime = this.#registry.get(event.projectId);
      if (!runtime) return;
      const previous = this.#runtimeCache.get(event.projectId);
      const previousCredits = previous
        ? this.#candidateCredits(this.#agedCandidate(previous))
        : 0;
      const previousIncarnation = previous?.snapshot.incarnation ?? 0;
      const next = this.#refreshRuntime(runtime);
      const nextCredits = this.#candidateCredits(next.candidate);
      const incarnationChanged = !!previous &&
        (next.snapshot.incarnation ?? 0) !== previousIncarnation;
      if (incarnationChanged) this.#rateLimitObservations.delete(event.projectId);
      // Awareness publishes frequently for frame/RTT progress. Queue draining
      // only becomes useful when admission credits increase, health returns,
      // or a fresh incarnation makes an excluded retry target safe again.
      if (
        !previous ||
        nextCredits > previousCredits ||
        incarnationChanged ||
        next.retryEpoch > previous.retryEpoch
      ) this.#scheduleDrain();
    });
  }

  async startRelay(
    body: Record<string, unknown>,
    options: StartRelayOptions = {},
  ): Promise<RunHandle> {
    if (this.#closed || this.#shuttingDown) throw new RouterShutdownError();
    this.#traffic.accepted();
    const now = this.#clock.now();
    const inheritedAcceptedAtMs = options.incomingLease?.acceptedAtMs;
    const acceptedAtMs = typeof inheritedAcceptedAtMs === "number" &&
        Number.isFinite(inheritedAcceptedAtMs) && inheritedAcceptedAtMs <= now
      ? inheritedAcceptedAtMs
      : now;
    const inheritedDeadlineMs = options.incomingLease?.deadlineMs;
    const admissionDeadlineMs = typeof inheritedDeadlineMs === "number" &&
        Number.isFinite(inheritedDeadlineMs) && inheritedDeadlineMs >= acceptedAtMs
      ? inheritedDeadlineMs
      : acceptedAtMs + this.#queueTimeoutMs;
    const traceId = this.#random.uuid();
    const source = options.source ?? "internal";
    const imageGeneration = usesImageGeneration(body);
    const injectFastTier = body.service_tier === undefined && !imageGeneration;
    let relayBody: Record<string, unknown>;
    if (imageGeneration && "service_tier" in body) {
      const { service_tier: _unsupportedImageTier, ...withoutImageTier } = body;
      relayBody = withoutImageTier;
    } else {
      relayBody = injectFastTier ? { ...body, service_tier: "fast" } : body;
    }
    const incomingBodyBytes = source === "http" ? options.incomingLease?.bodyBytes : undefined;
    const encodedBodyBytes = options.encodedBodyBytes ?? incomingBodyBytes;
    if (
      encodedBodyBytes !== undefined &&
      (!Number.isSafeInteger(encodedBodyBytes) || encodedBodyBytes < 0)
    ) {
      throw new RangeError("encodedBodyBytes must be a non-negative safe integer");
    }
    const workload = classifyWorkload(relayBody, encodedBodyBytes === undefined
      ? undefined
      : {
          // Covers an injected service_tier key/value and object punctuation.
          encodedBytes: Math.min(
            Number.MAX_SAFE_INTEGER,
            encodedBodyBytes + (injectFastTier ? 64 : 0),
          ),
        });
    // Multipart image input and its base64 relay representation remain live
    // together until the compatibility request finishes. The first fan-out
    // job receives the one-shot ingress lease and conservatively reserves both
    // phases in one replacement lease; later jobs use ordinary admission.
    const retainedIngressBytes = options.source === "image" &&
        typeof options.incomingLease?.reservedBytes === "number" &&
        Number.isSafeInteger(options.incomingLease.reservedBytes) &&
        options.incomingLease.reservedBytes > 0
      ? options.incomingLease.reservedBytes
      : 0;
    const admissionWorkload = retainedIngressBytes > 0
      ? {
          ...workload,
          reservedBytes: Math.min(
            Number.MAX_SAFE_INTEGER,
            workload.reservedBytes + retainedIngressBytes,
          ),
        }
      : workload;
    const initial = this.snapshot();
    this.#emit("request.accepted", {
      trace_id: traceId,
      source: options.source ?? "internal",
      model: workload.model ?? "unknown",
      service_tier: typeof relayBody.service_tier === "string" ? relayBody.service_tier : "unknown",
      stream: workload.stream,
      workload_class: workload.class,
      request_bytes: workload.requestBytes,
      reserved_bytes: admissionWorkload.reservedBytes,
      capacity: initial.capacity,
      active: initial.active,
      available: initial.available,
      queued: initial.queued,
    });
    try {
      validateResponsesInputJson(relayBody);
    } catch (error) {
      options.incomingLease?.release();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "validation", error, 0);
      throw error;
    }
    const admissionWaitController = new AbortController();
    const admissionTracker: IncomingRequestTracker = { controller: admissionWaitController };
    const abortAdmissionWait = () => admissionWaitController.abort(
      options.signal?.reason ?? abortError(),
    );
    const finishAdmissionWait = () => {
      options.signal?.removeEventListener("abort", abortAdmissionWait);
      this.#pendingAdmissions.delete(admissionTracker);
    };
    if (options.signal?.aborted) abortAdmissionWait();
    else options.signal?.addEventListener("abort", abortAdmissionWait, { once: true });
    this.#pendingAdmissions.add(admissionTracker);
    let memoryLease: ResourceLease;
    try {
      memoryLease = typeof options.incomingLease?.replace === "function"
        ? await options.incomingLease.replace(
            admissionWorkload,
            admissionWaitController.signal,
            { deadlineMs: admissionDeadlineMs },
          )
        : await this.#governor.acquire(
            admissionWorkload,
            admissionWaitController.signal,
            { deadlineMs: admissionDeadlineMs },
          );
    } catch (error) {
      finishAdmissionWait();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      const failure = admissionWaitController.signal.reason instanceof RouterShutdownError
        ? admissionWaitController.signal.reason
        : error;
      this.#failure(traceId, acceptedAtMs, "admission", failure, 0);
      throw failure;
    }
    finishAdmissionWait();
    const admittedAtMs = this.#clock.now();
    const resources = this.#governor.snapshot();
    this.#emit("admission.acquired", {
      trace_id: traceId,
      admission_wait_ms: Math.max(0, admittedAtMs - acceptedAtMs),
      reserved_bytes: admissionWorkload.reservedBytes,
      reserved_total_bytes: resources.reservedBytes,
      reservation_capacity_bytes: resources.reservationCapacityBytes,
      memory_paused: resources.paused,
    });
    if (options.signal?.aborted || this.#closed || this.#shuttingDown) {
      memoryLease.release();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      if (options.signal?.aborted) throw abortError();
      throw new RouterShutdownError();
    }
    return new Promise<RunHandle>((resolve, reject) => {
      const clientId = boundedClientId(options.clientId, source);
      const enqueuedAtMs = this.#clock.now();
      const job: QueuedJob = {
        body: relayBody,
        workload,
        clientId,
        source,
        signal: options.signal,
        memoryLease,
        retainFullLeaseAfterCommit: retainedIngressBytes > 0,
        attempts: 0,
        excluded: new Map(),
        resolve,
        reject,
        traceId,
        acceptedAtMs,
        admittedAtMs,
        queuedAtMs: enqueuedAtMs,
        queueWaitMs: 0,
        // One absolute admission deadline starts when the public request is
        // accepted; governor wait, fair queueing, safe failover, preparation,
        // and transport admission cannot each restart it.
        queueDeadlineMs: admissionDeadlineMs,
        queueClass: queueClassFor(workload),
        fairCostBytes: fairCostBytes(workload),
        fairnessCharged: false,
        queueLane: null,
        queuePrevious: null,
        queueNext: null,
        deadlineIndex: -1,
        retryWakeAtMs: null,
      };
      job.onAbort = () => {
        if (!this.#removeJob(job)) return;
        this.#clearRetryWake(job);
        memoryLease.release();
        const error = abortError();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        reject(error);
      };
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      if (!this.#enqueue(job)) {
        options.signal?.removeEventListener("abort", job.onAbort);
        memoryLease.release();
        const error = new RouterQueueFullError();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        reject(error);
        return;
      }
      const queued = this.snapshot();
      this.#emit("request.queued", {
        trace_id: job.traceId,
        queued: queued.queued,
        queued_bytes: queued.queuedBytes ?? 0,
        client_queue_depth: this.#clientDepth.get(clientId) ?? 0,
        queue_class: job.queueClass,
        active: queued.active,
        available: queued.available,
      });
      this.#scheduleDrain();
    });
  }

  async acquireIncomingBody(
    bytes: number,
    signal?: AbortSignal,
    options: IncomingBodyAcquireOptions = {},
  ): Promise<IncomingResourceLease> {
    if (this.#closed || this.#shuttingDown) {
      throw new RouterShutdownError();
    }
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      throw new RangeError("Incoming body reservation must be a non-negative safe integer");
    }
    const maxBytes = options.maxBytes;
    if (maxBytes !== undefined && (!Number.isSafeInteger(maxBytes) || maxBytes < 1)) {
      throw new RangeError("Incoming body maximum must be a positive safe integer");
    }
    if (maxBytes !== undefined && bytes > maxBytes) {
      throw new RangeError("Incoming body reservation exceeds its structural maximum");
    }
    const acceptedAtMs = this.#clock.now();
    const deadlineMs = acceptedAtMs + this.#queueTimeoutMs;
    const controller = new AbortController();
    const tracker: IncomingRequestTracker = { controller };
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    let active = true;
    let lease: ResourceLease | null = null;
    let observedBodyBytes = bytes;
    const onCallerAbort = () => controller.abort(signal?.reason ?? abortError());
    const cleanup = () => {
      if (!active) return;
      active = false;
      if (deadlineTimer) clearTimeout(deadlineTimer);
      deadlineTimer = undefined;
      signal?.removeEventListener("abort", onCallerAbort);
      this.#incomingRequests.delete(tracker);
    };
    if (signal?.aborted) onCallerAbort();
    else signal?.addEventListener("abort", onCallerAbort, { once: true });
    deadlineTimer = setTimeout(() => {
      deadlineTimer = undefined;
      controller.abort(new RouterCapacityTimeoutError(
        "Request body was not received before the routing deadline",
      ));
    }, Math.max(0, deadlineMs - this.#clock.now()));
    deadlineTimer.unref?.();
    this.#incomingRequests.add(tracker);

    const incomingWorkload = (
      bodyBytes: number,
      reservedBytes = Math.max(8 * MiB, bodyBytes * 2 + 2 * MiB),
    ): WorkloadDescriptor => ({
      class: bodyBytes > 512 * KiB ? "text-large" : "text-small",
      requestBytes: bodyBytes,
      // Chunk buffers and the final contiguous Buffer coexist briefly.
      reservedBytes,
      cpuPermits: 0,
      stream: false,
    });

    try {
      lease = await this.#governor.acquire(
        incomingWorkload(bytes),
        controller.signal,
        { deadlineMs },
      );
    } catch (error) {
      cleanup();
      if (controller.signal.reason instanceof RouterShutdownError) {
        throw controller.signal.reason;
      }
      throw error;
    }

    return {
      get reservedBytes() { return lease?.reservedBytes ?? 0; },
      get bodyBytes() { return observedBodyBytes; },
      acceptedAtMs,
      deadlineMs,
      signal: controller.signal,
      ensureBytes: async nextBytes => {
        if (!Number.isSafeInteger(nextBytes) || nextBytes < 0) {
          throw new RangeError("Incoming body size must be a non-negative safe integer");
        }
        if (!active || !lease) throw new Error("Incoming body lease is no longer active");
        observedBodyBytes = Math.max(observedBodyBytes, nextBytes);
        const required = incomingWorkload(nextBytes).reservedBytes;
        if (required <= lease.reservedBytes) return;
        const maximum = maxBytes === undefined
          ? Number.MAX_SAFE_INTEGER
          : incomingWorkload(maxBytes).reservedBytes;
        const grown = Math.min(
          maximum,
          Math.max(required, Math.min(Number.MAX_SAFE_INTEGER, lease.reservedBytes * 2)),
        );
        const next = incomingWorkload(nextBytes, grown);
        lease = await lease.replace(next, controller.signal, { deadlineMs });
      },
      replace: async (workload, replacementSignal, options) => {
        if (!active || !lease) throw new Error("Incoming body lease is no longer active");
        if (replacementSignal?.aborted) throw replacementSignal.reason ?? abortError();
        const requestedDeadline = options?.deadlineMs;
        const effectiveDeadline = typeof requestedDeadline === "number" &&
            Number.isFinite(requestedDeadline)
          ? Math.min(deadlineMs, requestedDeadline)
          : deadlineMs;
        const replacement = await lease.replace(
          workload,
          controller.signal,
          { deadlineMs: effectiveDeadline },
        );
        lease = null;
        cleanup();
        return replacement;
      },
      release: () => {
        if (!active) return;
        lease?.release();
        lease = null;
        cleanup();
      },
    };
  }

  trafficSnapshotAndReset(): RouterTrafficSnapshot {
    return this.#traffic.snapshotAndReset();
  }

  #unreflectedReservations(projectId: ProjectId): number {
    return this.#reserved.get(projectId)?.size ?? 0;
  }

  #reserveRuntime(job: QueuedJob, projectId: ProjectId): void {
    let jobs = this.#reserved.get(projectId);
    if (!jobs) {
      jobs = new Set();
      this.#reserved.set(projectId, jobs);
    }
    jobs.add(job);
  }

  #releaseRuntimeReservation(job: QueuedJob, projectId: ProjectId): void {
    const jobs = this.#reserved.get(projectId);
    if (!jobs?.delete(job)) return;
    if (jobs.size === 0) this.#reserved.delete(projectId);
  }

  snapshot(): MultiSandboxRouterSnapshot {
    const runtimes = [...this.#runtimeCache.values()].map(cached => {
      const runtime = this.#agedSnapshot(cached);
      const byModel = this.#rateLimitObservations.get(runtime.projectId);
      const incarnationKey = runtimeIncarnationKey(runtime);
      const observations = byModel
        ? [...byModel.entries()].filter(([, value]) => value.incarnationKey === incarnationKey)
        : [];
      const observation = observations.at(-1)?.[1];
      const organization = observation?.problem?.organization ?? null;
      return {
        ...runtime,
        upstreamOrganization: organization ? {
          id: organization,
          model: observation?.problem?.model ?? observation?.requestedModel ?? null,
          source: "rate_limit" as const,
          observedAgoMs: Math.max(0, this.#clock.now() - observation!.observedAtMs),
        } : null,
        upstreamRateLimit: observation ? {
          hint: observation.hint,
          problem: observation.problem,
          requestedModel: observation.requestedModel,
          observedAgoMs: Math.max(0, this.#clock.now() - observation.observedAtMs),
        } : null,
        upstreamRateLimits: Object.freeze(Object.fromEntries(
          observations.map(([model, value]) => [model, {
            hint: value.hint,
            problem: value.problem,
            requestedModel: value.requestedModel,
            observedAgoMs: Math.max(0, this.#clock.now() - value.observedAtMs),
          }]),
        )),
      };
    });
    const capacity = runtimes
      .filter(isRuntimeHealthy)
      .reduce((sum, runtime) => sum + Math.max(0, runtime.capacity - runtime.quarantined), 0);
    const available = runtimes
      .filter(isRuntimeHealthy)
      .reduce((sum, runtime) => sum + Math.max(
        0,
        runtime.available - this.#unreflectedReservations(runtime.projectId),
      ), 0);
    const active = runtimes.reduce(
      (sum, runtime) => sum + runtime.active + this.#unreflectedReservations(runtime.projectId),
      0,
    );
    return {
      capacity,
      active,
      available,
      queued: this.#queuedJobs,
      queuedBytes: this.#queuedBytes,
      runtimes,
      resources: this.#governor.snapshot(),
    };
  }

  beginShutdown(): void {
    if (this.#shuttingDown) return;
    this.#shuttingDown = true;
    const error = new RouterShutdownError();
    for (const request of this.#incomingRequests) {
      request.controller.abort(error);
    }
    for (const admission of this.#pendingAdmissions) {
      admission.controller.abort(error);
    }
    for (const job of [...this.#deadlines]) {
      if (!this.#removeJob(job)) continue;
      this.#clearRetryWake(job);
      job.signal?.removeEventListener("abort", job.onAbort!);
      job.memoryLease.release();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
      this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
      job.reject(error);
    }
    this.#clearDeadlineTimer();
  }

  async awaitIdle(timeoutMs: number, signal?: AbortSignal): Promise<void> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      throw new RangeError("Router idle timeout must be positive");
    }
    const deadline = this.#clock.now() + timeoutMs;
    for (;;) {
      if (signal?.aborted) throw new DOMException("Router idle wait aborted", "AbortError");
      const snapshot = this.snapshot();
      if (
        snapshot.active === 0 &&
        snapshot.queued === 0 &&
        this.#incomingRequests.size === 0 &&
        this.#pendingAdmissions.size === 0
      ) return;
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

  #enqueue(job: QueuedJob, front = false): boolean {
    if (job.queueLane) throw new Error("Router job is already queued");
    const requestBytes = Math.max(0, job.workload.requestBytes);
    if (
      this.#queuedJobs >= this.#queueCapacity ||
      requestBytes > this.#queueByteCapacity - this.#queuedBytes
    ) return false;

    const key = `${job.queueClass}\0${job.clientId}`;
    let lane = this.#lanes.get(key);
    if (!lane) {
      lane = {
        key,
        clientId: job.clientId,
        queueClass: job.queueClass,
        head: null,
        tail: null,
        size: 0,
        deficitBytes: this.#inactiveLaneDeficits.get(key) ?? 0,
        previous: undefined!,
        next: undefined!,
      };
      lane.previous = lane;
      lane.next = lane;
      this.#inactiveLaneDeficits.delete(key);
      this.#lanes.set(key, lane);
      this.#insertLane(lane);
    }

    job.queueLane = lane;
    if (front) {
      job.queueNext = lane.head;
      if (lane.head) lane.head.queuePrevious = job;
      else lane.tail = job;
      lane.head = job;
    } else {
      job.queuePrevious = lane.tail;
      if (lane.tail) lane.tail.queueNext = job;
      else lane.head = job;
      lane.tail = job;
    }
    lane.size++;
    this.#queuedJobs++;
    this.#queuedBytes += requestBytes;
    this.#clientDepth.set(job.clientId, (this.#clientDepth.get(job.clientId) ?? 0) + 1);
    this.#trackDeadline(job);
    return true;
  }

  #insertLane(lane: ClientLane): void {
    const state = this.#fairClasses[lane.queueClass];
    if (!state.cursor) {
      state.cursor = lane;
    } else {
      const tail = state.cursor.previous;
      tail.next = lane;
      lane.previous = tail;
      lane.next = state.cursor;
      state.cursor.previous = lane;
    }
    state.lanes++;
  }

  #removeLane(lane: ClientLane): void {
    if (lane.size !== 0) throw new Error("Cannot remove a non-empty router lane");
    const state = this.#fairClasses[lane.queueClass];
    if (state.lanes === 1) {
      state.cursor = null;
      state.lanes = 0;
    } else {
      lane.previous.next = lane.next;
      lane.next.previous = lane.previous;
      if (state.cursor === lane) state.cursor = lane.next;
      state.lanes--;
    }
    lane.previous = lane;
    lane.next = lane;
    this.#lanes.delete(lane.key);
    this.#rememberInactiveLaneDeficit(lane.key, lane.deficitBytes);
  }

  #rememberInactiveLaneDeficit(key: string, deficitBytes: number): void {
    this.#inactiveLaneDeficits.delete(key);
    this.#inactiveLaneDeficits.set(key, Math.max(
      -MAX_FAIR_COST_BYTES,
      Math.min(MAX_FAIR_COST_BYTES, deficitBytes),
    ));
    const limit = Math.max(1_024, this.#queueCapacity * 2);
    while (this.#inactiveLaneDeficits.size > limit) {
      this.#inactiveLaneDeficits.delete(this.#inactiveLaneDeficits.keys().next().value!);
    }
  }

  #unlinkQueuedJob(job: QueuedJob): boolean {
    const lane = job.queueLane;
    if (!lane) return false;
    if (job.queuePrevious) job.queuePrevious.queueNext = job.queueNext;
    else lane.head = job.queueNext;
    if (job.queueNext) job.queueNext.queuePrevious = job.queuePrevious;
    else lane.tail = job.queuePrevious;
    job.queueLane = null;
    job.queuePrevious = null;
    job.queueNext = null;
    lane.size--;
    this.#queuedJobs--;
    this.#queuedBytes = Math.max(0, this.#queuedBytes - Math.max(0, job.workload.requestBytes));
    const clientDepth = Math.max(0, (this.#clientDepth.get(job.clientId) ?? 1) - 1);
    if (clientDepth) this.#clientDepth.set(job.clientId, clientDepth);
    else this.#clientDepth.delete(job.clientId);
    if (lane.size === 0) this.#removeLane(lane);
    return true;
  }

  #removeJob(job: QueuedJob): boolean {
    if (!this.#unlinkQueuedJob(job)) return false;
    this.#removeDeadline(job);
    return true;
  }

  #takeFromClass(
    state: FairClassQueue,
    maximumFairCost = Number.POSITIVE_INFINITY,
  ): QueuedJob | null {
    if (!state.cursor || state.lanes < 1) return null;
    // A charged job is at most four client quanta. The bounded rounds avoid an
    // unbounded scan while allowing a large request's byte debt to make room
    // for other continuously backlogged clients.
    const maximumVisits = state.lanes *
      (Math.ceil(MAX_FAIR_COST_BYTES / CLIENT_QUANTUM_BYTES) + 1);
    for (let visit = 0; visit < maximumVisits && state.cursor; visit++) {
      const lane: ClientLane = state.cursor;
      state.cursor = lane.next;
      lane.deficitBytes = Math.min(
        MAX_FAIR_COST_BYTES,
        lane.deficitBytes + CLIENT_QUANTUM_BYTES,
      );
      const job = lane.head;
      if (!job) continue;
      if (
        lane.deficitBytes < job.fairCostBytes ||
        job.fairCostBytes > maximumFairCost
      ) continue;
      this.#unlinkQueuedJob(job);
      this.#removeDeadline(job);
      return job;
    }
    return null;
  }

  #nextJob(): QueuedJob | null {
    if (this.#queuedJobs < 1) return null;
    const latency = this.#fairClasses.latency;
    const bulk = this.#fairClasses.bulk;
    const activeClasses = Number(latency.lanes > 0) + Number(bulk.lanes > 0);
    if (activeClasses === 1) {
      const state = latency.lanes > 0 ? latency : bulk;
      return this.#takeFromClass(state);
    }

    const maximumVisits = 2 *
      (Math.ceil(MAX_FAIR_COST_BYTES / CLASS_QUANTUM_BYTES.bulk) + 1);
    for (let visit = 0; visit < maximumVisits; visit++) {
      const state = this.#fairClasses[this.#classCursor];
      if (state.lanes < 1) {
        state.turnActive = false;
        this.#classCursor = this.#classCursor === "latency" ? "bulk" : "latency";
        continue;
      }
      if (!state.turnActive) {
        state.deficitBytes = Math.min(
          MAX_FAIR_COST_BYTES,
          state.deficitBytes + state.quantumBytes,
        );
        state.turnActive = true;
      }
      const job = this.#takeFromClass(state, state.deficitBytes);
      if (!job) {
        state.turnActive = false;
        this.#classCursor = this.#classCursor === "latency" ? "bulk" : "latency";
        continue;
      }
      return job;
    }
    return null;
  }

  #chargeFairness(job: QueuedJob): void {
    if (job.fairnessCharged) return;
    job.fairnessCharged = true;
    const state = this.#fairClasses[job.queueClass];
    // Persist bounded class debt even while its sole lane is momentarily
    // empty between dispatch and refill.
    state.deficitBytes = Math.max(
      -MAX_FAIR_COST_BYTES,
      state.deficitBytes - job.fairCostBytes,
    );
    const lane = this.#lanes.get(`${job.queueClass}\0${job.clientId}`);
    if (lane) {
      lane.deficitBytes = Math.max(
        -MAX_FAIR_COST_BYTES,
        lane.deficitBytes - job.fairCostBytes,
      );
    } else {
      const key = `${job.queueClass}\0${job.clientId}`;
      this.#rememberInactiveLaneDeficit(
        key,
        (this.#inactiveLaneDeficits.get(key) ?? 0) - job.fairCostBytes,
      );
    }
  }

  #deadlineBefore(left: QueuedJob, right: QueuedJob): boolean {
    return left.queueDeadlineMs < right.queueDeadlineMs ||
      (left.queueDeadlineMs === right.queueDeadlineMs && left.acceptedAtMs < right.acceptedAtMs);
  }

  #swapDeadlines(leftIndex: number, rightIndex: number): void {
    const left = this.#deadlines[leftIndex]!;
    const right = this.#deadlines[rightIndex]!;
    this.#deadlines[leftIndex] = right;
    this.#deadlines[rightIndex] = left;
    right.deadlineIndex = leftIndex;
    left.deadlineIndex = rightIndex;
  }

  #trackDeadline(job: QueuedJob): void {
    if (job.deadlineIndex >= 0) return;
    job.deadlineIndex = this.#deadlines.length;
    this.#deadlines.push(job);
    for (let index = job.deadlineIndex; index > 0;) {
      const parent = Math.floor((index - 1) / 2);
      if (!this.#deadlineBefore(this.#deadlines[index]!, this.#deadlines[parent]!)) break;
      this.#swapDeadlines(index, parent);
      index = parent;
    }
    this.#armDeadlineTimer();
  }

  #removeDeadline(job: QueuedJob): void {
    const index = job.deadlineIndex;
    if (index < 0) return;
    const last = this.#deadlines.pop()!;
    job.deadlineIndex = -1;
    if (index < this.#deadlines.length) {
      this.#deadlines[index] = last;
      last.deadlineIndex = index;
      const parent = Math.floor((index - 1) / 2);
      if (index > 0 && this.#deadlineBefore(last, this.#deadlines[parent]!)) {
        for (let cursor = index; cursor > 0;) {
          const nextParent = Math.floor((cursor - 1) / 2);
          if (!this.#deadlineBefore(this.#deadlines[cursor]!, this.#deadlines[nextParent]!)) break;
          this.#swapDeadlines(cursor, nextParent);
          cursor = nextParent;
        }
      } else {
        for (let cursor = index;;) {
          const left = cursor * 2 + 1;
          const right = left + 1;
          if (left >= this.#deadlines.length) break;
          let child = left;
          if (
            right < this.#deadlines.length &&
            this.#deadlineBefore(this.#deadlines[right]!, this.#deadlines[left]!)
          ) child = right;
          if (!this.#deadlineBefore(this.#deadlines[child]!, this.#deadlines[cursor]!)) break;
          this.#swapDeadlines(cursor, child);
          cursor = child;
        }
      }
    }
    if (this.#deadlines.length === 0) this.#clearDeadlineTimer();
  }

  #armDeadlineTimer(): void {
    const next = this.#deadlines[0];
    if (!next) {
      this.#clearDeadlineTimer();
      return;
    }
    if (
      this.#deadlineTimer &&
      this.#deadlineTimerAtMs !== null &&
      this.#deadlineTimerAtMs <= next.queueDeadlineMs
    ) return;
    this.#clearDeadlineTimer();
    this.#deadlineTimerAtMs = next.queueDeadlineMs;
    this.#deadlineTimer = setTimeout(() => {
      this.#deadlineTimer = undefined;
      this.#deadlineTimerAtMs = null;
      const now = this.#clock.now();
      for (;;) {
        const expired = this.#deadlines[0];
        if (!expired || expired.queueDeadlineMs > now) break;
        this.#removeDeadline(expired);
        if (!this.#unlinkQueuedJob(expired)) continue;
        this.#expireJob(expired);
      }
      this.#armDeadlineTimer();
      this.#scheduleDrain();
    }, Math.max(0, next.queueDeadlineMs - this.#clock.now()));
    this.#deadlineTimer.unref?.();
  }

  #clearDeadlineTimer(): void {
    if (this.#deadlineTimer) clearTimeout(this.#deadlineTimer);
    this.#deadlineTimer = undefined;
    this.#deadlineTimerAtMs = null;
  }

  #clearRetryWake(job: QueuedJob): void {
    if (job.retryWakeTimer) clearTimeout(job.retryWakeTimer);
    job.retryWakeTimer = undefined;
    job.retryWakeAtMs = null;
  }

  #armRetryWake(job: QueuedJob): void {
    this.#clearRetryWake(job);
    const now = this.#clock.now();
    let wakeAt = Number.POSITIVE_INFINITY;
    for (const block of job.excluded.values()) {
      if (block.retryAtMs > now) wakeAt = Math.min(wakeAt, block.retryAtMs);
    }
    if (!Number.isFinite(wakeAt) || wakeAt >= job.queueDeadlineMs) return;
    job.retryWakeAtMs = wakeAt;
    job.retryWakeTimer = setTimeout(() => {
      job.retryWakeTimer = undefined;
      job.retryWakeAtMs = null;
      if (this.#clock.now() < wakeAt) {
        this.#armRetryWake(job);
        return;
      }
      if (job.queueLane) this.#scheduleDrain();
    }, Math.max(0, Math.ceil(wakeAt - this.#clock.now())));
    job.retryWakeTimer.unref?.();
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
    // Scan each job which was queued at drain entry at most once. Temporarily
    // unroutable retry heads go to the back of their fair lane so they cannot
    // stop unrelated clients which can still use a non-excluded runtime.
    let remainingScan = this.#queuedJobs;
    const candidates = [...this.#runtimeCache.values()]
      .map(cached => this.#candidateWithReservations(this.#agedCandidate(cached)))
      .filter(isRuntimeEligible);
    if (candidates.length === 0) return;
    while (remainingScan-- > 0) {
      const job = this.#nextJob();
      if (!job) return;
      if (this.#clock.now() >= job.queueDeadlineMs) {
        this.#expireJob(job);
        continue;
      }
      if (job.signal?.aborted) {
        job.signal.removeEventListener("abort", job.onAbort!);
        this.#clearRetryWake(job);
        job.memoryLease.release();
        const error = abortError();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        job.reject(error);
        continue;
      }
      const routeStarted = this.#clock.now();
      const selection = this.#choose(job, candidates);
      if (!selection) {
        if (!this.#enqueue(job)) {
          job.signal?.removeEventListener("abort", job.onAbort!);
          job.memoryLease.release();
          const error = new RouterQueueFullError();
          this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
          this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
          job.reject(error);
        } else {
          this.#armRetryWake(job);
        }
        continue;
      }
      const runtime = selection.runtime;
      this.#clearRetryWake(job);
      this.#chargeFairness(job);
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
        sandbox_admission_credits: candidate.available ?? null,
        sandbox_reserved: candidate.reserved,
        sandbox_queued: candidate.queueDepth,
        sandbox_quarantined: candidate.quarantined,
        sandbox_commit_backlog: candidate.commitBacklog ?? null,
        sandbox_scheduler_bytes: candidate.schedulerBytes ?? null,
        sandbox_scheduler_oldest_ms: candidate.schedulerOldestMs ?? null,
        sandbox_throughput_bytes_per_sec: candidate.throughputBytesPerSecond ?? null,
        awareness_rtt_ms: candidate.ackEwmaMs,
        completion_ewma_ms: candidate.completionEwmaMs,
        recent_failure_rate: candidate.recentFailureRate,
        heartbeat_age_ms: candidate.heartbeatAgeMs,
        credential_expires_in_ms: candidate.credentialExpiresInMs,
        memory_pressure: candidate.memoryPressure,
        eligible_candidates: selection.eligibleCandidates,
        queue_class: job.queueClass,
        fair_cost_bytes: job.fairCostBytes,
        route_attempt: job.attempts + 1,
      });
      this.#reserveRuntime(job, runtime.projectId);
      // This drain pass uses one immutable fleet snapshot. Charge its local
      // admission credit immediately so queued jobs cannot trigger repeated
      // all-runtime snapshots/scans on every Awareness publication.
      candidate.reserved++;
      if (!isRuntimeEligible(candidate)) {
        const index = candidates.indexOf(candidate);
        if (index >= 0) candidates.splice(index, 1);
      }
      void this.#dispatch(
        job,
        runtime,
        selection.incarnationKey,
        selection.retryEpoch,
      );
      if (candidates.length === 0) return;
    }
  }

  #expireJob(job: QueuedJob): void {
    job.signal?.removeEventListener("abort", job.onAbort!);
    this.#clearRetryWake(job);
    job.memoryLease.release();
    const error = new RouterCapacityTimeoutError();
    this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
    this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
    job.reject(error);
  }

  #routeCandidate(snapshot: RoutableSandboxSnapshot): RouteCandidate {
    const host = snapshot.diagnostics?.host;
    // This value is measured over an immutable flight from first publication
    // through cumulative acknowledgement. Published-byte deltas are not
    // goodput: cumulative state and retransmissions made a lossy runtime look
    // faster precisely while its commit latency was degrading.
    const acknowledgedGoodput = host?.acknowledgedGoodputBytesPerSecond;
    const throughputBytesPerSecond = typeof acknowledgedGoodput === "number" &&
        Number.isFinite(acknowledgedGoodput) && acknowledgedGoodput > 0
      ? acknowledgedGoodput
      : null;
    return {
      ...snapshot,
      available: snapshot.available,
      // Reservations are applied from the live router map when selected.
      reserved: 0,
      commitBacklog: host?.admittedUncommitted ?? snapshot.queueDepth,
      schedulerBytes: host?.schedulerBytes ?? 0,
      schedulerOldestMs: host?.oldestQueuedFrameMs ?? 0,
      throughputBytesPerSecond,
    };
  }

  #refreshRuntime(runtime: RoutableSandboxRuntime): CachedRuntime {
    const snapshot = runtime.snapshot();
    const previous = this.#runtimeCache.get(runtime.projectId);
    const previousSnapshot = previous ? this.#agedSnapshot(previous) : null;
    const retryStateChanged = !previousSnapshot ||
      (previousSnapshot.incarnation ?? 0) !== (snapshot.incarnation ?? 0) ||
      previousSnapshot.state !== snapshot.state ||
      isRuntimeHealthy(previousSnapshot) !== isRuntimeHealthy(snapshot);
    const cached = {
      runtime,
      snapshot,
      candidate: this.#routeCandidate(snapshot),
      observedAtMs: this.#clock.now(),
      retryEpoch: (previous?.retryEpoch ?? 0) + (retryStateChanged ? 1 : 0),
    };
    this.#runtimeCache.set(runtime.projectId, cached);
    return cached;
  }

  #refreshRuntimeSafely(runtime: RoutableSandboxRuntime): void {
    try {
      this.#refreshRuntime(runtime);
    } catch {
      // The supervisor owns snapshot-failure recovery. Retain the last safe
      // cache entry until that runtime publishes another lifecycle update.
    }
  }

  #agedSnapshot(cached: CachedRuntime): RoutableSandboxSnapshot {
    const elapsed = Math.max(0, this.#clock.now() - cached.observedAtMs);
    return {
      ...cached.snapshot,
      heartbeatAgeMs: cached.snapshot.heartbeatAgeMs === null
        ? null
        : cached.snapshot.heartbeatAgeMs + elapsed,
      credentialExpiresInMs: cached.snapshot.credentialExpiresInMs === null
        ? null
        : Math.max(0, cached.snapshot.credentialExpiresInMs - elapsed),
    };
  }

  #agedCandidate(cached: CachedRuntime): RouteCandidate {
    const snapshot = this.#agedSnapshot(cached);
    return {
      ...cached.candidate,
      heartbeatAgeMs: snapshot.heartbeatAgeMs,
      credentialExpiresInMs: snapshot.credentialExpiresInMs,
    };
  }

  #candidateWithReservations(candidate: RouteCandidate): RouteCandidate {
    return {
      ...candidate,
      reserved: this.#unreflectedReservations(candidate.projectId),
    };
  }

  #candidateCredits(candidate: RouteCandidate): number {
    const current = this.#candidateWithReservations(candidate);
    if (!isRuntimeHealthy(current)) return 0;
    const usable = current.capacity - current.quarantined;
    const advertised = Number.isFinite(current.available)
      ? Math.max(0, current.available!)
      : Math.max(0, usable - current.active);
    return Math.max(0, advertised - current.reserved);
  }

  #choose(job: QueuedJob, available: readonly RouteCandidate[]): RouteSelection | null {
    const now = this.#clock.now();
    const candidates = available.filter(
      candidate => {
        const block = job.excluded.get(runtimeIncarnationKey(candidate));
        if (!block) return true;
        const retryEpoch = this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0;
        return retryEpoch > block.retryEpoch || now >= block.retryAtMs;
      },
    );
    const decision = chooseRuntime(job.workload, candidates, this.#random);
    if (!decision) return null;
    const runtime = this.#runtimeCache.get(decision.projectId)?.runtime;
    const candidate = candidates.find(value => value.projectId === decision.projectId);
    return runtime && candidate
      ? {
          runtime,
          candidate,
          incarnationKey: runtimeIncarnationKey(candidate),
          eligibleCandidates: candidates.filter(isRuntimeEligible).length,
          retryEpoch: this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0,
        }
      : null;
  }

  async #dispatch(
    job: QueuedJob,
    runtime: RoutableSandboxRuntime,
    selectedIncarnation: string,
    selectedRetryEpoch: number,
  ): Promise<void> {
    const started = this.#clock.now();
    const requestModel = typeof job.body.model === "string"
      ? job.body.model
      : job.workload.model;
    const admissionController = new AbortController();
    let admissionDeadlineExpired = false;
    let routeAttemptExpired = false;
    let admissionTimer: ReturnType<typeof setTimeout> | undefined;
    const onCallerAbort = () => {
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimer = undefined;
      admissionController.abort(job.signal?.reason ?? abortError());
    };
    if (job.signal?.aborted) onCallerAbort();
    else job.signal?.addEventListener("abort", onCallerAbort, { once: true });
    const remainingAdmissionMs = job.queueDeadlineMs - this.#clock.now();
    if (admissionController.signal.aborted) {
      // Caller cancellation already owns the outcome.
    } else if (remainingAdmissionMs <= 0) {
      admissionDeadlineExpired = true;
      admissionController.abort(new RouterCapacityTimeoutError());
    } else {
      const attemptTimeoutMs = Math.min(
        remainingAdmissionMs,
        this.#routeAttemptTimeoutMs,
      );
      admissionTimer = setTimeout(() => {
        admissionTimer = undefined;
        if (this.#clock.now() >= job.queueDeadlineMs) {
          admissionDeadlineExpired = true;
          admissionController.abort(new RouterCapacityTimeoutError());
        } else {
          routeAttemptExpired = true;
          admissionController.abort(new RouterRouteAttemptTimeoutError());
        }
      }, attemptTimeoutMs);
      admissionTimer.unref?.();
    }
    const clearAdmissionDeadline = () => {
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimer = undefined;
    };
    const detachAdmission = () => {
      clearAdmissionDeadline();
      job.signal?.removeEventListener("abort", onCallerAbort);
    };
    try {
      // Older test seams may ignore the third argument and therefore retain
      // the conservative router reservation through commit. Production V2
      // explicitly calls it only after its request owns an admission credit.
      const handle = await runtime.run(job.body, admissionController.signal, {
        onAdmissionReflected: () => {
          clearAdmissionDeadline();
          this.#refreshRuntimeSafely(runtime);
          this.#releaseRuntimeReservation(job, runtime.projectId);
          this.#scheduleDrain();
        },
      });
      // Commitment ends the admission deadline immediately. Optional memory
      // accounting work must never turn an accepted upstream request into a
      // local capacity timeout or inflate the measured commit ACK latency.
      clearAdmissionDeadline();
      const committedAtMs = this.#clock.now();
      const commitAckMs = Math.max(0, committedAtMs - started);
      // The transport now owns canonical request bytes. Drop the router's
      // payload before reducing its accounting, not one microtask afterward.
      job.body = {};
      if (
        !job.retainFullLeaseAfterCommit &&
        job.memoryLease.reservedBytes > POST_COMMIT_RESERVATION_BYTES
      ) {
        const committedLease = job.memoryLease;
        try {
          job.memoryLease = await committedLease.replace({
            ...job.workload,
            reservedBytes: POST_COMMIT_RESERVATION_BYTES,
            cpuPermits: 0,
          });
        } catch {
          // Downgrading is an accounting optimization, never part of request
          // correctness. A failed replacement leaves the original lease fully
          // active by contract, so retain it and continue the committed run.
          job.memoryLease = committedLease;
        }
      }
      // The admission deadline ends at commit, but caller cancellation must
      // continue flowing through the same signal for the request's lifetime.
      this.#refreshRuntimeSafely(runtime);
      this.#releaseRuntimeReservation(job, runtime.projectId);
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
          request_decoded_bytes: handle.stats.diagnostics.requestDecodedBytes,
          request_encoded_bytes: handle.stats.diagnostics.requestEncodedBytes,
          request_part_count: handle.stats.diagnostics.requestPartCount,
          request_encoding: handle.stats.diagnostics.requestEncoding,
          request_compression_ratio:
            handle.stats.diagnostics.requestDecodedBytes &&
              handle.stats.diagnostics.requestEncodedBytes !== null
              ? handle.stats.diagnostics.requestEncodedBytes /
                handle.stats.diagnostics.requestDecodedBytes
              : null,
          host_scheduler_wait_ms: handle.stats.diagnostics.hostSchedulerWaitMs,
          host_publish_to_ack_ms: handle.stats.diagnostics.hostPublishToAckMs,
          sidecar_first_frame_to_commit_ms: handle.stats.diagnostics.sidecarFirstFrameToCommitMs,
          sidecar_commit_to_assembly_ms: handle.stats.diagnostics.sidecarCommitToAssemblyMs,
          sidecar_claim_sync_ms: handle.stats.diagnostics.sidecarClaimSyncMs,
          sidecar_admission_wait_ms: handle.stats.diagnostics.sidecarAdmissionWaitMs,
          sidecar_prepared_to_fetch_start_ms:
            handle.stats.diagnostics.sidecarPreparedToFetchStartMs,
          sidecar_fetch_start_sync_ms: handle.stats.diagnostics.sidecarFetchStartSyncMs,
          sidecar_commit_processing_ms: handle.stats.diagnostics.sidecarCommitProcessingMs,
          transport_commit_residual_ms: handle.stats.diagnostics.transportCommitResidualMs,
        } : {}),
      });
      this.#traffic.committed(job.queueWaitMs, commitAckMs);
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
      void handle.final.then(
        final => {
          const hint = rateLimitHintFromError(final, handle.rateLimit);
          this.#traffic.settled({
            totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs),
            firstByteMs: handle.stats.firstByteMs,
            responseBytes: handle.stats.bytes,
            succeeded: final?.status === "completed",
            rateLimited: !!hint,
          });
          if (hint) this.#observeRateLimit(
            hint,
            rateLimitProblemFromError(final),
            runtime.projectId,
            requestModel,
            selectedIncarnation,
          );
        },
        error => {
          const hint = rateLimitHintFromError(error);
          this.#traffic.settled({
            totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs),
            firstByteMs: handle.stats.firstByteMs,
            responseBytes: handle.stats.bytes,
            succeeded: false,
            rateLimited: !!hint,
          });
          if (hint) this.#observeRateLimit(
            hint,
            rateLimitProblemFromError(error),
            runtime.projectId,
            requestModel,
            selectedIncarnation,
          );
        },
      );
      // All post-commit callbacks capture only scalar metadata.
      job.resolve(traced);
      void traced.final.then(
        () => undefined,
        () => undefined,
      ).finally(() => {
        job.signal?.removeEventListener("abort", onCallerAbort);
        job.memoryLease.release();
        this.#refreshRuntimeSafely(runtime);
        void started;
        this.#scheduleDrain();
      });
      this.#scheduleDrain();
    } catch (error) {
      detachAdmission();
      this.#releaseRuntimeReservation(job, runtime.projectId);
      const commitment = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      if (
        admissionDeadlineExpired &&
        !isDeterministicRequestTooLarge(error) &&
        commitment.committed !== true &&
        commitment.ambiguous !== true
      ) {
        this.#expireJob(job);
        this.#scheduleDrain();
        return;
      }
      const rateLimit = rateLimitHintFromError(error);
      if (rateLimit) {
        const problem = rateLimitProblemFromError(error);
        this.#observeRateLimit(
          rateLimit,
          problem,
          runtime.projectId,
          requestModel,
          selectedIncarnation,
        );
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#traffic.rejected({
          totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs),
          rateLimited: true,
        });
        this.#failure(job.traceId, job.acceptedAtMs, "rate-limit", error, job.attempts);
        job.reject(error);
        this.#scheduleDrain();
        return;
      }
      if (isLocalCpuTransformFailure(error)) {
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "host-transform", error, job.attempts);
        job.reject(error);
        this.#scheduleDrain();
        return;
      }
      const value = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      const committed = value.committed === true;
      const ambiguous = value.ambiguous === true;
      const timedOutBeforeCommit = routeAttemptExpired &&
        !isDeterministicRequestTooLarge(error) &&
        !committed &&
        !ambiguous;
      const retryablePreCommit = timedOutBeforeCommit || isRetryablePreCommitFailure(error);
      if (
        retryablePreCommit &&
        !job.signal?.aborted &&
        !this.#shuttingDown &&
        this.#clock.now() >= job.queueDeadlineMs
      ) {
        this.#expireJob(job);
        this.#scheduleDrain();
        return;
      }
      if (
        retryablePreCommit &&
        !job.signal?.aborted &&
        !this.#shuttingDown &&
        this.#clock.now() < job.queueDeadlineMs
      ) {
        job.attempts++;
        this.#traffic.retried();
        const previousBlock = job.excluded.get(selectedIncarnation);
        const failures = (previousBlock?.failures ?? 0) + 1;
        const retryDelayMs = Math.min(
          PRECOMMIT_RETRY_MAX_MS,
          PRECOMMIT_RETRY_BASE_MS * 2 ** Math.min(4, failures - 1),
        );
        job.excluded.set(selectedIncarnation, {
          retryEpoch: selectedRetryEpoch,
          retryAtMs: Math.min(
            job.queueDeadlineMs,
            this.#clock.now() + retryDelayMs,
          ),
          failures,
        });
        if (!this.#enqueue(job, true)) {
          job.signal?.removeEventListener("abort", job.onAbort!);
          job.memoryLease.release();
          const queueError = new RouterQueueFullError();
          this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
          this.#failure(job.traceId, job.acceptedAtMs, "queue", queueError, job.attempts);
          job.reject(queueError);
        } else {
          this.#armRetryWake(job);
        }
      } else {
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(
          job.traceId,
          job.acceptedAtMs,
          committed || ambiguous ? "commit-ambiguous" : "pre-commit",
          error,
          job.attempts,
        );
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
      error_kind: typeof value.kind === "string"
        ? value.kind
        : typeof value.code === "string"
          ? value.code
          : null,
      status: typeof value.status === "number" ? value.status : null,
      error_message: sanitizeDiagnosticMessage(error instanceof Error ? error.message : error),
      committed: value.committed === true,
      quarantined: value.ambiguous === true,
      retry_count: retryCount,
    });
  }

  #observeRateLimit(
    hint: RateLimitHint,
    problem?: RateLimitProblem,
    projectId?: ProjectId,
    requestedModel?: unknown,
    observedIncarnation?: string,
  ): void {
    if (!projectId) return;
    const current = this.#runtimeCache.get(projectId);
    if (
      !observedIncarnation ||
      !current ||
      runtimeIncarnationKey(current.snapshot) !== observedIncarnation
    ) return;
    const normalizedRequestedModel = typeof requestedModel === "string" && requestedModel.trim()
      ? requestedModel.trim().toLowerCase()
      : null;
    const rawModel = problem?.model ?? normalizedRequestedModel;
    const model = rawModel && /^[a-z0-9][a-z0-9._:-]{0,127}$/.test(rawModel)
      ? rawModel
      : "unknown";
    let byModel = this.#rateLimitObservations.get(projectId);
    if (!byModel) {
      byModel = new Map();
      this.#rateLimitObservations.set(projectId, byModel);
    }
    byModel.delete(model);
    byModel.set(model, {
      hint: Object.freeze({ ...hint }),
      problem: problem ?? null,
      requestedModel: normalizedRequestedModel,
      observedAtMs: this.#clock.now(),
      incarnationKey: observedIncarnation,
    });
    while (byModel.size > 16) byModel.delete(byModel.keys().next().value!);
  }
}
