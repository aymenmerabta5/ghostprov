import { SYSTEM_CLOCK, SYSTEM_RANDOM, type Clock, type RandomSource } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import {
  SandboxRegistry,
  type SandboxAdmissionProgress,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
  type SandboxRunOptions,
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
import {
  classifyWorkload,
  jsonEncodedByteLength,
  type WorkloadDescriptor,
} from "./workload";
import type { LoggerLike } from "../observability/logger";
import { observabilityEvent, type JsonValue } from "../observability/events";
import { sanitizeDiagnosticMessage } from "../observability/redaction";
import { traceRunHandle } from "../observability/traced-run-handle";
import { validateResponsesInputJson } from "../relay/responses-validation";
import { normalizeUpstreamResponsesRequest } from "../relay/upstream-policy";
import { applyUpstreamServiceTierPolicy } from "../relay/service-tier-policy";
import {
  releaseAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import {
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";
import {
  ResponseProvenanceCapacityError,
  ResponseProvenanceCommitError,
  ResponseProvenanceRegistry,
  ResponseProvenanceScopeError,
  type ResponseProvenance,
  type ResponseProvenanceLookup,
  type ResponseProvenanceReservation,
  type ResponseProvenanceSnapshot,
} from "./provenance";
import {
  v2HostTransferBudget,
  v2RequestTransferCost,
} from "../v2/transport/admission-cost";
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
const ROUTE_CANCELLATION_PROOF_GRACE_MS = 25;
const PAYLOAD_AWARE_DEADLINE_BYTES = 8 * MiB;
const PROGRESS_AWARE_MIN_REQUEST_BYTES = 2 * MiB;
const MEDIUM_PROGRESS_PROBE_MIN_BYTES = 256 * KiB;
const FLEET_TRANSFER_EXEMPT_BYTES = 256 * KiB;
const FLEET_TRANSFER_PACED_MAX_BYTES = 3 * MiB;
const FLEET_TRANSFER_BUDGET = 4;
const FLEET_TRANSFER_PACER_BURST = 2;
const FLEET_TRANSFER_PACER_REFILL_TOKENS = 2;
const FLEET_TRANSFER_PACER_INTERVAL_MS = 500;
const MIN_ADMISSION_PROGRESS_FRESHNESS_MS = 5_000;
const MAX_ADMISSION_PROGRESS_FRESHNESS_MS = 10_000;
const MIN_PREPARED_PROOF_GRACE_MS = 3_000;
const MAX_PREPARED_PROOF_GRACE_MS = 5_000;
const MIN_FIRST_PROGRESS_TIMEOUT_MS = 5_000;
const MAX_FIRST_PROGRESS_TIMEOUT_MS = 8_000;
const MAX_TIMER_MS = 2_147_483_647;
const RESPONSE_TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.incomplete",
  "response.failed",
]);

/** @internal Deterministic bulk-profile admission-progress policy; exported for timing proofs. */
export function admissionProgressFreshnessMs(selectedRttMs: number): number {
  const finiteRttMs = Number.isFinite(selectedRttMs) ? Math.max(0, selectedRttMs) : 0;
  return Math.min(
    MAX_ADMISSION_PROGRESS_FRESHNESS_MS,
    Math.max(MIN_ADMISSION_PROGRESS_FRESHNESS_MS, Math.ceil(4 * finiteRttMs)),
  );
}

/** @internal Bounded final-body-ACK to exact prepared-proof allowance. */
export function preparedProofGraceMs(selectedRttMs: number): number {
  const finiteRttMs = Number.isFinite(selectedRttMs) ? Math.max(0, selectedRttMs) : 0;
  return Math.min(
    MAX_PREPARED_PROOF_GRACE_MS,
    Math.max(MIN_PREPARED_PROOF_GRACE_MS, Math.ceil(4 * finiteRttMs)),
  );
}

/** @internal First cumulative-ACK bound for medium bulk-profile transfers. */
export function mediumFirstProgressTimeoutMs(selectedRttMs: number): number {
  const finiteRttMs = Number.isFinite(selectedRttMs) ? Math.max(0, selectedRttMs) : 0;
  return Math.min(
    MAX_FIRST_PROGRESS_TIMEOUT_MS,
    Math.max(MIN_FIRST_PROGRESS_TIMEOUT_MS, Math.ceil(8 * finiteRttMs)),
  );
}

/** @internal Returns the next bounded progress check, or null when retry is due. */
export function nextAdmissionProgressCheckAtMs(
  nowMs: number,
  absoluteDeadlineMs: number,
  lastProgressAtMs: number | null,
  freshnessMs: number,
  retainStaleProgress = false,
): number | null {
  if (nowMs >= absoluteDeadlineMs || lastProgressAtMs === null) return null;
  if (nowMs - lastProgressAtMs < freshnessMs) {
    return Math.min(absoluteDeadlineMs, lastProgressAtMs + freshnessMs);
  }
  if (retainStaleProgress) return absoluteDeadlineMs;
  // A late-admitted multi-megabyte request may spend most of its global
  // deadline waiting behind the bounded fleet transfer gate. Once less than
  // one RTT-derived freshness window remains, a full retransmit cannot receive
  // stronger liveness evidence before the same hard deadline. Preserve the
  // already-acknowledged attempt instead of deterministically discarding its
  // bytes for a doomed retry. No-progress attempts still retry unchanged.
  return absoluteDeadlineMs - nowMs <= freshnessMs ? absoluteDeadlineMs : null;
}

/**
 * Keep a stale transfer only when protocol proof shows that the current copy
 * can still finish but retransmitting the full body at its measured unique-ACK
 * rate cannot. This is deliberately stricter than a byte-percentage heuristic:
 * scant progress, implausibly fast samples, and transfers which no longer fit
 * all retain the established retry behavior.
 *
 * @internal Exported for deterministic timing proofs.
 */
export function shouldRetainStaleAdmissionProgress(
  nowMs: number,
  absoluteDeadlineMs: number,
  attemptStartedAtMs: number,
  lastProgressAtMs: number | null,
  freshnessMs: number,
  progress: SandboxAdmissionProgress | null,
): boolean {
  if (
    lastProgressAtMs === null || progress === null ||
    nowMs >= absoluteDeadlineMs || freshnessMs < 1 ||
    !Number.isFinite(nowMs) || !Number.isFinite(absoluteDeadlineMs) ||
    !Number.isFinite(attemptStartedAtMs) || !Number.isFinite(lastProgressAtMs) ||
    !Number.isSafeInteger(progress.acknowledgedEncodedBytes) ||
    !Number.isSafeInteger(progress.totalEncodedBytes) ||
    progress.totalEncodedBytes < 1 ||
    progress.acknowledgedEncodedBytes < 1 ||
    progress.acknowledgedEncodedBytes > progress.totalEncodedBytes
  ) return false;
  if (
    progress.stage === "request_prepared" &&
    progress.acknowledgedEncodedBytes === progress.totalEncodedBytes
  ) return true;
  if (progress.stage !== "request_data") return false;
  // All body frames are present, but the sidecar has not produced the exact
  // durable request_prepared proof within its bounded proof grace. That is a
  // preparation stall, not useful transfer progress; another sandbox is the
  // only remaining pre-execution recovery path.
  if (progress.acknowledgedEncodedBytes === progress.totalEncodedBytes) return false;
  // A single early flight is not enough evidence to sacrifice failover. At
  // least half of the exact encoded body must already be cumulatively ACKed.
  if (progress.acknowledgedEncodedBytes * 2 < progress.totalEncodedBytes) return false;
  const progressElapsedMs = lastProgressAtMs - attemptStartedAtMs;
  if (!Number.isFinite(progressElapsedMs) || progressElapsedMs < 1) return false;
  const remainingBudgetMs = absoluteDeadlineMs - nowMs;
  const millisecondsPerByte = progressElapsedMs / progress.acknowledgedEncodedBytes;
  const currentRemainingMs = Math.ceil(
    (progress.totalEncodedBytes - progress.acknowledgedEncodedBytes) * millisecondsPerByte,
  );
  const fullRetransmitMs = Math.ceil(progress.totalEncodedBytes * millisecondsPerByte);
  return currentRemainingMs + freshnessMs <= remainingBudgetMs &&
    fullRetransmitMs + freshnessMs > remainingBudgetMs;
}

export interface RequestRoutingTimeouts {
  queueTimeoutMs: number;
  routeAttemptTimeoutMs: number;
}

function scaledRouteAttemptTimeoutMs(requestBytes: number, baseTimeoutMs: number): number {
  if (requestBytes <= PAYLOAD_AWARE_DEADLINE_BYTES) return baseTimeoutMs;
  return Math.min(
    MAX_TIMER_MS,
    Math.ceil(baseTimeoutMs * (requestBytes / PAYLOAD_AWARE_DEADLINE_BYTES)),
  );
}

function scaledQueueTimeoutMs(requestBytes: number, baseTimeoutMs: number): number {
  if (requestBytes <= PAYLOAD_AWARE_DEADLINE_BYTES) return baseTimeoutMs;
  const scale = requestBytes / PAYLOAD_AWARE_DEADLINE_BYTES;
  const failoverSlackMs = Math.max(1, Math.ceil(baseTimeoutMs / 6));
  return Math.min(
    MAX_TIMER_MS,
    Math.ceil(baseTimeoutMs * scale) + failoverSlackMs,
  );
}

/**
 * Keep the established deadlines byte-for-byte for requests through 8 MiB.
 * Above that boundary, scale by payload size so a legal 64 MiB transfer gets
 * enough ACK rounds even on a 400 ms link. The global deadline retains one-sixth
 * of its base as failover slack beyond two normal 15-second attempts.
 */
export function requestRoutingTimeouts(
  requestBytes: number,
  baseQueueTimeoutMs: number,
  baseRouteAttemptTimeoutMs: number,
): RequestRoutingTimeouts {
  if (!Number.isSafeInteger(requestBytes) || requestBytes < 0) {
    throw new RangeError("requestBytes must be a non-negative safe integer");
  }
  for (const [label, value] of [
    ["baseQueueTimeoutMs", baseQueueTimeoutMs],
    ["baseRouteAttemptTimeoutMs", baseRouteAttemptTimeoutMs],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_MS) {
      throw new RangeError(`${label} must be between 1 and ${MAX_TIMER_MS}`);
    }
  }
  if (baseRouteAttemptTimeoutMs > baseQueueTimeoutMs) {
    throw new RangeError("baseRouteAttemptTimeoutMs must not exceed baseQueueTimeoutMs");
  }
  const routeAttemptTimeoutMs = scaledRouteAttemptTimeoutMs(
    requestBytes,
    baseRouteAttemptTimeoutMs,
  );
  const queueTimeoutMs = scaledQueueTimeoutMs(requestBytes, baseQueueTimeoutMs);
  return Object.freeze({ queueTimeoutMs, routeAttemptTimeoutMs });
}

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
  /** Stops ingress deadline/cancellation tracking while retaining byte ownership. */
  completeAdmission?(): void;
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
  /** Opaque server-authenticated tenant/provider-account authority. */
  authorizationScope?: AuthenticatedTenantScope;
  /** Known wire bytes, avoiding a second full-body serialization for sizing. */
  encodedBodyBytes?: number;
  /** Exact JSON structural bytes for body, avoiding another hot-path traversal. */
  structuralBodyBytes?: number;
  /** Preprocessing bytes charged only to fair queue scheduling. */
  fairnessRequestBytes?: number;
}

interface QueuedJob {
  body: Record<string, unknown>;
  workload: WorkloadDescriptor;
  clientId: string;
  source: RelaySource;
  authorizationScope: AuthenticatedTenantScope | null;
  continuationProvenance: ResponseProvenance | null;
  provenanceReservation: ResponseProvenanceReservation | null;
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
  routeAttemptTimeoutMs: number;
  queueClass: RouterQueueClass;
  fairCostBytes: number;
  admissionCost: number;
  standardAdmissionCost: number;
  bulkAdmissionCost: number;
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
  incarnation: number;
  eligibleCandidates: number;
  retryEpoch: number;
  admissionCost: number;
  fleetTransferCost: number;
}

interface FleetTransferLease {
  cost: number;
  released: boolean;
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

export interface MultiSandboxRouterSnapshot {
  capacity: number;
  active: number;
  available: number;
  queued: number;
  queuedBytes?: number;
  fleetTransferReserved: number;
  fleetTransferBudget: number;
  provenance?: ResponseProvenanceSnapshot;
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
  readonly #responseProvenance: ResponseProvenanceRegistry;
  readonly #defaultAuthorizationScope: AuthenticatedTenantScope | null;
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
  #fleetTransferReserved = 0;
  #fleetTransferPacerTokens = FLEET_TRANSFER_PACER_BURST;
  #fleetTransferPacerNextTokenAtMs: number | null = null;
  #fleetTransferPacerTimer?: ReturnType<typeof setTimeout>;
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
    /** Opaque server-created tenant/provider-account scope. */
    authorizationScope?: AuthenticatedTenantScope;
    observability?: { slowRequestMs?: number; transport?: string };
  }) {
    this.#registry = options.registry;
    this.#governor = options.governor;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    if (
      options.authorizationScope !== undefined &&
      !isAuthenticatedTenantScope(options.authorizationScope)
    ) {
      throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
    }
    this.#defaultAuthorizationScope = options.authorizationScope ?? null;
    this.#responseProvenance = new ResponseProvenanceRegistry(() => this.#clock.now());
    this.#logger = options.logger;
    this.#slowRequestMs = options.observability?.slowRequestMs ?? 10_000;
    this.#transport = options.observability?.transport ?? "unknown";
    this.#queueTimeoutMs = options.queueTimeoutMs ?? 30_000;
    if (
      !Number.isSafeInteger(this.#queueTimeoutMs) ||
      this.#queueTimeoutMs < 1 ||
      this.#queueTimeoutMs > MAX_TIMER_MS
    ) {
      throw new RangeError("queueTimeoutMs must be a positive safe integer");
    }
    this.#routeAttemptTimeoutMs = options.routeAttemptTimeoutMs ?? Math.min(
      DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS,
      this.#queueTimeoutMs,
    );
    if (
      !Number.isSafeInteger(this.#routeAttemptTimeoutMs) ||
      this.#routeAttemptTimeoutMs < 1 ||
      this.#routeAttemptTimeoutMs > MAX_TIMER_MS ||
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
    const traceId = this.#random.uuid();
    const source = options.source ?? "internal";
    let authorizationScope: AuthenticatedTenantScope | null =
      options.authorizationScope ?? this.#defaultAuthorizationScope;
    if (
      authorizationScope !== null &&
      !isAuthenticatedTenantScope(authorizationScope)
    ) {
      options.incomingLease?.release();
      const error = new ResponseProvenanceScopeError();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "continuation", error, 0);
      throw error;
    }
    // This is the shared routing choke point. Public and producer-specific
    // callers normalize defensively too, while copy-on-write keeps client-facing
    // stream semantics and caller-owned request objects unchanged.
    const normalizedBody = normalizeUpstreamResponsesRequest(body);
    const relayBody: Record<string, unknown> = applyUpstreamServiceTierPolicy(normalizedBody);
    const hasPreviousResponse = Object.hasOwn(relayBody, "previous_response_id") &&
      relayBody.previous_response_id !== undefined &&
      relayBody.previous_response_id !== null;
    if (hasPreviousResponse && authorizationScope === null) {
      options.incomingLease?.release();
      const error = new ResponseProvenanceScopeError();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "continuation", error, 0);
      throw error;
    }
    const provenanceLookup = authorizationScope === null
      ? ({ kind: "unknown" } as const)
      : this.#responseProvenance.lookup(
          relayBody.previous_response_id,
          authorizationScope,
        );
    if (provenanceLookup.kind === "scope_mismatch") {
      options.incomingLease?.release();
      const error = new ResponseProvenanceScopeError();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "continuation", error, 0);
      throw error;
    }
    const continuationProvenance = provenanceLookup.kind === "known"
      ? provenanceLookup.provenance
      : null;
    const incomingBodyBytes = source === "http" ? options.incomingLease?.bodyBytes : undefined;
    const encodedBodyBytes = options.encodedBodyBytes ?? incomingBodyBytes;
    for (const [name, value] of [
      ["encodedBodyBytes", encodedBodyBytes],
      ["structuralBodyBytes", options.structuralBodyBytes],
      ["fairnessRequestBytes", options.fairnessRequestBytes],
    ] as const) {
      if (value !== undefined && (!Number.isSafeInteger(value) || value < 0)) {
        throw new RangeError(`${name} must be a non-negative safe integer`);
      }
    }
    const originalStructuralBytes = options.structuralBodyBytes ?? jsonEncodedByteLength(body);
    const relayStructuralBytes = relayBody === body
      ? originalStructuralBytes
      : jsonEncodedByteLength(relayBody);
    const adjustedEncodedBytes = encodedBodyBytes === undefined
      ? undefined
      : Math.min(
          Number.MAX_SAFE_INTEGER,
          Math.max(0, encodedBodyBytes - originalStructuralBytes) + relayStructuralBytes,
        );
    const workload = classifyWorkload(relayBody, {
      structuralBytes: relayStructuralBytes,
      ...(adjustedEncodedBytes === undefined ? {} : { encodedBytes: adjustedEncodedBytes }),
    });
    const schedulingWorkload = options.fairnessRequestBytes !== undefined &&
        options.fairnessRequestBytes > workload.requestBytes
      ? classifyWorkload(relayBody, {
          encodedBytes: options.fairnessRequestBytes,
          structuralBytes: relayStructuralBytes,
        })
      : workload;
    const queueTimeoutMs = scaledQueueTimeoutMs(
      workload.requestBytes,
      this.#queueTimeoutMs,
    );
    const routeAttemptTimeoutMs = scaledRouteAttemptTimeoutMs(
      workload.requestBytes,
      this.#routeAttemptTimeoutMs,
    );
    const payloadDeadlineMs = acceptedAtMs + queueTimeoutMs;
    const admissionDeadlineMs = typeof inheritedDeadlineMs === "number" &&
        Number.isFinite(inheritedDeadlineMs) && inheritedDeadlineMs >= acceptedAtMs
      ? Math.max(inheritedDeadlineMs, payloadDeadlineMs)
      : payloadDeadlineMs;
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
    const standardAdmissionCost = v2RequestTransferCost(workload.requestBytes, "standard");
    const bulkAdmissionCost = v2RequestTransferCost(workload.requestBytes, "bulk-v1");
    const initial = this.snapshot();
    this.#emit("request.accepted", {
      trace_id: traceId,
      source: options.source ?? "internal",
      model: workload.model ?? "unknown",
      service_tier: typeof relayBody.service_tier === "string" ? relayBody.service_tier : "unknown",
      stream: workload.stream,
      workload_class: schedulingWorkload.class,
      request_bytes: schedulingWorkload.requestBytes,
      // No runtime is selected yet; emit the conservative rollback profile.
      transfer_admission_cost: standardAdmissionCost,
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
    const provenanceReservation = authorizationScope === null
      ? null
      : this.#responseProvenance.reserve();
    if (authorizationScope !== null && provenanceReservation === null) {
      options.incomingLease?.release();
      const error = new ResponseProvenanceCapacityError();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "continuation", error, 0);
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
      this.#responseProvenance.release(provenanceReservation);
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
      this.#responseProvenance.release(provenanceReservation);
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
        authorizationScope,
        continuationProvenance,
        provenanceReservation,
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
        routeAttemptTimeoutMs,
        queueClass: queueClassFor(schedulingWorkload),
        fairCostBytes: fairCostBytes(schedulingWorkload),
        admissionCost: standardAdmissionCost,
        standardAdmissionCost,
        bulkAdmissionCost,
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
        this.#releaseJobProvenance(job);
        const error = abortError();
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        reject(error);
      };
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      if (!this.#enqueue(job)) {
        options.signal?.removeEventListener("abort", job.onAbort);
        memoryLease.release();
        this.#releaseJobProvenance(job);
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
    let deadlineMs = acceptedAtMs + scaledQueueTimeoutMs(bytes, this.#queueTimeoutMs);
    const controller = new AbortController();
    const tracker: IncomingRequestTracker = { controller };
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
    let active = true;
    let admissionActive = true;
    let lease: ResourceLease | null = null;
    let observedBodyBytes = bytes;
    const onCallerAbort = () => controller.abort(signal?.reason ?? abortError());
    const armDeadline = () => {
      if (deadlineTimer) clearTimeout(deadlineTimer);
      deadlineTimer = setTimeout(() => {
        deadlineTimer = undefined;
        controller.abort(new RouterCapacityTimeoutError(
          "Request body was not received before the routing deadline",
        ));
      }, Math.max(0, deadlineMs - this.#clock.now()));
      deadlineTimer.unref?.();
    };
    const completeAdmission = () => {
      if (!admissionActive) return;
      admissionActive = false;
      if (deadlineTimer) clearTimeout(deadlineTimer);
      deadlineTimer = undefined;
      signal?.removeEventListener("abort", onCallerAbort);
      this.#incomingRequests.delete(tracker);
    };
    const cleanup = () => {
      if (!active) return;
      active = false;
      completeAdmission();
    };
    if (signal?.aborted) onCallerAbort();
    else signal?.addEventListener("abort", onCallerAbort, { once: true });
    armDeadline();
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
      get deadlineMs() { return deadlineMs; },
      signal: controller.signal,
      completeAdmission,
      ensureBytes: async nextBytes => {
        if (!Number.isSafeInteger(nextBytes) || nextBytes < 0) {
          throw new RangeError("Incoming body size must be a non-negative safe integer");
        }
        if (!active || !admissionActive || !lease) {
          throw new Error("Incoming body lease is no longer accepting bytes");
        }
        observedBodyBytes = Math.max(observedBodyBytes, nextBytes);
        if (observedBodyBytes > PAYLOAD_AWARE_DEADLINE_BYTES) {
          const nextDeadlineMs = acceptedAtMs + scaledQueueTimeoutMs(
            observedBodyBytes,
            this.#queueTimeoutMs,
          );
          if (nextDeadlineMs > deadlineMs) {
            deadlineMs = nextDeadlineMs;
            armDeadline();
          }
        }
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
        if (!active || !admissionActive || !lease) {
          throw new Error("Incoming body lease is no longer available for replacement");
        }
        if (replacementSignal?.aborted) throw replacementSignal.reason ?? abortError();
        const maximumDeadline = acceptedAtMs + scaledQueueTimeoutMs(
          workload.requestBytes,
          this.#queueTimeoutMs,
        );
        const requestedDeadline = options?.deadlineMs;
        const effectiveDeadline = typeof requestedDeadline === "number" &&
            Number.isFinite(requestedDeadline)
          ? Math.min(maximumDeadline, requestedDeadline)
          : maximumDeadline;
        // Body collection is complete before production calls replace(). The
        // replacement governor wait now owns its payload-aware deadline.
        if (deadlineTimer) clearTimeout(deadlineTimer);
        deadlineTimer = undefined;
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

  /** Server-integration seam for the exact app-owned scope, if configured. */
  responseProvenanceAuthorizationScope(): AuthenticatedTenantScope | null {
    return this.#defaultAuthorizationScope;
  }

  /** Scope-aware lookup; a missing or untrusted scope fails closed. */
  lookupResponseProvenance(
    responseId: unknown,
    authorizationScope?: AuthenticatedTenantScope,
  ): ResponseProvenanceLookup {
    const scope = authorizationScope ?? this.#defaultAuthorizationScope;
    if (!scope || !isAuthenticatedTenantScope(scope)) {
      return { kind: "scope_mismatch" };
    }
    return this.#responseProvenance.lookup(responseId, scope);
  }

  trafficSnapshotAndReset(): RouterTrafficSnapshot {
    return this.#traffic.snapshotAndReset();
  }

  #unreflectedReservationCount(projectId: ProjectId): number {
    return this.#reserved.get(projectId)?.size ?? 0;
  }

  #unreflectedReservationCost(projectId: ProjectId): number {
    const jobs = this.#reserved.get(projectId);
    if (!jobs) return 0;
    let cost = 0;
    for (const job of jobs) cost += job.admissionCost;
    return cost;
  }

  #releaseJobProvenance(job: QueuedJob): void {
    this.#responseProvenance.release(job.provenanceReservation);
    job.provenanceReservation = null;
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

  #reserveFleetTransfer(cost: number): FleetTransferLease | null {
    if (cost <= 0) return null;
    if (this.#fleetTransferReserved + cost > FLEET_TRANSFER_BUDGET) {
      throw new Error("Fleet transfer budget was oversubscribed after route selection");
    }
    this.#fleetTransferReserved += cost;
    return { cost, released: false };
  }

  #releaseFleetTransfer(lease: FleetTransferLease | null): boolean {
    if (!lease || lease.released) return false;
    lease.released = true;
    this.#fleetTransferReserved = Math.max(0, this.#fleetTransferReserved - lease.cost);
    return true;
  }

  #downgradeFleetTransferToControl(lease: FleetTransferLease | null): boolean {
    if (!lease || lease.released || lease.cost <= 1) return false;
    const releasedCost = lease.cost - 1;
    lease.cost = 1;
    this.#fleetTransferReserved = Math.max(
      0,
      this.#fleetTransferReserved - releasedCost,
    );
    return true;
  }

  #refillFleetTransferPacer(now = this.#clock.now()): void {
    const nextAt = this.#fleetTransferPacerNextTokenAtMs;
    if (nextAt === null || now < nextAt) return;
    const intervals = 1 + Math.floor(
      (now - nextAt) / FLEET_TRANSFER_PACER_INTERVAL_MS,
    );
    const tokens = intervals * FLEET_TRANSFER_PACER_REFILL_TOKENS;
    this.#fleetTransferPacerTokens = Math.min(
      FLEET_TRANSFER_PACER_BURST,
      this.#fleetTransferPacerTokens + tokens,
    );
    this.#fleetTransferPacerNextTokenAtMs =
      this.#fleetTransferPacerTokens >= FLEET_TRANSFER_PACER_BURST
        ? null
        : nextAt + intervals * FLEET_TRANSFER_PACER_INTERVAL_MS;
  }

  #consumeFleetTransferPacer(cost: number, now: number): void {
    this.#refillFleetTransferPacer(now);
    if (cost < 1 || this.#fleetTransferPacerTokens < cost) {
      throw new Error("Fleet transfer pacer was consumed without a token");
    }
    this.#fleetTransferPacerTokens -= cost;
    this.#fleetTransferPacerNextTokenAtMs ??= now + FLEET_TRANSFER_PACER_INTERVAL_MS;
  }

  #armFleetTransferPacer(): void {
    if (this.#fleetTransferPacerTimer || this.#fleetTransferPacerNextTokenAtMs === null) return;
    const wakeAt = this.#fleetTransferPacerNextTokenAtMs;
    this.#fleetTransferPacerTimer = setTimeout(() => {
      this.#fleetTransferPacerTimer = undefined;
      this.#refillFleetTransferPacer();
      this.#scheduleDrain();
      if (this.#fleetTransferPacerTokens < 1) this.#armFleetTransferPacer();
    }, Math.max(0, Math.ceil(wakeAt - this.#clock.now())));
    this.#fleetTransferPacerTimer.unref?.();
  }

  #clearFleetTransferPacerTimer(): void {
    if (this.#fleetTransferPacerTimer) clearTimeout(this.#fleetTransferPacerTimer);
    this.#fleetTransferPacerTimer = undefined;
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
        runtime.available - this.#unreflectedReservationCount(runtime.projectId),
      ), 0);
    const active = runtimes.reduce(
      (sum, runtime) => sum + runtime.active + this.#unreflectedReservationCount(runtime.projectId),
      0,
    );
    return {
      capacity,
      active,
      available,
      queued: this.#queuedJobs,
      queuedBytes: this.#queuedBytes,
      fleetTransferReserved: this.#fleetTransferReserved,
      fleetTransferBudget: FLEET_TRANSFER_BUDGET,
      provenance: this.#responseProvenance.snapshot(),
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
      this.#releaseJobProvenance(job);
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
      this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
      job.reject(error);
    }
    this.#clearDeadlineTimer();
    this.#clearFleetTransferPacerTimer();
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
    this.#responseProvenance.clear();
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
        this.#releaseJobProvenance(job);
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
          this.#releaseJobProvenance(job);
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
      job.admissionCost = selection.admissionCost;
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
        sidecar_peer_age_ms: candidate.sidecarPeerAgeMs ?? null,
        credential_expires_in_ms: candidate.credentialExpiresInMs,
        memory_pressure: candidate.memoryPressure,
        eligible_candidates: selection.eligibleCandidates,
        queue_class: job.queueClass,
        fair_cost_bytes: job.fairCostBytes,
        transfer_admission_cost: job.admissionCost,
        fleet_transfer_cost: selection.fleetTransferCost,
        fleet_transfer_reserved: this.#fleetTransferReserved + selection.fleetTransferCost,
        fleet_transfer_budget: FLEET_TRANSFER_BUDGET,
        route_attempt: job.attempts + 1,
        capacity_reason: candidate.availabilityReason ?? "available",
      });
      this.#reserveRuntime(job, runtime.projectId);
      const fleetTransferLease = this.#reserveFleetTransfer(selection.fleetTransferCost);
      // This drain pass uses one immutable fleet snapshot. Charge its local
      // admission credit immediately so queued jobs cannot trigger repeated
      // all-runtime snapshots/scans on every Awareness publication.
      candidate.reserved++;
      candidate.reservedTransferCost = (candidate.reservedTransferCost ?? 0) +
        job.admissionCost;
      if (!isRuntimeEligible(candidate)) {
        const index = candidates.indexOf(candidate);
        if (index >= 0) candidates.splice(index, 1);
      }
      void this.#dispatch(
        job,
        runtime,
        selection.incarnationKey,
        selection.incarnation,
        selection.retryEpoch,
        candidate.availabilityReason ?? "available",
        candidate.transportProfile === "bulk-v1",
        candidate.ackEwmaMs,
        fleetTransferLease,
      );
      if (candidates.length === 0) return;
    }
  }

  #expireJob(job: QueuedJob): void {
    job.signal?.removeEventListener("abort", job.onAbort!);
    this.#clearRetryWake(job);
    job.memoryLease.release();
    this.#releaseJobProvenance(job);
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
      reservedTransferCost: 0,
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
      sidecarPeerAgeMs: cached.snapshot.sidecarPeerAgeMs == null
        ? null
        : cached.snapshot.sidecarPeerAgeMs + elapsed,
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
      sidecarPeerAgeMs: snapshot.sidecarPeerAgeMs,
      credentialExpiresInMs: snapshot.credentialExpiresInMs,
    };
  }

  #candidateWithReservations(candidate: RouteCandidate): RouteCandidate {
    return {
      ...candidate,
      reserved: this.#unreflectedReservationCount(candidate.projectId),
      reservedTransferCost: this.#unreflectedReservationCost(candidate.projectId),
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

  #fleetTransferCost(job: QueuedJob, candidate: RouteCandidate): number {
    return this.#runtimeCache.size > 1 &&
        candidate.transportProfile === "bulk-v1" &&
        job.workload.requestBytes > FLEET_TRANSFER_EXEMPT_BYTES
      ? job.bulkAdmissionCost
      : 0;
  }

  #choose(job: QueuedJob, available: readonly RouteCandidate[]): RouteSelection | null {
    const now = this.#clock.now();
    let candidates = available.filter(
      candidate => {
        if (
          job.continuationProvenance &&
          (
            candidate.projectId !== job.continuationProvenance.projectId ||
            (candidate.incarnation ?? 0) !== job.continuationProvenance.incarnation
          )
        ) return false;
        const profile = candidate.transportProfile === "bulk-v1" ? "bulk-v1" : "standard";
        const admissionCost = profile === "bulk-v1"
          ? job.bulkAdmissionCost
          : job.standardAdmissionCost;
        const fleetTransferCost = this.#fleetTransferCost(job, candidate);
        if (
          fleetTransferCost > 0 &&
          this.#fleetTransferReserved + fleetTransferCost > FLEET_TRANSFER_BUDGET
        ) return false;
        if (
          v2HostTransferBudget(profile) - (candidate.reservedTransferCost ?? 0) <
            admissionCost
        ) return false;
        const block = job.excluded.get(runtimeIncarnationKey(candidate));
        if (!block) return true;
        const retryEpoch = this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0;
        return retryEpoch > block.retryEpoch || now >= block.retryAtMs;
      },
    );
    const eligibleCandidates = candidates.filter(isRuntimeEligible);
    const fleetTransferPacerCost = job.workload.requestBytes <= PROGRESS_AWARE_MIN_REQUEST_BYTES
      ? FLEET_TRANSFER_PACER_BURST
      : 1;
    const paceFleetTransfer =
      job.workload.requestBytes > FLEET_TRANSFER_EXEMPT_BYTES &&
      job.workload.requestBytes <= FLEET_TRANSFER_PACED_MAX_BYTES &&
      eligibleCandidates.length > 1 &&
      eligibleCandidates.every(candidate => candidate.transportProfile === "bulk-v1");
    if (paceFleetTransfer) {
      this.#refillFleetTransferPacer(now);
      if (this.#fleetTransferPacerTokens < fleetTransferPacerCost) {
        this.#armFleetTransferPacer();
        return null;
      }
      const leastActive = Math.min(...eligibleCandidates.map(candidate => (
        candidate.active + candidate.reserved
      )));
      candidates = eligibleCandidates.filter(candidate => (
        candidate.active + candidate.reserved === leastActive
      ));
    }
    const decision = chooseRuntime(job.workload, candidates, this.#random);
    if (!decision) return null;
    const runtime = this.#runtimeCache.get(decision.projectId)?.runtime;
    const candidate = candidates.find(value => value.projectId === decision.projectId);
    if (runtime && candidate && paceFleetTransfer) {
      this.#consumeFleetTransferPacer(fleetTransferPacerCost, now);
    }
    return runtime && candidate
      ? {
          runtime,
          candidate,
          incarnationKey: runtimeIncarnationKey(candidate),
          incarnation: candidate.incarnation ?? 0,
          eligibleCandidates: eligibleCandidates.length,
          retryEpoch: this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0,
          admissionCost: candidate.transportProfile === "bulk-v1"
            ? job.bulkAdmissionCost
            : job.standardAdmissionCost,
          fleetTransferCost: this.#fleetTransferCost(job, candidate),
        }
      : null;
  }

  async #dispatch(
    job: QueuedJob,
    runtime: RoutableSandboxRuntime,
    selectedIncarnationKey: string,
    selectedIncarnation: number,
    selectedRetryEpoch: number,
    selectedCapacityReason: NonNullable<RoutableSandboxSnapshot["availabilityReason"]>,
    selectedBulkProfile: boolean,
    selectedRttMs: number,
    fleetTransferLease: FleetTransferLease | null,
  ): Promise<void> {
    const started = this.#clock.now();
    const requestModel = typeof job.body.model === "string"
      ? job.body.model
      : job.workload.model;
    const admissionController = new AbortController();
    let admissionDeadlineExpired = false;
    let routeAttemptExpired = false;
    let routeAttemptProgressObserved = false;
    let routeAttemptProgressAgeMs: number | null = null;
    let admissionTimer: ReturnType<typeof setTimeout> | undefined;
    let admissionTimerAtMs: number | null = null;
    let cancellationProofTimer: ReturnType<typeof setTimeout> | undefined;
    const progressAwareAttempt = selectedBulkProfile &&
      job.workload.requestBytes > PROGRESS_AWARE_MIN_REQUEST_BYTES &&
      job.workload.requestBytes <= PAYLOAD_AWARE_DEADLINE_BYTES;
    const mediumProgressProbe = selectedBulkProfile &&
      job.workload.requestBytes > MEDIUM_PROGRESS_PROBE_MIN_BYTES &&
      job.workload.requestBytes <= PROGRESS_AWARE_MIN_REQUEST_BYTES;
    const observeAdmissionProgress = progressAwareAttempt || mediumProgressProbe;
    const progressFreshnessMs = progressAwareAttempt
      ? admissionProgressFreshnessMs(selectedRttMs)
      : 0;
    const preparedProofGrace = progressAwareAttempt
      ? preparedProofGraceMs(selectedRttMs)
      : 0;
    const firstProgressTimeoutMs = mediumProgressProbe
      ? mediumFirstProgressTimeoutMs(selectedRttMs)
      : 0;
    let initialAttemptDeadlineMs: number | null = null;
    let firstProgressCheckAtMs: number | null = null;
    let admissionProgressAtMs: number | null = null;
    let admissionProgress: SandboxAdmissionProgress | null = null;
    let rejectAdmissionWait!: (error: unknown) => void;
    const admissionWait = new Promise<never>((_resolve, reject) => {
      rejectAdmissionWait = reject;
    });
    void admissionWait.catch(() => undefined);
    const captureAttemptProgress = (now: number) => {
      routeAttemptExpired = true;
      routeAttemptProgressObserved = admissionProgressAtMs !== null;
      routeAttemptProgressAgeMs = admissionProgressAtMs === null
        ? null
        : Math.max(0, now - admissionProgressAtMs);
    };
    const onCallerAbort = () => {
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimer = undefined;
      admissionTimerAtMs = null;
      const error = job.signal?.reason ?? abortError();
      const value = error && typeof error === "object"
        ? error as { code?: unknown }
        : {};
      // The ingress lease owns the same public deadline and can win the timer
      // race by a few milliseconds. Preserve the selected-attempt diagnostics
      // rather than misreporting a live transfer as an unexplained queue 503.
      if (value.code === "router_capacity_timeout" && !routeAttemptExpired) {
        admissionDeadlineExpired = true;
        captureAttemptProgress(this.#clock.now());
      }
      admissionController.abort(error);
      rejectAdmissionWait(error);
    };
    if (job.signal?.aborted) onCallerAbort();
    else job.signal?.addEventListener("abort", onCallerAbort, { once: true });
    let expireAdmissionAttempt!: () => void;
    const armAdmissionTimerAt = (atMs: number) => {
      const boundedAtMs = Math.min(job.queueDeadlineMs, atMs);
      if (
        admissionTimer && admissionTimerAtMs !== null &&
        admissionTimerAtMs <= boundedAtMs
      ) return;
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimerAtMs = boundedAtMs;
      admissionTimer = setTimeout(
        expireAdmissionAttempt,
        Math.max(1, Math.ceil(boundedAtMs - this.#clock.now())),
      );
      admissionTimer.unref?.();
    };
    expireAdmissionAttempt = () => {
      admissionTimer = undefined;
      admissionTimerAtMs = null;
      const now = this.#clock.now();
      if (now >= job.queueDeadlineMs) {
        admissionDeadlineExpired = true;
        captureAttemptProgress(now);
        const error = new RouterCapacityTimeoutError();
        admissionController.abort(error);
        cancellationProofTimer = setTimeout(
          () => rejectAdmissionWait(error),
          ROUTE_CANCELLATION_PROOF_GRACE_MS,
        );
        cancellationProofTimer.unref?.();
        return;
      }
      if (mediumProgressProbe && initialAttemptDeadlineMs !== null) {
        if (
          admissionProgressAtMs === null && firstProgressCheckAtMs !== null &&
          now < firstProgressCheckAtMs
        ) {
          armAdmissionTimerAt(firstProgressCheckAtMs);
          return;
        }
        if (admissionProgressAtMs !== null && now < initialAttemptDeadlineMs) {
          // Any exact cumulative data ACK proves this medium transfer is alive.
          // Preserve its established 15-second attempt after that first proof.
          armAdmissionTimerAt(initialAttemptDeadlineMs);
          return;
        }
      }
      const completeBodyAwaitingPrepared = admissionProgress?.stage === "request_data" &&
        admissionProgress.acknowledgedEncodedBytes === admissionProgress.totalEncodedBytes &&
        admissionProgressAtMs !== null;
      let nextProgressCheckAtMs: number | null = null;
      if (progressAwareAttempt && completeBodyAwaitingPrepared) {
        const preparedCheckAtMs = admissionProgressAtMs! + preparedProofGrace;
        nextProgressCheckAtMs = now < preparedCheckAtMs
          ? Math.min(job.queueDeadlineMs, preparedCheckAtMs)
          : null;
      } else if (progressAwareAttempt) {
        const retainStaleProgress = shouldRetainStaleAdmissionProgress(
          now,
          job.queueDeadlineMs,
          started,
          admissionProgressAtMs,
          progressFreshnessMs,
          admissionProgress,
        );
        nextProgressCheckAtMs = nextAdmissionProgressCheckAtMs(
          now,
          job.queueDeadlineMs,
          admissionProgressAtMs,
          progressFreshnessMs,
          retainStaleProgress,
        );
      }
      if (nextProgressCheckAtMs !== null) {
        // Do not restart the attempt budget. Check again when this exact proof
        // becomes stale, always capped by the one public admission deadline.
        armAdmissionTimerAt(nextProgressCheckAtMs);
        return;
      }
      captureAttemptProgress(now);
      const error = new RouterRouteAttemptTimeoutError();
      admissionController.abort(error);
      cancellationProofTimer = setTimeout(
        () => rejectAdmissionWait(error),
        ROUTE_CANCELLATION_PROOF_GRACE_MS,
      );
      cancellationProofTimer.unref?.();
    };
    const remainingAdmissionMs = job.queueDeadlineMs - this.#clock.now();
    if (admissionController.signal.aborted) {
      // Caller cancellation already owns the outcome.
    } else if (remainingAdmissionMs <= 0) {
      admissionDeadlineExpired = true;
      const error = new RouterCapacityTimeoutError();
      admissionController.abort(error);
      rejectAdmissionWait(error);
    } else {
      const attemptTimeoutMs = Math.min(
        remainingAdmissionMs,
        job.routeAttemptTimeoutMs,
      );
      initialAttemptDeadlineMs = this.#clock.now() + attemptTimeoutMs;
      firstProgressCheckAtMs = mediumProgressProbe
        ? Math.min(
            initialAttemptDeadlineMs,
            this.#clock.now() + firstProgressTimeoutMs,
          )
        : initialAttemptDeadlineMs;
      armAdmissionTimerAt(firstProgressCheckAtMs);
    }
    const clearAdmissionDeadline = () => {
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimer = undefined;
      admissionTimerAtMs = null;
      if (cancellationProofTimer) clearTimeout(cancellationProofTimer);
      cancellationProofTimer = undefined;
    };
    const detachAdmission = () => {
      clearAdmissionDeadline();
      job.signal?.removeEventListener("abort", onCallerAbort);
    };
    try {
      // Admission ownership releases only the router's duplicate reservation.
      // The selected-runtime attempt deadline remains active until V2 reports
      // its irreversible execution fence; an admitted R2 request is still safe
      // to move while request_start has not entered the cumulative window.
      const runOptions: SandboxRunOptions = {
        onAdmissionReflected: () => {
          if (admissionController.signal.aborted) return;
          this.#refreshRuntimeSafely(runtime);
          this.#releaseRuntimeReservation(job, runtime.projectId);
          this.#scheduleDrain();
        },
        onExecutionRiskReflected: () => {
          if (admissionController.signal.aborted) return;
          if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
          clearAdmissionDeadline();
        },
      };
      if (observeAdmissionProgress) {
        runOptions.onAdmissionProgress = progress => {
          // Late ACK/prepared callbacks cannot revive an aborted attempt or
          // move its absolute deadline. Standard, rollback, and <=256 KiB runs
          // never install this observer or its transport ACK scan.
          if (
            admissionController.signal.aborted || routeAttemptExpired ||
            admissionDeadlineExpired
          ) return;
          const now = this.#clock.now();
          if (now >= job.queueDeadlineMs) return;
          if (
            !Number.isSafeInteger(progress.acknowledgedEncodedBytes) ||
            !Number.isSafeInteger(progress.totalEncodedBytes) ||
            progress.totalEncodedBytes < 1 ||
            progress.acknowledgedEncodedBytes < 1 ||
            progress.acknowledgedEncodedBytes > progress.totalEncodedBytes ||
            (progress.stage !== "request_data" && progress.stage !== "request_prepared") ||
            (progress.stage === "request_prepared" &&
              progress.acknowledgedEncodedBytes !== progress.totalEncodedBytes) ||
            (admissionProgress !== null &&
              (progress.totalEncodedBytes !== admissionProgress.totalEncodedBytes ||
                progress.acknowledgedEncodedBytes <
                  admissionProgress.acknowledgedEncodedBytes ||
                admissionProgress.stage === "request_prepared" ||
                (progress.stage === "request_data" &&
                  progress.acknowledgedEncodedBytes ===
                    admissionProgress.acknowledgedEncodedBytes)))
          ) return;
          admissionProgress = Object.freeze({
            stage: progress.stage,
            acknowledgedEncodedBytes: progress.acknowledgedEncodedBytes,
            totalEncodedBytes: progress.totalEncodedBytes,
          });
          admissionProgressAtMs = now;
          if (progressAwareAttempt &&
            progress.acknowledgedEncodedBytes === progress.totalEncodedBytes
          ) {
            if (progress.stage === "request_prepared") {
              // Exact assembly/claim proof ends shared-link body ownership.
              // Local transport/start reservations remain unchanged.
              if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
            } else {
              // The payload is complete, but assembly/claim still consumes the
              // shared path. Retain one control unit rather than releasing the
              // lease entirely: two outstanding proofs can admit only one new
              // observed-size body, avoiding both head-of-line starvation and
              // the overload reproduced by a zero-cost prepared handshake.
              if (this.#downgradeFleetTransferToControl(fleetTransferLease)) {
                this.#scheduleDrain();
              }
              armAdmissionTimerAt(now + preparedProofGrace);
            }
          }
        };
      }
      const runtimeRun = Promise.resolve(runtime.run(
        job.body,
        admissionController.signal,
        runOptions,
      ));
      // Promise.race installs terminal handlers on runtimeRun. If a broken test
      // seam ignores cancellation, its late rejection remains consumed. V2's
      // abort path synchronously proves pre-execution safety or committed risk
      // before the short cancellation-proof fence permits a retry.
      const handle = await Promise.race([runtimeRun, admissionWait]);
      if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
      // Commitment also ends any remaining attempt deadline immediately.
      // Optional memory accounting work must never turn an accepted upstream
      // request into a local timeout or inflate measured commit ACK latency.
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
      const provenanceReservation = job.provenanceReservation;
      job.provenanceReservation = null;
      const provenanceCommitted = job.authorizationScope && provenanceReservation
        ? this.#withCommittedProvenance(handle, provenanceReservation, {
            projectId: runtime.projectId,
            incarnation: selectedIncarnation,
          }, job.authorizationScope)
        : handle;
      const traced = this.#logger
        ? traceRunHandle(provenanceCommitted, {
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
        : provenanceCommitted;
      void provenanceCommitted.final.then(
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
            selectedIncarnationKey,
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
            selectedIncarnationKey,
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
      if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
      const commitment = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      const committed = commitment.committed === true;
      const ambiguous = commitment.ambiguous === true;
      const timedOutBeforeCommit = routeAttemptExpired &&
        !isDeterministicRequestTooLarge(error) &&
        !committed &&
        !ambiguous;
      if (timedOutBeforeCommit) {
        // TypeScript does not model assignments performed by the asynchronous
        // transport callback above; capture its actual runtime value here.
        const finalAdmissionProgress = admissionProgress as
          SandboxAdmissionProgress | null;
        this.#emit("route.attempt_expired", {
          trace_id: job.traceId,
          project_id: runtime.projectId,
          route_attempt: job.attempts + 1,
          attempt_ms: Math.max(0, this.#clock.now() - started),
          progress_observed: routeAttemptProgressObserved,
          progress_age_ms: routeAttemptProgressAgeMs,
          progress_stage: finalAdmissionProgress?.stage ?? null,
          progress_acknowledged_encoded_bytes:
            finalAdmissionProgress?.acknowledgedEncodedBytes ?? null,
          progress_total_encoded_bytes: finalAdmissionProgress?.totalEncodedBytes ?? null,
          deadline_exhausted: admissionDeadlineExpired,
          capacity_reason: selectedCapacityReason,
          transfer_admission_cost: job.admissionCost,
        });
      }
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
          selectedIncarnationKey,
        );
        job.signal?.removeEventListener("abort", job.onAbort!);
        job.memoryLease.release();
        this.#releaseJobProvenance(job);
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
        this.#releaseJobProvenance(job);
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "host-transform", error, job.attempts);
        job.reject(error);
        this.#scheduleDrain();
        return;
      }
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
        const previousBlock = job.excluded.get(selectedIncarnationKey);
        const failures = (previousBlock?.failures ?? 0) + 1;
        const retryDelayMs = Math.min(
          PRECOMMIT_RETRY_MAX_MS,
          PRECOMMIT_RETRY_BASE_MS * 2 ** Math.min(4, failures - 1),
        );
        job.excluded.set(selectedIncarnationKey, {
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
          this.#releaseJobProvenance(job);
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
        this.#releaseJobProvenance(job);
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

  #withCommittedProvenance(
    handle: RunHandle,
    reservation: ResponseProvenanceReservation,
    provenance: ResponseProvenance,
    authorizationScope: AuthenticatedTenantScope,
  ): RunHandle {
    const final = handle.final.then(
      value => {
        if (value?.status !== "completed") {
          this.#responseProvenance.release(reservation);
          return value;
        }
        let recorded = false;
        try {
          if (!this.#closed) {
            recorded = this.#responseProvenance.commit(
              reservation,
              value?.id,
              authorizationScope,
              provenance,
            );
          }
        } catch {
          this.#responseProvenance.release(reservation);
          // Registry internals and provider IDs never enter the public/logged error.
        }
        if (!recorded) {
          releaseAtomicBytePermit(value);
          throw new ResponseProvenanceCommitError();
        }
        return value;
      },
      error => {
        this.#responseProvenance.release(reservation);
        throw error;
      },
    );
    void final.catch(() => undefined);
    return {
      async *events() {
        for await (const frame of handle.events()) {
          if (RESPONSE_TERMINAL_EVENTS.has(frame.event)) {
            try {
              await final;
            } catch (error) {
              releaseAtomicBytePermit(frame);
              throw error;
            }
          }
          yield frame;
        }
      },
      final,
      get rateLimit() { return handle.rateLimit; },
      get upstreamPrelude() { return handle.upstreamPrelude; },
      stats: handle.stats,
    };
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
