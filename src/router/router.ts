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
import { DEFAULT_ROUTER_ADMISSION_TIMEOUT_MS } from "./admission-defaults";
import type { ReleasableLease } from "./types";
import { RouterTrafficTelemetry, type RouterTrafficSnapshot } from "./telemetry";
import {
  classifyWorkload,
  jsonEncodedByteLength,
  jsonEncodedByteLengthAsync,
  MAX_WORKLOAD_JSON_ITEMS,
  WorkloadTraversalLimitError,
  type WorkloadDescriptor,
} from "./workload";
import type { LoggerLike } from "../observability/logger";
import { observabilityEvent, type JsonValue } from "../observability/events";
import {
  safeObservabilityModel,
  safeObservabilityReasoningEffort,
} from "../observability/redaction";
import { requestFailureFields } from "../observability/request-error";
import { traceRunHandle } from "../observability/traced-run-handle";
import { validateResponsesInputJson } from "../relay/responses-validation";
import { prepareHostUpstreamResponsesRequest } from "../relay/host-request-policy";
import { canFallbackFromModel, FALLBACK_UPSTREAM_MODEL } from "../relay/model-policy";
import { isDefinitiveModelNotFound, wrapModelFallbackHandle } from "../relay/model-fallback";
import { applyUpstreamServiceTierPolicy } from "../relay/service-tier-policy";
import {
  releaseAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import { V2_LATENCY_CRITICAL_REQUEST_BYTES } from "../v2/transport/session";
import {
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";
import {
  ResponseProvenanceCommitError,
  ResponseProvenanceRegistry,
  ResponseProvenanceScopeError,
  type ResponseProvenance,
  type ResponseProvenanceLookup,
  type ResponseProvenanceReservation,
  type ResponseProvenanceSnapshot,
} from "./provenance";
import {
  V2_BULK_TRANSFER_COST,
  v2HostTransferBudget,
  v2RequestTransferCost,
} from "../v2/transport/admission-cost";
import {
  rateLimitHintFromError,
  rateLimitProblemFromError,
  type RateLimitHint,
  type RateLimitProblem,
} from "../v2/rate-limit";
import { V2CommittedError, V2PreCommitError } from "../v2/transport/run-handle";

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
const DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS = 60_000;
const ROUTE_CANCELLATION_PROOF_GRACE_MS = 25;
// A committed ACK can be trapped behind a failed receive lane while the same
// runtime uses its bounded 150-second recovery window. Retain the original
// request long enough for that repair and proof delivery; never replay it.
const COMMITTED_HANDLE_PROOF_TIMEOUT_MS = 180_000;
// Real Prism telemetry showed 3.68-3.82 MiB JSON histories compressing to
// roughly 1.75 MiB and making steady ACK progress at about 50 KiB/s. Scaling
// only above 8 MiB made those healthy transfers deterministically hit 30s.
const QUEUE_PAYLOAD_AWARE_DEADLINE_BYTES = 2 * MiB;
const ROUTE_PAYLOAD_AWARE_DEADLINE_BYTES = 8 * MiB;
const PROGRESS_AWARE_MAX_REQUEST_BYTES = 8 * MiB;
const PROGRESS_AWARE_MIN_REQUEST_BYTES = 256 * KiB;
const MEDIUM_REQUEST_MAX_BYTES = 2 * MiB;
const FLEET_TRANSFER_EXEMPT_BYTES = 256 * KiB;
const FLEET_TRANSFER_PACED_MAX_BYTES = 3 * MiB;
const FLEET_ORDINARY_TRANSFER_BUDGET = 4;
// One bulk body retains four units. Reserve two further units for smaller
// requests so its upload cannot stall every otherwise idle runtime.
const FLEET_TRANSFER_HEADROOM = 2;
const FLEET_TRANSFER_BUDGET = V2_BULK_TRANSFER_COST + FLEET_TRANSFER_HEADROOM;
const FLEET_TRANSFER_PACER_BURST = 2;
const FLEET_TRANSFER_PACER_REFILL_TOKENS = 2;
const FLEET_TRANSFER_PACER_INTERVAL_MS = 500;
const WEIGHTED_TRANSFER_PROTECTION_AGE_MS = 8 * FLEET_TRANSFER_PACER_INTERVAL_MS;
const MIN_ADMISSION_PROGRESS_FRESHNESS_MS = 5_000;
const MAX_ADMISSION_PROGRESS_FRESHNESS_MS = 10_000;
const MIN_PREPARED_PROOF_GRACE_MS = 3_000;
const MAX_PREPARED_PROOF_GRACE_MS = 5_000;
const MAX_TIMER_MS = 2_147_483_647;
const SHALLOW_UPSTREAM_POLICY_HEADROOM_BYTES = 128;
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

/** @internal RTT-derived minimum for the bounded prepared-proof allowance. */
export function preparedProofGraceMs(selectedRttMs: number): number {
  const finiteRttMs = Number.isFinite(selectedRttMs) ? Math.max(0, selectedRttMs) : 0;
  return Math.min(
    MAX_PREPARED_PROOF_GRACE_MS,
    Math.max(MIN_PREPARED_PROOF_GRACE_MS, Math.ceil(4 * finiteRttMs)),
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
  // A late-admitted request may spend most of its global
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
  if (requestBytes <= ROUTE_PAYLOAD_AWARE_DEADLINE_BYTES) return baseTimeoutMs;
  return Math.min(
    MAX_TIMER_MS,
    Math.ceil(baseTimeoutMs * (requestBytes / ROUTE_PAYLOAD_AWARE_DEADLINE_BYTES)),
  );
}

function scaledQueueTimeoutMs(requestBytes: number, baseTimeoutMs: number): number {
  if (requestBytes <= QUEUE_PAYLOAD_AWARE_DEADLINE_BYTES) return baseTimeoutMs;
  // Give the observed 2-8 MiB history band up to four configured base windows,
  // then let the established 8 MiB large-payload slope take over. Every size
  // retains one bounded deadline measured from its original acceptance.
  const observedHistoryScale = Math.min(
    4,
    requestBytes / QUEUE_PAYLOAD_AWARE_DEADLINE_BYTES,
  );
  const legalPayloadScale = Math.max(
    1,
    requestBytes / ROUTE_PAYLOAD_AWARE_DEADLINE_BYTES,
  );
  const scale = Math.max(observedHistoryScale, legalPayloadScale);
  const failoverSlackMs = Math.max(1, Math.ceil(baseTimeoutMs / 6));
  return Math.min(
    MAX_TIMER_MS,
    Math.ceil(baseTimeoutMs * scale) + failoverSlackMs,
  );
}

function effectiveRouteAttemptTimeoutMs(
  requestBytes: number,
  queueTimeoutMs: number,
  baseRouteAttemptTimeoutMs: number,
  automaticRouteAttemptTimeout: boolean,
): number {
  const scaled = scaledRouteAttemptTimeoutMs(requestBytes, baseRouteAttemptTimeoutMs);
  // A local queue override can otherwise halve the automatic attempt back to
  // 30 seconds, even though the observed 3.7 MiB / ~1.7 MiB-on-wire history
  // needs slightly longer than that for one clean cumulative flight. Explicit
  // attempt settings retain exact caller control.
  const withObservedHistoryFloor = automaticRouteAttemptTimeout &&
      requestBytes > QUEUE_PAYLOAD_AWARE_DEADLINE_BYTES
    ? Math.max(scaled, DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS)
    : scaled;
  return Math.min(queueTimeoutMs, withObservedHistoryFloor);
}

/**
 * Keep the configured base deadline unchanged through 2 MiB. Above it,
 * scale queue/admission time for measured slow-but-progressing transfers up to
 * four base windows; above 8 MiB the established legal-payload slope takes
 * over. The initial per-runtime attempt uses its configured base through
 * 8 MiB (60 seconds by default), capped by the original admission deadline.
 */
export function requestRoutingTimeouts(
  requestBytes: number,
  baseQueueTimeoutMs: number,
  baseRouteAttemptTimeoutMs: number,
  automaticRouteAttemptTimeout = false,
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
  const queueTimeoutMs = scaledQueueTimeoutMs(requestBytes, baseQueueTimeoutMs);
  const routeAttemptTimeoutMs = effectiveRouteAttemptTimeoutMs(
    requestBytes,
    queueTimeoutMs,
    baseRouteAttemptTimeoutMs,
    automaticRouteAttemptTimeout,
  );
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
  /** Ends admission timing while retaining bytes and caller cancellation ownership. */
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
  /** Endpoint-specific peak multiplier reserved before retaining each chunk. */
  reservationMultiplier?: number;
}

export interface StartRelayOptions {
  /** Server-owned diagnostic linkage for the single model-rejection fallback. */
  fallbackFrom?: Readonly<{ traceId: string; model: string; origin: "http" | "sse" }>;
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
  /** Keep the original ingress allocation inside the atomic workload swap. */
  retainIncomingLeaseBytes?: boolean;
  /** Optional request-graph lifetime which must end before ingress memory releases. */
  retainMemoryUntil?: Promise<unknown>;
}

interface QueuedJob {
  body: Record<string, unknown>;
  source: RelaySource;
  workload: WorkloadDescriptor;
  decodedRequestBytes: number;
  clientId: string;
  authorizationScope: AuthenticatedTenantScope | null;
  /** Preferred origin only; provider lineage is independent of runtime availability. */
  continuationProvenance: ResponseProvenance | null;
  provenanceReservation: ResponseProvenanceReservation | null;
  signal?: AbortSignal;
  completeIncomingAdmission: (() => void) | null;
  memoryLease: ResourceLease;
  retainFullLeaseAfterCommit: boolean;
  retainMemoryUntil: Promise<unknown> | null;
  memoryReleaseScheduled: boolean;
  attempts: number;
  /** True after at least one healthy runtime was selected for transport admission. */
  routeSelected: boolean;
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
  queueCapacityHeld: boolean;
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
  fleetTransferPacerCost: number;
  selectedAvailable: number;
}

interface FleetTransferLease {
  cost: number;
  released: boolean;
}

interface WeightedTransferProtection {
  readonly job: QueuedJob;
  readonly projectId: ProjectId;
  readonly incarnationKey: string;
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
  fleetBulkTransferReserved: number;
  fleetOrdinaryTransferBudget: number;
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

export class RouterTransportAdmissionTimeoutError extends Error {
  readonly name = "RouterTransportAdmissionTimeoutError";
  readonly status = 503;
  readonly code = "transport_admission_timeout";

  constructor() {
    super("A selected sandbox did not acquire transport admission before the routing deadline");
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

function sanitizedRouterAbortReason(signal: AbortSignal | undefined): unknown {
  const reason = signal?.reason;
  if (
    reason instanceof RouterCapacityTimeoutError ||
    reason instanceof RouterTransportAdmissionTimeoutError ||
    reason instanceof RouterShutdownError ||
    (reason instanceof DOMException && reason.name === "AbortError")
  ) return reason;
  return abortError();
}

function selectedRouteDeadlineError(error: unknown, routeSelected: boolean): unknown {
  return routeSelected && error instanceof RouterCapacityTimeoutError
    ? new RouterTransportAdmissionTimeoutError()
    : error;
}

type JsonCloneFrame = {
  readonly source: Record<string, unknown> | unknown[];
  readonly target: Record<string, unknown> | unknown[];
  readonly keys: readonly string[] | null;
  index: number;
};

interface ImmutableRelaySnapshotResult {
  readonly body: Record<string, unknown>;
  readonly structuralBytes: number;
}

async function immutableRelaySnapshot(
  body: Record<string, unknown>,
  signal: AbortSignal | undefined,
  expired: () => boolean,
): Promise<ImmutableRelaySnapshotResult> {
  const root: Record<string, unknown> = Object.create(null);
  const assertActive = (): void => {
    if (signal?.aborted) throw signal.reason ?? abortError();
    if (expired()) throw new RouterCapacityTimeoutError();
  };
  const yieldTask = async (): Promise<void> => {
    await new Promise<void>(resolve => setImmediate(resolve));
    assertActive();
  };
  let flat = true;
  let flatKeys = 0;
  let flatStringUnits = 0;
  for (const key in body) {
    if (!Object.prototype.hasOwnProperty.call(body, key) || ++flatKeys > 256) {
      flat = false;
      break;
    }
    const descriptor = Object.getOwnPropertyDescriptor(body, key);
    if (!descriptor || !("value" in descriptor) ||
      (descriptor.value !== null && typeof descriptor.value === "object") ||
      typeof descriptor.value === "undefined" || typeof descriptor.value === "function" ||
      typeof descriptor.value === "symbol" || typeof descriptor.value === "bigint") {
      flat = false;
      break;
    }
    root[key] = descriptor.value;
    flatStringUnits = Math.min(
      Number.MAX_SAFE_INTEGER,
      flatStringUnits + key.length +
        (typeof descriptor.value === "string" ? descriptor.value.length : 0),
    );
  }
  if (flat) {
    assertActive();
    const immutable = Object.freeze(root);
    const structuralBytes = flatStringUnits <= MEDIUM_REQUEST_MAX_BYTES
      ? jsonEncodedByteLength(immutable)
      : await jsonEncodedByteLengthAsync(immutable, { signal, yieldTask });
    assertActive();
    return Object.freeze({ body: immutable, structuralBytes });
  }
  for (const key of Object.keys(root)) delete root[key];
  const active = new WeakSet<object>();
  active.add(body);
  const stack: JsonCloneFrame[] = [{
    source: body,
    target: root,
    keys: Object.keys(body),
    index: 0,
  }];
  let nodes = 1;
  let nodesSinceYield = 0;
  let work = 0;
  let structuralBytes = 2;
  const addStructuralBytes = (bytes: number): void => {
    const next = structuralBytes + bytes;
    if (!Number.isSafeInteger(next)) {
      throw new RangeError("JSON byte length exceeds safe integer bounds");
    }
    structuralBytes = next;
  };
  const measureString = async (value: string): Promise<number> => {
    if (value.length <= MEDIUM_REQUEST_MAX_BYTES) {
      return jsonEncodedByteLength(value);
    }
    return await jsonEncodedByteLengthAsync(value, { signal, yieldTask });
  };
  const checkpoint = async (): Promise<void> => {
    assertActive();
    if (nodes > MAX_WORKLOAD_JSON_ITEMS) {
      throw new RangeError("Relay request contains too many JSON values");
    }
    if (work < 64 * KiB && nodesSinceYield < 256) return;
    work = 0;
    nodesSinceYield = 0;
    await yieldTask();
  };
  while (stack.length > 0) {
    const frame = stack.at(-1)!;
    const isArray = Array.isArray(frame.source);
    const length = isArray ? frame.source.length : frame.keys!.length;
    if (frame.index >= length) {
      Object.freeze(frame.target);
      active.delete(frame.source);
      stack.pop();
      continue;
    }
    const entryIndex = frame.index;
    const sourceKey = isArray ? String(entryIndex) : frame.keys![entryIndex]!;
    const targetKey = isArray ? entryIndex : sourceKey;
    frame.index++;
    const descriptor = Object.getOwnPropertyDescriptor(frame.source, sourceKey);
    if (!descriptor || !("value" in descriptor)) {
      throw new TypeError("Relay request changed during immutable snapshotting");
    }
    const value = descriptor.value;
    nodes++;
    nodesSinceYield++;
    work += sourceKey.length + (typeof value === "string" ? value.length : 1);
    if (entryIndex > 0) addStructuralBytes(1);
    if (!isArray) {
      addStructuralBytes(await measureString(sourceKey));
      addStructuralBytes(1);
    }
    if (value && typeof value === "object") {
      if (active.has(value)) throw new TypeError("Converting circular structure to JSON");
      if (Array.isArray(value) && value.length > MAX_WORKLOAD_JSON_ITEMS - nodes) {
        throw new WorkloadTraversalLimitError("items");
      }
      const child = Array.isArray(value) ? new Array<unknown>(value.length) : Object.create(null);
      if (isArray) (frame.target as unknown[])[targetKey as number] = child;
      else (frame.target as Record<string, unknown>)[targetKey as string] = child;
      addStructuralBytes(2);
      active.add(value);
      stack.push({
        source: value as Record<string, unknown> | unknown[],
        target: child,
        keys: Array.isArray(value) ? null : Object.keys(value),
        index: 0,
      });
    } else {
      if (typeof value === "undefined" || typeof value === "function" ||
        typeof value === "symbol" || typeof value === "bigint") {
        throw new TypeError("Relay request contains a non-JSON value");
      }
      if (isArray) (frame.target as unknown[])[targetKey as number] = value;
      else (frame.target as Record<string, unknown>)[targetKey as string] = value;
      addStructuralBytes(typeof value === "string"
        ? await measureString(value)
        : jsonEncodedByteLength(value));
    }
    await checkpoint();
  }
  return Object.freeze({ body: root, structuralBytes });
}

function isJsonPrimitive(value: unknown): value is null | string | number | boolean {
  return value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean";
}

/**
 * The upstream normalizers change only shallow primitive policy fields. Reuse
 * the exact snapshot measurement for the retained graph instead of traversing
 * multi-megabyte input a second time. Exotic/non-shallow changes fall back to
 * the general cooperative measurer.
 */
function shallowPolicyStructuralBytes(
  before: Readonly<Record<string, unknown>>,
  beforeBytes: number,
  after: Readonly<Record<string, unknown>>,
): number | null {
  const beforeKeys = Object.keys(before);
  const afterKeys = Object.keys(after);
  const keys = new Set([...beforeKeys, ...afterKeys]);
  let propertyBytes = beforeBytes - 2 - Math.max(0, beforeKeys.length - 1);
  for (const key of keys) {
    const beforeHas = Object.hasOwn(before, key);
    const afterHas = Object.hasOwn(after, key);
    const beforeValue = beforeHas ? before[key] : undefined;
    const afterValue = afterHas ? after[key] : undefined;
    if (beforeHas === afterHas && Object.is(beforeValue, afterValue)) continue;
    if (beforeHas) {
      if (!isJsonPrimitive(beforeValue)) return null;
      propertyBytes -= jsonEncodedByteLength(key) + 1 + jsonEncodedByteLength(beforeValue);
    }
    if (afterHas) {
      if (!isJsonPrimitive(afterValue)) return null;
      propertyBytes += jsonEncodedByteLength(key) + 1 + jsonEncodedByteLength(afterValue);
    }
  }
  const adjusted = propertyBytes + 2 + Math.max(0, afterKeys.length - 1);
  return Number.isSafeInteger(adjusted) && adjusted >= 2 ? adjusted : null;
}

function isLocalCpuTransformFailure(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  if (error instanceof V2PreCommitError && error.code === "request_preparation_timeout") {
    return true;
  }
  const value = error as { name?: unknown; code?: unknown };
  return value.name === "CpuTransformQueueFullError" ||
    value.name === "CpuTransformTimeoutError" ||
    value.code === "cpu_transform_overloaded" ||
    value.code === "cpu_transform_timeout";
}

function isSafePreCommitFailure(error: unknown): boolean {
  // This boundary is process-local. Only the actual class is authoritative;
  // duck-typed name/committed fields cannot authorize duplicate execution.
  return error instanceof V2PreCommitError;
}

function isDeterministicRequestTooLarge(error: unknown): boolean {
  if (!isSafePreCommitFailure(error)) return false;
  const code = (error as { code?: unknown }).code;
  return code === "request_too_large";
}

function isRetryablePreCommitFailure(error: unknown): boolean {
  return isSafePreCommitFailure(error) && !isDeterministicRequestTooLarge(error);
}

function releaseLateRunHandle(handle: RunHandle): Promise<void> {
  const cleanup: Promise<unknown>[] = [];
  try {
    const discard = (handle as RunHandle & {
      discardUnconsumedEvents?: () => void;
    }).discardUnconsumedEvents;
    if (typeof discard === "function") discard.call(handle);
    else {
      const iterator = handle.events();
      // Async-generator return() before the first next() does not execute its
      // body/finally in Bun. Start one cancellation-bound read, release any
      // transferred frame it yields, then close the iterator.
      cleanup.push(iterator.next().then(result => {
        if (!result.done) releaseAtomicBytePermit(result.value);
        return iterator.return(undefined);
      }).catch(() => undefined));
    }
  } catch {
    // The already-aborted runtime signal remains the primary cleanup path.
  }
  try {
    cleanup.push(Promise.resolve(handle.final).then(releaseAtomicBytePermit, () => undefined));
  } catch {
    // A throwing final accessor cannot own a resolved terminal allocation.
  }
  try {
    const upstreamPrelude = handle.upstreamPrelude;
    if (upstreamPrelude) {
      cleanup.push(Promise.resolve(upstreamPrelude).catch(() => undefined));
    }
  } catch {
    // A malformed optional prelude accessor cannot prevent primary cleanup.
  }
  return Promise.allSettled(cleanup).then(() => undefined);
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
  readonly #committedHandleTimeoutMs: number;
  readonly #routeAttemptTimeoutMs: number;
  readonly #automaticRouteAttemptTimeout: boolean;
  readonly #queueCapacity: number;
  readonly #queueByteCapacity: number;
  readonly #registryUnsubscribe: () => void;
  readonly #traffic = new RouterTrafficTelemetry();
  readonly #responseProvenance: ResponseProvenanceRegistry;
  readonly #defaultAuthorizationScope: AuthenticatedTenantScope | null;
  readonly #runtimeCache = new Map<ProjectId, CachedRuntime>();
  readonly #incomingRequests = new Set<IncomingRequestTracker>();
  readonly #pendingAdmissions = new Set<IncomingRequestTracker>();
  readonly #deferredMemoryReleases = new Set<Promise<void>>();
  readonly #lateAttemptCleanups = new Set<Promise<void>>();
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
  #queueCapacityJobs = 0;
  #queueCapacityBytes = 0;
  #fleetTransferReserved = 0;
  #fleetBulkTransferReserved = 0;
  #fleetTransferProtection: WeightedTransferProtection | null = null;
  readonly #runtimeTransferProtections = new Map<ProjectId, WeightedTransferProtection>();
  /** Subset of queue-capacity-held jobs; count and lifetime share the queue bound. */
  readonly #weightedTransferWaiters = new Set<QueuedJob>();
  #weightedTransferProtectionCheckAtMs: number | null = null;
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
    this.#queueTimeoutMs = options.queueTimeoutMs ?? DEFAULT_ROUTER_ADMISSION_TIMEOUT_MS;
    if (
      !Number.isSafeInteger(this.#queueTimeoutMs) ||
      this.#queueTimeoutMs < 1 ||
      this.#queueTimeoutMs > MAX_TIMER_MS
    ) {
      throw new RangeError("queueTimeoutMs must be a positive safe integer");
    }
    this.#committedHandleTimeoutMs = Math.min(
      COMMITTED_HANDLE_PROOF_TIMEOUT_MS,
      this.#queueTimeoutMs + ROUTE_CANCELLATION_PROOF_GRACE_MS,
    );
    this.#automaticRouteAttemptTimeout = options.routeAttemptTimeoutMs === undefined;
    this.#routeAttemptTimeoutMs = options.routeAttemptTimeoutMs ?? Math.min(
      DEFAULT_ROUTE_ATTEMPT_TIMEOUT_MS,
      Math.max(1, Math.floor(this.#queueTimeoutMs / 2)),
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
        this.#clearRuntimeTransferProtection(event.projectId);
        this.#runtimeCache.delete(event.projectId);
        this.#rateLimitObservations.delete(event.projectId);
        this.#scheduleDrain();
        return;
      }
      const runtime = this.#registry.get(event.projectId);
      if (!runtime) return;
      const previous = this.#runtimeCache.get(event.projectId);
      const previousCredits = previous
        ? this.#candidateCreditLanes(this.#agedCandidate(previous))
        : { admission: 0, prefetch: 0, transfer: 0 };
      const previousIncarnation = previous?.snapshot.incarnation ?? 0;
      const next = this.#refreshRuntime(runtime);
      const nextCredits = this.#candidateCreditLanes(next.candidate);
      const incarnationChanged = !!previous &&
        (next.snapshot.incarnation ?? 0) !== previousIncarnation;
      if (incarnationChanged) {
        this.#rateLimitObservations.delete(event.projectId);
      }
      if (incarnationChanged || !isRuntimeHealthy(next.candidate)) {
        this.#clearRuntimeTransferProtection(event.projectId);
      }
      // Awareness publishes frequently for frame/RTT progress. Queue draining
      // only becomes useful when admission credits increase, health returns,
      // or a fresh incarnation makes an excluded retry target safe again.
      if (
        !previous ||
        nextCredits.admission > previousCredits.admission ||
        nextCredits.prefetch > previousCredits.prefetch ||
        nextCredits.transfer > previousCredits.transfer ||
        incarnationChanged ||
        next.retryEpoch > previous.retryEpoch
      ) this.#scheduleDrain();
    });
  }

  admissionTiming(): Readonly<{
    queueTimeoutMs: number;
    routeAttemptTimeoutMs: number;
    committedHandleTimeoutMs: number;
    preparedProofUsesAttemptBudget: true;
  }> {
    return Object.freeze({
      queueTimeoutMs: this.#queueTimeoutMs,
      routeAttemptTimeoutMs: this.#routeAttemptTimeoutMs,
      committedHandleTimeoutMs: this.#committedHandleTimeoutMs,
      preparedProofUsesAttemptBudget: true,
    });
  }

  async startRelay(
    body: Record<string, unknown>,
    options: StartRelayOptions = {},
  ): Promise<RunHandle> {
    let incomingLeaseOwned = options.incomingLease !== undefined;
    const releaseIncomingLease = () => {
      if (!incomingLeaseOwned) return;
      incomingLeaseOwned = false;
      this.#releaseLeaseAfter(options.incomingLease!, options.retainMemoryUntil);
    };
    try {
      return await this.#startRelayWithIncomingLeaseOwnership(body, options, {
        release: releaseIncomingLease,
        transfer: () => { incomingLeaseOwned = false; },
      });
    } catch (error) {
      // Until replace() succeeds, the router owns the caller's ingress lease.
      // One boundary covers normalization, provenance, structural sizing,
      // classification, and resource-admission failures without relying on an
      // adapter-specific outer finally. The one-shot guard prevents duplicates.
      releaseIncomingLease();
      throw error;
    }
  }

  async #startRelayWithIncomingLeaseOwnership(
    body: Record<string, unknown>,
    options: StartRelayOptions,
    incomingLeaseOwnership: Readonly<{
      release(): void;
      transfer(): void;
    }>,
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
    if (options.fallbackFrom) {
      this.#emit("request.model_fallback", {
        trace_id: traceId,
        parent_trace_id: options.fallbackFrom.traceId,
        requested_model: safeObservabilityModel(options.fallbackFrom.model),
        fallback_model: FALLBACK_UPSTREAM_MODEL,
        error_kind: "model_not_found",
        provider_origin: options.fallbackFrom.origin,
        retry_count: 1,
      });
    }
    const authorizationScope = this.#defaultAuthorizationScope;
    if (
      (options.authorizationScope !== undefined &&
        options.authorizationScope !== authorizationScope) ||
      authorizationScope !== null &&
      !isAuthenticatedTenantScope(authorizationScope)
    ) {
      const error = new ResponseProvenanceScopeError();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "continuation", error, 0);
      throw error;
    }
    // This is the shared routing choke point. Public and producer-specific
    // callers normalize defensively too, while copy-on-write keeps client-facing
    // stream semantics and caller-owned request objects unchanged.
    const normalizedBody = prepareHostUpstreamResponsesRequest(body);
    const relayBody: Record<string, unknown> = applyUpstreamServiceTierPolicy(normalizedBody);
    // The caller can retain and mutate its graph while resource admission is
    // pending. Continuation authority is therefore resolved only from the
    // immutable, policy-normalized snapshot which will actually execute.
    let continuationProvenance: ResponseProvenance | null = null;
    const incomingBodyBytes = options.incomingLease?.bodyBytes;
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
      : options.structuralBodyBytes !== undefined
        ? Math.min(
            Number.MAX_SAFE_INTEGER,
            originalStructuralBytes + SHALLOW_UPSTREAM_POLICY_HEADROOM_BYTES,
          )
        : jsonEncodedByteLength(relayBody);
    const adjustedEncodedBytes = encodedBodyBytes === undefined
      ? undefined
      : Math.min(
          Number.MAX_SAFE_INTEGER,
          Math.max(0, encodedBodyBytes - originalStructuralBytes) + relayStructuralBytes,
        );
    let workload = classifyWorkload(relayBody, {
      structuralBytes: relayStructuralBytes,
      ...(adjustedEncodedBytes === undefined ? {} : { encodedBytes: adjustedEncodedBytes }),
    });
    let decodedRequestBytes = relayStructuralBytes;
    let schedulingWorkload = options.fairnessRequestBytes !== undefined &&
        options.fairnessRequestBytes > workload.requestBytes
      ? classifyWorkload(relayBody, {
          encodedBytes: options.fairnessRequestBytes,
          structuralBytes: relayStructuralBytes,
        })
      : workload;
    const routingTimeouts = requestRoutingTimeouts(
      workload.requestBytes,
      this.#queueTimeoutMs,
      this.#routeAttemptTimeoutMs,
      this.#automaticRouteAttemptTimeout,
    );
    const queueTimeoutMs = routingTimeouts.queueTimeoutMs;
    let routeAttemptTimeoutMs = routingTimeouts.routeAttemptTimeoutMs;
    const payloadDeadlineMs = acceptedAtMs + queueTimeoutMs;
    let admissionDeadlineMs = typeof inheritedDeadlineMs === "number" &&
        Number.isFinite(inheritedDeadlineMs) && inheritedDeadlineMs >= acceptedAtMs
      ? Math.max(inheritedDeadlineMs, payloadDeadlineMs)
      : payloadDeadlineMs;
    // Multipart image input and its base64 relay representation remain live
    // together until the compatibility request finishes. The first fan-out
    // job receives the one-shot ingress lease and conservatively reserves both
    // phases in one replacement lease; later jobs use ordinary admission.
    const retainIncomingLeaseBytes = options.source === "image" ||
      options.retainIncomingLeaseBytes === true;
    const retainedIngressBytes = retainIncomingLeaseBytes &&
        typeof options.incomingLease?.reservedBytes === "number" &&
        Number.isSafeInteger(options.incomingLease.reservedBytes) &&
        options.incomingLease.reservedBytes > 0
      ? options.incomingLease.reservedBytes
      : 0;
    let admissionWorkload = retainedIngressBytes > 0
      ? {
          ...workload,
          reservedBytes: Math.min(
            Number.MAX_SAFE_INTEGER,
            workload.reservedBytes + retainedIngressBytes,
          ),
        }
      : workload;
    let standardAdmissionCost = v2RequestTransferCost(decodedRequestBytes, "standard");
    let bulkAdmissionCost = v2RequestTransferCost(decodedRequestBytes, "bulk-v1");
    const acceptedSnapshot = this.snapshot();
    this.#emit("request.accepted", {
      trace_id: traceId,
      source: options.source ?? "internal",
      model: safeObservabilityModel(workload.model),
      service_tier: typeof relayBody.service_tier === "string" ? relayBody.service_tier : "unknown",
      reasoning_effort: safeObservabilityReasoningEffort(relayBody.reasoning),
      stream: workload.stream,
      workload_class: schedulingWorkload.class,
      request_bytes: schedulingWorkload.requestBytes,
      input_items: Array.isArray(relayBody.input) ? relayBody.input.length : null,
      has_previous_response_id: Object.hasOwn(relayBody, "previous_response_id"),
      transfer_admission_cost: standardAdmissionCost,
      reserved_bytes: admissionWorkload.reservedBytes,
      capacity: acceptedSnapshot.capacity,
      active: acceptedSnapshot.active,
      available: acceptedSnapshot.available,
      queued: acceptedSnapshot.queued,
    });
    try {
      validateResponsesInputJson(relayBody);
    } catch (error) {
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "validation", error, 0);
      throw error;
    }
    const admissionWaitController = new AbortController();
    const admissionTracker: IncomingRequestTracker = { controller: admissionWaitController };
    const abortAdmissionWait = () => admissionWaitController.abort(
      sanitizedRouterAbortReason(options.signal),
    );
    const finishAdmissionWait = () => {
      options.signal?.removeEventListener("abort", abortAdmissionWait);
      this.#pendingAdmissions.delete(admissionTracker);
    };
    if (options.signal?.aborted) abortAdmissionWait();
    else options.signal?.addEventListener("abort", abortAdmissionWait, { once: true });
    this.#pendingAdmissions.add(admissionTracker);
    let memoryLease: ResourceLease | null = null;
    try {
      if (options.incomingLease?.replace) {
        memoryLease = await options.incomingLease.replace(
          admissionWorkload,
          admissionWaitController.signal,
          { deadlineMs: admissionDeadlineMs },
        );
        incomingLeaseOwnership.transfer();
      } else {
        memoryLease = await this.#governor.acquire(
          admissionWorkload,
          admissionWaitController.signal,
          { deadlineMs: admissionDeadlineMs },
        );
        incomingLeaseOwnership.release();
      }
    } catch (error) {
      if (memoryLease) this.#releaseLeaseAfter(memoryLease, options.retainMemoryUntil);
      finishAdmissionWait();
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      const controllerReason = admissionWaitController.signal.aborted
        ? admissionWaitController.signal.reason
        : undefined;
      const failure = controllerReason instanceof RouterShutdownError ||
          controllerReason instanceof RouterCapacityTimeoutError
        ? controllerReason
        : error;
      this.#failure(traceId, acceptedAtMs, "admission", failure, 0);
      throw failure;
    }
    if (!memoryLease) {
      finishAdmissionWait();
      throw new Error("Resource admission returned no lease");
    }
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
      finishAdmissionWait();
      this.#releaseLeaseAfter(memoryLease, options.retainMemoryUntil);
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      if (options.signal?.aborted) throw sanitizedRouterAbortReason(options.signal);
      throw new RouterShutdownError();
    }
    let immutableBody: Record<string, unknown>;
    let measuredFinalStructuralBytes = 0;
    try {
      const immutableCallerSnapshot = await immutableRelaySnapshot(
        body,
        admissionWaitController.signal,
        () => this.#clock.now() >= admissionDeadlineMs,
      );
      const immutableCallerBody = immutableCallerSnapshot.body;
      const normalizedSnapshot = prepareHostUpstreamResponsesRequest(immutableCallerBody);
      const policySnapshot = applyUpstreamServiceTierPolicy(normalizedSnapshot);
      immutableBody = Object.isFrozen(policySnapshot)
        ? policySnapshot
        : Object.freeze(policySnapshot);
      const shallowMeasuredBytes = policySnapshot === immutableCallerBody
        ? immutableCallerSnapshot.structuralBytes
        : shallowPolicyStructuralBytes(
            immutableCallerBody,
            immutableCallerSnapshot.structuralBytes,
            immutableBody,
          );
      measuredFinalStructuralBytes = shallowMeasuredBytes ??
        await jsonEncodedByteLengthAsync(immutableBody, {
          signal: admissionWaitController.signal,
        });
    } catch (error) {
      finishAdmissionWait();
      this.#releaseLeaseAfter(memoryLease, options.retainMemoryUntil);
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(traceId, acceptedAtMs, "snapshot", error, 0);
      throw error;
    }
    try {
      // The caller-owned graph may have changed while admission awaited. All
      // execution, fairness, deadlines, and permits are derived again from the
      // frozen snapshot so a small measured request cannot grow into a large
      // under-accounted attempt.
      // A caller-provided wire measurement may conservatively exceed the
      // materialized graph (tests and compatibility adapters use this for
      // already-owned encoded data), but it may never hide graph growth which
      // occurred while resource admission was pending.
      const finalStructuralBytes = Math.max(
        measuredFinalStructuralBytes,
        options.structuralBodyBytes === undefined ? 0 : relayStructuralBytes,
      );
      validateResponsesInputJson(immutableBody);
      const finalEncodedBytes = adjustedEncodedBytes === undefined
        ? undefined
        : Math.max(adjustedEncodedBytes, finalStructuralBytes);
      const finalWorkload = classifyWorkload(immutableBody, {
        structuralBytes: finalStructuralBytes,
        ...(finalEncodedBytes === undefined ? {} : { encodedBytes: finalEncodedBytes }),
      });
      continuationProvenance = this.#resolveContinuationProvenance(
        immutableBody,
        authorizationScope,
      );
      const finalSchedulingWorkload = options.fairnessRequestBytes !== undefined &&
          options.fairnessRequestBytes > finalWorkload.requestBytes
        ? classifyWorkload(immutableBody, {
            encodedBytes: options.fairnessRequestBytes,
            structuralBytes: finalStructuralBytes,
          })
        : finalWorkload;
      const finalAdmissionWorkload = retainedIngressBytes > 0
        ? {
            ...finalWorkload,
            reservedBytes: Math.min(
              Number.MAX_SAFE_INTEGER,
              finalWorkload.reservedBytes + retainedIngressBytes,
            ),
          }
        : finalWorkload;
      const finalRoutingTimeouts = requestRoutingTimeouts(
        finalWorkload.requestBytes,
        this.#queueTimeoutMs,
        this.#routeAttemptTimeoutMs,
        this.#automaticRouteAttemptTimeout,
      );
      admissionDeadlineMs = Math.max(
        admissionDeadlineMs,
        acceptedAtMs + finalRoutingTimeouts.queueTimeoutMs,
      );
      routeAttemptTimeoutMs = finalRoutingTimeouts.routeAttemptTimeoutMs;
      if (finalAdmissionWorkload.reservedBytes > memoryLease.reservedBytes) {
        memoryLease = await memoryLease.replace(
          finalAdmissionWorkload,
          admissionWaitController.signal,
          { deadlineMs: admissionDeadlineMs },
        );
      }
      workload = finalWorkload;
      decodedRequestBytes = finalStructuralBytes;
      schedulingWorkload = finalSchedulingWorkload;
      admissionWorkload = finalAdmissionWorkload;
      standardAdmissionCost = v2RequestTransferCost(decodedRequestBytes, "standard");
      bulkAdmissionCost = v2RequestTransferCost(decodedRequestBytes, "bulk-v1");
    } catch (error) {
      finishAdmissionWait();
      this.#releaseLeaseAfter(memoryLease, options.retainMemoryUntil);
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      this.#failure(
        traceId,
        acceptedAtMs,
        error instanceof ResponseProvenanceScopeError
          ? "continuation"
          : "snapshot",
        error,
        0,
      );
      throw error;
    }
    // A flat snapshot can resolve in the same microtask turn. An abort queued
    // behind that resolution would otherwise land before the job listener is
    // installed and remain queued until its full routing deadline.
    if (options.signal?.aborted || this.#closed || this.#shuttingDown) {
      finishAdmissionWait();
      this.#releaseLeaseAfter(memoryLease, options.retainMemoryUntil);
      this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
      if (options.signal?.aborted) throw sanitizedRouterAbortReason(options.signal);
      throw new RouterShutdownError();
    }
    return new Promise<RunHandle>((resolve, reject) => {
      const clientId = boundedClientId(options.clientId, source);
      const enqueuedAtMs = this.#clock.now();
      const job: QueuedJob = {
        body: immutableBody,
        source,
        workload,
        decodedRequestBytes,
        clientId,
        authorizationScope,
        continuationProvenance,
        provenanceReservation: null,
        signal: options.signal,
        completeIncomingAdmission: options.incomingLease?.completeAdmission
          ? () => options.incomingLease!.completeAdmission!()
          : null,
        memoryLease,
        retainFullLeaseAfterCommit: retainedIngressBytes > 0 ||
          options.retainMemoryUntil !== undefined,
        retainMemoryUntil: options.retainMemoryUntil ?? null,
        memoryReleaseScheduled: false,
        attempts: 0,
        routeSelected: false,
        excluded: new Map(),
        resolve,
        reject,
        traceId,
        acceptedAtMs,
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
        queueCapacityHeld: false,
        deadlineIndex: -1,
        retryWakeAtMs: null,
      };
      job.onAbort = () => {
        if (!this.#removeJob(job)) return;
        this.#clearRetryWake(job);
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
        const error = selectedRouteDeadlineError(
          sanitizedRouterAbortReason(job.signal),
          job.routeSelected,
        );
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
        reject(error);
      };
      options.signal?.addEventListener("abort", job.onAbort, { once: true });
      // Ownership has moved atomically from the shutdown-visible admission
      // tracker to the queued job and its abort listener.
      finishAdmissionWait();
      if (!this.#enqueue(job)) {
        options.signal?.removeEventListener("abort", job.onAbort);
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
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
    const reservationMultiplier = options.reservationMultiplier ?? 2;
    if (!Number.isSafeInteger(reservationMultiplier) ||
      reservationMultiplier < 2 || reservationMultiplier > 6) {
      throw new RangeError("Incoming body reservation multiplier must be an integer from 2 through 6");
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
    const onCallerAbort = () => controller.abort(sanitizedRouterAbortReason(signal));
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
      this.#incomingRequests.delete(tracker);
    };
    const cleanup = () => {
      if (!active) return;
      active = false;
      completeAdmission();
      signal?.removeEventListener("abort", onCallerAbort);
    };
    if (signal?.aborted) onCallerAbort();
    else signal?.addEventListener("abort", onCallerAbort, { once: true });
    armDeadline();
    this.#incomingRequests.add(tracker);

    const incomingWorkload = (
      bodyBytes: number,
      reservedBytes = Math.max(
        8 * MiB,
        bodyBytes * reservationMultiplier + 2 * MiB,
      ),
    ): WorkloadDescriptor => ({
      class: bodyBytes > 512 * KiB ? "text-large" : "text-small",
      requestBytes: bodyBytes,
      // Chunk buffers and the final contiguous Buffer coexist briefly.
      reservedBytes,
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
      if (controller.signal.aborted && controller.signal.reason !== undefined) {
        throw controller.signal.reason;
      }
      throw error;
    }
    if (controller.signal.aborted || !active) {
      const error = controller.signal.reason ?? abortError();
      lease.release();
      lease = null;
      cleanup();
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
        if (observedBodyBytes > QUEUE_PAYLOAD_AWARE_DEADLINE_BYTES) {
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
        const current = lease;
        const replacement = await current.replace(next, controller.signal, { deadlineMs });
        if (!active || controller.signal.aborted || lease !== current) {
          // release() may win while the governor atomically invalidates current
          // and this async continuation is still queued. Never publish an
          // orphaned replacement lease after ownership has ended.
          replacement.release();
          if (controller.signal.aborted) throw controller.signal.reason ?? abortError();
          throw new Error("Incoming body lease was released during growth");
        }
        lease = replacement;
      },
      replace: async (workload, replacementSignal, options) => {
        if (!active || !lease) {
          throw new Error("Incoming body lease is no longer available for replacement");
        }
        if (replacementSignal?.aborted) throw replacementSignal.reason ?? abortError();
        const effectiveSignal = replacementSignal ?? controller.signal;
        const maximumDeadline = Math.max(
          deadlineMs,
          acceptedAtMs + scaledQueueTimeoutMs(
            workload.requestBytes,
            this.#queueTimeoutMs,
          ),
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
        const current = lease;
        const replacement = await current.replace(
          workload,
          effectiveSignal,
          { deadlineMs: effectiveDeadline },
        );
        if (!active || effectiveSignal.aborted || lease !== current) {
          replacement.release();
          if (effectiveSignal.aborted) throw effectiveSignal.reason ?? abortError();
          throw new Error("Incoming body lease was released during replacement");
        }
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
    if (
      authorizationScope === undefined ||
      authorizationScope !== this.#defaultAuthorizationScope
    ) return { kind: "scope_mismatch" };
    const scope = this.#defaultAuthorizationScope;
    if (
      !scope ||
      !isAuthenticatedTenantScope(scope) ||
      (this.#defaultAuthorizationScope !== null &&
        scope !== this.#defaultAuthorizationScope)
    ) {
      return { kind: "scope_mismatch" };
    }
    return this.#responseProvenance.lookup(responseId, scope);
  }

  /** Fleet retirement fence for retained provider-lineage routing hints. */
  responseProvenanceBindingsForProject(projectId: ProjectId): number {
    return this.#responseProvenance.bindingsForProject(projectId);
  }

  #resolveContinuationProvenance(
    body: Readonly<Record<string, unknown>>,
    authorizationScope: AuthenticatedTenantScope | null,
  ): ResponseProvenance | null {
    const hasPreviousResponse = Object.hasOwn(body, "previous_response_id") &&
      body.previous_response_id !== undefined &&
      body.previous_response_id !== null;
    if (!hasPreviousResponse) return null;
    if (authorizationScope === null) throw new ResponseProvenanceScopeError();
    const lookup = this.#responseProvenance.lookup(
      body.previous_response_id,
      authorizationScope,
    );
    if (lookup.kind === "scope_mismatch") throw new ResponseProvenanceScopeError();
    if (lookup.kind !== "known") return null;
    // The provider owns the stored response. A retired local incarnation only
    // makes its routing preference stale; it cannot invalidate provider lineage.
    if (this.#isContinuationHintStale(lookup.provenance)) return null;
    return lookup.provenance;
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
    if (!job.provenanceReservation) return;
    this.#responseProvenance.release(job.provenanceReservation);
    job.provenanceReservation = null;
    this.#scheduleDrain();
  }

  #completeIncomingAdmission(job: QueuedJob): void {
    const complete = job.completeIncomingAdmission;
    job.completeIncomingAdmission = null;
    complete?.();
  }

  #releaseQueueCapacity(job: QueuedJob): void {
    if (!job.queueCapacityHeld) return;
    this.#clearWeightedTransferProtection(job);
    this.#weightedTransferWaiters.delete(job);
    if (this.#weightedTransferWaiters.size === 0) {
      this.#weightedTransferProtectionCheckAtMs = null;
    }
    job.queueCapacityHeld = false;
    this.#queueCapacityJobs = Math.max(0, this.#queueCapacityJobs - 1);
    this.#queueCapacityBytes = Math.max(
      0,
      this.#queueCapacityBytes - Math.max(0, job.workload.requestBytes),
    );
  }

  #releaseLeaseAfter(
    lease: ReleasableLease,
    retainUntil: Promise<unknown> | null | undefined,
  ): void {
    if (!retainUntil) {
      lease.release();
      return;
    }
    const release = Promise.resolve(retainUntil)
      .catch(() => undefined)
      .then(() => { lease.release(); })
      .catch(() => undefined);
    this.#deferredMemoryReleases.add(release);
    void release.finally(() => {
      this.#deferredMemoryReleases.delete(release);
    });
  }

  #releaseJobMemory(job: QueuedJob): void {
    if (job.memoryReleaseScheduled) return;
    job.memoryReleaseScheduled = true;
    const retainUntil = job.retainMemoryUntil;
    job.retainMemoryUntil = null;
    this.#releaseLeaseAfter(job.memoryLease, retainUntil);
  }

  async #releaseCommittedRequestBody(job: QueuedJob): Promise<void> {
    // Drop the immutable graph before reducing the reservation that owns it.
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
        });
      } catch {
        // Accounting optimization failure leaves the original lease authoritative.
        job.memoryLease = committedLease;
      }
    }
  }

  async #startModelFallback(
    job: QueuedJob,
    body: Record<string, unknown>,
    requestedModel: string,
    error: V2CommittedError,
    bodyRetainedUntil: Promise<void>,
  ): Promise<RunHandle> {
    if (this.#closed || this.#shuttingDown) throw error;
    if (job.signal?.aborted) throw sanitizedRouterAbortReason(job.signal);
    const originalLease = job.memoryLease;
    const reservationFloor = originalLease.reservedBytes;
    const originalRetention = job.retainMemoryUntil;
    const retainMemoryUntil = originalRetention
      ? Promise.allSettled([originalRetention, bodyRetainedUntil])
      : bodyRetainedUntil;
    const retainFloor = (lease: ResourceLease): ResourceLease => ({
      get reservedBytes() { return lease.reservedBytes; },
      release: () => lease.release(),
      replace: async (workload, signal, options) => retainFloor(await lease.replace({
        ...workload,
        reservedBytes: Math.max(reservationFloor, workload.reservedBytes),
      }, signal, options)),
    });
    // A rejected parent already owns enough memory for this request. Replace
    // that lease atomically instead of waiting for a second full allocation.
    // Its ingress graph may remain live, so preserve the original floor and
    // lifetime on every child replacement until the fallback is finished.
    const incomingLease: IncomingResourceLease = {
      replace: async (workload, signal, options) => {
        const replacement = await originalLease.replace({
          ...workload,
          reservedBytes: Math.max(reservationFloor, workload.reservedBytes),
        }, signal, options);
        job.memoryReleaseScheduled = true;
        job.retainMemoryUntil = null;
        return retainFloor(replacement);
      },
      // Before successful replacement the parent still owns cleanup. After it,
      // startRelay owns the returned child lease, including failed admission.
      release() {},
    };
    const handle = await this.startRelay({ ...body, model: FALLBACK_UPSTREAM_MODEL }, {
      clientId: job.clientId,
      source: job.source,
      signal: job.signal,
      authorizationScope: job.authorizationScope ?? undefined,
      incomingLease,
      retainMemoryUntil,
      fallbackFrom: {
        traceId: job.traceId,
        model: requestedModel,
        origin: error.providerError!.origin,
      },
    });
    return {
      modelFallback: Object.freeze({ from: requestedModel, to: FALLBACK_UPSTREAM_MODEL }),
      events: () => handle.events(),
      discardUnconsumedEvents: () => handle.discardUnconsumedEvents?.(),
      final: handle.final,
      get rateLimit() { return handle.rateLimit; },
      get upstreamPrelude() { return handle.upstreamPrelude; },
      stats: handle.stats,
    };
  }

  #isContinuationHintStale(provenance: ResponseProvenance): boolean {
    const current = this.#runtimeCache.get(provenance.projectId);
    return !current ||
      (current.snapshot.incarnation ?? 0) !== provenance.incarnation;
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

  #fleetTransferFits(cost: number): boolean {
    if (cost <= 0) return true;
    const budget = cost === V2_BULK_TRANSFER_COST || this.#fleetBulkTransferReserved > 0
      ? FLEET_TRANSFER_BUDGET
      : FLEET_ORDINARY_TRANSFER_BUDGET;
    return this.#fleetTransferReserved + cost <= budget;
  }

  #reserveFleetTransfer(cost: number): FleetTransferLease | null {
    if (cost <= 0) return null;
    if (!this.#fleetTransferFits(cost)) {
      throw new Error("Fleet transfer budget was oversubscribed after route selection");
    }
    this.#fleetTransferReserved += cost;
    if (cost === V2_BULK_TRANSFER_COST) this.#fleetBulkTransferReserved += cost;
    return { cost, released: false };
  }

  #releaseFleetTransfer(lease: FleetTransferLease | null): boolean {
    if (!lease || lease.released) return false;
    lease.released = true;
    this.#fleetTransferReserved = Math.max(0, this.#fleetTransferReserved - lease.cost);
    if (lease.cost === V2_BULK_TRANSFER_COST) {
      this.#fleetBulkTransferReserved -= lease.cost;
    }
    return true;
  }

  #downgradeFleetTransferToControl(lease: FleetTransferLease | null): boolean {
    if (!lease || lease.released || lease.cost <= 1) return false;
    const releasedCost = lease.cost - 1;
    if (lease.cost === V2_BULK_TRANSFER_COST) {
      this.#fleetBulkTransferReserved -= lease.cost;
    }
    lease.cost = 1;
    this.#fleetTransferReserved = Math.max(
      0,
      this.#fleetTransferReserved - releasedCost,
    );
    return true;
  }

  #clearWeightedTransferProtection(job: QueuedJob): void {
    let cleared = false;
    if (this.#fleetTransferProtection?.job === job) {
      this.#fleetTransferProtection = null;
      cleared = true;
    }
    for (const [projectId, protection] of this.#runtimeTransferProtections) {
      if (protection.job === job) {
        this.#runtimeTransferProtections.delete(projectId);
        cleared = true;
      }
    }
    if (cleared) {
      this.#weightedTransferProtectionCheckAtMs = null;
      this.#scheduleDrain();
    }
  }

  #clearRuntimeTransferProtection(projectId: ProjectId): void {
    const protection = this.#runtimeTransferProtections.get(projectId);
    if (protection) this.#clearWeightedTransferProtection(protection.job);
    if (this.#fleetTransferProtection?.projectId === projectId) {
      this.#clearWeightedTransferProtection(this.#fleetTransferProtection.job);
    }
  }

  #pruneWeightedTransferProtections(): void {
    const now = this.#clock.now();
    const protections = new Set<WeightedTransferProtection>();
    if (this.#fleetTransferProtection) protections.add(this.#fleetTransferProtection);
    for (const protection of this.#runtimeTransferProtections.values()) {
      protections.add(protection);
    }
    for (const protection of protections) {
      const cached = this.#runtimeCache.get(protection.projectId);
      const current = cached ? this.#agedCandidate(cached) : null;
      if (
        !protection.job.queueCapacityHeld ||
        protection.job.signal?.aborted ||
        now >= protection.job.queueDeadlineMs ||
        !current ||
        runtimeIncarnationKey(current) !== protection.incarnationKey ||
        !isRuntimeHealthy(current)
      ) this.#clearWeightedTransferProtection(protection.job);
    }
  }

  #weightedProtectionBlocks(
    job: QueuedJob,
    candidate: RouteCandidate,
    admissionCost: number,
    fleetTransferCost: number,
  ): boolean {
    // Tiny work is deliberately outside weighted fleet pacing and may always
    // use an otherwise eligible runtime. Its bounded one-unit transfer remains
    // authoritative inside V2.
    if (job.decodedRequestBytes <= FLEET_TRANSFER_EXEMPT_BYTES) return false;
    const fleet = this.#fleetTransferProtection;
    if (fleetTransferCost > 0 && fleet && fleet.job !== job) {
      // A waiting bulk body needs four units, not the two units reserved for
      // interactive traffic. Discount the current bulk body when protecting
      // its successor; otherwise an aged waiter reintroduces fleet-wide stalls.
      const ordinaryReserved = this.#fleetTransferReserved - this.#fleetBulkTransferReserved;
      if (fleetTransferCost === V2_BULK_TRANSFER_COST ||
          ordinaryReserved + fleetTransferCost > FLEET_TRANSFER_HEADROOM) return true;
    }
    const runtime = this.#runtimeTransferProtections.get(candidate.projectId);
    return admissionCost > 0 && !!runtime && runtime.job !== job;
  }

  #refreshAgedWeightedTransferProtection(available: readonly RouteCandidate[]): void {
    if (this.#fleetTransferProtection || this.#runtimeTransferProtections.size > 0) return;
    const now = this.#clock.now();
    if (
      this.#weightedTransferProtectionCheckAtMs !== null &&
      now < this.#weightedTransferProtectionCheckAtMs
    ) return;
    let nextCheckAtMs = Number.POSITIVE_INFINITY;
    for (const job of this.#weightedTransferWaiters) {
      if (
        !job.queueCapacityHeld ||
        !job.queueLane ||
        job.signal?.aborted ||
        now >= job.queueDeadlineMs
      ) {
        this.#weightedTransferWaiters.delete(job);
        continue;
      }
      const protectAt = job.acceptedAtMs + WEIGHTED_TRANSFER_PROTECTION_AGE_MS;
      if (now < protectAt) {
        nextCheckAtMs = Math.min(nextCheckAtMs, protectAt);
        continue;
      }
      const eligible = available.flatMap(candidate => {
        const retryBlock = job.excluded.get(runtimeIncarnationKey(candidate));
        if (retryBlock) {
          const retryEpoch = this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0;
          if (retryEpoch <= retryBlock.retryEpoch && now < retryBlock.retryAtMs) return [];
        }
        if (!isRuntimeEligible(this.#candidateForJob(job, candidate))) return [];
        const profile = candidate.transportProfile === "bulk-v1" ? "bulk-v1" : "standard";
        const admissionCost = profile === "bulk-v1"
          ? job.bulkAdmissionCost
          : job.standardAdmissionCost;
        if (admissionCost !== V2_BULK_TRANSFER_COST) return [];
        const reflected = Number.isFinite(candidate.transferCredits)
          ? Math.max(0, candidate.transferCredits!)
          : v2HostTransferBudget(profile);
        return [{
          candidate,
          fleetTransferCost: this.#fleetTransferCost(job, candidate),
          runtimeAvailable: Math.max(
            0,
            reflected - (candidate.reservedTransferCost ?? 0),
          ),
        }];
      });
      if (eligible.length === 0) {
        nextCheckAtMs = Math.min(nextCheckAtMs, now + PRECOMMIT_RETRY_BASE_MS);
        continue;
      }
      const target = eligible.sort((left, right) => (
        right.runtimeAvailable - left.runtimeAvailable ||
        (left.candidate.active + left.candidate.reserved) -
          (right.candidate.active + right.candidate.reserved) ||
        String(left.candidate.projectId).localeCompare(String(right.candidate.projectId))
      ))[0]!;
      const protection: WeightedTransferProtection = {
        job,
        projectId: target.candidate.projectId,
        incarnationKey: runtimeIncarnationKey(target.candidate),
      };
      this.#runtimeTransferProtections.set(target.candidate.projectId, protection);
      if (target.fleetTransferCost > 0) this.#fleetTransferProtection = protection;
      this.#weightedTransferProtectionCheckAtMs = null;
      return;
    }
    this.#weightedTransferProtectionCheckAtMs = Number.isFinite(nextCheckAtMs)
      ? nextCheckAtMs
      : null;
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
      fleetBulkTransferReserved: this.#fleetBulkTransferReserved,
      fleetOrdinaryTransferBudget: FLEET_ORDINARY_TRANSFER_BUDGET,
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
      this.#releaseJobMemory(job);
      this.#releaseJobProvenance(job);
      this.#releaseQueueCapacity(job);
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
        this.#pendingAdmissions.size === 0 &&
        this.#deferredMemoryReleases.size === 0 &&
        this.#lateAttemptCleanups.size === 0
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
    this.#fleetTransferProtection = null;
    this.#runtimeTransferProtections.clear();
    this.#weightedTransferWaiters.clear();
    this.#weightedTransferProtectionCheckAtMs = null;
    this.#responseProvenance.clear();
    this.#registryUnsubscribe();
  }

  #enqueue(job: QueuedJob, front = false): boolean {
    if (job.queueLane) throw new Error("Router job is already queued");
    const requestBytes = Math.max(0, job.workload.requestBytes);
    if (!job.queueCapacityHeld) {
      if (
        this.#queueCapacityJobs >= this.#queueCapacity ||
        requestBytes > this.#queueByteCapacity - this.#queueCapacityBytes
      ) return false;
      job.queueCapacityHeld = true;
      this.#queueCapacityJobs++;
      this.#queueCapacityBytes += requestBytes;
    }

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
    if (
      job.decodedRequestBytes > FLEET_TRANSFER_EXEMPT_BYTES &&
      (
        job.standardAdmissionCost === V2_BULK_TRANSFER_COST ||
        job.bulkAdmissionCost === V2_BULK_TRANSFER_COST
      )
    ) {
      this.#weightedTransferWaiters.add(job);
      const protectAt = job.acceptedAtMs + WEIGHTED_TRANSFER_PROTECTION_AGE_MS;
      this.#weightedTransferProtectionCheckAtMs =
        this.#weightedTransferProtectionCheckAtMs === null
          ? protectAt
          : Math.min(this.#weightedTransferProtectionCheckAtMs, protectAt);
    }
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
    scanned?: ReadonlySet<QueuedJob>,
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
      if (!job || scanned?.has(job)) continue;
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

  #nextJob(scanned: ReadonlySet<QueuedJob>): QueuedJob | null {
    if (this.#queuedJobs < 1) return null;
    const latency = this.#fairClasses.latency;
    const bulk = this.#fairClasses.bulk;
    const activeClasses = Number(latency.lanes > 0) + Number(bulk.lanes > 0);
    if (activeClasses === 1) {
      const state = latency.lanes > 0 ? latency : bulk;
      return this.#takeFromClass(state, Number.POSITIVE_INFINITY, scanned);
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
      const job = this.#takeFromClass(state, state.deficitBytes, scanned);
      if (!job) {
        state.turnActive = false;
        this.#classCursor = this.#classCursor === "latency" ? "bulk" : "latency";
        continue;
      }
      // Rotate classes after every successful dispatch while retaining each
      // class's byte deficit. This preserves weighted DRR credit without
      // allowing hundreds of tiny latency requests to monopolize one turn.
      this.#classCursor = this.#classCursor === "latency" ? "bulk" : "latency";
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
    this.#pruneWeightedTransferProtections();
    let remainingScan = this.#queuedJobs;
    // At most the queue's existing count bound, retained only for this drain.
    // A re-enqueued, high-credit blocked head must not consume every visit and
    // prevent a newly queued medium request from accruing enough DRR credit.
    const scanned = new Set<QueuedJob>();
    const candidates = [...this.#runtimeCache.values()]
      .map(cached => this.#candidateWithReservations(this.#agedCandidate(cached)))
      .filter(candidate => this.#candidateCredits(candidate) > 0);
    if (candidates.length === 0) return;
    this.#refreshAgedWeightedTransferProtection(candidates);
    while (remainingScan-- > 0) {
      const job = this.#nextJob(scanned);
      if (!job) return;
      scanned.add(job);
      if (this.#clock.now() >= job.queueDeadlineMs) {
        this.#expireJob(job);
        continue;
      }
      if (job.signal?.aborted) {
        job.signal.removeEventListener("abort", job.onAbort!);
        this.#clearRetryWake(job);
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
        const error = selectedRouteDeadlineError(
          sanitizedRouterAbortReason(job.signal),
          job.routeSelected,
        );
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
          this.#releaseJobMemory(job);
          this.#releaseJobProvenance(job);
          this.#releaseQueueCapacity(job);
          const error = new RouterQueueFullError();
          this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
          this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
          job.reject(error);
        } else {
          this.#armRetryWake(job);
        }
        continue;
      }
      if (job.authorizationScope && !job.provenanceReservation) {
        const reservation = this.#responseProvenance.reserve();
        if (!reservation) {
          // Terminal metadata capacity is an execution gate, not an ingress
          // quota. Keep the request in the already-bounded router queue until
          // an active terminal consumes or releases a reservation.
          if (!this.#enqueue(job)) {
            job.signal?.removeEventListener("abort", job.onAbort!);
            this.#releaseJobMemory(job);
            this.#releaseQueueCapacity(job);
            const error = new RouterQueueFullError();
            this.#traffic.rejected({
              totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs),
            });
            this.#failure(job.traceId, job.acceptedAtMs, "queue", error, job.attempts);
            job.reject(error);
          }
          // No terminal can free metadata capacity synchronously inside this
          // drain pass. commit/release explicitly schedules the next pass; stop
          // here rather than rescanning up to the full 4,096-job queue.
          return;
        }
        job.provenanceReservation = reservation;
      }
      const runtime = selection.runtime;
      this.#clearWeightedTransferProtection(job);
      this.#weightedTransferWaiters.delete(job);
      if (this.#weightedTransferWaiters.size === 0) {
        this.#weightedTransferProtectionCheckAtMs = null;
      }
      this.#clearRetryWake(job);
      this.#chargeFairness(job);
      const selectedAtMs = this.#clock.now();
      job.queueWaitMs = Math.max(0, selectedAtMs - job.queuedAtMs);
      const candidate = selection.candidate;
      job.admissionCost = selection.admissionCost;
      job.routeSelected = true;
      if (selection.fleetTransferPacerCost > 0) {
        this.#consumeFleetTransferPacer(selection.fleetTransferPacerCost, selectedAtMs);
      }
      this.#emit("route.selected", {
        trace_id: job.traceId,
        project_id: runtime.projectId,
        route_ms: Math.max(0, selectedAtMs - routeStarted),
        queue_wait_ms: job.queueWaitMs,
        sandbox_capacity: candidate.capacity,
        sandbox_active: candidate.active,
        sandbox_admission_credits: selection.selectedAvailable,
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
      if (this.#candidateCredits(candidate) <= 0) {
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
    this.#releaseJobMemory(job);
    this.#releaseJobProvenance(job);
    this.#releaseQueueCapacity(job);
    const error = job.routeSelected
      ? new RouterTransportAdmissionTimeoutError()
      : new RouterCapacityTimeoutError();
    this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
    this.#failure(
      job.traceId,
      job.acceptedAtMs,
      job.routeSelected ? "transport-admission" : "queue",
      error,
      job.attempts,
    );
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

  #candidateCreditLanes(candidate: RouteCandidate): {
    admission: number;
    prefetch: number;
    transfer: number;
  } {
    const current = this.#candidateWithReservations(candidate);
    if (!isRuntimeHealthy(current)) return { admission: 0, prefetch: 0, transfer: 0 };
    const usable = current.capacity - current.quarantined;
    const fallback = Number.isFinite(current.available)
      ? Math.max(0, current.available!)
      : Math.max(0, usable - current.active);
    const admission = Number.isFinite(current.admissionCredits)
      ? Math.max(0, current.admissionCredits!)
      : fallback;
    const prefetch = Number.isFinite(current.prefetchCredits)
      ? Math.max(0, current.prefetchCredits!)
      : fallback;
    return {
      admission: Math.max(0, admission - current.reserved),
      prefetch: Math.max(0, prefetch - current.reserved),
      transfer: Math.max(
        0,
        (Number.isFinite(current.transferCredits)
          ? Math.max(0, current.transferCredits!)
          : v2HostTransferBudget(
              current.transportProfile === "bulk-v1" ? "bulk-v1" : "standard",
            )) - (current.reservedTransferCost ?? 0),
      ),
    };
  }

  #candidateCredits(candidate: RouteCandidate): number {
    const lanes = this.#candidateCreditLanes(candidate);
    return Math.max(lanes.admission, lanes.prefetch);
  }

  #hasUnreflectedLatencyReservation(projectId: ProjectId): boolean {
    const jobs = this.#reserved.get(projectId);
    if (!jobs) return false;
    for (const reserved of jobs) {
      if (reserved.decodedRequestBytes <= V2_LATENCY_CRITICAL_REQUEST_BYTES) return true;
    }
    return false;
  }

  #candidateForJob(job: QueuedJob, candidate: RouteCandidate): RouteCandidate {
    const fallback = candidate.available;
    const mayPrefetch = candidate.transportProfile === "bulk-v1" &&
      job.decodedRequestBytes > V2_LATENCY_CRITICAL_REQUEST_BYTES &&
      !this.#hasUnreflectedLatencyReservation(candidate.projectId);
    const selected = mayPrefetch
      ? candidate.prefetchCredits ?? fallback
      : candidate.admissionCredits ?? fallback;
    return {
      ...candidate,
      available: selected,
    };
  }

  #fleetTransferCost(job: QueuedJob, candidate: RouteCandidate): number {
    return this.#runtimeCache.size > 1 &&
        candidate.transportProfile === "bulk-v1" &&
        job.decodedRequestBytes > FLEET_TRANSFER_EXEMPT_BYTES
      ? job.bulkAdmissionCost
      : 0;
  }

  #hasAlternativeRuntime(
    job: QueuedJob,
    selectedIncarnationKey: string,
    fleetTransferLease: FleetTransferLease | null,
  ): boolean {
    const now = this.#clock.now();
    if (now >= job.queueDeadlineMs) return false;
    const available = [...this.#runtimeCache.values()]
      .map(cached => this.#candidateWithReservations(this.#agedCandidate(cached)))
      .filter(candidate => this.#candidateCredits(candidate) > 0);
    for (const candidate of available) {
      if (runtimeIncarnationKey(candidate) === selectedIncarnationKey) continue;
      if (!this.#isCandidateAdmissible(job, candidate, now, fleetTransferLease)) continue;
      if (this.#requiresFleetPacing(job, candidate, available.length)) {
        this.#refillFleetTransferPacer(now);
        if (this.#fleetTransferPacerTokens < this.#fleetPacingCost(job)) continue;
      }
      return true;
    }
    return false;
  }

  #fleetPacingCost(job: QueuedJob): number {
    return job.decodedRequestBytes <= MEDIUM_REQUEST_MAX_BYTES ? FLEET_TRANSFER_PACER_BURST : 1;
  }

  #requiresFleetPacing(job: QueuedJob, candidate: RouteCandidate, availableCount: number): boolean {
    return availableCount > 1 && candidate.transportProfile === "bulk-v1" &&
      job.decodedRequestBytes > FLEET_TRANSFER_EXEMPT_BYTES &&
      job.decodedRequestBytes <= FLEET_TRANSFER_PACED_MAX_BYTES;
  }

  #isCandidateAdmissible(
    job: QueuedJob,
    candidate: RouteCandidate,
    now: number,
    releasedLease: FleetTransferLease | null = null,
  ): boolean {
    const retryBlock = job.excluded.get(runtimeIncarnationKey(candidate));
    if (retryBlock) {
      const retryEpoch = this.#runtimeCache.get(candidate.projectId)?.retryEpoch ?? 0;
      if (retryEpoch <= retryBlock.retryEpoch && now < retryBlock.retryAtMs) return false;
    }
    if (!isRuntimeEligible(this.#candidateForJob(job, candidate))) return false;
    const profile = candidate.transportProfile === "bulk-v1" ? "bulk-v1" : "standard";
    const admissionCost = profile === "bulk-v1" ? job.bulkAdmissionCost : job.standardAdmissionCost;
    const fleetTransferCost = this.#fleetTransferCost(job, candidate);
    if (this.#weightedProtectionBlocks(job, candidate, admissionCost, fleetTransferCost)) return false;
    // The retry can consume the current attempt's fleet permit only after its
    // cancellation proves no execution began. This is eligibility prediction,
    // not an early release or another reservation.
    const releasedCost = releasedLease && !releasedLease.released ? releasedLease.cost : 0;
    if (releasedCost > 0 && fleetTransferCost > 0) {
      const remainingBulkCost = this.#fleetBulkTransferReserved -
        (releasedCost === V2_BULK_TRANSFER_COST ? releasedCost : 0);
      const budget = fleetTransferCost === V2_BULK_TRANSFER_COST || remainingBulkCost > 0
        ? FLEET_TRANSFER_BUDGET
        : FLEET_ORDINARY_TRANSFER_BUDGET;
      if (this.#fleetTransferReserved - releasedCost + fleetTransferCost > budget) return false;
    } else if (!this.#fleetTransferFits(fleetTransferCost)) return false;
    const reflectedTransferCredits = Number.isFinite(candidate.transferCredits)
      ? Math.max(0, candidate.transferCredits!)
      : v2HostTransferBudget(profile);
    return reflectedTransferCredits - (candidate.reservedTransferCost ?? 0) >= admissionCost;
  }

  #choose(job: QueuedJob, available: readonly RouteCandidate[]): RouteSelection | null {
    const now = this.#clock.now();
    let candidates = available.filter(
      candidate => this.#isCandidateAdmissible(job, candidate, now),
    );
    if (candidates.length === 0) return null;
    const eligibleCandidates = candidates;
    // Provenance is a preference among fully admissible runtimes. An unhealthy,
    // busy, retired, or safely excluded origin must not strand a continuation
    // while a peer can accept the exact same immutable provider request.
    const provenance = job.continuationProvenance;
    const preferred = provenance && candidates.find(candidate => (
      candidate.projectId === provenance.projectId &&
      (candidate.incarnation ?? 0) === provenance.incarnation
    ));
    if (preferred) candidates = [preferred];
    const fleetTransferPacerCost = this.#fleetPacingCost(job);
    const balanceBulkFleet =
      job.decodedRequestBytes > FLEET_TRANSFER_EXEMPT_BYTES &&
      job.decodedRequestBytes <= FLEET_TRANSFER_PACED_MAX_BYTES &&
      candidates.length > 1 &&
      candidates.every(candidate => candidate.transportProfile === "bulk-v1");
    if (balanceBulkFleet) {
      const leastActive = Math.min(...candidates.map(candidate => (
        candidate.active + candidate.reserved
      )));
      candidates = candidates.filter(candidate => (
        candidate.active + candidate.reserved === leastActive
      ));
    }
    const scoredCandidates = candidates.map(candidate => this.#candidateForJob(job, candidate));
    const decision = chooseRuntime(job.workload, scoredCandidates, this.#random);
    if (!decision) return null;
    const runtime = this.#runtimeCache.get(decision.projectId)?.runtime;
    const candidate = candidates.find(value => value.projectId === decision.projectId);
    const selectedCandidate = scoredCandidates.find(
      value => value.projectId === decision.projectId,
    );
    const paceSelectedBulk = !!candidate && this.#requiresFleetPacing(job, candidate, available.length);
    if (paceSelectedBulk) {
      this.#refillFleetTransferPacer(now);
      if (this.#fleetTransferPacerTokens < fleetTransferPacerCost) {
        this.#armFleetTransferPacer();
        return null;
      }
    }
    return runtime && candidate && selectedCandidate
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
          fleetTransferPacerCost: paceSelectedBulk ? fleetTransferPacerCost : 0,
          selectedAvailable: Math.max(0, selectedCandidate.available ?? 0),
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
    let routeAttemptCapacityReason = selectedCapacityReason;
    let routeAttemptRemainingMs = 0;
    let routeAttemptFallbackPeerAvailable = false;
    let admissionTimer: ReturnType<typeof setTimeout> | undefined;
    let admissionTimerAtMs: number | null = null;
    let cancellationProofTimer: ReturnType<typeof setTimeout> | undefined;
    let executionRiskReflected = false;
    let runtimeRunStarted = false;
    let runtimeRun: Promise<RunHandle> | null = null;
    const runtimeRunState: {
      outcome: "pending" | "fulfilled" | "rejected";
      handle: RunHandle | null;
    } = { outcome: "pending", handle: null };
    let lateReturnedHandle: RunHandle | null = null;
    let runtimeHandleClaimed = false;
    // Medium uploads can receive their first exact data ACK after 5-8 seconds.
    // Give them the normal attempt budget, then retain proven progress under
    // the same bounded policy as larger uploads and the original hard deadline.
    const progressAwareAttempt = selectedBulkProfile &&
      job.decodedRequestBytes > PROGRESS_AWARE_MIN_REQUEST_BYTES &&
      job.decodedRequestBytes <= PROGRESS_AWARE_MAX_REQUEST_BYTES;
    // Every fleet-gated body must surface exact preparation proof so its data
    // share can be released independently of the one control unit retained
    // until request_start/commit. Timing probes remain size/profile specific.
    const observeAdmissionProgress = fleetTransferLease !== null || progressAwareAttempt;
    const progressFreshnessMs = progressAwareAttempt
      ? admissionProgressFreshnessMs(selectedRttMs)
      : 0;
    const preparedProofGrace = progressAwareAttempt
      ? Math.max(job.routeAttemptTimeoutMs, preparedProofGraceMs(selectedRttMs))
      : 0;
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
      routeAttemptRemainingMs = Math.max(0, job.queueDeadlineMs - now);
      routeAttemptFallbackPeerAvailable = this.#hasAlternativeRuntime(
        job, selectedIncarnationKey, fleetTransferLease,
      );
      try {
        const current = runtime.snapshot();
        routeAttemptCapacityReason = current.availabilityReason ??
          (isRuntimeHealthy(current) ? "available" : "lifecycle_unavailable");
      } catch {
        routeAttemptCapacityReason = "transport_unavailable";
      }
    };
    const rejectAfterCancellationProof = (
      fallback: unknown,
      code: string,
      proofDeadlineMs: number,
    ) => {
      if (!runtimeRunStarted) {
        rejectAdmissionWait(fallback);
        return;
      }
      if (cancellationProofTimer) clearTimeout(cancellationProofTimer);
      cancellationProofTimer = setTimeout(() => {
        const unproven = new V2CommittedError(
          "Runtime cancellation did not produce definitive pre-execution proof",
          code,
          true,
        );
        // Settle the router-owned race before abort listeners can synchronously
        // resume a non-cooperative runtime with a late handle in the same turn.
        // That handle is cleanup-only; it must never replace this authoritative
        // cancellation-proof failure or become eligible for replay.
        rejectAdmissionWait(unproven);
        if (!admissionController.signal.aborted) admissionController.abort(unproven);
      }, Math.max(1, Math.ceil(proofDeadlineMs - this.#clock.now())));
      cancellationProofTimer.unref?.();
    };
    const onCallerAbort = () => {
      if (admissionTimer) clearTimeout(admissionTimer);
      admissionTimer = undefined;
      admissionTimerAtMs = null;
      const error = sanitizedRouterAbortReason(job.signal);
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
      rejectAfterCancellationProof(
        error,
        "caller_cancellation_unproven",
        this.#clock.now() + ROUTE_CANCELLATION_PROOF_GRACE_MS,
      );
    };
    if (job.signal?.aborted) onCallerAbort();
    else job.signal?.addEventListener("abort", onCallerAbort, { once: true });
    let expireAdmissionAttempt!: () => void;
    const armAdmissionTimerAt = (atMs: number, phaseAdvanced = false) => {
      const boundedAtMs = Math.min(job.queueDeadlineMs, atMs);
      if (
        !phaseAdvanced && admissionTimer && admissionTimerAtMs !== null &&
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
        rejectAfterCancellationProof(
          error,
          "capacity_timeout_cancellation_unproven",
          now + ROUTE_CANCELLATION_PROOF_GRACE_MS,
        );
        return;
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
      if (
        this.#automaticRouteAttemptTimeout &&
        !this.#hasAlternativeRuntime(job, selectedIncarnationKey, fleetTransferLease)
      ) {
        // Cancelling the sole admissible copy cannot improve admission. Keep
        // its bounded ownership while checking for a usable peer; the original
        // public deadline above always wins, even if none becomes available.
        armAdmissionTimerAt(now + PRECOMMIT_RETRY_MAX_MS);
        return;
      }
      captureAttemptProgress(now);
      const error = new RouterRouteAttemptTimeoutError();
      admissionController.abort(error);
      rejectAfterCancellationProof(
        error,
        "route_timeout_cancellation_unproven",
        job.queueDeadlineMs + ROUTE_CANCELLATION_PROOF_GRACE_MS,
      );
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
      armAdmissionTimerAt(this.#clock.now() + attemptTimeoutMs);
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
          if (executionRiskReflected) return;
          executionRiskReflected = true;
          // Prefix inference retains the full-history lease independently from
          // this suffix's memory. End its admission timer at the same execution
          // fence, without releasing those bytes or caller cancellation.
          this.#completeIncomingAdmission(job);
          const cancellationAlreadyPending = admissionController.signal.aborted;
          // Once request_start is authoritative this job can never re-enter the
          // safe pre-commit queue, so its retained retry permit is no longer
          // needed even if the committed handle arrives later.
          this.#releaseQueueCapacity(job);
          if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
          if (!cancellationAlreadyPending) {
            clearAdmissionDeadline();
            // The request can no longer be replayed. Give the committed-handle
            // proof its own bounded window instead of only the few milliseconds
            // left on a nearly exhausted upload/admission deadline. Caller
            // cancellation still owns the separate 25ms cancellation fence.
            rejectAfterCancellationProof(
              new V2CommittedError(
                "Runtime reported execution risk without returning a committed handle",
                "execution_risk_handle_unproven",
                true,
              ),
              "execution_risk_handle_unproven",
              this.#clock.now() + this.#committedHandleTimeoutMs,
            );
          }
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
          if (progress.stage === "request_prepared") {
            // Exact assembly/claim proof ends body-transfer ownership for every
            // gated size. Keep one control unit until request_start/commit so a
            // prepared backlog cannot turn the shared link into an unbounded
            // zero-cost handshake queue.
            if (this.#downgradeFleetTransferToControl(fleetTransferLease)) {
              this.#scheduleDrain();
            }
          } else if (
            progressAwareAttempt &&
            progress.acknowledgedEncodedBytes === progress.totalEncodedBytes
          ) {
            // Full data ACK proves the upload completed, not that claim and
            // preparation proof crossed the return path. Give that new phase
            // one existing attempt window, capped by the public deadline.
            // Duplicate ACKs are rejected above and cannot renew this window.
            armAdmissionTimerAt(now + preparedProofGrace, true);
          }
        };
      }
      if (job.signal?.aborted) throw sanitizedRouterAbortReason(job.signal);
      if (admissionController.signal.aborted) {
        throw admissionController.signal.reason ?? new RouterCapacityTimeoutError();
      }
      if (this.#clock.now() >= job.queueDeadlineMs) {
        admissionDeadlineExpired = true;
        const error = new RouterCapacityTimeoutError();
        admissionController.abort(error);
        throw error;
      }
      runtimeRunStarted = true;
      runtimeRun = Promise.resolve(runtime.run(
        job.body,
        admissionController.signal,
        runOptions,
      )).then(handle => {
        runtimeRunState.outcome = "fulfilled";
        runtimeRunState.handle = handle;
        return handle;
      }, error => {
        runtimeRunState.outcome = "rejected";
        throw error;
      });
      // Promise.race installs terminal handlers on runtimeRun. If a broken test
      // seam ignores cancellation, its late rejection remains consumed. V2's
      // abort path synchronously proves pre-execution safety or committed risk
      // before the short cancellation-proof fence permits a retry.
      const handle = await Promise.race([runtimeRun, admissionWait]);
      // Retain cleanup ownership until every synchronous wrapper, accessor, and
      // terminal observer has been installed. A malformed committed handle or
      // observability failure must not strand its buffers or terminal permit.
      lateReturnedHandle = handle;
      if (
        admissionController.signal.aborted ||
        (!executionRiskReflected && this.#clock.now() >= job.queueDeadlineMs)
      ) {
        throw new V2CommittedError(
          "Runtime returned a handle after its admission deadline or cancellation",
          "late_handle_after_cancellation",
          true,
        );
      }
      // A committed handle can no longer need safe pre-commit replay. Only now
      // may a new ingress request consume this request's bounded queue permit.
      this.#releaseQueueCapacity(job);
      if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
      this.#completeIncomingAdmission(job);
      // Commitment also ends any remaining attempt deadline immediately.
      // Optional memory accounting work must never turn an accepted upstream
      // request into a local timeout or inflate measured commit ACK latency.
      clearAdmissionDeadline();
      const committedAtMs = this.#clock.now();
      const commitAckMs = Math.max(0, committedAtMs - started);
      // A definitive initial model rejection may select Terra once. Retain the
      // exact immutable request under its original lease until that decision;
      // any response event ends eligibility without retaining a transcript.
      let fallbackBody = canFallbackFromModel(job.body.model)
        ? job.body
        : null;
      if (!fallbackBody) await this.#releaseCommittedRequestBody(job);
      // The admission deadline ends at commit, but caller cancellation must
      // continue flowing through the same signal for the request's lifetime.
      this.#refreshRuntimeSafely(runtime);
      this.#releaseRuntimeReservation(job, runtime.projectId);
      job.signal?.removeEventListener("abort", job.onAbort!);
      const handleStats = handle.stats;
      const handleDiagnostics = handleStats.diagnostics;
      this.#emit("transport.committed", {
        trace_id: job.traceId,
        project_id: runtime.projectId,
        transport: this.#transport,
        queue_wait_ms: job.queueWaitMs,
        commit_ack_ms: commitAckMs,
        retry_count: job.attempts,
        ...(handleDiagnostics ? {
          host_prepare_ms: handleDiagnostics.hostPrepareMs,
          request_decoded_bytes: handleDiagnostics.requestDecodedBytes,
          request_encoded_bytes: handleDiagnostics.requestEncodedBytes,
          request_part_count: handleDiagnostics.requestPartCount,
          request_encoding: handleDiagnostics.requestEncoding,
          request_compression_ratio:
            handleDiagnostics.requestDecodedBytes &&
              handleDiagnostics.requestEncodedBytes !== null
              ? handleDiagnostics.requestEncodedBytes /
                handleDiagnostics.requestDecodedBytes
              : null,
          host_scheduler_wait_ms: handleDiagnostics.hostSchedulerWaitMs,
          host_publish_to_ack_ms: handleDiagnostics.hostPublishToAckMs,
          sidecar_first_frame_to_commit_ms: handleDiagnostics.sidecarFirstFrameToCommitMs,
          sidecar_commit_to_assembly_ms: handleDiagnostics.sidecarCommitToAssemblyMs,
          sidecar_claim_sync_ms: handleDiagnostics.sidecarClaimSyncMs,
          sidecar_admission_wait_ms: handleDiagnostics.sidecarAdmissionWaitMs,
          sidecar_prepared_to_fetch_start_ms:
            handleDiagnostics.sidecarPreparedToFetchStartMs,
          sidecar_fetch_start_sync_ms: handleDiagnostics.sidecarFetchStartSyncMs,
          sidecar_commit_processing_ms: handleDiagnostics.sidecarCommitProcessingMs,
          transport_commit_residual_ms: handleDiagnostics.transportCommitResidualMs,
        } : {}),
      });
      this.#traffic.committed(job.queueWaitMs, commitAckMs);
      const provenanceReservation = job.provenanceReservation;
      const provenanceCommitted = job.authorizationScope && provenanceReservation
        ? this.#withCommittedProvenance(handle, provenanceReservation, {
            projectId: runtime.projectId,
            incarnation: selectedIncarnation,
          }, job.authorizationScope)
        : handle;
      if (job.authorizationScope && provenanceReservation) {
        // The final wrapper now owns the reservation on every settlement path.
        job.provenanceReservation = null;
      }
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
          callerSignal: job.signal,
        })
        : provenanceCommitted;
      void provenanceCommitted.final.then(
        final => {
          const hint = rateLimitHintFromError(final, handle.rateLimit);
          this.#traffic.settled({
            totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs),
            firstByteMs: handleStats.firstByteMs,
            responseBytes: handleStats.bytes,
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
            firstByteMs: handleStats.firstByteMs,
            responseBytes: handleStats.bytes,
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
      let publicHandle = traced;
      let fallbackDecision = Promise.resolve();
      if (fallbackBody && typeof requestModel === "string") {
        let releaseRetainedBody!: () => void;
        const bodyRetainedUntil = new Promise<void>(resolve => { releaseRetainedBody = resolve; });
        const fallback = wrapModelFallbackHandle(traced, {
          requestedModel: requestModel,
          signal: job.signal,
          startFallback: error => {
            if (!fallbackBody || this.#closed || this.#shuttingDown) throw error;
            return this.#startModelFallback(job, fallbackBody, requestModel, error, bodyRetainedUntil);
          },
        });
        publicHandle = fallback.handle;
        fallbackDecision = fallback.decision.catch(() => undefined).then(async () => {
          fallbackBody = null;
          job.body = {};
          releaseRetainedBody();
          await this.#releaseCommittedRequestBody(job);
        });
      }
      const terminalCleanup = traced.final.then(
        () => undefined,
        () => undefined,
      ).finally(async () => {
        // Fallback snapshotting must finish before the original body lease ends.
        await fallbackDecision;
        job.signal?.removeEventListener("abort", onCallerAbort);
        this.#releaseJobMemory(job);
        this.#refreshRuntimeSafely(runtime);
        void started;
        this.#scheduleDrain();
      });
      void terminalCleanup.catch(() => undefined);
      this.#deferredMemoryReleases.add(terminalCleanup);
      void terminalCleanup.finally(() => {
        this.#deferredMemoryReleases.delete(terminalCleanup);
      }).catch(() => undefined);
      const selectedHandle = publicHandle;
      const publicFinal = selectedHandle.final.then(
        async value => { await terminalCleanup; return value; },
        async error => { await terminalCleanup; throw error; },
      );
      void publicFinal.catch(() => undefined);
      publicHandle = {
        events: () => selectedHandle.events(),
        discardUnconsumedEvents: () => selectedHandle.discardUnconsumedEvents?.(),
        final: publicFinal,
        get modelFallback() { return selectedHandle.modelFallback; },
        get rateLimit() { return selectedHandle.rateLimit; },
        get upstreamPrelude() { return selectedHandle.upstreamPrelude; },
        stats: selectedHandle.stats,
      };
      // All potentially throwing adoption work is complete. Only now may the
      // committed handle become caller-owned and exempt from late cleanup.
      runtimeHandleClaimed = true;
      lateReturnedHandle = null;
      job.resolve(publicHandle);
      this.#scheduleDrain();
    } catch (error) {
      let runtimeReservationDeferred = false;
      let fleetTransferReleaseDeferred = false;
      const releaseFleetAfterAttempt = () => {
        if (this.#releaseFleetTransfer(fleetTransferLease)) this.#scheduleDrain();
      };
      if (runtimeRun && !runtimeHandleClaimed && runtimeRunState.outcome !== "rejected") {
        // If the bounded cancellation outcome wins, a broken/slow runtime can
        // still return a committed handle later. Drain its local buffers and
        // terminal permit; never expose or retry it as a second execution.
        const returnedHandle = lateReturnedHandle ?? runtimeRunState.handle;
        let lateCleanup: Promise<void>;
        if (returnedHandle) {
          // A returned handle proves body-transfer settlement even though it is
          // too late to expose. Its response buffers still require cleanup.
          releaseFleetAfterAttempt();
          lateCleanup = releaseLateRunHandle(returnedHandle);
        } else {
          // Cancellation without a settled runtime promise proves neither that
          // scheduled bytes stopped nor that the transfer lease is reusable.
          // Hold the fleet weight until the runtime itself settles; a permanent
          // ambiguity intentionally quarantines this capacity.
          fleetTransferReleaseDeferred = true;
          lateCleanup = runtimeRun.then(
            handle => {
              releaseFleetAfterAttempt();
              return releaseLateRunHandle(handle);
            },
            () => { releaseFleetAfterAttempt(); },
          );
        }
        this.#lateAttemptCleanups.add(lateCleanup);
        runtimeReservationDeferred = true;
        const retained = job.retainMemoryUntil;
        job.retainMemoryUntil = retained
          ? Promise.allSettled([retained, lateCleanup])
          : lateCleanup;
        void lateCleanup.finally(() => {
          this.#lateAttemptCleanups.delete(lateCleanup);
          this.#releaseRuntimeReservation(job, runtime.projectId);
          this.#refreshRuntimeSafely(runtime);
          this.#scheduleDrain();
        });
      }
      const transportSettlement = error instanceof V2PreCommitError
        ? error.transportSettlement
        : null;
      if (
        !fleetTransferReleaseDeferred && transportSettlement &&
        fleetTransferLease && !fleetTransferLease.released
      ) {
        // Safe pre-execution cancellation authorizes a retry, but it does not
        // mean the immutable reliable body and its ordered cancellation have
        // left the old transport. Keep their fleet weight until the exact ACK
        // or a transport reset proves that flight no longer consumes the link.
        fleetTransferReleaseDeferred = true;
        const transferCleanup = transportSettlement.then(
          () => { releaseFleetAfterAttempt(); },
          () => { releaseFleetAfterAttempt(); },
        );
        this.#lateAttemptCleanups.add(transferCleanup);
        void transferCleanup.finally(() => {
          this.#lateAttemptCleanups.delete(transferCleanup);
        }).catch(() => undefined);
      }
      detachAdmission();
      if (!runtimeReservationDeferred) this.#releaseRuntimeReservation(job, runtime.projectId);
      if (!fleetTransferReleaseDeferred) releaseFleetAfterAttempt();
      if (
        runtimeRunState.outcome === "rejected" && !runtimeHandleClaimed &&
        !admissionController.signal.aborted && !job.signal?.aborted &&
        !this.#closed && !this.#shuttingDown &&
        typeof requestModel === "string" &&
        isDefinitiveModelNotFound(error, requestModel)
      ) {
        // A durable provider rejection can arrive before its committed ACK.
        // No handle or response event escaped this still-pending startRelay.
        this.#completeIncomingAdmission(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "model-rejection", error, job.attempts);
        let releaseRetainedBody!: () => void;
        const bodyRetainedUntil = new Promise<void>(resolve => { releaseRetainedBody = resolve; });
        try {
          job.resolve(await this.#startModelFallback(job, job.body, requestModel, error, bodyRetainedUntil));
        } catch (fallbackError) {
          job.reject(fallbackError);
        } finally {
          job.body = {};
          releaseRetainedBody();
          this.#releaseJobMemory(job);
          this.#refreshRuntimeSafely(runtime);
          this.#scheduleDrain();
        }
        return;
      }
      if (
        executionRiskReflected &&
        !(error && typeof error === "object" &&
          (error as { committed?: unknown }).committed === true)
      ) {
        error = new V2CommittedError(
          "Runtime failed after reporting irreversible execution risk",
          "execution_risk_reflected",
          true,
        );
      }
      const cancellationCommitment = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      if (
        runtimeRunStarted &&
        admissionController.signal.aborted &&
        cancellationCommitment.committed !== true &&
        cancellationCommitment.ambiguous !== true &&
        !isSafePreCommitFailure(error)
      ) {
        error = new V2CommittedError(
          "Runtime cancellation ended without definitive pre-execution proof",
          routeAttemptExpired
            ? "timeout_cancellation_unproven"
            : "caller_cancellation_unproven",
          true,
        );
      }
      const rawCommitment = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      if (
        routeAttemptExpired &&
        rawCommitment.committed !== true &&
        rawCommitment.ambiguous !== true &&
        !isSafePreCommitFailure(error)
      ) {
        error = new V2CommittedError(
          "Runtime timeout ended without definitive pre-execution proof",
          "timeout_cancellation_unproven",
          true,
        );
      }
      const commitment = error && typeof error === "object"
        ? error as { committed?: unknown; ambiguous?: unknown }
        : {};
      const committed = executionRiskReflected || commitment.committed === true;
      const ambiguous = executionRiskReflected || commitment.ambiguous === true;
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
          progress_observer_enabled: observeAdmissionProgress,
          progress_age_ms: routeAttemptProgressAgeMs,
          progress_stage: finalAdmissionProgress?.stage ?? null,
          progress_acknowledged_encoded_bytes:
            finalAdmissionProgress?.acknowledgedEncodedBytes ?? null,
          progress_total_encoded_bytes: finalAdmissionProgress?.totalEncodedBytes ?? null,
          deadline_exhausted: admissionDeadlineExpired,
          remaining_admission_ms: routeAttemptRemainingMs,
          fallback_peer_available: routeAttemptFallbackPeerAvailable,
          capacity_reason: routeAttemptCapacityReason,
          transfer_admission_cost: job.admissionCost,
        });
      }
      if (
        admissionDeadlineExpired &&
        !isDeterministicRequestTooLarge(error) &&
        !committed &&
        !ambiguous
      ) {
        this.#expireJob(job);
        this.#scheduleDrain();
        return;
      }
      if (job.signal?.aborted && isSafePreCommitFailure(error)) {
        // The runtime proof controls replay safety, but the public cancellation
        // reason still owns the caller-visible outcome once that proof arrives.
        error = sanitizedRouterAbortReason(job.signal);
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
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
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
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
        this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
        this.#failure(job.traceId, job.acceptedAtMs, "host-transform", error, job.attempts);
        job.reject(error);
        this.#scheduleDrain();
        return;
      }
      const retryablePreCommit = !committed && !ambiguous &&
        isRetryablePreCommitFailure(error);
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
        // This attempt proved no provider execution began. Return its terminal
        // metadata reservation while retaining the job's bounded queue permit;
        // the next eligible attempt will reserve again immediately pre-run.
        this.#releaseJobProvenance(job);
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
          this.#releaseJobMemory(job);
          this.#releaseJobProvenance(job);
          this.#releaseQueueCapacity(job);
          const queueError = new RouterQueueFullError();
          this.#traffic.rejected({ totalMs: Math.max(0, this.#clock.now() - job.acceptedAtMs) });
          this.#failure(job.traceId, job.acceptedAtMs, "queue", queueError, job.attempts);
          job.reject(queueError);
        } else {
          this.#armRetryWake(job);
        }
      } else {
        job.signal?.removeEventListener("abort", job.onAbort!);
        this.#releaseJobMemory(job);
        this.#releaseJobProvenance(job);
        this.#releaseQueueCapacity(job);
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
    let sourceFinal: Promise<any>;
    try {
      sourceFinal = Promise.resolve(handle.final);
    } catch (error) {
      this.#responseProvenance.release(reservation);
      this.#scheduleDrain();
      throw error;
    }
    const final = sourceFinal.then(
      value => {
        try {
          if (value?.status !== "completed") {
            this.#responseProvenance.release(reservation);
            this.#scheduleDrain();
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
          // commit() consumes the reservation on both success and conflict. Wake
          // metadata-gated queueing only after that capacity is actually free.
          this.#scheduleDrain();
          if (!recorded) {
            releaseAtomicBytePermit(value);
            throw new ResponseProvenanceCommitError();
          }
          return value;
        } catch (error) {
          // Accessors on a malformed trusted final must not strand the global
          // pre-execution reservation. release() is idempotent after commit().
          this.#responseProvenance.release(reservation);
          this.#scheduleDrain();
          throw error;
        }
      },
      error => {
        this.#responseProvenance.release(reservation);
        this.#scheduleDrain();
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
      discardUnconsumedEvents: () => handle.discardUnconsumedEvents?.(),
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
    this.#emit("request.failed", {
      trace_id: traceId,
      project_id: null,
      total_ms: Math.max(0, this.#clock.now() - acceptedAtMs),
      stage,
      ...requestFailureFields(error),
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
