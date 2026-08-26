import type { RandomSource } from "../core/ports";
import type { ProjectId } from "../sandbox/types";
import type { WorkloadDescriptor } from "./workload";

export interface RouteCandidate {
  projectId: ProjectId;
  /** Verified wire revision; ready production V2 runtimes always provide it. */
  protocolRevision?: "r1" | "r2" | null;
  incarnation?: number;
  state: string;
  capacity: number;
  active: number;
  /**
   * Runtime-advertised admission credits after transport, sidecar, and local
   * safety constraints. Unlike `capacity - active`, this is authoritative for
   * V2 and may fall independently while a transport is congested.
   */
  available?: number;
  /** Host reservations not yet reflected in runtime admission credits. */
  reserved: number;
  /** Weighted large-body transfer reservations not yet reflected by V2. */
  reservedTransferCost?: number;
  quarantined: number;
  queueDepth: number;
  ackEwmaMs: number;
  completionEwmaMs: number;
  recentFailureRate: number;
  heartbeatAgeMs: number | null;
  credentialExpiresInMs: number | null;
  memoryPressure: number;
  /** Requests accepted by the transport but not yet commit-acknowledged. */
  commitBacklog?: number;
  /** Encoded frames waiting behind the current Awareness flight. */
  schedulerBytes?: number;
  /** Age of the oldest frame waiting in the transport scheduler. */
  schedulerOldestMs?: number;
  /** Measured host-to-sidecar publication throughput for this runtime. */
  throughputBytesPerSecond?: number | null;
}

const KiB = 1024;
const UNKNOWN_THROUGHPUT_BYTES_PER_ACK: Readonly<Record<"r1" | "r2", number>> =
  Object.freeze({
    // R1's 128 KiB encoded flight fits roughly ten 8 KiB request parts.
    r1: 80 * KiB,
    // R2 carries exactly thirty-two 11 KiB request parts per full flight.
    r2: 32 * 11 * KiB,
  });
// A fresh/recovered remote sandbox must not look faster than every measured
// peer merely because it has no RTT sample yet. This matches the conservative
// upper-middle of observed production Awareness RTTs and lasts only until the
// first clean ACK.
const UNKNOWN_RTT_FLOOR_MS = 750;

export interface RouteDecision {
  projectId: ProjectId;
  predictedCompletionMs: number;
}

export type RuntimeHealthCandidate = Pick<
  RouteCandidate,
  "state" | "heartbeatAgeMs" | "credentialExpiresInMs"
>;

/**
 * Reports whether a runtime is alive and safe to count toward fleet capacity.
 * This deliberately does not consider load: a full runtime is still healthy,
 * even though it cannot accept another request immediately.
 */
export function isRuntimeHealthy(candidate: RuntimeHealthCandidate): boolean {
  if (candidate.state !== "ready") return false;
  if (candidate.heartbeatAgeMs === null || candidate.heartbeatAgeMs > 20_000) return false;
  if (candidate.credentialExpiresInMs !== null && candidate.credentialExpiresInMs < 30_000) return false;
  return true;
}

/**
 * Reports whether a runtime can accept one more reservation right now.
 * Keep this predicate shared with observability so `eligible_candidates`
 * never counts stale, draining, credential-expiring, quarantined-full, or
 * otherwise capacity-exhausted runtimes.
 */
export function isRuntimeEligible(candidate: RouteCandidate): boolean {
  if (!isRuntimeHealthy(candidate)) return false;
  const usable = candidate.capacity - candidate.quarantined;
  const advertised = Number.isFinite(candidate.available)
    ? Math.max(0, candidate.available!)
    : Math.max(0, usable - candidate.active);
  return advertised > candidate.reserved;
}

function score(workload: WorkloadDescriptor, candidate: RouteCandidate): number {
  const usable = Math.max(1, candidate.capacity - candidate.quarantined);
  const advertised = Number.isFinite(candidate.available)
    ? Math.max(0, candidate.available!)
    : Math.max(0, usable - candidate.active);
  const effectiveCredits = Math.max(0, advertised - candidate.reserved);
  const commitBacklog = Math.max(0, candidate.commitBacklog ?? candidate.queueDepth);
  // Admission credits are a small transport window (currently often four),
  // not the inverse of active work. Score true active streams plus only the
  // host reservations which have not yet appeared in the transport snapshot.
  const effectiveWork = candidate.active + candidate.reserved + 1;
  const loadDelay = (effectiveWork / usable) * candidate.completionEwmaMs;
  const queuedWaves = candidate.queueDepth / usable;
  const throughput = candidate.throughputBytesPerSecond ?? 0;
  const measuredRevision = candidate.protocolRevision === "r1" ||
      candidate.protocolRevision === "r2"
    ? candidate.protocolRevision
    : null;
  const effectiveAckMs = measuredRevision && candidate.ackEwmaMs <= 0
    ? UNKNOWN_RTT_FLOOR_MS
    : Math.max(0, candidate.ackEwmaMs);
  const fallbackFlightBytes = measuredRevision
    ? UNKNOWN_THROUGHPUT_BYTES_PER_ACK[measuredRevision]
    : 2 * 1024 ** 2;
  const payloadPenalty = throughput > 0
    ? Math.min(candidate.completionEwmaMs, workload.requestBytes / throughput * 1_000)
    : Math.min(
        candidate.completionEwmaMs,
        (workload.requestBytes / fallbackFlightBytes) * Math.max(25, effectiveAckMs),
      );
  const commitPenalty = commitBacklog * Math.max(1, effectiveAckMs) /
    Math.max(1, effectiveCredits);
  const schedulerPenalty = Math.max(0, candidate.schedulerOldestMs ?? 0) +
    (throughput > 0
      ? Math.max(0, candidate.schedulerBytes ?? 0) / throughput * 1_000
      : 0);
  const failurePenalty = Math.max(0, candidate.recentFailureRate) * candidate.completionEwmaMs * 5;
  const memoryPenalty = Math.max(0, candidate.memoryPressure) * candidate.completionEwmaMs;
  return effectiveAckMs + loadDelay + queuedWaves * candidate.completionEwmaMs +
    payloadPenalty + commitPenalty + schedulerPenalty + failurePenalty + memoryPenalty;
}

export function chooseRuntime(
  workload: WorkloadDescriptor,
  candidates: readonly RouteCandidate[],
  random: RandomSource,
): RouteDecision | null {
  let best = Number.POSITIVE_INFINITY;
  const tied: RouteCandidate[] = [];
  for (const candidate of candidates) {
    if (!isRuntimeEligible(candidate)) continue;
    const predicted = score(workload, candidate);
    if (predicted < best - 0.0001) {
      best = predicted;
      tied.length = 0;
      tied.push(candidate);
    } else if (Math.abs(predicted - best) <= 0.0001) {
      tied.push(candidate);
    }
  }
  if (!tied.length) return null;
  const selected = tied[Math.min(tied.length - 1, Math.floor(random.next() * tied.length))];
  return { projectId: selected.projectId, predictedCompletionMs: best };
}
