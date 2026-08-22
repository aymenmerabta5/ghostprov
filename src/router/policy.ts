import type { RandomSource } from "../core/ports";
import type { ProjectId } from "../sandbox/types";
import type { WorkloadDescriptor } from "./workload";

export interface RouteCandidate {
  projectId: ProjectId;
  state: string;
  capacity: number;
  active: number;
  reserved: number;
  quarantined: number;
  queueDepth: number;
  ackEwmaMs: number;
  completionEwmaMs: number;
  recentFailureRate: number;
  heartbeatAgeMs: number | null;
  credentialExpiresInMs: number | null;
  memoryPressure: number;
}

export interface RouteDecision {
  projectId: ProjectId;
  predictedCompletionMs: number;
}

function eligible(candidate: RouteCandidate): boolean {
  if (candidate.state !== "ready") return false;
  if (candidate.heartbeatAgeMs === null || candidate.heartbeatAgeMs > 20_000) return false;
  if (candidate.credentialExpiresInMs !== null && candidate.credentialExpiresInMs < 30_000) return false;
  const usable = candidate.capacity - candidate.quarantined;
  return usable > candidate.active + candidate.reserved;
}

function score(workload: WorkloadDescriptor, candidate: RouteCandidate): number {
  const usable = Math.max(1, candidate.capacity - candidate.quarantined);
  const work = candidate.active + candidate.reserved + 1;
  const loadDelay = (work / usable) * candidate.completionEwmaMs;
  const queuedWaves = candidate.queueDepth / usable;
  const payloadPenalty = Math.min(
    candidate.completionEwmaMs,
    (workload.requestBytes / (2 * 1024 ** 2)) * 100,
  );
  const failurePenalty = Math.max(0, candidate.recentFailureRate) * candidate.completionEwmaMs * 5;
  const memoryPenalty = Math.max(0, candidate.memoryPressure) * candidate.completionEwmaMs;
  return candidate.ackEwmaMs + loadDelay + queuedWaves * candidate.completionEwmaMs +
    payloadPenalty + failurePenalty + memoryPenalty;
}

export function chooseRuntime(
  workload: WorkloadDescriptor,
  candidates: readonly RouteCandidate[],
  random: RandomSource,
): RouteDecision | null {
  let best = Number.POSITIVE_INFINITY;
  const tied: RouteCandidate[] = [];
  for (const candidate of candidates) {
    if (!eligible(candidate)) continue;
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
