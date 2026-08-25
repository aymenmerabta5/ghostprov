export const DEFAULT_V2_INITIAL_RETRANSMIT_MS = 1_500;
/**
 * Awareness flights are bounded cumulative state. Keeping an active flight at
 * a five-second ceiling avoids the 1.5 -> 3 -> 6 -> 12 second tail observed
 * when an ACK is lost, without creating an unbounded retransmission stream.
 */
export const DEFAULT_V2_MAX_RETRANSMIT_MS = 5_000;

export function computeV2RetransmitDelayMs(
  awarenessRttMs: number | null,
  jitterSeed: number,
  minimumMs = 250,
  retransmissionAttempt = 0,
  initialRtoMs = DEFAULT_V2_INITIAL_RETRANSMIT_MS,
  maximumMs = Math.max(DEFAULT_V2_MAX_RETRANSMIT_MS, initialRtoMs),
  rttVariationMs?: number | null,
): number {
  if (!Number.isFinite(minimumMs) || minimumMs < 1) {
    throw new RangeError("minimumMs must be positive");
  }
  if (!Number.isSafeInteger(retransmissionAttempt) || retransmissionAttempt < 0) {
    throw new RangeError("retransmissionAttempt must be a non-negative safe integer");
  }
  if (!Number.isFinite(initialRtoMs) || initialRtoMs < minimumMs) {
    throw new RangeError("initialRtoMs must be finite and no less than minimumMs");
  }
  if (!Number.isFinite(maximumMs) || maximumMs < initialRtoMs) {
    throw new RangeError("maximumMs must be finite and no less than initialRtoMs");
  }
  if (
    rttVariationMs !== undefined && rttVariationMs !== null &&
    (!Number.isFinite(rttVariationMs) || rttVariationMs < 0)
  ) throw new RangeError("rttVariationMs must be finite and non-negative");
  const rtt = awarenessRttMs === null || !Number.isFinite(awarenessRttMs)
    ? 0
    : Math.max(0, awarenessRttMs);
  const base = awarenessRttMs === null || !Number.isFinite(awarenessRttMs)
    ? initialRtoMs
    : Math.max(
        minimumMs,
        rttVariationMs === undefined || rttVariationMs === null
          ? 2 * rtt
          : rtt + 4 * rttVariationMs,
      );
  const backedOff = Math.min(
    maximumMs,
    base * (2 ** Math.min(16, retransmissionAttempt)),
  );
  const normalizedSeed = Math.max(0, Math.min(1, jitterSeed));
  const jitterCeiling = Math.min(100, backedOff * 0.1);
  return Math.min(maximumMs, Math.ceil(backedOff + jitterCeiling * normalizedSeed));
}

export function deterministicV2JitterSeed(salt: string, revision: number): number {
  if (!Number.isSafeInteger(revision) || revision < 0) {
    throw new RangeError("revision must be a non-negative safe integer");
  }
  let hash = 2_166_136_261;
  const value = `${salt}:${revision}`;
  for (let index = 0; index < value.length; index++) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  return (hash >>> 0) / 0xffff_ffff;
}
