export const DEFAULT_V2_INITIAL_RETRANSMIT_MS = 1_500;

export function computeV2RetransmitDelayMs(
  awarenessRttMs: number | null,
  jitterSeed: number,
  minimumMs = 250,
  retransmissionAttempt = 0,
  initialRtoMs = DEFAULT_V2_INITIAL_RETRANSMIT_MS,
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
  const rtt = awarenessRttMs === null || !Number.isFinite(awarenessRttMs)
    ? 0
    : Math.max(0, awarenessRttMs);
  const base = awarenessRttMs === null || !Number.isFinite(awarenessRttMs)
    ? initialRtoMs
    : Math.max(minimumMs, 2 * rtt);
  const backedOff = Math.min(
    30_000,
    base * (2 ** Math.min(16, retransmissionAttempt)),
  );
  const normalizedSeed = Math.max(0, Math.min(1, jitterSeed));
  const jitterCeiling = Math.min(100, backedOff * 0.1);
  return Math.min(30_000, Math.ceil(backedOff + jitterCeiling * normalizedSeed));
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
