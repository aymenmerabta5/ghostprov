export function computeV2RetransmitDelayMs(
  awarenessRttMs: number | null,
  jitterSeed: number,
  minimumMs = 250,
  retransmissionAttempt = 0,
): number {
  if (!Number.isFinite(minimumMs) || minimumMs < 1) {
    throw new RangeError("minimumMs must be positive");
  }
  if (!Number.isSafeInteger(retransmissionAttempt) || retransmissionAttempt < 0) {
    throw new RangeError("retransmissionAttempt must be a non-negative safe integer");
  }
  const rtt = awarenessRttMs === null || !Number.isFinite(awarenessRttMs)
    ? 0
    : Math.max(0, awarenessRttMs);
  const base = Math.max(minimumMs, 2 * rtt);
  const backedOff = Math.min(
    30_000,
    base * (2 ** Math.min(16, retransmissionAttempt)),
  );
  const normalizedSeed = Math.max(0, Math.min(1, jitterSeed));
  const jitterCeiling = Math.min(100, backedOff * 0.1);
  return Math.min(30_000, Math.ceil(backedOff + jitterCeiling * normalizedSeed));
}
