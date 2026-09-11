export const SANDBOX_LIVENESS_MAX_AGE_MS = 20_000;
export const SANDBOX_LIVENESS_RECOVERY_MAX_MS = 150_000;

/** One runtime-owned episode; failed retries must never extend its deadline. */
export interface SandboxLivenessRecovery {
  readonly startedAtMs: number;
  readonly deadlineAtMs: number;
}

export function isActiveSandboxLivenessRecovery(
  recovery: SandboxLivenessRecovery | null | undefined,
  now: number,
): boolean {
  if (!recovery || !Number.isFinite(now)) return false;
  const { startedAtMs, deadlineAtMs } = recovery;
  return Number.isFinite(startedAtMs) && Number.isFinite(deadlineAtMs) &&
    startedAtMs >= 0 && startedAtMs <= now && deadlineAtMs > now &&
    deadlineAtMs - startedAtMs <= SANDBOX_LIVENESS_RECOVERY_MAX_MS;
}

export interface SandboxLivenessSignals {
  readonly heartbeatAgeMs: number | null;
  readonly sidecarPeerAgeMs?: number | null;
  /** Real V2 snapshots set this explicitly; omitted legacy/fake snapshots remain compatible. */
  readonly transportConnected?: boolean;
}

export function isFreshSandboxLivenessAge(
  value: number | null | undefined,
  maximumAgeMs = SANDBOX_LIVENESS_MAX_AGE_MS,
): boolean {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 &&
    value <= maximumAgeMs;
}

/**
 * The age is advanced only by an envelope from the exact bootstrap-bound
 * sidecar instance. A known-disconnected real transport cannot use its cached
 * age as live proof; optional transportConnected preserves older test doubles.
 */
export function hasFreshExactSidecarLiveness(
  value: Pick<SandboxLivenessSignals, "sidecarPeerAgeMs" | "transportConnected">,
  maximumAgeMs = SANDBOX_LIVENESS_MAX_AGE_MS,
): boolean {
  return value.transportConnected !== false &&
    isFreshSandboxLivenessAge(value.sidecarPeerAgeMs, maximumAgeMs);
}

export function hasFreshSandboxLiveness(
  value: SandboxLivenessSignals,
  maximumAgeMs = SANDBOX_LIVENESS_MAX_AGE_MS,
): boolean {
  return isFreshSandboxLivenessAge(value.heartbeatAgeMs, maximumAgeMs) ||
    hasFreshExactSidecarLiveness(value, maximumAgeMs);
}
