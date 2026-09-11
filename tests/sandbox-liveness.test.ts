import { describe, expect, it } from "bun:test";
import {
  hasFreshExactSidecarLiveness,
  hasFreshSandboxLiveness,
  isFreshSandboxLivenessAge,
  isActiveSandboxLivenessRecovery,
  SANDBOX_LIVENESS_RECOVERY_MAX_MS,
  SANDBOX_LIVENESS_MAX_AGE_MS,
} from "../src/sandbox/liveness";

describe("sandbox liveness", () => {
  it("accepts only a finite bounded recovery episode before its exact deadline", () => {
    const recovery = { startedAtMs: 1_000, deadlineAtMs: 1_000 + SANDBOX_LIVENESS_RECOVERY_MAX_MS };
    expect(isActiveSandboxLivenessRecovery(recovery, 1_000)).toBe(true);
    expect(isActiveSandboxLivenessRecovery(recovery, recovery.deadlineAtMs - 1)).toBe(true);
    expect(isActiveSandboxLivenessRecovery(recovery, recovery.deadlineAtMs)).toBe(false);
    expect(isActiveSandboxLivenessRecovery(recovery, 999)).toBe(false);
    for (const invalid of [
      { ...recovery, startedAtMs: -1 },
      { ...recovery, startedAtMs: Number.NaN },
      { ...recovery, deadlineAtMs: Number.POSITIVE_INFINITY },
      { ...recovery, deadlineAtMs: recovery.deadlineAtMs + 1 },
      { ...recovery, deadlineAtMs: recovery.startedAtMs },
    ]) expect(isActiveSandboxLivenessRecovery(invalid, 1_000)).toBe(false);
    expect(isActiveSandboxLivenessRecovery(recovery, Number.NaN)).toBe(false);
    expect(isActiveSandboxLivenessRecovery(null, 1_000)).toBe(false);
  });

  it("accepts only finite non-negative ages inside the exact freshness bound", () => {
    expect(isFreshSandboxLivenessAge(0)).toBe(true);
    expect(isFreshSandboxLivenessAge(SANDBOX_LIVENESS_MAX_AGE_MS)).toBe(true);
    expect(isFreshSandboxLivenessAge(SANDBOX_LIVENESS_MAX_AGE_MS + 1)).toBe(false);
    expect(isFreshSandboxLivenessAge(-1)).toBe(false);
    expect(isFreshSandboxLivenessAge(Number.POSITIVE_INFINITY)).toBe(false);
    expect(isFreshSandboxLivenessAge(Number.NaN)).toBe(false);
    expect(isFreshSandboxLivenessAge(null)).toBe(false);
  });

  it("requires a connected transport before cached exact-sidecar age is trusted", () => {
    expect(hasFreshExactSidecarLiveness({
      sidecarPeerAgeMs: 1,
      transportConnected: true,
    })).toBe(true);
    expect(hasFreshExactSidecarLiveness({
      sidecarPeerAgeMs: 1,
      transportConnected: false,
    })).toBe(false);
    expect(hasFreshExactSidecarLiveness({
      sidecarPeerAgeMs: SANDBOX_LIVENESS_MAX_AGE_MS + 1,
      transportConnected: true,
    })).toBe(false);
  });

  it("accepts either independent channel but fails when both are stale", () => {
    expect(hasFreshSandboxLiveness({
      heartbeatAgeMs: 1,
      sidecarPeerAgeMs: null,
      transportConnected: false,
    })).toBe(true);
    expect(hasFreshSandboxLiveness({
      heartbeatAgeMs: 60_000,
      sidecarPeerAgeMs: 1,
      transportConnected: true,
    })).toBe(true);
    expect(hasFreshSandboxLiveness({
      heartbeatAgeMs: 60_000,
      sidecarPeerAgeMs: 60_000,
      transportConnected: true,
    })).toBe(false);
  });
});
