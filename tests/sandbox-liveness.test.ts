import { describe, expect, it } from "bun:test";
import {
  hasFreshExactSidecarLiveness,
  hasFreshSandboxLiveness,
  isFreshSandboxLivenessAge,
  SANDBOX_LIVENESS_MAX_AGE_MS,
} from "../src/sandbox/liveness";

describe("sandbox liveness", () => {
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
