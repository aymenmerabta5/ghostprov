import { describe, expect, it } from "bun:test";
import { resolveR9RecoveryProofOptions, r9RuntimeIsReady } from "../scripts/live_r9_supervisor_recovery";

const enabled = {
  GP_LIVE_R9_SUPERVISOR_RECOVERY: "1",
  GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
  PRISM_COOKIE_FILE: "unused-offline-cookie-path.json",
};

describe("R9 supervisor live recovery guards", () => {
  it("requires a separate explicit guard before selecting R10", () => {
    expect(resolveR9RecoveryProofOptions(enabled).sidecarRevision).toBe("r9");
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_SIDECAR_REVISION: "r10" })).toThrow();
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_SIDECAR_REVISION: "r10", GP_LIVE_R10_DIAGNOSTICS: "1" }).sidecarRevision).toBe("r10");
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_SIDECAR_REVISION: "r11" })).toThrow();
  });
  it("requires explicit bounded reconnect rounds for controlled challenge and reply loss", () => {
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_SUPERVISOR_DROP_CHALLENGE: "1" })).toThrow();
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_CONNECTION_DROP_ROUNDS: "2", GP_LIVE_SUPERVISOR_DROP_REPLY: "yes" })).toThrow();
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_CONNECTION_DROP_ROUNDS: "2", GP_LIVE_SUPERVISOR_DROP_CHALLENGE: "1", GP_LIVE_SUPERVISOR_DROP_REPLY: "1" }))
      .toMatchObject({ dropChallenge: true, dropReply: true });
  });
  it("bounds forced connection drops and long reconnect verification independently of startup", () => {
    expect(resolveR9RecoveryProofOptions(enabled)).toMatchObject({ connectionDropRounds: 0, reconnectSoakMs: 0 });
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_CONNECTION_DROP_ROUNDS: "2", GP_LIVE_RECONNECT_SOAK_MS: "7800000" }))
      .toMatchObject({ connectionDropRounds: 2, reconnectSoakMs: 7_800_000 });
    for (const value of ["-1", "3", "1.5", "Infinity"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_CONNECTION_DROP_ROUNDS: value })).toThrow();
    }
    for (const value of ["-1", "59999", "9000001", "Infinity"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_RECONNECT_SOAK_MS: value })).toThrow();
    }
    expect(resolveR9RecoveryProofOptions(enabled).coldStartConcurrency).toBe(1);
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_PROJECT_COUNT: "8" }).coldStartConcurrency).toBe(2);
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_PROJECT_COUNT: "8", GP_LIVE_COLD_START_CONCURRENCY: "1" }).coldStartConcurrency).toBe(1);
    for (const value of ["0", "3", "1.5"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_COLD_START_CONCURRENCY: value })).toThrow();
    }
  });
  it("requires explicit LLM selection and a valid concurrency benchmark opt-in", () => {
    expect(resolveR9RecoveryProofOptions(enabled)).toMatchObject({ bootstrapMode: "direct", benchmarkConcurrency: false });
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "llm", GP_LIVE_BENCHMARK_CONCURRENCY: "1" }))
      .toMatchObject({ bootstrapMode: "llm", benchmarkConcurrency: true });
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "auto" })).toThrow();
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_BENCHMARK_CONCURRENCY: "true" })).toThrow();
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_RECOVERY_CYCLES: "3" }).recoveryCycles).toBe(3);
    for (const cycles of ["0", "4", "1.5", "NaN"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_RECOVERY_CYCLES: cycles })).toThrow();
    }
  });

  it("requires usable live capacity instead of treating a recovering ready-state runtime as ready", () => {
    const ready = { state: "ready", capacity: 8, available: 8, transportConnected: true, quarantined: 0, ambiguousQuarantined: 0 };
    expect(r9RuntimeIsReady(ready)).toBe(true);
    for (const unavailable of [
      { ...ready, state: "bootstrapping" }, { ...ready, capacity: 0 }, { ...ready, available: 0 },
      { ...ready, transportConnected: false }, { ...ready, quarantined: 1 }, { ...ready, ambiguousQuarantined: 1 },
    ]) expect(r9RuntimeIsReady(unavailable)).toBe(false);
  });
  it("requires both live and dedicated-project mutation authorization before reading credentials", () => {
    expect(() => resolveR9RecoveryProofOptions({})).toThrow();
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_R9_SUPERVISOR_RECOVERY: "0" })).toThrow();
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "0" })).toThrow();
  });

  it("restricts all candidate traffic to 18888", () => {
    expect(resolveR9RecoveryProofOptions(enabled).port).toBe(18888);
    for (const port of ["18787", "0", "18889", "invalid"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, PORT: port })).toThrow();
    }
  });

  it("bounds the disposable project count to the one-project and eight-project gates", () => {
    expect(resolveR9RecoveryProofOptions(enabled).target).toBe(1);
    expect(resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_PROJECT_COUNT: "8" }).target).toBe(8);
    for (const count of ["0", "2", "9", "-1", "1.5", "NaN"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_PROJECT_COUNT: count })).toThrow();
    }
  });

  it("requires bounded deadlines and exact credential-free model labels", () => {
    for (const timeout of ["0", "59999", "3600001", "Infinity"]) {
      expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_TIMEOUT_MS: timeout })).toThrow();
    }
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, GP_LIVE_MODEL: "model\n" })).toThrow();
    expect(() => resolveR9RecoveryProofOptions({ ...enabled, PRISM_COOKIE_FILE: "" })).toThrow();
  });
});
