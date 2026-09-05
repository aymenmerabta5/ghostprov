import { describe, expect, it } from "bun:test";
import { resolveR9RecoveryProofOptions, r9RuntimeIsReady } from "../scripts/live_r9_supervisor_recovery";

const enabled = {
  GP_LIVE_R9_SUPERVISOR_RECOVERY: "1",
  GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
  PRISM_COOKIE_FILE: "unused-offline-cookie-path.json",
};

describe("R9 supervisor live recovery guards", () => {
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
