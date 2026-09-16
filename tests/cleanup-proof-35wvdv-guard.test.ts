import { describe, expect, it } from "bun:test";
import {
  RETAINED_WORKER3_CLEANUP_PLAN,
  resolveRetainedWorker3CleanupAuthorization,
} from "../scripts/cleanup_proof_35wvdv_worker3";

describe("retained worker 3 cleanup guard", () => {
  const env = {
    GP_LIVE_PROOF_35WVDV_WORKER3_CLEANUP: "1",
    GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
  } as const;

  it("requires both exact live guards and the one explicit command", () => {
    expect(() => resolveRetainedWorker3CleanupAuthorization([], env)).toThrow();
    expect(() => resolveRetainedWorker3CleanupAuthorization(
      ["--execute-authorized-cleanup"],
      { ...env, GP_LIVE_PROOF_35WVDV_WORKER3_CLEANUP: "0" },
    )).toThrow();
    expect(() => resolveRetainedWorker3CleanupAuthorization(
      ["--execute-authorized-cleanup"],
      { ...env, GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "0" },
    )).toThrow();
    expect(resolveRetainedWorker3CleanupAuthorization(
      ["--execute-authorized-cleanup"],
      env,
    )).toBe(RETAINED_WORKER3_CLEANUP_PLAN);
  });

  it("models the retained sandbox and cleanup workspace as distinct", () => {
    expect(RETAINED_WORKER3_CLEANUP_PLAN).toMatchObject({
      proofAttempts: 1,
      workspaceAcquisitions: 1,
      projectDeleteAttempts: 1,
      recoveryMode: "journal_authorized_unaccepted_sequence_zero",
      cleanupWorkspaceMode: "new_disposable_sandbox_after_exact_supervisor_retirement",
      reusesRetainedSandbox: false,
      fullCleanupEligible: true,
      preservesFailedReport: true,
    });
  });
});
