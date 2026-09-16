import { describe, expect, it } from "bun:test";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import type { SupervisorBootstrapAttempt } from "../src/v2/supervisor/bootstrap-journal";
import { assessBootstrapRecoveryFrontier } from "../src/v2/supervisor/recovery-frontier";
import { GP_V2_SUPERVISOR_PROTOCOL, GP_V2_SUPERVISOR_VERSION, signSupervisorSandboxMessage } from "../src/v2/supervisor/protocol";
import { encodeSupervisorRetirementReceipt } from "../src/v2/supervisor/retirement-receipt";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const assets = await loadV2SidecarAssets({ revision: "r10" });
const sessionId = "22222222-2222-4222-8222-222222222222";
const binding = { projectId: "11111111-1111-4111-8111-111111111111", sandboxSessionId: "independent-prism-session",
  leaderGeneration: 2, buildHash: assets.buildHash, schemaHash: assets.schemaHash, nonce: "current-supervisor",
  protocolRevision: "r2" as const, schemaVersion: 2 as const };

async function fixture() {
  const key = createCredentialRotationKey(), instanceId = "current-instance";
  const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: { instanceId,
    credential: { url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: binding.projectId,
      token: "private-frontier-token", authorization: "full", rotationKey: key } } });
  const attempt: SupervisorBootstrapAttempt = { version: 1, phase: "submitted", sessionId, binding, sealed, connectTimeoutMs: 1000 };
  const receipt = async (change: "exact" | "older" | "newer" | "binding" | "instance" | "signature") => encodeSupervisorRetirementReceipt(
    await signSupervisorSandboxMessage(change === "signature" ? createCredentialRotationKey() : key, {
      protocol: GP_V2_SUPERVISOR_PROTOCOL, version: GP_V2_SUPERVISOR_VERSION, direction: "sandbox",
      binding: change === "older" ? { ...binding, leaderGeneration: 1 } : change === "newer" ? { ...binding, leaderGeneration: 3 }
        : change === "binding" ? { ...binding, nonce: "other-supervisor" } : binding,
      supervisorInstanceId: change === "instance" ? "other-instance" : instanceId,
      sequence: 2, message: { kind: "shutdown" },
    }));
  return { attempt, receipt };
}

describe("supervisor bootstrap recovery frontier", () => {
  it("keeps legacy unknown acceptance and absent authority unresolved", async () => {
    const f = await fixture();
    expect(await assessBootstrapRecoveryFrontier({ attempt: null, revision: "r10" })).toMatchObject({
      journalPhase: "missing", acceptance: "unknown", next: "inspect_journal_authority", mayReplay: false, mayDeleteProject: false,
    });
    const summary = await assessBootstrapRecoveryFrontier({ attempt: f.attempt, revision: "r10", retirementReceipt: null,
      executionObservation: "workspace_unavailable" });
    expect(summary).toMatchObject({ journalValid: true, releaseRegistered: true, sealedAuthorityValid: true,
      acceptance: "unknown", receipt: "missing", next: "restore_exact_execution_authority", mayReplay: false, mayDeleteProject: false });
    expect(JSON.stringify(summary)).not.toContain(sessionId);
    expect(JSON.stringify(summary)).not.toContain("private-frontier-token");
  });

  it.each(["older", "newer", "binding", "instance", "signature"] as const)("never clears current execution debt using %s receipt evidence", async change => {
    const f = await fixture();
    const categories = { older: "older_generation", newer: "newer_generation", binding: "binding_mismatch", instance: "instance_mismatch", signature: "signature_mismatch" };
    const before = JSON.stringify(f.attempt);
    const summary = await assessBootstrapRecoveryFrontier({ attempt: f.attempt, revision: "r10", retirementReceipt: await f.receipt(change),
      executionObservation: "job_reported_exited" });
    expect(summary).toMatchObject({ receipt: categories[change], next: "obtain_exact_supervisor_terminal", mayReplay: false, mayDeleteProject: false });
    expect(JSON.stringify(f.attempt)).toBe(before);
  });

  it("identifies exact signed proof without committing or treating command acceptance as cleanup", async () => {
    const f = await fixture();
    const accepted = { ...f.attempt, commandAcceptance: { sessionId, workspaceRoot: `/codex_workspace/${sessionId}`, jobId: "private-job" } };
    expect(await assessBootstrapRecoveryFrontier({ attempt: accepted, revision: "r10" })).toMatchObject({ acceptance: "accepted",
      next: "obtain_exact_supervisor_terminal", mayReplay: false, mayDeleteProject: false });
    expect(await assessBootstrapRecoveryFrontier({ attempt: accepted, revision: "r10", retirementReceipt: await f.receipt("exact") }))
      .toMatchObject({ receipt: "exact_signed_shutdown", next: "persist_exact_cleanup_proof", mayReplay: false, mayDeleteProject: false });
    expect(f.attempt.phase).toBe("submitted");
    expect(await assessBootstrapRecoveryFrontier({ attempt: { ...accepted, phase: "cleanup_proven" }, revision: "r10" }))
      .toMatchObject({ next: "finalize_exact_artifacts", mayReplay: false, mayDeleteProject: false });
  });

  it("distinguishes prepared work, unregistered release, and invalid sealed authority", async () => {
    const f = await fixture();
    expect(await assessBootstrapRecoveryFrontier({ attempt: { ...f.attempt, phase: "prepared" }, revision: "r10" }))
      .toMatchObject({ acceptance: "not_started", next: "clean_exact_prepared_artifacts" });
    const unregisteredBinding = { ...binding, buildHash: "a".repeat(64) };
    expect(await assessBootstrapRecoveryFrontier({ attempt: { ...f.attempt, binding: unregisteredBinding,
      sealed: { ...f.attempt.sealed, binding: unregisteredBinding } }, revision: "r10" }))
      .toMatchObject({ releaseRegistered: false, next: "inspect_registered_release" });
    expect(await assessBootstrapRecoveryFrontier({ attempt: { ...f.attempt, sessionId: "33333333-3333-4333-8333-333333333333" }, revision: "r10" }))
      .toMatchObject({ sealedAuthorityValid: false, next: "inspect_sealed_authority" });
  });

  it.each(["prepared", "submitted", "cleanup_proven"] as const)("rejects malformed %s journal authority before recommending recovery", async phase => {
    const f = await fixture();
    const valid = { ...f.attempt, phase };
    for (const attempt of [
      { ...valid, unknownField: "unexpected" },
      { ...valid, commandAcceptance: null },
      { ...valid, sealed: { ...valid.sealed, binding: { ...binding, nonce: "another-owner" } } },
    ]) {
      const before = JSON.stringify(attempt);
      expect(await assessBootstrapRecoveryFrontier({ attempt: attempt as never, revision: "r10" }))
        .toMatchObject({ journalValid: false, next: "inspect_journal_authority", mayReplay: false, mayDeleteProject: false });
      expect(JSON.stringify(attempt)).toBe(before);
    }
  });
});
