import { V2_RETIREMENT_RELEASE_IDENTITIES } from "../sidecar/assets";
import { validateSupervisorBootstrapAttempt, type SupervisorBootstrapAttempt } from "./bootstrap-journal";
import { sameSupervisorBinding, verifySupervisorSandboxMessage } from "./protocol";
import { parseSupervisorRetirementReceipt } from "./retirement-receipt";
import { openSupervisorSessionBootstrap } from "./session-bootstrap";

export type BootstrapExecutionObservation = "not_checked" | "files_present" | "workspace_unavailable" | "job_reported_exited";
export type BootstrapReceiptObservation = "not_checked" | "missing" | "invalid" | "other_project" | "older_generation" |
  "newer_generation" | "binding_mismatch" | "instance_mismatch" | "signature_mismatch" | "exact_signed_shutdown";

/** Content-free assessment only. It never writes a receipt, clears debt, launches, or deletes. */
export interface BootstrapRecoveryFrontier {
  readonly journalPhase: SupervisorBootstrapAttempt["phase"] | "missing";
  readonly acceptance: "not_started" | "unknown" | "accepted";
  readonly journalValid: boolean;
  readonly releaseRegistered: boolean;
  readonly sealedAuthorityValid: boolean;
  readonly receipt: BootstrapReceiptObservation;
  readonly execution: BootstrapExecutionObservation;
  readonly next: "inspect_journal_authority" | "inspect_registered_release" | "inspect_sealed_authority" |
    "clean_exact_prepared_artifacts" | "obtain_exact_supervisor_terminal" | "restore_exact_execution_authority" |
    "persist_exact_cleanup_proof" | "finalize_exact_artifacts";
  readonly mayReplay: false;
  readonly mayDeleteProject: false;
}

/**
 * Pure local inspection of already-read, bounded private journal/receipt data.
 * Missing workspace files, job exit, acceptance, and older retirement receipts
 * are never substitutes for the exact supervisor's authenticated terminal.
 */
export async function assessBootstrapRecoveryFrontier(input: {
  readonly attempt: SupervisorBootstrapAttempt | null;
  readonly revision: "r9" | "r10";
  readonly retirementReceipt?: unknown;
  readonly executionObservation?: BootstrapExecutionObservation;
}): Promise<BootstrapRecoveryFrontier> {
  const execution = input.executionObservation ?? "not_checked";
  if (!["not_checked", "files_present", "workspace_unavailable", "job_reported_exited"].includes(execution)) {
    throw new TypeError("Unknown bootstrap execution observation");
  }
  let journalPhase: BootstrapRecoveryFrontier["journalPhase"] = "missing";
  let acceptance: BootstrapRecoveryFrontier["acceptance"] = "unknown";
  let journalValid = false, releaseRegistered = false, sealedAuthorityValid = false;
  let receipt: BootstrapReceiptObservation = input.retirementReceipt === undefined ? "not_checked" : input.retirementReceipt === null ? "missing" : "invalid";
  const result = (next: BootstrapRecoveryFrontier["next"]): BootstrapRecoveryFrontier => Object.freeze({
    journalPhase, acceptance, journalValid, releaseRegistered, sealedAuthorityValid, receipt, execution, next,
    mayReplay: false, mayDeleteProject: false,
  });
  const attempt = input.attempt;
  if (!attempt) return result("inspect_journal_authority");
  try {
    validateSupervisorBootstrapAttempt(attempt);
    if (typeof attempt.sealed.salt !== "string" || attempt.sealed.salt.length > 64 ||
      typeof attempt.sealed.iv !== "string" || attempt.sealed.iv.length > 32 ||
      typeof attempt.sealed.ciphertext !== "string" || attempt.sealed.ciphertext.length > 16 * 1024) return result("inspect_journal_authority");
    journalPhase = attempt.phase;
    if (attempt.commandAcceptance !== undefined) {
      acceptance = "accepted";
    } else if (attempt.phase === "prepared") acceptance = "not_started";
    journalValid = true;
  } catch { return result("inspect_journal_authority"); }
  releaseRegistered = V2_RETIREMENT_RELEASE_IDENTITIES.some(value => value.revision === input.revision &&
    value.buildHash === attempt.binding.buildHash && value.schemaHash === attempt.binding.schemaHash &&
    value.protocolRevision === attempt.binding.protocolRevision);
  if (!releaseRegistered) return result("inspect_registered_release");
  let authority: Awaited<ReturnType<typeof openSupervisorSessionBootstrap>>;
  try {
    authority = await openSupervisorSessionBootstrap({ sessionId: attempt.sessionId, expectedBinding: attempt.binding, sealed: attempt.sealed });
    sealedAuthorityValid = true;
  } catch { return result("inspect_sealed_authority"); }

  if (input.retirementReceipt !== undefined && input.retirementReceipt !== null) {
    const parsed = parseSupervisorRetirementReceipt(input.retirementReceipt);
    if (parsed) {
      if (parsed.binding.projectId !== attempt.binding.projectId) receipt = "other_project";
      else if (parsed.binding.leaderGeneration < attempt.binding.leaderGeneration) receipt = "older_generation";
      else if (parsed.binding.leaderGeneration > attempt.binding.leaderGeneration) receipt = "newer_generation";
      else if (!sameSupervisorBinding(parsed.binding, attempt.binding)) receipt = "binding_mismatch";
      else if (parsed.supervisorInstanceId !== authority.instanceId) receipt = "instance_mismatch";
      else receipt = await verifySupervisorSandboxMessage(authority.credential.rotationKey, parsed)
        ? "exact_signed_shutdown" : "signature_mismatch";
    }
  }
  if (attempt.phase === "prepared") return result("clean_exact_prepared_artifacts");
  if (attempt.phase === "cleanup_proven") return result("finalize_exact_artifacts");
  if (receipt === "exact_signed_shutdown") return result("persist_exact_cleanup_proof");
  return result(execution === "workspace_unavailable" ? "restore_exact_execution_authority" : "obtain_exact_supervisor_terminal");
}
