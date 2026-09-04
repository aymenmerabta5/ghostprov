export const SIDECAR_TERM_TIMEOUT_MS = 15_000;
export const SIDECAR_KILL_TIMEOUT_MS = 2_000;
export const SUPERVISOR_RETIREMENT_TIMEOUT_MS = SIDECAR_TERM_TIMEOUT_MS + SIDECAR_KILL_TIMEOUT_MS + 3_000;

export interface SupervisorRetirementOperations {
  isRetired(): Promise<boolean>;
  isExactProcess(): Promise<boolean>;
  terminate(): void;
  waitForRetirement(timeoutMs: number): Promise<boolean>;
}

/** A supervisor must retain its child handle until child cleanup is proved. */
export async function retireVerifiedSupervisor(operations: SupervisorRetirementOperations): Promise<void> {
  if (await operations.isRetired()) return;
  if (!await operations.isExactProcess()) throw new Error("Existing supervisor process identity is invalid");
  try {
    operations.terminate();
  } catch (error) {
    if (await operations.isRetired()) return;
    throw error;
  }
  if (!await operations.waitForRetirement(SUPERVISOR_RETIREMENT_TIMEOUT_MS)) {
    // Killing the supervisor here would discard the only child resource that
    // can terminate the sidecar without a PID-reuse race. Leave it fenced.
    throw new Error("Existing supervisor did not prove retained-child cleanup");
  }
}
