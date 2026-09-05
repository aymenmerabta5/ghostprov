/** Protocol for the authenticated sandbox command service, not Responses. */
export const DIRECT_SUPERVISOR_EXEC_PATH = "codex/codex_exec_async";
export const DIRECT_SUPERVISOR_EXEC_MODEL = "gpt-5.6-sol";
export const DIRECT_SUPERVISOR_EXEC_RESPONSE_BYTES = 64 * 1024;

const SESSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const JOB_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$(?![\s\S])/;

export interface DirectSupervisorExecRequest {
  /** Host-generated execution identity. Never a provider response ID. */
  readonly sessionId: string;
}

export interface DirectSupervisorExecStarted {
  readonly sessionId: string;
  readonly workspaceRoot: string;
  readonly jobId: string;
}

export function directSupervisorExecBody(request: DirectSupervisorExecRequest) {
  if (typeof request.sessionId !== "string" || !SESSION_ID.test(request.sessionId)) {
    throw new TypeError("Invalid direct execution session identity");
  }
  return Object.freeze({
    // Required by the command service. /bin/sh consumes the staged `exec`
    // script; neither this prompt nor the model is used for inference.
    prompt: "Hiii",
    model: DIRECT_SUPERVISOR_EXEC_MODEL,
    codex_path: "/bin/sh",
    session_id: request.sessionId,
    timeout_s: 60,
    image_generation: false,
  });
}

export function parseDirectSupervisorExecStarted(
  value: unknown,
  expectedSessionId: string,
): DirectSupervisorExecStarted {
  if (typeof expectedSessionId !== "string" || !SESSION_ID.test(expectedSessionId) ||
    !value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("Invalid direct execution acknowledgement");
  }
  const raw = value as Record<string, unknown>;
  const workspaceRoot = `/codex_workspace/${expectedSessionId}`;
  if (
    raw.status !== "started" || raw.runner_kind !== "subprocess" || raw.direct_mode_active !== true ||
    raw.session_id !== expectedSessionId || raw.codex_workspace !== workspaceRoot ||
    raw.effective_model !== DIRECT_SUPERVISOR_EXEC_MODEL || typeof raw.job_id !== "string" || !JOB_ID.test(raw.job_id)
  ) throw new TypeError("Direct execution acknowledgement does not match its request");
  // A returned PID is deliberately neither retained nor treated as authority.
  return Object.freeze({ sessionId: expectedSessionId, workspaceRoot, jobId: raw.job_id });
}
