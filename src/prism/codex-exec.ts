import type { AccountRequestClient } from "./provisioning-client";
import type { ProjectId } from "../sandbox/types";

/**
 * Machine client for the platform's Codex turn API
 * (`POST /api/llm/response_with_tools_start` + `POST
 * /api/llm/response_with_tools_status`), as observed from first-party
 * traffic. Used only to execute the staged relay launcher inside the sandbox
 * container and read back its printed hello block. Pure policy + transport:
 * no clocks beyond injected ones, no globals, no credential logging.
 */

export type CodexExecErrorKind =
  | "transport"
  | "auth"
  | "rate_limited"
  | "protocol"
  | "timeout"
  | "turn_failed";

export class CodexExecError extends Error {
  constructor(
    message: string,
    readonly kind: CodexExecErrorKind,
    readonly status?: number,
  ) {
    super(message);
    this.name = "CodexExecError";
  }
}

export interface CodexExecCommandOptions {
  account: AccountRequestClient;
  /** Prism project that owns the sandbox the command runs in. */
  projectId: ProjectId;
  /** Sandbox session base URL, e.g. from provisioning. */
  sandboxUrl: string;
  /** Sandbox session token. Never logged; sent only to the first-party API. */
  sandboxToken: string;
  /** Exact shell command for the agent to execute verbatim. */
  command: string;
  /** Model identifier accepted by the turn API. Defaults to the observed one. */
  model?: string;
  /** Reasoning effort accepted by the turn API. Defaults to the observed one. */
  reasoningEffort?: string;
  /** Per-request timeout for a single API call. */
  requestTimeoutMs?: number;
  /** Delay between status polls. */
  pollIntervalMs?: number;
  /** Maximum status polls before giving up. */
  maxPolls?: number;
  /** How many times a not-yet-started turn may be restarted. */
  maxStartAttempts?: number;
  /** Upper bound for any single response body accepted for parsing. */
  maxBodyBytes?: number;
  signal?: AbortSignal;
}

export interface CodexExecResult {
  /** Concatenated assistant `output_text` of the completed turn. */
  transcript: string;
  requestId: string;
  polls: number;
}

const DEFAULT_MODEL = "gpt-5.6-sol";
// "low" completes mechanical relay turns in seconds (observed ~9 s) while
// "xhigh" costs tens of seconds the bootstrap child budget cannot spare.
// Hello validation fails closed, so a lazier turn only costs a retry.
const DEFAULT_REASONING_EFFORT = "low";
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_INTERVAL_MS = 5_000;
const DEFAULT_MAX_POLLS = 18;
const DEFAULT_MAX_START_ATTEMPTS = 3;
const DEFAULT_MAX_BODY_BYTES = 1024 * 1024;
const MAX_COMMAND_CHARS = 2_048;
const MAX_ERROR_SNIPPET_CHARS = 200;

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new DOMException("Codex exec aborted", "AbortError");
  }
}

/** Scrub anything credential-shaped before it can reach an error message. */
export function scrubCodexText(value: string): string {
  return value
    .replace(/eyJ[A-Za-z0-9_-]{10,}/g, "«jwt»")
    .replace(/(sk-[A-Za-z0-9_-]{6})[A-Za-z0-9_-]+/g, "$1…")
    .replace(/[A-Fa-f0-9]{40,}/g, "«hex»");
}

function snippet(value: unknown): string {
  const text = typeof value === "string" ? value : JSON.stringify(value) ?? "?";
  const clean = scrubCodexText(text).replace(/\s+/g, " ").trim();
  return clean.length > MAX_ERROR_SNIPPET_CHARS
    ? `${clean.slice(0, MAX_ERROR_SNIPPET_CHARS)}…`
    : clean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

async function readBoundedJson(
  response: Response,
  maxBodyBytes: number,
  operation: string,
): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) {
    const text = await response.text().catch(() => "");
    if (text.length > maxBodyBytes) {
      throw new CodexExecError(
        `Codex ${operation} response exceeded the bounded limit`,
        "protocol",
        response.status,
      );
    }
    try {
      return JSON.parse(text) as unknown;
    } catch {
      throw new CodexExecError(
        `Codex ${operation} returned invalid JSON (${response.status})`,
        "protocol",
        response.status,
      );
    }
  }
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBodyBytes) {
      await reader.cancel().catch(() => undefined);
      throw new CodexExecError(
        `Codex ${operation} response exceeded the bounded limit`,
        "protocol",
        response.status,
      );
    }
    chunks.push(value);
  }
  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder().decode(merged)) as unknown;
  } catch {
    throw new CodexExecError(
      `Codex ${operation} returned invalid JSON (${response.status})`,
      "protocol",
      response.status,
    );
  }
}

interface StartedTurn {
  requestId: string;
  turnState: Record<string, unknown> | null;
  terminal: { completed: boolean; transcript: string; reason?: string } | null;
}

function turnStatusOf(body: unknown): string | null {
  if (!isRecord(body)) return null;
  return typeof body.status === "string" ? body.status : null;
}

function requestIdOf(body: unknown): string | null {
  if (!isRecord(body)) return null;
  return typeof body.request_id === "string" && body.request_id.length > 0
    ? body.request_id
    : null;
}

function turnStateOf(body: unknown): Record<string, unknown> | null {
  if (!isRecord(body)) return null;
  return isRecord(body.turn_state) ? body.turn_state : null;
}

/** Collect assistant `output_text` fragments from a status/terminal body. */
export function collectCodexTranscript(body: unknown): string {
  const parts: string[] = [];
  const visit = (value: unknown): void => {
    if (typeof value === "string") return;
    if (Array.isArray(value)) {
      for (const entry of value) visit(entry);
      return;
    }
    if (!isRecord(value)) return;
    if (value.type === "output_text" && typeof value.text === "string" && value.text) {
      parts.push(value.text);
    }
    for (const entry of Object.values(value)) visit(entry);
  };
  const roots: unknown[] = [body];
  if (isRecord(body)) {
    const progress = body.codex_live_progress;
    if (isRecord(progress) && Array.isArray(progress.eventPreviews)) {
      for (const preview of progress.eventPreviews) {
        if (!isRecord(preview) || typeof preview.raw !== "string") continue;
        try {
          roots.push(JSON.parse(preview.raw) as unknown);
        } catch {
          // Non-JSON preview lines carry no transcript text.
        }
      }
    }
  }
  for (const root of roots) visit(root);
  return parts.join("\n");
}

function innerTurnError(body: unknown): string | null {
  if (!isRecord(body)) return null;
  const response = body.response;
  if (!isRecord(response)) return null;
  if (response.status === "error" && isRecord(response.payload)) {
    const reason = response.payload.reason;
    return typeof reason === "string" && reason ? reason : "unknown";
  }
  return null;
}

async function postJson(
  account: AccountRequestClient,
  path: string,
  body: unknown,
  options: { requestTimeoutMs: number; maxBodyBytes: number; operation: string; signal?: AbortSignal },
): Promise<{ status: number; body: unknown }> {
  checkAbort(options.signal);
  let payload: string;
  try {
    payload = JSON.stringify(body);
  } catch {
    throw new CodexExecError(`Codex ${options.operation} payload is not serializable`, "protocol");
  }
  if (payload.length > options.maxBodyBytes) {
    throw new CodexExecError(`Codex ${options.operation} payload exceeds the bounded limit`, "protocol");
  }
  const timeoutController = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    timeoutController.abort();
  }, options.requestTimeoutMs);
  const onAbort = () => timeoutController.abort(options.signal?.reason);
  options.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const response = await account.request(path, {
      method: "POST",
      signal: timeoutController.signal,
      headers: { "content-type": "application/json", accept: "application/json" },
      body: payload,
    });
    const parsed = await readBoundedJson(response, options.maxBodyBytes, options.operation);
    return { status: response.status, body: parsed };
  } catch (error) {
    if (error instanceof CodexExecError) throw error;
    checkAbort(options.signal);
    if (timedOut) {
      throw new CodexExecError(`Codex ${options.operation} timed out`, "timeout");
    }
    throw new CodexExecError(`Codex ${options.operation} transport failed`, "transport");
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", onAbort);
  }
}

function throwForHttpStatus(operation: string, status: number): never {
  if (status === 401 || status === 403) {
    throw new CodexExecError(`Codex ${operation} rejected the session (${status})`, "auth", status);
  }
  if (status === 429) {
    throw new CodexExecError(`Codex ${operation} was rate limited (${status})`, "rate_limited", status);
  }
  throw new CodexExecError(`Codex ${operation} failed (${status})`, "protocol", status);
}

async function startTurn(
  options: Required<Pick<CodexExecCommandOptions, "account" | "projectId" | "sandboxUrl" | "sandboxToken" | "command" | "model" | "reasoningEffort" | "requestTimeoutMs" | "maxBodyBytes">> & { signal?: AbortSignal },
  origin: string,
): Promise<StartedTurn> {
  const started = await postJson(
    options.account,
    "/api/llm/response_with_tools_start",
    {
      input: [
        {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text: options.command }],
        },
      ],
      metadata: {
        projectId: options.projectId,
        model: options.model,
        reasoning_effort: options.reasoningEffort,
        frontend_origin: origin,
        sandbox_url: options.sandboxUrl,
        sandbox_token: options.sandboxToken,
      },
    },
    {
      requestTimeoutMs: options.requestTimeoutMs,
      maxBodyBytes: options.maxBodyBytes,
      operation: "turn start",
      signal: options.signal,
    },
  );
  if (started.status < 200 || started.status >= 300) {
    throwForHttpStatus("turn start", started.status);
  }
  const status = turnStatusOf(started.body);
  const requestId = requestIdOf(started.body);
  if (!requestId) {
    throw new CodexExecError("Codex turn start returned no request id", "protocol", started.status);
  }
  if (status !== null && status !== "pending" && status !== "started") {
    const errorReason = innerTurnError(started.body);
    if (errorReason) return { requestId, turnState: null, terminal: { completed: false, transcript: "", reason: errorReason } };
    const transcript = collectCodexTranscript(started.body);
    return { requestId, turnState: null, terminal: { completed: true, transcript } };
  }
  return { requestId, turnState: turnStateOf(started.body), terminal: null };
}

/**
 * Runs one shell command through a platform Codex turn and returns the
 * assistant transcript. Each attempt starts a brand-new turn; a turn is only
 * restarted when the previous one provably never began (transport failure
 * before any response, or an explicit `sandbox_reconnecting` terminal).
 */
export async function runCodexCommand(input: CodexExecCommandOptions): Promise<CodexExecResult> {
  const {
    account,
    projectId,
    sandboxUrl,
    sandboxToken,
    command,
    signal,
  } = input;
  if (!command || command.length > MAX_COMMAND_CHARS) {
    throw new CodexExecError("Codex exec command must be 1 through 2048 characters", "protocol");
  }
  if (!sandboxUrl || !sandboxToken) {
    throw new CodexExecError("Codex exec requires a provisioned sandbox session", "protocol");
  }
  const model = input.model ?? DEFAULT_MODEL;
  const reasoningEffort = input.reasoningEffort ?? DEFAULT_REASONING_EFFORT;
  const requestTimeoutMs = input.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
  const pollIntervalMs = input.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const maxPolls = input.maxPolls ?? DEFAULT_MAX_POLLS;
  const maxStartAttempts = input.maxStartAttempts ?? DEFAULT_MAX_START_ATTEMPTS;
  const maxBodyBytes = input.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES;
  for (const [name, value] of [
    ["requestTimeoutMs", requestTimeoutMs],
    ["pollIntervalMs", pollIntervalMs],
    ["maxPolls", maxPolls],
    ["maxStartAttempts", maxStartAttempts],
    ["maxBodyBytes", maxBodyBytes],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 1) {
      throw new CodexExecError(`Codex exec ${name} must be a positive integer`, "protocol");
    }
  }
  // The turn API authenticates the account session; the origin mirrors the
  // first-party client the contract was observed from. It carries no secret.
  const origin = "https://prism.openai.com";

  let attempt = 0;
  let lastError: CodexExecError | null = null;
  while (attempt < maxStartAttempts) {
    attempt += 1;
    checkAbort(signal);
    let started: StartedTurn;
    try {
      started = await startTurn(
        { account, projectId, sandboxUrl, sandboxToken, command, model, reasoningEffort, requestTimeoutMs, maxBodyBytes, signal },
        origin,
      );
    } catch (error) {
      if (error instanceof CodexExecError && error.kind === "transport") {
        lastError = error;
        continue;
      }
      throw error;
    }
    if (started.terminal) {
      if (started.terminal.completed) {
        if (!started.terminal.transcript.trim()) {
          throw new CodexExecError("Codex turn completed without transcript text", "protocol");
        }
        return { transcript: started.terminal.transcript, requestId: started.requestId, polls: 0 };
      }
      if (started.terminal.reason === "sandbox_reconnecting") {
        lastError = new CodexExecError("Codex sandbox is reconnecting", "transport");
        await sleepBounded(pollIntervalMs, signal);
        continue;
      }
      throw new CodexExecError(
        `Codex turn failed before running (${started.terminal.reason ?? "unknown"})`,
        "turn_failed",
      );
    }
    if (!started.turnState) {
      throw new CodexExecError("Codex turn started without resumable state", "protocol");
    }
    return await pollTurn({
      account,
      requestId: started.requestId,
      turnState: started.turnState,
      requestTimeoutMs,
      pollIntervalMs,
      maxPolls,
      maxBodyBytes,
      signal,
    });
  }
  throw lastError ?? new CodexExecError("Codex turn could not be started", "transport");
}

async function pollTurn(input: {
  account: AccountRequestClient;
  requestId: string;
  turnState: Record<string, unknown>;
  requestTimeoutMs: number;
  pollIntervalMs: number;
  maxPolls: number;
  maxBodyBytes: number;
  signal?: AbortSignal;
}): Promise<CodexExecResult> {
  let turnState = input.turnState;
  let consecutiveTransportFailures = 0;
  for (let polls = 0; polls < input.maxPolls; polls += 1) {
    input.signal?.throwIfAborted();
    await sleepBounded(input.pollIntervalMs, input.signal);
    let status: number;
    let body: unknown;
    try {
      const polled = await postJson(
        input.account,
        "/api/llm/response_with_tools_status",
        { request_id: input.requestId, turn_state: turnState },
        {
          requestTimeoutMs: input.requestTimeoutMs,
          maxBodyBytes: input.maxBodyBytes,
          operation: "turn status",
          signal: input.signal,
        },
      );
      status = polled.status;
      body = polled.body;
      consecutiveTransportFailures = 0;
    } catch (error) {
      if (error instanceof CodexExecError && error.kind === "transport") {
        consecutiveTransportFailures += 1;
        if (consecutiveTransportFailures > 3) throw error;
        continue;
      }
      throw error;
    }
    if (status < 200 || status >= 300) {
      throwForHttpStatus("turn status", status);
    }
    const next = turnStateOf(body);
    if (next) turnState = next;
    const turnStatus = turnStatusOf(body);
    if (turnStatus === null || turnStatus === "pending" || turnStatus === "started") {
      continue;
    }
    const transcript = collectCodexTranscript(body);
    if (!transcript.trim()) {
      throw new CodexExecError(
        `Codex turn ended without transcript text (${snippet(turnStatus)})`,
        "turn_failed",
      );
    }
    return { transcript, requestId: input.requestId, polls: polls + 1 };
  }
  throw new CodexExecError("Codex turn did not complete inside its poll budget", "timeout");
}

function sleepBounded(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason instanceof Error ? signal.reason : new DOMException("Codex exec aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason instanceof Error ? signal.reason : new DOMException("Codex exec aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
