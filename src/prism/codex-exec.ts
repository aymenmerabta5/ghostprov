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
  /** Prism conversation identifier returned by a prior completed turn. */
  conversationId?: string;
  /** Exact response identifier returned by the prior completed turn. */
  previousResponseId?: string;
  /** Credential-free Prism listen snapshot returned by the prior turn. */
  listenSnapshot?: CodexExecListenSnapshot;
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
  /** Content-free progress summaries for a still-running turn. */
  onProgress?: (progress: CodexExecProgress) => void | Promise<void>;
  /** Scan bounded status state for a dynamically generated relay hello. */
  scanBodyForRelayBootstrapHello?: boolean;
  /** Called once concrete, bounded platform turn identities have been returned. */
  onStarted?: (started: CodexExecStarted) => void | Promise<void>;
  signal?: AbortSignal;
}

export interface CodexExecProgress {
  readonly poll: number;
  readonly status: string | null;
  readonly eventTypes: readonly string[];
  /** Strict public bootstrap payload observed only inside command-tool output. */
  readonly relayBootstrapHello: string | null;
}

export interface CodexExecStarted {
  readonly requestId: string;
  readonly identifiers: Readonly<Record<string, string>>;
}

export interface CodexExecListenSnapshot {
  readonly conversation_id: string;
  readonly workspace_session_id: string;
  readonly codex_session_id: string | null;
  readonly last_turn_id: string | null;
  readonly endpoint_identity: string | null;
  readonly last_exec_at: string | null;
  readonly transcript_cursor: number;
  readonly created_at: string;
  readonly updated_at: string;
  readonly last_saved_at: string | null;
  readonly user_id: string | null;
  readonly project_id: string | null;
}

export interface CodexExecContinuation {
  readonly conversationId: string;
  readonly previousResponseId: string;
  readonly listenSnapshot: CodexExecListenSnapshot;
  readonly codexSessionId: string;
}

export interface CodexExecResult {
  /** Concatenated assistant `output_text` of the completed turn. */
  transcript: string;
  requestId: string;
  polls: number;
  /** Bounded identifier-only projection of the terminal body. */
  identifiers: Readonly<Record<string, string>>;
  listenSnapshot: CodexExecListenSnapshot | null;
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
  identifiers: Readonly<Record<string, string>>;
  listenSnapshot: CodexExecListenSnapshot | null;
  turnState: Record<string, unknown> | null;
  terminal: { completed: boolean; transcript: string; reason?: string } | null;
}

function nullableBoundedString(value: unknown, maximum = 512): string | null | undefined {
  return value === null
    ? null
    : typeof value === "string" && value.length <= maximum
      ? value
      : undefined;
}

function validateListenSnapshot(value: unknown): CodexExecListenSnapshot | null {
  if (!isRecord(value)) return null;
  const conversationId = nullableBoundedString(value.conversation_id, 256);
  const workspaceSessionId = nullableBoundedString(value.workspace_session_id, 256);
  const codexSessionId = nullableBoundedString(value.codex_session_id, 256);
  const lastTurnId = nullableBoundedString(value.last_turn_id, 256);
  const endpointIdentity = nullableBoundedString(value.endpoint_identity, 512);
  const lastExecAt = nullableBoundedString(value.last_exec_at, 128);
  const createdAt = nullableBoundedString(value.created_at, 128);
  const updatedAt = nullableBoundedString(value.updated_at, 128);
  const lastSavedAt = nullableBoundedString(value.last_saved_at, 128);
  const userId = nullableBoundedString(value.user_id, 256);
  const projectId = nullableBoundedString(value.project_id, 256);
  if (
    !conversationId || !workspaceSessionId ||
    codexSessionId === undefined || lastTurnId === undefined ||
    endpointIdentity === undefined || lastExecAt === undefined ||
    !createdAt || !updatedAt || lastSavedAt === undefined ||
    userId === undefined || projectId === undefined ||
    !Number.isSafeInteger(value.transcript_cursor) || (value.transcript_cursor as number) < 0
  ) return null;
  return Object.freeze({
    conversation_id: conversationId,
    workspace_session_id: workspaceSessionId,
    codex_session_id: codexSessionId,
    last_turn_id: lastTurnId,
    endpoint_identity: endpointIdentity,
    last_exec_at: lastExecAt,
    transcript_cursor: value.transcript_cursor as number,
    created_at: createdAt,
    updated_at: updatedAt,
    last_saved_at: lastSavedAt,
    user_id: userId,
    project_id: projectId,
  });
}

function listenSnapshotOf(body: unknown): CodexExecListenSnapshot | null {
  if (!isRecord(body)) return null;
  const direct = validateListenSnapshot(body.codex_listen_snapshot ?? body.codexListenSnapshot);
  if (direct) return direct;
  const response = isRecord(body.response) ? body.response : null;
  const payload = response && isRecord(response.payload) ? response.payload : null;
  return validateListenSnapshot(payload?.codexListenSnapshot ?? payload?.codex_listen_snapshot);
}

function boundedIdentifiers(body: unknown): Readonly<Record<string, string>> {
  const identifiers: Record<string, string> = {};
  let visited = 0;
  const visit = (value: unknown, path: string): void => {
    if (++visited > 1_024 || Object.keys(identifiers).length >= 64) return;
    if (Array.isArray(value)) {
      value.slice(0, 128).forEach((entry, index) => visit(entry, `${path}[${index}]`));
      return;
    }
    if (!isRecord(value)) return;
    for (const [key, entry] of Object.entries(value)) {
      if (!/^[A-Za-z0-9_-]{1,64}$/.test(key)) continue;
      const next = path ? `${path}.${key}` : key;
      if (
        typeof entry === "string" &&
        (/(?:^|_)(?:id|identifier)$/i.test(key) || /(?:Id|Identifier)$/.test(key)) &&
        /^[A-Za-z0-9._:-]{1,128}$/.test(entry)
      ) identifiers[next] = entry;
      else visit(entry, next);
    }
  };
  visit(body, "");
  return Object.freeze(identifiers);
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

export function summarizeCodexProgress(
  body: unknown,
  poll: number,
  scanBodyForRelayBootstrapHello = false,
): CodexExecProgress {
  const eventTypes = new Set<string>();
  const bootstrapHellos = new Set<string>();
  const progress = isRecord(body) ? body.codex_live_progress : null;
  const previews = isRecord(progress) && Array.isArray(progress.eventPreviews)
    ? progress.eventPreviews.slice(0, 128)
    : [];
  let visited = 0;
  const scanToolOutput = (value: unknown): void => {
    if (typeof value === "string") {
      for (const match of value.matchAll(/(?:^|[^A-Za-z0-9_])GPV2_BOOTSTRAP ([A-Za-z0-9_-]{1,8192})(?=[^A-Za-z0-9_-]|$)/g)) {
        bootstrapHellos.add(match[1]!);
        if (bootstrapHellos.size > 1) return;
      }
      return;
    }
    if (Array.isArray(value)) {
      for (const entry of value) scanToolOutput(entry);
      return;
    }
    if (!isRecord(value)) return;
    for (const entry of Object.values(value)) scanToolOutput(entry);
  };
  const visit = (value: unknown): void => {
    if (++visited > 2_048) return;
    if (Array.isArray(value)) {
      for (const entry of value) visit(entry);
      return;
    }
    if (!isRecord(value)) return;
    if (
      typeof value.type === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/.test(value.type) &&
      eventTypes.size < 128
    ) eventTypes.add(value.type);
    if (value.type === "custom_tool_call_output" || value.type === "output_text") {
      scanToolOutput(value);
    }
    for (const entry of Object.values(value)) visit(entry);
  };
  for (const preview of previews) {
    if (!isRecord(preview) || typeof preview.raw !== "string") continue;
    try {
      const parsed = JSON.parse(preview.raw);
      visit(parsed);
      if (preview.raw.includes('"custom_tool_call_output"')) scanToolOutput(preview.raw);
    } catch {}
  }
  if (scanBodyForRelayBootstrapHello) {
    try { scanToolOutput(JSON.stringify(body)); } catch {}
  }
  return Object.freeze({
    poll,
    status: turnStatusOf(body),
    eventTypes: Object.freeze([...eventTypes].sort()),
    relayBootstrapHello: bootstrapHellos.size === 1 ? [...bootstrapHellos][0]! : null,
  });
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
  options: Required<Pick<CodexExecCommandOptions, "account" | "projectId" | "sandboxUrl" | "sandboxToken" | "command" | "model" | "reasoningEffort" | "requestTimeoutMs" | "maxBodyBytes">> & Pick<CodexExecCommandOptions, "conversationId" | "previousResponseId" | "listenSnapshot"> & { signal?: AbortSignal },
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
      previousResponseId: options.previousResponseId,
      metadata: {
        projectId: options.projectId,
        ...(options.listenSnapshot?.user_id ? { userId: options.listenSnapshot.user_id } : {}),
        model: options.model,
        reasoning_effort: options.reasoningEffort,
        frontend_origin: origin,
        sandbox_url: options.sandboxUrl,
        sandbox_token: options.sandboxToken,
        ...(options.listenSnapshot
          ? {
              codex_listen_snapshot: JSON.stringify({
                ...options.listenSnapshot,
                conversation_id: options.conversationId ?? options.listenSnapshot.conversation_id,
                project_id: options.projectId,
                sandbox_url: options.sandboxUrl,
                sandbox_token: options.sandboxToken,
              }),
            }
          : {}),
      },
      conversationId: options.conversationId,
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
  const identifiers = boundedIdentifiers(started.body);
  const listenSnapshot = listenSnapshotOf(started.body);
  if (status !== null && status !== "pending" && status !== "started") {
    const errorReason = innerTurnError(started.body);
    if (errorReason) return { requestId, identifiers, listenSnapshot, turnState: null, terminal: { completed: false, transcript: "", reason: errorReason } };
    const transcript = collectCodexTranscript(started.body);
    return { requestId, identifiers, listenSnapshot, turnState: null, terminal: { completed: true, transcript } };
  }
  return { requestId, identifiers, listenSnapshot, turnState: turnStateOf(started.body), terminal: null };
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
    conversationId,
    previousResponseId,
    listenSnapshot,
    signal,
  } = input;
  if (!command || command.length > MAX_COMMAND_CHARS) {
    throw new CodexExecError("Codex exec command must be 1 through 2048 characters", "protocol");
  }
  if (!sandboxUrl || !sandboxToken) {
    throw new CodexExecError("Codex exec requires a provisioned sandbox session", "protocol");
  }
  for (const [name, value] of [
    ["conversationId", conversationId],
    ["previousResponseId", previousResponseId],
  ] as const) {
    if (value !== undefined && !/^[A-Za-z0-9._:-]{1,256}$/.test(value)) {
      throw new CodexExecError(`Codex exec ${name} is invalid`, "protocol");
    }
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
        {
          account,
          projectId,
          sandboxUrl,
          sandboxToken,
          command,
          model,
          reasoningEffort,
          requestTimeoutMs,
          maxBodyBytes,
          conversationId,
          previousResponseId,
          listenSnapshot,
          signal,
        },
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
      await input.onStarted?.({ requestId: started.requestId, identifiers: started.identifiers });
      if (started.terminal.completed) {
        if (!started.terminal.transcript.trim()) {
          throw new CodexExecError("Codex turn completed without transcript text", "protocol");
        }
        return {
          transcript: started.terminal.transcript,
          requestId: started.requestId,
          polls: 0,
          identifiers: started.identifiers,
          listenSnapshot: started.listenSnapshot,
        };
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
    await input.onStarted?.({ requestId: started.requestId, identifiers: started.identifiers });
    return await pollTurn({
      account,
      requestId: started.requestId,
      turnState: started.turnState,
      requestTimeoutMs,
      pollIntervalMs,
      maxPolls,
      maxBodyBytes,
      onProgress: input.onProgress,
      scanBodyForRelayBootstrapHello: input.scanBodyForRelayBootstrapHello ?? false,
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
  onProgress?: (progress: CodexExecProgress) => void | Promise<void>;
  scanBodyForRelayBootstrapHello: boolean;
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
    await input.onProgress?.(summarizeCodexProgress(
      body,
      polls + 1,
      input.scanBodyForRelayBootstrapHello,
    ));
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
    return {
      transcript,
      requestId: input.requestId,
      polls: polls + 1,
      identifiers: boundedIdentifiers(body),
      listenSnapshot: listenSnapshotOf(body),
    };
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
