import {
  boundedAccountRequest,
  type AccountRequestClient,
} from "../prism/provisioning-client";
import {
  CodexExecError,
  runCodexCommand,
  type CodexExecContinuation,
} from "../prism/codex-exec";
import { createProjectConversation } from "../prism/conversation-client";
import {
  DIRECT_SUPERVISOR_EXEC_PATH,
  DIRECT_SUPERVISOR_EXEC_RESPONSE_BYTES,
  directSupervisorExecBody,
  parseDirectSupervisorExecStarted,
  type DirectSupervisorExecRequest,
  type DirectSupervisorExecStarted,
} from "../prism/direct-exec";
import {
  supervisorInstallCommand,
  type SupervisorExecRequest,
} from "../v2/supervisor/launcher";
import type { ProvisionedSandbox } from "../prism/protocol";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { ProjectId } from "./types";

export interface SandboxGatewayOptions {
  projectId: ProjectId;
  account: AccountRequestClient;
  sandbox: ProvisionedSandbox;
  clock?: Clock;
  syncBatchMs?: number;
  requestTimeoutMs?: number;
  responseBodyLimitBytes?: number;
  /** Maximum Codex status polls per relay exec (default 18, roughly 90 s). */
  relayExecMaxPolls?: number;
  /** Delay between Codex status polls (default 5 s). */
  relayExecPollIntervalMs?: number;
}

export interface HeartbeatResult {
  ok: boolean;
  status: number;
  durationMs: number;
  sessionId: string | null;
}

export interface RenderRequest {
  mainDocument: string;
  clientStateVector: string;
  clientDeleteSetUpdate: string;
}

export class SandboxGatewayError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly operation: string,
  ) {
    super(message);
    this.name = "SandboxGatewayError";
  }
}

function signalAbortError(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("Sandbox request aborted", "AbortError");
}

export class SandboxGateway {
  readonly projectId: ProjectId;
  readonly #account: AccountRequestClient;
  readonly #clock: Clock;
  readonly #syncBatchMs: number;
  readonly #requestTimeoutMs: number;
  readonly #responseBodyLimitBytes: number;
  #sandbox: ProvisionedSandbox;
  #effectiveSessionId: string | null;
  #relayExecMaxPolls: number;
  #relayExecPollIntervalMs: number;
  #cacheBust = 0;
  #syncRequestedEpoch = 0;
  #syncCompletedEpoch = 0;
  #syncLoop: Promise<void> | null = null;

  constructor(options: SandboxGatewayOptions) {
    this.projectId = options.projectId;
    this.#account = options.account;
    this.#sandbox = { ...options.sandbox };
    this.#effectiveSessionId = options.sandbox.sessionId ?? null;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#syncBatchMs = options.syncBatchMs ?? 5;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 15_000;
    this.#responseBodyLimitBytes = options.responseBodyLimitBytes ?? 8 * 1024 * 1024;
    this.#relayExecMaxPolls = options.relayExecMaxPolls ?? 18;
    this.#relayExecPollIntervalMs = options.relayExecPollIntervalMs ?? 5_000;
    if (!Number.isFinite(this.#syncBatchMs) || this.#syncBatchMs < 0 || this.#syncBatchMs > 1_000) {
      throw new RangeError("syncBatchMs must be between 0 and 1000");
    }
    if (!Number.isSafeInteger(this.#requestTimeoutMs) || this.#requestTimeoutMs < 1) {
      throw new RangeError("requestTimeoutMs must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.#responseBodyLimitBytes) || this.#responseBodyLimitBytes < 1) {
      throw new RangeError("responseBodyLimitBytes must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.#relayExecMaxPolls) || this.#relayExecMaxPolls < 1 || this.#relayExecMaxPolls > 64) {
      throw new RangeError("relayExecMaxPolls must be an integer from 1 through 64");
    }
    if (!Number.isSafeInteger(this.#relayExecPollIntervalMs) || this.#relayExecPollIntervalMs < 1 || this.#relayExecPollIntervalMs > 30_000) {
      throw new RangeError("relayExecPollIntervalMs must be an integer from 1 through 30000");
    }
  }

  async heartbeat(signal?: AbortSignal): Promise<HeartbeatResult> {
    const started = this.#clock.now();
    const response = await this.#request("heartbeat", { method: "GET", signal }, false);
    const sessionId = this.#headerSessionId(response);
    if (
      response.ok &&
      sessionId !== null &&
      this.#effectiveSessionId !== null &&
      sessionId !== this.#effectiveSessionId
    ) {
      // A live gateway silently switching sessions invalidates the V2 binding.
      // Fail closed and let the supervisor create a freshly fenced generation
      // instead of adopting it in place.
      return {
        ok: false,
        status: 409,
        durationMs: Math.max(0, this.#clock.now() - started),
        sessionId,
      };
    }
    if (response.ok && sessionId !== null) this.#effectiveSessionId = sessionId;
    return {
      ok: response.ok,
      status: response.status,
      durationMs: Math.max(0, this.#clock.now() - started),
      sessionId,
    };
  }

  /**
   * Atomically replaces credentials only when the candidate is known to
   * address the currently-running sandbox session. URLs and missing identity
   * fields are deliberately never treated as proof of identity.
   */
  rotateSameSession(candidate: ProvisionedSandbox): boolean {
    const currentSessionId = this.#effectiveSessionId;
    const candidateSessionId = candidate.sessionId ?? null;
    if (currentSessionId === null || candidateSessionId === null || candidateSessionId !== currentSessionId) {
      return false;
    }

    const current = this.#sandbox;
    if (
      current.sandboxId !== undefined &&
      candidate.sandboxId !== undefined &&
      current.sandboxId !== candidate.sandboxId
    ) return false;
    if (
      current.sessionFirstSeenAtMs !== undefined &&
      candidate.sessionFirstSeenAtMs !== undefined &&
      current.sessionFirstSeenAtMs !== candidate.sessionFirstSeenAtMs
    ) return false;

    this.#sandbox = {
      ...candidate,
      sandboxId: candidate.sandboxId ?? current.sandboxId,
      sessionId: candidate.sessionId,
      sessionFirstSeenAtMs: candidate.sessionFirstSeenAtMs ?? current.sessionFirstSeenAtMs,
    };
    this.#effectiveSessionId = candidateSessionId;
    return true;
  }

  async waitForSync(signal?: AbortSignal): Promise<{ response: Response; body: unknown }> {
    const response = await this.#request("wait-for-sync", { method: "GET", signal });
    let body: unknown = null;
    try { body = await response.json(); } catch {}
    return { response, body };
  }

  async syncWorkspace(signal?: AbortSignal): Promise<void> {
    const targetEpoch = ++this.#syncRequestedEpoch;
    while (this.#syncCompletedEpoch < targetEpoch) {
      if (signal?.aborted) throw new DOMException("Workspace synchronization aborted", "AbortError");
      if (!this.#syncLoop) {
        const tracked = this.#drainWorkspaceSync().finally(() => {
          if (this.#syncLoop === tracked) this.#syncLoop = null;
        });
        this.#syncLoop = tracked;
      }
      await this.#awaitSharedSync(this.#syncLoop, signal);
    }
  }

  async fetchText(suffix: string, init: RequestInit = {}): Promise<{ status: number; text: string; headers: Headers }> {
    const response = await this.#request(suffix, init);
    return { status: response.status, text: await response.text(), headers: response.headers };
  }

  async fetchBytes(
    suffix: string,
    init: RequestInit = {},
  ): Promise<{ status: number; bytes: Uint8Array; headers: Headers }> {
    const response = await this.#request(suffix, init);
    return {
      status: response.status,
      bytes: new Uint8Array(await response.arrayBuffer()),
      headers: response.headers,
    };
  }

  async postJson(suffix: string, json: unknown, signal?: AbortSignal): Promise<Response> {
    return this.#request(suffix, {
      method: "POST",
      signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(json),
    });
  }

  async triggerRender(request: RenderRequest, signal?: AbortSignal): Promise<void> {
    const response = await this.#request("render?renderMode=async&renderStatusMode=json", {
      method: "POST",
      signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
    }, false);
    if (response.status !== 202) {
      throw new SandboxGatewayError(`Render returned ${response.status}`, response.status, "render");
    }
  }

  /** One command submission; errors and ambiguous sends never start another job. */
  async startSupervisorExec(
    request: DirectSupervisorExecRequest,
    signal?: AbortSignal,
  ): Promise<DirectSupervisorExecStarted> {
    const body = directSupervisorExecBody(request);
    const response = await this.#request(DIRECT_SUPERVISOR_EXEC_PATH, {
      method: "POST",
      signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }, true, Math.min(this.#responseBodyLimitBytes, DIRECT_SUPERVISOR_EXEC_RESPONSE_BYTES));
    if (response.status !== 200) {
      throw new SandboxGatewayError("Direct supervisor execution was rejected", response.status, "supervisor-exec");
    }
    try {
      const value: unknown = await response.json();
      return parseDirectSupervisorExecStarted(value, body.session_id);
    } catch {
      throw new SandboxGatewayError("Direct supervisor execution identity is invalid", 502, "supervisor-exec");
    }
  }

  /**
   * Relay-mode hello acquisition: executes the staged relay launcher inside
   * the sandbox container through one platform Codex turn and returns the
   * launcher's printed transcript. The caller extracts and validates the
   * hello block. Bounded to roughly 90 seconds; at most one turn per call.
   */
  async runRelayExec(
    request: SupervisorExecRequest,
    continuation: CodexExecContinuation,
    signal?: AbortSignal,
  ): Promise<{ status: number; output: string }> {
    const sandbox = this.#sandbox;
    try {
      const result = await runCodexCommand({
        account: this.#account,
        projectId: this.projectId,
        sandboxUrl: sandbox.url,
        sandboxToken: sandbox.token.reveal(),
        conversationId: continuation.conversationId,
        previousResponseId: continuation.previousResponseId,
        listenSnapshot: continuation.listenSnapshot,
        command: `Execute exactly this shell command and reply with ONLY its complete stdout verbatim: ${supervisorInstallCommand(request)}`,
        maxPolls: this.#relayExecMaxPolls,
        pollIntervalMs: this.#relayExecPollIntervalMs,
        maxStartAttempts: 1,
        signal,
      });
      return { status: 200, output: result.transcript };
    } catch (error) {
      if (error instanceof CodexExecError) {
        throw new SandboxGatewayError(
          `Relay exec ${error.kind}`,
          error.status ?? (error.kind === "timeout" ? 408 : 502),
          "relay-exec",
        );
      }
      throw error;
    }
  }

  async prepareRelayExecSession(signal?: AbortSignal): Promise<CodexExecContinuation> {
    const sandbox = this.#sandbox;
    const conversationId = await createProjectConversation(this.#account, this.projectId, signal);
    let startedSessionId: string | null = null;
    const result = await runCodexCommand({
      account: this.#account,
      projectId: this.projectId,
      sandboxUrl: sandbox.url,
      sandboxToken: sandbox.token.reveal(),
      conversationId,
      command: "Execute exactly this shell command and reply with ONLY its complete stdout verbatim: printf 'GPV2_SESSION_READY\\n'",
      maxPolls: this.#relayExecMaxPolls,
      pollIntervalMs: this.#relayExecPollIntervalMs,
      maxStartAttempts: 1,
      signal,
      onStarted: started => {
        startedSessionId = started.identifiers["turn_state.codex_session_id"] ?? null;
      },
    });
    const returnedConversationId = result.identifiers["response.payload.conversationId"];
    const previousResponseId = result.identifiers["response.payload.id"];
    const listenSnapshot = result.listenSnapshot;
    const codexSessionId = listenSnapshot?.codex_session_id ?? startedSessionId;
    if (
      !result.transcript.includes("GPV2_SESSION_READY") ||
      returnedConversationId !== conversationId ||
      !previousResponseId ||
      !listenSnapshot ||
      listenSnapshot.conversation_id !== conversationId ||
      (listenSnapshot.project_id !== null && listenSnapshot.project_id !== this.projectId) ||
      !codexSessionId ||
      codexSessionId !== startedSessionId
    ) {
      throw new SandboxGatewayError("Relay session proof is incomplete", 502, "relay-session");
    }
    return Object.freeze({
      conversationId,
      previousResponseId,
      listenSnapshot,
      codexSessionId,
    });
  }

  async #request(
    suffix: string,
    init: RequestInit,
    bufferResponseBody = true,
    responseBodyLimitBytes = this.#responseBodyLimitBytes,
  ): Promise<Response> {
    // A request owns one immutable credential snapshot. Rotation may happen
    // while the request is in flight, but can only affect later requests.
    const sandbox = this.#sandbox;
    const delimiter = suffix.includes("?") ? "&" : "?";
    const path = `${sandbox.url}${suffix}${delimiter}prism_cache_bust=gp${Date.now().toString(36)}${this.#cacheBust++}`;
    const headers = new Headers(init.headers);
    headers.set("x-crixet-sandbox-token", sandbox.token.reveal());
    const operation = suffix.split("?", 1)[0] ?? "request";
    const timeoutError = () => new SandboxGatewayError(
      `Sandbox ${operation} request timed out`,
      408,
      operation,
    );
    return boundedAccountRequest({
      account: this.#account,
      path,
      init: { ...init, headers },
      timeoutMs: this.#requestTimeoutMs,
      responseBodyLimitBytes,
      bodyAdmissionKey: [
        "sandbox",
        sandbox.sessionId ?? sandbox.sandboxId ?? this.projectId,
        (init.method ?? "GET").toUpperCase(),
        suffix,
      ].join(":"),
      shouldBufferResponseBody: () => bufferResponseBody,
      timeoutError,
      responseBodyLimitError: () => new SandboxGatewayError(
        `Sandbox ${operation} response body exceeded the bounded limit`,
        502,
        operation,
      ),
      responseBodyReadError: () => new SandboxGatewayError(
        `Sandbox ${operation} response body could not be read`,
        502,
        operation,
      ),
      abortError: signalAbortError,
    });
  }

  #headerSessionId(response: Response): string | null {
    const value = response.headers.get("x-session-id")?.trim();
    return value ? value : null;
  }

  async #drainWorkspaceSync(): Promise<void> {
    while (this.#syncCompletedEpoch < this.#syncRequestedEpoch) {
      await this.#clock.sleep(this.#syncBatchMs);
      const targetEpoch = this.#syncRequestedEpoch;
      await this.#syncWorkspaceOnce();
      this.#syncCompletedEpoch = Math.max(this.#syncCompletedEpoch, targetEpoch);
    }
  }

  async #syncWorkspaceOnce(): Promise<void> {
    const deadline = this.#clock.now() + 30_000;
    let transientFailures = 0;
    for (;;) {
      const { response, body } = await this.waitForSync();
      if (!response.ok) {
        const retryable = response.status === 429 || response.status >= 500;
        if (retryable && this.#clock.now() < deadline) {
          transientFailures++;
          await this.#clock.sleep(Math.min(
            2_000,
            100 * 2 ** Math.min(5, transientFailures - 1),
            deadline - this.#clock.now(),
          ));
          continue;
        }
        throw new SandboxGatewayError(
          `Workspace synchronization returned ${response.status}`,
          response.status,
          "wait-for-sync",
        );
      }
      const value = body && typeof body === "object" ? body as Record<string, any> : {};
      const tokens = value.tokens && typeof value.tokens === "object" ? value.tokens as Record<string, unknown> : {};
      const capabilities = Array.isArray(value.readinessCapabilities)
        ? value.readinessCapabilities.filter((item: unknown) => typeof item === "string") as string[]
        : [];
      const currentProvider = capabilities.includes("current_y_sweet_provider");
      if (
        value.status === "synced" &&
        tokens.hasCurrentYSweetToken === true &&
        (!currentProvider || tokens.hasSyncedYSweetProvider === true)
      ) return;
      if (value.status !== "syncing" && value.status !== "synced") {
        throw new SandboxGatewayError("Invalid workspace synchronization state", 409, "wait-for-sync");
      }
      if (this.#clock.now() >= deadline) {
        throw new SandboxGatewayError("Workspace synchronization timed out", 408, "wait-for-sync");
      }
      await this.#clock.sleep(Math.min(50, deadline - this.#clock.now()));
    }
  }

  async #awaitSharedSync(sync: Promise<void>, signal?: AbortSignal): Promise<void> {
    if (!signal) return sync;
    if (signal.aborted) throw new DOMException("Workspace synchronization aborted", "AbortError");
    let abort!: () => void;
    const aborted = new Promise<never>((_resolve, reject) => {
      abort = () => reject(new DOMException("Workspace synchronization aborted", "AbortError"));
      signal.addEventListener("abort", abort, { once: true });
    });
    try {
      await Promise.race([sync, aborted]);
    } finally {
      signal.removeEventListener("abort", abort);
    }
  }

}
