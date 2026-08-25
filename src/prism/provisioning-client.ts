import {
  AccountAuthError,
  type PrismAccountSession,
} from "./account-session";
import {
  parseProvisionedSandbox,
  parseResourceCredential,
  parseYSweetEnvelope,
  type ProvisionedSandbox,
  type ResourceCredential,
  type YSweetEnvelope,
} from "./protocol";
import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "../core/ports";
import type { ProjectId } from "../sandbox/types";

export interface AccountRequestClient {
  request(pathOrUrl: string, init?: RequestInit): Promise<Response>;
}

export interface BoundedAccountRequestOptions {
  account: AccountRequestClient;
  path: string;
  init: RequestInit;
  timeoutMs: number;
  responseBodyLimitBytes: number;
  bodyAdmissionKey: string;
  shouldBufferResponseBody: (response: Response) => boolean;
  timeoutError: () => unknown;
  responseBodyLimitError: (response: Response) => unknown;
  responseBodyReadError: (response: Response, cause: unknown) => unknown;
  abortError: (signal: AbortSignal) => unknown;
}

export interface YSweetRequestContext {
  source: string;
  bootstrapAttempt: number;
  previouslyConnected: boolean;
  reprovisionSource?: string | null;
  sandboxUrl?: string | null;
  sandboxId?: string | null;
  sandboxSessionId?: string | null;
}

export interface ProvisionedRuntimeCredentials {
  projectId: ProjectId;
  sandbox: ProvisionedSandbox;
  resource: ResourceCredential;
  ySweet: YSweetEnvelope;
  provisionedAtMs: number;
}

export interface ProvisioningHooks {
  onSandboxAcquired?: (sandbox: ProvisionedSandbox) => void | Promise<void>;
}

export interface AcquireSandboxOptions {
  /**
   * Number of attempts made for retryable Prism responses. The default of
   * three preserves the original provisioning behaviour while allowing a
   * supervisor to opt into a single, externally-coordinated attempt.
   */
  maxAttempts?: number;
}

const DEFAULT_ACQUIRE_MAX_ATTEMPTS = 3;
const MAX_ACQUIRE_MAX_ATTEMPTS = 10;
const FAILED_SANDBOX_THRESHOLD = 2;
const FAILED_SANDBOX_QUARANTINE_MS = 5 * 60_000;
const MAX_FAILED_SANDBOX_IDENTITIES = 256;

export type ProvisioningErrorKind =
  | "auth"
  | "reprovision"
  | "rate_limit"
  | "protocol"
  | "timeout"
  | "transport";

export class ProvisioningError extends Error {
  constructor(
    message: string,
    readonly kind: ProvisioningErrorKind,
    readonly status?: number,
    readonly retryAfterMs?: number,
  ) {
    super(message);
    this.name = "ProvisioningError";
  }
}

export interface PrismProvisioningClientOptions {
  account: AccountRequestClient | PrismAccountSession;
  clock?: Clock;
  random?: RandomSource;
  workspaceSyncTimeoutMs?: number;
  readinessPollMs?: number;
  requestTimeoutMs?: number;
  responseBodyLimitBytes?: number;
}

const DEFAULT_PROVISIONING_RESPONSE_BODY_LIMIT_BYTES = 1024 * 1024;
const MAX_RESPONSE_BODY_CHUNKS = 16_384;
const MAX_CONSECUTIVE_EMPTY_BODY_CHUNKS = 16;

function abortError(): DOMException {
  return new DOMException("Provisioning aborted", "AbortError");
}

function signalAbortError(signal: AbortSignal): unknown {
  return signal.reason ?? abortError();
}

function monotonicNowMs(): number {
  return globalThis.performance?.now() ?? Date.now();
}

async function discardResponse(response: Response, reason?: unknown): Promise<void> {
  try {
    await response.body?.cancel(reason);
  } catch {}
}

class ResponseBodyLimitExceeded extends Error {
  constructor(readonly response: Response) {
    super("Account response body exceeded its bounded limit");
    this.name = "ResponseBodyLimitExceeded";
  }
}

class ResponseBodyReadFailed extends Error {
  constructor(readonly response: Response, options: ErrorOptions) {
    super("Account response body could not be read", options);
    this.name = "ResponseBodyReadFailed";
  }
}

const ABANDONED_RAW_ACCOUNT_OPERATIONS = new WeakMap<
  AccountRequestClient,
  Set<Promise<unknown>>
>();
const ABANDONED_BODY_OPERATIONS = new WeakMap<
  AccountRequestClient,
  Map<string, Set<Promise<unknown>>>
>();

function hasAbandonedRawAccountOperation(account: AccountRequestClient): boolean {
  return (ABANDONED_RAW_ACCOUNT_OPERATIONS.get(account)?.size ?? 0) > 0;
}

function trackAbandonedRawAccountOperation(
  account: AccountRequestClient,
  operation: Promise<unknown>,
): void {
  let operations = ABANDONED_RAW_ACCOUNT_OPERATIONS.get(account);
  if (!operations) {
    operations = new Set();
    ABANDONED_RAW_ACCOUNT_OPERATIONS.set(account, operations);
  }
  if (operations.has(operation)) return;
  operations.add(operation);
  const release = () => {
    operations!.delete(operation);
    if (operations!.size === 0) ABANDONED_RAW_ACCOUNT_OPERATIONS.delete(account);
  };
  void operation.then(release, release);
}

function hasAbandonedBodyOperation(
  account: AccountRequestClient,
  key: string,
): boolean {
  return (ABANDONED_BODY_OPERATIONS.get(account)?.get(key)?.size ?? 0) > 0;
}

function trackAbandonedBodyOperation(
  account: AccountRequestClient,
  key: string,
  operation: Promise<unknown>,
): void {
  let byKey = ABANDONED_BODY_OPERATIONS.get(account);
  if (!byKey) {
    byKey = new Map();
    ABANDONED_BODY_OPERATIONS.set(account, byKey);
  }
  let operations = byKey.get(key);
  if (!operations) {
    operations = new Set();
    byKey.set(key, operations);
  }
  if (operations.has(operation)) return;
  operations.add(operation);
  const release = () => {
    operations!.delete(operation);
    if (operations!.size > 0) return;
    byKey!.delete(key);
    if (byKey!.size === 0) ABANDONED_BODY_OPERATIONS.delete(account);
  };
  void operation.then(release, release);
}

function reconstructedResponse(response: Response, body: ArrayBuffer | null): Response {
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
}

async function readBoundedResponse(
  response: Response,
  limitBytes: number,
  signal: AbortSignal,
  deadlineAtMs: number,
): Promise<Response> {
  if (!response.body) return reconstructedResponse(response, null);
  const declaredLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > limitBytes) {
    await discardResponse(response, new Error("Response body limit exceeded"));
    throw new ResponseBodyLimitExceeded(response);
  }

  const reader = response.body.getReader();
  let cancellation: Promise<void> | null = null;
  const beginCancellation = (reason: unknown) => {
    if (cancellation) return;
    try {
      cancellation = Promise.resolve(reader.cancel(reason)).then(
        () => undefined,
        () => undefined,
      );
    } catch {
      cancellation = Promise.resolve();
    }
  };
  const onAbort = () => beginCancellation(signal.reason);
  signal.addEventListener("abort", onAbort, { once: true });
  if (signal.aborted) onAbort();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let chunkCount = 0;
  let consecutiveEmptyChunks = 0;
  try {
    for (;;) {
      if (signal.aborted || monotonicNowMs() >= deadlineAtMs) {
        beginCancellation(signal.reason ?? new DOMException("Request timed out", "TimeoutError"));
        throw signal.reason ?? new DOMException("Request timed out", "TimeoutError");
      }
      const { done, value } = await reader.read();
      if (done) break;
      if (signal.aborted || monotonicNowMs() >= deadlineAtMs) {
        beginCancellation(signal.reason ?? new DOMException("Request timed out", "TimeoutError"));
        throw signal.reason ?? new DOMException("Request timed out", "TimeoutError");
      }
      chunkCount += 1;
      if (chunkCount > MAX_RESPONSE_BODY_CHUNKS) {
        beginCancellation(new Error("Response body chunk limit exceeded"));
        throw new ResponseBodyLimitExceeded(response);
      }
      if (value.byteLength === 0) {
        consecutiveEmptyChunks += 1;
        if (consecutiveEmptyChunks > MAX_CONSECUTIVE_EMPTY_BODY_CHUNKS) {
          beginCancellation(new Error("Response body made no progress"));
          throw new ResponseBodyLimitExceeded(response);
        }
        continue;
      }
      consecutiveEmptyChunks = 0;
      if (total + value.byteLength > limitBytes) {
        beginCancellation(new Error("Response body limit exceeded"));
        throw new ResponseBodyLimitExceeded(response);
      }
      // Copy each accepted view so a tiny slice cannot retain an arbitrarily
      // large producer-owned ArrayBuffer behind the byte cap.
      chunks.push(Uint8Array.from(value));
      total += value.byteLength;
    }
  } catch (error) {
    if (error instanceof ResponseBodyLimitExceeded) throw error;
    throw new ResponseBodyReadFailed(response, { cause: error });
  } finally {
    signal.removeEventListener("abort", onAbort);
    try { reader.releaseLock(); } catch {}
    if (cancellation) await cancellation;
  }

  if (total === 0) return reconstructedResponse(response, null);
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return reconstructedResponse(response, body.buffer);
}

/**
 * Runs the complete account operation (headers plus a bounded body read) under
 * one deadline. An abandoned raw AccountSession operation fences the whole
 * client because its cookie lock is shared. Once headers have settled, only
 * the exact sandbox/path lane is fenced, so one broken logs body cannot freeze
 * unrelated sandboxes and heartbeats. Calls admitted concurrently before a
 * timeout cannot be revoked, but later retry generations cannot add waiters.
 *
 * @internal
 */
export async function boundedAccountRequest(
  options: BoundedAccountRequestOptions,
): Promise<Response> {
  const callerSignal = options.init.signal ?? undefined;
  if (callerSignal?.aborted) throw options.abortError(callerSignal);
  if (hasAbandonedRawAccountOperation(options.account)) throw options.timeoutError();
  if (hasAbandonedBodyOperation(options.account, options.bodyAdmissionKey)) {
    throw options.timeoutError();
  }

  const startedAtMs = monotonicNowMs();
  const timeout = AbortSignal.timeout(options.timeoutMs);
  const signal = callerSignal
    ? AbortSignal.any([callerSignal, timeout])
    : timeout;

  type Outcome =
    | { readonly type: "response"; readonly response: Response }
    | { readonly type: "error"; readonly error: unknown }
    | { readonly type: "discarded" };

  let state: "pending" | "settled" | "abandoned" = "pending";
  let rawRequest: Promise<Response> | null = null;
  let rawRequestSettled = false;
  let operation: Promise<Response> | null = null;
  let operationSettled = false;
  const isAbandoned = () => state === "abandoned";
  let resolveInterruption!: (outcome: Outcome) => void;
  const interruption = new Promise<Outcome>(resolve => { resolveInterruption = resolve; });
  const interruptionError = (): unknown => callerSignal?.aborted
    ? options.abortError(callerSignal)
    : options.timeoutError();
  const abandon = (error: unknown) => {
    if (state !== "pending") return;
    state = "abandoned";
    if (rawRequest && !rawRequestSettled) {
      trackAbandonedRawAccountOperation(options.account, rawRequest);
    } else if (operation && !operationSettled) {
      trackAbandonedBodyOperation(
        options.account,
        options.bodyAdmissionKey,
        operation,
      );
    }
    resolveInterruption({ type: "error", error });
  };
  const onTimeout = () => abandon(interruptionError());
  const onCallerAbort = () => {
    if (callerSignal) abandon(options.abortError(callerSignal));
  };
  timeout.addEventListener("abort", onTimeout, { once: true });
  callerSignal?.addEventListener("abort", onCallerAbort, { once: true });
  if (timeout.aborted) onTimeout();
  if (callerSignal?.aborted) onCallerAbort();

  let startedRequest: Promise<Response>;
  try {
    startedRequest = Promise.resolve(options.account.request(
      options.path,
      { ...options.init, signal },
    ));
  } catch (error) {
    startedRequest = Promise.reject(error);
  }
  rawRequest = startedRequest.then(
    response => {
      rawRequestSettled = true;
      return response;
    },
    error => {
      rawRequestSettled = true;
      throw error;
    },
  );
  operation = rawRequest.then(response => {
    if (isAbandoned()) {
      // The raw AccountSession operation has now released its shared lane.
      // Move any non-cooperative body cleanup to this exact response lane.
      trackAbandonedBodyOperation(
        options.account,
        options.bodyAdmissionKey,
        operation!,
      );
      return discardResponse(response, signal.reason).then(() => {
        throw new Error("Account response arrived after request abandonment");
      });
    }
    if (!options.shouldBufferResponseBody(response)) {
      const buffered = reconstructedResponse(response, null);
      if (response.body) {
        const cleanup = discardResponse(response, signal.reason);
        trackAbandonedBodyOperation(
          options.account,
          options.bodyAdmissionKey,
          cleanup,
        );
      }
      return buffered;
    }
    return readBoundedResponse(
      response,
      options.responseBodyLimitBytes,
      signal,
      startedAtMs + options.timeoutMs,
    );
  });
  if (isAbandoned() && !operationSettled) {
    if (!rawRequestSettled) {
      trackAbandonedRawAccountOperation(options.account, rawRequest);
    } else {
      trackAbandonedBodyOperation(
        options.account,
        options.bodyAdmissionKey,
        operation,
      );
    }
  }

  const request = operation.then<Outcome, Outcome>(
    response => {
      operationSettled = true;
      if (state === "abandoned") {
        void discardResponse(response, signal.reason);
        return { type: "discarded" };
      }
      if (
        timeout.aborted ||
        monotonicNowMs() - startedAtMs >= options.timeoutMs
      ) {
        state = "abandoned";
        void discardResponse(response, signal.reason);
        return { type: "error", error: interruptionError() };
      }
      state = "settled";
      return { type: "response", response };
    },
    error => {
      operationSettled = true;
      if (state === "abandoned") return { type: "discarded" };
      if (
        !callerSignal?.aborted &&
        (timeout.aborted || monotonicNowMs() - startedAtMs >= options.timeoutMs)
      ) {
        state = "abandoned";
        return { type: "error", error: options.timeoutError() };
      }
      state = "settled";
      if (error instanceof ResponseBodyLimitExceeded) {
        return { type: "error", error: options.responseBodyLimitError(error.response) };
      }
      if (error instanceof ResponseBodyReadFailed) {
        return {
          type: "error",
          error: options.responseBodyReadError(error.response, error.cause),
        };
      }
      return { type: "error", error };
    },
  );

  try {
    const outcome = await Promise.race([request, interruption]);
    if (outcome.type === "response") return outcome.response;
    if (outcome.type === "error") throw outcome.error;
    // A discarded operation can only settle after its interruption already
    // won the race. Retain deterministic timeout/caller classification.
    throw interruptionError();
  } finally {
    timeout.removeEventListener("abort", onTimeout);
    callerSignal?.removeEventListener("abort", onCallerAbort);
  }
}

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) throw signalAbortError(signal);
}

function acquireMaxAttempts(options?: AcquireSandboxOptions): number {
  const value = options?.maxAttempts ?? DEFAULT_ACQUIRE_MAX_ATTEMPTS;
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_ACQUIRE_MAX_ATTEMPTS) {
    throw new RangeError(`maxAttempts must be an integer between 1 and ${MAX_ACQUIRE_MAX_ATTEMPTS}`);
  }
  return value;
}

function retryAfterMs(response: Response, fallback: number): number {
  const header = response.headers.get("retry-after");
  if (!header) return fallback;
  const seconds = Number(header);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(300_000, seconds * 1000);
  const date = Date.parse(header);
  return Number.isNaN(date) ? fallback : Math.max(0, Math.min(300_000, date - Date.now()));
}

function sandboxPath(sandbox: ProvisionedSandbox, suffix: string): string {
  return `${sandbox.url}${suffix.replace(/^\/+/, "")}`;
}

function sandboxIdentityKey(sandbox: ProvisionedSandbox): string | null {
  if (sandbox.sessionId) return `session:${sandbox.sessionId}`;
  if (sandbox.sandboxId) {
    return sandbox.sessionFirstSeenAtMs === undefined
      ? `sandbox:${sandbox.sandboxId}`
      : `sandbox:${sandbox.sandboxId}:${sandbox.sessionFirstSeenAtMs}`;
  }
  return null;
}

function sandboxBodyAdmissionKey(
  sandbox: ProvisionedSandbox,
  method: string,
  suffix: string,
): string {
  return [
    "sandbox",
    sandbox.sessionId ?? sandbox.sandboxId ?? sandbox.url,
    method,
    suffix,
  ].join(":");
}

function resourceBaseFromSandbox(url: string): string {
  const withoutSlash = url.replace(/\/+$/, "");
  const replaced = withoutSlash.replace(/\/s\/sandboxes\/proxy(?:\/.*)?$/, "/s/sandbox-resources");
  return `${replaced}/`;
}

function ySweetJson(envelope: YSweetEnvelope): Record<string, unknown> {
  return {
    url: envelope.url,
    baseUrl: envelope.baseUrl,
    docId: envelope.docId,
    token: envelope.token.reveal(),
    authorization: envelope.authorization,
  };
}

async function parseJsonResponse(response: Response, label: string): Promise<Record<string, unknown>> {
  try {
    const value = await response.json();
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("not object");
    return value as Record<string, unknown>;
  } catch {
    throw new ProvisioningError(`${label} returned invalid JSON`, "protocol", response.status);
  }
}

export class PrismProvisioningClient {
  readonly #account: AccountRequestClient;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  readonly #workspaceSyncTimeoutMs: number;
  readonly #readinessPollMs: number;
  readonly #requestTimeoutMs: number;
  readonly #responseBodyLimitBytes: number;
  readonly #failedSandboxes = new Map<string, {
    failures: number;
    quarantinedUntilMs: number;
    updatedAtMs: number;
  }>();

  constructor(options: PrismProvisioningClientOptions) {
    this.#account = options.account;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#workspaceSyncTimeoutMs = options.workspaceSyncTimeoutMs ?? 30_000;
    this.#readinessPollMs = options.readinessPollMs ?? 250;
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 30_000;
    this.#responseBodyLimitBytes = options.responseBodyLimitBytes ??
      DEFAULT_PROVISIONING_RESPONSE_BODY_LIMIT_BYTES;
    if (!Number.isSafeInteger(this.#requestTimeoutMs) || this.#requestTimeoutMs < 1) {
      throw new RangeError("requestTimeoutMs must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.#responseBodyLimitBytes) || this.#responseBodyLimitBytes < 1) {
      throw new RangeError("responseBodyLimitBytes must be a positive safe integer");
    }
  }

  async provision(
    projectId: ProjectId,
    signal?: AbortSignal,
    hooks: ProvisioningHooks = {},
  ): Promise<ProvisionedRuntimeCredentials> {
    const sandbox = await this.acquireSandbox(signal);
    this.#assertSandboxSessionAllowed(projectId, sandbox);
    return this.provisionAcquired(projectId, sandbox, signal, hooks);
  }

  async acquireSandbox(
    signal?: AbortSignal,
    options?: AcquireSandboxOptions,
  ): Promise<ProvisionedSandbox> {
    checkAbort(signal);
    const sandboxResponse = await this.#requestWithRetry(
      "/api/backend/1/new",
      { method: "POST", signal },
      acquireMaxAttempts(options),
      signal,
    );
    return parseProvisionedSandbox(await parseJsonResponse(sandboxResponse, "Sandbox acquisition"));
  }

  async provisionAcquired(
    projectId: ProjectId,
    sandbox: ProvisionedSandbox,
    signal?: AbortSignal,
    hooks: ProvisioningHooks = {},
  ): Promise<ProvisionedRuntimeCredentials> {
    checkAbort(signal);
    this.#assertSandboxSessionAllowed(projectId, sandbox);
    const provisionedAtMs = this.#clock.now();
    await hooks.onSandboxAcquired?.(sandbox);

    const resource = await this.#syncResources(projectId, sandbox, signal);
    const context: YSweetRequestContext = {
      source: "initial-bootstrap",
      bootstrapAttempt: 1,
      previouslyConnected: false,
      reprovisionSource: null,
      sandboxUrl: sandbox.url,
      sandboxId: sandbox.sandboxId ?? null,
      sandboxSessionId: sandbox.sessionId ?? null,
    };
    const ySweet = await this.mintYSweet(projectId, context, signal);
    await this.#sandboxJson(
      sandbox,
      "token",
      ySweetJson(ySweet),
      signal,
    );
    await this.waitForWorkspaceSync(sandbox, signal);
    return { projectId, sandbox, resource, ySweet, provisionedAtMs };
  }

  /**
   * Records one generation-level failure against a stable sandbox identity.
   * The first failure is allowed to retry in place; repeated failures fence
   * that exact session for a bounded period while `/new` may return a fresh
   * sandbox for the same Prism project.
   */
  reportSandboxFailure(projectId: ProjectId, sandbox: ProvisionedSandbox): void {
    const identity = sandboxIdentityKey(sandbox);
    if (!identity) return;
    const now = this.#clock.now();
    this.#pruneFailedSandboxes(now);
    const key = `${projectId}:${identity}`;
    const previous = this.#failedSandboxes.get(key);
    const failures = (previous?.failures ?? 0) + 1;
    this.#failedSandboxes.delete(key);
    this.#failedSandboxes.set(key, {
      failures,
      quarantinedUntilMs: failures >= FAILED_SANDBOX_THRESHOLD
        ? now + FAILED_SANDBOX_QUARANTINE_MS
        : 0,
      updatedAtMs: now,
    });
    while (this.#failedSandboxes.size > MAX_FAILED_SANDBOX_IDENTITIES) {
      const oldest = this.#failedSandboxes.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.#failedSandboxes.delete(oldest);
    }
  }

  reportSandboxReady(projectId: ProjectId, sandbox: ProvisionedSandbox): void {
    const identity = sandboxIdentityKey(sandbox);
    if (!identity) return;
    this.#failedSandboxes.delete(`${projectId}:${identity}`);
  }

  #assertSandboxSessionAllowed(projectId: ProjectId, sandbox: ProvisionedSandbox): void {
    const identity = sandboxIdentityKey(sandbox);
    if (!identity) return;
    const now = this.#clock.now();
    this.#pruneFailedSandboxes(now);
    const record = this.#failedSandboxes.get(`${projectId}:${identity}`);
    if (!record || record.quarantinedUntilMs <= now) return;
    throw new ProvisioningError(
      "Prism returned a repeatedly failed sandbox session",
      "reprovision",
      409,
    );
  }

  #pruneFailedSandboxes(now: number): void {
    for (const [key, record] of this.#failedSandboxes) {
      const expiresAt = record.quarantinedUntilMs > 0
        ? record.quarantinedUntilMs
        : record.updatedAtMs + FAILED_SANDBOX_QUARANTINE_MS;
      if (expiresAt <= now) {
        this.#failedSandboxes.delete(key);
      }
    }
  }

  async refreshResources(
    current: ProvisionedRuntimeCredentials,
    signal?: AbortSignal,
  ): Promise<ResourceCredential> {
    return this.#syncResources(current.projectId, current.sandbox, signal);
  }

  async mintYSweet(
    projectId: ProjectId,
    context: YSweetRequestContext,
    signal?: AbortSignal,
  ): Promise<YSweetEnvelope> {
    const maxAttempts = 5;
    const response = await this.#requestWithRetry(
      "/api/y",
      {
        method: "POST",
        signal,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          docId: projectId,
          requestContext: {
            ...context,
            maxAttempts,
            requestSeriesId: this.#random.uuid(),
          },
        }),
      },
      maxAttempts,
      signal,
    );
    const envelope = parseYSweetEnvelope(await parseJsonResponse(response, "Y-Sweet mint"));
    if (envelope.docId !== projectId) {
      throw new ProvisioningError(
        "Y-Sweet mint returned a credential for another project",
        "protocol",
        response.status,
      );
    }
    return envelope;
  }

  async waitForWorkspaceSync(sandbox: ProvisionedSandbox, signal?: AbortSignal): Promise<void> {
    const deadline = this.#clock.now() + this.#workspaceSyncTimeoutMs;
    for (;;) {
      checkAbort(signal);
      const remainingMs = deadline - this.#clock.now();
      if (remainingMs < 1) {
        throw new ProvisioningError("Sandbox workspace synchronization timed out", "timeout");
      }
      const response = await this.#request(sandboxPath(sandbox, "wait-for-sync"), {
        method: "GET",
        signal,
        headers: { "x-crixet-sandbox-token": sandbox.token.reveal() },
      }, sandboxBodyAdmissionKey(sandbox, "GET", "wait-for-sync"), response => response.ok,
      Math.min(this.#requestTimeoutMs, Math.floor(remainingMs)));
      if (response.status === 404 || response.status === 501) return;
      if (response.status === 401 || response.status === 403 || response.status === 409) {
        throw new ProvisioningError(
          `Sandbox workspace synchronization requires reprovision (${response.status})`,
          "reprovision",
          response.status,
        );
      }
      if (!response.ok) {
        throw new ProvisioningError(
          `Sandbox workspace synchronization failed (${response.status})`,
          "transport",
          response.status,
        );
      }
      const body = await parseJsonResponse(response, "Workspace synchronization");
      const status = body.status;
      const capabilities = Array.isArray(body.readinessCapabilities)
        ? body.readinessCapabilities.filter(value => typeof value === "string") as string[]
        : undefined;
      const tokens = body.tokens && typeof body.tokens === "object"
        ? body.tokens as Record<string, unknown>
        : {};
      const current = tokens.hasCurrentYSweetToken === true;
      const provider = tokens.hasSyncedYSweetProvider;
      const currentProvider = capabilities?.includes("current_y_sweet_provider") ?? false;
      const legacy = capabilities === undefined;
      if (
        status === "synced" && current &&
        ((currentProvider && provider === true) || (legacy && provider !== false))
      ) return;
      const waitable = status === "syncing" || (
        status === "synced" && (!current || provider === false)
      );
      if (!waitable) {
        throw new ProvisioningError(
          status === "failed" ? "Sandbox workspace synchronization failed" : "Invalid workspace synchronization state",
          "reprovision",
        );
      }
      if (this.#clock.now() >= deadline) {
        throw new ProvisioningError("Sandbox workspace synchronization timed out", "timeout");
      }
      await this.#clock.sleep(Math.min(this.#readinessPollMs, deadline - this.#clock.now()), signal);
    }
  }

  async #syncResources(
    projectId: ProjectId,
    sandbox: ProvisionedSandbox,
    signal?: AbortSignal,
  ): Promise<ResourceCredential> {
    const response = await this.#requestWithRetry(
      `/api/projects/${encodeURIComponent(projectId)}/sandbox/resources-token`,
      {
        method: "POST",
        signal,
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sandbox_session_id: sandbox.sessionId ?? null }),
      },
      3,
      signal,
    );
    const raw = await parseJsonResponse(response, "Resource credential");
    if (typeof raw.resources_base_url !== "string" || !raw.resources_base_url) {
      raw.resources_base_url = resourceBaseFromSandbox(sandbox.url);
    }
    const resource = parseResourceCredential(raw, this.#clock.now());
    await this.#sandboxJson(sandbox, "resources-token", {
      token: resource.token.reveal(),
      resourceBaseUrl: resource.baseUrl,
      projectId,
    }, signal);
    return resource;
  }

  async #sandboxJson(
    sandbox: ProvisionedSandbox,
    suffix: string,
    json: unknown,
    signal?: AbortSignal,
  ): Promise<Response> {
    const response = await this.#request(sandboxPath(sandbox, suffix), {
      method: "POST",
      signal,
      headers: {
        "content-type": "application/json",
        "x-crixet-sandbox-token": sandbox.token.reveal(),
      },
      body: JSON.stringify(json),
    }, sandboxBodyAdmissionKey(sandbox, "POST", suffix), () => false);
    if (response.status === 401 || response.status === 403 || response.status === 409) {
      throw new ProvisioningError(`Sandbox credential synchronization failed (${response.status})`, "reprovision", response.status);
    }
    if (!response.ok) {
      throw new ProvisioningError(`Sandbox credential synchronization failed (${response.status})`, "transport", response.status);
    }
    return response;
  }

  async #requestWithRetry(
    path: string,
    init: RequestInit,
    maxAttempts: number,
    signal?: AbortSignal,
    bodyAdmissionKey?: string,
  ): Promise<Response> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      checkAbort(signal);
      let response: Response;
      try {
        response = await this.#request(path, init, bodyAdmissionKey);
      } catch (error) {
        if (error instanceof AccountAuthError) {
          throw new ProvisioningError(error.message, "auth", error.status);
        }
        throw error;
      }
      if (response.ok) return response;
      const retryable = response.status === 429 || (response.status >= 500 && response.status < 600);
      if (!retryable || attempt === maxAttempts) {
        const kind: ProvisioningErrorKind = response.status === 429 ? "rate_limit" : "transport";
        throw new ProvisioningError(
          `Prism request failed (${response.status})`,
          kind,
          response.status,
          response.status === 429 ? retryAfterMs(response, 1_000 * attempt) : undefined,
        );
      }
      await this.#clock.sleep(
        response.status === 429
          ? retryAfterMs(response, 1_000 * attempt)
          : 1_000 * attempt,
        signal,
      );
    }
    throw new ProvisioningError("Prism retry loop exhausted", "transport");
  }

  async #request(
    path: string,
    init: RequestInit,
    bodyAdmissionKey = `${(init.method ?? "GET").toUpperCase()}:${path}`,
    shouldBufferResponseBody: (response: Response) => boolean = response => response.ok,
    timeoutMs = this.#requestTimeoutMs,
  ): Promise<Response> {
    return boundedAccountRequest({
      account: this.#account,
      path,
      init,
      timeoutMs,
      responseBodyLimitBytes: this.#responseBodyLimitBytes,
      bodyAdmissionKey,
      shouldBufferResponseBody,
      timeoutError: () => new ProvisioningError("Prism request timed out", "timeout"),
      responseBodyLimitError: response => new ProvisioningError(
        "Prism response body exceeded the bounded limit",
        "protocol",
        response.status,
      ),
      responseBodyReadError: response => new ProvisioningError(
        "Prism response body could not be read",
        "transport",
        response.status,
      ),
      abortError: signalAbortError,
    });
  }
}
