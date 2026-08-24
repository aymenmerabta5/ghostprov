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
}

function abortError(): DOMException {
  return new DOMException("Provisioning aborted", "AbortError");
}

function checkAbort(signal?: AbortSignal): void {
  if (signal?.aborted) throw abortError();
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

  constructor(options: PrismProvisioningClientOptions) {
    this.#account = options.account;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#workspaceSyncTimeoutMs = options.workspaceSyncTimeoutMs ?? 30_000;
    this.#readinessPollMs = options.readinessPollMs ?? 250;
  }

  async provision(
    projectId: ProjectId,
    signal?: AbortSignal,
    hooks: ProvisioningHooks = {},
  ): Promise<ProvisionedRuntimeCredentials> {
    const sandbox = await this.acquireSandbox(signal);
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
    return parseYSweetEnvelope(await parseJsonResponse(response, "Y-Sweet mint"));
  }

  async waitForWorkspaceSync(sandbox: ProvisionedSandbox, signal?: AbortSignal): Promise<void> {
    const deadline = this.#clock.now() + this.#workspaceSyncTimeoutMs;
    for (;;) {
      checkAbort(signal);
      if (this.#clock.now() > deadline) {
        throw new ProvisioningError("Sandbox workspace synchronization timed out", "timeout");
      }
      const response = await this.#account.request(sandboxPath(sandbox, "wait-for-sync"), {
        method: "GET",
        signal,
        headers: { "x-crixet-sandbox-token": sandbox.token.reveal() },
      });
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
    const response = await this.#account.request(sandboxPath(sandbox, suffix), {
      method: "POST",
      signal,
      headers: {
        "content-type": "application/json",
        "x-crixet-sandbox-token": sandbox.token.reveal(),
      },
      body: JSON.stringify(json),
    });
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
  ): Promise<Response> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      checkAbort(signal);
      let response: Response;
      try {
        response = await this.#account.request(path, init);
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
}
