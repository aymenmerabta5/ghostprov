import type { AccountRequestClient } from "../prism/provisioning-client";
import type { ProvisionedSandbox } from "../prism/protocol";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { ProjectId } from "./types";

export interface SandboxGatewayOptions {
  projectId: ProjectId;
  account: AccountRequestClient;
  sandbox: ProvisionedSandbox;
  clock?: Clock;
  syncBatchMs?: number;
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

export class SandboxGateway {
  readonly projectId: ProjectId;
  readonly #account: AccountRequestClient;
  readonly #clock: Clock;
  readonly #syncBatchMs: number;
  #sandbox: ProvisionedSandbox;
  #effectiveSessionId: string | null;
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
    if (!Number.isFinite(this.#syncBatchMs) || this.#syncBatchMs < 0 || this.#syncBatchMs > 1_000) {
      throw new RangeError("syncBatchMs must be between 0 and 1000");
    }
  }

  async heartbeat(signal?: AbortSignal): Promise<HeartbeatResult> {
    const started = this.#clock.now();
    const response = await this.#request("heartbeat", { method: "GET", signal });
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

  async postJson(suffix: string, json: unknown, signal?: AbortSignal): Promise<Response> {
    return this.#request(suffix, {
      method: "POST",
      signal,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(json),
    });
  }

  async triggerRender(request: RenderRequest, signal?: AbortSignal): Promise<void> {
    const response = await this.postJson("render?renderMode=async&renderStatusMode=json", request, signal);
    if (response.status !== 202) {
      throw new SandboxGatewayError(`Render returned ${response.status}`, response.status, "render");
    }
  }

  async #request(suffix: string, init: RequestInit): Promise<Response> {
    // A request owns one immutable credential snapshot. Rotation may happen
    // while the request is in flight, but can only affect later requests.
    const sandbox = this.#sandbox;
    const delimiter = suffix.includes("?") ? "&" : "?";
    const path = `${sandbox.url}${suffix}${delimiter}prism_cache_bust=gp${Date.now().toString(36)}${this.#cacheBust++}`;
    const headers = new Headers(init.headers);
    headers.set("x-crixet-sandbox-token", sandbox.token.reveal());
    return this.#account.request(path, { ...init, headers });
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
