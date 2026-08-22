import {
  Unzip,
  UnzipInflate,
  type UnzipFile,
} from "fflate";
import { ZipPollCoordinator } from "../prism/zip-coordinator";
import type { AccountRequestClient } from "../prism/provisioning-client";
import type { ProvisionedSandbox } from "../prism/protocol";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { ProjectId } from "./types";

export interface SandboxGatewayOptions {
  projectId: ProjectId;
  account: AccountRequestClient;
  sandbox: ProvisionedSandbox;
  clock?: Clock;
  zipBatchMs?: number;
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

function concat(chunks: Uint8Array[], total: number): Uint8Array {
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

async function unzipStream(response: Response, wanted: readonly string[]): Promise<Map<string, Uint8Array>> {
  if (!response.body) throw new SandboxGatewayError("ZIP response has no body", response.status, "zip");
  const names = new Set(wanted);
  const output = new Map<string, Uint8Array>();
  let pending = 0;
  let inputDone = false;
  let settled = false;

  return new Promise<Map<string, Uint8Array>>((resolve, reject) => {
    const finish = () => {
      if (!settled && inputDone && pending === 0) {
        settled = true;
        resolve(output);
      }
    };
    const fail = (error: unknown) => {
      if (settled) return;
      settled = true;
      reject(error);
    };
    const unzip = new Unzip((file: UnzipFile) => {
      if (!names.has(file.name)) return;
      pending++;
      const chunks: Uint8Array[] = [];
      let total = 0;
      file.ondata = (error, data, final) => {
        if (error) return fail(error);
        if (data.length) {
          chunks.push(data);
          total += data.length;
        }
        if (final) {
          output.set(file.name, concat(chunks, total));
          pending--;
          finish();
        }
      };
      try { file.start(); } catch (error) { fail(error); }
    });
    unzip.register(UnzipInflate);

    void (async () => {
      const reader = response.body!.getReader();
      try {
        for (;;) {
          const { done, value } = await reader.read();
          unzip.push(value ?? new Uint8Array(), done);
          if (done) {
            inputDone = true;
            finish();
            return;
          }
        }
      } catch (error) {
        fail(error);
      } finally {
        reader.releaseLock();
      }
    })();
  });
}

export class SandboxGateway {
  readonly projectId: ProjectId;
  readonly #account: AccountRequestClient;
  readonly #sandbox: ProvisionedSandbox;
  readonly #clock: Clock;
  readonly #zipCoordinator: ZipPollCoordinator;
  readonly #syncBatchMs: number;
  #cacheBust = 0;
  #zipChain: Promise<void> = Promise.resolve();
  #syncRequestedEpoch = 0;
  #syncCompletedEpoch = 0;
  #syncLoop: Promise<void> | null = null;

  constructor(options: SandboxGatewayOptions) {
    this.projectId = options.projectId;
    this.#account = options.account;
    this.#sandbox = options.sandbox;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#syncBatchMs = options.syncBatchMs ?? 5;
    if (!Number.isFinite(this.#syncBatchMs) || this.#syncBatchMs < 0 || this.#syncBatchMs > 1_000) {
      throw new RangeError("syncBatchMs must be between 0 and 1000");
    }
    this.#zipCoordinator = new ZipPollCoordinator(
      names => this.#serializeZip(names),
      options.zipBatchMs ?? 10,
    );
  }

  async heartbeat(signal?: AbortSignal): Promise<HeartbeatResult> {
    const started = this.#clock.now();
    const response = await this.#request("heartbeat", { method: "GET", signal });
    return {
      ok: response.ok,
      status: response.status,
      durationMs: Math.max(0, this.#clock.now() - started),
      sessionId: response.headers.get("x-session-id"),
    };
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

  readFiles(names: string[]): Promise<Map<string, Uint8Array>> {
    return this.#zipCoordinator.read(names);
  }

  async triggerRender(request: RenderRequest, signal?: AbortSignal): Promise<void> {
    const response = await this.postJson("render?renderMode=async&renderStatusMode=json", request, signal);
    if (response.status !== 202) {
      throw new SandboxGatewayError(`Render returned ${response.status}`, response.status, "render");
    }
  }

  async deleteFile(filename: string, signal?: AbortSignal): Promise<void> {
    const response = await this.postJson("delete-file", { filename }, signal);
    if (!response.ok && response.status !== 404) {
      throw new SandboxGatewayError(`Delete returned ${response.status}`, response.status, "delete-file");
    }
  }

  async #request(suffix: string, init: RequestInit): Promise<Response> {
    const delimiter = suffix.includes("?") ? "&" : "?";
    const path = `${this.#sandbox.url}${suffix}${delimiter}prism_cache_bust=gp${Date.now().toString(36)}${this.#cacheBust++}`;
    const headers = new Headers(init.headers);
    headers.set("x-crixet-sandbox-token", this.#sandbox.token.reveal());
    return this.#account.request(path, { ...init, headers });
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

  #serializeZip(names: string[]): Promise<Map<string, Uint8Array>> {
    let resolveResult!: (value: Map<string, Uint8Array>) => void;
    let rejectResult!: (reason: unknown) => void;
    const result = new Promise<Map<string, Uint8Array>>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#zipChain = this.#zipChain
      .catch(() => undefined)
      .then(async () => {
        try { resolveResult(await this.#fetchZip(names)); }
        catch (error) { rejectResult(error); }
      });
    return result;
  }

  async #fetchZip(names: string[]): Promise<Map<string, Uint8Array>> {
    const response = await this.#request("zip", { method: "GET" });
    if (!response.ok) throw new SandboxGatewayError(`ZIP returned ${response.status}`, response.status, "zip");
    return unzipStream(response, names);
  }
}
