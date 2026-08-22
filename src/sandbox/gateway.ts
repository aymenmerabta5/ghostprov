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
  #cacheBust = 0;
  #zipChain: Promise<void> = Promise.resolve();

  constructor(options: SandboxGatewayOptions) {
    this.projectId = options.projectId;
    this.#account = options.account;
    this.#sandbox = options.sandbox;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
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
