import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { appendFile, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import {
  Agent,
  createServer as createHttpServer,
  request as httpRequest,
  type IncomingMessage,
  type Server as HttpServer,
  type ServerResponse,
} from "node:http";
import { connect as connectTcp, createServer as createTcpServer } from "node:net";
import * as path from "node:path";
import type { Duplex } from "node:stream";
import {
  apiKeyAuthorized,
  isProtectedApiPath,
  publicRequestAuthorityFailure,
  resolveGhostProvApiKey,
  type PublicRequestAuthorityFailure,
} from "../http/api-key";
import { isAnthropicRequestPath, requestPath } from "../http/request-policy";
import { sanitizeDiagnosticMessage } from "../observability/redaction";
import {
  parseSupervisorControlRequest,
  requestTargetsSupervisor,
  validSupervisorControlTarget,
  type SupervisorControlAction,
  type SupervisorControlTarget,
} from "./supervisor-control";

export type HostSupervisorState =
  | "starting"
  | "ready"
  | "draining"
  | "backoff"
  | "stopping"
  | "stopped";

export interface HostSupervisorSnapshot {
  state: HostSupervisorState;
  supervisorPid: number;
  startedAt: string;
  instanceNonce: string;
  workerPid: number | null;
  generation: number;
  externalPort: number | null;
  internalPort: number | null;
  restarts: number;
  upgrades: number;
  consecutiveFailures: number;
  queuedRequests: number;
  queuedRequestsPeak: number;
  queuedBytes: number;
  queuedBytesPeak: number;
  queueRejectedTotal: number;
  activeProxyRequests: number;
  activeWebSockets: number;
  forwardedTotal: number;
  proxyErrorsTotal: number;
  lastReadyAt: string | null;
  lastExitCode: number | null;
  lastExitSignal: NodeJS.Signals | null;
  shutdownClean: boolean | null;
  forcedStops: number;
  statusWritesTotal: number;
  statusWritesCoalesced: number;
}

export interface GhostProvHostSupervisorOptions {
  externalPort: number;
  /** Explicit public API credential; otherwise taken from the frozen worker environment. */
  apiKey?: string;
  host?: string;
  /** Additional explicit public hostnames accepted beside loopback authorities. */
  allowedHosts?: readonly string[];
  cwd?: string;
  workerEntry?: string;
  workerEnv?: NodeJS.ProcessEnv;
  readyTimeoutMs?: number;
  gracefulShutdownMs?: number;
  requestWaitMs?: number;
  maxQueuedRequests?: number;
  maxQueuedBytes?: number;
  restartBaseMs?: number;
  restartMaxMs?: number;
  stableResetMs?: number;
  sampleIntervalMs?: number;
  controlFile?: string;
  stopFile?: string;
  statusFile?: string;
  logFile?: string;
  startedAt?: string;
  instanceNonce?: string;
  /** Testable internal-port allocator; production uses an ephemeral loopback bind. */
  allocatePort?: () => Promise<number>;
  /** Reports the proven worker cleanup outcome after local supervision closes. */
  onClosed?: (clean: boolean) => Promise<void>;
}

interface WorkerTarget {
  generation: number;
  port: number;
  pid: number;
  agent: Agent;
}

interface TargetWaiter {
  minimumGeneration: number;
  resolve: (target: WorkerTarget) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
  signal?: AbortSignal;
  onAbort?: () => void;
}

interface ChildExit {
  code: number | null;
  signal: NodeJS.Signals | null;
  ready: boolean;
  uptimeMs: number;
}

const SUPERVISOR_SCHEMA = 1;

export function supervisorRestartDelayMs(
  consecutiveFailures: number,
  options: { baseMs?: number; maxMs?: number; jitter?: number } = {},
): number {
  const baseMs = options.baseMs ?? 500;
  const maxMs = options.maxMs ?? 30_000;
  const jitter = options.jitter ?? 0.5;
  if (!Number.isSafeInteger(consecutiveFailures) || consecutiveFailures < 1) {
    throw new RangeError("consecutiveFailures must be a positive integer");
  }
  if (!Number.isFinite(baseMs) || baseMs < 1 || !Number.isFinite(maxMs) || maxMs < baseMs) {
    throw new RangeError("supervisor restart bounds are invalid");
  }
  if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new RangeError("supervisor restart jitter must be from 0 through 1");
  }
  const exponential = Math.min(maxMs, baseMs * 2 ** Math.min(20, consecutiveFailures - 1));
  return Math.min(maxMs, Math.max(1, Math.ceil(exponential * (0.75 + jitter * 0.5))));
}

function abortError(message: string): DOMException {
  return new DOMException(message, "AbortError");
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function freeLoopbackPort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("Unable to allocate a loopback worker port");
  }
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

function safeReason(value: unknown): string {
  return sanitizeDiagnosticMessage(value).slice(0, 128);
}

function queuedRequestFootprint(request: IncomingMessage, extraBytes = 0): number | null {
  let bytes = Buffer.byteLength(
    `${request.method ?? "GET"} ${request.url ?? "/"} HTTP/${request.httpVersion}\r\n`,
  ) + extraBytes + 2;
  for (const value of request.rawHeaders) bytes += Buffer.byteLength(value) + 4;
  const contentLength = request.headers["content-length"];
  if (Array.isArray(contentLength)) return null;
  if (contentLength !== undefined) {
    if (!/^(?:0|[1-9][0-9]*)$/.test(contentLength)) return null;
    const parsed = Number(contentLength);
    if (!Number.isSafeInteger(parsed) || parsed < 0) return null;
    bytes += parsed;
  } else if (request.headers["transfer-encoding"] !== undefined) {
    // A queued chunked body has no provable upper bound. Ready workers still
    // receive it normally; only the restart/startup holding queue refuses it.
    return null;
  }
  return Number.isSafeInteger(bytes) && bytes >= 0 ? bytes : null;
}

function unavailableBody(code: "host_restarting" | "host_queue_full"): string {
  return JSON.stringify({ error: {
    message: code === "host_queue_full"
      ? "GhostProv restart queue is full"
      : "GhostProv worker is restarting",
    type: "service_unavailable_error",
    param: null,
    code,
  }});
}

function rejectHttpUnavailable(
  response: ServerResponse,
  code: "host_restarting" | "host_queue_full",
): void {
  if (response.headersSent || response.destroyed) return;
  const body = unavailableBody(code);
  response.writeHead(503, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
    "retry-after": "1",
    connection: "close",
  });
  response.end(body);
}

function rejectUpgradeUnavailable(
  socket: Duplex,
  code: "host_restarting" | "host_queue_full",
): void {
  if (socket.destroyed) return;
  const body = unavailableBody(code);
  socket.end(
    "HTTP/1.1 503 Service Unavailable\r\n" +
    "Content-Type: application/json\r\n" +
    `Content-Length: ${Buffer.byteLength(body)}\r\n` +
    "Retry-After: 1\r\n" +
    "Connection: close\r\n\r\n" +
    body,
  );
}

function protectedSupervisorPath(pathname: string): boolean {
  return isProtectedApiPath(pathname) ||
    pathname === "/__sandboxes" || pathname === "/__supervisor";
}

function supervisorApiAuthorized(request: IncomingMessage, apiKey: string, pathname: string): boolean {
  return apiKeyAuthorized({
    authorization: request.headers.authorization,
    xApiKey: request.headers["x-api-key"],
  }, apiKey, isAnthropicRequestPath(pathname) ? "anthropic" : "openai");
}

function endJson(
  response: ServerResponse,
  status: number,
  body: Record<string, unknown>,
  headers: Record<string, string> = {},
): void {
  if (response.headersSent || response.destroyed) return;
  const encoded = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(encoded),
    connection: "close",
    ...headers,
  });
  response.end(encoded);
}

function rejectSupervisorAuthority(
  response: ServerResponse,
  failure: PublicRequestAuthorityFailure,
): void {
  endJson(response, failure === "host" ? 421 : 403, { error: {
    message: failure === "host" ? "Request host is not allowed" : "Request origin is not allowed",
    type: "permission_error",
    param: null,
    code: failure === "host" ? "invalid_host" : "invalid_origin",
  }});
}

function rejectSupervisorUnauthorized(response: ServerResponse, pathname: string): void {
  if (isAnthropicRequestPath(pathname)) {
    const requestId = `req_${randomUUID().replaceAll("-", "")}`;
    endJson(response, 401, {
      type: "error",
      error: { type: "authentication_error", message: "Invalid API key" },
      request_id: requestId,
    }, { "request-id": requestId });
    return;
  }
  endJson(response, 401, { error: {
    message: "Invalid API key",
    type: "authentication_error",
    param: null,
    code: "invalid_api_key",
  }}, { "www-authenticate": 'Bearer realm="ghostprov"' });
}

function rejectSupervisorNotFound(response: ServerResponse): void {
  endJson(response, 404, { error: {
    message: "Not found",
    type: "invalid_request_error",
    param: null,
    code: "not_found",
  }});
}

function rejectUpgrade(
  socket: Duplex,
  status: 401 | 403 | 404 | 421,
  code: string,
): void {
  if (socket.destroyed) return;
  const reason = status === 401
    ? "Unauthorized"
    : status === 403
      ? "Forbidden"
      : status === 404
        ? "Not Found"
        : "Misdirected Request";
  const body = JSON.stringify({ error: {
    message: reason,
    type: status === 401 ? "authentication_error" : "permission_error",
    param: null,
    code,
  }});
  socket.end(
    `HTTP/1.1 ${status} ${reason}\r\n` +
    "Content-Type: application/json\r\n" +
    `Content-Length: ${Buffer.byteLength(body)}\r\n` +
    (status === 401 ? 'WWW-Authenticate: Bearer realm="ghostprov"\r\n' : "") +
    "Connection: close\r\n\r\n" + body,
  );
}

export class GhostProvHostSupervisor {
  readonly #options: Required<Omit<GhostProvHostSupervisorOptions,
    "workerEnv" | "controlFile" | "stopFile" | "statusFile" | "logFile">> & {
      workerEnv: NodeJS.ProcessEnv;
      controlFile: string;
      stopFile: string;
      statusFile: string;
      logFile: string;
    };
  readonly #server: HttpServer;
  readonly #targetWaiters = new Set<TargetWaiter>();
  readonly #proxySockets = new Set<Duplex>();
  readonly #condemnedChildren = new WeakSet<ChildProcess>();
  #state: HostSupervisorState = "starting";
  #target: WorkerTarget | null = null;
  #child: ChildProcess | null = null;
  #generation = 0;
  #restarts = 0;
  #upgrades = 0;
  #consecutiveFailures = 0;
  #plannedRestart = false;
  #stopping = false;
  #runPromise: Promise<void> | null = null;
  #runFailure: Error | null = null;
  #listenPromise: Promise<void> | null = null;
  #startPromise: Promise<void> | null = null;
  #preparePromise: Promise<void> | null = null;
  #closePromise: Promise<void> | null = null;
  #restartPromise: Promise<void> | null = null;
  #controlTimer: ReturnType<typeof setInterval> | null = null;
  #sampleTimer: ReturnType<typeof setInterval> | null = null;
  #controlContent: string | null = null;
  #stopContent: string | null = null;
  readonly #seenControlRequestIds = new Set<string>();
  #forceTimer: ReturnType<typeof setTimeout> | null = null;
  #statusWrite: Promise<void> = Promise.resolve();
  #statusWriteRunning = false;
  #statusPendingPayload: string | null = null;
  #logWrite: Promise<void> = Promise.resolve();
  #externalPort: number | null = null;
  #queuedRequests = 0;
  #queuedRequestsPeak = 0;
  #queuedBytes = 0;
  #queuedBytesPeak = 0;
  #queueRejectedTotal = 0;
  #activeProxyRequests = 0;
  #activeWebSockets = 0;
  #forwardedTotal = 0;
  #proxyErrorsTotal = 0;
  #sampleForwarded = 0;
  #sampleErrors = 0;
  #lastReadyAt: string | null = null;
  #lastExitCode: number | null = null;
  #lastExitSignal: NodeJS.Signals | null = null;
  #lastCleanupProven = false;
  #shutdownClean: boolean | null = null;
  #forcedStops = 0;
  #statusWritesTotal = 0;
  #statusWritesCoalesced = 0;

  constructor(options: GhostProvHostSupervisorOptions) {
    if (!Number.isSafeInteger(options.externalPort) || options.externalPort < 0 || options.externalPort > 65_535) {
      throw new RangeError("externalPort must be an integer from 0 through 65535");
    }
    if (options.host && !["127.0.0.1", "::1", "localhost"].includes(options.host.toLowerCase())) {
      throw new TypeError("GhostProv supervisor host must be loopback");
    }
    const apiKey = resolveGhostProvApiKey(options.apiKey ?? options.workerEnv?.GHOSTPROV_API_KEY);
    const allowedHosts = Object.freeze([...(options.allowedHosts ?? [])]);
    publicRequestAuthorityFailure({ host: "localhost" }, allowedHosts);
    const cwd = path.resolve(options.cwd ?? process.cwd());
    const startedAt = options.startedAt ?? new Date().toISOString();
    const instanceNonce = options.instanceNonce ?? randomUUID();
    if (!validSupervisorControlTarget({
      supervisorPid: process.pid,
      startedAt,
      instanceNonce,
    })) {
      throw new TypeError("GhostProv supervisor control identity is invalid");
    }
    this.#options = {
      externalPort: options.externalPort,
      apiKey,
      host: options.host ?? "127.0.0.1",
      allowedHosts,
      cwd,
      workerEntry: path.resolve(cwd, options.workerEntry ?? "src/server.ts"),
      // Freeze the complete environment once. A rolling restart must not pick
      // up later process.env mutations or a newly edited .env file.
      workerEnv: {
        ...process.env,
        ...options.workerEnv,
        GHOSTPROV_API_KEY: apiKey,
      },
      readyTimeoutMs: options.readyTimeoutMs ?? 120_000,
      gracefulShutdownMs: options.gracefulShutdownMs ?? 330_000,
      requestWaitMs: options.requestWaitMs ?? 480_000,
      maxQueuedRequests: options.maxQueuedRequests ?? 256,
      maxQueuedBytes: options.maxQueuedBytes ?? 64 * 1024 * 1024,
      restartBaseMs: options.restartBaseMs ?? 500,
      restartMaxMs: options.restartMaxMs ?? 30_000,
      stableResetMs: options.stableResetMs ?? 60_000,
      sampleIntervalMs: options.sampleIntervalMs ?? 10_000,
      controlFile: path.resolve(cwd, options.controlFile ?? ".ghostprov/upgrade.request"),
      stopFile: path.resolve(cwd, options.stopFile ?? ".ghostprov/stop.request"),
      statusFile: path.resolve(cwd, options.statusFile ?? ".ghostprov/supervisor.json"),
      logFile: path.resolve(cwd, options.logFile ?? ".ghostprov/logs/supervisor.jsonl"),
      startedAt,
      instanceNonce,
      allocatePort: options.allocatePort ?? freeLoopbackPort,
      onClosed: options.onClosed ?? (async () => {}),
    };
    for (const [name, value, minimum] of [
      ["readyTimeoutMs", this.#options.readyTimeoutMs, 100],
      ["gracefulShutdownMs", this.#options.gracefulShutdownMs, 100],
      ["requestWaitMs", this.#options.requestWaitMs, 100],
      ["restartBaseMs", this.#options.restartBaseMs, 1],
      ["restartMaxMs", this.#options.restartMaxMs, 1],
      ["stableResetMs", this.#options.stableResetMs, 100],
      ["sampleIntervalMs", this.#options.sampleIntervalMs, 100],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < minimum) {
        throw new RangeError(`${name} must be an integer of at least ${minimum}ms`);
      }
    }
    if (this.#options.restartMaxMs < this.#options.restartBaseMs) {
      throw new RangeError("restartMaxMs must be at least restartBaseMs");
    }
    if (!Number.isSafeInteger(this.#options.maxQueuedRequests) || this.#options.maxQueuedRequests < 1) {
      throw new RangeError("maxQueuedRequests must be a positive integer");
    }
    if (!Number.isSafeInteger(this.#options.maxQueuedBytes) || this.#options.maxQueuedBytes < 1_024) {
      throw new RangeError("maxQueuedBytes must be an integer of at least 1024 bytes");
    }
    this.#server = createHttpServer((request, response) => {
      void this.#forwardHttp(request, response);
    });
    this.#server.on("upgrade", (request, socket, head) => {
      void this.#forwardUpgrade(request, socket, head);
    });
  }

  snapshot(): HostSupervisorSnapshot {
    return {
      state: this.#state,
      supervisorPid: process.pid,
      startedAt: this.#options.startedAt,
      instanceNonce: this.#options.instanceNonce,
      workerPid: this.#child?.pid ?? null,
      generation: this.#generation,
      externalPort: this.#externalPort,
      internalPort: this.#target?.port ?? null,
      restarts: this.#restarts,
      upgrades: this.#upgrades,
      consecutiveFailures: this.#consecutiveFailures,
      queuedRequests: this.#queuedRequests,
      queuedRequestsPeak: this.#queuedRequestsPeak,
      queuedBytes: this.#queuedBytes,
      queuedBytesPeak: this.#queuedBytesPeak,
      queueRejectedTotal: this.#queueRejectedTotal,
      activeProxyRequests: this.#activeProxyRequests,
      activeWebSockets: this.#activeWebSockets,
      forwardedTotal: this.#forwardedTotal,
      proxyErrorsTotal: this.#proxyErrorsTotal,
      lastReadyAt: this.#lastReadyAt,
      lastExitCode: this.#lastExitCode,
      lastExitSignal: this.#lastExitSignal,
      shutdownClean: this.#shutdownClean,
      forcedStops: this.#forcedStops,
      statusWritesTotal: this.#statusWritesTotal,
      statusWritesCoalesced: this.#statusWritesCoalesced,
    };
  }

  start(): Promise<void> {
    if (this.#startPromise) return this.#startPromise;
    const starting = this.#startInternal();
    this.#startPromise = starting;
    return starting;
  }

  async #startInternal(): Promise<void> {
    if (this.#stopping) throw new Error("Supervisor is stopping");
    if (this.#runFailure) throw this.#runFailure;
    if (this.#runPromise) {
      await Promise.race([
        this.#waitForTarget(1, this.#options.readyTimeoutMs),
        this.#runPromise.then(() => {
          throw new Error("Supervisor run loop stopped before worker readiness");
        }),
      ]);
      return;
    }
    await this.prepareControlPlane();
    if (this.#stopping) throw new Error("Supervisor is stopping");
    const listening = new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => {
        this.#server.off("error", onError);
        reject(error);
      };
      this.#server.once("error", onError);
      try {
        this.#server.listen(this.#options.externalPort, this.#options.host, () => {
          this.#server.off("error", onError);
          const address = this.#server.address();
          this.#externalPort = address && typeof address !== "string" ? address.port : null;
          resolve();
        });
      } catch (error) {
        this.#server.off("error", onError);
        reject(error);
      }
    });
    this.#listenPromise = listening;
    try {
      await listening;
    } finally {
      if (this.#listenPromise === listening) this.#listenPromise = null;
    }
    if (this.#stopping) throw new Error("Supervisor is stopping");
    this.#controlTimer = setInterval(() => void this.#pollControlFile(), 1_000);
    this.#controlTimer.unref?.();
    this.#sampleTimer = setInterval(() => this.#emitSample(), this.#options.sampleIntervalMs);
    this.#sampleTimer.unref?.();
    const run = this.#runLoop().catch(error => {
      const failure = error instanceof Error ? error : new Error("Supervisor run loop failed");
      this.#runFailure = failure;
      for (const waiter of [...this.#targetWaiters]) this.#finishWaiter(waiter, failure);
      throw failure;
    });
    this.#runPromise = run;
    await Promise.race([
      this.#waitForTarget(1, this.#options.readyTimeoutMs),
      run.then(() => {
        throw new Error("Supervisor run loop stopped before worker readiness");
      }),
    ]);
  }

  prepareControlPlane(): Promise<void> {
    if (this.#preparePromise) return this.#preparePromise;
    this.#preparePromise = (async () => {
      await mkdir(path.dirname(this.#options.controlFile), { recursive: true });
      await mkdir(path.dirname(this.#options.statusFile), { recursive: true });
      await mkdir(path.dirname(this.#options.logFile), { recursive: true });
      this.#controlContent = await readFile(this.#options.controlFile, "utf8").catch(() => null);
      this.#stopContent = await readFile(this.#options.stopFile, "utf8").catch(() => null);
      for (const content of [this.#controlContent, this.#stopContent]) {
        if (content === null) continue;
        const request = parseSupervisorControlRequest(content);
        if (request) this.#seenControlRequestIds.add(request.requestId);
      }
    })();
    return this.#preparePromise;
  }

  requestRestart(reason = "upgrade-request"): Promise<void> {
    if (this.#stopping) return Promise.reject(new Error("Supervisor is stopping"));
    if (this.#runFailure) return Promise.reject(this.#runFailure);
    if (this.#restartPromise) return this.#restartPromise;
    const minimumGeneration = this.#generation + 1;
    const startedAt = performance.now();
    this.#plannedRestart = true;
    this.#state = "draining";
    this.#setTarget(null);
    this.#log("upgrade_started", { reason: safeReason(reason), minimum_generation: minimumGeneration });
    this.#requestChildShutdown(reason, 75);
    const restart = this.#waitForTarget(
      minimumGeneration,
      this.#options.readyTimeoutMs + this.#options.gracefulShutdownMs,
    ).then(() => {
      this.#upgrades++;
      this.#log("upgrade_completed", {
        generation: this.#generation,
        duration_ms: Math.max(0, performance.now() - startedAt),
      });
    }).finally(() => {
      if (this.#restartPromise === restart) this.#restartPromise = null;
      this.#writeStatus();
    });
    this.#restartPromise = restart;
    return restart;
  }

  close(): Promise<void> {
    if (this.#closePromise) return this.#closePromise;
    this.#closePromise = (async () => {
      this.#stopping = true;
      this.#state = "stopping";
      this.#setTarget(null);
      if (this.#controlTimer) clearInterval(this.#controlTimer);
      if (this.#sampleTimer) clearInterval(this.#sampleTimer);
      this.#controlTimer = null;
      this.#sampleTimer = null;
      for (const waiter of [...this.#targetWaiters]) {
        this.#finishWaiter(waiter, new Error("Supervisor is stopping"));
      }
      this.#requestChildShutdown("supervisor-stop", 0);
      const pendingListen = this.#listenPromise;
      const closingServer = (async () => {
        await pendingListen?.catch(() => undefined);
        if (!this.#server.listening) return;
        await new Promise<void>(resolve => this.#server.close(() => resolve()));
      })();
      let runCompleted = this.#runPromise === null;
      let shutdownTimer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([
          (this.#runPromise ?? Promise.resolve()).then(
            () => { runCompleted = true; },
            () => { runCompleted = true; },
          ),
          new Promise<void>(resolve => {
            shutdownTimer = setTimeout(resolve, this.#options.gracefulShutdownMs + 1_000);
          }),
        ]);
      } finally {
        // A successful drain must not keep the executable alive for the unused
        // graceful timeout after its cleanup receipt has already been written.
        if (shutdownTimer) clearTimeout(shutdownTimer);
      }
      const forced = this.#child !== null;
      if (forced) {
        this.#child?.kill();
      }
      for (const socket of this.#proxySockets) socket.destroy();
      this.#proxySockets.clear();
      this.#server.closeAllConnections?.();
      await closingServer;
      this.#shutdownClean = runCompleted && !forced && this.#forcedStops === 0 &&
        this.#generation > 0 && this.#lastCleanupProven &&
        this.#lastExitCode === 0 && this.#lastExitSignal === null;
      this.#state = "stopped";
      this.#log("shutdown_completed", {
        clean: this.#shutdownClean,
        forced,
        last_exit_code: this.#lastExitCode,
      });
      this.#writeStatus();
      await this.#flushStatusWrites();
      await this.#logWrite;
      await this.#options.onClosed(this.#shutdownClean);
    })();
    return this.#closePromise;
  }

  async #runLoop(): Promise<void> {
    while (!this.#stopping) {
      const planned = this.#plannedRestart;
      this.#plannedRestart = false;
      if (!planned && this.#consecutiveFailures > 0) {
        const delay = supervisorRestartDelayMs(this.#consecutiveFailures, {
          baseMs: this.#options.restartBaseMs,
          maxMs: this.#options.restartMaxMs,
          jitter: Math.random(),
        });
        this.#state = "backoff";
        this.#log("restart_scheduled", { delay_ms: delay, consecutive_failures: this.#consecutiveFailures });
        this.#writeStatus();
        await sleep(delay);
        if (this.#stopping) break;
      }
      const exit = await this.#launchWorker();
      if (this.#stopping) break;
      const expected = this.#plannedRestart || exit.code === 75;
      if (!expected && (!exit.ready || exit.uptimeMs < this.#options.stableResetMs)) {
        this.#consecutiveFailures++;
      } else if (!expected) {
        this.#consecutiveFailures = 0;
        // A stable worker crash is restarted immediately rather than backed
        // off, but it is still an automatic restart. Emit the same scheduling
        // lifecycle event with a zero delay so operators can distinguish the
        // intentional restart decision from an unexplained worker launch.
        this.#log("restart_scheduled", { delay_ms: 0, consecutive_failures: 0 });
        this.#writeStatus();
      }
    }
  }

  async #launchWorker(): Promise<ChildExit> {
    this.#state = "starting";
    this.#setTarget(null);
    const port = await this.#options.allocatePort();
    if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
      throw new Error("Internal worker port allocator returned an invalid port");
    }
    const generation = ++this.#generation;
    if (generation > 1) this.#restarts++;
    const startedAt = performance.now();
    const child = spawn(process.execPath, ["--no-env-file", this.#options.workerEntry], {
      cwd: this.#options.cwd,
      env: {
        ...this.#options.workerEnv,
        PORT: String(port),
        GP_SUPERVISED_CHILD: "1",
        GP_SUPERVISOR_GENERATION: String(generation),
        // Keep the null-device guard for any transitive dotenv import so an
        // edited .env cannot leak into a rolling worker restart. A fresh
        // supervisor has already frozen the intended startup environment.
        DOTENV_CONFIG_PATH: process.platform === "win32" ? "NUL" : "/dev/null",
        DOTENV_CONFIG_QUIET: "true",
      },
      stdio: ["inherit", "inherit", "inherit", "ipc"],
      windowsHide: true,
    });
    this.#child = child;
    this.#lastCleanupProven = false;
    this.#log("worker_starting", { generation, pid: child.pid ?? null, internal_port: port });
    this.#writeStatus();
    let ready = false;
    let cleanupAcknowledged = false;
    let cleanupRejected = false;
    let readyTimer: ReturnType<typeof setTimeout> | null = null;
    const markReady = () => {
      if (
        ready ||
        this.#child !== child ||
        this.#stopping ||
        this.#condemnedChildren.has(child)
      ) return;
      ready = true;
      if (readyTimer) clearTimeout(readyTimer);
      const agent = new Agent({ keepAlive: true, maxSockets: 1_024, maxFreeSockets: 64 });
      this.#setTarget({ generation, port, pid: child.pid ?? -1, agent });
      this.#state = "ready";
      this.#lastReadyAt = new Date().toISOString();
      this.#log("worker_ready", {
        generation,
        pid: child.pid ?? null,
        internal_port: port,
        startup_ms: Math.max(0, performance.now() - startedAt),
      });
      this.#writeStatus();
    };
    child.on("message", message => {
      if (!message || typeof message !== "object") return;
      const value = message as {
        type?: unknown;
        port?: unknown;
        pid?: unknown;
        generation?: unknown;
        success?: unknown;
      };
      if (value.type === "ghostprov.cleanup") {
        const exact = Object.keys(value).sort().join(",") === "generation,pid,success,type";
        if (this.#child !== child || !exact || value.pid !== child.pid || value.generation !== generation || value.success !== true) {
          cleanupRejected = true;
          cleanupAcknowledged = false;
        } else if (!cleanupRejected) cleanupAcknowledged = true;
        return;
      }
      if (
        value.type === "ghostprov.ready" &&
        value.port === port &&
        value.generation === generation &&
        Number.isSafeInteger(child.pid) &&
        (child.pid ?? 0) > 0 &&
        value.pid === child.pid
      ) markReady();
    });
    readyTimer = setTimeout(() => {
      if (ready || this.#child !== child) return;
      this.#log("worker_ready_timeout", { generation, pid: child.pid ?? null });
      this.#requestChildShutdown("ready-timeout", 1);
    }, this.#options.readyTimeoutMs);
    readyTimer.unref?.();
    const exit = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
      let settled = false;
      const finish = (value: { code: number | null; signal: NodeJS.Signals | null }) => {
        if (settled) return;
        settled = true;
        resolve(value);
      };
      child.once("error", error => {
        this.#log("worker_spawn_error", { generation, error_name: error.name });
        finish({ code: 1, signal: null });
      });
      child.once("exit", (code, signal) => finish({ code, signal }));
    });
    if (readyTimer) clearTimeout(readyTimer);
    if (this.#forceTimer) clearTimeout(this.#forceTimer);
    this.#forceTimer = null;
    if (this.#target?.generation === generation) {
      this.#target.agent.destroy();
      this.#setTarget(null);
    }
    if (this.#child === child) this.#child = null;
    this.#lastExitCode = exit.code;
    this.#lastExitSignal = exit.signal;
    this.#lastCleanupProven = cleanupAcknowledged && !cleanupRejected;
    const uptimeMs = Math.max(0, performance.now() - startedAt);
    this.#log("worker_exited", {
      generation,
      code: exit.code,
      signal: exit.signal,
      ready,
      uptime_ms: uptimeMs,
    });
    this.#writeStatus();
    return { ...exit, ready, uptimeMs };
  }

  #requestChildShutdown(reason: string, exitCode: number): void {
    const child = this.#child;
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    this.#condemnedChildren.add(child);
    try {
      if (child.connected && child.send) {
        child.send({
          type: "ghostprov.shutdown",
          reason: safeReason(reason),
          exitCode,
        }, error => {
          if (error) this.#log("worker_ipc_error", { error_name: error.name });
        });
      } else {
        child.kill("SIGTERM");
      }
    } catch (error) {
      this.#log("worker_ipc_error", {
        error_name: error instanceof Error ? error.name : "Error",
      });
      child.kill("SIGTERM");
    }
    if (this.#forceTimer) clearTimeout(this.#forceTimer);
    this.#forceTimer = setTimeout(() => {
      if (this.#child === child && child.exitCode === null && child.signalCode === null) {
        this.#forcedStops++;
        this.#log("worker_forced_stop", { pid: child.pid ?? null, reason: safeReason(reason) });
        child.kill();
      }
    }, this.#options.gracefulShutdownMs);
    this.#forceTimer.unref?.();
  }

  #setTarget(target: WorkerTarget | null): void {
    this.#target = target;
    if (!target) return;
    for (const waiter of [...this.#targetWaiters]) {
      if (target.generation >= waiter.minimumGeneration) this.#finishWaiter(waiter, null, target);
    }
  }

  #waitForTarget(
    minimumGeneration: number,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<WorkerTarget> {
    if (this.#target && this.#target.generation >= minimumGeneration) {
      return Promise.resolve(this.#target);
    }
    if (this.#runFailure) return Promise.reject(this.#runFailure);
    if (this.#stopping) return Promise.reject(new Error("Supervisor is stopping"));
    if (signal?.aborted) return Promise.reject(abortError("Supervisor target wait aborted"));
    return new Promise<WorkerTarget>((resolve, reject) => {
      const waiter: TargetWaiter = {
        minimumGeneration,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.#finishWaiter(waiter, new Error("GhostProv worker did not become ready before the proxy deadline"));
        }, timeoutMs),
        signal,
      };
      waiter.timer.unref?.();
      waiter.onAbort = () => this.#finishWaiter(waiter, abortError("Supervisor target wait aborted"));
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#targetWaiters.add(waiter);
    });
  }

  #finishWaiter(waiter: TargetWaiter, error: Error | null, target?: WorkerTarget): void {
    if (!this.#targetWaiters.delete(waiter)) return;
    clearTimeout(waiter.timer);
    waiter.signal?.removeEventListener("abort", waiter.onAbort!);
    if (error) waiter.reject(error);
    else waiter.resolve(target!);
  }

  #reserveQueuedRequest(bytes: number | null): number | null {
    if (
      bytes === null ||
      bytes > this.#options.maxQueuedBytes ||
      this.#queuedRequests >= this.#options.maxQueuedRequests ||
      this.#queuedBytes > this.#options.maxQueuedBytes - bytes
    ) {
      this.#queueRejectedTotal++;
      return null;
    }
    const wasEmpty = this.#queuedRequests === 0;
    this.#queuedRequests++;
    this.#queuedBytes += bytes;
    this.#queuedRequestsPeak = Math.max(this.#queuedRequestsPeak, this.#queuedRequests);
    this.#queuedBytesPeak = Math.max(this.#queuedBytesPeak, this.#queuedBytes);
    if (wasEmpty) this.#writeStatus();
    return bytes;
  }

  #releaseQueuedRequest(bytes: number): void {
    if (this.#queuedRequests < 1 || bytes < 0 || bytes > this.#queuedBytes) {
      this.#log("queue_accounting_error", {
        queued_requests: this.#queuedRequests,
        queued_bytes: this.#queuedBytes,
        release_bytes: bytes,
      });
      this.#queuedRequests = 0;
      this.#queuedBytes = 0;
      this.#writeStatus();
      return;
    }
    this.#queuedRequests--;
    this.#queuedBytes -= bytes;
    if (this.#queuedRequests === 0) this.#writeStatus();
  }

  async #forwardHttp(request: IncomingMessage, response: ServerResponse): Promise<void> {
    const pathname = requestPath(request.url);
    const authorityFailure = publicRequestAuthorityFailure({
      host: request.headers.host,
      forwardedHost: request.headers["x-forwarded-host"],
      origin: request.headers.origin,
    }, this.#options.allowedHosts);
    if (authorityFailure) {
      request.resume();
      rejectSupervisorAuthority(response, authorityFailure);
      return;
    }
    if (
      protectedSupervisorPath(pathname) &&
      !supervisorApiAuthorized(request, this.#options.apiKey, pathname)
    ) {
      request.resume();
      rejectSupervisorUnauthorized(response, pathname);
      return;
    }
    if (pathname === "/__supervisor") {
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify({ schema: SUPERVISOR_SCHEMA, ...this.snapshot() }));
      return;
    }
    if (!this.#target && pathname === "/health") {
      response.writeHead(503, { "content-type": "application/json", "retry-after": "1" });
      response.end(JSON.stringify({ ok: false, supervisor: this.#state, retryable: true }));
      return;
    }
    const queuedAt = performance.now();
    let target = this.#target;
    if (!target) {
      if (!isProtectedApiPath(pathname) && pathname !== "/__sandboxes") {
        request.resume();
        rejectSupervisorNotFound(response);
        return;
      }
      const reservation = this.#reserveQueuedRequest(queuedRequestFootprint(request));
      if (reservation === null) {
        rejectHttpUnavailable(response, "host_queue_full");
        return;
      }
      const controller = new AbortController();
      const abort = () => controller.abort();
      request.once("aborted", abort);
      response.once("close", abort);
      try {
        target = await this.#waitForTarget(
          this.#generation,
          this.#options.requestWaitMs,
          controller.signal,
        );
      } catch {
        rejectHttpUnavailable(response, "host_restarting");
        return;
      } finally {
        this.#releaseQueuedRequest(reservation);
        request.off("aborted", abort);
        response.off("close", abort);
      }
      if (controller.signal.aborted) return;
    }
    this.#activeProxyRequests++;
    this.#forwardedTotal++;
    const waitMs = Math.max(0, performance.now() - queuedAt);
    const headers = { ...request.headers };
    headers.host = `127.0.0.1:${target.port}`;
    delete headers["x-forwarded-host"];
    delete headers.origin;
    headers["x-forwarded-proto"] = "http";
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      this.#activeProxyRequests = Math.max(0, this.#activeProxyRequests - 1);
    };
    const upstream = httpRequest({
      host: "127.0.0.1",
      port: target.port,
      method: request.method,
      path: request.url,
      headers,
      agent: target.agent,
    }, upstreamResponse => {
      response.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      let responseSettled = false;
      const settleResponse = (failed: boolean, error?: Error) => {
        if (responseSettled) return;
        responseSettled = true;
        if (failed && !finished) this.#proxyErrorsTotal++;
        finish();
        if (error && !response.destroyed) response.destroy(error);
      };
      upstreamResponse.once("end", () => settleResponse(false));
      upstreamResponse.once("aborted", () => settleResponse(
        true,
        new Error("Upstream response aborted after headers"),
      ));
      upstreamResponse.once("error", error => settleResponse(true, error));
      upstreamResponse.once("close", () => settleResponse(
        !upstreamResponse.complete,
        upstreamResponse.complete
          ? undefined
          : new Error("Upstream response closed before completion"),
      ));
      upstreamResponse.pipe(response);
    });
    upstream.once("error", error => {
      if (!finished) this.#proxyErrorsTotal++;
      finish();
      if (!response.headersSent && !response.destroyed) {
        response.writeHead(503, { "content-type": "application/json", "retry-after": "1" });
        response.end(JSON.stringify({ error: {
          message: "GhostProv worker connection failed",
          type: "service_unavailable_error",
          param: null,
          code: "worker_unavailable",
        }}));
      } else if (!response.destroyed) {
        response.destroy(error);
      }
    });
    request.once("aborted", () => upstream.destroy(abortError("Proxy client aborted")));
    response.once("close", () => {
      if (!upstream.destroyed) upstream.destroy();
      finish();
    });
    request.pipe(upstream);
    if (waitMs >= 1_000) this.#log("request_waited_for_worker", { wait_ms: waitMs });
  }

  async #forwardUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer): Promise<void> {
    const pathname = requestPath(request.url);
    const authorityFailure = publicRequestAuthorityFailure({
      host: request.headers.host,
      forwardedHost: request.headers["x-forwarded-host"],
      origin: request.headers.origin,
    }, this.#options.allowedHosts);
    if (authorityFailure) {
      rejectUpgrade(
        socket,
        authorityFailure === "host" ? 421 : 403,
        authorityFailure === "host" ? "invalid_host" : "invalid_origin",
      );
      return;
    }
    if (pathname !== "/v1/responses") {
      rejectUpgrade(socket, 404, "not_found");
      return;
    }
    if (!supervisorApiAuthorized(request, this.#options.apiKey, pathname)) {
      rejectUpgrade(socket, 401, "invalid_api_key");
      return;
    }
    let target = this.#target;
    if (!target) {
      const reservation = this.#reserveQueuedRequest(
        queuedRequestFootprint(request, head.byteLength),
      );
      if (reservation === null) {
        rejectUpgradeUnavailable(socket, "host_queue_full");
        return;
      }
      const controller = new AbortController();
      const abort = () => controller.abort();
      socket.once("close", abort);
      try {
        target = await this.#waitForTarget(
          this.#generation,
          this.#options.requestWaitMs,
          controller.signal,
        );
      } catch {
        rejectUpgradeUnavailable(socket, "host_restarting");
        return;
      } finally {
        this.#releaseQueuedRequest(reservation);
        socket.off("close", abort);
      }
    }
    if (socket.destroyed) return;
    const upstream = connectTcp(target.port, "127.0.0.1");
    this.#proxySockets.add(socket);
    this.#proxySockets.add(upstream);
    this.#activeWebSockets++;
    let finished = false;
    const finish = () => {
      if (finished) return;
      finished = true;
      this.#activeWebSockets = Math.max(0, this.#activeWebSockets - 1);
      this.#proxySockets.delete(socket);
      this.#proxySockets.delete(upstream);
    };
    socket.once("close", finish);
    upstream.once("close", finish);
    upstream.once("error", () => {
      this.#proxyErrorsTotal++;
      socket.destroy();
    });
    upstream.once("connect", () => {
      let header = `${request.method ?? "GET"} ${request.url ?? "/"} HTTP/${request.httpVersion}\r\n`;
      for (let index = 0; index < request.rawHeaders.length; index += 2) {
        const name = request.rawHeaders[index]!;
        if (["host", "origin", "x-forwarded-host", "x-forwarded-proto"].includes(
          name.toLowerCase(),
        )) continue;
        header += `${name}: ${request.rawHeaders[index + 1]}\r\n`;
      }
      header += `Host: 127.0.0.1:${target.port}\r\n`;
      upstream.write(`${header}\r\n`);
      if (head.byteLength) upstream.write(head);
      socket.pipe(upstream).pipe(socket);
    });
  }

  async #pollControlFile(): Promise<void> {
    if (await this.#consumeControlRequest(this.#options.stopFile, "shutdown")) {
      await this.close();
      return;
    }
    if (!await this.#consumeControlRequest(this.#options.controlFile, "upgrade")) return;
    await this.requestRestart("control-request").catch(error => {
      this.#log("upgrade_failed", { error_name: error instanceof Error ? error.name : "Error" });
    });
  }

  #controlTarget(): SupervisorControlTarget {
    return {
      supervisorPid: process.pid,
      startedAt: this.#options.startedAt,
      instanceNonce: this.#options.instanceNonce,
    };
  }

  async #consumeControlRequest(
    target: string,
    action: SupervisorControlAction,
  ): Promise<boolean> {
    const content = await readFile(target, "utf8").catch(() => null);
    const previous = action === "upgrade" ? this.#controlContent : this.#stopContent;
    if (content === previous) return false;
    if (action === "upgrade") this.#controlContent = content;
    else this.#stopContent = content;
    if (content === null) return false;
    const request = parseSupervisorControlRequest(content);
    if (!request) {
      this.#log("control_rejected", { action, reason: "invalid_request" });
      return false;
    }
    if (this.#seenControlRequestIds.has(request.requestId)) {
      this.#log("control_rejected", { action, reason: "duplicate", request_id: request.requestId });
      return false;
    }
    // Remember every well-formed identity before target checks. Rewriting a
    // stale request after another supervisor starts can never make it fresh.
    this.#seenControlRequestIds.add(request.requestId);
    if (!requestTargetsSupervisor(request, action, this.#controlTarget())) {
      this.#log("control_rejected", { action, reason: "stale_target", request_id: request.requestId });
      return false;
    }
    this.#log("control_accepted", { action, request_id: request.requestId });
    return true;
  }

  #emitSample(): void {
    const snapshot = this.snapshot();
    const forwarded = snapshot.forwardedTotal - this.#sampleForwarded;
    const errors = snapshot.proxyErrorsTotal - this.#sampleErrors;
    this.#sampleForwarded = snapshot.forwardedTotal;
    this.#sampleErrors = snapshot.proxyErrorsTotal;
    this.#log("sample", {
      ...snapshot,
      interval_ms: this.#options.sampleIntervalMs,
      requests_per_second: forwarded / (this.#options.sampleIntervalMs / 1_000),
      proxy_errors_interval: errors,
    });
    this.#queuedRequestsPeak = this.#queuedRequests;
    this.#queuedBytesPeak = this.#queuedBytes;
    this.#writeStatus();
  }

  #writeStatus(): void {
    const payload = JSON.stringify({
      schema: SUPERVISOR_SCHEMA,
      updatedAt: new Date().toISOString(),
      ...this.snapshot(),
    }, null, 2);
    if (this.#statusWriteRunning) {
      this.#statusPendingPayload = payload;
      this.#statusWritesCoalesced++;
      return;
    }
    this.#statusWriteRunning = true;
    const target = this.#options.statusFile;
    const temporary = `${target}.${process.pid}.tmp`;
    this.#statusWrite = (async () => {
      try {
        let next: string | null = payload;
        while (next !== null) {
          const current = next;
          this.#statusPendingPayload = null;
          try {
            await writeFile(temporary, current, { encoding: "utf8", mode: 0o600 });
            await rename(temporary, target);
            this.#statusWritesTotal++;
          } catch {
            // Status is diagnostic and must never crash supervision. A dirty
            // replacement queued during the failed write is still attempted.
          }
          next = this.#statusPendingPayload;
        }
      } finally {
        // This runs in the same continuation that observed no pending payload,
        // so another caller either dirtied the active pump before this point or
        // sees running=false and starts the next pump itself.
        this.#statusWriteRunning = false;
      }
    })();
  }

  async #flushStatusWrites(): Promise<void> {
    for (;;) {
      const current = this.#statusWrite;
      await current;
      if (!this.#statusWriteRunning && this.#statusWrite === current) return;
    }
  }

  #log(event: string, fields: Record<string, unknown>): void {
    const line = `${JSON.stringify({
      schema: SUPERVISOR_SCHEMA,
      event: `host.supervisor.${event}`,
      ts: new Date().toISOString(),
      ...fields,
    })}\n`;
    process.stdout.write(line);
    const target = this.#options.logFile;
    this.#logWrite = this.#logWrite.then(async () => {
      const currentBytes = await stat(target).then(value => value.size).catch(() => 0);
      if (currentBytes + Buffer.byteLength(line) > 64 * 1024 * 1024) {
        const rotated = `${target}.1`;
        await rm(rotated, { force: true });
        await rename(target, rotated).catch(() => undefined);
      }
      await appendFile(target, line, { encoding: "utf8", mode: 0o600 });
    }).catch(() => undefined);
  }
}
