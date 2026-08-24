import { spawn, type ChildProcess } from "node:child_process";
import { appendFile, mkdir, rename, rm, stat, writeFile } from "node:fs/promises";
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
  workerPid: number | null;
  generation: number;
  externalPort: number | null;
  internalPort: number | null;
  restarts: number;
  upgrades: number;
  consecutiveFailures: number;
  queuedRequests: number;
  queuedRequestsPeak: number;
  activeProxyRequests: number;
  activeWebSockets: number;
  forwardedTotal: number;
  proxyErrorsTotal: number;
  lastReadyAt: string | null;
  lastExitCode: number | null;
  lastExitSignal: NodeJS.Signals | null;
}

export interface GhostProvHostSupervisorOptions {
  externalPort: number;
  host?: string;
  cwd?: string;
  workerEntry?: string;
  workerEnv?: NodeJS.ProcessEnv;
  readyTimeoutMs?: number;
  gracefulShutdownMs?: number;
  requestWaitMs?: number;
  restartBaseMs?: number;
  restartMaxMs?: number;
  stableResetMs?: number;
  sampleIntervalMs?: number;
  controlFile?: string;
  stopFile?: string;
  statusFile?: string;
  logFile?: string;
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
  return String(value ?? "unknown")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\b(?:Bearer|Basic)\s+\S+/gi, "[REDACTED]")
    .trim()
    .slice(0, 128) || "unknown";
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
  #closePromise: Promise<void> | null = null;
  #restartPromise: Promise<void> | null = null;
  #controlTimer: ReturnType<typeof setInterval> | null = null;
  #sampleTimer: ReturnType<typeof setInterval> | null = null;
  #controlMtimeMs = 0;
  #stopMtimeMs = 0;
  #forceTimer: ReturnType<typeof setTimeout> | null = null;
  #statusWrite: Promise<void> = Promise.resolve();
  #logWrite: Promise<void> = Promise.resolve();
  #externalPort: number | null = null;
  #queuedRequests = 0;
  #queuedRequestsPeak = 0;
  #activeProxyRequests = 0;
  #activeWebSockets = 0;
  #forwardedTotal = 0;
  #proxyErrorsTotal = 0;
  #sampleForwarded = 0;
  #sampleErrors = 0;
  #lastReadyAt: string | null = null;
  #lastExitCode: number | null = null;
  #lastExitSignal: NodeJS.Signals | null = null;

  constructor(options: GhostProvHostSupervisorOptions) {
    if (!Number.isSafeInteger(options.externalPort) || options.externalPort < 0 || options.externalPort > 65_535) {
      throw new RangeError("externalPort must be an integer from 0 through 65535");
    }
    if (options.host && !["127.0.0.1", "::1", "localhost"].includes(options.host.toLowerCase())) {
      throw new TypeError("GhostProv supervisor host must be loopback");
    }
    const cwd = path.resolve(options.cwd ?? process.cwd());
    this.#options = {
      externalPort: options.externalPort,
      host: options.host ?? "127.0.0.1",
      cwd,
      workerEntry: path.resolve(cwd, options.workerEntry ?? "src/server.ts"),
      workerEnv: { ...options.workerEnv },
      readyTimeoutMs: options.readyTimeoutMs ?? 120_000,
      gracefulShutdownMs: options.gracefulShutdownMs ?? 330_000,
      requestWaitMs: options.requestWaitMs ?? 480_000,
      restartBaseMs: options.restartBaseMs ?? 500,
      restartMaxMs: options.restartMaxMs ?? 30_000,
      stableResetMs: options.stableResetMs ?? 60_000,
      sampleIntervalMs: options.sampleIntervalMs ?? 10_000,
      controlFile: path.resolve(cwd, options.controlFile ?? ".ghostprov/upgrade.request"),
      stopFile: path.resolve(cwd, options.stopFile ?? ".ghostprov/stop.request"),
      statusFile: path.resolve(cwd, options.statusFile ?? ".ghostprov/supervisor.json"),
      logFile: path.resolve(cwd, options.logFile ?? ".ghostprov/logs/supervisor.jsonl"),
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
      workerPid: this.#child?.pid ?? null,
      generation: this.#generation,
      externalPort: this.#externalPort,
      internalPort: this.#target?.port ?? null,
      restarts: this.#restarts,
      upgrades: this.#upgrades,
      consecutiveFailures: this.#consecutiveFailures,
      queuedRequests: this.#queuedRequests,
      queuedRequestsPeak: this.#queuedRequestsPeak,
      activeProxyRequests: this.#activeProxyRequests,
      activeWebSockets: this.#activeWebSockets,
      forwardedTotal: this.#forwardedTotal,
      proxyErrorsTotal: this.#proxyErrorsTotal,
      lastReadyAt: this.#lastReadyAt,
      lastExitCode: this.#lastExitCode,
      lastExitSignal: this.#lastExitSignal,
    };
  }

  async start(): Promise<void> {
    if (this.#runPromise) {
      await this.#waitForTarget(1, this.#options.readyTimeoutMs);
      return;
    }
    await mkdir(path.dirname(this.#options.controlFile), { recursive: true });
    await mkdir(path.dirname(this.#options.statusFile), { recursive: true });
    await mkdir(path.dirname(this.#options.logFile), { recursive: true });
    this.#controlMtimeMs = await stat(this.#options.controlFile)
      .then(value => value.mtimeMs)
      .catch(() => 0);
    this.#stopMtimeMs = await stat(this.#options.stopFile)
      .then(value => value.mtimeMs)
      .catch(() => 0);
    await new Promise<void>((resolve, reject) => {
      this.#server.once("error", reject);
      this.#server.listen(this.#options.externalPort, this.#options.host, () => {
        this.#server.off("error", reject);
        const address = this.#server.address();
        this.#externalPort = address && typeof address !== "string" ? address.port : null;
        resolve();
      });
    });
    this.#controlTimer = setInterval(() => void this.#pollControlFile(), 1_000);
    this.#controlTimer.unref?.();
    this.#sampleTimer = setInterval(() => this.#emitSample(), this.#options.sampleIntervalMs);
    this.#sampleTimer.unref?.();
    this.#runPromise = this.#runLoop();
    await this.#waitForTarget(1, this.#options.readyTimeoutMs);
  }

  requestRestart(reason = "upgrade-request"): Promise<void> {
    if (this.#stopping) return Promise.reject(new Error("Supervisor is stopping"));
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
      const closingServer = new Promise<void>(resolve => {
        if (!this.#server.listening) return resolve();
        this.#server.close(() => resolve());
      });
      await Promise.race([
        this.#runPromise ?? Promise.resolve(),
        sleep(this.#options.gracefulShutdownMs + 1_000),
      ]);
      if (this.#child) this.#child.kill();
      for (const socket of this.#proxySockets) socket.destroy();
      this.#proxySockets.clear();
      this.#server.closeAllConnections?.();
      await closingServer;
      this.#state = "stopped";
      this.#writeStatus();
      await this.#statusWrite;
      await this.#logWrite;
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
      }
    }
  }

  async #launchWorker(): Promise<ChildExit> {
    this.#state = "starting";
    this.#setTarget(null);
    const port = await freeLoopbackPort();
    const generation = ++this.#generation;
    if (generation > 1) this.#restarts++;
    const startedAt = performance.now();
    const child = spawn(process.execPath, [this.#options.workerEntry], {
      cwd: this.#options.cwd,
      env: {
        ...process.env,
        ...this.#options.workerEnv,
        PORT: String(port),
        GP_SUPERVISED_CHILD: "1",
        GP_SUPERVISOR_GENERATION: String(generation),
      },
      stdio: ["inherit", "inherit", "inherit", "ipc"],
      windowsHide: true,
    });
    this.#child = child;
    this.#log("worker_starting", { generation, pid: child.pid ?? null, internal_port: port });
    this.#writeStatus();
    let ready = false;
    let readyTimer: ReturnType<typeof setTimeout> | null = null;
    let pollTimer: ReturnType<typeof setInterval> | null = null;
    const markReady = () => {
      if (ready || this.#child !== child || this.#stopping) return;
      ready = true;
      if (readyTimer) clearTimeout(readyTimer);
      if (pollTimer) clearInterval(pollTimer);
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
      const value = message as { type?: unknown; port?: unknown; pid?: unknown };
      if (
        value.type === "ghostprov.ready" &&
        value.port === port &&
        (value.pid === child.pid || child.pid === undefined)
      ) markReady();
    });
    pollTimer = setInterval(() => {
      void fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(1_000) })
        .then(response => { if (response.status === 200 || response.status === 503) markReady(); })
        .catch(() => undefined);
    }, 500);
    pollTimer.unref?.();
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
    if (pollTimer) clearInterval(pollTimer);
    if (this.#forceTimer) clearTimeout(this.#forceTimer);
    this.#forceTimer = null;
    if (this.#target?.generation === generation) {
      this.#target.agent.destroy();
      this.#setTarget(null);
    }
    if (this.#child === child) this.#child = null;
    this.#lastExitCode = exit.code;
    this.#lastExitSignal = exit.signal;
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

  async #forwardHttp(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (request.url === "/__supervisor") {
      response.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      response.end(JSON.stringify({ schema: SUPERVISOR_SCHEMA, ...this.snapshot() }));
      return;
    }
    if (!this.#target && request.url?.split("?", 1)[0] === "/health") {
      response.writeHead(503, { "content-type": "application/json", "retry-after": "1" });
      response.end(JSON.stringify({ ok: false, supervisor: this.#state, retryable: true }));
      return;
    }
    const controller = new AbortController();
    const abort = () => controller.abort();
    request.once("aborted", abort);
    response.once("close", abort);
    const queuedAt = performance.now();
    this.#queuedRequests++;
    this.#queuedRequestsPeak = Math.max(this.#queuedRequestsPeak, this.#queuedRequests);
    this.#writeStatus();
    let target: WorkerTarget;
    try {
      target = await this.#waitForTarget(this.#generation, this.#options.requestWaitMs, controller.signal);
    } catch {
      if (!response.headersSent && !response.destroyed) {
        response.writeHead(503, {
          "content-type": "application/json",
          "retry-after": "1",
          connection: "close",
        });
        response.end(JSON.stringify({ error: {
          message: "GhostProv worker is restarting",
          type: "service_unavailable_error",
          param: null,
          code: "host_restarting",
        }}));
      }
      return;
    } finally {
      this.#queuedRequests = Math.max(0, this.#queuedRequests - 1);
      request.off("aborted", abort);
      response.off("close", abort);
    }
    if (controller.signal.aborted) return;
    this.#activeProxyRequests++;
    this.#forwardedTotal++;
    const waitMs = Math.max(0, performance.now() - queuedAt);
    const headers = { ...request.headers };
    headers.host = `127.0.0.1:${target.port}`;
    if (request.headers.host) headers["x-forwarded-host"] = request.headers.host;
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
      upstreamResponse.pipe(response);
      upstreamResponse.once("end", finish);
    });
    upstream.once("error", error => {
      this.#proxyErrorsTotal++;
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
    const controller = new AbortController();
    const abort = () => controller.abort();
    socket.once("close", abort);
    this.#queuedRequests++;
    this.#queuedRequestsPeak = Math.max(this.#queuedRequestsPeak, this.#queuedRequests);
    let target: WorkerTarget;
    try {
      target = await this.#waitForTarget(this.#generation, this.#options.requestWaitMs, controller.signal);
    } catch {
      if (!socket.destroyed) {
        socket.end("HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\nConnection: close\r\n\r\n");
      }
      return;
    } finally {
      this.#queuedRequests = Math.max(0, this.#queuedRequests - 1);
      socket.off("close", abort);
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
        header += `${request.rawHeaders[index]}: ${request.rawHeaders[index + 1]}\r\n`;
      }
      upstream.write(`${header}\r\n`);
      if (head.byteLength) upstream.write(head);
      socket.pipe(upstream).pipe(socket);
    });
  }

  async #pollControlFile(): Promise<void> {
    const stopMtimeMs = await stat(this.#options.stopFile)
      .then(value => value.mtimeMs)
      .catch(() => 0);
    if (stopMtimeMs > this.#stopMtimeMs) {
      this.#stopMtimeMs = stopMtimeMs;
      await this.close();
      return;
    }
    const mtimeMs = await stat(this.#options.controlFile)
      .then(value => value.mtimeMs)
      .catch(() => 0);
    if (mtimeMs <= this.#controlMtimeMs) return;
    this.#controlMtimeMs = mtimeMs;
    await this.requestRestart("control-file").catch(error => {
      this.#log("upgrade_failed", { error_name: error instanceof Error ? error.name : "Error" });
    });
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
    this.#writeStatus();
  }

  #writeStatus(): void {
    const payload = JSON.stringify({
      schema: SUPERVISOR_SCHEMA,
      updatedAt: new Date().toISOString(),
      ...this.snapshot(),
    }, null, 2);
    const target = this.#options.statusFile;
    const temporary = `${target}.${process.pid}.tmp`;
    this.#statusWrite = this.#statusWrite.then(async () => {
      await writeFile(temporary, payload, { encoding: "utf8", mode: 0o600 });
      await rename(temporary, target);
    }).catch(() => undefined);
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
