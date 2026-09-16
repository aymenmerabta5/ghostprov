import { randomUUID } from "node:crypto";
import { lstat, mkdir, open, realpath, rename, unlink } from "node:fs/promises";
import * as path from "node:path";
import { FileLease, type FileLeaseProcessProbe } from "../core/file-lease";

export const SHUTDOWN_CONTROL_SCHEMA = 1 as const;
export const MAX_SHUTDOWN_CONTROL_BYTES = 4 * 1024;
export const MAX_SHUTDOWN_WAIT_MS = 3_660_000;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const FILES = { descriptor: "shutdown-control.json", request: "shutdown.request.json", result: "shutdown.result.json" } as const;

export interface GhostProvShutdownTarget {
  readonly pid: number;
  readonly startedAt: string;
  readonly instanceNonce: string;
}

export interface GhostProvShutdownDescriptor {
  readonly schema: typeof SHUTDOWN_CONTROL_SCHEMA;
  readonly mode: "direct" | "supervised";
  readonly target: GhostProvShutdownTarget;
  readonly timeoutMs: number;
}

export interface GhostProvShutdownRequest {
  readonly schema: typeof SHUTDOWN_CONTROL_SCHEMA;
  readonly target: GhostProvShutdownTarget;
  readonly requestId: string;
  readonly requestedAt: string;
}

export interface GhostProvShutdownResult {
  readonly schema: typeof SHUTDOWN_CONTROL_SCHEMA;
  readonly target: GhostProvShutdownTarget;
  readonly requestId: string;
  readonly outcome: "clean" | "failed";
  readonly completedAt: string;
}

export interface GhostProvShutdownControlOptions {
  readonly cwd?: string;
  readonly mode: GhostProvShutdownDescriptor["mode"];
  readonly timeoutMs: number;
  /** Must resolve only after all owned startup and cleanup work is proven complete. */
  readonly onShutdown: () => Promise<void>;
  readonly pollIntervalMs?: number;
  /** Offline tests can drive poll() explicitly and avoid process metadata probes. */
  readonly autoPoll?: boolean;
  readonly processProbe?: FileLeaseProcessProbe;
}

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : null;
}

function keys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  return Object.keys(value).sort().join(",") === [...expected].sort().join(",");
}

function iso(value: unknown): value is string {
  if (typeof value !== "string" || value.length !== 24) return false;
  const time = Date.parse(value);
  return Number.isFinite(time) && new Date(time).toISOString() === value;
}

function uuid(value: unknown): value is string { return typeof value === "string" && UUID.test(value); }

function target(value: unknown): value is GhostProvShutdownTarget {
  const raw = object(value);
  return !!raw && keys(raw, ["pid", "startedAt", "instanceNonce"]) &&
    Number.isSafeInteger(raw.pid) && Number(raw.pid) > 0 && iso(raw.startedAt) && uuid(raw.instanceNonce);
}

function boundedJson(raw: string): Record<string, unknown> | null {
  if (Buffer.byteLength(raw) > MAX_SHUTDOWN_CONTROL_BYTES) return null;
  try { return object(JSON.parse(raw)); } catch { return null; }
}

export function parseGhostProvShutdownDescriptor(raw: string): GhostProvShutdownDescriptor | null {
  const value = boundedJson(raw);
  if (!value || !keys(value, ["schema", "mode", "target", "timeoutMs"]) ||
    value.schema !== SHUTDOWN_CONTROL_SCHEMA || !["direct", "supervised"].includes(String(value.mode)) ||
    !target(value.target) || !Number.isSafeInteger(value.timeoutMs) ||
    Number(value.timeoutMs) < 1 || Number(value.timeoutMs) > MAX_SHUTDOWN_WAIT_MS) return null;
  return value as unknown as GhostProvShutdownDescriptor;
}

export function parseGhostProvShutdownRequest(raw: string): GhostProvShutdownRequest | null {
  const value = boundedJson(raw);
  if (!value || !keys(value, ["schema", "target", "requestId", "requestedAt"]) ||
    value.schema !== SHUTDOWN_CONTROL_SCHEMA || !target(value.target) ||
    !uuid(value.requestId) || !iso(value.requestedAt)) return null;
  return value as unknown as GhostProvShutdownRequest;
}

export function parseGhostProvShutdownResult(raw: string): GhostProvShutdownResult | null {
  const value = boundedJson(raw);
  if (!value || !keys(value, ["schema", "target", "requestId", "outcome", "completedAt"]) ||
    value.schema !== SHUTDOWN_CONTROL_SCHEMA || !target(value.target) ||
    !uuid(value.requestId) || !["clean", "failed"].includes(String(value.outcome)) || !iso(value.completedAt)) return null;
  return value as unknown as GhostProvShutdownResult;
}

export function sameGhostProvShutdownTarget(left: GhostProvShutdownTarget, right: GhostProvShutdownTarget): boolean {
  return left.pid === right.pid && left.startedAt === right.startedAt && left.instanceNonce === right.instanceNonce;
}

function sameDescriptor(left: GhostProvShutdownDescriptor, right: GhostProvShutdownDescriptor): boolean {
  return sameGhostProvShutdownTarget(left.target, right.target) && left.mode === right.mode && left.timeoutMs === right.timeoutMs;
}

async function directory(cwd: string, create: boolean): Promise<string> {
  const root = await realpath(cwd);
  const result = path.join(root, ".ghostprov");
  if (create) await mkdir(result, { mode: 0o700 }).catch(error => {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  });
  const metadata = await lstat(result);
  if (!metadata.isDirectory() || metadata.isSymbolicLink() || await realpath(result) !== result) {
    throw new Error("GhostProv shutdown control directory must be a real workspace directory");
  }
  return result;
}

async function readBounded(file: string): Promise<string | null> {
  let metadata;
  try { metadata = await lstat(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > MAX_SHUTDOWN_CONTROL_BYTES ||
    (process.platform !== "win32" && (metadata.mode & 0o077) !== 0)) {
    throw new Error("GhostProv shutdown control file is not a bounded private regular file");
  }
  const handle = await open(file, "r");
  try {
    const opened = await handle.stat();
    if (!opened.isFile() || opened.dev !== metadata.dev || opened.ino !== metadata.ino ||
      opened.size !== metadata.size || opened.mtimeMs !== metadata.mtimeMs) {
      throw new Error("GhostProv shutdown control file changed while opening");
    }
    const buffer = Buffer.alloc(MAX_SHUTDOWN_CONTROL_BYTES + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    const after = await handle.stat();
    if (bytesRead > MAX_SHUTDOWN_CONTROL_BYTES || after.size !== opened.size || after.mtimeMs !== opened.mtimeMs) {
      throw new Error("GhostProv shutdown control file changed while reading");
    }
    return buffer.subarray(0, bytesRead).toString("utf8");
  } finally { await handle.close(); }
}

async function atomicWrite(file: string, value: unknown): Promise<void> {
  const content = JSON.stringify(value);
  if (Buffer.byteLength(content) > MAX_SHUTDOWN_CONTROL_BYTES) throw new Error("GhostProv shutdown control write exceeds its bound");
  // Validate the existing destination before replacing it; never follow a link.
  await readBounded(file);
  const temporary = `${file}.${randomUUID()}.tmp`;
  try {
    const handle = await open(temporary, "wx", 0o600);
    try { await handle.writeFile(content); await handle.sync(); } finally { await handle.close(); }
    await rename(temporary, file);
    if (process.platform !== "win32") {
      const parent = await open(path.dirname(file), "r");
      try { await parent.sync(); } finally { await parent.close(); }
    }
  } finally {
    await unlink(temporary).catch(error => { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; });
  }
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "EPERM"; }
}

/** One process owner, one pending request, and one terminal result per workspace. */
export class GhostProvShutdownControl {
  readonly descriptor: GhostProvShutdownDescriptor;
  readonly #directory: string;
  readonly #lease: FileLease;
  readonly #onShutdown: () => Promise<void>;
  #timer: ReturnType<typeof setInterval> | null = null;
  #request: GhostProvShutdownRequest | null = null;
  #polling: Promise<void> | null = null;
  #shutdown: Promise<void> | null = null;
  #finish: Promise<void> | null = null;

  constructor(directory: string, descriptor: GhostProvShutdownDescriptor, lease: FileLease,
    onShutdown: () => Promise<void>) {
    this.#directory = directory;
    this.descriptor = Object.freeze({ ...descriptor, target: Object.freeze({ ...descriptor.target }) });
    this.#lease = lease;
    this.#onShutdown = onShutdown;
  }

  startPolling(intervalMs: number): void {
    this.#timer = setInterval(() => { void this.poll().catch(() => undefined); }, intervalMs);
    this.#timer.unref?.();
  }

  async #assertOwner(): Promise<void> {
    await this.#lease.acquire({ timeoutMs: 1 });
    const raw = await readBounded(path.join(this.#directory, FILES.descriptor));
    const current = raw === null ? null : parseGhostProvShutdownDescriptor(raw);
    if (!current || !sameDescriptor(this.descriptor, current)) throw new Error("GhostProv shutdown control owner changed");
  }

  async #readRequest(): Promise<void> {
    if (this.#request) return;
    const raw = await readBounded(path.join(this.#directory, FILES.request));
    const request = raw === null ? null : parseGhostProvShutdownRequest(raw);
    if (request && sameGhostProvShutdownTarget(request.target, this.descriptor.target) &&
      Date.parse(request.requestedAt) >= Date.parse(this.descriptor.target.startedAt)) this.#request = request;
  }

  poll(): Promise<void> {
    if (this.#finish || this.#shutdown) return Promise.resolve();
    if (this.#polling) return this.#polling;
    const polling = (async () => {
      await this.#assertOwner();
      await this.#readRequest();
      if (this.#request) void this.shutdown().catch(() => undefined);
    })();
    this.#polling = polling;
    void polling.finally(() => { if (this.#polling === polling) this.#polling = null; }).catch(() => undefined);
    return polling;
  }

  shutdown(): Promise<void> {
    if (this.#shutdown) return this.#shutdown;
    if (this.#finish) return this.#finish;
    this.#shutdown = (async () => {
      let failure: unknown;
      try { await this.#onShutdown(); }
      catch (error) { failure = error ?? new Error("GhostProv cleanup failed"); }
      await this.finish(failure === undefined);
      if (failure !== undefined) throw failure;
    })();
    return this.#shutdown;
  }

  /** External signal paths report their actual cleanup outcome through this method. */
  finish(success: boolean): Promise<void> {
    if (this.#finish) return this.#finish;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    this.#finish = (async () => {
      try {
        await this.#polling?.catch(() => undefined);
        await this.#assertOwner();
        await this.#readRequest();
        if (this.#request) {
          await atomicWrite(path.join(this.#directory, FILES.result), {
            schema: SHUTDOWN_CONTROL_SCHEMA,
            target: this.descriptor.target,
            requestId: this.#request.requestId,
            outcome: success ? "clean" : "failed",
            completedAt: new Date().toISOString(),
          } satisfies GhostProvShutdownResult);
        }
      } finally { await this.#lease.release(); }
    })();
    return this.#finish;
  }
}

export async function openGhostProvShutdownControl(options: GhostProvShutdownControlOptions): Promise<GhostProvShutdownControl> {
  const descriptor: GhostProvShutdownDescriptor = {
    schema: SHUTDOWN_CONTROL_SCHEMA,
    mode: options.mode,
    target: { pid: process.pid, startedAt: new Date().toISOString(), instanceNonce: randomUUID() },
    timeoutMs: options.timeoutMs,
  };
  if (!parseGhostProvShutdownDescriptor(JSON.stringify(descriptor))) throw new TypeError("Invalid GhostProv shutdown control options");
  const pollIntervalMs = options.pollIntervalMs ?? 250;
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1 || pollIntervalMs > 1_000) {
    throw new TypeError("Invalid GhostProv shutdown poll interval");
  }
  const root = await directory(options.cwd ?? process.cwd(), true);
  // Startup and a stop client's final action share this gate. A successor must
  // not begin owning work between the prior cleanup receipt and OS shutdown.
  // Lock ordering is always request -> owner; finish() releases only owner.
  const requestLease = new FileLease({ file: path.join(root, "shutdown-request.lock"), staleAfterMs: 120_000,
    processProbe: options.processProbe });
  const lease = new FileLease({ file: path.join(root, "shutdown-owner.lock"), staleAfterMs: 120_000,
    processProbe: options.processProbe });
  await requestLease.acquire({ timeoutMs: 1_000 });
  let ownsLease = false;
  let control: GhostProvShutdownControl;
  try {
    await lease.acquire({ timeoutMs: 1_000 });
    ownsLease = true;
    await atomicWrite(path.join(root, FILES.descriptor), descriptor);
    control = new GhostProvShutdownControl(root, descriptor, lease, options.onShutdown);
  } catch (error) {
    if (ownsLease) await lease.release();
    throw error;
  } finally { await requestLease.release(); }
  if (options.autoPoll !== false) control.startPolling(pollIntervalMs);
  return control;
}

export interface RequestGhostProvShutdownOptions {
  readonly timeoutMs?: number;
  readonly signal?: AbortSignal;
  readonly processProbe?: FileLeaseProcessProbe;
  readonly isProcessAlive?: (pid: number) => boolean;
  readonly monotonicNow?: () => number;
  /** Runs under the startup gate after proof, for example to request OS shutdown. */
  readonly afterCleanup?: (result: GhostProvShutdownResult) => Promise<void>;
}

/** Success is a durable, exact-instance cleanup receipt; disappearance is never proof. */
export async function requestGhostProvShutdown(cwd = process.cwd(), options: RequestGhostProvShutdownOptions = {}): Promise<GhostProvShutdownResult> {
  options.signal?.throwIfAborted();
  const root = await directory(cwd, false);
  const descriptorFile = path.join(root, FILES.descriptor);
  const raw = await readBounded(descriptorFile);
  const descriptor = raw === null ? null : parseGhostProvShutdownDescriptor(raw);
  const isAlive = options.isProcessAlive ?? alive;
  if (!descriptor || !isAlive(descriptor.target.pid)) throw new Error("No live GhostProv shutdown control is available");
  const timeoutMs = options.timeoutMs ?? descriptor.timeoutMs;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_SHUTDOWN_WAIT_MS) throw new TypeError("Invalid GhostProv shutdown wait timeout");
  const now = options.monotonicNow ?? (() => performance.now());
  const deadline = now() + timeoutMs;
  const lease = new FileLease({ file: path.join(root, "shutdown-request.lock"), staleAfterMs: 120_000,
    processProbe: options.processProbe });
  await lease.acquire({ timeoutMs: Math.min(timeoutMs, 1_000), signal: options.signal });
  try {
    const assertDescriptor = async () => {
      const latestRaw = await readBounded(descriptorFile);
      const latest = latestRaw === null ? null : parseGhostProvShutdownDescriptor(latestRaw);
      if (!latest || !sameDescriptor(descriptor, latest)) throw new Error("GhostProv instance changed; shutdown is unproven");
    };
    await assertDescriptor();
    options.signal?.throwIfAborted();
    if (now() >= deadline) throw new Error("GhostProv shutdown request deadline expired before submission");
    const request: GhostProvShutdownRequest = { schema: SHUTDOWN_CONTROL_SCHEMA, target: descriptor.target,
      requestId: randomUUID(), requestedAt: new Date().toISOString() };
    await atomicWrite(path.join(root, FILES.request), request);
    while (true) {
      options.signal?.throwIfAborted();
      await assertDescriptor();
      const resultRaw = await readBounded(path.join(root, FILES.result));
      const result = resultRaw === null ? null : parseGhostProvShutdownResult(resultRaw);
      if (result && sameGhostProvShutdownTarget(result.target, descriptor.target) && result.requestId === request.requestId &&
        Date.parse(result.completedAt) >= Date.parse(request.requestedAt)) {
        if (result.outcome !== "clean") throw new Error("GhostProv reported incomplete cleanup; shutdown is unproven");
        await assertDescriptor();
        // A receipt cannot authorize the final action after this requester has
        // lost the startup gate. Re-acquiring an already-held lease asserts its
        // exact ownership; waiting until finally would detect loss too late.
        await lease.acquire({ timeoutMs: 1 });
        options.signal?.throwIfAborted();
        if (now() >= deadline) throw new Error("GhostProv cleanup receipt arrived after the shutdown deadline");
        await options.afterCleanup?.(result);
        return result;
      }
      if (!isAlive(descriptor.target.pid)) throw new Error("GhostProv exited without an exact cleanup receipt");
      const remaining = deadline - now();
      if (remaining <= 0) throw new Error("GhostProv cleanup receipt timed out; shutdown is unproven");
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(done, Math.min(100, remaining));
        function done() { options.signal?.removeEventListener("abort", abort); resolve(); }
        function abort() { clearTimeout(timer); options.signal?.removeEventListener("abort", abort); reject(options.signal?.reason); }
        options.signal?.addEventListener("abort", abort, { once: true });
        if (options.signal?.aborted) abort();
      });
    }
  } finally { await lease.release(); }
}
