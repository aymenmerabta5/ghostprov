import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { SYSTEM_CLOCK } from "../../src/core/ports";
import { validateBootstrapBinding, type BootstrapBinding } from "../../src/v2/bootstrap/crypto";
import type { V2BootstrapGateway } from "../../src/v2/bootstrap/controller";
import { AtomicBytePermitPool, type AtomicBytePermit } from "../../src/v2/memory/atomic-byte-permits";
import { V2_R10_RELEASE, type V2SidecarAssets } from "../../src/v2/sidecar/assets";
import { directSupervisorLaunchFiles, DIRECT_SUPERVISOR_INSTALL_PATH, DIRECT_SUPERVISOR_LAUNCH_PATH, isRegisteredR10Entrypoint } from "../../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig, SUPERVISOR_STATIC_PATH } from "../../src/v2/supervisor/r10-launcher";

const MiB = 1024 * 1024;
const CHECKPOINTS = [0, 5, 20] as const;
const HORIZON_MS = 25_000, READ_MS = 5_000;
const SESSION = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const SHA256 = /^[0-9a-f]{64}$(?![\s\S])/;
const PHASES = new Set(["awaiting_acceptance", "accepted", "readiness_pending", "readiness_proven", "readiness_failed", "controller_closed"]);
// Shared across capture instances, not one independent budget per execution.
const MEMORY = new AtomicBytePermitPool({ ordinaryBytes: 48 * MiB, jumboBytes: 0, maxWaiters: 8 });
const hash = (bytes: string | Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const object = (value: unknown): Record<string, unknown> | null => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;

export type StartupArtifactLabel = "bundle" | "supervisor_static" | "exec" | "installer";
export type StartupArtifactWorker = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8;
export interface ArtifactExpectation {
  readonly label: StartupArtifactLabel;
  readonly path: string;
  readonly expectedBytes: number;
  readonly expectedSha256: string;
}
export type StartupArtifactExpectations = readonly [ArtifactExpectation, ArtifactExpectation, ArtifactExpectation, ArtifactExpectation];

function limits(label: StartupArtifactLabel) {
  return label === "bundle" ? { wire: 2 * MiB, decoded: MiB, permit: 16 * MiB }
    : { wire: 64 * 1024, decoded: 16 * 1024, permit: MiB };
}
function validPath(label: StartupArtifactLabel, path: string): boolean {
  return label === "bundle" ? isRegisteredR10Entrypoint(path)
    : label === "supervisor_static" ? path === SUPERVISOR_STATIC_PATH
      : label === "exec" ? path === DIRECT_SUPERVISOR_LAUNCH_PATH
        : label === "installer" && path === DIRECT_SUPERVISOR_INSTALL_PATH;
}
function expectation(label: StartupArtifactLabel, path: string, content: string): ArtifactExpectation {
  const expectedBytes = Buffer.byteLength(content);
  if (!validPath(label, path) || expectedBytes > limits(label).decoded) throw new Error("Startup artifact expectation exceeds its bound");
  return Object.freeze({ label, path, expectedBytes, expectedSha256: hash(content) });
}

/** Only public bundle/static/launcher bytes. No credential object or file is inspected. */
export function createStartupArtifactExpectations(assets: V2SidecarAssets, binding: BootstrapBinding, connectTimeoutMs: number): StartupArtifactExpectations {
  validateBootstrapBinding(binding);
  if (assets.revision !== "r10" || assets.buildHash !== V2_R10_RELEASE.buildHash || assets.schemaHash !== V2_R10_RELEASE.schemaHash ||
    binding.buildHash !== assets.buildHash || binding.schemaHash !== assets.schemaHash || !isRegisteredR10Entrypoint(assets.entrypointPath)) {
    throw new Error("Startup artifact release does not match its binding");
  }
  const matches = assets.files.filter(file => file.path === assets.entrypointPath);
  if (matches.length !== 1) throw new Error("Startup artifact bundle is missing or duplicated");
  const bundle = matches[0]!, expectedBundle = expectation("bundle", bundle.path, bundle.content);
  if (expectedBundle.expectedSha256 !== bundle.sha256) throw new Error("Startup artifact bundle hash is invalid");
  const files = directSupervisorLaunchFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: binding.nonce });
  return Object.freeze([
    expectedBundle,
    expectation("supervisor_static", SUPERVISOR_STATIC_PATH, supervisorStaticConfig({ binding, entrypointPath: assets.entrypointPath, connectTimeoutMs })),
    expectation("exec", DIRECT_SUPERVISOR_LAUNCH_PATH, files.launcher),
    expectation("installer", DIRECT_SUPERVISOR_INSTALL_PATH, files.installer),
  ]);
}

type Outcome = "matched" | "mismatch" | "empty" | "missing" | "http_error" | "timeout" | "cancelled" |
  "wire_overflow" | "decoded_overflow" | "invalid_response" | "identity_mismatch" | "invalid_encoding" |
  "request_scope_mismatch" | "account_unobserved" | "read_failed" | "previous_read_pending" | "horizon_expired" | "schedule_failed";
class CaptureError extends Error { constructor(readonly outcome: Outcome) { super(outcome); } }
interface ReadResult { status: number | null; outcome: Outcome; bytes: number | null; sha256: string | null; equal: boolean | null }
const failedResult = (outcome: Outcome): ReadResult => ({ status: null, outcome, bytes: null, sha256: null, equal: null });
const validObservation = (outcome: Outcome) => outcome === "matched" || outcome === "mismatch" || outcome === "empty" || outcome === "missing";

interface SessionCapture {
  readonly worker: StartupArtifactWorker;
  readonly sessionId: string;
  readonly gateway: Pick<V2BootstrapGateway, "fetchText">;
  readonly expectations: StartupArtifactExpectations;
  readonly phase: () => string;
  readonly acceptedAt: number;
  readonly active: Set<ReadScope>;
  done: Promise<void>;
}
interface ReadScope {
  readonly session: SessionCapture;
  readonly expected: ArtifactExpectation;
  readonly signal: AbortSignal;
  permit: AtomicBytePermit | null;
  readSettled: boolean;
  accountOwners: number;
  accountCalls: number;
  status: number | null;
}
export interface StartupArtifactCaptureOptions {
  readonly record: (row: Record<string, unknown>) => void;
  readonly signal: AbortSignal;
  readonly nowMs?: () => number;
  readonly sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}
export interface StartupArtifactAcceptance {
  readonly worker: StartupArtifactWorker;
  readonly sessionId: string;
  readonly acceptedAtMs?: number;
  readonly gateway: Pick<V2BootstrapGateway, "fetchText">;
  readonly expectations: StartupArtifactExpectations;
  readonly phase: () => string;
}

/** Two exact accepted sessions, twelve observations each, and no execution or retries. */
export class StartupArtifactCapture {
  readonly #options: StartupArtifactCaptureOptions;
  readonly #context = new AsyncLocalStorage<ReadScope>();
  readonly #sessions: SessionCapture[] = [];
  readonly #rows: Readonly<Record<string, unknown>>[] = [];
  readonly #now: () => number;
  readonly #sleep: (ms: number, signal?: AbortSignal) => Promise<void>;
  #completedSessions = 0;
  #errors = 0;
  #rejected = 0;
  #recordErrors = 0;
  #peakReservedBytes = 0;
  #settling = false;

  constructor(options: StartupArtifactCaptureOptions) {
    this.#options = options;
    this.#now = options.nowMs ?? (() => performance.now());
    this.#sleep = options.sleep ?? ((ms, signal) => SYSTEM_CLOCK.sleep(ms, signal));
  }

  accept(input: StartupArtifactAcceptance): { initial: Promise<void>; done: Promise<void> } {
    try {
      if (this.#settling || this.#sessions.length >= 2 || !Number.isInteger(input.worker) || input.worker < 1 || input.worker > 8 || !SESSION.test(input.sessionId) ||
        this.#sessions.some(session => session.sessionId === input.sessionId || session.worker === input.worker) || typeof input.gateway.fetchText !== "function" ||
        !Array.isArray(input.expectations) || input.expectations.length !== 4) throw new Error("Invalid capture acceptance");
      const labels = ["bundle", "supervisor_static", "exec", "installer"] as const;
      const expectations = input.expectations.map((value, index) => {
        if (!value || value.label !== labels[index] || !validPath(value.label, value.path) || !SHA256.test(value.expectedSha256) ||
          !Number.isSafeInteger(value.expectedBytes) || value.expectedBytes < 0 || value.expectedBytes > limits(value.label).decoded) throw new Error("Invalid artifact expectation");
        return Object.freeze({ label: value.label, path: value.path, expectedBytes: value.expectedBytes, expectedSha256: value.expectedSha256 });
      }) as unknown as StartupArtifactExpectations;
      const observedNow = this.#now(), acceptedAt = input.acceptedAtMs === undefined ? observedNow : input.acceptedAtMs;
      if (!Number.isFinite(observedNow) || observedNow < 0 || !Number.isFinite(acceptedAt) || acceptedAt < 0 || acceptedAt > observedNow) throw new Error("Invalid capture clock");
      const session: SessionCapture = { ...input, expectations: Object.freeze(expectations), acceptedAt, active: new Set(), done: Promise.resolve() };
      this.#sessions.push(session);
      const initial = this.#batch(session, 0).catch(() => { this.#errors++; });
      session.done = this.#schedule(session, initial).catch(() => { this.#errors++; }).finally(() => { this.#completedSessions++; });
      return { initial, done: session.done };
    } catch {
      this.#rejected = Math.min(Number.MAX_SAFE_INTEGER, this.#rejected + 1);
      this.#errors = Math.min(Number.MAX_SAFE_INTEGER, this.#errors + 1);
      const done = Promise.resolve(); return { initial: done, done };
    }
  }

  /** Must surround the account call used by the retained gateway. Ordinary traffic passes through. */
  async wrapAccountRequest(url: string, init: RequestInit, next: () => Promise<Response>): Promise<Response> {
    const scope = this.#context.getStore();
    if (!scope) return next();
    let target: URL;
    try { target = new URL(url); } catch { throw new CaptureError("request_scope_mismatch"); }
    if ((init.method ?? "GET").toUpperCase() !== "GET" || !target.pathname.endsWith("/codex/read_codex_workspace_file") ||
      target.searchParams.getAll("session_id").length !== 1 || target.searchParams.get("session_id") !== scope.session.sessionId ||
      target.searchParams.getAll("path").length !== 1 || target.searchParams.get("path") !== scope.expected.path ||
      scope.accountCalls !== 0 || !scope.permit?.active) throw new CaptureError("request_scope_mismatch");
    scope.accountCalls++;
    scope.accountOwners++;
    try {
      scope.signal.throwIfAborted();
      const response = await next();
      scope.status = response.status;
      return await this.#boundedResponse(response, scope);
    } finally {
      scope.accountOwners--;
      this.#release(scope);
    }
  }

  async settle(): Promise<void> {
    this.#settling = true;
    await Promise.all(this.#sessions.map(session => session.done));
  }

  snapshot() {
    const active = this.#sessions.flatMap(session => [...session.active]);
    const accountOwners = active.reduce((total, scope) => total + scope.accountOwners, 0);
    const expectedRows = this.#sessions.length * 12;
    return { sessions: this.#sessions.length, completedSessions: this.#completedSessions, expectedRows, rowCount: this.#rows.length,
      complete: this.#sessions.length > 0 && this.#completedSessions === this.#sessions.length && this.#rows.length === expectedRows &&
        this.#errors === 0 && this.#recordErrors === 0 && active.length === 0 && accountOwners === 0,
      errors: this.#errors, rejected: this.#rejected, recordErrors: this.#recordErrors, activeReads: active.length, accountOwners,
      rows: this.#rows.map(row => ({ ...row })), memory: MEMORY.snapshot(), peakReservedBytes: this.#peakReservedBytes };
  }

  async #schedule(session: SessionCapture, initial: Promise<void>): Promise<void> {
    await initial;
    for (const checkpoint of CHECKPOINTS.slice(1)) {
      try {
        const delay = session.acceptedAt + checkpoint * 1000 - this.#now();
        if (delay > 0) await this.#sleep(delay, this.#options.signal);
        await this.#batch(session, checkpoint);
      } catch {
        this.#skip(session, checkpoint, this.#options.signal.aborted ? "cancelled" : "schedule_failed");
      }
    }
  }

  async #batch(session: SessionCapture, checkpoint: 0 | 5 | 20): Promise<void> {
    if (this.#options.signal.aborted) { this.#skip(session, checkpoint, "cancelled"); return; }
    if (this.#now() >= session.acceptedAt + HORIZON_MS) { this.#skip(session, checkpoint, "horizon_expired"); return; }
    if (session.active.size !== 0) { this.#skip(session, checkpoint, "previous_read_pending"); return; }
    await Promise.all(session.expectations.map(expected => this.#read(session, checkpoint, expected)));
  }

  #skip(session: SessionCapture, checkpoint: 0 | 5 | 20, outcome: Outcome): void {
    for (const expected of session.expectations) this.#record(session, checkpoint, expected, this.#now(), failedResult(outcome));
  }

  async #read(session: SessionCapture, checkpoint: 0 | 5 | 20, expected: ArtifactExpectation): Promise<void> {
    const startedAt = this.#now(), controller = new AbortController(), timer = new AbortController();
    const scope: ReadScope = { session, expected, signal: controller.signal, permit: null, readSettled: false, accountOwners: 0, accountCalls: 0, status: null };
    session.active.add(scope);
    const abort = () => controller.abort(new CaptureError("cancelled"));
    this.#options.signal.addEventListener("abort", abort, { once: true });
    if (this.#options.signal.aborted) abort();
    let stopWaiting!: () => void;
    const boundary = new Promise<ReadResult>(resolve => {
      stopWaiting = () => resolve(failedResult(controller.signal.reason instanceof CaptureError ? controller.signal.reason.outcome : "cancelled"));
      controller.signal.addEventListener("abort", stopWaiting, { once: true });
      if (controller.signal.aborted) stopWaiting();
    });
    const timeoutMs = Math.max(0, Math.min(READ_MS, session.acceptedAt + HORIZON_MS - startedAt));
    const deadlineAt = startedAt + timeoutMs;
    const assertBeforeDeadline = () => {
      controller.signal.throwIfAborted();
      // Timer callbacks can be delayed by an occupied event loop. They never
      // extend the read's absolute deadline or the acceptance-based horizon.
      if (this.#now() >= deadlineAt) {
        controller.abort(new CaptureError("timeout"));
        throw controller.signal.reason;
      }
    };
    void this.#sleep(timeoutMs, timer.signal).then(() => controller.abort(new CaptureError("timeout")), () => {});
    const operation = (async (): Promise<ReadResult> => {
      try {
        scope.permit = await MEMORY.acquire(limits(expected.label).permit, { signal: controller.signal, label: "startup-artifact" });
        this.#peakReservedBytes = Math.max(this.#peakReservedBytes, MEMORY.snapshot().ordinaryUsedBytes);
        assertBeforeDeadline();
        const suffix = `codex/read_codex_workspace_file?path=${encodeURIComponent(expected.path)}&session_id=${encodeURIComponent(session.sessionId)}`;
        const response = await this.#context.run(scope, () => session.gateway.fetchText(suffix, { method: "GET", signal: controller.signal }));
        assertBeforeDeadline();
        if (scope.accountCalls !== 1) throw new CaptureError("account_unobserved");
        if (response.status === 404) return { ...failedResult("missing"), status: 404 };
        if (response.status !== 200) return { ...failedResult("http_error"), status: Number.isInteger(response.status) && response.status >= 100 && response.status <= 599 ? response.status : null };
        const result = this.#decode(response.text, scope);
        assertBeforeDeadline();
        return result;
      } catch (error) {
        return { ...failedResult(error instanceof CaptureError ? error.outcome : controller.signal.aborted
          ? controller.signal.reason instanceof CaptureError ? controller.signal.reason.outcome : "cancelled" : "read_failed"), status: scope.status };
      } finally { scope.readSettled = true; this.#release(scope); }
    })();
    try { this.#record(session, checkpoint, expected, startedAt, await Promise.race([operation, boundary])); }
    finally {
      timer.abort();
      controller.signal.removeEventListener("abort", stopWaiting);
      this.#options.signal.removeEventListener("abort", abort);
    }
  }

  async #boundedResponse(response: Response, scope: ReadScope): Promise<Response> {
    const maximum = limits(scope.expected.label).wire;
    if (scope.signal.aborted || response.status !== 200) {
      await response.body?.cancel().catch(() => undefined);
      scope.signal.throwIfAborted();
      return new Response(null, { status: response.status, statusText: response.statusText, headers: response.headers });
    }
    const length = response.headers.get("content-length");
    if (length !== null && (!/^\d+$(?![\s\S])/.test(length) || Number(length) > maximum)) {
      await response.body?.cancel().catch(() => undefined); throw new CaptureError("wire_overflow");
    }
    if (!response.body) throw new CaptureError("invalid_response");
    const reader = response.body.getReader(), bytes = new Uint8Array(maximum); let used = 0;
    let cancelling: Promise<void> | null = null;
    const cancel = () => cancelling ??= reader.cancel().catch(() => undefined);
    const abort = () => { void cancel(); };
    scope.signal.addEventListener("abort", abort, { once: true });
    try {
      for (;;) {
        scope.signal.throwIfAborted();
        const item = await reader.read(); if (item.done) break;
        if (used + item.value.byteLength > maximum) throw new CaptureError("wire_overflow");
        bytes.set(item.value, used); used += item.value.byteLength;
      }
      scope.signal.throwIfAborted();
      return new Response(bytes.slice(0, used), { status: response.status, statusText: response.statusText, headers: response.headers });
    } finally { scope.signal.removeEventListener("abort", abort); await cancel(); reader.releaseLock(); }
  }

  #decode(text: string, scope: ReadScope): ReadResult {
    const { expected, session } = scope, limit = limits(expected.label);
    if (Buffer.byteLength(text) > limit.wire) throw new CaptureError("wire_overflow");
    let value: Record<string, unknown> | null;
    try { value = object(JSON.parse(text)); } catch { throw new CaptureError("invalid_response"); }
    if (!value || value.session_id !== session.sessionId || value.path !== `/codex_workspace/${session.sessionId}/${expected.path}` ||
      (value.relative_path !== undefined && value.relative_path !== `${session.sessionId}/${expected.path}`)) throw new CaptureError("identity_mismatch");
    if (typeof value.bytes !== "number" || !Number.isSafeInteger(value.bytes) || value.bytes < 0) throw new CaptureError("invalid_response");
    if (value.bytes > limit.decoded) throw new CaptureError("decoded_overflow");
    if (typeof value.body_b64 !== "string" || value.body_b64.length > Math.ceil(limit.decoded / 3) * 4 ||
      !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$(?![\s\S])/.test(value.body_b64)) throw new CaptureError("invalid_encoding");
    const bytes = Buffer.from(value.body_b64, "base64");
    if (bytes.length !== value.bytes || bytes.toString("base64") !== value.body_b64) throw new CaptureError("invalid_encoding");
    const sha256 = hash(bytes), equal = bytes.length === expected.expectedBytes && sha256 === expected.expectedSha256;
    return { status: 200, outcome: equal ? "matched" : bytes.length === 0 ? "empty" : "mismatch", bytes: bytes.length, sha256, equal };
  }

  #release(scope: ReadScope): void {
    if (!scope.readSettled || scope.accountOwners !== 0) return;
    scope.permit?.release(); scope.permit = null; scope.session.active.delete(scope);
  }

  #record(session: SessionCapture, checkpoint: 0 | 5 | 20, expected: ArtifactExpectation, startedAt: number, result: ReadResult): void {
    if (this.#rows.length >= 24) { this.#errors++; return; }
    let phase = "other";
    try { const value = session.phase(); if (PHASES.has(value)) phase = value; } catch {}
    const row = Object.freeze({ worker: session.worker, checkpoint, artifact: expected.label, ...result,
      expectedBytes: expected.expectedBytes, expectedSha256: expected.expectedSha256,
      atMs: Math.max(0, startedAt - session.acceptedAt), durationMs: Math.max(0, this.#now() - startedAt), lifecyclePhase: phase });
    if (!validObservation(result.outcome)) this.#errors++;
    this.#rows.push(row);
    try { this.#options.record(row); } catch { this.#recordErrors++; }
  }
}
