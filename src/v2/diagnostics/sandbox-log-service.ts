import type { ResourceGovernor, ResourceLease } from "../../router/resource-governor";
import { isSandboxLogFile, SANDBOX_LOG_SEGMENT_BYTES, type SandboxLogFile } from "./sandbox-log-schema";

export const SANDBOX_LOG_READ_RESERVATION = 4 * 1024 * 1024;
const MAX_READS = 2;
const PROJECT = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\s\S])/;

export interface SandboxLogSource {
  readDiagnosticLog(file: SandboxLogFile, signal?: AbortSignal): Promise<string | null>;
}

const failure = (status: number, code: string, message: string) => Response.json({ error: { type: "diagnostic_error", code, message } },
  { status, headers: { "cache-control": "no-store" } });

/** Separate, bounded diagnostics; this service never participates in provider execution. */
export class SandboxLogService {
  readonly #sources = new Map<string, SandboxLogSource>();
  readonly #reads = new Set<AbortController>();
  readonly #governor: Pick<ResourceGovernor, "reserveFixedBytes">;
  readonly #maxSources: number;
  readonly #readTimeoutMs: number;
  #closed = false;

  constructor(options: { governor: Pick<ResourceGovernor, "reserveFixedBytes">; maxSources: number; readTimeoutMs?: number }) {
    if (!Number.isSafeInteger(options.maxSources) || options.maxSources < 1 || options.maxSources > 256) throw new RangeError("Sandbox log source bound must be 1..256");
    this.#readTimeoutMs = options.readTimeoutMs ?? 10000;
    if (!Number.isSafeInteger(this.#readTimeoutMs) || this.#readTimeoutMs < 1 || this.#readTimeoutMs > 30000) throw new RangeError("Sandbox log timeout must be 1..30000");
    this.#governor = options.governor;
    this.#maxSources = options.maxSources;
  }

  register(projectId: string, source: SandboxLogSource): () => void {
    if (this.#closed || !PROJECT.test(projectId) || (!this.#sources.has(projectId) && this.#sources.size >= this.#maxSources)) return () => {};
    this.#sources.set(projectId, source);
    return () => { if (this.#sources.get(projectId) === source) this.#sources.delete(projectId); };
  }

  async read(projectId: string, file: string, caller?: AbortSignal): Promise<Response> {
    if (!PROJECT.test(projectId) || !isSandboxLogFile(file)) return failure(404, "sandbox_log_not_found", "Sandbox log was not found");
    const source = this.#sources.get(projectId);
    if (!source || this.#closed) return failure(404, "sandbox_log_not_found", "Sandbox log was not found");
    if (this.#reads.size >= MAX_READS) return failure(503, "sandbox_log_busy", "Sandbox log reader is busy");
    let lease: ResourceLease;
    try { lease = this.#governor.reserveFixedBytes(SANDBOX_LOG_READ_RESERVATION); }
    catch { return failure(503, "sandbox_log_busy", "Sandbox log reader is busy"); }
    const operation = new AbortController();
    this.#reads.add(operation);
    const deadline = setTimeout(() => operation.abort(new DOMException("Sandbox log read timed out", "TimeoutError")), this.#readTimeoutMs);
    deadline.unref?.();
    const signal = caller ? AbortSignal.any([caller, operation.signal]) : operation.signal;
    let fetching = true;
    let released = false;
    let bytes: Uint8Array | null = null;
    let streamController: ReadableStreamDefaultController<Uint8Array> | null = null;
    const release = () => {
      if (released || fetching) return;
      released = true;
      bytes = null;
      clearTimeout(deadline);
      signal.removeEventListener("abort", onAbort);
      this.#reads.delete(operation);
      lease.release();
    };
    const onAbort = () => {
      if (streamController) { try { streamController.error(signal.reason); } catch { /* Already terminal. */ } }
      release();
    };
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      signal.throwIfAborted();
      const content = await source.readDiagnosticLog(file, signal);
      fetching = false;
      signal.throwIfAborted();
      if (this.#sources.get(projectId) !== source || this.#closed) throw new Error("Sandbox log source changed");
      if (content === null) { release(); return failure(404, "sandbox_log_not_found", "Sandbox log was not found"); }
      if (content.length > SANDBOX_LOG_SEGMENT_BYTES) throw new Error("Sandbox log exceeds its bound");
      bytes = new TextEncoder().encode(content);
      if (bytes.length > SANDBOX_LOG_SEGMENT_BYTES) throw new Error("Sandbox log exceeds its byte bound");
      const body = new ReadableStream<Uint8Array>({
        start(controller) { streamController = controller; },
        pull(controller) {
          if (signal.aborted) { onAbort(); return; }
          if (bytes) { const chunk = bytes; bytes = null; controller.enqueue(chunk); }
          else { controller.close(); release(); }
        },
        cancel() { operation.abort(new DOMException("Sandbox log client disconnected", "AbortError")); release(); },
      }, { highWaterMark: 0 });
      return new Response(body, { headers: { "content-type": "application/x-ndjson; charset=utf-8", "cache-control": "no-store",
        "content-disposition": `attachment; filename="${file}"` } });
    } catch {
      fetching = false;
      release();
      return failure(502, "sandbox_log_unavailable", "Sandbox log is temporarily unavailable");
    }
  }

  snapshot() { return { sources: this.#sources.size, activeReads: this.#reads.size }; }

  close(): void {
    this.#closed = true;
    this.#sources.clear();
    for (const operation of this.#reads) operation.abort(new DOMException("Sandbox log service closed", "AbortError"));
  }
}
