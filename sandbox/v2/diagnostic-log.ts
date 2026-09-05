import {
  SANDBOX_LOG_EVENTS, SANDBOX_LOG_RESERVED_BYTES, SANDBOX_LOG_SEGMENT_BYTES,
  SANDBOX_LOG_SEGMENT_RECORDS, sanitizeDiagnosticFields,
} from "../../src/v2/diagnostics/sandbox-log-schema.ts";
import { denoDiagnosticStorage, openDiagnosticSink, type DiagnosticSink } from "./diagnostic-log-storage.ts";

const MAX_REQUESTS = 128;
const encoder = new TextEncoder();
type Role = "sidecar" | "supervisor";

/** Bounded synchronous writes preserve the last failure without an async flush queue. */
export class DiagnosticLog {
  readonly #role: Role;
  readonly #origin = performance.now();
  readonly #pid = typeof Deno === "undefined" ? null : Deno.pid;
  readonly #requests = new Map<string, number>();
  readonly #segmentBytes = [0, 0];
  readonly #segmentRecords = [0, 0];
  #sink: DiagnosticSink | null = null;
  #nextRequest = 0;
  #records = 0;
  #bytes = 0;
  #segment = 0;
  #rotations = 0;
  #dropped = 0;
  #writeMs = 0;
  #formatMs = 0;
  #disabled = false;

  constructor(role: Role, sink?: DiagnosticSink) {
    this.#role = role;
    if (sink) { this.#sink = sink; return; }
    try {
      if (typeof Deno === "undefined") { this.#disabled = true; return; }
      this.#sink = openDiagnosticSink(denoDiagnosticStorage(), Deno.env.get("GHOSTPROV_WORKSPACE_ROOT") ?? "", role);
      this.record("process_start", { pid: Deno.pid }, true);
    } catch { this.#disabled = true; this.#dropped++; }
  }

  ordinal(id: string): number {
    if (this.#disabled) return 0;
    const existing = this.#requests.get(id);
    if (existing !== undefined) return existing;
    if (this.#requests.size >= MAX_REQUESTS || this.#nextRequest >= 1_000_000_000) { this.#dropped++; return 0; }
    const ordinal = ++this.#nextRequest;
    this.#requests.set(id, ordinal);
    return ordinal;
  }

  forget(id: string): void { this.#requests.delete(id); }

  record(event: string, fields: Record<string, unknown> = {}, critical = false): void {
    if (this.#disabled || !this.#sink) return;
    if (!SANDBOX_LOG_EVENTS.has(event)) { this.#dropped++; return; }
    const formattingAt = performance.now();
    try {
      const line = JSON.stringify({ schema: "ghostprov-sandbox-diagnostic", version: 1, role: this.#role,
        ts: new Date().toISOString(), mono_ms: performance.now() - this.#origin, event,
        ...(this.#pid === null ? {} : { pid: this.#pid }),
        ...sanitizeDiagnosticFields(fields), record_seq: this.#records + 1 }) + "\n";
      if (line.length > 2048) { this.#dropped++; return; }
      const bytes = encoder.encode(line);
      this.#formatMs += performance.now() - formattingAt;
      if (bytes.length > 2048) { this.#dropped++; return; }
      const started = performance.now();
      try {
        if (this.#segmentRecords[this.#segment]! >= SANDBOX_LOG_SEGMENT_RECORDS ||
          this.#segmentBytes[this.#segment]! + bytes.length > SANDBOX_LOG_SEGMENT_BYTES - (critical ? 0 : SANDBOX_LOG_RESERVED_BYTES)) {
          this.#sink.rotate();
          this.#segment = 1 - this.#segment;
          this.#rotations++;
          this.#segmentBytes[this.#segment] = 0;
          this.#segmentRecords[this.#segment] = 0;
        }
        this.#sink.append(bytes);
        this.#bytes += bytes.length;
        this.#records++;
        this.#segmentBytes[this.#segment]! += bytes.length;
        this.#segmentRecords[this.#segment]!++;
      } finally { this.#writeMs += performance.now() - started; }
    } catch {
      this.#disabled = true;
      this.#dropped++;
      try { this.#sink.close(); } catch { /* Diagnostic sinks cannot fail request execution. */ }
    }
  }

  snapshot() {
    return { records: this.#records, bytes: this.#bytes, retained_bytes: this.#segmentBytes[0]! + this.#segmentBytes[1]!,
      rotations: this.#rotations, dropped: this.#dropped, write_ms: this.#writeMs, format_ms: this.#formatMs };
  }

  close(): void {
    this.#disabled = true;
    this.#requests.clear();
    try { this.#sink?.close(); } catch { /* Logging must not fail a lifecycle operation. */ }
    this.#sink = null;
  }
}

const logs = new Map<Role, DiagnosticLog>();
export function diagnosticLog(role: Role): DiagnosticLog {
  let log = logs.get(role);
  if (!log) { log = new DiagnosticLog(role); logs.set(role, log); }
  return log;
}
