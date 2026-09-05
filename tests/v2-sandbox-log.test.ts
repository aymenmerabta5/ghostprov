import { describe, expect, it } from "bun:test";
import { DiagnosticLog } from "../sandbox/v2/diagnostic-log";
import { archiveChildDiagnosticFiles, openDiagnosticSink, type DiagnosticFile, type DiagnosticStorage } from "../sandbox/v2/diagnostic-log-storage";
import { decodeSandboxLogResponse } from "../src/v2/diagnostics/read-sandbox-log";
import { parseDiagnosticLine, SANDBOX_LOG_SEGMENT_BYTES } from "../src/v2/diagnostics/sandbox-log-schema";

const SESSION = "11111111-1111-4111-8111-111111111111";
const WORKSPACE = `/codex_workspace/${SESSION}`;
const ROOT = `${WORKSPACE}/.ghostprov-v2/diagnostics`;

class MemoryStorage implements DiagnosticStorage {
  readonly entries = new Map<string, { kind: "file" | "directory" | "other"; bytes: Uint8Array }>();
  openFiles = 0;
  constructor() { for (const path of [WORKSPACE, `${WORKSPACE}/.ghostprov-v2`]) this.mkdir(path); }
  inspect(path: string) { const value = this.entries.get(path); return value ? { kind: value.kind, size: value.bytes.length } : null; }
  mkdir(path: string) { if (this.entries.has(path)) throw new Error("exists"); this.entries.set(path, { kind: "directory", bytes: new Uint8Array() }); }
  create(path: string): DiagnosticFile {
    if (this.entries.has(path)) throw new Error("exists");
    const file = { kind: "file" as const, bytes: new Uint8Array() };
    this.entries.set(path, file);
    this.openFiles++;
    let position = 0;
    let closed = false;
    return {
      write: bytes => {
        if (closed) throw new Error("closed");
        // Exercise partial writes instead of assuming writeSync consumes all bytes.
        const count = Math.min(31, bytes.length);
        const next = new Uint8Array(Math.max(file.bytes.length, position + count));
        next.set(file.bytes); next.set(bytes.subarray(0, count), position); file.bytes = next; position += count;
        return count;
      },
      truncate: () => { file.bytes = new Uint8Array(); },
      rewind: () => { position = 0; },
      close: () => { if (!closed) this.openFiles--; closed = true; },
    };
  }
  remove(path: string) { if (!this.entries.delete(path)) throw new Error("missing"); }
  rename(source: string, target: string) {
    const value = this.entries.get(source);
    if (!value || this.entries.has(target)) throw new Error("invalid rename");
    this.entries.set(target, value); this.entries.delete(source);
  }
  text(path: string) { return new TextDecoder().decode(this.entries.get(path)?.bytes); }
}

function fileResponse(content: string) {
  const bytes = Buffer.from(content);
  return { session_id: SESSION, path: `${ROOT}/sidecar.jsonl`, relative_path: `${SESSION}/.ghostprov-v2/diagnostics/sidecar.jsonl`,
    bytes: bytes.length, body_b64: bytes.toString("base64") };
}
const LINE = JSON.stringify({ schema: "ghostprov-sandbox-diagnostic", version: 1, role: "sidecar", ts: "2026-09-05T00:00:00.000Z",
  mono_ms: 1, event: "phase", phase: "runtime", record_seq: 1 }) + "\n";

describe("R10 sandbox diagnostic persistence", () => {
  it("keeps six bounded files after many child replacements and preserves the previous child's failure", () => {
    const fs = new MemoryStorage();
    const supervisor = new DiagnosticLog("supervisor", openDiagnosticSink(fs, WORKSPACE, "supervisor"));
    supervisor.record("process_start", { pid: 1 }, true);
    for (let generation = 1; generation <= 32; generation++) {
      archiveChildDiagnosticFiles(fs, WORKSPACE);
      const child = new DiagnosticLog("sidecar", openDiagnosticSink(fs, WORKSPACE, "sidecar"));
      child.record("fatal", { pid: generation, error_code: "hello_timeout", phase: "awareness_connect" }, true);
      child.close();
      const files = [...fs.entries.values()].filter(entry => entry.kind === "file");
      expect(files.length).toBeLessThanOrEqual(6);
      expect(files.every(entry => entry.bytes.length <= SANDBOX_LOG_SEGMENT_BYTES)).toBe(true);
      if (generation > 1) expect(JSON.parse(fs.text(`${ROOT}/sidecar.previous.jsonl`)).pid).toBe(generation - 1);
    }
    expect(fs.openFiles).toBe(2);
    supervisor.close();
    expect(fs.openFiles).toBe(0);
  });

  it("rotates through exact retained handles without truncating a replacement path", () => {
    const fs = new MemoryStorage();
    const sink = openDiagnosticSink(fs, WORKSPACE, "sidecar");
    const sentinel = { kind: "other" as const, bytes: new TextEncoder().encode("untouched") };
    fs.entries.set(`${ROOT}/sidecar.1.jsonl`, sentinel);
    sink.rotate(); sink.append(new TextEncoder().encode(LINE)); sink.close();
    expect(fs.entries.get(`${ROOT}/sidecar.1.jsonl`)).toBe(sentinel);
    expect(new TextDecoder().decode(sentinel.bytes)).toBe("untouched");
  });

  it("rejects symlinks, collisions and corrupt archive bounds before altering either segment", () => {
    for (const bad of ["other", "oversized"] as const) {
      const fs = new MemoryStorage();
      const sink = openDiagnosticSink(fs, WORKSPACE, "sidecar"); sink.append(new TextEncoder().encode(LINE)); sink.close();
      fs.entries.set(`${ROOT}/sidecar.previous.1.jsonl`, { kind: bad === "other" ? "other" : "file", bytes: new Uint8Array(bad === "oversized" ? SANDBOX_LOG_SEGMENT_BYTES + 1 : 0) });
      expect(() => archiveChildDiagnosticFiles(fs, WORKSPACE)).toThrow();
      expect(fs.text(`${ROOT}/sidecar.jsonl`)).toBe(LINE);
      expect(() => openDiagnosticSink(fs, WORKSPACE, "sidecar")).toThrow();
      expect(fs.openFiles).toBe(0);
    }
  });

  it("closes a partially opened pair and never opens through an invalid directory", () => {
    const fs = new MemoryStorage(); fs.mkdir(ROOT);
    fs.entries.set(`${ROOT}/sidecar.1.jsonl`, { kind: "other", bytes: new Uint8Array() });
    expect(() => openDiagnosticSink(fs, WORKSPACE, "sidecar")).toThrow();
    expect(fs.openFiles).toBe(0);
    fs.entries.set(ROOT, { kind: "other", bytes: new Uint8Array() });
    expect(() => openDiagnosticSink(fs, WORKSPACE, "supervisor")).toThrow();
    expect(() => openDiagnosticSink(fs, "/workspace", "supervisor")).toThrow();
  });

  it("needs only the child's narrow .ghostprov-v2 read permission", () => {
    const fs = new MemoryStorage();
    const inspect = fs.inspect.bind(fs);
    fs.inspect = path => { if (path === WORKSPACE) throw new Error("workspace listing denied"); return inspect(path); };
    const sink = openDiagnosticSink(fs, WORKSPACE, "sidecar");
    sink.append(new TextEncoder().encode(LINE)); sink.close();
    expect(fs.text(`${ROOT}/sidecar.jsonl`)).toBe(LINE);
  });

  it("keeps logging after rotation and filters secrets, content, arbitrary categories and non-finite numbers", () => {
    const fs = new MemoryStorage();
    const log = new DiagnosticLog("sidecar", openDiagnosticSink(fs, WORKSPACE, "sidecar"));
    for (let i = 0; i < 1500; i++) log.record("upstream_event", { event_index: i, event_type: "response.output_text.delta", event_chars: 20 });
    log.record("fatal", { phase: "runtime", error_name: "Error", error_code: "arbitrary-secret", prompt: "private", headers: "secret",
      url: "https://private.invalid", response: "private", duration_ms: Infinity, processCpuPercent: null }, true);
    const rows = ["sidecar.jsonl", "sidecar.1.jsonl"].flatMap(file => fs.text(`${ROOT}/${file}`).split("\n").filter(Boolean).map(parseDiagnosticLine));
    const fatal = rows.find(row => row.event === "fatal")!;
    expect(fatal).toMatchObject({ error_code: "other", processCpuPercent: null });
    expect(Object.keys(fatal)).not.toContain("prompt");
    expect(JSON.stringify(rows)).not.toContain("private");
    expect(log.snapshot().rotations).toBeGreaterThan(0);
    expect(log.snapshot().dropped).toBe(0);
    expect(log.snapshot().retained_bytes).toBeLessThanOrEqual(2 * SANDBOX_LOG_SEGMENT_BYTES);
    log.close();
  });

  it("contains write, close and field-access failures and bounds request correlation", () => {
    const log = new DiagnosticLog("sidecar", { append() { throw new Error("disk unavailable"); }, rotate() {}, close() { throw new Error("close unavailable"); } });
    expect(() => log.record("fatal", { phase: "runtime" }, true)).not.toThrow();
    expect(log.snapshot().dropped).toBe(1);
    const fs = new MemoryStorage();
    const tracked = new DiagnosticLog("sidecar", openDiagnosticSink(fs, WORKSPACE, "sidecar"));
    for (let i = 0; i < 128; i++) expect(tracked.ordinal(String(i))).toBe(i + 1);
    expect(tracked.ordinal("extra")).toBe(0);
    tracked.forget("0"); expect(tracked.ordinal("extra")).toBe(129);
    expect(() => tracked.record("phase", { get phase() { throw new Error("getter"); } })).not.toThrow();
    tracked.close(); expect(fs.openFiles).toBe(0);
  });
});

describe("R10 sandbox file response", () => {
  it("decodes only the exact session, absolute path, body field and byte count", () => {
    const expected = { sessionId: SESSION, file: "sidecar.jsonl" as const };
    expect(decodeSandboxLogResponse(fileResponse(LINE), expected)).toBe(LINE);
    expect(decodeSandboxLogResponse(fileResponse(""), expected)).toBe("");
    for (const change of [
      { session_id: "other" }, { path: `${ROOT}/supervisor.jsonl` }, { relative_path: "sidecar.jsonl" },
      { bytes: -1 }, { bytes: 0 }, { body_b64: "bad?" }, { body_b64: undefined, content: LINE },
    ]) expect(() => decodeSandboxLogResponse({ ...fileResponse(LINE), ...change }, expected)).toThrow();
    expect(() => decodeSandboxLogResponse(fileResponse(LINE), { ...expected, sessionId: SESSION + "\n" })).toThrow();
  });

  it("rejects complete payload-bearing records and returns only complete bounded lines during append", () => {
    const expected = { sessionId: SESSION, file: "sidecar.jsonl" as const };
    const bad = JSON.stringify({ ...JSON.parse(LINE), arguments: "private" }) + "\n";
    expect(() => decodeSandboxLogResponse(fileResponse(bad), expected)).toThrow();
    expect(decodeSandboxLogResponse(fileResponse(LINE + '{"schema":'), expected)).toBe(LINE);
    expect(() => decodeSandboxLogResponse(fileResponse(LINE + "x".repeat(2049)), expected)).toThrow();
    expect(() => decodeSandboxLogResponse(fileResponse(LINE.repeat(1025)), expected)).toThrow();
  });
});
