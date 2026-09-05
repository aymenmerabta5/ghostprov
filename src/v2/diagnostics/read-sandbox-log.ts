import { SANDBOX_LOG_SEGMENT_BYTES, SANDBOX_LOG_SEGMENT_RECORDS, isSandboxLogFile, parseDiagnosticLine, type SandboxLogFile } from "./sandbox-log-schema";

export const SANDBOX_LOG_RESPONSE_BYTES = 192 * 1024;
const SESSION = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$(?![\s\S])/;

export function sandboxLogRelativePath(sessionId: string, file: SandboxLogFile): string {
  if (!SESSION.test(sessionId) || !isSandboxLogFile(file)) throw new TypeError("Invalid sandbox log location");
  return `.ghostprov-v2/diagnostics/${file}`;
}

/** Accept only the measured file-service contract and the exact requested scope. */
export function decodeSandboxLogResponse(raw: unknown, expected: { sessionId: string; file: SandboxLogFile }): string {
  const relative = sandboxLogRelativePath(expected.sessionId, expected.file);
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) throw new TypeError("Invalid sandbox log response");
  const value = raw as Record<string, unknown>;
  if (value.session_id !== expected.sessionId || value.path !== `/codex_workspace/${expected.sessionId}/${relative}` ||
    (value.relative_path !== undefined && value.relative_path !== `${expected.sessionId}/${relative}`) ||
    typeof value.bytes !== "number" || !Number.isSafeInteger(value.bytes) || value.bytes < 0 || value.bytes > SANDBOX_LOG_SEGMENT_BYTES ||
    typeof value.body_b64 !== "string" || value.body_b64.length > Math.ceil(SANDBOX_LOG_SEGMENT_BYTES / 3) * 4 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$(?![\s\S])/.test(value.body_b64)) {
    throw new TypeError("Sandbox log response does not match its requested file");
  }
  const bytes = Buffer.from(value.body_b64, "base64");
  if (bytes.length !== value.bytes || bytes.toString("base64") !== value.body_b64) throw new TypeError("Invalid sandbox log encoding");
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  const complete = text.endsWith("\n") ? text : text.slice(0, text.lastIndexOf("\n") + 1);
  const lines = complete.split("\n");
  if (lines.length > SANDBOX_LOG_SEGMENT_RECORDS + 1) throw new TypeError("Sandbox log record limit exceeded");
  for (let index = 0; index < lines.length - 1; index++) parseDiagnosticLine(lines[index]!);
  // A concurrently appended last line may be incomplete, but cannot be arbitrary
  // extra data or exceed the writer's per-record bound.
  if (text.length - complete.length > 2048) throw new TypeError("Sandbox log has an oversized partial record");
  return complete;
}
