import {
  GP_V2_FRAME_MAX_BYTES,
  GP_V2_PROTOCOL,
  GP_V2_REQUEST_MAX_BYTES,
  GP_V2_SCHEMA_VERSION,
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  base64UrlDecode,
  utf8Bytes,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "./shared";
import { parseDiagnosticObject, parseFirstEventDiagnostics, parseRequestAckDiagnostics, parseTerminalDiagnostics } from "../diagnostics/types";

export type FrameDirection = "host" | "sidecar";

export class V2ProtocolError extends Error {
  readonly name = "V2ProtocolError";

  constructor(message: string, readonly code: string) {
    super(message);
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SAFE_CODE = /^[A-Za-z][A-Za-z0-9._-]{0,63}$/;
const SAFE_EVENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const SHA256 = /^[a-f0-9]{64}$/;

function fail(message: string, code = "invalid"): never {
  throw new V2ProtocolError(message, code);
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    fail(`${label} must be an object`, "shape");
  }
  return value as Record<string, unknown>;
}

function exactKeys(
  value: Record<string, unknown>,
  expected: readonly string[],
  label: string,
): void {
  const allowed = new Set(expected);
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) fail(`${label} contains unknown field ${key}`, "unknown_field");
  }
  for (const key of expected) {
    if (!(key in value)) fail(`${label} is missing field ${key}`, "missing_field");
  }
}

function integer(
  value: unknown,
  label: string,
  minimum = 0,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    fail(`${label} must be an integer from ${minimum} through ${maximum}`, "integer");
  }
  return value as number;
}

function finiteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    fail(`${label} must be a non-negative finite number`, "number");
  }
  return value;
}

function string(
  value: unknown,
  label: string,
  maximum: number,
): string {
  if (typeof value !== "string" || value.length < 1 || value.length > maximum) {
    fail(`${label} must be a non-empty string of at most ${maximum} characters`, "string");
  }
  return value;
}

function safeId(value: unknown, label: string): string {
  const result = string(value, label, 128);
  if (!SAFE_ID.test(result)) fail(`${label} is invalid`, "id");
  return result;
}

function code(value: unknown, label: string): string {
  const result = string(value, label, 64);
  if (!SAFE_CODE.test(result)) fail(`${label} is invalid`, "code");
  return result;
}

function digest(value: unknown, label: string): string {
  if (typeof value !== "string" || !SHA256.test(value)) {
    fail(`${label} must be a lowercase SHA-256 digest`, "digest");
  }
  return value;
}

function project(value: unknown): string {
  if (typeof value !== "string" || !UUID.test(value)) {
    fail("projectId must be a UUID", "project");
  }
  return value;
}

function base64url(value: unknown, label: string): string {
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]+$/.test(value)) {
    fail(`${label} must be unpadded base64url`, "base64url");
  }
  try {
    base64UrlDecode(value);
  } catch {
    fail(`${label} must be valid base64url`, "base64url");
  }
  return value;
}

function frameIdentity(raw: Record<string, unknown>): { epoch: number; sequence: number } {
  return {
    epoch: integer(raw.epoch, "frame epoch"),
    sequence: integer(raw.sequence, "frame sequence"),
  };
}

function frameBytes(value: unknown): number {
  let encoded: Uint8Array;
  try {
    encoded = utf8Bytes(JSON.stringify(value));
  } catch {
    fail("frame is not JSON serializable", "frame_size");
  }
  if (encoded.byteLength > GP_V2_FRAME_MAX_BYTES) {
    fail(`frame exceeds ${GP_V2_FRAME_MAX_BYTES}-byte limit`, "frame_size");
  }
  return encoded.byteLength;
}

function parseHostFrame(raw: Record<string, unknown>): ReliableFrame<HostFrame> {
  const kind = raw.kind;
  const identity = frameIdentity(raw);
  switch (kind) {
    case "request_open": {
      exactKeys(raw, [
        "kind", "requestId", "requestDigest", "decodedBytes", "encodedBytes",
        "encoding", "partCount", "epoch", "sequence",
      ], "request_open frame");
      const encoding = raw.encoding;
      if (encoding !== "identity" && encoding !== "gzip+base64") {
        fail("request encoding is invalid", "encoding");
      }
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        requestDigest: digest(raw.requestDigest, "requestDigest"),
        decodedBytes: integer(raw.decodedBytes, "decodedBytes", 1, GP_V2_REQUEST_MAX_BYTES),
        encodedBytes: integer(raw.encodedBytes, "encodedBytes", 1, GP_V2_REQUEST_MAX_BYTES),
        encoding,
        partCount: integer(raw.partCount, "partCount", 1, 65_536),
        ...identity,
      };
    }
    case "request_data":
      exactKeys(raw, [
        "kind", "requestId", "partIndex", "offset", "dataBase64Url", "epoch", "sequence",
      ], "request_data frame");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        partIndex: integer(raw.partIndex, "partIndex"),
        offset: integer(raw.offset, "offset"),
        dataBase64Url: base64url(raw.dataBase64Url, "dataBase64Url"),
        ...identity,
      };
    case "request_commit":
      exactKeys(raw, ["kind", "requestId", "epoch", "sequence"], "request_commit frame");
      return { kind, requestId: safeId(raw.requestId, "requestId"), ...identity };
    case "request_cancel":
      exactKeys(raw, ["kind", "requestId", "reason", "epoch", "sequence"], "request_cancel frame");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        reason: string(raw.reason, "cancel reason", 256),
        ...identity,
      };
    case "response_ack": {
      const keys = raw.terminalDigest === undefined
        ? ["kind", "requestId", "epoch", "sequence"]
        : ["kind", "requestId", "terminalDigest", "epoch", "sequence"];
      exactKeys(raw, keys, "response_ack frame");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        ...(raw.terminalDigest === undefined
          ? {}
          : { terminalDigest: digest(raw.terminalDigest, "terminalDigest") }),
        ...identity,
      };
    }
    case "credential_rotate":
      exactKeys(raw, [
        "kind", "rotationId", "sealedJson", "epoch", "sequence",
      ], "credential_rotate frame");
      return {
        kind,
        rotationId: safeId(raw.rotationId, "rotationId"),
        sealedJson: string(raw.sealedJson, "sealedJson", 12_000),
        ...identity,
      };
    case "drain":
      exactKeys(raw, ["kind", "reason", "epoch", "sequence"], "drain frame");
      return { kind, reason: string(raw.reason, "drain reason", 256), ...identity };
    case "ping":
      exactKeys(raw, ["kind", "sentAtMs", "epoch", "sequence"], "ping frame");
      return { kind, sentAtMs: finiteNumber(raw.sentAtMs, "sentAtMs"), ...identity };
    default:
      fail(`Unknown host frame kind: ${String(kind)}`, "frame_kind");
  }
}

function parseSidecarFrame(raw: Record<string, unknown>): ReliableFrame<SidecarFrame> {
  const kind = raw.kind;
  const identity = frameIdentity(raw);
  switch (kind) {
    case "request_ack":
      exactKeys(raw, [
        "kind", "requestId", "requestDigest", "committed", "epoch", "sequence",
        ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
      ], "request_ack frame");
      if (raw.committed !== true) fail("request_ack must prove commitment", "commitment");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        requestDigest: digest(raw.requestDigest, "requestDigest"),
        committed: true,
        ...(parseRequestAckDiagnostics(raw.diagnostics) ? { diagnostics: parseRequestAckDiagnostics(raw.diagnostics)! } : {}),
        ...identity,
      };
    case "request_reject":
      exactKeys(raw, [
        "kind", "requestId", "code", "message", "epoch", "sequence",
      ], "request_reject frame");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        code: code(raw.code, "rejection code"),
        message: string(raw.message, "rejection message", 512),
        ...identity,
      };
    case "response_event": {
      exactKeys(raw, [
        "kind", "requestId", "eventIndex", "event", "partIndex", "partCount",
        "offset", "dataBase64Url", "epoch", "sequence",
        ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
      ], "response_event frame");
      const eventName = string(raw.event, "event", 128);
      if (!SAFE_EVENT.test(eventName)) fail("event is invalid", "event");
      const partCount = integer(raw.partCount, "partCount", 1, 65_536);
      const eventIndex = integer(raw.eventIndex, "eventIndex");
      const partIndex = integer(raw.partIndex, "partIndex", 0, partCount - 1);
      const diagnostics = eventIndex === 0 && partIndex === 0 ? parseFirstEventDiagnostics(raw.diagnostics) : null;
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        eventIndex,
        event: eventName,
        partIndex,
        partCount,
        offset: integer(raw.offset, "offset"),
        dataBase64Url: base64url(raw.dataBase64Url, "dataBase64Url"),
        ...(diagnostics ? { diagnostics } : {}),
        ...identity,
      };
    }
    case "response_terminal": {
      exactKeys(raw, [
        "kind", "requestId", "requestDigest", "responseId", "eventCount", "byteCount",
        "transcriptSha256", "terminalStatus", "finalEventIndex", "finalSha256",
        "epoch", "sequence", ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
      ], "response_terminal frame");
      const terminalStatus = raw.terminalStatus;
      if (
        terminalStatus !== "completed" &&
        terminalStatus !== "failed" &&
        terminalStatus !== "incomplete"
      ) fail("terminalStatus is invalid", "terminal_status");
      const diagnostics = parseTerminalDiagnostics(raw.diagnostics);
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        requestDigest: digest(raw.requestDigest, "requestDigest"),
        responseId: safeId(raw.responseId, "responseId"),
        eventCount: integer(raw.eventCount, "eventCount", 1),
        byteCount: integer(raw.byteCount, "byteCount", 1),
        transcriptSha256: digest(raw.transcriptSha256, "transcriptSha256"),
        terminalStatus,
        finalEventIndex: integer(raw.finalEventIndex, "finalEventIndex"),
        finalSha256: digest(raw.finalSha256, "finalSha256"),
        ...(diagnostics ? { diagnostics } : {}),
        ...identity,
      };
    }
    case "response_error":
      exactKeys(raw, [
        "kind", "requestId", "code", "message", "ambiguous", "epoch", "sequence",
      ], "response_error frame");
      if (typeof raw.ambiguous !== "boolean") fail("ambiguous must be boolean", "boolean");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        code: code(raw.code, "error code"),
        message: string(raw.message, "error message", 512),
        ambiguous: raw.ambiguous,
        ...identity,
      };
    case "credential_ack":
      exactKeys(raw, ["kind", "rotationId", "epoch", "sequence"], "credential_ack frame");
      return { kind, rotationId: safeId(raw.rotationId, "rotationId"), ...identity };
    case "drained":
      exactKeys(raw, ["kind", "active", "queued", "epoch", "sequence"], "drained frame");
      return {
        kind,
        active: integer(raw.active, "active"),
        queued: integer(raw.queued, "queued"),
        ...identity,
      };
    case "pong":
      exactKeys(raw, ["kind", "sentAtMs", "epoch", "sequence"], "pong frame");
      return { kind, sentAtMs: finiteNumber(raw.sentAtMs, "sentAtMs"), ...identity };
    default:
      fail(`Unknown sidecar frame kind: ${String(kind)}`, "frame_kind");
  }
}

export function parseFrame(
  value: unknown,
  direction: "host",
): ReliableFrame<HostFrame>;
export function parseFrame(
  value: unknown,
  direction: "sidecar",
): ReliableFrame<SidecarFrame>;
export function parseFrame(
  value: unknown,
  direction: FrameDirection,
): ReliableFrame<HostFrame> | ReliableFrame<SidecarFrame> {
  frameBytes(value);
  const raw = object(value, "frame");
  return direction === "host" ? parseHostFrame(raw) : parseSidecarFrame(raw);
}

export function parseAwarenessEnvelope(
  value: unknown,
  expected: ProtocolBinding,
  direction: "host",
): AwarenessEnvelope<HostFrame>;
export function parseAwarenessEnvelope(
  value: unknown,
  expected: ProtocolBinding,
  direction: "sidecar",
): AwarenessEnvelope<SidecarFrame>;
export function parseAwarenessEnvelope(
  value: unknown,
  expected: ProtocolBinding,
  direction: FrameDirection,
): AwarenessEnvelope<HostFrame | SidecarFrame> {
  const raw = object(value, "Awareness envelope");
  exactKeys(raw, [
    "protocol", "schemaVersion", "schemaHash", "buildHash", "projectId", "sessionId",
    "peerId", "instanceId", "leaderGeneration", "epoch", "ack", "credits", "active",
    "revision", "queued", "draining", "frames", ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
  ], "Awareness envelope");
  if (raw.protocol !== GP_V2_PROTOCOL) fail("Awareness protocol is invalid", "protocol");
  if (raw.schemaVersion !== GP_V2_SCHEMA_VERSION) fail("Awareness schema version is invalid", "schema");
  if (project(raw.projectId) !== expected.projectId) fail("Awareness project binding mismatch", "binding");
  if (safeId(raw.sessionId, "sessionId") !== expected.sessionId) fail("Awareness session binding mismatch", "binding");
  if (digest(raw.schemaHash, "schemaHash") !== expected.schemaHash) fail("Awareness schema binding mismatch", "binding");
  if (digest(raw.buildHash, "buildHash") !== expected.buildHash) fail("Awareness build binding mismatch", "binding");
  const leaderGeneration = integer(raw.leaderGeneration, "leaderGeneration", 1);
  if (leaderGeneration !== expected.leaderGeneration) fail("Awareness leader binding mismatch", "binding");
  const epoch = integer(raw.epoch, "epoch");
  const revision = integer(raw.revision, "revision");
  const ackRaw = object(raw.ack, "Awareness ACK");
  exactKeys(ackRaw, ["epoch", "sequence"], "Awareness ACK");
  const ack = {
    epoch: integer(ackRaw.epoch, "ACK epoch"),
    sequence: integer(ackRaw.sequence, "ACK sequence", -1),
  };
  if (!Array.isArray(raw.frames)) fail("Awareness frames must be an array", "frames");
  if (raw.frames.length > GP_V2_WINDOW_MAX_FRAMES) {
    fail(`Awareness state exceeds the ${GP_V2_WINDOW_MAX_FRAMES}-frame window`, "window_frames");
  }
  let encodedBytes = 0;
  let previousSequence = -1;
  const frames = raw.frames.map(frame => {
    encodedBytes += frameBytes(frame);
    const frameObject = object(frame, "frame");
    const parsed = direction === "host"
      ? parseHostFrame(frameObject)
      : parseSidecarFrame(frameObject);
    if (parsed.epoch !== epoch) fail("Frame epoch differs from envelope epoch", "epoch");
    if (parsed.sequence <= previousSequence) fail("Frame sequences must be strictly increasing", "sequence");
    previousSequence = parsed.sequence;
    return parsed;
  });
  if (encodedBytes > GP_V2_WINDOW_MAX_BYTES) {
    fail(`Awareness state exceeds the ${GP_V2_WINDOW_MAX_BYTES}-byte window`, "window_bytes");
  }
  if (typeof raw.draining !== "boolean") fail("draining must be boolean", "boolean");
  const diagnostics = parseDiagnosticObject(raw.diagnostics);
  return {
    protocol: GP_V2_PROTOCOL,
    schemaVersion: GP_V2_SCHEMA_VERSION,
    schemaHash: expected.schemaHash,
    buildHash: expected.buildHash,
    projectId: expected.projectId,
    sessionId: expected.sessionId,
    peerId: safeId(raw.peerId, "peerId"),
    instanceId: safeId(raw.instanceId, "instanceId"),
    leaderGeneration,
    epoch,
    revision,
    ack,
    credits: integer(raw.credits, "credits"),
    active: integer(raw.active, "active"),
    queued: integer(raw.queued, "queued"),
    draining: raw.draining,
    frames: Object.freeze(frames),
    ...(diagnostics ? { diagnostics } : {}),
  };
}

export function parseAwarenessEnvelopeWithDiagnostics(
  value: unknown,
  expected: ProtocolBinding,
  direction: FrameDirection,
): { envelope: AwarenessEnvelope<HostFrame | SidecarFrame>; malformed: boolean } {
  const raw = value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
  const malformed = raw?.diagnostics !== undefined && parseDiagnosticObject(raw.diagnostics) === null;
  return { envelope: parseAwarenessEnvelope(value, expected, direction as any) as AwarenessEnvelope<HostFrame | SidecarFrame>, malformed };
}
