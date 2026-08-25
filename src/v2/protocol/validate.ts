import {
  GP_V2_PROTOCOL,
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  protocolTransportLimits,
  utf8Bytes,
  type AwarenessLiveness,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ProtocolRevision,
  type ReliableFrame,
  type R2RequestCommitFrame,
  type R2RequestPreparedFrame,
  type R2RequestStartFrame,
  type SidecarFrame,
} from "./shared";
import { parseDiagnosticObject, parseFirstEventDiagnostics, parseRequestAckDiagnostics, parseTerminalDiagnostics } from "../diagnostics/types";
import { MAX_RATE_LIMIT_DELAY_MS, type RateLimitHint } from "../rate-limit";
import { parseProviderErrorEnvelope } from "../provider-error";

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

function parseRateLimitHint(value: unknown): RateLimitHint | null {
  if (value === undefined) return null;
  const raw = object(value, "rateLimit");
  exactKeys(raw, [
    ...(raw.retryAfterMs === undefined ? [] : ["retryAfterMs"]),
    ...(raw.resetTokensMs === undefined ? [] : ["resetTokensMs"]),
    ...(raw.limitTokens === undefined ? [] : ["limitTokens"]),
    ...(raw.remainingTokens === undefined ? [] : ["remainingTokens"]),
    ...(raw.limitProjectTokens === undefined ? [] : ["limitProjectTokens"]),
    ...(raw.remainingProjectTokens === undefined ? [] : ["remainingProjectTokens"]),
    ...(raw.resetProjectTokensMs === undefined ? [] : ["resetProjectTokensMs"]),
  ], "rateLimit");
  const retryAfterMs = raw.retryAfterMs === undefined
    ? undefined
    : integer(raw.retryAfterMs, "rateLimit.retryAfterMs", 0, MAX_RATE_LIMIT_DELAY_MS);
  const resetTokensMs = raw.resetTokensMs === undefined
    ? undefined
    : integer(raw.resetTokensMs, "rateLimit.resetTokensMs", 0, MAX_RATE_LIMIT_DELAY_MS);
  const limitTokens = raw.limitTokens === undefined
    ? undefined
    : integer(raw.limitTokens, "rateLimit.limitTokens");
  const remainingTokens = raw.remainingTokens === undefined
    ? undefined
    : integer(raw.remainingTokens, "rateLimit.remainingTokens");
  const limitProjectTokens = raw.limitProjectTokens === undefined
    ? undefined
    : integer(raw.limitProjectTokens, "rateLimit.limitProjectTokens");
  const remainingProjectTokens = raw.remainingProjectTokens === undefined
    ? undefined
    : integer(raw.remainingProjectTokens, "rateLimit.remainingProjectTokens");
  const resetProjectTokensMs = raw.resetProjectTokensMs === undefined
    ? undefined
    : integer(raw.resetProjectTokensMs, "rateLimit.resetProjectTokensMs", 0, MAX_RATE_LIMIT_DELAY_MS);
  if (
    retryAfterMs === undefined &&
    resetTokensMs === undefined &&
    limitTokens === undefined &&
    remainingTokens === undefined &&
    limitProjectTokens === undefined &&
    remainingProjectTokens === undefined &&
    resetProjectTokensMs === undefined
  ) {
    fail("rateLimit must contain at least one known header", "rate_limit");
  }
  return {
    ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
    ...(resetTokensMs === undefined ? {} : { resetTokensMs }),
    ...(limitTokens === undefined ? {} : { limitTokens }),
    ...(remainingTokens === undefined ? {} : { remainingTokens }),
    ...(limitProjectTokens === undefined ? {} : { limitProjectTokens }),
    ...(remainingProjectTokens === undefined ? {} : { remainingProjectTokens }),
    ...(resetProjectTokensMs === undefined ? {} : { resetProjectTokensMs }),
  };
}

function parseLiveness(
  value: unknown,
  direction: FrameDirection,
): AwarenessLiveness {
  const raw = object(value, "Awareness liveness");
  exactKeys(raw, ["kind", "sentAtMs"], "Awareness liveness");
  const expectedKind = direction === "host" ? "ping" : "pong";
  if (raw.kind !== expectedKind) {
    fail(
      `${direction === "host" ? "Host" : "Sidecar"} Awareness liveness must be ${expectedKind}`,
      "liveness_direction",
    );
  }
  return {
    kind: expectedKind,
    sentAtMs: finiteNumber(raw.sentAtMs, "liveness sentAtMs"),
  } as AwarenessLiveness;
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
  if (typeof value !== "string" || !/^[A-Za-z0-9_-]*$/.test(value)) {
    fail(`${label} must be unpadded base64url`, "base64url");
  }
  const remainder = value.length % 4;
  if (remainder === 1) {
    fail(`${label} must be valid base64url`, "base64url");
  }
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  if (remainder === 2 && (alphabet.indexOf(value.at(-1)!) & 0x0f) !== 0) {
    fail(`${label} must use canonical base64url tail bits`, "base64url");
  }
  if (remainder === 3 && (alphabet.indexOf(value.at(-1)!) & 0x03) !== 0) {
    fail(`${label} must use canonical base64url tail bits`, "base64url");
  }
  return value;
}

function frameIdentity(raw: Record<string, unknown>): { epoch: number; sequence: number } {
  return {
    epoch: integer(raw.epoch, "frame epoch"),
    sequence: integer(raw.sequence, "frame sequence"),
  };
}

function frameBytes(value: unknown, protocolRevision: ProtocolRevision): number {
  const limits = protocolTransportLimits(protocolRevision);
  let encoded: Uint8Array;
  try {
    encoded = utf8Bytes(JSON.stringify(value));
  } catch {
    fail("frame is not JSON serializable", "frame_size");
  }
  if (encoded.byteLength > limits.frameMaxBytes) {
    fail(`frame exceeds ${limits.frameMaxBytes}-byte limit`, "frame_size");
  }
  return encoded.byteLength;
}

type R2FencedFrame =
  | R2RequestCommitFrame
  | R2RequestStartFrame
  | R2RequestPreparedFrame;

function parseR2FencedFrame(
  raw: Record<string, unknown>,
  kind: R2FencedFrame["kind"],
  requestMaxBytes: number,
): ReliableFrame<R2FencedFrame> {
  exactKeys(raw, [
    "kind", "requestId", "protocolRevision", "schemaVersion", "projectId",
    "sessionId", "leaderGeneration", "buildHash", "schemaHash", "launchNonce",
    "hostInstanceId", "targetInstanceId", "requestDigest", "decodedBytes",
    "encodedBytes", "encoding", "partCount", "epoch", "sequence",
  ], `${kind} frame`);
  if (raw.protocolRevision !== "r2" || raw.schemaVersion !== GP_V2_R2_SCHEMA_VERSION) {
    fail(`${kind} protocol revision is invalid`, "revision");
  }
  const encoding = raw.encoding;
  if (encoding !== "identity" && encoding !== "gzip+base64") {
    fail("request encoding is invalid", "encoding");
  }
  return {
    kind,
    requestId: safeId(raw.requestId, "requestId"),
    protocolRevision: "r2",
    schemaVersion: GP_V2_R2_SCHEMA_VERSION,
    projectId: project(raw.projectId),
    sessionId: safeId(raw.sessionId, "sessionId"),
    leaderGeneration: integer(raw.leaderGeneration, "leaderGeneration", 1),
    buildHash: digest(raw.buildHash, "buildHash"),
    schemaHash: digest(raw.schemaHash, "schemaHash"),
    launchNonce: safeId(raw.launchNonce, "launchNonce"),
    hostInstanceId: safeId(raw.hostInstanceId, "hostInstanceId"),
    targetInstanceId: safeId(raw.targetInstanceId, "targetInstanceId"),
    requestDigest: digest(raw.requestDigest, "requestDigest"),
    decodedBytes: integer(raw.decodedBytes, "decodedBytes", 1, requestMaxBytes),
    encodedBytes: integer(raw.encodedBytes, "encodedBytes", 1, requestMaxBytes),
    encoding,
    partCount: integer(raw.partCount, "partCount", 1, 65_536),
    ...frameIdentity(raw),
  };
}

function parseHostFrame(
  raw: Record<string, unknown>,
  protocolRevision: ProtocolRevision,
): ReliableFrame<HostFrame> {
  const limits = protocolTransportLimits(protocolRevision);
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
        decodedBytes: integer(raw.decodedBytes, "decodedBytes", 1, limits.requestMaxBytes),
        encodedBytes: integer(raw.encodedBytes, "encodedBytes", 1, limits.requestMaxBytes),
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
    case "request_commit": {
      if (protocolRevision === "r1") {
        exactKeys(raw, ["kind", "requestId", "epoch", "sequence"], "request_commit frame");
        return { kind, requestId: safeId(raw.requestId, "requestId"), ...identity };
      }
      return parseR2FencedFrame(raw, kind, limits.requestMaxBytes) as
        ReliableFrame<Extract<HostFrame, { kind: "request_commit" }>>;
    }
    case "request_start":
      if (protocolRevision !== "r2") fail("request_start is not part of R1", "revision");
      return parseR2FencedFrame(raw, kind, limits.requestMaxBytes) as
        ReliableFrame<Extract<HostFrame, { kind: "request_start" }>>;
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

function parseSidecarFrame(
  raw: Record<string, unknown>,
  protocolRevision: ProtocolRevision,
): ReliableFrame<SidecarFrame> {
  const limits = protocolTransportLimits(protocolRevision);
  const kind = raw.kind;
  const identity = frameIdentity(raw);
  switch (kind) {
    case "request_prepared":
      if (protocolRevision !== "r2") fail("request_prepared is not part of R1", "revision");
      return parseR2FencedFrame(raw, kind, limits.requestMaxBytes) as
        ReliableFrame<Extract<SidecarFrame, { kind: "request_prepared" }>>;
    case "upstream_prelude":
      if (protocolRevision !== "r2") {
        fail("upstream_prelude is not part of R1", "revision");
      }
      exactKeys(raw, [
        "kind", "requestId", "status", "contentType", "upstreamRequestId", "epoch", "sequence",
      ], "upstream_prelude frame");
      if (
        raw.upstreamRequestId !== null &&
        (typeof raw.upstreamRequestId !== "string" || raw.upstreamRequestId.length > 256)
      ) fail("upstreamRequestId is invalid", "string");
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        status: integer(raw.status, "status", 200, 299),
        contentType: string(raw.contentType, "contentType", 256),
        upstreamRequestId: raw.upstreamRequestId as string | null,
        ...identity,
      };
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
    case "response_batch": {
      exactKeys(raw,["kind","requestId","firstEventIndex","items","epoch","sequence"],"response_batch frame");
      if(!Array.isArray(raw.items)||raw.items.length<1||raw.items.length>32) fail("response_batch must contain 1 through 32 items","batch_items");
      const items=raw.items.map((value,index)=>{const item=object(value,`batch item ${index}`);exactKeys(item,["event","dataBase64Url"],"response_batch item");const event=string(item.event,"event",128);if(!SAFE_EVENT.test(event))fail("event is invalid","event");return {event,dataBase64Url:base64url(item.dataBase64Url,"dataBase64Url")};});
      return {kind,requestId:safeId(raw.requestId,"requestId"),firstEventIndex:integer(raw.firstEventIndex,"firstEventIndex"),items,...identity};
    }
    case "response_terminal": {
      exactKeys(raw, [
        "kind", "requestId", "requestDigest", "responseId", "eventCount", "byteCount",
        "transcriptSha256", "terminalStatus", "finalEventIndex", "finalSha256",
        "epoch", "sequence", ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
        ...(raw.rateLimit === undefined ? [] : ["rateLimit"]),
      ], "response_terminal frame");
      const terminalStatus = raw.terminalStatus;
      if (
        terminalStatus !== "completed" &&
        terminalStatus !== "failed" &&
        terminalStatus !== "incomplete"
      ) fail("terminalStatus is invalid", "terminal_status");
      const diagnostics = parseTerminalDiagnostics(raw.diagnostics);
      const rateLimit = parseRateLimitHint(raw.rateLimit);
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
        ...(rateLimit ? { rateLimit } : {}),
        ...identity,
      };
    }
    case "response_error":
      exactKeys(raw, [
        "kind", "requestId", "code", "message", "ambiguous", "epoch", "sequence",
        ...(raw.rateLimit === undefined ? [] : ["rateLimit"]),
        ...(protocolRevision === "r2" && raw.providerError !== undefined ? ["providerError"] : []),
      ], "response_error frame");
      if (typeof raw.ambiguous !== "boolean") fail("ambiguous must be boolean", "boolean");
      const rateLimit = parseRateLimitHint(raw.rateLimit);
      const providerError = raw.providerError === undefined
        ? null
        : protocolRevision === "r2"
          ? parseProviderErrorEnvelope(raw.providerError)
          : null;
      if (raw.providerError !== undefined && !providerError) {
        fail("providerError is invalid", "provider_error");
      }
      return {
        kind,
        requestId: safeId(raw.requestId, "requestId"),
        code: code(raw.code, "error code"),
        message: string(raw.message, "error message", 512),
        ambiguous: raw.ambiguous,
        ...(rateLimit ? { rateLimit } : {}),
        ...(providerError ? { providerError } : {}),
        ...identity,
      };
    case "request_cancelled":
      exactKeys(raw, ["kind", "requestId", "epoch", "sequence"], "request_cancelled frame");
      return { kind, requestId: safeId(raw.requestId, "requestId"), ...identity };
    case "credential_ack":
      exactKeys(raw, ["kind", "rotationId", "epoch", "sequence"], "credential_ack frame");
      return { kind, rotationId: safeId(raw.rotationId, "rotationId"), ...identity };
    case "recovery_complete":
      if (protocolRevision !== "r2") {
        fail("recovery_complete is R2-only", "frame_kind");
      }
      exactKeys(raw, ["kind", "epoch", "sequence"], "recovery_complete frame");
      return { kind, ...identity };
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
  protocolRevision: ProtocolRevision,
): ReliableFrame<HostFrame>;
export function parseFrame(
  value: unknown,
  direction: "sidecar",
  protocolRevision: ProtocolRevision,
): ReliableFrame<SidecarFrame>;
export function parseFrame(
  value: unknown,
  direction: FrameDirection,
  protocolRevision: ProtocolRevision,
): ReliableFrame<HostFrame> | ReliableFrame<SidecarFrame> {
  frameBytes(value, protocolRevision);
  const raw = object(value, "frame");
  return direction === "host"
    ? parseHostFrame(raw, protocolRevision)
    : parseSidecarFrame(raw, protocolRevision);
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
  const limits = protocolTransportLimits(expected.protocolRevision);
  const flightMaxFrames = direction === "host"
    ? limits.hostFlightMaxFrames
    : limits.sidecarFlightMaxFrames;
  const flightMaxBytes = direction === "host"
    ? limits.hostFlightMaxBytes
    : limits.sidecarFlightMaxBytes;
  const raw = object(value, "Awareness envelope");
  const r2 = expected.protocolRevision === "r2";
  exactKeys(raw, [
    "protocol",
    ...(r2 ? ["protocolRevision"] : []),
    "schemaVersion", "schemaHash", "buildHash", "projectId", "sessionId",
    "peerId", "instanceId",
    ...(r2 ? ["targetInstanceId", "launchNonce"] : []),
    "leaderGeneration", "epoch", "ack", "credits", "active",
    "revision", "queued", "draining", "frames",
    ...(raw.lane === undefined ? [] : ["lane"]),
    ...(raw.liveness === undefined ? [] : ["liveness"]),
    ...(raw.diagnostics === undefined ? [] : ["diagnostics"]),
  ], "Awareness envelope");
  if (raw.protocol !== GP_V2_PROTOCOL) fail("Awareness protocol is invalid", "protocol");
  if (r2) {
    if (
      raw.protocolRevision !== "r2" ||
      raw.schemaVersion !== GP_V2_R2_SCHEMA_VERSION
    ) fail("Awareness protocol revision is invalid", "revision");
    if (safeId(raw.launchNonce, "launchNonce") !== expected.launchNonce) {
      fail("Awareness launch binding mismatch", "binding");
    }
  } else if (raw.schemaVersion !== GP_V2_R1_SCHEMA_VERSION) {
    fail("Awareness schema version is invalid", "schema");
  }
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
  if (raw.frames.length > limits.windowMaxFrames) {
    fail(`Awareness state exceeds the ${limits.windowMaxFrames}-frame window`, "window_frames");
  }
  if (raw.frames.length > flightMaxFrames) {
    fail(`Awareness state exceeds the ${flightMaxFrames}-frame flight`, "flight_frames");
  }
  let encodedBytes = 0;
  let previousSequence = -1;
  const frames = raw.frames.map(frame => {
    encodedBytes += frameBytes(frame, expected.protocolRevision);
    const frameObject = object(frame, "frame");
    const parsed = direction === "host"
      ? parseHostFrame(frameObject, expected.protocolRevision)
      : parseSidecarFrame(frameObject, expected.protocolRevision);
    if (parsed.epoch !== epoch) fail("Frame epoch differs from envelope epoch", "epoch");
    if (parsed.sequence <= previousSequence) fail("Frame sequences must be strictly increasing", "sequence");
    previousSequence = parsed.sequence;
    return parsed;
  });
  if (encodedBytes > limits.windowMaxBytes) {
    fail(`Awareness state exceeds the ${limits.windowMaxBytes}-byte window`, "window_bytes");
  }
  if (encodedBytes > flightMaxBytes) {
    fail(`Awareness state exceeds the ${flightMaxBytes}-byte flight`, "flight_bytes");
  }
  if (typeof raw.draining !== "boolean") fail("draining must be boolean", "boolean");
  const lane = raw.lane === undefined
    ? undefined
    : raw.lane === "data" || raw.lane === "control"
      ? raw.lane
      : fail("Awareness lane is invalid", "lane");
  let liveness: AwarenessLiveness | undefined;
  if (raw.liveness !== undefined) {
    if (lane !== "control") {
      fail("Awareness liveness is legal only on the control lane", "liveness_lane");
    }
    if (frames.length !== 0) {
      fail("Awareness liveness cannot carry reliable frames", "liveness_frames");
    }
    liveness = parseLiveness(raw.liveness, direction);
  }
  const diagnostics = parseDiagnosticObject(raw.diagnostics);
  const peerId = safeId(raw.peerId, "peerId");
  const instanceId = safeId(raw.instanceId, "instanceId");
  if (r2) {
    const targetInstanceId = safeId(raw.targetInstanceId, "targetInstanceId");
    if (direction === "host") {
      for (const frame of frames) {
        if (frame.kind !== "request_commit" && frame.kind !== "request_start") continue;
        const fenced = frame as ReliableFrame<R2RequestCommitFrame | R2RequestStartFrame>;
        if (
          fenced.protocolRevision !== "r2" ||
          fenced.schemaVersion !== GP_V2_R2_SCHEMA_VERSION ||
          fenced.projectId !== expected.projectId ||
          fenced.sessionId !== expected.sessionId ||
          fenced.leaderGeneration !== expected.leaderGeneration ||
          fenced.buildHash !== expected.buildHash ||
          fenced.schemaHash !== expected.schemaHash ||
          fenced.launchNonce !== expected.launchNonce ||
          fenced.hostInstanceId !== instanceId ||
          fenced.targetInstanceId !== targetInstanceId
        ) fail(`${frame.kind} fence does not match its envelope`, "commitment");
      }
    } else {
      for (const frame of frames) {
        if (frame.kind !== "request_prepared") continue;
        const prepared = frame as ReliableFrame<R2RequestPreparedFrame>;
        if (
          prepared.protocolRevision !== "r2" ||
          prepared.schemaVersion !== GP_V2_R2_SCHEMA_VERSION ||
          prepared.projectId !== expected.projectId ||
          prepared.sessionId !== expected.sessionId ||
          prepared.leaderGeneration !== expected.leaderGeneration ||
          prepared.buildHash !== expected.buildHash ||
          prepared.schemaHash !== expected.schemaHash ||
          prepared.launchNonce !== expected.launchNonce ||
          prepared.hostInstanceId !== targetInstanceId ||
          prepared.targetInstanceId !== instanceId
        ) fail("request_prepared fence does not match its envelope", "commitment");
      }
    }
    return {
      protocol: GP_V2_PROTOCOL,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      schemaHash: expected.schemaHash,
      buildHash: expected.buildHash,
      projectId: expected.projectId,
      sessionId: expected.sessionId,
      peerId,
      instanceId,
      targetInstanceId,
      launchNonce: expected.launchNonce,
      leaderGeneration,
      epoch,
      revision,
      ack,
      credits: integer(raw.credits, "credits"),
      active: integer(raw.active, "active"),
      queued: integer(raw.queued, "queued"),
      draining: raw.draining,
      frames: Object.freeze(frames),
      ...(lane ? { lane } : {}),
      ...(liveness ? { liveness } : {}),
      ...(diagnostics ? { diagnostics } : {}),
    };
  }
  return {
    protocol: GP_V2_PROTOCOL,
    schemaVersion: GP_V2_R1_SCHEMA_VERSION,
    schemaHash: expected.schemaHash,
    buildHash: expected.buildHash,
    projectId: expected.projectId,
    sessionId: expected.sessionId,
    peerId,
    instanceId,
    leaderGeneration,
    epoch,
    revision,
    ack,
    credits: integer(raw.credits, "credits"),
    active: integer(raw.active, "active"),
    queued: integer(raw.queued, "queued"),
    draining: raw.draining,
    frames: Object.freeze(frames),
    ...(lane ? { lane } : {}),
    ...(liveness ? { liveness } : {}),
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
