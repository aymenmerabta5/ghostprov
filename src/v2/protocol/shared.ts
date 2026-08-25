import type { V2EnvelopeDiagnostics, V2FirstEventDiagnostics, V2RequestAckDiagnostics, V2TerminalDiagnostics } from "../diagnostics/types";
import type { RateLimitHint } from "../rate-limit";
import type { ProviderErrorEnvelope } from "../provider-error";

export const GP_V2_PROTOCOL = "gp-awareness-v2" as const;
/** Frozen wire revision used by the immutable R1 sidecar release. */
export const GP_V2_R1_SCHEMA_VERSION = 1 as const;
/** Fenced wire revision. R2 never accepts an R1 envelope implicitly. */
export const GP_V2_R2_SCHEMA_VERSION = 2 as const;
/** @deprecated Use the explicit revision-specific constant. */
export const GP_V2_SCHEMA_VERSION = GP_V2_R1_SCHEMA_VERSION;

export type ProtocolRevision = "r1" | "r2";

export interface V2ProtocolTransportLimits {
  readonly frameMaxBytes: number;
  readonly windowMaxFrames: number;
  readonly windowMaxBytes: number;
  readonly flightMaxFrames: number;
  readonly flightMaxBytes: number;
  readonly hostFlightMaxFrames: number;
  readonly hostFlightMaxBytes: number;
  readonly sidecarFlightMaxFrames: number;
  readonly sidecarFlightMaxBytes: number;
  readonly requestMaxBytes: number;
}

/** Frozen R1 limits. These values are part of the immutable R1 wire contract. */
export const GP_V2_R1_TRANSPORT_LIMITS: V2ProtocolTransportLimits = Object.freeze({
  frameMaxBytes: 16 * 1024,
  windowMaxFrames: 128,
  windowMaxBytes: 512 * 1024,
  flightMaxFrames: 32,
  flightMaxBytes: 128 * 1024,
  hostFlightMaxFrames: 32,
  hostFlightMaxBytes: 128 * 1024,
  sidecarFlightMaxFrames: 32,
  sidecarFlightMaxBytes: 128 * 1024,
  requestMaxBytes: 64 * 1024 * 1024,
});

/**
 * R2 keeps the proven frame/window bounds but fills one larger immutable
 * ACK-clocked flight. Thirty-two 8 KiB request-data frames remain below the
 * existing 512 KiB encoded window even after base64url/JSON expansion.
 */
export const GP_V2_R2_TRANSPORT_LIMITS: V2ProtocolTransportLimits = Object.freeze({
  frameMaxBytes: 16 * 1024,
  windowMaxFrames: 128,
  windowMaxBytes: 512 * 1024,
  flightMaxFrames: 32,
  flightMaxBytes: 512 * 1024,
  hostFlightMaxFrames: 32,
  hostFlightMaxBytes: 512 * 1024,
  // Response egress stays conservative until per-request output/control
  // isolation makes a larger peer-wide response flight safe.
  sidecarFlightMaxFrames: 32,
  sidecarFlightMaxBytes: 128 * 1024,
  requestMaxBytes: 64 * 1024 * 1024,
});

export function protocolTransportLimits(
  revision: ProtocolRevision,
): V2ProtocolTransportLimits {
  switch (revision) {
    case "r1": return GP_V2_R1_TRANSPORT_LIMITS;
    case "r2": return GP_V2_R2_TRANSPORT_LIMITS;
    default: throw new TypeError(`Unsupported V2 protocol revision: ${String(revision)}`);
  }
}

/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_FRAME_MAX_BYTES = GP_V2_R1_TRANSPORT_LIMITS.frameMaxBytes;
/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_WINDOW_MAX_FRAMES = GP_V2_R1_TRANSPORT_LIMITS.windowMaxFrames;
/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_WINDOW_MAX_BYTES = GP_V2_R1_TRANSPORT_LIMITS.windowMaxBytes;
/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_FLIGHT_MAX_FRAMES = GP_V2_R1_TRANSPORT_LIMITS.flightMaxFrames;
/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_FLIGHT_MAX_BYTES = GP_V2_R1_TRANSPORT_LIMITS.flightMaxBytes;
/** @deprecated Frozen R1 alias retained for source and sidecar compatibility. */
export const GP_V2_REQUEST_MAX_BYTES = GP_V2_R1_TRANSPORT_LIMITS.requestMaxBytes;

export type AwarenessLane = "data" | "control";

export interface AckCursor {
  epoch: number;
  sequence: number;
}

export interface FrameIdentity {
  epoch: number;
  sequence: number;
}

interface ProtocolBindingBase {
  projectId: string;
  sessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
  /** Bootstrap launch nonce retained for the complete sidecar lifetime. */
  launchNonce: string;
}

export interface R1ProtocolBinding extends ProtocolBindingBase {
  protocolRevision: "r1";
}

export interface R2ProtocolBinding extends ProtocolBindingBase {
  protocolRevision: "r2";
}

/** Selected explicitly from immutable sidecar assets; never inferred from wire input. */
export type ProtocolBinding = R1ProtocolBinding | R2ProtocolBinding;

export type RequestEncoding = "identity" | "gzip+base64";
export type TerminalStatus = "completed" | "failed" | "incomplete";

export interface UpstreamPrelude {
  status: number;
  contentType: string;
  upstreamRequestId: string | null;
}

type SharedHostFrame =
  | {
    kind: "request_open";
    requestId: string;
    requestDigest: string;
    decodedBytes: number;
    encodedBytes: number;
    encoding: RequestEncoding;
    partCount: number;
  }
  | {
    kind: "request_data";
    requestId: string;
    partIndex: number;
    offset: number;
    dataBase64Url: string;
  }
  | { kind: "request_cancel"; requestId: string; reason: string }
  | { kind: "response_ack"; requestId: string; terminalDigest?: string }
  | { kind: "credential_rotate"; rotationId: string; sealedJson: string }
  | { kind: "drain"; reason: string }
  | { kind: "ping"; sentAtMs: number };

export interface R1RequestCommitFrame {
  kind: "request_commit";
  requestId: string;
}

/**
 * Commit-last R2 proof. The immutable request manifest is repeated here so a
 * stale/mixed open cannot cross the execution boundary, and the complete
 * launch/host/sidecar fence can be re-proved immediately before claim/fetch.
 */
export interface R2RequestCommitFrame {
  kind: "request_commit";
  requestId: string;
  protocolRevision: "r2";
  schemaVersion: typeof GP_V2_R2_SCHEMA_VERSION;
  projectId: string;
  sessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
  launchNonce: string;
  hostInstanceId: string;
  targetInstanceId: string;
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
}

/**
 * Immutable R2 execution identity repeated at every phase boundary. The
 * repetition is intentional: no stale/mixed phase frame can borrow identity
 * from an earlier Awareness publication.
 */
export type R2ExecutionFence = Omit<R2RequestCommitFrame, "kind">;

/** Host authorization to cross the irreversible upstream execution boundary. */
export interface R2RequestStartFrame extends R2ExecutionFence {
  kind: "request_start";
}

/**
 * Sidecar proof that the complete body is assembled, durably CLAIMED, and
 * covered by a bounded admission lease. It is not proof that fetch started.
 */
export interface R2RequestPreparedFrame extends R2ExecutionFence {
  kind: "request_prepared";
}

export type RequestCommitFrame = R1RequestCommitFrame | R2RequestCommitFrame;
export type HostFrame = SharedHostFrame | RequestCommitFrame | R2RequestStartFrame;

export type SidecarFrame =
  | R2RequestPreparedFrame
  | {
    kind: "upstream_prelude";
    requestId: string;
    status: number;
    contentType: string;
    upstreamRequestId: string | null;
  }
  | {
    kind: "response_batch"; requestId: string; firstEventIndex: number;
    items: readonly { event: string; dataBase64Url: string }[];
  }
  | {
    kind: "request_ack";
    requestId: string;
    requestDigest: string;
    committed: true;
    diagnostics?: V2RequestAckDiagnostics;
  }
  | {
    kind: "request_reject";
    requestId: string;
    code: string;
    message: string;
  }
  | {
    kind: "response_event";
    requestId: string;
    eventIndex: number;
    event: string;
    partIndex: number;
    partCount: number;
    offset: number;
    dataBase64Url: string;
    diagnostics?: V2FirstEventDiagnostics;
  }
  | {
    kind: "response_terminal";
    requestId: string;
    requestDigest: string;
    responseId: string;
    eventCount: number;
    byteCount: number;
    transcriptSha256: string;
    terminalStatus: TerminalStatus;
    finalEventIndex: number;
    finalSha256: string;
    diagnostics?: V2TerminalDiagnostics;
    rateLimit?: RateLimitHint;
  }
  | {
    kind: "response_error";
    requestId: string;
    code: string;
    message: string;
    ambiguous: boolean;
    rateLimit?: RateLimitHint;
    providerError?: ProviderErrorEnvelope;
  }
  | { kind: "request_cancelled"; requestId: string }
  | { kind: "credential_ack"; rotationId: string }
  | { kind: "recovery_complete" }
  | { kind: "drained"; active: number; queued: number }
  | { kind: "pong"; sentAtMs: number };

export type ReliableFrame<T> = T & FrameIdentity;

/**
 * A best-effort control-lane probe that deliberately stays outside the
 * reliable frame window. Direction is enforced by the wire validator because
 * generic envelope helpers commonly construct either side of the channel.
 */
export type AwarenessLiveness =
  | { kind: "ping"; sentAtMs: number }
  | { kind: "pong"; sentAtMs: number };

interface AwarenessEnvelopeBase<T> {
  protocol: typeof GP_V2_PROTOCOL;
  schemaHash: string;
  buildHash: string;
  projectId: string;
  sessionId: string;
  peerId: string;
  instanceId: string;
  leaderGeneration: number;
  epoch: number;
  revision: number;
  ack: AckCursor;
  credits: number;
  active: number;
  queued: number;
  draining: boolean;
  frames: readonly ReliableFrame<T>[];
  lane?: AwarenessLane;
  liveness?: AwarenessLiveness;
  diagnostics?: V2EnvelopeDiagnostics;
}

/** Exact frozen R1 envelope. It intentionally has no R2 fence fields. */
export interface R1AwarenessEnvelope<T> extends AwarenessEnvelopeBase<T> {
  schemaVersion: typeof GP_V2_R1_SCHEMA_VERSION;
}

/** Exact fenced R2 envelope. `instanceId` is sender; target is unambiguous. */
export interface R2AwarenessEnvelope<T> extends AwarenessEnvelopeBase<T> {
  schemaVersion: typeof GP_V2_R2_SCHEMA_VERSION;
  protocolRevision: "r2";
  launchNonce: string;
  targetInstanceId: string;
}

export type AwarenessEnvelope<T> = R1AwarenessEnvelope<T> | R2AwarenessEnvelope<T>;

export const GP_V2_R1_SCHEMA_DESCRIPTOR = Object.freeze({
  protocol: GP_V2_PROTOCOL,
  version: GP_V2_SCHEMA_VERSION,
  frameMaxBytes: GP_V2_FRAME_MAX_BYTES,
  windowMaxFrames: GP_V2_WINDOW_MAX_FRAMES,
  windowMaxBytes: GP_V2_WINDOW_MAX_BYTES,
  flightMaxFrames: GP_V2_FLIGHT_MAX_FRAMES,
  flightMaxBytes: GP_V2_FLIGHT_MAX_BYTES,
  requestMaxBytes: GP_V2_REQUEST_MAX_BYTES,
  envelopeFields: [
    "protocol",
    "schemaVersion",
    "schemaHash",
    "buildHash",
    "projectId",
    "sessionId",
    "peerId",
    "instanceId",
    "leaderGeneration",
    "epoch",
    "revision",
    "ack",
    "credits",
    "active",
    "queued",
    "draining",
    "frames",
    "lane?",
    "liveness?",
    "diagnostics?",
  ],
  livenessKinds: ["ping", "pong"],
  livenessFields: {
    ping: ["sentAtMs"],
    pong: ["sentAtMs"],
  },
  hostFrameKinds: [
    "request_open",
    "request_data",
    "request_commit",
    "request_cancel",
    "response_ack",
    "credential_rotate",
    "drain",
    "ping",
  ],
  sidecarFrameKinds: [
    "request_ack",
    "request_reject",
    "response_event",
    "response_batch",
    "response_terminal",
    "response_error",
    "request_cancelled",
    "credential_ack",
    "drained",
    "pong",
  ],
  frameFields: {
    request_open: [
      "requestId", "requestDigest", "decodedBytes", "encodedBytes", "encoding", "partCount",
    ],
    request_data: ["requestId", "partIndex", "offset", "dataBase64Url"],
    request_commit: ["requestId"],
    request_cancel: ["requestId", "reason"],
    response_ack: ["requestId", "terminalDigest?"],
    credential_rotate: ["rotationId", "sealedJson"],
    drain: ["reason"],
    ping: ["sentAtMs"],
    request_ack: ["requestId", "requestDigest", "committed", "diagnostics?"],
    request_reject: ["requestId", "code", "message"],
    response_event: [
      "requestId", "eventIndex", "event", "partIndex", "partCount", "offset", "dataBase64Url", "diagnostics?",
    ],
    response_batch: ["requestId", "firstEventIndex", "items"],
    response_terminal: [
      "requestId", "requestDigest", "responseId", "eventCount", "byteCount",
      "transcriptSha256", "terminalStatus", "finalEventIndex", "finalSha256", "diagnostics?", "rateLimit?",
    ],
    response_error: ["requestId", "code", "message", "ambiguous", "rateLimit?"],
    request_cancelled: ["requestId"],
    credential_ack: ["rotationId"],
    drained: ["active", "queued"],
    pong: ["sentAtMs"],
    reliable: ["epoch", "sequence"],
  },
});

export const GP_V2_SCHEMA_DESCRIPTOR = GP_V2_R1_SCHEMA_DESCRIPTOR;

export const GP_V2_R2_SCHEMA_DESCRIPTOR = Object.freeze({
  ...GP_V2_R1_SCHEMA_DESCRIPTOR,
  version: GP_V2_R2_SCHEMA_VERSION,
  frameMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.frameMaxBytes,
  windowMaxFrames: GP_V2_R2_TRANSPORT_LIMITS.windowMaxFrames,
  windowMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.windowMaxBytes,
  flightMaxFrames: GP_V2_R2_TRANSPORT_LIMITS.flightMaxFrames,
  flightMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.flightMaxBytes,
  hostFlightMaxFrames: GP_V2_R2_TRANSPORT_LIMITS.hostFlightMaxFrames,
  hostFlightMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.hostFlightMaxBytes,
  sidecarFlightMaxFrames: GP_V2_R2_TRANSPORT_LIMITS.sidecarFlightMaxFrames,
  sidecarFlightMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.sidecarFlightMaxBytes,
  requestMaxBytes: GP_V2_R2_TRANSPORT_LIMITS.requestMaxBytes,
  envelopeFields: [
    "protocol",
    "protocolRevision",
    "schemaVersion",
    "schemaHash",
    "buildHash",
    "projectId",
    "sessionId",
    "peerId",
    "instanceId",
    "targetInstanceId",
    "launchNonce",
    "leaderGeneration",
    "epoch",
    "revision",
    "ack",
    "credits",
    "active",
    "queued",
    "draining",
    "frames",
    "lane?",
    "liveness?",
    "diagnostics?",
  ],
  frameFields: {
    ...GP_V2_R1_SCHEMA_DESCRIPTOR.frameFields,
    recovery_complete: [],
    upstream_prelude: ["requestId", "status", "contentType", "upstreamRequestId"],
    request_commit: [
      "requestId",
      "protocolRevision",
      "schemaVersion",
      "projectId",
      "sessionId",
      "leaderGeneration",
      "buildHash",
      "schemaHash",
      "launchNonce",
      "hostInstanceId",
      "targetInstanceId",
      "requestDigest",
      "decodedBytes",
      "encodedBytes",
      "encoding",
      "partCount",
    ],
    request_start: [
      "requestId",
      "protocolRevision",
      "schemaVersion",
      "projectId",
      "sessionId",
      "leaderGeneration",
      "buildHash",
      "schemaHash",
      "launchNonce",
      "hostInstanceId",
      "targetInstanceId",
      "requestDigest",
      "decodedBytes",
      "encodedBytes",
      "encoding",
      "partCount",
    ],
    request_prepared: [
      "requestId",
      "protocolRevision",
      "schemaVersion",
      "projectId",
      "sessionId",
      "leaderGeneration",
      "buildHash",
      "schemaHash",
      "launchNonce",
      "hostInstanceId",
      "targetInstanceId",
      "requestDigest",
      "decodedBytes",
      "encodedBytes",
      "encoding",
      "partCount",
    ],
    response_error: [
      "requestId", "code", "message", "ambiguous", "rateLimit?", "providerError?",
    ],
  },
  sidecarFrameKinds: [
    ...GP_V2_R1_SCHEMA_DESCRIPTOR.sidecarFrameKinds,
    "request_prepared",
    "upstream_prelude",
    "recovery_complete",
  ],
  hostFrameKinds: [
    ...GP_V2_R1_SCHEMA_DESCRIPTOR.hostFrameKinds,
    "request_start",
  ],
});

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder("utf-8", { fatal: true });
const schemaHashPromises: Partial<Record<ProtocolRevision, Promise<string>>> = {};

function canonicalValue(value: unknown, seen: WeakSet<object>): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Canonical JSON numbers must be finite");
    return JSON.stringify(value);
  }
  if (typeof value !== "object") {
    throw new TypeError(`Canonical JSON does not support ${typeof value}`);
  }
  if (seen.has(value)) throw new TypeError("Canonical JSON does not support cycles");
  seen.add(value);
  try {
    if (Array.isArray(value)) {
      return `[${value.map(item => canonicalValue(item, seen)).join(",")}]`;
    }
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort()
      .map(key => `${JSON.stringify(key)}:${canonicalValue(object[key], seen)}`)
      .join(",")}}`;
  } finally {
    seen.delete(value);
  }
}

export function canonicalJson(value: unknown): string {
  return canonicalValue(value, new WeakSet());
}

export function utf8Bytes(value: string): Uint8Array {
  return TEXT_ENCODER.encode(value);
}

export function utf8Text(value: Uint8Array): string {
  return TEXT_DECODER.decode(value);
}

export function bytesToHex(value: Uint8Array): string {
  let output = "";
  for (const byte of value) output += byte.toString(16).padStart(2, "0");
  return output;
}

export async function sha256Hex(value: string | Uint8Array): Promise<string> {
  const bytes = typeof value === "string" ? utf8Bytes(value) : value;
  const input = bytes.buffer.slice(
    bytes.byteOffset,
    bytes.byteOffset + bytes.byteLength,
  ) as ArrayBuffer;
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", input)));
}

export function base64UrlEncode(value: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < value.length; offset += 32_768) {
    binary += String.fromCharCode(...value.subarray(offset, offset + 32_768));
  }
  return btoa(binary)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

export function base64UrlDecode(value: string): Uint8Array {
  if (!/^[A-Za-z0-9_-]*$/.test(value)) {
    throw new TypeError("Value must be unpadded base64url");
  }
  const padded = value
    .replace(/-/g, "+")
    .replace(/_/g, "/")
    .padEnd(value.length + (4 - value.length % 4) % 4, "=");
  let binary: string;
  try {
    binary = atob(padded);
  } catch {
    throw new TypeError("Value must be valid base64url");
  }
  const output = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index++) output[index] = binary.charCodeAt(index);
  return output;
}

export function protocolSchemaHash(revision: ProtocolRevision): Promise<string> {
  const existing = schemaHashPromises[revision];
  if (existing) return existing;
  const descriptor = revision === "r1"
    ? GP_V2_R1_SCHEMA_DESCRIPTOR
    : GP_V2_R2_SCHEMA_DESCRIPTOR;
  const created = sha256Hex(canonicalJson(descriptor));
  schemaHashPromises[revision] = created;
  return created;
}
