import type { V2EnvelopeDiagnostics, V2FirstEventDiagnostics, V2RequestAckDiagnostics, V2TerminalDiagnostics } from "../diagnostics/types";

export const GP_V2_PROTOCOL = "gp-awareness-v2" as const;
export const GP_V2_SCHEMA_VERSION = 1 as const;
export const GP_V2_FRAME_MAX_BYTES = 16 * 1024;
export const GP_V2_WINDOW_MAX_FRAMES = 128;
export const GP_V2_WINDOW_MAX_BYTES = 512 * 1024;
export const GP_V2_REQUEST_MAX_BYTES = 64 * 1024 * 1024;

export type AwarenessLane = "data" | "control";

export interface AckCursor {
  epoch: number;
  sequence: number;
}

export interface FrameIdentity {
  epoch: number;
  sequence: number;
}

export interface ProtocolBinding {
  projectId: string;
  sessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
}

export type RequestEncoding = "identity" | "gzip+base64";
export type TerminalStatus = "completed" | "failed" | "incomplete";

export type HostFrame =
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
  | { kind: "request_commit"; requestId: string }
  | { kind: "request_cancel"; requestId: string; reason: string }
  | { kind: "response_ack"; requestId: string; terminalDigest?: string }
  | { kind: "credential_rotate"; rotationId: string; sealedJson: string }
  | { kind: "drain"; reason: string }
  | { kind: "ping"; sentAtMs: number };

export type SidecarFrame =
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
  }
  | {
    kind: "response_error";
    requestId: string;
    code: string;
    message: string;
    ambiguous: boolean;
  }
  | { kind: "credential_ack"; rotationId: string }
  | { kind: "drained"; active: number; queued: number }
  | { kind: "pong"; sentAtMs: number };

export type ReliableFrame<T> = T & FrameIdentity;

export interface AwarenessEnvelope<T> {
  protocol: typeof GP_V2_PROTOCOL;
  schemaVersion: typeof GP_V2_SCHEMA_VERSION;
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
  diagnostics?: V2EnvelopeDiagnostics;
}

export const GP_V2_SCHEMA_DESCRIPTOR = Object.freeze({
  protocol: GP_V2_PROTOCOL,
  version: GP_V2_SCHEMA_VERSION,
  frameMaxBytes: GP_V2_FRAME_MAX_BYTES,
  windowMaxFrames: GP_V2_WINDOW_MAX_FRAMES,
  windowMaxBytes: GP_V2_WINDOW_MAX_BYTES,
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
    "diagnostics?",
  ],
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
      "transcriptSha256", "terminalStatus", "finalEventIndex", "finalSha256", "diagnostics?",
    ],
    response_error: ["requestId", "code", "message", "ambiguous"],
    credential_ack: ["rotationId"],
    drained: ["active", "queued"],
    pong: ["sentAtMs"],
    reliable: ["epoch", "sequence"],
  },
});

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder("utf-8", { fatal: true });
let schemaHashPromise: Promise<string> | null = null;

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

export function protocolSchemaHash(): Promise<string> {
  schemaHashPromise ??= sha256Hex(canonicalJson(GP_V2_SCHEMA_DESCRIPTOR));
  return schemaHashPromise;
}
