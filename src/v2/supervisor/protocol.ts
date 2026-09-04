import type { BootstrapBinding } from "../bootstrap/crypto";
import {
  GP_V2_R2_SCHEMA_VERSION,
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  utf8Bytes,
} from "../protocol/shared";

export const GP_V2_SUPERVISOR_PROTOCOL = "gp-v2-supervisor" as const;
export const GP_V2_SUPERVISOR_VERSION = 1 as const;
export const GP_V2_SUPERVISOR_FIELD = "ghostprovV2Supervisor" as const;
export const GP_V2_SUPERVISOR_MAX_SEQUENCE = 1_000_000_000;
export const GP_V2_SUPERVISOR_MAX_STATIC_BYTES = 32 * 1024;
export const GP_V2_SUPERVISOR_MAX_CREDENTIAL_BYTES = 32 * 1024;
export const GP_V2_SUPERVISOR_MAX_HELLO_CHARS = 8 * 1024;
export const GP_V2_SUPERVISOR_MAX_STOP_NONCES = 64;

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SAFE_REASON = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/;
const SAFE_ENTRYPOINT = /^[A-Za-z0-9.][A-Za-z0-9._/-]{0,255}$/;
const SHA256 = /^[a-f0-9]{64}$/;
const BASE64_URL = /^[A-Za-z0-9_-]+$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export type SupervisorErrorCode =
  | "binding_conflict"
  | "command_conflict"
  | "credential_invalid"
  | "entrypoint_invalid"
  | "hello_timeout"
  | "identity_invalid"
  | "launch_failed"
  | "not_ready"
  | "process_conflict"
  | "shutdown_failed"
  | "spawn_failed"
  | "stage_failed"
  | "sidecar_awareness"
  | "sidecar_bootstrap"
  | "sidecar_config"
  | "sidecar_environment"
  | "sidecar_engine"
  | "sidecar_ownership"
  | "sidecar_permission"
  | "sidecar_permission_env"
  | "sidecar_permission_ffi"
  | "sidecar_permission_net"
  | "sidecar_permission_read"
  | "sidecar_permission_run"
  | "sidecar_permission_sys"
  | "sidecar_permission_write"
  | "sidecar_wrapper"
  | "sidecar_exit"
  | "sidecar_storage"
  | "sidecar_supervisor_proc"
  | "handler_exception"
  | "handler_not_found"
  | "handler_type"
  | "stop_failed";

export interface SupervisorProcessIdentity {
  readonly pid: number;
  readonly startTick: string;
  readonly processGroup: number;
  readonly session: number;
}

export type SupervisorCommand =
  | {
      readonly kind: "launch";
      readonly sidecarBinding: BootstrapBinding;
      readonly entrypointPath: string;
      readonly staticConfig: string;
    }
  | {
      readonly kind: "credential";
      readonly sidecarBinding: BootstrapBinding;
      readonly sealedJson: string;
    }
  | {
      readonly kind: "stop";
      readonly launchNonces: readonly string[];
      readonly reason: string;
    }
  | {
      readonly kind: "rotate";
      readonly rotationId: string;
      readonly sealedJson: string;
    }
  | {
      readonly kind: "shutdown";
      readonly reason: string;
    };

export type SupervisorStatus =
  | { readonly kind: "ready" }
  | {
      readonly kind: "launch_hello";
      readonly sidecarBinding: BootstrapBinding;
      readonly encodedHello: string;
      readonly process: SupervisorProcessIdentity;
    }
  | {
      readonly kind: "credential_written";
      readonly launchNonce: string;
    }
  | {
      readonly kind: "stopped";
      readonly launchNonce: string | null;
    }
  | {
      readonly kind: "credential_rotated";
      readonly rotationId: string;
    }
  | { readonly kind: "shutdown" }
  | {
      readonly kind: "error";
      readonly code: SupervisorErrorCode;
    };

export interface SupervisorMessageBase<TMessage> {
  readonly protocol: typeof GP_V2_SUPERVISOR_PROTOCOL;
  readonly version: typeof GP_V2_SUPERVISOR_VERSION;
  readonly direction: "host" | "sandbox";
  readonly binding: BootstrapBinding;
  readonly supervisorInstanceId: string;
  readonly sequence: number;
  readonly message: TMessage;
}

export type SupervisorHostMessageBase = SupervisorMessageBase<SupervisorCommand> & {
  readonly direction: "host";
};

export type SupervisorSandboxMessageBase = SupervisorMessageBase<SupervisorStatus> & {
  readonly direction: "sandbox";
};

export type SignedSupervisorHostMessage = SupervisorHostMessageBase & {
  readonly mac: string;
};

export type SignedSupervisorSandboxMessage = SupervisorSandboxMessageBase & {
  readonly mac: string;
};

export class SupervisorProtocolError extends Error {
  readonly name = "SupervisorProtocolError";

  constructor(message: string, readonly code: "bounds" | "fields" | "mac") {
    super(message);
  }
}

function exactKeys(value: object, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}

function buffer(value: Uint8Array): ArrayBuffer {
  return value.buffer.slice(
    value.byteOffset,
    value.byteOffset + value.byteLength,
  ) as ArrayBuffer;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function validateBinding(value: unknown): BootstrapBinding {
  if (!isRecord(value)) throw new SupervisorProtocolError("Supervisor binding is invalid", "fields");
  if (!exactKeys(value, [
    "projectId",
    "sandboxSessionId",
    "leaderGeneration",
    "buildHash",
    "schemaHash",
    "nonce",
    "protocolRevision",
    "schemaVersion",
  ])) throw new SupervisorProtocolError("Supervisor binding fields are invalid", "fields");
  if (
    !UUID.test(String(value.projectId)) ||
    typeof value.sandboxSessionId !== "string" || !SAFE_ID.test(value.sandboxSessionId) ||
    !Number.isSafeInteger(value.leaderGeneration) || (value.leaderGeneration as number) < 1 ||
    typeof value.buildHash !== "string" || !SHA256.test(value.buildHash) ||
    typeof value.schemaHash !== "string" || !SHA256.test(value.schemaHash) ||
    typeof value.nonce !== "string" || !SAFE_ID.test(value.nonce) ||
    value.protocolRevision !== "r2" ||
    value.schemaVersion !== GP_V2_R2_SCHEMA_VERSION
  ) throw new SupervisorProtocolError("Supervisor binding values are invalid", "fields");
  return Object.freeze({
    projectId: value.projectId as string,
    sandboxSessionId: value.sandboxSessionId,
    leaderGeneration: value.leaderGeneration as number,
    buildHash: value.buildHash,
    schemaHash: value.schemaHash,
    nonce: value.nonce,
    protocolRevision: "r2",
    schemaVersion: GP_V2_R2_SCHEMA_VERSION,
  });
}

function validateProcess(value: unknown): SupervisorProcessIdentity {
  if (!isRecord(value) || !exactKeys(value, ["pid", "startTick", "processGroup", "session"])) {
    throw new SupervisorProtocolError("Supervisor process identity fields are invalid", "fields");
  }
  if (
    !Number.isSafeInteger(value.pid) || (value.pid as number) < 2 ||
    typeof value.startTick !== "string" || !/^\d{1,32}$/.test(value.startTick) ||
    value.processGroup !== value.pid || value.session !== value.pid
  ) throw new SupervisorProtocolError("Supervisor process identity is invalid", "fields");
  return Object.freeze({
    pid: value.pid as number,
    startTick: value.startTick,
    processGroup: value.processGroup as number,
    session: value.session as number,
  });
}

function validateReason(value: unknown): string {
  if (typeof value !== "string" || !SAFE_REASON.test(value)) {
    throw new SupervisorProtocolError("Supervisor reason is invalid", "fields");
  }
  return value;
}

function validateCommand(value: unknown): SupervisorCommand {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new SupervisorProtocolError("Supervisor command is invalid", "fields");
  }
  if (value.kind === "launch") {
    if (!exactKeys(value, ["kind", "sidecarBinding", "entrypointPath", "staticConfig"])) {
      throw new SupervisorProtocolError("Supervisor launch fields are invalid", "fields");
    }
    if (
      typeof value.entrypointPath !== "string" ||
      !SAFE_ENTRYPOINT.test(value.entrypointPath) ||
      value.entrypointPath.includes("..")
    ) throw new SupervisorProtocolError("Supervisor entrypoint is invalid", "fields");
    if (
      typeof value.staticConfig !== "string" ||
      utf8Bytes(value.staticConfig).byteLength < 2 ||
      utf8Bytes(value.staticConfig).byteLength > GP_V2_SUPERVISOR_MAX_STATIC_BYTES
    ) throw new SupervisorProtocolError("Supervisor static config exceeds bounds", "bounds");
    return Object.freeze({
      kind: "launch",
      sidecarBinding: validateBinding(value.sidecarBinding),
      entrypointPath: value.entrypointPath,
      staticConfig: value.staticConfig,
    });
  }
  if (value.kind === "credential") {
    if (!exactKeys(value, ["kind", "sidecarBinding", "sealedJson"])) {
      throw new SupervisorProtocolError("Supervisor credential fields are invalid", "fields");
    }
    if (
      typeof value.sealedJson !== "string" ||
      utf8Bytes(value.sealedJson).byteLength < 2 ||
      utf8Bytes(value.sealedJson).byteLength > GP_V2_SUPERVISOR_MAX_CREDENTIAL_BYTES
    ) throw new SupervisorProtocolError("Supervisor credential exceeds bounds", "bounds");
    return Object.freeze({
      kind: "credential",
      sidecarBinding: validateBinding(value.sidecarBinding),
      sealedJson: value.sealedJson,
    });
  }
  if (value.kind === "stop") {
    if (!exactKeys(value, ["kind", "launchNonces", "reason"]) || !Array.isArray(value.launchNonces)) {
      throw new SupervisorProtocolError("Supervisor stop fields are invalid", "fields");
    }
    const launchNonces = value.launchNonces.map(entry => {
      if (typeof entry !== "string" || !SAFE_ID.test(entry)) {
        throw new SupervisorProtocolError("Supervisor stop nonce is invalid", "fields");
      }
      return entry;
    });
    if (
      launchNonces.length < 1 ||
      launchNonces.length > GP_V2_SUPERVISOR_MAX_STOP_NONCES ||
      new Set(launchNonces).size !== launchNonces.length
    ) throw new SupervisorProtocolError("Supervisor stop nonce set exceeds bounds", "bounds");
    return Object.freeze({
      kind: "stop",
      launchNonces: Object.freeze([...launchNonces]),
      reason: validateReason(value.reason),
    });
  }
  if (value.kind === "rotate") {
    if (
      !exactKeys(value, ["kind", "rotationId", "sealedJson"]) ||
      typeof value.rotationId !== "string" || !SAFE_ID.test(value.rotationId) ||
      typeof value.sealedJson !== "string" ||
      utf8Bytes(value.sealedJson).byteLength < 2 ||
      utf8Bytes(value.sealedJson).byteLength > GP_V2_SUPERVISOR_MAX_CREDENTIAL_BYTES
    ) throw new SupervisorProtocolError("Supervisor rotation fields are invalid", "fields");
    return Object.freeze({
      kind: "rotate",
      rotationId: value.rotationId,
      sealedJson: value.sealedJson,
    });
  }
  if (value.kind === "shutdown") {
    if (!exactKeys(value, ["kind", "reason"])) {
      throw new SupervisorProtocolError("Supervisor shutdown fields are invalid", "fields");
    }
    return Object.freeze({ kind: "shutdown", reason: validateReason(value.reason) });
  }
  throw new SupervisorProtocolError("Supervisor command kind is invalid", "fields");
}

function validateStatus(value: unknown): SupervisorStatus {
  if (!isRecord(value) || typeof value.kind !== "string") {
    throw new SupervisorProtocolError("Supervisor status is invalid", "fields");
  }
  if (value.kind === "ready") {
    if (!exactKeys(value, ["kind"])) throw new SupervisorProtocolError("Supervisor ready fields are invalid", "fields");
    return Object.freeze({ kind: "ready" });
  }
  if (value.kind === "launch_hello") {
    if (!exactKeys(value, ["kind", "sidecarBinding", "encodedHello", "process"])) {
      throw new SupervisorProtocolError("Supervisor hello fields are invalid", "fields");
    }
    if (
      typeof value.encodedHello !== "string" ||
      value.encodedHello.length < 1 ||
      value.encodedHello.length > GP_V2_SUPERVISOR_MAX_HELLO_CHARS ||
      !BASE64_URL.test(value.encodedHello)
    ) throw new SupervisorProtocolError("Supervisor hello exceeds bounds", "bounds");
    return Object.freeze({
      kind: "launch_hello",
      sidecarBinding: validateBinding(value.sidecarBinding),
      encodedHello: value.encodedHello,
      process: validateProcess(value.process),
    });
  }
  if (value.kind === "credential_written") {
    if (!exactKeys(value, ["kind", "launchNonce"]) || typeof value.launchNonce !== "string" || !SAFE_ID.test(value.launchNonce)) {
      throw new SupervisorProtocolError("Supervisor credential status is invalid", "fields");
    }
    return Object.freeze({ kind: "credential_written", launchNonce: value.launchNonce });
  }
  if (value.kind === "stopped") {
    if (!exactKeys(value, ["kind", "launchNonce"]) || (value.launchNonce !== null && (typeof value.launchNonce !== "string" || !SAFE_ID.test(value.launchNonce)))) {
      throw new SupervisorProtocolError("Supervisor stopped status is invalid", "fields");
    }
    return Object.freeze({ kind: "stopped", launchNonce: value.launchNonce as string | null });
  }
  if (value.kind === "credential_rotated") {
    if (
      !exactKeys(value, ["kind", "rotationId"]) ||
      typeof value.rotationId !== "string" || !SAFE_ID.test(value.rotationId)
    ) throw new SupervisorProtocolError("Supervisor rotation status is invalid", "fields");
    return Object.freeze({ kind: "credential_rotated", rotationId: value.rotationId });
  }
  if (value.kind === "shutdown") {
    if (!exactKeys(value, ["kind"])) throw new SupervisorProtocolError("Supervisor shutdown status is invalid", "fields");
    return Object.freeze({ kind: "shutdown" });
  }
  if (value.kind === "error") {
    const codes = new Set<SupervisorErrorCode>([
      "binding_conflict",
      "command_conflict",
      "credential_invalid",
      "entrypoint_invalid",
      "hello_timeout",
      "identity_invalid",
      "launch_failed",
      "not_ready",
      "process_conflict",
      "shutdown_failed",
      "spawn_failed",
      "stage_failed",
      "sidecar_awareness",
      "sidecar_bootstrap",
      "sidecar_config",
      "sidecar_environment",
      "sidecar_engine",
      "sidecar_ownership",
      "sidecar_permission",
      "sidecar_permission_env",
      "sidecar_permission_ffi",
      "sidecar_permission_net",
      "sidecar_permission_read",
      "sidecar_permission_run",
      "sidecar_permission_sys",
      "sidecar_permission_write",
      "sidecar_wrapper",
      "sidecar_exit",
      "sidecar_storage",
      "sidecar_supervisor_proc",
      "handler_exception",
      "handler_not_found",
      "handler_type",
      "stop_failed",
    ]);
    if (!exactKeys(value, ["kind", "code"]) || typeof value.code !== "string" || !codes.has(value.code as SupervisorErrorCode)) {
      throw new SupervisorProtocolError("Supervisor error status is invalid", "fields");
    }
    return Object.freeze({ kind: "error", code: value.code as SupervisorErrorCode });
  }
  throw new SupervisorProtocolError("Supervisor status kind is invalid", "fields");
}

function validateMessageBase(
  value: unknown,
  direction: "host" | "sandbox",
): SupervisorMessageBase<SupervisorCommand | SupervisorStatus> & { readonly mac: string } {
  if (!isRecord(value) || !exactKeys(value, [
    "protocol",
    "version",
    "direction",
    "binding",
    "supervisorInstanceId",
    "sequence",
    "message",
    "mac",
  ])) throw new SupervisorProtocolError("Supervisor message fields are invalid", "fields");
  if (
    value.protocol !== GP_V2_SUPERVISOR_PROTOCOL ||
    value.version !== GP_V2_SUPERVISOR_VERSION ||
    value.direction !== direction ||
    typeof value.supervisorInstanceId !== "string" || !SAFE_ID.test(value.supervisorInstanceId) ||
    !Number.isSafeInteger(value.sequence) || (value.sequence as number) < 0 ||
    (value.sequence as number) > GP_V2_SUPERVISOR_MAX_SEQUENCE ||
    typeof value.mac !== "string" || value.mac.length !== 43 || !BASE64_URL.test(value.mac)
  ) throw new SupervisorProtocolError("Supervisor message values are invalid", "fields");
  return {
    protocol: GP_V2_SUPERVISOR_PROTOCOL,
    version: GP_V2_SUPERVISOR_VERSION,
    direction,
    binding: validateBinding(value.binding),
    supervisorInstanceId: value.supervisorInstanceId,
    sequence: value.sequence as number,
    message: direction === "host" ? validateCommand(value.message) : validateStatus(value.message),
    mac: value.mac,
  };
}

export function parseSupervisorCommand(value: unknown): SupervisorCommand {
  return validateCommand(value);
}

function signingBytes(value: SupervisorHostMessageBase | SupervisorSandboxMessageBase): Uint8Array {
  return utf8Bytes(canonicalJson(value));
}

async function importMacKey(rotationKey: string, usages: KeyUsage[]): Promise<CryptoKey> {
  let bytes: Uint8Array;
  try {
    bytes = base64UrlDecode(rotationKey);
  } catch {
    throw new SupervisorProtocolError("Supervisor MAC key is malformed", "mac");
  }
  if (bytes.byteLength !== 32) {
    bytes.fill(0);
    throw new SupervisorProtocolError("Supervisor MAC key must be 256 bits", "mac");
  }
  try {
    return await crypto.subtle.importKey(
      "raw",
      buffer(bytes),
      { name: "HMAC", hash: "SHA-256" },
      false,
      usages,
    );
  } finally {
    bytes.fill(0);
  }
}

async function signBase<T extends SupervisorHostMessageBase | SupervisorSandboxMessageBase>(
  rotationKey: string,
  value: T,
): Promise<T & { readonly mac: string }> {
  const key = await importMacKey(rotationKey, ["sign"]);
  const bytes = signingBytes(value);
  try {
    const signature = await crypto.subtle.sign("HMAC", key, buffer(bytes));
    return Object.freeze({ ...value, mac: base64UrlEncode(new Uint8Array(signature)) }) as unknown as T & {
      readonly mac: string;
    };
  } finally {
    bytes.fill(0);
  }
}

async function verifyBase(
  rotationKey: string,
  value: SupervisorHostMessageBase | SupervisorSandboxMessageBase,
  mac: string,
): Promise<boolean> {
  let signature: Uint8Array;
  try {
    signature = base64UrlDecode(mac);
  } catch {
    return false;
  }
  if (signature.byteLength !== 32) {
    signature.fill(0);
    return false;
  }
  const key = await importMacKey(rotationKey, ["verify"]);
  const bytes = signingBytes(value);
  try {
    return await crypto.subtle.verify(
      "HMAC",
      key,
      buffer(signature),
      buffer(bytes),
    );
  } finally {
    signature.fill(0);
    bytes.fill(0);
  }
}

export async function signSupervisorHostMessage(
  rotationKey: string,
  value: SupervisorHostMessageBase,
): Promise<SignedSupervisorHostMessage> {
  const parsed = validateMessageBase({ ...value, mac: "A".repeat(43) }, "host");
  const base: SupervisorHostMessageBase = {
    protocol: parsed.protocol,
    version: parsed.version,
    direction: "host",
    binding: parsed.binding,
    supervisorInstanceId: parsed.supervisorInstanceId,
    sequence: parsed.sequence,
    message: parsed.message as SupervisorCommand,
  };
  return signBase(rotationKey, base);
}

export async function signSupervisorSandboxMessage(
  rotationKey: string,
  value: SupervisorSandboxMessageBase,
): Promise<SignedSupervisorSandboxMessage> {
  const parsed = validateMessageBase({ ...value, mac: "A".repeat(43) }, "sandbox");
  const base: SupervisorSandboxMessageBase = {
    protocol: parsed.protocol,
    version: parsed.version,
    direction: "sandbox",
    binding: parsed.binding,
    supervisorInstanceId: parsed.supervisorInstanceId,
    sequence: parsed.sequence,
    message: parsed.message as SupervisorStatus,
  };
  return signBase(rotationKey, base);
}

export function parseSupervisorHostMessage(value: unknown): SignedSupervisorHostMessage {
  return validateMessageBase(value, "host") as SignedSupervisorHostMessage;
}

export function parseSupervisorSandboxMessage(value: unknown): SignedSupervisorSandboxMessage {
  return validateMessageBase(value, "sandbox") as SignedSupervisorSandboxMessage;
}

export async function verifySupervisorHostMessage(
  rotationKey: string,
  value: SignedSupervisorHostMessage,
): Promise<boolean> {
  const { mac, ...base } = value;
  return verifyBase(rotationKey, base, mac);
}

export async function verifySupervisorSandboxMessage(
  rotationKey: string,
  value: SignedSupervisorSandboxMessage,
): Promise<boolean> {
  const { mac, ...base } = value;
  return verifyBase(rotationKey, base, mac);
}

export function sameSupervisorBinding(left: BootstrapBinding, right: BootstrapBinding): boolean {
  return canonicalJson(validateBinding(left)) === canonicalJson(validateBinding(right));
}
