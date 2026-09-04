import {
  BootstrapCryptoError,
  validateBootstrapBinding,
  validateBootstrapCredential,
  type BootstrapBinding,
  type BootstrapCredentialPayload,
} from "../bootstrap/crypto";
import {
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  utf8Bytes,
  utf8Text,
} from "../protocol/shared";

const SESSION_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{15,255}$/;
const INSTANCE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const HKDF_INFO = utf8Bytes("ghostprov-v2-supervisor-session-bootstrap/v1");

export interface SupervisorSessionBootstrapPayload {
  readonly instanceId: string;
  readonly credential: BootstrapCredentialPayload;
}

export interface SealedSupervisorSessionBootstrap {
  readonly version: 1;
  readonly binding: BootstrapBinding;
  readonly salt: string;
  readonly iv: string;
  readonly ciphertext: string;
}

function buffer(value: Uint8Array): ArrayBuffer {
  return value.buffer.slice(
    value.byteOffset,
    value.byteOffset + value.byteLength,
  ) as ArrayBuffer;
}

function exactKeys(value: object, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}

function validateSessionId(value: string): string {
  if (!SESSION_ID.test(value)) {
    throw new BootstrapCryptoError("Supervisor bootstrap session identity is invalid", "key");
  }
  return value;
}

async function deriveKey(
  sessionId: string,
  salt: Uint8Array,
  usages: KeyUsage[],
): Promise<CryptoKey> {
  const input = utf8Bytes(validateSessionId(sessionId));
  try {
    const material = await crypto.subtle.importKey("raw", buffer(input), "HKDF", false, ["deriveKey"]);
    return await crypto.subtle.deriveKey({
      name: "HKDF",
      hash: "SHA-256",
      salt: buffer(salt),
      info: buffer(HKDF_INFO),
    }, material, { name: "AES-GCM", length: 256 }, false, usages);
  } catch (error) {
    if (error instanceof BootstrapCryptoError) throw error;
    throw new BootstrapCryptoError("Supervisor bootstrap key derivation failed", "key");
  } finally {
    input.fill(0);
  }
}

export async function sealSupervisorSessionBootstrap(input: {
  readonly sessionId: string;
  readonly binding: BootstrapBinding;
  readonly payload: SupervisorSessionBootstrapPayload;
}): Promise<SealedSupervisorSessionBootstrap> {
  const binding = validateBootstrapBinding(input.binding);
  if (!INSTANCE_ID.test(input.payload.instanceId)) {
    throw new BootstrapCryptoError("Supervisor bootstrap instance is invalid", "payload");
  }
  const payload = {
    instanceId: input.payload.instanceId,
    credential: validateBootstrapCredential(input.payload.credential, binding),
  } satisfies SupervisorSessionBootstrapPayload;
  const salt = crypto.getRandomValues(new Uint8Array(32));
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const plaintext = utf8Bytes(canonicalJson(payload));
  const associatedData = utf8Bytes(canonicalJson(binding));
  const key = await deriveKey(input.sessionId, salt, ["encrypt"]);
  try {
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt({
      name: "AES-GCM",
      iv: buffer(iv),
      additionalData: buffer(associatedData),
      tagLength: 128,
    }, key, buffer(plaintext)));
    return {
      version: 1,
      binding,
      salt: base64UrlEncode(salt),
      iv: base64UrlEncode(iv),
      ciphertext: base64UrlEncode(ciphertext),
    };
  } catch {
    throw new BootstrapCryptoError("Supervisor bootstrap encryption failed", "ciphertext");
  } finally {
    salt.fill(0);
    iv.fill(0);
    plaintext.fill(0);
    associatedData.fill(0);
  }
}

export async function openSupervisorSessionBootstrap(input: {
  readonly sessionId: string;
  readonly expectedBinding: BootstrapBinding;
  readonly sealed: SealedSupervisorSessionBootstrap;
}): Promise<SupervisorSessionBootstrapPayload> {
  const expected = validateBootstrapBinding(input.expectedBinding);
  const sealed = input.sealed;
  if (!sealed || typeof sealed !== "object" || !exactKeys(sealed, [
    "version", "binding", "salt", "iv", "ciphertext",
  ]) || sealed.version !== 1) {
    throw new BootstrapCryptoError("Sealed supervisor bootstrap is invalid", "ciphertext");
  }
  const binding = validateBootstrapBinding(sealed.binding);
  if (canonicalJson(binding) !== canonicalJson(expected)) {
    throw new BootstrapCryptoError("Supervisor bootstrap binding does not match", "binding");
  }
  let salt: Uint8Array;
  let iv: Uint8Array;
  let ciphertext: Uint8Array;
  try {
    salt = base64UrlDecode(sealed.salt);
    iv = base64UrlDecode(sealed.iv);
    ciphertext = base64UrlDecode(sealed.ciphertext);
  } catch {
    throw new BootstrapCryptoError("Supervisor bootstrap bytes are invalid", "ciphertext");
  }
  if (salt.byteLength !== 32 || iv.byteLength !== 12 || ciphertext.byteLength < 16) {
    salt.fill(0);
    iv.fill(0);
    ciphertext.fill(0);
    throw new BootstrapCryptoError("Supervisor bootstrap byte lengths are invalid", "ciphertext");
  }
  const associatedData = utf8Bytes(canonicalJson(expected));
  const key = await deriveKey(input.sessionId, salt, ["decrypt"]);
  let plaintext: Uint8Array | null = null;
  try {
    plaintext = new Uint8Array(await crypto.subtle.decrypt({
      name: "AES-GCM",
      iv: buffer(iv),
      additionalData: buffer(associatedData),
      tagLength: 128,
    }, key, buffer(ciphertext)));
    const decoded = JSON.parse(utf8Text(plaintext)) as unknown;
    if (!decoded || typeof decoded !== "object" || Array.isArray(decoded) ||
      !exactKeys(decoded, ["instanceId", "credential"])) {
      throw new BootstrapCryptoError("Supervisor bootstrap payload is invalid", "payload");
    }
    const raw = decoded as Record<string, unknown>;
    if (typeof raw.instanceId !== "string" || !INSTANCE_ID.test(raw.instanceId)) {
      throw new BootstrapCryptoError("Supervisor bootstrap instance is invalid", "payload");
    }
    return Object.freeze({
      instanceId: raw.instanceId,
      credential: validateBootstrapCredential(raw.credential as BootstrapCredentialPayload, expected),
    });
  } catch (error) {
    if (error instanceof BootstrapCryptoError) throw error;
    throw new BootstrapCryptoError("Supervisor bootstrap decryption failed", "ciphertext");
  } finally {
    salt.fill(0);
    iv.fill(0);
    ciphertext.fill(0);
    associatedData.fill(0);
    plaintext?.fill(0);
  }
}
