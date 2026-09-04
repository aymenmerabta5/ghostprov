import {
  GP_V2_R2_SCHEMA_VERSION,
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  utf8Bytes,
  utf8Text,
} from "../protocol/shared";

interface BootstrapBindingBase {
  projectId: string;
  sandboxSessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
  nonce: string;
}

/** Exact frozen R1 bootstrap binding. */
export interface R1BootstrapBinding extends BootstrapBindingBase {
  protocolRevision?: never;
  schemaVersion?: never;
}

/** Exact R2 bootstrap binding authenticated by the bootstrap ciphertext. */
export interface R2BootstrapBinding extends BootstrapBindingBase {
  protocolRevision: "r2";
  schemaVersion: typeof GP_V2_R2_SCHEMA_VERSION;
}

export type BootstrapBinding = R1BootstrapBinding | R2BootstrapBinding;

export interface BootstrapKeyPair {
  publicJwk: JsonWebKey;
  privateKey: CryptoKey;
}

export interface BootstrapCredentialPayload {
  url: string;
  baseUrl: string;
  docId: string;
  token: string;
  authorization: string;
  rotationKey: string;
}

export interface SealedBootstrapCredential {
  version: 1;
  binding: BootstrapBinding;
  hostPublicJwk: JsonWebKey;
  salt: string;
  iv: string;
  ciphertext: string;
}

interface CredentialRotationBindingBase {
  projectId: string;
  sandboxSessionId: string;
  leaderGeneration: number;
  buildHash: string;
  schemaHash: string;
  instanceId: string;
  rotationId: string;
}

export interface R1CredentialRotationBinding extends CredentialRotationBindingBase {
  protocolRevision?: never;
  schemaVersion?: never;
  launchNonce?: never;
}

export interface R2CredentialRotationBinding extends CredentialRotationBindingBase {
  protocolRevision: "r2";
  schemaVersion: typeof GP_V2_R2_SCHEMA_VERSION;
  launchNonce: string;
}

export type CredentialRotationBinding =
  | R1CredentialRotationBinding
  | R2CredentialRotationBinding;

export type RotatableCredentialPayload = Omit<BootstrapCredentialPayload, "rotationKey">;

export interface SealedCredentialRotation {
  version: 1;
  binding: CredentialRotationBinding;
  iv: string;
  ciphertext: string;
}

export type BootstrapCryptoErrorCode =
  | "binding"
  | "key"
  | "ciphertext"
  | "payload";

export class BootstrapCryptoError extends Error {
  readonly name = "BootstrapCryptoError";

  constructor(
    message: string,
    readonly code: BootstrapCryptoErrorCode,
  ) {
    super(message);
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SHA256 = /^[a-f0-9]{64}$/;
const HKDF_INFO = utf8Bytes("ghostprov-v2-bootstrap/v1");

function exactKeys(value: object, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length &&
    actual.every((key, index) => key === sorted[index]);
}

function buffer(value: Uint8Array): ArrayBuffer {
  return value.buffer.slice(
    value.byteOffset,
    value.byteOffset + value.byteLength,
  ) as ArrayBuffer;
}

// Public aliases let R9 reuse validation without changing frozen R8 identifiers.
export { validateBinding as validateBootstrapBinding, validateCredential as validateBootstrapCredential };

function validateBinding(value: BootstrapBinding): BootstrapBinding {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BootstrapCryptoError("Bootstrap binding is invalid", "binding");
  }
  const r2 = value.protocolRevision === "r2";
  if (!exactKeys(value, [
    "projectId", "sandboxSessionId", "leaderGeneration", "buildHash", "schemaHash", "nonce",
    ...(r2 ? ["protocolRevision", "schemaVersion"] : []),
  ])) {
    throw new BootstrapCryptoError("Bootstrap binding fields are invalid", "binding");
  }
  if (r2 && value.schemaVersion !== GP_V2_R2_SCHEMA_VERSION) {
    throw new BootstrapCryptoError("Bootstrap protocol revision is invalid", "binding");
  }
  if (!UUID.test(value.projectId)) {
    throw new BootstrapCryptoError("Bootstrap project binding is invalid", "binding");
  }
  if (!SAFE_ID.test(value.sandboxSessionId)) {
    throw new BootstrapCryptoError("Bootstrap sandbox-session binding is invalid", "binding");
  }
  if (!Number.isSafeInteger(value.leaderGeneration) || value.leaderGeneration < 1) {
    throw new BootstrapCryptoError("Bootstrap leader generation is invalid", "binding");
  }
  if (!SHA256.test(value.buildHash) || !SHA256.test(value.schemaHash)) {
    throw new BootstrapCryptoError("Bootstrap build/schema binding is invalid", "binding");
  }
  if (!SAFE_ID.test(value.nonce)) {
    throw new BootstrapCryptoError("Bootstrap nonce is invalid", "binding");
  }
  const base = {
    projectId: value.projectId,
    sandboxSessionId: value.sandboxSessionId,
    leaderGeneration: value.leaderGeneration,
    buildHash: value.buildHash,
    schemaHash: value.schemaHash,
    nonce: value.nonce,
  };
  return r2
    ? { ...base, protocolRevision: "r2", schemaVersion: GP_V2_R2_SCHEMA_VERSION }
    : base;
}

function validateUrl(value: string, protocols: readonly string[], label: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new BootstrapCryptoError(`${label} URL is invalid`, "payload");
  }
  if (!protocols.includes(parsed.protocol)) {
    throw new BootstrapCryptoError(`${label} URL protocol is invalid`, "payload");
  }
  if (parsed.username || parsed.password) {
    throw new BootstrapCryptoError(`${label} URL must not contain credentials`, "payload");
  }
  for (const key of parsed.searchParams.keys()) {
    if (/(?:token|key|secret|authorization|credential)/i.test(key)) {
      throw new BootstrapCryptoError(`${label} URL must not contain credential parameters`, "payload");
    }
  }
  return value;
}

function validateRotatableCredential(
  value: RotatableCredentialPayload,
  binding: BootstrapBinding,
): RotatableCredentialPayload {
  if (!value || typeof value !== "object") {
    throw new BootstrapCryptoError("Bootstrap credential must be an object", "payload");
  }
  if (!UUID.test(value.docId) || value.docId !== binding.projectId) {
    throw new BootstrapCryptoError("Bootstrap credential document is invalid", "payload");
  }
  if (typeof value.token !== "string" || value.token.length < 1 || value.token.length > 32_768) {
    throw new BootstrapCryptoError("Bootstrap credential token is invalid", "payload");
  }
  if (
    typeof value.authorization !== "string" ||
    value.authorization.length < 1 ||
    value.authorization.length > 64
  ) {
    throw new BootstrapCryptoError("Bootstrap credential authorization is invalid", "payload");
  }
  return {
    url: validateUrl(value.url, ["wss:", "ws:"], "Y-Sweet WebSocket"),
    baseUrl: validateUrl(value.baseUrl, ["https:", "http:"], "Y-Sweet base"),
    docId: value.docId,
    token: value.token,
    authorization: value.authorization,
  };
}

function validateCredential(
  value: BootstrapCredentialPayload,
  binding: BootstrapBinding,
): BootstrapCredentialPayload {
  const credential = validateRotatableCredential(value, binding);
  rotationKeyBytes(value.rotationKey).fill(0);
  return { ...credential, rotationKey: value.rotationKey };
}

async function importPublicKey(jwk: JsonWebKey): Promise<CryptoKey> {
  try {
    return await crypto.subtle.importKey(
      "jwk",
      jwk,
      { name: "ECDH", namedCurve: "P-256" },
      false,
      [],
    );
  } catch {
    throw new BootstrapCryptoError("Bootstrap public key is invalid", "key");
  }
}

async function deriveAesKey(input: {
  privateKey: CryptoKey;
  publicJwk: JsonWebKey;
  salt: Uint8Array;
  usages: KeyUsage[];
}): Promise<{ key: CryptoKey; shared: Uint8Array }> {
  const publicKey = await importPublicKey(input.publicJwk);
  let shared: Uint8Array;
  try {
    shared = new Uint8Array(await crypto.subtle.deriveBits(
      { name: "ECDH", public: publicKey },
      input.privateKey,
      256,
    ));
  } catch {
    throw new BootstrapCryptoError("Bootstrap key agreement failed", "key");
  }
  try {
    const material = await crypto.subtle.importKey(
      "raw",
      buffer(shared),
      "HKDF",
      false,
      ["deriveKey"],
    );
    const key = await crypto.subtle.deriveKey(
      {
        name: "HKDF",
        hash: "SHA-256",
        salt: buffer(input.salt),
        info: buffer(HKDF_INFO),
      },
      material,
      { name: "AES-GCM", length: 256 },
      false,
      input.usages,
    );
    return { key, shared };
  } catch {
    shared.fill(0);
    throw new BootstrapCryptoError("Bootstrap key derivation failed", "key");
  }
}

export async function createBootstrapKeyPair(
  binding: BootstrapBinding,
): Promise<BootstrapKeyPair> {
  validateBinding(binding);
  let keys: CryptoKeyPair;
  try {
    keys = await crypto.subtle.generateKey(
      { name: "ECDH", namedCurve: "P-256" },
      true,
      ["deriveBits"],
    ) as CryptoKeyPair;
    const publicJwk = await crypto.subtle.exportKey("jwk", keys.publicKey);
    return { publicJwk, privateKey: keys.privateKey };
  } catch {
    throw new BootstrapCryptoError("Bootstrap key generation failed", "key");
  }
}

export async function sealBootstrapCredential(input: {
  binding: BootstrapBinding;
  sidecarPublicJwk: JsonWebKey;
  credential: BootstrapCredentialPayload;
}): Promise<SealedBootstrapCredential> {
  const binding = validateBinding(input.binding);
  const credential = validateCredential(input.credential, binding);
  const host = await createBootstrapKeyPair(binding);
  const salt = crypto.getRandomValues(new Uint8Array(32));
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const plaintext = utf8Bytes(canonicalJson(credential));
  const associatedData = utf8Bytes(canonicalJson(binding));
  const derived = await deriveAesKey({
    privateKey: host.privateKey,
    publicJwk: input.sidecarPublicJwk,
    salt,
    usages: ["encrypt"],
  });
  try {
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv: buffer(iv),
        additionalData: buffer(associatedData),
        tagLength: 128,
      },
      derived.key,
      buffer(plaintext),
    ));
    return {
      version: 1,
      binding,
      hostPublicJwk: host.publicJwk,
      salt: base64UrlEncode(salt),
      iv: base64UrlEncode(iv),
      ciphertext: base64UrlEncode(ciphertext),
    };
  } catch (error) {
    if (error instanceof BootstrapCryptoError) throw error;
    throw new BootstrapCryptoError("Bootstrap credential encryption failed", "ciphertext");
  } finally {
    plaintext.fill(0);
    associatedData.fill(0);
    derived.shared.fill(0);
  }
}

function parseSealed(value: SealedBootstrapCredential): {
  binding: BootstrapBinding;
  hostPublicJwk: JsonWebKey;
  salt: Uint8Array;
  iv: Uint8Array;
  ciphertext: Uint8Array;
} {
  if (!value || typeof value !== "object" || value.version !== 1) {
    throw new BootstrapCryptoError("Sealed bootstrap credential is invalid", "ciphertext");
  }
  const binding = validateBinding(value.binding);
  if (!value.hostPublicJwk || typeof value.hostPublicJwk !== "object") {
    throw new BootstrapCryptoError("Sealed bootstrap host key is invalid", "key");
  }
  let salt: Uint8Array;
  let iv: Uint8Array;
  let ciphertext: Uint8Array;
  try {
    salt = base64UrlDecode(value.salt);
    iv = base64UrlDecode(value.iv);
    ciphertext = base64UrlDecode(value.ciphertext);
  } catch {
    throw new BootstrapCryptoError("Sealed bootstrap bytes are invalid", "ciphertext");
  }
  if (salt.byteLength !== 32 || iv.byteLength !== 12 || ciphertext.byteLength < 16) {
    throw new BootstrapCryptoError("Sealed bootstrap byte lengths are invalid", "ciphertext");
  }
  return { binding, hostPublicJwk: value.hostPublicJwk, salt, iv, ciphertext };
}

export async function openBootstrapCredential(input: {
  sealed: SealedBootstrapCredential;
  sidecarPrivateKey: CryptoKey;
  expectedBinding: BootstrapBinding;
}): Promise<BootstrapCredentialPayload> {
  const expected = validateBinding(input.expectedBinding);
  const sealed = parseSealed(input.sealed);
  if (canonicalJson(sealed.binding) !== canonicalJson(expected)) {
    throw new BootstrapCryptoError("Bootstrap credential binding does not match", "binding");
  }
  const associatedData = utf8Bytes(canonicalJson(expected));
  const derived = await deriveAesKey({
    privateKey: input.sidecarPrivateKey,
    publicJwk: sealed.hostPublicJwk,
    salt: sealed.salt,
    usages: ["decrypt"],
  });
  let plaintext: Uint8Array | null = null;
  try {
    plaintext = new Uint8Array(await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: buffer(sealed.iv),
        additionalData: buffer(associatedData),
        tagLength: 128,
      },
      derived.key,
      buffer(sealed.ciphertext),
    ));
    let decoded: unknown;
    try {
      decoded = JSON.parse(utf8Text(plaintext));
    } catch {
      throw new BootstrapCryptoError("Bootstrap credential payload is invalid", "payload");
    }
    return validateCredential(decoded as BootstrapCredentialPayload, expected);
  } catch (error) {
    if (error instanceof BootstrapCryptoError) throw error;
    throw new BootstrapCryptoError("Bootstrap credential decryption failed", "ciphertext");
  } finally {
    plaintext?.fill(0);
    associatedData.fill(0);
    derived.shared.fill(0);
    sealed.ciphertext.fill(0);
  }
}

function validateRotationBinding(
  value: CredentialRotationBinding,
): CredentialRotationBinding {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new BootstrapCryptoError("Credential rotation binding is invalid", "binding");
  }
  const r2 = value.protocolRevision === "r2";
  if (!exactKeys(value, [
    "projectId", "sandboxSessionId", "leaderGeneration", "buildHash", "schemaHash",
    "instanceId", "rotationId",
    ...(r2 ? ["protocolRevision", "schemaVersion", "launchNonce"] : []),
  ])) {
    throw new BootstrapCryptoError("Credential rotation binding fields are invalid", "binding");
  }
  const bootstrapBase = {
    projectId: value.projectId,
    sandboxSessionId: value.sandboxSessionId,
    leaderGeneration: value.leaderGeneration,
    buildHash: value.buildHash,
    schemaHash: value.schemaHash,
    nonce: value.rotationId,
  };
  const bootstrap = validateBinding(r2
    ? {
        ...bootstrapBase,
        protocolRevision: "r2",
        schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      }
    : bootstrapBase);
  if (!SAFE_ID.test(value.instanceId)) {
    throw new BootstrapCryptoError("Credential rotation instance is invalid", "binding");
  }
  if (r2 && !SAFE_ID.test(value.launchNonce)) {
    throw new BootstrapCryptoError("Credential rotation launch nonce is invalid", "binding");
  }
  const base = {
    projectId: bootstrap.projectId,
    sandboxSessionId: bootstrap.sandboxSessionId,
    leaderGeneration: bootstrap.leaderGeneration,
    buildHash: bootstrap.buildHash,
    schemaHash: bootstrap.schemaHash,
    instanceId: value.instanceId,
    rotationId: bootstrap.nonce,
  };
  return r2
    ? {
        ...base,
        protocolRevision: "r2",
        schemaVersion: GP_V2_R2_SCHEMA_VERSION,
        launchNonce: value.launchNonce,
      }
    : base;
}

function rotationKeyBytes(value: string): Uint8Array {
  let bytes: Uint8Array;
  try {
    bytes = base64UrlDecode(value);
  } catch {
    throw new BootstrapCryptoError("Credential rotation key is invalid", "key");
  }
  if (bytes.byteLength !== 32) {
    bytes.fill(0);
    throw new BootstrapCryptoError("Credential rotation key must be 256 bits", "key");
  }
  return bytes;
}

function bootstrapBindingForRotation(
  binding: CredentialRotationBinding,
): BootstrapBinding {
  const base = {
    projectId: binding.projectId,
    sandboxSessionId: binding.sandboxSessionId,
    leaderGeneration: binding.leaderGeneration,
    buildHash: binding.buildHash,
    schemaHash: binding.schemaHash,
    nonce: binding.rotationId,
  };
  return binding.protocolRevision === "r2"
    ? {
        ...base,
        protocolRevision: "r2",
        schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      }
    : base;
}

async function importRotationKey(
  value: string,
  usages: KeyUsage[],
): Promise<{ key: CryptoKey; bytes: Uint8Array }> {
  const bytes = rotationKeyBytes(value);
  try {
    const key = await crypto.subtle.importKey(
      "raw",
      buffer(bytes),
      { name: "AES-GCM", length: 256 },
      false,
      usages,
    );
    return { key, bytes };
  } catch {
    bytes.fill(0);
    throw new BootstrapCryptoError("Credential rotation key import failed", "key");
  }
}

export function createCredentialRotationKey(): string {
  return base64UrlEncode(crypto.getRandomValues(new Uint8Array(32)));
}

export async function sealCredentialRotation(input: {
  rotationKey: string;
  binding: CredentialRotationBinding;
  credential: RotatableCredentialPayload;
}): Promise<SealedCredentialRotation> {
  const binding = validateRotationBinding(input.binding);
  const credential = validateRotatableCredential(
    input.credential,
    bootstrapBindingForRotation(binding),
  );
  const imported = await importRotationKey(input.rotationKey, ["encrypt"]);
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const plaintext = utf8Bytes(canonicalJson(credential));
  const associatedData = utf8Bytes(canonicalJson(binding));
  try {
    const ciphertext = new Uint8Array(await crypto.subtle.encrypt(
      {
        name: "AES-GCM",
        iv: buffer(iv),
        additionalData: buffer(associatedData),
        tagLength: 128,
      },
      imported.key,
      buffer(plaintext),
    ));
    return {
      version: 1,
      binding,
      iv: base64UrlEncode(iv),
      ciphertext: base64UrlEncode(ciphertext),
    };
  } catch {
    throw new BootstrapCryptoError("Credential rotation encryption failed", "ciphertext");
  } finally {
    imported.bytes.fill(0);
    plaintext.fill(0);
    associatedData.fill(0);
  }
}

export async function openCredentialRotation(input: {
  rotationKey: string;
  sealed: SealedCredentialRotation;
  expectedBinding: CredentialRotationBinding;
}): Promise<RotatableCredentialPayload> {
  const expected = validateRotationBinding(input.expectedBinding);
  const sealed = input.sealed;
  if (!sealed || typeof sealed !== "object" || sealed.version !== 1) {
    throw new BootstrapCryptoError("Sealed credential rotation is invalid", "ciphertext");
  }
  const binding = validateRotationBinding(sealed.binding);
  if (canonicalJson(binding) !== canonicalJson(expected)) {
    throw new BootstrapCryptoError("Credential rotation binding does not match", "binding");
  }
  let iv: Uint8Array;
  let ciphertext: Uint8Array;
  try {
    iv = base64UrlDecode(sealed.iv);
    ciphertext = base64UrlDecode(sealed.ciphertext);
  } catch {
    throw new BootstrapCryptoError("Credential rotation bytes are invalid", "ciphertext");
  }
  if (iv.byteLength !== 12 || ciphertext.byteLength < 16) {
    throw new BootstrapCryptoError("Credential rotation byte lengths are invalid", "ciphertext");
  }
  const imported = await importRotationKey(input.rotationKey, ["decrypt"]);
  const associatedData = utf8Bytes(canonicalJson(expected));
  let plaintext: Uint8Array | null = null;
  try {
    plaintext = new Uint8Array(await crypto.subtle.decrypt(
      {
        name: "AES-GCM",
        iv: buffer(iv),
        additionalData: buffer(associatedData),
        tagLength: 128,
      },
      imported.key,
      buffer(ciphertext),
    ));
    let decoded: unknown;
    try {
      decoded = JSON.parse(utf8Text(plaintext));
    } catch {
      throw new BootstrapCryptoError("Credential rotation payload is invalid", "payload");
    }
    return validateRotatableCredential(
      decoded as RotatableCredentialPayload,
      bootstrapBindingForRotation(expected),
    );
  } catch (error) {
    if (error instanceof BootstrapCryptoError) throw error;
    throw new BootstrapCryptoError("Credential rotation decryption failed", "ciphertext");
  } finally {
    imported.bytes.fill(0);
    plaintext?.fill(0);
    associatedData.fill(0);
    ciphertext.fill(0);
  }
}
