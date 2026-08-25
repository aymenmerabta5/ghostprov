import { rateLimitProblemFromError } from "./rate-limit";

export const PROVIDER_ERROR_PAYLOAD_MAX_BYTES = 8 * 1024;
export const PROVIDER_ERROR_MESSAGE_MAX_BYTES = 2 * 1024;
/** Leaves headroom for the surrounding reliable response_error frame. */
export const PROVIDER_ERROR_ENVELOPE_MAX_BYTES = 12 * 1024;

export const PROVIDER_ERROR_HEADER_NAMES = [
  "retry-after",
  "x-ratelimit-limit-requests",
  "x-ratelimit-remaining-requests",
  "x-ratelimit-reset-requests",
  "x-ratelimit-limit-tokens",
  "x-ratelimit-remaining-tokens",
  "x-ratelimit-reset-tokens",
  "x-ratelimit-limit-input-tokens",
  "x-ratelimit-remaining-input-tokens",
  "x-ratelimit-reset-input-tokens",
  "x-ratelimit-limit-output-tokens",
  "x-ratelimit-remaining-output-tokens",
  "x-ratelimit-reset-output-tokens",
  "x-ratelimit-limit-project-tokens",
  "x-ratelimit-remaining-project-tokens",
  "x-ratelimit-reset-project-tokens",
] as const;

export type ProviderErrorHeaderName = typeof PROVIDER_ERROR_HEADER_NAMES[number];

export interface ProviderErrorShape {
  message: string;
  type: string;
  param: string | null;
  code: string | null;
}

export interface ProviderErrorPayload {
  format: "json" | "text";
  text: string;
  truncated: boolean;
}

/**
 * Credential-scrubbed, bounded upstream failure data. It is safe to persist or
 * move over the V2 protocol; it must never contain authorization/session
 * headers or an unbounded provider body.
 */
export interface ProviderErrorEnvelope {
  version: 1;
  origin: "http" | "sse";
  status: number;
  error: ProviderErrorShape;
  headers: Partial<Record<ProviderErrorHeaderName, string>>;
  upstreamRequestId: string | null;
  model: string | null;
  organizationFingerprint: string | null;
  payload: ProviderErrorPayload;
}

export interface CreateProviderErrorEnvelopeInput {
  origin: ProviderErrorEnvelope["origin"];
  status?: number;
  value: unknown;
  /** Original HTTP body or SSE data. Defaults to `value`. */
  payload?: unknown;
  headers?: Headers;
  model?: unknown;
  fallbackMessage?: string;
}

const ENCODER = new TextEncoder();
const SENSITIVE_KEY = /^(?:authorization|proxy-authorization|cookie|set-cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|token|secret|password)$/i;
const SAFE_REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/;
const HEADER_NAMES = new Set<string>(PROVIDER_ERROR_HEADER_NAMES);
const MAX_HEADER_VALUE_BYTES = 128;
const MAX_CATEGORY_BYTES = 128;
const MAX_PARAM_BYTES = 512;
const MAX_MODEL_BYTES = 128;
const MAX_SANITIZE_DEPTH = 8;
const MAX_SANITIZE_ENTRIES = 256;

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function utf8Length(value: string): number {
  return ENCODER.encode(value).byteLength;
}

function jsonUtf8Length(value: unknown): number {
  try {
    return utf8Length(JSON.stringify(value));
  } catch {
    return Number.POSITIVE_INFINITY;
  }
}

function truncateUtf8(value: string, maxBytes: number): { text: string; truncated: boolean } {
  const bytes = ENCODER.encode(value);
  if (bytes.byteLength <= maxBytes) return { text: value, truncated: false };
  let end = maxBytes;
  // TextDecoder replaces a split code point. Back up continuation bytes so the
  // retained diagnostic is valid UTF-8 and the byte cap remains strict.
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end--;
  return {
    text: new TextDecoder().decode(bytes.subarray(0, end)),
    truncated: true,
  };
}

/** Redacts credential-shaped strings without hiding model/org/request IDs. */
export function sanitizeProviderText(value: string): string {
  return value
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/("(?:authorization|proxy-authorization|cookie|set-cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|token|secret|password)"\s*:\s*)"(?:[^"\\]|\\.)*"/gi, "$1\"[REDACTED]\"")
    .replace(/\b(Bearer|Basic|Digest)\s+[^\s,;"']+/gi, "$1 [REDACTED]")
    .replace(/((?:authorization|proxy-authorization|cookie|set-cookie|api[-_]?key|access[-_]?token|refresh[-_]?token|session[-_]?token|token|secret|password)\s*[:=]\s*)[^\s,;"']+/gi, "$1[REDACTED]")
    .replace(/\bsk-[A-Za-z0-9_-]{16,}\b/g, "[REDACTED]")
    .replace(/\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}(?:\.[A-Za-z0-9_-]{12,})?\b/g, "[REDACTED]");
}

function boundedText(value: unknown, fallback: string, maxBytes: number): string {
  const candidate = typeof value === "string" && value.length > 0 ? value : fallback;
  return truncateUtf8(sanitizeProviderText(candidate), maxBytes).text;
}

function boundedNullableText(value: unknown, maxBytes: number): string | null {
  if (typeof value !== "string") return null;
  return truncateUtf8(sanitizeProviderText(value), maxBytes).text;
}

function statusCode(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 400 && value <= 599
    ? value
    : null;
}

function extractErrorShape(
  value: unknown,
  status: number,
  fallbackMessage: string,
): ProviderErrorShape {
  const root = record(value);
  const nested = record(root?.error);
  const candidate = nested ?? root;
  const typeFallback = status === 429
    ? "rate_limit_error"
    : status === 401 || status === 403
      ? "authentication_error"
      : "upstream_error";
  return {
    message: boundedText(candidate?.message, fallbackMessage, PROVIDER_ERROR_MESSAGE_MAX_BYTES),
    type: boundedText(candidate?.type, typeFallback, MAX_CATEGORY_BYTES),
    param: candidate?.param === null ? null : boundedNullableText(candidate?.param, MAX_PARAM_BYTES),
    code: candidate?.code === null || candidate?.code === undefined
      ? null
      : boundedNullableText(candidate.code, MAX_CATEGORY_BYTES),
  };
}

interface SanitizedValue {
  value: unknown;
  truncated: boolean;
}

function scrubValue(
  value: unknown,
  depth = 0,
  seen: Set<object> = new Set(),
): SanitizedValue {
  if (typeof value === "string") return { value: sanitizeProviderText(value), truncated: false };
  if (value === null || typeof value === "boolean") return { value, truncated: false };
  if (typeof value === "number") {
    return Number.isFinite(value)
      ? { value, truncated: false }
      : { value: null, truncated: true };
  }
  if (typeof value !== "object") return { value: null, truncated: true };
  if (depth >= MAX_SANITIZE_DEPTH || seen.has(value)) {
    return { value: "[TRUNCATED]", truncated: true };
  }
  seen.add(value);
  let truncated = false;
  try {
    if (Array.isArray(value)) {
      const output: unknown[] = [];
      const length = Math.min(value.length, MAX_SANITIZE_ENTRIES);
      for (let index = 0; index < length; index++) {
        const child = scrubValue(value[index], depth + 1, seen);
        output.push(child.value);
        truncated ||= child.truncated;
      }
      if (value.length > length) truncated = true;
      return { value: output, truncated };
    }
    const output: Record<string, unknown> = {};
    const entries = Object.entries(value as Record<string, unknown>);
    for (const [key, childValue] of entries.slice(0, MAX_SANITIZE_ENTRIES)) {
      if (SENSITIVE_KEY.test(key)) {
        output[key] = "[REDACTED]";
        truncated = true;
        continue;
      }
      const child = scrubValue(childValue, depth + 1, seen);
      output[key] = child.value;
      truncated ||= child.truncated;
    }
    if (entries.length > MAX_SANITIZE_ENTRIES) truncated = true;
    return { value: output, truncated };
  } finally {
    seen.delete(value);
  }
}

function safePayload(value: unknown): ProviderErrorPayload {
  let source = value;
  let format: ProviderErrorPayload["format"] = "json";
  if (typeof value === "string") {
    try {
      source = JSON.parse(value);
    } catch {
      format = "text";
      const bounded = truncateUtf8(sanitizeProviderText(value), PROVIDER_ERROR_PAYLOAD_MAX_BYTES);
      return { format, ...bounded };
    }
  }
  const scrubbed = scrubValue(source);
  let encoded: string;
  try {
    encoded = JSON.stringify(scrubbed.value);
  } catch {
    encoded = JSON.stringify("[UNSERIALIZABLE]");
    scrubbed.truncated = true;
  }
  const bounded = truncateUtf8(encoded, PROVIDER_ERROR_PAYLOAD_MAX_BYTES);
  return {
    format,
    text: bounded.text,
    truncated: scrubbed.truncated || bounded.truncated,
  };
}

function selectedHeaders(headers?: Headers): Partial<Record<ProviderErrorHeaderName, string>> {
  const output: Partial<Record<ProviderErrorHeaderName, string>> = {};
  if (!headers) return output;
  for (const name of PROVIDER_ERROR_HEADER_NAMES) {
    const value = headers.get(name);
    if (value === null) continue;
    output[name] = truncateUtf8(sanitizeProviderText(value), MAX_HEADER_VALUE_BYTES).text;
  }
  return output;
}

function upstreamRequestId(headers?: Headers): string | null {
  if (!headers) return null;
  for (const name of ["x-request-id", "openai-request-id", "request-id"]) {
    const value = headers.get(name)?.trim();
    if (value && SAFE_REQUEST_ID.test(value)) return value;
  }
  return null;
}

async function sha256(value: string): Promise<string | null> {
  try {
    const digest = await crypto.subtle.digest("SHA-256", ENCODER.encode(value));
    return Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join("");
  } catch {
    // Correlation metadata must never replace an otherwise definitive provider
    // error with an internal transport failure.
    return null;
  }
}

function organizationFrom(value: unknown, message: string): string | null {
  const problem = rateLimitProblemFromError(value);
  if (problem?.organization) return problem.organization;
  return /\bin organization\s+([A-Za-z0-9][A-Za-z0-9_-]{0,127})\b/i.exec(message)?.[1] ?? null;
}

function modelFrom(value: unknown, requested: unknown): string | null {
  if (typeof requested === "string" && requested.length > 0) {
    return truncateUtf8(sanitizeProviderText(requested), MAX_MODEL_BYTES).text;
  }
  const root = record(value);
  const nested = record(root?.error);
  const explicit = nested?.model ?? root?.model;
  if (typeof explicit === "string" && explicit.length > 0) {
    return truncateUtf8(sanitizeProviderText(explicit), MAX_MODEL_BYTES).text;
  }
  const message = typeof nested?.message === "string"
    ? nested.message
    : typeof root?.message === "string"
      ? root.message
      : "";
  return /\brate limit(?:ed)?\s+(?:reached\s+)?for\s+([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\b/i
    .exec(message)?.[1] ?? null;
}

export async function createProviderErrorEnvelope(
  input: CreateProviderErrorEnvelopeInput,
): Promise<ProviderErrorEnvelope> {
  const root = record(input.value);
  const nested = record(root?.error);
  const rateLimit = rateLimitProblemFromError(input.value);
  const status = statusCode(input.status) ??
    statusCode(nested?.status) ??
    statusCode(root?.status) ??
    rateLimit?.status ??
    500;
  const fallback = input.fallbackMessage ?? `Upstream Responses request failed (${status})`;
  const error = extractErrorShape(input.value, status, fallback);
  const organization = organizationFrom(input.value, error.message);
  const envelope: ProviderErrorEnvelope = {
    version: 1 as const,
    origin: input.origin,
    status,
    error: Object.freeze(error),
    headers: Object.freeze(selectedHeaders(input.headers)),
    upstreamRequestId: upstreamRequestId(input.headers),
    model: modelFrom(input.value, input.model),
    organizationFingerprint: organization ? await sha256(organization) : null,
    payload: Object.freeze(safePayload(input.payload ?? input.value)),
  };
  // The payload is duplicated as a JSON string inside the reliable frame, so
  // quoting can make its wire size larger than its UTF-8 source size. Shrink
  // only the diagnostic payload until the complete envelope has safe framing
  // headroom; the authoritative error fields remain exact and independently
  // bounded.
  while (jsonUtf8Length(envelope) > PROVIDER_ERROR_ENVELOPE_MAX_BYTES) {
    const currentBytes = utf8Length(envelope.payload.text);
    if (currentBytes === 0) break;
    const excess = jsonUtf8Length(envelope) - PROVIDER_ERROR_ENVELOPE_MAX_BYTES;
    const target = Math.max(0, currentBytes - Math.max(64, excess));
    const bounded = truncateUtf8(envelope.payload.text, target);
    envelope.payload = Object.freeze({
      ...envelope.payload,
      text: bounded.text,
      truncated: true,
    });
  }
  return Object.freeze(envelope);
}

function validBoundedText(value: unknown, maxBytes: number): value is string {
  return typeof value === "string" && utf8Length(value) <= maxBytes;
}

/** Strictly validates an envelope received from another process. */
export function parseProviderErrorEnvelope(value: unknown): ProviderErrorEnvelope | null {
  const root = record(value);
  const error = record(root?.error);
  const headers = record(root?.headers);
  const payload = record(root?.payload);
  if (
    !root ||
    Object.keys(root).some(key => ![
      "version", "origin", "status", "error", "headers", "upstreamRequestId",
      "model", "organizationFingerprint", "payload",
    ].includes(key)) ||
    root?.version !== 1 ||
    (root.origin !== "http" && root.origin !== "sse") ||
    statusCode(root.status) === null ||
    !error || !headers || !payload ||
    !validBoundedText(error.message, PROVIDER_ERROR_MESSAGE_MAX_BYTES) ||
    !validBoundedText(error.type, MAX_CATEGORY_BYTES) ||
    !(error.param === null || validBoundedText(error.param, MAX_PARAM_BYTES)) ||
    !(error.code === null || validBoundedText(error.code, MAX_CATEGORY_BYTES)) ||
    !(root.upstreamRequestId === null || (
      typeof root.upstreamRequestId === "string" && SAFE_REQUEST_ID.test(root.upstreamRequestId)
    )) ||
    !(root.model === null || validBoundedText(root.model, MAX_MODEL_BYTES)) ||
    !(root.organizationFingerprint === null || (
      typeof root.organizationFingerprint === "string" && /^[a-f0-9]{64}$/.test(root.organizationFingerprint)
    )) ||
    (payload.format !== "json" && payload.format !== "text") ||
    !validBoundedText(payload.text, PROVIDER_ERROR_PAYLOAD_MAX_BYTES) ||
    typeof payload.truncated !== "boolean" ||
    jsonUtf8Length(root) > PROVIDER_ERROR_ENVELOPE_MAX_BYTES
  ) return null;
  if (
    Object.keys(error).some(key => !["message", "type", "param", "code"].includes(key)) ||
    Object.keys(payload).some(key => !["format", "text", "truncated"].includes(key)) ||
    sanitizeProviderText(error.message as string) !== error.message ||
    sanitizeProviderText(error.type as string) !== error.type ||
    (typeof error.param === "string" && sanitizeProviderText(error.param) !== error.param) ||
    (typeof error.code === "string" && sanitizeProviderText(error.code) !== error.code) ||
    sanitizeProviderText(payload.text as string) !== payload.text
  ) return null;
  for (const [name, headerValue] of Object.entries(headers)) {
    if (
      !HEADER_NAMES.has(name) ||
      !validBoundedText(headerValue, MAX_HEADER_VALUE_BYTES) ||
      sanitizeProviderText(headerValue) !== headerValue
    ) return null;
  }
  return value as ProviderErrorEnvelope;
}

/** Finds the bounded envelope on direct, relay, or V2 committed errors. */
export function providerErrorEnvelopeFrom(value: unknown): ProviderErrorEnvelope | null {
  const root = record(value);
  if (!root) return parseProviderErrorEnvelope(value);
  const candidates = [
    value,
    root.providerError,
    root.provider_error,
    root.details,
    record(root.details)?.providerError,
    record(root.details)?.provider_error,
    root.cause,
    record(root.cause)?.providerError,
    record(root.cause)?.provider_error,
  ];
  for (const candidate of candidates) {
    const envelope = parseProviderErrorEnvelope(candidate);
    if (envelope) return envelope;
  }
  return null;
}

export function providerErrorPayloadJson(
  envelope: ProviderErrorEnvelope,
): Record<string, unknown> | null {
  if (envelope.payload.format !== "json" || envelope.payload.truncated) return null;
  try {
    return record(JSON.parse(envelope.payload.text));
  } catch {
    return null;
  }
}
