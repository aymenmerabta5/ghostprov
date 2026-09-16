import {
  createProviderErrorEnvelope,
  parseProviderErrorEnvelope,
  PROVIDER_ERROR_ENVELOPE_MAX_BYTES,
  PROVIDER_ERROR_HEADER_NAMES,
  PROVIDER_ERROR_MESSAGE_MAX_BYTES,
  PROVIDER_ERROR_PAYLOAD_MAX_BYTES,
  sanitizeProviderText,
  type ProviderErrorEnvelope,
} from "../provider-error";
import { UpstreamExecutionError } from "./upstream";

const ENCODER = new TextEncoder();
const DECODER = new TextDecoder();
const REDACTED = "[REDACTED]";

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function bounded(value: unknown, bytes: number): value is string {
  return typeof value === "string" && ENCODER.encode(value).byteLength <= bytes;
}

function hasOnlyTextNormalizationFailure(value: ProviderErrorEnvelope): boolean {
  if (!record(value) || !record(value.error) || !record(value.headers) || !record(value.payload)) return false;
  const error = value.error;
  if (!bounded(error.message, PROVIDER_ERROR_MESSAGE_MAX_BYTES) || !bounded(error.type, 128) ||
    !(error.param === null || bounded(error.param, 512)) || !(error.code === null || bounded(error.code, 128)) ||
    !bounded(value.payload.text, PROVIDER_ERROR_PAYLOAD_MAX_BYTES)) return false;
  const headers = Object.entries(value.headers);
  if (headers.length > PROVIDER_ERROR_HEADER_NAMES.length || headers.some(([, value]) => !bounded(value, 128))) return false;
  // Clear only text subject to re-sanitization. The frozen parser still checks
  // every key, type, origin, status, metadata identity, header name and flag.
  const structural = parseProviderErrorEnvelope({
    ...value,
    error: { ...error, message: "", type: "", param: error.param === null ? null : "", code: error.code === null ? null : "" },
    headers: Object.fromEntries(headers.map(([name]) => [name, ""])),
    payload: { ...value.payload, text: "" },
  });
  return structural !== null && ENCODER.encode(JSON.stringify(value)).byteLength <= PROVIDER_ERROR_ENVELOPE_MAX_BYTES;
}

function boundedStableText(value: string, maxBytes: number, fallback: string): string {
  let text = value;
  // These fields are already bounded by the frozen upstream parser. Limit
  // normalization passes as well as bytes; unexpected forms use a safe label.
  for (let pass = 0; pass < 4; pass++) {
    const sanitized = sanitizeProviderText(text);
    const bytes = ENCODER.encode(sanitized);
    let end = Math.min(bytes.byteLength, maxBytes);
    while (end > 0 && end < bytes.byteLength && (bytes[end] & 0xc0) === 0x80) end--;
    let prefix = DECODER.decode(bytes.subarray(0, end));
    const markerStart = prefix.lastIndexOf("[");
    if (markerStart >= 0 && sanitized.startsWith(REDACTED, markerStart) &&
      prefix.length < markerStart + REDACTED.length) prefix = prefix.slice(0, markerStart);
    if (sanitizeProviderText(prefix) === prefix) return prefix;
    text = prefix;
  }
  return fallback;
}

async function repairProviderErrorEnvelope(
  original: ProviderErrorEnvelope,
  expectedStatus?: number,
): Promise<ProviderErrorEnvelope> {
  if (parseProviderErrorEnvelope(original)) return original;
  if (!hasOnlyTextNormalizationFailure(original) || (expectedStatus !== undefined && original.status !== expectedStatus)) return original;
  const error = original.error;
  const fields = {
    message: boundedStableText(error.message, PROVIDER_ERROR_MESSAGE_MAX_BYTES, "Upstream provider request failed"),
    type: boundedStableText(error.type, 128, "upstream_error"),
    param: error.param === null ? null : boundedStableText(error.param, 512, ""),
    code: error.code === null ? null : boundedStableText(error.code, 128, "upstream_error"),
  };
  const created = await createProviderErrorEnvelope({
    origin: original.origin,
    status: original.status,
    value: { error: fields },
    payload: "",
  });
  const repaired: ProviderErrorEnvelope = Object.freeze({
    ...created,
    headers: Object.freeze(Object.fromEntries(Object.entries(original.headers).map(([name, value]) => [
      name, boundedStableText(value!, 128, ""),
    ]))),
    upstreamRequestId: original.upstreamRequestId,
    model: original.model,
    organizationFingerprint: original.organizationFingerprint,
    // Only the optional diagnostic copy is discarded. A malformed JSON escape
    // or clipped redaction marker cannot replace the authoritative rejection.
    payload: Object.freeze({ format: "text" as const, text: "", truncated: true }),
  });
  // Unknown structural corruption must retain the existing fail-closed path.
  return parseProviderErrorEnvelope(repaired) ? repaired : original;
}

/** Repair only parser-produced diagnostic formatting, using the frozen wire validator. */
export async function normalizeR10ProviderErrorEnvelope(
  original: ProviderErrorEnvelope,
  expectedStatus?: number,
): Promise<ProviderErrorEnvelope> {
  try {
    return await repairProviderErrorEnvelope(original, expectedStatus);
  } catch {
    // Unexpected evidence must reach the original journal's fail-closed path,
    // rather than escaping the engine's error handler during optional repair.
    return original;
  }
}

/** Keep R1-R9's journal and provider parser byte-for-byte frozen. */
export async function normalizeR10ProviderError(
  original: UpstreamExecutionError,
): Promise<UpstreamExecutionError> {
  if (original.ambiguous) return original;
  // Match the frozen journal's summary normalization, then trim the cutoff so
  // its canonical validation is idempotent. Existing stored records are never
  // changed, including previously valid summaries with partial markers.
  const message = original.message.trim()
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\b(?:Bearer|Basic|Digest)\s+[^\s,;]+/gi, match => `${match.split(/\s+/, 1)[0]} [REDACTED]`)
    .replace(/(\b(?:authorization|cookie|set-cookie|api[_-]?key|secret|password|access[_-]?token)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]")
    .replace(/[A-Za-z0-9+/_=-]{48,}/g, "[REDACTED]")
    .trim().slice(0, 512).trim() || "Upstream provider request failed";
  const providerError = original.providerError
    ? await normalizeR10ProviderErrorEnvelope(original.providerError, original.status)
    : undefined;
  if (message === original.message && providerError === original.providerError) return original;
  return new UpstreamExecutionError(
    message, original.code, original.ambiguous, original.status, original.rateLimit, providerError,
  );
}
