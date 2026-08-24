import { AnthropicRequestError } from "./errors";
import {
  DEFAULT_ANTHROPIC_REQUEST_LIMITS,
  measureBoundedJson,
  requireArray,
  requireRecord,
} from "./request-validation";

const BYTES_PER_TOKEN = 3.5;
const MESSAGE_OVERHEAD_TOKENS = 4;
const CONTENT_BLOCK_OVERHEAD_TOKENS = 3;
const SYSTEM_OVERHEAD_TOKENS = 4;
const TOOL_OVERHEAD_TOKENS = 12;
const MEDIA_OVERHEAD_TOKENS = 85;
const MAX_MEDIA_TOKENS = 1_000_000;

type VisitFrame = { value: unknown };

function utf8Bytes(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

function scalarBytes(value: string | number | boolean | null): number {
  if (typeof value === "string") return utf8Bytes(value);
  if (value === null) return 4;
  if (typeof value === "boolean") return value ? 4 : 5;
  return Object.is(value, -0) ? 1 : String(value).length;
}

function base64Value(code: number): boolean {
  return (code >= 0x41 && code <= 0x5a) ||
    (code >= 0x61 && code <= 0x7a) ||
    (code >= 0x30 && code <= 0x39) ||
    code === 0x2b || code === 0x2f;
}

function decodedBase64Bytes(value: string): number {
  if (!value.length || value.length % 4 !== 0) {
    throw new AnthropicRequestError(400, "base64 media data must be canonical base64");
  }
  const padding = value.endsWith("==") ? 2 : value.endsWith("=") ? 1 : 0;
  const contentEnd = value.length - padding;
  for (let index = 0; index < contentEnd; index += 1) {
    if (!base64Value(value.charCodeAt(index))) {
      throw new AnthropicRequestError(400, "base64 media data must be canonical base64");
    }
  }
  for (let index = contentEnd; index < value.length; index += 1) {
    if (value.charCodeAt(index) !== 0x3d) {
      throw new AnthropicRequestError(400, "base64 media data must be canonical base64");
    }
  }
  return (value.length / 4) * 3 - padding;
}

function fixedOverhead(value: unknown): number {
  if (!value || typeof value !== "object" || Array.isArray(value)) return 0;
  const request = value as Record<string, unknown>;
  let overhead = 0;

  if (Array.isArray(request.messages)) {
    overhead += request.messages.length * MESSAGE_OVERHEAD_TOKENS;
    for (const messageValue of request.messages) {
      if (!messageValue || typeof messageValue !== "object" || Array.isArray(messageValue)) continue;
      const content = (messageValue as Record<string, unknown>).content;
      overhead += (Array.isArray(content) ? content.length : 1) * CONTENT_BLOCK_OVERHEAD_TOKENS;
    }
  }
  if (request.system !== undefined) {
    overhead += SYSTEM_OVERHEAD_TOKENS;
    if (Array.isArray(request.system)) {
      overhead += request.system.length * CONTENT_BLOCK_OVERHEAD_TOKENS;
    }
  }
  if (Array.isArray(request.tools)) {
    overhead += request.tools.length * TOOL_OVERHEAD_TOKENS;
  }
  return overhead;
}

/**
 * Deterministic, allocation-bounded estimate for Anthropic input payloads.
 * Base64 media is charged from its decoded length without constructing decoded bytes.
 */
export function estimateAnthropicInputTokens(value: unknown): number {
  const limits = DEFAULT_ANTHROPIC_REQUEST_LIMITS;
  measureBoundedJson(value, "inbound request", limits.inboundBytes, limits);

  const stack: VisitFrame[] = [{ value }];
  let jsonBytes = 0;
  let mediaTokens = 0;

  while (stack.length) {
    const current = stack.pop()!.value;
    if (current === null || typeof current === "string" ||
      typeof current === "number" || typeof current === "boolean") {
      jsonBytes += scalarBytes(current as string | number | boolean | null);
      continue;
    }

    if (Array.isArray(current)) {
      jsonBytes += 2 + Math.max(0, current.length - 1);
      for (let index = current.length - 1; index >= 0; index -= 1) {
        stack.push({ value: current[index] });
      }
      continue;
    }

    const record = current as Record<string, unknown>;
    const keys = Object.keys(record);
    const base64Data = record.type === "base64" && typeof record.data === "string"
      ? record.data
      : null;
    jsonBytes += 2 + Math.max(0, keys.length - 1);
    for (let index = keys.length - 1; index >= 0; index -= 1) {
      const key = keys[index];
      jsonBytes += utf8Bytes(key) + 1;
      if (key === "data" && base64Data !== null) {
        const decodedBytes = decodedBase64Bytes(base64Data);
        mediaTokens += Math.min(
          MAX_MEDIA_TOKENS,
          MEDIA_OVERHEAD_TOKENS + Math.ceil(decodedBytes / BYTES_PER_TOKEN),
        );
      } else {
        stack.push({ value: record[key] });
      }
    }
  }

  return Math.max(
    1,
    Math.ceil(jsonBytes / BYTES_PER_TOKEN) + mediaTokens + fixedOverhead(value),
  );
}

export function validateAnthropicCountTokenRequest(value: unknown): void {
  const request = requireRecord(value, "request");
  if (typeof request.model !== "string" || !request.model.trim()) {
    throw new AnthropicRequestError(400, "model is required");
  }
  if (!Array.isArray(request.messages)) {
    throw new AnthropicRequestError(400, "messages is required");
  }
  requireArray(request.messages, "messages", DEFAULT_ANTHROPIC_REQUEST_LIMITS);
}
