import { AnthropicRequestError } from "./errors";
import {
  decodedCanonicalBase64Bytes,
  DEFAULT_ANTHROPIC_REQUEST_LIMITS,
  measureBoundedJson,
} from "./request-validation";
import { validateAnthropicCountRequest } from "./request";

const BYTES_PER_TOKEN = 3.5;
const MESSAGE_OVERHEAD_TOKENS = 4;
const CONTENT_BLOCK_OVERHEAD_TOKENS = 3;
const SYSTEM_OVERHEAD_TOKENS = 4;
const TOOL_OVERHEAD_TOKENS = 12;
const MEDIA_OVERHEAD_TOKENS = 85;
const MAX_MEDIA_TOKENS = 1_000_000;

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function contentBlocks(value: unknown, stack: unknown[]): void {
  if (!Array.isArray(value)) return;
  for (let index = value.length - 1; index >= 0; index -= 1) stack.push(value[index]);
}

function contextualBase64Media(value: unknown): string[] {
  const request = record(value);
  if (!request || !Array.isArray(request.messages)) return [];
  const stack: unknown[] = [];
  for (const messageValue of request.messages) {
    const message = record(messageValue);
    contentBlocks(message?.content, stack);
  }

  const media: string[] = [];
  while (stack.length) {
    const block = record(stack.pop());
    if (!block || typeof block.type !== "string") continue;
    if (block.type === "tool_result") {
      contentBlocks(block.content, stack);
      continue;
    }
    if (block.type !== "image" && block.type !== "document") continue;
    const source = record(block.source);
    if (!source || typeof source.type !== "string") continue;
    if (source.type === "base64" && typeof source.data === "string") {
      media.push(source.data);
    } else if (block.type === "document" && source.type === "content") {
      contentBlocks(source.content, stack);
    }
  }
  return media;
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
  let jsonBytes = measureBoundedJson(value, "inbound request", limits.inboundBytes, limits);
  let mediaTokens = 0;
  for (const data of contextualBase64Media(value)) {
    const decodedBytes = decodedCanonicalBase64Bytes(data);
    if (decodedBytes === null) {
      throw new AnthropicRequestError(400, "base64 media data must be canonical base64");
    }
    jsonBytes -= data.length + 2;
    mediaTokens += Math.min(
      MAX_MEDIA_TOKENS,
      MEDIA_OVERHEAD_TOKENS + Math.ceil(decodedBytes / BYTES_PER_TOKEN),
    );
  }

  return Math.max(
    1,
    Math.ceil(jsonBytes / BYTES_PER_TOKEN) + mediaTokens + fixedOverhead(value),
  );
}

export function validateAnthropicCountTokenRequest(value: unknown): void {
  validateAnthropicCountRequest(value);
}
