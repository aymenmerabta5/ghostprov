import { expect } from "bun:test";
import { AnthropicRequestError } from "../../src/anthropic/errors";
import type { AnthropicRequestLimits } from "../../src/anthropic/request";
import {
  responseToAnthropic as rawResponseToAnthropic,
  type AnthropicResponseContext,
} from "../../src/anthropic/response";

export const CONTEXT = {
  requestId: "req_test",
  requestedModel: "claude-sonnet-4-6",
  thinkingDisplay: "summarized",
  stopSequences: [],
  estimatedInputTokens: 17,
  maxTokensZero: false,
} satisfies AnthropicResponseContext;

export const NULLABLE_USAGE_FIELDS = {
  cache_creation: null,
  inference_geo: null,
  iterations: null,
  server_tool_use: null,
  service_tier: null,
  speed: null,
  output_tokens_details: null,
} as const;

export const DEFAULT_RESPONSES_USAGE = {
  input_tokens: 17,
  output_tokens: 0,
  input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
  output_tokens_details: { reasoning_tokens: 0 },
  total_tokens: 17,
} as const;

export type ResponseTestLimits = Partial<AnthropicRequestLimits>;

export type ResponseTestContext = Omit<AnthropicResponseContext, "limits"> & {
  limits?: ResponseTestLimits;
};

export function context(overrides: Partial<ResponseTestContext> = {}): ResponseTestContext {
  return { ...CONTEXT, ...overrides };
}

export function convertResponseToAnthropic(
  finalValue: unknown,
  responseContext: ResponseTestContext,
) {
  return rawResponseToAnthropic(finalValue, responseContext);
}

export function responseToAnthropic(
  finalValue: unknown,
  responseContext: ResponseTestContext,
) {
  if (finalValue !== null && typeof finalValue === "object" && !Array.isArray(finalValue)) {
    const candidate = finalValue as Record<string, unknown>;
    const output = Array.isArray(candidate.output)
      ? candidate.output.map(item => {
        if (!item || typeof item !== "object" || Array.isArray(item)) return item;
        const outputItem = item as Record<string, unknown>;
        if (outputItem.type !== "message" || !Array.isArray(outputItem.content)) return item;
        return {
          ...outputItem,
          content: outputItem.content.map(part => {
            if (!part || typeof part !== "object" || Array.isArray(part)) return part;
            const contentPart = part as Record<string, unknown>;
            return contentPart.type === "output_text"
              ? { logprobs: [], ...contentPart }
              : part;
          }),
        };
      })
      : candidate.output;
    return rawResponseToAnthropic({
      object: "response",
      created_at: 1,
      model: "gpt-5.6-terra",
      error: null,
      incomplete_details: null,
      usage: DEFAULT_RESPONSES_USAGE,
      ...candidate,
      output,
    }, responseContext);
  }
  return rawResponseToAnthropic(finalValue, responseContext);
}

export function captureAnthropicError(run: () => unknown): AnthropicRequestError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(AnthropicRequestError);
    return error as AnthropicRequestError;
  }
  throw new Error("expected an AnthropicRequestError");
}
