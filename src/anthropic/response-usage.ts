import type { AnthropicUsage } from "./types";
import { invalidUpstream, record } from "./response-validation";

function tokenCount(value: unknown): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }
  return Number(value);
}

function serviceTier(value: unknown): AnthropicUsage["service_tier"] {
  if (value === "auto" || value === "default") return "standard";
  if (value === "priority" || value === "fast") return "priority";
  if (value === "batch") return "batch";
  return null;
}

export function usageFromResponses(
  usageValue: unknown,
  estimatedInputTokens: number,
  serviceTierValue?: unknown,
  estimatedOutputTokens = 0,
): AnthropicUsage {
  const estimate = tokenCount(estimatedInputTokens);
  const outputEstimate = tokenCount(estimatedOutputTokens);
  const currentDetails = {
    cache_creation: null,
    inference_geo: null,
    iterations: null,
    server_tool_use: null,
    service_tier: serviceTier(serviceTierValue),
    speed: null,
  } as const;
  if (usageValue === undefined || usageValue === null) {
    return {
      input_tokens: estimate,
      output_tokens: outputEstimate,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      ...currentDetails,
      output_tokens_details: null,
    };
  }

  const usage = record(usageValue);
  if (!usage) throw invalidUpstream("Responses relay returned malformed usage.");
  const inputTokens = tokenCount(usage.input_tokens);
  const outputTokens = tokenCount(usage.output_tokens);
  const inputDetails = record(usage.input_tokens_details);
  if (!inputDetails || inputDetails.cached_tokens === undefined ||
    inputDetails.cache_write_tokens === undefined) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }
  const cacheReadTokens = tokenCount(inputDetails.cached_tokens);
  const cacheWriteTokens = tokenCount(inputDetails.cache_write_tokens);
  if (cacheReadTokens + cacheWriteTokens > inputTokens) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }

  const outputDetails = record(usage.output_tokens_details);
  if (!outputDetails || outputDetails.reasoning_tokens === undefined) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }
  const reasoningTokens = tokenCount(outputDetails.reasoning_tokens);
  if (reasoningTokens > outputTokens) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }
  if (usage.total_tokens === undefined ||
    tokenCount(usage.total_tokens) !== inputTokens + outputTokens) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }

  return {
    input_tokens: inputTokens - cacheReadTokens - cacheWriteTokens,
    output_tokens: outputTokens,
    cache_creation_input_tokens: cacheWriteTokens,
    cache_read_input_tokens: cacheReadTokens,
    ...currentDetails,
    output_tokens_details: { thinking_tokens: reasoningTokens },
  };
}
