import { AnthropicRequestError } from "./errors";
import { resolveAnthropicModel, type AnthropicModelOptions } from "./model";
import { translateMessages, translateSystem } from "./request-content";
import {
  translateMetadata,
  translateOutputConfig,
  translateSampling,
  translateServiceTier,
  translateStopSequences,
  translateThinking,
  translateToolChoice,
  translateTools,
  type AnthropicThinkingDisplay,
} from "./request-options";
import type { ResponsesInputItem, ResponsesRequestBody } from "./request-types";
import {
  type AnthropicRequestLimits,
  hasOwn,
  measureBoundedJson,
  RequestMediaBudget,
  requireArray,
  requireRecord,
  resolveAnthropicRequestLimits,
} from "./request-validation";

export { DEFAULT_ANTHROPIC_REQUEST_LIMITS } from "./request-validation";
export type { AnthropicRequestLimits } from "./request-validation";
export type { AnthropicThinkingDisplay } from "./request-options";
export type {
  ResponsesFunctionCallInput,
  ResponsesFunctionCallOutput,
  ResponsesFunctionTool,
  ResponsesInputFile,
  ResponsesInputImage,
  ResponsesInputItem,
  ResponsesInputText,
  ResponsesMessageInput,
  ResponsesRequestBody,
  ResponsesUserContent,
} from "./request-types";

export type TranslateAnthropicRequestOptions = {
  models: AnthropicModelOptions;
  limits?: Partial<AnthropicRequestLimits>;
};

export type TranslatedAnthropicRequest = {
  requestedModel: string;
  relayBody: ResponsesRequestBody;
  stream: boolean;
  thinkingDisplay: AnthropicThinkingDisplay;
  stopSequences: string[];
  estimatedInputTokens: number;
  maxTokensZero: boolean;
};

type ValidatedRequest = {
  body: Record<string, unknown>;
  requestedModel: string;
  maxTokens: number;
  messages: unknown[];
  stream: boolean;
};

function requireRequest(value: unknown, limits: AnthropicRequestLimits): ValidatedRequest {
  const body = requireRecord(value, "request");
  if (typeof body.model !== "string" || !body.model.trim()) {
    throw new AnthropicRequestError(400, "model is required");
  }
  if (!Number.isSafeInteger(body.max_tokens) || Number(body.max_tokens) < 0) {
    throw new AnthropicRequestError(400, "max_tokens must be a nonnegative integer");
  }
  if (!Array.isArray(body.messages)) {
    throw new AnthropicRequestError(400, "messages is required");
  }
  const messages = requireArray(body.messages, "messages", limits);
  if (hasOwn(body, "stream") && typeof body.stream !== "boolean") {
    throw new AnthropicRequestError(400, "stream must be a boolean");
  }
  const stream = body.stream === true;
  const maxTokens = Number(body.max_tokens);
  if (maxTokens === 0 && stream) {
    throw new AnthropicRequestError(400, "max_tokens 0 is not supported with stream:true");
  }
  return {
    body,
    requestedModel: body.model,
    maxTokens,
    messages,
    stream,
  };
}

export function translateAnthropicRequest(
  value: unknown,
  options: TranslateAnthropicRequestOptions,
): TranslatedAnthropicRequest {
  const limits = resolveAnthropicRequestLimits(options.limits);
  measureBoundedJson(value, "inbound request", limits.inboundBytes, limits);
  const { body, requestedModel, maxTokens, messages, stream } = requireRequest(value, limits);
  const mediaBudget = new RequestMediaBudget(limits.mediaBytes);
  const input: ResponsesInputItem[] = translateMessages(messages, mediaBudget, limits);
  const instructions = translateSystem(body.system, limits);
  const tools = translateTools(body.tools, limits);
  const declaredToolNames = new Set(tools.map(tool => tool.name));
  const { toolChoice, parallelToolCalls } = translateToolChoice(
    body.tool_choice,
    declaredToolNames,
  );
  const output = translateOutputConfig(body.output_config);
  const thinking = translateThinking(body.thinking, output.effort, maxTokens);
  const stopSequences = translateStopSequences(body.stop_sequences, limits);
  const serviceTier = translateServiceTier(body);
  const sampling = translateSampling(body);
  const privacy = translateMetadata(body.metadata);
  const maxTokensZero = maxTokens === 0;
  if (maxTokensZero && output.text) {
    throw new AnthropicRequestError(400, "max_tokens 0 does not support structured output");
  }
  if (maxTokensZero && (toolChoice === "required" || typeof toolChoice === "object")) {
    throw new AnthropicRequestError(400, "max_tokens 0 does not support forced tool choice");
  }

  const relayBody: ResponsesRequestBody = {
    model: resolveAnthropicModel(requestedModel, options.models),
    store: false,
    stream,
    input,
    include: ["reasoning.encrypted_content"],
    max_output_tokens: maxTokensZero ? 1 : maxTokens,
    ...(instructions ? { instructions } : {}),
    ...(tools.length ? { tools } : {}),
    ...(toolChoice !== undefined ? { tool_choice: toolChoice } : {}),
    ...(parallelToolCalls !== undefined ? { parallel_tool_calls: parallelToolCalls } : {}),
    ...(thinking.reasoning ? { reasoning: thinking.reasoning } : {}),
    ...(output.text ? { text: output.text } : {}),
    service_tier: serviceTier,
    ...(sampling.temperature !== undefined ? { temperature: sampling.temperature } : {}),
    ...(sampling.topP !== undefined ? { top_p: sampling.topP } : {}),
    ...(privacy.safetyIdentifier ? { safety_identifier: privacy.safetyIdentifier } : {}),
    ...(privacy.metadata ? { metadata: privacy.metadata } : {}),
  };
  const relayBytes = measureBoundedJson(relayBody, "relay request", limits.relayBytes, limits);

  return {
    requestedModel,
    relayBody,
    stream,
    thinkingDisplay: thinking.display,
    stopSequences,
    estimatedInputTokens: Math.max(1, Math.ceil(relayBytes / 4)),
    maxTokensZero,
  };
}
