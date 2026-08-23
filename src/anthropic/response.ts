import { AnthropicRequestError } from "./errors";
import { encodeReasoningItem, type ResponsesReasoningItem } from "./reasoning";
import type { TranslatedAnthropicRequest } from "./request";
import {
  MAX_ANTHROPIC_STOP_SEQUENCES,
  MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH,
} from "./request-options";
import {
  measureBoundedJson,
  resolveAnthropicRequestLimits,
  type AnthropicRequestLimits,
} from "./request-validation";
import type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalMessageItem,
  ResponsesFinalOutputItem,
  ResponsesFinalReasoningItem,
  ResponsesIncompleteReason,
} from "./response-types";
import type { AnthropicContentBlock, AnthropicMessage, AnthropicUsage } from "./types";

export type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalMessageItem,
  ResponsesFinalOutputItem,
  ResponsesFinalReasoningItem,
  ResponsesFinalResponse,
  ResponsesIncompleteReason,
  ResponsesOutputRefusal,
  ResponsesOutputText,
} from "./response-types";

export type AnthropicStopReason =
  | "end_turn"
  | "max_tokens"
  | "stop_sequence"
  | "tool_use"
  | "refusal";

export type AnthropicResponseContext = Pick<
  TranslatedAnthropicRequest,
  | "requestedModel"
  | "thinkingDisplay"
  | "stopSequences"
  | "estimatedInputTokens"
  | "maxTokensZero"
> & {
  requestId: string;
  limits?: Partial<AnthropicRequestLimits>;
};

type TerminalResponse = {
  record: Record<string, unknown>;
  output: unknown[];
  status: "completed" | "incomplete";
  incompleteReason: ResponsesIncompleteReason | null;
};

type StopMatch = {
  sequence: string;
  blockIndex: number;
  blockOffset: number;
};

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function invalidUpstream(message: string): AnthropicRequestError {
  return new AnthropicRequestError(500, message, "api_error");
}

function malformedFinal(): never {
  throw invalidUpstream("Responses relay returned a malformed final response.");
}

function terminalResponse(value: unknown): TerminalResponse {
  const final = record(value);
  if (!final) malformedFinal();

  if (typeof final.id !== "string" || final.id.length === 0) malformedFinal();
  if (final.status !== "completed" && final.status !== "incomplete") {
    throw invalidUpstream("Responses relay did not return a successful terminal response.");
  }
  if (!Array.isArray(final.output)) malformedFinal();

  let incompleteReason: ResponsesIncompleteReason | null = null;
  if (final.status === "incomplete") {
    const details = record(final.incomplete_details);
    if (!details || (details.reason !== "max_output_tokens" && details.reason !== "content_filter")) {
      malformedFinal();
    }
    incompleteReason = details.reason;
  } else if (final.incomplete_details !== undefined && final.incomplete_details !== null) {
    malformedFinal();
  }

  return {
    record: final,
    output: final.output,
    status: final.status,
    incompleteReason,
  };
}

function finalItemStatus(item: Record<string, unknown>): boolean {
  return typeof item.id === "string" && item.id.length > 0 &&
    (item.status === "completed" || item.status === "incomplete");
}

function finalOutputItem(value: unknown): ResponsesFinalOutputItem {
  const item = record(value);
  if (!item || !finalItemStatus(item)) malformedFinal();

  if (item.type === "message") {
    if (item.role !== "assistant" || !Array.isArray(item.content)) malformedFinal();
    for (const partValue of item.content) {
      const part = record(partValue);
      if (!part) malformedFinal();
      if (part.type === "output_text") {
        if (typeof part.text !== "string" || !Array.isArray(part.annotations)) malformedFinal();
      } else if (part.type === "refusal") {
        if (typeof part.refusal !== "string") malformedFinal();
      } else {
        malformedFinal();
      }
    }
    return item as ResponsesFinalMessageItem;
  }

  if (item.type === "reasoning") {
    if (typeof item.encrypted_content !== "string" || !item.encrypted_content ||
      !Array.isArray(item.summary)) {
      malformedFinal();
    }
    for (const summaryValue of item.summary) {
      const summary = record(summaryValue);
      if (!summary || summary.type !== "summary_text" || typeof summary.text !== "string") {
        malformedFinal();
      }
    }
    return item as ResponsesFinalReasoningItem;
  }

  if (item.type === "function_call") {
    if (typeof item.call_id !== "string" || !item.call_id ||
      typeof item.name !== "string" || !item.name || typeof item.arguments !== "string") {
      malformedFinal();
    }
    return item as ResponsesFinalFunctionCallItem;
  }

  malformedFinal();
}

function tokenCount(value: unknown): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }
  return Number(value);
}

export function usageFromResponses(
  usageValue: unknown,
  estimatedInputTokens: number,
  serviceTierValue?: unknown,
): AnthropicUsage {
  const estimate = tokenCount(estimatedInputTokens);
  const serviceTier = (() => {
    if (serviceTierValue === "auto" || serviceTierValue === "default") return "standard";
    if (serviceTierValue === "flex") return "batch";
    if (serviceTierValue === "scale" || serviceTierValue === "priority" ||
      serviceTierValue === "fast" || serviceTierValue === "ultrafast") {
      return "priority";
    }
    return null;
  })();
  const currentDetails = {
    cache_creation: null,
    inference_geo: null,
    iterations: null,
    server_tool_use: null,
    service_tier: serviceTier,
    speed: null,
  } as const;
  if (usageValue === undefined || usageValue === null) {
    return {
      input_tokens: estimate,
      output_tokens: 0,
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

  return {
    input_tokens: inputTokens - cacheReadTokens - cacheWriteTokens,
    output_tokens: outputTokens,
    cache_creation_input_tokens: cacheWriteTokens,
    cache_read_input_tokens: cacheReadTokens,
    ...currentDetails,
    output_tokens_details: { thinking_tokens: reasoningTokens },
  };
}

function reasoningBlock(
  item: ResponsesFinalReasoningItem,
  display: AnthropicResponseContext["thinkingDisplay"],
): AnthropicContentBlock {
  if (typeof item.id !== "string" || item.id.length === 0 ||
    typeof item.encrypted_content !== "string" || item.encrypted_content.length === 0 ||
    !Array.isArray(item.summary)) {
    malformedFinal();
  }

  const summaryText: string[] = [];
  for (const summaryValue of item.summary) {
    const summary = record(summaryValue);
    if (!summary || summary.type !== "summary_text" || typeof summary.text !== "string") {
      malformedFinal();
    }
    summaryText.push(summary.text);
  }

  const reasoning: ResponsesReasoningItem = {
    type: "reasoning",
    id: item.id,
    encrypted_content: item.encrypted_content,
    summary: item.summary,
  };
  let signature: string;
  try {
    signature = encodeReasoningItem(reasoning);
  } catch {
    malformedFinal();
  }
  return {
    type: "thinking",
    thinking: display === "omitted" ? "" : summaryText.join(""),
    signature,
  };
}

function appendMessageBlocks(
  item: ResponsesFinalMessageItem,
  blocks: AnthropicContentBlock[],
  ordinaryText: Array<string | null>,
): boolean {
  if (item.role !== "assistant" || !Array.isArray(item.content)) malformedFinal();

  let sawRefusal = false;
  for (const partValue of item.content) {
    const part = record(partValue);
    if (!part) malformedFinal();
    if (part.type === "output_text") {
      if (typeof part.text !== "string") malformedFinal();
      blocks.push({ type: "text", text: part.text, citations: null });
      ordinaryText.push(part.text);
      continue;
    }
    if (part.type === "refusal") {
      if (typeof part.refusal !== "string") malformedFinal();
      blocks.push({ type: "text", text: part.refusal, citations: null });
      ordinaryText.push(null);
      sawRefusal = true;
      continue;
    }
    malformedFinal();
  }
  return sawRefusal;
}

function functionCallBlock(
  item: ResponsesFinalFunctionCallItem,
  limits: AnthropicRequestLimits,
): AnthropicContentBlock {
  if (typeof item.call_id !== "string" || item.call_id.length === 0 ||
    typeof item.name !== "string" || item.name.length === 0 ||
    typeof item.arguments !== "string") {
    malformedFinal();
  }

  let input: Record<string, unknown> = {};
  if (item.arguments.trim().length > 0) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(item.arguments);
    } catch {
      throw invalidUpstream("Responses relay returned invalid function call arguments.");
    }
    const parsedRecord = record(parsed);
    if (!parsedRecord) {
      throw invalidUpstream("Responses relay returned invalid function call arguments.");
    }
    input = parsedRecord;
  }
  try {
    measureBoundedJson(input, "function call arguments", limits.inboundBytes, limits);
  } catch {
    throw invalidUpstream("Responses relay returned invalid function call arguments.");
  }

  return {
    type: "tool_use",
    id: item.call_id,
    name: item.name,
    input,
    caller: { type: "direct" },
  };
}

function findStopMatch(
  ordinaryText: readonly (string | null)[],
  stopSequences: readonly string[],
): StopMatch | null {
  let segmentStart = 0;
  while (segmentStart < ordinaryText.length) {
    while (segmentStart < ordinaryText.length && ordinaryText[segmentStart] === null) {
      segmentStart += 1;
    }
    if (segmentStart >= ordinaryText.length) break;

    let segmentEnd = segmentStart;
    const parts: string[] = [];
    while (segmentEnd < ordinaryText.length && ordinaryText[segmentEnd] !== null) {
      parts.push(ordinaryText[segmentEnd]!);
      segmentEnd += 1;
    }
    const segment = parts.join("");
    let sequence: string | null = null;
    let segmentOffset = -1;
    for (const candidate of stopSequences) {
      const offset = segment.indexOf(candidate);
      if (offset >= 0 && (segmentOffset < 0 || offset < segmentOffset)) {
        sequence = candidate;
        segmentOffset = offset;
      }
    }
    if (sequence !== null) {
      let remaining = segmentOffset;
      for (let blockIndex = segmentStart; blockIndex < segmentEnd; blockIndex += 1) {
        const text = ordinaryText[blockIndex]!;
        if (remaining < text.length) {
          return { sequence, blockIndex, blockOffset: remaining };
        }
        remaining -= text.length;
      }
    }
    segmentStart = segmentEnd + 1;
  }
  return null;
}

function truncateAtStop(
  blocks: readonly AnthropicContentBlock[],
  match: StopMatch,
): AnthropicContentBlock[] {
  const result = blocks.slice(0, match.blockIndex);
  const matchedBlock = blocks[match.blockIndex];
  if (match.blockOffset > 0) {
    result.push({
      ...matchedBlock,
      text: String(matchedBlock.text).slice(0, match.blockOffset),
    });
  }
  return result;
}

function responseStopSequences(value: unknown): string[] {
  if (!Array.isArray(value) || value.length > MAX_ANTHROPIC_STOP_SEQUENCES ||
    value.some(sequence => typeof sequence !== "string" || sequence.length === 0 ||
      sequence.length > MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH)) {
    throw invalidUpstream("Anthropic response context is malformed.");
  }
  return value as string[];
}

function hasToolUse(blocks: readonly AnthropicContentBlock[]): boolean {
  return blocks.some(block => block.type === "tool_use");
}

function resolveStopReason(
  terminal: Pick<TerminalResponse, "status" | "incompleteReason">,
  blocks: readonly AnthropicContentBlock[],
  matchedStop: string | null,
  sawRefusal: boolean,
): AnthropicStopReason {
  if (sawRefusal || terminal.incompleteReason === "content_filter") return "refusal";
  if (matchedStop !== null) {
    return hasToolUse(blocks) ? "tool_use" : "stop_sequence";
  }
  if (terminal.status === "incomplete") return "max_tokens";
  if (hasToolUse(blocks)) return "tool_use";
  return "end_turn";
}

function finalContainsRefusal(output: readonly unknown[]): boolean {
  for (const itemValue of output) {
    const item = finalOutputItem(itemValue);
    if (item.type !== "message") continue;
    for (const partValue of item.content) {
      if (partValue.type === "refusal") return true;
    }
  }
  return false;
}

export function stopReasonFromResponses(
  finalValue: unknown,
  blocks: readonly AnthropicContentBlock[],
  matchedStop: string | null,
): AnthropicStopReason {
  const terminal = terminalResponse(finalValue);
  return resolveStopReason(
    terminal,
    blocks,
    matchedStop,
    finalContainsRefusal(terminal.output),
  );
}

export function responseToAnthropic(
  finalValue: unknown,
  context: AnthropicResponseContext,
): AnthropicMessage {
  const terminal = terminalResponse(finalValue);
  const usage = usageFromResponses(
    terminal.record.usage,
    context.estimatedInputTokens,
    terminal.record.service_tier,
  );
  const id = terminal.record.id === undefined || terminal.record.id === null
    ? context.requestId
    : terminal.record.id as string;

  if (context.maxTokensZero) {
    return {
      id: `msg_${id}`,
      type: "message",
      role: "assistant",
      model: context.requestedModel,
      content: [],
      stop_details: null,
      stop_reason: "max_tokens",
      stop_sequence: null,
      container: null,
      usage: {
        ...usage,
        output_tokens: 0,
        output_tokens_details: usage.output_tokens_details === null
          ? null
          : { thinking_tokens: 0 },
      },
    };
  }

  const converted: AnthropicContentBlock[] = [];
  const ordinaryText: Array<string | null> = [];
  let sawRefusal = false;
  let limits: AnthropicRequestLimits;
  try {
    limits = resolveAnthropicRequestLimits(context.limits);
  } catch {
    throw invalidUpstream("Anthropic response context is malformed.");
  }
  const stopSequences = responseStopSequences(context.stopSequences);
  const callIds = new Set<string>();

  for (const itemValue of terminal.output) {
    const item = finalOutputItem(itemValue);
    if (item.type === "reasoning") {
      converted.push(reasoningBlock(item, context.thinkingDisplay));
      ordinaryText.push(null);
    } else if (item.type === "message") {
      sawRefusal = appendMessageBlocks(item, converted, ordinaryText) || sawRefusal;
    } else if (item.type === "function_call") {
      const block = functionCallBlock(item, limits);
      const callId = block.id as string;
      if (callIds.has(callId)) {
        throw invalidUpstream("Responses relay returned malformed function calls.");
      }
      callIds.add(callId);
      converted.push(block);
      ordinaryText.push(null);
    } else {
      malformedFinal();
    }
  }

  const refusal = sawRefusal || terminal.incompleteReason === "content_filter";
  const match = refusal ? null : findStopMatch(ordinaryText, stopSequences);
  const content = match ? truncateAtStop(converted, match) : converted;
  const stopReason = resolveStopReason(
    terminal,
    content,
    match?.sequence ?? null,
    sawRefusal,
  );
  return {
    id: `msg_${id}`,
    type: "message",
    role: "assistant",
    model: context.requestedModel,
    content,
    stop_details: null,
    stop_reason: stopReason,
    stop_sequence: stopReason === "stop_sequence" ? match!.sequence : null,
    container: null,
    usage,
  };
}
