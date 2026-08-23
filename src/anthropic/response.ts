import { AnthropicRequestError } from "./errors";
import { encodeReasoningItem, type ResponsesReasoningItem } from "./reasoning";
import type { TranslatedAnthropicRequest } from "./request";
import type { AnthropicContentBlock, AnthropicMessage, AnthropicUsage } from "./types";

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
};

type TerminalResponse = {
  record: Record<string, unknown>;
  output: unknown[];
  status: "completed" | "incomplete";
  incompleteReason: string | null;
};

type StopMatch = {
  sequence: string;
  textIndex: number;
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

  if (final.id !== undefined && final.id !== null &&
    (typeof final.id !== "string" || final.id.length === 0)) {
    malformedFinal();
  }
  if (final.status !== "completed" && final.status !== "incomplete") {
    throw invalidUpstream("Responses relay did not return a successful terminal response.");
  }
  if (!Array.isArray(final.output)) malformedFinal();

  let incompleteReason: string | null = null;
  if (final.incomplete_details !== undefined && final.incomplete_details !== null) {
    const details = record(final.incomplete_details);
    if (!details || (details.reason !== undefined && typeof details.reason !== "string")) {
      malformedFinal();
    }
    incompleteReason = typeof details.reason === "string" ? details.reason : null;
  }

  return {
    record: final,
    output: final.output,
    status: final.status,
    incompleteReason,
  };
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
): AnthropicUsage {
  const estimate = tokenCount(estimatedInputTokens);
  if (usageValue === undefined || usageValue === null) {
    return {
      input_tokens: estimate,
      output_tokens: 0,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: 0,
    };
  }

  const usage = record(usageValue);
  if (!usage) throw invalidUpstream("Responses relay returned malformed usage.");
  const inputTokens = tokenCount(usage.input_tokens);
  const outputTokens = tokenCount(usage.output_tokens);
  let cachedTokens = 0;
  if (usage.input_tokens_details !== undefined && usage.input_tokens_details !== null) {
    const details = record(usage.input_tokens_details);
    if (!details) throw invalidUpstream("Responses relay returned malformed usage.");
    if (details.cached_tokens !== undefined) cachedTokens = tokenCount(details.cached_tokens);
  }
  if (cachedTokens > inputTokens) {
    throw invalidUpstream("Responses relay returned malformed usage.");
  }

  return {
    input_tokens: inputTokens - cachedTokens,
    output_tokens: outputTokens,
    cache_creation_input_tokens: null,
    cache_read_input_tokens: cachedTokens,
  };
}

function reasoningBlock(
  item: Record<string, unknown>,
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
  return {
    type: "thinking",
    thinking: display === "omitted" ? "" : summaryText.join(""),
    signature: encodeReasoningItem(reasoning),
  };
}

function appendMessageBlocks(
  item: Record<string, unknown>,
  blocks: AnthropicContentBlock[],
): boolean {
  if (item.role !== "assistant" || !Array.isArray(item.content)) malformedFinal();

  let sawRefusal = false;
  for (const partValue of item.content) {
    const part = record(partValue);
    if (!part) malformedFinal();
    if (part.type === "output_text") {
      if (typeof part.text !== "string") malformedFinal();
      blocks.push({ type: "text", text: part.text, citations: null });
      continue;
    }
    if (part.type === "refusal") {
      if (typeof part.refusal !== "string") malformedFinal();
      blocks.push({ type: "text", text: part.refusal, citations: null });
      sawRefusal = true;
      continue;
    }
    malformedFinal();
  }
  return sawRefusal;
}

function functionCallBlock(item: Record<string, unknown>): AnthropicContentBlock {
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

  return {
    type: "tool_use",
    id: item.call_id,
    name: item.name,
    input,
    caller: { type: "direct" },
  };
}

function findStopMatch(
  blocks: readonly AnthropicContentBlock[],
  stopSequences: readonly string[],
): StopMatch | null {
  const text = blocks
    .filter(block => block.type === "text")
    .map(block => String(block.text))
    .join("");
  let match: StopMatch | null = null;
  for (const sequence of stopSequences) {
    const textIndex = text.indexOf(sequence);
    if (textIndex >= 0 && (match === null || textIndex < match.textIndex)) {
      match = { sequence, textIndex };
    }
  }
  return match;
}

function truncateAtStop(
  blocks: readonly AnthropicContentBlock[],
  match: StopMatch,
): AnthropicContentBlock[] {
  if (match.sequence.length === 0) return [];

  const result: AnthropicContentBlock[] = [];
  let textOffset = 0;
  for (const block of blocks) {
    if (block.type !== "text") {
      result.push(block);
      continue;
    }

    const text = String(block.text);
    const textEnd = textOffset + text.length;
    if (match.textIndex >= textEnd) {
      result.push(block);
      textOffset = textEnd;
      continue;
    }

    const visibleLength = match.textIndex - textOffset;
    if (visibleLength > 0) result.push({ ...block, text: text.slice(0, visibleLength) });
    break;
  }
  return result;
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
  if (matchedStop !== null) {
    return hasToolUse(blocks) ? "tool_use" : "stop_sequence";
  }
  if (terminal.status === "incomplete") {
    return terminal.incompleteReason === "content_filter" ? "refusal" : "max_tokens";
  }
  if (sawRefusal) return "refusal";
  if (hasToolUse(blocks)) return "tool_use";
  return "end_turn";
}

function finalContainsRefusal(output: readonly unknown[]): boolean {
  for (const itemValue of output) {
    const item = record(itemValue);
    if (!item) malformedFinal();
    if (item.type !== "message") continue;
    if (!Array.isArray(item.content)) malformedFinal();
    for (const partValue of item.content) {
      const part = record(partValue);
      if (!part) malformedFinal();
      if (part.type === "refusal") return true;
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
  const converted: AnthropicContentBlock[] = [];
  let sawRefusal = false;

  for (const itemValue of terminal.output) {
    const item = record(itemValue);
    if (!item || typeof item.type !== "string") malformedFinal();
    if (item.type === "reasoning") {
      converted.push(reasoningBlock(item, context.thinkingDisplay));
    } else if (item.type === "message") {
      sawRefusal = appendMessageBlocks(item, converted) || sawRefusal;
    } else if (item.type === "function_call") {
      converted.push(functionCallBlock(item));
    } else {
      malformedFinal();
    }
  }

  const match = context.maxTokensZero
    ? null
    : findStopMatch(converted, context.stopSequences);
  const content = context.maxTokensZero
    ? []
    : match
      ? truncateAtStop(converted, match)
      : converted;
  const stopReason = resolveStopReason(
    terminal,
    content,
    match?.sequence ?? null,
    sawRefusal,
  );
  const usage = usageFromResponses(terminal.record.usage, context.estimatedInputTokens);
  const id = terminal.record.id === undefined || terminal.record.id === null
    ? context.requestId
    : terminal.record.id as string;

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
    usage: context.maxTokensZero ? { ...usage, output_tokens: 0 } : usage,
  };
}
