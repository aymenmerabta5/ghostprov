import { encodeReasoningItem, type ResponsesReasoningItem } from "./reasoning";
import type { TranslatedAnthropicRequest } from "./request";
import {
  measureBoundedJson,
  resolveAnthropicRequestLimits,
  type AnthropicRequestLimits,
} from "./request-validation";
import {
  findStopMatch,
  resolveStopReason,
  responseStopSequences,
  stopReasonFromResponses,
  truncateAtStop,
} from "./response-stop";
import type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalMessageItem,
  ResponsesFinalReasoningItem,
} from "./response-types";
import {
  invalidUpstream,
  malformedFinal,
  record,
  validateTerminalOutputItemProjection,
  validateTerminalResponseProjection,
} from "./response-validation";
import { usageFromResponses } from "./response-usage";
import type { AnthropicStreamLimits } from "./stream-limits";
import { assertUnicodeWellFormed } from "./stream-stop-filter";
import type {
  AnthropicAdapterMessage,
  AnthropicContextManagementResponse,
  AnthropicResponseContentBlock,
  AnthropicThinkingBlock,
  AnthropicToolUseBlock,
} from "./types";

export type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalItemStatus,
  ResponsesFinalMessageItem,
  ResponsesFinalOutputItem,
  ResponsesFinalReasoningItem,
  ResponsesIncompleteReason,
  ResponsesLogprob,
  ResponsesOutputRefusal,
  ResponsesOutputText,
  ResponsesReasoningSummary,
  ResponsesUsage,
  TerminalResponseProjection,
} from "./response-types";
export type {
  AnthropicAdapterMessage,
  AnthropicAdapterStopReason,
  AnthropicStopReason,
} from "./types";
export { stopReasonFromResponses, usageFromResponses };

export type AnthropicResponseContext = Pick<
  TranslatedAnthropicRequest,
  | "requestedModel"
  | "thinkingDisplay"
  | "stopSequences"
  | "estimatedInputTokens"
  | "maxTokensZero"
> & {
  requestId: string;
  beta?: boolean;
  contextManagement?: AnthropicContextManagementResponse | null;
  limits?: Partial<AnthropicRequestLimits>;
  streamLimits?: Partial<AnthropicStreamLimits>;
};

function betaResponseFields(context: AnthropicResponseContext): {
  context_management?: AnthropicContextManagementResponse | null;
} {
  return context.beta
    ? { context_management: context.contextManagement ?? null }
    : {};
}

function reasoningBlock(
  item: ResponsesFinalReasoningItem,
  encryptedContent: string,
  display: AnthropicResponseContext["thinkingDisplay"],
): AnthropicThinkingBlock {
  const summaryText: string[] = [];
  for (const summaryValue of item.summary) {
    const summary = record(summaryValue);
    if (!summary || summary.type !== "summary_text" || typeof summary.text !== "string") {
      malformedFinal();
    }
    assertUnicodeWellFormed(summary.text as string);
    summaryText.push(summary.text);
  }

  const reasoning: ResponsesReasoningItem = {
    type: "reasoning",
    id: item.id,
    encrypted_content: encryptedContent,
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
  blocks: AnthropicResponseContentBlock[],
  ordinaryText: Array<string | null>,
  stopBlockIndexes: number[],
): boolean {
  let sawRefusal = false;
  for (const part of item.content) {
    if (part.type === "output_text") {
      assertUnicodeWellFormed(part.text);
      blocks.push({ type: "text", text: part.text, citations: null });
      ordinaryText.push(part.text);
      stopBlockIndexes.push(blocks.length - 1);
    } else {
      assertUnicodeWellFormed(part.refusal);
      blocks.push({ type: "text", text: part.refusal, citations: null });
      ordinaryText.push(null);
      stopBlockIndexes.push(blocks.length - 1);
      sawRefusal = true;
    }
  }
  return sawRefusal;
}

function functionCallBlock(
  item: ResponsesFinalFunctionCallItem,
  limits: AnthropicRequestLimits,
): AnthropicToolUseBlock {
  assertUnicodeWellFormed(item.arguments);
  if (Buffer.byteLength(item.arguments, "utf8") > limits.outputBytes) {
    throw invalidUpstream("Responses relay returned invalid function call arguments.");
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
    measureBoundedJson(input, "function call arguments", limits.outputBytes, limits);
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

function estimatedOutputTokens(
  blocks: readonly AnthropicResponseContentBlock[],
  limits: AnthropicRequestLimits,
): number {
  let bytes = 0;
  const add = (value: string): void => {
    bytes += Buffer.byteLength(value, "utf8");
    if (!Number.isSafeInteger(bytes) || bytes > limits.relayBytes) {
      throw invalidUpstream("Responses relay returned malformed usage.");
    }
  };
  for (const block of blocks) {
    if (block.type === "text") add(block.text);
    else if (block.type === "thinking") add(block.thinking);
    else {
      add(block.name);
      try {
        add(JSON.stringify(block.input));
      } catch {
        throw invalidUpstream("Responses relay returned malformed usage.");
      }
    }
  }
  return Math.ceil(bytes / 4);
}

export function responseToAnthropic(
  finalValue: unknown,
  context: AnthropicResponseContext,
): AnthropicAdapterMessage {
  const terminal = validateTerminalResponseProjection(finalValue);
  const id = terminal.record.id as string;

  if (context.maxTokensZero) {
    if (terminal.record.usage === null) {
      throw invalidUpstream("Responses relay returned malformed usage.");
    }
    const usage = usageFromResponses(
      terminal.record.usage,
      context.estimatedInputTokens,
      terminal.record.service_tier,
    );
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
      ...betaResponseFields(context),
      usage: {
        ...usage,
        output_tokens: 0,
        output_tokens_details: usage.output_tokens_details === null
          ? null
          : { thinking_tokens: 0 },
      },
    };
  }

  let limits: AnthropicRequestLimits;
  try {
    limits = resolveAnthropicRequestLimits(context.limits);
  } catch {
    throw invalidUpstream("Anthropic response context is malformed.");
  }
  const stopSequences = responseStopSequences(context.stopSequences);
  const items = terminal.output.map(
    item => validateTerminalOutputItemProjection(item, terminal.status),
  );
  const suppressTools = terminal.incompleteReason === "content_filter" || items.some(item =>
    item.type === "message" && item.content.some(part => part.type === "refusal")
  );

  const converted: AnthropicResponseContentBlock[] = [];
  const ordinaryText: Array<string | null> = [];
  const stopBlockIndexes: number[] = [];
  const callIds = new Set<string>();
  let sawRefusal = false;

  for (const item of items) {
    if (item.type === "reasoning") {
      if (typeof item.encrypted_content !== "string") {
        ordinaryText.push(null);
        stopBlockIndexes.push(converted.length);
        continue;
      }
      converted.push(reasoningBlock(item, item.encrypted_content, context.thinkingDisplay));
      ordinaryText.push(null);
      stopBlockIndexes.push(converted.length - 1);
    } else if (item.type === "message") {
      sawRefusal = appendMessageBlocks(
        item,
        converted,
        ordinaryText,
        stopBlockIndexes,
      ) || sawRefusal;
    } else {
      if (callIds.has(item.call_id)) {
        throw invalidUpstream("Responses relay returned malformed function calls.");
      }
      callIds.add(item.call_id);
      if (item.status === "incomplete" || suppressTools) {
        ordinaryText.push(null);
        stopBlockIndexes.push(converted.length);
        continue;
      }
      converted.push(functionCallBlock(item, limits));
      ordinaryText.push(null);
      stopBlockIndexes.push(converted.length - 1);
    }
  }

  const refusal = sawRefusal || terminal.incompleteReason === "content_filter";
  const match = refusal
    ? null
    : findStopMatch(ordinaryText, stopBlockIndexes, stopSequences);
  const content = match ? truncateAtStop(converted, match) : converted;
  const stopReason = resolveStopReason(
    terminal,
    content,
    match?.sequence ?? null,
    sawRefusal,
  );
  const usage = usageFromResponses(
    terminal.record.usage,
    context.estimatedInputTokens,
    terminal.record.service_tier,
    estimatedOutputTokens(content, limits),
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
    ...betaResponseFields(context),
    usage,
  };
}
