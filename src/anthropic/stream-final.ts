import type {
  ResponsesFinalFunctionCallItem,
  ResponsesFinalMessageItem,
  ResponsesFinalReasoningItem,
} from "./response-types";
import {
  invalidUpstream,
  validateTerminalOutputItemProjection,
  validateTerminalResponseProjection,
} from "./response-validation";
import type {
  AnthropicAdapterMessage,
  AnthropicResponseContentBlock,
} from "./types";

export type FinalStreamDescriptor = {
  kind: "thinking" | "text" | "tool_use";
  itemId: string;
  outputIndex: number;
  contentIndex: number | null;
  block: AnthropicResponseContentBlock;
  arguments: string | null;
  reasoningSummary: readonly string[] | null;
};

function isCompletedTool(item: ResponsesFinalFunctionCallItem): boolean {
  return item.status !== "incomplete";
}

function containsRefusal(items: readonly (
  | ResponsesFinalReasoningItem
  | ResponsesFinalMessageItem
  | ResponsesFinalFunctionCallItem
)[]): boolean {
  return items.some(item => item.type === "message" &&
    item.content.some(part => part.type === "refusal"));
}

export function finalStreamDescriptors(
  finalValue: unknown,
  converted: AnthropicAdapterMessage,
  maxTokensZero: boolean,
): FinalStreamDescriptor[] {
  if (maxTokensZero) return [];

  const terminal = validateTerminalResponseProjection(finalValue);
  const items = terminal.output.map(value =>
    validateTerminalOutputItemProjection(value, terminal.status)
  );
  const suppressTools = terminal.incompleteReason === "content_filter" || containsRefusal(items);
  const candidates: Omit<FinalStreamDescriptor, "block">[] = [];

  for (let outputIndex = 0; outputIndex < items.length; outputIndex += 1) {
    const item = items[outputIndex];
    if (item.type === "reasoning") {
      if (typeof item.encrypted_content === "string") {
        candidates.push({
          kind: "thinking",
          itemId: item.id,
          outputIndex,
          contentIndex: null,
          arguments: null,
          reasoningSummary: item.summary.map(part => part.text),
        });
      }
      continue;
    }
    if (item.type === "message") {
      for (let contentIndex = 0; contentIndex < item.content.length; contentIndex += 1) {
        candidates.push({
          kind: "text",
          itemId: item.id,
          outputIndex,
          contentIndex,
          arguments: null,
          reasoningSummary: null,
        });
      }
      continue;
    }
    if (!suppressTools && isCompletedTool(item)) {
      candidates.push({
        kind: "tool_use",
        itemId: item.id ?? `@${outputIndex}`,
        outputIndex,
        contentIndex: null,
        arguments: item.arguments.trim().length > 0 ? item.arguments : "{}",
        reasoningSummary: null,
      });
    }
  }

  const descriptors: FinalStreamDescriptor[] = [];
  for (let index = 0; index < converted.content.length; index += 1) {
    const block = converted.content[index];
    const candidate = candidates[index];
    if (!candidate || candidate.kind !== block.type) {
      throw invalidUpstream("Responses relay returned a malformed final response.");
    }
    descriptors.push({ ...candidate, block });
  }
  return descriptors;
}

export function terminalForStreamItem(item: Record<string, unknown>): Record<string, unknown> {
  const incomplete = item.status === "incomplete";
  return {
    id: "stream_item",
    object: "response",
    created_at: 0,
    model: "stream",
    status: incomplete ? "incomplete" : "completed",
    error: null,
    incomplete_details: null,
    output: [item],
    usage: {
      input_tokens: 0,
      output_tokens: 0,
      input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
      output_tokens_details: { reasoning_tokens: 0 },
      total_tokens: 0,
    },
  };
}
