import {
  MAX_ANTHROPIC_STOP_SEQUENCES,
  MAX_ANTHROPIC_STOP_SEQUENCE_LENGTH,
} from "./request-options";
import type {
  AnthropicAdapterStopReason,
  AnthropicContentBlock,
  AnthropicResponseContentBlock,
} from "./types";
import {
  finalContainsRefusal,
  invalidUpstream,
  terminalResponse,
  type TerminalResponse,
} from "./response-validation";

export type StopMatch = {
  sequence: string;
  blockIndex: number;
  blockOffset: number;
};

export function findStopMatch(
  ordinaryText: readonly (string | null)[],
  stopBlockIndexes: readonly number[],
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
          return {
            sequence,
            blockIndex: stopBlockIndexes[blockIndex],
            blockOffset: remaining,
          };
        }
        remaining -= text.length;
      }
    }
    segmentStart = segmentEnd + 1;
  }
  return null;
}

export function truncateAtStop(
  blocks: readonly AnthropicResponseContentBlock[],
  match: StopMatch,
): AnthropicResponseContentBlock[] {
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

export function responseStopSequences(value: unknown): string[] {
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

export function resolveStopReason(
  terminal: Pick<TerminalResponse, "status" | "incompleteReason">,
  blocks: readonly AnthropicContentBlock[],
  matchedStop: string | null,
  sawRefusal: boolean,
): AnthropicAdapterStopReason {
  if (sawRefusal || terminal.incompleteReason === "content_filter") return "refusal";
  if (matchedStop !== null) return hasToolUse(blocks) ? "tool_use" : "stop_sequence";
  if (terminal.status === "incomplete") return "max_tokens";
  if (hasToolUse(blocks)) return "tool_use";
  return "end_turn";
}

export function stopReasonFromResponses(
  finalValue: unknown,
  blocks: readonly AnthropicContentBlock[],
  matchedStop: string | null,
): AnthropicAdapterStopReason {
  const terminal = terminalResponse(finalValue);
  return resolveStopReason(
    terminal,
    blocks,
    matchedStop,
    finalContainsRefusal(terminal.output, terminal.status),
  );
}
