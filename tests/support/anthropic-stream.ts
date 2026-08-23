import type { SseFrame } from "../../src/relay/sse";
import {
  AnthropicStreamTranslator,
  type AnthropicSseEvent,
} from "../../src/anthropic/stream";
import { DEFAULT_RESPONSES_USAGE } from "./anthropic-response";

export type FinalOutput = Record<string, unknown>;
export type FinalResponse = Record<string, unknown> & { output: FinalOutput[] };

export function streamFrame(
  type: string,
  fields: Record<string, unknown> = {},
  sequenceNumber = 1,
): SseFrame {
  return {
    event: type,
    data: JSON.stringify({ type, sequence_number: sequenceNumber, ...fields }),
  };
}

function withSequence(value: SseFrame, sequenceNumber: number): SseFrame {
  return {
    event: value.event,
    data: JSON.stringify({ ...JSON.parse(value.data), sequence_number: sequenceNumber }),
  };
}

export function finalResponse(
  id: string,
  output: FinalOutput[],
  overrides: Record<string, unknown> = {},
): FinalResponse {
  return {
    id,
    object: "response",
    created_at: 1,
    model: "gpt-5.6-terra",
    status: "completed",
    error: null,
    incomplete_details: null,
    output,
    usage: DEFAULT_RESPONSES_USAGE,
    ...overrides,
  } as FinalResponse;
}

export function reasoningOutput(
  id: string,
  text: string,
  encryptedContent = `${id}-cipher`,
): FinalOutput {
  return {
    type: "reasoning",
    id,
    status: "completed",
    encrypted_content: encryptedContent,
    summary: text ? [{ type: "summary_text", text }] : [],
  };
}

export function messageOutput(
  id: string,
  parts: Array<Record<string, unknown>>,
  status: "completed" | "incomplete" = "completed",
): FinalOutput {
  return {
    type: "message",
    id,
    status,
    role: "assistant",
    content: parts.map(part => part.type === "output_text"
      ? { annotations: [], logprobs: [], ...part }
      : part),
  };
}

export function textPart(text: string): Record<string, unknown> {
  return { type: "output_text", text };
}

export function refusalPart(refusal: string): Record<string, unknown> {
  return { type: "refusal", refusal };
}

export function toolOutput(
  outputId: string,
  callId: string,
  name: string,
  args: string,
  status: "completed" | "incomplete" = "completed",
): FinalOutput {
  return {
    type: "function_call",
    id: outputId,
    status,
    call_id: callId,
    name,
    arguments: args,
  };
}

export function collectStreamEvents(
  translator: AnthropicStreamTranslator,
  frames: SseFrame[],
): AnthropicSseEvent[] {
  const events = [...translator.start()];
  for (let index = 0; index < frames.length; index += 1) {
    events.push(...translator.push(withSequence(frames[index], index + 1)));
  }
  return events;
}

export function streamDeltas(
  events: readonly AnthropicSseEvent[],
  type: string,
  field: string,
): string[] {
  return events.flatMap(event => {
    if (event.type !== "content_block_delta" || event.delta.type !== type) return [];
    const value = (event.delta as Record<string, unknown>)[field];
    return typeof value === "string" ? [value] : [];
  });
}
