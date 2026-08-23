import type { AnthropicErrorEnvelope } from "./errors";
import type {
  AnthropicAdapterMessage,
  AnthropicAdapterStopReason,
  AnthropicResponseContentBlock,
  AnthropicUsage,
} from "./types";

export type AnthropicMessageStartEvent = {
  type: "message_start";
  message: Omit<AnthropicAdapterMessage, "content" | "stop_reason" | "stop_sequence"> & {
    content: [];
    stop_reason: null;
    stop_sequence: null;
  };
};

export type AnthropicContentBlockStartEvent = {
  type: "content_block_start";
  index: number;
  content_block: AnthropicResponseContentBlock;
};

export type AnthropicContentBlockDelta =
  | { type: "text_delta"; text: string }
  | { type: "thinking_delta"; thinking: string }
  | { type: "signature_delta"; signature: string }
  | { type: "input_json_delta"; partial_json: string };

export type AnthropicContentBlockDeltaEvent = {
  type: "content_block_delta";
  index: number;
  delta: AnthropicContentBlockDelta;
};

export type AnthropicContentBlockStopEvent = {
  type: "content_block_stop";
  index: number;
};

export type AnthropicMessageDeltaEvent = {
  type: "message_delta";
  delta: {
    stop_reason: AnthropicAdapterStopReason;
    stop_sequence: string | null;
  };
  usage: AnthropicUsage;
};

export type AnthropicMessageStopEvent = { type: "message_stop" };

export type AnthropicSseEvent =
  | AnthropicMessageStartEvent
  | AnthropicContentBlockStartEvent
  | AnthropicContentBlockDeltaEvent
  | AnthropicContentBlockStopEvent
  | AnthropicMessageDeltaEvent
  | AnthropicMessageStopEvent
  | AnthropicErrorEnvelope;

/** Format one canonical Anthropic event. Anthropic streams do not use `[DONE]`. */
export function anthropicSseFrame(event: AnthropicSseEvent): string {
  return `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;
}
