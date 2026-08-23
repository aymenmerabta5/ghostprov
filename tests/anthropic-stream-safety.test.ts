import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import { ApiError } from "../src/http/errors";
import { AnthropicRequestError } from "../src/anthropic/errors";
import {
  AnthropicStreamTranslator,
  type AnthropicSseEvent,
} from "../src/anthropic/stream";
import {
  CONTEXT,
  DEFAULT_RESPONSES_USAGE,
  context,
} from "./support/anthropic-response";

function frame(
  sequenceNumber: number,
  type: string,
  fields: Record<string, unknown> = {},
): SseFrame {
  return {
    event: type,
    data: JSON.stringify({ type, sequence_number: sequenceNumber, ...fields }),
  };
}

type FinalResponse = Record<string, unknown> & { output: Record<string, unknown>[] };

function finalResponse(
  output: Record<string, unknown>[],
  overrides: Record<string, unknown> = {},
): FinalResponse {
  return {
    id: "resp_safety",
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

function message(id: string, text: string): Record<string, unknown> {
  return {
    type: "message",
    id,
    status: "completed",
    role: "assistant",
    content: [{ type: "output_text", text, annotations: [], logprobs: [] }],
  };
}

function reasoning(id: string, summary: string): Record<string, unknown> {
  return {
    type: "reasoning",
    id,
    status: "completed",
    encrypted_content: `${id}-cipher`,
    summary: [{ type: "summary_text", text: summary }],
  };
}

function textDeltas(events: readonly AnthropicSseEvent[]): string[] {
  return events.flatMap(event => event.type === "content_block_delta" &&
    event.delta.type === "text_delta" ? [event.delta.text] : []);
}

function thinkingDeltas(events: readonly AnthropicSseEvent[]): string[] {
  return events.flatMap(event => event.type === "content_block_delta" &&
    event.delta.type === "thinking_delta" ? [event.delta.thinking] : []);
}

function hasLoneSurrogate(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return true;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return true;
    }
  }
  return false;
}

describe("Anthropic stream reasoning privacy", () => {
  it("validates but never exposes raw reasoning_text events as summary thinking", () => {
    const final = finalResponse([reasoning("rs_private", "visible summary")]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events: AnthropicSseEvent[] = [...translator.start()];
    for (const value of [
      frame(1, "response.reasoning_text.delta", {
        item_id: "rs_private",
        output_index: 0,
        content_index: 0,
        delta: "private raw chain",
      }),
      frame(2, "response.reasoning_text.done", {
        item_id: "rs_private",
        output_index: 0,
        content_index: 0,
        text: "private raw chain",
      }),
      frame(3, "response.reasoning_summary_text.delta", {
        item_id: "rs_private",
        output_index: 0,
        summary_index: 0,
        delta: "visible summary",
      }),
      frame(4, "response.output_item.done", { output_index: 0, item: final.output[0] }),
      frame(5, "response.completed", { response: final }),
    ]) events.push(...translator.push(value));

    expect(thinkingDeltas(events)).toEqual(["visible summary"]);
    expect(JSON.stringify(events)).not.toContain("private raw chain");
    expect(translator.snapshot().reasoningBytes).toBe(
      Buffer.byteLength("private raw chainvisible summary", "utf8"),
    );
  });

  it("rejects malformed raw reasoning completion payloads", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    expect(translator.push(frame(1, "response.reasoning_text.done", {
      item_id: "rs_bad_raw",
      output_index: 0,
      content_index: 0,
      text: { private: true },
    }))).toMatchObject([{ type: "error", error: { type: "api_error" } }]);
  });
});

describe("Anthropic stream Unicode-safe stop filtering", () => {
  it("validates stop-sequence length in Unicode code points", () => {
    const stop = "🚀".repeat(4_096);
    expect(() => new AnthropicStreamTranslator(context({ stopSequences: [stop] })))
      .not.toThrow();
  });

  it("combines an astral character split across deltas before emitting text", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events: AnthropicSseEvent[] = [...translator.start()];
    events.push(...translator.push(frame(1, "response.output_text.delta", {
      item_id: "msg_astral",
      output_index: 0,
      content_index: 0,
      delta: "\ud83d",
    })));
    expect(textDeltas(events)).toEqual([]);
    events.push(...translator.push(frame(2, "response.output_text.delta", {
      item_id: "msg_astral",
      output_index: 0,
      content_index: 0,
      delta: "\ude80",
    })));
    events.push(...translator.finish(finalResponse([message("msg_astral", "🚀")])));

    expect(textDeltas(events)).toEqual(["🚀"]);
    expect(textDeltas(events).some(hasLoneSurrogate)).toBe(false);
  });

  it("tracks retained stop suffixes in code points rather than UTF-16 units", () => {
    const translator = new AnthropicStreamTranslator(context({
      stopSequences: ["🚀X"],
    }));
    translator.start();
    const events = translator.push(frame(1, "response.output_text.delta", {
      item_id: "msg_suffix_points",
      output_index: 0,
      content_index: 0,
      delta: "a🚀",
    }));

    expect(textDeltas(events)).toEqual(["a"]);
    expect(translator.snapshot().retainedTextCodePoints).toBe(1);
  });

  it("emits no empty block for a one-code-point stop at offset zero", () => {
    const final = finalResponse([message("msg_stop_zero", "Xafter")]);
    const translator = new AnthropicStreamTranslator(context({ stopSequences: ["X"] }));
    const events: AnthropicSseEvent[] = [...translator.start()];
    events.push(...translator.push(frame(1, "response.content_part.added", {
      item_id: "msg_stop_zero",
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text: "", annotations: [] },
    })));
    events.push(...translator.push(frame(2, "response.output_text.delta", {
      item_id: "msg_stop_zero",
      output_index: 0,
      content_index: 0,
      delta: "Xafter",
    })));
    events.push(...translator.push(frame(3, "response.completed", { response: final })));

    expect(events.map(event => event.type)).toEqual([
      "message_start",
      "message_delta",
      "message_stop",
    ]);
    expect(events.at(-2)).toMatchObject({
      delta: { stop_reason: "stop_sequence", stop_sequence: "X" },
    });
  });
});

describe("Anthropic stream error privacy", () => {
  it("redacts ApiError and AnthropicRequestError 5xx messages", () => {
    for (const error of [
      new ApiError(501, "secret-token upstream body", { type: "server_error" }),
      new AnthropicRequestError(500, "secret encrypted reasoning", "api_error"),
    ]) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      const events = translator.fail(error);
      expect(events).toEqual([{
        type: "error",
        error: { type: "api_error", message: "Responses stream failed." },
        request_id: CONTEXT.requestId,
      }]);
      expect(JSON.stringify(events)).not.toContain("secret");
    }

    const rateLimited = new AnthropicStreamTranslator(CONTEXT);
    rateLimited.start();
    const rateEvents = rateLimited.fail(new ApiError(429, "secret rate-limit body"));
    expect(rateEvents).toEqual([{
      type: "error",
      error: { type: "rate_limit_error", message: "Responses stream failed." },
      request_id: CONTEXT.requestId,
    }]);
    expect(JSON.stringify(rateEvents)).not.toContain("secret");
  });

  it("preserves only trusted client validation messages", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    expect(translator.fail(new ApiError(400, "client field is invalid"))).toEqual([{
      type: "error",
      error: { type: "invalid_request_error", message: "client field is invalid" },
      request_id: CONTEXT.requestId,
    }]);
  });
});

describe("Anthropic stream nullable terminal usage", () => {
  it("emits estimated cumulative usage for an incomplete null-usage terminal", () => {
    const terminal = finalResponse([message("msg_null_usage", "12345678")], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      usage: null,
    });
    (terminal.output as Array<Record<string, unknown>>)[0].status = "incomplete";
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = [...translator.start(), ...translator.finish(terminal)];

    expect(events.at(-2)).toMatchObject({
      type: "message_delta",
      delta: { stop_reason: "max_tokens" },
      usage: {
        input_tokens: CONTEXT.estimatedInputTokens,
        output_tokens: 2,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        output_tokens_details: null,
      },
    });
    expect(events.at(-1)).toEqual({ type: "message_stop" });
  });
});
