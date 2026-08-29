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

function reasoningParts(id: string, summaries: string[]): Record<string, unknown> {
  return {
    type: "reasoning",
    id,
    status: "completed",
    encrypted_content: `${id}-cipher`,
    summary: summaries.map(text => ({ type: "summary_text", text })),
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
    const signature = events.find(event =>
      event.type === "content_block_delta" && event.delta.type === "signature_delta");
    const signatureBytes = signature?.type === "content_block_delta" &&
        signature.delta.type === "signature_delta"
      ? Buffer.byteLength(signature.delta.signature, "utf8")
      : 0;
    expect(signatureBytes).toBeGreaterThan(0);
    expect(translator.snapshot().reasoningBytes).toBe(
      Buffer.byteLength("private raw chainvisible summary", "utf8") + signatureBytes,
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

  it("combines a reasoning-summary astral scalar split across delta frames", () => {
    const final = finalResponse([reasoning("rs_split_summary", "🚀")]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events: AnthropicSseEvent[] = [...translator.start()];
    events.push(...translator.push(frame(0, "response.reasoning_summary_text.delta", {
      item_id: "rs_split_summary",
      output_index: 0,
      summary_index: 0,
      delta: "\ud83d",
    })));
    expect(thinkingDeltas(events)).toEqual([]);
    expect(translator.snapshot().retainedReasoningCodePoints).toBe(1);
    events.push(...translator.push(frame(1, "response.reasoning_summary_text.delta", {
      item_id: "rs_split_summary",
      output_index: 0,
      summary_index: 0,
      delta: "\ude80",
    })));
    events.push(...translator.push(frame(2, "response.output_item.done", {
      output_index: 0,
      item: final.output[0],
    })));
    events.push(...translator.push(frame(3, "response.completed", { response: final })));

    expect(thinkingDeltas(events)).toEqual(["🚀"]);
    expect(thinkingDeltas(events).some(hasLoneSurrogate)).toBe(false);
    expect(translator.snapshot().retainedReasoningCodePoints).toBe(0);
  });

  it("reconciles multiple reasoning summary parts independently in summary order", () => {
    const item = reasoningParts("rs_multi_summary", ["first!", "second!"]);
    const final = finalResponse([item]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events: AnthropicSseEvent[] = [...translator.start()];

    for (const value of [
      frame(0, "response.output_item.added", {
        output_index: 0,
        item: {
          type: "reasoning",
          id: "rs_multi_summary",
          status: "in_progress",
          summary: [],
        },
      }),
      frame(1, "response.reasoning_summary_text.delta", {
        item_id: "rs_multi_summary",
        output_index: 0,
        summary_index: 0,
        delta: "first",
      }),
      frame(2, "response.reasoning_summary_text.done", {
        item_id: "rs_multi_summary",
        output_index: 0,
        summary_index: 0,
        text: "first!",
      }),
      frame(3, "response.reasoning_summary_text.delta", {
        item_id: "rs_multi_summary",
        output_index: 0,
        summary_index: 1,
        delta: "second",
      }),
      frame(4, "response.reasoning_summary_text.done", {
        item_id: "rs_multi_summary",
        output_index: 0,
        summary_index: 1,
        text: "second!",
      }),
      frame(5, "response.output_item.done", { output_index: 0, item }),
      frame(6, "response.completed", { response: final }),
    ]) events.push(...translator.push(value));

    expect(thinkingDeltas(events)).toEqual(["first", "!", "second", "!"]);
    expect(events.some(event => event.type === "error")).toBe(false);
    expect(events.at(-1)).toEqual({ type: "message_stop" });
  });

  it("keeps split Unicode scalar accumulators isolated by summary index", () => {
    const item = reasoningParts("rs_multi_scalar", ["🚀", "🌟"]);
    const final = finalResponse([item]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events: AnthropicSseEvent[] = [...translator.start()];

    events.push(...translator.push(frame(0, "response.reasoning_summary_text.delta", {
      item_id: "rs_multi_scalar",
      output_index: 0,
      summary_index: 0,
      delta: "\ud83d",
    })));
    events.push(...translator.push(frame(1, "response.reasoning_summary_text.delta", {
      item_id: "rs_multi_scalar",
      output_index: 0,
      summary_index: 1,
      delta: "\ud83c",
    })));
    expect(translator.snapshot().retainedReasoningCodePoints).toBe(2);

    for (const value of [
      frame(2, "response.reasoning_summary_text.delta", {
        item_id: "rs_multi_scalar",
        output_index: 0,
        summary_index: 0,
        delta: "\ude80",
      }),
      frame(3, "response.reasoning_summary_text.done", {
        item_id: "rs_multi_scalar",
        output_index: 0,
        summary_index: 0,
        text: "🚀",
      }),
      frame(4, "response.reasoning_summary_text.delta", {
        item_id: "rs_multi_scalar",
        output_index: 0,
        summary_index: 1,
        delta: "\udf1f",
      }),
      frame(5, "response.reasoning_summary_text.done", {
        item_id: "rs_multi_scalar",
        output_index: 0,
        summary_index: 1,
        text: "🌟",
      }),
      frame(6, "response.output_item.done", { output_index: 0, item }),
      frame(7, "response.completed", { response: final }),
    ]) events.push(...translator.push(value));

    expect(thinkingDeltas(events)).toEqual(["🚀", "🌟"]);
    expect(events.some(event => event.type === "error")).toBe(false);
    expect(translator.snapshot().retainedReasoningCodePoints).toBe(0);
  });

  it("rejects final reasoning summaries that move streamed units across part boundaries", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(0, "response.reasoning_summary_text.delta", {
      item_id: "rs_shifted_summary",
      output_index: 0,
      summary_index: 0,
      delta: "ab",
    }));
    translator.push(frame(1, "response.reasoning_summary_text.delta", {
      item_id: "rs_shifted_summary",
      output_index: 0,
      summary_index: 1,
      delta: "",
    }));

    expect(translator.finish(finalResponse([
      reasoningParts("rs_shifted_summary", ["a", "b"]),
    ]))).toMatchObject([{ type: "error", error: { type: "api_error" } }]);
  });

  it("still rejects same-length reasoning summary changes across completed and terminal items", () => {
    const completed = reasoningParts("rs_changed_summary", ["same"]);
    const terminal = finalResponse([
      reasoningParts("rs_changed_summary", ["else"]),
    ]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = [...translator.start()];
    events.push(...translator.push(frame(0, "response.output_item.done", {
      output_index: 0,
      item: completed,
    })));
    events.push(...translator.push(frame(1, "response.completed", { response: terminal })));

    expect(events.at(-1)).toMatchObject({ type: "error", error: { type: "api_error" } });
    expect(events.some(event => event.type === "message_stop")).toBe(false);
  });

  it("rejects invalid reasoning-summary surrogates before thinking emission", () => {
    const low = new AnthropicStreamTranslator(CONTEXT);
    low.start();
    const lowEvents = low.push(frame(0, "response.reasoning_summary_text.delta", {
      item_id: "rs_low",
      output_index: 0,
      summary_index: 0,
      delta: "\udc00",
    }));
    expect(lowEvents).toMatchObject([{ type: "error" }]);
    expect(thinkingDeltas(lowEvents)).toEqual([]);

    const high = new AnthropicStreamTranslator(CONTEXT);
    high.start();
    const first = high.push(frame(5, "response.reasoning_summary_text.delta", {
      item_id: "rs_high",
      output_index: 0,
      summary_index: 0,
      delta: "\ud800",
    }));
    expect(thinkingDeltas(first)).toEqual([]);
    const done = high.push(frame(6, "response.reasoning_summary_text.done", {
      item_id: "rs_high",
      output_index: 0,
      summary_index: 0,
      text: "\ud800",
    }));
    expect(done).toMatchObject([{ type: "error" }]);
    expect(thinkingDeltas(done)).toEqual([]);
    expect(high.snapshot().retainedReasoningCodePoints).toBe(0);
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
      part: { type: "output_text", text: "", annotations: [], logprobs: [] },
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

  it("rejects lone surrogate data in authoritative final fallback", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    const events = translator.finish(finalResponse([
      message("msg_bad_fallback_unicode", "\ud800"),
    ]));

    expect(events).toEqual([{
      type: "error",
      error: { type: "api_error", message: "Responses stream failed." },
      request_id: CONTEXT.requestId,
    }]);
    expect(events.some(event => event.type === "message_stop")).toBe(false);
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

  it("redacts every externally supplied fail error, including 400 and 413", () => {
    for (const [error, type] of [
      [new ApiError(400, "secret client field"), "invalid_request_error"],
      [new AnthropicRequestError(413, "secret oversized body", "request_too_large"), "request_too_large"],
    ] as const) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      const events = translator.fail(error);
      expect(events).toEqual([{
        type: "error",
        error: { type, message: "Responses stream failed." },
        request_id: CONTEXT.requestId,
      }]);
      expect(JSON.stringify(events)).not.toContain("secret");
    }
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
