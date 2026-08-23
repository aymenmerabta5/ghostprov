import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import {
  AnthropicStreamTranslator,
  DEFAULT_ANTHROPIC_STREAM_LIMITS,
  type AnthropicStreamLimits,
  type AnthropicStreamSnapshot,
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

function finalText(text: string): Record<string, unknown> {
  return {
    id: "resp_bounds",
    object: "response",
    created_at: 1,
    model: "gpt-5.6-terra",
    status: "completed",
    error: null,
    incomplete_details: null,
    output: [{
      type: "message",
      id: "msg_bounds",
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text, annotations: [], logprobs: [] }],
    }],
    usage: DEFAULT_RESPONSES_USAGE,
  };
}

function streamContext(limits: Partial<AnthropicStreamLimits>) {
  return context({ streamLimits: limits });
}

function expectError(events: ReturnType<AnthropicStreamTranslator["push"]>): void {
  expect(events).toMatchObject([{ type: "error", error: { type: "api_error" } }]);
}

function expectNumericSnapshot(snapshot: AnthropicStreamSnapshot): void {
  for (const value of Object.values(snapshot)) {
    expect(typeof value).toBe("number");
    expect(Number.isSafeInteger(value)).toBe(true);
    expect(value).toBeGreaterThanOrEqual(0);
  }
}

describe("Anthropic stream cumulative production bounds", () => {
  it("publishes finite positive production defaults", () => {
    expect(DEFAULT_ANTHROPIC_STREAM_LIMITS).toEqual({
      maxEvents: expect.any(Number),
      maxWireBytes: expect.any(Number),
      maxOutputBlocks: expect.any(Number),
      maxIdentities: expect.any(Number),
      maxStreamedBytes: expect.any(Number),
      maxFunctionBytes: expect.any(Number),
      maxReasoningBytes: expect.any(Number),
      maxTextBytes: expect.any(Number),
      maxPendingChunks: expect.any(Number),
    });
    for (const value of Object.values(DEFAULT_ANTHROPIC_STREAM_LIMITS)) {
      expect(Number.isSafeInteger(value)).toBe(true);
      expect(value).toBeGreaterThan(0);
    }
    expect(DEFAULT_ANTHROPIC_STREAM_LIMITS.maxOutputBlocks).toBeLessThanOrEqual(4_096);
  });

  it("bounds zero-byte event, block, and identity cardinality", () => {
    const events = new AnthropicStreamTranslator(streamContext({ maxEvents: 2 }));
    events.start();
    expect(events.push(frame(1, "response.future_one"))).toEqual([]);
    expect(events.push(frame(2, "response.future_two"))).toEqual([]);
    expectError(events.push(frame(3, "response.future_three")));

    const blocks = new AnthropicStreamTranslator(streamContext({ maxOutputBlocks: 2 }));
    blocks.start();
    expect(blocks.push(frame(1, "response.output_text.delta", {
      item_id: "msg_blocks",
      output_index: 0,
      content_index: 0,
      delta: "",
    })).some(event => event.type === "error")).toBe(false);
    expect(blocks.push(frame(2, "response.output_text.delta", {
      item_id: "msg_blocks",
      output_index: 0,
      content_index: 1,
      delta: "",
    })).some(event => event.type === "error")).toBe(false);
    expectError(blocks.push(frame(3, "response.output_text.delta", {
      item_id: "msg_blocks",
      output_index: 0,
      content_index: 2,
      delta: "",
    })));

    const identities = new AnthropicStreamTranslator(streamContext({ maxIdentities: 2 }));
    identities.start();
    identities.push(frame(1, "response.output_text.delta", {
      item_id: "msg_identities",
      output_index: 0,
      content_index: 0,
      delta: "",
    }));
    expectError(identities.push(frame(2, "response.output_text.delta", {
      item_id: "msg_identities",
      output_index: 0,
      content_index: 1,
      delta: "",
    })));
  });

  it("bounds cumulative UTF-8 category bytes after emitted buffers drain", () => {
    const text = new AnthropicStreamTranslator(streamContext({
      maxStreamedBytes: 3,
      maxTextBytes: 3,
      maxPendingChunks: 10,
    }));
    text.start();
    text.push(frame(1, "response.output_text.delta", {
      item_id: "msg_bytes",
      output_index: 0,
      content_index: 0,
      delta: "ab",
    }));
    expectError(text.push(frame(2, "response.output_text.delta", {
      item_id: "msg_bytes",
      output_index: 0,
      content_index: 0,
      delta: "cd",
    })));

    const reasoning = new AnthropicStreamTranslator(streamContext({
      maxStreamedBytes: 3,
      maxReasoningBytes: 3,
    }));
    reasoning.start();
    expectError(reasoning.push(frame(1, "response.reasoning_summary_text.delta", {
      item_id: "rs_bytes",
      output_index: 0,
      summary_index: 0,
      delta: "🚀",
    })));

    const tools = new AnthropicStreamTranslator(streamContext({
      maxStreamedBytes: 3,
      maxFunctionBytes: 3,
    }));
    tools.start();
    expectError(tools.push(frame(1, "response.function_call_arguments.delta", {
      item_id: "fc_bytes",
      output_index: 0,
      delta: "1234",
    })));
  });

  it("counts every raw wire frame, including unknown and terminal error frames", () => {
    const unknown = frame(0, "response.future_wire", { payload: "abc" });
    const failed = frame(1, "response.failed", {
      response: { error: { message: "private" } },
    });
    const expectedBytes = Buffer.byteLength(unknown.data, "utf8") +
      Buffer.byteLength(failed.data, "utf8");
    const translator = new AnthropicStreamTranslator(streamContext({
      maxWireBytes: expectedBytes,
    }));
    translator.start();
    expect(translator.push(unknown)).toEqual([]);
    expect(translator.push(failed)).toMatchObject([{ type: "error" }]);
    expect(translator.snapshot().wireBytes).toBe(expectedBytes);

    const bounded = new AnthropicStreamTranslator(streamContext({
      maxWireBytes: Buffer.byteLength(unknown.data, "utf8"),
    }));
    bounded.start();
    bounded.push(unknown);
    expectError(bounded.push(failed));
  });

  it("counts only unseen done-event suffix bytes and never double-counts deltas", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(0, "response.output_text.delta", {
      item_id: "msg_bounds",
      output_index: 0,
      content_index: 0,
      delta: "ab",
    }));
    translator.push(frame(1, "response.output_text.done", {
      item_id: "msg_bounds",
      output_index: 0,
      content_index: 0,
      text: "abcd",
    }));
    expect(translator.snapshot()).toMatchObject({
      textBytes: 4,
      streamedBytes: 4,
    });
    translator.finish(finalText("abcd"));
    expect(translator.snapshot()).toMatchObject({
      textBytes: 4,
      streamedBytes: 4,
    });

    for (const [deltaType, doneType] of [
      ["response.reasoning_summary_text.delta", "response.reasoning_summary_text.done"],
      ["response.reasoning_text.delta", "response.reasoning_text.done"],
    ] as const) {
      const reasoning = new AnthropicStreamTranslator(CONTEXT);
      reasoning.start();
      reasoning.push(frame(0, deltaType, {
        item_id: "rs_done_bytes",
        output_index: 0,
        ...(deltaType.includes("summary") ? { summary_index: 0 } : { content_index: 0 }),
        delta: "ab",
      }));
      reasoning.push(frame(1, doneType, {
        item_id: "rs_done_bytes",
        output_index: 0,
        ...(doneType.includes("summary") ? { summary_index: 0 } : { content_index: 0 }),
        text: "abcd",
      }));
      expect(reasoning.snapshot()).toMatchObject({
        reasoningBytes: 4,
        streamedBytes: 4,
      });
    }

    const blankTool = new AnthropicStreamTranslator(CONTEXT);
    blankTool.start();
    blankTool.push(frame(0, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call",
        id: "fc_blank_bytes",
        status: "in_progress",
        call_id: "call_blank_bytes",
        name: "Read",
        arguments: "",
      },
    }));
    blankTool.push(frame(1, "response.function_call_arguments.done", {
      item_id: "fc_blank_bytes",
      output_index: 0,
      name: "Read",
      arguments: "",
    }));
    expect(blankTool.snapshot()).toMatchObject({
      functionBytes: 2,
      streamedBytes: 2,
    });
  });

  it("accounts final-only text, reasoning, and function data", () => {
    const final = {
      ...finalText("text"),
      output: [
        {
          type: "reasoning",
          id: "rs_final_bytes",
          status: "completed",
          encrypted_content: "cipher",
          summary: [{ type: "summary_text", text: "think" }],
        },
        {
          type: "message",
          id: "msg_bounds",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "text", annotations: [], logprobs: [] }],
        },
        {
          type: "function_call",
          id: "fc_final_bytes",
          status: "completed",
          call_id: "call_final_bytes",
          name: "Read",
          arguments: "{\"x\":1}",
        },
      ],
    };
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.finish(final);
    expect(translator.snapshot()).toMatchObject({
      textBytes: 4,
      reasoningBytes: 5,
      functionBytes: 7,
      streamedBytes: 16,
    });
  });

  it("enforces category limits for authoritative terminal-only output", () => {
    const translator = new AnthropicStreamTranslator(streamContext({
      maxTextBytes: 3,
      maxStreamedBytes: 3,
    }));
    translator.start();
    const events = translator.finish(finalText("four"));
    expectError(events);
    expect(events.some(event => event.type === "message_stop")).toBe(false);
  });

  it("bounds cumulative pending chunk history even after chunks are emitted", () => {
    const translator = new AnthropicStreamTranslator(streamContext({ maxPendingChunks: 2 }));
    translator.start();
    for (let sequence = 1; sequence <= 2; sequence += 1) {
      expect(translator.push(frame(sequence, "response.output_text.delta", {
        item_id: "msg_chunks",
        output_index: 0,
        content_index: 0,
        delta: "x",
      })).some(event => event.type === "error")).toBe(false);
    }
    expectError(translator.push(frame(3, "response.output_text.delta", {
      item_id: "msg_chunks",
      output_index: 0,
      content_index: 0,
      delta: "x",
    })));
  });

  it("does not retain a 10,000-delta transcript or duplicate final fallback", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    let emittedCharacters = 0;
    for (let sequence = 1; sequence <= 10_000; sequence += 1) {
      const events = translator.push(frame(sequence, "response.output_text.delta", {
        item_id: "msg_bounds",
        output_index: 0,
        content_index: 0,
        delta: "x",
      }));
      emittedCharacters += events.flatMap(event =>
        event.type === "content_block_delta" && event.delta.type === "text_delta"
          ? [event.delta.text.length]
          : []
      ).reduce((sum, length) => sum + length, 0);
    }
    expect(emittedCharacters).toBe(10_000);
    expect(translator.snapshot()).toMatchObject({
      events: 10_000,
      streamedBytes: 10_000,
      retainedPendingChunks: 0,
      retainedPendingBytes: 0,
      retainedTextCodePoints: 0,
    });
    const terminal = translator.finish(finalText("x".repeat(10_000)));
    expect(terminal.some(event =>
      event.type === "content_block_delta" && event.delta.type === "text_delta"
    )).toBe(false);
  });

  it("bounds aggregate currently pending bytes across interleaved blocks", () => {
    const translator = new AnthropicStreamTranslator(context({
      limits: { outputBytes: 32, relayBytes: 10 },
    }));
    translator.start();
    translator.push(frame(1, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call",
        id: "fc_pending_1",
        status: "in_progress",
        call_id: "call_pending_1",
        name: "One",
        arguments: "",
      },
    }));
    translator.push(frame(2, "response.function_call_arguments.delta", {
      item_id: "fc_pending_1",
      output_index: 0,
      delta: "123456",
    }));
    translator.push(frame(3, "response.output_item.added", {
      output_index: 1,
      item: {
        type: "function_call",
        id: "fc_pending_2",
        status: "in_progress",
        call_id: "call_pending_2",
        name: "Two",
        arguments: "",
      },
    }));
    expectError(translator.push(frame(4, "response.function_call_arguments.delta", {
      item_id: "fc_pending_2",
      output_index: 1,
      delta: "abcdef",
    })));
  });
});

describe("Anthropic stream sanitized snapshots", () => {
  it("reports cumulative counts and clears all retained state on success", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(1, "response.output_text.delta", {
      item_id: "msg_bounds",
      output_index: 0,
      content_index: 0,
      delta: "x",
    }));

    const active = translator.snapshot();
    expectNumericSnapshot(active);
    expect(active).toMatchObject({
      events: 1,
      outputBlocks: 1,
      identities: 2,
      streamedBytes: 1,
      wireBytes: expect.any(Number),
      textBytes: 1,
      reasoningBytes: 0,
      functionBytes: 0,
      pendingChunks: 1,
      retainedBlocks: 1,
      retainedIdentities: 2,
      retainedPendingChunks: 0,
      retainedPendingBytes: 0,
    });

    translator.finish(finalText("x"));
    const terminal = translator.snapshot();
    expectNumericSnapshot(terminal);
    expect(terminal).toMatchObject({
      events: 1,
      outputBlocks: 1,
      identities: 2,
      retainedBlocks: 0,
      retainedIdentities: 0,
      retainedPendingChunks: 0,
      retainedPendingBytes: 0,
      retainedTextCodePoints: 0,
    });
  });

  it("clears retained state on error and explicit failure", () => {
    const errored = new AnthropicStreamTranslator(streamContext({ maxEvents: 1 }));
    errored.start();
    errored.push(frame(1, "response.output_text.delta", {
      item_id: "msg_error_cleanup",
      output_index: 0,
      content_index: 0,
      delta: "x",
    }));
    expectError(errored.push(frame(2, "response.future_over_limit")));
    expect(errored.snapshot()).toMatchObject({
      retainedBlocks: 0,
      retainedIdentities: 0,
      retainedPendingChunks: 0,
      retainedPendingBytes: 0,
      retainedTextCodePoints: 0,
    });

    const failed = new AnthropicStreamTranslator(CONTEXT);
    failed.start();
    failed.push(frame(1, "response.output_text.delta", {
      item_id: "msg_fail_cleanup",
      output_index: 0,
      content_index: 0,
      delta: "x",
    }));
    failed.fail(new Error("cancel-equivalent"));
    expect(failed.snapshot()).toMatchObject({
      retainedBlocks: 0,
      retainedIdentities: 0,
      retainedPendingChunks: 0,
      retainedPendingBytes: 0,
      retainedTextCodePoints: 0,
    });
  });
});
