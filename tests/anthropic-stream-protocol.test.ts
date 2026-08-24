import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import {
  AnthropicStreamTranslator,
  DEFAULT_ANTHROPIC_STREAM_LIMITS,
  type AnthropicSseEvent,
} from "../src/anthropic/stream";
import { ResponsesStreamProtocol } from "../src/anthropic/stream-protocol";
import {
  CONTEXT,
  DEFAULT_RESPONSES_USAGE,
  context,
} from "./support/anthropic-response";

function frame(
  sequenceNumber: number | undefined,
  type: string,
  fields: Record<string, unknown> = {},
): SseFrame {
  return {
    event: type,
    data: JSON.stringify({
      type,
      ...(sequenceNumber === undefined ? {} : { sequence_number: sequenceNumber }),
      ...fields,
    }),
  };
}

type FinalResponse = Record<string, unknown> & { output: Record<string, unknown>[] };

function finalResponse(output: Record<string, unknown>[] = []): FinalResponse {
  return {
    id: "resp_protocol",
    object: "response",
    created_at: 1,
    model: "gpt-5.6-terra",
    status: "completed",
    error: null,
    incomplete_details: null,
    output,
    usage: DEFAULT_RESPONSES_USAGE,
  } as FinalResponse;
}

function message(
  id: string,
  texts: string[],
): Record<string, unknown> {
  return {
    type: "message",
    id,
    status: "completed",
    role: "assistant",
    content: texts.map(text => ({
      type: "output_text",
      text,
      annotations: [],
      logprobs: [],
    })),
  };
}

function reasoning(id: string, text: string): Record<string, unknown> {
  return {
    type: "reasoning",
    id,
    status: "completed",
    encrypted_content: `${id}-cipher`,
    summary: [{ type: "summary_text", text }],
  };
}

function expectError(events: AnthropicSseEvent[]): void {
  expect(events).toMatchObject([{
    type: "error",
    error: { type: "api_error", message: expect.any(String) },
    request_id: CONTEXT.requestId,
  }]);
}

function eventTypes(events: AnthropicSseEvent[]): string[] {
  return events.map(event => event.type);
}

describe("Anthropic stream sequence integrity", () => {
  it("accepts any nonnegative safe first sequence as the exact progression anchor", () => {
    for (const first of [0, 1, 7]) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      expect(translator.push(frame(first, "response.future_anchor"))).toEqual([]);
      expect(translator.push(frame(first + 1, "response.future_next"))).toEqual([]);
    }

    for (const first of [undefined, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      expectError(translator.push(frame(first, "response.created", {
        response: { id: "resp_sequence", status: "in_progress" },
      })));
    }

    for (const [second, label] of [[4, "duplicate"], [6, "gap"], [3, "regression"]] as const) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      expect(translator.push(frame(4, "response.future_ignored", { label }))).toEqual([]);
      expectError(translator.push(frame(second, "response.another_ignored", { label })));
    }
  });

  it("returns no output for every push or finish after message_stop", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    expect(eventTypes(translator.push(frame(1, "response.completed", {
      response: finalResponse(),
    })))).toEqual(["message_delta", "message_stop"]);
    expect(translator.push(frame(2, "response.completed", {
      response: finalResponse(),
    }))).toEqual([]);
    expect(translator.push({ event: "malformed", data: "not-json" })).toEqual([]);
    expect(translator.finish(finalResponse())).toEqual([]);

    const failed = new AnthropicStreamTranslator(CONTEXT);
    failed.start();
    expect(failed.push(frame(0, "response.failed", {
      response: { error: { message: "private" } },
    }))).toMatchObject([{ type: "error" }]);
    expect(failed.push(frame(1, "response.future_after_error"))).toEqual([]);
    expect(failed.finish(finalResponse())).toEqual([]);
  });
});

describe("Anthropic stream known event envelopes", () => {
  it("validates response.created and response.in_progress envelopes", () => {
    for (const type of ["response.created", "response.in_progress"] as const) {
      const malformed = new AnthropicStreamTranslator(CONTEXT);
      malformed.start();
      expectError(malformed.push(frame(0, type)));

      const valid = new AnthropicStreamTranslator(CONTEXT);
      valid.start();
      expect(valid.push(frame(0, type, {
        response: { id: "resp_lifecycle", status: "in_progress" },
      }))).toEqual([]);
    }
  });

  it("validates documented failed-response and error-event envelopes", () => {
    const limits = {
      maxEvents: DEFAULT_ANTHROPIC_STREAM_LIMITS.maxEvents,
      maxIdentities: DEFAULT_ANTHROPIC_STREAM_LIMITS.maxIdentities,
    };
    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "response.failed",
      sequence_number: 0,
      response: { error: { message: "missing lifecycle" } },
    })).toThrow();
    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "response.failed",
      sequence_number: 0,
      response: {
        id: "resp_failed",
        status: "failed",
        error: { code: "server_error", message: "failed" },
      },
    })).not.toThrow();

    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "error",
      sequence_number: 0,
      error: { message: "wrong envelope" },
    })).toThrow();
    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "error",
      sequence_number: 0,
      code: "server_error",
      message: "failed",
      param: null,
    })).not.toThrow();
    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "error",
      sequence_number: 0,
      code: null,
      message: "failed",
      param: null,
    })).not.toThrow();
    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "error",
      sequence_number: 0,
      code: "",
      message: "failed",
      param: null,
    })).toThrow();
  });

  it("makes output item status requirements specific to the item kind", () => {
    const limits = {
      maxEvents: DEFAULT_ANTHROPIC_STREAM_LIMITS.maxEvents,
      maxIdentities: DEFAULT_ANTHROPIC_STREAM_LIMITS.maxIdentities,
    };
    const reasoningProtocol = new ResponsesStreamProtocol(limits);
    expect(() => reasoningProtocol.accept({
      type: "response.output_item.added",
      sequence_number: 0,
      output_index: 0,
      item: { type: "reasoning", id: "rs_optional_status", summary: [] },
    })).not.toThrow();
    expect(() => reasoningProtocol.accept({
      type: "response.output_item.done",
      sequence_number: 1,
      output_index: 0,
      item: { type: "reasoning", id: "rs_optional_status", summary: [] },
    })).not.toThrow();

    for (const item of [
      { type: "message", id: "msg_status", role: "assistant", content: [] },
      {
        type: "function_call",
        id: "fc_status",
        call_id: "call_status",
        name: "Read",
        arguments: "{}",
      },
    ]) {
      expect(() => new ResponsesStreamProtocol(limits).accept({
        type: "response.output_item.done",
        sequence_number: 0,
        output_index: 0,
        item,
      })).toThrow();
    }

    expect(() => new ResponsesStreamProtocol(limits).accept({
      type: "response.output_item.added",
      sequence_number: 0,
      output_index: 0,
      item: {
        type: "reasoning",
        id: "rs_wrong_status",
        status: "completed",
        summary: [],
      },
    })).toThrow();
  });
});

describe("Anthropic stream identity and lifecycle integrity", () => {
  it("rejects reverse output creation and item ID/index/type collisions", () => {
    const reversed = new AnthropicStreamTranslator(CONTEXT);
    reversed.start();
    expectError(reversed.push(frame(1, "response.output_text.delta", {
      item_id: "msg_reverse",
      output_index: 1,
      content_index: 0,
      delta: "x",
    })));

    const reusedId = new AnthropicStreamTranslator(CONTEXT);
    reusedId.start();
    reusedId.push(frame(1, "response.output_text.delta", {
      item_id: "msg_collision",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    expectError(reusedId.push(frame(2, "response.output_text.delta", {
      item_id: "msg_collision",
      output_index: 1,
      content_index: 0,
      delta: "b",
    })));

    const conflictingId = new AnthropicStreamTranslator(CONTEXT);
    conflictingId.start();
    conflictingId.push(frame(1, "response.output_text.delta", {
      item_id: "msg_a",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    expectError(conflictingId.push(frame(2, "response.output_text.delta", {
      item_id: "msg_b",
      output_index: 0,
      content_index: 0,
      delta: "b",
    })));

    const conflictingType = new AnthropicStreamTranslator(CONTEXT);
    conflictingType.start();
    conflictingType.push(frame(1, "response.output_text.delta", {
      item_id: "item_type",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    expectError(conflictingType.push(frame(2, "response.function_call_arguments.delta", {
      item_id: "item_type",
      output_index: 0,
      delta: "{}",
    })));
  });

  it("rejects conflicting function call aliases", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(1, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call",
        id: "fc_alias",
        status: "in_progress",
        call_id: "call_original",
        name: "Read",
        arguments: "",
      },
    }));
    expectError(translator.push(frame(2, "response.function_call_arguments.delta", {
      item_id: "fc_alias",
      output_index: 0,
      call_id: "call_conflict",
      name: "Write",
      delta: "{}",
    })));
  });

  it("rejects an authoritative final that renames a streamed output position", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(1, "response.output_text.delta", {
      item_id: "msg_original",
      output_index: 0,
      content_index: 0,
      delta: "same",
    }));
    const events = translator.finish(finalResponse([message("msg_conflict", ["same"])]));
    expectError(events);
  });

  it("rejects an authoritative final that changes a zero-byte streamed item type", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(1, "response.output_text.delta", {
      item_id: "item_zero_type",
      output_index: 0,
      content_index: 0,
      delta: "",
    }));
    expectError(translator.finish(finalResponse([{
      type: "function_call",
      id: "item_zero_type",
      status: "completed",
      call_id: "call_zero_type",
      name: "Read",
      arguments: "{}",
    }])));
  });

  it("rejects an observed zero-byte item that disappears from authoritative final", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame(0, "response.output_text.delta", {
      item_id: "msg_disappears",
      output_index: 0,
      content_index: 0,
      delta: "",
    }));
    expectError(translator.finish(finalResponse([])));
  });

  it("rejects observed content or summary parts that disappear from final", () => {
    const content = new AnthropicStreamTranslator(CONTEXT);
    content.start();
    content.push(frame(0, "response.output_text.delta", {
      item_id: "msg_parts_disappear",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    content.push(frame(1, "response.output_text.delta", {
      item_id: "msg_parts_disappear",
      output_index: 0,
      content_index: 1,
      delta: "",
    }));
    expectError(content.finish(finalResponse([message("msg_parts_disappear", ["a"])])));

    const summary = new AnthropicStreamTranslator(CONTEXT);
    summary.start();
    summary.push(frame(4, "response.reasoning_summary_text.delta", {
      item_id: "rs_parts_disappear",
      output_index: 0,
      summary_index: 0,
      delta: "a",
    }));
    summary.push(frame(5, "response.reasoning_summary_text.delta", {
      item_id: "rs_parts_disappear",
      output_index: 0,
      summary_index: 1,
      delta: "",
    }));
    expectError(summary.finish(finalResponse([reasoning("rs_parts_disappear", "a")])));
  });

  it("rejects reverse/gapped content and summary index creation", () => {
    for (const contentIndex of [1, 2]) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      translator.start();
      expectError(translator.push(frame(1, "response.output_text.delta", {
        item_id: "msg_content",
        output_index: 0,
        content_index: contentIndex,
        delta: "x",
      })));
    }

    const contentGap = new AnthropicStreamTranslator(CONTEXT);
    contentGap.start();
    contentGap.push(frame(1, "response.output_text.delta", {
      item_id: "msg_gap",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    expectError(contentGap.push(frame(2, "response.output_text.delta", {
      item_id: "msg_gap",
      output_index: 0,
      content_index: 2,
      delta: "b",
    })));

    const summary = new AnthropicStreamTranslator(CONTEXT);
    summary.start();
    expectError(summary.push(frame(1, "response.reasoning_summary_text.delta", {
      item_id: "rs_reverse",
      output_index: 0,
      summary_index: 1,
      delta: "hidden",
    })));

    const summaryType = new AnthropicStreamTranslator(CONTEXT);
    summaryType.start();
    expectError(summaryType.push(frame(1, "response.reasoning_summary_part.added", {
      item_id: "rs_type",
      output_index: 0,
      summary_index: 0,
      part: { type: "future_summary", text: "" },
    })));
  });

  it("rejects deltas and duplicate done events after content/item completion", () => {
    const text = new AnthropicStreamTranslator(CONTEXT);
    text.start();
    text.push(frame(1, "response.output_text.delta", {
      item_id: "msg_done",
      output_index: 0,
      content_index: 0,
      delta: "a",
    }));
    text.push(frame(2, "response.output_text.done", {
      item_id: "msg_done",
      output_index: 0,
      content_index: 0,
      text: "a",
    }));
    expectError(text.push(frame(3, "response.output_text.delta", {
      item_id: "msg_done",
      output_index: 0,
      content_index: 0,
      delta: "late",
    })));

    const duplicateDone = new AnthropicStreamTranslator(CONTEXT);
    duplicateDone.start();
    duplicateDone.push(frame(1, "response.output_text.done", {
      item_id: "msg_duplicate_done",
      output_index: 0,
      content_index: 0,
      text: "a",
    }));
    expectError(duplicateDone.push(frame(2, "response.output_text.done", {
      item_id: "msg_duplicate_done",
      output_index: 0,
      content_index: 0,
      text: "a",
    })));

    const itemDone = new AnthropicStreamTranslator(CONTEXT);
    itemDone.start();
    itemDone.push(frame(1, "response.output_item.done", {
      output_index: 0,
      item: message("msg_item_done", ["done"]),
    }));
    expectError(itemDone.push(frame(2, "response.output_text.delta", {
      item_id: "msg_item_done",
      output_index: 0,
      content_index: 0,
      delta: "late",
    })));
  });

  it("buffers later output until every lower output item is canonically complete", () => {
    const final = finalResponse([
      message("msg_order", ["first", "second"]),
      reasoning("rs_order", "thought"),
    ]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const emitted: AnthropicSseEvent[] = [...translator.start()];
    const push = (value: SseFrame): void => { emitted.push(...translator.push(value)); };

    push(frame(1, "response.output_item.added", {
      output_index: 0,
      item: { type: "message", id: "msg_order", status: "in_progress", role: "assistant", content: [] },
    }));
    push(frame(2, "response.output_text.delta", {
      item_id: "msg_order",
      output_index: 0,
      content_index: 0,
      delta: "first",
    }));
    push(frame(3, "response.output_text.done", {
      item_id: "msg_order",
      output_index: 0,
      content_index: 0,
      text: "first",
    }));
    push(frame(4, "response.output_item.added", {
      output_index: 1,
      item: { type: "reasoning", id: "rs_order", status: "in_progress", summary: [] },
    }));
    expect(emitted.some(event =>
      event.type === "content_block_start" && event.content_block.type === "thinking"
    )).toBe(false);
    push(frame(5, "response.output_text.delta", {
      item_id: "msg_order",
      output_index: 0,
      content_index: 1,
      delta: "second",
    }));
    push(frame(6, "response.output_text.done", {
      item_id: "msg_order",
      output_index: 0,
      content_index: 1,
      text: "second",
    }));
    push(frame(7, "response.output_item.done", {
      output_index: 0,
      item: final.output[0],
    }));
    push(frame(8, "response.reasoning_summary_text.delta", {
      item_id: "rs_order",
      output_index: 1,
      summary_index: 0,
      delta: "thought",
    }));
    push(frame(9, "response.output_item.done", {
      output_index: 1,
      item: final.output[1],
    }));
    push(frame(10, "response.completed", { response: final }));

    expect(emitted.filter(event => event.type === "content_block_start")
      .map(event => event.content_block.type)).toEqual(["text", "text", "thinking"]);
    let open = false;
    for (const event of emitted) {
      if (event.type === "content_block_start") {
        expect(open).toBe(false);
        open = true;
      } else if (event.type === "content_block_stop") {
        expect(open).toBe(true);
        open = false;
      }
    }
    expect(open).toBe(false);
  });

  it("validates known event payloads even when prewarm suppresses content", () => {
    const translator = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
    translator.start();
    expectError(translator.push(frame(0, "response.content_part.added", {
      item_id: "msg_bad_prewarm",
      output_index: 0,
      content_index: 0,
      part: { type: "output_text" },
    })));

    const functionDone = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
    functionDone.start();
    expectError(functionDone.push(frame(5, "response.function_call_arguments.done", {
      item_id: "fc_bad_done",
      output_index: 0,
    })));

    const missingStatus = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
    missingStatus.start();
    expectError(missingStatus.push(frame(9, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call",
        id: "fc_missing_status",
        call_id: "call_missing_status",
        name: "Read",
        arguments: "",
      },
    })));
  });

  it("accepts the live function-arguments done shape without a repeated name", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    expect(translator.push(frame(0, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call",
        id: "fc_live_shape",
        status: "in_progress",
        call_id: "call_live_shape",
        name: "Write",
        arguments: "",
      },
    }))).not.toEqual(expect.arrayContaining([expect.objectContaining({ type: "error" })]));
    expect(translator.push(frame(1, "response.function_call_arguments.delta", {
      item_id: "fc_live_shape",
      output_index: 0,
      delta: "{\"file_path\":\"proof.txt\"}",
    }))).toEqual([]);

    const done = translator.push(frame(2, "response.function_call_arguments.done", {
      item_id: "fc_live_shape",
      output_index: 0,
      arguments: "{\"file_path\":\"proof.txt\"}",
    }));
    expect(done).not.toEqual(expect.arrayContaining([expect.objectContaining({ type: "error" })]));

    const item = {
      type: "function_call",
      id: "fc_live_shape",
      status: "completed",
      call_id: "call_live_shape",
      name: "Write",
      arguments: "{\"file_path\":\"proof.txt\"}",
    };
    const completed = [
      ...translator.push(frame(3, "response.output_item.done", { output_index: 0, item })),
      ...translator.push(frame(4, "response.completed", { response: finalResponse([item]) })),
    ];
    expect(completed.some(event => event.type === "error")).toBe(false);
    expect(completed).toEqual(expect.arrayContaining([
      expect.objectContaining({
        type: "content_block_start",
        content_block: expect.objectContaining({
          type: "tool_use",
          id: "call_live_shape",
          name: "Write",
        }),
      }),
      expect.objectContaining({ type: "message_stop" }),
    ]));
  });

  it("rejects tool alias changes on done events or authoritative final", () => {
    const changedDone = new AnthropicStreamTranslator(CONTEXT);
    changedDone.start();
    changedDone.push(frame(0, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "function_call", id: "fc_alias", status: "in_progress",
        call_id: "call_alias", name: "Write", arguments: "",
      },
    }));
    expectError(changedDone.push(frame(1, "response.function_call_arguments.done", {
      item_id: "fc_alias", output_index: 0, name: "Read", arguments: "{}",
    })));

    const changedFinal = new AnthropicStreamTranslator(CONTEXT);
    changedFinal.start();
    const streamed = {
      type: "function_call", id: "fc_final_alias", status: "completed",
      call_id: "call_final_alias", name: "Write", arguments: "{}",
    };
    changedFinal.push(frame(0, "response.output_item.added", {
      output_index: 0,
      item: { ...streamed, status: "in_progress", arguments: "" },
    }));
    changedFinal.push(frame(1, "response.function_call_arguments.done", {
      item_id: "fc_final_alias", output_index: 0, arguments: "{}",
    }));
    changedFinal.push(frame(2, "response.output_item.done", { output_index: 0, item: streamed }));
    expectError(changedFinal.push(frame(3, "response.completed", {
      response: finalResponse([{ ...streamed, name: "Read" }]),
    })));
  });

  it("recursively validates known output item members during prewarm", () => {
    const malformedItems: Record<string, unknown>[] = [
      {
        type: "message",
        id: "msg_bad_nested_text",
        status: "in_progress",
        role: "assistant",
        content: [{ type: "output_text", text: 42, annotations: [], logprobs: [] }],
      },
      {
        type: "message",
        id: "msg_bad_nested_refusal",
        status: "in_progress",
        role: "assistant",
        content: [{ type: "refusal", refusal: 42 }],
      },
      {
        type: "reasoning",
        id: "rs_bad_nested_summary",
        status: "in_progress",
        summary: [{ type: "summary_text", text: 42 }],
      },
    ];

    for (const item of malformedItems) {
      const translator = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
      translator.start();
      expectError(translator.push(frame(0, "response.output_item.added", {
        output_index: 0,
        item,
      })));
    }
  });

  it("requires known output_text members in nested prewarm items", () => {
    const translator = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
    translator.start();
    expectError(translator.push(frame(0, "response.output_item.added", {
      output_index: 0,
      item: {
        type: "message",
        id: "msg_bad_nested_logprobs",
        status: "in_progress",
        role: "assistant",
        content: [{ type: "output_text", text: "text", annotations: [] }],
      },
    })));
  });
});
