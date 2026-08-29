import { describe, expect, it } from "bun:test";
import { RelayError } from "../src/relay/run-handle";
import {
  AnthropicStreamTranslator,
  anthropicSseFrame,
  type AnthropicSseEvent,
} from "../src/anthropic/stream";
import { ApiError } from "../src/http/errors";
import {
  CONTEXT,
  context,
} from "./support/anthropic-response";
import {
  collectStreamEvents as collect,
  finalResponse,
  messageOutput as message,
  reasoningOutput as reasoning,
  refusalPart,
  streamDeltas as deltas,
  streamFrame as frame,
  textPart,
  toolOutput as tool,
} from "./support/anthropic-stream";

describe("Responses to Anthropic stream translation", () => {
  it("emits canonical ordered thinking, text, and interleaved parallel tool blocks", () => {
    const final = finalResponse("resp_stream", [
      reasoning("rs_1", "considering"),
      message("msg_1", [textPart("answer")]),
      tool("fc_1", "call_1", "Read", "{\"file_path\":\"a\"}"),
      tool("fc_2", "call_2", "Read", "{\"file_path\":\"b\"}"),
    ], {
      usage: {
        input_tokens: 100,
        output_tokens: 20,
        input_tokens_details: { cached_tokens: 40, cache_write_tokens: 10 },
        output_tokens_details: { reasoning_tokens: 7 },
        total_tokens: 120,
      },
    });
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.created", { response: { id: "resp_stream", status: "in_progress" } }),
      frame("response.future_additive_event", { private_payload: "ignored" }),
      frame("response.output_item.added", {
        output_index: 0,
        item: { type: "reasoning", id: "rs_1", status: "in_progress", summary: [] },
      }),
      frame("response.reasoning_summary_part.added", {
        item_id: "rs_1",
        output_index: 0,
        summary_index: 0,
        part: { type: "summary_text", text: "" },
      }),
      frame("response.reasoning_summary_text.delta", {
        item_id: "rs_1",
        output_index: 0,
        summary_index: 0,
        delta: "considering",
      }),
      frame("response.output_item.done", { output_index: 0, item: final.output[0] }),
      frame("response.output_item.added", {
        output_index: 1,
        item: { type: "message", id: "msg_1", status: "in_progress", role: "assistant", content: [] },
      }),
      frame("response.content_part.added", {
        item_id: "msg_1",
        output_index: 1,
        content_index: 0,
        part: { type: "output_text", text: "", annotations: [], logprobs: [] },
      }),
      frame("response.output_text.delta", {
        item_id: "msg_1",
        output_index: 1,
        content_index: 0,
        delta: "answer",
      }),
      frame("response.output_text.done", {
        item_id: "msg_1",
        output_index: 1,
        content_index: 0,
        text: "answer",
      }),
      frame("response.output_item.done", { output_index: 1, item: final.output[1] }),
      frame("response.output_item.added", {
        output_index: 2,
        item: {
          type: "function_call",
          id: "fc_1",
          status: "in_progress",
          call_id: "call_1",
          name: "Read",
          arguments: "",
        },
      }),
      frame("response.output_item.added", {
        output_index: 3,
        item: {
          type: "function_call",
          id: "fc_2",
          status: "in_progress",
          call_id: "call_2",
          name: "Read",
          arguments: "",
        },
      }),
      // The second call produces JSON first, but Anthropic blocks remain contiguous and indexed.
      frame("response.function_call_arguments.delta", {
        item_id: "fc_2",
        output_index: 3,
        delta: "{\"file_path\":\"b\"}",
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_1",
        output_index: 2,
        delta: "{\"file_path\":\"a\"}",
      }),
      frame("response.function_call_arguments.done", {
        item_id: "fc_1",
        output_index: 2,
        name: "Read",
        arguments: "{\"file_path\":\"a\"}",
      }),
      frame("response.output_item.done", { output_index: 2, item: final.output[2] }),
      frame("response.function_call_arguments.done", {
        item_id: "fc_2",
        output_index: 3,
        name: "Read",
        arguments: "{\"file_path\":\"b\"}",
      }),
      frame("response.output_item.done", { output_index: 3, item: final.output[3] }),
      frame("response.completed", { response: final }),
    ]);

    expect(events.map(event => event.type)).toEqual([
      "message_start",
      "content_block_start", "content_block_delta", "content_block_delta", "content_block_stop",
      "content_block_start", "content_block_delta", "content_block_stop",
      "content_block_start", "content_block_delta", "content_block_stop",
      "content_block_start", "content_block_delta", "content_block_stop",
      "message_delta", "message_stop",
    ]);
    expect(events.filter(event => event.type === "content_block_start")
      .map(event => event.index)).toEqual([0, 1, 2, 3]);
    expect(events.find(event =>
      event.type === "content_block_delta" && event.delta.type === "signature_delta"
    )).toMatchObject({ delta: { signature: expect.stringContaining("ghostprov-r2.") } });
    expect(deltas(events, "thinking_delta", "thinking")).toEqual(["considering"]);
    expect(deltas(events, "text_delta", "text")).toEqual(["answer"]);
    expect(deltas(events, "input_json_delta", "partial_json")).toEqual([
      "{\"file_path\":\"a\"}",
      "{\"file_path\":\"b\"}",
    ]);
    expect(events[0]).toMatchObject({
      type: "message_start",
      message: {
        id: "msg_req_test",
        type: "message",
        role: "assistant",
        model: CONTEXT.requestedModel,
        content: [],
        stop_reason: null,
        stop_sequence: null,
        usage: { input_tokens: CONTEXT.estimatedInputTokens, output_tokens: 0 },
      },
    });
    expect(events.at(-2)).toMatchObject({
      type: "message_delta",
      delta: { stop_reason: "tool_use", stop_sequence: null },
      usage: {
        input_tokens: 50,
        output_tokens: 20,
        cache_creation_input_tokens: 10,
        cache_read_input_tokens: 40,
        output_tokens_details: { thinking_tokens: 7 },
      },
    });
  });

  it("opens text and tool blocks lazily when added events are absent", () => {
    const final = finalResponse("resp_lazy", [
      message("msg_lazy", [textPart("lazy")]),
      tool("fc_lazy", "call_lazy", "List", "{\"path\":\".\"}"),
    ]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_text.delta", {
        item_id: "msg_lazy",
        output_index: 0,
        content_index: 0,
        delta: "lazy",
      }),
      frame("response.output_text.done", {
        item_id: "msg_lazy",
        output_index: 0,
        content_index: 0,
        text: "lazy",
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_lazy",
        output_index: 1,
        call_id: "call_lazy",
        name: "List",
        delta: "{\"path\":\".\"}",
      }),
      frame("response.output_item.done", { output_index: 1, item: final.output[1] }),
      frame("response.completed", { response: final }),
    ]);

    expect(events.filter(event => event.type === "content_block_start")).toEqual([
      {
        type: "content_block_start",
        index: 0,
        content_block: { type: "text", text: "", citations: null },
      },
      {
        type: "content_block_start",
        index: 1,
        content_block: {
          type: "tool_use",
          id: "call_lazy",
          name: "List",
          input: {},
          caller: { type: "direct" },
        },
      },
    ]);
    expect(deltas(events, "text_delta", "text").join("")).toBe("lazy");
    expect(deltas(events, "input_json_delta", "partial_json").join("")).toBe(
      "{\"path\":\".\"}",
    );
  });

  it("uses terminal-only tools and missing text suffixes as authoritative fallback without duplication", () => {
    const final = finalResponse("resp_fallback", [
      message("msg_fallback", [textPart("hello world")]),
      tool("fc_terminal", "call_terminal", "Read", "{\"file_path\":\"README.md\"}"),
    ]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = [
      ...translator.start(),
      ...translator.push(frame("response.output_text.delta", {
        item_id: "msg_fallback",
        output_index: 0,
        content_index: 0,
        delta: "hello",
      })),
      ...translator.finish(final),
    ];

    expect(deltas(events, "text_delta", "text")).toEqual(["hello", " world"]);
    expect(deltas(events, "input_json_delta", "partial_json")).toEqual([
      "{\"file_path\":\"README.md\"}",
    ]);
    expect(events.filter(event => event.type === "message_delta")).toHaveLength(1);
    expect(events.filter(event => event.type === "message_stop")).toHaveLength(1);
    expect(translator.finish(final)).toEqual([]);
    expect(translator.push(frame("response.completed", { response: final })))
      .toEqual([]);
  });

  it("rejects an authoritative final whose text diverges from the streamed prefix", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    translator.start();
    translator.push(frame("response.output_text.delta", {
      item_id: "msg_diverged",
      output_index: 0,
      content_index: 0,
      delta: "hello",
    }));
    const events = translator.finish(finalResponse("resp_diverged", [
      message("msg_diverged", [textPart("jello")]),
    ]));

    expect(events).toMatchObject([{
      type: "error",
      error: { type: "api_error", message: expect.any(String) },
    }]);
    expect(events.some(event => event.type === "message_stop")).toBe(false);
  });

  it("emits full terminal arguments only when no argument delta was observed", () => {
    const first = tool("fc_no_delta", "call_no_delta", "Read", "{\"path\":\"a\"}");
    const second = tool("fc_with_delta", "call_with_delta", "Read", "{\"path\":\"b\"}");
    const final = finalResponse("resp_tool_fallback", [first, second]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_no_delta",
          status: "in_progress",
          call_id: "call_no_delta",
          name: "Read",
          arguments: "",
        },
      }),
      frame("response.output_item.done", { output_index: 0, item: first }),
      frame("response.output_item.added", {
        output_index: 1,
        item: {
          type: "function_call",
          id: "fc_with_delta",
          status: "in_progress",
          call_id: "call_with_delta",
          name: "Read",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_with_delta",
        output_index: 1,
        delta: "{\"path\":\"b\"}",
      }),
      frame("response.output_item.done", { output_index: 1, item: second }),
      frame("response.completed", { response: final }),
    ]);

    expect(deltas(events, "input_json_delta", "partial_json")).toEqual([
      "{\"path\":\"a\"}",
      "{\"path\":\"b\"}",
    ]);
  });

  it("preserves incremental function JSON chunk boundaries after completion safety", () => {
    const completed = tool("fc_chunks", "call_chunks", "Read", "{\"path\":\"a\"}");
    const final = finalResponse("resp_chunks", [completed]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_chunks",
          status: "in_progress",
          call_id: "call_chunks",
          name: "Read",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_chunks",
        output_index: 0,
        delta: "{\"path\":",
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_chunks",
        output_index: 0,
        delta: "\"a\"}",
      }),
      frame("response.output_item.done", { output_index: 0, item: completed }),
      frame("response.completed", { response: final }),
    ]);

    expect(deltas(events, "input_json_delta", "partial_json")).toEqual([
      "{\"path\":",
      "\"a\"}",
    ]);
  });

  it("appends only the missing terminal suffix after partial function JSON", () => {
    const completed = tool("fc_suffix", "call_suffix", "Read", "{\"path\":\"a\"}");
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_suffix",
          status: "in_progress",
          call_id: "call_suffix",
          name: "Read",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_suffix",
        output_index: 0,
        delta: "{\"path\":",
      }),
      frame("response.completed", {
        response: finalResponse("resp_suffix", [completed]),
      }),
    ]);

    expect(deltas(events, "input_json_delta", "partial_json")).toEqual([
      "{\"path\":",
      "\"a\"}",
    ]);
  });

  it("filters a stop sequence across deltas and suppresses all later visible blocks", () => {
    const final = finalResponse("resp_stop", [
      message("msg_stop", [textPart("before STOP after")]),
      tool("fc_after_stop", "call_after_stop", "Delete", "{\"unsafe\":true}"),
    ]);
    const translator = new AnthropicStreamTranslator(context({ stopSequences: ["STOP"] }));
    const events = collect(translator, [
      frame("response.output_text.delta", {
        item_id: "msg_stop",
        output_index: 0,
        content_index: 0,
        delta: "before ST",
      }),
      frame("response.output_text.delta", {
        item_id: "msg_stop",
        output_index: 0,
        content_index: 0,
        delta: "OP after",
      }),
      frame("response.output_text.done", {
        item_id: "msg_stop",
        output_index: 0,
        content_index: 0,
        text: "before STOP after",
      }),
      frame("response.output_item.added", {
        output_index: 1,
        item: {
          type: "function_call",
          id: "fc_after_stop",
          status: "in_progress",
          call_id: "call_after_stop",
          name: "Delete",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_after_stop",
        output_index: 1,
        delta: "{\"unsafe\":true}",
      }),
      frame("response.output_item.done", { output_index: 1, item: final.output[1] }),
      frame("response.completed", { response: final }),
    ]);

    expect(deltas(events, "text_delta", "text").join("")).toBe("before ");
    expect(events.some(event =>
      event.type === "content_block_start" && event.content_block.type === "tool_use"
    )).toBe(false);
    expect(JSON.stringify(events)).not.toContain("unsafe");
    expect(events.at(-2)).toMatchObject({
      type: "message_delta",
      delta: { stop_reason: "stop_sequence", stop_sequence: "STOP" },
    });
  });

  it("matches stops across adjacent ordinary text blocks but resets at reasoning boundaries", () => {
    const adjacentFinal = finalResponse("resp_adjacent", [
      message("msg_adjacent", [textPart("before ST"), textPart("OP after")]),
    ]);
    const adjacent = new AnthropicStreamTranslator(context({ stopSequences: ["STOP"] }));
    const adjacentEvents = collect(adjacent, [
      frame("response.output_text.delta", {
        item_id: "msg_adjacent",
        output_index: 0,
        content_index: 0,
        delta: "before ST",
      }),
      frame("response.output_text.done", {
        item_id: "msg_adjacent",
        output_index: 0,
        content_index: 0,
        text: "before ST",
      }),
      frame("response.output_text.delta", {
        item_id: "msg_adjacent",
        output_index: 0,
        content_index: 1,
        delta: "OP after",
      }),
      frame("response.output_text.done", {
        item_id: "msg_adjacent",
        output_index: 0,
        content_index: 1,
        text: "OP after",
      }),
      frame("response.completed", { response: adjacentFinal }),
    ]);
    expect(deltas(adjacentEvents, "text_delta", "text").join("")).toBe("before ");
    expect(adjacentEvents.at(-2)).toMatchObject({
      delta: { stop_reason: "stop_sequence", stop_sequence: "STOP" },
    });

    const boundaryFinal = finalResponse("resp_boundary", [
      message("msg_before", [textPart("ST")]),
      reasoning("rs_boundary", "thinking"),
      message("msg_after", [textPart("OP")]),
    ]);
    const boundary = new AnthropicStreamTranslator(context({ stopSequences: ["STOP"] }));
    const boundaryEvents = collect(boundary, [
      frame("response.output_text.delta", {
        item_id: "msg_before",
        output_index: 0,
        content_index: 0,
        delta: "ST",
      }),
      frame("response.output_text.done", {
        item_id: "msg_before",
        output_index: 0,
        content_index: 0,
        text: "ST",
      }),
      frame("response.output_item.added", {
        output_index: 1,
        item: { type: "reasoning", id: "rs_boundary", status: "in_progress", summary: [] },
      }),
      frame("response.reasoning_summary_text.delta", {
        item_id: "rs_boundary",
        output_index: 1,
        summary_index: 0,
        delta: "thinking",
      }),
      frame("response.output_item.done", { output_index: 1, item: boundaryFinal.output[1] }),
      frame("response.output_text.delta", {
        item_id: "msg_after",
        output_index: 2,
        content_index: 0,
        delta: "OP",
      }),
      frame("response.output_text.done", {
        item_id: "msg_after",
        output_index: 2,
        content_index: 0,
        text: "OP",
      }),
      frame("response.completed", { response: boundaryFinal }),
    ]);
    expect(deltas(boundaryEvents, "text_delta", "text").join("")).toBe("STOP");
    expect(boundaryEvents.at(-2)).toMatchObject({
      delta: { stop_reason: "end_turn", stop_sequence: null },
    });
  });

  it("uses refusal provenance over tentative stop filtering and suppresses terminal tools", () => {
    const final = finalResponse("resp_refusal", [
      message("msg_text", [textPart("before STOP after")]),
      tool("fc_refused", "call_refused", "Delete", "{\"secret\":true}"),
      message("msg_refusal", [refusalPart("I cannot do that.")]),
    ]);
    const translator = new AnthropicStreamTranslator(context({ stopSequences: ["STOP"] }));
    const events = collect(translator, [
      frame("response.output_text.delta", {
        item_id: "msg_text",
        output_index: 0,
        content_index: 0,
        delta: "before ST",
      }),
      frame("response.output_text.delta", {
        item_id: "msg_text",
        output_index: 0,
        content_index: 0,
        delta: "OP after",
      }),
      frame("response.output_text.done", {
        item_id: "msg_text",
        output_index: 0,
        content_index: 0,
        text: "before STOP after",
      }),
      frame("response.completed", { response: final }),
    ]);

    expect(deltas(events, "text_delta", "text").join("")).toBe(
      "before STOP afterI cannot do that.",
    );
    expect(events.some(event =>
      event.type === "content_block_start" && event.content_block.type === "tool_use"
    )).toBe(false);
    expect(JSON.stringify(events)).not.toContain("call_refused");
    expect(JSON.stringify(events)).not.toContain("secret");
    expect(events.at(-2)).toMatchObject({
      delta: { stop_reason: "refusal", stop_sequence: null },
    });
  });

  it("does not expose a streamed completed tool when the final response refuses", () => {
    const completedTool = tool(
      "fc_stream_refused",
      "call_stream_refused",
      "Delete",
      "{\"private\":true}",
    );
    const final = finalResponse("resp_stream_refused", [
      completedTool,
      message("msg_stream_refused", [refusalPart("I cannot do that.")]),
    ]);
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_stream_refused",
          status: "in_progress",
          call_id: "call_stream_refused",
          name: "Delete",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_stream_refused",
        output_index: 0,
        delta: "{\"private\":true}",
      }),
      frame("response.output_item.done", { output_index: 0, item: completedTool }),
      frame("response.completed", { response: final }),
    ]);

    expect(events.some(event =>
      event.type === "content_block_start" && event.content_block.type === "tool_use"
    )).toBe(false);
    expect(deltas(events, "text_delta", "text")).toEqual(["I cannot do that."]);
    expect(JSON.stringify(events)).not.toContain("private");
    expect(events.at(-2)).toMatchObject({ delta: { stop_reason: "refusal" } });
  });

  it("suppresses incomplete terminal tools and preserves max-token usage", () => {
    const final = finalResponse("resp_incomplete", [
      tool("fc_incomplete", "call_incomplete", "Write", "{\"unsafe\":true}", "incomplete"),
      message("msg_partial", [textPart("partial")], "incomplete"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      usage: {
        input_tokens: 10,
        output_tokens: 2,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: 12,
      },
    });
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = [...translator.start(), ...translator.finish(final)];

    expect(deltas(events, "text_delta", "text")).toEqual(["partial"]);
    expect(events.some(event =>
      event.type === "content_block_start" && event.content_block.type === "tool_use"
    )).toBe(false);
    expect(JSON.stringify(events)).not.toContain("unsafe");
    expect(events.at(-2)).toMatchObject({
      type: "message_delta",
      delta: { stop_reason: "max_tokens", stop_sequence: null },
      usage: { input_tokens: 10, output_tokens: 2 },
    });
  });

  it("preserves an encrypted incomplete reasoning item under an incomplete response", () => {
    const incompleteReasoning = {
      ...reasoning("rs_incomplete", "partial thought"),
      status: "incomplete",
    };
    const final = finalResponse("resp_reasoning_incomplete", [incompleteReasoning], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    });
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: { type: "reasoning", id: "rs_incomplete", status: "in_progress", summary: [] },
      }),
      frame("response.reasoning_summary_text.delta", {
        item_id: "rs_incomplete",
        output_index: 0,
        summary_index: 0,
        delta: "partial thought",
      }),
      frame("response.output_item.done", { output_index: 0, item: incompleteReasoning }),
      frame("response.incomplete", { response: final }),
    ]);

    expect(deltas(events, "thinking_delta", "thinking")).toEqual(["partial thought"]);
    expect(deltas(events, "signature_delta", "signature")[0]).toStartWith("ghostprov-r2.");
    expect(events.at(-2)).toMatchObject({ delta: { stop_reason: "max_tokens" } });
  });

  it("does not expose a streamed tool whose output item finishes incomplete", () => {
    const incompleteTool = tool(
      "fc_stream_incomplete",
      "call_stream_incomplete",
      "Write",
      "{\"private\":true}",
      "incomplete",
    );
    const final = finalResponse("resp_stream_incomplete", [incompleteTool], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
    });
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_stream_incomplete",
          status: "in_progress",
          call_id: "call_stream_incomplete",
          name: "Write",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_stream_incomplete",
        output_index: 0,
        delta: "{\"private\":true}",
      }),
      frame("response.output_item.done", { output_index: 0, item: incompleteTool }),
      frame("response.incomplete", { response: final }),
    ]);

    expect(events.some(event => event.type === "content_block_start")).toBe(false);
    expect(JSON.stringify(events)).not.toContain("call_stream_incomplete");
    expect(JSON.stringify(events)).not.toContain("private");
    expect(events.at(-2)).toMatchObject({ delta: { stop_reason: "max_tokens" } });
  });

  it("rejects completed malformed tool JSON before exposing its streamed chunks", () => {
    const translator = new AnthropicStreamTranslator(CONTEXT);
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: {
          type: "function_call",
          id: "fc_invalid_json",
          status: "in_progress",
          call_id: "call_invalid_json",
          name: "Write",
          arguments: "",
        },
      }),
      frame("response.function_call_arguments.delta", {
        item_id: "fc_invalid_json",
        output_index: 0,
        delta: "{\"private\":",
      }),
      frame("response.function_call_arguments.done", {
        item_id: "fc_invalid_json",
        output_index: 0,
        arguments: "{\"private\":",
      }),
    ]);

    expect(events.map(event => event.type)).toEqual(["message_start", "error"]);
    expect(JSON.stringify(events)).not.toContain("private");
    expect(events[1]).toMatchObject({
      error: {
        type: "api_error",
        message: "Responses stream failed.",
      },
    });
  });

  it("suppresses all generated content and output usage for zero-token prewarm", () => {
    const final = finalResponse("resp_prewarm", [
      reasoning("rs_warm", "generated reasoning"),
      message("msg_warm", [textPart("generated text")]),
      tool("fc_warm", "call_warm", "Read", "{\"generated\":true}"),
    ], {
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      usage: {
        input_tokens: 50,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 10, cache_write_tokens: 5 },
        output_tokens_details: { reasoning_tokens: 1 },
        total_tokens: 51,
      },
    });
    const translator = new AnthropicStreamTranslator(context({ maxTokensZero: true }));
    const events = collect(translator, [
      frame("response.output_item.added", {
        output_index: 0,
        item: { type: "reasoning", id: "rs_warm", status: "in_progress", summary: [] },
      }),
      frame("response.output_text.delta", {
        item_id: "msg_warm",
        output_index: 1,
        content_index: 0,
        delta: "generated text",
      }),
      frame("response.incomplete", { response: final }),
    ]);

    expect(events.map(event => event.type)).toEqual([
      "message_start",
      "message_delta",
      "message_stop",
    ]);
    expect(events.at(-2)).toMatchObject({
      delta: { stop_reason: "max_tokens", stop_sequence: null },
      usage: {
        input_tokens: 35,
        output_tokens: 0,
        cache_creation_input_tokens: 5,
        cache_read_input_tokens: 10,
        output_tokens_details: { thinking_tokens: 0 },
      },
    });
  });

  it("turns failed, upstream error, malformed JSON, and thrown failures into safe canonical errors", () => {
    const cases: Array<[string, (translator: AnthropicStreamTranslator) => AnthropicSseEvent[]]> = [
      ["response.failed", translator => translator.push(frame("response.failed", {
        response: { error: { message: "secret failed payload" } },
      }))],
      ["error", translator => translator.push(frame("error", {
        error: { message: "secret upstream payload" },
      }))],
      ["malformed", translator => translator.push({
        event: "response.output_text.delta",
        data: "{secret malformed payload",
      })],
      ["relay", translator => translator.fail(
        new RelayError("secret upstream relay payload", "upstream"),
      )],
      ["fail", translator => translator.fail({ secret: "private thrown payload" })],
    ];

    for (const [name, run] of cases) {
      const translator = new AnthropicStreamTranslator(CONTEXT);
      expect(translator.start()).toHaveLength(1);
      const events = run(translator);
      expect(events).toHaveLength(1);
      expect(events[0]).toMatchObject({
        type: "error",
        error: { type: "api_error", message: expect.any(String) },
        request_id: CONTEXT.requestId,
      });
      expect(JSON.stringify(events), name).not.toContain("secret");
      expect(JSON.stringify(events), name).not.toContain("private");
      expect(translator.fail(new Error("later"))).toEqual([]);
    }

    const relay = new AnthropicStreamTranslator(CONTEXT);
    relay.start();
    expect(relay.fail(new RelayError("secret upstream relay payload", "upstream")))
      .toMatchObject({
        0: { error: { message: "Responses stream failed." } },
      });

    const typed = new AnthropicStreamTranslator(CONTEXT);
    typed.start();
    expect(typed.fail(new ApiError(429, "Rate limited", { type: "rate_limit_error" })))
      .toEqual([{
        type: "error",
        error: { type: "rate_limit_error", message: "Responses stream failed." },
        request_id: CONTEXT.requestId,
      }]);
  });

});

describe("Anthropic SSE formatting", () => {
  it("formats canonical named SSE without a done sentinel", () => {
    const event: AnthropicSseEvent = { type: "message_stop" };
    expect(anthropicSseFrame(event)).toBe(
      'event: message_stop\ndata: {"type":"message_stop"}\n\n',
    );
    expect(anthropicSseFrame(event)).not.toContain("[DONE]");
  });
});
