import { describe, expect, it } from "bun:test";
import { AnthropicRequestError } from "../src/anthropic/errors";
import { decodeReasoningItem } from "../src/anthropic/reasoning";
import {
  responseToAnthropic,
  stopReasonFromResponses,
  usageFromResponses,
  type AnthropicResponseContext,
} from "../src/anthropic/response";

const CONTEXT = {
  requestId: "req_test",
  requestedModel: "claude-sonnet-4-6",
  thinkingDisplay: "summarized",
  stopSequences: [],
  estimatedInputTokens: 17,
  maxTokensZero: false,
} satisfies AnthropicResponseContext;

function context(overrides: Partial<AnthropicResponseContext> = {}): AnthropicResponseContext {
  return { ...CONTEXT, ...overrides };
}

function captureAnthropicError(run: () => unknown): AnthropicRequestError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(AnthropicRequestError);
    return error as AnthropicRequestError;
  }
  throw new Error("expected an AnthropicRequestError");
}

describe("Anthropic final response conversion", () => {
  it("converts ordered reasoning, text, parallel tools, usage, and nullables", () => {
    const reasoning = {
      type: "reasoning",
      id: "rs_1",
      encrypted_content: "cipher",
      summary: [{ type: "summary_text", text: "considering" }],
    } as const;
    const message = responseToAnthropic({
      id: "resp_1",
      status: "completed",
      output: [
        reasoning,
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "checking", annotations: [] }],
        },
        { type: "function_call", call_id: "call_1", name: "Read", arguments: "{\"file_path\":\"a\"}" },
        { type: "function_call", call_id: "call_2", name: "Read", arguments: "{\"file_path\":\"b\"}" },
      ],
      usage: {
        input_tokens: 100,
        output_tokens: 20,
        input_tokens_details: { cached_tokens: 40 },
      },
    }, CONTEXT);

    expect(message).toMatchObject({
      id: "msg_resp_1",
      type: "message",
      role: "assistant",
      model: CONTEXT.requestedModel,
      container: null,
      stop_details: null,
      stop_reason: "tool_use",
      stop_sequence: null,
      usage: {
        input_tokens: 60,
        output_tokens: 20,
        cache_creation_input_tokens: null,
        cache_read_input_tokens: 40,
      },
    });
    expect(message.content.map(block => block.type)).toEqual([
      "thinking",
      "text",
      "tool_use",
      "tool_use",
    ]);
    expect(message.content[0]).toMatchObject({ type: "thinking", thinking: "considering" });
    const signature = message.content[0].signature;
    expect(typeof signature).toBe("string");
    expect((signature as string).startsWith("ghostprov-r1.")).toBe(true);
    expect(decodeReasoningItem(signature as string)).toEqual(reasoning);
    expect(message.content[1]).toEqual({ type: "text", text: "checking", citations: null });
    expect(message.content.slice(2)).toEqual([
      {
        type: "tool_use",
        id: "call_1",
        name: "Read",
        input: { file_path: "a" },
        caller: { type: "direct" },
      },
      {
        type: "tool_use",
        id: "call_2",
        name: "Read",
        input: { file_path: "b" },
        caller: { type: "direct" },
      },
    ]);
  });

  it("returns empty displayed thinking while preserving the opaque reasoning envelope", () => {
    const reasoning = {
      type: "reasoning",
      id: "rs_hidden",
      encrypted_content: "opaque-secret",
      summary: [
        { type: "summary_text", text: "first " },
        { type: "summary_text", text: "second" },
      ],
    } as const;

    const message = responseToAnthropic({
      id: "resp_hidden",
      status: "completed",
      output: [reasoning],
    }, context({ thinkingDisplay: "omitted" }));

    expect(message.content).toHaveLength(1);
    expect(message.content[0]).toMatchObject({ type: "thinking", thinking: "" });
    expect(decodeReasoningItem(message.content[0].signature as string)).toEqual(reasoning);
  });

  it("preserves every message content part and uses an empty object for blank tool arguments", () => {
    const message = responseToAnthropic({
      id: "resp_parts",
      status: "completed",
      output: [
        {
          type: "message",
          role: "assistant",
          content: [
            { type: "output_text", text: "first", annotations: [] },
            { type: "output_text", text: "second", annotations: [] },
          ],
        },
        { type: "function_call", call_id: "call_empty", name: "List", arguments: "  \n" },
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "third", annotations: [] }],
        },
      ],
    }, CONTEXT);

    expect(message.content).toEqual([
      { type: "text", text: "first", citations: null },
      { type: "text", text: "second", citations: null },
      {
        type: "tool_use",
        id: "call_empty",
        name: "List",
        input: {},
        caller: { type: "direct" },
      },
      { type: "text", text: "third", citations: null },
    ]);
  });

  it("maps max-output incompletion, refusals, and an empty terminal response", () => {
    const incomplete = responseToAnthropic({
      id: "resp_incomplete",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [{
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: "partial", annotations: [] }],
      }],
    }, CONTEXT);
    expect(incomplete.stop_reason).toBe("max_tokens");

    const refused = responseToAnthropic({
      id: "resp_refused",
      status: "completed",
      output: [{
        type: "message",
        role: "assistant",
        content: [{ type: "refusal", refusal: "I cannot help with that." }],
      }],
    }, CONTEXT);
    expect(refused.content).toEqual([
      { type: "text", text: "I cannot help with that.", citations: null },
    ]);
    expect(refused.stop_reason).toBe("refusal");

    const contentFiltered = responseToAnthropic({
      id: "resp_filtered",
      status: "incomplete",
      incomplete_details: { reason: "content_filter" },
      output: [],
    }, CONTEXT);
    expect(contentFiltered.stop_reason).toBe("refusal");

    const empty = responseToAnthropic({ status: "completed", output: [] }, CONTEXT);
    expect(empty).toMatchObject({
      id: "msg_req_test",
      content: [],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: {
        input_tokens: 17,
        output_tokens: 0,
        cache_creation_input_tokens: null,
        cache_read_input_tokens: 0,
      },
    });
  });

  it("truncates at the earliest exact stop across text blocks and suppresses later output", () => {
    const message = responseToAnthropic({
      id: "resp_stop",
      status: "completed",
      output: [
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "alpha ST", annotations: [] }],
        },
        {
          type: "message",
          role: "assistant",
          content: [
            { type: "output_text", text: "OP omega", annotations: [] },
            { type: "output_text", text: " LATE", annotations: [] },
          ],
        },
        { type: "function_call", call_id: "call_after", name: "Unsafe", arguments: "{}" },
      ],
    }, context({ stopSequences: ["LATE", "STOP"] }));

    expect(message.content).toEqual([
      { type: "text", text: "alpha ", citations: null },
    ]);
    expect(message.stop_reason).toBe("stop_sequence");
    expect(message.stop_sequence).toBe("STOP");
  });

  it("lets a tool before a matched text boundary take stop-reason precedence", () => {
    const message = responseToAnthropic({
      id: "resp_tool_stop",
      status: "completed",
      output: [
        { type: "function_call", call_id: "call_before", name: "Read", arguments: "{}" },
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "before STOP after", annotations: [] }],
        },
      ],
    }, context({ stopSequences: ["STOP"] }));

    expect(message.content).toEqual([
      {
        type: "tool_use",
        id: "call_before",
        name: "Read",
        input: {},
        caller: { type: "direct" },
      },
      { type: "text", text: "before ", citations: null },
    ]);
    expect(message.stop_reason).toBe("tool_use");
    expect(message.stop_sequence).toBeNull();
  });

  it("suppresses prewarm generation and visible output usage while retaining input cache usage", () => {
    const message = responseToAnthropic({
      id: "resp_warm",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [
        {
          type: "reasoning",
          id: "rs_warm",
          encrypted_content: "warm-cipher",
          summary: [{ type: "summary_text", text: "warm reasoning" }],
        },
        {
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: "generated", annotations: [] }],
        },
        { type: "function_call", call_id: "call_warm", name: "Read", arguments: "{}" },
      ],
      usage: {
        input_tokens: 50,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 10 },
      },
    }, context({ maxTokensZero: true }));

    expect(message.content).toEqual([]);
    expect(message.stop_reason).toBe("max_tokens");
    expect(message.stop_sequence).toBeNull();
    expect(message.usage).toEqual({
      input_tokens: 40,
      output_tokens: 0,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: 10,
    });
  });

  it("throws a generic api_error for non-empty invalid or non-object tool JSON", () => {
    for (const argumentsValue of ["{\"secret\":", "[]", "null"]) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_bad_args",
        status: "completed",
        output: [{
          type: "function_call",
          call_id: "call_bad",
          name: "Read",
          arguments: argumentsValue,
        }],
      }, CONTEXT));

      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned invalid function call arguments.");
      expect(error.message).not.toContain(argumentsValue);
      expect(error.message).not.toContain("secret");
    }
  });

  it("rejects malformed upstream final shapes without echoing encrypted content", () => {
    const malformed: unknown[] = [
      null,
      { status: "completed", output: {} },
      { status: "completed", output: [null] },
      { status: "completed", output: [{ type: "unknown_output" }] },
      { status: "completed", output: [{ type: "message", role: "user", content: [] }] },
      {
        status: "completed",
        output: [{
          type: "message",
          role: "assistant",
          content: [{ type: "output_text", text: 42 }],
        }],
      },
      {
        status: "completed",
        output: [{
          type: "reasoning",
          id: "rs_bad",
          encrypted_content: "cipher-must-not-leak",
          summary: [{ type: "summary_text", text: 42 }],
        }],
      },
      {
        status: "completed",
        output: [],
        usage: { input_tokens: 2, output_tokens: -1 },
      },
      { status: "failed", error: { message: "cipher-must-not-leak" }, output: [] },
    ];

    for (const final of malformed) {
      const error = captureAnthropicError(() => responseToAnthropic(final, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).not.toContain("cipher-must-not-leak");
    }
  });

  it("exposes deterministic usage and stop-reason helpers", () => {
    expect(usageFromResponses(undefined, 23)).toEqual({
      input_tokens: 23,
      output_tokens: 0,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: 0,
    });
    expect(usageFromResponses({
      input_tokens: 12,
      output_tokens: 4,
      input_tokens_details: { cached_tokens: 5 },
    }, 23)).toEqual({
      input_tokens: 7,
      output_tokens: 4,
      cache_creation_input_tokens: null,
      cache_read_input_tokens: 5,
    });

    const tool = [{ type: "tool_use", id: "call_1", name: "Read", input: {} }];
    expect(stopReasonFromResponses(
      { status: "completed", output: [] },
      tool,
      null,
    )).toBe("tool_use");
    expect(stopReasonFromResponses(
      {
        status: "incomplete",
        incomplete_details: { reason: "max_output_tokens" },
        output: [],
      },
      tool,
      null,
    )).toBe("max_tokens");
    expect(stopReasonFromResponses(
      { status: "completed", output: [] },
      [],
      "END",
    )).toBe("stop_sequence");
  });
});
