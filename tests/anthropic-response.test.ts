import { describe, expect, it } from "bun:test";
import { AnthropicRequestError } from "../src/anthropic/errors";
import { decodeReasoningItem } from "../src/anthropic/reasoning";
import type { AnthropicRequestLimits } from "../src/anthropic/request";
import {
  responseToAnthropic,
  stopReasonFromResponses,
  usageFromResponses,
  type AnthropicResponseContext,
  type ResponsesFinalOutputItem,
} from "../src/anthropic/response";

const CONTEXT = {
  requestId: "req_test",
  requestedModel: "claude-sonnet-4-6",
  thinkingDisplay: "summarized",
  stopSequences: [],
  estimatedInputTokens: 17,
  maxTokensZero: false,
} satisfies AnthropicResponseContext;

const NULLABLE_USAGE_FIELDS = {
  cache_creation: null,
  inference_geo: null,
  iterations: null,
  server_tool_use: null,
  service_tier: null,
  speed: null,
  output_tokens_details: null,
} as const;

const typedFinalOutput: ResponsesFinalOutputItem = {
  type: "message",
  id: "msg_typed",
  status: "completed",
  role: "assistant",
  content: [{ type: "output_text", text: "typed", annotations: [] }],
};

// @ts-expect-error Current final output items require a terminal item status.
const invalidTypedFinalOutput: ResponsesFinalOutputItem = {
  type: "function_call",
  id: "fc_typed",
  call_id: "call_typed",
  name: "Read",
  arguments: "{}",
};

void typedFinalOutput;
void invalidTypedFinalOutput;

type ResponseTestContext = AnthropicResponseContext & {
  limits?: Partial<AnthropicRequestLimits>;
};

function context(overrides: Partial<ResponseTestContext> = {}): ResponseTestContext {
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
        { ...reasoning, status: "completed" },
        {
          type: "message",
          id: "msg_1",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "checking", annotations: [] }],
        },
        {
          type: "function_call",
          id: "fc_1",
          status: "completed",
          call_id: "call_1",
          name: "Read",
          arguments: "{\"file_path\":\"a\"}",
        },
        {
          type: "function_call",
          id: "fc_2",
          status: "completed",
          call_id: "call_2",
          name: "Read",
          arguments: "{\"file_path\":\"b\"}",
        },
      ],
      usage: {
        input_tokens: 100,
        output_tokens: 20,
        input_tokens_details: { cached_tokens: 40, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
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
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 40,
        cache_creation: null,
        inference_geo: null,
        iterations: null,
        server_tool_use: null,
        service_tier: null,
        speed: null,
        output_tokens_details: { thinking_tokens: 0 },
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
    expect(decodeReasoningItem(signature as string)).toEqual({ ...reasoning, summary: [] });
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
      output: [{ ...reasoning, status: "completed" }],
    }, context({ thinkingDisplay: "omitted" }));

    expect(message.content).toHaveLength(1);
    expect(message.content[0]).toMatchObject({ type: "thinking", thinking: "" });
    const signature = message.content[0].signature as string;
    const rawEnvelope = Buffer.from(signature.slice("ghostprov-r1.".length), "base64url")
      .toString("utf8");
    expect(rawEnvelope).not.toContain("first");
    expect(rawEnvelope).not.toContain("second");
    expect(decodeReasoningItem(signature)).toEqual({ ...reasoning, summary: [] });
  });

  it("preserves every message content part and uses an empty object for blank tool arguments", () => {
    const message = responseToAnthropic({
      id: "resp_parts",
      status: "completed",
      output: [
        {
          type: "message",
          id: "msg_parts_1",
          status: "completed",
          role: "assistant",
          content: [
            { type: "output_text", text: "first", annotations: [] },
            { type: "output_text", text: "second", annotations: [] },
          ],
        },
        {
          type: "function_call",
          id: "fc_empty",
          status: "completed",
          call_id: "call_empty",
          name: "List",
          arguments: "  \n",
        },
        {
          type: "message",
          id: "msg_parts_2",
          status: "completed",
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
        id: "msg_incomplete",
        status: "incomplete",
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
        id: "msg_refused",
        status: "completed",
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
      output: [{
        type: "message",
        id: "msg_filtered",
        status: "incomplete",
        role: "assistant",
        content: [{
          type: "output_text",
          text: "filtered STOP explanation",
          annotations: [],
        }],
      }],
    }, context({ stopSequences: ["STOP"] }));
    expect(contentFiltered.stop_reason).toBe("refusal");
    expect(contentFiltered.stop_sequence).toBeNull();
    expect(contentFiltered.content).toEqual([{
      type: "text",
      text: "filtered STOP explanation",
      citations: null,
    }]);

    const empty = responseToAnthropic({
      id: "resp_empty",
      status: "completed",
      output: [],
    }, CONTEXT);
    expect(empty).toMatchObject({
      id: "msg_resp_empty",
      content: [],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: {
        input_tokens: 17,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
      },
    });
  });

  it("maps cache writes, reasoning usage, service tiers, and explicit current nullables", () => {
    const message = responseToAnthropic({
      id: "resp_usage",
      status: "completed",
      service_tier: "priority",
      output: [],
      usage: {
        input_tokens: 100,
        output_tokens: 20,
        input_tokens_details: { cached_tokens: 30, cache_write_tokens: 20 },
        output_tokens_details: { reasoning_tokens: 7 },
      },
    }, CONTEXT);

    expect(message.usage).toEqual({
      input_tokens: 50,
      output_tokens: 20,
      cache_creation_input_tokens: 20,
      cache_read_input_tokens: 30,
      cache_creation: null,
      inference_geo: null,
      iterations: null,
      server_tool_use: null,
      service_tier: "priority",
      speed: null,
      output_tokens_details: { thinking_tokens: 7 },
    });
    expect(usageFromResponses(undefined, 3, "default")).toEqual({
      input_tokens: 3,
      output_tokens: 0,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      ...NULLABLE_USAGE_FIELDS,
      service_tier: "standard",
    });
    expect(usageFromResponses(undefined, 3, "flex").service_tier).toBe("batch");
    expect(usageFromResponses(undefined, 3, "future-tier").service_tier).toBeNull();
  });

  it("rejects cache and reasoning token breakdowns that exceed their totals", () => {
    const malformedUsage = [
      {
        input_tokens: 10,
        output_tokens: 1,
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: {},
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 6, cache_write_tokens: 5 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 2 },
      },
    ];

    for (const usage of malformedUsage) {
      const error = captureAnthropicError(() => usageFromResponses(usage, 3));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned malformed usage.");
    }
  });

  it("truncates at the earliest exact stop across text blocks and suppresses later output", () => {
    const message = responseToAnthropic({
      id: "resp_stop",
      status: "completed",
      output: [
        {
          type: "message",
          id: "msg_stop_1",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "alpha ST", annotations: [] }],
        },
        {
          type: "message",
          id: "msg_stop_2",
          status: "completed",
          role: "assistant",
          content: [
            { type: "output_text", text: "OP omega", annotations: [] },
            { type: "output_text", text: " LATE", annotations: [] },
          ],
        },
        {
          type: "function_call",
          id: "fc_after",
          status: "completed",
          call_id: "call_after",
          name: "Unsafe",
          arguments: "{}",
        },
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
        {
          type: "function_call",
          id: "fc_before",
          status: "completed",
          call_id: "call_before",
          name: "Read",
          arguments: "{}",
        },
        {
          type: "message",
          id: "msg_tool_stop",
          status: "completed",
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

  it("does not match a stop sequence across reasoning or tool boundaries", () => {
    const reasoningBoundary = responseToAnthropic({
      id: "resp_reasoning_boundary",
      status: "completed",
      output: [
        {
          type: "message",
          id: "msg_before_reasoning",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "ST", annotations: [] }],
        },
        {
          type: "reasoning",
          id: "rs_boundary",
          status: "completed",
          encrypted_content: "boundary-cipher",
          summary: [],
        },
        {
          type: "message",
          id: "msg_after_reasoning",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "OP", annotations: [] }],
        },
      ],
    }, context({ stopSequences: ["STOP"] }));
    expect(reasoningBoundary.content.map(block => block.type)).toEqual([
      "text",
      "thinking",
      "text",
    ]);
    expect(reasoningBoundary.stop_reason).toBe("end_turn");
    expect(reasoningBoundary.stop_sequence).toBeNull();

    const toolBoundary = responseToAnthropic({
      id: "resp_tool_boundary",
      status: "completed",
      output: [
        {
          type: "message",
          id: "msg_before_tool",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "ST", annotations: [] }],
        },
        {
          type: "function_call",
          id: "fc_boundary",
          status: "completed",
          call_id: "call_boundary",
          name: "Read",
          arguments: "{}",
        },
        {
          type: "message",
          id: "msg_after_tool",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: "OP", annotations: [] }],
        },
      ],
    }, context({ stopSequences: ["STOP"] }));
    expect(toolBoundary.content.map(block => block.type)).toEqual([
      "text",
      "tool_use",
      "text",
    ]);
    expect(toolBoundary.stop_reason).toBe("tool_use");
    expect(toolBoundary.stop_sequence).toBeNull();
  });

  it("never stop-filters refusal text and gives refusal provenance precedence", () => {
    const message = responseToAnthropic({
      id: "resp_refusal_stop",
      status: "completed",
      output: [{
        type: "message",
        id: "msg_refusal_stop",
        status: "completed",
        role: "assistant",
        content: [
          { type: "output_text", text: "before STOP after", annotations: [] },
          { type: "refusal", refusal: "refusal STOP remains" },
        ],
      }],
    }, context({ stopSequences: ["STOP"] }));

    expect(message.content).toEqual([
      { type: "text", text: "before STOP after", citations: null },
      { type: "text", text: "refusal STOP remains", citations: null },
    ]);
    expect(message.stop_reason).toBe("refusal");
    expect(message.stop_sequence).toBeNull();
  });

  it("rejects unbounded stop sequences supplied through response context", () => {
    const invalidStopSets: unknown[] = [
      ["1", "2", "3", "4", "5"],
      [""],
      ["x".repeat(8192)],
      [1],
    ];
    for (const stopSequences of invalidStopSets) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_invalid_stops",
        status: "completed",
        output: [],
      }, context({ stopSequences: stopSequences as string[] })));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Anthropic response context is malformed.");
    }
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
        input_tokens_details: { cached_tokens: 10, cache_write_tokens: 5 },
        output_tokens_details: { reasoning_tokens: 1 },
      },
    }, context({ maxTokensZero: true }));

    expect(message.content).toEqual([]);
    expect(message.stop_reason).toBe("max_tokens");
    expect(message.stop_sequence).toBeNull();
    expect(message.usage).toEqual({
      input_tokens: 35,
      output_tokens: 0,
      cache_creation_input_tokens: 5,
      cache_read_input_tokens: 10,
      cache_creation: null,
      inference_geo: null,
      iterations: null,
      server_tool_use: null,
      service_tier: null,
      speed: null,
      output_tokens_details: { thinking_tokens: 0 },
    });
  });

  it("short-circuits completed zero-token prewarm before parsing generated output", () => {
    const message = responseToAnthropic({
      id: "resp_warm_completed",
      status: "completed",
      service_tier: "default",
      output: [{
        type: "function_call",
        arguments: "{secret-invalid-json",
      }],
      usage: {
        input_tokens: 20,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 4, cache_write_tokens: 3 },
        output_tokens_details: { reasoning_tokens: 1 },
      },
    }, context({ maxTokensZero: true }));

    expect(message).toMatchObject({
      id: "msg_resp_warm_completed",
      content: [],
      stop_reason: "max_tokens",
      stop_sequence: null,
      usage: {
        input_tokens: 13,
        output_tokens: 0,
        cache_creation_input_tokens: 3,
        cache_read_input_tokens: 4,
        service_tier: "standard",
        output_tokens_details: { thinking_tokens: 0 },
      },
    });
  });

  it("throws a generic api_error for non-empty invalid or non-object tool JSON", () => {
    for (const argumentsValue of ["{\"secret\":", "[]", "null"]) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_bad_args",
        status: "completed",
        output: [{
          type: "function_call",
          id: "fc_bad",
          status: "completed",
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

  it("rejects duplicate function call IDs", () => {
    const error = captureAnthropicError(() => responseToAnthropic({
      id: "resp_duplicate_calls",
      status: "completed",
      output: [
        {
          type: "function_call",
          id: "fc_1",
          status: "completed",
          call_id: "call_duplicate",
          name: "Read",
          arguments: "{}",
        },
        {
          type: "function_call",
          id: "fc_2",
          status: "completed",
          call_id: "call_duplicate",
          name: "Write",
          arguments: "{}",
        },
      ],
    }, CONTEXT));

    expect(error.status).toBe(500);
    expect(error.errorType).toBe("api_error");
    expect(error.message).toBe("Responses relay returned malformed function calls.");
    expect(error.message).not.toContain("call_duplicate");
  });

  it("rejects non-finite, deeply nested, oversized, or over-itemized function arguments", () => {
    let deepValue = "0";
    for (let index = 0; index < 66; index += 1) deepValue = `{\"next\":${deepValue}}`;
    const cases: Array<[string, Partial<AnthropicRequestLimits> | undefined]> = [
      ["{\"value\":1e400}", undefined],
      [deepValue, undefined],
      [JSON.stringify({ secret_payload: "x".repeat(256) }), { inboundBytes: 128 }],
      [JSON.stringify({ items: [1, 2, 3] }), { maxArrayItems: 2 }],
    ];

    for (const [argumentsValue, limits] of cases) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_bounded_args",
        status: "completed",
        output: [{
          type: "function_call",
          id: "fc_bounded",
          status: "completed",
          call_id: "call_bounded",
          name: "Read",
          arguments: argumentsValue,
        }],
      }, context(limits ? { limits } : {})));

      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned invalid function call arguments.");
      expect(error.message).not.toContain("secret_payload");
      expect(error.message).not.toContain(argumentsValue);
    }
  });

  it("requires current final IDs and exact incomplete details", () => {
    const malformedFinals: unknown[] = [
      { status: "completed", output: [] },
      { id: "", status: "completed", output: [] },
      { id: "resp_missing_details", status: "incomplete", output: [] },
      {
        id: "resp_unknown_incomplete",
        status: "incomplete",
        incomplete_details: { reason: "future_reason" },
        output: [],
      },
      {
        id: "resp_completed_details",
        status: "completed",
        incomplete_details: { reason: "max_output_tokens" },
        output: [],
      },
    ];

    for (const final of malformedFinals) {
      const error = captureAnthropicError(() => responseToAnthropic(final, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned a malformed final response.");
    }
  });

  it("requires discriminated current message, function, and reasoning output fields", () => {
    const malformedItems: unknown[] = [
      {
        type: "message",
        status: "completed",
        role: "assistant",
        content: [],
      },
      {
        type: "message",
        id: "msg_missing_status",
        role: "assistant",
        content: [],
      },
      {
        type: "message",
        id: "msg_in_progress",
        status: "in_progress",
        role: "assistant",
        content: [],
      },
      {
        type: "message",
        id: "msg_missing_annotations",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "text" }],
      },
      {
        type: "function_call",
        status: "completed",
        call_id: "call_missing_id",
        name: "Read",
        arguments: "{}",
      },
      {
        type: "function_call",
        id: "fc_missing_status",
        call_id: "call_missing_status",
        name: "Read",
        arguments: "{}",
      },
      {
        type: "reasoning",
        id: "rs_missing_status",
        encrypted_content: "cipher-must-not-leak",
        summary: [],
      },
      { type: "future_output", id: "future_1", status: "completed" },
    ];

    for (const item of malformedItems) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_malformed_item",
        status: "completed",
        output: [item],
      }, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned a malformed final response.");
      expect(error.message).not.toContain("cipher-must-not-leak");
    }
  });

  it("rejects malformed upstream final shapes without echoing encrypted content", () => {
    const malformed: unknown[] = [
      null,
      { id: "resp_bad", status: "completed", output: {} },
      { id: "resp_bad", status: "completed", output: [null] },
      { id: "resp_bad", status: "completed", output: [{ type: "unknown_output" }] },
      {
        id: "resp_bad",
        status: "completed",
        output: [{
          type: "message",
          id: "msg_bad_role",
          status: "completed",
          role: "user",
          content: [],
        }],
      },
      {
        id: "resp_bad",
        status: "completed",
        output: [{
          type: "message",
          id: "msg_bad_text",
          status: "completed",
          role: "assistant",
          content: [{ type: "output_text", text: 42, annotations: [] }],
        }],
      },
      {
        id: "resp_bad",
        status: "completed",
        output: [{
          type: "reasoning",
          id: "rs_bad",
          status: "completed",
          encrypted_content: "cipher-must-not-leak",
          summary: [{ type: "summary_text", text: 42 }],
        }],
      },
      {
        id: "resp_bad",
        status: "completed",
        output: [],
        usage: { input_tokens: 2, output_tokens: -1 },
      },
      {
        id: "resp_bad",
        status: "failed",
        error: { message: "cipher-must-not-leak" },
        output: [],
      },
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
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      ...NULLABLE_USAGE_FIELDS,
    });
    expect(usageFromResponses({
      input_tokens: 12,
      output_tokens: 4,
      input_tokens_details: { cached_tokens: 5, cache_write_tokens: 0 },
      output_tokens_details: { reasoning_tokens: 0 },
    }, 23)).toEqual({
      input_tokens: 7,
      output_tokens: 4,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 5,
      ...NULLABLE_USAGE_FIELDS,
      output_tokens_details: { thinking_tokens: 0 },
    });

    const tool = [{ type: "tool_use", id: "call_1", name: "Read", input: {} }];
    expect(stopReasonFromResponses(
      { id: "resp_stop_helper", status: "completed", output: [] },
      tool,
      null,
    )).toBe("tool_use");
    expect(stopReasonFromResponses(
      {
        id: "resp_stop_helper_incomplete",
        status: "incomplete",
        incomplete_details: { reason: "max_output_tokens" },
        output: [],
      },
      tool,
      null,
    )).toBe("max_tokens");
    expect(stopReasonFromResponses(
      { id: "resp_stop_helper_match", status: "completed", output: [] },
      [],
      "END",
    )).toBe("stop_sequence");
  });
});
