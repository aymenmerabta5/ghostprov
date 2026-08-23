import { describe, expect, it } from "bun:test";
import { stopReasonFromResponses, usageFromResponses } from "../src/anthropic/response";
import {
  CONTEXT,
  DEFAULT_RESPONSES_USAGE,
  NULLABLE_USAGE_FIELDS,
  captureAnthropicError,
  context,
  responseToAnthropic,
} from "./support/anthropic-response";

describe("Anthropic response usage and prewarm", () => {
  it("estimates bounded ordinary usage when an incomplete terminal has null usage", () => {
    const message = responseToAnthropic({
      id: "resp_null_usage",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [{
        type: "message",
        id: "msg_null_usage",
        status: "incomplete",
        role: "assistant",
        content: [{ type: "output_text", text: "12345678", annotations: [] }],
      }],
      usage: null,
    }, CONTEXT);

    expect(message.stop_reason).toBe("max_tokens");
    expect(message.usage).toMatchObject({
      input_tokens: CONTEXT.estimatedInputTokens,
      output_tokens: 2,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
      output_tokens_details: null,
    });
  });

  it("still requires authoritative usage for zero-token prewarm", () => {
    const error = captureAnthropicError(() => responseToAnthropic({
      id: "resp_null_prewarm_usage",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [],
      usage: null,
    }, context({ maxTokensZero: true })));

    expect(error.status).toBe(500);
    expect(error.errorType).toBe("api_error");
    expect(error.message).toBe("Responses relay returned malformed usage.");
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
        total_tokens: 120,
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
    expect(usageFromResponses(undefined, 3, "flex").service_tier).toBeNull();
    expect(usageFromResponses(undefined, 3, "batch").service_tier).toBe("batch");
    expect(usageFromResponses(undefined, 3, "scale").service_tier).toBeNull();
    expect(usageFromResponses(undefined, 3, "ultrafast").service_tier).toBeNull();
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
      {
        input_tokens: 10.5,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: Number.MAX_SAFE_INTEGER + 1,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: 10,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: 12,
      },
      {
        input_tokens: 10,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: 11.5,
      },
    ];

    for (const usage of malformedUsage) {
      const error = captureAnthropicError(() => usageFromResponses(usage, 3));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned malformed usage.");
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
        total_tokens: 51,
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
        total_tokens: 21,
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
      total_tokens: 16,
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
      {
        id: "resp_stop_helper",
        object: "response",
        created_at: 1,
        model: "gpt-5.6-terra",
        status: "completed",
        error: null,
        incomplete_details: null,
        output: [],
        usage: DEFAULT_RESPONSES_USAGE,
      },
      tool,
      null,
    )).toBe("tool_use");
    expect(stopReasonFromResponses(
      {
        id: "resp_stop_helper_incomplete",
        object: "response",
        created_at: 1,
        model: "gpt-5.6-terra",
        status: "incomplete",
        error: null,
        incomplete_details: { reason: "max_output_tokens" },
        output: [],
        usage: DEFAULT_RESPONSES_USAGE,
      },
      tool,
      null,
    )).toBe("max_tokens");
    expect(stopReasonFromResponses(
      {
        id: "resp_stop_helper_match",
        object: "response",
        created_at: 1,
        model: "gpt-5.6-terra",
        status: "completed",
        error: null,
        incomplete_details: null,
        output: [],
        usage: DEFAULT_RESPONSES_USAGE,
      },
      [],
      "END",
    )).toBe("stop_sequence");
  });
});
