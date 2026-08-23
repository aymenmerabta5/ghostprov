import { describe, expect, it } from "bun:test";
import type { AnthropicResponseContentBlock } from "../src/anthropic/types";
import {
  responseToAnthropic as publicResponseToAnthropic,
  type AnthropicAdapterMessage,
  type AnthropicResponseContext,
  type ResponsesFinalOutputItem,
  type ResponsesFinalItemStatus,
  type ResponsesLogprob,
  type TerminalResponseProjection,
} from "../src/anthropic/response";
import {
  CONTEXT,
  DEFAULT_RESPONSES_USAGE,
  captureAnthropicError,
  context,
  convertResponseToAnthropic,
  responseToAnthropic,
} from "./support/anthropic-response";

const typedFinalOutput: ResponsesFinalOutputItem = {
  type: "message",
  id: "msg_typed",
  status: "completed",
  role: "assistant",
  content: [{ type: "output_text", text: "typed", annotations: [], logprobs: [] }],
};
const typedOptionalFunctionOutput: ResponsesFinalOutputItem = {
  type: "function_call",
  status: "completed",
  call_id: "call_optional_id",
  name: "Read",
  arguments: "{}",
};
const typedOptionalReasoningStatus: ResponsesFinalOutputItem = {
  type: "reasoning",
  id: "rs_optional_status",
  encrypted_content: "cipher",
  summary: [],
};
// @ts-expect-error Output messages require their documented status.
const invalidTypedFinalOutput: ResponsesFinalOutputItem = {
  type: "message",
  id: "msg_typed_invalid",
  role: "assistant",
  content: [],
};
const typedAnthropicOutput: AnthropicResponseContentBlock[] = [
  { type: "thinking", thinking: "thought", signature: "opaque" },
  { type: "text", text: "answer", citations: null },
  { type: "tool_use", id: "call_typed", name: "Read", input: {}, caller: { type: "direct" } },
];
// @ts-expect-error Outgoing text blocks require explicit citations.
const invalidAnthropicOutput: AnthropicResponseContentBlock = {
  type: "text",
  text: "missing citations",
};
void typedFinalOutput;
void typedOptionalFunctionOutput;
void typedOptionalReasoningStatus;
void invalidTypedFinalOutput;
void typedAnthropicOutput;
void invalidAnthropicOutput;

const typedFinalResponse: TerminalResponseProjection = {
  id: "resp_typed",
  object: "response",
  created_at: 1,
  model: "gpt-5.6-terra",
  status: "completed",
  error: null,
  incomplete_details: null,
  output: [],
  usage: DEFAULT_RESPONSES_USAGE,
  irrelevant_additive_field: { ignored: true },
};
const invalidTypedFinalResponse: TerminalResponseProjection = {
  ...typedFinalResponse,
  // @ts-expect-error Current final usage requires total_tokens.
  usage: {
    input_tokens: 1,
    output_tokens: 0,
    input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  },
};
void typedFinalResponse;
void invalidTypedFinalResponse;

const typedItemStatuses: ResponsesFinalItemStatus[] = ["in_progress", "completed", "incomplete"];
const typedAdapterConverter: (
  finalValue: unknown,
  context: AnthropicResponseContext,
) => AnthropicAdapterMessage = publicResponseToAnthropic;
void typedItemStatuses;
void typedAdapterConverter;

const typedLogprob: ResponsesLogprob = {
  token: "a",
  bytes: [97],
  logprob: -0.1,
  top_logprobs: [],
};
void typedLogprob;

describe("Anthropic response terminal validation", () => {
  it("requires current top-level response, error, and ordinary terminal usage fields", () => {
    const valid = {
      id: "resp_top_level",
      object: "response",
      created_at: 1,
      model: "gpt-5.6-terra",
      status: "completed",
      error: null,
      incomplete_details: null,
      output: [],
      usage: DEFAULT_RESPONSES_USAGE,
    };
    const malformedFinals: unknown[] = [
      { ...valid, object: undefined },
      { ...valid, object: "future_object" },
      { ...valid, created_at: undefined },
      { ...valid, created_at: -1 },
      { ...valid, created_at: Number.NaN },
      { ...valid, created_at: Number.POSITIVE_INFINITY },
      { ...valid, model: undefined },
      { ...valid, model: "" },
      { ...valid, error: undefined },
      { ...valid, error: { message: "secret-upstream-error" } },
      { ...valid, incomplete_details: undefined },
      { ...valid, incomplete_details: {} },
      { ...valid, usage: undefined },
      { ...valid, usage: null },
    ];

    for (const final of malformedFinals) {
      const error = captureAnthropicError(() => convertResponseToAnthropic(final, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned a malformed final response.");
      expect(error.message).not.toContain("secret-upstream-error");
    }

    const prewarmError = captureAnthropicError(() => convertResponseToAnthropic({
      ...valid,
      id: "resp_preflight_without_usage",
      usage: undefined,
      output: [{ type: "future_output" }],
    }, context({ maxTokensZero: true })));
    expect(prewarmError.status).toBe(500);
    expect(prewarmError.errorType).toBe("api_error");
    expect(prewarmError.message).toBe("Responses relay returned a malformed final response.");
  });

  it("requires incomplete details while allowing an optional reason", () => {
    for (const details of [null, {}]) {
      const final: Record<string, unknown> = {
        id: "resp_optional_incomplete",
        status: "incomplete",
        output: [],
        incomplete_details: details,
      };
      const message = responseToAnthropic(final, CONTEXT);
      expect(message.stop_reason).toBe("max_tokens");
      expect(message.stop_sequence).toBeNull();
    }

    for (const final of [
      {
        id: "resp_missing_incomplete_details",
        object: "response",
        created_at: 1,
        model: "gpt-5.6-terra",
        status: "incomplete",
        error: null,
        output: [],
        usage: DEFAULT_RESPONSES_USAGE,
      },
      {
        id: "resp_undefined_incomplete_details",
        object: "response",
        created_at: 1,
        model: "gpt-5.6-terra",
        status: "incomplete",
        error: null,
        incomplete_details: undefined,
        output: [],
        usage: DEFAULT_RESPONSES_USAGE,
      },
    ]) {
      const error = captureAnthropicError(() => convertResponseToAnthropic(final, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned a malformed final response.");
    }

    for (const reason of [null, "future_reason"]) {
      const error = captureAnthropicError(() => responseToAnthropic({
        id: "resp_bad_optional_reason",
        status: "incomplete",
        incomplete_details: { reason },
        output: [],
      }, CONTEXT));
      expect(error.status).toBe(500);
      expect(error.errorType).toBe("api_error");
      expect(error.message).toBe("Responses relay returned a malformed final response.");
    }
  });

  it("requires current final IDs and rejects completed-only incomplete details", () => {
    const malformedFinals: unknown[] = [
      { status: "completed", output: [] },
      { id: "", status: "completed", output: [] },
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
        type: "message",
        id: "msg_bad_annotations",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "text", annotations: [null] }],
      },
      {
        type: "message",
        id: "msg_bad_known_annotation",
        status: "completed",
        role: "assistant",
        content: [{
          type: "output_text",
          text: "text",
          annotations: [{ type: "url_citation" }],
        }],
      },
      {
        type: "message",
        id: "msg_bad_logprobs",
        status: "completed",
        role: "assistant",
        content: [{
          type: "output_text",
          text: "text",
          annotations: [],
          logprobs: [{ token: 1, bytes: [], logprob: 0, top_logprobs: [] }],
        }],
      },
      {
        type: "message",
        id: "msg_missing_logprobs",
        status: "completed",
        role: "assistant",
        content: [{
          type: "output_text",
          text: "text",
          annotations: [],
          logprobs: undefined,
        }],
      },
      {
        type: "message",
        id: "msg_bad_phase",
        status: "completed",
        role: "assistant",
        phase: "future_phase",
        content: [],
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
        id: "rs_bad_content",
        status: "completed",
        encrypted_content: "cipher-must-not-leak",
        summary: [],
        content: [{ type: "reasoning_text", text: 1 }],
      },
      {
        type: "message",
        id: "msg_incomplete_completed_terminal",
        status: "incomplete",
        role: "assistant",
        content: [],
      },
      {
        type: "reasoning",
        id: "rs_incomplete_completed_terminal",
        status: "incomplete",
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

  it("accepts documented optional and additive terminal output fields", () => {
    const message = responseToAnthropic({
      id: "resp_optional_output_fields",
      status: "completed",
      metadata: { irrelevant: "metadata-marker" },
      temperature: 0.75,
      tools: [{ type: "future_tool", marker: "tool-marker" }],
      future_top_level_field: { ignored: true },
      output: [
        {
          type: "reasoning",
          id: "rs_optional_output",
          encrypted_content: "optional-cipher",
          summary: [{ type: "summary_text", text: "thinking" }],
          content: [{ type: "reasoning_text", text: "opaque reasoning" }],
          additive_reasoning_field: true,
        },
        {
          type: "message",
          id: "msg_optional_output",
          status: "completed",
          role: "assistant",
          phase: "final_answer",
          content: [{
            type: "output_text",
            text: "answer",
            annotations: [{ type: "future_annotation", additive: true }],
            logprobs: [{
              token: "answer",
              bytes: [97],
              logprob: -0.1,
              top_logprobs: [{ token: "answer", bytes: [97], logprob: -0.1 }],
            }],
          }],
        },
        {
          type: "function_call",
          status: "completed",
          call_id: "call_optional_output",
          name: "Read",
          arguments: "{}",
          caller: { type: "direct" },
          namespace: "tools",
        },
      ],
    }, CONTEXT);

    expect(message.content.map(block => block.type)).toEqual([
      "thinking",
      "text",
      "tool_use",
    ]);
    expect(message.stop_reason).toBe("tool_use");
    expect(JSON.stringify(message)).not.toContain("metadata-marker");
    expect(JSON.stringify(message)).not.toContain("tool-marker");
    expect(message).not.toHaveProperty("temperature");
    expect(message).not.toHaveProperty("future_top_level_field");

    const incomplete = responseToAnthropic({
      id: "resp_optional_incomplete_items",
      status: "incomplete",
      output: [
        {
          type: "reasoning",
          id: "rs_optional_incomplete",
          status: "incomplete",
          encrypted_content: "incomplete-cipher",
          summary: [],
        },
        {
          type: "message",
          id: "msg_optional_incomplete",
          status: "incomplete",
          role: "assistant",
          content: [{ type: "output_text", text: "partial", annotations: [] }],
        },
      ],
    }, CONTEXT);
    expect(incomplete.content.map(block => block.type)).toEqual(["thinking", "text"]);
    expect(incomplete.stop_reason).toBe("max_tokens");
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

});
