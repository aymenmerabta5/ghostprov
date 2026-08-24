import { describe, expect, it } from "bun:test";
import {
  CONTEXT,
  captureAnthropicError,
  context,
  responseToAnthropic,
  type ResponseTestLimits,
} from "./support/anthropic-response";

describe("Anthropic response tool conversion", () => {
  it("suppresses incomplete function calls under an incomplete terminal", () => {
    const message = responseToAnthropic({
      id: "resp_incomplete_tool",
      status: "incomplete",
      incomplete_details: { reason: "max_output_tokens" },
      output: [
        {
          type: "function_call",
          id: "fc_incomplete",
          status: "incomplete",
          call_id: "call_incomplete",
          name: "Write",
          arguments: "{\"unsafe\":true}",
        },
        {
          type: "message",
          id: "msg_incomplete_tool",
          status: "incomplete",
          role: "assistant",
          content: [{ type: "output_text", text: "partial", annotations: [] }],
        },
      ],
    }, CONTEXT);

    expect(message.content).toEqual([
      { type: "text", text: "partial", citations: null },
    ]);
    expect(message.stop_reason).toBe("max_tokens");
    expect(JSON.stringify(message)).not.toContain("call_incomplete");
    expect(JSON.stringify(message)).not.toContain("unsafe");
  });

  it("rejects incomplete function calls under a completed terminal", () => {
    const error = captureAnthropicError(() => responseToAnthropic({
      id: "resp_completed_incomplete_tool",
      status: "completed",
      output: [{
        type: "function_call",
        id: "fc_incomplete_completed",
        status: "incomplete",
        call_id: "call_incomplete_completed",
        name: "Write",
        arguments: "{}",
      }],
    }, CONTEXT));

    expect(error.status).toBe(500);
    expect(error.errorType).toBe("api_error");
    expect(error.message).toBe("Responses relay returned a malformed final response.");
    expect(error.message).not.toContain("call_incomplete_completed");
  });

  it("suppresses completed tools when refusal content or content-filter provenance exists", () => {
    for (const final of [
      {
        id: "resp_mixed_refusal",
        status: "completed",
        output: [
          {
            type: "function_call",
            id: "fc_mixed_refusal",
            status: "completed",
            call_id: "call_mixed_refusal",
            name: "Delete",
            arguments: "{\"dangerous\":true}",
          },
          {
            type: "message",
            id: "msg_mixed_refusal",
            status: "completed",
            role: "assistant",
            content: [{ type: "refusal", refusal: "I cannot do that." }],
          },
        ],
      },
      {
        id: "resp_filtered_tool",
        status: "incomplete",
        incomplete_details: { reason: "content_filter" },
        output: [
          {
            type: "function_call",
            id: "fc_filtered",
            status: "completed",
            call_id: "call_filtered",
            name: "Delete",
            arguments: "{\"dangerous\":true}",
          },
          {
            type: "message",
            id: "msg_filtered_tool",
            status: "incomplete",
            role: "assistant",
            content: [{ type: "output_text", text: "filtered", annotations: [] }],
          },
        ],
      },
    ]) {
      const message = responseToAnthropic(final, CONTEXT);
      expect(message.stop_reason).toBe("refusal");
      expect(message.content.some(block => block.type === "tool_use")).toBe(false);
      expect(JSON.stringify(message)).not.toContain("dangerous");
      expect(JSON.stringify(message)).not.toContain("call_");
    }
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
    const cases: Array<[string, ResponseTestLimits | undefined]> = [
      ["{\"value\":1e400}", undefined],
      [deepValue, undefined],
      [JSON.stringify({ secret_payload: "x".repeat(256) }), { outputBytes: 128 }],
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

  it("rejects oversized raw UTF-8 function arguments before parsing", () => {
    const rawArguments = JSON.stringify({ value: "🚀".repeat(40) });
    expect(rawArguments.length).toBeLessThan(128);
    expect(Buffer.byteLength(rawArguments, "utf8")).toBeGreaterThan(128);

    const error = captureAnthropicError(() => responseToAnthropic({
      id: "resp_raw_args",
      status: "completed",
      output: [{
        type: "function_call",
        id: "fc_raw_args",
        status: "completed",
        call_id: "call_raw_args",
        name: "Read",
        arguments: rawArguments,
      }],
    }, context({ limits: { outputBytes: 128 } })));

    expect(error.status).toBe(500);
    expect(error.errorType).toBe("api_error");
    expect(error.message).toBe("Responses relay returned invalid function call arguments.");
    expect(error.message).not.toContain("🚀");
    expect(error.message).not.toContain(rawArguments);
  });

});
