import { describe, expect, it } from "bun:test";
import {
  captureAnthropicError,
  context,
  responseToAnthropic,
} from "./support/anthropic-response";

describe("Anthropic response stop conversion", () => {
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

});
