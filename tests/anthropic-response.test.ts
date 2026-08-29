import { describe, expect, it } from "bun:test";
import { decodeReasoningItem } from "../src/anthropic/reasoning";
import { CONTEXT, context, responseToAnthropic } from "./support/anthropic-response";

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
        total_tokens: 120,
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
    expect((signature as string).startsWith("ghostprov-r2.")).toBe(true);
    expect(decodeReasoningItem(signature as string, "considering")).toEqual(reasoning);
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
    const rawEnvelope = Buffer.from(signature.slice("ghostprov-r2.".length), "base64url")
      .toString("utf8");
    expect(rawEnvelope).not.toContain("first");
    expect(rawEnvelope).not.toContain("second");
    expect(decodeReasoningItem(signature)).toEqual({ ...reasoning, summary: [] });
  });

  it("bounds aggregate reasoning signatures before retaining every encoded block", () => {
    const output = ["left", "right"].map(id => ({
      type: "reasoning",
      id: `rs_${id}`,
      status: "completed",
      encrypted_content: id.repeat(100),
      summary: [],
    }));

    expect(() => responseToAnthropic({
      id: "resp_signature_budget",
      status: "completed",
      output,
    }, context({ limits: { outputBytes: 1_024 } }))).toThrow(
      "Responses relay returned a malformed final response.",
    );
  });

  it("applies the aggregate output bound to max_tokens zero responses", () => {
    expect(() => responseToAnthropic({
      id: `resp_${"x".repeat(512)}`,
      status: "completed",
      output: [],
    }, context({
      maxTokensZero: true,
      limits: { outputBytes: 128 },
    }))).toThrow("oversized Anthropic response");
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

});
