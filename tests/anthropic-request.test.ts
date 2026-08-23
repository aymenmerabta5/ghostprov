import { describe, expect, it } from "bun:test";
import { AnthropicRequestError } from "../src/anthropic/errors";
import { translateAnthropicRequest } from "../src/anthropic/request";
import { encodeReasoningItem } from "../src/anthropic/reasoning";

const OPTIONS = {
  models: {
    opus: "gpt-5.6-sol",
    sonnet: "gpt-5.6-terra",
    haiku: "gpt-5.6-luna",
    fallback: "gpt-5.6-sol",
    passthrough: new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]),
  },
};

const OUTPUT_SCHEMA = {
  type: "object",
  properties: { answer: { type: "string" } },
  required: ["answer"],
  additionalProperties: false,
};

function request(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    model: "claude-sonnet-4-6",
    max_tokens: 1024,
    messages: [{ role: "user", content: "hello" }],
    ...overrides,
  };
}

function expectInvalid(value: unknown, message: string): void {
  try {
    translateAnthropicRequest(value, OPTIONS);
    throw new Error("expected translateAnthropicRequest to throw");
  } catch (error) {
    expect(error).toBeInstanceOf(AnthropicRequestError);
    expect((error as AnthropicRequestError).status).toBe(400);
    expect((error as Error).message).toContain(message);
  }
}

describe("Anthropic request translation", () => {
  it("maps a Claude Code tool-result turn and restores encrypted reasoning", () => {
    const signature = encodeReasoningItem({
      type: "reasoning",
      id: "rs_1",
      encrypted_content: "cipher",
      summary: [],
    });
    const translated = translateAnthropicRequest({
      model: "claude-sonnet-4-6",
      max_tokens: 32000,
      stream: true,
      system: [{
        type: "text",
        text: "You are Claude Code",
        cache_control: { type: "ephemeral" },
      }],
      messages: [
        { role: "user", content: "read package.json" },
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "", signature },
            {
              type: "tool_use",
              id: "call_1",
              name: "Read",
              input: { file_path: "package.json" },
            },
          ],
        },
        {
          role: "user",
          content: [{ type: "tool_result", tool_use_id: "call_1", content: "{}" }],
        },
      ],
      tools: [{
        name: "Read",
        description: "Read a file",
        input_schema: {
          type: "object",
          properties: { file_path: { type: "string" } },
          required: ["file_path"],
        },
      }],
      thinking: { type: "adaptive" },
      output_config: { effort: "xhigh" },
    }, OPTIONS);

    expect(translated).toMatchObject({
      requestedModel: "claude-sonnet-4-6",
      stream: true,
      thinkingDisplay: "summarized",
      stopSequences: [],
    });
    expect(translated.estimatedInputTokens).toBeGreaterThan(0);
    expect(translated.relayBody).toMatchObject({
      model: "gpt-5.6-terra",
      store: false,
      stream: true,
      max_output_tokens: 32000,
      include: ["reasoning.encrypted_content"],
      instructions: "You are Claude Code",
      reasoning: { effort: "xhigh", summary: "auto" },
      tools: [{
        type: "function",
        name: "Read",
        description: "Read a file",
        parameters: {
          type: "object",
          properties: { file_path: { type: "string" } },
          required: ["file_path"],
        },
      }],
    });

    const input = translated.relayBody.input as Array<Record<string, unknown>>;
    expect(input.map(item => item.type)).toEqual([
      "message",
      "reasoning",
      "function_call",
      "function_call_output",
    ]);
    expect(input[1]).toEqual({
      type: "reasoning",
      id: "rs_1",
      encrypted_content: "cipher",
      summary: [],
    });
    expect(input).toContainEqual(expect.objectContaining({
      type: "function_call",
      call_id: "call_1",
      name: "Read",
      arguments: JSON.stringify({ file_path: "package.json" }),
    }));
    expect(input).toContainEqual(expect.objectContaining({
      type: "function_call_output",
      call_id: "call_1",
      output: "{}",
    }));
    expect(JSON.stringify(translated.relayBody)).not.toContain("ghostprov-r1.");
    expect(JSON.stringify(translated.relayBody)).not.toContain("cache_control");
  });

  it("preserves reasoning, assistant text, tool calls, tool outputs, and media order", () => {
    const signature = encodeReasoningItem({
      type: "reasoning",
      id: "rs_order",
      encrypted_content: "ordered-cipher",
      summary: [{ type: "summary_text", text: "summary" }],
    });
    const translated = translateAnthropicRequest(request({
      messages: [
        { role: "user", content: "start" },
        {
          role: "assistant",
          content: [
            { type: "text", text: "before" },
            { type: "thinking", thinking: "never forward this", signature },
            { type: "text", text: "between" },
            { type: "tool_use", id: "call_order", name: "Read", input: { path: "a" } },
            { type: "text", text: "after" },
            {
              type: "thinking",
              thinking: "foreign private thought",
              signature: "anthropic-native-signature",
            },
            { type: "text", text: "tail" },
            { type: "redacted_thinking", data: "anthropic-native-redacted-data" },
          ],
        },
        {
          role: "user",
          content: [
            { type: "tool_result", tool_use_id: "call_order", content: "result" },
            {
              type: "image",
              source: { type: "url", url: "https://example.com/after-tool.png" },
            },
            { type: "text", text: "done" },
          ],
        },
      ],
    }), OPTIONS);

    const input = translated.relayBody.input as Array<Record<string, unknown>>;
    expect(input.map(item => item.type)).toEqual([
      "message",
      "message",
      "reasoning",
      "message",
      "function_call",
      "message",
      "message",
      "function_call_output",
      "message",
    ]);
    expect(input[1]).toMatchObject({
      role: "assistant",
      content: [{ type: "input_text", text: "before" }],
    });
    expect(input[2]).toMatchObject({ id: "rs_order", encrypted_content: "ordered-cipher" });
    expect(input[3]).toMatchObject({
      role: "assistant",
      content: [{ type: "input_text", text: "between" }],
    });
    expect(input[5]).toMatchObject({
      role: "assistant",
      content: [{ type: "input_text", text: "after" }],
    });
    expect(input[6]).toMatchObject({
      role: "assistant",
      content: [{ type: "input_text", text: "tail" }],
    });
    expect(input[7]).toMatchObject({ type: "function_call_output", call_id: "call_order" });
    expect(input[8]).toMatchObject({
      role: "user",
      content: [
        { type: "input_image", image_url: "https://example.com/after-tool.png" },
        { type: "input_text", text: "done" },
      ],
    });
    const serialized = JSON.stringify(translated.relayBody);
    expect(serialized).not.toContain("never forward this");
    expect(serialized).not.toContain("foreign private thought");
    expect(serialized).not.toContain("anthropic-native-signature");
    expect(serialized).not.toContain("anthropic-native-redacted-data");
  });

  it("maps base64, URL, file-ID, plain-text, and composed media sources", () => {
    const body = translateAnthropicRequest(request({
      messages: [
        {
          role: "user",
          content: [
            {
              type: "image",
              source: { type: "base64", media_type: "image/png", data: "AA==" },
              cache_control: { type: "ephemeral" },
            },
            { type: "image", source: { type: "file", file_id: "file_image" } },
          ],
        },
        {
          role: "user",
          content: [
            { type: "image", source: { type: "url", url: "https://example.com/cat.webp" } },
            {
              type: "document",
              title: "report",
              source: { type: "base64", media_type: "application/pdf", data: "JVBERi0=" },
            },
            { type: "document", source: { type: "url", url: "https://example.com/report.pdf" } },
            { type: "document", source: { type: "file", file_id: "file_document" } },
            {
              type: "document",
              source: { type: "text", media_type: "text/plain", data: "plain document" },
            },
            {
              type: "document",
              source: {
                type: "content",
                content: [
                  { type: "text", text: "composed text" },
                  {
                    type: "image",
                    source: { type: "base64", media_type: "image/jpeg", data: "/9j/" },
                  },
                ],
              },
            },
          ],
        },
      ],
    }), OPTIONS).relayBody;

    expect(body.input).toContainEqual(expect.objectContaining({
      type: "message",
      role: "user",
      content: [
        { type: "input_image", image_url: "data:image/png;base64,AA==" },
        { type: "input_image", file_id: "file_image" },
        { type: "input_image", image_url: "https://example.com/cat.webp" },
        {
          type: "input_file",
          file_data: "data:application/pdf;base64,JVBERi0=",
          filename: "report.pdf",
        },
        { type: "input_file", file_url: "https://example.com/report.pdf" },
        { type: "input_file", file_id: "file_document" },
        { type: "input_text", text: "plain document" },
        { type: "input_text", text: "composed text" },
        { type: "input_image", image_url: "data:image/jpeg;base64,/9j/" },
      ],
    }));
  });

  it("keeps tool-result media structured and prepends the documented error marker", () => {
    const translated = translateAnthropicRequest(request({
      messages: [
        {
          role: "assistant",
          content: [{
            type: "tool_use",
            id: "call_error",
            name: "Read",
            input: {},
          }],
        },
        {
          role: "user",
          content: [
          {
            type: "tool_result",
            tool_use_id: "call_error",
            is_error: true,
            content: [
              { type: "text", text: "permission denied" },
              {
                type: "image",
                source: { type: "base64", media_type: "image/png", data: "AQ==" },
              },
              { type: "document", source: { type: "file", file_id: "file_log" } },
            ],
          },
          { type: "text", text: "recover" },
          ],
        },
      ],
    }), OPTIONS);

    expect(translated.relayBody.input).toEqual([
      {
        type: "function_call",
        call_id: "call_error",
        name: "Read",
        arguments: "{}",
      },
      {
        type: "function_call_output",
        call_id: "call_error",
        output: [
          { type: "input_text", text: "Tool execution failed:\n" },
          { type: "input_text", text: "permission denied" },
          { type: "input_image", image_url: "data:image/png;base64,AQ==" },
          { type: "input_file", file_id: "file_log" },
        ],
      },
      {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "recover" }],
      },
    ]);
  });

  it("requires all tool results to precede ordinary user content", () => {
    expectInvalid(request({
      messages: [
        {
          role: "assistant",
          content: [{
            type: "tool_use",
            id: "call_late",
            name: "Read",
            input: {},
          }],
        },
        {
          role: "user",
          content: [
            { type: "text", text: "ordinary" },
            { type: "tool_result", tool_use_id: "call_late", content: "late" },
          ],
        },
      ],
    }), "tool_result blocks must precede");
  });

  it("maps every current client tool choice and parallel-tool setting", () => {
    const choices = [
      [{ type: "auto" }, "auto"],
      [{ type: "any" }, "required"],
      [{ type: "tool", name: "Read" }, { type: "function", name: "Read" }],
      [{ type: "none" }, "none"],
    ] as const;

    for (const [toolChoice, expected] of choices) {
      const body = translateAnthropicRequest(request({
        tools: [{ name: "Read", input_schema: { type: "object", properties: {} } }],
        tool_choice: toolChoice,
      }), OPTIONS).relayBody;
      expect(body.tool_choice).toEqual(expected);
    }

    const body = translateAnthropicRequest(request({
      tools: [{ name: "Read", input_schema: { type: "object", properties: {} } }],
      tool_choice: { type: "tool", name: "Read", disable_parallel_tool_use: true },
    }), OPTIONS).relayBody;
    expect(body.tool_choice).toEqual({ type: "function", name: "Read" });
    expect(body.parallel_tool_calls).toBe(false);
  });

  it("maps adaptive, disabled, manual-budget, and explicit xhigh reasoning", () => {
    const cases = [
      [{ type: "enabled", budget_tokens: 1024 }, "low"],
      [{ type: "enabled", budget_tokens: 4095 }, "low"],
      [{ type: "enabled", budget_tokens: 4096 }, "medium"],
      [{ type: "enabled", budget_tokens: 15999 }, "medium"],
      [{ type: "enabled", budget_tokens: 16000 }, "high"],
      [{ type: "enabled", budget_tokens: 31999 }, "high"],
      [{ type: "enabled", budget_tokens: 32000 }, "max"],
      [{ type: "adaptive" }, "high"],
    ] as const;

    for (const [thinking, effort] of cases) {
      const translated = translateAnthropicRequest(request({ max_tokens: 64000, thinking }), OPTIONS);
      expect(translated.relayBody.reasoning).toEqual({ effort, summary: "auto" });
    }

    const disabled = translateAnthropicRequest(request({
      thinking: { type: "disabled" },
    }), OPTIONS);
    expect(disabled.relayBody.reasoning).toEqual({ effort: "none" });

    const explicit = translateAnthropicRequest(request({
      thinking: { type: "adaptive", display: "omitted" },
      output_config: { effort: "xhigh" },
    }), OPTIONS);
    expect(explicit.relayBody.reasoning).toEqual({ effort: "xhigh", summary: "auto" });
    expect(explicit.thinkingDisplay).toBe("omitted");

    const effortOnly = translateAnthropicRequest(request({
      output_config: { effort: "max" },
    }), OPTIONS);
    expect(effortOnly.relayBody.reasoning).toEqual({ effort: "max", summary: "auto" });
  });

  it("maps structured output, named choice, speed, and compatible sampling fields", () => {
    const body = translateAnthropicRequest(request({
      cache_control: { type: "ephemeral", ttl: "1h" },
      additive_future_field: { accepted: true },
      context_management: { edits: [{ type: "clear_tool_uses_20250919" }] },
      top_k: 17,
      temperature: 0.2,
      top_p: 0.9,
      speed: "fast",
      tools: [{
        type: "custom",
        name: "Read",
        description: "Read",
        input_schema: { type: "object", properties: {} },
        strict: true,
        cache_control: { type: "ephemeral" },
        additive_tool_property: true,
      }],
      tool_choice: { type: "tool", name: "Read", disable_parallel_tool_use: true },
      thinking: { type: "adaptive", additive_thinking_property: true },
      output_config: {
        effort: "xhigh",
        format: {
          type: "json_schema",
          schema: OUTPUT_SCHEMA,
          additive_format_property: true,
        },
        additive_output_property: true,
      },
    }), OPTIONS).relayBody;

    expect(body.text).toEqual({
      format: {
        type: "json_schema",
        name: "anthropic_output",
        schema: OUTPUT_SCHEMA,
        strict: true,
      },
    });
    expect(body.tool_choice).toEqual({ type: "function", name: "Read" });
    expect(body.parallel_tool_calls).toBe(false);
    expect(body.service_tier).toBe("fast");
    expect(body.temperature).toBe(0.2);
    expect(body.top_p).toBe(0.9);
    expect(body.tools).toEqual([{
      type: "function",
      name: "Read",
      description: "Read",
      parameters: { type: "object", properties: {} },
      strict: true,
    }]);
    for (const omitted of [
      "cache_control",
      "context_management",
      "top_k",
      "speed",
      "output_config",
      "additive_future_field",
    ]) {
      expect(body).not.toHaveProperty(omitted);
    }
  });

  it("joins system blocks, strips cache controls, and tolerates additive properties", () => {
    const translated = translateAnthropicRequest(request({
      system: [
        {
          type: "text",
          text: "first",
          cache_control: { type: "ephemeral" },
          citations: null,
          additive_system_property: true,
        },
        { type: "text", text: "second" },
      ],
      messages: [{
        role: "user",
        additive_message_property: true,
        content: [{
          type: "text",
          text: "hello",
          cache_control: { type: "ephemeral" },
          additive_block_property: true,
        }],
      }],
    }), OPTIONS);

    expect(translated.relayBody.instructions).toBe("first\n\nsecond");
    expect(translated.relayBody.input).toEqual([{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "hello" }],
    }]);
    expect(JSON.stringify(translated.relayBody)).not.toContain("cache_control");
    expect(JSON.stringify(translated.relayBody)).not.toContain("additive_");
  });

  it("returns adapter-only stop sequences and a deterministic input estimate", () => {
    const first = translateAnthropicRequest(request({
      stop_sequences: ["STOP", "END"],
      stream: false,
    }), OPTIONS);
    const second = translateAnthropicRequest(structuredClone(request({
      stop_sequences: ["STOP", "END"],
      stream: false,
    })), OPTIONS);

    expect(first.stopSequences).toEqual(["STOP", "END"]);
    expect(first.relayBody).not.toHaveProperty("stop_sequences");
    expect(first.relayBody).not.toHaveProperty("stop");
    expect(first.stream).toBe(false);
    expect(first.relayBody.stream).toBe(false);
    expect(first.estimatedInputTokens).toBeGreaterThan(0);
    expect(first.estimatedInputTokens).toBe(second.estimatedInputTokens);
  });

  it("rejects malformed required request, message, block, and tool fields", () => {
    const cases: Array<[unknown, string]> = [
      [null, "request must be an object"],
      [{}, "model is required"],
      [request({ model: "" }), "model is required"],
      [request({ max_tokens: -1 }), "max_tokens must be a nonnegative integer"],
      [request({ max_tokens: 1.5 }), "max_tokens must be a nonnegative integer"],
      [request({ messages: "hello" }), "messages is required"],
      [request({ messages: [null] }), "messages[0] must be an object"],
      [request({ messages: [{ role: "system", content: "no" }] }), "messages[0].role"],
      [request({ messages: [{ role: "user", content: 17 }] }), "messages[0].content"],
      [request({ messages: [{ role: "user", content: [null] }] }), "content[0] must be an object"],
      [
        request({ messages: [{ role: "user", content: [{ type: "text" }] }] }),
        "text is required",
      ],
      [
        request({ messages: [{ role: "user", content: [{ type: "image", source: {} }] }] }),
        "image source type is required",
      ],
      [
        request({
          messages: [{
            role: "assistant",
            content: [{ type: "tool_use", id: "call_1", name: "Read" }],
          }],
        }),
        "tool_use input is required",
      ],
      [
        request({ messages: [{ role: "user", content: [{ type: "tool_result" }] }] }),
        "tool_result tool_use_id is required",
      ],
      [request({ tools: [{ input_schema: {} }] }), "tool name is required"],
      [request({ tools: [{ name: "Read" }] }), "tool input_schema is required"],
      [request({ stop_sequences: ["ok", 1] }), "stop_sequences must contain only strings"],
    ];

    for (const [value, message] of cases) expectInvalid(value, message);
  });

  it("rejects unsafe content, media-source, tool, and tool-choice discriminators", () => {
    const cases: Array<[unknown, string]> = [
      [
        request({
          messages: [{ role: "user", content: [{ type: "future_block", payload: "unsafe" }] }],
        }),
        "unsupported user content block type: future_block",
      ],
      [
        request({
          messages: [{ role: "assistant", content: [{ type: "image", source: {} }] }],
        }),
        "unsupported assistant content block type: image",
      ],
      [
        request({
          messages: [{
            role: "user",
            content: [{ type: "image", source: { type: "future_source", value: "unsafe" } }],
          }],
        }),
        "unsupported image source type: future_source",
      ],
      [
        request({
          messages: [
            {
              role: "assistant",
              content: [{ type: "tool_use", id: "call_1", name: "Read", input: {} }],
            },
            {
              role: "user",
              content: [{
                type: "tool_result",
                tool_use_id: "call_1",
                content: [{ type: "future_result", content: [] }],
              }],
            },
          ],
        }),
        "unsupported tool_result content block type: future_result",
      ],
      [
        request({ tools: [{ type: "computer_20250124", name: "computer" }] }),
        "unsupported tool type: computer_20250124",
      ],
      [
        request({ tool_choice: { type: "future_choice" } }),
        "unsupported tool_choice type: future_choice",
      ],
    ];

    for (const [value, message] of cases) expectInvalid(value, message);
  });
});
