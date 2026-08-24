import { createHash } from "node:crypto";
import { describe, expect, it } from "bun:test";
import { AnthropicRequestError } from "../src/anthropic/errors";
import * as requestModule from "../src/anthropic/request";
import {
  translateAnthropicRequest,
  type AnthropicRequestLimits,
  type ResponsesInputItem,
} from "../src/anthropic/request";

const OPTIONS = {
  models: {
    opus: "gpt-5.6-sol",
    sonnet: "gpt-5.6-terra",
    haiku: "gpt-5.6-luna",
    fallback: "gpt-5.6-sol",
    passthrough: new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]),
  },
};

const typedAssistantItem: ResponsesInputItem = {
  type: "message",
  role: "assistant",
  content: "history",
};

const invalidTypedAssistantItem: ResponsesInputItem = {
  type: "message",
  role: "assistant",
  // @ts-expect-error Live GPT Responses rejects input_text arrays for assistant easy-input history.
  content: [{ type: "input_text", text: "history" }],
};

void typedAssistantItem;
void invalidTypedAssistantItem;

function request(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    model: "claude-sonnet-4-6",
    max_tokens: 4096,
    messages: [{ role: "user", content: "hello" }],
    ...overrides,
  };
}

function translate(value: unknown, limits?: Partial<AnthropicRequestLimits>) {
  return translateAnthropicRequest(value, {
    ...OPTIONS,
    ...(limits ? { limits } : {}),
  });
}

function expectInvalid(
  value: unknown,
  message?: string,
  expectation: {
    status?: number;
    errorType?: string;
    limits?: Partial<AnthropicRequestLimits>;
  } = {},
): void {
  try {
    translate(value, expectation.limits);
    throw new Error("expected translateAnthropicRequest to throw");
  } catch (error) {
    expect(error).toBeInstanceOf(AnthropicRequestError);
    expect((error as AnthropicRequestError).status).toBe(expectation.status ?? 400);
    if (expectation.errorType) {
      expect((error as AnthropicRequestError).errorType).toBe(expectation.errorType);
    }
    if (message) expect((error as Error).message).toContain(message);
  }
}

function toolUse(id: string, input: unknown = {}): Record<string, unknown> {
  return { type: "tool_use", id, name: "Read", input };
}

function toolResult(id: string, content: unknown = "ok"): Record<string, unknown> {
  return { type: "tool_result", tool_use_id: id, content };
}

describe("Anthropic request current contracts", () => {
  it("preserves Claude Code custom-model system and developer message roles", () => {
    const translated = translate(request({
      model: "gpt-5.6-sol",
      messages: [
        { role: "user", content: "Hi" },
        { role: "system", content: "Claude Code compatibility instructions" },
        { role: "developer", content: [{ type: "text", text: "Use tools safely" }] },
      ],
    }));

    expect(translated.relayBody.input).toEqual([
      { type: "message", role: "user", content: [{ type: "input_text", text: "Hi" }] },
      { type: "message", role: "system", content: [{ type: "input_text", text: "Claude Code compatibility instructions" }] },
      { type: "message", role: "developer", content: [{ type: "input_text", text: "Use tools safely" }] },
    ]);
  });

  it("accepts final mid-conversation system messages on currently supported Claude models", () => {
    for (const model of [
      "claude-fable-5",
      "claude-mythos-5",
      "claude-opus-4-8",
      "claude-opus-5",
    ]) {
      const input = translate(request({
        model,
        messages: [
          { role: "user", content: "Apply an updated constraint" },
          { role: "system", content: "Use the updated constraint" },
        ],
      })).relayBody.input;

      expect(input.at(-1), model).toEqual({
        type: "message",
        role: "system",
        content: [{ type: "input_text", text: "Use the updated constraint" }],
      });
    }
  });

  it("accepts a supported mid-conversation system message before an assistant turn", () => {
    const input = translate(request({
      model: "claude-opus-5",
      messages: [
        { role: "user", content: "Apply an updated constraint" },
        { role: "system", content: "Use the updated constraint" },
        { role: "assistant", content: "Understood" },
      ],
    })).relayBody.input;

    expect(input.map(item => item.type)).toEqual(["message", "message", "message"]);
    expect(input[1]).toMatchObject({ role: "system" });
    expect(input[2]).toMatchObject({ role: "assistant", content: "Understood" });
  });

  it("accepts a supported system message after a user tool-result turn", () => {
    const input = translate(request({
      model: "claude-opus-5",
      messages: [
        { role: "user", content: "Read the file" },
        { role: "assistant", content: [toolUse("call_read")] },
        { role: "user", content: [toolResult("call_read", "contents")] },
        { role: "system", content: "Now summarize only the key point" },
      ],
    })).relayBody.input;

    expect(input.slice(-2)).toEqual([
      { type: "function_call_output", call_id: "call_read", output: "contents" },
      {
        type: "message",
        role: "system",
        content: [{ type: "input_text", text: "Now summarize only the key point" }],
      },
    ]);
  });

  it("rejects mid-conversation system messages on unsupported Claude models", () => {
    for (const model of [
      "claude-sonnet-5",
      "claude-sonnet-4-6",
      "claude-opus-4-6",
      "claude-haiku-5",
      "claude-unknown",
    ]) {
      expectInvalid(request({
        model,
        messages: [
          { role: "user", content: "hello" },
          { role: "system", content: "unsupported update" },
        ],
      }), "system role is not supported for this model");
    }
  });

  it("keeps developer messages exclusive to custom non-Claude models", () => {
    for (const model of [
      "claude-fable-5",
      "claude-mythos-5",
      "claude-opus-4-8",
      "claude-opus-5",
      "claude-sonnet-5",
    ]) {
      expectInvalid(request({
        model,
        messages: [{ role: "developer", content: "not an Anthropic role" }],
      }), "developer role is only supported for custom models");
    }
  });

  it("normalizes surrounding model whitespace before selecting the role policy", () => {
    const supported = translate(request({
      model: "  claude-opus-5  ",
      messages: [
        { role: "user", content: "hello" },
        { role: "system", content: "supported update" },
      ],
    })).relayBody.input;
    expect(supported.at(-1)).toMatchObject({ role: "system" });

    expectInvalid(request({
      model: "  claude-sonnet-5  ",
      messages: [
        { role: "user", content: "hello" },
        { role: "system", content: "unsupported update" },
      ],
    }), "system role is not supported for this model");
    expectInvalid(request({
      model: "  claude-opus-5  ",
      messages: [{ role: "developer", content: "not an Anthropic role" }],
    }), "developer role is only supported for custom models");
  });

  it("enforces current mid-conversation system placement", () => {
    const cases: Array<[unknown[], string]> = [
      [
        [{ role: "system", content: "too early" }],
        "system message must immediately follow a user turn",
      ],
      [
        [
          { role: "user", content: "hello" },
          { role: "assistant", content: "answer" },
          { role: "system", content: "misplaced" },
        ],
        "system message must immediately follow a user turn",
      ],
      [
        [
          { role: "user", content: "hello" },
          { role: "system", content: "misplaced" },
          { role: "user", content: "continue" },
        ],
        "system message must be final or immediately followed by an assistant turn",
      ],
      [
        [
          { role: "user", content: "hello" },
          { role: "system", content: "first" },
          { role: "system", content: "second" },
        ],
        "system message must be final or immediately followed by an assistant turn",
      ],
    ];

    for (const [messages, error] of cases) {
      expectInvalid(request({ model: "claude-opus-5", messages }), error);
    }
  });

  it("does not let compatibility control roles interrupt pending tool results", () => {
    expectInvalid(request({
      model: "gpt-5.6-sol",
      messages: [
        { role: "assistant", content: [toolUse("call_pending")] },
        { role: "system", content: "intervening control message" },
        { role: "user", content: [toolResult("call_pending")] },
      ],
    }), "system turn intervened before pending tool_result blocks");
  });

  it("does not let a supported system message interrupt pending tool results", () => {
    expectInvalid(request({
      model: "claude-opus-5",
      messages: [
        { role: "assistant", content: [toolUse("call_pending")] },
        { role: "system", content: "intervening control message" },
        { role: "user", content: [toolResult("call_pending")] },
      ],
    }), "system turn intervened before pending tool_result blocks");
  });

  it("uses the live-compatible Responses string form for assistant history", () => {
    const body = translate(request({
      messages: [
        { role: "user", content: "hello" },
        {
          role: "assistant",
          content: [
            { type: "text", text: "first" },
            { type: "text", text: "second" },
          ],
        },
        { role: "user", content: "continue" },
      ],
    })).relayBody;

    expect(body.input).toContainEqual({
      type: "message",
      role: "assistant",
      content: "firstsecond",
    });
    expect(JSON.stringify(body.input)).not.toContain("output_text");
  });

  it("preserves valid parallel calls and accepts each result exactly once", () => {
    const input = translate(request({
      messages: [
        { role: "user", content: "read both" },
        { role: "assistant", content: [toolUse("call_a"), toolUse("call_b")] },
        {
          role: "user",
          content: [
            toolResult("call_b", "B"),
            toolResult("call_a", "A"),
            { type: "text", text: "summarize" },
          ],
        },
      ],
    })).relayBody.input as Array<Record<string, unknown>>;

    expect(input.map(item => item.type)).toEqual([
      "message",
      "function_call",
      "function_call",
      "function_call_output",
      "function_call_output",
      "message",
    ]);
    expect(input.slice(1, 5).map(item => item.call_id)).toEqual([
      "call_a",
      "call_b",
      "call_b",
      "call_a",
    ]);
  });

  it("coalesces split same-role tool calls and results into logical turns", () => {
    const input = translate(request({
      messages: [
        { role: "user", content: "read both" },
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "assistant", content: [toolUse("call_b")] },
        { role: "user", content: [toolResult("call_a", "A")] },
        { role: "user", content: [toolResult("call_b", "B")] },
      ],
    })).relayBody.input as Array<Record<string, unknown>>;

    expect(input.map(item => item.type)).toEqual([
      "message",
      "function_call",
      "function_call",
      "function_call_output",
      "function_call_output",
    ]);
    expect(input.slice(1).map(item => item.call_id)).toEqual([
      "call_a",
      "call_b",
      "call_a",
      "call_b",
    ]);
  });

  it("coalesces consecutive ordinary same-role messages in order", () => {
    const input = translate(request({
      messages: [
        { role: "user", content: "one" },
        { role: "user", content: [{ type: "text", text: "two" }] },
        { role: "assistant", content: "three" },
        { role: "assistant", content: [{ type: "text", text: "four" }] },
        { role: "user", content: "five" },
      ],
    })).relayBody.input;

    expect(input).toEqual([
      {
        type: "message",
        role: "user",
        content: [
          { type: "input_text", text: "one" },
          { type: "input_text", text: "two" },
        ],
      },
      {
        type: "message",
        role: "assistant",
        content: "threefour",
      },
      {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "five" }],
      },
    ]);
  });

  const integrityCases: Array<[string, () => unknown]> = [
    ["orphan tool results", () => request({
      messages: [{ role: "user", content: [toolResult("never_called")] }],
    })],
    ["duplicate tool results", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: [toolResult("call_a"), toolResult("call_a")] },
      ],
    })],
    ["missing parallel tool results", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a"), toolUse("call_b")] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["ordinary content before all pending results", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a"), toolUse("call_b")] },
        {
          role: "user",
          content: [toolResult("call_a"), { type: "text", text: "too early" }],
        },
      ],
    })],
    ["an intervening assistant turn", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "assistant", content: "intervening" },
      ],
    })],
    ["an intervening ordinary user turn", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: "intervening" },
      ],
    })],
    ["repeated completed tool results", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: [toolResult("call_a")] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["mismatched pending tool results", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: [toolResult("call_b")] },
      ],
    })],
    ["duplicate assistant tool-use IDs", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a"), toolUse("call_a")] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["repeated assistant tool-use IDs", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: [toolResult("call_a")] },
        { role: "assistant", content: [toolUse("call_a")] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["a final unresolved tool call", () => request({
      messages: [{ role: "assistant", content: [toolUse("call_a")] }],
    })],
  ];

  for (const [name, makeValue] of integrityCases) {
    it(`rejects ${name}`, () => expectInvalid(makeValue()));
  }

  it("maps current search_result blocks to bounded textual function output", () => {
    const input = translate(request({
      messages: [
        { role: "assistant", content: [toolUse("call_search")] },
        {
          role: "user",
          content: [toolResult("call_search", [{
            type: "search_result",
            title: "Example result",
            source: "https://example.com/result",
            content: [
              { type: "text", text: "first excerpt" },
              { type: "text", text: "second excerpt" },
            ],
            cache_control: { type: "ephemeral" },
          }])],
        },
      ],
    })).relayBody.input as Array<Record<string, unknown>>;

    expect(input).toContainEqual({
      type: "function_call_output",
      call_id: "call_search",
      output: [{
        type: "input_text",
        text: [
          "Search result",
          "Title: Example result",
          "Source: https://example.com/result",
          "",
          "first excerpt",
          "second excerpt",
        ].join("\n"),
      }],
    });
  });

  it("maps a current ordinary search_result block to user input text", () => {
    const body = translate(request({
      messages: [{
        role: "user",
        content: [{
          type: "search_result",
          title: "Documentation",
          source: "urn:docs:1",
          content: [{ type: "text", text: "documented excerpt" }],
        }],
      }],
    })).relayBody;

    expect(body.input).toEqual([{
      type: "message",
      role: "user",
      content: [{
        type: "input_text",
        text: "Search result\nTitle: Documentation\nSource: urn:docs:1\n\ndocumented excerpt",
      }],
    }]);
  });

  const strictCases: Array<[string, () => unknown]> = [
    ["null tools", () => request({ tools: null })],
    ["null tool_choice", () => request({ tool_choice: null })],
    ["null output_config", () => request({ output_config: null })],
    ["null thinking", () => request({ thinking: null })],
    ["null stop_sequences", () => request({ stop_sequences: null })],
    ["null system", () => request({ system: null })],
    ["null temperature", () => request({ temperature: null })],
    ["null top_p", () => request({ top_p: null })],
    ["null top_k", () => request({ top_k: null })],
    ["null stream", () => request({ stream: null })],
    ["null service_tier", () => request({ service_tier: null })],
    ["null speed", () => request({ speed: null })],
    ["null tool_use input", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a", null)] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["array tool_use input", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a", [])] },
        { role: "user", content: [toolResult("call_a")] },
      ],
    })],
    ["non-boolean tool_result.is_error", () => request({
      messages: [
        { role: "assistant", content: [toolUse("call_a")] },
        {
          role: "user",
          content: [{ ...toolResult("call_a"), is_error: "yes" }],
        },
      ],
    })],
    ["non-boolean disable_parallel_tool_use", () => request({
      tool_choice: { type: "auto", disable_parallel_tool_use: 1 },
    })],
    ["non-boolean tool strict", () => request({
      tools: [{ name: "Read", input_schema: { type: "object" }, strict: 1 }],
    })],
    ["non-string tool description", () => request({
      tools: [{ name: "Read", input_schema: { type: "object" }, description: 1 }],
    })],
    ["non-boolean defer_loading", () => request({
      tools: [{ name: "Read", input_schema: { type: "object" }, defer_loading: null }],
    })],
    ["non-boolean eager_input_streaming", () => request({
      tools: [{ name: "Read", input_schema: { type: "object" }, eager_input_streaming: "yes" }],
    })],
    ["input_schema without object type", () => request({
      tools: [{ name: "Read", input_schema: { properties: {} } }],
    })],
    ["input_schema with a non-object type", () => request({
      tools: [{ name: "Read", input_schema: { type: "string" } }],
    })],
    ["unknown output format", () => request({
      output_config: { format: { type: "future_format", schema: {} } },
    })],
    ["non-object output schema", () => request({
      output_config: { format: { type: "json_schema", schema: [] } },
    })],
    ["unknown effort", () => request({ output_config: { effort: "extreme" } })],
    ["unknown thinking type", () => request({ thinking: { type: "future_thinking" } })],
    ["unknown thinking display", () => request({
      thinking: { type: "adaptive", display: "hidden" },
    })],
    ["enabled budget below 1024", () => request({
      max_tokens: 4096,
      thinking: { type: "enabled", budget_tokens: 1023 },
    })],
    ["enabled budget equal to max_tokens", () => request({
      max_tokens: 4096,
      thinking: { type: "enabled", budget_tokens: 4096 },
    })],
    ["invalid enabled budget despite explicit effort", () => request({
      max_tokens: 4096,
      thinking: { type: "enabled", budget_tokens: 1 },
      output_config: { effort: "xhigh" },
    })],
    ["temperature below zero", () => request({ temperature: -0.01 })],
    ["temperature above one", () => request({ temperature: 1.01 })],
    ["non-numeric temperature", () => request({ temperature: "0.5" })],
    ["top_p below zero", () => request({ top_p: -0.01 })],
    ["top_p above one", () => request({ top_p: 1.01 })],
    ["non-numeric top_p", () => request({ top_p: "0.5" })],
    ["negative top_k", () => request({ top_k: -1 })],
    ["fractional top_k", () => request({ top_k: 1.5 })],
    ["non-numeric top_k", () => request({ top_k: "1" })],
    ["non-boolean stream", () => request({ stream: "true" })],
    ["malformed search_result", () => request({
      messages: [{
        role: "user",
        content: [{ type: "search_result", title: "missing fields", content: "no" }],
      }],
    })],
  ];

  for (const [name, makeValue] of strictCases) {
    it(`rejects ${name}`, () => expectInvalid(makeValue()));
  }

  it("accepts valid known booleans, sampling boundaries, schemas, and efforts", () => {
    for (const effort of ["low", "medium", "high", "xhigh", "max"] as const) {
      const body = translate(request({
        stream: true,
        temperature: 0,
        top_p: 1,
        top_k: 0,
        tools: [{
          name: "Read",
          input_schema: { type: "object", properties: {} },
          strict: false,
          defer_loading: true,
          eager_input_streaming: false,
        }],
        tool_choice: { type: "auto", disable_parallel_tool_use: false },
        thinking: { type: "adaptive", display: "summarized" },
        output_config: {
          effort,
          format: { type: "json_schema", schema: { type: "object" } },
        },
      })).relayBody;
      expect(body.reasoning).toEqual({ effort, summary: "auto" });
      expect(body.temperature).toBe(0);
      expect(body.top_p).toBe(1);
      expect(body).not.toHaveProperty("top_k");
    }
  });

  it("accepts null eager_input_streaming as omitted", () => {
    const body = translate(request({
      tools: [{
        name: "Read",
        input_schema: { type: "object" },
        eager_input_streaming: null,
      }],
    })).relayBody;
    expect(body.tools).toEqual([{
      type: "function",
      name: "Read",
      parameters: { type: "object" },
    }]);
  });

  it("rejects duplicate declared tool names", () => {
    expectInvalid(request({
      tools: [
        { name: "Read", input_schema: { type: "object" } },
        { name: "Read", input_schema: { type: "object" } },
      ],
    }), "duplicate tool name");
  });

  it("rejects a named choice absent from declared tools", () => {
    expectInvalid(request({
      tools: [{ name: "Read", input_schema: { type: "object" } }],
      tool_choice: { type: "tool", name: "Write" },
    }), "not declared");
  });

  it("rejects tool_choice any when no tools are declared", () => {
    expectInvalid(request({ tool_choice: { type: "any" } }), "requires at least one declared tool");
  });

  for (const [name, toolChoice] of [
    ["any", { type: "any" }],
    ["named", { type: "tool", name: "Read" }],
  ] as const) {
    it(`rejects ${name} tool choice with manual extended thinking`, () => {
      expectInvalid(request({
        max_tokens: 4096,
        tools: [{ name: "Read", input_schema: { type: "object" } }],
        tool_choice: toolChoice,
        thinking: { type: "enabled", budget_tokens: 1024 },
      }), "manual thinking");
    });
  }

  it("allows auto and none tool choice with manual extended thinking", () => {
    for (const toolChoice of [{ type: "auto" }, { type: "none" }] as const) {
      const translated = translate(request({
        max_tokens: 4096,
        tools: [{ name: "Read", input_schema: { type: "object" } }],
        tool_choice: toolChoice,
        thinking: { type: "enabled", budget_tokens: 1024 },
      }));
      expect(translated.relayBody.tool_choice).toBe(toolChoice.type);
    }
  });

  it("allows forced choices with adaptive thinking", () => {
    const required = translate(request({
      tools: [{ name: "Read", input_schema: { type: "object" } }],
      tool_choice: { type: "any" },
      thinking: { type: "adaptive" },
    })).relayBody;
    const named = translate(request({
      tools: [{ name: "Read", input_schema: { type: "object" } }],
      tool_choice: { type: "tool", name: "Read" },
      thinking: { type: "adaptive" },
    })).relayBody;
    expect(required.tool_choice).toBe("required");
    expect(named.tool_choice).toEqual({ type: "function", name: "Read" });
  });

  const invalidMediaCases: Array<[string, Record<string, unknown>]> = [
    ["unsupported image MIME", {
      type: "image",
      source: { type: "base64", media_type: "image/bmp", data: "AA==" },
    }],
    ["malformed base64 alphabet", {
      type: "image",
      source: { type: "base64", media_type: "image/png", data: "A$==" },
    }],
    ["malformed base64 length", {
      type: "image",
      source: { type: "base64", media_type: "image/png", data: "AAA" },
    }],
    ["non-PDF base64 document", {
      type: "document",
      source: { type: "base64", media_type: "text/plain", data: "AA==" },
    }],
    ["non-text plain document", {
      type: "document",
      source: { type: "text", media_type: "application/json", data: "{}" },
    }],
    ["empty image file ID", {
      type: "image",
      source: { type: "file", file_id: "   " },
    }],
    ["empty document file ID", {
      type: "document",
      source: { type: "file", file_id: "" },
    }],
    ["non-HTTP image URL", {
      type: "image",
      source: { type: "url", url: "ftp://example.com/image.png" },
    }],
    ["relative document URL", {
      type: "document",
      source: { type: "url", url: "/document.pdf" },
    }],
  ];

  for (const [name, block] of invalidMediaCases) {
    it(`rejects ${name}`, () => expectInvalid(request({
      messages: [{ role: "user", content: [block] }],
    })));
  }

  it("accepts every current image MIME with canonical base64", () => {
    for (const mediaType of ["image/jpeg", "image/png", "image/gif", "image/webp"]) {
      const body = translate(request({
        messages: [{
          role: "user",
          content: [{
            type: "image",
            source: { type: "base64", media_type: mediaType, data: "AA==" },
          }],
        }],
      })).relayBody;
      expect(JSON.stringify(body.input)).toContain(`data:${mediaType};base64,AA==`);
    }
  });

  it("maps current Anthropic service tiers and lets fast speed override", () => {
    expect(translate(request()).relayBody.service_tier).toBe("auto");
    expect(translate(request({ service_tier: "auto" })).relayBody.service_tier).toBe("auto");
    expect(translate(request({ service_tier: "standard_only" })).relayBody.service_tier).toBe("default");
    expect(translate(request({
      service_tier: "standard_only",
      speed: "fast",
    })).relayBody.service_tier).toBe("fast");
    expect(translate(request({ speed: "standard" })).relayBody.service_tier).toBe("auto");
  });

  for (const [name, fields] of [
    ["unknown service tier", { service_tier: "priority" }],
    ["non-string service tier", { service_tier: 1 }],
    ["unknown speed", { speed: "turbo" }],
    ["non-string speed", { speed: true }],
  ] as const) {
    it(`rejects ${name}`, () => expectInvalid(request(fields)));
  }

  it("hashes metadata.user_id into bounded privacy-safe Responses fields", () => {
    const userId = "opaque-user-123";
    const digest = createHash("sha256").update(userId, "utf8").digest("hex");
    const first = translate(request({
      metadata: { user_id: userId, additive_field: "ignored" },
    })).relayBody;
    const second = translate(request({ metadata: { user_id: userId } })).relayBody;

    expect(first.safety_identifier).toBe(digest);
    expect(first.metadata).toEqual({ anthropic_user_id_sha256: digest });
    expect(second.safety_identifier).toBe(digest);
    expect(String(first.safety_identifier)).toHaveLength(64);
    expect(JSON.stringify(first)).not.toContain(userId);
    expect(JSON.stringify(first)).not.toContain("additive_field");
  });

  for (const [name, metadata] of [
    ["null metadata", null],
    ["numeric metadata user_id", { user_id: 123 }],
    ["empty metadata user_id", { user_id: "" }],
  ] as const) {
    it(`rejects ${name}`, () => expectInvalid(request({ metadata })));
  }

  it("publishes current inbound and internal relay byte defaults", () => {
    const defaults = (requestModule as unknown as {
      DEFAULT_ANTHROPIC_REQUEST_LIMITS?: AnthropicRequestLimits;
    }).DEFAULT_ANTHROPIC_REQUEST_LIMITS;
    expect(defaults).toMatchObject({
      inboundBytes: 32 * 1024 * 1024,
      outputBytes: 32 * 1024 * 1024,
      relayBytes: 64 * 1024 * 1024,
      mediaBytes: 32 * 1024 * 1024,
      maxJsonDepth: 64,
      maxArrayItems: 100_000,
    });
  });

  it("rejects messages beyond an injected item limit without large allocations", () => {
    expectInvalid(request({
      messages: Array.from({ length: 3 }, () => ({ role: "user", content: "x" })),
    }), "2 items", { limits: { maxArrayItems: 2 } });
  });

  it("rejects content beyond an injected item limit without large allocations", () => {
    expectInvalid(request({
      messages: [{
        role: "user",
        content: Array.from({ length: 3 }, () => ({ type: "text", text: "x" })),
      }],
    }), "2 items", { limits: { maxArrayItems: 2 } });
  });

  it("rejects JSON nesting beyond the bounded depth", () => {
    let nested: Record<string, unknown> = { type: "string" };
    for (let index = 0; index < 8; index += 1) {
      nested = { type: "object", properties: { nested } };
    }
    expectInvalid(request({
      tools: [{ name: "Deep", input_schema: nested }],
    }), "depth", { limits: { maxJsonDepth: 4 } });
  });

  it("rejects cyclic and non-JSON-serializable values", () => {
    const cyclicSchema: Record<string, unknown> = { type: "object", properties: {} };
    (cyclicSchema.properties as Record<string, unknown>).self = cyclicSchema;
    expectInvalid(request({
      tools: [{ name: "Cyclic", input_schema: cyclicSchema }],
    }), "cyclic");
    expectInvalid(request({
      tools: [{
        name: "BigInt",
        input_schema: { type: "object", const: 1n },
      }],
    }), "JSON-compatible");
  });

  it("rejects an inbound request beyond an injected byte bound with HTTP 413", () => {
    expectInvalid(request({
      messages: [{ role: "user", content: "x".repeat(256) }],
    }), "inbound request", {
      status: 413,
      errorType: "request_too_large",
      limits: { inboundBytes: 128 },
    });
  });

  it("rejects relay growth beyond its separate injected byte bound with HTTP 413", () => {
    expectInvalid(request(), "relay request", {
      status: 413,
      errorType: "request_too_large",
      limits: { inboundBytes: 4096, relayBytes: 64 },
    });
  });

  it("rejects cumulative media beyond an injected byte bound with HTTP 413", () => {
    expectInvalid(request({
      messages: [{
        role: "user",
        content: [{
          type: "image",
          source: { type: "base64", media_type: "image/png", data: "AAAA" },
        }],
      }],
    }), "media", {
      status: 413,
      errorType: "request_too_large",
      limits: { inboundBytes: 4096, relayBytes: 4096, mediaBytes: 2 },
    });
  });

  it("supports non-streaming max_tokens zero prewarming with adapter context", () => {
    const translated = translate(request({ max_tokens: 0, stream: false }));
    expect(translated.maxTokensZero).toBe(true);
    expect(translated.stream).toBe(false);
    expect(translated.relayBody.max_output_tokens).toBe(1);

    const positive = translate(request({ max_tokens: 1 }));
    expect(positive.maxTokensZero).toBe(false);
    expect(positive.relayBody.max_output_tokens).toBe(1);
  });

  it("allows only automatic or disabled tool choice during zero-token prewarming", () => {
    for (const toolChoice of [{ type: "auto" }, { type: "none" }]) {
      const translated = translate(request({
        max_tokens: 0,
        tools: [{ name: "Read", input_schema: { type: "object" } }],
        tool_choice: toolChoice,
      }));
      expect(translated.maxTokensZero).toBe(true);
      expect(translated.relayBody.max_output_tokens).toBe(1);
    }
  });

  it("rejects structured output during zero-token prewarming", () => {
    expectInvalid(request({
      max_tokens: 0,
      output_config: {
        format: { type: "json_schema", schema: { type: "object" } },
      },
    }), "max_tokens 0");
  });

  it("rejects forced tool choice during zero-token prewarming", () => {
    expectInvalid(request({
      max_tokens: 0,
      tools: [{ name: "Read", input_schema: { type: "object" } }],
      tool_choice: { type: "any" },
    }), "max_tokens 0");
    expectInvalid(request({
      max_tokens: 0,
      tools: [{ name: "Read", input_schema: { type: "object" } }],
      tool_choice: { type: "tool", name: "Read" },
    }), "max_tokens 0");
  });

  it("rejects streaming max_tokens zero", () => {
    expectInvalid(request({ max_tokens: 0, stream: true }), "stream");
  });
});
