import { describe, expect, it } from "bun:test";
import { AnthropicRequestError } from "../src/anthropic/errors";
import { estimateAnthropicInputTokens } from "../src/anthropic/token-count";

describe("Anthropic deterministic input token estimation", () => {
  it("counts text, tools, images, documents, and overhead deterministically", () => {
    const request = {
      model: "claude-sonnet-4-6",
      messages: [{
        role: "user",
        content: [
          { type: "text", text: "hello" },
          {
            type: "image",
            source: {
              type: "base64",
              media_type: "image/png",
              data: "A".repeat(4096),
            },
          },
          {
            type: "document",
            source: {
              type: "text",
              media_type: "text/plain",
              data: "document text",
            },
          },
        ],
      }],
      system: [{ type: "text", text: "Be concise." }],
      tools: [{
        name: "Read",
        description: "Read a file",
        input_schema: { type: "object", properties: { path: { type: "string" } } },
      }],
    };

    const first = estimateAnthropicInputTokens(request);
    expect(first).toBeGreaterThan(100);
    expect(estimateAnthropicInputTokens(structuredClone(request))).toBe(first);
    expect(first).toBeGreaterThan(estimateAnthropicInputTokens({
      model: request.model,
      messages: [{ role: "user", content: "hello" }],
    }));
  });

  it("uses UTF-8 bytes at 3.5 bytes per token and always returns at least one", () => {
    expect(estimateAnthropicInputTokens("")).toBe(1);
    expect(estimateAnthropicInputTokens("abc")).toBe(1);
    expect(estimateAnthropicInputTokens("éé")).toBe(2);
    expect(estimateAnthropicInputTokens("🚀🚀")).toBe(3);
  });

  it("caps each decoded base64 media contribution without allocating decoded media", () => {
    const data = "A".repeat(8 * 1024 * 1024);
    const media = {
      type: "image",
      source: { type: "base64", media_type: "image/png", data },
    };
    const one = estimateAnthropicInputTokens({
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: [media] }],
    });
    const two = estimateAnthropicInputTokens({
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: [media, structuredClone(media)] }],
    });

    expect(one).toBeGreaterThan(999_900);
    expect(one).toBeLessThan(1_000_200);
    expect(two).toBeGreaterThan(1_999_800);
    expect(two).toBeLessThan(2_000_300);
  });

  it("rejects cyclic, over-depth, non-JSON, and over-32-MiB inputs", () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => estimateAnthropicInputTokens(cyclic)).toThrow(/cyclic/i);

    let deep: Record<string, unknown> = {};
    const root = deep;
    for (let index = 0; index < 65; index += 1) {
      const child: Record<string, unknown> = {};
      deep.child = child;
      deep = child;
    }
    expect(() => estimateAnthropicInputTokens(root)).toThrow(/maximum JSON depth of 64/i);
    expect(() => estimateAnthropicInputTokens({ value: Number.NaN })).toThrow(/JSON-compatible/i);

    try {
      estimateAnthropicInputTokens({ text: "x".repeat(32 * 1024 * 1024) });
      throw new Error("expected the input bound to reject");
    } catch (error) {
      expect(error).toBeInstanceOf(AnthropicRequestError);
      expect((error as AnthropicRequestError).status).toBe(413);
      expect((error as AnthropicRequestError).errorType).toBe("request_too_large");
    }
  });
});
