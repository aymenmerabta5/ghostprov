import { describe, expect, it } from "bun:test";
import { ApiError } from "../src/http/errors";
import {
  AnthropicRequestError,
  anthropicErrorEvent,
  anthropicErrorResponse,
  anthropicErrorType,
} from "../src/anthropic/errors";
import { resolveAnthropicModel } from "../src/anthropic/model";
import { decodeReasoningItem, encodeReasoningItem } from "../src/anthropic/reasoning";
import type { AnthropicMessage, AnthropicMessageRequest } from "../src/anthropic/types";

const models = {
  opus: "gpt-5.6-sol",
  sonnet: "gpt-5.6-terra",
  haiku: "gpt-5.6-luna",
  fallback: "gpt-5.6-sol",
  passthrough: new Set(["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-5.5"]),
};

describe("Anthropic foundations", () => {
  it("maps current and dated Claude model names by tier", () => {
    expect(resolveAnthropicModel("claude-opus-4-6", models)).toBe("gpt-5.6-sol");
    expect(resolveAnthropicModel("claude-sonnet-4-6-20260801", models)).toBe("gpt-5.6-terra");
    expect(resolveAnthropicModel("claude-haiku-4-5-20251001", models)).toBe("gpt-5.6-luna");
    expect(resolveAnthropicModel("gpt-5.5", models)).toBe("gpt-5.5");
  });

  it("maps tiers case-insensitively and falls back for unknown models", () => {
    expect(resolveAnthropicModel("CLAUDE-OPUS-LATEST", models)).toBe("gpt-5.6-sol");
    expect(resolveAnthropicModel("claude-unknown", models)).toBe("gpt-5.6-sol");
  });

  it("round-trips only a bounded OpenAI reasoning item", () => {
    const item = { type: "reasoning", id: "rs_1", encrypted_content: "cipher", summary: [] } as const;
    const signature = encodeReasoningItem(item);
    expect(signature.startsWith("ghostprov-r1.")).toBe(true);
    expect(decodeReasoningItem(signature)).toEqual(item);
    expect(decodeReasoningItem("anthropic-native-signature")).toBeNull();
    expect(decodeReasoningItem("ghostprov-r1.not-base64")).toBeNull();
  });

  it("strips extra reasoning fields and rejects malformed or oversized envelopes", () => {
    const encoded = Buffer.from(JSON.stringify({
      type: "reasoning",
      id: "rs_2",
      encrypted_content: "cipher-2",
      summary: [{ type: "summary_text", text: "summary" }],
      extra: "discarded",
    }), "utf8").toString("base64url");

    expect(decodeReasoningItem(`ghostprov-r1.${encoded}`)).toEqual({
      type: "reasoning",
      id: "rs_2",
      encrypted_content: "cipher-2",
      summary: [{ type: "summary_text", text: "summary" }],
    });

    for (const value of [
      { type: "message", id: "rs_1", encrypted_content: "cipher", summary: [] },
      { type: "reasoning", id: 1, encrypted_content: "cipher", summary: [] },
      { type: "reasoning", id: "rs_1", encrypted_content: 1, summary: [] },
      { type: "reasoning", id: "rs_1", encrypted_content: "cipher", summary: {} },
    ]) {
      const malformed = Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
      expect(decodeReasoningItem(`ghostprov-r1.${malformed}`)).toBeNull();
    }

    expect(decodeReasoningItem(`ghostprov-r1.${"A".repeat(8 * 1024 * 1024)}`)).toBeNull();
  });

  it("returns current Anthropic error envelopes", async () => {
    const response = anthropicErrorResponse(
      new AnthropicRequestError(400, "messages is required"),
      "req_test",
    );
    expect(response.status).toBe(400);
    expect(response.headers.get("request-id")).toBe("req_test");
    expect(await response.json()).toEqual({
      type: "error",
      error: { type: "invalid_request_error", message: "messages is required" },
      request_id: "req_test",
    });
  });

  it("maps current Anthropic error statuses", () => {
    const cases = [
      [400, "invalid_request_error"],
      [401, "authentication_error"],
      [402, "billing_error"],
      [403, "permission_error"],
      [404, "not_found_error"],
      [413, "request_too_large"],
      [429, "rate_limit_error"],
      [504, "timeout_error"],
      [503, "overloaded_error"],
      [529, "overloaded_error"],
      [500, "api_error"],
    ] as const;

    for (const [status, type] of cases) {
      expect(anthropicErrorType(status)).toBe(type);
    }
  });

  it("normalizes internal errors and maps HTTP 503 to Anthropic 529", async () => {
    const response = anthropicErrorResponse(new ApiError(503, "temporarily unavailable"), "req_busy");
    expect(response.status).toBe(529);
    expect(await response.json()).toEqual({
      type: "error",
      error: { type: "overloaded_error", message: "temporarily unavailable" },
      request_id: "req_busy",
    });
    expect(anthropicErrorEvent(new ApiError(503, "temporarily unavailable"), "req_busy")).toEqual({
      type: "error",
      error: { type: "overloaded_error", message: "temporarily unavailable" },
      request_id: "req_busy",
    });
  });

  it("exposes permissive current request and response wire types", () => {
    const request: AnthropicMessageRequest = {
      model: "claude-sonnet-4-6",
      max_tokens: 128,
      messages: [{ role: "user", content: [{ type: "text", text: "hello", cache_control: {} }] }],
      additive_beta_field: true,
    };
    const message: AnthropicMessage = {
      id: "msg_1",
      type: "message",
      role: "assistant",
      model: request.model,
      content: [{ type: "text", text: "hi" }],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: {
        input_tokens: 1,
        output_tokens: 1,
        cache_creation_input_tokens: null,
        cache_read_input_tokens: null,
      },
      additive_beta_field: true,
    };

    expect(message.model).toBe(request.model);
  });
});
