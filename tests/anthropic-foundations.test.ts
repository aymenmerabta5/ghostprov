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
import type {
  AnthropicAdapterStopReason,
  AnthropicMessage,
  AnthropicMessageRequest,
  AnthropicStopReason,
  AnthropicUsage,
} from "../src/anthropic/types";

const currentStopReasons: AnthropicStopReason[] = [
  "end_turn",
  "max_tokens",
  "stop_sequence",
  "tool_use",
  "pause_turn",
  "compaction",
  "refusal",
  "model_context_window_exceeded",
];
const adapterStopReasons: AnthropicAdapterStopReason[] = [
  "end_turn",
  "max_tokens",
  "stop_sequence",
  "tool_use",
  "refusal",
];
// @ts-expect-error The adapter never emits upstream-only pause turns.
const invalidAdapterStopReason: AnthropicAdapterStopReason = "pause_turn";
// @ts-expect-error Current outgoing usage requires every nullable/detail field explicitly.
const incompleteOutgoingUsage: AnthropicUsage = {
  input_tokens: 1,
  output_tokens: 1,
  cache_creation_input_tokens: 0,
  cache_read_input_tokens: 0,
};

void currentStopReasons;
void adapterStopReasons;
void invalidAdapterStopReason;
void incompleteOutgoingUsage;

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

  it("round-trips only encrypted reasoning continuity without summary plaintext", () => {
    const item = {
      type: "reasoning",
      id: "rs_1",
      encrypted_content: "cipher",
      summary: [{ type: "summary_text", text: "private summary" }],
    } as const;
    const signature = encodeReasoningItem(item);
    expect(signature.startsWith("ghostprov-r1.")).toBe(true);
    const rawEnvelope = Buffer.from(signature.slice("ghostprov-r1.".length), "base64url")
      .toString("utf8");
    expect(JSON.parse(rawEnvelope)).toEqual({ id: "rs_1", encrypted_content: "cipher" });
    expect(rawEnvelope).not.toContain("private summary");
    expect(decodeReasoningItem(signature)).toEqual({
      type: "reasoning",
      id: "rs_1",
      encrypted_content: "cipher",
      summary: [],
    });
    expect(decodeReasoningItem("anthropic-native-signature")).toBeNull();
    expect(decodeReasoningItem("ghostprov-r1.not-base64")).toBeNull();
    expect(decodeReasoningItem("ghostprov-r1.%%%")).toBeNull();
  });

  it("rejects extra, malformed, noncanonical, or oversized reasoning envelopes", () => {
    const encoded = Buffer.from(JSON.stringify({
      id: "rs_2",
      encrypted_content: "cipher-2",
    }), "utf8").toString("base64url");

    expect(decodeReasoningItem(`ghostprov-r1.${encoded}`)).toEqual({
      type: "reasoning",
      id: "rs_2",
      encrypted_content: "cipher-2",
      summary: [],
    });

    for (const value of [
      { id: "rs_1", encrypted_content: "cipher", summary: [] },
      { type: "reasoning", id: "rs_1", encrypted_content: "cipher" },
      { id: 1, encrypted_content: "cipher" },
      { id: "", encrypted_content: "cipher" },
      { id: "rs_1", encrypted_content: 1 },
      { id: "rs_1", encrypted_content: "" },
    ]) {
      const malformed = Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
      expect(decodeReasoningItem(`ghostprov-r1.${malformed}`)).toBeNull();
    }

    const duplicateKeys = Buffer.from(
      '{"id":"rs_first","id":"rs_second","encrypted_content":"cipher"}',
      "utf8",
    ).toString("base64url");
    expect(decodeReasoningItem(`ghostprov-r1.${duplicateKeys}`)).toBeNull();
    expect(decodeReasoningItem(`ghostprov-r1.${encoded}=`)).toBeNull();
    expect(decodeReasoningItem(`ghostprov-r1.${"A".repeat(8 * 1024 * 1024)}`)).toBeNull();
    expect(() => encodeReasoningItem({
      type: "reasoning",
      id: 1,
      encrypted_content: "cipher",
      summary: [],
    } as unknown as Parameters<typeof encodeReasoningItem>[0])).toThrow();
  });

  it("preflights reasoning envelopes by decoded bytes rather than encoded characters", () => {
    const encryptedContent = "x".repeat(6 * 1024 * 1024);
    const item = {
      type: "reasoning",
      id: "rs_large_valid",
      encrypted_content: encryptedContent,
      summary: [],
    } as const;

    const signature = encodeReasoningItem(item);
    expect(signature.length).toBeGreaterThan(8 * 1024 * 1024);
    expect(decodeReasoningItem(signature)).toEqual(item);

    const oversizedJson = JSON.stringify({
      id: "rs_too_large",
      encrypted_content: "x".repeat(8 * 1024 * 1024),
    });
    const oversized = `ghostprov-r1.${Buffer.from(oversizedJson, "utf8").toString("base64url")}`;
    expect(decodeReasoningItem(oversized)).toBeNull();
  });

  it("measures the reasoning source envelope before JSON serialization", () => {
    const originalStringify = JSON.stringify;
    let stringifyCalled = false;
    let thrown: unknown;
    Object.defineProperty(JSON, "stringify", {
      configurable: true,
      value: (...args: Parameters<typeof JSON.stringify>) => {
        stringifyCalled = true;
        return originalStringify(...args);
      },
    });
    try {
      encodeReasoningItem({
        type: "reasoning",
        id: "rs_source_bound",
        encrypted_content: "x".repeat(8 * 1024 * 1024),
        summary: [],
      });
    } catch (error) {
      thrown = error;
    } finally {
      Object.defineProperty(JSON, "stringify", {
        configurable: true,
        value: originalStringify,
      });
    }

    expect(thrown).toBeInstanceOf(RangeError);
    expect(stringifyCalled).toBe(false);
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
      [409, "conflict_error"],
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
      error: { type: "overloaded_error", message: "Service temporarily overloaded" },
      request_id: "req_busy",
    });
    expect(anthropicErrorEvent(new ApiError(503, "temporarily unavailable"), "req_busy")).toEqual({
      type: "error",
      error: { type: "overloaded_error", message: "Service temporarily overloaded" },
      request_id: "req_busy",
    });
  });

  it("sanitizes untrusted statuses/messages and preserves only safe retry headers", async () => {
    const cases = [
      [new ApiError(502, "secret gateway response"), 500, "api_error", "Internal server error"],
      [new ApiError(503, "secret capacity detail"), 529, "overloaded_error", "Service temporarily overloaded"],
      [new ApiError(504, "secret timeout detail"), 504, "timeout_error", "Request timed out"],
    ] as const;
    for (const [error, status, type, message] of cases) {
      const response = anthropicErrorResponse(error, "req_safe");
      expect(response.status).toBe(status);
      expect(await response.json()).toEqual({
        type: "error",
        error: { type, message },
        request_id: "req_safe",
      });
    }

    const retryable = Object.assign(new ApiError(429, "secret quota"), {
      headers: {
        "Retry-After": "12",
        "X-Should-Retry": "false",
        "set-cookie": "secret=1",
      },
    });
    const response = anthropicErrorResponse(retryable, "req_retry");
    expect(response.headers.get("retry-after")).toBe("12");
    expect(response.headers.get("x-should-retry")).toBe("false");
    expect(response.headers.get("set-cookie")).toBeNull();
    expect(JSON.stringify(await response.json())).not.toContain("secret quota");
  });

  it("exposes permissive current request and response wire types", () => {
    const usage: AnthropicUsage = {
      input_tokens: 10,
      output_tokens: 5,
      cache_creation_input_tokens: 3,
      cache_read_input_tokens: 2,
      cache_creation: {
        ephemeral_1h_input_tokens: 1,
        ephemeral_5m_input_tokens: 2,
      },
      inference_geo: "global",
      output_tokens_details: { thinking_tokens: 3 },
      server_tool_use: { web_fetch_requests: 1, web_search_requests: 2 },
      service_tier: "priority",
      speed: "fast",
      iterations: [{
        type: "message",
        model: "claude-sonnet-4-6",
        input_tokens: 10,
        output_tokens: 5,
        cache_creation_input_tokens: 3,
        cache_read_input_tokens: 2,
        cache_creation: null,
      }],
      future_usage_field: { accepted: true },
    };
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
      content: [{ type: "text", text: "hi", citations: null }],
      container: null,
      stop_details: null,
      stop_reason: "end_turn",
      stop_sequence: null,
      usage,
      additive_beta_field: true,
    };

    expect(message.model).toBe(request.model);
    expect(usage.cache_creation?.ephemeral_1h_input_tokens).toBe(1);
    expect(usage.iterations?.[0]?.type).toBe("message");
    expect(usage.future_usage_field).toEqual({ accepted: true });
  });
});
