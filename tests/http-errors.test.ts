import { describe, expect, it } from "bun:test";
import { ApiError, apiErrorEvent, apiErrorResponse, normalizeApiError } from "../src/http/errors";
import { RelayError } from "../src/relay/run-handle";
import { RouterCapacityTimeoutError, RouterShutdownError } from "../src/router/router";
import {
  ResourceGovernorClosedError,
  ResourceGovernorQueueFullError,
  ResourceGovernorReplacementOverloadError,
  ResourceGovernorTimeoutError,
  ResourceReservationTooLargeError,
} from "../src/router/resource-governor";
import {
  CpuTransformQueueFullError,
  CpuTransformTimeoutError,
} from "../src/router/cpu-transform-pool";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";

describe("OpenAI-compatible HTTP errors", () => {
  it("maps router shutdown admission to a typed service-unavailable response", () => {
    expect(normalizeApiError(new RouterShutdownError())).toEqual({
      status: 503,
      error: {
        message: "GhostProv is shutting down",
        type: "service_unavailable_error",
        param: null,
        code: "router_shutting_down",
      },
    });
  });

  it("maps router capacity timeout to an explicit retryable service error", () => {
    expect(normalizeApiError(new RouterCapacityTimeoutError())).toEqual({
      status: 503,
      error: {
        message: "No sandbox capacity became available before the routing deadline",
        type: "service_unavailable_error",
        param: null,
        code: "router_capacity_timeout",
      },
    });
  });

  it("maps resource admission failures without exposing host accounting", () => {
    const cases = [
      [
        new ResourceGovernorQueueFullError(),
        503,
        "overloaded_error",
        "resource_admission_overloaded",
      ],
      [
        new ResourceGovernorReplacementOverloadError(),
        503,
        "overloaded_error",
        "resource_replacement_overloaded",
      ],
      [
        new ResourceGovernorTimeoutError(),
        503,
        "service_unavailable_error",
        "resource_admission_timeout",
      ],
      [
        new ResourceGovernorClosedError(),
        503,
        "service_unavailable_error",
        "resource_governor_closed",
      ],
      [
        new ResourceReservationTooLargeError(),
        413,
        "invalid_request_error",
        "request_too_large",
      ],
    ] as const;

    for (const [error, status, type, code] of cases) {
      const normalized = normalizeApiError(error);
      expect(normalized).toMatchObject({
        status,
        error: { type, code },
      });
      expect(normalized.error.message).not.toContain("capacity");
      expect(normalized.error.message).not.toContain("bytes");
    }
  });

  it("maps CPU preparation pressure as sanitized local service errors", () => {
    expect(normalizeApiError(new CpuTransformQueueFullError())).toEqual({
      status: 503,
      error: {
        message: "GhostProv request preparation is temporarily overloaded",
        type: "overloaded_error",
        param: null,
        code: "cpu_transform_overloaded",
      },
    });
    expect(normalizeApiError(new CpuTransformTimeoutError(123_456))).toEqual({
      status: 503,
      error: {
        message: "GhostProv request preparation timed out",
        type: "service_unavailable_error",
        param: null,
        code: "cpu_transform_timeout",
      },
    });
  });

  it("preserves typed validation status and fields", async () => {
    const response = apiErrorResponse(new ApiError(400, "prompt is required", {
      param: "prompt",
      code: "missing_required_parameter",
    }));

    expect(response.status).toBe(400);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(await response.json()).toEqual({
      error: {
        message: "prompt is required",
        type: "invalid_request_error",
        param: "prompt",
        code: "missing_required_parameter",
      },
    });
  });

  it("maps relay infrastructure kinds to stable HTTP statuses", () => {
    const cases = [
      ["exhausted", 503],
      ["no-bridge", 503],
      ["timeout", 504],
      ["sync", 503],
      ["render", 502],
      ["upstream", 502],
    ] as const;

    for (const [kind, status] of cases) {
      const normalized = normalizeApiError(Object.assign(new Error(`${kind} failed`), { kind }));
      expect(normalized.status).toBe(status);
      expect(normalized.error.message).toBe(`${kind} failed`);
    }
  });

  it("maps downstream cancellation without reporting an internal failure", () => {
    expect(normalizeApiError(new DOMException("client disconnected", "AbortError"))).toEqual({
      status: 499,
      error: {
        message: "Request was cancelled",
        type: "request_aborted",
        param: null,
        code: "request_aborted",
      },
    });
  });

  it("returns the full organization TPM response and parsed headers", async () => {
    const message = "Rate limit reached for gpt-5.6-sol in organization " +
      "org-ExampleOrganization123 on tokens per min (TPM): Limit 40000000, " +
      "Used 40000000, Requested 609930. Please try again in 914ms. Visit " +
      "https://platform.openai.com/account/rate-limits";
    const response = apiErrorResponse(Object.assign(new Error(message), {
      status: 429,
      code: "rate_limit_exceeded",
      rateLimit: {
        retryAfterMs: 914,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
    }));

    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("1");
    expect(response.headers.get("x-ratelimit-limit-tokens")).toBe("40000000");
    expect(response.headers.get("x-ratelimit-remaining-tokens")).toBe("0");
    expect(await response.json()).toEqual({ error: {
      message,
      type: "rate_limit_error",
      param: null,
      code: "rate_limit_exceeded",
    }});
  });

  it("preserves a bounded HTTP provider response and its selected headers", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 429,
      model: "gpt-5.6-sol",
      headers: new Headers({
        "retry-after": "0.136",
        "x-ratelimit-limit-tokens": "40000000",
        "x-ratelimit-remaining-tokens": "0",
        "x-request-id": "req_upstream_42",
        "set-cookie": "must-not-cross",
      }),
      value: {
        error: {
          message: "Rate limit reached for gpt-5.6-sol in organization org-Test123",
          type: "rate_limit_error",
          param: "input",
          code: "rate_limit_exceeded",
        },
      },
    });
    const error = Object.assign(new Error(providerError.error.message), {
      providerError,
    });
    const normalized = normalizeApiError(error);
    expect(normalized).toMatchObject({
      status: 429,
      error: {
        message: providerError.error.message,
        type: "rate_limit_error",
        param: "input",
        code: "rate_limit_exceeded",
      },
    });

    const response = apiErrorResponse(error);
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("0.136");
    expect(response.headers.get("x-ratelimit-limit-tokens")).toBe("40000000");
    expect(response.headers.get("x-request-id")).toBe("req_upstream_42");
    expect(response.headers.has("set-cookie")).toBe(false);
    expect(await response.json()).toEqual({
      error: {
        message: providerError.error.message,
        type: "rate_limit_error",
        param: "input",
        code: "rate_limit_exceeded",
      },
    });
    expect(apiErrorEvent(error)).toEqual({
      error: {
        message: providerError.error.message,
        type: "rate_limit_error",
        param: "input",
        code: "rate_limit_exceeded",
      },
    });
  });

  it("replays the safe original SSE error data rather than a generic 500", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 422,
      value: {
        type: "invalid_request_error",
        message: "Invalid tool schema",
        param: "tools[0].parameters",
        code: "invalid_tool_schema",
      },
    });
    expect(apiErrorEvent({ providerError })).toEqual({
      type: "invalid_request_error",
      message: "Invalid tool schema",
      param: "tools[0].parameters",
      code: "invalid_tool_schema",
    });
  });

  it("preserves provider type in a nested safe SSE fallback when payload was redacted", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 401,
      value: {
        type: "authentication_error",
        message: "Session expired",
        param: null,
        code: "invalid_api_key",
        authorization: "Bearer must-not-cross",
      },
    });
    expect(providerError.payload.truncated).toBe(true);
    expect(apiErrorEvent({ providerError })).toEqual({
      type: "error",
      error: {
        message: "Session expired",
        type: "authentication_error",
        param: null,
        code: "invalid_api_key",
      },
    });
  });

  it("replays a bounded safe plain-text provider SSE payload", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 429,
      value: {
        type: "rate_limit_error",
        message: "Rate limit reached",
        param: null,
        code: "rate_limit_exceeded",
      },
      payload: "Rate limit reached; authorization=must-not-cross",
    });
    expect(apiErrorEvent({ providerError }))
      .toBe("Rate limit reached; authorization=[REDACTED]");
  });

  it("produces an SSE-safe event from the same normalized error", () => {
    expect(apiErrorEvent(new ApiError(501, "Audio input is unavailable", {
      type: "unsupported_feature",
      code: "unsupported_feature",
    }))).toEqual({
      type: "error",
      message: "Audio input is unavailable",
      param: null,
      code: "unsupported_feature",
    });
  });

  it("does not expose non-error objects or private fields", async () => {
    const response = apiErrorResponse({ gp_ack: "secret", gp_sse: "private" });
    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({ error: {
      message: "Internal server error",
      type: "internal_error",
      param: null,
      code: "internal_error",
    }});
  });

  it("keeps structured upstream details on relay errors", () => {
    const details = { code: "http_400", message: "{}" };
    expect(new RelayError("failed", "upstream", details).details).toBe(details);
  });
});
