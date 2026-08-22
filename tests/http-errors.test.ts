import { describe, expect, it } from "bun:test";
import { ApiError, apiErrorEvent, apiErrorResponse, normalizeApiError } from "../src/http/errors";
import { RelayError } from "../src/relay/run-handle";
import { SlotRunnerError } from "../src/relay/slot-runner";

describe("OpenAI-compatible HTTP errors", () => {
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

  it("unwraps daemon http_400 envelopes without relying on instanceof", async () => {
    const upstream = {
      error: {
        message: "Unknown parameter: 'tools[0].n'.",
        type: "invalid_request_error",
        param: "tools[0].n",
        code: "unknown_parameter",
      },
    };
    const daemon = {
      code: "http_400",
      message: JSON.stringify(upstream),
    };
    const error = Object.assign(
      new Error(`upstream failed: ${JSON.stringify(daemon)}`),
      { kind: "upstream" },
    );

    const response = apiErrorResponse(error);

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual(upstream);
  });

  it("prefers structured daemon details when the human message is truncated", async () => {
    const details = {
      code: "http_422",
      message: JSON.stringify({
        error: {
          message: "The request body could not be processed",
          type: "invalid_request_error",
          param: "input",
          code: "unprocessable_entity",
        },
      }),
    };
    const response = apiErrorResponse(Object.assign(
      new Error("upstream failed: {truncated"),
      { kind: "upstream", details },
    ));

    expect(response.status).toBe(422);
    expect(await response.json()).toEqual({ error: {
      message: "The request body could not be processed",
      type: "invalid_request_error",
      param: "input",
      code: "unprocessable_entity",
    }});
  });

  it("maps relay infrastructure kinds to stable HTTP statuses", () => {
    const cases = [
      ["exhausted", 429],
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

  it("maps daemon retry exhaustion to a rate-limit error", async () => {
    const daemon = {
      code: "rate_limited_429",
      message: JSON.stringify({
        error: {
          message: "Rate limit reached",
          type: "rate_limit_error",
          code: "rate_limit_exceeded",
        },
      }),
    };
    const response = apiErrorResponse(Object.assign(
      new Error(`upstream failed: ${JSON.stringify(daemon)}`),
      { kind: "upstream" },
    ));

    expect(response.status).toBe(429);
    expect(await response.json()).toEqual({ error: {
      message: "Rate limit reached",
      type: "rate_limit_error",
      param: null,
      code: "rate_limit_exceeded",
    }});
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

  it("keeps structured upstream details on both relay error implementations", () => {
    const details = { code: "http_400", message: "{}" };
    expect(new RelayError("failed", "upstream", details).details).toBe(details);
    expect(new SlotRunnerError("failed", "upstream", details).details).toBe(details);
  });
});
