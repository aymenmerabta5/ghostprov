import { describe, expect, it } from "bun:test";
import type { SseFrame } from "../src/relay/sse";
import {
  DirectResponsesClient,
  UpstreamExecutionError,
} from "../src/v2/sidecar/upstream";

const FINAL = {
  id: "resp_1",
  object: "response",
  status: "completed",
  output: [{ type: "message", content: [{ type: "output_text", text: "hello" }] }],
};

const FULL_TPM_MESSAGE = "Rate limit reached for gpt-5.6-sol in organization " +
  "org-ExampleOrganization123 on tokens per min (TPM): Limit 40000000, " +
  "Used 40000000, Requested 609930. Please try again in 914ms. Visit " +
  "https://platform.openai.com/account/rate-limits";

function secret(value: string): { reveal(): string } {
  return { reveal: () => value };
}

function sseResponse(
  frames: Array<[string, unknown]>,
  splitEveryBytes = 7,
): Response {
  const bytes = new TextEncoder().encode(frames.map(([event, data]) => (
    `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`
  )).join(""));
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      for (let offset = 0; offset < bytes.length; offset += splitEveryBytes) {
        controller.enqueue(bytes.slice(offset, offset + splitEveryBytes));
      }
      controller.close();
    },
  }), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function completeSse(): Response {
  return sseResponse([
    ["response.created", { type: "response.created", response: { id: "resp_1" } }],
    ["response.output_text.delta", { type: "response.output_text.delta", delta: "hello" }],
    ["response.completed", { type: "response.completed", response: FINAL }],
  ]);
}

describe("DirectResponsesClient", () => {
  it("marks FETCH_STARTING before fetch and streams complete SSE events", async () => {
    const order: string[] = [];
    const bodies: any[] = [];
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1/",
      apiKey: secret("api-secret"),
      fetch: async (_url, init) => {
        order.push("fetch");
        bodies.push(JSON.parse(String(init?.body)));
        expect(new Headers(init?.headers).get("authorization")).toBe("Bearer api-secret");
        return completeSse();
      },
    });
    const events: SseFrame[] = [];
    const body = new TextEncoder().encode(JSON.stringify({
      model: "gpt-5.6-sol",
      input: "hello",
      stream: false,
      reasoning: { effort: "max", mode: "pro" },
      service_tier: "fast",
    }));

    const terminal = await client.execute({
      body,
      beforeFetch: async () => { order.push("fetch_starting"); },
      onPrelude: async prelude => {
        expect(prelude).toMatchObject({ status: 200, contentType: "text/event-stream" });
        order.push("prelude");
      },
      onEvent: async event => { events.push(event); },
    });

    expect(order).toEqual(["fetch_starting", "fetch", "prelude"]);
    expect(bodies).toEqual([{
      model: "gpt-5.6-sol",
      input: "hello",
      stream: true,
      reasoning: { effort: "max", mode: "pro" },
      service_tier: "fast",
    }]);
    expect(events.map(event => event.event)).toEqual([
      "response.created",
      "response.output_text.delta",
      "response.completed",
    ]);
    expect(terminal).toMatchObject({
      responseId: "resp_1",
      status: "completed",
      final: FINAL,
      eventCount: 3,
    });
    expect(terminal.transcriptBytes.byteLength).toBe(terminal.byteCount);
  });

  it("classifies a transport exception after FETCH_STARTING as ambiguous", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => { throw new TypeError("connection reset api-secret"); },
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      throw new Error("expected transport failure");
    } catch (error) {
      expect(error).toBeInstanceOf(UpstreamExecutionError);
      expect(error).toMatchObject({ ambiguous: true, code: "transport" });
      expect(String(error)).not.toContain("api-secret");
    }
  });

  it("preserves a rate-limit problem carried by a fetch transport rejection", async () => {
    const message = `stream disconnected before completion: ${FULL_TPM_MESSAGE}`;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => { throw new Error(message); },
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({
      message,
      ambiguous: false,
      status: 429,
      code: "rate_limit_exceeded",
      rateLimit: {
        retryAfterMs: 914,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
    });
  });

  it("preserves a rate-limit problem carried by a broken response stream", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.error(new Error(FULL_TPM_MESSAGE));
        },
      }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({
      message: FULL_TPM_MESSAGE,
      ambiguous: false,
      status: 429,
      code: "rate_limit_exceeded",
      rateLimit: {
        retryAfterMs: 914,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
    });
  });

  it("converts a streamed error event into a definitive rate-limit failure", async () => {
    const events: SseFrame[] = [];
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => sseResponse([["error", {
        type: "error",
        message: FULL_TPM_MESSAGE,
        code: "rate_limit_exceeded",
      }]]),
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async event => { events.push(event); },
      });
      throw new Error("expected streamed provider error");
    } catch (error) {
      expect(error).toMatchObject({
        message: FULL_TPM_MESSAGE,
        ambiguous: false,
        status: 429,
        code: "rate_limit_exceeded",
        rateLimit: {
          retryAfterMs: 914,
          limitTokens: 40_000_000,
          remainingTokens: 0,
        },
        providerError: {
          version: 1,
          origin: "sse",
          status: 429,
          error: {
            message: FULL_TPM_MESSAGE,
            type: "error",
            param: null,
            code: "rate_limit_exceeded",
          },
          model: "gpt-5.6-sol",
        },
      });
      const providerError = (error as UpstreamExecutionError).providerError!;
      expect(providerError.organizationFingerprint).toMatch(/^[a-f0-9]{64}$/);
      expect(JSON.parse(providerError.payload.text)).toEqual({
        type: "error",
        message: FULL_TPM_MESSAGE,
        code: "rate_limit_exceeded",
      });
    }
    expect(events).toEqual([]);
  });

  it("keeps a plain-text streamed rate-limit payload instead of replacing it", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => new Response(
        `event: error\ndata: ${FULL_TPM_MESSAGE}\n\n`,
        { status: 200, headers: { "content-type": "text/event-stream" } },
      ),
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      throw new Error("expected plain-text provider error");
    } catch (error) {
      expect(error).toMatchObject({
        status: 429,
        code: "rate_limit_exceeded",
        providerError: {
          origin: "sse",
          status: 429,
          payload: {
            format: "text",
            text: FULL_TPM_MESSAGE,
            truncated: false,
          },
        },
      });
    }
  });

  it("preserves bounded safe SSE provider fields including param and status", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => sseResponse([["error", {
        type: "invalid_request_error",
        status: 422,
        message: "Invalid tool schema",
        param: "tools[0].parameters",
        code: "invalid_tool_schema",
        authorization: "Bearer must-not-cross",
      }]]),
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-terra","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      throw new Error("expected streamed provider error");
    } catch (error) {
      expect(error).toMatchObject({
        status: 422,
        code: "invalid_tool_schema",
        providerError: {
          origin: "sse",
          status: 422,
          model: "gpt-5.6-terra",
          error: {
            message: "Invalid tool schema",
            type: "invalid_request_error",
            param: "tools[0].parameters",
            code: "invalid_tool_schema",
          },
        },
      });
      const payload = (error as UpstreamExecutionError).providerError!.payload.text;
      expect(payload).not.toContain("must-not-cross");
      expect(JSON.parse(payload).authorization).toBe("[REDACTED]");
    }
  });

  it("keeps a beforeFetch failure non-ambiguous and never calls fetch", async () => {
    let calls = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => { calls++; return completeSse(); },
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => { throw new Error("journal failed"); },
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ ambiguous: false, code: "before_fetch" });
    expect(calls).toBe(0);
  });

  it("propagates an R2 election loss without wrapping it as a provider outcome", async () => {
    let calls = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => { calls++; return completeSse(); },
    });
    const fence = Object.assign(new Error("election lost"), {
      name: "V2ExecutionFenceError",
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => { throw fence; },
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toBe(fence);
    expect(calls).toBe(0);
  });

  it("returns a definitive sanitized HTTP error without parsing SSE", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => Response.json({
        error: {
          message: `invalid request token=${"x".repeat(80)}`,
          type: "invalid_request_error",
          code: "invalid_value",
        },
      }, { status: 400 }),
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      throw new Error("expected HTTP error");
    } catch (error) {
      expect(error).toMatchObject({ ambiguous: false, code: "invalid_value", status: 400 });
      expect(String(error)).not.toContain("x".repeat(64));
    }
  });

  it("keeps a known HTTP failure definitive when its error body cannot be read", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.error(new Error("broken private error body"));
        },
      }), { status: 502 }),
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({
      ambiguous: false,
      status: 502,
      code: "upstream_error",
      providerError: {
        origin: "http",
        status: 502,
        error: {
          message: "Upstream Responses request failed (502)",
          type: "upstream_error",
          param: null,
          code: null,
        },
      },
    });
  });

  it("captures TPM headers from a 429 for observation-only host telemetry", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => Response.json({
        error: {
          message: "Rate limit reached. Please try again in 109ms.",
          type: "rate_limit_error",
          param: "input",
          code: "rate_limit_exceeded",
        },
        api_key: "must-not-cross",
      }, {
        status: 429,
        headers: {
          "retry-after": "0.109",
          "x-ratelimit-limit-tokens": "40000000",
          "x-ratelimit-remaining-tokens": "0",
          "x-ratelimit-reset-tokens": "6m0s",
          "x-request-id": "req_provider_123",
          "set-cookie": "session=must-not-cross",
        },
      }),
    });

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      throw new Error("expected HTTP provider error");
    } catch (error) {
      expect(error).toMatchObject({
        status: 429,
        code: "rate_limit_exceeded",
        rateLimit: {
          retryAfterMs: 109,
          limitTokens: 40_000_000,
          remainingTokens: 0,
          resetTokensMs: 360_000,
        },
        providerError: {
          version: 1,
          origin: "http",
          status: 429,
          upstreamRequestId: "req_provider_123",
          model: "gpt-5.6-sol",
          organizationFingerprint: null,
          error: {
            message: "Rate limit reached. Please try again in 109ms.",
            type: "rate_limit_error",
            param: "input",
            code: "rate_limit_exceeded",
          },
          headers: {
            "retry-after": "0.109",
            "x-ratelimit-limit-tokens": "40000000",
            "x-ratelimit-remaining-tokens": "0",
            "x-ratelimit-reset-tokens": "6m0s",
          },
        },
      });
      const envelope = (error as UpstreamExecutionError).providerError!;
      expect(envelope.headers).not.toHaveProperty("set-cookie");
      expect(envelope.payload.text).not.toContain("must-not-cross");
      expect(JSON.parse(envelope.payload.text).api_key).toBe("[REDACTED]");
    }
  });

  it("keeps rate-limit headers on a streamed response.failed terminal", async () => {
    const failed = {
      id: "resp_limited",
      object: "response",
      status: "failed",
      error: {
        message: "Rate limit reached for gpt-5.6-sol on tokens per min. Please try again in 42ms.",
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
      },
    };
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => {
        const response = sseResponse([
          ["response.created", { type: "response.created", response: { id: "resp_limited" } }],
          ["response.failed", { type: "response.failed", response: failed }],
        ]);
        for (const [name, value] of Object.entries({
          "x-ratelimit-limit-tokens": "40000000",
          "x-ratelimit-remaining-tokens": "0",
          "x-ratelimit-reset-tokens": "6m0s",
        })) response.headers.set(name, value);
        return response;
      },
    });

    const terminal = await client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    });
    expect(terminal).toMatchObject({
      status: "failed",
      rateLimit: {
        retryAfterMs: 42,
        limitTokens: 40_000_000,
        remainingTokens: 0,
        resetTokensMs: 360_000,
      },
    });
  });

  it("treats a completed stream without terminal proof as ambiguous", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => sseResponse([
        ["response.created", { type: "response.created", response: { id: "resp_1" } }],
      ]),
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ ambiguous: true, code: "missing_terminal" });
  });

  it("bounds an unterminated SSE event", async () => {
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      maxEventBytes: 64,
      fetch: async () => new Response(`event: response.output_text.delta\ndata: ${"x".repeat(128)}`, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ ambiguous: true, code: "event_too_large" });
  });
});
