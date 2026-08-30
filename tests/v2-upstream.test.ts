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

async function rejectionBeforeCancelTimeout(
  operation: Promise<unknown>,
  message: string,
): Promise<unknown> {
  return await Promise.race([
    operation.then(
      () => { throw new Error("expected upstream rejection"); },
      error => error,
    ),
    Bun.sleep(250).then(() => { throw new Error(message); }),
  ]);
}

function nonSettlingCancelStream(chunks: Uint8Array[] = []): {
  stream: ReadableStream<Uint8Array>;
  cancelCalls(): number;
} {
  let calls = 0;
  return {
    stream: new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(chunk);
      },
      cancel() {
        calls++;
        return new Promise<void>(() => undefined);
      },
    }),
    cancelCalls: () => calls,
  };
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
        expect(init?.body).toBeInstanceOf(Blob);
        body.fill(0);
        const forwarded = await (init?.body as Blob).text();
        expect(forwarded).toBe(canonicalText);
        bodies.push(JSON.parse(forwarded));
        expect(new Headers(init?.headers).get("authorization")).toBe("Bearer api-secret");
        return completeSse();
      },
    });
    const events: SseFrame[] = [];
    let spooledBytes = 0;
    const canonicalText = JSON.stringify({
      model: "gpt-5.6-sol",
      input: "hello",
      stream: true,
      store: true,
      reasoning: { effort: "max", mode: "pro" },
      service_tier: "fast",
      "vendor\\key": { preserved: true },
    });
    const body = new TextEncoder().encode(canonicalText);

    const terminal = await client.execute({
      body,
      bodyIsCanonicalStream: true,
      beforeFetch: async () => { order.push("fetch_starting"); },
      onPrelude: async prelude => {
        expect(prelude).toMatchObject({ status: 200, contentType: "text/event-stream" });
        order.push("prelude");
      },
      onTranscriptBytes: async bytes => { spooledBytes += bytes.byteLength; },
      onEvent: async event => { events.push(event); },
    });

    expect(order).toEqual(["fetch_starting", "fetch", "prelude"]);
    expect(bodies).toEqual([{
      model: "gpt-5.6-sol",
      input: "hello",
      stream: true,
      store: true,
      reasoning: { effort: "max", mode: "pro" },
      service_tier: "fast",
      "vendor\\key": { preserved: true },
    }]);
    expect(events.map(event => event.event)).toEqual([
      "response.created",
      "response.output_text.delta",
    ]);
    expect(terminal.event?.event).toBe("response.completed");
    expect(terminal).toMatchObject({
      responseId: "resp_1",
      status: "completed",
      final: FINAL,
      eventCount: 3,
    });
    expect(spooledBytes).toBe(terminal.byteCount);
    expect("transcriptBytes" in terminal).toBe(false);
  });

  it("normalizes the legacy noncanonical seam to the R8 upstream policy", async () => {
    // Production R8 calls normally set bodyIsCanonicalStream after the host
    // transport has enforced the policy. This fallback remains defensive for
    // compatible custom handles and must preserve every unrelated field.
    let forwarded: Record<string, unknown> | null = null;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1/",
      apiKey: secret("api-secret"),
      fetch: async (_url, init) => {
        expect(typeof init?.body).toBe("string");
        forwarded = JSON.parse(init?.body as string);
        return completeSse();
      },
    });
    const input = {
      model: "gpt-5.6-sol",
      input: "continue",
      stream: false,
      store: false,
      previous_response_id: "resp_parent_exact",
      vendor_extension: { retain: "verbatim" },
    };
    const bytes = new TextEncoder().encode(JSON.stringify(input));

    await client.execute({
      body: bytes,
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    });

    expect(forwarded as unknown).toEqual({ ...input, stream: true, store: true });
    expect(JSON.parse(new TextDecoder().decode(bytes))).toEqual(input);
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

  it("keeps the validated prelude identity when a stream later fails ambiguously", async () => {
    const encoder = new TextEncoder();
    const first = encoder.encode(
      'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_partial"}}\n\n',
    );
    let pull = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("api-secret"),
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        pull(controller) {
          if (pull++ === 0) controller.enqueue(first);
          else controller.error(new Error("socket reset api-secret"));
        },
      }), {
        status: 200,
        headers: {
          "content-type": "text/event-stream",
          "x-request-id": "req_partial_stream",
        },
      }),
    });
    const events: SseFrame[] = [];
    const upstreamRequestIds: Array<string | null> = [];

    try {
      await client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async prelude => {
          upstreamRequestIds.push(prelude.upstreamRequestId);
        },
        onEvent: async event => { events.push(event); },
      });
      throw new Error("expected stream failure");
    } catch (error) {
      expect(error).toBeInstanceOf(UpstreamExecutionError);
      expect(error).toMatchObject({
        message: "Upstream event stream failed after request start",
        ambiguous: true,
        code: "stream",
      });
      expect(String(error)).not.toContain("api-secret");
    }
    expect(upstreamRequestIds).toEqual(["req_partial_stream"]);
    expect(events).toHaveLength(1);
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

  it("does not await a non-settling body cancel for an invalid content type", async () => {
    const source = nonSettlingCancelStream();
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(source.stream, {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "invalid content type waited for non-settling cancel");
    expect(error).toMatchObject({ ambiguous: true, code: "content_type" });
    expect(source.cancelCalls()).toBe(1);
  });

  it("does not await a non-settling body cancel after prelude failure", async () => {
    const source = nonSettlingCancelStream();
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(source.stream, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => { throw new Error("journal failed"); },
      onEvent: async () => {},
    }), "prelude failure waited for non-settling cancel");
    expect(error).toMatchObject({ ambiguous: true, code: "prelude" });
    expect(source.cancelCalls()).toBe(1);
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

  it("returns one definitive invalid_json rejection without replay after FETCH_STARTING", async () => {
    const body = new TextEncoder().encode(JSON.stringify({
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: "large canonical context" }],
      stream: true,
      store: true,
    }));
    let fetches = 0;
    let fetchBoundaries = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async (_url, init) => {
        fetches++;
        expect(init?.body).toBeInstanceOf(Blob);
        return Response.json({
          error: {
            message: "Invalid body: failed to parse JSON value.",
            type: "invalid_request_error",
            code: "invalid_json",
          },
        }, { status: 400 });
      },
    });

    await expect(client.execute({
      body,
      bodyIsCanonicalStream: true,
      beforeFetch: async () => { fetchBoundaries++; },
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ code: "invalid_json", status: 400, ambiguous: false });

    expect(fetches).toBe(1);
    expect(fetchBoundaries).toBe(1);
  });

  it("returns a non-JSON provider rejection without replay", async () => {
    let fetches = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => {
        fetches++;
        return Response.json({
          error: {
            message: "Unknown parameter",
            type: "invalid_request_error",
            code: "unknown_parameter",
          },
        }, { status: 400 });
      },
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      bodyIsCanonicalStream: true,
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ code: "unknown_parameter", status: 400 });
    expect(fetches).toBe(1);
  });

  it("defensively normalizes canonical bytes with false or omitted policy fields", async () => {
    const inputs = [
      {
        model: "gpt-5.6-sol",
        input: "false policy",
        stream: false,
        store: false,
        previous_response_id: "resp_parent_false",
        vendor_extension: { retain: "false-case", stream: true, store: true },
      },
      {
        model: "gpt-5.6-terra",
        input: "omitted policy",
        previous_response_id: "resp_parent_omitted",
        vendor_extension: { retain: "omitted-case" },
      },
    ] as const;
    const forwarded: Record<string, unknown>[] = [];
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async (_url, init) => {
        expect(typeof init?.body).toBe("string");
        forwarded.push(JSON.parse(init?.body as string));
        return completeSse();
      },
    });

    for (const input of inputs) {
      const bytes = new TextEncoder().encode(JSON.stringify(input));
      const original = bytes.slice();
      await client.execute({
        body: bytes,
        bodyIsCanonicalStream: true,
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      expect(bytes).toEqual(original);
    }

    expect(forwarded).toEqual(inputs.map(input => ({
      ...input,
      stream: true,
      store: true,
    })));
  });

  it("falls back before FETCH_STARTING for escaped or duplicate policy keys", async () => {
    const encoded = [
      '{"model":"gpt-5.6-sol","stream":true,"store":true,"\\u0073tore":false,"previous_response_id":"resp_escaped"}',
      '{"model":"gpt-5.6-sol","stream":true,"stream":{"unexpected":true},"store":true,"previous_response_id":"resp_duplicate"}',
    ].map(value => new TextEncoder().encode(value));
    const forwarded: Record<string, unknown>[] = [];
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async (_url, init) => {
        expect(typeof init?.body).toBe("string");
        forwarded.push(JSON.parse(init?.body as string));
        return completeSse();
      },
    });

    for (const body of encoded) {
      const original = body.slice();
      await client.execute({
        body,
        bodyIsCanonicalStream: true,
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      });
      expect(body).toEqual(original);
    }

    expect(forwarded).toEqual([
      {
        model: "gpt-5.6-sol",
        stream: true,
        store: true,
        previous_response_id: "resp_escaped",
      },
      {
        model: "gpt-5.6-sol",
        stream: true,
        store: true,
        previous_response_id: "resp_duplicate",
      },
    ]);
  });

  it("rejects invalid local JSON before FETCH_STARTING or fetch", async () => {
    let fetches = 0;
    let fetchBoundaries = 0;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => {
        fetches++;
        return completeSse();
      },
    });

    await expect(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol"'),
      bodyIsCanonicalStream: true,
      beforeFetch: async () => { fetchBoundaries++; },
      onPrelude: async () => {},
      onEvent: async () => {},
    })).rejects.toMatchObject({ code: "invalid_body", ambiguous: false });
    expect(fetches).toBe(0);
    expect(fetchBoundaries).toBe(0);
  });

  it("clears the total error-body deadline after an early body completion", async () => {
    let clearedTimers = 0;
    const originalClearTimeout = globalThis.clearTimeout;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      httpErrorReadTimeoutMs: 5_000,
      fetch: async () => Response.json({
        error: { message: "bad request", type: "invalid_request_error" },
      }, { status: 400 }),
    });

    globalThis.clearTimeout = ((timer?: ReturnType<typeof setTimeout>) => {
      clearedTimers++;
      return originalClearTimeout(timer);
    }) as typeof clearTimeout;
    let error: unknown;
    try {
      error = await rejectionBeforeCancelTimeout(client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      }), "completed HTTP error body retained its deadline timer");
    } finally {
      globalThis.clearTimeout = originalClearTimeout;
    }
    expect(error).toMatchObject({ ambiguous: false, status: 400 });
    expect(clearedTimers).toBeGreaterThan(0);
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

  it("does not await a non-settling error-body cancel after an oversized first chunk", async () => {
    const source = nonSettlingCancelStream([new Uint8Array(64 * 1024 + 1).fill(0x78)]);
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(source.stream, { status: 502 }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "oversized HTTP error body waited for non-settling cancel");
    expect(error).toMatchObject({ ambiguous: false, status: 502 });
    expect(source.cancelCalls()).toBe(1);
  });

  it("does not await a non-settling error-body cancel after the exact byte cap", async () => {
    const source = nonSettlingCancelStream([
      new Uint8Array(64 * 1024).fill(0x78),
      Uint8Array.of(0x79),
    ]);
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(source.stream, { status: 502 }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "capped HTTP error body waited for non-settling cancel");
    expect(error).toMatchObject({ ambiguous: false, status: 502 });
    expect(source.cancelCalls()).toBe(1);
  });

  it("bounds an HTTP error body that never yields and preserves definitive retry headers", async () => {
    const source = nonSettlingCancelStream();
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      httpErrorReadTimeoutMs: 20,
      fetch: async () => new Response(source.stream, {
        status: 429,
        headers: {
          "retry-after": "0.25",
          "x-ratelimit-limit-tokens": "40000000",
          "x-ratelimit-remaining-tokens": "0",
          "x-request-id": "req_never_yields",
        },
      }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "never-yielding HTTP error body exceeded its total deadline");
    expect(error).toMatchObject({
      ambiguous: false,
      status: 429,
      message: "Upstream Responses request failed (429)",
      rateLimit: {
        retryAfterMs: 250,
        limitTokens: 40_000_000,
        remainingTokens: 0,
      },
      providerError: {
        origin: "http",
        status: 429,
        upstreamRequestId: "req_never_yields",
        headers: {
          "retry-after": "0.25",
          "x-ratelimit-limit-tokens": "40000000",
          "x-ratelimit-remaining-tokens": "0",
        },
      },
    });
    expect(source.cancelCalls()).toBe(1);
  });

  it("uses one total deadline while an HTTP error body keeps yielding slow chunks", async () => {
    let chunks = 0;
    let cancelCalls = 0;
    let interval: ReturnType<typeof setInterval> | null = null;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        interval = setInterval(() => {
          chunks++;
          controller.enqueue(Uint8Array.of(0x78));
        }, 2);
      },
      cancel() {
        cancelCalls++;
        if (interval !== null) clearInterval(interval);
        interval = null;
        return new Promise<void>(() => undefined);
      },
    });
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      httpErrorReadTimeoutMs: 40,
      fetch: async () => new Response(stream, {
        status: 503,
        headers: {
          "retry-after": "0.4",
          "x-request-id": "req_slow_error",
        },
      }),
    });

    let error: unknown;
    try {
      error = await rejectionBeforeCancelTimeout(client.execute({
        body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      }), "slow HTTP error chunks reset the total deadline");
    } finally {
      if (interval !== null) clearInterval(interval);
    }
    expect(chunks).toBeGreaterThan(0);
    expect(cancelCalls).toBe(1);
    expect(error).toMatchObject({
      ambiguous: false,
      status: 503,
      message: "Upstream Responses request failed (503)",
      providerError: {
        origin: "http",
        status: 503,
        upstreamRequestId: "req_slow_error",
        headers: { "retry-after": "0.4" },
      },
    });
  });

  it("stops a definitive HTTP error-body read when the request signal aborts", async () => {
    const source = nonSettlingCancelStream();
    const abort = new AbortController();
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      httpErrorReadTimeoutMs: 5_000,
      fetch: async () => new Response(source.stream, { status: 502 }),
    });
    abort.abort(new DOMException("request ended", "AbortError"));

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      signal: abort.signal,
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "aborted HTTP error-body read waited for its deadline");
    expect(error).toMatchObject({ ambiguous: false, status: 502 });
    expect(source.cancelCalls()).toBe(1);
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

  it("does not await a non-settling reader cancel after a parser failure", async () => {
    const source = nonSettlingCancelStream([
      new TextEncoder().encode("event: .invalid\ndata: x\n\n"),
    ]);
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: secret("key"),
      fetch: async () => new Response(source.stream, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });

    const error = await rejectionBeforeCancelTimeout(client.execute({
      body: new TextEncoder().encode('{"model":"gpt-5.6-sol","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onEvent: async () => {},
    }), "parser failure waited for non-settling cancel");
    expect(error).toMatchObject({ ambiguous: true, code: "invalid_event" });
    expect(source.cancelCalls()).toBe(1);
  });
});
