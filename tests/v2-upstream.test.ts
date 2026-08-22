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
      onEvent: async event => { events.push(event); },
    });

    expect(order).toEqual(["fetch_starting", "fetch"]);
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
        onEvent: async () => {},
      });
      throw new Error("expected transport failure");
    } catch (error) {
      expect(error).toBeInstanceOf(UpstreamExecutionError);
      expect(error).toMatchObject({ ambiguous: true, code: "transport" });
      expect(String(error)).not.toContain("api-secret");
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
      onEvent: async () => {},
    })).rejects.toMatchObject({ ambiguous: false, code: "before_fetch" });
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
        onEvent: async () => {},
      });
      throw new Error("expected HTTP error");
    } catch (error) {
      expect(error).toMatchObject({ ambiguous: false, code: "invalid_value", status: 400 });
      expect(String(error)).not.toContain("x".repeat(64));
    }
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
      onEvent: async () => {},
    })).rejects.toMatchObject({ ambiguous: true, code: "event_too_large" });
  });
});
