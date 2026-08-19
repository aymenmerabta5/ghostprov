import { describe, it, expect, vi } from "vitest";
import app from "../src/server";

vi.mock("../src/relay/client", () => ({
  relayCreate: vi.fn(async (body: any) => ({
    id: "resp_mock_123",
    object: "response",
    model: body?.model ?? "gpt-5.6-sol",
    output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "PING" }] }],
    output_text: "PING",
  })),
  relayCreateStream: vi.fn(async (body: any) => {
    // Minimal synthetic SSE payload that satisfies "event:" check; plan requires
    // response.output_item.added, response.function_call_arguments.delta etc.
    const chunks = [
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "message", role: "assistant" } })}\n\n`,
      `event: response.function_call_arguments.delta\ndata: ${JSON.stringify({ type: "response.function_call_arguments.delta", delta: "{}" })}\n\n`,
      `event: response.function_call_arguments.done\ndata: ${JSON.stringify({ type: "response.function_call_arguments.done" })}\n\n`,
      `event: done\ndata: [DONE]\n\n`,
    ];
    return new TextEncoder().encode(chunks.join(""));
  }),
}));

// y-websocket mock is not required when relayCreate is mocked, but keeps parity with proxy/relay tests
vi.mock("y-websocket", () => {
  return {
    WebsocketProvider: class {
      on(_event: string, cb: (v: boolean) => void) {
        if (_event === "sync") setTimeout(() => cb(true), 0);
      }
      destroy() {}
    },
  };
});

describe("health", () => {
  it("GET /health returns ok", async () => {
    const res = await app.request("/health");
    expect(res.status).toBe(200);
    const j = await res.json();
    expect(j.ok).toBe(true);
  });
});

describe("responses", () => {
  it("POST /v1/responses proxies to relay", async () => {
    const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "gpt-5.6-sol", input: "Reply with exactly: PING", store: false }) });
    expect(res.status).toBe(200);
    const j:any = await res.json();
    expect(j.output_text || JSON.stringify(j)).toContain("PING");
  });
  it("streams SSE", async () => {
    const res = await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ model: "gpt-5.6-sol", input: "Hi", stream: true, store: false }) });
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    const text = await res.text();
    expect(text).toContain("event:");
  });
});
