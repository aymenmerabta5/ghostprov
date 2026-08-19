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
});
