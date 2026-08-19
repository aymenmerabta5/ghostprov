import { describe, it, expect, vi, beforeAll, afterAll } from "vitest";
import { WebSocket } from "ws";
import { createServer } from "node:http";

// Mock relayCreate for deterministic unit test without live Prism
vi.mock("../src/relay/client", () => ({
  relayCreate: vi.fn(async (body: any) => ({
    id: `resp_mock_${Math.random().toString(36).slice(2, 8)}`,
    object: "response",
    model: body?.model ?? "gpt-5.6-sol",
    output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "ok" }] }],
    output_text: "ok",
    previous_response_id: body?.previous_response_id ?? null,
  })),
  relayCreateStream: vi.fn(async (body: any) => {
    const chunks = [
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "message", role: "assistant" } })}\n\n`,
      `event: done\ndata: [DONE]\n\n`,
    ];
    return new TextEncoder().encode(chunks.join(""));
  }),
}));

vi.mock("y-websocket", () => ({
  WebsocketProvider: class {
    on(_event: string, cb: (v: boolean) => void) {
      if (_event === "sync") setTimeout(() => cb(true), 0);
    }
    destroy() {}
  },
}));

let server: any;
let app: any;
let attachWss: any;

beforeAll(async () => {
  const mod: any = await import("../src/server");
  app = mod.default;
  attachWss = mod.attachWss;
  const http = createServer(async (req: any, res: any) => {
    const host = req.headers.host ?? "localhost:8787";
    const url = `http://${host}${req.url}`;
    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (v) headers.set(k, Array.isArray(v) ? v.join(",") : String(v));
    }
    let body: Buffer | undefined;
    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      if (chunks.length) body = Buffer.concat(chunks);
    }
    const request = new Request(url, { method: req.method, headers, body });
    const response = await app.fetch(request);
    res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    if (response.body) {
      const ab = await response.arrayBuffer();
      res.end(Buffer.from(ab));
    } else {
      res.end();
    }
  });
  if (typeof attachWss === "function") {
    attachWss(http);
  }
  await new Promise<void>((resolve, reject) => {
    http.listen(8787, () => resolve());
    http.on("error", reject);
  });
  server = http;
  // small delay to ensure listening
  await new Promise((r) => setTimeout(r, 100));
});

afterAll(async () => {
  if (server) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    // wait for port release
    await new Promise((r) => setTimeout(r, 100));
  }
});

describe("WSS /v1/responses", () => {
  // Exact code from plan Task 6 Step 1 (verbatim)
  it("WSS incremental", async () => {
    const ws = new WebSocket("ws://localhost:8787/v1/responses", { headers: { Authorization: "Bearer test" } });
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("ws open timeout - WSS not implemented")), 3000);
      ws.on("open", () => {
        clearTimeout(t);
        resolve();
      });
      ws.on("error", (e) => {
        clearTimeout(t);
        reject(e);
      });
    });
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "Find fizz_buzz()" }] }] }));
    const msg = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("ws message timeout")), 3000);
      ws.on("message", (data) => {
        clearTimeout(t);
        resolve(data as Buffer);
      });
      ws.on("error", (e) => {
        clearTimeout(t);
        reject(e);
      });
    });
    expect(msg.toString()).toContain("response.create");
    ws.close();
  });

  it("chains previous_response_id per connection", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const ws = new WebSocket("ws://localhost:8787/v1/responses", { headers: { Authorization: "Bearer test" } });
    await new Promise<void>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("ws open timeout")), 3000);
      ws.on("open", () => {
        clearTimeout(t);
        resolve();
      });
      ws.on("error", (e) => {
        clearTimeout(t);
        reject(e);
      });
    });

    // First response.create
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: "first" }));
    const msg1 = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg1 timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    const j1: any = JSON.parse(msg1.toString());
    const id1: string = j1.id ?? j1.previous_response_id ?? j1.response?.id;
    expect(id1).toBeTruthy();
    expect(mock).toHaveBeenCalledTimes(1);
    expect(mock.mock.calls[0][0].previous_response_id ?? null).toBeFalsy();

    // Second response.create without explicit previous_response_id - should chain
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: "second" }));
    const msg2 = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg2 timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    expect(msg2.toString()).toContain("response.create");
    expect(mock).toHaveBeenCalledTimes(2);
    const secondBody = mock.mock.calls[1][0];
    // server should have injected previous_response_id from first response
    expect(secondBody.previous_response_id).toBe(id1);

    // Third: explicit previous_response_id should be forwarded verbatim
    mock.mockClear();
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        input: "third",
        previous_response_id: "resp_explicit_123",
      }),
    );
    const msg3 = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg3 timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    expect(msg3.toString()).toContain("response.create");
    expect(mock).toHaveBeenCalledTimes(1);
    expect(mock.mock.calls[0][0].previous_response_id).toBe("resp_explicit_123");

    ws.close();
  });
});
