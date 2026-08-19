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
    const request = new Request(url, { method: req.method, headers, body: body ? new Uint8Array(body) : undefined });
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

  it("chains previous_response_id per connection (store:true)", async () => {
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

    // First response.create with store:true (legacy behavior - should use previous_response_id cache)
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: true, input: "first" }));
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
    expect(mock.mock.calls[0][0].store).toBe(true);

    // Second response.create without explicit previous_response_id - should chain when store:true
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: true, input: "second" }));
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
    // server should have injected previous_response_id from first response when store:true
    expect(secondBody.previous_response_id).toBe(id1);

    // Third: explicit previous_response_id should be forwarded verbatim with store:true
    mock.mockClear();
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: true,
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

  it("store:false does NOT inject previous_response_id (full history replay)", async () => {
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

    // First with store:false
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "first" }] }] }));
    const msg1 = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg1 timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    const j1: any = JSON.parse(msg1.toString());
    const id1: string = j1.id ?? j1.response?.id;
    expect(id1).toBeTruthy();
    expect(mock).toHaveBeenCalledTimes(1);
    expect(mock.mock.calls[0][0].store).toBe(false);
    expect(mock.mock.calls[0][0].previous_response_id ?? null).toBeFalsy();

    // Second with store:false and full history including reasoning with encrypted_content — should NOT inject previous_response_id
    const fullHistoryInput = [
      { type: "message", role: "user", content: [{ type: "input_text", text: "first" }] },
      { type: "reasoning", id: "rs_123", encrypted_content: "enc_abc123", summary: "thinking" },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "ok" }] },
      { type: "message", role: "user", content: [{ type: "input_text", text: "second" }] },
    ];
    mock.mockClear();
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: fullHistoryInput }));
    const msg2 = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg2 timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    expect(msg2.toString()).toContain("response.create");
    expect(mock).toHaveBeenCalledTimes(1);
    const secondBody = mock.mock.calls[0][0];
    // store:false must NOT have injected previous_response_id even though connection has cached id1
    expect(secondBody.previous_response_id ?? null).toBeFalsy();
    expect(secondBody.store).toBe(false);
    // full history must be forwarded verbatim including encrypted_content
    expect(JSON.stringify(secondBody.input)).toContain("enc_abc123");
    expect(secondBody.input.find((it: any) => it.type === "reasoning")?.encrypted_content).toBe("enc_abc123");

    ws.close();
  });

  it("store:false with previous_response_id present handles gracefully (verbatim forward)", async () => {
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

    // Send store:false with both previous_response_id and full history — should forward verbatim, relayCreate will handle fallback
    const fullHistory = [
      { type: "message", role: "user", content: "hi" },
      { type: "reasoning", id: "rs_prev", encrypted_content: "enc_xyz", summary: "auto" },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "prev answer" }] },
      { type: "message", role: "user", content: "next" },
    ];
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        input: fullHistory,
        previous_response_id: "resp_stale_999",
      }),
    );
    const msg = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    expect(msg.toString()).toContain("response.create");
    expect(mock).toHaveBeenCalledTimes(1);
    const body = mock.mock.calls[0][0];
    expect(body.store).toBe(false);
    // should preserve encrypted_content and full history
    expect(JSON.stringify(body.input)).toContain("enc_xyz");
    // previous_response_id handling: either forwarded verbatim or stripped for fallback — both are valid per task
    // but if forwarded, input must still be preserved
    expect(body.input.length).toBe(4);

    ws.close();
  });

  it("WSS store:false with reasoning encrypted_content preserves all_turns", async () => {
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

    const reasoningWithEncrypted = { type: "reasoning", id: "rs_all_turns", encrypted_content: "enc_all_123", summary: "detailed" };
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        reasoning: { effort: "medium", context: "all_turns", summary: "detailed" },
        input: [
          { type: "message", role: "user", content: "first turn" },
          reasoningWithEncrypted,
          { type: "message", role: "assistant", content: [{ type: "output_text", text: "answer1" }] },
          { type: "message", role: "user", content: "follow up with context" },
        ],
      }),
    );
    const msg = await new Promise<Buffer>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("msg timeout")), 3000);
      ws.once("message", (d) => {
        clearTimeout(t);
        resolve(d as Buffer);
      });
    });
    expect(msg.toString()).toContain("response.create");
    expect(mock).toHaveBeenCalledTimes(1);
    const b = mock.mock.calls[0][0];
    expect(b.store).toBe(false);
    expect(b.reasoning.context).toBe("all_turns");
    expect(JSON.stringify(b.input)).toContain("enc_all_123");
    expect(b.input.find((x: any) => x.type === "reasoning")?.encrypted_content).toBe("enc_all_123");

    ws.close();
  });
});
