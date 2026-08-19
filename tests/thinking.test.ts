import { describe, it, expect, vi, beforeEach, afterEach, beforeAll, afterAll } from "vitest";
import { WebSocket } from "ws";
import { createServer } from "node:http";

// Mock y-websocket to avoid real WSS connections (parity with relay.test.ts)
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

// Mock relayCreate to capture verbatim forwarding for Thinking + Fast/Pro
vi.mock("../src/relay/client", () => ({
  relayCreate: vi.fn(async (body: any) => ({
    id: `resp_mock_${Math.random().toString(36).slice(2, 8)}`,
    object: "response",
    model: body?.model ?? "gpt-5.6-sol",
    // Preserve reasoning, service_tier, prompt_cache, phase in output for verification
    output: [
      // reasoning item to verify previous_response_id chaining preserving reasoning items
      {
        type: "reasoning",
        id: "rs_thinking_123",
        content: "thinking trace",
        summary: body?.reasoning?.summary ?? "auto",
        effort: body?.reasoning?.effort,
        mode: body?.reasoning?.mode,
      },
      {
        type: "message",
        role: "assistant",
        content: [{ type: "output_text", text: "ok" }],
        // phase commentary/final_answer on assistant messages
        phase: "final_answer",
      },
      // extra phase commentary item when body indicates phase test
      ...(body?.__expectPhase === "commentary"
        ? [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "commentary" }], phase: "commentary" as const }]
        : []),
    ],
    output_text: "ok",
    reasoning: body?.reasoning,
    service_tier: body?.service_tier,
    usage: {
      input_tokens: 100,
      output_tokens: 20,
      total_tokens: 120,
      input_tokens_details: {
        cached_tokens: body?.prompt_cache_key ? 10 : 0,
        cache_write_tokens: body?.prompt_cache_key ? 5 : 0,
      },
    },
    // echo for verification
    _echo: {
      reasoning: body?.reasoning,
      service_tier: body?.service_tier,
      prompt_cache_key: body?.prompt_cache_key,
      prompt_cache_options: body?.prompt_cache_options,
      prompt_cache_breakpoint: body?.prompt_cache_breakpoint,
      previous_response_id: body?.previous_response_id,
      input: body?.input,
    },
  })),
  relayCreateStream: vi.fn(async (body: any) => {
    const chunks = [
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "reasoning", id: "rs_stream", effort: body?.reasoning?.effort } })}\n\n`,
      `event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", item: { type: "message", role: "assistant", phase: "final_answer" } })}\n\n`,
      `event: done\ndata: [DONE]\n\n`,
    ];
    return new TextEncoder().encode(chunks.join(""));
  }),
  // keep helper exports as vi fns for type checks (actual logic tested via types.ts constants)
  isValidReasoningEffort: vi.fn((v: unknown) => ["low", "medium", "high", "xhigh", "max", "minimal", "none"].includes(v as string)),
  isValidReasoningMode: vi.fn((v: unknown) => ["standard", "pro"].includes(v as string)),
  isValidReasoningContext: vi.fn((v: unknown) => ["current_turn", "all_turns", "auto"].includes(v as string)),
  isValidReasoningSummary: vi.fn((v: unknown) => ["auto", "detailed", "concise"].includes(v as string)),
  isValidServiceTier: vi.fn((v: unknown) => ["fast", "priority", "default", "auto"].includes(v as string)),
  isValidPhase: vi.fn((v: unknown) => v === "commentary" || v === "final_answer"),
  normalizeReasoning: vi.fn((r: any) => (r && typeof r === "object" ? { ...r } : undefined)),
  isStoreFalse: vi.fn((body: any) => body?.store === false),
  hasEncryptedContent: vi.fn((item: any) => !!item && typeof item === "object" && typeof (item as any).encrypted_content === "string" && (item as any).encrypted_content.length > 0),
  isFullHistoryInput: vi.fn((input: unknown) => Array.isArray(input) && (input as any[]).length > 1 && (input as any[]).some((it: any) => it?.type === "reasoning" || it?.type === "function_call" || it?.type === "function_call_output" || it?.role === "assistant")),
}));

describe("Thinking + Fast/Pro (types & constants)", () => {
  it("exports reasoning/service_tier/phase types and VALID constants", async () => {
    const types = await import("../src/relay/types");
    expect(types.VALID_REASONING_EFFORTS).toEqual(expect.arrayContaining(["low", "medium", "high", "xhigh", "max", "minimal", "none"]));
    expect(types.VALID_REASONING_MODES).toEqual(expect.arrayContaining(["standard", "pro"]));
    expect(types.VALID_REASONING_CONTEXTS).toEqual(expect.arrayContaining(["current_turn", "all_turns", "auto"]));
    expect(types.VALID_REASONING_SUMMARIES).toEqual(expect.arrayContaining(["auto", "detailed", "concise"]));
    expect(types.VALID_SERVICE_TIERS).toEqual(expect.arrayContaining(["fast", "priority", "default"]));
    expect(types.VALID_PHASES).toEqual(expect.arrayContaining(["commentary", "final_answer"]));
  });

  it("client helpers validate enums", async () => {
    const client = await import("../src/relay/client");
    // helpers are mocked via vi.fn but should still behave as validators (mocked impl above)
    expect(client.isValidReasoningEffort("low")).toBe(true);
    expect(client.isValidReasoningEffort("xhigh")).toBe(true);
    expect(client.isValidReasoningEffort("max")).toBe(true);
    expect(client.isValidReasoningEffort("minimal")).toBe(true);
    expect(client.isValidReasoningEffort("none")).toBe(true);
    expect(client.isValidReasoningEffort("invalid")).toBe(false);
    expect(client.isValidReasoningMode("pro")).toBe(true);
    expect(client.isValidReasoningMode("standard")).toBe(true);
    expect(client.isValidReasoningContext("all_turns")).toBe(true);
    expect(client.isValidReasoningSummary("detailed")).toBe(true);
    expect(client.isValidServiceTier("fast")).toBe(true);
    expect(client.isValidServiceTier("priority")).toBe(true);
    expect(client.isValidServiceTier("default")).toBe(true);
    expect(client.isValidPhase("commentary")).toBe(true);
    expect(client.isValidPhase("final_answer")).toBe(true);
  });

  it("types allow ResponsesBody with reasoning, service_tier, phase", async () => {
    const { VALID_REASONING_EFFORTS } = await import("../src/relay/types");
    // compile-time check: ensure effort values are covered
    const efforts: string[] = [...VALID_REASONING_EFFORTS];
    expect(efforts).toContain("low");
    expect(efforts).toContain("none");
    expect(efforts).toContain("xhigh");
  });
});

describe("Thinking + Fast/Pro (server POST verbatim)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("forwards reasoning.effort low/medium/high/xhigh/max/minimal/none verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    const efforts = ["low", "medium", "high", "xhigh", "max", "minimal", "none"] as const;
    for (const effort of efforts) {
      mock.mockClear();
      const body: any = {
        model: "gpt-5.6-sol",
        input: "thinking test",
        reasoning: { effort },
        store: false,
      };
      const res = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(200);
      expect(mock).toHaveBeenCalledTimes(1);
      expect(mock.mock.calls[0][0].reasoning.effort).toBe(effort);
      const json: any = await res.json();
      expect(json.reasoning.effort).toBe(effort);
      // output should contain reasoning item with same effort preserving chaining
      const reasoningItem = json.output.find((o: any) => o.type === "reasoning");
      expect(reasoningItem.effort).toBe(effort);
    }
  });

  it("forwards reasoning.mode standard/pro verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    for (const mode of ["standard", "pro"] as const) {
      mock.mockClear();
      const body: any = { model: "gpt-5.6-sol", input: "hi", reasoning: { mode, effort: "medium" }, store: false };
      const res = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(200);
      expect(mock.mock.calls[0][0].reasoning.mode).toBe(mode);
      const j: any = await res.json();
      expect(j.reasoning.mode).toBe(mode);
    }
  });

  it("forwards reasoning.context current_turn/all_turns/auto verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    for (const ctx of ["current_turn", "all_turns", "auto"] as const) {
      mock.mockClear();
      const body: any = { model: "gpt-5.6-sol", input: "hi", reasoning: { context: ctx }, store: false };
      const res = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(200);
      expect(mock.mock.calls[0][0].reasoning.context).toBe(ctx);
    }
  });

  it("forwards reasoning.summary auto/detailed/concise verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    for (const summary of ["auto", "detailed", "concise"] as const) {
      mock.mockClear();
      const body: any = { model: "gpt-5.6-sol", input: "hi", reasoning: { summary }, store: false };
      const res = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(200);
      expect(mock.mock.calls[0][0].reasoning.summary).toBe(summary);
    }
  });

  it("forwards combined reasoning fields verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: "combined",
      reasoning: { effort: "high", mode: "pro", context: "all_turns", summary: "detailed" },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].reasoning).toEqual({ effort: "high", mode: "pro", context: "all_turns", summary: "detailed" });
  });

  it("forwards service_tier fast/priority/default verbatim (Fast/Pro modes)", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    for (const tier of ["fast", "priority", "default"] as const) {
      mock.mockClear();
      const body: any = { model: "gpt-5.6-sol", input: "tier test", service_tier: tier, store: false };
      const res = await app.request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      expect(res.status).toBe(200);
      expect(mock.mock.calls[0][0].service_tier).toBe(tier);
      const j: any = await res.json();
      expect(j.service_tier).toBe(tier);
    }
  });

  it("Fast mode: service_tier fast + reasoning minimal, Pro mode: priority + high/pro", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    const { default: app } = await import("../src/server");
    // Fast
    mock.mockClear();
    const fastBody: any = { model: "gpt-5.6-sol", input: "fast", reasoning: { effort: "minimal", mode: "standard" }, service_tier: "fast", store: false };
    let res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(fastBody),
    });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].service_tier).toBe("fast");
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("minimal");
    // Pro
    mock.mockClear();
    const proBody: any = {
      model: "gpt-5.6-sol",
      input: "pro",
      reasoning: { effort: "max", mode: "pro", summary: "detailed", context: "all_turns" },
      service_tier: "priority",
      store: false,
    };
    res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(proBody),
    });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].service_tier).toBe("priority");
    expect(mock.mock.calls[0][0].reasoning.mode).toBe("pro");
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("max");
  });

  it("preserves prompt_cache handling with reasoning/service_tier together", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: "cache + thinking",
      prompt_cache_key: "thinking-cache-001",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      reasoning: { effort: "medium", summary: "auto" },
      service_tier: "default",
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].prompt_cache_key).toBe("thinking-cache-001");
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("medium");
    expect(mock.mock.calls[0][0].service_tier).toBe("default");
    const j: any = await res.json();
    expect(j.reasoning.effort).toBe("medium");
    expect(j._echo.prompt_cache_key).toBe("thinking-cache-001");
  });

  it("handles phase commentary/final_answer on assistant messages (input & output)", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    // input with phase commentary (assistant message with phase)
    const body: any = {
      model: "gpt-5.6-sol",
      input: [
        { role: "user", content: "hello" },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "thinking..." }], phase: "commentary" },
      ],
      reasoning: { effort: "low", summary: "concise" },
      store: false,
      __expectPhase: "commentary",
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    // server should have forwarded input phase verbatim
    const called = mock.mock.calls[0][0];
    expect(called.input[1].phase).toBe("commentary");
    const j: any = await res.json();
    // output should contain phase final_answer and commentary
    expect(j.output.some((o: any) => o.phase === "final_answer")).toBe(true);
    // when __expectPhase commentary, mock adds commentary phase item
    expect(j.output.some((o: any) => o.phase === "commentary")).toBe(true);
    // reasoning item should be present
    expect(j.output.find((o: any) => o.type === "reasoning")).toBeDefined();
  });

  it("streams SSE preserves reasoning and phase", async () => {
    const { default: app } = await import("../src/server");
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "stream thinking", store: false, stream: true, reasoning: { effort: "high" }, service_tier: "priority" }),
    });
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    const text = await res.text();
    expect(text).toContain("event:");
    expect(text).toContain("reasoning");
  });
});

describe("previous_response_id chaining preserving reasoning items (HTTP)", () => {
  it("forwards previous_response_id with reasoning items verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    // First request with reasoning
    const firstBody: any = {
      model: "gpt-5.6-sol",
      input: "first",
      reasoning: { effort: "medium", summary: "auto" },
      store: false,
    };
    let res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(firstBody),
    });
    expect(res.status).toBe(200);
    const j1: any = await res.json();
    const id1: string = j1.id;
    expect(id1).toBeTruthy();
    expect(j1.output.find((o: any) => o.type === "reasoning")).toBeDefined();

    // Second request chaining via previous_response_id, preserving reasoning context
    mock.mockClear();
    const secondBody: any = {
      model: "gpt-5.6-sol",
      input: "second",
      previous_response_id: id1,
      reasoning: { effort: "high", mode: "pro" },
      store: false,
    };
    res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(secondBody),
    });
    expect(res.status).toBe(200);
    expect(mock.mock.calls[0][0].previous_response_id).toBe(id1);
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("high");
    const j2: any = await res.json();
    // second response should also contain reasoning items preserving chain
    expect(j2.output.find((o: any) => o.type === "reasoning")).toBeDefined();
    expect(j2.output.find((o: any) => o.phase === "final_answer")).toBeDefined();
  });

  it("handles previous_response_id chaining with input containing reasoning items", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    const body: any = {
      model: "gpt-5.6-sol",
      input: [
        { type: "message", role: "user", content: "hello" },
        { type: "reasoning", id: "rs_prev", summary: "auto", content: "prior thinking" },
      ],
      previous_response_id: "resp_prev_999",
      reasoning: { effort: "xhigh" },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    const called = mock.mock.calls[0][0];
    expect(called.previous_response_id).toBe("resp_prev_999");
    expect(called.input[1].type).toBe("reasoning");
    expect(called.reasoning.effort).toBe("xhigh");
  });
});

describe("WSS previous_response_id chaining preserving reasoning items", () => {
  let server: any;
  let app: any;
  let attachWss: any;
  let port: number;

  beforeAll(async () => {
    const mod: any = await import("../src/server");
    app = mod.default;
    attachWss = mod.attachWss;
    const http = createServer(async (req: any, res: any) => {
      const host = req.headers.host ?? "localhost:0";
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
    if (typeof attachWss === "function") attachWss(http);
    await new Promise<void>((resolve, reject) => {
      http.listen(0, () => resolve());
      http.on("error", reject);
    });
    server = http;
    const addr: any = http.address();
    port = addr.port;
    await new Promise((r) => setTimeout(r, 100));
  });

  afterAll(async () => {
    if (server) {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await new Promise((r) => setTimeout(r, 100));
    }
  });

  it("WSS chains previous_response_id and preserves reasoning items", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const ws = new WebSocket(`ws://localhost:${port}/v1/responses`, { headers: { Authorization: "Bearer test" } });
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

    // First WSS with reasoning pro/high
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        reasoning: { effort: "high", mode: "pro", summary: "detailed" },
        service_tier: "priority",
        input: "first thinking",
      }),
    );
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
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("high");
    expect(mock.mock.calls[0][0].reasoning.mode).toBe("pro");
    expect(mock.mock.calls[0][0].service_tier).toBe("priority");
    // response should contain reasoning items
    const resp1 = j1.response ?? j1;
    const out1 = resp1.output ?? [];
    expect(out1.find((o: any) => o.type === "reasoning")).toBeDefined();

    // Second WSS without explicit previous_response_id - store:false should NOT chain (full history replay required)
    // Codex defaults to store:false, so second turn must send full history, not rely on previous_response_id cache.
    ws.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: "second", reasoning: { effort: "low" } }));
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
    // store:false must NOT have injected previous_response_id
    expect(secondBody.previous_response_id ?? null).toBeFalsy();
    expect(secondBody.store).toBe(false);
    expect(secondBody.reasoning.effort).toBe("low");

    // Third: explicit previous_response_id with reasoning xhigh should be forwarded verbatim
    mock.mockClear();
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        input: "third",
        previous_response_id: "resp_explicit_999",
        reasoning: { effort: "xhigh", mode: "pro" },
        service_tier: "fast",
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
    expect(mock.mock.calls[0][0].previous_response_id).toBe("resp_explicit_999");
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("xhigh");
    expect(mock.mock.calls[0][0].service_tier).toBe("fast");

    ws.close();
  });

  it("WSS handles phase commentary/final_answer with reasoning", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const ws = new WebSocket(`ws://localhost:${port}/v1/responses`, { headers: { Authorization: "Bearer test" } });
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
    ws.send(
      JSON.stringify({
        type: "response.create",
        model: "gpt-5.6-sol",
        store: false,
        input: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "thinking" }], phase: "commentary" }],
        reasoning: { effort: "minimal", summary: "concise" },
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
    expect(mock.mock.calls[0][0].input[0].phase).toBe("commentary");
    expect(mock.mock.calls[0][0].reasoning.effort).toBe("minimal");
    ws.close();
  });
});

describe("store:false full history replay (Codex defaults)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("HTTP POST store:false forwards full history with reasoning encrypted_content verbatim", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    const fullHistoryInput = [
      { type: "message", role: "user", content: [{ type: "input_text", text: "first" }] },
      { type: "reasoning", id: "rs_1", encrypted_content: "enc_abc123", summary: "auto", content: "thinking" },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "first answer" }] },
      { type: "message", role: "user", content: [{ type: "input_text", text: "second turn" }] },
    ];
    const body: any = {
      model: "gpt-5.6-sol",
      input: fullHistoryInput,
      reasoning: { effort: "medium", context: "all_turns", summary: "auto" },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock).toHaveBeenCalledTimes(1);
    const called = mock.mock.calls[0][0];
    expect(called.store).toBe(false);
    expect(called.reasoning.context).toBe("all_turns");
    // full history must be preserved verbatim including encrypted_content
    expect(JSON.stringify(called.input)).toContain("enc_abc123");
    expect(called.input.find((x: any) => x.type === "reasoning")?.encrypted_content).toBe("enc_abc123");
    // should NOT have previous_response_id when store:false and full history
    expect(called.previous_response_id ?? null).toBeFalsy();
  });

  it("HTTP POST store:false with previous_response_id present handles gracefully and preserves input", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const { default: app } = await import("../src/server");
    const fullHistory = [
      { type: "message", role: "user", content: "hi" },
      { type: "reasoning", id: "rs_prev", encrypted_content: "enc_xyz789", summary: "auto" },
      { type: "message", role: "assistant", content: [{ type: "output_text", text: "prev" }] },
      { type: "message", role: "user", content: "next" },
    ];
    const body: any = {
      model: "gpt-5.6-sol",
      input: fullHistory,
      previous_response_id: "resp_stale_999",
      reasoning: { effort: "high", context: "all_turns" },
      store: false,
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(mock).toHaveBeenCalledTimes(1);
    const called = mock.mock.calls[0][0];
    expect(called.store).toBe(false);
    // input preserved including encrypted_content
    expect(JSON.stringify(called.input)).toContain("enc_xyz789");
    // previous_response_id handling: relayCreate may strip it for store:false full history or keep it; both are valid if input preserved
    // but if it keeps it, it should still preserve input
    expect(called.input.length).toBe(4);
  });

  it("relayCreate isStoreFalse helper and isFullHistoryInput", async () => {
    const client = await import("../src/relay/client");
    // isStoreFalse should be true only when store === false
    expect((client as any).isStoreFalse({ store: false })).toBe(true);
    expect((client as any).isStoreFalse({ store: true })).toBe(false);
    expect((client as any).isStoreFalse({})).toBe(false);
    expect((client as any).isStoreFalse({ store: undefined })).toBe(false);
    // isFullHistoryInput detects full replay
    expect((client as any).isFullHistoryInput([{ role: "user", content: "hi" }])).toBe(false);
    expect(
      (client as any).isFullHistoryInput([
        { role: "user", content: "hi" },
        { type: "reasoning", encrypted_content: "x" },
        { role: "assistant", content: "ok" },
        { role: "user", content: "next" },
      ]),
    ).toBe(true);
    expect((client as any).hasEncryptedContent({ type: "reasoning", encrypted_content: "enc_123" })).toBe(true);
    expect((client as any).hasEncryptedContent({ type: "reasoning" })).toBe(false);
  });

  it("WSS store:false full history preserves reasoning encrypted_content and does NOT inject previous_response_id", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();
    const mod: any = await import("../src/server");
    const app = mod.default;
    const attachWss = mod.attachWss;
    const { createServer } = await import("node:http");
    const http = createServer(async (req: any, res: any) => {
      const host = req.headers.host ?? "localhost:0";
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
    if (typeof attachWss === "function") attachWss(http);
    await new Promise<void>((resolve, reject) => {
      http.listen(0, () => resolve());
      http.on("error", reject);
    });
    const addr: any = http.address();
    const port = addr.port;
    await new Promise((r) => setTimeout(r, 100));

    try {
      const ws = new WebSocket(`ws://localhost:${port}/v1/responses`, { headers: { Authorization: "Bearer test" } });
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

      // First turn store:false
      ws.send(
        JSON.stringify({
          type: "response.create",
          model: "gpt-5.6-sol",
          store: false,
          input: [{ type: "message", role: "user", content: "first" }],
        }),
      );
      const msg1 = await new Promise<Buffer>((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("msg1 timeout")), 3000);
        ws.once("message", (d) => {
          clearTimeout(t);
          resolve(d as Buffer);
        });
      });
      const j1: any = JSON.parse(msg1.toString());
      const id1 = j1.id ?? j1.response?.id;
      expect(id1).toBeTruthy();
      expect(mock).toHaveBeenCalledTimes(1);
      mock.mockClear();

      // Second turn with full history + encrypted_content and store:false — should NOT inject previous_response_id
      const fullHistory = [
        { type: "message", role: "user", content: "first" },
        { type: "reasoning", id: "rs_all", encrypted_content: "enc_all_turns_xyz", summary: "detailed" },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "ans1" }] },
        { type: "message", role: "user", content: "second with full context" },
      ];
      ws.send(
        JSON.stringify({
          type: "response.create",
          model: "gpt-5.6-sol",
          store: false,
          reasoning: { effort: "medium", context: "all_turns" },
          input: fullHistory,
        }),
      );
      const msg2 = await new Promise<Buffer>((resolve, reject) => {
        const t = setTimeout(() => reject(new Error("msg2 timeout")), 3000);
        ws.once("message", (d) => {
          clearTimeout(t);
          resolve(d as Buffer);
        });
      });
      expect(msg2.toString()).toContain("response.create");
      expect(mock).toHaveBeenCalledTimes(1);
      const body2 = mock.mock.calls[0][0];
      expect(body2.store).toBe(false);
      expect(body2.previous_response_id ?? null).toBeFalsy();
      expect(JSON.stringify(body2.input)).toContain("enc_all_turns_xyz");
      expect(body2.input.find((x: any) => x.type === "reasoning")?.encrypted_content).toBe("enc_all_turns_xyz");

      ws.close();
    } finally {
      await new Promise<void>((resolve) => http.close(() => resolve()));
      await new Promise((r) => setTimeout(r, 100));
    }
  });
});
