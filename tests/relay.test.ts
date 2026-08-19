import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";

// Mock y-websocket to avoid real WSS connections in unit tests (same as proxy.test.ts)
vi.mock("y-websocket", () => {
  return {
    WebsocketProvider: class {
      on(event: string, cb: (v: boolean) => void) {
        if (event === "sync") setTimeout(() => cb(true), 0);
      }
      destroy() {}
    },
  };
});

// Plan's original interface test — live integration (requires Prism credentials + proxy:18080)
// Skipped by default when PRISM_DOC_ID / PRISM_SANDBOX_TOKEN not set.
describe("relay (live, plan)", () => {
  const hasCreds = !!process.env.PRISM_DOC_ID && !!process.env.PRISM_SANDBOX_TOKEN;
  const live = hasCreds ? it : it.skip;
  live("forwards tool definition and returns function_call", async () => {
    const { relayCreate } = await import("../src/relay/client");
    const body = {
      model: "gpt-5.6-sol",
      input: "Use get_horoscope for Aquarius",
      tools: [
        {
          type: "function",
          name: "get_horoscope",
          parameters: { type: "object", properties: { sign: { type: "string" } }, required: ["sign"] },
        },
      ],
      store: false,
    };
    const res: any = await relayCreate(body);
    const fc = res.output.find((o: any) => o.type === "function_call");
    expect(fc.name).toBe("get_horoscope");
  });
});

describe("relay (mocked unit)", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("forwards tool definition and returns function_call (mocked)", async () => {
    const body = {
      model: "gpt-5.6-sol",
      input: "Use get_horoscope for Aquarius",
      tools: [
        {
          type: "function",
          name: "get_horoscope",
          parameters: { type: "object", properties: { sign: { type: "string" } }, required: ["sign"] },
        },
      ],
      store: false,
    };

    const mockResponse = {
      id: "resp_mock",
      output: [{ type: "function_call", name: "get_horoscope", arguments: '{"sign":"Aquarius"}', call_id: "call_123" }],
      output_text: "",
    };

    // Prepare zip containing job.json (echo of body) and response.json (mockResponse)
    // fetchZip will return this zip when triggered via utils/zip
    const zipBytes = zipSync({
      "job.json": new TextEncoder().encode(JSON.stringify(body)),
      "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();

      if (url.includes("/api/y") && init?.method === "POST") {
        return new Response(JSON.stringify({ token: "mock_y_token" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) {
        return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      }
      if (url.includes("/s/sandboxes/proxy/render-status")) {
        return new Response(JSON.stringify({ status: "complete" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (url.includes("/s/sandboxes/proxy/zip")) {
        return new Response(zipBytes as unknown as BodyInit, {
          status: 200,
          headers: { "content-type": "application/zip" },
        });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });

    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    const fc = res.output.find((o: any) => o.type === "function_call");
    expect(fc.name).toBe("get_horoscope");
    // also verify passthrough: job.json preserved tools verbatim
    expect((globalThis as any).__LAST_JOB__.tools[0].name).toBe("get_horoscope");
  });

  it("preserves prompt_cache_key, tool_choice, previous_response_id, reasoning, instructions verbatim (mocked)", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: "hello",
      tools: [{ type: "function", name: "get_horoscope", parameters: { type: "object", properties: { sign: { type: "string" } } } }],
      tool_choice: { type: "function", name: "get_horoscope" },
      previous_response_id: "resp_prev_123",
      prompt_cache_key: "my-cache-key-42",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      reasoning: { effort: "medium" },
      instructions: "You are helpful",
      text: { format: { type: "text" } },
      store: false,
    };

    const mockResponse = { id: "resp_mock2", output: [{ type: "message", content: "ok" }], usage: {} };

    const zipBytes = zipSync({
      "job.json": new TextEncoder().encode(JSON.stringify(body)),
      "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") {
        return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) {
        return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      }
      if (url.includes("/s/sandboxes/proxy/render-status")) {
        return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/s/sandboxes/proxy/zip")) {
        return new Response(zipBytes as unknown as BodyInit, { status: 200, headers: { "content-type": "application/zip" } });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });

    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_mock2");

    const job = (globalThis as any).__LAST_JOB__;
    expect(job.tool_choice.name).toBe("get_horoscope");
    expect(job.previous_response_id).toBe("resp_prev_123");
    expect(job.prompt_cache_key).toBe("my-cache-key-42");
    expect(job.prompt_cache_options.mode).toBe("explicit");
    expect(job.reasoning.effort).toBe("medium");
    expect(job.instructions).toBe("You are helpful");
    expect(job.tools[0].name).toBe("get_horoscope");
  });

  it("preserves input_image data URLs via input passthrough (mocked)", async () => {
    const oneByOnePng = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+ip1sAAAAASUVORK5CYII=";
    const body: any = {
      model: "gpt-5.6-sol",
      input: [
        {
          role: "user",
          content: [
            { type: "input_text", text: "what is in this image" },
            { type: "input_image", image_url: oneByOnePng, detail: "auto" },
          ],
        },
      ],
      store: false,
    };

    const mockResponse = { id: "resp_img", output: [{ type: "message", content: "image ok" }] };
    const zipBytes = zipSync({
      "job.json": new TextEncoder().encode(JSON.stringify(body)),
      "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") {
        return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) {
        return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      }
      if (url.includes("/s/sandboxes/proxy/render-status")) {
        return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      }
      if (url.includes("/s/sandboxes/proxy/zip")) {
        return new Response(zipBytes as unknown as BodyInit, { status: 200, headers: { "content-type": "application/zip" } });
      }
      return new Response("not mocked: " + url, { status: 404 });
    });

    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res.id).toBe("resp_img");
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.input[0].content[1].image_url).toBe(oneByOnePng);
  });

  it("relayCreate and types expose correct interfaces (mocked)", async () => {
    const relayMod = await import("../src/relay/client");
    expect(typeof relayMod.relayCreate).toBe("function");

    const typesMod = await import("../src/relay/types");
    // ResponsesBody type exists at runtime as object check is compile-time only,
    // but we verify the module loads and exports are present in built artifacts
    expect(typesMod).toBeDefined();
  });
});
