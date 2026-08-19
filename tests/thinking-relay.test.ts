import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";

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

describe("relay thinking (mocked unit, verbatim via job.json)", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  const efforts = ["low", "medium", "high", "xhigh", "max", "minimal", "none"] as const;

  for (const effort of efforts) {
    it(`preserves reasoning.effort=${effort} verbatim`, async () => {
      const body: any = {
        model: "gpt-5.6-sol",
        input: "thinking",
        reasoning: { effort },
        store: false,
      };
      const mockResponse = { id: "resp_reasoning", output: [{ type: "reasoning", effort }, { type: "message", role: "assistant", content: "ok", phase: "final_answer" }] };
      const zipBytes = zipSync({
        "job.json": new TextEncoder().encode(JSON.stringify(body)),
        "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
      });
      const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200, headers: { "content-type": "application/json" } });
        if (url.includes("/__bridge/write") && init?.method === "POST") {
          const parsed = JSON.parse(init?.body as string);
          (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/__internal/write-job") && init?.method === "POST") {
          (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
        if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
        if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200, headers: { "content-type": "application/zip" } });
        return new Response("not mocked: " + url, { status: 404 });
      });
      // @ts-ignore
      globalThis.fetch = fetchMock as unknown as typeof fetch;
      const { relayCreate } = await import("../src/relay/client");
      const res: any = await relayCreate(body);
      expect(res.output.find((o: any) => o.type === "reasoning").effort).toBe(effort);
      expect((globalThis as any).__LAST_JOB__.reasoning.effort).toBe(effort);
    });
  }

  it("preserves reasoning.mode standard/pro and context/summary verbatim", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: "hi",
      reasoning: { effort: "high", mode: "pro", context: "all_turns", summary: "detailed" },
      store: false,
    };
    const mockResponse = { id: "resp_combo", output: [{ type: "reasoning" }, { type: "message", phase: "final_answer", content: "ok" }] };
    const zipBytes = zipSync({
      "job.json": new TextEncoder().encode(JSON.stringify(body)),
      "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)),
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect(res).toBeDefined();
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.reasoning.mode).toBe("pro");
    expect(job.reasoning.context).toBe("all_turns");
    expect(job.reasoning.summary).toBe("detailed");
  });

  it("preserves service_tier fast/priority/default verbatim (Fast/Pro)", async () => {
    for (const tier of ["fast", "priority", "default"] as const) {
      const body: any = { model: "gpt-5.6-sol", input: "tier", service_tier: tier, store: false };
      const mockResponse = { id: "resp_tier", output: [{ type: "message", content: "ok", phase: "final_answer" }], service_tier: tier };
      const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
      const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = typeof input === "string" ? input : input.toString();
        if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
        if (url.includes("/__bridge/write") && init?.method === "POST") {
          const parsed = JSON.parse(init?.body as string);
          (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/__internal/write-job") && init?.method === "POST") {
          (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
          return new Response(JSON.stringify({ ok: true }), { status: 200 });
        }
        if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
        if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
        if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
        return new Response("not mocked", { status: 404 });
      });
      // @ts-ignore
      globalThis.fetch = fetchMock as unknown as typeof fetch;
      const { relayCreate } = await import("../src/relay/client");
      const res: any = await relayCreate(body);
      expect(res.service_tier).toBe(tier);
      expect((globalThis as any).__LAST_JOB__.service_tier).toBe(tier);
    }
  });

  it("preserves phase commentary/final_answer on assistant messages", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ role: "user", content: "hi" }, { type: "message", role: "assistant", content: [{ type: "output_text", text: "thinking..." }], phase: "commentary" }],
      reasoning: { effort: "low", summary: "concise" },
      store: false,
    };
    const mockResponse = {
      id: "resp_phase",
      output: [
        { type: "reasoning", id: "rs1" },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "thinking..." }], phase: "commentary" },
        { type: "message", role: "assistant", content: [{ type: "output_text", text: "final" }], phase: "final_answer" },
      ],
    };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect((globalThis as any).__LAST_JOB__.input[1].phase).toBe("commentary");
    expect(res.output.find((o: any) => o.phase === "commentary")).toBeDefined();
    expect(res.output.find((o: any) => o.phase === "final_answer")).toBeDefined();
    expect(res.output.find((o: any) => o.type === "reasoning")).toBeDefined();
  });

  it("preserves previous_response_id chaining with reasoning items", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: [{ type: "reasoning", id: "rs_prev", content: "prior" }, { type: "message", role: "user", content: "next" }],
      previous_response_id: "resp_prev_123",
      reasoning: { effort: "xhigh", mode: "pro" },
      service_tier: "priority",
      store: false,
    };
    const mockResponse = { id: "resp_next", output: [{ type: "reasoning" }, { type: "message", phase: "final_answer", content: "ok" }] };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    const res: any = await relayCreate(body);
    expect((globalThis as any).__LAST_JOB__.previous_response_id).toBe("resp_prev_123");
    expect((globalThis as any).__LAST_JOB__.input[0].type).toBe("reasoning");
    expect((globalThis as any).__LAST_JOB__.reasoning.effort).toBe("xhigh");
    expect((globalThis as any).__LAST_JOB__.service_tier).toBe("priority");
    expect(res.output.find((o: any) => o.type === "reasoning")).toBeDefined();
  });

  it("preserves prompt_cache with reasoning/service_tier together (keep prompt_cache)", async () => {
    const body: any = {
      model: "gpt-5.6-sol",
      input: "combined",
      prompt_cache_key: "keep-cache-42",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      reasoning: { effort: "medium", summary: "auto" },
      service_tier: "fast",
      store: false,
    };
    const mockResponse = { id: "resp_keep", output: [{ type: "reasoning" }, { type: "message", phase: "final_answer", content: "ok" }] };
    const zipBytes = zipSync({ "job.json": new TextEncoder().encode(JSON.stringify(body)), "response.json": new TextEncoder().encode(JSON.stringify(mockResponse)) });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/api/y") && init?.method === "POST") return new Response(JSON.stringify({ token: "mock_y_token" }), { status: 200 });
      if (url.includes("/__bridge/write") && init?.method === "POST") {
        const parsed = JSON.parse(init?.body as string);
        (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/__internal/write-job") && init?.method === "POST") {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      }
      if (url.includes("/s/sandboxes/proxy/render") && url.includes("renderMode=async")) return new Response(JSON.stringify({ status: "rendering" }), { status: 202 });
      if (url.includes("/s/sandboxes/proxy/render-status")) return new Response(JSON.stringify({ status: "complete" }), { status: 200, headers: { "content-type": "application/json" } });
      if (url.includes("/s/sandboxes/proxy/zip")) return new Response(zipBytes as unknown as BodyInit, { status: 200 });
      return new Response("not mocked", { status: 404 });
    });
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");
    await relayCreate(body);
    const job = (globalThis as any).__LAST_JOB__;
    expect(job.prompt_cache_key).toBe("keep-cache-42");
    expect(job.reasoning.effort).toBe("medium");
    expect(job.service_tier).toBe("fast");
  });
});
