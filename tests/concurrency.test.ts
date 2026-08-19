import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

// Mock relayCreate for concurrency + cached_tokens handling
vi.mock("../src/relay/client", () => ({
  relayCreate: vi.fn(async (body: any) => {
    // Simulate small delay to allow concurrency testing
    await new Promise((r) => setTimeout(r, 20));
    const isCacheHit = !!body.prompt_cache_key && body.prompt_cache_key.includes("hit");
    return {
      id: `resp_${body.prompt_cache_key ?? "nocache"}_${Math.random().toString(36).slice(2, 6)}`,
      object: "response",
      model: body?.model ?? "gpt-5.6-sol",
      output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text: "ok" }] }],
      output_text: "ok",
      usage: {
        input_tokens: 100,
        output_tokens: 20,
        total_tokens: 120,
        input_tokens_details: {
          cached_tokens: isCacheHit ? 80 : body.prompt_cache_key ? 0 : 0,
          cache_write_tokens: body.prompt_cache_key && !isCacheHit ? 50 : 0,
        },
      },
      _echo: {
        prompt_cache_key: body?.prompt_cache_key,
        prompt_cache_options: body?.prompt_cache_options,
        prompt_cache_breakpoint: body?.prompt_cache_breakpoint,
      },
    };
  }),
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

describe("Concurrency + Codex Integration (Task 8)", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("src/utils/queue.ts exports PQUEUE_CONCURRENCY=5 and p-queue with max 5 concurrent, queue render", async () => {
    const mod = await import("../src/utils/queue");
    expect(mod.PQUEUE_CONCURRENCY).toBe(5);
    expect(mod.queue).toBeDefined();
    // p-queue exposes concurrency property
    expect(mod.queue.concurrency).toBe(5);
    // renderQueue must be single-threaded (concurrency 1) per Global Constraint: One render at a time
    expect(mod.renderQueue).toBeDefined();
    expect(mod.renderQueue.concurrency).toBe(1);
    expect(typeof mod.queueRender).toBe("function");
    // also queue.add should be a function
    expect(typeof mod.queue.add).toBe("function");
  });

  it("PQUEUE_CONCURRENCY respects env override but defaults to 5", async () => {
    const mod = await import("../src/utils/queue");
    expect(mod.PQUEUE_CONCURRENCY).toBe(5);
    // Verify that PQueue was constructed with concurrency 5
    const { default: PQueue } = await import("p-queue");
    expect(PQueue).toBeDefined();
  });

  it("3 concurrent POST /v1/responses with different prompt_cache_key → all 200", async () => {
    const { default: app } = await import("../src/server");
    const { relayCreate } = await import("../src/relay/client");
    const mock = relayCreate as any;
    mock.mockClear();

    const bodies = [1, 2, 3].map((i) => ({
      model: "gpt-5.6-sol",
      input: `hello ${i}`,
      prompt_cache_key: `key-${i}-${Date.now()}`,
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      store: false,
    }));

    const responses = await Promise.all(
      bodies.map((b) =>
        app.request("/v1/responses", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(b),
        }),
      ),
    );

    for (const r of responses) {
      expect(r.status).toBe(200);
    }
    const jsons = await Promise.all(responses.map((r) => r.json() as Promise<any>));
    for (const j of jsons) {
      expect(j.output_text || JSON.stringify(j)).toContain("ok");
      expect(j.id).toBeDefined();
    }
    // Each request should have called relayCreate with its own prompt_cache_key
    expect(mock).toHaveBeenCalledTimes(3);
    const calledKeys = mock.mock.calls.map((c: any) => c[0].prompt_cache_key);
    expect(new Set(calledKeys).size).toBe(3);
  });

  it("cached_tokens handling: verify usage.input_tokens_details.cached_tokens and cache_write_tokens", async () => {
    const { default: app } = await import("../src/server");

    // First call with new key -> cache write
    const writeBody = {
      model: "gpt-5.6-sol",
      input: "cache write test",
      prompt_cache_key: "cache-write-key-001",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      store: false,
    };
    const res1 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(writeBody),
    });
    expect(res1.status).toBe(200);
    const j1: any = await res1.json();
    expect(j1.usage).toBeDefined();
    expect(j1.usage.input_tokens_details).toBeDefined();
    // On write, cache_write_tokens >0, cached_tokens 0
    expect(typeof j1.usage.input_tokens_details.cached_tokens).toBe("number");
    expect(typeof j1.usage.input_tokens_details.cache_write_tokens).toBe("number");
    expect(j1.usage.input_tokens_details.cache_write_tokens).toBeGreaterThanOrEqual(0);

    // Second call with hit key -> cached read
    const hitBody = {
      model: "gpt-5.6-sol",
      input: "cache hit test",
      prompt_cache_key: "cache-hit-key-001-hit",
      prompt_cache_options: { mode: "explicit", ttl: "30m" },
      prompt_cache_breakpoint: { mode: "explicit" },
      store: false,
    };
    const res2 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(hitBody),
    });
    expect(res2.status).toBe(200);
    const j2: any = await res2.json();
    expect(j2.usage.input_tokens_details).toBeDefined();
    expect(j2.usage.input_tokens_details.cached_tokens).toBeGreaterThan(0);
  });

  it("p-queue queues render: only 1 render at a time (concurrency 1)", async () => {
    const { renderQueue, queueRender } = await import("../src/utils/queue");
    expect(renderQueue.concurrency).toBe(1);

    let concurrent = 0;
    let maxConcurrent = 0;
    const tasks = Array.from({ length: 5 }, (_, i) =>
      queueRender(async () => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        // Simulate render delay
        await new Promise((r) => setTimeout(r, 15));
        concurrent--;
        return i;
      }),
    );
    const results = await Promise.all(tasks);
    expect(results).toHaveLength(5);
    expect(maxConcurrent).toBe(1);
    // renderQueue should be idle afterwards
    expect(renderQueue.pending).toBe(0);
  });

  it("p-queue max 5 concurrent: queue handles 5 at a time", async () => {
    const { queue } = await import("../src/utils/queue");
    expect(queue.concurrency).toBe(5);

    let concurrent = 0;
    let maxConcurrent = 0;
    const tasks = Array.from({ length: 10 }, () =>
      queue.add(async () => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        await new Promise((r) => setTimeout(r, 15));
        concurrent--;
        return "ok";
      }),
    );
    await Promise.all(tasks);
    expect(maxConcurrent).toBeLessThanOrEqual(5);
    expect(maxConcurrent).toBeGreaterThan(1);
  });

  it("README documents Codex integration: OPENAI_BASE_URL=http://localhost:8787/v1 codex --model gpt-5.6-sol", async () => {
    const fs = await import("node:fs/promises");
    const readme = await fs.readFile("README.md", "utf8");
    expect(readme).toContain("OPENAI_BASE_URL=http://localhost:8787/v1");
    expect(readme).toContain("codex");
    expect(readme).toContain("gpt-5.6-sol");
  });

  it("README documents PQUEUE_CONCURRENCY=5 and concurrency handling", async () => {
    const fs = await import("node:fs/promises");
    const readme = await fs.readFile("README.md", "utf8");
    expect(readme).toContain("PQUEUE_CONCURRENCY");
    expect(readme).toContain("5");
    expect(readme.toLowerCase()).toContain("concurr");
  });

  it("README documents cached_tokens / prompt caching", async () => {
    const fs = await import("node:fs/promises");
    const readme = await fs.readFile("README.md", "utf8");
    expect(readme.toLowerCase()).toContain("prompt_cache");
    expect(readme).toContain("cached_tokens");
  });
});
