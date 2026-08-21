/**
 * Mirror suite — tests the NEW architecture at its real boundaries:
 *   - server routes mock `runRelay` (pipeline boundary)
 *   - pipeline tests mock gateway (network boundary)
 *   - SseDeltaExtractor tested pure
 *
 * Verbatim passthrough guarantee: whatever the client sends in the body must
 * reach runRelay untouched; whatever runRelay returns must reach the client.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import type { RunHandle } from "../src/relay/pipeline";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------
const runRelayMock = vi.fn();
vi.mock("../src/relay/pipeline", () => ({
  RelayError: class RelayError extends Error {
    constructor(message: string, readonly kind: string) { super(message); }
  },
  runRelay: (...args: any[]) => runRelayMock(...args),
}));

vi.mock("../src/bridge/playwright", () => ({ startPlaywrightBridge: vi.fn() }));

import app from "../src/server";
import { SseDeltaExtractor, parseFrames } from "../src/relay/sse";

function makeHandle(opts: {
  final: any;
  frames?: { event: string; data: string }[];
}): RunHandle {
  const frames = opts.frames ?? [];
  return {
    stats: { bytes: 0, firstByteMs: 1, frames: frames.length },
    async *events() { for (const f of frames) yield f; },
    final: Promise.resolve(opts.final),
  };
}

const FINAL = {
  id: "resp_test",
  object: "response",
  status: "completed",
  output: [
    { type: "message", role: "assistant", content: [{ type: "output_text", text: "hello" }] },
  ],
  usage: { input_tokens: 5, output_tokens: 2, total_tokens: 7 },
};

beforeEach(() => { runRelayMock.mockReset(); });

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------
describe("GET /health", () => {
  it("returns ok", async () => {
    const res = await app.request("/health");
    expect(res.status).toBe(200);
    expect((await res.json()).ok).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// POST /v1/responses — non-stream passthrough
// ---------------------------------------------------------------------------
describe("POST /v1/responses (non-stream)", () => {
  it("passes body verbatim to pipeline and returns final object", async () => {
    runRelayMock.mockResolvedValueOnce(makeHandle({ final: FINAL }));
    const body = {
      model: "gpt-5.6-sol",
      input: "hi",
      store: false,
      reasoning: { effort: "max" },
      prompt_cache_key: "k1",
      tools: [{ type: "image_generation" }],
    };
    const res = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual(FINAL);
    expect(runRelayMock).toHaveBeenCalledTimes(1);
    expect(runRelayMock.mock.calls[0][0]).toEqual(body); // verbatim
  });

  it("preserves input_image data URLs verbatim", async () => {
    runRelayMock.mockResolvedValueOnce(makeHandle({ final: FINAL }));
    const dot = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
    const body = {
      model: "gpt-5.6-sol",
      input: [{
        role: "user",
        content: [
          { type: "input_text", text: "color?" },
          { type: "input_image", image_url: `data:image/png;base64,${dot}`, detail: "low" },
        ],
      }],
      store: false,
    };
    await app.request("/v1/responses", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    expect(runRelayMock.mock.calls[0][0]).toEqual(body);
  });

  it("maps sync/no-bridge errors to 503", async () => {
    const { RelayError } = await import("../src/relay/pipeline");
    runRelayMock.mockRejectedValueOnce(new (RelayError as any)("sync timeout on job.json", "sync"));
    const res = await app.request("/v1/responses", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x" }),
    });
    expect(res.status).toBe(503);
    const j = await res.json();
    expect(j.error.type).toBe("sync");
  });

  it("maps stall timeouts to 504", async () => {
    const { RelayError } = await import("../src/relay/pipeline");
    runRelayMock.mockRejectedValueOnce(new (RelayError as any)("stream stalled 300s", "timeout"));
    const res = await app.request("/v1/responses", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "x" }),
    });
    expect(res.status).toBe(504);
  });

  it("rejects invalid JSON body", async () => {
    const res = await app.request("/v1/responses", {
      method: "POST", headers: { "content-type": "application/json" }, body: "{nope",
    });
    expect(res.status).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// POST /v1/responses — streaming (real relay frames)
// ---------------------------------------------------------------------------
describe("POST /v1/responses (stream)", () => {
  it("emits real relay frames then response.completed", async () => {
    runRelayMock.mockResolvedValueOnce(makeHandle({
      final: FINAL,
      frames: [
        { event: "response.created", data: JSON.stringify({ type: "response.created" }) },
        { event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta: "hel" }) },
        { event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta: "lo" }) },
      ],
    }));
    const res = await app.request("/v1/responses", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "hi", stream: true }),
    });
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("text/event-stream");
    const text = await res.text();
    expect(text).toContain("event: response.created");
    expect(text).toContain('"delta":"hel"');
    expect(text).toContain('"delta":"lo"');
    expect(text).toContain("event: response.completed");
    const completed = text.split("\n\n").find(b => b.includes("response.completed"))!;
    const payload = JSON.parse(completed.replace(/^event: .*\ndata: /, ""));
    expect(payload.response.id).toBe(FINAL.id);
  });

  it("emits error frame when pipeline fails mid-stream", async () => {
    const { RelayError } = await import("../src/relay/pipeline");
    let rejectFinal: (e: any) => void = () => {};
    runRelayMock.mockResolvedValueOnce({
      stats: { bytes: 0, firstByteMs: 1, frames: 1 },
      async *events() {
        yield { event: "response.created", data: "{}" };
        setTimeout(() => rejectFinal(new (RelayError as any)("upstream failed: boom", "upstream")), 5);
      },
      final: new Promise((_, rej) => { rejectFinal = rej; }),
    });
    const res = await app.request("/v1/responses", {
      method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "hi", stream: true }),
    });
    const text = await res.text();
    expect(text).toContain("event: response.created");
    expect(text).toContain("event: error");
    expect(text).toContain("upstream");
  });
});

// ---------------------------------------------------------------------------
// SseDeltaExtractor — windowed protocol v2 + legacy v1
// ---------------------------------------------------------------------------
describe("SseDeltaExtractor", () => {
  const A = 'event: a\ndata: {"x":1}\n\n';
  const B = 'event: b\ndata: {"x":2}\n\n';
  const C = 'event: c\ndata: {"x":3}\n\n';

  it("v2: emits only new frames across windowed polls", () => {
    const ex = new SseDeltaExtractor();
    // poll 1: full A visible
    const p1 = ex.feed(JSON.stringify({ streaming: true, total: A.length, start: 0, tail: A }));
    expect(p1.map(f => f.event)).toEqual(["a"]);
    // poll 2: window slid — tail covers end of A + all of B
    const tail2 = A.slice(8) + B;
    const p2 = ex.feed(JSON.stringify({ streaming: true, total: A.length + B.length, start: 8, tail: tail2 }));
    expect(p2.map(f => f.event)).toEqual(["b"]);
    // poll 3: no growth → no frames
    expect(ex.feed(JSON.stringify({ streaming: true, total: A.length + B.length, start: 8, tail: tail2 }))).toEqual([]);
    // poll 4: C arrives
    const tail4 = B.slice(8) + C;
    const p4 = ex.feed(JSON.stringify({ streaming: true, total: A.length + B.length + C.length, start: A.length + 8, tail: tail4 }));
    expect(p4.map(f => f.event)).toEqual(["c"]);
  });

  it("v2: handles window slide past cursor by dropping missed region", () => {
    const ex = new SseDeltaExtractor();
    ex.feed(JSON.stringify({ streaming: true, total: 10, start: 0, tail: "0123456789" }));
    const frames = ex.feed(JSON.stringify({
      streaming: true, total: 100, start: 90,
      tail: "event: z\ndata: {}\n\n",
    }));
    expect(frames.map(f => f.event)).toEqual(["z"]);
  });

  it("legacy v1: full accumulated sse", () => {
    const ex = new SseDeltaExtractor();
    const f1 = ex.feed(JSON.stringify({ streaming: true, sse: "event: a\ndata: 1\n\n" }));
    expect(f1.map(f => f.event)).toEqual(["a"]);
    const f2 = ex.feed(JSON.stringify({ streaming: true, sse: "event: a\ndata: 1\n\nevent: b\ndata: 2\n\n" }));
    expect(f2.map(f => f.event)).toEqual(["b"]);
  });

  it("tolerates partial JSON mid-write", () => {
    const ex = new SseDeltaExtractor();
    expect(ex.feed('{"streaming":true,"total":99,"start":0,"tail":"event: x\\n')).toEqual([]);
  });
});

describe("parseFrames", () => {
  it("keeps incomplete trailing frame unconsumed", () => {
    const { frames, nextOffset } = parseFrames("event: a\ndata: 1\n\nevent: b\nda", 0);
    expect(frames).toHaveLength(1);
    expect(nextOffset).toBe("event: a\ndata: 1\n\n".length);
  });
});
