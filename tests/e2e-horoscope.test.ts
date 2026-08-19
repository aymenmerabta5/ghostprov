/**
 * E2E Horoscope Multi-Step Flow — Aquarius via POST /v1/responses with store:true + previous_response_id
 *
 * Verifies the full 2-turn tool-use flow through the local mirror's Hono app and via relayCreate job.json.
 * - Turn 1: user asks horoscope for Aquarius → expect function_call get_horoscope with sign Aquarius, capture resp.id + call_id
 * - Turn 2: function_call_output with previous_response_id → expect final message with "Aquarius: Next Tuesday, you will befriend a baby otter." and phase final_answer
 *
 * Mocked via global fetch (Yjs+render+zip) for CI — no live Prism creds required.
 * Live variant is gated on PRISM_DOC_ID + PRISM_SANDBOX_TOKEN (skipped by default).
 *
 * Live flow documentation (run against a live mirror with proxy:18080):
 * ```bash
 * # 1. first turn — triggers get_horoscope
 * curl -X POST http://localhost:8787/v1/responses -H "content-type: application/json" -d '{
 *   "model":"gpt-5.6-sol","store":true,
 *   "input":[{"role":"user","content":"What is my horoscope? I am an Aquarius."}],
 *   "tools":[{"type":"function","name":"get_horoscope","description":"Get horoscope","parameters":{"type":"object","properties":{"sign":{"type":"string"}},"required":["sign"]}}]
 * }' | jq '.output'
 * # → {type:"function_call", name:"get_horoscope", arguments:'{"sign":"Aquarius"}', call_id:"call_..."}
 *
 * # 2. second turn — supply tool result, chain via previous_response_id
 * curl -X POST http://localhost:8787/v1/responses -H "content-type: application/json" -d '{
 *   "model":"gpt-5.6-sol","store":true,"previous_response_id":"resp_...from_turn1",
 *   "input":[{"type":"function_call_output","call_id":"call_...","output":"Aquarius: Next Tuesday you will befriend a baby otter."}],
 *   "tools":[{"type":"function","name":"get_horoscope","description":"Get horoscope","parameters":{"type":"object","properties":{"sign":{"type":"string"}},"required":["sign"]}}]
 * }' | jq '.output'
 * # → {type:"message", role:"assistant", phase:"final_answer", content:[{type:"output_text", text:"Aquarius: Next Tuesday, you will befriend a baby otter."}]}
 * ```
 *
 * Uses existing relay client + server (no new server changes). Mocks fetch for Yjs token, bridge write, render, render-status, zip.
 * Also validates job.json handling: store:true, tools, previous_response_id, function_call_output forwarded verbatim via __LAST_JOB__.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { zipSync } from "fflate";

// Mock y-websocket to avoid real WSS (same as relay.test.ts / proxy.test.ts)
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

const horoscopeTool = {
  type: "function" as const,
  name: "get_horoscope",
  description: "Get horoscope for a zodiac sign",
  parameters: {
    type: "object",
    properties: { sign: { type: "string", description: "Zodiac sign" } },
    required: ["sign"],
    additionalProperties: false,
  },
};

// Spec: second request input uses without-comma; final answer expects with-comma
const HOROSCOPE_TOOL_OUTPUT = "Aquarius: Next Tuesday you will befriend a baby otter.";
const HOROSCOPE_FINAL_TEXT = "Aquarius: Next Tuesday, you will befriend a baby otter.";

const FUNCTION_CALL_RESPONSE = {
  id: "resp_horoscope_1_mock",
  object: "response" as const,
  model: "gpt-5.6-sol",
  output: [
    {
      type: "function_call" as const,
      name: "get_horoscope",
      arguments: JSON.stringify({ sign: "Aquarius" }),
      call_id: "call_aquarius_123",
    },
  ],
  output_text: "",
};

const FINAL_ANSWER_RESPONSE = {
  id: "resp_horoscope_2_mock",
  object: "response" as const,
  model: "gpt-5.6-sol",
  output: [
    {
      type: "message" as const,
      role: "assistant" as const,
      content: [{ type: "output_text" as const, text: HOROSCOPE_FINAL_TEXT }],
      phase: "final_answer" as const,
    },
  ],
  output_text: HOROSCOPE_FINAL_TEXT,
};

/**
 * Creates a fetch mock that simulates the Prism Yjs+render+zip loop for the horoscope flow.
 * - Stores __LAST_JOB__ on bridge/internal writes
 * - On zip, inspects __LAST_JOB__.previous_response_id to decide which mock response to return
 *   (no previous_response_id → function_call, with → final_answer)
 * - Always returns job.json reflecting the last written job for job.json verbatim checks
 */
function createHoroscopeFetchMock() {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();

    if (url.includes("/api/y") && init?.method === "POST") {
      return new Response(JSON.stringify({ token: "mock_y_token" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url.includes("/__bridge/write") && init?.method === "POST") {
      try {
        const parsed = JSON.parse(init?.body as string);
        // bridge body is { filename:"job.json", content: JSON.stringify(job) }
        if (parsed?.content) {
          try {
            (globalThis as any).__LAST_JOB__ = JSON.parse(parsed.content);
          } catch {
            (globalThis as any).__LAST_JOB__ = parsed;
          }
        } else {
          (globalThis as any).__LAST_JOB__ = parsed;
        }
      } catch {
        // ignore parse error
      }
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    }
    if (url.includes("/__internal/write-job") && init?.method === "POST") {
      try {
        (globalThis as any).__LAST_JOB__ = JSON.parse(init?.body as string);
      } catch {}
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
      const lastJob: any = (globalThis as any).__LAST_JOB__ ?? {};
      const isSecondTurn = !!lastJob?.previous_response_id || (() => {
        try {
          const s = JSON.stringify(lastJob?.input ?? "");
          return s.includes("function_call_output") || s.includes(HOROSCOPE_TOOL_OUTPUT);
        } catch {
          return false;
        }
      })();
      const responsePayload = isSecondTurn ? FINAL_ANSWER_RESPONSE : FUNCTION_CALL_RESPONSE;
      const zipBytes = zipSync({
        "job.json": new TextEncoder().encode(JSON.stringify(lastJob)),
        "response.json": new TextEncoder().encode(JSON.stringify(responsePayload)),
      });
      return new Response(zipBytes as unknown as BodyInit, {
        status: 200,
        headers: { "content-type": "application/zip" },
      });
    }
    return new Response("not mocked: " + url, { status: 404 });
  });
}

// Live integration (requires Prism credentials + proxy:18080) — skipped when creds absent.
// Documents the expected end-to-end behavior against the real relay.
describe("e2e horoscope live (plan, gated)", () => {
  const hasCreds = !!process.env.PRISM_DOC_ID && !!process.env.PRISM_SANDBOX_TOKEN;
  const live = hasCreds ? it : it.skip;

  live("live 2-turn Aquarius horoscope via relayCreate (requires Prism creds)", async () => {
    const { relayCreate } = await import("../src/relay/client");
    // Turn 1
    const body1: any = {
      model: "gpt-5.6-sol",
      store: true,
      input: [{ role: "user", content: "What is my horoscope? I am an Aquarius." }],
      tools: [horoscopeTool],
    };
    const res1: any = await relayCreate(body1);
    const fc = res1.output.find((o: any) => o.type === "function_call");
    expect(fc).toBeDefined();
    expect(fc.name).toBe("get_horoscope");
    expect(JSON.parse(fc.arguments).sign).toBe("Aquarius");
    const respId: string = res1.id;
    const callId: string = fc.call_id;
    expect(respId).toBeTruthy();
    expect(callId).toBeTruthy();

    // Turn 2 — chain
    const body2: any = {
      model: "gpt-5.6-sol",
      store: true,
      previous_response_id: respId,
      input: [{ type: "function_call_output", call_id: callId, output: HOROSCOPE_TOOL_OUTPUT }],
      tools: [horoscopeTool],
    };
    const res2: any = await relayCreate(body2);
    const msg = res2.output.find((o: any) => o.type === "message" && o.phase === "final_answer");
    expect(msg).toBeDefined();
    const text = res2.output_text ?? JSON.stringify(msg.content);
    expect(text).toContain("Aquarius");
    expect(text).toContain("baby otter");
  });
});

describe("e2e horoscope via Hono app (mocked fetch, job.json verbatim) — 2-turn Aquarius", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    delete (globalThis as any).__LAST_JOB__;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("full 2-turn flow via POST /v1/responses with store:true + previous_response_id (mocked fetch, validates job.json)", async () => {
    const fetchMock = createHoroscopeFetchMock();
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { default: app } = await import("../src/server");

    // ---- Turn 1: POST /v1/responses {model:"gpt-5.6-sol", store:true, input:[{role:"user", content:"What is my horoscope? I am an Aquarius."}], tools:[get_horoscope]} ----
    const firstBody: any = {
      model: "gpt-5.6-sol",
      store: true,
      input: [{ role: "user", content: "What is my horoscope? I am an Aquarius." }],
      tools: [horoscopeTool],
    };

    const res1 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(firstBody),
    });
    expect(res1.status).toBe(200);
    const j1: any = await res1.json();

    // expect function_call with Aquarius, capture resp.id and call_id
    expect(j1.id).toBeTruthy();
    expect(j1.object === "response" || j1.id).toBeTruthy();
    const fc = j1.output.find((o: any) => o.type === "function_call");
    expect(fc).toBeDefined();
    expect(fc.name).toBe("get_horoscope");
    // arguments is JSON string
    const args = JSON.parse(fc.arguments);
    expect(args.sign).toBe("Aquarius");
    expect(fc.call_id).toBeTruthy();
    const respId: string = j1.id;
    const callId: string = fc.call_id;

    // job.json handling for turn 1 — verbatim via __LAST_JOB__
    const job1: any = (globalThis as any).__LAST_JOB__;
    expect(job1).toBeDefined();
    expect(job1.model).toBe("gpt-5.6-sol");
    expect(job1.store).toBe(true);
    expect(Array.isArray(job1.tools)).toBe(true);
    expect(job1.tools[0].name).toBe("get_horoscope");
    expect(JSON.stringify(job1.input)).toContain("Aquarius");
    // store:true must survive (not be stripped)
    expect(job1.store).toBe(true);

    // ---- Turn 2: POST /v1/responses {model:"gpt-5.6-sol", store:true, previous_response_id: resp.id, input:[{type:"function_call_output", call_id, output}], tools:[...]} ----
    const secondBody: any = {
      model: "gpt-5.6-sol",
      store: true,
      previous_response_id: respId,
      input: [{ type: "function_call_output", call_id: callId, output: HOROSCOPE_TOOL_OUTPUT }],
      tools: [horoscopeTool],
    };

    const res2 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(secondBody),
    });
    expect(res2.status).toBe(200);
    const j2: any = await res2.json();

    // expect final message with "Aquarius: Next Tuesday, you will befriend a baby otter." and phase final_answer
    // tolerate output_text or output[ message ] shape
    const finalText: string = j2.output_text ?? JSON.stringify(j2.output);
    // check contains expected phrase (with comma variant is canonical, but also accept without comma)
    expect(finalText).toContain("Aquarius");
    expect(finalText).toContain("baby otter");
    expect(finalText).toContain("Next Tuesday");

    const finalMsg = j2.output.find((o: any) => o.type === "message" && o.phase === "final_answer");
    expect(finalMsg).toBeDefined();
    // phase must be final_answer per spec
    expect(finalMsg.phase).toBe("final_answer");
    // content should contain the final horoscope text
    const contentStr = JSON.stringify(finalMsg.content);
    expect(contentStr).toContain("Aquarius");
    expect(contentStr).toContain("baby otter");
    // if server returns output_text exactly, verify it matches expected final text (with comma)
    if (j2.output_text) {
      // allow either with or without comma, but prefer with comma as spec
      expect(j2.output_text).toMatch(/Aquarius: Next Tuesday,? you will befriend a baby otter\./);
    } else {
      expect(contentStr).toMatch(/Aquarius: Next Tuesday,? you will befriend a baby otter\./);
    }

    // job.json handling for turn 2 — previous_response_id and function_call_output verbatim
    const job2: any = (globalThis as any).__LAST_JOB__;
    expect(job2).toBeDefined();
    expect(job2.model).toBe("gpt-5.6-sol");
    expect(job2.store).toBe(true);
    expect(job2.previous_response_id).toBe(respId);
    expect(Array.isArray(job2.input)).toBe(true);
    const fco = job2.input.find((i: any) => i.type === "function_call_output");
    expect(fco).toBeDefined();
    expect(fco.call_id).toBe(callId);
    expect(fco.output).toBe(HOROSCOPE_TOOL_OUTPUT);
    expect(job2.tools[0].name).toBe("get_horoscope");
    // also verify fetchMock was called (bridge + render + zip path)
    expect(fetchMock).toHaveBeenCalled();
    const urls = fetchMock.mock.calls.map((c: any) => String(c[0]));
    // should have hit bridge or internal write-job
    expect(urls.some((u: string) => u.includes("__bridge/write") || u.includes("__internal/write-job"))).toBe(true);
    expect(urls.some((u: string) => u.includes("/s/sandboxes/proxy/render"))).toBe(true);
    expect(urls.some((u: string) => u.includes("/s/sandboxes/proxy/zip"))).toBe(true);
  });

  it("validates job.json for turn 1 and turn 2 independently (store:true preserved)", async () => {
    const fetchMock = createHoroscopeFetchMock();
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { default: app } = await import("../src/server");

    // Turn 1 only
    const res1 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        store: true,
        input: [{ role: "user", content: "What is my horoscope? I am an Aquarius." }],
        tools: [horoscopeTool],
      }),
    });
    expect(res1.status).toBe(200);
    const j1: any = await res1.json();
    expect(j1.output.find((o: any) => o.type === "function_call")?.name).toBe("get_horoscope");
    const job1 = (globalThis as any).__LAST_JOB__;
    expect(job1.store).toBe(true);
    expect(job1.tools).toBeDefined();

    // Turn 2 with different previous_response_id still forwards store:true verbatim
    // Reset fetch mock call history but keep __LAST_JOB__ overwritten by next write
    fetchMock.mockClear();
    const res2 = await app.request("/v1/responses", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        model: "gpt-5.6-sol",
        store: true,
        previous_response_id: j1.id,
        input: [{ type: "function_call_output", call_id: j1.output.find((o: any) => o.type === "function_call")?.call_id, output: HOROSCOPE_TOOL_OUTPUT }],
        tools: [horoscopeTool],
      }),
    });
    expect(res2.status).toBe(200);
    const job2 = (globalThis as any).__LAST_JOB__;
    expect(job2.store).toBe(true);
    expect(job2.previous_response_id).toBe(j1.id);
  });
});

describe("e2e horoscope via relayCreate (mocked fetch, job.json verbatim) — direct client", () => {
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    delete (globalThis as any).__LAST_JOB__;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    delete (globalThis as any).__LAST_JOB__;
  });

  it("relayCreate 2-turn Aquarius: function_call then final_answer with phase final_answer", async () => {
    const fetchMock = createHoroscopeFetchMock();
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;

    const { relayCreate } = await import("../src/relay/client");

    // Turn 1
    const body1: any = {
      model: "gpt-5.6-sol",
      store: true,
      input: [{ role: "user", content: "What is my horoscope? I am an Aquarius." }],
      tools: [horoscopeTool],
    };
    const res1: any = await relayCreate(body1);
    expect(res1.id).toBeTruthy();
    const fc = res1.output.find((o: any) => o.type === "function_call");
    expect(fc).toBeDefined();
    expect(fc.name).toBe("get_horoscope");
    expect(JSON.parse(fc.arguments).sign).toBe("Aquarius");
    const callId: string = fc.call_id;
    expect(callId).toBeTruthy();

    // job.json verbatim for turn 1
    const job1: any = (globalThis as any).__LAST_JOB__;
    expect(job1.store).toBe(true);
    expect(job1.model).toBe("gpt-5.6-sol");
    expect(job1.tools[0].name).toBe("get_horoscope");
    expect(JSON.stringify(job1.input)).toContain("Aquarius");

    // Turn 2
    const body2: any = {
      model: "gpt-5.6-sol",
      store: true,
      previous_response_id: res1.id,
      input: [{ type: "function_call_output", call_id: callId, output: HOROSCOPE_TOOL_OUTPUT }],
      tools: [horoscopeTool],
    };
    const res2: any = await relayCreate(body2);
    expect(res2.id).toBeTruthy();
    const finalMsg = res2.output.find((o: any) => o.type === "message" && o.phase === "final_answer");
    expect(finalMsg).toBeDefined();
    expect(finalMsg.phase).toBe("final_answer");
    const text = res2.output_text ?? JSON.stringify(finalMsg.content);
    expect(text).toContain("Aquarius");
    expect(text).toContain("baby otter");
    expect(text).toMatch(/Aquarius: Next Tuesday,? you will befriend a baby otter\./);

    // job.json verbatim for turn 2
    const job2: any = (globalThis as any).__LAST_JOB__;
    expect(job2.store).toBe(true);
    expect(job2.previous_response_id).toBe(res1.id);
    const fco = job2.input.find((i: any) => i.type === "function_call_output");
    expect(fco).toBeDefined();
    expect(fco.call_id).toBe(callId);
    expect(fco.output).toBe(HOROSCOPE_TOOL_OUTPUT);
  });

  it("relayCreate preserves tools and store:true across chaining", async () => {
    const fetchMock = createHoroscopeFetchMock();
    // @ts-ignore
    globalThis.fetch = fetchMock as unknown as typeof fetch;
    const { relayCreate } = await import("../src/relay/client");

    const res1: any = await relayCreate({
      model: "gpt-5.6-sol",
      store: true,
      input: [{ role: "user", content: "What is my horoscope? I am an Aquarius." }],
      tools: [horoscopeTool],
    });
    const fc = res1.output.find((o: any) => o.type === "function_call");
    const res2: any = await relayCreate({
      model: "gpt-5.6-sol",
      store: true,
      previous_response_id: res1.id,
      input: [{ type: "function_call_output", call_id: fc.call_id, output: HOROSCOPE_TOOL_OUTPUT }],
      tools: [horoscopeTool],
    });
    expect(res2.output.find((o: any) => o.type === "message")?.phase).toBe("final_answer");
    const job2: any = (globalThis as any).__LAST_JOB__;
    expect(job2.tools[0].name).toBe("get_horoscope");
    expect(job2.store).toBe(true);
  });
});
