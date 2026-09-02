import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import {
  buildStandaloneSearchResponsesRequest,
  StandaloneSearchAdapterError,
  standaloneSearchResponseFromFinal,
} from "../src/search/responses-adapter";
import { createRouterStandaloneSearchHandler } from "../src/search/router-handler";
import { createGhostProvApp } from "../src/server/app";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { createFakeV2System } from "./support/fake-v2-system";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { attachAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import type {
  StandaloneSearchHandlerContext,
  StandaloneSearchRequest,
} from "../src/http/standalone-search";

function request(overrides: Partial<StandaloneSearchRequest> = {}): StandaloneSearchRequest {
  return {
    id: "01a05ef8-afbd-7131-83cf-ff3a68888cf2",
    model: "gpt-5.6-sol",
    input: [{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "Find current documentation." }],
    }],
    commands: {
      search_query: [{ q: "OpenAI API documentation", domains: ["developers.openai.com"] }],
      response_length: "short",
    },
    settings: {
      allowed_callers: ["direct"],
      external_web_access: true,
      search_context_size: "low",
      filters: { allowed_domains: ["developers.openai.com"] },
      user_location: { type: "approximate", country: "DZ", timezone: "Africa/Algiers" },
    },
    max_output_tokens: 10_000,
    ...overrides,
  };
}

describe("standalone search Responses adapter", () => {
  it("maps the fixed Codex request to a required native hosted web-search turn", () => {
    const original = request();
    const body = buildStandaloneSearchResponsesRequest(original);

    expect(body).toMatchObject({
      model: "gpt-5.6-sol",
      stream: true,
      store: true,
      tool_choice: "required",
      max_output_tokens: 10_000,
      tools: [{
        type: "web_search",
        external_web_access: true,
        search_context_size: "low",
        filters: { allowed_domains: ["developers.openai.com"] },
        user_location: { type: "approximate", country: "DZ" },
      }],
    });
    expect(body).not.toHaveProperty("id");
    expect(body).not.toHaveProperty("commands");
    expect(body).not.toHaveProperty("settings");
    expect((body.input as unknown[])[0]).toBe((original.input as readonly unknown[])[0]);
    expect(JSON.stringify((body.input as unknown[]).at(-1))).toContain("search_query");
  });

  it("enables image results only for image commands and caps inner output", () => {
    const body = buildStandaloneSearchResponsesRequest(request({
      commands: { image_query: [{ q: "Golden Gate Bridge" }] },
      max_output_tokens: 100_000,
    }));
    expect(body.max_output_tokens).toBe(10_000);
    expect(body.include).toContain("web_search_call.results");
    expect(body.tools).toEqual([expect.objectContaining({
      type: "web_search",
      search_content_types: ["image", "text"],
      image_settings: { max_results: 1, caption: true },
    })]);
  });

  it("accepts optional Codex fields, text input, indexed mode, and reasoning", () => {
    const body = buildStandaloneSearchResponsesRequest({
      id: "01a05ef8-afbd-7131-83cf-ff3a68888cf2",
      model: "gpt-5.6-sol",
      input: "Find current documentation.",
      settings: {
        external_web_access: "indexed",
        allowed_callers: ["shell", "code_interpreter"],
      },
      reasoning: { effort: "low" },
    });
    expect(body).toMatchObject({
      model: "gpt-5.6-sol",
      reasoning: { effort: "low" },
      tools: [{
        type: "web_search",
        external_web_access: true,
        indexed_web_access: true,
      }],
    });
    expect(body.input).toEqual([
      expect.objectContaining({ role: "user" }),
      expect.objectContaining({ role: "user" }),
    ]);
  });

  it("renders native output text and bounded deduplicated source URLs", () => {
    const response = standaloneSearchResponseFromFinal({
      id: "resp_search",
      status: "completed",
      output: [{
        type: "reasoning",
        encrypted_content: "opaque-search-state",
      }, {
        type: "web_search_call",
        action: { sources: [
          { url: "https://developers.openai.com/api/docs", title: "API docs" },
          { url: "https://developers.openai.com/api/docs", title: "duplicate" },
        ] },
        results: [{
          type: "image_result",
          ref_id: "turn0image0",
          image_url: "https://example.com/image.png",
          source_website_url: "https://example.com/source",
          future_field: { retained: true },
        }],
      }, {
        type: "message",
        content: [{
          type: "output_text",
          text: "The official documentation is available online.",
          annotations: [{
            type: "url_citation",
            url: "https://developers.openai.com/api/docs/guides/tools-web-search",
            title: "Web search",
          }],
        }],
      }],
    });

    expect(response.output).toContain("official documentation");
    expect(response.output).toContain("https://developers.openai.com/api/docs");
    expect(response.results).toHaveLength(4);
    expect(response.results).toContainEqual({
      url: "https://developers.openai.com/api/docs",
      title: "duplicate",
    });
    expect(response.encrypted_output).toBe("opaque-search-state");
    expect(response.results).toContainEqual({
      type: "image_result",
      ref_id: "turn0image0",
      image_url: "https://example.com/image.png",
      source_website_url: "https://example.com/source",
      future_field: { retained: true },
    });
  });

  it("fails closed on incomplete or textless native finals", () => {
    expect(() => standaloneSearchResponseFromFinal({ status: "failed", output: [] }))
      .toThrow(StandaloneSearchAdapterError);
    expect(() => standaloneSearchResponseFromFinal({ status: "completed", output: [] }))
      .toThrow(StandaloneSearchAdapterError);
  });

  it("uses the shared router, transfers ingress once, and drains events", async () => {
    let capturedBody: Record<string, unknown> | null = null;
    let capturedOptions: Record<string, unknown> | null = null;
    let discarded = 0;
    const handle: RunHandle = {
      async *events() {},
      discardUnconsumedEvents: () => { discarded++; },
      upstreamPrelude: Promise.resolve({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      }),
      final: Promise.resolve({
        id: "resp_search",
        status: "completed",
        output: [{
          type: "message",
          content: [{ type: "output_text", text: "grounded result" }],
        }],
      }),
      stats: { bytes: 1, firstByteMs: 1, frames: 1 },
    };
    const router = {
      async startRelay(body: Record<string, unknown>, options: Record<string, unknown>) {
        capturedBody = body;
        capturedOptions = options;
        return handle;
      },
    };
    let transfers = 0;
    const context: StandaloneSearchHandlerContext = {
      signal: new AbortController().signal,
      incomingLease: { release() {} },
      transferIncomingLease: () => {
        transfers++;
        return { release() {} };
      },
      clientId: "search-client",
      authorizationScope: null,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    };

    const result = await createRouterStandaloneSearchHandler(router as never)(request(), context);

    expect(transfers).toBe(1);
    expect(discarded).toBe(1);
    expect(capturedBody).toMatchObject({ tools: [{ type: "web_search" }] });
    expect(capturedOptions).toMatchObject({
      source: "internal",
      clientId: "search-client",
    });
    expect(result).toEqual({ kind: "success", status: 200, body: { output: "grounded result" } });
  });

  it("runs end to end through the real app, router, and V2 exact-once transport", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const system = await createFakeV2System({
      runtimes: 1,
      physicalPerRuntime: 4,
      authorizationScope,
    });
    const app = createGhostProvApp({
      router: system.router,
      apiKey: "search-test-key",
      authorizationScope,
      standaloneSearch: createRouterStandaloneSearchHandler(system.router),
    });
    try {
      const response = await app.request("/v1/alpha/search", {
        method: "POST",
        headers: {
          authorization: "Bearer search-test-key",
          "content-type": "application/json",
        },
        body: JSON.stringify(request()),
      });
      const value = await response.json() as { output: string; results?: unknown[] };

      expect(response.status).toBe(200);
      expect(value.output).toContain("official OpenAI API documentation");
      expect(value.output).toContain("https://developers.openai.com/api/docs");
      expect(value.results).toHaveLength(2);
      expect(system.totalUpstreamCalls()).toBe(1);
      expect(system.runtimes[0]!.upstream.receivedBodies[0]).toMatchObject({
        model: "gpt-5.6-sol",
        stream: true,
        store: true,
        service_tier: "fast",
        tool_choice: "required",
        tools: [{ type: "web_search", external_web_access: true }],
      });
      expect(system.router.snapshot().provenance).toMatchObject({
        records: 1,
        reservedRecords: 0,
      });
    } finally {
      await system.close();
    }
  });

  it("serializes one search session and continues follow-up ref commands by provider ID", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const system = await createFakeV2System({
      runtimes: 1,
      physicalPerRuntime: 4,
      authorizationScope,
    });
    const app = createGhostProvApp({
      router: system.router,
      authorizationScope,
      standaloneSearch: createRouterStandaloneSearchHandler(system.router),
    });
    try {
      const first = await app.request("/v1/alpha/search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(request()),
      });
      expect(first.status).toBe(200);
      await first.text();

      const secondRequest = request({
        commands: { open: [{ ref_id: "https://developers.openai.com/api/docs" }] },
      });
      const second = await app.request("/v1/alpha/search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(secondRequest),
      });
      expect(second.status).toBe(200);
      await second.text();

      expect(system.runtimes[0]!.upstream.receivedBodies).toHaveLength(2);
      expect(system.runtimes[0]!.upstream.receivedBodies[1]).toMatchObject({
        previous_response_id: "resp_1_1",
        input: [expect.objectContaining({ role: "user" })],
        tools: [{ type: "web_search" }],
      });
      expect((system.runtimes[0]!.upstream.receivedBodies[1]!.input as unknown[]))
        .toHaveLength(1);
    } finally {
      await system.close();
    }
  });

  it("releases a settled final when a malformed handle rejects its prelude", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 2 * 1024 * 1024, jumboBytes: 0 });
    const final = attachAtomicBytePermit({
      id: "resp_search",
      status: "completed",
      output: [{
        type: "message",
        content: [{ type: "output_text", text: "result" }],
      }],
    }, await pool.acquire(1024));
    const handle: RunHandle = {
      async *events() {},
      discardUnconsumedEvents() {},
      upstreamPrelude: Promise.reject(new Error("missing prelude")),
      final: Promise.resolve(final),
      stats: { bytes: 1, firstByteMs: 1, frames: 1 },
    };
    const router = { startRelay: async () => handle };
    const context: StandaloneSearchHandlerContext = {
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      clientId: "search-client",
      authorizationScope: null,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    };

    await expect(createRouterStandaloneSearchHandler(router as never)(request(), context))
      .rejects.toThrow("missing prelude");
    expect(pool.snapshot().activePermits).toBe(0);
    pool.close();
  });
});
