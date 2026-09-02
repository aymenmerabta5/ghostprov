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
import {
  atomicBytePermitOf,
  attachAtomicBytePermit,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";
import { v2RetainedEventPermitBytes } from "../src/v2/memory/event-memory-sizing";
import { ResponseProvenanceObsoleteError } from "../src/router/provenance";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { V2CommittedError } from "../src/v2/transport/run-handle";
import { StandaloneSearchSessionIndex } from "../src/search/session-index";
import { STANDALONE_SEARCH_MAX_RESPONSE_BYTES } from "../src/search/limits";
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

  it("projects native output text and bounded opaque results without copying", () => {
    const firstSource = {
      url: "https://developers.openai.com/api/docs",
      title: "API docs",
    };
    const duplicateSource = {
      url: "https://developers.openai.com/api/docs",
      title: "duplicate",
    };
    const imageResult = {
      type: "image_result",
      ref_id: "turn0image0",
      image_url: "https://example.com/image.png",
      source_website_url: "https://example.com/source",
      future_field: { retained: true },
    };
    const annotation = {
      type: "url_citation",
      url: "https://developers.openai.com/api/docs/guides/tools-web-search",
      title: "Web search",
    };
    const final = {
      id: "resp_search",
      status: "completed",
      output: [{
        type: "reasoning",
        encrypted_content: "opaque-search-state",
      }, {
        type: "web_search_call",
        action: { sources: [
          firstSource,
          duplicateSource,
        ] },
        results: [imageResult],
      }, {
        type: "message",
        content: [{
          type: "output_text",
          text: "The official documentation is available online.",
          // The repeated identity is intentionally ignored by the projection.
          annotations: [annotation, firstSource],
        }],
      }],
    };
    const response = standaloneSearchResponseFromFinal(final);
    const results = response.results as readonly unknown[];

    expect(response.output).toBe("The official documentation is available online.");
    expect(response.output).not.toContain("https://developers.openai.com/api/docs");
    expect(results).toHaveLength(4);
    expect(results[0]).toBe(firstSource);
    expect(results[1]).toBe(duplicateSource);
    expect(results[2]).toBe(imageResult);
    expect(results[3]).toBe(annotation);
    expect(response.encrypted_output).toBe("opaque-search-state");
    expect(response.results).toContainEqual({
      type: "image_result",
      ref_id: "turn0image0",
      image_url: "https://example.com/image.png",
      source_website_url: "https://example.com/source",
      future_field: { retained: true },
    });
  });

  it("keeps a large-source projection no larger than its provider final", () => {
    const sources = Array.from({ length: 64 }, (_, index) => ({
      type: "url_citation",
      url: `https://example.com/${index}/${"x".repeat(1_800)}`,
      title: "t".repeat(200),
    }));
    const annotations = sources.map(source => ({ ...source }));
    const final = {
      id: "resp_large_search",
      status: "completed",
      output: [{
        type: "web_search_call",
        action: { sources },
      }, {
        type: "message",
        content: [{
          type: "output_text",
          text: "Provider-authored grounded result.",
          annotations,
        }],
      }],
    };

    const response = standaloneSearchResponseFromFinal(final);
    const results = response.results as readonly unknown[];
    const encodedBytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value), "utf8");

    expect(results).toHaveLength(128);
    expect(results[0]).toBe(sources[0]);
    expect(results[64]).toBe(annotations[0]);
    expect(encodedBytes(response)).toBeLessThanOrEqual(encodedBytes(final));
  });

  it("keeps the transferred terminal permit large enough for the public projection", async () => {
    const sources = Array.from({ length: 64 }, (_, index) => ({
      type: "url_citation",
      url: `https://example.com/${index}/${"x".repeat(1_800)}`,
      title: "t".repeat(200),
    }));
    const final = {
      id: "resp_permitted_search",
      status: "completed",
      output: [{
        type: "web_search_call",
        action: { sources },
      }, {
        type: "message",
        content: [{
          type: "output_text",
          text: "Provider-authored grounded result.",
          annotations: sources.map(source => ({ ...source })),
        }],
      }],
    };
    const terminalWireBytes = Buffer.byteLength(JSON.stringify({
      type: "response.completed",
      response: final,
    }), "utf8");
    const terminalPermitBytes = v2RetainedEventPermitBytes(terminalWireBytes);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: terminalPermitBytes,
      jumboBytes: 0,
    });
    const permittedFinal = attachAtomicBytePermit(
      final,
      await pool.acquire(terminalPermitBytes),
    );
    const handle: RunHandle = {
      async *events() {},
      discardUnconsumedEvents() {},
      upstreamPrelude: Promise.resolve({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      }),
      final: Promise.resolve(permittedFinal),
      stats: { bytes: terminalWireBytes, firstByteMs: 1, frames: 1 },
    };
    const handler = createRouterStandaloneSearchHandler({
      startRelay: async () => handle,
    } as never);
    const result = await handler(request(), {
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      clientId: "search-client",
      authorizationScope: null,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    });
    expect(result.kind).toBe("success");
    if (result.kind !== "success") throw new Error("expected search success");
    const publicBytes = Buffer.byteLength(JSON.stringify(result.body), "utf8");
    expect(atomicBytePermitOf(result.body)?.bytes)
      .toBeGreaterThanOrEqual(v2RetainedEventPermitBytes(publicBytes));
    releaseAtomicBytePermit(result.body);
    expect(pool.snapshot().activePermits).toBe(0);
    pool.close();
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

  it("deletes an obsolete cached search parent and retries the immutable request only once", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const calls: Record<string, unknown>[] = [];
    const attemptLeases: unknown[] = [];
    const transferredLeases: unknown[] = [];
    let rootObsolete = false;
    let leaseTransfers = 0;
    const successfulHandle = (responseId: string): RunHandle => ({
      async *events() {},
      discardUnconsumedEvents() {},
      upstreamPrelude: Promise.resolve({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      }),
      final: Promise.resolve({
        id: responseId,
        status: "completed",
        output: [{
          type: "message",
          content: [{ type: "output_text", text: "grounded result" }],
        }],
      }),
      stats: { bytes: 1, firstByteMs: 1, frames: 1 },
    });
    const router = {
      async startRelay(body: Record<string, unknown>, options: { incomingLease?: unknown }) {
        calls.push(structuredClone(body));
        attemptLeases.push(options.incomingLease);
        if (body.previous_response_id === "resp_poisoned") {
          throw new ResponseProvenanceObsoleteError();
        }
        if (rootObsolete && body.previous_response_id === "resp_recovered") {
          throw new ResponseProvenanceObsoleteError();
        }
        if (rootObsolete && !Object.hasOwn(body, "previous_response_id")) {
          throw new ResponseProvenanceObsoleteError();
        }
        return successfulHandle(calls.length === 1 ? "resp_poisoned" : "resp_recovered");
      },
    };
    const handler = createRouterStandaloneSearchHandler(router as never);
    const context = (): StandaloneSearchHandlerContext => {
      const lease = { release() {} };
      transferredLeases.push(lease);
      return {
        signal: new AbortController().signal,
        incomingLease: lease,
        transferIncomingLease: () => {
          leaseTransfers++;
          return lease;
        },
        clientId: "search-client",
        authorizationScope,
        encodedBodyBytes: 100,
        structuralBodyBytes: 90,
      };
    };

    await handler(request(), context());
    const recovered = await handler(request(), context());
    expect(recovered).toMatchObject({ kind: "success", status: 200 });
    expect(calls).toHaveLength(3);
    expect(calls[1]).toMatchObject({ previous_response_id: "resp_poisoned" });
    expect(calls[2]).not.toHaveProperty("previous_response_id");
    expect(calls[2]!.input).toEqual(calls[0]!.input);
    expect(attemptLeases[1]).not.toBe(transferredLeases[1]);
    expect(attemptLeases[2]).toBe(transferredLeases[1]);
    expect(leaseTransfers).toBe(2);

    await handler(request(), context());
    expect(calls[3]).toMatchObject({ previous_response_id: "resp_recovered" });

    rootObsolete = true;
    await expect(handler(request(), context())).rejects.toBeInstanceOf(
      ResponseProvenanceObsoleteError,
    );
    expect(calls.slice(4)).toHaveLength(2);
    expect(calls[4]).toMatchObject({ previous_response_id: "resp_recovered" });
    expect(calls[5]).not.toHaveProperty("previous_response_id");
    expect(leaseTransfers).toBe(4);
  });

  it("expires opaque search refs without a root retry and retains lineage on generic failure", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const calls: Record<string, unknown>[] = [];
    let failure: "none" | "generic" | "obsolete" | "post-handle" = "none";
    let leaseTransfers = 0;
    const router = {
      async startRelay(body: Record<string, unknown>): Promise<RunHandle> {
        calls.push(structuredClone(body));
        if (body.previous_response_id === "resp_stable") {
          if (failure === "generic") throw new Error("generic search failure");
          if (failure === "obsolete") throw new ResponseProvenanceObsoleteError();
        }
        const postHandleFailure = body.previous_response_id === "resp_stable" &&
          failure === "post-handle";
        return {
          async *events() {},
          discardUnconsumedEvents() {},
          upstreamPrelude: Promise.resolve({
            status: 200,
            contentType: "text/event-stream",
            upstreamRequestId: null,
          }),
          final: postHandleFailure
            ? Promise.reject(new ResponseProvenanceObsoleteError())
            : Promise.resolve({
                id: "resp_stable",
                status: "completed",
                output: [{
                  type: "message",
                  content: [{ type: "output_text", text: "grounded result" }],
                }],
              }),
          stats: { bytes: 1, firstByteMs: 1, frames: 1 },
        };
      },
    };
    const handler = createRouterStandaloneSearchHandler(router as never);
    const context = (): StandaloneSearchHandlerContext => ({
      signal: new AbortController().signal,
      transferIncomingLease: () => {
        leaseTransfers++;
        return undefined;
      },
      clientId: "search-client",
      authorizationScope,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    });

    await handler(request(), context());
    failure = "generic";
    await expect(handler(request(), context())).rejects.toThrow("generic search failure");
    failure = "none";
    await handler(request(), context());
    expect(calls[1]).toMatchObject({ previous_response_id: "resp_stable" });
    expect(calls[2]).toMatchObject({ previous_response_id: "resp_stable" });

    failure = "post-handle";
    await expect(handler(request(), context())).rejects.toBeInstanceOf(
      ResponseProvenanceObsoleteError,
    );
    failure = "none";
    await handler(request(), context());
    expect(calls[3]).toMatchObject({ previous_response_id: "resp_stable" });
    expect(calls[4]).toMatchObject({ previous_response_id: "resp_stable" });

    failure = "obsolete";
    const expired = await handler(request({
      commands: { open: [{ ref_id: "turn0search0" }] },
    }), context());
    expect(expired).toMatchObject({
      kind: "failure",
      status: 400,
      body: { error: { code: "search_reference_expired" } },
    });
    expect(calls).toHaveLength(6);
    expect(calls[5]).toMatchObject({ previous_response_id: "resp_stable" });

    failure = "none";
    await handler(request(), context());
    expect(calls[6]).not.toHaveProperty("previous_response_id");

    failure = "obsolete";
    const expiredClick = await handler(request({
      commands: { click: [{ ref_id: "https://example.com/page", id: 1 }] },
    }), context());
    expect(expiredClick).toMatchObject({
      kind: "failure",
      status: 400,
      body: { error: { code: "search_reference_expired" } },
    });
    expect(calls).toHaveLength(8);

    failure = "none";
    await handler(request(), context());
    failure = "obsolete";
    const recoveredLiteralUrl = await handler(request({
      commands: { open: [{ ref_id: "https://example.com/page" }] },
    }), context());
    expect(recoveredLiteralUrl).toMatchObject({ kind: "success", status: 200 });
    expect(calls[9]).toMatchObject({ previous_response_id: "resp_stable" });
    expect(calls[10]).not.toHaveProperty("previous_response_id");
    expect(leaseTransfers).toBe(10);
  });

  it("retries one definitive HTTP-missing search parent but never retries after a relay event", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 404,
      value: { error: {
        message: "Previous response was not found",
        type: "invalid_request_error",
        param: "previous_response_id",
        code: "previous_response_not_found",
      } },
    });
    const missing = new V2CommittedError(
      providerError.error.message,
      providerError.error.code!,
      false,
      undefined,
      providerError,
    );
    const calls: Record<string, unknown>[] = [];
    let missingFrames: number | null = null;
    const completed = (id: string): RunHandle => ({
      async *events() {},
      discardUnconsumedEvents() {},
      upstreamPrelude: Promise.resolve({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: null,
      }),
      final: Promise.resolve({
        id,
        status: "completed",
        output: [{
          type: "message",
          content: [{ type: "output_text", text: "grounded result" }],
        }],
      }),
      stats: { bytes: 1, firstByteMs: 1, frames: 1 },
    });
    const router = {
      async startRelay(body: Record<string, unknown>): Promise<RunHandle> {
        calls.push(structuredClone(body));
        if (missingFrames !== null && Object.hasOwn(body, "previous_response_id")) {
          return {
            async *events() {},
            discardUnconsumedEvents() {},
            final: Promise.reject(missing),
            stats: { bytes: 1, firstByteMs: 1, frames: missingFrames },
          };
        }
        return completed(calls.length === 1 ? "resp_http_parent" : "resp_http_recovered");
      },
    };
    const handler = createRouterStandaloneSearchHandler(router as never);
    const context = (): StandaloneSearchHandlerContext => ({
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      clientId: "search-client",
      authorizationScope,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    });

    await handler(request(), context());
    missingFrames = 0;
    const recovered = await handler(request(), context());
    expect(recovered).toMatchObject({ kind: "success", status: 200 });
    expect(calls[1]).toMatchObject({ previous_response_id: "resp_http_parent" });
    expect(calls[2]).not.toHaveProperty("previous_response_id");

    missingFrames = 1;
    await expect(handler(request(), context())).rejects.toBe(missing);
    expect(calls).toHaveLength(4);
    missingFrames = null;
    await handler(request(), context());
    expect(calls[4]).toMatchObject({ previous_response_id: "resp_http_recovered" });
  });

  it("retains cached ingress bytes through terminal while ending admission timing immediately", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    let calls = 0;
    let releaseFinal!: (value: Record<string, unknown>) => void;
    const gatedFinal = new Promise<Record<string, unknown>>(resolve => { releaseFinal = resolve; });
    let cachedStarted!: () => void;
    const cachedStart = new Promise<void>(resolve => { cachedStarted = resolve; });
    let capturedLease: unknown;
    const response = (id: string) => ({
      id,
      status: "completed",
      output: [{
        type: "message",
        content: [{ type: "output_text", text: "grounded result" }],
      }],
    });
    const router = {
      async startRelay(_body: Record<string, unknown>, options: { incomingLease?: unknown }) {
        calls++;
        if (calls === 1) {
          return {
            async *events() {},
            discardUnconsumedEvents() {},
            upstreamPrelude: Promise.resolve({
              status: 200,
              contentType: "text/event-stream",
              upstreamRequestId: null,
            }),
            final: Promise.resolve(response("resp_lease_parent")),
            stats: { bytes: 1, firstByteMs: 1, frames: 1 },
          } satisfies RunHandle;
        }
        capturedLease = options.incomingLease;
        cachedStarted();
        return {
          async *events() {},
          discardUnconsumedEvents() {},
          upstreamPrelude: Promise.resolve({
            status: 200,
            contentType: "text/event-stream",
            upstreamRequestId: null,
          }),
          final: gatedFinal,
          stats: { bytes: 1, firstByteMs: 1, frames: 1 },
        } satisfies RunHandle;
      },
    };
    const handler = createRouterStandaloneSearchHandler(router as never);
    const emptyContext: StandaloneSearchHandlerContext = {
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      clientId: "search-client",
      authorizationScope,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    };
    await handler(request(), emptyContext);

    let transfers = 0;
    let completions = 0;
    let releases = 0;
    const lease = {
      bodyBytes: 1024,
      reservedBytes: 4096,
      release() { releases++; },
      completeAdmission() { completions++; },
    };
    const pending = handler(request(), {
      ...emptyContext,
      incomingLease: lease,
      transferIncomingLease: () => {
        transfers++;
        return lease;
      },
    });
    await cachedStart;
    await Promise.resolve();
    expect(transfers).toBe(1);
    expect(completions).toBe(1);
    expect(releases).toBe(0);
    expect(capturedLease).not.toBe(lease);

    releaseFinal(response("resp_lease_child"));
    await pending;
    expect(releases).toBe(1);
  });

  it("does not advance a search session before oversized output is rejected", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const sessions = new StandaloneSearchSessionIndex({
      hmacKey: new Uint8Array(32).fill(0x5a),
    });
    const calls: Record<string, unknown>[] = [];
    const router = {
      async startRelay(body: Record<string, unknown>): Promise<RunHandle> {
        calls.push(structuredClone(body));
        const oversized = calls.length === 2;
        return {
          async *events() {},
          discardUnconsumedEvents() {},
          upstreamPrelude: Promise.resolve({
            status: 200,
            contentType: "text/event-stream",
            upstreamRequestId: null,
          }),
          final: Promise.resolve({
            id: oversized ? "resp_invisible_oversized" : "resp_visible_parent",
            status: "completed",
            output: [
              ...(oversized ? [{
                type: "web_search_call",
                results: [{ payload: "x".repeat(STANDALONE_SEARCH_MAX_RESPONSE_BYTES + 1024) }],
              }] : []),
              {
                type: "message",
                content: [{ type: "output_text", text: "grounded result" }],
              },
            ],
          }),
          stats: { bytes: 1, firstByteMs: 1, frames: 1 },
        };
      },
    };
    const handler = createRouterStandaloneSearchHandler(router as never, sessions);
    const context = (): StandaloneSearchHandlerContext => ({
      signal: new AbortController().signal,
      transferIncomingLease: () => undefined,
      clientId: "search-client",
      authorizationScope,
      encodedBodyBytes: 100,
      structuralBodyBytes: 90,
    });

    await handler(request(), context());
    await expect(handler(request(), context())).rejects.toThrow(
      "Standalone search returned an invalid response",
    );
    expect(sessions.snapshot().records).toBe(1);
    await handler(request(), context());
    expect(calls[1]).toMatchObject({ previous_response_id: "resp_visible_parent" });
    expect(calls[2]).toMatchObject({ previous_response_id: "resp_visible_parent" });
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
