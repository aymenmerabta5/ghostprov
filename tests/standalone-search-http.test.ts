import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import {
  createGhostProvApp,
  type RouterLike,
} from "../src/server/app";
import {
  parseStandaloneSearchRequest,
  STANDALONE_SEARCH_MAX_REQUEST_BYTES,
  STANDALONE_SEARCH_MAX_RESPONSE_BYTES,
  type StandaloneSearchHandler,
  type StandaloneSearchHandlerResult,
  type StandaloneSearchRequest,
} from "../src/http/standalone-search";
import { requestBodyLimitForUrl } from "../src/http/request-policy";
import type { IncomingResourceLease } from "../src/router/router";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { attachAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";

const API_KEY = "standalone-search-test-key";
const JSON_HEADERS = { "content-type": "application/json" };

function router(): RouterLike {
  return {
    async startRelay(): Promise<RunHandle> {
      throw new Error("Responses relay must not handle standalone search");
    },
    snapshot: () => ({
      capacity: 1,
      active: 0,
      available: 1,
      queued: 0,
      runtimes: [],
      resources: {},
    }),
  };
}

function requestBody(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "01a05ef8-afbd-7131-83cf-ff3a68888cf2",
    model: "gpt-5.5",
    input: [{
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "Use web search." }],
      internal_chat_message_metadata_passthrough: {
        content_item_kinds: ["user.text"],
        create_time: 1_788_299_751.4971955,
        turn_id: "01a05ef8-aff0-7d61-b17f-e2b890b9f3c5",
      },
    }],
    commands: {
      search_query: [{ q: "OpenAI API documentation", domains: ["developers.openai.com"] }],
      response_length: "short",
    },
    settings: {
      allowed_callers: ["direct"],
      external_web_access: true,
      filters: { allowed_domains: ["developers.openai.com"] },
      image_settings: { max_results: 3, caption: true },
      search_context_size: "high",
      user_location: {
        type: "approximate",
        country: "DZ",
        region: "Algiers",
        city: "Algiers",
        timezone: "Africa/Algiers",
      },
    },
    max_output_tokens: 10_000,
    ...overrides,
  };
}

function app(handler?: StandaloneSearchHandler, apiKey?: string) {
  return createGhostProvApp({
    router: router(),
    standaloneSearch: handler,
    ...(apiKey === undefined ? {} : { apiKey }),
  });
}

async function post(
  handler: StandaloneSearchHandler | undefined,
  body: string | Record<string, unknown>,
  headers: Record<string, string> = JSON_HEADERS,
) {
  return app(handler).request("/v1/alpha/search", {
    method: "POST",
    headers,
    body: typeof body === "string" ? body : JSON.stringify(body),
  });
}

function success(body: Record<string, unknown>, status = 200): StandaloneSearchHandlerResult {
  return { kind: "success", status, body: body as never };
}

describe("Codex standalone search HTTP contract", () => {
  it("caps network body collection at the standalone-search contract boundary", () => {
    expect(requestBodyLimitForUrl("/v1/alpha/search", 64 * 1024 * 1024))
      .toBe(STANDALONE_SEARCH_MAX_REQUEST_BYTES);
    expect(requestBodyLimitForUrl("/v1/alpha/search?trace=ignored", 512 * 1024))
      .toBe(512 * 1024);
  });

  it("passes one deeply immutable validated body and preserves the exact success JSON", async () => {
    const output = {
      output: "fixture",
      encrypted_output: null,
      results: [{ type: "computer_initialize_state", id: "7" }],
      future_field: { retained: true },
    };
    let calls = 0;
    const response = await post(async (body, context) => {
      calls++;
      expect(context.signal).toBeInstanceOf(AbortSignal);
      expect(Object.isFrozen(context)).toBe(true);
      expect(Object.isFrozen(body)).toBe(true);
      expect(Object.isFrozen(body.commands)).toBe(true);
      expect(Object.isFrozen(body.commands!.search_query?.[0])).toBe(true);
      const input = body.input as ReadonlyArray<{ content?: unknown }>;
      expect(Object.isFrozen(input[0]?.content)).toBe(true);
      expect(() => {
        (body.commands!.search_query![0] as { q: string }).q = "mutated";
      }).toThrow();
      return success(output, 201);
    }, requestBody());

    expect(calls).toBe(1);
    expect(response.status).toBe(201);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(await response.text()).toBe(JSON.stringify(output));
  });

  it("keeps ingress ownership through validation and exposes a one-shot router handoff", async () => {
    const raw = JSON.stringify(requestBody());
    let releases = 0;
    const lease: IncomingResourceLease = {
      bodyBytes: Buffer.byteLength(raw),
      release: () => { releases++; },
    };
    const searchApp = app(async (_body, context) => {
      expect(context.incomingLease).toBe(lease);
      expect(context.clientId).toBe("http-connection-7");
      expect(context.encodedBodyBytes).toBe(Buffer.byteLength(raw));
      expect(context.structuralBodyBytes).toBe(Buffer.byteLength(raw));
      expect(context.transferIncomingLease()).toBe(lease);
      expect(() => context.transferIncomingLease()).toThrow(/already transferred/);
      lease.release();
      return success({ output: "owned" });
    });
    const response = await searchApp.fetch(new Request("http://localhost/v1/alpha/search", {
      method: "POST",
      headers: JSON_HEADERS,
      body: raw,
    }), {
      incomingBodyLease: lease,
      httpConnectionClientId: "http-connection-7",
    });
    expect(response.status).toBe(200);
    expect(releases).toBe(1);

    let invalidReleases = 0;
    const invalidLease: IncomingResourceLease = {
      bodyBytes: 2,
      release: () => { invalidReleases++; },
    };
    const invalid = await searchApp.fetch(new Request("http://localhost/v1/alpha/search", {
      method: "POST",
      headers: JSON_HEADERS,
      body: "{}",
    }), { incomingBodyLease: invalidLease });
    expect(invalid.status).toBe(400);
    expect(invalidReleases).toBe(1);
  });

  it("inherits the app authentication boundary before invoking the executor", async () => {
    let calls = 0;
    const handler: StandaloneSearchHandler = async () => {
      calls++;
      return success({ output: "ok" });
    };
    const searchApp = app(handler, API_KEY);
    const unauthorized = await searchApp.request("/v1/alpha/search", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify(requestBody()),
    });
    expect(unauthorized.status).toBe(401);
    expect(unauthorized.headers.get("www-authenticate")).toBe('Bearer realm="ghostprov"');
    expect(calls).toBe(0);

    const authorized = await searchApp.request("/v1/alpha/search", {
      method: "POST",
      headers: { ...JSON_HEADERS, authorization: `Bearer ${API_KEY}` },
      body: JSON.stringify(requestBody()),
    });
    expect(authorized.status).toBe(200);
    expect(calls).toBe(1);
  });

  it("returns a clear authenticated 503 when no executor is configured", async () => {
    const searchApp = app(undefined, API_KEY);
    const unauthorized = await searchApp.request("/v1/alpha/search", {
      method: "POST",
      headers: JSON_HEADERS,
      body: "not-json",
    });
    expect(unauthorized.status).toBe(401);

    const unavailable = await searchApp.request("/v1/alpha/search", {
      method: "POST",
      headers: { ...JSON_HEADERS, authorization: `Bearer ${API_KEY}` },
      body: "not-json",
    });
    expect(unavailable.status).toBe(503);
    expect(await unavailable.json()).toMatchObject({
      error: { code: "standalone_search_unavailable" },
    });
  });

  it("accepts only a JSON object with the fixed standalone-search fields", async () => {
    let calls = 0;
    const handler: StandaloneSearchHandler = async () => {
      calls++;
      return success({ output: "unexpected" });
    };
    for (const body of ["not-json", "null", "[]", '"text"']) {
      const response = await post(handler, body);
      expect(response.status, body).toBe(400);
    }
    for (const forbidden of [
      { url: "https://example.com" },
      { method: "DELETE" },
      { headers: { authorization: "Bearer secret" } },
    ]) {
      const response = await post(handler, requestBody(forbidden));
      expect(response.status, Object.keys(forbidden)[0]).toBe(400);
      expect((await response.json() as any).error.code).toBe("unknown_parameter");
    }
    expect(calls).toBe(0);
  });

  it("validates command and settings boundaries before dispatch", async () => {
    const handler: StandaloneSearchHandler = async () => success({ output: "unexpected" });
    const cases = [
      requestBody({ commands: { search_query: [{ q: "x", url: "https://example.com" }] } }),
      requestBody({ commands: { click: [{ ref_id: "turn0search0", id: 1.5 }] } }),
      requestBody({ settings: { allowed_callers: ["indirect"] } }),
      requestBody({ settings: { external_web_access: "yes" } }),
      requestBody({ settings: { headers: { authorization: "secret" } } }),
      requestBody({ max_output_tokens: 0 }),
      requestBody({ id: "" }),
    ];
    for (const body of cases) {
      const response = await post(handler, body);
      expect(response.status).toBe(400);
    }
  });

  it("accepts Codex default commands, empty fallback queries, and optional fields", async () => {
    const seen: StandaloneSearchRequest[] = [];
    const handler: StandaloneSearchHandler = async body => {
      seen.push(body);
      return success({ output: "ok" });
    };
    const minimal = await post(handler, {
      id: "01a05ef8-afbd-7131-83cf-ff3a68888cf2",
      model: "gpt-5.6-sol",
    });
    const empty = await post(handler, requestBody({
      input: "search context",
      commands: { search_query: [{ q: "" }] },
      settings: {
        allowed_callers: ["shell", "code_interpreter"],
        external_web_access: "indexed",
      },
      reasoning: { effort: "low" },
    }));
    expect(minimal.status).toBe(200);
    expect(empty.status).toBe(200);
    expect(seen).toHaveLength(2);
  });

  it("rejects lone surrogates, excessive depth, and oversized requests", async () => {
    const handler: StandaloneSearchHandler = async () => success({ output: "unexpected" });
    const unicode = await post(handler, requestBody({
      input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "bad\ud800" }] }],
    }));
    expect(unicode.status).toBe(400);
    expect((await unicode.json() as any).error.code).toBe("invalid_unicode");

    let nested: Record<string, unknown> = { text: "leaf" };
    for (let index = 0; index < 260; index++) nested = { child: nested };
    await expect(parseStandaloneSearchRequest(requestBody({ input: [nested] })))
      .rejects.toMatchObject({ status: 400 });

    const oversizedText = "x".repeat(STANDALONE_SEARCH_MAX_REQUEST_BYTES + 1);
    await expect(parseStandaloneSearchRequest(requestBody({
      input: [{ type: "message", role: "user", content: [{ type: "input_text", text: oversizedText }] }],
    }))).rejects.toMatchObject({ status: 413 });
  });

  it("preserves only a bounded safe failure status, JSON body, and Retry-After", async () => {
    const body = {
      error: {
        message: "rate limited",
        type: "rate_limit_error",
        param: null,
        code: "rate_limit_exceeded",
      },
    };
    const response = await post(async () => ({
      kind: "failure",
      status: 429,
      body,
      retryAfter: "7",
    }), requestBody());
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("7");
    expect(response.headers.get("authorization")).toBeNull();
    expect(await response.text()).toBe(JSON.stringify(body));
  });

  it("preserves a typed provider HTTP error through the shared safe error path", async () => {
    const body = {
      error: {
        message: "quota reached",
        type: "rate_limit_error",
        param: null,
        code: "organization_spend_limit_exceeded",
      },
    };
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 429,
      value: body,
      payload: body,
      headers: new Headers({
        "retry-after": "11",
        authorization: "Bearer must-not-cross",
      }),
    });
    const response = await post(async () => {
      throw Object.assign(new Error("typed provider failure"), { providerError });
    }, requestBody());
    expect(response.status).toBe(429);
    expect(response.headers.get("retry-after")).toBe("11");
    expect(response.headers.get("authorization")).toBeNull();
    expect(await response.json()).toEqual(body);
  });

  it("fails closed on unsafe result envelopes without reflecting callback secrets", async () => {
    const invalidResults: unknown[] = [
      { kind: "success", status: 204, body: { output: "x" } },
      { kind: "success", status: 200, body: {} },
      { kind: "failure", status: 302, body: { error: "x" } },
      { kind: "failure", status: 429, body: { error: "x" }, retryAfter: "7\r\nx-secret: value" },
      { kind: "failure", status: 429, body: { error: "x" }, headers: { authorization: "secret" } },
    ];
    for (const result of invalidResults) {
      const response = await post(async () => result as StandaloneSearchHandlerResult, requestBody());
      expect(response.status).toBe(502);
      expect(await response.text()).not.toContain("x-secret");
      expect(response.headers.get("retry-after")).toBeNull();
    }

    const thrown = await post(async () => {
      throw new Error("Bearer sk-must-not-leak");
    }, requestBody());
    expect(thrown.status).toBe(500);
    expect(await thrown.text()).not.toContain("sk-must-not-leak");
  });

  it("rejects oversized and malformed success bodies instead of partially serializing", async () => {
    const tooLarge = await post(async () => success({
      output: "x".repeat(STANDALONE_SEARCH_MAX_RESPONSE_BYTES + 1),
    }), requestBody());
    expect(tooLarge.status).toBe(502);
    expect((await tooLarge.json() as any).error.code).toBe("invalid_search_response");

    const unicode = await post(async () => success({ output: "bad\ud800" }), requestBody());
    expect(unicode.status).toBe(502);
    expect((await unicode.json() as any).error.code).toBe("invalid_search_response");
  });

  it("releases a transferred final permit when result validation rejects", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 2 * 1024 * 1024, jumboBytes: 0 });
    const invalid = attachAtomicBytePermit(
      { future_field: "missing required output" },
      await pool.acquire(1024, { label: "invalid-search-result" }),
    );
    const response = await post(async () => success(invalid), requestBody());
    expect(response.status).toBe(502);
    await response.text();
    expect(pool.snapshot().activePermits).toBe(0);
    pool.close();
  });
});
