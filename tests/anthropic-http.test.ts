import { afterEach, describe, expect, it } from "bun:test";
import {
  request as httpRequest,
  type IncomingHttpHeaders,
  type Server,
} from "node:http";
import type { RunHandle } from "../src/relay/run-handle";
import type { SseFrame } from "../src/relay/sse";
import {
  anthropicModelsFromEnv,
  type AnthropicModelOptions,
} from "../src/anthropic/model";
import { estimateAnthropicInputTokens } from "../src/anthropic/token-count";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import {
  createGhostProvServer,
  requestBodyLimitForUrl,
} from "../src/server/runtime";
import {
  finalResponse,
  messageOutput,
  streamFrame,
  textPart,
} from "./support/anthropic-stream";

const MiB = 1024 ** 2;
const ANTHROPIC_MAX_BODY_BYTES = 32 * MiB;
const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => {
    server.close(() => resolve());
  })));
});

type RelayOptions = {
  clientId?: string;
  source?: "http" | "websocket" | "image" | "anthropic" | "internal";
  signal?: AbortSignal;
};

type RelayCall = {
  body: Record<string, unknown>;
  options: RelayOptions;
};

function completed(id: string, text = "hello from GhostProv") {
  return finalResponse(id, [messageOutput(`msg_${id}`, [textPart(text)])]);
}

function relayHandle(
  final: Promise<unknown> | unknown,
  events: () => AsyncGenerator<SseFrame> = async function* () {},
): RunHandle {
  return {
    events,
    final: Promise.resolve(final),
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

class FakeRouter implements RouterLike {
  readonly calls: RelayCall[] = [];

  constructor(
    private readonly makeHandle: (
      body: Record<string, unknown>,
      options: RelayOptions,
      call: number,
    ) => Promise<RunHandle> | RunHandle = (_body, _options, call) =>
      relayHandle(completed(`resp_${call}`)),
  ) {}

  async startRelay(
    body: Record<string, unknown>,
    options: RelayOptions = {},
  ): Promise<RunHandle> {
    this.calls.push({ body, options });
    return this.makeHandle(body, options, this.calls.length);
  }

  snapshot() {
    return {
      capacity: 1,
      active: 0,
      available: 1,
      queued: 0,
      runtimes: [],
      resources: {},
    };
  }
}

function messageRequest(overrides: Record<string, unknown> = {}) {
  return {
    model: "claude-sonnet-4-6",
    max_tokens: 64,
    messages: [{ role: "user", content: "hello" }],
    ...overrides,
  };
}

function requestId(response: Response): string {
  const value = response.headers.get("request-id");
  expect(value).toMatch(/^req_[a-f0-9]{32}$/);
  return value!;
}

async function expectAnthropicError(response: Response, status: number, message: RegExp) {
  expect(response.status).toBe(status);
  const id = requestId(response);
  const body = await response.json() as any;
  expect(body).toMatchObject({
    type: "error",
    error: { message: expect.stringMatching(message) },
    request_id: id,
  });
  return body;
}

async function listen(app: ReturnType<typeof createGhostProvApp>, router: RouterLike) {
  const server = createGhostProvServer({
    app,
    router,
    candidate: true,
    port: 0,
    maxBodyBytes: 64 * MiB,
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  return address.port;
}

function nodeRequest(options: {
  port: number;
  path: string;
  headers: Record<string, string | number>;
  write?: (request: ReturnType<typeof httpRequest>) => void;
}): Promise<{ status: number; headers: IncomingHttpHeaders; body: string }> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const request = httpRequest({
      hostname: "127.0.0.1",
      port: options.port,
      path: options.path,
      method: "POST",
      headers: options.headers,
    }, response => {
      response.setEncoding("utf8");
      let body = "";
      response.on("data", chunk => { body += chunk; });
      response.on("end", () => {
        settled = true;
        resolve({ status: response.statusCode ?? 0, headers: response.headers, body });
      });
    });
    request.on("error", error => {
      if (!settled) reject(error);
    });
    if (options.write) options.write(request);
    else request.end();
  });
}

describe("Anthropic HTTP routes", () => {
  it("serves stable and beta JSON through one Anthropic router job with tolerant headers", async () => {
    const models = anthropicModelsFromEnv({
      GP_ANTHROPIC_OPUS_MODEL: "custom-opus",
      GP_ANTHROPIC_SONNET_MODEL: "custom-sonnet",
      GP_ANTHROPIC_HAIKU_MODEL: "custom-haiku",
      GP_ANTHROPIC_DEFAULT_MODEL: "custom-default",
    });
    const router = new FakeRouter();
    const app = createGhostProvApp({ router, anthropicModels: models });

    const stable = await app.request("/v1/messages", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "text/event-stream",
        "x-api-key": "local-dummy-key",
        "anthropic-version": "2023-06-01",
        "stainless-lang": "js",
        "x-unknown-claude-header": "tolerated",
      },
      body: JSON.stringify(messageRequest()),
    });
    const beta = await app.request("/v1/messages?beta=true", {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: "Bearer local-dummy-token",
        "x-api-key": "also-tolerated",
        "anthropic-beta": "legacy-private-beta,unknown-future-beta",
        "x-ghostprov-client-id": "claude-session",
      },
      body: JSON.stringify(messageRequest({ model: "claude-opus-4-6" })),
    });

    expect(stable.status).toBe(200);
    expect(stable.headers.get("content-type")).toContain("application/json");
    expect((await stable.json() as any).model).toBe("claude-sonnet-4-6");
    requestId(stable);
    expect(beta.status).toBe(200);
    expect((await beta.json() as any).model).toBe("claude-opus-4-6");
    requestId(beta);
    expect(router.calls).toHaveLength(2);
    expect(router.calls.map(call => ({
      source: call.options.source,
      clientId: call.options.clientId,
      model: call.body.model,
      stream: call.body.stream,
    }))).toEqual([
      { source: "anthropic", clientId: "http", model: "custom-sonnet", stream: false },
      { source: "anthropic", clientId: "claude-session", model: "custom-opus", stream: false },
    ]);
  });

  it("counts stable and beta requests without dispatching a relay", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const body = {
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: "count this" }],
      system: "system text",
      tools: [{ name: "Read", input_schema: { type: "object", properties: {} } }],
    };

    for (const path of [
      "/v1/messages/count_tokens",
      "/v1/messages/count_tokens?beta=true",
    ]) {
      const response = await app.request(path, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: "Bearer ignored",
          "anthropic-beta": "unknown-beta",
        },
        body: JSON.stringify(body),
      });
      expect(response.status).toBe(200);
      requestId(response);
      expect(await response.json()).toEqual({
        input_tokens: estimateAnthropicInputTokens(body),
      });
    }
    expect(router.calls).toHaveLength(0);
  });

  it("returns current Anthropic JSON errors before a stream starts", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });

    await expectAnthropicError(await app.request("/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{not-json",
    }), 400, /valid JSON/i);
    await expectAnthropicError(await app.request("/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "claude-sonnet-4-6", messages: [] }),
    }), 400, /max_tokens/i);
    await expectAnthropicError(await app.request("/v1/messages/count_tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ messages: [] }),
    }), 400, /model is required/i);
    await expectAnthropicError(await app.request("/v1/messages/count_tokens", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "claude-sonnet-4-6", messages: "invalid" }),
    }), 400, /messages is required/i);
    expect(router.calls).toHaveLength(0);
  });

  it("drains relay events concurrently with final for non-streaming requests", async () => {
    let drained = false;
    let resolveFinal!: (value: unknown) => void;
    const final = new Promise<unknown>(resolve => { resolveFinal = resolve; });
    const router = new FakeRouter(() => relayHandle(final, async function* () {
      drained = true;
      resolveFinal(completed("resp_drained", "drained"));
    }));
    const app = createGhostProvApp({ router });

    const response = await Promise.race([
      app.request("/v1/messages", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(messageRequest()),
      }),
      new Promise<never>((_, reject) => setTimeout(
        () => reject(new Error("non-streaming Anthropic route deadlocked")),
        500,
      )),
    ]);

    expect(response.status).toBe(200);
    expect(drained).toBe(true);
    expect((await response.json() as any).content[0].text).toBe("drained");
    expect(router.calls).toHaveLength(1);
  });

  it("emits canonical named SSE from body.stream with request and no-buffer headers", async () => {
    const final = completed("resp_stream", "streamed");
    const router = new FakeRouter(() => relayHandle(final, async function* () {
      yield streamFrame("response.completed", { response: final });
    }));
    const app = createGhostProvApp({ router, keepAliveMs: 60_000 });

    const response = await app.request("/v1/messages?beta=true", {
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json" },
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const text = await response.text();

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream; charset=utf-8");
    expect(response.headers.get("cache-control")).toContain("no-cache");
    expect(response.headers.get("x-accel-buffering")).toBe("no");
    requestId(response);
    expect(text).toContain("event: message_start\n");
    expect(text).toContain("event: content_block_start\n");
    expect(text).toContain("event: content_block_delta\n");
    expect(text).toContain("event: content_block_stop\n");
    expect(text).toContain("event: message_delta\n");
    expect(text).toContain("event: message_stop\n");
    expect(text).not.toContain("[DONE]");
    expect(router.calls).toHaveLength(1);
    expect(router.calls[0].body.stream).toBe(true);
  });

  it("keeps one pending upstream read across keepalives and aborts/returns on cancel", async () => {
    let nextCalls = 0;
    let returnCalls = 0;
    let settleNext: ((result: IteratorResult<SseFrame>) => void) | undefined;
    const iterator = {
      next() {
        nextCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(resolve => { settleNext = resolve; });
      },
      return() {
        returnCalls += 1;
        settleNext?.({ done: true, value: undefined });
        return Promise.resolve({ done: true, value: undefined });
      },
      throw(error?: unknown) {
        return Promise.reject(error);
      },
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    let relaySignal: AbortSignal | undefined;
    let finalAccesses = 0;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      return {
        events: () => iterator,
        get final() {
          finalAccesses += 1;
          return new Promise(() => undefined);
        },
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      };
    });
    const app = createGhostProvApp({ router, keepAliveMs: 10 });
    const response = await app.request("/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();

    expect(decoder.decode((await reader.read()).value)).toContain("event: message_start");
    expect(decoder.decode((await reader.read()).value)).toBe(": keepalive\n\n");
    expect(decoder.decode((await reader.read()).value)).toBe(": keepalive\n\n");
    expect(nextCalls).toBe(1);

    await new Promise(resolve => setTimeout(resolve, 0));
    await reader.cancel("test cancellation");
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalls).toBe(1);
    expect(finalAccesses).toBe(0);
  });

  it("links the incoming request signal and sanitizes midstream failures", async () => {
    let relaySignal: AbortSignal | undefined;
    let failNext!: (error: unknown) => void;
    const iterator = {
      next: () => new Promise<IteratorResult<SseFrame>>((_resolve, reject) => { failNext = reject; }),
      return: () => Promise.resolve({ done: true, value: undefined }),
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      return relayHandle(new Promise(() => undefined), () => iterator);
    });
    const app = createGhostProvApp({ router, keepAliveMs: 60_000 });
    const incoming = new AbortController();
    const response = await app.fetch(new Request("http://local/v1/messages", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(messageRequest({ stream: true })),
      signal: incoming.signal,
    }));
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(new TextDecoder().decode(first.value)).toContain("event: message_start");

    incoming.abort();
    expect(relaySignal?.aborted).toBe(true);
    failNext(new Error("secret upstream payload must not leak"));
    let rest = "";
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      rest += new TextDecoder().decode(chunk.value);
    }
    expect(rest).toContain("event: error\n");
    expect(rest).not.toContain("secret upstream payload");
  });

  it("closes after a translator-detected stream error without draining upstream", async () => {
    let nextCalls = 0;
    let returnCalls = 0;
    const iterator = {
      next() {
        nextCalls += 1;
        if (nextCalls === 1) {
          return Promise.resolve({
            done: false as const,
            value: { event: "response.output_text.delta", data: "secret malformed frame" },
          });
        }
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      return() {
        returnCalls += 1;
        return Promise.resolve({ done: true as const, value: undefined });
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter(() => relayHandle(new Promise(() => undefined), () => iterator));
    const response = await createGhostProvApp({ router, keepAliveMs: 60_000 }).request(
      "/v1/messages",
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(messageRequest({ stream: true })),
      },
    );
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();

    expect(decoder.decode((await reader.read()).value)).toContain("event: message_start");
    const error = decoder.decode((await reader.read()).value);
    expect(error).toContain("event: error\n");
    expect(error).not.toContain("secret malformed frame");
    try {
      const terminal = await Promise.race([
        reader.read(),
        new Promise<never>((_, reject) => setTimeout(
          () => reject(new Error("translator error did not close the stream")),
          100,
        )),
      ]);
      expect(terminal.done).toBe(true);
    } finally {
      await reader.cancel().catch(() => undefined);
    }
    expect(nextCalls).toBe(1);
    expect(returnCalls).toBe(1);
  });
});

describe("Anthropic runtime body limits", () => {
  it("selects 32 MiB for both Anthropic paths without lowering unrelated routes", () => {
    expect(requestBodyLimitForUrl("/v1/messages?beta=true", 64 * MiB))
      .toBe(ANTHROPIC_MAX_BODY_BYTES);
    expect(requestBodyLimitForUrl("/v1/messages/count_tokens?beta=true", 64 * MiB))
      .toBe(ANTHROPIC_MAX_BODY_BYTES);
    expect(requestBodyLimitForUrl("/v1/responses", 64 * MiB)).toBe(64 * MiB);
    expect(requestBodyLimitForUrl("/v1/images/edits", 64 * MiB)).toBe(64 * MiB);
    expect(requestBodyLimitForUrl("/v1/messages", 16 * MiB)).toBe(16 * MiB);
  });

  it("rejects an oversized declared Anthropic body before buffering", async () => {
    const router = new FakeRouter();
    const port = await listen(createGhostProvApp({ router }), router);
    const response = await nodeRequest({
      port,
      path: "/v1/messages?beta=true",
      headers: {
        "content-type": "application/json",
        "content-length": ANTHROPIC_MAX_BODY_BYTES + 1,
      },
    });

    expect(response.status).toBe(413);
    const body = JSON.parse(response.body);
    expect(response.headers["request-id"]).toBe(body.request_id);
    expect(body).toMatchObject({ type: "error", error: { type: "request_too_large" } });
    expect(router.calls).toHaveLength(0);
  });

  it("rejects an oversized chunked Anthropic body while buffering", async () => {
    const router = new FakeRouter();
    const port = await listen(createGhostProvApp({ router }), router);
    const response = await nodeRequest({
      port,
      path: "/v1/messages/count_tokens?beta=true",
      headers: { "content-type": "application/json", "transfer-encoding": "chunked" },
      write(request) {
        request.write(Buffer.alloc(ANTHROPIC_MAX_BODY_BYTES));
        request.end(Buffer.alloc(1));
      },
    });

    expect(response.status).toBe(413);
    const body = JSON.parse(response.body);
    expect(response.headers["request-id"]).toBe(body.request_id);
    expect(body).toMatchObject({ type: "error", error: { type: "request_too_large" } });
    expect(router.calls).toHaveLength(0);
  }, 15_000);
});

describe("Anthropic model options", () => {
  it("uses documented defaults and explicit GP_ANTHROPIC environment overrides", () => {
    const defaults = anthropicModelsFromEnv({});
    expect(defaults).toMatchObject({
      opus: "gpt-5.6-sol",
      sonnet: "gpt-5.6-terra",
      haiku: "gpt-5.6-luna",
      fallback: "gpt-5.6-sol",
    });
    const overridden: AnthropicModelOptions = anthropicModelsFromEnv({
      GP_ANTHROPIC_OPUS_MODEL: "o",
      GP_ANTHROPIC_SONNET_MODEL: "s",
      GP_ANTHROPIC_HAIKU_MODEL: "h",
      GP_ANTHROPIC_DEFAULT_MODEL: "d",
    });
    expect(overridden).toMatchObject({ opus: "o", sonnet: "s", haiku: "h", fallback: "d" });
    expect([...overridden.passthrough]).toEqual(expect.arrayContaining(["o", "s", "h", "d"]));
  });
});
