import { afterEach, describe, expect, it } from "bun:test";
import {
  request as httpRequest,
  type IncomingHttpHeaders,
  type Server,
} from "node:http";
import type { RunHandle } from "../src/relay/run-handle";
import type { SseFrame } from "../src/relay/sse";
import { ApiError } from "../src/http/errors";
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
import type { IncomingResourceLease } from "../src/router/router";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import {
  finalResponse,
  messageOutput,
  streamFrame,
  textPart,
} from "./support/anthropic-stream";

const MiB = 1024 ** 2;
const ANTHROPIC_MAX_BODY_BYTES = 32 * MiB;
const servers: Server[] = [];
const ANTHROPIC_HEADERS = {
  "content-type": "application/json",
  "anthropic-version": "2023-06-01",
} as const;

afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => {
    server.close(() => resolve());
  })));
});

type RelayOptions = {
  clientId?: string;
  source?: "http" | "websocket" | "image" | "anthropic" | "internal";
  signal?: AbortSignal;
  incomingLease?: IncomingResourceLease;
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

async function waitFor(predicate: () => boolean, message: string): Promise<void> {
  const deadline = Date.now() + 500;
  while (!predicate() && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 1));
  }
  if (!predicate()) throw new Error(message);
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
        "anthropic-version": "2023-06-01",
        authorization: "Bearer local-dummy-token",
        "x-api-key": "also-tolerated",
        "anthropic-beta": "legacy-private-beta,unknown-future-beta",
        "x-ghostprov-client-id": "claude-session",
      },
      body: JSON.stringify(messageRequest({
        model: "claude-opus-4-6",
        context_management: { edits: [{ type: "clear_tool_uses_20250919" }] },
      })),
    });

    expect(stable.status).toBe(200);
    expect(stable.headers.get("content-type")).toContain("application/json");
    const stableBody = await stable.json() as any;
    expect(stableBody.model).toBe("claude-sonnet-4-6");
    expect(stableBody).not.toHaveProperty("context_management");
    expect(stableBody).not.toHaveProperty("diagnostics");
    requestId(stable);
    expect(beta.status).toBe(200);
    const betaBody = await beta.json() as any;
    expect(betaBody.model).toBe("claude-opus-4-6");
    expect(betaBody.context_management).toEqual({ applied_edits: [] });
    expect(betaBody).not.toHaveProperty("diagnostics");
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
          "anthropic-version": "2023-06-01",
          authorization: "Bearer ignored",
          ...(path.includes("beta=true") ? { "anthropic-beta": "unknown-beta" } : {}),
        },
        body: JSON.stringify(body),
      });
      expect(response.status).toBe(200);
      requestId(response);
      expect(await response.json()).toEqual(path.includes("beta=true")
        ? { input_tokens: estimateAnthropicInputTokens(body), context_management: null }
        : { input_tokens: estimateAnthropicInputTokens(body) });
    }
    expect(router.calls).toHaveLength(0);
  });

  it("shares current mid-conversation system validation between generation and count", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const validMessages = [
      { role: "user", content: "Apply an updated constraint" },
      { role: "system", content: "Use the updated constraint" },
    ];

    for (const path of ["/v1/messages", "/v1/messages/count_tokens"]) {
      const response = await app.request(path, {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify({
          model: "claude-opus-5",
          ...(path === "/v1/messages" ? { max_tokens: 64 } : {}),
          messages: validMessages,
        }),
      });
      expect(response.status, path).toBe(200);
    }
    expect(router.calls).toHaveLength(1);

    const invalid = [
      {
        name: "unsupported model",
        model: "claude-sonnet-5",
        messages: validMessages,
        error: /system role is not supported for this model/i,
      },
      {
        name: "invalid placement",
        model: "claude-opus-5",
        messages: [{ role: "system", content: "too early" }],
        error: /system message must immediately follow a user turn/i,
      },
    ];
    for (const value of invalid) {
      for (const path of ["/v1/messages", "/v1/messages/count_tokens"]) {
        const response = await app.request(path, {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify({
            model: value.model,
            ...(path === "/v1/messages" ? { max_tokens: 64 } : {}),
            messages: value.messages,
          }),
        });
        await expectAnthropicError(response, 400, value.error);
      }
    }
    expect(router.calls, "invalid requests must not dispatch").toHaveLength(1);
  });

  it("activates beta response shapes from a nonempty anthropic-beta header", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const headers = { ...ANTHROPIC_HEADERS, "anthropic-beta": "context-management-2025-06-27" };
    const message = await app.request("/v1/messages", {
      method: "POST",
      headers,
      body: JSON.stringify(messageRequest({ context_management: { edits: [] } })),
    });
    const count = await app.request("/v1/messages/count_tokens", {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: "claude-sonnet-4-6",
        messages: [],
        context_management: { edits: [] },
      }),
    });
    const stable = await app.request("/v1/messages", {
      method: "POST",
      headers: { ...ANTHROPIC_HEADERS, "anthropic-beta": "   " },
      body: JSON.stringify(messageRequest()),
    });

    expect((await message.json() as any).context_management).toEqual({ applied_edits: [] });
    expect(await count.json()).toMatchObject({ context_management: null });
    expect(await stable.json()).not.toHaveProperty("context_management");
  });

  it("validates current context-management edits for generation and count", async () => {
    const invalid = [
      { edits: "not-an-array" },
      { edits: [{ type: "unknown_edit" }] },
      { edits: [{ type: "clear_tool_uses_20250919", clear_tool_inputs: ["Read", 7] }] },
      { edits: [{ type: "clear_thinking_20251015", keep: { type: "thinking_turns", value: -1 } }] },
      { edits: [{ type: "compact_20260112", pause_after_compaction: "yes" }] },
    ];
    for (const context_management of invalid) {
      for (const path of ["/v1/messages?beta=true", "/v1/messages/count_tokens?beta=true"]) {
        const router = new FakeRouter();
        const body = path.includes("count_tokens")
          ? { model: "claude-sonnet-4-6", messages: [], context_management }
          : messageRequest({ context_management });
        const response = await createGhostProvApp({ router }).request(path, {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify(body),
        });
        expect(response.status).toBe(400);
        expect((await response.json() as any).error.message).toMatch(/context_management/i);
        expect(router.calls).toHaveLength(0);
      }
    }

    const valid = {
      edits: [
        {
          type: "clear_tool_uses_20250919",
          clear_at_least: { type: "input_tokens", value: 100 },
          clear_tool_inputs: ["Read"],
          exclude_tools: ["Keep"],
          keep: { type: "tool_uses", value: 2 },
          trigger: { type: "tool_uses", value: 10 },
        },
        { type: "clear_thinking_20251015", keep: { type: "thinking_turns", value: 2 } },
        {
          type: "compact_20260112",
          instructions: "Keep decisions",
          pause_after_compaction: false,
          trigger: { type: "input_tokens", value: 1000 },
        },
      ],
    };
    const router = new FakeRouter();
    const response = await createGhostProvApp({ router }).request("/v1/messages?beta=true", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest({ context_management: valid })),
    });
    expect(response.status).toBe(200);
    expect((await response.json() as any).context_management).toEqual({ applied_edits: [] });
  });

  it("accepts omitted, null, and empty context management as no-op", async () => {
    for (const contextManagement of [undefined, null, {}]) {
      const messageRouter = new FakeRouter();
      const countRouter = new FakeRouter();
      const override = contextManagement === undefined
        ? {}
        : { context_management: contextManagement };
      const message = await createGhostProvApp({ router: messageRouter }).request(
        "/v1/messages?beta=true",
        {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify(messageRequest(override)),
        },
      );
      const count = await createGhostProvApp({ router: countRouter }).request(
        "/v1/messages/count_tokens?beta=true",
        {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify({
            model: "claude-sonnet-4-6",
            messages: [],
            ...override,
          }),
        },
      );

      expect(message.status).toBe(200);
      expect(count.status).toBe(200);
      expect(messageRouter.calls).toHaveLength(1);
      expect(countRouter.calls).toHaveLength(0);
      expect(await count.json()).toMatchObject({ context_management: null });
    }
  });

  it("validates and strips prompt cache controls for generation and count", async () => {
    const malformedBodies = [
      messageRequest({ cache_control: "ephemeral" }),
      messageRequest({ cache_control: { type: "persistent" } }),
      messageRequest({
        messages: [{
          role: "user",
          content: [{ type: "text", text: "x", cache_control: { type: "ephemeral", ttl: "10m" } }],
        }],
      }),
      messageRequest({
        system: [{ type: "text", text: "x", cache_control: { type: "ephemeral", ttl: null } }],
      }),
      messageRequest({
        tools: [{
          name: "Read",
          input_schema: { type: "object" },
          cache_control: { type: "ephemeral", ttl: "forever" },
        }],
      }),
      messageRequest({
        messages: [{
          role: "user",
          content: [{
            type: "document",
            source: {
              type: "content",
              content: [{
                type: "text",
                text: "nested",
                cache_control: { type: "persistent" },
              }],
            },
          }],
        }],
      }),
    ];
    for (const generationBody of malformedBodies) {
      for (const path of ["/v1/messages", "/v1/messages/count_tokens"]) {
        const router = new FakeRouter();
        const body = path.endsWith("count_tokens")
          ? Object.fromEntries(Object.entries(generationBody).filter(([key]) => key !== "max_tokens"))
          : generationBody;
        const response = await createGhostProvApp({ router }).request(path, {
          method: "POST",
          headers: ANTHROPIC_HEADERS,
          body: JSON.stringify(body),
        });
        expect(response.status).toBe(400);
        expect((await response.json() as any).error.message).toMatch(/cache_control/i);
        expect(router.calls).toHaveLength(0);
      }
    }

    const valid = messageRequest({
      cache_control: null,
      system: [{
        type: "text",
        text: "system",
        cache_control: { type: "ephemeral", ttl: "1h", future: true },
      }],
      messages: [{
        role: "user",
        content: [{
          type: "text",
          text: "hello",
          cache_control: { type: "ephemeral", ttl: "5m" },
        }],
      }],
      tools: [{
        name: "Read",
        input_schema: { type: "object" },
        cache_control: { type: "ephemeral" },
      }],
    });
    const router = new FakeRouter();
    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(valid),
    });
    expect(response.status).toBe(200);
    expect(JSON.stringify(router.calls[0].body)).not.toContain("cache_control");
  });

  it("applies current Messages validation to count-token inputs without dispatch", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    const malformed = [
      {
        name: "message role",
        body: { model: "claude-sonnet-4-6", messages: [{ role: "invalid", content: "x" }] },
        message: /role must be user, assistant, system, or developer/i,
      },
      {
        name: "message content",
        body: { model: "claude-sonnet-4-6", messages: [{ role: "user", content: 7 }] },
        message: /content must be a string or an array/i,
      },
      {
        name: "media",
        body: {
          model: "claude-sonnet-4-6",
          messages: [{ role: "user", content: [{
            type: "image",
            source: { type: "base64", media_type: "image/tiff", data: "AAAA" },
          }] }],
        },
        message: /media_type is unsupported/i,
      },
      {
        name: "system",
        body: { model: "claude-sonnet-4-6", messages: [], system: null },
        message: /system must not be null/i,
      },
      {
        name: "tools",
        body: {
          model: "claude-sonnet-4-6",
          messages: [],
          tools: [{ name: "Read", input_schema: { type: "string" } }],
        },
        message: /input_schema\.type must be object/i,
      },
      {
        name: "tool choice",
        body: {
          model: "claude-sonnet-4-6",
          messages: [],
          tools: [{ name: "Read", input_schema: { type: "object" } }],
          tool_choice: { type: "tool", name: "Write" },
        },
        message: /tool is not declared/i,
      },
      {
        name: "thinking",
        body: {
          model: "claude-sonnet-4-6",
          messages: [],
          thinking: { type: "enabled", budget_tokens: 12 },
        },
        message: /budget_tokens/i,
      },
      {
        name: "output config",
        body: {
          model: "claude-sonnet-4-6",
          messages: [],
          output_config: { format: { type: "xml", schema: {} } },
        },
        message: /unsupported output_config\.format type/i,
      },
      {
        name: "metadata",
        body: {
          model: "claude-sonnet-4-6",
          messages: [],
          metadata: { user_id: 7 },
        },
        message: /metadata\.user_id is required/i,
      },
      {
        name: "known speed",
        body: { model: "claude-sonnet-4-6", messages: [], speed: "instant" },
        message: /speed must be standard or fast/i,
      },
    ];

    for (const value of malformed) {
      const response = await app.request("/v1/messages/count_tokens", {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(value.body),
      });
      expect(response.status, value.name).toBe(400);
      expect((await response.json() as any).error.message, value.name)
        .toMatch(value.message);
    }
    expect(router.calls).toHaveLength(0);
  });

  it("accepts valid thinking and ignored context-management hints while counting", async () => {
    const router = new FakeRouter();
    const body = {
      model: "claude-sonnet-4-6",
      messages: [{ role: "user", content: "hello" }],
      thinking: { type: "enabled", budget_tokens: 1024 },
      context_management: { edits: [{ type: "clear_tool_uses_20250919" }] },
    };
    const response = await createGhostProvApp({ router }).request(
      "/v1/messages/count_tokens?beta=true",
      {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(body),
      },
    );

    expect(response.status).toBe(200);
    expect((await response.json() as any).input_tokens).toBeGreaterThan(0);
    expect(router.calls).toHaveLength(0);
  });

  it("requires JSON content type and the current Anthropic version on POST routes", async () => {
    const paths = ["/v1/messages", "/v1/messages/count_tokens"];
    for (const path of paths) {
      const body = path.endsWith("count_tokens")
        ? { model: "claude-sonnet-4-6", messages: [] }
        : messageRequest();
      const cases: Array<{ headers: Record<string, string>; message: RegExp }> = [
        {
          headers: { "anthropic-version": "2023-06-01" },
          message: /content-type must be application\/json/i,
        },
        {
          headers: { "content-type": "text/plain", "anthropic-version": "2023-06-01" },
          message: /content-type must be application\/json/i,
        },
        {
          headers: { "content-type": "application/json" },
          message: /anthropic-version.*required/i,
        },
        {
          headers: { "content-type": "application/json", "anthropic-version": "2024-01-01" },
          message: /anthropic-version.*2023-06-01/i,
        },
      ];
      for (const value of cases) {
        const response = await createGhostProvApp({ router: new FakeRouter() }).request(path, {
          method: "POST",
          headers: value.headers,
          body: JSON.stringify(body),
        });
        await expectAnthropicError(response, 400, value.message);
      }
    }

    const accepted = await createGhostProvApp({ router: new FakeRouter() }).request(
      "/v1/messages/count_tokens",
      {
        method: "POST",
        headers: {
          "content-type": "Application/JSON; charset=utf-8",
          "anthropic-version": "2023-06-01",
        },
        body: JSON.stringify({ model: "claude-sonnet-4-6", messages: [] }),
      },
    );
    expect(accepted.status).toBe(200);
  });

  it("returns route-local Anthropic 405 envelopes with Allow POST", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });
    for (const path of [
      "/v1/messages",
      "/v1/messages?beta=true",
      "/v1/messages/count_tokens",
      "/v1/messages/count_tokens?beta=true",
    ]) {
      const response = await app.request(path, { method: "PUT" });
      const body = await expectAnthropicError(response, 405, /method.*not allowed/i);
      expect(response.headers.get("allow")).toBe("POST");
      expect(body.error.type).toBe("invalid_request_error");
    }
    expect(router.calls).toHaveLength(0);
  });

  it("returns current Anthropic JSON errors before a stream starts", async () => {
    const router = new FakeRouter();
    const app = createGhostProvApp({ router });

    await expectAnthropicError(await app.request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: "{not-json",
    }), 400, /valid JSON/i);
    await expectAnthropicError(await app.request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify({ model: "claude-sonnet-4-6", messages: [] }),
    }), 400, /max_tokens/i);
    await expectAnthropicError(await app.request("/v1/messages/count_tokens", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify({ messages: [] }),
    }), 400, /model is required/i);
    await expectAnthropicError(await app.request("/v1/messages/count_tokens", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
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
        headers: ANTHROPIC_HEADERS,
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

  it("discards non-stream relay events without Array.fromAsync transcript retention", async () => {
    const original = Array.fromAsync;
    let fromAsyncCalled = false;
    Object.defineProperty(Array, "fromAsync", {
      configurable: true,
      value: () => {
        fromAsyncCalled = true;
        throw new Error("Array.fromAsync must not retain the relay transcript");
      },
    });
    try {
      const router = new FakeRouter(() => relayHandle(
        completed("resp_discard", "discarded"),
        async function* () {
          for (let index = 0; index < 1_000; index += 1) {
            yield { event: "unknown", data: String(index) };
          }
        },
      ));
      const response = await createGhostProvApp({ router }).request("/v1/messages", {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(messageRequest()),
      });

      expect(response.status).toBe(200);
      expect(fromAsyncCalled).toBe(false);
      expect((await response.json() as any).content[0].text).toBe("discarded");
    } finally {
      Object.defineProperty(Array, "fromAsync", {
        configurable: true,
        value: original,
      });
    }
  });

  it("aborts, returns, and settles final after a non-stream drain failure", async () => {
    let returnCalls = 0;
    let finalSettled = false;
    let relaySignal: AbortSignal | undefined;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      const final = new Promise<unknown>((_resolve, reject) => {
        options.signal?.addEventListener("abort", () => {
          setTimeout(() => {
            finalSettled = true;
            reject(new Error("late final secret"));
          }, 10);
        }, { once: true });
      });
      const iterator = {
        next: () => Promise.reject(new Error("drain secret")),
        return: () => {
          returnCalls += 1;
          return Promise.resolve({ done: true as const, value: undefined });
        },
        throw: (error?: unknown) => Promise.reject(error),
        [Symbol.asyncIterator]() { return this; },
      } as unknown as AsyncGenerator<SseFrame>;
      return relayHandle(final, () => iterator);
    });

    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest()),
    });
    const text = await response.text();

    expect(response.status).toBe(500);
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalls).toBe(1);
    expect(finalSettled).toBe(true);
    expect(text).not.toContain("drain secret");
    expect(text).not.toContain("late final secret");
  });

  it("aborts, returns, and settles the drain after a non-stream final failure", async () => {
    let returnCalls = 0;
    let drainSettled = false;
    let settleNext!: (result: IteratorResult<SseFrame>) => void;
    const router = new FakeRouter((_body, options) => {
      const iterator = {
        next: () => new Promise<IteratorResult<SseFrame>>(resolve => { settleNext = resolve; }),
        return: () => {
          returnCalls += 1;
          setTimeout(() => {
            drainSettled = true;
            settleNext({ done: true, value: undefined });
          }, 10);
          return Promise.resolve({ done: true as const, value: undefined });
        },
        throw: (error?: unknown) => Promise.reject(error),
        [Symbol.asyncIterator]() { return this; },
      } as unknown as AsyncGenerator<SseFrame>;
      return relayHandle(Promise.reject(new Error("final secret")), () => iterator);
    });

    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest()),
    });
    const text = await response.text();

    expect(response.status).toBe(500);
    expect(returnCalls).toBe(1);
    expect(drainSettled).toBe(true);
    expect(text).not.toContain("final secret");
  });

  it("settles non-stream cancellation without waiting for noncooperative final or drain", async () => {
    let nextCalls = 0;
    let returnCalls = 0;
    let relayStarted!: () => void;
    const started = new Promise<void>(resolve => { relayStarted = resolve; });
    const iterator = {
      next() {
        nextCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      return() {
        returnCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter(() => {
      relayStarted();
      return relayHandle(new Promise(() => undefined), () => iterator);
    });
    const incoming = new AbortController();
    const pending = createGhostProvApp({ router }).fetch(new Request(
      "http://local/v1/messages",
      {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(messageRequest()),
        signal: incoming.signal,
      },
    ));
    await started;
    await waitFor(() => nextCalls === 1, "non-stream drain did not start");
    incoming.abort();

    const response = await Promise.race([
      pending,
      new Promise<never>((_, reject) => setTimeout(
        () => reject(new Error("non-stream cancellation waited for noncooperative work")),
        100,
      )),
    ]);
    expect(response.status).toBe(499);
    expect(returnCalls).toBe(1);
  });

  it("bounds non-stream failure cleanup when the drain never cooperates", async () => {
    let settleNext!: (result: IteratorResult<SseFrame>) => void;
    let returnCalls = 0;
    const iterator = {
      next: () => new Promise<IteratorResult<SseFrame>>(resolve => { settleNext = resolve; }),
      return() {
        returnCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter(() => relayHandle(
      Promise.reject(new Error("final failure secret")),
      () => iterator,
    ));
    const pending = Promise.resolve(createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest()),
    }));
    const winner = await Promise.race([
      pending.then(response => ({ response })),
      new Promise<{ response: null }>(resolve => setTimeout(() => resolve({ response: null }), 100)),
    ]);
    if (winner.response === null) {
      settleNext({ done: true, value: undefined });
      await pending;
    }

    expect(winner.response).not.toBeNull();
    expect(winner.response?.status).toBe(500);
    expect(returnCalls).toBe(1);
  });

  it("sanitizes pre-stream upstream failures and preserves only retry headers", async () => {
    const upstream = Object.assign(
      new ApiError(502, "secret upstream response body", { type: "upstream_error" }),
      {
        headers: {
          "retry-after": "7",
          "x-should-retry": "true",
          authorization: "Bearer secret",
        },
      },
    );
    const router = new FakeRouter(() => Promise.reject(upstream));
    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest()),
    });
    const body = await response.json() as any;

    expect(response.status).toBe(500);
    expect(body).toMatchObject({ type: "error", error: { type: "api_error" } });
    expect(JSON.stringify(body)).not.toContain("secret upstream response body");
    expect(response.headers.get("retry-after")).toBe("7");
    expect(response.headers.get("x-should-retry")).toBe("true");
    expect(response.headers.get("authorization")).toBeNull();
  });

  it("awaits the R2 prelude before committing Anthropic SSE headers", async () => {
    const providerError = await createProviderErrorEnvelope({
      origin: "http",
      status: 429,
      model: "gpt-5.6-sol",
      headers: new Headers({
        "retry-after": "7",
        authorization: "Bearer must-not-cross",
      }),
      value: {
        error: {
          message: "Rate limit reached in organization org-Test123",
          type: "rate_limit_error",
          param: null,
          code: "rate_limit_exceeded",
        },
      },
    });
    const upstream = Object.assign(new Error(providerError.error.message), {
      providerError,
    });
    const router = new FakeRouter(() => ({
      async *events() {},
      final: new Promise(() => undefined),
      upstreamPrelude: Promise.reject(upstream),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }));

    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const body = await response.json() as any;

    expect(response.status).toBe(429);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(response.headers.get("retry-after")).toBe("7");
    expect(body).toMatchObject({
      type: "error",
      error: { type: "rate_limit_error", message: "Rate limit exceeded" },
    });
    expect(JSON.stringify(body)).not.toContain("org-Test123");
    expect(response.headers.get("authorization")).toBeNull();
  });

  it("emits canonical named SSE from body.stream with request and no-buffer headers", async () => {
    const final = completed("resp_stream", "streamed");
    const router = new FakeRouter(() => relayHandle(final, async function* () {
      yield streamFrame("response.completed", { response: final });
    }));
    const app = createGhostProvApp({ router, keepAliveMs: 60_000 });

    const response = await app.request("/v1/messages?beta=true", {
      method: "POST",
      headers: { ...ANTHROPIC_HEADERS, accept: "application/json" },
      body: JSON.stringify(messageRequest({
        stream: true,
        context_management: { edits: [{ type: "clear_tool_uses_20250919" }] },
      })),
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
    const events = text.split("\n")
      .filter(line => line.startsWith("data: "))
      .map(line => JSON.parse(line.slice("data: ".length)));
    const start = events.find(event => event.type === "message_start");
    const delta = events.find(event => event.type === "message_delta");
    expect(start?.message.context_management).toEqual({ applied_edits: [] });
    expect(delta?.context_management).toEqual({ applied_edits: [] });
    expect(delta?.delta).toMatchObject({ container: null, stop_details: null });
    expect(start?.message).not.toHaveProperty("diagnostics");
    expect(events.every(event => !("diagnostics" in event))).toBe(true);
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
      headers: ANTHROPIC_HEADERS,
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

  it("wakes cancellation for a noncooperative iterator and clears the keepalive timer", async () => {
    let nextCalls = 0;
    let returnCalls = 0;
    let relaySignal: AbortSignal | undefined;
    let clearedTimers = 0;
    const originalClearTimeout = globalThis.clearTimeout;
    const iterator = {
      next() {
        nextCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      return() {
        returnCalls += 1;
        return new Promise<IteratorResult<SseFrame>>(() => undefined);
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      return relayHandle(new Promise(() => undefined), () => iterator);
    });
    const response = await createGhostProvApp({ router, keepAliveMs: 60_000 }).request(
      "/v1/messages",
      {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(messageRequest({ stream: true })),
      },
    );
    const reader = response.body!.getReader();
    await reader.read();
    const pending = reader.read();
    await waitFor(() => nextCalls === 1, "upstream read did not start");
    globalThis.clearTimeout = ((timer?: ReturnType<typeof setTimeout>) => {
      clearedTimers += 1;
      return originalClearTimeout(timer);
    }) as typeof clearTimeout;
    try {
      await reader.cancel("noncooperative cancellation");
      await Promise.race([
        pending,
        new Promise((_, reject) => setTimeout(
          () => reject(new Error("pending read was not woken by cancellation")),
          100,
        )),
      ]);
    } finally {
      globalThis.clearTimeout = originalClearTimeout;
    }

    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalls).toBe(1);
    expect(clearedTimers).toBeGreaterThan(0);
  });

  it("does not finish translation when a pending final settles after cancellation", async () => {
    let resolveFinal!: (value: unknown) => void;
    let finalConsumed = false;
    let finalAccessed = false;
    const final = new Promise<unknown>(resolve => { resolveFinal = resolve; });
    const iterator = {
      next: () => Promise.resolve({ done: true as const, value: undefined }),
      return: () => Promise.resolve({ done: true as const, value: undefined }),
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter(() => ({
      events: () => iterator,
      get final() {
        finalAccessed = true;
        return final;
      },
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }));
    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const reader = response.body!.getReader();
    await reader.read();
    const pending = reader.read();
    await waitFor(() => finalAccessed, "final wait did not start");
    await reader.cancel("cancel pending final");
    const lateFinal = new Proxy(completed("resp_late"), {
      get(target, key, receiver) {
        if (key !== "then") finalConsumed = true;
        return Reflect.get(target, key, receiver);
      },
    });
    resolveFinal(lateFinal);
    await Promise.resolve();
    await Promise.resolve();
    await pending.catch(() => undefined);

    expect(finalConsumed).toBe(false);
  });

  it("keeps one pending final promise while emitting keepalives and cancels it promptly", async () => {
    let resolveFinal!: (value: unknown) => void;
    let finalAccesses = 0;
    let returnCalls = 0;
    const final = new Promise<unknown>(resolve => { resolveFinal = resolve; });
    const iterator = {
      next: () => Promise.resolve({ done: true as const, value: undefined }),
      return() {
        returnCalls += 1;
        return Promise.resolve({ done: true as const, value: undefined });
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter(() => ({
      events: () => iterator,
      get final() {
        finalAccesses += 1;
        return final;
      },
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }));
    const response = await createGhostProvApp({ router, keepAliveMs: 10 }).request(
      "/v1/messages",
      {
        method: "POST",
        headers: ANTHROPIC_HEADERS,
        body: JSON.stringify(messageRequest({ stream: true })),
      },
    );
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    await reader.read();
    try {
      const first = await Promise.race([
        reader.read(),
        new Promise<null>(resolve => setTimeout(() => resolve(null), 100)),
      ]);
      expect(first).not.toBeNull();
      expect(decoder.decode(first?.value)).toBe(": keepalive\n\n");
      expect(decoder.decode((await reader.read()).value)).toBe(": keepalive\n\n");
      expect(finalAccesses).toBe(1);
    } finally {
      await reader.cancel("pending final cancellation");
      resolveFinal(completed("resp_after_cancel"));
    }
    expect(returnCalls).toBe(1);
  });

  it("converts synchronous iterator.next failures into a safe terminal error", async () => {
    let returnCalls = 0;
    let relaySignal: AbortSignal | undefined;
    const iterator = {
      next(): Promise<IteratorResult<SseFrame>> {
        throw new Error("synchronous next secret");
      },
      return() {
        returnCalls += 1;
        return Promise.resolve({ done: true as const, value: undefined });
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      return relayHandle(new Promise(() => undefined), () => iterator);
    });
    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const text = await response.text();

    expect(text).toContain("event: error\n");
    expect(text).not.toContain("synchronous next secret");
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalls).toBe(1);
  });

  it("aborts and returns after an asynchronous iterator.next rejection", async () => {
    let returnCalls = 0;
    let relaySignal: AbortSignal | undefined;
    const iterator = {
      next: () => Promise.reject(new Error("asynchronous next secret")),
      return() {
        returnCalls += 1;
        return Promise.resolve({ done: true as const, value: undefined });
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<SseFrame>;
    const router = new FakeRouter((_body, options) => {
      relaySignal = options.signal;
      return relayHandle(new Promise(() => undefined), () => iterator);
    });
    const response = await createGhostProvApp({ router }).request("/v1/messages", {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest({ stream: true })),
    });
    const text = await response.text();

    expect(text).toContain("event: error\n");
    expect(text).not.toContain("asynchronous next secret");
    expect(relaySignal?.aborted).toBe(true);
    expect(returnCalls).toBe(1);
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
      headers: ANTHROPIC_HEADERS,
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
        headers: ANTHROPIC_HEADERS,
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

  it("hands the Anthropic ingress lease to its one relay admission", async () => {
    const base = new FakeRouter();
    let releases = 0;
    const lease: IncomingResourceLease = {
      release() { releases++; },
    };
    const router: RouterLike = {
      snapshot: () => base.snapshot(),
      startRelay: (body, options) => base.startRelay(body, options),
      acquireIncomingBody: async () => lease,
    };
    const port = await listen(createGhostProvApp({ router }), router);
    const body = JSON.stringify(messageRequest());
    const response = await fetch(`http://127.0.0.1:${port}/v1/messages`, {
      method: "POST",
      headers: {
        ...ANTHROPIC_HEADERS,
        "content-length": String(Buffer.byteLength(body)),
      },
      body,
    });

    expect(response.status).toBe(200);
    expect(base.calls).toHaveLength(1);
    expect(base.calls[0].options.incomingLease).toBe(lease);
    expect(releases).toBe(1);
  });

  it("releases a zero-body lease when Anthropic parsing fails before handoff", async () => {
    const base = new FakeRouter();
    let releases = 0;
    const router: RouterLike = {
      snapshot: () => base.snapshot(),
      startRelay: (body, options) => base.startRelay(body, options),
      acquireIncomingBody: async () => ({
        release() { releases++; },
      }),
    };
    const port = await listen(createGhostProvApp({ router }), router);
    const response = await nodeRequest({
      port,
      path: "/v1/messages",
      headers: { ...ANTHROPIC_HEADERS, "content-length": 0 },
    });

    expect(response.status).toBe(400);
    expect(base.calls).toHaveLength(0);
    expect(releases).toBe(1);
  });

  it("aborts a blocked incoming-body lease when the client disconnects", async () => {
    let leaseStarted!: () => void;
    let leaseAborted!: () => void;
    const started = new Promise<void>(resolve => { leaseStarted = resolve; });
    const aborted = new Promise<void>(resolve => { leaseAborted = resolve; });
    const base = new FakeRouter();
    const router: RouterLike = {
      ...base,
      snapshot: () => base.snapshot(),
      startRelay: (body, options) => base.startRelay(body, options),
      acquireIncomingBody: (_bytes, signal) => {
        leaseStarted();
        return new Promise((_resolve, reject) => {
          signal?.addEventListener("abort", () => {
            leaseAborted();
            reject(new DOMException("body lease aborted", "AbortError"));
          }, { once: true });
        });
      },
    };
    const port = await listen(createGhostProvApp({ router }), router);
    const request = httpRequest({
      hostname: "127.0.0.1",
      port,
      path: "/v1/messages",
      method: "POST",
      headers: { ...ANTHROPIC_HEADERS, "content-length": "1024" },
    });
    request.on("error", () => undefined);
    request.flushHeaders();
    await started;
    request.destroy();

    await Promise.race([
      aborted,
      new Promise((_, reject) => setTimeout(
        () => reject(new Error("body lease did not receive client cancellation")),
        300,
      )),
    ]);
  });

  it("sanitizes an internal body-admission 413 instead of trusting its message", async () => {
    const base = new FakeRouter();
    const router: RouterLike = {
      snapshot: () => base.snapshot(),
      startRelay: (body, options) => base.startRelay(body, options),
      acquireIncomingBody: () => Promise.reject(new ApiError(
        413,
        "secret body admission accounting detail",
        { code: "request_too_large" },
      )),
    };
    const port = await listen(createGhostProvApp({ router }), router);
    const response = await fetch(`http://127.0.0.1:${port}/v1/messages`, {
      method: "POST",
      headers: ANTHROPIC_HEADERS,
      body: JSON.stringify(messageRequest()),
    });
    const body = await response.json() as any;

    expect(response.status).toBe(413);
    expect(body).toMatchObject({
      type: "error",
      error: { type: "request_too_large", message: "Request too large" },
    });
    expect(JSON.stringify(body)).not.toContain("secret body admission accounting detail");
  });
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
