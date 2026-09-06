import { afterEach, describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:http";
import { WebSocket } from "ws";
import { createAuthenticatedTenantScope, type AuthenticatedTenantScope } from "../src/http/api-key";
import { WebSocketHttpFallbackIndex } from "../src/ws/http-fallback";
import { WebSocketProcessBudget } from "../src/ws/session";
import { WebSocketTelemetry } from "../src/ws/telemetry";
import {
  attachResponsesWebSocket,
  closeResponsesWebSockets,
  DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES,
  type AttachResponsesWebSocketOptions,
} from "../src/ws/server";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import type { RunHandle } from "../src/relay/run-handle";
import { ConsoleSink } from "../src/observability/logger";
import { observabilityEvent, type JsonValue, type ObservabilityEvent } from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";

const TEST_KEY = "offline-websocket-fallback-key-0123456789abcdef";
const servers: Server[] = [];
const sockets: WebSocket[] = [];

afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.terminate();
  for (const server of servers.splice(0)) {
    await closeResponsesWebSockets(server, { timeoutMs: 100 });
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

async function listen(server: Server): Promise<string> {
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  return `http://127.0.0.1:${address.port}/v1/responses`;
}

async function connect(url: string, headers: Record<string, string> = {}) {
  const socket = new WebSocket(url.replace("http:", "ws:"), {
    headers: { authorization: `Bearer ${TEST_KEY}`, "thread-id": "large-task", ...headers },
  });
  sockets.push(socket);
  socket.on("error", () => undefined);
  const status = await new Promise<number>((resolve, reject) => {
    socket.once("open", () => resolve(101));
    socket.once("unexpected-response", (_request, response) => {
      response.resume();
      resolve(response.statusCode!);
      socket.terminate();
    });
    socket.once("error", reject);
  });
  return { socket, status };
}

function handle(id: string): RunHandle {
  const response = { id, object: "response", status: "completed", model: "gpt-5.6-sol", output: [] };
  return {
    async *events() {
      yield { event: "response.completed", data: JSON.stringify({ type: "response.completed", response }) };
    },
    final: Promise.resolve(response),
    stats: { bytes: 0, firstByteMs: null, frames: 1 },
  };
}

async function rejectedPayload(socket: WebSocket, payload: string): Promise<void> {
  const closed = new Promise<void>(resolve => socket.once("close", () => resolve()));
  socket.send(payload);
  await closed;
}

async function fixture(overrides: Partial<AttachResponsesWebSocketOptions> = {}) {
  const events: ObservabilityEvent[] = [];
  let calls = 0;
  const scope = createAuthenticatedTenantScope();
  const budget = new WebSocketProcessBudget({
    maxSessions: 8, maxQueuedBytes: 64 * 1024 ** 2, maxIngressBytes: 128 * 1024 ** 2,
  });
  const server = createServer((_request, response) => response.writeHead(404).end());
  const webSockets = attachResponsesWebSocket(server, {
    apiKey: TEST_KEY,
    authorizationScope: scope,
    maxPayloadBytes: 512,
    processBudget: budget,
    logger: { emit: event => { assertSafeObservabilityEvent(event); events.push(event); } },
    startRelay: async () => { calls++; return handle("resp_small"); },
    ...overrides,
  });
  return { url: await listen(server), server, webSockets, events, budget, calls: () => calls };
}

describe("bounded WebSocket HTTP fallback hints", () => {
  it("retains only independent keyed digests and rejects malformed contexts", () => {
    const scope = createAuthenticatedTenantScope();
    const index = new WebSocketHttpFallbackIndex(scope);
    const other = new WebSocketHttpFallbackIndex(scope);
    const key = index.key(scope, "private-task-id")!;
    expect(key).toMatch(/^[a-f0-9]{64}$/);
    expect(key).not.toContain("private-task-id");
    expect(key).toBe(index.key(scope, "private-task-id")!);
    expect(key).not.toBe(other.key(scope, "private-task-id")!);
    for (const context of [undefined, null, [], ["task"], 1, "", "a".repeat(129), "task\n", "task\r", "task\0", "task/other", "tâsk"]) {
      expect(index.key(scope, context)).toBeNull();
    }
    expect(index.key(scope, "a".repeat(128))).not.toBeNull();
    expect(index.remember(scope, null)).toBe(false);
    for (const key of ["task", "a".repeat(63) + "\n", "a".repeat(65)]) {
      expect(() => index.remember(scope, key)).toThrow(TypeError);
    }
    index.close(); other.close();
  });

  it("expires and evicts only bounded oldest hints, without extending lookups", () => {
    let now = 0;
    const scope = createAuthenticatedTenantScope();
    const index = new WebSocketHttpFallbackIndex(scope, { now: () => now, maxEntries: 2, ttlMs: 10 });
    const [a, b, c] = ["a", "b", "c"].map(value => index.key(scope, value));
    index.remember(scope, a);
    now = 1; index.remember(scope, b);
    now = 2; index.remember(scope, c);
    expect(index.snapshot().entries).toBe(2);
    expect(index.requiresHttp(scope, a)).toBe(false);
    expect(index.requiresHttp(scope, b)).toBe(true);
    now = 11;
    expect(index.requiresHttp(scope, b)).toBe(false);
    expect(index.requiresHttp(scope, c)).toBe(true);
    now = 12;
    expect(index.requiresHttp(scope, c)).toBe(false);
    expect(index.snapshot().entries).toBe(0);
    index.close();
  });

  it("fails closed on scope conflicts and fences late shutdown writes", () => {
    const scope = createAuthenticatedTenantScope();
    const foreign = createAuthenticatedTenantScope();
    const index = new WebSocketHttpFallbackIndex(scope);
    const key = index.key(scope, "task");
    expect(() => new WebSocketHttpFallbackIndex({} as AuthenticatedTenantScope)).toThrow(TypeError);
    expect(() => index.key(foreign, "task")).toThrow(TypeError);
    expect(() => index.remember(foreign, key)).toThrow(TypeError);
    expect(() => index.requiresHttp(foreign, key)).toThrow(TypeError);
    expect(index.remember(scope, key)).toBe(true);
    index.close();
    expect(index.snapshot()).toMatchObject({ entries: 0, closed: true });
    expect(index.remember(scope, key)).toBe(false);
    expect(index.requiresHttp(scope, key)).toBe(false);
    expect(index.key(scope, "task")).toBeNull();
  });
});

describe("WebSocket payload fallback", () => {
  it("returns 426 only to the authenticated affected task and releases ingress", async () => {
    const f = await fixture();
    const first = await connect(f.url);
    await rejectedPayload(first.socket, "x".repeat(513));
    expect(f.calls()).toBe(0);
    expect(f.budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0, ingressReservedBytes: 0 });
    expect((await connect(f.url, { authorization: "Bearer incorrect" })).status).toBe(401);
    expect((await connect(f.url, { origin: "https://untrusted.example" })).status).toBe(403);
    expect((await connect(f.url)).status).toBe(426);
    expect((await connect(f.url, { "thread-id": "small-task" })).status).toBe(101);
    expect(f.events.map(event => event.event)).toEqual([
      "websocket.payload_rejected", "websocket.http_fallback",
    ]);
    expect(f.events[0]).toMatchObject({ limit_bytes: 512, http_fallback_available: true });
    expect(JSON.stringify(f.events)).not.toMatch(/large-task|small-task|Bearer|untrusted/);
  });

  it("does not infer an affected task from a missing header or absent authentication", async () => {
    for (const options of [{}, { apiKey: undefined }]) {
      const f = await fixture(options);
      const headers: Record<string, string> = options.apiKey === undefined && "apiKey" in options
        ? {} : { "thread-id": "" };
      await rejectedPayload((await connect(f.url, headers)).socket, "x".repeat(513));
      expect((await connect(f.url, headers)).status).toBe(101);
      expect(f.events[0]?.http_fallback_available).toBe(false);
    }
  });

  it("never learns fallback from a peer's claimed size error", async () => {
    const f = await fixture();
    const { socket } = await connect(f.url);
    const closed = new Promise<void>(resolve => socket.once("close", () => resolve()));
    socket.close(1009, "Maximum payload size exceeded");
    await closed;
    expect((await connect(f.url)).status).toBe(101);
    expect(f.events).toEqual([]);
  });

  it("never learns fallback from a late parser error after connection cleanup", async () => {
    const f = await fixture();
    const { socket } = await connect(f.url);
    const peer = [...f.webSockets.clients][0]!;
    const closed = new Promise<void>(resolve => socket.once("close", () => resolve()));
    const peerClosed = new Promise<void>(resolve => peer.once("close", () => resolve()));
    socket.close();
    await Promise.all([closed, peerClosed]);
    expect(f.budget.snapshot().sessions).toBe(0);
    peer.emit("error", Object.assign(new Error("Maximum payload size exceeded"), {
      code: "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH",
    }));
    expect(f.events[0]?.http_fallback_available).toBe(false);
    expect((await connect(f.url)).status).toBe(101);
  });

  it("does not redirect a connection with active provider work", async () => {
    let started!: () => void;
    const running = new Promise<void>(resolve => { started = resolve; });
    let finish!: (value: unknown) => void;
    const final = new Promise(resolve => { finish = resolve; });
    const f = await fixture({ startRelay: async () => {
      started();
      return { async *events() {}, final, stats: { bytes: 0, firstByteMs: null, frames: 0 } };
    } });
    const { socket } = await connect(f.url);
    socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", input: "small" }));
    await running;
    await rejectedPayload(socket, "x".repeat(513));
    expect(f.events[0]?.http_fallback_available).toBe(false);
    expect((await connect(f.url)).status).toBe(101);
    finish({ id: "resp_active", status: "completed", output: [] });
  });

  it("recovers a real >16 MiB history through SSE exactly once, preserving lineage and small WebSockets", async () => {
    const scope = createAuthenticatedTenantScope();
    const calls: Array<{ body: Record<string, unknown>; options: Parameters<RouterLike["startRelay"]>[1] }> = [];
    let activeBodyLeases = 0;
    const router: RouterLike = {
      responseProvenanceAuthorizationScope: () => scope,
      async acquireIncomingBody() {
        activeBodyLeases++;
        let released = false;
        return { release() { if (!released) { released = true; activeBodyLeases--; } } };
      },
      async startRelay(body, options) { calls.push({ body, options }); return handle(`resp_${calls.length}`); },
      snapshot: () => ({ capacity: 8, active: 0, available: 8, queued: 0, runtimes: [], resources: {} }),
    };
    const events: ObservabilityEvent[] = [];
    const telemetry = new WebSocketTelemetry();
    const app = createGhostProvApp({ router, apiKey: TEST_KEY, authorizationScope: scope });
    const server = createGhostProvServer({
      app, router, apiKey: TEST_KEY, authorizationScope: scope, port: 0, candidate: true,
      webSocketTelemetry: telemetry,
      logger: { emit: event => { assertSafeObservabilityEvent(event); events.push(event); } },
    });
    const url = await listen(server);
    const history = "x".repeat(DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES + 1_024);
    const request = { model: "gpt-5.6-sol", input: history, stream: true, store: false, previous_response_id: "resp_preserve_exactly" };
    const { socket } = await connect(url);
    await rejectedPayload(socket, JSON.stringify({ type: "response.create", ...request }));
    expect(calls.length).toBe(0);
    expect(telemetry.snapshotAndReset().ingressParserRejections).toBe(1);
    expect((await connect(url)).status).toBe(426);
    const response = await fetch(url, {
      method: "POST", headers: { authorization: `Bearer ${TEST_KEY}`, "content-type": "application/json" },
      body: JSON.stringify(request),
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toContain("text/event-stream");
    expect(await response.text()).toContain("response.completed");
    expect(calls.length).toBe(1);
    expect(calls[0]!.options).toMatchObject({ source: "http", authorizationScope: scope });
    expect(calls[0]!.options?.authorizationScope).toBe(scope);
    expect(calls[0]!.body).toMatchObject({ stream: true, store: true, previous_response_id: "resp_preserve_exactly" });
    expect(calls[0]!.body.input).toBe(history);
    expect(request.store).toBe(false);
    expect(activeBodyLeases).toBe(0);
    const small = await connect(url, { "thread-id": "small-task" });
    expect(small.status).toBe(101);
    const smallDone = new Promise<void>(resolve => small.socket.on("message", raw => {
      if (JSON.parse(raw.toString()).type === "response.completed") resolve();
    }));
    small.socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", input: "small" }));
    await smallDone;
    expect(calls.length).toBe(2);
    expect(calls[1]!.options?.source).toBe("websocket");
    expect(events.map(event => event.event)).toEqual(["websocket.payload_rejected", "websocket.http_fallback"]);
  }, 20_000);
});

describe("visible, content-free WebSocket fallback diagnostics", () => {
  it("prints rejection and fallback even when progress is throttled", async () => {
    const lines: string[] = [];
    const sink = new ConsoleSink({ now: () => 0, write: line => lines.push(line) });
    await sink.writeBatch([JSON.stringify(observabilityEvent("fleet.sample", {}))]);
    const rejected = observabilityEvent("websocket.payload_rejected", { limit_bytes: 16 * 1024 ** 2, http_fallback_available: true });
    const fallback = observabilityEvent("websocket.http_fallback", { limit_bytes: 16 * 1024 ** 2, status: 426 });
    assertSafeObservabilityEvent(rejected);
    assertSafeObservabilityEvent(fallback);
    await sink.writeBatch([JSON.stringify(rejected), JSON.stringify(fallback)]);
    expect(lines.join("\n")).toContain("websocket.payload_rejected");
    expect(lines.join("\n")).toContain("websocket.http_fallback");
    const invalidFields: Array<Record<string, JsonValue>> = [
      { limit_bytes: "private content", http_fallback_available: true },
      { limit_bytes: 0, http_fallback_available: true },
      { limit_bytes: 65 * 1024 ** 2, http_fallback_available: true },
      { limit_bytes: 1, http_fallback_available: "private content" },
      { limit_bytes: 1, http_fallback_available: true, thread_id: "private-task" },
    ];
    for (const fields of invalidFields) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("websocket.payload_rejected", fields))).toThrow();
    }
    expect(() => assertSafeObservabilityEvent(observabilityEvent("websocket.http_fallback", { limit_bytes: 1, status: 200 }))).toThrow();
  });
});
