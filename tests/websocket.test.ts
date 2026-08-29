import { afterEach, describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:http";
import { WebSocket } from "ws";
import {
  attachResponsesWebSocket,
  DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES,
  DEFAULT_WS_MAX_PAYLOAD_BYTES,
  DEFAULT_WS_PROMOTION_HEADROOM_BYTES,
  DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
  decodeWebSocketRawData,
  MAX_WS_PAYLOAD_BYTES,
  webSocketIngressReservationBytes,
  type AttachResponsesWebSocketOptions,
  type WebSocketRelayStarter,
} from "../src/ws/server";
import type { RunHandle } from "../src/relay/run-handle";
import {
  ResponsesWebSocketSession,
  WebSocketProcessBudget,
  type ResponsesWebSocketSessionOptions,
} from "../src/ws/session";
import { WebSocketTelemetry } from "../src/ws/telemetry";
import {
  ResponseGraph,
  SharedByteBudget,
  SYNTHETIC_WARMUP_ID_PREFIX,
} from "../src/ws/response-graph";
import { ApiError } from "../src/http/errors";
import {
  createAuthenticatedTenantScope,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../src/http/api-key";

import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { RouterQueueFullError } from "../src/router/router";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  V2CommittedError,
  V2RunHandle,
} from "../src/v2/transport/run-handle";
import {
  attachAtomicBytePermit,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";

const TEST_API_KEY = "test-websocket-key-0123456789abcdef-0123456789";
const servers: Server[] = [];
const sockets: WebSocket[] = [];

afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.close();
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
});

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function relayHandle(final: Promise<any>, frames: Array<{ event: string; data: string }> = []): RunHandle {
  return {
    async *events() { for (const frame of frames) yield frame; },
    final,
    stats: { bytes: 0, firstByteMs: null, frames: frames.length },
  };
}

function completed(id: string, output: unknown[] = []) {
  return { id, object: "response", model: "gpt-5.6-sol", status: "completed", output };
}

type TestAttachOptions = Omit<AttachResponsesWebSocketOptions, "authorizationScope"> & {
  authorizationScope?: AuthenticatedTenantScope;
};

function attachTestResponsesWebSocket(server: Server, options: TestAttachOptions) {
  return attachResponsesWebSocket(server, {
    ...options,
    authorizationScope: options.authorizationScope ?? createAuthenticatedTenantScope(),
  });
}

type TestSessionOptions = Omit<ResponsesWebSocketSessionOptions, "authorizationScope"> & {
  authorizationScope?: AuthenticatedTenantScope;
};

function createTestSession(socket: WebSocket, options: TestSessionOptions) {
  return new ResponsesWebSocketSession(socket, {
    ...options,
    authorizationScope: options.authorizationScope ?? createAuthenticatedTenantScope(),
  });
}

async function startServer(
  startRelay: WebSocketRelayStarter,
  options: Omit<TestAttachOptions, "startRelay"> = {},
  clientHeaders?: Record<string, string>,
) {
  const server = createServer((_req, res) => { res.writeHead(404).end(); });
  attachTestResponsesWebSocket(server, { startRelay, ...options });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  const socket = new WebSocket(
    `ws://127.0.0.1:${address.port}/v1/responses`,
    clientHeaders ? { headers: clientHeaders } : undefined,
  );
  sockets.push(socket);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  return socket;
}

function collect(socket: WebSocket) {
  const messages: any[] = [];
  socket.on("message", data => messages.push(JSON.parse(data.toString())));
  return messages;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for websocket state");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

describe("Responses WebSocket server", () => {
  it("rejects hostile WebSocket Host and Origin before auth or process allocation", async () => {
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: 128 * 1024 * 1024,
    });
    let relayCalls = 0;
    attachTestResponsesWebSocket(server, {
      apiKey: TEST_API_KEY,
      processBudget: budget,
      startRelay: async () => {
        relayCalls++;
        return relayHandle(Promise.resolve(completed("unexpected")));
      },
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const url = `ws://127.0.0.1:${address.port}/v1/responses`;

    const rejectStatus = (headers: Record<string, string>) => new Promise<number>((resolve, reject) => {
      const socket = new WebSocket(url, { headers });
      socket.once("unexpected-response", (_request, response) => {
        response.resume();
        resolve(response.statusCode ?? 0);
      });
      socket.once("open", () => reject(new Error("hostile WebSocket authority was accepted")));
      socket.once("error", () => undefined);
    });

    expect(await rejectStatus({
      host: "attacker.example",
      authorization: "Bearer wrong",
    })).toBe(421);
    expect(await rejectStatus({
      host: "attacker.test",
      authorization: "Bearer wrong",
    })).toBe(421);
    expect(await rejectStatus({
      origin: "https://attacker.example",
      authorization: "Bearer wrong",
    })).toBe(403);
    expect(await rejectStatus({
      origin: "https://attacker.test",
      authorization: "Bearer wrong",
    })).toBe(403);
    expect(budget.snapshot()).toMatchObject({
      sessions: 0,
      queuedBytes: 0,
      ingressReservedBytes: 0,
    });
    expect(relayCalls).toBe(0);
  });

  it("accepts an explicitly allowed WebSocket Host and Origin", async () => {
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    const budget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: 128 * 1024 * 1024,
    });
    attachTestResponsesWebSocket(server, {
      apiKey: TEST_API_KEY,
      allowedHosts: ["ghostprov.example"],
      processBudget: budget,
      startRelay: async () => relayHandle(Promise.resolve(completed("unused"))),
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const socket = new WebSocket(
      `ws://127.0.0.1:${address.port}/v1/responses`,
      {
        headers: {
          host: "ghostprov.example",
          origin: "https://ghostprov.example",
          authorization: `Bearer ${TEST_API_KEY}`,
        },
      },
    );
    sockets.push(socket);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });

    expect(budget.snapshot().sessions).toBe(1);
    socket.close();
    await waitFor(() => budget.snapshot().sessions === 0);
  });

  it("requires the configured bearer key before allocating an upgrade", async () => {
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    const budget = new WebSocketProcessBudget({
      maxSessions: 8,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: 64 * 1024 * 1024,
    });
    attachTestResponsesWebSocket(server, {
      apiKey: TEST_API_KEY,
      processBudget: budget,
      startRelay: async () => relayHandle(Promise.resolve(completed("unused"))),
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const url = `ws://127.0.0.1:${address.port}/v1/responses`;

    for (const headers of [undefined, { authorization: "Bearer wrong" }]) {
      const rejected = new WebSocket(url, headers ? { headers } : undefined);
      rejected.on("error", () => undefined);
      const status = await new Promise<number>(resolve => {
        rejected.once("unexpected-response", (_request, response) => {
          response.resume();
          resolve(response.statusCode ?? 0);
        });
      });
      expect(status).toBe(401);
      expect(budget.snapshot().sessions).toBe(0);
    }

    const accepted = new WebSocket(url, {
      headers: { authorization: `Bearer ${TEST_API_KEY}` },
    });
    sockets.push(accepted);
    await new Promise<void>((resolve, reject) => {
      accepted.once("open", resolve);
      accepted.once("error", reject);
    });
    expect(budget.snapshot().sessions).toBe(1);
  });

  it("guarantees forty-two default sessions plus full promotion headroom", () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const jumboBytes = webSocketIngressReservationBytes(MAX_WS_PAYLOAD_BYTES);
    expect(normalBytes).toBe(17 * 1024 * 1024);
    expect(jumboBytes).toBe(257 * 1024 * 1024);

    const budget = new WebSocketProcessBudget({
      maxSessions: 256,
      maxQueuedBytes: 512 * 1024 * 1024,
      maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
    });
    expect(DEFAULT_WS_PROMOTION_HEADROOM_BYTES).toBe(48 * 1024 * 1024);
    const defaults = Array.from(
      { length: 42 },
      () => budget.openSession(normalBytes, DEFAULT_WS_PROMOTION_HEADROOM_BYTES),
    );
    expect(defaults.every(Boolean)).toBe(true);
    expect(budget.openSession(normalBytes, DEFAULT_WS_PROMOTION_HEADROOM_BYTES)).toBeNull();
    expect(budget.snapshot()).toMatchObject({
      sessions: 42,
      ingressReservedBytes: 714 * 1024 * 1024,
      queuedBytes: 0,
      maxIngressBytes: 768 * 1024 * 1024,
    });
    expect(defaults[0]!.tryEnsureIngress(DEFAULT_WS_PROMOTION_HEADROOM_BYTES)).toBe(true);
    expect(budget.snapshot().ingressReservedBytes).toBe(762 * 1024 * 1024);
    defaults[0]!.releaseIngress();
    for (const lease of defaults) lease!.close();

    const jumbo = budget.openSession(jumboBytes);
    expect(jumbo).not.toBeNull();
    const alongsideJumbo = Array.from({ length: 30 }, () => budget.openSession(normalBytes));
    expect(alongsideJumbo.every(Boolean)).toBe(true);
    expect(budget.openSession(normalBytes)).toBeNull();
    expect(budget.snapshot()).toMatchObject({
      sessions: 31,
      ingressReservedBytes: 767 * 1024 * 1024,
    });
    jumbo!.close();
    for (const lease of alongsideJumbo) lease!.close();
    expect(budget.snapshot()).toMatchObject({
      sessions: 0,
      ingressReservedBytes: 0,
      queuedBytes: 0,
    });
  });

  it("validates parser, fast-path, and trusted scope limits before upgrade", () => {
    const startRelay = async () => relayHandle(Promise.resolve(completed("unused")));
    for (const maxPayloadBytes of [0, Number.NaN, MAX_WS_PAYLOAD_BYTES + 1]) {
      const server = createServer();
      expect(() => attachTestResponsesWebSocket(server, {
        maxPayloadBytes,
        startRelay,
      })).toThrow(RangeError);
    }
    for (const fastPathPayloadBytes of [0, DEFAULT_WS_MAX_PAYLOAD_BYTES + 1]) {
      const server = createServer();
      expect(() => attachTestResponsesWebSocket(server, {
        maxPayloadBytes: DEFAULT_WS_MAX_PAYLOAD_BYTES,
        fastPathPayloadBytes,
        startRelay,
      })).toThrow(RangeError);
    }
    const server = createServer();
    expect(() => attachTestResponsesWebSocket(server, {
      authorizationScope: {} as AuthenticatedTenantScope,
      startRelay,
    })).toThrow(TypeError);
    const missingScopeServer = createServer();
    expect(() => attachResponsesWebSocket(missingScopeServer, {
      startRelay,
    } as unknown as AttachResponsesWebSocketOptions)).toThrow(TypeError);
    expect(() => new ResponsesWebSocketSession(
      new FakeSocket() as unknown as WebSocket,
      { startRelay } as unknown as ResponsesWebSocketSessionOptions,
    )).toThrow(TypeError);
  });

  it("reserves enough raw parser memory when a custom fast path is smaller", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 64,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: 512 * MiB,
    });
    await startServer(
      async () => relayHandle(Promise.resolve(completed("unused"))),
      {
        processBudget: budget,
        fastPathPayloadBytes: MiB,
        maxPayloadBytes: DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES,
      },
    );
    expect(budget.snapshot().ingressReservedBytes).toBe(17 * MiB);
  });

  it("does not spend a 257 MiB jumbo reservation on Bun's compatibility path", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 64,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: 512 * MiB,
    });
    await startServer(
      async () => relayHandle(Promise.resolve(completed("unused"))),
      { processBudget: budget },
      { "x-ghostprov-ws-jumbo": "1" },
    );
    expect(budget.snapshot().ingressReservedBytes).toBe(17 * MiB);
  });

  it("automatically promotes a large message without changing the connection", async () => {
    const input = "x".repeat(10 * 1024 * 1024);
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed(`resp_auto_${calls}`)));
    }, { telemetry });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "large",
      model: "gpt-5.6-sol",
      store: false,
      input,
    }));
    await waitFor(() => messages.some(message => message.type === "response.completed"), 5_000);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "small",
      model: "gpt-5.6-sol",
      store: false,
      input: "small follow-up",
    }));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
    expect(calls).toBe(2);
    expect(socket.readyState).toBe(WebSocket.OPEN);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressRejections: 0,
      ingressMessageBytesPeak: expect.any(Number),
    });
  }, 15_000);

  it("enforces Bun's native ceiling and retains the jumbo header below it", async () => {
    const input = "x".repeat(DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES + 1024);
    const parserTelemetry = new WebSocketTelemetry();
    let normalCalls = 0;
    const normal = await startServer(async () => {
      normalCalls++;
      return relayHandle(Promise.resolve(completed("unexpected")));
    }, { telemetry: parserTelemetry });
    const normalClosed = new Promise<number>(resolve => normal.once("close", resolve));
    normal.send(JSON.stringify({ type: "response.create", input }));
    // Bun's `ws` compatibility layer may surface a parser-sized close as
    // abnormal (1006) for a multi-MiB frame even though smaller overflows
    // preserve the protocol's 1009 close code.
    expect([1006, 1009]).toContain(await normalClosed);
    expect(normalCalls).toBe(0);
    expect(parserTelemetry.snapshotAndReset().ingressParserRejections).toBe(1);

    let jumboCalls = 0;
    const jumbo = await startServer(async () => {
      jumboCalls++;
      return relayHandle(Promise.resolve(completed("resp_jumbo_ingress")));
    }, {}, { "x-ghostprov-ws-jumbo": "1" });
    const messages = collect(jumbo);
    jumbo.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(10 * 1024 * 1024),
    }));
    await waitFor(() => messages.some(message => message.type === "response.completed"), 5_000);
    expect(jumboCalls).toBe(1);
  }, 20_000);

  it("fails fast and releases its connection when large-message ingress is unavailable", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 64 * MiB,
      maxIngressBytes: 70 * MiB,
    });
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed("resp_waited_jumbo")));
    }, { processBudget: budget, telemetry });
    const blocker = budget.openSession(0)!;
    expect(blocker.tryEnsureIngress(53 * MiB)).toBe(true);
    const closed = new Promise<number>(resolve => socket.once("close", resolve));
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "unavailable",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(5 * MiB),
    }));
    expect(await closed).toBe(1013);
    expect(calls).toBe(0);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressRejections: 1,
      ingressReservedBytes: 53 * MiB,
    });
    await waitFor(() => budget.snapshot().sessions === 1);
    expect(budget.snapshot().ingressReservedBytes).toBe(53 * MiB);
    blocker.close();
  }, 15_000);

  it("never lets a later small message overtake a large message awaiting ingress", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 64 * MiB,
      maxIngressBytes: 70 * MiB,
    });
    const calls: string[] = [];
    const socket = await startServer(async body => {
      const serialized = JSON.stringify(body.input);
      calls.push(serialized.includes("small-follow-up") ? "small" : "large");
      return relayHandle(Promise.resolve(completed(`resp_${calls.length}`)));
    }, { processBudget: budget });
    const blocker = budget.openSession(0)!;
    expect(blocker.tryEnsureIngress(53 * MiB)).toBe(true);
    const closed = new Promise<number>(resolve => socket.once("close", resolve));
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "ordered",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(5 * MiB),
    }));
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "ordered",
      model: "gpt-5.6-sol",
      store: false,
      input: "small-follow-up",
    }));
    expect(await closed).toBe(1013);
    expect(calls).toEqual([]);
    blocker.close();
  }, 15_000);

  it("keeps thirty-six normal agents healthy while one sends a 10 MiB turn", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 48,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
    });
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachTestResponsesWebSocket(server, {
      processBudget: budget,
      telemetry,
      startRelay: async () => {
        calls++;
        return relayHandle(Promise.resolve(completed(`resp_mixed_${calls}`)));
      },
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const clients = Array.from(
      { length: 37 },
      () => new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`),
    );
    sockets.push(...clients);
    await Promise.all(clients.map(socket => new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    })));
    const outputs = clients.map(collect);
    clients.forEach((socket, index) => socket.send(JSON.stringify({
      type: "response.create",
      stream_id: `mixed-${index}`,
      model: "gpt-5.6-sol",
      store: false,
      input: index === 0 ? "x".repeat(10 * MiB) : `small-${index}`,
    })));
    await waitFor(
      () => outputs.every(messages => messages.some(message => message.type === "response.completed")),
      10_000,
    );
    expect(calls).toBe(37);
    expect(clients.every(socket => socket.readyState === WebSocket.OPEN)).toBe(true);
    expect(budget.snapshot()).toMatchObject({
      sessions: 37,
      queuedBytes: 0,
    });
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressGuaranteedSessions: 42,
      ingressPromotions: 1,
      ingressRejections: 0,
      ingressUpgradeRejectionsTotal: 0,
    });
  }, 20_000);

  it("rejects an upgrade before parser construction when ingress is exhausted", async () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const budget = new WebSocketProcessBudget({
      maxSessions: 8,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: normalBytes,
    });
    const telemetry = new WebSocketTelemetry();
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachTestResponsesWebSocket(server, {
      processBudget: budget,
      telemetry,
      startRelay: async () => relayHandle(new Promise(() => {})),
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const url = `ws://127.0.0.1:${address.port}/v1/responses`;

    const first = new WebSocket(url);
    sockets.push(first);
    await new Promise<void>((resolve, reject) => {
      first.once("open", resolve);
      first.once("error", reject);
    });
    expect(budget.snapshot()).toMatchObject({
      sessions: 1,
      ingressReservedBytes: normalBytes,
    });

    const rejected = new WebSocket(url);
    sockets.push(rejected);
    rejected.on("error", () => undefined);
    const status = await new Promise<number>(resolve => {
      rejected.once("unexpected-response", (_request, response) => {
        response.resume();
        resolve(response.statusCode ?? 0);
      });
    });
    expect(status).toBe(503);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressUpgradeRejections: 1,
      ingressUpgradeRejectionsTotal: 1,
    });
    expect(budget.snapshot()).toMatchObject({
      sessions: 1,
      ingressReservedBytes: normalBytes,
    });
    first.close();
    await waitFor(() => budget.snapshot().sessions === 0);
    expect(budget.snapshot().ingressReservedBytes).toBe(0);
  });

  it("reaps twenty-seven dead-idle peers and recovers every ingress reservation", async () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const budget = new WebSocketProcessBudget({
      maxSessions: 27,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: normalBytes * 27 + DEFAULT_WS_PROMOTION_HEADROOM_BYTES,
    });
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachTestResponsesWebSocket(server, {
      processBudget: budget,
      heartbeatIntervalMs: 50,
      pongTimeoutMs: 100,
      idleTimeoutMs: 200,
      startRelay: async () => relayHandle(new Promise(() => {})),
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const url = `ws://127.0.0.1:${address.port}/v1/responses`;

    const dead = Array.from({ length: 27 }, () => new WebSocket(url));
    sockets.push(...dead);
    await Promise.all(dead.map(socket => new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    })));
    expect(budget.snapshot()).toMatchObject({
      sessions: 27,
      ingressReservedBytes: normalBytes * 27,
    });
    await waitFor(() => budget.snapshot().sessions === 0, 2_000);
    expect(budget.snapshot()).toMatchObject({ sessions: 0, ingressReservedBytes: 0 });

    const replacement = new WebSocket(url);
    sockets.push(replacement);
    await new Promise<void>((resolve, reject) => {
      replacement.once("open", resolve);
      replacement.once("error", reject);
    });
    expect(budget.snapshot().sessions).toBe(1);
    replacement.close();
    await waitFor(() => budget.snapshot().sessions === 0);
  }, 10_000);

  it("expires a responsive but application-idle peer and clears its timer", async () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const budget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: normalBytes,
    });
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachTestResponsesWebSocket(server, {
      processBudget: budget,
      heartbeatIntervalMs: 10,
      pongTimeoutMs: 30,
      idleTimeoutMs: 60,
      startRelay: async () => relayHandle(new Promise(() => {})),
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing test address");
    const socket = new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`);
    sockets.push(socket);
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    const closed = new Promise<number>(resolve => socket.once("close", resolve));
    expect(await closed).toBe(1006);
    expect(budget.snapshot()).toMatchObject({ sessions: 0, ingressReservedBytes: 0 });
    await new Promise(resolve => setTimeout(resolve, 40));
    expect(budget.snapshot()).toMatchObject({ sessions: 0, ingressReservedBytes: 0 });
  });

  it("does not idle-expire a live silent relay", async () => {
    let started = false;
    const socket = await startServer(async () => {
      started = true;
      return relayHandle(new Promise(() => {}));
    }, {
      heartbeatIntervalMs: 10,
      pongTimeoutMs: 30,
      idleTimeoutMs: 50,
    });
    socket.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "silent but active",
    }));
    await waitFor(() => started);
    await new Promise(resolve => setTimeout(resolve, 120));
    expect(socket.readyState).toBe(WebSocket.OPEN);
    socket.close();
  });

  it("sends direct event objects, makes streaming implicit, and echoes only named stream IDs", async () => {
    const bodies: Record<string, unknown>[] = [];
    const startRelay: WebSocketRelayStarter = async body => {
      bodies.push(body);
      const id = `resp_${bodies.length}`;
      return relayHandle(Promise.resolve(completed(id)), [
        { event: "response.created", data: JSON.stringify({ type: "response.created", response: { id } }) },
        { event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta: "hi" }) },
      ]);
    };
    const socket = await startServer(startRelay);
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "default",
      reasoning: {
        effort: "max",
        mode: "pro",
        context: "all_turns",
        summary: "concise",
      },
      service_tier: "fast",
    }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "agent-1", model: "gpt-5.6-sol", store: false, input: "named" }));

    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
    expect(messages.every(message => !("parsed" in message))).toBe(true);
    const defaultEvents = messages.filter(message => message.response?.id === "resp_1" || message.delta === "hi" && !("stream_id" in message));
    expect(defaultEvents.some(message => !("stream_id" in message))).toBe(true);
    expect(messages.filter(message => message.stream_id === "agent-1").length).toBeGreaterThan(0);
    expect(bodies).toHaveLength(2);
    expect(bodies[0]).toMatchObject({
      stream: true,
      store: true,
      reasoning: {
        effort: "max",
        mode: "pro",
        context: "all_turns",
        summary: "concise",
      },
      service_tier: "fast",
    });
    expect(bodies[0]).not.toHaveProperty("stream_id");
    expect(bodies[1]).not.toHaveProperty("stream_id");
  });

  it("accepts Codex stream:true as an implicit-stream compatibility hint but rejects false", async () => {
    const bodies: Record<string, unknown>[] = [];
    const socket = await startServer(async body => {
      bodies.push(body);
      return relayHandle(Promise.resolve(completed("resp_codex")));
    });
    const messages = collect(socket);

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "codex",
      model: "gpt-5.6-sol",
      store: false,
      stream: true,
      input: "hello from Codex",
    }));

    await waitFor(() => messages.some(message => message.type === "response.completed"));
    expect(messages.some(message => message.type === "error")).toBe(false);
    expect(bodies).toHaveLength(1);
    expect(bodies[0]).toMatchObject({ stream: true, store: true, input: "hello from Codex" });

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "codex-false",
      model: "gpt-5.6-sol",
      store: false,
      stream: false,
      input: "invalid non-streaming WebSocket request",
    }));
    await waitFor(() => messages.some(message => message.stream_id === "codex-false"));
    const error = messages.find(message => message.stream_id === "codex-false");
    expect(error).toMatchObject({
      stream_id: "codex-false",
      error: {
        code: "unsupported_parameter",
        param: "stream",
        message: "stream:false is incompatible with implicit WebSocket streaming",
      },
    });
    expect(bodies).toHaveLength(1);
  });

  it("serializes the same lane while different lanes overlap", async () => {
    const gates = new Map<string, ReturnType<typeof deferred<any>>>();
    const started: string[] = [];
    let active = 0;
    let maxActive = 0;
    const startRelay: WebSocketRelayStarter = async body => {
      const label = String(body.input);
      started.push(label);
      active++;
      maxActive = Math.max(maxActive, active);
      const gate = deferred<any>();
      gates.set(label, gate);
      const final = gate.promise.finally(() => { active--; });
      return relayHandle(final);
    };
    const socket = await startServer(startRelay, { maxActive: 16 });
    const messages = collect(socket);
    const create = (stream_id: string, text: string) => socket.send(JSON.stringify({
      type: "response.create", stream_id, model: "gpt-5.6-sol", store: false, input: text,
    }));
    create("same", "a1");
    create("same", "a2");
    create("other", "b1");
    await waitFor(() => started.length === 2);
    expect(started).toEqual(["a1", "b1"]);
    expect(maxActive).toBe(2);
    gates.get("a1")!.resolve(completed("resp_a1"));
    await waitFor(() => started.includes("a2"));
    gates.get("b1")!.resolve(completed("resp_b1"));
    gates.get("a2")!.resolve(completed("resp_a2"));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 3);
  });

  it("forwards a Codex continuation as store:true with only its current delta", async () => {
    const bodies: Record<string, unknown>[] = [];
    const call = { type: "function_call", id: "fc_1", call_id: "call_1", name: "tool", arguments: "{}" };
    const startRelay: WebSocketRelayStarter = async body => {
      bodies.push(body);
      const final = bodies.length === 1
        ? completed("resp_parent", [call])
        : {
            ...completed("resp_child"),
            previous_response_id: body.previous_response_id,
          };
      return relayHandle(Promise.resolve(final));
    };
    const socket = await startServer(startRelay);
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", stream_id: "main", model: "gpt-5.6-sol", store: false, input: "use tool" }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_parent"));
    const delta = [{ type: "function_call_output", call_id: "call_1", output: "ok" }];
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "main",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_parent",
      input: delta,
    }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_child"));
    expect(messages.find(message => message.response?.id === "resp_child").response.previous_response_id).toBe("resp_parent");
    expect(bodies[1]).toMatchObject({
      previous_response_id: "resp_parent",
      input: delta,
      store: true,
      stream: true,
    });
    expect(JSON.stringify(bodies[1].input)).not.toContain('"type":"function_call"');
  });

  it("expands only scoped warmup state into the first persisted generated child", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const graph = new ResponseGraph();
    const relays: Array<{
      body: Record<string, unknown>;
      authorizationScope: AuthenticatedTenantScope;
      encodedBodyBytes?: number;
    }> = [];
    const socket = await startServer(async (body, options) => {
      relays.push({
        body,
        authorizationScope: options.authorizationScope,
        encodedBodyBytes: options.encodedBodyBytes,
      });
      const final = {
        ...completed(relays.length === 1
          ? "resp_generated_after_warmup"
          : "resp_provider_next"),
        previous_response_id: body.previous_response_id ?? null,
      };
      return relayHandle(Promise.resolve(final));
    }, {
      authorizationScope,
      graphFactory: () => graph,
    });
    const messages = collect(socket);
    const normalizedUser = (text: string) => ({
      type: "message",
      role: "user",
      content: [{ type: "input_text", text }],
    });

    socket.send(JSON.stringify({
      type: "response.create",
      generate: false,
      stream_id: "warmup-lane",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_provider_before_warmup",
      input: "warmup one",
    }));
    await waitFor(() => messages.some(message =>
      typeof message.response?.id === "string" &&
      message.response.id.startsWith(SYNTHETIC_WARMUP_ID_PREFIX)));
    const firstAliasMessage = messages.find(message =>
      message.response?.id?.startsWith(SYNTHETIC_WARMUP_ID_PREFIX));
    const firstAlias = firstAliasMessage.response.id as string;
    expect(firstAliasMessage.response.previous_response_id)
      .toBe("resp_provider_before_warmup");

    const warmupDelta = {
      type: "function_call_output",
      call_id: "warm_call",
      output: "warmup two",
    };
    socket.send(JSON.stringify({
      type: "response.create",
      generate: false,
      stream_id: "warmup-lane",
      store: true,
      previous_response_id: firstAlias,
      input: [warmupDelta],
    }));
    await waitFor(() => messages.filter(message =>
      message.response?.id?.startsWith(SYNTHETIC_WARMUP_ID_PREFIX)).length === 2);
    const aliasMessages = messages.filter(message =>
      message.response?.id?.startsWith(SYNTHETIC_WARMUP_ID_PREFIX));
    const secondAlias = aliasMessages[1]!.response.id as string;
    expect(aliasMessages[1]!.response.previous_response_id).toBe(firstAlias);

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "warmup-lane",
      store: false,
      previous_response_id: secondAlias,
      client_metadata: { authorization_scope: "client-controlled" },
      input: "generate now",
    }));
    await waitFor(() => messages.some(message =>
      message.response?.id === "resp_generated_after_warmup"));

    expect(relays).toHaveLength(1);
    expect(relays[0]).toEqual({
      authorizationScope,
      encodedBodyBytes: undefined,
      body: expect.objectContaining({
        model: "gpt-5.6-sol",
        previous_response_id: "resp_provider_before_warmup",
        input: [normalizedUser("warmup one"), warmupDelta, normalizedUser("generate now")],
        store: true,
        stream: true,
      }),
    });
    expect(JSON.stringify(relays[0]!.body)).not.toContain(SYNTHETIC_WARMUP_ID_PREFIX);
    expect(JSON.stringify(relays[0]!.body)).not.toContain("authorizationScope");
    expect(messages.find(message =>
      message.response?.id === "resp_generated_after_warmup").response.previous_response_id)
      .toBe(secondAlias);
    expect(graph.stats().nodes).toBe(2);

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "warmup-lane",
      store: false,
      previous_response_id: "resp_generated_after_warmup",
      input: "provider continuation",
    }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_provider_next"));
    expect(relays[1]!.body).toMatchObject({
      previous_response_id: "resp_generated_after_warmup",
      input: "provider continuation",
      store: true,
      stream: true,
    });
    expect(relays[1]!.encodedBodyBytes).toBeGreaterThan(0);
    expect(graph.stats().nodes).toBe(2);
  });

  it("withholds a provider terminal before allowing cross-lane persisted forks", async () => {
    const parent = completed("resp_terminal_barrier");
    const parentFinal = deferred<any>();
    const bodies: Record<string, unknown>[] = [];
    const socket = await startServer(async body => {
      bodies.push(body);
      if (bodies.length === 1) {
        return relayHandle(parentFinal.promise, [{
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: parent }),
        }]);
      }
      const text = String(body.input);
      return relayHandle(Promise.resolve({
        ...completed(`resp_fork_${text}`),
        previous_response_id: body.previous_response_id,
      }));
    });
    const messages: any[] = [];
    let forksSent = false;
    socket.on("message", data => {
      const message = JSON.parse(data.toString());
      messages.push(message);
      if (message.response?.id === parent.id && !forksSent) {
        forksSent = true;
        for (const fork of ["left", "right"]) {
          socket.send(JSON.stringify({
            type: "response.create",
            stream_id: `child-${fork}`,
            model: "gpt-5.6-sol",
            store: false,
            previous_response_id: parent.id,
            input: fork,
          }));
        }
      }
    });
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "parent-lane",
      model: "gpt-5.6-sol",
      store: false,
      input: "parent",
    }));
    await new Promise(resolve => setTimeout(resolve, 25));
    const terminalExposedBeforeFinal = forksSent;
    parentFinal.resolve(parent);
    await waitFor(() => ["left", "right"].every(fork =>
      messages.some(message => message.response?.id === `resp_fork_${fork}`)) ||
      messages.some(message => message.error?.code === "previous_response_not_found")
    );

    expect(terminalExposedBeforeFinal).toBe(false);
    expect(messages.some(message => message.error?.code === "previous_response_not_found")).toBe(false);
    expect(bodies).toHaveLength(3);
    for (const fork of ["left", "right"]) {
      expect(bodies).toContainEqual(expect.objectContaining({
        previous_response_id: parent.id,
        input: fork,
        store: true,
        stream: true,
      }));
    }
  });

  it("does not replay a large image across a small provider-backed continuation", async () => {
    const MiB = 1024 * 1024;
    const imageUrl = `data:image/png;base64,${"A".repeat(10 * MiB)}`;
    const bodies: Record<string, unknown>[] = [];
    const telemetry = new WebSocketTelemetry();
    const socket = await startServer(async body => {
      bodies.push(body);
      return relayHandle(Promise.resolve(completed(
        bodies.length === 1 ? "resp_image_parent" : "resp_image_child",
      )));
    }, { telemetry });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "image-lineage",
      model: "gpt-5.6-sol",
      store: false,
      input: [{
        role: "user",
        content: [
          { type: "input_text", text: "inspect this exact image" },
          { type: "input_image", image_url: imageUrl, detail: "original" },
        ],
      }],
    }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_image_parent"), 5_000);
    const continuation = JSON.stringify({
      type: "response.create",
      stream_id: "image-lineage",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_image_parent",
      input: "now inspect the same image again",
    });
    expect(Buffer.byteLength(continuation)).toBeLessThan(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    socket.send(continuation);
    await waitFor(() => messages.some(message => message.response?.id === "resp_image_child"), 5_000);
    const forwardedDelta = JSON.stringify(bodies[1]?.input);
    expect(forwardedDelta).not.toContain(imageUrl);
    expect(forwardedDelta).toContain("now inspect the same image again");
    expect(bodies[1]).toMatchObject({
      previous_response_id: "resp_image_parent",
      store: true,
      stream: true,
    });
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressRejections: 0,
    });
  }, 15_000);

  it("holds warmup materialization capacity only until router handoff completes", async () => {
    const shared = new SharedByteBudget(16 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxBytes: 8 * 1024 * 1024,
      sharedBudget: shared,
    });
    const authorizationScope = createAuthenticatedTenantScope();
    const warmupId = `${SYNTHETIC_WARMUP_ID_PREFIX}lineage_root`;
    graph.recordWarmup(warmupId, {
      type: "response.create",
      generate: false,
      store: false,
      input: [{ type: "message", content: "x".repeat(1024 * 1024) }],
    }, { authorizationScope });
    const storedBytes = shared.used;
    const handoff = deferred<RunHandle>();
    let started = false;
    const socket = await startServer(async () => {
      started = true;
      return handoff.promise;
    }, {
      authorizationScope,
      graphFactory: () => graph,
    });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "lineage",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: warmupId,
      input: "next",
    }));
    await waitFor(() => started);
    expect(shared.used).toBeGreaterThan(storedBytes);

    handoff.resolve(relayHandle(Promise.resolve({
      id: "resp_warmup_child",
      status: "completed",
      output: [],
    })));
    await waitFor(() => messages.some(message => message.type === "response.completed"));
    expect(shared.used).toBe(storedBytes);
    expect(graph.stats().nodes).toBe(1);
    socket.close();
  });

  it("keeps cancelled warmup handoff memory charged until late relay cleanup settles", async () => {
    const shared = new SharedByteBudget(16 * 1024 * 1024);
    const graph = new ResponseGraph({ maxBytes: 8 * 1024 * 1024, sharedBudget: shared });
    const authorizationScope = createAuthenticatedTenantScope();
    const warmupId = `${SYNTHETIC_WARMUP_ID_PREFIX}aborted_handoff`;
    graph.recordWarmup(warmupId, {
      type: "response.create",
      generate: false,
      input: [{ type: "message", content: "x".repeat(1024 * 1024) }],
    }, { authorizationScope });
    const storedBytes = shared.used;
    const processBudget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 8 * 1024 * 1024,
      maxIngressBytes: 128 * 1024 * 1024,
    });
    const handoff = deferred<RunHandle>();
    let retainedBody: Record<string, unknown> | null = null;
    let started = false;
    const socket = await startServer(async body => {
      started = true;
      retainedBody = body;
      return handoff.promise;
    }, {
      authorizationScope,
      graphFactory: () => graph,
      processBudget,
    });

    socket.send(JSON.stringify({
      type: "response.create",
      previous_response_id: warmupId,
      input: "next",
    }));
    await waitFor(() => started && shared.used > storedBytes);
    socket.close();
    await waitFor(() => socket.readyState === WebSocket.CLOSED);

    expect((retainedBody!.input as unknown[])[0]).toMatchObject({ type: "message" });
    expect(shared.used).toBeGreaterThan(storedBytes);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(processBudget.snapshot()).toMatchObject({
      sessions: 1,
      queuedBytes: expect.any(Number),
    });
    expect(processBudget.snapshot().queuedBytes).toBeGreaterThan(0);
    expect(() => createTestSession(new FakeSocket() as unknown as WebSocket, {
      authorizationScope,
      processBudget,
      startRelay: async () => relayHandle(Promise.resolve(completed("unexpected"))),
    })).toThrow("session capacity");

    const lateFinal = deferred<any>();
    let lateCleanupStarted = false;
    handoff.resolve({
      async *events() {
        lateCleanupStarted = true;
      },
      final: lateFinal.promise,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    });
    await waitFor(() => lateCleanupStarted);
    expect(shared.used).toBeGreaterThan(storedBytes);
    expect(processBudget.snapshot().sessions).toBe(1);

    const latePermits = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    lateFinal.resolve(attachAtomicBytePermit(
      completed("resp_late_cleanup"),
      await latePermits.acquire(8_192),
    ));
    await waitFor(() => (
      shared.used === 0 &&
      processBudget.snapshot().sessions === 0 &&
      latePermits.snapshot().activePermits === 0
    ));
    latePermits.close();
    expect(processBudget.snapshot().queuedBytes).toBe(0);
    const replacement = createTestSession(new FakeSocket() as unknown as WebSocket, {
      authorizationScope,
      processBudget,
      startRelay: async () => relayHandle(Promise.resolve(completed("unused"))),
    });
    expect(processBudget.snapshot().sessions).toBe(1);
    replacement.close();
    expect(processBudget.snapshot().sessions).toBe(0);
  });

  it("aborts relay work after malformed local frame processing", async () => {
    let relaySignal: AbortSignal | undefined;
    const socket = await startServer(async (_body, options) => {
      relaySignal = options.signal;
      return relayHandle(new Promise(() => undefined), [{
        event: "response.output_text.delta",
        data: "{",
      }]);
    });
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", input: "x" }));

    await waitFor(() => messages.some(message => message.type === "error"));
    expect(relaySignal?.aborted).toBe(true);
  });

  it("rejects a buffered terminal that conflicts with the authoritative final", async () => {
    let relaySignal: AbortSignal | undefined;
    const completedFinal = completed("resp_terminal_conflict");
    const failedFinal = { ...completedFinal, status: "failed" };
    const socket = await startServer(async (_body, options) => {
      relaySignal = options.signal;
      return relayHandle(Promise.resolve(failedFinal), [{
        event: "response.completed",
        data: JSON.stringify({ type: "response.completed", response: completedFinal }),
      }]);
    });
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", input: "x" }));

    await waitFor(() => messages.some(message => message.type === "error"));
    expect(messages.some(message => message.type === "response.completed")).toBe(false);
    expect(relaySignal?.aborted).toBe(true);
  });

  it("publishes authoritative final content instead of buffered terminal content", async () => {
    const final = {
      ...completed("resp_authoritative_terminal"),
      previous_response_id: "resp_exact_parent",
      output: [{ type: "message", content: "authoritative" }],
    };
    const buffered = {
      ...final,
      previous_response_id: "resp_wrong_parent",
      output: [{ type: "message", content: "wrong" }],
    };
    const socket = await startServer(async () => relayHandle(Promise.resolve(final), [{
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response: buffered }),
    }]));
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", input: "x" }));

    await waitFor(() => messages.some(message => message.type === "response.completed"));
    const terminal = messages.find(message => message.type === "response.completed");
    expect(terminal.response.previous_response_id).toBe("resp_exact_parent");
    expect(terminal.response.output).toEqual(final.output);
    expect(JSON.stringify(terminal)).not.toContain("wrong");
  });

  it("does not read past a terminal before awaiting an already-ready final", async () => {
    const final = completed("resp_terminal_then_stall");
    const handle: RunHandle = {
      async *events() {
        yield {
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: final }),
        };
        await new Promise<void>(() => undefined);
      },
      final: Promise.resolve(final),
      stats: { bytes: 0, firstByteMs: null, frames: 1 },
    };
    const socket = await startServer(async () => handle);
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", input: "x" }));

    await waitFor(() => messages.some(message => message.type === "response.completed"));
    expect(messages.find(message => message.type === "response.completed").response.id)
      .toBe("resp_terminal_then_stall");
  });

  it("caps active responses per connection and drains queued lanes", async () => {
    const gates: Array<ReturnType<typeof deferred<any>>> = [];
    let active = 0;
    let maxActive = 0;
    const startRelay: WebSocketRelayStarter = async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      const gate = deferred<any>();
      gates.push(gate);
      return relayHandle(gate.promise.finally(() => { active--; }));
    };
    const socket = await startServer(startRelay, { maxActive: 16 });
    const messages = collect(socket);
    for (let index = 0; index < 20; index++) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id: `lane-${index}`,
        model: "gpt-5.6-sol",
        store: false,
        input: String(index),
      }));
    }
    await waitFor(() => gates.length === 16);
    expect(maxActive).toBe(16);
    gates.splice(0, 4).forEach((gate, index) => gate.resolve(completed(`first_${index}`)));
    await waitFor(() => gates.length === 16);
    gates.forEach((gate, index) => gate.resolve(completed(`rest_${index}`)));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 20);
    expect(maxActive).toBe(16);
  });

  it("does not synthesize a duplicate terminal event", async () => {
    const final = completed("resp_once");
    const socket = await startServer(async () => relayHandle(Promise.resolve(final), [
      { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) },
    ]));
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: "once" }));
    await waitFor(() => messages.some(message => message.type === "response.completed"));
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(messages.filter(message => message.type === "response.completed")).toHaveLength(1);
  });

  it("synthesizes the terminal event type from a final-only response", async () => {
    const socket = await startServer(async body => {
      const label = String(body.input);
      const status = label === "failed" ? "failed" : "incomplete";
      return relayHandle(Promise.resolve({
        ...completed(`resp_${status}`),
        status,
      }));
    });
    const messages = collect(socket);
    for (const status of ["failed", "incomplete"]) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id: status,
        model: "gpt-5.6-sol",
        input: status,
      }));
    }

    await waitFor(() => messages.filter(message =>
      message.type === "response.failed" || message.type === "response.incomplete").length === 2);
    expect(messages).toEqual(expect.arrayContaining([
      expect.objectContaining({
        type: "response.failed",
        stream_id: "failed",
        response: expect.objectContaining({ status: "failed" }),
      }),
      expect.objectContaining({
        type: "response.incomplete",
        stream_id: "incomplete",
        response: expect.objectContaining({ status: "incomplete" }),
      }),
    ]));
  });

  it("returns direct validation errors without closing other lanes", async () => {
    const gates = new Map<string, ReturnType<typeof deferred<any>>>();
    const startRelay: WebSocketRelayStarter = async body => {
      const label = typeof body.input === "string" ? body.input : "unknown";
      const gate = deferred<any>();
      gates.set(label, gate);
      return relayHandle(gate.promise);
    };
    const socket = await startServer(startRelay, { maxNamedStreams: 2 });
    const messages = collect(socket);
    socket.send("not json");
    socket.send(JSON.stringify({ type: "response.create", stream_id: "bad space", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "background", background: true, input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "stream", stream: false, input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "one", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "two", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "three", input: "x" }));
    await waitFor(() => messages.filter(message => message.type === "error").length === 5);
    expect(messages.every(message => !("parsed" in message))).toBe(true);
    expect(messages.some(message => message.error?.param === "stream")).toBe(true);
    expect(messages.some(message => message.error?.code === "websocket_stream_limit_reached")).toBe(true);
    expect(socket.readyState).toBe(WebSocket.OPEN);
    for (const [label, gate] of gates) gate.resolve(completed(`resp_${label}`));
  });

  it("aborts queued relay startup and releases session state on disconnect", async () => {
    const aborted = deferred<void>();
    const startRelay: WebSocketRelayStarter = async (_body, options) => {
      options.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      return relayHandle(new Promise(() => {}), [{ event: "response.created", data: '{"type":"response.created"}' }]);
    };
    const socket = await startServer(startRelay);
    socket.send(JSON.stringify({ type: "response.create", stream_id: "main", model: "gpt-5.6-sol", store: false, input: "long" }));
    await new Promise(resolve => setTimeout(resolve, 20));
    socket.close();
    await Promise.race([
      aborted.promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error("abort not observed")), 1_000)),
    ]);
  });

  it("rejects an oversized message at the WebSocket parser boundary", async () => {
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed("unexpected")));
    }, { maxPayloadBytes: 256 });
    const closed = new Promise<number>(resolve => {
      socket.once("close", code => resolve(code));
    });

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "oversized",
      input: "x".repeat(1_024),
    }));

    expect(await closed).toBe(1009);
    expect(calls).toBe(0);
  });

  it("bounds pending request count while retaining accepted FIFO work", async () => {
    const gates: Array<ReturnType<typeof deferred<any>>> = [];
    const socket = await startServer(async () => {
      const gate = deferred<any>();
      gates.push(gate);
      return relayHandle(gate.promise);
    }, {
      maxActive: 1,
      maxPendingRequests: 2,
      maxPendingRequestBytes: 1024 * 1024,
    });
    const messages = collect(socket);
    const create = (stream_id: string) => socket.send(JSON.stringify({
      type: "response.create",
      stream_id,
      model: "gpt-5.6-sol",
      store: false,
      input: stream_id,
    }));

    create("one");
    create("two");
    create("three");
    await waitFor(() => messages.some(message =>
      message.error?.code === "websocket_request_queue_full"));
    expect(gates).toHaveLength(1);
    expect(messages.find(message => message.error?.code === "websocket_request_queue_full"))
      .toMatchObject({
        status: 503,
        stream_id: "three",
        error: { type: "overloaded_error" },
      });

    gates[0]!.resolve(completed("resp_one"));
    await waitFor(() => gates.length === 2);
    gates[1]!.resolve(completed("resp_two"));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
  });

  it("bounds aggregate bytes retained by pending requests", async () => {
    const gate = deferred<any>();
    const first = JSON.stringify({
      type: "response.create",
      stream_id: "large-one",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(256),
    });
    const second = JSON.stringify({
      type: "response.create",
      stream_id: "large-two",
      model: "gpt-5.6-sol",
      store: false,
      input: "y".repeat(128),
    });
    const socket = await startServer(
      async () => relayHandle(gate.promise),
      {
        maxActive: 1,
        maxPendingRequests: 8,
        maxPendingRequestBytes: Buffer.byteLength(first) + Buffer.byteLength(second) - 1,
      },
    );
    const messages = collect(socket);

    socket.send(first);
    socket.send(second);
    await waitFor(() => messages.some(message => message.stream_id === "large-two"));
    expect(messages.find(message => message.stream_id === "large-two"))
      .toMatchObject({
        status: 503,
        error: {
          type: "overloaded_error",
          code: "websocket_request_queue_full",
        },
      });
    gate.resolve(completed("resp_large_one"));
  });

  it("uses stable server-derived relay identity and authorization scope per session", async () => {
    const clientIds: string[] = [];
    const authorizationScopes: AuthenticatedTenantScope[] = [];
    const socket = await startServer(async (_body, options) => {
      clientIds.push(options.clientId);
      authorizationScopes.push(options.authorizationScope);
      return relayHandle(Promise.resolve(completed(`resp_${clientIds.length}`)));
    });
    const messages = collect(socket);
    for (const stream_id of ["identity-a", "identity-b"]) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        client_metadata: { authorization_scope: stream_id },
        input: stream_id,
      }));
    }
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
    expect(clientIds).toHaveLength(2);
    expect(clientIds[0]).toMatch(/^ws-/);
    expect(clientIds[1]).toBe(clientIds[0]);
    expect(authorizationScopes).toHaveLength(2);
    expect(isAuthenticatedTenantScope(authorizationScopes[0])).toBe(true);
    expect(authorizationScopes[1]).toBe(authorizationScopes[0]);
    expect(JSON.stringify(authorizationScopes)).not.toContain("identity-a");
    expect(JSON.stringify(authorizationScopes)).not.toContain("identity-b");
  });

  it("preserves normalized provider status and error identity", async () => {
    const rateMessage = "Rate limit reached for gpt-5.6-sol in organization " +
      "org-ExampleOrganization123 on tokens per min (TPM): Limit 40000000, " +
      "Used 40000000, Requested 609930. Please try again in 914ms.";
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: 429,
      model: "gpt-5.6-sol",
      value: {
        type: "rate_limit_error",
        message: rateMessage,
        param: "input",
        code: "rate_limit_exceeded",
      },
    });
    const failures = [
      Object.assign(new Error(rateMessage), { providerError }),
      new ApiError(401, "The upstream session is not authorized", {
        type: "authentication_error",
        param: null,
        code: "invalid_api_key",
      }),
    ];
    const socket = await startServer(async () => relayHandle(
      Promise.reject(failures.shift()!),
    ));
    const messages = collect(socket);
    for (const stream_id of ["limited", "unauthorized"]) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        input: stream_id,
      }));
    }

    await waitFor(() => messages.filter(message => message.type === "error").length === 2);
    expect(messages.find(message => message.stream_id === "limited")).toMatchObject({
      status: 429,
      error: {
        message: rateMessage,
        type: "rate_limit_error",
        param: "input",
        code: "rate_limit_exceeded",
      },
    });
    expect(messages.find(message => message.stream_id === "unauthorized")).toMatchObject({
      status: 401,
      error: {
        message: "The upstream session is not authorized",
        type: "authentication_error",
        code: "invalid_api_key",
      },
    });
  });

  it("delivers an accepted prefix before a definitive provider error", async () => {
    const relay = new V2RunHandle({ maxBufferedBytes: 4_096 });
    let started = false;
    const socket = await startServer(async () => {
      started = true;
      return relay;
    });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "prefix",
      model: "gpt-5.6-sol",
      store: false,
      input: "prefix then fail",
    }));
    await waitFor(() => started);
    await relay.acceptEvent({
      event: "response.output_text.delta",
      data: JSON.stringify({
        type: "response.output_text.delta",
        delta: "valid prefix",
      }),
    });
    relay.fail(new V2CommittedError(
      "provider rate limit",
      "rate_limit_exceeded",
      false,
    ));

    await waitFor(() => messages.some(message => message.type === "error"));
    expect(messages.map(message => message.type)).toEqual([
      "response.output_text.delta",
      "error",
    ]);
    expect(messages[0]).toMatchObject({ stream_id: "prefix", delta: "valid prefix" });
    expect(messages[1]).toMatchObject({
      status: 429,
      stream_id: "prefix",
      error: { code: "rate_limit_exceeded" },
    });
  });

  it("maps router queue pressure to a WebSocket 503 overload", async () => {
    const socket = await startServer(async () => {
      throw new RouterQueueFullError("private queue accounting");
    });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "queue-full",
      model: "gpt-5.6-sol",
      store: false,
      input: "queue-full",
    }));

    await waitFor(() => messages.some(message => message.stream_id === "queue-full"));
    expect(messages.find(message => message.stream_id === "queue-full")).toMatchObject({
      type: "error",
      status: 503,
      error: {
        message: "GhostProv routing is temporarily overloaded",
        type: "overloaded_error",
        code: "router_queue_full",
      },
    });
  });

  it("recycles completed named-stream slots", async () => {
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed(`resp_recycled_${calls}`)));
    }, { maxNamedStreams: 2 });
    const messages = collect(socket);

    for (let index = 0; index < 5; index++) {
      const stream_id = `sequential-${index}`;
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        input: stream_id,
      }));
      await waitFor(() => messages.some(message =>
        message.type === "response.completed" && message.stream_id === stream_id));
      await new Promise(resolve => setTimeout(resolve, 0));
    }

    expect(calls).toBe(5);
    expect(messages.some(message => message.error?.code === "websocket_stream_limit_reached"))
      .toBe(false);
  });
});

describe("WebSocket raw message decoding", () => {
  it("joins fragmented Buffer data and decodes ArrayBuffer data without commas", () => {
    const fragments = [Buffer.from('{"type":"response.'), Buffer.from('create"}')];
    expect(decodeWebSocketRawData(fragments)).toEqual({
      bytes: fragments[0]!.byteLength + fragments[1]!.byteLength,
      text: '{"type":"response.create"}',
    });
    const bytes = new TextEncoder().encode('{"input":"hello"}');
    const arrayBuffer = bytes.buffer.slice(
      bytes.byteOffset,
      bytes.byteOffset + bytes.byteLength,
    ) as ArrayBuffer;
    expect(decodeWebSocketRawData(arrayBuffer)).toEqual({
      bytes: bytes.byteLength,
      text: '{"input":"hello"}',
    });
  });
});

class FakeSocket {
  readonly CONNECTING = 0;
  readonly OPEN = 1;
  readonly CLOSED = 3;
  readyState = this.OPEN;
  bufferedAmount = 0;
  readonly sent: string[] = [];
  readonly callbacks: Array<(error?: Error) => void> = [];
  closeCode: number | null = null;
  holdCallbacks = false;
  sendError: Error | null = null;

  send(data: string, callback: (error?: Error) => void): void {
    this.sent.push(data);
    if (this.holdCallbacks) this.callbacks.push(callback);
    else queueMicrotask(() => callback(this.sendError ?? undefined));
  }

  close(code: number): void {
    this.closeCode = code;
    this.readyState = this.CLOSED;
  }

  terminate(): void { this.readyState = this.CLOSED; }

  flush(error?: Error): void {
    for (const callback of this.callbacks.splice(0)) callback(error);
  }
}

describe("Responses WebSocket bounded delivery", () => {
  it("normalizes omitted, false, and true client store hints before relay", async () => {
    const socket = new FakeSocket();
    const graph = new ResponseGraph();
    const bodies: Record<string, unknown>[] = [];
    const authorizationScope = createAuthenticatedTenantScope();
    const scopes: AuthenticatedTenantScope[] = [];
    const session = createTestSession(socket as unknown as WebSocket, {
      authorizationScope,
      graph,
      startRelay: async (body, options) => {
        bodies.push(body);
        scopes.push(options.authorizationScope);
        return relayHandle(Promise.resolve(completed(`resp_store_${bodies.length}`)));
      },
    });

    for (const [index, store] of [undefined, false, true].entries()) {
      session.receive(JSON.stringify({
        type: "response.create",
        stream_id: `store-${index}`,
        model: "gpt-5.6-sol",
        ...(store === undefined ? {} : { store }),
        input: `delta-${index}`,
      }));
    }
    await waitFor(() => socket.sent.length === 3);

    expect(bodies).toHaveLength(3);
    expect(bodies.map(body => body.store)).toEqual([true, true, true]);
    expect(bodies.map(body => body.stream)).toEqual([true, true, true]);
    expect(scopes).toEqual([
      authorizationScope,
      authorizationScope,
      authorizationScope,
    ]);
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    session.close();
  });

  it("preserves the prior warmup LRU when a new terminal cannot enter the send queue", async () => {
    const socket = new FakeSocket();
    const authorizationScope = createAuthenticatedTenantScope();
    const graph = new ResponseGraph({ maxNodes: 1 });
    const existing = `${SYNTHETIC_WARMUP_ID_PREFIX}existing`;
    graph.recordWarmup(existing, {
      type: "response.create",
      generate: false,
      input: "keep me",
    }, { authorizationScope, inputOwnership: "owned" });
    const retained = graph.stats();
    const session = createTestSession(socket as unknown as WebSocket, {
      authorizationScope,
      graph,
      maxSendQueueBytes: 64,
      startRelay: async () => relayHandle(Promise.resolve(completed("unused"))),
    });

    session.receive(JSON.stringify({
      type: "response.create",
      generate: false,
      stream_id: "warmup-overflow",
      model: "gpt-5.6-sol",
      input: "must not replace the prior alias",
    }));
    await waitFor(() => session.isIdle());

    expect(socket.sent).toHaveLength(0);
    expect(graph.stats()).toEqual(retained);
    session.close();
  });

  it("never retries an unknown provider ID by dropping the continuation", async () => {
    const socket = new FakeSocket();
    const bodies: Record<string, unknown>[] = [];
    const session = createTestSession(socket as unknown as WebSocket, {
      authorizationScope: createAuthenticatedTenantScope(),
      startRelay: async body => {
        bodies.push(structuredClone(body));
        throw new ApiError(404, "Provider continuation was not found", {
          type: "invalid_request_error",
          param: "previous_response_id",
          code: "previous_response_not_found",
        });
      },
    });

    session.receive(JSON.stringify({
      type: "response.create",
      stream_id: "unknown-provider",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_provider_unknown_exact",
      input: "current delta",
    }));
    await waitFor(() => socket.sent.length === 1);

    expect(bodies).toHaveLength(1);
    expect(bodies[0]).toMatchObject({
      previous_response_id: "resp_provider_unknown_exact",
      store: true,
      stream: true,
    });
    expect(JSON.parse(socket.sent[0]!)).toMatchObject({
      stream_id: "unknown-provider",
      error: { code: "previous_response_not_found" },
    });
    session.close();
  });

  it("holds transferred event memory until the socket send callback settles", async () => {
    const socket = new FakeSocket();
    socket.holdCallbacks = true;
    const permits = new AtomicBytePermitPool({ ordinaryBytes: 4_096, jumboBytes: 1 });
    const frame = attachAtomicBytePermit({
      event: "response.created",
      data: JSON.stringify({ type: "response.created", response: { id: "resp_owned" } }),
    }, (await permits.acquire(4_096)).transfer("websocket-test"));
    const session = createTestSession(socket as unknown as WebSocket, {
      sendTimeoutMs: 10_000,
      startRelay: async () => relayHandle(new Promise(() => {}), [frame]),
    });

    session.receive(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "owned output",
    }));
    await waitFor(() => socket.sent.length === 1);
    expect(permits.snapshot().ordinaryUsedBytes).toBe(4_096);
    socket.flush();
    await waitFor(() => permits.snapshot().ordinaryUsedBytes === 0);
    session.close();
    permits.close();
  });

  it("keeps generated results out of graph storage and decoupled from transport permits", async () => {
    const permits = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const final = attachAtomicBytePermit(
      completed("resp_cached", [{ type: "message", content: "remember" }]),
      (await permits.acquire(8_192)).transfer("graph-test"),
    );
    const graph = new ResponseGraph();
    const resolved = graph.resolve({
      type: "response.create",
      store: false,
      input: "remember",
    }, { authorizationScope: createAuthenticatedTenantScope() });
    expect(resolved.relayBody.input).toBe("remember");
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(permits.snapshot().ordinaryUsedBytes).toBe(8_192);
    graph.clear();
    // Generated output remains solely under its transport permit; the graph
    // keeps no response-history copy.
    expect(permits.snapshot().ordinaryUsedBytes).toBe(8_192);
    releaseAtomicBytePermit(final);
    expect(permits.snapshot().ordinaryUsedBytes).toBe(0);
    permits.close();
  });


  it("enforces a process-wide session budget and releases it on close", () => {
    const budget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 1024 * 1024,
    });
    const first = createTestSession(
      new FakeSocket() as unknown as WebSocket,
      {
        processBudget: budget,
        startRelay: async () => relayHandle(new Promise(() => {})),
      },
    );
    expect(budget.snapshot().sessions).toBe(1);
    expect(() => createTestSession(
      new FakeSocket() as unknown as WebSocket,
      {
        processBudget: budget,
        startRelay: async () => relayHandle(new Promise(() => {})),
      },
    )).toThrow("session capacity");

    first.close();
    expect(budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0 });
    const replacement = createTestSession(
      new FakeSocket() as unknown as WebSocket,
      {
        processBudget: budget,
        startRelay: async () => relayHandle(new Promise(() => {})),
      },
    );
    expect(budget.snapshot().sessions).toBe(1);
    replacement.close();
  });

  it("enforces aggregate retained bytes across independent sessions", async () => {
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 1_024,
    });
    const firstSocket = new FakeSocket();
    const secondSocket = new FakeSocket();
    const firstGate = deferred<any>();
    let secondCalls = 0;
    const first = createTestSession(firstSocket as unknown as WebSocket, {
      processBudget: budget,
      startRelay: async () => relayHandle(firstGate.promise),
    });
    const second = createTestSession(secondSocket as unknown as WebSocket, {
      processBudget: budget,
      startRelay: async () => {
        secondCalls++;
        return relayHandle(Promise.resolve(completed("unexpected")));
      },
    });
    const firstRaw = JSON.stringify({
      type: "response.create",
      stream_id: "budget-one",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(650),
    });
    const secondRaw = JSON.stringify({
      type: "response.create",
      stream_id: "budget-two",
      model: "gpt-5.6-sol",
      store: false,
      input: "y".repeat(650),
    });

    first.receive(firstRaw);
    second.receive(secondRaw);
    await waitFor(() => secondSocket.sent.length > 0);
    expect(secondCalls).toBe(0);
    expect(JSON.parse(secondSocket.sent[0]!)).toMatchObject({
      status: 503,
      stream_id: "budget-two",
      error: {
        type: "overloaded_error",
        code: "websocket_request_queue_full",
      },
    });
    expect(budget.snapshot().queuedBytes).toBeLessThanOrEqual(1_024);
    first.close();
    second.close();
    expect(budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0 });
  });

  it("does not repopulate graph or process budget after a delayed final settles", async () => {
    const budget = new WebSocketProcessBudget({
      maxSessions: 1,
      maxQueuedBytes: 1024 * 1024,
    });
    const graph = new ResponseGraph();
    const final = deferred<any>();
    let started = false;
    const session = createTestSession(
      new FakeSocket() as unknown as WebSocket,
      {
        graph,
        processBudget: budget,
        startRelay: async () => {
          started = true;
          return relayHandle(final.promise);
        },
      },
    );
    session.receive(JSON.stringify({
      type: "response.create",
      stream_id: "delayed-final",
      model: "gpt-5.6-sol",
      store: false,
      input: "remember nothing after close",
    }));
    await waitFor(() => started);
    expect(budget.snapshot().queuedBytes).toBeGreaterThan(0);

    session.close();
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0 });
    final.resolve(completed("resp_too_late", [{ type: "message", content: "late" }]));
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(graph.stats()).toEqual({ nodes: 0, bytes: 0 });
    expect(budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0 });
  });

  it("keeps unrelated lanes alive when one response frame is oversized", async () => {
    const socket = new FakeSocket();
    const signals: AbortSignal[] = [];
    let calls = 0;
    const session = createTestSession(socket as unknown as WebSocket, {
      maxActive: 2,
      maxSendQueueBytes: 512,
      startRelay: async (_body, options) => {
        signals.push(options.signal);
        calls++;
        if (calls === 1) {
          return relayHandle(new Promise(() => {}), [{
            event: "response.output_text.delta",
            data: JSON.stringify({
              type: "response.output_text.delta",
              delta: "x".repeat(4_096),
            }),
          }]);
        }
        return relayHandle(Promise.resolve(completed("resp_healthy")));
      },
    });

    for (const stream_id of ["oversized-lane", "healthy-lane"]) {
      session.receive(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        input: stream_id,
      }));
    }

    await waitFor(() => socket.sent.some(value =>
      JSON.parse(value).stream_id === "healthy-lane" &&
      JSON.parse(value).type === "response.completed"));
    await waitFor(() => socket.sent.some(value =>
      JSON.parse(value).error?.code === "websocket_output_queue_full"));
    expect(socket.closeCode).toBeNull();
    expect(signals[0]!.aborted).toBe(true);
    expect(signals[1]!.aborted).toBe(false);
    expect(socket.sent.map(value => JSON.parse(value))).toEqual(expect.arrayContaining([
      expect.objectContaining({
        status: 503,
        stream_id: "oversized-lane",
        error: expect.objectContaining({
          type: "overloaded_error",
          code: "websocket_output_queue_full",
        }),
      }),
      expect.objectContaining({
        type: "response.completed",
        stream_id: "healthy-lane",
      }),
    ]));
    session.close();
  });

  it("waits for socket capacity before consuming the next response event", async () => {
    const socket = new FakeSocket();
    socket.bufferedAmount = 1_024;
    const telemetry = new WebSocketTelemetry();
    const session = createTestSession(socket as unknown as WebSocket, {
      telemetry,
      maxBufferedBytes: 128,
      sendTimeoutMs: 1_000,
      startRelay: async () => relayHandle(Promise.resolve(completed("resp_slow")), [{
        event: "response.created",
        data: JSON.stringify({ type: "response.created", response: { id: "resp_slow" } }),
      }]),
    });

    session.receive(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "slow socket",
    }));
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(socket.sent).toHaveLength(0);

    socket.bufferedAmount = 0;
    await waitFor(() => socket.sent.length === 2);
    expect(socket.sent.map(value => JSON.parse(value).type))
      .toEqual(["response.created", "response.completed"]);
    expect(telemetry.snapshotAndReset().delayedByBackpressure).toBe(1);
    session.close();
  });

  it("isolates bounded send-queue overflow to the offending named stream", async () => {
    const socket = new FakeSocket();
    socket.holdCallbacks = true;
    const signals: AbortSignal[] = [];
    const telemetry = new WebSocketTelemetry();
    const session = createTestSession(socket as unknown as WebSocket, {
      telemetry,
      maxActive: 3,
      maxSendQueueItems: 2,
      maxSendQueueBytes: 1024 * 1024,
      sendTimeoutMs: 10_000,
      startRelay: async (_body, options) => {
        signals.push(options.signal);
        return relayHandle(new Promise(() => {}), [{
          event: "response.created",
          data: JSON.stringify({ type: "response.created", response: { id: "pending" } }),
        }]);
      },
    });

    for (const stream_id of ["a", "b", "c"]) {
      session.receive(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        input: stream_id,
      }));
    }
    await waitFor(() => signals.length === 3 && signals.some(signal => signal.aborted));
    expect(socket.closeCode).toBeNull();
    expect(signals).toHaveLength(3);
    expect(signals[0]!.aborted).toBe(false);
    expect(signals[1]!.aborted).toBe(false);
    expect(signals[2]!.aborted).toBe(true);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      rejectedSends: 1,
      deliveryErrors: 0,
      sendQueueDepthPeak: 2,
    });
    socket.holdCallbacks = false;
    socket.flush();
    await waitFor(() => socket.sent.some(value =>
      JSON.parse(value).error?.code === "websocket_output_queue_full"));
    expect(JSON.parse(socket.sent.at(-1)!)).toMatchObject({
      type: "error",
      status: 503,
      stream_id: "c",
      error: {
        type: "overloaded_error",
        code: "websocket_output_queue_full",
      },
    });
    session.close();
  });

  it("aborts relay work when the socket send callback fails", async () => {
    const socket = new FakeSocket();
    socket.sendError = new Error("socket write failed");
    let signal: AbortSignal | undefined;
    const telemetry = new WebSocketTelemetry();
    const session = createTestSession(socket as unknown as WebSocket, {
      telemetry,
      startRelay: async (_body, options) => {
        signal = options.signal;
        return relayHandle(new Promise(() => {}), [{
          event: "response.created",
          data: JSON.stringify({ type: "response.created", response: { id: "failing" } }),
        }]);
      },
    });

    session.receive(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "fail send",
    }));
    await waitFor(() => socket.closeCode !== null);
    expect(socket.closeCode).toBe(1011);
    expect(signal?.aborted).toBe(true);
    expect(telemetry.snapshotAndReset().deliveryErrors).toBe(1);
  });
});
