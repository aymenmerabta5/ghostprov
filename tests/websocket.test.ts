import { afterEach, describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:http";
import { WebSocket } from "ws";
import {
  attachResponsesWebSocket,
  DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES,
  DEFAULT_WS_MAX_PAYLOAD_BYTES,
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
} from "../src/ws/session";
import { WebSocketTelemetry } from "../src/ws/telemetry";
import { ResponseGraph, SharedByteBudget } from "../src/ws/response-graph";
import { ApiError } from "../src/http/errors";
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

async function startServer(
  startRelay: WebSocketRelayStarter,
  options: Omit<AttachResponsesWebSocketOptions, "startRelay"> = {},
  clientHeaders?: Record<string, string>,
) {
  const server = createServer((_req, res) => { res.writeHead(404).end(); });
  attachResponsesWebSocket(server, { startRelay, ...options });
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
  it("requires the configured bearer key before allocating an upgrade", async () => {
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    const budget = new WebSocketProcessBudget({
      maxSessions: 8,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: 64 * 1024 * 1024,
    });
    attachResponsesWebSocket(server, {
      apiKey: "GHOST_IS_KING",
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
      headers: { authorization: "Bearer GHOST_IS_KING" },
    });
    sockets.push(accepted);
    await new Promise<void>((resolve, reject) => {
      accepted.once("open", resolve);
      accepted.once("error", reject);
    });
    expect(budget.snapshot().sessions).toBe(1);
  });

  it("bounds thirty default parser reservations and one explicit jumbo mix", () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const jumboBytes = webSocketIngressReservationBytes(MAX_WS_PAYLOAD_BYTES);
    expect(normalBytes).toBe(17 * 1024 * 1024);
    expect(jumboBytes).toBe(257 * 1024 * 1024);

    const budget = new WebSocketProcessBudget({
      maxSessions: 256,
      maxQueuedBytes: 512 * 1024 * 1024,
      maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
    });
    const defaults = Array.from({ length: 30 }, () => budget.openSession(normalBytes));
    expect(defaults.every(Boolean)).toBe(true);
    expect(budget.openSession(normalBytes)).toBeNull();
    expect(budget.snapshot()).toMatchObject({
      sessions: 30,
      ingressReservedBytes: 510 * 1024 * 1024,
      queuedBytes: 0,
      maxIngressBytes: 512 * 1024 * 1024,
    });
    for (const lease of defaults) lease!.close();

    const jumbo = budget.openSession(jumboBytes);
    expect(jumbo).not.toBeNull();
    const alongsideJumbo = Array.from({ length: 15 }, () => budget.openSession(normalBytes));
    expect(alongsideJumbo.every(Boolean)).toBe(true);
    expect(budget.openSession(normalBytes)).toBeNull();
    expect(budget.snapshot()).toMatchObject({
      sessions: 16,
      ingressReservedBytes: 512 * 1024 * 1024,
    });
    jumbo!.close();
    for (const lease of alongsideJumbo) lease!.close();
    expect(budget.snapshot()).toMatchObject({
      sessions: 0,
      ingressReservedBytes: 0,
      queuedBytes: 0,
    });
  });

  it("validates adaptive parser and fast-path limits before installing an upgrade", () => {
    const startRelay = async () => relayHandle(Promise.resolve(completed("unused")));
    for (const maxPayloadBytes of [0, Number.NaN, MAX_WS_PAYLOAD_BYTES + 1]) {
      const server = createServer();
      expect(() => attachResponsesWebSocket(server, {
        maxPayloadBytes,
        startRelay,
      })).toThrow(RangeError);
    }
    for (const fastPathPayloadBytes of [0, DEFAULT_WS_MAX_PAYLOAD_BYTES + 1]) {
      const server = createServer();
      expect(() => attachResponsesWebSocket(server, {
        maxPayloadBytes: DEFAULT_WS_MAX_PAYLOAD_BYTES,
        fastPathPayloadBytes,
        startRelay,
      })).toThrow(RangeError);
    }
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
    let normalCalls = 0;
    const normal = await startServer(async () => {
      normalCalls++;
      return relayHandle(Promise.resolve(completed("unexpected")));
    });
    const normalClosed = new Promise<number>(resolve => normal.once("close", resolve));
    normal.send(JSON.stringify({ type: "response.create", input }));
    // Bun's `ws` compatibility layer may surface a parser-sized close as
    // abnormal (1006) for a multi-MiB frame even though smaller overflows
    // preserve the protocol's 1009 close code.
    expect([1006, 1009]).toContain(await normalClosed);
    expect(normalCalls).toBe(0);

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

  it("holds a large decoded message until shared ingress memory becomes available", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 64 * MiB,
      maxIngressBytes: 70 * MiB,
    });
    const blocker = budget.openSession(17 * MiB)!;
    expect(blocker.tryEnsureIngress(36 * MiB)).toBe(true);
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed("resp_waited_jumbo")));
    }, { processBudget: budget, telemetry });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "waited",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(5 * MiB),
    }));
    await waitFor(() => budget.snapshot().ingressWaiters === 1);
    expect(calls).toBe(0);
    blocker.releaseIngress();
    await waitFor(() => messages.some(message => message.type === "response.completed"), 5_000);
    expect(calls).toBe(1);
    expect(budget.snapshot().ingressWaiters).toBe(0);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressWaits: 1,
      ingressRejections: 0,
    });
    blocker.close();
  }, 15_000);

  it("closes only a large message connection after bounded ingress wait timeout", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 64 * MiB,
      maxIngressBytes: 70 * MiB,
    });
    const blocker = budget.openSession(17 * MiB)!;
    expect(blocker.tryEnsureIngress(36 * MiB)).toBe(true);
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed("unexpected")));
    }, {
      processBudget: budget,
      telemetry,
      ingressWaitTimeoutMs: 20,
    });
    const closed = new Promise<number>(resolve => socket.once("close", resolve));
    socket.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(5 * MiB),
    }));
    expect(await closed).toBe(1013);
    expect(calls).toBe(0);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressWaits: 1,
      ingressRejections: 1,
      ingressTimeouts: 1,
    });
    await waitFor(() => budget.snapshot().sessions === 1);
    expect(budget.snapshot()).toMatchObject({
      sessions: 1,
      ingressReservedBytes: 53 * MiB,
      ingressWaiters: 0,
    });
    blocker.close();
  }, 15_000);

  it("cancels a waiting ingress lease immediately when its connection terminates", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 2,
      maxQueuedBytes: 64 * MiB,
      maxIngressBytes: 70 * MiB,
    });
    const blocker = budget.openSession(17 * MiB)!;
    expect(blocker.tryEnsureIngress(36 * MiB)).toBe(true);
    let calls = 0;
    const socket = await startServer(async () => {
      calls++;
      return relayHandle(Promise.resolve(completed("unexpected")));
    }, {
      processBudget: budget,
      ingressWaitTimeoutMs: 10_000,
    });
    socket.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "x".repeat(5 * MiB),
    }));
    await waitFor(() => budget.snapshot().ingressWaiters === 1);
    socket.terminate();
    await waitFor(() => budget.snapshot().sessions === 1);
    expect(calls).toBe(0);
    expect(budget.snapshot()).toMatchObject({
      sessions: 1,
      ingressReservedBytes: 53 * MiB,
      ingressWaiters: 0,
    });
    blocker.close();
    expect(budget.snapshot()).toMatchObject({
      sessions: 0,
      ingressReservedBytes: 0,
      ingressWaiters: 0,
    });
  }, 15_000);

  it("keeps nineteen normal agents healthy while one agent sends a large image-sized turn", async () => {
    const MiB = 1024 * 1024;
    const budget = new WebSocketProcessBudget({
      maxSessions: 32,
      maxQueuedBytes: 512 * MiB,
      maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
    });
    const telemetry = new WebSocketTelemetry();
    let calls = 0;
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachResponsesWebSocket(server, {
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
      { length: 20 },
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
      input: index === 0 ? "x".repeat(5 * MiB) : `small-${index}`,
    })));
    await waitFor(
      () => outputs.every(messages => messages.some(message => message.type === "response.completed")),
      10_000,
    );
    expect(calls).toBe(20);
    expect(clients.every(socket => socket.readyState === WebSocket.OPEN)).toBe(true);
    expect(budget.snapshot()).toMatchObject({
      sessions: 20,
      ingressWaiters: 0,
      queuedBytes: 0,
    });
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressWaits: 0,
      ingressRejections: 0,
    });
  }, 20_000);

  it("rejects an upgrade before parser construction when ingress is exhausted", async () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const budget = new WebSocketProcessBudget({
      maxSessions: 8,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: normalBytes,
    });
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachResponsesWebSocket(server, {
      processBudget: budget,
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
    expect(budget.snapshot()).toMatchObject({
      sessions: 1,
      ingressReservedBytes: normalBytes,
    });
    first.close();
    await waitFor(() => budget.snapshot().sessions === 0);
    expect(budget.snapshot().ingressReservedBytes).toBe(0);
  });

  it("reaps thirty dead-idle peers and recovers every ingress reservation", async () => {
    const normalBytes = webSocketIngressReservationBytes(DEFAULT_WS_MAX_PAYLOAD_BYTES);
    const budget = new WebSocketProcessBudget({
      maxSessions: 30,
      maxQueuedBytes: 1024 * 1024,
      maxIngressBytes: normalBytes * 30,
    });
    const server = createServer((_req, res) => { res.writeHead(404).end(); });
    attachResponsesWebSocket(server, {
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

    const dead = Array.from({ length: 30 }, () => new WebSocket(url));
    sockets.push(...dead);
    await Promise.all(dead.map(socket => new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    })));
    expect(budget.snapshot()).toMatchObject({
      sessions: 30,
      ingressReservedBytes: normalBytes * 30,
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
    attachResponsesWebSocket(server, {
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
      store: false,
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
    expect(bodies[0]).toMatchObject({ stream: true, input: [{ role: "user" }] });

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
      const label = (body.input as any[]).at(-1).content[0].text as string;
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

  it("reconstructs a Codex store:false tool continuation from previous_response_id", async () => {
    const bodies: Record<string, unknown>[] = [];
    const call = { type: "function_call", id: "fc_1", call_id: "call_1", name: "tool", arguments: "{}" };
    const startRelay: WebSocketRelayStarter = async body => {
      bodies.push(body);
      return relayHandle(Promise.resolve(
        bodies.length === 1 ? completed("resp_parent", [call]) : completed("resp_child"),
      ));
    };
    const socket = await startServer(startRelay);
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", stream_id: "main", model: "gpt-5.6-sol", store: false, input: "use tool" }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_parent"));
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "main",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_parent",
      input: [{ type: "function_call_output", call_id: "call_1", output: "ok" }],
    }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_child"));
    expect(messages.find(message => message.response?.id === "resp_child").response.previous_response_id).toBe("resp_parent");
    expect(bodies[1]).not.toHaveProperty("previous_response_id");
    expect(JSON.stringify(bodies[1].input)).toContain("function_call");
    expect(JSON.stringify(bodies[1].input)).toContain("function_call_output");
  });

  it("preserves one exact large image across a small store:false continuation", async () => {
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
    const materialized = JSON.stringify(bodies[1]?.input);
    expect(materialized.split(imageUrl).length - 1).toBe(1);
    expect(materialized).toContain("now inspect the same image again");
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 1,
      ingressRejections: 0,
    });
  }, 15_000);

  it("holds lineage capacity only until router handoff completes", async () => {
    const shared = new SharedByteBudget(16 * 1024 * 1024);
    const graph = new ResponseGraph({
      maxBytes: 8 * 1024 * 1024,
      sharedBudget: shared,
    });
    const root = graph.resolve({
      type: "response.create",
      store: false,
      input: "root",
    });
    graph.record(root, {
      id: "resp_lineage_root",
      status: "completed",
      output: [{ type: "message", content: "x".repeat(1024 * 1024) }],
    });
    const storedBytes = shared.used;
    const handoff = deferred<RunHandle>();
    let started = false;
    const socket = await startServer(async () => {
      started = true;
      return handoff.promise;
    }, { graphFactory: () => graph });
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "lineage",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_lineage_root",
      input: "next",
    }));
    await waitFor(() => started);
    expect(shared.used).toBeGreaterThan(storedBytes);

    handoff.resolve(relayHandle(Promise.resolve({
      status: "completed",
      output: [],
    })));
    await waitFor(() => messages.some(message => message.type === "response.completed"));
    expect(shared.used).toBe(storedBytes);
    socket.close();
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

  it("returns direct validation errors without closing other lanes", async () => {
    const gates = new Map<string, ReturnType<typeof deferred<any>>>();
    const startRelay: WebSocketRelayStarter = async body => {
      const label = (body.input as any[]).at(-1)?.content?.[0]?.text ?? "unknown";
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

  it("uses one stable relay client identity per WebSocket session", async () => {
    const clientIds: string[] = [];
    const socket = await startServer(async (_body, options) => {
      clientIds.push(options.clientId);
      return relayHandle(Promise.resolve(completed(`resp_${clientIds.length}`)));
    });
    const messages = collect(socket);
    for (const stream_id of ["identity-a", "identity-b"]) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id,
        model: "gpt-5.6-sol",
        store: false,
        input: stream_id,
      }));
    }
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
    expect(clientIds).toHaveLength(2);
    expect(clientIds[0]).toMatch(/^ws-/);
    expect(clientIds[1]).toBe(clientIds[0]);
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
  it("holds transferred event memory until the socket send callback settles", async () => {
    const socket = new FakeSocket();
    socket.holdCallbacks = true;
    const permits = new AtomicBytePermitPool({ ordinaryBytes: 4_096, jumboBytes: 1 });
    const frame = attachAtomicBytePermit({
      event: "response.created",
      data: JSON.stringify({ type: "response.created", response: { id: "resp_owned" } }),
    }, (await permits.acquire(4_096)).transfer("websocket-test"));
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
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

  it("keeps graph storage decoupled from its caller's transport permit", async () => {
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
    });
    graph.record(resolved, final);
    expect(graph.stats().nodes).toBe(1);
    expect(permits.snapshot().ordinaryUsedBytes).toBe(8_192);
    graph.clear();
    // Graph storage is covered by its fixed governor region, not this permit.
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
    const first = new ResponsesWebSocketSession(
      new FakeSocket() as unknown as WebSocket,
      {
        processBudget: budget,
        startRelay: async () => relayHandle(new Promise(() => {})),
      },
    );
    expect(budget.snapshot().sessions).toBe(1);
    expect(() => new ResponsesWebSocketSession(
      new FakeSocket() as unknown as WebSocket,
      {
        processBudget: budget,
        startRelay: async () => relayHandle(new Promise(() => {})),
      },
    )).toThrow("session capacity");

    first.close();
    expect(budget.snapshot()).toMatchObject({ sessions: 0, queuedBytes: 0 });
    const replacement = new ResponsesWebSocketSession(
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
    const first = new ResponsesWebSocketSession(firstSocket as unknown as WebSocket, {
      processBudget: budget,
      startRelay: async () => relayHandle(firstGate.promise),
    });
    const second = new ResponsesWebSocketSession(secondSocket as unknown as WebSocket, {
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
    const session = new ResponsesWebSocketSession(
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
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
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
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
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
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
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
    const session = new ResponsesWebSocketSession(socket as unknown as WebSocket, {
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
