import type { Server } from "node:http";
import { WebSocketServer, type RawData } from "ws";
import {
  ResponsesWebSocketSession,
  WebSocketProcessBudget,
  WebSocketSessionCapacityError,
  type ResponsesWebSocketSessionOptions,
  type WebSocketProcessLease,
  type WebSocketRelayStarter,
} from "./session";
import type { ResponseGraph } from "./response-graph";

export type { WebSocketRelayStarter } from "./session";

export const DEFAULT_WS_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024;
export const MAX_WS_PAYLOAD_BYTES = 64 * 1024 * 1024;
export const WS_INGRESS_WORKING_SET_FACTOR = 4;
export const WS_INGRESS_FIXED_OVERHEAD_BYTES = 1024 * 1024;
export const DEFAULT_WS_PROCESS_MAX_SESSIONS = 256;
export const DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES = 512 * 1024 * 1024;
export const DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES = 512 * 1024 * 1024;
export const DEFAULT_WS_HEARTBEAT_INTERVAL_MS = 30_000;
export const DEFAULT_WS_PONG_TIMEOUT_MS = 15_000;
export const DEFAULT_WS_IDLE_TIMEOUT_MS = 5 * 60_000;

const DEFAULT_PROCESS_BUDGET = new WebSocketProcessBudget({
  maxSessions: DEFAULT_WS_PROCESS_MAX_SESSIONS,
  maxQueuedBytes: DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES,
  maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
});

/** Raw frame + UTF-16 decode + parsed JSON/object working-set coverage. */
export function webSocketIngressReservationBytes(maxPayloadBytes: number): number {
  if (
    !Number.isSafeInteger(maxPayloadBytes) ||
    maxPayloadBytes < 1 ||
    maxPayloadBytes > MAX_WS_PAYLOAD_BYTES
  ) throw new RangeError(`maxPayloadBytes must be between 1 and ${MAX_WS_PAYLOAD_BYTES}`);
  const value = maxPayloadBytes * WS_INGRESS_WORKING_SET_FACTOR +
    WS_INGRESS_FIXED_OVERHEAD_BYTES;
  if (!Number.isSafeInteger(value)) {
    throw new RangeError("WebSocket ingress reservation exceeds safe integer capacity");
  }
  return value;
}

export function decodeWebSocketRawData(data: RawData): {
  bytes: number;
  text: string;
} {
  if (Array.isArray(data)) {
    const bytes = data.reduce((total, part) => total + part.byteLength, 0);
    return { bytes, text: Buffer.concat(data, bytes).toString("utf8") };
  }
  if (data instanceof ArrayBuffer) {
    return {
      bytes: data.byteLength,
      text: Buffer.from(data).toString("utf8"),
    };
  }
  return {
    bytes: data.byteLength,
    text: data.toString("utf8"),
  };
}

export interface AttachResponsesWebSocketOptions extends Omit<
  ResponsesWebSocketSessionOptions,
  "startRelay" | "graph" | "clientId" | "processLease" | "parserReservationBytes" | "onActivity"
> {
  startRelay: WebSocketRelayStarter;
  /** Hard parser-level limit enforced before a complete message reaches JS. */
  maxPayloadBytes?: number;
  /** Test/integration hook; production defaults to a fresh ID per connection. */
  clientIdFactory?: () => string;
  /** Test/composition hook; production creates one isolated graph per socket. */
  graphFactory?: () => ResponseGraph;
  heartbeatIntervalMs?: number;
  pongTimeoutMs?: number;
  idleTimeoutMs?: number;
}

const attachedServers = new WeakMap<Server, readonly WebSocketServer[]>();

export function attachResponsesWebSocket(
  server: Server,
  options: AttachResponsesWebSocketOptions,
): WebSocketServer {
  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_WS_MAX_PAYLOAD_BYTES;
  const ingressReservationBytes = webSocketIngressReservationBytes(maxPayloadBytes);
  const jumboIngressReservationBytes = webSocketIngressReservationBytes(MAX_WS_PAYLOAD_BYTES);
  const processBudget = options.processBudget ?? DEFAULT_PROCESS_BUDGET;
  const heartbeatIntervalMs = positiveTimer(
    options.heartbeatIntervalMs ?? DEFAULT_WS_HEARTBEAT_INTERVAL_MS,
    "heartbeatIntervalMs",
  );
  const pongTimeoutMs = positiveTimer(
    options.pongTimeoutMs ?? DEFAULT_WS_PONG_TIMEOUT_MS,
    "pongTimeoutMs",
  );
  const idleTimeoutMs = positiveTimer(
    options.idleTimeoutMs ?? DEFAULT_WS_IDLE_TIMEOUT_MS,
    "idleTimeoutMs",
  );
  const wss = new WebSocketServer({ noServer: true, maxPayload: maxPayloadBytes });
  const jumboWss = new WebSocketServer({ noServer: true, maxPayload: MAX_WS_PAYLOAD_BYTES });
  const pendingLeases = new WeakMap<object, WebSocketProcessLease>();
  const acceptConnection = (socket: import("ws").WebSocket, parserLimitBytes: number) => {
    const processLease = pendingLeases.get(socket);
    pendingLeases.delete(socket);
    if (!processLease) {
      socket.close(1013, "GhostProv WebSocket ingress capacity unavailable");
      return;
    }
    let lastApplicationActivityAt = Date.now();
    let session: ResponsesWebSocketSession;
    try {
      session = new ResponsesWebSocketSession(socket, {
        ...options,
        processBudget,
        processLease,
        graph: options.graphFactory?.(),
        onActivity: () => { lastApplicationActivityAt = Date.now(); },
        clientId: options.clientIdFactory?.() ?? `ws-${crypto.randomUUID()}`,
      });
    } catch (error) {
      processLease.close();
      socket.close(
        error instanceof WebSocketSessionCapacityError ? 1013 : 1011,
        error instanceof WebSocketSessionCapacityError
          ? "GhostProv WebSocket capacity exhausted"
          : "GhostProv WebSocket session initialization failed",
      );
      return;
    }
    let cleaned = false;
    let pingSentAt: number | null = null;
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      clearInterval(heartbeat);
      session.close();
    };
    const terminate = () => {
      cleanup();
      try { socket.terminate(); } catch {}
    };
    const heartbeat = setInterval(() => {
      if (cleaned) return;
      const now = Date.now();
      if (session.isIdle() && now - lastApplicationActivityAt >= idleTimeoutMs) {
        terminate();
        return;
      }
      if (pingSentAt !== null) {
        if (now - pingSentAt >= pongTimeoutMs) terminate();
        return;
      }
      try {
        socket.ping();
        pingSentAt = now;
      } catch {
        terminate();
      }
    }, heartbeatIntervalMs);
    heartbeat.unref?.();
    socket.on("pong", () => { pingSentAt = null; });
    socket.on("message", data => {
      lastApplicationActivityAt = Date.now();
      pingSentAt = null;
      const bytes = Array.isArray(data)
        ? data.reduce((total, part) => total + part.byteLength, 0)
        : data.byteLength;
      // `ws` enforces maxPayload while parsing on Node. Bun's compatibility
      // layer has historically ignored that option, so retain this secondary
      // cross-runtime check before decoding the message as text.
      if (bytes > parserLimitBytes) {
        cleanup();
        socket.close(1009, "WebSocket message exceeds configured limit");
        return;
      }
      const decoded = decodeWebSocketRawData(data);
      session.receive(decoded.text, decoded.bytes);
    });
    socket.once("close", cleanup);
    socket.once("error", cleanup);
  };
  wss.on("connection", socket => acceptConnection(socket, maxPayloadBytes));
  jumboWss.on("connection", socket => acceptConnection(socket, MAX_WS_PAYLOAD_BYTES));
  server.on("upgrade", (request, socket, head) => {
    try {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
      if (url.pathname !== "/v1/responses") {
        socket.destroy();
        return;
      }
      const jumboHeader = request.headers["x-ghostprov-ws-jumbo"];
      const jumbo = jumboHeader === "1" || (
        Array.isArray(jumboHeader) && jumboHeader.length === 1 && jumboHeader[0] === "1"
      );
      const selectedWss = jumbo ? jumboWss : wss;
      const selectedIngressBytes = jumbo
        ? jumboIngressReservationBytes
        : ingressReservationBytes;
      const processLease = processBudget.openSession(selectedIngressBytes);
      if (!processLease) {
        socket.end(
          "HTTP/1.1 503 Service Unavailable\r\n" +
          "Connection: close\r\n" +
          "Content-Length: 0\r\n\r\n",
        );
        return;
      }
      let handedOff = false;
      const releasePending = () => {
        if (!handedOff) processLease.close();
      };
      socket.once("close", releasePending);
      try {
        selectedWss.handleUpgrade(request, socket, head, websocket => {
          handedOff = true;
          socket.off("close", releasePending);
          pendingLeases.set(websocket, processLease);
          selectedWss.emit("connection", websocket, request);
        });
      } catch (error) {
        socket.off("close", releasePending);
        processLease.close();
        throw error;
      }
    } catch {
      socket.destroy();
    }
  });
  attachedServers.set(server, [wss, jumboWss]);
  return wss;
}

function positiveTimer(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) {
    throw new RangeError(`${label} must be a positive schedulable integer`);
  }
  return value;
}

/** Closes only WebSockets owned by this GhostProv server after active relays drain. */
export async function closeResponsesWebSockets(
  server: Server,
  options: { timeoutMs?: number; code?: number; reason?: string } = {},
): Promise<void> {
  const servers = attachedServers.get(server);
  if (!servers) return;
  attachedServers.delete(server);
  const timeoutMs = options.timeoutMs ?? 5_000;
  const code = options.code ?? 1012;
  const reason = (options.reason ?? "GhostProv service restart").slice(0, 120);
  await new Promise<void>(resolve => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      for (const wss of servers) {
        for (const socket of wss.clients) socket.terminate();
      }
      finish();
    }, timeoutMs);
    timer.unref?.();
    let remaining = servers.length;
    for (const wss of servers) {
      for (const socket of wss.clients) socket.close(code, reason);
      wss.close(() => {
        remaining--;
        if (remaining === 0) finish();
      });
    }
  });
}
