import type { Server } from "node:http";
import { WebSocketServer, type RawData } from "ws";
import {
  ResponsesWebSocketSession,
  WebSocketProcessBudget,
  WebSocketIngressTimeoutError,
  WebSocketSessionCapacityError,
  type ResponsesWebSocketSessionOptions,
  type WebSocketProcessLease,
  type WebSocketRelayStarter,
} from "./session";
import type { ResponseGraph } from "./response-graph";
import { apiKeyAuthorized } from "../http/api-key";

export type { WebSocketRelayStarter } from "./session";

export const DEFAULT_WS_MAX_PAYLOAD_BYTES = 4 * 1024 * 1024;
export const DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;
/** Compatibility ceiling for Node/ws hosts; Bun 1.4 itself caps messages at 16 MiB. */
export const MAX_WS_PAYLOAD_BYTES = 64 * 1024 * 1024;
export const WS_INGRESS_WORKING_SET_FACTOR = 4;
export const WS_INGRESS_FIXED_OVERHEAD_BYTES = 1024 * 1024;
export const DEFAULT_WS_PROCESS_MAX_SESSIONS = 256;
export const DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES = 512 * 1024 * 1024;
export const DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES = 512 * 1024 * 1024;
export const DEFAULT_WS_HEARTBEAT_INTERVAL_MS = 30_000;
export const DEFAULT_WS_PONG_TIMEOUT_MS = 15_000;
export const DEFAULT_WS_IDLE_TIMEOUT_MS = 5 * 60_000;
export const DEFAULT_WS_INGRESS_WAIT_TIMEOUT_MS = 30_000;

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
  /** Shared OpenAI-compatible bearer credential for the upgrade request. */
  apiKey?: string;
  /** Hard parser-level limit enforced before a complete message reaches JS. */
  maxPayloadBytes?: number;
  /** Normal-path threshold before automatic per-message memory promotion. */
  fastPathPayloadBytes?: number;
  /** Test/integration hook; production defaults to a fresh ID per connection. */
  clientIdFactory?: () => string;
  /** Test/composition hook; production creates one isolated graph per socket. */
  graphFactory?: () => ResponseGraph;
  heartbeatIntervalMs?: number;
  pongTimeoutMs?: number;
  idleTimeoutMs?: number;
  ingressWaitTimeoutMs?: number;
}

const attachedServers = new WeakMap<Server, readonly WebSocketServer[]>();

export function attachResponsesWebSocket(
  server: Server,
  options: AttachResponsesWebSocketOptions,
): WebSocketServer {
  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_WS_AUTOMATIC_MAX_PAYLOAD_BYTES;
  if (
    !Number.isSafeInteger(maxPayloadBytes) ||
    maxPayloadBytes < 1 ||
    maxPayloadBytes > MAX_WS_PAYLOAD_BYTES
  ) throw new RangeError(`maxPayloadBytes must be between 1 and ${MAX_WS_PAYLOAD_BYTES}`);
  const fastPathPayloadBytes = options.fastPathPayloadBytes ??
    Math.min(DEFAULT_WS_MAX_PAYLOAD_BYTES, maxPayloadBytes);
  if (
    !Number.isSafeInteger(fastPathPayloadBytes) ||
    fastPathPayloadBytes < 1 ||
    fastPathPayloadBytes > maxPayloadBytes
  ) throw new RangeError("fastPathPayloadBytes must be within the normal parser limit");
  const ingressReservationBytes = webSocketIngressReservationBytes(fastPathPayloadBytes);
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
  const ingressWaitTimeoutMs = positiveTimer(
    options.ingressWaitTimeoutMs ?? DEFAULT_WS_INGRESS_WAIT_TIMEOUT_MS,
    "ingressWaitTimeoutMs",
  );
  const wss = new WebSocketServer({ noServer: true, maxPayload: maxPayloadBytes });
  const jumboWss = new WebSocketServer({ noServer: true, maxPayload: MAX_WS_PAYLOAD_BYTES });
  const pendingLeases = new WeakMap<object, WebSocketProcessLease>();
  const acceptConnection = (
    socket: import("ws").WebSocket,
    parserLimitBytes: number,
    baseReservationBytes: number,
    automaticPromotion: boolean,
  ) => {
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
    let ingressWaiting = false;
    const ingressAbort = new AbortController();
    const cleanup = () => {
      if (cleaned) return;
      cleaned = true;
      ingressAbort.abort(new DOMException("WebSocket ingress closed", "AbortError"));
      processLease.releaseIngress();
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
    const pauseIngress = () => {
      try { (socket as import("ws").WebSocket & { pause?(): void }).pause?.(); } catch {}
    };
    const resumeIngress = () => {
      try { (socket as import("ws").WebSocket & { resume?(): void }).resume?.(); } catch {}
    };
    const consumeMessage = (data: RawData, releaseIngress: boolean) => {
      try {
        const decoded = decodeWebSocketRawData(data);
        session.receive(decoded.text, decoded.bytes);
      } finally {
        if (releaseIngress) processLease.releaseIngress();
      }
    };
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
      options.telemetry?.recordIngressMessageBytes(bytes);
      if (!automaticPromotion || bytes <= fastPathPayloadBytes) {
        consumeMessage(data, false);
        return;
      }
      const totalWorkingSet = bytes > 0
        ? webSocketIngressReservationBytes(bytes)
        : baseReservationBytes;
      const dynamicIngressBytes = Math.max(0, totalWorkingSet - baseReservationBytes);
      options.telemetry?.ingressPromoted();
      if (
        dynamicIngressBytes === 0 ||
        processLease.tryEnsureIngress(dynamicIngressBytes)
      ) {
        consumeMessage(data, true);
        return;
      }
      if (ingressWaiting) {
        options.telemetry?.ingressRejected(false);
        ingressWaiting = false;
        resumeIngress();
        cleanup();
        socket.close(1013, "GhostProv WebSocket ingress already has a waiting message");
        return;
      }
      ingressWaiting = true;
      options.telemetry?.ingressWaited();
      pauseIngress();
      void processLease.ensureIngress(
        dynamicIngressBytes,
        ingressAbort.signal,
        ingressWaitTimeoutMs,
      ).then(() => {
        if (cleaned) return;
        ingressWaiting = false;
        try {
          consumeMessage(data, true);
        } finally {
          resumeIngress();
        }
      }, error => {
        if (cleaned) return;
        ingressWaiting = false;
        const timedOut = error instanceof WebSocketIngressTimeoutError;
        options.telemetry?.ingressRejected(timedOut);
        resumeIngress();
        cleanup();
        socket.close(
          1013,
          timedOut
            ? "GhostProv WebSocket large-message ingress timed out"
            : "GhostProv WebSocket large-message ingress is unavailable",
        );
      });
    });
    socket.once("close", cleanup);
    socket.once("error", cleanup);
  };
  wss.on("connection", socket => acceptConnection(
    socket,
    maxPayloadBytes,
    ingressReservationBytes,
    true,
  ));
  jumboWss.on("connection", socket => acceptConnection(
    socket,
    MAX_WS_PAYLOAD_BYTES,
    jumboIngressReservationBytes,
    false,
  ));
  server.on("upgrade", (request, socket, head) => {
    try {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
      if (url.pathname !== "/v1/responses") {
        socket.destroy();
        return;
      }
      if (
        options.apiKey &&
        !apiKeyAuthorized({ authorization: request.headers.authorization }, options.apiKey, "openai")
      ) {
        const body = JSON.stringify({
          error: {
            message: "Invalid API key",
            type: "authentication_error",
            param: null,
            code: "invalid_api_key",
          },
        });
        socket.end(
          "HTTP/1.1 401 Unauthorized\r\n" +
          "Connection: close\r\n" +
          "Content-Type: application/json\r\n" +
          'WWW-Authenticate: Bearer realm="ghostprov"\r\n' +
          `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
        );
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
