import type { Server } from "node:http";
import { WebSocketServer, type RawData } from "ws";
import {
  ResponsesWebSocketSession,
  WebSocketProcessBudget,
  WebSocketSessionCapacityError,
  type ResponsesWebSocketSessionOptions,
  type WebSocketRelayStarter,
} from "./session";

export type { WebSocketRelayStarter } from "./session";

export const DEFAULT_WS_MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;
export const DEFAULT_WS_PROCESS_MAX_SESSIONS = 256;
export const DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES = 512 * 1024 * 1024;

const DEFAULT_PROCESS_BUDGET = new WebSocketProcessBudget({
  maxSessions: DEFAULT_WS_PROCESS_MAX_SESSIONS,
  maxQueuedBytes: DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES,
});

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
  "startRelay" | "graph" | "clientId"
> {
  startRelay: WebSocketRelayStarter;
  /** Hard parser-level limit enforced before a complete message reaches JS. */
  maxPayloadBytes?: number;
  /** Test/integration hook; production defaults to a fresh ID per connection. */
  clientIdFactory?: () => string;
}

const attachedServers = new WeakMap<Server, WebSocketServer>();

export function attachResponsesWebSocket(
  server: Server,
  options: AttachResponsesWebSocketOptions,
): WebSocketServer {
  const maxPayloadBytes = options.maxPayloadBytes ?? DEFAULT_WS_MAX_PAYLOAD_BYTES;
  if (!Number.isSafeInteger(maxPayloadBytes) || maxPayloadBytes < 1) {
    throw new RangeError("maxPayloadBytes must be a positive safe integer");
  }
  const wss = new WebSocketServer({ noServer: true, maxPayload: maxPayloadBytes });
  wss.on("connection", socket => {
    let session: ResponsesWebSocketSession;
    try {
      session = new ResponsesWebSocketSession(socket, {
        ...options,
        processBudget: options.processBudget ?? DEFAULT_PROCESS_BUDGET,
        clientId: options.clientIdFactory?.() ?? `ws-${crypto.randomUUID()}`,
      });
    } catch (error) {
      socket.close(
        error instanceof WebSocketSessionCapacityError ? 1013 : 1011,
        error instanceof WebSocketSessionCapacityError
          ? "GhostProv WebSocket capacity exhausted"
          : "GhostProv WebSocket session initialization failed",
      );
      return;
    }
    socket.on("message", data => {
      const bytes = Array.isArray(data)
        ? data.reduce((total, part) => total + part.byteLength, 0)
        : data.byteLength;
      // `ws` enforces maxPayload while parsing on Node. Bun's compatibility
      // layer has historically ignored that option, so retain this secondary
      // cross-runtime check before decoding the message as text.
      if (bytes > maxPayloadBytes) {
        socket.close(1009, "WebSocket message exceeds configured limit");
        return;
      }
      const decoded = decodeWebSocketRawData(data);
      session.receive(decoded.text, decoded.bytes);
    });
    socket.once("close", () => session.close());
    socket.once("error", () => session.close());
  });
  server.on("upgrade", (request, socket, head) => {
    try {
      const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
      if (url.pathname !== "/v1/responses") {
        socket.destroy();
        return;
      }
      wss.handleUpgrade(request, socket, head, websocket => wss.emit("connection", websocket, request));
    } catch {
      socket.destroy();
    }
  });
  attachedServers.set(server, wss);
  return wss;
}

/** Closes only WebSockets owned by this GhostProv server after active relays drain. */
export async function closeResponsesWebSockets(
  server: Server,
  options: { timeoutMs?: number; code?: number; reason?: string } = {},
): Promise<void> {
  const wss = attachedServers.get(server);
  if (!wss) return;
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
      for (const socket of wss.clients) socket.terminate();
      finish();
    }, timeoutMs);
    timer.unref?.();
    for (const socket of wss.clients) socket.close(code, reason);
    wss.close(() => finish());
  });
}
