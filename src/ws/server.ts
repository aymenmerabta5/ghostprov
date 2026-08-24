import type { Server } from "node:http";
import { WebSocketServer } from "ws";
import {
  ResponsesWebSocketSession,
  type ResponsesWebSocketSessionOptions,
  type WebSocketRelayStarter,
} from "./session";

export type { WebSocketRelayStarter } from "./session";

export interface AttachResponsesWebSocketOptions extends Omit<ResponsesWebSocketSessionOptions, "startRelay" | "graph"> {
  startRelay: WebSocketRelayStarter;
}

const attachedServers = new WeakMap<Server, WebSocketServer>();

export function attachResponsesWebSocket(
  server: Server,
  options: AttachResponsesWebSocketOptions,
): WebSocketServer {
  const wss = new WebSocketServer({ noServer: true });
  wss.on("connection", socket => {
    const session = new ResponsesWebSocketSession(socket, options);
    socket.on("message", data => session.receive(data.toString()));
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
