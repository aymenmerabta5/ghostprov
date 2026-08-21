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
  return wss;
}
