import { createServer } from "node:http";
import { WebSocketServer } from "ws";

const port = Number(process.env.PORT);
const generation = Number(process.env.GP_SUPERVISOR_GENERATION ?? 0);
if (!Number.isSafeInteger(port) || port < 1) throw new Error("fake worker PORT is invalid");

const server = createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://127.0.0.1:${port}`);
  if (url.pathname === "/health") {
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ ok: true, generation }));
    return;
  }
  if (url.pathname === "/slow") {
    const delay = Math.min(1_000, Math.max(0, Number(url.searchParams.get("ms")) || 0));
    setTimeout(() => {
      response.setHeader("content-type", "application/json");
      response.end(JSON.stringify({ generation, delayed: true }));
    }, delay);
    return;
  }
  if (url.pathname === "/crash") {
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify({ generation, crashing: true }), () => {
      setTimeout(() => process.exit(2), 5);
    });
    return;
  }
  response.setHeader("content-type", "application/json");
  response.end(JSON.stringify({ generation }));
});
const wss = new WebSocketServer({ noServer: true });
wss.on("connection", socket => {
  socket.send(JSON.stringify({ generation }));
  socket.on("message", data => socket.send(data.toString()));
});
server.on("upgrade", (request, socket, head) => {
  if (new URL(request.url ?? "/", `http://127.0.0.1:${port}`).pathname !== "/v1/responses") {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(request, socket, head, websocket => wss.emit("connection", websocket, request));
});

let closing = false;
let intentionalDisconnect = false;
function close(exitCode: number): void {
  if (closing) return;
  closing = true;
  const finish = () => server.close(() => {
      process.send?.({ type: "ghostprov.stopped", pid: process.pid, exitCode });
      if (process.connected) {
        intentionalDisconnect = true;
        process.disconnect?.();
      }
      process.exitCode = exitCode;
    });
  for (const socket of wss.clients) socket.close(1012, "restart");
  wss.close(() => finish());
}

process.on("message", message => {
  if (!message || typeof message !== "object") return;
  const value = message as { type?: unknown; exitCode?: unknown };
  if (value.type === "ghostprov.shutdown") {
    close(Number.isSafeInteger(value.exitCode) ? Number(value.exitCode) : 0);
  }
});
process.once("SIGTERM", () => close(0));
process.once("SIGINT", () => close(0));
process.once("disconnect", () => { if (!intentionalDisconnect) close(1); });

server.listen(port, "127.0.0.1", () => {
  process.send?.({ type: "ghostprov.ready", pid: process.pid, port });
});
