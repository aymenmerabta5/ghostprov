import { Hono } from "hono";
import { relayCreate, relayCreateStream } from "./relay/client";
import { WebSocketServer } from "ws";
import { createServer as createHttpServer } from "node:http";
const app = new Hono();
app.get("/health", (c) => c.json({ ok: true }));
app.post("/v1/responses", async (c) => {
  const body = await c.req.json();
  if (body.stream) {
    const stream = new ReadableStream({
      async start(controller) {
        const sse = await relayCreateStream(body);
        controller.enqueue(sse);
        controller.close();
      },
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  const res = await relayCreate(body);
  return c.json(res);
});
export function attachWss(server: any) {
  const wss = new WebSocketServer({ noServer: true });
  wss.on("connection", (ws: any) => {
    let previous_response_id: string | null = null;
    ws.on("message", async (data: any) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "response.create") {
          const body: any = { ...msg };
          delete body.type;
          // previous_response_id chaining: keep one in-memory per connection (store:false compatible)
          // If client did not provide previous_response_id but we have cached, inject it
          if (!body.previous_response_id && previous_response_id) {
            body.previous_response_id = previous_response_id;
          }
          const result: any = await relayCreate(body);
          const newId: string =
            result?.id ?? result?.response?.id ?? `resp_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
          previous_response_id = newId;
          // Forward each response.create to relayCreate with previous_response_id if present
          // Respond with type containing "response.create" substring to satisfy plan's expect
          ws.send(
            JSON.stringify({
              type: "response.created",
              response: result,
              id: newId,
              previous_response_id,
            }),
          );
        } else {
          ws.send(JSON.stringify({ type: "error", error: `unknown type ${msg.type}` }));
        }
      } catch (e: any) {
        ws.send(JSON.stringify({ type: "error", error: e?.message ?? String(e) }));
      }
    });
  });
  server.on("upgrade", (req: any, socket: any, head: any) => {
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost:8787"}`);
      if (url.pathname === "/v1/responses") {
        wss.handleUpgrade(req, socket, head, (ws: any) => {
          wss.emit("connection", ws, req);
        });
      } else {
        socket.destroy();
      }
    } catch {
      socket.destroy();
    }
  });
  return wss;
}

export default app;
if (import.meta.main) {
  const port = Number(process.env.PORT || 8787);
  console.log(`ghostprov listening on http://localhost:${port}`);
  const httpServer = createHttpServer(async (req, res) => {
    const host = req.headers.host ?? `localhost:${port}`;
    const url = `http://${host}${req.url}`;
    const headers = new Headers();
    for (const [k, v] of Object.entries(req.headers)) {
      if (v) headers.set(k, Array.isArray(v) ? v.join(",") : String(v));
    }
    let body: Buffer | undefined;
    if (req.method !== "GET" && req.method !== "HEAD") {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      if (chunks.length) body = Buffer.concat(chunks);
    }
    const request = new Request(url, { method: req.method, headers, body });
    const response = await app.fetch(request);
    res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
    if (response.body) {
      const ab = await response.arrayBuffer();
      res.end(Buffer.from(ab));
    } else {
      res.end();
    }
  });
  attachWss(httpServer);
  httpServer.listen(port);
}
