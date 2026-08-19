import { Hono } from "hono";
import { relayCreate, relayCreateStream } from "./relay/client";
import { uploadFileFromFormData, getFileContent, getFile, listFiles } from "./proxy/files";
import { WebSocketServer } from "ws";
import { createServer as createHttpServer } from "node:http";
import { queue } from "./utils/queue";
const app = new Hono();
app.get("/health", (c) => c.json({ ok: true }));

// Files + Prompt Caching Passthrough (Task 7)
// POST /v1/files -> POST /api/project-files/upload?prism_cache_bust=... (via src/proxy/files.ts)
// GET /v1/files/:id/content -> GET /s/sandboxes/proxy/zip?prism_cache_bust=... (via src/proxy/files.ts)
// prompt_cache_key / prompt_cache_options / prompt_cache_breakpoint forwarded verbatim (see relayCreate)
// tool_choice: allowed_tools forwarded verbatim (Global Constraints: forward verbatim)
// 15 rpm note: prompt_cache_key is rate-limited to 15 rpm per key; writes cost 1.25x, reads 0.1x
app.post("/v1/files", async (c) => {
  try {
    const formData = await c.req.formData();
    const file = formData.get("file");
    if (!file) return c.json({ error: "missing file" }, 400);
    const result = await uploadFileFromFormData(formData);
    return c.json(result);
  } catch (e: any) {
    return c.json({ error: e?.message ?? String(e) }, 500);
  }
});
app.get("/v1/files", async (c) => {
  try {
    const files = await listFiles();
    return c.json({ object: "list", data: files });
  } catch (e: any) {
    return c.json({ error: e?.message ?? String(e) }, 500);
  }
});
app.get("/v1/files/:id", async (c) => {
  try {
    const id = c.req.param("id");
    const file = await getFile(id);
    return c.json(file);
  } catch (e: any) {
    return c.json({ error: e?.message ?? String(e) }, 404);
  }
});
app.get("/v1/files/:id/content", async (c) => {
  try {
    const id = c.req.param("id");
    const data = await getFileContent(id);
    // Try to infer content-type from file metadata if available
    let contentType = "application/octet-stream";
    try {
      const meta: any = await getFile(id).catch(() => null);
      if (meta?.filename?.endsWith(".png")) contentType = "image/png";
      else if (meta?.filename?.endsWith(".jpg") || meta?.filename?.endsWith(".jpeg")) contentType = "image/jpeg";
      else if (meta?.filename?.endsWith(".json")) contentType = "application/json";
    } catch {}
    return new Response(data as any, { headers: { "content-type": contentType } });
  } catch (e: any) {
    return c.json({ error: e?.message ?? String(e) }, 404);
  }
});

app.post("/v1/responses", async (c) => {
  const body: any = await c.req.json();
  // Prompt caching passthrough: forward verbatim per Global Constraints
  // - prompt_cache_key: string (15 rpm limit, 1.25x write, 0.1x read)
  // - prompt_cache_options: {mode:"explicit", ttl:"30m"}
  // - prompt_cache_breakpoint: {mode:"explicit"}
  // Tool choice passthrough: allow "auto" | "required" | "none" | {type:"function", name} | {type:"allowed_tools", allowed_tools: [...]}
  // All fields are preserved via relayCreate's verbatim spread; no stripping.
  // Concurrency + Codex Integration (Task 8): queue with PQUEUE_CONCURRENCY=5
  // - concurrent POST /v1/responses with different prompt_cache_key are queued via p-queue (max 5 concurrent, queue render via renderQueue concurrency 1)
  // - cached_tokens / cache_write_tokens in usage.input_tokens_details are forwarded verbatim (handled by relayCreate returning usage)
  if (body.stream) {
    const sse = await queue.add(() => relayCreateStream(body)) as Uint8Array;
    const stream = new ReadableStream({
      async start(controller) {
        controller.enqueue(sse as any);
        controller.close();
      },
    });
    return new Response(stream, { headers: { "content-type": "text/event-stream" } });
  }
  const res: any = await queue.add(() => relayCreate(body));
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
          // Concurrency: WSS also respects queue (PQUEUE_CONCURRENCY=5) and renderQueue (1)
          const result: any = await queue.add(() => relayCreate(body));
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
