/**
 * GhostProv — OpenAI-compatible mirror over the Prism sandbox relay.
 *
 * Routes:
 *   GET  /health                 → {ok:true}
 *   POST /v1/responses           → text/vision/audio/image-gen; stream:true → real SSE
 *   WS   /v1/responses           → session-style {type:"response.create"} over WebSocket
 *   POST /v1/files               → upload (vision/audio purpose) via Prism project-files
 *   GET  /v1/files/:id/content   → fetch uploaded bytes
 *   GET  /__bridge/token         → inspect minted tokens
 *
 * Everything is verbatim passthrough to https://proxy:18080/v1/responses via
 * the sandbox runner pipeline (see src/relay/pipeline.ts).
 */
import "dotenv/config";
import { Hono } from "hono";
import { createServer as createHttpServer } from "node:http";
import { WebSocketServer } from "ws";
import { config } from "./config";
import { queue, enqueue } from "./utils/queue";
import { runRelay, RelayError } from "./relay/pipeline";
import { sseFrame, SSE_HEADERS } from "./relay/sse";

const app = new Hono();

app.get("/health", (c) => c.json({ ok: true }));

app.get("/__bridge/token", async (c) => {
  // Resolution order: bridge memory → globalThis → .pw/tokens.json → env
  let mem: any = null;
  try {
    const bridge: any = await import("./bridge/playwright");
    mem = bridge.getBridgeTokens?.() ?? null;
  } catch {}
  const glob = (globalThis as any).__BRIDGE_TOKENS__ ?? null;
  let file: any = null;
  try {
    const fs = await import("node:fs");
    for (const f of ["D:/MyWork/AI/ghostprov/.pw/tokens.json", "/mnt/d/MyWork/AI/ghostprov/.pw/tokens.json"]) {
      if (fs.existsSync(f)) { file = JSON.parse(fs.readFileSync(f, "utf8")); break; }
    }
  } catch {}
  const pick = (k: "sandboxToken" | "ySweetToken"): { v: string | null; src: string } => {
    if (mem?.[k] && !String(mem[k]).includes("mock")) return { v: mem[k], src: "bridge" };
    if (glob?.[k] && !String(glob[k]).includes("mock")) return { v: glob[k], src: "global" };
    if (file?.[k] && !String(file[k]).includes("mock")) return { v: file[k], src: "file" };
    const envKey = k === "sandboxToken" ? "PRISM_SANDBOX_TOKEN" : "PRISM_Y_TOKEN";
    const env = (process.env[envKey] ?? "").trim();
    if (env.length > 5) return { v: env, src: "env" };
    return { v: k === "sandboxToken" ? "mock-token" : "mock_y_token", src: "mock" };
  };
  const sand = pick("sandboxToken");
  const y = pick("ySweetToken");
  const source = sand.src === y.src ? sand.src : `${sand.src}+${y.src}`;
  return c.json({
    ok: true,
    sandboxToken: sand.v,
    ySweetToken: y.v,
    updatedAt: mem?.updatedAt ?? file?.updatedAt ?? null,
    source: source === "mock+mock" ? "mock" : source,
  });
});

// ---------------------------------------------------------------------------
// POST /v1/responses — the core mirror
// ---------------------------------------------------------------------------
app.post("/v1/responses", async (c) => {
  let body: Record<string, unknown>;
  try {
    body = await c.req.json();
  } catch {
    return c.json({ error: { message: "invalid JSON body" } }, 400);
  }

  const wantsStream = body.stream === true;
  try {
    if (!wantsStream) {
      const final = await enqueue(() => runRelay(body).then((h) => h.final));
      return c.json(final);
    }

    // Streaming: pipe real relay frames as they land. SSE comments (`: ka`)
    // keep intermediaries from cutting the connection during long silent
    // reasoning phases; they are ignored by every SSE client.
    const handle = await enqueue(() => runRelay(body));
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        const enc = new TextEncoder();
        const push = (s: string) => controller.enqueue(enc.encode(s));
        let closed = false;
        const ka = setInterval(() => { if (!closed) push(": keepalive\n\n"); }, config.keepAliveMs);
        try {
          for await (const frame of handle.events()) {
            push(sseFrame(frame.event, frame.data));
          }
          const final = await handle.final;
          push(sseFrame("response.completed", { type: "response.completed", response: final }));
        } catch (e: any) {
          push(sseFrame("error", { type: "error", code: e instanceof RelayError ? e.kind : "internal", message: String(e?.message ?? e) }));
        } finally {
          clearInterval(ka);
          closed = true;
          controller.close();
        }
      },
    });
    return new Response(stream, { headers: SSE_HEADERS });
  } catch (e: any) {
    const kind = e instanceof RelayError ? e.kind : "internal";
    console.error(`[responses] ${kind}:`, String(e?.message ?? e).slice(0, 300));
    const status = kind === "no-bridge" || kind === "sync" ? 503 : kind === "timeout" ? 504 : 500;
    return c.json({ error: { type: kind, message: String(e?.message ?? e) } }, status as any);
  }
});

// ---------------------------------------------------------------------------
// Files (vision / audio assets)
// ---------------------------------------------------------------------------
app.post("/v1/files", async (c) => {
  try {
    const { uploadFileFromFormData } = await import("./proxy/files");
    const result = await uploadFileFromFormData(await c.req.formData());
    return c.json(result);
  } catch (e: any) {
    return c.json({ error: { message: String(e?.message ?? e) } }, 500);
  }
});

app.get("/v1/files/:id/content", async (c) => {
  try {
    const { getFileContent } = await import("./proxy/files");
    const data = await getFileContent(c.req.param("id"));
    return new Response(data as any, { headers: { "content-type": "application/octet-stream" } });
  } catch (e: any) {
    return c.json({ error: { message: String(e?.message ?? e) } }, 404);
  }
});

export default app;

// ---------------------------------------------------------------------------
// HTTP + WebSocket server wiring
// ---------------------------------------------------------------------------
export function attachWss(server: import("node:http").Server): void {
  const wss = new WebSocketServer({ noServer: true });
  wss.on("connection", (ws: any) => {
    let closed = false;
    ws.on("message", async (data: any) => {
      if (closed) return;
      let msg: any;
      try {
        msg = JSON.parse(data.toString());
      } catch {
        ws.send(JSON.stringify({ type: "error", error: "invalid JSON" }));
        return;
      }
      if (msg.type !== "response.create") {
        ws.send(JSON.stringify({ type: "error", error: `unknown type ${msg.type}` }));
        return;
      }
      const body: any = { ...msg };
      delete body.type;
      try {
        const handle = await enqueue(() => runRelay(body));
        (async () => {
          for await (const frame of handle.events()) {
            if (closed) return;
            ws.send(JSON.stringify({ type: frame.event, parsed: safeParse(frame.data) }));
          }
          const final = await handle.final;
          if (!closed) ws.send(JSON.stringify({ type: "response.completed", response: final }));
        })().catch((e) => {
          if (!closed) ws.send(JSON.stringify({ type: "error", error: String(e?.message ?? e) }));
        });
      } catch (e: any) {
        if (!closed) ws.send(JSON.stringify({ type: "error", error: String(e?.message ?? e) }));
      }
    });
    ws.on("close", () => { closed = true; });
  });
  server.on("upgrade", (req, socket, head) => {
    try {
      const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
      if (url.pathname === "/v1/responses") {
        wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
      } else {
        socket.destroy();
      }
    } catch {
      socket.destroy();
    }
  });
}

function safeParse(s: string): any {
  try { return JSON.parse(s); } catch { return s; }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
const isMain = process.argv[1] && (process.argv[1].endsWith("server.ts") || process.argv[1].endsWith("server.js"));
if (isMain) {
  const server = createHttpServer((req, res) => {
    void (async () => {
      const host = req.headers.host ?? `localhost:${config.port}`;
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
      const request = new Request(`http://${host}${req.url}`, {
        method: req.method,
        headers,
        body: body ? new Uint8Array(body) : undefined,
      });
      const response = await app.fetch(request);
      res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
      if (response.body) res.end(Buffer.from(await response.arrayBuffer()));
      else res.end();
    })().catch((e) => {
      console.error("[server] handler error:", e);
      res.writeHead(500);
      res.end("internal error");
    });
  });
  attachWss(server);
  server.listen(config.port, () => {
    console.log(`ghostprov listening on http://localhost:${config.port}`);
    // auto-launch bridge unless explicitly disabled
    if (process.env.BRIDGE_DISABLE !== "1" && process.env.VITEST !== "1") {
      setTimeout(() => {
        import("./bridge/playwright")
          .then(({ startPlaywrightBridge }) => {
            const headless = process.argv.includes("--headless") || process.env.BRIDGE_HEADLESS === "1";
            return startPlaywrightBridge({ headless });
          })
          .then(() => console.log("[ghostprov] bridge ready"))
          .catch((e) => console.warn("[ghostprov] bridge failed:", String(e?.message ?? e)));
      }, 500);
    }
  });
}
