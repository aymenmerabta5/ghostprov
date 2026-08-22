import { Hono } from "hono";
import type { RunHandle } from "../relay/run-handle";
import { sseFrame, SSE_HEADERS } from "../relay/sse";
import { apiErrorEvent, apiErrorResponse, normalizeApiError } from "../http/errors";
import { registerImageRoutes } from "../images/routes";
import type { ImageCompatOptions } from "../images/compat";

export interface RouterLike {
  startRelay(
    body: Record<string, unknown>,
    options?: {
      clientId?: string;
      source?: "http" | "websocket" | "image" | "internal";
      signal?: AbortSignal;
    },
  ): Promise<RunHandle>;
  acquireIncomingBody?(bytes: number, signal?: AbortSignal): Promise<{ release(): void }>;
  snapshot(): {
    capacity: number;
    active: number;
    available: number;
    queued: number;
    runtimes: unknown[];
    resources: unknown;
  };
}

export interface GhostProvAppOptions {
  router: RouterLike;
  imageOptions?: ImageCompatOptions;
  keepAliveMs?: number;
}

export function createGhostProvApp(options: GhostProvAppOptions): Hono {
  const app = new Hono();
  const keepAliveMs = options.keepAliveMs ?? 15_000;

  app.get("/health", c => {
    const snapshot = options.router.snapshot();
    return c.json({
      ok: true,
      capacity: snapshot.capacity,
      active: snapshot.active,
      available: snapshot.available,
      queued: snapshot.queued,
      runtimes: snapshot.runtimes.length,
    });
  });

  app.get("/__sandboxes", c => {
    const snapshot = options.router.snapshot();
    return c.json({
      capacity: snapshot.capacity,
      active: snapshot.active,
      available: snapshot.available,
      queued: snapshot.queued,
      runtimes: snapshot.runtimes,
      resources: snapshot.resources,
    });
  });

  app.post("/v1/responses", async c => {
    let body: Record<string, unknown>;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: { message: "invalid JSON body" } }, 400);
    }
    try {
      const handle = await options.router.startRelay(body, {
        source: "http",
        clientId: c.req.header("x-ghostprov-client-id") ?? "http",
        signal: c.req.raw.signal,
      });
      if (body.stream !== true) return c.json(await handle.final);

      const stream = new ReadableStream<Uint8Array>({
        async start(controller) {
          const encoder = new TextEncoder();
          const push = (value: string) => controller.enqueue(encoder.encode(value));
          let closed = false;
          let sawCompleted = false;
          const keepAlive = setInterval(() => { if (!closed) push(": keepalive\n\n"); }, keepAliveMs);
          try {
            for await (const frame of handle.events()) {
              if (frame.event === "response.completed") sawCompleted = true;
              push(sseFrame(frame.event, frame.data));
            }
            if (!sawCompleted) {
              const final = await handle.final;
              push(sseFrame("response.completed", { type: "response.completed", response: final }));
            }
          } catch (error) {
            push(sseFrame("error", apiErrorEvent(error)));
          } finally {
            clearInterval(keepAlive);
            closed = true;
            controller.close();
          }
        },
      });
      return new Response(stream, { headers: SSE_HEADERS });
    } catch (error) {
      const normalized = normalizeApiError(error);
      console.error(`[responses] ${normalized.error.type}: ${normalized.error.message.slice(0, 240)}`);
      return apiErrorResponse(error);
    }
  });

  registerImageRoutes(app, {
    options: options.imageOptions ?? {
      responsesModel: "gpt-5.6-sol",
      maxEditInputBytes: 40 * 1024 ** 2,
    },
    startRelay: (body, signal) => options.router.startRelay(body, {
      source: "image",
      clientId: "images",
      signal,
    }),
  });

  return app;
}
