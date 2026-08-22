import { createServer as createHttpServer, type Server } from "node:http";
import type { Hono } from "hono";
import { writeFetchResponse } from "../http/adapter";
import { collectRequestBody, contentLengthExceeds } from "../http/body";
import { ApiError, apiErrorResponse } from "../http/errors";
import { attachResponsesWebSocket } from "../ws/server";
import type { RouterLike } from "./app";

export interface GhostProvServerOptions {
  app: Hono;
  router: RouterLike;
  port: number;
  candidate?: boolean;
  maxBodyBytes?: number;
}

export function createGhostProvServer(options: GhostProvServerOptions): Server {
  if (options.candidate && options.port === 8787) {
    throw new RangeError("Candidate server cannot bind protected port 8787");
  }
  const server = createHttpServer((request, response) => {
    void (async () => {
      const host = request.headers.host ?? `127.0.0.1:${options.port}`;
      const headers = new Headers();
      for (const [name, value] of Object.entries(request.headers)) {
        if (value !== undefined) headers.set(name, Array.isArray(value) ? value.join(",") : String(value));
      }
      let bodyLease: { release(): void } | null = null;
      let fetchResponse: Response;
      try {
        let body: Uint8Array | undefined;
        if (request.method !== "GET" && request.method !== "HEAD") {
          const maxBodyBytes = options.maxBodyBytes ?? 64 * 1024 ** 2;
          if (contentLengthExceeds(request.headers["content-length"], maxBodyBytes)) {
            request.resume();
            fetchResponse = apiErrorResponse(new ApiError(
              413,
              `Request body exceeds the configured ${maxBodyBytes}-byte limit.`,
              { type: "invalid_request_error", param: "body", code: "request_too_large" },
            ));
            await writeFetchResponse(response, fetchResponse);
            return;
          }
          const declared = Number(request.headers["content-length"]);
          const reservationBytes = Number.isFinite(declared) && declared >= 0
            ? declared
            : maxBodyBytes;
          bodyLease = await options.router.acquireIncomingBody?.(reservationBytes) ?? null;
          const collected = await collectRequestBody(request, maxBodyBytes);
          if (collected.length) body = new Uint8Array(collected);
        }
        const fetchRequest = new Request(`http://${host}${request.url ?? "/"}`, {
          method: request.method,
          headers,
          body: body as unknown as BodyInit,
        });
        fetchResponse = await options.app.fetch(fetchRequest);
      } finally {
        bodyLease?.release();
      }
      await writeFetchResponse(response, fetchResponse);
    })().catch(async error => {
      if (response.headersSent) {
        response.destroy(error as Error);
        return;
      }
      await writeFetchResponse(response, apiErrorResponse(error));
    });
  });
  attachResponsesWebSocket(server, {
    startRelay: (body, relayOptions) => options.router.startRelay(body, {
      source: "websocket",
      clientId: "websocket",
      signal: relayOptions.signal,
    }),
  });
  return server;
}
