import { createServer as createHttpServer, type Server } from "node:http";
import type { Hono } from "hono";
import { writeFetchResponse } from "../http/adapter";
import { collectRequestBody, contentLengthExceeds } from "../http/body";
import { ApiError, apiErrorResponse } from "../http/errors";
import {
  isAnthropicRequestPath,
  requestBodyLimitForUrl,
  requestPath,
} from "../http/request-policy";
import {
  AnthropicRequestError,
  anthropicErrorResponse,
  newAnthropicRequestId,
} from "../anthropic/errors";
import { attachResponsesWebSocket } from "../ws/server";
import type { RouterLike } from "./app";
import type { WebSocketTelemetry } from "../ws/telemetry";

export interface GhostProvServerOptions {
  app: Hono;
  router: RouterLike;
  port: number;
  candidate?: boolean;
  maxBodyBytes?: number;
  webSocketTelemetry?: WebSocketTelemetry;
}

export { requestBodyLimitForUrl } from "../http/request-policy";

function bodyTooLarge(maxBodyBytes: number): ApiError {
  return new ApiError(
    413,
    `Request body exceeds the configured ${maxBodyBytes}-byte limit.`,
    { type: "invalid_request_error", param: "body", code: "request_too_large" },
  );
}

function requestErrorResponse(error: unknown, anthropic: boolean): Response {
  if (!anthropic) return apiErrorResponse(error);
  const normalized = error instanceof ApiError && error.status === 413
    ? new AnthropicRequestError(413, error.message, "request_too_large")
    : error;
  return anthropicErrorResponse(normalized, newAnthropicRequestId());
}

export function createGhostProvServer(options: GhostProvServerOptions): Server {
  if (options.candidate && options.port === 8787) {
    throw new RangeError("Candidate server cannot bind protected port 8787");
  }
  const server = createHttpServer((request, response) => {
    const relayAbort = new AbortController();
    const abortRelay = () => relayAbort.abort();
    const abortOnIncompleteClose = () => {
      if (!response.writableEnded) abortRelay();
    };
    request.once("aborted", abortRelay);
    response.once("close", abortOnIncompleteClose);
    void (async () => {
      const path = requestPath(request.url);
      const anthropicPath = isAnthropicRequestPath(path);
      try {
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
            const maxBodyBytes = requestBodyLimitForUrl(
              request.url,
              options.maxBodyBytes ?? 64 * 1024 ** 2,
            );
            if (contentLengthExceeds(request.headers["content-length"], maxBodyBytes)) {
              request.resume();
              fetchResponse = requestErrorResponse(bodyTooLarge(maxBodyBytes), anthropicPath);
              await writeFetchResponse(response, fetchResponse);
              return;
            }
            const declared = Number(request.headers["content-length"]);
            const reservationBytes = Number.isFinite(declared) && declared >= 0
              ? declared
              : maxBodyBytes;
            bodyLease = await options.router.acquireIncomingBody?.(
              reservationBytes,
              relayAbort.signal,
            ) ?? null;
            const collected = await collectRequestBody(request, maxBodyBytes);
            if (collected.length) body = new Uint8Array(collected);
          }
          const fetchRequest = new Request(`http://${host}${request.url ?? "/"}`, {
            method: request.method,
            headers,
            body: body as unknown as BodyInit,
            signal: relayAbort.signal,
          });
          fetchResponse = await options.app.fetch(fetchRequest);
        } finally {
          bodyLease?.release();
        }
        await writeFetchResponse(response, fetchResponse);
      } catch (error) {
        if (response.destroyed || response.headersSent) throw error;
        await writeFetchResponse(response, requestErrorResponse(error, anthropicPath));
      } finally {
        request.off("aborted", abortRelay);
        response.off("close", abortOnIncompleteClose);
      }
    })().catch(async error => {
      if (response.destroyed) return;
      if (response.headersSent) {
        response.destroy(error as Error);
        return;
      }
      await writeFetchResponse(response, apiErrorResponse(error));
    });
  });
  attachResponsesWebSocket(server, {
    telemetry: options.webSocketTelemetry,
    startRelay: (body, relayOptions) => options.router.startRelay(body, {
      source: "websocket",
      clientId: "websocket",
      signal: relayOptions.signal,
    }),
  });
  return server;
}
