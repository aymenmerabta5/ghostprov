import { createServer as createHttpServer, type Server } from "node:http";
import type { Hono } from "hono";
import { writeFetchResponse } from "../http/adapter";
import {
  collectRequestBody,
  contentLengthExceeds,
  declaredContentLength,
} from "../http/body";
import { ApiError, apiErrorResponse } from "../http/errors";
import {
  isAnthropicRequestPath,
  requestBodyLimitForUrl,
  requestPath,
} from "../http/request-policy";
import {
  anthropicErrorResponse,
  newAnthropicRequestId,
} from "../anthropic/errors";
import { attachResponsesWebSocket } from "../ws/server";
import type { RouterLike } from "./app";
import type { WebSocketTelemetry } from "../ws/telemetry";
import type { WebSocketProcessBudget } from "../ws/session";
import type { IncomingResourceLease } from "../router/router";
import type { HttpRequestBindings } from "../http/request-lease";

export interface GhostProvServerOptions {
  app: Hono;
  router: RouterLike;
  port: number;
  candidate?: boolean;
  maxBodyBytes?: number;
  webSocketTelemetry?: WebSocketTelemetry;
  webSocketProcessBudget?: WebSocketProcessBudget;
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
  return anthropicErrorResponse(error, newAnthropicRequestId());
}

export function createGhostProvServer(options: GhostProvServerOptions): Server {
  if (options.candidate && options.port === 8787) {
    throw new RangeError("Candidate server cannot bind protected port 8787");
  }
  const connectionIds = new WeakMap<object, string>();
  let nextConnectionId = 0;
  const server = createHttpServer((request, response) => {
    let connectionClientId = connectionIds.get(request.socket);
    if (!connectionClientId) {
      connectionClientId = `http-connection-${nextConnectionId}`;
      nextConnectionId = (nextConnectionId + 1) % 10_000;
      connectionIds.set(request.socket, connectionClientId);
    }
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
        let bodyLease: IncomingResourceLease | null = null;
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
            const declared = declaredContentLength(request.headers["content-length"]);
            // Unknown/chunked bodies start with the governor's small ingress
            // floor and grow before each chunk is retained. Reserving the full
            // 64 MiB ceiling here used to consume roughly 130 MiB per tiny
            // request and destroyed useful concurrency.
            const reservationBytes = declared ?? 0;
            bodyLease = await options.router.acquireIncomingBody?.(
              reservationBytes,
              relayAbort.signal,
              { maxBytes: maxBodyBytes },
            ) ?? null;
            const collected = await collectRequestBody(request, maxBodyBytes, {
              signal: bodyLease?.signal ?? relayAbort.signal,
              beforeRetain: bodyLease?.ensureBytes
                ? bytes => bodyLease!.ensureBytes!(bytes)
                : undefined,
            });
            if (collected.length) body = new Uint8Array(collected);
          }
          const fetchRequest = new Request(`http://${host}${request.url ?? "/"}`, {
            method: request.method,
            headers,
            body: body as unknown as BodyInit,
            signal: relayAbort.signal,
          });
          const bindings: HttpRequestBindings = {
            incomingBodyLease: bodyLease ?? undefined,
            httpConnectionClientId: connectionClientId,
          };
          fetchResponse = await options.app.fetch(fetchRequest, bindings);
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
    processBudget: options.webSocketProcessBudget,
    startRelay: (body, relayOptions) => options.router.startRelay(body, {
      source: "websocket",
      clientId: relayOptions.clientId,
      signal: relayOptions.signal,
      encodedBodyBytes: relayOptions.encodedBodyBytes,
    }),
  });
  return server;
}
