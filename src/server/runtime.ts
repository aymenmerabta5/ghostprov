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
  AnthropicRequestError,
  anthropicErrorResponse,
  newAnthropicRequestId,
} from "../anthropic/errors";
import { attachResponsesWebSocket } from "../ws/server";
import {
  ghostProvAppAuthorizationScope,
  type RouterLike,
} from "./app";
import type { WebSocketTelemetry } from "../ws/telemetry";
import type { WebSocketProcessBudget } from "../ws/session";
import type { IncomingResourceLease } from "../router/router";
import type { HttpRequestBindings } from "../http/request-lease";
import {
  apiKeyAuthorized,
  isAuthenticatedTenantScope,
  isProtectedApiPath,
  type AuthenticatedTenantScope,
} from "../http/api-key";

export interface GhostProvServerOptions {
  app: Hono;
  router: RouterLike;
  /** Omitted only by narrow unit-test/library seams. Production always sets it. */
  apiKey?: string;
  port: number;
  candidate?: boolean;
  maxBodyBytes?: number;
  webSocketTelemetry?: WebSocketTelemetry;
  webSocketProcessBudget?: WebSocketProcessBudget;
  /** Shared trusted scope for HTTP, Anthropic, and WebSocket continuations. */
  authorizationScope?: AuthenticatedTenantScope;
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
  const routerScope = options.router.responseProvenanceAuthorizationScope?.() ?? null;
  if (routerScope !== null && !isAuthenticatedTenantScope(routerScope)) {
    throw new TypeError("router authorization scope must be an AuthenticatedTenantScope");
  }
  if (
    options.authorizationScope !== undefined &&
    options.authorizationScope !== routerScope
  ) {
    throw new TypeError("authorizationScope must be the router's exact AuthenticatedTenantScope");
  }
  const scopeValue = routerScope;
  if (!isAuthenticatedTenantScope(scopeValue)) {
    throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
  }
  const authorizationScope = scopeValue;
  const appScope = ghostProvAppAuthorizationScope(options.app);
  if (appScope === undefined || appScope !== authorizationScope) {
    throw new TypeError(
      "app, HTTP router, and WebSocket router must share the exact AuthenticatedTenantScope",
    );
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
        if (
          options.apiKey &&
          isProtectedApiPath(path) &&
          !apiKeyAuthorized({
            authorization: request.headers.authorization,
            xApiKey: request.headers["x-api-key"],
          }, options.apiKey, anthropicPath ? "anthropic" : "openai")
        ) {
          request.resume();
          const unauthorized = anthropicPath
            ? anthropicErrorResponse(
                new AnthropicRequestError(401, "Invalid API key"),
                newAnthropicRequestId(),
              )
            : apiErrorResponse(new ApiError(401, "Invalid API key", {
                type: "authentication_error",
                code: "invalid_api_key",
              }));
          if (!anthropicPath) {
            unauthorized.headers.set("www-authenticate", 'Bearer realm="ghostprov"');
          }
          await writeFetchResponse(response, unauthorized);
          return;
        }
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
              {
                maxBytes: maxBodyBytes,
                // Multipart image parsing temporarily owns raw, File,
                // ArrayBuffer/Buffer, and base64 representations together.
                reservationMultiplier: path === "/v1/images/edits" ? 5 : 2,
              },
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
    apiKey: options.apiKey,
    telemetry: options.webSocketTelemetry,
    processBudget: options.webSocketProcessBudget,
    authorizationScope,
    startRelay: (body, relayOptions) => options.router.startRelay(body, {
      source: "websocket",
      clientId: relayOptions.clientId,
      signal: relayOptions.signal,
      authorizationScope: relayOptions.authorizationScope,
      encodedBodyBytes: relayOptions.encodedBodyBytes,
      structuralBodyBytes: relayOptions.structuralBodyBytes,
    }),
  });
  return server;
}
