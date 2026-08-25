import type { Hono } from "hono";
import type { RunHandle } from "../relay/run-handle";
import {
  AnthropicRequestError,
  anthropicErrorResponse,
  newAnthropicRequestId,
} from "./errors";
import type { AnthropicModelOptions } from "./model";
import { createAnthropicPullStream } from "./pull-stream";
import { translateAnthropicRequest } from "./request";
import { responseToAnthropic, type AnthropicResponseContext } from "./response";
import { AnthropicStreamTranslator } from "./stream";
import {
  estimateAnthropicInputTokens,
  validateAnthropicCountTokenRequest,
} from "./token-count";
import type { AnthropicContextManagementResponse } from "./types";
import type { IncomingResourceLease } from "../router/router";
import { takeIncomingBodyLease } from "../http/request-lease";

export type AnthropicRelayStarter = (
  body: Record<string, unknown>,
  signal: AbortSignal,
  clientId: string,
  incomingLease?: IncomingResourceLease,
) => Promise<RunHandle>;

export type AnthropicRouteDependencies = {
  keepAliveMs: number;
  models: AnthropicModelOptions;
  startRelay: AnthropicRelayStarter;
};

const ANTHROPIC_SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
  "x-accel-buffering": "no",
} as const;

const ANTHROPIC_VERSION = "2023-06-01";
const NONSTREAM_CLEANUP_MS = 25;

async function jsonBody(request: { json(): Promise<unknown> }): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new AnthropicRequestError(400, "Request body is not valid JSON.");
  }
}

function linkedAbortController(signal: AbortSignal): {
  controller: AbortController;
  unlink(): void;
} {
  const controller = new AbortController();
  const abort = () => controller.abort(signal.reason);
  if (signal.aborted) abort();
  else signal.addEventListener("abort", abort, { once: true });
  return {
    controller,
    unlink: () => signal.removeEventListener("abort", abort),
  };
}

function responseContext(
  translated: ReturnType<typeof translateAnthropicRequest>,
  requestId: string,
  beta: boolean,
  contextManagement: AnthropicContextManagementResponse | null,
): AnthropicResponseContext {
  return {
    requestId,
    requestedModel: translated.requestedModel,
    thinkingDisplay: translated.thinkingDisplay,
    stopSequences: translated.stopSequences,
    estimatedInputTokens: translated.estimatedInputTokens,
    maxTokensZero: translated.maxTokensZero,
    beta,
    contextManagement,
  };
}

function validateProtocolHeaders(request: { header(name: string): string | undefined }): void {
  const contentType = request.header("content-type")?.split(";", 1)[0].trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new AnthropicRequestError(400, "content-type must be application/json");
  }
  const version = request.header("anthropic-version")?.trim();
  if (!version) {
    throw new AnthropicRequestError(400, "anthropic-version header is required");
  }
  if (version !== ANTHROPIC_VERSION) {
    throw new AnthropicRequestError(
      400,
      `anthropic-version must be ${ANTHROPIC_VERSION}`,
    );
  }
}

function betaContextManagement(
  body: unknown,
  beta: boolean,
): AnthropicContextManagementResponse | null {
  if (!beta || !body || typeof body !== "object" || Array.isArray(body)) return null;
  const request = body as Record<string, unknown>;
  return Object.prototype.hasOwnProperty.call(request, "context_management") &&
      request.context_management !== null
    ? { applied_edits: [] }
    : null;
}

function betaRequest(request: {
  query(name: string): string | undefined;
  header(name: string): string | undefined;
}): boolean {
  return request.query("beta") === "true" || Boolean(request.header("anthropic-beta")?.trim());
}

function methodNotAllowed(requestId: string): Response {
  const response = anthropicErrorResponse(
    new AnthropicRequestError(405, "Method not allowed"),
    requestId,
  );
  response.headers.set("allow", "POST");
  return response;
}

async function discardRelayEvents(iterator: AsyncGenerator<unknown>): Promise<void> {
  for await (const _event of iterator) {
    // V2 must be drained, but a non-streaming HTTP response never retains the transcript.
  }
}

type AsyncOutcome<T> =
  | { ok: true; value: T }
  | { ok: false; error: unknown };

function settle<T>(promise: Promise<T>): Promise<AsyncOutcome<T>> {
  return promise.then(
    value => ({ ok: true, value }),
    error => ({ ok: false, error }),
  );
}

function cancellation(signal: AbortSignal): {
  promise: Promise<{ cancelled: true; error: unknown }>;
  dispose(): void;
} {
  let onAbort: (() => void) | undefined;
  const promise = new Promise<{ cancelled: true; error: unknown }>(resolve => {
    const finish = () => resolve({
      cancelled: true,
      error: signal.reason ?? new DOMException("Request was cancelled", "AbortError"),
    });
    if (signal.aborted) finish();
    else {
      onAbort = finish;
      signal.addEventListener("abort", finish, { once: true });
    }
  });
  return {
    promise,
    dispose() {
      if (onAbort) signal.removeEventListener("abort", onAbort);
      onAbort = undefined;
    },
  };
}

async function boundedCleanup(promises: readonly Promise<unknown>[]): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<void>(resolve => {
    timer = setTimeout(resolve, NONSTREAM_CLEANUP_MS);
  });
  await Promise.race([Promise.all(promises).then(() => undefined), timeout]);
  if (timer !== undefined) clearTimeout(timer);
}

async function nonStreamingFinal(
  handle: RunHandle,
  controller: AbortController,
): Promise<unknown> {
  const iterator = handle.events();
  const drain = discardRelayEvents(iterator);
  let iteratorReturned = false;
  const returnIterator = (): void => {
    if (iteratorReturned) return;
    iteratorReturned = true;
    try {
      void Promise.resolve(iterator.return(undefined)).catch(() => undefined);
    } catch {
      // Cancellation/error completion never depends on iterator cooperation.
    }
  };
  let final: Promise<unknown>;
  try {
    final = Promise.resolve(handle.final);
  } catch (error) {
    final = Promise.reject(error);
  }
  const finalOutcome = settle(final);
  const drainOutcome = settle(drain);
  const work = Promise.race([
    finalOutcome.then(result => ({ source: "final" as const, result })),
    drainOutcome.then(result => ({ source: "drain" as const, result })),
  ]).then(async first => {
    if (!first.result.ok) {
      return { completed: false as const, error: first.result.error };
    }
    const second = first.source === "final" ? await drainOutcome : await finalOutcome;
    if (!second.ok) return { completed: false as const, error: second.error };
    const finalResult = first.source === "final" ? first.result : second;
    return { completed: true as const, value: finalResult.value };
  });
  const cancelled = cancellation(controller.signal);
  const winner = await Promise.race([work, cancelled.promise]);
  cancelled.dispose();

  if ("cancelled" in winner) {
    returnIterator();
    void Promise.all([finalOutcome, drainOutcome]);
    throw winner.error;
  }
  if (!winner.completed) {
    if (!controller.signal.aborted) controller.abort(winner.error);
    returnIterator();
    await boundedCleanup([finalOutcome, drainOutcome]);
    throw winner.error;
  }
  return winner.value;
}

export function registerAnthropicRoutes(
  app: Hono,
  dependencies: AnthropicRouteDependencies,
): void {
  app.post("/v1/messages/count_tokens", async c => {
    const requestId = newAnthropicRequestId();
    try {
      validateProtocolHeaders(c.req);
      const body = await jsonBody(c.req);
      validateAnthropicCountTokenRequest(body);
      const beta = betaRequest(c.req);
      return c.json(
        {
          input_tokens: estimateAnthropicInputTokens(body),
          ...(beta ? { context_management: null } : {}),
        },
        200,
        { "request-id": requestId },
      );
    } catch (error) {
      return anthropicErrorResponse(error, requestId);
    }
  });

  app.post("/v1/messages", async c => {
    const requestId = newAnthropicRequestId();
    const linked = linkedAbortController(c.req.raw.signal);
    let streamOwnsLink = false;
    try {
      validateProtocolHeaders(c.req);
      const body = await jsonBody(c.req);
      const translated = translateAnthropicRequest(body, { models: dependencies.models });
      const beta = betaRequest(c.req);
      const context = responseContext(
        translated,
        requestId,
        beta,
        betaContextManagement(body, beta),
      );
      const clientId = c.req.header("x-ghostprov-client-id") ?? "http";
      const handle = await dependencies.startRelay(
        translated.relayBody,
        linked.controller.signal,
        clientId,
        takeIncomingBodyLease(c.env),
      );

      if (!translated.stream) {
        const final = await nonStreamingFinal(handle, linked.controller);
        return c.json(
          responseToAnthropic(final, context),
          200,
          { "request-id": requestId },
        );
      }

      const stream = createAnthropicPullStream({
        handle,
        translator: new AnthropicStreamTranslator(context),
        keepAliveMs: dependencies.keepAliveMs,
        abortController: linked.controller,
        onClose: linked.unlink,
      });
      streamOwnsLink = true;
      return new Response(stream, {
        headers: { ...ANTHROPIC_SSE_HEADERS, "request-id": requestId },
      });
    } catch (error) {
      if (!linked.controller.signal.aborted) linked.controller.abort(error);
      return anthropicErrorResponse(error, requestId);
    } finally {
      if (!streamOwnsLink) linked.unlink();
    }
  });

  app.all("/v1/messages/count_tokens", c => methodNotAllowed(newAnthropicRequestId()));
  app.all("/v1/messages", c => methodNotAllowed(newAnthropicRequestId()));
}
