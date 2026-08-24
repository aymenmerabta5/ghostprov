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

export type AnthropicRelayStarter = (
  body: Record<string, unknown>,
  signal: AbortSignal,
  clientId: string,
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
): AnthropicResponseContext {
  return {
    requestId,
    requestedModel: translated.requestedModel,
    thinkingDisplay: translated.thinkingDisplay,
    stopSequences: translated.stopSequences,
    estimatedInputTokens: translated.estimatedInputTokens,
    maxTokensZero: translated.maxTokensZero,
  };
}

export function registerAnthropicRoutes(
  app: Hono,
  dependencies: AnthropicRouteDependencies,
): void {
  app.post("/v1/messages/count_tokens", async c => {
    const requestId = newAnthropicRequestId();
    try {
      const body = await jsonBody(c.req);
      validateAnthropicCountTokenRequest(body);
      return c.json(
        { input_tokens: estimateAnthropicInputTokens(body) },
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
      const body = await jsonBody(c.req);
      const translated = translateAnthropicRequest(body, { models: dependencies.models });
      const context = responseContext(translated, requestId);
      const clientId = c.req.header("x-ghostprov-client-id") ?? "http";
      const handle = await dependencies.startRelay(
        translated.relayBody,
        linked.controller.signal,
        clientId,
      );

      if (!translated.stream) {
        const [final] = await Promise.all([
          handle.final,
          Array.fromAsync(handle.events()),
        ]);
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
}
