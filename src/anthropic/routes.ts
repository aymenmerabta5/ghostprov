import type { Hono } from "hono";
import { BoundedPreparationScheduler } from "../relay/preparation-scheduler";
import type { RunHandle } from "../relay/run-handle";
import { normalizeUpstreamResponsesRequest } from "../relay/upstream-policy";
import type { AuthenticatedTenantScope } from "../http/api-key";
import {
  AnthropicContinuationIndex,
  projectAnthropicReplay,
  terminalProviderResponseId,
  type PreparedAnthropicContinuation,
} from "./continuation";
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
import { definitivePreviousResponseNotFound } from "../relay/continuation-error";
import type { IncomingResourceLease } from "../router/router";
import {
  incomingLeaseTimingView,
  linkIncomingLeaseAbort,
  resolveHttpClientId,
  takeIncomingBodyLease,
} from "../http/request-lease";
import { permittedJsonResponse } from "../http/permitted-json";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";

export type AnthropicRelayStarter = (
  body: Record<string, unknown>,
  signal: AbortSignal,
  clientId: string,
  incomingLease?: IncomingResourceLease,
  admission?: AnthropicRelayAdmission,
) => Promise<RunHandle>;

export type AnthropicRouteDependencies = {
  keepAliveMs: number;
  models: AnthropicModelOptions;
  startRelay: AnthropicRelayStarter;
  /** One bounded index shared by every stable/beta route in this app. */
  continuationIndex?: AnthropicContinuationIndex;
  /** Opaque app-owned authority. Missing scope disables continuation reuse. */
  continuationScope?: AuthenticatedTenantScope;
};

const ANTHROPIC_SSE_HEADERS = {
  "content-type": "text/event-stream; charset=utf-8",
  "cache-control": "no-cache",
  "x-accel-buffering": "no",
} as const;

const ANTHROPIC_VERSION = "2023-06-01";
const NONSTREAM_CLEANUP_MS = 25;
const CONTINUATION_PREPARE_MAX_QUEUED = 64;
const CONTINUATION_PREPARE_MAX_QUEUED_BYTES = 256 * 1024 * 1024;

export type AnthropicRelayAdmission = Readonly<{
  /** Full preprocessing bytes charged only to fair scheduling. */
  fairnessBytes?: number;
}>;

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

async function discardRelayEvents(
  iterator: AsyncGenerator<unknown>,
  onEvent: () => void,
): Promise<void> {
  for await (const _event of iterator) {
    onEvent();
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
  onFailure: (
    source: "final" | "events" | "cancelled",
    sawRelayEvent: boolean,
  ) => void = () => undefined,
): Promise<unknown> {
  const iterator = handle.events();
  let sawRelayEvent = false;
  const drain = discardRelayEvents(iterator, () => { sawRelayEvent = true; });
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
  const releaseFinalOutcome = (): void => {
    void finalOutcome.then(result => {
      if (result.ok) releaseAtomicBytePermit(result.value);
    });
  };
  const drainOutcome = settle(drain);
  const work = Promise.race([
    finalOutcome.then(result => ({ source: "final" as const, result })),
    drainOutcome.then(result => ({ source: "events" as const, result })),
  ]).then(async first => {
    if (!first.result.ok) {
      return {
        completed: false as const,
        error: first.result.error,
        source: first.source,
      };
    }
    const second = first.source === "final" ? await drainOutcome : await finalOutcome;
    if (!second.ok) {
      return {
        completed: false as const,
        error: second.error,
        source: first.source === "final" ? "events" as const : "final" as const,
      };
    }
    const finalResult = first.source === "final" ? first.result : second;
    return { completed: true as const, value: finalResult.value };
  });
  const cancelled = cancellation(controller.signal);
  const winner = await Promise.race([work, cancelled.promise]);
  cancelled.dispose();

  if ("cancelled" in winner) {
    onFailure("cancelled", sawRelayEvent);
    releaseFinalOutcome();
    returnIterator();
    void Promise.all([finalOutcome, drainOutcome]);
    throw winner.error;
  }
  if (!winner.completed) {
    releaseFinalOutcome();
    if (!controller.signal.aborted) controller.abort(winner.error);
    returnIterator();
    await boundedCleanup([finalOutcome, drainOutcome]);
    onFailure(winner.source, sawRelayEvent);
    throw winner.error;
  }
  return winner.value;
}

function continuationPreparationBytes(
  translated: ReturnType<typeof translateAnthropicRequest>,
  incomingLease: IncomingResourceLease | undefined,
): number {
  const leased = incomingLease?.bodyBytes ?? incomingLease?.reservedBytes;
  const inbound = Number.isSafeInteger(leased) && Number(leased) >= 0
    ? Number(leased)
    : translated.inboundBytes;
  return Math.min(Number.MAX_SAFE_INTEGER, inbound + translated.relayBytes);
}

async function prepareContinuation(
  index: AnthropicContinuationIndex,
  scheduler: BoundedPreparationScheduler<PreparedAnthropicContinuation>,
  translated: ReturnType<typeof translateAnthropicRequest>,
  scope: AuthenticatedTenantScope | null,
  signal: AbortSignal,
  costBytes: number,
): Promise<PreparedAnthropicContinuation | null> {
  if (scope === null || translated.maxTokensZero || translated.stopSequences.length > 0) {
    return null;
  }
  return scheduler.prepare(async () => {
    try {
      return await index.prepareAsync(translated.relayBody, scope, signal);
    } catch {
      // Continuations are an optimization. Canonicalization must never alter the
      // existing Anthropic request surface.
      return null;
    }
  }, costBytes, signal);
}

type AnthropicContinuationCommitToken = Pick<
  PreparedAnthropicContinuation,
  "decision" | "inputDigest" | "settingsDigest" | "scopeDigest"
>;

function commitContinuation(
  index: AnthropicContinuationIndex,
  prepared: AnthropicContinuationCommitToken | null,
  scope: AuthenticatedTenantScope | null,
  finalValue: unknown,
  projected: ReturnType<typeof responseToAnthropic>,
): void {
  if (!prepared || scope === null || prepared.decision === "empty_suffix") return;
  try {
    const replay = projectAnthropicReplay(finalValue, projected);
    const responseId = terminalProviderResponseId(finalValue);
    if (!replay || !responseId) return;
    index.commit(prepared, scope, responseId, replay);
  } catch {
    // A failed metadata commit cannot replace an otherwise valid response.
  }
}

function releaseFailedPreludeHandle(
  handle: RunHandle,
  controller: AbortController,
  error: unknown,
): void {
  if (!controller.signal.aborted) controller.abort(error);
  try {
    void Promise.resolve(handle.final).then(releaseAtomicBytePermit, () => undefined);
  } catch {
    // A throwing final accessor has no terminal allocation to release.
  }
}

export function registerAnthropicRoutes(
  app: Hono,
  dependencies: AnthropicRouteDependencies,
): void {
  const continuationIndex = dependencies.continuationIndex ?? new AnthropicContinuationIndex();
  const continuationScheduler = new BoundedPreparationScheduler<PreparedAnthropicContinuation>({
    maxQueued: CONTINUATION_PREPARE_MAX_QUEUED,
    maxQueuedBytes: CONTINUATION_PREPARE_MAX_QUEUED_BYTES,
  });
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
    const requestLink = linkedAbortController(c.req.raw.signal);
    let streamOwnsLink = false;
    let unlinkIncomingSignal: () => void = () => undefined;
    let completeIncomingAdmission: () => void = () => undefined;
    let retainedIncomingLease: IncomingResourceLease | undefined;
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
      const clientId = resolveHttpClientId(c.env, c.req.header("x-ghostprov-client-id"));
      const scope = dependencies.continuationScope ?? null;
      const incomingLease = takeIncomingBodyLease(c.env);
      unlinkIncomingSignal = linkIncomingLeaseAbort(
        requestLink.controller,
        incomingLease,
      );
      let incomingAdmissionCompleted = false;
      completeIncomingAdmission = () => {
        if (incomingAdmissionCompleted) return;
        incomingAdmissionCompleted = true;
        incomingLease?.completeAdmission?.();
        unlinkIncomingSignal();
      };
      const prepared = await prepareContinuation(
        continuationIndex,
        continuationScheduler,
        translated,
        scope,
        requestLink.controller.signal,
        continuationPreparationBytes(translated, incomingLease),
      );
      const preparedDecision = prepared?.decision ?? null;
      const preparedMatch = prepared?.match ?? null;
      const commitToken: AnthropicContinuationCommitToken | null = prepared
        ? {
            decision: prepared.decision,
            inputDigest: prepared.inputDigest,
            settingsDigest: prepared.settingsDigest,
            scopeDigest: prepared.scopeDigest,
          }
        : null;
      if (preparedDecision === "hit") retainedIncomingLease = incomingLease;
      const continuationLease = retainedIncomingLease
        ? incomingLeaseTimingView(retainedIncomingLease)
        : undefined;
      const initialRelayBody = prepared?.relayBody ?? translated.relayBody;
      let fallbackRelayBody: Record<string, unknown> | null = translated.relayBody;
      const clientWantsStream = translated.stream;
      let attempt = 0;

      for (;;) {
        const attemptLink = linkedAbortController(requestLink.controller.signal);
        let relayBody: Record<string, unknown> = normalizeUpstreamResponsesRequest(
          attempt === 0 ? initialRelayBody : fallbackRelayBody!,
        );
        let handle: RunHandle | null = null;
        let stream: ReadableStream<Uint8Array> | null = null;
        let streamOwnsAttempt = false;
        let stage: "start" | "nonstream" | "projection" | "prelude" | "sse-commit" = "start";
        const nonstreamFailure: {
          source: "final" | "events" | "cancelled" | null;
          sawRelayEvent: boolean;
        } = { source: null, sawRelayEvent: false };
        try {
          handle = await dependencies.startRelay(
            relayBody,
            attemptLink.controller.signal,
            clientId,
            attempt === 0 && !retainedIncomingLease ? incomingLease : continuationLease,
            attempt === 0 && preparedDecision === "hit"
              ? {
                  fairnessBytes: Math.max(
                    incomingLease?.bodyBytes ?? 0,
                    commitToken!.inputDigest.canonicalBytes,
                  ),
                }
              : undefined,
          );
          completeIncomingAdmission();

          if (attempt > 0) {
            fallbackRelayBody = null;
            relayBody = {};
            if (retainedIncomingLease) {
              retainedIncomingLease.release();
              retainedIncomingLease = undefined;
            }
          }

          if (!clientWantsStream) {
            stage = "nonstream";
            const final = await nonStreamingFinal(
              handle,
              attemptLink.controller,
              (source, sawRelayEvent) => {
                nonstreamFailure.source = source;
                nonstreamFailure.sawRelayEvent = sawRelayEvent;
              },
            );
            stage = "projection";
            let response: ReturnType<typeof responseToAnthropic> | null = null;
            try {
              response = responseToAnthropic(final, context);
              commitContinuation(continuationIndex, commitToken, scope, final, response);
              transferAtomicBytePermit(final, response, "anthropic-json-output");
              return permittedJsonResponse(response, {
                headers: { "request-id": requestId },
              });
            } catch (error) {
              releaseAtomicBytePermit(final);
              releaseAtomicBytePermit(response);
              throw error;
            }
          }

          // Do not commit an Anthropic SSE 200 until R2 has proven successful
          // upstream headers. Initial provider failures then use the normal
          // Anthropic HTTP error mapping; failures after this point stay in-band.
          stage = "prelude";
          if (handle.upstreamPrelude) await handle.upstreamPrelude;
          stage = "sse-commit";
          stream = createAnthropicPullStream({
            handle,
            translator: new AnthropicStreamTranslator(context, {
              beforeMessageStop: (final, response) => {
                commitContinuation(continuationIndex, commitToken, scope, final, response);
              },
            }),
            keepAliveMs: dependencies.keepAliveMs,
            abortController: attemptLink.controller,
            onClose: () => {
              attemptLink.unlink();
              requestLink.unlink();
            },
          });
          const response = new Response(stream, {
            headers: { ...ANTHROPIC_SSE_HEADERS, "request-id": requestId },
          });
          streamOwnsAttempt = true;
          streamOwnsLink = true;
          return response;
        } catch (error) {
          if (handle && stage === "prelude") {
            releaseFailedPreludeHandle(handle, attemptLink.controller, error);
          } else if (!attemptLink.controller.signal.aborted) {
            attemptLink.controller.abort(error);
          }
          if (stream && !streamOwnsAttempt) void stream.cancel(error).catch(() => undefined);

          const safeNonstreamFailure = stage === "nonstream" &&
            nonstreamFailure?.source === "final" && !nonstreamFailure.sawRelayEvent;
          const safePreStreamFailure = stage === "start" || stage === "prelude";
          const safePreJsonFailure = stage === "start" || safeNonstreamFailure;
          const retry = attempt === 0 && preparedDecision === "hit" &&
            preparedMatch !== null && definitivePreviousResponseNotFound(error) &&
            (clientWantsStream ? safePreStreamFailure : safePreJsonFailure);
          if (retry && continuationIndex.compareDelete(preparedMatch!)) {
            completeIncomingAdmission();
            attempt += 1;
            continue;
          }
          throw error;
        } finally {
          if (!streamOwnsAttempt) attemptLink.unlink();
        }
      }
    } catch (error) {
      if (!requestLink.controller.signal.aborted) requestLink.controller.abort(error);
      return anthropicErrorResponse(error, requestId);
    } finally {
      completeIncomingAdmission();
      retainedIncomingLease?.release();
      retainedIncomingLease = undefined;
      if (!streamOwnsLink) requestLink.unlink();
    }
  });

  app.all("/v1/messages/count_tokens", c => methodNotAllowed(newAnthropicRequestId()));
  app.all("/v1/messages", c => methodNotAllowed(newAnthropicRequestId()));
}
