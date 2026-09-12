import type { Hono } from "hono";
import { BoundedPreparationScheduler } from "../relay/preparation-scheduler";
import type { RunHandle } from "../relay/run-handle";
import { prepareHostUpstreamResponsesRequest } from "../relay/host-request-policy";
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
import {
  AnthropicStreamTranslator,
  type AnthropicTerminalReplayProof,
} from "./stream";
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
import { jsonEncodedByteLengthAsync } from "../router/workload";

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
  /** Exact JSON bytes for the normalized body used by this relay attempt. */
  structuralBodyBytes?: number;
  /** Full preprocessing bytes charged only to fair scheduling. */
  fairnessBytes?: number;
  retainIncomingLeaseBytes?: boolean;
  retainMemoryUntil?: Promise<unknown>;
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
    source: "final" | "events" | "cancelled" | "unknown",
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
    let failureSource: "final" | "events" | "unknown" = winner.source;
    if (failureSource === "final") {
      let observationTimer: ReturnType<typeof setTimeout> | undefined;
      const observation = await Promise.race([
        drainOutcome.then(result => ({ settled: true as const, result })),
        new Promise<{ settled: false }>(resolve => {
          observationTimer = setTimeout(
            () => resolve({ settled: false }),
            NONSTREAM_CLEANUP_MS,
          );
          observationTimer.unref?.();
        }),
      ]);
      if (observationTimer !== undefined) clearTimeout(observationTimer);
      if (!observation.settled) failureSource = "unknown";
      else if (!observation.result.ok) failureSource = "events";
    }
    releaseFinalOutcome();
    if (!controller.signal.aborted) controller.abort(winner.error);
    returnIterator();
    await boundedCleanup([finalOutcome, drainOutcome]);
    onFailure(failureSource, sawRelayEvent);
    throw winner.error;
  }
  return winner.value;
}

/**
 * Charges only preprocessing memory not already covered by the HTTP ingress
 * lease. Production ingress reserves roughly two body copies plus scratch, so
 * retaining the parsed Anthropic body and translated Responses body while the
 * bounded digest scheduler runs must not be counted a second time. The queue's
 * count limit remains authoritative even when the byte cost is fully covered.
 *
 * @internal Exported for deterministic capacity proofs.
 */
export function continuationPreparationCostBytes(
  inboundBytes: number,
  relayBytes: number,
  reservedBytes = 0,
): number {
  for (const [name, value] of [
    ["inboundBytes", inboundBytes],
    ["relayBytes", relayBytes],
    ["reservedBytes", reservedBytes],
  ] as const) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError(`${name} must be a non-negative safe integer`);
    }
  }
  // Cooperative continuation preparation owns an immutable container snapshot
  // in addition to the inbound and translated bodies. Strings are shared, but
  // charging one full relay-sized copy conservatively bounds wide object graphs.
  const retainedBytes = inboundBytes + relayBytes + relayBytes;
  if (!Number.isSafeInteger(retainedBytes)) {
    throw new RangeError("continuation preparation bytes exceed the safe integer range");
  }
  return Math.max(
    1,
    // Reservation coverage reduces memory pressure, not cooperative CPU work.
    // Keep structural relay bytes visible so the scheduler can prioritize a
    // small continuation ahead of a large covered job.
    relayBytes,
    retainedBytes - Math.min(retainedBytes, reservedBytes),
  );
}

function continuationPreparationBytes(
  translated: ReturnType<typeof translateAnthropicRequest>,
  incomingLease: IncomingResourceLease | undefined,
): number {
  const leased = incomingLease?.bodyBytes ?? incomingLease?.reservedBytes;
  const inbound = Number.isSafeInteger(leased) && Number(leased) >= 0
    ? Number(leased)
    : translated.inboundBytes;
  const reserved = Number.isSafeInteger(incomingLease?.reservedBytes) &&
      Number(incomingLease?.reservedBytes) >= 0
    ? Number(incomingLease?.reservedBytes)
    : 0;
  return continuationPreparationCostBytes(inbound, translated.relayBytes, reserved);
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
  // This generic guard protects future translated/native callers even though
  // today's Anthropic request surface never synthesizes an explicit lineage.
  if (Object.hasOwn(translated.relayBody, "previous_response_id")) return null;
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
  "decision" | "inputDigest" | "settingsDigest" |
    "settingsCanonicalBytes" | "settingsCanonicalNodes" | "scopeDigest"
>;

async function commitContinuation(
  index: AnthropicContinuationIndex,
  prepared: AnthropicContinuationCommitToken | null,
  scope: AuthenticatedTenantScope | null,
  finalValue: unknown,
  projected: ReturnType<typeof responseToAnthropic>,
  signal: AbortSignal,
  terminalBytes: number,
  replayProof?: AnthropicTerminalReplayProof,
): Promise<void> {
  if (!prepared || scope === null || prepared.decision === "empty_suffix" ||
    prepared.decision === "explicit") return;
  try {
    const replay = projectAnthropicReplay(finalValue, projected, {
      ...replayProof,
      terminalBytes,
    });
    const responseId = terminalProviderResponseId(finalValue);
    if (!replay || !responseId) return;
    await index.commitAsync(prepared, scope, responseId, replay, signal);
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
  handle.discardUnconsumedEvents?.();
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
  continuationIndex.onShutdown(() => continuationScheduler.close());
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
    let finishRequestLifetime!: () => void;
    const requestLifetime = new Promise<void>(resolve => { finishRequestLifetime = resolve; });
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
      if (prepared) {
        // The immutable snapshot now owns both initial and fallback execution.
        // Drop the translated container graph; large strings remain shared.
        (translated as { relayBody: Record<string, unknown> }).relayBody =
          prepared.fullRelayBody;
      }
      const preparedDecision = prepared?.decision ?? null;
      const preparedMatch = prepared?.match ?? null;
      const commitToken: AnthropicContinuationCommitToken | null = prepared
        ? {
            decision: prepared.decision,
            inputDigest: prepared.inputDigest,
            settingsDigest: prepared.settingsDigest,
            settingsCanonicalBytes: prepared.settingsCanonicalBytes,
            settingsCanonicalNodes: prepared.settingsCanonicalNodes,
            scopeDigest: prepared.scopeDigest,
          }
        : null;
      if (preparedDecision === "hit") retainedIncomingLease = incomingLease;
      const continuationLease = retainedIncomingLease
        ? incomingLeaseTimingView(retainedIncomingLease)
        : undefined;
      const initialRelayBody = prepared?.relayBody ?? translated.relayBody;
      let fallbackRelayBody: Record<string, unknown> | null =
        prepared?.fullRelayBody ?? translated.relayBody;
      const clientWantsStream = translated.stream;
      let attempt = 0;

      for (;;) {
        const attemptLink = linkedAbortController(requestLink.controller.signal);
        let relayBody: Record<string, unknown> = prepareHostUpstreamResponsesRequest(
          attempt === 0 ? initialRelayBody : fallbackRelayBody!,
        );
        let handle: RunHandle | null = null;
        let stream: ReadableStream<Uint8Array> | null = null;
        let streamOwnsAttempt = false;
        let stage: "start" | "nonstream" | "projection" | "prelude" | "sse-commit" = "start";
        const nonstreamFailure: {
          source: "final" | "events" | "cancelled" | "unknown" | null;
          sawRelayEvent: boolean;
        } = { source: null, sawRelayEvent: false };
        try {
          const structuralBodyBytes = await jsonEncodedByteLengthAsync(relayBody, {
            signal: attemptLink.controller.signal,
          });
          const attemptIncomingLease = attempt === 0
            ? preparedDecision === "hit" ? continuationLease : incomingLease
            : retainedIncomingLease;
          handle = await dependencies.startRelay(
            relayBody,
            attemptLink.controller.signal,
            clientId,
            attemptIncomingLease,
            {
              structuralBodyBytes,
              ...(preparedDecision === "hit" ? {
                  fairnessBytes: Math.max(
                    incomingLease?.bodyBytes ?? 0,
                    translated.inboundBytes,
                    translated.relayBytes,
                    commitToken!.inputDigest.canonicalBytes,
                  ),
                  retainIncomingLeaseBytes: attempt > 0,
                } : {}),
              ...(preparedDecision !== "hit" || attempt > 0
                ? { retainMemoryUntil: requestLifetime }
                : {}),
            },
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
              await commitContinuation(
                continuationIndex,
                handle.modelFallback ? null : commitToken,
                scope,
                final,
                response,
                attemptLink.controller.signal,
                handle.stats.bytes,
              );
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
              beforeMessageStopAsync: async (final, response, replayProof) => {
                await commitContinuation(
                  continuationIndex,
                  handle!.modelFallback ? null : commitToken,
                  scope,
                  final,
                  response,
                  attemptLink.controller.signal,
                  handle!.stats.bytes,
                  replayProof,
                );
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
            preparedMatch !== null && !requestLink.controller.signal.aborted &&
            definitivePreviousResponseNotFound(error) &&
            (clientWantsStream ? safePreStreamFailure : safePreJsonFailure);
          if (retry) {
            // Exact compare-delete protects a concurrently replaced generation,
            // but every request with its own definitive pre-event rejection is
            // independently safe to retry once with its immutable full history.
            continuationIndex.compareDelete(preparedMatch!);
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
      setImmediate(finishRequestLifetime);
    }
  });

  app.all("/v1/messages/count_tokens", _c => methodNotAllowed(newAnthropicRequestId()));
  app.all("/v1/messages", _c => methodNotAllowed(newAnthropicRequestId()));
}
