import {
  ResponsesHttpContinuationIndex,
  responsesContinuationPreparationCostBytes,
} from "../http/responses-continuation";
import { Hono } from "hono";
import type { RunHandle } from "../relay/run-handle";
import { definitivePreviousResponseNotFound } from "../relay/continuation-error";
import { normalizeHostUpstreamResponsesRequest as normalizeUpstreamResponsesRequest } from "../relay/host-upstream-policy";
import { validateResponsesInputJson } from "../relay/responses-validation";
import { sseFrame, SSE_HEADERS, type SseFrame } from "../relay/sse";
import { ApiError, apiErrorEvent, apiErrorResponse, normalizeApiError } from "../http/errors";
import { registerImageRoutes } from "../images/routes";
import type { ImageCompatOptions } from "../images/compat";
import { registerAnthropicRoutes } from "../anthropic/routes";
import { AnthropicContinuationIndex } from "../anthropic/continuation";
import {
  anthropicModelsFromEnv,
  type AnthropicModelOptions,
} from "../anthropic/model";
import type { ResponseProvenanceLookup } from "../router/provenance";
import {
  jsonEncodedByteLengthAsync,
} from "../router/workload";
import type { IncomingResourceLease } from "../router/router";
import {
  incomingLeaseTimingView,
  linkIncomingLeaseAbort,
  resolveHttpClientId,
  takeIncomingBodyLease,
} from "../http/request-lease";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import {
  safeV2ReleaseIdentity,
  summarizeV2Releases,
} from "../v2/release-observability";
import type { FleetSnapshot } from "../fleet/types";
import {
  apiKeyAuthorized,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";
import { isAnthropicRequestPath } from "../http/request-policy";
import {
  AnthropicRequestError,
  anthropicErrorResponse,
  newAnthropicRequestId,
} from "../anthropic/errors";

export interface RouterLike {
  startRelay(
    body: Record<string, unknown>,
    options?: {
      clientId?: string;
      source?: "http" | "websocket" | "image" | "anthropic" | "internal";
      signal?: AbortSignal;
      incomingLease?: IncomingResourceLease;
      /** Trusted server-derived tenant/provider-account scope; never request data. */
      authorizationScope?: AuthenticatedTenantScope;
      encodedBodyBytes?: number;
      structuralBodyBytes?: number;
      fairnessRequestBytes?: number;
      retainIncomingLeaseBytes?: boolean;
      retainMemoryUntil?: Promise<unknown>;
    },
  ): Promise<RunHandle>;
  acquireIncomingBody?(
    bytes: number,
    signal?: AbortSignal,
    options?: { maxBytes?: number; reservationMultiplier?: number },
  ): Promise<IncomingResourceLease>;
  responseProvenanceAuthorizationScope?(): AuthenticatedTenantScope | null;
  lookupResponseProvenance?(
    responseId: unknown,
    authorizationScope?: AuthenticatedTenantScope,
  ): ResponseProvenanceLookup;
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
  /** Omitted only by narrow unit-test/library seams. Production always sets it. */
  apiKey?: string;
  fleetSnapshot?: () => FleetSnapshot;
  imageOptions?: ImageCompatOptions;
  anthropicModels?: AnthropicModelOptions;
  /** One bounded, content-free native Responses full-history index. */
  responsesContinuationIndex?: ResponsesHttpContinuationIndex;
  /** One bounded, content-free Claude continuation index owned by this app. */
  anthropicContinuationIndex?: AnthropicContinuationIndex;
  /** Trusted server-derived tenant/provider-account scope; never request data. */
  authorizationScope?: AuthenticatedTenantScope;
  keepAliveMs?: number;
}

const CODEX_BASE_INSTRUCTIONS =
  "You are Codex, an expert agentic coding assistant working in a shared workspace. " +
  "Follow system, developer, and user instructions in priority order. Work autonomously, " +
  "preserve existing user changes, use tools safely, communicate concise progress, and " +
  "run relevant verification before claiming completion.";

const REASONING_DESCRIPTIONS = {
  low: "Fast responses with lighter reasoning",
  medium: "Balances speed and reasoning depth for everyday tasks",
  high: "Greater reasoning depth for complex problems",
  xhigh: "Extra high reasoning depth for complex problems",
  max: "Maximum reasoning depth for the hardest problems",
  ultra: "Maximum reasoning with automatic task delegation",
} as const;

const RESPONSE_TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.incomplete",
  "response.failed",
]);
const MAX_TIMER_MS = 2_147_483_647;
const MAX_SYNCHRONOUS_STRUCTURAL_MEASURE_BYTES = 256 * 1024;
const MAX_PUBLIC_UNAVAILABLE_PROJECTS = 256;
const PUBLIC_PROJECT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const RUNTIME_PRIVATE_KEYS = new Set([
  "token", "cookie", "authorization", "sessionid", "sandboxsessionid",
  "instanceid", "resourcetoken", "ysweettoken",
]);

function sanitizeRuntimeValue(
  value: unknown,
  depth = 0,
  seen = new WeakSet<object>(),
): unknown {
  if (depth > 16) return null;
  if (Array.isArray(value)) return value.map(child => sanitizeRuntimeValue(child, depth + 1, seen));
  if (!value || typeof value !== "object") return value;
  if (seen.has(value)) return null;
  seen.add(value);
  try {
    const output: Record<string, unknown> = {};
    for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
      const normalized = key.replaceAll("_", "").toLowerCase();
      if (RUNTIME_PRIVATE_KEYS.has(normalized)) continue;
      output[key] = sanitizeRuntimeValue(child, depth + 1, seen);
    }
    return output;
  } finally {
    seen.delete(value);
  }
}

function publicRuntimeSnapshot(value: unknown): Record<string, unknown> {
  const raw = sanitizeRuntimeValue(value) as Record<string, unknown> ?? {};
  const release = safeV2ReleaseIdentity(raw);
  raw.releaseRevision = release?.releaseRevision ?? null;
  raw.protocolRevision = release?.protocolRevision ?? null;
  raw.buildHash = release?.buildHash ?? null;
  raw.schemaHash = release?.schemaHash ?? null;
  return raw;
}

function publicFleetState(
  value: FleetSnapshot | undefined,
  runtimeCount: number,
): {
  desired: number;
  ready: number;
  unavailable: number;
  unavailableProjects: string[];
  unavailableProjectsTruncated: boolean;
} {
  if (!value) {
    return {
      desired: runtimeCount,
      ready: runtimeCount,
      unavailable: 0,
      unavailableProjects: [],
      unavailableProjectsTruncated: false,
    };
  }
  const all = value.unavailableProjects
    .filter((id): id is typeof value.unavailableProjects[number] => PUBLIC_PROJECT_ID.test(id));
  return {
    desired: value.desired,
    ready: value.ready,
    unavailable: value.unavailable,
    unavailableProjects: all.slice(0, MAX_PUBLIC_UNAVAILABLE_PROJECTS),
    unavailableProjectsTruncated: all.length > MAX_PUBLIC_UNAVAILABLE_PROJECTS,
  };
}

function terminalEventForFinal(final: unknown): string {
  if (!final || typeof final !== "object" || Array.isArray(final)) {
    throw new Error("Responses relay returned a malformed final response");
  }
  const status = (final as Record<string, unknown>).status;
  if (status === "completed" || status === "incomplete" || status === "failed") {
    return `response.${status}`;
  }
  throw new Error("Responses relay returned an invalid final status");
}

function nextResponseSequence(data: string, current: number | null): number | null {
  // Never parse a multi-MiB delta a second time solely to decorate a rare
  // final-only fallback. Provider Responses events are a contiguous sequence,
  // so advance the already-validated stream cursor without retaining/parsing
  // the large payload. This also prevents a synthesized terminal from reusing
  // the large event's sequence number.
  if (data.length > 64 * 1024) {
    return current === null ? 1 : current < Number.MAX_SAFE_INTEGER ? current + 1 : null;
  }
  let value: unknown;
  try {
    value = JSON.parse(data);
  } catch {
    return current;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return current;
  const sequence = (value as Record<string, unknown>).sequence_number;
  if (!Number.isSafeInteger(sequence) || Number(sequence) < 0 ||
      Number(sequence) >= Number.MAX_SAFE_INTEGER) {
    return current;
  }
  const next = Number(sequence) + 1;
  return current === null ? next : Math.max(current, next);
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

const NONSTREAM_CLEANUP_MS = 25;

type Settled<T> = { ok: true; value: T } | { ok: false; error: unknown };

function settle<T>(promise: Promise<T>): Promise<Settled<T>> {
  return promise.then(
    value => ({ ok: true as const, value }),
    error => ({ ok: false as const, error }),
  );
}

async function nonStreamingFinal(
  handle: RunHandle,
  controller: AbortController,
  onFailure: (
    source: "final" | "events" | "unknown",
    sawRelayEvent: boolean,
  ) => void = () => undefined,
): Promise<any> {
  const iterator = handle.events();
  let sawRelayEvent = false;
  let drainSettled = false;
  const drain = (async () => {
    try {
      while (!(await iterator.next()).done) sawRelayEvent = true;
    } finally {
      drainSettled = true;
    }
  })();
  let finalPromise: Promise<any>;
  try { finalPromise = Promise.resolve(handle.final); }
  catch (error) { finalPromise = Promise.reject(error); }
  const finalOutcome = settle(finalPromise);
  const drainOutcome = settle(drain);
  let returnCalled = false;
  const returnIterator = (): void => {
    if (returnCalled) return;
    returnCalled = true;
    try { void Promise.resolve(iterator.return(undefined)).catch(() => undefined); }
    catch {}
  };
  const releaseLateFinal = (): void => {
    void finalOutcome.then(outcome => {
      if (outcome.ok) releaseAtomicBytePermit(outcome.value);
    });
  };
  const waitForCancellation = (): {
    promise: Promise<{ cancelled: true; error: unknown }>;
    dispose(): void;
  } => {
    let listener: (() => void) | null = null;
    const promise = new Promise<{ cancelled: true; error: unknown }>(resolve => {
      const finish = () => resolve({
        cancelled: true,
        error: controller.signal.reason ?? new DOMException("Request was cancelled", "AbortError"),
      });
      if (controller.signal.aborted) finish();
      else {
        listener = finish;
        controller.signal.addEventListener("abort", finish, { once: true });
      }
    });
    return {
      promise,
      dispose() {
        if (listener) controller.signal.removeEventListener("abort", listener);
        listener = null;
      },
    };
  };
  const cancellation = waitForCancellation();
  const first = await Promise.race([
    finalOutcome.then(result => ({ source: "final" as const, result })),
    drainOutcome.then(result => ({ source: "events" as const, result })),
    cancellation.promise,
  ]);
  cancellation.dispose();
  if ("cancelled" in first) {
    releaseLateFinal();
    returnIterator();
    onFailure("unknown", sawRelayEvent);
    throw first.error;
  }

  let failure: { source: "final" | "events" | "unknown"; error: unknown } | null = null;
  let value: any = null;
  if (!first.result.ok) {
    failure = { source: first.source, error: first.result.error };
  } else {
    const second = first.source === "final" ? drainOutcome : finalOutcome;
    const secondCancellation = waitForCancellation();
    const secondResult = await Promise.race([second, secondCancellation.promise]);
    secondCancellation.dispose();
    if ("cancelled" in secondResult) {
      releaseLateFinal();
      returnIterator();
      onFailure("unknown", sawRelayEvent);
      throw secondResult.error;
    }
    if (!secondResult.ok) {
      failure = {
        source: first.source === "final" ? "events" : "final",
        error: secondResult.error,
      };
    } else {
      value = first.source === "final" ? first.result.value : secondResult.value;
    }
  }
  if (!failure) return value;

  if (failure.source === "final" && !drainSettled) {
    // Let an already-accepted event become observable before cancellation. If
    // the read remains pending, absence is unproven and replay stays disabled.
    const observation = await Promise.race([
      drainOutcome.then(result => ({ settled: true as const, result })),
      new Promise<{ settled: false }>(resolve => setTimeout(
        () => resolve({ settled: false }),
        0,
      )),
    ]);
    if (!observation.settled) failure.source = "unknown";
    else if (!observation.result.ok) failure.source = "events";
  }
  releaseLateFinal();
  if (!controller.signal.aborted) controller.abort(failure.error);
  returnIterator();
  let cleanupTimer: ReturnType<typeof setTimeout> | undefined;
  await Promise.race([
    Promise.all([finalOutcome, drainOutcome]).then(() => undefined),
    new Promise<void>(resolve => {
      cleanupTimer = setTimeout(resolve, NONSTREAM_CLEANUP_MS);
      cleanupTimer.unref?.();
    }),
  ]);
  if (cleanupTimer !== undefined) clearTimeout(cleanupTimer);
  onFailure(failure.source, sawRelayEvent);
  throw failure.error;
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

/**
 * Lazily serialize a non-stream response while its terminal allocation is
 * still charged, then hand that same charge to the exact bytes written by the
 * Node adapter. A second pull proves the first chunk drained for Fetch-only
 * consumers; the adapter releases it earlier on the write callback.
 */
function permittedJsonResponse(final: Record<string, unknown>): Response {
  let chunk: Uint8Array | null = null;
  let enqueued = false;
  let closed = false;
  const release = () => {
    releaseAtomicBytePermit(chunk);
    chunk = null;
    releaseAtomicBytePermit(final);
  };
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (closed) return;
      if (enqueued) {
        closed = true;
        release();
        controller.close();
        return;
      }
      try {
        const json = JSON.stringify(final);
        chunk = new TextEncoder().encode(json);
        transferAtomicBytePermit(final, chunk, "http-json-chunk");
        controller.enqueue(chunk);
        enqueued = true;
      } catch (error) {
        closed = true;
        release();
        controller.error(error);
      }
    },
    cancel() {
      if (closed) return;
      closed = true;
      release();
    },
  }, { highWaterMark: 0 });
  try {
    return new Response(stream, {
      headers: { "content-type": "application/json; charset=UTF-8" },
    });
  } catch (error) {
    release();
    throw error;
  }
}

function responseEventStream(
  handle: RunHandle,
  keepAliveMs: number,
  options: {
    cancelRelay(reason?: unknown): void;
    close(): void;
    signal: AbortSignal;
  },
): ReadableStream<Uint8Array> {
  type NextOutcome = { kind: "next"; result: IteratorResult<SseFrame, void> };
  type CancelledOutcome = { kind: "cancelled" };
  type AbortedOutcome = { kind: "aborted"; reason: unknown };
  const encoder = new TextEncoder();
  const iterator = handle.events();
  let deliveredChunk: Uint8Array | null = null;
  let dequeuedFrame: SseFrame | null = null;
  let pendingNext: Promise<NextOutcome> | null = null;
  let sawTerminal = false;
  let nextSequence: number | null = null;
  let finished = false;
  let cancelled = false;
  let wakeCancellation: (() => void) | null = null;
  let finalForCleanup: Record<string, unknown> | null = null;
  let closeAfterDeliveredChunk = false;
  let streamController: ReadableStreamDefaultController<Uint8Array> | null = null;
  let resolveAborted!: (value: AbortedOutcome) => void;
  const aborted = new Promise<AbortedOutcome>(resolve => { resolveAborted = resolve; });
  const onAbort = () => {
    resolveAborted({ kind: "aborted", reason: options.signal.reason });
    const controller = streamController;
    if (!controller || finished || cancelled) return;
    // A Fetch consumer is allowed to never pull the returned body. Abort must
    // still release the final/event permits and iterator immediately rather
    // than waiting for a pull or explicit body.cancel() that may never happen.
    discardAndReturnIterator();
    finish(controller);
  };

  const discardAndReturnIterator = (): void => {
    handle.discardUnconsumedEvents?.();
    void iterator.return(undefined).catch(() => undefined);
  };

  void handle.final.then(final => {
    if (finished || cancelled) releaseAtomicBytePermit(final);
    else finalForCleanup = final;
  }, () => undefined);

  const cancellationWaiter = (): {
    promise: Promise<CancelledOutcome>;
    dispose(): void;
  } => {
    if (cancelled) {
      return { promise: Promise.resolve({ kind: "cancelled" }), dispose() {} };
    }
    let wake!: () => void;
    const promise = new Promise<CancelledOutcome>(resolve => {
      wake = () => resolve({ kind: "cancelled" });
      wakeCancellation = wake;
    });
    return {
      promise,
      dispose() {
        if (wakeCancellation === wake) wakeCancellation = null;
      },
    };
  };

  const finish = (controller: ReadableStreamDefaultController<Uint8Array>): void => {
    if (finished) return;
    finished = true;
    releaseAtomicBytePermit(deliveredChunk);
    deliveredChunk = null;
    releaseAtomicBytePermit(dequeuedFrame);
    dequeuedFrame = null;
    releaseAtomicBytePermit(finalForCleanup);
    finalForCleanup = null;
    options.signal.removeEventListener("abort", onAbort);
    options.close();
    if (!cancelled) controller.close();
  };

  const fail = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    error: unknown,
  ): void => {
    if (finished || cancelled) return;
    try {
      controller.enqueue(encoder.encode(sseFrame("error", apiErrorEvent(error))));
    } finally {
      discardAndReturnIterator();
      finish(controller);
      // finish() removes the request-abort listener first. Aborting relay work
      // before the public error is queued would synchronously close this stream
      // and silently drop the committed HTTP-200 terminal error frame.
      options.cancelRelay(error);
    }
  };

  const nextOrKeepAlive = async (): Promise<
    | NextOutcome
    | { kind: "keepalive" }
    | CancelledOutcome
    | AbortedOutcome
  > => {
    const next = pendingNext ??= iterator.next().then(result => {
      if (!result.done) {
        if (cancelled || finished) {
          releaseAtomicBytePermit(result.value);
        } else {
          // Own a frame as soon as iterator.next resolves. Cancellation may run
          // before the awaiting pull continuation is scheduled.
          releaseAtomicBytePermit(dequeuedFrame);
          dequeuedFrame = result.value;
        }
      }
      return { kind: "next" as const, result };
    });
    const cancellation = cancellationWaiter();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const keepAlive = new Promise<{ kind: "keepalive" }>(resolve => {
      timer = setTimeout(() => resolve({ kind: "keepalive" }), keepAliveMs);
      timer.unref?.();
    });
    try {
      const result = await Promise.race([
        next,
        keepAlive,
        cancellation.promise,
        aborted,
      ]);
      if (result.kind === "next" && pendingNext === next) pendingNext = null;
      return result;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      cancellation.dispose();
    }
  };

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      streamController = controller;
    },
    async pull(controller) {
      if (finished || cancelled) return;
      // highWaterMark=0 means a new pull proves the previous chunk left the
      // stream queue. Node's adapter releases earlier on its write callback.
      releaseAtomicBytePermit(deliveredChunk);
      deliveredChunk = null;
      if (closeAfterDeliveredChunk) {
        finish(controller);
        return;
      }
      try {
        const outcome = await nextOrKeepAlive();
        if (cancelled || outcome.kind === "cancelled") return;
        if (outcome.kind === "aborted") {
          discardAndReturnIterator();
          finish(controller);
          return;
        }
        if (outcome.kind === "keepalive") {
          controller.enqueue(encoder.encode(": keepalive\n\n"));
          return;
        }

        if (!outcome.result.done) {
          const frame = outcome.result.value;
          if (RESPONSE_TERMINAL_EVENTS.has(frame.event)) {
            // A terminal transcript frame can reach the bounded event queue just
            // before the authoritative final/provenance Promise settles. Never
            // expose it until that commit is observable through handle.final.
            const cancellation = cancellationWaiter();
            const finalOutcome = await Promise.race([
              handle.final.then(() => ({ kind: "final" as const })),
              cancellation.promise,
              aborted,
            ]).finally(cancellation.dispose);
            if (cancelled || finalOutcome.kind === "cancelled") return;
            if (finalOutcome.kind === "aborted") {
              discardAndReturnIterator();
              finish(controller);
              return;
            }
            sawTerminal = true;
          }
          nextSequence = nextResponseSequence(frame.data, nextSequence);
          let encoded: Uint8Array | null = null;
          try {
            encoded = encoder.encode(sseFrame(frame.event, frame.data));
            transferAtomicBytePermit(frame, encoded, "http-sse-chunk");
            if (dequeuedFrame === frame) dequeuedFrame = null;
            controller.enqueue(encoded);
            deliveredChunk = encoded;
          } catch (error) {
            releaseAtomicBytePermit(encoded);
            releaseAtomicBytePermit(frame);
            if (dequeuedFrame === frame) dequeuedFrame = null;
            throw error;
          }
          return;
        }

        if (!sawTerminal) {
          const cancellation = cancellationWaiter();
          const finalOutcome = await Promise.race([
            handle.final.then(value => ({ kind: "final" as const, value })),
            cancellation.promise,
            aborted,
          ]).finally(cancellation.dispose);
          if (cancelled || finalOutcome.kind === "cancelled") return;
          if (finalOutcome.kind === "aborted") {
            discardAndReturnIterator();
            finish(controller);
            return;
          }
          const event = terminalEventForFinal(finalOutcome.value);
          let encoded: Uint8Array | null = null;
          try {
            encoded = encoder.encode(sseFrame(event, {
              type: event,
              ...(nextSequence === null ? {} : { sequence_number: nextSequence }),
              response: finalOutcome.value,
            }));
            transferAtomicBytePermit(
              finalOutcome.value,
              encoded,
              "http-sse-final-chunk",
            );
            controller.enqueue(encoded);
            deliveredChunk = encoded;
            closeAfterDeliveredChunk = true;
          } catch (error) {
            releaseAtomicBytePermit(encoded);
            releaseAtomicBytePermit(finalOutcome.value);
            throw error;
          }
          return;
        }
        finish(controller);
      } catch (error) {
        fail(controller, error);
      }
    },
    cancel(reason) {
      if (cancelled || finished) return;
      cancelled = true;
      finished = true;
      releaseAtomicBytePermit(deliveredChunk);
      deliveredChunk = null;
      releaseAtomicBytePermit(dequeuedFrame);
      dequeuedFrame = null;
      releaseAtomicBytePermit(finalForCleanup);
      finalForCleanup = null;
      options.signal.removeEventListener("abort", onAbort);
      wakeCancellation?.();
      wakeCancellation = null;
      options.cancelRelay(reason);
      options.close();
      discardAndReturnIterator();
    },
  }, { highWaterMark: 0 });
  options.signal.addEventListener("abort", onAbort, { once: true });
  if (options.signal.aborted) onAbort();
  return stream;
}

function codexModel(input: {
  slug: string;
  displayName: string;
  description: string;
  defaultReasoning: keyof typeof REASONING_DESCRIPTIONS;
  reasoning: readonly (keyof typeof REASONING_DESCRIPTIONS)[];
  priority: number;
  maxContextWindow?: number;
  compHash?: string;
  useResponsesLite?: boolean;
  multiAgentVersion?: "v1" | "v2";
}) {
  return {
    slug: input.slug,
    display_name: input.displayName,
    description: input.description,
    base_instructions: CODEX_BASE_INSTRUCTIONS,
    default_reasoning_level: input.defaultReasoning,
    supported_reasoning_levels: input.reasoning.map(effort => ({
      effort,
      description: REASONING_DESCRIPTIONS[effort],
    })),
    shell_type: "shell_command",
    visibility: "list",
    supported_in_api: true,
    priority: input.priority,
    additional_speed_tiers: [],
    service_tiers: [{
      id: "priority",
      name: "Fast",
      description: "Lower-latency priority processing",
    }],
    availability_nux: null,
    upgrade: null,
    include_skills_usage_instructions: true,
    include_plugin_usage_instructions: true,
    include_apps_usage_instructions: true,
    default_reasoning_summary: "none",
    support_verbosity: true,
    default_verbosity: "low",
    apply_patch_tool_type: "freeform",
    web_search_tool_type: "text_and_image",
    truncation_policy: { mode: "tokens", limit: 10_000 },
    supports_image_detail_original: true,
    context_window: 272_000,
    max_context_window: input.maxContextWindow ?? 872_000,
    comp_hash: input.compHash ?? "3000",
    effective_context_window_percent: 95,
    experimental_supported_tools: [],
    input_modalities: ["text", "image"],
    supports_search_tool: true,
    use_responses_lite: input.useResponsesLite ?? true,
    node_repl_auto_review_required: false,
    node_repl_disabled: false,
    tool_mode: "code_mode_only",
    multi_agent_version: input.multiAgentVersion ?? "v1",
  };
}

const CODEX_MODELS = Object.freeze([
  codexModel({
    slug: "gpt-5.6-sol",
    displayName: "GPT-5.6-Sol",
    description: "Latest frontier agentic coding model.",
    defaultReasoning: "max",
    reasoning: ["low", "medium", "high", "xhigh", "max", "ultra"],
    priority: 1,
    multiAgentVersion: "v2",
  }),
  codexModel({
    slug: "gpt-5.6-terra",
    displayName: "GPT-5.6-Terra",
    description: "Balanced agentic coding model for everyday work.",
    defaultReasoning: "medium",
    reasoning: ["low", "medium", "high", "xhigh", "max", "ultra"],
    priority: 2,
    multiAgentVersion: "v2",
  }),
  codexModel({
    slug: "gpt-5.6-luna",
    displayName: "GPT-5.6-Luna",
    description: "Fast and affordable agentic coding model.",
    defaultReasoning: "medium",
    reasoning: ["low", "medium", "high", "xhigh", "max"],
    priority: 3,
  }),
  codexModel({
    slug: "gpt-5.5",
    displayName: "GPT-5.5",
    description: "Frontier model for complex coding, research, and real-world work.",
    defaultReasoning: "medium",
    reasoning: ["low", "medium", "high", "xhigh"],
    priority: 7,
    maxContextWindow: 272_000,
    compHash: "2911",
    useResponsesLite: false,
  }),
]);

const GHOSTPROV_APP_AUTHORIZATION_SCOPES = new WeakMap<
  object,
  AuthenticatedTenantScope | null
>();

export function ghostProvAppAuthorizationScope(
  app: object,
): AuthenticatedTenantScope | null | undefined {
  return GHOSTPROV_APP_AUTHORIZATION_SCOPES.get(app);
}

export function createGhostProvApp(options: GhostProvAppOptions): Hono {
  const app = new Hono();
  const keepAliveMs = options.keepAliveMs ?? 15_000;
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
  const scopeValue = options.authorizationScope ?? routerScope;
  if (scopeValue !== null && !isAuthenticatedTenantScope(scopeValue)) {
    throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
  }
  const authorizationScope = scopeValue;
  GHOSTPROV_APP_AUTHORIZATION_SCOPES.set(app, authorizationScope);
  const responsesContinuationIndex = options.responsesContinuationIndex ??
    new ResponsesHttpContinuationIndex();
  const anthropicContinuationIndex = options.anthropicContinuationIndex ??
    new AnthropicContinuationIndex();
  if (!Number.isSafeInteger(keepAliveMs) || keepAliveMs < 1 || keepAliveMs > MAX_TIMER_MS) {
    throw new RangeError(`keepAliveMs must be an integer from 1 through ${MAX_TIMER_MS}`);
  }

  if (options.apiKey) {
    app.use("/v1/*", async (c, next) => {
      const anthropic = isAnthropicRequestPath(c.req.path);
      const authorized = apiKeyAuthorized({
        authorization: c.req.header("authorization"),
        xApiKey: c.req.header("x-api-key"),
      }, options.apiKey!, anthropic ? "anthropic" : "openai");
      if (authorized) return next();
      if (anthropic) {
        return anthropicErrorResponse(
          new AnthropicRequestError(401, "Invalid API key"),
          newAnthropicRequestId(),
        );
      }
      const response = apiErrorResponse(new ApiError(401, "Invalid API key", {
        type: "authentication_error",
        code: "invalid_api_key",
      }));
      response.headers.set("www-authenticate", 'Bearer realm="ghostprov"');
      return response;
    });
  }

  app.get("/health", c => {
    const snapshot = options.router.snapshot();
    const runtimes = snapshot.runtimes.map(publicRuntimeSnapshot);
    const fleet = publicFleetState(options.fleetSnapshot?.(), runtimes.length);
    const healthy = snapshot.capacity > 0;
    return c.json({
      ok: healthy,
      capacity: snapshot.capacity,
      active: snapshot.active,
      available: snapshot.available,
      queued: snapshot.queued,
      runtimes: runtimes.length,
      ...fleet,
      release: summarizeV2Releases(runtimes),
    }, healthy ? 200 : 503);
  });

  app.get("/__sandboxes", c => {
    const snapshot = options.router.snapshot();
    const runtimes = snapshot.runtimes.map(publicRuntimeSnapshot);
    const fleet = publicFleetState(options.fleetSnapshot?.(), runtimes.length);
    return c.json({
      capacity: snapshot.capacity,
      active: snapshot.active,
      available: snapshot.available,
      queued: snapshot.queued,
      runtimes,
      ...fleet,
      release: summarizeV2Releases(runtimes),
      resources: snapshot.resources,
    });
  });

  app.get("/v1/models", c => c.json({
    object: "list",
    data: [
      { id: "gpt-5.6-sol", object: "model", created: 0, owned_by: "ghostprov" },
      { id: "gpt-5.6-terra", object: "model", created: 0, owned_by: "ghostprov" },
      { id: "gpt-5.6-luna", object: "model", created: 0, owned_by: "ghostprov" },
      { id: "gpt-5.5", object: "model", created: 0, owned_by: "ghostprov" },
      { id: "gpt-image-2", object: "model", created: 0, owned_by: "ghostprov" },
      { id: "gpt-image-2-2026-04-21", object: "model", created: 0, owned_by: "ghostprov" },
    ],
    models: CODEX_MODELS,
  }));


  app.post("/v1/responses", async c => {
    let parsedBody: unknown;
    try {
      parsedBody = await c.req.json();
    } catch {
      return c.json({ error: { message: "invalid JSON body" } }, 400);
    }
    if (parsedBody === null || typeof parsedBody !== "object" || Array.isArray(parsedBody)) {
      return apiErrorResponse(new ApiError(400, "Request body must be a JSON object.", {
        type: "invalid_request_error",
        param: "body",
        code: "invalid_type",
      }));
    }
    let body = parsedBody as Record<string, unknown>;
    const clientWantsStream = body.stream === true;
    const incomingLease = takeIncomingBodyLease(c.env);
    const requestSignal = c.req.raw.signal;
    const relayAbort = new AbortController();
    const abortRelay = () => relayAbort.abort(requestSignal.reason);
    if (requestSignal.aborted) abortRelay();
    else requestSignal.addEventListener("abort", abortRelay, { once: true });
    const unlinkIncomingSignal = linkIncomingLeaseAbort(relayAbort, incomingLease);
    let rawStructuralBytes: number;
    let upstreamBody: Record<string, unknown>;
    let upstreamStructuralBytes: number;
    try {
      const leasedBodyBytes = Number.isSafeInteger(incomingLease?.bodyBytes) &&
          Number(incomingLease?.bodyBytes) >= 0
        ? Number(incomingLease!.bodyBytes)
        : null;
      upstreamBody = normalizeUpstreamResponsesRequest(body);
      upstreamStructuralBytes = await jsonEncodedByteLengthAsync(upstreamBody, {
        signal: relayAbort.signal,
      });
      // Validate the complete client history before advisory prefix inference
      // can strip it. The router will validate the selected suffix again.
      validateResponsesInputJson(upstreamBody);
      rawStructuralBytes = upstreamBody === body
        ? upstreamStructuralBytes
        : leasedBodyBytes === null ||
            leasedBodyBytes <= MAX_SYNCHRONOUS_STRUCTURAL_MEASURE_BYTES
          ? await jsonEncodedByteLengthAsync(body, { signal: relayAbort.signal })
          : upstreamStructuralBytes;
    } catch (error) {
      unlinkIncomingSignal();
      requestSignal.removeEventListener("abort", abortRelay);
      incomingLease?.release();
      return apiErrorResponse(error);
    }
    let incomingAdmissionCompleted = false;
    const completeIncomingAdmission = () => {
      if (incomingAdmissionCompleted) return;
      incomingAdmissionCompleted = true;
      incomingLease?.completeAdmission?.();
      unlinkIncomingSignal();
    };
    let incomingLeaseOwned = true;
    const releaseIncomingLease = () => {
      if (!incomingLeaseOwned) return;
      incomingLeaseOwned = false;
      incomingLease?.release();
    };
    let streamOwnsCleanup = false;
    let finishRequestLifetime!: () => void;
    const requestLifetime = new Promise<void>(resolve => { finishRequestLifetime = resolve; });
    const cleanupSignal = () => requestSignal.removeEventListener("abort", abortRelay);
    try {
      const preparation = authorizationScope === null
        ? null
        : await responsesContinuationIndex.prepare(
            upstreamBody,
            authorizationScope,
            relayAbort.signal,
            responsesContinuationPreparationCostBytes(
              incomingLease?.bodyBytes ?? upstreamStructuralBytes,
              incomingLease?.reservedBytes ?? 0,
              upstreamStructuralBytes,
            ),
          );
      const continuationToken = preparation?.token ?? null;
      if (preparation) {
        upstreamBody = preparation.fullRelayBody;
        body = {};
      }
      const initialRelayBody = preparation?.relayBody ?? upstreamBody;
      const hit = continuationToken?.decision === "hit";
      const continuationLease = hit ? incomingLeaseTimingView(incomingLease) : undefined;
      if (!hit) incomingLeaseOwned = false;
      const clientId = resolveHttpClientId(c.env, c.req.header("x-ghostprov-client-id"));
      let attempt = 0;

      for (;;) {
        const attemptLink = linkedAbortController(relayAbort.signal);
        let relayBody = attempt === 0 ? initialRelayBody : upstreamBody;
        const relayStructuralBytes = relayBody === upstreamBody
          ? upstreamStructuralBytes
          : await jsonEncodedByteLengthAsync(relayBody, {
              signal: attemptLink.controller.signal,
            });
        const encodedBodyBytes = typeof incomingLease?.bodyBytes === "number"
          ? Math.min(
              Number.MAX_SAFE_INTEGER,
              Math.max(0, incomingLease.bodyBytes - rawStructuralBytes) + relayStructuralBytes,
            )
          : undefined;
        let rawHandle: RunHandle | null = null;
        let stream: ReadableStream<Uint8Array> | null = null;
        let streamOwnsAttempt = false;
        let stage: "start" | "nonstream" | "prelude" | "sse-commit" = "start";
        let sawRelayEvent = false;
        let nonstreamFailureSource: "final" | "events" | "unknown" | null = null;
        try {
          const attemptIncomingLease = attempt === 0
            ? hit ? continuationLease : incomingLease
            : hit ? incomingLease : undefined;
          if (attempt > 0 && hit) incomingLeaseOwned = false;
          rawHandle = await options.router.startRelay(relayBody, {
            source: "http",
            clientId,
            signal: attemptLink.controller.signal,
            incomingLease: attemptIncomingLease,
            retainIncomingLeaseBytes: attempt > 0 && hit,
            encodedBodyBytes,
            structuralBodyBytes: relayStructuralBytes,
            fairnessRequestBytes: attempt === 0 && hit
              ? Math.max(incomingLease?.bodyBytes ?? 0, upstreamStructuralBytes)
              : undefined,
            retainMemoryUntil: !hit || attempt > 0
              ? requestLifetime
              : undefined,
            authorizationScope: authorizationScope ?? undefined,
          });
          completeIncomingAdmission();
          if (attempt > 0 && hit) {
            releaseIncomingLease();
            relayBody = {};
            upstreamBody = {};
            body = {};
          }
          const handle: RunHandle = {
            events: () => rawHandle!.events(),
            discardUnconsumedEvents: () => rawHandle!.discardUnconsumedEvents?.(),
            get rateLimit() { return rawHandle!.rateLimit; },
            get upstreamPrelude() { return rawHandle!.upstreamPrelude; },
            stats: rawHandle.stats,
            final: rawHandle.final.then(async final => {
              if (authorizationScope !== null) {
                await responsesContinuationIndex.commit(
                  continuationToken,
                  authorizationScope,
                  final,
                  attemptLink.controller.signal,
                );
              }
              return final;
            }),
          };
          if (!clientWantsStream) {
            stage = "nonstream";
            const final = await nonStreamingFinal(
              handle,
              attemptLink.controller,
              (source, sawEvent) => {
                nonstreamFailureSource = source;
                sawRelayEvent = sawEvent;
              },
            );
            return permittedJsonResponse(final);
          }

          // R2 proves successful upstream HTTP headers before any response event.
          // Waiting here lets an initial provider non-2xx remain an actual HTTP
          // error instead of first committing a misleading outer 200. Legacy
          // handles have no prelude and retain their established behavior.
          stage = "prelude";
          let upstreamPrelude: Awaited<NonNullable<RunHandle["upstreamPrelude"]>> | undefined;
          try {
            upstreamPrelude = await handle.upstreamPrelude;
          } catch (error) {
            releaseFailedPreludeHandle(handle, attemptLink.controller, error);
            throw error;
          }
          stage = "sse-commit";
          stream = responseEventStream(handle, keepAliveMs, {
            cancelRelay: reason => attemptLink.controller.abort(reason),
            signal: attemptLink.controller.signal,
            close: () => {
              attemptLink.unlink();
              cleanupSignal();
            },
          });
          const headers = new Headers(SSE_HEADERS);
          if (upstreamPrelude?.upstreamRequestId) {
            headers.set("x-request-id", upstreamPrelude.upstreamRequestId);
          }
          const response = new Response(stream, {
            status: upstreamPrelude?.status ?? 200,
            headers,
          });
          streamOwnsAttempt = true;
          streamOwnsCleanup = true;
          return response;
        } catch (error) {
          if (rawHandle && stage !== "prelude" && !attemptLink.controller.signal.aborted) {
            attemptLink.controller.abort(error);
          }
          if (stream && !streamOwnsAttempt) void stream.cancel(error).catch(() => undefined);
          const safeFailure = stage === "start" || stage === "prelude" ||
            (stage === "nonstream" && nonstreamFailureSource === "final" && !sawRelayEvent);
          const retry = attempt === 0 && hit && continuationToken !== null &&
            continuationToken.match !== null && !relayAbort.signal.aborted && safeFailure &&
            definitivePreviousResponseNotFound(error);
          if (retry && responsesContinuationIndex.compareDelete(continuationToken!)) {
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
      const normalized = normalizeApiError(error);
      if (normalized.status !== 499) {
        console.error(`[responses] ${normalized.error.type}: ${normalized.error.message.slice(0, 240)}`);
      }
      return apiErrorResponse(error);
    } finally {
      completeIncomingAdmission();
      releaseIncomingLease();
      if (!streamOwnsCleanup) cleanupSignal();
      setImmediate(finishRequestLifetime);
    }
  });

  registerImageRoutes(app, {
    options: options.imageOptions ?? {
      responsesModel: "gpt-5.6-sol",
      maxEditInputBytes: 40 * 1024 ** 2,
    },
    startRelay: (
      body,
      signal,
      clientId,
      incomingLease,
      retainMemoryUntil,
      structuralBodyBytes,
    ) =>
      options.router.startRelay(body, {
      source: "image",
      clientId,
      signal,
      incomingLease,
      structuralBodyBytes,
      authorizationScope: authorizationScope ?? undefined,
      retainMemoryUntil,
    }),
  });

  registerAnthropicRoutes(app, {
    keepAliveMs,
    models: options.anthropicModels ?? anthropicModelsFromEnv(),
    continuationIndex: anthropicContinuationIndex,
    continuationScope: authorizationScope ?? undefined,
    startRelay: (body, signal, clientId, incomingLease, admission) => options.router.startRelay(body, {
      source: "anthropic",
      clientId,
      signal,
      incomingLease,
      authorizationScope: authorizationScope ?? undefined,
      structuralBodyBytes: admission?.structuralBodyBytes,
      fairnessRequestBytes: admission?.fairnessBytes,
      retainIncomingLeaseBytes: admission?.retainIncomingLeaseBytes,
      retainMemoryUntil: admission?.retainMemoryUntil,
    }),
  });

  return app;
}
