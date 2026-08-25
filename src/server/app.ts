import { Hono } from "hono";
import type { RunHandle } from "../relay/run-handle";
import { sseFrame, SSE_HEADERS, type SseFrame } from "../relay/sse";
import { apiErrorEvent, apiErrorResponse, normalizeApiError } from "../http/errors";
import { registerImageRoutes } from "../images/routes";
import type { ImageCompatOptions } from "../images/compat";
import { registerAnthropicRoutes } from "../anthropic/routes";
import {
  anthropicModelsFromEnv,
  type AnthropicModelOptions,
} from "../anthropic/model";
import type { IncomingResourceLease } from "../router/router";
import { takeIncomingBodyLease } from "../http/request-lease";

export interface RouterLike {
  startRelay(
    body: Record<string, unknown>,
    options?: {
      clientId?: string;
      source?: "http" | "websocket" | "image" | "anthropic" | "internal";
      signal?: AbortSignal;
      incomingLease?: IncomingResourceLease;
    },
  ): Promise<RunHandle>;
  acquireIncomingBody?(bytes: number, signal?: AbortSignal): Promise<IncomingResourceLease>;
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
  anthropicModels?: AnthropicModelOptions;
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

async function nonStreamingFinal(
  handle: RunHandle,
  cancelRelay: (reason?: unknown) => void,
): Promise<any> {
  const iterator = handle.events();
  const drain = (async () => {
    while (!(await iterator.next()).done) {
      // Non-streaming callers still have to consume the bounded transport
      // queue. The authoritative response is provided by handle.final.
    }
  })();
  try {
    const [final] = await Promise.all([handle.final, drain]);
    return final;
  } catch (error) {
    cancelRelay(error);
    void iterator.return(undefined).catch(() => undefined);
    throw error;
  }
}

function responseEventStream(
  handle: RunHandle,
  keepAliveMs: number,
  options: {
    cancelRelay(reason?: unknown): void;
    close(): void;
  },
): ReadableStream<Uint8Array> {
  type NextOutcome = { kind: "next"; result: IteratorResult<SseFrame, void> };
  type CancelledOutcome = { kind: "cancelled" };
  const encoder = new TextEncoder();
  const iterator = handle.events();
  let pendingNext: Promise<NextOutcome> | null = null;
  let sawTerminal = false;
  let nextSequence: number | null = null;
  let finished = false;
  let cancelled = false;
  let wakeCancellation: (() => void) | null = null;

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
    options.close();
    if (!cancelled) controller.close();
  };

  const fail = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    error: unknown,
  ): void => {
    if (finished || cancelled) return;
    options.cancelRelay(error);
    void iterator.return(undefined).catch(() => undefined);
    try {
      controller.enqueue(encoder.encode(sseFrame("error", apiErrorEvent(error))));
    } finally {
      finish(controller);
    }
  };

  const nextOrKeepAlive = async (): Promise<
    | NextOutcome
    | { kind: "keepalive" }
    | CancelledOutcome
  > => {
    const next = pendingNext ??= iterator.next().then(result => ({ kind: "next", result }));
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
      ]);
      if (result.kind === "next" && pendingNext === next) pendingNext = null;
      return result;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      cancellation.dispose();
    }
  };

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (finished || cancelled) return;
      try {
        const outcome = await nextOrKeepAlive();
        if (cancelled || outcome.kind === "cancelled") return;
        if (outcome.kind === "keepalive") {
          controller.enqueue(encoder.encode(": keepalive\n\n"));
          return;
        }

        if (!outcome.result.done) {
          const frame = outcome.result.value;
          if (RESPONSE_TERMINAL_EVENTS.has(frame.event)) sawTerminal = true;
          nextSequence = nextResponseSequence(frame.data, nextSequence);
          controller.enqueue(encoder.encode(sseFrame(frame.event, frame.data)));
          return;
        }

        if (!sawTerminal) {
          const cancellation = cancellationWaiter();
          const finalOutcome = await Promise.race([
            handle.final.then(value => ({ kind: "final" as const, value })),
            cancellation.promise,
          ]).finally(cancellation.dispose);
          if (cancelled || finalOutcome.kind === "cancelled") return;
          const event = terminalEventForFinal(finalOutcome.value);
          controller.enqueue(encoder.encode(sseFrame(event, {
            type: event,
            ...(nextSequence === null ? {} : { sequence_number: nextSequence }),
            response: finalOutcome.value,
          })));
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
      wakeCancellation?.();
      wakeCancellation = null;
      options.cancelRelay(reason);
      options.close();
      void iterator.return(undefined).catch(() => undefined);
    },
  });
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

export function createGhostProvApp(options: GhostProvAppOptions): Hono {
  const app = new Hono();
  const keepAliveMs = options.keepAliveMs ?? 15_000;
  if (!Number.isSafeInteger(keepAliveMs) || keepAliveMs < 1 || keepAliveMs > MAX_TIMER_MS) {
    throw new RangeError(`keepAliveMs must be an integer from 1 through ${MAX_TIMER_MS}`);
  }

  app.get("/health", c => {
    const snapshot = options.router.snapshot();
    const healthy = snapshot.capacity > 0;
    return c.json({
      ok: healthy,
      capacity: snapshot.capacity,
      active: snapshot.active,
      available: snapshot.available,
      queued: snapshot.queued,
      runtimes: snapshot.runtimes.length,
    }, healthy ? 200 : 503);
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
    let body: Record<string, unknown>;
    try {
      body = await c.req.json();
    } catch {
      return c.json({ error: { message: "invalid JSON body" } }, 400);
    }
    const requestSignal = c.req.raw.signal;
    const relayAbort = new AbortController();
    const abortRelay = () => relayAbort.abort(requestSignal.reason);
    if (requestSignal.aborted) abortRelay();
    else requestSignal.addEventListener("abort", abortRelay, { once: true });
    let streamOwnsCleanup = false;
    const cleanupSignal = () => requestSignal.removeEventListener("abort", abortRelay);
    try {
      const handle = await options.router.startRelay(body, {
        source: "http",
        clientId: c.req.header("x-ghostprov-client-id") ?? "http",
        signal: relayAbort.signal,
        incomingLease: takeIncomingBodyLease(c.env),
      });
      if (body.stream !== true) {
        const final = await nonStreamingFinal(handle, reason => relayAbort.abort(reason));
        return c.json(final);
      }

      // R2 proves successful upstream HTTP headers before any response event.
      // Waiting here lets an initial provider non-2xx remain an actual HTTP
      // error instead of first committing a misleading outer 200. Legacy
      // handles have no prelude and retain their established behavior.
      let upstreamPrelude: Awaited<NonNullable<RunHandle["upstreamPrelude"]>> | undefined;
      try {
        upstreamPrelude = await handle.upstreamPrelude;
      } catch (error) {
        relayAbort.abort(error);
        throw error;
      }

      const stream = responseEventStream(handle, keepAliveMs, {
        cancelRelay: reason => relayAbort.abort(reason),
        close: cleanupSignal,
      });
      streamOwnsCleanup = true;
      const headers = new Headers(SSE_HEADERS);
      if (upstreamPrelude?.upstreamRequestId) {
        headers.set("x-request-id", upstreamPrelude.upstreamRequestId);
      }
      return new Response(stream, {
        status: upstreamPrelude?.status ?? 200,
        headers,
      });
    } catch (error) {
      const normalized = normalizeApiError(error);
      if (normalized.status !== 499) {
        console.error(`[responses] ${normalized.error.type}: ${normalized.error.message.slice(0, 240)}`);
      }
      return apiErrorResponse(error);
    } finally {
      if (!streamOwnsCleanup) cleanupSignal();
    }
  });

  registerImageRoutes(app, {
    options: options.imageOptions ?? {
      responsesModel: "gpt-5.6-sol",
      maxEditInputBytes: 40 * 1024 ** 2,
    },
    startRelay: (body, signal, incomingLease) => options.router.startRelay(body, {
      source: "image",
      clientId: "images",
      signal,
      incomingLease,
    }),
  });

  registerAnthropicRoutes(app, {
    keepAliveMs,
    models: options.anthropicModels ?? anthropicModelsFromEnv(),
    startRelay: (body, signal, clientId, incomingLease) => options.router.startRelay(body, {
      source: "anthropic",
      clientId,
      signal,
      incomingLease,
    }),
  });

  return app;
}
