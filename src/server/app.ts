import { Hono } from "hono";
import type { RunHandle } from "../relay/run-handle";
import { sseFrame, SSE_HEADERS } from "../relay/sse";
import { apiErrorEvent, apiErrorResponse, normalizeApiError } from "../http/errors";
import { registerImageRoutes } from "../images/routes";
import type { ImageCompatOptions } from "../images/compat";
import { registerAnthropicRoutes } from "../anthropic/routes";
import {
  anthropicModelsFromEnv,
  type AnthropicModelOptions,
} from "../anthropic/model";

export interface RouterLike {
  startRelay(
    body: Record<string, unknown>,
    options?: {
      clientId?: string;
      source?: "http" | "websocket" | "image" | "anthropic" | "internal";
      signal?: AbortSignal;
    },
  ): Promise<RunHandle>;
  acquireIncomingBody?(bytes: number, signal?: AbortSignal): Promise<{ release(): void }>;
  snapshot(): {
    capacity: number;
    active: number;
    available: number;
    queued: number;
    runtimes: unknown[];
    resources: unknown;
    rateLimit?: unknown;
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
      rateLimit: snapshot.rateLimit ?? null,
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
    try {
      const handle = await options.router.startRelay(body, {
        source: "http",
        clientId: c.req.header("x-ghostprov-client-id") ?? "http",
        signal: c.req.raw.signal,
      });
      if (body.stream !== true) return c.json(await handle.final);

      const stream = new ReadableStream<Uint8Array>({
        async start(controller) {
          const encoder = new TextEncoder();
          const push = (value: string) => controller.enqueue(encoder.encode(value));
          let closed = false;
          let sawTerminal = false;
          let nextSequence: number | null = null;
          const keepAlive = setInterval(() => { if (!closed) push(": keepalive\n\n"); }, keepAliveMs);
          try {
            for await (const frame of handle.events()) {
              if (RESPONSE_TERMINAL_EVENTS.has(frame.event)) sawTerminal = true;
              nextSequence = nextResponseSequence(frame.data, nextSequence);
              push(sseFrame(frame.event, frame.data));
            }
            if (!sawTerminal) {
              const final = await handle.final;
              const event = terminalEventForFinal(final);
              push(sseFrame(event, {
                type: event,
                ...(nextSequence === null ? {} : { sequence_number: nextSequence }),
                response: final,
              }));
            }
          } catch (error) {
            push(sseFrame("error", apiErrorEvent(error)));
          } finally {
            clearInterval(keepAlive);
            closed = true;
            controller.close();
          }
        },
      });
      return new Response(stream, { headers: SSE_HEADERS });
    } catch (error) {
      const normalized = normalizeApiError(error);
      if (normalized.status !== 499) {
        console.error(`[responses] ${normalized.error.type}: ${normalized.error.message.slice(0, 240)}`);
      }
      return apiErrorResponse(error);
    }
  });

  registerImageRoutes(app, {
    options: options.imageOptions ?? {
      responsesModel: "gpt-5.6-sol",
      maxEditInputBytes: 40 * 1024 ** 2,
    },
    startRelay: (body, signal) => options.router.startRelay(body, {
      source: "image",
      clientId: "images",
      signal,
    }),
  });

  registerAnthropicRoutes(app, {
    keepAliveMs,
    models: options.anthropicModels ?? anthropicModelsFromEnv(),
    startRelay: (body, signal, clientId) => options.router.startRelay(body, {
      source: "anthropic",
      clientId,
      signal,
    }),
  });

  return app;
}
