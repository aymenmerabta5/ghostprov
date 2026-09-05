import { diagnosticLog, type DiagnosticLog } from "../../../sandbox/v2/diagnostic-log.ts";
import type { SseFrame } from "../../relay/sse";
import {
  DirectResponsesClient as BaseClient, UpstreamExecutionError,
  type DirectResponsesClientOptions, type DirectResponsesExecution as BaseExecution, type UpstreamTerminal,
} from "./upstream";

export { UpstreamExecutionError } from "./upstream";
export type { UpstreamTerminal } from "./upstream";
export * from "./upstream";
export interface DirectResponsesExecution extends BaseExecution { readonly diagnosticOrdinal?: number; }

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

function declaresTool(frame: SseFrame): boolean {
  if (frame.event !== "response.output_item.added" || frame.data.length > 8192) return false;
  try {
    const type = object(object(JSON.parse(frame.data))?.item)?.type;
    return type === "function_call" || type === "custom_tool_call";
  } catch { return false; }
}

/** R10 observes the unchanged HTTP/SSE implementation and its exact terminal object. */
export class DirectResponsesClient {
  readonly #options: Readonly<DirectResponsesClientOptions>;
  readonly #log: DiagnosticLog;
  readonly #now: () => number;

  constructor(options: DirectResponsesClientOptions & { diagnosticLog?: DiagnosticLog; now?: () => number }) {
    new BaseClient(options);
    this.#options = Object.freeze({ ...options });
    this.#log = options.diagnosticLog ?? diagnosticLog("sidecar");
    this.#now = options.now ?? (() => performance.now());
  }

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    const log = this.#log;
    const now = this.#now;
    const request = input.diagnosticOrdinal ?? 0;
    const started = now();
    let fetchStarted: number | null = null;
    let previousEvent: number | null = null;
    let firstEvent: number | null = null;
    let firstText: number | null = null;
    let firstTool: number | null = null;
    let toolReady: number | null = null;
    let events = 0;
    let sampledEvents = 0;
    let maxGap = 0;
    let deliveryTotal = 0;
    let deliveryMax = 0;
    let phase = "before_fetch";
    log.record("upstream_start", { request, bytes: input.body.byteLength }, true);
    const baseFetch = this.#options.fetch ?? globalThis.fetch;
    const client = new BaseClient({ ...this.#options, fetch: async (url, init) => {
      phase = "fetch";
      fetchStarted = now();
      const response = await baseFetch(url, init);
      phase = "headers";
      log.record("upstream_headers", { request, status: response.status, duration_ms: now() - fetchStarted });
      return response;
    } });
    try {
      const terminal = await client.execute({ ...input,
        beforeFetch: async () => {
          const before = now();
          await input.beforeFetch();
          log.record("request_phase", { request, phase: "before_fetch", duration_ms: now() - before });
        },
        onEvent: async frame => {
          const arrived = now();
          const gap = previousEvent === null ? null : arrived - previousEvent;
          const text = frame.event === "response.output_text.delta";
          const tool = frame.event === "response.function_call_arguments.delta" || frame.event === "response.custom_tool_call_input.delta" || declaresTool(frame);
          const ready = frame.event === "response.function_call_arguments.done" || frame.event === "response.custom_tool_call_input.done";
          const sampled = events < 4 || events % 32 === 0 || (text && firstText === null) ||
            (tool && firstTool === null) || ready || (gap !== null && gap >= 250);
          if (sampled) {
            sampledEvents++;
            log.record("upstream_event", { request, event_type: frame.event, event_index: events,
              since_start_ms: arrived - started, gap_ms: gap ?? 0, event_chars: frame.data.length,
              first_event: firstEvent === null, first_text: text && firstText === null,
              first_tool: tool && firstTool === null, tool_ready: ready });
          }
          events++;
          firstEvent ??= arrived;
          if (text) firstText ??= arrived;
          if (tool) firstTool ??= arrived;
          if (ready) toolReady ??= arrived;
          if (gap !== null) maxGap = Math.max(maxGap, gap);
          previousEvent = arrived;
          const deliveryStarted = now();
          phase = "deliver";
          await input.onEvent(frame);
          phase = "parse";
          const duration = now() - deliveryStarted;
          deliveryTotal += duration;
          deliveryMax = Math.max(deliveryMax, duration);
          if (sampled || duration >= 50) log.record("event_delivery", { request, event_type: frame.event, duration_ms: duration });
        },
      });
      const usage = object(terminal.final.usage);
      phase = "terminal";
      const elapsed = (value: number | null) => value === null ? null : value - started;
      log.record("upstream_terminal", { request, duration_ms: now() - started,
        fetch_to_terminal_ms: fetchStarted === null ? null : now() - fetchStarted,
        bytes: terminal.byteCount, events: terminal.eventCount, sampled_events: sampledEvents,
        first_event_ms: elapsed(firstEvent), first_text_ms: elapsed(firstText), first_tool_ms: elapsed(firstTool), tool_ready_ms: elapsed(toolReady),
        event_gap_max_ms: maxGap, event_delivery_max_ms: deliveryMax, event_delivery_total_ms: deliveryTotal,
        input_tokens: usage?.input_tokens, output_tokens: usage?.output_tokens,
        cached_tokens: object(usage?.input_tokens_details)?.cached_tokens }, true);
      return terminal;
    } catch (error) {
      log.record("upstream_error", { request, duration_ms: now() - started,
        phase, events, sampled_events: sampledEvents, event_gap_max_ms: maxGap,
        event_delivery_max_ms: deliveryMax, event_delivery_total_ms: deliveryTotal,
        error_name: error instanceof Error ? error.name : "other",
        error_code: error instanceof UpstreamExecutionError ? error.status === 429 ? "rate_limit" : error.code : "other",
        status: error instanceof UpstreamExecutionError ? error.status : undefined }, true);
      throw error;
    } finally {
      log.record("logger_stats", log.snapshot(), true);
    }
  }
}
