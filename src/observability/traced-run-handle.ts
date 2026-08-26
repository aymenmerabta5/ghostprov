import type { Clock } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import { observabilityEvent, type JsonValue } from "./events";
import type { LoggerLike } from "./logger";
import { sanitizeDiagnosticMessage } from "./redaction";
import { providerErrorEnvelopeFrom } from "../v2/provider-error";

export interface RunTraceContext {
  logger: LoggerLike;
  clock: Clock;
  traceId: string;
  projectId: string;
  acceptedAtMs: number;
  queueWaitMs: number;
  commitAckMs: number;
  committedAtMs: number;
  retryCount: number;
  slowRequestMs: number;
}

function errorFields(error: unknown): Record<string, JsonValue> {
  const value = error && typeof error === "object"
    ? error as Record<string, unknown>
    : {};
  const providerError = providerErrorEnvelopeFrom(error);
  return {
    error_name: error instanceof Error ? error.name : "Error",
    error_kind: typeof value.kind === "string"
      ? value.kind
      : typeof value.code === "string"
        ? value.code
        : null,
    status: typeof value.status === "number"
      ? value.status
      : providerError?.status ?? null,
    error_message: sanitizeDiagnosticMessage(
      error instanceof Error ? error.message : error,
    ),
    ...(providerError ? {
      provider_origin: providerError.origin,
      ...(providerError.upstreamRequestId
        ? { upstream_request_id: providerError.upstreamRequestId }
        : {}),
    } : {}),
  };
}

export function traceRunHandle(
  handle: RunHandle,
  context: RunTraceContext,
): RunHandle {
  let firstEventAtMs: number | null = null;
  let terminalLogged = false;
  const final = handle.final.then(
    value => {
      if (!terminalLogged) {
        terminalLogged = true;
        const totalMs = Math.max(0, context.clock.now() - context.acceptedAtMs);
        context.logger.emit(observabilityEvent("request.completed", {
          trace_id: context.traceId,
          project_id: context.projectId,
          total_ms: totalMs,
          queue_wait_ms: context.queueWaitMs,
          commit_ack_ms: context.commitAckMs,
          first_event_ms: firstEventAtMs === null
            ? null
            : Math.max(0, firstEventAtMs - context.acceptedAtMs),
          response_bytes: handle.stats.bytes,
          frames: handle.stats.frames,
          terminal_status: typeof value?.status === "string" ? value.status : "completed",
          retry_count: context.retryCount,
          slow: totalMs >= context.slowRequestMs,
        }), "terminal");
      }
      return value;
    },
    error => {
      if (!terminalLogged) {
        terminalLogged = true;
        context.logger.emit(observabilityEvent("request.failed", {
          trace_id: context.traceId,
          project_id: context.projectId,
          total_ms: Math.max(0, context.clock.now() - context.acceptedAtMs),
          stage: "terminal",
          ...errorFields(error),
          committed: true,
          quarantined: (error as any)?.ambiguous === true ||
            (error as any)?.kind === "ambiguous",
          retry_count: context.retryCount,
        }), "terminal");
      }
      throw error;
    },
  );
  void final.catch(() => undefined);

  return {
    async *events() {
      for await (const frame of handle.events()) {
        if (firstEventAtMs === null) {
          firstEventAtMs = context.clock.now();
          context.logger.emit(observabilityEvent("response.first_event", {
            trace_id: context.traceId,
            project_id: context.projectId,
            first_event_ms: Math.max(0, firstEventAtMs - context.acceptedAtMs),
            post_commit_first_event_ms: Math.max(0, firstEventAtMs - context.committedAtMs),
            ...(handle.stats.diagnostics ? {
              upstream_first_event_ms: handle.stats.diagnostics.upstreamFirstEventMs,
              sidecar_scheduler_ms: handle.stats.diagnostics.sidecarFirstEventSchedulerMs,
              host_ack_to_first_frame_ms: handle.stats.diagnostics.hostAckToFirstFrameMs,
              host_assembly_to_delivery_ms: handle.stats.diagnostics.hostAssemblyToDeliveryMs,
            } : {}),
          }), "progress");
        }
        yield frame;
      }
    },
    final,
    get rateLimit() { return handle.rateLimit; },
    get upstreamPrelude() { return handle.upstreamPrelude; },
    stats: handle.stats,
  };
}
