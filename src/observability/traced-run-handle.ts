import type { Clock } from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import { observabilityEvent, type JsonValue } from "./events";
import type { LoggerLike } from "./logger";
import { sanitizeDiagnosticMessage } from "./redaction";
import { providerErrorEnvelopeFrom } from "../v2/provider-error";
import { createHash } from "node:crypto";
import { requestFailureFields } from "./request-error";
import { requestCancellationMetadata } from "../core/cancellation-metadata";

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
  callerSignal?: AbortSignal;
}

const SAFE_PROVIDER_REQUEST_ID = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/;

function observableProviderRequestId(value: unknown): string | null {
  const safe = typeof value === "string" && SAFE_PROVIDER_REQUEST_ID.test(value)
    ? value
    : null;
  return safe !== null && sanitizeDiagnosticMessage(safe) === safe
    ? createHash("sha256").update(safe).digest("hex") : null;
}

function errorFields(
  error: unknown,
  preludeRequestId: string | null,
): Record<string, JsonValue> {
  let providerError: ReturnType<typeof providerErrorEnvelopeFrom> = null;
  try { providerError = providerErrorEnvelopeFrom(error); } catch {
    // Optional metadata must not invoke an untrusted accessor into settlement.
  }
  const upstreamRequestId = observableProviderRequestId(
    providerError?.upstreamRequestId,
  ) ?? preludeRequestId;
  return {
    ...requestFailureFields(error, providerError?.status),
    ...(providerError ? {
      provider_origin: providerError.origin,
    } : {}),
    ...(upstreamRequestId ? { upstream_request_id_sha256: upstreamRequestId } : {}),
  };
}

export function traceRunHandle(
  handle: RunHandle,
  context: RunTraceContext,
): RunHandle {
  let firstEventAtMs: number | null = null;
  let firstTextMs: number | null = null;
  let firstToolMs: number | null = null;
  let terminalLogged = false;
  let preludeRequestId: string | null = null;
  try {
    const prelude = handle.upstreamPrelude;
    if (prelude) {
      void Promise.resolve(prelude).then(value => {
        try {
          preludeRequestId = observableProviderRequestId(
            value && typeof value === "object"
              ? (value as { upstreamRequestId?: unknown }).upstreamRequestId
              : null,
          );
        } catch {
          // A hostile accessor cannot escape an optional telemetry observer.
        }
      }, () => undefined).catch(() => undefined);
    }
  } catch {
    // Correlation metadata is optional and cannot alter relay settlement.
  }
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
          first_text_ms: firstTextMs,
          first_tool_ms: firstToolMs,
          upstream_total_ms: handle.stats.diagnostics?.upstreamTotalMs ?? null,
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
        const failure = errorFields(error, preludeRequestId);
        const cancellation = failure.error_name === "AbortError" ||
            failure.error_kind === "cancelled" || failure.error_kind === "cancelled_precommit"
          ? requestCancellationMetadata(context.callerSignal) : null;
        context.logger.emit(observabilityEvent("request.failed", {
          trace_id: context.traceId,
          project_id: context.projectId,
          total_ms: Math.max(0, context.clock.now() - context.acceptedAtMs),
          stage: "terminal",
          first_text_ms: firstTextMs,
          first_tool_ms: firstToolMs,
          ...failure,
          ...(cancellation ? {
            cancellation_origin: cancellation.origin,
            ...(cancellation.websocketCloseCode === undefined ? {} : { websocket_close_code: cancellation.websocketCloseCode }),
          } : {}),
          committed: true,
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
        // Event names suffice; never parse or retain text or tool arguments for
        // telemetry. A metadata event must not masquerade as visible output.
        if (firstTextMs === null && (frame.event === "response.output_text.delta" ||
            frame.event === "response.refusal.delta")) {
          firstTextMs = Math.max(0, context.clock.now() - context.acceptedAtMs);
        }
        if (firstToolMs === null && (frame.event === "response.function_call_arguments.delta" ||
            frame.event === "response.custom_tool_call_input.delta")) {
          firstToolMs = Math.max(0, context.clock.now() - context.acceptedAtMs);
        }
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
    discardUnconsumedEvents: () => handle.discardUnconsumedEvents?.(),
    final,
    get rateLimit() { return handle.rateLimit; },
    get upstreamPrelude() { return handle.upstreamPrelude; },
    stats: handle.stats,
  };
}
