import type { ObservabilityEvent, ObservabilityEventName } from "./events";

const CREDENTIAL = /(?:\bBearer\s+[^\s]+|\bsk-[A-Za-z0-9_-]{16,}|\bgAAAA[A-Za-z0-9_-]{16,}|\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}(?:\.[A-Za-z0-9_-]{8,})?|\b(?:token|key|secret)\s*[=:]\s*[^\s,;]+)/gi;
const FORBIDDEN_KEY = /(?:prompt|instructions|output|content|arguments|headers?|cookie|authorization|token|secret|encrypted|body|url)/i;
const COMMON = new Set(["schema", "event", "ts", "mono_ms"]);
const EVENT_FIELDS: Record<ObservabilityEventName, ReadonlySet<string>> = {
  "server.started": new Set(["port", "transport", "runtimes", "capacity", "pid"]),
  "server.stopping": new Set(["reason", "active", "queued"]),
  "request.accepted": new Set(["trace_id", "source", "model", "service_tier", "stream", "workload_class", "request_bytes", "reserved_bytes", "capacity", "active", "available", "queued"]),
  "admission.acquired": new Set(["trace_id", "admission_wait_ms", "reserved_bytes", "reserved_total_bytes", "reservation_capacity_bytes", "memory_paused"]),
  "request.queued": new Set(["trace_id", "queued", "client_queue_depth", "active", "available"]),
  "route.selected": new Set(["trace_id", "project_id", "route_ms", "queue_wait_ms", "sandbox_capacity", "sandbox_active", "sandbox_reserved", "sandbox_queued", "sandbox_quarantined", "awareness_rtt_ms", "completion_ewma_ms", "recent_failure_rate", "heartbeat_age_ms", "credential_expires_in_ms", "memory_pressure", "eligible_candidates"]),
  "transport.committed": new Set(["trace_id", "project_id", "transport", "queue_wait_ms", "commit_ack_ms", "retry_count"]),
  "response.first_event": new Set(["trace_id", "project_id", "first_event_ms", "post_commit_first_event_ms"]),
  "request.completed": new Set(["trace_id", "project_id", "total_ms", "queue_wait_ms", "commit_ack_ms", "first_event_ms", "response_bytes", "frames", "terminal_status", "retry_count", "slow"]),
  "request.failed": new Set(["trace_id", "project_id", "total_ms", "stage", "error_name", "error_kind", "status", "error_message", "committed", "quarantined", "retry_count"]),
  "fleet.sample": new Set(["capacity", "active", "available", "queued", "rss_bytes", "heap_bytes", "external_bytes", "system_total_bytes", "system_free_bytes", "event_loop_p50_ms", "event_loop_p95_ms", "event_loop_max_ms", "cpu_percent", "resources", "logger", "runtimes"]),
  "monitor.sample": new Set(["poll_ms", "health_status", "sandboxes_status", "capacity", "active", "available", "queued", "resources", "runtimes"]),
  "monitor.error": new Set(["poll_ms", "attempt", "status", "error_name", "error_message", "retry_ms"]),
  "logger.dropped": new Set(["samples", "progress", "queued_events", "queued_bytes"]),
  "logger.error": new Set(["sink", "error_name", "error_message", "count"]),
};

export function sanitizeDiagnosticMessage(value: unknown): string {
  const normalized = String(value ?? "unknown")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(CREDENTIAL, match => {
      const separator = /^([^:=]+\s*[=:]\s*)/.exec(match)?.[1];
      if (/^Bearer\s/i.test(match)) return "Bearer [REDACTED]";
      return separator ? `${separator}[REDACTED]` : "[REDACTED]";
    })
    .trim();
  return normalized.slice(0, 256) || "unknown";
}

function containsCredential(value: unknown): boolean {
  if (typeof value === "string") {
    CREDENTIAL.lastIndex = 0;
    return CREDENTIAL.test(value);
  }
  if (Array.isArray(value)) return value.some(containsCredential);
  if (value && typeof value === "object") {
    return Object.entries(value).some(([key, nested]) => (
      FORBIDDEN_KEY.test(key) || containsCredential(nested)
    ));
  }
  return false;
}

export function assertSafeObservabilityEvent(event: ObservabilityEvent): void {
  if (event.schema !== 1 || !EVENT_FIELDS[event.event]) {
    throw new TypeError("Observability event schema or name is invalid");
  }
  const allowed = EVENT_FIELDS[event.event];
  for (const key of Object.keys(event)) {
    if (!COMMON.has(key) && !allowed.has(key)) {
      throw new TypeError(`Observability event field is not allowed: ${key}`);
    }
    if (FORBIDDEN_KEY.test(key)) {
      throw new TypeError(`Observability event field is forbidden: ${key}`);
    }
  }
  if (containsCredential(event)) {
    throw new TypeError("Observability event contains credential-shaped data");
  }
}
