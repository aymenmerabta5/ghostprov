import {
  CREDENTIAL_REFRESH_OUTCOMES,
  PRISM_CIRCUIT_SCOPES,
  PRISM_CIRCUIT_STATES,
  RECOVERY_OBSERVABILITY_EVENT_NAMES,
  ROUTER_QUEUE_CLASSES,
  SANDBOX_RECOVERY_CAUSES,
  SANDBOX_RECOVERY_ERROR_KINDS,
  SANDBOX_RECOVERY_ERROR_NAMES,
  SANDBOX_RECOVERY_OPERATIONS,
  SANDBOX_RECOVERY_TRANSPORTS,
  V2_SIDECAR_FATAL_CAUSE_NAMES,
  V2_SIDECAR_FATAL_ERROR_CODES,
  V2_SIDECAR_FATAL_ERROR_NAMES,
  V2_SIDECAR_FATAL_PERMISSIONS,
  V2_SIDECAR_FATAL_PHASES,
  type JsonValue,
  type ObservabilityEvent,
  type ObservabilityEventName,
} from "./events";
import {
  parseSafeV2ReleaseSummary,
  safeV2ReleaseIdentity,
} from "../v2/release-observability";

const CREDENTIAL = /(?:\b(?:Bearer|Basic|Digest)\s+(?!\[REDACTED\](?:\s|$))[^\s]+|\bsk-[A-Za-z0-9_-]{16,}|\bgAAAA[A-Za-z0-9_-]{16,}|\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}(?:\.[A-Za-z0-9_-]{8,})?|\b(?:token|key|secret)\s*[=:](?!\s*\[REDACTED\](?:[\s,;]|$))\s*[^\s,;]+|\b(?:set-cookie|cookie|authorization)\s*[=:](?!\s*\[REDACTED\](?:\s|$))\s*[^\r\n]+)/gi;
const URL_VALUE = /https?:\/\/[^\s"'<>]+/gi;
const FORBIDDEN_KEY = /(?:prompt|instructions|output|content|arguments|headers?|cookie|authorization|token|secret|encrypted|body|url)/i;
const COMMON = new Set(["schema", "event", "ts", "mono_ms"]);
const RECOVERY_FIELDS = new Set([
  "project_id",
  "worker_index",
  "transport",
  "cause",
  "attempt",
  "incarnation",
  "leader_generation",
  "consecutive_failures",
  "status",
  "error_name",
  "error_kind",
  "sidecar_error_name",
  "sidecar_error_code",
  "sidecar_cause_name",
  "sidecar_phase",
  "sidecar_permission",
  "operation",
  "retry_delay_ms",
  "attempt_duration_ms",
  "duration_ms",
  "credential_refresh_outcome",
  "circuit_state",
  "circuit_scope",
  "queued",
]);
const EVENT_FIELDS: Record<ObservabilityEventName, ReadonlySet<string>> = {
  "server.started": new Set(["port", "transport", "runtimes", "capacity", "pid", "release"]),
  "server.stopping": new Set(["reason", "active", "queued"]),
  "request.accepted": new Set(["trace_id", "source", "model", "service_tier", "stream", "workload_class", "request_bytes", "reserved_bytes", "capacity", "active", "available", "queued"]),
  "admission.acquired": new Set(["trace_id", "admission_wait_ms", "reserved_bytes", "reserved_total_bytes", "reservation_capacity_bytes", "memory_paused"]),
  "request.queued": new Set(["trace_id", "queued", "queued_bytes", "client_queue_depth", "queue_class", "active", "available"]),
  "route.selected": new Set(["trace_id", "project_id", "route_ms", "queue_wait_ms", "sandbox_capacity", "sandbox_active", "sandbox_admission_credits", "sandbox_reserved", "sandbox_queued", "sandbox_quarantined", "sandbox_commit_backlog", "sandbox_scheduler_bytes", "sandbox_scheduler_oldest_ms", "sandbox_throughput_bytes_per_sec", "awareness_rtt_ms", "completion_ewma_ms", "recent_failure_rate", "heartbeat_age_ms", "credential_expires_in_ms", "memory_pressure", "eligible_candidates", "queue_class", "fair_cost_bytes", "route_attempt"]),
  "transport.committed": new Set(["trace_id", "project_id", "transport", "queue_wait_ms", "commit_ack_ms", "retry_count", "request_decoded_bytes", "request_encoded_bytes", "request_part_count", "request_encoding", "request_compression_ratio", "host_prepare_ms", "host_scheduler_wait_ms", "host_publish_to_ack_ms", "sidecar_first_frame_to_commit_ms", "sidecar_commit_to_assembly_ms", "sidecar_claim_sync_ms", "sidecar_admission_wait_ms", "sidecar_prepared_to_fetch_start_ms", "sidecar_fetch_start_sync_ms", "sidecar_commit_processing_ms", "transport_commit_residual_ms"]),
  "response.first_event": new Set(["trace_id", "project_id", "first_event_ms", "post_commit_first_event_ms", "upstream_first_event_ms", "sidecar_scheduler_ms", "host_ack_to_first_frame_ms", "host_assembly_to_delivery_ms"]),
  "request.completed": new Set(["trace_id", "project_id", "total_ms", "queue_wait_ms", "commit_ack_ms", "first_event_ms", "response_bytes", "frames", "terminal_status", "retry_count", "slow"]),
  "request.failed": new Set(["trace_id", "project_id", "total_ms", "stage", "error_name", "error_kind", "status", "error_message", "provider_origin", "upstream_request_id", "committed", "quarantined", "retry_count"]),
  "fleet.sample": new Set(["capacity", "active", "available", "queued", "queued_bytes", "desired", "ready", "unavailable", "unavailable_projects", "unavailable_projects_truncated", "rss_bytes", "heap_bytes", "external_bytes", "array_buffer_bytes", "system_total_bytes", "system_free_bytes", "event_loop_p50_ms", "event_loop_p95_ms", "event_loop_max_ms", "cpu_percent", "resources", "logger", "runtimes", "provisioning_circuit", "traffic", "websocket", "release"]),
  "monitor.sample": new Set(["poll_ms", "health_status", "sandboxes_status", "capacity", "active", "available", "queued", "resources", "runtimes"]),
  "monitor.error": new Set(["poll_ms", "attempt", "status", "error_name", "error_message", "retry_ms"]),
  "logger.dropped": new Set(["samples", "progress", "terminals", "queued_events", "queued_bytes"]),
  "logger.error": new Set(["sink", "error_name", "error_message", "count"]),
  "sandbox.recovery.started": RECOVERY_FIELDS,
  "sandbox.recovery.retry_scheduled": RECOVERY_FIELDS,
  "sandbox.recovery.completed": RECOVERY_FIELDS,
  "sandbox.recovery.aborted": RECOVERY_FIELDS,
  "sandbox.credential_refresh": RECOVERY_FIELDS,
  "prism.circuit.changed": RECOVERY_FIELDS,
};

const RECOVERY_EVENTS = new Set<string>(RECOVERY_OBSERVABILITY_EVENT_NAMES);
const PROJECT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RECOVERY_ENUM_FIELDS: Readonly<Record<string, ReadonlySet<string>>> = {
  transport: new Set(SANDBOX_RECOVERY_TRANSPORTS),
  cause: new Set(SANDBOX_RECOVERY_CAUSES),
  error_name: new Set(SANDBOX_RECOVERY_ERROR_NAMES),
  error_kind: new Set(SANDBOX_RECOVERY_ERROR_KINDS),
  sidecar_error_name: new Set(V2_SIDECAR_FATAL_ERROR_NAMES),
  sidecar_error_code: new Set(V2_SIDECAR_FATAL_ERROR_CODES),
  sidecar_cause_name: new Set(V2_SIDECAR_FATAL_CAUSE_NAMES),
  sidecar_phase: new Set(V2_SIDECAR_FATAL_PHASES),
  sidecar_permission: new Set(V2_SIDECAR_FATAL_PERMISSIONS),
  operation: new Set(SANDBOX_RECOVERY_OPERATIONS),
  credential_refresh_outcome: new Set(CREDENTIAL_REFRESH_OUTCOMES),
  circuit_state: new Set(PRISM_CIRCUIT_STATES),
  circuit_scope: new Set(PRISM_CIRCUIT_SCOPES),
};
const RECOVERY_INTEGER_FIELDS = new Set([
  "worker_index",
  "attempt",
  "incarnation",
  "leader_generation",
  "consecutive_failures",
  "queued",
]);
const RECOVERY_DURATION_FIELDS = new Set([
  "retry_delay_ms",
  "attempt_duration_ms",
  "duration_ms",
]);
const RECOVERY_NULLABLE_ENUM_FIELDS = new Set([
  "error_name",
  "error_kind",
  "sidecar_error_name",
  "sidecar_error_code",
  "sidecar_cause_name",
  "sidecar_phase",
  "sidecar_permission",
  "operation",
  "circuit_scope",
]);
const ROUTER_QUEUE_CLASS_SET = new Set<string>(ROUTER_QUEUE_CLASSES);
const ROUTER_INTEGER_FIELDS = new Set([
  "queued_bytes",
  "sandbox_admission_credits",
  "sandbox_commit_backlog",
  "sandbox_scheduler_bytes",
  "fair_cost_bytes",
  "route_attempt",
]);
const ROUTER_NULLABLE_FIELDS = new Set([
  "sandbox_admission_credits",
  "sandbox_commit_backlog",
  "sandbox_scheduler_bytes",
  "sandbox_scheduler_oldest_ms",
  "sandbox_throughput_bytes_per_sec",
]);
const ROUTER_NUMERIC_FIELDS = new Set([
  ...ROUTER_INTEGER_FIELDS,
  "sandbox_scheduler_oldest_ms",
  "sandbox_throughput_bytes_per_sec",
]);
const RUNTIME_PRIVATE_FIELD_NAMES = new Set([
  "token", "cookie", "authorization", "sessionid", "sandboxsessionid",
  "instanceid", "resourcetoken", "ysweettoken",
]);

export function sanitizeDiagnosticMessage(value: unknown): string {
  const normalized = String(value ?? "unknown")
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(CREDENTIAL, match => {
      const separator = /^([^:=]+\s*[=:]\s*)/.exec(match)?.[1];
      if (/^Bearer\s/i.test(match)) return "Bearer [REDACTED]";
      return separator ? `${separator}[REDACTED]` : "[REDACTED]";
    })
    .replace(URL_VALUE, "[URL]")
    .trim();
  return normalized.slice(0, 256) || "unknown";
}

function containsCredential(value: unknown): boolean {
  if (typeof value === "string") {
    CREDENTIAL.lastIndex = 0;
    URL_VALUE.lastIndex = 0;
    return CREDENTIAL.test(value) || URL_VALUE.test(value);
  }
  if (Array.isArray(value)) return value.some(containsCredential);
  if (value && typeof value === "object") {
    return Object.entries(value).some(([key, nested]) => (
      FORBIDDEN_KEY.test(key) || containsCredential(nested)
    ));
  }
  return false;
}

function assertSafeRecoveryFields(event: ObservabilityEvent): void {
  for (const [key, value] of Object.entries(event)) {
    if (COMMON.has(key)) continue;
    if (key === "project_id") {
      if (typeof value !== "string" || !PROJECT_ID.test(value)) {
        throw new TypeError("Recovery observability project_id is invalid");
      }
      continue;
    }
    if (key === "status") {
      if (
        value !== null &&
        (typeof value !== "number" || !Number.isInteger(value) || value < 100 || value > 599)
      ) throw new TypeError("Recovery observability status is invalid");
      continue;
    }
    if (RECOVERY_INTEGER_FIELDS.has(key)) {
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
        throw new TypeError(`Recovery observability ${key} must be a non-negative integer`);
      }
      continue;
    }
    if (RECOVERY_DURATION_FIELDS.has(key)) {
      if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
        throw new TypeError(`Recovery observability ${key} must be a non-negative number`);
      }
      continue;
    }
    const allowedValues = RECOVERY_ENUM_FIELDS[key];
    const nullable = RECOVERY_NULLABLE_ENUM_FIELDS.has(key);
    if (
      !allowedValues ||
      (value === null ? !nullable : typeof value !== "string" || !allowedValues.has(value))
    ) {
      throw new TypeError(`Recovery observability ${key} is not an allowed category`);
    }
  }
}

function assertSafeRouterTelemetry(event: ObservabilityEvent): void {
  if (
    event.event !== "request.queued" &&
    event.event !== "route.selected" &&
    event.event !== "fleet.sample"
  ) return;
  if ("queue_class" in event && !ROUTER_QUEUE_CLASS_SET.has(String(event.queue_class))) {
    throw new TypeError("Router observability queue_class is invalid");
  }
  for (const [key, value] of Object.entries(event)) {
    if (!ROUTER_NUMERIC_FIELDS.has(key)) continue;
    if (value === null && ROUTER_NULLABLE_FIELDS.has(key)) continue;
    if (
      typeof value !== "number" ||
      !Number.isFinite(value) ||
      value < 0 ||
      value > Number.MAX_SAFE_INTEGER ||
      (ROUTER_INTEGER_FIELDS.has(key) && !Number.isSafeInteger(value))
    ) throw new TypeError(`Router observability ${key} is invalid`);
  }
  if (
    event.event === "route.selected" &&
    "route_attempt" in event &&
    typeof event.route_attempt === "number" &&
    event.route_attempt < 1
  ) throw new TypeError("Router observability route_attempt must be positive");
}

function assertSafeFleetRelease(event: ObservabilityEvent): void {
  if (event.event !== "fleet.sample" && event.event !== "server.started") return;
  if ("release" in event && parseSafeV2ReleaseSummary(event.release) === null) {
    throw new TypeError("Fleet release observability is invalid");
  }
  if (event.event !== "fleet.sample") return;
  for (const key of ["desired", "ready", "unavailable"] as const) {
    if (!(key in event)) continue;
    const value = event[key];
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`Fleet availability ${key} is invalid`);
    }
  }
  if (
    typeof event.desired === "number" &&
    ((typeof event.ready === "number" && event.ready > event.desired) ||
      (typeof event.unavailable === "number" && event.unavailable > event.desired))
  ) throw new TypeError("Fleet availability counts are inconsistent");
  if ("unavailable_projects" in event) {
    if (!Array.isArray(event.unavailable_projects) || event.unavailable_projects.length > 256) {
      throw new TypeError("Fleet unavailable project list is invalid");
    }
    const unique = new Set<string>();
    for (const value of event.unavailable_projects) {
      if (typeof value !== "string" || !PROJECT_ID.test(value) || unique.has(value)) {
        throw new TypeError("Fleet unavailable project ID is invalid");
      }
      unique.add(value);
    }
    if (typeof event.unavailable === "number" && unique.size > event.unavailable) {
      throw new TypeError("Fleet unavailable project count is inconsistent");
    }
  }
  if (
    "unavailable_projects_truncated" in event &&
    typeof event.unavailable_projects_truncated !== "boolean"
  ) throw new TypeError("Fleet unavailable project truncation flag is invalid");
  if (!Array.isArray(event.runtimes)) return;
  for (const value of event.runtimes) {
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const runtime = value as Record<string, JsonValue>;
    const pending: unknown[] = [runtime];
    const seen = new WeakSet<object>();
    while (pending.length) {
      const current = pending.pop();
      if (!current || typeof current !== "object") continue;
      if (seen.has(current)) continue;
      seen.add(current);
      if (Array.isArray(current)) {
        pending.push(...current);
        continue;
      }
      for (const [key, child] of Object.entries(current)) {
        if (RUNTIME_PRIVATE_FIELD_NAMES.has(key.replaceAll("_", "").toLowerCase())) {
          throw new TypeError(`Fleet runtime field is forbidden: ${key}`);
        }
        pending.push(child);
      }
    }
    const hasReleaseFields = ["release_revision", "protocol_revision", "build_hash", "schema_hash"]
      .some(key => key in runtime);
    if (!hasReleaseFields) continue;
    const hasRelease = runtime.release_revision !== null ||
      runtime.protocol_revision !== null ||
      runtime.build_hash !== null || runtime.schema_hash !== null;
    if (!hasRelease) continue;
    if (safeV2ReleaseIdentity({
      releaseRevision: runtime.release_revision,
      protocolRevision: runtime.protocol_revision,
      buildHash: runtime.build_hash,
      schemaHash: runtime.schema_hash,
    }) === null) throw new TypeError("Fleet runtime release identity is invalid");
  }
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
  assertSafeRouterTelemetry(event);
  assertSafeFleetRelease(event);
  if (RECOVERY_EVENTS.has(event.event)) assertSafeRecoveryFields(event);
}
