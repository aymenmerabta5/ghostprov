import { BOOTSTRAP_FAILURE_CODES, BOOTSTRAP_FAILURE_PHASES } from "../sandbox/types";
import {
  CREDENTIAL_REFRESH_OUTCOMES,
  FLEET_LIFECYCLE_EVENT_NAMES,
  FLEET_MUTATION_BLOCK_REASONS,
  FLEET_MUTATION_OPERATIONS,
  FLEET_REPLACEMENT_FAILURE_KINDS,
  FLEET_RETIREMENT_REASONS,
  FLEET_STARTUP_PERMIT_ACTIONS,
  PRISM_CIRCUIT_SCOPES,
  PRISM_CIRCUIT_STATES,
  RECOVERY_OBSERVABILITY_EVENT_NAMES,
  SANDBOX_AWARENESS_LANES,
  SANDBOX_AVAILABILITY_REASONS,
  SANDBOX_LIVENESS_ERROR_KINDS,
  SANDBOX_LIVENESS_ERROR_NAMES,
  SANDBOX_LIVENESS_OUTCOMES,
  SANDBOX_LIVENESS_SIGNALS,
  SANDBOX_RECONNECT_PHASES,
  SANDBOX_SUPERVISOR_PHASES,
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

const CREDENTIAL_LABEL =
  "(?:authorization|proxy[-_]?authorization|cookie|set[-_]?cookie|api[-_]?key|" +
  "access[-_]?token|refresh[-_]?token|session[-_]?token|token|secret|password)";
const JSON_CREDENTIAL = new RegExp(
  `("${CREDENTIAL_LABEL}"\\s*:\\s*)"(?:[^"\\\\]|\\\\.)*"`,
  "gi",
);
const LABELED_CREDENTIAL = new RegExp(
  `(^|[\\s,;{[(])((?:"|')?${CREDENTIAL_LABEL}(?:"|')?\\s*[=:]\\s*)` +
    `("(?:[^"\\\\]|\\\\.)*"|'[^']*'|[^\\r\\n,;}\\]]+)`,
  "gi",
);
const SAFE_REDACTED_CREDENTIAL = new RegExp(
  `(?:"|')?${CREDENTIAL_LABEL}(?:"|')?\\s*[=:]\\s*(?:"|')?\\[REDACTED\\](?:"|')?` +
    `|\\b(?:Bearer|Basic|Digest)\\s+\\[REDACTED\\]`,
  "gi",
);
const AUTHORIZATION_CREDENTIAL = /\b(Bearer|Basic|Digest)\s+(?!\[REDACTED\](?:\s|$))[^\s,;"']+/gi;
const OPAQUE_CREDENTIAL = /(?:\bsk-[A-Za-z0-9_-]{16,}|\bgAAAA[A-Za-z0-9_-]{16,}|\beyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}(?:\.[A-Za-z0-9_-]{8,})?)/gi;
const URL_VALUE = /\b(?:https?|wss?|ftp|file|data|blob|mailto):[^\s"'<>]+/gi;
const ORGANIZATION_VALUE = /\b(in\s+organization\s+)[A-Za-z0-9][A-Za-z0-9_-]{0,127}\b/gi;
const JSON_ORGANIZATION_VALUE = /("organization"\s*:\s*)"(?:[^"\\]|\\.)*"/gi;
const FORBIDDEN_KEY = /(?:prompt|instructions|output|content|arguments|headers?|cookie|authorization|api.?key|password|token|secret|encrypted|body|url|client.?id|organization)/i;
const DIAGNOSTIC_SOURCE_MAX_UNITS = 4_096;
const REASONING_EFFORTS = new Set([
  "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
]);
const COMMON = new Set(["schema", "event", "ts", "mono_ms"]);
const RECOVERY_FIELDS = new Set([
  "bootstrap_phase", "bootstrap_code",
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
const FLEET_LIFECYCLE_FIELDS = new Set([
  "project_id",
  "worker_index",
  "leader_generation",
  "failure_kind",
  "failure_count",
  "retirement_reason",
  "managed",
  "mutation_operation",
  "block_reason",
  "cold_start",
  "permit_action",
  "total_limit",
  "total_in_use",
  "total_queued",
  "cold_limit",
  "cold_in_use",
  "cold_queued",
]);
const LIVENESS_FIELDS = new Set([
  "project_id",
  "worker_index",
  "transport",
  "liveness_signal",
  "liveness_outcome",
  "attempt",
  "consecutive_failures",
  "status",
  "error_name",
  "error_kind",
  "duration_ms",
  "heartbeat_age_ms",
  "sidecar_peer_age_ms",
  "transport_connected",
  "reliable_stall",
  "awareness_lane",
  "connection_code",
  "reconnect_phase",
  "supervisor_phase",
]);
const EVENT_FIELDS: Record<ObservabilityEventName, ReadonlySet<string>> = {
  "server.started": new Set(["port", "transport", "runtimes", "capacity", "pid", "release"]),
  "server.stopping": new Set(["reason", "active", "queued"]),
  "request.accepted": new Set(["trace_id", "source", "model", "service_tier", "reasoning_effort", "stream", "workload_class", "request_bytes", "input_items", "has_previous_response_id", "transfer_admission_cost", "reserved_bytes", "capacity", "active", "available", "queued"]),
  "admission.acquired": new Set(["trace_id", "admission_wait_ms", "reserved_bytes", "reserved_total_bytes", "reservation_capacity_bytes", "memory_paused"]),
  "request.queued": new Set(["trace_id", "queued", "queued_bytes", "client_queue_depth", "queue_class", "active", "available"]),
  "route.selected": new Set(["trace_id", "project_id", "route_ms", "queue_wait_ms", "sandbox_capacity", "sandbox_active", "sandbox_admission_credits", "sandbox_reserved", "sandbox_queued", "sandbox_quarantined", "sandbox_commit_backlog", "sandbox_scheduler_bytes", "sandbox_scheduler_oldest_ms", "sandbox_throughput_bytes_per_sec", "awareness_rtt_ms", "completion_ewma_ms", "recent_failure_rate", "heartbeat_age_ms", "sidecar_peer_age_ms", "credential_expires_in_ms", "memory_pressure", "eligible_candidates", "queue_class", "fair_cost_bytes", "transfer_admission_cost", "fleet_transfer_cost", "fleet_transfer_reserved", "fleet_transfer_budget", "route_attempt", "capacity_reason"]),
  "route.attempt_expired": new Set(["trace_id", "project_id", "route_attempt", "attempt_ms", "progress_observed", "progress_age_ms", "progress_stage", "progress_acknowledged_encoded_bytes", "progress_total_encoded_bytes", "deadline_exhausted", "capacity_reason", "transfer_admission_cost"]),
  "transport.committed": new Set(["trace_id", "project_id", "transport", "queue_wait_ms", "commit_ack_ms", "retry_count", "request_decoded_bytes", "request_encoded_bytes", "request_part_count", "request_encoding", "request_compression_ratio", "host_prepare_ms", "host_scheduler_wait_ms", "host_publish_to_ack_ms", "sidecar_first_frame_to_commit_ms", "sidecar_commit_to_assembly_ms", "sidecar_claim_sync_ms", "sidecar_admission_wait_ms", "sidecar_prepared_to_fetch_start_ms", "sidecar_fetch_start_sync_ms", "sidecar_commit_processing_ms", "transport_commit_residual_ms"]),
  "response.first_event": new Set(["trace_id", "project_id", "first_event_ms", "post_commit_first_event_ms", "upstream_first_event_ms", "sidecar_scheduler_ms", "host_ack_to_first_frame_ms", "host_assembly_to_delivery_ms"]),
  "request.completed": new Set(["trace_id", "project_id", "total_ms", "queue_wait_ms", "commit_ack_ms", "first_event_ms", "first_text_ms", "first_tool_ms", "upstream_total_ms", "response_bytes", "frames", "terminal_status", "retry_count", "slow"]),
  "request.failed": new Set(["trace_id", "project_id", "total_ms", "stage", "first_text_ms", "first_tool_ms", "error_name", "error_kind", "status", "error_message", "provider_origin", "upstream_request_id", "committed", "quarantined", "retry_count"]),
  "websocket.payload_rejected": new Set(["limit_bytes", "http_fallback_available"]),
  "websocket.http_fallback": new Set(["status", "limit_bytes"]),
  "fleet.sample": new Set(["capacity", "active", "available", "queued", "queued_bytes", "desired", "ready", "routable", "unavailable", "unavailable_projects", "unavailable_projects_truncated", "pending_retirements", "project_mutation_state", "project_mutation_failures", "project_mutation_retry_ms", "rss_bytes", "heap_bytes", "external_bytes", "array_buffer_bytes", "system_total_bytes", "system_free_bytes", "event_loop_p50_ms", "event_loop_p95_ms", "event_loop_max_ms", "cpu_percent", "resources", "logger", "runtimes", "provisioning_circuit", "traffic", "websocket", "release"]),
  "monitor.sample": new Set(["poll_ms", "health_status", "sandboxes_status", "capacity", "active", "available", "queued", "resources", "runtimes"]),
  "monitor.error": new Set(["poll_ms", "attempt", "status", "error_name", "error_message", "retry_ms"]),
  "logger.dropped": new Set(["samples", "progress", "terminals", "queued_events", "queued_bytes"]),
  "logger.error": new Set(["sink", "error_name", "error_message", "count"]),
  "sandbox.liveness": LIVENESS_FIELDS,
  "fleet.replacement.qualified": FLEET_LIFECYCLE_FIELDS,
  "fleet.replacement.started": FLEET_LIFECYCLE_FIELDS,
  "fleet.replacement.successor_reserved": FLEET_LIFECYCLE_FIELDS,
  "fleet.replacement.successor_created": FLEET_LIFECYCLE_FIELDS,
  "fleet.replacement.successor_activated": FLEET_LIFECYCLE_FIELDS,
  "fleet.replacement.old_retired": FLEET_LIFECYCLE_FIELDS,
  "fleet.mutation.blocked": FLEET_LIFECYCLE_FIELDS,
  "fleet.startup_permits.changed": FLEET_LIFECYCLE_FIELDS,
  "sandbox.recovery.started": RECOVERY_FIELDS,
  "sandbox.recovery.retry_scheduled": RECOVERY_FIELDS,
  "sandbox.recovery.completed": RECOVERY_FIELDS,
  "sandbox.recovery.aborted": RECOVERY_FIELDS,
  "sandbox.credential_refresh": RECOVERY_FIELDS,
  "prism.circuit.changed": RECOVERY_FIELDS,
};

const RECOVERY_EVENTS = new Set<string>(RECOVERY_OBSERVABILITY_EVENT_NAMES);
const FLEET_LIFECYCLE_EVENTS = new Set<string>(FLEET_LIFECYCLE_EVENT_NAMES);
const PROJECT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RECOVERY_ENUM_FIELDS: Readonly<Record<string, ReadonlySet<string>>> = {
  bootstrap_phase: new Set(BOOTSTRAP_FAILURE_PHASES),
  bootstrap_code: new Set(BOOTSTRAP_FAILURE_CODES),
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
const FLEET_LIFECYCLE_ENUM_FIELDS: Readonly<Record<string, ReadonlySet<string>>> = {
  failure_kind: new Set(FLEET_REPLACEMENT_FAILURE_KINDS),
  retirement_reason: new Set(FLEET_RETIREMENT_REASONS),
  mutation_operation: new Set(FLEET_MUTATION_OPERATIONS),
  block_reason: new Set(FLEET_MUTATION_BLOCK_REASONS),
  permit_action: new Set(FLEET_STARTUP_PERMIT_ACTIONS),
};
const FLEET_LIFECYCLE_INTEGER_FIELDS = new Set([
  "worker_index",
  "leader_generation",
  "failure_count",
  "total_limit",
  "total_in_use",
  "total_queued",
  "cold_limit",
  "cold_in_use",
  "cold_queued",
]);
const LIVENESS_SIGNAL_SET = new Set<string>(SANDBOX_LIVENESS_SIGNALS);
const LIVENESS_OUTCOME_SET = new Set<string>(SANDBOX_LIVENESS_OUTCOMES);
const LIVENESS_ERROR_NAME_SET = new Set<string>(SANDBOX_LIVENESS_ERROR_NAMES);
const LIVENESS_ERROR_KIND_SET = new Set<string>(SANDBOX_LIVENESS_ERROR_KINDS);
const RECONNECT_PHASE_SET = new Set<string>(SANDBOX_RECONNECT_PHASES);
const SUPERVISOR_PHASE_SET = new Set<string>(SANDBOX_SUPERVISOR_PHASES);
const AWARENESS_LANE_SET = new Set<string>(SANDBOX_AWARENESS_LANES);
const LIVENESS_TRANSPORT_SET = new Set<string>(SANDBOX_RECOVERY_TRANSPORTS);
const RECOVERY_NULLABLE_ENUM_FIELDS = new Set([
  "bootstrap_phase", "bootstrap_code",
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
const SANDBOX_AVAILABILITY_REASON_SET = new Set<string>(SANDBOX_AVAILABILITY_REASONS);
const ROUTER_INTEGER_FIELDS = new Set([
  "queued_bytes",
  "sandbox_admission_credits",
  "sandbox_commit_backlog",
  "sandbox_scheduler_bytes",
  "fair_cost_bytes",
  "route_attempt",
  "transfer_admission_cost",
  "progress_acknowledged_encoded_bytes",
  "progress_total_encoded_bytes",
]);
const ROUTER_NULLABLE_FIELDS = new Set([
  "sandbox_admission_credits",
  "sandbox_commit_backlog",
  "sandbox_scheduler_bytes",
  "sandbox_scheduler_oldest_ms",
  "sandbox_throughput_bytes_per_sec",
  "progress_age_ms",
  "progress_acknowledged_encoded_bytes",
  "progress_total_encoded_bytes",
]);
const ROUTER_NUMERIC_FIELDS = new Set([
  ...ROUTER_INTEGER_FIELDS,
  "attempt_ms",
  "progress_age_ms",
  "sandbox_scheduler_oldest_ms",
  "sandbox_throughput_bytes_per_sec",
]);
const RUNTIME_PRIVATE_FIELD_NAMES = new Set([
  "token", "cookie", "authorization", "sessionid", "sandboxsessionid",
  "instanceid", "resourcetoken", "ysweettoken", "apikey", "password",
  "accesstoken", "refreshtoken", "sessiontoken",
]);

function redactCredentialText(value: string): string {
  return value
    .replace(JSON_CREDENTIAL, '$1"[REDACTED]"')
    .replace(AUTHORIZATION_CREDENTIAL, "$1 [REDACTED]")
    .replace(LABELED_CREDENTIAL, "$1$2[REDACTED]")
    .replace(OPAQUE_CREDENTIAL, "[REDACTED]");
}

function redactUrlText(value: string): string {
  return value.replace(URL_VALUE, "[URL]");
}

function redactIdentifierText(value: string): string {
  return value
    .replace(JSON_ORGANIZATION_VALUE, '$1"[REDACTED]"')
    .replace(ORGANIZATION_VALUE, "$1[REDACTED]");
}

/** A request model is telemetry only when it is a small opaque protocol atom. */
export function safeObservabilityModel(value: unknown): string {
  if (typeof value !== "string" || value.length > 128) return "unknown";
  const normalized = value.trim().toLowerCase();
  return /^[a-z0-9][a-z0-9._:-]{0,127}$/.test(normalized) ? normalized : "unknown";
}

/** Record only a recognized effort label, never reasoning content or accessors. */
export function safeObservabilityReasoningEffort(reasoning: unknown): string {
  if (reasoning === undefined) return "unspecified";
  if (reasoning === null || typeof reasoning !== "object" || Array.isArray(reasoning)) return "unknown";
  try {
    const descriptor = Object.getOwnPropertyDescriptor(reasoning, "effort");
    if (!descriptor) return "unspecified";
    const effort = "value" in descriptor ? descriptor.value : undefined;
    return typeof effort === "string" && REASONING_EFFORTS.has(effort) ? effort : "unknown";
  } catch {
    return "unknown";
  }
}

export function sanitizeDiagnosticMessage(value: unknown): string {
  const source = String(value ?? "unknown");
  const controlled = source.slice(0, DIAGNOSTIC_SOURCE_MAX_UNITS)
    .replace(/[\u0000-\u001f\u007f]/g, " ");
  const normalized = redactIdentifierText(redactUrlText(redactCredentialText(controlled))).trim();
  return normalized.slice(0, 256) || "unknown";
}

function containsCredential(value: unknown): boolean {
  if (typeof value === "string") {
    if (value.length > DIAGNOSTIC_SOURCE_MAX_UNITS) return true;
    const withoutSafeRedactions = value.replace(SAFE_REDACTED_CREDENTIAL, "[REDACTED]");
    return redactCredentialText(withoutSafeRedactions) !== withoutSafeRedactions ||
      redactUrlText(withoutSafeRedactions) !== withoutSafeRedactions ||
      redactIdentifierText(withoutSafeRedactions) !== withoutSafeRedactions;
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

function assertSafeFleetLifecycleFields(event: ObservabilityEvent): void {
  for (const [key, value] of Object.entries(event)) {
    if (COMMON.has(key)) continue;
    if (key === "project_id") {
      if (typeof value !== "string" || !PROJECT_ID.test(value)) {
        throw new TypeError("Fleet lifecycle project_id is invalid");
      }
      continue;
    }
    if (key === "managed" || key === "cold_start") {
      if (typeof value !== "boolean") {
        throw new TypeError(`Fleet lifecycle ${key} must be boolean`);
      }
      continue;
    }
    if (FLEET_LIFECYCLE_INTEGER_FIELDS.has(key)) {
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
        throw new TypeError(`Fleet lifecycle ${key} must be a non-negative integer`);
      }
      continue;
    }
    const allowed = FLEET_LIFECYCLE_ENUM_FIELDS[key];
    if (!allowed || typeof value !== "string" || !allowed.has(value)) {
      throw new TypeError(`Fleet lifecycle ${key} is not an allowed category`);
    }
  }
  if (typeof event.total_in_use === "number" && typeof event.total_limit === "number" &&
    event.total_in_use > event.total_limit) {
    throw new TypeError("Fleet total startup permit usage exceeds its limit");
  }
  if (typeof event.cold_in_use === "number" && typeof event.cold_limit === "number" &&
    event.cold_in_use > event.cold_limit) {
    throw new TypeError("Fleet cold startup permit usage exceeds its limit");
  }
}

function assertSafeLivenessFields(event: ObservabilityEvent): void {
  if (typeof event.project_id !== "string" || !PROJECT_ID.test(event.project_id)) {
    throw new TypeError("Liveness observability project_id is invalid");
  }
  if (typeof event.worker_index !== "number" ||
      !Number.isSafeInteger(event.worker_index) || event.worker_index < 0) {
    throw new TypeError("Liveness observability worker_index must be a non-negative integer");
  }
  for (const key of ["attempt", "consecutive_failures"] as const) {
    const value = event[key];
    if (value === undefined) continue;
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`Liveness observability ${key} must be a non-negative integer`);
    }
  }
  if (typeof event.transport !== "string" || !LIVENESS_TRANSPORT_SET.has(event.transport)) {
    throw new TypeError("Liveness observability transport is invalid");
  }
  if (typeof event.liveness_signal !== "string" ||
      !LIVENESS_SIGNAL_SET.has(event.liveness_signal)) {
    throw new TypeError("Liveness observability signal is invalid");
  }
  if (typeof event.liveness_outcome !== "string" ||
      !LIVENESS_OUTCOME_SET.has(event.liveness_outcome)) {
    throw new TypeError("Liveness observability outcome is invalid");
  }
  if (event.status !== undefined && event.status !== null &&
      (typeof event.status !== "number" || !Number.isInteger(event.status) ||
        event.status < 100 || event.status > 599)) {
    throw new TypeError("Liveness observability status is invalid");
  }
  if (event.error_name !== undefined && event.error_name !== null &&
      (typeof event.error_name !== "string" ||
        !LIVENESS_ERROR_NAME_SET.has(event.error_name))) {
    throw new TypeError("Liveness observability error_name is invalid");
  }
  if (event.error_kind !== undefined && event.error_kind !== null &&
      (typeof event.error_kind !== "string" ||
        !LIVENESS_ERROR_KIND_SET.has(event.error_kind))) {
    throw new TypeError("Liveness observability error_kind is invalid");
  }
  for (const key of ["duration_ms", "heartbeat_age_ms", "sidecar_peer_age_ms"] as const) {
    const value = event[key];
    if (value === undefined || value === null) continue;
    if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
      throw new TypeError(`Liveness observability ${key} must be a non-negative number`);
    }
  }
  for (const key of ["transport_connected", "reliable_stall"] as const) {
    const value = event[key];
    if (value !== undefined && typeof value !== "boolean") {
      throw new TypeError(`Liveness observability ${key} must be boolean`);
    }
  }
  if (event.awareness_lane !== undefined &&
      (typeof event.awareness_lane !== "string" ||
        !AWARENESS_LANE_SET.has(event.awareness_lane))) {
    throw new TypeError("Liveness observability awareness_lane is invalid");
  }
  if (event.connection_code !== undefined && event.connection_code !== null &&
      (typeof event.connection_code !== "number" ||
        !Number.isSafeInteger(event.connection_code) ||
        event.connection_code < 0 || event.connection_code > 65_535)) {
    throw new TypeError("Liveness observability connection_code is invalid");
  }
  for (const [key, allowed] of [["reconnect_phase", RECONNECT_PHASE_SET], ["supervisor_phase", SUPERVISOR_PHASE_SET]] as const) {
    const value = event[key];
    if (value !== undefined && value !== null && (typeof value !== "string" || !allowed.has(value))) {
      throw new TypeError(`Liveness observability ${key} is invalid`);
    }
  }
}

function assertSafeRouterTelemetry(event: ObservabilityEvent): void {
  for (const key of ["first_text_ms", "first_tool_ms", "upstream_total_ms"] as const) {
    const value = event[key];
    if (value !== undefined && value !== null &&
        (typeof value !== "number" || !Number.isFinite(value) || value < 0)) {
      throw new TypeError(`Request timing ${key} is invalid`);
    }
  }
  if (event.event === "request.accepted") {
    const effort = event.reasoning_effort;
    if (effort !== undefined && (typeof effort !== "string" ||
        (!REASONING_EFFORTS.has(effort) && effort !== "unspecified" && effort !== "unknown"))) {
      throw new TypeError("Request reasoning effort must be a bounded category");
    }
    if ("has_previous_response_id" in event && typeof event.has_previous_response_id !== "boolean") {
      throw new TypeError("Request continuation presence must be boolean");
    }
    const count = event.input_items;
    if (count !== undefined && count !== null &&
        (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0 || count > 0xffffffff)) {
      throw new TypeError("Request input item count is invalid");
    }
  }
  if (
    event.event !== "request.queued" &&
    event.event !== "route.selected" &&
    event.event !== "route.attempt_expired" &&
    event.event !== "fleet.sample"
  ) return;
  if ("queue_class" in event && !ROUTER_QUEUE_CLASS_SET.has(String(event.queue_class))) {
    throw new TypeError("Router observability queue_class is invalid");
  }
  if (
    "capacity_reason" in event &&
    !SANDBOX_AVAILABILITY_REASON_SET.has(String(event.capacity_reason))
  ) throw new TypeError("Router observability capacity_reason is invalid");
  if ("progress_observed" in event && typeof event.progress_observed !== "boolean") {
    throw new TypeError("Router observability progress_observed is invalid");
  }
  if ("deadline_exhausted" in event && typeof event.deadline_exhausted !== "boolean") {
    throw new TypeError("Router observability deadline_exhausted is invalid");
  }
  if (
    "progress_stage" in event && event.progress_stage !== null &&
    event.progress_stage !== "request_data" && event.progress_stage !== "request_prepared"
  ) throw new TypeError("Router observability progress_stage is invalid");
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
  if (
    "transfer_admission_cost" in event &&
    (
      typeof event.transfer_admission_cost !== "number" ||
      !Number.isSafeInteger(event.transfer_admission_cost) ||
      event.transfer_admission_cost < 1 ||
      event.transfer_admission_cost > 4
    )
  ) throw new TypeError("Router observability transfer_admission_cost is invalid");
}

function assertSafeFleetRelease(event: ObservabilityEvent): void {
  if (event.event !== "fleet.sample" && event.event !== "server.started") return;
  if ("release" in event && parseSafeV2ReleaseSummary(event.release) === null) {
    throw new TypeError("Fleet release observability is invalid");
  }
  if (event.event !== "fleet.sample") return;
  for (const key of ["desired", "ready", "routable", "unavailable"] as const) {
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
  for (const key of ["pending_retirements", "project_mutation_failures"] as const) {
    if (!(key in event)) continue;
    const value = event[key];
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`Fleet project mutation ${key} is invalid`);
    }
  }
  if ("project_mutation_state" in event &&
    event.project_mutation_state !== "closed" && event.project_mutation_state !== "open") {
    throw new TypeError("Fleet project mutation state is invalid");
  }
  if ("project_mutation_retry_ms" in event && event.project_mutation_retry_ms !== null &&
    (typeof event.project_mutation_retry_ms !== "number" ||
      !Number.isSafeInteger(event.project_mutation_retry_ms) ||
      event.project_mutation_retry_ms < 0)) {
    throw new TypeError("Fleet project mutation retry is invalid");
  }
  if (!Array.isArray(event.runtimes)) return;
  for (const value of event.runtimes) {
    if (!value || typeof value !== "object" || Array.isArray(value)) continue;
    const runtime = value as Record<string, JsonValue>;
    if (
      "availability_reason" in runtime &&
      !SANDBOX_AVAILABILITY_REASON_SET.has(String(runtime.availability_reason))
    ) throw new TypeError("Fleet runtime availability reason is invalid");
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
  if (event.event === "websocket.payload_rejected" || event.event === "websocket.http_fallback") {
    if (
      typeof event.limit_bytes !== "number" ||
      !Number.isSafeInteger(event.limit_bytes) ||
      event.limit_bytes < 1 || event.limit_bytes > 64 * 1024 * 1024 ||
      (event.event === "websocket.payload_rejected"
        ? typeof event.http_fallback_available !== "boolean"
        : event.status !== 426)
    ) throw new TypeError("WebSocket transport telemetry fields are invalid");
  }
  assertSafeRouterTelemetry(event);
  assertSafeFleetRelease(event);
  if (RECOVERY_EVENTS.has(event.event)) assertSafeRecoveryFields(event);
  if (FLEET_LIFECYCLE_EVENTS.has(event.event)) assertSafeFleetLifecycleFields(event);
  if (event.event === "sandbox.liveness") assertSafeLivenessFields(event);
}
