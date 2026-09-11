export type ObservabilityPriority = "terminal" | "progress" | "sample";

/** Bounded router scheduler classes safe to expose in request telemetry. */
export const ROUTER_QUEUE_CLASSES = ["latency", "bulk"] as const;
export type RouterQueueClass = typeof ROUTER_QUEUE_CLASSES[number];

export const FLEET_LIFECYCLE_EVENT_NAMES = [
  "fleet.replacement.qualified",
  "fleet.replacement.started",
  "fleet.replacement.successor_reserved",
  "fleet.replacement.successor_created",
  "fleet.replacement.successor_activated",
  "fleet.replacement.old_retired",
  "fleet.mutation.blocked",
  "fleet.startup_permits.changed",
] as const;
export type FleetLifecycleObservabilityEventName =
  typeof FLEET_LIFECYCLE_EVENT_NAMES[number];

export const FLEET_REPLACEMENT_FAILURE_KINDS = [
  "hello", "readiness", "render", "timeout",
] as const;
export const FLEET_RETIREMENT_REASONS = ["replacement", "missing"] as const;
export const FLEET_MUTATION_OPERATIONS = ["replace", "create", "delete"] as const;
export const FLEET_MUTATION_BLOCK_REASONS = [
  "release_policy",
  "recovery_circuit",
  "mutation_backoff",
  "runtime_missing",
  "runtime_healthy",
  "work_in_flight",
  "continuation_binding",
  "healthy_majority",
  "project_client",
  "control_plane",
] as const;
export const FLEET_STARTUP_PERMIT_ACTIONS = ["acquired", "released"] as const;

export const SANDBOX_AVAILABILITY_REASONS = [
  "available",
  "lifecycle_unavailable",
  "sidecar_suspect",
  "credential_refresh",
  "awareness_reconnect",
  "transport_unavailable",
  "transport_recovery",
  "transport_congested",
  "zero_remote_credits",
  "host_ceiling",
  "execution_window",
  "transfer_budget",
  "quarantine",
] as const;

export const RECOVERY_OBSERVABILITY_EVENT_NAMES = [
  "sandbox.recovery.started",
  "sandbox.recovery.retry_scheduled",
  "sandbox.recovery.completed",
  "sandbox.recovery.aborted",
  "sandbox.credential_refresh",
  "prism.circuit.changed",
] as const;

export type RecoveryObservabilityEventName =
  typeof RECOVERY_OBSERVABILITY_EVENT_NAMES[number];

export const SANDBOX_LIVENESS_SIGNALS = [
  "heartbeat",
  "sidecar_probe",
  "sidecar_recovery",
  "awareness",
] as const;
export type SandboxLivenessSignal = typeof SANDBOX_LIVENESS_SIGNALS[number];

export const SANDBOX_LIVENESS_OUTCOMES = [
  "failed",
  "recovered",
  "started",
  "succeeded",
  "preflight_unavailable",
  "superseded",
] as const;
export type SandboxLivenessOutcome = typeof SANDBOX_LIVENESS_OUTCOMES[number];

export const SANDBOX_LIVENESS_ERROR_NAMES = [
  "AccountAuthError",
  "AccountTransportError",
  "AwarenessChannelError",
  "Error",
  "PrismProjectError",
  "PrismSessionError",
  "ProvisioningError",
  "SandboxGatewayError",
  "SidecarCredentialRotationError",
  "SplitAwarenessChannelError",
  "V2SidecarRecoveryPreflightUnavailableError",
  "V2SupervisorError",
  "V2TransportLivenessError",
  "YDocSessionError",
] as const;
export type SandboxLivenessErrorName = typeof SANDBOX_LIVENESS_ERROR_NAMES[number];

export const SANDBOX_LIVENESS_ERROR_KINDS = [
  "auth",
  "account_transport",
  "closed",
  "connect",
  "disconnected",
  "http",
  "instance_changed",
  "other",
  "preflight_unavailable",
  "provision_timeout",
  "provision_transport",
  "probe_in_flight",
  "publish",
  "rate_limit",
  "reliable_stall",
  "server",
  "state",
  "sync",
  "timeout",
  "transport",
  "unavailable",
] as const;
export type SandboxLivenessErrorKind = typeof SANDBOX_LIVENESS_ERROR_KINDS[number];

export const SANDBOX_RECONNECT_PHASES = ["mint_credential", "supervisor_rotation", "host_reconnect", "sidecar_rotation"] as const;
export type SandboxReconnectPhase = typeof SANDBOX_RECONNECT_PHASES[number];
export const SANDBOX_SUPERVISOR_PHASES = ["transport", "bootstrap", "sequence", "challenge", "command"] as const;
export type SandboxSupervisorPhase = typeof SANDBOX_SUPERVISOR_PHASES[number];

export const SANDBOX_AWARENESS_LANES = ["data", "control", "unknown"] as const;
export type SandboxAwarenessLane = typeof SANDBOX_AWARENESS_LANES[number];

/**
 * Recovery events deliberately use bounded categories instead of free-form
 * reasons. Raw exception messages and supervisor reason strings must never be
 * copied into observability records.
 */
export const SANDBOX_RECOVERY_CAUSES = [
  "startup",
  "heartbeat",
  "sidecar",
  "credentials",
  "runtime",
  "snapshot",
  "shutdown",
  "circuit",
  "unknown",
] as const;
export type SandboxRecoveryCause = typeof SANDBOX_RECOVERY_CAUSES[number];

export const SANDBOX_RECOVERY_TRANSPORTS = ["v2"] as const;
export type SandboxRecoveryTransport = typeof SANDBOX_RECOVERY_TRANSPORTS[number];

export const SANDBOX_RECOVERY_OPERATIONS = [
  "provision",
  "bootstrap",
  "heartbeat",
  "sidecar_ping",
  "sidecar_liveness",
  "credential_refresh",
  "account_revalidate",
  "replace",
  "refresh_resources",
  "render",
  "wait_for_sync",
  "sync",
  "stop",
  "probe",
  "unknown",
] as const;
export type SandboxRecoveryOperation = typeof SANDBOX_RECOVERY_OPERATIONS[number];

export const SANDBOX_RECOVERY_ERROR_NAMES = [
  "AbortError",
  "AccountAuthError",
  "AccountTransportError",
  "AwarenessChannelError",
  "BootstrapCryptoError",
  "ChildStartupTimeoutError",
  "Error",
  "PrismProjectError",
  "PrismSessionError",
  "ProvisioningError",
  "SandboxGatewayError",
  "SidecarCredentialRotationError",
  "UnhealthyChildError",
  "V2BootstrapError",
  "V2RuntimeLifecycleFailure",
  "V2TransportLivenessError",
  "YDocSessionError",
] as const;
export type SandboxRecoveryErrorName = typeof SANDBOX_RECOVERY_ERROR_NAMES[number];

export const SANDBOX_RECOVERY_ERROR_KINDS = [
  "local",
  "auth",
  "rate_limit",
  "server",
  "account_transport",
  "provision_transport",
  "provision_timeout",
  "other",
  "auth_required",
  "guest_identity_lost",
  "mode_mismatch",
  "protocol",
  "reprovision",
  "timeout",
  "transport",
  "heartbeat_expired",
  "heartbeat_unreachable",
  "sidecar_unreachable",
  "ambiguity_exhausted",
  "resource_credential_expired",
  "sync",
  "missing",
  "stopped",
  "http",
  "closed",
  "connect",
  "state",
  "deploy",
  "render",
  "hello",
  "crypto",
  "readiness",
  "unavailable",
  "probe_in_flight",
  "publish",
  "instance_changed",
] as const;
export type SandboxRecoveryErrorKind = typeof SANDBOX_RECOVERY_ERROR_KINDS[number];

/** Exact secret-free atoms the R2 sidecar may emit in its fatal JSON record. */
export const V2_SIDECAR_FATAL_ERROR_NAMES = [
  "AdmissionError",
  "AggregateError",
  "AwarenessChannelError",
  "ClaimJournalError",
  "DOMException",
  "Error",
  "JournalStorageCapacityError",
  "JournalStorageDurabilityError",
  "NotCapable",
  "RangeError",
  "RequestAssemblyError",
  "SidecarCloseError",
  "SidecarCredentialRotationError",
  "SidecarLifecycleError",
  "SidecarOwnershipError",
  "SyntaxError",
  "TypeError",
  "UpstreamExecutionError",
  "V2ExecutionFenceError",
  "unknown",
] as const;
export type V2SidecarFatalErrorName = typeof V2_SIDECAR_FATAL_ERROR_NAMES[number];

export const V2_SIDECAR_FATAL_ERROR_CODES = [
  "cleanup_timeout",
  "contended",
  "event_memory_leak",
  "fatal",
  "forced_drain",
  "instance_mismatch",
  "lost",
  "storage",
  "unknown",
] as const;
export type V2SidecarFatalErrorCode = typeof V2_SIDECAR_FATAL_ERROR_CODES[number];

export const V2_SIDECAR_FATAL_CAUSE_NAMES = [
  "AggregateError",
  "AlreadyExists",
  "BadResource",
  "DOMException",
  "Error",
  "NotCapable",
  "NotFound",
  "PermissionDenied",
  "RangeError",
  "SidecarLifecycleError",
  "SidecarOwnershipError",
  "SyntaxError",
  "TypeError",
  "unknown",
] as const;
export type V2SidecarFatalCauseName = typeof V2_SIDECAR_FATAL_CAUSE_NAMES[number] | null;

export const V2_SIDECAR_FATAL_PHASES = [
  "awareness_connect",
  "bootstrap",
  "config",
  "engine_start",
  "environment",
  "ownership",
  "runtime",
  "storage",
  "unknown",
] as const;
export type V2SidecarFatalPhase = typeof V2_SIDECAR_FATAL_PHASES[number];

export const V2_SIDECAR_FATAL_PERMISSIONS = [
  "env", "ffi", "net", "read", "run", "sys", "write", "unknown",
] as const;
export type V2SidecarFatalPermission = typeof V2_SIDECAR_FATAL_PERMISSIONS[number] | null;

export const CREDENTIAL_REFRESH_OUTCOMES = [
  "cookie_retry_succeeded",
  "updated_in_place",
  "replacement_required",
  "auth_rejected",
  "transport_failed",
  "aborted",
  "not_attempted",
] as const;
export type CredentialRefreshOutcome = typeof CREDENTIAL_REFRESH_OUTCOMES[number];

export const PRISM_CIRCUIT_STATES = [
  "closed",
  "revalidating",
  "open",
  "half_open",
  "closed_for_shutdown",
] as const;
export type PrismCircuitState = typeof PRISM_CIRCUIT_STATES[number];

export const PRISM_CIRCUIT_SCOPES = ["auth", "outage"] as const;
export type PrismCircuitScope = typeof PRISM_CIRCUIT_SCOPES[number] | null;

export type ObservabilityEventName =
  | "server.started"
  | "server.stopping"
  | "request.accepted"
  | "admission.acquired"
  | "request.queued"
  | "route.selected"
  | "route.attempt_expired"
  | "transport.committed"
  | "response.first_event"
  | "request.completed"
  | "request.failed"
  | "websocket.payload_rejected"
  | "websocket.http_fallback"
  | "fleet.sample"
  | "monitor.sample"
  | "monitor.error"
  | "logger.dropped"
  | "logger.error"
  | "sandbox.liveness"
  | FleetLifecycleObservabilityEventName
  | RecoveryObservabilityEventName;

export type JsonScalar = string | number | boolean | null;
export type JsonValue = JsonScalar | JsonValue[] | { [key: string]: JsonValue };

export interface ObservabilityEvent {
  schema: 1;
  event: ObservabilityEventName;
  ts: string;
  mono_ms: number;
  [key: string]: JsonValue;
}

/** Secret-free fields accepted by recovery, refresh, and circuit events. */
export interface RecoveryObservabilityFields {
  project_id?: string;
  worker_index?: number;
  transport?: SandboxRecoveryTransport;
  cause?: SandboxRecoveryCause;
  attempt?: number;
  incarnation?: number;
  leader_generation?: number;
  consecutive_failures?: number;
  status?: number | null;
  error_name?: SandboxRecoveryErrorName | null;
  error_kind?: SandboxRecoveryErrorKind | null;
  bootstrap_phase?: import("../sandbox/types").SandboxFailureSnapshot["bootstrapPhase"] | null;
  bootstrap_code?: import("../sandbox/types").SandboxFailureSnapshot["bootstrapCode"] | null;
  sidecar_error_name?: V2SidecarFatalErrorName | null;
  sidecar_error_code?: V2SidecarFatalErrorCode | null;
  sidecar_cause_name?: V2SidecarFatalCauseName;
  sidecar_phase?: V2SidecarFatalPhase | null;
  sidecar_permission?: V2SidecarFatalPermission;
  operation?: SandboxRecoveryOperation | null;
  retry_delay_ms?: number;
  attempt_duration_ms?: number;
  duration_ms?: number;
  credential_refresh_outcome?: CredentialRefreshOutcome;
  circuit_state?: PrismCircuitState;
  circuit_scope?: PrismCircuitScope;
  queued?: number;
}

/** Fixed, content-free liveness transitions; routine successful probes are omitted. */
export interface SandboxLivenessObservabilityFields {
  project_id: string;
  worker_index: number;
  transport: SandboxRecoveryTransport;
  liveness_signal: SandboxLivenessSignal;
  liveness_outcome: SandboxLivenessOutcome;
  attempt?: number;
  consecutive_failures?: number;
  status?: number | null;
  error_name?: SandboxLivenessErrorName | null;
  error_kind?: SandboxLivenessErrorKind | null;
  duration_ms?: number;
  heartbeat_age_ms?: number | null;
  sidecar_peer_age_ms?: number | null;
  transport_connected?: boolean;
  reliable_stall?: boolean;
  awareness_lane?: SandboxAwarenessLane;
  connection_code?: number | null;
  reconnect_phase?: SandboxReconnectPhase | null;
  supervisor_phase?: SandboxSupervisorPhase | null;
}

/** Fixed, content-free fields accepted by fleet lifecycle events. */
export interface FleetLifecycleObservabilityFields {
  project_id?: string;
  worker_index?: number;
  leader_generation?: number;
  failure_kind?: typeof FLEET_REPLACEMENT_FAILURE_KINDS[number];
  failure_count?: number;
  retirement_reason?: typeof FLEET_RETIREMENT_REASONS[number];
  managed?: boolean;
  mutation_operation?: typeof FLEET_MUTATION_OPERATIONS[number];
  block_reason?: typeof FLEET_MUTATION_BLOCK_REASONS[number];
  cold_start?: boolean;
  permit_action?: typeof FLEET_STARTUP_PERMIT_ACTIONS[number];
  total_limit?: number;
  total_in_use?: number;
  total_queued?: number;
  cold_limit?: number;
  cold_in_use?: number;
  cold_queued?: number;
}

const RECOVERY_CAUSE_SET = new Set<string>(SANDBOX_RECOVERY_CAUSES);
const RECOVERY_OPERATION_SET = new Set<string>(SANDBOX_RECOVERY_OPERATIONS);
const RECOVERY_ERROR_NAME_SET = new Set<string>(SANDBOX_RECOVERY_ERROR_NAMES);
const RECOVERY_ERROR_KIND_SET = new Set<string>(SANDBOX_RECOVERY_ERROR_KINDS);
const V2_SIDECAR_FATAL_ERROR_NAME_SET = new Set<string>(V2_SIDECAR_FATAL_ERROR_NAMES);
const V2_SIDECAR_FATAL_ERROR_CODE_SET = new Set<string>(V2_SIDECAR_FATAL_ERROR_CODES);
const V2_SIDECAR_FATAL_CAUSE_NAME_SET = new Set<string>(V2_SIDECAR_FATAL_CAUSE_NAMES);
const V2_SIDECAR_FATAL_PHASE_SET = new Set<string>(V2_SIDECAR_FATAL_PHASES);
const V2_SIDECAR_FATAL_PERMISSION_SET = new Set<string>(V2_SIDECAR_FATAL_PERMISSIONS);

/** Collapse a possibly dynamic supervisor reason to a fixed safe category. */
export function normalizeSandboxRecoveryCause(value: unknown): SandboxRecoveryCause {
  if (typeof value !== "string") return "unknown";
  const normalized = value.trim().toLowerCase();
  if (RECOVERY_CAUSE_SET.has(normalized)) return normalized as SandboxRecoveryCause;
  if (normalized.includes("heartbeat")) return "heartbeat";
  if (
    normalized.includes("sidecar") ||
    normalized.includes("liveness") ||
    normalized.includes("ambiguity")
  ) return "sidecar";
  if (
    normalized.includes("credential") ||
    normalized.includes("auth") ||
    normalized.includes("resource")
  ) return "credentials";
  if (normalized.includes("snapshot")) return "snapshot";
  if (normalized === "start" || normalized.includes("startup")) return "startup";
  if (
    normalized.includes("shutdown") ||
    normalized.includes("drain") ||
    normalized.includes("stop") ||
    normalized.includes("abort")
  ) return "shutdown";
  if (normalized.includes("circuit")) return "circuit";
  if (
    normalized.includes("runtime") ||
    normalized.includes("state") ||
    normalized.includes("failed") ||
    normalized.includes("stuck")
  ) return "runtime";
  return "unknown";
}

/** Preserve only known operation atoms; all other values become `unknown`. */
export function normalizeSandboxRecoveryOperation(value: unknown): SandboxRecoveryOperation {
  if (typeof value !== "string") return "unknown";
  const normalized = value.trim().toLowerCase().replace(/[\s-]+/g, "_");
  return RECOVERY_OPERATION_SET.has(normalized)
    ? normalized as SandboxRecoveryOperation
    : "unknown";
}

/** Preserve only known exception class names; all other values become `Error`. */
export function normalizeSandboxRecoveryErrorName(value: unknown): SandboxRecoveryErrorName {
  return typeof value === "string" && RECOVERY_ERROR_NAME_SET.has(value)
    ? value as SandboxRecoveryErrorName
    : "Error";
}

/** Preserve only known failure categories; all other values become `other`. */
export function normalizeSandboxRecoveryErrorKind(value: unknown): SandboxRecoveryErrorKind {
  return typeof value === "string" && RECOVERY_ERROR_KIND_SET.has(value)
    ? value as SandboxRecoveryErrorKind
    : "other";
}

export function normalizeV2SidecarFatalErrorName(value: unknown): V2SidecarFatalErrorName {
  return typeof value === "string" && V2_SIDECAR_FATAL_ERROR_NAME_SET.has(value)
    ? value as V2SidecarFatalErrorName
    : "unknown";
}

export function normalizeV2SidecarFatalErrorCode(value: unknown): V2SidecarFatalErrorCode {
  return typeof value === "string" && V2_SIDECAR_FATAL_ERROR_CODE_SET.has(value)
    ? value as V2SidecarFatalErrorCode
    : "unknown";
}

export function normalizeV2SidecarFatalCauseName(value: unknown): V2SidecarFatalCauseName {
  if (value === null || value === undefined) return null;
  return typeof value === "string" && V2_SIDECAR_FATAL_CAUSE_NAME_SET.has(value)
    ? value as Exclude<V2SidecarFatalCauseName, null>
    : "unknown";
}

export function normalizeV2SidecarFatalPhase(value: unknown): V2SidecarFatalPhase {
  return typeof value === "string" && V2_SIDECAR_FATAL_PHASE_SET.has(value)
    ? value as V2SidecarFatalPhase
    : "unknown";
}

export function normalizeV2SidecarFatalPermission(value: unknown): V2SidecarFatalPermission {
  if (value === null || value === undefined) return null;
  return typeof value === "string" && V2_SIDECAR_FATAL_PERMISSION_SET.has(value)
    ? value as Exclude<V2SidecarFatalPermission, null>
    : "unknown";
}

export function observabilityEvent(
  event: ObservabilityEventName,
  fields: Record<string, JsonValue> = {},
  now = Date.now(),
  monoMs = performance.now(),
): ObservabilityEvent {
  return Object.freeze({
    schema: 1 as const,
    event,
    ts: new Date(now).toISOString(),
    mono_ms: Math.round(monoMs * 1000) / 1000,
    ...fields,
  });
}

/** Typed constructor which prevents free-form recovery fields at call sites. */
export function recoveryObservabilityEvent(
  event: RecoveryObservabilityEventName,
  fields: RecoveryObservabilityFields = {},
  now = Date.now(),
  monoMs = performance.now(),
): ObservabilityEvent {
  return observabilityEvent(
    event,
    fields as unknown as Record<string, JsonValue>,
    now,
    monoMs,
  );
}

export function sandboxLivenessObservabilityEvent(
  fields: SandboxLivenessObservabilityFields,
  now = Date.now(),
  monoMs = performance.now(),
): ObservabilityEvent {
  return observabilityEvent(
    "sandbox.liveness",
    fields as unknown as Record<string, JsonValue>,
    now,
    monoMs,
  );
}

export function fleetLifecycleObservabilityEvent(
  event: FleetLifecycleObservabilityEventName,
  fields: FleetLifecycleObservabilityFields = {},
  now = Date.now(),
  monoMs = performance.now(),
): ObservabilityEvent {
  return observabilityEvent(
    event,
    fields as unknown as Record<string, JsonValue>,
    now,
    monoMs,
  );
}
