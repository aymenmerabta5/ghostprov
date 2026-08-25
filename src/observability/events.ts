export type ObservabilityPriority = "terminal" | "progress" | "sample";

/** Bounded router scheduler classes safe to expose in request telemetry. */
export const ROUTER_QUEUE_CLASSES = ["latency", "bulk"] as const;
export type RouterQueueClass = typeof ROUTER_QUEUE_CLASSES[number];

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
  "AggregateError",
  "DOMException",
  "Error",
  "RangeError",
  "SidecarLifecycleError",
  "SidecarOwnershipError",
  "SyntaxError",
  "TypeError",
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
  | "transport.committed"
  | "response.first_event"
  | "request.completed"
  | "request.failed"
  | "fleet.sample"
  | "monitor.sample"
  | "monitor.error"
  | "logger.dropped"
  | "logger.error"
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
  sidecar_error_name?: V2SidecarFatalErrorName | null;
  sidecar_error_code?: V2SidecarFatalErrorCode | null;
  sidecar_cause_name?: V2SidecarFatalCauseName;
  operation?: SandboxRecoveryOperation | null;
  retry_delay_ms?: number;
  attempt_duration_ms?: number;
  duration_ms?: number;
  credential_refresh_outcome?: CredentialRefreshOutcome;
  circuit_state?: PrismCircuitState;
  circuit_scope?: PrismCircuitScope;
  queued?: number;
}

const RECOVERY_CAUSE_SET = new Set<string>(SANDBOX_RECOVERY_CAUSES);
const RECOVERY_OPERATION_SET = new Set<string>(SANDBOX_RECOVERY_OPERATIONS);
const RECOVERY_ERROR_NAME_SET = new Set<string>(SANDBOX_RECOVERY_ERROR_NAMES);
const RECOVERY_ERROR_KIND_SET = new Set<string>(SANDBOX_RECOVERY_ERROR_KINDS);
const V2_SIDECAR_FATAL_ERROR_NAME_SET = new Set<string>(V2_SIDECAR_FATAL_ERROR_NAMES);
const V2_SIDECAR_FATAL_ERROR_CODE_SET = new Set<string>(V2_SIDECAR_FATAL_ERROR_CODES);
const V2_SIDECAR_FATAL_CAUSE_NAME_SET = new Set<string>(V2_SIDECAR_FATAL_CAUSE_NAMES);

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
