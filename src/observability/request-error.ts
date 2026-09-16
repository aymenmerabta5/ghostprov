import { types as utilTypes } from "node:util";
import {
  SANDBOX_RECOVERY_ERROR_KINDS,
  SANDBOX_RECOVERY_ERROR_NAMES,
  V2_SIDECAR_FATAL_ERROR_NAMES,
} from "./events";

/** Persistent request diagnostics are categories, never excerpts of an error. */
export const REQUEST_FAILURE_MESSAGE = "Request failed";
export const REQUEST_FAILURE_NAMES: ReadonlySet<string> = new Set([
  ...SANDBOX_RECOVERY_ERROR_NAMES, ...V2_SIDECAR_FATAL_ERROR_NAMES,
  "ApiError", "RelayError", "TimeoutError", "V2CommittedError", "V2PreCommitError",
  "V2EventMemoryError", "RouterShutdownError", "RouterCapacityTimeoutError",
  "RouterTransportAdmissionTimeoutError", "RouterRouteAttemptTimeoutError", "RouterQueueFullError",
  "ResourceReservationTooLargeError", "ResourceGovernorQueueFullError",
  "ResourceGovernorReplacementOverloadError", "ResourceGovernorTimeoutError",
  "ResourceGovernorClosedError", "ResourceGovernorCapacityUnavailableError",
  "ResponseProvenanceScopeError", "ResponseProvenanceObsoleteError", "ResponseProvenanceCommitError",
  "CpuTransformQueueFullError", "CpuTransformTimeoutError", "CpuTransformShutdownError",
  "ResponsesInputJsonError", "ResponsesInputUnicodeError", "ResponsesInputStructureError",
  "WorkloadTraversalLimitError",
]);
export const REQUEST_FAILURE_KINDS: ReadonlySet<string> = new Set([
  ...SANDBOX_RECOVERY_ERROR_KINDS,
  "ambiguous", "stream", "upstream", "internal", "invalid_json", "invalid_value",
  "invalid_request_error", "model_not_found", "previous_response_not_found", "cyber_policy",
  "context_length_exceeded", "insufficient_quota", "rate_limit_exceeded", "server_error",
  "ack_before_start", "ack_digest", "cancelled", "cancelled_precommit", "capacity_changed",
  "cpu_transform_overloaded", "cpu_transform_shutdown_timeout", "cpu_transform_timeout", "draining",
  "event_memory_capacity", "event_memory_delivery", "execution_risk_handle_unproven", "execution_risk_reflected",
  "invalid_function_call_arguments", "invalid_json_structure", "invalid_unicode", "late_handle_after_cancellation",
  "missing_prelude", "not_connected", "prelude_conflict", "preparation_capacity", "prepared_fence", "prepared_expired", "prepared_failed",
  "recovery_prefix_mismatch", "recovery_prefix_missing", "request_id_collision", "request_preparation_timeout",
  "request_too_large", "resource_admission_overloaded", "resource_admission_timeout", "resource_capacity_unavailable",
  "resource_governor_closed", "resource_replacement_overloaded", "resource_reservation_too_large",
  "response_conflict", "response_encoding", "response_gap", "response_provenance_commit_failed",
  "response_provenance_obsolete", "response_provenance_scope_mismatch", "response_size_mismatch", "response_too_large",
  "router_capacity_timeout", "router_queue_full", "router_route_attempt_timeout", "router_shutting_down",
  "sandbox_recovery", "sidecar_recovering", "sidecar_recovery", "sidecar_recovery_suspend_failed",
  "sidecar_recovery_unresolved", "startup_recovery_capacity", "terminal_conflict", "terminal_count", "terminal_proof",
  "terminal_receipt_capacity", "timeout_cancellation_unproven", "transport_admission_timeout",
]);

function ownData(value: unknown, key: string): unknown {
  if (!value || typeof value !== "object" || utilTypes.isProxy(value)) return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && "value" in descriptor ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function failureName(error: unknown): unknown {
  if (utilTypes.isProxy(error)) return "Error";
  const ownName = ownData(error, "name");
  if (ownName !== undefined) return ownName;
  try {
    // Unrestricted instanceof can enter a proxy hidden in an ordinary
    // object's prototype chain. Inspect at most eight non-proxy prototypes.
    let current = error;
    for (let depth = 0; depth < 8 && current !== null &&
      (typeof current === "object" || typeof current === "function"); depth++) {
      if (utilTypes.isProxy(current)) return "Error";
      const prototype: object | null = Object.getPrototypeOf(current);
      if (prototype === DOMException.prototype) {
        // Only the native getter may read a branded DOMException's name.
        return Object.getOwnPropertyDescriptor(DOMException.prototype, "name")?.get?.call(error);
      }
      if (prototype === TypeError.prototype) return "TypeError";
      if (prototype === RangeError.prototype) return "RangeError";
      if (prototype === SyntaxError.prototype) return "SyntaxError";
      current = prototype;
    }
  } catch { /* Unknown/custom objects use the fixed fallback. */ }
  return "Error";
}

export function safeRequestFailureStatus(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value : null;
}

/** Never reads message/cause/payload, invokes caller accessors, or mutates the error. */
export function requestFailureFields(error: unknown, fallbackStatus?: unknown): {
  error_name: string;
  error_kind: string | null;
  error_message: typeof REQUEST_FAILURE_MESSAGE;
  status: number | null;
  committed: boolean;
  quarantined: boolean;
} {
  const name = failureName(error);
  const kind = ownData(error, "kind") ?? ownData(error, "code");
  return {
    error_name: typeof name === "string" && REQUEST_FAILURE_NAMES.has(name) ? name : "Error",
    error_kind: kind === undefined || kind === null ? null
      : typeof kind === "string" && REQUEST_FAILURE_KINDS.has(kind) ? kind : "other",
    error_message: REQUEST_FAILURE_MESSAGE,
    status: safeRequestFailureStatus(ownData(error, "status")) ?? safeRequestFailureStatus(fallbackStatus),
    committed: ownData(error, "committed") === true,
    quarantined: ownData(error, "ambiguous") === true || kind === "ambiguous",
  };
}
