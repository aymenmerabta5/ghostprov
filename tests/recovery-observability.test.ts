import { describe, expect, it } from "bun:test";
import {
  CREDENTIAL_REFRESH_OUTCOMES,
  PRISM_CIRCUIT_SCOPES,
  PRISM_CIRCUIT_STATES,
  RECOVERY_OBSERVABILITY_EVENT_NAMES,
  normalizeSandboxRecoveryCause,
  normalizeSandboxRecoveryErrorKind,
  normalizeSandboxRecoveryErrorName,
  normalizeSandboxRecoveryOperation,
  observabilityEvent,
  recoveryObservabilityEvent,
} from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";

const PROJECT_ID = "e631d12c-d917-4434-b360-41b4715d6679";

describe("recovery observability schema", () => {
  it("accepts the fixed recovery, refresh, and circuit event vocabulary", () => {
    for (const eventName of RECOVERY_OBSERVABILITY_EVENT_NAMES) {
      const event = recoveryObservabilityEvent(eventName, {
        project_id: PROJECT_ID,
        worker_index: 0,
        transport: "v2",
        cause: "heartbeat",
        attempt: 2,
        incarnation: 3,
        leader_generation: 4,
        consecutive_failures: 1,
        status: 503,
        error_name: "V2TransportLivenessError",
        error_kind: "timeout",
        operation: "sidecar_ping",
        retry_delay_ms: 1_000,
        attempt_duration_ms: 250.5,
        duration_ms: 1_250.5,
        credential_refresh_outcome: "not_attempted",
        circuit_state: "open",
        circuit_scope: "outage",
        queued: 2,
      });
      expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
    }
  });

  it("accepts every credential outcome and circuit state/scope category", () => {
    for (const credential_refresh_outcome of CREDENTIAL_REFRESH_OUTCOMES) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent(
        "sandbox.credential_refresh",
        { project_id: PROJECT_ID, credential_refresh_outcome },
      ))).not.toThrow();
    }
    for (const circuit_state of PRISM_CIRCUIT_STATES) {
      for (const circuit_scope of [...PRISM_CIRCUIT_SCOPES, null]) {
        expect(() => assertSafeObservabilityEvent(observabilityEvent(
          "prism.circuit.changed",
          { circuit_state, circuit_scope, consecutive_failures: 0, queued: 0 },
        ))).not.toThrow();
      }
    }
  });

  it("normalizes dynamic reasons and errors without retaining free-form text", () => {
    expect(normalizeSandboxRecoveryCause("state:auth_required")).toBe("credentials");
    expect(normalizeSandboxRecoveryCause("heartbeat-stale")).toBe("heartbeat");
    expect(normalizeSandboxRecoveryCause("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeSandboxRecoveryOperation("wait-for-sync")).toBe("wait_for_sync");
    expect(normalizeSandboxRecoveryOperation("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeSandboxRecoveryErrorName("ProvisioningError")).toBe("ProvisioningError");
    expect(normalizeSandboxRecoveryErrorName("SUPER_SECRET_CANARY")).toBe("Error");
    expect(normalizeSandboxRecoveryErrorKind("rate_limit")).toBe("rate_limit");
    expect(normalizeSandboxRecoveryErrorKind("SUPER_SECRET_CANARY")).toBe("other");
  });

  it("rejects free-form canaries, invalid identifiers, and invalid numbers", () => {
    const invalidFields: Array<Record<string, unknown>> = [
      { cause: "SUPER_SECRET_CANARY" },
      { operation: "send the raw request" },
      { error_name: "SUPER_SECRET_CANARY" },
      { error_kind: "SUPER_SECRET_CANARY" },
      { project_id: "project-SUPER_SECRET_CANARY" },
      { worker_index: -1 },
      { attempt: 1.5 },
      { retry_delay_ms: Number.POSITIVE_INFINITY },
      { status: 42 },
      { transport: null },
      { credential_refresh_outcome: null },
      { circuit_state: null },
    ];
    for (const fields of invalidFields) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent(
        "sandbox.recovery.retry_scheduled",
        fields as any,
      ))).toThrow();
    }
  });
});
