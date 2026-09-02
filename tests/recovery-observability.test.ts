import { describe, expect, it } from "bun:test";
import {
  CREDENTIAL_REFRESH_OUTCOMES,
  FLEET_LIFECYCLE_EVENT_NAMES,
  PRISM_CIRCUIT_SCOPES,
  PRISM_CIRCUIT_STATES,
  RECOVERY_OBSERVABILITY_EVENT_NAMES,
  normalizeV2SidecarFatalCauseName,
  normalizeV2SidecarFatalErrorCode,
  normalizeV2SidecarFatalErrorName,
  normalizeV2SidecarFatalPermission,
  normalizeV2SidecarFatalPhase,
  normalizeSandboxRecoveryCause,
  normalizeSandboxRecoveryErrorKind,
  normalizeSandboxRecoveryErrorName,
  normalizeSandboxRecoveryOperation,
  observabilityEvent,
  fleetLifecycleObservabilityEvent,
  recoveryObservabilityEvent,
} from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { describeSandboxFailure } from "../src/sandbox/failure";
import { V2BootstrapError } from "../src/v2/bootstrap/controller";

const PROJECT_ID = "e631d12c-d917-4434-b360-41b4715d6679";

describe("recovery observability schema", () => {
  it("carries only structured sidecar fatal atoms through the sandbox failure snapshot", () => {
    const failure = describeSandboxFailure(new V2BootstrapError(
      "sidecar failed",
      "hello",
      1,
      {
        sidecarDiagnostic: {
          errorName: "SidecarOwnershipError",
          errorCode: "contended",
          causeName: null,
        },
      },
    ));

    expect(failure).toMatchObject({
      name: "V2BootstrapError",
      kind: "hello",
      operation: "bootstrap",
      sidecarErrorName: "SidecarOwnershipError",
      sidecarErrorCode: "contended",
      sidecarCauseName: null,
    });
  });

  it("recovers structured sidecar diagnostics through bounded lifecycle wrappers", () => {
    const nested = new V2BootstrapError(
      "sidecar failed",
      "hello",
      1,
      {
        sidecarDiagnostic: {
          errorName: "SidecarOwnershipError",
          errorCode: "contended",
          causeName: null,
        },
      },
    );
    const aggregate = new AggregateError([new Error("cleanup failed"), nested], "retirement failed");
    const outer = new Error("runtime failed", { cause: aggregate }) as Error & {
      kind: string;
      operation: string;
    };
    outer.name = "V2RuntimeLifecycleFailure";
    outer.kind = "sidecar_unreachable";
    outer.operation = "sidecar_liveness";

    expect(describeSandboxFailure(outer)).toMatchObject({
      name: "V2RuntimeLifecycleFailure",
      kind: "sidecar_unreachable",
      operation: "sidecar_liveness",
      sidecarErrorName: "SidecarOwnershipError",
      sidecarErrorCode: "contended",
      sidecarCauseName: null,
    });
  });

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
        sidecar_error_name: "SidecarOwnershipError",
        sidecar_error_code: "contended",
        sidecar_cause_name: null,
        sidecar_phase: "engine_start",
        sidecar_permission: "write",
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

  it("accepts the fixed fleet replacement and startup-permit vocabulary", () => {
    const shared = {
      project_id: PROJECT_ID,
      worker_index: 3,
      leader_generation: 7,
      failure_kind: "readiness" as const,
      failure_count: 3,
      retirement_reason: "replacement" as const,
      managed: true,
      mutation_operation: "replace" as const,
      block_reason: "healthy_majority" as const,
      cold_start: true,
      permit_action: "acquired" as const,
      total_limit: 4,
      total_in_use: 2,
      total_queued: 0,
      cold_limit: 2,
      cold_in_use: 2,
      cold_queued: 1,
    };
    for (const eventName of FLEET_LIFECYCLE_EVENT_NAMES) {
      expect(() => assertSafeObservabilityEvent(
        fleetLifecycleObservabilityEvent(eventName, shared),
      )).not.toThrow();
    }
  });

  it("rejects free-form fleet lifecycle categories and overcommitted permits", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent(
      "fleet.mutation.blocked",
      {
        project_id: PROJECT_ID,
        worker_index: 1,
        mutation_operation: "replace",
        block_reason: "SUPER_SECRET_CANARY",
      },
    ))).toThrow();
    expect(() => assertSafeObservabilityEvent(fleetLifecycleObservabilityEvent(
      "fleet.startup_permits.changed",
      {
        cold_start: true,
        permit_action: "acquired",
        total_limit: 4,
        total_in_use: 5,
        cold_limit: 2,
        cold_in_use: 2,
      },
    ))).toThrow();
  });

  it("normalizes dynamic reasons and errors without retaining free-form text", () => {
    expect(normalizeSandboxRecoveryCause("state:auth_required")).toBe("credentials");
    expect(normalizeSandboxRecoveryCause("heartbeat-stale")).toBe("heartbeat");
    expect(normalizeSandboxRecoveryCause("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeSandboxRecoveryOperation("wait-for-sync")).toBe("wait_for_sync");
    expect(normalizeSandboxRecoveryOperation("sidecar_liveness")).toBe("sidecar_liveness");
    expect(normalizeSandboxRecoveryOperation("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeSandboxRecoveryErrorName("ProvisioningError")).toBe("ProvisioningError");
    expect(normalizeSandboxRecoveryErrorName("SUPER_SECRET_CANARY")).toBe("Error");
    expect(normalizeSandboxRecoveryErrorKind("rate_limit")).toBe("rate_limit");
    expect(normalizeSandboxRecoveryErrorKind("SUPER_SECRET_CANARY")).toBe("other");
    expect(normalizeV2SidecarFatalErrorName("SidecarOwnershipError"))
      .toBe("SidecarOwnershipError");
    expect(normalizeV2SidecarFatalErrorCode("contended")).toBe("contended");
    expect(normalizeV2SidecarFatalCauseName("TypeError")).toBe("TypeError");
    expect(normalizeV2SidecarFatalPhase("engine_start")).toBe("engine_start");
    expect(normalizeV2SidecarFatalPermission("write")).toBe("write");
    expect(normalizeV2SidecarFatalErrorName("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeV2SidecarFatalErrorCode("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeV2SidecarFatalCauseName("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeV2SidecarFatalPhase("SUPER_SECRET_CANARY")).toBe("unknown");
    expect(normalizeV2SidecarFatalPermission("SUPER_SECRET_CANARY")).toBe("unknown");
  });

  it("rejects free-form canaries, invalid identifiers, and invalid numbers", () => {
    const invalidFields: Array<Record<string, unknown>> = [
      { cause: "SUPER_SECRET_CANARY" },
      { operation: "send the raw request" },
      { error_name: "SUPER_SECRET_CANARY" },
      { error_kind: "SUPER_SECRET_CANARY" },
      { sidecar_error_name: "SUPER_SECRET_CANARY" },
      { sidecar_error_code: "SUPER_SECRET_CANARY" },
      { sidecar_cause_name: "SUPER_SECRET_CANARY" },
      { sidecar_phase: "SUPER_SECRET_CANARY" },
      { sidecar_permission: "SUPER_SECRET_CANARY" },
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
