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
import { V2SupervisorBootstrapError } from "../src/v2/supervisor/controller";
import { V2SupervisorError } from "../src/v2/supervisor/host";
import { bootstrapCleanupFailureDiagnostic } from "../src/fleet/manager";
import { BOOTSTRAP_FAILURE_CODES, BOOTSTRAP_FAILURE_REASONS } from "../src/sandbox/types";

const PROJECT_ID = "e631d12c-d917-4434-b360-41b4715d6679";

describe("recovery observability schema", () => {
  it.each(["bootstrap", "challenge"] as const)("preserves a wrapped cleanup %s failure without raw details", phase => {
    const inner = new V2SupervisorError("PRIVATE_CLEANUP_CANARY https://private.example", "timeout", undefined, phase);
    const supervisor = new V2SupervisorBootstrapError("private supervisor", "deploy", {
      cause: inner, remoteCode: "bootstrap_cleanup_unproven", reason: phase,
    });
    const error = new V2BootstrapError("private wrapper", "deploy", 1, { cause: supervisor });
    const diagnostic = bootstrapCleanupFailureDiagnostic(error);
    expect(diagnostic).toEqual({ errorName: "V2SupervisorError", bootstrapPhase: "deploy",
      bootstrapCode: "bootstrap_cleanup_unproven", bootstrapReason: phase, supervisorPhase: phase });
    expect(JSON.stringify(diagnostic)).not.toContain("private");
  });

  it("retains the cleanup wrapper while classifying a journal-save cause separately from supervisor transport", () => {
    const error = new V2SupervisorBootstrapError("Previous bootstrap cleanup remains unproven", "deploy", {
      cause: new Error("PRIVATE_JOURNAL_PATH token=PRIVATE_CLEANUP_CANARY"), remoteCode: "bootstrap_cleanup_unproven",
    });
    const diagnostic = bootstrapCleanupFailureDiagnostic(error);
    expect(diagnostic.errorName).toBe("Error");
    expect(diagnostic.bootstrapCode).toBe("bootstrap_cleanup_unproven");
    expect(diagnostic.bootstrapPhase).toBe("deploy");
    expect(diagnostic.bootstrapReason).toBeUndefined();
    expect(diagnostic.supervisorPhase).toBeUndefined();
    expect(JSON.stringify(diagnostic)).not.toContain("PRIVATE");
  });

  it("bounds cleanup cause inspection to six records, stops cycles and never invokes accessors", () => {
    const cause = new V2SupervisorError("private", "timeout", undefined, "challenge");
    let boundary: unknown = cause;
    for (let index = 0; index < 5; index++) boundary = new Error("private", { cause: boundary });
    expect(bootstrapCleanupFailureDiagnostic(boundary).supervisorPhase).toBe("challenge");
    expect(bootstrapCleanupFailureDiagnostic(new Error("private", { cause: boundary })).supervisorPhase).toBeUndefined();
    const cyclic: Record<string, unknown> = { name: "V2SupervisorError", phase: "bootstrap" };
    cyclic.cause = cyclic;
    expect(bootstrapCleanupFailureDiagnostic(cyclic).supervisorPhase).toBe("bootstrap");
    const getters: Record<string, unknown> = {};
    let calls = 0;
    for (const key of ["name", "cause", "kind", "phase", "remoteCode", "reason", "message", "status", "code", "committed", "ambiguous"]) {
      Object.defineProperty(getters, key, { get: () => { calls++; throw new Error("must not execute"); } });
    }
    expect(bootstrapCleanupFailureDiagnostic(getters)).toEqual({ errorName: "Error" });
    expect(calls).toBe(0);
    expect(bootstrapCleanupFailureDiagnostic(new Proxy({}, {
      getOwnPropertyDescriptor: () => { throw new Error("private descriptor failure"); },
    }))).toEqual({ errorName: "Error" });
  });

  it("accepts only exact cleanup bootstrap atoms and ignores unrelated error payloads", () => {
    const diagnostic = bootstrapCleanupFailureDiagnostic({ name: "V2SupervisorBootstrapError", kind: "deploy\n",
      remoteCode: "bootstrap_cleanup_unproven/private", reason: "CHALLENGE", message: "PRIVATE_CLEANUP_CANARY",
      cause: { name: "V2SupervisorError", phase: "challenge\n", error: { name: "V2SupervisorError", phase: "command" } },
    });
    expect(diagnostic.errorName).toBe("V2SupervisorError");
    expect(diagnostic.bootstrapPhase).toBeUndefined();
    expect(diagnostic.bootstrapCode).toBeUndefined();
    expect(diagnostic.bootstrapReason).toBeUndefined();
    expect(diagnostic.supervisorPhase).toBeUndefined();
    expect(bootstrapCleanupFailureDiagnostic({ name: "PRIVATE_CLEANUP_CANARY" }).errorName).toBe("Error");
    expect(bootstrapCleanupFailureDiagnostic({ name: "Error", remoteCode: "bootstrap_cleanup_unproven", reason: "bootstrap", phase: "command" }))
      .toEqual({ errorName: "Error" });
  });

  it("retains only fixed recovery reasons and synchronization outcomes", () => {
    for (const reason of BOOTSTRAP_FAILURE_REASONS) {
      const inner = new V2SupervisorBootstrapError("private detail", "deploy", { remoteCode: "bootstrap_ownership_conflict", reason });
      const failure = describeSandboxFailure(new V2BootstrapError("deployment failed", "deploy", 1, { cause: inner }));
      expect(failure.bootstrapReason).toBe(reason);
      const event = recoveryObservabilityEvent("sandbox.recovery.retry_scheduled", { bootstrap_reason: failure.bootstrapReason });
      expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
      expect(() => assertSafeObservabilityEvent({ ...event, bootstrap_reason: "private-token" })).toThrow();
    }
    for (const code of BOOTSTRAP_FAILURE_CODES) {
      expect(() => assertSafeObservabilityEvent(recoveryObservabilityEvent("sandbox.recovery.retry_scheduled", { bootstrap_code: code }))).not.toThrow();
    }
  });
  it("preserves allowlisted bootstrap causes through deployment wrappers", () => {
    const inner = new V2SupervisorBootstrapError("private detail", "deploy", { remoteCode: "bootstrap_ownership_conflict" });
    const failure = describeSandboxFailure(new V2BootstrapError("deployment failed", "deploy", 1, { cause: inner }));
    expect(failure).toMatchObject({ bootstrapPhase: "deploy", bootstrapCode: "bootstrap_ownership_conflict" });
    const event = recoveryObservabilityEvent("sandbox.recovery.retry_scheduled", {
      bootstrap_phase: failure.bootstrapPhase, bootstrap_code: failure.bootstrapCode,
    });
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
    expect(JSON.stringify(event)).not.toContain("private detail");
    const unknown = new V2SupervisorBootstrapError("private detail", "deploy", { remoteCode: "secret-token-123" });
    expect(describeSandboxFailure(unknown).bootstrapCode).toBeUndefined();
    expect(() => assertSafeObservabilityEvent({ ...event, bootstrap_code: "secret-token-123" })).toThrow();
  });
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

  it("accepts the fixed fleet replacement, startup-permit, and cleanup vocabulary", () => {
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
      const fields = eventName.startsWith("fleet.bootstrap_cleanup.") ? {
        project_id: PROJECT_ID,
        worker_index: 3,
        leader_generation: 7,
        ...(eventName === "fleet.bootstrap_cleanup.deferred" ? { cleanup_reason: "proof_unavailable" as const } : {}),
      } : shared;
      expect(() => assertSafeObservabilityEvent(
        fleetLifecycleObservabilityEvent(eventName, fields),
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
    expect(normalizeSandboxRecoveryCause("heartbeat-stale")).toBe("heartbeat_stale");
    expect(normalizeSandboxRecoveryCause("heartbeat-missing")).toBe("heartbeat_missing");
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
