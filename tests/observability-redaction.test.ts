import { describe, expect, it } from "bun:test";
import {
  assertSafeObservabilityEvent,
  safeObservabilityReasoningEffort,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import {
  observabilityEvent,
  sandboxLivenessObservabilityEvent,
  fleetLifecycleObservabilityEvent,
  FLEET_BOOTSTRAP_CLEANUP_REASONS,
  FLEET_BOOTSTRAP_CLEANUP_STAGES,
  FLEET_BOOTSTRAP_CLEANUP_ERROR_NAMES,
  SANDBOX_SUPERVISOR_PHASES,
  type ObservabilityEvent,
  type JsonValue,
} from "../src/observability/events";
import { V2_R3_RELEASE } from "../src/v2/sidecar/assets";
import { BOOTSTRAP_FAILURE_CODES, BOOTSTRAP_FAILURE_PHASES, BOOTSTRAP_FAILURE_REASONS } from "../src/sandbox/types";

describe("observability privacy", () => {
  it("keeps cleanup failure diagnostics optional, fixed and confined to deferred events", () => {
    const identity = { project_id: "00000000-0000-4000-8000-000000000032", worker_index: 31, leader_generation: 7 };
    for (const [key, values] of [
      ["cleanup_stage", FLEET_BOOTSTRAP_CLEANUP_STAGES], ["error_name", FLEET_BOOTSTRAP_CLEANUP_ERROR_NAMES],
      ["bootstrap_phase", BOOTSTRAP_FAILURE_PHASES], ["bootstrap_code", BOOTSTRAP_FAILURE_CODES],
      ["bootstrap_reason", BOOTSTRAP_FAILURE_REASONS], ["supervisor_phase", SANDBOX_SUPERVISOR_PHASES],
    ] as const) {
      for (const value of values) {
        const valid = observabilityEvent("fleet.bootstrap_cleanup.deferred", { ...identity, cleanup_reason: "proof_unavailable", [key]: value });
        expect(() => assertSafeObservabilityEvent(valid)).not.toThrow();
        for (const invalid of ["PRIVATE_CLEANUP_CANARY", value + "\n", "prefix" + value, value + "suffix", null, 1]) {
          expect(() => assertSafeObservabilityEvent({ ...valid, [key]: invalid })).toThrow();
        }
        for (const event of ["fleet.bootstrap_cleanup.started", "fleet.bootstrap_cleanup.succeeded", "fleet.mutation.blocked"] as const) {
          expect(() => assertSafeObservabilityEvent(observabilityEvent(event, { ...identity, [key]: value }))).toThrow();
        }
      }
    }
  });

  it("accepts only exact held-bootstrap cleanup identities and deferred reasons", () => {
    const identity = { project_id: "00000000-0000-4000-8000-000000000032", worker_index: 31, leader_generation: 7 };
    for (const event of ["fleet.bootstrap_cleanup.started", "fleet.bootstrap_cleanup.succeeded"] as const) {
      expect(() => assertSafeObservabilityEvent(fleetLifecycleObservabilityEvent(event, identity))).not.toThrow();
      expect(() => assertSafeObservabilityEvent(fleetLifecycleObservabilityEvent(event, { ...identity, cleanup_reason: "timeout" }))).toThrow();
    }
    for (const cleanup_reason of FLEET_BOOTSTRAP_CLEANUP_REASONS) {
      expect(() => assertSafeObservabilityEvent(fleetLifecycleObservabilityEvent("fleet.bootstrap_cleanup.deferred", {
        ...identity, cleanup_reason,
      }))).not.toThrow();
    }
    const valid: Record<string, JsonValue> = { ...identity, cleanup_reason: "proof_unavailable" };
    const invalidFields: Array<Record<string, JsonValue>> = [
      { cleanup_reason: "PRIVATE_CLEANUP_CANARY" }, { cleanup_reason: "timeout\n" },
      { cleanup_reason: "TIMEOUT" }, { cleanup_reason: null }, { cleanup_reason: 1 },
      { reason: "timeout" }, { error_message: "PRIVATE_CLEANUP_CANARY" },
      { project_id: identity.project_id + "\n" }, { project_id: "prefix" + identity.project_id },
      { project_id: identity.project_id + "suffix" }, { worker_index: -1 }, { worker_index: "private" },
      { leader_generation: 1.5 }, { leader_generation: Number.MAX_SAFE_INTEGER + 1 },
    ];
    for (const fields of invalidFields) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.bootstrap_cleanup.deferred", { ...valid, ...fields }))).toThrow();
    }
    for (const key of ["project_id", "worker_index", "leader_generation", "cleanup_reason"]) {
      const missing = { ...valid };
      delete missing[key];
      expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.bootstrap_cleanup.deferred", missing))).toThrow();
    }
  });
  it("keeps effective timeout and route-progress diagnostics numeric and content-free", () => {
    const startup = observabilityEvent("server.started", {
      router_queue_timeout_ms: 60_000, initial_route_attempt_timeout_ms: 30_000,
      committed_handle_timeout_ms: 60_000, prepared_proof_uses_attempt_budget: true,
    });
    expect(() => assertSafeObservabilityEvent(startup)).not.toThrow();
    for (const key of ["router_queue_timeout_ms", "initial_route_attempt_timeout_ms", "committed_handle_timeout_ms"]) {
      for (const value of [-1, 0, Number.NaN, Infinity, "private timeout"]) {
        expect(() => assertSafeObservabilityEvent({ ...startup, [key]: value })).toThrow();
      }
    }
    const expiry = observabilityEvent("route.attempt_expired", {
      progress_observer_enabled: true, fallback_peer_available: false, remaining_admission_ms: 123.5,
    });
    expect(() => assertSafeObservabilityEvent(expiry)).not.toThrow();
    expect(() => assertSafeObservabilityEvent({ ...expiry, progress_observer_enabled: "private state" })).toThrow();
    expect(() => assertSafeObservabilityEvent({ ...expiry, fallback_peer_available: 1 })).toThrow();
    expect(() => assertSafeObservabilityEvent({ ...expiry, remaining_admission_ms: -1 })).toThrow();
  });

  it("records only bounded reasoning effort labels without reading reasoning content", () => {
    for (const effort of ["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]) {
      expect(safeObservabilityReasoningEffort({ effort, summary: "private reasoning" })).toBe(effort);
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.accepted", {
        reasoning_effort: effort,
      }))).not.toThrow();
    }
    expect(safeObservabilityReasoningEffort(undefined)).toBe("unspecified");
    expect(safeObservabilityReasoningEffort({})).toBe("unspecified");
    for (const reasoning of [null, [], "private reasoning", { effort: "private reasoning" }, { effort: 4 }]) {
      expect(safeObservabilityReasoningEffort(reasoning)).toBe("unknown");
    }
    let reads = 0;
    const accessor = Object.defineProperty({}, "effort", { get() { reads++; throw new Error("private"); } });
    expect(safeObservabilityReasoningEffort(accessor)).toBe("unknown");
    expect(reads).toBe(0);
    for (const effort of [null, 1, "private reasoning", "high\n", "HIGH"]) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.accepted", {
        reasoning_effort: effort,
      }))).toThrow(TypeError);
    }
  });

  it("accepts only numeric timing and structural counts with boolean lineage presence", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent("request.accepted", {
      input_items: 3, has_previous_response_id: true,
    }))).not.toThrow();
    const invalidFields: Array<Record<string, string | number>> = [
      { input_items: "private input" }, { input_items: -1 },
      { has_previous_response_id: "private provider id" },
    ];
    for (const fields of invalidFields) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.accepted", fields)))
        .toThrow(TypeError);
    }
    for (const value of ["private output", -1, Number.NaN, Infinity]) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.completed", {
        first_text_ms: value,
      }))).toThrow(TypeError);
    }
    expect(() => assertSafeObservabilityEvent(observabilityEvent("request.completed", {
      first_text_ms: 2, first_tool_ms: null, upstream_total_ms: 5,
    }))).not.toThrow();
  });

  it("redacts credential-shaped values and control characters", () => {
    const authorization = sanitizeDiagnosticMessage(
      "Authorization: Bearer sk-bootstrap-secret-123456789012345678901234\0",
    );
    const token = sanitizeDiagnosticMessage(
      "token=gAAAA-super-secret-token-value-12345678901234567890\0",
    );
    expect(authorization).toContain("Authorization: [REDACTED]");
    expect(token).toContain("token=[REDACTED]");
    expect(authorization).not.toContain("sk-bootstrap-secret");
    expect(token).not.toContain("gAAAA-super-secret");
    expect(authorization).not.toMatch(/[\n\r\0]/);
    expect(token).not.toMatch(/[\n\r\0]/);
    expect(() => assertSafeObservabilityEvent({
      schema: 1,
      event: "monitor.error",
      ts: "2026-08-22T00:00:00.000Z",
      mono_ms: 100,
      poll_ms: 1,
      attempt: 1,
      status: null,
      error_name: "transport",
      error_message: `${authorization} ${token}`,
      retry_ms: 500,
    })).not.toThrow();
  });

  it("redacts cookie, basic authorization, and URL-bearing diagnostics", () => {
    const cookie = sanitizeDiagnosticMessage("Cookie: session=opaque-value");
    const authorization = sanitizeDiagnosticMessage("Authorization: Basic abc123");
    const url = sanitizeDiagnosticMessage("failed at https://private.example/path?q=1");
    expect(cookie).not.toContain("opaque-value");
    expect(authorization).not.toContain("abc123");
    expect(url).not.toContain("private.example");
    expect(cookie).toContain("[REDACTED]");
    expect(authorization).toContain("[REDACTED]");
    expect(url).toContain("[URL]");

    expect(() => assertSafeObservabilityEvent(observabilityEvent("monitor.error", {
      error_message: "Set-Cookie: api=opaque",
    }))).toThrow("credential");
    expect(() => assertSafeObservabilityEvent(observabilityEvent("monitor.error", {
      error_message: "failed at https://private.example/path",
    }))).toThrow("credential");
  });

  it("accepts an allowlisted request event", () => {
    const event: ObservabilityEvent = {
      schema: 1,
      event: "request.accepted",
      ts: "2026-08-22T00:00:00.000Z",
      mono_ms: 100,
      trace_id: "trace-1",
      source: "websocket",
      model: "gpt-5.6-sol",
      service_tier: "fast",
      stream: true,
      workload_class: "text-small",
      request_bytes: 100,
      transfer_admission_cost: 1,
      reserved_bytes: 1024,
      capacity: 512,
      active: 16,
      available: 496,
      queued: 0,
    };
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
  });

  it("accepts bounded provider attribution on terminal failures", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent("request.failed", {
      trace_id: "trace-provider-error",
      project_id: "e631d12c-d917-4434-b360-41b4715d6679",
      status: 400,
      error_kind: "invalid_json",
      provider_origin: "sse",
      upstream_request_id_sha256: "a".repeat(64),
      committed: true,
    }))).not.toThrow();
  });

  it("rejects free-form request failure text and malformed correlation fields", () => {
    const invalidFailures: Array<Record<string, JsonValue>> = [
      { error_message: "ordinary reflected private text" }, { error_name: "PRIVATE_CANARY" },
      { error_kind: "PRIVATE_CANARY" }, { upstream_request_id: "req_PRIVATE_CANARY" },
      { upstream_request_id_sha256: "a".repeat(64) + "\n" }, { upstream_request_id_sha256: "PRIVATE_CANARY" },
      { provider_origin: "PRIVATE_CANARY" }, { status: 42.5 }, { committed: "PRIVATE_CANARY" },
      { error_kind: "cancelled", cancellation_origin: "PRIVATE_CANARY" },
      { error_kind: "stream", cancellation_origin: "websocket_remote_close" },
      { error_kind: "cancelled", websocket_close_code: 1001 },
      { error_kind: "cancelled", cancellation_origin: "websocket_remote_close", websocket_close_code: 1001.5 },
      { error_kind: "cancelled", cancellation_origin: "websocket_remote_close", websocket_close_code: 5000 },
    ];
    for (const fields of invalidFailures) {
      expect(() => assertSafeObservabilityEvent(observabilityEvent("request.failed", fields))).toThrow();
    }
  });

  it("validates bounded runtime-detail sample correlation and privacy", () => {
    const sample = observabilityEvent("fleet.runtime_sample", {
      sample_id: 1, runtime_index: 31, runtime_count: 32,
      runtime: { availability_reason: "available", diagnostics: { host: { oldest_outstanding_frame_kind: "ping" } } },
    });
    expect(() => assertSafeObservabilityEvent(sample)).not.toThrow();
    const invalidSamples: Array<Record<string, JsonValue>> = [
      { sample_id: 0 }, { sample_id: "private" }, { runtime_index: 32 }, { runtime_index: -1 },
      { runtime_count: 257 }, { runtime_count: 1.5 }, { runtime: null },
      { runtime: { diagnostics: { instance_id: "private" } } },
      { runtime: { diagnostics: { host: { oldest_outstanding_frame_kind: "private" } } } },
    ];
    for (const fields of invalidSamples) {
      expect(() => assertSafeObservabilityEvent({ ...sample, ...fields })).toThrow();
    }
  });

  it("rejects forbidden fields and credential values", () => {
    expect(() => assertSafeObservabilityEvent({
      schema: 1,
      event: "request.accepted",
      ts: "2026-08-22T00:00:00.000Z",
      mono_ms: 100,
      prompt: "secret prompt",
    } as any)).toThrow("prompt");

    expect(() => assertSafeObservabilityEvent({
      schema: 1,
      event: "request.failed",
      ts: "2026-08-22T00:00:00.000Z",
      mono_ms: 100,
      trace_id: "trace-1",
      error_message: "Bearer sk-secret-123456789012345678901234",
    } as any)).toThrow("credential");
  });

  it("never admits token, session, cookie, URL, or message fields on recovery events", () => {
    for (const key of ["token", "session", "cookie", "url", "message"] as const) {
      expect(() => assertSafeObservabilityEvent({
        schema: 1,
        event: "sandbox.recovery.started",
        ts: "2026-08-22T00:00:00.000Z",
        mono_ms: 100,
        project_id: "e631d12c-d917-4434-b360-41b4715d6679",
        [key]: `SUPER_SECRET_CANARY_${key}`,
      } as any)).toThrow();
    }
  });

  it("accepts only fixed content-free sandbox liveness transitions", () => {
    const event = sandboxLivenessObservabilityEvent({
      project_id: "e631d12c-d917-4434-b360-41b4715d6679",
      worker_index: 3,
      transport: "v2",
      liveness_signal: "sidecar_probe",
      liveness_outcome: "failed",
      attempt: 2,
      consecutive_failures: 2,
      status: null,
      error_name: "V2TransportLivenessError",
      error_kind: "timeout",
      duration_ms: 5_000,
      heartbeat_age_ms: 18_000,
      sidecar_peer_age_ms: 20_000,
      transport_connected: true,
      reliable_stall: false,
      awareness_lane: "control",
      connection_code: 1006,
      reconnect_phase: "supervisor_rotation",
      supervisor_phase: "challenge",
    });
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();

    for (const fields of [
      { liveness_signal: "private-network-name" },
      { liveness_outcome: "dump state" },
      { error_name: "SUPER_SECRET_CANARY" },
      { error_kind: "private failure text" },
      { awareness_lane: "secret-lane" },
      { reconnect_phase: "private credential contents" },
      { supervisor_phase: "private command contents" },
      { connection_code: 65_536 },
      { worker_index: undefined },
      { duration_ms: Number.POSITIVE_INFINITY },
      { sidecar_peer_age_ms: -1 },
      { transport_connected: "yes" },
      { token: "SUPER_SECRET_CANARY" },
    ]) {
      expect(() => assertSafeObservabilityEvent({
        ...event,
        ...fields,
      } as any)).toThrow();
    }
  });

  it("accepts credential-free soak telemetry rollups without provider identifiers", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      traffic: {
        accepted: 10,
        requests_per_second: 2,
        total: { count: 10, p50_ms: 100, p95_ms: 500, max_ms: 900 },
      },
      runtimes: [{
        project_id: "e631d12c-d917-4434-b360-41b4715d6679",
        rate_limit_observations: [{
          model: "gpt-5.6-sol",
          limit: 40_000_000,
          used: 40_000_000,
          requested: 609_930,
          remaining: 0,
          retry_after_ms: 914,
        }],
      }],
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      runtimes: [{ organization: "org-Example123" }],
    }))).toThrow("credential-shaped");
  });

  it("accepts only bounded router fairness telemetry", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent("request.queued", {
      trace_id: "trace-1",
      queued: 2,
      queued_bytes: 65_536,
      client_queue_depth: 1,
      queue_class: "bulk",
      active: 4,
      available: 8,
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("route.selected", {
      trace_id: "trace-1",
      project_id: "e631d12c-d917-4434-b360-41b4715d6679",
      sandbox_admission_credits: 4,
      sandbox_commit_backlog: 1,
      sandbox_scheduler_bytes: 8_192,
      sandbox_scheduler_oldest_ms: 20.5,
      sandbox_throughput_bytes_per_sec: 1_000_000.5,
      queue_class: "latency",
      fair_cost_bytes: 4_096,
      transfer_admission_cost: 3,
      route_attempt: 1,
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("route.selected", {
      queue_class: "private-client-class",
    }))).toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("request.queued", {
      queued_bytes: -1,
    }))).toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("route.selected", {
      sandbox_throughput_bytes_per_sec: Number.POSITIVE_INFINITY,
    }))).toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("route.selected", {
      route_attempt: 0,
    }))).toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("route.selected", {
      transfer_admission_cost: 1.5,
    }))).toThrow();
  });

  it("accepts bounded host-only transport evidence and exact frame categories", () => {
    const fields = {
      retained_precommit_cancellations: 1,
      last_published_host_credit_ceiling: 128,
      oldest_outstanding_frame_kind: "request_cancel",
      outstanding_flight_age_ms: 25.5,
      data_peer_age_ms: 2_000.25,
      control_peer_age_ms: 30_000,
      last_data_revision: 987,
      last_control_revision: 12,
    };
    const hostEvent = (host: Record<string, string | number | boolean | null>) =>
      observabilityEvent("fleet.sample", { runtimes: [{ diagnostics: { host } }] });
    expect(() => assertSafeObservabilityEvent(hostEvent(fields))).not.toThrow();
    const detailEvent = (host: Record<string, JsonValue>) => observabilityEvent("fleet.runtime_sample", {
      sample_id: 1, runtime_index: 0, runtime_count: 32, runtime: { diagnostics: { host } },
    });
    expect(() => assertSafeObservabilityEvent(detailEvent(fields))).not.toThrow();
    for (const key of ["data_peer_age_ms", "control_peer_age_ms", "inbound_accepted_progress_age_ms",
      "inbound_processed_progress_age_ms", "sidecar_diagnostics_age_ms", "sidecar_pending_age_ms"]) {
      for (const age of [null, 0, 0.25, Number.MAX_SAFE_INTEGER]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: age }))).not.toThrow();
      }
      for (const age of [-1, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "PRIVATE_LANE_AGE", false, [], {}]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: age }))).toThrow(TypeError);
      }
    }
    for (const key of ["last_data_revision", "last_control_revision", "inbound_accepted_epoch", "inbound_processed_epoch",
      "inbound_processing_frames", "sidecar_pending_data_publications"]) {
      for (const revision of [null, 0, 987, Number.MAX_SAFE_INTEGER]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: revision }))).not.toThrow();
      }
      for (const revision of [-1, 0.25, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "PRIVATE_LANE_REVISION", false, [], {}]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: revision }))).toThrow(TypeError);
      }
    }
    for (const key of ["inbound_accepted_sequence", "inbound_processed_sequence"]) {
      for (const sequence of [null, -1, 0, 987, Number.MAX_SAFE_INTEGER]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: sequence }))).not.toThrow();
      }
      for (const sequence of [-2, 0.25, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, "PRIVATE_SEQUENCE", false, [], {}]) {
        expect(() => assertSafeObservabilityEvent(detailEvent({ ...fields, [key]: sequence }))).toThrow(TypeError);
      }
    }
    for (const key of ["retained_precommit_cancellations", "last_published_host_credit_ceiling"] as const) {
      for (const value of [0, 128]) {
        expect(() => assertSafeObservabilityEvent(hostEvent({ ...fields, [key]: value })))
          .not.toThrow();
      }
      for (const value of [-1, 0.5, 129, Number.NaN, Infinity, "private count"]) {
        expect(() => assertSafeObservabilityEvent(hostEvent({ ...fields, [key]: value })))
          .toThrow(TypeError);
      }
    }
    for (const value of [
      null, "request_open", "request_data", "request_commit", "request_start", "request_cancel",
      "response_ack", "credential_rotate", "drain", "ping",
    ]) {
      expect(() => assertSafeObservabilityEvent(hostEvent({
        ...fields, oldest_outstanding_frame_kind: value,
      }))).not.toThrow();
    }
    for (const value of ["response_data", "request_cancel\n", "REQUEST_CANCEL", "private frame", 1, false]) {
      expect(() => assertSafeObservabilityEvent(hostEvent({
        ...fields, oldest_outstanding_frame_kind: value,
      }))).toThrow(TypeError);
    }
    expect(() => assertSafeObservabilityEvent(hostEvent({
      retained_precommit_cancellations: 0,
      last_published_host_credit_ceiling: null,
      oldest_outstanding_frame_kind: null,
      outstanding_flight_age_ms: null,
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(hostEvent({ ...fields, outstanding_flight_age_ms: 0 })))
      .not.toThrow();
    for (const value of [-1, Number.NaN, Infinity, "private age"]) {
      expect(() => assertSafeObservabilityEvent(hostEvent({ ...fields, outstanding_flight_age_ms: value })))
        .toThrow(TypeError);
    }
    for (const key of ["session_id", "instance_id", "request_body", "authorization"]) {
      expect(() => assertSafeObservabilityEvent(hostEvent({ ...fields, [key]: "PRIVATE_HOST_CANARY" })))
        .toThrow(TypeError);
    }
  });

  it("accepts only exact release hashes and rejects lifecycle binding fields", () => {
    const buildHash = V2_R3_RELEASE.buildHash;
    const schemaHash = V2_R3_RELEASE.schemaHash;
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      release: {
        knownRuntimes: 1,
        unknownRuntimes: 0,
        selections: [{ releaseRevision: "r3", protocolRevision: "r2", buildHash, schemaHash, runtimes: 1 }],
      },
      runtimes: [{
        release_revision: "r3",
        protocol_revision: "r2",
        build_hash: buildHash,
        schema_hash: schemaHash,
      }],
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      release: {
        knownRuntimes: 1,
        unknownRuntimes: 0,
        selections: [{ releaseRevision: "r3", protocolRevision: "r2", buildHash: "short", schemaHash, runtimes: 1 }],
      },
    }))).toThrow("release");
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      release: {
        knownRuntimes: 1,
        unknownRuntimes: 0,
        selections: [{ releaseRevision: "r3", protocolRevision: "r1", buildHash, schemaHash, runtimes: 1 }],
      },
    }))).toThrow("release");
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      runtimes: [{ diagnostics: { instance_id: "forbidden" } }],
    }))).toThrow("forbidden");
  });

  it("accepts bounded unavailable project IDs without free-form fleet errors", () => {
    const unavailable = "e631d12c-d917-4434-b360-41b4715d6679";
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      desired: 2,
      ready: 1,
      unavailable: 1,
      unavailable_projects: [unavailable],
      unavailable_projects_truncated: false,
      pending_retirements: 1,
      project_mutation_state: "open",
      project_mutation_failures: 2,
      project_mutation_retry_ms: 1000,
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      desired: 1,
      ready: 1,
      unavailable: 1,
      unavailable_projects: ["not-a-project"],
    }))).toThrow("project ID");
    expect(() => assertSafeObservabilityEvent(observabilityEvent("fleet.sample", {
      desired: 2,
      ready: 1,
      unavailable: 1,
      unavailable_error: "project metadata secret",
    } as any))).toThrow("not allowed");
  });
});
