import { describe, expect, it } from "bun:test";
import {
  assertSafeObservabilityEvent,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import { observabilityEvent, type ObservabilityEvent } from "../src/observability/events";
import { V2_R3_RELEASE } from "../src/v2/sidecar/assets";

describe("observability privacy", () => {
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
      upstream_request_id: "req_invalid_json_1",
      committed: true,
    }))).not.toThrow();
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
