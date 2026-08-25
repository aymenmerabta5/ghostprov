import { describe, expect, it } from "bun:test";
import {
  assertSafeObservabilityEvent,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import { observabilityEvent, type ObservabilityEvent } from "../src/observability/events";

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
      reserved_bytes: 1024,
      capacity: 512,
      active: 16,
      available: 496,
      queued: 0,
    };
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
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

  it("accepts credential-free soak telemetry rollups", () => {
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
          organization: "org-Example123",
          limit: 40_000_000,
          used: 40_000_000,
          requested: 609_930,
          remaining: 0,
          retry_after_ms: 914,
        }],
      }],
    }))).not.toThrow();
  });
});
