import { describe, expect, it } from "bun:test";
import {
  assertSafeObservabilityEvent,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import type { ObservabilityEvent } from "../src/observability/events";

describe("observability privacy", () => {
  it("redacts credential-shaped values and control characters", () => {
    const value = sanitizeDiagnosticMessage(
      "Authorization: Bearer sk-bootstrap-secret-123456789012345678901234\n" +
      "token=gAAAA-super-secret-token-value-12345678901234567890\0",
    );
    expect(value).toContain("Authorization: Bearer [REDACTED]");
    expect(value).toContain("token=[REDACTED]");
    expect(value).not.toContain("sk-bootstrap-secret");
    expect(value).not.toContain("gAAAA-super-secret");
    expect(value).not.toMatch(/[\n\r\0]/);
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
});
