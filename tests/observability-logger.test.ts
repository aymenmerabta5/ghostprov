import { describe, expect, it } from "bun:test";
import {
  ConsoleSink,
  formatConsoleObservabilityLine,
  ObservabilityLogger,
  type ObservabilitySink,
} from "../src/observability/logger";
import {
  observabilityEvent,
  recoveryObservabilityEvent,
  sandboxLivenessObservabilityEvent,
  type JsonValue,
} from "../src/observability/events";
import type { ObservabilityConfig } from "../src/observability/config";
import {
  assertSafeObservabilityEvent,
  safeObservabilityModel,
} from "../src/observability/redaction";

function config(overrides: Partial<ObservabilityConfig> = {}): ObservabilityConfig {
  return {
    level: "info",
    directory: "D:/unused",
    rotateBytes: 64 * 1024 * 1024,
    retainFiles: 5,
    queueMaxEvents: 10_000,
    queueMaxBytes: 8 * 1024 * 1024,
    flushMs: 60_000,
    metricsIntervalMs: 5_000,
    slowRequestMs: 10_000,
    slowFirstEventMs: 5_000,
    ...overrides,
  };
}

class MemorySink implements ObservabilitySink {
  readonly lines: string[] = [];
  async writeBatch(lines: readonly string[]): Promise<void> { this.lines.push(...lines); }
  async close(): Promise<void> {}
  events(): any[] { return this.lines.map(line => JSON.parse(line)); }
}

function modelFallbackEvent(fields: Record<string, JsonValue> = {}) {
  return observabilityEvent("request.model_fallback", {
    trace_id: "trace-terra",
    parent_trace_id: "trace-original",
    requested_model: safeObservabilityModel(" GPT-6-ASTRA "),
    fallback_model: "gpt-5.6-terra",
    error_kind: "model_not_found",
    provider_origin: "http",
    retry_count: 1,
    ...fields,
  });
}

describe("ObservabilityLogger", () => {
  it("records bounded model fallback attribution at error level without console sampling", async () => {
    const sink = new MemorySink();
    const writes: string[] = [];
    const consoleSink = new ConsoleSink({ now: () => 0, write: value => writes.push(value) });
    const logger = new ObservabilityLogger({
      config: config({ level: "error" }),
      sinks: [sink, consoleSink],
    });
    for (const provider_origin of ["http", "sse"] as const) {
      const event = modelFallbackEvent({ provider_origin });
      expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
      logger.emit(event, "progress");
    }
    await logger.close();

    expect(logger.snapshot().sinkErrors).toBe(0);
    expect(sink.events()).toHaveLength(2);
    for (const event of sink.events()) {
      expect(event).toMatchObject({
        event: "request.model_fallback",
        trace_id: "trace-terra",
        parent_trace_id: "trace-original",
        requested_model: "gpt-6-astra",
        fallback_model: "gpt-5.6-terra",
        error_kind: "model_not_found",
        retry_count: 1,
      });
    }
    expect(sink.events().map(event => event.provider_origin)).toEqual(["http", "sse"]);
    expect(writes.join("\n").match(/"event":"request.model_fallback"/g)).toHaveLength(2);
    expect(() => assertSafeObservabilityEvent(modelFallbackEvent({
      trace_id: "t".repeat(128),
      parent_trace_id: "p".repeat(128),
      requested_model: "m".repeat(128),
    }))).not.toThrow();
    expect(() => assertSafeObservabilityEvent(modelFallbackEvent({
      requested_model: safeObservabilityModel("private prompt\n"),
    }))).not.toThrow();
  });

  it("rejects malformed or sensitive model fallback fields before sink delivery", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config(), sinks: [sink] });
    const invalidFields: Array<Record<string, JsonValue>> = [
      { trace_id: "" }, { trace_id: "t".repeat(129) }, { trace_id: "trace\n" },
      { trace_id: "private prompt" }, { trace_id: null },
      { parent_trace_id: "p".repeat(129) }, { parent_trace_id: "trace\r\n" },
      { parent_trace_id: ["trace-original"] },
      { requested_model: "GPT-6-ASTRA" }, { requested_model: "gpt-6-astra\n" },
      { requested_model: "m".repeat(129) }, { requested_model: "private prompt" },
      { requested_model: "sk-private-canary-12345678901234567890" },
      { requested_model: 1 }, { fallback_model: "gpt-6-astra" },
      { fallback_model: "gpt-5.6-terra\n" }, { error_kind: "rate_limit" },
      { provider_origin: "local" }, { provider_origin: "HTTP" },
      { retry_count: 0 }, { retry_count: 2 }, { retry_count: "1" },
    ];
    for (const key of ["prompt", "request_body", "headers", "cookie", "token", "url", "client_id", "error_message"]) {
      invalidFields.push({ [key]: "PRIVATE_CANARY" });
    }
    for (const key of ["trace_id", "parent_trace_id", "requested_model"]) {
      invalidFields.push({ [key]: "Bearer private-credential-canary" });
      invalidFields.push({ [key]: "https://private.example/prompt" });
    }
    for (const fields of invalidFields) {
      const event = modelFallbackEvent(fields);
      expect(() => assertSafeObservabilityEvent(event)).toThrow(TypeError);
      logger.emit(event, "progress");
    }
    for (const key of ["trace_id", "parent_trace_id", "requested_model", "fallback_model", "error_kind", "provider_origin", "retry_count"]) {
      const event = { ...modelFallbackEvent() };
      delete event[key];
      expect(() => assertSafeObservabilityEvent(event)).toThrow(TypeError);
      logger.emit(event, "progress");
    }
    await logger.close();

    expect(sink.lines).toHaveLength(0);
    expect(logger.snapshot().sinkErrors).toBe(invalidFields.length + 7);
  });

  it("keeps WebSocket payload rejection and HTTP fallback visible at error level", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config({ level: "error" }), sinks: [sink] });
    logger.emit(observabilityEvent("websocket.payload_rejected", {
      limit_bytes: 16 * 1024 ** 2, http_fallback_available: true,
    }), "terminal");
    logger.emit(observabilityEvent("websocket.http_fallback", {
      limit_bytes: 16 * 1024 ** 2, status: 426,
    }), "terminal");
    await logger.close();
    expect(sink.events().map(event => event.event)).toEqual([
      "websocket.payload_rejected", "websocket.http_fallback",
    ]);
  });

  it("keeps console fleet samples fixed-size while preserving full JSONL elsewhere", () => {
    const full = `${JSON.stringify(observabilityEvent("fleet.sample", {
      capacity: 1_280,
      active: 20,
      available: 1_260,
      queued: 0,
      queued_bytes: 16_384,
      cpu_percent: 12,
      rss_bytes: 500,
      event_loop_p95_ms: 4,
      resources: {
        memory_paused: false,
        governor_queued: 0,
        transform_queued: 0,
      },
      logger: { queued_events: 1, queued_bytes: 100 },
      traffic: {
        requests_per_second: 20,
        success_rate: 1,
        failed: 0,
        total: { p95_ms: 1_000 },
      },
      websocket: {
        activeSessions: 8,
        closedSessions: 5,
        closedActiveResponses: 4,
        closeCauses: {
          remote_close: 5,
          heartbeat_timeout: 0,
          delivery_error: 0,
          protocol_error: 0,
          server_shutdown: 0,
          idle_timeout: 0,
          ingress_capacity: 0,
          socket_error: 0,
        },
        pendingRequests: 2,
        ingressReservedBytes: 136 * 1024 * 1024,
        ingressReservedBytesPeak: 160 * 1024 * 1024,
        ingressCapacityBytes: 512 * 1024 * 1024,
        ingressGuaranteedSessions: 27,
        ingressPromotions: 1,
        ingressRejections: 0,
        ingressParserRejections: 1,
        ingressUpgradeRejections: 0,
        responseGraphRejections: 0,
      },
      runtimes: Array.from({ length: 100 }, (_, index) => ({
        project_id: `private-${index}`,
        state: index === 99 ? "recovering" : "ready",
        diagnostics: { padding: "x".repeat(500) },
      })),
    }))}\n`;
    const compact = formatConsoleObservabilityLine(full);
    expect(compact.length).toBeLessThan(1_000);
    expect(JSON.parse(compact)).toMatchObject({
      event: "fleet.sample",
      runtime_states: { ready: 99, recovering: 1 },
      queued_bytes: 16_384,
      pressure: { governor_queued: 0 },
      traffic: { requests_per_second: 20, total_p95_ms: 1_000 },
      websocket: {
        active_sessions: 8,
        closed_sessions: 5,
        closed_active_responses: 4,
        close_causes: { remote_close: 5 },
        ingress_promotions: 1,
        ingress_guaranteed_sessions: 27,
        ingress_parser_rejections: 1,
      },
    });
    expect(compact).not.toContain("private-99");
    expect(formatConsoleObservabilityLine('{"event":"request.completed"}\n'))
      .toBe('{"event":"request.completed"}');
  });

  it("preserves terminal events and drops samples first under a bounded queue", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({
      config: config({ queueMaxEvents: 2 }),
      sinks: [sink],
    });
    logger.emit(observabilityEvent("fleet.sample", { capacity: 512 }), "sample");
    logger.emit(observabilityEvent("request.completed", { trace_id: "a" }), "terminal");
    logger.emit(observabilityEvent("request.failed", { trace_id: "b" }), "terminal");

    await logger.close();

    expect(sink.events().map(event => event.event)).toEqual([
      "request.completed",
      "request.failed",
      "logger.dropped",
    ]);
    expect(sink.events().at(-1)).toMatchObject({ samples: 1, progress: 0 });
  });

  it("serializes concurrent sink writes and reports queue state", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config(), sinks: [sink] });
    for (let index = 0; index < 100; index++) {
      logger.emit(observabilityEvent("request.completed", { trace_id: `t-${index}` }), "terminal");
    }
    expect(logger.snapshot().queuedEvents).toBe(100);
    expect(logger.snapshot().queuedBytes).toBeGreaterThan(0);

    await logger.close();

    expect(sink.events().filter(event => event.event === "request.completed")).toHaveLength(100);
    expect(logger.snapshot().queuedEvents).toBe(0);
  });

  it("never rejects the caller when a sink fails", async () => {
    const sink: ObservabilitySink = {
      async writeBatch() { throw new Error("disk offline"); },
      async close() {},
    };
    const logger = new ObservabilityLogger({ config: config(), sinks: [sink] });
    expect(() => logger.emit(observabilityEvent("request.failed", { trace_id: "t" }), "terminal"))
      .not.toThrow();
    await expect(logger.close()).resolves.toBeUndefined();
    expect(logger.snapshot().sinkErrors).toBeGreaterThan(0);
  });

  it("keeps actionable recovery and circuit transitions at error log level", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config({ level: "error" }), sinks: [sink] });
    logger.emit(recoveryObservabilityEvent("sandbox.recovery.started", {
      project_id: "00000000-0000-4000-8000-000000000001",
      cause: "heartbeat",
      attempt: 1,
    }), "terminal");
    logger.emit(recoveryObservabilityEvent("prism.circuit.changed", {
      cause: "circuit",
      circuit_state: "open",
      circuit_scope: "outage",
    }), "terminal");
    logger.emit(recoveryObservabilityEvent("sandbox.recovery.retry_scheduled", {
      cause: "runtime",
      retry_delay_ms: 1_000,
    }), "progress");
    logger.emit(sandboxLivenessObservabilityEvent({
      project_id: "00000000-0000-4000-8000-000000000001",
      worker_index: 1,
      transport: "v2",
      liveness_signal: "heartbeat",
      liveness_outcome: "failed",
      error_name: "Error",
      error_kind: "timeout",
      consecutive_failures: 1,
    }), "terminal");
    await logger.close();

    expect(sink.events().map(event => event.event)).toEqual([
      "sandbox.recovery.started",
      "prism.circuit.changed",
      "sandbox.liveness",
      "sandbox.recovery.retry_scheduled",
    ]);
  });

  it("keeps hot-path enqueue bounded", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({
      config: config({ queueMaxEvents: 20_000, queueMaxBytes: 32 * 1024 * 1024 }),
      sinks: [sink],
    });
    const samples: number[] = [];
    for (let index = 0; index < 10_000; index++) {
      const started = performance.now();
      logger.emit(observabilityEvent("request.completed", { trace_id: `p-${index}` }), "terminal");
      samples.push(performance.now() - started);
    }
    samples.sort((left, right) => left - right);
    expect(samples[Math.ceil(samples.length * 0.95) - 1]).toBeLessThan(0.25);
    await logger.close();
  });

  it("defers deep validation and serialization while never leaking an invalid event", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config(), sinks: [sink] });
    const invalid = Object.freeze({
      schema: 1,
      event: "request.failed",
      ts: "2026-08-25T00:00:00.000Z",
      mono_ms: 1,
      trace_id: "trace",
      get error_message() { throw new Error("payload getter must stay off emit"); },
    }) as any;

    expect(() => logger.emit(invalid, "terminal")).not.toThrow();
    expect(logger.snapshot().sinkErrors).toBe(0);
    await logger.close();
    expect(logger.snapshot().sinkErrors).toBe(1);
    expect(sink.lines).toHaveLength(0);
  });

  it("preserves terminals by evicting samples then progress in O(1) order", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({
      config: config({ queueMaxEvents: 3 }),
      sinks: [sink],
    });
    for (let index = 0; index < 3; index++) {
      logger.emit(observabilityEvent("request.queued", {
        trace_id: `progress-${index}`,
      }), "progress");
    }
    logger.emit(observabilityEvent("request.failed", { trace_id: "terminal" }), "terminal");
    await logger.close();

    expect(sink.events().map(event => event.event)).toEqual([
      "request.failed",
      "request.queued",
      "request.queued",
      "logger.dropped",
    ]);
    expect(sink.events().at(-1)).toMatchObject({ progress: 1, terminals: 0 });
  });

  it("drains 10k and 20k queues with near-linear scaling", async () => {
    const run = async (count: number) => {
      const sink = new MemorySink();
      const logger = new ObservabilityLogger({
        config: config({
          queueMaxEvents: count + 1,
          queueMaxBytes: 64 * 1024 * 1024,
        }),
        sinks: [sink],
      });
      for (let index = 0; index < count; index++) {
        logger.emit(observabilityEvent("request.completed", { trace_id: `scale-${index}` }), "terminal");
      }
      const started = performance.now();
      await logger.close(30_000);
      return { elapsed: performance.now() - started, lines: sink.lines.length };
    };
    const ten = await run(10_000);
    const twenty = await run(20_000);
    expect(ten.lines).toBe(10_000);
    expect(twenty.lines).toBe(20_000);
    expect(twenty.elapsed).toBeLessThan(ten.elapsed * 3.5 + 50);
  }, 30_000);

  it("bounds console writes while retaining every terminal record", async () => {
    let now = 0;
    const writes: string[] = [];
    const consoleSink = new ConsoleSink({
      now: () => now,
      progressIntervalMs: 1_000,
      write: value => writes.push(value),
    });
    const logger = new ObservabilityLogger({
      config: config({ queueMaxEvents: 2_000, queueMaxBytes: 8 * 1024 * 1024 }),
      sinks: [consoleSink],
    });
    for (let index = 0; index < 1_000; index++) {
      logger.emit(observabilityEvent("request.completed", { trace_id: `terminal-${index}` }), "terminal");
    }
    for (let index = 0; index < 500; index++) {
      logger.emit(observabilityEvent("route.selected", { trace_id: `progress-${index}` }), "progress");
    }
    logger.emit(observabilityEvent("fleet.replacement.qualified", {
      project_id: "e631d12c-d917-4434-b360-41b4715d6679",
      worker_index: 3,
      leader_generation: 7,
      failure_kind: "readiness",
      failure_count: 3,
    }), "progress");
    await logger.close();

    expect(writes.length).toBeLessThanOrEqual(10);
    const output = writes.join("\n");
    expect(output.match(/"event":"request.completed"/g)).toHaveLength(1_000);
    expect(output.match(/"event":"route.selected"/g)?.length ?? 0).toBeLessThanOrEqual(1);
    expect(output.match(/"event":"fleet.replacement.qualified"/g)).toHaveLength(1);

    const beforeRateLimited = writes.length;
    await consoleSink.writeBatch([
      `${JSON.stringify(observabilityEvent("route.selected", { trace_id: "later" }))}\n`,
    ]);
    expect(writes).toHaveLength(beforeRateLimited);
    now = 1_000;
    await consoleSink.writeBatch([
      `${JSON.stringify(observabilityEvent("route.selected", { trace_id: "later" }))}\n`,
    ]);
    expect(writes.at(-1)).toContain("later");
  });
});
