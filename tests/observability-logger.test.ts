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
  fleetLifecycleObservabilityEvent,
  FLEET_BOOTSTRAP_CLEANUP_REASONS,
  type JsonValue,
} from "../src/observability/events";
import type { ObservabilityConfig } from "../src/observability/config";
import {
  assertSafeObservabilityEvent,
  safeObservabilityModel,
} from "../src/observability/redaction";
import { emitSandboxLivenessDiagnostic } from "../src/server/system";
import { projectId } from "../src/sandbox/types";
import type { V2RuntimeLivenessDiagnostic } from "../src/v2/runtime";

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
  it("persists data and control recovery through the production diagnostic mapper and both sinks", async () => {
    const sink = new MemorySink();
    const writes: string[] = [];
    const logger = new ObservabilityLogger({ config: config({ level: "error" }), sinks: [sink,
      new ConsoleSink({ now: () => 0, write: batch => writes.push(batch) }),
    ] });
    const id = projectId("00000000-0000-4000-8000-000000000032");
    const diagnostics: V2RuntimeLivenessDiagnostic[] = [
      { signal: "awareness", outcome: "failed", attempt: 7, awarenessLane: "control", errorName: "V2TransportLivenessError", errorKind: "timeout", durationMs: 30_000 },
      { signal: "awareness", outcome: "started", attempt: 7, awarenessLane: "control" },
      { signal: "awareness", outcome: "succeeded", attempt: 7, awarenessLane: "control", reconnectPhase: "host_reconnect", durationMs: 125.5 },
      { signal: "awareness", outcome: "superseded", attempt: 7, awarenessLane: "control", reconnectPhase: "host_reconnect", durationMs: 130 },
      { signal: "awareness", outcome: "failed", attempt: 8, awarenessLane: "control", reconnectPhase: "host_reconnect", errorName: "V2TransportLivenessError", errorKind: "unavailable", durationMs: 12_000 },
      { signal: "awareness", outcome: "failed", attempt: 9, awarenessLane: "data", errorName: "V2TransportLivenessError", errorKind: "reliable_stall", durationMs: 2_000 },
      { signal: "awareness", outcome: "started", attempt: 9, awarenessLane: "data" },
      { signal: "awareness", outcome: "succeeded", attempt: 9, awarenessLane: "data", reconnectPhase: "host_reconnect", durationMs: 150.5 },
    ];
    for (const diagnostic of diagnostics) emitSandboxLivenessDiagnostic(logger, id, 31, diagnostic);
    await logger.close();
    const events = sink.events();
    expect(events).toHaveLength(diagnostics.length);
    expect(events.map(event => event.liveness_outcome)).toEqual(diagnostics.map(diagnostic => diagnostic.outcome));
    for (let index = 0; index < diagnostics.length; index++) {
      expect(events[index]).toMatchObject({ event: "sandbox.liveness", project_id: id, worker_index: 31, transport: "v2", liveness_signal: "awareness", awareness_lane: diagnostics[index]!.awarenessLane, attempt: diagnostics[index]!.attempt });
    }
    expect(events[0]).toMatchObject({ error_name: "V2TransportLivenessError", error_kind: "timeout", duration_ms: 30_000 });
    expect(events[1]).not.toHaveProperty("error_kind");
    expect(events[2]).toMatchObject({ reconnect_phase: "host_reconnect", duration_ms: 125.5 });
    expect(events[3]).toMatchObject({ reconnect_phase: "host_reconnect", duration_ms: 130 });
    expect(events[4]).toMatchObject({ error_kind: "unavailable", reconnect_phase: "host_reconnect", duration_ms: 12_000 });
    expect(events[5]).toMatchObject({ error_kind: "reliable_stall", awareness_lane: "data", duration_ms: 2_000 });
    expect(events[6]).not.toHaveProperty("error_kind");
    expect(events[7]).toMatchObject({ reconnect_phase: "host_reconnect", awareness_lane: "data", duration_ms: 150.5 });
    expect(writes.join("\n").split("\n").map(line => JSON.parse(line))).toEqual(events);
    expect(logger.snapshot()).toMatchObject({ sinkErrors: 0, droppedTerminals: 0, queuedEvents: 0, queuedBytes: 0 });
  });
  it("retains every held-bootstrap cleanup transition and reason at error level without console sampling", async () => {
    const sink = new MemorySink();
    const writes: string[] = [];
    const logger = new ObservabilityLogger({ config: config({ level: "error" }), sinks: [sink,
      new ConsoleSink({ now: () => 0, write: batch => writes.push(batch) }),
    ] });
    const identity = { project_id: "00000000-0000-4000-8000-000000000032", worker_index: 31, leader_generation: 7 };
    const transitions = [
      fleetLifecycleObservabilityEvent("fleet.bootstrap_cleanup.started", identity),
      ...FLEET_BOOTSTRAP_CLEANUP_REASONS.map(cleanup_reason =>
        fleetLifecycleObservabilityEvent("fleet.bootstrap_cleanup.deferred", { ...identity, cleanup_reason })),
      fleetLifecycleObservabilityEvent("fleet.bootstrap_cleanup.succeeded", identity),
    ];
    for (const event of transitions) logger.emit(event, "terminal");
    await logger.close();
    expect(sink.events()).toEqual(transitions);
    expect(writes.join("\n").split("\n").map(line => JSON.parse(line))).toEqual(transitions);
    expect(logger.snapshot()).toMatchObject({ queuedEvents: 0, queuedBytes: 0, droppedTerminals: 0, sinkErrors: 0 });
  });

  it("never writes raw held-bootstrap cleanup reasons or extra error details to a sink", async () => {
    const sink = new MemorySink();
    const logger = new ObservabilityLogger({ config: config({ level: "error" }), sinks: [sink] });
    const identity = { project_id: "00000000-0000-4000-8000-000000000032", worker_index: 31, leader_generation: 7 };
    const invalidEvents: Array<Record<string, JsonValue>> = [
      { cleanup_reason: "PRIVATE_CLEANUP_CANARY" },
      { cleanup_reason: "timeout", message: "PRIVATE_CLEANUP_CANARY" },
      { cleanup_reason: "timeout", error_message: "PRIVATE_CLEANUP_CANARY" },
    ];
    for (const fields of invalidEvents) logger.emit(observabilityEvent("fleet.bootstrap_cleanup.deferred", { ...identity, ...fields }), "terminal");
    await logger.close();
    expect(sink.lines).toHaveLength(0);
    expect(logger.snapshot().sinkErrors).toBe(3);
  });
  it("retains degraded startup readiness in JSONL and console", async () => {
    const memory = new MemorySink();
    const consoleLines: string[] = [];
    const logger = new ObservabilityLogger({ config: config(), sinks: [memory,
      new ConsoleSink({ write: batch => consoleLines.push(batch) }),
    ] });
    logger.emit(observabilityEvent("server.started", {
      desired: 8, ready: 5, routable: 4, held_project_count: 3, fleet_readiness: "degraded",
    }));
    await logger.close();
    expect(memory.events()).toMatchObject([{ event: "server.started", desired: 8, ready: 5, routable: 4, held_project_count: 3, fleet_readiness: "degraded" }]);
    expect(consoleLines).toHaveLength(1);
    expect(JSON.parse(consoleLines[0]!)).toMatchObject(memory.events()[0]);
  });
  it("keeps the console sample slot for the aggregate after runtime detail pages", async () => {
    const writes: string[] = [];
    const sink = new ConsoleSink({ now: () => 0, write: batch => writes.push(batch) });
    await sink.writeBatch([
      JSON.stringify(observabilityEvent("fleet.runtime_sample", { sample_id: 1, runtime_index: 0, runtime_count: 1, runtime: {} })),
      JSON.stringify(observabilityEvent("fleet.sample", { sample_id: 1, capacity: 128, runtimes: [] })),
    ]);
    expect(writes).toHaveLength(1);
    expect(JSON.parse(writes[0]!)).toMatchObject({ event: "fleet.sample", capacity: 128 });
  });
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

  it("shows routable capacity and bounded blocker counts when ready workers cannot admit work", () => {
    const reasons = ["sidecar_suspect", "sidecar_suspect", "sidecar_suspect", "awareness_reconnect",
      "transport_congested", "transport_congested", "zero_remote_credits", "lifecycle_unavailable"];
    const compact = formatConsoleObservabilityLine(JSON.stringify(observabilityEvent("fleet.sample", {
      desired: 8, ready: 7, routable: 0, capacity: 4, active: 2, available: 0,
      runtimes: [...reasons, "private-unrecognized-reason"].map((reason, index) => ({
        state: index < 7 ? "ready" : "recovering", availability_reason: reason,
      })),
    })));
    expect(JSON.parse(compact)).toMatchObject({
      ready: 7, routable: 0, capacity: 4, available: 0,
      availability_reasons: { sidecar_suspect: 3, awareness_reconnect: 1,
        transport_congested: 2, zero_remote_credits: 1, lifecycle_unavailable: 1 },
    });
    expect(compact).not.toContain("private-unrecognized-reason");
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
