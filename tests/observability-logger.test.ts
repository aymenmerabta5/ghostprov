import { describe, expect, it } from "bun:test";
import {
  ConsoleSink,
  formatConsoleObservabilityLine,
  ObservabilityLogger,
  type ObservabilitySink,
} from "../src/observability/logger";
import { observabilityEvent, recoveryObservabilityEvent } from "../src/observability/events";
import type { ObservabilityConfig } from "../src/observability/config";

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

describe("ObservabilityLogger", () => {
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
        cpu_queued: 0,
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

  it("keeps terminal recovery and circuit transitions at error log level", async () => {
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
    await logger.close();

    expect(sink.events().map(event => event.event)).toEqual([
      "sandbox.recovery.started",
      "prism.circuit.changed",
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
    await logger.close();

    expect(writes.length).toBeLessThanOrEqual(10);
    const output = writes.join("\n");
    expect(output.match(/"event":"request.completed"/g)).toHaveLength(1_000);
    expect(output.match(/"event":"route.selected"/g)?.length ?? 0).toBeLessThanOrEqual(1);

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
