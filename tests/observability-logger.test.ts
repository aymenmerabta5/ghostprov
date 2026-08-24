import { describe, expect, it } from "bun:test";
import { ObservabilityLogger, type ObservabilitySink } from "../src/observability/logger";
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
});
