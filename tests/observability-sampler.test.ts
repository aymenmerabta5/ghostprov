import { describe, expect, it } from "bun:test";
import type { Clock, MachineProbe } from "../src/core/ports";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { FleetSampler } from "../src/observability/sampler";
import { projectId } from "../src/sandbox/types";

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  emit(event: ObservabilityEvent): void { this.events.push(event); }
  snapshot() { return { queuedEvents: 2, queuedBytes: 200, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }; }
  async close(): Promise<void> {}
}

class FakeClock implements Clock {
  value = 1_000;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> { this.value = deadlineMs; }
}

describe("FleetSampler", () => {
  it("emits sanitized aggregate, host, logger, and per-runtime metrics", () => {
    const logger = new MemoryLogger();
    const clock = new FakeClock();
    const machineProbe: MachineProbe = {
      sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 12 * 1024 ** 3,
        rssBytes: 512 * 1024 ** 2,
        heapBytes: 128 * 1024 ** 2,
        externalBytes: 64 * 1024 ** 2,
        logicalProcessors: 12,
      }),
    };
    const eventLoop = {
      min: 1_000_000,
      max: 8_000_000,
      percentile: (value: number) => value === 50 ? 2_000_000 : 5_000_000,
      enable() {},
      disable() {},
      reset() {},
    };
    let cpu = { user: 0, system: 0 };
    const sampler = new FleetSampler({
      logger,
      clock,
      machineProbe,
      intervalMs: 5_000,
      routerSnapshot: () => ({
        capacity: 512,
        active: 16,
        available: 496,
        queued: 0,
        resources: {
          totalBytes: 32 * 1024 ** 3,
          startupFreeBytes: 20 * 1024 ** 3,
          startupRssBytes: 100,
          systemReserveBytes: 4 * 1024 ** 3,
          processCeilingBytes: 16 * 1024 ** 3,
          reservationCapacityBytes: 12 * 1024 ** 3,
          pauseFreeBytes: 2 * 1024 ** 3,
          resumeFreeBytes: 3 * 1024 ** 3,
          cpuTransformWorkers: 6,
          paused: false,
          reservedBytes: 256 * 1024 ** 2,
          queued: 0,
          cpuActive: 1,
          cpuQueued: 0,
        },
        runtimes: [{
          projectId: projectId("00000000-0000-4000-8000-000000000001"),
          state: "ready",
          capacity: 128,
          active: 4,
          available: 124,
          quarantined: 0,
          heartbeatAgeMs: 500,
          credentialExpiresInMs: 3_000_000,
          ackEwmaMs: 15,
          completionEwmaMs: 2_000,
          recentFailureRate: 0,
          queueDepth: 0,
          memoryPressure: 0.1,
          failure: null,
        }],
      }),
      cpuPoolSnapshot: () => ({ workers: 6, active: 1, queued: 0, maxActive: 2, closed: false }),
      eventLoop,
      memoryUsage: () => ({ rss: 500, heapTotal: 400, heapUsed: 300, external: 200, arrayBuffers: 100 }),
      cpuUsage: () => (cpu = { user: cpu.user + 250_000, system: cpu.system + 50_000 }),
    });

    sampler.sample();
    clock.value += 5_000;
    sampler.sample();

    const event = logger.events.at(-1)!;
    expect(event).toMatchObject({
      event: "fleet.sample",
      capacity: 512,
      active: 16,
      event_loop_p50_ms: 2,
      event_loop_p95_ms: 5,
      event_loop_max_ms: 8,
    });
    expect((event.resources as any).transform_workers).toBe(6);
    expect((event.runtimes as any[])[0]).toMatchObject({
      project_id: "00000000-0000-4000-8000-000000000001",
      awareness_rtt_ms: 15,
      completion_ewma_ms: 2_000,
    });
    expect(JSON.stringify(event)).not.toMatch(/token|cookie|prompt|output|url/i);
  });
});
