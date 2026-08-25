import { describe, expect, it } from "bun:test";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";

describe("SidecarResourceSampler", () => {
  it("samples bounded optional resources and event-loop delay", async () => {
    let now = 1_000;
    const sampler = new SidecarResourceSampler({
      enabled: true,
      intervalMs: 1_000,
      now: () => now,
      probe: {
        memory: () => ({
          rss: 500,
          heapUsed: 200,
          heapTotal: 300,
          external: 25,
          arrayBuffers: 7,
        }),
        systemMemory: () => ({ total: 4_000, free: 1_500 }),
        normalizedLoad1: () => 0.25,
        processCpuPercent: () => null,
      },
    });
    sampler.recordEventLoopDelay(2);
    sampler.recordEventLoopDelay(9);
    sampler.recordGcDuration(3);
    sampler.recordGcDuration(12);
    expect(sampler.snapshot()).toEqual({
      processRssBytes: 500, heapUsedBytes: 200, heapTotalBytes: 300, externalBytes: 25,
      arrayBufferBytes: 7,
      systemTotalBytes: 4_000, systemFreeBytes: 1_500,
      processCpuPercent: null, normalizedLoad1: 0.25,
      eventLoopSamples: 2,
      eventLoopP50Ms: 2, eventLoopP95Ms: 9, eventLoopMaxMs: 9,
      gcSamples: 2, gcDurationP95Ms: 12, gcDurationMaxMs: 12,
    });
    // Control publications may read the snapshot repeatedly without clearing
    // the rolling event-loop/GC evidence.
    expect(sampler.snapshot()).toMatchObject({
      eventLoopSamples: 2,
      eventLoopP95Ms: 9,
      gcSamples: 2,
      gcDurationP95Ms: 12,
    });
    now += 1_000;
    await sampler.close();
    expect(sampler.estimatedBytes).toBeLessThan(1024 * 1024);
  });

  it("returns zero/null metrics while disabled", async () => {
    const sampler = new SidecarResourceSampler({ enabled: false, intervalMs: 1_000 });
    sampler.start();
    expect(sampler.snapshot()).toMatchObject({ processRssBytes: 0, systemFreeBytes: null });
    await sampler.close();
  });

  it("serves cached O(1) snapshots after the interval sampler starts", async () => {
    let memoryReads = 0;
    let cpuReads = 0;
    const sampler = new SidecarResourceSampler({
      enabled: true,
      intervalMs: 60_000,
      probe: {
        memory: () => {
          memoryReads++;
          return { rss: 1, heapUsed: 1, heapTotal: 1, external: 1 };
        },
        systemMemory: () => null,
        processCpuPercent: () => { cpuReads++; return 12.5; },
        normalizedLoad1: () => null,
      },
    });
    sampler.start();
    expect(sampler.snapshot().processCpuPercent).toBe(12.5);
    expect(sampler.snapshot().processCpuPercent).toBe(12.5);
    expect(memoryReads).toBe(1);
    expect(cpuReads).toBe(1);
    await sampler.close();
  });
});
