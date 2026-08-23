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
        memory: () => ({ rss: 500, heapUsed: 200, heapTotal: 300, external: 25 }),
        systemMemory: () => ({ total: 4_000, free: 1_500 }),
        normalizedLoad1: () => 0.25,
        processCpuPercent: () => null,
      },
    });
    sampler.recordEventLoopDelay(2);
    sampler.recordEventLoopDelay(9);
    expect(sampler.snapshot()).toEqual({
      processRssBytes: 500, heapUsedBytes: 200, heapTotalBytes: 300, externalBytes: 25,
      systemTotalBytes: 4_000, systemFreeBytes: 1_500,
      processCpuPercent: null, normalizedLoad1: 0.25,
      eventLoopP50Ms: 2, eventLoopP95Ms: 9, eventLoopMaxMs: 9,
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
});
