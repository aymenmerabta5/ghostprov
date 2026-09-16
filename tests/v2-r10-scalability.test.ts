import { expect, it } from "bun:test";
import { runV2FleetPerformanceHarness } from "./support/v2-performance-harness";

const MiB = 1024 ** 2;
const scenarios = [
  {
    name: "fourteen mixed-context agents with two unavailable workers",
    unavailableRuntimeCount: 2,
    requestBytesByRequest: Array.from({ length: 14 }, (_, index) =>
      [128 * 1024, MiB, Math.floor(2.9 * MiB)][index % 3]!),
  },
  {
    name: "forty concurrent agents while substantial responses are delivered",
    unavailableRuntimeCount: 0,
    requestBytesByRequest: Array.from({ length: 40 }, () => 128 * 1024),
  },
] as const;

for (const scenario of scenarios) {
  it(`keeps R10 exact and bounded for ${scenario.name}`, async () => {
    const report = await runV2FleetPerformanceHarness({
      sidecarRevision: "r10", protocolRevision: "r2", transportProfile: "bulk-v1",
      runtimeCount: 8, unavailableRuntimeCount: scenario.unavailableRuntimeCount,
      requestBytes: 128 * 1024,
      requestBytesByRequest: scenario.requestBytesByRequest,
      rttMs: 600, linkBandwidthBytesPerSecond: 400 * 1024,
      serializationBytesPerSecond: 100 * MiB,
      corpus: "incompressible",
      responseEventCount: 256, responseDeltaBytes: 1024, responseIntervalMs: 15,
      streamWhileAdmitting: true, timeoutMs: 60_000,
    });
    expect(report.usableRuntimeCount).toBe(8 - scenario.unavailableRuntimeCount);
    expect(report.requests).toHaveLength(scenario.requestBytesByRequest.length);
    expect(report.exactRequests).toBe(true);
    expect(report.exactTerminals).toBe(true);
    expect(report.requests.map(request => request.requestBytes))
      .toEqual(scenario.requestBytesByRequest);
    expect(report.reports.reduce((sum, runtime) => sum + runtime.upstream.calls, 0))
      .toBe(scenario.requestBytesByRequest.length);
    expect(report.modeledCommitMs.max).toBeLessThan(60_000);
    for (const runtime of report.reports) {
      expect(runtime.requests.every(request => request.eventCount === 256)).toBe(true);
      expect(runtime.transport.peaks.sidecarWindowFrames).toBeLessThanOrEqual(32);
      expect(runtime.transport.peaks.sidecarWindowBytes).toBeLessThanOrEqual(128 * 1024);
      expect(runtime.transport.peaks.sidecarSchedulerBytes).toBeLessThanOrEqual(MiB);
      expect(runtime.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(MiB);
      expect(runtime.router.active).toBe(0);
      expect(runtime.router.queued).toBe(0);
      expect(runtime.preparation.unresolvedSessionRuns).toBe(0);
    }
    console.info("R10_SCALABILITY", JSON.stringify({
      requests: report.requests.length, usableRuntimes: report.usableRuntimeCount,
      modeledCommitMs: report.modeledCommitMs, modeledTotalMs: report.modeledTotalMs,
      retransmissions: report.retransmissions,
    }));
  }, 120_000);
}

