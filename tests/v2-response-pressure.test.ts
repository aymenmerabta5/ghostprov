import { expect, it } from "bun:test";
import { runV2PerformanceHarness } from "./support/v2-performance-harness";

it.each([600, 1_200, 2_500])("delivers large R10 streams at %ims RTT while later requests are admitted", async rttMs => {
  const report = await runV2PerformanceHarness({
    sidecarRevision: "r10", protocolRevision: "r2", transportProfile: "bulk-v1",
    rttMs, requestBytes: 256 * 1024, concurrency: 6,
    corpus: "incompressible", responseEventCount: 256, responseDeltaBytes: 1_024,
    responseIntervalMs: 15, streamWhileAdmitting: true,
    linkBandwidthBytesPerSecond: 400 * 1024,
    serializationBytesPerSecond: 100 * 1024 * 1024,
    timeoutMs: 30_000,
  });
  expect(report.requests).toHaveLength(6);
  expect(report.requests.every(request => request.exactRequest && request.exactTerminal)).toBe(true);
  expect(report.requests.every(request => request.eventCount === 256)).toBe(true);
  expect(report.requests.every(request => request.modeledCommitMs < 60_000)).toBe(true);
  expect(report.upstream.calls).toBe(6);
  expect(report.upstream.maxActive).toBeGreaterThan(1);
  expect(report.responseEventsEmitted).toBe(6 * 256);
  expect(report.admissionReopenedDuringResponses).toBeGreaterThan(0);
  expect(report.transport.peaks.sidecarWindowFrames).toBeLessThanOrEqual(32);
  expect(report.transport.peaks.sidecarWindowBytes).toBeLessThanOrEqual(128 * 1024);
  expect(report.transport.peaks.sidecarSchedulerBytes).toBeLessThanOrEqual(1024 * 1024);
  expect(report.router.active).toBe(0);
  expect(report.router.queued).toBe(0);
}, 75_000);

it("keeps large R10 response delivery exact through loss and reordered acknowledgements", async () => {
  const report = await runV2PerformanceHarness({
    sidecarRevision: "r10", protocolRevision: "r2", transportProfile: "bulk-v1",
    rttMs: 1_200, requestBytes: 128 * 1024, concurrency: 4,
    corpus: "incompressible", responseEventCount: 128, responseDeltaBytes: 1_024,
    responseIntervalMs: 15, streamWhileAdmitting: true,
    linkBandwidthBytesPerSecond: 400 * 1024,
    faults: [
      { direction: "sidecar-to-host", lane: "data", ordinal: 3, actions: ["drop"] },
      { direction: "host-to-sidecar", lane: "control", ordinal: 12, actions: ["duplicate", "reorder"] },
    ],
    timeoutMs: 30_000,
  });
  expect(report.requests.every(request => request.exactRequest && request.exactTerminal)).toBe(true);
  expect(report.upstream.calls).toBe(4);
  expect(report.requests.every(request => request.modeledCommitMs < 60_000)).toBe(true);
  expect(report.responseEventsEmitted).toBe(4 * 128);
  expect(report.wire.droppedUpdates).toBeGreaterThan(0);
  expect(report.wire.duplicateDeliveries).toBeGreaterThan(0);
  expect(report.wire.reorderedUpdates).toBeGreaterThan(0);
  expect(report.transport.peaks.sidecarWindowBytes).toBeLessThanOrEqual(128 * 1024);
  expect(report.transport.peaks.sidecarSchedulerBytes).toBeLessThanOrEqual(1024 * 1024);
  expect(report.router.active).toBe(0);
  expect(report.router.queued).toBe(0);
}, 75_000);
