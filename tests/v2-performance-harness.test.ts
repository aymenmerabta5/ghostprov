import { describe, expect, it } from "bun:test";
import {
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  canonicalJson,
  utf8Bytes,
} from "../src/v2/protocol/shared";
import {
  V2_PERFORMANCE_CONCURRENCY,
  V2_PERFORMANCE_PAYLOAD_BYTES,
  V2_PERFORMANCE_RTTS_MS,
  createExactSizedV2Request,
  runV2PerformanceHarness,
  type V2PerformanceHarnessReport,
} from "./support/v2-performance-harness";

function expectExactAndBounded(report: V2PerformanceHarnessReport): void {
  expect(report.requests).toHaveLength(report.concurrency);
  expect(report.upstream.calls).toBe(report.concurrency);
  expect(report.upstream.maxActive).toBe(report.concurrency);
  expect(report.requests.every(value => value.exactRequest)).toBe(true);
  expect(report.requests.every(value => value.exactTerminal)).toBe(true);
  expect(report.requests.every(value => value.eventCount === 2)).toBe(true);
  expect(report.requests.every(value => value.ackRoundsAtCommit > 0)).toBe(true);
  expect(report.requests.every(value => value.firstEventMs >= value.commitMs)).toBe(true);
  expect(report.wire.usefulRequestBytes).toBe(report.requestBytes * report.concurrency);
  expect(report.wire.uniqueRequestDataBytes).toBe(report.wire.usefulRequestBytes);
  expect(report.wire.attemptedBytes).toBeGreaterThan(report.wire.usefulRequestBytes);
  expect(report.transport.peaks.hostWindowFrames).toBeLessThanOrEqual(
    GP_V2_WINDOW_MAX_FRAMES,
  );
  expect(report.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(
    GP_V2_WINDOW_MAX_BYTES,
  );
  expect(report.transport.peaks.sidecarWindowFrames).toBeLessThanOrEqual(
    GP_V2_WINDOW_MAX_FRAMES,
  );
  expect(report.transport.peaks.sidecarWindowBytes).toBeLessThanOrEqual(
    GP_V2_WINDOW_MAX_BYTES,
  );
  expect(report.transport.sidecar).not.toBeNull();
  expect(report.transport.awarenessRttMs).toBeGreaterThan(0);
  expect(report.router).toMatchObject({ active: 0, queued: 0 });
}

describe("V2 deterministic performance harness", () => {
  it("constructs exact canonical request sizes without approximate payload accounting", () => {
    for (const bytes of [
      V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      V2_PERFORMANCE_PAYLOAD_BYTES.kib512,
    ]) {
      const request = createExactSizedV2Request(bytes, 7);
      expect(utf8Bytes(canonicalJson(request)).byteLength).toBe(bytes);
    }
  });

  it("measures 50/300/1000 ms RTT with monotonic ACK-clocked latency", async () => {
    const reports: V2PerformanceHarnessReport[] = [];
    for (const rttMs of V2_PERFORMANCE_RTTS_MS) {
      const report = await runV2PerformanceHarness({
        rttMs,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency: 1,
      });
      expectExactAndBounded(report);
      reports.push(report);
    }

    const rounds = reports.map(value => value.requests[0]!.ackRoundsAtCommit);
    expect(new Set(rounds).size).toBe(1);
    expect(reports[1]!.timings.commitMs.p50).toBeGreaterThan(
      reports[0]!.timings.commitMs.p50,
    );
    expect(reports[2]!.timings.commitMs.p50).toBeGreaterThan(
      reports[1]!.timings.commitMs.p50,
    );
    expect(reports[2]!.timings.firstEventMs.p50).toBeGreaterThan(
      reports[1]!.timings.firstEventMs.p50,
    );
  }, 30_000);

  it("preserves 128 KiB, 512 KiB, and 3.35 MiB requests and terminal proofs", async () => {
    const reports: V2PerformanceHarnessReport[] = [];
    for (const requestBytes of [
      V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      V2_PERFORMANCE_PAYLOAD_BYTES.kib512,
      V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
    ]) {
      const report = await runV2PerformanceHarness({
        rttMs: 50,
        requestBytes,
        concurrency: 1,
      });
      expectExactAndBounded(report);
      reports.push(report);
    }

    const rounds = reports.map(value => value.requests[0]!.ackRoundsAtCommit);
    expect(rounds[1]).toBeGreaterThan(rounds[0]!);
    expect(rounds[2]).toBeGreaterThan(rounds[1]!);
    expect(reports[2]!.transport.peaks.hostSchedulerBytes).toBeGreaterThan(
      reports[0]!.transport.peaks.hostSchedulerBytes,
    );
  }, 30_000);

  it("survives loss, duplication, update reordering, and a transient disconnect exactly once", async () => {
    const report = await runV2PerformanceHarness({
      rttMs: 300,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      concurrency: 1,
      transientDisconnects: [{
        direction: "host-to-sidecar",
        lane: "data",
        fromOrdinal: 1,
        publicationCount: 2,
      }],
      faults: [
        {
          direction: "host-to-sidecar",
          lane: "data",
          ordinal: 3,
          actions: ["drop"],
        },
        {
          direction: "host-to-sidecar",
          lane: "data",
          ordinal: 4,
          actions: ["reorder", "duplicate"],
        },
      ],
    });

    expectExactAndBounded(report);
    expect(report.upstream.calls).toBe(1);
    expect(report.transport.retransmissions).toBeGreaterThanOrEqual(3);
    expect(report.wire.disconnectedDrops).toBe(2);
    expect(report.wire.droppedUpdates).toBe(1);
    expect(report.wire.duplicateDeliveries).toBe(1);
    expect(report.wire.reorderedUpdates).toBe(1);
  }, 30_000);

  it("dispatches 1/4/16/32/40 Sol requests without a provider concurrency gate", async () => {
    for (const concurrency of V2_PERFORMANCE_CONCURRENCY) {
      const report = await runV2PerformanceHarness({
        rttMs: 50,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency,
      });
      expectExactAndBounded(report);
      // The upstream gate is held until every request arrives. Reaching this
      // exact maximum proves no model-wide 16-request gate delayed dispatch.
      expect(report.upstream.maxActive).toBe(concurrency);
    }
  }, 60_000);
});
