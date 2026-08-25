import { describe, expect, it } from "bun:test";
import { GP_V2_R2_TRANSPORT_LIMITS } from "../src/v2/protocol/shared";
import {
  V2_PERFORMANCE_PAYLOAD_BYTES,
  runV2PerformanceHarness,
} from "./support/v2-performance-harness";

const largeDescribe = process.env.GP_V2_PERF_LARGE === "1"
  ? describe
  : describe.skip;

largeDescribe("V2 opt-in large transport performance", () => {
  it("preserves one 16 MiB R2 request with bounded windows", async () => {
    const report = await runV2PerformanceHarness({
      rttMs: 300,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib16,
      concurrency: 1,
      maxAggregateRequestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib16,
      timeoutMs: 120_000,
    });

    expect(report.requests).toHaveLength(1);
    expect(report.requests[0]).toMatchObject({
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib16,
      exactRequest: true,
      exactTerminal: true,
    });
    expect(report.upstream).toEqual({ calls: 1, maxActive: 1 });
    expect(report.wire.usefulRequestBytes).toBe(
      V2_PERFORMANCE_PAYLOAD_BYTES.mib16,
    );
    expect(report.wire.uniqueRequestDataBytes).toBe(
      report.wire.postCompressionRequestBytes,
    );
    expect(report.transport.retransmissions).toBe(0);
  }, 180_000);
});

const veryLargeDescribe = process.env.GP_V2_PERF_64M === "1"
  ? describe
  : describe.skip;

veryLargeDescribe("V2 opt-in maximum request integrity", () => {
  it("preserves one exact 64 MiB R2 request with bounded windows", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      corpus: "incompressible",
      rttMs: 300,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib64,
      concurrency: 1,
      maxAggregateRequestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib64,
      timeoutMs: 300_000,
    });

    expect(report.implementation).toBe("mutable-source-components");
    expect(report.requests).toHaveLength(1);
    expect(report.requests[0]).toMatchObject({
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib64,
      exactRequest: true,
      exactTerminal: true,
    });
    expect(report.requests[0]!.encodedRequestBytes).toBeGreaterThan(0);
    expect(report.requests[0]!.encodedRequestBytes).toBeLessThanOrEqual(
      V2_PERFORMANCE_PAYLOAD_BYTES.mib64,
    );
    expect(report.upstream).toEqual({ calls: 1, maxActive: 1 });
    expect(report.wire.usefulRequestBytes).toBe(
      V2_PERFORMANCE_PAYLOAD_BYTES.mib64,
    );
    expect(report.wire.uniqueRequestDataBytes).toBe(
      report.wire.postCompressionRequestBytes,
    );
    expect(report.transport.peaks.hostWindowFrames).toBeLessThanOrEqual(
      GP_V2_R2_TRANSPORT_LIMITS.windowMaxFrames,
    );
    expect(report.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(
      GP_V2_R2_TRANSPORT_LIMITS.windowMaxBytes,
    );
    expect(report.transport.retransmissions).toBe(0);
    expect(report.timings.measuredWall.hostPreparationMs.p50).toBeGreaterThan(0);
    expect(report.preparation.gzipWallMs).toBeGreaterThan(0);
  }, 360_000);
});
