import { describe, expect, it } from "bun:test";
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
    expect(report.wire.uniqueRequestDataBytes).toBe(
      V2_PERFORMANCE_PAYLOAD_BYTES.mib16,
    );
    expect(report.transport.retransmissions).toBe(0);
  }, 180_000);
});
