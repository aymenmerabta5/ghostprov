import { describe, expect, it } from "bun:test";
import {
  canonicalJson,
  protocolTransportLimits,
  utf8Bytes,
} from "../src/v2/protocol/shared";
import {
  loadV2SidecarAssets,
  V2_R1_RELEASE,
} from "../src/v2/sidecar/assets";
import {
  V2_PERFORMANCE_CONCURRENCY,
  V2_PERFORMANCE_PAYLOAD_BYTES,
  V2_PERFORMANCE_RTTS_MS,
  createExactSizedV2Request,
  runV2PerformanceHarness,
  type V2PerformanceHarnessReport,
} from "./support/v2-performance-harness";

function expectExactAndBounded(
  report: V2PerformanceHarnessReport,
  expectedEventCount = 2,
): void {
  const limits = protocolTransportLimits(
    report.protocolRevision,
    report.transportProfile,
  );
  expect(report.requests).toHaveLength(report.concurrency);
  expect(report.upstream.calls).toBe(report.concurrency);
  expect(report.upstream.maxActive).toBe(report.concurrency);
  expect(report.requests.every(value => value.exactRequest)).toBe(true);
  expect(report.requests.every(value => value.exactTerminal)).toBe(true);
  expect(report.requests.every(value => value.eventCount === expectedEventCount)).toBe(true);
  expect(report.requests.every(value => value.ackRoundsAtCommit > 0)).toBe(true);
  expect(report.requests.every(value =>
    value.modeledFirstEventMs >= value.modeledCommitMs)).toBe(true);
  expect(report.requests.every(value =>
    value.wallFirstEventMs >= value.wallCommitMs)).toBe(true);
  expect(report.requests.every(value =>
    value.wallCommitMs >= value.hostPreparationWallMs)).toBe(true);
  expect(report.preparation.gzipCalls).toBeGreaterThanOrEqual(report.concurrency);
  expect(report.preparation.gzipCalls).toBeLessThanOrEqual(report.concurrency * 2);
  expect(report.preparation.gzipWallMs).toBeGreaterThanOrEqual(0);
  expect(report.wire.usefulRequestBytes).toBe(
    report.requests.reduce((total, request) => total + request.requestBytes, 0),
  );
  expect(report.wire.uniqueRequestDataBytes).toBe(report.wire.postCompressionRequestBytes);
  expect(report.wire.postCompressionRequestBytes).toBeGreaterThan(0);
  expect(report.wire.attemptedBytes).toBeGreaterThan(report.wire.postCompressionRequestBytes);
  expect(report.transport.peaks.hostWindowFrames).toBeLessThanOrEqual(
    limits.windowMaxFrames,
  );
  expect(report.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(
    limits.windowMaxBytes,
  );
  expect(report.transport.peaks.sidecarWindowFrames).toBeLessThanOrEqual(
    limits.windowMaxFrames,
  );
  expect(report.transport.peaks.sidecarWindowBytes).toBeLessThanOrEqual(
    limits.windowMaxBytes,
  );
  expect(report.transport.sidecar).not.toBeNull();
  if (report.transport.retransmissions === 0) {
    expect(report.transport.awarenessRttMs).toBeGreaterThan(0);
  } else if (report.transport.awarenessRttMs !== null) {
    expect(report.transport.awarenessRttMs).toBeGreaterThan(0);
  }
  expect(report.router).toMatchObject({ active: 0, queued: 0 });
}

function p95(values: readonly number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)]!;
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

  it("rejects a heterogeneous corpus that does not exactly match concurrency", async () => {
    await expect(runV2PerformanceHarness({
      rttMs: 400,
      requestBytes: 128 * 1024,
      requestBytesByRequest: [128 * 1024],
      concurrency: 2,
    })).rejects.toThrow("requestBytesByRequest must match concurrency exactly");
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
    expect(reports[1]!.timings.modeledLink.commitMs.p50).toBeGreaterThan(
      reports[0]!.timings.modeledLink.commitMs.p50,
    );
    expect(reports[2]!.timings.modeledLink.commitMs.p50).toBeGreaterThan(
      reports[1]!.timings.modeledLink.commitMs.p50,
    );
    expect(reports[2]!.timings.modeledLink.firstEventMs.p50).toBeGreaterThan(
      reports[1]!.timings.modeledLink.firstEventMs.p50,
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

  it("compares the source-level R1 wire-limit baseline with R2 without claiming to run the frozen bundle", async () => {
    const [smallR1, smallR2, largeR1, largeR2] = await Promise.all([
      runV2PerformanceHarness({
        protocolRevision: "r1",
        rttMs: 300,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency: 1,
        corpus: "incompressible",
      }),
      runV2PerformanceHarness({
        protocolRevision: "r2",
        rttMs: 300,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency: 1,
        corpus: "incompressible",
      }),
      runV2PerformanceHarness({
        protocolRevision: "r1",
        rttMs: 300,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
        concurrency: 1,
        corpus: "incompressible",
      }),
      runV2PerformanceHarness({
        protocolRevision: "r2",
        rttMs: 300,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
        concurrency: 1,
        corpus: "incompressible",
      }),
    ]);
    for (const report of [smallR1, smallR2, largeR1, largeR2]) {
      expect(report.implementation).toBe("mutable-source-components");
      expectExactAndBounded(report);
      expect(report.transport.retransmissions).toBe(0);
    }

    // Deterministic <=10% plus 100 ms regression guard for every public stage.
    expect(smallR2.timings.modeledLink.commitMs.p50)
      .toBeLessThanOrEqual(smallR1.timings.modeledLink.commitMs.p50 * 1.1 + 100);
    expect(smallR2.timings.modeledLink.firstEventMs.p50)
      .toBeLessThanOrEqual(smallR1.timings.modeledLink.firstEventMs.p50 * 1.1 + 100);
    expect(smallR2.timings.modeledLink.totalMs.p50)
      .toBeLessThanOrEqual(smallR1.timings.modeledLink.totalMs.p50 * 1.1 + 100);

    expect(largeR1.requests[0]!.ackRoundsAtCommit)
      .toBeGreaterThan(largeR2.requests[0]!.ackRoundsAtCommit * 2);
    expect(largeR1.transport.peaks.hostWindowFrames).toBeLessThan(32);
    expect(largeR2.transport.peaks.hostWindowFrames).toBe(32);
    expect(largeR2.transport.peaks.hostWindowBytes).toBeGreaterThan(128 * 1024);
    expect(largeR2.timings.modeledLink.commitMs.p50)
      .toBeLessThan(largeR1.timings.modeledLink.commitMs.p50);
    expect(largeR2.timings.modeledLink.firstEventMs.p50)
      .toBeLessThan(largeR1.timings.modeledLink.firstEventMs.p50);
  }, 30_000);

  it("keeps the actual frozen R1 artifact identity as a separate proof", async () => {
    const frozen = await loadV2SidecarAssets({ revision: "r1" });
    expect(frozen.revision).toBe("r1");
    expect(frozen.buildHash).toBe(V2_R1_RELEASE.buildHash);
    expect(frozen.schemaHash).toBe(V2_R1_RELEASE.schemaHash);
    expect(frozen.files[0]!.sha256).toBe(V2_R1_RELEASE.bundleSha256);
  });

  it("reports production-gzip sizes and byte-sensitive link delay for three deterministic corpora", async () => {
    const reports = [];
    for (const corpus of ["compressible", "mixed", "incompressible"] as const) {
      const report = await runV2PerformanceHarness({
        corpus,
        rttMs: 300,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
        concurrency: 1,
      });
      expectExactAndBounded(report);
      expect(report.requests[0]!.encodedRequestBytes).toBe(
        report.wire.postCompressionRequestBytes,
      );
      reports.push(report);
    }
    const [compressible, mixed, incompressible] = reports;
    expect(compressible!.requests[0]!.requestEncoding).toBe("gzip+base64");
    expect(mixed!.requests[0]!.requestEncoding).toBe("gzip+base64");
    expect(incompressible!.requests[0]!.requestEncoding).toBe("identity");
    expect(incompressible!.wire.postCompressionRequestBytes).toBe(
      incompressible!.wire.usefulRequestBytes,
    );
    // Modeled link time and measured host CPU/wall time are deliberately
    // separate. Never use the virtual latency as a CPU benchmark.
    expect(incompressible!.timings.measuredWall.hostPreparationMs.p50)
      .toBeGreaterThan(0);
    expect(incompressible!.preparation.gzipCalls).toBe(1);
    expect(incompressible!.preparation.gzipWallMs).toBeGreaterThan(0);
    expect(compressible!.wire.postCompressionRequestBytes)
      .toBeLessThan(mixed!.wire.postCompressionRequestBytes);
    expect(mixed!.wire.postCompressionRequestBytes)
      .toBeLessThan(incompressible!.wire.postCompressionRequestBytes);
    expect(compressible!.requests[0]!.ackRoundsAtCommit)
      .toBeLessThanOrEqual(mixed!.requests[0]!.ackRoundsAtCommit);
    expect(mixed!.requests[0]!.ackRoundsAtCommit)
      .toBeLessThanOrEqual(incompressible!.requests[0]!.ackRoundsAtCommit);

    const slow = await runV2PerformanceHarness({
      corpus: "incompressible",
      rttMs: 50,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib512,
      concurrency: 1,
      linkBandwidthBytesPerSecond: 1 * 1024 * 1024,
      serializationBytesPerSecond: 10 * 1024 * 1024,
    });
    const fast = await runV2PerformanceHarness({
      corpus: "incompressible",
      rttMs: 50,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib512,
      concurrency: 1,
      linkBandwidthBytesPerSecond: 100 * 1024 * 1024,
      serializationBytesPerSecond: 1_000 * 1024 * 1024,
    });
    expect(slow.wire.postCompressionRequestBytes).toBe(
      fast.wire.postCompressionRequestBytes,
    );
    // Total wire bytes include timing-dependent control/credit chatter. The
    // immutable request flights themselves must remain byte-stable.
    expect(Math.abs(
      slow.wire.hostRequestDataAttemptedBytes -
      fast.wire.hostRequestDataAttemptedBytes,
    ))
      .toBeLessThanOrEqual(64);
    expect(slow.timings.modeledLink.commitMs.p50)
      .toBeGreaterThan(fast.timings.modeledLink.commitMs.p50);
  }, 30_000);

  it("bounds the observed four-way 2 MiB transfer workload without losing integrity", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      corpus: "incompressible",
      rttMs: 800,
      requestBytes: 2 * 1024 * 1024,
      concurrency: 4,
      linkBandwidthBytesPerSecond: 350 * 1024,
      serializationBytesPerSecond: 100 * 1024 * 1024,
      maxAggregateRequestBytes: 16 * 1024 * 1024,
      timeoutMs: 120_000,
    });

    expect(report.requests.every(request => request.exactRequest && request.exactTerminal)).toBe(true);
    expect(report.timings.modeledLink.commitMs.p50).toBeLessThan(30_000);
    expect(report.timings.modeledLink.commitMs.p95).toBeLessThan(60_000);
    expect(report.virtualElapsedMs).toBeLessThan(60_000);
    expect(report.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(512 * 1024);
    expect(report.wire.droppedUpdates).toBe(0);
  }, 30_000);

  it("cuts a real-world 13 MiB request below ten seconds without changing the small R2 flight", async () => {
    const standardSmall = await runV2PerformanceHarness({
      protocolRevision: "r2",
      corpus: "incompressible",
      rttMs: 400,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      concurrency: 1,
    });
    const acceleratedSmall = await runV2PerformanceHarness({
      protocolRevision: "r2",
      corpus: "incompressible",
      rttMs: 400,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      concurrency: 1,
      transportProfile: "bulk-v1",
    });
    const acceleratedLarge = await runV2PerformanceHarness({
      protocolRevision: "r2",
      corpus: "incompressible",
      rttMs: 400,
      requestBytes: 13 * 1024 * 1024,
      concurrency: 1,
      maxAggregateRequestBytes: 16 * 1024 * 1024,
      timeoutMs: 120_000,
      transportProfile: "bulk-v1",
    });

    expectExactAndBounded(standardSmall);
    expectExactAndBounded(acceleratedSmall);
    expectExactAndBounded(acceleratedLarge);
    expect(acceleratedSmall.requests[0]!.ackRoundsAtCommit)
      .toBe(standardSmall.requests[0]!.ackRoundsAtCommit);
    expect(acceleratedSmall.wire.uniqueRequestDataBytes)
      .toBe(standardSmall.wire.uniqueRequestDataBytes);
    expect(Math.abs(
      acceleratedSmall.timings.modeledLink.commitMs.p50 -
      standardSmall.timings.modeledLink.commitMs.p50,
    )).toBeLessThanOrEqual(0.001);
    expect(acceleratedLarge.timings.modeledLink.commitMs.p50).toBeLessThan(10_000);
    expect(acceleratedLarge.requests[0]!.ackRoundsAtCommit).toBe(19);
    expect(acceleratedLarge.timings.modeledLink.commitMs.p50).toBeLessThan(8_900);
    expect(acceleratedLarge.transport.peaks.hostWindowBytes).toBeGreaterThan(512 * 1024);
    expect(acceleratedLarge.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(1024 * 1024);
  }, 30_000);

  it("accelerates isolated 512 KiB through 2 MiB requests without changing the 256 KiB boundary", async () => {
    const compare = async (requestBytes: number) => {
      const common = {
        protocolRevision: "r2" as const,
        corpus: "incompressible" as const,
        rttMs: 400,
        requestBytes,
        concurrency: 1,
      };
      const standard = await runV2PerformanceHarness(common);
      const adaptive = await runV2PerformanceHarness({
        ...common,
        transportProfile: "bulk-v1" as const,
      });
      expectExactAndBounded(standard);
      expectExactAndBounded(adaptive);
      return { standard, adaptive };
    };

    const tiny = await compare(256 * 1024);
    expect(tiny.adaptive.requests[0]!.ackRoundsAtCommit)
      .toBe(tiny.standard.requests[0]!.ackRoundsAtCommit);
    expect(Math.abs(
      tiny.adaptive.timings.modeledLink.commitMs.p50 -
      tiny.standard.timings.modeledLink.commitMs.p50,
    )).toBeLessThanOrEqual(0.01);

    for (const [requestBytes, standardRounds, adaptiveRounds, minimumGainMs] of [
      [512 * 1024, 3, 2, 300],
      [1024 * 1024, 4, 3, 300],
      [2 * 1024 * 1024, 7, 4, 1_000],
    ] as const) {
      const result = await compare(requestBytes);
      expect(result.standard.requests[0]!.ackRoundsAtCommit).toBe(standardRounds);
      expect(result.adaptive.requests[0]!.ackRoundsAtCommit).toBe(adaptiveRounds);
      expect(
        result.standard.timings.modeledLink.commitMs.p50 -
        result.adaptive.timings.modeledLink.commitMs.p50,
      ).toBeGreaterThan(minimumGainMs);
    }
  }, 30_000);

  it("reduces 512 KiB p95 latency at 4/16/40 concurrency without losing upstream scale", async () => {
    for (const concurrency of [4, 16, 40]) {
      const common = {
        protocolRevision: "r2" as const,
        corpus: "incompressible" as const,
        rttMs: 400,
        requestBytes: 512 * 1024,
        concurrency,
        maxAggregateRequestBytes: 32 * 1024 * 1024,
        timeoutMs: 120_000,
      };
      const standard = await runV2PerformanceHarness(common);
      const adaptive = await runV2PerformanceHarness({
        ...common,
        transportProfile: "bulk-v1" as const,
      });
      expectExactAndBounded(standard);
      expectExactAndBounded(adaptive);
      expect(adaptive.upstream.maxActive).toBe(concurrency);
      expect(adaptive.transport.retransmissions).toBe(0);
      expect(adaptive.transport.peaks.hostWindowBytes).toBeGreaterThan(512 * 1024);
      expect(adaptive.timings.modeledLink.commitMs.p95)
        .toBeLessThan(standard.timings.modeledLink.commitMs.p95 * 0.9);
      expect(adaptive.virtualElapsedMs).toBeLessThan(standard.virtualElapsedMs * 0.95);
      expect(adaptive.wire.usefulRequestBytes).toBe(standard.wire.usefulRequestBytes);
    }
  }, 60_000);

  it("keeps sixteen exact 2 MiB agents below the production admission deadline", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      transportProfile: "bulk-v1",
      corpus: "incompressible",
      rttMs: 400,
      requestBytes: 2 * 1024 * 1024,
      concurrency: 16,
      maxAggregateRequestBytes: 32 * 1024 * 1024,
      timeoutMs: 60_000,
    });
    expectExactAndBounded(report);
    expect(report.upstream.maxActive).toBe(16);
    // The 64-frame/1-MiB wire ceiling gives four equal streams sixteen frames
    // per RTT. This bound requires >20% improvement without inventing a larger
    // R6 window or weakening the R2 start fence.
    expect(report.timings.modeledLink.commitMs.p95).toBeLessThan(24_000);
    expect(report.transport.retransmissions).toBe(0);
  }, 30_000);

  it("reproduces the observed 2.9 MiB admission cliff on a 600 ms, 400 KiB/s link", async () => {
    const requestBytes = Math.floor(2.9 * 1024 * 1024);
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      transportProfile: "bulk-v1",
      corpus: "incompressible",
      rttMs: 600,
      linkBandwidthBytesPerSecond: 400 * 1024,
      serializationBytesPerSecond: 100 * 1024 * 1024,
      requestBytes,
      concurrency: 3,
      maxAggregateRequestBytes: 16 * 1024 * 1024,
      timeoutMs: 60_000,
    });
    expectExactAndBounded(report);
    expect(report.upstream.maxActive).toBe(3);
    expect(report.transport.retransmissions).toBe(0);
    expect(report.timings.modeledLink.commitMs.p95).toBeGreaterThan(40_000);
  }, 30_000);

  it("keeps tiny p95 exact while accelerating a 24-way 128/512 KiB production-deadline mix", async () => {
    const requestBytesByRequest = Array.from(
      { length: 24 },
      (_, index) => index % 2 === 0 ? 128 * 1024 : 512 * 1024,
    );
    const common = {
      protocolRevision: "r2" as const,
      corpus: "incompressible" as const,
      rttMs: 400,
      requestBytes: 512 * 1024,
      requestBytesByRequest,
      concurrency: requestBytesByRequest.length,
      maxAggregateRequestBytes: 16 * 1024 * 1024,
      timeoutMs: 30_000,
    };
    const standard = await runV2PerformanceHarness(common);
    const adaptive = await runV2PerformanceHarness({
      ...common,
      transportProfile: "bulk-v1" as const,
    });
    expectExactAndBounded(standard);
    expectExactAndBounded(adaptive);
    const commits = (report: V2PerformanceHarnessReport, bytes: number) => report.requests
      .filter(request => request.requestBytes === bytes)
      .map(request => request.modeledCommitMs);
    const standardTiny = p95(commits(standard, 128 * 1024));
    const adaptiveTiny = p95(commits(adaptive, 128 * 1024));
    const standardMedium = p95(commits(standard, 512 * 1024));
    const adaptiveMedium = p95(commits(adaptive, 512 * 1024));

    expect(Math.abs(adaptiveTiny - standardTiny)).toBeLessThanOrEqual(0.01);
    expect(adaptiveMedium).toBeLessThan(standardMedium * 0.9);
    expect(adaptive.virtualElapsedMs).toBeLessThan(standard.virtualElapsedMs * 0.9);
    expect(adaptive.upstream.maxActive).toBe(24);
    expect(adaptive.transport.retransmissions).toBe(0);
    expect(adaptive.transport.peaks.hostWindowBytes).toBeGreaterThan(512 * 1024);
  }, 60_000);

  it("preserves a 3.35 MiB R5 bulk request across disconnect, loss, duplicate, and reorder faults", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      transportProfile: "bulk-v1",
      corpus: "incompressible",
      rttMs: 300,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
      concurrency: 1,
      timeoutMs: 30_000,
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
          actions: ["duplicate", "reorder"],
        },
      ],
    });

    expectExactAndBounded(report);
    expect(report.upstream.calls).toBe(1);
    expect(report.wire.disconnectedDrops).toBe(2);
    expect(report.wire.droppedUpdates).toBe(1);
    expect(report.wire.duplicateDeliveries).toBe(1);
    expect(report.wire.reorderedUpdates).toBe(1);
    expect(report.transport.retransmissions).toBeGreaterThanOrEqual(3);
    expect(report.transport.peaks.hostWindowBytes).toBeGreaterThan(512 * 1024);
  }, 45_000);

  it("rotates the credential while a 3.35 MiB R5 bulk request is in transport", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
      transportProfile: "bulk-v1",
      corpus: "incompressible",
      rttMs: 300,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.mib3_35,
      concurrency: 1,
      rotateCredentialDuringTransfer: true,
      timeoutMs: 30_000,
    });

    expectExactAndBounded(report);
    expect(report.operations.credentialRotations).toBe(1);
    expect(report.upstream.calls).toBe(1);
    expect(report.transport.retransmissions).toBe(0);
    expect(report.transport.peaks.hostWindowBytes).toBeGreaterThan(512 * 1024);
  }, 45_000);

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

  it("dispatches 1/4/16/32/40 Sol requests identically under standard and bulk-v1", async () => {
    for (const concurrency of V2_PERFORMANCE_CONCURRENCY) {
      const standard = await runV2PerformanceHarness({
        rttMs: 50,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency,
      });
      const bulk = await runV2PerformanceHarness({
        protocolRevision: "r2",
        transportProfile: "bulk-v1",
        rttMs: 50,
        requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
        concurrency,
      });
      expectExactAndBounded(standard);
      expectExactAndBounded(bulk);
      // The upstream gate is held until every request arrives. Reaching this
      // exact maximum proves no model-wide 16-request gate delayed dispatch.
      expect(standard.upstream.maxActive).toBe(concurrency);
      expect(bulk.upstream.maxActive).toBe(concurrency);
      expect(bulk.transport.peaks.hostWindowBytes).toBeLessThanOrEqual(512 * 1024);
      expect(bulk.wire.uniqueRequestDataBytes).toBe(standard.wire.uniqueRequestDataBytes);
      expect(bulk.requests.map(request => request.ackRoundsAtCommit).sort((a, b) => a - b))
        .toEqual(standard.requests.map(request => request.ackRoundsAtCommit).sort((a, b) => a - b));
      expect(Math.abs(
        bulk.timings.modeledLink.commitMs.p95 - standard.timings.modeledLink.commitMs.p95,
      )).toBeLessThanOrEqual(0.01);
    }
  }, 60_000);

  it("delivers 17 concurrent 512-event bursts without ambiguity or loss", async () => {
    const report = await runV2PerformanceHarness({
      rttMs: 50,
      requestBytes: V2_PERFORMANCE_PAYLOAD_BYTES.kib128,
      concurrency: 17,
      responseEventCount: 512,
      timeoutMs: 60_000,
    });

    expectExactAndBounded(report, 512);
    expect(report.timings.measuredWall.totalMs.p95).toBeLessThan(10_000);
  }, 60_000);
});
