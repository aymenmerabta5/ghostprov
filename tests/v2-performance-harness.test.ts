import { describe, expect, it } from "bun:test";
import {
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  canonicalJson,
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

function expectExactAndBounded(report: V2PerformanceHarnessReport): void {
  expect(report.requests).toHaveLength(report.concurrency);
  expect(report.upstream.calls).toBe(report.concurrency);
  expect(report.upstream.maxActive).toBe(report.concurrency);
  expect(report.requests.every(value => value.exactRequest)).toBe(true);
  expect(report.requests.every(value => value.exactTerminal)).toBe(true);
  expect(report.requests.every(value => value.eventCount === 2)).toBe(true);
  expect(report.requests.every(value => value.ackRoundsAtCommit > 0)).toBe(true);
  expect(report.requests.every(value =>
    value.modeledFirstEventMs >= value.modeledCommitMs)).toBe(true);
  expect(report.requests.every(value =>
    value.wallFirstEventMs >= value.wallCommitMs)).toBe(true);
  expect(report.requests.every(value =>
    value.wallCommitMs >= value.hostPreparationWallMs)).toBe(true);
  expect(report.preparation.gzipCalls).toBe(report.concurrency);
  expect(report.preparation.gzipWallMs).toBeGreaterThanOrEqual(0);
  expect(report.wire.usefulRequestBytes).toBe(report.requestBytes * report.concurrency);
  expect(report.wire.uniqueRequestDataBytes).toBe(report.wire.postCompressionRequestBytes);
  expect(report.wire.postCompressionRequestBytes).toBeGreaterThan(0);
  expect(report.wire.attemptedBytes).toBeGreaterThan(report.wire.postCompressionRequestBytes);
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

  it("preserves a 3.35 MiB R2 request across disconnect, loss, duplicate, and reorder faults", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
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
  }, 45_000);

  it("rotates the credential while a 3.35 MiB R2 request is in transport", async () => {
    const report = await runV2PerformanceHarness({
      protocolRevision: "r2",
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
