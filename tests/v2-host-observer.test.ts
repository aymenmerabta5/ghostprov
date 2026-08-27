import { describe, expect, it } from "bun:test";
import { HostRequestDiagnosticsObserver } from "../src/v2/diagnostics/host-observer";
import type { V2TransportDiagnostics } from "../src/v2/diagnostics/types";

function transportDiagnostics(publicationsTotal: number): V2TransportDiagnostics {
  return {
    windowFrames: publicationsTotal,
    windowBytes: 0,
    schedulerFrames: 0,
    schedulerBytes: 0,
    schedulerStreams: 0,
    oldestQueuedFrameMs: 0,
    ackProgressAgeMs: 0,
    retransmissionAttempt: 0,
    peakRetransmissionAttempt: 0,
    retransmissionsTotal: 0,
    publicationsTotal,
    publishedBytesTotal: 0,
    admittedUncommitted: 0,
    roundTripEwmaMs: null,
    pacedPublicationsTotal: 0,
    urgentPublicationsTotal: 0,
    logicalEventsTotal: 0,
    responseBatchFramesTotal: 0,
    batchedEventsTotal: 0,
    dataPublicationsTotal: 0,
    controlPublicationsTotal: 0,
    dataPublishedBytesTotal: 0,
    controlPublishedBytesTotal: 0,
    flightCompletionsTotal: 0,
    adaptiveCapacity: 0,
    capacityIncreasesTotal: 0,
    capacityReductionsTotal: 0,
    unadmittedTimeoutsTotal: 0,
  };
}

describe("HostRequestDiagnosticsObserver", () => {
  it("correlates host-local stages with sidecar durations without mixing clocks", () => {
    let now = 1_000_000;
    const observer = new HostRequestDiagnosticsObserver(() => now);
    now += 10; observer.prepared({ decodedBytes: 1_000, encodedBytes: 400, partCount: 1, encoding: "gzip+base64" });
    now += 5; observer.published();
    now += 20; observer.committed({ firstFrameToCommitMs: 4, commitToAssemblyMs: 1, claimSyncMs: 2, admissionWaitMs: 0, commitProcessingMs: 3, preparedToFetchStartMs: 7, fetchStartSyncMs: 2 });
    now += 25; observer.firstFrame({ upstreamFirstEventMs: 100, schedulerWaitMs: 4 });
    expect(observer.value()).toMatchObject({ requestDecodedBytes: 1_000, requestEncodedBytes: 400, requestPartCount: 1, requestEncoding: "gzip+base64", hostPrepareMs: 10, hostSchedulerWaitMs: 5, hostPublishToAckMs: 20, sidecarCommitToAssemblyMs: 1, sidecarPreparedToFetchStartMs: 7, sidecarFetchStartSyncMs: 2, sidecarCommitProcessingMs: 3, transportCommitResidualMs: 17, hostAckToFirstFrameMs: 25, upstreamFirstEventMs: 100, sidecarFirstEventSchedulerMs: 4 });
  });

  it("lazily snapshots shared peaks with request isolation and freezes on exit", () => {
    let current = { revision: 1, value: transportDiagnostics(1) };
    const source = () => current;
    const first = new HostRequestDiagnosticsObserver(undefined, source);
    expect(first.value().peakTransport).toBeNull();

    current = { revision: 2, value: transportDiagnostics(2) };
    const firstPeak = first.value().peakTransport!;
    expect(firstPeak.publicationsTotal).toBe(2);
    expect(first.value().peakTransport).toBe(firstPeak);

    const second = new HostRequestDiagnosticsObserver(undefined, source);
    expect(second.value().peakTransport).toBeNull();
    current = { revision: 3, value: transportDiagnostics(3) };
    const refreshedFirst = first.value().peakTransport!;
    const secondPeak = second.value().peakTransport!;
    expect(refreshedFirst).not.toBe(secondPeak);
    refreshedFirst.windowFrames = 999;
    expect(secondPeak.windowFrames).toBe(3);
    expect(current.value.windowFrames).toBe(3);

    first.freezePeak();
    const frozen = first.value().peakTransport;
    current = { revision: 4, value: transportDiagnostics(4) };
    expect(first.value().peakTransport).toBe(frozen);
    expect(second.value().peakTransport?.publicationsTotal).toBe(4);
  });

  it("follows a replacement transport only after its next publication", () => {
    let sourceOne = { revision: 1, value: transportDiagnostics(1) };
    const observer = new HostRequestDiagnosticsObserver(undefined, () => sourceOne);
    sourceOne = { revision: 2, value: transportDiagnostics(2) };
    expect(observer.value().peakTransport?.publicationsTotal).toBe(2);
    observer.freezePeak();

    let sourceTwo = { revision: 10, value: transportDiagnostics(10) };
    observer.followPeak(() => sourceTwo);
    expect(observer.value().peakTransport?.publicationsTotal).toBe(2);
    sourceTwo = { revision: 11, value: transportDiagnostics(11) };
    expect(observer.value().peakTransport?.publicationsTotal).toBe(11);
  });
});
