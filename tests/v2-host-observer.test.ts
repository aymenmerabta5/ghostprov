import { describe, expect, it } from "bun:test";
import { HostRequestDiagnosticsObserver } from "../src/v2/diagnostics/host-observer";

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
});
