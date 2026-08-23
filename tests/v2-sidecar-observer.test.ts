import { describe, expect, it } from "bun:test";
import { SidecarDiagnosticsObserver } from "../src/v2/diagnostics/sidecar-observer";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";

describe("SidecarDiagnosticsObserver", () => {
  it("reports only local request durations and bounded transport counters", () => {
    let now = 0;
    const sampler = new SidecarResourceSampler({ enabled: false, intervalMs: 1_000 });
    const observer = new SidecarDiagnosticsObserver({ enabled: true, now: () => now, sampler });
    observer.requestOpened("req-1"); now = 4; observer.commitReceived("req-1"); now = 5; observer.assemblyCompleted("req-1"); observer.claimCompleted("req-1", 2); observer.admissionCompleted("req-1", 3);
    now = 10;
    expect(observer.requestAck("req-1")).toEqual({ firstFrameToCommitMs: 4, commitToAssemblyMs: 1, claimSyncMs: 2, admissionWaitMs: 3, commitProcessingMs: 6 });
    observer.fetchStarted("req-1"); now = 25;
    expect(observer.upstreamEvent("req-1", 0)).toEqual({ upstreamFirstEventMs: 15, schedulerWaitMs: 0 });
    observer.transport({ windowFrames: 2, windowBytes: 100, schedulerFrames: 3, schedulerBytes: 200, schedulerStreams: 1, retransmissionAttempt: 1, retransmissionsTotal: 2, publicationsTotal: 3, publishedBytesTotal: 400, admittedUncommitted: 1, roundTripEwmaMs: null, pacedPublicationsTotal: 2, urgentPublicationsTotal: 1, logicalEventsTotal: 8, responseBatchFramesTotal: 2, batchedEventsTotal: 6 });
    expect(observer.envelope().transport).toMatchObject({ windowFrames: 2, schedulerFrames: 3, pacedPublicationsTotal: 2, logicalEventsTotal: 8, batchedEventsTotal: 6 });
  });
});
