import { describe, expect, it } from "bun:test";
import { HostRequestDiagnosticsObserver } from "../src/v2/diagnostics/host-observer";

describe("HostRequestDiagnosticsObserver", () => {
  it("correlates host-local stages with sidecar durations without mixing clocks", () => {
    let now = 1_000_000;
    const observer = new HostRequestDiagnosticsObserver(() => now);
    now += 10; observer.prepared();
    now += 5; observer.published();
    now += 20; observer.committed({ firstFrameToCommitMs: 4, commitToAssemblyMs: 1, claimSyncMs: 2, admissionWaitMs: 0, commitProcessingMs: 3 });
    now += 25; observer.firstFrame({ upstreamFirstEventMs: 100, schedulerWaitMs: 4 });
    expect(observer.value()).toMatchObject({ hostPrepareMs: 10, hostSchedulerWaitMs: 5, hostPublishToAckMs: 20, sidecarCommitProcessingMs: 3, transportCommitResidualMs: 17, hostAckToFirstFrameMs: 25, upstreamFirstEventMs: 100, sidecarFirstEventSchedulerMs: 4 });
  });
});
