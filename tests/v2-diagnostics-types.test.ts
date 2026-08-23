import { describe, expect, it } from "bun:test";
import {
  encodedDiagnosticBytes,
  parseDiagnosticObject,
  type V2EnvelopeDiagnostics,
} from "../src/v2/diagnostics/types";

export function safeDiagnostics(): V2EnvelopeDiagnostics {
  return {
    version: 1,
    resource: {
      processRssBytes: 100,
      heapUsedBytes: 50,
      heapTotalBytes: 80,
      externalBytes: 10,
      systemTotalBytes: null,
      systemFreeBytes: null,
      processCpuPercent: null,
      normalizedLoad1: null,
      eventLoopP50Ms: 1,
      eventLoopP95Ms: 2,
      eventLoopMaxMs: 3,
    },
    transport: {
      windowFrames: 1,
      windowBytes: 100,
      schedulerFrames: 2,
      schedulerBytes: 200,
      schedulerStreams: 1,
      oldestQueuedFrameMs: 4,
      ackProgressAgeMs: 5,
      retransmissionAttempt: 0,
      peakRetransmissionAttempt: 1,
      retransmissionsTotal: 2,
      publicationsTotal: 3,
      publishedBytesTotal: 400,
      admittedUncommitted: 1,
      roundTripEwmaMs: 6,
      pacedPublicationsTotal: 7,
      urgentPublicationsTotal: 3,
      logicalEventsTotal: 40,
      responseBatchFramesTotal: 8,
      batchedEventsTotal: 32,
      dataPublicationsTotal: 6,
      controlPublicationsTotal: 4,
      dataPublishedBytesTotal: 300,
      controlPublishedBytesTotal: 100,
      flightCompletionsTotal: 5,
      adaptiveCapacity: 8,
      capacityIncreasesTotal: 1,
      capacityReductionsTotal: 2,
      unadmittedTimeoutsTotal: 3,
    },
    malformedCount: 0,
  };
}

describe("V2 diagnostic types", () => {
  it("parses a fixed safe snapshot inside the two KiB ceiling", () => {
    const value = safeDiagnostics();
    expect(parseDiagnosticObject(value)).toEqual(value);
    expect(encodedDiagnosticBytes(value)).toBeLessThanOrEqual(2_048);
  });

  it("rejects forbidden, non-finite, unknown, and oversized values", () => {
    expect(parseDiagnosticObject({ ...safeDiagnostics(), prompt: "forbidden" })).toBeNull();
    expect(parseDiagnosticObject({
      ...safeDiagnostics(),
      resource: { ...safeDiagnostics().resource!, processRssBytes: Number.NaN },
    })).toBeNull();
    expect(parseDiagnosticObject({ ...safeDiagnostics(), unknown: 1 })).toBeNull();
    expect(parseDiagnosticObject({ ...safeDiagnostics(), padding: "x".repeat(3_000) })).toBeNull();
  });
});
