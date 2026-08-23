import { describe, expect, it } from "bun:test";
import { WebSocketTelemetry } from "../src/ws/telemetry";

describe("WebSocketTelemetry", () => {
  it("keeps aggregate fixed-memory delivery metrics without identities", () => {
    const telemetry = new WebSocketTelemetry({ capacity: 16 });
    telemetry.sessionOpened(); telemetry.responseStarted(); telemetry.sendQueued(1);
    telemetry.recordSend({ queueMs: 12, callbackMs: 3, bufferedBytes: 2_048, delayed: true });
    const value = telemetry.snapshotAndReset();
    expect(value).toMatchObject({ activeSessions: 1, activeResponses: 1, sendQueueDepthPeak: 1, bufferedBytesPeak: 2_048, delayedByBackpressure: 1, enqueueToSend: { p95: 12 }, sendCallback: { p95: 3 } });
    expect(JSON.stringify(value)).not.toMatch(/client|stream|url|token|prompt/i);
    telemetry.responseFinished(); telemetry.sessionClosed();
  });
});
