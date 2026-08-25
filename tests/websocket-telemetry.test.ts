import { describe, expect, it } from "bun:test";
import { WebSocketTelemetry } from "../src/ws/telemetry";

describe("WebSocketTelemetry", () => {
  it("keeps aggregate fixed-memory delivery metrics without identities", () => {
    const telemetry = new WebSocketTelemetry({ capacity: 16 });
    telemetry.sessionOpened();
    telemetry.requestQueued(512); telemetry.responseStarted(512);
    telemetry.requestQueued(256);
    telemetry.sendQueued(1_024); telemetry.sendStarted(1_024);
    telemetry.sendQueued(512);
    telemetry.recordSend({ queueMs: 12, callbackMs: 3, bufferedBytes: 2_048, delayed: true });
    telemetry.recordIngressMessageBytes(6 * 1024 * 1024);
    telemetry.recordIngressBudget({ ingressReservedBytes: 17 * 1024 * 1024, maxIngressBytes: 512 * 1024 * 1024, ingressGuaranteedSessions: 27 });
    telemetry.ingressPromoted(8 * 1024 * 1024);
    telemetry.ingressRejected(); telemetry.ingressParserRejected(); telemetry.ingressUpgradeRejected();
    telemetry.responseGraphRejected();
    const value = telemetry.snapshotAndReset();
    expect(value).toMatchObject({
      activeSessions: 1,
      activeResponses: 1,
      activeResponseBytes: 512,
      pendingRequests: 2,
      pendingRequestBytes: 768,
      waitingRequests: 1,
      waitingRequestBytes: 256,
      requestQueueDepthPeak: 2,
      requestQueueBytesPeak: 768,
      waitingRequestDepthPeak: 1,
      waitingRequestBytesPeak: 512,
      sendQueueDepth: 2,
      sendQueueBytes: 1_536,
      activeSends: 1,
      waitingSends: 1,
      waitingSendBytes: 512,
      sendQueueDepthPeak: 2,
      sendQueueBytesPeak: 1_536,
      waitingSendDepthPeak: 1,
      waitingSendBytesPeak: 1_024,
      bufferedBytesPeak: 2_048,
      delayedByBackpressure: 1,
      ingressMessageBytesPeak: 6 * 1024 * 1024,
      ingressPromotionBytesPeak: 8 * 1024 * 1024,
      ingressReservedBytes: 17 * 1024 * 1024,
      ingressReservedBytesPeak: 17 * 1024 * 1024,
      ingressCapacityBytes: 512 * 1024 * 1024,
      ingressGuaranteedSessions: 27,
      ingressPromotions: 1,
      ingressRejections: 1,
      ingressParserRejections: 1,
      ingressUpgradeRejections: 1,
      ingressPromotionsTotal: 1,
      ingressRejectionsTotal: 1,
      ingressParserRejectionsTotal: 1,
      ingressUpgradeRejectionsTotal: 1,
      responseGraphRejections: 1,
      responseGraphRejectionsTotal: 1,
      enqueueToSend: { p95: 12 },
      sendCallback: { p95: 3 },
    });
    expect(JSON.stringify(value)).not.toMatch(/client|stream|url|token|prompt/i);
    expect(telemetry.snapshotAndReset()).toMatchObject({
      ingressPromotions: 0,
      ingressRejections: 0,
      ingressParserRejections: 0,
      ingressUpgradeRejections: 0,
      ingressPromotionsTotal: 1,
      ingressRejectionsTotal: 1,
      ingressParserRejectionsTotal: 1,
      ingressUpgradeRejectionsTotal: 1,
      responseGraphRejections: 0,
      responseGraphRejectionsTotal: 1,
      ingressReservedBytes: 17 * 1024 * 1024,
    });
    telemetry.sendSettled(1_024, true); telemetry.sendSettled(512);
    telemetry.responseFinished(512);
    telemetry.requestSettled(512); telemetry.requestSettled(256);
    telemetry.sessionClosed();
  });
});
