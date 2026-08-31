import {
  BoundedDistribution,
  type DistributionSnapshot,
} from "../v2/diagnostics/statistics";

export const WEBSOCKET_CLOSE_CAUSES = Object.freeze([
  "remote_close",
  "heartbeat_timeout",
  "delivery_error",
  "protocol_error",
  "server_shutdown",
  "idle_timeout",
  "ingress_capacity",
  "socket_error",
] as const);

export type WebSocketCloseCause = typeof WEBSOCKET_CLOSE_CAUSES[number];

export type WebSocketCloseCauseCounts = Readonly<
  Record<WebSocketCloseCause, number>
>;

export interface WebSocketTelemetrySnapshot {
  activeSessions: number;
  closedSessions: number;
  closedActiveResponses: number;
  closeCauses: WebSocketCloseCauseCounts;
  activeResponses: number;
  activeResponseBytes: number;
  pendingRequests: number;
  pendingRequestBytes: number;
  waitingRequests: number;
  waitingRequestBytes: number;
  requestQueueDepthPeak: number;
  requestQueueBytesPeak: number;
  waitingRequestDepthPeak: number;
  waitingRequestBytesPeak: number;
  rejectedRequests: number;
  sendQueueDepth: number;
  sendQueueBytes: number;
  activeSends: number;
  waitingSends: number;
  waitingSendBytes: number;
  sendQueueDepthPeak: number;
  sendQueueBytesPeak: number;
  waitingSendDepthPeak: number;
  waitingSendBytesPeak: number;
  rejectedSends: number;
  deliveryErrors: number;
  bufferedBytesPeak: number;
  delayedByBackpressure: number;
  ingressMessageBytesPeak: number;
  ingressPromotionBytesPeak: number;
  ingressReservedBytes: number;
  ingressReservedBytesPeak: number;
  ingressCapacityBytes: number;
  ingressGuaranteedSessions: number;
  ingressPromotions: number;
  ingressRejections: number;
  ingressParserRejections: number;
  ingressUpgradeRejections: number;
  ingressPromotionsTotal: number;
  ingressRejectionsTotal: number;
  ingressParserRejectionsTotal: number;
  ingressUpgradeRejectionsTotal: number;
  responseGraphRejections: number;
  responseGraphRejectionsTotal: number;
  enqueueToSend: DistributionSnapshot;
  sendCallback: DistributionSnapshot;
}

/** Aggregate, fixed-memory telemetry with no client or stream identities. */
export class WebSocketTelemetry {
  readonly #queue: BoundedDistribution;
  readonly #callback: BoundedDistribution;
  #sessions = 0;
  #closedSessions = 0;
  #closedActiveResponses = 0;
  readonly #closeCauses = emptyCloseCauseCounts();
  #responses = 0;
  #activeResponseBytes = 0;
  #pendingRequests = 0;
  #pendingRequestBytes = 0;
  #requestQueuePeak = 0;
  #requestQueueBytesPeak = 0;
  #waitingRequestPeak = 0;
  #waitingRequestBytesPeak = 0;
  #rejectedRequests = 0;
  #sendQueueDepth = 0;
  #sendQueueBytes = 0;
  #activeSends = 0;
  #waitingSends = 0;
  #waitingSendBytes = 0;
  #sendQueuePeak = 0;
  #sendQueueBytesPeak = 0;
  #waitingSendPeak = 0;
  #waitingSendBytesPeak = 0;
  #rejectedSends = 0;
  #deliveryErrors = 0;
  #bufferPeak = 0;
  #delayed = 0;
  #ingressMessageBytesPeak = 0;
  #ingressPromotionBytesPeak = 0;
  #ingressReservedBytes = 0;
  #ingressReservedBytesPeak = 0;
  #ingressCapacityBytes = 0;
  #ingressGuaranteedSessions = 0;
  #ingressPromotions = 0;
  #ingressRejections = 0;
  #ingressParserRejections = 0;
  #ingressUpgradeRejections = 0;
  #ingressPromotionsTotal = 0;
  #ingressRejectionsTotal = 0;
  #ingressParserRejectionsTotal = 0;
  #ingressUpgradeRejectionsTotal = 0;
  #responseGraphRejections = 0;
  #responseGraphRejectionsTotal = 0;

  constructor(options: { capacity?: number } = {}) {
    this.#queue = new BoundedDistribution(options.capacity ?? 256);
    this.#callback = new BoundedDistribution(options.capacity ?? 256);
  }

  sessionOpened(): void { this.#sessions++; }
  sessionClosed(cause: WebSocketCloseCause, activeResponses = 0): void {
    switch (cause) {
      case "remote_close":
      case "heartbeat_timeout":
      case "delivery_error":
      case "protocol_error":
      case "server_shutdown":
      case "idle_timeout":
      case "ingress_capacity":
      case "socket_error":
        this.#closeCauses[cause]++;
        break;
      default:
        throw new TypeError("Unsupported WebSocket close cause");
    }
    this.#sessions = Math.max(0, this.#sessions - 1);
    this.#closedSessions++;
    this.#closedActiveResponses += nonNegativeInteger(activeResponses);
  }
  responseStarted(bytes = 0): void {
    this.#responses++;
    this.#activeResponseBytes += nonNegativeInteger(bytes);
    this.#observeRequestPressure();
  }
  responseFinished(bytes = 0): void {
    this.#responses = Math.max(0, this.#responses - 1);
    this.#activeResponseBytes = Math.max(
      0,
      this.#activeResponseBytes - nonNegativeInteger(bytes),
    );
    this.#observeRequestPressure();
  }

  requestQueued(bytes: number): void {
    this.#pendingRequests++;
    this.#pendingRequestBytes += nonNegativeInteger(bytes);
    this.#requestQueuePeak = Math.max(this.#requestQueuePeak, this.#pendingRequests);
    this.#requestQueueBytesPeak = Math.max(
      this.#requestQueueBytesPeak,
      this.#pendingRequestBytes,
    );
    this.#observeRequestPressure();
  }

  requestSettled(bytes: number): void {
    this.#pendingRequests = Math.max(0, this.#pendingRequests - 1);
    this.#pendingRequestBytes = Math.max(
      0,
      this.#pendingRequestBytes - nonNegativeInteger(bytes),
    );
    this.#observeRequestPressure();
  }

  requestRejected(): void { this.#rejectedRequests++; }

  sendQueued(bytes: number): void {
    const value = nonNegativeInteger(bytes);
    this.#sendQueueDepth++;
    this.#sendQueueBytes += value;
    this.#waitingSends++;
    this.#waitingSendBytes += value;
    this.#sendQueuePeak = Math.max(this.#sendQueuePeak, this.#sendQueueDepth);
    this.#sendQueueBytesPeak = Math.max(
      this.#sendQueueBytesPeak,
      this.#sendQueueBytes,
    );
    this.#waitingSendPeak = Math.max(this.#waitingSendPeak, this.#waitingSends);
    this.#waitingSendBytesPeak = Math.max(
      this.#waitingSendBytesPeak,
      this.#waitingSendBytes,
    );
  }

  sendStarted(bytes: number): void {
    const value = nonNegativeInteger(bytes);
    this.#waitingSends = Math.max(0, this.#waitingSends - 1);
    this.#waitingSendBytes = Math.max(0, this.#waitingSendBytes - value);
    this.#activeSends++;
  }

  sendSettled(bytes: number, started = false): void {
    const value = nonNegativeInteger(bytes);
    this.#sendQueueDepth = Math.max(0, this.#sendQueueDepth - 1);
    this.#sendQueueBytes = Math.max(
      0,
      this.#sendQueueBytes - value,
    );
    if (started) this.#activeSends = Math.max(0, this.#activeSends - 1);
    else {
      this.#waitingSends = Math.max(0, this.#waitingSends - 1);
      this.#waitingSendBytes = Math.max(0, this.#waitingSendBytes - value);
    }
  }

  sendRejected(): void { this.#rejectedSends++; }
  deliveryFailed(): void { this.#deliveryErrors++; }
  recordIngressMessageBytes(bytes: number): void {
    this.#ingressMessageBytesPeak = Math.max(
      this.#ingressMessageBytesPeak,
      nonNegativeInteger(bytes),
    );
  }
  recordIngressBudget(value: {
    ingressReservedBytes: number;
    maxIngressBytes: number;
    ingressGuaranteedSessions?: number;
  }): void {
    this.#ingressReservedBytes = nonNegativeInteger(value.ingressReservedBytes);
    this.#ingressCapacityBytes = nonNegativeInteger(value.maxIngressBytes);
    if (value.ingressGuaranteedSessions !== undefined) {
      this.#ingressGuaranteedSessions = nonNegativeInteger(value.ingressGuaranteedSessions);
    }
    this.#ingressReservedBytesPeak = Math.max(
      this.#ingressReservedBytesPeak,
      this.#ingressReservedBytes,
    );
  }
  ingressPromoted(dynamicBytes = 0): void {
    this.#ingressPromotions++;
    this.#ingressPromotionsTotal++;
    this.#ingressPromotionBytesPeak = Math.max(
      this.#ingressPromotionBytesPeak,
      nonNegativeInteger(dynamicBytes),
    );
  }
  ingressRejected(): void {
    this.#ingressRejections++;
    this.#ingressRejectionsTotal++;
  }
  ingressParserRejected(): void {
    this.#ingressParserRejections++;
    this.#ingressParserRejectionsTotal++;
  }
  ingressUpgradeRejected(): void {
    this.#ingressUpgradeRejections++;
    this.#ingressUpgradeRejectionsTotal++;
  }
  responseGraphRejected(): void {
    this.#responseGraphRejections++;
    this.#responseGraphRejectionsTotal++;
  }

  recordSend(value: {
    queueMs: number;
    callbackMs: number;
    bufferedBytes: number;
    delayed: boolean;
  }): void {
    this.#queue.record(value.queueMs);
    this.#callback.record(value.callbackMs);
    this.#bufferPeak = Math.max(this.#bufferPeak, Math.max(0, value.bufferedBytes));
    if (value.delayed) this.#delayed++;
  }

  snapshotAndReset(): WebSocketTelemetrySnapshot {
    const value = {
      activeSessions: this.#sessions,
      closedSessions: this.#closedSessions,
      closedActiveResponses: this.#closedActiveResponses,
      closeCauses: Object.freeze({ ...this.#closeCauses }),
      activeResponses: this.#responses,
      activeResponseBytes: this.#activeResponseBytes,
      pendingRequests: this.#pendingRequests,
      pendingRequestBytes: this.#pendingRequestBytes,
      waitingRequests: Math.max(0, this.#pendingRequests - this.#responses),
      waitingRequestBytes: Math.max(
        0,
        this.#pendingRequestBytes - this.#activeResponseBytes,
      ),
      requestQueueDepthPeak: this.#requestQueuePeak,
      requestQueueBytesPeak: this.#requestQueueBytesPeak,
      waitingRequestDepthPeak: this.#waitingRequestPeak,
      waitingRequestBytesPeak: this.#waitingRequestBytesPeak,
      rejectedRequests: this.#rejectedRequests,
      sendQueueDepth: this.#sendQueueDepth,
      sendQueueBytes: this.#sendQueueBytes,
      activeSends: this.#activeSends,
      waitingSends: this.#waitingSends,
      waitingSendBytes: this.#waitingSendBytes,
      sendQueueDepthPeak: this.#sendQueuePeak,
      sendQueueBytesPeak: this.#sendQueueBytesPeak,
      waitingSendDepthPeak: this.#waitingSendPeak,
      waitingSendBytesPeak: this.#waitingSendBytesPeak,
      rejectedSends: this.#rejectedSends,
      deliveryErrors: this.#deliveryErrors,
      bufferedBytesPeak: this.#bufferPeak,
      delayedByBackpressure: this.#delayed,
      ingressMessageBytesPeak: this.#ingressMessageBytesPeak,
      ingressPromotionBytesPeak: this.#ingressPromotionBytesPeak,
      ingressReservedBytes: this.#ingressReservedBytes,
      ingressReservedBytesPeak: this.#ingressReservedBytesPeak,
      ingressCapacityBytes: this.#ingressCapacityBytes,
      ingressGuaranteedSessions: this.#ingressGuaranteedSessions,
      ingressPromotions: this.#ingressPromotions,
      ingressRejections: this.#ingressRejections,
      ingressParserRejections: this.#ingressParserRejections,
      ingressUpgradeRejections: this.#ingressUpgradeRejections,
      ingressPromotionsTotal: this.#ingressPromotionsTotal,
      ingressRejectionsTotal: this.#ingressRejectionsTotal,
      ingressParserRejectionsTotal: this.#ingressParserRejectionsTotal,
      ingressUpgradeRejectionsTotal: this.#ingressUpgradeRejectionsTotal,
      responseGraphRejections: this.#responseGraphRejections,
      responseGraphRejectionsTotal: this.#responseGraphRejectionsTotal,
      enqueueToSend: this.#queue.snapshotAndReset(),
      sendCallback: this.#callback.snapshotAndReset(),
    };
    this.#requestQueuePeak = this.#pendingRequests;
    this.#requestQueueBytesPeak = this.#pendingRequestBytes;
    this.#waitingRequestPeak = Math.max(0, this.#pendingRequests - this.#responses);
    this.#waitingRequestBytesPeak = Math.max(
      0,
      this.#pendingRequestBytes - this.#activeResponseBytes,
    );
    this.#rejectedRequests = 0;
    this.#closedSessions = 0;
    this.#closedActiveResponses = 0;
    for (const cause of WEBSOCKET_CLOSE_CAUSES) this.#closeCauses[cause] = 0;
    this.#sendQueuePeak = this.#sendQueueDepth;
    this.#sendQueueBytesPeak = this.#sendQueueBytes;
    this.#waitingSendPeak = this.#waitingSends;
    this.#waitingSendBytesPeak = this.#waitingSendBytes;
    this.#rejectedSends = 0;
    this.#deliveryErrors = 0;
    this.#bufferPeak = 0;
    this.#delayed = 0;
    this.#ingressMessageBytesPeak = 0;
    this.#ingressPromotionBytesPeak = 0;
    this.#ingressReservedBytesPeak = this.#ingressReservedBytes;
    this.#ingressPromotions = 0;
    this.#ingressRejections = 0;
    this.#ingressParserRejections = 0;
    this.#ingressUpgradeRejections = 0;
    this.#responseGraphRejections = 0;
    return value;
  }

  #observeRequestPressure(): void {
    this.#waitingRequestPeak = Math.max(
      this.#waitingRequestPeak,
      Math.max(0, this.#pendingRequests - this.#responses),
    );
    this.#waitingRequestBytesPeak = Math.max(
      this.#waitingRequestBytesPeak,
      Math.max(0, this.#pendingRequestBytes - this.#activeResponseBytes),
    );
  }
}

function nonNegativeInteger(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

function emptyCloseCauseCounts(): Record<WebSocketCloseCause, number> {
  return {
    remote_close: 0,
    heartbeat_timeout: 0,
    delivery_error: 0,
    protocol_error: 0,
    server_shutdown: 0,
    idle_timeout: 0,
    ingress_capacity: 0,
    socket_error: 0,
  };
}
