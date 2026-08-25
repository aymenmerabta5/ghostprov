import {
  BoundedDistribution,
  type DistributionSnapshot,
} from "../v2/diagnostics/statistics";

export interface WebSocketTelemetrySnapshot {
  activeSessions: number;
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
  enqueueToSend: DistributionSnapshot;
  sendCallback: DistributionSnapshot;
}

/** Aggregate, fixed-memory telemetry with no client or stream identities. */
export class WebSocketTelemetry {
  readonly #queue: BoundedDistribution;
  readonly #callback: BoundedDistribution;
  #sessions = 0;
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

  constructor(options: { capacity?: number } = {}) {
    this.#queue = new BoundedDistribution(options.capacity ?? 256);
    this.#callback = new BoundedDistribution(options.capacity ?? 256);
  }

  sessionOpened(): void { this.#sessions++; }
  sessionClosed(): void { this.#sessions = Math.max(0, this.#sessions - 1); }
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
    this.#sendQueuePeak = this.#sendQueueDepth;
    this.#sendQueueBytesPeak = this.#sendQueueBytes;
    this.#waitingSendPeak = this.#waitingSends;
    this.#waitingSendBytesPeak = this.#waitingSendBytes;
    this.#rejectedSends = 0;
    this.#deliveryErrors = 0;
    this.#bufferPeak = 0;
    this.#delayed = 0;
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
