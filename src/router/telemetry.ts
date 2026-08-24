import { BoundedDistribution, type DistributionSnapshot } from "../v2/diagnostics/statistics";

export interface RouterTrafficSnapshot {
  accepted: number;
  committed: number;
  succeeded: number;
  failed: number;
  rateLimited: number;
  retries: number;
  responseBytes: number;
  queueWaitMs: DistributionSnapshot;
  commitAckMs: DistributionSnapshot;
  totalMs: DistributionSnapshot;
  firstByteMs: DistributionSnapshot;
}

/** Fixed-memory interval telemetry for soak-test speed and scalability analysis. */
export class RouterTrafficTelemetry {
  readonly #queueWait: BoundedDistribution;
  readonly #commitAck: BoundedDistribution;
  readonly #total: BoundedDistribution;
  readonly #firstByte: BoundedDistribution;
  #accepted = 0;
  #committed = 0;
  #succeeded = 0;
  #failed = 0;
  #rateLimited = 0;
  #retries = 0;
  #responseBytes = 0;

  constructor(capacity = 1_024) {
    this.#queueWait = new BoundedDistribution(capacity);
    this.#commitAck = new BoundedDistribution(capacity);
    this.#total = new BoundedDistribution(capacity);
    this.#firstByte = new BoundedDistribution(capacity);
  }

  accepted(): void { this.#accepted++; }

  committed(queueWaitMs: number, commitAckMs: number): void {
    this.#committed++;
    this.#queueWait.record(queueWaitMs);
    this.#commitAck.record(commitAckMs);
  }

  retried(): void { this.#retries++; }

  settled(input: {
    totalMs: number;
    firstByteMs: number | null;
    responseBytes: number;
    succeeded: boolean;
    rateLimited: boolean;
  }): void {
    if (input.succeeded) this.#succeeded++;
    else this.#failed++;
    if (input.rateLimited) this.#rateLimited++;
    this.#responseBytes += Math.max(0, Math.floor(input.responseBytes));
    this.#total.record(input.totalMs);
    if (input.firstByteMs !== null) this.#firstByte.record(input.firstByteMs);
  }

  rejected(input: { totalMs: number; rateLimited?: boolean }): void {
    this.#failed++;
    if (input.rateLimited) this.#rateLimited++;
    this.#total.record(input.totalMs);
  }

  snapshotAndReset(): RouterTrafficSnapshot {
    const snapshot = {
      accepted: this.#accepted,
      committed: this.#committed,
      succeeded: this.#succeeded,
      failed: this.#failed,
      rateLimited: this.#rateLimited,
      retries: this.#retries,
      responseBytes: this.#responseBytes,
      queueWaitMs: this.#queueWait.snapshotAndReset(),
      commitAckMs: this.#commitAck.snapshotAndReset(),
      totalMs: this.#total.snapshotAndReset(),
      firstByteMs: this.#firstByte.snapshotAndReset(),
    };
    this.#accepted = 0;
    this.#committed = 0;
    this.#succeeded = 0;
    this.#failed = 0;
    this.#rateLimited = 0;
    this.#retries = 0;
    this.#responseBytes = 0;
    return snapshot;
  }
}
