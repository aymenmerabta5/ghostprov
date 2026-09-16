import type { CongestionSample } from "./congestion";

export interface ProgressCongestionSample extends CongestionSample {
  /** True only after a validated cumulative ACK advances in this observation. */
  ackProgressed?: boolean;
  roundTripMs?: number | null;
  roundTripVariationMs?: number | null;
}

/** Bounded queue pressure and stalled delivery are separate admission gates. */
export class ProgressAwareCongestion {
  readonly #baseStallMs: number;
  #stallThresholdMs: number;
  #pressure = false;
  #pressureRecoverySamples = 0;
  #stalled = false;

  constructor(options: { highAgeMs?: number } = {}) {
    const stallMs = options.highAgeMs ?? 2_000;
    if (!Number.isFinite(stallMs) || stallMs < 1) {
      throw new RangeError("transport congestion age threshold is invalid");
    }
    this.#baseStallMs = stallMs;
    this.#stallThresholdMs = stallMs;
  }

  update(value: ProgressCongestionSample): boolean {
    const rtt = this.#duration(value.roundTripMs);
    const variation = this.#duration(value.roundTripVariationMs);
    this.#stallThresholdMs = Math.min(
      Math.max(this.#baseStallMs, 10_000),
      Math.max(this.#baseStallMs, rtt + 4 * variation),
    );
    if (value.schedulerFrames === 0 && value.schedulerBytes === 0 && value.windowFrames === 0) {
      this.#pressure = false;
      this.#pressureRecoverySamples = 0;
      this.#stalled = false;
      return false;
    }

    // Preserve the existing high/low byte and frame limits. Real buffering
    // pressure still closes admission, even when acknowledgements are healthy.
    if (value.schedulerFrames >= 128 || value.schedulerBytes >= 256 * 1024) {
      this.#pressure = true;
      this.#pressureRecoverySamples = 0;
    } else if (this.#pressure) {
      const low = value.schedulerFrames <= 32 && value.schedulerBytes <= 64 * 1024;
      this.#pressureRecoverySamples = low ? this.#pressureRecoverySamples + 1 : 0;
      if (this.#pressureRecoverySamples >= 2) {
        this.#pressure = false;
        this.#pressureRecoverySamples = 0;
      }
    }

    // Queue age describes waiting behind an immutable flight, not lack of
    // delivery. A real ACK can reopen a latency gate with output still queued.
    // It cannot reopen the independent byte/frame pressure gate above.
    if (value.ackProgressed) {
      this.#stalled = false;
    } else if (
      value.windowFrames > 0
        ? value.ackProgressAgeMs >= this.#stallThresholdMs
        : value.schedulerFrames > 0 && value.oldestQueuedFrameMs >= this.#stallThresholdMs
    ) {
      this.#stalled = true;
    }
    return this.congested;
  }

  get congested(): boolean { return this.#pressure || this.#stalled; }
  get stalled(): boolean { return this.#stalled; }
  get stallThresholdMs(): number { return this.#stallThresholdMs; }

  #duration(value: number | null | undefined): number {
    if (value === undefined || value === null) return 0;
    if (!Number.isFinite(value) || value < 0) throw new RangeError("transport RTT must be finite and non-negative");
    return value;
  }
}
