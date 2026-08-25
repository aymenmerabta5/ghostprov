export interface CongestionSample {
  schedulerFrames: number;
  schedulerBytes: number;
  oldestQueuedFrameMs: number;
  ackProgressAgeMs: number;
  windowFrames: number;
}

export class TransportCongestion {
  readonly #highAgeMs: number;
  readonly #lowAgeMs: number;
  #congested = false;
  #healthySamples = 0;

  constructor(options: { highAgeMs?: number; lowAgeMs?: number } = {}) {
    this.#highAgeMs = options.highAgeMs ?? 2_000;
    this.#lowAgeMs = options.lowAgeMs ?? Math.min(500, this.#highAgeMs / 4);
    if (
      !Number.isFinite(this.#highAgeMs) || this.#highAgeMs < 1 ||
      !Number.isFinite(this.#lowAgeMs) || this.#lowAgeMs < 0 ||
      this.#lowAgeMs >= this.#highAgeMs
    ) throw new RangeError("transport congestion age thresholds are invalid");
  }

  update(value: CongestionSample): boolean {
    const drained = value.schedulerFrames === 0 &&
      value.schedulerBytes === 0 &&
      value.windowFrames === 0;
    if (drained) {
      this.#congested = false;
      this.#healthySamples = 0;
      return false;
    }

    const high = value.schedulerFrames >= 128 ||
      value.schedulerBytes >= 256 * 1024 ||
      value.oldestQueuedFrameMs >= this.#highAgeMs ||
      (value.windowFrames > 0 && value.ackProgressAgeMs >= this.#highAgeMs);
    if (high) {
      this.#congested = true;
      this.#healthySamples = 0;
      return true;
    }

    if (!this.#congested) return false;
    const low = value.schedulerFrames <= 32 &&
      value.schedulerBytes <= 64 * 1024 &&
      value.oldestQueuedFrameMs <= this.#lowAgeMs &&
      (value.windowFrames === 0 || value.ackProgressAgeMs <= this.#lowAgeMs);
    this.#healthySamples = low ? this.#healthySamples + 1 : 0;
    if (this.#healthySamples >= 2) {
      this.#congested = false;
      this.#healthySamples = 0;
    }
    return this.#congested;
  }

  get congested(): boolean {
    return this.#congested;
  }
}
