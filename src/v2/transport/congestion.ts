export interface CongestionSample {
  schedulerFrames: number;
  schedulerBytes: number;
  oldestQueuedFrameMs: number;
  ackProgressAgeMs: number;
  windowFrames: number;
}

export class TransportCongestion {
  #congested = false;
  #healthySamples = 0;

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
      value.oldestQueuedFrameMs >= 2_000 ||
      (value.windowFrames > 0 && value.ackProgressAgeMs >= 2_000);
    if (high) {
      this.#congested = true;
      this.#healthySamples = 0;
      return true;
    }

    if (!this.#congested) return false;
    const low = value.schedulerFrames <= 32 &&
      value.schedulerBytes <= 64 * 1024 &&
      value.oldestQueuedFrameMs <= 500 &&
      (value.windowFrames === 0 || value.ackProgressAgeMs <= 500);
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
