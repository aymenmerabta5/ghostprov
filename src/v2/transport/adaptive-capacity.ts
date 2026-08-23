export interface TransportHealthSample {
  rttMs: number;
  congested: boolean;
  ackStalled: boolean;
}

export interface AdaptiveTransportCapacitySnapshot {
  limit: number;
  healthyStreak: number;
  increases: number;
  reductions: number;
}

export class AdaptiveTransportCapacity {
  readonly #maximum: number;
  readonly #severeRttMs: number;
  readonly #healthyRttMs: number;
  readonly #healthySamples: number;
  #limit: number;
  #healthyStreak = 0;
  #increases = 0;
  #reductions = 0;

  constructor(options: {
    initial: number;
    maximum: number;
    severeRttMs?: number;
    healthyRttMs?: number;
    healthySamples?: number;
  }) {
    if (!Number.isSafeInteger(options.initial) || options.initial < 1) {
      throw new RangeError("initial capacity must be a positive safe integer");
    }
    if (!Number.isSafeInteger(options.maximum) || options.maximum < options.initial) {
      throw new RangeError("maximum capacity must be at least initial capacity");
    }
    this.#severeRttMs = options.severeRttMs ?? 5_000;
    this.#healthyRttMs = options.healthyRttMs ?? 750;
    this.#healthySamples = options.healthySamples ?? 8;
    if (
      !Number.isFinite(this.#severeRttMs) ||
      !Number.isFinite(this.#healthyRttMs) ||
      this.#healthyRttMs < 0 ||
      this.#severeRttMs <= this.#healthyRttMs
    ) throw new RangeError("adaptive RTT thresholds are invalid");
    if (!Number.isSafeInteger(this.#healthySamples) || this.#healthySamples < 1) {
      throw new RangeError("healthySamples must be a positive safe integer");
    }
    this.#limit = options.initial;
    this.#maximum = options.maximum;
  }

  get limit(): number {
    return this.#limit;
  }

  observe(sample: TransportHealthSample): boolean {
    if (!Number.isFinite(sample.rttMs) || sample.rttMs < 0) {
      throw new RangeError("rttMs must be finite and non-negative");
    }
    if (sample.congested || sample.ackStalled || sample.rttMs >= this.#severeRttMs) {
      this.#healthyStreak = 0;
      const next = Math.max(1, Math.ceil(this.#limit / 2));
      if (next === this.#limit) return false;
      this.#limit = next;
      this.#reductions++;
      return true;
    }
    if (sample.rttMs > this.#healthyRttMs) {
      this.#healthyStreak = 0;
      return false;
    }
    this.#healthyStreak++;
    if (this.#healthyStreak < this.#healthySamples || this.#limit >= this.#maximum) {
      return false;
    }
    this.#healthyStreak = 0;
    this.#limit++;
    this.#increases++;
    return true;
  }

  available(active: number, engineCredits: number): number {
    if (!Number.isSafeInteger(active) || active < 0) {
      throw new RangeError("active must be a non-negative safe integer");
    }
    if (!Number.isSafeInteger(engineCredits) || engineCredits < 0) {
      throw new RangeError("engineCredits must be a non-negative safe integer");
    }
    return Math.max(0, Math.min(engineCredits, this.#limit - active));
  }

  snapshot(): AdaptiveTransportCapacitySnapshot {
    return {
      limit: this.#limit,
      healthyStreak: this.#healthyStreak,
      increases: this.#increases,
      reductions: this.#reductions,
    };
  }
}
