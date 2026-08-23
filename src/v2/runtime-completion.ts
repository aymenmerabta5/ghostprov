import { SYSTEM_CLOCK, type Clock } from "../core/ports";

export class DecayingRuntimeCompletionEstimate {
  readonly #clock: Clock;
  readonly #baselineMs: number;
  readonly #maximumSampleMs: number;
  readonly #weight: number;
  readonly #halfLifeMs: number;
  #estimateMs: number;
  #updatedAtMs: number;

  constructor(options: {
    clock?: Clock;
    baselineMs?: number;
    maximumSampleMs?: number;
    weight?: number;
    halfLifeMs?: number;
  } = {}) {
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#baselineMs = options.baselineMs ?? 10_000;
    this.#maximumSampleMs = options.maximumSampleMs ?? 60_000;
    this.#weight = options.weight ?? 0.20;
    this.#halfLifeMs = options.halfLifeMs ?? 60_000;
    if (!Number.isFinite(this.#baselineMs) || this.#baselineMs < 1) {
      throw new RangeError("completion baseline must be positive");
    }
    if (
      !Number.isFinite(this.#maximumSampleMs) ||
      this.#maximumSampleMs < this.#baselineMs
    ) {
      throw new RangeError("maximum completion sample must be at least the baseline");
    }
    if (!Number.isFinite(this.#weight) || this.#weight <= 0 || this.#weight > 1) {
      throw new RangeError("completion weight must be in (0, 1]");
    }
    if (!Number.isFinite(this.#halfLifeMs) || this.#halfLifeMs <= 0) {
      throw new RangeError("completion half-life must be positive");
    }
    this.#estimateMs = this.#baselineMs;
    this.#updatedAtMs = this.#now();
  }

  observe(durationMs: number): void {
    if (!Number.isFinite(durationMs)) {
      throw new RangeError("completion duration must be finite");
    }
    if (durationMs < 0) {
      throw new RangeError("completion duration must be non-negative");
    }
    this.#advance();
    const sampleMs = Math.max(1, Math.min(this.#maximumSampleMs, durationMs));
    this.#estimateMs = this.#estimateMs * (1 - this.#weight) +
      sampleMs * this.#weight;
  }

  value(): number {
    this.#advance();
    return this.#estimateMs;
  }

  #advance(): void {
    const now = this.#now();
    if (now < this.#updatedAtMs) {
      throw new RangeError("completion clock moved backwards");
    }
    const factor = Math.pow(0.5, (now - this.#updatedAtMs) / this.#halfLifeMs);
    this.#estimateMs = this.#baselineMs +
      (this.#estimateMs - this.#baselineMs) * factor;
    this.#updatedAtMs = now;
  }

  #now(): number {
    const value = this.#clock.now();
    if (!Number.isFinite(value)) {
      throw new RangeError("completion clock must be finite");
    }
    return value;
  }
}
