export interface DistributionSnapshot { count: number; p50: number; p95: number; max: number }

export class BoundedDistribution {
  readonly #values: Float64Array;
  #count = 0;
  #cursor = 0;

  constructor(capacity = 256) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4_096) {
      throw new RangeError("distribution capacity must be 1 through 4096");
    }
    this.#values = new Float64Array(capacity);
  }

  get estimatedBytes(): number { return this.#values.byteLength + 32; }

  record(value: number): void {
    if (!Number.isFinite(value) || value < 0) return;
    this.#values[this.#cursor] = value;
    this.#cursor = (this.#cursor + 1) % this.#values.length;
    this.#count = Math.min(this.#count + 1, this.#values.length);
  }

  /** A bounded rolling snapshot. Reading diagnostics must not mutate them. */
  snapshot(): DistributionSnapshot {
    const sorted = Array.from(this.#values.subarray(0, this.#count)).sort((a, b) => a - b);
    const percentile = (p: number) => sorted.length
      ? sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]!
      : 0;
    return { count: sorted.length, p50: percentile(0.5), p95: percentile(0.95), max: percentile(1) };
  }

  snapshotAndReset(): DistributionSnapshot {
    const snapshot = this.snapshot();
    this.#count = 0;
    this.#cursor = 0;
    return snapshot;
  }
}
