import { describe, expect, it } from "bun:test";
import { BoundedDistribution } from "../src/v2/diagnostics/statistics";

describe("BoundedDistribution", () => {
  it("keeps fixed capacity and resets interval percentiles", () => {
    const values = new BoundedDistribution(4);
    for (const value of [1, 2, 3, 4, 100]) values.record(value);
    values.record(Number.NaN);
    values.record(-1);
    expect(values.snapshotAndReset()).toEqual({ count: 4, p50: 3, p95: 100, max: 100 });
    expect(values.snapshotAndReset()).toEqual({ count: 0, p50: 0, p95: 0, max: 0 });
    expect(values.estimatedBytes).toBeLessThanOrEqual(64);
  });

  it("rejects unbounded capacities", () => {
    expect(() => new BoundedDistribution(0)).toThrow("capacity");
    expect(() => new BoundedDistribution(4_097)).toThrow("capacity");
  });
});
