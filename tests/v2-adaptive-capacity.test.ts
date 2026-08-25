import { describe, expect, it } from "bun:test";
import { AdaptiveTransportCapacity } from "../src/v2/transport/adaptive-capacity";

describe("AdaptiveTransportCapacity", () => {
  it("backs off severe transport latency and grows after a healthy streak", () => {
    const capacity = new AdaptiveTransportCapacity({ initial: 8, maximum: 16 });
    for (let index = 0; index < 64; index++) {
      capacity.observe({ rttMs: 500, congested: false });
    }
    expect(capacity.limit).toBe(16);
    expect(capacity.observe({ rttMs: 5_000, congested: false })).toBe(true);
    expect(capacity.limit).toBe(8);
    expect(capacity.observe({ rttMs: 10_000, congested: true })).toBe(true);
    expect(capacity.limit).toBe(4);
  });

  it("caps advertised credits by active work, engine credits, and health", () => {
    const capacity = new AdaptiveTransportCapacity({ initial: 8, maximum: 16 });
    expect(capacity.available(3, 100)).toBe(5);
    expect(capacity.available(3, 2)).toBe(2);
    expect(capacity.available(8, 100)).toBe(0);
    capacity.observe({ rttMs: 1, congested: true });
    expect(capacity.limit).toBe(4);
    expect(capacity.available(3, 100)).toBe(1);
  });

  it("requires consecutive health and never crosses configured bounds", () => {
    const capacity = new AdaptiveTransportCapacity({ initial: 1, maximum: 2 });
    for (let index = 0; index < 7; index++) {
      capacity.observe({ rttMs: 500, congested: false });
    }
    capacity.observe({ rttMs: 2_000, congested: false });
    capacity.observe({ rttMs: 500, congested: false });
    expect(capacity.limit).toBe(1);
    for (let index = 0; index < 16; index++) {
      capacity.observe({ rttMs: 500, congested: false });
    }
    expect(capacity.limit).toBe(2);
    capacity.observe({ rttMs: 10_000, congested: false });
    expect(capacity.limit).toBe(1);
  });

  it("validates construction and samples", () => {
    expect(() => new AdaptiveTransportCapacity({ initial: 0, maximum: 1 })).toThrow("initial");
    expect(() => new AdaptiveTransportCapacity({ initial: 2, maximum: 1 })).toThrow("maximum");
    expect(() => new AdaptiveTransportCapacity({ initial: 2, maximum: 3, minimum: 3 }))
      .toThrow("minimum");
    const capacity = new AdaptiveTransportCapacity({ initial: 1, maximum: 1 });
    expect(() => capacity.observe({
      rttMs: Number.NaN,
      congested: false,
    })).toThrow("rttMs");
    expect(() => capacity.available(-1, 1)).toThrow("active");
  });
});
