import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import { DecayingRuntimeCompletionEstimate } from "../src/v2/runtime-completion";

class ManualClock implements Clock {
  value = 0;

  now(): number {
    return this.value;
  }

  sleep(): Promise<void> {
    return Promise.resolve();
  }

  waitUntil(): Promise<void> {
    return Promise.resolve();
  }
}

describe("DecayingRuntimeCompletionEstimate", () => {
  it("caps extreme completions and recovers toward baseline while idle", () => {
    const clock = new ManualClock();
    const estimate = new DecayingRuntimeCompletionEstimate({ clock });

    expect(estimate.value()).toBe(10_000);
    estimate.observe(553_855);
    expect(estimate.value()).toBe(20_000);
    estimate.observe(553_855);
    expect(estimate.value()).toBe(28_000);

    clock.value = 60_000;
    expect(estimate.value()).toBe(19_000);
    clock.value = 120_000;
    expect(estimate.value()).toBe(14_500);
  });

  it("tracks ordinary completions without dropping below one millisecond", () => {
    const clock = new ManualClock();
    const ordinary = new DecayingRuntimeCompletionEstimate({ clock });
    ordinary.observe(6_000);
    expect(ordinary.value()).toBe(9_200);

    const zero = new DecayingRuntimeCompletionEstimate({ clock });
    zero.observe(0);
    expect(zero.value()).toBeCloseTo(8_000.2);
  });

  it("rejects invalid tuning, samples, and non-monotonic clocks", () => {
    const clock = new ManualClock();
    expect(() => new DecayingRuntimeCompletionEstimate({ clock, baselineMs: 0 })).toThrow();
    expect(() => new DecayingRuntimeCompletionEstimate({
      clock,
      baselineMs: 20_000,
      maximumSampleMs: 10_000,
    })).toThrow();
    expect(() => new DecayingRuntimeCompletionEstimate({ clock, weight: 0 })).toThrow();
    expect(() => new DecayingRuntimeCompletionEstimate({ clock, weight: 1.01 })).toThrow();
    expect(() => new DecayingRuntimeCompletionEstimate({ clock, halfLifeMs: 0 })).toThrow();

    const estimate = new DecayingRuntimeCompletionEstimate({ clock });
    expect(() => estimate.observe(-1)).toThrow("non-negative");
    expect(() => estimate.observe(Number.NaN)).toThrow("finite");
    clock.value = 100;
    estimate.value();
    clock.value = 99;
    expect(() => estimate.value()).toThrow("clock moved backwards");
  });
});
