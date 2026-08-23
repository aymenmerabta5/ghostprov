import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  DecayingRuntimeFailureScore,
  classifyRuntimeError,
} from "../src/v2/runtime-health";

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

describe("V2 runtime health classification", () => {
  it("keeps caller, lifecycle, and definitive upstream outcomes neutral", () => {
    const neutral = [
      new DOMException("aborted", "AbortError"),
      { committed: true, code: "cancelled", ambiguous: true },
      { committed: true, code: "missing_required_parameter", ambiguous: false },
      { committed: true, code: "http_429", ambiguous: false },
      { committed: false, code: "capacity_changed" },
      { committed: false, code: "capacity" },
      { committed: false, code: "draining" },
      { committed: false, code: "request_too_large" },
      { committed: true, code: "response_too_large", ambiguous: true },
    ];

    for (const error of neutral) {
      expect(classifyRuntimeError(error)).toBe("neutral");
    }
  });

  it("attributes ambiguous, protocol, and unknown transport failures", () => {
    const unhealthy = [
      { committed: true, code: "internal", ambiguous: true },
      { committed: false, code: "protocol" },
      { committed: false, code: "instance_changed" },
      new Error("channel failed"),
    ];

    for (const error of unhealthy) {
      expect(classifyRuntimeError(error)).toBe("unhealthy");
    }
  });
});

describe("DecayingRuntimeFailureScore", () => {
  it("accumulates failures, heals on success, and decays while idle", () => {
    const clock = new ManualClock();
    const score = new DecayingRuntimeFailureScore({ clock });

    score.record("unhealthy");
    expect(score.value()).toBeCloseTo(0.20);

    clock.value = 60_000;
    expect(score.value()).toBeCloseTo(0.10);

    score.record("unhealthy");
    expect(score.value()).toBeCloseTo(0.28);

    score.record("healthy");
    expect(score.value()).toBeCloseTo(0.224);
  });

  it("keeps one hundred client cancellations at a zero failure score", () => {
    const clock = new ManualClock();
    const score = new DecayingRuntimeFailureScore({ clock });

    for (let index = 0; index < 100; index++) {
      score.record(classifyRuntimeError({
        committed: true,
        code: "cancelled",
        ambiguous: true,
      }));
    }

    expect(score.value()).toBe(0);
  });

  it("rejects invalid tuning and non-monotonic clocks", () => {
    const clock = new ManualClock();
    expect(() => new DecayingRuntimeFailureScore({ clock, weight: 0 })).toThrow();
    expect(() => new DecayingRuntimeFailureScore({ clock, weight: 1.01 })).toThrow();
    expect(() => new DecayingRuntimeFailureScore({ clock, halfLifeMs: 0 })).toThrow();

    clock.value = 100;
    const score = new DecayingRuntimeFailureScore({ clock });
    clock.value = 99;
    expect(() => score.value()).toThrow("clock moved backwards");
  });
});
