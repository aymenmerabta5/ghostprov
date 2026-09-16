import { describe, expect, it } from "bun:test";
import { ProgressAwareCongestion, type ProgressCongestionSample } from "../src/v2/transport/progress-congestion";

const sample = (values: Partial<ProgressCongestionSample> = {}): ProgressCongestionSample => ({
  schedulerFrames: 16, schedulerBytes: 100_503, oldestQueuedFrameMs: 2_240,
  windowFrames: 32, ackProgressAgeMs: 0.23, roundTripMs: 1_428,
  roundTripVariationMs: 41, ...values,
});

describe("progress-aware transport congestion", () => {
  it("keeps the logged response backlog admissible while ACKs advance", () => {
    const congestion = new ProgressAwareCongestion({ highAgeMs: 2_000 });
    for (let turn = 0; turn < 100; turn++) {
      expect(congestion.update(sample({ ackProgressed: true, oldestQueuedFrameMs: 2_240 + turn * 1_200 }))).toBe(false);
      expect(congestion.update(sample({ ackProgressAgeMs: 1_200 }))).toBe(false);
    }
  });

  it("retains hard queue pressure through ACK progress until low water", () => {
    for (const high of [{ schedulerFrames: 128 }, { schedulerBytes: 256 * 1024 }]) {
      const congestion = new ProgressAwareCongestion({ highAgeMs: 2_000 });
      expect(congestion.update(sample({ ...high, ackProgressed: true }))).toBe(true);
      expect(congestion.update(sample({ ackProgressed: true }))).toBe(true);
      const low = sample({ schedulerFrames: 32, schedulerBytes: 64 * 1024, ackProgressed: true });
      expect(congestion.update(low)).toBe(true);
      expect(congestion.update(low)).toBe(false);
    }
  });

  it("closes on a bounded RTT-aware stall and needs real progress to reopen", () => {
    // Short policy seam exercises RTT adaptation; host/R10 production pass 60 seconds.
    const congestion = new ProgressAwareCongestion({ highAgeMs: 2_000 });
    const slow = sample({ roundTripMs: 2_500, roundTripVariationMs: 500 });
    expect(congestion.update({ ...slow, ackProgressAgeMs: 4_499 })).toBe(false);
    expect(congestion.stallThresholdMs).toBe(4_500);
    expect(congestion.update({ ...slow, ackProgressAgeMs: 4_500 })).toBe(true);
    expect(congestion.stalled).toBe(true);
    expect(congestion.update({ ...slow, ackProgressAgeMs: 0 })).toBe(true);
    expect(congestion.update({ ...slow, ackProgressed: true, ackProgressAgeMs: 0 })).toBe(false);
    expect(congestion.update({ ...slow, roundTripMs: 50_000, roundTripVariationMs: 50_000, ackProgressAgeMs: 10_000 })).toBe(true);
    expect(congestion.stallThresholdMs).toBe(10_000);
  });

  it("detects stalled scheduling without a flight and clears a drained transport", () => {
    const congestion = new ProgressAwareCongestion({ highAgeMs: 2_000 });
    expect(congestion.update(sample({ windowFrames: 0, oldestQueuedFrameMs: 2_000 }))).toBe(true);
    expect(congestion.update(sample({ windowFrames: 0, schedulerFrames: 0, schedulerBytes: 0 }))).toBe(false);
  });

  it("rejects invalid latency evidence and preserves explicit stall bounds", () => {
    expect(() => new ProgressAwareCongestion({ highAgeMs: 0 })).toThrow();
    const congestion = new ProgressAwareCongestion({ highAgeMs: 30 });
    expect(congestion.update(sample({ roundTripMs: null, roundTripVariationMs: null, ackProgressAgeMs: 29 }))).toBe(false);
    expect(congestion.update(sample({ roundTripMs: null, roundTripVariationMs: null, ackProgressAgeMs: 30 }))).toBe(true);
    for (const invalid of [-1, Number.NaN, Infinity]) {
      expect(() => congestion.update(sample({ roundTripMs: invalid }))).toThrow();
    }
  });
});
