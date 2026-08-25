import { describe, expect, it } from "bun:test";
import {
  TransportCongestion,
  type CongestionSample,
} from "../src/v2/transport/congestion";

function sample(values: Partial<CongestionSample> = {}): CongestionSample {
  return {
    schedulerFrames: 0,
    schedulerBytes: 0,
    oldestQueuedFrameMs: 0,
    ackProgressAgeMs: 0,
    windowFrames: 0,
    ...values,
  };
}

describe("TransportCongestion", () => {
  it("retains two-sample hysteresis while low-water work remains active", () => {
    const congestion = new TransportCongestion();
    const lowActive = sample({
      schedulerFrames: 1,
      schedulerBytes: 1024,
      windowFrames: 1,
      ackProgressAgeMs: 100,
    });

    expect(congestion.update(sample({ schedulerFrames: 128 }))).toBe(true);
    expect(congestion.update(lowActive)).toBe(true);
    expect(congestion.update(lowActive)).toBe(false);
  });

  it("clears congestion on the first observation of a fully drained transport", () => {
    const congestion = new TransportCongestion();

    expect(congestion.update(sample({
      windowFrames: 1,
      ackProgressAgeMs: 2_000,
    }))).toBe(true);
    expect(congestion.update(sample())).toBe(false);
  });

  it("uses byte, queue-age and active-window ACK thresholds", () => {
    for (const value of [
      sample({ schedulerFrames: 1, schedulerBytes: 256 * 1024 }),
      sample({ schedulerFrames: 1, oldestQueuedFrameMs: 2_000 }),
      sample({ windowFrames: 1, ackProgressAgeMs: 2_000 }),
    ]) {
      expect(new TransportCongestion().update(value)).toBe(true);
    }
    expect(new TransportCongestion().update(sample({
      windowFrames: 0,
      ackProgressAgeMs: 9_999,
    }))).toBe(false);
  });

  it("supports a deterministic age fence without changing queue bounds", () => {
    const congestion = new TransportCongestion({ highAgeMs: 30, lowAgeMs: 5 });
    expect(congestion.update(sample({ windowFrames: 1, ackProgressAgeMs: 29 }))).toBe(false);
    expect(congestion.update(sample({ windowFrames: 1, ackProgressAgeMs: 30 }))).toBe(true);
    expect(() => new TransportCongestion({ highAgeMs: 10, lowAgeMs: 10 }))
      .toThrow("thresholds");
  });
});
