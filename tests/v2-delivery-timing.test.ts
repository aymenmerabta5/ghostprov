import { describe, expect, it } from "bun:test";
import { ReliableDeliveryTiming } from "../src/v2/protocol/delivery-timing";

describe("ReliableDeliveryTiming", () => {
  it("measures first-send sequence RTT and updates a cumulative ACK EWMA", () => {
    const timing = new ReliableDeliveryTiming({
      epoch: 1,
      minimumMs: 250,
      maxTracked: 128,
    });

    timing.published([{ epoch: 1, sequence: 0 }], 100);
    timing.published([{ epoch: 1, sequence: 0 }], 180);
    expect(timing.trackedSequences).toBe(1);
    expect(timing.acknowledged({ epoch: 1, sequence: 0 }, false, 300)).toBe(true);
    expect(timing.roundTripEwmaMs).toBe(200);
    expect(timing.trackedSequences).toBe(0);

    timing.published([{ epoch: 1, sequence: 1 }], 400);
    expect(timing.acknowledged({ epoch: 1, sequence: 1 }, false, 520)).toBe(true);
    expect(timing.roundTripEwmaMs).toBe(184);

    timing.published([
      { epoch: 1, sequence: 2 },
      { epoch: 1, sequence: 3 },
    ], 600);
    timing.published([{ epoch: 1, sequence: 3 }], 620);
    expect(timing.acknowledged({ epoch: 1, sequence: 3 }, false, 700)).toBe(true);
    expect(timing.roundTripEwmaMs).toBeCloseTo(167.2, 8);
  });

  it("anchors its deadline to progress rather than duplicate publications", () => {
    const timing = new ReliableDeliveryTiming({ epoch: 1, minimumMs: 250 });
    timing.published([{ epoch: 1, sequence: 0 }], 100);
    expect(timing.nextDelayMs(100, 0)).toBe(250);

    timing.published([{ epoch: 1, sequence: 0 }], 180);
    expect(timing.nextDelayMs(180, 0)).toBe(170);

    timing.retransmitted(350);
    expect(timing.attempt).toBe(1);
    expect(timing.nextDelayMs(350, 0)).toBe(500);

    timing.published([
      { epoch: 1, sequence: 0 },
      { epoch: 1, sequence: 1 },
    ], 400);
    expect(timing.nextDelayMs(400, 0)).toBe(450);

    expect(timing.acknowledged({ epoch: 1, sequence: 0 }, true, 500)).toBe(true);
    expect(timing.attempt).toBe(0);
    expect(timing.nextDelayMs(500, 0)).toBe(800);
  });

  it("bounds sequence timestamps and resets cleanly on an epoch advance", () => {
    const timing = new ReliableDeliveryTiming({ epoch: 1, maxTracked: 2 });
    timing.published([
      { epoch: 1, sequence: 0 },
      { epoch: 1, sequence: 1 },
    ], 10);
    expect(() => timing.published([{ epoch: 1, sequence: 2 }], 11)).toThrow("sequence bound");
    expect(timing.acknowledged({ epoch: 0, sequence: 99 }, true, 20)).toBe(false);
    expect(() => timing.acknowledged({ epoch: 2, sequence: 0 }, true, 20)).toThrow("future");

    timing.acknowledged({ epoch: 1, sequence: 1 }, false, 30);
    expect(timing.roundTripEwmaMs).toBe(20);
    timing.reset(2);
    expect(timing.roundTripEwmaMs).toBeNull();
    expect(timing.attempt).toBe(0);
    expect(timing.trackedSequences).toBe(0);
    expect(timing.nextDelayMs(30, 0)).toBeNull();
    expect(() => timing.reset(2)).toThrow("advance");
  });

  it("rejects invalid construction and impossible delivery transitions", () => {
    expect(() => new ReliableDeliveryTiming({ epoch: -1 })).toThrow("epoch");
    expect(() => new ReliableDeliveryTiming({ epoch: 1, minimumMs: 0 })).toThrow("minimumMs");
    expect(() => new ReliableDeliveryTiming({ epoch: 1, maxTracked: 0 })).toThrow("maxTracked");

    const timing = new ReliableDeliveryTiming({ epoch: 1 });
    expect(() => timing.published([{ epoch: 1, sequence: -1 }], 0)).toThrow("sequence");
    expect(() => timing.published([{ epoch: 2, sequence: 0 }], 0)).toThrow("epoch");
    expect(() => timing.published([{ epoch: 1, sequence: 0 }], Number.NaN)).toThrow("time");
    expect(() => timing.retransmitted(0)).toThrow("empty");
  });
});
