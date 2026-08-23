import { describe, expect, it } from "bun:test";
import { AckClockedFlight } from "../src/v2/protocol/flight";
import {
  GP_V2_FLIGHT_MAX_BYTES,
  GP_V2_FLIGHT_MAX_FRAMES,
  type FrameIdentity,
} from "../src/v2/protocol/shared";

const frames = (...sequences: number[]): FrameIdentity[] => sequences.map(sequence => ({
  epoch: 1,
  sequence,
}));

describe("AckClockedFlight", () => {
  it("freezes one reliable sequence set until its high ACK", () => {
    const flight = new AckClockedFlight({ epoch: 1 });
    flight.published(frames(0, 1, 2));

    expect(flight.active).toBe(true);
    expect(flight.highSequence).toBe(2);
    expect(flight.frameCount).toBe(3);
    expect(flight.acknowledged({ epoch: 1, sequence: 0 })).toBe(false);
    expect(flight.frameCount).toBe(2);
    expect(() => flight.published(frames(1, 2))).not.toThrow();
    expect(() => flight.published(frames(1, 2, 3))).toThrow("immutable");

    expect(flight.acknowledged({ epoch: 1, sequence: 2 })).toBe(true);
    expect(flight.active).toBe(false);
    expect(flight.highSequence).toBeNull();
  });

  it("ignores stale ACKs and resets only to a newer epoch", () => {
    const flight = new AckClockedFlight({ epoch: 2 });
    flight.published([{ epoch: 2, sequence: 0 }]);
    expect(flight.acknowledged({ epoch: 1, sequence: 99 })).toBe(false);
    expect(() => flight.acknowledged({ epoch: 3, sequence: 0 })).toThrow("future");
    flight.reset(3);
    expect(flight.active).toBe(false);
    expect(() => flight.reset(3)).toThrow("advance");
  });

  it("rejects malformed or conflicting publications", () => {
    const flight = new AckClockedFlight({ epoch: 1 });
    flight.published([]);
    expect(flight.active).toBe(false);
    expect(() => flight.published(frames(1, 0))).toThrow("increasing");
    expect(() => flight.published([{ epoch: 2, sequence: 0 }])).toThrow("epoch");
    flight.published(frames(0, 1));
    expect(() => flight.published([])).toThrow("active flight");
    expect(() => flight.published(frames(1))).toThrow("immutable");
  });

  it("publishes the bounded flight targets independently of hard windows", () => {
    expect(GP_V2_FLIGHT_MAX_FRAMES).toBe(32);
    expect(GP_V2_FLIGHT_MAX_BYTES).toBe(128 * 1024);
  });
});
