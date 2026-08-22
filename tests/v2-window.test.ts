import { describe, expect, it } from "bun:test";
import type { HostFrame, ReliableFrame } from "../src/v2/protocol/shared";
import {
  InboundFrameCursor,
  OutboundFrameWindow,
  V2WindowError,
} from "../src/v2/protocol/window";

function ping(sentAtMs: number): HostFrame {
  return { kind: "ping", sentAtMs };
}

function reliable(sequence: number, sentAtMs = sequence): ReliableFrame<HostFrame> {
  return { ...ping(sentAtMs), epoch: 7, sequence };
}

describe("V2 reliable frame windows", () => {
  it("retains unacknowledged frames and advances cumulatively", () => {
    const outbound = new OutboundFrameWindow<HostFrame>({ epoch: 7 });
    outbound.push(ping(1));
    outbound.push(ping(2));
    expect(outbound.snapshot().frames.map(frame => frame.sequence)).toEqual([0, 1]);

    outbound.acknowledge({ epoch: 7, sequence: 0 });
    expect(outbound.snapshot().frames.map(frame => frame.sequence)).toEqual([1]);
    outbound.acknowledge({ epoch: 6, sequence: 100 });
    expect(outbound.snapshot().frames.map(frame => frame.sequence)).toEqual([1]);
    expect(() => outbound.acknowledge({ epoch: 7, sequence: 9 })).toThrow("beyond published");
  });

  it("delivers reordered frames once and never acknowledges across a gap", () => {
    const inbound = new InboundFrameCursor<HostFrame>({ epoch: 7 });
    expect(inbound.accept([reliable(1), reliable(0), reliable(1)])).toEqual([
      reliable(0),
      reliable(1),
    ]);
    expect(inbound.ack()).toEqual({ epoch: 7, sequence: 1 });
    expect(inbound.accept([reliable(3)])).toEqual([]);
    expect(inbound.ack()).toEqual({ epoch: 7, sequence: 1 });
    expect(inbound.accept([reliable(2)])).toEqual([reliable(2), reliable(3)]);
    expect(inbound.ack()).toEqual({ epoch: 7, sequence: 3 });
  });

  it("rejects conflicting duplicates, future epochs, and oversized gaps", () => {
    const duplicate = new InboundFrameCursor<HostFrame>({ epoch: 7 });
    duplicate.accept([reliable(1, 1)]);
    expect(() => duplicate.accept([reliable(1, 2)])).toThrow("conflicting duplicate");
    expect(() => duplicate.accept([{ ...reliable(2), epoch: 8 }])).toThrow("future epoch");

    const gap = new InboundFrameCursor<HostFrame>({ epoch: 7 });
    gap.accept(Array.from({ length: 32 }, (_, index) => reliable(index + 1)));
    expect(() => gap.accept([reliable(33)])).toThrow("32-frame");
  });

  it("enforces the hard outbound frame count and physical frame size", () => {
    const window = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
    for (let index = 0; index < 32; index++) window.push(ping(index));
    expect(() => window.push(ping(33))).toThrow("32-frame");

    const oversized = new OutboundFrameWindow<any>({ epoch: 1 });
    expect(() => oversized.push({ kind: "credential_rotate", sealedJson: "x".repeat(20_000) })).toThrow("16 KiB");
  });

  it("resets epochs without accepting stale frames", () => {
    const outbound = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
    outbound.push(ping(1));
    outbound.reset(2);
    expect(outbound.snapshot()).toMatchObject({ epoch: 2, nextSequence: 0, frames: [] });

    const inbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
    inbound.reset(2);
    expect(inbound.accept([{ ...reliable(0), epoch: 1 }])).toEqual([]);
    expect(inbound.ack()).toEqual({ epoch: 2, sequence: -1 });
  });

  it("preserves ordering and uniqueness over more than 10,000 reordered operations", () => {
    const outbound = new OutboundFrameWindow<HostFrame>({ epoch: 7 });
    const inbound = new InboundFrameCursor<HostFrame>({ epoch: 7 });
    const delivered: ReliableFrame<HostFrame>[] = [];

    for (let batch = 0; batch < 313; batch++) {
      const published = Array.from({ length: 32 }, (_, offset) => outbound.push(
        ping(batch * 32 + offset),
      ));
      const network = [...published].reverse();
      network.splice(4, 0, network[4]);
      delivered.push(...inbound.accept(network));
      outbound.acknowledge(inbound.ack());
      expect(outbound.snapshot().frames).toEqual([]);
    }

    expect(delivered).toHaveLength(10_016);
    expect(new Set(delivered.map(frame => `${frame.epoch}:${frame.sequence}`)).size).toBe(delivered.length);
    expect(delivered.map(frame => frame.sequence)).toEqual(
      [...delivered].map(frame => frame.sequence).sort((left, right) => left - right),
    );
  });

  it("uses typed errors for invalid construction", () => {
    expect(() => new OutboundFrameWindow({ epoch: -1 })).toThrow(V2WindowError);
    expect(() => new InboundFrameCursor({ epoch: -1 })).toThrow(V2WindowError);
  });
});
