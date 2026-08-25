import { describe, expect, it } from "bun:test";
import {
  V2SchedulerError,
  V2SchedulerOverflowError,
  WeightedFrameScheduler,
} from "../src/v2/protocol/scheduler";

interface TestFrame {
  kind: string;
  requestId: string;
  value: number;
  payload?: string;
}

function frame(kind: string, requestId: string, value: number, payload?: string): TestFrame {
  return { kind, requestId, value, ...(payload === undefined ? {} : { payload }) };
}

describe("WeightedFrameScheduler", () => {
  it("reports queued bytes and oldest frame age without changing selection", () => {
    let now = 0;
    const scheduler = new WeightedFrameScheduler<any>({ now: () => now });
    scheduler.enqueue("data", "a", { value: "hello" });
    now = 25;
    expect(scheduler.snapshot()).toMatchObject({ queuedFrames: 1, streams: 1, queuedEncodedBytes: expect.any(Number), oldestQueuedFrameMs: 25 });
    expect(scheduler.take(1)).toEqual([{ value: "hello" }]);
  });
  it("keeps strict priority without letting a terminal overtake its own data", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    scheduler.enqueue("data", "large", frame("response_event", "large", 0));
    scheduler.enqueue("data", "large", frame("response_event", "large", 1));
    scheduler.enqueue("data", "small", frame("response_event", "small", 0));
    scheduler.enqueue("terminal", "small", frame("response_terminal", "small", 1));
    scheduler.enqueue("control", "control", frame("request_reject", "control", 1));

    const scheduled = scheduler.take(5);
    expect(scheduled[0]).toEqual(frame("request_reject", "control", 1));
    expect(scheduled.findIndex(value => value.kind === "response_terminal"))
      .toBeGreaterThan(scheduled.findIndex(value =>
        value.kind === "response_event" && value.requestId === "small"
      ));
    expect(scheduled).toHaveLength(5);
  });

  it("uses weighted byte deficits without starving a lower-weight stream", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 256,
      quantumBytes: 512,
    });
    for (let index = 0; index < 100; index++) {
      scheduler.enqueue("data", "heavy", frame("data", "heavy", index, "x".repeat(400)), 2);
      scheduler.enqueue("data", "light", frame("data", "light", index, "x".repeat(400)), 1);
    }

    const order = scheduler.take(30).map(value => value.requestId);
    expect(order.filter(value => value === "heavy")).toHaveLength(20);
    expect(order.filter(value => value === "light")).toHaveLength(10);
  });

  it("fairly schedules bytes rather than frame counts", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 256,
      quantumBytes: 1_024,
    });
    for (let index = 0; index < 100; index++) {
      scheduler.enqueue("data", "large", frame("data", "large", index, "x".repeat(900)));
      scheduler.enqueue("data", "small", frame("data", "small", index, "x".repeat(100)));
    }

    const scheduled = scheduler.take(60);
    const bytes = { large: 0, small: 0 };
    for (const value of scheduled) {
      bytes[value.requestId as keyof typeof bytes] += new TextEncoder()
        .encode(JSON.stringify(value)).byteLength;
    }
    expect(scheduled.filter(value => value.requestId === "small").length)
      .toBeGreaterThan(scheduled.filter(value => value.requestId === "large").length);
    expect(bytes.large / bytes.small).toBeGreaterThan(0.85);
    expect(bytes.large / bytes.small).toBeLessThan(1.15);
  });

  it("honors frame and encoded-byte budgets without dropping queued work", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    scheduler.enqueue("data", "one", frame("data", "one", 0, "x".repeat(64)));
    scheduler.enqueue("data", "two", frame("data", "two", 0, "y".repeat(64)));
    const oneFrameBytes = new TextEncoder().encode(JSON.stringify(frame("data", "one", 0, "x".repeat(64)))).byteLength;

    expect(scheduler.take(10, oneFrameBytes)).toHaveLength(1);
    expect(scheduler.snapshot().queuedFrames).toBe(1);
    expect(scheduler.take(1)).toHaveLength(1);
    expect(scheduler.snapshot()).toEqual({ queuedFrames: 0, streams: 0, queuedEncodedBytes: 0, oldestQueuedFrameMs: 0 });
  });

  it("removes one stream from every priority without affecting others", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    scheduler.enqueue("terminal", "gone", frame("terminal", "gone", 0));
    scheduler.enqueue("data", "gone", frame("data", "gone", 1));
    scheduler.enqueue("data", "kept", frame("data", "kept", 0));

    scheduler.removeStream("gone");

    expect(scheduler.take(10)).toEqual([frame("data", "kept", 0)]);
  });

  it("rejects invalid weights and budgets", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    expect(() => scheduler.enqueue("data", "one", frame("data", "one", 0), 0)).toThrow(V2SchedulerError);
    expect(() => scheduler.take(0)).toThrow(V2SchedulerError);
    expect(() => scheduler.take(1, 0)).toThrow(V2SchedulerError);
  });

  it("throws typed local overflow errors without mutating queue accounting", () => {
    const first = frame("data", "one", 0);
    const firstBytes = new TextEncoder().encode(JSON.stringify(first)).byteLength;

    const frames = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 1,
      maxQueuedEncodedBytes: 1_024,
      maxStreams: 2,
    });
    frames.enqueue("data", "one", first);
    try {
      frames.enqueue("data", "one", frame("data", "one", 1));
      throw new Error("expected frame overflow");
    } catch (error) {
      expect(error).toBeInstanceOf(V2SchedulerOverflowError);
      expect(error).toMatchObject({ dimension: "frames", code: "overflow_frames", limit: 1 });
    }
    expect(frames.snapshot()).toMatchObject({ queuedFrames: 1, streams: 1 });

    const bytes = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 2,
      maxQueuedEncodedBytes: firstBytes,
      maxStreams: 2,
    });
    bytes.enqueue("data", "one", first);
    expect(() => bytes.enqueue("data", "one", first)).toThrow(V2SchedulerOverflowError);
    expect(bytes.snapshot().queuedEncodedBytes).toBe(firstBytes);

    const streams = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 2,
      maxQueuedEncodedBytes: 1_024,
      maxStreams: 1,
    });
    streams.enqueue("data", "one", first);
    try {
      streams.enqueue("data", "two", frame("data", "two", 0));
      throw new Error("expected stream overflow");
    } catch (error) {
      expect(error).toMatchObject({ dimension: "streams", code: "overflow_streams", limit: 1 });
    }
    expect(streams.snapshot()).toMatchObject({ queuedFrames: 1, streams: 1 });
  });

  it("drains a 10k-frame backlog with bounded bookkeeping cost", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>({
      maxQueuedFrames: 10_000,
      maxQueuedEncodedBytes: 4 * 1024 * 1024,
      maxStreams: 100,
    });
    const started = performance.now();
    for (let index = 0; index < 10_000; index++) {
      const stream = `stream-${index % 100}`;
      scheduler.enqueue("data", stream, frame("data", stream, index));
    }
    expect(scheduler.snapshot()).toMatchObject({ queuedFrames: 10_000, streams: 100 });
    expect(scheduler.take(10_000)).toHaveLength(10_000);
    expect(scheduler.snapshot()).toEqual({
      queuedFrames: 0,
      streams: 0,
      queuedEncodedBytes: 0,
      oldestQueuedFrameMs: 0,
    });
    expect(performance.now() - started).toBeLessThan(2_000);
  });
});
