import { describe, expect, it } from "bun:test";
import {
  V2SchedulerError,
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
  it("prioritizes terminal and control traffic then round-robins data streams", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    scheduler.enqueue("data", "large", frame("response_event", "large", 0));
    scheduler.enqueue("data", "large", frame("response_event", "large", 1));
    scheduler.enqueue("data", "small", frame("response_event", "small", 0));
    scheduler.enqueue("terminal", "small", frame("response_terminal", "small", 1));
    scheduler.enqueue("control", "control", frame("request_reject", "control", 1));

    expect(scheduler.take(5).map(value => [value.kind, value.requestId, value.value])).toEqual([
      ["response_terminal", "small", 1],
      ["request_reject", "control", 1],
      ["response_event", "large", 0],
      ["response_event", "small", 0],
      ["response_event", "large", 1],
    ]);
  });

  it("uses smooth weights without starving a lower-weight stream", () => {
    const scheduler = new WeightedFrameScheduler<TestFrame>();
    for (let index = 0; index < 6; index++) {
      scheduler.enqueue("data", "heavy", frame("data", "heavy", index), 2);
    }
    for (let index = 0; index < 3; index++) {
      scheduler.enqueue("data", "light", frame("data", "light", index), 1);
    }

    const order = scheduler.take(9).map(value => value.requestId);
    expect(order.slice(0, 3)).toEqual(["heavy", "light", "heavy"]);
    expect(order.filter(value => value === "heavy")).toHaveLength(6);
    expect(order.filter(value => value === "light")).toHaveLength(3);
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
});
