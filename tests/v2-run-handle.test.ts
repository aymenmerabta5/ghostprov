import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  V2CommittedError,
  V2RunHandle,
} from "../src/v2/transport/run-handle";

class FakeClock implements Clock {
  value = 1_000;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> {
    this.value = Math.max(this.value, deadlineMs);
  }
}

async function promiseState(value: Promise<unknown>): Promise<"pending" | "settled"> {
  return Promise.race([
    value.then(() => "settled" as const, () => "settled" as const),
    new Promise<"pending">(resolve => queueMicrotask(() => resolve("pending"))),
  ]);
}

describe("V2RunHandle", () => {
  it("streams accepted events, records UTF-8 stats, and settles once", async () => {
    const clock = new FakeClock();
    const handle = new V2RunHandle({ clock, maxBufferedBytes: 4_096 });
    clock.value += 25;
    const first = {
      event: "response.output_text.delta",
      data: JSON.stringify({ type: "response.output_text.delta", delta: "hello 🌍" }),
    };
    const terminal = {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        response: { id: "resp_1", status: "completed" },
      }),
    };

    await handle.acceptEvent(first);
    await handle.acceptEvent(terminal);
    handle.settle({ id: "resp_1", status: "completed" });
    handle.settle({ id: "ignored", status: "completed" });

    expect(await Array.fromAsync(handle.events())).toEqual([first, terminal]);
    await expect(handle.final).resolves.toEqual({ id: "resp_1", status: "completed" });
    expect(handle.stats).toEqual({
      bytes: new TextEncoder().encode(
        `event: ${first.event}\ndata: ${first.data}\n\n` +
        `event: ${terminal.event}\ndata: ${terminal.data}\n\n`,
      ).byteLength,
      firstByteMs: 25,
      frames: 2,
    });
  });

  it("reuses a transcript-proven wire size without re-materializing SSE bytes", async () => {
    const handle = new V2RunHandle({ maxBufferedBytes: 4_096 });
    const frame = { event: "delta", data: "hello" };
    const wireBytes = new TextEncoder().encode(
      `event: ${frame.event}\ndata: ${frame.data}\n\n`,
    ).byteLength;

    await handle.acceptEvent(frame, wireBytes);
    handle.settle({ id: "resp_1", status: "completed" });

    expect(await Array.fromAsync(handle.events())).toEqual([frame]);
    expect(handle.stats.bytes).toBe(wireBytes);
  });

  it("drains a long response in order through its head-index queue", async () => {
    const handle = new V2RunHandle({ maxBufferedBytes: 4 * 1024 * 1024 });
    const frames = Array.from({ length: 4_096 }, (_, index) => ({
      event: "delta",
      data: String(index),
    }));
    for (const frame of frames) await handle.acceptEvent(frame);
    handle.settle({ id: "resp_1", status: "completed" });

    const received = await Array.fromAsync(handle.events());
    expect(received).toEqual(frames);
  });

  it("backpressures the producer until buffered events are consumed", async () => {
    const handle = new V2RunHandle({ maxBufferedBytes: 160 });
    const first = { event: "delta", data: "a".repeat(100) };
    const second = { event: "delta", data: "b".repeat(100) };

    await handle.acceptEvent(first);
    const pending = handle.acceptEvent(second);
    expect(await promiseState(pending)).toBe("pending");

    const events = handle.events();
    await expect(events.next()).resolves.toEqual({ value: first, done: false });
    await expect(pending).resolves.toBeUndefined();
    handle.settle({ id: "resp_1", status: "completed" });
    await expect(events.next()).resolves.toEqual({ value: second, done: false });
    await expect(events.next()).resolves.toEqual({ value: undefined, done: true });
  });

  it("admits one logical event larger than the target buffer without deadlocking", async () => {
    const handle = new V2RunHandle({ maxBufferedBytes: 32 });
    const large = { event: "delta", data: "x".repeat(100) };

    await expect(handle.acceptEvent(large)).resolves.toBeUndefined();
    const blocked = handle.acceptEvent({ event: "delta", data: "next" });
    expect(await promiseState(blocked)).toBe("pending");
    const events = handle.events();
    await events.next();
    await expect(blocked).resolves.toBeUndefined();
    handle.settle({ id: "resp_1", status: "completed" });
  });

  it("rejects the final idempotently and closes blocked producers", async () => {
    const handle = new V2RunHandle({ maxBufferedBytes: 128 });
    await handle.acceptEvent({ event: "delta", data: "x".repeat(80) });
    const blocked = handle.acceptEvent({ event: "delta", data: "y".repeat(80) });
    const error = new V2CommittedError("upstream reset", "transport", true);

    handle.fail(error);
    handle.cancel(new Error("ignored"));

    await expect(handle.final).rejects.toBe(error);
    await expect(blocked).rejects.toBe(error);
    expect(await Array.fromAsync(handle.events())).toHaveLength(1);
  });
});
