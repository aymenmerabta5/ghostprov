import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  V2CommittedError,
  V2EventMemoryError,
  V2RunHandle,
} from "../src/v2/transport/run-handle";
import { sseFrame } from "../src/relay/sse";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  deriveV2HostEventMemorySizing,
  V2_EVENT_MAX_BUFFERED_EVENTS,
  V2_EVENT_SOFT_BUFFER_BYTES,
  v2EventPhysicalWorkingSetBytes,
  v2RetainedEventPermitBytes,
} from "../src/v2/memory/event-memory-sizing";
import {
  atomicBytePermitOf,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";

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
    const handle = new V2RunHandle({ maxBufferedBytes: V2_EVENT_SOFT_BUFFER_BYTES });
    const frames = Array.from({ length: 128 }, (_, index) => ({
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

  it("admits an event above the soft buffer only with transferred jumbo coverage", async () => {
    const handle = new V2RunHandle({
      maxBufferedBytes: 32,
      requireTransferredEventMemory: true,
    });
    const large = { event: "delta", data: "x".repeat(100) };
    const bytes = new TextEncoder().encode(sseFrame(large.event, large.data)).byteLength;
    const coverage = v2EventPhysicalWorkingSetBytes(bytes, 32);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: coverage,
      jumboBytes: coverage,
    });

    await expect(handle.acceptEvent(large, bytes)).rejects.toBeInstanceOf(V2EventMemoryError);
    const ordinary = await pool.acquire(coverage, { region: "ordinary" });
    await expect(handle.acceptEvent(large, bytes, ordinary.transfer("run-handle")))
      .rejects.toMatchObject({ code: "jumbo_required" });
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);

    const source = await pool.acquireJumbo(coverage);
    const transferred = source.transfer("run-handle");
    expect(source.active).toBe(false);
    await expect(handle.acceptEvent(large, bytes, transferred)).resolves.toBeUndefined();
    expect(pool.snapshot().jumboUsedBytes).toBe(coverage);
    const events = handle.events();
    await expect(events.next()).resolves.toEqual({ value: large, done: false });
    // Yield ownership remains charged until the consumer resumes or returns.
    expect(pool.snapshot().jumboUsedBytes).toBe(coverage);
    await events.return(undefined);
    expect(pool.snapshot().jumboUsedBytes).toBe(0);
    pool.close();
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
    expect(await Array.fromAsync(handle.events())).toHaveLength(0);
  });

  it("drains an accepted prefix before a definitive provider failure", async () => {
    const frame = {
      event: "response.output_text.delta",
      data: JSON.stringify({ type: "response.output_text.delta", delta: "valid prefix" }),
    };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const retained = v2RetainedEventPermitBytes(wireBytes);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: retained, jumboBytes: 1 });
    const handle = new V2RunHandle({ requireTransferredEventMemory: true });
    expect(handle.tryAcceptEvent(frame, wireBytes, pool.tryAcquire(retained)!)).toBe(true);
    const error = new V2CommittedError(
      "provider rate limit",
      "rate_limit_exceeded",
      false,
    );
    handle.fail(error);

    await expect(handle.final).rejects.toBe(error);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(retained);
    const events = handle.events();
    await expect(events.next()).resolves.toEqual({ value: frame, done: false });
    expect(pool.snapshot().ordinaryUsedBytes).toBe(retained);
    await expect(events.next()).resolves.toEqual({ value: undefined, done: true });
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("releases transferred permits for no-consumer disposal and cancellation", async () => {
    const frame = { event: "delta", data: "small" };
    const bytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const coverage = v2EventPhysicalWorkingSetBytes(bytes);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: coverage * 2, jumboBytes: 1 });

    const explicit = new V2RunHandle({});
    await explicit.acceptEvent(
      frame,
      bytes,
      (await pool.acquire(coverage)).transfer("explicit"),
    );
    explicit.discardUnconsumedEvents();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);

    const failed = new V2RunHandle({});
    await failed.acceptEvent(
      { ...frame },
      bytes,
      (await pool.acquire(coverage)).transfer("failed"),
    );
    failed.cancel(new Error("test cancellation"));
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("bounds the aggregate peak of sixteen slow consumers in one pool", () => {
    const frame = {
      event: "response.output_text.delta",
      data: JSON.stringify({ type: "response.output_text.delta", delta: "held" }),
    };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const coverage = v2EventPhysicalWorkingSetBytes(wireBytes);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: coverage * 16,
      jumboBytes: coverage,
    });
    const handles = Array.from({ length: 16 }, () => new V2RunHandle({
      maxBufferedBytes: 4_096,
      requireTransferredEventMemory: true,
    }));
    handles.forEach((handle, index) => {
      const permit = pool.tryAcquire(coverage, { label: `slow-${index}` })!;
      expect(handle.tryAcceptEvent({ ...frame }, wireBytes, permit)).toBe(true);
    });
    expect(pool.snapshot()).toMatchObject({
      activePermits: 16,
      ordinaryUsedBytes: coverage * 16,
    });
    for (const handle of handles) handle.discardUnconsumedEvents();
    expect(pool.snapshot()).toMatchObject({
      activePermits: 0,
      ordinaryUsedBytes: 0,
    });
  });

  it("keeps one jumbo terminal charge until event and final owners both release", async () => {
    const output = [{ type: "message", content: "x".repeat(1024 * 1024) }];
    const final = { id: "resp_shared", status: "completed", output };
    const frame = {
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response: final }),
    };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const retained = v2RetainedEventPermitBytes(wireBytes);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 1, jumboBytes: retained });
    const handle = new V2RunHandle({
      requireTransferredEventMemory: true,
    });

    expect(handle.tryAcceptEvent(
      frame,
      wireBytes,
      pool.tryAcquire(retained, { region: "jumbo" })!,
    )).toBe(true);
    handle.settle(final);
    expect(atomicBytePermitOf(final)).toMatchObject({
      active: true,
      bytes: retained,
      region: "jumbo",
    });

    const events = handle.events();
    await expect(events.next()).resolves.toEqual({ value: frame, done: false });
    expect(pool.snapshot()).toMatchObject({ activePermits: 1, jumboUsedBytes: retained });
    await events.return(undefined);
    // Returning the event iterator releases only the event/output reference.
    expect(pool.snapshot()).toMatchObject({ activePermits: 1, jumboUsedBytes: retained });
    releaseAtomicBytePermit(final);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, jumboUsedBytes: 0 });
    pool.close();
  });

  it("enforces the retained event-count boundary independently of bytes", async () => {
    const frame = { event: "delta", data: "x" };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const retained = v2RetainedEventPermitBytes(wireBytes);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: retained * 3, jumboBytes: 1 });
    const handle = new V2RunHandle({
      maxBufferedEvents: 2,
      requireTransferredEventMemory: true,
    });
    expect(handle.tryAcceptEvent({ ...frame }, wireBytes, pool.tryAcquire(retained)!)).toBe(true);
    expect(handle.tryAcceptEvent({ ...frame }, wireBytes, pool.tryAcquire(retained)!)).toBe(true);
    const third = pool.tryAcquire(retained)!;
    expect(handle.tryAcceptEvent({ ...frame }, wireBytes, third)).toBe(false);
    expect(third.active).toBe(true);

    const events = handle.events();
    await events.next();
    expect(handle.tryAcceptEvent({ ...frame }, wireBytes, third)).toBe(true);
    handle.settle({ id: "resp_count", status: "completed" });
    await events.return(undefined);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("fits sixteen full tiny-event queues inside derived ordinary capacity", () => {
    const MiB = 1024 * 1024;
    const sizing = deriveV2HostEventMemorySizing({
      runtimes: 1,
      maxActivePerRuntime: 16,
      responseMaxBytes: 64 * MiB,
    });
    const frame = { event: "delta", data: "x" };
    const wireBytes = new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
    const retained = v2RetainedEventPermitBytes(wireBytes);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: sizing.ordinaryBytes,
      jumboBytes: sizing.jumboBytes,
    });
    const handles = Array.from({ length: 16 }, () => new V2RunHandle({
      requireTransferredEventMemory: true,
    }));

    for (const handle of handles) {
      for (let index = 0; index < V2_EVENT_MAX_BUFFERED_EVENTS; index++) {
        const permit = pool.tryAcquire(retained);
        expect(permit).not.toBeNull();
        expect(handle.tryAcceptEvent({ ...frame }, wireBytes, permit!)).toBe(true);
      }
    }
    expect(pool.snapshot().ordinaryUsedBytes).toBeLessThanOrEqual(sizing.ordinaryBytes);
    for (const handle of handles) handle.discardUnconsumedEvents();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("rejects unsafe buffer enlargement and checked physical-size overflow", () => {
    expect(() => new V2RunHandle({
      maxBufferedBytes: V2_EVENT_SOFT_BUFFER_BYTES + 1,
    })).toThrow("maxBufferedBytes");
    expect(() => new V2RunHandle({
      maxBufferedEvents: V2_EVENT_MAX_BUFFERED_EVENTS + 1,
    })).toThrow("maxBufferedEvents");
    expect(() => v2EventPhysicalWorkingSetBytes(Number.MAX_SAFE_INTEGER))
      .toThrow("safe integer");
  });
});
