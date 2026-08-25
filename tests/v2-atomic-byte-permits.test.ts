import { describe, expect, it } from "bun:test";
import {
  AtomicBytePermitError,
  AtomicBytePermitPool,
} from "../src/v2/memory/atomic-byte-permits";
import { deriveV2SidecarEventMemorySizing } from "../src/v2/memory/event-memory-sizing";

describe("AtomicBytePermitPool", () => {
  it("partitions the default sidecar budget into one legal jumbo lane and ordinary remainder", () => {
    const MiB = 1024 * 1024;
    expect(deriveV2SidecarEventMemorySizing({
      totalBytes: 512 * MiB,
      responseMaxBytes: 64 * MiB,
    })).toEqual({
      ordinaryBytes: 255 * MiB,
      jumboBytes: 257 * MiB,
      totalBytes: 512 * MiB,
    });
    expect(() => deriveV2SidecarEventMemorySizing({
      totalBytes: 257 * MiB,
      responseMaxBytes: 64 * MiB,
    })).toThrow("must exceed");
  });

  it("serves ordinary waiters FIFO without letting a smaller request bypass", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 20 });
    const held = await pool.acquire(6);
    const order: string[] = [];
    const first = pool.acquire(8).then(permit => { order.push("first"); return permit; });
    const second = pool.acquire(2).then(permit => { order.push("second"); return permit; });
    await Bun.sleep(0);
    expect(pool.snapshot()).toMatchObject({ waiters: 2, ordinaryWaiters: 2 });
    expect(order).toEqual([]);

    held.release();
    const firstPermit = await first;
    await Bun.sleep(0);
    expect(order).toEqual(["first", "second"]);
    const secondPermit = await second;
    expect(pool.snapshot().ordinaryUsedBytes).toBe(10);
    firstPermit.release();
    secondPermit.release();
  });

  it("removes an aborted waiter and leaves the FIFO usable", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 4, jumboBytes: 8 });
    const held = await pool.acquire(4);
    const controller = new AbortController();
    const aborted = pool.acquire(4, { signal: controller.signal });
    controller.abort(new DOMException("test abort", "AbortError"));
    await expect(aborted).rejects.toMatchObject({ name: "AbortError" });
    expect(pool.snapshot().waiters).toBe(0);

    const next = pool.acquire(4);
    held.release();
    const permit = await next;
    permit.release();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
  });

  it("transfers ownership atomically and makes old/repeated release harmless", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 16, jumboBytes: 32 });
    const ingress = await pool.acquire(12, { label: "ingress" });
    const execution = ingress.transfer("execution");
    ingress.release();
    ingress.release();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(12);
    expect(execution.label).toBe("execution");

    execution.shrink(7);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(7);
    execution.release();
    execution.release();
    expect(pool.snapshot()).toMatchObject({ ordinaryUsedBytes: 0, activePermits: 0 });
  });

  it("atomically promotes N partially-grown permits without grow deadlock", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 16,
      jumboBytes: 16,
      maxWaiters: 16,
    });
    const permits = await Promise.all(Array.from({ length: 4 }, () => pool.acquire(4)));
    const completed: number[] = [];
    await Promise.race([
      Promise.all(permits.map(async (permit, index) => {
        await permit.resize(8);
        expect(permit.bytes).toBe(8);
        completed.push(index);
        permit.release();
      })),
      Bun.sleep(1_000).then(() => { throw new Error("partial promotion deadlocked"); }),
    ]);
    expect(completed.sort((a, b) => a - b)).toEqual([0, 1, 2, 3]);
    expect(pool.snapshot()).toMatchObject({
      activePermits: 0,
      waiters: 0,
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
    });
  });

  it("bounds waiters and keeps the one jumbo lane exclusive", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 1, jumboBytes: 8, maxWaiters: 1 });
    const ordinary = await pool.acquire(1);
    const jumbo = await pool.acquireJumbo(8);
    const queued = pool.acquireJumbo(1);
    await expect(pool.acquire(1)).rejects.toBeInstanceOf(AtomicBytePermitError);
    expect(pool.snapshot()).toMatchObject({ jumboActive: true, waiters: 1 });
    jumbo.release();
    const nextJumbo = await queued;
    expect(nextJumbo.region).toBe("jumbo");
    nextJumbo.release();
    ordinary.release();
  });

  it("keeps a failed or aborted promotion on its original ordinary charge", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8, jumboBytes: 8 });
    const occupied = await pool.acquireJumbo(8);
    const partial = await pool.acquire(4);
    const controller = new AbortController();
    const promotion = partial.promote(8, controller.signal);
    expect(() => partial.transfer("illegal-race")).toThrow(AtomicBytePermitError);
    controller.abort();
    await expect(promotion).rejects.toMatchObject({ name: "AbortError" });
    expect(partial).toMatchObject({ active: true, bytes: 4, region: "ordinary" });
    expect(pool.snapshot()).toMatchObject({ ordinaryUsedBytes: 4, jumboUsedBytes: 8 });
    occupied.release();
    partial.release();
  });

  it("never waits behind an acquisition while retaining the bytes that block it", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 4 });
    const holder = await pool.acquire(2);
    const older = pool.acquire(9);
    await holder.resize(3);
    expect(holder.region).toBe("jumbo");
    const acquired = await older;
    expect(acquired.region).toBe("ordinary");
    holder.release();
    acquired.release();

    const noBypass = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 20 });
    const retained = await noBypass.acquire(2);
    const queued = noBypass.acquire(9);
    await expect(retained.resize(3, { allowJumbo: false })).rejects.toMatchObject({
      code: "capacity",
    });
    expect(retained.bytes).toBe(2);
    expect(noBypass.snapshot()).toMatchObject({ waiters: 1, ordinaryUsedBytes: 2 });
    retained.release();
    (await queued).release();
  });

  it("provides no-queue try acquisition/resize/promotion with exact no-mutation failure", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 12 });
    const held = pool.tryAcquire(8, { label: "held" })!;
    const blocker = pool.tryAcquire(2)!;
    const before = pool.snapshot();
    expect(pool.tryAcquire(3)).toBeNull();
    expect(pool.snapshot()).toEqual(before);
    expect(held.tryResize(9, { allowJumbo: false })).toBe(false);
    expect(held).toMatchObject({ bytes: 8, region: "ordinary" });
    expect(pool.snapshot()).toEqual(before);

    expect(held.tryPromote(11)).toBe(true);
    expect(held).toMatchObject({ bytes: 11, region: "jumbo" });
    blocker.release();
    const ordinary = pool.tryAcquire(10)!;
    expect(pool.tryAcquire(1)).toBeNull();
    expect(ordinary.tryPromote(12)).toBe(false);
    expect(ordinary).toMatchObject({ bytes: 10, region: "ordinary" });
    ordinary.release();
    held.release();
  });

  it("does not let tryAcquire bypass an older FIFO waiter", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 10 });
    const held = await pool.acquire(6);
    const older = pool.acquire(8);
    expect(pool.tryAcquire(2)).toBeNull();
    expect(pool.snapshot()).toMatchObject({ ordinaryUsedBytes: 6, ordinaryWaiters: 1 });
    held.release();
    (await older).release();
  });

  it("prevents transfer for the full async operation, including immediate resize", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 10, jumboBytes: 10 });
    const permit = await pool.acquire(2);
    const resizing = permit.resize(3);
    expect(() => permit.transfer("racy-successor")).toThrow(AtomicBytePermitError);
    expect(() => permit.shrink(1)).toThrow(AtomicBytePermitError);
    await resizing;
    expect(permit.bytes).toBe(3);
    permit.release();
  });
});
