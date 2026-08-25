import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  AdmissionController,
  AdmissionError,
} from "../src/v2/sidecar/admission";

const MiB = 1024 * 1024;

class FakeClock implements Clock {
  value = 1_000;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadline: number): Promise<void> { this.value = Math.max(this.value, deadline); }
  advanceBy(ms: number): void { this.value += ms; }
}

describe("AdmissionController", () => {
  it("does not double-count bytes already held by the global atomic pool", async () => {
    const admission = new AdmissionController({
      physicalMax: 2,
      acceptedMax: 4,
      hostCeiling: 2,
      memoryBudgetBytes: 1,
    });
    const lease = await admission.acquire(64 * 1024 * 1024, undefined, true);
    expect(admission.snapshot()).toMatchObject({ active: 1, reservedBytes: 0 });
    lease.release();
    expect(admission.snapshot()).toMatchObject({ active: 0, reservedBytes: 0 });
  });

  it("caps active work at 128 and applies the lower host ceiling", async () => {
    const admission = new AdmissionController({
      physicalMax: 128,
      acceptedMax: 256,
      hostCeiling: 64,
      memoryBudgetBytes: 512 * MiB,
    });
    const leases = await Promise.all(Array.from(
      { length: 64 },
      () => admission.acquire(4 * MiB),
    ));

    expect(admission.snapshot()).toMatchObject({
      active: 64,
      effectiveLimit: 64,
      reservedBytes: 256 * MiB,
    });
    expect(admission.tryAcquire(4 * MiB)).toBeNull();
    leases.forEach(lease => lease.release());
    expect(admission.snapshot()).toMatchObject({ active: 0, reservedBytes: 0 });
  });

  it("throttles large requests by bytes before the count limit", () => {
    const admission = new AdmissionController({
      physicalMax: 128,
      acceptedMax: 256,
      hostCeiling: 128,
      memoryBudgetBytes: 128 * MiB,
    });
    const leases = Array.from(
      { length: 8 },
      () => admission.tryAcquire(32 * MiB),
    ).filter(value => value !== null);

    expect(leases).toHaveLength(4);
    expect(admission.snapshot().reservedBytes).toBe(128 * MiB);
    leases.forEach(lease => lease.release());
  });

  it("queues FIFO, wakes on release, and supports abort", async () => {
    const admission = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 3,
      hostCeiling: 1,
      memoryBudgetBytes: 12 * MiB,
    });
    const active = await admission.acquire(4 * MiB);
    const first = admission.acquire(4 * MiB);
    const abort = new AbortController();
    const second = admission.acquire(4 * MiB, abort.signal);
    abort.abort();
    await expect(second).rejects.toMatchObject({ name: "AbortError" });
    active.release();
    const admitted = await first;
    expect(admission.snapshot()).toMatchObject({ active: 1, queued: 0 });
    admitted.release();
  });

  it("counts queued reservations against the memory budget", async () => {
    const admission = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 4,
      hostCeiling: 1,
      memoryBudgetBytes: 8 * MiB,
    });
    const active = await admission.acquire(4 * MiB);
    const queued = admission.acquire(4 * MiB);

    await expect(admission.acquire(1)).rejects.toMatchObject({ code: "capacity" });
    expect(admission.snapshot()).toMatchObject({
      active: 1,
      queued: 1,
      reservedBytes: 8 * MiB,
    });
    active.release();
    (await queued).release();
  });

  it("rejects work beyond the bounded accepted queue", async () => {
    const admission = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 2,
      hostCeiling: 1,
      memoryBudgetBytes: 8 * MiB,
    });
    const active = await admission.acquire(4 * MiB);
    const queued = admission.acquire(4 * MiB);
    await expect(admission.acquire(4 * MiB)).rejects.toBeInstanceOf(AdmissionError);
    active.release();
    (await queued).release();
  });

  it("halves capacity after reset bursts and recovers one slot per 30 seconds", () => {
    const clock = new FakeClock();
    const admission = new AdmissionController({
      physicalMax: 128,
      acceptedMax: 256,
      hostCeiling: 128,
      memoryBudgetBytes: 512 * MiB,
      clock,
    });
    admission.recordProxyReset();
    admission.recordProxyReset();
    admission.recordProxyReset();
    expect(admission.snapshot().effectiveLimit).toBe(64);
    clock.advanceBy(30_000);
    admission.refresh();
    expect(admission.snapshot().effectiveLimit).toBe(65);
  });

  it("validates physical and memory limits", () => {
    expect(() => new AdmissionController({
      physicalMax: 129,
      acceptedMax: 256,
      hostCeiling: 128,
      memoryBudgetBytes: 512 * MiB,
    })).toThrow("128");
    const admission = new AdmissionController({
      physicalMax: 1,
      acceptedMax: 2,
      hostCeiling: 1,
      memoryBudgetBytes: 4 * MiB,
    });
    expect(() => admission.tryAcquire(5 * MiB)).toThrow(AdmissionError);
  });
});
