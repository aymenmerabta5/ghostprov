import { describe, expect, it } from "bun:test";
import type { MachineProbe, MachineSample } from "../src/core/ports";
import {
  ResourceGovernor,
  ResourceGovernorCapacityUnavailableError,
  ResourceGovernorClosedError,
  ResourceGovernorQueueFullError,
  ResourceGovernorReplacementOverloadError,
  ResourceGovernorTimeoutError,
  ResourceReservationTooLargeError,
  SystemMachineProbe,
  deriveResourceSizing,
  resolveEffectiveMemory,
} from "../src/router/resource-governor";
import type { WorkloadDescriptor } from "../src/router/workload";

const GiB = 1024 ** 3;
const MiB = 1024 ** 2;
const gib = (value: number) => value * GiB;

class MutableProbe implements MachineProbe {
  constructor(public value: MachineSample) {}
  sample(): MachineSample { return { ...this.value }; }
}

function sample(totalGiB: number, freeGiB: number, rssMiB = 320): MachineSample {
  return {
    totalBytes: gib(totalGiB),
    freeBytes: gib(freeGiB),
    rssBytes: rssMiB * MiB,
    heapBytes: 100 * MiB,
    externalBytes: 20 * MiB,
    logicalProcessors: 12,
  };
}

function workload(reservedBytes: number): WorkloadDescriptor {
  return {
    class: "text-large",
    requestBytes: 2 * MiB,
    reservedBytes,
    stream: true,
  };
}

describe("resource sizing", () => {
  it("reads a valid cross-platform machine sample", () => {
    const current = new SystemMachineProbe().sample();
    expect(current.totalBytes).toBeGreaterThan(0);
    expect(current.freeBytes).toBeGreaterThan(0);
    expect(current.rssBytes).toBeGreaterThan(0);
    expect(current.logicalProcessors).toBeGreaterThan(0);
  });

  it.each([
    [8, 5, 1],
    [16, 12, 8],
    [32, 19.19, 13],
    [64, 52, 38.4],
  ])("derives safe process ceilings for %i GiB", (total, free, expectedGiB) => {
    const sizing = deriveResourceSizing(sample(total, free));
    expect(sizing.processCeilingBytes / GiB).toBeCloseTo(expectedGiB, 0);
    expect(sizing.cpuTransformWorkers).toBe(6);
  });

  it("keeps pause and reserve thresholds reachable on small Linux-sized hosts", () => {
    const sizing = deriveResourceSizing(sample(2, 1.5, 128));
    expect(sizing.systemReserveBytes).toBe(gib(1));
    expect(sizing.pauseFreeBytes).toBe(gib(0.5));
    expect(sizing.resumeFreeBytes).toBeCloseTo(gib(0.8), 0);
    expect(sizing.resumeFreeBytes).toBeLessThan(sizing.totalBytes);
    expect(sizing.reservationCapacityBytes).toBeGreaterThan(0);
    expect(deriveResourceSizing({ ...sample(2, 1.5, 128), logicalProcessors: 1 })
      .cpuTransformWorkers).toBe(1);
  });

  it("uses constrained and available memory instead of host-wide Linux values", () => {
    expect(resolveEffectiveMemory({
      hostTotalBytes: gib(32),
      hostFreeBytes: gib(20),
      rssBytes: gib(1),
      constrainedBytes: gib(8),
      availableBytes: gib(3),
    })).toEqual({ totalBytes: gib(8), freeBytes: gib(3) });

    expect(resolveEffectiveMemory({
      hostTotalBytes: gib(32),
      hostFreeBytes: gib(20),
      rssBytes: gib(1),
      constrainedBytes: 0,
    })).toEqual({ totalBytes: gib(32), freeBytes: gib(20) });

    expect(resolveEffectiveMemory({
      hostTotalBytes: gib(32),
      hostFreeBytes: gib(20),
      rssBytes: gib(2),
      constrainedBytes: gib(8),
    })).toEqual({ totalBytes: gib(8), freeBytes: gib(6) });
  });
});

describe("ResourceGovernor", () => {
  it("accounts one fixed process pool and fails it closed without queueing", () => {
    const governor = new ResourceGovernor({ probe: new MutableProbe(sample(8, 5)) });
    expect(governor.snapshot()).not.toHaveProperty("cpuActive");
    expect(governor.snapshot()).not.toHaveProperty("cpuQueued");
    const fixed = governor.reserveFixedBytes(256 * MiB);
    expect(governor.snapshot()).toMatchObject({
      reservedBytes: 256 * MiB,
      queued: 0,
    });
    expect(() => governor.reserveFixedBytes(
      governor.snapshot().reservationCapacityBytes,
    )).toThrow(ResourceGovernorCapacityUnavailableError);
    fixed.release();
    expect(governor.snapshot().reservedBytes).toBe(0);
  });

  it("charges elastic process budgets only for their current retained bytes", () => {
    const governor = new ResourceGovernor({ probe: new MutableProbe(sample(32, 19)) });
    const fixed = governor.reserveFixedBytes(705 * MiB);
    const elastic = governor.createElasticReservation(2 * GiB);

    expect(elastic).toMatchObject({ maximumBytes: 2 * GiB, reservedBytes: 0 });
    expect(governor.snapshot().reservedBytes).toBe(705 * MiB);

    expect(elastic.tryResize(64 * MiB)).toBe(true);
    expect(elastic.reservedBytes).toBe(64 * MiB);
    expect(governor.snapshot().reservedBytes).toBe(769 * MiB);

    expect(elastic.tryResize(8 * MiB)).toBe(true);
    expect(governor.snapshot().reservedBytes).toBe(713 * MiB);
    expect(elastic.tryResize(0)).toBe(true);
    expect(governor.snapshot().reservedBytes).toBe(705 * MiB);

    elastic.close();
    elastic.close();
    fixed.release();
    expect(governor.snapshot().reservedBytes).toBe(0);
    governor.close();
  });

  it("uses bounded quanta without resizing global accounting for every small frame", () => {
    let samples = 0;
    const machine = sample(16, 12);
    const governor = new ResourceGovernor({
      probe: { sample: () => { samples++; return { ...machine }; } },
    });
    const elastic = governor.createElasticReservation(16 * MiB, { quantumBytes: MiB });

    expect(elastic.tryResize(1)).toBe(true);
    expect(elastic.reservedBytes).toBe(MiB);
    const afterFirstGrowth = samples;
    expect(elastic.tryResize(512 * 1024)).toBe(true);
    expect(elastic.reservedBytes).toBe(MiB);
    expect(samples).toBe(afterFirstGrowth);
    for (let frame = 0; frame < 10_000; frame++) {
      expect(elastic.tryResize(1 + frame % (512 * 1024))).toBe(true);
    }
    expect(samples).toBe(afterFirstGrowth);
    expect(elastic.tryResize(MiB + 1)).toBe(true);
    expect(elastic.reservedBytes).toBe(2 * MiB);
    expect(samples).toBe(afterFirstGrowth + 1);
    expect(elastic.tryResize(MiB)).toBe(true);
    expect(elastic.reservedBytes).toBe(2 * MiB);
    expect(samples).toBe(afterFirstGrowth + 1);
    expect(elastic.tryResize(0)).toBe(true);
    expect(elastic.reservedBytes).toBe(0);

    elastic.close();
    governor.close();
  });

  it("leaves an elastic reservation unchanged when pressure blocks growth", () => {
    const probe = new MutableProbe(sample(32, 19));
    const governor = new ResourceGovernor({ probe });
    const elastic = governor.createElasticReservation(2 * GiB);
    expect(elastic.tryResize(64 * MiB)).toBe(true);

    probe.value.freeBytes = gib(6);
    expect(elastic.tryResize(128 * MiB)).toBe(false);
    expect(elastic.reservedBytes).toBe(64 * MiB);
    expect(governor.snapshot().reservedBytes).toBe(64 * MiB);

    expect(elastic.tryResize(0)).toBe(true);
    elastic.close();
    governor.close();
  });

  it("does not let elastic process budgets bypass an older request waiter", async () => {
    const governor = new ResourceGovernor({ probe: new MutableProbe(sample(16, 12)) });
    const elastic = governor.createElasticReservation(MiB);
    const capacity = governor.snapshot().reservationCapacityBytes;
    const blocker = governor.tryAcquire(workload(capacity - MiB))!;
    const abort = new AbortController();
    const waiting = governor.acquire(workload(2 * MiB), abort.signal);
    expect(governor.snapshot().queued).toBe(1);

    expect(elastic.tryResize(512 * 1024)).toBe(false);
    expect(elastic.reservedBytes).toBe(0);
    abort.abort();
    await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
    expect(elastic.tryResize(512 * 1024)).toBe(true);

    elastic.close();
    blocker.release();
    governor.close();
  });

  it("admits 120 text jobs but throttles maximum image-like reservations by bytes", () => {
    const probe = new MutableProbe(sample(32, 19.19));
    const governor = new ResourceGovernor({ probe });
    const textLeases = Array.from({ length: 120 }, () => governor.tryAcquire(workload(16 * MiB)));
    expect(textLeases.every(Boolean)).toBe(true);
    textLeases.forEach(lease => lease?.release());

    const imageLeases = Array.from({ length: 120 }, () => governor.tryAcquire(workload(152 * MiB)));
    const admitted = imageLeases.filter(Boolean);
    expect(admitted.length).toBeGreaterThan(16);
    expect(admitted.length).toBeLessThan(120);
    admitted.forEach(lease => lease?.release());
  });

  it("pauses under free-memory pressure and resumes with hysteresis", () => {
    const probe = new MutableProbe(sample(32, 19));
    const governor = new ResourceGovernor({ probe });
    probe.value.freeBytes = gib(2);
    expect(governor.tryAcquire(workload(MiB))).toBeNull();
    expect(governor.snapshot().paused).toBe(true);
    probe.value.freeBytes = gib(8);
    governor.refreshPressure();
    expect(governor.snapshot().paused).toBe(false);
    expect(governor.tryAcquire(workload(MiB))).not.toBeNull();
  });

  it("queues byte reservations, supports abort, and releases idempotently", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const capacity = governor.snapshot().reservationCapacityBytes;
    const first = governor.tryAcquire(workload(capacity))!;
    const abort = new AbortController();
    const waiting = governor.acquire(workload(MiB), abort.signal);
    abort.abort();
    await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
    first.release();
    first.release();
    expect(governor.snapshot().reservedBytes).toBe(0);
  });

  it("advertises a bounded V2 capacity from remaining byte reservations", () => {
    const probe = new MutableProbe(sample(32, 19.19));
    const governor = new ResourceGovernor({ probe });
    const perRequest = 16 * MiB;
    expect(governor.capacityHint(perRequest, 128)).toBe(128);

    const leases = Array.from({ length: 100 }, () => governor.tryAcquire(workload(perRequest))!);
    const remaining = governor.snapshot().reservationCapacityBytes -
      governor.snapshot().reservedBytes;
    expect(governor.capacityHint(perRequest, 128)).toBe(
      Math.min(128, Math.floor(remaining / perRequest)),
    );

    probe.value.freeBytes = gib(2);
    expect(governor.capacityHint(perRequest, 128)).toBe(0);
    leases.forEach(lease => lease.release());
    expect(() => governor.capacityHint(0, 128)).toThrow(RangeError);
    expect(() => governor.capacityHint(perRequest, -1)).toThrow(RangeError);
  });

  it("rejects reservations which can never fit instead of queueing them", async () => {
    const governor = new ResourceGovernor({ probe: new MutableProbe(sample(8, 5)) });
    const impossible = governor.snapshot().maximumReservationCapacityBytes! + 1;

    const failure = await governor.acquire(workload(impossible)).catch(error => error);
    expect(failure).toBeInstanceOf(ResourceReservationTooLargeError);
    expect(failure).toMatchObject({ status: 413, code: "resource_reservation_too_large" });
    expect(governor.tryAcquire(workload(impossible))).toBeNull();
    expect(governor.snapshot().queued).toBe(0);
  });

  it("lowers idle capacity when live RSS rises and free memory falls", () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const initialCapacity = governor.snapshot().reservationCapacityBytes;
    probe.value.rssBytes = gib(4);
    probe.value.freeBytes = gib(4.1);

    expect(governor.tryAcquire(workload(initialCapacity))).toBeNull();
    expect(governor.snapshot().reservationCapacityBytes).toBeLessThan(initialCapacity);
  });

  it("does not subtract a materialized reservation from both RSS and logical capacity", () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const lease = governor.tryAcquire(workload(512 * MiB))!;
    const before = governor.snapshot().reservationCapacityBytes -
      governor.snapshot().reservedBytes;

    probe.value.rssBytes += 512 * MiB;
    probe.value.freeBytes -= 512 * MiB;
    governor.refreshPressure();
    const after = governor.snapshot().reservationCapacityBytes -
      governor.snapshot().reservedBytes;

    expect(after).toBe(before);
    lease.release();
  });

  it("queues structurally valid work when transient RSS consumes all live headroom", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const hardMaximum = governor.snapshot().maximumReservationCapacityBytes!;
    expect(hardMaximum).toBe(Math.floor(probe.value.totalBytes * 0.60));
    probe.value.rssBytes = gib(5);
    probe.value.freeBytes = gib(3);
    const abort = new AbortController();

    const waiting = governor.acquire(workload(MiB), abort.signal);
    expect(governor.snapshot()).toMatchObject({
      reservationCapacityBytes: 0,
      maximumReservationCapacityBytes: hardMaximum,
      queued: 1,
    });
    abort.abort();
    await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
  });

  it("bounds queued jobs and queued reservation bytes", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const capacity = deriveResourceSizing(probe.sample()).reservationCapacityBytes;
    const governor = new ResourceGovernor({
      probe,
      maxByteWaiters: 1,
      maxQueuedReservationBytes: capacity,
    });
    const active = governor.tryAcquire(workload(capacity))!;
    const queued = governor.acquire(workload(MiB)).catch(error => error);

    await expect(governor.acquire(workload(MiB))).rejects.toBeInstanceOf(
      ResourceGovernorQueueFullError,
    );
    expect(governor.snapshot()).toMatchObject({
      queued: 1,
      queuedReservationBytes: MiB,
      queueCapacity: 1,
    });

    governor.close();
    expect(await queued).toBeInstanceOf(ResourceGovernorClosedError);
    active.release();
  });

  it("purges expired waiters before applying queue bounds", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const capacity = deriveResourceSizing(probe.sample()).reservationCapacityBytes;
    let now = 0;
    const governor = new ResourceGovernor({
      probe,
      maxByteWaiters: 1,
      maxQueuedReservationBytes: capacity,
      waiterTimeoutMs: 1_000,
      now: () => now,
    });
    const active = governor.tryAcquire(workload(capacity))!;
    const expired = governor.acquire(workload(MiB)).catch(error => error);
    now = 1_001;
    const replacement = governor.acquire(workload(MiB));

    expect(await expired).toBeInstanceOf(ResourceGovernorTimeoutError);
    expect(governor.snapshot().queued).toBe(1);
    active.release();
    const replacementLease = await replacement;
    replacementLease.release();
  });

  it("expires queued acquisition without requiring a lease release", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe, waiterTimeoutMs: 10 });
    const active = governor.tryAcquire(workload(governor.snapshot().reservationCapacityBytes))!;

    await expect(governor.acquire(workload(MiB))).rejects.toBeInstanceOf(
      ResourceGovernorTimeoutError,
    );
    expect(governor.snapshot().queued).toBe(0);
    active.release();
  });

  it("skips a non-fitting head deterministically without letting tryAcquire bypass waiters", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const capacity = governor.snapshot().reservationCapacityBytes;
    const active = governor.tryAcquire(workload(capacity - 64 * MiB))!;
    const large = governor.acquire(workload(128 * MiB));
    const small = await governor.acquire(workload(32 * MiB));

    expect(governor.snapshot()).toMatchObject({ queued: 1 });
    expect(governor.tryAcquire(workload(MiB))).toBeNull();

    small.release();
    active.release();
    const largeLease = await large;
    largeLease.release();
  });

  it("bounds small-job bypasses so an older large waiter can accumulate headroom", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const capacity = governor.snapshot().reservationCapacityBytes;
    const blocker = governor.tryAcquire(workload(capacity - 2 * MiB))!;
    const large = governor.acquire(workload(3 * MiB));

    for (let index = 0; index < 8; index++) {
      const small = await governor.acquire(workload(MiB));
      small.release();
    }
    const protectedSmall = governor.acquire(workload(MiB));
    await Bun.sleep(0);
    expect(governor.snapshot().queued).toBe(2);

    blocker.release();
    const largeLease = await large;
    const smallLease = await protectedSmall;
    largeLease.release();
    smallLease.release();
  });

  it("ratchets aged-waiter headroom so repeated small releases cannot recycle it", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const capacity = governor.snapshot().reservationCapacityBytes;
    const blocker = governor.tryAcquire(workload(capacity - 6 * MiB))!;
    const firstGrowth = governor.tryAcquire(workload(2 * MiB))!;
    const secondGrowth = governor.tryAcquire(workload(2 * MiB))!;
    const large = governor.acquire(workload(6 * MiB));

    for (let index = 0; index < 8; index++) {
      const small = await governor.acquire(workload(MiB));
      small.release();
    }
    let recycled = 0;
    const followers = Array.from({ length: 4 }, () => (
      governor.acquire(workload(MiB)).then(lease => {
        recycled++;
        lease.release();
      })
    ));
    await Bun.sleep(0);
    expect(recycled).toBe(0);

    // The newly freed bytes must raise the aged waiter's protected floor.
    // Otherwise each resolving follower can release and recycle the same MiB,
    // permanently keeping the 6 MiB request below its required headroom.
    firstGrowth.release();
    await Bun.sleep(0);
    expect(recycled).toBe(0);

    secondGrowth.release();
    const largeLease = await large;
    largeLease.release();
    await Promise.all(followers);
    blocker.release();
  });

  it("self-wakes after host memory pressure recovers", async () => {
    const probe = new MutableProbe(sample(32, 19));
    const governor = new ResourceGovernor({ probe, pressurePollMs: 5 });
    probe.value.freeBytes = gib(2);
    const waiting = governor.acquire(workload(MiB));
    expect(governor.snapshot()).toMatchObject({ paused: true, queued: 1 });

    probe.value.freeBytes = gib(8);
    const lease = await waiting;
    expect(governor.snapshot().paused).toBe(false);
    lease.release();
  });

  it("recovers capacity when constructed during transient startup pressure", async () => {
    const probe = new MutableProbe(sample(8, 3));
    const governor = new ResourceGovernor({ probe, pressurePollMs: 5 });
    expect(governor.snapshot().reservationCapacityBytes).toBe(0);
    const waiting = governor.acquire(workload(MiB));

    probe.value.freeBytes = gib(6);
    const lease = await waiting;
    expect(governor.snapshot().reservationCapacityBytes).toBeGreaterThan(0);
    lease.release();
  });

  it("fails replacement in place without releasing the old reservation", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const capacity = deriveResourceSizing(probe.sample()).reservationCapacityBytes;
    const governor = new ResourceGovernor({
      probe,
      maxQueuedReservationBytes: capacity,
    });
    const firstBytes = Math.floor(capacity / 2);
    const replacementBytes = Math.floor(capacity * 0.75);
    const first = governor.tryAcquire(workload(firstBytes))!;
    const other = governor.tryAcquire(workload(capacity - firstBytes))!;
    await expect(first.replace(workload(replacementBytes))).rejects.toBeInstanceOf(
      ResourceGovernorReplacementOverloadError,
    );
    expect(first.reservedBytes).toBe(firstBytes);
    expect(governor.snapshot()).toMatchObject({
      reservedBytes: capacity,
      queued: 0,
      queuedReservationBytes: 0,
    });
    other.release();

    const replacementLease = await first.replace(workload(replacementBytes));
    expect(first.reservedBytes).toBe(0);
    expect(replacementLease.reservedBytes).toBe(replacementBytes);
    first.release();
    replacementLease.release();
  });

  it("allows an in-place downsize while pressure-paused", async () => {
    const probe = new MutableProbe(sample(8, 5));
    const governor = new ResourceGovernor({ probe });
    const original = governor.tryAcquire(workload(128 * MiB))!;
    probe.value.freeBytes = gib(1);
    governor.refreshPressure();
    expect(governor.snapshot().paused).toBe(true);

    const replacement = await original.replace(workload(64 * MiB));
    expect(original.reservedBytes).toBe(0);
    expect(governor.snapshot().reservedBytes).toBe(64 * MiB);
    replacement.release();
  });

  it("keeps replacement failure-atomic when a later machine probe would throw", async () => {
    let calls = 0;
    let throwAfter = Number.POSITIVE_INFINITY;
    const probe: MachineProbe = {
      sample() {
        calls++;
        if (calls >= throwAfter) throw new Error("probe failed after replacement swap");
        return sample(8, 5);
      },
    };
    const governor = new ResourceGovernor({ probe });
    const original = governor.tryAcquire(workload(128 * MiB))!;
    throwAfter = calls + 2;

    const replacement = await original.replace(workload(64 * MiB));
    expect(calls).toBe(throwAfter - 1);
    expect(original.reservedBytes).toBe(0);
    expect(replacement.reservedBytes).toBe(64 * MiB);
    expect(governor.snapshot().reservedBytes).toBe(64 * MiB);
    throwAfter = Number.POSITIVE_INFINITY;
    replacement.release();
    expect(governor.snapshot().reservedBytes).toBe(0);
  });

  it("closes the byte queue and rejects future acquisition", async () => {
    const probe = new MutableProbe(sample(32, 19));
    const governor = new ResourceGovernor({ probe });
    const byteLease = governor.tryAcquire(workload(governor.snapshot().reservationCapacityBytes))!;
    const byteWaiter = governor.acquire(workload(MiB)).catch(error => error);

    governor.close();
    expect(await byteWaiter).toBeInstanceOf(ResourceGovernorClosedError);
    await expect(governor.acquire(workload(MiB))).rejects.toBeInstanceOf(
      ResourceGovernorClosedError,
    );
    expect(governor.snapshot()).toMatchObject({ closed: true, queued: 0 });

    byteLease.release();
  });
});
