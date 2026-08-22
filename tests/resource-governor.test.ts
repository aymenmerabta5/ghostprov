import { describe, expect, it } from "bun:test";
import type { MachineProbe, MachineSample } from "../src/core/ports";
import {
  ResourceGovernor,
  SystemMachineProbe,
  deriveResourceSizing,
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

function workload(reservedBytes: number, cpuPermits: 0 | 1 = 0): WorkloadDescriptor {
  return {
    class: "text-large",
    requestBytes: 2 * MiB,
    reservedBytes,
    cpuPermits,
    stream: true,
    serializedBody: "{}",
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
});

describe("ResourceGovernor", () => {
  it("admits 120 text jobs but throttles maximum image-like reservations by bytes", () => {
    const probe = new MutableProbe(sample(32, 19.19));
    const governor = new ResourceGovernor({ probe });
    const textLeases = Array.from({ length: 120 }, () => governor.tryAcquire(workload(16 * MiB)));
    expect(textLeases.every(Boolean)).toBe(true);
    textLeases.forEach(lease => lease?.release());

    const imageLeases = Array.from({ length: 120 }, () => governor.tryAcquire(workload(152 * MiB, 1)));
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
    probe.value.freeBytes = gib(6);
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

  it("limits CPU transforms independently from request byte leases", async () => {
    const probe = new MutableProbe(sample(32, 19));
    const governor = new ResourceGovernor({ probe });
    const permits = await Promise.all(Array.from({ length: 6 }, () => governor.acquireCpu()));
    expect(governor.snapshot().cpuActive).toBe(6);
    expect(governor.tryAcquireCpu()).toBeNull();
    permits[0].release();
    expect(governor.tryAcquireCpu()).not.toBeNull();
    permits.slice(1).forEach(permit => permit.release());
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
});
