import { describe, expect, it } from "bun:test";
import {
  chooseRuntime,
  isRuntimeEligible,
  isRuntimeHealthy,
  type RouteCandidate,
} from "../src/router/policy";
import type { RandomSource } from "../src/core/ports";
import type { WorkloadDescriptor } from "../src/router/workload";
import { projectId } from "../src/sandbox/types";

const TEXT: WorkloadDescriptor = {
  class: "text-small",
  requestBytes: 1024,
  reservedBytes: 8 * 1024 ** 2,
  cpuPermits: 0,
  stream: true,
};

const RANDOM: RandomSource = { next: () => 0.25, uuid: () => "id" };

function candidate(
  suffix: string,
  values: Partial<RouteCandidate> = {},
): RouteCandidate {
  return {
    projectId: projectId(`00000000-0000-4000-8000-${suffix.padStart(12, "0")}`),
    state: "ready",
    capacity: 30,
    active: 0,
    reserved: 0,
    quarantined: 0,
    queueDepth: 0,
    ackEwmaMs: 1_000,
    completionEwmaMs: 10_000,
    recentFailureRate: 0,
    heartbeatAgeMs: 1_000,
    credentialExpiresInMs: 3_600_000,
    memoryPressure: 0,
    ...values,
  };
}

describe("chooseRuntime", () => {
  it("separates runtime health from immediate spare capacity", () => {
    expect(isRuntimeHealthy(candidate("1", { active: 30 }))).toBe(true);
    expect(isRuntimeEligible(candidate("1", { active: 30 }))).toBe(false);
    expect(isRuntimeEligible(candidate("1", { active: 28, reserved: 1 }))).toBe(true);
    expect(isRuntimeEligible(candidate("1", { capacity: 30, quarantined: 2, active: 28 }))).toBe(false);
    expect(isRuntimeHealthy(candidate("2", { heartbeatAgeMs: null }))).toBe(false);
    expect(isRuntimeHealthy(candidate("3", { heartbeatAgeMs: 20_001 }))).toBe(false);
    expect(isRuntimeHealthy(candidate("4", { credentialExpiresInMs: 29_999 }))).toBe(false);
  });

  it("chooses the lowest predicted completion time instead of round robin", () => {
    const decision = chooseRuntime(TEXT, [
      candidate("1", { active: 28, ackEwmaMs: 900 }),
      candidate("2", { active: 4, ackEwmaMs: 1_400 }),
      candidate("3", { active: 12, ackEwmaMs: 1_000 }),
    ], RANDOM);
    expect(decision?.projectId).toBe(candidate("2").projectId);
  });

  it("filters stale, draining, full, and credential-expiring runtimes", () => {
    const decision = chooseRuntime(TEXT, [
      candidate("1", { state: "draining" }),
      candidate("2", { heartbeatAgeMs: 25_000 }),
      candidate("3", { active: 30 }),
      candidate("4", { credentialExpiresInMs: 5_000 }),
      candidate("5", { active: 5 }),
    ], RANDOM);
    expect(decision?.projectId).toBe(candidate("5").projectId);
  });

  it("uses admission credits for eligibility without treating their cap as active work", () => {
    const idle = candidate("1", { capacity: 128, active: 0, available: 4 });
    const busy = candidate("2", { capacity: 128, active: 64, available: 4 });
    expect(isRuntimeEligible(candidate("3", {
      capacity: 128,
      active: 0,
      available: 0,
    }))).toBe(false);
    expect(chooseRuntime(TEXT, [busy, idle], RANDOM)?.projectId).toBe(idle.projectId);
  });

  it("subtracts only unreflected host reservations from advertised admission credits", () => {
    expect(isRuntimeEligible(candidate("1", {
      capacity: 90,
      active: 0,
      available: 4,
      reserved: 3,
    }))).toBe(true);
    expect(isRuntimeEligible(candidate("1", {
      capacity: 90,
      active: 0,
      available: 4,
      reserved: 4,
    }))).toBe(false);
  });

  it("uses deterministic randomized tie-breaking", () => {
    const left = candidate("1");
    const right = candidate("2");
    expect(chooseRuntime(TEXT, [left, right], { ...RANDOM, next: () => 0.1 })?.projectId).toBe(left.projectId);
    expect(chooseRuntime(TEXT, [left, right], { ...RANDOM, next: () => 0.9 })?.projectId).toBe(right.projectId);
  });

  it("penalizes measured commit and scheduler backlog monotonically", () => {
    const congested = candidate("1", {
      available: 4,
      commitBacklog: 3,
      schedulerBytes: 512 * 1024,
      schedulerOldestMs: 250,
      throughputBytesPerSecond: 1024 * 1024,
    });
    const clear = candidate("2", {
      available: 4,
      commitBacklog: 0,
      schedulerBytes: 0,
      schedulerOldestMs: 0,
      throughputBytesPerSecond: 1024 * 1024,
    });
    expect(chooseRuntime(TEXT, [congested, clear], RANDOM)?.projectId).toBe(clear.projectId);
  });

  it("uses measured publication throughput for a large request", () => {
    const large: WorkloadDescriptor = {
      ...TEXT,
      class: "text-large",
      requestBytes: 4 * 1024 ** 2,
    };
    const slow = candidate("1", {
      available: 4,
      throughputBytesPerSecond: 512 * 1024,
    });
    const fast = candidate("2", {
      available: 4,
      throughputBytesPerSecond: 8 * 1024 ** 2,
    });
    expect(chooseRuntime(large, [slow, fast], RANDOM)?.projectId).toBe(fast.projectId);
  });

  it("keeps 100,000 four-runtime decisions below the 2ms p95 budget", () => {
    const candidates = [candidate("1"), candidate("2"), candidate("3"), candidate("4")];
    const samples: number[] = [];
    for (let batch = 0; batch < 100; batch++) {
      const started = performance.now();
      for (let index = 0; index < 1_000; index++) chooseRuntime(TEXT, candidates, RANDOM);
      samples.push((performance.now() - started) / 1_000);
    }
    samples.sort((a, b) => a - b);
    expect(samples[Math.ceil(samples.length * 0.95) - 1]).toBeLessThan(2);
  });
});
