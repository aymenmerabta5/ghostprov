import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  FleetRecoveryCoordinator,
  classifyFleetRecoveryFailure,
  fleetRecoveryBackoffMs,
  type FleetRecoveryLease,
  type FleetRecoverySnapshot,
} from "../src/prism/recovery-coordinator";

interface ClockWaiter {
  deadline: number;
  signal?: AbortSignal;
  resolve(): void;
  reject(error: unknown): void;
  onAbort?: () => void;
}

class ManualClock implements Clock {
  value = 1_000;
  readonly waiters: ClockWaiter[] = [];

  now(): number { return this.value; }

  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return this.waitUntil(this.value + ms, signal);
  }

  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const waiter: ClockWaiter = { deadline, signal, resolve, reject };
      if (signal) {
        waiter.onAbort = () => {
          this.#remove(waiter);
          reject(new DOMException("aborted", "AbortError"));
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
    });
  }

  advanceBy(ms: number): void {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.deadline > this.value) continue;
      this.#remove(waiter);
      waiter.resolve();
    }
  }

  pendingTimers(): number { return this.waiters.length; }

  #remove(waiter: ClockWaiter): void {
    const index = this.waiters.indexOf(waiter);
    if (index >= 0) this.waiters.splice(index, 1);
    if (waiter.onAbort) waiter.signal?.removeEventListener("abort", waiter.onAbort);
  }
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

function observe<T>(promise: Promise<T>): { settled: boolean; value?: T; error?: unknown } {
  const result: { settled: boolean; value?: T; error?: unknown } = { settled: false };
  void promise.then(
    value => {
      result.settled = true;
      result.value = value;
    },
    error => {
      result.settled = true;
      result.error = error;
    },
  );
  return result;
}

function outage(status = 503, retryAfterMs?: number): Record<string, unknown> {
  return {
    name: "ProvisioningError",
    kind: status === 429 ? "rate_limit" : "transport",
    status,
    retryAfterMs,
  };
}

describe("FleetRecoveryCoordinator classification", () => {
  it("classifies only known fleet-wide failures and gives local statuses priority", () => {
    expect(classifyFleetRecoveryFailure({ status: 404, kind: "transport" })).toEqual({
      kind: "local",
      disposition: "local",
      retryAfterMs: null,
    });
    expect(classifyFleetRecoveryFailure({ failure: { status: 403 } })).toEqual({
      kind: "auth",
      disposition: "auth",
      retryAfterMs: null,
    });
    expect(classifyFleetRecoveryFailure({ status: 429, retryAfterMs: 999_999 })).toEqual({
      kind: "rate_limit",
      disposition: "outage",
      retryAfterMs: 300_000,
    });
    expect(classifyFleetRecoveryFailure({ status: 502 })).toMatchObject({
      kind: "server",
      disposition: "outage",
    });
    expect(classifyFleetRecoveryFailure({ name: "AccountTransportError" })).toMatchObject({
      kind: "account_transport",
      disposition: "outage",
    });
    expect(classifyFleetRecoveryFailure({
      name: "ProvisioningError",
      kind: "timeout",
    })).toMatchObject({
      kind: "provision_timeout",
      disposition: "outage",
    });
    expect(classifyFleetRecoveryFailure({
      name: "ProvisioningError",
      kind: "transport",
    })).toMatchObject({
      kind: "provision_transport",
      disposition: "outage",
    });
    expect(classifyFleetRecoveryFailure({
      name: "ProvisioningError",
      kind: "protocol",
    })).toMatchObject({ kind: "other", disposition: "local" });
    expect(classifyFleetRecoveryFailure({ kind: "transport" })).toMatchObject({
      kind: "other",
      disposition: "local",
    });
  });

  it("uses capped exponential jitter", () => {
    expect(fleetRecoveryBackoffMs(1, () => 0)).toBe(1_000);
    expect(fleetRecoveryBackoffMs(2, () => 0)).toBe(2_000);
    expect(fleetRecoveryBackoffMs(3, () => 0.5)).toBe(4_400);
    expect(fleetRecoveryBackoffMs(99, () => 1)).toBe(60_000);
    expect(() => fleetRecoveryBackoffMs(1, () => 0, 2_000, 1_000)).toThrow();
  });
});

describe("FleetRecoveryCoordinator", () => {
  it("admits one outage probe at a time, backs off forever, and wakes all waiters on health", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const firstPending = coordinator.enter(outage());
    const secondPending = coordinator.enter(outage());
    const thirdPending = coordinator.enter(outage());
    const second = observe(secondPending);
    const third = observe(thirdPending);

    const first = await firstPending;
    expect(first.probe).toBe(true);
    expect(second.settled).toBe(false);
    expect(third.settled).toBe(false);
    expect(coordinator.snapshot()).toMatchObject({
      state: "half_open",
      scope: "outage",
      queued: 2,
      probeInFlight: true,
    });

    first.fail(outage());
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      consecutiveFailures: 1,
      nextProbeAtMs: 2_000,
      lastFailureKind: "server",
    });
    clock.advanceBy(999);
    await flush();
    expect(second.settled).toBe(false);
    clock.advanceBy(1);
    await flush();
    expect(second.settled).toBe(true);
    const secondLease = second.value!;
    expect(secondLease.probe).toBe(true);

    secondLease.fail(outage());
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      consecutiveFailures: 2,
      nextProbeAtMs: 4_000,
      queued: 1,
    });
    clock.advanceBy(2_000);
    await flush();
    expect(third.settled).toBe(true);
    const thirdLease = third.value!;
    expect(thirdLease.probe).toBe(true);

    const fourthPending = coordinator.enter(outage());
    const fourth = observe(fourthPending);
    thirdLease.succeed();
    await flush();
    expect(fourth.settled).toBe(true);
    expect(fourth.value?.probe).toBe(false);
    expect(coordinator.snapshot()).toEqual({
      state: "closed",
      scope: null,
      consecutiveFailures: 0,
      queued: 0,
      probeInFlight: false,
      revalidationInFlight: false,
      nextProbeAtMs: null,
      lastFailureKind: null,
    });
    fourth.value?.cancel();
    coordinator.close();
  });

  it("honors a safe Retry-After up to five minutes", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const first = await coordinator.enter(outage(429));
    const nextPending = coordinator.enter(outage(429));
    const next = observe(nextPending);

    first.fail(outage(429, 900_000));
    expect(coordinator.snapshot().nextProbeAtMs).toBe(301_000);
    clock.advanceBy(299_999);
    await flush();
    expect(next.settled).toBe(false);
    clock.advanceBy(1);
    await flush();
    expect(next.value?.probe).toBe(true);
    next.value?.cancel();
    coordinator.close();
  });

  it("singleflights account revalidation and admits exactly one auth probe", async () => {
    const clock = new ManualClock();
    const gate = deferred<void>();
    let revalidations = 0;
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidateAccount: async () => {
        revalidations++;
        await gate.promise;
      },
    });

    const onePending = coordinator.enter({ status: 401 });
    const twoPending = coordinator.enter({ failure: { kind: "auth" } });
    const threePending = coordinator.enter({ reason: "auth-required" });
    const one = observe(onePending);
    const two = observe(twoPending);
    const three = observe(threePending);
    await flush();
    expect(revalidations).toBe(1);
    expect(coordinator.snapshot()).toMatchObject({
      state: "revalidating",
      scope: "auth",
      queued: 3,
      revalidationInFlight: true,
      probeInFlight: false,
    });
    const localDuringRevalidation = await coordinator.enter({ status: 404 });

    gate.resolve();
    await flush();
    expect(one.settled).toBe(true);
    expect(one.value?.probe).toBe(true);
    expect(two.settled).toBe(false);
    expect(three.settled).toBe(false);
    expect(coordinator.snapshot()).toMatchObject({
      state: "half_open",
      queued: 2,
      probeInFlight: true,
      revalidationInFlight: false,
    });

    localDuringRevalidation.succeed();
    expect(coordinator.snapshot()).toMatchObject({
      state: "half_open",
      scope: "auth",
      queued: 2,
      probeInFlight: true,
    });

    one.value?.succeed();
    await flush();
    expect(two.value?.probe).toBe(false);
    expect(three.value?.probe).toBe(false);
    two.value?.cancel();
    three.value?.cancel();
    coordinator.close();
  });

  it("retries failed account revalidation after the shared backoff", async () => {
    const clock = new ManualClock();
    let revalidations = 0;
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidateAccount: () => {
        revalidations++;
        if (revalidations === 1) {
          throw Object.assign(new Error("offline"), { name: "AccountTransportError" });
        }
      },
    });
    const pending = coordinator.enter({ status: 403 });
    const result = observe(pending);
    await flush();
    expect(revalidations).toBe(1);
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      scope: "auth",
      consecutiveFailures: 1,
      nextProbeAtMs: 2_000,
      lastFailureKind: "account_transport",
    });
    clock.advanceBy(999);
    await flush();
    expect(revalidations).toBe(1);
    clock.advanceBy(1);
    await flush();
    expect(revalidations).toBe(2);
    expect(result.value?.probe).toBe(true);
    result.value?.succeed();
    coordinator.close();
  });

  it("times out a hung account revalidation and opens the shared auth circuit", async () => {
    const clock = new ManualClock();
    const revalidationSignal: { current?: AbortSignal } = {};
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidationTimeoutMs: 10_000,
      revalidateAccount: signal => {
        revalidationSignal.current = signal;
        return new Promise<void>(() => undefined);
      },
    });
    const waiting = observe(coordinator.enter({ status: 401 }));
    await flush();
    expect(coordinator.snapshot().state).toBe("revalidating");

    clock.advanceBy(10_000);
    await flush();

    expect(revalidationSignal.current?.aborted).toBe(true);
    expect(waiting.settled).toBe(false);
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      scope: "auth",
      consecutiveFailures: 1,
      nextProbeAtMs: clock.now() + 1_000,
    });
    coordinator.close();
  });

  it("lets local sandbox failures bypass an open circuit without mutating it", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const probe = await coordinator.enter(outage());
    const queuedPending = coordinator.enter(outage());
    probe.fail(outage());
    const before = coordinator.snapshot();

    for (const status of [404, 409, 410]) {
      const local = await coordinator.enter({ status });
      expect(local.probe).toBe(false);
      local.fail({ status });
      expect(coordinator.snapshot()).toEqual(before);
    }

    const healthyLocal = await coordinator.enter({ status: 404 });
    healthyLocal.succeed();
    const queued = await queuedPending;
    expect(queued.probe).toBe(false);
    expect(coordinator.snapshot().state).toBe("closed");
    queued.cancel();
    coordinator.close();
  });

  it("removes aborted waiters and automatically releases an aborted probe", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const probeAbort = new AbortController();
    const first = await coordinator.enter(outage(), probeAbort.signal);
    const waitingAbort = new AbortController();
    const abortedPending = coordinator.enter(outage(), waitingAbort.signal);
    const nextPending = coordinator.enter(outage());
    waitingAbort.abort();
    await expect(abortedPending).rejects.toMatchObject({ name: "AbortError" });
    expect(coordinator.snapshot().queued).toBe(1);

    probeAbort.abort();
    const next = await nextPending;
    expect(first.probe).toBe(true);
    expect(next.probe).toBe(true);
    expect(coordinator.snapshot().queued).toBe(0);
    next.cancel();
    coordinator.close();
  });

  it("drops the shared timer when the last open-circuit waiter aborts", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const first = await coordinator.enter(outage());
    const abort = new AbortController();
    const waiting = coordinator.enter(outage(), abort.signal);
    first.fail(outage());
    expect(clock.pendingTimers()).toBe(1);
    abort.abort();
    await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
    expect(clock.pendingTimers()).toBe(0);
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      queued: 0,
      nextProbeAtMs: 2_000,
    });

    const later = observe(coordinator.enter(outage()));
    expect(clock.pendingTimers()).toBe(1);
    clock.advanceBy(1_000);
    await flush();
    expect(later.value?.probe).toBe(true);
    later.value?.cancel();
    coordinator.close();
  });

  it("does not strand waiters when an injected clock rejects unexpectedly", async () => {
    const brokenClock: Clock = {
      now: () => 1_000,
      sleep: () => Promise.reject(new Error("clock failed")),
      waitUntil: () => Promise.reject(new Error("clock failed")),
    };
    const coordinator = new FleetRecoveryCoordinator({
      clock: brokenClock,
      random: { next: () => 0 },
    });
    const first = await coordinator.enter(outage());
    const nextPending = coordinator.enter(outage());
    const next = observe(nextPending);
    first.fail(outage());
    await flush();
    expect(next.value?.probe).toBe(true);
    expect(coordinator.snapshot()).toMatchObject({
      state: "half_open",
      queued: 0,
      probeInFlight: true,
    });
    next.value?.cancel();
    coordinator.close();
  });

  it("fences stale failures after a healthy child closes the circuit", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const staleLocal = await coordinator.enter({ status: 404 });
    const probe = await coordinator.enter(outage());
    probe.succeed();
    staleLocal.fail(outage());
    expect(coordinator.snapshot()).toMatchObject({
      state: "closed",
      consecutiveFailures: 0,
      lastFailureKind: null,
    });
    coordinator.close();
  });

  it("does not let a pre-outage local success close a newer open circuit", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const staleLocal = await coordinator.enter({ status: 404 });
    const probe = await coordinator.enter(outage());
    probe.fail(outage());
    staleLocal.succeed();
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      consecutiveFailures: 1,
      lastFailureKind: "server",
    });
    coordinator.close();
  });

  it("keeps the probe slot while a local bypass discovers a global outage", async () => {
    const clock = new ManualClock();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
    });
    const oldProbe = await coordinator.enter(outage());
    const local = await coordinator.enter({ status: 404 });
    const queuedPending = coordinator.enter(outage());
    const queued = observe(queuedPending);

    local.fail(outage());
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      probeInFlight: true,
      queued: 1,
      nextProbeAtMs: 2_000,
    });
    clock.advanceBy(500);
    await flush();
    expect(queued.settled).toBe(false);

    oldProbe.cancel();
    await flush();
    expect(queued.settled).toBe(false);
    expect(coordinator.snapshot().nextProbeAtMs).toBe(2_000);
    clock.advanceBy(500);
    await flush();
    expect(queued.value?.probe).toBe(true);
    queued.value?.cancel();
    coordinator.close();
  });

  it("requires auth revalidation even when an older local child becomes healthy", async () => {
    const clock = new ManualClock();
    const gate = deferred<void>();
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidateAccount: () => gate.promise,
    });
    const staleLocal = await coordinator.enter({ status: 404 });
    const authPending = coordinator.enter({ status: 401 });
    const auth = observe(authPending);
    await flush();
    staleLocal.succeed();
    expect(coordinator.snapshot()).toMatchObject({
      state: "revalidating",
      scope: "auth",
      revalidationInFlight: true,
    });
    expect(auth.settled).toBe(false);
    gate.resolve();
    await flush();
    expect(auth.value?.probe).toBe(true);
    auth.value?.succeed();
    coordinator.close();
  });

  it("fences an outage probe when auth is observed during half-open", async () => {
    const clock = new ManualClock();
    const gate = deferred<void>();
    let revalidations = 0;
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidateAccount: async () => {
        revalidations++;
        await gate.promise;
      },
    });
    const oldOutageProbe = await coordinator.enter(outage());
    const authPending = coordinator.enter({ status: 403 });
    const auth = observe(authPending);
    await flush();
    expect(revalidations).toBe(0);
    expect(coordinator.snapshot()).toMatchObject({
      state: "half_open",
      scope: "auth",
      probeInFlight: true,
    });

    oldOutageProbe.succeed();
    await flush();
    expect(revalidations).toBe(1);
    expect(coordinator.snapshot().state).toBe("revalidating");
    gate.resolve();
    await flush();
    expect(auth.value?.probe).toBe(true);
    auth.value?.succeed();
    coordinator.close();
  });

  it("closes abortably, rejects waiters, and emits only safe snapshots", async () => {
    const clock = new ManualClock();
    const transitions: Readonly<FleetRecoverySnapshot>[] = [];
    let revalidationSignal: AbortSignal | undefined;
    const coordinator = new FleetRecoveryCoordinator({
      clock,
      random: { next: () => 0 },
      revalidateAccount: signal => {
        revalidationSignal = signal;
        return new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), {
            once: true,
          });
        });
      },
      onTransition: snapshot => {
        transitions.push(snapshot);
        if (snapshot.state === "open") throw new Error("ignored observer failure");
      },
    });
    const one = coordinator.enter({
      status: 401,
      projectId: "project-secret",
      reason: "authorization=top-secret",
    });
    const two = coordinator.enter({ status: 401 });
    await flush();
    expect(revalidationSignal?.aborted).toBe(false);

    coordinator.close();
    expect(revalidationSignal?.aborted).toBe(true);
    await expect(one).rejects.toMatchObject({ name: "AbortError" });
    await expect(two).rejects.toMatchObject({ name: "AbortError" });
    expect(coordinator.snapshot()).toEqual({
      state: "closed_for_shutdown",
      scope: null,
      consecutiveFailures: 0,
      queued: 0,
      probeInFlight: false,
      revalidationInFlight: false,
      nextProbeAtMs: null,
      lastFailureKind: null,
    });
    await expect(coordinator.enter(outage())).rejects.toMatchObject({ name: "AbortError" });
    expect(clock.pendingTimers()).toBe(0);
    expect(transitions.length).toBeGreaterThan(0);
    const serialized = JSON.stringify(transitions);
    expect(serialized).not.toContain("project-secret");
    expect(serialized).not.toContain("top-secret");
    expect(serialized).not.toMatch(/token|cookie|authorization|secret/i);
    expect(transitions.every(Object.isFrozen)).toBe(true);
  });

  it("does not invoke a queued revalidation after immediate close", async () => {
    let calls = 0;
    const coordinator = new FleetRecoveryCoordinator({
      revalidateAccount: () => { calls++; },
    });
    const pending = coordinator.enter({ status: 401 });
    coordinator.close();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    await flush();
    expect(calls).toBe(0);
  });

  it("abandons an orphan revalidation when its final waiter aborts", async () => {
    const signals: AbortSignal[] = [];
    const coordinator = new FleetRecoveryCoordinator({
      revalidateAccount: signal => {
        signals.push(signal);
        return new Promise<void>(() => {});
      },
    });
    const abort = new AbortController();
    const pending = coordinator.enter({ status: 401 }, abort.signal);
    await flush();
    expect(signals).toHaveLength(1);
    abort.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(signals[0].aborted).toBe(true);
    expect(coordinator.snapshot()).toMatchObject({
      state: "open",
      queued: 0,
      revalidationInFlight: false,
    });

    const nextAbort = new AbortController();
    const next = coordinator.enter({ status: 401 }, nextAbort.signal);
    await flush();
    expect(signals).toHaveLength(2);
    nextAbort.abort();
    await expect(next).rejects.toMatchObject({ name: "AbortError" });
    coordinator.close();
  });

  it("does not expose mutable lease bookkeeping", async () => {
    const coordinator = new FleetRecoveryCoordinator();
    const lease = await coordinator.enter({ status: 404 });
    expect(Object.isFrozen(lease)).toBe(true);
    expect(Object.getOwnPropertySymbols(lease)).toEqual([]);
    expect(Object.keys(lease).sort()).toEqual(["cancel", "fail", "probe", "succeed"]);
    lease.cancel();
    coordinator.close();
  });

  it("makes lease settlement idempotent", async () => {
    const coordinator = new FleetRecoveryCoordinator({ random: { next: () => 0 } });
    const lease: FleetRecoveryLease = await coordinator.enter(outage());
    lease.fail(outage());
    const afterFailure = coordinator.snapshot();
    lease.fail(outage());
    lease.succeed();
    lease.cancel();
    expect(coordinator.snapshot()).toEqual(afterFailure);
    coordinator.close();
  });
});
