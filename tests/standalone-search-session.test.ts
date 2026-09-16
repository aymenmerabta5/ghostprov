import { describe, expect, it } from "bun:test";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import {
  StandaloneSearchQueueTimeoutError,
  StandaloneSearchSessionIndex,
} from "../src/search/session-index";
import { normalizeApiError } from "../src/http/errors";
import { RouterCapacityTimeoutError } from "../src/router/router";

class QueueClock {
  now = 0;
  readonly timers = new Set<{ at: number; callback: () => void }>();
  readonly schedule = (callback: () => void, delayMs: number): (() => void) => {
    const timer = { at: this.now + delayMs, callback };
    this.timers.add(timer);
    return () => { this.timers.delete(timer); };
  };
  advance(now: number): void {
    this.now = now;
    for (const timer of [...this.timers]) {
      if (timer.at > now || !this.timers.delete(timer)) continue;
      timer.callback();
    }
  }
}

function gate(): { promise: Promise<void>; release: () => void } {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

describe("StandaloneSearchSessionIndex", () => {
  it("serializes one opaque session and advances only after successful completion", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(7) });
    const scope = createAuthenticatedTenantScope();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const seen: Array<string | null> = [];
    const first = index.run(scope, "session-a", async previous => {
      seen.push(previous);
      await gate;
      return { responseId: "resp_first", value: "first" };
    });
    const second = index.run(scope, "session-a", async previous => {
      seen.push(previous);
      return { responseId: "resp_second", value: "second" };
    });
    for (let index = 0; index < 4; index++) await Promise.resolve();
    expect(seen).toEqual([null]);
    release();
    expect(await Promise.all([first, second])).toEqual(["first", "second"]);
    expect(seen).toEqual([null, "resp_first"]);
    expect(index.snapshot()).toMatchObject({ records: 1, activeKeys: 0 });
  });

  it("keeps trusted scopes isolated and retains the previous ID after a failed turn", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(8) });
    const firstScope = createAuthenticatedTenantScope();
    const otherScope = createAuthenticatedTenantScope();
    await index.run(firstScope, "same-public-id", async previous => ({
      responseId: "resp_first",
      value: previous,
    }));
    await expect(index.run(firstScope, "same-public-id", async (previous, compareDelete) => {
      expect(previous).toBe("resp_first");
      expect(compareDelete("resp_other")).toBe(false);
      throw new Error("failed follow-up");
    })).rejects.toThrow("failed follow-up");
    const retained = await index.run(firstScope, "same-public-id", async previous => ({
      responseId: "resp_recovered",
      value: previous,
    }));
    const isolated = await index.run(otherScope, "same-public-id", async previous => ({
      responseId: "resp_other",
      value: previous,
    }));
    expect(retained).toBe("resp_first");
    expect(isolated).toBeNull();
  });

  it("compare-deletes only the exact serialized session binding", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(10) });
    const scope = createAuthenticatedTenantScope();
    await index.run(scope, "session-delete", async () => ({
      responseId: "resp_poisoned",
      value: null,
    }));

    await expect(index.run(scope, "session-delete", async (previous, compareDelete) => {
      expect(previous).toBe("resp_poisoned");
      expect(compareDelete("resp_replaced")).toBe(false);
      expect(compareDelete("resp_poisoned")).toBe(true);
      expect(compareDelete("resp_poisoned")).toBe(false);
      throw new Error("retry failed");
    })).rejects.toThrow("retry failed");

    const next = await index.run(scope, "session-delete", async previous => ({
      responseId: "resp_fresh",
      value: previous,
    }));
    expect(next).toBeNull();
  });

  it("expires and evicts bounded content-free records without blocking execution", async () => {
    let now = 0;
    const index = new StandaloneSearchSessionIndex({
      hmacKey: new Uint8Array(32).fill(9),
      maxRecords: 1,
      maxBytes: 1024,
      ttlMs: 10,
      now: () => now,
    });
    const scope = createAuthenticatedTenantScope();
    await index.run(scope, "one", async () => ({ responseId: "resp_one", value: null }));
    await index.run(scope, "two", async () => ({ responseId: "resp_two", value: null }));
    expect(index.snapshot().records).toBe(1);
    const evicted = await index.run(scope, "one", async previous => ({
      responseId: "resp_one_new",
      value: previous,
    }));
    expect(evicted).toBeNull();
    now = 11;
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0 });
  });

  it("fences late metadata across clear and all metadata after shutdown", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(11) });
    const scope = createAuthenticatedTenantScope();
    let releaseClear!: () => void;
    const clearGate = new Promise<void>(resolve => { releaseClear = resolve; });
    const pendingClear = index.run(scope, "clear-session", async () => {
      await clearGate;
      return { responseId: "resp_after_clear", value: "cleared" };
    });
    for (let turn = 0; turn < 4; turn++) await Promise.resolve();
    index.clear();
    releaseClear();
    expect(await pendingClear).toBe("cleared");
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0, activeKeys: 0 });

    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const pending = index.run(scope, "shutdown-session", async () => {
      await gate;
      return { responseId: "resp_late", value: "late" };
    });
    for (let turn = 0; turn < 4; turn++) await Promise.resolve();

    index.beginShutdown();
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0, activeKeys: 1 });
    release();
    expect(await pending).toBe("late");
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0, activeKeys: 0 });

    let invoked = false;
    await expect(index.run(scope, "shutdown-session", async previous => {
      invoked = true;
      return { responseId: "resp_after_shutdown", value: previous };
    })).rejects.toThrow("shutting down");
    expect(invoked).toBe(false);
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0, activeKeys: 0 });
  });

  it("removes an aborted waiter immediately and preserves the next exact provider parent", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const firstGate = gate();
    const seen: Array<string | null> = [];
    const first = index.run(scope, "same", async previous => {
      seen.push(previous);
      await firstGate.promise;
      return { responseId: "resp_exact_parent", value: "first" };
    });
    await Promise.resolve();
    const controller = new AbortController();
    const cancelled = index.run(scope, "same", async () => {
      throw new Error("Cancelled callback must not run");
    }, { signal: controller.signal });
    const cancellation = cancelled.catch(error => error);
    const next = index.run(scope, "same", async previous => {
      seen.push(previous);
      return { responseId: "resp_next", value: "next" };
    });
    expect(index.snapshot().pendingRequests).toBe(2);
    controller.abort();
    expect(await cancellation).toMatchObject({ name: "AbortError" });
    expect(index.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 1 });
    expect(clock.timers.size).toBe(1);
    expect(seen).toEqual([null]);
    firstGate.release();
    expect(await Promise.all([first, next])).toEqual(["first", "next"]);
    expect(seen).toEqual([null, "resp_exact_parent"]);
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
  });

  it("expires waiting work on the original fractional ingress deadline before its predecessor completes", async () => {
    const clock = new QueueClock();
    clock.now = 100.25;
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const firstGate = gate();
    const first = index.run(scope, "same", async () => {
      await firstGate.promise;
      return { responseId: "resp_first", value: null };
    });
    await Promise.resolve();
    let invoked = false;
    const waiting = index.run(scope, "same", async () => {
      invoked = true;
      return { responseId: "resp_expired", value: null };
    }, { deadlineMs: 110.25 });
    const expiry = waiting.catch(error => error);
    clock.advance(109.25);
    expect(index.snapshot().pendingRequests).toBe(1);
    clock.advance(110.25);
    expect(await expiry).toBeInstanceOf(StandaloneSearchQueueTimeoutError);
    expect(index.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
    firstGate.release();
    await first;
    expect(invoked).toBe(false);
  });

  it("checks expired deadlines again before dispatch even when the wait timer has not fired", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    let invoked = false;
    const pending = index.run(scope, "same", async () => {
      invoked = true;
      return { responseId: "resp_wrong", value: null };
    }, { deadlineMs: 10 });
    const expiry = pending.catch(error => error);
    clock.now = 10;
    expect(await expiry).toBeInstanceOf(StandaloneSearchQueueTimeoutError);
    expect(invoked).toBe(false);
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
    await expect(index.run(scope, "same", async () => {
      invoked = true;
      return { responseId: "resp_wrong", value: null };
    }, { deadlineMs: 9 })).rejects.toBeInstanceOf(StandaloneSearchQueueTimeoutError);
    expect(invoked).toBe(false);
  });

  it("classifies an ingress timeout while queued as a local search wait rather than router capacity", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const controller = new AbortController();
    const pending = index.run(scope, "same", async () => {
      throw new Error("Expired callback must not run");
    }, { signal: controller.signal, deadlineMs: 10.5 });
    const error = pending.catch(error => error);
    controller.abort(new RouterCapacityTimeoutError());
    expect(normalizeApiError(await error)).toMatchObject({
      status: 408,
      error: { code: "search_session_queue_timeout" },
    });
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
  });

  it("fences queued work on clear while retaining the running session's ordering lock", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const firstGate = gate();
    const first = index.run(scope, "same", async () => {
      await firstGate.promise;
      return { responseId: "resp_stale", value: null };
    });
    await Promise.resolve();
    const pending = index.run(scope, "same", async () => {
      throw new Error("Cleared callback must not run");
    });
    const cleared = pending.catch(error => error);
    index.clear();
    expect(await cleared).toMatchObject({ message: expect.stringContaining("cleared before execution") });
    expect(index.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 0, records: 0 });
    expect(clock.timers.size).toBe(0);
    let nextInvoked = false;
    const next = index.run(scope, "same", async previous => {
      nextInvoked = true;
      expect(previous).toBeNull();
      return { responseId: "resp_after_clear", value: null };
    });
    await Promise.resolve();
    expect(nextInvoked).toBe(false);
    firstGate.release();
    await Promise.all([first, next]);
    expect(nextInvoked).toBe(true);
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0, records: 1 });
  });

  it.each(["clear", "beginShutdown"] as const)("%s fences callbacks before their first dispatch", async action => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const pending = index.run(createAuthenticatedTenantScope(), "same", async () => {
      throw new Error("Fenced callback must not run");
    });
    const stopped = pending.catch(error => error);
    index[action]();
    expect(await stopped).toMatchObject({
      message: expect.stringContaining(action === "clear" ? "cleared" : "shutting down"),
    });
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0, records: 0 });
    expect(clock.timers.size).toBe(0);
  });

  it("keeps unrelated sessions parallel and enforces per-session, global, and active-key bounds", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({
      now: () => clock.now,
      schedule: clock.schedule,
      maxPendingPerKey: 1,
      maxPendingRequests: 2,
      maxActiveKeys: 2,
    });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const active: string[] = [];
    const start = (key: string) => index.run(scope, key, async () => {
      active.push(key);
      await activeGate.promise;
      return { responseId: `resp_${key}`, value: key };
    });
    const first = start("one");
    const other = start("two");
    await Promise.resolve();
    expect(active).toEqual(["one", "two"]);
    await expect(start("third")).rejects.toThrow("queue is full");
    const controller = new AbortController();
    const pending = index.run(scope, "one", async () => ({ responseId: "resp_queued", value: null }), {
      signal: controller.signal,
    });
    const aborted = pending.catch(error => error);
    await expect(start("one")).rejects.toThrow("queue is full");
    const otherPending = start("two");
    expect(index.snapshot()).toMatchObject({ activeKeys: 2, pendingRequests: 2 });
    await expect(start("fourth")).rejects.toThrow("queue is full");
    controller.abort();
    expect(await aborted).toMatchObject({ name: "AbortError" });
    const replacement = start("one");
    expect(index.snapshot().pendingRequests).toBe(2);
    activeGate.release();
    await Promise.all([first, other, otherPending, replacement]);
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
  });

  it("bounds a queue without an ingress lease and rejects an already aborted request", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule, maxWaitMs: 20 });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const first = index.run(scope, "same", async () => {
      await activeGate.promise;
      return { responseId: "resp_first", value: null };
    });
    await Promise.resolve();
    const pending = index.run(scope, "same", async () => {
      throw new Error("Expired callback must not run");
    });
    const expired = pending.catch(error => error);
    clock.advance(20);
    expect(await expired).toBeInstanceOf(StandaloneSearchQueueTimeoutError);
    const controller = new AbortController();
    controller.abort();
    await expect(index.run(scope, "same", async () => {
      throw new Error("Aborted callback must not run");
    }, { signal: controller.signal })).rejects.toMatchObject({ name: "AbortError" });
    expect(index.snapshot().pendingRequests).toBe(0);
    activeGate.release();
    await first;
    expect(clock.timers.size).toBe(0);
  });

  it("holds the active serialization lock through cancellation until the operation settles", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const controller = new AbortController();
    const first = index.run(scope, "same", async () => {
      await activeGate.promise;
      throw new DOMException("Active operation cancellation proved", "AbortError");
    }, { signal: controller.signal });
    const firstResult = first.catch(error => error);
    await Promise.resolve();
    let nextInvoked = false;
    const next = index.run(scope, "same", async previous => {
      nextInvoked = true;
      expect(previous).toBeNull();
      return { responseId: "resp_next", value: null };
    });
    controller.abort();
    await Promise.resolve();
    expect(nextInvoked).toBe(false);
    expect(index.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 1 });
    activeGate.release();
    expect(await firstResult).toMatchObject({ name: "AbortError" });
    await next;
    expect(nextInvoked).toBe(true);
  });

  it("preserves a supplied ingress budget beyond the fallback wait limit", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({
      now: () => clock.now,
      schedule: clock.schedule,
      maxWaitMs: 10,
    });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const first = index.run(scope, "same", async () => {
      await activeGate.promise;
      return { responseId: "resp_first", value: null };
    });
    await Promise.resolve();
    const pending = index.run(scope, "same", async () => {
      throw new Error("Expired callback must not run");
    }, { deadlineMs: 100 });
    const expiry = pending.catch(error => error);
    clock.advance(10);
    expect(index.snapshot().pendingRequests).toBe(1);
    clock.advance(100);
    expect(await expiry).toBeInstanceOf(StandaloneSearchQueueTimeoutError);
    activeGate.release();
    await first;
    expect(clock.timers.size).toBe(0);
  });

  it("enforces the global pending limit even when key and per-session space remains", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({
      now: () => clock.now,
      schedule: clock.schedule,
      maxPendingRequests: 1,
      maxPendingPerKey: 2,
      maxActiveKeys: 3,
    });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const first = index.run(scope, "same", async () => {
      await activeGate.promise;
      return { responseId: "resp_first", value: null };
    });
    await Promise.resolve();
    const pending = index.run(scope, "same", async () => ({ responseId: "resp_second", value: null }));
    await expect(index.run(scope, "different", async () => {
      throw new Error("Overloaded callback must not run");
    })).rejects.toThrow("queue is full");
    expect(index.snapshot()).toMatchObject({ activeKeys: 1, pendingRequests: 1 });
    activeGate.release();
    await Promise.all([first, pending]);
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
  });

  it("admits ten independent sessions concurrently under the default bounds", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    let active = 0;
    const calls = Array.from({ length: 10 }, (_, client) => index.run(scope, `session-${client}`, async previous => {
      expect(previous).toBeNull();
      active++;
      await activeGate.promise;
      return { responseId: `resp_${client}`, value: client };
    }));
    await Promise.resolve();
    expect(active).toBe(10);
    expect(index.snapshot()).toMatchObject({ activeKeys: 10, pendingRequests: 0 });
    activeGate.release();
    expect(await Promise.all(calls)).toEqual(Array.from({ length: 10 }, (_, client) => client));
    expect(index.snapshot()).toMatchObject({ activeKeys: 0, pendingRequests: 0 });
    expect(clock.timers.size).toBe(0);
  });

  it("rechecks an early timer and cannot rearm a removed waiter's timer", async () => {
    const clock = new QueueClock();
    const index = new StandaloneSearchSessionIndex({ now: () => clock.now, schedule: clock.schedule });
    const scope = createAuthenticatedTenantScope();
    const activeGate = gate();
    const first = index.run(scope, "same", async () => {
      await activeGate.promise;
      return { responseId: "resp_first", value: null };
    });
    await Promise.resolve();
    const controller = new AbortController();
    const pending = index.run(scope, "same", async () => {
      throw new Error("Cancelled callback must not run");
    }, { signal: controller.signal, deadlineMs: 20 });
    const result = pending.catch(error => error);
    const early = [...clock.timers][0]!;
    clock.timers.delete(early);
    clock.now = 10;
    early.callback();
    expect(index.snapshot().pendingRequests).toBe(1);
    expect([...clock.timers].map(timer => timer.at)).toEqual([20]);
    controller.abort();
    expect(await result).toMatchObject({ name: "AbortError" });
    early.callback();
    expect(clock.timers.size).toBe(0);
    activeGate.release();
    await first;
  });
});
