import { describe, expect, it } from "bun:test";
import {
  PublicationPacer,
  type PublicationReason,
  type PublicationTimer,
} from "../src/v2/protocol/publication-pacer";

interface ScheduledTimer {
  callback: () => void;
  dueAt: number;
}

class ManualPublicationTimer implements PublicationTimer {
  #now = 0;
  #nextId = 1;
  readonly #timers = new Map<number, ScheduledTimer>();
  readonly #microtasks: Array<() => void> = [];

  now(): number {
    return this.#now;
  }

  set(callback: () => void, delayMs: number): number {
    const id = this.#nextId++;
    this.#timers.set(id, { callback, dueAt: this.#now + delayMs });
    return id;
  }

  clear(handle: number | object): void {
    if (typeof handle === "number") this.#timers.delete(handle);
  }

  defer(callback: () => void): void {
    this.#microtasks.push(callback);
  }

  advanceBy(durationMs: number): void {
    const target = this.#now + durationMs;
    for (;;) {
      let selectedId: number | null = null;
      let selected: ScheduledTimer | null = null;
      for (const [id, timer] of this.#timers) {
        if (timer.dueAt > target) continue;
        if (!selected || timer.dueAt < selected.dueAt || (
          timer.dueAt === selected.dueAt && id < selectedId!
        )) {
          selectedId = id;
          selected = timer;
        }
      }
      if (!selected || selectedId === null) break;
      this.#now = selected.dueAt;
      this.#timers.delete(selectedId);
      selected.callback();
      this.flushMicrotasks();
    }
    this.#now = target;
    this.flushMicrotasks();
  }

  flushMicrotasks(): void {
    while (this.#microtasks.length > 0) this.#microtasks.shift()!();
  }

  pendingTimers(): number {
    return this.#timers.size;
  }
}

describe("PublicationPacer", () => {
  it("coalesces data from every stream onto one shared cadence", () => {
    const timer = new ManualPublicationTimer();
    const reasons: PublicationReason[] = [];
    const pacer = new PublicationPacer({
      intervalMs: 25,
      timer,
      publish: reason => reasons.push(reason),
    });

    pacer.flushNow("initial");
    for (let elapsed = 5; elapsed <= 50; elapsed += 5) {
      timer.advanceBy(5);
      pacer.requestPaced();
    }

    expect(reasons).toEqual(["initial", "paced", "paced"]);
    expect(timer.pendingTimers()).toBe(1);
    pacer.close();
    expect(timer.pendingTimers()).toBe(0);
  });

  it("coalesces urgent work and preempts a pending data publication", () => {
    const timer = new ManualPublicationTimer();
    const reasons: PublicationReason[] = [];
    const pacer = new PublicationPacer({
      intervalMs: 25,
      timer,
      publish: reason => reasons.push(reason),
    });

    pacer.flushNow("initial");
    timer.advanceBy(5);
    pacer.requestPaced();
    pacer.requestUrgent();
    pacer.requestUrgent();

    expect(reasons).toEqual(["initial"]);
    expect(timer.pendingTimers()).toBe(0);
    timer.flushMicrotasks();
    expect(reasons).toEqual(["initial", "urgent"]);
  });

  it("lets an immediate retransmission satisfy pending paced work", () => {
    const timer = new ManualPublicationTimer();
    const reasons: PublicationReason[] = [];
    const pacer = new PublicationPacer({
      intervalMs: 25,
      timer,
      publish: reason => reasons.push(reason),
    });

    pacer.flushNow("initial");
    timer.advanceBy(5);
    pacer.requestPaced();
    pacer.flushNow("retransmission");
    timer.advanceBy(100);

    expect(reasons).toEqual(["initial", "retransmission"]);
    expect(timer.pendingTimers()).toBe(0);
  });

  it("suppresses deferred callbacks after close and validates its interval", () => {
    const timer = new ManualPublicationTimer();
    const reasons: PublicationReason[] = [];
    const pacer = new PublicationPacer({ timer, publish: reason => reasons.push(reason) });
    pacer.requestUrgent();
    pacer.close();
    timer.flushMicrotasks();
    expect(reasons).toEqual([]);
    expect(() => new PublicationPacer({
      intervalMs: 0,
      timer,
      publish: () => undefined,
    })).toThrow("publication interval");
  });
});
