import { describe, expect, it } from "bun:test";
import { ResponseScheduler, scheduleRelay } from "../src/relay/scheduler";
import type { RunHandle } from "../src/relay/run-handle";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function handle(final: Promise<unknown>): RunHandle {
  return {
    async *events() {},
    final,
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

describe("ResponseScheduler", () => {
  it("holds capacity until relay final settles and admits waiters FIFO", async () => {
    const scheduler = new ResponseScheduler(2);
    const finals = Array.from({ length: 4 }, () => deferred<unknown>());
    const started: number[] = [];
    const relay = async (body: Record<string, unknown>) => {
      const id = body.id as number;
      started.push(id);
      return handle(finals[id].promise);
    };

    const pending = Array.from({ length: 4 }, (_, id) =>
      scheduleRelay(scheduler, relay, { id }),
    );
    await Promise.resolve();
    await Promise.resolve();
    expect(started).toEqual([0, 1]);
    expect(scheduler.stats()).toMatchObject({ active: 2, queued: 2 });

    finals[1].resolve({ id: 1 });
    await pending[1].then(result => result.final);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(started).toEqual([0, 1, 2]);

    finals[0].resolve({ id: 0 });
    await pending[0].then(result => result.final);
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(started).toEqual([0, 1, 2, 3]);

    finals[2].resolve({ id: 2 });
    finals[3].resolve({ id: 3 });
    await Promise.all(pending.map(async result => (await result).final));
    expect(scheduler.stats()).toMatchObject({ active: 0, queued: 0, completed: 4 });
  });

  it("removes an aborted waiter without consuming capacity", async () => {
    const scheduler = new ResponseScheduler(1);
    const firstFinal = deferred<unknown>();
    const started: string[] = [];
    const relay = async (body: Record<string, unknown>) => {
      started.push(body.id as string);
      return handle(body.id === "first" ? firstFinal.promise : Promise.resolve({}));
    };

    const first = scheduleRelay(scheduler, relay, { id: "first" });
    await first;
    const abort = new AbortController();
    const cancelled = scheduleRelay(scheduler, relay, { id: "cancelled" }, { signal: abort.signal });
    const last = scheduleRelay(scheduler, relay, { id: "last" });
    abort.abort();

    await expect(cancelled).rejects.toMatchObject({ name: "AbortError" });
    firstFinal.resolve({});
    await (await first).final;
    await (await last).final;
    expect(started).toEqual(["first", "last"]);
    expect(scheduler.stats()).toMatchObject({ active: 0, queued: 0, cancelled: 1 });
  });

  it("releases capacity when relay startup fails", async () => {
    const scheduler = new ResponseScheduler(1);
    await expect(scheduleRelay(scheduler, async () => { throw new Error("boom"); }, {})).rejects.toThrow("boom");
    expect(scheduler.stats()).toMatchObject({ active: 0, failed: 1 });
  });
});
