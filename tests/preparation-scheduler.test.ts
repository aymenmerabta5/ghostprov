import { describe, expect, it } from "bun:test";
import { BoundedPreparationScheduler } from "../src/relay/preparation-scheduler";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(finish => { resolve = finish; });
  return { promise, resolve };
}

describe("bounded preparation scheduler", () => {
  it("serializes work, rejects invalid costs, and drains queued jobs on close", async () => {
    const scheduler = new BoundedPreparationScheduler<string>({
      maxQueued: 1,
      maxQueuedBytes: 10,
    });
    const controller = new AbortController();
    const gate = deferred<string | null>();
    const started = deferred<void>();
    const first = scheduler.prepare(async () => {
      started.resolve();
      return gate.promise;
    }, 5, controller.signal);
    await started.promise;

    const queued = scheduler.prepare(async () => "queued", 10, controller.signal);
    expect(await scheduler.prepare(async () => "overflow", 1, controller.signal)).toBeNull();
    expect(await scheduler.prepare(async () => "invalid", Number.NaN, controller.signal)).toBeNull();

    scheduler.close();
    expect(await queued).toBeNull();
    gate.resolve("first");
    expect(await first).toBe("first");
    expect(await scheduler.prepare(async () => "late", 1, controller.signal)).toBeNull();
    scheduler.close();
  });
});
