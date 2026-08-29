import { describe, expect, it } from "bun:test";
import { BoundedPreparationScheduler } from "../src/relay/preparation-scheduler";
import { continuationPreparationCostBytes } from "../src/anthropic/routes";
import { AnthropicContinuationIndex } from "../src/anthropic/continuation";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(finish => { resolve = finish; });
  return { promise, resolve };
}

describe("bounded preparation scheduler", () => {
  it("charges snapshot memory while retaining ingress-covered continuation concurrency", async () => {
    const MiB = 1024 * 1024;
    const uncoveredCost = continuationPreparationCostBytes(13 * MiB, 13 * MiB, 28 * MiB);
    const unreservedCost = continuationPreparationCostBytes(13 * MiB, 13 * MiB);
    expect(uncoveredCost).toBe(13 * MiB);
    expect(unreservedCost).toBe(39 * MiB);

    const runBurst = async (costBytes: number): Promise<number> => {
      const scheduler = new BoundedPreparationScheduler<number>({
        maxQueued: 64,
        maxQueuedBytes: 256 * MiB,
      });
      let prepared = 0;
      try {
        await Promise.all(Array.from({ length: 16 }, () => scheduler.prepare(async () => {
          prepared += 1;
          return prepared;
        }, costBytes, new AbortController().signal)));
        return prepared;
      } finally {
        scheduler.close();
      }
    };

    expect(await runBurst(unreservedCost)).toBe(6);
    expect(await runBurst(uncoveredCost)).toBe(16);
  });

  it("rejects invalid continuation preparation accounting without weakening bounds", () => {
    expect(continuationPreparationCostBytes(8, 13, 5)).toBe(29);
    for (const [inboundBytes, relayBytes, reservedBytes] of [
      [-1, 1, 0],
      [1, -1, 0],
      [1, 1, -1],
      [Number.MAX_SAFE_INTEGER, 1, 0],
    ] as const) {
      expect(() => continuationPreparationCostBytes(
        inboundBytes,
        relayBytes,
        reservedBytes,
      )).toThrow(RangeError);
    }
  });

  it("serializes work, rejects invalid costs, and drains queued jobs on close", async () => {
    const scheduler = new BoundedPreparationScheduler<string>({
      maxQueued: 1,
      maxQueuedBytes: 15,
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

  it("lets bounded small work overtake bulk without starving the queue head", async () => {
    const scheduler = new BoundedPreparationScheduler<string>({
      maxQueued: 16,
      maxQueuedBytes: 1_000,
    });
    const signal = new AbortController().signal;
    const gate = deferred<string | null>();
    const started = deferred<void>();
    const order: string[] = [];
    const running = scheduler.prepare(async () => {
      started.resolve();
      await gate.promise;
      order.push("running");
      return "running";
    }, 100, signal);
    await started.promise;
    const bulk = Array.from({ length: 3 }, (_, index) => scheduler.prepare(async () => {
      order.push(`bulk-${index}`);
      return `bulk-${index}`;
    }, 100, signal));
    const small = Array.from({ length: 9 }, (_, index) => scheduler.prepare(async () => {
      order.push(`small-${index}`);
      return `small-${index}`;
    }, 1, signal));
    gate.resolve(null);

    await Promise.all([running, ...bulk, ...small]);
    expect(order).toEqual([
      "running",
      ...Array.from({ length: 8 }, (_, index) => `small-${index}`),
      "bulk-0",
      "small-8",
      "bulk-1",
      "bulk-2",
    ]);
    scheduler.close();
  });

  it("is shutdown-reachable from the app-scoped continuation index lifecycle", async () => {
    const scheduler = new BoundedPreparationScheduler<string>({
      maxQueued: 2,
      maxQueuedBytes: 16,
    });
    const index = new AnthropicContinuationIndex();
    const gate = deferred<string | null>();
    const started = deferred<void>();
    const signal = new AbortController().signal;
    const running = scheduler.prepare(async () => {
      started.resolve();
      return gate.promise;
    }, 4, signal);
    await started.promise;
    const queued = scheduler.prepare(async () => "must-not-run", 4, signal);
    let shutdownCalls = 0;
    index.onShutdown(() => {
      shutdownCalls += 1;
      scheduler.close();
    });

    index.beginShutdown();
    index.beginShutdown();
    expect(await queued).toBeNull();
    expect(shutdownCalls).toBe(1);
    gate.resolve("running-finished");
    expect(await running).toBe("running-finished");

    let lateCalls = 0;
    index.onShutdown(() => { lateCalls += 1; });
    expect(lateCalls).toBe(1);
  });
});
