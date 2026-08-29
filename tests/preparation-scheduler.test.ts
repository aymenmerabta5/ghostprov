import { describe, expect, it } from "bun:test";
import { BoundedPreparationScheduler } from "../src/relay/preparation-scheduler";
import { continuationPreparationCostBytes } from "../src/anthropic/routes";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(finish => { resolve = finish; });
  return { promise, resolve };
}

describe("bounded preparation scheduler", () => {
  it("uses ingress-covered memory so sixteen large continuations do not fall back to bulk relay", async () => {
    const MiB = 1024 * 1024;
    const uncoveredCost = continuationPreparationCostBytes(13 * MiB, 13 * MiB, 28 * MiB);
    const unreservedCost = continuationPreparationCostBytes(13 * MiB, 13 * MiB);
    expect(uncoveredCost).toBe(1);
    expect(unreservedCost).toBe(26 * MiB);

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

    expect(await runBurst(unreservedCost)).toBe(9);
    expect(await runBurst(uncoveredCost)).toBe(16);
  });

  it("rejects invalid continuation preparation accounting without weakening bounds", () => {
    expect(continuationPreparationCostBytes(8, 13, 5)).toBe(16);
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
