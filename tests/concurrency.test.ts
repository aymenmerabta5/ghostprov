import { describe, expect, it } from "bun:test";
import { ResponseScheduler, scheduleRelay } from "../src/relay/scheduler";
import type { RunHandle } from "../src/relay/pipeline";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>(res => { resolve = res; });
  return { promise, resolve };
}

describe("mixed transport concurrency", () => {
  it("runs 24 HTTP/WebSocket jobs through one eight-slot ceiling", async () => {
    const scheduler = new ResponseScheduler(8);
    const gates = Array.from({ length: 24 }, () => deferred<any>());
    const starts: number[] = [];
    let active = 0;
    let maxActive = 0;
    const relay = async (body: Record<string, unknown>): Promise<RunHandle> => {
      const id = body.id as number;
      starts.push(id);
      active++;
      maxActive = Math.max(maxActive, active);
      return {
        async *events() {},
        final: gates[id].promise.finally(() => { active--; }),
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      };
    };

    const jobs = Array.from({ length: 24 }, (_, id) => scheduleRelay(
      scheduler,
      relay,
      { id, transport: id % 2 ? "websocket" : "http" },
    ));
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(starts).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);

    for (let wave = 0; wave < 3; wave++) {
      for (let id = wave * 8; id < wave * 8 + 8; id++) gates[id].resolve({ id });
      await Promise.all(jobs.slice(wave * 8, wave * 8 + 8).map(async job => (await job).final));
      await new Promise(resolve => setTimeout(resolve, 0));
    }
    expect(starts).toEqual(Array.from({ length: 24 }, (_, id) => id));
    expect(maxActive).toBe(8);
    expect(scheduler.stats()).toMatchObject({ active: 0, queued: 0, completed: 24 });
  });
});
