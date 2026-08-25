import { describe, expect, it } from "bun:test";
import { gzipSync, gunzipSync } from "node:zlib";
import {
  CpuTransformPool,
  CpuTransformQueueFullError,
  CpuTransformTimeoutError,
} from "../src/router/cpu-transform-pool";

type WorkerMessage = { id: number; data: Uint8Array };

class FakeWorker extends EventTarget {
  onmessage: ((event: MessageEvent<WorkerMessage>) => void) | null = null;
  onerror: ((event: ErrorEvent) => void) | null = null;
  onmessageerror: ((event: MessageEvent) => void) | null = null;
  terminated = false;

  constructor(readonly receive: (worker: FakeWorker, message: { id: number; data: Uint8Array }) => void) {
    super();
  }

  postMessage(message: { id: number; data: Uint8Array }): void {
    this.receive(this, message);
  }

  terminate(): void {
    if (this.terminated) return;
    this.terminated = true;
    queueMicrotask(() => this.dispatchEvent(new CloseEvent("close", {
      code: 0,
      reason: "Worker terminated normally",
      wasClean: true,
    })));
  }

  result(id: number, data: Uint8Array): void {
    this.onmessage?.(new MessageEvent("message", { data: { id, data } }));
  }

  fail(message: string): void {
    this.onerror?.(new ErrorEvent("error", { message }));
  }
}

function asWorker(worker: FakeWorker): Worker {
  return worker as unknown as Worker;
}

describe("CpuTransformPool", () => {
  it("runs compression away from the main event loop with bounded workers", async () => {
    const pool = new CpuTransformPool({ workers: 2 });
    const payloads = Array.from({ length: 6 }, (_, index) => new TextEncoder().encode(`payload-${index}-`.repeat(80_000)));
    const started = performance.now();
    const timer = new Promise<number>(resolve => setTimeout(() => resolve(performance.now() - started), 10));
    const compressed = Promise.all(payloads.map(payload => pool.gzip(payload)));

    const timerDelay = await timer;
    const results = await compressed;

    expect(timerDelay).toBeLessThan(50);
    expect(pool.snapshot().maxActive).toBeLessThanOrEqual(2);
    for (let index = 0; index < results.length; index++) {
      expect(payloads[index].byteLength).toBeGreaterThan(0);
      expect(new TextDecoder().decode(gunzipSync(results[index]))).toBe(new TextDecoder().decode(payloads[index]));
    }
    await pool.close();
  });

  it("supports explicit zero-copy input ownership transfer", async () => {
    const pool = new CpuTransformPool({ workers: 1 });
    const expected = new TextEncoder().encode("owned-payload-".repeat(10_000));
    const input = expected.slice();

    const compressed = pool.gzip(input, { transferInput: true });
    expect(input.byteLength).toBe(0);
    const result = await compressed;
    expect(result.byteOffset).toBe(0);
    expect(result.buffer.byteLength).toBe(result.byteLength);
    expect(new Uint8Array(gunzipSync(result))).toEqual(expected);

    await pool.close();
  });

  it("bounds the waiting queue before copying input", async () => {
    const workers: FakeWorker[] = [];
    const pool = new CpuTransformPool({
      workers: 1,
      maxQueued: 1,
      workerFactory: () => {
        const worker = new FakeWorker(() => undefined);
        workers.push(worker);
        return asWorker(worker);
      },
    });
    const first = pool.gzip(new Uint8Array([1])).catch(error => error);
    const second = pool.gzip(new Uint8Array([2])).catch(error => error);
    const rejectedInput = new Uint8Array([3]);

    await expect(pool.gzip(rejectedInput, { transferInput: true })).rejects.toBeInstanceOf(
      CpuTransformQueueFullError,
    );
    expect(rejectedInput.byteLength).toBe(1);
    expect(pool.snapshot().queued).toBe(1);

    await pool.close();
    expect(await first).toBeInstanceOf(Error);
    expect(await second).toBeInstanceOf(Error);
    expect(workers[0].terminated).toBe(true);
  });

  it("removes an aborted queued transform without disturbing active work", async () => {
    const pool = new CpuTransformPool({
      workers: 1,
      workerFactory: () => asWorker(new FakeWorker(() => undefined)),
    });
    const active = pool.gzip(new Uint8Array([1])).catch(error => error);
    const controller = new AbortController();
    const queued = pool.gzip(new Uint8Array([2]), { signal: controller.signal });
    controller.abort();

    await expect(queued).rejects.toMatchObject({ name: "AbortError" });
    expect(pool.snapshot().queued).toBe(0);
    await pool.close();
    expect(await active).toBeInstanceOf(Error);
  });

  it("replaces a timed-out worker so later transforms still complete", async () => {
    let creations = 0;
    const pool = new CpuTransformPool({
      workers: 1,
      workerFactory: () => {
        creations++;
        if (creations === 1) return asWorker(new FakeWorker(() => undefined));
        return asWorker(new FakeWorker((worker, message) => {
          queueMicrotask(() => worker.result(message.id, gzipSync(message.data)));
        }));
      },
    });

    await expect(pool.gzip(new Uint8Array([1]), { timeoutMs: 10 })).rejects.toBeInstanceOf(
      CpuTransformTimeoutError,
    );
    const expected = new TextEncoder().encode("after-restart");
    const result = await pool.gzip(expected, { timeoutMs: 1_000 });
    expect(new Uint8Array(gunzipSync(result))).toEqual(expected);
    expect(creations).toBe(2);
    expect(pool.snapshot().restarts).toBe(1);

    await pool.close();
  });

  it("replaces an actively aborted worker and preserves AbortError", async () => {
    let creations = 0;
    const pool = new CpuTransformPool({
      workers: 1,
      workerFactory: () => {
        creations++;
        if (creations === 1) return asWorker(new FakeWorker(() => undefined));
        return asWorker(new FakeWorker((worker, message) => {
          queueMicrotask(() => worker.result(message.id, gzipSync(message.data)));
        }));
      },
    });
    const controller = new AbortController();
    const active = pool.gzip(new Uint8Array([1]), { signal: controller.signal });
    controller.abort();

    await expect(active).rejects.toMatchObject({ name: "AbortError" });
    const expected = new TextEncoder().encode("after-abort");
    expect(new Uint8Array(gunzipSync(await pool.gzip(expected)))).toEqual(expected);
    expect(creations).toBe(2);

    await pool.close();
  });

  it("does not overlap retiring real workers during abort and timeout storms", async () => {
    const pool = new CpuTransformPool({ workers: 2 });
    const payload = new Uint8Array(8 * 1024 * 1024);
    let random = 0x9e3779b9;
    for (let index = 0; index < payload.length; index++) {
      random ^= random << 13;
      random ^= random >>> 17;
      random ^= random << 5;
      payload[index] = random;
    }

    for (let round = 0; round < 4; round++) {
      const controllers = [new AbortController(), new AbortController()];
      const aborted = controllers.map(controller => pool.gzip(payload, { signal: controller.signal }));
      for (const controller of controllers) controller.abort();

      // terminate() is asynchronous. Until both real workers emit close, their
      // slots remain retiring and no replacement is considered ready.
      const retiring = pool.snapshot();
      expect(retiring.active).toBe(0);
      expect(retiring.readyWorkers).toBe(0);
      expect(retiring.retiringWorkers).toBe(2);
      expect(retiring.liveWorkers).toBeLessThanOrEqual(retiring.workers);
      const abortedResults = await Promise.allSettled(aborted);
      expect(abortedResults.every(result => (
        result.status === "rejected" && (result.reason as { name?: string }).name === "AbortError"
      ))).toBe(true);

      const recovery = await Promise.all([
        pool.gzip(new TextEncoder().encode(`abort-recovery-a-${round}`)),
        pool.gzip(new TextEncoder().encode(`abort-recovery-b-${round}`)),
      ]);
      expect(recovery.every(result => gunzipSync(result).byteLength > 0)).toBe(true);
      expect(pool.snapshot().liveWorkers).toBeLessThanOrEqual(2);

      const timedOut = [
        pool.gzip(payload, { timeoutMs: 1 }),
        pool.gzip(payload, { timeoutMs: 1 }),
      ];
      const timeoutResults = await Promise.allSettled(timedOut);
      expect(timeoutResults.every(result => (
        result.status === "rejected" && result.reason instanceof CpuTransformTimeoutError
      ))).toBe(true);

      await Promise.all([
        pool.gzip(new TextEncoder().encode(`timeout-recovery-a-${round}`)),
        pool.gzip(new TextEncoder().encode(`timeout-recovery-b-${round}`)),
      ]);
      const recovered = pool.snapshot();
      expect(recovered.readyWorkers).toBe(2);
      expect(recovered.retiringWorkers).toBe(0);
      expect(recovered.liveWorkers).toBeLessThanOrEqual(recovered.workers);
    }

    await pool.close();
    expect(pool.snapshot().liveWorkers).toBe(0);
  });

  it("recovers from a worker error and closes idempotently", async () => {
    let creations = 0;
    const pool = new CpuTransformPool({
      workers: 1,
      workerFactory: () => {
        creations++;
        if (creations === 1) {
          return asWorker(new FakeWorker(worker => queueMicrotask(() => worker.fail("worker exploded"))));
        }
        return asWorker(new FakeWorker((worker, message) => {
          queueMicrotask(() => worker.result(message.id, gzipSync(message.data)));
        }));
      },
    });

    await expect(pool.gzip(new Uint8Array([1]))).rejects.toThrow("worker exploded");
    const result = await pool.gzip(new TextEncoder().encode("healthy replacement"));
    expect(new TextDecoder().decode(gunzipSync(result))).toBe("healthy replacement");

    await pool.close();
    await pool.close();
    expect(pool.snapshot().closed).toBe(true);
    await expect(pool.gzip(new Uint8Array([1]))).rejects.toThrow("closed");
  });
});
