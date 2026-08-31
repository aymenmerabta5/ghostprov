import { describe, expect, it } from "bun:test";
import { gzipSync, gunzipSync } from "node:zlib";
import {
  CpuTransformPool,
  CpuTransformQueueFullError,
  CpuTransformShutdownError,
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

class NonClosingWorker extends FakeWorker {
  override terminate(): void {
    this.terminated = true;
  }
}

function asWorker(worker: FakeWorker): Worker {
  return worker as unknown as Worker;
}

describe("CpuTransformPool", () => {
  it("recovers inline-image base64 expansion for bulk request transport", async () => {
    let state = 0x5eed1234;
    const imageBytes = new Uint8Array(512 * 1024);
    for (let index = 0; index < imageBytes.length; index++) {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      imageBytes[index] = state & 0xff;
    }
    const payload = new TextEncoder().encode(JSON.stringify({
      type: "input_image",
      image_url: `data:image/png;base64,${Buffer.from(imageBytes).toString("base64")}`,
    }));
    const pool = new CpuTransformPool({ workers: 1 });

    try {
      const compressed = await pool.gzip(payload, { priority: "bulk" });

      expect(compressed.byteLength).toBeLessThan(payload.byteLength * 0.8);
      expect(new Uint8Array(gunzipSync(compressed))).toEqual(payload);
    } finally {
      await pool.close();
    }
  });

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

  it("bounds worker teardown without falsely reporting physical exit", async () => {
    const worker = new NonClosingWorker(() => undefined);
    const pool = new CpuTransformPool({
      workers: 1,
      workerCloseTimeoutMs: 10,
      workerFactory: () => asWorker(worker),
    });

    await expect(pool.close()).rejects.toBeInstanceOf(CpuTransformShutdownError);
    expect(worker.terminated).toBe(true);
    expect(pool.snapshot()).toMatchObject({
      closed: true,
      readyWorkers: 0,
      retiringWorkers: 1,
      liveWorkers: 1,
    });
    await expect(pool.close()).rejects.toBeInstanceOf(CpuTransformShutdownError);
  });

  it("prioritizes queued latency work without leaving a healthy worker idle", async () => {
    const workers: FakeWorker[] = [];
    const assignments: Array<{ worker: FakeWorker; id: number; value: number }> = [];
    const pool = new CpuTransformPool({
      workers: 2,
      workerFactory: () => {
        const worker = new FakeWorker((owner, message) => {
          assignments.push({ worker: owner, id: message.id, value: message.data[0]! });
        });
        workers.push(worker);
        return asWorker(worker);
      },
    });

    const bulkOne = pool.gzip(new Uint8Array([1]), { priority: "bulk" });
    const bulkTwo = pool.gzip(new Uint8Array([2]), { priority: "bulk" });
    expect(pool.snapshot()).toMatchObject({ active: 2, queued: 0 });
    expect(assignments.map(value => value.value)).toEqual([1, 2]);

    const bulkThree = pool.gzip(new Uint8Array([4]), { priority: "bulk" });
    const latency = pool.gzip(new Uint8Array([3]), { priority: "latency" });
    expect(pool.snapshot()).toMatchObject({ active: 2, queued: 2 });

    const secondBulk = assignments.find(value => value.value === 2)!;
    secondBulk.worker.result(secondBulk.id, new Uint8Array([20]));
    expect(await bulkTwo).toEqual(new Uint8Array([20]));
    await Promise.resolve();
    expect(assignments.map(value => value.value)).toEqual([1, 2, 3]);
    const latencyAssignment = assignments.at(-1)!;
    latencyAssignment.worker.result(latencyAssignment.id, new Uint8Array([30]));
    expect(await latency).toEqual(new Uint8Array([30]));
    await Promise.resolve();
    expect(assignments.map(value => value.value)).toEqual([1, 2, 3, 4]);
    const thirdBulk = assignments.at(-1)!;
    thirdBulk.worker.result(thirdBulk.id, new Uint8Array([40]));
    expect(await bulkThree).toEqual(new Uint8Array([40]));

    const firstBulk = assignments.find(value => value.value === 1)!;
    firstBulk.worker.result(firstBulk.id, new Uint8Array([10]));
    expect(await bulkOne).toEqual(new Uint8Array([10]));
    await pool.close();
  });

  it("moves the latency reservation when the previously reserved worker is busy", async () => {
    const workers: FakeWorker[] = [];
    const assignments: Array<{ worker: FakeWorker; id: number; value: number }> = [];
    const pool = new CpuTransformPool({
      workers: 2,
      workerFactory: () => {
        const worker = new FakeWorker((owner, message) => {
          assignments.push({ worker: owner, id: message.id, value: message.data[0]! });
        });
        workers.push(worker);
        return asWorker(worker);
      },
    });

    const first = pool.gzip(new Uint8Array([1]), { priority: "bulk" });
    const second = pool.gzip(new Uint8Array([2]), { priority: "bulk" });
    const queuedBulk = pool.gzip(new Uint8Array([3]), { priority: "bulk" });
    const latency = pool.gzip(new Uint8Array([4]), { priority: "latency" });

    const firstAssignment = assignments.find(value => value.value === 1)!;
    firstAssignment.worker.result(firstAssignment.id, new Uint8Array([10]));
    await first;
    await Promise.resolve();
    expect(assignments.at(-1)?.value).toBe(4);
    expect(assignments.at(-1)?.worker).toBe(firstAssignment.worker);

    for (const assignment of assignments.slice(1)) {
      if (assignment.value === 2) assignment.worker.result(assignment.id, new Uint8Array([20]));
      if (assignment.value === 4) assignment.worker.result(assignment.id, new Uint8Array([40]));
    }
    await Promise.all([second, latency]);
    await Promise.resolve();
    const bulkAssignment = assignments.find(value => value.value === 3)!;
    bulkAssignment.worker.result(bulkAssignment.id, new Uint8Array([30]));
    await queuedBulk;
    await pool.close();
  });

  it("lets bulk work use the reserved slot when it is the only healthy worker", async () => {
    const workers: FakeWorker[] = [];
    const assignments: Array<{ worker: FakeWorker; id: number; value: number }> = [];
    const pool = new CpuTransformPool({
      workers: 2,
      workerFactory: () => {
        const worker = new FakeWorker((owner, message) => {
          assignments.push({ worker: owner, id: message.id, value: message.data[0]! });
        });
        workers.push(worker);
        return asWorker(worker);
      },
    });

    const failed = pool.gzip(new Uint8Array([1]), { priority: "bulk" }).catch(error => error);
    const active = pool.gzip(new Uint8Array([2]), { priority: "bulk" });
    const queued = pool.gzip(new Uint8Array([3]), { priority: "bulk" });
    expect(assignments.map(value => value.value)).toEqual([1, 2]);

    workers[0]!.fail("first worker failed");
    assignments[1]!.worker.result(assignments[1]!.id, new Uint8Array([20]));
    expect(await active).toEqual(new Uint8Array([20]));
    expect(assignments.map(value => value.value)).toEqual([1, 2, 3]);
    expect(assignments[2]!.worker).toBe(workers[1]);
    assignments[2]!.worker.result(assignments[2]!.id, new Uint8Array([30]));

    expect(await failed).toBeInstanceOf(Error);
    expect(await queued).toEqual(new Uint8Array([30]));
    await pool.close();
  });

  it("bounds latency overtakes when only one worker is healthy", async () => {
    const assignments: Array<{ worker: FakeWorker; id: number; value: number }> = [];
    const pool = new CpuTransformPool({
      workers: 1,
      workerFactory: () => asWorker(new FakeWorker((worker, message) => {
        assignments.push({ worker, id: message.id, value: message.data[0]! });
      })),
    });

    const active = pool.gzip(new Uint8Array([0]), { priority: "bulk" });
    const bulk = pool.gzip(new Uint8Array([1]), { priority: "bulk" });
    const latency = Array.from({ length: 12 }, (_, index) => (
      pool.gzip(new Uint8Array([index + 2]), { priority: "latency" })
    ));

    assignments[0]!.worker.result(assignments[0]!.id, new Uint8Array([0]));
    await active;
    while (!assignments.some(value => value.value === 1)) {
      const assignment = assignments.at(-1)!;
      assignment.worker.result(assignment.id, new Uint8Array([assignment.value]));
      await Promise.resolve();
    }

    expect(assignments.findIndex(value => value.value === 1)).toBeLessThanOrEqual(9);
    const bulkAssignment = assignments.find(value => value.value === 1)!;
    bulkAssignment.worker.result(bulkAssignment.id, new Uint8Array([1]));
    await bulk;

    await pool.close();
    await Promise.allSettled(latency);
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
