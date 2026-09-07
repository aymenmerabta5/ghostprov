import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import { ResourceGovernor } from "../src/router/resource-governor";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  V2_EVENT_SOFT_BUFFER_BYTES,
  v2EventPhysicalWorkingSetBytes,
  v2RetainedEventPermitBytes,
} from "../src/v2/memory/event-memory-sizing";
import { attachAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import { V2RunHandle } from "../src/v2/transport/run-handle";

const MiB = 1024 * 1024;
const LARGE_IMAGE_BYTES = 700 * 1024;
const PNG = new Uint8Array([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
  0x00, 0x00, 0x00, 0x0d,
]);

function flushTasks(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve));
}

function imageFinal(index: number, result: string) {
  return {
    id: `resp_image_${index}`,
    object: "response",
    status: "completed",
    created_at: 100 + index,
    output: [{
      id: `ig_${index}`,
      type: "image_generation_call",
      status: "completed",
      result,
      quality: "high",
      output_format: "png",
      size: "1024x1024",
    }],
  };
}

function harness(options: {
  n?: number;
  imageBytes?: number;
  route?: "generations" | "edits";
  denyReservationAfter?: number;
  ignoreAbortFor?: number;
} = {}) {
  const n = options.n ?? 2;
  const imageBytes = options.imageBytes ?? LARGE_IMAGE_BYTES;
  const pool = new AtomicBytePermitPool({ ordinaryBytes: 32 * MiB, jumboBytes: 32 * MiB });
  const governor = new ResourceGovernor({
    probe: { sample: () => ({
      totalBytes: 16 * 1024 ** 3,
      freeBytes: 12 * 1024 ** 3,
      rssBytes: 256 * MiB,
      heapBytes: 64 * MiB,
      externalBytes: 8 * MiB,
      logicalProcessors: 8,
    }) },
  });
  const controller = new AbortController();
  const handles: V2RunHandle[] = [];
  const bodies: Record<string, unknown>[] = [];
  const workers: Promise<void>[] = [];
  const reservations: number[] = [];
  const reservationJumboStates: boolean[] = [];
  const router: RouterLike = {
    async startRelay(body, requestOptions): Promise<RunHandle> {
      bodies.push(body);
      const handle = new V2RunHandle({ requireTransferredEventMemory: true });
      handles.push(handle);
      if (handles.length - 1 !== options.ignoreAbortFor) {
        requestOptions?.signal?.addEventListener("abort", () => {
          handle.cancel(new DOMException("Test client disconnected", "AbortError"));
        }, { once: true });
      }
      return handle;
    },
    snapshot: () => ({ capacity: 10, active: 0, available: 10, queued: 0, runtimes: [], resources: {} }),
  };
  const app = createGhostProvApp({
    router,
    reserveImageResult: reservedBytes => {
      reservationJumboStates.push(pool.snapshot().jumboActive);
      reservations.push(reservedBytes);
      return options.denyReservationAfter !== undefined && reservations.length > options.denyReservationAfter
        ? null : governor.tryAcquire({
        class: "image-generation", requestBytes: 0, reservedBytes, stream: false,
      });
    },
  });
  const route = options.route ?? "generations";
  const form = new FormData();
  form.append("prompt", "offline image fixture");
  form.append("n", String(n));
  form.append("image", new File([PNG], "input.png", { type: "image/png" }));
  const pending = Promise.resolve(app.request(`/v1/images/${route}`, {
    method: "POST",
    ...(route === "generations" ? {
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-image-2", prompt: "offline image fixture", n, quality: "high" }),
    } : { body: form }),
    signal: controller.signal,
  }));
  let response: Response | undefined;
  void pending.then(value => { response = value; });

  return {
    pool, governor, controller, handles, bodies, reservations, reservationJumboStates, pending,
    get response() { return response; },
    complete(index: number, status = "completed", proof?: Promise<void>): Promise<void> {
      const worker = (async () => {
        const empty = imageFinal(index, "");
        empty.status = status;
        const event = "response.completed";
        const emptyData = JSON.stringify({ type: event, response: empty });
        const wireBytes = new TextEncoder().encode(sseFrame(event, emptyData)).byteLength + imageBytes;
        const permit = await pool.acquire(v2EventPhysicalWorkingSetBytes(wireBytes), {
          region: wireBytes > V2_EVENT_SOFT_BUFFER_BYTES ? "jumbo" : "ordinary",
          signal: controller.signal,
        });
        try {
          const final = { ...empty, output: imageFinal(index, String(index).repeat(imageBytes)).output };
          const frame = { event, data: JSON.stringify({ type: event, response: final }) };
          permit.shrink(v2RetainedEventPermitBytes(wireBytes));
          await handles[index]!.acceptEvent(frame, wireBytes, permit, undefined, controller.signal);
          await proof;
          handles[index]!.settle(final);
        } catch (error) {
          permit.release();
          throw error;
        }
      })();
      workers.push(worker);
      void worker.catch(() => undefined);
      return worker;
    },
    async close() {
      controller.abort();
      pool.close();
      await Promise.allSettled(workers);
      const result = await pending;
      if (!result.bodyUsed) await result.body?.cancel();
      await flushTasks();
      governor.close();
    },
  };
}

describe("image batch result memory", () => {
  it.each(["generations", "edits"] as const)("completes two large %s results through one shared jumbo lane", async route => {
    const test = harness({ route });
    try {
      await flushTasks();
      expect(test.bodies).toHaveLength(2);
      expect(test.bodies.every(body => body.stream === true && body.store === true)).toBe(true);
      // Both jobs are already started. The second result arrives first; each
      // terminal uses the same exclusive slot as a real V2 image event.
      void test.complete(1);
      void test.complete(0);
      await flushTasks();

      expect(test.response).toBeDefined();
      expect(test.response!.status).toBe(200);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, jumboActive: false, waiters: 0 });
      // An undrained image batch must not retain either event-memory region or
      // prevent another request from using the shared large-event slot.
      const nextEvent = test.pool.tryAcquire(MiB, { region: "jumbo" });
      expect(nextEvent).not.toBeNull();
      nextEvent!.release();
      const retainedBytes = test.reservations.reduce((sum, bytes) => sum + bytes, 0);
      expect(test.reservations).toHaveLength(2);
      expect(test.reservationJumboStates).toEqual([true, true]);
      expect(test.governor.snapshot().reservedBytes).toBe(retainedBytes);

      const reader = test.response!.body!.getReader();
      const first = await reader.read();
      const result = JSON.parse(new TextDecoder().decode(first.value));
      expect(result.data.map((item: { b64_json: string }) => item.b64_json[0])).toEqual(["0", "1"]);
      expect(result.data.every((item: { b64_json: string }) => item.b64_json.length === LARGE_IMAGE_BYTES)).toBe(true);
      expect(test.governor.snapshot().reservedBytes).toBe(retainedBytes);
      await expect(reader.read()).resolves.toEqual({ done: true, value: undefined });
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await test.close();
    }
  });

  it("handles the maximum ten-image batch without serializing provider starts", async () => {
    const test = harness({ n: 10 });
    try {
      await flushTasks();
      expect(test.bodies).toHaveLength(10);
      for (let index = 9; index >= 0; index--) void test.complete(index);
      await flushTasks();
      expect(test.response?.status).toBe(200);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
      const result = await test.response!.json();
      expect(result.data.map((item: { b64_json: string }) => item.b64_json[0]))
        .toEqual(Array.from({ length: 10 }, (_, index) => String(index)));
      expect(test.reservations).toHaveLength(10);
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await test.close();
    }
  });

  it.each([
    { n: 1, imageBytes: LARGE_IMAGE_BYTES },
    { n: 2, imageBytes: 64 * 1024 },
  ])("preserves the existing single-image and ordinary-event paths: %j", async options => {
    const test = harness(options);
    try {
      await flushTasks();
      for (let index = 0; index < options.n; index++) void test.complete(index);
      await flushTasks();
      expect(test.response?.status).toBe(200);
      expect(test.reservations).toEqual([]);
      expect(test.pool.snapshot().activePermits).toBe(options.n);
      expect((await test.response!.json()).data).toHaveLength(options.n);
      expect(test.pool.snapshot().activePermits).toBe(0);
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await test.close();
    }
  });

  it("keeps the jumbo charge until authoritative final proof settles", async () => {
    const test = harness();
    let prove!: () => void;
    const proof = new Promise<void>(resolve => { prove = resolve; });
    try {
      await flushTasks();
      void test.complete(0, "completed", proof);
      void test.complete(1);
      await flushTasks();
      expect(test.response).toBeUndefined();
      expect(test.reservations).toEqual([]);
      expect(test.pool.snapshot()).toMatchObject({ jumboActive: true, jumboWaiters: 1 });
      prove();
      await flushTasks();
      expect(test.response?.status).toBe(200);
      await test.response!.body!.cancel();
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      prove();
      await test.close();
    }
  });

  it.each([false, true])("releases retained results on HTTP cancellation (read first: %s)", async readFirst => {
    const test = harness();
    try {
      await flushTasks();
      void test.complete(0);
      void test.complete(1);
      await flushTasks();
      const reader = test.response!.body!.getReader();
      if (readFirst) await reader.read();
      expect(test.governor.snapshot().reservedBytes).toBeGreaterThan(0);
      await reader.cancel();
      expect(test.governor.snapshot().reservedBytes).toBe(0);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
    } finally {
      await test.close();
    }
  });

  it("releases an earlier result when a sibling is cancelled, including a late final", async () => {
    const test = harness({ ignoreAbortFor: 1 });
    try {
      await flushTasks();
      await test.complete(0);
      await flushTasks();
      expect(test.response).toBeUndefined();
      expect(test.governor.snapshot().reservedBytes).toBeGreaterThan(0);
      test.controller.abort();
      await flushTasks();
      expect(test.response?.status).toBe(499);
      expect(test.governor.snapshot().reservedBytes).toBe(0);

      const latePermit = test.pool.tryAcquire(8 * MiB, { region: "jumbo" })!;
      const late = attachAtomicBytePermit(imageFinal(1, "late"), latePermit);
      test.handles[1]!.settle(late);
      await flushTasks();
      expect(test.reservations).toHaveLength(1);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      test.handles[1]?.cancel(new DOMException("Test cleanup", "AbortError"));
      await test.close();
    }
  });

  it.each(["relay", "malformed"] as const)("releases retained siblings after a %s failure", async failure => {
    const test = harness();
    try {
      await flushTasks();
      await test.complete(0);
      await flushTasks();
      expect(test.governor.snapshot().reservedBytes).toBeGreaterThan(0);
      if (failure === "relay") test.handles[1]!.fail(new Error("fixture failure"));
      else await test.complete(1, "incomplete");
      await flushTasks();
      expect(test.response?.status).toBe(failure === "relay" ? 500 : 502);
      expect(test.response!.headers.get("x-should-retry")).toBeNull();
      expect(test.governor.snapshot().reservedBytes).toBe(0);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
      expect(test.bodies).toHaveLength(2);
    } finally {
      await test.close();
    }
  });

  it.each([0, 1])("fails without waiting or inviting a replay after %s result reservations", async denyReservationAfter => {
    const test = harness({ denyReservationAfter });
    try {
      await flushTasks();
      void test.complete(0);
      void test.complete(1);
      await flushTasks();
      expect(test.response?.status).toBe(503);
      expect(test.response!.headers.get("x-should-retry")).toBe("false");
      expect(await test.response!.json()).toMatchObject({ error: { code: "image_result_memory_exhausted" } });
      expect(test.bodies).toHaveLength(2);
      expect(test.pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
      expect(test.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await test.close();
    }
  });
});
