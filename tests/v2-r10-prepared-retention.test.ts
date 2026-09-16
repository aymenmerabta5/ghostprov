import { describe, expect, it, spyOn } from "bun:test";
import type { Clock } from "../src/core/ports";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { AdmissionController } from "../src/v2/sidecar/admission";
import { RequestAssembler } from "../src/v2/sidecar/assembler";
import { ClaimJournal } from "../src/v2/sidecar/journal";
import { SidecarEngine } from "../src/v2/sidecar/r10-engine";
import { base64UrlEncode, sha256Hex, utf8Bytes, type R2RequestStartFrame, type SidecarFrame } from "../src/v2/protocol/shared";
import { completedUpstream, FakeSidecarStorage, TEST_R2_BINDING, TEST_R2_HOST_INSTANCE, TEST_R2_SIDECAR_INSTANCE } from "./support/fake-v2-sidecar";

const RETENTION_MS = 360_000;
const BODY = Object.freeze({ model: "gpt-6-astra", input: "offline retained delta".repeat(100),
  previous_response_id: "resp_exact_prior", stream: true, store: true });

function fixture(preparedTimeoutMs?: number) {
  let now = 0;
  const timers = new Map<object, { at: number; run(): void }>();
  const set = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void,
    delay = 0, ...args: unknown[]) => {
    const handle = { unref() { return handle; } };
    timers.set(handle, { at: now + delay, run: () => callback(...args) });
    return handle;
  }) as typeof setTimeout);
  const clear = spyOn(globalThis, "clearTimeout").mockImplementation((handle => {
    timers.delete(handle as object);
  }) as typeof clearTimeout);
  const sleep = (ms: number, signal?: AbortSignal): Promise<void> => {
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const onAbort = () => { clearTimeout(timer); reject(signal?.reason); };
      const timer = setTimeout(() => { signal?.removeEventListener("abort", onAbort); resolve(); }, ms);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  };
  const clock: Clock = { now: () => now, sleep,
    waitUntil: (deadline, signal) => sleep(Math.max(0, deadline - now), signal) };
  const permits = new AtomicBytePermitPool({ ordinaryBytes: 16 * 1024 * 1024, jumboBytes: 0, maxWaiters: 2 });
  const admission = new AdmissionController({ physicalMax: 1, acceptedMax: 1, hostCeiling: 1,
    memoryBudgetBytes: 16 * 1024 * 1024, clock });
  const storage = new FakeSidecarStorage([]);
  const begin = storage.beginTerminalSpool.bind(storage);
  let openSpools = 0;
  let abortedSpools = 0;
  storage.beginTerminalSpool = async (...args) => {
    const spool = await begin(...args);
    openSpools++;
    let released = false;
    const release = () => { if (!released) { released = true; openSpools--; } };
    return { ...spool,
      get byteCount() { return spool.byteCount; },
      async promote(proof) { await spool.promote(proof); release(); },
      async abort() { await spool.abort(); abortedSpools++; release(); },
    };
  };
  const journal = new ClaimJournal({ storage, flushIntervalMs: 0, maxBatch: 64, clock });
  const frames: SidecarFrame[] = [];
  let providerCalls = 0;
  const engine = new SidecarEngine({ binding: TEST_R2_BINDING,
    assembler: new RequestAssembler({ atomicBytePermits: permits }), journal, admission,
    atomicBytePermits: permits, assertExecutionFence: () => {}, preparedTimeoutMs,
    upstream: { async execute(input) { providerCalls++; return completedUpstream(input); } },
    emit: frame => { frames.push(frame); },
  });
  const flush = async () => {
    for (let turn = 0; turn < 100; turn++) {
      for (const [handle, timer] of [...timers]) {
        if (timer.at <= now) { timers.delete(handle); timer.run(); }
      }
      await Promise.resolve();
    }
    await Bun.sleep(0);
  };
  const settle = async <T>(promise: Promise<T>): Promise<T> => {
    let done = false;
    void promise.then(() => { done = true; }, () => { done = true; });
    for (let turn = 0; turn < 100 && !done; turn++) await flush();
    if (!done) throw new Error("Offline engine operation did not settle without advancing time");
    return promise;
  };
  const prepare = async (requestId = "prepared-r10-1"): Promise<R2RequestStartFrame> => {
    await settle(engine.start());
    const bytes = utf8Bytes(JSON.stringify(BODY));
    const manifest = { requestDigest: await sha256Hex(bytes), decodedBytes: bytes.byteLength,
      encodedBytes: bytes.byteLength, encoding: "identity" as const, partCount: 1 };
    const fence = { requestId, ...TEST_R2_BINDING, protocolRevision: "r2" as const, schemaVersion: 2 as const,
      hostInstanceId: TEST_R2_HOST_INSTANCE, targetInstanceId: TEST_R2_SIDECAR_INSTANCE, ...manifest };
    await settle(engine.accept({ kind: "request_open", requestId, ...manifest, epoch: 1, sequence: 0 }));
    await settle(engine.accept({ kind: "request_data", requestId, partIndex: 0, offset: 0,
      dataBase64Url: base64UrlEncode(bytes), epoch: 1, sequence: 1 }));
    await settle(engine.accept({ kind: "request_commit", ...fence, epoch: 1, sequence: 2 }));
    return { kind: "request_start", ...fence };
  };
  const released = () => {
    expect(engine.snapshot()).toMatchObject({ active: 0, queued: 0, ambiguous: 0, replaying: 0 });
    expect(admission.snapshot()).toMatchObject({ active: 0, queued: 0, reservedBytes: 0 });
    expect(permits.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0, jumboUsedBytes: 0, waiters: 0 });
    expect(openSpools).toBe(0);
  };
  return { engine, admission, permits, storage, journal, frames, timers, prepare, settle, released,
    providerCalls: () => providerCalls, openSpools: () => openSpools, abortedSpools: () => abortedSpools,
    async advance(ms: number) { now += ms; await flush(); await flush(); },
    async finishExecution() {
      for (let turn = 0; turn < 100 && engine.snapshot().active > 0; turn++) await flush();
      expect(engine.snapshot().active).toBe(0);
    },
    async close() {
      try { await settle(engine.drain("offline_cleanup")); await settle(journal.close()); released(); expect(timers.size).toBe(0); }
      finally { set.mockRestore(); clear.mockRestore(); }
    },
  };
}

describe("R10 prepared retention", () => {
  it.each([60_001, 180_001, RETENTION_MS - 1])("retains one exact prepared request for a delayed start at %i ms", async delay => {
    const test = fixture();
    try {
      const start = await test.prepare();
      const held = test.permits.snapshot().ordinaryUsedBytes;
      expect(held).toBeGreaterThan(0);
      expect(test.admission.snapshot().active).toBe(1);
      expect(test.openSpools()).toBe(1);
      await test.advance(delay);
      expect(test.frames.filter(frame => frame.kind === "response_error")).toEqual([]);
      expect(test.permits.snapshot().ordinaryUsedBytes).toBe(held);
      expect(test.providerCalls()).toBe(0);
      const frame = { ...start, epoch: 1, sequence: 3 };
      await test.settle(Promise.all([test.engine.accept(frame), test.engine.accept(frame)]));
      await test.finishExecution();
      expect(test.providerCalls()).toBe(1);
      expect(test.frames.some(frame => frame.kind === "response_terminal")).toBe(true);
      test.released();
    } finally { await test.close(); }
  });

  it.each([
    { name: "360-second default", override: undefined },
    { name: "5-millisecond override", override: 5 },
    { name: "60-second override", override: 60_000 },
  ])("expires at the exact $name timer boundary", async ({ override }) => {
    const test = fixture(override);
    const timeout = override ?? RETENTION_MS;
    try {
      const start = await test.prepare();
      await test.advance(timeout - 1);
      expect(test.engine.snapshot().active).toBe(1);
      expect(test.openSpools()).toBe(1);
      await test.advance(1);
      expect(test.frames).toContainEqual(expect.objectContaining({ kind: "response_error",
        requestId: start.requestId, code: "prepared_expired", ambiguous: false }));
      expect(test.journal.status(start.requestId)).toBe("cancelled");
      expect(test.providerCalls()).toBe(0);
      expect(test.abortedSpools()).toBe(1);
      test.released();
      await test.settle(test.engine.accept({ ...start, epoch: 1, sequence: 3 }));
      expect(test.providerCalls()).toBe(0);
      expect(test.storage.records.some(record => record.state === "fetch_starting")).toBe(false);
      await test.settle(test.engine.acknowledgeDeliveredOutcomes([start.requestId]));
      expect(test.journal.status(start.requestId)).toBe("acked");
      test.released();
    } finally { await test.close(); }
  });

  it.each(["cancel", "drain"] as const)("%s releases prepared permits immediately without provider execution", async mode => {
    const test = fixture();
    try {
      const start = await test.prepare();
      await test.advance(1_000);
      const operation = mode === "cancel"
        ? test.engine.accept({ kind: "request_cancel", requestId: start.requestId,
          reason: "offline_cancel", epoch: 1, sequence: 3 })
        : test.engine.drain("offline_cancel");
      await test.settle(operation);
      expect(test.journal.status(start.requestId)).toBe("cancelled");
      expect(test.providerCalls()).toBe(0);
      expect(test.abortedSpools()).toBe(1);
      test.released();
      await test.advance(RETENTION_MS);
      expect(test.frames.some(frame => frame.kind === "response_error" && frame.code === "prepared_expired")).toBe(false);
      expect(test.providerCalls()).toBe(0);
    } finally { await test.close(); }
  });

  it("does not renew retention on duplicate commit or raise admission and byte ceilings", async () => {
    const test = fixture();
    try {
      const start = await test.prepare();
      const retainedBytes = test.permits.snapshot().ordinaryUsedBytes;
      await test.advance(300_000);
      await test.settle(test.engine.accept({ ...start, kind: "request_commit", epoch: 1, sequence: 2 }));
      await test.prepare("prepared-r10-over-capacity");
      expect(test.admission.snapshot().active).toBe(1);
      expect(test.permits.snapshot().ordinaryUsedBytes).toBe(retainedBytes);
      expect(test.openSpools()).toBe(1);
      expect(test.frames.some(frame => frame.kind === "request_prepared" && frame.requestId === "prepared-r10-over-capacity")).toBe(false);
      await test.advance(59_999);
      expect(test.engine.snapshot().active).toBe(1);
      await test.advance(1);
      expect(test.providerCalls()).toBe(0);
      expect(test.journal.status(start.requestId)).toBe("cancelled");
      test.released();
    } finally { await test.close(); }
  });
});
