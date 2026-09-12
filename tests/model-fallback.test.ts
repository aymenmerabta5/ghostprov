import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame, type SseFrame } from "../src/relay/sse";
import {
  isDefinitiveModelNotFound,
  wrapModelFallbackHandle,
} from "../src/relay/model-fallback";
import { createProviderErrorEnvelope, type ProviderErrorEnvelope } from "../src/v2/provider-error";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { v2RetainedEventPermitBytes } from "../src/v2/memory/event-memory-sizing";
import { atomicBytePermitOf } from "../src/v2/memory/permit-carrier";
import type { UpstreamPrelude } from "../src/v2/protocol/shared";

const MODEL = "gpt-6-astra";
const TERRA = "gpt-5.6-terra";
const PRELUDE: UpstreamPrelude = {
  status: 200,
  contentType: "text/event-stream",
  upstreamRequestId: "request_primary",
};
const FINAL = { id: "resp_fallback", status: "completed", model: TERRA, output: [] };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}

async function flush(): Promise<void> {
  for (let index = 0; index < 20; index++) await Promise.resolve();
}

async function denial(options: {
  origin?: "http" | "sse";
  status?: number;
  model?: string | null;
  param?: string | null;
  code?: string;
  outerCode?: string;
  ambiguous?: boolean;
} = {}): Promise<V2CommittedError> {
  const providerError = await createProviderErrorEnvelope({
    origin: options.origin ?? "sse",
    status: options.status ?? 404,
    model: options.model === undefined ? MODEL : options.model,
    value: {
      error: {
        message: "The requested model is unavailable",
        type: "invalid_request_error",
        code: options.code ?? "model_not_found",
        param: options.param === undefined ? "model" : options.param,
      },
    },
  });
  return new V2CommittedError(
    "Untrusted free-form text is not classification evidence",
    options.outerCode ?? options.code ?? "model_not_found",
    options.ambiguous ?? false,
    undefined,
    providerError,
  );
}

function completeHandle(): V2RunHandle {
  const handle = new V2RunHandle({});
  handle.settle(FINAL);
  return handle;
}

function chargedFrame(pool: AtomicBytePermitPool, handle: V2RunHandle, data: string): SseFrame {
  const frame = { event: "response.output_text.delta", data };
  const bytes = Buffer.byteLength(sseFrame(frame.event, frame.data));
  const permit = pool.tryAcquire(v2RetainedEventPermitBytes(bytes))!;
  expect(permit).not.toBeNull();
  expect(handle.tryAcceptEvent(frame, bytes, permit)).toBe(true);
  return frame;
}

function framePool(): AtomicBytePermitPool {
  return new AtomicBytePermitPool({
    ordinaryBytes: 4 * v2RetainedEventPermitBytes(1_024),
    jumboBytes: 1,
  });
}

describe("structured model denial classification", () => {
  it("accepts HTTP/SSE denials with exact or absent model and parameter", async () => {
    for (const origin of ["http", "sse"] as const) {
      for (const model of [MODEL, null]) {
        for (const param of ["model", null]) {
          const error = await denial({ origin, model, param, status: origin === "sse" ? 500 : 404 });
          expect(isDefinitiveModelNotFound(error, MODEL)).toBe(true);
        }
      }
    }
  });

  it("does not read the free-form exception message", async () => {
    const error = await denial();
    Object.defineProperty(error, "message", { get() { throw new Error("Message was inspected"); } });
    expect(isDefinitiveModelNotFound(error, MODEL)).toBe(true);
  });

  it("rejects ambiguous, mismatched, generic, nested, and malformed evidence", async () => {
    const valid = await denial();
    const invalid: unknown[] = [
      await denial({ ambiguous: true }),
      await denial({ model: "other-model" }),
      await denial({ param: "previous_response_id" }),
      await denial({ code: "rate_limit_exceeded" }),
      await denial({ outerCode: "provider_error" }),
      new Error("model_not_found"),
      { name: "V2CommittedError", committed: true, ambiguous: false, code: "model_not_found", providerError: valid.providerError },
      Object.assign(new Error("model_not_found"), { cause: valid }),
      Object.assign(new V2CommittedError("denied", "model_not_found", false), { details: { providerError: valid.providerError } }),
      new V2CommittedError("denied", "model_not_found", false),
    ];
    for (const changed of [
      { version: 2 },
      { origin: "local" },
      { status: 200 },
      { model: 123 },
      { unrecognized: true },
      { error: { ...valid.providerError!.error, code: "not_model_not_found" } },
      { error: { ...valid.providerError!.error, param: undefined } },
      { payload: { format: "json", text: "", truncated: "no" } },
    ]) {
      invalid.push(new V2CommittedError("denied", "model_not_found", false, undefined,
        { ...valid.providerError!, ...changed } as ProviderErrorEnvelope));
    }
    for (const error of invalid) expect(isDefinitiveModelNotFound(error, MODEL)).toBe(false);
    expect(isDefinitiveModelNotFound(await denial({ model: TERRA }), TERRA)).toBe(false);
  });
});

describe("bounded model fallback handle", () => {
  for (const origin of ["http", "sse"] as const) {
    it(`replaces an initial ${origin} model denial with one successful Terra response`, async () => {
      const primary = new V2RunHandle({});
      const fallback = new V2RunHandle({});
      const error = await denial({ origin });
      let calls = 0;
      const wrapped = wrapModelFallbackHandle(primary, {
        requestedModel: MODEL,
        startFallback: async reason => {
          expect(reason).toBe(error);
          calls++;
          return fallback;
        },
      });
      const capturedStats = wrapped.handle.stats;
      expect(wrapped.handle.modelFallback).toBeUndefined();
      primary.fail(error);
      await wrapped.decision;
      const frame = { event: "response.output_text.delta", data: "success" };
      await fallback.acceptEvent(frame);
      fallback.settle(FINAL);
      expect(await Array.fromAsync(wrapped.handle.events())).toEqual([frame]);
      expect(await wrapped.handle.final).toBe(FINAL);
      expect(calls).toBe(1);
      expect(wrapped.handle.modelFallback).toEqual({ from: MODEL, to: TERRA });
      expect(capturedStats).toBe(wrapped.handle.stats);
      expect(capturedStats.frames).toBe(1);
      expect(capturedStats.bytes).toBe(fallback.stats.bytes);
      expect(capturedStats.firstByteMs).toBe(fallback.stats.firstByteMs);
      fallback.rateLimit = { remainingTokens: 42 };
      expect(wrapped.handle.rateLimit).toBe(fallback.rateLimit);
      fallback.rateLimit = { remainingTokens: 21 };
      expect(wrapped.handle.rateLimit?.remainingTokens).toBe(21);
    });
  }

  it("withholds the successful primary prelude until the fallback handle is selected", async () => {
    const primary = new V2RunHandle({ expectUpstreamPrelude: true });
    const fallback = new V2RunHandle({ expectUpstreamPrelude: true });
    const start = deferred<RunHandle>();
    primary.acceptUpstreamPrelude(PRELUDE);
    const wrapped = wrapModelFallbackHandle(primary, { requestedModel: MODEL, startFallback: () => start.promise });
    let observedPrelude: UpstreamPrelude | undefined;
    let decided = false;
    void wrapped.handle.upstreamPrelude!.then(value => { observedPrelude = value; });
    void wrapped.decision.then(() => { decided = true; });
    await flush();
    expect(observedPrelude).toBeUndefined();
    primary.fail(await denial());
    await flush();
    expect(wrapped.handle.modelFallback).toEqual({ from: MODEL, to: TERRA });
    expect(observedPrelude).toBeUndefined();
    expect(decided).toBe(false);
    fallback.acceptUpstreamPrelude({ ...PRELUDE, upstreamRequestId: "request_fallback" });
    start.resolve(fallback);
    await wrapped.decision;
    expect(await wrapped.handle.upstreamPrelude).toEqual({ ...PRELUDE, upstreamRequestId: "request_fallback" });
    fallback.settle(FINAL);
    expect(await wrapped.handle.final).toBe(FINAL);
    wrapped.handle.discardUnconsumedEvents?.();
  });

  it("releases the primary prelude after its first metadata event without waiting for final", async () => {
    const primary = new V2RunHandle({ expectUpstreamPrelude: true });
    primary.acceptUpstreamPrelude(PRELUDE);
    let calls = 0;
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { calls++; return completeHandle(); },
    });
    const frame = { event: "response.created", data: "{}" };
    await primary.acceptEvent(frame);
    await wrapped.decision;
    expect(await wrapped.handle.upstreamPrelude).toEqual(PRELUDE);
    const events = wrapped.handle.events();
    expect((await events.next()).value).toBe(frame);
    const error = await denial();
    primary.fail(error);
    expect((await events.next()).done).toBe(true);
    await expect(wrapped.handle.final).rejects.toBe(error);
    expect(calls).toBe(0);
  });

  for (const event of ["response.created", "response.output_text.delta"]) {
    it(`never falls back after an accepted ${event} frame`, async () => {
      const primary = new V2RunHandle({});
      const frame = { event, data: "already accepted" };
      await primary.acceptEvent(frame);
      const error = await denial();
      primary.fail(error);
      let calls = 0;
      const wrapped = wrapModelFallbackHandle(primary, {
        requestedModel: MODEL,
        startFallback: async () => { calls++; return completeHandle(); },
      });
      expect(await Array.fromAsync(wrapped.handle.events())).toEqual([frame]);
      await expect(wrapped.handle.final).rejects.toBe(error);
      expect(calls).toBe(0);
      expect(wrapped.handle.modelFallback).toBeUndefined();
    });
  }

  it("treats the observed first frame as disqualifying even if a handle reports zero stats", async () => {
    const error = await denial();
    const frame = { event: "response.created", data: "{}" };
    let calls = 0;
    const primary: RunHandle = {
      async *events() { yield frame; },
      final: Promise.reject(error),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { calls++; return completeHandle(); },
    });
    expect(await Array.fromAsync(wrapped.handle.events())).toEqual([frame]);
    await expect(wrapped.handle.final).rejects.toBe(error);
    expect(calls).toBe(0);
  });

  it("consults authoritative final after an empty iterator ends", async () => {
    const terminal = deferred<unknown>();
    let calls = 0;
    const primary: RunHandle = {
      async *events() {},
      final: terminal.promise,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { calls++; return completeHandle(); },
    });
    let decided = false;
    void wrapped.decision.then(() => { decided = true; });
    await flush();
    expect(decided).toBe(false);
    terminal.reject(await denial());
    expect(await wrapped.handle.final).toBe(FINAL);
    expect(calls).toBe(1);
  });

  it("returns a second model denial directly without recursively retrying Terra", async () => {
    const primary = new V2RunHandle({});
    const fallback = new V2RunHandle({});
    let calls = 0;
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { calls++; return fallback; },
    });
    primary.fail(await denial());
    await wrapped.decision;
    const second = await denial({ model: TERRA });
    fallback.fail(second);
    expect(await Array.fromAsync(wrapped.handle.events())).toEqual([]);
    await expect(wrapped.handle.final).rejects.toBe(second);
    expect(calls).toBe(1);
  });

  it("does not reinterpret an iterator failure or a generic final as model denial", async () => {
    for (const scenario of ["iterator", "generic", "mismatched"] as const) {
      const iteratorError = new Error("iterator failed");
      const finalError = scenario === "generic" ? new Error("model_not_found") :
        await denial({ model: scenario === "mismatched" ? "other-model" : MODEL });
      let calls = 0;
      const primary: RunHandle = {
        async *events() { if (scenario === "iterator") throw iteratorError; },
        final: Promise.reject(finalError),
        stats: { bytes: 0, firstByteMs: null, frames: 0 },
      };
      const wrapped = wrapModelFallbackHandle(primary, {
        requestedModel: MODEL,
        startFallback: async () => { calls++; return completeHandle(); },
      });
      await expect(wrapped.handle.final).rejects.toBe(finalError);
      if (scenario === "iterator") {
        await expect(wrapped.handle.events().next()).rejects.toBe(iteratorError);
      } else {
        expect(await Array.fromAsync(wrapped.handle.events())).toEqual([]);
      }
      expect(calls).toBe(0);
    }
  });

  it("rejects Terra, cancelled, ambiguous, and partial-stat attempts", async () => {
    for (const scenario of ["terra", "cancelled", "ambiguous", "frames", "bytes"] as const) {
      const primary = new V2RunHandle({});
      const controller = new AbortController();
      if (scenario === "cancelled") controller.abort();
      if (scenario === "frames") primary.stats.frames = 1;
      if (scenario === "bytes") primary.stats.bytes = 1;
      let calls = 0;
      const error = await denial({ ambiguous: scenario === "ambiguous", model: scenario === "terra" ? TERRA : MODEL });
      const wrapped = wrapModelFallbackHandle(primary, {
        requestedModel: scenario === "terra" ? TERRA : MODEL,
        signal: controller.signal,
        startFallback: async () => { calls++; return completeHandle(); },
      });
      primary.fail(error);
      await expect(wrapped.handle.final).rejects.toBe(error);
      expect(calls).toBe(0);
    }
  });

  it("preserves fallback eligibility when a final-only caller discards before selection", async () => {
    const primary = new V2RunHandle({});
    const fallback = new V2RunHandle({ requireTransferredEventMemory: true });
    const pool = framePool();
    const frame = chargedFrame(pool, fallback, "discard me");
    const wrapped = wrapModelFallbackHandle(primary, { requestedModel: MODEL, startFallback: async () => fallback });
    wrapped.handle.discardUnconsumedEvents?.();
    expect(await Array.fromAsync(wrapped.handle.events())).toEqual([]);
    primary.fail(await denial());
    await wrapped.decision;
    fallback.settle(FINAL);
    expect(await wrapped.handle.final).toBe(FINAL);
    expect(atomicBytePermitOf(frame)).toBeNull();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("drains a discarded primary without closing its live event producer", async () => {
    const primary = new V2RunHandle({ requireTransferredEventMemory: true, maxBufferedEvents: 1 });
    const pool = framePool();
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { throw new Error("unexpected fallback"); },
    });
    wrapped.handle.discardUnconsumedEvents?.();
    chargedFrame(pool, primary, "one");
    await wrapped.decision;
    await flush();
    chargedFrame(pool, primary, "two");
    await flush();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    expect(primary.stats.frames).toBe(2);
    primary.settle(FINAL);
    expect(await wrapped.handle.final).toBe(FINAL);
    await flush();
    pool.close();
  });

  it("holds one charged first frame and leaves later events backpressured until consumption", async () => {
    const primary = new V2RunHandle({ requireTransferredEventMemory: true, maxBufferedEvents: 1 });
    const pool = framePool();
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { throw new Error("unexpected fallback"); },
    });
    const first = chargedFrame(pool, primary, "first");
    await wrapped.decision;
    const second = chargedFrame(pool, primary, "second");
    const third = { event: "response.output_text.delta", data: "third" };
    const thirdBytes = Buffer.byteLength(sseFrame(third.event, third.data));
    const thirdPermit = pool.tryAcquire(v2RetainedEventPermitBytes(thirdBytes))!;
    let admitted = false;
    const blocked = primary.acceptEvent(third, thirdBytes, thirdPermit).then(() => { admitted = true; });
    await flush();
    expect(admitted).toBe(false);
    expect(atomicBytePermitOf(first)?.active).toBe(true);
    const events = wrapped.handle.events();
    expect((await events.next()).value).toBe(first);
    expect(atomicBytePermitOf(first)?.active).toBe(true);
    expect(admitted).toBe(false);
    expect((await events.next()).value).toBe(second);
    await blocked;
    expect(atomicBytePermitOf(first)).toBeNull();
    primary.settle(FINAL);
    expect((await events.next()).value).toBe(third);
    expect((await events.next()).done).toBe(true);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    expect(await wrapped.handle.final).toBe(FINAL);
    pool.close();
  });

  it("releases the prefetched permit on abort before any public event", async () => {
    const primary = new V2RunHandle({ requireTransferredEventMemory: true });
    const pool = framePool();
    const controller = new AbortController();
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      signal: controller.signal,
      startFallback: async () => { throw new Error("unexpected fallback"); },
    });
    const first = chargedFrame(pool, primary, "cancelled prefix");
    await wrapped.decision;
    expect(atomicBytePermitOf(first)?.active).toBe(true);
    controller.abort();
    primary.cancel(new Error("cancelled"));
    await expect(wrapped.handle.final).rejects.toThrow("cancelled");
    await flush();
    expect(atomicBytePermitOf(first)).toBeNull();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("keeps decision pending until an in-flight fallback start returns after cancellation", async () => {
    const primary = new V2RunHandle({});
    const fallback = new V2RunHandle({ requireTransferredEventMemory: true });
    const pool = framePool();
    const frame = chargedFrame(pool, fallback, "late fallback");
    const controller = new AbortController();
    const start = deferred<RunHandle>();
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL, signal: controller.signal, startFallback: () => start.promise,
    });
    primary.fail(await denial());
    await flush();
    let decided = false;
    void wrapped.decision.then(() => { decided = true; });
    controller.abort();
    await flush();
    expect(decided).toBe(false);
    start.resolve(fallback);
    await wrapped.decision;
    expect(atomicBytePermitOf(frame)).toBeNull();
    fallback.cancel(new Error("cancelled"));
    await expect(wrapped.handle.final).rejects.toThrow("cancelled");
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("releases an unconsumed prefix when cancellation follows a successful final", async () => {
    const primary = new V2RunHandle({ requireTransferredEventMemory: true });
    const pool = framePool();
    const controller = new AbortController();
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      signal: controller.signal,
      startFallback: async () => { throw new Error("unexpected fallback"); },
    });
    const frame = chargedFrame(pool, primary, "unconsumed final prefix");
    primary.settle(FINAL);
    expect(await wrapped.handle.final).toBe(FINAL);
    expect(atomicBytePermitOf(frame)?.active).toBe(true);
    controller.abort();
    await flush();
    expect(atomicBytePermitOf(frame)).toBeNull();
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("returns fallback startup failures through final and decision", async () => {
    const primary = new V2RunHandle({});
    const failure = new Error("fallback admission rejected");
    const wrapped = wrapModelFallbackHandle(primary, {
      requestedModel: MODEL,
      startFallback: async () => { throw failure; },
    });
    primary.fail(await denial());
    await expect(wrapped.decision).rejects.toBe(failure);
    await expect(wrapped.handle.final).rejects.toBe(failure);
    await expect(wrapped.handle.events().next()).rejects.toBe(failure);
  });
});
