import { describe, expect, it, spyOn } from "bun:test";
import type { Clock } from "../src/core/ports";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { incomingLeaseTimingView } from "../src/http/request-lease";
import type { RunHandle } from "../src/relay/run-handle";
import { ResourceGovernor } from "../src/router/resource-governor";
import { MultiSandboxRouter, type IncomingResourceLease } from "../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import { createGhostProvApp } from "../src/server/app";
import { V2CommittedError, V2PreCommitError } from "../src/v2/transport/run-handle";
import { finalResponse, messageOutput, textPart } from "./support/anthropic-stream";

const MiB = 1024 ** 2;
const MODEL = "gpt-5.6-terra";
const QUEUE_MS = 1_000;
const ROOT = { type: "message", role: "user", content: [{ type: "input_text", text: "root" }] };
const NEXT = { type: "message", role: "user", content: [{ type: "input_text", text: "next" }] };

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}

async function flush(): Promise<void> {
  for (let index = 0; index < 100; index++) await Promise.resolve();
  await new Promise<void>(resolve => setImmediate(resolve));
  for (let index = 0; index < 100; index++) await Promise.resolve();
}

function manualTimers() {
  let now = 100_000;
  const pending = new Map<object, { at: number; callback(): void }>();
  const set = spyOn(globalThis, "setTimeout").mockImplementation(((
    callback: (...args: unknown[]) => void, delay = 0, ...args: unknown[]
  ) => {
    const handle = { unref() { return handle; } };
    pending.set(handle, { at: now + Math.max(0, delay), callback: () => callback(...args) });
    return handle;
  }) as typeof setTimeout);
  const clear = spyOn(globalThis, "clearTimeout").mockImplementation(handle => {
    pending.delete(handle as unknown as object);
  });
  const clock: Clock = {
    now: () => now,
    sleep: (ms, signal) => new Promise<void>((resolve, reject) => {
      const abort = () => {
        clearTimeout(timer);
        reject(signal?.reason ?? new DOMException("Aborted", "AbortError"));
      };
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", abort);
        resolve();
      }, ms);
      if (signal?.aborted) abort();
      else signal?.addEventListener("abort", abort, { once: true });
    }),
    waitUntil: (deadline, signal) => clock.sleep(Math.max(0, deadline - now), signal),
  };
  return {
    clock,
    async advanceTo(elapsedMs: number) {
      const target = 100_000 + elapsedMs;
      if (target < now) throw new Error("Clock cannot move backwards");
      for (;;) {
        const next = [...pending].filter(([, value]) => value.at <= target)
          .sort((left, right) => left[1].at - right[1].at)[0];
        if (!next) break;
        now = next[1].at;
        pending.delete(next[0]);
        next[1].callback();
        await flush();
      }
      now = target;
      await flush();
    },
    restore() { set.mockRestore(); clear.mockRestore(); },
  };
}

type Final = ReturnType<typeof finalResponse>;

class ControlledRuntime implements RoutableSandboxRuntime {
  readonly projectId = projectId("00000000-0000-4000-8000-000000000001");
  readonly calls: Array<{
    body: Record<string, unknown>;
    signal: AbortSignal | undefined;
    reflectExecutionRisk(): void;
    returnHandle(): void;
    complete(value: Final): void;
  }> = [];
  async start() {}
  async drain() {}
  async stop() {}
  snapshot() {
    return {
      projectId: this.projectId, incarnation: 1, state: "ready", capacity: 1,
      active: 0, available: 1, quarantined: 0, heartbeatAgeMs: 0,
      credentialExpiresInMs: 3_600_000, ackEwmaMs: 10, completionEwmaMs: 100,
      recentFailureRate: 0, queueDepth: 0, memoryPressure: 0, failure: null,
    };
  }
  async run(body: Record<string, unknown>, signal?: AbortSignal, options?: SandboxRunOptions) {
    const committed = deferred<RunHandle>();
    const final = deferred<Final>();
    let executionRisk = false;
    const stats: RunHandle["stats"] = { bytes: 0, frames: 0, firstByteMs: null };
    const handle: RunHandle = { async *events() {}, final: final.promise, stats };
    const abort = () => {
      const error = executionRisk
        ? new V2CommittedError("Controlled caller cancellation", "test_cancelled", true)
        : new V2PreCommitError("Controlled pre-execution cancellation", "test_precommit");
      committed.reject(error);
      final.reject(error);
    };
    signal?.addEventListener("abort", abort, { once: true });
    void final.promise.finally(() => signal?.removeEventListener("abort", abort)).catch(() => undefined);
    this.calls.push({
      body, signal,
      reflectExecutionRisk() { executionRisk = true; options?.onExecutionRiskReflected?.(); },
      returnHandle() { committed.resolve(handle); },
      complete(value) { stats.bytes = Buffer.byteLength(JSON.stringify(value)); final.resolve(value); },
    });
    options?.onAdmissionReflected?.();
    return committed.promise;
  }
}

function fixture() {
  const timers = manualTimers();
  const runtime = new ControlledRuntime();
  const governor = new ResourceGovernor({
    now: timers.clock.now,
    probe: { sample: () => ({
      totalBytes: 32 * 1024 ** 3, freeBytes: 20 * 1024 ** 3,
      rssBytes: 320 * MiB, heapBytes: 100 * MiB, externalBytes: 20 * MiB,
      logicalProcessors: 12,
    }) },
  });
  const authorizationScope = createAuthenticatedTenantScope();
  const router = new MultiSandboxRouter({
    registry: new SandboxRegistry([runtime]), governor, authorizationScope,
    clock: timers.clock, queueTimeoutMs: QUEUE_MS,
  });
  const app = createGhostProvApp({
    router, authorizationScope,
    anthropicModels: {
      opus: MODEL, sonnet: MODEL, haiku: MODEL, fallback: MODEL, passthrough: new Set([MODEL]),
    },
  });
  return {
    timers, runtime, governor, router, app,
    async close() {
      try { await router.close(); } finally { timers.restore(); }
    },
  };
}

type Lane = "native" | "anthropic";

async function post(
  system: ReturnType<typeof fixture>,
  lane: Lane,
  body: Record<string, unknown>,
  incomingLease?: IncomingResourceLease,
  signal?: AbortSignal,
) {
  return system.app.fetch(new Request(`http://localhost/v1/${lane === "native" ? "responses" : "messages"}`, {
    method: "POST",
    headers: { "content-type": "application/json", "anthropic-version": "2023-06-01" },
    body: JSON.stringify(body), signal,
  }), { incomingBodyLease: incomingLease });
}

async function startPrefixHit(system: ReturnType<typeof fixture>, lane: Lane, signal?: AbortSignal) {
  const parent = finalResponse("resp_parent", [messageOutput("msg_parent", [textPart("parent")])]);
  const firstBody = lane === "native"
    ? { model: MODEL, stream: false, input: [ROOT] }
    : { model: MODEL, stream: false, max_tokens: 64, messages: [{ role: "user", content: "root" }] };
  const first = post(system, lane, firstBody);
  await flush();
  expect(system.runtime.calls).toHaveLength(1);
  system.runtime.calls[0]!.reflectExecutionRisk();
  system.runtime.calls[0]!.returnHandle();
  system.runtime.calls[0]!.complete(parent);
  const parentResponse = await first;
  expect(parentResponse.status).toBe(200);
  const parentPublic = await parentResponse.json() as { content: unknown[] };
  await flush();
  expect(system.governor.snapshot().reservedBytes).toBe(0);

  const body = lane === "native"
    ? { model: MODEL, stream: false, input: [ROOT, ...parent.output, NEXT] }
    : { model: MODEL, stream: false, max_tokens: 64, messages: [
        { role: "user", content: "root" },
        { role: "assistant", content: parentPublic.content },
        { role: "user", content: "next" },
      ] };
  const incoming = await system.router.acquireIncomingBody(Buffer.byteLength(JSON.stringify(body)), signal);
  const ingressBytes = incoming.reservedBytes!;
  const pending = post(system, lane, body, incoming, signal);
  await flush();
  expect(system.runtime.calls).toHaveLength(2);
  const attempt = system.runtime.calls[1]!;
  expect(attempt.body).toMatchObject({ previous_response_id: "resp_parent", stream: true, store: true });
  expect(attempt.body.input).toHaveLength(1);
  return { incoming, ingressBytes, pending, attempt };
}

describe("inferred continuation admission ownership", () => {
  for (const lane of ["native", "anthropic"] as const) {
    it(`${lane} keeps full-history memory while a committed handle crosses the ingress deadline`, async () => {
      const system = fixture();
      try {
        const { incoming, ingressBytes, pending, attempt } = await startPrefixHit(system, lane);
        await system.timers.advanceTo(700);
        attempt.reflectExecutionRisk();
        expect(incoming.reservedBytes).toBe(ingressBytes);
        await system.timers.advanceTo(1_100);
        expect(attempt.signal?.aborted).toBe(false);
        expect(incoming.signal?.aborted).toBe(false);
        expect(incoming.reservedBytes).toBe(ingressBytes);
        expect(system.governor.snapshot().reservedBytes).toBeGreaterThanOrEqual(ingressBytes);
        attempt.returnHandle();
        await flush();
        expect(incoming.reservedBytes).toBe(ingressBytes);
        attempt.complete(finalResponse("resp_child", [messageOutput("msg_child", [textPart("child")])]));
        const response = await pending;
        expect(response.status).toBe(200);
        await response.json();
        await flush();
        expect(system.runtime.calls).toHaveLength(2);
        expect(incoming.reservedBytes).toBe(0);
        expect(system.governor.snapshot().reservedBytes).toBe(0);
      } finally { await system.close(); }
    });

    for (const afterHandle of [false, true]) {
      it(`${lane} preserves caller cancellation ${afterHandle ? "after" : "before"} the committed handle`, async () => {
        const system = fixture();
        const caller = new AbortController();
        try {
          const { incoming, ingressBytes, pending, attempt } = await startPrefixHit(system, lane, caller.signal);
          await system.timers.advanceTo(700);
          attempt.reflectExecutionRisk();
          if (afterHandle) {
            await system.timers.advanceTo(1_100);
            attempt.returnHandle();
            await flush();
          }
          expect(incoming.reservedBytes).toBe(ingressBytes);
          caller.abort(new DOMException("Caller disconnected", "AbortError"));
          const response = await pending;
          expect(response.status).toBeGreaterThanOrEqual(400);
          expect(attempt.signal?.aborted).toBe(true);
          await response.text();
          await flush();
          expect(system.runtime.calls).toHaveLength(2);
          expect(incoming.reservedBytes).toBe(0);
          expect(system.governor.snapshot().reservedBytes).toBe(0);
        } finally { await system.close(); }
      });
    }

    it(`${lane} retains the original admission deadline before execution risk`, async () => {
      const system = fixture();
      try {
        const { incoming, pending, attempt } = await startPrefixHit(system, lane);
        await system.timers.advanceTo(QUEUE_MS);
        const response = await pending;
        expect(response.status).toBeGreaterThanOrEqual(400);
        expect(attempt.signal?.aborted).toBe(true);
        await response.text();
        await flush();
        expect(system.runtime.calls).toHaveLength(2);
        expect(incoming.reservedBytes).toBe(0);
        expect(system.governor.snapshot().reservedBytes).toBe(0);
      } finally { await system.close(); }
    });
  }

  it("ending retained ingress admission preserves its original disconnect signal until release", async () => {
    const system = fixture();
    const caller = new AbortController();
    try {
      const incoming = await system.router.acquireIncomingBody(1_024, caller.signal);
      const bytes = incoming.reservedBytes;
      incomingLeaseTimingView(incoming)!.completeAdmission!();
      await system.timers.advanceTo(QUEUE_MS + 100);
      expect(incoming.signal?.aborted).toBe(false);
      expect(incoming.reservedBytes).toBe(bytes);
      caller.abort(new DOMException("Caller disconnected", "AbortError"));
      expect(incoming.signal?.aborted).toBe(true);
      incoming.release();
      expect(system.governor.snapshot().reservedBytes).toBe(0);
    } finally { await system.close(); }
  });
});
