import { afterEach, describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { ResponsesHttpContinuationIndex } from "../src/http/responses-continuation";
import type { ObservabilityEvent } from "../src/observability/events";
import type { LoggerLike } from "../src/observability/logger";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import type { RunHandle } from "../src/relay/run-handle";
import type { SseFrame } from "../src/relay/sse";
import {
  ResourceGovernor,
  type ResourceAcquireOptions,
  type ResourceLease,
} from "../src/router/resource-governor";
import { MultiSandboxRouter } from "../src/router/router";
import type { WorkloadDescriptor } from "../src/router/workload";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import { createGhostProvApp } from "../src/server/app";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
import { V2CommittedError, V2RunHandle } from "../src/v2/transport/run-handle";

const TERRA = "gpt-5.6-terra";
const NOW = 1_000;
const cleanups: Array<() => void | Promise<void>> = [];

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(yes => { resolve = yes; });
  return { promise, resolve };
}

// These scenarios advance through explicit runtime gates, never real time.
const clock: Clock = {
  now: () => NOW,
  sleep: (_ms, signal) => new Promise<void>((_resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
  }),
  waitUntil(deadlineMs, signal) {
    return deadlineMs <= NOW ? Promise.resolve() : this.sleep(deadlineMs - NOW, signal);
  },
};

async function flush(): Promise<void> {
  for (let index = 0; index < 60; index++) await Promise.resolve();
}

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  emit(event: ObservabilityEvent): void { this.events.push(event); }
  snapshot() {
    return { queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 };
  }
  async close(): Promise<void> {}
}

interface Reservation {
  readonly initialBytes: number;
  readonly replacements: number[];
  released: boolean;
  releases: number;
}

class TrackedGovernor extends ResourceGovernor {
  readonly reservations: Reservation[] = [];

  constructor() {
    super({
      now: clock.now,
      probe: { sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 20 * 1024 ** 3,
        rssBytes: 256 * 1024 ** 2,
        heapBytes: 64 * 1024 ** 2,
        externalBytes: 8 * 1024 ** 2,
        logicalProcessors: 8,
      }) },
    });
  }

  override async acquire(
    workload: WorkloadDescriptor,
    signal?: AbortSignal,
    options?: ResourceAcquireOptions,
  ): Promise<ResourceLease> {
    const lease = await super.acquire(workload, signal, options);
    const reservation = { initialBytes: lease.reservedBytes, replacements: [], released: false, releases: 0 };
    this.reservations.push(reservation);
    return this.track(lease, reservation);
  }

  private track(lease: ResourceLease, reservation: Reservation): ResourceLease {
    return {
      get reservedBytes() { return lease.reservedBytes; },
      release: () => {
        reservation.released = true;
        reservation.releases++;
        lease.release();
      },
      replace: async (workload, signal, options) => {
        const replacement = await lease.replace(workload, signal, options);
        reservation.replacements.push(replacement.reservedBytes);
        return this.track(replacement, reservation);
      },
    };
  }
}

interface RuntimeCall {
  readonly body: Record<string, unknown>;
  readonly signal?: AbortSignal;
  readonly options?: SandboxRunOptions;
}

class Runtime implements RoutableSandboxRuntime {
  readonly projectId = projectId("00000000-0000-4000-8000-000000000041");
  readonly calls: RuntimeCall[] = [];
  readonly listeners = new Set<() => void>();
  readonly callWaiters = new Map<number, ReturnType<typeof deferred<RuntimeCall>>>();
  active = 0;

  constructor(readonly startAttempt: (index: number, call: RuntimeCall) => Promise<RunHandle>) {}

  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  snapshot() {
    return {
      projectId: this.projectId,
      incarnation: 1,
      state: "ready" as const,
      capacity: 4,
      active: this.active,
      available: 4 - this.active,
      quarantined: 0,
      heartbeatAgeMs: 10,
      sidecarPeerAgeMs: 10,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 10,
      completionEwmaMs: 1_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
  waitForCall(index: number): Promise<RuntimeCall> {
    if (this.calls[index]) return Promise.resolve(this.calls[index]!);
    let waiter = this.callWaiters.get(index);
    if (!waiter) this.callWaiters.set(index, waiter = deferred<RuntimeCall>());
    return waiter.promise;
  }
  async run(body: Record<string, unknown>, signal?: AbortSignal, options?: SandboxRunOptions): Promise<RunHandle> {
    const call = { body, signal, options };
    const index = this.calls.push(call) - 1;
    this.callWaiters.get(index)?.resolve(call);
    this.callWaiters.delete(index);
    this.active++;
    options?.onAdmissionReflected?.();
    options?.onExecutionRiskReflected?.();
    const settled = () => {
      this.active--;
      for (const listener of this.listeners) listener();
    };
    try {
      const handle = await this.startAttempt(index, call);
      void handle.final.then(settled, settled);
      return handle;
    } catch (error) {
      settled();
      throw error;
    }
  }
}

function createSystem(startAttempt: Runtime["startAttempt"]) {
  const runtime = new Runtime(startAttempt);
  const governor = new TrackedGovernor();
  const logger = new MemoryLogger();
  const authorizationScope = createAuthenticatedTenantScope();
  let trace = 0;
  const router = new MultiSandboxRouter({
    registry: new SandboxRegistry([runtime]),
    governor,
    logger,
    authorizationScope,
    clock,
    random: { next: () => 0.5, uuid: () => `trace-model-${++trace}` },
  });
  cleanups.push(async () => {
    await router.close();
    governor.close();
  });
  return { runtime, governor, logger, authorizationScope, router };
}

async function denial(model: string, origin: "http" | "sse" = "sse", ambiguous = false) {
  return new V2CommittedError("The requested model is unavailable", "model_not_found", ambiguous, undefined,
    await createProviderErrorEnvelope({
      origin,
      status: 404,
      model,
      value: { error: {
        type: "invalid_request_error",
        code: "model_not_found",
        param: "model",
        message: "The requested model is unavailable",
      } },
    }));
}

function response(id = "resp_terra") {
  return {
    id,
    object: "response",
    status: "completed",
    model: TERRA,
    output: [{
      type: "message",
      id: `msg_${id}`,
      status: "completed",
      role: "assistant",
      content: [{ type: "output_text", text: "Terra answer", annotations: [], logprobs: [] }],
    }],
  };
}

function assertClean(system: ReturnType<typeof createSystem>): void {
  expect(system.runtime.active).toBe(0);
  expect(system.router.snapshot()).toMatchObject({ queued: 0, queuedBytes: 0, fleetTransferReserved: 0 });
  expect(system.router.snapshot().provenance).toMatchObject({ reservedRecords: 0, reservedBytes: 0 });
  expect(system.governor.reservations.every(reservation => reservation.released)).toBe(true);
  expect(system.governor.reservations.every(reservation => reservation.releases === 1)).toBe(true);
  expect(system.governor.snapshot().reservedBytes).toBe(0);
}

describe("router model fallback", () => {
  for (const [model, origin] of [["gpt-5.2", "sse"], ["another-unavailable-model", "http"]] as const) {
    it(`retries ${model} once on a definitive ${origin} denial with immutable history and exact lineage`, async () => {
      const primary = new V2RunHandle({ clock });
      const fallback = new V2RunHandle({ clock });
      const fallbackAdmission = deferred<RunHandle>();
      const system = createSystem(async index => {
        if (index === 0) return primary;
        if (index === 1) return fallbackAdmission.promise;
        throw new Error("Unexpected third provider attempt");
      });
      const body = {
        model,
        stream: false,
        store: false,
        previous_response_id: "resp_exact_caller_parent",
        input: [{ role: "user", content: [{ type: "input_text", text: "original history" }] }],
        metadata: { owner: "original metadata" },
      };
      const original = structuredClone(body);
      const handle = await system.router.startRelay(body, { authorizationScope: system.authorizationScope });
      const firstReservation = system.governor.reservations[0]!;
      expect(firstReservation.initialBytes).toBeGreaterThan(4 * 1024 ** 2);
      expect(firstReservation.replacements).toEqual([]);
      expect(firstReservation.released).toBe(false);
      body.previous_response_id = "resp_mutated_parent";
      body.input[0]!.content[0]!.text = "mutated caller history";
      body.metadata.owner = "mutated metadata";

      primary.fail(await denial(model, origin));
      const second = await system.runtime.waitForCall(1);
      // Fallback atomically inherits the parent's reservation. It must never
      // acquire a second full lease, lower the retained-body floor, or release
      // the bytes while the child still owns them.
      expect(system.governor.reservations).toHaveLength(1);
      expect(firstReservation.replacements).toEqual([firstReservation.initialBytes]);
      expect(firstReservation.released).toBe(false);
      expect(system.governor.snapshot().reservedBytes).toBe(firstReservation.initialBytes);
      expect(second.body).toEqual({ ...original, model: TERRA, stream: true, store: true, service_tier: "fast" });
      expect(system.runtime.calls[0]!.body).toEqual({ ...original, stream: true, store: true, service_tier: "fast" });
      expect(Object.isFrozen(second.body)).toBe(true);
      expect(Object.isFrozen(second.body.input)).toBe(true);

      fallbackAdmission.resolve(fallback);
      await flush();
      expect(firstReservation.released).toBe(false);
      expect(system.governor.snapshot().reservedBytes).toBe(firstReservation.initialBytes);
      const final = response();
      const frame: SseFrame = { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) };
      await fallback.acceptEvent(frame);
      fallback.settle(final);
      expect(await Array.fromAsync(handle.events())).toEqual([frame]);
      expect(await handle.final).toEqual(final);
      expect(handle.modelFallback).toEqual({ from: model, to: TERRA });
      expect(system.runtime.calls).toHaveLength(2);

      const fallbacks = system.logger.events.filter(event => event.event === "request.model_fallback");
      expect(fallbacks).toHaveLength(1);
      const accepted = system.logger.events.filter(event => event.event === "request.accepted");
      expect(accepted).toHaveLength(2);
      expect(fallbacks[0]).toMatchObject({
        trace_id: accepted[1]!.trace_id,
        parent_trace_id: accepted[0]!.trace_id,
        requested_model: model,
        fallback_model: TERRA,
        provider_origin: origin,
        retry_count: 1,
      });
      expect(fallbacks[0]!.trace_id).not.toBe(fallbacks[0]!.parent_trace_id);
      for (const event of system.logger.events) assertSafeObservabilityEvent(event);
      const logs = JSON.stringify(system.logger.events);
      expect(logs).not.toContain("original history");
      expect(logs).not.toContain("resp_exact_caller_parent");
      await flush();
      assertClean(system);
    });
  }

  it("admits the one model fallback with no spare governor headroom by transferring the existing reservation", async () => {
    const primary = new V2RunHandle({ clock });
    const fallback = new V2RunHandle({ clock });
    const caller = new AbortController();
    const system = createSystem(async index => {
      if (index === 0) return primary;
      if (index === 1) return fallback;
      throw new Error("Unexpected third provider attempt");
    });
    const handle = await system.router.startRelay({
      model: "gpt-5.2", input: "bounded fallback", previous_response_id: "resp_parent",
    }, { signal: caller.signal });
    void handle.final.catch(() => undefined);
    const before = system.governor.snapshot();
    const occupied = await system.governor.acquire({
      class: "text-small",
      requestBytes: 0,
      reservedBytes: before.reservationCapacityBytes - before.reservedBytes,
      stream: true,
    });
    try {
      expect(system.governor.snapshot().reservedBytes).toBe(before.reservationCapacityBytes);
      primary.fail(await denial("gpt-5.2"));
      await flush();
      expect(system.runtime.calls.map(call => call.body.model)).toEqual(["gpt-5.2", TERRA]);
      expect(system.runtime.calls[1]!.body.previous_response_id).toBe("resp_parent");
      // One request lease plus the independent occupancy lease; fallback must
      // neither queue for a third allocation nor momentarily release its body.
      expect(system.governor.reservations).toHaveLength(2);
      expect(system.governor.snapshot()).toMatchObject({
        reservedBytes: before.reservationCapacityBytes,
        queued: 0,
      });
      expect(system.governor.reservations[0]!.released).toBe(false);
      fallback.settle(response());
      expect(await handle.final).toEqual(response());
      expect(system.governor.reservations[0]!.releases).toBe(1);
      expect(system.governor.snapshot().reservedBytes).toBe(occupied.reservedBytes);
    } finally {
      caller.abort();
      fallback.settle(response());
      occupied.release();
      await handle.final.catch(() => undefined);
    }
    assertClean(system);
  });

  it("falls back once when runtime.run rejects with a definitive committed denial before returning a handle", async () => {
    const primaryError = await denial("gpt-5.2", "http");
    const fallback = new V2RunHandle({ clock });
    const system = createSystem(async index => {
      if (index === 0) throw primaryError;
      if (index === 1) return fallback;
      throw new Error("Unexpected third provider attempt");
    });
    const handle = await system.router.startRelay({
      model: "gpt-5.2", input: "request", previous_response_id: "resp_parent",
    });
    expect(system.runtime.calls.map(call => call.body.model)).toEqual(["gpt-5.2", TERRA]);
    expect(system.runtime.calls[1]!.body.previous_response_id).toBe("resp_parent");
    expect(handle.modelFallback).toEqual({ from: "gpt-5.2", to: TERRA });
    fallback.settle(response());
    expect(await handle.final).toEqual(response());
    await flush();
    assertClean(system);
  });

  it("returns Terra's rejection without a third attempt", async () => {
    const handles = [new V2RunHandle({ clock }), new V2RunHandle({ clock })];
    const system = createSystem(async index => {
      if (!handles[index]) throw new Error("Unexpected third provider attempt");
      return handles[index]!;
    });
    const handle = await system.router.startRelay({ model: "gpt-5.2", input: "request" });
    handles[0]!.fail(await denial("gpt-5.2"));
    await system.runtime.waitForCall(1);
    const fallbackError = await denial(TERRA);
    handles[1]!.fail(fallbackError);
    await expect(handle.final).rejects.toBe(fallbackError);
    expect(await Array.fromAsync(handle.events())).toEqual([]);
    expect(system.runtime.calls).toHaveLength(2);
    expect(system.logger.events.filter(event => event.event === "request.model_fallback")).toHaveLength(1);
    await flush();
    assertClean(system);
  });

  it("retains a successful primary body until its first event, then shrinks and releases its reservation", async () => {
    const primary = new V2RunHandle({ clock });
    const system = createSystem(async () => primary);
    const handle = await system.router.startRelay({ model: "gpt-5.2", input: "request" });
    const reservation = system.governor.reservations[0]!;
    expect(reservation.initialBytes).toBeGreaterThan(4 * 1024 ** 2);
    expect(reservation.replacements).toEqual([]);
    expect(reservation.released).toBe(false);

    const frame = { event: "response.created", data: '{"type":"response.created"}' };
    const events = handle.events();
    const firstEvent = events.next();
    await primary.acceptEvent(frame);
    expect((await firstEvent).value).toEqual(frame);
    await flush();
    expect(reservation.replacements).toEqual([4 * 1024 ** 2]);
    expect(reservation.released).toBe(false);
    expect(system.governor.snapshot().reservedBytes).toBe(4 * 1024 ** 2);

    const final = { ...response("resp_primary"), model: "gpt-5.2" };
    primary.settle(final);
    expect(await handle.final).toEqual(final);
    expect((await events.next()).done).toBe(true);
    expect(handle.modelFallback).toBeUndefined();
    expect(system.logger.events.some(event => event.event === "request.model_fallback")).toBe(false);
    expect(system.runtime.calls).toHaveLength(1);
    assertClean(system);
  });

  it("cleans both attempts when the caller cancels pending fallback admission", async () => {
    const primary = new V2RunHandle({ clock });
    const caller = new AbortController();
    const cancellation = new V2CommittedError("Fallback admission cancelled", "test_fallback_cancelled", true);
    const system = createSystem(async (index, call) => {
      if (index === 0) return primary;
      if (index !== 1) throw new Error("Unexpected third provider attempt");
      return new Promise<RunHandle>((_resolve, reject) => {
        if (call.signal?.aborted) return reject(cancellation);
        call.signal?.addEventListener("abort", () => reject(cancellation), { once: true });
      });
    });
    const handle = await system.router.startRelay({ model: "gpt-5.2", input: "request" }, { signal: caller.signal });
    primary.fail(await denial("gpt-5.2"));
    await system.runtime.waitForCall(1);
    expect(system.governor.reservations.every(reservation => !reservation.released)).toBe(true);
    caller.abort(new DOMException("Caller cancelled", "AbortError"));
    await expect(handle.final).rejects.toBeInstanceOf(V2CommittedError);
    await flush();
    expect(system.runtime.calls).toHaveLength(2);
    expect(system.runtime.calls[1]!.signal?.aborted).toBe(true);
    expect(system.logger.events.filter(event => event.event === "request.model_fallback")).toHaveLength(1);
    assertClean(system);
  });

  for (const scenario of ["generic", "liveness", "ambiguous"] as const) {
    it(`does not fall back for a ${scenario} rejection before a handle is returned`, async () => {
      const error = scenario === "generic"
        ? new Error("model_not_found")
        : scenario === "liveness"
          ? new V2CommittedError("model_not_found", "sidecar_liveness_timeout", true)
          : await denial("gpt-5.2", "http", true);
      const system = createSystem(async () => { throw error; });
      await expect(system.router.startRelay({ model: "gpt-5.2", input: "request" })).rejects.toBeInstanceOf(V2CommittedError);
      expect(system.runtime.calls).toHaveLength(1);
      expect(system.logger.events.some(event => event.event === "request.model_fallback")).toBe(false);
      await flush();
      assertClean(system);
    });
  }

  for (const scenario of ["generic", "liveness", "ambiguous", "already-terra", "partial-event"] as const) {
    it(`does not fall back for ${scenario}`, async () => {
      const primary = new V2RunHandle({ clock });
      const system = createSystem(async () => primary);
      const model = scenario === "already-terra" ? TERRA : "gpt-5.2";
      const handle = await system.router.startRelay({ model, input: "request" });
      const frames: SseFrame[] = [];
      if (scenario === "partial-event") {
        const frame = { event: "response.created", data: "{}" };
        await primary.acceptEvent(frame);
        frames.push(frame);
      }
      const error = scenario === "generic"
        ? new Error("model_not_found")
        : scenario === "liveness"
          ? new V2CommittedError("model_not_found", "sidecar_liveness_timeout", true)
          : await denial(model, "sse", scenario === "ambiguous");
      primary.fail(error);
      await expect(handle.final).rejects.toBe(error);
      expect(await Array.fromAsync(handle.events())).toEqual(frames);
      expect(handle.modelFallback).toBeUndefined();
      expect(system.runtime.calls).toHaveLength(1);
      expect(system.logger.events.some(event => event.event === "request.model_fallback")).toBe(false);
      await flush();
      assertClean(system);
    });
  }
});

describe("HTTP model fallback composition", () => {
  for (const stream of [false, true]) {
    it(`exposes only Terra's success for ${stream ? "SSE" : "JSON"} and leaves no rejected-model prefix record`, async () => {
      const firstError = await denial("gpt-5.2", stream ? "sse" : "http");
      const primary = new V2RunHandle({ clock, expectUpstreamPrelude: true });
      const fallback = new V2RunHandle({ clock, expectUpstreamPrelude: true });
      const handles = [primary, fallback];
      const system = createSystem(async index => {
        if (!handles[index]) throw new Error("Unexpected third provider attempt");
        return handles[index]!;
      });
      const index = new ResponsesHttpContinuationIndex({ key: new Uint8Array(32).fill(0x58) });
      cleanups.push(() => { index.beginShutdown(); index.clear(); });
      const app = createGhostProvApp({
        router: system.router,
        authorizationScope: system.authorizationScope,
        responsesContinuationIndex: index,
      });
      const pending = app.fetch(new Request("http://localhost/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-5.2", stream, store: false,
          input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "question" }] }],
        }),
      }));
      await system.runtime.waitForCall(0);
      primary.acceptUpstreamPrelude({ status: 200, contentType: "text/event-stream", upstreamRequestId: "request_rejected" });
      primary.fail(firstError);
      await system.runtime.waitForCall(1);
      fallback.acceptUpstreamPrelude({ status: 200, contentType: "text/event-stream", upstreamRequestId: "request_terra" });
      const final = response();
      await fallback.acceptEvent({ event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) });
      fallback.settle(final);
      const publicResponse = await pending;
      expect(publicResponse.status).toBe(200);
      if (stream) {
        expect(publicResponse.headers.get("content-type")).toContain("text/event-stream");
        expect(publicResponse.headers.get("x-request-id")).toBe("request_terra");
        const text = await publicResponse.text();
        expect(text).toContain("event: response.completed");
        expect(text).toContain('"model":"gpt-5.6-terra"');
        expect(text).not.toContain("model_not_found");
        expect(text).not.toContain("request_rejected");
      } else {
        expect(publicResponse.headers.get("content-type")).toContain("application/json");
        expect(await publicResponse.json()).toEqual(final);
      }
      expect(index.snapshot().records).toBe(0);
      expect(system.runtime.calls.map(call => call.body.model)).toEqual(["gpt-5.2", TERRA]);
      expect(system.runtime.calls.every(call => call.body.stream === true && call.body.store === true)).toBe(true);
      expect(system.router.snapshot().provenance).toMatchObject({ records: 1 });
      // The app releases its parsed-request lifetime on the next immediate.
      await new Promise<void>(resolve => setImmediate(resolve));
      await flush();
      assertClean(system);
    });
  }
});
