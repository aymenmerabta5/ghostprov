import { describe, expect, it, spyOn } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import {
  type IncomingResourceLease,
  admissionProgressFreshnessMs,
  MultiSandboxRouter,
  RouterCapacityTimeoutError,
  nextAdmissionProgressCheckAtMs,
  preparedProofGraceMs,
  requestRoutingTimeouts,
  shouldRetainStaleAdmissionProgress,
} from "../src/router/router";
import {
  ResourceGovernor,
  type ResourceLease,
} from "../src/router/resource-governor";
import { CpuTransformQueueFullError } from "../src/router/cpu-transform-pool";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxAdmissionProgress,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { V2CommittedError, V2PreCommitError } from "../src/v2/transport/run-handle";
import type { Clock } from "../src/core/ports";
import {
  createAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../src/http/api-key";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

class FakeRuntime implements RoutableSandboxRuntime {
  incarnation = 1;
  active = 0;
  uncommitted = 0;
  uncommittedPeak = 0;
  executions = 0;
  completedResponses = 0;
  snapshotCalls = 0;
  state: "ready" | "draining" | "stopped" = "ready";
  failStarts = 0;
  failError: Error = new V2PreCommitError("pre-publication failure", "test_precommit");
  mutateBeforeFailure?: (body: Record<string, unknown>) => void;
  failDelayMs = 0;
  waitForAdmissionAbort = false;
  admissionAbortError: Error | null = null;
  admissionAbortDelayMs = 0;
  ignoreAdmissionAbortMs = 0;
  holdBeforeExecutionRisk = false;
  reflectTransferCosts = false;
  reflectedTransferUsed = 0;
  postAdmissionDelayMs = 0;
  waitForPostAdmissionAbort = false;
  reflectExecutionRisk = true;
  failAfterExecutionRiskError: Error | null = null;
  returnPendingHandleOnAbortAfterExecutionRisk = false;
  lateHandleDiscardCalls = 0;
  admissionWindow: number | null = null;
  admissionLimit: number | null = null;
  prefetchLimit: number | null = null;
  transferCredits: number | undefined;
  ackEwmaMs = 1_000;
  heartbeatAgeMs: number | null = 1_000;
  sidecarPeerAgeMs: number | null = null;
  transportConnected: boolean | undefined;
  diagnostics: any = undefined;
  transportProfile: "standard" | "bulk-v1" | null = null;
  lastRunSignal?: AbortSignal;
  pulseAdmissionOnFailure = false;
  readonly executionStartedAtMs: number[] = [];
  readonly admissionProgressCallbacks: Array<(progress: SandboxAdmissionProgress) => void> = [];
  readonly executionRiskCallbacks: Array<() => void> = [];
  readonly preExecutionHolds: Array<{
    body: Record<string, unknown>;
    release: () => void;
  }> = [];
  readonly finals: Array<ReturnType<typeof deferred<any>>> = [];
  readonly bodies: Record<string, unknown>[] = [];
  readonly listeners = new Set<() => void>();
  constructor(readonly projectId: ProjectId, readonly capacity = 30) {}
  async start(): Promise<void> {}
  async drain(): Promise<void> {
    this.state = "draining";
    if (this.active === 0) this.state = "stopped";
  }
  async stop(): Promise<void> { this.state = "stopped"; }
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  replaceIncarnation(): void {
    this.incarnation++;
    this.notify();
  }
  notify(): void {
    for (const listener of this.listeners) listener();
  }
  snapshot() {
    this.snapshotCalls++;
    const defaultAvailable = Math.max(0, Math.min(
      this.admissionWindow === null
        ? this.capacity
        : this.admissionWindow - this.uncommitted,
      this.capacity - this.active,
    ));
    const admissionCredits = this.admissionLimit === null
      ? undefined
      : Math.max(0, Math.min(
          this.admissionLimit - this.uncommitted,
          this.capacity - this.active,
        ));
    const prefetchCredits = this.prefetchLimit === null
      ? undefined
      : Math.max(0, Math.min(
          this.prefetchLimit - this.uncommitted,
          this.capacity - this.active,
        ));
    return {
      projectId: this.projectId,
      transportProfile: this.transportProfile,
      incarnation: this.incarnation,
      state: this.state,
      capacity: this.capacity,
      active: this.active,
      available: Math.max(
        defaultAvailable,
        admissionCredits ?? 0,
        prefetchCredits ?? 0,
      ),
      ...(admissionCredits === undefined ? {} : { admissionCredits }),
      ...(prefetchCredits === undefined ? {} : { prefetchCredits }),
      ...(this.transferCredits === undefined ? {} : {
        transferCredits: Math.max(0, this.transferCredits - this.reflectedTransferUsed),
      }),
      quarantined: 0,
      heartbeatAgeMs: this.heartbeatAgeMs,
      sidecarPeerAgeMs: this.sidecarPeerAgeMs,
      transportConnected: this.transportConnected,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: this.ackEwmaMs,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
      diagnostics: this.diagnostics,
    } as const;
  }
  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.lastRunSignal = signal;
    this.executions++;
    this.executionStartedAtMs.push(performance.now());
    this.bodies.push(body);
    if (this.failStarts > 0) {
      this.failStarts--;
      this.mutateBeforeFailure?.(body);
      if (this.failDelayMs > 0) await Bun.sleep(this.failDelayMs);
      if (this.pulseAdmissionOnFailure) {
        const previousWindow = this.admissionWindow;
        this.admissionWindow = 0;
        this.notify();
        this.admissionWindow = previousWindow;
        this.notify();
      }
      throw this.failError;
    }
    if (this.waitForAdmissionAbort) {
      await new Promise<never>((_resolve, reject) => {
        if (signal?.aborted) {
          reject(this.admissionAbortError ?? new V2PreCommitError(
            "fake runtime proved no execution began before admission",
            "test_precommit_abort",
          ));
          return;
        }
        signal?.addEventListener("abort", () => {
          const fail = () => reject(this.admissionAbortError ?? new V2PreCommitError(
            "fake runtime proved no execution began before admission",
            "test_precommit_abort",
          ));
          if (this.admissionAbortDelayMs > 0) setTimeout(fail, this.admissionAbortDelayMs);
          else fail();
        }, { once: true });
      });
    }
    if (this.ignoreAdmissionAbortMs > 0) {
      await Bun.sleep(this.ignoreAdmissionAbortMs);
      throw new V2PreCommitError("late pre-commit cleanup", "late_precommit");
    }
    this.active++;
    this.uncommitted++;
    this.uncommittedPeak = Math.max(this.uncommittedPeak, this.uncommitted);
    const reflectedTransferCost = this.reflectTransferCosts &&
        Number.isSafeInteger(body.test_transfer_cost) &&
        Number(body.test_transfer_cost) > 0
      ? Number(body.test_transfer_cost)
      : 0;
    this.reflectedTransferUsed += reflectedTransferCost;
    if (options?.onAdmissionProgress) {
      this.admissionProgressCallbacks.push(options.onAdmissionProgress);
    }
    options?.onAdmissionReflected?.();
    if (this.holdBeforeExecutionRisk) {
      const gate = deferred<void>();
      this.preExecutionHolds.push({ body, release: () => gate.resolve() });
      await gate.promise;
    }
    if (reflectedTransferCost > 0) {
      this.reflectedTransferUsed = Math.max(0, this.reflectedTransferUsed - reflectedTransferCost);
      this.notify();
    }
    if (options?.onExecutionRiskReflected) {
      if (this.reflectExecutionRisk) options.onExecutionRiskReflected();
      else this.executionRiskCallbacks.push(options.onExecutionRiskReflected);
    }
    if (this.failAfterExecutionRiskError) {
      this.active--;
      this.uncommitted--;
      throw this.failAfterExecutionRiskError;
    }
    if (this.returnPendingHandleOnAbortAfterExecutionRisk) {
      await new Promise<void>(resolve => {
        if (signal?.aborted) resolve();
        else signal?.addEventListener("abort", () => resolve(), { once: true });
      });
      this.uncommitted--;
      const gate = deferred<any>();
      this.finals.push(gate);
      return {
        async *events() {},
        discardUnconsumedEvents: () => { this.lateHandleDiscardCalls++; },
        final: gate.promise.finally(() => {
          this.active--;
          this.notify();
        }),
        stats: { bytes: 0, firstByteMs: 1, frames: 0 },
      };
    }
    if (this.waitForPostAdmissionAbort) {
      try {
        await new Promise<never>((_resolve, reject) => {
          if (signal?.aborted) {
            reject(new V2PreCommitError(
              "fake runtime proved admitted work never started",
              "test_precommit_abort",
            ));
            return;
          }
          signal?.addEventListener("abort", () => reject(new V2PreCommitError(
            "fake runtime proved admitted work never started",
            "test_precommit_abort",
          )), { once: true });
        });
      } finally {
        this.active--;
        this.uncommitted--;
        this.notify();
      }
    }
    const gate = deferred<any>();
    this.finals.push(gate);
    if (this.postAdmissionDelayMs > 0) await Bun.sleep(this.postAdmissionDelayMs);
    else await Promise.resolve();
    this.uncommitted--;
    return {
      async *events() {},
      final: gate.promise.finally(() => {
        this.active--;
        this.notify();
      }),
      stats: { bytes: 0, firstByteMs: 1, frames: 0 },
    };
  }
  completeOne(value: unknown = {
    id: `resp_${this.projectId}_${++this.completedResponses}`,
    status: "completed",
  }): void {
    this.finals.shift()?.resolve(value);
  }
  rejectOne(error: unknown): void {
    this.finals.shift()?.reject(error);
  }
  emitAdmissionProgress(
    progress: SandboxAdmissionProgress = {
      stage: "request_data",
      acknowledgedEncodedBytes: 768 * 1024,
      totalEncodedBytes: Math.floor(2.9 * 1024 ** 2),
    },
    index = this.admissionProgressCallbacks.length - 1,
  ): void {
    this.admissionProgressCallbacks[index]?.(progress);
  }
  emitExecutionRisk(index = this.executionRiskCallbacks.length - 1): void {
    this.executionRiskCallbacks[index]?.();
  }
}

function createSystem(
  projects = 4,
  slots = 30,
  queueTimeoutMs?: number,
  queueCapacity?: number,
  queueByteCapacity?: number,
  clock?: Clock,
  routeAttemptTimeoutMs?: number,
  authorizationScope?: AuthenticatedTenantScope,
) {
  const runtimes = Array.from({ length: projects }, (_, index) => new FakeRuntime(
    projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`),
    slots,
  ));
  const registry = new SandboxRegistry(runtimes);
  const governor = new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 20 * 1024 ** 3,
        rssBytes: 320 * 1024 ** 2,
        heapBytes: 100 * 1024 ** 2,
        externalBytes: 20 * 1024 ** 2,
        logicalProcessors: 12,
      }),
    },
    now: clock ? () => clock.now() : undefined,
  });
  const router = new MultiSandboxRouter({
    registry,
    governor,
    random: { next: () => 0.5, uuid: () => "id" },
    queueTimeoutMs,
    queueCapacity,
    queueByteCapacity,
    clock,
    routeAttemptTimeoutMs,
    authorizationScope,
  });
  return { runtimes, registry, governor, router };
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

function manualRouterTimers() {
  const startedAtMs = 100_000;
  let nowMs = startedAtMs;
  const pending = new Map<object, { dueAtMs: number; callback(): void }>();
  const set = spyOn(globalThis, "setTimeout").mockImplementation(((
    callback: (...args: unknown[]) => void, delay = 0, ...args: unknown[]
  ) => {
    const handle = { unref() { return handle; } };
    pending.set(handle, { dueAtMs: nowMs + Math.max(0, delay), callback: () => callback(...args) });
    return handle;
  }) as typeof setTimeout);
  const clear = spyOn(globalThis, "clearTimeout").mockImplementation((handle => {
    pending.delete(handle as object);
  }) as typeof clearTimeout);
  const clock: Clock = {
    now: () => nowMs,
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
    waitUntil: (deadlineMs, signal) => clock.sleep(Math.max(0, deadlineMs - nowMs), signal),
  };
  return {
    clock,
    async advanceTo(elapsedMs: number) {
      const targetMs = startedAtMs + elapsedMs;
      if (targetMs < nowMs) throw new Error("Manual router time must be monotonic");
      for (;;) {
        const next = [...pending].filter(([, timer]) => timer.dueAtMs <= targetMs)
          .sort((left, right) => left[1].dueAtMs - right[1].dueAtMs)[0];
        if (!next) break;
        nowMs = next[1].dueAtMs;
        pending.delete(next[0]);
        next[1].callback();
        await flush(100);
      }
      nowMs = targetMs;
      await flush(100);
    },
    restore() { set.mockRestore(); clear.mockRestore(); },
  };
}

function controlledAdmission(runtime: FakeRuntime) {
  let providerStarts = 0;
  let safeAborts = 0;
  const attempts: Array<{
    signal?: AbortSignal;
    progress(value: SandboxAdmissionProgress): void;
    commit(): void;
  }> = [];
  const run = spyOn(runtime, "run").mockImplementation(async (body, signal, options) => {
    runtime.executions++;
    runtime.bodies.push(body);
    runtime.active++;
    runtime.uncommitted++;
    const admitted = deferred<void>();
    const abort = () => admitted.reject(new V2PreCommitError(
      "Controlled transport proved the provider never started", "test_precommit_abort",
    ));
    attempts.push({
      signal,
      progress: value => options?.onAdmissionProgress?.(value),
      commit: () => admitted.resolve(),
    });
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    options?.onAdmissionReflected?.();
    try {
      await admitted.promise;
      if (signal?.aborted) throw new V2PreCommitError(
        "Controlled transport cancelled before provider start", "test_precommit_abort",
      );
    } catch (error) {
      safeAborts++;
      runtime.active--;
      runtime.uncommitted--;
      runtime.notify();
      throw error;
    } finally {
      signal?.removeEventListener("abort", abort);
    }
    providerStarts++;
    runtime.uncommitted--;
    options?.onExecutionRiskReflected?.();
    const final = deferred<any>();
    runtime.finals.push(final);
    return {
      async *events() {},
      final: final.promise.finally(() => {
        runtime.active--;
        runtime.notify();
      }),
      stats: { bytes: 0, firstByteMs: 1, frames: 0 },
    };
  });
  return {
    attempts,
    get providerStarts() { return providerStarts; },
    get safeAborts() { return safeAborts; },
    restore: () => run.mockRestore(),
  };
}

function mediumAdmissionFixture(
  requestBytes = 2_012_715,
  profile: "standard" | "bulk-v1" = "bulk-v1",
) {
  const timers = manualRouterTimers();
  const system = createSystem(2, 1, 30_000, undefined, undefined, timers.clock, 15_000);
  const controls = system.runtimes.map(runtime => {
    runtime.transportProfile = profile;
    runtime.ackEwmaMs = 600;
    runtime.notify();
    return controlledAdmission(runtime);
  });
  const body = {
    model: "gpt-5.6-sol", input: "", stream: true, store: true, service_tier: "fast",
  };
  body.input = "x".repeat(requestBytes - Buffer.byteLength(JSON.stringify(body)));
  const controller = new AbortController();
  const pending = system.router.startRelay(body, {
    source: "websocket",
    clientId: "medium-live-size",
    structuralBodyBytes: requestBytes,
    encodedBodyBytes: requestBytes,
    signal: controller.signal,
  });
  const outcome = pending.then(handle => ({ handle }), error => ({ error }));
  return {
    ...system, timers, controls, requestBytes, body, pending, outcome,
    async close() {
      try {
        controller.abort();
        await flush(100);
        for (const runtime of system.runtimes) while (runtime.finals.length) runtime.completeOne();
        await outcome;
        await system.router.close();
      } finally {
        for (const control of controls) control.restore();
        timers.restore();
      }
    },
  };
}

describe("MultiSandboxRouter", () => {
  it("returns a preparation timeout once without rerouting the same CPU work", async () => {
    const system = createSystem(2, 4, 5_000);
    for (const runtime of system.runtimes) {
      runtime.failStarts = 1;
      runtime.failError = new V2PreCommitError(
        "V2 request preparation timed out before admission",
        "request_preparation_timeout",
      );
    }
    const pending = system.router.startRelay({ model: "gpt-6-astra", input: "prepare once" });
    try {
      await expect(pending).rejects.toMatchObject({
        committed: false, code: "request_preparation_timeout",
      });
      expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
      expect(system.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
    } finally {
      for (const runtime of system.runtimes) while (runtime.finals.length) runtime.completeOne();
      await system.router.close();
    }
  });

  it("keeps medium requests moving beside a large upload and an aged large waiter", async () => {
    let now = performance.timeOrigin + performance.now();
    const clock: Clock = {
      now: () => now,
      sleep: async ms => { now += ms; },
      waitUntil: async deadline => { now = Math.max(now, deadline); },
    };
    const system = createSystem(4, 8, 30_000, undefined, undefined, clock, 20_000);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.reflectExecutionRisk = false;
      runtime.waitForPostAdmissionAbort = true;
      runtime.notify();
    }
    const controllers: AbortController[] = [];
    const outcomes: Promise<unknown>[] = [];
    const launch = (label: string, bytes: number) => {
      const controller = new AbortController();
      controllers.push(controller);
      const pending = system.router.startRelay({ model: "gpt-6-astra", input: label, stream: true }, {
        clientId: label, signal: controller.signal,
        encodedBodyBytes: bytes, structuralBodyBytes: bytes,
      });
      outcomes.push(pending.catch(() => undefined));
    };
    const count = (label: string) => system.runtimes.reduce((sum, runtime) =>
      sum + runtime.bodies.filter(body => String(body.input).startsWith(label)).length, 0);
    try {
      launch("large-first", 55 * 1024 ** 2);
      await flush(100);
      launch("large-waiter", 55 * 1024 ** 2);
      await flush(100);
      now += 5_000;
      for (const runtime of system.runtimes) runtime.notify();
      launch("medium-one", 1024 ** 2);
      await flush(100);
      now += 500;
      for (const runtime of system.runtimes) runtime.notify();
      launch("medium-two", 1024 ** 2);
      await flush(100);
      expect(count("large-")).toBe(1);
      expect(count("medium-")).toBe(2);
      expect(system.router.snapshot()).toMatchObject({ queued: 1, fleetTransferReserved: 6 });
      launch("medium-three", 1024 ** 2);
      now += 500;
      for (const runtime of system.runtimes) runtime.notify();
      await flush(100);
      expect(count("medium-")).toBe(2);
      expect(system.router.snapshot().fleetTransferReserved).toBe(6);
      controllers[0]!.abort();
      await flush(100);
      expect(count("large-")).toBe(2);
      expect(system.router.snapshot().fleetTransferReserved).toBe(6);
    } finally {
      for (const controller of controllers) controller.abort();
      await Promise.allSettled(outcomes);
      await system.router.close();
    }
    expect(system.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
  });

  it("uses four regular credits for tiny work and six prefetch credits for medium work", async () => {
    const tiny = createSystem(1, 6, 1_000);
    tiny.runtimes[0]!.transportProfile = "bulk-v1";
    tiny.runtimes[0]!.admissionLimit = 4;
    tiny.runtimes[0]!.prefetchLimit = 6;
    tiny.runtimes[0]!.postAdmissionDelayMs = 50;
    tiny.runtimes[0]!.notify();
    const tinyRuns = Array.from({ length: 6 }, (_, index) => tiny.router.startRelay({
      model: "gpt-5.6-sol",
      input: `tiny-${index}`,
      stream: true,
    }, { clientId: `tiny-credit-${index}`, source: "websocket" }));
    await flush(100);
    expect(tiny.runtimes[0]!.executions).toBe(4);
    expect(tiny.router.snapshot().queued).toBe(2);
    await Bun.sleep(60);
    await flush(100);
    expect(tiny.runtimes[0]!.executions).toBe(6);
    for (let index = 0; index < 6; index++) tiny.runtimes[0]!.completeOne();
    const tinyHandles = await Promise.all(tinyRuns);
    await Promise.all(tinyHandles.map(handle => handle.final));
    await tiny.router.close();

    const medium = createSystem(1, 6, 1_000);
    medium.runtimes[0]!.transportProfile = "bulk-v1";
    medium.runtimes[0]!.admissionLimit = 4;
    medium.runtimes[0]!.prefetchLimit = 6;
    medium.runtimes[0]!.notify();
    const mediumInput = "x".repeat(512 * 1024);
    const mediumStructuralBytes = Buffer.byteLength(JSON.stringify({
      model: "gpt-5.6-sol",
      input: mediumInput,
      stream: true,
    }));
    const mediumRuns = Array.from({ length: 6 }, (_, index) => medium.router.startRelay({
      model: "gpt-5.6-sol",
      input: mediumInput,
      tag: `medium-${index}`,
      stream: true,
    }, {
      clientId: `medium-credit-${index}`,
      source: "websocket",
      structuralBodyBytes: mediumStructuralBytes,
    }));
    await flush(100);
    expect(medium.runtimes[0]!.executions).toBe(6);
    for (let index = 0; index < 6; index++) medium.runtimes[0]!.completeOne();
    const mediumHandles = await Promise.all(mediumRuns);
    await Promise.all(mediumHandles.map(handle => handle.final));
    await medium.router.close();
  });

  it("does not treat reflected transfer units as whole-request credits", async () => {
    const system = createSystem(1, 6, 1_000);
    const runtime = system.runtimes[0]!;
    runtime.transportProfile = "bulk-v1";
    runtime.admissionLimit = 6;
    runtime.prefetchLimit = 6;
    runtime.transferCredits = 2;
    runtime.notify();
    const input = "x".repeat(3 * 1024 ** 2 + 1);
    const body = { model: "gpt-5.6-sol", input, stream: true };
    const pending = system.router.startRelay(body, {
      source: "websocket",
      structuralBodyBytes: Buffer.byteLength(JSON.stringify(body)),
    });
    for (let turn = 0; turn < 100 && system.router.snapshot().queued === 0; turn++) {
      await Bun.sleep(1);
    }
    expect(runtime.executions).toBe(0);
    expect(system.router.snapshot().queued).toBe(1);

    runtime.transferCredits = 6;
    runtime.notify();
    const handle = await pending;
    expect(runtime.executions).toBe(1);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("caps R5 fleet bursts at four weighted units while tiny work bypasses the gate", async () => {
    const system = createSystem(8, 8, 10_000, undefined, undefined, undefined, 5_000);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.reflectExecutionRisk = false;
      runtime.waitForPostAdmissionAbort = true;
      runtime.notify();
    }
    const controllers = Array.from({ length: 9 }, () => new AbortController());
    const large = Array.from({ length: 8 }, (_, index) => system.router.startRelay(
      { model: "gpt-5.6-sol", input: `fleet-gate-large-${index}`, stream: true },
      {
        clientId: `fleet-gate-large-${index}`,
        source: "websocket",
        signal: controllers[index]!.signal,
        encodedBodyBytes: Math.floor(2.9 * 1024 ** 2),
        structuralBodyBytes: Math.floor(2.9 * 1024 ** 2),
      },
    ));
    await flush(100);

    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(2);
    expect(system.runtimes.filter(runtime => runtime.executions > 0)).toHaveLength(2);
    expect(system.router.snapshot()).toMatchObject({
      queued: 6,
      fleetTransferReserved: 4,
      fleetTransferBudget: 6,
    });

    const tiny = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "fleet-gate-tiny", stream: true },
      {
        clientId: "fleet-gate-tiny",
        source: "websocket",
        signal: controllers[8]!.signal,
        encodedBodyBytes: 128 * 1024,
        structuralBodyBytes: 128 * 1024,
      },
    );
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot().fleetTransferReserved).toBe(4);

    await Bun.sleep(510);
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 6, fleetTransferReserved: 4 });

    const holding = system.runtimes.find(runtime => (
      runtime.admissionProgressCallbacks.length > 0 &&
      runtime.executionRiskCallbacks.length > 0
    ))!;
    const otherHolding = system.runtimes.find(runtime => (
      runtime !== holding &&
      runtime.admissionProgressCallbacks.length > 0 &&
      runtime.executionRiskCallbacks.length > 0
    ))!;
    holding.emitAdmissionProgress();
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 6, fleetTransferReserved: 4 });

    holding.emitAdmissionProgress({
      stage: "request_data",
      acknowledgedEncodedBytes: Math.floor(2.9 * 1024 ** 2),
      totalEncodedBytes: Math.floor(2.9 * 1024 ** 2),
    });
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 6, fleetTransferReserved: 4 });

    otherHolding.emitAdmissionProgress({
      stage: "request_data",
      acknowledgedEncodedBytes: Math.floor(2.9 * 1024 ** 2),
      totalEncodedBytes: Math.floor(2.9 * 1024 ** 2),
    });
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 6, fleetTransferReserved: 4 });

    holding.emitAdmissionProgress({
      stage: "request_prepared",
      acknowledgedEncodedBytes: Math.floor(2.9 * 1024 ** 2),
      totalEncodedBytes: Math.floor(2.9 * 1024 ** 2),
    });
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 6, fleetTransferReserved: 3 });

    // Prepared proof releases only the data share. The remaining control unit
    // is released at execution risk, at which point one new body may enter.
    holding.emitExecutionRisk(0);
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(4);
    expect(system.router.snapshot()).toMatchObject({ queued: 5, fleetTransferReserved: 4 });

    for (const controller of controllers) controller.abort();
    await Promise.allSettled([...large, tiny]);
    await system.router.close();
    expect(system.router.snapshot().fleetTransferReserved).toBe(0);
  });

  it("protects an aged bulk continuation on a healthy peer when its origin is down", async () => {
    let now = performance.timeOrigin + performance.now();
    const clock: Clock = {
      now: () => now,
      sleep: async ms => { now += ms; },
      waitUntil: async deadline => { now = Math.max(now, deadline); },
    };
    const authorizationScope = createAuthenticatedTenantScope();
    const system = createSystem(2, 4, 20_000, undefined, undefined, clock, undefined, authorizationScope);
    const controller = new AbortController();
    const outcomes: Promise<unknown>[] = [];
    try {
      for (const runtime of system.runtimes) {
        runtime.transportProfile = "bulk-v1";
        runtime.transferCredits = 4;
        runtime.notify();
      }
      const seed = await system.router.startRelay({ model: "gpt-5.6-terra", input: "seed" });
      const origin = system.runtimes.find(runtime => runtime.finals.length > 0)!;
      const peer = system.runtimes.find(runtime => runtime !== origin)!;
      origin.completeOne({ id: "resp_aged_bulk_origin", status: "completed" });
      await seed.final;

      origin.state = "stopped";
      origin.notify();
      peer.transferCredits = 1;
      peer.holdBeforeExecutionRisk = true;
      peer.reflectTransferCosts = true;
      peer.notify();
      const continuationBody = Object.freeze({
        model: "gpt-5.6-terra",
        input: "aged bulk continuation",
        previous_response_id: "resp_aged_bulk_origin",
        test_transfer_cost: 4,
        stream: false,
        store: false,
      });
      const continuation = system.router.startRelay(continuationBody, {
        source: "websocket",
        signal: controller.signal,
        encodedBodyBytes: Math.floor(3.2 * 1024 ** 2),
        structuralBodyBytes: Math.floor(3.2 * 1024 ** 2),
      });
      outcomes.push(continuation.then(handle => handle.final).catch(() => undefined));
      await flush(100);
      expect(system.router.snapshot().queued).toBe(1);
      expect(peer.executions).toBe(0);

      // The aged request must protect the live peer's credits even though its
      // persisted provenance still names the stopped origin.
      now += 4_000;
      peer.transferCredits = 2;
      peer.notify();
      await flush(100);
      const medium = system.router.startRelay({
        model: "gpt-5.6-terra",
        input: "younger medium request",
        test_transfer_cost: 2,
      }, {
        signal: controller.signal,
        encodedBodyBytes: Math.floor(2.5 * 1024 ** 2),
        structuralBodyBytes: Math.floor(2.5 * 1024 ** 2),
      });
      outcomes.push(medium.then(handle => handle.final).catch(() => undefined));
      await flush(100);
      expect(peer.executions).toBe(0);
      expect(system.router.snapshot()).toMatchObject({ queued: 2, fleetTransferReserved: 0 });

      peer.transferCredits = 4;
      peer.notify();
      await flush(100);
      expect(origin.executions).toBe(1);
      expect(peer.executions).toBe(1);
      expect(peer.preExecutionHolds).toHaveLength(1);
      expect(peer.bodies[0]).toMatchObject({ ...continuationBody, stream: true, store: true });
      expect(continuationBody).toMatchObject({ stream: false, store: false });
      expect(system.router.snapshot()).toMatchObject({
        queued: 1,
        fleetTransferReserved: 4,
        provenance: { reservedRecords: 1 },
      });

      // Finishing the body transfer releases the protection and lets the
      // younger request use the same peer without waiting for the terminal.
      peer.holdBeforeExecutionRisk = false;
      peer.preExecutionHolds.shift()!.release();
      const continuationHandle = await continuation;
      const mediumHandle = await medium;
      expect(peer.executions).toBe(2);
      expect(peer.bodies.filter(body => body.previous_response_id === continuationBody.previous_response_id))
        .toHaveLength(1);
      peer.completeOne({ id: "resp_aged_bulk_peer", status: "completed" });
      peer.completeOne({ id: "resp_after_aged_bulk", status: "completed" });
      await Promise.all([continuationHandle.final, mediumHandle.final]);
      expect(system.router.lookupResponseProvenance("resp_aged_bulk_origin", authorizationScope))
        .toEqual({ kind: "known", provenance: { projectId: origin.projectId, incarnation: 1 } });
      expect(system.router.lookupResponseProvenance("resp_aged_bulk_peer", authorizationScope))
        .toEqual({ kind: "known", provenance: { projectId: peer.projectId, incarnation: 1 } });
      expect(system.router.snapshot()).toMatchObject({
        active: 0,
        queued: 0,
        queuedBytes: 0,
        fleetTransferReserved: 0,
        provenance: { reservedRecords: 0, reservedBytes: 0 },
      });
      expect(peer.reflectedTransferUsed).toBe(0);
      expect(system.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      controller.abort();
      for (let round = 0; round < 3; round++) {
        for (const runtime of system.runtimes) {
          runtime.holdBeforeExecutionRisk = false;
          while (runtime.preExecutionHolds.length > 0) runtime.preExecutionHolds.shift()!.release();
        }
        await flush(100);
        for (const runtime of system.runtimes) {
          while (runtime.finals.length > 0) runtime.completeOne();
        }
      }
      await Promise.allSettled(outcomes);
      await system.router.close();
    }
  });

  for (const scenario of [
    { name: "medium", bytes: 1024 ** 2, initialCost: 1 },
    { name: "observed", bytes: Math.floor(2.9 * 1024 ** 2), initialCost: 2 },
    { name: "true bulk", bytes: Math.floor(3.35 * 1024 ** 2), initialCost: 4 },
    { name: "payload-scaled", bytes: 9 * 1024 ** 2, initialCost: 4 },
  ]) {
    it(`retains control capacity after exact request_prepared for ${scenario.name} bodies`, async () => {
      const system = createSystem(2, 8, 100_000, undefined, undefined, undefined, 50_000);
      for (const runtime of system.runtimes) {
        runtime.transportProfile = "bulk-v1";
        runtime.reflectExecutionRisk = false;
        runtime.waitForPostAdmissionAbort = true;
        runtime.notify();
      }
      const controller = new AbortController();
      const pending = system.router.startRelay(
        { model: "gpt-5.6-sol", input: scenario.name, stream: true },
        {
          clientId: `prepared-${scenario.name}`,
          source: "websocket",
          signal: controller.signal,
          encodedBodyBytes: scenario.bytes,
          structuralBodyBytes: scenario.bytes,
        },
      );
      await flush(100);
      const selected = system.runtimes.find(runtime => runtime.executions === 1)!;
      expect(selected.admissionProgressCallbacks).toHaveLength(1);
      expect(system.router.snapshot().fleetTransferReserved).toBe(scenario.initialCost);

      selected.emitAdmissionProgress({
        stage: "request_data",
        acknowledgedEncodedBytes: scenario.bytes,
        totalEncodedBytes: scenario.bytes,
      });
      await flush(20);
      expect(system.router.snapshot().fleetTransferReserved).toBe(scenario.initialCost);

      selected.emitAdmissionProgress({
        stage: "request_prepared",
        acknowledgedEncodedBytes: scenario.bytes,
        totalEncodedBytes: scenario.bytes,
      });
      await flush(20);
      expect(system.router.snapshot().fleetTransferReserved).toBe(1);

      selected.emitExecutionRisk();
      await flush(20);
      expect(system.router.snapshot().fleetTransferReserved).toBe(0);

      controller.abort();
      await Promise.allSettled([pending]);
      await system.router.close();
    });
  }

  it("paces a synchronized R5 medium burst while preserving single-start and tiny-request fast paths", async () => {
    let now = performance.timeOrigin + performance.now();
    const clock: Clock = {
      now: () => now,
      sleep: () => Promise.resolve(),
      waitUntil: () => Promise.resolve(),
    };
    const system = createSystem(8, 8, 20_000, undefined, undefined, clock, 10_000);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.reflectExecutionRisk = false;
      runtime.waitForPostAdmissionAbort = true;
      runtime.notify();
    }
    const controllers = Array.from({ length: 5 }, () => new AbortController());
    const medium = Array.from({ length: 4 }, (_, index) => system.router.startRelay(
      { model: "gpt-5.6-sol", input: `paced-medium-${index}`, stream: true },
      {
        clientId: `paced-medium-${index}`,
        source: "websocket",
        signal: controllers[index]!.signal,
        encodedBodyBytes: 1024 ** 2,
        structuralBodyBytes: 1024 ** 2,
      },
    ));
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    expect(system.runtimes.filter(runtime => runtime.executions > 0)).toHaveLength(1);
    expect(system.router.snapshot()).toMatchObject({ queued: 3, fleetTransferReserved: 1 });

    const tiny = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "paced-tiny-bypass", stream: true },
      {
        clientId: "paced-tiny-bypass",
        source: "websocket",
        signal: controllers[4]!.signal,
        encodedBodyBytes: 128 * 1024,
        structuralBodyBytes: 128 * 1024,
      },
    );
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(2);
    expect(system.router.snapshot()).toMatchObject({ queued: 3, fleetTransferReserved: 1 });

    now += 500;
    system.runtimes.forEach(runtime => runtime.notify());
    await Bun.sleep(510);
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(3);
    expect(system.router.snapshot()).toMatchObject({ queued: 2, fleetTransferReserved: 2 });

    now += 500;
    system.runtimes.forEach(runtime => runtime.notify());
    await Bun.sleep(510);
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(4);
    expect(system.router.snapshot()).toMatchObject({ queued: 1, fleetTransferReserved: 3 });

    now += 500;
    system.runtimes.forEach(runtime => runtime.notify());
    await Bun.sleep(510);
    await flush(100);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(5);
    expect(system.router.snapshot()).toMatchObject({ queued: 0, fleetTransferReserved: 4 });

    for (const controller of controllers) controller.abort();
    await Promise.allSettled([...medium, tiny]);
    await system.router.close();
    expect(system.router.snapshot().fleetTransferReserved).toBe(0);
  });

  it("moves a continuation to a healthy peer when its selected sandbox goes down before execution", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const system = createSystem(2, 4, 1_000, undefined, undefined, undefined, undefined, authorizationScope);
    const controller = new AbortController();
    let pending: Promise<RunHandle> | undefined;
    try {
      const seed = await system.router.startRelay({ model: "gpt-5.6-sol", input: "seed" });
      const owner = system.runtimes.find(runtime => runtime.finals.length > 0)!;
      const peer = system.runtimes.find(runtime => runtime !== owner)!;
      owner.completeOne({ id: "resp_before_sandbox_down", status: "completed" });
      await seed.final;

      owner.failStarts = 1;
      owner.mutateBeforeFailure = () => {
        owner.state = "stopped";
        owner.notify();
      };
      const body = {
        model: "gpt-5.6-sol",
        input: [{ role: "user", content: "continue after sandbox failure" }],
        previous_response_id: "resp_before_sandbox_down",
        stream: false,
        store: false,
      };
      pending = system.router.startRelay(body, {
        source: "websocket",
        authorizationScope,
        signal: controller.signal,
      });
      void pending.catch(() => undefined);
      await flush(100);

      expect(owner.state).toBe("stopped");
      expect(owner.executions).toBe(2);
      expect(peer.executions).toBe(1);
      const handle = await pending;
      expect(peer.bodies[0]).toBe(owner.bodies[1]);
      expect(peer.bodies[0]).toEqual({ ...body, stream: true, store: true, service_tier: "fast" });
      expect(body).toMatchObject({ stream: false, store: false });
      peer.completeOne({ id: "resp_after_sandbox_down", status: "completed" });
      await handle.final;
      expect(system.router.lookupResponseProvenance("resp_before_sandbox_down", authorizationScope))
        .toEqual({ kind: "known", provenance: { projectId: owner.projectId, incarnation: 1 } });
      expect(system.router.lookupResponseProvenance("resp_after_sandbox_down", authorizationScope))
        .toEqual({ kind: "known", provenance: { projectId: peer.projectId, incarnation: 1 } });
      expect(system.router.snapshot()).toMatchObject({ queued: 0, active: 0, fleetTransferReserved: 0 });
      expect(system.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      controller.abort();
      for (const runtime of system.runtimes) {
        while (runtime.finals.length > 0) runtime.completeOne();
      }
      if (pending) await Promise.allSettled([pending]);
      await system.router.close();
    }
  });

  it("paces medium work with a preferred persisted-provenance sandbox", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const system = createSystem(
      2,
      4,
      2_000,
      undefined,
      undefined,
      undefined,
      undefined,
      authorizationScope,
    );
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const seed = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "pacing provenance seed",
      stream: true,
    }, { authorizationScope });
    const bound = system.runtimes.find(runtime => runtime.finals.length > 0)!;
    bound.completeOne({ id: "resp_pacing_seed", status: "completed" });
    await seed.final;
    for (const runtime of system.runtimes) {
      runtime.reflectExecutionRisk = false;
      runtime.waitForPostAdmissionAbort = true;
      runtime.notify();
    }
    const controllers = Array.from({ length: 4 }, () => new AbortController());
    const pending = controllers.map((controller, index) => system.router.startRelay({
      model: "gpt-5.6-sol",
      input: `pinned-medium-${index}`,
      previous_response_id: "resp_pacing_seed",
      stream: true,
    }, {
      authorizationScope,
      signal: controller.signal,
      encodedBodyBytes: 1024 ** 2,
      structuralBodyBytes: 1024 ** 2,
    }));
    await flush(100);

    expect(bound.executions).toBe(2);
    expect(system.runtimes.find(runtime => runtime !== bound)!.executions).toBe(0);
    expect(system.router.snapshot().queued).toBe(3);

    for (const controller of controllers) controller.abort();
    await Promise.allSettled(pending);
    await system.router.close();
  });

  it("paces a selected bulk target in a mixed transport fleet", async () => {
    const system = createSystem(2, 4, 2_000);
    const standard = system.runtimes[0]!;
    const bulk = system.runtimes[1]!;
    standard.transportProfile = "standard";
    standard.ackEwmaMs = 1_000_000;
    bulk.transportProfile = "bulk-v1";
    bulk.ackEwmaMs = 1;
    for (const runtime of system.runtimes) {
      runtime.reflectExecutionRisk = false;
      runtime.waitForPostAdmissionAbort = true;
      runtime.notify();
    }
    const controllers = Array.from({ length: 4 }, () => new AbortController());
    const pending = controllers.map((controller, index) => system.router.startRelay({
      model: "gpt-5.6-sol",
      input: `mixed-medium-${index}`,
      stream: true,
    }, {
      signal: controller.signal,
      encodedBodyBytes: 1024 ** 2,
      structuralBodyBytes: 1024 ** 2,
    }));
    await flush(100);

    expect(bulk.executions).toBe(1);
    expect(standard.executions).toBe(0);
    expect(system.router.snapshot().queued).toBe(3);

    for (const controller of controllers) controller.abort();
    await Promise.allSettled(pending);
    await system.router.close();
  });

  for (const scenario of [
    { name: "R5 medium", profile: "bulk-v1" as const, bytes: 1024 ** 2, expected: 1, reserved: 1 },
    { name: "R5 true bulk", profile: "bulk-v1" as const, bytes: Math.floor(3.35 * 1024 ** 2), expected: 1, reserved: 4 },
    { name: "standard rollback", profile: "standard" as const, bytes: Math.floor(2.9 * 1024 ** 2), expected: 8, reserved: 0 },
  ]) {
    it(`keeps ${scenario.name} fleet transfer concurrency exact`, async () => {
      const system = createSystem(8, 8, 1_000, undefined, undefined, undefined, 500);
      for (const runtime of system.runtimes) {
        runtime.transportProfile = scenario.profile;
        runtime.reflectExecutionRisk = false;
        runtime.waitForPostAdmissionAbort = true;
        runtime.notify();
      }
      // A historically fast project must not absorb a synchronized R5 burst
      // while equally idle projects are available. Standard rollback keeps its
      // existing policy and is constrained independently per runtime.
      system.runtimes[0]!.ackEwmaMs = 25;
      system.runtimes.slice(1).forEach(runtime => { runtime.ackEwmaMs = 2_000; });
      system.runtimes.forEach(runtime => runtime.notify());
      const controllers = Array.from({ length: 8 }, () => new AbortController());
      const pending = Array.from({ length: 8 }, (_, index) => system.router.startRelay(
        { model: "gpt-5.6-sol", input: `${scenario.name}-${index}`, stream: true },
        {
          clientId: `${scenario.name}-${index}`,
          source: "websocket",
          signal: controllers[index]!.signal,
          encodedBodyBytes: scenario.bytes,
          structuralBodyBytes: scenario.bytes,
        },
      ));
      await flush(100);

      expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0))
        .toBe(scenario.expected);
      if (scenario.profile === "bulk-v1") {
        expect(system.runtimes.filter(runtime => runtime.executions > 0))
          .toHaveLength(scenario.expected);
      }
      expect(system.router.snapshot().fleetTransferReserved).toBe(scenario.reserved);

      for (const controller of controllers) controller.abort();
      await Promise.allSettled(pending);
      await system.router.close();
      expect(system.router.snapshot().fleetTransferReserved).toBe(0);
    });
  }

  it("uses profile-specific transfer reservations without oversubscribing standard rollback", async () => {
    const launch = (system: ReturnType<typeof createSystem>) => Array.from(
      { length: 4 },
      (_, index) => system.router.startRelay({
        model: "gpt-5.6-sol",
        input: `profile-cost-${index}`,
        service_tier: "fast",
        store: true,
        stream: true,
      }, {
        clientId: `profile-cost-${index}`,
        source: "websocket",
        encodedBodyBytes: 2 * 1024 ** 2,
        structuralBodyBytes: 2 * 1024 ** 2,
      }),
    );

    const standard = createSystem(1, 4, 100, undefined, undefined, undefined, 20);
    standard.runtimes[0]!.transportProfile = "standard";
    standard.runtimes[0]!.waitForAdmissionAbort = true;
    standard.runtimes[0]!.notify();
    const standardPending = launch(standard);
    await flush();
    expect(standard.runtimes[0]!.executions).toBe(1);
    await standard.router.close();
    await Promise.allSettled(standardPending);

    const bulk = createSystem(1, 4, 100, undefined, undefined, undefined, 20);
    bulk.runtimes[0]!.transportProfile = "bulk-v1";
    bulk.runtimes[0]!.waitForAdmissionAbort = true;
    bulk.runtimes[0]!.notify();
    const bulkPending = launch(bulk);
    await flush();
    expect(bulk.runtimes[0]!.executions).toBe(4);
    await bulk.router.close();
    await Promise.allSettled(bulkPending);

    const observed = createSystem(1, 8, 100, undefined, undefined, undefined, 20);
    observed.runtimes[0]!.transportProfile = "bulk-v1";
    observed.runtimes[0]!.waitForAdmissionAbort = true;
    observed.runtimes[0]!.notify();
    const observedPending = Array.from({ length: 4 }, (_, index) =>
      observed.router.startRelay({
        model: "gpt-5.6-sol",
        input: `observed-cost-${index}`,
        service_tier: "fast",
        store: true,
        stream: true,
      }, {
        clientId: `observed-cost-${index}`,
        source: "websocket",
        encodedBodyBytes: 3 * 1024 ** 2,
        structuralBodyBytes: 3 * 1024 ** 2,
      }));
    await flush();
    expect(observed.runtimes[0]!.executions).toBe(3);
    await observed.router.close();
    await Promise.allSettled(observedPending);
  });

  it("keeps small deadlines exact and scales observed long-history admission", () => {
    expect(requestRoutingTimeouts(128 * 1024, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 30_000,
      routeAttemptTimeoutMs: 15_000,
    });
    expect(requestRoutingTimeouts(2 * 1024 ** 2, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 30_000,
      routeAttemptTimeoutMs: 15_000,
    });
    expect(requestRoutingTimeouts(3.75 * 1024 ** 2, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 61_250,
      routeAttemptTimeoutMs: 15_000,
    });
    expect(requestRoutingTimeouts(8 * 1024 ** 2, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 125_000,
      routeAttemptTimeoutMs: 15_000,
    });
    expect(requestRoutingTimeouts(13 * 1024 ** 2, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 125_000,
      routeAttemptTimeoutMs: 24_375,
    });
    expect(requestRoutingTimeouts(64 * 1024 ** 2, 30_000, 15_000)).toEqual({
      queueTimeoutMs: 245_000,
      routeAttemptTimeoutMs: 120_000,
    });
    expect(() => requestRoutingTimeouts(-1, 30_000, 15_000)).toThrow(RangeError);
    expect(() => requestRoutingTimeouts(1, 10_000, 15_000)).toThrow(RangeError);

    expect(admissionProgressFreshnessMs(600)).toBe(5_000);
    expect(admissionProgressFreshnessMs(2_000)).toBe(8_000);
    expect(admissionProgressFreshnessMs(4_000)).toBe(10_000);
    expect(preparedProofGraceMs(0)).toBe(3_000);
    expect(preparedProofGraceMs(600)).toBe(3_000);
    expect(preparedProofGraceMs(1_000)).toBe(4_000);
    expect(preparedProofGraceMs(2_000)).toBe(5_000);
    // At the production 15-second attempt boundary, no progress retries;
    // progress one millisecond earlier stays on this runtime until 19.999s.
    expect(nextAdmissionProgressCheckAtMs(15_000, 30_000, null, 5_000)).toBeNull();
    expect(nextAdmissionProgressCheckAtMs(15_000, 30_000, 14_999, 5_000)).toBe(19_999);
    expect(nextAdmissionProgressCheckAtMs(15_000, 30_000, 10_000, 5_000)).toBeNull();
    // A late-admitted request must not discard transferred bytes when less
    // than one freshness window remains for a full retry. With more time left,
    // the existing stale-progress failover behavior is preserved.
    expect(nextAdmissionProgressCheckAtMs(27_800, 30_000, 21_800, 5_000)).toBe(30_000);
    expect(nextAdmissionProgressCheckAtMs(24_000, 30_000, 18_000, 5_000)).toBeNull();
    expect(nextAdmissionProgressCheckAtMs(15_000, 30_000, 9_337, 5_000, true))
      .toBe(30_000);
    // Fresh progress can never cross the absolute 30-second deadline.
    expect(nextAdmissionProgressCheckAtMs(30_000, 30_000, 29_999, 5_000)).toBeNull();

    const totalEncodedBytes = Math.floor(2.9 * 1024 ** 2);
    const nearComplete = {
      stage: "request_data" as const,
      acknowledgedEncodedBytes: Math.floor(2.25 * 1024 ** 2),
      totalEncodedBytes,
    };
    // Exact reproduction of the live edge: the final ACK is 5.663 seconds old
    // at the 15-second attempt boundary. The remaining copy fits at its proven
    // unique-ACK rate, while a full retransmit plus one freshness window does not.
    expect(shouldRetainStaleAdmissionProgress(
      15_000,
      30_000,
      0,
      9_337,
      5_000,
      nearComplete,
    )).toBe(true);
    // Scant progress still retries, and a comfortably sized retry budget does
    // not pin work to the first runtime.
    expect(shouldRetainStaleAdmissionProgress(
      15_000,
      30_000,
      0,
      9_337,
      5_000,
      { ...nearComplete, acknowledgedEncodedBytes: 512 * 1024 },
    )).toBe(false);
    expect(shouldRetainStaleAdmissionProgress(
      15_000,
      40_000,
      0,
      9_337,
      5_000,
      nearComplete,
    )).toBe(false);
    // A complete body without the exact prepared proof is stalled processing,
    // not useful fresh transfer progress, and must retain failover.
    expect(shouldRetainStaleAdmissionProgress(
      15_000,
      30_000,
      0,
      9_337,
      5_000,
      {
        stage: "request_data",
        acknowledgedEncodedBytes: totalEncodedBytes,
        totalEncodedBytes,
      },
    )).toBe(false);
    expect(shouldRetainStaleAdmissionProgress(
      15_000,
      30_000,
      0,
      1_000,
      5_000,
      {
        stage: "request_prepared",
        acknowledgedEncodedBytes: totalEncodedBytes,
        totalEncodedBytes,
      },
    )).toBe(true);
  });

  it("hands the payload-aware global deadline through an HTTP ingress lease", async () => {
    const system = createSystem(1, 1);
    const acceptedAtMs = performance.timeOrigin + performance.now() - 1;
    let replacementDeadlineMs: number | undefined;
    let replacementRequestBytes = 0;
    let resourceLease!: ResourceLease;
    resourceLease = {
      reservedBytes: 96 * 1024 ** 2,
      replace: async () => resourceLease,
      release: () => undefined,
    };
    const incomingLease: IncomingResourceLease = {
      reservedBytes: 32 * 1024 ** 2,
      bodyBytes: 13 * 1024 ** 2,
      acceptedAtMs,
      deadlineMs: acceptedAtMs + 30_000,
      replace: async (workload, _signal, options) => {
        replacementRequestBytes = workload.requestBytes;
        replacementDeadlineMs = options?.deadlineMs;
        return resourceLease;
      },
      release: () => undefined,
    };

    const handle = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "body bytes are carried by the ingress lease",
      service_tier: "fast",
      stream: true,
    }, { source: "http", incomingLease });
    expect(replacementRequestBytes).toBe(13 * 1024 ** 2 + 13);
    expect(replacementDeadlineMs).toBe(acceptedAtMs + 125_000);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("extends declared and chunked large-body ingress without changing the small base", async () => {
    const system = createSystem(1, 1);
    const declared = await system.router.acquireIncomingBody(13 * 1024 ** 2, undefined, {
      maxBytes: 64 * 1024 ** 2,
    });
    expect(declared.deadlineMs! - declared.acceptedAtMs!).toBe(125_000);
    declared.release();

    const chunked = await system.router.acquireIncomingBody(0, undefined, {
      maxBytes: 64 * 1024 ** 2,
    });
    expect(chunked.deadlineMs! - chunked.acceptedAtMs!).toBe(30_000);
    await chunked.ensureBytes!(13 * 1024 ** 2);
    expect(chunked.deadlineMs! - chunked.acceptedAtMs!).toBe(125_000);
    chunked.release();
    await system.router.close();
  });

  it("releases replacement permits when ingress release wins the async handoff", async () => {
    const system = createSystem(1, 1);
    const growing = await system.router.acquireIncomingBody(0, undefined, {
      maxBytes: 64 * 1024 ** 2,
    });
    const growth = growing.ensureBytes!(13 * 1024 ** 2);
    growing.release();
    await expect(growth).rejects.toThrow("released during growth");
    expect(system.governor.snapshot().reservedBytes).toBe(0);

    const replacing = await system.router.acquireIncomingBody(1024);
    const replacement = replacing.replace!({
      class: "text-small",
      requestBytes: 1024,
      reservedBytes: 4 * 1024 ** 2,
      stream: true,
    });
    replacing.release();
    await expect(replacement).rejects.toThrow("released during replacement");
    expect(system.governor.snapshot().reservedBytes).toBe(0);

    const retained = await system.router.acquireIncomingBody(0, undefined, {
      maxBytes: 64 * 1024 ** 2,
    });
    const retainedGrowth = retained.ensureBytes!(13 * 1024 ** 2);
    retained.completeAdmission?.();
    await retainedGrowth;
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);
    retained.release();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("allows a retained completed ingress lease to hand off during fallback", async () => {
    const system = createSystem(1, 1);
    const incoming = await system.router.acquireIncomingBody(1024);
    incoming.completeAdmission?.();
    const replacement = await incoming.replace!({
      class: "text-large",
      requestBytes: 2 * 1024 ** 2,
      reservedBytes: 16 * 1024 ** 2,
      stream: true,
    });

    expect(replacement.reservedBytes).toBe(16 * 1024 ** 2);
    replacement.release();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("does not return a granted incoming lease after its abort wins the resume race", async () => {
    const system = createSystem(1, 1);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    const controller = new AbortController();
    const pending = system.router.acquireIncomingBody(0, controller.signal);
    await Promise.resolve();
    blocker.release();
    const reason = new DOMException("client disconnected", "AbortError");
    controller.abort(reason);

    await expect(pending).rejects.toBe(reason);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("observes aborts between immutable snapshot completion and queue listener setup", async () => {
    const system = createSystem(0, 1, 100);
    const controller = new AbortController();
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "abort during immutable handoff",
      stream: true,
    }, { signal: controller.signal });
    queueMicrotask(() => controller.abort(new DOMException("client disconnected", "AbortError")));

    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(system.router.snapshot()).toMatchObject({ queued: 0, active: 0 });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("retains admitted memory through the declared graph lifetime after queue failure", async () => {
    const system = createSystem(0, 1, 20);
    const retained = deferred<void>();
    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "retained source graph",
      stream: true,
    }, {
      source: "image",
      retainMemoryUntil: retained.promise,
    })).rejects.toMatchObject({ code: "router_capacity_timeout" });
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);
    const idle = system.router.awaitIdle(200);
    expect(await Promise.race([
      idle.then(() => "idle" as const),
      Bun.sleep(10).then(() => "retained" as const),
    ])).toBe("retained");

    retained.resolve();
    await flush();
    await expect(idle).resolves.toBeUndefined();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("keeps cooperative snapshot work visible to shutdown until it is aborted", async () => {
    const system = createSystem(0, 1, 1_000);
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: Array.from({ length: 20_000 }, (_, index) => ({
        type: "input_text",
        text: `snapshot-${index}`,
      })),
      stream: true,
    });
    for (let turn = 0;
      turn < 100 && system.governor.snapshot().reservedBytes === 0;
      turn++) await Promise.resolve();
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);

    system.router.beginShutdown();
    await expect(pending).rejects.toMatchObject({ code: "router_shutting_down" });
    await expect(system.router.awaitIdle(1_000)).resolves.toBeUndefined();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("does not retry host-local CPU overload through another sandbox", async () => {
    const system = createSystem(2, 1);
    for (const runtime of system.runtimes) {
      runtime.failStarts = 1;
      runtime.failError = new CpuTransformQueueFullError();
    }

    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "local-overload",
      stream: true,
    })).rejects.toBeInstanceOf(CpuTransformQueueFullError);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("rejects malformed nested function arguments before sandbox admission", async () => {
    const system = createSystem(2, 1);

    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: [{
        type: "function_call",
        call_id: "call_bad_json",
        name: "read",
        arguments: "{\"path\":",
      }],
      stream: true,
    })).rejects.toMatchObject({
      name: "ResponsesInputJsonError",
      status: 400,
      code: "invalid_function_call_arguments",
      param: "input[0].arguments",
    });
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(0);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("rejects provider-incompatible Unicode before sandbox admission", async () => {
    const system = createSystem(2, 1);

    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: [{
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: "broken-\ud800" }],
      }],
      stream: true,
    })).rejects.toMatchObject({
      name: "ResponsesInputUnicodeError",
      status: 400,
      code: "invalid_unicode",
      param: "input[0]",
    });
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(0);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("fails a deterministic oversized request once without exhausting the fleet", async () => {
    const system = createSystem(8, 1, 1_000);
    const error = new V2PreCommitError("request too large", "request_too_large");
    for (const runtime of system.runtimes) {
      runtime.failStarts = 100;
      runtime.failError = error;
    }

    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "oversized",
      stream: true,
    })).rejects.toBe(error);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("preserves deterministic overflow even when it arrives after the admission deadline", async () => {
    const system = createSystem(1, 1, 10);
    const error = new V2PreCommitError("request too large", "request_too_large");
    system.runtimes[0]!.failStarts = 1;
    system.runtimes[0]!.failError = error;
    system.runtimes[0]!.failDelayMs = 20;

    await expect(system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "late-oversized",
      stream: true,
    })).rejects.toBe(error);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("expires zero-capacity queueing with a typed service-unavailable error", async () => {
    const system = createSystem(0, 1, 10);
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "no-capacity", stream: true },
      { clientId: "timeout", source: "websocket" },
    );
    const result = await Promise.race([
      pending.then(() => "resolved", error => error),
      Bun.sleep(50).then(() => "client-idle-timeout"),
    ]);
    await system.router.close();
    expect(result).toMatchObject({
      status: 503,
      code: "router_capacity_timeout",
    });
    expect(system.router.snapshot().queued).toBe(0);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
  });

  it("bounds the admitted routing queue and releases an overflow lease", async () => {
    const system = createSystem(0, 1, 1_000, 1);
    const first = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "first", stream: true },
      { clientId: "first", source: "websocket" },
    );
    await flush();
    const reserved = system.governor.snapshot().reservedBytes;
    expect(reserved).toBeGreaterThan(0);

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "overflow", stream: true },
      { clientId: "second", source: "websocket" },
    )).rejects.toMatchObject({ status: 503, code: "router_queue_full" });
    expect(system.router.snapshot().queued).toBe(1);
    expect(system.governor.snapshot().reservedBytes).toBe(reserved);

    await system.router.close();
    await expect(first).rejects.toMatchObject({ code: "router_shutting_down" });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
  });

  it("keeps a bounded queue permit for a safely retryable pre-commit request", async () => {
    const system = createSystem(1, 1, 500, 1);
    const runtime = system.runtimes[0]!;
    runtime.failStarts = 1;
    runtime.failDelayMs = 40;
    const retrying = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "must retain retry capacity", stream: true },
      { clientId: "retry-capacity", source: "websocket" },
    );
    while (runtime.executions === 0) await Promise.resolve();

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "cannot steal retry capacity", stream: true },
      { clientId: "competing", source: "websocket" },
    )).rejects.toMatchObject({ code: "router_queue_full" });

    const handle = await retrying;
    expect(runtime.executions).toBe(2);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
  });

  it("releases lightweight incoming leases on success and pre-admission failure", async () => {
    const system = createSystem(1, 1);
    let successfulReleases = 0;
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "lightweight lease", stream: true },
      {
        incomingLease: {
          reservedBytes: 1,
          release: () => { successfulReleases++; },
        },
      },
    );
    expect(successfulReleases).toBe(1);
    system.runtimes[0]!.completeOne();
    await handle.final;

    let rejectedReleases = 0;
    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "invalid sizing", stream: true },
      {
        encodedBodyBytes: -1,
        incomingLease: {
          reservedBytes: 1,
          release: () => { rejectedReleases++; },
        },
      },
    )).rejects.toBeInstanceOf(RangeError);
    expect(rejectedReleases).toBe(1);

    const cyclic: Record<string, unknown> = {
      model: "gpt-5.6-sol",
      input: "cyclic structural sizing",
      stream: true,
    };
    cyclic.self = cyclic;
    let structuralFailureReleases = 0;
    await expect(system.router.startRelay(cyclic, {
      incomingLease: {
        reservedBytes: 1,
        release: () => { structuralFailureReleases++; },
      },
    })).rejects.toBeInstanceOf(TypeError);
    expect(structuralFailureReleases).toBe(1);

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "throwing lease release", stream: true },
      {
        incomingLease: {
          reservedBytes: 1,
          release: () => { throw new Error("synthetic incoming release failure"); },
        },
      },
    )).rejects.toThrow("synthetic incoming release failure");
    expect(system.governor.snapshot().reservedBytes).toBe(0);

    await system.router.close();
    expect(system.governor.snapshot().reservedBytes).toBe(0);

    let closedRouterReleases = 0;
    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "closed router", stream: true },
      {
        incomingLease: {
          reservedBytes: 1,
          release: () => { closedRouterReleases++; },
        },
      },
    )).rejects.toMatchObject({ code: "router_shutting_down" });
    expect(closedRouterReleases).toBe(1);

    const closed = createSystem(1, 1);
    closed.governor.close();
    let admissionFailureReleases = 0;
    await expect(closed.router.startRelay(
      { model: "gpt-5.6-sol", input: "closed governor", stream: true },
      {
        incomingLease: {
          reservedBytes: 1,
          release: () => { admissionFailureReleases++; },
        },
      },
    )).rejects.toMatchObject({ code: "resource_governor_closed" });
    expect(admissionFailureReleases).toBe(1);
    await closed.router.close();
  });

  it("bounds queued request bytes atomically", async () => {
    const system = createSystem(0, 1, 1_000, 10, 1);
    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "larger-than-one-byte", stream: true },
      { clientId: "byte-overflow", source: "websocket" },
    )).rejects.toMatchObject({ status: 503, code: "router_queue_full" });
    expect(system.router.snapshot()).toMatchObject({ queued: 0, queuedBytes: 0 });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("retains one absolute queue deadline across a safe startup retry", async () => {
    const system = createSystem(1, 1, 20);
    system.runtimes[0].failStarts = 1;
    system.runtimes[0].failDelayMs = 15;
    const started = performance.now();
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "retry-timeout", stream: true },
      { clientId: "retry-timeout", source: "websocket" },
    );
    const result = await Promise.race([
      pending.then(() => "resolved", error => error),
      Bun.sleep(50).then(() => "client-idle-timeout"),
    ]);
    const elapsed = performance.now() - started;
    await system.router.close();
    expect(result).toMatchObject({ code: "transport_admission_timeout" });
    expect(elapsed).toBeLessThan(32);
    expect(system.runtimes[0].executions).toBe(1);
  });

  it("retries one transient pre-commit failure on the same live incarnation", async () => {
    const system = createSystem(1, 1, 500);
    const runtime = system.runtimes[0];
    runtime.failStarts = 1;
    const incarnation = runtime.incarnation;
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "transient-same-incarnation", stream: true },
      { clientId: "transient", source: "websocket" },
    );
    await flush();
    expect(runtime.executions).toBe(1);
    await Bun.sleep(5);
    expect(runtime.executions).toBe(1);

    const handle = await pending;
    expect(runtime.incarnation).toBe(incarnation);
    expect(runtime.executions).toBe(2);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("keeps the relay body immutable across a safe runtime retry", async () => {
    const system = createSystem(2, 1, 500);
    const first = system.runtimes[1]!;
    first.failStarts = 1;
    first.mutateBeforeFailure = body => {
      const input = body.input as Array<Record<string, unknown>>;
      expect(Object.isFrozen(body)).toBe(true);
      expect(Object.isFrozen(input)).toBe(true);
      expect(Object.isFrozen(input[0])).toBe(true);
      try {
        input[0]!.text = "mutated by failed runtime";
      } catch {
        // Strict-mode mutation rejection is the expected immutable boundary.
      }
    };
    const original = {
      model: "gpt-5.6-sol",
      input: [{ type: "input_text", text: "original retry body" }],
      stream: true,
    };

    const handle = await system.router.startRelay(original, {
      clientId: "immutable-retry",
      source: "websocket",
    });
    const successful = system.runtimes.find(runtime => runtime.finals.length > 0)!;
    expect(successful.bodies.at(-1)?.input).toEqual([
      { type: "input_text", text: "original retry body" },
    ]);
    expect(original.input[0]!.text).toBe("original retry body");
    successful.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("re-sizes the frozen body when caller mutation wins the admission wait", async () => {
    const system = createSystem(1, 1, 1_000);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    system.runtimes[0]!.postAdmissionDelayMs = 50;
    const body = {
      model: "gpt-5.6-sol",
      input: [{ type: "input_text", text: "x" }],
      stream: true,
    };
    const pending = system.router.startRelay(body, {
      clientId: "mutation-resize",
      source: "internal",
    });
    await Promise.resolve();
    body.input[0]!.text = "x".repeat(2 * 1024 ** 2);
    blocker.release();
    for (let turn = 0; turn < 100 && system.runtimes[0]!.executions === 0; turn++) {
      await Bun.sleep(1);
    }

    expect((system.runtimes[0]!.bodies[0]!.input as any[])[0].text.length)
      .toBe(2 * 1024 ** 2);
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThanOrEqual(16 * 1024 ** 2);
    const handle = await pending;
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("re-measures a frozen mutation after an exact structural hint becomes stale", async () => {
    const system = createSystem(1, 1, 1_000);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    system.runtimes[0]!.postAdmissionDelayMs = 50;
    const body = {
      model: "gpt-5.6-sol",
      input: [{ type: "input_text", text: "x" }],
      service_tier: "fast",
      store: true,
      stream: true,
    };
    const initialBytes = Buffer.byteLength(JSON.stringify(body));
    const pending = system.router.startRelay(body, {
      clientId: "mutation-stale-structural-hint",
      source: "internal",
      encodedBodyBytes: initialBytes,
      structuralBodyBytes: initialBytes,
    });
    while (system.governor.snapshot().queued === 0) await Promise.resolve();
    body.input[0]!.text = "x".repeat(2 * 1024 ** 2);
    blocker.release();
    for (let turn = 0; turn < 100 && system.runtimes[0]!.executions === 0; turn++) {
      await Bun.sleep(1);
    }

    const executed = system.runtimes[0]!.bodies[0]!;
    const executedBytes = Buffer.byteLength(JSON.stringify(executed));
    expect((executed.input as any[])[0].text.length).toBe(2 * 1024 ** 2);
    expect(system.governor.snapshot().reservedBytes)
      .toBeGreaterThanOrEqual(executedBytes * 6 + 4 * 1024 ** 2);
    const handle = await pending;
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("preserves a typed router deadline while queued for resource admission", async () => {
    const system = createSystem(0, 1, 1_000);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    const controller = new AbortController();
    const deadline = new RouterCapacityTimeoutError("synthetic ingress deadline");
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "typed admission deadline",
      stream: true,
    }, { signal: controller.signal });
    while (system.governor.snapshot().queued === 0) await Promise.resolve();
    controller.abort(deadline);

    await expect(pending).rejects.toBe(deadline);
    blocker.release();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("re-normalizes store and stream on the exact frozen admission snapshot", async () => {
    const system = createSystem(1, 1, 1_000);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    const body: Record<string, unknown> = {
      model: "gpt-5.6-sol",
      input: "policy before admission",
      service_tier: "fast",
      store: true,
      stream: true,
    };
    const pending = system.router.startRelay(body);
    while (system.governor.snapshot().queued === 0) await Promise.resolve();
    body.store = false;
    body.stream = false;
    body.input = "mutation captured by frozen snapshot";
    blocker.release();

    const handle = await pending;
    expect(system.runtimes[0]!.bodies).toHaveLength(1);
    expect(system.runtimes[0]!.bodies[0]).toMatchObject({
      input: "mutation captured by frozen snapshot",
      store: true,
      stream: true,
    });
    expect(Object.isFrozen(system.runtimes[0]!.bodies[0])).toBe(true);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("binds a mutated continuation only from the frozen scoped snapshot", async () => {
    const authorizationScope = createAuthenticatedTenantScope();
    const system = createSystem(
      2,
      1,
      1_000,
      undefined,
      undefined,
      undefined,
      undefined,
      authorizationScope,
    );
    const seed = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "seed lineage",
      service_tier: "fast",
      store: true,
      stream: true,
    });
    const bound = system.runtimes.find(runtime => runtime.executions === 1)!;
    const other = system.runtimes.find(runtime => runtime !== bound)!;
    bound.completeOne({ id: "resp_mutation_seed", status: "completed" });
    await seed.final;
    await flush();
    bound.ackEwmaMs = 10_000;
    other.ackEwmaMs = 1;
    bound.notify();
    other.notify();

    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    const body: Record<string, unknown> = {
      model: "gpt-5.6-sol",
      input: "continue after wait",
      service_tier: "fast",
      store: true,
      stream: true,
    };
    const pending = system.router.startRelay(body, { authorizationScope });
    while (system.governor.snapshot().queued === 0) await Promise.resolve();
    body.previous_response_id = "resp_mutation_seed";
    blocker.release();

    const continuation = await pending;
    expect(bound.executions).toBe(2);
    expect(other.executions).toBe(0);
    expect(bound.bodies.at(-1)?.previous_response_id).toBe("resp_mutation_seed");
    bound.completeOne();
    await continuation.final;
    await system.router.close();
  });

  it("fails closed when an unscoped frozen snapshot gains a continuation", async () => {
    const system = createSystem(1, 1, 1_000);
    const blocker = system.governor.reserveFixedBytes(
      system.governor.snapshot().reservationCapacityBytes,
    );
    const body: Record<string, unknown> = {
      model: "gpt-5.6-sol",
      input: "initially standalone",
      service_tier: "fast",
      store: true,
      stream: true,
    };
    const pending = system.router.startRelay(body);
    while (system.governor.snapshot().queued === 0) await Promise.resolve();
    body.previous_response_id = "resp_added_during_admission";
    blocker.release();

    await expect(pending).rejects.toMatchObject({
      code: "response_provenance_scope_mismatch",
      committed: false,
    });
    expect(system.runtimes[0]!.executions).toBe(0);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("wakes a blocked same-incarnation retry on a meaningful health change", async () => {
    const system = createSystem(1, 1, 500);
    const runtime = system.runtimes[0];
    runtime.failStarts = 1;
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "health-recovered", stream: true },
      { clientId: "health-recovered", source: "websocket" },
    );
    await flush();
    expect(runtime.executions).toBe(1);
    runtime.state = "draining";
    runtime.notify();
    runtime.state = "ready";
    runtime.notify();

    const handle = await pending;
    expect(runtime.incarnation).toBe(1);
    expect(runtime.executions).toBe(2);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("backs off persistent pre-commit failures without extending the deadline", async () => {
    const system = createSystem(1, 1, 120);
    const runtime = system.runtimes[0];
    runtime.failStarts = 100;
    const started = performance.now();
    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "persistent-precommit", stream: true },
      { clientId: "persistent-precommit", source: "websocket" },
    )).rejects.toMatchObject({ code: "transport_admission_timeout" });
    const elapsed = performance.now() - started;
    expect(runtime.executions).toBeLessThanOrEqual(3);
    expect(runtime.executions).toBeGreaterThanOrEqual(1);
    expect(elapsed).toBeGreaterThanOrEqual(100);
    expect(elapsed).toBeLessThan(300);
    await system.router.close();
  });

  it("does not let self-induced admission reflections bypass retry backoff", async () => {
    const system = createSystem(1, 1, 2_000);
    const runtime = system.runtimes[0];
    runtime.failStarts = 5;
    runtime.pulseAdmissionOnFailure = true;
    const started = performance.now();
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "self-reflection-backoff", stream: true },
      { clientId: "self-reflection-backoff", source: "websocket" },
    );
    const elapsed = performance.now() - started;
    expect(runtime.executions).toBe(6);
    expect(elapsed).toBeGreaterThanOrEqual(550);
    expect(runtime.executionStartedAtMs[1]! - runtime.executionStartedAtMs[0]!)
      .toBeGreaterThanOrEqual(20);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("bounds runtime preparation with the same absolute admission deadline", async () => {
    const system = createSystem(1, 1, 20);
    system.runtimes[0].waitForAdmissionAbort = true;
    const started = performance.now();
    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "stuck-preparation", stream: true },
      { clientId: "stuck-preparation", source: "websocket" },
    )).rejects.toMatchObject({ code: "transport_admission_timeout" });
    expect(performance.now() - started).toBeLessThan(60);
    expect(system.router.snapshot()).toMatchObject({ active: 0, queued: 0 });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("classifies a generic runtime rejection after caller abort as ambiguous", async () => {
    const system = createSystem(1, 1, 500);
    const runtime = system.runtimes[0]!;
    runtime.waitForAdmissionAbort = true;
    runtime.admissionAbortError = new Error("generic cancellation race");
    const controller = new AbortController();
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "generic abort rejection",
      stream: true,
    }, {
      source: "websocket",
      signal: controller.signal,
    });
    await flush();
    expect(runtime.executions).toBe(1);
    controller.abort(new DOMException("client disconnected", "AbortError"));

    await expect(pending).rejects.toMatchObject({
      code: "caller_cancellation_unproven",
      committed: true,
      ambiguous: true,
    });
    expect(runtime.executions).toBe(1);
    await system.router.close();
  });

  it("keeps a cancellation-unproven runtime charged until its late cleanup settles", async () => {
    const system = createSystem(1, 1, 500);
    const runtime = system.runtimes[0]!;
    runtime.ignoreAdmissionAbortMs = 100;
    const controller = new AbortController();
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "late cancellation cleanup",
      stream: true,
    }, { source: "websocket", signal: controller.signal });
    await flush();
    controller.abort(new DOMException("client disconnected", "AbortError"));

    await expect(pending).rejects.toMatchObject({
      code: "caller_cancellation_unproven",
      committed: true,
    });
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);
    await expect(system.router.awaitIdle(30)).rejects.toMatchObject({
      code: "router_shutting_down",
    });

    await Bun.sleep(90);
    await flush();
    await expect(system.router.awaitIdle(100)).resolves.toBeUndefined();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("retains fleet transfer weight until an ambiguous late runtime settles", async () => {
    const system = createSystem(2, 1, 1_000);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.holdBeforeExecutionRisk = true;
      runtime.reflectExecutionRisk = false;
      runtime.transferCredits = 6;
      runtime.notify();
    }
    const controller = new AbortController();
    const body = {
      model: "gpt-5.6-sol",
      input: "late weighted transfer",
      stream: true,
    };
    const bytes = Math.floor(3.1 * 1024 ** 2);
    const first = system.router.startRelay(body, {
      signal: controller.signal,
      encodedBodyBytes: bytes,
      structuralBodyBytes: bytes,
    });
    for (let turn = 0;
      turn < 100 && system.runtimes.reduce((sum, runtime) => (
        sum + runtime.preExecutionHolds.length
      ), 0) < 1;
      turn++) await Bun.sleep(1);
    controller.abort(new DOMException("client disconnected", "AbortError"));
    await expect(first).rejects.toMatchObject({
      code: "caller_cancellation_unproven",
      committed: true,
      ambiguous: true,
    });

    const second = system.router.startRelay({ ...body, input: "second weighted transfer" }, {
      encodedBodyBytes: bytes,
      structuralBodyBytes: bytes,
    });
    await Bun.sleep(20);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    expect(system.router.snapshot().fleetTransferReserved).toBe(4);

    const firstRuntime = system.runtimes.find(runtime => runtime.preExecutionHolds.length > 0)!;
    firstRuntime.preExecutionHolds.shift()!.release();
    for (let turn = 0;
      turn < 100 && system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0) < 2;
      turn++) await Bun.sleep(1);
    const secondRuntime = system.runtimes.find(runtime => runtime.preExecutionHolds.length > 0)!;
    secondRuntime.preExecutionHolds.shift()!.release();
    const secondHandle = await second;
    for (const runtime of system.runtimes) {
      while (runtime.finals.length > 0) runtime.completeOne();
    }
    await secondHandle.final;
    await expect(system.router.awaitIdle(1_000)).resolves.toBeUndefined();
    await system.router.close();
  });

  it("fails over a safely unadmitted route before the global deadline", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    system.runtimes[1]!.waitForAdmissionAbort = true;
    const started = performance.now();

    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "early-safe-failover", stream: true },
      { clientId: "early-safe-failover", source: "websocket" },
    );

    expect(performance.now() - started).toBeLessThan(150);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("fails over after transport admission while R2 execution is still provably safe", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const selected = system.runtimes[1]!;
    selected.reflectExecutionRisk = false;
    selected.waitForPostAdmissionAbort = true;
    const started = performance.now();

    const outcome = await Promise.race([
      system.router.startRelay(
        { model: "gpt-5.6-sol", input: "admitted-but-not-started", stream: true },
        {
          clientId: "admitted-but-not-started",
          source: "websocket",
          encodedBodyBytes: 128 * 1024,
        },
      ).then(handle => ({ kind: "handle" as const, handle })),
      Bun.sleep(120).then(() => ({ kind: "timeout" as const })),
    ]);

    if (outcome.kind === "timeout") {
      await system.router.close();
      throw new Error("safe admitted work was stranded on its first runtime");
    }
    expect(performance.now() - started).toBeLessThan(100);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    expect(selected.admissionProgressCallbacks).toHaveLength(0);
    system.runtimes[0]!.completeOne();
    await outcome.handle.final;
    await system.router.close();
  });

  for (const requestBytes of [2_012_715, 2 * 1024 ** 2]) {
    it(`keeps delayed medium bulk ACK progress through 15 seconds for ${requestBytes} bytes`, async () => {
      const fixture = mediumAdmissionFixture(requestBytes);
      try {
        await flush(100);
        expect(Buffer.byteLength(JSON.stringify(fixture.body))).toBe(requestBytes);
        const selected = fixture.controls[1]!;
        const attempt = selected.attempts[0]!;
        expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);

        // The live failure cancelled at 5 seconds, before the first real ACK.
        // This transfer first acknowledges at 8 seconds and commits at 22.
        for (const [elapsedMs, acknowledgedEncodedBytes] of [
          [8_000, 400_000], [12_000, 800_000], [16_000, 1_200_000], [20_000, 1_600_000],
        ]) {
          await fixture.timers.advanceTo(elapsedMs!);
          expect(attempt.signal?.aborted).toBe(false);
          expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
          attempt.progress({
            stage: "request_data", acknowledgedEncodedBytes: acknowledgedEncodedBytes!,
            totalEncodedBytes: requestBytes,
          });
        }
        await fixture.timers.advanceTo(22_000);
        expect(attempt.signal?.aborted).toBe(false);
        attempt.progress({
          stage: "request_prepared", acknowledgedEncodedBytes: requestBytes,
          totalEncodedBytes: requestBytes,
        });
        attempt.commit();
        const handle = await fixture.pending;
        expect(fixture.controls.map(control => control.providerStarts)).toEqual([0, 1]);
        expect(fixture.controls.map(control => control.safeAborts)).toEqual([0, 0]);
        fixture.runtimes[1]!.completeOne();
        await handle.final;
        await fixture.timers.advanceTo(30_001);
        expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
        expect(fixture.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
        expect(fixture.governor.snapshot().reservedBytes).toBe(0);
      } finally {
        await fixture.close();
      }
    });
  }

  it("reroutes medium bulk work with no ACK at 15 seconds and expires it at 30 seconds", async () => {
    const fixture = mediumAdmissionFixture();
    try {
      await flush(100);
      const first = fixture.controls[1]!.attempts[0]!;
      await fixture.timers.advanceTo(14_999);
      expect(first.signal?.aborted).toBe(false);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);

      await fixture.timers.advanceTo(15_000);
      expect(first.signal?.aborted).toBe(true);
      expect(fixture.controls[1]!.safeAborts).toBe(1);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
      expect(fixture.runtimes[0]!.bodies[0]).toBe(fixture.runtimes[1]!.bodies[0]);
      const second = fixture.controls[0]!.attempts[0]!;
      await fixture.timers.advanceTo(29_999);
      expect(second.signal?.aborted).toBe(false);
      first.progress({
        stage: "request_prepared", acknowledgedEncodedBytes: fixture.requestBytes,
        totalEncodedBytes: fixture.requestBytes,
      });
      await fixture.timers.advanceTo(30_000);
      expect(await fixture.outcome).toMatchObject({ error: { code: "transport_admission_timeout" } });
      expect(second.signal?.aborted).toBe(true);
      expect(fixture.controls.map(control => control.safeAborts)).toEqual([1, 1]);
      expect(fixture.controls.map(control => control.providerStarts)).toEqual([0, 0]);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
      expect(fixture.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
      expect(fixture.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it("never extends fresh medium bulk ACK progress beyond the 30-second deadline", async () => {
    const fixture = mediumAdmissionFixture();
    try {
      await flush(100);
      const attempt = fixture.controls[1]!.attempts[0]!;
      for (const elapsedMs of [8_000, 12_000, 16_000, 20_000, 24_000, 28_000, 29_999]) {
        await fixture.timers.advanceTo(elapsedMs);
        expect(attempt.signal?.aborted).toBe(false);
        attempt.progress({
          stage: "request_data", acknowledgedEncodedBytes: elapsedMs * 40,
          totalEncodedBytes: fixture.requestBytes,
        });
      }
      await fixture.timers.advanceTo(30_000);
      expect(await fixture.outcome).toMatchObject({ error: { code: "transport_admission_timeout" } });
      expect(attempt.signal?.aborted).toBe(true);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
      expect(fixture.controls.map(control => control.providerStarts)).toEqual([0, 0]);
      attempt.progress({
        stage: "request_prepared", acknowledgedEncodedBytes: fixture.requestBytes,
        totalEncodedBytes: fixture.requestBytes,
      });
      await fixture.timers.advanceTo(35_000);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
      expect(fixture.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
      expect(fixture.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  it("bounds medium bulk prepared-proof wait after the full body ACK", async () => {
    const fixture = mediumAdmissionFixture();
    try {
      await flush(100);
      const first = fixture.controls[1]!.attempts[0]!;
      await fixture.timers.advanceTo(8_000);
      first.progress({
        stage: "request_data", acknowledgedEncodedBytes: 400_000,
        totalEncodedBytes: fixture.requestBytes,
      });
      await fixture.timers.advanceTo(12_000);
      first.progress({
        stage: "request_data", acknowledgedEncodedBytes: fixture.requestBytes,
        totalEncodedBytes: fixture.requestBytes,
      });
      // RTT 600 ms gives exactly 3 seconds for assembly/claim proof. Repeated
      // full-data ACKs must not reset that grace or impersonate prepared proof.
      await fixture.timers.advanceTo(14_999);
      expect(first.signal?.aborted).toBe(false);
      first.progress({
        stage: "request_data", acknowledgedEncodedBytes: fixture.requestBytes,
        totalEncodedBytes: fixture.requestBytes,
      });
      await fixture.timers.advanceTo(15_000);
      expect(first.signal?.aborted).toBe(true);
      expect(fixture.controls[1]!.safeAborts).toBe(1);
      expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
      expect(fixture.controls.map(control => control.providerStarts)).toEqual([0, 0]);
      fixture.controls[0]!.attempts[0]!.commit();
      const handle = await fixture.pending;
      expect(fixture.controls.map(control => control.providerStarts)).toEqual([1, 0]);
      fixture.runtimes[0]!.completeOne();
      await handle.final;
      expect(fixture.router.snapshot()).toMatchObject({ active: 0, queued: 0, fleetTransferReserved: 0 });
      expect(fixture.governor.snapshot().reservedBytes).toBe(0);
    } finally {
      await fixture.close();
    }
  });

  for (const scenario of [
    { name: "latency-critical bulk", requestBytes: 256 * 1024, profile: "bulk-v1" as const },
    { name: "standard-profile medium", requestBytes: 2_012_715, profile: "standard" as const },
  ]) {
    it(`preserves the normal attempt timeout for ${scenario.name} work`, async () => {
      const fixture = mediumAdmissionFixture(scenario.requestBytes, scenario.profile);
      try {
        await flush(100);
        const first = fixture.controls[1]!.attempts[0]!;
        await fixture.timers.advanceTo(14_999);
        first.progress({
          stage: "request_data", acknowledgedEncodedBytes: 100_000,
          totalEncodedBytes: scenario.requestBytes,
        });
        await fixture.timers.advanceTo(15_000);
        expect(first.signal?.aborted).toBe(true);
        expect(fixture.controls[1]!.safeAborts).toBe(1);
        expect(fixture.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
        fixture.controls[0]!.attempts[0]!.commit();
        const handle = await fixture.pending;
        expect(fixture.controls.map(control => control.providerStarts)).toEqual([1, 0]);
        fixture.runtimes[0]!.completeOne();
        await handle.final;
        expect(fixture.governor.snapshot().reservedBytes).toBe(0);
      } finally {
        await fixture.close();
      }
    });
  }

  it("retries a 2.9 MiB R5 attempt at its first deadline when no progress arrives", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 25);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const selected = system.runtimes[1]!;
    selected.reflectExecutionRisk = false;
    selected.waitForPostAdmissionAbort = true;
    const started = performance.now();

    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "large-no-progress", stream: true },
      {
        clientId: "large-no-progress",
        source: "websocket",
        encodedBodyBytes: Math.floor(2.9 * 1024 ** 2),
        structuralBodyBytes: Math.floor(2.9 * 1024 ** 2),
      },
    );
    const elapsed = performance.now() - started;

    expect(elapsed).toBeGreaterThanOrEqual(20);
    expect(elapsed).toBeLessThan(120);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    expect(selected.admissionProgressCallbacks).toHaveLength(1);
    expect(selected.lastRunSignal?.aborted).toBe(true);
    // A late ACK callback from the retired attempt cannot mutate the accepted
    // replacement or dispatch a third copy.
    selected.emitAdmissionProgress();
    await flush();
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("keeps fresh 2.9 MiB R5 progress on one runtime but never extends the hard deadline", async () => {
    const system = createSystem(2, 1, 70, undefined, undefined, undefined, 20);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const selected = system.runtimes[1]!;
    selected.ackEwmaMs = 600;
    selected.reflectExecutionRisk = false;
    selected.waitForPostAdmissionAbort = true;
    const started = performance.now();
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "large-progressing", stream: true },
      {
        clientId: "large-progressing",
        source: "websocket",
        encodedBodyBytes: Math.floor(2.9 * 1024 ** 2),
        structuralBodyBytes: Math.floor(2.9 * 1024 ** 2),
      },
    );
    await flush();
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    expect(selected.admissionProgressCallbacks).toHaveLength(1);

    const progressTimer = setTimeout(() => selected.emitAdmissionProgress(), 15);
    await Bun.sleep(40);
    clearTimeout(progressTimer);
    expect(selected.lastRunSignal?.aborted).toBe(false);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);

    const result = await pending.then(() => null, error => error);
    const elapsed = performance.now() - started;
    expect(result).toMatchObject({ code: "transport_admission_timeout" });
    expect(elapsed).toBeGreaterThanOrEqual(55);
    expect(elapsed).toBeLessThan(130);
    expect(selected.lastRunSignal?.aborted).toBe(true);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    selected.emitAdmissionProgress();
    await Bun.sleep(30);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    await system.router.close();
  });

  it("retries stale 2.9 MiB progress when the scaled deadline can fit a full failover", async () => {
    let clockOffsetMs = 0;
    const clock: Clock = {
      now: () => performance.timeOrigin + performance.now() + clockOffsetMs,
      sleep: async ms => { await Bun.sleep(ms); },
      waitUntil: async deadlineMs => {
        const remaining = deadlineMs - (performance.timeOrigin + performance.now() + clockOffsetMs);
        if (remaining > 0) await Bun.sleep(remaining);
      },
    };
    const system = createSystem(2, 1, 30_000, undefined, undefined, clock, 25);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const selected = system.runtimes[1]!;
    selected.ackEwmaMs = 600;
    selected.reflectExecutionRisk = false;
    selected.waitForPostAdmissionAbort = true;
    const totalEncodedBytes = Math.floor(2.9 * 1024 ** 2);
    const controller = new AbortController();
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "near-complete-live-shape", stream: true },
      {
        clientId: "near-complete-live-shape",
        source: "websocket",
        encodedBodyBytes: totalEncodedBytes,
        structuralBodyBytes: totalEncodedBytes,
        signal: controller.signal,
      },
    );
    await flush();
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);

    // Model the live trace without sleeping 15 seconds: 2.25 MiB was uniquely
    // acknowledged by 9.337s, then becomes stale. The new global budget is
    // long enough for one complete safe retransmit, so the stale route moves.
    clockOffsetMs = 9_337;
    selected.emitAdmissionProgress({
      stage: "request_data",
      acknowledgedEncodedBytes: Math.floor(2.25 * 1024 ** 2),
      totalEncodedBytes,
    });
    clockOffsetMs = 15_000;
    await Bun.sleep(60);

    expect(selected.lastRunSignal?.aborted).toBe(true);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    const handle = await pending;
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("retries a fully acknowledged 2.9 MiB body when its exact prepared proof stalls", async () => {
    let clockOffsetMs = 0;
    const clock: Clock = {
      now: () => performance.timeOrigin + performance.now() + clockOffsetMs,
      sleep: async ms => { await Bun.sleep(ms); },
      waitUntil: async deadlineMs => {
        const remaining = deadlineMs - (performance.timeOrigin + performance.now() + clockOffsetMs);
        if (remaining > 0) await Bun.sleep(remaining);
      },
    };
    const system = createSystem(2, 1, 30_000, undefined, undefined, clock, 25);
    for (const runtime of system.runtimes) {
      runtime.transportProfile = "bulk-v1";
      runtime.notify();
    }
    const stalled = system.runtimes[1]!;
    stalled.reflectExecutionRisk = false;
    stalled.waitForPostAdmissionAbort = true;
    const totalEncodedBytes = Math.floor(2.9 * 1024 ** 2);
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "body-acked-prepared-stalled", stream: true },
      {
        clientId: "body-acked-prepared-stalled",
        source: "websocket",
        encodedBodyBytes: totalEncodedBytes,
        structuralBodyBytes: totalEncodedBytes,
      },
    );
    await flush();
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);

    clockOffsetMs = 9_000;
    stalled.emitAdmissionProgress({
      stage: "request_data",
      acknowledgedEncodedBytes: totalEncodedBytes,
      totalEncodedBytes,
    });
    // Four seconds is still inside the general five-second body freshness
    // floor, but it exhausts the RTT-derived prepared-proof grace.
    clockOffsetMs = 13_001;
    const handle = await pending;

    expect(stalled.lastRunSignal?.aborted).toBe(true);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("keeps standard-profile large requests on the original retry behavior", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    const selected = system.runtimes[1]!;
    selected.reflectExecutionRisk = false;
    selected.waitForPostAdmissionAbort = true;

    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "large-standard", stream: true },
      {
        clientId: "large-standard",
        source: "websocket",
        encodedBodyBytes: Math.floor(2.9 * 1024 ** 2),
        structuralBodyBytes: Math.floor(2.9 * 1024 ** 2),
      },
    );
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    expect(selected.admissionProgressCallbacks).toHaveLength(0);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("reroutes a concurrent admitted-but-unstarted burst without reducing fleet concurrency", async () => {
    const system = createSystem(8, 8, 500, undefined, undefined, undefined, 25);
    for (const runtime of system.runtimes) {
      runtime.admissionWindow = 4;
      runtime.notify();
    }
    const stalled = system.runtimes[4]!;
    stalled.reflectExecutionRisk = false;
    stalled.waitForPostAdmissionAbort = true;
    const started = performance.now();

    const handles = await Promise.all(Array.from({ length: 24 }, (_, index) => (
      system.router.startRelay(
        { model: "gpt-5.6-sol", input: `burst-${index}`, stream: true },
        { clientId: `burst-${index}`, source: "websocket" },
      )
    )));

    expect(performance.now() - started).toBeLessThan(200);
    expect(stalled.executions).toBeGreaterThan(0);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0))
      .toBe(24 + stalled.executions);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.finals.length, 0)).toBe(24);
    for (const runtime of system.runtimes) {
      while (runtime.finals.length > 0) runtime.completeOne();
    }
    await Promise.all(handles.map(handle => handle.final));
    expect(system.router.snapshot()).toMatchObject({ queued: 0 });
    await system.router.close();
  });

  it("waits for explicit pre-commit cleanup proof before failover", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    system.runtimes[1]!.ignoreAdmissionAbortMs = 120;
    const started = performance.now();

    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "detach-safe-cleanup", stream: true },
      { clientId: "detach-safe-cleanup", source: "websocket" },
    );
    await Bun.sleep(60);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    const handle = await pending;
    expect(performance.now() - started).toBeGreaterThanOrEqual(110);
    expect(performance.now() - started).toBeLessThan(190);
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1]);
    system.runtimes[0]!.completeOne();
    await handle.final;
    await Bun.sleep(130);
    await system.router.close();
  });

  it("enforces the absolute deadline when every runtime ignores cancellation", async () => {
    const system = createSystem(1, 1, 60, undefined, undefined, undefined, 20);
    system.runtimes[0]!.ignoreAdmissionAbortMs = 200;
    const started = performance.now();

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "hard-deadline", stream: true },
      { clientId: "hard-deadline", source: "websocket" },
    )).rejects.toMatchObject({
      code: "route_timeout_cancellation_unproven",
      committed: true,
      ambiguous: true,
    });

    expect(performance.now() - started).toBeLessThan(110);
    await Bun.sleep(220);
    await system.router.close();
  });

  it("never fails over an attempt timeout once the runtime reports execution risk", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    system.runtimes[1]!.waitForAdmissionAbort = true;
    system.runtimes[1]!.admissionAbortError = new V2CommittedError(
      "request start may have crossed the durable boundary",
      "start_risk",
      true,
    );

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "never-duplicate", stream: true },
      { clientId: "never-duplicate", source: "websocket" },
    )).rejects.toMatchObject({ committed: true, ambiguous: true, code: "start_risk" });

    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    await system.router.close();
  });

  it("bounds a runtime that reports execution risk but never returns its handle", async () => {
    const system = createSystem(1, 1, 60, undefined, undefined, undefined, 20);
    const runtime = system.runtimes[0]!;
    runtime.postAdmissionDelayMs = 200;
    const started = performance.now();

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "risk-without-handle", stream: true },
      { clientId: "risk-without-handle", source: "websocket" },
    )).rejects.toMatchObject({
      code: "execution_risk_handle_unproven",
      committed: true,
      ambiguous: true,
    });
    expect(performance.now() - started).toBeLessThan(120);
    expect(runtime.executions).toBe(1);
    await Bun.sleep(210);
    runtime.completeOne();
    await system.router.close();
  });

  it("keeps same-turn late handles cleanup-only after execution-risk proof expires", async () => {
    const system = createSystem(1, 1, 40, undefined, undefined, undefined, 20);
    const runtime = system.runtimes[0]!;
    runtime.returnPendingHandleOnAbortAfterExecutionRisk = true;

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "same-turn-late-handle", stream: true },
      { clientId: "same-turn-late-handle", source: "websocket" },
    )).rejects.toMatchObject({
      code: "execution_risk_handle_unproven",
      committed: true,
      ambiguous: true,
    });
    await flush();
    expect(runtime.executions).toBe(1);
    expect(runtime.lateHandleDiscardCalls).toBe(1);
    expect(runtime.active).toBe(1);
    expect(runtime.finals).toHaveLength(1);
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);
    await expect(system.router.awaitIdle(10)).rejects.toMatchObject({
      code: "router_shutting_down",
    });

    runtime.completeOne();
    await expect(system.router.awaitIdle(100)).resolves.toBeUndefined();
    expect(runtime.lateHandleDiscardCalls).toBe(1);
    expect(runtime.active).toBe(0);
    expect(runtime.finals).toHaveLength(0);
    expect(system.router.snapshot()).toMatchObject({ active: 0, queued: 0 });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("never retries a precommit-shaped error after execution risk was reflected", async () => {
    const system = createSystem(2, 1, 200);
    system.runtimes[1]!.failAfterExecutionRiskError = new V2PreCommitError(
      "misclassified failure after request_start",
      "misclassified_after_start",
    );

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "latched-risk", stream: true },
      { clientId: "latched-risk", source: "websocket" },
    )).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "execution_risk_reflected",
    });
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    await system.router.close();
  });

  it("preserves delayed execution-risk proof inside the cancellation fence", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    system.runtimes[1]!.waitForAdmissionAbort = true;
    system.runtimes[1]!.admissionAbortDelayMs = 10;
    system.runtimes[1]!.admissionAbortError = new V2CommittedError(
      "late durable start proof",
      "late_start_risk",
      true,
    );

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "delayed-risk", stream: true },
      { clientId: "delayed-risk", source: "websocket" },
    )).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "late_start_risk",
    });
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([0, 1]);
    await system.router.close();
  });

  it("ends the route-attempt deadline once the runtime reports execution risk", async () => {
    const system = createSystem(1, 1, 10);
    system.runtimes[0].postAdmissionDelayMs = 25;
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "admitted-before-deadline", stream: true },
      { clientId: "admitted-before-deadline", source: "websocket" },
    );
    system.runtimes[0].completeOne();
    await handle.final;
    await system.router.close();
  });

  it("forwards sanitized caller cancellation after commit until the terminal settles", async () => {
    const system = createSystem(1, 1);
    const controller = new AbortController();
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "cancel-after-commit", stream: true },
      { signal: controller.signal, source: "http" },
    );
    expect(system.runtimes[0].lastRunSignal?.aborted).toBe(false);
    const reason = new Error("client disconnected");
    controller.abort(reason);
    expect(system.runtimes[0].lastRunSignal?.aborted).toBe(true);
    expect(system.runtimes[0].lastRunSignal?.reason).toMatchObject({ name: "AbortError" });
    expect(system.runtimes[0].lastRunSignal?.reason).not.toBe(reason);
    system.runtimes[0].completeOne();
    await handle.final;
    await system.router.close();
  });

  it("allows a safe retry on a fresh incarnation of the same Prism project", async () => {
    const system = createSystem(1, 1);
    const runtime = system.runtimes[0];
    runtime.failStarts = 1;
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "same-project-recovery", stream: true },
      { clientId: "same-project-recovery", source: "websocket" },
    );
    await flush();
    expect(runtime.executions).toBe(1);

    runtime.replaceIncarnation();
    const handle = await pending;
    expect(runtime.executions).toBe(2);
    runtime.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("never dispatches a retry after its absolute queue deadline", async () => {
    const system = createSystem(2, 1, 100);
    system.runtimes[1].failStarts = 1;
    system.runtimes[1].failDelayMs = 150;
    const pending = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "expired-retry", stream: true },
      { clientId: "expired-retry", source: "websocket" },
    );
    const outcome = await Promise.race([
      pending.then(
        handle => ({ kind: "handle" as const, handle }),
        error => ({ kind: "error" as const, error }),
      ),
      Bun.sleep(500).then(() => ({ kind: "idle-timeout" as const })),
    ]);
    if (outcome.kind === "handle") {
      system.runtimes[0].completeOne();
      await outcome.handle.final;
    }
    await system.router.close();

    expect(outcome).toMatchObject({
      kind: "error",
      error: {
        code: "capacity_timeout_cancellation_unproven",
        committed: true,
        ambiguous: true,
      },
    });
    expect(system.runtimes[1].executions).toBe(1);
    expect(system.runtimes[0].executions).toBe(0);
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(0);
    await Bun.sleep(30);
    await system.router.awaitIdle(100);
    expect(system.governor.snapshot().reservedBytes).toBe(0);
  });

  it("adds, drains, and removes runtimes without replacing the registry", async () => {
    const registry = new SandboxRegistry();
    const runtime = new FakeRuntime(projectId("00000000-0000-4000-8000-000000000001"));
    const events: string[] = [];
    const unsubscribe = registry.subscribe(event => events.push(event.type));

    registry.add(runtime);
    expect(registry.get(runtime.projectId)).toBe(runtime);
    await registry.drain(runtime.projectId, "scale-down");
    await registry.removeStopped(runtime.projectId);

    expect(registry.get(runtime.projectId)).toBeUndefined();
    expect(events).toEqual(["added", "changed", "removed"]);
    unsubscribe();
  });

  it("wakes queued routing when a ready runtime is added", async () => {
    const registry = new SandboxRegistry();
    const governor = createSystem(0).governor;
    const router = new MultiSandboxRouter({
      registry,
      governor,
      random: { next: () => 0.5, uuid: () => "id" },
    });
    const pending = router.startRelay(
      { model: "gpt-5.6-sol", input: "queued", stream: true },
      { clientId: "queued", source: "http" },
    );
    await flush();
    const runtime = new FakeRuntime(projectId("00000000-0000-4000-8000-000000000001"), 1);
    registry.add(runtime);

    const handle = await pending;
    runtime.completeOne();
    await handle.final;
    await router.close();
  });

  it("admits exactly 120 simultaneous jobs across four 30-slot runtimes", async () => {
    const system = createSystem();
    const jobs = Array.from({ length: 121 }, (_, index) => system.router.startRelay(
      { model: "gpt-5.6-sol", input: `job-${index}`, stream: true },
      { clientId: "load", source: "http" },
    ));
    await flush(200);
    expect(system.router.snapshot()).toMatchObject({ active: 120, queued: 1, capacity: 120 });
    expect(system.runtimes.map(runtime => runtime.active)).toEqual([30, 30, 30, 30]);
    expect(system.runtimes.flatMap(runtime => runtime.bodies))
      .toHaveLength(120);
    expect(system.runtimes.flatMap(runtime => runtime.bodies)
      .every(body => body.service_tier === "fast")).toBe(true);

    const firstHandle = await jobs[0];
    system.runtimes.forEach(runtime => runtime.completeOne());
    await firstHandle.final;
    await flush();
    expect(await jobs[120]).toBeDefined();
  });

  it("spreads multi-megabyte transfer ownership across the fleet", async () => {
    const system = createSystem(4, 4, 1_000);
    const body = {
      model: "gpt-5.6-sol",
      input: "x".repeat(2 * 1024 * 1024),
      stream: true,
    };

    const handles = await Promise.all(Array.from(
      { length: 4 },
      (_, index) => system.router.startRelay(body, {
        clientId: `large-${index}`,
        source: "websocket",
      }),
    ));

    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1, 1, 1]);
    expect(system.runtimes.map(runtime => runtime.uncommittedPeak)).toEqual([1, 1, 1, 1]);
    system.runtimes.forEach(runtime => runtime.completeOne());
    await Promise.all(handles.map(handle => handle.final));
    await system.router.close();
  });

  it("does not collapse an eight-runtime fleet during a shared direct-heartbeat lapse", async () => {
    const system = createSystem(8, 8);
    const sidecarAges = [5_874, 1_551, 5_319, 6_494, 7_481, 5_760, 1_627, 4_232];
    system.runtimes.forEach((runtime, index) => {
      runtime.heartbeatAgeMs = 24_000 + index * 1_000;
      runtime.sidecarPeerAgeMs = sidecarAges[index]!;
      runtime.transportConnected = true;
      runtime.notify();
    });

    expect(system.router.snapshot()).toMatchObject({
      capacity: 64,
      available: 64,
    });

    const handles = await Promise.all(Array.from({ length: 14 }, (_, index) =>
      system.router.startRelay({
        model: "gpt-5.6-sol",
        input: `correlated-heartbeat-outage-${index}`,
        stream: true,
      }, {
        clientId: `outage-${index}`,
        source: index % 2 === 0 ? "websocket" : "http",
      })
    ));
    expect(system.runtimes.reduce((total, runtime) => total + runtime.executions, 0)).toBe(14);
    expect(Math.max(...system.runtimes.map(runtime => runtime.active))).toBeLessThanOrEqual(2);
    for (const runtime of system.runtimes) {
      while (runtime.finals.length > 0) runtime.completeOne();
    }
    await Promise.all(handles.map(handle => handle.final));
    await system.router.close();
  });

  it("reroutes one pre-publication startup failure", async () => {
    const system = createSystem(2, 1);
    system.runtimes[1].failStarts = 1;
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "retry", stream: true },
      { clientId: "one", source: "http" },
    );
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(2);
    system.runtimes.find(runtime => runtime.finals.length)?.completeOne();
    await expect(handle.final).resolves.toBeDefined();
  });

  it("never reroutes a commit-published ambiguous startup error", async () => {
    const system = createSystem(2, 1);
    const selected = system.runtimes[1];
    const ambiguous = Object.assign(new Error("commit ACK was lost"), {
      committed: true,
      ambiguous: true,
      code: "sidecar_unreachable",
    });
    selected.run = async () => {
      selected.executions++;
      throw ambiguous;
    };

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "ambiguous", stream: true },
      { clientId: "ambiguous", source: "websocket" },
    )).rejects.toBe(ambiguous);

    expect(selected.executions).toBe(1);
    expect(system.runtimes[0].executions).toBe(0);
    await system.router.close();
  });

  it("forces every non-image tier to fast and strips every image tier", async () => {
    const system = createSystem(1, 1);
    const implicit = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "implicit",
    });
    expect(system.runtimes[0].bodies[0]).toMatchObject({ service_tier: "fast" });
    system.runtimes[0].completeOne();
    await implicit.final;

    for (const [index, service_tier] of [
      "auto",
      "default",
      "flex",
      "priority",
      "ultrafast",
    ].entries()) {
      const explicit = await system.router.startRelay({
        model: "gpt-5.6-sol",
        input: `explicit-${service_tier}`,
        service_tier,
      });
      expect(system.runtimes[0].bodies[index + 1]).toMatchObject({ service_tier: "fast" });
      system.runtimes[0].completeOne();
      await explicit.final;
    }

    const implicitImage = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "implicit image tier",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "medium" }],
    });
    expect(system.runtimes[0].bodies[6]).not.toHaveProperty("service_tier");
    system.runtimes[0].completeOne();
    await implicitImage.final;

    const explicitImage = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "explicit image tier",
      service_tier: "priority",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "medium" }],
    });
    expect(system.runtimes[0].bodies[7]).not.toHaveProperty("service_tier");
    system.runtimes[0].completeOne();
    await explicitImage.final;
  });

  it("never reroutes a rejection after a handle was published", async () => {
    const system = createSystem(2, 1);
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "published", stream: true },
      { clientId: "one", source: "http" },
    );
    const executions = system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0);
    system.runtimes.find(runtime => runtime.finals.length)?.rejectOne(new Error("terminal failure"));
    await expect(handle.final).rejects.toThrow("terminal failure");
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(executions);
  });

  it("does not route new work to a draining runtime", async () => {
    const system = createSystem(2, 1);
    system.runtimes[0].state = "draining";
    system.runtimes[0].notify();
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "healthy", stream: true },
      { clientId: "one", source: "http" },
    );
    expect(system.runtimes[0].executions).toBe(0);
    expect(system.runtimes[1].executions).toBe(1);
    system.runtimes[1].completeOne();
    await handle.final;
  });

  it("keeps physical capacity, true active work, and capped admission credits distinct", async () => {
    const system = createSystem(1, 90);
    system.runtimes[0].admissionWindow = 4;
    system.runtimes[0].notify();

    expect(system.router.snapshot()).toMatchObject({
      capacity: 90,
      active: 0,
      available: 4,
      queued: 0,
    });
    await system.router.close();
  });

  it("hands each router reservation to runtime admission exactly once", async () => {
    const system = createSystem(1, 90);
    const runtime = system.runtimes[0];
    runtime.admissionWindow = 4;
    runtime.notify();
    const handles = await Promise.all(Array.from({ length: 40 }, (_, index) =>
      system.router.startRelay(
        { model: "gpt-5.6-sol", input: `handoff-${index}`, stream: true },
        { clientId: `handoff-${index}`, source: "websocket" },
      )));

    expect(runtime.active).toBe(40);
    expect(runtime.uncommittedPeak).toBe(4);
    expect(system.router.snapshot()).toMatchObject({
      capacity: 90,
      active: 40,
      available: 4,
      queued: 0,
    });
    while (runtime.finals.length) runtime.completeOne();
    await Promise.all(handles.map(handle => handle.final));
    await system.router.close();
  });

  it("downgrades Terra request memory after commitment without retaining a fallback body", async () => {
    const system = createSystem(1, 1);
    const handle = await system.router.startRelay({
      model: "gpt-5.6-terra",
      input: "committed-memory",
      stream: true,
    });
    expect(system.governor.snapshot().reservedBytes).toBe(4 * 1024 * 1024);
    system.runtimes[0]!.completeOne({ id: "resp-memory", status: "completed" });
    await handle.final;
    await flush();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("keeps a committed run alive when the optional lease downgrade fails", async () => {
    const system = createSystem(1, 1);
    system.runtimes[0]!.postAdmissionDelayMs = 20;
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "committed-memory-fallback",
      stream: true,
    });
    while (system.runtimes[0]!.uncommitted === 0) await Bun.sleep(1);
    system.governor.close();

    const handle = await pending;
    expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(4 * 1024 * 1024);
    system.runtimes[0]!.completeOne({ id: "resp-memory-fallback", status: "completed" });
    await handle.final;
    await flush();
    expect(system.governor.snapshot().reservedBytes).toBe(0);
    await system.router.close();
  });

  it("ends admission timing before an optional slow committed-lease downgrade", async () => {
    const system = createSystem(1, 1, 10);
    let replacementReleased = false;
    const replacement = {
      reservedBytes: 4 * 1024 * 1024,
      replace: async () => { throw new Error("replacement is already minimal"); },
      release: () => { replacementReleased = true; },
    };
    const committedLease = {
      reservedBytes: 16 * 1024 * 1024,
      replace: async () => {
        await Bun.sleep(40);
        return replacement;
      },
      release: () => undefined,
    };
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "slow-accounting",
      stream: true,
    }, {
      incomingLease: {
        acceptedAtMs: Date.now(),
        deadlineMs: Date.now() + 10,
        replace: async () => committedLease,
        release: () => undefined,
      },
    });

    const handle = await pending;
    expect(system.runtimes[0]!.lastRunSignal?.aborted).toBe(false);
    system.runtimes[0]!.completeOne({ id: "resp-slow-accounting", status: "completed" });
    await handle.final;
    await flush();
    expect(replacementReleased).toBe(true);
    await system.router.close();
  });

  it("tries every distinct runtime after explicit safe pre-commit failures", async () => {
    const system = createSystem(3, 1);
    system.runtimes[1].failStarts = 1;
    system.runtimes[2].failStarts = 1;

    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "three-way-failover", stream: true },
      { clientId: "failover", source: "websocket" },
    );
    expect(system.runtimes.map(runtime => runtime.executions)).toEqual([1, 1, 1]);
    system.runtimes[0].completeOne();
    await handle.final;
    await system.router.close();
  });

  it("never treats an untyped runtime rejection as safe to duplicate", async () => {
    const system = createSystem(2, 1);
    system.runtimes[1].failStarts = 1;
    system.runtimes[1].failError = new Error("untyped lifecycle failure");

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "not-proven-safe", stream: true },
      { clientId: "not-proven-safe", source: "websocket" },
    )).rejects.toThrow("untyped lifecycle failure");
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    await system.router.close();
  });

  it("never accepts a duck-typed pre-commit claim as replay proof", async () => {
    const system = createSystem(2, 1);
    system.runtimes[1].failStarts = 1;
    const forged = Object.assign(new Error("forged pre-commit claim"), {
      name: "V2PreCommitError",
      committed: false,
      code: "forged_precommit",
    });
    system.runtimes[1].failError = forged;

    await expect(system.router.startRelay(
      { model: "gpt-5.6-sol", input: "do not replay forged proof", stream: true },
      { clientId: "forged-proof", source: "websocket" },
    )).rejects.toBe(forged);
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.executions, 0)).toBe(1);
    await system.router.close();
  });

  it("skips an unroutable retry head and dispatches another client", async () => {
    const system = createSystem(2, 1);
    const [full, retrying] = system.runtimes;
    full.active = 1;
    full.notify();
    retrying.failStarts = 1;
    const controller = new AbortController();
    const blocked = system.router.startRelay(
      { model: "gpt-5.6-sol", input: "blocked-retry", stream: true },
      { clientId: "blocked", source: "websocket", signal: controller.signal },
    );
    const blockedOutcome = blocked.then(
      () => "resolved",
      error => error,
    );
    await flush();

    const healthy = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "can-use-first-runtime", stream: true },
      { clientId: "healthy", source: "websocket" },
    );
    expect(retrying.executions).toBe(2);
    controller.abort();
    expect(await blockedOutcome).toBeInstanceOf(DOMException);
    retrying.completeOne();
    await healthy.final;
    await system.router.close();
  });

  for (const scenario of [
    { name: "global fleet", projects: 2, slots: 1, transferCredits: 6 },
    { name: "exact runtime", projects: 1, slots: 2, transferCredits: 4 },
  ] as const) {
    it(`protects bounded weighted headroom from ${scenario.name} starvation`, async () => {
      let now = performance.timeOrigin + performance.now();
      const clock: Clock = {
        now: () => now,
        sleep: async ms => { now += ms; },
        waitUntil: async deadline => { now = Math.max(now, deadline); },
      };
      const system = createSystem(
        scenario.projects,
        scenario.slots,
        20_000,
        undefined,
        undefined,
        clock,
      );
      for (const runtime of system.runtimes) {
        runtime.transportProfile = "bulk-v1";
        runtime.holdBeforeExecutionRisk = true;
        runtime.reflectTransferCosts = true;
        runtime.transferCredits = scenario.transferCredits;
        runtime.notify();
      }
      const controllers: AbortController[] = [];
      const outcomes: Promise<unknown>[] = [];
      let sequence = 0;
      const launch = (kind: "tiny" | "medium" | "large") => {
        const controller = new AbortController();
        controllers.push(controller);
        const bytes = kind === "tiny"
          ? 128 * 1024
          : kind === "medium"
            ? Math.floor(2.5 * 1024 ** 2)
            : Math.floor(3.2 * 1024 ** 2);
        const transferCost = kind === "tiny" ? 1 : kind === "medium" ? 2 : 4;
        const pending = system.router.startRelay({
          model: "gpt-5.6-sol",
          input: `${kind}-${sequence++}`,
          tag: kind,
          test_transfer_cost: transferCost,
          stream: true,
        }, {
          clientId: `${kind}-${sequence}`,
          signal: controller.signal,
          encodedBodyBytes: bytes,
          structuralBodyBytes: bytes,
        });
        outcomes.push(pending.then(handle => handle.final, () => undefined));
      };

      launch("medium");
      launch("medium");
      for (let turn = 0;
        turn < 100 && system.runtimes.reduce((sum, runtime) => (
          sum + runtime.preExecutionHolds.length
        ), 0) < 2;
        turn++) await Bun.sleep(1);
      launch("large");
      for (let index = 0; index < 24; index++) launch("medium");

      const largeExecutions = () => system.runtimes.reduce((sum, runtime) => (
        sum + runtime.bodies.filter(body => body.tag === "large").length
      ), 0);
      for (let cycle = 0; cycle < 24 && largeExecutions() === 0; cycle++) {
        now += 500;
        const runtime = system.runtimes.find(candidate => (
          candidate.preExecutionHolds.some(hold => hold.body.tag === "medium")
        ));
        expect(runtime).toBeDefined();
        const index = runtime!.preExecutionHolds.findIndex(hold => hold.body.tag === "medium");
        runtime!.preExecutionHolds.splice(index, 1)[0]!.release();
        for (let turn = 0; turn < 100 && runtime!.finals.length === 0; turn++) {
          await Promise.resolve();
        }
        runtime!.completeOne();
        await flush(20);
        if (cycle === 7 && largeExecutions() === 0) {
          launch("tiny");
          for (let turn = 0;
            turn < 100 && !system.runtimes.some(candidate => (
              candidate.preExecutionHolds.some(hold => hold.body.tag === "tiny")
            ));
            turn++) await Promise.resolve();
          const tinyRuntime = system.runtimes.find(candidate => (
            candidate.preExecutionHolds.some(hold => hold.body.tag === "tiny")
          ));
          expect(tinyRuntime).toBeDefined();
          const tinyIndex = tinyRuntime!.preExecutionHolds.findIndex(
            hold => hold.body.tag === "tiny",
          );
          tinyRuntime!.preExecutionHolds.splice(tinyIndex, 1)[0]!.release();
          for (let turn = 0; turn < 100 && tinyRuntime!.finals.length === 0; turn++) {
            await Promise.resolve();
          }
          tinyRuntime!.completeOne();
          await flush(20);
        }
      }

      expect(largeExecutions()).toBe(1);

      for (const controller of controllers) controller.abort();
      for (let round = 0; round < 20; round++) {
        let progressed = false;
        for (const runtime of system.runtimes) {
          while (runtime.preExecutionHolds.length > 0) {
            runtime.preExecutionHolds.shift()!.release();
            progressed = true;
          }
        }
        await flush(20);
        for (const runtime of system.runtimes) {
          while (runtime.finals.length > 0) {
            runtime.completeOne();
            progressed = true;
          }
        }
        await flush(20);
        if (!progressed && system.router.snapshot().active === 0) break;
      }
      await Promise.allSettled(outcomes);
      await system.router.close();
    });
  }

  it("keeps latency work fast without starving a backlogged bulk class", async () => {
    const system = createSystem(1, 1);
    const runtime = system.runtimes[0];
    const blocker = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "blocker",
      stream: true,
    }, { clientId: "blocker", source: "websocket" });
    const pending = [
      ...Array.from({ length: 20 }, (_, index) => system.router.startRelay({
        model: "gpt-5.6-sol",
        input: `small-${index}`,
        tag: `small-${index}`,
        stream: true,
      }, { clientId: "small-client", source: "websocket" })),
      ...Array.from({ length: 2 }, (_, index) => system.router.startRelay({
        model: "gpt-5.6-sol",
        input: "x".repeat(600 * 1024),
        tag: `bulk-${index}`,
        stream: true,
      }, {
        clientId: "bulk-client",
        source: "websocket",
        structuralBodyBytes: 600 * 1024 + 256,
      })),
    ];
    for (let turn = 0; turn < 100 && system.router.snapshot().queued < 22; turn++) {
      await Bun.sleep(1);
    }
    expect(system.router.snapshot().queued).toBe(22);

    runtime.completeOne();
    await blocker.final;
    const order: string[] = [];
    for (let index = 0; index < pending.length; index++) {
      for (let turn = 0; runtime.bodies.length < index + 2 && turn < 100; turn++) {
        await Promise.resolve();
      }
      order.push(String(runtime.bodies[index + 1]?.tag));
      runtime.completeOne();
    }
    const handles = await Promise.all(pending);
    await Promise.all(handles.map(handle => handle.final));

    const bulkPositions = order
      .map((tag, index) => tag.startsWith("bulk-") ? index : -1)
      .filter(index => index >= 0);
    expect(bulkPositions).toHaveLength(2);
    expect(bulkPositions[0]).toBeLessThanOrEqual(4);
    expect(bulkPositions[1]).toBeLessThanOrEqual(9);
    expect(order[0]?.startsWith("small-")).toBe(true);
    await system.router.close();
  });

  it("rotates class turns while preserving byte credit across one-request lane refill", async () => {
    const system = createSystem(1, 1, 5_000);
    const runtime = system.runtimes[0]!;
    const blocker = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "blocker",
      stream: true,
    }, { clientId: "class-blocker", source: "websocket" });
    const controllers: AbortController[] = [];
    const bulkInput = "x".repeat(1024 * 1024);
    let sequence = 0;
    const launch = (queueClass: "latency" | "bulk") => {
      const controller = new AbortController();
      controllers.push(controller);
      return system.router.startRelay({
        model: "gpt-5.6-sol",
        input: queueClass === "latency" ? "x" : bulkInput,
        tag: `${queueClass}-${sequence++}`,
        stream: true,
      }, {
        clientId: `${queueClass}-class-client`,
        source: "websocket",
        signal: controller.signal,
        ...(queueClass === "bulk"
          ? { structuralBodyBytes: 1024 * 1024 + 256 }
          : {}),
      }).then(handle => ({ queueClass, handle }));
    };
    const waiting: Record<"latency" | "bulk", ReturnType<typeof launch>> = {
      latency: launch("latency"),
      bulk: launch("bulk"),
    };
    await Bun.sleep(20);

    runtime.completeOne();
    await blocker.final;
    const order: Array<"latency" | "bulk"> = [];
    for (let turn = 0; turn < 40; turn++) {
      const selected = await Promise.race([waiting.latency, waiting.bulk]);
      order.push(selected.queueClass);
      waiting[selected.queueClass] = launch(selected.queueClass);
      await Bun.sleep(0);
      runtime.completeOne();
      await selected.handle.final;
    }

    const bulkPositions = order
      .map((queueClass, index) => queueClass === "bulk" ? index : -1)
      .filter(index => index >= 0);
    expect(order[0]).toBe("latency");
    expect(bulkPositions.length).toBeGreaterThanOrEqual(6);
    expect(bulkPositions[0]).toBeLessThanOrEqual(4);
    const bulkGaps = bulkPositions.slice(1).map((position, index) => (
      position - bulkPositions[index]!
    ));
    expect(Math.max(...bulkGaps)).toBeLessThanOrEqual(6);

    for (const controller of controllers) controller.abort();
    await flush();
    while (runtime.finals.length > 0) runtime.completeOne();
    await Promise.allSettled([waiting.latency, waiting.bulk]);
    await system.router.close();
  });

  it("refreshes only the changed runtime and ignores no-credit Awareness churn", async () => {
    const system = createSystem(12, 1, 1_000);
    for (const runtime of system.runtimes) {
      runtime.active = 1;
      runtime.notify();
    }
    const pending = Array.from({ length: 96 }, (_, index) => system.router.startRelay({
      model: "gpt-5.6-sol",
      input: `queued-${index}`,
      stream: true,
    }, { clientId: `client-${index}`, source: "websocket" }).catch(error => error));
    await flush();
    expect(system.router.snapshot().queued).toBe(96);
    for (const runtime of system.runtimes) runtime.snapshotCalls = 0;

    for (let update = 0; update < 48; update++) {
      system.runtimes[update % system.runtimes.length]!.notify();
      await flush(2);
    }
    expect(system.runtimes.reduce((sum, runtime) => sum + runtime.snapshotCalls, 0)).toBe(48);

    await system.router.close();
    await Promise.all(pending);
  });

  it("does not mistake cold-runtime control traffic for payload throughput", async () => {
    let now = performance.timeOrigin + performance.now();
    const clock: Clock = {
      now: () => now,
      sleep: () => Promise.resolve(),
      waitUntil: () => Promise.resolve(),
    };
    const system = createSystem(2, 8, undefined, undefined, undefined, clock);
    const cold = system.runtimes[0]!;
    const warm = system.runtimes[1]!;
    cold.ackEwmaMs = 100;
    warm.ackEwmaMs = 300;

    cold.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: null },
    };
    warm.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: 1024 ** 2 },
    };
    cold.notify();
    warm.notify();

    now += 1_000;
    cold.diagnostics = {
      // An idle runtime still publishes control frames. They are not evidence
      // that request payloads move at only 1 KiB/s.
      host: { acknowledgedGoodputBytesPerSecond: null },
    };
    warm.diagnostics = {
      host: {
        acknowledgedGoodputBytesPerSecond: 1024 ** 2,
      },
    };
    cold.notify();
    warm.notify();

    const handle = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "x".repeat(256 * 1024),
      stream: true,
    });
    expect(cold.executions).toBe(1);
    expect(warm.executions).toBe(0);
    cold.completeOne();
    await handle.final;
    await system.router.close();
  });

  it("forgets a stale slow payload sample so an idle runtime can recover", async () => {
    let now = performance.timeOrigin + performance.now();
    const clock: Clock = {
      now: () => now,
      sleep: () => Promise.resolve(),
      waitUntil: () => Promise.resolve(),
    };
    const system = createSystem(2, 8, undefined, undefined, undefined, clock);
    const recovered = system.runtimes[0]!;
    const warm = system.runtimes[1]!;
    recovered.ackEwmaMs = 0;
    warm.ackEwmaMs = 300;
    recovered.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: null },
    };
    warm.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: null },
    };
    recovered.notify();
    warm.notify();

    now += 1_000;
    recovered.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: 1_024 },
    };
    warm.diagnostics = {
      host: { acknowledgedGoodputBytesPerSecond: 1024 ** 2 },
    };
    recovered.notify();
    warm.notify();

    const beforeReset = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "x".repeat(256 * 1024),
      stream: true,
    });
    expect(warm.executions).toBe(1);
    expect(recovered.executions).toBe(0);
    warm.completeOne();
    await beforeReset.final;

    now += 60_001;
    recovered.diagnostics = {
      // The transport expires an idle goodput estimate rather than deriving a
      // synthetic rate from cumulative publication counters.
      host: { acknowledgedGoodputBytesPerSecond: null },
    };
    warm.diagnostics = {
      host: {
        acknowledgedGoodputBytesPerSecond: 1024 ** 2,
      },
    };
    recovered.notify();
    warm.notify();
    // A cold runtime is explored conservatively once the measured peer has
    // real work, rather than being permanently poisoned by a stale sample.
    warm.active = 1;
    warm.notify();

    const afterReset = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "x".repeat(256 * 1024),
      stream: true,
    });
    expect(recovered.executions).toBe(1);
    recovered.completeOne();
    await afterReset.final;
    await system.router.close();
  });

  it("ages cached heartbeat and credential health without registry events", async () => {
    let now = 1_000;
    const clock: Clock = {
      now: () => now,
      sleep: () => Promise.resolve(),
      waitUntil: () => Promise.resolve(),
    };
    const system = createSystem(1, 1, undefined, undefined, undefined, clock);
    expect(system.router.snapshot()).toMatchObject({ capacity: 1, available: 1 });

    now += 100;
    expect(system.router.snapshot().runtimes[0]).toMatchObject({
      heartbeatAgeMs: 1_100,
      credentialExpiresInMs: 3_599_900,
    });
    now += 18_901; // cached heartbeatAgeMs crosses the 20-second health fence
    expect(system.router.snapshot()).toMatchObject({ capacity: 0, available: 0 });
    await system.router.close();
  });

  it("ages cached sidecar liveness instead of keeping a dead peer healthy forever", async () => {
    let now = 1_000;
    const clock: Clock = {
      now: () => now,
      sleep: () => Promise.resolve(),
      waitUntil: () => Promise.resolve(),
    };
    const system = createSystem(1, 1, undefined, undefined, undefined, clock);
    const runtime = system.runtimes[0]!;
    runtime.heartbeatAgeMs = 30_000;
    runtime.sidecarPeerAgeMs = 19_000;
    runtime.notify();
    expect(system.router.snapshot()).toMatchObject({ capacity: 1, available: 1 });

    now += 1_001;
    expect(system.router.snapshot()).toMatchObject({ capacity: 0, available: 0 });
    expect(system.router.snapshot().runtimes[0]).toMatchObject({
      heartbeatAgeMs: 31_001,
      sidecarPeerAgeMs: 20_001,
    });
    const pending = system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "both-liveness-signals-aged-out",
      stream: true,
    });
    await flush();
    expect(runtime.executions).toBe(0);
    expect(system.router.snapshot().queued).toBe(1);
    await system.router.close();
    await expect(pending).rejects.toMatchObject({ code: "router_shutting_down" });
  });

  it("retains byte debt when unequal bulk clients each keep only one job queued", async () => {
    const system = createSystem(1, 1);
    const runtime = system.runtimes[0]!;
    const blocker = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "blocker",
      stream: true,
    }, { clientId: "blocker", source: "websocket" });
    const controllers: AbortController[] = [];
    let sequence = 0;
    const launch = (client: "smaller" | "larger", bytes: number) => {
      const controller = new AbortController();
      controllers.push(controller);
      return system.router.startRelay({
        model: "gpt-5.6-sol",
        input: "x".repeat(bytes),
        tag: `${client}-${sequence++}`,
        stream: true,
      }, { clientId: client, source: "websocket", signal: controller.signal })
        .then(handle => ({ client, bytes, handle }));
    };
    const waiting: Record<"smaller" | "larger", ReturnType<typeof launch>> = {
      smaller: launch("smaller", 520 * 1024),
      larger: launch("larger", 1024 * 1024),
    };

    runtime.completeOne();
    await blocker.final;
    const order: Array<"smaller" | "larger"> = [];
    for (let turn = 0; turn < 20; turn++) {
      const selected = await Promise.race([waiting.smaller, waiting.larger]);
      order.push(selected.client);
      // Refill this client before its active request completes: every client
      // owns at most one queued request, so fairness depends on retained debt.
      waiting[selected.client] = launch(selected.client, selected.bytes);
      await flush(2);
      runtime.completeOne();
      await selected.handle.final;
    }

    const smaller = order.filter(client => client === "smaller").length;
    const larger = order.length - smaller;
    expect(smaller).toBeGreaterThan(larger);
    expect(larger).toBeGreaterThanOrEqual(5);

    for (const controller of controllers) controller.abort();
    await flush();
    while (runtime.finals.length > 0) runtime.completeOne();
    await Promise.allSettled([waiting.smaller, waiting.larger]);
    await system.router.close();
  });
});
