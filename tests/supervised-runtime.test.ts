import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import type { RunHandle } from "../src/relay/run-handle";
import { parseProvisionedSandbox, type ProvisionedSandbox } from "../src/prism/protocol";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
  SandboxRunOptions,
} from "../src/sandbox/registry";
import {
  RecoveryPermitPool,
  SupervisedSandboxRuntime,
  sandboxRecoveryBackoffMs,
  type SandboxSupervisorEvent,
} from "../src/sandbox/supervised-runtime";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { V2PreCommitError } from "../src/v2/transport/run-handle";

const PROJECT = projectId("00000000-0000-4000-8000-000000000042");

interface ClockWaiter {
  deadline: number;
  signal?: AbortSignal;
  resolve(): void;
  reject(error: unknown): void;
  onAbort?: () => void;
}

class ManualClock implements Clock {
  value = 1_000;
  readonly waiters: ClockWaiter[] = [];

  now(): number { return this.value; }

  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return this.waitUntil(this.value + ms, signal);
  }

  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const waiter: ClockWaiter = { deadline, signal, resolve, reject };
      if (signal) {
        waiter.onAbort = () => {
          this.#remove(waiter);
          reject(new DOMException("aborted", "AbortError"));
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
    });
  }

  advanceBy(ms: number): void {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.deadline > this.value) continue;
      this.#remove(waiter);
      waiter.resolve();
    }
  }

  pendingTimers(): number { return this.waiters.length; }

  #remove(waiter: ClockWaiter): void {
    const index = this.waiters.indexOf(waiter);
    if (index >= 0) this.waiters.splice(index, 1);
    if (waiter.onAbort) waiter.signal?.removeEventListener("abort", waiter.onAbort);
  }
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

async function waitFor(predicate: () => boolean, turns = 200): Promise<void> {
  for (let index = 0; index < turns; index++) {
    if (predicate()) return;
    await Promise.resolve();
  }
  throw new Error("condition did not become true");
}

type StartBehavior = (runtime: FakeRuntime) => void | Promise<void>;

class FakeRuntime implements RoutableSandboxRuntime {
  state = "configured";
  starts = 0;
  stops = 0;
  drains = 0;
  heartbeatAgeMs: number | null = 0;
  credentialExpiresInMs: number | null = 60_000;
  readonly listeners = new Set<() => void>();
  stopBehavior?: () => void | Promise<void>;
  drainBehavior?: () => void | Promise<void>;
  replacementSandbox: ProvisionedSandbox | null = null;
  runCalls = 0;
  runFailure: unknown = null;

  constructor(
    readonly projectId: ProjectId = PROJECT,
    readonly startBehavior?: StartBehavior,
  ) {}

  async start(): Promise<void> {
    this.starts++;
    if (this.startBehavior) await this.startBehavior(this);
    else this.setState("ready");
  }

  async drain(): Promise<void> {
    this.drains++;
    await this.drainBehavior?.();
    this.setState("stopped");
  }

  async stop(): Promise<void> {
    this.stops++;
    await this.stopBehavior?.();
    this.setState("stopped");
  }

  async run(
    _body?: Record<string, unknown>,
    _signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.runCalls++;
    if (this.runFailure) throw this.runFailure;
    options?.onAdmissionReflected?.();
    options?.onExecutionRiskReflected?.();
    return {
      async *events() {},
      final: Promise.resolve({ id: "resp_supervised", status: "completed" }),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
  }
  takeReplacementSandbox(): ProvisionedSandbox | null {
    const value = this.replacementSandbox;
    this.replacementSandbox = null;
    return value;
  }

  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: this.projectId,
      releaseRevision: "r3",
      protocolRevision: "r2",
      buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64),
      state: this.state,
      capacity: this.state === "ready" ? 128 : 0,
      active: 0,
      available: this.state === "ready" ? 128 : 0,
      quarantined: 0,
      heartbeatAgeMs: this.heartbeatAgeMs,
      credentialExpiresInMs: this.credentialExpiresInMs,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  setState(state: string): void {
    this.state = state;
    this.emit();
  }

  emit(): void {
    for (const listener of this.listeners) listener();
  }
}

function supervisor(input: {
  clock: ManualClock;
  initial: FakeRuntime;
  create: () => RoutableSandboxRuntime | Promise<RoutableSandboxRuntime>;
  heartbeatGraceMs?: number;
  startupTimeoutMs?: number;
  acquirePermit?: (signal: AbortSignal) => Promise<() => void>;
  onRecoveryEvent?: (event: SandboxSupervisorEvent) => void;
}): SupervisedSandboxRuntime {
  return new SupervisedSandboxRuntime({
    projectId: PROJECT,
    initialChild: input.initial,
    createChild: input.create,
    clock: input.clock,
    random: { next: () => 0 },
    checkIntervalMs: 1_000,
    heartbeatGraceMs: input.heartbeatGraceMs ?? 40_000,
    startupTimeoutMs: input.startupTimeoutMs ?? 10_000,
    initialBackoffMs: 1_000,
    maxBackoffMs: 60_000,
    acquirePermit: input.acquirePermit,
    onRecoveryEvent: input.onRecoveryEvent,
  });
}

describe("SupervisedSandboxRuntime", () => {
  it("forwards admission and execution-risk ownership to the selected child exactly once", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });
    await runtime.start();
    let reflected = 0;
    let executionRisks = 0;
    const handle = await runtime.run(
      { input: "handoff" },
      undefined,
      {
        onAdmissionReflected: () => { reflected++; },
        onExecutionRiskReflected: () => { executionRisks++; },
      },
    );
    expect(reflected).toBe(1);
    expect(executionRisks).toBe(1);
    await handle.final;
    await runtime.stop();
  });

  it("returns an explicit pre-commit guard before the supervisor is live", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });

    await expect(runtime.run({ input: "not-live" })).rejects.toMatchObject({
      name: "V2PreCommitError",
      committed: false,
      code: "supervisor_not_live",
    });
    expect(initial.runCalls).toBe(0);
  });

  it("returns an explicit pre-commit guard while recovering without dispatching", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let release!: (runtime: RoutableSandboxRuntime) => void;
    const pending = new Promise<RoutableSandboxRuntime>(resolve => { release = resolve; });
    const replacement = new FakeRuntime();
    const runtime = supervisor({ clock, initial, create: () => pending });
    await runtime.start();
    initial.setState("failed");
    await waitFor(() => runtime.supervisorSnapshot().phase === "recovering");
    expect(runtime.snapshot()).toMatchObject({
      releaseRevision: null,
      protocolRevision: null,
      buildHash: null,
      schemaHash: null,
    });

    await expect(runtime.run({ input: "recovering" })).rejects.toEqual(
      expect.objectContaining({
        name: "V2PreCommitError",
        committed: false,
        code: "supervisor_recovering",
      }),
    );
    expect(initial.runCalls).toBe(0);
    release(replacement);
    await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");
    expect(runtime.snapshot()).toMatchObject({ releaseRevision: "r3", protocolRevision: "r2" });
    await runtime.stop();
  });

  it("returns an explicit pre-commit guard while a first child is absent", async () => {
    const clock = new ManualClock();
    const child = new FakeRuntime();
    const runtime = new SupervisedSandboxRuntime({
      projectId: PROJECT,
      createChild: () => child,
      clock,
      random: { next: () => 0 },
      checkIntervalMs: 1_000,
      startupTimeoutMs: 10_000,
      initialBackoffMs: 1_000,
      maxBackoffMs: 60_000,
    });
    const starting = runtime.start();

    const guarded = runtime.run({ input: "no-child" });
    await expect(guarded).rejects.toBeInstanceOf(V2PreCommitError);
    await expect(guarded).rejects.toMatchObject({
      committed: false,
      code: "supervisor_no_child",
    });
    await starting;
    await runtime.stop();
  });

  it("never wraps a child error after dispatch may have escaped", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const escaped = Object.assign(new Error("child transport failed"), {
      committed: true,
      ambiguous: true,
      code: "transport",
    });
    initial.runFailure = escaped;
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });
    await runtime.start();

    let observed: unknown;
    try {
      await runtime.run({ input: "escaped" });
    } catch (error) {
      observed = error;
    }
    expect(observed).toBe(escaped);
    expect(initial.runCalls).toBe(1);
    await runtime.stop();
  });

  it("replaces an unexpectedly failed child immediately and emits recovery changes", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const replacement = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return replacement; },
    });
    let changes = 0;
    runtime.subscribe(() => { changes++; });

    await runtime.start();
    initial.setState("failed");
    await waitFor(() => creates === 1 && runtime.supervisorSnapshot().phase === "healthy");

    expect(creates).toBe(1);
    expect(initial.stops).toBe(1);
    expect(replacement.starts).toBe(1);
    expect(runtime.snapshot().state).toBe("ready");
    expect(runtime.supervisorSnapshot()).toMatchObject({
      intendedLive: true,
      phase: "healthy",
      childGeneration: 2,
    });
    expect(changes).toBeGreaterThan(2);
    await runtime.stop();
  });

  it("backs off failed fresh generations and eventually becomes ready", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const failed = new FakeRuntime(PROJECT, runtime => {
      runtime.setState("failed");
      throw new Error("provisioning unavailable");
    });
    const recovered = new FakeRuntime();
    const queue = [failed, recovered];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return queue.shift()!; },
    });

    await runtime.start();
    initial.setState("failed");
    await waitFor(() => runtime.supervisorSnapshot().recovery?.nextRetryAtMs !== null);
    expect(creates).toBe(1);
    expect(runtime.supervisorSnapshot().recovery).toMatchObject({
      consecutiveFailures: 1,
      nextRetryAtMs: clock.now() + 1_000,
    });

    clock.advanceBy(999);
    await flush();
    expect(creates).toBe(1);
    clock.advanceBy(1);
    await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");

    expect(creates).toBe(2);
    expect(recovered.starts).toBe(1);
    expect(runtime.supervisorSnapshot().phase).toBe("healthy");
    expect(runtime.supervisorSnapshot().recovery).toBeUndefined();
    await runtime.stop();
  });

  it("never adopts a replacement from another Prism project", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const wrongProject = projectId("00000000-0000-4000-8000-000000000043");
    const wrong = new FakeRuntime(wrongProject);
    const correct = new FakeRuntime(PROJECT);
    const replacements = [wrong, correct];
    const runtime = supervisor({
      clock,
      initial,
      create: () => replacements.shift()!,
    });
    await runtime.start();

    initial.setState("failed");
    await waitFor(() => runtime.supervisorSnapshot().recovery?.consecutiveFailures === 1);
    expect(wrong.stops).toBe(1);
    clock.advanceBy(1_000);
    await waitFor(() => runtime.snapshot().state === "ready");

    expect(runtime.snapshot().projectId).toBe(PROJECT);
    expect(runtime.snapshot().incarnation).toBe(2);
    await runtime.stop();
  });

  it("passes a pre-acquired sandbox to exactly one fresh generation", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const seed = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "replacement-token",
      sandbox_session_id: "replacement-session",
    });
    initial.replacementSandbox = seed;
    const seen: Array<ProvisionedSandbox | undefined> = [];
    const runtime = new SupervisedSandboxRuntime({
      projectId: PROJECT,
      initialChild: initial,
      createChild: candidate => {
        seen.push(candidate);
        return new FakeRuntime();
      },
      clock,
      random: { next: () => 0 },
      checkIntervalMs: 1_000,
      heartbeatGraceMs: 40_000,
      startupTimeoutMs: 10_000,
      initialBackoffMs: 1_000,
      maxBackoffMs: 60_000,
    });
    await runtime.start();

    initial.setState("failed");
    await waitFor(() => runtime.snapshot().state === "ready");

    expect(seen).toEqual([seed]);
    expect(initial.takeReplacementSandbox()).toBeNull();
    await runtime.stop();
  });

  it("recovers stale and missing heartbeats after the configured grace", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const staleReplacement = new FakeRuntime();
    const missingReplacement = new FakeRuntime();
    missingReplacement.heartbeatAgeMs = null;
    const finalReplacement = new FakeRuntime();
    const queue = [staleReplacement, missingReplacement, finalReplacement];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      heartbeatGraceMs: 2_000,
      create: () => { creates++; return queue.shift()!; },
    });

    await runtime.start();
    initial.heartbeatAgeMs = 2_001;
    clock.advanceBy(1_000);
    await flush();
    expect(creates).toBe(1);
    await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");

    staleReplacement.setState("failed");
    await waitFor(() => creates === 2);
    expect(creates).toBe(2);
    for (let tick = 0; tick < 4 && creates < 3; tick++) {
      clock.advanceBy(1_000);
      await flush();
    }
    await waitFor(() => creates === 3);
    expect(creates).toBe(3);
    await waitFor(() => runtime.snapshot().state === "ready");
    await runtime.stop();
  });

  it("does not recycle a sandbox during one missed-heartbeat window", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const replacement = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return replacement; },
    });
    await runtime.start();

    initial.heartbeatAgeMs = 25_000;
    clock.advanceBy(5_000);
    await flush();
    expect(creates).toBe(0);

    initial.heartbeatAgeMs = 40_000;
    clock.advanceBy(5_000);
    await waitFor(() => creates === 1);
    await waitFor(() => runtime.snapshot().state === "ready");
    await runtime.stop();
  });

  it("reprovisions auth-required and expired-credential generations", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const afterAuth = new FakeRuntime();
    const afterExpiry = new FakeRuntime();
    const queue = [afterAuth, afterExpiry];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return queue.shift()!; },
    });

    await runtime.start();
    initial.setState("auth_required");
    await flush();
    expect(creates).toBe(1);
    await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");

    afterAuth.credentialExpiresInMs = 0;
    afterAuth.emit();
    await waitFor(() => creates === 2);
    expect(creates).toBe(2);
    await waitFor(() => runtime.snapshot().state === "ready");
    await runtime.stop();
  });

  it("keeps recovery single-flight while a replacement factory is pending", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const replacement = new FakeRuntime();
    let release!: (runtime: RoutableSandboxRuntime) => void;
    const gate = new Promise<RoutableSandboxRuntime>(resolve => { release = resolve; });
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return gate; },
    });

    await runtime.start();
    initial.setState("failed");
    initial.emit();
    initial.emit();
    clock.advanceBy(5_000);
    await flush();
    expect(creates).toBe(1);

    release(replacement);
    await flush();
    expect(creates).toBe(1);
    expect(replacement.starts).toBe(1);
    expect(runtime.snapshot().state).toBe("ready");
    await runtime.stop();
  });

  it("does not lose an unhealthy event that a child overwrites before start returns", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const flapping = new FakeRuntime(PROJECT, async child => {
      child.setState("failed");
      await Promise.resolve();
      child.setState("ready");
    });
    const healthy = new FakeRuntime();
    const children = [flapping, healthy];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return children.shift()!; },
    });
    await runtime.start();

    initial.setState("failed");
    await waitFor(() => runtime.supervisorSnapshot().recovery?.nextRetryAtMs !== null);
    expect(flapping.stops).toBe(1);
    clock.advanceBy(1_000);
    await waitFor(() => runtime.snapshot().state === "ready");

    expect(creates).toBe(2);
    expect(runtime.snapshot().incarnation).toBe(3);
    await runtime.stop();
  });

  it("keeps recovery backoff when the first heartbeat of a fresh child fails", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const awaitingHeartbeat = new FakeRuntime();
    awaitingHeartbeat.heartbeatAgeMs = null;
    const healthy = new FakeRuntime();
    const children = [awaitingHeartbeat, healthy];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return children.shift()!; },
    });
    await runtime.start();

    initial.setState("failed");
    await waitFor(() => creates === 1);
    await waitFor(() => awaitingHeartbeat.starts === 1);
    awaitingHeartbeat.setState("failed");
    clock.advanceBy(1_000);
    await waitFor(() => runtime.supervisorSnapshot().recovery?.nextRetryAtMs !== null);
    expect(creates).toBe(1);
    clock.advanceBy(999);
    await flush();
    expect(creates).toBe(1);
    clock.advanceBy(1);
    await waitFor(() => runtime.snapshot().state === "ready");

    expect(creates).toBe(2);
    await runtime.stop();
  });

  it("aborts retry timers on shutdown and never resurrects", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => {
        creates++;
        return new FakeRuntime(PROJECT, child => {
          child.setState("failed");
          throw new Error("still down");
        });
      },
    });

    await runtime.start();
    initial.setState("failed");
    await waitFor(() => runtime.supervisorSnapshot().recovery?.nextRetryAtMs !== null);
    expect(creates).toBe(1);
    expect(runtime.supervisorSnapshot().recovery?.nextRetryAtMs).not.toBeNull();

    await runtime.stop();
    expect(clock.pendingTimers()).toBe(0);
    clock.advanceBy(1_000_000);
    await flush();
    expect(creates).toBe(1);
    expect(runtime.snapshot().state).toBe("stopped");
    await expect(runtime.start()).rejects.toThrow("has been stopped");
  });

  it("retires a child that resolves after shutdown aborted its factory", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let resolveLate!: (runtime: FakeRuntime) => void;
    const pending = new Promise<FakeRuntime>(resolve => { resolveLate = resolve; });
    const late = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return pending; },
    });
    await runtime.start();
    initial.setState("failed");
    await waitFor(() => creates === 1);

    await runtime.stop();
    resolveLate(late);
    await waitFor(() => late.stops === 1);

    expect(runtime.snapshot().state).toBe("stopped");
  });

  it("isolates throwing subscribers from startup and cleanup", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });
    runtime.subscribe(() => { throw new Error("listener failure"); });

    await expect(runtime.start()).resolves.toBeUndefined();
    await expect(runtime.stop()).resolves.toBeUndefined();
  });

  it("emits bounded recovery lifecycle telemetry", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const replacement = new FakeRuntime();
    const events: SandboxSupervisorEvent[] = [];
    const runtime = supervisor({
      clock,
      initial,
      create: () => replacement,
      onRecoveryEvent: event => events.push(event),
    });
    await runtime.start();
    initial.setState("failed");
    await waitFor(() => runtime.snapshot().state === "ready");
    await runtime.stop();

    expect(events.map(event => event.type)).toEqual([
      "started",
      "completed",
      "started",
      "completed",
    ]);
    expect(events.every(event => event.projectId === PROJECT)).toBe(true);
    expect(JSON.stringify(events)).not.toContain("token");
  });

  it("bounds a hung graceful drain and falls back to force stop", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    initial.drainBehavior = () => new Promise<void>(() => undefined);
    const runtime = supervisor({
      clock,
      initial,
      create: () => new FakeRuntime(),
      startupTimeoutMs: 2_000,
    });
    await runtime.start();

    const draining = runtime.drain("test-shutdown");
    await flush();
    expect(initial.drains).toBe(1);
    clock.advanceBy(2_000);
    await flush();
    expect(initial.stops).toBe(1);
    await draining;
  });

  it("times out a stuck startup and shares a permit only across create plus start", async () => {
    const clock = new ManualClock();
    let releaseStart!: () => void;
    const stuck = new FakeRuntime(PROJECT, () => new Promise<void>(resolve => { releaseStart = resolve; }));
    const permits = new RecoveryPermitPool(1);
    const runtime = supervisor({
      clock,
      initial: stuck,
      startupTimeoutMs: 2_000,
      create: () => new FakeRuntime(),
      acquirePermit: signal => permits.acquire(signal),
    });

    const starting = runtime.start();
    const startupResult = starting.then(
      () => null,
      error => error as Error,
    );
    await flush();
    expect(permits.snapshot().inUse).toBe(1);
    clock.advanceBy(2_000);
    const startupError = await startupResult;
    expect(startupError?.message).toContain("did not start within 2000ms");
    await flush();
    expect(permits.snapshot().inUse).toBe(0);
    releaseStart();
    await runtime.stop();
  });

  it("bounds a stuck child retirement before starting a fresh generation", async () => {
    const clock = new ManualClock();
    const never = new Promise<void>(() => undefined);
    const initial = new FakeRuntime();
    initial.stopBehavior = () => never;
    const replacement = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return replacement; },
      startupTimeoutMs: 2_000,
    });
    await runtime.start();

    initial.setState("failed");
    await flush();
    expect(creates).toBe(0);
    clock.advanceBy(2_000);
    await waitFor(() => runtime.snapshot().state === "ready");

    expect(creates).toBe(1);
    expect(runtime.snapshot().incarnation).toBe(2);
    await runtime.stop();
  });

  it("times out a stuck replacement factory and retires its late child", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let resolveLate!: (runtime: FakeRuntime) => void;
    const late = new Promise<FakeRuntime>(resolve => { resolveLate = resolve; });
    const lateChild = new FakeRuntime();
    const replacement = new FakeRuntime();
    const events: SandboxSupervisorEvent[] = [];
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => ++creates === 1 ? late : replacement,
      startupTimeoutMs: 2_000,
      onRecoveryEvent: event => events.push(event),
    });
    await runtime.start();

    initial.setState("failed");
    await flush();
    clock.advanceBy(2_000);
    await waitFor(() => runtime.supervisorSnapshot().recovery?.consecutiveFailures === 1);
    clock.advanceBy(1_000);
    await waitFor(() => runtime.snapshot().state === "ready");
    resolveLate(lateChild);
    await waitFor(() => lateChild.stops === 1);

    expect(creates).toBe(2);
    expect(runtime.snapshot().incarnation).toBe(2);
    const retry = events.find(event => event.type === "retry_scheduled")!;
    const completed = events.at(-1)!;
    expect(retry).toMatchObject({
      attempt: 1,
      attemptDurationMs: 2_000,
      totalDurationMs: 2_000,
      retryDelayMs: 1_000,
    });
    expect(completed).toMatchObject({
      type: "completed",
      attempt: 2,
      attemptDurationMs: 0,
      totalDurationMs: 3_000,
    });
    await runtime.stop();
  });

  it("continues beyond sixty-five failed generations and eventually recovers", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => {
        creates++;
        if (creates <= 65) {
          return new FakeRuntime(PROJECT, child => {
            child.setState("failed");
            throw new Error("still unavailable");
          });
        }
        return new FakeRuntime(PROJECT);
      },
    });
    await runtime.start();
    initial.setState("failed");

    for (let attempt = 0; attempt < 70 && runtime.snapshot().state !== "ready"; attempt++) {
      await waitFor(() => (
        runtime.snapshot().state === "ready" ||
        typeof runtime.supervisorSnapshot().recovery?.nextRetryAtMs === "number"
      ));
      const retryAt = runtime.supervisorSnapshot().recovery?.nextRetryAtMs;
      if (retryAt !== null && retryAt !== undefined) {
        clock.advanceBy(Math.max(0, retryAt - clock.now()));
        await flush(50);
      }
    }

    await waitFor(() => runtime.snapshot().state === "ready", 1_000);
    expect(creates).toBe(66);
    expect(runtime.snapshot().projectId).toBe(PROJECT);
    expect(runtime.supervisorSnapshot().recovery).toBeUndefined();
    await runtime.stop();
  });
});

describe("supervisor recovery utilities", () => {
  it("uses bounded exponential jitter", () => {
    expect(sandboxRecoveryBackoffMs(1, 1_000, 60_000, () => 0)).toBe(1_000);
    expect(sandboxRecoveryBackoffMs(2, 1_000, 60_000, () => 1)).toBe(2_400);
    expect(sandboxRecoveryBackoffMs(20, 1_000, 60_000, () => 1)).toBe(60_000);
  });

  it("queues permits FIFO and removes an aborted waiter", async () => {
    const pool = new RecoveryPermitPool(1);
    const release = await pool.acquire();
    const controller = new AbortController();
    const waiting = pool.acquire(controller.signal);
    controller.abort();
    await expect(waiting).rejects.toHaveProperty("name", "AbortError");
    expect(pool.snapshot()).toEqual({ limit: 1, inUse: 1, queued: 0 });
    release();
    expect(pool.snapshot()).toEqual({ limit: 1, inUse: 0, queued: 0 });
  });
});
