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
import { FleetStartupPermitPool } from "../src/fleet/startup-permits";

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
  sidecarPeerAgeMs: number | null = null;
  transportConnected: boolean | undefined;
  credentialExpiresInMs: number | null = 60_000;
  readonly listeners = new Set<() => void>();
  stopBehavior?: () => void | Promise<void>;
  drainBehavior?: () => void | Promise<void>;
  localCleanupComplete?: () => boolean;
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
    options?.onAdmissionProgress?.({
      stage: "request_data",
      acknowledgedEncodedBytes: 1,
      totalEncodedBytes: 1,
    });
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
      sidecarPeerAgeMs: this.sidecarPeerAgeMs,
      transportConnected: this.transportConnected,
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
  shutdownDeadlineMs?: () => number | undefined;
  recoverBootstrapCleanup?: (signal: AbortSignal) => Promise<"not_required" | "proven" | "unproved">;
  acquirePermit?: (signal: AbortSignal) => Promise<() => void>;
  onRecoveryEvent?: (event: SandboxSupervisorEvent) => void;
}): SupervisedSandboxRuntime {
  return new SupervisedSandboxRuntime({
    projectId: input.initial.projectId,
    initialChild: input.initial,
    createChild: input.create,
    clock: input.clock,
    random: { next: () => 0 },
    checkIntervalMs: 1_000,
    heartbeatGraceMs: input.heartbeatGraceMs ?? 40_000,
    startupTimeoutMs: input.startupTimeoutMs ?? 10_000,
    shutdownDeadlineMs: input.shutdownDeadlineMs,
    recoverBootstrapCleanup: input.recoverBootstrapCleanup,
    initialBackoffMs: 1_000,
    maxBackoffMs: 60_000,
    acquirePermit: input.acquirePermit,
    onRecoveryEvent: input.onRecoveryEvent,
  });
}

describe("SupervisedSandboxRuntime", () => {
  it("retains a failed child's physical cleanup witness beyond the public stop rejection", async () => {
    const clock = new ManualClock(), child = new FakeRuntime();
    let physicallyClosed = false;
    child.localCleanupComplete = () => physicallyClosed;
    child.stopBehavior = async () => { child.state = "stopped"; throw new Error("offline remote cleanup unproven"); };
    const runtime = supervisor({ clock, initial: child, create: () => new FakeRuntime() });
    await runtime.start();
    await expect(runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(runtime.localCleanupComplete()).toBe(false);
    physicallyClosed = true;
    expect(runtime.localCleanupComplete()).toBe(true);
    expect(child.stops).toBe(1);
    await expect(runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(child.stops).toBe(1);
  });

  it("keeps actual pending stop ownership after its public timeout even if the child reports local completion", async () => {
    const clock = new ManualClock(), child = new FakeRuntime();
    let finish!: () => void;
    const pending = new Promise<void>(resolve => { finish = resolve; });
    child.localCleanupComplete = () => true;
    child.stopBehavior = () => { child.state = "stopped"; return pending; };
    const runtime = supervisor({ clock, initial: child, create: () => new FakeRuntime(), startupTimeoutMs: 1000 });
    await runtime.start();
    const stopped = runtime.stop().then(() => null, error => error as Error);
    await flush(); clock.advanceBy(1000);
    expect(await stopped).toBeInstanceOf(Error);
    expect(runtime.localCleanupComplete()).toBe(false);
    finish(); await flush();
    expect(runtime.localCleanupComplete()).toBe(true);
    expect(child.stops).toBe(1);
  });

  it("requires cleanup-only proof before constructing another blocked bootstrap generation", async () => {
    const clock = new ManualClock();
    const failure = Object.assign(new Error("offline bootstrap cleanup remains unproven"), {
      name: "V2BootstrapError", kind: "deploy", operation: "bootstrap",
      cause: Object.assign(new Error("offline original owner"), {
        name: "V2SupervisorBootstrapError", kind: "deploy", remoteCode: "bootstrap_cleanup_unproven",
      }),
    });
    const events: SandboxSupervisorEvent[] = [];
    let creates = 0, probes = 0, submitted = false;
    const runtime = supervisor({ clock, initial: new FakeRuntime(PROJECT, async () => { submitted = true; throw failure; }),
      create: () => { creates++; return new FakeRuntime(); },
      recoverBootstrapCleanup: async () => {
        if (!submitted) return "not_required";
        probes++;
        return probes === 3 ? "proven" : "unproved";
      },
      onRecoveryEvent: event => events.push(event),
    });
    await expect(runtime.start()).rejects.toBe(failure);
    await flush();
    for (const [delay, count] of [[1000, 1], [2000, 2]] as const) {
      clock.advanceBy(delay);
      await waitFor(() => runtime.supervisorSnapshot().recovery?.consecutiveFailures === count + 1);
      expect(probes).toBe(count);
      expect(creates).toBe(0);
      expect(runtime.snapshot().incarnation).toBe(1);
      await flush();
    }
    clock.advanceBy(4000);
    await waitFor(() => runtime.snapshot().state === "ready");
    expect(probes).toBe(3);
    expect(creates).toBe(1);
    expect(runtime.snapshot().incarnation).toBe(2);
    const retries = events.filter(event => event.type === "retry_scheduled");
    expect(retries.map(event => event.attempt)).toEqual([1, 2, 3]);
    expect(retries.every(event => event.failure?.bootstrapCode === "bootstrap_cleanup_unproven")).toBe(true);
    await runtime.stop();
  });

  it("cancels an owned cleanup-only probe without starting a successor", async () => {
    const clock = new ManualClock();
    const failure = Object.assign(new Error("offline bootstrap blocked"), { name: "V2BootstrapError", kind: "deploy", operation: "bootstrap",
      cause: Object.assign(new Error("offline cleanup"), { name: "V2SupervisorBootstrapError", kind: "deploy", remoteCode: "bootstrap_cleanup_unproven" }) });
    let probes = 0, creates = 0, aborted = false, submitted = false;
    const runtime = supervisor({ clock, initial: new FakeRuntime(PROJECT, async () => { submitted = true; throw failure; }),
      create: () => { creates++; return new FakeRuntime(); },
      recoverBootstrapCleanup: async signal => {
        if (!submitted) return "not_required";
        probes++;
        return new Promise<"unproved">((_resolve, reject) => {
          signal.addEventListener("abort", () => { aborted = true; reject(signal.reason); }, { once: true });
        });
      },
    });
    await expect(runtime.start()).rejects.toBe(failure);
    await flush(); clock.advanceBy(1000);
    await waitFor(() => probes === 1);
    await runtime.stop();
    expect(aborted).toBe(true);
    expect(creates).toBe(0);
    expect(probes).toBe(1);
  });

  it.each(["absent", "prepared"] as const)("allows %s journals through initial startup and unrelated bootstrap retries", async phase => {
    const clock = new ManualClock();
    const failure = Object.assign(new Error("offline bootstrap timeout"), { name: "V2BootstrapError", kind: "timeout", operation: "bootstrap" });
    let assessments = 0, creates = 0;
    const journal = phase === "absent" ? null : { phase: "prepared" as const };
    const runtime = supervisor({ clock, initial: new FakeRuntime(PROJECT, async () => { throw failure; }),
      create: () => { creates++; return new FakeRuntime(); },
      recoverBootstrapCleanup: async () => {
        assessments++;
        expect(journal?.phase ?? null).toBe(phase === "absent" ? null : "prepared");
        return "not_required";
      },
    });
    await expect(runtime.start()).rejects.toBe(failure);
    await flush(); clock.advanceBy(1000);
    await waitFor(() => runtime.snapshot().state === "ready");
    expect(assessments).toBe(2);
    expect(creates).toBe(1);
    await runtime.stop();
  });

  it("assesses first readiness cleanup debt before another cold permit or child", async () => {
    const clock = new ManualClock();
    const failure = Object.assign(new Error("offline first readiness failure"), {
      name: "V2BootstrapError", kind: "deploy", operation: "bootstrap",
      cause: Object.assign(new Error("offline supervisor readiness timed out"), {
        name: "V2SupervisorBootstrapError", kind: "readiness",
      }),
    });
    let submitted = false, proofAvailable = false, assessments = 0, coldAdmissions = 0, creates = 0;
    const initial = new FakeRuntime(PROJECT, async () => { submitted = true; throw failure; });
    initial.localCleanupComplete = () => true;
    initial.stopBehavior = async () => {
      initial.state = "stopped";
      throw new Error("offline submitted supervisor cleanup remains unproven");
    };
    const runtime = supervisor({ clock, initial,
      create: () => { creates++; return new FakeRuntime(); },
      acquirePermit: async () => { coldAdmissions++; return () => {}; },
      recoverBootstrapCleanup: async () => {
        assessments++;
        return !submitted ? "not_required" : proofAvailable ? "proven" : "unproved";
      },
    });
    try {
      await expect(runtime.start()).rejects.toBe(failure);
      await flush();
      expect(runtime.cleanupProofRequired()).toBe(true);
      clock.advanceBy(1000);
      await flush(100);
      expect(coldAdmissions).toBe(1);
      expect(creates).toBe(0);
      expect(assessments).toBe(2);
      expect(initial.starts).toBe(1);
      expect(initial.stops).toBe(1);

      proofAvailable = true;
      const retryAt = runtime.supervisorSnapshot().recovery?.nextRetryAtMs;
      expect(typeof retryAt).toBe("number");
      clock.advanceBy(retryAt! - clock.now());
      await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");
      expect(coldAdmissions).toBe(2);
      expect(creates).toBe(1);
      expect(assessments).toBe(3);
      expect(runtime.cleanupProofRequired()).toBe(false);
    } finally { await runtime.stop().catch(() => undefined); }
    expect(clock.pendingTimers()).toBe(0);
  });

  it("keeps first-readiness cleanup debts off cold slots while fresh workers start", async () => {
    const clock = new ManualClock(), permits = new FleetStartupPermitPool(4, 2);
    const proof = Promise.withResolvers<void>(), prematureStart = Promise.withResolvers<void>();
    let proofAvailable = false, cleanupProbes = 0;
    const creates = [0, 0], coldAdmissions = [0, 0];
    const owners = [0, 1].map(index => {
      const id = projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`);
      let submitted = false;
      const failure = Object.assign(new Error("offline first readiness failure"), {
        name: "V2BootstrapError", kind: "deploy", operation: "bootstrap",
        cause: Object.assign(new Error("offline original supervisor readiness"), {
          name: "V2SupervisorBootstrapError", kind: "readiness",
        }),
      });
      const initial = new FakeRuntime(id, async () => { submitted = true; throw failure; });
      initial.localCleanupComplete = () => true;
      initial.stopBehavior = async () => { initial.state = "stopped"; throw new Error("offline signed cleanup missing"); };
      return supervisor({ clock, initial,
        create: () => {
          creates[index]!++;
          return new FakeRuntime(id, async child => {
            if (!proofAvailable) await prematureStart.promise;
            child.setState("ready");
          });
        },
        acquirePermit: signal => { coldAdmissions[index]!++; return permits.acquire(true, signal); },
        recoverBootstrapCleanup: async signal => {
          if (!submitted) return "not_required";
          const release = await permits.acquire(false, signal);
          cleanupProbes++;
          try { await proof.promise; signal.throwIfAborted(); return "proven"; }
          finally { release(); }
        },
      });
    });
    const fresh = [2, 3].map(index => {
      const id = projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`);
      return supervisor({ clock, initial: new FakeRuntime(id), create: () => new FakeRuntime(id),
        acquirePermit: signal => permits.acquire(true, signal), recoverBootstrapCleanup: async () => "not_required" });
    });
    const freshStarts: Promise<unknown>[] = [];
    try {
      const started = await Promise.allSettled(owners.map(runtime => runtime.start()));
      expect(started.every(result => result.status === "rejected")).toBe(true);
      await flush(); clock.advanceBy(1000); await flush(100);
      freshStarts.push(...fresh.map(runtime => runtime.start().catch(error => error)));
      await flush(100);
      expect(fresh.map(runtime => runtime.supervisorSnapshot().phase)).toEqual(["healthy", "healthy"]);
      expect(cleanupProbes).toBe(2);
      expect(creates).toEqual([0, 0]);
      expect(coldAdmissions).toEqual([1, 1]);
      expect(permits.snapshot()).toEqual({
        total: { limit: 4, inUse: 2, queued: 0 }, cold: { limit: 2, inUse: 0, queued: 0 },
      });

      proofAvailable = true; proof.resolve();
      await waitFor(() => owners.every(runtime => runtime.supervisorSnapshot().phase === "healthy"));
      expect(creates).toEqual([1, 1]);
      expect(coldAdmissions).toEqual([2, 2]);
      expect(cleanupProbes).toBe(2);
    } finally {
      proofAvailable = true; proof.resolve(); prematureStart.resolve();
      await Promise.allSettled([...owners, ...fresh].map(runtime => runtime.stop()));
      await Promise.all(freshStarts);
    }
    expect(permits.snapshot()).toEqual({
      total: { limit: 4, inUse: 0, queued: 0 }, cold: { limit: 2, inUse: 0, queued: 0 },
    });
    expect(clock.pendingTimers()).toBe(0);
  });

  it("waits for first-readiness local cleanup owners before assessing submitted debt", async () => {
    const clock = new ManualClock();
    const failure = Object.assign(new Error("offline first readiness failure"), {
      name: "V2BootstrapError", kind: "deploy", operation: "bootstrap",
      cause: Object.assign(new Error("offline supervisor readiness"), { name: "V2SupervisorBootstrapError", kind: "readiness" }),
    });
    let submitted = false, localComplete = false, assessments = 0, coldAdmissions = 0, creates = 0;
    const initial = new FakeRuntime(PROJECT, async () => { submitted = true; throw failure; });
    initial.localCleanupComplete = () => localComplete;
    initial.stopBehavior = async () => { initial.state = "stopped"; throw new Error("offline cleanup owner retained"); };
    const runtime = supervisor({ clock, initial,
      create: () => { creates++; return new FakeRuntime(); },
      acquirePermit: async () => { coldAdmissions++; return () => {}; },
      recoverBootstrapCleanup: async () => { assessments++; return submitted ? "proven" : "not_required"; },
    });
    try {
      await expect(runtime.start()).rejects.toBe(failure);
      await flush(); clock.advanceBy(1000); await flush(100);
      expect(assessments).toBe(1);
      expect(coldAdmissions).toBe(1);
      expect(creates).toBe(0);
      localComplete = true;
      const retryAt = runtime.supervisorSnapshot().recovery?.nextRetryAtMs;
      expect(typeof retryAt).toBe("number");
      clock.advanceBy(retryAt! - clock.now());
      await waitFor(() => runtime.supervisorSnapshot().phase === "healthy");
      expect(assessments).toBe(2);
      expect(creates).toBe(1);
    } finally { localComplete = true; await runtime.stop().catch(() => undefined); }
    expect(clock.pendingTimers()).toBe(0);
  });

  it("forwards admission progress and execution-risk ownership to the selected child exactly once", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });
    await runtime.start();
    let reflected = 0;
    let progress = 0;
    let executionRisks = 0;
    const handle = await runtime.run(
      { input: "handoff" },
      undefined,
      {
        onAdmissionReflected: () => { reflected++; },
        onAdmissionProgress: () => { progress++; },
        onExecutionRiskReflected: () => { executionRisks++; },
      },
    );
    expect(reflected).toBe(1);
    expect(progress).toBe(1);
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

  it("accepts fresh exact-sidecar proof while heartbeat is missing at startup", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    initial.heartbeatAgeMs = null;
    initial.sidecarPeerAgeMs = 0;
    initial.transportConnected = true;
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      heartbeatGraceMs: 2_000,
      create: () => { creates++; return new FakeRuntime(); },
    });

    await runtime.start();

    expect(runtime.supervisorSnapshot().phase).toBe("healthy");
    expect(runtime.snapshot()).toMatchObject({ state: "ready", capacity: 128 });
    expect(creates).toBe(0);
    await runtime.stop();
  });

  it("does not replace a heartbeat-stale runtime until exact-sidecar proof is also lost", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    initial.heartbeatAgeMs = 40_001;
    initial.sidecarPeerAgeMs = 500;
    initial.transportConnected = true;
    const replacement = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({
      clock,
      initial,
      create: () => { creates++; return replacement; },
    });
    await runtime.start();

    for (let sample = 0; sample < 5; sample++) {
      clock.advanceBy(1_000);
      initial.heartbeatAgeMs += 1_000;
      initial.sidecarPeerAgeMs = 500;
      initial.emit();
      await flush();
    }
    expect(runtime.supervisorSnapshot().phase).toBe("healthy");
    expect(runtime.snapshot()).toMatchObject({ state: "ready", capacity: 128 });
    expect(creates).toBe(0);

    initial.transportConnected = false;
    initial.emit();
    await waitFor(() => creates === 1);
    await waitFor(() => runtime.snapshot().state === "ready");
    initial.emit();
    await flush();
    expect(creates).toBe(1);
    expect(initial.stops).toBe(1);
    await runtime.stop();
  });

  it("preserves an eight-runtime fleet during correlated heartbeat loss", async () => {
    const clock = new ManualClock();
    const children = Array.from({ length: 8 }, (_, index) => {
      const child = new FakeRuntime(projectId(
        `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`,
      ));
      child.heartbeatAgeMs = 40_001;
      child.sidecarPeerAgeMs = 250 + index * 1_250;
      child.transportConnected = true;
      return child;
    });
    const replacementCounts = Array.from({ length: 8 }, () => 0);
    const supervisors = children.map((child, index) => new SupervisedSandboxRuntime({
      projectId: child.projectId,
      initialChild: child,
      createChild: () => {
        replacementCounts[index]++;
        return new FakeRuntime(child.projectId);
      },
      clock,
      random: { next: () => 0 },
      checkIntervalMs: 1_000,
      heartbeatGraceMs: 40_000,
      startupTimeoutMs: 10_000,
      initialBackoffMs: 1_000,
      maxBackoffMs: 60_000,
    }));
    await Promise.all(supervisors.map(runtime => runtime.start()));

    for (let phase = 0; phase < children.length; phase++) {
      children[phase]!.emit();
      clock.advanceBy(1_000);
      await flush();
    }

    expect(replacementCounts).toEqual([0, 0, 0, 0, 0, 0, 0, 0]);
    expect(supervisors.map(runtime => runtime.supervisorSnapshot().phase))
      .toEqual(Array.from({ length: 8 }, () => "healthy"));
    expect(supervisors.reduce((total, runtime) => total + runtime.snapshot().capacity, 0))
      .toBe(8 * 128);
    await Promise.all(supervisors.map(runtime => runtime.stop()));
  });

  it("defers stale-heartbeat replacement only until the unroutable child's recovery deadline", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    const replacement = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({ clock, initial, create: () => { creates++; return replacement; } });
    await runtime.start();
    const snapshot = initial.snapshot.bind(initial);
    const recovery = { startedAtMs: clock.now(), deadlineAtMs: clock.now() + 150_000 };
    initial.snapshot = () => ({ ...snapshot(), capacity: 0, available: 0, livenessRecovery: recovery });
    initial.heartbeatAgeMs = 40_001;
    initial.transportConnected = false;
    initial.emit();
    await flush();
    clock.advanceBy(149_999);
    await flush();
    expect(creates).toBe(0);
    expect(runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0, available: 0 });
    clock.advanceBy(1);
    initial.emit();
    await waitFor(() => creates === 1);
    await waitFor(() => runtime.snapshot().available === 128);
    expect(initial.stops).toBe(1);
    await runtime.stop();
    expect(clock.pendingTimers()).toBe(0);
  });

  it("does not let a recovery episode suppress credential expiry", async () => {
    const clock = new ManualClock();
    const initial = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({ clock, initial, create: () => { creates++; return new FakeRuntime(); } });
    await runtime.start();
    const snapshot = initial.snapshot.bind(initial);
    initial.snapshot = () => ({
      ...snapshot(), capacity: 0, available: 0, credentialExpiresInMs: 0,
      livenessRecovery: { startedAtMs: clock.now(), deadlineAtMs: clock.now() + 150_000 },
    });
    initial.emit();
    await waitFor(() => creates === 1);
    await waitFor(() => runtime.snapshot().available === 128);
    await runtime.stop();
    expect(clock.pendingTimers()).toBe(0);
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
    expect(runtime.localCleanupComplete()).toBe(false);
    resolveLate(late);
    await waitFor(() => late.stops === 1);
    await flush();

    expect(runtime.snapshot().state).toBe("stopped");
    expect(runtime.localCleanupComplete()).toBe(true);
  });

  it("revokes a cached successful stop verdict when a late factory child's remote cleanup fails", async () => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    let resolveLate!: (child: FakeRuntime) => void;
    const pending = new Promise<FakeRuntime>(resolve => { resolveLate = resolve; });
    const late = new FakeRuntime();
    late.localCleanupComplete = () => true;
    late.stopBehavior = async () => { late.state = "stopped"; throw new Error("offline remote cleanup unproven"); };
    let creates = 0;
    const runtime = supervisor({ clock, initial, create: () => { creates++; return pending; } });
    await runtime.start();
    initial.setState("failed");
    await waitFor(() => creates === 1);
    await expect(runtime.stop()).resolves.toBeUndefined();
    expect(runtime.localCleanupComplete()).toBe(false);
    resolveLate(late);
    await waitFor(() => late.stops === 1);
    await flush();
    expect(runtime.localCleanupComplete()).toBe(true);
    await expect(runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(runtime.cleanupProofRequired()).toBe(true);
    expect(late.stops).toBe(1);
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

  it.each(["stop", "drain"] as const)("waits beyond the recovery cap for a child during system %s", async kind => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    const cleanup = Promise.withResolvers<void>();
    initial.stopBehavior = () => cleanup.promise;
    initial.drainBehavior = () => cleanup.promise;
    let shutdownDeadline: number | undefined;
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime(),
      startupTimeoutMs: 120_000, shutdownDeadlineMs: () => shutdownDeadline });
    await runtime.start();
    shutdownDeadline = clock.now() + 60_000;
    let settled = false;
    const stopping = (kind === "stop" ? runtime.stop() : runtime.drain("system-shutdown"))
      .finally(() => { settled = true; });
    await flush();
    clock.advanceBy(35_000);
    await flush();
    expect(settled).toBe(false);
    expect(initial.stops).toBe(kind === "stop" ? 1 : 0);
    cleanup.resolve();
    await expect(stopping).resolves.toBeUndefined();
    expect(runtime.snapshot().state).toBe("stopped");
    expect(clock.pendingTimers()).toBe(0);
  });

  it("does not renew the shared system deadline when drain falls back to stop", async () => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    const cleanup = Promise.withResolvers<void>();
    initial.drainBehavior = () => cleanup.promise;
    // A fallback that completes immediately after expiry is still too late.
    initial.stopBehavior = async () => {};
    let shutdownDeadline: number | undefined;
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime(),
      shutdownDeadlineMs: () => shutdownDeadline });
    await runtime.start();
    shutdownDeadline = clock.now() + 60_000;
    const stopping = runtime.drain("system-shutdown").then(() => null, error => error);
    await flush();
    clock.advanceBy(60_000);
    await flush();
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(initial.drains).toBe(1);
    expect(initial.stops).toBe(1);
    cleanup.resolve();
    await flush();
    expect(clock.pendingTimers()).toBe(0);
    await expect(runtime.stop()).rejects.toThrow("cleanup was incomplete");
  });

  it("rejects a child cleanup that resolves at the deadline before its timer runs", async () => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    const cleanup = Promise.withResolvers<void>();
    initial.stopBehavior = () => cleanup.promise;
    let shutdownDeadline: number | undefined;
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime(),
      shutdownDeadlineMs: () => shutdownDeadline });
    await runtime.start();
    shutdownDeadline = clock.now() + 60_000;
    const stopping = runtime.stop().then(() => null, error => error);
    await flush();
    // Advance the clock without dispatching timers, as after a stalled event loop.
    clock.value = shutdownDeadline;
    cleanup.resolve();
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(initial.stops).toBe(1);
    expect(clock.pendingTimers()).toBe(0);
  });

  it.each(["stop", "drain"] as const)("preserves a child's terminal cleanup rejection through %s and repeated calls", async kind => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    initial.stopBehavior = async () => { throw new Error("offline cleanup unproven"); };
    initial.drainBehavior = initial.stopBehavior;
    const runtime = supervisor({ clock, initial, create: () => new FakeRuntime() });
    await runtime.start();
    const stopping = kind === "stop" ? runtime.stop() : runtime.drain("offline_shutdown");
    await expect(stopping).rejects.toThrow("cleanup was incomplete");
    expect(runtime.snapshot().state).toBe("stopped");
    expect(runtime.stop()).toBe(stopping);
    await expect(runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(initial.stops).toBe(1);
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

  it("bounds retained factory owners when timed-out factories never settle", async () => {
    const clock = new ManualClock(), initial = new FakeRuntime();
    let creates = 0;
    const runtime = supervisor({ clock, initial, startupTimeoutMs: 1000,
      create: () => { creates++; return new Promise<RoutableSandboxRuntime>(() => undefined); } });
    await runtime.start();
    initial.setState("failed");
    await flush();
    for (let count = 1; count <= 4; count++) {
      clock.advanceBy(1000);
      await waitFor(() => runtime.supervisorSnapshot().recovery?.consecutiveFailures === count);
      await flush();
      clock.advanceBy(1000 * 2 ** (count - 1));
      await flush();
    }
    await waitFor(() => runtime.supervisorSnapshot().recovery?.consecutiveFailures === 5);
    expect(creates).toBe(4);
    expect(runtime.snapshot().state).toBe("recovering");
    await runtime.stop();
    expect(runtime.localCleanupComplete()).toBe(false);
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
  it("limits never-ready cold starts without blocking warm recovery headroom", async () => {
    const transitions: Array<{
      action: string;
      coldStart: boolean;
      coldInUse: number;
      totalInUse: number;
    }> = [];
    const pool = new FleetStartupPermitPool(4, 2, {
      onTransition: transition => transitions.push({
        action: transition.action,
        coldStart: transition.coldStart,
        coldInUse: transition.snapshot.cold.inUse,
        totalInUse: transition.snapshot.total.inUse,
      }),
    });
    const coldA = await pool.acquire(true);
    const coldB = await pool.acquire(true);
    const waitingCold = pool.acquire(true);
    const warmA = await pool.acquire(false);
    const warmB = await pool.acquire(false);
    expect(pool.snapshot()).toEqual({
      total: { limit: 4, inUse: 4, queued: 0 },
      cold: { limit: 2, inUse: 2, queued: 1 },
    });

    warmA();
    warmB();
    coldA();
    const coldC = await waitingCold;
    expect(pool.snapshot()).toEqual({
      total: { limit: 4, inUse: 2, queued: 0 },
      cold: { limit: 2, inUse: 2, queued: 0 },
    });
    coldB();
    coldC();
    expect(pool.snapshot()).toEqual({
      total: { limit: 4, inUse: 0, queued: 0 },
      cold: { limit: 2, inUse: 0, queued: 0 },
    });
    expect(Math.max(...transitions.map(value => value.coldInUse))).toBe(2);
    expect(Math.max(...transitions.map(value => value.totalInUse))).toBe(4);
    expect(transitions.filter(value => value.action === "acquired" && value.coldStart))
      .toHaveLength(3);
  });

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
