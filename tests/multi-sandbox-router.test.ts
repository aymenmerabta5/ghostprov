import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { CpuTransformQueueFullError } from "../src/router/cpu-transform-pool";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { V2CommittedError, V2PreCommitError } from "../src/v2/transport/run-handle";
import type { Clock } from "../src/core/ports";

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
  snapshotCalls = 0;
  state: "ready" | "draining" | "stopped" = "ready";
  failStarts = 0;
  failError: Error = new V2PreCommitError("pre-publication failure", "test_precommit");
  failDelayMs = 0;
  waitForAdmissionAbort = false;
  admissionAbortError: Error | null = null;
  admissionAbortDelayMs = 0;
  ignoreAdmissionAbortMs = 0;
  postAdmissionDelayMs = 0;
  admissionWindow: number | null = null;
  ackEwmaMs = 1_000;
  diagnostics: any = undefined;
  lastRunSignal?: AbortSignal;
  pulseAdmissionOnFailure = false;
  readonly executionStartedAtMs: number[] = [];
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
    return {
      projectId: this.projectId,
      incarnation: this.incarnation,
      state: this.state,
      capacity: this.capacity,
      active: this.active,
      available: Math.max(0, Math.min(
        this.admissionWindow === null
          ? this.capacity
          : this.admissionWindow - this.uncommitted,
        this.capacity - this.active,
      )),
      quarantined: 0,
      heartbeatAgeMs: 1_000,
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
    options?: { onAdmissionReflected(): void },
  ): Promise<RunHandle> {
    this.lastRunSignal = signal;
    this.executions++;
    this.executionStartedAtMs.push(performance.now());
    this.bodies.push(body);
    if (this.failStarts > 0) {
      this.failStarts--;
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
          reject(signal.reason);
          return;
        }
        signal?.addEventListener("abort", () => {
          const fail = () => reject(this.admissionAbortError ?? signal.reason);
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
    options?.onAdmissionReflected();
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
  completeOne(value: unknown = { id: "resp", status: "completed" }): void {
    this.finals.shift()?.resolve(value);
  }
  rejectOne(error: unknown): void {
    this.finals.shift()?.reject(error);
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
  });
  return { runtimes, registry, governor, router };
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

describe("MultiSandboxRouter", () => {
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
    expect(result).toMatchObject({ code: "router_capacity_timeout" });
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
    )).rejects.toMatchObject({ code: "router_capacity_timeout" });
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
    )).rejects.toMatchObject({ code: "router_capacity_timeout" });
    expect(performance.now() - started).toBeLessThan(60);
    expect(system.router.snapshot()).toMatchObject({ active: 0, queued: 0 });
    expect(system.governor.snapshot().reservedBytes).toBe(0);
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

  it("does not await non-cooperative cleanup after the admission handoff deadline", async () => {
    const system = createSystem(2, 1, 200, undefined, undefined, undefined, 20);
    system.runtimes[1]!.ignoreAdmissionAbortMs = 120;
    const started = performance.now();

    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "detach-safe-cleanup", stream: true },
      { clientId: "detach-safe-cleanup", source: "websocket" },
    );

    expect(performance.now() - started).toBeLessThan(80);
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
    )).rejects.toMatchObject({ code: "router_capacity_timeout" });

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

  it("ends the admission deadline once the runtime owns the credit", async () => {
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

  it("forwards caller cancellation after commit until the terminal settles", async () => {
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
    expect(system.runtimes[0].lastRunSignal?.reason).toBe(reason);
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
      error: { code: "router_capacity_timeout" },
    });
    expect(system.runtimes[1].executions).toBe(1);
    expect(system.runtimes[0].executions).toBe(0);
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

  it("defaults non-image work to fast, preserves explicit text tiers, and strips image tiers", async () => {
    const system = createSystem(1, 1);
    const implicit = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "implicit",
    });
    expect(system.runtimes[0].bodies[0]).toMatchObject({ service_tier: "fast" });
    system.runtimes[0].completeOne();
    await implicit.final;

    const explicit = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "explicit",
      service_tier: "priority",
    });
    expect(system.runtimes[0].bodies[1]).toMatchObject({ service_tier: "priority" });
    system.runtimes[0].completeOne();
    await explicit.final;

    const implicitImage = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "implicit image tier",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "medium" }],
    });
    expect(system.runtimes[0].bodies[2]).not.toHaveProperty("service_tier");
    system.runtimes[0].completeOne();
    await implicitImage.final;

    const explicitImage = await system.router.startRelay({
      model: "gpt-5.6-sol",
      input: "explicit image tier",
      service_tier: "priority",
      tools: [{ type: "image_generation", model: "gpt-image-2", quality: "medium" }],
    });
    expect(system.runtimes[0].bodies[3]).not.toHaveProperty("service_tier");
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

  it("downgrades request memory after transport commitment and releases it at terminal", async () => {
    const system = createSystem(1, 1);
    const handle = await system.router.startRelay({
      model: "gpt-5.6-sol",
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
      }, { clientId: "bulk-client", source: "websocket" })),
    ];
    await flush(100);
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
    expect(bulkPositions).toEqual([20, 21]);
    expect(order[0]?.startsWith("small-")).toBe(true);
    await system.router.close();
  });

  it("persists class byte credit across one-request lane refill", async () => {
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
      }).then(handle => ({ queueClass, handle }));
    };
    const waiting: Record<"latency" | "bulk", ReturnType<typeof launch>> = {
      latency: launch("latency"),
      bulk: launch("bulk"),
    };

    runtime.completeOne();
    await blocker.final;
    const order: Array<"latency" | "bulk"> = [];
    for (let turn = 0; turn < 540; turn++) {
      const selected = await Promise.race([waiting.latency, waiting.bulk]);
      order.push(selected.queueClass);
      waiting[selected.queueClass] = launch(selected.queueClass);
      await flush(1);
      runtime.completeOne();
      await selected.handle.final;
    }

    const bulkPositions = order
      .map((queueClass, index) => queueClass === "bulk" ? index : -1)
      .filter(index => index >= 0);
    expect(bulkPositions).toHaveLength(1);
    expect(bulkPositions[0]).toBeGreaterThanOrEqual(500);
    expect(bulkPositions[0]).toBeLessThanOrEqual(520);

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
