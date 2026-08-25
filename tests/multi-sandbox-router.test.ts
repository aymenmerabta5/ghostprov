import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { CpuTransformQueueFullError } from "../src/router/cpu-transform-pool";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

class FakeRuntime implements RoutableSandboxRuntime {
  incarnation = 1;
  active = 0;
  executions = 0;
  state: "ready" | "draining" | "stopped" = "ready";
  failStarts = 0;
  failError: Error = new Error("pre-publication failure");
  failDelayMs = 0;
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
    for (const listener of this.listeners) listener();
  }
  snapshot() {
    return {
      projectId: this.projectId,
      incarnation: this.incarnation,
      state: this.state,
      capacity: this.capacity,
      active: this.active,
      available: Math.max(0, this.capacity - this.active),
      quarantined: 0,
      heartbeatAgeMs: 1_000,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 1_000,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    } as const;
  }
  async run(body: Record<string, unknown>): Promise<RunHandle> {
    this.executions++;
    this.bodies.push(body);
    if (this.failStarts > 0) {
      this.failStarts--;
      if (this.failDelayMs > 0) await Bun.sleep(this.failDelayMs);
      throw this.failError;
    }
    this.active++;
    const gate = deferred<any>();
    this.finals.push(gate);
    return {
      async *events() {},
      final: gate.promise.finally(() => { this.active--; }),
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

function createSystem(projects = 4, slots = 30, queueTimeoutMs?: number) {
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

  it("defaults omitted service tiers to fast and preserves explicit tiers", async () => {
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
    const handle = await system.router.startRelay(
      { model: "gpt-5.6-sol", input: "healthy", stream: true },
      { clientId: "one", source: "http" },
    );
    expect(system.runtimes[0].executions).toBe(0);
    expect(system.runtimes[1].executions).toBe(1);
    system.runtimes[1].completeOne();
    await handle.final;
  });
});
