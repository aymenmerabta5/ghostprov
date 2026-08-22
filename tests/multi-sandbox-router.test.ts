import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

class FakeRuntime implements RoutableSandboxRuntime {
  active = 0;
  executions = 0;
  state: "ready" | "draining" = "ready";
  failStarts = 0;
  readonly finals: Array<ReturnType<typeof deferred<any>>> = [];
  constructor(readonly projectId: ProjectId, readonly capacity = 30) {}
  async start(): Promise<void> {}
  async stop(): Promise<void> {}
  snapshot() {
    return {
      projectId: this.projectId,
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
    } as const;
  }
  async run(): Promise<RunHandle> {
    this.executions++;
    if (this.failStarts > 0) {
      this.failStarts--;
      throw new Error("pre-publication failure");
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

function createSystem(projects = 4, slots = 30) {
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
  const router = new MultiSandboxRouter({ registry, governor, random: { next: () => 0.5, uuid: () => "id" } });
  return { runtimes, registry, governor, router };
}

async function flush(turns = 20): Promise<void> {
  for (let index = 0; index < turns; index++) await Promise.resolve();
}

describe("MultiSandboxRouter", () => {
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
