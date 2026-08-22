import { describe, expect, it } from "bun:test";
import type { MachineProbe } from "../src/core/ports";
import type { RunHandle } from "../src/relay/run-handle";
import { ResourceGovernor } from "../src/router/resource-governor";
import {
  MultiSandboxRouter,
  RouterShutdownError,
} from "../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");

class Runtime implements RoutableSandboxRuntime {
  active = 0;
  resolve!: (value: Record<string, unknown>) => void;
  readonly projectId = PROJECT;
  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}
  async run(): Promise<RunHandle> {
    this.active++;
    const final = new Promise<Record<string, unknown>>(resolve => {
      this.resolve = resolve;
    }).finally(() => { this.active--; });
    return {
      async *events() {},
      final,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    };
  }
  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: PROJECT,
      state: "ready",
      capacity: 1,
      active: this.active,
      available: Math.max(0, 1 - this.active),
      quarantined: 0,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
}

const probe: MachineProbe = {
  sample: () => ({
    totalBytes: 32 * 1024 ** 3,
    freeBytes: 20 * 1024 ** 3,
    rssBytes: 256 * 1024 ** 2,
    heapBytes: 64 * 1024 ** 2,
    externalBytes: 16 * 1024 ** 2,
    logicalProcessors: 8,
  }),
};

describe("graceful router shutdown", () => {
  it("rejects new and queued work but waits for an existing terminal", async () => {
    const runtime = new Runtime();
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor: new ResourceGovernor({ probe }),
    });
    const handle = await router.startRelay({ model: "gpt-5.6-sol", input: "active" });
    const queued = router.startRelay({ model: "gpt-5.6-sol", input: "queued" })
      .catch(error => error);

    router.beginShutdown();

    expect(await queued).toBeInstanceOf(RouterShutdownError);
    await expect(router.startRelay({ model: "gpt-5.6-sol", input: "new" }))
      .rejects.toMatchObject({ status: 503, code: "router_shutting_down" });
    const idle = router.awaitIdle(1_000);
    expect(await Promise.race([
      idle.then(() => "idle"),
      Bun.sleep(10).then(() => "pending"),
    ])).toBe("pending");

    runtime.resolve({ id: "resp_1", status: "completed" });
    await handle.final;

    await expect(idle).resolves.toBeUndefined();
    expect(router.snapshot()).toMatchObject({ active: 0, queued: 0 });
    await router.close();
  });

  it("times out without claiming an unproven terminal", async () => {
    const runtime = new Runtime();
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor: new ResourceGovernor({ probe }),
    });
    await router.startRelay({ model: "gpt-5.6-sol", input: "active" });
    router.beginShutdown();

    await expect(router.awaitIdle(5)).rejects.toThrow("timed out");
    expect(runtime.active).toBe(1);
    runtime.resolve({ id: "resp_1", status: "completed" });
    await router.close();
  });
});
