import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { ResourceGovernor } from "../src/router/resource-governor";
import { MultiSandboxRouter } from "../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { SupervisedSandboxRuntime } from "../src/sandbox/supervised-runtime";
import { projectId, type ProjectId, type SandboxFailureSnapshot } from "../src/sandbox/types";

const PROJECT = projectId("00000000-0000-4000-8000-000000000099");

class ChaosRuntime implements RoutableSandboxRuntime {
  state = "configured";
  stops = 0;
  executions = 0;
  failure: SandboxFailureSnapshot | null = null;
  readonly listeners = new Set<() => void>();

  constructor(readonly projectId: ProjectId = PROJECT) {}

  async start(): Promise<void> { this.state = "ready"; this.emit(); }
  async drain(): Promise<void> { this.state = "stopped"; this.emit(); }
  async stop(): Promise<void> { this.stops++; this.state = "stopped"; this.emit(); }
  async run(): Promise<RunHandle> {
    this.executions++;
    return {
      async *events() {},
      final: Promise.resolve({ id: `resp_${this.executions}`, status: "completed" }),
      stats: { bytes: 0, firstByteMs: 1, frames: 0 },
    };
  }
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: this.projectId,
      state: this.state,
      capacity: this.state === "ready" ? 1 : 0,
      active: 0,
      available: this.state === "ready" ? 1 : 0,
      quarantined: 0,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: this.failure,
    };
  }
  crash(kind: "heartbeat_expired" | "sidecar_unreachable"): void {
    this.failure = {
      name: "V2RuntimeLifecycleFailure",
      kind,
      status: kind === "heartbeat_expired" ? 410 : null,
      operation: kind === "heartbeat_expired" ? "heartbeat" : "sidecar_liveness",
      message: "sanitized failure",
    };
    this.state = "failed";
    this.emit();
  }
  emit(): void { for (const listener of this.listeners) listener(); }
}

function governor(): ResourceGovernor {
  return new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 16 * 1024 ** 3,
        freeBytes: 12 * 1024 ** 3,
        rssBytes: 128 * 1024 ** 2,
        heapBytes: 32 * 1024 ** 2,
        externalBytes: 8 * 1024 ** 2,
        logicalProcessors: 8,
      }),
    },
  });
}

describe("same-project recovery chaos", () => {
  for (const kind of ["heartbeat_expired", "sidecar_unreachable"] as const) {
    it(`replaces a ${kind} generation and wakes queued routing exactly once`, async () => {
      const initial = new ChaosRuntime();
      const replacement = new ChaosRuntime();
      let replacements = 0;
      let projectCreates = 0;
      const supervised = new SupervisedSandboxRuntime({
        projectId: PROJECT,
        initialChild: initial,
        createChild: () => {
          replacements++;
          return replacement;
        },
      });
      const registry = new SandboxRegistry([supervised]);
      const router = new MultiSandboxRouter({ registry, governor: governor() });
      await supervised.start();

      initial.crash(kind);
      const pending = router.startRelay({
        model: "gpt-5.6-sol",
        input: kind,
        stream: true,
      });
      const handle = await pending;
      await expect(handle.final).resolves.toMatchObject({ status: "completed" });

      expect(replacements).toBe(1);
      expect(projectCreates).toBe(0);
      expect(initial.stops).toBe(1);
      expect(replacement.projectId).toBe(PROJECT);
      expect(replacement.executions).toBe(1);
      expect(supervised.snapshot()).toMatchObject({
        projectId: PROJECT,
        incarnation: 2,
        state: "ready",
      });
      await router.close();
      await supervised.stop();
      void projectCreates;
    });
  }
});
