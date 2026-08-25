import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(res => { resolve = res; });
  return { promise, resolve };
}

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  emit(event: ObservabilityEvent): void { this.events.push(event); }
  snapshot() { return { queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }; }
  async close(): Promise<void> {}
}

class Runtime implements RoutableSandboxRuntime {
  readonly projectId;
  readonly terminal = deferred<any>();
  constructor(
    id = "00000000-0000-4000-8000-000000000001",
    readonly active = 0,
  ) {
    this.projectId = projectId(id);
  }
  async start(): Promise<void> {}
  async drain(): Promise<void> {}
  async stop(): Promise<void> {}
  snapshot() {
    return {
      projectId: this.projectId,
      state: "ready" as const,
      capacity: 128,
      active: this.active,
      available: 128 - this.active,
      quarantined: 0,
      heartbeatAgeMs: 10,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 12,
      completionEwmaMs: 2_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
  async run(): Promise<RunHandle> {
    return {
      async *events() { yield { event: "response.completed", data: "{}" }; },
      final: this.terminal.promise,
      stats: { bytes: 50, firstByteMs: 5, frames: 1 },
    };
  }
}

describe("router observability", () => {
  it("emits ordered lifecycle phases with no request content", async () => {
    const logger = new MemoryLogger();
    const runtime = new Runtime();
    // This second runtime is alive but full. It must remain visible in fleet
    // capacity while not inflating route.selected.eligible_candidates.
    const fullRuntime = new Runtime("00000000-0000-4000-8000-000000000002", 128);
    const registry = new SandboxRegistry([runtime, fullRuntime]);
    const governor = new ResourceGovernor({
      probe: { sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 20 * 1024 ** 3,
        rssBytes: 100 * 1024 ** 2,
        heapBytes: 50 * 1024 ** 2,
        externalBytes: 10 * 1024 ** 2,
        logicalProcessors: 12,
      }) },
    });
    const router = new MultiSandboxRouter({
      registry,
      governor,
      logger,
      random: { next: () => 0.5, uuid: () => "trace-fixed" },
      observability: { slowRequestMs: 10_000 },
    });

    const handle = await router.startRelay({
      model: "gpt-5.6-sol",
      input: "must never appear in logs",
      stream: true,
    }, { source: "websocket", clientId: "private-client" });
    for await (const _frame of handle.events()) {}
    runtime.terminal.resolve({ id: "resp_1", status: "completed" });
    await handle.final;

    expect(logger.events.map(event => event.event)).toEqual([
      "request.accepted",
      "admission.acquired",
      "request.queued",
      "route.selected",
      "transport.committed",
      "response.first_event",
      "request.completed",
    ]);
    const serialized = JSON.stringify(logger.events);
    expect(serialized).not.toContain("must never appear");
    expect(serialized).not.toContain("private-client");
    expect(logger.events.find(event => event.event === "route.selected")).toMatchObject({
      project_id: runtime.projectId,
      eligible_candidates: 1,
    });
  });
});
