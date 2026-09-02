import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { MultiSandboxRouter } from "../src/router/router";
import { ResourceGovernor } from "../src/router/resource-governor";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { V2PreCommitError } from "../src/v2/transport/run-handle";

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
  waitForAdmissionAbort = false;
  transportProfile: "standard" | "bulk-v1" | null = null;
  emitAdmissionProgress = false;
  afterAdmissionProgress?: () => void;
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
      transportProfile: this.transportProfile,
      state: "ready" as const,
      capacity: 128,
      active: this.active,
      available: 128 - this.active,
      availabilityReason: this.active < 128 ? "available" as const : "execution_window" as const,
      quarantined: 0,
      heartbeatAgeMs: 10,
      sidecarPeerAgeMs: 20,
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 12,
      completionEwmaMs: 2_000,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
  async run(
    _body?: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    if (this.emitAdmissionProgress) {
      options?.onAdmissionProgress?.({
        stage: "request_data",
        acknowledgedEncodedBytes: 64 * 1024,
        totalEncodedBytes: Math.floor(2.9 * 1024 ** 2),
      });
      this.afterAdmissionProgress?.();
    }
    if (this.waitForAdmissionAbort) {
      await new Promise<never>((_resolve, reject) => {
        const safe = () => reject(new V2PreCommitError(
          "observability fake proved no provider execution",
          "test_precommit_abort",
        ));
        if (signal?.aborted) return safe();
        signal?.addEventListener("abort", safe, { once: true });
      });
    }
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
      model: "PRIVATE PROMPT IN MODEL",
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
    expect(serialized).not.toContain("PRIVATE PROMPT IN MODEL");
    expect(serialized).not.toContain("private-client");
    expect(logger.events.find(event => event.event === "request.accepted"))
      .toMatchObject({ model: "unknown" });
    for (const event of logger.events) {
      expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
    }
    expect(logger.events.find(event => event.event === "request.queued")).toMatchObject({
      queued: 1,
      queued_bytes: expect.any(Number),
      queue_class: "latency",
    });
    const routeSelected = logger.events.find(event => event.event === "route.selected");
    const sidecarPeerAge = (routeSelected as unknown as Record<string, unknown>)
      .sidecar_peer_age_ms;
    expect(routeSelected).toMatchObject({
      project_id: runtime.projectId,
      eligible_candidates: 1,
      sandbox_admission_credits: 128,
      sandbox_commit_backlog: 0,
      sandbox_scheduler_bytes: 0,
      sandbox_scheduler_oldest_ms: 0,
      sandbox_throughput_bytes_per_sec: null,
      sidecar_peer_age_ms: expect.any(Number),
      queue_class: "latency",
      fair_cost_bytes: 4 * 1024,
      route_attempt: 1,
      capacity_reason: "available",
    });
    expect(typeof sidecarPeerAge).toBe("number");
    if (typeof sidecarPeerAge === "number") {
      expect(sidecarPeerAge).toBeGreaterThanOrEqual(20);
      expect(sidecarPeerAge).toBeLessThan(1_000);
    }
  });

  it("records a sanitized selected-runtime attempt expiry", async () => {
    const logger = new MemoryLogger();
    const runtime = new Runtime();
    runtime.waitForAdmissionAbort = true;
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([runtime]),
      governor: new ResourceGovernor({
        probe: { sample: () => ({
          totalBytes: 32 * 1024 ** 3,
          freeBytes: 20 * 1024 ** 3,
          rssBytes: 100 * 1024 ** 2,
          heapBytes: 50 * 1024 ** 2,
          externalBytes: 10 * 1024 ** 2,
          logicalProcessors: 12,
        }) },
      }),
      logger,
      queueTimeoutMs: 45,
      routeAttemptTimeoutMs: 10,
      random: { next: () => 0.5, uuid: () => "trace-timeout" },
    });
    try {
      await expect(router.startRelay({
        model: "gpt-5.6-sol",
        input: "must not be logged",
        stream: true,
      }, {
        source: "http",
        encodedBodyBytes: Math.floor(2.9 * 1024 * 1024),
      })).rejects.toMatchObject({ code: "transport_admission_timeout" });
      const expired = logger.events.filter(event => event.event === "route.attempt_expired");
      expect(expired.length).toBeGreaterThan(0);
      expect(expired[0]).toMatchObject({
        project_id: runtime.projectId,
        route_attempt: 1,
        progress_observed: false,
        progress_age_ms: null,
        progress_stage: null,
        progress_acknowledged_encoded_bytes: null,
        progress_total_encoded_bytes: null,
        deadline_exhausted: false,
        capacity_reason: "available",
      });
      expect(JSON.stringify(expired)).not.toContain("must not be logged");
      for (const event of expired) {
        expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
      }
    } finally {
      await router.close();
    }
  });

  it("records observed progress and its age when a large R5 attempt becomes stale", async () => {
    const logger = new MemoryLogger();
    let clockOffsetMs = 0;
    const clock = {
      now: () => performance.timeOrigin + performance.now() + clockOffsetMs,
      sleep: async (ms: number) => { await Bun.sleep(ms); },
      waitUntil: async (deadlineMs: number) => {
        const remaining = deadlineMs - (performance.timeOrigin + performance.now() + clockOffsetMs);
        if (remaining > 0) await Bun.sleep(remaining);
      },
    };
    const fallback = new Runtime("00000000-0000-4000-8000-000000000001");
    const stalled = new Runtime("00000000-0000-4000-8000-000000000002");
    fallback.transportProfile = "bulk-v1";
    stalled.transportProfile = "bulk-v1";
    stalled.waitForAdmissionAbort = true;
    stalled.emitAdmissionProgress = true;
    stalled.afterAdmissionProgress = () => { clockOffsetMs = 6_000; };
    const router = new MultiSandboxRouter({
      registry: new SandboxRegistry([fallback, stalled]),
      governor: new ResourceGovernor({
        probe: { sample: () => ({
          totalBytes: 32 * 1024 ** 3,
          freeBytes: 20 * 1024 ** 3,
          rssBytes: 100 * 1024 ** 2,
          heapBytes: 50 * 1024 ** 2,
          externalBytes: 10 * 1024 ** 2,
          logicalProcessors: 12,
        }) },
        now: () => clock.now(),
      }),
      logger,
      clock,
      // Preserve enough global slack that stale progress must still retry;
      // the separate late-budget policy test covers retaining bytes when a
      // fresh full transfer can no longer fit before the absolute deadline.
      queueTimeoutMs: 20_000,
      routeAttemptTimeoutMs: 10,
      random: { next: () => 0.5, uuid: () => "trace-progress-timeout" },
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "must not be logged",
        stream: true,
      }, {
        source: "http",
        encodedBodyBytes: Math.floor(2.9 * 1024 * 1024),
        structuralBodyBytes: Math.floor(2.9 * 1024 * 1024),
      });
      const expired = logger.events.filter(event => event.event === "route.attempt_expired");
      expect(expired).toHaveLength(1);
      const progressAge = (expired[0] as unknown as Record<string, unknown>).progress_age_ms;
      expect(typeof progressAge).toBe("number");
      expect(progressAge as number).toBeGreaterThanOrEqual(5_999);
      expect(progressAge as number).toBeLessThan(6_100);
      expect(expired[0]).toMatchObject({
        project_id: stalled.projectId,
        route_attempt: 1,
        progress_observed: true,
        progress_age_ms: expect.any(Number),
        progress_stage: "request_data",
        progress_acknowledged_encoded_bytes: 64 * 1024,
        progress_total_encoded_bytes: Math.floor(2.9 * 1024 ** 2),
        deadline_exhausted: false,
        capacity_reason: "available",
      });
      expect(JSON.stringify(expired)).not.toContain("must not be logged");
      fallback.terminal.resolve({ id: "resp_progress_fallback", status: "completed" });
      await handle.final;
    } finally {
      await router.close();
    }
  });
});
