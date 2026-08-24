import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import type { RunHandle } from "../src/relay/run-handle";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { traceRunHandle } from "../src/observability/traced-run-handle";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

class FakeClock implements Clock {
  value = 100;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> { this.value = deadlineMs; }
}

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  emit(event: ObservabilityEvent): void { this.events.push(event); }
  snapshot() { return { queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }; }
  async close(): Promise<void> {}
}

describe("traceRunHandle", () => {
  it("logs first consumption and one terminal without changing the handle", async () => {
    const final = deferred<any>();
    const stats: RunHandle["stats"] = { bytes: 25, firstByteMs: 8, frames: 1, diagnostics: { hostPrepareMs:10,hostSchedulerWaitMs:5,hostPublishToAckMs:20,sidecarFirstFrameToCommitMs:4,sidecarClaimSyncMs:2,sidecarAdmissionWaitMs:1,sidecarCommitProcessingMs:3,transportCommitResidualMs:17,upstreamFirstEventMs:100,sidecarFirstEventSchedulerMs:4,hostAckToFirstFrameMs:25,hostAssemblyToDeliveryMs:1,peakTransport:null } };
    const original: RunHandle = {
      async *events() { yield { event: "response.output_text.delta", data: "{}" }; },
      final: final.promise,
      stats,
    };
    const clock = new FakeClock();
    const logger = new MemoryLogger();
    const traced = traceRunHandle(original, {
      logger,
      clock,
      traceId: "trace-1",
      projectId: "00000000-0000-4000-8000-000000000001",
      acceptedAtMs: 90,
      queueWaitMs: 2,
      commitAckMs: 3,
      committedAtMs: 98,
      retryCount: 0,
      slowRequestMs: 1_000,
    });

    const frames = [];
    for await (const frame of traced.events()) frames.push(frame);
    clock.value = 120;
    final.resolve({ id: "resp_1", status: "completed" });
    await expect(traced.final).resolves.toMatchObject({ id: "resp_1" });

    expect(frames).toHaveLength(1);
    expect(traced.stats).toBe(stats);
    expect(logger.events.map(event => event.event)).toEqual([
      "response.first_event",
      "request.completed",
    ]);
    expect(logger.events[0]).toMatchObject({ first_event_ms: 10, post_commit_first_event_ms: 2, upstream_first_event_ms:100, sidecar_scheduler_ms:4, host_ack_to_first_frame_ms:25 });
    expect(logger.events[1]).toMatchObject({ total_ms: 30, response_bytes: 25, frames: 1 });
  });

  it("logs a committed terminal failure once", async () => {
    const logger = new MemoryLogger();
    const final = Promise.reject(Object.assign(new Error("upstream failed"), {
      kind: "ambiguous",
      status: 502,
    }));
    void final.catch(() => undefined);
    const traced = traceRunHandle({
      async *events() {},
      final,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }, {
      logger,
      clock: new FakeClock(),
      traceId: "trace-2",
      projectId: "00000000-0000-4000-8000-000000000001",
      acceptedAtMs: 90,
      queueWaitMs: 1,
      commitAckMs: 2,
      committedAtMs: 99,
      retryCount: 0,
      slowRequestMs: 1_000,
    });

    await expect(traced.final).rejects.toThrow("upstream failed");
    expect(logger.events).toHaveLength(1);
    expect(logger.events[0]).toMatchObject({
      event: "request.failed",
      committed: true,
      error_kind: "ambiguous",
      status: 502,
    });
  });

  it("logs V2 committed ambiguity as quarantined with its code", async () => {
    const logger = new MemoryLogger();
    const final = Promise.reject(Object.assign(new Error("sidecar unavailable"), {
      committed: true,
      ambiguous: true,
      code: "sidecar_unreachable",
    }));
    void final.catch(() => undefined);
    const traced = traceRunHandle({
      async *events() {},
      final,
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }, {
      logger,
      clock: new FakeClock(),
      traceId: "trace-3",
      projectId: "00000000-0000-4000-8000-000000000001",
      acceptedAtMs: 90,
      queueWaitMs: 1,
      commitAckMs: 2,
      committedAtMs: 99,
      retryCount: 0,
      slowRequestMs: 1_000,
    });

    await expect(traced.final).rejects.toThrow("sidecar unavailable");
    expect(logger.events[0]).toMatchObject({
      event: "request.failed",
      committed: true,
      quarantined: true,
      error_kind: "sidecar_unreachable",
    });
  });
});
