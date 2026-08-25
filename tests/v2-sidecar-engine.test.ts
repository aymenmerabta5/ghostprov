import { describe, expect, it, spyOn } from "bun:test";
import type { HostFrame, ReliableFrame, SidecarFrame } from "../src/v2/protocol/shared";
import {
  JournalStorageCapacityError,
  type JournalRecord,
} from "../src/v2/sidecar/journal";
import { OutputBackpressure } from "../src/v2/sidecar/output-sink";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { estimateAtomicEventPermitBytes } from "../src/v2/memory/atomic-byte-permits";
import { atomicBytePermitOf } from "../src/v2/memory/permit-carrier";
import { v2RequestBodyWorkingSetBytes } from "../src/v2/memory/request-memory-sizing";
import {
  UpstreamExecutionError,
  type DirectResponsesExecution,
  type UpstreamTerminal,
} from "../src/v2/sidecar/upstream";
import {
  SidecarHarness,
  TEST_BINDING,
  TEST_R2_BINDING,
  completedUpstream,
} from "./support/fake-v2-sidecar";

const BODY = { model: "gpt-5.6-sol", input: "engine", stream: true };

describe("SidecarEngine", () => {
  it("releases transferred request memory after upstream completion", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 10 * 1024 * 1024,
      jumboBytes: 1,
    });
    const harness = new SidecarHarness({ atomicBytePermits: pool });
    await harness.sendRequest(BODY);
    await harness.settle();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
  });

  it("grows a compressed request body before fetch while preserving the jumbo response lane", async () => {
    const MiB = 1024 * 1024;
    expect(v2RequestBodyWorkingSetBytes(64 * MiB)).toBe(128 * MiB + 64 * 1024);
    const jumboCharge = estimateAtomicEventPermitBytes(512 * 1024);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 3 * MiB,
      jumboBytes: jumboCharge,
    });
    let observedBodyCharge = 0;
    const harness = new SidecarHarness({
      atomicBytePermits: pool,
      behavior: async input => {
        const bodyPermit = atomicBytePermitOf(input.body);
        observedBodyCharge = bodyPermit?.bytes ?? 0;
        expect(observedBodyCharge).toBeGreaterThanOrEqual(
          v2RequestBodyWorkingSetBytes(input.body.byteLength),
        );
        const jumbo = pool.tryAcquire(jumboCharge, {
          region: "jumbo",
          label: "concurrent-response-jumbo",
        });
        expect(jumbo).not.toBeNull();
        expect(pool.snapshot()).toMatchObject({
          ordinaryUsedBytes: observedBodyCharge,
          jumboUsedBytes: jumboCharge,
          activePermits: 2,
        });
        jumbo!.release();
        await input.beforeFetch();
        throw new UpstreamExecutionError("test complete", "test_complete", false);
      },
    });

    await harness.sendRequest({ ...BODY, input: "x".repeat(MiB) }, { gzip: true });
    await harness.settle();
    expect(observedBodyCharge).toBeGreaterThan(2 * MiB);
    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
      activePermits: 0,
    });
    pool.close();
  });

  it("aborts startup without waiting for a stalled journal recovery", async () => {
    const harness = new SidecarHarness({
      beforeReadRecords: () => new Promise<void>(() => undefined),
    });
    const controller = new AbortController();
    const starting = harness.engine.start(controller.signal);
    controller.abort();

    await expect(starting).rejects.toMatchObject({ name: "AbortError" });
  });

  it("can hold synchronized frames until staged startup completes", async () => {
    const harness = new SidecarHarness({ deferAcceptUntilStarted: true });
    let accepted = false;
    const waiting = harness.engine.accept({
      kind: "ping",
      sentAtMs: 99,
      epoch: 1,
      sequence: 0,
    }).then(() => { accepted = true; });

    await Bun.sleep(0);
    expect(accepted).toBe(false);
    await harness.engine.start();
    await waiting;

    expect(harness.frames).toEqual([{ kind: "pong", sentAtMs: 99 }]);
  });

  it("retains the legacy reliable bootstrap ping without touching journal or upstream", async () => {
    const harness = new SidecarHarness();
    await harness.engine.start();

    await harness.engine.accept({
      kind: "ping",
      sentAtMs: 12_345.5,
      epoch: 1,
      sequence: 0,
    });

    expect(harness.frames).toEqual([{ kind: "pong", sentAtMs: 12_345.5 }]);
    expect(harness.storage.records).toEqual([]);
    expect(harness.upstream.calls).toBe(0);
  });

  it("acknowledges cancellation only after releasing the active execution lease", async () => {
    const harness = new SidecarHarness({
      behavior: async input => {
        await input.beforeFetch();
        if (input.signal?.aborted) {
          throw new UpstreamExecutionError("cancelled", "cancelled", true);
        }
        return new Promise<UpstreamTerminal>((_resolve, reject) => {
          input.signal?.addEventListener("abort", () => reject(
            new UpstreamExecutionError("cancelled", "cancelled", true),
          ), { once: true });
        });
      },
    });
    await harness.sendRequest(BODY);
    expect(harness.engine.snapshot().active).toBe(1);

    await harness.engine.accept({
      kind: "request_cancel",
      requestId: harness.requestId,
      reason: "client aborted",
      epoch: 1,
      sequence: 3,
    });
    await harness.settle();

    const release = harness.timeline.lastIndexOf("release");
    const acknowledgement = harness.timeline.lastIndexOf("emit:request_cancelled");
    expect(release).toBeGreaterThanOrEqual(0);
    expect(acknowledgement).toBeGreaterThan(release);
    expect(harness.frames.some(frame => frame.kind === "response_error")).toBe(false);
  });

  it("backpressures small batched events before reading more upstream data", async () => {
    const backpressure = new OutputBackpressure({
      highFrames: 1,
      highBytes: 100,
      lowFrames: 0,
      lowBytes: 0,
      hardFrames: 2,
      hardBytes: 200,
    });
    backpressure.update({ frames: 1, bytes: 0 });
    let signalEventAttempt!: () => void;
    const eventAttempted = new Promise<void>(resolve => { signalEventAttempt = resolve; });
    let eventAccepted = false;
    const harness = new SidecarHarness({
      batching: true,
      outputBackpressure: backpressure,
      behavior: async input => {
        await input.beforeFetch();
        signalEventAttempt();
        await input.onEvent({
          event: "response.output_text.delta",
          data: JSON.stringify({ type: "response.output_text.delta", delta: "held" }),
        });
        eventAccepted = true;
        throw new UpstreamExecutionError("test stop", "test_stop", true);
      },
    });

    await harness.sendRequest(BODY);
    await eventAttempted;
    await Bun.sleep(0);
    expect(eventAccepted).toBe(false);
    expect(harness.engine.snapshot().active).toBe(1);

    backpressure.update({ frames: 0, bytes: 0 });
    await harness.settle();
    expect(eventAccepted).toBe(true);
    backpressure.close();
  });

  it("flushes batched response events before terminal proof", async () => {
    const harness = new SidecarHarness({ batching: true });
    await harness.sendRequest({ model: "gpt-5.6-sol", input: "batch" });
    await harness.settle();
    const firstOutput = harness.frames.find(frame =>
      frame.kind === "response_event" || frame.kind === "response_batch"
    );
    expect(firstOutput).toMatchObject({ kind: "response_event", eventIndex: 0 });
    const batchIndex = harness.frames.findIndex(frame => frame.kind === "response_batch");
    const terminalIndex = harness.frames.findIndex(frame => frame.kind === "response_terminal");
    expect(batchIndex).toBeGreaterThanOrEqual(0);
    expect(terminalIndex).toBeGreaterThan(batchIndex);
  });
  it("ACKs R1 after durable claim and permit, then never retries after fetch start", async () => {
    let harness!: SidecarHarness;
    const behavior = async (input: DirectResponsesExecution): Promise<UpstreamTerminal> => {
      await input.beforeFetch();
      harness.timeline.push("fetch");
      throw new UpstreamExecutionError("reset", "transport", true);
    };
    harness = new SidecarHarness({ behavior });

    await harness.sendRequest(BODY);
    await harness.settle();

    const ordered = [
      "sync:claimed",
      "permit",
      "emit:request_ack",
      "sync:fetch_starting",
      "fetch",
      "emit:response_error",
    ];
    expect(harness.timeline.filter(value => ordered.includes(value))).toEqual(ordered);
    expect(harness.upstream.calls).toBe(1);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      ambiguous: true,
    }));
    expect(harness.journal.canRetry(harness.requestId)).toBe(false);
  });

  it("durably replays a definitive provider error without changing it to ambiguity", async () => {
    const rateLimit = { retryAfterMs: 123, limitTokens: 40_000_000 };
    const first = new SidecarHarness({
      behavior: async input => {
        await input.beforeFetch();
        throw new UpstreamExecutionError(
          "Rate limit reached",
          "rate_limit_exceeded",
          false,
          429,
          rateLimit,
        );
      },
    });
    await first.sendRequest(BODY);
    await first.settle();
    expect(first.journal.status(first.requestId)).toBe("provider_error");

    const restarted = new SidecarHarness({ records: first.storage.records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: first.requestId }),
      expect.objectContaining({
        kind: "response_error",
        requestId: first.requestId,
        code: "rate_limit_exceeded",
        message: "Rate limit reached",
        ambiguous: false,
        rateLimit,
      }),
    ]);
    await restarted.engine.acknowledgeDeliveredOutcomes([first.requestId]);
    expect(restarted.journal.status(first.requestId)).toBe("acked");
  });

  it("never emits a definitive provider error when its journal transition fails", async () => {
    let appends = 0;
    const fatals: Error[] = [];
    const first = new SidecarHarness({
      beforeAppend: async () => {
        if (++appends === 3) throw new Error("synthetic provider outcome sync failure");
      },
      onFatalError: error => fatals.push(error),
      behavior: async input => {
        await input.beforeFetch();
        throw new UpstreamExecutionError(
          "Provider rejected the request",
          "provider_rejected",
          false,
          400,
        );
      },
    });
    await first.sendRequest(BODY);
    await first.settle();

    expect(fatals).toHaveLength(1);
    expect(first.journal.status(first.requestId)).toBe("fetch_starting");
    expect(first.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId: first.requestId,
      code: "provider_rejected",
      ambiguous: true,
    }));
    expect(first.frames).not.toContainEqual(expect.objectContaining({
      kind: "response_error",
      ambiguous: false,
    }));

    const restarted = new SidecarHarness({ records: first.storage.records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();
    expect(restarted.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "recovered_ambiguous",
      ambiguous: true,
    }));
  });

  it("durably replays client cancellation without executing upstream again", async () => {
    const first = new SidecarHarness({
      behavior: async input => {
        await input.beforeFetch();
        if (input.signal?.aborted) {
          throw new UpstreamExecutionError("cancelled", "cancelled", true);
        }
        return new Promise<UpstreamTerminal>((_resolve, reject) => {
          input.signal?.addEventListener("abort", () => reject(
            new UpstreamExecutionError("cancelled", "cancelled", true),
          ), { once: true });
        });
      },
    });
    await first.sendRequest(BODY);
    await first.engine.accept({
      kind: "request_cancel",
      requestId: first.requestId,
      reason: "client stopped",
      epoch: 1,
      sequence: 3,
    });
    await first.settle();
    expect(first.journal.status(first.requestId)).toBe("cancelled");

    const restarted = new SidecarHarness({ records: first.storage.records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();
    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: first.requestId }),
      { kind: "request_cancelled", requestId: first.requestId },
    ]);
    await restarted.engine.acknowledgeDeliveredOutcomes([first.requestId]);
    expect(restarted.journal.status(first.requestId)).toBe("acked");
  });

  it("never emits definitive cancellation when its journal transition fails", async () => {
    let appends = 0;
    const fatals: Error[] = [];
    const harness = new SidecarHarness({
      beforeAppend: async () => {
        if (++appends === 3) throw new Error("synthetic cancellation sync failure");
      },
      onFatalError: error => fatals.push(error),
      behavior: async input => {
        await input.beforeFetch();
        if (input.signal?.aborted) {
          throw new UpstreamExecutionError("cancelled", "cancelled", true);
        }
        return new Promise<UpstreamTerminal>((_resolve, reject) => {
          input.signal?.addEventListener("abort", () => reject(
            new UpstreamExecutionError("cancelled", "cancelled", true),
          ), { once: true });
        });
      },
    });
    await harness.sendRequest(BODY);
    await harness.engine.accept({
      kind: "request_cancel",
      requestId: harness.requestId,
      reason: "client aborted",
      epoch: 1,
      sequence: 3,
    });
    await harness.settle();

    expect(fatals).toHaveLength(1);
    expect(harness.journal.status(harness.requestId)).toBe("fetch_starting");
    expect(harness.frames.some(frame => frame.kind === "request_cancelled")).toBe(false);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "cancellation_unproven",
      ambiguous: true,
    }));
  });

  it("treats prefetch, unknown, and duplicate cancellation as request-local idempotence", async () => {
    const fatals: Error[] = [];
    const harness = new SidecarHarness({ onFatalError: error => fatals.push(error) });
    await harness.engine.start();
    await harness.engine.accept({
      kind: "request_open",
      requestId: "req-prefetch-cancel",
      requestDigest: "a".repeat(64),
      decodedBytes: 1,
      encodedBytes: 1,
      encoding: "identity",
      partCount: 1,
      epoch: 1,
      sequence: 0,
    });
    await harness.engine.accept({
      kind: "request_cancel",
      requestId: "req-prefetch-cancel",
      reason: "cancel before body",
      epoch: 1,
      sequence: 1,
    });
    for (let sequence = 2; sequence <= 3; sequence++) {
      await harness.engine.accept({
        kind: "request_cancel",
        requestId: "req-unknown-cancel",
        reason: "duplicate stale cancel",
        epoch: 1,
        sequence,
      });
    }

    expect(fatals).toEqual([]);
    expect(harness.frames.filter(frame => (
      frame.kind === "request_cancelled" && frame.requestId === "req-prefetch-cancel"
    ))).toHaveLength(1);
    expect(harness.frames.filter(frame => (
      frame.kind === "request_cancelled" && frame.requestId === "req-unknown-cancel"
    ))).toHaveLength(2);
    expect(harness.engine.snapshot()).toMatchObject({ active: 0, draining: false });
  });

  it("keeps an unrelated active stream alive across unknown and retired cancellation", async () => {
    const fatals: Error[] = [];
    const active = new SidecarHarness({
      onFatalError: error => fatals.push(error),
      behavior: async input => {
        await input.beforeFetch();
        if (input.signal?.aborted) {
          throw new UpstreamExecutionError("cancelled", "cancelled", true);
        }
        return new Promise<UpstreamTerminal>((_resolve, reject) => {
          input.signal?.addEventListener("abort", () => reject(
            new UpstreamExecutionError("cancelled", "cancelled", true),
          ), { once: true });
        });
      },
    });
    await active.engine.start();
    await active.journal.claim({
      requestId: "req-unrelated-retired",
      requestDigest: "b".repeat(64),
      leaderGeneration: active.binding.leaderGeneration,
    });
    await active.journal.markCancelled("req-unrelated-retired");
    await active.journal.acknowledgeOutcome("req-unrelated-retired");
    expect(active.journal.status("req-unrelated-retired")).toBe("acked");
    await active.sendRequest(BODY);
    expect(active.engine.snapshot().active).toBe(1);
    await active.engine.accept({
      kind: "request_cancel",
      requestId: "req-unrelated-retired",
      reason: "stale cancel",
      epoch: 1,
      sequence: 3,
    });
    expect(active.engine.snapshot().active).toBe(1);
    expect(fatals).toEqual([]);
    expect(active.frames.at(-1)).toEqual({
      kind: "request_cancelled",
      requestId: "req-unrelated-retired",
    });

    await active.engine.accept({
      kind: "request_cancel",
      requestId: active.requestId,
      reason: "test cleanup",
      epoch: 1,
      sequence: 4,
    });
    await active.settle();
    expect(fatals).toEqual([]);
  });

  it("persists terminal before publishing and deletes it only after matching host ACK", async () => {
    const harness = new SidecarHarness();
    await harness.sendRequest(BODY);
    await harness.settle();

    expect(harness.timeline.indexOf("terminal-spool")).toBeLessThan(
      harness.timeline.indexOf("sync:terminal"),
    );
    expect(harness.timeline.indexOf("sync:terminal")).toBeLessThan(
      harness.timeline.lastIndexOf("emit:response_event"),
    );
    expect(harness.timeline.lastIndexOf("emit:response_event")).toBeLessThan(
      harness.timeline.indexOf("emit:response_terminal"),
    );
    const terminalEventIndex = harness.frames.findIndex(frame => (
      frame.kind === "response_event" && frame.event === "response.completed"
    ));
    const terminalProofIndex = harness.frames.findIndex(frame => frame.kind === "response_terminal");
    expect(terminalEventIndex).toBeGreaterThanOrEqual(0);
    expect(terminalEventIndex).toBeLessThan(terminalProofIndex);
    const terminal = harness.frames.find(
      (frame): frame is Extract<SidecarFrame, { kind: "response_terminal" }> => (
        frame.kind === "response_terminal"
      ),
    )!;
    expect(await harness.storage.readTerminal(harness.requestId)).not.toBeNull();

    await harness.engine.accept({
      kind: "response_ack",
      requestId: harness.requestId,
      terminalDigest: "0".repeat(64),
      epoch: 1,
      sequence: 3,
    });
    expect(await harness.storage.readTerminal(harness.requestId)).not.toBeNull();

    const ack: ReliableFrame<HostFrame> = {
      kind: "response_ack",
      requestId: harness.requestId,
      terminalDigest: terminal.transcriptSha256,
      epoch: 1,
      sequence: 3,
    };
    await harness.engine.accept(ack);

    expect(await harness.storage.readTerminal(harness.requestId)).toBeNull();
    expect(harness.timeline.slice(-2)).toEqual(["sync:acked", "terminal-delete"]);
  });

  it("never exposes a terminal SSE event when terminal journaling fails", async () => {
    let appends = 0;
    const fatals: Error[] = [];
    const harness = new SidecarHarness({
      beforeAppend: async () => {
        if (++appends === 3) throw new Error("synthetic terminal journal failure");
      },
      onFatalError: error => { fatals.push(error); },
    });
    await harness.sendRequest(BODY);
    await harness.settle();

    expect(fatals).toHaveLength(1);
    expect(harness.journal.status(harness.requestId)).toBe("fetch_starting");
    expect(harness.frames.some(frame => (
      frame.kind === "response_event" &&
      (frame.event === "response.completed" ||
        frame.event === "response.failed" ||
        frame.event === "response.incomplete")
    ))).toBe(false);
    expect(harness.frames.some(frame => frame.kind === "response_terminal")).toBe(false);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      ambiguous: true,
    }));
  });

  it("replays a duplicate committed request without a second upstream call", async () => {
    const harness = new SidecarHarness();
    await harness.sendRequest(BODY);
    await harness.settle();
    const firstTerminalCount = harness.frames.filter(
      frame => frame.kind === "response_terminal",
    ).length;

    await harness.sendRequest(BODY);
    await harness.settle();

    expect(harness.upstream.calls).toBe(1);
    expect(harness.frames.filter(frame => frame.kind === "response_terminal")).toHaveLength(
      firstTerminalCount + 1,
    );
  });

  it("segments large response events below the physical frame limit", async () => {
    const delta = "x".repeat(30_000);
    const harness = new SidecarHarness({
      behavior: input => completedUpstream(input, delta),
    });
    await harness.sendRequest(BODY);
    await harness.settle();

    const deltaParts = harness.frames.filter(frame => (
      frame.kind === "response_event" && frame.event === "response.output_text.delta"
    ));
    expect(deltaParts.length).toBeGreaterThan(1);
    for (const frame of harness.frames) {
      expect(new TextEncoder().encode(JSON.stringify(frame)).byteLength).toBeLessThan(16 * 1024);
    }
  });

  it("replays proven terminal spools after restart without calling upstream", async () => {
    const prelude = {
      status: 200,
      contentType: "text/event-stream; charset=utf-8",
      upstreamRequestId: "req_exact_replay",
    };
    const first = new SidecarHarness({
      binding: TEST_R2_BINDING,
      behavior: input => completedUpstream({
        ...input,
        beforeFetch: async () => {
          await input.beforeFetch();
          await input.onPrelude(prelude);
        },
      }, "x".repeat(160 * 1024)),
    });
    await first.sendRequest(BODY);
    await first.settle();
    const replayPermits = new AtomicBytePermitPool({
      ordinaryBytes: 10 * 1024 * 1024,
      jumboBytes: 257 * 1024 * 1024,
    });
    const restarted = new SidecarHarness({
      binding: TEST_R2_BINDING,
      records: first.storage.records,
      terminals: first.storage.terminals,
      atomicBytePermits: replayPermits,
    });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toContainEqual(expect.objectContaining({
      kind: "response_terminal",
      responseId: "resp_1",
    }));
    expect(restarted.frames).toContainEqual({
      kind: "upstream_prelude",
      requestId: first.requestId,
      ...prelude,
    });
    expect(restarted.frames.filter(frame => frame.kind === "response_event").length).toBeGreaterThan(0);
    expect(restarted.frames.findIndex(frame => frame.kind === "request_ack")).toBeLessThan(
      restarted.frames.findIndex(frame => frame.kind === "response_event"),
    );
    expect(restarted.storage.terminalReadPasses).toBe(2);
    expect(restarted.storage.terminalMaxChunkBytes).toBeLessThanOrEqual(64 * 1024);
    expect(replayPermits.snapshot()).toMatchObject({ activePermits: 0 });
  });

  it("validates the complete recovered terminal before exposing any event", async () => {
    const first = new SidecarHarness();
    await first.sendRequest(BODY);
    await first.settle();
    const terminals = new Map(first.storage.terminals);
    const tampered = terminals.get(first.requestId)!.slice();
    tampered[0] = "!".charCodeAt(0);
    terminals.set(first.requestId, tampered);
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals,
    });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.frames.some(frame => frame.kind === "response_event")).toBe(false);
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: first.requestId }),
      expect.objectContaining({
        kind: "response_error",
        requestId: first.requestId,
        code: "terminal_recovery_failed",
        ambiguous: true,
      }),
    ]);
  });

  it("turns a recovered spool read failure into a deterministic committed error", async () => {
    const first = new SidecarHarness();
    await first.sendRequest(BODY);
    await first.settle();
    const fatals: Error[] = [];
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals: first.storage.terminals,
      terminalReadError: new Error("disk unavailable"),
      onFatalError: error => fatals.push(error),
    });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(fatals).toHaveLength(1);
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: first.requestId }),
      expect.objectContaining({
        kind: "response_error",
        code: "terminal_recovery_failed",
        ambiguous: true,
      }),
    ]);
  });

  it("scans recovery without output and preserves proven terminals if delivery aborts", async () => {
    const first = new SidecarHarness();
    await first.sendRequest(BODY);
    await first.settle();
    const backpressure = new OutputBackpressure({
      highFrames: 1,
      highBytes: 1,
      lowFrames: 0,
      lowBytes: 0,
      hardFrames: 2,
      hardBytes: 2,
    });
    backpressure.update({ frames: 1, bytes: 0 });
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals: first.storage.terminals,
      outputBackpressure: backpressure,
    });

    const startedAt = performance.now();
    await restarted.engine.start();

    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(restarted.frames).toEqual([]);
    expect(restarted.engine.snapshot()).toMatchObject({ ready: true, ambiguous: 0 });
    expect(restarted.storage.terminalExistenceChecks).toBeGreaterThan(0);
    expect(restarted.storage.terminalReads).toBe(0);

    const controller = new AbortController();
    const replay = restarted.engine.replayRecovered(controller.signal);
    for (let attempt = 0; attempt < 50 && backpressure.snapshot().waiters === 0; attempt++) {
      await Bun.sleep(1);
    }
    expect(backpressure.snapshot().waiters).toBe(1);
    controller.abort();
    await expect(replay).rejects.toMatchObject({ name: "AbortError" });
    expect(restarted.storage.terminalReads).toBe(1);
    expect(backpressure.snapshot().waiters).toBe(0);
    expect(restarted.engine.snapshot()).toMatchObject({ ambiguous: 0, replaying: 0 });
    expect(restarted.frames.some(frame => frame.kind === "response_error")).toBe(false);
    backpressure.close();
  });

  it("reports unresolved FETCH_STARTING as ambiguous and never replays it", async () => {
    const records: JournalRecord[] = [
      {
        version: 1,
        sequence: 1,
        requestId: "req-ambiguous",
        requestDigest: "a".repeat(64),
        leaderGeneration: TEST_BINDING.leaderGeneration,
        state: "claimed",
        recordedAtMs: 1,
      },
      {
        version: 1,
        sequence: 2,
        requestId: "req-ambiguous",
        requestDigest: "a".repeat(64),
        leaderGeneration: TEST_BINDING.leaderGeneration,
        state: "fetch_starting",
        recordedAtMs: 2,
      },
    ];
    const harness = new SidecarHarness({ records });

    await harness.engine.start();
    await harness.engine.replayRecovered();

    expect(harness.upstream.calls).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId: "req-ambiguous",
      ambiguous: true,
    }));
    expect(harness.frames.findIndex(frame => frame.kind === "request_ack")).toBeLessThan(
      harness.frames.findIndex(frame => frame.kind === "response_error"),
    );
    expect(harness.engine.snapshot().ambiguous).toBe(1);
  });

  it("drains idly and rejects new commits without starting upstream", async () => {
    const harness = new SidecarHarness();
    await harness.engine.start();
    await harness.engine.drain("test");
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 0, queued: 0 });

    await harness.sendRequest(BODY);
    await harness.settle();
    expect(harness.upstream.calls).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "request_reject",
      code: "draining",
    }));
  });

  it("bounds drain and aborts an execution that exceeds the deadline", async () => {
    let aborted = false;
    const harness = new SidecarHarness({
      drainTimeoutMs: 10,
      drainAbortSettlementMs: 10,
      behavior: async input => {
        await input.beforeFetch();
        return new Promise<UpstreamTerminal>((_resolve, reject) => {
          input.signal?.addEventListener("abort", () => {
            aborted = true;
            reject(new UpstreamExecutionError("drain timeout", "timeout", true));
          }, { once: true });
        });
      },
    });
    await harness.sendRequest(BODY);

    const startedAt = performance.now();
    await harness.engine.drain("deadline test");
    await harness.settle();

    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(aborted).toBe(true);
    expect(harness.engine.snapshot()).toMatchObject({
      draining: true,
      active: 0,
      forcedDrain: true,
      ambiguous: 1,
    });
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "forced_drain_cancelled",
      ambiguous: true,
    }));
    expect(harness.frames.some(frame => frame.kind === "request_cancelled")).toBe(false);
    expect(harness.journal.status(harness.requestId)).toBe("forced_ambiguous");

    const restarted = new SidecarHarness({ records: harness.storage.records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: harness.requestId }),
      expect.objectContaining({
        kind: "response_error",
        code: "forced_drain_cancelled",
        ambiguous: true,
      }),
    ]);
  });

  it("reports a non-clean forced drain when upstream ignores cancellation", async () => {
    const harness = new SidecarHarness({
      drainTimeoutMs: 10,
      drainAbortSettlementMs: 10,
      behavior: async input => {
        await input.beforeFetch();
        return new Promise<UpstreamTerminal>(() => undefined);
      },
    });
    await harness.sendRequest(BODY);

    const startedAt = performance.now();
    await harness.engine.drain("forced deadline test");

    expect(performance.now() - startedAt).toBeGreaterThanOrEqual(15);
    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId: harness.requestId,
      code: "forced_drain_cancelled",
      ambiguous: true,
    }));
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 1, queued: 0 });
  });

  it("lets a forced deadline beat an abort-ignoring terminal promotion and restart cleanly", async () => {
    let promotionStarted!: () => void;
    const started = new Promise<void>(resolve => { promotionStarted = resolve; });
    let releasePromotion!: () => void;
    const promotionGate = new Promise<void>(resolve => { releasePromotion = resolve; });
    const harness = new SidecarHarness({
      drainTimeoutMs: 20,
      drainAbortSettlementMs: 20,
      beforePromoteTerminalSpool: async () => {
        promotionStarted();
        await promotionGate;
      },
      // completedUpstream deliberately ignores an abort after its terminal has
      // been assembled, reproducing the terminal-vs-forced journal race.
      behavior: input => completedUpstream(input),
    });
    await harness.sendRequest(BODY);
    await started;

    const forcedMark = spyOn(harness.journal, "markForcedAmbiguous");
    try {
      const draining = harness.engine.drain("terminal promotion race");
      const deadline = performance.now() + 500;
      while (forcedMark.mock.calls.length === 0) {
        if (performance.now() >= deadline) {
          throw new Error("forced drain did not install its journal intent");
        }
        await Bun.sleep(0);
      }
      // markForcedAmbiguous installs its intent synchronously before its first
      // await. Release the competing promotion only after that linearization
      // point instead of racing two chains of wall-clock timers.
      releasePromotion();
      const forcedResult = forcedMark.mock.results[0]?.value;
      if (!(forcedResult instanceof Promise)) {
        throw new Error("forced journal transition did not return a promise");
      }
      await forcedResult;
      await draining;
    } finally {
      releasePromotion();
      forcedMark.mockRestore();
    }
    await harness.settle();

    expect(harness.journal.status(harness.requestId)).toBe("forced_ambiguous");
    expect(harness.storage.records.some(record => record.state === "terminal")).toBe(false);
    expect(harness.storage.records.some(record => record.state === "provider_error")).toBe(false);
    expect(harness.storage.terminals.has(harness.requestId)).toBe(false);

    const restarted = new SidecarHarness({ records: harness.storage.records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();
    expect(restarted.frames).toEqual([
      expect.objectContaining({ kind: "request_ack", requestId: harness.requestId }),
      expect.objectContaining({
        kind: "response_error",
        requestId: harness.requestId,
        code: "forced_drain_cancelled",
        ambiguous: true,
      }),
    ]);
  });

  it("bounds a forced outcome blocked behind a non-settling terminal promotion", async () => {
    let promotionStarted!: () => void;
    const started = new Promise<void>(resolve => { promotionStarted = resolve; });
    const fatal: Error[] = [];
    const harness = new SidecarHarness({
      drainTimeoutMs: 5,
      drainAbortSettlementMs: 5,
      onFatalError: error => { fatal.push(error); },
      beforePromoteTerminalSpool: async () => {
        promotionStarted();
        await new Promise<void>(() => undefined);
      },
      behavior: input => completedUpstream(input),
    });
    await harness.sendRequest(BODY);
    await started;

    const startedAt = performance.now();
    await harness.engine.drain("non-settling terminal promotion");
    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(fatal.some(error => /forced-drain.*timed out/i.test(error.message))).toBe(true);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId: harness.requestId,
      code: "forced_drain_unproven",
      ambiguous: true,
    }));
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 1, queued: 0 });
  });

  it("suppresses a forced error when a pre-deadline terminal append wins", async () => {
    let appends = 0;
    let terminalAppendStarted!: () => void;
    const terminalAppend = new Promise<void>(resolve => { terminalAppendStarted = resolve; });
    let releaseTerminalAppend!: () => void;
    const terminalGate = new Promise<void>(resolve => { releaseTerminalAppend = resolve; });
    const harness = new SidecarHarness({
      drainTimeoutMs: 20,
      drainAbortSettlementMs: 20,
      beforeAppend: async () => {
        appends++;
        if (appends !== 3) return;
        terminalAppendStarted();
        await terminalGate;
      },
      behavior: input => completedUpstream(input),
    });
    await harness.sendRequest(BODY);
    await terminalAppend;

    const draining = harness.engine.drain("terminal append linearization");
    await Bun.sleep(45);
    releaseTerminalAppend();
    await draining;
    await harness.settle();

    expect(harness.journal.status(harness.requestId)).toBe("terminal");
    expect(harness.storage.records.some(record => record.state === "forced_ambiguous")).toBe(false);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_terminal",
      requestId: harness.requestId,
    }));
    expect(harness.frames.some(frame => (
      frame.kind === "response_error" &&
      (frame.code === "forced_drain_cancelled" || frame.code === "forced_drain_unproven")
    ))).toBe(false);

    const restarted = new SidecarHarness({
      records: harness.storage.records,
      terminals: harness.storage.terminals,
    });
    await restarted.engine.start();
    expect((await restarted.journal.recover()).terminals).toEqual([harness.requestId]);
  });

  it("does not emit a late terminal after an unproven forced-drain timeout", async () => {
    let appends = 0;
    let terminalAppendStarted!: () => void;
    const terminalAppend = new Promise<void>(resolve => { terminalAppendStarted = resolve; });
    let releaseTerminalAppend!: () => void;
    const terminalGate = new Promise<void>(resolve => { releaseTerminalAppend = resolve; });
    const harness = new SidecarHarness({
      drainTimeoutMs: 5,
      drainAbortSettlementMs: 5,
      beforeAppend: async () => {
        appends++;
        if (appends !== 3) return;
        terminalAppendStarted();
        await terminalGate;
      },
      behavior: input => completedUpstream(input),
    });
    await harness.sendRequest(BODY);
    await terminalAppend;

    await harness.engine.drain("late terminal after forced timeout");
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "forced_drain_unproven",
    }));
    releaseTerminalAppend();
    await harness.settle();

    expect(harness.journal.status(harness.requestId)).toBe("terminal");
    expect(harness.frames.some(frame => frame.kind === "response_terminal")).toBe(false);
    const restarted = new SidecarHarness({
      records: harness.storage.records,
      terminals: harness.storage.terminals,
    });
    await restarted.engine.start();
    expect((await restarted.journal.recover()).terminals).toEqual([harness.requestId]);
  });

  it("never reports a clean drain while a pre-fetch commit is still settling", async () => {
    let appendStarted!: () => void;
    const started = new Promise<void>(resolve => { appendStarted = resolve; });
    let releaseAppend!: () => void;
    const appendBlocked = new Promise<void>(resolve => { releaseAppend = resolve; });
    let firstAppend = true;
    const harness = new SidecarHarness({
      drainTimeoutMs: 10,
      drainAbortSettlementMs: 10,
      beforeAppend: async () => {
        if (!firstAppend) return;
        firstAppend = false;
        appendStarted();
        await appendBlocked;
      },
    });
    const sending = harness.sendRequest(BODY);
    await started;

    await harness.engine.drain("commit deadline test");

    expect(harness.engine.snapshot()).toMatchObject({ forcedDrain: true, active: 0 });
    expect(harness.frames.some(frame => frame.kind === "request_ack")).toBe(false);
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 1, queued: 0 });
    expect(harness.upstream.calls).toBe(0);

    releaseAppend();
    await sending;
    expect(harness.upstream.calls).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "request_reject",
      code: "draining",
    }));
  });

  it("applies the host admission ceiling without exceeding the physical limit", async () => {
    const harness = new SidecarHarness();

    harness.engine.setHostCeiling(17);

    expect(harness.admission.hostCeilings).toEqual([17]);
    expect(harness.engine.snapshot().credits).toBe(17);
  });

  it("notifies the process lifecycle once when a host drain frame arrives", async () => {
    const reasons: string[] = [];
    const harness = new SidecarHarness({
      onDrainRequested: reason => reasons.push(reason),
    });
    await harness.engine.start();

    await harness.engine.accept({
      kind: "drain",
      reason: "fleet shutdown",
      epoch: 1,
      sequence: 0,
    });
    await harness.engine.accept({
      kind: "drain",
      reason: "duplicate",
      epoch: 1,
      sequence: 1,
    });

    expect(reasons).toEqual(["fleet shutdown"]);
  });

  it("holds an R2 body behind prepared until an exact request_start", async () => {
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING });
    const start = await harness.prepareRequest(BODY);

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot()).toMatchObject({ active: 1, credits: 127 });
    expect(harness.frames.filter(frame => frame.kind === "request_prepared")).toHaveLength(1);
    expect(harness.frames.some(frame => frame.kind === "request_ack")).toBe(false);
    expect(harness.storage.records.map(record => record.state)).toEqual(["claimed"]);

    await harness.startPrepared(start);
    await harness.settle();

    expect(harness.upstream.calls).toBe(1);
    expect(harness.storage.records.map(record => record.state)).toContain("fetch_starting");
    expect(harness.frames.filter(frame => frame.kind === "request_ack")).toHaveLength(1);
    expect(harness.timeline.indexOf("emit:request_prepared")).toBeLessThan(
      harness.timeline.indexOf("sync:fetch_starting"),
    );
    expect(harness.timeline.indexOf("sync:fetch_starting")).toBeLessThan(
      harness.timeline.indexOf("emit:request_ack"),
    );
  });

  it("deduplicates R2 commit and start without a second fetch", async () => {
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING });
    const start = await harness.prepareRequest(BODY);
    await harness.engine.accept({
      ...start,
      kind: "request_commit",
      epoch: 1,
      sequence: 4,
    });
    expect(harness.frames.filter(frame => frame.kind === "request_prepared")).toHaveLength(1);
    expect(harness.upstream.calls).toBe(0);

    await Promise.all([
      harness.startPrepared(start),
      harness.engine.accept({ ...start, epoch: 1, sequence: 5 }),
    ]);
    await harness.settle();
    expect(harness.upstream.calls).toBe(1);
    expect(harness.storage.records.filter(record => record.state === "fetch_starting"))
      .toHaveLength(1);
  });

  it("rejects a mismatched R2 start without consuming prepared work", async () => {
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING });
    const start = await harness.prepareRequest(BODY);
    await harness.startPrepared({ ...start, requestDigest: "e".repeat(64) });

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot().active).toBe(1);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "request_reject",
      code: "start_conflict",
    }));

    await harness.startPrepared(start);
    await harness.settle();
    expect(harness.upstream.calls).toBe(1);
  });

  it("never fetches an R2 request_start that has no prepared body", async () => {
    const source = new SidecarHarness({ binding: TEST_R2_BINDING });
    const start = await source.prepareRequest(BODY);
    const empty = new SidecarHarness({ binding: TEST_R2_BINDING });
    await empty.engine.start();

    await empty.startPrepared(start);

    expect(empty.upstream.calls).toBe(0);
    expect(empty.frames).toContainEqual(expect.objectContaining({
      kind: "request_reject",
      code: "not_prepared",
    }));
    await source.engine.drain("test cleanup");
  });

  it("cancels R2 prepared work and releases its bounded admission lease", async () => {
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING });
    await harness.prepareRequest(BODY);
    await harness.engine.accept({
      kind: "request_cancel",
      requestId: harness.requestId,
      reason: "client aborted",
      epoch: 1,
      sequence: 4,
    });

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot().active).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.frames.filter(frame => frame.kind === "request_cancelled")).toHaveLength(1);
  });

  it("drains R2 prepared work immediately without crossing FETCH_STARTING", async () => {
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING });
    await harness.prepareRequest(BODY);

    await harness.engine.drain("sidecar shutdown");

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot().active).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.storage.records.some(record => record.state === "fetch_starting"))
      .toBe(false);
    expect(harness.journal.status(harness.requestId)).toBe("cancelled");
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "draining",
      ambiguous: false,
    }));
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 0, queued: 0 });
  });

  it("bounds drain when prepared cancellation storage never settles", async () => {
    let appends = 0;
    const harness = new SidecarHarness({
      binding: TEST_R2_BINDING,
      drainTimeoutMs: 5,
      drainAbortSettlementMs: 5,
      beforeAppend: async () => {
        if (++appends === 2) await new Promise<void>(() => undefined);
      },
    });
    await harness.prepareRequest(BODY);

    const startedAt = performance.now();
    await harness.engine.drain("blocked prepared cleanup");
    expect(performance.now() - startedAt).toBeLessThan(250);
    expect(harness.upstream.calls).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.engine.snapshot()).toMatchObject({
      active: 1,
      draining: true,
      forcedDrain: true,
    });
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 1, queued: 0 });
    expect(harness.storage.records.some(record => record.state === "fetch_starting")).toBe(false);
  });

  it("never reports a clean drain when prepared cancellation durability rejects", async () => {
    let appends = 0;
    const fatal: Error[] = [];
    const harness = new SidecarHarness({
      binding: TEST_R2_BINDING,
      drainTimeoutMs: 5,
      drainAbortSettlementMs: 5,
      onFatalError: error => { fatal.push(error); },
      beforeAppend: async () => {
        if (++appends === 2) throw new Error("synthetic prepared cancellation failure");
      },
    });
    await harness.prepareRequest(BODY);
    await harness.engine.drain("rejecting prepared cleanup");

    expect(fatal).toHaveLength(1);
    expect(harness.engine.snapshot()).toMatchObject({
      active: 1,
      draining: true,
      forcedDrain: true,
    });
    expect(harness.frames).not.toContainEqual({ kind: "drained", active: 0, queued: 0 });
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 1, queued: 0 });
    expect(harness.journal.status(harness.requestId)).toBe("claimed");
    expect(harness.upstream.calls).toBe(0);
  });

  it("cannot insert R2 prepared work behind a concurrent drain sweep", async () => {
    let beginReached!: () => void;
    const reached = new Promise<void>(resolve => { beginReached = resolve; });
    let releaseBegin!: () => void;
    const blocked = new Promise<void>(resolve => { releaseBegin = resolve; });
    const harness = new SidecarHarness({
      binding: TEST_R2_BINDING,
      beforeBeginTerminalSpool: async () => {
        beginReached();
        await blocked;
      },
    });
    const preparing = harness.prepareRequest(BODY);
    await reached;
    const draining = harness.engine.drain("race shutdown");
    releaseBegin();
    await Promise.all([preparing, draining]);

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot().active).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.frames.some(frame => frame.kind === "request_prepared")).toBe(false);
    expect(harness.frames.at(-1)).toEqual({ kind: "drained", active: 0, queued: 0 });
  });

  it("expires lost R2 starts and releases every prepared resource", async () => {
    const harness = new SidecarHarness({
      binding: TEST_R2_BINDING,
      preparedTimeoutMs: 5,
    });
    await harness.prepareRequest(BODY);
    for (
      let attempt = 0;
      attempt < 50 && !harness.frames.some(frame => (
        frame.kind === "response_error" && frame.code === "prepared_expired"
      ));
      attempt++
    ) {
      await Bun.sleep(2);
    }

    expect(harness.upstream.calls).toBe(0);
    expect(harness.engine.snapshot().active).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      code: "prepared_expired",
      ambiguous: false,
    }));
    expect(harness.storage.records.map(record => record.state).at(-1)).toBe("cancelled");
    await harness.engine.acknowledgeDeliveredOutcomes([harness.requestId]);
    expect(harness.journal.status(harness.requestId)).toBe("acked");
  });

  it("rejects R2 spool exhaustion before prepared or fetch and cleans the claim", async () => {
    const harness = new SidecarHarness({
      binding: TEST_R2_BINDING,
      terminalSpoolError: new JournalStorageCapacityError("spool full"),
    });
    await harness.prepareRequest(BODY);

    expect(harness.upstream.calls).toBe(0);
    expect(harness.admission.active).toBe(0);
    expect(harness.frames.some(frame => frame.kind === "request_prepared")).toBe(false);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "request_reject",
      code: "capacity",
    }));
    expect(harness.storage.records.map(record => record.state))
      .toEqual(["claimed", "cancelled", "acked"]);
  });

  it("retires recovered R2 CLAIMED work without fetching or leaking quota", async () => {
    const first = new SidecarHarness({ binding: TEST_R2_BINDING });
    await first.prepareRequest(BODY);
    const records = first.storage.records.map(record => ({ ...record }));

    const restarted = new SidecarHarness({ binding: TEST_R2_BINDING, records });
    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.journal.status(first.requestId)).toBe("cancelled");
    expect(restarted.frames).toContainEqual({
      kind: "request_cancelled",
      requestId: first.requestId,
    });
    await restarted.engine.acknowledgeDeliveredOutcomes([first.requestId]);
    expect(restarted.journal.status(first.requestId)).toBe("acked");
  });

  it("reports recovered R2 FETCH_STARTING without ever refetching", async () => {
    const requestId = "req-r2-fetch-starting";
    const digest = "9".repeat(64);
    const records: JournalRecord[] = [
      {
        version: 1,
        sequence: 1,
        requestId,
        requestDigest: digest,
        leaderGeneration: TEST_R2_BINDING.leaderGeneration,
        state: "claimed",
        recordedAtMs: 1,
      },
      {
        version: 1,
        sequence: 2,
        requestId,
        requestDigest: digest,
        leaderGeneration: TEST_R2_BINDING.leaderGeneration,
        state: "fetch_starting",
        recordedAtMs: 2,
      },
    ];
    const harness = new SidecarHarness({ binding: TEST_R2_BINDING, records });
    await harness.engine.start();
    await harness.engine.replayRecovered();

    expect(harness.upstream.calls).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId,
      code: "recovered_ambiguous",
      ambiguous: true,
    }));
  });
});
