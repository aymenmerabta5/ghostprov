import { describe, expect, it } from "bun:test";
import type { HostFrame, ReliableFrame, SidecarFrame } from "../src/v2/protocol/shared";
import type { JournalRecord } from "../src/v2/sidecar/journal";
import { OutputBackpressure } from "../src/v2/sidecar/output-sink";
import {
  UpstreamExecutionError,
  type DirectResponsesExecution,
  type UpstreamTerminal,
} from "../src/v2/sidecar/upstream";
import {
  SidecarHarness,
  TEST_BINDING,
  completedUpstream,
} from "./support/fake-v2-sidecar";

const BODY = { model: "gpt-5.6-sol", input: "engine", stream: true };

describe("SidecarEngine", () => {
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
    const batchIndex = harness.frames.findIndex(frame => frame.kind === "response_batch");
    const terminalIndex = harness.frames.findIndex(frame => frame.kind === "response_terminal");
    expect(batchIndex).toBeGreaterThanOrEqual(0);
    expect(terminalIndex).toBeGreaterThan(batchIndex);
  });
  it("ACKs only after durable claim and permit, then never retries after fetch start", async () => {
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

  it("persists terminal before publishing and deletes it only after matching host ACK", async () => {
    const harness = new SidecarHarness();
    await harness.sendRequest(BODY);
    await harness.settle();

    expect(harness.timeline.indexOf("terminal-spool")).toBeLessThan(
      harness.timeline.indexOf("sync:terminal"),
    );
    expect(harness.timeline.indexOf("sync:terminal")).toBeLessThan(
      harness.timeline.indexOf("emit:response_terminal"),
    );
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
    const first = new SidecarHarness();
    await first.sendRequest(BODY);
    await first.settle();
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals: first.storage.terminals,
    });

    await restarted.engine.start();
    await restarted.engine.replayRecovered();

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toContainEqual(expect.objectContaining({
      kind: "response_terminal",
      responseId: "resp_1",
    }));
    expect(restarted.frames.filter(frame => frame.kind === "response_event").length).toBeGreaterThan(0);
    expect(restarted.frames.findIndex(frame => frame.kind === "request_ack")).toBeLessThan(
      restarted.frames.findIndex(frame => frame.kind === "response_event"),
    );
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
});
