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

    expect(restarted.upstream.calls).toBe(0);
    expect(restarted.frames).toContainEqual(expect.objectContaining({
      kind: "response_terminal",
      responseId: "resp_1",
    }));
    expect(restarted.frames.filter(frame => frame.kind === "response_event").length).toBeGreaterThan(0);
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

    expect(harness.upstream.calls).toBe(0);
    expect(harness.frames).toContainEqual(expect.objectContaining({
      kind: "response_error",
      requestId: "req-ambiguous",
      ambiguous: true,
    }));
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
