import { describe, expect, it } from "bun:test";
import type { AwarenessChannel } from "../src/v2/awareness/channel";
import { projectId } from "../src/sandbox/types";
import {
  GP_V2_R2_SCHEMA_VERSION,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type R2RequestCommitFrame,
  type R2RequestPreparedFrame,
  type ReliableFrame,
  type SidecarFrame,
} from "../src/v2/protocol/shared";
import {
  V2CommittedError,
  V2PreCommitError,
} from "../src/v2/transport/run-handle";
import { V2TransportSession } from "../src/v2/transport/session";
import { FakeAwarenessNetwork } from "./support/fake-awareness";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const HOST = "host-r2-handshake";
const SIDECAR = "sidecar-r2-handshake";
const BINDING: ProtocolBinding = {
  protocolRevision: "r2",
  projectId: PROJECT,
  sessionId: "sandbox-session-r2-handshake",
  leaderGeneration: 21,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  launchNonce: "launch-r2-handshake",
};
const BODY = { model: "gpt-5.6-sol", input: "two phase", stream: true };

async function promiseState(value: Promise<unknown>): Promise<"pending" | "settled"> {
  return Promise.race([
    value.then(() => "settled" as const, () => "settled" as const),
    new Promise<"pending">(resolve => queueMicrotask(() => resolve("pending"))),
  ]);
}

function withThrowingStart(
  channel: AwarenessChannel<HostFrame, SidecarFrame>,
): AwarenessChannel<HostFrame, SidecarFrame> {
  return {
    get projectId() { return channel.projectId; },
    get localPeerId() { return channel.localPeerId; },
    connect: signal => channel.connect(signal),
    publish(envelope, lane) {
      if (lane === "data" && envelope.frames.some(frame => frame.kind === "request_start")) {
        throw new Error("synthetic request_start publication failure");
      }
      channel.publish(envelope, lane);
    },
    clear: lane => channel.clear(lane),
    subscribe: listener => channel.subscribe(listener),
    peerIds: () => channel.peerIds(),
    close: () => channel.close(),
  };
}

class R2Harness {
  readonly network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>();
  readonly sidecar = this.network.connectSidecar("sidecar", PROJECT);
  readonly session: V2TransportSession;
  readonly hostFrames = new Map<number, ReliableFrame<HostFrame>>();
  readonly hostEnvelopes: AwarenessEnvelope<HostFrame>[] = [];
  readonly sidecarOutstanding: ReliableFrame<SidecarFrame>[] = [];
  hostAck = -1;
  nextHostSequence = 0;
  sidecarSequence = 0;
  dataRevision = 0;
  controlRevision = 0;

  constructor(options: { throwOnStart?: boolean } = {}) {
    const base = this.network.connectHost("host", PROJECT);
    const host = options.throwOnStart ? withThrowingStart(base) : base;
    this.sidecar.subscribe(update => {
      this.hostEnvelopes.push(update.envelope);
      if ((update.envelope.lane ?? "data") !== "data") return;
      for (const frame of update.envelope.frames) this.hostFrames.set(frame.sequence, frame);
      while (this.hostFrames.has(this.nextHostSequence)) {
        this.hostAck = this.nextHostSequence++;
      }
    });
    this.session = new V2TransportSession({
      binding: BINDING,
      channel: host,
      hostInstanceId: HOST,
      expectedSidecarInstanceId: SIDECAR,
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 10_000,
    });
  }

  async start(): Promise<void> {
    await this.sidecar.connect();
    await this.session.connect();
    this.publish([], { lane: "control" });
    await this.settle();
  }

  publish(
    frames: readonly SidecarFrame[],
    options: { lane?: "data" | "control"; ackSequence?: number } = {},
  ): void {
    for (const frame of frames) {
      this.sidecarOutstanding.push({
        ...frame,
        epoch: 1,
        sequence: this.sidecarSequence++,
      } as ReliableFrame<SidecarFrame>);
    }
    const lane = options.lane ?? "data";
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      schemaHash: BINDING.schemaHash,
      buildHash: BINDING.buildHash,
      projectId: PROJECT,
      sessionId: BINDING.sessionId,
      peerId: "sidecar",
      instanceId: SIDECAR,
      targetInstanceId: HOST,
      launchNonce: BINDING.launchNonce,
      leaderGeneration: BINDING.leaderGeneration,
      epoch: 1,
      revision: lane === "data" ? this.dataRevision++ : this.controlRevision++,
      ack: { epoch: 1, sequence: options.ackSequence ?? this.hostAck },
      credits: 4,
      active: 0,
      queued: 0,
      draining: false,
      frames: lane === "data" ? [...this.sidecarOutstanding] : [],
      lane,
    };
    this.sidecar.publish(envelope, lane);
  }

  commit(): ReliableFrame<R2RequestCommitFrame> {
    const frame = [...this.hostFrames.values()].find(
      (candidate): candidate is ReliableFrame<R2RequestCommitFrame> =>
        candidate.kind === "request_commit" && "protocolRevision" in candidate,
    );
    if (!frame) throw new Error("R2 request_commit was not published");
    return frame;
  }

  prepared(overrides: Partial<R2RequestPreparedFrame> = {}): R2RequestPreparedFrame {
    const commit = this.commit();
    return {
      kind: "request_prepared",
      requestId: commit.requestId,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      projectId: commit.projectId,
      sessionId: commit.sessionId,
      leaderGeneration: commit.leaderGeneration,
      buildHash: commit.buildHash,
      schemaHash: commit.schemaHash,
      launchNonce: commit.launchNonce,
      hostInstanceId: commit.hostInstanceId,
      targetInstanceId: commit.targetInstanceId,
      requestDigest: commit.requestDigest,
      decodedBytes: commit.decodedBytes,
      encodedBytes: commit.encodedBytes,
      encoding: commit.encoding,
      partCount: commit.partCount,
      ...overrides,
    };
  }

  starts(): ReliableFrame<Extract<HostFrame, { kind: "request_start" }>>[] {
    return [...this.hostFrames.values()].filter(
      (frame): frame is ReliableFrame<Extract<HostFrame, { kind: "request_start" }>> =>
        frame.kind === "request_start",
    );
  }

  async settle(rounds = 12): Promise<void> {
    await Bun.sleep(2);
    for (let index = 0; index < rounds; index++) {
      await this.network.flushAll();
      await Bun.sleep(0);
    }
  }

  async close(): Promise<void> {
    await this.session.close();
    await this.sidecar.close();
  }
}

describe("R2 prepared/start host boundary", () => {
  it("reports admission before execution risk and reports start risk exactly once", async () => {
    const harness = new R2Harness();
    await harness.start();
    let admissions = 0;
    let executionRisks = 0;
    const pending = harness.session.run(BODY, undefined, {
      onAdmissionReflected: () => { admissions++; },
      onExecutionRiskReflected: () => { executionRisks++; },
    });
    await harness.settle();

    expect(admissions).toBe(1);
    expect(executionRisks).toBe(0);
    const prepared = harness.prepared();
    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);
    expect(executionRisks).toBe(1);

    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);
    expect(executionRisks).toBe(1);
    harness.publish([{
      kind: "request_ack",
      requestId: prepared.requestId,
      requestDigest: prepared.requestDigest,
      committed: true,
    }]);
    await harness.settle();
    await expect(pending).resolves.toBeDefined();
    await harness.close();
  });

  it("keeps a published network commit safely precommit until prepared", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    expect(harness.commit()).toBeDefined();
    expect(await promiseState(pending)).toBe("pending");

    await harness.session.close();
    await expect(pending).rejects.toBeInstanceOf(V2PreCommitError);
    await harness.sidecar.close();
  });

  it("queues one exact start for duplicate prepared proofs and resolves only on request_ack", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    const prepared = harness.prepared();

    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);
    expect(await promiseState(pending)).toBe("pending");

    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);

    harness.publish([{
      kind: "request_ack",
      requestId: prepared.requestId,
      requestDigest: prepared.requestDigest,
      committed: true,
    }]);
    await harness.settle();
    await expect(pending).resolves.toBeDefined();
    await harness.close();
  });

  it("replays an R2 processed ACK when the sidecar retransmits a prepared flight", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    const prepared = harness.prepared();

    harness.publish([prepared]);
    await harness.settle();
    const preparedSequence = harness.sidecarOutstanding[0]!.sequence;
    expect(harness.starts()).toHaveLength(1);
    expect(harness.hostEnvelopes.some(envelope => (
      envelope.ack.sequence >= preparedSequence
    ))).toBe(true);
    harness.hostEnvelopes.length = 0;

    // Re-publish the same immutable outstanding sequence as if the first host
    // ACK/control update was lost before it reached the sidecar.
    harness.publish([]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);
    expect(harness.hostEnvelopes).toHaveLength(1);
    expect(harness.hostEnvelopes[0]).toMatchObject({
      lane: "control",
      ack: { epoch: 1, sequence: preparedSequence },
      frames: [],
    });

    await harness.session.close();
    await expect(pending).rejects.toBeInstanceOf(V2CommittedError);
    await harness.sidecar.close();
  });

  it("can retract a merely queued start on abort without quarantine", async () => {
    const harness = new R2Harness();
    await harness.start();
    const controller = new AbortController();
    const pending = harness.session.run(BODY, controller.signal);
    await harness.settle();

    // Do not ACK the active commit flight, so request_start remains only in the
    // local scheduler and has not entered the cumulative outbound window.
    harness.publish([harness.prepared()], { ackSequence: -1 });
    await harness.settle();
    expect(harness.starts()).toHaveLength(0);
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(harness.session.snapshot().quarantined).toBe(0);
    await harness.close();
  });

  it("treats a throwing start publication as committed ambiguity after window staging", async () => {
    const harness = new R2Harness({ throwOnStart: true });
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    harness.publish([harness.prepared()]);
    await harness.settle();

    expect(await failed).toBeInstanceOf(V2CommittedError);
    expect(harness.session.snapshot().quarantined).toBe(1);
    await harness.close();
  });

  it("rejects a mismatched prepared proof before start-risk", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    harness.publish([harness.prepared({ requestDigest: "f".repeat(64) })]);
    await harness.settle();

    expect(await failed).toBeInstanceOf(V2PreCommitError);
    expect(harness.starts()).toHaveLength(0);
    expect(harness.session.snapshot().quarantined).toBe(0);
    await harness.close();
  });

  it("fails request-local upstream evidence before start as precommit protocol error", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    const commit = harness.commit();
    harness.publish([{
      kind: "upstream_prelude",
      requestId: commit.requestId,
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: null,
    }]);
    await harness.settle();

    expect(await failed).toBeInstanceOf(V2PreCommitError);
    await harness.close();
  });

  it("treats request_ack before start as committed provider risk", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    const commit = harness.commit();
    harness.publish([{
      kind: "request_ack",
      requestId: commit.requestId,
      requestDigest: commit.requestDigest,
      committed: true,
    }]);
    await harness.settle();

    expect(await failed).toBeInstanceOf(V2CommittedError);
    expect(harness.session.snapshot().quarantined).toBe(1);
    await harness.close();
  });

  it("accepts strict UTF-8 event-stream media-type equivalence during recovery replay", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    harness.publish([harness.prepared()]);
    await harness.settle();
    const commit = harness.commit();
    harness.publish([{
      kind: "request_ack",
      requestId: commit.requestId,
      requestDigest: commit.requestDigest,
      committed: true,
    }, {
      kind: "upstream_prelude",
      requestId: commit.requestId,
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_equivalent",
    }, {
      kind: "upstream_prelude",
      requestId: commit.requestId,
      status: 200,
      contentType: "Text/Event-Stream; Charset=\"UTF-8\"",
      upstreamRequestId: "req_equivalent",
    }]);
    await harness.settle();

    const handle = await pending;
    await expect(handle.upstreamPrelude).resolves.toEqual({
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_equivalent",
    });
    expect(harness.session.snapshot().connected).toBe(true);
    await harness.close();
    await Promise.allSettled([handle.final]);
  });

  it("releases cancellation quarantine when a definitive outcome wins the race", async () => {
    const harness = new R2Harness();
    await harness.start();
    const controller = new AbortController();
    const pending = harness.session.run(BODY, controller.signal);
    const failed = pending.catch(error => error);
    await harness.settle();
    const prepared = harness.prepared();
    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);

    controller.abort();
    expect(await failed).toBeInstanceOf(V2CommittedError);
    expect(harness.session.snapshot().quarantined).toBe(1);

    harness.publish([{
      kind: "response_error",
      requestId: prepared.requestId,
      code: "provider_rejected",
      message: "Provider definitively rejected the request",
      ambiguous: false,
    }]);
    await harness.settle();
    expect(harness.session.snapshot().quarantined).toBe(0);
    expect(harness.session.snapshot().admissionCredits).toBeGreaterThan(0);
    await harness.close();
  });

  it("force-stops active output by publishing cancel plus drain before local close", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    const prepared = harness.prepared();
    harness.publish([prepared]);
    await harness.settle();
    expect(harness.starts()).toHaveLength(1);

    const stopping = harness.session.forceStop("supervisor recovery");
    // ACK the start flight so the already-queued cancellation/drain controls
    // can enter the next immutable host flight.
    harness.publish([]);
    await harness.settle();
    const kinds = [...harness.hostFrames.values()].map(frame => frame.kind);
    expect(kinds).toContain("request_cancel");
    expect(kinds).toContain("drain");

    // Output already in flight must still be processed/ACKed even though its
    // local request state was failed by recovery.
    harness.publish([{
      kind: "response_event",
      requestId: prepared.requestId,
      eventIndex: 0,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: "",
    }]);
    await harness.settle();
    harness.publish([{ kind: "drained", active: 0, queued: 0 }]);
    await harness.settle();

    await expect(stopping).resolves.toBeUndefined();
    expect(await failed).toBeInstanceOf(V2CommittedError);
    expect(harness.session.snapshot().connected).toBe(false);
    await harness.sidecar.close();
  });

  it("force-stops prepared work before start without creating execution risk", async () => {
    const harness = new R2Harness();
    await harness.start();
    const pending = harness.session.run(BODY);
    const failed = pending.catch(error => error);
    await harness.settle();
    // Hold the commit flight unacknowledged: prepared is proven, while start is
    // still retractable in the local scheduler.
    harness.publish([harness.prepared()], { ackSequence: -1 });
    await harness.settle();
    expect(harness.starts()).toHaveLength(0);

    const stopping = harness.session.forceStop("prepared orphan recovery");
    harness.publish([]);
    await harness.settle();
    expect([...harness.hostFrames.values()].map(frame => frame.kind))
      .toEqual(expect.arrayContaining(["request_cancel", "drain"]));
    harness.publish([{ kind: "drained", active: 0, queued: 0 }]);
    await harness.settle();

    await stopping;
    expect(await failed).toBeInstanceOf(V2PreCommitError);
    expect(harness.starts()).toHaveLength(0);
    await harness.sidecar.close();
  });
});
