import { describe, expect, it, spyOn } from "bun:test";
import { gzipSync } from "fflate";
import type { RunHandle } from "../src/relay/run-handle";
import { projectId } from "../src/sandbox/types";
import type { AwarenessChannel } from "../src/v2/awareness/channel";
import {
  base64UrlEncode,
  utf8Bytes,
  type AwarenessEnvelope,
  type AwarenessLiveness,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../src/v2/protocol/shared";
import { TranscriptProof } from "../src/v2/protocol/transcript";
import {
  V2CommittedError,
  V2PreCommitError,
} from "../src/v2/transport/run-handle";
import {
  computeV2RetransmitDelayMs,
  V2TransportSession,
  type V2GzipOptions,
} from "../src/v2/transport/session";
import { FakeAwarenessNetwork } from "./support/fake-awareness";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BINDING: ProtocolBinding = {
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};
const BODY = { model: "gpt-5.6-sol", input: "hello", stream: true };

async function promiseState(value: Promise<unknown>): Promise<"pending" | "settled"> {
  return Promise.race([
    value.then(() => "settled" as const, () => "settled" as const),
    new Promise<"pending">(resolve => queueMicrotask(() => resolve("pending"))),
  ]);
}

function throwOnSelectedPublish(
  channel: AwarenessChannel<HostFrame, SidecarFrame>,
  options: { data: boolean; commit: boolean; liveness: boolean },
): AwarenessChannel<HostFrame, SidecarFrame> {
  return {
    get projectId() { return channel.projectId; },
    get localPeerId() { return channel.localPeerId; },
    connect: signal => channel.connect(signal),
    publish(envelope, lane) {
      if (options.data && lane === "data") {
        throw new Error("synthetic data publication failure");
      }
      if (
        options.commit &&
        lane === "data" &&
        envelope.frames.some(frame => frame.kind === "request_commit")
      ) {
        throw new Error("synthetic commit publication failure");
      }
      if (options.liveness && envelope.liveness?.kind === "ping") {
        throw new Error("synthetic liveness publication failure");
      }
      channel.publish(envelope, lane);
    },
    clear: lane => channel.clear(lane),
    subscribe: listener => channel.subscribe(listener),
    peerIds: () => channel.peerIds(),
    close: () => channel.close(),
  };
}

class TransportHarness {
  readonly network: FakeAwarenessNetwork<HostFrame, SidecarFrame>;
  readonly host: AwarenessChannel<HostFrame, SidecarFrame>;
  readonly sidecar: AwarenessChannel<SidecarFrame, HostFrame>;
  readonly session: V2TransportSession;
  readonly hostFrames = new Map<number, ReliableFrame<HostFrame>>();
  readonly hostEnvelopes: AwarenessEnvelope<HostFrame>[] = [];
  readonly sidecarOutstanding: ReliableFrame<SidecarFrame>[] = [];
  revision = 0;
  sidecarSequence = 0;
  sidecarEpoch = 1;
  sidecarInstanceId = "sidecar-instance-1";
  nextHostSequence = 0;
  hostAck = -1;
  credits = 128;
  readonly publicationIntervalMs: number;
  readonly autoPong: boolean;

  constructor(options: {
    drop?: readonly number[];
    retransmitMs?: number;
    maxResponseBytes?: number;
    maxBufferedResponseBytes?: number;
    gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
    rotationTimeoutMs?: number;
    publicationIntervalMs?: number;
    unadmittedTimeoutMs?: number;
    livenessTimeoutMs?: number;
    autoPong?: boolean;
    throwOnDataPublish?: boolean;
    throwOnCommitPublish?: boolean;
    throwOnLivenessPublish?: boolean;
    expectedSidecarInstanceId?: string | null;
  } = {}) {
    this.publicationIntervalMs = options.publicationIntervalMs ?? 1;
    this.autoPong = options.autoPong ?? false;
    this.network = new FakeAwarenessNetwork({ drop: options.drop });
    const host = this.network.connectHost("host", PROJECT);
    this.host = options.throwOnDataPublish ||
      options.throwOnCommitPublish ||
      options.throwOnLivenessPublish
      ? throwOnSelectedPublish(host, {
          data: options.throwOnDataPublish ?? false,
          commit: options.throwOnCommitPublish ?? false,
          liveness: options.throwOnLivenessPublish ?? false,
        })
      : host;
    this.sidecar = this.network.connectSidecar("sidecar", PROJECT);
    this.sidecar.subscribe(update => {
      const envelope = update.envelope;
      this.hostEnvelopes.push(envelope);
      for (const frame of envelope.frames) this.hostFrames.set(frame.sequence, frame);
      while (this.hostFrames.has(this.nextHostSequence)) {
        this.hostAck = this.nextHostSequence++;
      }
      for (let index = this.sidecarOutstanding.length - 1; index >= 0; index--) {
        const frame = this.sidecarOutstanding[index]!;
        if (frame.epoch === envelope.ack.epoch && frame.sequence <= envelope.ack.sequence) {
          this.sidecarOutstanding.splice(index, 1);
        }
      }
      const liveness = envelope.liveness;
      if (this.autoPong && liveness?.kind === "ping") {
        this.publish([], {
          lane: "control",
          ackSequence: -1,
          liveness: { kind: "pong", sentAtMs: liveness.sentAtMs },
        });
      }
    });
    this.session = new V2TransportSession({
      binding: BINDING,
      channel: this.host,
      maxResponseBytes: options.maxResponseBytes ?? 2 * 1024 * 1024,
      maxBufferedResponseBytes: options.maxBufferedResponseBytes,
      gzip: options.gzip,
      rotationTimeoutMs: options.rotationTimeoutMs,
      retransmitMs: options.retransmitMs ?? 250,
      publicationIntervalMs: this.publicationIntervalMs,
      unadmittedTimeoutMs: options.unadmittedTimeoutMs,
      ...(options.expectedSidecarInstanceId === null
        ? {}
        : { expectedSidecarInstanceId: options.expectedSidecarInstanceId ?? "sidecar-instance-1" }),
      livenessTimeoutMs: options.livenessTimeoutMs,
    });
  }

  async start(): Promise<void> {
    await this.sidecar.connect();
    await this.session.connect();
    this.publish([]);
    await this.settle();
  }

  publish(
    frames: readonly SidecarFrame[],
    options: {
      lane?: "data" | "control";
      ackSequence?: number;
      revision?: number;
      instanceId?: string;
      liveness?: AwarenessLiveness;
    } = {},
  ): void {
    for (const frame of frames) {
      this.sidecarOutstanding.push({
        ...frame,
        epoch: this.sidecarEpoch,
        sequence: this.sidecarSequence++,
      } as ReliableFrame<SidecarFrame>);
    }
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: BINDING.schemaHash,
      buildHash: BINDING.buildHash,
      projectId: PROJECT,
      sessionId: BINDING.sessionId,
      peerId: "sidecar",
      instanceId: options.instanceId ?? this.sidecarInstanceId,
      leaderGeneration: BINDING.leaderGeneration,
      epoch: this.sidecarEpoch,
      revision: options.revision ?? this.revision++,
      ack: { epoch: 1, sequence: options.ackSequence ?? this.hostAck },
      credits: this.credits,
      active: 128 - this.credits,
      queued: 0,
      draining: false,
      frames: options.lane === "control" ? [] : [...this.sidecarOutstanding],
      ...(options.lane ? { lane: options.lane } : {}),
      ...(options.liveness ? { liveness: options.liveness } : {}),
    };
    this.sidecar.publish(envelope);
  }

  lastHostPing(): Extract<AwarenessLiveness, { kind: "ping" }> {
    for (let index = this.hostEnvelopes.length - 1; index >= 0; index--) {
      const liveness = this.hostEnvelopes[index]!.liveness;
      if (liveness?.kind === "ping") return liveness;
    }
    throw new Error("Host liveness ping was not published");
  }

  restartSidecar(): void {
    this.sidecarInstanceId = "sidecar-instance-2";
    this.sidecarEpoch++;
    this.sidecarSequence = 0;
    this.revision = 0;
    this.sidecarOutstanding.length = 0;
  }

  async settle(rounds = 12): Promise<void> {
    await Bun.sleep(this.publicationIntervalMs + 1);
    for (let index = 0; index < rounds; index++) {
      await this.network.flushAll();
      await Bun.sleep(0);
    }
  }

  requestIdentity(): { requestId: string; requestDigest: string } {
    const open = [...this.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    );
    if (!open) throw new Error("request_open was not published");
    return { requestId: open.requestId, requestDigest: open.requestDigest };
  }

  async commit(pending: Promise<RunHandle>): Promise<RunHandle> {
    await this.settle();
    const identity = this.requestIdentity();
    this.credits = 127;
    this.publish([{
      kind: "request_ack",
      ...identity,
      committed: true,
    }]);
    await this.settle();
    return pending;
  }

  async completedFrames(): Promise<SidecarFrame[]> {
    const identity = this.requestIdentity();
    const final = {
      id: "resp_1",
      object: "response",
      status: "completed",
      output: [{ type: "message", content: [{ type: "output_text", text: "hello" }] }],
    };
    const events = [
      {
        event: "response.created",
        data: JSON.stringify({ type: "response.created", response: { id: "resp_1" } }),
      },
      {
        event: "response.output_text.delta",
        data: JSON.stringify({ type: "response.output_text.delta", delta: "hello" }),
      },
      {
        event: "response.completed",
        data: JSON.stringify({ type: "response.completed", response: final }),
      },
    ];
    const transcript = new TranscriptProof(identity.requestId, identity.requestDigest);
    const frames: SidecarFrame[] = events.flatMap((event, eventIndex) => {
      transcript.append(event);
      const bytes = utf8Bytes(event.data);
      const partBytes = 24;
      const partCount = Math.ceil(bytes.byteLength / partBytes);
      return Array.from({ length: partCount }, (_, partIndex) => {
        const offset = partIndex * partBytes;
        return {
          kind: "response_event" as const,
          requestId: identity.requestId,
          eventIndex,
          event: event.event,
          partIndex,
          partCount,
          offset,
          dataBase64Url: base64UrlEncode(bytes.subarray(offset, offset + partBytes)),
        };
      });
    });
    frames.push(await transcript.createTerminal());
    return frames;
  }

  async close(): Promise<void> {
    await this.session.close();
    await this.sidecar.close();
  }
}

describe("V2TransportSession", () => {
  it("probes on the control lane without disturbing an active reliable flight", async () => {
    const harness = new TransportHarness({ autoPong: true, livenessTimeoutMs: 250 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run({
      ...BODY,
      input: "liveness-flight-".repeat(30_000),
    });
    try {
      await harness.settle();
      const dataBefore = harness.hostEnvelopes.filter(envelope => envelope.lane === "data");
      const framesBefore = [...harness.hostFrames.values()];
      expect(dataBefore).toHaveLength(1);
      expect(dataBefore[0]!.frames.length).toBeGreaterThan(0);

      const probing = harness.session.probeLiveness();
      await expect(harness.session.probeLiveness()).rejects.toMatchObject({
        code: "probe_in_flight",
      });
      await harness.settle();

      await expect(probing).resolves.toMatchObject({
        rttMs: expect.any(Number),
        sentAtMs: expect.any(Number),
      });
      const pingEnvelope = harness.hostEnvelopes.find(envelope => (
        envelope.liveness?.kind === "ping"
      ));
      expect(pingEnvelope).toMatchObject({ lane: "control", frames: [] });
      expect(harness.hostEnvelopes.filter(envelope => envelope.lane === "data"))
        .toHaveLength(dataBefore.length);
      expect([...harness.hostFrames.values()]).toEqual(framesBefore);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("observes a control pong while reliable response processing is backpressured", async () => {
    const harness = new TransportHarness({
      autoPong: true,
      livenessTimeoutMs: 250,
      maxResponseBytes: 4_096,
      maxBufferedResponseBytes: 160,
    });
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const requestId = harness.requestIdentity().requestId;
    harness.publish([0, 1, 2].map(eventIndex => ({
      kind: "response_event" as const,
      requestId,
      eventIndex,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("x".repeat(100))),
    })));
    await harness.settle();
    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence))).toBe(0);

    const probing = harness.session.probeLiveness();
    await harness.settle();
    await expect(Promise.race([
      probing,
      Bun.sleep(100).then(() => {
        throw new Error("liveness pong waited behind response backpressure");
      }),
    ])).resolves.toMatchObject({ sentAtMs: expect.any(Number) });
    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence)))
      .toBeLessThan(harness.sidecarSequence - 1);

    await harness.close();
    await Promise.allSettled([handle.final]);
  });

  it("fails terminally instead of ACK-jumping after backpressured processing throws", async () => {
    const harness = new TransportHarness({
      maxResponseBytes: 4_096,
      maxBufferedResponseBytes: 160,
    });
    await harness.start();
    const controller = new AbortController();
    const handle = await harness.commit(harness.session.run(BODY, controller.signal));
    const final = handle.final.catch(error => error);
    const requestId = harness.requestIdentity().requestId;
    harness.publish([0, 1, 2].map(eventIndex => ({
      kind: "response_event" as const,
      requestId,
      eventIndex,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("x".repeat(100))),
    })));
    await harness.settle();

    controller.abort();
    for (let round = 0; round < 8; round++) await Bun.sleep(0);
    expect(await final).toMatchObject({ committed: true, ambiguous: true });
    expect(harness.session.snapshot()).toMatchObject({ connected: false, credits: 0 });
    await harness.network.flushAll();
    const ackAfterFailure = Math.max(
      ...harness.hostEnvelopes.map(envelope => envelope.ack.sequence),
    );
    expect(ackAfterFailure).toBeLessThan(harness.sidecarSequence - 1);

    harness.publish([], { lane: "control" });
    await harness.settle();
    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence)))
      .toBe(ackAfterFailure);
    await harness.close();
  });

  it("accepts only an exact pong from the bootstrap-bound sidecar instance", async () => {
    const harness = new TransportHarness({ livenessTimeoutMs: 500 });
    await harness.start();
    const probing = harness.session.probeLiveness();
    await harness.settle();
    const ping = harness.lastHostPing();

    harness.publish([], {
      lane: "data",
      instanceId: "sidecar-instance-stale",
    });
    await harness.settle();
    expect(await promiseState(probing)).toBe("pending");

    harness.publish([], {
      lane: "control",
      liveness: { kind: "pong", sentAtMs: ping.sentAtMs - 1 },
    });
    await harness.settle();
    expect(await promiseState(probing)).toBe("pending");

    harness.publish([], {
      lane: "control",
      instanceId: "sidecar-instance-unbound",
      liveness: { kind: "pong", sentAtMs: ping.sentAtMs },
    });
    await harness.settle();
    await expect(probing).rejects.toMatchObject({ code: "instance_changed" });
    await harness.close();
  });

  it("rejects a foreign instance before the first expected sidecar envelope", async () => {
    const harness = new TransportHarness({ livenessTimeoutMs: 500 });
    await harness.sidecar.connect();
    await harness.session.connect();
    const probing = harness.session.probeLiveness();
    const rejected = probing.catch(error => error);
    harness.publish([], {
      lane: "data",
      instanceId: "sidecar-instance-foreign",
    });
    await harness.settle();

    expect(await rejected).toMatchObject({ code: "instance_changed" });
    await harness.close();
  });

  it("times out even when a generic sidecar envelope was seen recently", async () => {
    const harness = new TransportHarness({ livenessTimeoutMs: 15 });
    await harness.start();
    expect(harness.session.snapshot().sidecarPeerAgeMs).not.toBeNull();

    await expect(harness.session.probeLiveness()).rejects.toMatchObject({ code: "timeout" });
    await harness.settle();
    const latestControl = harness.hostEnvelopes
      .filter(envelope => envelope.lane === "control")
      .at(-1)!;
    expect(latestControl.liveness).toBeUndefined();
    await harness.close();
  });

  it("bounds a synchronous liveness publication failure", async () => {
    const harness = new TransportHarness({ throwOnLivenessPublish: true });
    await harness.start();

    await expect(harness.session.probeLiveness()).rejects.toMatchObject({
      name: "V2TransportLivenessError",
      code: "publish",
      message: "V2 sidecar liveness probe could not be published",
    });
    const latestControl = harness.hostEnvelopes
      .filter(envelope => envelope.lane === "control")
      .at(-1)!;
    expect(latestControl.liveness).toBeUndefined();
    await harness.close();
  });

  it("aborts cleanly and ignores a late pong from the cancelled probe", async () => {
    const harness = new TransportHarness({ livenessTimeoutMs: 1_000 });
    await harness.start();
    const controller = new AbortController();
    const first = harness.session.probeLiveness(controller.signal);
    await harness.settle();
    const firstPing = harness.lastHostPing();
    controller.abort();
    await expect(first).rejects.toMatchObject({ name: "AbortError" });

    const second = harness.session.probeLiveness();
    await harness.settle();
    const secondPing = harness.lastHostPing();
    expect(secondPing.sentAtMs).toBeGreaterThan(firstPing.sentAtMs);

    harness.publish([], {
      lane: "control",
      liveness: { kind: "pong", sentAtMs: firstPing.sentAtMs },
    });
    await harness.settle();
    expect(await promiseState(second)).toBe("pending");

    harness.publish([], {
      lane: "control",
      liveness: { kind: "pong", sentAtMs: secondPing.sentAtMs },
    });
    await harness.settle();
    await expect(second).resolves.toMatchObject({ sentAtMs: secondPing.sentAtMs });
    await harness.close();
  });

  it("rejects and cleans up an outstanding liveness probe when closed", async () => {
    const harness = new TransportHarness({ livenessTimeoutMs: 1_000 });
    await harness.start();
    const controller = new AbortController();
    const probing = harness.session.probeLiveness(controller.signal);
    await harness.settle();

    const closing = harness.session.close();
    await expect(probing).rejects.toMatchObject({ code: "unavailable" });
    await closing;
    controller.abort();
    expect(harness.sidecar.peerIds()).toEqual([]);
    await expect(harness.session.probeLiveness()).rejects.toMatchObject({
      code: "unavailable",
    });
    await harness.sidecar.close();
  });

  it("treats a published commit with a lost ACK as committed and ambiguous", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    expect([...harness.hostFrames.values()]).toContainEqual(expect.objectContaining({
      kind: "request_commit",
    }));

    const closing = harness.session.close();
    await expect(pending).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "closed",
    });
    await closing;
    expect(harness.session.snapshot().quarantined).toBe(1);
    await harness.sidecar.close();
  });

  it("treats an abort after commit staging as ambiguous before paced publication", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 100 });
    await harness.start();
    // Reset the peer-wide cadence immediately before staging the request.
    harness.session.setHostCeiling(127);
    await Bun.sleep(0);
    await harness.network.flushAll();

    const controller = new AbortController();
    const pending = harness.session.run(BODY, controller.signal);
    for (let attempt = 0; attempt < 50; attempt++) {
      if (harness.session.snapshot().active === 1) break;
      await Bun.sleep(1);
    }
    expect(harness.session.snapshot().active).toBe(1);
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit"))
      .toBe(false);

    controller.abort();
    await expect(pending).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "cancelled",
    });
    for (let round = 0; round < 4; round++) {
      await Bun.sleep(0);
      await harness.network.flushAll();
    }
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit"))
      .toBe(true);
    await harness.close();
  });

  it("marks commit risk before a synchronous data publication failure", async () => {
    const harness = new TransportHarness({ throwOnCommitPublish: true });
    await harness.start();
    const pending = harness.session.run(BODY);
    const rejected = pending.catch(error => error);
    await harness.settle();

    expect(await rejected).toMatchObject({
      committed: true,
      ambiguous: true,
      code: "protocol",
      message: "synthetic commit publication failure",
    });
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit"))
      .toBe(false);
    expect(harness.session.snapshot()).toMatchObject({
      connected: false,
      credits: 0,
      quarantined: 1,
    });
    await harness.close();
  });

  it("makes the transport unavailable after a pre-commit data publication failure", async () => {
    const harness = new TransportHarness({ throwOnDataPublish: true });
    await harness.start();
    const pending = harness.session.run({
      ...BODY,
      input: "precommit-flight-".repeat(30_000),
    });
    const rejected = pending.catch(error => error);
    await harness.settle();

    expect(await rejected).toMatchObject({
      committed: false,
      code: "protocol",
      message: "synthetic data publication failure",
    });
    expect(harness.session.snapshot()).toMatchObject({ connected: false, credits: 0 });
    await expect(harness.session.run(BODY)).rejects.toMatchObject({
      committed: false,
      code: "not_connected",
    });
    await harness.close();
  });

  it("rejects only never-admitted work when sandbox capacity changes", async () => {
    const harness = new TransportHarness({
      publicationIntervalMs: 1,
      unadmittedTimeoutMs: 10,
    });
    await harness.start();
    harness.credits = 0;
    harness.publish([], { lane: "control" });
    await harness.settle();
    harness.hostFrames.clear();

    const pending = harness.session.run(BODY);
    try {
      const result = await Promise.race([
        pending.then(() => "resolved", error => error),
        Bun.sleep(50).then(() => "timed-out"),
      ]);
      expect(result).toMatchObject({
        committed: false,
        code: "capacity_changed",
      });
      expect(harness.hostFrames.size).toBe(0);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("never applies the unadmitted timeout after scheduler admission", async () => {
    const harness = new TransportHarness({
      publicationIntervalMs: 1,
      unadmittedTimeoutMs: 10,
    });
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    await Bun.sleep(20);
    expect(await promiseState(pending)).toBe("pending");
    await harness.commit(pending);
    await harness.close();
  });

  it("publishes ACK control state without repeating an active request flight", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run({
      ...BODY,
      input: "control-split-".repeat(20_000),
    });
    try {
      await harness.settle();
      const requestFlight = harness.hostEnvelopes.find(value => value.lane === "data");
      expect(requestFlight?.frames.length).toBeGreaterThan(0);

      harness.publish([{
        kind: "pong",
        sentAtMs: 1,
      }], { lane: "data", ackSequence: -1 });
      await harness.settle();

      const control = harness.hostEnvelopes.find(value => value.lane === "control");
      expect(control?.frames).toEqual([]);
      expect(JSON.stringify(control).length * 4).toBeLessThan(JSON.stringify(requestFlight).length);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("freezes a bounded request flight until its high sequence is acknowledged", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run({ ...BODY, input: "large-flight-".repeat(40_000) });
    try {
      await harness.settle();

      const first = harness.hostEnvelopes.filter(value => value.lane === "data");
      expect(first).toHaveLength(1);
      expect(first[0]!.frames.length).toBeLessThanOrEqual(32);
      const flightBytes = first[0]!.frames.reduce(
        (sum, frame) => sum + utf8Bytes(JSON.stringify(frame)).byteLength,
        0,
      );
      expect(flightBytes).toBeLessThanOrEqual(128 * 1024);
      await Bun.sleep(10);
      await harness.settle();
      expect(harness.hostEnvelopes.filter(value => value.lane === "data")).toHaveLength(1);

      const high = first[0]!.frames.at(-1)!.sequence;
      harness.publish([], { lane: "control", ackSequence: high });
      await harness.settle();
      expect(harness.hostEnvelopes.filter(value => value.lane === "data").length)
        .toBeGreaterThan(1);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("tracks control and data revisions independently", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    const identity = harness.requestIdentity();
    harness.credits = 127;
    try {
      harness.publish([], { lane: "control", revision: 100 });
      harness.publish([{
        kind: "request_ack",
        ...identity,
        committed: true,
      }], { lane: "data", revision: 1 });
      await harness.settle();

      expect(await promiseState(pending)).toBe("settled");
      await pending;
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("coalesces concurrent request data into one peer-wide publication", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 25 });
    await harness.start();
    harness.hostEnvelopes.length = 0;

    const pending = Array.from({ length: 4 }, (_, index) => harness.session.run({
      ...BODY,
      input: `paced-${index}`,
    }));
    await harness.settle();

    expect(harness.hostEnvelopes).toHaveLength(1);
    expect(harness.hostEnvelopes[0]!.frames.filter(
      frame => frame.kind === "request_open",
    )).toHaveLength(4);
    expect(harness.session.snapshot().diagnostics?.host).toMatchObject({
      pacedPublicationsTotal: 1,
      urgentPublicationsTotal: 0,
      logicalEventsTotal: 0,
      responseBatchFramesTotal: 0,
      batchedEventsTotal: 0,
    });
    await harness.close();
    await Promise.allSettled(pending);
  });

  it("measures ACK RTT from first sequence send instead of a later cumulative publish", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    await Bun.sleep(30);
    harness.session.setHostCeiling(127);
    await Bun.sleep(10);

    const identity = harness.requestIdentity();
    harness.credits = 127;
    harness.publish([{ kind: "request_ack", ...identity, committed: true }]);
    await harness.settle();

    expect(harness.session.snapshot().awarenessRttMs).toBeGreaterThan(30);
    await pending;
    await harness.close();
  });

  it("expands response batches into the exact logical SSE transcript", async () => {
    const harness=new TransportHarness();await harness.start();const handle=await harness.commit(harness.session.run(BODY));const id=harness.requestIdentity();
    const final={id:"resp_batch",object:"response",status:"completed",output:[]};const events=[{event:"response.created",data:JSON.stringify({type:"response.created",response:{id:"resp_batch"}})},{event:"response.output_text.delta",data:JSON.stringify({type:"response.output_text.delta",delta:"hello"})},{event:"response.completed",data:JSON.stringify({type:"response.completed",response:final})}];const proof=new TranscriptProof(id.requestId,id.requestDigest);for(const event of events)proof.append(event);const terminal=await proof.createTerminal();
    harness.publish([{kind:"response_batch",requestId:id.requestId,firstEventIndex:0,items:events.map(event=>({event:event.event,dataBase64Url:base64UrlEncode(utf8Bytes(event.data))}))},terminal]);await harness.settle();const received=[];for await(const event of handle.events())received.push(event);expect(received).toEqual(events);await expect(handle.final).resolves.toMatchObject({id:"resp_batch"});await harness.close();
  });
  it("admits at most four uncommitted requests per sandbox", async () => {
    const harness=new TransportHarness();await harness.start();
    const pending=Array.from({length:5},(_,i)=>harness.session.run({...BODY,input:`request-${i}`}));
    await harness.settle();expect(harness.session.snapshot()).toMatchObject({admissionCredits:0,queued:1});
    await harness.close();await Promise.allSettled(pending);
  });
  it("derives retransmission delay from the RTT floor plus bounded jitter", () => {
    expect(computeV2RetransmitDelayMs(null, 0)).toBe(250);
    expect(computeV2RetransmitDelayMs(200, 0)).toBe(400);
    expect(computeV2RetransmitDelayMs(200, 1)).toBeGreaterThan(400);
    expect(computeV2RetransmitDelayMs(200, 1)).toBeLessThanOrEqual(440);
    expect(computeV2RetransmitDelayMs(null, 0, 250, 1)).toBe(500);
    expect(computeV2RetransmitDelayMs(null, 0, 250, 20)).toBe(30_000);
  });

  it("does not return a RunHandle before a matching committed ACK", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();

    expect([...harness.hostFrames.values()].map(frame => frame.kind)).toEqual([
      "request_open",
      "request_data",
      "request_commit",
    ]);
    expect(await promiseState(pending)).toBe("pending");

    const handle = await harness.commit(pending);
    expect(handle.stats.frames).toBe(0);
    await harness.close();
  });

  it("streams segmented events and settles only after terminal proof", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    harness.credits = 128;
    harness.publish(await harness.completedFrames());
    await harness.settle();

    const events = await Array.fromAsync(handle.events());
    expect(events.map(event => event.event)).toEqual([
      "response.created",
      "response.output_text.delta",
      "response.completed",
    ]);
    await expect(handle.final).resolves.toMatchObject({ id: "resp_1", status: "completed" });
    await harness.settle();
    expect([...harness.hostFrames.values()]).toContainEqual(expect.objectContaining({
      kind: "response_ack",
      requestId: harness.requestIdentity().requestId,
    }));
    await harness.close();
  });

  it("holds an early priority terminal until all logical events arrive", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const frames = await harness.completedFrames();
    const terminal = frames.pop()!;
    const snapshot = spyOn(TranscriptProof.prototype, "snapshot");

    try {
      harness.publish([terminal]);
      await harness.settle();
      expect(await promiseState(handle.final)).toBe("pending");
      expect(snapshot).not.toHaveBeenCalled();

      harness.publish(frames);
      await harness.settle();
      await expect(handle.final).resolves.toMatchObject({ id: "resp_1" });
      expect(await Array.fromAsync(handle.events())).toHaveLength(3);
      expect(snapshot).toHaveBeenCalledTimes(1);
    } finally {
      snapshot.mockRestore();
      await harness.close();
    }
  });

  it("quarantines a tampered terminal proof without acknowledging it", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const frames = await harness.completedFrames();
    const terminal = frames.at(-1)! as Extract<SidecarFrame, { kind: "response_terminal" }>;
    frames[frames.length - 1] = {
      ...terminal,
      transcriptSha256: "f".repeat(64),
    };
    harness.publish(frames);
    await harness.settle();

    await expect(handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "terminal_proof",
    });
    expect(harness.session.snapshot().quarantined).toBe(1);
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "response_ack")).toBe(false);
    await harness.close();
  });

  it("withholds the cumulative network ACK while the client buffer is full", async () => {
    const harness = new TransportHarness({
      maxResponseBytes: 4_096,
      maxBufferedResponseBytes: 160,
    });
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const requestId = harness.requestIdentity().requestId;
    harness.publish([0, 1].map(eventIndex => ({
      kind: "response_event" as const,
      requestId,
      eventIndex,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("x".repeat(100))),
    })));
    await harness.settle();

    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence))).toBe(0);
    const events = handle.events();
    await events.next();
    await harness.settle();
    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence))).toBe(2);
    await harness.close();
  });

  it("distinguishes pre-commit rejection from committed ambiguity", async () => {
    const before = new TransportHarness();
    await before.start();
    const pending = before.session.run(BODY);
    const rejected = pending.catch(error => error);
    await before.settle();
    const identity = before.requestIdentity();
    before.publish([{
      kind: "request_reject",
      requestId: identity.requestId,
      code: "capacity",
      message: "full",
    }]);
    await before.settle();
    expect(await rejected).toBeInstanceOf(V2PreCommitError);
    await before.close();

    const after = new TransportHarness();
    await after.start();
    const handle = await after.commit(after.session.run(BODY));
    const committedIdentity = after.requestIdentity();
    after.publish([{
      kind: "response_error",
      requestId: committedIdentity.requestId,
      code: "transport",
      message: "reset",
      ambiguous: true,
    }]);
    await after.settle();
    await expect(handle.final).rejects.toBeInstanceOf(V2CommittedError);
    expect(after.session.snapshot()).toMatchObject({
      quarantined: 1,
      ambiguousQuarantined: 1,
      ambiguityLimit: 4,
    });
    await after.close();
  });

  it("reports ambiguity exhaustion against the configured admission ceiling", async () => {
    const harness = new TransportHarness();
    await harness.start();
    harness.session.setHostCeiling(1);
    const handle = await harness.commit(harness.session.run(BODY));
    const requestId = harness.requestIdentity().requestId;
    harness.publish([{
      kind: "response_error",
      requestId,
      code: "transport",
      message: "lost terminal proof",
      ambiguous: true,
    }]);
    await harness.settle();

    await expect(handle.final).rejects.toMatchObject({ ambiguous: true });
    expect(harness.session.snapshot()).toMatchObject({
      quarantined: 1,
      ambiguousQuarantined: 1,
      ambiguityLimit: 1,
      admissionCredits: 0,
    });
    await harness.close();
  });

  it("fences a replacement sidecar and quarantines work committed to the retired instance", async () => {
    const idle = new TransportHarness({ expectedSidecarInstanceId: null });
    await idle.start();
    idle.restartSidecar();
    idle.credits = 64;
    idle.publish([]);
    await idle.settle();
    expect(idle.session.snapshot()).toMatchObject({ connected: true, credits: 64 });
    await idle.close();

    const active = new TransportHarness({ expectedSidecarInstanceId: null });
    await active.start();
    const handle = await active.commit(active.session.run(BODY));
    active.restartSidecar();
    active.publish([]);
    await active.settle();

    await expect(handle.final).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
    });
    expect(active.session.snapshot().quarantined).toBe(1);
    await active.close();
  });

  it("advances a large request across multiple windows without commit overtaking data", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "large-window-".repeat(35_000),
      stream: true,
    });

    for (let window = 0; window < 8; window++) {
      await harness.settle();
      if ([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit")) break;
      harness.publish([]);
    }
    await harness.settle();
    const frames = [...harness.hostFrames.values()];
    expect(frames.length).toBeGreaterThan(32);
    expect(frames[0]?.kind).toBe("request_open");
    expect(frames.at(-1)?.kind).toBe("request_commit");
    expect(frames.slice(1, -1).every(frame => frame.kind === "request_data")).toBe(true);
    for (const frame of frames) {
      expect(utf8Bytes(JSON.stringify(frame)).byteLength).toBeLessThanOrEqual(16 * 1024);
    }

    const handle = await harness.commit(pending);
    expect(handle).toBeDefined();
    await harness.close();
  });

  it("uses gzip only when it reduces the request part count", async () => {
    const controller = new AbortController();
    let observedOptions: V2GzipOptions | undefined;
    const harness = new TransportHarness({
      gzip: async (bytes, options) => {
        observedOptions = options;
        return gzipSync(bytes);
      },
    });
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "compressible-context-".repeat(10_000),
      stream: true,
    }, controller.signal);
    await harness.settle();
    const open = [...harness.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    )!;
    expect(open.encoding).toBe("gzip+base64");
    expect(open.encodedBytes).toBeLessThan(open.decodedBytes);
    expect(open.partCount).toBe(1);
    expect(observedOptions).toEqual({
      signal: controller.signal,
      inputOwnership: "owned",
    });
    await harness.commit(pending);
    await harness.close();
  });

  it("closes only after the sidecar proves an idle drain", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const draining = harness.session.drain("test shutdown");
    await harness.settle();
    const drain = [...harness.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "drain" }> => (
        frame.kind === "drain"
      ),
    );
    expect(drain).toMatchObject({ reason: "test shutdown" });
    expect(await promiseState(draining)).toBe("pending");

    harness.publish([{ kind: "drained", active: 0, queued: 0 }]);
    await harness.settle();
    await expect(draining).resolves.toBeUndefined();
    expect(harness.session.snapshot().connected).toBe(false);
    await harness.close();
  });

  it("settles credential rotation only after a matching sidecar ACK", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const rotating = harness.session.rotateCredential(
      "rotation-1",
      JSON.stringify({ encrypted: "ciphertext" }),
    );
    await harness.settle();
    expect([...harness.hostFrames.values()]).toContainEqual(expect.objectContaining({
      kind: "credential_rotate",
      rotationId: "rotation-1",
    }));
    expect(await promiseState(rotating)).toBe("pending");

    harness.publish([{ kind: "credential_ack", rotationId: "rotation-1" }]);
    await harness.settle();

    await expect(rotating).resolves.toBeUndefined();
    await harness.close();
  });

  it("bounds a credential rotation whose ACK never arrives", async () => {
    const harness = new TransportHarness({ rotationTimeoutMs: 5 });
    await harness.start();

    const result = await Promise.race([
      harness.session.rotateCredential("rotation-timeout", "ciphertext")
        .then(() => "resolved", () => "rejected"),
      Bun.sleep(30).then(() => "hung"),
    ]);

    expect(result).toBe("rejected");
    await harness.close();
  });

  it("removes drain abort listeners and rejects cleanly when closed mid-drain", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const controller = new AbortController();
    const draining = harness.session.drain("interrupted", controller.signal).catch(error => error);
    await harness.settle();

    await harness.session.close();

    expect(await draining).toBeInstanceOf(V2PreCommitError);
    await harness.sidecar.close();
  });

  it("cancels queued work safely and committed work without reroute", async () => {
    const queued = new TransportHarness();
    await queued.start();
    queued.credits = 0;
    queued.publish([]);
    await queued.settle();
    const abortQueued = new AbortController();
    const pending = queued.session.run(BODY, abortQueued.signal);
    abortQueued.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(queued.session.snapshot().queued).toBe(0);
    await queued.close();

    const published = new TransportHarness();
    await published.start();
    const abortPublished = new AbortController();
    const awaitingAck = published.session.run(BODY, abortPublished.signal);
    await published.settle();
    const publishedRequest = published.requestIdentity().requestId;
    abortPublished.abort();
    await expect(awaitingAck).rejects.toMatchObject({
      committed: true,
      ambiguous: true,
      code: "cancelled",
    });
    expect(published.session.snapshot()).toMatchObject({
      quarantined: 1,
      ambiguousQuarantined: 0,
    });
    published.publish([{ kind: "request_cancelled", requestId: publishedRequest }]);
    await published.settle();
    expect(published.session.snapshot().quarantined).toBe(0);
    await published.close();

    const committed = new TransportHarness();
    await committed.start();
    const abortCommitted = new AbortController();
    const handle = await committed.commit(committed.session.run(BODY, abortCommitted.signal));
    abortCommitted.abort();
    await committed.settle();
    const cancelledRequest = committed.requestIdentity().requestId;
    expect([...committed.hostFrames.values()]).toContainEqual(expect.objectContaining({
      kind: "request_cancel",
    }));
    await expect(handle.final).rejects.toMatchObject({ committed: true });
    expect(committed.session.snapshot()).toMatchObject({
      quarantined: 1,
      ambiguousQuarantined: 0,
    });
    committed.publish([{
      kind: "request_cancelled",
      requestId: cancelledRequest,
    }]);
    await committed.settle();
    expect(committed.session.snapshot().quarantined).toBe(0);
    committed.publish([
      { kind: "request_cancelled", requestId: cancelledRequest },
      { kind: "request_cancelled", requestId: "req-unrelated" },
    ]);
    await committed.settle();
    expect(committed.session.snapshot().quarantined).toBe(0);
    await committed.close();
  });

  it("retransmits the unchanged window after a dropped publication", async () => {
    const harness = new TransportHarness({ drop: [3], retransmitMs: 100 });
    await harness.start();
    const pending = harness.session.run(BODY);
    await harness.settle();
    expect(harness.hostFrames.size).toBe(0);

    await Bun.sleep(120);
    await harness.settle();
    expect(harness.requestIdentity().requestId).toBeTruthy();
    expect(harness.session.snapshot().retransmissions).toBeGreaterThan(0);
    const handle = await harness.commit(pending);
    expect(handle).toBeDefined();
    await harness.close();
  });
});
