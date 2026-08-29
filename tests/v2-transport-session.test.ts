import { describe, expect, it, spyOn } from "bun:test";
import { gunzipSync, gzipSync } from "fflate";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import { projectId } from "../src/sandbox/types";
import type { AwarenessChannel } from "../src/v2/awareness/channel";
import {
  base64UrlEncode,
  base64UrlDecode,
  canonicalJson,
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  sha256Hex,
  utf8Bytes,
  type AwarenessEnvelope,
  type AwarenessLiveness,
  type HostFrame,
  type ProtocolBinding,
  type R2RequestCommitFrame,
  type ReliableFrame,
  type SidecarFrame,
  type V2TransportProfile,
} from "../src/v2/protocol/shared";
import { TranscriptProof } from "../src/v2/protocol/transcript";
import { WeightedFrameScheduler } from "../src/v2/protocol/scheduler";
import {
  V2CommittedError,
  V2PreCommitError,
} from "../src/v2/transport/run-handle";
import {
  computeV2RetransmitDelayMs,
  MAX_TERMINAL_OBJECT_KEYS,
  V2_R2_REQUEST_PART_BYTES,
  V2TransportSession,
  validateHostTerminalFinalStructure,
  type V2GzipOptions,
} from "../src/v2/transport/session";
import {
  v2HostTransferBudget,
  v2RequestTransferCost,
} from "../src/v2/transport/admission-cost";
import { FakeAwarenessNetwork } from "./support/fake-awareness";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import { AwarenessChannelError } from "../src/v2/awareness/y-sweet-channel";
import { createExactSizedV2Request } from "./support/v2-performance-harness";
import { HostRequestDiagnosticsObserver } from "../src/v2/diagnostics/host-observer";
import {
  MAX_RESPONSES_JSON_DEPTH,
  MAX_RESPONSES_JSON_ITEMS,
  ResponsesInputStructureError,
} from "../src/relay/responses-validation";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};
const R2_BINDING: ProtocolBinding = {
  ...BINDING,
  protocolRevision: "r2",
  launchNonce: "launch-nonce-r2",
};
const HOST_INSTANCE_ID = "host-instance-1";
const BODY = { model: "gpt-5.6-sol", input: "hello", stream: true };

async function promiseState(value: Promise<unknown>): Promise<"pending" | "settled"> {
  return Promise.race([
    value.then(() => "settled" as const, () => "settled" as const),
    new Promise<"pending">(resolve => queueMicrotask(() => resolve("pending"))),
  ]);
}

function throwOnSelectedPublish(
  channel: AwarenessChannel<HostFrame, SidecarFrame>,
  options: { data: boolean; commit: boolean; liveness: boolean; clear: boolean },
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
    clear: lane => {
      if (options.clear) throw new Error("synthetic channel clear failure");
      channel.clear(lane);
    },
    subscribe: listener => channel.subscribe(listener),
    peerIds: () => channel.peerIds(),
    close: () => channel.close(),
  };
}

interface ReconnectableFault {
  armed: boolean;
  failed: boolean;
  reconnects: number;
}

function throwOneReconnectablePublish(
  channel: AwarenessChannel<HostFrame, SidecarFrame>,
  fault: ReconnectableFault,
): AwarenessChannel<HostFrame, SidecarFrame> {
  return {
    get projectId() { return channel.projectId; },
    get localPeerId() { return channel.localPeerId; },
    async connect(signal) {
      if (fault.failed) fault.reconnects++;
      await channel.connect(signal);
    },
    publish(envelope, lane) {
      if (fault.armed && !fault.failed && lane === "data") {
        fault.failed = true;
        throw new AwarenessChannelError(
          "synthetic post-sync disconnect",
          "disconnected",
        );
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
  readonly binding: ProtocolBinding;
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
  stateChanges = 0;

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
    throwOnClear?: boolean;
    reconnectableFault?: ReconnectableFault;
    expectedSidecarInstanceId?: string | null;
    eventMemoryPool?: AtomicBytePermitPool;
    transportStallMs?: number;
    binding?: ProtocolBinding;
    transportProfile?: V2TransportProfile;
  } = {}) {
    this.publicationIntervalMs = options.publicationIntervalMs ?? 1;
    this.autoPong = options.autoPong ?? false;
    this.network = new FakeAwarenessNetwork({ drop: options.drop });
    const host = this.network.connectHost("host", PROJECT);
    const selected = options.throwOnDataPublish ||
      options.throwOnCommitPublish ||
      options.throwOnLivenessPublish ||
      options.throwOnClear
      ? throwOnSelectedPublish(host, {
          data: options.throwOnDataPublish ?? false,
          commit: options.throwOnCommitPublish ?? false,
          liveness: options.throwOnLivenessPublish ?? false,
          clear: options.throwOnClear ?? false,
        })
      : host;
    this.host = options.reconnectableFault
      ? throwOneReconnectablePublish(selected, options.reconnectableFault)
      : selected;
    this.sidecar = this.network.connectSidecar("sidecar", PROJECT);
    this.binding = options.binding ?? BINDING;
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
      binding: this.binding,
      transportProfile: options.transportProfile,
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
      hostInstanceId: HOST_INSTANCE_ID,
      livenessTimeoutMs: options.livenessTimeoutMs,
      eventMemoryPool: options.eventMemoryPool,
      transportStallMs: options.transportStallMs,
      onStateChange: () => { this.stateChanges++; },
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
    const common = {
      protocol: "gp-awareness-v2" as const,
      schemaHash: this.binding.schemaHash,
      buildHash: this.binding.buildHash,
      projectId: PROJECT,
      sessionId: this.binding.sessionId,
      peerId: "sidecar",
      instanceId: options.instanceId ?? this.sidecarInstanceId,
      leaderGeneration: this.binding.leaderGeneration,
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
    const envelope: AwarenessEnvelope<SidecarFrame> = this.binding.protocolRevision === "r2"
      ? {
          ...common,
          schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          protocolRevision: "r2",
          launchNonce: this.binding.launchNonce,
          targetInstanceId: HOST_INSTANCE_ID,
        }
      : { ...common, schemaVersion: GP_V2_R1_SCHEMA_VERSION };
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

  async settleUntil(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
    const deadline = performance.now() + timeoutMs;
    do {
      await this.settle();
      if (predicate()) return;
      await Bun.sleep(1);
    } while (performance.now() < deadline);
    throw new Error("transport harness condition did not settle before its deadline");
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
  it("bounds host terminal structure before canonical proof hashing", () => {
    expect(() => validateHostTerminalFinalStructure({
      id: "resp_valid",
      status: "completed",
      output: [],
    })).not.toThrow();
    const exact = {
      id: "resp_exact_structure",
      status: "completed",
      output: new Array(MAX_RESPONSES_JSON_ITEMS - 4).fill(null),
    };
    expect(() => validateHostTerminalFinalStructure(exact)).not.toThrow();
    expect(() => validateHostTerminalFinalStructure({
      ...exact,
      output: new Array(MAX_RESPONSES_JSON_ITEMS - 3).fill(null),
    })).toThrow("too many JSON values");

    const oversized = new Array(MAX_RESPONSES_JSON_ITEMS + 1);
    Object.defineProperty(oversized, "0", {
      get() { throw new Error("oversized terminal element was read"); },
    });
    expect(() => validateHostTerminalFinalStructure({ output: oversized }))
      .toThrow("too many JSON values");
    expect(() => validateHostTerminalFinalStructure({ output: new Array(2) }))
      .toThrow("sparse array");
    expect(() => validateHostTerminalFinalStructure({ ["k".repeat(4 * 1024 + 1)]: 1 }))
      .toThrow("canonical keys");

    const boundedObject = Object.fromEntries(Array.from(
      { length: MAX_TERMINAL_OBJECT_KEYS },
      (_, index) => [`key-${index.toString(36).padStart(3, "0")}`, null],
    ));
    expect(() => validateHostTerminalFinalStructure(boundedObject)).not.toThrow();
    boundedObject["one-key-too-many"] = null;
    expect(() => validateHostTerminalFinalStructure(boundedObject))
      .toThrow("too many keys");

    // Count already-discovered siblings before scheduling a nested container.
    // The rejection must happen before any nested child is retained/read.
    const outer = new Array(32_769).fill(null);
    const nested = new Array(32_767).fill(null);
    Object.defineProperty(nested, "0", {
      configurable: true,
      enumerable: true,
      get() { throw new Error("over-limit nested terminal child was read"); },
    });
    outer[0] = nested;
    expect(() => validateHostTerminalFinalStructure({ output: outer }))
      .toThrow("too many JSON values");
  });

  it("keeps standard rollback accounting conservative while R5 admits bounded medium concurrency", () => {
    expect(v2HostTransferBudget("standard")).toBe(5);
    expect(v2HostTransferBudget("bulk-v1")).toBe(6);
    expect(v2RequestTransferCost(2 * 1024 ** 2, "standard")).toBe(3);
    expect(v2RequestTransferCost(2 * 1024 ** 2, "bulk-v1")).toBe(1);
    expect(v2RequestTransferCost(2 * 1024 ** 2 + 1, "standard")).toBe(4);
    expect(v2RequestTransferCost(2 * 1024 ** 2 + 1, "bulk-v1")).toBe(2);
    expect(v2RequestTransferCost(3 * 1024 ** 2, "bulk-v1")).toBe(2);
    expect(v2RequestTransferCost(3 * 1024 ** 2 + 1, "bulk-v1")).toBe(4);
    expect(() => v2HostTransferBudget("invalid" as never)).toThrow(RangeError);
  });

  it("packs the dense R2 request part under the hard frame cap at worst metadata widths", () => {
    const frame: ReliableFrame<HostFrame> = {
      kind: "request_data",
      requestId: `req-${"f".repeat(36)}`,
      partIndex: 9_999_999,
      offset: 64 * 1024 ** 2 - 1,
      dataBase64Url: base64UrlEncode(new Uint8Array(V2_R2_REQUEST_PART_BYTES)),
      epoch: Number.MAX_SAFE_INTEGER,
      sequence: Number.MAX_SAFE_INTEGER,
    };
    const encodedBytes = utf8Bytes(JSON.stringify(frame)).byteLength;
    expect(encodedBytes).toBeLessThanOrEqual(16 * 1024);
    expect(16 * 1024 - encodedBytes).toBeGreaterThanOrEqual(256);
  });

  it("probes on the control lane without disturbing an active reliable flight", async () => {
    const harness = new TransportHarness({ autoPong: true, livenessTimeoutMs: 250 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run({
      ...BODY,
      input: "liveness-flight-".repeat(30_000),
    });
    try {
      await harness.settleUntil(() => (
        harness.hostEnvelopes.some(envelope => envelope.lane === "data")
      ));
      const dataBefore = harness.hostEnvelopes.filter(envelope => envelope.lane === "data");
      const framesBefore = [...harness.hostFrames.values()];
      expect(dataBefore).toHaveLength(1);
      expect(dataBefore[0]!.frames.length).toBeGreaterThan(0);

      const stateChangesBeforeProbe = harness.stateChanges;
      const probing = harness.session.probeLiveness();
      await expect(harness.session.probeLiveness()).rejects.toMatchObject({
        code: "probe_in_flight",
      });
      await harness.settle();

      await expect(probing).resolves.toMatchObject({
        rttMs: expect.any(Number),
        sentAtMs: expect.any(Number),
      });
      await Promise.resolve();
      expect(harness.stateChanges).toBeGreaterThan(stateChangesBeforeProbe);
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

  it("observes a control pong while the reliable data lane backpressures a slow consumer", async () => {
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
    expect(await promiseState(handle.final)).toBe("pending");

    const probing = harness.session.probeLiveness();
    await harness.settle();
    await expect(Promise.race([
      probing,
      Bun.sleep(100).then(() => {
        throw new Error("liveness pong waited behind response backpressure");
      }),
    ])).resolves.toMatchObject({ sentAtMs: expect.any(Number) });
    const events = handle.events();
    for (let index = 0; index < 3; index++) {
      await expect(events.next()).resolves.toMatchObject({
        value: { event: "response.output_text.delta" },
        done: false,
      });
      await harness.settle();
    }
    await harness.settleUntil(() => Math.max(
      ...harness.hostEnvelopes.map(envelope => envelope.ack.sequence),
    ) === harness.sidecarSequence - 1);
    expect(await promiseState(handle.final)).toBe("pending");
    await events.return(undefined);

    await harness.close();
    await Promise.allSettled([handle.final]);
  });

  it("lets cancellation close a backpressured request without poisoning the transport", async () => {
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
    expect(await final).toMatchObject({
      committed: true,
      ambiguous: true,
      code: "cancelled",
    });
    expect(harness.session.snapshot()).toMatchObject({ connected: true });
    await harness.network.flushAll();
    const ackAfterFailure = Math.max(
      ...harness.hostEnvelopes.map(envelope => envelope.ack.sequence),
    );
    expect(ackAfterFailure).toBe(harness.sidecarSequence - 1);

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

  it("retains its reliable flight across a reconnectable Awareness publish failure", async () => {
    const fault: ReconnectableFault = { armed: false, failed: false, reconnects: 0 };
    const harness = new TransportHarness({ reconnectableFault: fault });
    await harness.start();
    fault.armed = true;
    const pending = harness.session.run(BODY);
    void pending.catch(() => undefined);
    await Bun.sleep(harness.publicationIntervalMs + 5);
    await harness.network.flushAll();

    expect(fault.failed).toBe(true);
    expect(harness.session.snapshot()).toMatchObject({
      connected: true,
      credits: 0,
    });
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_open"))
      .toBe(false);

    await harness.session.resumeAwareness();
    await harness.settle();
    expect(fault.reconnects).toBe(1);
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit"))
      .toBe(true);
    const handle = await harness.commit(pending);
    harness.publish(await harness.completedFrames());
    await harness.settle();
    await expect(handle.final).resolves.toMatchObject({ status: "completed" });
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

  it("hands admission and R1 execution-risk ownership off exactly once", async () => {
    const harness = new TransportHarness();
    await harness.start();
    let reflected = 0;
    let executionRisks = 0;
    const pending = harness.session.run(BODY, undefined, {
      onAdmissionReflected: () => { reflected++; },
      onExecutionRiskReflected: () => { executionRisks++; },
    });
    await harness.settle();

    expect(reflected).toBe(1);
    expect(executionRisks).toBe(1);
    expect(harness.session.snapshot()).toMatchObject({
      active: 1,
      admissionCredits: 3,
    });
    await harness.settle();
    expect(reflected).toBe(1);
    expect(executionRisks).toBe(1);
    await harness.close();
    await Promise.allSettled([pending]);
  });

  it("never hands ownership off when a request fails before admission", async () => {
    const harness = new TransportHarness({ unadmittedTimeoutMs: 50 });
    await harness.start();
    harness.credits = 0;
    harness.publish([], { lane: "control" });
    await harness.settle();
    const controller = new AbortController();
    let reflected = 0;
    const pending = harness.session.run(BODY, controller.signal, {
      onAdmissionReflected: () => { reflected++; },
    });
    await harness.settle();
    expect(reflected).toBe(0);
    controller.abort();
    await expect(pending).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
      committed: false,
    });
    expect(reflected).toBe(0);
    await harness.close();
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
      await harness.settleUntil(() => (
        harness.hostEnvelopes.some(value => value.lane === "data")
      ));
      const requestFlight = harness.hostEnvelopes.find(value => value.lane === "data");
      expect(requestFlight?.frames.length).toBeGreaterThan(0);

      harness.publish([{
        kind: "pong",
        sentAtMs: 1,
      }], { lane: "data", ackSequence: -1 });
      await harness.settleUntil(() => (
        harness.hostEnvelopes.some(value => value.lane === "control")
      ));

      const control = harness.hostEnvelopes.find(value => value.lane === "control");
      expect(control?.frames).toEqual([]);
      expect(JSON.stringify(control).length * 4).toBeLessThan(JSON.stringify(requestFlight).length);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("replays the processed ACK when a duplicate sidecar flight proves its ACK was lost", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    harness.publish([{
      kind: "pong",
      sentAtMs: 1,
    }], { lane: "data" });
    const duplicateFrame = structuredClone(harness.sidecarOutstanding[0]!);
    await harness.settleUntil(() => (
      harness.hostEnvelopes.some(value => value.ack.sequence === duplicateFrame.sequence)
    ));
    harness.hostEnvelopes.length = 0;

    const duplicateEnvelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: BINDING.schemaHash,
      buildHash: BINDING.buildHash,
      projectId: PROJECT,
      sessionId: BINDING.sessionId,
      peerId: "sidecar",
      instanceId: harness.sidecarInstanceId,
      leaderGeneration: BINDING.leaderGeneration,
      epoch: harness.sidecarEpoch,
      revision: harness.revision++,
      ack: { epoch: 1, sequence: harness.hostAck },
      credits: harness.credits,
      active: 0,
      queued: 0,
      draining: false,
      frames: [duplicateFrame],
      lane: "data",
    };
    harness.sidecar.publish(duplicateEnvelope);
    await harness.settle();

    expect(harness.hostEnvelopes).toHaveLength(1);
    expect(harness.hostEnvelopes[0]).toMatchObject({
      lane: "control",
      ack: { epoch: duplicateFrame.epoch, sequence: duplicateFrame.sequence },
      frames: [],
    });
    harness.hostEnvelopes.length = 0;
    harness.sidecar.publish(duplicateEnvelope);
    await harness.settle();
    expect(harness.hostEnvelopes).toHaveLength(0);
    await harness.close();
  });

  it("freezes a bounded request flight until its high sequence is acknowledged", async () => {
    const harness = new TransportHarness({ publicationIntervalMs: 1 });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run({ ...BODY, input: "large-flight-".repeat(40_000) });
    try {
      await harness.settleUntil(() => (
        harness.hostEnvelopes.some(value => value.lane === "data")
      ));

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
      await harness.settleUntil(() => (
        harness.hostEnvelopes.filter(value => value.lane === "data").length > 1
      ));
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
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
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
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));

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

  it("bounds a concurrent 128 KiB request to one standard flight during a 13 MiB bulk transfer", async () => {
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
      publicationIntervalMs: 1,
      unadmittedTimeoutMs: 10_000,
    });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const large = harness.session.run(createExactSizedV2Request(
      13 * 1024 ** 2,
      1,
      "incompressible",
    ));
    const smallController = new AbortController();
    let small: Promise<RunHandle> | null = null;
    try {
      await harness.settleUntil(() => (
        harness.hostEnvelopes.some(envelope => (
          envelope.lane === "data" && envelope.frames.length > 32
        ))
      ), 10_000);
      const first = harness.hostEnvelopes.find(envelope => (
        envelope.lane === "data" && envelope.frames.length > 32
      ))!;
      expect(first.frames).toHaveLength(64);
      expect(first.frames.reduce(
        (bytes, frame) => bytes + utf8Bytes(JSON.stringify(frame)).byteLength,
        0,
      )).toBeGreaterThan(512 * 1024);

      small = harness.session.run(
        createExactSizedV2Request(128 * 1024, 2, "incompressible"),
        smallController.signal,
      );
      await harness.settleUntil(() => harness.session.snapshot().active === 2, 5_000);
      const beforeProtectedFlight = harness.hostEnvelopes.filter(
        envelope => envelope.lane === "data",
      ).length;
      harness.publish([], {
        lane: "control",
        ackSequence: first.frames.at(-1)!.sequence,
      });
      await harness.settleUntil(() => (
        harness.hostEnvelopes.filter(envelope => envelope.lane === "data").length >
          beforeProtectedFlight
      ), 5_000);
      const protectedFlight = harness.hostEnvelopes.filter(
        envelope => envelope.lane === "data",
      )[beforeProtectedFlight]!;
      expect(protectedFlight.frames.length).toBeLessThanOrEqual(32);
      const smallOpen = protectedFlight.frames.find(
        (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
          frame.kind === "request_open" && frame.decodedBytes === 128 * 1024
        ),
      );
      expect(smallOpen).toBeDefined();
      expect(protectedFlight.frames.some(frame => (
        frame.kind === "request_commit" && frame.requestId === smallOpen!.requestId
      ))).toBe(true);

      smallController.abort();
      await expect(small).rejects.toMatchObject({
        name: "V2PreCommitError",
        code: "cancelled_precommit",
        committed: false,
      });
      await harness.settleUntil(() => harness.session.snapshot().active === 1, 5_000);
      const beforeBulkResume = harness.hostEnvelopes.filter(
        envelope => envelope.lane === "data",
      ).length;
      harness.publish([], {
        lane: "control",
        ackSequence: protectedFlight.frames.at(-1)!.sequence,
      });
      await harness.settleUntil(() => (
        harness.hostEnvelopes.filter(envelope => envelope.lane === "data").length >
          beforeBulkResume
      ), 5_000);
      const resumedBulk = harness.hostEnvelopes.filter(
        envelope => envelope.lane === "data",
      )[beforeBulkResume]!;
      expect(resumedBulk.frames.length).toBeGreaterThan(32);
      expect(resumedBulk.frames.length).toBeLessThanOrEqual(64);
    } finally {
      smallController.abort();
      await harness.close();
      await Promise.allSettled(small ? [large, small] : [large]);
    }
  }, 30_000);

  it("falls medium transfers back after loss and restores acceleration only after two clean flights", async () => {
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
      publicationIntervalMs: 1,
      retransmitMs: 100,
      unadmittedTimeoutMs: 10_000,
    });
    await harness.start();
    harness.hostEnvelopes.length = 0;
    const pending = harness.session.run(createExactSizedV2Request(
      2 * 1024 ** 2,
      3,
      "incompressible",
    ));
    const dataFlights = () => harness.hostEnvelopes.filter(
      envelope => envelope.lane === "data" && envelope.frames.length > 0,
    );
    const acknowledgeAndTakeNext = async (
      flight: AwarenessEnvelope<HostFrame>,
    ): Promise<AwarenessEnvelope<HostFrame>> => {
      const before = dataFlights().length;
      harness.publish([], {
        lane: "control",
        ackSequence: flight.frames.at(-1)!.sequence,
      });
      await harness.settleUntil(() => dataFlights().length > before, 2_000);
      return dataFlights()[before]!;
    };

    try {
      await harness.settleUntil(() => dataFlights().length >= 1, 2_000);
      const first = dataFlights()[0]!;
      expect(first.frames).toHaveLength(64);
      await harness.settleUntil(() => dataFlights().length >= 2, 2_000);
      const retransmission = dataFlights()[1]!;
      expect(retransmission.frames.map(frame => frame.sequence))
        .toEqual(first.frames.map(frame => frame.sequence));

      const firstRecovery = await acknowledgeAndTakeNext(first);
      expect(firstRecovery.frames.length).toBeLessThanOrEqual(32);
      const secondRecovery = await acknowledgeAndTakeNext(firstRecovery);
      expect(secondRecovery.frames.length).toBeLessThanOrEqual(32);
      const acceleratedAgain = await acknowledgeAndTakeNext(secondRecovery);
      expect(acceleratedAgain.frames.length).toBeGreaterThan(32);
      expect(acceleratedAgain.frames.length).toBeLessThanOrEqual(64);
      expect(harness.session.snapshot().retransmissions).toBeGreaterThanOrEqual(1);
    } finally {
      await harness.close();
      await Promise.allSettled([pending]);
    }
  }, 10_000);

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

  it("routes with acknowledged request goodput rather than cumulative publications", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run({
      ...BODY,
      input: "acknowledged-goodput-".repeat(4_096),
    }));
    const diagnostics = harness.session.snapshot().diagnostics!.host;
    expect(diagnostics.acknowledgedGoodputBytesPerSecond).toBeGreaterThan(0);
    expect(diagnostics.uniqueAcknowledgedBytesTotal).toBeGreaterThan(0);
    expect(diagnostics.retransmittedBytesTotal).toBe(0);
    expect(diagnostics.stallEventsTotal).toBe(0);
    expect(handle).toBeDefined();
    await harness.close();
  });

  it("fails admission closed and fast-retransmits when a live peer leaves an ACK stale", async () => {
    const harness = new TransportHarness({
      retransmitMs: 10_000,
      transportStallMs: 30,
    });
    await harness.start();
    const pending = harness.session.run({ ...BODY, input: "stalled-flight" });
    await harness.settle();
    await Bun.sleep(50);
    await harness.settle();

    expect(harness.session.snapshot().admissionCredits).toBe(0);
    expect(harness.session.snapshot().diagnostics!.host.stallEventsTotal).toBe(1);
    expect(harness.stateChanges).toBeGreaterThan(0);
    expect(harness.hostEnvelopes.filter(envelope => envelope.lane === "control").at(-1))
      .toMatchObject({ credits: 1 });

    const before = harness.session.snapshot().retransmissions;
    harness.publish([], { lane: "control", ackSequence: -1 });
    await harness.settle();
    expect(harness.session.snapshot().retransmissions).toBeGreaterThan(before);
    expect(harness.session.snapshot().diagnostics!.host.retransmittedBytesTotal)
      .toBeGreaterThan(0);

    await harness.close();
    await expect(pending).rejects.toBeDefined();
  });

  it("rejects over-deep direct transport bodies before recursive canonicalization", async () => {
    const harness = new TransportHarness({ binding: R2_BINDING });
    await harness.start();
    harness.hostFrames.clear();
    let input: Record<string, unknown> = { leaf: true };
    for (let depth = 0; depth <= MAX_RESPONSES_JSON_DEPTH + 10_000; depth++) {
      input = { nested: input };
    }

    await expect(harness.session.run({ model: "gpt-5.6-sol", input }))
      .rejects.toBeInstanceOf(ResponsesInputStructureError);
    expect(harness.hostFrames.size).toBe(0);
    expect(harness.session.snapshot()).toMatchObject({ connected: true, active: 0, queued: 0 });
    await harness.close();
  });

  it("matches the shared depth boundary for top-level input items", async () => {
    const nested = (containers: number): unknown => {
      let value: unknown = "leaf";
      for (let depth = 0; depth < containers; depth++) value = { nested: value };
      return value;
    };
    const harness = new TransportHarness({ binding: R2_BINDING });
    await harness.start();
    harness.hostFrames.clear();

    await expect(harness.session.run({
      model: "gpt-5.6-sol",
      input: [nested(MAX_RESPONSES_JSON_DEPTH + 2)],
    })).rejects.toBeInstanceOf(ResponsesInputStructureError);
    expect(harness.hostFrames.size).toBe(0);

    const controller = new AbortController();
    const accepted = harness.session.run({
      model: "gpt-5.6-sol",
      input: [nested(MAX_RESPONSES_JSON_DEPTH + 1)],
    }, controller.signal);
    void accepted.catch(() => undefined);
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
    controller.abort();
    await expect(accepted).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
    });
    await harness.close();
  });

  it("yields during large direct serialization so a zero-delay abort wins before admission", async () => {
    const harness = new TransportHarness({ binding: R2_BINDING });
    await harness.start();
    harness.hostFrames.clear();
    const controller = new AbortController();
    let timerFired = false;
    const timer = setTimeout(() => {
      timerFired = true;
      controller.abort();
    }, 0);

    try {
      await expect(harness.session.run({
        model: "gpt-5.6-sol",
        input: "x".repeat(8 * 1024 * 1024),
      }, controller.signal)).rejects.toMatchObject({
        name: "V2PreCommitError",
        code: "cancelled_precommit",
      });
      expect(timerFired).toBe(true);
      expect(harness.hostFrames.size).toBe(0);
      expect(harness.session.snapshot()).toMatchObject({ active: 0, queued: 0 });
    } finally {
      clearTimeout(timer);
      await harness.close();
    }
  });

  it("bounds concurrent direct request preparation before aggregate serialization grows", async () => {
    let activePreparations = 0;
    let peakPreparations = 0;
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
      gzip: async (_bytes, options) => {
        activePreparations++;
        peakPreparations = Math.max(peakPreparations, activePreparations);
        return new Promise<Uint8Array>((_resolve, reject) => {
          const signal = options?.signal;
          const onAbort = () => {
            activePreparations--;
            signal?.removeEventListener("abort", onAbort);
            reject(signal?.reason ?? new DOMException("aborted", "AbortError"));
          };
          signal?.addEventListener("abort", onAbort, { once: true });
        });
      },
    });
    await harness.start();
    const controllers = Array.from({ length: 7 }, () => new AbortController());
    const body = {
      model: "gpt-5.6-sol",
      input: "bounded-preparation-".padEnd(2 * 1024 * 1024, "x"),
    };
    const pending = controllers.map(controller => harness.session.run(body, controller.signal));
    pending.forEach(value => void value.catch(() => undefined));

    await harness.settleUntil(() => activePreparations === 4);
    expect(peakPreparations).toBe(4);
    for (const queued of pending.slice(4)) {
      expect(await promiseState(queued)).toBe("pending");
    }
    for (const controller of controllers) controller.abort();
    const settled = await Promise.allSettled(pending);
    expect(settled.every(result => (
      result.status === "rejected" && result.reason?.name === "V2PreCommitError" &&
        result.reason?.code === "cancelled_precommit"
    ))).toBe(true);
    expect(harness.session.snapshot()).toMatchObject({ active: 0, queued: 0 });
    await harness.close();
  });

  it("times out a noncooperative active preparation without hanging its caller", async () => {
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
      unadmittedTimeoutMs: 20,
      gzip: async () => new Promise<Uint8Array>(() => undefined),
    });
    await harness.start();
    const started = performance.now();
    await expect(harness.session.run({
      model: "gpt-5.6-sol",
      input: "noncooperative-preparation-".padEnd(2 * 1024 * 1024, "x"),
    })).rejects.toMatchObject({ committed: false, code: "capacity_changed" });
    expect(performance.now() - started).toBeLessThan(100);
    await harness.close();
    expect(harness.session.snapshot().queued).toBe(0);
  });

  it("does not continue full compression after a timed-out sample returns", async () => {
    let resolveSample!: (bytes: Uint8Array) => void;
    let gzipCalls = 0;
    const sample = new Promise<Uint8Array>(resolve => { resolveSample = resolve; });
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
      unadmittedTimeoutMs: 20,
      gzip: async bytes => {
        gzipCalls++;
        if (gzipCalls === 1) return sample;
        return bytes;
      },
    });
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "late-sample-".padEnd(2 * 1024 * 1024, "x"),
    });
    await expect(pending).rejects.toMatchObject({ code: "capacity_changed" });
    resolveSample(new Uint8Array(1));
    await Bun.sleep(20);
    expect(gzipCalls).toBe(1);
    await harness.close();
  });

  it("aborts active and queued preparation when the transport closes", async () => {
    let preparationStarted!: () => void;
    const started = new Promise<void>(resolve => { preparationStarted = resolve; });
    const harness = new TransportHarness({
      binding: R2_BINDING,
      gzip: async (_bytes, options) => {
        preparationStarted();
        return new Promise<Uint8Array>((_resolve, reject) => {
          const signal = options?.signal;
          const onAbort = () => reject(signal?.reason ?? new DOMException("aborted", "AbortError"));
          signal?.addEventListener("abort", onAbort, { once: true });
        });
      },
    });
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "close-preparation-".padEnd(2 * 1024 * 1024, "x"),
    });
    void pending.catch(() => undefined);
    await started;

    const closing = harness.session.close();
    await expect(pending).rejects.toMatchObject({ committed: false, code: "draining" });
    await closing;
    expect(harness.session.snapshot()).toMatchObject({ connected: false, active: 0, queued: 0 });
    await harness.sidecar.close();
  });

  it("rejects cyclic, non-finite, and unsupported direct canonical values", async () => {
    const harness = new TransportHarness({ binding: R2_BINDING });
    await harness.start();
    harness.hostFrames.clear();
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const cases: Array<[Record<string, unknown>, string]> = [
      [{ model: "gpt-5.6-sol", input: cyclic }, "cycles"],
      [{ model: "gpt-5.6-sol", input: Number.POSITIVE_INFINITY }, "finite"],
      [{ model: "gpt-5.6-sol", input: undefined }, "undefined"],
    ];

    for (const [body, message] of cases) {
      await expect(harness.session.run(body)).rejects.toThrow(message);
    }
    expect(harness.hostFrames.size).toBe(0);
    expect(harness.session.snapshot()).toMatchObject({ active: 0, queued: 0 });
    await harness.close();
  });

  it("expands response batches into the exact logical SSE transcript", async () => {
    const harness=new TransportHarness();await harness.start();const handle=await harness.commit(harness.session.run(BODY));const id=harness.requestIdentity();
    const final={id:"resp_batch",object:"response",status:"completed",output:[]};const events=[{event:"response.created",data:JSON.stringify({type:"response.created",response:{id:"resp_batch"}})},{event:"response.output_text.delta",data:JSON.stringify({type:"response.output_text.delta",delta:"hello"})},{event:"response.completed",data:JSON.stringify({type:"response.completed",response:final})}];const proof=new TranscriptProof(id.requestId,id.requestDigest);for(const event of events)proof.append(event);const terminal=await proof.createTerminal();
    harness.publish([{kind:"response_batch",requestId:id.requestId,firstEventIndex:0,items:events.map(event=>({event:event.event,dataBase64Url:base64UrlEncode(utf8Bytes(event.data))}))},terminal]);await harness.settle();const received=[];for await(const event of handle.events())received.push(event);expect(received).toEqual(events);await expect(handle.final).resolves.toMatchObject({id:"resp_batch"});await harness.close();
  });

  it("backpressures a 768-event batched burst without ambiguity or loss", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const identity = harness.requestIdentity();
    const final = { id: "resp_burst", object: "response", status: "completed", output: [] };
    const events = Array.from({ length: 768 }, (_, index) => index === 767
      ? {
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: final }),
        }
      : {
          event: "response.output_text.delta",
          data: JSON.stringify({ type: "response.output_text.delta", delta: String(index) }),
        });
    const proof = new TranscriptProof(identity.requestId, identity.requestDigest);
    for (const event of events) proof.append(event);
    const batches = Array.from({ length: 24 }, (_, batchIndex) => ({
      kind: "response_batch" as const,
      requestId: identity.requestId,
      firstEventIndex: batchIndex * 32,
      items: events.slice(batchIndex * 32, batchIndex * 32 + 32).map(event => ({
        event: event.event,
        dataBase64Url: base64UrlEncode(utf8Bytes(event.data)),
      })),
    }));

    harness.publish([...batches, await proof.createTerminal()]);
    // Let the data path reach the bounded event-count boundary before attaching the
    // consumer. The request must remain committed and pending, not ambiguous.
    await harness.settle();
    expect(await promiseState(handle.final)).toBe("pending");
    expect(harness.session.snapshot()).toMatchObject({
      connected: true,
      quarantined: 0,
      ambiguousQuarantined: 0,
    });

    const received = await Promise.race([
      Array.fromAsync(handle.events()),
      Bun.sleep(2_000).then(() => { throw new Error("batched burst remained backpressured"); }),
    ]);
    await harness.settleUntil(() => harness.sidecarOutstanding.length === 0);
    expect(received).toEqual(events);
    await expect(handle.final).resolves.toMatchObject({ id: "resp_burst" });
    expect(harness.session.snapshot()).toMatchObject({
      quarantined: 0,
      ambiguousQuarantined: 0,
    });
    await harness.close();
  });

  it("advertises bulk body prefetch only after exact medium admission", async () => {
    const harness = new TransportHarness({
      binding: R2_BINDING,
      transportProfile: "bulk-v1",
    });
    await harness.start();
    expect(harness.session.snapshot()).toMatchObject({
      admissionCredits: 4,
      prefetchCredits: 4,
    });

    const controller = new AbortController();
    const pending = harness.session.run(
      createExactSizedV2Request(512 * 1024, 19, "incompressible"),
      controller.signal,
    );
    try {
      await harness.settleUntil(() => harness.session.snapshot().active === 1);
      expect(harness.session.snapshot()).toMatchObject({
        admissionCredits: 3,
        prefetchCredits: 5,
      });
    } finally {
      controller.abort();
      await harness.close();
      await Promise.allSettled([pending]);
    }
  });

  it("admits at most four uncommitted requests per sandbox", async () => {
    const harness=new TransportHarness();await harness.start();
    const pending=Array.from({length:5},(_,i)=>harness.session.run({...BODY,input:`request-${i}`}));
    await harness.settle();expect(harness.session.snapshot()).toMatchObject({admissionCredits:0,queued:1});
    await harness.close();await Promise.allSettled(pending);
  });
  it("admits four exact 2 MiB transfers concurrently but never two true bulk transfers", async () => {
    const harness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});await harness.start();
    const medium=createExactSizedV2Request(2*1024*1024,10,"incompressible");
    const controllers=Array.from({length:5},()=>new AbortController());
    const pending=controllers.map(controller=>harness.session.run(medium,controller.signal));
    await harness.settleUntil(() => harness.session.snapshot().prefetched === 1);
    expect(harness.session.snapshot()).toMatchObject({
      admissionCredits: 0,
      active: 4,
      prefetched: 1,
      queued: 1,
    });
    controllers[0]!.abort();
    await expect(pending[0]!).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
    });
    await harness.settle();
    expect(harness.session.snapshot()).toMatchObject({active:4,queued:0});
    await harness.close();await Promise.allSettled(pending);

    const observedHarness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});await observedHarness.start();
    const observed=createExactSizedV2Request(3*1024*1024,11,"incompressible");
    const observedPending=Array.from({length:4},()=>observedHarness.session.run(observed));
    await observedHarness.settleUntil(() => observedHarness.session.snapshot().active === 3);
    expect(observedHarness.session.snapshot()).toMatchObject({active:3,queued:1});
    await observedHarness.close();await Promise.allSettled(observedPending);

    const bulkHarness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});await bulkHarness.start();
    const bulk=createExactSizedV2Request(3*1024*1024+1,12,"incompressible");
    const bulkPending=[bulkHarness.session.run(bulk),bulkHarness.session.run(bulk)];
    await bulkHarness.settleUntil(() => bulkHarness.session.snapshot().active === 1);
    expect(bulkHarness.session.snapshot()).toMatchObject({active:1,queued:1});
    await bulkHarness.close();await Promise.allSettled(bulkPending);
  });
  it("keeps the R5 tiny-request precommit ceiling at four", async () => {
    const harness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});
    await harness.start();
    const controllers=Array.from({length:6},()=>new AbortController());
    const pending=controllers.map((controller,index)=>harness.session.run(
      createExactSizedV2Request(128*1024,20+index,"incompressible"),
      controller.signal,
    ));
    try {
      for(let round=0;round<12;round++){
        if([...harness.hostFrames.values()].filter(frame=>frame.kind==="request_commit").length===4)break;
        harness.publish([],{lane:"control"});
        await harness.settle();
      }
      expect(harness.session.snapshot()).toMatchObject({
        active:4,
        queued:2,
        admissionBlockReason:"execution_window",
      });
      expect([...harness.hostFrames.values()].filter(frame=>frame.kind==="request_commit"))
        .toHaveLength(4);
    }finally{
      for(const controller of controllers)controller.abort();
      await harness.close();
      await Promise.allSettled(pending);
    }
  });
  it("does not admit a late tiny body through medium-only prefetch credits", async () => {
    const harness = new TransportHarness({ binding: R2_BINDING, transportProfile: "bulk-v1" });
    await harness.start();
    const controllers = Array.from({ length: 5 }, () => new AbortController());
    const medium = createExactSizedV2Request(512 * 1024, 70, "incompressible");
    const pending = controllers.slice(0, 4).map(controller =>
      harness.session.run(medium, controller.signal));
    try {
      await harness.settleUntil(() => harness.session.snapshot().active === 4);
      pending.push(harness.session.run(
        createExactSizedV2Request(128 * 1024, 71, "incompressible"),
        controllers[4]!.signal,
      ));
      await harness.settle();
      expect(harness.session.snapshot()).toMatchObject({ active: 4, queued: 1 });
      expect([...harness.hostFrames.values()].filter(frame => (
        frame.kind === "request_open" && frame.decodedBytes === 128 * 1024
      ))).toHaveLength(0);
    } finally {
      for (const controller of controllers) controller.abort();
      await harness.close();
      await Promise.allSettled(pending);
    }
  });
  it("admits and frames a queued tiny R5 request ahead of medium prefetch", async () => {
    const harness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});
    await harness.start();
    harness.credits=0;
    harness.publish([],{lane:"control"});
    await harness.settle();
    const controllers=Array.from({length:7},()=>new AbortController());
    const pending=[
      ...controllers.slice(0,6).map((controller,index)=>harness.session.run(
        createExactSizedV2Request(512*1024,40+index,"incompressible"),
        controller.signal,
      )),
      harness.session.run(
        createExactSizedV2Request(128*1024,46,"incompressible"),
        controllers[6]!.signal,
      ),
    ];
    try {
      await harness.settleUntil(()=>harness.session.snapshot().queued===7);
      harness.credits=128;
      harness.publish([],{lane:"control"});
      await harness.settleUntil(()=>(
        [...harness.hostFrames.values()].some(frame=>frame.kind==="request_open")
      ));
      const firstOpen=[...harness.hostFrames.values()].find(
        (frame):frame is Extract<ReliableFrame<HostFrame>,{kind:"request_open"}>=>(
          frame.kind==="request_open"
        ),
      );
      expect(firstOpen?.decodedBytes).toBe(128*1024);
      expect(harness.session.snapshot()).toMatchObject({active:4,queued:3,prefetched:0});
    }finally{
      for(const controller of controllers)controller.abort();
      await harness.close();
      await Promise.allSettled(pending);
    }
  });
  it("prefetches six R5 medium bodies while reserving at most four execution starts", async () => {
    const harness=new TransportHarness({binding:R2_BINDING,transportProfile:"bulk-v1"});
    await harness.start();
    const controllers=Array.from({length:6},()=>new AbortController());
    const pending=controllers.map((controller,index)=>harness.session.run(
      createExactSizedV2Request(512*1024,30+index,"incompressible"),
      controller.signal,
    ));
    const hostFrames=()=>[...harness.hostFrames.values()];
    try {
      for(let round=0;round<24;round++){
        if(hostFrames().filter(frame=>frame.kind==="request_commit").length===6)break;
        harness.publish([],{lane:"control"});
        await harness.settle();
      }
      const commits=hostFrames().filter(
        (frame):frame is ReliableFrame<R2RequestCommitFrame>=>(
          frame.kind==="request_commit" &&
          "protocolRevision" in frame && frame.protocolRevision==="r2"
        ),
      );
      expect(commits).toHaveLength(6);
      harness.publish(commits.map(commit=>({...commit,kind:"request_prepared" as const})));
      await harness.settleUntil(()=>(
        hostFrames().filter(frame=>frame.kind==="request_start").length===4
      ));
      await harness.settle();
      let starts=hostFrames().filter(
        (frame):frame is Extract<ReliableFrame<HostFrame>,{kind:"request_start"}>=>(
          frame.kind==="request_start"
        ),
      );
      expect(starts).toHaveLength(4);

      let acknowledgedStarts=0;
      for(const expectedStarts of [5,6]){
        const acknowledged=starts[acknowledgedStarts++]!;
        harness.publish([{
          kind:"request_ack",
          requestId:acknowledged.requestId,
          requestDigest:acknowledged.requestDigest,
          committed:true,
        }]);
        await harness.settleUntil(()=>(
          hostFrames().filter(frame=>frame.kind==="request_start").length===expectedStarts
        ));
        starts=hostFrames().filter(
          (frame):frame is Extract<ReliableFrame<HostFrame>,{kind:"request_start"}>=>(
            frame.kind==="request_start"
          ),
        );
        expect(starts.length-acknowledgedStarts).toBeLessThanOrEqual(4);
      }
      expect(starts).toHaveLength(6);
    }finally{
      for(const controller of controllers)controller.abort();
      await harness.close();
      await Promise.allSettled(pending);
    }
  },20_000);
  it("keeps snapshot bookkeeping O(1) with 128 queued requests", async () => {
    const idle=new TransportHarness();
    const loaded=new TransportHarness();
    await idle.start();
    await loaded.start();
    loaded.credits=0;
    loaded.publish([],{lane:"control"});
    await loaded.settle();
    const controllers=Array.from({length:128},()=>new AbortController());
    const pending=controllers.map((controller,index)=>loaded.session.run(
      {...BODY,input:`snapshot-load-${index}`},
      controller.signal,
    ));
    const median=(values:number[])=>{
      const sorted=[...values].sort((left,right)=>left-right);
      return sorted[Math.floor(sorted.length/2)]!;
    };
    const measure=(harness:TransportHarness)=>{
      let checksum=0;
      for(let index=0;index<10_000;index++)checksum+=harness.session.snapshot().queued;
      const samples:number[]=[];
      for(let round=0;round<5;round++){
        const started=performance.now();
        for(let index=0;index<75_000;index++)checksum+=harness.session.snapshot().queued;
        samples.push(performance.now()-started);
      }
      return {elapsed:median(samples),checksum};
    };
    try {
      await loaded.settleUntil(()=>loaded.session.snapshot().queued===128,10_000);
      const idleCost=measure(idle);
      const loadedCost=measure(loaded);
      expect(idleCost.checksum).toBe(0);
      expect(loadedCost.checksum).toBeGreaterThan(0);
      expect(loadedCost.elapsed).toBeLessThan(idleCost.elapsed*3);
    }finally{
      for(const controller of controllers)controller.abort();
      await loaded.close();
      await idle.close();
      await Promise.allSettled(pending);
    }
  },30_000);
  it("does not eagerly clone transport diagnostics for every queued request", async () => {
    const peak=spyOn(HostRequestDiagnosticsObserver.prototype,"peak");
    const harness=new TransportHarness();
    await harness.start();
    harness.credits=0;
    harness.publish([],{lane:"control"});
    await harness.settle();
    const controllers=Array.from({length:128},()=>new AbortController());
    const pending=controllers.map((controller,index)=>harness.session.run(
      {...BODY,input:`diagnostic-fanout-${index}`},
      controller.signal,
    ));
    try {
      await harness.settleUntil(()=>harness.session.snapshot().queued===128,10_000);
      peak.mockClear();
      harness.session.setHostCeiling(127);
      await Bun.sleep(0);
      expect(peak).toHaveBeenCalledTimes(0);
    }finally{
      peak.mockRestore();
      for(const controller of controllers)controller.abort();
      await harness.close();
      await Promise.allSettled(pending);
    }
  },20_000);
  it("derives retransmission delay from the RTT floor plus bounded jitter", () => {
    expect(computeV2RetransmitDelayMs(null, 0)).toBe(1_500);
    expect(computeV2RetransmitDelayMs(200, 0)).toBe(400);
    expect(computeV2RetransmitDelayMs(200, 1)).toBeGreaterThan(400);
    expect(computeV2RetransmitDelayMs(200, 1)).toBeLessThanOrEqual(440);
    expect(computeV2RetransmitDelayMs(null, 0, 250, 1)).toBe(3_000);
    expect(computeV2RetransmitDelayMs(null, 0, 250, 20)).toBe(5_000);
    expect(() => computeV2RetransmitDelayMs(null, 0, 250, 0, 100))
      .toThrow("initialRtoMs");
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
      expect(snapshot.mock.results[0]?.value).toMatchObject({
        canonicalBytes: null,
        retainsBytes: false,
      });
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

  it("withholds the cumulative ACK until a full client buffer has space", async () => {
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

    expect(await promiseState(handle.final)).toBe("pending");
    const events = handle.events();
    await expect(events.next()).resolves.toMatchObject({ done: false });
    await harness.settle();
    expect(Math.max(...harness.hostEnvelopes.map(envelope => envelope.ack.sequence))).toBe(2);
    await events.return(undefined);
    await harness.close();
  });

  it("tears down promptly while reliable delivery is waiting for client space", async () => {
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

    await expect(Promise.race([
      harness.close(),
      Bun.sleep(250).then(() => { throw new Error("transport close remained backpressured"); }),
    ])).resolves.toBeUndefined();
    await expect(handle.final).rejects.toMatchObject({ committed: true, code: "closed" });
  });

  it("shares one event pool across sessions without letting slow A block B", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 3 * 1024 * 1024,
      jumboBytes: 8 * 1024 * 1024,
    });
    const a = new TransportHarness({
      maxResponseBytes: 4_096,
      maxBufferedResponseBytes: 160,
      eventMemoryPool: pool,
    });
    const b = new TransportHarness({
      maxResponseBytes: 4_096,
      maxBufferedResponseBytes: 160,
      eventMemoryPool: pool,
    });
    await a.start();
    await b.start();
    const handleA = await a.commit(a.session.run(BODY));
    const handleB = await b.commit(b.session.run(BODY));
    const idA = a.requestIdentity().requestId;
    const idB = b.requestIdentity().requestId;
    const response = (requestId: string, eventIndex: number) => ({
      kind: "response_event" as const,
      requestId,
      eventIndex,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("x".repeat(100))),
    });

    a.publish([response(idA, 0)]);
    b.publish([response(idB, 0)]);
    await a.settle();
    await b.settle();
    expect(pool.snapshot().activePermits).toBe(2);

    a.publish([response(idA, 1)]);
    await a.settle();
    expect(await promiseState(handleA.final)).toBe("pending");
    expect(b.session.snapshot().connected).toBe(true);
    expect(pool.snapshot().activePermits).toBe(3);

    const eventsB = handleB.events();
    await expect(eventsB.next()).resolves.toMatchObject({
      value: { event: "response.output_text.delta" },
      done: false,
    });
    expect(pool.snapshot().activePermits).toBe(3);
    await eventsB.return(undefined);
    expect(pool.snapshot().activePermits).toBe(2);

    const eventsA = handleA.events();
    await expect(eventsA.next()).resolves.toMatchObject({ done: false });
    await a.settle();
    await expect(eventsA.next()).resolves.toMatchObject({ done: false });
    await eventsA.return(undefined);
    expect(pool.snapshot().activePermits).toBe(0);

    await a.close();
    await b.close();
    pool.close();
  });

  it("releases an incomplete event assembly permit on session close", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 2 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
    });
    const harness = new TransportHarness({
      maxResponseBytes: 4_096,
      eventMemoryPool: pool,
    });
    await harness.start();
    await harness.commit(harness.session.run(BODY));
    const requestId = harness.requestIdentity().requestId;
    harness.publish([{
      kind: "response_event",
      requestId,
      eventIndex: 0,
      event: "response.output_text.delta",
      partIndex: 0,
      partCount: 2,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("partial")),
    }]);
    await harness.settle();
    expect(pool.snapshot().activePermits).toBe(1);
    await harness.close();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("promotes an exact 600 KiB segmented event to jumbo before RunHandle transfer", async () => {
    const targetWireBytes = 600 * 1024;
    const eventName = "response.output_text.delta";
    const emptyData = JSON.stringify({ type: eventName, delta: "" });
    const fillBytes = targetWireBytes - utf8Bytes(sseFrame(eventName, emptyData)).byteLength;
    const data = JSON.stringify({ type: eventName, delta: "x".repeat(fillBytes) });
    expect(utf8Bytes(sseFrame(eventName, data)).byteLength).toBe(targetWireBytes);

    const pool = new AtomicBytePermitPool({
      // Deliberately leave enough ordinary space to reproduce the bug where a
      // normal resize never moved the completed event into the jumbo lane.
      ordinaryBytes: 12 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
    });
    const harness = new TransportHarness({
      maxResponseBytes: 2 * 1024 * 1024,
      eventMemoryPool: pool,
    });
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const identity = harness.requestIdentity();
    const final = { id: "resp_jumbo", object: "response", status: "completed", output: [] };
    const completed = {
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response: final }),
    };
    const transcript = new TranscriptProof(identity.requestId, identity.requestDigest);
    transcript.append({ event: eventName, data });
    transcript.append(completed);
    const terminal = await transcript.createTerminal();

    const bytes = utf8Bytes(data);
    const partBytes = 8 * 1024;
    const partCount = Math.ceil(bytes.byteLength / partBytes);
    const parts: SidecarFrame[] = Array.from({ length: partCount }, (_, partIndex) => {
      const offset = partIndex * partBytes;
      return {
        kind: "response_event",
        requestId: identity.requestId,
        eventIndex: 0,
        event: eventName,
        partIndex,
        partCount,
        offset,
        dataBase64Url: base64UrlEncode(bytes.subarray(offset, offset + partBytes)),
      };
    });
    for (let offset = 0; offset < parts.length; offset += 16) {
      harness.publish(parts.slice(offset, offset + 16));
      await harness.settle();
    }

    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboActive: true,
      activePermits: 1,
    });
    const events = handle.events();
    await expect(events.next()).resolves.toMatchObject({
      value: { event: eventName, data },
      done: false,
    });
    const terminalEvent = events.next();
    await Bun.sleep(0);

    harness.publish([{
      kind: "response_event",
      requestId: identity.requestId,
      eventIndex: 1,
      event: completed.event,
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes(completed.data)),
    }, terminal]);
    await harness.settle();

    await expect(terminalEvent).resolves.toMatchObject({
      value: completed,
      done: false,
    });
    await expect(events.next()).resolves.toMatchObject({ done: true });
    const settledFinal = await handle.final;
    expect(settledFinal).toMatchObject({ id: "resp_jumbo" });
    releaseAtomicBytePermit(settledFinal);
    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
      activePermits: 0,
    });
    await harness.close();
    pool.close();
  });

  it("releases a completed-event permit when terminal JSON validation throws", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 4 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
    });
    const harness = new TransportHarness({ eventMemoryPool: pool });
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    const requestId = harness.requestIdentity().requestId;

    harness.publish([{
      kind: "response_event",
      requestId,
      eventIndex: 0,
      event: "response.completed",
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes("{not-json")),
    }]);
    await harness.settle();

    await expect(handle.final).rejects.toMatchObject({
      committed: true,
      code: "response_transcript",
    });
    expect(harness.session.snapshot().connected).toBe(true);
    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
      activePermits: 0,
    });
    await harness.close();
    pool.close();
  });

  it("keeps a backpressured terminal charged through cancelled proof finalization", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: 4 * 1024 * 1024,
      jumboBytes: 4 * 1024 * 1024,
    });
    const harness = new TransportHarness({
      eventMemoryPool: pool,
      maxBufferedResponseBytes: 160,
    });
    const controller = new AbortController();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY, controller.signal));
    const identity = harness.requestIdentity();
    const final = {
      id: "resp_cancelled_terminal_proof",
      object: "response",
      status: "completed",
      output: [],
    };
    const events = [{
      event: "response.created",
      data: JSON.stringify({
        type: "response.created",
        sequence_number: 0,
        response: { id: final.id },
      }),
    }, {
      event: "response.completed",
      data: JSON.stringify({
        type: "response.completed",
        sequence_number: 1,
        response: final,
      }),
    }];
    const transcript = new TranscriptProof(identity.requestId, identity.requestDigest);
    for (const event of events) transcript.append(event);
    const terminal = await transcript.createTerminal();
    const eventFrame = (eventIndex: number): SidecarFrame => ({
      kind: "response_event",
      requestId: identity.requestId,
      eventIndex,
      event: events[eventIndex]!.event,
      partIndex: 0,
      partCount: 1,
      offset: 0,
      dataBase64Url: base64UrlEncode(utf8Bytes(events[eventIndex]!.data)),
    });
    const rejectedFinal = handle.final.catch(error => error);

    try {
      // Proof may arrive ahead of its reliable response frames.
      harness.publish([terminal]);
      await harness.settle();
      harness.publish([eventFrame(0)]);
      await harness.settleUntil(() => pool.snapshot().activePermits === 1);

      // The second event cannot enter the full one-event handle buffer.
      harness.publish([eventFrame(1)]);
      await harness.settleUntil(() => pool.snapshot().activePermits === 2);
      controller.abort(new DOMException("client disconnected", "AbortError"));
      await harness.settleUntil(() => pool.snapshot().activePermits === 1);

      // Cancelling releases the public frame/internal final references, while
      // the session proof reference keeps the parsed transcript final charged.
      expect(await rejectedFinal).toMatchObject({ committed: true, code: "cancelled" });
      expect(pool.snapshot()).toMatchObject({ activePermits: 1 });

      // A replayed proof wakes finalization after the blocked delivery unwinds.
      harness.publish([terminal]);
      await harness.settleUntil(() => pool.snapshot().activePermits === 0);
      expect(harness.session.snapshot().committed).toBe(0);
    } finally {
      await harness.close();
      pool.close();
    }
  });

  it("enforces the exact canonical SSE response limit including newline framing", async () => {
    const final = { id: "resp_exact", object: "response", status: "completed", output: [] };
    const events = [{
      event: "response.created",
      data: '{\r\n"type":"response.created",\r"response":{"id":"resp_exact"}\n}',
    }, {
      event: "response.completed",
      data: JSON.stringify({ type: "response.completed", response: final }),
    }];
    const canonicalBytes = events.reduce(
      (total, event) => total + utf8Bytes(
        `event: ${event.event}\ndata: ${event.data}\n\n`,
      ).byteLength,
      0,
    );
    const run = async (limit: number) => {
      const pool = new AtomicBytePermitPool({
        ordinaryBytes: 4 * 1024 * 1024,
        jumboBytes: 4 * 1024 * 1024,
      });
      const harness = new TransportHarness({
        maxResponseBytes: limit,
        eventMemoryPool: pool,
      });
      await harness.start();
      const handle = await harness.commit(harness.session.run(BODY));
      const identity = harness.requestIdentity();
      const transcript = new TranscriptProof(identity.requestId, identity.requestDigest);
      const frames: SidecarFrame[] = events.map((event, eventIndex) => {
        transcript.append(event);
        return {
          kind: "response_event",
          requestId: identity.requestId,
          eventIndex,
          event: event.event,
          partIndex: 0,
          partCount: 1,
          offset: 0,
          dataBase64Url: base64UrlEncode(utf8Bytes(event.data)),
        };
      });
      frames.push(await transcript.createTerminal());
      harness.publish(frames);
      await harness.settle();
      return { harness, handle, pool };
    };

    const exact = await run(canonicalBytes);
    const exactFinal = await exact.handle.final;
    expect(exactFinal).toMatchObject({ id: "resp_exact" });
    for await (const _event of exact.handle.events()) { /* release carried permits */ }
    releaseAtomicBytePermit(exactFinal);
    await exact.harness.close();
    expect(exact.pool.snapshot().activePermits).toBe(0);
    exact.pool.close();

    const tooSmall = await run(canonicalBytes - 1);
    await expect(tooSmall.handle.final).rejects.toMatchObject({ code: "response_too_large" });
    await tooSmall.harness.close();
    expect(tooSmall.pool.snapshot().activePermits).toBe(0);
    tooSmall.pool.close();
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

  it("digests, accounts, compresses, and frames the exact normalized R8 request bytes", async () => {
    const harness = new TransportHarness({ binding: R2_BINDING });
    await harness.start();
    const controller = new AbortController();
    const body = Object.freeze({
      model: "gpt-5.6-sol",
      input: "persisted continuation",
      previous_response_id: "resp_parent_exact",
      stream: false,
      store: false,
      vendor_extension: {
        preserved: true,
        negative_zero: -0,
        escaped: "quote=\" slash=\\ newline=\n",
        surrogate_pair_across_chunk: `${"x".repeat(64 * 1024 - 1)}😀`,
      },
    });
    const expectedBody = { ...body, stream: true, store: true };
    const expectedBytes = utf8Bytes(canonicalJson(expectedBody));
    const pending = harness.session.run(body, controller.signal);
    void pending.catch(() => undefined);

    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_commit")
    ));
    const frames = [...harness.hostFrames.values()];
    const open = frames.find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    )!;
    const data = frames
      .filter((frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_data" }> => (
        frame.kind === "request_data"
      ))
      .sort((left, right) => left.partIndex - right.partIndex);
    const commit = frames.find(
      (frame): frame is ReliableFrame<R2RequestCommitFrame> => frame.kind === "request_commit",
    )!;
    const carried = Buffer.concat(data.map(frame => Buffer.from(
      base64UrlDecode(frame.dataBase64Url),
    )));

    expect(open).toMatchObject({
      requestDigest: await sha256Hex(expectedBytes),
      decodedBytes: expectedBytes.byteLength,
      encodedBytes: expectedBytes.byteLength,
      encoding: "identity",
      partCount: data.length,
    });
    expect(commit).toMatchObject({
      requestDigest: open.requestDigest,
      decodedBytes: open.decodedBytes,
      encodedBytes: open.encodedBytes,
      encoding: open.encoding,
      partCount: open.partCount,
    });
    expect(Buffer.compare(carried, Buffer.from(expectedBytes))).toBe(0);
    expect(JSON.parse(carried.toString("utf8"))).toEqual(JSON.parse(canonicalJson(expectedBody)));
    expect(body).toMatchObject({ stream: false, store: false });

    controller.abort();
    await expect(pending).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
    });
    await harness.close();
  });

  it("advances a large request across multiple windows without commit overtaking data", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "large-window-".repeat(35_000),
      stream: true,
    });

    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
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
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
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
      priority: "latency",
    });
    await harness.commit(pending);
    await harness.close();
  });

  it("skips gzip only for one-part requests", async () => {
    const body = {
      model: "gpt-5.6-sol",
      input: "tiny",
      stream: true,
    };
    let gzipCalls = 0;
    const harness = new TransportHarness({
      gzip: async bytes => {
        gzipCalls++;
        return gzipSync(bytes);
      },
    });
    await harness.start();
    const pending = harness.session.run(body);
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
    const open = [...harness.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    )!;
    expect(open.encoding).toBe("identity");
    expect(gzipCalls).toBe(0);
    await harness.commit(pending);
    await harness.close();
  });

  it("keeps sub-bulk inline base64 off the compression workers", async () => {
    let gzipCalls = 0;
    const harness = new TransportHarness({
      gzip: async bytes => {
        gzipCalls++;
        return gzipSync(bytes);
      },
    });
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: [{
        role: "user",
        content: [{
          type: "input_image",
          image_url: `data:image/png;base64,${"A".repeat(128 * 1024)}`,
        }],
      }],
      stream: true,
    });
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
    expect(gzipCalls).toBe(0);
    await harness.commit(pending);
    await harness.close();
  });

  it("recovers base64 expansion for a large high-entropy inline image", async () => {
    let state = 0x12345678;
    const bytes = new Uint8Array(2 * 1024 * 1024);
    for (let index = 0; index < bytes.length; index++) {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      bytes[index] = state & 0xff;
    }
    const body = {
      model: "gpt-5.6-sol",
      input: [{
        role: "user",
        content: [{
          type: "input_image",
          image_url: `data:image/png;base64,${Buffer.from(bytes).toString("base64")}`,
        }],
      }],
      stream: true,
    };
    let gzipCalls = 0;
    const gzipOptions: Array<V2GzipOptions | undefined> = [];
    const harness = new TransportHarness({
      gzip: async (input, options) => {
        gzipCalls++;
        gzipOptions.push(options);
        return gzipSync(input, { level: 1 });
      },
    });
    await harness.start();
    const pending = harness.session.run(body);
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
    const open = [...harness.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    )!;
    expect(open.encoding).toBe("gzip+base64");
    expect(open.encodedBytes).toBeLessThan(open.decodedBytes * 0.8);
    expect(gzipCalls).toBe(2);
    expect(gzipOptions.map(options => ({
      inputOwnership: options?.inputOwnership,
      priority: options?.priority,
    }))).toEqual([
      { inputOwnership: "owned", priority: "bulk" },
      { inputOwnership: "owned", priority: "bulk" },
    ]);
    expect(gzipOptions[0]?.signal).toBe(gzipOptions[1]?.signal);
    expect(gzipOptions[0]?.signal).toBeInstanceOf(AbortSignal);
    expect(gzipOptions[0]?.signal?.aborted).toBe(false);
    for (let round = 0; round < 64; round++) {
      if ([...harness.hostFrames.values()].some(frame => (
        frame.kind === "request_commit" && frame.requestId === open.requestId
      ))) break;
      harness.publish([]);
      await harness.settle();
    }
    expect([...harness.hostFrames.values()].some(frame => (
      frame.kind === "request_commit" && frame.requestId === open.requestId
    ))).toBe(true);
    const data = [...harness.hostFrames.values()]
      .filter((frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_data" }> => (
        frame.kind === "request_data" && frame.requestId === open.requestId
      ))
      .sort((left, right) => left.partIndex - right.partIndex);
    expect(data).toHaveLength(open.partCount);
    expect(data.every((frame, index) => frame.partIndex === index)).toBe(true);
    const encoded = Buffer.concat(data.map(frame => Buffer.from(
      base64UrlDecode(frame.dataBase64Url),
    )));
    expect(encoded.byteLength).toBe(open.encodedBytes);
    expect(Buffer.compare(
      Buffer.from(gunzipSync(encoded)),
      Buffer.from(utf8Bytes(canonicalJson({ ...body, store: true }))),
    )).toBe(0);
    await harness.commit(pending);
    await harness.close();
  });

  it("uses one bounded sample and skips full gzip when savings are negligible", async () => {
    let gzipCalls = 0;
    const harness = new TransportHarness({
      gzip: async bytes => {
        gzipCalls++;
        return bytes;
      },
    });
    await harness.start();
    const pending = harness.session.run({
      model: "gpt-5.6-sol",
      input: "incompressible".repeat(20_000),
      stream: true,
    });
    await harness.settleUntil(() => (
      [...harness.hostFrames.values()].some(frame => frame.kind === "request_open")
    ));
    const open = [...harness.hostFrames.values()].find(
      (frame): frame is Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> => (
        frame.kind === "request_open"
      ),
    )!;
    expect(open.encoding).toBe("identity");
    expect(gzipCalls).toBe(1);
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

  it("aborts an active-request drain wait without leaving a stale idle waiter", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const handle = await harness.commit(harness.session.run(BODY));
    void handle.final.catch(() => undefined);
    const controller = new AbortController();
    const firstDrain = harness.session.drain("abort active wait", controller.signal);
    expect(await promiseState(firstDrain)).toBe("pending");
    controller.abort();
    await expect(firstDrain).rejects.toMatchObject({ name: "AbortError" });

    harness.publish(await harness.completedFrames());
    await harness.settle();
    await handle.final;
    const secondDrain = harness.session.drain("retry after abort");
    await harness.settle();
    harness.publish([{ kind: "drained", active: 0, queued: 0 }]);
    await harness.settle();
    await expect(secondDrain).resolves.toBeUndefined();
    await harness.sidecar.close();
  });

  it("does not poison drain retry when enqueue throws before the drain flag is set", async () => {
    const harness = new TransportHarness();
    await harness.start();
    const original = WeightedFrameScheduler.prototype.enqueue;
    const enqueue = spyOn(WeightedFrameScheduler.prototype, "enqueue").mockImplementation(
      function (this: WeightedFrameScheduler<HostFrame>, priority, streamId, frame, weight) {
        if ((frame as HostFrame).kind === "drain") {
          throw new Error("synthetic drain enqueue failure");
        }
        return original.call(this, priority, streamId, frame, weight);
      },
    );
    await expect(harness.session.drain("first attempt")).rejects.toThrow(
      "synthetic drain enqueue failure",
    );
    enqueue.mockRestore();

    const retry = harness.session.drain("second attempt");
    await harness.settle();
    expect([...harness.hostFrames.values()]).toContainEqual(expect.objectContaining({
      kind: "drain",
      reason: "second attempt",
    }));
    harness.publish([{ kind: "drained", active: 0, queued: 0 }]);
    await harness.settle();
    await expect(retry).resolves.toBeUndefined();
    await harness.sidecar.close();
  });

  it("joins an active teardown from closed drain and force-stop fast paths", async () => {
    const harness = new TransportHarness();
    await harness.start();
    let releaseClose!: () => void;
    const closeGate = new Promise<void>(resolve => { releaseClose = resolve; });
    const originalClose = harness.host.close.bind(harness.host);
    (harness.host as { close(): Promise<void> }).close = async () => {
      await closeGate;
      await originalClose();
    };
    const closing = harness.session.close();
    const draining = harness.session.drain("join teardown");
    const stopping = harness.session.forceStop("join teardown");
    expect(await promiseState(draining)).toBe("pending");
    expect(await promiseState(stopping)).toBe("pending");
    releaseClose();
    await Promise.all([closing, draining, stopping]);
    await harness.sidecar.close();
  });

  it("still closes the channel when teardown presence clearing throws", async () => {
    const harness = new TransportHarness({ throwOnClear: true });
    await harness.start();
    await expect(harness.session.close()).rejects.toThrow("synthetic channel clear failure");
    expect(harness.sidecar.peerIds()).toEqual([]);
    await harness.sidecar.close();
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

  it("removes a timed-out credential rotation queued behind an active flight", async () => {
    const harness = new TransportHarness({ rotationTimeoutMs: 5 });
    await harness.start();
    const request = harness.session.run(BODY);
    void request.catch(() => undefined);
    await Bun.sleep(harness.publicationIntervalMs + 5);
    await harness.network.flushAll();
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "request_commit"))
      .toBe(true);

    const sealedCanary = "sealed-rotation-secret-canary";
    await expect(harness.session.rotateCredential(
      "rotation-blocked-timeout",
      sealedCanary,
    )).rejects.toThrow("timed out");
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "credential_rotate"))
      .toBe(false);

    // ACK the request flight after the rotation waiter has expired. A leaked
    // scheduler stream would now publish its stale sealed credential.
    harness.publish([], { lane: "control", ackSequence: harness.hostAck });
    await harness.settle();
    expect([...harness.hostFrames.values()].some(frame => frame.kind === "credential_rotate"))
      .toBe(false);
    expect(JSON.stringify(harness.hostEnvelopes)).not.toContain(sealedCanary);
    expect(harness.session.snapshot().diagnostics?.host).toMatchObject({
      schedulerFrames: 0,
      schedulerBytes: 0,
    });

    await harness.session.close();
    await expect(request).rejects.toBeInstanceOf(V2CommittedError);
    await harness.sidecar.close();
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
    await expect(pending).rejects.toMatchObject({
      name: "V2PreCommitError",
      code: "cancelled_precommit",
    });
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
    expect(harness.session.snapshot().diagnostics!.host).toMatchObject({
      stallEventsTotal: 0,
    });
    expect(harness.session.snapshot().diagnostics!.host.retransmittedBytesTotal)
      .toBeGreaterThan(0);
    await harness.close();
  });
});
