import { describe, expect, it, spyOn } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import { DenoAwarenessPeer } from "../sandbox/v2/awareness-peer";
import { DenoAwarenessPeer as R10DenoAwarenessPeer } from "../sandbox/v2/r10-awareness-peer";
import type { AwarenessChannel, AwarenessPeerUpdate } from "../src/v2/awareness/channel";
import { V2TransportSession } from "../src/v2/transport/session";
import { projectId } from "../src/sandbox/types";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import { SidecarDiagnosticsObserver } from "../src/v2/diagnostics/sidecar-observer";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";
import { OutputBackpressure } from "../src/v2/sidecar/output-sink";
import { OutboundFrameWindow } from "../src/v2/protocol/window";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  ReliableFrame,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import {
  SidecarHarness,
  TEST_BINDING,
  completedUpstream,
} from "./support/fake-v2-sidecar";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const BINDING: ProtocolBinding = {
  protocolRevision: "r1",
  launchNonce: "launch-nonce-r1",
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

function r10LivenessHarness(options: {
  accept?: (frame: HostFrame) => Promise<void>;
  retransmitMs?: number;
  outputBackpressure?: OutputBackpressure;
  now?: () => number;
  transportStallMs?: number;
} = {}) {
  const binding: ProtocolBinding = {
    ...BINDING,
    protocolRevision: "r2",
    launchNonce: "r10-liveness-launch",
  };
  const providers = new Map<"data" | "control", FakeProvider>();
  const hostDocs = { data: new Y.Doc(), control: new Y.Doc() };
  const hostAwareness = {
    data: new Awareness(hostDocs.data),
    control: new Awareness(hostDocs.control),
  };
  const hostCeilings: number[] = [];
  const accepted: HostFrame[] = [];
  const peer = new R10DenoAwarenessPeer({
    binding,
    instanceId: "sidecar-instance-1",
    credential: {
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
      rotationKey: createCredentialRotationKey(),
    },
    engine: {
      snapshot: () => ({ ready: true, credits: 8, active: 0, queued: 0, draining: false }),
      setHostCeiling: (credits: number) => { hostCeilings.push(credits); },
      accept: async (frame: HostFrame) => {
        accepted.push(frame);
        await options.accept?.(frame);
      },
    } as any,
    providerFactory: (doc, _credential, lane) => {
      const provider = new FakeProvider(doc);
      providers.set(lane, provider);
      return provider;
    },
    publicationIntervalMs: 60_000,
    retransmitMs: options.retransmitMs ?? 60_000,
    outputBackpressure: options.outputBackpressure,
    now: options.now,
    transportStallMs: options.transportStallMs,
    connectTimeoutMs: 100,
  });
  const publish = (envelope: AwarenessEnvelope<HostFrame>, lane: "data" | "control" = "data") => {
    hostAwareness[lane].setLocalStateField(
      lane === "data" ? "ghostprovV2Data" : "ghostprovV2Control",
      envelope,
    );
    applyAwarenessUpdate(
      providers.get(lane)!.awareness,
      encodeAwarenessUpdate(hostAwareness[lane], [hostDocs[lane].clientID]),
      "r10-host",
    );
  };
  let revision = 0;
  return {
    binding, peer, providers, hostCeilings, accepted, publish, hostDocs, hostAwareness,
    control: () => providers.get("control")!.awareness.getLocalState()
      ?.ghostprovV2Control as AwarenessEnvelope<SidecarFrame>,
    sendControl(overrides: Record<string, unknown> = {}) {
      publish({
        ...hostEnvelope(-1, revision++),
        protocolRevision: "r2",
        schemaVersion: 2,
        launchNonce: binding.launchNonce,
        targetInstanceId: "sidecar-instance-1",
        lane: "control",
        ...overrides,
      } as AwarenessEnvelope<HostFrame>, "control");
    },
    sendData(frames: readonly ReliableFrame<HostFrame>[], overrides: Record<string, unknown> = {}) {
      publish({
        ...hostEnvelope(-1, revision++),
        protocolRevision: "r2",
        schemaVersion: 2,
        launchNonce: binding.launchNonce,
        targetInstanceId: "sidecar-instance-1",
        lane: "data",
        frames,
        ...overrides,
      } as AwarenessEnvelope<HostFrame>);
    },
    async close() {
      await peer.close();
      for (const awareness of Object.values(hostAwareness)) awareness.destroy();
      for (const doc of Object.values(hostDocs)) doc.destroy();
    },
  };
}

describe("R10 Awareness liveness state", () => {
  it("keeps a cold response flight credited below the sixty-second default while queue pressure remains fail-closed", async () => {
    let now = 1_000;
    const harness = r10LivenessHarness({ now: () => now });
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.peer.enqueue(responseEvent(0, "req-cold-response"), "data", "req-cold-response");
      harness.peer.publishEngineState();
      const data = () => harness.providers.get("data")!.awareness.getLocalState()!
        .ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
      const original = data();

      now += 39_249;
      harness.sendControl({ ack: { epoch: 1, sequence: -1 } });
      harness.peer.publishEngineState();
      expect(harness.control().credits).toBe(8);
      expect(data().frames).toEqual(original.frames);
      expect(harness.control().diagnostics?.transport).toMatchObject({
        stallEventsTotal: 0,
      });

      for (let index = 1; index <= 128; index++) {
        harness.peer.enqueue(
          responseEvent(index, "req-cold-response"),
          "data",
          "req-cold-response",
        );
      }
      harness.peer.publishEngineState();
      expect(harness.control().credits).toBe(0);
      expect(harness.control().diagnostics?.transport.schedulerFrames).toBeGreaterThanOrEqual(128);
      expect(data().frames).toEqual(original.frames);
    } finally { await harness.close(); }
  });

  it("retires unpublished cancelled output without changing its active flight or another stream", async () => {
    const outputBackpressure = new OutputBackpressure({
      highFrames: 16, highBytes: 2_048, lowFrames: 4, lowBytes: 1_024,
      hardFrames: 128, hardBytes: 16_384,
    });
    const harness = r10LivenessHarness({ outputBackpressure });
    const requestId = "req-cancelled-output";
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.peer.enqueue(responseEvent(0, requestId), "data", requestId);
      harness.peer.publishEngineState();
      const data = () => harness.providers.get("data")!.awareness.getLocalState()!
        .ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
      const activeFlight = data();
      for (let index = 1; index <= 32; index++) {
        harness.peer.enqueue(responseEvent(index, requestId), "data", requestId);
      }
      harness.peer.enqueue(responseEvent(0, "req-active"), "data", "req-active");
      const pressureBefore = outputBackpressure.snapshot();
      expect(pressureBefore).toMatchObject({ paused: true, frames: 33 });
      let pressureReleased = false;
      const released = outputBackpressure.wait().then(() => { pressureReleased = true; });
      // The engine emits this only after durable cancellation has won.
      harness.peer.enqueue({ kind: "request_cancelled", requestId }, "control", requestId);
      await released;
      expect(pressureReleased).toBe(true);
      expect(outputBackpressure.snapshot()).toMatchObject({ paused: false, frames: 2, waiters: 0 });
      expect(outputBackpressure.snapshot().bytes).toBeLessThan(pressureBefore.bytes);
      harness.peer.publishEngineState();
      expect(data().frames).toEqual(activeFlight.frames);
      harness.sendControl({ ack: { epoch: 1, sequence: activeFlight.frames.at(-1)!.sequence } });
      harness.peer.publishEngineState();
      const nextFlight = data();
      expect(nextFlight.frames.map(frame => frame.kind)).toEqual([
        "request_cancelled", "response_event",
      ]);
      expect(nextFlight.frames.map(frame => "requestId" in frame ? frame.requestId : null))
        .toEqual([requestId, "req-active"]);
      expect(nextFlight.frames.map(frame => frame.sequence)).toEqual([1, 2]);
      expect(outputBackpressure.snapshot()).toMatchObject({ frames: 0, bytes: 0, waiters: 0 });
    } finally { await harness.close(); }
  });

  it("does not immediately duplicate a timer retransmission when a stale ACK arrives", async () => {
    let now = 1_000;
    const clock = spyOn(performance, "now").mockImplementation(() => now);
    const timers = new Map<object, { at: number; callback(): void }>();
    const set = spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, delay = 0) => {
      const handle = { unref() { return handle; } };
      timers.set(handle, { at: now + delay, callback });
      return handle;
    }) as typeof setTimeout);
    const clear = spyOn(globalThis, "clearTimeout").mockImplementation((handle => {
      timers.delete(handle as object);
    }) as typeof clearTimeout);
    const harness = r10LivenessHarness({ retransmitMs: 1_000 });
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.peer.enqueue(responseEvent(0, "req-retransmission"), "data", "req-retransmission");
      harness.peer.publishEngineState();
      const data = () => (harness.providers.get("data")!.awareness.getLocalState()!
        .ghostprovV2Data as AwarenessEnvelope<SidecarFrame>);
      const original = data();
      now += 1_600;
      for (const [handle, timer] of [...timers]) {
        if (timer.at > now) continue;
        timers.delete(handle);
        timer.callback();
      }
      const retransmitted = data();
      expect(retransmitted.revision).toBeGreaterThan(original.revision);
      harness.sendControl({ ack: { epoch: 1, sequence: -1 } });
      expect(data().revision).toBe(retransmitted.revision);
      expect(data().frames).toEqual(original.frames);
      now += 2_000;
      harness.sendControl({ ack: { epoch: 1, sequence: -1 } });
      expect(data().revision).toBeGreaterThan(retransmitted.revision);
      expect(data().frames).toEqual(original.frames);
    } finally {
      await harness.close();
      set.mockRestore(); clear.mockRestore(); clock.mockRestore();
    }
  });

  it("keeps exact-owner cleanup usable after draining control overtakes data", async () => {
    const harness = r10LivenessHarness();
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.sendControl({ draining: true });
      harness.sendData([
        { kind: "request_cancel", requestId: "req-draining", reason: "client", epoch: 1, sequence: 0 },
        { kind: "response_ack", requestId: "req-terminal", terminalDigest: "a".repeat(64), epoch: 1, sequence: 1 },
        { kind: "drain", reason: "shutdown", epoch: 1, sequence: 2 },
      ], { draining: true });
      await Bun.sleep(0);
      expect(harness.accepted.map(frame => frame.kind)).toEqual([
        "request_cancel", "response_ack", "drain",
      ]);
      expect(harness.control().ack).toEqual({ epoch: 1, sequence: 2 });
    } finally { await harness.close(); }
  });

  it("does not ACK draining cleanup before its durable processing settles", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const harness = r10LivenessHarness({ accept: () => gate });
    try {
      await harness.peer.connect();
      harness.sendControl({ draining: true });
      harness.sendData([
        { kind: "response_ack", requestId: "req-terminal", terminalDigest: "a".repeat(64), epoch: 1, sequence: 0 },
      ], { draining: true });
      await Bun.sleep(0);
      expect(harness.accepted).toHaveLength(1);
      expect(harness.control().ack).toEqual({ epoch: 1, sequence: -1 });
      release();
      await Bun.sleep(0);
      expect(harness.control().ack).toEqual({ epoch: 1, sequence: 0 });
    } finally { release(); await harness.close(); }
  });

  it.each(["missing", "removed", "rollback", "foreign", "target", "binding"] as const)(
    "rejects draining cleanup without current exact control authority: %s", async failure => {
      const harness = r10LivenessHarness();
      try {
        await harness.peer.connect();
        if (failure !== "missing") {
          harness.sendControl();
          harness.sendControl({ draining: true });
          if (failure === "removed") {
            harness.hostAwareness.control.setLocalStateField("ghostprovV2Control", null);
            applyAwarenessUpdate(harness.providers.get("control")!.awareness,
              encodeAwarenessUpdate(harness.hostAwareness.control, [harness.hostDocs.control.clientID]), "removed-host");
          } else {
            const invalid = failure === "rollback" ? { revision: 0 }
              : failure === "foreign" ? { instanceId: "foreign-host" }
              : failure === "target" ? { targetInstanceId: "foreign-sidecar" }
              : { buildHash: "c".repeat(64) };
            harness.sendControl({ draining: true, ...invalid });
          }
        }
        harness.sendData([{ kind: "drain", reason: "shutdown", epoch: 1, sequence: 0 }], { draining: true });
        await Bun.sleep(0);
        expect(harness.accepted).toHaveLength(0);
        expect(harness.control().ack).toEqual({ epoch: 1, sequence: -1 });
      } finally { await harness.close(); }
    },
  );

  it("does not admit new request data or skip its sequence while draining", async () => {
    const harness = r10LivenessHarness();
    try {
      await harness.peer.connect();
      harness.sendControl({ draining: true });
      harness.sendData([
        { kind: "request_open", requestId: "req-new", requestDigest: "a".repeat(64),
          decodedBytes: 1, encodedBytes: 1, encoding: "identity", partCount: 1, epoch: 1, sequence: 0 },
        { kind: "drain", reason: "shutdown", epoch: 1, sequence: 1 },
      ], { draining: true });
      await Bun.sleep(0);
      expect(harness.accepted).toHaveLength(0);
      expect(harness.control().ack).toEqual({ epoch: 1, sequence: -1 });
    } finally { await harness.close(); }
  });

  it("recovers derated capacity during steady 1200ms ACK rounds", async () => {
    let now = 1_000;
    const clock = spyOn(performance, "now").mockImplementation(() => now);
    // Short deterministic override for the adaptive boundary; production R10 is 60 seconds.
    const harness = r10LivenessHarness({ transportStallMs: 2_000 });
    let acknowledged = -1;
    let eventIndex = 0;
    const round = (latencyMs: number, stall: boolean) => {
      harness.peer.enqueue(responseEvent(eventIndex++, "req-rtt-recovery"), "data", "req-rtt-recovery");
      harness.peer.publishEngineState();
      const flight = harness.providers.get("data")!.awareness.getLocalState()!.ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
      now += latencyMs;
      if (stall) {
        harness.sendControl({ ack: { epoch: 1, sequence: acknowledged } });
        harness.peer.publishEngineState();
        expect(harness.control().credits).toBe(0);
      }
      acknowledged = flight.frames.at(-1)!.sequence;
      harness.sendControl({ ack: { epoch: 1, sequence: acknowledged } });
      harness.peer.publishEngineState();
    };
    try {
      await harness.peer.connect();
      harness.sendControl();
      round(2_100, true);
      round(7_000, true);
      expect(harness.control().credits).toBe(4);
      for (let index = 0; index < 9; index++) round(1_200, false);
      expect(harness.control().credits).toBeGreaterThan(4);
      expect(harness.control().credits).toBeLessThanOrEqual(8);
    } finally {
      await harness.close();
      clock.mockRestore();
    }
  });

  it("keeps response admission usable after ACK progress while queued output remains", async () => {
    let now = 1_000;
    const clock = spyOn(performance, "now").mockImplementation(() => now);
    const harness = r10LivenessHarness();
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.peer.enqueue(responseEvent(0, "req-output-stream"), "data", "req-output-stream");
      harness.peer.publishEngineState();
      const first = harness.providers.get("data")!.awareness.getLocalState()!.ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
      expect(first.frames).toHaveLength(1);
      let eventIndex = 1;
      const dataBase64Url = Buffer.from(JSON.stringify({ delta: "x".repeat(2_200) })).toString("base64url");
      const enqueue = (count: number) => {
        for (let index = 0; index < count; index++) {
          harness.peer.enqueue({ ...responseEvent(eventIndex++, "req-output-stream"),
            dataBase64Url } as SidecarFrame, "data", "req-output-stream");
        }
      };
      enqueue(60);
      // This models the logged 2.24s queue age with fresh ACK progress. The
      // response queue stays below its hard byte/frame admission watermarks.
      now += 2_500;
      let flight = first;
      for (let round = 0; round < 6; round++) {
        harness.sendControl({ ack: { epoch: 1, sequence: flight.frames.at(-1)!.sequence } });
        harness.peer.publishEngineState();
        flight = harness.providers.get("data")!.awareness.getLocalState()!.ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
        expect(flight.frames.length).toBeGreaterThan(0);
        expect(flight.frames.length).toBeLessThanOrEqual(32);
        expect(harness.control().credits).toBeGreaterThan(0);
        if (round < 5) enqueue(flight.frames.length);
        now += 1_200;
      }
    } finally {
      await harness.close();
      clock.mockRestore();
    }
  });

  it.each([1, 2])("recovers an idle host probe after %i isolated pong losses", async lostPongs => {
    const harness = r10LivenessHarness();
    const pings: AwarenessEnvelope<HostFrame>[] = [];
    let listener: ((update: AwarenessPeerUpdate<SidecarFrame>) => void) | null = null;
    const channel: AwarenessChannel<HostFrame, SidecarFrame> = {
      projectId: projectId(PROJECT), localPeerId: "host",
      connect: async () => undefined,
      publish(envelope, lane) {
        harness.publish(envelope, lane);
        if (envelope.liveness?.kind !== "ping") return;
        pings.push(envelope);
        // The peer is otherwise idle. No periodic engine update, host ACK,
        // reconnect, or synthetic publication may rescue the dropped pong.
        if (pings.length <= lostPongs) return;
        listener!({ peerClientId: "sidecar-control", envelope: harness.control() });
      },
      clear() {},
      subscribe(next) { listener = next; return () => { listener = null; }; },
      peerIds: () => ["sidecar"], close: async () => undefined,
    };
    const session = new V2TransportSession({
      binding: harness.binding, channel, maxResponseBytes: 2 * 1024 * 1024,
      hostInstanceId: "host-instance-1", expectedSidecarInstanceId: "sidecar-instance-1",
      livenessTimeoutMs: 300, publicationIntervalMs: 60_000, retransmitMs: 60_000,
    });
    try {
      await harness.peer.connect();
      await session.connect();
      await expect(session.probeLiveness()).resolves.toMatchObject({
        sentAtMs: pings[0]!.liveness!.sentAtMs,
      });
      expect(pings).toHaveLength(lostPongs + 1);
      for (const envelope of pings) {
        expect(envelope).toMatchObject({
          lane: "control", frames: [], targetInstanceId: "sidecar-instance-1",
          liveness: pings[0]!.liveness,
        });
      }
      expect(harness.accepted).toEqual([]);
      expect(session.snapshot()).toMatchObject({ connected: true, active: 0 });
    } finally {
      await session.close();
      await harness.close();
    }
  });

  it("settles a host probe when its first pong is superseded before replication", async () => {
    const harness = r10LivenessHarness();
    let listener: ((update: AwarenessPeerUpdate<SidecarFrame>) => void) | null = null;
    const channel: AwarenessChannel<HostFrame, SidecarFrame> = {
      projectId: projectId(PROJECT),
      localPeerId: "host",
      connect: async () => undefined,
      publish: harness.publish,
      clear() {},
      subscribe(next) { listener = next; return () => { listener = null; }; },
      peerIds: () => ["sidecar"],
      close: async () => undefined,
    };
    const session = new V2TransportSession({
      binding: harness.binding,
      channel,
      maxResponseBytes: 2 * 1024 * 1024,
      hostInstanceId: "host-instance-1",
      expectedSidecarInstanceId: "sidecar-instance-1",
      livenessTimeoutMs: 1_000,
      publicationIntervalMs: 60_000,
      retransmitMs: 60_000,
    });
    try {
      await harness.peer.connect();
      await session.connect();
      const pending = session.probeLiveness();
      void pending.catch(() => undefined);
      const firstPong = harness.control().liveness;
      expect(firstPong?.kind).toBe("pong");
      // Awareness replicates current state; an intermediate publication need
      // not reach a receiver before a later engine/credit update replaces it.
      harness.peer.publishEngineState();
      expect(harness.control().liveness).toEqual(firstPong);
      listener!({ peerClientId: "sidecar-control", envelope: harness.control() });
      await expect(pending).resolves.toMatchObject({ sentAtMs: firstPong!.sentAtMs });
      // The exact host clears its persistent ping after accepting the pong.
      harness.peer.publishEngineState();
      expect(harness.control().liveness).toBeUndefined();
      expect(harness.accepted).toEqual([]);
    } finally {
      await session.close();
      await harness.close();
    }
  });

  it("processes a bound ping's ACK and credits without admitting a reliable ping", async () => {
    const harness = r10LivenessHarness();
    try {
      await harness.peer.connect();
      harness.sendControl();
      harness.peer.enqueue({ kind: "pong", sentAtMs: 7 }, "control", "first");
      harness.peer.publishEngineState();
      const first = harness.providers.get("data")!.awareness.getLocalState()!.ghostprovV2Data;
      expect(first.frames).toHaveLength(1);
      harness.peer.enqueue({ kind: "pong", sentAtMs: 8 }, "control", "second");
      harness.sendControl({
        credits: 3,
        ack: { epoch: 1, sequence: first.frames[0].sequence },
        liveness: { kind: "ping", sentAtMs: 123 },
      });
      harness.peer.publishEngineState();
      expect(harness.hostCeilings.at(-1)).toBe(3);
      const next = harness.providers.get("data")!.awareness.getLocalState()!.ghostprovV2Data;
      expect(next.frames).toEqual([expect.objectContaining({ kind: "pong", sentAtMs: 8 })]);
      expect(harness.control().liveness).toEqual({ kind: "pong", sentAtMs: 123 });
      expect(harness.accepted).toEqual([]);
    } finally {
      await harness.close();
    }
  });

  it.each(["clear", "remove", "disconnect", "malformed", "wrong-target", "wrong-instance", "stale"])(
    "clears retained probe state when the bound host control becomes %s",
    async reason => {
      const harness = r10LivenessHarness();
      try {
        await harness.peer.connect();
        harness.sendControl({ liveness: { kind: "ping", sentAtMs: 123 } });
        harness.sendControl({ liveness: { kind: "ping", sentAtMs: 124 } });
        harness.peer.publishEngineState();
        expect(harness.control().liveness).toEqual({ kind: "pong", sentAtMs: 124 });
        if (reason === "clear") harness.sendControl();
        else if (reason === "disconnect") harness.providers.get("control")!.emit("connection-close");
        else if (reason === "remove") {
          harness.hostAwareness.control.setLocalState(null);
          applyAwarenessUpdate(
            harness.providers.get("control")!.awareness,
            encodeAwarenessUpdate(harness.hostAwareness.control, [harness.hostDocs.control.clientID]),
            "r10-remove-host",
          );
        } else {
          harness.sendControl(reason === "malformed"
            ? { liveness: { kind: "ping", sentAtMs: -1 } }
            : reason === "wrong-target" ? { targetInstanceId: "other-sidecar" }
            : reason === "wrong-instance" ? { instanceId: "other-host" }
            : { revision: 0, liveness: { kind: "ping", sentAtMs: 123 } });
        }
        harness.peer.publishEngineState();
        expect(harness.control().liveness).toBeUndefined();
      } finally {
        await harness.close();
      }
    },
  );
});

describe("R10 processed host ACK replay", () => {
  it.each(["request_data", "request_cancel"] as const)(
    "replays a dropped ACK-only publication for an immutable %s flight",
    async kind => {
      const harness = r10LivenessHarness();
      const outbound = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
      let revision = 0;
      const publishFlight = () => harness.publish({
        ...hostEnvelope(-1, revision++),
        protocolRevision: "r2",
        schemaVersion: 2,
        launchNonce: harness.binding.launchNonce,
        targetInstanceId: "sidecar-instance-1",
        lane: "data",
        frames: outbound.snapshot().frames,
      });
      const nextProcessedAck = (sequence: number) => (
        new Promise<AwarenessEnvelope<SidecarFrame>>(resolve => {
          const awareness = harness.providers.get("control")!.awareness;
          const onUpdate = () => {
            const envelope = harness.control();
            if (envelope.ack.sequence !== sequence) return;
            awareness.off("update", onUpdate);
            resolve(envelope);
          };
          awareness.on("update", onUpdate);
        })
      );
      const open: HostFrame = {
        kind: "request_open",
        requestId: "req-lost-ack",
        requestDigest: "a".repeat(64),
        decodedBytes: 1,
        encodedBytes: 1,
        encoding: "identity",
        partCount: 1,
      };
      try {
        await harness.peer.connect();
        // R2 data requires the exact host to first establish its live control
        // binding; connecting the peer alone does not authorize execution.
        harness.sendControl();
        outbound.push(open);
        const opened = nextProcessedAck(0);
        publishFlight();
        outbound.acknowledge((await opened).ack);

        const frame = outbound.push(kind === "request_data"
          ? { kind, requestId: open.requestId, partIndex: 0, offset: 0, dataBase64Url: "eA" }
          : { kind, requestId: open.requestId, reason: "client aborted" });
        const immutableFlight = outbound.snapshot().frames;
        const firstAck = nextProcessedAck(frame.sequence);
        publishFlight();
        const dropped = await firstAck;
        expect(dropped).toMatchObject({ lane: "control", frames: [] });
        // Drop this ACK before the sender sees it. Nothing else may publish
        // engine state or release the retained immutable flight.
        expect(outbound.snapshot().frames).toBe(immutableFlight);
        expect(harness.accepted.filter(value => value.kind === kind)).toHaveLength(1);

        const replayedAck = nextProcessedAck(frame.sequence);
        publishFlight();
        const replayed = await replayedAck;
        expect(replayed).toMatchObject({ lane: "control", ack: dropped.ack, frames: [] });
        expect(replayed.revision).toBeGreaterThan(dropped.revision);
        outbound.acknowledge(replayed.ack);
        expect(outbound.snapshot()).toMatchObject({ frames: [], encodedBytes: 0 });
        expect(harness.accepted.filter(value => value.kind === kind)).toHaveLength(1);

        const following = outbound.push({ ...open, requestId: "req-after-lost-ack" });
        const followingAck = nextProcessedAck(following.sequence);
        publishFlight();
        outbound.acknowledge((await followingAck).ack);
        expect(harness.accepted).toEqual([
          expect.objectContaining({ kind: "request_open", requestId: open.requestId }),
          expect.objectContaining({ kind, requestId: open.requestId }),
          expect.objectContaining({ kind: "request_open", requestId: "req-after-lost-ack" }),
        ]);
        expect(outbound.snapshot()).toMatchObject({ frames: [], encodedBytes: 0 });
      } finally {
        await harness.close();
      }
    },
  );
});

class FakeProvider {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  destroyed = false;
  failConnect = false;
  holdConnect = false;

  constructor(doc: Y.Doc) {
    this.awareness = new Awareness(doc);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let values = this.listeners.get(event);
    if (!values) this.listeners.set(event, values = new Set());
    values.add(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }

  connect(): void {
    queueMicrotask(() => {
      if (this.holdConnect) return;
      if (this.failConnect) this.emit("connection-error", new Error("failed"));
      else this.emit("sync", true);
    });
  }

  disconnect(): void {}

  destroy(): void {
    this.destroyed = true;
    this.awareness.destroy();
  }

  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }
}

function responseEvent(eventIndex: number, requestId = `req-${eventIndex % 16}`): SidecarFrame {
  return {
    kind: "response_event",
    requestId,
    eventIndex,
    event: "response.output_text.delta",
    partIndex: 0,
    partCount: 1,
    offset: 0,
    dataBase64Url: "eA",
  };
}

function hostEnvelope(ackSequence: number, revision = 0): AwarenessEnvelope<HostFrame> {
  return {
    protocol: "gp-awareness-v2",
    schemaVersion: 1,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: PROJECT,
    sessionId: BINDING.sessionId,
    peerId: "host",
    instanceId: "host-instance-1",
    leaderGeneration: BINDING.leaderGeneration,
    epoch: 1,
    revision,
    ack: { epoch: 1, sequence: ackSequence },
    credits: 128,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
}

describe("DenoAwarenessPeer", () => {
  it("keeps R2 sidecar response egress at the conservative 128 KiB flight", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-r2-sidecar-flight",
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-instance-r2-flight",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    for (let index = 0; index < 40; index++) {
      peer.enqueue({
        ...(responseEvent(
          index,
          "req-r2-large-response",
        ) as Extract<SidecarFrame, { kind: "response_event" }>),
        dataBase64Url: "eA".repeat(5_000),
      }, "data", "req-r2-large-response");
    }
    await Bun.sleep(5);

    const envelope = providers.get("data")!.awareness.getLocalState()
      ?.ghostprovV2Data as AwarenessEnvelope<SidecarFrame>;
    const encodedFrames = envelope.frames.reduce(
      (total, frame) => total + new TextEncoder().encode(JSON.stringify(frame)).byteLength,
      0,
    );
    expect("protocolRevision" in envelope && envelope.protocolRevision).toBe("r2");
    expect(envelope.frames.length).toBeLessThan(32);
    expect(encodedFrames).toBeLessThanOrEqual(128 * 1024);
    expect(encodedFrames).toBeGreaterThan(100 * 1024);
    await peer.close();
  });

  it("keeps R2 credits at zero until the reliable recovery marker is acknowledged", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-r2-recovery-barrier",
    };
    let replays = 0;
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-recovery-barrier",
      engine: {
        snapshot: () => ({ ready: true, credits: 1, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
        hasRecoveredWork: () => true,
        replayRecovered: async () => { replays++; },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
      initialActivePerSandbox: 1,
      maxActivePerSandbox: 1,
    });
    await peer.connect();
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);
    let revision = 0;
    const publishHost = (ack: number) => {
      remote.setLocalStateField("ghostprovV2Control", {
        protocol: "gp-awareness-v2",
        protocolRevision: "r2",
        schemaVersion: 2,
        schemaHash: binding.schemaHash,
        buildHash: binding.buildHash,
        projectId: binding.projectId,
        sessionId: binding.sessionId,
        peerId: "host",
        instanceId: "host-recovery-barrier",
        targetInstanceId: "sidecar-recovery-barrier",
        launchNonce: binding.launchNonce,
        leaderGeneration: binding.leaderGeneration,
        epoch: 1,
        revision: revision++,
        ack: { epoch: 1, sequence: ack },
        credits: 1,
        active: 0,
        queued: 0,
        draining: false,
        frames: [],
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "recovery-host",
      );
    };
    publishHost(-1);
    let markerSequence = -1;
    for (let turn = 0; turn < 100 && markerSequence < 0; turn++) {
      await Bun.sleep(2);
      const frames = providers.get("data")!.awareness.getLocalState()
        ?.ghostprovV2Data?.frames ?? [];
      markerSequence = frames.find((frame: any) => frame.kind === "recovery_complete")?.sequence ?? -1;
    }
    expect(replays).toBe(1);
    expect(markerSequence).toBeGreaterThanOrEqual(0);
    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
      .toBe(0);
    publishHost(markerSequence);
    for (let turn = 0; turn < 100; turn++) {
      await Bun.sleep(2);
      if (providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits === 1) break;
    }
    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
      .toBe(1);
    remote.destroy();
    remoteDoc.destroy();
    await peer.close();
  });

  it("emits the recovery marker only after ACKed outcomes finish durable cleanup", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-r2-durable-recovery",
    };
    let peer!: DenoAwarenessPeer;
    let cleanupStarted!: () => void;
    const cleanupObserved = new Promise<void>(resolve => { cleanupStarted = resolve; });
    let releaseCleanup!: () => void;
    const cleanupGate = new Promise<void>(resolve => { releaseCleanup = resolve; });
    peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-durable-recovery",
      engine: {
        snapshot: () => ({ ready: true, credits: 1, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
        hasRecoveredWork: () => true,
        replayRecovered: async () => {
          peer.enqueue({
            kind: "response_error",
            requestId: "req-durable-outcome",
            code: "provider_error",
            message: "durable provider outcome",
            ambiguous: false,
          }, "terminal", "req-durable-outcome");
        },
        acknowledgeDeliveredOutcomes: async () => {
          cleanupStarted();
          await cleanupGate;
        },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);
    let revision = 0;
    const publishAck = (ack: number) => {
      remote.setLocalStateField("ghostprovV2Control", {
        protocol: "gp-awareness-v2",
        protocolRevision: "r2",
        schemaVersion: 2,
        schemaHash: binding.schemaHash,
        buildHash: binding.buildHash,
        projectId: binding.projectId,
        sessionId: binding.sessionId,
        peerId: "host",
        instanceId: "host-durable-recovery",
        targetInstanceId: "sidecar-durable-recovery",
        launchNonce: binding.launchNonce,
        leaderGeneration: binding.leaderGeneration,
        epoch: 1,
        revision: revision++,
        ack: { epoch: 1, sequence: ack },
        credits: 1,
        active: 0,
        queued: 0,
        draining: false,
        frames: [],
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "durable-recovery-host",
      );
    };

    try {
      await peer.connect();
      publishAck(-1);
      let outcomeSequence = -1;
      for (let turn = 0; turn < 100 && outcomeSequence < 0; turn++) {
        await Bun.sleep(2);
        const frames = providers.get("data")!.awareness.getLocalState()
          ?.ghostprovV2Data?.frames ?? [];
        outcomeSequence = frames.find((frame: any) => frame.kind === "response_error")?.sequence ?? -1;
      }
      expect(outcomeSequence).toBeGreaterThanOrEqual(0);
      publishAck(outcomeSequence);
      await cleanupObserved;
      await Bun.sleep(20);
      expect((providers.get("data")!.awareness.getLocalState()
        ?.ghostprovV2Data?.frames ?? []).some((frame: any) => frame.kind === "recovery_complete"))
        .toBe(false);

      releaseCleanup();
      let markerSequence = -1;
      for (let turn = 0; turn < 100 && markerSequence < 0; turn++) {
        await Bun.sleep(2);
        const frames = providers.get("data")!.awareness.getLocalState()
          ?.ghostprovV2Data?.frames ?? [];
        markerSequence = frames.find((frame: any) => frame.kind === "recovery_complete")?.sequence ?? -1;
      }
      expect(markerSequence).toBeGreaterThan(outcomeSequence);
      publishAck(markerSequence);
      await Bun.sleep(10);
    } finally {
      releaseCleanup?.();
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("holds recovery_complete across split lanes until terminal response_ack is durably applied", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const binding: ProtocolBinding = {
      ...BINDING,
      protocolRevision: "r2",
      launchNonce: "launch-r2-terminal-fence",
    };
    const requestId = "req-recovered-terminal-fence";
    const terminalDigest = "d".repeat(64);
    let pendingTerminal = true;
    let releaseTerminalAck!: () => void;
    const terminalAckGate = new Promise<void>(resolve => { releaseTerminalAck = resolve; });
    let terminalAckStarted!: () => void;
    const terminalAckObserved = new Promise<void>(resolve => { terminalAckStarted = resolve; });
    let peer!: DenoAwarenessPeer;
    peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: "sidecar-terminal-fence",
      engine: {
        snapshot: () => ({ ready: true, credits: 1, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        hasRecoveredWork: () => true,
        hasPendingTerminalAcknowledgement: () => pendingTerminal,
        replayRecovered: async () => {
          peer.enqueue({
            kind: "response_terminal",
            requestId,
            requestDigest: "a".repeat(64),
            responseId: "resp_terminal_fence",
            eventCount: 1,
            byteCount: 1,
            transcriptSha256: terminalDigest,
            terminalStatus: "completed",
            finalEventIndex: 0,
            finalSha256: "b".repeat(64),
          }, "terminal", requestId);
        },
        accept: async (frame: any) => {
          if (frame.kind !== "response_ack") return;
          terminalAckStarted();
          await terminalAckGate;
          pendingTerminal = false;
        },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const controlDoc = new Y.Doc();
    const control = new Awareness(controlDoc);
    const dataDoc = new Y.Doc();
    const data = new Awareness(dataDoc);
    let controlRevision = 0;
    const hostBase = {
      protocol: "gp-awareness-v2" as const,
      protocolRevision: "r2" as const,
      schemaVersion: 2 as const,
      schemaHash: binding.schemaHash,
      buildHash: binding.buildHash,
      projectId: binding.projectId,
      sessionId: binding.sessionId,
      peerId: "host" as const,
      instanceId: "host-terminal-fence",
      targetInstanceId: "sidecar-terminal-fence",
      launchNonce: binding.launchNonce,
      leaderGeneration: binding.leaderGeneration,
      epoch: 1,
      credits: 1,
      active: 0,
      queued: 0,
      draining: false,
    };
    const publishControlAck = (ack: number) => {
      control.setLocalStateField("ghostprovV2Control", {
        ...hostBase,
        revision: controlRevision++,
        ack: { epoch: 1, sequence: ack },
        frames: [],
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(control, [controlDoc.clientID]),
        "terminal-fence-control",
      );
    };

    try {
      await peer.connect();
      publishControlAck(-1);
      let terminalSequence = -1;
      for (let turn = 0; turn < 100 && terminalSequence < 0; turn++) {
        await Bun.sleep(2);
        const frames = providers.get("data")!.awareness.getLocalState()
          ?.ghostprovV2Data?.frames ?? [];
        terminalSequence = frames.find((frame: any) => frame.kind === "response_terminal")?.sequence ?? -1;
      }
      expect(terminalSequence).toBeGreaterThanOrEqual(0);
      publishControlAck(terminalSequence);
      await Bun.sleep(15);
      expect((providers.get("data")!.awareness.getLocalState()
        ?.ghostprovV2Data?.frames ?? []).some((frame: any) => frame.kind === "recovery_complete"))
        .toBe(false);

      data.setLocalStateField("ghostprovV2Data", {
        ...hostBase,
        revision: 0,
        ack: { epoch: 1, sequence: terminalSequence },
        frames: [{
          kind: "response_ack",
          requestId,
          terminalDigest,
          epoch: 1,
          sequence: 0,
        }],
        lane: "data",
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(data, [dataDoc.clientID]),
        "terminal-fence-data",
      );
      await terminalAckObserved;
      await Bun.sleep(15);
      expect((providers.get("data")!.awareness.getLocalState()
        ?.ghostprovV2Data?.frames ?? []).some((frame: any) => frame.kind === "recovery_complete"))
        .toBe(false);

      releaseTerminalAck();
      let markerSequence = -1;
      for (let turn = 0; turn < 100 && markerSequence < 0; turn++) {
        await Bun.sleep(2);
        const frames = providers.get("data")!.awareness.getLocalState()
          ?.ghostprovV2Data?.frames ?? [];
        markerSequence = frames.find((frame: any) => frame.kind === "recovery_complete")?.sequence ?? -1;
      }
      expect(markerSequence).toBeGreaterThan(terminalSequence);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(0);
      publishControlAck(markerSequence);
      for (let turn = 0; turn < 100; turn++) {
        await Bun.sleep(2);
        if (providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits === 1) break;
      }
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(1);
    } finally {
      releaseTerminalAck?.();
      control.destroy();
      controlDoc.destroy();
      data.destroy();
      dataDoc.destroy();
      await peer.close();
    }
  });

  it("advertises no credits until the real engine becomes ready", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let ready = false;
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });

    await peer.connect();
    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
      .toBe(0);
    ready = true;
    peer.publishEngineState();
    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
      .toBe(8);
    await peer.close();
  });

  it("does not ACK synchronized host work before staged engine recovery", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const accepted: string[] = [];
    let ready = false;
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async (frame: { kind: string }) => { accepted.push(frame.kind); },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{ kind: "ping", sentAtMs: 7, epoch: 1, sequence: 0 }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(5);

      expect(accepted).toEqual([]);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: -1 });

      ready = true;
      peer.publishEngineState();
      await Bun.sleep(5);
      expect(accepted).toEqual(["ping"]);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 0 });
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("never lets host-bootstrap pin or trigger the steady recovery transport", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let replayCalls = 0;
    const engine = {
      snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
      hasRecoveredWork: () => true,
      replayRecovered: async () => { replayCalls++; },
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });
    const bootstrapDoc = new Y.Doc();
    const bootstrapRemote = new Awareness(bootstrapDoc);
    const hostDoc = new Y.Doc();
    const hostRemote = new Awareness(hostDoc);

    try {
      await peer.connect();
      bootstrapRemote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(-1, 1),
        peerId: "host-bootstrap",
        instanceId: "bootstrap-instance",
        lane: "control",
        liveness: { kind: "ping", sentAtMs: 1 },
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(bootstrapRemote, [bootstrapDoc.clientID]),
        "bootstrap-host",
      );
      await Bun.sleep(0);
      expect(replayCalls).toBe(0);

      hostRemote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(-1, 1),
        instanceId: "steady-host-instance",
        lane: "control",
        liveness: { kind: "ping", sentAtMs: 2 },
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(hostRemote, [hostDoc.clientID]),
        "steady-host",
      );
      await Bun.sleep(5);
      expect(replayCalls).toBe(1);
    } finally {
      bootstrapRemote.destroy();
      bootstrapDoc.destroy();
      hostRemote.destroy();
      hostDoc.destroy();
      await peer.close();
    }
  });

  it("finishes bootstrap before replaying a backpressured large recovered spool", async () => {
    const largeDelta = "x".repeat(2 * 1024 * 1024);
    const first = new SidecarHarness({
      behavior: input => completedUpstream(input, largeDelta),
    });
    await first.sendRequest({ model: "gpt-5.6-sol", input: "large recovery" });
    await first.settle();

    const backpressure = new OutputBackpressure({
      highFrames: 256,
      highBytes: 256 * 1024,
      lowFrames: 64,
      lowBytes: 64 * 1024,
      hardFrames: 1_024,
      hardBytes: 1_024 * 1024,
    });
    let peer: DenoAwarenessPeer | null = null;
    const restarted = new SidecarHarness({
      records: first.storage.records,
      terminals: first.storage.terminals,
      outputBackpressure: backpressure,
      emit: frame => peer?.enqueue(frame, frame.kind === "response_event" ? "data" : "terminal", "req-engine-1"),
    });
    const providers = new Map<"data" | "control", FakeProvider>();
    peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: TEST_BINDING,
      instanceId: "sidecar-recovered",
      engine: restarted.engine,
      outputBackpressure: backpressure,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const controlDoc = new Y.Doc();
    const controlRemote = new Awareness(controlDoc);
    const dataDoc = new Y.Doc();
    const dataRemote = new Awareness(dataDoc);
    let revision = 1;
    const host = (ack: number): AwarenessEnvelope<HostFrame> => ({
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: TEST_BINDING.schemaHash,
      buildHash: TEST_BINDING.buildHash,
      projectId: TEST_BINDING.projectId,
      sessionId: TEST_BINDING.sessionId,
      peerId: "host",
      instanceId: "host-transport-after-readiness",
      leaderGeneration: TEST_BINDING.leaderGeneration,
      epoch: 1,
      revision: revision++,
      ack: { epoch: 1, sequence: ack },
      credits: 128,
      active: 0,
      queued: 0,
      draining: false,
      frames: [],
      lane: "control",
    });

    try {
      const startedAt = performance.now();
      await restarted.engine.start();
      await peer.connect();
      expect(performance.now() - startedAt).toBeLessThan(250);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(8);
      expect(providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data)
        .toBeUndefined();
      expect(restarted.frames).toEqual([]);

      controlRemote.setLocalStateField("ghostprovV2Control", host(-1));
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(controlRemote, [controlDoc.clientID]),
        "bound-host-transport",
      );

      const received = new Map<number, SidecarFrame>();
      let sawTerminal = false;
      let sentTerminalAck = false;
      let recoveryDrained = false;
      for (let attempt = 0; attempt < 250; attempt++) {
        await Bun.sleep(5);
        const envelope = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
        if (envelope?.frames?.length) {
          for (const frame of envelope.frames) received.set(frame.sequence, frame);
          const high = envelope.frames.at(-1)!.sequence;
          controlRemote.setLocalStateField("ghostprovV2Control", host(high));
          applyAwarenessUpdate(
            providers.get("control")!.awareness,
            encodeAwarenessUpdate(controlRemote, [controlDoc.clientID]),
            "bound-host-ack",
          );
        }
        sawTerminal ||= [...received.values()].some(frame => frame.kind === "response_terminal");
        if (sawTerminal && !sentTerminalAck) {
          const terminal = [...received.values()].find(
            (frame): frame is Extract<SidecarFrame, { kind: "response_terminal" }> => (
              frame.kind === "response_terminal"
            ),
          )!;
          dataRemote.setLocalStateField("ghostprovV2Data", {
            ...host(envelope?.frames?.at(-1)?.sequence ?? -1),
            revision: 1,
            frames: [{
              kind: "response_ack",
              requestId: terminal.requestId,
              terminalDigest: terminal.transcriptSha256,
              epoch: 1,
              sequence: 0,
            }],
            lane: "data",
          });
          applyAwarenessUpdate(
            providers.get("data")!.awareness,
            encodeAwarenessUpdate(dataRemote, [dataDoc.clientID]),
            "bound-host-terminal-ack",
          );
          sentTerminalAck = true;
        }
        if (sawTerminal && !envelope) {
          recoveryDrained = true;
          break;
        }
      }

      expect(recoveryDrained).toBe(true);
      await Bun.sleep(0);
      expect([...received.values()].some(frame => frame.kind === "response_event")).toBe(true);
      expect([...received.values()].some(frame => frame.kind === "response_terminal")).toBe(true);
      expect(restarted.engine.snapshot().ambiguous).toBe(0);
    } finally {
      controlRemote.destroy();
      controlDoc.destroy();
      dataRemote.destroy();
      dataDoc.destroy();
      await peer.closeGracefully();
      backpressure.close();
    }
  }, 10_000);

  it("closes both split providers when either lane cannot connect", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        provider.failConnect = lane === "control";
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });

    await expect(peer.connect()).rejects.toThrow("control connection failed");
    expect([...providers.values()].every(provider => provider.destroyed)).toBe(true);
  });

  it("aborts an in-progress connection and removes temporary listeners", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: false, credits: 0, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        provider.holdConnect = true;
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 60_000,
    });

    const connecting = peer.connect();
    await Bun.sleep(0);
    await peer.close();
    await expect(connecting).rejects.toMatchObject({ name: "AbortError" });
    for (const provider of providers.values()) {
      expect(provider.destroyed).toBe(true);
      expect(provider.listeners.get("sync")?.size ?? 0).toBe(0);
      expect(provider.listeners.get("connection-error")?.size ?? 0).toBe(0);
      expect(provider.listeners.get("connection-close")?.size ?? 0).toBe(0);
    }
  });

  it("rejects request-local accept failures without killing healthy streams", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const failures: Array<{ error: Error; kind: string }> = [];
    const accepted: string[] = [];
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async (frame: { kind: string }) => {
          accepted.push(frame.kind);
          if (accepted.length === 1) throw new Error("unsafe internal detail");
        },
      } as any,
      onAcceptError: (error, frame) => failures.push({ error, kind: frame.kind }),
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{
          kind: "request_open",
          requestId: "req-failure",
          requestDigest: "a".repeat(64),
          decodedBytes: 1,
          encodedBytes: 1,
          encoding: "identity",
          partCount: 1,
          epoch: 1,
          sequence: 0,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(10);

      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 2),
        lane: "data",
        frames: [{
          kind: "request_data",
          requestId: "req-failure",
          partIndex: 0,
          offset: 0,
          dataBase64Url: "eA",
          epoch: 1,
          sequence: 1,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(10);

      expect(accepted).toEqual(["request_open", "request_data"]);
      expect(failures).toHaveLength(0);
      expect(providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data.frames)
        .toContainEqual(expect.objectContaining({
          kind: "request_reject",
          requestId: "req-failure",
          code: "sidecar_accept_failed",
        }));
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("escalates when scheduler saturation prevents an in-band request rejection", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const failures: Array<{ error: Error; kind: string }> = [];
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => { throw new Error("request-local failure"); },
      } as any,
      onAcceptError: (error, frame) => failures.push({ error, kind: frame.kind }),
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 60_000,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      let saturated = false;
      for (let index = 0; index < 5_000; index++) {
        try {
          peer.enqueue({
            kind: "response_error",
            requestId: "saturation",
            code: "synthetic",
            message: "bounded",
            ambiguous: false,
          }, "terminal", "saturation");
        } catch {
          saturated = true;
          break;
        }
      }
      expect(saturated).toBe(true);

      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{
          kind: "request_open",
          requestId: "req-saturated-reject",
          requestDigest: "a".repeat(64),
          decodedBytes: 1,
          encodedBytes: 1,
          encoding: "identity",
          partCount: 1,
          epoch: 1,
          sequence: 0,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(20);

      expect(failures).toHaveLength(1);
      expect(failures[0]!.kind).toBe("request_open");
      expect(failures[0]!.error.name).toBe("V2SchedulerOverflowError");
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("escalates durable control acceptance failures to lifecycle shutdown", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const fatalKinds: string[] = [];
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => { throw new Error("journal unavailable"); },
      } as any,
      onAcceptError: (_error, frame) => fatalKinds.push(frame.kind),
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{
          kind: "response_ack",
          requestId: "req-terminal",
          terminalDigest: "a".repeat(64),
          epoch: 1,
          sequence: 0,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(5);
      expect(fatalKinds).toEqual(["response_ack"]);
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("bounds close even when an accept handler ignores cancellation", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let acceptStarted!: () => void;
    const accepting = new Promise<void>(resolve => { acceptStarted = resolve; });
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => {
          acceptStarted();
          await new Promise<void>(() => undefined);
        },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
      closeTimeoutMs: 10,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    await peer.connect();
    remote.setLocalStateField("ghostprovV2Data", {
      ...hostEnvelope(-1, 1),
      lane: "data",
      frames: [{ kind: "ping", sentAtMs: 1, epoch: 1, sequence: 0 }],
    });
    applyAwarenessUpdate(
      providers.get("data")!.awareness,
      encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
      "test-host",
    );
    await accepting;
    const startedAt = performance.now();
    await expect(peer.close()).rejects.toMatchObject({
      name: "SidecarCloseError",
      code: "timeout",
    });

    expect(performance.now() - startedAt).toBeLessThan(250);
    expect([...providers.values()].every(provider => provider.destroyed)).toBe(true);
    remote.destroy();
    remoteDoc.destroy();
  });

  it("publishes only the successfully processed host ACK cursor", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let acceptStarted!: () => void;
    const accepting = new Promise<void>(resolve => { acceptStarted = resolve; });
    let releaseAccept!: () => void;
    const blocked = new Promise<void>(resolve => { releaseAccept = resolve; });
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => { acceptStarted(); await blocked; },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{ kind: "ping", sentAtMs: 1, epoch: 1, sequence: 0 }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await accepting;
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: -1 });

      releaseAccept();
      await Bun.sleep(5);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 0 });
    } finally {
      releaseAccept();
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("coalesces a processed host flight to its high ACK and replays a lost ACK", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 25,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);
    const frames = Array.from({ length: 32 }, (_, sequence) => ({
      kind: "request_open" as const,
      requestId: `req-ack-${sequence}`,
      requestDigest: "a".repeat(64),
      decodedBytes: 1,
      encodedBytes: 1,
      encoding: "identity" as const,
      partCount: 1,
      epoch: 1,
      sequence,
    }));

    try {
      await peer.connect();
      let controlPublications = 0;
      providers.get("control")!.awareness.on("update", () => { controlPublications++; });
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames,
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(30);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 31 });
      expect(controlPublications).toBeLessThanOrEqual(2);

      controlPublications = 0;
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 2),
        lane: "data",
        frames,
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await Bun.sleep(10);
      expect(controlPublications).toBe(1);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 31 });
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("lets drain control bypass a stalled request while ACK stays contiguous", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let requestStarted!: () => void;
    const started = new Promise<void>(resolve => { requestStarted = resolve; });
    let releaseRequest!: () => void;
    const blocked = new Promise<void>(resolve => { releaseRequest = resolve; });
    let drainSeen!: () => void;
    const drained = new Promise<void>(resolve => { drainSeen = resolve; });
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async (frame: { kind: string }) => {
          if (frame.kind === "request_open") {
            requestStarted();
            await blocked;
          } else if (frame.kind === "drain") {
            drainSeen();
          }
        },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{
          kind: "request_open",
          requestId: "req-stalled",
          requestDigest: "a".repeat(64),
          decodedBytes: 1,
          encodedBytes: 1,
          encoding: "identity",
          partCount: 1,
          epoch: 1,
          sequence: 0,
        }, {
          kind: "drain",
          reason: "shutdown",
          epoch: 1,
          sequence: 1,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await started;
      await drained;
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: -1 });

      releaseRequest();
      await Bun.sleep(5);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 1 });
    } finally {
      releaseRequest();
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("keeps request cancellation behind earlier work for the same request", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    let openStarted!: () => void;
    const started = new Promise<void>(resolve => { openStarted = resolve; });
    let releaseOpen!: () => void;
    const blocked = new Promise<void>(resolve => { releaseOpen = resolve; });
    const accepted: string[] = [];
    const ordering: string[] = [];
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async (frame: { kind: string }) => {
          accepted.push(frame.kind);
          ordering.push(`accept:${frame.kind}`);
          if (frame.kind === "request_open") {
            openStarted();
            await blocked;
          }
        },
        preemptRequest: () => ordering.push("preempt"),
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      remote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1, 1),
        lane: "data",
        frames: [{
          kind: "request_open",
          requestId: "req-ordered-cancel",
          requestDigest: "a".repeat(64),
          decodedBytes: 1,
          encodedBytes: 1,
          encoding: "identity",
          partCount: 1,
          epoch: 1,
          sequence: 0,
        }, {
          kind: "request_cancel",
          requestId: "req-ordered-cancel",
          reason: "client stopped",
          epoch: 1,
          sequence: 1,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host",
      );
      await started;
      await Bun.sleep(0);
      expect(accepted).toEqual(["request_open"]);
      expect(ordering).toEqual(["accept:request_open", "preempt"]);

      releaseOpen();
      await Bun.sleep(5);
      expect(accepted).toEqual(["request_open", "request_cancel"]);
      expect(ordering).toEqual([
        "accept:request_open",
        "preempt",
        "accept:request_cancel",
      ]);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.ack)
        .toEqual({ epoch: 1, sequence: 1 });
    } finally {
      releaseOpen();
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("graceful close waits within its bound for final output ACK", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 128, active: 0, queued: 0, draining: true }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
      closeTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    await peer.connect();
    peer.enqueue({ kind: "drained", active: 0, queued: 0 }, "control", "control");
    await Bun.sleep(5);
    const flight = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data.frames;
    expect(flight).toHaveLength(1);

    const closing = peer.closeGracefully();
    await Bun.sleep(5);
    expect(providers.get("data")!.destroyed).toBe(false);
    remote.setLocalStateField("ghostprovV2Control", {
      ...hostEnvelope(flight[0].sequence, 1),
      lane: "control",
    });
    applyAwarenessUpdate(
      providers.get("control")!.awareness,
      encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
      "final-host-ack",
    );
    await closing;

    expect([...providers.values()].every(provider => provider.destroyed)).toBe(true);
    remote.destroy();
    remoteDoc.destroy();
  });

  it("acknowledges durable outcomes only after cumulative transport ACK", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const acknowledged: string[][] = [];
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 8, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
        acknowledgeDeliveredOutcomes: async (ids: readonly string[]) => {
          acknowledged.push([...ids]);
        },
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      peer.enqueue({
        kind: "response_error",
        requestId: "req-provider-error",
        code: "rate_limit_exceeded",
        message: "limited",
        ambiguous: false,
      }, "terminal", "req-provider-error");
      await Bun.sleep(5);
      expect(acknowledged).toEqual([]);
      const flight = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data.frames;
      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(flight.at(-1).sequence, 1),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "outcome-ack",
      );
      await Bun.sleep(5);
      expect(acknowledged).toEqual([["req-provider-error"]]);
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("reports a typed non-clean close when reliable output remains unacknowledged", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ ready: true, credits: 0, active: 0, queued: 0, draining: true }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
      closeTimeoutMs: 10,
    });

    await peer.connect();
    peer.enqueue({ kind: "drained", active: 0, queued: 0 }, "control", "control");
    await Bun.sleep(5);
    await expect(peer.closeGracefully()).rejects.toMatchObject({
      name: "SidecarCloseError",
      code: "output",
    });
    expect([...providers.values()].every(provider => provider.destroyed)).toBe(true);
  });

  it("answers control-lane liveness outside stalled reliable and backpressure paths", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const acceptedKinds: string[] = [];
    let signalAccepted!: () => void;
    const accepted = new Promise<void>(resolve => { signalAccepted = resolve; });
    let releaseAccept!: () => void;
    const acceptBlocker = new Promise<void>(resolve => { releaseAccept = resolve; });
    let hostCeilingUpdates = 0;
    let backpressureUpdates = 0;
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => { hostCeilingUpdates++; },
      accept: async (frame: { kind: string }) => {
        acceptedKinds.push(frame.kind);
        signalAccepted();
        await acceptBlocker;
      },
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      outputBackpressure: {
        update: () => { backpressureUpdates++; },
      } as any,
      publicationIntervalMs: 60_000,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    const dataDoc = new Y.Doc();
    const dataRemote = new Awareness(dataDoc);
    const controlDoc = new Y.Doc();
    const controlRemote = new Awareness(controlDoc);

    try {
      await peer.connect();
      dataRemote.setLocalStateField("ghostprovV2Data", {
        ...hostEnvelope(-1),
        lane: "data",
        frames: [{
          kind: "drain",
          reason: "hold reliable accept chain",
          epoch: 1,
          sequence: 0,
        }],
      });
      applyAwarenessUpdate(
        providers.get("data")!.awareness,
        encodeAwarenessUpdate(dataRemote, [dataDoc.clientID]),
        "test-host-data",
      );
      await accepted;
      const backpressureBeforeProbe = backpressureUpdates;

      controlRemote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(-1),
        lane: "control",
        liveness: { kind: "ping", sentAtMs: 12_345.5 },
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(controlRemote, [controlDoc.clientID]),
        "test-host-control",
      );

      const pong = providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control;
      expect(pong).toMatchObject({
        lane: "control",
        frames: [],
        liveness: { kind: "pong", sentAtMs: 12_345.5 },
      });
      expect(pong.liveness).toEqual({ kind: "pong", sentAtMs: 12_345.5 });
      expect(acceptedKinds).toEqual(["drain"]);
      expect(hostCeilingUpdates).toBe(0);
      expect(backpressureUpdates).toBe(backpressureBeforeProbe);
    } finally {
      releaseAccept();
      dataRemote.destroy();
      dataDoc.destroy();
      controlRemote.destroy();
      controlDoc.destroy();
      await peer.close();
    }
  });

  it("keeps response flights separate from frame-free host ACK control", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane: "data" | "control") => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });

    try {
      await peer.connect();
      expect([...providers.keys()].sort()).toEqual(["control", "data"]);
      const data = providers.get("data")!;
      const control = providers.get("control")!;
      expect(control.awareness.getLocalState()?.ghostprovV2Control)
        .toMatchObject({ lane: "control", frames: [], credits: 8 });
      expect(data.awareness.getLocalState()?.ghostprovV2Data).toBeUndefined();

      for (let index = 0; index < 64; index++) {
        peer.enqueue(responseEvent(index), "data", `req-${index % 16}`);
      }
      await Bun.sleep(10);
      const first = data.awareness.getLocalState()?.ghostprovV2Data;
      expect(first.frames).toHaveLength(32);
      expect(first.lane).toBe("data");
      const firstRevision = first.revision;
      await Bun.sleep(10);
      expect(data.awareness.getLocalState()?.ghostprovV2Data.revision).toBe(firstRevision);

      const high = first.frames.at(-1).sequence;
      const remoteDoc = new Y.Doc();
      const remote = new Awareness(remoteDoc);
      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(high, 1),
        lane: "control",
      });
      applyAwarenessUpdate(
        control.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-control",
      );
      await Bun.sleep(10);

      expect(data.awareness.getLocalState()?.ghostprovV2Data.revision)
        .toBeGreaterThan(firstRevision);
      expect(control.awareness.getLocalState()?.ghostprovV2Control.frames).toEqual([]);
      remote.destroy();
      remoteDoc.destroy();
    } finally {
      await peer.close();
    }
  });

  it("waits for Y-Sweet sync before publishing sidecar readiness", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      connectTimeoutMs: 100,
    });

    await peer.connect();

    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control).toMatchObject({
      peerId: "sidecar",
      instanceId: "sidecar-instance-1",
      credits: 8,
      lane: "control",
    });
    await peer.close();
    expect([...providers.values()].every(provider => provider.destroyed)).toBe(true);
  });

  it("coalesces burst frames and never republishes an unchanged full window", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 16, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    providers.get("data")!.awareness.on("update", () => { publications++; });

    for (let eventIndex = 0; eventIndex < 64; eventIndex++) {
      peer.enqueue({
        kind: "response_event",
        requestId: `req-${eventIndex % 16}`,
        eventIndex,
        event: "response.output_text.delta",
        partIndex: 0,
        partCount: 1,
        offset: 0,
        dataBase64Url: "eA",
      }, "data", `req-${eventIndex % 16}`);
    }
    await Bun.sleep(30);

    const envelope = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
    expect(envelope.frames).toHaveLength(32);
    expect(publications).toBe(1);
    await peer.close();
  });

  it("does not overlap spaced data while one peer-wide flight is active", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 16, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 25,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    providers.get("data")!.awareness.on("update", () => { publications++; });

    for (let eventIndex = 0; eventIndex < 16; eventIndex++) {
      peer.enqueue(responseEvent(eventIndex), "data", `req-${eventIndex}`);
      await Bun.sleep(5);
    }
    await Bun.sleep(30);

    expect(publications).toBe(1);
    expect(providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data.frames.length)
      .toBeGreaterThan(0);
    await peer.close();
  });

  it("measures sidecar RTT from the first send of an acknowledged sequence", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 1, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const sampler = new SidecarResourceSampler({ enabled: false, intervalMs: 1_000 });
    const diagnostics = new SidecarDiagnosticsObserver({ enabled: true, sampler });
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      diagnostics,
      publicationIntervalMs: 5,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    peer.enqueue({
      kind: "response_batch",
      requestId: "req-rtt",
      firstEventIndex: 0,
      items: ["a", "b", "c"].map(value => ({
        event: "response.output_text.delta",
        dataBase64Url: value,
      })),
    }, "data", "req-rtt");
    await Bun.sleep(15);
    expect(providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data.frames)
      .toHaveLength(1);

    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);
    remote.setLocalStateField("ghostprovV2Control", {
      ...hostEnvelope(0),
      lane: "control",
    });
    await Bun.sleep(10);
    applyAwarenessUpdate(
      providers.get("control")!.awareness,
      encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
      "test-host",
    );
    await Bun.sleep(0);

    const rtt = providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control
      .diagnostics?.transport?.roundTripEwmaMs;
    expect(rtt).toBeNumber();
    expect(rtt).toBeGreaterThan(0);
    expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.diagnostics?.transport)
      .toMatchObject({
        pacedPublicationsTotal: 2,
        urgentPublicationsTotal: 1,
        logicalEventsTotal: 3,
        responseBatchFramesTotal: 1,
        batchedEventsTotal: 3,
      });
    remote.destroy();
    remoteDoc.destroy();
    await peer.close();
  });

  it("restores full credits after one stall and derates only after a repeated stall", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      transportStallMs: 30,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      peer.enqueue(responseEvent(0, "req-recover"), "data", "req-recover");
      await Bun.sleep(10);

      const first = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
      expect(first.frames).toHaveLength(1);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(8);

      await Bun.sleep(50);
      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(-1, 1),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-stalled",
      );
      await Bun.sleep(10);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(0);

      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(first.frames[0].sequence, 2),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-drained",
      );
      await Bun.sleep(10);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(8);

      peer.enqueue(responseEvent(1, "req-recover"), "data", "req-recover");
      await Bun.sleep(10);
      const second = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
      await Bun.sleep(50);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(0);
      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(second.frames.at(-1).sequence, 3),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-second-drained",
      );
      await Bun.sleep(10);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(4);
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("does not derate when an ACK arrives while queued output keeps congestion hysteresis high", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: {
        snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
        setHostCeiling: () => undefined,
        accept: async () => undefined,
      } as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 1,
      retransmitMs: 60_000,
      transportStallMs: 30,
      connectTimeoutMs: 100,
    });
    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);

    try {
      await peer.connect();
      peer.enqueue(responseEvent(0, "req-busy-ack"), "data", "req-busy-ack");
      await Bun.sleep(10);
      peer.enqueue(responseEvent(1, "req-busy-ack"), "data", "req-busy-ack");
      await Bun.sleep(40);
      const first = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
      expect(first.frames).toHaveLength(1);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(0);

      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(first.frames[0].sequence, 1),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-busy-ack",
      );
      await Bun.sleep(5);
      const second = providers.get("data")!.awareness.getLocalState()?.ghostprovV2Data;
      expect(second.frames).toHaveLength(1);

      remote.setLocalStateField("ghostprovV2Control", {
        ...hostEnvelope(second.frames[0].sequence, 2),
        lane: "control",
      });
      applyAwarenessUpdate(
        providers.get("control")!.awareness,
        encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
        "test-host-busy-drained",
      );
      await Bun.sleep(10);
      expect(providers.get("control")!.awareness.getLocalState()?.ghostprovV2Control.credits)
        .toBe(8);
    } finally {
      remote.destroy();
      remoteDoc.destroy();
      await peer.close();
    }
  });

  it("backs off repeated full-window publication until ACK progress resumes", async () => {
    const providers = new Map<"data" | "control", FakeProvider>();
    const engine = {
      snapshot: () => ({ credits: 128, active: 1, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    };
    const peer = new DenoAwarenessPeer({
      credential: {
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: "y-secret",
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance-1",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => {
        const provider = new FakeProvider(doc);
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 5,
      retransmitMs: 50,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    providers.get("data")!.awareness.on("update", () => { publications++; });
    peer.enqueue(responseEvent(0, "req-1"), "data", "req-1");
    await Bun.sleep(10);
    expect(publications).toBe(1);

    await Bun.sleep(70);
    expect(publications).toBe(2);
    await Bun.sleep(60);
    expect(publications).toBe(2);
    await peer.close();
  });
});
