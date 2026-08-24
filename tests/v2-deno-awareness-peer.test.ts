import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import { DenoAwarenessPeer } from "../sandbox/v2/awareness-peer";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import { SidecarDiagnosticsObserver } from "../src/v2/diagnostics/sidecar-observer";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";

const PROJECT = "e631d12c-d917-4434-b360-41b4715d6679";
const BINDING: ProtocolBinding = {
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

class FakeProvider {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  destroyed = false;
  failConnect = false;

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

  it("restores credits on the ACK that fully drains a stalled flight", async () => {
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

      await Bun.sleep(2_100);
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
