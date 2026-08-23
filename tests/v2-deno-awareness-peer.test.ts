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
    queueMicrotask(() => this.emit("sync", true));
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
  it("waits for Y-Sweet sync before publishing sidecar readiness", async () => {
    let provider: FakeProvider | null = null;
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
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      connectTimeoutMs: 100,
    });

    await peer.connect();

    expect(provider!.awareness.getLocalState()?.ghostprovV2).toMatchObject({
      peerId: "sidecar",
      instanceId: "sidecar-instance-1",
      credits: 128,
    });
    await peer.close();
    expect(provider!.destroyed).toBe(true);
  });

  it("coalesces burst frames and never republishes an unchanged full window", async () => {
    let provider: FakeProvider | null = null;
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
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    provider!.awareness.on("update", () => { publications++; });

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

    const envelope = provider!.awareness.getLocalState()?.ghostprovV2;
    expect(envelope.frames).toHaveLength(64);
    expect(publications).toBe(1);
    await peer.close();
  });

  it("paces spaced data from every request on one peer-wide cadence", async () => {
    let provider: FakeProvider | null = null;
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
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      publicationIntervalMs: 25,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    provider!.awareness.on("update", () => { publications++; });

    for (let eventIndex = 0; eventIndex < 16; eventIndex++) {
      peer.enqueue(responseEvent(eventIndex), "data", `req-${eventIndex}`);
      await Bun.sleep(5);
    }
    await Bun.sleep(30);

    expect(publications).toBeGreaterThanOrEqual(3);
    expect(publications).toBeLessThanOrEqual(5);
    expect(provider!.awareness.getLocalState()?.ghostprovV2.frames).toHaveLength(16);
    await peer.close();
  });

  it("measures sidecar RTT from the first send of an acknowledged sequence", async () => {
    let provider: FakeProvider | null = null;
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
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      diagnostics,
      publicationIntervalMs: 5,
      retransmitMs: 60_000,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    peer.enqueue(responseEvent(0, "req-rtt"), "data", "req-rtt");
    await Bun.sleep(15);
    expect(provider!.awareness.getLocalState()?.ghostprovV2.frames).toHaveLength(1);

    const remoteDoc = new Y.Doc();
    const remote = new Awareness(remoteDoc);
    remote.setLocalStateField("ghostprovV2", hostEnvelope(0));
    await Bun.sleep(10);
    applyAwarenessUpdate(
      provider!.awareness,
      encodeAwarenessUpdate(remote, [remoteDoc.clientID]),
      "test-host",
    );
    await Bun.sleep(0);

    const rtt = provider!.awareness.getLocalState()?.ghostprovV2
      .diagnostics?.transport?.roundTripEwmaMs;
    expect(rtt).toBeNumber();
    expect(rtt).toBeGreaterThan(0);
    remote.destroy();
    remoteDoc.destroy();
    await peer.close();
  });

  it("backs off repeated full-window publication until ACK progress resumes", async () => {
    let provider: FakeProvider | null = null;
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
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      publicationIntervalMs: 5,
      retransmitMs: 50,
      connectTimeoutMs: 100,
    });
    await peer.connect();
    let publications = 0;
    provider!.awareness.on("update", () => { publications++; });
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
