import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../src/v2/awareness/y-sweet-channel";
import { FakeAwarenessNetwork } from "./support/fake-awareness";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const BINDING: ProtocolBinding = {
  projectId: PROJECT,
  sessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};

function hostEnvelope(epoch = 1): AwarenessEnvelope<HostFrame> {
  return {
    protocol: "gp-awareness-v2",
    schemaVersion: 1,
    schemaHash: BINDING.schemaHash,
    buildHash: BINDING.buildHash,
    projectId: PROJECT,
    sessionId: BINDING.sessionId,
    peerId: "host",
    instanceId: "host-instance",
    leaderGeneration: BINDING.leaderGeneration,
    epoch,
    revision: epoch,
    ack: { epoch, sequence: -1 },
    credits: 32,
    active: 0,
    queued: 0,
    draining: false,
    frames: [],
  };
}

function sidecarEnvelope(epoch = 1): AwarenessEnvelope<SidecarFrame> {
  return {
    ...hostEnvelope(epoch),
    peerId: "sidecar",
    instanceId: "sidecar-instance",
    credits: 128,
    frames: [],
  };
}

class FakeProvider implements AwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  destroyed = false;

  constructor(readonly doc: Y.Doc) {
    this.awareness = new Awareness(doc);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let values = this.listeners.get(event);
    if (!values) {
      values = new Set();
      this.listeners.set(event, values);
    }
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

describe("Awareness channels", () => {
  it("publishes one ephemeral field and clears peer presence on close", async () => {
    const network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>();
    const host = network.connectHost("host", PROJECT);
    const sidecar = network.connectSidecar("sidecar", PROJECT);
    const received: AwarenessEnvelope<HostFrame>[] = [];
    sidecar.subscribe(update => received.push(update.envelope));

    host.publish(hostEnvelope());
    await network.flushAll();

    expect(received).toEqual([hostEnvelope()]);
    expect(sidecar.peerIds()).toEqual(["host"]);
    expect(network.persistentWrites).toBe(0);
    await host.close();
    await network.flushAll();
    expect(sidecar.peerIds()).toEqual([]);
    await sidecar.close();
  });

  it("survives deterministic duplication, reordering, and dropped updates", async () => {
    const network = new FakeAwarenessNetwork<HostFrame, SidecarFrame>({
      drop: [2, 7],
      duplicate: [1, 5],
      reverseEvery: 3,
    });
    const host = network.connectHost("host", PROJECT);
    const sidecar = network.connectSidecar("sidecar", PROJECT);
    const seen = new Set<number>();
    sidecar.subscribe(update => seen.add(update.envelope.epoch));

    for (let epoch = 1; epoch <= 10; epoch++) host.publish(hostEnvelope(epoch));
    await network.flushAll();

    expect(Math.max(...seen)).toBe(10);
    expect(seen.size).toBeGreaterThanOrEqual(8);
    expect(network.persistentWrites).toBe(0);
    await host.close();
    await sidecar.close();
  });

  it("uses a dedicated empty Y.Doc and validates remote awareness state", async () => {
    let provider: FakeProvider | null = null;
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => {
        provider = new FakeProvider(doc);
        return provider;
      },
      syncTimeoutMs: 100,
    });
    const updates: AwarenessEnvelope<SidecarFrame>[] = [];
    channel.subscribe(update => updates.push(update.envelope));

    await channel.connect();
    channel.publish(hostEnvelope());
    expect(provider!.doc.share.size).toBe(0);
    expect(provider!.awareness.getLocalState()?.ghostprovV2).toEqual(hostEnvelope());

    const remoteDoc = new Y.Doc();
    const remoteAwareness = new Awareness(remoteDoc);
    remoteAwareness.setLocalStateField("ghostprovV2", sidecarEnvelope());
    applyAwarenessUpdate(
      provider!.awareness,
      encodeAwarenessUpdate(remoteAwareness, [remoteDoc.clientID]),
      "test",
    );
    expect(updates).toEqual([sidecarEnvelope()]);
    expect(channel.peerIds()).toEqual(["sidecar"]);

    remoteAwareness.setLocalStateField("ghostprovV2", { invalid: true });
    applyAwarenessUpdate(
      provider!.awareness,
      encodeAwarenessUpdate(remoteAwareness, [remoteDoc.clientID]),
      "test",
    );
    expect(updates).toHaveLength(1);

    await channel.close();
    expect(provider!.destroyed).toBe(true);
    expect(provider!.doc.share.size).toBe(0);
    remoteAwareness.destroy();
    remoteDoc.destroy();
  });

  it("rejects an in-progress connection immediately when closed", async () => {
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => new FakeProvider(doc),
      syncTimeoutMs: 1_000,
    });

    const pending = channel.connect();
    await channel.close();

    await expect(pending).rejects.toMatchObject({ kind: "closed" });
  });

  it("updates the credential source used by future reconnects", async () => {
    const first = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret-1",
      authorization: "full",
    });
    let current!: () => typeof first;
    const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: first,
      localPeerId: "host",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: (doc, credential) => {
        current = credential;
        return new FakeProvider(doc);
      },
      syncTimeoutMs: 100,
    });
    await channel.connect();
    const rotated = parseYSweetEnvelope({
      url: first.url,
      baseUrl: first.baseUrl,
      docId: PROJECT,
      token: "y-secret-2",
      authorization: "full",
    });

    channel.rotateCredential(rotated);

    expect(current().token.reveal()).toBe("y-secret-2");
    expect(() => channel.rotateCredential(parseYSweetEnvelope({
      url: first.url,
      baseUrl: first.baseUrl,
      docId: "69288e8b-c183-45f3-9131-d64b05b98928",
      token: "foreign",
      authorization: "full",
    }))).toThrow("another project");
    await channel.close();
  });
});
