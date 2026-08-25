import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import { DenoAwarenessPeer, type DenoAwarenessProviderLike } from "../sandbox/v2/awareness-peer";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { projectId } from "../src/sandbox/types";
import { SplitAwarenessChannel } from "../src/v2/awareness/split-channel";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../src/v2/awareness/y-sweet-channel";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type {
  HostFrame,
  ProtocolBinding,
  ReliableFrame,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import { V2TransportSession } from "../src/v2/transport/session";

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

class HubProvider implements AwarenessProviderLike, DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  connected = false;

  constructor(
    readonly hub: AwarenessHub,
    readonly doc: Y.Doc,
    readonly label: string,
  ) {
    this.awareness = new Awareness(doc);
    this.awareness.on("update", this.#onUpdate);
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
    if (this.connected) return;
    this.connected = true;
    this.hub.connect(this);
    queueMicrotask(() => this.emit("sync", true));
  }

  disconnect(): void {
    if (!this.connected) return;
    this.connected = false;
    this.hub.disconnect(this);
  }

  destroy(): void {
    this.disconnect();
    this.awareness.off("update", this.#onUpdate);
    this.awareness.destroy();
  }

  emit(event: string, ...args: any[]): void {
    for (const listener of this.listeners.get(event) ?? []) listener(...args);
  }

  readonly #onUpdate = (
    change: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ): void => {
    if (origin === this.hub) return;
    this.hub.broadcast(this, [...change.added, ...change.updated, ...change.removed]);
  };
}

class AwarenessHub {
  readonly providers = new Set<HubProvider>();
  readonly bytesByLabel = new Map<string, number>();

  create(doc: Y.Doc, label: string): HubProvider {
    return new HubProvider(this, doc, label);
  }

  connect(provider: HubProvider): void {
    for (const existing of this.providers) {
      this.copyLocal(existing, provider);
      this.copyLocal(provider, existing);
    }
    this.providers.add(provider);
  }

  disconnect(provider: HubProvider): void {
    this.providers.delete(provider);
  }

  broadcast(source: HubProvider, clientIds: readonly number[]): void {
    if (clientIds.length === 0) return;
    const update = encodeAwarenessUpdate(source.awareness, [...clientIds]);
    this.bytesByLabel.set(
      source.label,
      (this.bytesByLabel.get(source.label) ?? 0) + update.byteLength,
    );
    for (const target of this.providers) {
      if (target !== source) applyAwarenessUpdate(target.awareness, update, this);
    }
  }

  copyLocal(source: HubProvider, target: HubProvider): void {
    if (!source.awareness.getLocalState()) return;
    const update = encodeAwarenessUpdate(source.awareness, [source.doc.clientID]);
    applyAwarenessUpdate(target.awareness, update, this);
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error("split-flight condition timed out");
    await Bun.sleep(2);
  }
}

describe("V2 split-flight production components", () => {
  it("commits a request through independent host and sidecar lane providers", async () => {
    const hub = new AwarenessHub();
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const hostLane = (
      lane: "data" | "control",
      field: "ghostprovV2Data" | "ghostprovV2Control",
    ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: `host:${lane}`,
      field,
      parseRemote: value => parseAwarenessEnvelope(value, BINDING, "sidecar"),
      providerFactory: doc => hub.create(doc, `host-${lane}`),
      syncTimeoutMs: 100,
    });
    const channel = new SplitAwarenessChannel({
      data: hostLane("data", "ghostprovV2Data"),
      control: hostLane("control", "ghostprovV2Control"),
    });
    const session = new V2TransportSession({
      binding: BINDING,
      channel,
      maxResponseBytes: 2 * 1024 * 1024,
      publicationIntervalMs: 1,
      retransmitMs: 50,
    });

    let peer!: DenoAwarenessPeer;
    let opened: Extract<ReliableFrame<HostFrame>, { kind: "request_open" }> | null = null;
    let commits = 0;
    const engine = {
      snapshot: () => ({ credits: 128, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async (frame: ReliableFrame<HostFrame>) => {
        if (frame.kind === "request_open") opened = frame;
        if (frame.kind !== "request_commit" || !opened) return;
        commits++;
        peer.enqueue({
          kind: "request_ack",
          requestId: opened.requestId,
          requestDigest: opened.requestDigest,
          committed: true,
        }, "terminal", opened.requestId);
      },
    };
    peer = new DenoAwarenessPeer({
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding: BINDING,
      instanceId: "sidecar-instance",
      engine: engine as any,
      providerFactory: (doc, _credential, lane) => hub.create(doc, `sidecar-${lane}`),
      publicationIntervalMs: 1,
      retransmitMs: 50,
      connectTimeoutMs: 100,
      initialActivePerSandbox: 8,
      maxActivePerSandbox: 16,
    });

    try {
      await peer.connect();
      await session.connect();
      await waitFor(() => session.snapshot().credits === 8);
      const handle = await session.run({
        model: "gpt-5.6-sol",
        input: "split-flight-integration",
        store: false,
      });
      void handle.final.catch(() => undefined);

      expect(commits).toBe(1);
      expect(session.snapshot().committed).toBe(1);
      const hostControl = [...hub.providers]
        .find(provider => provider.label === "host-control")!
        .awareness.getLocalState()?.ghostprovV2Control;
      const sidecarControl = [...hub.providers]
        .find(provider => provider.label === "sidecar-control")!
        .awareness.getLocalState()?.ghostprovV2Control;
      expect(hostControl).toMatchObject({ lane: "control", frames: [] });
      expect(sidecarControl).toMatchObject({ lane: "control", frames: [] });
      expect(hub.bytesByLabel.get("host-control")).toBeLessThan(
        hub.bytesByLabel.get("host-data")!,
      );
    } finally {
      await session.close();
      await peer.close();
    }
  });
});
