import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import { DenoAwarenessPeer } from "../sandbox/v2/awareness-peer";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type { ProtocolBinding } from "../src/v2/protocol/shared";

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
});
