import { describe, expect, it, spyOn } from "bun:test";
import * as Y from "yjs";
import { Awareness, applyAwarenessUpdate, encodeAwarenessUpdate, removeAwarenessStates } from "y-protocols/awareness";
import {
  DenoAwarenessPeer,
  type DenoAwarenessPeerOptions,
  type DenoAwarenessProviderLike,
} from "../sandbox/v2/r10-awareness-peer";
import type { PublicationTimer } from "../src/v2/protocol/publication-pacer";
import type {
  AwarenessEnvelope, AwarenessLane, HostFrame, ProtocolBinding,
  R2RequestStartFrame, ReliableFrame, SidecarFrame,
} from "../src/v2/protocol/shared";

const BINDING: ProtocolBinding = {
  protocolRevision: "r2", launchNonce: "r10-control-mirror-launch",
  projectId: "e631d12c-d917-4434-b360-41b4715d6679", sessionId: "r10-control-mirror-session",
  leaderGeneration: 4, buildHash: "a".repeat(64), schemaHash: "b".repeat(64),
};
const HOST = "r10-control-mirror-host";
const SIDECAR = "r10-control-mirror-sidecar";
const START: R2RequestStartFrame = {
  ...BINDING, protocolRevision: "r2", schemaVersion: 2, kind: "request_start",
  hostInstanceId: HOST, targetInstanceId: SIDECAR, requestId: "pending-response",
  requestDigest: "c".repeat(64), decodedBytes: 1, encodedBytes: 1,
  encoding: "identity", partCount: 1,
};

async function settle(): Promise<void> {
  for (let index = 0; index < 12; index++) await Promise.resolve();
}

class Clock {
  now = 1_000;
  readonly pending = new Map<unknown, { at: number; callback(): void }>();
  readonly #set = spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, delay = 0) =>
    this.set(callback, delay)) as typeof setTimeout);
  readonly #clear = spyOn(globalThis, "clearTimeout").mockImplementation((handle => {
    this.pending.delete(handle);
  }) as typeof clearTimeout);
  readonly publicationTimer: PublicationTimer = {
    now: () => this.now,
    set: (callback, delay) => this.set(callback, delay),
    clear: handle => { this.pending.delete(handle); },
    defer: callback => queueMicrotask(callback),
  };

  set(callback: () => void, delay: number): ReturnType<typeof setTimeout> {
    const handle = { unref() { return handle; } };
    this.pending.set(handle, { at: this.now + delay, callback });
    return handle as unknown as ReturnType<typeof setTimeout>;
  }

  async advance(milliseconds: number): Promise<void> {
    const target = this.now + milliseconds;
    for (let steps = 0; steps < 1_000; steps++) {
      await settle();
      const next = [...this.pending].filter(([, timer]) => timer.at <= target)
        .sort((left, right) => left[1].at - right[1].at)[0];
      if (!next) { this.now = target; await settle(); return; }
      this.now = Math.max(this.now, next[1].at);
      this.pending.delete(next[0]);
      next[1].callback();
    }
    throw new Error("Fake timer did not settle within its bound");
  }

  restore(): void { this.#set.mockRestore(); this.#clear.mockRestore(); }
}

class FakeProvider implements DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();
  connects = 0;
  disconnects = 0;
  behavior: "sync" | "hold" | "error" | "throw" = "sync";
  onConnect?: () => void;

  constructor(readonly doc: Y.Doc) { this.awareness = new Awareness(doc); }
  on(event: string, listener: (...args: any[]) => void): void {
    let listeners = this.listeners.get(event);
    if (!listeners) this.listeners.set(event, listeners = new Set());
    listeners.add(listener);
  }
  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }
  emit(event: string, ...args: unknown[]): void {
    for (const listener of [...this.listeners.get(event) ?? []]) listener(...args);
  }
  connect(): void {
    this.connects++;
    if (this.behavior === "throw") throw new Error("Synthetic connect failure");
    this.onConnect?.();
    queueMicrotask(() => {
      if (this.behavior === "sync") this.emit("sync", true);
      if (this.behavior === "error") this.emit("connection-error");
    });
  }
  disconnect(): void {
    this.disconnects++;
    this.emit("connection-close");
    removeAwarenessStates(this.awareness,
      [...this.awareness.getStates().keys()].filter(id => id !== this.doc.clientID), this);
  }
  destroy(): void { this.awareness.destroy(); }
}

function harness(options: { accept?: (frame: HostFrame) => Promise<void> } = {}) {
  const clock = new Clock();
  const providers = new Map<AwarenessLane, FakeProvider>();
  const docs = { data: new Y.Doc(), control: new Y.Doc(), foreign: new Y.Doc() };
  docs.data.clientID = 101; docs.control.clientID = 102; docs.foreign.clientID = 103;
  const host = { data: new Awareness(docs.data), control: new Awareness(docs.control), foreign: new Awareness(docs.foreign) };
  const accepted: HostFrame[] = [];
  const ceilings: number[] = [];
  const errors: Error[] = [];
  const peer = new DenoAwarenessPeer({
    binding: BINDING, instanceId: SIDECAR,
    credential: {
      url: "wss://prism.test/synthetic", baseUrl: "https://prism.test/synthetic",
      docId: BINDING.projectId, token: "synthetic-credential", authorization: "full",
      rotationKey: Buffer.alloc(32, 3).toString("base64url"),
    },
    engine: {
      snapshot: () => ({ ready: true, credits: 8, active: 1, queued: 0, draining: false }),
      setHostCeiling: (credits: number) => { ceilings.push(credits); },
      accept: async (frame: HostFrame) => { accepted.push(frame); await options.accept?.(frame); },
    } as unknown as DenoAwarenessPeerOptions["engine"],
    providerFactory: (doc, _credential, lane) => {
      doc.clientID = lane === "data" ? 201 : 202;
      const provider = new FakeProvider(doc); providers.set(lane, provider); return provider;
    },
    now: () => clock.now, publicationTimer: clock.publicationTimer,
    publicationIntervalMs: 60_000, retransmitMs: 60_000, transportStallMs: 60_000,
    connectTimeoutMs: 15_000, onEngineError: error => { errors.push(error); },
  });
  const controlProvider = providers.get("control")!;
  const dataProvider = providers.get("data")!;
  const envelope = (lane: AwarenessLane, revision: number, overrides: Record<string, unknown> = {}) => ({
    ...BINDING, protocol: "gp-awareness-v2", schemaVersion: 2, peerId: "host",
    instanceId: HOST, targetInstanceId: SIDECAR, lane, epoch: 1, revision,
    ack: { epoch: 1, sequence: -1 }, credits: 128, active: 0, queued: 0, draining: false,
    frames: [], ...overrides,
  });
  const deliver = (source: keyof typeof host, destinations: readonly AwarenessLane[]) => {
    const bytes = encodeAwarenessUpdate(host[source], [docs[source].clientID]);
    for (const lane of destinations) applyAwarenessUpdate(providers.get(lane)!.awareness, bytes, "synthetic-host");
  };
  const control = () => controlProvider.awareness.getLocalState()!.ghostprovV2Control as AwarenessEnvelope<SidecarFrame>;
  const data = () => dataProvider.awareness.getLocalState()!.ghostprovV2Data as AwarenessEnvelope<SidecarFrame> | null;
  return {
    clock, peer, providers, controlProvider, dataProvider, accepted, ceilings, errors, host, docs,
    control, data, deliver,
    native(revision: number, overrides: Record<string, unknown> = {}, source: "control" | "foreign" = "control") {
      host[source].setLocalStateField("ghostprovV2Control", envelope("control", revision, overrides));
      deliver(source, ["control"]);
    },
    mirror(revision: number, overrides: Record<string, unknown> = {}, source: "control" | "foreign" = "control") {
      host[source].setLocalStateField("ghostprovV2Control", envelope("control", revision, overrides));
      deliver(source, ["data"]);
    },
    sendData(revision: number, frames: readonly ReliableFrame<HostFrame>[] = []) {
      host.data.setLocalStateField("ghostprovV2Data", envelope("data", revision, { frames }));
      deliver("data", ["data"]);
    },
    enqueue(index: number, requestId = "pending-response") {
      peer.enqueue({ kind: "response_event", requestId, eventIndex: index,
        event: "response.output_text.delta", partIndex: 0, partCount: 1,
        offset: 0, dataBase64Url: "eA" }, "data", requestId);
    },
    async close() {
      try {
        await peer.close(); await settle();
        for (const awareness of Object.values(host)) awareness.destroy();
        for (const doc of Object.values(docs)) doc.destroy();
      } finally { clock.restore(); }
    },
  };
}

describe("R10 native CONTROL repair from bounded mirrored evidence", () => {
  it("repairs the stalled native lane while preserving its exact pending flight and requiring native proof", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.sendData(0);
      h.enqueue(0); h.peer.publishEngineState();
      const flight = h.data()!.frames;
      for (let index = 1; index <= 10; index++) h.enqueue(index);
      const localControl = h.controlProvider.awareness.getLocalState();
      const localData = h.dataProvider.awareness.getLocalState();
      h.controlProvider.onConnect = () => {
        expect(h.controlProvider.awareness.getLocalState()).toBe(localControl);
        expect(h.dataProvider.awareness.getLocalState()).toBe(localData);
        expect(() => h.peer.assertExecutionFence(START)).toThrow();
      };
      h.mirror(1, { ack: { epoch: 1, sequence: 0 }, credits: 7, liveness: { kind: "ping", sentAtMs: 123 } });
      await h.clock.advance(4_999);
      expect(h.controlProvider.connects).toBe(1);
      expect(h.data()!.frames).toBe(flight);
      expect(h.control().liveness).toBeUndefined();
      expect(h.ceilings).toEqual([128]);
      await h.clock.advance(1);
      expect(h.controlProvider.connects).toBe(2);
      expect(h.controlProvider.disconnects).toBe(1);
      expect(h.dataProvider.connects).toBe(1);
      expect(h.dataProvider.disconnects).toBe(0);
      expect(h.data()!.frames).toBe(flight);
      expect(h.controlProvider.doc.clientID).toBe(202);
      expect(h.dataProvider.doc.clientID).toBe(201);
      expect(() => h.peer.assertExecutionFence(START)).toThrow();
      expect(h.accepted).toEqual([]);
      expect(h.ceilings).toEqual([128]);
      // Only native receipt releases the original frame and supplies this pong.
      h.deliver("control", ["control"]); await settle(); h.peer.publishEngineState();
      expect(() => h.peer.assertExecutionFence(START)).not.toThrow();
      expect(h.control().liveness).toEqual({ kind: "pong", sentAtMs: 123 });
      expect(h.ceilings).toEqual([128, 7]);
      expect(h.data()!.frames.map(frame => frame.sequence)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
      expect(h.control().diagnostics!.transport.schedulerFrames).toBe(0);
      expect(h.errors).toEqual([]);
    } finally { await h.close(); }
  });

  it("cancels repair when native CONTROL catches up during grace", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(1);
      await h.clock.advance(4_000); h.deliver("control", ["control"]);
      await h.clock.advance(20_000);
      expect(h.controlProvider.connects).toBe(1);
      expect(() => h.peer.assertExecutionFence(START)).not.toThrow();
    } finally { await h.close(); }
  });

  it("starts grace at the first ahead mirror after idleness and preserves it across newer mirrors", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); await h.clock.advance(30_000);
      h.mirror(1); await h.clock.advance(4_000); h.mirror(2);
      await h.clock.advance(999); expect(h.controlProvider.connects).toBe(1);
      await h.clock.advance(1); expect(h.controlProvider.connects).toBe(2);
    } finally { await h.close(); }
  });

  it.each([
    ["wrong project", { projectId: "9cd01994-3c4a-42f1-ad4c-54a357718eb4" }],
    ["wrong session", { sessionId: "other-session" }],
    ["wrong generation", { leaderGeneration: 5 }],
    ["wrong build", { buildHash: "c".repeat(64) }],
    ["wrong schema", { schemaHash: "c".repeat(64) }],
    ["wrong nonce", { launchNonce: "other-launch" }],
    ["wrong host", { instanceId: "other-host" }],
    ["wrong target", { targetInstanceId: "other-sidecar" }],
    ["bootstrap peer", { peerId: "host-bootstrap", instanceId: BINDING.launchNonce }],
    ["wrong lane", { lane: "data" }],
    ["omitted lane", { lane: undefined }],
    ["malformed revision", { revision: -1 }],
    ["reliable frames", { frames: [{ kind: "request_cancel", requestId: "pending-response", reason: "client", epoch: 1, sequence: 0 }] }],
    ["equal revision", { revision: 0 }],
  ] as const)("rejects %s mirror evidence", async (_name, overrides) => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(1, overrides);
      await h.clock.advance(20_000);
      expect(h.controlProvider.connects).toBe(1);
      expect(h.accepted).toEqual([]);
      expect(h.ceilings).toEqual([128]);
    } finally { await h.close(); }
  });

  it("requires an earlier native election and ignores a foreign mirrored client", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.mirror(1); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(1);
      expect(() => h.peer.assertExecutionFence(START)).toThrow();
      h.native(2); h.mirror(3, {}, "foreign"); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(1);
    } finally { await h.close(); }
  });

  it.each(["null", "removed", "rollback", "wrong binding"] as const)("cancels a hint replaced by %s", async replacement => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(2); await h.clock.advance(4_000);
      if (replacement === "null") h.host.control.setLocalStateField("ghostprovV2Control", null);
      else if (replacement === "removed") h.host.control.setLocalState(null);
      else h.mirror(replacement === "rollback" ? 1 : 3, replacement === "wrong binding" ? { buildHash: "c".repeat(64) } : {});
      h.deliver("control", ["data"]); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(1);
      // Rediscovery of the same revision cannot refresh invalidated evidence.
      h.mirror(2); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(1);
    } finally { await h.close(); }
  });

  it("expires late timer evidence without letting duplicate state or fresh DATA rearm it", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(1);
      h.clock.now += 10_001; await h.clock.advance(0);
      expect(h.controlProvider.connects).toBe(1);
      for (let revision = 0; revision < 3; revision++) {
        h.mirror(1, { credits: 100 + revision }); h.sendData(revision);
        await h.clock.advance(5_000);
      }
      expect(h.controlProvider.connects).toBe(1);
      h.mirror(2); await h.clock.advance(4_999);
      expect(h.controlProvider.connects).toBe(1);
      await h.clock.advance(1); expect(h.controlProvider.connects).toBe(2);
    } finally { await h.close(); }
  });

  it("caps attempts, requires new mirror evidence, and does not reset its budget on stale native resync", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(1); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(2);
      for (let revision = 2; revision <= 3; revision++) {
        h.native(0); h.mirror(revision);
        await h.clock.advance(9_999); expect(h.controlProvider.connects).toBe(revision);
        await h.clock.advance(1); expect(h.controlProvider.connects).toBe(revision + 1);
      }
      h.native(0); h.mirror(4); await h.clock.advance(15_000);
      expect(h.controlProvider.connects).toBe(4);
      h.native(1); h.mirror(5); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(5);
    } finally { await h.close(); }
  });

  it("does not reuse a consumed mirror after newer native proof that is still behind it", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(5); h.mirror(10); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(2);
      h.native(6); h.mirror(10); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(2);
      h.mirror(11); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(3);
    } finally { await h.close(); }
  });

  it.each([
    ["missing explicit lane", { lane: undefined }],
    ["reliable frames", { frames: [{ kind: "request_cancel", requestId: "pending-response", reason: "client", epoch: 1, sequence: 0 }] }],
    ["ACK of unsent output", { ack: { epoch: 1, sequence: 0 } }],
  ] as const)("does not replenish the repair budget with native CONTROL containing %s", async (_name, overrides) => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0);
      for (let revision = 1; revision <= 3; revision++) {
        h.mirror(revision); await h.clock.advance(revision === 1 ? 5_000 : 10_000);
      }
      expect(h.controlProvider.connects).toBe(4);
      h.native(10, overrides); h.mirror(11); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(4);
    } finally { await h.close(); }
  });

  it("updates the hint owner only through newer native proof", async () => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.mirror(1);
      h.native(2, {}, "foreign"); h.mirror(3); await h.clock.advance(10_000);
      expect(h.controlProvider.connects).toBe(1);
      h.mirror(4, {}, "foreign"); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(2);
    } finally { await h.close(); }
  });

  it.each(["hold", "error", "throw"] as const)("keeps %s repair bounded and leaves dynamic authority revoked", async behavior => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.controlProvider.behavior = behavior;
      h.mirror(1); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(2);
      expect(() => h.peer.assertExecutionFence(START)).toThrow();
      h.mirror(2); await h.clock.advance(5_000);
      expect(h.controlProvider.connects).toBe(2);
      // A pending synchronization times out after 15s. Without a currently
      // fresh new revision, its eventual completion cannot trigger another try.
      await h.clock.advance(15_001);
      expect(h.controlProvider.connects).toBe(behavior === "hold" ? 2 : 3);
      expect(h.controlProvider.listeners.get("sync")?.size ?? 0).toBe(0);
      expect(h.errors).toEqual([]);
    } finally { await h.close(); }
  });

  it.each(["pending", "in-flight"] as const)("closes with a %s reconnect without late reopening or leaked listeners", async phase => {
    const h = harness();
    try {
      await h.peer.connect(); h.native(0); h.controlProvider.behavior = "hold"; h.mirror(1);
      if (phase === "in-flight") await h.clock.advance(5_000);
      const connects = h.controlProvider.connects;
      await h.peer.close(); await settle();
      h.controlProvider.emit("sync", true);
      await h.clock.advance(30_000);
      expect(h.controlProvider.connects).toBe(connects);
      expect(h.clock.pending.size).toBe(0);
      for (const listeners of h.controlProvider.listeners.values()) expect(listeners.size).toBe(0);
      expect(() => h.peer.assertExecutionFence(START)).toThrow();
    } finally { await h.close(); }
  });

  it("keeps cancellation processing durable while a CONTROL repair is pending", async () => {
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const h = harness({ accept: () => gate });
    try {
      await h.peer.connect(); h.native(0); h.mirror(1);
      h.sendData(0, [{ kind: "request_cancel", requestId: "pending-response", reason: "client", epoch: 1, sequence: 0 }]);
      await settle(); expect(h.accepted).toHaveLength(1);
      expect(h.control().ack.sequence).toBe(-1);
      await h.clock.advance(5_000);
      expect(h.control().ack.sequence).toBe(-1);
      h.deliver("control", ["control"]); release(); await settle(); h.peer.publishEngineState();
      expect(h.control().ack.sequence).toBe(0);
      expect(h.accepted).toHaveLength(1);
    } finally { release(); await h.close(); }
  });
});
