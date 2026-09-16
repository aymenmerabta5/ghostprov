import { describe, expect, it } from "bun:test";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";
import {
  DenoAwarenessPeer,
  type DenoAwarenessPeerOptions,
  type DenoAwarenessProviderLike,
} from "../sandbox/v2/r10-awareness-peer";
import { SidecarDiagnosticsObserver } from "../src/v2/diagnostics/sidecar-observer";
import { SidecarResourceSampler } from "../src/v2/diagnostics/sidecar-sampler";
import {
  encodedDiagnosticBytes,
  parseDiagnosticObject,
  type V2EnvelopeDiagnostics,
} from "../src/v2/diagnostics/types";
import type { PublicationTimer } from "../src/v2/protocol/publication-pacer";
import type {
  AwarenessEnvelope,
  AwarenessLane,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";

const BINDING: ProtocolBinding = {
  protocolRevision: "r2",
  launchNonce: "r10-receive-evidence-launch",
  projectId: "e631d12c-d917-4434-b360-41b4715d6679",
  sessionId: "sandbox-receive-evidence",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
};
const DIAGNOSTIC_MODES = ["enabled", "disabled", "absent"] as const;
type DiagnosticMode = typeof DIAGNOSTIC_MODES[number];

class FakeProvider implements DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly listeners = new Map<string, Set<(...args: any[]) => void>>();

  constructor(doc: Y.Doc) { this.awareness = new Awareness(doc); }
  on(event: string, listener: (...args: any[]) => void): void {
    let listeners = this.listeners.get(event);
    if (!listeners) this.listeners.set(event, listeners = new Set());
    listeners.add(listener);
  }
  off(event: string, listener: (...args: any[]) => void): void {
    this.listeners.get(event)?.delete(listener);
  }
  connect(): void {
    queueMicrotask(() => {
      for (const listener of this.listeners.get("sync") ?? []) listener(true);
    });
  }
  disconnect(): void {}
  destroy(): void { this.awareness.destroy(); }
}

function harness(mode: DiagnosticMode) {
  let now = 1_000;
  let revision = 0;
  let resourceReads = 0;
  const scheduled = new Map<object, () => void>();
  // Keep ordinary control publications paused: a pong must supply its own
  // current evidence without a resource sampler tick or engine publication.
  const publicationTimer: PublicationTimer = {
    now: () => now,
    set(callback) {
      const handle = {};
      scheduled.set(handle, callback);
      return handle;
    },
    clear(handle) { scheduled.delete(handle as object); },
    defer(callback) { scheduled.set({}, callback); },
  };
  const sampler = new SidecarResourceSampler({
    enabled: true,
    intervalMs: 1_000,
    now: () => now,
    probe: {
      memory: () => {
        resourceReads++;
        return { rss: 1_024, heapUsed: 256, heapTotal: 512, external: 64 };
      },
      systemMemory: () => null,
      processCpuPercent: () => null,
      normalizedLoad1: () => null,
    },
  });
  const diagnostics = mode === "absent" ? undefined : new SidecarDiagnosticsObserver({
    enabled: mode === "enabled", sampler, now: () => now,
  });
  const providers = new Map<AwarenessLane, FakeProvider>();
  const hostDoc = new Y.Doc();
  const hostAwareness = new Awareness(hostDoc);
  const peer = new DenoAwarenessPeer({
    binding: BINDING,
    instanceId: "sidecar-receive-evidence",
    credential: {
      url: `wss://prism.test/y/d/${BINDING.projectId}/ws`,
      baseUrl: `https://prism.test/y/d/${BINDING.projectId}`,
      docId: BINDING.projectId,
      token: "synthetic-credential",
      authorization: "full",
      rotationKey: Buffer.alloc(32, 3).toString("base64url"),
    },
    engine: {
      snapshot: () => ({ ready: true, credits: 8, active: 0, queued: 0, draining: false }),
      setHostCeiling: () => undefined,
      accept: async () => undefined,
    } as unknown as DenoAwarenessPeerOptions["engine"],
    providerFactory: (doc, _credential, lane) => {
      const provider = new FakeProvider(doc);
      providers.set(lane, provider);
      return provider;
    },
    diagnostics,
    now: () => now,
    publicationTimer,
    publicationIntervalMs: 60_000,
    retransmitMs: 60_000,
    transportStallMs: 60_000,
    connectTimeoutMs: 100,
  });
  const control = () => providers.get("control")!.awareness.getLocalState()!
    .ghostprovV2Control as AwarenessEnvelope<SidecarFrame>;
  const sendControl = (ackSequence = -1, ping = false) => {
    const envelope: AwarenessEnvelope<HostFrame> = {
      ...BINDING,
      protocol: "gp-awareness-v2",
      protocolRevision: "r2",
      schemaVersion: 2,
      launchNonce: "r10-receive-evidence-launch",
      peerId: "host",
      instanceId: "host-receive-evidence",
      targetInstanceId: "sidecar-receive-evidence",
      lane: "control",
      epoch: 1,
      revision: revision++,
      ack: { epoch: 1, sequence: ackSequence },
      credits: 128,
      active: 0,
      queued: 0,
      draining: false,
      frames: [],
      ...(ping ? { liveness: { kind: "ping" as const, sentAtMs: now } } : {}),
    };
    hostAwareness.setLocalStateField("ghostprovV2Control", envelope);
    applyAwarenessUpdate(providers.get("control")!.awareness,
      encodeAwarenessUpdate(hostAwareness, [hostDoc.clientID]), "synthetic-host");
  };
  return {
    peer, control, sendControl, diagnostics,
    data: () => providers.get("data")!.awareness.getLocalState()!
      .ghostprovV2Data as AwarenessEnvelope<SidecarFrame> | null | undefined,
    advance(milliseconds: number) { now += milliseconds; },
    ping(ackSequence = -1) {
      now++;
      sendControl(ackSequence, true);
      const pong = control();
      expect(pong.liveness).toEqual({ kind: "pong", sentAtMs: now });
      expect(pong.frames).toEqual([]);
      expect(pong.diagnostics).toBeDefined();
      expect(parseDiagnosticObject(pong.diagnostics)).not.toBeNull();
      expect(encodedDiagnosticBytes(pong.diagnostics)).toBeLessThanOrEqual(2_048);
      if (mode === "enabled") {
        expect(pong.diagnostics!.resource?.processRssBytes).toBe(1_024);
      } else {
        expect(pong.diagnostics!.resource).toBeNull();
        expect(resourceReads).toBe(0);
      }
      return pong;
    },
    async close() {
      await peer.close();
      await sampler.close();
      hostAwareness.destroy();
      hostDoc.destroy();
      scheduled.clear();
    },
  };
}

function responseEvent(eventIndex: number): SidecarFrame {
  return {
    kind: "response_event", requestId: "req-receive-evidence", eventIndex,
    event: "response.output_text.delta", partIndex: 0, partCount: 1,
    offset: 0, dataBase64Url: "eA",
  };
}

describe("R10 control-pong receive evidence", () => {
  it.each([2_048, 2_049, 2_101])("preserves every transport field across the %d-byte diagnostic boundary", async bytes => {
    const h = harness("enabled");
    try {
      await h.peer.connect();
      h.sendControl();
      h.peer.publishEngineState();
      const ordinary = h.control().diagnostics!;
      // A nonnegative finite JSON number needs at most 24 characters; this
      // value attains that bound. Legal integer counters need at most 16.
      const longestFloat = 0.0000010000000000000002;
      const floatTransport = new Set(["oldestQueuedFrameMs", "ackProgressAgeMs",
        "roundTripEwmaMs", "roundTripVariationMs", "acknowledgedGoodputBytesPerSecond"]);
      const largest: V2EnvelopeDiagnostics = {
        version: 1, malformedCount: Number.MAX_SAFE_INTEGER,
        resource: Object.fromEntries(Object.keys(ordinary.resource!).map(key => [key,
          key === "eventLoopSamples" || key === "gcSamples" ? Number.MAX_SAFE_INTEGER : longestFloat,
        ])) as unknown as NonNullable<V2EnvelopeDiagnostics["resource"]>,
        transport: Object.fromEntries(Object.keys(ordinary.transport).map(key => [key,
          floatTransport.has(key) ? longestFloat : Number.MAX_SAFE_INTEGER,
        ])) as unknown as V2EnvelopeDiagnostics["transport"],
      };
      expect(encodedDiagnosticBytes(largest)).toBe(2_101);
      expect(parseDiagnosticObject({ ...largest, resource: null })).not.toBeNull();
      expect(encodedDiagnosticBytes({ ...largest, resource: null })).toBe(1_441);
      // Separately validate the resource fields, without the combined cap.
      expect(parseDiagnosticObject({ ...largest, transport: ordinary.transport })).not.toBeNull();
      if (bytes < 2_101) {
        largest.resource!.processRssBytes = 0;
        largest.resource!.heapUsedBytes = 0;
        largest.resource!.heapTotalBytes = bytes === 2_048 ? 12345678901234568 : 1234567890123456.8;
      }
      expect(encodedDiagnosticBytes(largest)).toBe(bytes);
      h.diagnostics!.envelope = () => largest;
      h.peer.publishEngineState();
      const published = h.control().diagnostics!;
      expect(parseDiagnosticObject(published)).not.toBeNull();
      expect(encodedDiagnosticBytes(published)).toBeLessThanOrEqual(2_048);
      expect(published.transport).toEqual(largest.transport);
      expect(published.malformedCount).toBe(Number.MAX_SAFE_INTEGER);
      expect(published.resource).toEqual(bytes <= 2_048 ? largest.resource : null);
      h.advance(1);
      h.sendControl(-1, true);
      const pong = h.control().diagnostics!;
      expect(parseDiagnosticObject(pong)).not.toBeNull();
      expect(pong.transport).toEqual(largest.transport);
      expect(pong.resource).toEqual(bytes <= 2_048 ? largest.resource : null);
      expect(largest.resource).not.toBeNull();
    } finally { await h.close(); }
  });

  it.each([...DIAGNOSTIC_MODES])(
    "samples pending output and clears acknowledged output with diagnostics %s", async mode => {
      const h = harness(mode);
      try {
        await h.peer.connect();
        h.sendControl();
        h.peer.publishEngineState();
        const idle = h.control();
        const idleData = h.data();

        h.peer.enqueue(responseEvent(0), "data", "req-receive-evidence");
        expect(h.control()).toBe(idle);
        const pending = h.ping();
        expect(pending.diagnostics!.transport).toMatchObject({ windowFrames: 1, schedulerFrames: 0 });
        expect(pending.diagnostics!.transport.windowBytes).toBeGreaterThan(0);
        expect(h.data()).toBe(idleData);
        const repeated = h.ping();
        expect(repeated.diagnostics!.transport.publicationsTotal)
          .toBeGreaterThan(pending.diagnostics!.transport.publicationsTotal);
        expect(repeated.diagnostics).not.toBe(pending.diagnostics);

        h.peer.publishEngineState();
        const published = h.data()!;
        expect(published.frames).toHaveLength(1);
        const beforeAck = h.control();
        const ackSequence = published.frames[0]!.sequence;
        h.sendControl(ackSequence);
        expect(h.data()).toBeNull();
        expect(h.control()).toBe(beforeAck);
        const cleared = h.ping(ackSequence);
        expect(cleared.diagnostics!.transport).toMatchObject({
          windowFrames: 0, windowBytes: 0, schedulerFrames: 0, schedulerBytes: 0,
        });
        expect(cleared.diagnostics!.transport.publicationsTotal)
          .toBeGreaterThan(beforeAck.diagnostics!.transport.publicationsTotal);
        expect(beforeAck.diagnostics!.transport.windowFrames).toBe(1);
      } finally { await h.close(); }
    },
  );

  it.each([...DIAGNOSTIC_MODES])(
    "refreshes queue and retransmission evidence after data-only publication with diagnostics %s", async mode => {
      const h = harness(mode);
      try {
        await h.peer.connect();
        h.sendControl();
        h.peer.enqueue(responseEvent(0), "data", "req-receive-evidence");
        h.peer.publishEngineState();
        const firstFlight = h.data()!;
        const beforeRetransmission = h.control();
        h.peer.enqueue(responseEvent(1), "data", "req-receive-evidence");
        h.advance(1_600);
        // A stale ACK causes an admissible retransmission of the identical
        // reliable flight. It does not publish a new control snapshot.
        h.sendControl();
        expect(h.data()!.revision).toBeGreaterThan(firstFlight.revision);
        expect(h.data()!.frames).toEqual(firstFlight.frames);
        expect(h.control()).toBe(beforeRetransmission);

        const pong = h.ping();
        expect(pong.diagnostics!.transport).toMatchObject({
          windowFrames: 1, schedulerFrames: 1, retransmissionsTotal: 1,
        });
        expect(pong.diagnostics!.transport.schedulerBytes).toBeGreaterThan(0);
        expect(pong.diagnostics!.transport.oldestQueuedFrameMs).toBeGreaterThanOrEqual(1_600);
        expect(pong.diagnostics!.transport.publicationsTotal)
          .toBeGreaterThan(beforeRetransmission.diagnostics!.transport.publicationsTotal);
        expect(pong.diagnostics!.transport.dataPublicationsTotal)
          .toBeGreaterThan(beforeRetransmission.diagnostics!.transport.dataPublicationsTotal);
        expect(beforeRetransmission.diagnostics!.transport.retransmissionsTotal).toBe(0);
        const repeated = h.ping();
        expect(repeated.diagnostics!.transport.publicationsTotal)
          .toBeGreaterThan(pong.diagnostics!.transport.publicationsTotal);
        expect(repeated.diagnostics!.transport.retransmissionsTotal).toBe(1);
      } finally { await h.close(); }
    },
  );
});
