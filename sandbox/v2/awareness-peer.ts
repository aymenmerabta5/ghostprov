import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import type { Awareness } from "y-protocols/awareness";
import type { BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import { SidecarCredentialRotator } from "../../src/v2/bootstrap/rotation.ts";
import {
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../../src/v2/protocol/shared.ts";
import {
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../../src/v2/protocol/window.ts";
import {
  WeightedFrameScheduler,
  type FramePriority,
} from "../../src/v2/protocol/scheduler.ts";
import { computeV2RetransmitDelayMs } from "../../src/v2/protocol/retransmit.ts";
import type { SidecarEngine } from "../../src/v2/sidecar/engine.ts";
import type { SidecarDiagnosticsObserver } from "../../src/v2/diagnostics/sidecar-observer.ts";
import { parseAwarenessEnvelopeWithDiagnostics } from "../../src/v2/protocol/validate.ts";
import type { OutputBackpressure } from "../../src/v2/sidecar/output-sink.ts";

export interface DenoAwarenessPeerOptions {
  credential: BootstrapCredentialPayload;
  binding: ProtocolBinding;
  instanceId: string;
  engine: SidecarEngine;
  retransmitMs?: number;
  connectTimeoutMs?: number;
  providerFactory?: (
    doc: Y.Doc,
    credential: () => BootstrapCredentialPayload,
  ) => DenoAwarenessProviderLike;
  diagnostics?: SidecarDiagnosticsObserver;
  outputBackpressure?: OutputBackpressure;
}

export interface DenoAwarenessProviderLike {
  readonly awareness: Pick<Awareness,
    | "getStates"
    | "getLocalState"
    | "setLocalState"
    | "setLocalStateField"
    | "on"
    | "off"
  >;
  on(event: string, listener: (...args: any[]) => void): void;
  off(event: string, listener: (...args: any[]) => void): void;
  connect(): void;
  disconnect(): void;
  destroy(): void;
}

function defaultProviderFactory(
  doc: Y.Doc,
  credential: () => BootstrapCredentialPayload,
): DenoAwarenessProviderLike {
  class AuthenticatedWebSocket extends WebSocket {
    constructor(url: string | URL, protocols?: string | string[]) {
      const target = new URL(String(url));
      target.searchParams.set("token", credential().token);
      super(target, protocols);
    }
  }
  return new WebsocketProvider(
    credential().url,
    credential().docId,
    doc,
    {
      connect: false,
      disableBc: true,
      WebSocketPolyfill: AuthenticatedWebSocket,
    } as any,
  ) as DenoAwarenessProviderLike;
}

export class DenoAwarenessPeer {
  readonly #binding: ProtocolBinding;
  readonly #instanceId: string;
  readonly #engine: SidecarEngine;
  readonly #rotator: SidecarCredentialRotator;
  readonly #doc: Y.Doc;
  readonly #provider: DenoAwarenessProviderLike;
  readonly #outbound = new OutboundFrameWindow<SidecarFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
  readonly #scheduler = new WeightedFrameScheduler<SidecarFrame>();
  readonly #retransmitMs: number;
  readonly #connectTimeoutMs: number;
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #outputBackpressure?: OutputBackpressure;
  #revision = 0;
  #hostPeerId: string | null = null;
  #hostClientId: number | null = null;
  #hostRevision = -1;
  #hostCredits = -1;
  #timer: number | null = null;
  #retransmissionAttempt = 0;
  #publishScheduled = false;
  #retransmissions = 0; #publications = 0; #publishedBytes = 0; #peakAttempt = 0; #lastAckAt = performance.now();
  #closed = false;
  #acceptChain: Promise<void> = Promise.resolve();

  constructor(options: DenoAwarenessPeerOptions) {
    this.#binding = options.binding;
    this.#instanceId = options.instanceId;
    this.#engine = options.engine;
    this.#rotator = new SidecarCredentialRotator({
      credential: options.credential,
      binding: options.binding,
      instanceId: options.instanceId,
    });
    this.#retransmitMs = options.retransmitMs ?? 250;
    this.#connectTimeoutMs = options.connectTimeoutMs ?? 15_000;
    this.#diagnostics = options.diagnostics;
    this.#outputBackpressure = options.outputBackpressure;
    if (!Number.isFinite(this.#connectTimeoutMs) || this.#connectTimeoutMs < 1) {
      throw new RangeError("connectTimeoutMs must be positive");
    }
    this.#doc = new Y.Doc({ guid: options.credential.docId });
    this.#provider = (options.providerFactory ?? defaultProviderFactory)(
      this.#doc,
      () => this.#rotator.current(),
    );
    this.#provider.awareness.on("change", this.#onAwarenessChange);
  }

  async connect(): Promise<void> {
    if (this.#closed) throw new Error("Awareness peer is closed");
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.#provider.off("sync", onSync);
        this.#provider.off("connection-error", onError);
        error ? reject(error) : resolve();
      };
      const onSync = (synced: boolean) => { if (synced) finish(); };
      const onError = () => finish(new Error("Y-Sweet sidecar connection failed"));
      const timer = setTimeout(
        () => finish(new Error("Y-Sweet sidecar synchronization timed out")),
        this.#connectTimeoutMs,
      );
      this.#provider.on("sync", onSync);
      this.#provider.on("connection-error", onError);
      this.#provider.connect();
    });
    this.#publish();
  }

  enqueue(
    frame: SidecarFrame,
    priority: FramePriority,
    streamId: string,
    weight = 1,
  ): void {
    if (this.#closed) return;
    this.#scheduler.enqueue(priority, streamId, frame, weight);
    if (this.#pump()) this.#schedulePublish();
    this.#updateBackpressure();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    await this.#acceptChain.catch(() => undefined);
    this.#provider.awareness.off("change", this.#onAwarenessChange);
    this.#provider.awareness.setLocalState(null);
    this.#provider.disconnect();
    this.#provider.destroy();
    this.#doc.destroy();
  }

  readonly #onAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => {
    for (const clientId of change.removed) {
      if (clientId === this.#hostClientId) {
        this.#hostPeerId = null;
        this.#hostClientId = null;
        this.#hostRevision = -1;
      }
    }
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#doc.clientID) continue;
      const raw = this.#provider.awareness.getStates().get(clientId)?.ghostprovV2;
      if (raw == null) continue;
      let envelope: AwarenessEnvelope<HostFrame>;
      try {
        const parsed = parseAwarenessEnvelopeWithDiagnostics(raw, this.#binding, "host");
        envelope = parsed.envelope as AwarenessEnvelope<HostFrame>;
        if (parsed.malformed) this.#diagnostics?.malformed();
      } catch {
        continue;
      }
      if (this.#hostPeerId && envelope.peerId !== this.#hostPeerId) continue;
      if (envelope.revision <= this.#hostRevision) continue;
      this.#hostPeerId = envelope.peerId;
      this.#hostClientId = clientId;
      this.#hostRevision = envelope.revision;
      try {
        const previousOutstanding = this.#outbound.snapshot().frames.length;
        const ceilingChanged = envelope.credits !== this.#hostCredits;
        this.#hostCredits = envelope.credits;
        this.#engine.setHostCeiling(envelope.credits);
        this.#outbound.acknowledge(envelope.ack);
        const acknowledgementAdvanced =
          this.#outbound.snapshot().frames.length < previousOutstanding;
        if (acknowledgementAdvanced) this.#retransmissionAttempt = 0;
        if (acknowledgementAdvanced) this.#lastAckAt = performance.now();
        const currentEpoch = this.#inbound.ack().epoch;
        if (envelope.epoch > currentEpoch) this.#inbound.reset(envelope.epoch);
        const frames = this.#inbound.accept(envelope.frames);
        for (const frame of frames) {
          this.#acceptChain = this.#acceptChain
            .then(() => this.#acceptHostFrame(frame))
            .catch(() => undefined);
        }
        const pumped = this.#pump();
        this.#updateBackpressure();
        if (ceilingChanged || acknowledgementAdvanced || frames.length > 0 || pumped) {
          this.#schedulePublish();
        }
      } catch {
        // Invalid ACK/window transitions are ignored; a valid current host
        // envelope must re-establish contiguous state.
      }
    }
  };

  #pump(): boolean {
    const wasEmpty = this.#outbound.snapshot().frames.length === 0;
    let changed = false;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const remainingFrames = GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length;
      const remainingBytes = GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes;
      if (remainingFrames < 1 || remainingBytes < 512) break;
      const next = this.#scheduler.take(1, Math.min(15 * 1024, remainingBytes - 256));
      if (!next.length) break;
      this.#outbound.push(next[0]);
      changed = true;
    }
    if (changed && wasEmpty) this.#retransmissionAttempt = 0;
    return changed;
  }

  #updateBackpressure():void{const value=this.#scheduler.snapshot();this.#outputBackpressure?.update({frames:value.queuedFrames,bytes:value.queuedEncodedBytes});}

  #schedulePublish(): void {
    if (this.#closed || this.#publishScheduled) return;
    this.#publishScheduled = true;
    queueMicrotask(() => {
      this.#publishScheduled = false;
      if (!this.#closed) this.#publish();
    });
  }

  #publish(): void {
    if (this.#closed) return;
    const transport = this.#engine.snapshot();
    const outbound = this.#outbound.snapshot();
    const scheduled = this.#scheduler.snapshot();
    this.#publications++;
    this.#diagnostics?.transport({ windowFrames: outbound.frames.length, windowBytes: outbound.encodedBytes, schedulerFrames: scheduled.queuedFrames, schedulerBytes: scheduled.queuedEncodedBytes, schedulerStreams: scheduled.streams, oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs, ackProgressAgeMs: Math.max(0, performance.now() - this.#lastAckAt), retransmissionAttempt: this.#retransmissionAttempt, peakRetransmissionAttempt: this.#peakAttempt, retransmissionsTotal: this.#retransmissions, publicationsTotal: this.#publications, publishedBytesTotal: this.#publishedBytes, admittedUncommitted: transport.active, roundTripEwmaMs: null });
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: this.#binding.schemaHash,
      buildHash: this.#binding.buildHash,
      projectId: this.#binding.projectId,
      sessionId: this.#binding.sessionId,
      peerId: "sidecar",
      instanceId: this.#instanceId,
      leaderGeneration: this.#binding.leaderGeneration,
      epoch: outbound.epoch,
      revision: this.#revision++,
      ack: this.#inbound.ack(),
      credits: transport.credits,
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames: outbound.frames,
      ...(this.#diagnostics?.enabled ? { diagnostics: this.#diagnostics.envelope() } : {}),
    };
    this.#publishedBytes += new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    this.#provider.awareness.setLocalStateField("ghostprovV2", envelope);
    this.#armRetransmission();
  }

  #armRetransmission(): void {
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#closed || this.#outbound.snapshot().frames.length === 0) return;
    const jitterSeed = ((this.#revision * 2_654_435_761) >>> 0) / 0xffff_ffff;
    const delay = computeV2RetransmitDelayMs(
      null,
      jitterSeed,
      this.#retransmitMs,
      this.#retransmissionAttempt,
    );
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (this.#closed || this.#outbound.snapshot().frames.length === 0) return;
      this.#retransmissionAttempt++;
      this.#retransmissions++; this.#peakAttempt = Math.max(this.#peakAttempt, this.#retransmissionAttempt);
      this.#publish();
    }, delay) as unknown as number;
  }

  async #acceptHostFrame(frame: ReliableFrame<HostFrame>): Promise<void> {
    if (frame.kind !== "credential_rotate") {
      await this.#engine.accept(frame);
      return;
    }
    await this.#rotator.accept(frame.rotationId, frame.sealedJson);
    this.enqueue(
      { kind: "credential_ack", rotationId: frame.rotationId },
      "control",
      "credential",
    );
  }

}
