import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import type { Awareness } from "y-protocols/awareness";
import type { BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import { SidecarCredentialRotator } from "../../src/v2/bootstrap/rotation.ts";
import {
  GP_V2_FLIGHT_MAX_BYTES,
  GP_V2_FLIGHT_MAX_FRAMES,
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  type AwarenessLane,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../../src/v2/protocol/shared.ts";
import { AckClockedFlight } from "../../src/v2/protocol/flight.ts";
import {
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../../src/v2/protocol/window.ts";
import {
  WeightedFrameScheduler,
  type FramePriority,
} from "../../src/v2/protocol/scheduler.ts";
import { ReliableDeliveryTiming } from "../../src/v2/protocol/delivery-timing.ts";
import {
  PublicationPacer,
  type PublicationReason,
} from "../../src/v2/protocol/publication-pacer.ts";
import type { SidecarEngine } from "../../src/v2/sidecar/engine.ts";
import type { SidecarDiagnosticsObserver } from "../../src/v2/diagnostics/sidecar-observer.ts";
import { parseAwarenessEnvelopeWithDiagnostics } from "../../src/v2/protocol/validate.ts";
import type { OutputBackpressure } from "../../src/v2/sidecar/output-sink.ts";
import { TransportCongestion } from "../../src/v2/transport/congestion.ts";
import { AdaptiveTransportCapacity } from "../../src/v2/transport/adaptive-capacity.ts";

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
    lane: AwarenessLane,
  ) => DenoAwarenessProviderLike;
  diagnostics?: SidecarDiagnosticsObserver;
  outputBackpressure?: OutputBackpressure;
  publicationIntervalMs?: number;
  initialActivePerSandbox?: number;
  maxActivePerSandbox?: number;
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
  _lane: AwarenessLane,
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

type PublicationUrgency = "paced" | "urgent";

function sidecarFrameUrgency(frame: SidecarFrame): PublicationUrgency {
  return frame.kind === "response_event" || frame.kind === "response_batch"
    ? "paced"
    : "urgent";
}

function strongestUrgency(
  left: PublicationUrgency | null,
  right: PublicationUrgency | null,
): PublicationUrgency | null {
  if (left === "urgent" || right === "urgent") return "urgent";
  return left ?? right;
}

export class DenoAwarenessPeer {
  readonly #binding: ProtocolBinding;
  readonly #instanceId: string;
  readonly #engine: SidecarEngine;
  readonly #rotator: SidecarCredentialRotator;
  readonly #dataDoc: Y.Doc;
  readonly #controlDoc: Y.Doc;
  readonly #dataProvider: DenoAwarenessProviderLike;
  readonly #controlProvider: DenoAwarenessProviderLike;
  readonly #outbound = new OutboundFrameWindow<SidecarFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
  readonly #scheduler = new WeightedFrameScheduler<SidecarFrame>();
  readonly #flight = new AckClockedFlight({ epoch: 1 });
  readonly #retransmitMs: number;
  readonly #connectTimeoutMs: number;
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #outputBackpressure?: OutputBackpressure;
  readonly #delivery: ReliableDeliveryTiming;
  readonly #pacer: PublicationPacer;
  readonly #adaptiveCapacity: AdaptiveTransportCapacity;
  #dataRevision = 0;
  #controlRevision = 0;
  #dataDirty = false;
  #hostPeerId: string | null = null;
  readonly #hostClientId: Record<AwarenessLane, number | null> = {
    data: null,
    control: null,
  };
  readonly #hostRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  #hostCredits = -1;
  #timer: number | null = null;
  #retransmissions = 0;
  #publications = 0;
  #pacedPublications = 0;
  #urgentPublications = 0;
  #publishedBytes = 0;
  #peakAttempt = 0;
  #lastAckAt = performance.now();
  #logicalEvents = 0;
  #responseBatchFrames = 0;
  #batchedEvents = 0;
  readonly #congestion = new TransportCongestion();
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
    if (!Number.isFinite(this.#retransmitMs) || this.#retransmitMs < 1) {
      throw new RangeError("retransmitMs must be positive");
    }
    this.#connectTimeoutMs = options.connectTimeoutMs ?? 15_000;
    this.#diagnostics = options.diagnostics;
    this.#outputBackpressure = options.outputBackpressure;
    if (!Number.isFinite(this.#connectTimeoutMs) || this.#connectTimeoutMs < 1) {
      throw new RangeError("connectTimeoutMs must be positive");
    }
    this.#dataDoc = new Y.Doc({ guid: `${options.credential.docId}:ghostprov:data` });
    this.#controlDoc = new Y.Doc({ guid: `${options.credential.docId}:ghostprov:control` });
    const providerFactory = options.providerFactory ?? defaultProviderFactory;
    const credential = () => this.#rotator.current();
    this.#dataProvider = providerFactory(this.#dataDoc, credential, "data");
    this.#controlProvider = providerFactory(this.#controlDoc, credential, "control");
    this.#delivery = new ReliableDeliveryTiming({
      epoch: 1,
      minimumMs: this.#retransmitMs,
    });
    this.#pacer = new PublicationPacer({
      intervalMs: options.publicationIntervalMs,
      publish: reason => this.#publish(reason),
    });
    this.#adaptiveCapacity = new AdaptiveTransportCapacity({
      initial: options.initialActivePerSandbox ?? 8,
      maximum: options.maxActivePerSandbox ?? 16,
    });
    this.#dataProvider.awareness.on("change", this.#onDataAwarenessChange);
    this.#controlProvider.awareness.on("change", this.#onControlAwarenessChange);
  }

  async connect(): Promise<void> {
    if (this.#closed) throw new Error("Awareness peer is closed");
    try {
      await Promise.all([
        this.#connectProvider(this.#dataProvider, "data"),
        this.#connectProvider(this.#controlProvider, "control"),
      ]);
    } catch (error) {
      await this.close();
      throw error;
    }
    this.#pacer.flushNow("initial");
  }

  async #connectProvider(
    provider: DenoAwarenessProviderLike,
    lane: AwarenessLane,
  ): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        provider.off("sync", onSync);
        provider.off("connection-error", onError);
        error ? reject(error) : resolve();
      };
      const onSync = (synced: boolean) => { if (synced) finish(); };
      const onError = () => finish(new Error(`Y-Sweet sidecar ${lane} connection failed`));
      const timer = setTimeout(
        () => finish(new Error(`Y-Sweet sidecar ${lane} synchronization timed out`)),
        this.#connectTimeoutMs,
      );
      provider.on("sync", onSync);
      provider.on("connection-error", onError);
      provider.connect();
    });
  }

  enqueue(
    frame: SidecarFrame,
    priority: FramePriority,
    streamId: string,
    weight = 1,
  ): void {
    if (this.#closed) return;
    if (frame.kind === "response_batch") {
      this.#responseBatchFrames++;
      this.#logicalEvents += frame.items.length;
      this.#batchedEvents += frame.items.length;
    } else if (frame.kind === "response_event" && frame.partIndex === 0) {
      this.#logicalEvents++;
    }
    this.#scheduler.enqueue(priority, streamId, frame, weight);
    const urgency = this.#pump();
    const congestionChanged = this.#updateCongestion();
    this.#requestPublication(congestionChanged ? "urgent" : urgency);
    this.#updateBackpressure();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#pacer.close();
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    await this.#acceptChain.catch(() => undefined);
    this.#dataProvider.awareness.off("change", this.#onDataAwarenessChange);
    this.#controlProvider.awareness.off("change", this.#onControlAwarenessChange);
    for (const provider of [this.#dataProvider, this.#controlProvider]) {
      provider.awareness.setLocalState(null);
      provider.disconnect();
      provider.destroy();
    }
    this.#dataDoc.destroy();
    this.#controlDoc.destroy();
  }

  readonly #onDataAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => this.#onAwarenessChange(
    "data",
    this.#dataProvider,
    "ghostprovV2Data",
    change,
  );

  readonly #onControlAwarenessChange = (
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void => this.#onAwarenessChange(
    "control",
    this.#controlProvider,
    "ghostprovV2Control",
    change,
  );

  #onAwarenessChange(
    lane: AwarenessLane,
    provider: DenoAwarenessProviderLike,
    field: "ghostprovV2Data" | "ghostprovV2Control",
    change: { added: number[]; updated: number[]; removed: number[] },
  ): void {
    for (const clientId of change.removed) {
      if (clientId === this.#hostClientId[lane]) {
        this.#hostClientId[lane] = null;
        this.#hostRevision[lane] = -1;
        if (this.#hostClientId.data === null && this.#hostClientId.control === null) {
          this.#hostPeerId = null;
        }
      }
    }
    for (const clientId of [...change.added, ...change.updated]) {
      if (clientId === this.#dataDoc.clientID || clientId === this.#controlDoc.clientID) continue;
      const raw = provider.awareness.getStates().get(clientId)?.[field];
      if (raw == null) {
        if (clientId === this.#hostClientId[lane]) {
          this.#hostClientId[lane] = null;
          this.#hostRevision[lane] = -1;
        }
        continue;
      }
      let envelope: AwarenessEnvelope<HostFrame>;
      try {
        const parsed = parseAwarenessEnvelopeWithDiagnostics(raw, this.#binding, "host");
        envelope = parsed.envelope as AwarenessEnvelope<HostFrame>;
        if (parsed.malformed) this.#diagnostics?.malformed();
      } catch {
        continue;
      }
      if (envelope.lane !== undefined && envelope.lane !== lane) continue;
      if (this.#hostPeerId && envelope.peerId !== this.#hostPeerId) continue;
      if (envelope.revision <= this.#hostRevision[lane]) continue;
      this.#hostPeerId = envelope.peerId;
      this.#hostClientId[lane] = clientId;
      this.#hostRevision[lane] = envelope.revision;
      try {
        if (lane === "control" && envelope.frames.length > 0) {
          throw new Error("host control Awareness state contains reliable frames");
        }
        const ceilingChanged = lane === "control" && envelope.credits !== this.#hostCredits;
        if (lane === "control") {
          this.#hostCredits = envelope.credits;
          this.#engine.setHostCeiling(envelope.credits);
        }
        this.#outbound.acknowledge(envelope.ack);
        const flightCompleted = this.#flight.acknowledged(envelope.ack);
        const now = performance.now();
        const acknowledgementAdvanced = this.#delivery.acknowledged(
          envelope.ack,
          this.#outbound.snapshot().frames.length > 0,
          now,
        );
        let capacityChanged = false;
        if (acknowledgementAdvanced) {
          const ackStalled = now - this.#lastAckAt >= 2_000;
          this.#lastAckAt = now;
          const rttMs = this.#delivery.roundTripEwmaMs;
          if (rttMs !== null) {
            capacityChanged = this.#adaptiveCapacity.observe({
              rttMs,
              congested: this.#congestion.congested,
              ackStalled,
            });
          }
          this.#armRetransmission();
        }
        let frames: readonly ReliableFrame<HostFrame>[] = [];
        if (lane === "data") {
          const currentEpoch = this.#inbound.ack().epoch;
          if (envelope.epoch > currentEpoch) this.#inbound.reset(envelope.epoch);
          frames = this.#inbound.accept(envelope.frames);
        }
        for (const frame of frames) {
          this.#acceptChain = this.#acceptChain
            .then(() => this.#acceptHostFrame(frame))
            .catch(() => undefined);
        }
        const urgency = this.#pump();
        if (flightCompleted && urgency === null) {
          this.#dataProvider.awareness.setLocalStateField("ghostprovV2Data", null);
        }
        this.#updateBackpressure();
        const congestionChanged=this.#updateCongestion();
        this.#requestPublication(
          frames.length > 0 || ceilingChanged || congestionChanged || capacityChanged
            ? "urgent"
            : urgency,
        );
      } catch {
        // Invalid ACK/window transitions are ignored; a valid current host
        // envelope must re-establish contiguous state.
      }
    }
  }

  #pump(): PublicationUrgency | null {
    if (this.#flight.active) return null;
    let urgency: PublicationUrgency | null = null;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const remainingFrames = Math.min(
        GP_V2_FLIGHT_MAX_FRAMES - snapshot.frames.length,
        GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length,
      );
      const remainingBytes = Math.min(
        GP_V2_FLIGHT_MAX_BYTES - snapshot.encodedBytes,
        GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes,
      );
      if (remainingFrames < 1 || remainingBytes < 512) break;
      const next = this.#scheduler.take(1, Math.min(15 * 1024, remainingBytes - 256));
      if (!next.length) break;
      const reliable = this.#outbound.push(next[0]);
      this.#dataDirty = true;
      urgency = strongestUrgency(urgency, sidecarFrameUrgency(reliable));
    }
    return urgency;
  }

  #updateBackpressure():void{const value=this.#scheduler.snapshot();this.#outputBackpressure?.update({frames:value.queuedFrames,bytes:value.queuedEncodedBytes});}
  #updateCongestion():boolean{const scheduled=this.#scheduler.snapshot();const outbound=this.#outbound.snapshot();const before=this.#congestion.congested;this.#congestion.update({schedulerFrames:scheduled.queuedFrames,schedulerBytes:scheduled.queuedEncodedBytes,oldestQueuedFrameMs:scheduled.oldestQueuedFrameMs,ackProgressAgeMs:Math.max(0,performance.now()-this.#lastAckAt),windowFrames:outbound.frames.length});return before!==this.#congestion.congested;}

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
  }

  #publish(reason: PublicationReason): void {
    if (this.#closed) return;
    const outbound = this.#outbound.snapshot();
    const publishData = reason === "retransmission" || this.#dataDirty;
    if (publishData) {
      this.#flight.published(outbound.frames);
      this.#delivery.published(outbound.frames, performance.now());
      this.#publishLane("data", outbound.frames, reason);
      this.#dataDirty = false;
    }
    if (reason !== "retransmission" || !publishData) {
      this.#publishLane("control", [], reason);
    }
    this.#armRetransmission();
  }

  #publishLane(
    lane: AwarenessLane,
    frames: readonly ReliableFrame<SidecarFrame>[],
    reason: PublicationReason,
  ): void {
    const transport = this.#engine.snapshot();
    const outbound = this.#outbound.snapshot();
    const scheduled = this.#scheduler.snapshot();
    this.#publications++;
    if (reason === "paced") this.#pacedPublications++;
    else if (reason === "urgent") this.#urgentPublications++;
    this.#diagnostics?.transport({ windowFrames: outbound.frames.length, windowBytes: outbound.encodedBytes, schedulerFrames: scheduled.queuedFrames, schedulerBytes: scheduled.queuedEncodedBytes, schedulerStreams: scheduled.streams, oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs, ackProgressAgeMs: Math.max(0, performance.now() - this.#lastAckAt), retransmissionAttempt: this.#delivery.attempt, peakRetransmissionAttempt: this.#peakAttempt, retransmissionsTotal: this.#retransmissions, publicationsTotal: this.#publications, publishedBytesTotal: this.#publishedBytes, admittedUncommitted: transport.active, roundTripEwmaMs: this.#delivery.roundTripEwmaMs, pacedPublicationsTotal: this.#pacedPublications, urgentPublicationsTotal: this.#urgentPublications, logicalEventsTotal: this.#logicalEvents, responseBatchFramesTotal: this.#responseBatchFrames, batchedEventsTotal: this.#batchedEvents });
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
      revision: lane === "data" ? this.#dataRevision++ : this.#controlRevision++,
      ack: this.#inbound.ack(),
      credits: this.#congestion.congested
        ? 0
        : this.#adaptiveCapacity.available(transport.active, transport.credits),
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames,
      lane,
      ...(lane === "control" && this.#diagnostics?.enabled
        ? { diagnostics: this.#diagnostics.envelope() }
        : {}),
    };
    this.#publishedBytes += new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    const provider = lane === "data" ? this.#dataProvider : this.#controlProvider;
    const field = lane === "data" ? "ghostprovV2Data" : "ghostprovV2Control";
    provider.awareness.setLocalStateField(field, envelope);
  }

  #armRetransmission(): void {
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#closed || this.#outbound.snapshot().frames.length === 0) return;
    const revision = this.#dataRevision + this.#controlRevision;
    const jitterSeed = ((revision * 2_654_435_761) >>> 0) / 0xffff_ffff;
    const delay = this.#delivery.nextDelayMs(performance.now(), jitterSeed);
    if (delay === null) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (this.#closed || this.#outbound.snapshot().frames.length === 0) return;
      this.#delivery.retransmitted(performance.now());
      this.#retransmissions++;
      this.#peakAttempt = Math.max(this.#peakAttempt, this.#delivery.attempt);
      this.#pacer.flushNow("retransmission");
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
