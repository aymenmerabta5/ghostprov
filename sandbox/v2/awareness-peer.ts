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
  closeTimeoutMs?: number;
  onAcceptError?: (
    error: Error,
    frame: ReliableFrame<HostFrame>,
  ) => void;
  onEngineError?: (error: Error) => void;
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

export class SidecarCloseError extends Error {
  readonly name = "SidecarCloseError";
  constructor(
    message: string,
    readonly code: "timeout" | "operation" | "output" | "teardown",
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
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

function hostFrameIsControl(frame: ReliableFrame<HostFrame>): boolean {
  return frame.kind === "request_cancel" ||
    frame.kind === "response_ack" ||
    frame.kind === "drain" ||
    frame.kind === "ping" ||
    frame.kind === "credential_rotate";
}

function sleepWithSignal(delayMs: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
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
  readonly #bootstrapInbound = new InboundFrameCursor<HostFrame>({ epoch: 1 });
  #processedAck = { epoch: 1, sequence: -1 };
  #bootstrapProcessedAck = { epoch: 1, sequence: -1 };
  readonly #scheduler = new WeightedFrameScheduler<SidecarFrame>();
  readonly #flight = new AckClockedFlight({ epoch: 1 });
  readonly #retransmitMs: number;
  readonly #connectTimeoutMs: number;
  readonly #closeTimeoutMs: number;
  readonly #onAcceptError?: DenoAwarenessPeerOptions["onAcceptError"];
  readonly #onEngineError?: DenoAwarenessPeerOptions["onEngineError"];
  readonly #diagnostics?: SidecarDiagnosticsObserver;
  readonly #outputBackpressure?: OutputBackpressure;
  readonly #delivery: ReliableDeliveryTiming;
  readonly #pacer: PublicationPacer;
  readonly #adaptiveCapacity: AdaptiveTransportCapacity;
  #dataRevision = 0;
  #controlRevision = 0;
  #dataDirty = false;
  #hostPeerId: string | null = null;
  #hostInstanceId: string | null = null;
  readonly #hostClientId: Record<AwarenessLane, number | null> = {
    data: null,
    control: null,
  };
  readonly #hostRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  readonly #bootstrapRevision: Record<AwarenessLane, number> = {
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
  #dataPublications = 0;
  #controlPublications = 0;
  #dataPublishedBytes = 0;
  #controlPublishedBytes = 0;
  #flightCompletions = 0;
  #peakAttempt = 0;
  #logicalEvents = 0;
  #responseBatchFrames = 0;
  #batchedEvents = 0;
  readonly #congestion = new TransportCongestion();
  readonly #lifecycleAbort = new AbortController();
  #closed = false;
  #closing = false;
  #connected = false;
  #connectPromise: Promise<void> | null = null;
  #closePromise: Promise<void> | null = null;
  #recoveryReplayStarted = false;
  #recoveryReplayCompleted = false;
  #recoveryReplayFailed = false;
  #recoveryReplayInProgress = false;
  #recoveryReplayPromise: Promise<void> | null = null;
  #inboundFrozen = false;
  readonly #requestAcceptChains = new Map<string, Promise<void>>();
  #controlAcceptChain: Promise<void> = Promise.resolve();
  readonly #processedReady = new Map<number, ReliableFrame<HostFrame>>();
  readonly #bootstrapProcessedReady = new Map<number, ReliableFrame<HostFrame>>();

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
    this.#closeTimeoutMs = options.closeTimeoutMs ?? 5_000;
    this.#onAcceptError = options.onAcceptError;
    this.#onEngineError = options.onEngineError;
    this.#diagnostics = options.diagnostics;
    this.#outputBackpressure = options.outputBackpressure;
    if (!Number.isFinite(this.#connectTimeoutMs) || this.#connectTimeoutMs < 1) {
      throw new RangeError("connectTimeoutMs must be positive");
    }
    if (!Number.isFinite(this.#closeTimeoutMs) || this.#closeTimeoutMs < 1) {
      throw new RangeError("closeTimeoutMs must be positive");
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

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed || this.#closing) return Promise.reject(new Error("Awareness peer is closed"));
    if (!this.#connectPromise) this.#connectPromise = this.#connect(signal);
    return this.#connectPromise;
  }

  async #connect(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw signal.reason;
    const onAbort = () => this.#lifecycleAbort.abort(signal?.reason);
    signal?.addEventListener("abort", onAbort, { once: true });
    try {
      await Promise.all([
        this.#connectProvider(this.#dataProvider, "data", this.#lifecycleAbort.signal),
        this.#connectProvider(this.#controlProvider, "control", this.#lifecycleAbort.signal),
      ]);
      if (this.#closed) throw new Error("Awareness peer closed while connecting");
      this.#connected = true;
    } catch (error) {
      try {
        await this.close();
      } catch (cleanupError) {
        throw new AggregateError(
          [error, cleanupError],
          "Awareness connection and teardown both failed",
        );
      }
      throw error;
    } finally {
      signal?.removeEventListener("abort", onAbort);
    }
    this.#pacer.flushNow("initial");
  }

  async #connectProvider(
    provider: DenoAwarenessProviderLike,
    lane: AwarenessLane,
    signal: AbortSignal,
  ): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        provider.off("sync", onSync);
        provider.off("connection-error", onError);
        provider.off("connection-close", onClose);
        signal.removeEventListener("abort", onAbort);
        error ? reject(error) : resolve();
      };
      const onSync = (synced: boolean) => { if (synced) finish(); };
      const onError = () => finish(new Error(`Y-Sweet sidecar ${lane} connection failed`));
      const onClose = () => finish(new Error(`Y-Sweet sidecar ${lane} connection closed before synchronization`));
      const onAbort = () => finish(
        signal.reason instanceof Error
          ? signal.reason
          : new DOMException(`Y-Sweet sidecar ${lane} connection was aborted`, "AbortError"),
      );
      const timer = setTimeout(
        () => finish(new Error(`Y-Sweet sidecar ${lane} synchronization timed out`)),
        this.#connectTimeoutMs,
      );
      provider.on("sync", onSync);
      provider.on("connection-error", onError);
      provider.on("connection-close", onClose);
      signal.addEventListener("abort", onAbort, { once: true });
      if (signal.aborted) {
        onAbort();
        return;
      }
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

  /** Publishes changed engine readiness/credits without changing the wire shape. */
  publishEngineState(): void {
    if (this.#closed || !this.#connected) return;
    // A provider can synchronize the host's existing Awareness state before
    // journal recovery is finished. Revisit that exact state only after the
    // engine is ready so the transport does not ACK work merely parked behind
    // startup.
    this.#reprocessSteadyHostData();
    this.#pacer.flushNow("urgent");
  }

  freezeInbound(): void {
    this.#inboundFrozen = true;
    this.#pacer.requestUrgent();
  }

  #reprocessSteadyHostData(): void {
    const hostClientId = this.#hostClientId.data;
    if (hostClientId === null || this.#engine.snapshot().ready === false) return;
    this.#hostRevision.data = -1;
    this.#onAwarenessChange(
      "data",
      this.#dataProvider,
      "ghostprovV2Data",
      { added: [], updated: [hostClientId], removed: [] },
    );
  }

  close(): Promise<void> {
    if (!this.#closePromise) this.#closePromise = this.#close(false);
    return this.#closePromise;
  }

  closeGracefully(): Promise<void> {
    if (!this.#closePromise) this.#closePromise = this.#close(true);
    return this.#closePromise;
  }

  async #close(graceful: boolean): Promise<void> {
    this.#closing = true;
    this.#inboundFrozen = true;
    this.#connected = false;
    if (!graceful) {
      this.#lifecycleAbort.abort(new DOMException("Awareness peer closed", "AbortError"));
    }
    const deadline = performance.now() + this.#closeTimeoutMs;
    const lifecycleErrors: unknown[] = [];
    try {
      if (this.#recoveryReplayPromise) {
        await this.#awaitBounded(this.#recoveryReplayPromise, deadline, "recovery replay");
      }
      await this.#awaitAcceptChain(deadline);
      if (graceful) {
        this.#pacer.flushNow("urgent");
        await this.#awaitOutputDrain(deadline);
        // A terminal ACK can arrive while the output window is draining. Let
        // its durable journal cleanup settle before ownership is released.
        this.#inboundFrozen = true;
        await this.#awaitAcceptChain(deadline);
      }
    } catch (error) {
      lifecycleErrors.push(error);
    }
    this.#lifecycleAbort.abort(new DOMException("Awareness peer closed", "AbortError"));
    this.#closed = true;
    try { this.#pacer.close(); } catch (error) { lifecycleErrors.push(error); }
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    const cleanup = (operation: () => void) => {
      try { operation(); } catch (error) { lifecycleErrors.push(error); }
    };
    cleanup(() => this.#dataProvider.awareness.off("change", this.#onDataAwarenessChange));
    cleanup(() => this.#controlProvider.awareness.off("change", this.#onControlAwarenessChange));
    for (const provider of [this.#dataProvider, this.#controlProvider]) {
      cleanup(() => provider.awareness.setLocalState(null));
      cleanup(() => provider.disconnect());
      cleanup(() => provider.destroy());
    }
    cleanup(() => this.#dataDoc.destroy());
    cleanup(() => this.#controlDoc.destroy());
    if (lifecycleErrors.length) {
      if (lifecycleErrors.length === 1 && lifecycleErrors[0] instanceof SidecarCloseError) {
        throw lifecycleErrors[0];
      }
      throw new SidecarCloseError(
        "Awareness peer did not close cleanly",
        "teardown",
        { cause: new AggregateError(lifecycleErrors, "Awareness peer teardown failures") },
      );
    }
  }

  async #awaitAcceptChain(deadline: number): Promise<void> {
    await this.#awaitBounded(
      Promise.all([
        ...this.#requestAcceptChains.values(),
        this.#controlAcceptChain,
      ]),
      deadline,
      "host frame processing",
    );
  }

  async #awaitBounded(
    promise: Promise<unknown>,
    deadline: number,
    operation: string,
  ): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const result = await Promise.race([
        promise.then(
          () => ({ kind: "complete" as const }),
          error => ({ kind: "failed" as const, error }),
        ),
        new Promise<{ kind: "timeout" }>(resolve => {
          timer = setTimeout(
            () => resolve({ kind: "timeout" }),
            Math.max(0, deadline - performance.now()),
          );
          (timer as any).unref?.();
        }),
      ]);
      if (result.kind === "timeout") {
        throw new SidecarCloseError(`${operation} exceeded the close deadline`, "timeout");
      }
      if (result.kind === "failed") {
        throw new SidecarCloseError(`${operation} failed during close`, "operation", {
          cause: result.error,
        });
      }
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  async #awaitOutputDrain(deadline: number): Promise<void> {
    while (performance.now() < deadline) {
      const outbound = this.#outbound.snapshot();
      const scheduled = this.#scheduler.snapshot();
      if (outbound.frames.length === 0 && scheduled.queuedFrames === 0) return;
      await new Promise<void>(resolve => setTimeout(
        resolve,
        Math.min(10, Math.max(0, deadline - performance.now())),
      ));
    }
    throw new SidecarCloseError(
      "Reliable output remained pending at the close deadline",
      "output",
    );
  }

  async #waitForReliableOutputDrain(signal: AbortSignal): Promise<void> {
    for (;;) {
      if (signal.aborted) throw signal.reason;
      const outbound = this.#outbound.snapshot();
      const scheduled = this.#scheduler.snapshot();
      if (outbound.frames.length === 0 && scheduled.queuedFrames === 0) return;
      await sleepWithSignal(10, signal);
    }
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
          this.#hostInstanceId = null;
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
          if (this.#hostClientId.data === null && this.#hostClientId.control === null) {
            this.#hostPeerId = null;
            this.#hostInstanceId = null;
          }
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
      const steadyHost = envelope.peerId === "host";
      const bootstrapHost = envelope.peerId === "host-bootstrap";
      if (!steadyHost && !bootstrapHost) continue;
      if (bootstrapHost) {
        // Bootstrap can ping/drain before a transport exists, but it must never
        // pin the steady host identity or trigger recovered response delivery.
        if (this.#hostInstanceId !== null) continue;
        if (envelope.revision <= this.#bootstrapRevision[lane]) continue;
        this.#bootstrapRevision[lane] = envelope.revision;
      } else {
        if (this.#hostPeerId && envelope.peerId !== this.#hostPeerId) continue;
        if (this.#hostInstanceId && envelope.instanceId !== this.#hostInstanceId) continue;
        if (envelope.revision <= this.#hostRevision[lane]) continue;
        this.#hostPeerId = envelope.peerId;
        this.#hostInstanceId = envelope.instanceId;
        this.#hostClientId[lane] = clientId;
        this.#hostRevision[lane] = envelope.revision;
        this.#observeBoundHostTransport(envelope);
      }
      if (envelope.liveness?.kind === "ping") {
        // Liveness is deliberately best-effort and out of band. Answer it
        // before (and instead of) the reliable ACK/window, admission, engine,
        // scheduler, and output-backpressure paths below.
        try {
          this.#publishLivenessPong(envelope.liveness.sentAtMs);
        } catch (error) {
          this.#reportEngineError(error, "Sidecar liveness publication failed");
        }
        continue;
      }
      try {
        if (lane === "control" && envelope.frames.length > 0) {
          throw new Error("host control Awareness state contains reliable frames");
        }
        const ceilingChanged = lane === "control" && envelope.credits !== this.#hostCredits;
        if (lane === "control") {
          this.#hostCredits = envelope.credits;
          this.#engine.setHostCeiling(envelope.credits);
        }
        const acknowledgedOutcomeIds = this.#outbound.snapshot().frames.flatMap(frame => (
          frame.epoch === envelope.ack.epoch &&
          frame.sequence <= envelope.ack.sequence &&
          (frame.kind === "response_error" || frame.kind === "request_cancelled")
            ? [frame.requestId]
            : []
        ));
        this.#outbound.acknowledge(envelope.ack);
        if (acknowledgedOutcomeIds.length) {
          this.#queueOutcomeAcknowledgements(acknowledgedOutcomeIds);
        }
        const flightCompleted = this.#flight.acknowledged(envelope.ack);
        if (flightCompleted) this.#flightCompletions++;
        const now = performance.now();
        const acknowledgementAdvanced = this.#delivery.acknowledged(
          envelope.ack,
          this.#outbound.snapshot().frames.length > 0,
          now,
        );
        let capacityChanged = false;
        if (acknowledgementAdvanced) {
          const rttMs = this.#delivery.roundTripEwmaMs;
          if (rttMs !== null) {
            capacityChanged = this.#adaptiveCapacity.observe({
              rttMs,
              congested: this.#congestion.congested,
            });
          }
          this.#armRetransmission();
        }
        let frames: readonly ReliableFrame<HostFrame>[] = [];
        if (lane === "data") {
          // Connecting before recovery is required so large recovered spools
          // can drain. Do not advance the inbound cursor until recovery has
          // actually completed, otherwise a synced host frame could be ACKed
          // before the engine ever processed it.
          if (this.#engine.snapshot().ready !== false) {
            const inbound = bootstrapHost ? this.#bootstrapInbound : this.#inbound;
            const processed = bootstrapHost
              ? this.#bootstrapProcessedAck
              : this.#processedAck;
            const currentEpoch = inbound.ack().epoch;
            if (envelope.epoch > currentEpoch) {
              if (processed.sequence !== inbound.ack().sequence) {
                throw new Error("Host epoch changed while accepted frames were still processing");
              }
              inbound.reset(envelope.epoch);
              if (bootstrapHost) {
                this.#bootstrapProcessedAck = { epoch: envelope.epoch, sequence: -1 };
                this.#bootstrapProcessedReady.clear();
              } else {
                this.#processedAck = { epoch: envelope.epoch, sequence: -1 };
                this.#processedReady.clear();
              }
            }
            const pendingFrames = envelope.frames.filter(frame => (
              frame.epoch === inbound.ack().epoch &&
              frame.sequence > inbound.ack().sequence
            ));
            if (!this.#inboundFrozen || pendingFrames.every(hostFrameIsControl)) {
              frames = inbound.accept(envelope.frames);
            }
          }
        }
        for (const frame of frames) this.#queueHostFrame(frame, bootstrapHost);
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
      } catch (error) {
        this.#reportEngineError(error, "Bound host protocol transition failed");
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
  #updateCongestion():boolean{const scheduled=this.#scheduler.snapshot();const outbound=this.#outbound.snapshot();const before=this.#congestion.congested;this.#congestion.update({schedulerFrames:scheduled.queuedFrames,schedulerBytes:scheduled.queuedEncodedBytes,oldestQueuedFrameMs:scheduled.oldestQueuedFrameMs,ackProgressAgeMs:this.#delivery.ackProgressAgeMs(performance.now()),windowFrames:outbound.frames.length});return before!==this.#congestion.congested;}

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

  #publishLivenessPong(sentAtMs: number): void {
    const current = this.#controlProvider.awareness.getLocalState()
      ?.ghostprovV2Control as AwarenessEnvelope<SidecarFrame> | undefined;
    const transport = this.#engine.snapshot();
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
      epoch: current?.epoch ?? 1,
      revision: this.#controlRevision++,
      ack: this.#publishedHostAck(),
      credits: this.#inboundFrozen || this.#recoveryReplayInProgress
        ? 0
        : (current?.credits ?? 0),
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames: [],
      lane: "control",
      liveness: { kind: "pong", sentAtMs },
      ...(current?.diagnostics ? { diagnostics: current.diagnostics } : {}),
    };
    const encodedBytes = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    this.#publications++;
    this.#urgentPublications++;
    this.#publishedBytes += encodedBytes;
    this.#controlPublications++;
    this.#controlPublishedBytes += encodedBytes;
    this.#controlProvider.awareness.setLocalStateField("ghostprovV2Control", envelope);
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
    const adaptive = this.#adaptiveCapacity.snapshot();
    this.#diagnostics?.transport({ windowFrames: outbound.frames.length, windowBytes: outbound.encodedBytes, schedulerFrames: scheduled.queuedFrames, schedulerBytes: scheduled.queuedEncodedBytes, schedulerStreams: scheduled.streams, oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs, ackProgressAgeMs: this.#delivery.ackProgressAgeMs(performance.now()), retransmissionAttempt: this.#delivery.attempt, peakRetransmissionAttempt: this.#peakAttempt, retransmissionsTotal: this.#retransmissions, publicationsTotal: this.#publications, publishedBytesTotal: this.#publishedBytes, admittedUncommitted: transport.active, roundTripEwmaMs: this.#delivery.roundTripEwmaMs, pacedPublicationsTotal: this.#pacedPublications, urgentPublicationsTotal: this.#urgentPublications, logicalEventsTotal: this.#logicalEvents, responseBatchFramesTotal: this.#responseBatchFrames, batchedEventsTotal: this.#batchedEvents, dataPublicationsTotal: this.#dataPublications, controlPublicationsTotal: this.#controlPublications, dataPublishedBytesTotal: this.#dataPublishedBytes, controlPublishedBytesTotal: this.#controlPublishedBytes, flightCompletionsTotal: this.#flightCompletions, adaptiveCapacity: adaptive.limit, capacityIncreasesTotal: adaptive.increases, capacityReductionsTotal: adaptive.reductions, unadmittedTimeoutsTotal: 0 });
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
      ack: this.#publishedHostAck(),
      credits:
        transport.ready === false ||
        transport.draining ||
        this.#inboundFrozen ||
        this.#recoveryReplayInProgress ||
        this.#congestion.congested
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
    const encodedBytes = new TextEncoder().encode(JSON.stringify(envelope)).byteLength;
    this.#publishedBytes += encodedBytes;
    if (lane === "data") {
      this.#dataPublications++;
      this.#dataPublishedBytes += encodedBytes;
    } else {
      this.#controlPublications++;
      this.#controlPublishedBytes += encodedBytes;
    }
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

  #observeBoundHostTransport(envelope: AwarenessEnvelope<HostFrame>): void {
    // Bootstrap publishes as "host-bootstrap" and is not a steady-state
    // transport capable of draining a large response window. The production
    // V2TransportSession publishes as "host" only after bootstrap readiness.
    if (
      this.#recoveryReplayStarted ||
      this.#recoveryReplayCompleted ||
      this.#recoveryReplayFailed ||
      envelope.peerId !== "host" ||
      this.#engine.snapshot().ready === false
    ) return;
    const replayRecovered = (this.#engine as SidecarEngine & {
      replayRecovered?: (signal?: AbortSignal) => Promise<void>;
    }).replayRecovered;
    if (typeof replayRecovered !== "function") return;
    this.#recoveryReplayStarted = true;
    this.#recoveryReplayInProgress = true;
    this.#inboundFrozen = true;
    this.#pacer.requestUrgent();
    const replay = (async () => {
      await replayRecovered.call(this.#engine, this.#lifecycleAbort.signal);
      await this.#waitForReliableOutputDrain(this.#lifecycleAbort.signal);
    })();
    this.#recoveryReplayPromise = replay;
    void replay.then(
      () => {
        this.#recoveryReplayCompleted = true;
        this.#inboundFrozen = false;
        this.#reprocessSteadyHostData();
      },
      value => {
        if (this.#lifecycleAbort.signal.aborted) return;
        this.#recoveryReplayFailed = true;
        this.#reportEngineError(value, "Recovered terminal replay failed");
      },
    ).finally(() => {
      if (this.#recoveryReplayPromise === replay) this.#recoveryReplayPromise = null;
      this.#recoveryReplayStarted = false;
      this.#recoveryReplayInProgress = false;
      this.#pacer.requestUrgent();
    });
  }

  #reportEngineError(value: unknown, fallback: string): void {
    const error = value instanceof Error ? value : new Error(fallback);
    this.#onEngineError?.(error);
  }

  #publishedHostAck(): { epoch: number; sequence: number } {
    return this.#hostInstanceId === null
      ? this.#bootstrapProcessedAck
      : this.#processedAck;
  }

  #queueOutcomeAcknowledgements(requestIds: readonly string[]): void {
    const acknowledge = (this.#engine as SidecarEngine & {
      acknowledgeDeliveredOutcomes?: (requestIds: readonly string[]) => Promise<void>;
    }).acknowledgeDeliveredOutcomes;
    if (typeof acknowledge !== "function") return;
    const next = this.#controlAcceptChain.then(() => (
      acknowledge.call(this.#engine, requestIds)
    ));
    this.#controlAcceptChain = next;
    void next.catch(error => {
      this.#reportEngineError(error, "Durable outcome acknowledgement failed");
    });
  }

  #queueHostFrame(
    frame: ReliableFrame<HostFrame>,
    bootstrapHost: boolean,
  ): void {
    if (frame.kind === "drain") this.#inboundFrozen = true;
    const requestId = "requestId" in frame ? frame.requestId : null;
    const previous = requestId
      ? this.#requestAcceptChains.get(requestId) ?? Promise.resolve()
      : this.#controlAcceptChain;
    const next = previous
      .then(() => this.#acceptHostFrame(frame, this.#lifecycleAbort.signal))
      .then(() => this.#markHostFrameProcessed(frame, bootstrapHost))
      .catch(error => {
        if (!this.#reportAcceptError(frame, error)) {
          this.#markHostFrameProcessed(frame, bootstrapHost);
        }
      });
    if (requestId) {
      this.#requestAcceptChains.set(requestId, next);
      void next.then(
        () => {
          if (this.#requestAcceptChains.get(requestId) === next) {
            this.#requestAcceptChains.delete(requestId);
          }
        },
        () => {
          if (this.#requestAcceptChains.get(requestId) === next) {
            this.#requestAcceptChains.delete(requestId);
          }
        },
      );
    } else {
      this.#controlAcceptChain = next;
    }
  }

  #markHostFrameProcessed(
    frame: ReliableFrame<HostFrame>,
    bootstrapHost: boolean,
  ): void {
    const processed = bootstrapHost
      ? this.#bootstrapProcessedAck
      : this.#processedAck;
    if (frame.epoch !== processed.epoch || frame.sequence <= processed.sequence) return;
    const ready = bootstrapHost
      ? this.#bootstrapProcessedReady
      : this.#processedReady;
    ready.set(frame.sequence, frame);
    let sequence = processed.sequence;
    while (ready.has(sequence + 1)) {
      ready.delete(sequence + 1);
      sequence++;
    }
    if (sequence !== processed.sequence) {
      if (bootstrapHost) {
        this.#bootstrapProcessedAck = { epoch: frame.epoch, sequence };
      } else {
        this.#processedAck = { epoch: frame.epoch, sequence };
      }
      this.#pacer.requestUrgent();
    }
  }

  /** Returns true when the failure is engine-wide and must remain un-ACKed. */
  #reportAcceptError(frame: ReliableFrame<HostFrame>, value: unknown): boolean {
    if (this.#lifecycleAbort.signal.aborted) return true;
    const error = value instanceof Error
      ? value
      : new Error("Sidecar frame processing failed");
    if (
      frame.kind === "request_open" ||
      frame.kind === "request_data" ||
      frame.kind === "request_commit"
    ) {
      this.enqueue({
        kind: "request_reject",
        requestId: frame.requestId,
        code: "sidecar_accept_failed",
        message: "Sidecar could not safely accept the request frame",
      }, "control", frame.requestId);
    } else if (frame.kind === "request_cancel") {
      this.enqueue({
        kind: "response_error",
        requestId: frame.requestId,
        code: "sidecar_cancel_failed",
        message: "Sidecar could not safely process cancellation",
        ambiguous: true,
      }, "terminal", frame.requestId);
    } else if (frame.kind === "credential_rotate") {
      this.enqueue({
        kind: "response_error",
        requestId: "credential",
        code: "credential_rotation_failed",
        message: "Sidecar credential rotation failed",
        ambiguous: false,
      }, "terminal", "credential");
    }
    // Malformed or incomplete request-local state is rejected in-band and
    // must not tear down unrelated healthy streams. Only failures in durable
    // terminal ACK, lifecycle control, engine progress, or credentials imply
    // that the sidecar as a whole is no longer trustworthy.
    const errorCode = "code" in error
      ? (error as Error & { code?: unknown }).code
      : undefined;
    const systemic =
      error.name === "SidecarOwnershipError" ||
      (error.name === "ClaimJournalError" &&
        (errorCode === "storage" || errorCode === "corrupt"));
    const fatal = systemic || (
      frame.kind === "response_ack" ||
      frame.kind === "drain" ||
      frame.kind === "ping" ||
      frame.kind === "credential_rotate"
    );
    if (fatal) {
      this.#onAcceptError?.(error, frame);
    }
    return fatal;
  }

  async #acceptHostFrame(
    frame: ReliableFrame<HostFrame>,
    signal: AbortSignal,
  ): Promise<void> {
    if (frame.kind !== "credential_rotate") {
      await this.#engine.accept(frame, signal);
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
