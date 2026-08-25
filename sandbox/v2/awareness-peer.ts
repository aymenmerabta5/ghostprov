import * as Y from "yjs";
import { WebsocketProvider } from "y-websocket";
import type { Awareness } from "y-protocols/awareness";
import type { BootstrapCredentialPayload } from "../../src/v2/bootstrap/crypto.ts";
import { SidecarCredentialRotator } from "../../src/v2/bootstrap/rotation.ts";
import {
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  awarenessEnvelopeEncodedBytes,
  protocolTransportLimits,
  type AwarenessLane,
  type AwarenessEnvelope,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type R2AwarenessEnvelope,
  type R2RequestCommitFrame,
  type R2RequestStartFrame,
  type SidecarFrame,
  type V2ProtocolTransportLimits,
} from "../../src/v2/protocol/shared.ts";
import { AckClockedFlight } from "../../src/v2/protocol/flight.ts";
import {
  encodedFrameBytes,
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
import { V2_TERMINAL_SPOOL_CAPACITY } from "../../src/v2/sidecar/capacity.ts";
import {
  DEFAULT_V2_INITIAL_RETRANSMIT_MS,
  deterministicV2JitterSeed,
} from "../../src/v2/protocol/retransmit.ts";

const DEFAULT_TRANSPORT_STALL_MS = 2_000;
const GOODPUT_IDLE_RESET_MS = 60_000;
const STALL_DERATE_STRIKES = 2;
const STALL_DERATE_WINDOW_MS = 60_000;

function sidecarFrameCarriesResponseData(frame: ReliableFrame<SidecarFrame>): boolean {
  return frame.kind === "response_event" || frame.kind === "response_batch";
}

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
  transportStallMs?: number;
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
  readonly #limits: V2ProtocolTransportLimits;
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
  #processedFlightHigh = -1;
  #bootstrapProcessedFlightHigh = -1;
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
  #steadyHostActive = false;
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
  #stallTimer: number | null = null;
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
  readonly #congestion: TransportCongestion;
  readonly #transportStallMs: number;
  readonly #fastRetransmitFloorMs: number;
  #stallEvents = 0;
  #stallStrikes = 0;
  #lastStallAtMs: number | null = null;
  #flightStartedAtMs: number | null = null;
  #flightUniqueBytes = 0;
  #acknowledgedGoodputBytesPerSecond: number | null = null;
  #goodputUpdatedAtMs: number | null = null;
  #uniqueAcknowledgedBytes = 0;
  #retransmittedBytes = 0;
  #lastFastRetransmitAtMs: number | null = null;
  #stallTimerDueAtMs: number | null = null;
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
  readonly #recoveryTerminalAcks = new Set<string>();
  #inboundFrozen = false;
  readonly #requestAcceptChains = new Map<string, Promise<void>>();
  readonly #requestAcceptStarts = new Map<string, Promise<void>>();
  #controlAcceptChain: Promise<void> = Promise.resolve();
  readonly #processedReady = new Map<number, ReliableFrame<HostFrame>>();
  readonly #bootstrapProcessedReady = new Map<number, ReliableFrame<HostFrame>>();

  constructor(options: DenoAwarenessPeerOptions) {
    this.#binding = options.binding;
    this.#limits = protocolTransportLimits(options.binding.protocolRevision);
    this.#instanceId = options.instanceId;
    if (
      options.binding.protocolRevision === "r2" &&
      !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(options.binding.launchNonce)
    ) throw new TypeError("R2 launch nonce is invalid");
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
    this.#transportStallMs = options.transportStallMs ?? DEFAULT_TRANSPORT_STALL_MS;
    if (!Number.isFinite(this.#transportStallMs) || this.#transportStallMs < 1) {
      throw new RangeError("transportStallMs must be positive");
    }
    this.#fastRetransmitFloorMs = Math.min(1_000, this.#transportStallMs);
    this.#congestion = new TransportCongestion({ highAgeMs: this.#transportStallMs });
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
      initialRtoMs: options.retransmitMs === undefined
        ? Math.max(this.#retransmitMs, DEFAULT_V2_INITIAL_RETRANSMIT_MS)
        : this.#retransmitMs,
    });
    this.#pacer = new PublicationPacer({
      intervalMs: options.publicationIntervalMs,
      publish: reason => this.#publish(reason),
    });
    const initialTransportCapacity = options.initialActivePerSandbox ?? 8;
    this.#adaptiveCapacity = new AdaptiveTransportCapacity({
      initial: initialTransportCapacity,
      maximum: options.maxActivePerSandbox ?? V2_TERMINAL_SPOOL_CAPACITY,
      // Preserve useful fleet-wide concurrency during correlated degradation.
      // Active congestion still advertises zero until progress resumes; the
      // persistent penalty only halves the normal eight-slot ceiling.
      minimum: Math.min(4, initialTransportCapacity),
    });
    this.#dataProvider.awareness.on("change", this.#onDataAwarenessChange);
    this.#controlProvider.awareness.on("change", this.#onControlAwarenessChange);
    this.#controlProvider.on("connection-close", this.#onControlConnectionUnavailable);
    this.#controlProvider.on("connection-error", this.#onControlConnectionUnavailable);
  }

  /**
   * Re-proves the dynamic half of an R2 execution fence. The engine invokes
   * this before durable claim and at the upstream fetch boundary, not merely
   * when the transport first observes the commit frame.
   */
  assertExecutionFence(frame: R2RequestCommitFrame | R2RequestStartFrame): void {
    if (this.#binding.protocolRevision !== "r2") {
      throw new Error("R2 execution fence cannot be used by an R1 peer");
    }
    if (
      frame.protocolRevision !== "r2" ||
      frame.schemaVersion !== GP_V2_R2_SCHEMA_VERSION ||
      frame.projectId !== this.#binding.projectId ||
      frame.sessionId !== this.#binding.sessionId ||
      frame.leaderGeneration !== this.#binding.leaderGeneration ||
      frame.buildHash !== this.#binding.buildHash ||
      frame.schemaHash !== this.#binding.schemaHash ||
      frame.launchNonce !== this.#binding.launchNonce ||
      frame.targetInstanceId !== this.#instanceId ||
      this.#closed ||
      this.#closing ||
      !this.#steadyHostActive ||
      this.#hostClientId.control === null ||
      this.#hostInstanceId === null ||
      frame.hostInstanceId !== this.#hostInstanceId
    ) {
      throw new Error("R2 execution fence is no longer elected");
    }
    const raw = this.#controlProvider.awareness.getStates()
      .get(this.#hostClientId.control)?.ghostprovV2Control;
    try {
      const current = parseAwarenessEnvelopeWithDiagnostics(
        raw,
        this.#binding,
        "host",
      ).envelope as R2AwarenessEnvelope<HostFrame>;
      if (
        current.peerId !== "host" ||
        current.instanceId !== this.#hostInstanceId ||
        current.targetInstanceId !== this.#instanceId ||
        current.lane !== "control" ||
        current.revision !== this.#hostRevision.control ||
        current.draining
      ) throw new Error("R2 control election state is not current");
    } catch (error) {
      throw new Error("R2 execution fence is no longer elected", { cause: error });
    }
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
    if (
      this.#recoveryReplayInProgress &&
      frame.kind === "response_terminal" &&
      typeof (this.#engine as SidecarEngine & {
        hasPendingTerminalAcknowledgement?: (requestId: string) => boolean;
      }).hasPendingTerminalAcknowledgement === "function"
    ) {
      this.#recoveryTerminalAcks.add(frame.requestId);
    }
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
    this.#recoveryTerminalAcks.clear();
    this.#requestAcceptStarts.clear();
    try { this.#pacer.close(); } catch (error) { lifecycleErrors.push(error); }
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#stallTimer !== null) clearTimeout(this.#stallTimer);
    this.#stallTimer = null;
    this.#stallTimerDueAtMs = null;
    const cleanup = (operation: () => void) => {
      try { operation(); } catch (error) { lifecycleErrors.push(error); }
    };
    cleanup(() => this.#dataProvider.awareness.off("change", this.#onDataAwarenessChange));
    cleanup(() => this.#controlProvider.awareness.off("change", this.#onControlAwarenessChange));
    cleanup(() => this.#controlProvider.off("connection-close", this.#onControlConnectionUnavailable));
    cleanup(() => this.#controlProvider.off("connection-error", this.#onControlConnectionUnavailable));
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

  readonly #onControlConnectionUnavailable = (): void => {
    if (this.#binding.protocolRevision !== "r2") return;
    this.#steadyHostActive = false;
    this.#hostClientId.control = null;
    this.#hostRevision.control = -1;
  };

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
        if (this.#binding.protocolRevision === "r2" && lane === "control") {
          this.#steadyHostActive = false;
        }
        if (
          this.#binding.protocolRevision === "r1" &&
          this.#hostClientId.data === null && this.#hostClientId.control === null
        ) {
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
          if (this.#binding.protocolRevision === "r2" && lane === "control") {
            this.#steadyHostActive = false;
          }
          if (
            this.#binding.protocolRevision === "r1" &&
            this.#hostClientId.data === null && this.#hostClientId.control === null
          ) {
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
        if (
          this.#binding.protocolRevision === "r2" &&
          lane === "control" &&
          clientId === this.#hostClientId.control
        ) {
          this.#hostClientId.control = null;
          this.#hostRevision.control = -1;
          this.#steadyHostActive = false;
        }
        continue;
      }
      if (
        this.#binding.protocolRevision === "r2" &&
        (envelope as R2AwarenessEnvelope<HostFrame>).targetInstanceId !== this.#instanceId
      ) {
        if (lane === "control" && clientId === this.#hostClientId.control) {
          this.#hostClientId.control = null;
          this.#hostRevision.control = -1;
          this.#steadyHostActive = false;
        }
        continue;
      }
      if (envelope.lane !== undefined && envelope.lane !== lane) {
        if (
          this.#binding.protocolRevision === "r2" &&
          lane === "control" &&
          clientId === this.#hostClientId.control
        ) {
          this.#hostClientId.control = null;
          this.#hostRevision.control = -1;
          this.#steadyHostActive = false;
        }
        continue;
      }
      const steadyHost = envelope.peerId === "host";
      const bootstrapHost = envelope.peerId === "host-bootstrap";
      if (!steadyHost && !bootstrapHost) {
        if (
          this.#binding.protocolRevision === "r2" &&
          lane === "control" &&
          clientId === this.#hostClientId.control
        ) {
          this.#hostClientId.control = null;
          this.#hostRevision.control = -1;
          this.#steadyHostActive = false;
        }
        continue;
      }
      if (bootstrapHost) {
        // Bootstrap can ping/drain before a transport exists, but it must never
        // pin the steady host identity or trigger recovered response delivery.
        if (this.#hostInstanceId !== null) {
          if (
            this.#binding.protocolRevision === "r2" &&
            lane === "control" &&
            clientId === this.#hostClientId.control
          ) {
            this.#hostClientId.control = null;
            this.#hostRevision.control = -1;
            this.#steadyHostActive = false;
          }
          continue;
        }
        if (envelope.frames.some(frame => frame.kind !== "ping" && frame.kind !== "drain")) {
          continue;
        }
        if (
          this.#binding.protocolRevision === "r2" &&
          envelope.instanceId !== this.#binding.launchNonce
        ) continue;
        if (envelope.revision <= this.#bootstrapRevision[lane]) continue;
        this.#bootstrapRevision[lane] = envelope.revision;
      } else {
        if (this.#binding.protocolRevision === "r2") {
          // R2 election is proven only by a currently present, exact control
          // state. Data is executable only while that live control proof is
          // active. The pinned ID survives removal solely to permit a
          // same-host reconnect; a replacement ID can never take over.
          if (this.#hostInstanceId === null) {
            if (lane !== "control") continue;
            this.#hostPeerId = envelope.peerId;
            this.#hostInstanceId = envelope.instanceId;
          }
          if (
            envelope.peerId !== this.#hostPeerId ||
            envelope.instanceId !== this.#hostInstanceId
          ) {
            if (lane === "control" && clientId === this.#hostClientId.control) {
              this.#hostClientId.control = null;
              this.#hostRevision.control = -1;
              this.#steadyHostActive = false;
            }
            continue;
          }
          if (envelope.revision <= this.#hostRevision[lane]) {
            if (lane === "control" && clientId === this.#hostClientId.control) {
              // Awareness replaces the raw value even when its application
              // revision rolls back. Revoke election until a strictly newer
              // exact control proof arrives; never validate execution against
              // the stale recorded revision and the rolled-back raw state.
              this.#hostClientId.control = null;
              this.#steadyHostActive = false;
            }
            continue;
          }
          if (lane === "control") this.#steadyHostActive = !envelope.draining;
          else if (!this.#steadyHostActive) continue;
          this.#hostClientId[lane] = clientId;
          this.#hostRevision[lane] = envelope.revision;
          this.#observeBoundHostTransport(envelope);
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
      }
      if (envelope.liveness?.kind === "ping") {
        // Liveness is deliberately best-effort and out of band. Answer it
        // before (and instead of) the reliable ACK/window, admission, engine,
        // scheduler, and output-backpressure paths below.
        try {
          // The control ping proves the peer is alive even though liveness is
          // deliberately not allowed to advance the reliable ACK cursor. Use
          // that proof to retransmit a stale flight without waiting for the
          // exponential RTO.
          this.#maybeFastRetransmit(performance.now());
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
        const now = performance.now();
        if (flightCompleted) {
          this.#flightCompletions++;
          this.#recordAcknowledgedFlight(now);
        }
        const rttSamplesBefore = this.#delivery.roundTripSamples;
        const acknowledgementAdvanced = this.#delivery.acknowledged(
          envelope.ack,
          this.#outbound.snapshot().frames.length > 0,
          now,
        );
        let congestionChanged = this.#updateCongestion();
        let capacityChanged = false;
        if (acknowledgementAdvanced) {
          this.#lastFastRetransmitAtMs = null;
          const rttMs = this.#delivery.roundTripEwmaMs;
          const freshHealthyRtt = this.#delivery.roundTripSamples > rttSamplesBefore &&
            !this.#congestion.congested;
          if (rttMs !== null && freshHealthyRtt) {
            capacityChanged = this.#adaptiveCapacity.observe({
              rttMs,
              // Persistent reductions are owned exclusively by #recordStall's
              // two-strike gate. ACK progress can only prove healthy recovery.
              congested: false,
            });
          }
          this.#armRetransmission();
        }
        if (!acknowledgementAdvanced && lane === "control") {
          this.#maybeFastRetransmit(now);
        }
        let frames: readonly ReliableFrame<HostFrame>[] = [];
        let replayProcessedAck = false;
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
                this.#bootstrapProcessedFlightHigh = -1;
                this.#bootstrapProcessedReady.clear();
              } else {
                this.#processedAck = { epoch: envelope.epoch, sequence: -1 };
                this.#processedFlightHigh = -1;
                this.#processedReady.clear();
              }
            }
            const envelopeHigh = envelope.frames.reduce(
              (high, frame) => frame.epoch === inbound.ack().epoch
                ? Math.max(high, frame.sequence)
                : high,
              -1,
            );
            const pendingFrames = envelope.frames.filter(frame => (
              frame.epoch === inbound.ack().epoch &&
              frame.sequence > inbound.ack().sequence
            ));
            if (!this.#inboundFrozen || pendingFrames.every(hostFrameIsControl)) {
              frames = inbound.accept(envelope.frames);
            }
            if (frames.length > 0) {
              const acceptedHigh = frames.at(-1)!.sequence;
              if (bootstrapHost) {
                this.#bootstrapProcessedFlightHigh = Math.max(
                  this.#bootstrapProcessedFlightHigh,
                  acceptedHigh,
                );
              } else {
                this.#processedFlightHigh = Math.max(this.#processedFlightHigh, acceptedHigh);
              }
            } else if (envelopeHigh >= 0 && envelopeHigh <= processed.sequence) {
              // A repeated immutable flight proves the sender missed the final
              // ACK-only update. Republish the current processed cursor.
              replayProcessedAck = true;
            }
          }
        }
        for (const frame of frames) this.#queueHostFrame(frame, bootstrapHost);
        const urgency = this.#pump();
        if (flightCompleted && urgency === null) {
          this.#dataProvider.awareness.setLocalStateField("ghostprovV2Data", null);
        }
        this.#updateBackpressure();
        congestionChanged = this.#updateCongestion() || congestionChanged;
        this.#requestPublication(
          replayProcessedAck || ceilingChanged || congestionChanged || capacityChanged
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
        this.#limits.sidecarFlightMaxFrames - snapshot.frames.length,
        this.#limits.windowMaxFrames - snapshot.frames.length,
      );
      const remainingBytes = Math.min(
        this.#limits.sidecarFlightMaxBytes - snapshot.encodedBytes,
        this.#limits.windowMaxBytes - snapshot.encodedBytes,
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
  #updateCongestion():boolean{const scheduled=this.#scheduler.snapshot();const outbound=this.#outbound.snapshot();const before=this.#congestion.congested;const ackProgressAgeMs=this.#delivery.ackProgressAgeMs(performance.now());this.#congestion.update({schedulerFrames:scheduled.queuedFrames,schedulerBytes:scheduled.queuedEncodedBytes,oldestQueuedFrameMs:scheduled.oldestQueuedFrameMs,ackProgressAgeMs,windowFrames:outbound.frames.length});if(!before&&this.#congestion.congested&&outbound.frames.length>0&&ackProgressAgeMs>=this.#transportStallMs){this.#stallEvents++;this.#recordStall();}return before!==this.#congestion.congested;}

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
    this.#armStallWatchdog();
  }

  #publish(reason: PublicationReason): void {
    if (this.#closed) return;
    const outbound = this.#outbound.snapshot();
    const publishData = reason === "retransmission" || this.#dataDirty;
    if (publishData) {
      if (!this.#flight.active) {
        this.#flightStartedAtMs = performance.now();
        this.#flightUniqueBytes = outbound.frames
          .filter(sidecarFrameCarriesResponseData)
          .reduce(
          (total, frame) => total + encodedFrameBytes(frame),
          0,
          );
      }
      if (reason === "retransmission") {
        this.#retransmittedBytes += outbound.encodedBytes;
      }
      this.#flight.published(outbound.frames);
      this.#delivery.published(outbound.frames, performance.now());
      this.#publishLane("data", outbound.frames, reason);
      this.#dataDirty = false;
    }
    if (reason !== "retransmission" || !publishData) {
      this.#publishLane("control", [], reason);
    }
    this.#armRetransmission();
    this.#armStallWatchdog();
  }

  #publishLivenessPong(sentAtMs: number): void {
    const current = this.#controlProvider.awareness.getLocalState()
      ?.ghostprovV2Control as AwarenessEnvelope<SidecarFrame> | undefined;
    const transport = this.#engine.snapshot();
    const common = {
      protocol: "gp-awareness-v2",
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
      credits: this.#inboundFrozen || this.#recoveryReplayInProgress || this.#congestion.congested
        ? 0
        : (current?.credits ?? 0),
      active: transport.active,
      queued: transport.queued,
      draining: transport.draining,
      frames: [],
      lane: "control",
      liveness: { kind: "pong", sentAtMs },
      ...(current?.diagnostics ? { diagnostics: current.diagnostics } : {}),
    } as const;
    const envelope: AwarenessEnvelope<SidecarFrame> =
      this.#binding.protocolRevision === "r2"
        ? {
            ...common,
            protocolRevision: "r2",
            schemaVersion: GP_V2_R2_SCHEMA_VERSION,
            launchNonce: this.#binding.launchNonce,
            targetInstanceId: this.#hostInstanceId ?? this.#binding.launchNonce,
          }
        : { ...common, schemaVersion: GP_V2_R1_SCHEMA_VERSION };
    const encodedBytes = awarenessEnvelopeEncodedBytes(
      envelope,
      0,
    );
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
    this.#diagnostics?.transport({ windowFrames: outbound.frames.length, windowBytes: outbound.encodedBytes, schedulerFrames: scheduled.queuedFrames, schedulerBytes: scheduled.queuedEncodedBytes, schedulerStreams: scheduled.streams, oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs, ackProgressAgeMs: this.#delivery.ackProgressAgeMs(performance.now()), retransmissionAttempt: this.#delivery.attempt, peakRetransmissionAttempt: this.#peakAttempt, retransmissionsTotal: this.#retransmissions, publicationsTotal: this.#publications, publishedBytesTotal: this.#publishedBytes, admittedUncommitted: transport.active, roundTripEwmaMs: this.#delivery.roundTripEwmaMs, roundTripVariationMs: this.#delivery.roundTripVariationMs, pacedPublicationsTotal: this.#pacedPublications, urgentPublicationsTotal: this.#urgentPublications, logicalEventsTotal: this.#logicalEvents, responseBatchFramesTotal: this.#responseBatchFrames, batchedEventsTotal: this.#batchedEvents, dataPublicationsTotal: this.#dataPublications, controlPublicationsTotal: this.#controlPublications, dataPublishedBytesTotal: this.#dataPublishedBytes, controlPublishedBytesTotal: this.#controlPublishedBytes, flightCompletionsTotal: this.#flightCompletions, adaptiveCapacity: adaptive.limit, capacityIncreasesTotal: adaptive.increases, capacityReductionsTotal: adaptive.reductions, unadmittedTimeoutsTotal: 0, acknowledgedGoodputBytesPerSecond: this.#currentGoodput(), uniqueAcknowledgedBytesTotal: this.#uniqueAcknowledgedBytes, retransmittedBytesTotal: this.#retransmittedBytes, stallEventsTotal: this.#stallEvents });
    const common = {
      protocol: "gp-awareness-v2",
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
    } as const;
    const envelope: AwarenessEnvelope<SidecarFrame> =
      this.#binding.protocolRevision === "r2"
        ? {
            ...common,
            protocolRevision: "r2",
            schemaVersion: GP_V2_R2_SCHEMA_VERSION,
            launchNonce: this.#binding.launchNonce,
            targetInstanceId: this.#hostInstanceId ?? this.#binding.launchNonce,
          }
        : { ...common, schemaVersion: GP_V2_R1_SCHEMA_VERSION };
    const encodedBytes = awarenessEnvelopeEncodedBytes(
      envelope,
      frames.length > 0 ? outbound.encodedBytes : 0,
    );
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
    const jitterSeed = deterministicV2JitterSeed(this.#instanceId, revision);
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

  #currentGoodput(): number | null {
    if (
      this.#acknowledgedGoodputBytesPerSecond === null ||
      this.#goodputUpdatedAtMs === null ||
      performance.now() - this.#goodputUpdatedAtMs >= GOODPUT_IDLE_RESET_MS
    ) return null;
    return this.#acknowledgedGoodputBytesPerSecond;
  }

  #recordAcknowledgedFlight(now: number): void {
    const started = this.#flightStartedAtMs;
    const bytes = this.#flightUniqueBytes;
    this.#flightStartedAtMs = null;
    this.#flightUniqueBytes = 0;
    if (bytes > 0) this.#uniqueAcknowledgedBytes += bytes;
    if (started === null || bytes <= 0 || now <= started) return;
    const sample = bytes * 1_000 / (now - started);
    this.#acknowledgedGoodputBytesPerSecond =
      this.#acknowledgedGoodputBytesPerSecond === null
        ? sample
        : this.#acknowledgedGoodputBytesPerSecond * 0.75 + sample * 0.25;
    this.#goodputUpdatedAtMs = now;
  }

  #armStallWatchdog(): void {
    if (this.#closed) return;
    const scheduled = this.#scheduler.snapshot();
    const outbound = this.#outbound.snapshot();
    if (scheduled.queuedFrames === 0 && outbound.frames.length === 0) {
      if (this.#stallTimer !== null) clearTimeout(this.#stallTimer);
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#updateCongestion()) this.#pacer.requestUrgent();
      return;
    }
    const now = performance.now();
    const age = Math.max(
      scheduled.oldestQueuedFrameMs,
      outbound.frames.length > 0
        ? this.#delivery.ackProgressAgeMs(now)
        : 0,
    );
    const delay = this.#congestion.congested
      ? Math.max(100, Math.min(500, this.#transportStallMs / 2))
      : Math.max(1, this.#transportStallMs - age);
    const dueAt = now + delay;
    if (
      this.#stallTimer !== null && this.#stallTimerDueAtMs !== null &&
      this.#stallTimerDueAtMs <= dueAt
    ) return;
    if (this.#stallTimer !== null) clearTimeout(this.#stallTimer);
    this.#stallTimerDueAtMs = dueAt;
    this.#stallTimer = setTimeout(() => {
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#closed) return;
      const changed = this.#updateCongestion();
      if (changed) this.#pacer.requestUrgent();
      this.#armStallWatchdog();
    }, delay) as unknown as number;
    (this.#stallTimer as any).unref?.();
  }

  #maybeFastRetransmit(now: number): boolean {
    if (!this.#flight.active || this.#outbound.snapshot().frames.length === 0) return false;
    const rtt = this.#delivery.roundTripEwmaMs;
    const threshold = Math.max(
      this.#fastRetransmitFloorMs,
      (rtt ?? this.#fastRetransmitFloorMs) * 1.5,
    );
    if (this.#delivery.ackProgressAgeMs(now) < threshold) return false;
    if (
      this.#lastFastRetransmitAtMs !== null &&
      now - this.#lastFastRetransmitAtMs < Math.max(
        this.#fastRetransmitFloorMs,
        (rtt ?? this.#fastRetransmitFloorMs) * 2,
      )
    ) return false;
    this.#lastFastRetransmitAtMs = now;
    this.#delivery.retransmitted(now);
    this.#retransmissions++;
    this.#peakAttempt = Math.max(this.#peakAttempt, this.#delivery.attempt);
    this.#pacer.flushNow("retransmission");
    return true;
  }

  #recordStall(): void {
    const now = performance.now();
    if (
      this.#lastStallAtMs === null ||
      now - this.#lastStallAtMs > STALL_DERATE_WINDOW_MS
    ) this.#stallStrikes = 0;
    this.#lastStallAtMs = now;
    this.#stallStrikes++;
    if (this.#stallStrikes < STALL_DERATE_STRIKES) return;
    this.#stallStrikes = 0;
    this.#adaptiveCapacity.observe({
      rttMs: this.#delivery.roundTripEwmaMs ?? this.#transportStallMs,
      congested: true,
    });
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
      hasRecoveredWork?: () => boolean;
    }).replayRecovered;
    if (typeof replayRecovered !== "function") return;
    const hasRecoveredWork = (this.#engine as SidecarEngine & {
      hasRecoveredWork?: () => boolean;
    }).hasRecoveredWork?.call(this.#engine) === true;
    const recoveryRequested = this.#binding.protocolRevision === "r2" &&
      envelope.credits === 0 && envelope.active > 0 && !envelope.draining;
    if (!hasRecoveredWork && !recoveryRequested) return;
    this.#recoveryReplayStarted = true;
    this.#recoveryReplayInProgress = true;
    this.#inboundFrozen = true;
    this.#pacer.requestUrgent();
    const replay = (async () => {
      await replayRecovered.call(this.#engine, this.#lifecycleAbort.signal);
      await this.#waitForReliableOutputDrain(this.#lifecycleAbort.signal);
      await this.#waitForStableAcceptDrain(this.#lifecycleAbort.signal);
      await this.#waitForRecoveryTerminalAcks(this.#lifecycleAbort.signal);
      await this.#waitForStableAcceptDrain(this.#lifecycleAbort.signal);
      if (this.#binding.protocolRevision === "r2") {
        this.enqueue(
          { kind: "recovery_complete" },
          "control",
          "recovery",
        );
        // Keep advertised credits at zero until the host has cumulatively
        // acknowledged the explicit replay barrier itself.
        await this.#waitForReliableOutputDrain(this.#lifecycleAbort.signal);
      }
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

  async #waitForStableAcceptDrain(signal: AbortSignal): Promise<void> {
    for (;;) {
      if (signal.aborted) throw signal.reason;
      const control = this.#controlAcceptChain;
      const requests = [...this.#requestAcceptChains.entries()];
      await Promise.all([control, ...requests.map(([, pending]) => pending)]);
      if (signal.aborted) throw signal.reason;
      if (
        control === this.#controlAcceptChain &&
        this.#requestAcceptChains.size === 0
      ) return;
    }
  }

  async #waitForRecoveryTerminalAcks(signal: AbortSignal): Promise<void> {
    while (this.#recoveryTerminalAcks.size > 0) {
      await sleepWithSignal(5, signal);
    }
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
    let markStarted: (() => void) | null = null;
    const started = requestId
      ? new Promise<void>(resolve => { markStarted = resolve; })
      : Promise.resolve();
    if (requestId) {
      const previousStarted = this.#requestAcceptStarts.get(requestId) ?? Promise.resolve();
      this.#requestAcceptStarts.set(requestId, started);
      if (frame.kind === "request_cancel") {
        // Preserve invocation order (open/data/commit must start first), then
        // preempt a commit blocked in admission without waiting for its full
        // asynchronous completion. The ordered accept below still processes
        // the cancellation only after all earlier same-request frames settle.
        void previousStarted.then(() => {
          const preempt = (this.#engine as SidecarEngine & {
            preemptRequest?: (requestId: string) => void;
          }).preemptRequest;
          preempt?.call(this.#engine, frame.requestId);
        }).catch(error => this.#reportEngineError(error, "Request cancellation preemption failed"));
      }
    }
    const next = previous
      .then(() => {
        markStarted?.();
        return this.#acceptHostFrame(frame, this.#lifecycleAbort.signal);
      })
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
            if (this.#requestAcceptStarts.get(requestId) === started) {
              this.#requestAcceptStarts.delete(requestId);
            }
          }
        },
        () => {
          if (this.#requestAcceptChains.get(requestId) === next) {
            this.#requestAcceptChains.delete(requestId);
            if (this.#requestAcceptStarts.get(requestId) === started) {
              this.#requestAcceptStarts.delete(requestId);
            }
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
      const flightHigh = bootstrapHost
        ? this.#bootstrapProcessedFlightHigh
        : this.#processedFlightHigh;
      if (flightHigh >= 0 && sequence >= flightHigh) {
        if (bootstrapHost) this.#bootstrapProcessedFlightHigh = -1;
        else this.#processedFlightHigh = -1;
        this.#pacer.requestUrgent();
      } else {
        this.#pacer.requestPaced();
      }
    }
  }

  /** Returns true when the failure is engine-wide and must remain un-ACKed. */
  #reportAcceptError(frame: ReliableFrame<HostFrame>, value: unknown): boolean {
    if (this.#lifecycleAbort.signal.aborted) return true;
    const error = value instanceof Error
      ? value
      : new Error("Sidecar frame processing failed");
    if (error.name === "V2ExecutionFenceError") {
      // A fence that became stale between transport acceptance and execution
      // must produce neither an ACK nor an in-band rejection. Draining forces
      // a fresh, explicitly bound launch rather than migrating committed work.
      this.#onAcceptError?.(error, frame);
      return true;
    }
    try {
      if (
        frame.kind === "request_open" ||
        frame.kind === "request_data" ||
        frame.kind === "request_commit" ||
        frame.kind === "request_start"
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
    } catch (cause) {
      // The inbound sequence was already accepted. If its in-band rejection
      // cannot enter the bounded scheduler, explicitly fail the lifecycle so
      // recovery can replay it; never leave a permanent silent ACK gap.
      const enqueueError = cause instanceof Error
        ? cause
        : new Error("Sidecar rejection could not enter the output scheduler");
      try { this.#onAcceptError?.(enqueueError, frame); } catch {}
      return true;
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
      if (frame.kind === "response_ack" && this.#recoveryTerminalAcks.has(frame.requestId)) {
        const pending = (this.#engine as SidecarEngine & {
          hasPendingTerminalAcknowledgement?: (requestId: string) => boolean;
        }).hasPendingTerminalAcknowledgement;
        if (pending && !pending.call(this.#engine, frame.requestId)) {
          this.#recoveryTerminalAcks.delete(frame.requestId);
        }
      }
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
