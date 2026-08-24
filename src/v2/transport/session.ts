import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import type { SseFrame } from "../../relay/sse";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../awareness/channel";
import {
  GP_V2_FLIGHT_MAX_BYTES,
  GP_V2_FLIGHT_MAX_FRAMES,
  GP_V2_REQUEST_MAX_BYTES,
  GP_V2_WINDOW_MAX_BYTES,
  GP_V2_WINDOW_MAX_FRAMES,
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  sha256Hex,
  utf8Bytes,
  utf8Text,
  type AwarenessEnvelope,
  type AwarenessLiveness,
  type AwarenessLane,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type RequestEncoding,
  type SidecarFrame,
} from "../protocol/shared";
import { AckClockedFlight } from "../protocol/flight";
import { WeightedFrameScheduler } from "../protocol/scheduler";
import { ReliableDeliveryTiming } from "../protocol/delivery-timing";
import {
  PublicationPacer,
  type PublicationReason,
} from "../protocol/publication-pacer";
import { HostRequestDiagnosticsObserver } from "../diagnostics/host-observer";
import type { V2EnvelopeDiagnostics, V2TransportDiagnostics } from "../diagnostics/types";
import {
  TranscriptProof,
  validateTerminalProof,
} from "../protocol/transcript";
import {
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../protocol/window";
import {
  V2CommittedError,
  V2PreCommitError,
  V2RunHandle,
} from "./run-handle";

export { computeV2RetransmitDelayMs } from "../protocol/retransmit";

const REQUEST_PART_BYTES = 8 * 1024;

export interface V2TransportSnapshot {
  connected: boolean;
  committed: number;
  active: number;
  queued: number;
  quarantined: number;
  ambiguousQuarantined?: number;
  ambiguityLimit?: number;
  credits: number;
  admissionCredits?: number;
  retransmissions: number;
  awarenessRttMs: number | null;
  /** Age of the newest valid envelope from the bootstrap-bound sidecar instance. */
  sidecarPeerAgeMs?: number | null;
  diagnostics?: { host: V2TransportDiagnostics; sidecar: V2EnvelopeDiagnostics | null };
}

export type V2TransportLivenessErrorCode =
  | "unavailable"
  | "probe_in_flight"
  | "timeout"
  | "publish"
  | "instance_changed";

export class V2TransportLivenessError extends Error {
  readonly name = "V2TransportLivenessError";

  constructor(
    message: string,
    readonly code: V2TransportLivenessErrorCode,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface V2TransportLivenessResult {
  rttMs: number;
  sentAtMs: number;
}

export interface V2TransportSessionOptions {
  binding: ProtocolBinding;
  channel: AwarenessChannel<HostFrame, SidecarFrame>;
  clock?: Clock;
  gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
  maxResponseBytes: number;
  maxBufferedResponseBytes?: number;
  hostCeiling?: number;
  retransmitMs?: number;
  rotationTimeoutMs?: number;
  publicationIntervalMs?: number;
  unadmittedTimeoutMs?: number;
  /** Sidecar instance proven by the bootstrap handshake. Required for liveness probes. */
  expectedSidecarInstanceId?: string;
  /** Primarily injectable for deterministic tests. Production defaults to five seconds. */
  livenessTimeoutMs?: number;
}

interface EventPart {
  offset: number;
  bytes: Uint8Array;
}

interface EventAssembly {
  event: string;
  partCount: number;
  parts: Map<number, EventPart>;
}

interface RequestState {
  requestId: string;
  requestDigest: string;
  frames: readonly HostFrame[];
  handle: V2RunHandle;
  transcript: TranscriptProof;
  resolveRun: (handle: RunHandle) => void;
  rejectRun: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  admitted: boolean;
  published: boolean;
  commitStaged: boolean;
  commitPublished: boolean;
  committed: boolean;
  cancelled: boolean;
  quarantineKind: "cancellation" | "ambiguity" | null;
  responseBytes: number;
  nextEventIndex: number;
  assemblies: Map<number, EventAssembly>;
  completedEvents: Map<number, SseFrame>;
  terminal: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }> | null;
  diagnostics: HostRequestDiagnosticsObserver;
  firstResponseSeen: boolean;
  admissionTimer?: ReturnType<typeof setTimeout>;
}

interface RotationWaiter {
  resolve: () => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

interface LivenessProbe {
  sentAtMs: number;
  startedAtMs: number;
  resolve: (result: V2TransportLivenessResult) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

const SAFE_INSTANCE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const DEFAULT_LIVENESS_TIMEOUT_MS = 5_000;

function abortError(message: string): DOMException {
  return new DOMException(message, "AbortError");
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.byteLength; index++) {
    difference |= left[index]! ^ right[index]!;
  }
  return difference === 0;
}

function concat(parts: readonly Uint8Array[], total: number): Uint8Array {
  const output = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

function validBinding(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: ProtocolBinding,
): boolean {
  return envelope.protocol === "gp-awareness-v2" &&
    envelope.schemaVersion === 1 &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.peerId === "sidecar";
}

type PublicationUrgency = "paced" | "urgent";

function hostFrameUrgency(frame: HostFrame): PublicationUrgency {
  switch (frame.kind) {
    case "request_open":
    case "request_data":
    case "request_commit":
      return "paced";
    default:
      return "urgent";
  }
}

function strongestUrgency(
  left: PublicationUrgency | null,
  right: PublicationUrgency | null,
): PublicationUrgency | null {
  if (left === "urgent" || right === "urgent") return "urgent";
  return left ?? right;
}

export class V2TransportSession {
  readonly #binding: ProtocolBinding;
  readonly #channel: AwarenessChannel<HostFrame, SidecarFrame>;
  readonly #clock: Clock;
  readonly #gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
  readonly #maxResponseBytes: number;
  readonly #maxBufferedResponseBytes: number;
  #hostCeiling: number;
  readonly #rotationTimeoutMs: number;
  readonly #unadmittedTimeoutMs: number;
  readonly #expectedSidecarInstanceId: string | null;
  readonly #livenessTimeoutMs: number;
  readonly #hostInstanceId = `host-${crypto.randomUUID()}`;
  readonly #outbound = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<SidecarFrame>({ epoch: 1 });
  #processedInboundAck = { epoch: 1, sequence: -1 };
  readonly #scheduler = new WeightedFrameScheduler<HostFrame>();
  readonly #delivery: ReliableDeliveryTiming;
  readonly #flight = new AckClockedFlight({ epoch: 1 });
  readonly #pacer: PublicationPacer;
  readonly #requests = new Map<string, RequestState>();
  readonly #rotations = new Map<string, RotationWaiter>();
  readonly #pending: RequestState[] = [];
  readonly #idleWaiters = new Set<() => void>();
  readonly #retiredSidecarInstances = new Set<string>();
  #connected = false;
  #closed = false;
  #draining = false;
  #protocolFailed = false;
  #connecting: Promise<void> | null = null;
  #unsubscribe: (() => void) | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #processing: Promise<void> = Promise.resolve();
  #dataRevision = 0;
  #controlRevision = 0;
  #dataDirty = false;
  #sidecarInstanceId: string | null = null;
  readonly #sidecarRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  readonly #observedPeerRevision: Record<AwarenessLane, number> = {
    data: -1,
    control: -1,
  };
  #lastValidPeerAtMs: number | null = null;
  #lastLivenessSentAtMs = -1;
  #livenessProbe: LivenessProbe | null = null;
  #remoteCredits = 0;
  #creditReservations = 0;
  #quarantined = 0;
  #ambiguousQuarantined = 0;
  #retransmissions = 0;
  #drainResolve: (() => void) | null = null;
  #drainReject: ((error: unknown) => void) | null = null;
  #sidecarDiagnostics: V2EnvelopeDiagnostics | null = null;
  #publications = 0;
  #pacedPublications = 0;
  #urgentPublications = 0;
  #publishedBytes = 0;
  #dataPublications = 0;
  #controlPublications = 0;
  #dataPublishedBytes = 0;
  #controlPublishedBytes = 0;
  #flightCompletions = 0;
  #peakRetransmissionAttempt = 0;
  #unadmittedTimeouts = 0;

  constructor(options: V2TransportSessionOptions) {
    if (options.channel.projectId !== options.binding.projectId) {
      throw new TypeError("Awareness channel belongs to another V2 project");
    }
    if (!Number.isSafeInteger(options.maxResponseBytes) || options.maxResponseBytes < 1) {
      throw new RangeError("maxResponseBytes must be a positive safe integer");
    }
    const maxBufferedResponseBytes = options.maxBufferedResponseBytes ?? Math.min(
      options.maxResponseBytes,
      512 * 1024,
    );
    if (
      !Number.isSafeInteger(maxBufferedResponseBytes) ||
      maxBufferedResponseBytes < 1 ||
      maxBufferedResponseBytes > options.maxResponseBytes
    ) {
      throw new RangeError("maxBufferedResponseBytes must be between 1 and maxResponseBytes");
    }
    const hostCeiling = options.hostCeiling ?? 128;
    if (!Number.isSafeInteger(hostCeiling) || hostCeiling < 0 || hostCeiling > 128) {
      throw new RangeError("hostCeiling must be an integer from 0 through 128");
    }
    const retransmitMs = options.retransmitMs ?? 250;
    if (!Number.isFinite(retransmitMs) || retransmitMs < 1) {
      throw new RangeError("retransmitMs must be positive");
    }
    const rotationTimeoutMs = options.rotationTimeoutMs ?? 30_000;
    if (!Number.isSafeInteger(rotationTimeoutMs) || rotationTimeoutMs < 1) {
      throw new RangeError("rotationTimeoutMs must be a positive safe integer");
    }
    const unadmittedTimeoutMs = options.unadmittedTimeoutMs ?? 1_000;
    if (!Number.isSafeInteger(unadmittedTimeoutMs) || unadmittedTimeoutMs < 1) {
      throw new RangeError("unadmittedTimeoutMs must be a positive safe integer");
    }
    if (
      options.expectedSidecarInstanceId !== undefined &&
      !SAFE_INSTANCE_ID.test(options.expectedSidecarInstanceId)
    ) {
      throw new TypeError("expectedSidecarInstanceId is invalid");
    }
    const livenessTimeoutMs = options.livenessTimeoutMs ?? DEFAULT_LIVENESS_TIMEOUT_MS;
    if (!Number.isSafeInteger(livenessTimeoutMs) || livenessTimeoutMs < 1) {
      throw new RangeError("livenessTimeoutMs must be a positive safe integer");
    }
    this.#binding = options.binding;
    this.#channel = options.channel;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#gzip = options.gzip;
    this.#maxResponseBytes = options.maxResponseBytes;
    this.#maxBufferedResponseBytes = maxBufferedResponseBytes;
    this.#hostCeiling = hostCeiling;
    this.#rotationTimeoutMs = rotationTimeoutMs;
    this.#unadmittedTimeoutMs = unadmittedTimeoutMs;
    this.#expectedSidecarInstanceId = options.expectedSidecarInstanceId ?? null;
    this.#livenessTimeoutMs = livenessTimeoutMs;
    this.#delivery = new ReliableDeliveryTiming({
      epoch: 1,
      minimumMs: retransmitMs,
    });
    this.#pacer = new PublicationPacer({
      intervalMs: options.publicationIntervalMs,
      publish: reason => this.#publish(reason),
    });
  }

  connect(signal?: AbortSignal): Promise<void> {
    if (this.#closed) return Promise.reject(new Error("V2 transport session is closed"));
    if (this.#protocolFailed) {
      return Promise.reject(new Error("V2 transport session failed its protocol"));
    }
    if (this.#connected) return Promise.resolve();
    if (this.#connecting) return this.#connecting;
    const connecting = (async () => {
      if (signal?.aborted) throw abortError("V2 transport connection aborted");
      this.#unsubscribe ??= this.#channel.subscribe(this.#onAwarenessUpdate);
      await this.#channel.connect(signal);
      if (this.#closed) throw new Error("V2 transport session closed while connecting");
      this.#connected = true;
      this.#pacer.flushNow("initial");
    })().finally(() => {
      if (this.#connecting === connecting) this.#connecting = null;
    });
    this.#connecting = connecting;
    return connecting;
  }

  /**
   * Proves that the exact sidecar instance accepted during bootstrap can still
   * execute its engine loop. The ping lives in the control Awareness state and
   * never enters the reliable request/response window.
   */
  probeLiveness(signal?: AbortSignal): Promise<V2TransportLivenessResult> {
    if (signal?.aborted) {
      return Promise.reject(abortError("V2 sidecar liveness probe aborted"));
    }
    if (!this.#connected || this.#closed || this.#draining) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 transport is unavailable for a liveness probe",
        "unavailable",
      ));
    }
    if (!this.#expectedSidecarInstanceId) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 transport has no bootstrap-bound sidecar instance",
        "unavailable",
      ));
    }
    if (
      (this.#sidecarInstanceId &&
        this.#sidecarInstanceId !== this.#expectedSidecarInstanceId) ||
      this.#retiredSidecarInstances.has(this.#expectedSidecarInstanceId)
    ) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 sidecar instance changed after bootstrap",
        "instance_changed",
      ));
    }
    if (this.#livenessProbe) {
      return Promise.reject(new V2TransportLivenessError(
        "A V2 sidecar liveness probe is already in flight",
        "probe_in_flight",
      ));
    }

    const startedAtMs = this.#clock.now();
    const clockSentAtMs = Math.floor(startedAtMs);
    const sentAtMs = Math.max(clockSentAtMs, this.#lastLivenessSentAtMs + 1);
    if (!Number.isSafeInteger(sentAtMs) || sentAtMs < 0) {
      return Promise.reject(new V2TransportLivenessError(
        "V2 liveness clock is outside the supported range",
        "unavailable",
      ));
    }
    this.#lastLivenessSentAtMs = sentAtMs;

    let resolve!: (result: V2TransportLivenessResult) => void;
    let reject!: (error: unknown) => void;
    const pending = new Promise<V2TransportLivenessResult>((resolveValue, rejectValue) => {
      resolve = resolveValue;
      reject = rejectValue;
    });
    const probe: LivenessProbe = {
      sentAtMs,
      startedAtMs,
      resolve,
      reject,
      signal,
    };
    probe.onAbort = () => {
      this.#rejectLivenessProbe(
        probe,
        abortError("V2 sidecar liveness probe aborted"),
      );
    };
    signal?.addEventListener("abort", probe.onAbort, { once: true });
    probe.timer = setTimeout(() => {
      this.#rejectLivenessProbe(
        probe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe timed out",
          "timeout",
        ),
      );
    }, this.#livenessTimeoutMs);
    probe.timer.unref?.();
    this.#livenessProbe = probe;

    try {
      this.#publishLane("control", [], "urgent");
    } catch (cause) {
      this.#rejectLivenessProbe(
        probe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe could not be published",
          "publish",
          { cause },
        ),
      );
    }
    return pending;
  }

  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<RunHandle> {
    if (signal?.aborted) throw abortError("V2 request aborted before commitment");
    if (!this.#connected || this.#closed) {
      throw new V2PreCommitError("V2 transport is not connected", "not_connected");
    }
    if (this.#draining) {
      throw new V2PreCommitError("V2 transport is draining", "draining");
    }
    const requestId = `req-${crypto.randomUUID()}`;
    const diagnostics = new HostRequestDiagnosticsObserver(() => this.#clock.now());
    const prepared = await this.#prepareRequest(requestId, body);
    diagnostics.prepared();
    if (signal?.aborted) throw abortError("V2 request aborted before commitment");
    if (this.#draining || this.#closed) {
      throw new V2PreCommitError("V2 transport stopped during request preparation", "draining");
    }
    const handle = new V2RunHandle({
      clock: this.#clock,
      maxBufferedBytes: this.#maxBufferedResponseBytes,
    });
    handle.stats.diagnostics = diagnostics.value();
    let resolveRun!: (handle: RunHandle) => void;
    let rejectRun!: (error: unknown) => void;
    const committed = new Promise<RunHandle>((resolve, reject) => {
      resolveRun = resolve;
      rejectRun = reject;
    });
    void committed.catch(() => undefined);
    const state: RequestState = {
      requestId,
      requestDigest: prepared.requestDigest,
      frames: prepared.frames,
      handle,
      transcript: new TranscriptProof(requestId, prepared.requestDigest, {
        maxBytes: this.#maxResponseBytes,
      }),
      resolveRun,
      rejectRun,
      signal,
      admitted: false,
      published: false,
      commitStaged: false,
      commitPublished: false,
      committed: false,
      cancelled: false,
      quarantineKind: null,
      responseBytes: 0,
      nextEventIndex: 0,
      assemblies: new Map(),
      completedEvents: new Map(),
      terminal: null,
      diagnostics,
      firstResponseSeen: false,
    };
    state.onAbort = () => this.#abortRequest(state);
    signal?.addEventListener("abort", state.onAbort, { once: true });
    this.#requests.set(requestId, state);
    this.#pending.push(state);
    state.admissionTimer = setTimeout(() => {
      if (!this.#requests.has(state.requestId) || state.admitted) return;
      this.#unadmittedTimeouts++;
      this.#failPreCommit(state, new V2PreCommitError(
        "Sandbox capacity changed before admission",
        "capacity_changed",
      ));
    }, this.#unadmittedTimeoutMs);
    state.admissionTimer.unref?.();
    this.#admitPending();
    this.#requestPublication(this.#pump());
    return committed;
  }

  async drain(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#closed) return;
    this.#draining = true;
    for (const state of [...this.#pending]) {
      if (state.admitted) continue;
      this.#failPreCommit(
        state,
        new V2PreCommitError("V2 transport drained before admission", "draining"),
      );
    }
    while (this.#requests.size > 0) {
      if (signal?.aborted) throw abortError("V2 transport drain aborted");
      await new Promise<void>(resolve => this.#idleWaiters.add(resolve));
    }
    const drained = new Promise<void>((resolve, reject) => {
      this.#drainResolve = resolve;
      this.#drainReject = reject;
    });
    this.#scheduler.enqueue("control", "control", {
      kind: "drain",
      reason: reason.replace(/[\r\n\0]/g, " ").trim().slice(0, 256) || "shutdown",
    });
    this.#requestPublication(this.#pump());
    if (!signal) await drained;
    else {
      let onAbort!: () => void;
      const aborted = new Promise<never>((_resolve, reject) => {
        onAbort = () => reject(abortError("V2 transport drain aborted"));
        signal.addEventListener("abort", onAbort, { once: true });
      });
      try {
        await Promise.race([drained, aborted]);
      } finally {
        signal.removeEventListener("abort", onAbort);
      }
    }
    this.#channel.clear();
    await this.close();
  }

  rotateCredential(
    rotationId: string,
    sealedJson: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.#connected || this.#closed || this.#draining) {
      return Promise.reject(new Error("V2 transport is unavailable for credential rotation"));
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(rotationId)) {
      return Promise.reject(new TypeError("rotationId is invalid"));
    }
    if (typeof sealedJson !== "string" || sealedJson.length < 1 || sealedJson.length > 10_000) {
      return Promise.reject(new RangeError("sealed credential rotation is invalid"));
    }
    if (signal?.aborted) {
      return Promise.reject(abortError("Credential rotation aborted"));
    }
    if (this.#rotations.has(rotationId)) {
      return Promise.reject(new Error(`Credential rotation is already pending: ${rotationId}`));
    }
    const pending = new Promise<void>((resolve, reject) => {
      const waiter: RotationWaiter = { resolve, reject, signal };
      waiter.onAbort = () => {
        if (this.#rotations.get(rotationId) !== waiter) return;
        this.#rotations.delete(rotationId);
        if (waiter.timer) clearTimeout(waiter.timer);
        reject(abortError("Credential rotation aborted"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#rotations.set(rotationId, waiter);
      waiter.timer = setTimeout(() => {
        if (this.#rotations.get(rotationId) !== waiter) return;
        this.#rotations.delete(rotationId);
        signal?.removeEventListener("abort", waiter.onAbort!);
        reject(new Error(`Credential rotation ACK timed out: ${rotationId}`));
      }, this.#rotationTimeoutMs);
    });
    void pending.catch(() => undefined);
    this.#scheduler.enqueue("control", `rotation:${rotationId}`, {
      kind: "credential_rotate",
      rotationId,
      sealedJson,
    });
    this.#requestPublication(this.#pump());
    return pending;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#connected = false;
    this.#draining = true;
    if (this.#livenessProbe) {
      this.#rejectLivenessProbe(
        this.#livenessProbe,
        new V2TransportLivenessError(
          "V2 transport closed during its liveness probe",
          "unavailable",
        ),
        false,
      );
    }
    this.#pacer.close();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    const error = new V2PreCommitError("V2 transport session closed", "closed");
    for (const state of [...this.#requests.values()]) {
      if (state.committed || state.commitPublished) {
        this.#failCommitted(state, new V2CommittedError(
          "V2 transport closed after request commitment may have been published",
          "closed",
          true,
        ));
      } else {
        this.#failPreCommit(state, error);
      }
    }
    for (const [rotationId, waiter] of this.#rotations) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(error);
      this.#rotations.delete(rotationId);
    }
    this.#drainReject?.(error);
    this.#drainResolve = null;
    this.#drainReject = null;
    this.#channel.clear();
    await this.#channel.close();
    await this.#processing.catch(() => undefined);
  }

  snapshot(): V2TransportSnapshot {
    let committed = 0;
    for (const state of this.#requests.values()) {
      if (state.committed) committed++;
    }
    return {
      connected: this.#connected && !this.#closed,
      committed,
      active: this.#requests.size - this.#pending.filter(state => !state.admitted).length,
      queued: this.#pending.filter(state => !state.admitted).length,
      quarantined: this.#quarantined,
      ambiguousQuarantined: this.#ambiguousQuarantined,
      ambiguityLimit: Math.max(1, Math.min(4, this.#hostCeiling)),
      credits: Math.max(0, this.#remoteCredits - this.#creditReservations),
      admissionCredits: this.#availableCredits(),
      retransmissions: this.#retransmissions,
      awarenessRttMs: this.#delivery.roundTripEwmaMs,
      sidecarPeerAgeMs: this.#lastValidPeerAtMs === null
        ? null
        : Math.max(0, this.#clock.now() - this.#lastValidPeerAtMs),
      diagnostics: { host: this.#hostTransportDiagnostics(), sidecar: this.#sidecarDiagnostics },
    };
  }

  setHostCeiling(value: number): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > 128) {
      throw new RangeError("host ceiling must be an integer from 0 through 128");
    }
    if (value === this.#hostCeiling) return;
    this.#hostCeiling = value;
    if (this.#connected && !this.#closed) this.#pacer.requestUrgent();
  }

  #observeSidecarLiveness(envelope: AwarenessEnvelope<SidecarFrame>): void {
    if (this.#closed || this.#protocolFailed || !validBinding(envelope, this.#binding)) return;
    const lane: AwarenessLane = envelope.lane ?? "data";
    if (lane === "control" && envelope.frames.length > 0) return;
    const expectedInstanceId = this.#expectedSidecarInstanceId;
    if (!expectedInstanceId) return;
    if (envelope.instanceId !== expectedInstanceId) {
      if (
        this.#sidecarInstanceId &&
        lane === "data" &&
        envelope.epoch <= this.#inbound.ack().epoch
      ) return;
      const probe = this.#livenessProbe;
      if (probe && !this.#retiredSidecarInstances.has(envelope.instanceId)) {
        this.#rejectLivenessProbe(
          probe,
          new V2TransportLivenessError(
            "V2 sidecar instance changed during its liveness probe",
            "instance_changed",
          ),
        );
      }
      return;
    }
    if (this.#retiredSidecarInstances.has(envelope.instanceId)) return;
    if (envelope.revision <= this.#observedPeerRevision[lane]) return;
    this.#observedPeerRevision[lane] = envelope.revision;
    this.#lastValidPeerAtMs = this.#clock.now();
    if (lane !== "control") return;

    const pong = envelope.liveness;
    const probe = this.#livenessProbe;
    if (!probe || pong?.kind !== "pong" || pong.sentAtMs !== probe.sentAtMs) return;
    this.#resolveLivenessProbe(probe, {
      rttMs: Math.max(0, this.#clock.now() - probe.startedAtMs),
      sentAtMs: probe.sentAtMs,
    });
  }

  #resolveLivenessProbe(
    probe: LivenessProbe,
    result: V2TransportLivenessResult,
  ): void {
    if (!this.#finishLivenessProbe(probe, true)) return;
    probe.resolve(result);
  }

  #rejectLivenessProbe(
    probe: LivenessProbe,
    error: unknown,
    clearState = true,
  ): void {
    if (!this.#finishLivenessProbe(probe, clearState)) return;
    probe.reject(error);
  }

  #finishLivenessProbe(probe: LivenessProbe, clearState: boolean): boolean {
    if (this.#livenessProbe !== probe) return false;
    this.#livenessProbe = null;
    if (probe.timer) clearTimeout(probe.timer);
    probe.signal?.removeEventListener("abort", probe.onAbort!);
    if (clearState && this.#connected && !this.#closed) {
      try {
        // Replace the persistent control state so a late pong cannot heal a
        // later probe and reconnecting peers do not replay a stale ping.
        this.#publishLane("control", [], "urgent");
      } catch {
        // The probe result is already known. Cleanup publication is best effort.
      }
    }
    return true;
  }

  readonly #onAwarenessUpdate = (
    update: AwarenessPeerUpdate<SidecarFrame>,
  ): void => {
    // Liveness must not queue behind a response frame waiting for client-side
    // buffer space. Only the reliable data path remains serialized below.
    this.#observeSidecarLiveness(update.envelope);
    this.#processing = this.#processing
      .then(() => this.#processEnvelope(update.envelope))
      .catch(error => this.#failProtocol(error));
  };

  async #processEnvelope(
    envelope: AwarenessEnvelope<SidecarFrame>,
  ): Promise<void> {
    if (this.#closed || this.#protocolFailed) return;
    if (!validBinding(envelope, this.#binding)) {
      throw new Error("Sidecar Awareness binding is invalid");
    }
    const lane: AwarenessLane = envelope.lane ?? "data";
    if (
      this.#expectedSidecarInstanceId &&
      envelope.instanceId !== this.#expectedSidecarInstanceId
    ) {
      // An old data-state replay cannot take over the transport and may arrive
      // after the bootstrap-bound peer is already live. Ignore that stale
      // epoch; a foreign current/new epoch or any control state fails closed.
      if (
        this.#sidecarInstanceId &&
        lane === "data" &&
        envelope.epoch <= this.#inbound.ack().epoch
      ) return;
      this.#retiredSidecarInstances.add(envelope.instanceId);
      throw new V2TransportLivenessError(
        "Sidecar instance does not match the bootstrap-bound instance",
        "instance_changed",
      );
    }
    const legacy = envelope.lane === undefined;
    if (lane === "control" && envelope.frames.length > 0) {
      throw new Error("Sidecar control Awareness state contains reliable frames");
    }
    let replaced = false;
    if (this.#sidecarInstanceId && envelope.instanceId !== this.#sidecarInstanceId) {
      if (this.#retiredSidecarInstances.has(envelope.instanceId)) return;
      if (lane === "data" && envelope.epoch <= this.#inbound.ack().epoch) return;
      this.#retiredSidecarInstances.add(this.#sidecarInstanceId);
      const message = "Sidecar instance changed while transport state was active";
      for (const state of [...this.#requests.values()]) {
        if (state.committed || state.commitPublished) {
          this.#failCommitted(state, new V2CommittedError(message, "instance_changed", true));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(message, "instance_changed"));
        }
      }
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#flight.reset(nextEpoch);
      this.#channel.clear("data");
      this.#dataDirty = false;
      this.#sidecarRevision.data = -1;
      this.#sidecarRevision.control = -1;
      replaced = true;
    }
    if (envelope.revision <= this.#sidecarRevision[lane]) return;
    this.#sidecarInstanceId = envelope.instanceId;
    this.#sidecarRevision[lane] = envelope.revision;
    if (lane === "control" || legacy) {
      this.#sidecarDiagnostics = envelope.diagnostics ?? null;
      this.#remoteCredits = Math.max(0, Math.min(128, envelope.credits));
    }

    this.#outbound.acknowledge(envelope.ack);
    const flightCompleted = this.#flight.acknowledged(envelope.ack);
    if (flightCompleted) this.#flightCompletions++;
    const now = this.#clock.now();
    const acknowledgementAdvanced = this.#delivery.acknowledged(
      envelope.ack,
      this.#outbound.snapshot().frames.length > 0,
      now,
    );
    if (acknowledgementAdvanced) {
      this.#armRetransmission();
    }
    let delivered: readonly ReliableFrame<SidecarFrame>[] = [];
    if (lane === "data") {
      const currentEpoch = this.#inbound.ack().epoch;
      if (envelope.epoch > currentEpoch) {
        this.#inbound.reset(envelope.epoch);
        this.#processedInboundAck = { epoch: envelope.epoch, sequence: -1 };
      }
      delivered = this.#inbound.accept(envelope.frames);
      for (const frame of delivered) {
        await this.#processFrame(frame);
        this.#processedInboundAck = { epoch: frame.epoch, sequence: frame.sequence };
      }
    }
    this.#admitPending();
    const urgency = this.#pump();
    if (flightCompleted && urgency === null) this.#channel.clear("data");
    this.#requestPublication(
      replaced || delivered.length > 0 ? "urgent" : urgency,
    );
  }

  async #processFrame(frame: ReliableFrame<SidecarFrame>): Promise<void> {
    switch (frame.kind) {
      case "request_ack": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (frame.requestDigest !== state.requestDigest) {
          if (state.committed || state.commitPublished) {
            this.#failCommitted(state, new V2CommittedError(
              "Committed ACK digest does not match the request",
              "ack_digest",
              true,
            ));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(
              "Committed ACK digest does not match the request",
              "ack_digest",
            ));
          }
          return;
        }
        if (state.committed) return;
        state.committed = true;
        state.diagnostics.committed(frame.diagnostics);
        this.#creditReservations = Math.max(0, this.#creditReservations - 1);
        state.resolveRun(state.handle);
        return;
      }
      case "request_reject": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (state.committed) {
          this.#failCommitted(state, new V2CommittedError(
            frame.message,
            frame.code,
            true,
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
        }
        return;
      }
      case "response_event": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (!state.committed) {
          const message = "Sidecar emitted response data before commitment";
          if (state.commitPublished) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (!state.firstResponseSeen) { state.firstResponseSeen = true; state.diagnostics.firstFrame(frame.diagnostics); }
        await this.#acceptResponsePart(state, frame);
        return;
      }
      case "response_batch": {
        const state=this.#requests.get(frame.requestId);if(!state)return;
        if(!state.committed){const message="Sidecar emitted response data before commitment";if(state.commitPublished)this.#failCommitted(state,new V2CommittedError(message,"protocol",true));else this.#failPreCommit(state,new V2PreCommitError(message,"protocol"));return;}
        for(let offset=0;offset<frame.items.length;offset++){
          const eventIndex=frame.firstEventIndex+offset;const item=frame.items[offset]!;let bytes:Uint8Array;let data:string;
          try{bytes=base64UrlDecode(item.dataBase64Url);data=utf8Text(bytes);}catch{this.#failCommitted(state,new V2CommittedError("Response batch encoding is invalid","response_encoding",true));return;}
          state.responseBytes+=bytes.byteLength;if(state.responseBytes>this.#maxResponseBytes){this.#failCommitted(state,new V2CommittedError("Response exceeds the configured byte limit","response_too_large",true));return;}
          if(eventIndex<state.nextEventIndex||state.completedEvents.has(eventIndex)){this.#failCommitted(state,new V2CommittedError("Response batch event indexes conflict","response_conflict",true));return;}
          state.completedEvents.set(eventIndex,{event:item.event,data});
        }
        await this.#drainCompletedEvents(state);return;
      }
      case "response_terminal": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (!state.committed) {
          const message = "Sidecar emitted a terminal before commitment";
          if (state.commitPublished) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (state.terminal && state.terminal.transcriptSha256 !== frame.transcriptSha256) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted conflicting terminal proofs",
            "terminal_conflict",
            true,
          ));
          return;
        }
        state.terminal = frame;
        await this.#tryFinalize(state);
        return;
      }
      case "response_error": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (state.committed) {
          this.#failCommitted(state, new V2CommittedError(
            frame.message,
            frame.code,
            frame.ambiguous,
            frame.rateLimit,
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
        }
        return;
      }
      case "request_cancelled": {
        const state = this.#requests.get(frame.requestId);
        if (!state?.cancelled) return;
        const releaseQuarantine = state.quarantineKind === "cancellation";
        this.#removeState(state);
        if (releaseQuarantine) {
          this.#quarantined = Math.max(0, this.#quarantined - 1);
          state.quarantineKind = null;
        }
        return;
      }
      case "drained":
        if (frame.active === 0 && frame.queued === 0) {
          this.#drainResolve?.();
          this.#drainResolve = null;
          this.#drainReject = null;
        }
        return;
      case "credential_ack": {
        const waiter = this.#rotations.get(frame.rotationId);
        if (!waiter) return;
        this.#rotations.delete(frame.rotationId);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        waiter.resolve();
        return;
      }
      case "pong":
        return;
    }
  }

  async #acceptResponsePart(
    state: RequestState,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
  ): Promise<void> {
    let bytes: Uint8Array;
    try {
      bytes = base64UrlDecode(frame.dataBase64Url);
    } catch {
      this.#failCommitted(state, new V2CommittedError(
        "Response event part is not valid base64url",
        "response_encoding",
        true,
      ));
      return;
    }
    let assembly = state.assemblies.get(frame.eventIndex);
    if (!assembly) {
      assembly = {
        event: frame.event,
        partCount: frame.partCount,
        parts: new Map(),
      };
      state.assemblies.set(frame.eventIndex, assembly);
    }
    if (assembly.event !== frame.event || assembly.partCount !== frame.partCount) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event parts conflict",
        "response_conflict",
        true,
      ));
      return;
    }
    const existing = assembly.parts.get(frame.partIndex);
    if (existing) {
      if (existing.offset !== frame.offset || !bytesEqual(existing.bytes, bytes)) {
        this.#failCommitted(state, new V2CommittedError(
          "Response event contains a conflicting duplicate part",
          "response_conflict",
          true,
        ));
      }
      return;
    }
    state.responseBytes += bytes.byteLength;
    if (state.responseBytes > this.#maxResponseBytes) {
      this.#failCommitted(state, new V2CommittedError(
        "Response exceeds the configured byte limit",
        "response_too_large",
        true,
      ));
      return;
    }
    assembly.parts.set(frame.partIndex, { offset: frame.offset, bytes });
    if (assembly.parts.size !== assembly.partCount) return;

    const parts: Uint8Array[] = [];
    let expectedOffset = 0;
    for (let index = 0; index < assembly.partCount; index++) {
      const part = assembly.parts.get(index);
      if (!part || part.offset !== expectedOffset) {
        this.#failCommitted(state, new V2CommittedError(
          "Response event has a byte gap",
          "response_gap",
          true,
        ));
        return;
      }
      parts.push(part.bytes);
      expectedOffset += part.bytes.byteLength;
    }
    let data: string;
    try {
      data = utf8Text(concat(parts, expectedOffset));
    } catch {
      this.#failCommitted(state, new V2CommittedError(
        "Response event is not valid UTF-8",
        "response_encoding",
        true,
      ));
      return;
    }
    state.assemblies.delete(frame.eventIndex);
    state.completedEvents.set(frame.eventIndex, { event: frame.event, data });
    await this.#drainCompletedEvents(state);
  }

  async #drainCompletedEvents(state: RequestState): Promise<void> {
    for (;;) {
      const event = state.completedEvents.get(state.nextEventIndex);
      if (!event) break;
      state.completedEvents.delete(state.nextEventIndex);
      state.transcript.append(event);
      state.nextEventIndex++;
      if (!state.cancelled) { const started=this.#clock.now(); await state.handle.acceptEvent(event); state.diagnostics.assemblyDelivered(this.#clock.now()-started); }
    }
    await this.#tryFinalize(state);
  }

  async #tryFinalize(state: RequestState): Promise<void> {
    const terminal = state.terminal;
    if (!terminal) return;
    const snapshot = state.transcript.snapshot();
    if (snapshot.eventCount > terminal.eventCount) {
      this.#failCommitted(state, new V2CommittedError(
        "Response contains more events than its terminal proof",
        "terminal_count",
        true,
      ));
      return;
    }
    if (snapshot.eventCount < terminal.eventCount) return;
    if (state.assemblies.size > 0 || state.completedEvents.size > 0) return;
    let final: Record<string, unknown>;
    try {
      final = await validateTerminalProof(terminal, snapshot);
    } catch (cause) {
      this.#failCommitted(state, new V2CommittedError(
        cause instanceof Error ? cause.message : "Terminal proof validation failed",
        "terminal_proof",
        true,
      ));
      return;
    }
    const wasCancelled = state.cancelled;
    const wasQuarantined = state.quarantineKind === "cancellation";
    this.#removeState(state);
    if (!wasCancelled) state.handle.settle(final, terminal.rateLimit);
    if (wasQuarantined) {
      this.#quarantined = Math.max(0, this.#quarantined - 1);
      state.quarantineKind = null;
    }
    this.#scheduler.enqueue("control", state.requestId, {
      kind: "response_ack",
      requestId: state.requestId,
      terminalDigest: terminal.transcriptSha256,
    });
    this.#requestPublication(this.#pump());
  }

  async #prepareRequest(
    requestId: string,
    body: Record<string, unknown>,
  ): Promise<{ requestDigest: string; frames: readonly HostFrame[] }> {
    const decoded = utf8Bytes(canonicalJson(body));
    if (decoded.byteLength < 1 || decoded.byteLength > GP_V2_REQUEST_MAX_BYTES) {
      throw new V2PreCommitError("V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const requestDigest = await sha256Hex(decoded);
    let encoded = decoded;
    let encoding: RequestEncoding = "identity";
    if (this.#gzip) {
      const compressed = await this.#gzip(decoded.slice());
      const identityParts = Math.ceil(decoded.byteLength / REQUEST_PART_BYTES);
      const compressedParts = compressed.byteLength > 0
        ? Math.ceil(compressed.byteLength / REQUEST_PART_BYTES)
        : Number.POSITIVE_INFINITY;
      if (compressedParts < identityParts) {
        encoded = compressed;
        encoding = "gzip+base64";
      }
    }
    if (encoded.byteLength < 1 || encoded.byteLength > GP_V2_REQUEST_MAX_BYTES) {
      throw new V2PreCommitError("Encoded V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const partCount = Math.ceil(encoded.byteLength / REQUEST_PART_BYTES);
    const frames: HostFrame[] = [{
      kind: "request_open",
      requestId,
      requestDigest,
      decodedBytes: decoded.byteLength,
      encodedBytes: encoded.byteLength,
      encoding,
      partCount,
    }];
    for (let offset = 0, partIndex = 0; offset < encoded.byteLength; offset += REQUEST_PART_BYTES, partIndex++) {
      frames.push({
        kind: "request_data",
        requestId,
        partIndex,
        offset,
        dataBase64Url: base64UrlEncode(encoded.subarray(offset, offset + REQUEST_PART_BYTES)),
      });
    }
    frames.push({ kind: "request_commit", requestId });
    return { requestDigest, frames };
  }

  #admitPending(): void {
    while (this.#availableCredits() > 0) {
      const state = this.#pending.find(candidate => !candidate.admitted);
      if (!state) return;
      state.admitted = true;
      if (state.admissionTimer) clearTimeout(state.admissionTimer);
      state.admissionTimer = undefined;
      this.#creditReservations++;
      for (const frame of state.frames) {
        // Request frames share one priority queue so commit can never overtake data.
        this.#scheduler.enqueue("data", state.requestId, frame);
      }
    }
  }

  #availableCredits(): number {
    return Math.max(0, Math.min(
      this.#remoteCredits - this.#creditReservations,
      this.#hostCeiling - this.#creditReservations - this.#ambiguousQuarantined,
      4 - this.#creditReservations - this.#ambiguousQuarantined,
    ));
  }

  #pump(): PublicationUrgency | null {
    if (!this.#connected || this.#closed) return null;
    if (this.#flight.active) return null;
    let urgency: PublicationUrgency | null = null;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const frameBudget = Math.min(
        GP_V2_FLIGHT_MAX_FRAMES - snapshot.frames.length,
        GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length,
      );
      const byteBudget = Math.min(
        GP_V2_FLIGHT_MAX_BYTES - snapshot.encodedBytes,
        GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes,
      );
      if (frameBudget < 1 || byteBudget < 512) break;
      const next = this.#scheduler.take(1, Math.max(1, byteBudget - 256));
      if (next.length === 0) break;
      const reliable = this.#outbound.push(next[0]!);
      this.#dataDirty = true;
      urgency = strongestUrgency(urgency, hostFrameUrgency(reliable));
      if ("requestId" in reliable && typeof reliable.requestId === "string") {
        const state = this.#requests.get(reliable.requestId);
        if (state) state.published = true;
        if (state && reliable.kind === "request_commit") state.commitStaged = true;
        if (state) state.diagnostics.published();
      }
    }
    return urgency;
  }

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
  }

  #publish(reason: PublicationReason): void {
    if (!this.#connected || this.#closed) return;
    try {
      const outbound = this.#outbound.snapshot();
      const publishData = reason === "retransmission" || this.#dataDirty;
      if (publishData) {
        this.#flight.published(outbound.frames);
        this.#delivery.published(outbound.frames, this.#clock.now());
        this.#publishLane("data", outbound.frames, reason);
        this.#dataDirty = false;
      }
      if (reason === "initial" || reason === "urgent" || !publishData) {
        this.#publishLane("control", [], reason);
      }
      const diagnostics = this.#hostTransportDiagnostics();
      for (const state of this.#requests.values()) state.diagnostics.peak(diagnostics);
      this.#armRetransmission();
    } catch (cause) {
      this.#failProtocol(cause);
    }
  }

  #publishLane(
    lane: AwarenessLane,
    frames: readonly ReliableFrame<HostFrame>[],
    reason: PublicationReason,
  ): void {
    const outbound = this.#outbound.snapshot();
    const committed = [...this.#requests.values()].filter(state => state.committed).length;
    const envelope: AwarenessEnvelope<HostFrame> = {
      protocol: "gp-awareness-v2",
      schemaVersion: 1,
      schemaHash: this.#binding.schemaHash,
      buildHash: this.#binding.buildHash,
      projectId: this.#binding.projectId,
      sessionId: this.#binding.sessionId,
      peerId: "host",
      instanceId: this.#hostInstanceId,
      leaderGeneration: this.#binding.leaderGeneration,
      epoch: outbound.epoch,
      revision: lane === "data" ? this.#dataRevision++ : this.#controlRevision++,
      ack: this.#processedInboundAck,
      credits: this.#hostCeiling,
      active: committed,
      queued: this.#pending.filter(state => !state.admitted).length,
      draining: this.#draining,
      frames,
      lane,
      ...(lane === "control" && this.#livenessProbe
        ? { liveness: {
            kind: "ping",
            sentAtMs: this.#livenessProbe.sentAtMs,
          } satisfies AwarenessLiveness }
        : {}),
    };
    this.#publications++;
    if (reason === "paced") this.#pacedPublications++;
    else if (reason === "urgent") this.#urgentPublications++;
    const encodedBytes = utf8Bytes(JSON.stringify(envelope)).byteLength;
    this.#publishedBytes += encodedBytes;
    if (lane === "data") {
      this.#dataPublications++;
      this.#dataPublishedBytes += encodedBytes;
    } else {
      this.#controlPublications++;
      this.#controlPublishedBytes += encodedBytes;
    }
    if (lane === "data") {
      // A transport may throw after handing bytes to the underlying socket.
      // Fence the request before the call so publish failure is never treated
      // as safely retryable once request_commit may have escaped.
      for (const frame of frames) {
        if (frame.kind !== "request_commit") continue;
        const state = this.#requests.get(frame.requestId);
        if (state) state.commitPublished = true;
      }
    }
    this.#channel.publish(envelope, lane);
  }

  #hostTransportDiagnostics(): V2TransportDiagnostics {
    const window = this.#outbound.snapshot();
    const scheduler = this.#scheduler.snapshot();
    return {
      windowFrames: window.frames.length,
      windowBytes: window.encodedBytes,
      schedulerFrames: scheduler.queuedFrames,
      schedulerBytes: scheduler.queuedEncodedBytes,
      schedulerStreams: scheduler.streams,
      oldestQueuedFrameMs: scheduler.oldestQueuedFrameMs,
      ackProgressAgeMs: this.#delivery.ackProgressAgeMs(this.#clock.now()),
      retransmissionAttempt: this.#delivery.attempt,
      peakRetransmissionAttempt: this.#peakRetransmissionAttempt,
      retransmissionsTotal: this.#retransmissions,
      publicationsTotal: this.#publications,
      publishedBytesTotal: this.#publishedBytes,
      admittedUncommitted: [...this.#requests.values()]
        .filter(value => value.admitted && !value.committed).length,
      roundTripEwmaMs: this.#delivery.roundTripEwmaMs,
      pacedPublicationsTotal: this.#pacedPublications,
      urgentPublicationsTotal: this.#urgentPublications,
      logicalEventsTotal: 0,
      responseBatchFramesTotal: 0,
      batchedEventsTotal: 0,
      dataPublicationsTotal: this.#dataPublications,
      controlPublicationsTotal: this.#controlPublications,
      dataPublishedBytesTotal: this.#dataPublishedBytes,
      controlPublishedBytesTotal: this.#controlPublishedBytes,
      flightCompletionsTotal: this.#flightCompletions,
      adaptiveCapacity: 0,
      capacityIncreasesTotal: 0,
      capacityReductionsTotal: 0,
      unadmittedTimeoutsTotal: this.#unadmittedTimeouts,
    };
  }

  #armRetransmission(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (
      !this.#connected ||
      this.#closed ||
      this.#outbound.snapshot().frames.length === 0
    ) return;
    const revision = this.#dataRevision + this.#controlRevision;
    const jitterSeed = ((revision * 2_654_435_761) >>> 0) / 0xffff_ffff;
    const delay = this.#delivery.nextDelayMs(this.#clock.now(), jitterSeed);
    if (delay === null) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (!this.#connected || this.#closed) return;
      if (this.#outbound.snapshot().frames.length === 0) return;
      this.#delivery.retransmitted(this.#clock.now());
      this.#retransmissions++;
      this.#peakRetransmissionAttempt = Math.max(
        this.#peakRetransmissionAttempt,
        this.#delivery.attempt,
      );
      this.#pacer.flushNow("retransmission");
    }, delay);
  }

  #abortRequest(state: RequestState): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#scheduler.removeStream(state.requestId);
    if (state.committed || state.commitPublished || state.commitStaged) {
      const acknowledged = state.committed;
      // A staged commit cannot be retracted from the cumulative window. The
      // already-scheduled publication may still send it after this callback.
      if (state.commitStaged) state.commitPublished = true;
      state.cancelled = true;
      if (!state.quarantineKind) {
        state.quarantineKind = "cancellation";
        this.#quarantined++;
      }
      const error = new V2CommittedError(
        "V2 request was cancelled after commitment",
        "cancelled",
        true,
      );
      if (!acknowledged) state.rejectRun(error);
      state.handle.cancel(error);
      this.#scheduler.enqueue("control", state.requestId, {
        kind: "request_cancel",
        requestId: state.requestId,
        reason: "client aborted",
      });
    } else {
      const published = state.published;
      this.#removeState(state);
      state.rejectRun(abortError("V2 request aborted before commitment"));
      state.handle.cancel(abortError("V2 request aborted before commitment"));
      if (published) {
        this.#scheduler.enqueue("control", state.requestId, {
          kind: "request_cancel",
          requestId: state.requestId,
          reason: "client aborted",
        });
      }
    }
    this.#requestPublication(this.#pump());
  }

  #failPreCommit(state: RequestState, error: Error): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#removeState(state);
    state.rejectRun(error);
    state.handle.fail(error);
  }

  #failCommitted(state: RequestState, error: V2CommittedError): void {
    if (!this.#requests.has(state.requestId)) return;
    const acknowledged = state.committed;
    if (error.ambiguous) {
      if (!state.quarantineKind) {
        state.quarantineKind = "ambiguity";
        this.#quarantined++;
        this.#ambiguousQuarantined++;
      } else if (state.quarantineKind === "cancellation") {
        state.quarantineKind = "ambiguity";
        this.#ambiguousQuarantined++;
      }
    }
    // commitPublished is deliberately promoted to committed-risk here so
    // removal cannot release capacity for work that may still be executing.
    state.committed = true;
    this.#removeState(state);
    if (!acknowledged) state.rejectRun(error);
    state.handle.fail(error);
  }

  #removeState(state: RequestState): void {
    this.#requests.delete(state.requestId);
    const pendingIndex = this.#pending.indexOf(state);
    if (pendingIndex >= 0) this.#pending.splice(pendingIndex, 1);
    this.#scheduler.removeStream(state.requestId);
    if (state.admitted && !state.committed) {
      this.#creditReservations = Math.max(0, this.#creditReservations - 1);
    }
    state.signal?.removeEventListener("abort", state.onAbort!);
    if (state.admissionTimer) clearTimeout(state.admissionTimer);
    state.admissionTimer = undefined;
    if (this.#requests.size === 0) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
    this.#admitPending();
  }

  #failProtocol(cause: unknown): void {
    const message = cause instanceof Error ? cause.message : "V2 transport protocol failed";
    this.#protocolFailed = true;
    this.#connected = false;
    this.#draining = true;
    this.#remoteCredits = 0;
    if (this.#livenessProbe) {
      this.#rejectLivenessProbe(
        this.#livenessProbe,
        new V2TransportLivenessError(
          "V2 transport failed during its liveness probe",
          "unavailable",
        ),
        false,
      );
    }
    for (const state of [...this.#requests.values()]) {
      if (state.committed || state.commitPublished) {
        this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
      } else {
        this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
      }
    }
  }
}
