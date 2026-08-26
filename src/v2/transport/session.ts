import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import { sseFrame, type SseFrame } from "../../relay/sse";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../awareness/channel";
import {
  GP_V2_R1_SCHEMA_VERSION,
  GP_V2_R2_SCHEMA_VERSION,
  awarenessEnvelopeEncodedBytes,
  base64UrlDecode,
  base64UrlEncode,
  canonicalJson,
  protocolTransportLimits,
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
  type R2AwarenessEnvelope,
  type R2RequestPreparedFrame,
  type R2RequestStartFrame,
  type SidecarFrame,
  type UpstreamPrelude,
  type V2ProtocolTransportLimits,
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
  MAX_TRANSCRIPT_CHUNK_BYTES,
  TranscriptProof,
  validateTerminalProof,
  type IncrementalSha256Hasher,
} from "../protocol/transcript";
import {
  encodedFrameBytes,
  InboundFrameCursor,
  OutboundFrameWindow,
} from "../protocol/window";
import { TransportCongestion } from "./congestion";
import {
  V2CommittedError,
  V2PreCommitError,
  V2RunHandle,
} from "./run-handle";
import { createNativeTranscriptHasher } from "./native-transcript-hasher";
import {
  V2_HOST_TRANSFER_BUDGET,
  v2RequestTransferCost,
} from "./admission-cost";
import {
  AtomicBytePermit,
  AtomicBytePermitPool,
  estimateAtomicEventPermitBytes,
} from "../memory/atomic-byte-permits";
import { v2RetainedEventPermitBytes } from "../memory/event-memory-sizing";

export { computeV2RetransmitDelayMs } from "../protocol/retransmit";
import {
  DEFAULT_V2_INITIAL_RETRANSMIT_MS,
  deterministicV2JitterSeed,
} from "../protocol/retransmit";

const R1_REQUEST_PART_BYTES = 8 * 1024;
const R2_REQUEST_PART_BYTES = 11 * 1024;
const MIN_GZIP_SAVINGS_RATIO = 0.05;
const LARGE_INLINE_BASE64_BYTES = 64 * 1024;
const GZIP_SAMPLE_BYTES = 64 * 1024;
const DEFAULT_TRANSPORT_STALL_MS = 2_000;
const GOODPUT_IDLE_RESET_MS = 60_000;

function hostFrameCarriesRequest(frame: ReliableFrame<HostFrame>): boolean {
  // Route payloads by request-body goodput only. Tiny request_start/commit
  // control flights would otherwise drag the EWMA toward a few KiB/s after
  // every successful handshake and make healthy sandboxes look unusable.
  return frame.kind === "request_data";
}

function containsLargeInlineBase64(value: unknown, seen = new Set<object>()): boolean {
  if (typeof value === "string") {
    return value.length >= LARGE_INLINE_BASE64_BYTES &&
      /^data:[^;,]{1,127};base64,/i.test(value);
  }
  if (!value || typeof value !== "object" || seen.has(value)) return false;
  seen.add(value);
  try {
    if (Array.isArray(value)) return value.some(item => containsLargeInlineBase64(item, seen));
    return Object.values(value).some(item => containsLargeInlineBase64(item, seen));
  } finally {
    seen.delete(value);
  }
}

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

export interface V2GzipOptions {
  signal?: AbortSignal;
  /** The supplied view is a disposable, complete buffer owned by the caller. */
  inputOwnership?: "owned";
}

export interface V2TransportRunOptions {
  /** Ownership handoff after this session charges its admission credit. */
  onAdmissionReflected?(): void;
}

export interface V2TransportSessionOptions {
  binding: ProtocolBinding;
  channel: AwarenessChannel<HostFrame, SidecarFrame>;
  clock?: Clock;
  gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
  maxResponseBytes: number;
  maxBufferedResponseBytes?: number;
  hostCeiling?: number;
  retransmitMs?: number;
  rotationTimeoutMs?: number;
  publicationIntervalMs?: number;
  unadmittedTimeoutMs?: number;
  /** Sidecar instance proven by the bootstrap handshake. Required for liveness probes. */
  expectedSidecarInstanceId?: string;
  /** Deterministic tests may inject this; production uses a fresh UUID. */
  hostInstanceId?: string;
  /** Primarily injectable for deterministic tests. Production defaults to five seconds. */
  livenessTimeoutMs?: number;
  /** One process-wide pool shared by every V2 transport session. */
  eventMemoryPool?: AtomicBytePermitPool;
  /** Coalesced notification when admission or transport-health state changes. */
  onStateChange?(): void;
  /** Deterministic tests may shorten the production two-second stall fence. */
  transportStallMs?: number;
}

interface EventPart {
  offset: number;
  bytes: Uint8Array;
}

interface EventAssembly {
  event: string;
  partCount: number;
  parts: Map<number, EventPart>;
  rawBytes: number;
  permit: AtomicBytePermit | null;
}

interface CompletedEvent {
  frame: SseFrame;
  wireBytes: number;
  permit: AtomicBytePermit | null;
}

interface RecoveryPrefix {
  readonly expectedEventCount: number;
  readonly expectedByteCount: number;
  readonly expectedDigest: string;
  readonly hasher: IncrementalSha256Hasher;
  hashedEventCount: number;
  hashedByteCount: number;
  nextEventIndex: number;
  segmented: {
    event: string;
    partCount: number;
    nextPartIndex: number;
    nextOffset: number;
  } | null;
}

interface PreparedRequestPlan {
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
  partBytes: number;
  encodedPayload: Uint8Array | null;
  nextPartIndex: number;
  stage: "open" | "data" | "commit" | "done";
}

interface RequestManifest {
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  encoding: RequestEncoding;
  partCount: number;
}

interface RequestState {
  requestId: string;
  requestDigest: string;
  manifest: RequestManifest;
  /** Lazily consumed so large base64 frame arrays are never retained. */
  preparedRequest: PreparedRequestPlan | null;
  handle: V2RunHandle;
  transcript: TranscriptProof<false>;
  resolveRun: (handle: RunHandle) => void;
  rejectRun: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  onAdmissionReflected?: () => void;
  admitted: boolean;
  transferCost: number;
  published: boolean;
  commitStaged: boolean;
  commitPublished: boolean;
  /** Irreversible R2 boundary after request_start enters the cumulative window. */
  startRisk: boolean;
  startQueued: boolean;
  startStaged: boolean;
  committed: boolean;
  cancelled: boolean;
  quarantineKind: "cancellation" | "ambiguity" | null;
  responseBytes: number;
  nextEventIndex: number;
  assemblies: Map<number, EventAssembly>;
  completedEvents: Map<number, CompletedEvent>;
  terminal: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }> | null;
  upstreamPrelude: UpstreamPrelude | null;
  diagnostics: HostRequestDiagnosticsObserver;
  firstResponseSeen: boolean;
  admissionTimer?: ReturnType<typeof setTimeout>;
  recoveryPrefix?: RecoveryPrefix;
}

export interface V2SidecarRecoveryToken {
  readonly id: string;
  readonly binding: ProtocolBinding;
  readonly previousInstanceId: string;
}

interface SidecarRecoveryState {
  token: V2SidecarRecoveryToken;
  markerSeen: boolean;
  resolve(): void;
  reject(error: unknown): void;
  promise: Promise<void>;
}

interface TerminalReceipt {
  terminal: Extract<SidecarFrame, { kind: "response_terminal" }>;
  queued: boolean;
  replayed: boolean;
  cursor: { epoch: number; sequence: number } | null;
}

interface SidecarRecoveryCapsulePayload {
  states: RequestState[];
  receipts: Map<string, TerminalReceipt>;
  ambiguityTombstones: Map<string, string>;
  consumed: boolean;
}

const SIDECAR_RECOVERY_CAPSULES = new WeakMap<object, SidecarRecoveryCapsulePayload>();

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
const MAX_TERMINAL_RECEIPTS = 4_096;

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

function base64UrlDecodedBytes(value: string): number | null {
  if (!/^[A-Za-z0-9_-]*$/.test(value) || value.length % 4 === 1) return null;
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const remainder = value.length % 4;
  if (remainder === 2 && (alphabet.indexOf(value.at(-1)!) & 0x0f) !== 0) return null;
  if (remainder === 3 && (alphabet.indexOf(value.at(-1)!) & 0x03) !== 0) return null;
  return Math.floor(value.length * 3 / 4);
}

/**
 * Host-only fast path after base64UrlDecodedBytes has already proved the
 * alphabet, padding shape, trailing bits, and exact decoded length. The shared
 * browser/Deno decoder must materialize an atob binary string and copy it one
 * byte at a time; Bun's native Buffer decoder avoids that transient string.
 */
function decodeCanonicalBase64Url(value: string, decodedBytes: number): Uint8Array {
  const output = Buffer.from(value, "base64url");
  if (output.byteLength !== decodedBytes) {
    throw new TypeError("Value must be canonical unpadded base64url");
  }
  return output;
}

function partialEventLogicalBytes(event: string, rawBytes: number): number {
  return utf8Bytes(`event: ${event}\ndata: \n\n`).byteLength + rawBytes;
}

function canonicalEventWireBytes(event: string, parts: readonly Uint8Array[]): number {
  let rawBytes = 0;
  for (const part of parts) {
    rawBytes += part.byteLength;
  }
  // TranscriptProof's frozen canonical contract is one data prefix followed by
  // the exact event-data string, including any embedded CR/LF bytes.
  return partialEventLogicalBytes(event, rawBytes);
}

function equivalentEventStreamContentType(left: string, right: string): boolean {
  const normalize = (value: string): string | null => {
    const parts = value.split(";").map(part => part.trim().toLowerCase());
    if (parts.shift() !== "text/event-stream") return null;
    const retained: string[] = [];
    for (const part of parts) {
      if (!part) continue;
      const separator = part.indexOf("=");
      const key = separator < 0 ? part : part.slice(0, separator).trim();
      const raw = separator < 0 ? "" : part.slice(separator + 1).trim().replace(/^"|"$/g, "");
      if (key === "charset" && (raw === "utf-8" || raw === "utf8")) continue;
      retained.push(`${key}=${raw}`);
    }
    retained.sort();
    return `text/event-stream;${retained.join(";")}`;
  };
  const normalizedLeft = normalize(left);
  return normalizedLeft !== null && normalizedLeft === normalize(right);
}

function validBinding(
  envelope: AwarenessEnvelope<SidecarFrame>,
  binding: ProtocolBinding,
): boolean {
  const revisionMatches = binding.protocolRevision === "r2"
    ? envelope.schemaVersion === GP_V2_R2_SCHEMA_VERSION &&
      envelope.protocolRevision === "r2" &&
      envelope.launchNonce === binding.launchNonce
    : envelope.schemaVersion === GP_V2_R1_SCHEMA_VERSION;
  return envelope.protocol === "gp-awareness-v2" &&
    revisionMatches &&
    envelope.projectId === binding.projectId &&
    envelope.sessionId === binding.sessionId &&
    envelope.leaderGeneration === binding.leaderGeneration &&
    envelope.buildHash === binding.buildHash &&
    envelope.schemaHash === binding.schemaHash &&
    envelope.peerId === "sidecar";
}

function sameRecoveryScope(left: ProtocolBinding, right: ProtocolBinding): boolean {
  return left.projectId === right.projectId &&
    left.sessionId === right.sessionId &&
    left.leaderGeneration === right.leaderGeneration &&
    left.buildHash === right.buildHash &&
    left.schemaHash === right.schemaHash &&
    left.protocolRevision === right.protocolRevision;
}

type PublicationUrgency = "paced" | "urgent";

function hostFrameUrgency(frame: HostFrame): PublicationUrgency {
  switch (frame.kind) {
    case "request_open":
    case "request_data":
    case "request_commit":
      return "paced";
    case "request_start":
      return "urgent";
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
  readonly #limits: V2ProtocolTransportLimits;
  readonly #channel: AwarenessChannel<HostFrame, SidecarFrame>;
  readonly #clock: Clock;
  readonly #gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
  readonly #maxResponseBytes: number;
  readonly #maxBufferedResponseBytes: number;
  readonly #eventMemoryPool?: AtomicBytePermitPool;
  readonly #onStateChange?: () => void;
  readonly #transportStallMs: number;
  readonly #fastRetransmitFloorMs: number;
  #hostCeiling: number;
  readonly #rotationTimeoutMs: number;
  readonly #unadmittedTimeoutMs: number;
  readonly #expectedSidecarInstanceId: string | null;
  readonly #livenessTimeoutMs: number;
  readonly #hostInstanceId: string;
  readonly #outbound = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<SidecarFrame>({ epoch: 1 });
  #processedInboundAck = { epoch: 1, sequence: -1 };
  readonly #scheduler = new WeightedFrameScheduler<HostFrame>();
  readonly #delivery: ReliableDeliveryTiming;
  readonly #flight = new AckClockedFlight({ epoch: 1 });
  readonly #pacer: PublicationPacer;
  readonly #requests = new Map<string, RequestState>();
  readonly #rotations = new Map<string, RotationWaiter>();
  readonly #rotationStreams = new Set<string>();
  readonly #pending: RequestState[] = [];
  readonly #idleWaiters = new Set<() => void>();
  readonly #retiredSidecarInstances = new Set<string>();
  readonly #terminalReceipts = new Map<string, TerminalReceipt>();
  readonly #ambiguityTombstones = new Map<string, string>();
  #sidecarRecovery: SidecarRecoveryState | null = null;
  #connected = false;
  #closed = false;
  #draining = false;
  #protocolFailed = false;
  #connecting: Promise<void> | null = null;
  #unsubscribe: (() => void) | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #stallTimer: ReturnType<typeof setTimeout> | null = null;
  #processing: Promise<void> = Promise.resolve();
  #lifecycleEpoch = 0;
  #teardownPromise: Promise<void> | null = null;
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
  #transferReservations = 0;
  #quarantined = 0;
  #ambiguousQuarantined = 0;
  #retransmissions = 0;
  readonly #drainWaiters = new Set<{
    resolve(): void;
    reject(error: unknown): void;
  }>();
  #drainFrameQueued = false;
  #remoteDrained = false;
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
  readonly #congestion: TransportCongestion;
  #stallEvents = 0;
  #stateChangeScheduled = false;
  #flightStartedAtMs: number | null = null;
  #flightRequestBytes = 0;
  #acknowledgedGoodputBytesPerSecond: number | null = null;
  #goodputUpdatedAtMs: number | null = null;
  #uniqueAcknowledgedBytes = 0;
  #retransmittedBytes = 0;
  #lastFastRetransmitAtMs: number | null = null;
  #stallTimerDueAtMs: number | null = null;

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
    if (options.hostInstanceId !== undefined && !SAFE_INSTANCE_ID.test(options.hostInstanceId)) {
      throw new TypeError("hostInstanceId is invalid");
    }
    if (!SAFE_INSTANCE_ID.test(options.binding.launchNonce)) {
      throw new TypeError("binding launchNonce is invalid");
    }
    if (options.binding.protocolRevision === "r2" && !options.expectedSidecarInstanceId) {
      throw new TypeError("R2 transport requires expectedSidecarInstanceId");
    }
    const livenessTimeoutMs = options.livenessTimeoutMs ?? DEFAULT_LIVENESS_TIMEOUT_MS;
    if (!Number.isSafeInteger(livenessTimeoutMs) || livenessTimeoutMs < 1) {
      throw new RangeError("livenessTimeoutMs must be a positive safe integer");
    }
    this.#binding = options.binding;
    this.#limits = protocolTransportLimits(options.binding.protocolRevision);
    this.#channel = options.channel;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#gzip = options.gzip;
    this.#maxResponseBytes = options.maxResponseBytes;
    this.#maxBufferedResponseBytes = maxBufferedResponseBytes;
    this.#eventMemoryPool = options.eventMemoryPool;
    this.#onStateChange = options.onStateChange;
    this.#transportStallMs = options.transportStallMs ?? DEFAULT_TRANSPORT_STALL_MS;
    if (!Number.isFinite(this.#transportStallMs) || this.#transportStallMs < 1) {
      throw new RangeError("transportStallMs must be positive");
    }
    this.#fastRetransmitFloorMs = Math.min(1_000, this.#transportStallMs);
    this.#congestion = new TransportCongestion({ highAgeMs: this.#transportStallMs });
    this.#hostCeiling = hostCeiling;
    this.#rotationTimeoutMs = rotationTimeoutMs;
    this.#unadmittedTimeoutMs = unadmittedTimeoutMs;
    this.#expectedSidecarInstanceId = options.expectedSidecarInstanceId ?? null;
    this.#hostInstanceId = options.hostInstanceId ?? `host-${crypto.randomUUID()}`;
    this.#livenessTimeoutMs = livenessTimeoutMs;
    this.#delivery = new ReliableDeliveryTiming({
      epoch: 1,
      minimumMs: retransmitMs,
      initialRtoMs: options.retransmitMs === undefined
        ? Math.max(retransmitMs, DEFAULT_V2_INITIAL_RETRANSMIT_MS)
        : retransmitMs,
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

  async resumeAwareness(signal?: AbortSignal): Promise<void> {
    if (this.#closed || this.#draining || this.#protocolFailed || !this.#connected) {
      throw new V2TransportLivenessError(
        "V2 transport is unavailable for Awareness reconnect",
        "unavailable",
      );
    }
    await this.#channel.connect(signal);
    if (this.#closed || this.#draining || this.#protocolFailed) {
      throw new V2TransportLivenessError(
        "V2 transport closed during Awareness reconnect",
        "unavailable",
      );
    }
    this.#pacer.flushNow("urgent");
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
    options?: V2TransportRunOptions,
  ): Promise<RunHandle> {
    if (signal?.aborted) throw abortError("V2 request aborted before commitment");
    if (!this.#connected || this.#closed) {
      throw new V2PreCommitError("V2 transport is not connected", "not_connected");
    }
    if (this.#sidecarRecovery) {
      throw new V2PreCommitError(
        "V2 sidecar recovery is in progress",
        "sidecar_recovering",
      );
    }
    if (this.#draining) {
      throw new V2PreCommitError("V2 transport is draining", "draining");
    }
    const requestId = `req-${crypto.randomUUID()}`;
    const diagnostics = new HostRequestDiagnosticsObserver(() => this.#clock.now());
    const prepared = await this.#prepareRequest(body, signal);
    diagnostics.prepared(prepared);
    if (signal?.aborted) throw abortError("V2 request aborted before commitment");
    if (this.#sidecarRecovery || !this.#connected) {
      throw new V2PreCommitError(
        "V2 sidecar recovery started during request preparation",
        "sidecar_recovering",
      );
    }
    if (this.#draining || this.#closed) {
      throw new V2PreCommitError("V2 transport stopped during request preparation", "draining");
    }
    const handle = new V2RunHandle({
      clock: this.#clock,
      maxBufferedBytes: this.#maxBufferedResponseBytes,
      expectUpstreamPrelude: this.#binding.protocolRevision === "r2",
      requireTransferredEventMemory: this.#eventMemoryPool !== undefined,
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
      manifest: {
        requestDigest: prepared.requestDigest,
        decodedBytes: prepared.decodedBytes,
        encodedBytes: prepared.encodedBytes,
        encoding: prepared.encoding,
        partCount: prepared.partCount,
      },
      preparedRequest: prepared,
      handle,
      transcript: new TranscriptProof(requestId, prepared.requestDigest, {
        maxBytes: this.#maxResponseBytes,
        retainBytes: false,
        hasher: createNativeTranscriptHasher(),
      }),
      resolveRun,
      rejectRun,
      signal,
      onAdmissionReflected: options?.onAdmissionReflected,
      admitted: false,
      transferCost: v2RequestTransferCost(prepared.encodedBytes),
      published: false,
      commitStaged: false,
      commitPublished: false,
      startRisk: false,
      startQueued: false,
      startStaged: false,
      committed: false,
      cancelled: false,
      quarantineKind: null,
      responseBytes: 0,
      nextEventIndex: 0,
      assemblies: new Map(),
      completedEvents: new Map(),
      terminal: null,
      upstreamPrelude: null,
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

  /**
   * R2-only in-place recovery boundary. It revokes the elected sidecar and all
   * old publications while retaining only requests for which execution may
   * already have started. The returned token is process-local and single use.
   */
  async suspendSidecarRecovery(
    _reason: string,
    signal?: AbortSignal,
  ): Promise<V2SidecarRecoveryToken> {
    if (this.#binding.protocolRevision !== "r2") {
      throw new V2TransportLivenessError(
        "In-place sidecar recovery requires R2",
        "unavailable",
      );
    }
    if (signal?.aborted) throw abortError("V2 sidecar recovery was aborted");
    if (this.#closed || this.#draining || !this.#connected || this.#sidecarRecovery) {
      throw new V2TransportLivenessError(
        "V2 transport is unavailable for sidecar recovery",
        "unavailable",
      );
    }
    const previousInstanceId = this.#expectedSidecarInstanceId ?? this.#sidecarInstanceId;
    if (!previousInstanceId) {
      throw new V2TransportLivenessError(
        "V2 transport has no elected sidecar to recover",
        "unavailable",
      );
    }
    const token = Object.freeze({
      id: crypto.randomUUID(),
      binding: Object.freeze({ ...this.#binding }),
      previousInstanceId,
    });
    const payload: SidecarRecoveryCapsulePayload = {
      states: [],
      receipts: new Map(),
      ambiguityTombstones: new Map(),
      consumed: false,
    };

    // This is the irreversible suspension boundary. Stop every future
    // publication/update source before clearing Awareness, then drain the
    // stable processing tail. Caller cancellation is intentionally ignored
    // beyond this point: we must produce a capsule or fail closed, never leave
    // a half-revoked transport with live handles.
    this.#draining = true;
    this.#remoteCredits = 0;
    this.#connected = false;
    this.#pacer.close();
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimer = null;
    this.#stallTimerDueAtMs = null;
    if (this.#livenessProbe) {
      this.#rejectLivenessProbe(
        this.#livenessProbe,
        new V2TransportLivenessError(
          "V2 sidecar liveness probe was superseded by recovery",
          "instance_changed",
        ),
        false,
      );
    }
    try {
      this.#channel.clear();
    } catch (cause) {
      const message = cause instanceof Error
        ? cause.message
        : "V2 sidecar recovery could not revoke its Awareness state";
      await this.#beginTeardown(
        new V2PreCommitError(message, "sidecar_recovery_suspend_failed"),
        new V2CommittedError(message, "sidecar_recovery_suspend_failed", true),
      ).catch(() => undefined);
      throw cause;
    }
    await this.#joinProcessingTail();
    if (this.#teardownPromise || this.#protocolFailed) {
      throw new V2TransportLivenessError(
        "V2 transport failed while suspending sidecar recovery",
        "unavailable",
      );
    }
    // Frame processing can classify a request as ambiguous while the
    // suspension tail is draining. Snapshot only after that tail is stable so
    // the replacement cannot lose any safety-budget tombstone.
    payload.ambiguityTombstones = new Map(this.#ambiguityTombstones);

    const rotationIds = new Set([
      ...this.#rotationStreams,
      ...this.#rotations.keys(),
    ]);
    for (const rotation of this.#rotations.values()) {
      rotation.signal?.removeEventListener("abort", rotation.onAbort!);
      if (rotation.timer) clearTimeout(rotation.timer);
      rotation.reject(new V2TransportLivenessError(
        "Credential rotation was interrupted by sidecar recovery",
        "instance_changed",
      ));
    }
    this.#rotations.clear();
    this.#scheduler.removeStream("control");
    for (const rotationId of rotationIds) {
      this.#removeRotationStream(rotationId);
    }
    this.#rotationStreams.clear();

    for (const state of [...this.#requests.values()]) {
      this.#scheduler.removeStream(state.requestId);
      if (!this.#hasExecutionRisk(state)) {
        this.#failPreCommit(state, new V2PreCommitError(
          "V2 sidecar stopped before execution during recovery",
          "sidecar_recovery",
        ));
        continue;
      }
      this.#releasePartialResponseState(state);
      state.preparedRequest = null;
      state.terminal = null;
      state.startQueued = false;
      state.startStaged = false;
      state.onAdmissionReflected = undefined;
      const prefix = state.transcript.snapshot();
      state.responseBytes = prefix.byteCount;
      state.recoveryPrefix = {
        expectedEventCount: prefix.eventCount,
        expectedByteCount: prefix.byteCount,
        expectedDigest: prefix.transcriptSha256,
        hasher: createNativeTranscriptHasher(),
        hashedEventCount: 0,
        hashedByteCount: 0,
        nextEventIndex: 0,
        segmented: null,
      };
      this.#detachStateForRecovery(state);
      payload.states.push(state);
    }
    this.#creditReservations = 0;
    this.#transferReservations = 0;
    for (const [requestId, receipt] of this.#terminalReceipts) {
      this.#scheduler.removeStream(requestId);
      receipt.queued = false;
      receipt.replayed = false;
      receipt.cursor = null;
      payload.receipts.set(requestId, receipt);
    }
    this.#terminalReceipts.clear();
    this.#ambiguityTombstones.clear();
    this.#ambiguousQuarantined = 0;
    this.#quarantined = 0;
    // The retired session is intentionally never reconnectable. Drop every
    // cumulative window/cursor after ownership has moved so prompt parts and
    // sealed credential frames cannot remain retained behind close()'s
    // already-closed fast path.
    const nextOutboundEpoch = this.#outbound.snapshot().epoch + 1;
    this.#outbound.reset(nextOutboundEpoch);
    this.#delivery.reset(nextOutboundEpoch);
    this.#flight.reset(nextOutboundEpoch);
    this.#resetFlightMeasurement();
    const nextInboundEpoch = this.#inbound.ack().epoch + 1;
    this.#inbound.reset(nextInboundEpoch);
    this.#processedInboundAck = { epoch: nextInboundEpoch, sequence: -1 };
    this.#dataDirty = false;
    this.#sidecarDiagnostics = null;

    this.#closed = true;
    SIDECAR_RECOVERY_CAPSULES.set(token, payload);
    try {
      await this.#channel.close();
    } catch (cause) {
      this.failSidecarRecovery(token, new V2CommittedError(
        cause instanceof Error ? cause.message : "Old sidecar channel could not close",
        "sidecar_recovery_suspend_failed",
        true,
      ));
      throw cause;
    }
    return token;
  }

  adoptRecoveredSidecar(token: V2SidecarRecoveryToken): Promise<void> {
    if (this.#binding.protocolRevision !== "r2" || this.#connected || this.#closed) {
      throw new V2TransportLivenessError(
        "Recovery capsule requires a fresh, disconnected R2 transport",
        "unavailable",
      );
    }
    const payload = SIDECAR_RECOVERY_CAPSULES.get(token);
    if (!payload || payload.consumed) {
      throw new V2TransportLivenessError(
        "V2 sidecar recovery capsule is stale or already consumed",
        "instance_changed",
      );
    }
    if (
      !sameRecoveryScope(this.#binding, token.binding) ||
      this.#binding.launchNonce === token.binding.launchNonce ||
      this.#expectedSidecarInstanceId === token.previousInstanceId
    ) {
      throw new V2TransportLivenessError(
        "Recovery target does not match the same-sandbox R2 scope",
        "instance_changed",
      );
    }
    payload.consumed = true;
    SIDECAR_RECOVERY_CAPSULES.delete(token);
    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<void>((resolveValue, rejectValue) => {
      resolve = resolveValue;
      reject = rejectValue;
    });
    void promise.catch(() => undefined);
    this.#sidecarRecovery = {
      token,
      markerSeen: false,
      resolve,
      reject,
      promise,
    };
    this.#remoteCredits = 0;
    for (const [requestId, requestDigest] of payload.ambiguityTombstones) {
      if (this.#ambiguityTombstones.has(requestId)) continue;
      this.#ambiguityTombstones.set(requestId, requestDigest);
      this.#ambiguousQuarantined++;
      this.#quarantined++;
    }
    for (const [requestId, receipt] of payload.receipts) {
      this.#terminalReceipts.set(requestId, receipt);
    }
    for (const state of payload.states) {
      state.signal?.removeEventListener("abort", state.onAbort!);
      state.onAbort = () => this.#abortRequest(state);
      state.signal?.addEventListener("abort", state.onAbort, { once: true });
      this.#requests.set(state.requestId, state);
      if (state.quarantineKind) this.#quarantined++;
      if (state.quarantineKind === "ambiguity") this.#ambiguousQuarantined++;
      if (state.signal?.aborted) this.#abortRequest(state);
    }
    return promise;
  }

  failSidecarRecovery(token: V2SidecarRecoveryToken, cause: unknown): void {
    const error = cause instanceof V2CommittedError
      ? cause
      : new V2CommittedError(
          cause instanceof Error ? cause.message : "V2 sidecar recovery failed",
          "sidecar_recovery_failed",
          true,
        );
    const recovery = this.#sidecarRecovery;
    if (recovery?.token === token) {
      void this.#beginTeardown(
        new V2PreCommitError(error.message, error.code),
        error,
      ).catch(() => undefined);
      return;
    }
    const payload = SIDECAR_RECOVERY_CAPSULES.get(token);
    if (!payload || payload.consumed) return;
    payload.consumed = true;
    SIDECAR_RECOVERY_CAPSULES.delete(token);
    for (const state of payload.states) {
      if (!state.committed) state.rejectRun(error);
      state.handle.fail(error);
    }
    payload.receipts.clear();
    payload.ambiguityTombstones.clear();
  }
  async drain(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#closed) return this.#teardownPromise ?? Promise.resolve();
    this.#draining = true;
    for (const state of [...this.#pending]) {
      if (state.admitted) continue;
      this.#failPreCommit(
        state,
        new V2PreCommitError("V2 transport drained before admission", "draining"),
      );
    }
    while (this.#requests.size > 0) {
      await this.#waitForIdle(signal);
    }
    this.#queueDrain(reason);
    await this.#waitForDrained(signal);
    await this.close();
  }

  /**
   * Recovery-only stop: cancel every local request, publish one fenced drain
   * immediately, continue ACKing sidecar output until drained/deadline, then
   * close locally. No request is exposed as safely retryable after start-risk.
   */
  async forceStop(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#closed) return this.#teardownPromise ?? Promise.resolve();
    if (!this.#connected) {
      await this.close();
      return;
    }
    this.#draining = true;
    for (const state of [...this.#requests.values()]) {
      const requestId = state.requestId;
      const published = state.published;
      if (this.#hasExecutionRisk(state)) {
        this.#failCommitted(state, new V2CommittedError(
          "V2 request was interrupted by sandbox recovery",
          "sandbox_recovery",
          true,
        ));
      } else {
        this.#failPreCommit(state, new V2PreCommitError(
          "V2 request stopped before execution during sandbox recovery",
          "sandbox_recovery",
        ));
      }
      if (published) {
        try {
          this.#scheduler.enqueue("control", requestId, {
            kind: "request_cancel",
            requestId,
            reason: "sandbox recovery",
          });
        } catch {
          // The drain frame remains the authoritative process-wide stop.
        }
      }
    }
    try {
      this.#queueDrain(reason);
      await this.#waitForDrained(signal);
    } finally {
      await this.close();
    }
  }

  #queueDrain(reason: string): void {
    if (this.#drainFrameQueued || this.#closed) return;
    this.#scheduler.enqueue("control", "control", {
      kind: "drain",
      reason: reason.replace(/[\r\n\0]/g, " ").trim().slice(0, 256) || "shutdown",
    });
    this.#drainFrameQueued = true;
    this.#requestPublication(this.#pump());
  }

  #waitForIdle(signal?: AbortSignal): Promise<void> {
    if (this.#requests.size === 0) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(abortError("V2 transport drain aborted"));
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        this.#idleWaiters.delete(onIdle);
        signal?.removeEventListener("abort", onAbort);
        operation();
      };
      const onIdle = () => finish(resolve);
      const onAbort = () => finish(() => reject(abortError("V2 transport drain aborted")));
      this.#idleWaiters.add(onIdle);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (this.#requests.size === 0) onIdle();
    });
  }

  #waitForDrained(signal?: AbortSignal): Promise<void> {
    if (this.#remoteDrained && this.#terminalReceipts.size === 0) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(abortError("V2 transport drain aborted"));
    return new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        this.#drainWaiters.delete(waiter);
        signal?.removeEventListener("abort", onAbort);
        operation();
      };
      const waiter = {
        resolve: () => finish(resolve),
        reject: (error: unknown) => finish(() => reject(error)),
      };
      const onAbort = () => waiter.reject(abortError("V2 transport drain aborted"));
      this.#drainWaiters.add(waiter);
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }

  #maybeResolveDrainWaiters(): void {
    if (!this.#remoteDrained || this.#terminalReceipts.size > 0) return;
    for (const waiter of [...this.#drainWaiters]) waiter.resolve();
  }

  #removeRotationStream(rotationId: string): void {
    this.#scheduler.removeStream(`rotation:${rotationId}`);
    this.#rotationStreams.delete(rotationId);
  }

  rotateCredential(
    rotationId: string,
    sealedJson: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.#connected || this.#closed || this.#draining || this.#sidecarRecovery) {
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
        this.#removeRotationStream(rotationId);
        if (waiter.timer) clearTimeout(waiter.timer);
        reject(abortError("Credential rotation aborted"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#rotations.set(rotationId, waiter);
      waiter.timer = setTimeout(() => {
        if (this.#rotations.get(rotationId) !== waiter) return;
        this.#rotations.delete(rotationId);
        this.#removeRotationStream(rotationId);
        signal?.removeEventListener("abort", waiter.onAbort!);
        reject(new Error(`Credential rotation ACK timed out: ${rotationId}`));
      }, this.#rotationTimeoutMs);
    });
    void pending.catch(() => undefined);
    this.#rotationStreams.add(rotationId);
    try {
      this.#scheduler.enqueue("control", `rotation:${rotationId}`, {
        kind: "credential_rotate",
        rotationId,
        sealedJson,
      });
    } catch (cause) {
      const waiter = this.#rotations.get(rotationId);
      if (waiter) {
        this.#rotations.delete(rotationId);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        this.#removeRotationStream(rotationId);
        waiter.reject(cause);
      }
      return pending;
    }
    this.#requestPublication(this.#pump());
    return pending;
  }

  close(): Promise<void> {
    if (this.#closed && !this.#teardownPromise) return Promise.resolve();
    return this.#beginTeardown(
      new V2PreCommitError("V2 transport session closed", "closed"),
      new V2CommittedError(
        "V2 transport closed after request commitment may have been published",
        "closed",
        true,
      ),
    );
  }

  #beginTeardown(
    preCommitError: Error,
    committedError: V2CommittedError,
  ): Promise<void> {
    if (this.#teardownPromise) return this.#teardownPromise;
    this.#closed = true;
    this.#connected = false;
    this.#draining = true;
    this.#remoteCredits = 0;
    this.#lifecycleEpoch++;
    if (this.#sidecarRecovery) {
      const recovery = this.#sidecarRecovery;
      this.#sidecarRecovery = null;
      recovery.reject(committedError);
    }
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
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimer = null;
    this.#stallTimerDueAtMs = null;
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    for (const [rotationId, waiter] of this.#rotations) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      if (waiter.timer) clearTimeout(waiter.timer);
      waiter.reject(preCommitError);
      this.#rotations.delete(rotationId);
      this.#removeRotationStream(rotationId);
    }
    for (const rotationId of [...this.#rotationStreams]) {
      this.#removeRotationStream(rotationId);
    }
    this.#rotationStreams.clear();
    for (const waiter of [...this.#drainWaiters]) waiter.reject(preCommitError);
    // Wake any reliable-data delivery currently waiting for client buffer
    // space before joining the serialized processing tail. Teardown is already
    // irreversible, so retaining a blocked producer here would deadlock close
    // and prevent the state below from being classified and released.
    for (const state of this.#requests.values()) {
      if (this.#hasExecutionRisk(state)) state.handle.cancel(committedError);
      else state.handle.fail(preCommitError);
    }
    const teardown = Promise.resolve().then(async () => {
      await this.#joinProcessingTail();
      for (const state of [...this.#requests.values()]) {
        if (this.#hasExecutionRisk(state)) this.#failCommitted(state, committedError);
        else this.#failPreCommit(state, preCommitError);
      }
      for (const requestId of this.#terminalReceipts.keys()) {
        this.#scheduler.removeStream(requestId);
      }
      this.#scheduler.removeStream("control");
      this.#terminalReceipts.clear();
      this.#creditReservations = 0;
      this.#transferReservations = 0;
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#flight.reset(nextEpoch);
      this.#resetFlightMeasurement();
      this.#dataDirty = false;
      try {
        this.#channel.clear();
      } finally {
        await this.#channel.close();
      }
    });
    this.#teardownPromise = teardown;
    return teardown;
  }

  snapshot(): V2TransportSnapshot {
    let committed = 0;
    for (const state of this.#requests.values()) {
      if (state.committed) committed++;
    }
    return {
      connected: this.#connected && !this.#closed && !this.#sidecarRecovery,
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

  #notifyStateChange(): void {
    if (!this.#onStateChange || this.#stateChangeScheduled || this.#closed) return;
    this.#stateChangeScheduled = true;
    queueMicrotask(() => {
      this.#stateChangeScheduled = false;
      if (!this.#closed) this.#onStateChange?.();
    });
  }

  #observeSidecarLiveness(envelope: AwarenessEnvelope<SidecarFrame>): void {
    if (this.#closed || this.#protocolFailed || !validBinding(envelope, this.#binding)) return;
    if (
      this.#binding.protocolRevision === "r2" &&
      (envelope as R2AwarenessEnvelope<SidecarFrame>).targetInstanceId !== this.#hostInstanceId
    ) return;
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

  #ownsLifecycle(epoch: number): boolean {
    return epoch === this.#lifecycleEpoch && !this.#closed && !this.#protocolFailed;
  }

  async #joinProcessingTail(): Promise<void> {
    for (;;) {
      const tail = this.#processing;
      await tail.catch(() => undefined);
      if (tail === this.#processing) return;
    }
  }

  readonly #onAwarenessUpdate = (
    update: AwarenessPeerUpdate<SidecarFrame>,
  ): void => {
    if (this.#closed || this.#protocolFailed) return;
    const lifecycleEpoch = this.#lifecycleEpoch;
    // Liveness must not queue behind a response frame waiting for client-side
    // buffer space. Only the reliable data path remains serialized below.
    this.#observeSidecarLiveness(update.envelope);
    this.#processing = this.#processing
      .then(() => this.#ownsLifecycle(lifecycleEpoch)
        ? this.#processEnvelope(update.envelope, lifecycleEpoch)
        : undefined)
      .catch(error => {
        if (this.#ownsLifecycle(lifecycleEpoch)) this.#failProtocol(error);
      });
  };

  async #processEnvelope(
    envelope: AwarenessEnvelope<SidecarFrame>,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    if (!validBinding(envelope, this.#binding)) {
      throw new Error("Sidecar Awareness binding is invalid");
    }
    if (
      this.#binding.protocolRevision === "r2" &&
      (envelope as R2AwarenessEnvelope<SidecarFrame>).targetInstanceId !== this.#hostInstanceId
    ) return;
    const lane: AwarenessLane = envelope.lane ?? "data";
    if (
      this.#expectedSidecarInstanceId &&
      envelope.instanceId !== this.#expectedSidecarInstanceId
    ) {
      if (this.#binding.protocolRevision === "r2") return;
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
        if (this.#hasExecutionRisk(state)) {
          this.#failCommitted(state, new V2CommittedError(message, "instance_changed", true));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(message, "instance_changed"));
        }
      }
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#flight.reset(nextEpoch);
      this.#resetFlightMeasurement();
      this.#channel.clear("data");
      this.#dataDirty = false;
      this.#sidecarRevision.data = -1;
      this.#sidecarRevision.control = -1;
      replaced = true;
    }
    if (envelope.revision <= this.#sidecarRevision[lane]) return;
    this.#sidecarInstanceId = envelope.instanceId;
    this.#sidecarRevision[lane] = envelope.revision;
    let creditsChanged = false;
    if (lane === "control" || legacy) {
      this.#sidecarDiagnostics = envelope.diagnostics ?? null;
      const nextCredits = Math.max(0, Math.min(128, envelope.credits));
      creditsChanged = nextCredits !== this.#remoteCredits;
      this.#remoteCredits = nextCredits;
    }

    this.#acknowledgeOutbound(envelope.ack);
    const flightCompleted = this.#flight.acknowledged(envelope.ack);
    const now = this.#clock.now();
    let goodputChanged = false;
    if (flightCompleted) {
      this.#flightCompletions++;
      goodputChanged = this.#recordAcknowledgedFlight(now);
    }
    const acknowledgementAdvanced = this.#delivery.acknowledged(
      envelope.ack,
      this.#outbound.snapshot().frames.length > 0,
      now,
    );
    if (acknowledgementAdvanced) {
      this.#lastFastRetransmitAtMs = null;
      this.#armRetransmission();
    }
    const congestionChanged = this.#updateCongestion();
    if (creditsChanged || congestionChanged || goodputChanged) this.#notifyStateChange();
    if (!acknowledgementAdvanced && lane === "control") {
      this.#maybeFastRetransmit(now);
    }
    let delivered: readonly ReliableFrame<SidecarFrame>[] = [];
    let replayProcessedAck = false;
    if (lane === "data") {
      const currentEpoch = this.#inbound.ack().epoch;
      if (envelope.epoch > currentEpoch) {
        this.#inbound.reset(envelope.epoch);
        this.#processedInboundAck = { epoch: envelope.epoch, sequence: -1 };
      }
      const envelopeHigh = envelope.frames.reduce(
        (high, frame) => frame.epoch === this.#processedInboundAck.epoch
          ? Math.max(high, frame.sequence)
          : high,
        -1,
      );
      delivered = this.#inbound.accept(envelope.frames);
      for (const frame of delivered) {
        await this.#processFrame(frame, lifecycleEpoch);
        if (!this.#ownsLifecycle(lifecycleEpoch)) return;
        this.#processedInboundAck = { epoch: frame.epoch, sequence: frame.sequence };
      }
      if (
        delivered.length === 0 &&
        envelopeHigh >= 0 &&
        envelopeHigh <= this.#processedInboundAck.sequence
      ) {
        // A newer publication carrying an already-processed immutable flight
        // proves the peer missed our ACK-only state. Replay the current ACK
        // immediately instead of making it climb the retransmission backoff.
        replayProcessedAck = true;
      }
    }
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    this.#maybeCompleteSidecarRecovery();
    this.#admitPending();
    const urgency = this.#pump();
    if (flightCompleted && urgency === null) this.#channel.clear("data");
    this.#requestPublication(
      replaced || delivered.length > 0 || replayProcessedAck ? "urgent" : urgency,
    );
    this.#armStallWatchdog();
  }

  async #processFrame(
    frame: ReliableFrame<SidecarFrame>,
    lifecycleEpoch: number,
  ): Promise<void> {
    switch (frame.kind) {
      case "request_prepared": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (this.#binding.protocolRevision !== "r2") {
          this.#failPreCommit(state, new V2PreCommitError(
            "R1 sidecar emitted an R2 preparation proof",
            "protocol",
          ));
          return;
        }
        if (!this.#preparedProofMatches(state, frame) || !state.commitPublished) {
          const error = "Sidecar preparation proof does not match the committed request";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(error, "prepared_fence", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(error, "prepared_fence"));
          }
          return;
        }
        if (state.startQueued || state.startRisk) return;
        state.startQueued = true;
        try {
          this.#scheduler.enqueue(
            "commit",
            state.requestId,
            this.#requestStartFromPrepared(frame),
          );
        } catch (cause) {
          state.startQueued = false;
          this.#failPreCommit(state, new V2PreCommitError(
            cause instanceof Error ? cause.message : "R2 request start could not be staged",
            "start_stage",
          ));
        }
        return;
      }
      case "upstream_prelude": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (!state.committed) {
          const message = "Sidecar emitted upstream headers before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        const prelude = {
          status: frame.status,
          contentType: frame.contentType,
          upstreamRequestId: frame.upstreamRequestId,
        };
        if (state.upstreamPrelude) {
          if (
            state.upstreamPrelude.status !== prelude.status ||
            !equivalentEventStreamContentType(
              state.upstreamPrelude.contentType,
              prelude.contentType,
            ) ||
            (
              state.upstreamPrelude.upstreamRequestId !== prelude.upstreamRequestId &&
              !(state.recoveryPrefix && prelude.upstreamRequestId === null)
            )
          ) {
            this.#failCommitted(state, new V2CommittedError(
              "Sidecar emitted conflicting upstream headers",
              "prelude_conflict",
              true,
            ));
          }
          return;
        }
        state.upstreamPrelude = prelude;
        state.handle.acceptUpstreamPrelude(prelude);
        return;
      }
      case "request_ack": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (frame.requestDigest !== state.requestDigest) {
          if (this.#binding.protocolRevision === "r2") state.startRisk = true;
          if (this.#hasExecutionRisk(state)) {
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
        if (this.#binding.protocolRevision === "r2" && !state.startRisk) {
          state.startRisk = true;
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar claimed fetch-started commitment before request_start",
            "ack_before_start",
            true,
          ));
          return;
        }
        if (state.committed) return;
        state.committed = true;
        state.diagnostics.committed(frame.diagnostics);
        this.#creditReservations = Math.max(0, this.#creditReservations - 1);
        this.#transferReservations = Math.max(
          0,
          this.#transferReservations - state.transferCost,
        );
        state.resolveRun(state.handle);
        return;
      }
      case "request_reject": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (state.committed || (
          this.#binding.protocolRevision === "r2" && state.startRisk
        )) {
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
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted response data before upstream headers",
            "missing_prelude",
            true,
          ));
          return;
        }
        if (this.#consumeRecoveryPrefixPart(state, frame)) return;
        if (!state.firstResponseSeen) { state.firstResponseSeen = true; state.diagnostics.firstFrame(frame.diagnostics); }
        await this.#acceptResponsePart(state, frame, lifecycleEpoch);
        return;
      }
      case "response_batch": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (!state.committed) {
          const message = "Sidecar emitted response data before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted response data before upstream headers",
            "missing_prelude",
            true,
          ));
          return;
        }
        for (let offset = 0; offset < frame.items.length; offset++) {
          const eventIndex = frame.firstEventIndex + offset;
          const item = frame.items[offset]!;
          if (this.#consumeRecoveryPrefixWhole(
            state,
            eventIndex,
            item.event,
            item.dataBase64Url,
          )) {
            if (!this.#requests.has(state.requestId)) return;
            continue;
          }
          if (!this.#acceptWholeEncodedEvent(
            state,
            eventIndex,
            item.event,
            item.dataBase64Url,
          )) return;
        }
        await this.#drainCompletedEvents(state, lifecycleEpoch);
        return;
      }
      case "response_terminal": {
        const state = this.#requests.get(frame.requestId);
        if (!state) {
          const receipt = this.#terminalReceipts.get(frame.requestId);
          if (!receipt || !this.#sidecarRecovery) return;
          if (!this.#terminalMatchesReceipt(frame, receipt.terminal)) {
            this.failSidecarRecovery(
              this.#sidecarRecovery.token,
              new V2CommittedError(
                "Recovered terminal conflicts with its retained receipt",
                "terminal_receipt_conflict",
                true,
              ),
            );
            return;
          }
          receipt.replayed = true;
          this.#queueTerminalReceipt(frame.requestId, receipt);
          return;
        }
        if (!state.committed) {
          const message = "Sidecar emitted a terminal before commitment";
          if (this.#hasExecutionRisk(state)) {
            this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
          } else {
            this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
          }
          return;
        }
        if (this.#binding.protocolRevision === "r2" && !state.upstreamPrelude) {
          this.#failCommitted(state, new V2CommittedError(
            "Sidecar emitted terminal proof before upstream headers",
            "missing_prelude",
            true,
          ));
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
        await this.#tryFinalize(state, lifecycleEpoch);
        return;
      }
      case "response_error": {
        const state = this.#requests.get(frame.requestId);
        if (!state) return;
        if (state.committed || (
          this.#binding.protocolRevision === "r2" && state.startRisk
        )) {
          this.#failCommitted(state, new V2CommittedError(
            frame.message,
            frame.code,
            frame.ambiguous,
            frame.rateLimit,
            frame.providerError,
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
        }
        return;
      }
      case "request_cancelled": {
        const state = this.#requests.get(frame.requestId);
        if (state && this.#sidecarRecovery && !state.cancelled) {
          this.#failCommitted(state, new V2CommittedError(
            "Recovered request was durably cancelled",
            "cancelled",
            false,
          ));
          this.#maybeCompleteSidecarRecovery();
          return;
        }
        if (!state?.cancelled) return;
        const releaseQuarantine = state.quarantineKind === "cancellation";
        this.#removeState(state);
        if (releaseQuarantine) {
          this.#quarantined = Math.max(0, this.#quarantined - 1);
          state.quarantineKind = null;
        }
        return;
      }
      case "recovery_complete":
        if (this.#sidecarRecovery) {
          this.#sidecarRecovery.markerSeen = true;
          this.#maybeCompleteSidecarRecovery();
        }
        return;
      case "drained":
        if (frame.active === 0 && frame.queued === 0) {
          this.#remoteDrained = true;
          this.#maybeResolveDrainWaiters();
        }
        return;
      case "credential_ack": {
        const waiter = this.#rotations.get(frame.rotationId);
        if (!waiter) return;
        this.#rotations.delete(frame.rotationId);
        this.#removeRotationStream(frame.rotationId);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        if (waiter.timer) clearTimeout(waiter.timer);
        waiter.resolve();
        return;
      }
      case "pong":
        return;
    }
  }

  #hasExecutionRisk(state: RequestState): boolean {
    if (state.committed) return true;
    return this.#binding.protocolRevision === "r2"
      ? state.startRisk
      : state.commitPublished;
  }

  #consumeRecoveryPrefixWhole(
    state: RequestState,
    eventIndex: number,
    event: string,
    encoded: string,
  ): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix || eventIndex >= prefix.expectedEventCount) return false;
    if (prefix.segmented || eventIndex !== prefix.nextEventIndex) {
      this.#failRecoveryPrefix(state, "Recovered response prefix order conflicts");
      return true;
    }
    let bytes: Uint8Array;
    try { bytes = base64UrlDecode(encoded); } catch {
      this.#failRecoveryPrefix(state, "Recovered response prefix encoding is invalid");
      return true;
    }
    this.#appendRecoveryPrefixBytes(prefix, event, bytes);
    this.#finishRecoveryPrefixIfReady(state, false);
    return true;
  }

  #consumeRecoveryPrefixPart(
    state: RequestState,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
  ): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix || frame.eventIndex >= prefix.expectedEventCount) return false;
    if (frame.eventIndex !== prefix.nextEventIndex) {
      this.#failRecoveryPrefix(state, "Recovered response prefix order conflicts");
      return true;
    }
    let segmented = prefix.segmented;
    if (!segmented) {
      if (frame.partIndex !== 0 || frame.offset !== 0) {
        this.#failRecoveryPrefix(state, "Recovered response prefix begins with a gap");
        return true;
      }
      segmented = prefix.segmented = {
        event: frame.event,
        partCount: frame.partCount,
        nextPartIndex: 0,
        nextOffset: 0,
      };
      const header = utf8Bytes(`event: ${frame.event}\ndata: `);
      prefix.hasher.update(header);
      prefix.hashedByteCount += header.byteLength;
    }
    if (
      segmented.event !== frame.event ||
      segmented.partCount !== frame.partCount ||
      frame.partIndex !== segmented.nextPartIndex ||
      frame.offset !== segmented.nextOffset
    ) {
      this.#failRecoveryPrefix(state, "Recovered response prefix parts conflict");
      return true;
    }
    let bytes: Uint8Array;
    try { bytes = base64UrlDecode(frame.dataBase64Url); } catch {
      this.#failRecoveryPrefix(state, "Recovered response prefix encoding is invalid");
      return true;
    }
    prefix.hasher.update(bytes);
    prefix.hashedByteCount += bytes.byteLength;
    segmented.nextPartIndex++;
    segmented.nextOffset += bytes.byteLength;
    if (segmented.nextPartIndex === segmented.partCount) {
      const footer = utf8Bytes("\n\n");
      prefix.hasher.update(footer);
      prefix.hashedByteCount += footer.byteLength;
      prefix.hashedEventCount++;
      prefix.nextEventIndex++;
      prefix.segmented = null;
      this.#finishRecoveryPrefixIfReady(state, false);
    }
    return true;
  }

  #appendRecoveryPrefixBytes(
    prefix: RecoveryPrefix,
    event: string,
    bytes: Uint8Array,
  ): void {
    const header = utf8Bytes(`event: ${event}\ndata: `);
    const footer = utf8Bytes("\n\n");
    prefix.hasher.update(header).update(bytes).update(footer);
    prefix.hashedByteCount += header.byteLength + bytes.byteLength + footer.byteLength;
    prefix.hashedEventCount++;
    prefix.nextEventIndex++;
  }

  #failRecoveryPrefix(state: RequestState, message: string): void {
    this.#failCommitted(state, new V2CommittedError(
      message,
      "recovery_prefix_mismatch",
      true,
    ));
  }

  #preparedProofMatches(
    state: RequestState,
    frame: ReliableFrame<R2RequestPreparedFrame>,
  ): boolean {
    const manifest = state.manifest;
    return frame.protocolRevision === "r2" &&
      frame.schemaVersion === GP_V2_R2_SCHEMA_VERSION &&
      frame.projectId === this.#binding.projectId &&
      frame.sessionId === this.#binding.sessionId &&
      frame.leaderGeneration === this.#binding.leaderGeneration &&
      frame.buildHash === this.#binding.buildHash &&
      frame.schemaHash === this.#binding.schemaHash &&
      frame.launchNonce === this.#binding.launchNonce &&
      frame.hostInstanceId === this.#hostInstanceId &&
      frame.targetInstanceId === this.#expectedSidecarInstanceId &&
      frame.requestDigest === manifest.requestDigest &&
      frame.decodedBytes === manifest.decodedBytes &&
      frame.encodedBytes === manifest.encodedBytes &&
      frame.encoding === manifest.encoding &&
      frame.partCount === manifest.partCount;
  }

  #requestStartFromPrepared(
    frame: ReliableFrame<R2RequestPreparedFrame>,
  ): R2RequestStartFrame {
    return {
      kind: "request_start",
      requestId: frame.requestId,
      protocolRevision: "r2",
      schemaVersion: GP_V2_R2_SCHEMA_VERSION,
      projectId: frame.projectId,
      sessionId: frame.sessionId,
      leaderGeneration: frame.leaderGeneration,
      buildHash: frame.buildHash,
      schemaHash: frame.schemaHash,
      launchNonce: frame.launchNonce,
      hostInstanceId: frame.hostInstanceId,
      targetInstanceId: frame.targetInstanceId,
      requestDigest: frame.requestDigest,
      decodedBytes: frame.decodedBytes,
      encodedBytes: frame.encodedBytes,
      encoding: frame.encoding,
      partCount: frame.partCount,
    };
  }

  #tryAcquireEventPermit(
    logicalBytes: number,
    label: string,
  ): AtomicBytePermit | null | undefined {
    if (!this.#eventMemoryPool) return undefined;
    try {
      return this.#eventMemoryPool.tryAcquire(
        estimateAtomicEventPermitBytes(logicalBytes),
        {
          region: logicalBytes > this.#maxBufferedResponseBytes ? "jumbo" : "auto",
          label,
        },
      );
    } catch {
      return null;
    }
  }

  #tryResizeEventPermit(
    permit: AtomicBytePermit | null,
    logicalBytes: number,
  ): boolean {
    if (!this.#eventMemoryPool) return true;
    if (!permit) return false;
    try {
      const targetBytes = estimateAtomicEventPermitBytes(logicalBytes);
      // RunHandle deliberately admits an event above its soft queue bound only
      // while that event owns the single jumbo lane. A normal resize prefers
      // spare ordinary capacity, so force the region transition before the
      // permit is transferred even when ordinary growth would still fit.
      if (
        logicalBytes > this.#maxBufferedResponseBytes &&
        permit.region !== "jumbo"
      ) return permit.tryPromote(targetBytes);
      return permit.tryResize(targetBytes, {
        allowJumbo: true,
      });
    } catch {
      return false;
    }
  }

  #tryRetainEventPermit(
    permit: AtomicBytePermit | null,
    wireBytes: number,
  ): boolean {
    if (!this.#eventMemoryPool) return true;
    if (!permit) return false;
    try {
      // Jumbo ownership is a delivery invariant, not merely a capacity
      // fallback. Shrinking must never demote an above-soft event.
      if (
        wireBytes > this.#maxBufferedResponseBytes &&
        permit.region !== "jumbo"
      ) return false;
      permit.shrink(v2RetainedEventPermitBytes(wireBytes));
      return true;
    } catch {
      return false;
    }
  }

  #failEventMemory(state: RequestState, message: string): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#failCommitted(state, new V2CommittedError(
      message,
      "event_memory_capacity",
      true,
    ));
    this.#scheduler.enqueue("control", state.requestId, {
      kind: "request_cancel",
      requestId: state.requestId,
      reason: "host event memory unavailable",
    });
    this.#requestPublication(this.#pump());
  }

  #acceptWholeEncodedEvent(
    state: RequestState,
    eventIndex: number,
    event: string,
    encoded: string,
  ): boolean {
    const recoveryPrefix = state.recoveryPrefix;
    const nextEventIndex = recoveryPrefix
      ? recoveryPrefix.nextEventIndex
      : state.nextEventIndex;
    const decodedLength = base64UrlDecodedBytes(encoded);
    if (decodedLength === null) {
      this.#failCommitted(state, new V2CommittedError(
        "Response batch encoding is invalid",
        "response_encoding",
        true,
      ));
      return false;
    }
    let permit = this.#tryAcquireEventPermit(
      partialEventLogicalBytes(event, decodedLength),
      `host-response:${state.requestId}`,
    );
    if (permit === null) {
      this.#failEventMemory(state, "Host event memory is unavailable");
      return false;
    }
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(encoded, decodedLength);
    } catch {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch encoding is invalid",
        "response_encoding",
        true,
      ));
      return false;
    }
    const wireBytes = canonicalEventWireBytes(event, [bytes]);
    if (
      (!recoveryPrefix || eventIndex >= recoveryPrefix.expectedEventCount) &&
        state.responseBytes + wireBytes > this.#maxResponseBytes ||
      !this.#tryResizeEventPermit(permit ?? null, wireBytes)
    ) {
      permit?.release();
      if (
        (!recoveryPrefix || eventIndex >= recoveryPrefix.expectedEventCount) &&
        state.responseBytes + wireBytes > this.#maxResponseBytes
      ) {
        this.#failCommitted(state, new V2CommittedError(
          "Response exceeds the configured canonical byte limit",
          "response_too_large",
          true,
        ));
      } else {
        this.#failEventMemory(state, "Host event memory cannot grow without blocking");
      }
      return false;
    }
    let data: string;
    try {
      data = utf8Text(bytes);
    } catch {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch event is not valid UTF-8",
        "response_encoding",
        true,
      ));
      return false;
    }
    if (
      eventIndex < nextEventIndex ||
      state.completedEvents.has(eventIndex) ||
      state.assemblies.has(eventIndex)
    ) {
      permit?.release();
      this.#failCommitted(state, new V2CommittedError(
        "Response batch event indexes conflict",
        "response_conflict",
        true,
      ));
      return false;
    }
    if (!this.#tryRetainEventPermit(permit ?? null, wireBytes)) {
      permit?.release();
      this.#failEventMemory(state, "Host event memory cannot retain the completed event");
      return false;
    }
    if (!recoveryPrefix || eventIndex >= recoveryPrefix.expectedEventCount) {
      state.responseBytes += wireBytes;
    }
    state.completedEvents.set(eventIndex, {
      frame: { event, data },
      wireBytes,
      permit: permit ?? null,
    });
    return true;
  }

  async #acceptResponsePart(
    state: RequestState,
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_event" }>,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch)) return;
    const recoveryPrefix = state.recoveryPrefix;
    const nextEventIndex = recoveryPrefix
      ? recoveryPrefix.nextEventIndex
      : state.nextEventIndex;
    if (
      frame.eventIndex < nextEventIndex ||
      state.completedEvents.has(frame.eventIndex)
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event index conflicts with completed output",
        "response_conflict",
        true,
      ));
      return;
    }
    const decodedLength = base64UrlDecodedBytes(frame.dataBase64Url);
    if (decodedLength === null) {
      this.#failCommitted(state, new V2CommittedError(
        "Response event part is not valid base64url",
        "response_encoding",
        true,
      ));
      return;
    }
    let assembly = state.assemblies.get(frame.eventIndex);
    if (!assembly) {
      const permit = this.#tryAcquireEventPermit(
        partialEventLogicalBytes(frame.event, decodedLength),
        `host-response:${state.requestId}`,
      );
      if (permit === null) {
        this.#failEventMemory(state, "Host event memory is unavailable");
        return;
      }
      assembly = {
        event: frame.event,
        partCount: frame.partCount,
        parts: new Map(),
        rawBytes: 0,
        permit: permit ?? null,
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
    if (!existing && !this.#tryResizeEventPermit(
      assembly.permit,
      partialEventLogicalBytes(assembly.event, assembly.rawBytes + decodedLength),
    )) {
      this.#failEventMemory(state, "Host event memory cannot grow without blocking");
      return;
    }
    let bytes: Uint8Array;
    try {
      bytes = decodeCanonicalBase64Url(frame.dataBase64Url, decodedLength);
    } catch {
      this.#failCommitted(state, new V2CommittedError(
        "Response event part is not valid base64url",
        "response_encoding",
        true,
      ));
      return;
    }
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
    assembly.parts.set(frame.partIndex, { offset: frame.offset, bytes });
    assembly.rawBytes += bytes.byteLength;
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
    const wireBytes = canonicalEventWireBytes(assembly.event, parts);
    if (
      (!recoveryPrefix || frame.eventIndex >= recoveryPrefix.expectedEventCount) &&
      state.responseBytes + wireBytes > this.#maxResponseBytes
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Response exceeds the configured canonical byte limit",
        "response_too_large",
        true,
      ));
      return;
    }
    if (!this.#tryResizeEventPermit(assembly.permit, wireBytes)) {
      this.#failEventMemory(state, "Host event memory cannot materialize the complete event");
      return;
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
    assembly.parts.clear();
    parts.length = 0;
    if (!this.#tryRetainEventPermit(assembly.permit, wireBytes)) {
      this.#failEventMemory(state, "Host event memory cannot retain the completed event");
      return;
    }
    state.assemblies.delete(frame.eventIndex);
    if (!recoveryPrefix || frame.eventIndex >= recoveryPrefix.expectedEventCount) {
      state.responseBytes += wireBytes;
    }
    state.completedEvents.set(frame.eventIndex, {
      frame: { event: frame.event, data },
      wireBytes,
      permit: assembly.permit,
    });
    assembly.permit = null;
    await this.#drainCompletedEvents(state, lifecycleEpoch);
  }

  async #drainCompletedEvents(
    state: RequestState,
    lifecycleEpoch: number,
  ): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch) || !this.#requests.has(state.requestId)) return;
    if (!this.#finishRecoveryPrefixIfReady(state, false)) return;
    for (;;) {
      const completed = state.completedEvents.get(state.nextEventIndex);
      if (!completed) break;
      let wireBytes: number;
      try {
        // Batched response events are normally small. Hash their exact-size
        // canonical buffer instead of TranscriptProof.append()'s 64 KiB
        // general-purpose scratch allocation per logical event. Jumbo events
        // retain the existing fixed-memory synchronous path; making delivery
        // await timer-based transcript yields would change the serialized ACK
        // boundary and can strand an unref-only test or shutdown lifecycle.
        wireBytes = completed.wireBytes <= MAX_TRANSCRIPT_CHUNK_BYTES
          ? state.transcript.appendCanonical(completed.frame).byteLength
          : state.transcript.append(completed.frame);
      } catch (cause) {
        // Keep the entry map-owned until failure cleanup releases its permit.
        // Transcript validation (notably terminal JSON parsing) may throw
        // synchronously after the event has been fully materialized.
        this.#failCommitted(state, new V2CommittedError(
          cause instanceof Error ? cause.message : "Response transcript validation failed",
          "response_transcript",
          true,
        ));
        return;
      }
      if (wireBytes !== completed.wireBytes) {
        this.#failCommitted(state, new V2CommittedError(
          "Canonical response byte accounting diverged",
          "response_size_mismatch",
          true,
        ));
        return;
      }
      if (state.cancelled) {
        state.completedEvents.delete(state.nextEventIndex);
        state.nextEventIndex++;
        completed.permit?.release();
        completed.permit = null;
        continue;
      }
      let permit: AtomicBytePermit | undefined;
      try {
        permit = completed.permit?.transfer("host-run-handle-event");
      } catch (cause) {
        this.#failCommitted(state, new V2CommittedError(
          cause instanceof Error ? cause.message : "Host event memory transfer failed",
          "event_memory_delivery",
          true,
        ));
        return;
      }
      completed.permit = null;
      state.completedEvents.delete(state.nextEventIndex);
      state.nextEventIndex++;
      const started = this.#clock.now();
      try {
        // Backpressure the reliable data lane instead of converting a short
        // consumer pause into an ambiguous committed failure. While this
        // awaits, the inbound cumulative ACK intentionally does not advance,
        // so the sidecar's bounded reliable window and output backpressure
        // stop upstream consumption. Control-lane liveness is observed before
        // this serialized data path and therefore remains responsive.
        await state.handle.acceptEvent(completed.frame, wireBytes, permit);
        permit = undefined;
      } catch (error) {
        // V2RunHandle releases a transferred permit on rejection. A client
        // abort may close the stream while delivery is waiting for space; the
        // cancellation path owns that outcome and must not become ambiguity.
        permit = undefined;
        if (
          !this.#ownsLifecycle(lifecycleEpoch) ||
          !this.#requests.has(state.requestId) ||
          state.cancelled
        ) return;
        this.#failCommitted(state, new V2CommittedError(
          error instanceof Error ? error.message : "Host event delivery failed",
          "event_memory_delivery",
          true,
        ));
        return;
      }
      state.diagnostics.assemblyDelivered(this.#clock.now() - started);
    }
    await this.#tryFinalize(state, lifecycleEpoch);
  }

  #finishRecoveryPrefixIfReady(state: RequestState, terminalSeen: boolean): boolean {
    const prefix = state.recoveryPrefix;
    if (!prefix) return true;
    if (prefix.nextEventIndex < prefix.expectedEventCount) {
      if (terminalSeen) {
        this.#failCommitted(state, new V2CommittedError(
          "Recovered terminal omitted part of the previously delivered response prefix",
          "recovery_prefix_missing",
          true,
        ));
      }
      return false;
    }
    if (
      prefix.hashedEventCount !== prefix.expectedEventCount ||
      prefix.hashedByteCount !== prefix.expectedByteCount ||
      prefix.hasher.digestHex() !== prefix.expectedDigest ||
      prefix.segmented !== null
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Recovered response prefix does not match the original transcript",
        "recovery_prefix_mismatch",
        true,
      ));
      return false;
    }
    state.recoveryPrefix = undefined;
    return true;
  }

  async #tryFinalize(state: RequestState, lifecycleEpoch: number): Promise<void> {
    if (!this.#ownsLifecycle(lifecycleEpoch) || !this.#requests.has(state.requestId)) return;
    const terminal = state.terminal;
    if (!terminal) return;
    // A terminal may arrive before one or more response frames. Check the
    // cheap counters first: snapshot() materializes the entire transcript and
    // doing that after every newly completed event is quadratic.
    const eventCount = state.transcript.eventCount;
    if (eventCount > terminal.eventCount) {
      this.#failCommitted(state, new V2CommittedError(
        "Response contains more events than its terminal proof",
        "terminal_count",
        true,
      ));
      return;
    }
    if (eventCount < terminal.eventCount) return;
    if (state.assemblies.size > 0 || state.completedEvents.size > 0) return;
    const snapshot = state.transcript.snapshot();
    let final: Record<string, unknown>;
    try {
      final = await validateTerminalProof(terminal, snapshot);
    } catch (cause) {
      if (
        !this.#ownsLifecycle(lifecycleEpoch) ||
        this.#requests.get(state.requestId) !== state
      ) return;
      this.#failCommitted(state, new V2CommittedError(
        cause instanceof Error ? cause.message : "Terminal proof validation failed",
        "terminal_proof",
        true,
      ));
      return;
    }
    if (
      !this.#ownsLifecycle(lifecycleEpoch) ||
      this.#requests.get(state.requestId) !== state ||
      state.terminal !== terminal
    ) return;
    if (
      !this.#terminalReceipts.has(state.requestId) &&
      this.#terminalReceipts.size >= MAX_TERMINAL_RECEIPTS
    ) {
      this.#failCommitted(state, new V2CommittedError(
        "Host terminal receipt capacity is exhausted",
        "terminal_receipt_capacity",
        true,
      ));
      return;
    }
    const wasCancelled = state.cancelled;
    const wasQuarantined = state.quarantineKind === "cancellation";
    // Even a client-cancelled request owns a durable terminal spool. Suppress
    // client delivery, but retain and ACK its verified terminal so recovery
    // cannot replay that spool forever.
    const receipt: TerminalReceipt = {
      terminal: { ...terminal },
      queued: false,
      replayed: this.#sidecarRecovery !== null,
      cursor: null,
    };
    this.#terminalReceipts.set(state.requestId, receipt);
    this.#removeState(state);
    if (!wasCancelled) state.handle.settle(final, terminal.rateLimit);
    if (wasQuarantined) {
      this.#quarantined = Math.max(0, this.#quarantined - 1);
      state.quarantineKind = null;
    }
    this.#queueTerminalReceipt(state.requestId, receipt);
    this.#requestPublication(this.#pump());
  }

  #queueTerminalReceipt(requestId: string, receipt: TerminalReceipt): void {
    if (receipt.queued) return;
    this.#scheduler.enqueue("control", requestId, {
      kind: "response_ack",
      requestId,
      terminalDigest: receipt.terminal.transcriptSha256,
    });
    receipt.queued = true;
  }

  #terminalMatchesReceipt(
    frame: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }>,
    expected: Extract<SidecarFrame, { kind: "response_terminal" }>,
  ): boolean {
    return frame.requestId === expected.requestId &&
      frame.requestDigest === expected.requestDigest &&
      frame.responseId === expected.responseId &&
      frame.eventCount === expected.eventCount &&
      frame.byteCount === expected.byteCount &&
      frame.transcriptSha256 === expected.transcriptSha256 &&
      frame.terminalStatus === expected.terminalStatus &&
      frame.finalEventIndex === expected.finalEventIndex &&
      frame.finalSha256 === expected.finalSha256;
  }

  #acknowledgeOutbound(cursor: { epoch: number; sequence: number }): void {
    this.#outbound.acknowledge(cursor);
    for (const [requestId, receipt] of this.#terminalReceipts) {
      if (
        receipt.cursor?.epoch === cursor.epoch &&
        receipt.cursor.sequence <= cursor.sequence
      ) this.#terminalReceipts.delete(requestId);
    }
    this.#maybeResolveDrainWaiters();
    this.#maybeCompleteSidecarRecovery();
  }

  async #prepareRequest(
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<PreparedRequestPlan> {
    // The sidecar forwards these proven canonical bytes directly to fetch to
    // avoid a decoded-string + parsed-object + reserialized-string peak. Force
    // streaming here while the outer HTTP handler still owns client semantics.
    const upstreamBody = body.stream === true ? body : { ...body, stream: true };
    const decoded = utf8Bytes(canonicalJson(upstreamBody));
    if (decoded.byteLength < 1 || decoded.byteLength > this.#limits.requestMaxBytes) {
      throw new V2PreCommitError("V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const requestDigest = await sha256Hex(decoded);
    const partBytes = this.#binding.protocolRevision === "r2"
      ? R2_REQUEST_PART_BYTES
      : R1_REQUEST_PART_BYTES;
    let encoded = decoded;
    let encoding: RequestEncoding = "identity";
    if (
      this.#gzip &&
      decoded.byteLength > partBytes &&
      !containsLargeInlineBase64(body)
    ) {
      const sampleLength = Math.min(decoded.byteLength, GZIP_SAMPLE_BYTES);
      const sample = new Uint8Array(sampleLength);
      const firstLength = Math.ceil(sampleLength / 2);
      sample.set(decoded.subarray(0, firstLength), 0);
      sample.set(
        decoded.subarray(decoded.byteLength - (sampleLength - firstLength)),
        firstLength,
      );
      const sampleCompressed = await this.#gzip(sample, {
        signal,
        inputOwnership: "owned",
      });
      const sampleSavings = sampleLength - sampleCompressed.byteLength;
      const sampleWorthwhile = sampleCompressed.byteLength > 0 &&
        sampleSavings >= Math.ceil(sampleLength * MIN_GZIP_SAVINGS_RATIO);
      // Compression owns this scratch copy, so a worker pool may transfer its
      // complete ArrayBuffer without detaching the canonical request bytes.
      const compressed = !sampleWorthwhile
        ? null
        : sampleLength === decoded.byteLength
          ? sampleCompressed
          : await this.#gzip(decoded.slice(), {
              signal,
              inputOwnership: "owned",
            });
      const identityParts = Math.ceil(decoded.byteLength / partBytes);
      const compressedParts = compressed && compressed.byteLength > 0
        ? Math.ceil(compressed.byteLength / partBytes)
        : Number.POSITIVE_INFINITY;
      const savedBytes = compressed ? decoded.byteLength - compressed.byteLength : 0;
      if (
        compressedParts < identityParts &&
        savedBytes >= partBytes &&
        savedBytes >= Math.ceil(decoded.byteLength * MIN_GZIP_SAVINGS_RATIO)
      ) {
        encoded = compressed!;
        encoding = "gzip+base64";
      }
    }
    if (encoded.byteLength < 1 || encoded.byteLength > this.#limits.requestMaxBytes) {
      throw new V2PreCommitError("Encoded V2 request exceeds the 64 MiB limit", "request_too_large");
    }
    const partCount = Math.ceil(encoded.byteLength / partBytes);
    return {
      requestDigest,
      decodedBytes: decoded.byteLength,
      encodedBytes: encoded.byteLength,
      encoding,
      partCount,
      partBytes,
      encodedPayload: encoded,
      nextPartIndex: 0,
      stage: "open",
    };
  }

  #takePreparedFrame(
    requestId: string,
    plan: PreparedRequestPlan,
  ): HostFrame | null {
    if (plan.stage === "done") return null;
    if (plan.stage === "open") {
      plan.stage = "data";
      return {
        kind: "request_open",
        requestId,
        requestDigest: plan.requestDigest,
        decodedBytes: plan.decodedBytes,
        encodedBytes: plan.encodedBytes,
        encoding: plan.encoding,
        partCount: plan.partCount,
      };
    }
    if (plan.stage === "data") {
      const encoded = plan.encodedPayload;
      if (!encoded || plan.nextPartIndex >= plan.partCount) {
        throw new Error("Prepared V2 request data state is invalid");
      }
      const partIndex = plan.nextPartIndex++;
      const offset = partIndex * plan.partBytes;
      const frame: HostFrame = {
        kind: "request_data",
        requestId,
        partIndex,
        offset,
        dataBase64Url: base64UrlEncode(
          encoded.subarray(offset, Math.min(encoded.byteLength, offset + plan.partBytes)),
        ),
      };
      if (plan.nextPartIndex === plan.partCount) {
        // Every byte now lives only in a scheduled/outbound physical frame.
        // Release the compact source before the upstream response starts.
        plan.encodedPayload = null;
        plan.stage = "commit";
      }
      return frame;
    }
    plan.stage = "done";
    return this.#binding.protocolRevision === "r2"
      ? {
          kind: "request_commit",
          requestId,
          protocolRevision: "r2",
          schemaVersion: GP_V2_R2_SCHEMA_VERSION,
          projectId: this.#binding.projectId,
          sessionId: this.#binding.sessionId,
          leaderGeneration: this.#binding.leaderGeneration,
          buildHash: this.#binding.buildHash,
          schemaHash: this.#binding.schemaHash,
          launchNonce: this.#binding.launchNonce,
          hostInstanceId: this.#hostInstanceId,
          targetInstanceId: this.#expectedSidecarInstanceId!,
          requestDigest: plan.requestDigest,
          decodedBytes: plan.decodedBytes,
          encodedBytes: plan.encodedBytes,
          encoding: plan.encoding,
          partCount: plan.partCount,
        }
      : { kind: "request_commit", requestId };
  }

  #stageNextRequestFrame(state: RequestState): void {
    const plan = state.preparedRequest;
    if (!plan) return;
    const frame = this.#takePreparedFrame(state.requestId, plan);
    if (!frame) {
      state.preparedRequest = null;
      return;
    }
    // One frame per request is enough for weighted fairness; #pump replenishes
    // it only after the scheduler transfers ownership of the previous frame.
    this.#scheduler.enqueue("data", state.requestId, frame);
    if (plan.stage === "done") state.preparedRequest = null;
  }

  #admitPending(): void {
    while (this.#availableCredits() > 0) {
      const transferAvailable = V2_HOST_TRANSFER_BUDGET - this.#transferReservations;
      const state = this.#pending.find(candidate => (
        !candidate.admitted && candidate.transferCost <= transferAvailable
      ));
      if (!state) return;
      state.admitted = true;
      if (state.admissionTimer) clearTimeout(state.admissionTimer);
      state.admissionTimer = undefined;
      this.#creditReservations++;
      this.#transferReservations += state.transferCost;
      const onAdmissionReflected = state.onAdmissionReflected;
      state.onAdmissionReflected = undefined;
      // The credit is owned by the transport before the router is notified.
      // An observer bug must retain the conservative router reservation rather
      // than unwind an otherwise valid transport admission.
      try { onAdmissionReflected?.(); } catch {}
      this.#stageNextRequestFrame(state);
    }
  }

  #availableCredits(): number {
    if (
      this.#sidecarRecovery || !this.#connected || this.#closed || this.#draining ||
      this.#congestion.congested
    ) return 0;
    return Math.max(0, Math.min(
      this.#remoteCredits - this.#creditReservations,
      this.#hostCeiling - this.#creditReservations - this.#ambiguousQuarantined,
      4 - this.#creditReservations - this.#ambiguousQuarantined,
      V2_HOST_TRANSFER_BUDGET - this.#transferReservations,
    ));
  }

  #pump(): PublicationUrgency | null {
    if (!this.#connected || this.#closed) return null;
    if (this.#flight.active) return null;
    let urgency: PublicationUrgency | null = null;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const frameBudget = Math.min(
        this.#limits.hostFlightMaxFrames - snapshot.frames.length,
        this.#limits.windowMaxFrames - snapshot.frames.length,
      );
      const byteBudget = Math.min(
        this.#limits.hostFlightMaxBytes - snapshot.encodedBytes,
        this.#limits.windowMaxBytes - snapshot.encodedBytes,
      );
      if (frameBudget < 1 || byteBudget < 512) break;
      const next = this.#scheduler.take(1, Math.max(1, byteBudget - 256));
      if (next.length === 0) break;
      const reliable = this.#outbound.push(next[0]!);
      if (reliable.kind === "response_ack") {
        const receipt = this.#terminalReceipts.get(reliable.requestId);
        if (receipt && receipt.terminal.transcriptSha256 === reliable.terminalDigest) {
          receipt.cursor = { epoch: reliable.epoch, sequence: reliable.sequence };
        }
      }
      this.#dataDirty = true;
      urgency = strongestUrgency(urgency, hostFrameUrgency(reliable));
      if ("requestId" in reliable && typeof reliable.requestId === "string") {
        const state = this.#requests.get(reliable.requestId);
        if (state) state.published = true;
        if (state && reliable.kind === "request_commit") state.commitStaged = true;
        if (state && reliable.kind === "request_start") {
          state.startStaged = true;
          state.startRisk = true;
        }
        if (state) state.diagnostics.published();
        if (state && reliable.kind !== "request_commit") {
          this.#stageNextRequestFrame(state);
        }
      }
    }
    return urgency;
  }

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
    this.#armStallWatchdog();
  }

  #publish(reason: PublicationReason): void {
    if (!this.#connected || this.#closed) return;
    try {
      const outbound = this.#outbound.snapshot();
      const publishData = reason === "retransmission" || this.#dataDirty;
      if (publishData) {
        const newFlight = !this.#flight.active;
        if (newFlight) {
          this.#flightStartedAtMs = this.#clock.now();
          this.#flightRequestBytes = outbound.frames
            .filter(hostFrameCarriesRequest)
            .reduce((total, frame) => total + encodedFrameBytes(frame), 0);
        }
        if (reason === "retransmission") {
          this.#retransmittedBytes += outbound.encodedBytes;
        }
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
      this.#armStallWatchdog();
    } catch (cause) {
      if (this.#isReconnectableAwarenessFailure(cause)) {
        this.#remoteCredits = 0;
        return;
      }
      this.#failProtocol(cause);
    }
  }

  #isReconnectableAwarenessFailure(value: unknown): boolean {
    if (!value || typeof value !== "object") return false;
    const error = value as { name?: unknown; kind?: unknown };
    return error.name === "AwarenessChannelError" &&
      (error.kind === "auth" || error.kind === "disconnected");
  }

  #publishLane(
    lane: AwarenessLane,
    frames: readonly ReliableFrame<HostFrame>[],
    reason: PublicationReason,
  ): void {
    const outbound = this.#outbound.snapshot();
    const committed = [...this.#requests.values()].filter(state => state.committed).length;
    const recoveryActive = [...this.#requests.values()]
      .filter(state => this.#hasExecutionRisk(state)).length;
    const common = {
      protocol: "gp-awareness-v2",
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
      credits: this.#sidecarRecovery || this.#congestion.congested ? 0 : this.#hostCeiling,
      active: this.#sidecarRecovery ? Math.max(1, recoveryActive) : committed,
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
    } as const;
    const envelope: AwarenessEnvelope<HostFrame> =
      this.#binding.protocolRevision === "r2"
        ? {
            ...common,
            protocolRevision: "r2",
            schemaVersion: GP_V2_R2_SCHEMA_VERSION,
            launchNonce: this.#binding.launchNonce,
            targetInstanceId: this.#expectedSidecarInstanceId!,
          }
        : {
            ...common,
            schemaVersion: GP_V2_R1_SCHEMA_VERSION,
          };
    this.#publications++;
    if (reason === "paced") this.#pacedPublications++;
    else if (reason === "urgent") this.#urgentPublications++;
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
    if (lane === "data") {
      // R1 crosses its irreversible boundary at request_commit. R2 records its
      // later start-risk before request_start is even staged.
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
      roundTripVariationMs: this.#delivery.roundTripVariationMs,
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
      acknowledgedGoodputBytesPerSecond: this.#currentGoodput(),
      uniqueAcknowledgedBytesTotal: this.#uniqueAcknowledgedBytes,
      retransmittedBytesTotal: this.#retransmittedBytes,
      stallEventsTotal: this.#stallEvents,
    };
  }

  #currentGoodput(): number | null {
    if (
      this.#acknowledgedGoodputBytesPerSecond === null ||
      this.#goodputUpdatedAtMs === null ||
      this.#clock.now() - this.#goodputUpdatedAtMs >= GOODPUT_IDLE_RESET_MS
    ) return null;
    return this.#acknowledgedGoodputBytesPerSecond;
  }

  #recordAcknowledgedFlight(now: number): boolean {
    const started = this.#flightStartedAtMs;
    const bytes = this.#flightRequestBytes;
    this.#flightStartedAtMs = null;
    this.#flightRequestBytes = 0;
    if (bytes > 0) this.#uniqueAcknowledgedBytes += bytes;
    if (started === null || bytes <= 0 || now <= started) return false;
    const sample = bytes * 1_000 / (now - started);
    this.#acknowledgedGoodputBytesPerSecond =
      this.#acknowledgedGoodputBytesPerSecond === null
        ? sample
        : this.#acknowledgedGoodputBytesPerSecond * 0.75 + sample * 0.25;
    this.#goodputUpdatedAtMs = now;
    return true;
  }

  #resetFlightMeasurement(): void {
    this.#flightStartedAtMs = null;
    this.#flightRequestBytes = 0;
    this.#lastFastRetransmitAtMs = null;
  }

  #updateCongestion(): boolean {
    const before = this.#congestion.congested;
    const scheduled = this.#scheduler.snapshot();
    const outbound = this.#outbound.snapshot();
    const ackProgressAgeMs = this.#delivery.ackProgressAgeMs(this.#clock.now());
    this.#congestion.update({
      schedulerFrames: scheduled.queuedFrames,
      schedulerBytes: scheduled.queuedEncodedBytes,
      oldestQueuedFrameMs: scheduled.oldestQueuedFrameMs,
      ackProgressAgeMs,
      windowFrames: outbound.frames.length,
    });
    if (
      !before && this.#congestion.congested && outbound.frames.length > 0 &&
      ackProgressAgeMs >= this.#transportStallMs
    ) this.#stallEvents++;
    return before !== this.#congestion.congested;
  }

  #armStallWatchdog(): void {
    if (!this.#connected || this.#closed) return;
    const scheduled = this.#scheduler.snapshot();
    const outbound = this.#outbound.snapshot();
    if (scheduled.queuedFrames === 0 && outbound.frames.length === 0) {
      if (this.#stallTimer) clearTimeout(this.#stallTimer);
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#updateCongestion()) this.#notifyStateChange();
      return;
    }
    const now = this.#clock.now();
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
      this.#stallTimer && this.#stallTimerDueAtMs !== null &&
      this.#stallTimerDueAtMs <= dueAt
    ) return;
    if (this.#stallTimer) clearTimeout(this.#stallTimer);
    this.#stallTimerDueAtMs = dueAt;
    this.#stallTimer = setTimeout(() => {
      this.#stallTimer = null;
      this.#stallTimerDueAtMs = null;
      if (this.#closed) return;
      if (this.#updateCongestion()) {
        this.#notifyStateChange();
        this.#pacer.requestUrgent();
      }
      this.#armStallWatchdog();
    }, delay);
    this.#stallTimer.unref?.();
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
    this.#peakRetransmissionAttempt = Math.max(
      this.#peakRetransmissionAttempt,
      this.#delivery.attempt,
    );
    this.#pacer.flushNow("retransmission");
    return true;
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
    const jitterSeed = deterministicV2JitterSeed(this.#hostInstanceId, revision);
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

  #requireSidecarRecovery(token: V2SidecarRecoveryToken): SidecarRecoveryState {
    const recovery = this.#sidecarRecovery;
    if (!recovery || recovery.token !== token) {
      throw new V2TransportLivenessError(
        "V2 sidecar recovery token is stale or invalid",
        "instance_changed",
      );
    }
    return recovery;
  }

  #releasePartialResponseState(state: RequestState): void {
    for (const assembly of state.assemblies.values()) assembly.permit?.release();
    state.assemblies.clear();
    for (const completed of state.completedEvents.values()) completed.permit?.release();
    state.completedEvents.clear();
  }

  #detachStateForRecovery(state: RequestState): void {
    this.#requests.delete(state.requestId);
    const pendingIndex = this.#pending.indexOf(state);
    if (pendingIndex >= 0) this.#pending.splice(pendingIndex, 1);
    this.#scheduler.removeStream(state.requestId);
    state.signal?.removeEventListener("abort", state.onAbort!);
    if (state.admissionTimer) clearTimeout(state.admissionTimer);
    state.admissionTimer = undefined;
    if (this.#requests.size === 0) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
  }

  #maybeCompleteSidecarRecovery(): void {
    const recovery = this.#sidecarRecovery;
    if (!recovery || !recovery.markerSeen) return;
    for (const state of [...this.#requests.values()]) {
      if (!this.#finishRecoveryPrefixIfReady(state, true)) continue;
      if (!this.#requests.has(state.requestId)) continue;
      this.#failCommitted(state, new V2CommittedError(
        "Recovered sidecar has no durable outcome for the retained request",
        "sidecar_recovery_unresolved",
        true,
      ));
    }
    for (const [requestId, receipt] of this.#terminalReceipts) {
      if (!receipt.replayed) this.#terminalReceipts.delete(requestId);
    }
    if (this.#requests.size > 0 || this.#terminalReceipts.size > 0) return;
    this.#sidecarRecovery = null;
    recovery.resolve();
    this.#pacer.requestUrgent();
  }

  #abortRequest(state: RequestState): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#scheduler.removeStream(state.requestId);
    if (
      this.#hasExecutionRisk(state) ||
      (this.#binding.protocolRevision === "r1" && state.commitStaged)
    ) {
      const acknowledged = state.committed;
      // A staged commit cannot be retracted from the cumulative window. The
      // already-scheduled publication may still send it after this callback.
      if (this.#binding.protocolRevision === "r1" && state.commitStaged) {
        state.commitPublished = true;
      }
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
      this.#ambiguityTombstones.set(state.requestId, state.requestDigest);
      if (!state.quarantineKind) {
        state.quarantineKind = "ambiguity";
        this.#quarantined++;
        this.#ambiguousQuarantined++;
      } else if (state.quarantineKind === "cancellation") {
        state.quarantineKind = "ambiguity";
        this.#ambiguousQuarantined++;
      }
    } else {
      // A durable definitive outcome can win a cancellation race before the
      // fetch-started ACK. Release both the cancellation quarantine and the
      // still-charged admission reservation exactly once.
      if (state.quarantineKind === "cancellation") {
        state.quarantineKind = null;
        this.#quarantined = Math.max(0, this.#quarantined - 1);
      }
      if (state.admitted && !state.committed) {
        this.#creditReservations = Math.max(0, this.#creditReservations - 1);
        this.#transferReservations = Math.max(
          0,
          this.#transferReservations - state.transferCost,
        );
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
    state.preparedRequest = null;
    state.recoveryPrefix = undefined;
    for (const assembly of state.assemblies.values()) assembly.permit?.release();
    state.assemblies.clear();
    for (const completed of state.completedEvents.values()) completed.permit?.release();
    state.completedEvents.clear();
    const pendingIndex = this.#pending.indexOf(state);
    if (pendingIndex >= 0) this.#pending.splice(pendingIndex, 1);
    this.#scheduler.removeStream(state.requestId);
    if (state.admitted && !state.committed) {
      this.#creditReservations = Math.max(0, this.#creditReservations - 1);
      this.#transferReservations = Math.max(
        0,
        this.#transferReservations - state.transferCost,
      );
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
    if (this.#protocolFailed || this.#teardownPromise) return;
    const message = cause instanceof Error ? cause.message : "V2 transport protocol failed";
    this.#protocolFailed = true;
    void this.#beginTeardown(
      new V2PreCommitError(message, "protocol"),
      new V2CommittedError(message, "protocol", true),
    ).catch(() => undefined);
  }
}
