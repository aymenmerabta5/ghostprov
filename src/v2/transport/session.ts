import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import type { RunHandle } from "../../relay/run-handle";
import type { SseFrame } from "../../relay/sse";
import type {
  AwarenessChannel,
  AwarenessPeerUpdate,
} from "../awareness/channel";
import {
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
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type RequestEncoding,
  type SidecarFrame,
} from "../protocol/shared";
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
  credits: number;
  admissionCredits?: number;
  retransmissions: number;
  awarenessRttMs: number | null;
  diagnostics?: { host: V2TransportDiagnostics; sidecar: V2EnvelopeDiagnostics | null };
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
  committed: boolean;
  cancelled: boolean;
  quarantined: boolean;
  responseBytes: number;
  nextEventIndex: number;
  assemblies: Map<number, EventAssembly>;
  completedEvents: Map<number, SseFrame>;
  terminal: Extract<ReliableFrame<SidecarFrame>, { kind: "response_terminal" }> | null;
  diagnostics: HostRequestDiagnosticsObserver;
  firstResponseSeen: boolean;
}

interface RotationWaiter {
  resolve: () => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
  timer?: ReturnType<typeof setTimeout>;
}

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
  readonly #hostInstanceId = `host-${crypto.randomUUID()}`;
  readonly #outbound = new OutboundFrameWindow<HostFrame>({ epoch: 1 });
  readonly #inbound = new InboundFrameCursor<SidecarFrame>({ epoch: 1 });
  readonly #scheduler = new WeightedFrameScheduler<HostFrame>();
  readonly #delivery: ReliableDeliveryTiming;
  readonly #pacer: PublicationPacer;
  readonly #requests = new Map<string, RequestState>();
  readonly #rotations = new Map<string, RotationWaiter>();
  readonly #pending: RequestState[] = [];
  readonly #idleWaiters = new Set<() => void>();
  readonly #retiredSidecarInstances = new Set<string>();
  #connected = false;
  #closed = false;
  #draining = false;
  #connecting: Promise<void> | null = null;
  #unsubscribe: (() => void) | null = null;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #processing: Promise<void> = Promise.resolve();
  #revision = 0;
  #sidecarInstanceId: string | null = null;
  #sidecarRevision = -1;
  #remoteCredits = 0;
  #creditReservations = 0;
  #quarantined = 0;
  #retransmissions = 0;
  #drainResolve: (() => void) | null = null;
  #drainReject: ((error: unknown) => void) | null = null;
  #sidecarDiagnostics: V2EnvelopeDiagnostics | null = null;
  #publications = 0;
  #publishedBytes = 0;
  #lastAckAtMs: number;

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
    this.#binding = options.binding;
    this.#channel = options.channel;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#gzip = options.gzip;
    this.#maxResponseBytes = options.maxResponseBytes;
    this.#maxBufferedResponseBytes = maxBufferedResponseBytes;
    this.#hostCeiling = hostCeiling;
    this.#rotationTimeoutMs = rotationTimeoutMs;
    this.#lastAckAtMs = this.#clock.now();
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
      committed: false,
      cancelled: false,
      quarantined: false,
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
    this.#pacer.close();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    this.#unsubscribe?.();
    this.#unsubscribe = null;
    const error = new V2PreCommitError("V2 transport session closed", "closed");
    for (const state of [...this.#requests.values()]) {
      if (state.committed) {
        this.#failCommitted(state, new V2CommittedError(
          "V2 transport closed after commitment",
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
      credits: Math.max(0, this.#remoteCredits - this.#creditReservations),
      admissionCredits: this.#availableCredits(),
      retransmissions: this.#retransmissions,
      awarenessRttMs: this.#delivery.roundTripEwmaMs,
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

  readonly #onAwarenessUpdate = (
    update: AwarenessPeerUpdate<SidecarFrame>,
  ): void => {
    this.#processing = this.#processing
      .then(() => this.#processEnvelope(update.envelope))
      .catch(error => this.#failProtocol(error));
  };

  async #processEnvelope(
    envelope: AwarenessEnvelope<SidecarFrame>,
  ): Promise<void> {
    if (this.#closed) return;
    if (!validBinding(envelope, this.#binding)) {
      throw new Error("Sidecar Awareness binding is invalid");
    }
    let replaced = false;
    if (this.#sidecarInstanceId && envelope.instanceId !== this.#sidecarInstanceId) {
      if (this.#retiredSidecarInstances.has(envelope.instanceId)) return;
      if (envelope.epoch <= this.#inbound.ack().epoch) return;
      this.#retiredSidecarInstances.add(this.#sidecarInstanceId);
      const message = "Sidecar instance changed while transport state was active";
      for (const state of [...this.#requests.values()]) {
        if (state.committed) {
          this.#failCommitted(state, new V2CommittedError(message, "instance_changed", true));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(message, "instance_changed"));
        }
      }
      const nextEpoch = this.#outbound.snapshot().epoch + 1;
      this.#outbound.reset(nextEpoch);
      this.#delivery.reset(nextEpoch);
      this.#sidecarRevision = -1;
      replaced = true;
    }
    if (envelope.revision <= this.#sidecarRevision) return;
    this.#sidecarInstanceId = envelope.instanceId;
    this.#sidecarRevision = envelope.revision;
    this.#sidecarDiagnostics = envelope.diagnostics ?? null;
    this.#outbound.acknowledge(envelope.ack);
    const now = this.#clock.now();
    const acknowledgementAdvanced = this.#delivery.acknowledged(
      envelope.ack,
      this.#outbound.snapshot().frames.length > 0,
      now,
    );
    if (acknowledgementAdvanced) {
      this.#lastAckAtMs = now;
      this.#armRetransmission();
    }
    this.#remoteCredits = Math.max(0, Math.min(128, envelope.credits));
    const currentEpoch = this.#inbound.ack().epoch;
    if (envelope.epoch > currentEpoch) this.#inbound.reset(envelope.epoch);
    const delivered = this.#inbound.accept(envelope.frames);
    for (const frame of delivered) await this.#processFrame(frame);
    this.#admitPending();
    const urgency = this.#pump();
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
          if (state.committed) {
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
          this.#failPreCommit(state, new V2PreCommitError(
            "Sidecar emitted response data before commitment",
            "protocol",
          ));
          return;
        }
        if (!state.firstResponseSeen) { state.firstResponseSeen = true; state.diagnostics.firstFrame(frame.diagnostics); }
        await this.#acceptResponsePart(state, frame);
        return;
      }
      case "response_batch": {
        const state=this.#requests.get(frame.requestId);if(!state)return;
        if(!state.committed){this.#failPreCommit(state,new V2PreCommitError("Sidecar emitted response data before commitment","protocol"));return;}
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
          this.#failPreCommit(state, new V2PreCommitError(
            "Sidecar emitted a terminal before commitment",
            "protocol",
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
          ));
        } else {
          this.#failPreCommit(state, new V2PreCommitError(frame.message, frame.code));
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
    const wasQuarantined = state.quarantined;
    this.#removeState(state);
    if (!wasCancelled) state.handle.settle(final);
    if (wasQuarantined) this.#quarantined = Math.max(0, this.#quarantined - 1);
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
      this.#creditReservations++;
      for (const frame of state.frames) {
        // Request frames share one priority queue so commit can never overtake data.
        this.#scheduler.enqueue("data", state.requestId, frame);
      }
    }
  }

  #availableCredits(): number {
    return Math.max(0, Math.min(this.#remoteCredits - this.#creditReservations, 4 - this.#creditReservations));
  }

  #pump(): PublicationUrgency | null {
    if (!this.#connected || this.#closed) return null;
    let urgency: PublicationUrgency | null = null;
    for (;;) {
      const snapshot = this.#outbound.snapshot();
      const frameBudget = GP_V2_WINDOW_MAX_FRAMES - snapshot.frames.length;
      const byteBudget = GP_V2_WINDOW_MAX_BYTES - snapshot.encodedBytes;
      if (frameBudget < 1 || byteBudget < 512) break;
      const next = this.#scheduler.take(1, Math.max(1, byteBudget - 256));
      if (next.length === 0) break;
      const reliable = this.#outbound.push(next[0]!);
      urgency = strongestUrgency(urgency, hostFrameUrgency(reliable));
      if ("requestId" in reliable && typeof reliable.requestId === "string") {
        const state = this.#requests.get(reliable.requestId);
        if (state) state.published = true;
        if (state) state.diagnostics.published();
      }
    }
    return urgency;
  }

  #requestPublication(urgency: PublicationUrgency | null): void {
    if (urgency === "urgent") this.#pacer.requestUrgent();
    else if (urgency === "paced") this.#pacer.requestPaced();
  }

  #publish(_reason: PublicationReason): void {
    if (!this.#connected || this.#closed) return;
    const outbound = this.#outbound.snapshot();
    this.#delivery.published(outbound.frames, this.#clock.now());
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
      revision: this.#revision++,
      ack: this.#inbound.ack(),
      credits: this.#hostCeiling,
      active: committed,
      queued: this.#pending.filter(state => !state.admitted).length,
      draining: this.#draining,
      frames: outbound.frames,
    };
    this.#publications++;
    this.#publishedBytes += utf8Bytes(JSON.stringify(envelope)).byteLength;
    const diagnostics = this.#hostTransportDiagnostics();
    for (const state of this.#requests.values()) state.diagnostics.peak(diagnostics);
    this.#channel.publish(envelope);
    this.#armRetransmission();
  }

  #hostTransportDiagnostics(): V2TransportDiagnostics {
    const window=this.#outbound.snapshot(); const scheduler=this.#scheduler.snapshot();
    return { windowFrames:window.frames.length, windowBytes:window.encodedBytes, schedulerFrames:scheduler.queuedFrames, schedulerBytes:scheduler.queuedEncodedBytes, schedulerStreams:scheduler.streams, oldestQueuedFrameMs:scheduler.oldestQueuedFrameMs, ackProgressAgeMs:Math.max(0,this.#clock.now()-this.#lastAckAtMs), retransmissionAttempt:this.#delivery.attempt, peakRetransmissionAttempt:this.#delivery.attempt, retransmissionsTotal:this.#retransmissions, publicationsTotal:this.#publications, publishedBytesTotal:this.#publishedBytes, admittedUncommitted:[...this.#requests.values()].filter(value=>value.admitted&&!value.committed).length, roundTripEwmaMs:this.#delivery.roundTripEwmaMs };
  }

  #armRetransmission(): void {
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = null;
    if (
      !this.#connected ||
      this.#closed ||
      this.#outbound.snapshot().frames.length === 0
    ) return;
    const jitterSeed = ((this.#revision * 2_654_435_761) >>> 0) / 0xffff_ffff;
    const delay = this.#delivery.nextDelayMs(this.#clock.now(), jitterSeed);
    if (delay === null) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      if (!this.#connected || this.#closed) return;
      if (this.#outbound.snapshot().frames.length === 0) return;
      this.#delivery.retransmitted(this.#clock.now());
      this.#retransmissions++;
      this.#pacer.flushNow("retransmission");
    }, delay);
  }

  #abortRequest(state: RequestState): void {
    if (!this.#requests.has(state.requestId)) return;
    this.#scheduler.removeStream(state.requestId);
    if (state.committed) {
      state.cancelled = true;
      if (!state.quarantined) {
        state.quarantined = true;
        this.#quarantined++;
      }
      state.handle.cancel(new V2CommittedError(
        "V2 request was cancelled after commitment",
        "cancelled",
        true,
      ));
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
    if (error.ambiguous && !state.quarantined) {
      state.quarantined = true;
      this.#quarantined++;
    }
    this.#removeState(state);
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
    if (this.#requests.size === 0) {
      for (const resolve of this.#idleWaiters) resolve();
      this.#idleWaiters.clear();
    }
    this.#admitPending();
  }

  #failProtocol(cause: unknown): void {
    const message = cause instanceof Error ? cause.message : "V2 transport protocol failed";
    for (const state of [...this.#requests.values()]) {
      if (state.committed) {
        this.#failCommitted(state, new V2CommittedError(message, "protocol", true));
      } else {
        this.#failPreCommit(state, new V2PreCommitError(message, "protocol"));
      }
    }
  }
}
