import type { Clock } from "../../src/core/ports";
import type { RunHandle } from "../../src/relay/run-handle";
import type { SseFrame } from "../../src/relay/sse";
import { ResourceGovernor } from "../../src/router/resource-governor";
import { MultiSandboxRouter } from "../../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../../src/sandbox/registry";
import { projectId, type ProjectId } from "../../src/sandbox/types";
import { parseYSweetEnvelope } from "../../src/prism/protocol";
import { SplitAwarenessChannel } from "../../src/v2/awareness/split-channel";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../../src/v2/awareness/y-sweet-channel";
import { createCredentialRotationKey } from "../../src/v2/bootstrap/crypto";
import { SidecarDiagnosticsObserver } from "../../src/v2/diagnostics/sidecar-observer";
import { SidecarResourceSampler } from "../../src/v2/diagnostics/sidecar-sampler";
import type { V2TransportDiagnostics } from "../../src/v2/diagnostics/types";
import {
  GP_V2_REQUEST_MAX_BYTES,
  base64UrlDecode,
  canonicalJson,
  protocolSchemaHash,
  sha256Hex,
  utf8Bytes,
  type AwarenessEnvelope,
  type AwarenessLane,
  type HostFrame,
  type ProtocolBinding,
  type ReliableFrame,
  type SidecarFrame,
} from "../../src/v2/protocol/shared";
import { AdmissionController } from "../../src/v2/sidecar/admission";
import { RequestAssembler } from "../../src/v2/sidecar/assembler";
import { SidecarEngine } from "../../src/v2/sidecar/engine";
import { ClaimJournal } from "../../src/v2/sidecar/journal";
import type {
  DirectResponsesExecution,
  UpstreamTerminal,
} from "../../src/v2/sidecar/upstream";
import { TranscriptProof } from "../../src/v2/protocol/transcript";
import { parseAwarenessEnvelope } from "../../src/v2/protocol/validate";
import { V2TransportSession } from "../../src/v2/transport/session";
import {
  DenoAwarenessPeer,
  type DenoAwarenessProviderLike,
} from "../../sandbox/v2/awareness-peer";
import { FakeSidecarStorage } from "./fake-v2-sidecar";
import * as Y from "yjs";
import {
  Awareness,
  applyAwarenessUpdate,
  encodeAwarenessUpdate,
} from "y-protocols/awareness";

const KiB = 1024;
const MiB = KiB * KiB;
const PROJECT = projectId("9a41e916-8c9f-4c27-97bb-c8d87d20d001");
const HOST_INSTANCE = "host-perf-harness";
const SIDECAR_INSTANCE = "sidecar-perf-harness";
const LAUNCH_NONCE = "launch-perf-harness";

export const V2_PERFORMANCE_RTTS_MS = [50, 300, 1_000] as const;
export const V2_PERFORMANCE_CONCURRENCY = [1, 4, 16, 32, 40] as const;
export const V2_PERFORMANCE_PAYLOAD_BYTES = Object.freeze({
  kib128: 128 * KiB,
  kib512: 512 * KiB,
  mib3_35: Math.round(3.35 * MiB),
  mib16: 16 * MiB,
  mib64: 64 * MiB,
});

export type V2HarnessDirection = "host-to-sidecar" | "sidecar-to-host";
export type V2HarnessFaultAction = "drop" | "duplicate" | "reorder";

export interface V2HarnessFaultRule {
  direction: V2HarnessDirection;
  lane: AwarenessLane;
  /** One-based publication ordinal within this direction and lane. */
  ordinal: number;
  actions: readonly V2HarnessFaultAction[];
}

export interface V2HarnessTransientDisconnect {
  direction: V2HarnessDirection;
  lane: AwarenessLane;
  /** One-based first publication dropped while the link is disconnected. */
  fromOrdinal: number;
  publicationCount: number;
}

export interface V2PerformanceHarnessOptions {
  rttMs: number;
  requestBytes: number;
  concurrency: number;
  faults?: readonly V2HarnessFaultRule[];
  transientDisconnects?: readonly V2HarnessTransientDisconnect[];
  /** Prevents accidental large cross-products in normal CI. */
  maxAggregateRequestBytes?: number;
  timeoutMs?: number;
}

export interface V2HarnessTimingSummary {
  p50: number;
  p95: number;
  max: number;
}

export interface V2HarnessRequestResult {
  index: number;
  requestBytes: number;
  requestDigest: string;
  upstreamDigest: string;
  commitMs: number;
  firstEventMs: number;
  totalMs: number;
  ackRoundsAtCommit: number;
  eventCount: number;
  transcriptDigest: string;
  upstreamTranscriptDigest: string;
  finalDigest: string;
  upstreamFinalDigest: string;
  exactRequest: boolean;
  exactTerminal: boolean;
}

export interface V2HarnessTransportPeaks {
  hostWindowFrames: number;
  hostWindowBytes: number;
  hostSchedulerFrames: number;
  hostSchedulerBytes: number;
  hostSchedulerStreams: number;
  hostAckProgressAgeMs: number;
  sidecarWindowFrames: number;
  sidecarWindowBytes: number;
  sidecarSchedulerFrames: number;
  sidecarSchedulerBytes: number;
}

export interface V2HarnessWireSnapshot {
  attemptedPublications: number;
  deliveredUpdates: number;
  attemptedBytes: number;
  deliveredBytes: number;
  droppedUpdates: number;
  disconnectedDrops: number;
  duplicateDeliveries: number;
  reorderedUpdates: number;
  usefulRequestBytes: number;
  uniqueRequestDataBytes: number;
  hostDataPublications: number;
  sidecarDataPublications: number;
}

export interface V2PerformanceHarnessReport {
  rttMs: number;
  requestBytes: number;
  concurrency: number;
  virtualElapsedMs: number;
  requests: readonly V2HarnessRequestResult[];
  timings: {
    commitMs: V2HarnessTimingSummary;
    firstEventMs: V2HarnessTimingSummary;
    totalMs: V2HarnessTimingSummary;
  };
  upstream: {
    calls: number;
    maxActive: number;
  };
  transport: {
    retransmissions: number;
    ackRounds: number;
    awarenessRttMs: number | null;
    host: V2TransportDiagnostics;
    sidecar: V2TransportDiagnostics | null;
    peaks: V2HarnessTransportPeaks;
  };
  wire: V2HarnessWireSnapshot;
  router: ReturnType<MultiSandboxRouter["snapshot"]>;
}

class HarnessClock implements Clock {
  #now = 0;

  now(): number {
    return this.#now;
  }

  advanceTo(value: number): void {
    if (!Number.isFinite(value) || value < this.#now) {
      throw new RangeError("Harness clock cannot move backwards");
    }
    this.#now = value;
  }

  async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw signal.reason ?? new DOMException("aborted", "AbortError");
    }
    this.advanceTo(this.#now + Math.max(0, ms));
  }

  async waitUntil(deadlineMs: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) {
      throw signal.reason ?? new DOMException("aborted", "AbortError");
    }
    if (deadlineMs > this.#now) this.advanceTo(deadlineMs);
  }
}

interface WireRequestTrace {
  requestId: string;
  requestDigest: string;
  decodedBytes: number;
  encodedBytes: number;
  uniqueDataBytes: number;
  parts: Set<number>;
  flights: Set<string>;
}

interface HubEndpointMetadata {
  role: "host" | "sidecar";
  lane: AwarenessLane;
  label: string;
}

interface QueuedUpdate {
  id: number;
  dueAtMs: number;
  target: HarnessAwarenessProvider | null;
  update: Uint8Array;
  bytes: number;
}

class HarnessAwarenessProvider
implements AwarenessProviderLike, DenoAwarenessProviderLike {
  readonly awareness: Awareness;
  readonly #listeners = new Map<string, Set<(...args: any[]) => void>>();
  connected = false;

  constructor(
    readonly hub: FaultyAwarenessHub,
    readonly doc: Y.Doc,
    readonly metadata: HubEndpointMetadata,
  ) {
    this.awareness = new Awareness(doc);
    this.awareness.on("update", this.#onUpdate);
  }

  on(event: string, listener: (...args: any[]) => void): void {
    let listeners = this.#listeners.get(event);
    if (!listeners) this.#listeners.set(event, listeners = new Set());
    listeners.add(listener);
  }

  off(event: string, listener: (...args: any[]) => void): void {
    this.#listeners.get(event)?.delete(listener);
  }

  connect(): void {
    if (this.connected) return;
    this.connected = true;
    this.hub.connect(this);
    queueMicrotask(() => this.#emit("sync", true));
  }

  disconnect(): void {
    if (!this.connected) return;
    this.connected = false;
    this.hub.disconnect(this);
  }

  destroy(): void {
    this.disconnect();
    this.awareness.off("update", this.#onUpdate);
    this.awareness.destroy();
    this.#listeners.clear();
  }

  #emit(event: string, ...args: any[]): void {
    for (const listener of this.#listeners.get(event) ?? []) listener(...args);
  }

  readonly #onUpdate = (
    change: { added: number[]; updated: number[]; removed: number[] },
    origin: unknown,
  ): void => {
    if (origin === this.hub || !this.connected) return;
    this.hub.broadcast(this, [
      ...change.added,
      ...change.updated,
      ...change.removed,
    ]);
  };
}

class FaultyAwarenessHub {
  readonly #clock: HarnessClock;
  readonly #rttMs: number;
  readonly #faults: readonly V2HarnessFaultRule[];
  readonly #disconnects: readonly V2HarnessTransientDisconnect[];
  readonly #providers = new Set<HarnessAwarenessProvider>();
  readonly #queue: QueuedUpdate[] = [];
  readonly #heldReorders = new Map<string, QueuedUpdate[]>();
  readonly #ordinals = new Map<string, number>();
  readonly #requestTraces = new Map<string, WireRequestTrace>();
  readonly #digestToRequest = new Map<string, string>();
  readonly #activity = new Set<() => void>();
  #nextUpdateId = 1;
  #pump: ReturnType<typeof setTimeout> | null = null;
  #closed = false;
  #attemptedPublications = 0;
  #deliveredUpdates = 0;
  #attemptedBytes = 0;
  #deliveredBytes = 0;
  #droppedUpdates = 0;
  #disconnectedDrops = 0;
  #duplicateDeliveries = 0;
  #reorderedUpdates = 0;
  #hostDataPublications = 0;
  #sidecarDataPublications = 0;

  constructor(options: V2PerformanceHarnessOptions, clock: HarnessClock) {
    this.#clock = clock;
    this.#rttMs = options.rttMs;
    this.#faults = options.faults ?? [];
    this.#disconnects = options.transientDisconnects ?? [];
  }

  create(
    doc: Y.Doc,
    role: "host" | "sidecar",
    lane: AwarenessLane,
  ): HarnessAwarenessProvider {
    return new HarnessAwarenessProvider(this, doc, {
      role,
      lane,
      label: `${role}-${lane}`,
    });
  }

  onActivity(listener: () => void): () => void {
    this.#activity.add(listener);
    return () => this.#activity.delete(listener);
  }

  connect(provider: HarnessAwarenessProvider): void {
    for (const existing of this.#providers) {
      if (
        existing.metadata.lane !== provider.metadata.lane ||
        existing.metadata.role === provider.metadata.role
      ) continue;
      this.#copyLocal(existing, provider);
      this.#copyLocal(provider, existing);
    }
    this.#providers.add(provider);
  }

  disconnect(provider: HarnessAwarenessProvider): void {
    this.#providers.delete(provider);
  }

  broadcast(
    source: HarnessAwarenessProvider,
    clientIds: readonly number[],
  ): void {
    if (this.#closed || clientIds.length === 0) return;
    const update = encodeAwarenessUpdate(source.awareness, [...clientIds]);
    for (const target of this.#providers) {
      if (
        target === source ||
        target.metadata.lane !== source.metadata.lane ||
        target.metadata.role === source.metadata.role
      ) continue;
      this.#send(source, target, update);
    }
  }

  requestTraceByDigest(digest: string): WireRequestTrace | null {
    const requestId = this.#digestToRequest.get(digest);
    return requestId ? this.#requestTraces.get(requestId) ?? null : null;
  }

  snapshot(): V2HarnessWireSnapshot {
    let usefulRequestBytes = 0;
    let uniqueRequestDataBytes = 0;
    for (const trace of this.#requestTraces.values()) {
      usefulRequestBytes += trace.decodedBytes;
      uniqueRequestDataBytes += trace.uniqueDataBytes;
    }
    return {
      attemptedPublications: this.#attemptedPublications,
      deliveredUpdates: this.#deliveredUpdates,
      attemptedBytes: this.#attemptedBytes,
      deliveredBytes: this.#deliveredBytes,
      droppedUpdates: this.#droppedUpdates,
      disconnectedDrops: this.#disconnectedDrops,
      duplicateDeliveries: this.#duplicateDeliveries,
      reorderedUpdates: this.#reorderedUpdates,
      usefulRequestBytes,
      uniqueRequestDataBytes,
      hostDataPublications: this.#hostDataPublications,
      sidecarDataPublications: this.#sidecarDataPublications,
    };
  }

  close(): void {
    this.#closed = true;
    if (this.#pump) clearTimeout(this.#pump);
    this.#pump = null;
    this.#queue.length = 0;
    this.#heldReorders.clear();
    this.#providers.clear();
    this.#activity.clear();
  }

  #copyLocal(
    source: HarnessAwarenessProvider,
    target: HarnessAwarenessProvider,
  ): void {
    if (!source.awareness.getLocalState()) return;
    const update = encodeAwarenessUpdate(source.awareness, [source.doc.clientID]);
    this.#send(source, target, update);
  }

  #send(
    source: HarnessAwarenessProvider,
    target: HarnessAwarenessProvider,
    update: Uint8Array,
  ): void {
    const direction: V2HarnessDirection = source.metadata.role === "host"
      ? "host-to-sidecar"
      : "sidecar-to-host";
    const lane = source.metadata.lane;
    const key = `${direction}:${lane}`;
    const ordinal = (this.#ordinals.get(key) ?? 0) + 1;
    this.#ordinals.set(key, ordinal);
    this.#attemptedPublications++;
    this.#attemptedBytes += update.byteLength;
    if (direction === "host-to-sidecar" && lane === "data") {
      this.#hostDataPublications++;
      this.#trackHostDataEnvelope(source);
    } else if (direction === "sidecar-to-host" && lane === "data") {
      this.#sidecarDataPublications++;
    }

    const disconnected = this.#disconnects.some(value => (
      value.direction === direction &&
      value.lane === lane &&
      ordinal >= value.fromOrdinal &&
      ordinal < value.fromOrdinal + value.publicationCount
    ));
    if (disconnected) {
      this.#disconnectedDrops++;
      this.#scheduleClockOnly();
      return;
    }

    const actions = new Set(this.#faults
      .filter(value => (
        value.direction === direction &&
        value.lane === lane &&
        value.ordinal === ordinal
      ))
      .flatMap(value => value.actions));
    if (actions.has("drop")) {
      this.#droppedUpdates++;
      this.#scheduleClockOnly();
      return;
    }

    const queued: QueuedUpdate = {
      id: this.#nextUpdateId++,
      dueAtMs: this.#clock.now() + this.#rttMs / 2,
      target,
      update: update.slice(),
      bytes: update.byteLength,
    };
    const copies = actions.has("duplicate") ? 2 : 1;
    if (actions.has("duplicate")) this.#duplicateDeliveries++;
    const deliveries = Array.from({ length: copies }, (_, index) => ({
      ...queued,
      id: index === 0 ? queued.id : this.#nextUpdateId++,
      update: queued.update.slice(),
    }));

    if (actions.has("reorder")) {
      this.#reorderedUpdates++;
      this.#heldReorders.set(key, deliveries);
      return;
    }

    this.#queue.push(...deliveries);
    const held = this.#heldReorders.get(key);
    if (held) {
      this.#heldReorders.delete(key);
      const afterCurrent = queued.dueAtMs + 0.001;
      this.#queue.push(...held.map(value => ({
        ...value,
        dueAtMs: Math.max(afterCurrent, value.dueAtMs),
      })));
    }
    this.#schedulePump();
  }

  #scheduleClockOnly(): void {
    this.#queue.push({
      id: this.#nextUpdateId++,
      dueAtMs: this.#clock.now() + this.#rttMs,
      target: null,
      update: new Uint8Array(),
      bytes: 0,
    });
    this.#schedulePump();
  }

  #schedulePump(): void {
    if (this.#pump || this.#closed) return;
    this.#pump = setTimeout(() => {
      this.#pump = null;
      this.#flushNext();
    }, 0);
  }

  #flushNext(): void {
    if (this.#closed || this.#queue.length === 0) return;
    this.#queue.sort((left, right) =>
      left.dueAtMs - right.dueAtMs || left.id - right.id);
    const dueAtMs = this.#queue[0]!.dueAtMs;
    this.#clock.advanceTo(dueAtMs);
    const ready: QueuedUpdate[] = [];
    while (this.#queue[0]?.dueAtMs === dueAtMs) ready.push(this.#queue.shift()!);
    for (const item of ready) {
      if (item.bytes === 0 || !item.target?.connected) continue;
      applyAwarenessUpdate(item.target.awareness, item.update, this);
      this.#deliveredUpdates++;
      this.#deliveredBytes += item.bytes;
    }
    for (const listener of this.#activity) listener();
    if (this.#queue.length > 0) this.#schedulePump();
  }

  #trackHostDataEnvelope(source: HarnessAwarenessProvider): void {
    const envelope = source.awareness.getLocalState()
      ?.ghostprovV2Data as AwarenessEnvelope<HostFrame> | undefined;
    if (!envelope || envelope.frames.length === 0) return;
    const flightKey = `${envelope.epoch}:${envelope.frames[0]!.sequence}-${envelope.frames.at(-1)!.sequence}`;
    const requestIds = new Set<string>();
    for (const frame of envelope.frames) {
      if (!("requestId" in frame) || typeof frame.requestId !== "string") continue;
      requestIds.add(frame.requestId);
      if (frame.kind === "request_open") {
        let trace = this.#requestTraces.get(frame.requestId);
        if (!trace) {
          trace = {
            requestId: frame.requestId,
            requestDigest: frame.requestDigest,
            decodedBytes: frame.decodedBytes,
            encodedBytes: frame.encodedBytes,
            uniqueDataBytes: 0,
            parts: new Set(),
            flights: new Set(),
          };
          this.#requestTraces.set(frame.requestId, trace);
          this.#digestToRequest.set(frame.requestDigest, frame.requestId);
        }
      }
      if (frame.kind === "request_data") {
        const trace = this.#requestTraces.get(frame.requestId);
        if (trace && !trace.parts.has(frame.partIndex)) {
          trace.parts.add(frame.partIndex);
          trace.uniqueDataBytes += base64UrlDecode(frame.dataBase64Url).byteLength;
        }
      }
    }
    for (const requestId of requestIds) {
      this.#requestTraces.get(requestId)?.flights.add(flightKey);
    }
  }
}

class HoldGate {
  #released = false;
  #resolve!: () => void;
  readonly #pending = new Promise<void>(resolve => { this.#resolve = resolve; });

  wait(): Promise<void> {
    return this.#released ? Promise.resolve() : this.#pending;
  }

  release(): void {
    if (this.#released) return;
    this.#released = true;
    this.#resolve();
  }
}

interface UpstreamProof {
  requestDigest: string;
  transcriptDigest: string;
  finalDigest: string;
}

class DigestingUpstream {
  calls = 0;
  active = 0;
  maxActive = 0;
  readonly proofs = new Map<number, UpstreamProof>();
  readonly #gate = new HoldGate();

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    await input.beforeFetch();
    const body = JSON.parse(new TextDecoder().decode(input.body)) as {
      input?: unknown;
    };
    const match = /^case-(\d{6}):/.exec(String(body.input ?? ""));
    if (!match) throw new Error("Harness request identity is missing");
    const index = Number(match[1]);
    const requestDigest = await sha256Hex(input.body);
    this.calls++;
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    try {
      await this.#gate.wait();
      const responseId = `resp_perf_${String(index).padStart(6, "0")}`;
      await input.onPrelude({
        status: 200,
        contentType: "text/event-stream",
        upstreamRequestId: `upstream_perf_${String(index).padStart(6, "0")}`,
      });
      const final = {
        id: responseId,
        object: "response",
        status: "completed",
        output: [{
          type: "message",
          role: "assistant",
          status: "completed",
          content: [{
            type: "output_text",
            text: `ok:${index}:${requestDigest}`,
          }],
        }],
      };
      const events: SseFrame[] = [
        {
          event: "response.created",
          data: JSON.stringify({
            type: "response.created",
            response: { id: responseId },
          }),
        },
        {
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: final }),
        },
      ];
      const transcript = new TranscriptProof("upstream", "0".repeat(64));
      for (const event of events) {
        transcript.append(event);
        await input.onEvent(event);
      }
      const terminal = await transcript.createTerminal();
      const snapshot = transcript.snapshot();
      this.proofs.set(index, {
        requestDigest,
        transcriptDigest: terminal.transcriptSha256,
        finalDigest: terminal.finalSha256,
      });
      return {
        responseId,
        status: terminal.terminalStatus,
        eventCount: terminal.eventCount,
        byteCount: terminal.byteCount,
        transcriptSha256: terminal.transcriptSha256,
        finalEventIndex: terminal.finalEventIndex,
        finalSha256: terminal.finalSha256,
        final,
        transcriptBytes: snapshot.canonicalBytes,
      };
    } finally {
      this.active--;
    }
  }

  release(): void {
    this.#gate.release();
  }
}

function emptyPeaks(): V2HarnessTransportPeaks {
  return {
    hostWindowFrames: 0,
    hostWindowBytes: 0,
    hostSchedulerFrames: 0,
    hostSchedulerBytes: 0,
    hostSchedulerStreams: 0,
    hostAckProgressAgeMs: 0,
    sidecarWindowFrames: 0,
    sidecarWindowBytes: 0,
    sidecarSchedulerFrames: 0,
    sidecarSchedulerBytes: 0,
  };
}

function recordPeak(
  peaks: V2HarnessTransportPeaks,
  host: V2TransportDiagnostics,
  sidecar: V2TransportDiagnostics | null,
): void {
  peaks.hostWindowFrames = Math.max(peaks.hostWindowFrames, host.windowFrames);
  peaks.hostWindowBytes = Math.max(peaks.hostWindowBytes, host.windowBytes);
  peaks.hostSchedulerFrames = Math.max(peaks.hostSchedulerFrames, host.schedulerFrames);
  peaks.hostSchedulerBytes = Math.max(peaks.hostSchedulerBytes, host.schedulerBytes);
  peaks.hostSchedulerStreams = Math.max(peaks.hostSchedulerStreams, host.schedulerStreams);
  peaks.hostAckProgressAgeMs = Math.max(peaks.hostAckProgressAgeMs, host.ackProgressAgeMs);
  if (!sidecar) return;
  peaks.sidecarWindowFrames = Math.max(peaks.sidecarWindowFrames, sidecar.windowFrames);
  peaks.sidecarWindowBytes = Math.max(peaks.sidecarWindowBytes, sidecar.windowBytes);
  peaks.sidecarSchedulerFrames = Math.max(peaks.sidecarSchedulerFrames, sidecar.schedulerFrames);
  peaks.sidecarSchedulerBytes = Math.max(peaks.sidecarSchedulerBytes, sidecar.schedulerBytes);
}

class PerformanceRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId = PROJECT;
  readonly upstream: DigestingUpstream;
  readonly peaks = emptyPeaks();
  readonly #hub: FaultyAwarenessHub;
  readonly #session: V2TransportSession;
  readonly #peer: DenoAwarenessPeer;
  readonly #engine: SidecarEngine;
  readonly #journal: ClaimJournal;
  readonly #sampler: SidecarResourceSampler;
  readonly #physical: number;
  #state = "configured";
  #unsubscribeActivity: (() => void) | null = null;

  private constructor(options: {
    hub: FaultyAwarenessHub;
    session: V2TransportSession;
    peer: DenoAwarenessPeer;
    engine: SidecarEngine;
    upstream: DigestingUpstream;
    journal: ClaimJournal;
    sampler: SidecarResourceSampler;
    physical: number;
  }) {
    this.#hub = options.hub;
    this.#session = options.session;
    this.#peer = options.peer;
    this.#engine = options.engine;
    this.upstream = options.upstream;
    this.#journal = options.journal;
    this.#sampler = options.sampler;
    this.#physical = options.physical;
    this.#unsubscribeActivity = this.#hub.onActivity(() => this.sample());
  }

  static async create(
    options: V2PerformanceHarnessOptions,
    clock: HarnessClock,
    hub: FaultyAwarenessHub,
  ): Promise<PerformanceRuntime> {
    const faulted = (options.faults?.length ?? 0) > 0 ||
      (options.transientDisconnects?.length ?? 0) > 0;
    // Wall timers remain production timers. Keep normal runs clear of
    // artificial retransmissions; fault cases use a short bounded RTO so CI
    // does not wait for the virtual RTT in wall-clock time.
    const retransmitMs = faulted ? 5 : 1_000;
    const binding: ProtocolBinding = {
      protocolRevision: "r2",
      projectId: PROJECT,
      sessionId: "sandbox-session-perf",
      leaderGeneration: 7,
      buildHash: "a".repeat(64),
      schemaHash: await protocolSchemaHash("r2"),
      launchNonce: LAUNCH_NONCE,
    };
    const ySweet = parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-secret",
      authorization: "full",
    });
    const hostLane = (
      lane: AwarenessLane,
      field: "ghostprovV2Data" | "ghostprovV2Control",
    ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
      envelope: ySweet,
      localPeerId: `host:${lane}`,
      field,
      parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      providerFactory: doc => hub.create(doc, "host", lane),
      syncTimeoutMs: 1_000,
    });
    const channel = new SplitAwarenessChannel({
      data: hostLane("data", "ghostprovV2Data"),
      control: hostLane("control", "ghostprovV2Control"),
    });
    const session = new V2TransportSession({
      binding,
      channel,
      clock,
      maxResponseBytes: 2 * MiB,
      maxBufferedResponseBytes: 512 * KiB,
      hostCeiling: options.concurrency,
      retransmitMs,
      publicationIntervalMs: 1,
      unadmittedTimeoutMs: options.timeoutMs ?? 20_000,
      expectedSidecarInstanceId: SIDECAR_INSTANCE,
      hostInstanceId: HOST_INSTANCE,
    });
    const timeline: string[] = [];
    const storage = new FakeSidecarStorage(timeline);
    const journal = new ClaimJournal({
      storage,
      flushIntervalMs: 0,
      maxBatch: Math.max(64, options.concurrency * 2),
    });
    const aggregate = options.requestBytes * options.concurrency;
    const admission = new AdmissionController({
      physicalMax: options.concurrency,
      acceptedMax: Math.min(128, options.concurrency * 2),
      hostCeiling: options.concurrency,
      memoryBudgetBytes: Math.max(64 * MiB, aggregate * 2, options.concurrency * 4 * MiB),
    });
    const sampler = new SidecarResourceSampler({
      enabled: false,
      intervalMs: 100,
      now: () => clock.now(),
    });
    const diagnostics = new SidecarDiagnosticsObserver({
      enabled: true,
      now: () => clock.now(),
      sampler,
    });
    let peer!: DenoAwarenessPeer;
    const upstream = new DigestingUpstream();
    const engine = new SidecarEngine({
      binding,
      assembler: new RequestAssembler({
        maxReservedBytes: Math.max(64 * MiB, aggregate * 2),
      }),
      journal,
      admission,
      upstream,
      diagnostics,
      batching: true,
      assertExecutionFence: frame => peer.assertExecutionFence(frame),
      emit: (frame, priority, streamId) => peer.enqueue(frame, priority, streamId),
    });
    peer = new DenoAwarenessPeer({
      credential: {
        url: ySweet.url,
        baseUrl: ySweet.baseUrl,
        docId: ySweet.docId,
        token: ySweet.token.reveal(),
        authorization: "full",
        rotationKey: createCredentialRotationKey(),
      },
      binding,
      instanceId: SIDECAR_INSTANCE,
      engine,
      diagnostics,
      providerFactory: (doc, _credential, lane) => hub.create(doc, "sidecar", lane),
      publicationIntervalMs: 1,
      retransmitMs,
      connectTimeoutMs: 1_000,
      closeTimeoutMs: 2_000,
      initialActivePerSandbox: options.concurrency,
      maxActivePerSandbox: options.concurrency,
    });
    const runtime = new PerformanceRuntime({
      hub,
      session,
      peer,
      engine,
      upstream,
      journal,
      sampler,
      physical: options.concurrency,
    });
    return runtime;
  }

  async start(): Promise<void> {
    if (this.#state === "ready") return;
    await Promise.all([
      this.#peer.connect(),
      this.#session.connect(),
    ]);
    await this.#engine.start();
    this.#peer.publishEngineState();
    await waitFor(
      () => this.#session.snapshot().credits > 0,
      5_000,
      "V2 harness initial credits",
    );
    this.#state = "ready";
  }

  async drain(reason: string): Promise<void> {
    this.#state = "draining";
    await this.#session.drain(reason);
    this.#state = "stopped";
  }

  async stop(): Promise<void> {
    this.upstream.release();
    await Promise.allSettled([
      this.#session.close(),
      this.#peer.close(),
    ]);
    await Promise.allSettled([
      this.#journal.close(),
      this.#sampler.close(),
    ]);
    this.#unsubscribeActivity?.();
    this.#unsubscribeActivity = null;
    this.#state = "stopped";
  }

  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    return this.#session.run(body, signal);
  }

  snapshot(): RoutableSandboxSnapshot {
    const transport = this.#session.snapshot();
    this.sample();
    const capacity = this.#state === "ready" ? this.#physical : 0;
    return {
      projectId: this.projectId,
      state: this.#state,
      capacity,
      active: transport.active,
      available: this.#state === "ready"
        ? Math.max(0, Math.min(
          transport.credits,
          capacity - transport.active - transport.quarantined,
        ))
        : 0,
      quarantined: transport.quarantined,
      ambiguousQuarantined: transport.ambiguousQuarantined,
      ambiguityLimit: transport.ambiguityLimit,
      heartbeatAgeMs: 0,
      sidecarPeerAgeMs: transport.sidecarPeerAgeMs,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: transport.awarenessRttMs ?? 0,
      completionEwmaMs: 0,
      recentFailureRate: 0,
      queueDepth: transport.queued,
      memoryPressure: 0,
      failure: null,
      diagnostics: transport.diagnostics,
    };
  }

  sample(): void {
    const diagnostics = this.#session.snapshot().diagnostics;
    recordPeak(
      this.peaks,
      diagnostics!.host,
      diagnostics!.sidecar?.transport ?? null,
    );
  }

  transportSnapshot() {
    return this.#session.snapshot();
  }

  releaseUpstream(): void {
    this.upstream.release();
  }
}

function deterministicPadding(length: number): string {
  if (length <= 0) return "";
  const pattern = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
  return pattern.repeat(Math.ceil(length / pattern.length)).slice(0, length);
}

export function createExactSizedV2Request(
  requestBytes: number,
  index = 0,
): Record<string, unknown> {
  if (!Number.isSafeInteger(requestBytes) || requestBytes < 1) {
    throw new RangeError("requestBytes must be a positive safe integer");
  }
  if (!Number.isSafeInteger(index) || index < 0 || index > 999_999) {
    throw new RangeError("request index must be 0 through 999999");
  }
  const prefix = `case-${String(index).padStart(6, "0")}:`;
  const request: Record<string, unknown> = {
    model: "gpt-5.6-sol",
    input: prefix,
    service_tier: "fast",
    store: false,
    stream: true,
  };
  const fixedBytes = utf8Bytes(canonicalJson(request)).byteLength;
  if (requestBytes < fixedBytes) {
    throw new RangeError(`requestBytes must be at least ${fixedBytes}`);
  }
  request.input = prefix + deterministicPadding(requestBytes - fixedBytes);
  const actual = utf8Bytes(canonicalJson(request)).byteLength;
  if (actual !== requestBytes) {
    throw new Error(`Exact request construction failed: ${actual} != ${requestBytes}`);
  }
  return request;
}

function percentile(values: readonly number[], quantile: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * quantile) - 1)]!;
}

function summary(values: readonly number[]): V2HarnessTimingSummary {
  return {
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    max: percentile(values, 1),
  };
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() >= deadline) throw new Error(`${label} timed out`);
    await Bun.sleep(1);
  }
}

function validateOptions(options: V2PerformanceHarnessOptions): void {
  if (!Number.isFinite(options.rttMs) || options.rttMs <= 0) {
    throw new RangeError("rttMs must be positive");
  }
  if (
    !Number.isSafeInteger(options.requestBytes) ||
    options.requestBytes < 1 ||
    options.requestBytes > GP_V2_REQUEST_MAX_BYTES
  ) {
    throw new RangeError("requestBytes is outside the V2 request boundary");
  }
  if (
    !Number.isSafeInteger(options.concurrency) ||
    options.concurrency < 1 ||
    options.concurrency > 128
  ) {
    throw new RangeError("concurrency must be 1 through 128");
  }
  const aggregateLimit = options.maxAggregateRequestBytes ?? 64 * MiB;
  if (!Number.isSafeInteger(aggregateLimit) || aggregateLimit < 1) {
    throw new RangeError("maxAggregateRequestBytes must be a positive safe integer");
  }
  if (options.requestBytes * options.concurrency > aggregateLimit) {
    throw new RangeError("aggregate request bytes exceed the harness safety limit");
  }
  if (
    options.timeoutMs !== undefined &&
    (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1)
  ) {
    throw new RangeError("timeoutMs must be a positive safe integer");
  }
  for (const fault of options.faults ?? []) {
    if (!Number.isSafeInteger(fault.ordinal) || fault.ordinal < 1) {
      throw new RangeError("fault ordinal must be a positive safe integer");
    }
    if (fault.actions.length === 0) {
      throw new RangeError("fault actions must not be empty");
    }
  }
  for (const disconnect of options.transientDisconnects ?? []) {
    if (
      !Number.isSafeInteger(disconnect.fromOrdinal) ||
      disconnect.fromOrdinal < 1 ||
      !Number.isSafeInteger(disconnect.publicationCount) ||
      disconnect.publicationCount < 1
    ) {
      throw new RangeError("transient disconnect bounds must be positive safe integers");
    }
  }
}

export async function runV2PerformanceHarness(
  options: V2PerformanceHarnessOptions,
): Promise<V2PerformanceHarnessReport> {
  validateOptions(options);
  const timeoutMs = options.timeoutMs ?? 20_000;
  const clock = new HarnessClock();
  const hub = new FaultyAwarenessHub(options, clock);
  const runtime = await PerformanceRuntime.create(options, clock, hub);
  const registry = new SandboxRegistry([runtime]);
  const governor = new ResourceGovernor({
    probe: {
      sample: () => ({
        totalBytes: 64 * 1024 ** 3,
        freeBytes: 52 * 1024 ** 3,
        rssBytes: 256 * MiB,
        heapBytes: 64 * MiB,
        externalBytes: 16 * MiB,
        logicalProcessors: 16,
      }),
    },
  });
  const router = new MultiSandboxRouter({
    registry,
    governor,
    clock,
    queueTimeoutMs: timeoutMs,
    random: {
      next: () => 0.5,
      uuid: (() => {
        let id = 0;
        return () => `perf-trace-${id++}`;
      })(),
    },
  });
  try {
    await runtime.start();
    const bodies = Array.from({ length: options.concurrency }, (_, index) =>
      createExactSizedV2Request(options.requestBytes, index));
    const expectedDigests = await Promise.all(bodies.map(body =>
      sha256Hex(utf8Bytes(canonicalJson(body)))));
    const starts = bodies.map(() => clock.now());
    const committed = bodies.map(async (body, index) => {
      const handle = await router.startRelay(body, {
        clientId: `perf-client-${index}`,
        source: "http",
      });
      const commitMs = clock.now() - starts[index]!;
      const trace = hub.requestTraceByDigest(expectedDigests[index]!);
      const events = (async () => {
        let firstEventMs: number | null = null;
        const transcript = new TranscriptProof(
          `perf-observed-${index}`,
          expectedDigests[index]!,
        );
        let eventCount = 0;
        for await (const event of handle.events()) {
          if (firstEventMs === null) firstEventMs = clock.now() - starts[index]!;
          transcript.append(event);
          eventCount++;
        }
        const terminal = await transcript.createTerminal();
        return {
          firstEventMs: firstEventMs ?? clock.now() - starts[index]!,
          eventCount,
          transcriptDigest: terminal.transcriptSha256,
          finalDigest: terminal.finalSha256,
        };
      })();
      return {
        index,
        handle,
        commitMs,
        ackRoundsAtCommit: trace?.flights.size ?? 0,
        events,
      };
    });
    const handles = await withTimeout(
      Promise.all(committed),
      timeoutMs,
      "V2 harness commitment",
    );
    await waitFor(
      () => runtime.upstream.active === options.concurrency,
      timeoutMs,
      "V2 harness upstream concurrency",
    );
    runtime.releaseUpstream();
    const requests = await withTimeout(Promise.all(handles.map(async value => {
      const [final, observed] = await Promise.all([
        value.handle.final,
        value.events,
      ]);
      const upstream = runtime.upstream.proofs.get(value.index);
      if (!upstream) throw new Error(`Missing upstream proof for request ${value.index}`);
      const finalDigest = await sha256Hex(canonicalJson(final));
      const requestDigest = expectedDigests[value.index]!;
      return {
        index: value.index,
        requestBytes: options.requestBytes,
        requestDigest,
        upstreamDigest: upstream.requestDigest,
        commitMs: value.commitMs,
        firstEventMs: observed.firstEventMs,
        totalMs: clock.now() - starts[value.index]!,
        ackRoundsAtCommit: value.ackRoundsAtCommit,
        eventCount: observed.eventCount,
        transcriptDigest: observed.transcriptDigest,
        upstreamTranscriptDigest: upstream.transcriptDigest,
        finalDigest,
        upstreamFinalDigest: upstream.finalDigest,
        exactRequest: requestDigest === upstream.requestDigest,
        exactTerminal:
          observed.transcriptDigest === upstream.transcriptDigest &&
          finalDigest === upstream.finalDigest,
      } satisfies V2HarnessRequestResult;
    })), timeoutMs, "V2 harness terminal");
    await waitFor(
      () => runtime.transportSnapshot().active === 0,
      timeoutMs,
      "V2 harness idle transport",
    );
    await waitFor(() => {
      const value = runtime.transportSnapshot().diagnostics!.host;
      return value.windowFrames === 0 && value.schedulerFrames === 0;
    }, timeoutMs, "V2 harness terminal ACK drain");
    runtime.sample();
    const transport = runtime.transportSnapshot();
    const host = transport.diagnostics!.host;
    const sidecar = transport.diagnostics!.sidecar?.transport ?? null;
    return {
      rttMs: options.rttMs,
      requestBytes: options.requestBytes,
      concurrency: options.concurrency,
      virtualElapsedMs: clock.now(),
      requests,
      timings: {
        commitMs: summary(requests.map(value => value.commitMs)),
        firstEventMs: summary(requests.map(value => value.firstEventMs)),
        totalMs: summary(requests.map(value => value.totalMs)),
      },
      upstream: {
        calls: runtime.upstream.calls,
        maxActive: runtime.upstream.maxActive,
      },
      transport: {
        retransmissions: transport.retransmissions,
        ackRounds: host.flightCompletionsTotal,
        awarenessRttMs: transport.awarenessRttMs,
        host,
        sidecar,
        peaks: { ...runtime.peaks },
      },
      wire: hub.snapshot(),
      router: router.snapshot(),
    };
  } finally {
    runtime.releaseUpstream();
    router.beginShutdown();
    await runtime.stop();
    await router.close();
    hub.close();
  }
}
