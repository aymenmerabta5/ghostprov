import type { Clock } from "../../src/core/ports";
import { gzipSync } from "node:zlib";
import type { RunHandle } from "../../src/relay/run-handle";
import type { SseFrame } from "../../src/relay/sse";
import { ResourceGovernor } from "../../src/router/resource-governor";
import { MultiSandboxRouter } from "../../src/router/router";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
  type SandboxRunOptions,
} from "../../src/sandbox/registry";
import { projectId, type ProjectId } from "../../src/sandbox/types";
import { parseYSweetEnvelope } from "../../src/prism/protocol";
import { SplitAwarenessChannel } from "../../src/v2/awareness/split-channel";
import {
  YSweetAwarenessChannel,
  type AwarenessProviderLike,
} from "../../src/v2/awareness/y-sweet-channel";
import {
  createCredentialRotationKey,
  sealCredentialRotation,
  type BootstrapCredentialPayload,
} from "../../src/v2/bootstrap/crypto";
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
  type ProtocolRevision,
  type RequestEncoding,
  type SidecarFrame,
  type V2TransportProfile,
} from "../../src/v2/protocol/shared";
import { AdmissionController } from "../../src/v2/sidecar/admission";
import { RequestAssembler } from "../../src/v2/sidecar/assembler";
import { SidecarEngine } from "../../src/v2/sidecar/engine";
import { SidecarEngine as R10SidecarEngine } from "../../src/v2/sidecar/r10-engine";
import { OutputBackpressure } from "../../src/v2/sidecar/output-sink";
import { ClaimJournal } from "../../src/v2/sidecar/journal";
import type {
  DirectResponsesExecution,
  UpstreamTerminal,
} from "../../src/v2/sidecar/upstream";
import { TranscriptProof } from "../../src/v2/protocol/transcript";
import type {
  PublicationTimer,
  PublicationTimerHandle,
} from "../../src/v2/protocol/publication-pacer";
import { parseAwarenessEnvelope } from "../../src/v2/protocol/validate";
import { V2TransportSession } from "../../src/v2/transport/session";
import {
  DenoAwarenessPeer,
  type DenoAwarenessProviderLike,
} from "../../sandbox/v2/awareness-peer";
import { DenoAwarenessPeer as R10DenoAwarenessPeer } from "../../sandbox/v2/r10-awareness-peer";
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
const DEFAULT_LINK_BANDWIDTH_BYTES_PER_SECOND = 25 * MiB;
const DEFAULT_SERIALIZATION_BYTES_PER_SECOND = 250 * MiB;

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
export type V2PerformanceCorpus = "compressible" | "mixed" | "incompressible";

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
  /** Selects the real mutable R10 engine/peer, including its output backpressure. */
  sidecarRevision?: "r8" | "r10";
  /** Defaults to R2. R1 selects source-level wire limits, not the frozen bundle. */
  protocolRevision?: ProtocolRevision;
  transportProfile?: V2TransportProfile;
  rttMs: number;
  /** Homogeneous size, or the report label/default when requestBytesByRequest is supplied. */
  requestBytes: number;
  concurrency: number;
  /** Optional exact heterogeneous corpus; length must equal concurrency. */
  requestBytesByRequest?: readonly number[];
  /** Logical SSE events emitted per response, including created/completed. */
  responseEventCount?: number;
  /** Bounded response delta payload; zero preserves the small digest fixture. */
  responseDeltaBytes?: number;
  /** Deterministic delay between streamed response events. */
  responseIntervalMs?: number;
  /** Emit responses while later requests are still being admitted. */
  streamWhileAdmitting?: boolean;
  corpus?: V2PerformanceCorpus;
  linkBandwidthBytesPerSecond?: number;
  serializationBytesPerSecond?: number;
  /** Rotate the sidecar Y-Sweet credential after request data starts moving. */
  rotateCredentialDuringTransfer?: boolean;
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
  encodedRequestBytes: number;
  requestEncoding: RequestEncoding;
  requestDigest: string;
  upstreamDigest: string;
  /** Deterministic virtual network/serialization model; excludes host CPU. */
  modeledCommitMs: number;
  modeledFirstEventMs: number;
  modeledTotalMs: number;
  /** Measured wall time through the real host preparation and protocol code. */
  wallCommitMs: number;
  wallFirstEventMs: number;
  wallTotalMs: number;
  /** Wall time through the first encoded Yjs request publication. */
  hostPreparationWallMs: number;
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
  /** Host data-lane bytes for request open/data/commit flights only. */
  hostRequestDataAttemptedBytes: number;
  deliveredBytes: number;
  droppedUpdates: number;
  disconnectedDrops: number;
  duplicateDeliveries: number;
  reorderedUpdates: number;
  usefulRequestBytes: number;
  postCompressionRequestBytes: number;
  uniqueRequestDataBytes: number;
  hostDataPublications: number;
  sidecarDataPublications: number;
}

export interface V2PerformanceHarnessReport {
  /** This harness executes mutable TypeScript production components. */
  implementation: "mutable-source-components";
  protocolRevision: ProtocolRevision;
  transportProfile: V2TransportProfile;
  corpus: V2PerformanceCorpus;
  rttMs: number;
  requestBytes: number;
  concurrency: number;
  virtualElapsedMs: number;
  link: {
    bandwidthBytesPerSecond: number;
    serializationBytesPerSecond: number;
  };
  operations: {
    credentialRotations: number;
  };
  requests: readonly V2HarnessRequestResult[];
  timings: {
    modeledLink: {
      commitMs: V2HarnessTimingSummary;
      firstEventMs: V2HarnessTimingSummary;
      totalMs: V2HarnessTimingSummary;
    };
    measuredWall: {
      commitMs: V2HarnessTimingSummary;
      firstEventMs: V2HarnessTimingSummary;
      totalMs: V2HarnessTimingSummary;
      hostPreparationMs: V2HarnessTimingSummary;
    };
  };
  preparation: {
    gzipCalls: number;
    gzipWallMs: number;
    unresolvedSessionRuns: number;
    peakUnresolvedSessionRuns: number;
  };
  upstream: {
    calls: number;
    maxActive: number;
  };
  responseEventsEmitted: number;
  admissionReopenedDuringResponses: number;
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

export interface V2FleetPerformanceHarnessOptions
extends Omit<V2PerformanceHarnessOptions, "concurrency" | "requestBytesByRequest"> {
  runtimeCount: number;
  unavailableRuntimeCount?: number;
  requestBytesByRequest: readonly number[];
}

export interface V2FleetPerformanceHarnessReport {
  runtimeCount: number;
  usableRuntimeCount: number;
  assignments: readonly number[];
  reports: readonly V2PerformanceHarnessReport[];
  requests: readonly (V2HarnessRequestResult & { runtimeIndex: number })[];
  modeledCommitMs: V2HarnessTimingSummary;
  modeledTotalMs: V2HarnessTimingSummary;
  exactRequests: boolean;
  exactTerminals: boolean;
  upstreamMaxActive: number;
  retransmissions: number;
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
  encoding: RequestEncoding;
  uniqueDataBytes: number;
  parts: Set<number>;
  flights: Set<string>;
  firstPublicationWallMs: number | null;
}

interface PreparationWallMetrics {
  gzipCalls: number;
  gzipWallMs: number;
}

interface HubEndpointMetadata {
  role: "host" | "sidecar";
  lane: AwarenessLane;
  label: string;
}

interface QueuedWorkBase {
  id: number;
  dueAtMs: number;
}

interface QueuedUpdate extends QueuedWorkBase {
  kind: "delivery";
  target: HarnessAwarenessProvider | null;
  update: Uint8Array;
  bytes: number;
}

interface HarnessPublicationTimerHandle {
  readonly id: number;
  cancelled: boolean;
}

interface QueuedPublication extends QueuedWorkBase {
  kind: "publication";
  callback: () => void;
  handle: HarnessPublicationTimerHandle | null;
}

type QueuedWork = QueuedUpdate | QueuedPublication;

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
  readonly #queue: QueuedWork[] = [];
  readonly #heldReorders = new Map<string, QueuedUpdate[]>();
  readonly #ordinals = new Map<string, number>();
  readonly #requestTraces = new Map<string, WireRequestTrace>();
  readonly #digestToRequest = new Map<string, string>();
  readonly #activity = new Set<() => void>();
  readonly #linkAvailableAt = new Map<string, number>();
  readonly #bandwidthBytesPerSecond: number;
  readonly #serializationBytesPerSecond: number;
  #hostPreparing: (() => number) | null = null;
  #preparationPauseObserved = false;
  #stablePreparationTurns = 0;
  #nextWorkId = 1;
  #pump: ReturnType<typeof setTimeout> | null = null;
  #closed = false;
  #attemptedPublications = 0;
  #deliveredUpdates = 0;
  #attemptedBytes = 0;
  #hostRequestDataAttemptedBytes = 0;
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
    this.#bandwidthBytesPerSecond = options.linkBandwidthBytesPerSecond ??
      DEFAULT_LINK_BANDWIDTH_BYTES_PER_SECOND;
    this.#serializationBytesPerSecond = options.serializationBytesPerSecond ??
      DEFAULT_SERIALIZATION_BYTES_PER_SECOND;
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

  publicationTimer(
    owner: "host" | "sidecar",
    preparing?: () => number,
  ): PublicationTimer {
    if (owner === "host" && preparing) this.#hostPreparing = preparing;
    return {
      now: () => this.#clock.now(),
      set: (callback, delayMs) => {
        const handle: HarnessPublicationTimerHandle = {
          id: this.#nextWorkId++,
          cancelled: this.#closed,
        };
        if (!handle.cancelled) {
          this.#queue.push({
            kind: "publication",
            id: handle.id,
            dueAtMs: this.#clock.now() + Math.max(0, delayMs),
            callback,
            handle,
          });
          this.#schedulePump();
        }
        return handle;
      },
      clear: (opaque: PublicationTimerHandle) => {
        if (!opaque || typeof opaque !== "object") return;
        (opaque as HarnessPublicationTimerHandle).cancelled = true;
      },
      defer: callback => {
        if (this.#closed) return;
        this.#queue.push({
          kind: "publication",
          id: this.#nextWorkId++,
          dueAtMs: this.#clock.now(),
          callback,
          handle: null,
        });
        this.#schedulePump();
      },
    };
  }

  onActivity(listener: () => void): () => void {
    this.#activity.add(listener);
    return () => this.#activity.delete(listener);
  }

  delay(ms: number): Promise<void> {
    return new Promise(resolve => { this.publicationTimer("sidecar").set(resolve, ms); });
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
    let postCompressionRequestBytes = 0;
    let uniqueRequestDataBytes = 0;
    for (const trace of this.#requestTraces.values()) {
      usefulRequestBytes += trace.decodedBytes;
      postCompressionRequestBytes += trace.encodedBytes;
      uniqueRequestDataBytes += trace.uniqueDataBytes;
    }
    return {
      attemptedPublications: this.#attemptedPublications,
      deliveredUpdates: this.#deliveredUpdates,
      attemptedBytes: this.#attemptedBytes,
      hostRequestDataAttemptedBytes: this.#hostRequestDataAttemptedBytes,
      deliveredBytes: this.#deliveredBytes,
      droppedUpdates: this.#droppedUpdates,
      disconnectedDrops: this.#disconnectedDrops,
      duplicateDeliveries: this.#duplicateDeliveries,
      reorderedUpdates: this.#reorderedUpdates,
      usefulRequestBytes,
      postCompressionRequestBytes,
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
    const dueAtMs = this.#deliveryDueAt(key, update.byteLength);
    if (direction === "host-to-sidecar" && lane === "data") {
      this.#hostDataPublications++;
      if (this.#trackHostDataEnvelope(source)) {
        this.#hostRequestDataAttemptedBytes += update.byteLength;
      }
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
      this.#scheduleClockOnly(dueAtMs);
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
      this.#scheduleClockOnly(dueAtMs);
      return;
    }

    const queued: QueuedUpdate = {
      kind: "delivery",
      id: this.#nextWorkId++,
      dueAtMs,
      target,
      update: update.slice(),
      bytes: update.byteLength,
    };
    const copies = actions.has("duplicate") ? 2 : 1;
    if (actions.has("duplicate")) this.#duplicateDeliveries++;
    const deliveries = Array.from({ length: copies }, (_, index) => ({
      ...queued,
      id: index === 0 ? queued.id : this.#nextWorkId++,
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

  #deliveryDueAt(key: string, bytes: number): number {
    const now = this.#clock.now();
    const startsAt = Math.max(now, this.#linkAvailableAt.get(key) ?? now);
    const serviceMs = bytes * 1_000 / this.#bandwidthBytesPerSecond +
      bytes * 1_000 / this.#serializationBytesPerSecond;
    const completedAt = startsAt + serviceMs;
    this.#linkAvailableAt.set(key, completedAt);
    return completedAt + this.#rttMs / 2;
  }

  #scheduleClockOnly(dueAtMs: number): void {
    this.#queue.push({
      kind: "delivery",
      id: this.#nextWorkId++,
      dueAtMs,
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
    for (let index = this.#queue.length - 1; index >= 0; index--) {
      const item = this.#queue[index]!;
      if (item.kind === "publication" && item.handle?.cancelled) {
        this.#queue.splice(index, 1);
      }
    }
    this.#queue.sort((left, right) =>
      left.dueAtMs - right.dueAtMs || left.id - right.id);
    const preparing = Math.max(0, Math.min(128, this.#hostPreparing?.() ?? 0));
    if (preparing > 0) {
      // Host CPU preparation is measured separately from the virtual model.
      // Do not let wall-time hash completion decide how far the modeled link
      // advances before the next prepared request becomes publishable.
      this.#preparationPauseObserved = true;
      this.#stablePreparationTurns = 0;
      this.#schedulePump();
      return;
    }
    if (this.#preparationPauseObserved && this.#stablePreparationTurns === 0) {
      this.#stablePreparationTurns = 1;
      this.#schedulePump();
      return;
    }
    this.#preparationPauseObserved = false;
    this.#stablePreparationTurns = 0;
    const next = this.#queue[0];
    if (!next) return;
    const item = this.#queue.shift()!;
    this.#clock.advanceTo(Math.max(this.#clock.now(), item.dueAtMs));
    if (item.kind === "publication") {
      if (!item.handle?.cancelled) item.callback();
    } else if (item.bytes > 0 && item.target?.connected) {
      applyAwarenessUpdate(item.target.awareness, item.update, this);
      this.#deliveredUpdates++;
      this.#deliveredBytes += item.bytes;
    }
    for (const listener of this.#activity) listener();
    if (this.#queue.length > 0) this.#schedulePump();
  }

  #trackHostDataEnvelope(source: HarnessAwarenessProvider): boolean {
    const envelope = source.awareness.getLocalState()
      ?.ghostprovV2Data as AwarenessEnvelope<HostFrame> | undefined;
    if (!envelope || envelope.frames.length === 0) return false;
    const containsRequestData = envelope.frames.some(frame =>
      frame.kind === "request_open" ||
      frame.kind === "request_data" ||
      frame.kind === "request_commit" ||
      frame.kind === "request_start");
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
            encoding: frame.encoding,
            uniqueDataBytes: 0,
            parts: new Set(),
            flights: new Set(),
            firstPublicationWallMs: performance.now(),
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
    return containsRequestData;
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
  emittedEvents = 0;
  readonly proofs = new Map<number, UpstreamProof>();
  readonly #gate = new HoldGate();

  constructor(
    readonly responseEventCount = 2,
    readonly responseDeltaBytes = 0,
    readonly pause: (() => Promise<void>) | null = null,
  ) {}

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
      const delta = this.responseDeltaBytes > 0 ? "x".repeat(this.responseDeltaBytes) : null;
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
            text: delta === null ? `ok:${index}:${requestDigest}` : delta.repeat(this.responseEventCount - 2),
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
        ...Array.from({ length: this.responseEventCount - 2 }, (_, eventIndex) => ({
          event: "response.output_text.delta",
          data: JSON.stringify({
            type: "response.output_text.delta",
            delta: delta ?? `${index}:${eventIndex}`,
          }),
        })),
        {
          event: "response.completed",
          data: JSON.stringify({ type: "response.completed", response: final }),
        },
      ];
      const transcript = new TranscriptProof("upstream", "0".repeat(64), { retainBytes: false });
      for (const event of events) {
        if (this.pause) await this.pause();
        await transcript.appendCanonicalStream(event, { onChunk: input.onTranscriptBytes });
        await input.onEvent(event);
        this.emittedEvents++;
      }
      const terminal = await transcript.createTerminal();
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

type PerformancePeer = Pick<DenoAwarenessPeer, keyof DenoAwarenessPeer>;
type PerformanceEngine = Pick<SidecarEngine, keyof SidecarEngine>;

class PerformanceRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId = PROJECT;
  readonly upstream: DigestingUpstream;
  readonly peaks = emptyPeaks();
  admissionReopenedDuringResponses = 0;
  #lastAdmissionCredits: number | null = null;
  readonly #hub: FaultyAwarenessHub;
  readonly #session: V2TransportSession;
  readonly #peer: PerformancePeer;
  readonly #engine: PerformanceEngine;
  readonly #journal: ClaimJournal;
  readonly #sampler: SidecarResourceSampler;
  readonly #binding: ProtocolBinding;
  readonly #transportProfile: V2TransportProfile;
  readonly #credential: BootstrapCredentialPayload;
  readonly #peerErrors: Error[];
  readonly #preparationWall: PreparationWallMetrics;
  readonly #physical: number;
  readonly #listeners = new Set<() => void>();
  #unresolvedSessionRuns = 0;
  #peakUnresolvedSessionRuns = 0;
  #state = "configured";
  #lastRoutingSignal = "";
  #credentialRotations = 0;
  #unsubscribeActivity: (() => void) | null = null;

  private constructor(options: {
    hub: FaultyAwarenessHub;
    session: V2TransportSession;
    peer: PerformancePeer;
    engine: PerformanceEngine;
    upstream: DigestingUpstream;
    journal: ClaimJournal;
    sampler: SidecarResourceSampler;
    binding: ProtocolBinding;
    transportProfile: V2TransportProfile;
    credential: BootstrapCredentialPayload;
    peerErrors: Error[];
    preparationWall: PreparationWallMetrics;
    physical: number;
  }) {
    this.#hub = options.hub;
    this.#session = options.session;
    this.#peer = options.peer;
    this.#engine = options.engine;
    this.upstream = options.upstream;
    this.#journal = options.journal;
    this.#sampler = options.sampler;
    this.#binding = options.binding;
    this.#transportProfile = options.transportProfile;
    this.#credential = options.credential;
    this.#peerErrors = options.peerErrors;
    this.#preparationWall = options.preparationWall;
    this.#physical = options.physical;
    this.#unsubscribeActivity = this.#hub.onActivity(() => {
      this.sample();
      this.#notifyIfRoutingChanged();
    });
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
    const requestSizes = performanceRequestSizes(options);
    const retransmitMs = faulted
      ? 5
      : Math.max(1_000, Math.ceil(Math.max(...requestSizes) / MiB) * 500);
    const protocolRevision = options.protocolRevision ?? "r2";
    const transportProfile = options.transportProfile ?? "standard";
    const bindingBase = {
      projectId: PROJECT,
      sessionId: "sandbox-session-perf",
      leaderGeneration: 7,
      buildHash: "a".repeat(64),
      schemaHash: await protocolSchemaHash(protocolRevision, transportProfile),
      launchNonce: LAUNCH_NONCE,
    };
    const binding: ProtocolBinding = protocolRevision === "r2"
      ? { ...bindingBase, protocolRevision: "r2" }
      : { ...bindingBase, protocolRevision: "r1" };
    const preparationWall: PreparationWallMetrics = {
      gzipCalls: 0,
      gzipWallMs: 0,
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
      parseRemote: value => parseAwarenessEnvelope(
        value,
        binding,
        "sidecar",
        transportProfile,
      ),
      providerFactory: doc => hub.create(doc, "host", lane),
      syncTimeoutMs: 1_000,
    });
    const channel = new SplitAwarenessChannel({
      data: hostLane("data", "ghostprovV2Data"),
      control: hostLane("control", "ghostprovV2Control"),
    });
    let session!: V2TransportSession;
    const hostPublicationTimer = hub.publicationTimer(
      "host",
      () => session.snapshot().preparing ?? 0,
    );
    session = new V2TransportSession({
      binding,
      transportProfile,
      channel,
      clock,
      maxResponseBytes: 2 * MiB,
      maxBufferedResponseBytes: 512 * KiB,
      hostCeiling: options.concurrency,
      retransmitMs,
      publicationIntervalMs: options.sidecarRevision === "r10" ? 25 : 1,
      publicationTimer: hostPublicationTimer,
      unadmittedTimeoutMs: options.timeoutMs ?? 20_000,
      gzip: async bytes => {
        const startedAt = performance.now();
        preparationWall.gzipCalls++;
        try {
          return Uint8Array.from(gzipSync(bytes, { level: 1 }));
        } finally {
          preparationWall.gzipWallMs += performance.now() - startedAt;
        }
      },
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
    const aggregate = requestSizes.reduce((total, bytes) => total + bytes, 0);
    // Assembly owns decoded + encoded bytes plus a dense part index. Keep
    // explicit metadata/headroom so an exact 64 MiB request is not rejected by
    // the harness budget before the production protocol boundary is tested.
    const sidecarMemoryBudget = Math.max(
      64 * MiB,
      aggregate * 3,
      options.concurrency * 4 * MiB,
    );
    const admission = new AdmissionController({
      physicalMax: options.concurrency,
      acceptedMax: Math.min(128, options.concurrency * 2),
      hostCeiling: options.concurrency,
      memoryBudgetBytes: sidecarMemoryBudget,
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
    let peer!: PerformancePeer;
    const r10 = options.sidecarRevision === "r10";
    const outputBackpressure = r10 ? new OutputBackpressure({
      highFrames: 256, highBytes: 256 * KiB, lowFrames: 64, lowBytes: 64 * KiB,
      hardFrames: 1024, hardBytes: MiB,
    }) : undefined;
    const upstream = new DigestingUpstream(
      options.responseEventCount ?? 2,
      options.responseDeltaBytes ?? 0,
      options.responseIntervalMs ? () => hub.delay(options.responseIntervalMs!) : null,
    );
    const Engine = r10 ? R10SidecarEngine : SidecarEngine;
    const engine = new Engine({
      binding,
      assembler: new RequestAssembler({
        maxReservedBytes: sidecarMemoryBudget,
      }),
      journal,
      admission,
      upstream,
      diagnostics,
      batching: true,
      ...(outputBackpressure ? { outputBackpressure } : {}),
      assertExecutionFence: frame => peer.assertExecutionFence(frame),
      emit: (frame, priority, streamId) => peer.enqueue(frame, priority, streamId),
    });
    const credential: BootstrapCredentialPayload = {
      url: ySweet.url,
      baseUrl: ySweet.baseUrl,
      docId: ySweet.docId,
      token: ySweet.token.reveal(),
      authorization: "full",
      rotationKey: createCredentialRotationKey(),
    };
    const peerErrors: Error[] = [];
    const Peer = r10 ? R10DenoAwarenessPeer : DenoAwarenessPeer;
    peer = new Peer({
      credential,
      binding,
      transportProfile,
      instanceId: SIDECAR_INSTANCE,
      engine,
      diagnostics,
      ...(outputBackpressure ? { outputBackpressure } : {}),
      ...(r10 ? { now: () => clock.now(), publicationTimer: hub.publicationTimer("sidecar") } : {}),
      providerFactory: (doc, _credential, lane) => hub.create(doc, "sidecar", lane),
      publicationIntervalMs: r10 ? 25 : 1,
      retransmitMs,
      connectTimeoutMs: 1_000,
      closeTimeoutMs: 2_000,
      initialActivePerSandbox: options.concurrency,
      maxActivePerSandbox: options.concurrency,
      onAcceptError: error => peerErrors.push(error),
      onEngineError: error => peerErrors.push(error),
    });
    const runtime = new PerformanceRuntime({
      hub,
      session,
      peer,
      engine,
      upstream,
      journal,
      sampler,
      binding,
      transportProfile,
      credential,
      peerErrors,
      preparationWall,
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
    this.#notifyIfRoutingChanged();
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
    this.#listeners.clear();
    this.#state = "stopped";
  }

  run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.#unresolvedSessionRuns++;
    this.#peakUnresolvedSessionRuns = Math.max(
      this.#peakUnresolvedSessionRuns,
      this.#unresolvedSessionRuns,
    );
    try {
      return this.#session.run(body, signal, options).finally(() => {
        this.#unresolvedSessionRuns = Math.max(0, this.#unresolvedSessionRuns - 1);
      });
    } catch (error) {
      this.#unresolvedSessionRuns = Math.max(0, this.#unresolvedSessionRuns - 1);
      throw error;
    }
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async rotateCredential(): Promise<void> {
    const rotationId = `perf-rotation-${this.#credentialRotations + 1}`;
    const rotationBindingBase = {
      projectId: this.#binding.projectId,
      sandboxSessionId: this.#binding.sessionId,
      leaderGeneration: this.#binding.leaderGeneration,
      buildHash: this.#binding.buildHash,
      schemaHash: this.#binding.schemaHash,
      instanceId: SIDECAR_INSTANCE,
      rotationId,
    };
    const rotationBinding = this.#binding.protocolRevision === "r2"
      ? {
          ...rotationBindingBase,
          protocolRevision: "r2" as const,
          schemaVersion: 2 as const,
          launchNonce: this.#binding.launchNonce,
        }
      : rotationBindingBase;
    const sealed = await sealCredentialRotation({
      rotationKey: this.#credential.rotationKey,
      binding: rotationBinding,
      credential: {
        url: this.#credential.url,
        baseUrl: this.#credential.baseUrl,
        docId: this.#credential.docId,
        token: `rotated-perf-token-${rotationId}-${"x".repeat(32)}`,
        authorization: this.#credential.authorization,
      },
    });
    await this.#session.rotateCredential(rotationId, JSON.stringify(sealed));
    this.#credentialRotations++;
  }

  get credentialRotations(): number {
    return this.#credentialRotations;
  }

  peerErrorMessages(): readonly string[] {
    return this.#peerErrors.map(error => `${error.name}:${error.message}`);
  }

  preparationWallMetrics(): PreparationWallMetrics {
    return { ...this.#preparationWall };
  }

  sessionRunMetrics(): {
    unresolvedSessionRuns: number;
    peakUnresolvedSessionRuns: number;
  } {
    return {
      unresolvedSessionRuns: this.#unresolvedSessionRuns,
      peakUnresolvedSessionRuns: this.#peakUnresolvedSessionRuns,
    };
  }

  #notifyIfRoutingChanged(): void {
    const transport = this.#session.snapshot();
    const capacity = this.#state === "ready" ? this.#physical : 0;
    const prefetched = transport.prefetched ?? 0;
    const remaining = Math.max(
      0,
      capacity - transport.active - prefetched - transport.quarantined,
    );
    const admissionCredits = this.#state === "ready"
      ? Math.max(0, Math.min(
        transport.admissionCredits ?? transport.credits,
        remaining,
      ))
      : 0;
    const prefetchCredits = this.#state === "ready"
      ? Math.max(0, Math.min(
        transport.prefetchCredits ?? transport.admissionCredits ?? transport.credits,
        remaining,
      ))
      : 0;
    const available = Math.max(admissionCredits, prefetchCredits);
    const signal = [
      this.#state,
      capacity,
      transport.active,
      prefetched,
      available,
      admissionCredits,
      prefetchCredits,
      transport.transferCredits,
      transport.queued,
      transport.quarantined,
    ].join(":");
    if (signal === this.#lastRoutingSignal) return;
    this.#lastRoutingSignal = signal;
    for (const listener of this.#listeners) listener();
  }

  snapshot(): RoutableSandboxSnapshot {
    const transport = this.#session.snapshot();
    this.sample();
    const capacity = this.#state === "ready" ? this.#physical : 0;
    const prefetched = transport.prefetched ?? 0;
    const remaining = Math.max(
      0,
      capacity - transport.active - prefetched - transport.quarantined,
    );
    const admissionCredits = this.#state === "ready"
      ? Math.max(0, Math.min(
        transport.admissionCredits ?? transport.credits,
        remaining,
      ))
      : 0;
    const prefetchCredits = this.#state === "ready"
      ? Math.max(0, Math.min(
        transport.prefetchCredits ?? transport.admissionCredits ?? transport.credits,
        remaining,
      ))
      : 0;
    return {
      projectId: this.projectId,
      transportProfile: this.#transportProfile,
      state: this.#state,
      capacity,
      active: transport.active,
      available: Math.max(admissionCredits, prefetchCredits),
      admissionCredits,
      prefetchCredits,
      transferCredits: transport.transferCredits,
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
    const snapshot = this.#session.snapshot();
    const credits = snapshot.admissionCredits ?? snapshot.credits;
    if (this.#lastAdmissionCredits === 0 && credits > 0 &&
        this.upstream.active > 0 && this.upstream.emittedEvents > 0) {
      this.admissionReopenedDuringResponses++;
    }
    this.#lastAdmissionCredits = credits;
    const diagnostics = snapshot.diagnostics;
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

function compressiblePadding(length: number): string {
  if (length <= 0) return "";
  const pattern = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-";
  return pattern.repeat(Math.ceil(length / pattern.length)).slice(0, length);
}

const JSON_SAFE_PRINTABLE = Array.from({ length: 95 }, (_, index) => index + 32)
  .filter(code => code !== 0x22 && code !== 0x5c)
  .map(code => String.fromCharCode(code))
  .join("");

function corpusPadding(
  length: number,
  index: number,
  corpus: V2PerformanceCorpus,
): string {
  if (corpus === "compressible") return compressiblePadding(length);
  const chunks: string[] = [];
  let state = (0x9e37_79b9 ^ (index + 1) * 0x85eb_ca6b) >>> 0;
  for (let offset = 0; offset < length;) {
    const size = Math.min(8_192, length - offset);
    if (corpus === "mixed" && Math.floor(offset / 16_384) % 2 === 0) {
      chunks.push(compressiblePadding(size));
    } else {
      const characters = new Array<string>(size);
      for (let position = 0; position < size; position++) {
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        characters[position] = JSON_SAFE_PRINTABLE[(state >>> 0) % JSON_SAFE_PRINTABLE.length]!;
      }
      chunks.push(characters.join(""));
    }
    offset += size;
  }
  return chunks.join("");
}

export function createExactSizedV2Request(
  requestBytes: number,
  index = 0,
  corpus: V2PerformanceCorpus = "compressible",
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
    store: true,
    stream: true,
  };
  const fixedBytes = utf8Bytes(canonicalJson(request)).byteLength;
  if (requestBytes < fixedBytes) {
    throw new RangeError(`requestBytes must be at least ${fixedBytes}`);
  }
  request.input = prefix + corpusPadding(requestBytes - fixedBytes, index, corpus);
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

function performanceRequestSizes(options: V2PerformanceHarnessOptions): readonly number[] {
  return options.requestBytesByRequest === undefined
    ? Array.from({ length: options.concurrency }, () => options.requestBytes)
    : [...options.requestBytesByRequest];
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
  if (options.requestBytesByRequest !== undefined) {
    if (
      !Array.isArray(options.requestBytesByRequest) ||
      options.requestBytesByRequest.length !== options.concurrency
    ) {
      throw new RangeError("requestBytesByRequest must match concurrency exactly");
    }
    for (const requestBytes of options.requestBytesByRequest) {
      if (
        !Number.isSafeInteger(requestBytes) ||
        requestBytes < 1 ||
        requestBytes > GP_V2_REQUEST_MAX_BYTES
      ) throw new RangeError("requestBytesByRequest contains an invalid V2 request size");
    }
  }
  if (
    options.responseEventCount !== undefined &&
    (!Number.isSafeInteger(options.responseEventCount) ||
      options.responseEventCount < 2 || options.responseEventCount > 4_096)
  ) {
    throw new RangeError("responseEventCount must be 2 through 4096");
  }
  if (options.sidecarRevision !== undefined && options.sidecarRevision !== "r8" && options.sidecarRevision !== "r10") {
    throw new RangeError("sidecarRevision is invalid");
  }
  if (options.sidecarRevision === "r10" && options.protocolRevision === "r1") {
    throw new RangeError("The R10 response harness requires R2");
  }
  const deltaBytes = options.responseDeltaBytes ?? 0;
  if (!Number.isSafeInteger(deltaBytes) || deltaBytes < 0 || deltaBytes > 8 * KiB ||
      (options.responseEventCount ?? 2) * deltaBytes > 768 * KiB) {
    throw new RangeError("Response fixture exceeds its bounded payload budget");
  }
  if (options.responseIntervalMs !== undefined &&
      (!Number.isFinite(options.responseIntervalMs) || options.responseIntervalMs < 0 || options.responseIntervalMs > 1_000)) {
    throw new RangeError("Response fixture interval is invalid");
  }
  if (
    options.corpus !== undefined &&
    options.corpus !== "compressible" &&
    options.corpus !== "mixed" &&
    options.corpus !== "incompressible"
  ) {
    throw new RangeError("corpus is invalid");
  }
  if (
    options.transportProfile !== undefined &&
    options.transportProfile !== "standard" &&
    options.transportProfile !== "bulk-v1"
  ) {
    throw new RangeError("transportProfile is invalid");
  }
  if (options.protocolRevision === "r1" && options.transportProfile === "bulk-v1") {
    throw new RangeError("bulk-v1 requires R2");
  }
  for (const [label, value] of [
    ["linkBandwidthBytesPerSecond", options.linkBandwidthBytesPerSecond],
    ["serializationBytesPerSecond", options.serializationBytesPerSecond],
  ] as const) {
    if (value !== undefined && (!Number.isFinite(value) || value <= 0)) {
      throw new RangeError(`${label} must be positive`);
    }
  }
  const aggregateLimit = options.maxAggregateRequestBytes ?? 64 * MiB;
  if (!Number.isSafeInteger(aggregateLimit) || aggregateLimit < 1) {
    throw new RangeError("maxAggregateRequestBytes must be a positive safe integer");
  }
  const aggregateBytes = performanceRequestSizes(options)
    .reduce((total, requestBytes) => total + requestBytes, 0);
  if (!Number.isSafeInteger(aggregateBytes) || aggregateBytes > aggregateLimit) {
    throw new RangeError("aggregate request bytes exceed the harness safety limit");
  }
  if (
    options.timeoutMs !== undefined &&
    (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1)
  ) {
    throw new RangeError("timeoutMs must be a positive safe integer");
  }
  if (
    options.rotateCredentialDuringTransfer !== undefined &&
    typeof options.rotateCredentialDuringTransfer !== "boolean"
  ) {
    throw new RangeError("rotateCredentialDuringTransfer must be boolean");
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
    now: () => clock.now(),
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
    if (options.streamWhileAdmitting) runtime.releaseUpstream();
    const corpus = options.corpus ?? "incompressible";
    const requestSizes = performanceRequestSizes(options);
    const bodies = requestSizes.map((requestBytes, index) =>
      createExactSizedV2Request(requestBytes, index, corpus));
    const expectedDigests = await Promise.all(bodies.map(body =>
      sha256Hex(utf8Bytes(canonicalJson(body)))));
    const modeledStarts = bodies.map(() => clock.now());
    const wallStarts = bodies.map(() => performance.now());
    const committed = bodies.map(async (body, index) => {
      const handle = await router.startRelay(body, {
        clientId: `perf-client-${index}`,
        source: "http",
      });
      const modeledCommitMs = clock.now() - modeledStarts[index]!;
      const wallCommitMs = performance.now() - wallStarts[index]!;
      const trace = hub.requestTraceByDigest(expectedDigests[index]!);
      if (!trace) throw new Error(`Missing wire request trace for request ${index}`);
      const events = (async () => {
        let modeledFirstEventMs: number | null = null;
        let wallFirstEventMs: number | null = null;
        const transcript = new TranscriptProof(
          `perf-observed-${index}`,
          expectedDigests[index]!,
        );
        let eventCount = 0;
        for await (const event of handle.events()) {
          if (modeledFirstEventMs === null) {
            modeledFirstEventMs = clock.now() - modeledStarts[index]!;
            wallFirstEventMs = performance.now() - wallStarts[index]!;
          }
          transcript.append(event);
          eventCount++;
        }
        const modeledEventsDoneMs = clock.now() - modeledStarts[index]!;
        const wallEventsDoneMs = performance.now() - wallStarts[index]!;
        const terminal = await transcript.createTerminal();
        return {
          modeledEventsDoneMs,
          wallEventsDoneMs,
          modeledFirstEventMs:
            modeledFirstEventMs ?? clock.now() - modeledStarts[index]!,
          wallFirstEventMs:
            wallFirstEventMs ?? performance.now() - wallStarts[index]!,
          eventCount,
          transcriptDigest: terminal.transcriptSha256,
          finalDigest: terminal.finalSha256,
        };
      })();
      const finalResult = handle.final.then(final => ({
        final,
        modeledFinalMs: clock.now() - modeledStarts[index]!,
        wallFinalMs: performance.now() - wallStarts[index]!,
      }));
      // If another admission fails first, teardown closes this stream before
      // Promise.all(handles) can attach its observer. Preserve the rejection for
      // that observer without leaking an unhandled failure into the next test.
      void events.catch(() => undefined);
      void finalResult.catch(() => undefined);
      return {
        index,
        handle,
        modeledCommitMs,
        wallCommitMs,
        hostPreparationWallMs:
          Math.max(0, trace.firstPublicationWallMs! - wallStarts[index]!),
        encodedRequestBytes: trace.encodedBytes,
        requestEncoding: trace.encoding,
        ackRoundsAtCommit: trace.flights.size,
        events,
        finalResult,
      };
    });
    const credentialRotation = options.rotateCredentialDuringTransfer
      ? (async () => {
          await waitFor(
            () => hub.snapshot().uniqueRequestDataBytes > 0,
            timeoutMs,
            "V2 harness request data before credential rotation",
          );
          await runtime.rotateCredential();
        })()
      : Promise.resolve();
    let handles: Awaited<(typeof committed)[number]>[];
    try {
      handles = await withTimeout(
        Promise.all(committed),
        timeoutMs,
        "V2 harness commitment",
      );
    } catch (cause) {
      throw new Error(`V2 harness commitment failed: ${JSON.stringify({
        protocolRevision: options.protocolRevision ?? "r2",
        transport: runtime.transportSnapshot(),
        wire: hub.snapshot(),
        upstream: {
          calls: runtime.upstream.calls,
          active: runtime.upstream.active,
        },
        router: router.snapshot(),
      })}`, { cause });
    }
    try {
      await withTimeout(
        credentialRotation,
        timeoutMs,
        "V2 harness credential rotation",
      );
    } catch (cause) {
      throw new Error(`V2 harness credential rotation failed: ${JSON.stringify({
        transport: runtime.transportSnapshot(),
        wire: hub.snapshot(),
        router: router.snapshot(),
        peerErrors: runtime.peerErrorMessages(),
      })}`, { cause });
    }
    if (!options.streamWhileAdmitting) {
      await waitFor(
        () => runtime.upstream.active === options.concurrency,
        timeoutMs,
        "V2 harness upstream concurrency",
      );
    }
    runtime.releaseUpstream();
    const requests = await withTimeout(Promise.all(handles.map(async value => {
      const [settled, observed] = await Promise.all([
        value.finalResult,
        value.events,
      ]);
      const final = settled.final;
      const upstream = runtime.upstream.proofs.get(value.index);
      if (!upstream) throw new Error(`Missing upstream proof for request ${value.index}`);
      const finalDigest = await sha256Hex(canonicalJson(final));
      const requestDigest = expectedDigests[value.index]!;
      return {
        index: value.index,
        requestBytes: requestSizes[value.index]!,
        encodedRequestBytes: value.encodedRequestBytes,
        requestEncoding: value.requestEncoding,
        requestDigest,
        upstreamDigest: upstream.requestDigest,
        modeledCommitMs: value.modeledCommitMs,
        modeledFirstEventMs: observed.modeledFirstEventMs,
        // Integrity verification below can await hashing while unrelated ACK
        // traffic advances the model clock. Measure the actual public terminal
        // and stream completion, not that test-only verification/cleanup delay.
        modeledTotalMs: Math.max(settled.modeledFinalMs, observed.modeledEventsDoneMs),
        wallCommitMs: value.wallCommitMs,
        wallFirstEventMs: observed.wallFirstEventMs,
        wallTotalMs: Math.max(settled.wallFinalMs, observed.wallEventsDoneMs),
        hostPreparationWallMs: value.hostPreparationWallMs,
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
      implementation: "mutable-source-components",
      protocolRevision: options.protocolRevision ?? "r2",
      transportProfile: options.transportProfile ?? "standard",
      corpus,
      rttMs: options.rttMs,
      requestBytes: options.requestBytes,
      concurrency: options.concurrency,
      virtualElapsedMs: clock.now(),
      link: {
        bandwidthBytesPerSecond: options.linkBandwidthBytesPerSecond ??
          DEFAULT_LINK_BANDWIDTH_BYTES_PER_SECOND,
        serializationBytesPerSecond: options.serializationBytesPerSecond ??
          DEFAULT_SERIALIZATION_BYTES_PER_SECOND,
      },
      operations: {
        credentialRotations: runtime.credentialRotations,
      },
      requests,
      timings: {
        modeledLink: {
          commitMs: summary(requests.map(value => value.modeledCommitMs)),
          firstEventMs: summary(requests.map(value => value.modeledFirstEventMs)),
          totalMs: summary(requests.map(value => value.modeledTotalMs)),
        },
        measuredWall: {
          commitMs: summary(requests.map(value => value.wallCommitMs)),
          firstEventMs: summary(requests.map(value => value.wallFirstEventMs)),
          totalMs: summary(requests.map(value => value.wallTotalMs)),
          hostPreparationMs: summary(
            requests.map(value => value.hostPreparationWallMs),
          ),
        },
      },
      preparation: {
        ...runtime.preparationWallMetrics(),
        ...runtime.sessionRunMetrics(),
      },
      upstream: {
        calls: runtime.upstream.calls,
        maxActive: runtime.upstream.maxActive,
      },
      responseEventsEmitted: runtime.upstream.emittedEvents,
      admissionReopenedDuringResponses: runtime.admissionReopenedDuringResponses,
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

export async function runV2FleetPerformanceHarness(
  options: V2FleetPerformanceHarnessOptions,
): Promise<V2FleetPerformanceHarnessReport> {
  if (!Number.isSafeInteger(options.runtimeCount) || options.runtimeCount < 1 ||
      options.runtimeCount > 32) {
    throw new RangeError("runtimeCount must be an integer from 1 through 32");
  }
  const unavailable = options.unavailableRuntimeCount ?? 0;
  if (!Number.isSafeInteger(unavailable) || unavailable < 0 || unavailable >= options.runtimeCount) {
    throw new RangeError("unavailableRuntimeCount must leave at least one usable runtime");
  }
  if (options.requestBytesByRequest.length < 1) {
    throw new RangeError("fleet harness requires at least one request");
  }
  const usableRuntimeCount = options.runtimeCount - unavailable;
  const assignments = options.requestBytesByRequest.map(
    (_bytes, index) => index % usableRuntimeCount,
  );
  const groups = Array.from({ length: usableRuntimeCount }, () => (
    [] as Array<{ index: number; bytes: number }>
  ));
  options.requestBytesByRequest.forEach((bytes, index) => {
    groups[assignments[index]!]!.push({ index, bytes });
  });
  const reports = await Promise.all(groups.map(group => runV2PerformanceHarness({
    ...options,
    requestBytes: Math.max(...group.map(value => value.bytes)),
    requestBytesByRequest: group.map(value => value.bytes),
    concurrency: group.length,
    maxAggregateRequestBytes: group.reduce((total, value) => total + value.bytes, 0),
  })));
  const requests = reports.flatMap((report, runtimeIndex) => report.requests.map(
    (request, localIndex) => ({
      ...request,
      index: groups[runtimeIndex]![localIndex]!.index,
      runtimeIndex,
    }),
  )).sort((left, right) => left.index - right.index);
  return Object.freeze({
    runtimeCount: options.runtimeCount,
    usableRuntimeCount,
    assignments: Object.freeze(assignments),
    reports: Object.freeze(reports),
    requests: Object.freeze(requests),
    modeledCommitMs: summary(requests.map(value => value.modeledCommitMs)),
    modeledTotalMs: summary(requests.map(value => value.modeledTotalMs)),
    exactRequests: requests.every(value => value.exactRequest),
    exactTerminals: requests.every(value => value.exactTerminal),
    upstreamMaxActive: reports.reduce((total, report) => total + report.upstream.maxActive, 0),
    retransmissions: reports.reduce(
      (total, report) => total + report.transport.retransmissions,
      0,
    ),
  });
}
