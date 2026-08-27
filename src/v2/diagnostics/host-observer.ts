import type { RunDiagnostics } from "../../relay/run-handle";
import type { RequestEncoding } from "../protocol/shared";
import type {
  V2FirstEventDiagnostics,
  V2RequestAckDiagnostics,
  V2TransportDiagnostics,
} from "./types";

export interface VersionedHostTransportDiagnostics {
  revision: number;
  value: V2TransportDiagnostics;
}

export type HostTransportDiagnosticsSource =
  () => VersionedHostTransportDiagnostics | null;

export class HostRequestDiagnosticsObserver {
  readonly #now: () => number;
  readonly #start: number;
  #prepared: number | null = null;
  #published: number | null = null;
  #committed: number | null = null;
  #peakSource: HostTransportDiagnosticsSource | undefined;
  #peakRevision: number;
  #peakTransport: V2TransportDiagnostics | null = null;
  readonly #value: RunDiagnostics;

  constructor(
    now: () => number = () => performance.now(),
    peakSource?: HostTransportDiagnosticsSource,
  ) {
    this.#now = now;
    this.#start = now();
    this.#peakSource = peakSource;
    this.#peakRevision = peakSource?.()?.revision ?? -1;
    this.#value = {
      requestDecodedBytes: null,
      requestEncodedBytes: null,
      requestPartCount: null,
      requestEncoding: null,
      hostPrepareMs: null,
      hostSchedulerWaitMs: null,
      hostPublishToAckMs: null,
      sidecarFirstFrameToCommitMs: null,
      sidecarCommitToAssemblyMs: null,
      sidecarClaimSyncMs: null,
      sidecarAdmissionWaitMs: null,
      sidecarPreparedToFetchStartMs: null,
      sidecarFetchStartSyncMs: null,
      sidecarCommitProcessingMs: null,
      transportCommitResidualMs: null,
      upstreamFirstEventMs: null,
      sidecarFirstEventSchedulerMs: null,
      hostAckToFirstFrameMs: null,
      hostAssemblyToDeliveryMs: null,
      peakTransport: null,
    };
    Object.defineProperty(this.#value, "peakTransport", {
      enumerable: true,
      configurable: false,
      get: () => this.#readPeakTransport(),
    });
  }

  prepared(request?: {
    decodedBytes: number;
    encodedBytes: number;
    partCount: number;
    encoding: RequestEncoding;
  }): void {
    this.#prepared = this.#now();
    this.#value.hostPrepareMs = Math.max(0, this.#prepared - this.#start);
    if (!request) return;
    this.#value.requestDecodedBytes = request.decodedBytes;
    this.#value.requestEncodedBytes = request.encodedBytes;
    this.#value.requestPartCount = request.partCount;
    this.#value.requestEncoding = request.encoding;
  }

  published(): void {
    if (this.#published !== null) return;
    this.#published = this.#now();
    this.#value.hostSchedulerWaitMs = this.#prepared === null
      ? null
      : Math.max(0, this.#published - this.#prepared);
  }

  committed(sidecar?: V2RequestAckDiagnostics): void {
    this.#committed = this.#now();
    const host = this.#published === null
      ? null
      : Math.max(0, this.#committed - this.#published);
    this.#value.hostPublishToAckMs = host;
    if (!sidecar) return;
    this.#value.sidecarFirstFrameToCommitMs = sidecar.firstFrameToCommitMs;
    this.#value.sidecarCommitToAssemblyMs = sidecar.commitToAssemblyMs;
    this.#value.sidecarClaimSyncMs = sidecar.claimSyncMs;
    this.#value.sidecarAdmissionWaitMs = sidecar.admissionWaitMs;
    this.#value.sidecarPreparedToFetchStartMs = sidecar.preparedToFetchStartMs ?? null;
    this.#value.sidecarFetchStartSyncMs = sidecar.fetchStartSyncMs ?? null;
    this.#value.sidecarCommitProcessingMs = sidecar.commitProcessingMs;
    this.#value.transportCommitResidualMs = host === null
      ? null
      : Math.max(0, host - sidecar.commitProcessingMs);
  }

  firstFrame(sidecar?: V2FirstEventDiagnostics): void {
    this.#value.hostAckToFirstFrameMs = this.#committed === null
      ? null
      : Math.max(0, this.#now() - this.#committed);
    if (!sidecar) return;
    this.#value.upstreamFirstEventMs = sidecar.upstreamFirstEventMs;
    this.#value.sidecarFirstEventSchedulerMs = sidecar.schedulerWaitMs;
  }

  assemblyDelivered(ms: number): void {
    this.#value.hostAssemblyToDeliveryMs = Math.max(0, ms);
  }

  peak(value: V2TransportDiagnostics): void {
    this.#peakTransport = { ...value };
    const current = this.#peakSource?.();
    if (current) this.#peakRevision = current.revision;
  }

  followPeak(source: HostTransportDiagnosticsSource): void {
    this.#peakSource = source;
    this.#peakRevision = source()?.revision ?? -1;
  }

  freezePeak(): void {
    this.#readPeakTransport();
    this.#peakSource = undefined;
  }

  value(): RunDiagnostics {
    return this.#value;
  }

  #readPeakTransport(): V2TransportDiagnostics | null {
    const current = this.#peakSource?.();
    if (current && current.revision !== this.#peakRevision) {
      this.#peakRevision = current.revision;
      this.#peakTransport = { ...current.value };
    }
    return this.#peakTransport;
  }
}
