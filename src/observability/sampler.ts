import { monitorEventLoopDelay } from "node:perf_hooks";
import type { Clock, MachineProbe } from "../core/ports";
import { SYSTEM_CLOCK } from "../core/ports";
import type { CpuTransformPoolSnapshot } from "../router/cpu-transform-pool";
import type { MultiSandboxRouterSnapshot } from "../router/router";
import { observabilityEvent, type JsonValue } from "./events";
import type { LoggerLike } from "./logger";
import type { WebSocketTelemetrySnapshot } from "../ws/telemetry";
import type { V2EnvelopeDiagnostics, V2TransportDiagnostics } from "../v2/diagnostics/types";
import type { FleetRecoverySnapshot } from "../prism/recovery-coordinator";

function ratio(numerator: number, denominator: number): number {
  return denominator > 0 ? Math.round((numerator / denominator) * 1_000) / 1_000 : 0;
}

function transportJson(value: V2TransportDiagnostics): JsonValue {
  return {
    window_frames: value.windowFrames,
    window_bytes: value.windowBytes,
    scheduler_frames: value.schedulerFrames,
    scheduler_bytes: value.schedulerBytes,
    scheduler_streams: value.schedulerStreams,
    oldest_queued_frame_ms: value.oldestQueuedFrameMs,
    ack_progress_age_ms: value.ackProgressAgeMs,
    retransmission_attempt: value.retransmissionAttempt,
    peak_retransmission_attempt: value.peakRetransmissionAttempt,
    retransmissions_total: value.retransmissionsTotal,
    publications_total: value.publicationsTotal,
    published_bytes_total: value.publishedBytesTotal,
    admitted_uncommitted: value.admittedUncommitted,
    round_trip_ewma_ms: value.roundTripEwmaMs,
    paced_publications_total: value.pacedPublicationsTotal,
    urgent_publications_total: value.urgentPublicationsTotal,
    logical_events_total: value.logicalEventsTotal,
    response_batch_frames_total: value.responseBatchFramesTotal,
    batched_events_total: value.batchedEventsTotal,
    data_publications_total: value.dataPublicationsTotal,
    control_publications_total: value.controlPublicationsTotal,
    data_published_bytes_total: value.dataPublishedBytesTotal,
    control_published_bytes_total: value.controlPublishedBytesTotal,
    flight_completions_total: value.flightCompletionsTotal,
    adaptive_capacity: value.adaptiveCapacity,
    capacity_increases_total: value.capacityIncreasesTotal,
    capacity_reductions_total: value.capacityReductionsTotal,
    unadmitted_timeouts_total: value.unadmittedTimeoutsTotal,
    logical_events_per_batch: ratio(
      value.batchedEventsTotal,
      value.responseBatchFramesTotal,
    ),
    logical_events_per_publication: ratio(
      value.logicalEventsTotal,
      value.publicationsTotal,
    ),
    logical_events_per_data_publication: ratio(
      value.logicalEventsTotal,
      value.dataPublicationsTotal,
    ),
  };
}
function sidecarJson(value: V2EnvelopeDiagnostics | null): JsonValue { if(!value)return null;const r=value.resource;return { version:value.version,malformed_count:value.malformedCount,resource:r?{process_rss_bytes:r.processRssBytes,heap_used_bytes:r.heapUsedBytes,heap_total_bytes:r.heapTotalBytes,external_bytes:r.externalBytes,system_total_bytes:r.systemTotalBytes,system_free_bytes:r.systemFreeBytes,process_cpu_percent:r.processCpuPercent,normalized_load_1:r.normalizedLoad1,event_loop_p50_ms:r.eventLoopP50Ms,event_loop_p95_ms:r.eventLoopP95Ms,event_loop_max_ms:r.eventLoopMaxMs}:null,transport:transportJson(value.transport) }; }

interface EventLoopDelayLike {
  min: number | bigint;
  max: number | bigint;
  percentile(value: number): number | bigint;
  enable(): void;
  disable(): void;
  reset(): void;
}

function milliseconds(value: number | bigint): number {
  const numeric = typeof value === "bigint" ? Number(value) : value;
  return Number.isFinite(numeric)
    ? Math.round((numeric / 1_000_000) * 1000) / 1000
    : 0;
}

function rounded(value: number): number {
  return Number.isFinite(value) ? Math.round(value * 1000) / 1000 : 0;
}

export interface FleetSamplerOptions {
  logger: LoggerLike;
  routerSnapshot: () => MultiSandboxRouterSnapshot;
  machineProbe: MachineProbe;
  intervalMs: number;
  cpuPoolSnapshot?: () => CpuTransformPoolSnapshot;
  clock?: Clock;
  eventLoop?: EventLoopDelayLike;
  memoryUsage?: () => NodeJS.MemoryUsage;
  cpuUsage?: (previousValue?: NodeJS.CpuUsage) => NodeJS.CpuUsage;
  webSocketSnapshot?: () => WebSocketTelemetrySnapshot;
  recoveryCircuitSnapshot?: () => FleetRecoverySnapshot;
}

export class FleetSampler {
  readonly #logger: LoggerLike;
  readonly #routerSnapshot: () => MultiSandboxRouterSnapshot;
  readonly #machineProbe: MachineProbe;
  readonly #intervalMs: number;
  readonly #cpuPoolSnapshot?: () => CpuTransformPoolSnapshot;
  readonly #clock: Clock;
  readonly #eventLoop: EventLoopDelayLike;
  readonly #memoryUsage: () => NodeJS.MemoryUsage;
  readonly #cpuUsage: (previousValue?: NodeJS.CpuUsage) => NodeJS.CpuUsage;
  readonly #webSocketSnapshot?: () => WebSocketTelemetrySnapshot;
  readonly #recoveryCircuitSnapshot?: () => FleetRecoverySnapshot;
  #lastCpu: NodeJS.CpuUsage;
  #lastSampleAtMs: number;
  #timer: ReturnType<typeof setInterval> | null = null;
  #sampling = false;
  #closed = false;

  constructor(options: FleetSamplerOptions) {
    if (!Number.isFinite(options.intervalMs) || options.intervalMs < 100) {
      throw new RangeError("Fleet sample interval must be at least 100ms");
    }
    this.#logger = options.logger;
    this.#routerSnapshot = options.routerSnapshot;
    this.#machineProbe = options.machineProbe;
    this.#intervalMs = options.intervalMs;
    this.#cpuPoolSnapshot = options.cpuPoolSnapshot;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#eventLoop = options.eventLoop ?? monitorEventLoopDelay({ resolution: 20 });
    this.#memoryUsage = options.memoryUsage ?? process.memoryUsage;
    this.#cpuUsage = options.cpuUsage ?? process.cpuUsage;
    this.#webSocketSnapshot = options.webSocketSnapshot;
    this.#recoveryCircuitSnapshot = options.recoveryCircuitSnapshot;
    this.#lastCpu = this.#cpuUsage();
    this.#lastSampleAtMs = this.#clock.now();
  }

  start(): void {
    if (this.#closed || this.#timer) return;
    this.#eventLoop.enable();
    this.#timer = setInterval(() => this.sample(), this.#intervalMs);
    this.#timer.unref?.();
  }

  sample(): void {
    if (this.#closed || this.#sampling) return;
    this.#sampling = true;
    try {
      const now = this.#clock.now();
      const elapsedMs = Math.max(0, now - this.#lastSampleAtMs);
      const cpu = this.#cpuUsage(this.#lastCpu);
      this.#lastCpu = this.#cpuUsage();
      this.#lastSampleAtMs = now;
      const cpuPercent = elapsedMs > 0
        ? ((cpu.user + cpu.system) / (elapsedMs * 1000)) * 100
        : 0;
      const router = this.#routerSnapshot();
      const machine = this.#machineProbe.sample();
      const memory = this.#memoryUsage();
      const transforms = this.#cpuPoolSnapshot?.();
      const resources: JsonValue = {
        reserved_bytes: router.resources.reservedBytes,
        reservation_capacity_bytes: router.resources.reservationCapacityBytes,
        memory_paused: router.resources.paused,
        governor_queued: router.resources.queued,
        cpu_active: router.resources.cpuActive,
        cpu_queued: router.resources.cpuQueued,
        transform_workers: transforms?.workers ?? router.resources.cpuTransformWorkers,
        transform_active: transforms?.active ?? 0,
        transform_queued: transforms?.queued ?? 0,
      };
      const runtimes: JsonValue = router.runtimes.map(runtime => ({
        project_id: runtime.projectId,
        incarnation: runtime.incarnation ?? 0,
        state: runtime.state,
        capacity: runtime.capacity,
        active: runtime.active,
        available: runtime.available,
        quarantined: runtime.quarantined,
        queue_depth: runtime.queueDepth,
        awareness_rtt_ms: runtime.ackEwmaMs,
        completion_ewma_ms: runtime.completionEwmaMs,
        recent_failure_rate: runtime.recentFailureRate,
        heartbeat_age_ms: runtime.heartbeatAgeMs,
        sidecar_peer_age_ms: runtime.sidecarPeerAgeMs ?? null,
        credential_expires_in_ms: runtime.credentialExpiresInMs,
        memory_pressure: runtime.memoryPressure,
        error_kind: runtime.failure?.kind ?? null,
        recovery: runtime.recovery ? {
          attempt: runtime.recovery.attempt,
          consecutive_failures: runtime.recovery.consecutiveFailures,
          retry_in_ms: runtime.recovery.retryInMs,
        } : null,
        diagnostics: runtime.diagnostics ? {
          host: transportJson(runtime.diagnostics.host),
          sidecar: sidecarJson(runtime.diagnostics.sidecar),
        } : null,
      }));
      const circuit = this.#recoveryCircuitSnapshot?.();
      this.#logger.emit(observabilityEvent("fleet.sample", {
        capacity: router.capacity,
        active: router.active,
        available: router.available,
        queued: router.queued,
        rss_bytes: memory.rss,
        heap_bytes: memory.heapUsed,
        external_bytes: memory.external + memory.arrayBuffers,
        system_total_bytes: machine.totalBytes,
        system_free_bytes: machine.freeBytes,
        event_loop_p50_ms: milliseconds(this.#eventLoop.percentile(50)),
        event_loop_p95_ms: milliseconds(this.#eventLoop.percentile(95)),
        event_loop_max_ms: milliseconds(this.#eventLoop.max),
        cpu_percent: rounded(cpuPercent),
        resources,
        logger: this.#logger.snapshot() as unknown as JsonValue,
        runtimes,
        provisioning_circuit: circuit ? {
          state: circuit.state,
          scope: circuit.scope,
          consecutive_failures: circuit.consecutiveFailures,
          retry_in_ms: circuit.nextProbeAtMs === null
            ? null
            : Math.max(0, circuit.nextProbeAtMs - this.#clock.now()),
          queued: circuit.queued,
          last_failure_kind: circuit.lastFailureKind,
        } : null,
        websocket: this.#webSocketSnapshot?.() as unknown as JsonValue ?? null,
      }), "sample");
      this.#eventLoop.reset();
    } finally {
      this.#sampling = false;
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    this.#eventLoop.disable();
  }
}
