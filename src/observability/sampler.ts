import { monitorEventLoopDelay } from "node:perf_hooks";
import type { Clock, MachineProbe } from "../core/ports";
import { SYSTEM_CLOCK } from "../core/ports";
import type { CpuTransformPoolSnapshot } from "../router/cpu-transform-pool";
import type { MultiSandboxRouterSnapshot } from "../router/router";
import type { RouterTrafficSnapshot } from "../router/telemetry";
import { observabilityEvent, type JsonValue } from "./events";
import type { LoggerLike } from "./logger";
import type { WebSocketTelemetrySnapshot } from "../ws/telemetry";
import type { V2EnvelopeDiagnostics, V2TransportDiagnostics } from "../v2/diagnostics/types";
import type { FleetRecoverySnapshot } from "../prism/recovery-coordinator";
import type { AtomicBytePermitPoolSnapshot } from "../v2/memory/atomic-byte-permits";
import {
  safeV2ReleaseIdentity,
  summarizeV2Releases,
} from "../v2/release-observability";
import type { FleetSnapshot } from "../fleet/types";

const MAX_UNAVAILABLE_PROJECT_SAMPLES = 256;

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
    round_trip_variation_ms: value.roundTripVariationMs ?? null,
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
    acknowledged_goodput_bytes_per_sec:
      value.acknowledgedGoodputBytesPerSecond ?? null,
    unique_acknowledged_bytes_total: value.uniqueAcknowledgedBytesTotal ?? 0,
    retransmitted_bytes_total: value.retransmittedBytesTotal ?? 0,
    stall_events_total: value.stallEventsTotal ?? 0,
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
function sidecarJson(value: V2EnvelopeDiagnostics | null): JsonValue {
  if (!value) return null;
  const resource = value.resource;
  return {
    version: value.version,
    malformed_count: value.malformedCount,
    resource: resource ? {
      process_rss_bytes: resource.processRssBytes,
      heap_used_bytes: resource.heapUsedBytes,
      heap_total_bytes: resource.heapTotalBytes,
      external_bytes: resource.externalBytes,
      array_buffer_bytes: resource.arrayBufferBytes,
      system_total_bytes: resource.systemTotalBytes,
      system_free_bytes: resource.systemFreeBytes,
      process_cpu_percent: resource.processCpuPercent,
      normalized_load_1: resource.normalizedLoad1,
      event_loop_samples: resource.eventLoopSamples,
      event_loop_p50_ms: resource.eventLoopP50Ms,
      event_loop_p95_ms: resource.eventLoopP95Ms,
      event_loop_max_ms: resource.eventLoopMaxMs,
      gc_samples: resource.gcSamples,
      gc_duration_p95_ms: resource.gcDurationP95Ms,
      gc_duration_max_ms: resource.gcDurationMaxMs,
    } : null,
    transport: transportJson(value.transport),
  };
}

function distributionJson(value: RouterTrafficSnapshot["totalMs"]): JsonValue {
  return { count: value.count, p50_ms: value.p50, p95_ms: value.p95, max_ms: value.max };
}

function trafficJson(value: RouterTrafficSnapshot, intervalMs: number): JsonValue {
  const terminals = value.succeeded + value.failed;
  return {
    accepted: value.accepted,
    committed: value.committed,
    succeeded: value.succeeded,
    failed: value.failed,
    rate_limited: value.rateLimited,
    retries: value.retries,
    response_bytes: value.responseBytes,
    requests_per_second: intervalMs > 0 ? terminals / (intervalMs / 1_000) : 0,
    success_rate: terminals > 0 ? value.succeeded / terminals : 0,
    queue_wait: distributionJson(value.queueWaitMs),
    commit_ack: distributionJson(value.commitAckMs),
    total: distributionJson(value.totalMs),
    first_byte: distributionJson(value.firstByteMs),
  };
}

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

function rateLimitObservationJson(
  model: string,
  value: NonNullable<MultiSandboxRouterSnapshot["runtimes"][number]["upstreamRateLimit"]>,
): JsonValue {
  return {
    model: value.problem?.model ?? value.requestedModel ?? model,
    organization: value.problem?.organization ?? null,
    kind: value.problem?.limitKind?.includes("token")
      ? "tpm"
      : value.problem?.limitKind?.includes("request")
        ? "rpm"
        : value.problem?.limitKind ?? null,
    limit: value.problem?.tokenLimit ?? value.hint.limitTokens ?? null,
    used: value.problem?.usedTokens ?? null,
    requested: value.problem?.requestedTokens ?? null,
    remaining: value.hint.remainingTokens ?? null,
    retry_after_ms: value.problem?.retryAfterMs ?? value.hint.retryAfterMs ?? null,
    observed_ago_ms: value.observedAgoMs,
  };
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
  routerTrafficSnapshot?: () => RouterTrafficSnapshot;
  hostEventMemorySnapshot?: () => AtomicBytePermitPoolSnapshot;
  fleetSnapshot?: () => FleetSnapshot;
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
  readonly #routerTrafficSnapshot?: () => RouterTrafficSnapshot;
  readonly #hostEventMemorySnapshot?: () => AtomicBytePermitPoolSnapshot;
  readonly #fleetSnapshot?: () => FleetSnapshot;
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
    this.#routerTrafficSnapshot = options.routerTrafficSnapshot;
    this.#hostEventMemorySnapshot = options.hostEventMemorySnapshot;
    this.#fleetSnapshot = options.fleetSnapshot;
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
        fleet_transfer_reserved: router.fleetTransferReserved,
        fleet_transfer_budget: router.fleetTransferBudget,
        event_memory: this.#hostEventMemorySnapshot
          ? (() => {
              const value = this.#hostEventMemorySnapshot!();
              return {
                ordinary_capacity_bytes: value.ordinaryCapacityBytes,
                ordinary_used_bytes: value.ordinaryUsedBytes,
                ordinary_available_bytes: value.ordinaryAvailableBytes,
                jumbo_capacity_bytes: value.jumboCapacityBytes,
                jumbo_used_bytes: value.jumboUsedBytes,
                jumbo_available_bytes: value.jumboAvailableBytes,
                jumbo_active: value.jumboActive,
                active_permits: value.activePermits,
                waiters: value.waiters,
                promotion_waiters: value.promotionWaiters,
                closed: value.closed,
              };
            })()
          : null,
      };
      const runtimes: JsonValue = router.runtimes.map(runtime => {
        const release = safeV2ReleaseIdentity(runtime);
        return {
        project_id: runtime.projectId,
        release_revision: release?.releaseRevision ?? null,
        protocol_revision: release?.protocolRevision ?? null,
        build_hash: release?.buildHash ?? null,
        schema_hash: release?.schemaHash ?? null,
        incarnation: runtime.incarnation ?? 0,
        state: runtime.state,
        capacity: runtime.capacity,
        active: runtime.active,
        available: runtime.available,
        availability_reason: runtime.availabilityReason ??
          (runtime.available > 0 ? "available" : "transport_unavailable"),
        quarantined: runtime.quarantined,
        ambiguous_quarantined: runtime.ambiguousQuarantined ?? 0,
        ambiguity_limit: runtime.ambiguityLimit ?? null,
        queue_depth: runtime.queueDepth,
        awareness_rtt_ms: runtime.ackEwmaMs,
        completion_ewma_ms: runtime.completionEwmaMs,
        recent_failure_rate: runtime.recentFailureRate,
        heartbeat_age_ms: runtime.heartbeatAgeMs,
        sidecar_peer_age_ms: runtime.sidecarPeerAgeMs ?? null,
        transport_connected: runtime.transportConnected ?? null,
        credential_expires_in_ms: runtime.credentialExpiresInMs,
        memory_pressure: runtime.memoryPressure,
        error_kind: runtime.failure?.kind ?? null,
        recovery: runtime.recovery ? {
          attempt: runtime.recovery.attempt,
          consecutive_failures: runtime.recovery.consecutiveFailures,
          retry_in_ms: runtime.recovery.retryInMs,
        } : null,
        rate_limit_observations: Object.entries(runtime.upstreamRateLimits ?? {})
          .slice(0, 16)
          .map(([model, value]) => rateLimitObservationJson(model, value)),
        diagnostics: runtime.diagnostics ? {
          host: transportJson(runtime.diagnostics.host),
          sidecar: sidecarJson(runtime.diagnostics.sidecar),
        } : null,
      };
      });
      const circuit = this.#recoveryCircuitSnapshot?.();
      const traffic = this.#routerTrafficSnapshot?.();
      const fleet = this.#fleetSnapshot?.();
      const unavailableProjects = (fleet?.unavailableProjects ?? [])
        .slice(0, MAX_UNAVAILABLE_PROJECT_SAMPLES);
      this.#logger.emit(observabilityEvent("fleet.sample", {
        capacity: router.capacity,
        active: router.active,
        available: router.available,
        queued: router.queued,
        queued_bytes: router.queuedBytes ?? 0,
        desired: fleet?.desired ?? router.runtimes.length,
        ready: fleet?.ready ?? router.runtimes.filter(runtime => runtime.state === "ready").length,
        routable: router.runtimes.filter(runtime => runtime.available > 0).length,
        unavailable: fleet?.unavailable ?? 0,
        unavailable_projects: unavailableProjects,
        unavailable_projects_truncated:
          (fleet?.unavailableProjects.length ?? 0) > unavailableProjects.length,
        rss_bytes: memory.rss,
        heap_bytes: memory.heapUsed,
        external_bytes: memory.external,
        array_buffer_bytes: memory.arrayBuffers,
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
        traffic: traffic ? trafficJson(traffic, this.#intervalMs) : null,
        websocket: this.#webSocketSnapshot?.() as unknown as JsonValue ?? null,
        release: summarizeV2Releases(router.runtimes) as unknown as JsonValue,
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
