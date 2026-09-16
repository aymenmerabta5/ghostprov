import { describe, expect, it } from "bun:test";
import type { Clock, MachineProbe } from "../src/core/ports";
import { ObservabilityLogger, type LoggerLike } from "../src/observability/logger";
import { resolveObservabilityConfig } from "../src/observability/config";
import type { ObservabilityEvent } from "../src/observability/events";
import { FleetSampler } from "../src/observability/sampler";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { projectId } from "../src/sandbox/types";
import type { V2HostTransportDiagnostics } from "../src/v2/diagnostics/host-types";
import type { V2TransportDiagnostics } from "../src/v2/diagnostics/types";
import { V2_R3_RELEASE } from "../src/v2/sidecar/assets";
import { WebSocketTelemetry } from "../src/ws/telemetry";
import { latestFleetSampleFromCompleteTail } from "../scripts/inspect_existing_retirement";

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  delegate?: LoggerLike;
  emit(event: ObservabilityEvent): void { this.events.push(event); this.delegate?.emit(event, "sample"); }
  snapshot() { return { queuedEvents: 2, queuedBytes: 200, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }; }
  async close(): Promise<void> {}
}

class FakeClock implements Clock {
  value = 1_000;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadlineMs: number): Promise<void> { this.value = deadlineMs; }
}

describe("FleetSampler", () => {
  it("emits sanitized aggregate, host, logger, and per-runtime metrics within the full-fleet log budget", async () => {
    const logger = new MemoryLogger();
    const clock = new FakeClock();
    const machineProbe: MachineProbe = {
      sample: () => ({
        totalBytes: 32 * 1024 ** 3,
        freeBytes: 12 * 1024 ** 3,
        rssBytes: 512 * 1024 ** 2,
        heapBytes: 128 * 1024 ** 2,
        externalBytes: 64 * 1024 ** 2,
        logicalProcessors: 12,
      }),
    };
    const eventLoop = {
      min: 1_000_000,
      max: 8_000_000,
      percentile: (value: number) => value === 50 ? 2_000_000 : 5_000_000,
      enable() {},
      disable() {},
      reset() {},
    };
    let cpu = { user: 0, system: 0 };
    const webSocketTelemetry = new WebSocketTelemetry({ capacity: 8 });
    const unavailableProject = projectId("00000000-0000-4000-8000-000000000002");
    let fleetRecovered = false;
    let runtimeCopies = 1;
    const transport: V2TransportDiagnostics = {
      windowFrames: 1,
      windowBytes: 100,
      schedulerFrames: 2,
      schedulerBytes: 200,
      schedulerStreams: 1,
      oldestQueuedFrameMs: 4,
      ackProgressAgeMs: 5,
      retransmissionAttempt: 0,
      peakRetransmissionAttempt: 1,
      retransmissionsTotal: 2,
      publicationsTotal: 10,
      publishedBytesTotal: 400,
      admittedUncommitted: 1,
      roundTripEwmaMs: 6,
      pacedPublicationsTotal: 7,
      urgentPublicationsTotal: 3,
      logicalEventsTotal: 40,
      responseBatchFramesTotal: 8,
      batchedEventsTotal: 32,
      dataPublicationsTotal: 8,
      controlPublicationsTotal: 2,
      dataPublishedBytesTotal: 300,
      controlPublishedBytesTotal: 100,
      flightCompletionsTotal: 7,
      adaptiveCapacity: 8,
      capacityIncreasesTotal: 1,
      capacityReductionsTotal: 2,
      unadmittedTimeoutsTotal: 3,
    };
    const hostEvidence = {
      retainedPrecommitCancellations: 2,
      lastPublishedHostCreditCeiling: 128,
      oldestOutstandingFrameKind: "request_cancel" as const,
      outstandingFlightAgeMs: 25.5,
      dataPeerAgeMs: 2_000.25,
      controlPeerAgeMs: 30_000,
      lastDataRevision: 987,
      lastControlRevision: 12,
      inboundAcceptedEpoch: 2,
      inboundAcceptedSequence: 73,
      inboundProcessedEpoch: 2,
      inboundProcessedSequence: 73,
      inboundAcceptedProgressAgeMs: 35_000.5,
      inboundProcessedProgressAgeMs: 35_000.25,
      inboundProcessingFrames: 0,
      sidecarDiagnosticsAgeMs: 12.5,
      sidecarPendingAgeMs: 34_000,
      sidecarPendingDataPublications: 7,
    };
    const privateEvidence = {
      sessionId: "PRIVATE_SESSION_CANARY",
      requestId: "PRIVATE_REQUEST_CANARY",
      payload: "PRIVATE_PAYLOAD_CANARY",
    };
    const privateComplexEvidence = {
      inboundFrames: [{ requestId: "PRIVATE_FRAME_CANARY", payload: "PRIVATE_PAYLOAD_CANARY" }],
      pendingByRequest: new Map([["PRIVATE_MAP_CANARY", "PRIVATE_PAYLOAD_CANARY"]]),
    };
    let hostTransport: V2HostTransportDiagnostics = { ...transport, ...privateEvidence, ...privateComplexEvidence };
    const sidecarTransport = { ...transport, ...hostEvidence, ...privateEvidence, ...privateComplexEvidence };
    const sampler = new FleetSampler({
      logger,
      clock,
      machineProbe,
      intervalMs: 5_000,
      processMemoryPoolsSnapshot: () => ({
        hostEventReservedBytes: 705 * 1024 ** 2,
        responseGraphReservedBytes: 0,
        responseGraphMaximumBytes: 512 * 1024 ** 2,
        webSocketReservedBytes: 65 * 1024 ** 2,
        webSocketMaximumBytes: 1280 * 1024 ** 2,
      }),
      routerSnapshot: () => ({
        capacity: 512,
        active: 16,
        available: 496,
        queued: 0,
        queuedBytes: 1_234,
        fleetTransferReserved: 4,
        fleetTransferBudget: 4,
        fleetBulkTransferReserved: 0,
        fleetOrdinaryTransferBudget: 4,
        resources: {
          totalBytes: 32 * 1024 ** 3,
          startupFreeBytes: 20 * 1024 ** 3,
          startupRssBytes: 100,
          systemReserveBytes: 4 * 1024 ** 3,
          processCeilingBytes: 16 * 1024 ** 3,
          reservationCapacityBytes: 12 * 1024 ** 3,
          pauseFreeBytes: 2 * 1024 ** 3,
          resumeFreeBytes: 3 * 1024 ** 3,
          cpuTransformWorkers: 6,
          paused: false,
          reservedBytes: 256 * 1024 ** 2,
          queued: 0,
        },
        runtimes: [{
          projectId: projectId("00000000-0000-4000-8000-000000000001"),
          releaseRevision: "r3" as const,
          protocolRevision: "r2" as const,
          buildHash: V2_R3_RELEASE.buildHash,
          schemaHash: V2_R3_RELEASE.schemaHash,
          incarnation: 3,
          state: "ready" as const,
          capacity: 128,
          active: 4,
          available: 124,
          availabilityReason: "available" as const,
          quarantined: 0,
          ambiguousQuarantined: 1,
          ambiguityLimit: 4,
          heartbeatAgeMs: 500,
          sidecarPeerAgeMs: 250,
          transportConnected: true,
          credentialExpiresInMs: 3_000_000,
          ackEwmaMs: 15,
          completionEwmaMs: 2_000,
          recentFailureRate: 0,
          queueDepth: 0,
          memoryPressure: 0.1,
          failure: null,
          upstreamRateLimit: {
            hint: { limitTokens: 40_000_000, remainingTokens: 0, retryAfterMs: 914 },
            problem: {
              status: 429 as const,
              message: "Rate limit reached",
              type: "rate_limit_error",
              code: "rate_limit_exceeded",
              param: null,
              model: "gpt-5.6-sol",
              organization: "org-Example123",
              limitKind: "tokens_per_min",
              tokenLimit: 40_000_000,
              usedTokens: 40_000_000,
              requestedTokens: 609_930,
              retryAfterMs: 914,
            },
            requestedModel: "gpt-5.6-sol",
            observedAgoMs: 25,
          },
          upstreamRateLimits: {
            "gpt-5.6-sol": {
              hint: { limitTokens: 40_000_000, remainingTokens: 0, retryAfterMs: 914 },
              problem: {
                status: 429 as const,
                message: "Rate limit reached",
                type: "rate_limit_error",
                code: "rate_limit_exceeded",
                param: null,
                model: "gpt-5.6-sol",
                organization: "org-Example123",
                limitKind: "tokens_per_min",
                tokenLimit: 40_000_000,
                usedTokens: 40_000_000,
                requestedTokens: 609_930,
                retryAfterMs: 914,
              },
              requestedModel: "gpt-5.6-sol",
              observedAgoMs: 25,
            },
          },
          recovery: { attempt: 2, consecutiveFailures: 1, retryInMs: 1_000 },
          diagnostics: {
            host: { ...hostTransport, logicalEventsTotal: 0, responseBatchFramesTotal: 0, batchedEventsTotal: 0 },
            sidecar: {
              version: 1 as const,
              resource: {
                processRssBytes: 1_000,
                heapUsedBytes: 500,
                heapTotalBytes: 800,
                externalBytes: 100,
                arrayBufferBytes: 25,
                systemTotalBytes: 4_000,
                systemFreeBytes: 2_000,
                processCpuPercent: 20,
                normalizedLoad1: 0.25,
                eventLoopSamples: 8,
                eventLoopP50Ms: 1,
                eventLoopP95Ms: 3,
                eventLoopMaxMs: 5,
                gcSamples: 2,
                gcDurationP95Ms: 4,
                gcDurationMaxMs: 6,
              },
              transport: sidecarTransport,
              malformedCount: 0,
            },
          },
        }].flatMap(runtime => Array.from({ length: runtimeCopies }, (_, index) => ({
          ...runtime,
          projectId: projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`),
          upstreamRateLimits: runtimeCopies === 1 ? runtime.upstreamRateLimits : Object.fromEntries(
            Array.from({ length: 16 }, (_, model) => [`model-${model}`, {
              ...runtime.upstreamRateLimits["gpt-5.6-sol"],
              requestedModel: `model-${model}`,
              problem: { ...runtime.upstreamRateLimits["gpt-5.6-sol"].problem, model: `model-${model}` },
            }]),
          ),
        }))),
      }),
      cpuPoolSnapshot: () => ({ workers: 6, active: 1, queued: 0, maxActive: 2, closed: false }),
      eventLoop,
      memoryUsage: () => ({ rss: 500, heapTotal: 400, heapUsed: 300, external: 200, arrayBuffers: 100 }),
      cpuUsage: () => (cpu = { user: cpu.user + 250_000, system: cpu.system + 50_000 }),
      webSocketSnapshot: () => webSocketTelemetry.snapshotAndReset(),
      recoveryCircuitSnapshot: () => ({
        state: "open",
        scope: "outage",
        consecutiveFailures: 2,
        queued: 3,
        probeInFlight: false,
        revalidationInFlight: false,
        nextProbeAtMs: clock.now() + 1_000,
        lastFailureKind: "server",
      }),
      routerTrafficSnapshot: () => ({
        accepted: 50,
        committed: 48,
        succeeded: 45,
        failed: 3,
        rateLimited: 1,
        retries: 2,
        responseBytes: 4096,
        queueWaitMs: { count: 48, p50: 4, p95: 20, max: 30 },
        commitAckMs: { count: 48, p50: 30, p95: 100, max: 150 },
        totalMs: { count: 48, p50: 500, p95: 2_000, max: 3_000 },
        firstByteMs: { count: 47, p50: 100, p95: 400, max: 600 },
      }),
      hostEventMemorySnapshot: () => ({
        ordinaryCapacityBytes: 64 * 1024 ** 2,
        ordinaryUsedBytes: 2 * 1024 ** 2,
        ordinaryAvailableBytes: 62 * 1024 ** 2,
        jumboCapacityBytes: 257 * 1024 ** 2,
        jumboUsedBytes: 0,
        jumboAvailableBytes: 257 * 1024 ** 2,
        jumboActive: false,
        activePermits: 4,
        waiters: 1,
        ordinaryWaiters: 1,
        jumboWaiters: 0,
        promotionWaiters: 0,
        closed: false,
      }),
      fleetSnapshot: () => ({
        desired: 2,
        ready: fleetRecovered ? 2 : 1,
        provisioning: 0,
        draining: 0,
        failed: fleetRecovered ? 0 : 1,
        unavailable: fleetRecovered ? 0 : 1,
        unavailableProjects: fleetRecovered ? [] : [unavailableProject],
        projects: [
          projectId("00000000-0000-4000-8000-000000000001"),
          unavailableProject,
        ],
      }),
    });

    sampler.sample();
    const partial = logger.events.at(-1)!;
    expect(partial).toMatchObject({
      desired: 2,
      ready: 1,
      unavailable: 1,
      unavailable_projects: [unavailableProject],
      unavailable_projects_truncated: false,
      pending_retirements: 0,
      project_mutation_state: "closed",
      project_mutation_failures: 0,
      project_mutation_retry_ms: null,
    });
    const partialDetail = logger.events.at(-2)!;
    expect(partialDetail).toMatchObject({ event: "fleet.runtime_sample", sample_id: partial.sample_id, runtime_index: 0, runtime_count: 1, ts: partial.ts });
    expect((partialDetail.runtime as any).diagnostics.host).toMatchObject({
      retained_precommit_cancellations: 0,
      last_published_host_credit_ceiling: null,
      oldest_outstanding_frame_kind: null,
      outstanding_flight_age_ms: null,
      data_peer_age_ms: null,
      control_peer_age_ms: null,
      last_data_revision: null,
      last_control_revision: null,
      inbound_accepted_epoch: null,
      inbound_accepted_sequence: null,
      inbound_processed_epoch: null,
      inbound_processed_sequence: null,
      inbound_accepted_progress_age_ms: null,
      inbound_processed_progress_age_ms: null,
      inbound_processing_frames: null,
      sidecar_diagnostics_age_ms: null,
      sidecar_pending_age_ms: null,
      sidecar_pending_data_publications: null,
    });
    expect(() => assertSafeObservabilityEvent(partial)).not.toThrow();
    hostTransport = { ...hostTransport, ...hostEvidence };
    webSocketTelemetry.recordIngressBudget({
      ingressReservedBytes: 17 * 1024 ** 2,
      maxIngressBytes: 512 * 1024 ** 2,
      ingressGuaranteedSessions: 27,
    });
    webSocketTelemetry.ingressPromoted(24 * 1024 ** 2);
    webSocketTelemetry.ingressParserRejected();
    fleetRecovered = true;
    clock.value += 5_000;
    sampler.sample();

    const event = logger.events.at(-1)!;
    const detail = logger.events.at(-2)!;
    expect(detail).toMatchObject({ event: "fleet.runtime_sample", sample_id: event.sample_id, runtime_index: 0, runtime_count: 1, ts: event.ts });
    expect(event.sample_id).toBe(2);
    expect(event).toMatchObject({
      event: "fleet.sample",
      capacity: 512,
      active: 16,
      queued_bytes: 1_234,
      desired: 2,
      ready: 2,
      unavailable: 0,
      unavailable_projects: [],
      event_loop_p50_ms: 2,
      event_loop_p95_ms: 5,
      event_loop_max_ms: 8,
      external_bytes: 200,
      array_buffer_bytes: 100,
    });
    expect((event.resources as any).transform_workers).toBe(6);
    expect(event.resources).not.toHaveProperty("cpu_active");
    expect(event.resources).not.toHaveProperty("cpu_queued");
    expect((event.resources as any).fleet_transfer_reserved).toBe(4);
    expect((event.resources as any).fleet_transfer_budget).toBe(4);
    expect((event.resources as any).reservation_headroom_bytes).toBe(
      12 * 1024 ** 3 - 256 * 1024 ** 2,
    );
    expect((event.resources as any).process_pools).toEqual({
      host_event_reserved_bytes: 705 * 1024 ** 2,
      response_graph_reserved_bytes: 0,
      response_graph_maximum_bytes: 512 * 1024 ** 2,
      websocket_reserved_bytes: 65 * 1024 ** 2,
      websocket_maximum_bytes: 1280 * 1024 ** 2,
    });
    expect((event.resources as any).event_memory).toMatchObject({
      ordinary_capacity_bytes: 64 * 1024 ** 2,
      ordinary_used_bytes: 2 * 1024 ** 2,
      jumbo_capacity_bytes: 257 * 1024 ** 2,
      jumbo_active: false,
      active_permits: 4,
      waiters: 1,
    });
    expect(event.websocket).toMatchObject({
      ingressPromotionBytesPeak: 24 * 1024 ** 2,
      ingressReservedBytes: 17 * 1024 ** 2,
      ingressReservedBytesPeak: 17 * 1024 ** 2,
      ingressCapacityBytes: 512 * 1024 ** 2,
      ingressGuaranteedSessions: 27,
      ingressPromotions: 1,
      ingressPromotionsTotal: 1,
      ingressParserRejections: 1,
      ingressParserRejectionsTotal: 1,
    });
    expect((event.runtimes as any[])[0]).toEqual({
      project_id: "00000000-0000-4000-8000-000000000001",
      state: "ready", capacity: 128, active: 4, available: 124,
      availability_reason: "available", quarantined: 0,
      ambiguous_quarantined: 1, queue_depth: 0,
    });
    expect(detail.runtime).toMatchObject({
      project_id: "00000000-0000-4000-8000-000000000001",
      release_revision: "r3",
      protocol_revision: "r2",
      build_hash: V2_R3_RELEASE.buildHash,
      schema_hash: V2_R3_RELEASE.schemaHash,
      incarnation: 3,
      awareness_rtt_ms: 15,
      availability_reason: "available",
      sidecar_peer_age_ms: 250,
      transport_connected: true,
      recovery: { attempt: 2, consecutive_failures: 1, retry_in_ms: 1_000 },
      ambiguous_quarantined: 1,
      ambiguity_limit: 4,
      completion_ewma_ms: 2_000,
      rate_limit_observations: [{
        model: "gpt-5.6-sol",
        limit: 40_000_000,
        used: 40_000_000,
        requested: 609_930,
        remaining: 0,
        retry_after_ms: 914,
        observed_ago_ms: 25,
      }],
      diagnostics: {
        host: {
          retained_precommit_cancellations: 2,
          last_published_host_credit_ceiling: 128,
          oldest_outstanding_frame_kind: "request_cancel",
          outstanding_flight_age_ms: 25.5,
          data_peer_age_ms: 2_000.25,
          control_peer_age_ms: 30_000,
          last_data_revision: 987,
          last_control_revision: 12,
          inbound_accepted_epoch: 2,
          inbound_accepted_sequence: 73,
          inbound_processed_epoch: 2,
          inbound_processed_sequence: 73,
          inbound_accepted_progress_age_ms: 35_000.5,
          inbound_processed_progress_age_ms: 35_000.25,
          inbound_processing_frames: 0,
          sidecar_diagnostics_age_ms: 12.5,
          sidecar_pending_age_ms: 34_000,
          sidecar_pending_data_publications: 7,
        },
        sidecar: {
          resource: {
            process_cpu_percent: 20,
            array_buffer_bytes: 25,
            event_loop_samples: 8,
            gc_samples: 2,
            gc_duration_p95_ms: 4,
          },
          transport: {
            logical_events_total: 40,
            response_batch_frames_total: 8,
            logical_events_per_batch: 4,
            logical_events_per_publication: 4,
            logical_events_per_data_publication: 5,
            data_publications_total: 8,
            control_publications_total: 2,
            adaptive_capacity: 8,
          },
        },
      },
    });
    const diagnostics = (detail.runtime as any).diagnostics;
    for (const key of [
      "retained_precommit_cancellations", "last_published_host_credit_ceiling",
      "oldest_outstanding_frame_kind", "outstanding_flight_age_ms",
      "data_peer_age_ms", "control_peer_age_ms", "last_data_revision", "last_control_revision",
      "inbound_accepted_epoch", "inbound_accepted_sequence", "inbound_processed_epoch", "inbound_processed_sequence",
      "inbound_accepted_progress_age_ms", "inbound_processed_progress_age_ms", "inbound_processing_frames",
      "sidecar_diagnostics_age_ms", "sidecar_pending_age_ms", "sidecar_pending_data_publications",
      ...Object.keys(hostEvidence),
    ]) {
      expect(diagnostics.sidecar.transport).not.toHaveProperty(key);
    }
    for (const key of [...Object.keys(privateEvidence), ...Object.keys(privateComplexEvidence)]) {
      expect(diagnostics.host).not.toHaveProperty(key);
      expect(diagnostics.sidecar.transport).not.toHaveProperty(key);
    }
    for (const value of Object.values(privateEvidence)) {
      expect(JSON.stringify(event)).not.toContain(value);
      expect(JSON.stringify(detail)).not.toContain(value);
    }
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
    expect(() => assertSafeObservabilityEvent(detail)).not.toThrow();
    expect(JSON.stringify(event)).not.toContain("org-Example123");
    expect(event.routable).toBe(1);
    expect(event.provisioning_circuit).toMatchObject({
      state: "open",
      scope: "outage",
      consecutive_failures: 2,
      retry_in_ms: 1_000,
      queued: 3,
    });
    expect(event.release).toEqual({
      knownRuntimes: 1,
      unknownRuntimes: 0,
      selections: [{
        releaseRevision: "r3",
        protocolRevision: "r2",
        buildHash: V2_R3_RELEASE.buildHash,
        schemaHash: V2_R3_RELEASE.schemaHash,
        runtimes: 1,
      }],
    });
    expect(event.traffic).toMatchObject({
      accepted: 50,
      succeeded: 45,
      failed: 3,
      requests_per_second: 9.6,
      total: { p50_ms: 500, p95_ms: 2_000, max_ms: 3_000 },
    });
    expect(event).not.toHaveProperty("rate_limit");
    expect(JSON.stringify(event)).not.toMatch(/token|cookie|prompt|output|url/i);

    // Exercise the actual logger ceiling and queue reservations with every
    // configured runtime, full diagnostics and all sixteen model observations.
    runtimeCopies = 32;
    const lines: string[] = [];
    const config = resolveObservabilityConfig({ GP_LOG_QUEUE_MAX_BYTES: String(1024 ** 2), GP_LOG_QUEUE_MAX_EVENTS: "33" });
    const boundedLogger = new ObservabilityLogger({ config, sinks: [{
      writeBatch: async batch => { lines.push(...batch); }, close: async () => {},
    }] });
    logger.delegate = boundedLogger;
    sampler.sample();
    expect(boundedLogger.snapshot()).toMatchObject({ queuedEvents: 33, droppedSamples: 0, sinkErrors: 0 });
    expect(boundedLogger.snapshot().queuedBytes).toBeLessThanOrEqual(config.queueMaxBytes);
    await boundedLogger.close();
    const records = lines.map(line => JSON.parse(line));
    const aggregate = records.find(record => record.event === "fleet.sample")!;
    const details = records.filter(record => record.event === "fleet.runtime_sample");
    expect(details).toHaveLength(32);
    expect(aggregate.runtimes).toHaveLength(32);
    expect(aggregate).toMatchObject({ sample_id: 3, routable: 32 });
    expect(latestFleetSampleFromCompleteTail(lines.join(""), [], Date.parse(aggregate.ts)).runtimes)
      .toHaveLength(32);
    expect(Buffer.byteLength(JSON.stringify(aggregate))).toBeLessThan(64 * 1024);
    for (let index = 0; index < details.length; index++) {
      expect(details[index]).toMatchObject({ sample_id: aggregate.sample_id, ts: aggregate.ts, runtime_index: index, runtime_count: 32 });
      expect(details[index].runtime.rate_limit_observations).toHaveLength(16);
      expect(details[index].runtime.diagnostics.host).toMatchObject({
        retained_precommit_cancellations: 2,
        data_peer_age_ms: 2_000.25, control_peer_age_ms: 30_000,
        last_data_revision: 987, last_control_revision: 12,
        inbound_accepted_epoch: 2, inbound_accepted_sequence: 73,
        inbound_processed_epoch: 2, inbound_processed_sequence: 73,
        inbound_accepted_progress_age_ms: 35_000.5, inbound_processed_progress_age_ms: 35_000.25,
        inbound_processing_frames: 0, sidecar_diagnostics_age_ms: 12.5,
        sidecar_pending_age_ms: 34_000, sidecar_pending_data_publications: 7,
      });
      expect(Buffer.byteLength(JSON.stringify(details[index]))).toBeLessThan(64 * 1024);
    }
    expect(boundedLogger.snapshot()).toMatchObject({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, sinkErrors: 0 });
    expect(lines.join("")).not.toContain("PRIVATE_");

    const pressuredLines: string[] = [];
    const pressured = new ObservabilityLogger({ config: { ...config, queueMaxEvents: 1 }, sinks: [{
      writeBatch: async batch => { pressuredLines.push(...batch); }, close: async () => {},
    }] });
    logger.delegate = pressured;
    sampler.sample();
    expect(pressured.snapshot()).toMatchObject({ queuedEvents: 1, droppedSamples: 32 });
    expect(pressured.snapshot().queuedBytes).toBeLessThanOrEqual(config.queueMaxBytes);
    await pressured.close();
    expect(pressuredLines.map(line => JSON.parse(line)).filter(record => record.event === "fleet.sample"))
      .toMatchObject([{ sample_id: 4, routable: 32 }]);
    await sampler.close();
  });
});
