import { describe, expect, it } from "bun:test";
import type { Clock, MachineProbe } from "../src/core/ports";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { FleetSampler } from "../src/observability/sampler";
import { projectId } from "../src/sandbox/types";
import type { V2TransportDiagnostics } from "../src/v2/diagnostics/types";
import { WebSocketTelemetry } from "../src/ws/telemetry";

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  emit(event: ObservabilityEvent): void { this.events.push(event); }
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
  it("emits sanitized aggregate, host, logger, and per-runtime metrics", () => {
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
    const sampler = new FleetSampler({
      logger,
      clock,
      machineProbe,
      intervalMs: 5_000,
      routerSnapshot: () => ({
        capacity: 512,
        active: 16,
        available: 496,
        queued: 0,
        queuedBytes: 1_234,
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
          cpuActive: 1,
          cpuQueued: 0,
        },
        runtimes: [{
          projectId: projectId("00000000-0000-4000-8000-000000000001"),
          releaseRevision: "r3" as const,
          protocolRevision: "r2" as const,
          buildHash: "a".repeat(64),
          schemaHash: "b".repeat(64),
          incarnation: 3,
          state: "ready",
          capacity: 128,
          active: 4,
          available: 124,
          availabilityReason: "available" as const,
          quarantined: 0,
          ambiguousQuarantined: 1,
          ambiguityLimit: 4,
          heartbeatAgeMs: 500,
          sidecarPeerAgeMs: 250,
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
              status: 429,
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
                status: 429,
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
            host: { ...transport, logicalEventsTotal: 0, responseBatchFramesTotal: 0, batchedEventsTotal: 0 },
            sidecar: {
              version: 1,
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
              transport,
              malformedCount: 0,
            },
          },
        }],
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
    });
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
    expect((event.runtimes as any[])[0]).toMatchObject({
      project_id: "00000000-0000-4000-8000-000000000001",
      release_revision: "r3",
      protocol_revision: "r2",
      build_hash: "a".repeat(64),
      schema_hash: "b".repeat(64),
      incarnation: 3,
      awareness_rtt_ms: 15,
      availability_reason: "available",
      sidecar_peer_age_ms: 250,
      recovery: { attempt: 2, consecutive_failures: 1, retry_in_ms: 1_000 },
      ambiguous_quarantined: 1,
      ambiguity_limit: 4,
      completion_ewma_ms: 2_000,
      rate_limit_observations: [{
        model: "gpt-5.6-sol",
        organization: "org-Example123",
        limit: 40_000_000,
        used: 40_000_000,
        requested: 609_930,
        remaining: 0,
        retry_after_ms: 914,
        observed_ago_ms: 25,
      }],
      diagnostics: {
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
        buildHash: "a".repeat(64),
        schemaHash: "b".repeat(64),
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
  });
});
