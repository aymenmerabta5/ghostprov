import { describe, expect, it } from "bun:test";
import type { LoggerLike } from "../src/observability/logger";
import type { ObservabilityEvent } from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import {
  main,
  monitorBackoffMs,
  pollMonitorSample,
  resolveMonitorOptions,
  runMonitor,
} from "../scripts/monitor_v2";

class MemoryLogger implements LoggerLike {
  readonly events: ObservabilityEvent[] = [];
  onEmit?: (event: ObservabilityEvent) => void;
  emit(event: ObservabilityEvent): void {
    this.events.push(event);
    this.onEmit?.(event);
  }
  snapshot() {
    return {
      queuedEvents: 0,
      queuedBytes: 0,
      droppedSamples: 0,
      droppedProgress: 0,
      droppedTerminals: 0,
      sinkErrors: 0,
    };
  }
  async close(): Promise<void> {}
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function successfulFetch(onRequest?: (path: string, init?: RequestInit) => void): typeof fetch {
  return (async (input: string | URL | Request, init?: RequestInit) => {
    const path = new URL(String(input)).pathname;
    onRequest?.(path, init);
    if (path === "/health") {
      return json({ ok: true, capacity: 512, active: 16, available: 496, queued: 0 });
    }
    if (path === "/__sandboxes") {
      return json({
        capacity: 512,
        active: 16,
        available: 496,
        queued: 0,
        prompt: "must never be copied",
        resources: {
          reservedBytes: 1_024,
          reservationCapacityBytes: 65_536,
          paused: false,
          queued: 0,
          cpuActive: 2,
          cpuQueued: 1,
          forbiddenToken: "sk-secret-value-that-must-not-appear",
        },
        runtimes: [{
          projectId: "00000000-0000-4000-8000-000000000001",
          state: "ready",
          capacity: 128,
          active: 4,
          available: 124,
          quarantined: 0,
          queueDepth: 0,
          ackEwmaMs: 12,
          completionEwmaMs: 2_000,
          recentFailureRate: 0,
          heartbeatAgeMs: 500,
          credentialExpiresInMs: 3_000_000,
          memoryPressure: 0.1,
          output: "must never be copied",
        }],
      });
    }
    return json({ error: "unexpected endpoint" }, 404);
  }) as typeof fetch;
}

describe("V2 external monitor", () => {
  it("accepts only a loopback root and rejects paths that could target mutating endpoints", () => {
    expect(resolveMonitorOptions({})).toEqual({
      root: "http://127.0.0.1:18787",
      intervalMs: 2_000,
      requestTimeoutMs: 5_000,
    });
    expect(() => resolveMonitorOptions({
      GP_MONITOR_ROOT: "https://example.com",
    })).toThrow("loopback");
    expect(() => resolveMonitorOptions({
      GP_MONITOR_ROOT: "http://127.0.0.1:18787/v1/responses",
    })).toThrow("origin only");
  });

  it("polls only the two read-only endpoints with GET and emits sanitized metrics", async () => {
    const requests: Array<{ path: string; method: string | undefined }> = [];
    const event = await pollMonitorSample(
      resolveMonitorOptions({}),
      successfulFetch((path, init) => requests.push({ path, method: init?.method })),
    );

    expect(requests).toEqual([
      { path: "/health", method: "GET" },
      { path: "/__sandboxes", method: "GET" },
    ]);
    expect(event).toMatchObject({
      event: "monitor.sample",
      health_status: 200,
      sandboxes_status: 200,
      capacity: 512,
      active: 16,
      available: 496,
      queued: 0,
    });
    expect((event.resources as any)).toEqual({
      reserved_bytes: 1_024,
      reservation_capacity_bytes: 65_536,
      memory_paused: false,
      governor_queued: 0,
      cpu_active: 2,
      cpu_queued: 1,
    });
    expect((event.runtimes as any[])[0]).toMatchObject({
      project_id: "00000000-0000-4000-8000-000000000001",
      awareness_rtt_ms: 12,
      completion_ewma_ms: 2_000,
    });
    expect(JSON.stringify(event)).not.toMatch(/prompt|output|forbiddenToken|sk-secret/i);
    expect(() => assertSafeObservabilityEvent(event)).not.toThrow();
  });

  it("reports HTTP failure without reading or copying its response body", async () => {
    const fetchImpl = (async (input: string | URL | Request) => {
      const path = new URL(String(input)).pathname;
      return path === "/health"
        ? json({ secret: "must not be read" }, 503)
        : json({ capacity: 0, runtimes: [] });
    }) as typeof fetch;

    await expect(pollMonitorSample(resolveMonitorOptions({}), fetchImpl))
      .rejects.toThrow("GET /health returned HTTP 503");
  });

  it("cancels the sibling read when either endpoint fails", async () => {
    let siblingAborted = false;
    const fetchImpl = (async (input: string | URL | Request, init?: RequestInit) => {
      const path = new URL(String(input)).pathname;
      if (path === "/health") return json({}, 503);
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          siblingAborted = true;
          reject(new DOMException("aborted", "AbortError"));
        }, { once: true });
      });
    }) as unknown as typeof fetch;

    await expect(pollMonitorSample(resolveMonitorOptions({}), fetchImpl))
      .rejects.toThrow("GET /health returned HTTP 503");
    await Bun.sleep(0);
    expect(siblingAborted).toBe(true);
  });

  it("uses deterministic exponential jitter with a hard 30 second cap", () => {
    expect(monitorBackoffMs(1, () => 0)).toBe(500);
    expect(monitorBackoffMs(1, () => 1)).toBe(600);
    expect(monitorBackoffMs(4, () => 0.5)).toBe(4_400);
    expect(monitorBackoffMs(20, () => 1)).toBe(30_000);
  });

  it("sanitizes transport errors and exits cleanly when aborted", async () => {
    const logger = new MemoryLogger();
    const controller = new AbortController();
    logger.onEmit = event => {
      if (event.event === "monitor.error") controller.abort();
    };
    const fetchImpl = (async () => {
      throw new Error("Authorization: Bearer sk-secret-value-that-must-not-appear");
    }) as unknown as typeof fetch;

    await runMonitor(resolveMonitorOptions({}), {
      logger,
      fetchImpl,
      signal: controller.signal,
      random: () => 0,
      sleep: async () => {},
    });

    expect(logger.events).toHaveLength(1);
    expect(logger.events[0]).toMatchObject({
      event: "monitor.error",
      attempt: 1,
      retry_ms: 500,
    });
    expect(JSON.stringify(logger.events[0])).toContain("[REDACTED]");
    expect(JSON.stringify(logger.events[0])).not.toContain("sk-secret-value");
    expect(() => assertSafeObservabilityEvent(logger.events[0]!)).not.toThrow();
  });

  it("is import-safe and returns from a successful loop on abort", async () => {
    expect(typeof main).toBe("function");
    const logger = new MemoryLogger();
    const controller = new AbortController();
    logger.onEmit = event => {
      if (event.event === "monitor.sample") controller.abort();
    };

    await runMonitor(resolveMonitorOptions({}), {
      logger,
      fetchImpl: successfulFetch(),
      signal: controller.signal,
      sleep: async () => {},
    });

    expect(logger.events.map(event => event.event)).toEqual(["monitor.sample"]);
  });
});
