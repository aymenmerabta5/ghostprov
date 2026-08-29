import { resolveObservabilityConfig } from "../src/observability/config";
import {
  observabilityEvent,
  type JsonValue,
  type ObservabilityEvent,
} from "../src/observability/events";
import {
  createObservabilityLogger,
  type LoggerLike,
} from "../src/observability/logger";
import {
  safeObservabilityModel,
  sanitizeDiagnosticMessage,
} from "../src/observability/redaction";
import { resolveGhostProvApiKey } from "../src/http/api-key";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const RUNTIME_STATES = new Set([
  "configured",
  "provisioning",
  "syncing_credentials",
  "syncing_workspace",
  "deploying_daemon",
  "recovering",
  "ready",
  "auth_required",
  "draining",
  "failed",
  "stopped",
]);
const MAX_MONITOR_RUNTIMES = 256;
const MONITOR_RESPONSE_MAX_BYTES = 2 * 1024 * 1024;

export interface MonitorOptions {
  root: string;
  apiKey: string;
  intervalMs: number;
  requestTimeoutMs: number;
}

export interface MonitorRunDependencies {
  logger: LoggerLike;
  signal: AbortSignal;
  fetchImpl?: typeof fetch;
  random?: () => number;
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

class MonitorPollError extends Error {
  pollMs = 0;

  constructor(
    readonly kind: "http" | "timeout" | "transport" | "invalid_response",
    message: string,
    readonly status: number | null = null,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "MonitorPollError";
  }
}

function integer(
  value: string | undefined,
  fallback: number,
  name: string,
  minimum: number,
  maximum: number,
): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new RangeError(`${name} must be an integer from ${minimum} through ${maximum}`);
  }
  return parsed;
}

function monitorRoot(raw: string | undefined): string {
  let parsed: URL;
  try {
    parsed = new URL(raw?.trim() || "http://127.0.0.1:18787");
  } catch (error) {
    throw new TypeError("GP_MONITOR_ROOT must be a valid URL", { cause: error });
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new TypeError("GP_MONITOR_ROOT must use HTTP or HTTPS");
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash) {
    throw new TypeError("GP_MONITOR_ROOT must not contain credentials, a query, or a fragment");
  }
  if (parsed.pathname !== "/") {
    throw new TypeError("GP_MONITOR_ROOT must contain the origin only");
  }
  const hostname = parsed.hostname.toLowerCase().replace(/^\[|\]$/g, "");
  const loopback = hostname === "localhost" ||
    hostname.endsWith(".localhost") ||
    hostname === "::1" ||
    /^127(?:\.\d{1,3}){3}$/.test(hostname);
  if (!loopback) throw new TypeError("GP_MONITOR_ROOT must resolve to a loopback host");
  return parsed.origin;
}

export function resolveMonitorOptions(
  env: Record<string, string | undefined> = process.env,
): MonitorOptions {
  return {
    root: monitorRoot(env.GP_MONITOR_ROOT),
    apiKey: resolveGhostProvApiKey(env.GHOSTPROV_API_KEY),
    intervalMs: integer(
      env.GP_MONITOR_INTERVAL_MS,
      2_000,
      "GP_MONITOR_INTERVAL_MS",
      100,
      3_600_000,
    ),
    requestTimeoutMs: integer(
      env.GP_MONITOR_TIMEOUT_MS,
      5_000,
      "GP_MONITOR_TIMEOUT_MS",
      100,
      60_000,
    ),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function metric(value: unknown): number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value * 1_000) / 1_000
    : 0;
}

function nullableMetric(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? Math.round(value * 1_000) / 1_000
    : null;
}

function runtimeMetrics(value: unknown): JsonValue | null {
  if (!isRecord(value)) return null;
  const id = typeof value.projectId === "string" && UUID.test(value.projectId)
    ? value.projectId
    : "unknown";
  const state = typeof value.state === "string" && RUNTIME_STATES.has(value.state)
    ? value.state
    : "unknown";
  const observations = isRecord(value.upstreamRateLimits)
    ? Object.entries(value.upstreamRateLimits).slice(0, 16)
    : [];
  const rateLimitObservations = observations.flatMap(([fallbackModel, raw]) => {
    if (!isRecord(raw)) return [];
    const problem = isRecord(raw.problem) ? raw.problem : null;
    const hint = isRecord(raw.hint) ? raw.hint : null;
    const rawKind = typeof problem?.limitKind === "string" && problem.limitKind.length <= 64
      ? problem.limitKind
      : null;
    return [{
      model: safeObservabilityModel(problem?.model ?? raw.requestedModel ?? fallbackModel),
      kind: rawKind
        ? rawKind.includes("token")
          ? "tpm"
          : rawKind.includes("request")
            ? "rpm"
            : /^[a-z][a-z0-9_]{0,63}$/.test(rawKind) ? rawKind : null
        : null,
      limit: nullableMetric(problem?.tokenLimit ?? hint?.limitTokens),
      used: nullableMetric(problem?.usedTokens),
      requested: nullableMetric(problem?.requestedTokens),
      remaining: nullableMetric(hint?.remainingTokens),
      retry_after_ms: nullableMetric(problem?.retryAfterMs ?? hint?.retryAfterMs),
      observed_ago_ms: nullableMetric(raw.observedAgoMs),
    } satisfies JsonValue];
  });
  return {
    project_id: id,
    state,
    capacity: metric(value.capacity),
    active: metric(value.active),
    available: metric(value.available),
    quarantined: metric(value.quarantined),
    ambiguous_quarantined: metric(value.ambiguousQuarantined),
    ambiguity_limit: metric(value.ambiguityLimit),
    queue_depth: metric(value.queueDepth),
    awareness_rtt_ms: metric(value.ackEwmaMs),
    completion_ewma_ms: metric(value.completionEwmaMs),
    recent_failure_rate: metric(value.recentFailureRate),
    heartbeat_age_ms: metric(value.heartbeatAgeMs),
    sidecar_peer_age_ms: metric(value.sidecarPeerAgeMs),
    credential_expires_in_ms: metric(value.credentialExpiresInMs),
    memory_pressure: metric(value.memoryPressure),
    recovery: isRecord(value.recovery) ? {
      attempt: metric(value.recovery.attempt),
      consecutive_failures: metric(value.recovery.consecutiveFailures),
      retry_in_ms: metric(value.recovery.retryInMs),
    } : null,
    rate_limit_observations: rateLimitObservations,
  };
}

function resourceMetrics(value: unknown): JsonValue {
  const resources = isRecord(value) ? value : {};
  return {
    reserved_bytes: metric(resources.reservedBytes),
    reservation_capacity_bytes: metric(resources.reservationCapacityBytes),
    memory_paused: resources.paused === true,
    governor_queued: metric(resources.queued),
  };
}

function requestScope(timeoutMs: number, parent?: AbortSignal): {
  signal: AbortSignal;
  timedOut: () => boolean;
  dispose: () => void;
} {
  const controller = new AbortController();
  let timeout = false;
  const onAbort = () => controller.abort(parent?.reason);
  if (parent?.aborted) onAbort();
  else parent?.addEventListener("abort", onAbort, { once: true });
  const timer = setTimeout(() => {
    timeout = true;
    controller.abort(new DOMException("Monitor request timed out", "TimeoutError"));
  }, timeoutMs);
  timer.unref?.();
  return {
    signal: controller.signal,
    timedOut: () => timeout,
    dispose: () => {
      clearTimeout(timer);
      parent?.removeEventListener("abort", onAbort);
      if (!controller.signal.aborted) {
        controller.abort(new DOMException("Monitor poll finished", "AbortError"));
      }
    },
  };
}

async function getJson(
  root: string,
  path: "/health" | "/__sandboxes",
  signal: AbortSignal,
  timedOut: () => boolean,
  fetchImpl: typeof fetch,
  apiKey: string,
): Promise<{ status: number; value: unknown }> {
  let response: Response;
  try {
    response = await fetchImpl(new URL(path, root), {
      method: "GET",
      signal,
      redirect: "error",
      credentials: "omit",
      cache: "no-store",
      ...(path === "/__sandboxes"
        ? { headers: { authorization: `Bearer ${apiKey}` } }
        : {}),
    });
  } catch (error) {
    throw new MonitorPollError(
      timedOut() ? "timeout" : "transport",
      timedOut() ? `GET ${path} timed out` : `GET ${path} failed`,
      null,
      { cause: error },
    );
  }
  if (!response.ok) {
    throw new MonitorPollError(
      "http",
      `GET ${path} returned HTTP ${response.status}`,
      response.status,
    );
  }
  try {
    const declared = response.headers.get("content-length");
    if (declared && /^\d+$/.test(declared) && Number(declared) > MONITOR_RESPONSE_MAX_BYTES) {
      void response.body?.cancel().catch(() => undefined);
      throw new MonitorPollError(
        "invalid_response",
        `GET ${path} exceeded the bounded response limit`,
        response.status,
      );
    }
    const reader = response.body?.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    if (reader) {
      try {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          total += next.value.byteLength;
          if (total > MONITOR_RESPONSE_MAX_BYTES) {
            void reader.cancel().catch(() => undefined);
            throw new MonitorPollError(
              "invalid_response",
              `GET ${path} exceeded the bounded response limit`,
              response.status,
            );
          }
          chunks.push(next.value.slice());
        }
      } finally {
        try { reader.releaseLock(); } catch {}
      }
    }
    const text = chunks.length === 0
      ? ""
      : Buffer.concat(chunks.map(chunk => Buffer.from(
          chunk.buffer,
          chunk.byteOffset,
          chunk.byteLength,
        )), total).toString("utf8");
    return { status: response.status, value: JSON.parse(text) };
  } catch (error) {
    if (error instanceof MonitorPollError) throw error;
    throw new MonitorPollError(
      "invalid_response",
      `GET ${path} returned invalid JSON`,
      response.status,
      { cause: error },
    );
  }
}

export async function pollMonitorSample(
  options: MonitorOptions,
  fetchImpl: typeof fetch = fetch,
  parentSignal?: AbortSignal,
): Promise<ObservabilityEvent> {
  const startedAt = performance.now();
  const scope = requestScope(options.requestTimeoutMs, parentSignal);
  try {
    const [health, sandboxes] = await Promise.all([
      getJson(options.root, "/health", scope.signal, scope.timedOut, fetchImpl, options.apiKey),
      getJson(options.root, "/__sandboxes", scope.signal, scope.timedOut, fetchImpl, options.apiKey),
    ]);
    if (!isRecord(health.value) || !isRecord(sandboxes.value)) {
      throw new MonitorPollError(
        "invalid_response",
        "Monitor endpoints returned an invalid JSON object",
      );
    }
    const runtimes = Array.isArray(sandboxes.value.runtimes)
      ? sandboxes.value.runtimes.slice(0, MAX_MONITOR_RUNTIMES)
        .map(runtimeMetrics)
        .filter((runtime): runtime is JsonValue => runtime !== null)
      : [];
    return observabilityEvent("monitor.sample", {
      poll_ms: Math.round((performance.now() - startedAt) * 1_000) / 1_000,
      health_status: health.status,
      sandboxes_status: sandboxes.status,
      capacity: metric(sandboxes.value.capacity),
      active: metric(sandboxes.value.active),
      available: metric(sandboxes.value.available),
      queued: metric(sandboxes.value.queued),
      resources: resourceMetrics(sandboxes.value.resources),
      runtimes,
    });
  } catch (error) {
    if (error instanceof MonitorPollError) {
      error.pollMs = Math.round((performance.now() - startedAt) * 1_000) / 1_000;
    }
    throw error;
  } finally {
    scope.dispose();
  }
}

export function monitorBackoffMs(
  consecutiveFailures: number,
  random: () => number = Math.random,
): number {
  const exponent = Math.max(0, Math.min(30, Math.trunc(consecutiveFailures) - 1));
  const base = Math.min(30_000, 500 * (2 ** exponent));
  const rawJitter = random();
  const jitter = Number.isFinite(rawJitter)
    ? Math.max(0, Math.min(1, rawJitter))
    : 0;
  return Math.min(30_000, Math.round(base * (1 + jitter * 0.2)));
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise(resolve => {
    const timer = setTimeout(done, ms);
    timer.unref?.();
    function done(): void {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    }
    signal.addEventListener("abort", done, { once: true });
  });
}

function errorDetails(error: unknown): {
  pollMs: number;
  status: number | null;
  name: string;
  message: string;
} {
  if (error instanceof MonitorPollError) {
    const cause = error.cause instanceof Error ? error.cause : null;
    return {
      pollMs: error.pollMs,
      status: error.status,
      name: error.kind,
      message: sanitizeDiagnosticMessage(cause?.message || error.message),
    };
  }
  return {
    pollMs: 0,
    status: null,
    name: error instanceof Error
      ? sanitizeDiagnosticMessage(error.name)
      : "unknown",
    message: sanitizeDiagnosticMessage(error instanceof Error ? error.message : error),
  };
}

export async function runMonitor(
  options: MonitorOptions,
  dependencies: MonitorRunDependencies,
): Promise<void> {
  const fetchImpl = dependencies.fetchImpl ?? fetch;
  const random = dependencies.random ?? Math.random;
  const sleep = dependencies.sleep ?? abortableSleep;
  let failures = 0;
  while (!dependencies.signal.aborted) {
    try {
      const sample = await pollMonitorSample(options, fetchImpl, dependencies.signal);
      if (dependencies.signal.aborted) return;
      dependencies.logger.emit(sample, "sample");
      failures = 0;
      await sleep(options.intervalMs, dependencies.signal);
    } catch (error) {
      if (dependencies.signal.aborted) return;
      failures++;
      const retryMs = monitorBackoffMs(failures, random);
      const details = errorDetails(error);
      dependencies.logger.emit(observabilityEvent("monitor.error", {
        poll_ms: details.pollMs,
        attempt: failures,
        status: details.status,
        error_name: details.name,
        error_message: details.message,
        retry_ms: retryMs,
      }), "terminal");
      await sleep(retryMs, dependencies.signal);
    }
  }
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolveMonitorOptions(env);
  const config = resolveObservabilityConfig(env);
  const logger = createObservabilityLogger(config, "ghostprov-v2-monitor");
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    await runMonitor(options, { logger, signal: controller.signal });
  } finally {
    process.off("SIGINT", stop);
    process.off("SIGTERM", stop);
    await logger.close();
  }
}

if (import.meta.main) await main();
