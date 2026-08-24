import "dotenv/config";
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
import { sanitizeDiagnosticMessage } from "../src/observability/redaction";

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

export interface MonitorOptions {
  root: string;
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
  };
}

function rateLimitMetrics(value: unknown, queuedValue: unknown): JsonValue {
  if (!isRecord(value)) return null;
  const queued = isRecord(queuedValue) ? queuedValue : {};
  const output: Record<string, JsonValue> = {};
  for (const [scope, candidate] of Object.entries(value).slice(0, 64)) {
    if (!/^[a-z0-9][a-z0-9._:-]{0,127}$/i.test(scope) || !isRecord(candidate)) continue;
    output[scope] = {
      state: typeof candidate.state === "string" ? candidate.state.slice(0, 16) : "unknown",
      retry_after_ms: nullableMetric(candidate.retryAfterMs),
      budget_limit: nullableMetric(candidate.tokenLimit),
      budget_remaining: nullableMetric(candidate.remainingTokens),
      active: metric(candidate.active),
      queued: metric(queued[scope]),
      cold_max_active: metric(candidate.coldMaxActive),
    };
  }
  return output;
}

function resourceMetrics(value: unknown): JsonValue {
  const resources = isRecord(value) ? value : {};
  return {
    reserved_bytes: metric(resources.reservedBytes),
    reservation_capacity_bytes: metric(resources.reservationCapacityBytes),
    memory_paused: resources.paused === true,
    governor_queued: metric(resources.queued),
    cpu_active: metric(resources.cpuActive),
    cpu_queued: metric(resources.cpuQueued),
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
): Promise<{ status: number; value: unknown }> {
  let response: Response;
  try {
    response = await fetchImpl(new URL(path, root), {
      method: "GET",
      signal,
      redirect: "error",
      credentials: "omit",
      cache: "no-store",
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
    return { status: response.status, value: await response.json() };
  } catch (error) {
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
      getJson(options.root, "/health", scope.signal, scope.timedOut, fetchImpl),
      getJson(options.root, "/__sandboxes", scope.signal, scope.timedOut, fetchImpl),
    ]);
    if (!isRecord(health.value) || !isRecord(sandboxes.value)) {
      throw new MonitorPollError(
        "invalid_response",
        "Monitor endpoints returned an invalid JSON object",
      );
    }
    const runtimes = Array.isArray(sandboxes.value.runtimes)
      ? sandboxes.value.runtimes.slice(0, 4_096)
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
      rate_limit: rateLimitMetrics(
        sandboxes.value.rateLimit,
        sandboxes.value.rateLimitQueued,
      ),
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
