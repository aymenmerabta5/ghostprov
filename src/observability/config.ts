import * as path from "node:path";

export type ObservabilityLevel = "off" | "error" | "info" | "debug";

export interface ObservabilityConfig {
  level: ObservabilityLevel;
  directory: string;
  rotateBytes: number;
  retainFiles: number;
  queueMaxEvents: number;
  queueMaxBytes: number;
  flushMs: number;
  metricsIntervalMs: number;
  slowRequestMs: number;
  slowFirstEventMs: number;
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

export function resolveObservabilityConfig(
  env: Record<string, string | undefined>,
  cwd = process.cwd(),
): ObservabilityConfig {
  const rawLevel = env.GP_LOG_LEVEL?.trim() || "info";
  if (!["off", "error", "info", "debug"].includes(rawLevel)) {
    throw new TypeError("GP_LOG_LEVEL must be one of: off, error, info, debug");
  }
  return {
    level: rawLevel as ObservabilityLevel,
    directory: path.resolve(cwd, env.GP_LOG_DIR?.trim() || ".ghostprov/logs"),
    rotateBytes: integer(env.GP_LOG_ROTATE_BYTES, 64 * 1024 * 1024, "GP_LOG_ROTATE_BYTES", 1024, 1024 ** 3),
    retainFiles: integer(env.GP_LOG_RETAIN_FILES, 5, "GP_LOG_RETAIN_FILES", 1, 100),
    queueMaxEvents: integer(env.GP_LOG_QUEUE_MAX_EVENTS, 10_000, "GP_LOG_QUEUE_MAX_EVENTS", 1, 1_000_000),
    queueMaxBytes: integer(env.GP_LOG_QUEUE_MAX_BYTES, 8 * 1024 * 1024, "GP_LOG_QUEUE_MAX_BYTES", 1024 * 1024, 1024 ** 3),
    flushMs: integer(env.GP_LOG_FLUSH_MS, 250, "GP_LOG_FLUSH_MS", 1, 60_000),
    metricsIntervalMs: integer(env.GP_METRICS_INTERVAL_MS, 5_000, "GP_METRICS_INTERVAL_MS", 100, 3_600_000),
    slowRequestMs: integer(env.GP_SLOW_REQUEST_MS, 10_000, "GP_SLOW_REQUEST_MS", 1, 3_600_000),
    slowFirstEventMs: integer(env.GP_SLOW_FIRST_EVENT_MS, 5_000, "GP_SLOW_FIRST_EVENT_MS", 1, 3_600_000),
  };
}
