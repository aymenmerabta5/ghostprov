import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { resolveObservabilityConfig } from "../src/observability/config";

describe("observability configuration", () => {
  it("uses bounded production defaults", () => {
    expect(resolveObservabilityConfig({}, "D:/ghostprov")).toEqual({
      level: "info",
      directory: path.resolve("D:/ghostprov", ".ghostprov/logs"),
      rotateBytes: 64 * 1024 * 1024,
      retainFiles: 5,
      queueMaxEvents: 10_000,
      queueMaxBytes: 8 * 1024 * 1024,
      flushMs: 250,
      metricsIntervalMs: 5_000,
      slowRequestMs: 10_000,
      slowFirstEventMs: 5_000,
    });
  });

  it("resolves explicit values and supports disabling logging", () => {
    const cwd = path.resolve("ghostprov-test-root");
    const logDirectory = path.resolve("ghostprov-test-logs");
    expect(resolveObservabilityConfig({
      GP_LOG_LEVEL: "off",
      GP_LOG_DIR: logDirectory,
      GP_LOG_ROTATE_BYTES: "1048576",
      GP_LOG_RETAIN_FILES: "2",
      GP_LOG_QUEUE_MAX_EVENTS: "100",
      GP_LOG_QUEUE_MAX_BYTES: "2097152",
      GP_LOG_FLUSH_MS: "50",
      GP_METRICS_INTERVAL_MS: "1000",
      GP_SLOW_REQUEST_MS: "3000",
      GP_SLOW_FIRST_EVENT_MS: "1500",
    }, cwd)).toMatchObject({
      level: "off",
      directory: logDirectory,
      rotateBytes: 1_048_576,
      retainFiles: 2,
      queueMaxEvents: 100,
      queueMaxBytes: 2_097_152,
      flushMs: 50,
      metricsIntervalMs: 1_000,
      slowRequestMs: 3_000,
      slowFirstEventMs: 1_500,
    });
  });

  it("rejects invalid levels and unsafe numeric bounds", () => {
    expect(() => resolveObservabilityConfig({ GP_LOG_LEVEL: "verbose" })).toThrow("GP_LOG_LEVEL");
    for (const [key, value] of [
      ["GP_LOG_ROTATE_BYTES", "0"],
      ["GP_LOG_RETAIN_FILES", "101"],
      ["GP_LOG_QUEUE_MAX_EVENTS", "0"],
      ["GP_LOG_QUEUE_MAX_BYTES", "1024"],
      ["GP_LOG_FLUSH_MS", "0"],
      ["GP_METRICS_INTERVAL_MS", "99"],
    ]) {
      expect(() => resolveObservabilityConfig({ [key]: value })).toThrow(key);
    }
  });
});
