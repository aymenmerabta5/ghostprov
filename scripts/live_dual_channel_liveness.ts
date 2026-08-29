import "dotenv/config";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import path from "node:path";
import WebSocket from "ws";
import { resolveObservabilityConfig } from "../src/observability/config";
import type { ObservabilityEvent, ObservabilityPriority } from "../src/observability/events";
import {
  createObservabilityLogger,
  type LoggerLike,
} from "../src/observability/logger";
import { SandboxHeartbeat } from "../src/sandbox/heartbeat";
import { isRuntimeHealthy } from "../src/router/policy";
import { createBrowserlessSystem } from "../src/server/system";
import type {
  V2RuntimeHeartbeat,
  V2RuntimeHeartbeatCallbacks,
} from "../src/v2/runtime";

const MiB = 1024 * 1024;
const PORT = Number(process.env.GP_DUAL_CHANNEL_TEST_PORT ?? 18888);
const WAVE_COUNT = Number(process.env.GP_DUAL_CHANNEL_TEST_WAVES ?? 3);
const TEST_ROOT = path.resolve(
  process.env.GP_DUAL_CHANNEL_TEST_ROOT ??
    "C:/Users/Aymen/.ghostprov/live-debug-r5-20260827212454",
);

type CapturedHeartbeat = {
  readonly workerIndex: number;
  readonly callbacks: V2RuntimeHeartbeatCallbacks;
  readonly heartbeat: V2RuntimeHeartbeat;
};

function percentile(values: readonly number[], fraction: number): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? 0;
}

async function waitFor(
  description: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(100);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function assertPortUnused(): Promise<void> {
  try {
    const response = await fetch(`http://127.0.0.1:${PORT}/health`, {
      signal: AbortSignal.timeout(500),
    });
    await response.body?.cancel().catch(() => undefined);
    throw new Error(`Test port ${PORT} is already serving HTTP`);
  } catch (error) {
    if (error instanceof Error && error.message.includes("already serving HTTP")) throw error;
  }
}

function deterministicEntropy(length: number, seed: number): string {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  const chunks: string[] = [];
  let state = seed >>> 0;
  for (let offset = 0; offset < length; offset += 8192) {
    const count = Math.min(8192, length - offset);
    const chars = new Array<string>(count);
    for (let index = 0; index < count; index++) {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      chars[index] = alphabet[(state >>> 0) & 63]!;
    }
    chunks.push(chars.join(""));
  }
  return chunks.join("");
}

function exactWebSocketRequest(index: number, targetBytes: number): string {
  const streamId = `dual-channel-${String(index).padStart(2, "0")}`;
  const base = {
    type: "response.create",
    stream_id: streamId,
    model: process.env.GP_LIVE_MODEL ?? "gpt-5.6-sol",
    store: false,
    stream: true,
    service_tier: "fast",
    max_output_tokens: 16,
    input: "Reply with OK.",
    x_ghostprov_transport_probe: "",
  };
  const fixedBytes = Buffer.byteLength(JSON.stringify(base));
  if (targetBytes <= fixedBytes) throw new Error("Target request is smaller than its envelope");
  const encoded = JSON.stringify({
    ...base,
    x_ghostprov_transport_probe: deterministicEntropy(
      targetBytes - fixedBytes,
      0x6d2b79f5 ^ index,
    ),
  });
  if (Buffer.byteLength(encoded) !== targetBytes) {
    throw new Error(`Exact request ${index} drifted from ${targetBytes} bytes`);
  }
  return encoded;
}

async function runWebSocketRequest(
  index: number,
  targetBytes: number,
  apiKey: string,
): Promise<{ totalMs: number; terminal: string; code: string | null }> {
  const encoded = exactWebSocketRequest(index, targetBytes);
  const started = performance.now();
  const socket = new WebSocket(`ws://127.0.0.1:${PORT}/v1/responses`, {
    headers: { authorization: `Bearer ${apiKey}` },
  });
  return await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (
      action: () => void,
    ) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      socket.close();
      action();
    };
    const timeout = setTimeout(() => {
      socket.terminate();
      finish(() => reject(new Error(`Live request ${index} exceeded 60 seconds`)));
    }, 60_000);
    socket.once("open", () => socket.send(encoded));
    socket.on("message", raw => {
      let event: Record<string, any>;
      try {
        event = JSON.parse(raw.toString()) as Record<string, any>;
      } catch {
        return;
      }
      if (!["error", "response.failed", "response.incomplete", "response.completed"].includes(event.type)) {
        return;
      }
      finish(() => resolve({
        totalMs: performance.now() - started,
        terminal: String(event.type),
        code: typeof event.error?.code === "string" ? event.error.code : null,
      }));
    });
    socket.once("error", error => finish(() => reject(error)));
    socket.once("close", () => {
      if (!settled) finish(() => reject(new Error(`Live request ${index} closed without a terminal`)));
    });
  });
}

export async function main(): Promise<void> {
  if (process.env.GP_LIVE_DUAL_CHANNEL_TEST !== "1") {
    throw new Error("GP_LIVE_DUAL_CHANNEL_TEST=1 is required");
  }
  if (!Number.isSafeInteger(PORT) || PORT !== 18888 || PORT === 18787) {
    throw new Error("The dual-channel live proof is hard-fenced to test port 18888");
  }
  if (!Number.isSafeInteger(WAVE_COUNT) || WAVE_COUNT < 1 || WAVE_COUNT > 20) {
    throw new Error("GP_DUAL_CHANNEL_TEST_WAVES must be an integer from 1 through 20");
  }
  await assertPortUnused();
  const manifestFile = path.join(TEST_ROOT, "fleet.json");
  const cookieFile = path.join(TEST_ROOT, "account-cookies.json");
  const logDirectory = path.join(TEST_ROOT, "test_logs");
  const manifest = JSON.parse(await readFile(manifestFile, "utf8")) as {
    workers?: Array<{ projectId?: unknown; index?: unknown }>;
  };
  const workers = [...(manifest.workers ?? [])]
    .sort((left, right) => Number(left.index) - Number(right.index));
  const projectIds = workers.map(worker => String(worker.projectId ?? ""));
  if (
    projectIds.length !== 8 ||
    new Set(projectIds).size !== 8 ||
    projectIds.some(value => !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value))
  ) throw new Error("The isolated test manifest must contain eight unique project UUIDs");
  const apiKey = process.env.GHOSTPROV_API_KEY;
  if (!apiKey) throw new Error("GHOSTPROV_API_KEY is required");
  await mkdir(logDirectory, { recursive: true });

  const env: Record<string, string | undefined> = {
    ...process.env,
    PORT: String(PORT),
    GP_CANDIDATE: "1",
    GP_AUTO_CREATE_PROJECTS: "0",
    GP_MAX_PROJECTS: "8",
    GP_PROVISION_CONCURRENCY: "4",
    GP_V2_SIDECAR_REVISION: "r8",
    GP_V2_DIAGNOSTICS: "1",
    GP_V2_DIAGNOSTICS_INTERVAL_MS: "1000",
    GP_METRICS_INTERVAL_MS: "1000",
    PRISM_COOKIE_FILE: cookieFile,
    GP_FLEET_MANIFEST_FILE: manifestFile,
    GP_LOG_DIR: logDirectory,
    GP_V2_DIAGNOSTIC_ROOT: path.join(logDirectory, "diagnostics"),
    PRISM_PROJECT_IDS: projectIds.join(","),
  };

  const captured = new Map<string, CapturedHeartbeat>();
  const observedEvents: ObservabilityEvent[] = [];
  const baseLogger = createObservabilityLogger(resolveObservabilityConfig(env));
  const logger: LoggerLike = {
    emit(event: ObservabilityEvent, priority?: ObservabilityPriority) {
      if (
        event.event.startsWith("sandbox.recovery.") ||
        event.event === "request.failed" ||
        event.event === "fleet.sample"
      ) observedEvents.push(event);
      baseLogger.emit(event, priority);
    },
    snapshot: () => baseLogger.snapshot(),
    close: deadlineMs => baseLogger.close(deadlineMs),
  };

  const system = await createBrowserlessSystem(env, {
    logger,
    heartbeatFactory: ({ projectId, workerIndex, gateway, callbacks }) => {
      const heartbeat = new SandboxHeartbeat({
        projectId,
        gateway,
        onSuccess: callbacks.onSuccess,
        onAuthenticationFailure: callbacks.onAuthenticationFailure,
        onExpired: callbacks.onExpired,
        onTransientFailure: callbacks.onTransientFailure,
        onUnreachable: callbacks.onUnreachable,
      });
      const key = String(projectId);
      const wrapped: V2RuntimeHeartbeat = {
        start: () => heartbeat.start(),
        stop: async () => {
          if (captured.get(key)?.heartbeat === wrapped) captured.delete(key);
          await heartbeat.stop();
        },
        snapshot: () => heartbeat.snapshot(),
      };
      captured.set(key, { workerIndex, callbacks, heartbeat: wrapped });
      return wrapped;
    },
  });

  const primarySamples: Array<{
    ready: number;
    healthy: number;
    routable: number;
    capacity: number;
    active: number;
    queued: number;
    unavailable: number;
  }> = [];
  let monitoring = false;
  let monitor: Promise<void> | null = null;
  try {
    await system.start();
    await waitFor("eight live R8 runtimes", () => {
      const snapshots = system.registry.snapshots();
      return captured.size === 8 && snapshots.length === 8 && snapshots.every(snapshot =>
        snapshot.state === "ready" &&
        snapshot.protocolRevision === "r2" &&
        snapshot.releaseRevision === "r8" &&
        snapshot.transportConnected === true &&
        isRuntimeHealthy(snapshot)
      );
    }, 240_000);
    const health = await fetch(`http://127.0.0.1:${PORT}/health`, {
      signal: AbortSignal.timeout(10_000),
    });
    if (!health.ok) throw new Error(`Test server health returned ${health.status}`);
    await health.body?.cancel();

    const baselineIncarnations = new Map(
      system.registry.snapshots().map(snapshot => [String(snapshot.projectId), snapshot.incarnation ?? 0]),
    );
    const recoveryStartsBefore = observedEvents.filter(
      event => event.event === "sandbox.recovery.started",
    ).length;
    monitoring = true;
    monitor = (async () => {
      while (monitoring) {
        const snapshots = system.registry.snapshots();
        const router = system.router.snapshot();
        primarySamples.push({
          ready: snapshots.filter(snapshot => snapshot.state === "ready").length,
          healthy: snapshots.filter(isRuntimeHealthy).length,
          routable: snapshots.filter(snapshot => snapshot.state === "ready" && snapshot.available > 0).length,
          capacity: router.capacity,
          active: router.active,
          queued: router.queued,
          unavailable: system.fleet.snapshot().unavailable,
        });
        await Bun.sleep(50);
      }
    })();

    // Exact 14-agent distribution captured by the original live R6 proof:
    // six tiny, four medium, and four incompressible observed-size requests.
    const observed = Math.floor(2.9 * MiB);
    const requestSizes = [
      128 * 1024, 128 * 1024, MiB, observed,
      128 * 1024, observed, MiB, 128 * 1024,
      observed, MiB, 128 * 1024, observed, MiB, 128 * 1024,
    ];
    const results: Array<{ totalMs: number; terminal: string; code: string | null }> = [];
    const waveLatencies: Array<{ p50: number; p95: number; max: number }> = [];
    for (let wave = 0; wave < WAVE_COUNT; wave++) {
      if (wave > 0) {
        await waitFor(`idle fleet before wave ${wave + 1}`, () => {
          const router = system.router.snapshot();
          return router.active === 0 && router.queued === 0 &&
            system.registry.snapshots().every(isRuntimeHealthy);
        }, 30_000);
      }
      const requests = requestSizes.map((bytes, index) => runWebSocketRequest(
        wave * requestSizes.length + index,
        bytes,
        apiKey,
      ));
      if (wave === 0) {
        await waitFor("live request admission", () => {
          const snapshot = system.router.snapshot();
          return snapshot.active > 0 || snapshot.queued > 0;
        }, 10_000);

        for (const [project, controller] of [...captured.entries()]
          .sort((left, right) => left[1].workerIndex - right[1].workerIndex)) {
          const snapshot = system.registry.snapshots().find(value => String(value.projectId) === project);
          if (
            !snapshot ||
            snapshot.state !== "ready" ||
            snapshot.transportConnected !== true ||
            snapshot.sidecarPeerAgeMs === null ||
            snapshot.sidecarPeerAgeMs === undefined ||
            snapshot.sidecarPeerAgeMs > 20_000
          ) throw new Error(`Worker ${controller.workerIndex} lacked fresh exact-sidecar proof`);
          controller.callbacks.onTransientFailure(503);
          controller.callbacks.onTransientFailure(503);
          controller.callbacks.onTransientFailure(503);
          controller.callbacks.onUnreachable(503);
          const after = system.registry.snapshots().find(value => String(value.projectId) === project);
          if (after?.state !== "ready" || after.transportConnected !== true) {
            throw new Error(`Worker ${controller.workerIndex} was falsely evicted`);
          }
          await Bun.sleep(200);
        }
      }

      const waveResults = await Promise.all(requests);
      const unexpected = waveResults.filter(result => (
        result.terminal !== "error" || result.code !== "unknown_parameter"
      ));
      if (unexpected.length > 0) {
        throw new Error(`${unexpected.length} live requests returned an unexpected terminal in wave ${wave + 1}`);
      }
      results.push(...waveResults);
      waveLatencies.push({
        p50: percentile(waveResults.map(result => result.totalMs), 0.50),
        p95: percentile(waveResults.map(result => result.totalMs), 0.95),
        max: Math.max(...waveResults.map(result => result.totalMs)),
      });
    }
    await Bun.sleep(5_000);
    monitoring = false;
    await monitor;
    monitor = null;

    const recoveryStartsAfterPrimary = observedEvents.filter(
      event => event.event === "sandbox.recovery.started",
    ).length;
    if (recoveryStartsAfterPrimary !== recoveryStartsBefore) {
      throw new Error("A heartbeat-only outage started sandbox recovery");
    }
    const primaryFinal = system.registry.snapshots();
    if (
      primaryFinal.some(snapshot =>
        snapshot.state !== "ready" ||
        !isRuntimeHealthy(snapshot) ||
        (snapshot.incarnation ?? 0) !== baselineIncarnations.get(String(snapshot.projectId))
      )
    ) throw new Error("The heartbeat-only outage changed a sandbox generation");
    const capacityTimeouts = observedEvents.filter(event =>
      event.event === "request.failed" &&
      ((event as Record<string, unknown>).error_name === "RouterCapacityTimeoutError" ||
        (event as Record<string, unknown>).error_kind === "router_capacity_timeout")
    ).length;
    if (capacityTimeouts !== 0) throw new Error("Live heartbeat-only wave hit router capacity timeout");
    const minimumReady = Math.min(...primarySamples.map(sample => sample.ready));
    const minimumHealthy = Math.min(...primarySamples.map(sample => sample.healthy));
    const minimumCapacity = Math.min(...primarySamples.map(sample => sample.capacity));
    if (minimumReady !== 8 || minimumHealthy !== 8 || minimumCapacity <= 0) {
      throw new Error("The live heartbeat-only wave lost fleet health or capacity");
    }

    const controlEntry = [...captured.entries()].sort(
      (left, right) => left[1].workerIndex - right[1].workerIndex,
    )[0];
    if (!controlEntry) throw new Error("No heartbeat control was captured");
    const [controlProject, control] = controlEntry;
    const targetedControlRecoveries = () => observedEvents.filter(event =>
      event.event === "sandbox.recovery.started" &&
      String((event as Record<string, unknown>).project_id ?? "") === controlProject &&
      (event as Record<string, unknown>).status === 409 &&
      (event as Record<string, unknown>).error_kind === "heartbeat_expired"
    ).length;
    control.callbacks.onExpired(409);
    await waitFor("one fail-closed control recovery", () => (
      targetedControlRecoveries() === 1
    ), 10_000);
    await waitFor("control runtime recovery completion", () => {
      const snapshots = system.registry.snapshots();
      return snapshots.length === 8 && snapshots.every(snapshot =>
        snapshot.state === "ready" && isRuntimeHealthy(snapshot)
      );
    }, 240_000);
    const totalRecoveryStarts = observedEvents.filter(
      event => event.event === "sandbox.recovery.started",
    ).length;
    if (targetedControlRecoveries() !== 1) {
      throw new Error("The fail-closed control did not retain one fenced recovery session");
    }
    const falseHeartbeatRecoveries = observedEvents.filter(event =>
      event.event === "sandbox.recovery.started" &&
      (event as Record<string, unknown>).error_kind === "heartbeat_unreachable"
    ).length;
    if (falseHeartbeatRecoveries !== 0) {
      throw new Error("A heartbeat-only outage still started recovery");
    }

    const report = {
      schema: 1,
      test: "dual-channel-live-liveness",
      port: PORT,
      runtimes: 8,
      waves: WAVE_COUNT,
      requests: results.length,
      requestBytes: requestSizes,
      waveLatenciesMs: waveLatencies,
      terminals: results.reduce<Record<string, number>>((counts, result) => {
        const key = `${result.terminal}:${result.code ?? "none"}`;
        counts[key] = (counts[key] ?? 0) + 1;
        return counts;
      }, {}),
      latencyMs: {
        p50: percentile(results.map(result => result.totalMs), 0.50),
        p95: percentile(results.map(result => result.totalMs), 0.95),
        max: Math.max(...results.map(result => result.totalMs)),
      },
      primary: {
        heartbeatUnreachableInjections: 8,
        recoveryStarts: recoveryStartsAfterPrimary - recoveryStartsBefore,
        capacityTimeouts,
        minimumReady,
        minimumHealthy,
        minimumRoutable: Math.min(...primarySamples.map(sample => sample.routable)),
        minimumCapacity,
        minimumAvailableFleetProjects: 8 - Math.max(
          ...primarySamples.map(sample => sample.unavailable),
        ),
        peakActive: Math.max(...primarySamples.map(sample => sample.active)),
        peakQueued: Math.max(...primarySamples.map(sample => sample.queued)),
        unchangedIncarnations: true,
      },
      failClosedControl: {
        status: 409,
        targetedRecoveryStarts: targetedControlRecoveries(),
        additionalIndependentRecoveryStarts:
          totalRecoveryStarts - recoveryStartsAfterPrimary - targetedControlRecoveries(),
        falseHeartbeatRecoveryStarts: falseHeartbeatRecoveries,
        recoveredToEightHealthy: true,
      },
      completedAt: new Date().toISOString(),
    };
    const target = path.join(logDirectory, `dual-channel-live-${Date.now()}.json`);
    const temporary = `${target}.${process.pid}.tmp`;
    await writeFile(temporary, `${JSON.stringify(report, null, 2)}\n`);
    await rename(temporary, target);
    console.log(JSON.stringify({ report: target, summary: report }, null, 2));
  } finally {
    monitoring = false;
    await monitor?.catch(() => undefined);
    await system.close();
  }
}

if (import.meta.main) await main();
