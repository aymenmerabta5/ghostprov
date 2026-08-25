import "dotenv/config";
import { randomUUID } from "node:crypto";
import * as path from "node:path";
import { GhostProvHostSupervisor } from "../src/host/supervisor";
import {
  SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
  writeSupervisorControlDescriptor,
} from "./supervisor_control";

export const MAX_SUPERVISOR_LIFECYCLE_MS = 1_800_000;

function integer(
  value: string | undefined,
  fallback: number,
  name: string,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new TypeError(`${name} must be a positive integer no greater than ${maximum}`);
  }
  return parsed;
}

function port(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0 || parsed > 65_535) {
    throw new TypeError("PORT must be an integer from 0 through 65535");
  }
  return parsed;
}

export function supervisorOptions(env: NodeJS.ProcessEnv = process.env) {
  const externalPort = port(env.PORT, 18787);
  if (env.GP_CANDIDATE === "1" && externalPort === 8787) {
    throw new RangeError("Supervised candidate cannot bind protected external port 8787");
  }
  return {
    externalPort,
    host: "127.0.0.1",
    cwd: process.cwd(),
    workerEntry: path.resolve(env.GP_SUPERVISOR_WORKER_ENTRY ?? "src/server.ts"),
    // A rolling worker restart must use the exact configuration with which this
    // supervisor started.  In particular, it must not pretend that an edited
    // .env revision selector has been reloaded.  The service-level
    // `config-restart` action starts a new supervisor (and therefore a new
    // startup snapshot) when configuration needs to change.
    workerEnv: { ...env },
    readyTimeoutMs: integer(
      env.GP_SUPERVISOR_READY_TIMEOUT_MS,
      120_000,
      "GP_SUPERVISOR_READY_TIMEOUT_MS",
      MAX_SUPERVISOR_LIFECYCLE_MS,
    ),
    gracefulShutdownMs: integer(
      env.GP_SUPERVISOR_GRACE_MS,
      330_000,
      "GP_SUPERVISOR_GRACE_MS",
      MAX_SUPERVISOR_LIFECYCLE_MS,
    ),
    requestWaitMs: integer(env.GP_SUPERVISOR_REQUEST_WAIT_MS, 480_000, "GP_SUPERVISOR_REQUEST_WAIT_MS"),
    maxQueuedRequests: integer(
      env.GP_SUPERVISOR_QUEUE_MAX_REQUESTS,
      256,
      "GP_SUPERVISOR_QUEUE_MAX_REQUESTS",
    ),
    maxQueuedBytes: integer(
      env.GP_SUPERVISOR_QUEUE_MAX_BYTES,
      64 * 1024 * 1024,
      "GP_SUPERVISOR_QUEUE_MAX_BYTES",
    ),
    controlFile: env.GP_SUPERVISOR_CONTROL_FILE ?? ".ghostprov/upgrade.request",
    stopFile: env.GP_SUPERVISOR_STOP_FILE ?? ".ghostprov/stop.request",
    statusFile: env.GP_SUPERVISOR_STATUS_FILE ?? ".ghostprov/supervisor.json",
    logFile: env.GP_SUPERVISOR_LOG_FILE ?? ".ghostprov/logs/supervisor.jsonl",
  };
}

export async function runSupervisor(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const options = supervisorOptions(env);
  const startedAt = new Date().toISOString();
  const instanceNonce = randomUUID();
  const supervisor = new GhostProvHostSupervisor({ ...options, startedAt, instanceNonce });
  let closing: Promise<void> | null = null;
  let terminationRequested = false;
  const close = () => {
    closing ??= supervisor.close();
    return closing;
  };
  const terminate = () => {
    terminationRequested = true;
    void close();
  };
  process.once("SIGINT", terminate);
  process.once("SIGTERM", terminate);
  process.on("SIGHUP", () => {
    void supervisor.requestRestart("SIGHUP").catch(() => undefined);
  });
  process.once("uncaughtException", error => {
    console.error(`[ghostprov-supervisor] uncaughtException: ${error.name}`);
    void close().finally(() => { process.exitCode = 1; });
  });
  process.once("unhandledRejection", error => {
    console.error(`[ghostprov-supervisor] unhandledRejection: ${error instanceof Error ? error.name : "Error"}`);
    void close().finally(() => { process.exitCode = 1; });
  });
  try {
    // Baseline old request files before advertising the live control paths. Any
    // request written after this descriptor is published will therefore be seen
    // by the first control poll rather than mistaken for stale state.
    await supervisor.prepareControlPlane();
    if (terminationRequested) return await close();
    await writeSupervisorControlDescriptor({
      schema: SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
      supervisorPid: process.pid,
      startedAt,
      instanceNonce,
      controlFile: path.resolve(options.cwd, options.controlFile),
      stopFile: path.resolve(options.cwd, options.stopFile),
      statusFile: path.resolve(options.cwd, options.statusFile),
      readyTimeoutMs: options.readyTimeoutMs,
      gracefulShutdownMs: options.gracefulShutdownMs,
    }, options.cwd);
    if (terminationRequested) return await close();
    await supervisor.start();
    if (terminationRequested) return await close();
    const snapshot = supervisor.snapshot();
    console.log(
      `ghostprov supervisor listening on http://127.0.0.1:${snapshot.externalPort} ` +
      `worker=${snapshot.workerPid} generation=${snapshot.generation}`,
    );
  } catch (error) {
    const shutdownInProgress = terminationRequested ||
      supervisor.snapshot().state === "stopping" ||
      supervisor.snapshot().state === "stopped";
    await close();
    if (!shutdownInProgress) throw error;
  }
}

const isMain = process.argv[1]?.endsWith("supervise.ts") || process.argv[1]?.endsWith("supervise.js");
if (isMain) await runSupervisor();
