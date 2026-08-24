import "dotenv/config";
import * as path from "node:path";
import { GhostProvHostSupervisor } from "../src/host/supervisor";

function integer(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 1) throw new TypeError(`${name} must be a positive integer`);
  return parsed;
}

export function supervisorOptions(env: NodeJS.ProcessEnv = process.env) {
  return {
    externalPort: integer(env.PORT, 18787, "PORT"),
    host: "127.0.0.1",
    cwd: process.cwd(),
    workerEntry: path.resolve(env.GP_SUPERVISOR_WORKER_ENTRY ?? "src/server.ts"),
    readyTimeoutMs: integer(env.GP_SUPERVISOR_READY_TIMEOUT_MS, 120_000, "GP_SUPERVISOR_READY_TIMEOUT_MS"),
    gracefulShutdownMs: integer(env.GP_SUPERVISOR_GRACE_MS, 330_000, "GP_SUPERVISOR_GRACE_MS"),
    requestWaitMs: integer(env.GP_SUPERVISOR_REQUEST_WAIT_MS, 480_000, "GP_SUPERVISOR_REQUEST_WAIT_MS"),
    controlFile: env.GP_SUPERVISOR_CONTROL_FILE ?? ".ghostprov/upgrade.request",
    stopFile: env.GP_SUPERVISOR_STOP_FILE ?? ".ghostprov/stop.request",
    statusFile: env.GP_SUPERVISOR_STATUS_FILE ?? ".ghostprov/supervisor.json",
    logFile: env.GP_SUPERVISOR_LOG_FILE ?? ".ghostprov/logs/supervisor.jsonl",
  };
}

export async function runSupervisor(env: NodeJS.ProcessEnv = process.env): Promise<void> {
  const supervisor = new GhostProvHostSupervisor(supervisorOptions(env));
  await supervisor.start();
  const snapshot = supervisor.snapshot();
  console.log(
    `ghostprov supervisor listening on http://127.0.0.1:${snapshot.externalPort} ` +
    `worker=${snapshot.workerPid} generation=${snapshot.generation}`,
  );
  let closing: Promise<void> | null = null;
  const close = () => {
    closing ??= supervisor.close();
    return closing;
  };
  process.once("SIGINT", () => void close());
  process.once("SIGTERM", () => void close());
  process.on("SIGHUP", () => void supervisor.requestRestart("SIGHUP"));
  process.once("uncaughtException", error => {
    console.error(`[ghostprov-supervisor] uncaughtException: ${error.name}`);
    void close().finally(() => { process.exitCode = 1; });
  });
  process.once("unhandledRejection", error => {
    console.error(`[ghostprov-supervisor] unhandledRejection: ${error instanceof Error ? error.name : "Error"}`);
    void close().finally(() => { process.exitCode = 1; });
  });
}

const isMain = process.argv[1]?.endsWith("supervise.ts") || process.argv[1]?.endsWith("supervise.js");
if (isMain) await runSupervisor();
