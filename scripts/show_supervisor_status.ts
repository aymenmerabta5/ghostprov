import "dotenv/config";
import { readFile } from "node:fs/promises";
import {
  parseSupervisorReadyStatus,
  sameSupervisorInstance,
  statusMatchesReadySupervisor,
  type SupervisorReadyStatus,
} from "../src/host/supervisor-control";
import {
  readRunningSupervisorControlDescriptor,
  type SupervisorControlDescriptor,
} from "./supervisor_control";

const MAX_STATUS_WAIT_MS = 1_830_000;

async function readBoundStatus(
  descriptor: SupervisorControlDescriptor,
): Promise<{ raw: string; status: SupervisorReadyStatus } | null> {
  let raw: string;
  try {
    raw = await readFile(descriptor.statusFile, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
  const status = parseSupervisorReadyStatus(raw);
  if (!status || !sameSupervisorInstance(status, descriptor)) return null;
  return { raw, status };
}

export async function readSupervisorStatus(
  _env: NodeJS.ProcessEnv = process.env,
  cwd = process.cwd(),
): Promise<string | null> {
  const descriptor = await readRunningSupervisorControlDescriptor(cwd);
  if (!descriptor) return null;
  const bound = await readBoundStatus(descriptor);
  if (!bound) return null;
  const confirmed = await readRunningSupervisorControlDescriptor(cwd);
  return confirmed && sameSupervisorInstance(descriptor, confirmed) ? bound.raw : null;
}

export interface WaitForFreshSupervisorReadyOptions {
  readonly notBefore: string | Date;
  readonly previousInstanceNonce?: string;
  readonly timeoutMs?: number;
  readonly pollMs?: number;
  readonly cwd?: string;
}

export async function waitForFreshSupervisorReady(
  options: WaitForFreshSupervisorReadyOptions,
): Promise<{ descriptor: SupervisorControlDescriptor; status: SupervisorReadyStatus }> {
  const notBeforeMs = options.notBefore instanceof Date
    ? options.notBefore.getTime()
    : Date.parse(options.notBefore);
  const timeoutMs = options.timeoutMs ?? MAX_STATUS_WAIT_MS;
  const pollMs = options.pollMs ?? 250;
  if (!Number.isFinite(notBeforeMs)) throw new TypeError("notBefore must be a valid date");
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_STATUS_WAIT_MS) {
    throw new RangeError(`timeoutMs must be 1 through ${MAX_STATUS_WAIT_MS}`);
  }
  if (!Number.isSafeInteger(pollMs) || pollMs < 10 || pollMs > 5_000) {
    throw new RangeError("pollMs must be 10 through 5000");
  }
  const deadline = Date.now() + timeoutMs;
  let readyDeadline = deadline;
  let selectedNonce: string | null = null;
  while (Date.now() < Math.min(deadline, readyDeadline)) {
    const descriptor = await readRunningSupervisorControlDescriptor(options.cwd);
    if (
      descriptor &&
      Date.parse(descriptor.startedAt) >= notBeforeMs &&
      descriptor.instanceNonce !== options.previousInstanceNonce
    ) {
      if (selectedNonce !== descriptor.instanceNonce) {
        selectedNonce = descriptor.instanceNonce;
        readyDeadline = Math.min(deadline, Date.now() + descriptor.readyTimeoutMs + 30_000);
      }
      const bound = await readBoundStatus(descriptor);
      if (bound && statusMatchesReadySupervisor(bound.status, descriptor)) {
        const confirmed = await readRunningSupervisorControlDescriptor(options.cwd);
        if (confirmed && sameSupervisorInstance(descriptor, confirmed)) {
          return { descriptor: confirmed, status: bound.status };
        }
      }
    }
    await Bun.sleep(Math.min(pollMs, Math.max(1, Math.min(deadline, readyDeadline) - Date.now())));
  }
  throw new Error("Fresh GhostProv supervisor did not publish matching ready status before timeout");
}

const isMain = process.argv[1]?.endsWith("show_supervisor_status.ts") ||
  process.argv[1]?.endsWith("show_supervisor_status.js");
if (isMain) {
  const mode = process.argv[2] ?? "status";
  if (mode === "status") {
    const status = await readSupervisorStatus();
    if (status === null) {
      console.error("No live descriptor-bound GhostProv supervisor status is available");
      process.exitCode = 1;
    } else {
      console.log(status);
    }
  } else if (mode === "descriptor") {
    const descriptor = await readRunningSupervisorControlDescriptor();
    if (!descriptor) {
      console.error("No live GhostProv supervisor control descriptor is available");
      process.exitCode = 1;
    } else {
      console.log(JSON.stringify(descriptor));
    }
  } else if (mode === "wait-ready") {
    const notBefore = process.argv[3];
    if (!notBefore) throw new TypeError("wait-ready requires notBefore ISO time");
    const previousInstanceNonce = process.argv[4] && process.argv[4] !== "-"
      ? process.argv[4]
      : undefined;
    const timeoutMs = process.argv[5] ? Number(process.argv[5]) : undefined;
    const result = await waitForFreshSupervisorReady({
      notBefore,
      previousInstanceNonce,
      timeoutMs,
    });
    console.log(JSON.stringify(result.status));
  } else {
    throw new TypeError(`Unknown supervisor status mode: ${mode}`);
  }
}
