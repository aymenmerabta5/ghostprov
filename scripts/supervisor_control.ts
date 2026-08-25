import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import * as path from "node:path";
import {
  parseSupervisorReadyStatus,
  sameSupervisorControlDescriptor,
  sameSupervisorInstance,
  supervisorControlTarget,
  SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
  SUPERVISOR_CONTROL_REQUEST_SCHEMA,
  validSupervisorControlDescriptor,
  type SupervisorControlAction,
  type SupervisorControlDescriptor,
  type SupervisorControlRequest,
} from "../src/host/supervisor-control";

export {
  SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
  SUPERVISOR_CONTROL_REQUEST_SCHEMA,
  type SupervisorControlAction,
  type SupervisorControlDescriptor,
  type SupervisorControlRequest,
};

export const SUPERVISOR_CONTROL_DESCRIPTOR_FILE = ".ghostprov/supervisor-control.json";

export function supervisorControlDescriptorPath(cwd = process.cwd()): string {
  return path.resolve(cwd, SUPERVISOR_CONTROL_DESCRIPTOR_FILE);
}

export async function writeSupervisorControlDescriptor(
  descriptor: SupervisorControlDescriptor,
  cwd = process.cwd(),
): Promise<string> {
  if (!validSupervisorControlDescriptor(descriptor, path.isAbsolute)) {
    throw new TypeError("Invalid supervisor control descriptor");
  }
  const target = supervisorControlDescriptorPath(cwd);
  await mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, JSON.stringify(descriptor, null, 2), {
      encoding: "utf8",
      mode: 0o600,
    });
    await rename(temporary, target);
  } finally {
    await rm(temporary, { force: true }).catch(() => undefined);
  }
  return target;
}

export async function readSupervisorControlDescriptor(
  cwd = process.cwd(),
): Promise<SupervisorControlDescriptor | null> {
  try {
    const value: unknown = JSON.parse(await readFile(supervisorControlDescriptorPath(cwd), "utf8"));
    return validSupervisorControlDescriptor(value, path.isAbsolute) ? value : null;
  } catch {
    return null;
  }
}

export function isProcessRunning(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid < 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

export async function readRunningSupervisorControlDescriptor(
  cwd = process.cwd(),
): Promise<SupervisorControlDescriptor | null> {
  // Read twice around the process probe so a descriptor replacement cannot be
  // mistaken for the instance whose PID was just checked.
  for (let attempt = 0; attempt < 3; attempt++) {
    const before = await readSupervisorControlDescriptor(cwd);
    if (!before || !isProcessRunning(before.supervisorPid)) return null;
    const after = await readSupervisorControlDescriptor(cwd);
    if (!after || !sameSupervisorControlDescriptor(before, after)) continue;
    const status = await readFile(after.statusFile, "utf8")
      .then(parseSupervisorReadyStatus)
      .catch(() => null);
    if (!status || !sameSupervisorInstance(status, after)) return null;
    const confirmed = await readSupervisorControlDescriptor(cwd);
    if (confirmed && sameSupervisorControlDescriptor(after, confirmed)) return confirmed;
  }
  return null;
}

export async function confirmRunningSupervisorControlDescriptor(
  expected: SupervisorControlDescriptor,
  cwd = process.cwd(),
): Promise<SupervisorControlDescriptor | null> {
  const current = await readRunningSupervisorControlDescriptor(cwd);
  return current && sameSupervisorControlDescriptor(expected, current) ? current : null;
}

async function writeRequestFile(
  target: string,
  request: SupervisorControlRequest,
): Promise<void> {
  await mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${process.pid}.${request.requestId}.tmp`;
  try {
    await writeFile(temporary, JSON.stringify(request), {
      encoding: "utf8",
      mode: 0o600,
    });
    await rename(temporary, target);
  } finally {
    await rm(temporary, { force: true }).catch(() => undefined);
  }
}

/**
 * Writes only descriptor-bound requests and rechecks the descriptor after the
 * atomic rename. If the live supervisor changes during the write, retry with a
 * fresh request ID and the new target instead of claiming the stale request.
 */
export async function writeBoundSupervisorControlRequest(
  action: SupervisorControlAction,
  cwd = process.cwd(),
): Promise<{ path: string; request: SupervisorControlRequest; descriptor: SupervisorControlDescriptor }> {
  for (let attempt = 0; attempt < 20; attempt++) {
    const descriptor = await readRunningSupervisorControlDescriptor(cwd);
    if (!descriptor) {
      if (attempt === 0) {
        throw new Error("No live GhostProv supervisor control descriptor is available");
      }
      await new Promise(resolve => setTimeout(resolve, 50));
      continue;
    }
    const request: SupervisorControlRequest = {
      schema: SUPERVISOR_CONTROL_REQUEST_SCHEMA,
      requestId: randomUUID(),
      action,
      requestedAt: new Date().toISOString(),
      target: supervisorControlTarget(descriptor),
    };
    const target = action === "upgrade" ? descriptor.controlFile : descriptor.stopFile;
    await writeRequestFile(target, request);
    const confirmed = await confirmRunningSupervisorControlDescriptor(descriptor, cwd);
    if (confirmed) {
      return { path: target, request, descriptor: confirmed };
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error("GhostProv supervisor changed while writing the control request");
}
