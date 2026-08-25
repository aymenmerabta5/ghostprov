export const SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA = 2 as const;
export const SUPERVISOR_CONTROL_REQUEST_SCHEMA = 1 as const;
export const SUPERVISOR_STATUS_SCHEMA = 1 as const;

export type SupervisorControlAction = "upgrade" | "shutdown";

export interface SupervisorControlTarget {
  readonly supervisorPid: number;
  readonly startedAt: string;
  readonly instanceNonce: string;
}

export interface SupervisorControlDescriptor extends SupervisorControlTarget {
  readonly schema: typeof SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA;
  readonly controlFile: string;
  readonly stopFile: string;
  readonly statusFile: string;
  readonly readyTimeoutMs: number;
  readonly gracefulShutdownMs: number;
}

export interface SupervisorControlRequest {
  readonly schema: typeof SUPERVISOR_CONTROL_REQUEST_SCHEMA;
  readonly requestId: string;
  readonly action: SupervisorControlAction;
  readonly requestedAt: string;
  readonly target: SupervisorControlTarget;
}

export interface SupervisorReadyStatus extends SupervisorControlTarget {
  readonly schema: typeof SUPERVISOR_STATUS_SCHEMA;
  readonly state: string;
  readonly workerPid: number | null;
  readonly lastReadyAt: string | null;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}

function positiveInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && Number(value) > 0;
}

function validIso(value: unknown): value is string {
  return typeof value === "string" && Number.isFinite(Date.parse(value));
}

function validNonce(value: unknown): value is string {
  return typeof value === "string" && UUID.test(value);
}

export function validSupervisorControlTarget(value: unknown): value is SupervisorControlTarget {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const raw = value as Record<string, unknown>;
  return exactKeys(raw, ["supervisorPid", "startedAt", "instanceNonce"]) &&
    positiveInteger(raw.supervisorPid) &&
    validIso(raw.startedAt) &&
    validNonce(raw.instanceNonce);
}

export function validSupervisorControlDescriptor(
  value: unknown,
  isAbsolutePath: (value: string) => boolean,
): value is SupervisorControlDescriptor {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const raw = value as Record<string, unknown>;
  if (!exactKeys(raw, [
    "schema",
    "supervisorPid",
    "startedAt",
    "instanceNonce",
    "controlFile",
    "stopFile",
    "statusFile",
    "readyTimeoutMs",
    "gracefulShutdownMs",
  ])) return false;
  return raw.schema === SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA &&
    positiveInteger(raw.supervisorPid) &&
    validIso(raw.startedAt) &&
    validNonce(raw.instanceNonce) &&
    typeof raw.controlFile === "string" && isAbsolutePath(raw.controlFile) &&
    typeof raw.stopFile === "string" && isAbsolutePath(raw.stopFile) &&
    typeof raw.statusFile === "string" && isAbsolutePath(raw.statusFile) &&
    positiveInteger(raw.readyTimeoutMs) &&
    positiveInteger(raw.gracefulShutdownMs);
}

export function supervisorControlTarget(
  descriptor: SupervisorControlDescriptor,
): SupervisorControlTarget {
  return Object.freeze({
    supervisorPid: descriptor.supervisorPid,
    startedAt: descriptor.startedAt,
    instanceNonce: descriptor.instanceNonce,
  });
}

export function sameSupervisorInstance(
  left: SupervisorControlTarget,
  right: SupervisorControlTarget,
): boolean {
  return left.supervisorPid === right.supervisorPid &&
    left.startedAt === right.startedAt &&
    left.instanceNonce === right.instanceNonce;
}

export function sameSupervisorControlDescriptor(
  left: SupervisorControlDescriptor,
  right: SupervisorControlDescriptor,
): boolean {
  return sameSupervisorInstance(left, right) &&
    left.schema === right.schema &&
    left.controlFile === right.controlFile &&
    left.stopFile === right.stopFile &&
    left.statusFile === right.statusFile &&
    left.readyTimeoutMs === right.readyTimeoutMs &&
    left.gracefulShutdownMs === right.gracefulShutdownMs;
}

export function parseSupervisorControlRequest(value: string): SupervisorControlRequest | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const raw = parsed as Record<string, unknown>;
  if (!exactKeys(raw, ["schema", "requestId", "action", "requestedAt", "target"])) return null;
  if (
    raw.schema !== SUPERVISOR_CONTROL_REQUEST_SCHEMA ||
    !validNonce(raw.requestId) ||
    (raw.action !== "upgrade" && raw.action !== "shutdown") ||
    !validIso(raw.requestedAt) ||
    !raw.target ||
    typeof raw.target !== "object" ||
    Array.isArray(raw.target)
  ) return null;
  if (!validSupervisorControlTarget(raw.target)) return null;
  const target = raw.target;
  return {
    schema: SUPERVISOR_CONTROL_REQUEST_SCHEMA,
    requestId: raw.requestId,
    action: raw.action,
    requestedAt: raw.requestedAt,
    target: {
      supervisorPid: target.supervisorPid,
      startedAt: target.startedAt,
      instanceNonce: target.instanceNonce,
    },
  };
}

export function requestTargetsSupervisor(
  request: SupervisorControlRequest,
  action: SupervisorControlAction,
  target: SupervisorControlTarget,
): boolean {
  return request.action === action && sameSupervisorInstance(request.target, target);
}

export function parseSupervisorReadyStatus(value: string): SupervisorReadyStatus | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return null;
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
  const raw = parsed as Record<string, unknown>;
  if (
    raw.schema !== SUPERVISOR_STATUS_SCHEMA ||
    typeof raw.state !== "string" ||
    !positiveInteger(raw.supervisorPid) ||
    !validIso(raw.startedAt) ||
    !validNonce(raw.instanceNonce) ||
    !(raw.workerPid === null || positiveInteger(raw.workerPid)) ||
    !(raw.lastReadyAt === null || validIso(raw.lastReadyAt))
  ) return null;
  return {
    schema: SUPERVISOR_STATUS_SCHEMA,
    state: raw.state,
    supervisorPid: raw.supervisorPid,
    startedAt: raw.startedAt,
    instanceNonce: raw.instanceNonce,
    workerPid: raw.workerPid,
    lastReadyAt: raw.lastReadyAt,
  };
}

export function statusMatchesReadySupervisor(
  status: SupervisorReadyStatus,
  descriptor: SupervisorControlDescriptor,
): boolean {
  return status.state === "ready" &&
    status.workerPid !== null &&
    status.lastReadyAt !== null &&
    sameSupervisorInstance(status, descriptor) &&
    Date.parse(status.lastReadyAt) >= Date.parse(descriptor.startedAt);
}
