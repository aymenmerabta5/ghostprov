import * as path from "node:path";
import type { MachineSample } from "../core/ports";
import { projectId, type ProjectId } from "../sandbox/types";

export type TransportMode = "v1" | "v2" | "auto";
export type PrismSessionMode = "authenticated" | "auto" | "anonymous";
export type ProjectReuseMode = "managed" | "eligible-owned" | "explicit";

export interface MultiSandboxConfig {
  projectIds: readonly ProjectId[];
  prismOrigin: string;
  cookieFile: string;
  initialCookieHeader?: string;
  port: number;
  candidate: boolean;
  heartbeatMs: 10_000;
  routerQueueTimeoutMs: number;
  maxProjects: number;
  slotsPerProject: number;
  machine: MachineSample;
  transport: TransportMode;
  sessionMode: PrismSessionMode;
  allowAnonymous: boolean;
  guestMaxProjects: number;
  autoCreateProjects: boolean;
  projectReuse: ProjectReuseMode;
  targetConcurrency: number;
  targetSandboxes: number | null;
  v2SandboxCapacity: number;
  v2InitialActivePerSandbox: number;
  v2MaxActivePerSandbox: number;
  v2ResponseMaxBytes: number;
  v2TransportReservationBytes: number;
  v2YSweetRefreshMs: number;
  v2SidecarMemoryBudgetBytes: number;
  v2DiagnosticsEnabled: boolean;
  v2DiagnosticsIntervalMs: number;
  v2BatchingEnabled: boolean;
  desiredSandboxes: number;
  provisionConcurrency: number;
  fleetManifestFile: string;
}

function positiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) throw new TypeError(`${name} must be a positive integer`);
  return parsed;
}

function optionalPositiveInteger(value: string | undefined, name: string): number | null {
  if (value === undefined || value.trim() === "") return null;
  return positiveInteger(value, 1, name);
}

function boundedPositiveInteger(value: string | undefined, fallback: number, name: string, minimum: number, maximum: number): number {
  const parsed = positiveInteger(value, fallback, name);
  if (parsed < minimum || parsed > maximum) throw new RangeError(`${name} must be from ${minimum} through ${maximum}`);
  return parsed;
}

function enumValue<T extends string>(
  value: string | undefined,
  fallback: T,
  allowed: readonly T[],
  name: string,
): T {
  if (value === undefined || value.trim() === "") return fallback;
  const normalized = value.trim() as T;
  if (!allowed.includes(normalized)) {
    throw new TypeError(`${name} must be one of: ${allowed.join(", ")}`);
  }
  return normalized;
}

function binaryFlag(value: string | undefined, name: string): boolean {
  if (value === undefined || value.trim() === "" || value === "0") return false;
  if (value === "1") return true;
  throw new TypeError(`${name} must be 0 or 1`);
}

export function resolveDesiredSandboxCount(input: {
  targetConcurrency: number;
  targetSandboxes: number | null;
  v2SandboxCapacity: number;
  maxProjects: number;
  sessionMode: PrismSessionMode;
  guestMaxProjects: number;
}): number {
  const requested = input.targetSandboxes ?? Math.ceil(
    input.targetConcurrency / input.v2SandboxCapacity,
  );
  const identityCeiling = input.sessionMode === "anonymous"
    ? Math.min(input.maxProjects, input.guestMaxProjects)
    : input.maxProjects;
  return Math.max(1, Math.min(requested, identityCeiling));
}

export function resolveMultiSandboxConfig(
  env: Record<string, string | undefined>,
  machine: MachineSample,
): MultiSandboxConfig {
  const transport = enumValue<TransportMode>(
    env.GP_TRANSPORT,
    "auto",
    ["v1", "v2", "auto"],
    "GP_TRANSPORT",
  );
  const sessionMode = enumValue<PrismSessionMode>(
    env.GP_PRISM_SESSION,
    "authenticated",
    ["authenticated", "auto", "anonymous"],
    "GP_PRISM_SESSION",
  );
  if (sessionMode === "anonymous" && transport !== "v2") {
    throw new TypeError("anonymous Prism sessions require GP_TRANSPORT=v2");
  }

  const allowAnonymous = binaryFlag(env.GP_ALLOW_ANONYMOUS, "GP_ALLOW_ANONYMOUS");
  const autoCreateProjects = binaryFlag(
    env.GP_AUTO_CREATE_PROJECTS,
    "GP_AUTO_CREATE_PROJECTS",
  );
  const projectReuse = enumValue<ProjectReuseMode>(
    env.GP_PROJECT_REUSE,
    "managed",
    ["managed", "eligible-owned", "explicit"],
    "GP_PROJECT_REUSE",
  );

  const rawProjects = (env.PRISM_PROJECT_IDS ?? env.PRISM_DOC_ID ?? "").trim();
  if (!rawProjects && (transport === "v1" || !autoCreateProjects)) {
    throw new TypeError(
      "PRISM_PROJECT_IDS or PRISM_DOC_ID is required unless V2 automatic project creation is enabled",
    );
  }
  const projectIds = rawProjects
    ? [...new Set(rawProjects.split(",").map(value => value.trim()).filter(Boolean))]
      .map(projectId)
    : [];
  const maxProjects = positiveInteger(
    env.GP_MAX_PROJECTS,
    Math.max(32, projectIds.length),
    "GP_MAX_PROJECTS",
  );
  const guestMaxProjects = positiveInteger(
    env.GP_GUEST_MAX_PROJECTS,
    1,
    "GP_GUEST_MAX_PROJECTS",
  );
  if (guestMaxProjects > maxProjects) {
    throw new RangeError("GP_GUEST_MAX_PROJECTS must not exceed GP_MAX_PROJECTS");
  }
  const slotsPerProject = positiveInteger(env.GP_SLOTS, 30, "GP_SLOTS");
  if (slotsPerProject > 30) throw new RangeError("GP_SLOTS must not exceed the daemon's 30-lane capacity");
  if (projectIds.length > maxProjects) throw new RangeError(`Configured ${projectIds.length} projects exceeds GP_MAX_PROJECTS=${maxProjects}`);

  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  const initialCookieHeader = env.PRISM_COOKIE?.trim();
  if (!cookieFile) throw new TypeError("PRISM_COOKIE_FILE is required");
  if (sessionMode === "anonymous" && initialCookieHeader) {
    throw new TypeError("PRISM_COOKIE cannot seed an anonymous Prism session");
  }

  const candidate = env.GP_CANDIDATE === "1";
  const port = positiveInteger(env.PORT, candidate ? 18787 : 8787, "PORT");
  if (candidate && port === 8787) throw new RangeError("Candidate mode must never bind protected port 8787");

  const heartbeatMs = positiveInteger(env.GP_HEARTBEAT_MS, 10_000, "GP_HEARTBEAT_MS");
  if (heartbeatMs !== 10_000) throw new RangeError("GP_HEARTBEAT_MS must remain exactly 10000");
  const routerQueueTimeoutMs = boundedPositiveInteger(
    env.GP_ROUTER_QUEUE_TIMEOUT_MS,
    30_000,
    "GP_ROUTER_QUEUE_TIMEOUT_MS",
    1_000,
    300_000,
  );

  const targetConcurrency = positiveInteger(
    env.GP_TARGET_CONCURRENCY,
    512,
    "GP_TARGET_CONCURRENCY",
  );
  const targetSandboxes = optionalPositiveInteger(
    env.GP_TARGET_SANDBOXES,
    "GP_TARGET_SANDBOXES",
  );
  const v2SandboxCapacity = positiveInteger(
    env.GP_V2_SANDBOX_CAPACITY,
    128,
    "GP_V2_SANDBOX_CAPACITY",
  );
  if (v2SandboxCapacity > 128) {
    throw new RangeError("GP_V2_SANDBOX_CAPACITY must not exceed 128");
  }
  const v2MaxActivePerSandbox = positiveInteger(
    env.GP_V2_MAX_ACTIVE_PER_SANDBOX,
    16,
    "GP_V2_MAX_ACTIVE_PER_SANDBOX",
  );
  if (v2MaxActivePerSandbox > v2SandboxCapacity) {
    throw new RangeError("GP_V2_MAX_ACTIVE_PER_SANDBOX must not exceed physical V2 capacity");
  }
  const v2InitialActivePerSandbox = positiveInteger(
    env.GP_V2_INITIAL_ACTIVE_PER_SANDBOX,
    8,
    "GP_V2_INITIAL_ACTIVE_PER_SANDBOX",
  );
  if (v2InitialActivePerSandbox > v2MaxActivePerSandbox) {
    throw new RangeError("GP_V2_INITIAL_ACTIVE_PER_SANDBOX must not exceed GP_V2_MAX_ACTIVE_PER_SANDBOX");
  }
  const v2ResponseMaxBytes = positiveInteger(
    env.GP_V2_RESPONSE_MAX_BYTES,
    64 * 1024 * 1024,
    "GP_V2_RESPONSE_MAX_BYTES",
  );
  if (v2ResponseMaxBytes > 256 * 1024 * 1024) {
    throw new RangeError("GP_V2_RESPONSE_MAX_BYTES must not exceed 256 MiB");
  }
  const v2TransportReservationBytes = positiveInteger(
    env.GP_V2_TRANSPORT_RESERVATION_BYTES,
    16 * 1024 * 1024,
    "GP_V2_TRANSPORT_RESERVATION_BYTES",
  );
  if (v2TransportReservationBytes < 1024 * 1024) {
    throw new RangeError("GP_V2_TRANSPORT_RESERVATION_BYTES must be at least 1 MiB");
  }
  const v2YSweetRefreshMs = positiveInteger(
    env.GP_V2_YSWEET_REFRESH_MS,
    45 * 60 * 1000,
    "GP_V2_YSWEET_REFRESH_MS",
  );
  if (v2YSweetRefreshMs < 5 * 60 * 1000) {
    throw new RangeError("GP_V2_YSWEET_REFRESH_MS must be at least 5 minutes");
  }
  const v2SidecarMemoryBudgetBytes = positiveInteger(
    env.GP_V2_SIDECAR_MEMORY_BUDGET_BYTES,
    512 * 1024 * 1024,
    "GP_V2_SIDECAR_MEMORY_BUDGET_BYTES",
  );
  if (v2SidecarMemoryBudgetBytes < 64 * 1024 * 1024) {
    throw new RangeError("GP_V2_SIDECAR_MEMORY_BUDGET_BYTES must be at least 64 MiB");
  }
  const v2DiagnosticsEnabled = binaryFlag(env.GP_V2_DIAGNOSTICS, "GP_V2_DIAGNOSTICS");
  const v2BatchingEnabled = binaryFlag(env.GP_V2_BATCHING, "GP_V2_BATCHING");
  const v2DiagnosticsIntervalMs = boundedPositiveInteger(
    env.GP_V2_DIAGNOSTICS_INTERVAL_MS, 1_000, "GP_V2_DIAGNOSTICS_INTERVAL_MS", 100, 60_000,
  );
  let desiredSandboxes = resolveDesiredSandboxCount({
    targetConcurrency,
    targetSandboxes,
    v2SandboxCapacity: v2MaxActivePerSandbox,
    maxProjects,
    sessionMode,
    guestMaxProjects,
  });
  if (targetSandboxes === null && !autoCreateProjects && projectIds.length > 0) {
    desiredSandboxes = Math.min(desiredSandboxes, projectIds.length);
  }
  const provisionConcurrency = Math.min(
    desiredSandboxes,
    positiveInteger(env.GP_PROVISION_CONCURRENCY, 4, "GP_PROVISION_CONCURRENCY"),
  );

  return {
    projectIds: Object.freeze(projectIds),
    prismOrigin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    cookieFile: path.resolve(cookieFile),
    initialCookieHeader,
    port,
    candidate,
    heartbeatMs: 10_000,
    routerQueueTimeoutMs,
    maxProjects,
    slotsPerProject,
    machine,
    transport,
    sessionMode,
    allowAnonymous,
    guestMaxProjects,
    autoCreateProjects,
    projectReuse,
    targetConcurrency,
    targetSandboxes,
    v2SandboxCapacity,
    v2InitialActivePerSandbox,
    v2MaxActivePerSandbox,
    v2ResponseMaxBytes,
    v2TransportReservationBytes,
    v2YSweetRefreshMs,
    v2SidecarMemoryBudgetBytes,
    v2DiagnosticsEnabled,
    v2DiagnosticsIntervalMs,
    v2BatchingEnabled,
    desiredSandboxes,
    provisionConcurrency,
    fleetManifestFile: path.resolve(
      env.GP_FLEET_MANIFEST_FILE?.trim() || ".ghostprov/fleet.json",
    ),
  };
}
