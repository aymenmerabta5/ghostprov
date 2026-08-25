import * as path from "node:path";
import type { MachineSample } from "../core/ports";
import { projectId, type ProjectId } from "../sandbox/types";
import {
  DEFAULT_V2_SIDECAR_REVISION,
  resolveV2SidecarRevision,
  type V2SidecarRevision,
} from "../v2/sidecar/assets";
import {
  deriveV2HostEventMemorySizing,
  deriveV2SidecarEventMemorySizing,
  type V2HostEventMemorySizing,
} from "../v2/memory/event-memory-sizing";
import { V2_TERMINAL_SPOOL_CAPACITY } from "../v2/sidecar/capacity";

export type PrismSessionMode = "authenticated" | "auto" | "anonymous";

export interface MultiSandboxConfig {
  projectIds: readonly ProjectId[];
  prismOrigin: string;
  cookieFile: string;
  initialCookieHeader?: string;
  port: number;
  candidate: boolean;
  heartbeatMs: 10_000;
  routerQueueTimeoutMs: number;
  shutdownDrainMs: number;
  maxProjects: number;
  machine: MachineSample;
  sessionMode: PrismSessionMode;
  allowAnonymous: boolean;
  guestMaxProjects: number;
  v2SandboxCapacity: number;
  v2InitialActivePerSandbox: number;
  v2MaxActivePerSandbox: number;
  v2ResponseMaxBytes: number;
  /** Process-wide host event-memory pool, fully pre-reserved at startup. */
  v2HostEventMemory: V2HostEventMemorySizing;
  v2TransportReservationBytes: number;
  v2YSweetRefreshMs: number;
  v2SidecarMemoryBudgetBytes: number;
  v2DiagnosticsEnabled: boolean;
  v2DiagnosticsIntervalMs: number;
  v2BatchingEnabled: boolean;
  /** Process-wide immutable V2 release. R2 is normal operation; R1 is explicit rollback. */
  v2SidecarRevision: V2SidecarRevision;
  /** Exact configured-project overrides only; no prefix, cohort, or fallback matching. */
  v2SidecarProjectRevisions: Readonly<Record<string, V2SidecarRevision>>;
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

function v2SidecarProjectRevisions(
  value: string | undefined,
  projectIds: readonly ProjectId[],
): Readonly<Record<string, V2SidecarRevision>> {
  if (value === undefined || value.trim() === "") return Object.freeze({});
  const known = new Set<string>(projectIds);
  const overrides: Record<string, V2SidecarRevision> = {};
  for (const rawEntry of value.split(",")) {
    const entry = rawEntry.trim();
    const separator = entry.indexOf("=");
    if (
      separator < 1 ||
      separator !== entry.lastIndexOf("=") ||
      separator === entry.length - 1
    ) {
      throw new TypeError(
        "GP_V2_SIDECAR_PROJECT_REVISIONS must be project-id=r1|r2 entries",
      );
    }
    const rawProjectId = entry.slice(0, separator).trim();
    const selectedProject = projectId(rawProjectId);
    if (!known.has(selectedProject)) {
      throw new TypeError(
        `GP_V2_SIDECAR_PROJECT_REVISIONS names an unknown configured project: ${rawProjectId}`,
      );
    }
    if (Object.hasOwn(overrides, selectedProject)) {
      throw new TypeError(
        `GP_V2_SIDECAR_PROJECT_REVISIONS repeats project: ${rawProjectId}`,
      );
    }
    overrides[selectedProject] = resolveV2SidecarRevision(
      entry.slice(separator + 1).trim(),
    );
  }
  return Object.freeze(overrides);
}

export function resolveMultiSandboxConfig(
  env: Record<string, string | undefined>,
  machine: MachineSample,
): MultiSandboxConfig {
  const sessionMode = enumValue<PrismSessionMode>(
    env.GP_PRISM_SESSION,
    "authenticated",
    ["authenticated", "auto", "anonymous"],
    "GP_PRISM_SESSION",
  );
  const allowAnonymous = binaryFlag(env.GP_ALLOW_ANONYMOUS, "GP_ALLOW_ANONYMOUS");
  const autoCreateProjects = binaryFlag(
    env.GP_AUTO_CREATE_PROJECTS,
    "GP_AUTO_CREATE_PROJECTS",
  );
  if (autoCreateProjects) {
    throw new TypeError(
      "GP_AUTO_CREATE_PROJECTS is unsupported in V2-only mode; configure existing PRISM_PROJECT_IDS",
    );
  }
  const projectReuse = env.GP_PROJECT_REUSE?.trim() || "explicit";
  if (projectReuse !== "explicit") {
    throw new TypeError(
      "GP_PROJECT_REUSE must be explicit in V2-only mode",
    );
  }

  const rawProjects = (env.PRISM_PROJECT_IDS ?? env.PRISM_DOC_ID ?? "").trim();
  if (!rawProjects) {
    throw new TypeError(
      "PRISM_PROJECT_IDS or PRISM_DOC_ID is required in V2-only mode",
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
  if (sessionMode === "anonymous" && projectIds.length > guestMaxProjects) {
    throw new RangeError(
      `Configured ${projectIds.length} projects exceeds GP_GUEST_MAX_PROJECTS=${guestMaxProjects}`,
    );
  }
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
  const shutdownDrainMs = boundedPositiveInteger(
    env.GP_SHUTDOWN_DRAIN_MS,
    300_000,
    "GP_SHUTDOWN_DRAIN_MS",
    1_000,
    3_600_000,
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
    V2_TERMINAL_SPOOL_CAPACITY,
    "GP_V2_MAX_ACTIVE_PER_SANDBOX",
  );
  if (v2MaxActivePerSandbox > v2SandboxCapacity) {
    throw new RangeError("GP_V2_MAX_ACTIVE_PER_SANDBOX must not exceed physical V2 capacity");
  }
  if (v2MaxActivePerSandbox > V2_TERMINAL_SPOOL_CAPACITY) {
    throw new RangeError(
      `GP_V2_MAX_ACTIVE_PER_SANDBOX must not exceed the proven terminal-spool capacity (${V2_TERMINAL_SPOOL_CAPACITY})`,
    );
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
  if (v2ResponseMaxBytes !== 64 * 1024 * 1024) {
    throw new RangeError("GP_V2_RESPONSE_MAX_BYTES must remain exactly 64 MiB in this release");
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
  try {
    deriveV2SidecarEventMemorySizing({
      totalBytes: v2SidecarMemoryBudgetBytes,
      responseMaxBytes: 64 * 1024 * 1024,
    });
  } catch {
    throw new RangeError(
      "GP_V2_SIDECAR_MEMORY_BUDGET_BYTES must fit a 64 MiB atomic event plus ordinary memory",
    );
  }
  const v2DiagnosticsEnabled = binaryFlag(env.GP_V2_DIAGNOSTICS, "GP_V2_DIAGNOSTICS");
  const v2BatchingEnabled = binaryFlag(env.GP_V2_BATCHING, "GP_V2_BATCHING");
  const v2SidecarRevision = resolveV2SidecarRevision(
    env.GP_V2_SIDECAR_REVISION?.trim() || DEFAULT_V2_SIDECAR_REVISION,
  );
  const v2SidecarRevisionOverrides = v2SidecarProjectRevisions(
    env.GP_V2_SIDECAR_PROJECT_REVISIONS,
    projectIds,
  );
  const v2DiagnosticsIntervalMs = boundedPositiveInteger(
    env.GP_V2_DIAGNOSTICS_INTERVAL_MS, 1_000, "GP_V2_DIAGNOSTICS_INTERVAL_MS", 100, 60_000,
  );
  const desiredSandboxes = projectIds.length;
  const provisionConcurrency = Math.min(
    desiredSandboxes,
    positiveInteger(env.GP_PROVISION_CONCURRENCY, 4, "GP_PROVISION_CONCURRENCY"),
  );
  const v2HostEventMemory = deriveV2HostEventMemorySizing({
    responseMaxBytes: v2ResponseMaxBytes,
    runtimes: projectIds.length,
    maxActivePerRuntime: v2MaxActivePerSandbox,
  });

  return {
    projectIds: Object.freeze(projectIds),
    prismOrigin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    cookieFile: path.resolve(cookieFile),
    initialCookieHeader,
    port,
    candidate,
    heartbeatMs: 10_000,
    routerQueueTimeoutMs,
    shutdownDrainMs,
    maxProjects,
    machine,
    sessionMode,
    allowAnonymous,
    guestMaxProjects,
    v2SandboxCapacity,
    v2InitialActivePerSandbox,
    v2MaxActivePerSandbox,
    v2ResponseMaxBytes,
    v2HostEventMemory,
    v2TransportReservationBytes,
    v2YSweetRefreshMs,
    v2SidecarMemoryBudgetBytes,
    v2DiagnosticsEnabled,
    v2DiagnosticsIntervalMs,
    v2BatchingEnabled,
    v2SidecarRevision,
    v2SidecarProjectRevisions: v2SidecarRevisionOverrides,
    desiredSandboxes,
    provisionConcurrency,
    fleetManifestFile: path.resolve(
      env.GP_FLEET_MANIFEST_FILE?.trim() || ".ghostprov/fleet.json",
    ),
  };
}
