import * as path from "node:path";
import {
  assertCandidatePort,
  DEFAULT_CANDIDATE_PORT,
  PROTECTED_SERVICE_PORT,
  type MachineSample,
} from "../core/ports";
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
import { resolveGhostProvApiKey } from "../http/api-key";

export type PrismSessionMode = "authenticated" | "auto" | "anonymous";

export const DEFAULT_MANAGED_SANDBOXES = 8;
export const MAX_MANAGED_SANDBOXES = 32;

export interface MultiSandboxConfig {
  /** Shared inbound credential for the OpenAI and Anthropic compatibility APIs. */
  apiKey: string;
  /**
   * Deprecated one-time migration seeds. The identity-scoped fleet manifest,
   * never this array, is the runtime project authority.
   */
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
  v2TerminalReceiptCoalescing: boolean;
  /** Process-wide immutable sidecar release. R10 is normal operation; prior releases are rollback. */
  v2SidecarRevision: V2SidecarRevision;
  /** Bootstrap launch transport. Relay stages a sandbox-container launcher; render forks from the render VM. */
  v2BootstrapLaunchMode: "render" | "relay";
  /** Cold bootstrap only. Git callbacks require R10; all modes retain the supervisor. */
  v2SupervisorBootstrapMode: "direct" | "llm" | "git-fsmonitor" | "git-ssh-signing";
  /** Phase-zero credential wait published into the sidecar static config. */
  v2BootstrapTimeoutMs: number;
  /** Exact persisted-project overrides only; no prefix, cohort, or fallback matching. */
  v2SidecarProjectRevisions: Readonly<Record<string, V2SidecarRevision>>;
  desiredSandboxes: number;
  provisionConcurrency: number;
  /** Separate ceiling for projects which have never reached verified readiness. */
  coldStartConcurrency: number;
  /** Null selects the identity-scoped ~/.ghostprov/fleets/<fingerprint> path. */
  fleetManifestFile: string | null;
  /** Repeated identical project-local failures required before replacement. */
  projectReplacementFailureThreshold: number;
  /**
   * Per-generation startup budget (provision + bootstrap). Slow relay
   * bootstraps may need more than the historical 120 s; steady-state request
   * deadlines are separate and unaffected.
   */
  sandboxStartupTimeoutMs: number;
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
): Readonly<Record<string, V2SidecarRevision>> {
  if (value === undefined || value.trim() === "") return Object.freeze({});
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
        "GP_V2_SIDECAR_PROJECT_REVISIONS must be project-id=r1|r2|r3|r4|r5|r6|r7|r8|r9|r10 entries",
      );
    }
    const rawProjectId = entry.slice(0, separator).trim();
    const selectedProject = projectId(rawProjectId);
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
  const apiKey = resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  const sessionMode = enumValue<PrismSessionMode>(
    env.GP_PRISM_SESSION,
    "authenticated",
    ["authenticated", "auto", "anonymous"],
    "GP_PRISM_SESSION",
  );
  const allowAnonymous = binaryFlag(env.GP_ALLOW_ANONYMOUS, "GP_ALLOW_ANONYMOUS");
  // These legacy switches no longer control behavior. Accept their two safe
  // historical values so an existing installation can upgrade before removing
  // them, while project creation remains an unconditional managed-fleet duty.
  if (env.GP_AUTO_CREATE_PROJECTS !== undefined) {
    binaryFlag(env.GP_AUTO_CREATE_PROJECTS, "GP_AUTO_CREATE_PROJECTS");
  }
  const projectReuse = env.GP_PROJECT_REUSE?.trim();
  if (projectReuse && projectReuse !== "explicit" && projectReuse !== "managed") {
    throw new TypeError(
      "GP_PROJECT_REUSE may only be explicit or managed during migration",
    );
  }

  // Environment IDs are imported exactly once into the account-bound
  // manifest. They are deliberately not an allowlist and are not required for
  // subsequent starts (or for a fresh automatically managed fleet).
  const rawProjects = (
    env.PRISM_PROJECT_IDS?.trim() || env.PRISM_DOC_ID?.trim() || ""
  );
  const projectIds = rawProjects
    ? [...new Set(rawProjects.split(",").map(value => value.trim()).filter(Boolean))]
      .map(projectId)
    : [];
  const maxProjects = boundedPositiveInteger(
    env.GP_MAX_PROJECTS,
    MAX_MANAGED_SANDBOXES,
    "GP_MAX_PROJECTS",
    1,
    MAX_MANAGED_SANDBOXES,
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
  if (projectIds.length > maxProjects) {
    throw new RangeError(
      `Legacy migration contains ${projectIds.length} projects, exceeding GP_MAX_PROJECTS=${maxProjects}`,
    );
  }
  const targetWasConfigured = env.GP_TARGET_SANDBOXES !== undefined &&
    env.GP_TARGET_SANDBOXES.trim() !== "";
  const requestedSandboxes = targetWasConfigured
    ? positiveInteger(env.GP_TARGET_SANDBOXES, 1, "GP_TARGET_SANDBOXES")
    : Math.min(DEFAULT_MANAGED_SANDBOXES, maxProjects);
  if (requestedSandboxes > maxProjects) {
    throw new RangeError("GP_TARGET_SANDBOXES must not exceed GP_MAX_PROJECTS");
  }
  if (
    sessionMode === "anonymous" &&
    targetWasConfigured &&
    requestedSandboxes > guestMaxProjects
  ) {
    throw new RangeError("GP_TARGET_SANDBOXES must not exceed GP_GUEST_MAX_PROJECTS in anonymous mode");
  }
  const desiredSandboxes = sessionMode === "anonymous"
    ? Math.min(requestedSandboxes, guestMaxProjects)
    : requestedSandboxes;

  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  const initialCookieHeader = env.PRISM_COOKIE?.trim();
  if (!cookieFile) throw new TypeError("PRISM_COOKIE_FILE is required");
  if (sessionMode === "anonymous" && initialCookieHeader) {
    throw new TypeError("PRISM_COOKIE cannot seed an anonymous Prism session");
  }

  const candidate = binaryFlag(env.GP_CANDIDATE, "GP_CANDIDATE");
  const port = positiveInteger(
    env.PORT,
    candidate ? DEFAULT_CANDIDATE_PORT : PROTECTED_SERVICE_PORT,
    "PORT",
  );
  if (candidate) assertCandidatePort(port, "Candidate mode");

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
  const v2BatchingEnabled = env.GP_V2_BATCHING === undefined
    ? true
    : binaryFlag(env.GP_V2_BATCHING, "GP_V2_BATCHING");
  const v2TerminalReceiptCoalescing = env.GP_V2_TERMINAL_RECEIPT_COALESCING === undefined
    ? true
    : binaryFlag(env.GP_V2_TERMINAL_RECEIPT_COALESCING, "GP_V2_TERMINAL_RECEIPT_COALESCING");
  const v2SidecarRevision = resolveV2SidecarRevision(
    env.GP_V2_SIDECAR_REVISION?.trim() || DEFAULT_V2_SIDECAR_REVISION,
  );
  const v2SidecarRevisionOverrides = v2SidecarProjectRevisions(
    env.GP_V2_SIDECAR_PROJECT_REVISIONS,
  );
  const v2BootstrapLaunchMode = enumValue(
    env.GP_V2_BOOTSTRAP_LAUNCH_MODE,
    "relay",
    ["render", "relay"] as const,
    "GP_V2_BOOTSTRAP_LAUNCH_MODE",
  );
  if (v2BootstrapLaunchMode !== "relay" &&
    (v2SidecarRevision === "r10" || Object.values(v2SidecarRevisionOverrides).includes("r10"))) {
    throw new TypeError("R10 requires GP_V2_BOOTSTRAP_LAUNCH_MODE=relay and its persistent supervisor");
  }
  const v2BootstrapTimeoutMs = boundedPositiveInteger(
    env.GP_V2_BOOTSTRAP_TIMEOUT_MS, 300_000, "GP_V2_BOOTSTRAP_TIMEOUT_MS", 30_000, 600_000,
  );
  const v2SupervisorBootstrapMode = enumValue(
    env.GP_V2_SUPERVISOR_BOOTSTRAP_MODE,
    "direct",
    ["direct", "llm", "git-fsmonitor", "git-ssh-signing"] as const,
    "GP_V2_SUPERVISOR_BOOTSTRAP_MODE",
  );
  if ((v2SupervisorBootstrapMode === "git-fsmonitor" || v2SupervisorBootstrapMode === "git-ssh-signing") &&
    (v2BootstrapLaunchMode !== "relay" || v2SidecarRevision !== "r10" ||
      Object.values(v2SidecarRevisionOverrides).some(revision => revision !== "r10"))) {
    throw new TypeError(`GP_V2_SUPERVISOR_BOOTSTRAP_MODE=${v2SupervisorBootstrapMode} requires R10 relay bootstrap for every project`);
  }
  const v2DiagnosticsIntervalMs = boundedPositiveInteger(
    env.GP_V2_DIAGNOSTICS_INTERVAL_MS, 1_000, "GP_V2_DIAGNOSTICS_INTERVAL_MS", 100, 60_000,
  );
  const provisionConcurrency = Math.min(
    desiredSandboxes,
    positiveInteger(env.GP_PROVISION_CONCURRENCY, 4, "GP_PROVISION_CONCURRENCY"),
  );
  const coldStartConcurrency = boundedPositiveInteger(
    env.GP_COLD_START_CONCURRENCY,
    Math.min(2, provisionConcurrency),
    "GP_COLD_START_CONCURRENCY",
    1,
    provisionConcurrency,
  );
  const sandboxStartupTimeoutMs = boundedPositiveInteger(
    env.GP_SANDBOX_STARTUP_TIMEOUT_MS, 120_000, "GP_SANDBOX_STARTUP_TIMEOUT_MS", 120_000, 1_800_000,
  );
  const v2HostEventMemory = deriveV2HostEventMemorySizing({
    responseMaxBytes: v2ResponseMaxBytes,
    runtimes: desiredSandboxes,
    maxActivePerRuntime: v2MaxActivePerSandbox,
  });

  return {
    apiKey,
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
    v2TerminalReceiptCoalescing,
    v2SidecarRevision,
    v2SidecarProjectRevisions: v2SidecarRevisionOverrides,
    v2BootstrapLaunchMode,
    v2SupervisorBootstrapMode,
    v2BootstrapTimeoutMs,
    sandboxStartupTimeoutMs,
    desiredSandboxes,
    provisionConcurrency,
    coldStartConcurrency,
    fleetManifestFile: env.GP_FLEET_MANIFEST_FILE?.trim()
      ? path.resolve(env.GP_FLEET_MANIFEST_FILE.trim())
      : null,
    projectReplacementFailureThreshold: boundedPositiveInteger(
      env.GP_PROJECT_REPLACEMENT_FAILURES,
      3,
      "GP_PROJECT_REPLACEMENT_FAILURES",
      3,
      100,
    ),
  };
}
