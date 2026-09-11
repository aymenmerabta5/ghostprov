import type { PrismIdentityKind } from "../prism/session-manager";
import type { RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";

export type FleetWorkerState =
  | "creating"
  | "created"
  | "ready"
  | "draining"
  | "failed";

export type FleetRetirementReason = "replacement" | "missing" | "surplus" | "bootstrap_hold";

export type FleetReplacementFailureKind = "hello" | "readiness" | "render" | "timeout" | "bootstrap_blocked";

export type FleetMutationOperation = "replace" | "create" | "delete";

export type FleetMutationBlockReason =
  | "release_policy"
  | "recovery_circuit"
  | "mutation_backoff"
  | "runtime_missing"
  | "runtime_healthy"
  | "work_in_flight"
  | "continuation_binding"
  | "healthy_majority"
  | "project_client"
  | "control_plane"
  | "bootstrap_hold_limit"
  | "bootstrap_cleanup";

/** Content-free fleet transitions suitable for bounded observability. */
export type FleetLifecycleEvent =
  | {
      type: "replacement_qualified";
      projectId: ProjectId;
      workerIndex: number;
      leaderGeneration: number;
      failureKind: FleetReplacementFailureKind;
      failureCount: number;
    }
  | {
      type: "replacement_started";
      projectId: ProjectId;
      workerIndex: number;
      leaderGeneration: number;
      failureKind: FleetReplacementFailureKind;
      failureCount: number;
    }
  | {
      type: "successor_reserved" | "successor_created";
      projectId: ProjectId;
      workerIndex: number;
    }
  | {
      type: "successor_activated";
      projectId: ProjectId;
      workerIndex: number;
      leaderGeneration: number;
    }
  | {
      type: "old_retired";
      projectId: ProjectId;
      workerIndex: number;
      retirementReason: Exclude<FleetRetirementReason, "surplus" | "bootstrap_hold">;
      managed: boolean;
    }
  | {
      type: "mutation_blocked";
      projectId: ProjectId;
      workerIndex: number;
      operation: FleetMutationOperation;
      reason: FleetMutationBlockReason;
    };

export interface FleetReplacementEvidence {
  /** SHA-256 of fixed, credential-free failure atoms; never a raw message. */
  signatureHash: string;
  kind: FleetReplacementFailureKind;
  count: number;
  firstObservedAtMs: number;
  lastObservedAtMs: number;
  lastAttempt: number;
  lastIncarnation: number;
  /** Durable generation identity used to ignore a replayed supervisor callback. */
  lastLeaderGeneration: number;
  totalDurationMs: number;
}

export interface FleetWorkerRecord {
  index: number;
  projectId: ProjectId;
  title: string;
  state: FleetWorkerState;
  createdAtMs: number;
  updatedAtMs: number;
  leaderGeneration: number;
  /** Monotonic readiness proof; unlike `state`, failure never resets it. */
  hasReachedReady: boolean;
  managed: boolean;
  retirementReason: FleetRetirementReason | null;
  replacementEvidence: FleetReplacementEvidence | null;
}

export interface ManagedProjectInspection {
  writable: boolean;
  empty: boolean;
  marker: {
    protocol: "gp-awareness-v2";
    workerIndex: number;
  } | null;
}

export interface ManagedProjectInspector {
  inspect(
    projectId: ProjectId,
    signal?: AbortSignal,
  ): Promise<ManagedProjectInspection>;
}

export interface ManagedRuntimeFactory {
  create(input: {
    projectId: ProjectId;
    workerIndex: number;
    leaderGeneration: number;
    /** True only when the manifest proves this project was previously ready. */
    previouslyReady?: boolean;
  }): RoutableSandboxRuntime;
}

export interface FleetSnapshot {
  desired: number;
  ready: number;
  provisioning: number;
  draining: number;
  failed: number;
  /** Selected manifest projects without a currently healthy runtime. */
  unavailable: number;
  /** Selected manifest projects lacking healthy capacity. */
  unavailableProjects: readonly ProjectId[];
  projects: readonly ProjectId[];
  pendingRetirements?: number;
  /** Preserved projects excluded from routing and automatic deletion. */
  heldProjects?: readonly ProjectId[];
  projectMutation?: Readonly<{
    state: "closed" | "open";
    consecutiveFailures: number;
    retryInMs: number | null;
  }>;
}

export interface FleetManifest {
  version: 1;
  identityKind: PrismIdentityKind;
  identityFingerprint: string;
  /** True once legacy environment project IDs have been copied into workers. */
  legacyMigrationCompleted: boolean;
  workers: readonly FleetWorkerRecord[];
}
