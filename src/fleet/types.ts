import type { PrismIdentityKind } from "../prism/session-manager";
import type { RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";

export type FleetWorkerState =
  | "creating"
  | "created"
  | "ready"
  | "draining"
  | "failed";

export type FleetRetirementReason = "replacement" | "missing" | "surplus";

export interface FleetWorkerRecord {
  index: number;
  projectId: ProjectId;
  title: string;
  state: FleetWorkerState;
  createdAtMs: number;
  updatedAtMs: number;
  leaderGeneration: number;
  managed: boolean;
  retirementReason: FleetRetirementReason | null;
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
