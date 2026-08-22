import type { PrismIdentityKind } from "../prism/session-manager";
import type { RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";

export type FleetWorkerState =
  | "creating"
  | "created"
  | "ready"
  | "draining"
  | "failed";

export interface FleetWorkerRecord {
  index: number;
  projectId: ProjectId;
  title: string;
  state: FleetWorkerState;
  createdAtMs: number;
  updatedAtMs: number;
  leaderGeneration: number;
  managed: boolean;
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
  projects: readonly ProjectId[];
}

export interface FleetManifest {
  version: 1;
  identityKind: PrismIdentityKind;
  identityFingerprint: string;
  workers: readonly FleetWorkerRecord[];
}
