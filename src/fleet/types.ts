import type { PrismIdentityKind } from "../prism/session-manager";
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
  managed: true;
}

export interface FleetManifest {
  version: 1;
  identityKind: PrismIdentityKind;
  identityFingerprint: string;
  workers: readonly FleetWorkerRecord[];
}
