import type { ProjectFleetManagerOptions } from "../fleet/manager";
import type { FleetWorkerRecord } from "../fleet/types";
import type { AccountRequestClient, PrismProvisioningClient } from "../prism/provisioning-client";
import type { ProvisionedRuntimeCredentials } from "../prism/provisioning-client";
import { SandboxGateway } from "../sandbox/gateway";
import type { ProjectId } from "../sandbox/types";
import type { BootstrapCleanupAssessment } from "../sandbox/supervised-runtime";
import { SandboxYDoc } from "../sandbox/ydoc";
import type { V2SidecarAssets } from "../v2/sidecar/assets";
import type { SupervisorBootstrapJournalFactory } from "../v2/supervisor/bootstrap-cleanup-proof";
import {
  finalizeHeldSupervisorBootstrapCleanup,
  proveBlockedSupervisorBootstrapCleanup,
  proveHeldSupervisorBootstrapCleanup,
  type V2HeldSupervisorBootstrapCleanupOptions,
} from "../v2/supervisor/controller";
import type { SupervisorLifecycle } from "../v2/supervisor/lifecycle";

type CleanupArtifacts = Awaited<ReturnType<V2HeldSupervisorBootstrapCleanupOptions["getArtifacts"]>>;
type MaintenancePorts = Required<Pick<ProjectFleetManagerOptions,
  "cleanupHeldBootstrap" | "finalizeHeldBootstrapCleanup">> & {
    cleanupBlockedBootstrap(worker: Readonly<FleetWorkerRecord>, signal: AbortSignal): Promise<BootstrapCleanupAssessment>;
  };

export interface HeldBootstrapMaintenanceOptions {
  readonly journalFactory: SupervisorBootstrapJournalFactory;
  readonly assets: (projectId: ProjectId) => V2SidecarAssets;
  readonly provisioning: Pick<PrismProvisioningClient, "mintYSweet" | "acquireSandbox" | "provisionAcquired">;
  readonly account: AccountRequestClient;
  readonly acquirePermit: (signal: AbortSignal) => Promise<() => void>;
  readonly lifecycle: SupervisorLifecycle;
  readonly hostFactory?: V2HeldSupervisorBootstrapCleanupOptions["hostFactory"];
  /** Offline composition seam; production uses the existing workspace adapters. */
  readonly createArtifacts?: (credentials: ProvisionedRuntimeCredentials) => CleanupArtifacts;
}

function retainWorkspaceSyncOwnership(resources: CleanupArtifacts): CleanupArtifacts {
  return {
    ydoc: resources.ydoc,
    gateway: {
      async syncWorkspace(signal) {
        signal?.throwIfAborted();
        // SandboxGateway cancellation detaches a waiter from its shared sync.
        // Retain this maintenance operation and its permit until that bounded
        // underlying sync actually settles, then honor cancellation before proof.
        await resources.gateway.syncWorkspace();
        signal?.throwIfAborted();
      },
    },
    close: () => resources.close(),
  };
}

/** One fleet-owned operation; resources are acquired only after journal authority is checked. */
export function createHeldBootstrapMaintenance(options: HeldBootstrapMaintenanceOptions): MaintenancePorts {
  const run = async <T>(
    worker: Readonly<FleetWorkerRecord>,
    signal: AbortSignal,
    operation: (cleanup: V2HeldSupervisorBootstrapCleanupOptions, signal: AbortSignal) => Promise<T>,
  ): Promise<T> => {
    let releasePermit: (() => void) | undefined;
    const acquire = async (activeSignal: AbortSignal): Promise<void> => {
      activeSignal.throwIfAborted();
      options.lifecycle.assertAvailable();
      releasePermit ??= await options.acquirePermit(activeSignal);
      activeSignal.throwIfAborted();
      options.lifecycle.assertAvailable();
    };
    try {
      return await operation({
        worker,
        assets: options.assets(worker.projectId),
        bootstrapJournal: options.journalFactory(worker.projectId),
        ...(options.hostFactory ? { hostFactory: options.hostFactory } : {}),
        // The manifest hold plus exact submitted journal already persist this
        // historical obligation. A failed maintenance probe closes its
        // temporary host and must not poison the current process lifecycle.
        onCleanupProven: binding => options.lifecycle.confirmCleanup(binding),
        getYSweet: async activeSignal => {
          await acquire(activeSignal);
          // Reconnect only to the submitted supervisor's project. No workspace
          // acquisition, generation advance, or launch occurs in the proof phase.
          return options.provisioning.mintYSweet(worker.projectId, {
            source: "held-bootstrap-cleanup",
            bootstrapAttempt: 1,
            previouslyConnected: true,
          }, activeSignal);
        },
        getArtifacts: async (_binding, activeSignal) => {
          await acquire(activeSignal);
          // This phase is reachable only after durable supervisor cleanup proof
          // and the manifest's retirement transition. Workspace access is used
          // to synchronize owned tombstones; it never launches another sidecar.
          const sandbox = await options.provisioning.acquireSandbox(activeSignal, { maxAttempts: 1 });
          const credentials = await options.provisioning.provisionAcquired(worker.projectId, sandbox, activeSignal);
          activeSignal.throwIfAborted();
          if (options.createArtifacts) return retainWorkspaceSyncOwnership(options.createArtifacts(credentials));
          let firstCredential = true;
          const ydoc = new SandboxYDoc({
            projectId: worker.projectId,
            mintYSweet: () => {
              if (firstCredential) {
                firstCredential = false;
                return Promise.resolve(credentials.ySweet);
              }
              return options.provisioning.mintYSweet(worker.projectId, {
                source: "held-bootstrap-artifact-cleanup",
                bootstrapAttempt: 1,
                previouslyConnected: true,
              }, activeSignal);
            },
          });
          return retainWorkspaceSyncOwnership({
            ydoc,
            gateway: new SandboxGateway({ projectId: worker.projectId, account: options.account, sandbox }),
            close: () => ydoc.destroy(),
          });
        },
      }, signal);
    } finally {
      releasePermit?.();
    }
  };
  return {
    cleanupBlockedBootstrap: async (worker, signal) => {
      // The existing operation validates worker/journal authority. It performs
      // no network work for absent or prepared journals, and uses only YSweet
      // plus a general permit to prove a submitted supervisor's exact cleanup.
      const proven = await run(worker, signal, proveBlockedSupervisorBootstrapCleanup);
      signal.throwIfAborted();
      // Neither absence nor one incarnation's receipt clears another binding.
      if (options.lifecycle.hasPendingCleanup(worker.projectId)) return "unproved";
      return proven ? "proven" : "not_required";
    },
    cleanupHeldBootstrap: (worker, signal) => run(worker, signal, proveHeldSupervisorBootstrapCleanup),
    finalizeHeldBootstrapCleanup: (worker, signal) => run(worker, signal, finalizeHeldSupervisorBootstrapCleanup),
  };
}
