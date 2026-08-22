import { resolveMultiSandboxConfig } from "../config/multi-sandbox";
import type { MachineProbe } from "../core/ports";
import { ProjectFleetManager, type FleetProjectsClient } from "../fleet/manager";
import { FleetManifestStore } from "../fleet/manifest";
import { YDocProjectInspector } from "../fleet/project-inspector";
import type {
  ManagedProjectInspector,
  ManagedRuntimeFactory,
} from "../fleet/types";
import type { PrismAccountSession } from "../prism/account-session";
import { PrismProjectsClient } from "../prism/projects-client";
import { PrismProvisioningClient } from "../prism/provisioning-client";
import {
  PrismSessionManager,
  type ResolvedPrismIdentity,
} from "../prism/session-manager";
import { CpuTransformPool } from "../router/cpu-transform-pool";
import { ResourceGovernor, SystemMachineProbe } from "../router/resource-governor";
import { MultiSandboxRouter } from "../router/router";
import { SandboxGateway } from "../sandbox/gateway";
import { SandboxHeartbeat } from "../sandbox/heartbeat";
import {
  ManagedV1Runtime,
  bundledV1DaemonSource,
  v1DaemonBuild,
} from "../sandbox/managed-v1-runtime";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import { SandboxYDoc } from "../sandbox/ydoc";
import { YSweetAwarenessChannel } from "../v2/awareness/y-sweet-channel";
import { V2BootstrapController } from "../v2/bootstrap/controller";
import { parseAwarenessEnvelope } from "../v2/protocol/validate";
import { V2SandboxRuntime } from "../v2/runtime";
import { loadV2SidecarAssets, type V2SidecarAssets } from "../v2/sidecar/assets";
import { V2TransportSession } from "../v2/transport/session";
import { TransportSelectingRuntime } from "../v2/transport-selector";
import { createGhostProvApp } from "./app";
import { createGhostProvServer } from "./runtime";

export interface BrowserlessSystemServer {
  readonly listening: boolean;
  once(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  listen(port: number, host: string, callback: () => void): this;
  close(callback?: (error?: Error) => void): this;
}

export interface BrowserlessSessionManager {
  resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  close(): Promise<void>;
}

export interface BrowserlessCpuPool {
  gzip?(bytes: Uint8Array): Promise<Uint8Array>;
  close(): Promise<void>;
}

export interface BrowserlessSystemDependencies {
  machineProbe?: MachineProbe;
  governor?: ResourceGovernor;
  sessionManager?: BrowserlessSessionManager;
  projects?: FleetProjectsClient;
  manifest?: FleetManifestStore;
  inspector?: ManagedProjectInspector;
  runtimeFactory?: ManagedRuntimeFactory;
  registry?: SandboxRegistry;
  router?: MultiSandboxRouter;
  server?: BrowserlessSystemServer;
  cpuPool?: BrowserlessCpuPool;
  assets?: V2SidecarAssets;
}

export interface BrowserlessSystem {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  account: PrismAccountSession;
  registry: SandboxRegistry;
  governor: ResourceGovernor;
  router: MultiSandboxRouter;
  fleet: ProjectFleetManager;
  app: ReturnType<typeof createGhostProvApp>;
  server: BrowserlessSystemServer;
  start(): Promise<void>;
  close(): Promise<void>;
}

function defaultRuntimeFactory(options: {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  identity: ResolvedPrismIdentity;
  provisioning: PrismProvisioningClient;
  governor: ResourceGovernor;
  cpuPool: BrowserlessCpuPool;
  assets: V2SidecarAssets | null;
}): ManagedRuntimeFactory {
  const { config, identity, provisioning, governor, cpuPool, assets } = options;
  const needsV1 = config.transport === "v1" || (
    config.transport === "auto" && identity.kind === "authenticated"
  );
  const source = needsV1 ? bundledV1DaemonSource() : null;
  if (!cpuPool.gzip) throw new Error("CPU transform pool does not provide gzip");

  const createV2 = (input: {
    projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"];
    workerIndex: number;
    leaderGeneration: number;
  }): RoutableSandboxRuntime => {
    if (!assets) throw new Error("V2 sidecar assets are unavailable");
    const { projectId, workerIndex, leaderGeneration } = input;
    return new V2SandboxRuntime({
      projectId,
      workerIndex,
      leaderGeneration,
      provisioning,
      governor,
      assets,
      transportReservationBytes: config.v2TransportReservationBytes,
      maxResponseBytes: config.v2ResponseMaxBytes,
      ySweetRefreshMs: config.v2YSweetRefreshMs,
      gzip: bytes => cpuPool.gzip!(bytes),
      createGateway: sandbox => new SandboxGateway({
        projectId,
        account: identity.account,
        sandbox,
      }),
      createHeartbeat: (gateway, callbacks) => new SandboxHeartbeat({
        projectId,
        gateway,
        onExpired: callbacks.onExpired,
        onTransientFailure: callbacks.onTransientFailure,
      }),
      createYDoc: credentials => {
        let first = true;
        return new SandboxYDoc({
          projectId,
          mintYSweet: async () => {
            if (first) {
              first = false;
              return credentials.ySweet;
            }
            return provisioning.mintYSweet(projectId, {
              source: "v2-ydoc-reconnect",
              bootstrapAttempt: 1,
              previouslyConnected: true,
              sandboxUrl: credentials.sandbox.url,
              sandboxId: credentials.sandbox.sandboxId ?? null,
              sandboxSessionId: credentials.sandbox.sessionId ?? null,
            });
          },
        });
      },
      createAwareness: (binding, credentials) => new YSweetAwarenessChannel({
        envelope: credentials.ySweet,
        localPeerId: `host-${workerIndex}`,
        field: "ghostprovV2",
        parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      }),
      createBootstrap: bootstrap => new V2BootstrapController({
        projectId,
        workerIndex,
        sandboxSessionId: bootstrap.binding.sessionId,
        leaderGeneration,
        ySweet: bootstrap.credentials.ySweet,
        ydoc: bootstrap.ydoc as SandboxYDoc,
        gateway: bootstrap.gateway as SandboxGateway,
        awareness: bootstrap.awareness,
        assets: bootstrap.assets,
        physicalMax: config.v2SandboxCapacity,
        acceptedMax: config.v2SandboxCapacity * 2,
        memoryBudgetBytes: config.v2SidecarMemoryBudgetBytes,
      }),
      createTransport: transport => new V2TransportSession({
        binding: transport.binding,
        channel: transport.channel,
        maxResponseBytes: transport.maxResponseBytes,
        gzip: transport.gzip,
        hostCeiling: governor.capacityHint(
          config.v2TransportReservationBytes,
          config.v2SandboxCapacity,
        ),
      }),
    });
  };

  return {
    create(input) {
      const v1 = needsV1
        ? new ManagedV1Runtime({
          projectId: input.projectId,
          account: identity.account,
          provisioning,
          cpuPool: cpuPool as CpuTransformPool,
          daemonSource: source!,
          expectedBuild: v1DaemonBuild(source!),
          requestedCapacity: config.slotsPerProject,
        })
        : undefined;
      if (config.transport === "v1") return v1!;
      const v2 = createV2(input);
      return config.transport === "auto"
        ? new TransportSelectingRuntime({
          mode: "auto",
          identityKind: identity.kind,
          v2,
          v1,
        })
        : v2;
    },
  };
}

function closeServer(server: BrowserlessSystemServer): Promise<void> {
  if (!server.listening) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
  });
}

export async function createBrowserlessSystem(
  env: Record<string, string | undefined> = process.env,
  dependencies: BrowserlessSystemDependencies = {},
): Promise<BrowserlessSystem> {
  const machineProbe = dependencies.machineProbe ?? new SystemMachineProbe();
  const config = resolveMultiSandboxConfig(env, machineProbe.sample());
  const sessionManager = dependencies.sessionManager ?? await PrismSessionManager.create({
    origin: config.prismOrigin,
    mode: config.sessionMode,
    allowAnonymous: config.allowAnonymous,
    cookieFile: config.cookieFile,
    initialCookieHeader: config.initialCookieHeader,
  });
  let identity: ResolvedPrismIdentity;
  try {
    identity = await sessionManager.resolve();
  } catch (error) {
    await sessionManager.close().catch(() => undefined);
    throw error;
  }
  const governor = dependencies.governor ?? new ResourceGovernor({ probe: machineProbe });
  const cpuPool = dependencies.cpuPool ?? new CpuTransformPool({
    workers: governor.snapshot().cpuTransformWorkers,
  });
  const provisioning = new PrismProvisioningClient({ account: identity.account });
  const projects = dependencies.projects ?? new PrismProjectsClient(identity.account);
  const manifest = dependencies.manifest ?? new FleetManifestStore({
    file: config.fleetManifestFile,
    identityFingerprint: identity.fingerprint,
  });
  const inspector = dependencies.inspector ?? new YDocProjectInspector({
    mintYSweet: (projectId, signal) => provisioning.mintYSweet(projectId, {
      source: "fleet-inspection",
      bootstrapAttempt: 1,
      previouslyConnected: false,
      sandboxUrl: null,
      sandboxId: null,
      sandboxSessionId: null,
    }, signal),
  });
  const assets = config.transport === "v1"
    ? null
    : dependencies.assets ?? await loadV2SidecarAssets();
  const runtimeFactory = dependencies.runtimeFactory ?? defaultRuntimeFactory({
    config,
    identity,
    provisioning,
    governor,
    cpuPool,
    assets,
  });
  const registry = dependencies.registry ?? new SandboxRegistry();
  const fleet = new ProjectFleetManager({
    config,
    identity: { kind: identity.kind, fingerprint: identity.fingerprint },
    projects,
    inspector,
    manifest,
    registry,
    runtimeFactory,
  });
  const router = dependencies.router ?? new MultiSandboxRouter({ registry, governor });
  const app = createGhostProvApp({ router });
  const server = dependencies.server ?? createGhostProvServer({
    app,
    router,
    port: config.port,
    candidate: config.candidate,
  }) as BrowserlessSystemServer;
  let reconciliation: Promise<unknown> | null = null;
  let starting: Promise<void> | null = null;
  let closing: Promise<void> | null = null;

  const system: BrowserlessSystem = {
    config,
    account: identity.account,
    registry,
    governor,
    router,
    fleet,
    app,
    server,
    start() {
      if (starting) return starting;
      starting = (async () => {
        reconciliation ??= fleet.reconcile();
        await Promise.race([
          fleet.waitForReady(1),
          reconciliation.then(snapshot => {
            if ((snapshot as { ready: number }).ready < 1) {
              throw new Error("Fleet reconciliation produced no ready runtime");
            }
          }),
        ]);
        if (server.listening) return;
        await new Promise<void>((resolve, reject) => {
          const onError = (error: Error) => reject(error);
          server.once("error", onError);
          server.listen(config.port, "127.0.0.1", () => {
            server.off("error", onError);
            resolve();
          });
        });
      })();
      return starting;
    },
    close() {
      if (closing) return closing;
      closing = (async () => {
        const serverClosing = closeServer(server);
        router.beginShutdown();
        let firstError: unknown = null;
        try {
          await router.awaitIdle(30_000);
        } catch (error) {
          firstError = error;
        }
        try {
          await fleet.shutdown("system-shutdown");
        } catch (error) {
          firstError ??= error;
        }
        await router.close().catch(error => { firstError ??= error; });
        await serverClosing.catch(error => { firstError ??= error; });
        await sessionManager.close().catch(error => { firstError ??= error; });
        await cpuPool.close().catch(error => { firstError ??= error; });
        if (firstError) throw firstError;
      })();
      return closing;
    },
  };
  return system;
}
