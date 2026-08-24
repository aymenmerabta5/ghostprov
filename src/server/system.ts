import { resolveMultiSandboxConfig } from "../config/multi-sandbox";
import { SYSTEM_CLOCK, type MachineProbe } from "../core/ports";
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
import { FleetRecoveryCoordinator } from "../prism/recovery-coordinator";
import {
  PrismSessionManager,
  type ResolvedPrismIdentity,
} from "../prism/session-manager";
import {
  CpuTransformPool,
  type CpuTransformPoolSnapshot,
} from "../router/cpu-transform-pool";
import { ResourceGovernor, SystemMachineProbe } from "../router/resource-governor";
import { ModelRateLimitGates } from "../router/rate-limit-gate";
import { MultiSandboxRouter } from "../router/router";
import { SandboxGateway } from "../sandbox/gateway";
import { SandboxHeartbeat } from "../sandbox/heartbeat";
import {
  ManagedV1Runtime,
  bundledV1DaemonSource,
  v1DaemonBuild,
} from "../sandbox/managed-v1-runtime";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import {
  RecoveryPermitPool,
  SupervisedSandboxRuntime,
} from "../sandbox/supervised-runtime";
import { SandboxYDoc } from "../sandbox/ydoc";
import { YSweetAwarenessChannel } from "../v2/awareness/y-sweet-channel";
import { SplitAwarenessChannel } from "../v2/awareness/split-channel";
import { V2BootstrapController } from "../v2/bootstrap/controller";
import type { HostFrame, SidecarFrame } from "../v2/protocol/shared";
import { parseAwarenessEnvelope } from "../v2/protocol/validate";
import { V2SandboxRuntime } from "../v2/runtime";
import { loadV2SidecarAssets, type V2SidecarAssets } from "../v2/sidecar/assets";
import { V2TransportSession } from "../v2/transport/session";
import { TransportSelectingRuntime } from "../v2/transport-selector";
import { createGhostProvApp } from "./app";
import { createGhostProvServer } from "./runtime";
import { resolveObservabilityConfig } from "../observability/config";
import {
  createObservabilityLogger,
  type LoggerLike,
} from "../observability/logger";
import {
  normalizeSandboxRecoveryCause,
  normalizeSandboxRecoveryErrorKind,
  normalizeSandboxRecoveryErrorName,
  normalizeSandboxRecoveryOperation,
  observabilityEvent,
  recoveryObservabilityEvent,
} from "../observability/events";
import { FleetSampler } from "../observability/sampler";
import { WebSocketTelemetry } from "../ws/telemetry";
import { closeResponsesWebSockets } from "../ws/server";

export interface BrowserlessSystemServer {
  readonly listening: boolean;
  once(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  listen(port: number, host: string, callback: () => void): this;
  close(callback?: (error?: Error) => void): this;
}

export interface BrowserlessSessionManager {
  resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  revalidate?(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  close(): Promise<void>;
}

export interface BrowserlessCpuPool {
  gzip?(bytes: Uint8Array): Promise<Uint8Array>;
  snapshot?(): CpuTransformPoolSnapshot;
  close(): Promise<void>;
}

export interface BrowserlessFleetSampler {
  start(): void;
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
  logger?: LoggerLike;
  sampler?: BrowserlessFleetSampler;
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
  logger: LoggerLike;
  sampler: BrowserlessFleetSampler;
  start(): Promise<void>;
  close(): Promise<void>;
}

function rateLimitScopeResolver(
  groups: readonly (readonly string[])[],
): (model: string) => string {
  const scopes = new Map<string, string>();
  for (const [index, group] of groups.entries()) {
    const scope = `shared:${index + 1}`;
    for (const model of group) scopes.set(model, scope);
  }
  return model => scopes.get(model) ?? model;
}

function defaultRuntimeFactory(options: {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  identity: ResolvedPrismIdentity;
  provisioning: PrismProvisioningClient;
  governor: ResourceGovernor;
  cpuPool: BrowserlessCpuPool;
  assets: V2SidecarAssets | null;
  nextLeaderGeneration(projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"]): Promise<number>;
  recoveryCoordinator: FleetRecoveryCoordinator;
  revalidateAccount(signal: AbortSignal): Promise<ResolvedPrismIdentity>;
  logger: LoggerLike;
}): ManagedRuntimeFactory {
  const {
    config,
    identity,
    provisioning,
    governor,
    cpuPool,
    assets,
    nextLeaderGeneration,
    recoveryCoordinator,
    revalidateAccount,
    logger,
  } = options;
  const needsV1 = config.transport === "v1" || (
    config.transport === "auto" && identity.kind === "authenticated"
  );
  const source = needsV1 ? bundledV1DaemonSource() : null;
  if (!cpuPool.gzip) throw new Error("CPU transform pool does not provide gzip");

  const createV2 = (input: {
    projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"];
    workerIndex: number;
    leaderGeneration: number;
    initialSandbox?: import("../prism/protocol").ProvisionedSandbox;
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
      initialSandbox: input.initialSandbox,
      acquireSandboxForAuth: (status, signal) => acquireSandboxForAuth(
        projectId,
        status,
        signal,
      ),
      onCredentialRefresh: (outcome, status) => {
        logger.emit(recoveryObservabilityEvent("sandbox.credential_refresh", {
          project_id: projectId,
          worker_index: workerIndex,
          transport: "v2",
          cause: "credentials",
          operation: "credential_refresh",
          status,
          credential_refresh_outcome: outcome,
        }), "progress");
      },
      gzip: bytes => cpuPool.gzip!(bytes),
      createGateway: sandbox => new SandboxGateway({
        projectId,
        account: identity.account,
        sandbox,
      }),
      createHeartbeat: (gateway, callbacks) => new SandboxHeartbeat({
        projectId,
        gateway,
        onSuccess: callbacks.onSuccess,
        onAuthenticationFailure: callbacks.onAuthenticationFailure,
        onExpired: callbacks.onExpired,
        onTransientFailure: callbacks.onTransientFailure,
        onUnreachable: callbacks.onUnreachable,
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
      createAwareness: (binding, credentials) => {
        const createLane = (
          lane: "data" | "control",
          field: "ghostprovV2Data" | "ghostprovV2Control",
        ) => new YSweetAwarenessChannel<HostFrame, SidecarFrame>({
          envelope: credentials.ySweet,
          localPeerId: `host-${workerIndex}:${lane}`,
          field,
          parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
        });
        return new SplitAwarenessChannel({
          data: createLane("data", "ghostprovV2Data"),
          control: createLane("control", "ghostprovV2Control"),
        });
      },
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
        initialActivePerSandbox: config.v2InitialActivePerSandbox,
        maxActivePerSandbox: config.v2MaxActivePerSandbox,
        memoryBudgetBytes: config.v2SidecarMemoryBudgetBytes,
        diagnostics: { enabled: config.v2DiagnosticsEnabled, intervalMs: config.v2DiagnosticsIntervalMs },
        batching: config.v2BatchingEnabled,
      }),
      createTransport: transport => new V2TransportSession({
        binding: transport.binding,
        channel: transport.channel,
        maxResponseBytes: transport.maxResponseBytes,
        gzip: transport.gzip,
        expectedSidecarInstanceId: transport.expectedSidecarInstanceId,
        hostCeiling: governor.capacityHint(
          config.v2TransportReservationBytes,
          config.v2SandboxCapacity,
        ),
      }),
    });
  };

  const recoveryPermits = new RecoveryPermitPool(config.provisionConcurrency);
  const acquireSandboxForAuth = async (
    projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"],
    _status: number,
    signal: AbortSignal,
  ) => {
    let release: (() => void) | null = null;
    try {
      await revalidateAccount(signal);
      release = await recoveryPermits.acquire(signal);
      return await provisioning.acquireSandbox(signal, { maxAttempts: 1 });
    } finally {
      release?.();
    }
  };
  const createRuntime = (
    input: Parameters<ManagedRuntimeFactory["create"]>[0],
    initialSandbox?: import("../prism/protocol").ProvisionedSandbox,
  ): RoutableSandboxRuntime => {
    const v1 = needsV1
      ? new ManagedV1Runtime({
        projectId: input.projectId,
        account: identity.account,
        provisioning,
        cpuPool: cpuPool as CpuTransformPool,
        daemonSource: source!,
        expectedBuild: v1DaemonBuild(source!),
        requestedCapacity: config.slotsPerProject,
        leaderGeneration: input.leaderGeneration,
        initialSandbox,
        acquireSandboxForAuth: (status, signal) => acquireSandboxForAuth(
          input.projectId,
          status,
          signal,
        ),
        onCredentialRefresh: (outcome, status) => {
          logger.emit(recoveryObservabilityEvent("sandbox.credential_refresh", {
            project_id: input.projectId,
            worker_index: input.workerIndex,
            transport: "v1",
            cause: "credentials",
            operation: "credential_refresh",
            status,
            credential_refresh_outcome: outcome,
          }), "progress");
        },
      })
      : undefined;
    if (config.transport === "v1") return v1!;
    const v2 = createV2({ ...input, initialSandbox });
    return config.transport === "auto"
      ? new TransportSelectingRuntime({
        mode: "auto",
        identityKind: identity.kind,
        v2,
        v1,
      })
      : v2;
  };

  return {
    create(input) {
      const initialChild = createRuntime(input);
      return new SupervisedSandboxRuntime({
        projectId: input.projectId,
        initialChild,
        // Recovery always reuses the same Prism project/worker. Only the
        // disposable sandbox session and its fenced leader generation change.
        createChild: async seed => createRuntime({
          projectId: input.projectId,
          workerIndex: input.workerIndex,
          leaderGeneration: await nextLeaderGeneration(input.projectId),
        }, seed),
        acquirePermit: signal => recoveryPermits.acquire(signal),
        recoveryCoordinator,
        workerIndex: input.workerIndex,
        transport: config.transport === "v1" ? "v1" : "v2",
        onRecoveryEvent: event => {
          const failure = event.failure;
          const name = event.type === "started"
            ? "sandbox.recovery.started"
            : event.type === "retry_scheduled"
              ? "sandbox.recovery.retry_scheduled"
              : event.type === "completed"
                ? "sandbox.recovery.completed"
                : "sandbox.recovery.aborted";
          logger.emit(recoveryObservabilityEvent(name, {
            project_id: event.projectId,
            worker_index: event.workerIndex,
            transport: event.transport,
            cause: normalizeSandboxRecoveryCause(event.reason),
            attempt: event.attempt,
            incarnation: event.incarnation,
            consecutive_failures: event.consecutiveFailures,
            ...(event.retryDelayMs === undefined
              ? {}
              : { retry_delay_ms: event.retryDelayMs }),
            duration_ms: event.durationMs,
            status: failure?.status ?? null,
            error_name: failure
              ? normalizeSandboxRecoveryErrorName(failure.name)
              : null,
            error_kind: failure
              ? normalizeSandboxRecoveryErrorKind(failure.kind)
              : null,
            operation: failure
              ? normalizeSandboxRecoveryOperation(failure.operation)
              : null,
          }), event.type === "retry_scheduled" ? "progress" : "terminal");
        },
      });
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
  const observabilityConfig = resolveObservabilityConfig(env);
  const logger = dependencies.logger ?? createObservabilityLogger(observabilityConfig);
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
    await logger.close().catch(() => undefined);
    throw error;
  }
  const governor = dependencies.governor ?? new ResourceGovernor({ probe: machineProbe });
  const cpuPool = dependencies.cpuPool ?? new CpuTransformPool({
    workers: governor.snapshot().cpuTransformWorkers,
  });
  const provisioning = new PrismProvisioningClient({ account: identity.account });
  const recoveryCoordinator = new FleetRecoveryCoordinator({
    revalidateAccount: signal => sessionManager.revalidate
      ? sessionManager.revalidate(signal)
      : Promise.resolve(identity),
    onTransition: snapshot => {
      logger.emit(recoveryObservabilityEvent("prism.circuit.changed", {
        cause: "circuit",
        operation: snapshot.state === "revalidating"
          ? "account_revalidate"
          : "probe",
        circuit_state: snapshot.state,
        circuit_scope: snapshot.scope,
        consecutive_failures: snapshot.consecutiveFailures,
        queued: snapshot.queued,
      }), "terminal");
    },
  });
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
    nextLeaderGeneration: project => manifest.nextLeaderGeneration(
      project,
      SYSTEM_CLOCK.now(),
    ),
    recoveryCoordinator,
    revalidateAccount: signal => sessionManager.revalidate
      ? sessionManager.revalidate(signal)
      : Promise.resolve(identity),
    logger,
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
  const rateLimitGates = !dependencies.router
    ? new ModelRateLimitGates({
      scopeForModel: rateLimitScopeResolver(config.rateLimitSharedModelGroups),
      coldMaxActive: config.rateLimitColdMaxActive,
    })
    : undefined;
  const router = dependencies.router ?? new MultiSandboxRouter({
    registry,
    governor,
    logger,
    queueTimeoutMs: config.routerQueueTimeoutMs,
    observability: {
      slowRequestMs: observabilityConfig.slowRequestMs,
      transport: config.transport,
    },
    rateLimitGates,
  });
  const webSocketTelemetry = config.v2DiagnosticsEnabled ? new WebSocketTelemetry() : undefined;
  const sampler = dependencies.sampler ?? new FleetSampler({
    logger,
    routerSnapshot: () => router.snapshot(),
    machineProbe,
    intervalMs: observabilityConfig.metricsIntervalMs,
    cpuPoolSnapshot: cpuPool.snapshot ? () => cpuPool.snapshot!() : undefined,
    webSocketSnapshot: webSocketTelemetry ? () => webSocketTelemetry.snapshotAndReset() : undefined,
    recoveryCircuitSnapshot: () => recoveryCoordinator.snapshot(),
    routerTrafficSnapshot: () => router.trafficSnapshotAndReset(),
  });
  const app = createGhostProvApp({ router });
  const server = dependencies.server ?? createGhostProvServer({
    app,
    router,
    port: config.port,
    candidate: config.candidate,
    webSocketTelemetry,
  }) as BrowserlessSystemServer;
  let reconciliation: Promise<unknown> | null = null;
  let starting: Promise<void> | null = null;
  let closing: Promise<void> | null = null;
  const startupController = new AbortController();

  const system: BrowserlessSystem = {
    config,
    account: identity.account,
    registry,
    governor,
    router,
    fleet,
    app,
    server,
    logger,
    sampler,
    start() {
      if (starting) return starting;
      starting = (async () => {
        reconciliation ??= fleet.reconcile();
        await Promise.race([
          fleet.waitForReady(1, startupController.signal),
          reconciliation.then(snapshot => {
            if (
              (snapshot as { ready: number }).ready < 1 &&
              registry.runtimes().length === 0
            ) {
              throw new Error("Fleet reconciliation produced no ready runtime");
            }
            // Registered supervisors continue retrying failed sandbox
            // generations in the background. Keep waiting for one to recover.
            return new Promise<never>(() => undefined);
          }),
        ]);
        sampler.start();
        if (server.listening) return;
        await new Promise<void>((resolve, reject) => {
          const onError = (error: Error) => reject(error);
          server.once("error", onError);
          server.listen(config.port, "127.0.0.1", () => {
            server.off("error", onError);
            resolve();
          });
        });
        const snapshot = router.snapshot();
        logger.emit(observabilityEvent("server.started", {
          port: config.port,
          transport: config.transport,
          runtimes: snapshot.runtimes.length,
          capacity: snapshot.capacity,
          pid: process.pid,
        }), "terminal");
      })();
      return starting;
    },
    close() {
      if (closing) return closing;
      closing = (async () => {
        startupController.abort();
        const before = router.snapshot();
        logger.emit(observabilityEvent("server.stopping", {
          reason: "system-shutdown",
          active: before.active,
          queued: before.queued,
        }), "terminal");
        const serverClosing = closeServer(server);
        router.beginShutdown();
        let firstError: unknown = null;
        await sampler.close().catch(error => { firstError ??= error; });
        try {
          await router.awaitIdle(config.shutdownDrainMs);
        } catch (error) {
          firstError = error;
        }
        await closeResponsesWebSockets(server as import("node:http").Server, {
          reason: "GhostProv graceful shutdown",
        }).catch(error => { firstError ??= error; });
        try {
          recoveryCoordinator.close();
          await fleet.shutdown("system-shutdown");
        } catch (error) {
          firstError ??= error;
        }
        await router.close().catch(error => { firstError ??= error; });
        await serverClosing.catch(error => { firstError ??= error; });
        await sessionManager.close().catch(error => { firstError ??= error; });
        await cpuPool.close().catch(error => { firstError ??= error; });
        await logger.close().catch(error => { firstError ??= error; });
        if (firstError) throw firstError;
      })();
      return closing;
    },
  };
  return system;
}
