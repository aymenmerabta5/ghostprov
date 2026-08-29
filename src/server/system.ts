import { resolveMultiSandboxConfig } from "../config/multi-sandbox";
import { SYSTEM_CLOCK, type MachineProbe } from "../core/ports";
import { ProjectFleetManager, type FleetProjectsClient } from "../fleet/manager";
import { FleetManifestStore } from "../fleet/manifest";
import type { ManagedRuntimeFactory } from "../fleet/types";
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
  type CpuTransformJobOptions,
  type CpuTransformPoolSnapshot,
} from "../router/cpu-transform-pool";
import {
  ResourceGovernor,
  SystemMachineProbe,
  type ResourceLease,
} from "../router/resource-governor";
import { MultiSandboxRouter } from "../router/router";
import { SandboxGateway } from "../sandbox/gateway";
import { SandboxHeartbeat } from "../sandbox/heartbeat";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import type { ProjectId } from "../sandbox/types";
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
import {
  V2SandboxRuntime,
  type V2RuntimeGateway,
  type V2RuntimeHeartbeat,
  type V2RuntimeHeartbeatCallbacks,
} from "../v2/runtime";
import type { V2SidecarAssets } from "../v2/sidecar/assets";
import { createBundledV2SidecarRevisionSelector } from "../v2/sidecar/revisions";
import { V2TransportSession, type V2GzipOptions } from "../v2/transport/session";
import { createGhostProvApp } from "./app";
import { ResponsesHttpContinuationIndex } from "../http/responses-continuation";
import { AnthropicContinuationIndex } from "../anthropic/continuation";
import {
  createAuthenticatedTenantScope,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";
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
  normalizeV2SidecarFatalCauseName,
  normalizeV2SidecarFatalErrorCode,
  normalizeV2SidecarFatalErrorName,
  normalizeV2SidecarFatalPermission,
  normalizeV2SidecarFatalPhase,
  observabilityEvent,
  recoveryObservabilityEvent,
  type JsonValue,
} from "../observability/events";
import { FleetSampler } from "../observability/sampler";
import { WebSocketTelemetry } from "../ws/telemetry";
import {
  closeResponsesWebSockets,
  DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
  DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES,
  DEFAULT_WS_PROCESS_MAX_SESSIONS,
} from "../ws/server";
import { WebSocketProcessBudget } from "../ws/session";
import {
  AtomicBytePermitPool,
  type AtomicBytePermitPoolSnapshot,
} from "../v2/memory/atomic-byte-permits";
import {
  checkedMemoryAdd,
  type V2HostEventMemorySizing,
} from "../v2/memory/event-memory-sizing";
import { summarizeV2Releases } from "../v2/release-observability";
import {
  processResponseGraphBudget,
  type SharedByteBudget,
} from "../ws/response-graph";

export interface BrowserlessSystemServer {
  readonly listening: boolean;
  once(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  listen(port: number, host: string, callback: () => void): this;
  close(callback?: (error?: Error) => void): this;
  closeAllConnections?(): void;
}

export interface BrowserlessSessionManager {
  resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  revalidate?(signal?: AbortSignal): Promise<ResolvedPrismIdentity>;
  close(): Promise<void>;
}

export interface BrowserlessCpuPool {
  gzip?(bytes: Uint8Array, options?: CpuTransformJobOptions): Promise<Uint8Array>;
  snapshot?(): CpuTransformPoolSnapshot;
  close(): Promise<void>;
}

export interface BrowserlessFleetSampler {
  start(): void;
  close(): Promise<void>;
}

export interface BrowserlessV2RevisionSelector {
  preload(): Promise<void>;
  loadedForProject(projectId: ProjectId): {
    revision: string;
    assets: V2SidecarAssets;
  };
}

/** One process-wide pool backed by one fixed global-governor reservation. */
export interface BrowserlessHostEventMemory {
  readonly permits: AtomicBytePermitPool;
  readonly reservedBytes: number;
  snapshot(): AtomicBytePermitPoolSnapshot;
  close(): void;
}

/** Fixed governor backing for long-lived WebSocket continuation graph bytes. */
export interface BrowserlessResponseGraphMemory {
  readonly budget: SharedByteBudget;
  readonly reservedBytes: number;
  close(): void;
}

export interface BrowserlessWebSocketIngressMemory {
  readonly budget: WebSocketProcessBudget;
  readonly reservedBytes: number;
  close(): void;
}

export function reserveWebSocketIngressMemory(
  governor: ResourceGovernor,
  budget: WebSocketProcessBudget,
): BrowserlessWebSocketIngressMemory {
  const snapshot = budget.snapshot();
  const reservedBytes = snapshot.maxIngressBytes + snapshot.maxQueuedBytes;
  if (!Number.isSafeInteger(reservedBytes)) {
    throw new RangeError("WebSocket process memory reservation exceeds the safe integer range");
  }
  let reservation: ResourceLease | null = governor.reserveFixedBytes(reservedBytes);
  let closed = false;
  return {
    budget,
    reservedBytes,
    close() {
      if (closed) return;
      const snapshot = budget.snapshot();
      if (
        snapshot.sessions !== 0 ||
        snapshot.ingressReservedBytes !== 0 ||
        snapshot.queuedBytes !== 0
      ) {
        throw new Error(
          `WebSocket ingress memory closed with ${snapshot.sessions} sessions and ` +
          `${snapshot.ingressReservedBytes} ingress bytes and ${snapshot.queuedBytes} queued bytes`,
        );
      }
      closed = true;
      reservation?.release();
      reservation = null;
    },
  };
}

export function reserveResponseGraphMemory(
  governor: ResourceGovernor,
  budget: SharedByteBudget = processResponseGraphBudget,
): BrowserlessResponseGraphMemory {
  const reservedBytes = budget.limit;
  let reservation: ResourceLease | null = governor.reserveFixedBytes(reservedBytes);
  let closed = false;
  return {
    budget,
    reservedBytes,
    close() {
      if (closed) return;
      // Never make live graph bytes uncharged. Callers must first close every
      // WebSocket session, whose ResponseGraph.clear() returns this to zero.
      if (budget.used !== 0) {
        throw new Error(`Response graph memory closed with ${budget.used} retained bytes`);
      }
      closed = true;
      reservation?.release();
      reservation = null;
    },
  };
}

export function reserveHostEventMemoryPool(
  governor: ResourceGovernor,
  sizing: V2HostEventMemorySizing,
): BrowserlessHostEventMemory {
  const reservedBytes = checkedMemoryAdd(
    sizing.ordinaryBytes,
    sizing.jumboBytes,
    "V2 host event pool reservation",
  );
  if (reservedBytes !== sizing.totalBytes) {
    throw new RangeError("V2 host event pool sizing total is inconsistent");
  }
  let reservation: ResourceLease | null = governor.reserveFixedBytes(reservedBytes);
  let permits: AtomicBytePermitPool;
  try {
    permits = new AtomicBytePermitPool({
      ordinaryBytes: sizing.ordinaryBytes,
      jumboBytes: sizing.jumboBytes,
    });
  } catch (error) {
    reservation.release();
    reservation = null;
    throw error;
  }
  let poolClosed = false;
  return {
    permits,
    reservedBytes,
    snapshot: () => permits.snapshot(),
    close() {
      if (!poolClosed) {
        poolClosed = true;
        // Reject new acquisitions immediately, but existing allocation handles
        // remain valid so deferred downstream cleanup can still release them.
        permits.close();
      }
      const leakedPermits = permits.snapshot().activePermits;
      if (leakedPermits !== 0) {
        // Never make live carrier memory uncharged. A later idempotent close
        // finalizes the fixed lease after all outstanding owners release.
        throw new Error(`Host event memory closed with ${leakedPermits} active permits`);
      }
      reservation?.release();
      reservation = null;
    },
  };
}

export interface BrowserlessSystemDependencies {
  machineProbe?: MachineProbe;
  governor?: ResourceGovernor;
  sessionManager?: BrowserlessSessionManager;
  projects?: FleetProjectsClient;
  manifest?: FleetManifestStore;
  runtimeFactory?: ManagedRuntimeFactory;
  registry?: SandboxRegistry;
  router?: MultiSandboxRouter;
  /** One opaque authority shared by every public continuation surface. */
  authorizationScope?: AuthenticatedTenantScope;
  /** Test seam for the app-scoped native Responses full-history index. */
  responsesContinuationIndex?: ResponsesHttpContinuationIndex;
  /** Test seam for the one app-scoped, bounded Claude continuation index. */
  anthropicContinuationIndex?: AnthropicContinuationIndex;
  webSocketProcessBudget?: WebSocketProcessBudget;
  server?: BrowserlessSystemServer;
  cpuPool?: BrowserlessCpuPool;
  assets?: V2SidecarAssets;
  revisionSelector?: BrowserlessV2RevisionSelector;
  logger?: LoggerLike;
  sampler?: BrowserlessFleetSampler;
  /** Test/integration seam; production omits it and uses SandboxHeartbeat. */
  heartbeatFactory?: (input: {
    projectId: ProjectId;
    workerIndex: number;
    gateway: V2RuntimeGateway;
    callbacks: V2RuntimeHeartbeatCallbacks;
  }) => V2RuntimeHeartbeat;
}

export interface BrowserlessSystem {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  account: PrismAccountSession;
  registry: SandboxRegistry;
  governor: ResourceGovernor;
  /** Narrow integration hook for V2 transport event-permit transfer. */
  hostEventMemory: BrowserlessHostEventMemory;
  responseGraphMemory: BrowserlessResponseGraphMemory;
  webSocketIngressMemory: BrowserlessWebSocketIngressMemory;
  router: MultiSandboxRouter;
  /** App-scoped native Responses full-history continuation metadata. */
  responsesContinuationIndex: ResponsesHttpContinuationIndex;
  /** Shared by the app's stable and beta Anthropic Messages routes. */
  anthropicContinuationIndex: AnthropicContinuationIndex;
  fleet: ProjectFleetManager;
  app: ReturnType<typeof createGhostProvApp>;
  server: BrowserlessSystemServer;
  logger: LoggerLike;
  sampler: BrowserlessFleetSampler;
  start(): Promise<void>;
  close(): Promise<void>;
}

export function createV2CpuGzip(
  cpuPool: BrowserlessCpuPool,
): (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array> {
  if (!cpuPool.gzip) throw new Error("CPU transform pool does not provide gzip");
  return (bytes, gzipOptions) => cpuPool.gzip!(bytes, {
    signal: gzipOptions?.signal,
    ...(gzipOptions?.priority ? { priority: gzipOptions.priority } : {}),
    // Only a disposable full-buffer view may be detached. The V2 transport
    // explicitly marks its scratch copy as owned; arbitrary subviews still use
    // the pool's copying path.
    transferInput: gzipOptions?.inputOwnership === "owned" &&
      bytes.buffer instanceof ArrayBuffer &&
      bytes.byteOffset === 0 &&
      bytes.byteLength === bytes.buffer.byteLength,
  });
}

function fixedV2RevisionSelector(
  projectIds: readonly ProjectId[],
  assets: V2SidecarAssets,
): BrowserlessV2RevisionSelector {
  const projects = new Set(projectIds);
  return {
    async preload() {},
    loadedForProject(projectId) {
      if (!projects.has(projectId)) {
        throw new Error(`Cannot select V2 assets for unknown project: ${projectId}`);
      }
      return { revision: assets.revision, assets };
    },
  };
}

export function selectV2ProjectRelease(
  selector: BrowserlessV2RevisionSelector,
  projectId: ProjectId,
): { revision: string; assets: V2SidecarAssets } {
  const selected = selector.loadedForProject(projectId);
  if (selected.assets.revision !== selected.revision) {
    throw new Error(
      `Selected V2 revision ${selected.revision} does not match loaded assets ${selected.assets.revision}`,
    );
  }
  return selected;
}

function defaultRuntimeFactory(options: {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  identity: ResolvedPrismIdentity;
  provisioning: PrismProvisioningClient;
  governor: ResourceGovernor;
  cpuPool: BrowserlessCpuPool;
  hostEventMemory: BrowserlessHostEventMemory;
  revisionSelector: BrowserlessV2RevisionSelector;
  nextLeaderGeneration(projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"]): Promise<number>;
  recoveryCoordinator: FleetRecoveryCoordinator;
  revalidateAccount(signal: AbortSignal): Promise<ResolvedPrismIdentity>;
  logger: LoggerLike;
  heartbeatFactory?: BrowserlessSystemDependencies["heartbeatFactory"];
}): ManagedRuntimeFactory {
  const {
    config,
    identity,
    provisioning,
    governor,
    cpuPool,
    hostEventMemory,
    revisionSelector,
    nextLeaderGeneration,
    recoveryCoordinator,
    revalidateAccount,
    logger,
    heartbeatFactory,
  } = options;
  const gzip = createV2CpuGzip(cpuPool);

  const createV2 = (input: {
    projectId: Parameters<ManagedRuntimeFactory["create"]>[0]["projectId"];
    workerIndex: number;
    leaderGeneration: number;
    initialSandbox?: import("../prism/protocol").ProvisionedSandbox;
  }): RoutableSandboxRuntime => {
    const { projectId, workerIndex, leaderGeneration } = input;
    const release = selectV2ProjectRelease(revisionSelector, projectId);
    return new V2SandboxRuntime({
      projectId,
      workerIndex,
      leaderGeneration,
      provisioning,
      governor,
      assets: release.assets,
      transportReservationBytes: config.v2TransportReservationBytes,
      maxResponseBytes: config.v2ResponseMaxBytes,
      eventMemoryPool: hostEventMemory.permits,
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
      gzip,
      createGateway: sandbox => new SandboxGateway({
        projectId,
        account: identity.account,
        sandbox,
      }),
      createHeartbeat: (gateway, callbacks) => heartbeatFactory
        ? heartbeatFactory({ projectId, workerIndex, gateway, callbacks })
        : new SandboxHeartbeat({
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
          parseRemote: value => parseAwarenessEnvelope(
            value,
            binding,
            "sidecar",
            release.assets.transportProfile,
          ),
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
        revision: release.revision,
        launchNonce: bootstrap.binding.launchNonce,
        // The outer same-project supervisor already retries forever with
        // observable backoff. One bounded bootstrap attempt prevents three
        // opaque 45-second tries from exceeding its 120-second child budget.
        attempts: 1,
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
        transportProfile: transport.transportProfile,
        channel: transport.channel,
        maxResponseBytes: transport.maxResponseBytes,
        gzip: transport.gzip,
        expectedSidecarInstanceId: transport.expectedSidecarInstanceId,
        eventMemoryPool: transport.eventMemoryPool,
        onStateChange: transport.onStateChange,
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
  ): RoutableSandboxRuntime => createV2({ ...input, initialSandbox });

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
        transport: "v2",
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
            attempt_duration_ms: event.attemptDurationMs,
            duration_ms: event.totalDurationMs,
            status: failure?.status ?? null,
            error_name: failure
              ? normalizeSandboxRecoveryErrorName(failure.name)
              : null,
            error_kind: failure
              ? normalizeSandboxRecoveryErrorKind(failure.kind)
              : null,
            sidecar_error_name: failure?.sidecarErrorName
              ? normalizeV2SidecarFatalErrorName(failure.sidecarErrorName)
              : null,
            sidecar_error_code: failure?.sidecarErrorCode
              ? normalizeV2SidecarFatalErrorCode(failure.sidecarErrorCode)
              : null,
            sidecar_cause_name: failure && "sidecarCauseName" in failure
              ? normalizeV2SidecarFatalCauseName(failure.sidecarCauseName)
              : null,
            sidecar_phase: failure?.sidecarPhase
              ? normalizeV2SidecarFatalPhase(failure.sidecarPhase)
              : null,
            sidecar_permission: failure && "sidecarPermission" in failure
              ? normalizeV2SidecarFatalPermission(failure.sidecarPermission)
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

async function settleServerClose(
  server: BrowserlessSystemServer,
  closing: Promise<void>,
  deadlineMs: number,
): Promise<{ closed: boolean; error?: unknown }> {
  const observed = closing.then(
    () => ({ kind: "closed" as const }),
    error => ({ kind: "closed" as const, error }),
  );
  const wait = async (milliseconds: number) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        observed,
        new Promise<{ kind: "timeout" }>(resolve => {
          timer = setTimeout(() => resolve({ kind: "timeout" }), Math.max(0, milliseconds));
        }),
      ]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  };
  let result = await wait(Math.max(0, deadlineMs - Date.now()));
  if (result.kind === "closed") {
    return "error" in result ? { closed: true, error: result.error } : { closed: true };
  }
  try {
    server.closeAllConnections?.();
  } catch (error) {
    return { closed: false, error };
  }
  result = await wait(1_000);
  if (result.kind === "closed") {
    return "error" in result ? { closed: true, error: result.error } : { closed: true };
  }
  return {
    closed: false,
    error: new Error("HTTP server output did not close before the shutdown deadline"),
  };
}

export async function createBrowserlessSystem(
  env: Record<string, string | undefined> = process.env,
  dependencies: BrowserlessSystemDependencies = {},
): Promise<BrowserlessSystem> {
  const machineProbe = dependencies.machineProbe ?? new SystemMachineProbe();
  const config = resolveMultiSandboxConfig(env, machineProbe.sample());
  const existingRouterScope = dependencies.router?.responseProvenanceAuthorizationScope();
  if (
    dependencies.authorizationScope !== undefined &&
    existingRouterScope !== null &&
    existingRouterScope !== undefined &&
    dependencies.authorizationScope !== existingRouterScope
  ) {
    throw new TypeError("Injected router and system authorization scopes must match");
  }
  const authorizationScope = dependencies.authorizationScope ?? existingRouterScope ??
    createAuthenticatedTenantScope();
  if (!isAuthenticatedTenantScope(authorizationScope)) {
    throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
  }
  const revisionSelector: BrowserlessV2RevisionSelector = dependencies.revisionSelector ??
    (dependencies.assets
      ? fixedV2RevisionSelector(config.projectIds, dependencies.assets)
      : createBundledV2SidecarRevisionSelector({
        projectIds: config.projectIds,
        defaultRevision: config.v2SidecarRevision,
        projectOverrides: config.v2SidecarProjectRevisions,
      }));
  // A selected release must be present and authentic before account/session,
  // server, fleet, or sandbox state is created. Never fall back to R1 here.
  await revisionSelector.preload();
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
  let hostEventMemory!: BrowserlessHostEventMemory;
  let responseGraphMemory!: BrowserlessResponseGraphMemory;
  let webSocketIngressMemory!: BrowserlessWebSocketIngressMemory;
  try {
    hostEventMemory = reserveHostEventMemoryPool(governor, config.v2HostEventMemory);
    responseGraphMemory = reserveResponseGraphMemory(governor);
    webSocketIngressMemory = reserveWebSocketIngressMemory(
      governor,
      dependencies.webSocketProcessBudget ?? new WebSocketProcessBudget({
        maxSessions: DEFAULT_WS_PROCESS_MAX_SESSIONS,
        maxQueuedBytes: DEFAULT_WS_PROCESS_MAX_QUEUED_BYTES,
        maxIngressBytes: DEFAULT_WS_PROCESS_MAX_INGRESS_BYTES,
      }),
    );
  } catch (error) {
    try { responseGraphMemory?.close(); } catch {}
    try { hostEventMemory?.close(); } catch {}
    governor.close();
    await sessionManager.close().catch(() => undefined);
    await logger.close().catch(() => undefined);
    throw error;
  }
  let cpuPool: BrowserlessCpuPool;
  try {
    cpuPool = dependencies.cpuPool ?? new CpuTransformPool({
      workers: governor.snapshot().cpuTransformWorkers,
    });
  } catch (error) {
    webSocketIngressMemory.close();
    responseGraphMemory.close();
    hostEventMemory.close();
    governor.close();
    await sessionManager.close().catch(() => undefined);
    await logger.close().catch(() => undefined);
    throw error;
  }
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
  const runtimeFactory = dependencies.runtimeFactory ?? defaultRuntimeFactory({
    config,
    identity,
    provisioning,
    governor,
    cpuPool,
    hostEventMemory,
    revisionSelector,
    nextLeaderGeneration: project => manifest.nextLeaderGeneration(
      project,
      SYSTEM_CLOCK.now(),
    ),
    recoveryCoordinator,
    revalidateAccount: signal => sessionManager.revalidate
      ? sessionManager.revalidate(signal)
      : Promise.resolve(identity),
    logger,
    heartbeatFactory: dependencies.heartbeatFactory,
  });
  const registry = dependencies.registry ?? new SandboxRegistry();
  const fleet = new ProjectFleetManager({
    config,
    identity: { kind: identity.kind, fingerprint: identity.fingerprint },
    projects,
    manifest,
    registry,
    runtimeFactory,
  });
  const router = dependencies.router ?? new MultiSandboxRouter({
    registry,
    governor,
    logger,
    queueTimeoutMs: config.routerQueueTimeoutMs,
    authorizationScope,
    observability: {
      slowRequestMs: observabilityConfig.slowRequestMs,
      transport: "v2",
    },
  });
  const responsesContinuationIndex = dependencies.responsesContinuationIndex ??
    new ResponsesHttpContinuationIndex();
  const anthropicContinuationIndex = dependencies.anthropicContinuationIndex ??
    new AnthropicContinuationIndex();
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
    hostEventMemorySnapshot: () => hostEventMemory.snapshot(),
    fleetSnapshot: () => fleet.snapshot(),
  });
  const app = createGhostProvApp({
    router,
    apiKey: config.apiKey,
    fleetSnapshot: () => fleet.snapshot(),
    authorizationScope,
    responsesContinuationIndex,
    anthropicContinuationIndex,
  });
  const server = dependencies.server ?? createGhostProvServer({
    app,
    router,
    apiKey: config.apiKey,
    port: config.port,
    candidate: config.candidate,
    webSocketTelemetry,
    webSocketProcessBudget: webSocketIngressMemory.budget,
    authorizationScope,
  }) as BrowserlessSystemServer;
  let reconciliation: Promise<unknown> | null = null;
  let starting: Promise<void> | null = null;
  let closing: Promise<void> | null = null;
  let listenOperation: Promise<void> | null = null;
  const startupController = new AbortController();

  const listen = (): Promise<void> => {
    if (server.listening) return Promise.resolve();
    if (startupController.signal.aborted || closing) {
      return Promise.reject(new DOMException("GhostProv startup aborted", "AbortError"));
    }
    let operation!: Promise<void>;
    operation = new Promise<void>((resolve, reject) => {
      let settled = false;
      const cleanup = () => {
        server.off("error", onError);
        startupController.signal.removeEventListener("abort", onAbort);
      };
      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (error) reject(error);
        else resolve();
      };
      const closeAbortedListener = () => {
        if (!server.listening) return;
        server.close(error => finish(
          error ?? new DOMException("GhostProv startup aborted", "AbortError"),
        ));
      };
      const onAbort = () => closeAbortedListener();
      const onError = (error: Error) => finish(error);
      startupController.signal.addEventListener("abort", onAbort, { once: true });
      server.once("error", onError);
      try {
        server.listen(config.port, "127.0.0.1", () => {
          if (startupController.signal.aborted || closing) {
            closeAbortedListener();
            return;
          }
          finish();
        });
      } catch (error) {
        finish(error);
      }
    }).finally(() => {
      if (listenOperation === operation) listenOperation = null;
    });
    listenOperation = operation;
    return operation;
  };

  const system: BrowserlessSystem = {
    config,
    account: identity.account,
    registry,
    governor,
    hostEventMemory,
    responseGraphMemory,
    webSocketIngressMemory,
    router,
    responsesContinuationIndex,
    anthropicContinuationIndex,
    fleet,
    app,
    server,
    logger,
    sampler,
    start() {
      if (starting) return starting;
      if (closing || startupController.signal.aborted) {
        return Promise.reject(new DOMException("GhostProv startup aborted", "AbortError"));
      }
      starting = (async () => {
        try {
          // Startup can legitimately spend time provisioning or bootstrapping.
          // Sample before readiness so both JSONL and the compact console show
          // the exact runtime phases instead of appearing completely frozen.
          sampler.start();
          reconciliation ??= fleet.reconcile(startupController.signal);
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
          if (startupController.signal.aborted || closing) {
            throw new DOMException("GhostProv startup aborted", "AbortError");
          }
          await listen();
          if (startupController.signal.aborted || closing) {
            throw new DOMException("GhostProv startup aborted", "AbortError");
          }
          const snapshot = router.snapshot();
          logger.emit(observabilityEvent("server.started", {
            port: config.port,
            transport: "v2",
            runtimes: snapshot.runtimes.length,
            capacity: snapshot.capacity,
            pid: process.pid,
            release: summarizeV2Releases(snapshot.runtimes) as unknown as JsonValue,
          }), "terminal");
        } catch (error) {
          if (!closing) await system.close().catch(() => undefined);
          throw error;
        }
      })();
      return starting;
    },
    close() {
      if (closing) return closing;
      closing = (async () => {
        startupController.abort();
        const shutdownDeadlineMs = Date.now() + config.shutdownDrainMs;
        const before = router.snapshot();
        logger.emit(observabilityEvent("server.stopping", {
          reason: "system-shutdown",
          active: before.active,
          queued: before.queued,
        }), "terminal");
        const pendingListen = listenOperation;
        const serverClosing = pendingListen
          ? (async () => {
              await pendingListen.catch(() => undefined);
              await closeServer(server);
            })()
          : closeServer(server);
        router.beginShutdown();
        responsesContinuationIndex.beginShutdown();
        anthropicContinuationIndex.beginShutdown();
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
        const serverClose = await settleServerClose(server, serverClosing, shutdownDeadlineMs);
        if (serverClose.error !== undefined) firstError ??= serverClose.error;
        // HTTP/SSE outputs are now drained (or timed out), and beginShutdown has
        // made every late translator callback a no-op. Clear once more so no
        // continuation metadata survives process teardown.
        responsesContinuationIndex.clear();
        anthropicContinuationIndex.clear();
        // Reject any governor waiters which are not owned by the router (for
        // example a body reservation or CPU transform waiting during teardown).
        // Never close the permit pool while a slow HTTP/SSE output may still
        // own a carrier. A failed force-close intentionally leaves the fixed
        // reservation intact rather than making live socket memory uncharged.
        if (serverClose.closed) {
          let fixedMemoryClosed = true;
          try { webSocketIngressMemory.close(); } catch (error) {
            fixedMemoryClosed = false;
            firstError ??= error;
          }
          try { responseGraphMemory.close(); } catch (error) {
            fixedMemoryClosed = false;
            firstError ??= error;
          }
          try { hostEventMemory.close(); } catch (error) {
            fixedMemoryClosed = false;
            firstError ??= error;
          }
          if (fixedMemoryClosed) governor.close();
        }
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
