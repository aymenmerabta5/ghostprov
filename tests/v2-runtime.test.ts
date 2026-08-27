import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import type { RunHandle } from "../src/relay/run-handle";
import {
  parseProvisionedSandbox,
  parseResourceCredential,
  parseYSweetEnvelope,
  type ProvisionedSandbox,
  type YSweetEnvelope,
} from "../src/prism/protocol";
import type {
  ProvisionedRuntimeCredentials,
  ProvisioningHooks,
} from "../src/prism/provisioning-client";
import type { SandboxRunOptions } from "../src/sandbox/registry";
import { projectId } from "../src/sandbox/types";
import type { AwarenessChannel } from "../src/v2/awareness/channel";
import { AwarenessChannelError } from "../src/v2/awareness/y-sweet-channel";
import type { V2ReadyDescriptor } from "../src/v2/bootstrap/controller";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";
import type {
  V2TransportSnapshot,
} from "../src/v2/transport/session";
import type { V2TransportDiagnostics } from "../src/v2/diagnostics/types";
import {
  V2CommittedError,
  V2PreCommitError,
  V2RunHandle,
} from "../src/v2/transport/run-handle";
import {
  V2SandboxRuntime,
  type V2RuntimeBootstrap,
  type V2RuntimeGovernor,
  type V2RuntimeHeartbeat,
  type V2RuntimeHeartbeatCallbacks,
  type V2RuntimeHeartbeatResult,
  type V2RuntimeTransport,
  type V2RuntimeYDoc,
} from "../src/v2/runtime";
import type { V2SidecarRecoveryToken } from "../src/v2/transport/session";
import { chooseRuntime, type RouteCandidate } from "../src/router/policy";
import type { WorkloadDescriptor } from "../src/router/workload";

const PROJECT = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const ASSETS: V2SidecarAssets = {
  revision: "r1",
  protocolRevision: "r1",
  transportProfile: "standard",
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  entrypointPath: `.ghostprov-v2/revisions/r1/${"b".repeat(64)}/${"a".repeat(64)}/sidecar.bundle.js`,
  manifest: {
    path: `.ghostprov-v2/revisions/r1/${"b".repeat(64)}/${"a".repeat(64)}/manifest.json`,
    logicalPath: "manifest.json",
    content: "{}\n",
    sha256: "c".repeat(64),
  },
  obsoletePaths: Object.freeze([]),
  files: [],
};

class ManualClock implements Clock {
  value = 0;
  waiters: Array<{
    deadline: number;
    resolve: () => void;
    reject: (error: unknown) => void;
    signal?: AbortSignal;
    onAbort?: () => void;
  }> = [];
  now(): number { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return this.waitUntil(this.value + ms, signal);
  }
  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const waiter: (typeof this.waiters)[number] = { deadline, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new DOMException("aborted", "AbortError"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }
  async advanceBy(ms: number): Promise<void> {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.deadline > this.value) continue;
      this.waiters.splice(this.waiters.indexOf(waiter), 1);
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve();
    }
    for (let index = 0; index < 10; index++) await Promise.resolve();
  }
}

class FakeHeartbeat implements V2RuntimeHeartbeat {
  started = false;
  stops = 0;
  constructor(readonly order: string[], readonly clock: ManualClock) {}
  start(): void { this.started = true; this.order.push("heartbeat"); }
  async stop(): Promise<void> { this.stops++; this.started = false; }
  snapshot() { return { lastSuccessAtMs: this.clock.now() }; }
}

class FakeYDoc implements V2RuntimeYDoc {
  constructor(readonly order: string[]) {}
  async connect(): Promise<void> { this.order.push("ydoc"); }
  async destroy(): Promise<void> {}
}

class FakeAwareness implements AwarenessChannel<HostFrame, SidecarFrame> {
  readonly projectId = PROJECT;
  readonly localPeerId = "host";
  rotations = 0;
  readonly rotationTokens: string[] = [];
  readonly failureListeners = new Set<(error: Error) => void>();
  rotationError: Error | null = null;
  async connect(): Promise<void> {}
  publish(_envelope: AwarenessEnvelope<HostFrame>): void {}
  clear(): void {}
  subscribe(): () => void { return () => {}; }
  peerIds(): readonly string[] { return []; }
  async rotateCredential(ySweet: YSweetEnvelope): Promise<void> {
    this.rotations++;
    this.rotationTokens.push(ySweet.token.reveal());
    if (this.rotationError) throw this.rotationError;
  }
  onConnectionFailure(listener: (error: Error) => void): () => void {
    this.failureListeners.add(listener);
    return () => this.failureListeners.delete(listener);
  }
  fail(error = new Error("awareness disconnected")): void {
    for (const listener of this.failureListeners) listener(error);
  }
  async close(): Promise<void> {}
}

class FakeBootstrap implements V2RuntimeBootstrap {
  drains = 0;
  forceStops = 0;
  ensureReadyWait: ((signal?: AbortSignal) => Promise<void>) | null = null;
  constructor(
    readonly order: string[],
    readonly descriptor: V2ReadyDescriptor,
  ) {}
  async ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor> {
    this.order.push("bootstrap");
    await this.ensureReadyWait?.(signal);
    return this.descriptor;
  }
  async drainAndStop(): Promise<void> { this.drains++; }
  async forceStop(): Promise<void> { this.forceStops++; }
  async sealCredentialRotation(): Promise<string> {
    return JSON.stringify({ encrypted: "rotation" });
  }
}

class FakeTransport implements V2RuntimeTransport {
  snapshotValue: V2TransportSnapshot;
  readonly pending: Array<{
    resolve: (value: Record<string, unknown>) => void;
    reject: (error: unknown) => void;
  }> = [];
  hostCeilings: number[] = [];
  rotations = 0;
  awarenessResumes = 0;
  drains = 0;
  forceStops = 0;
  closes = 0;
  livenessProbes = 0;
  recoverySuspends = 0;
  recoveryAdopts = 0;
  recoveryFailures = 0;
  recoveryAdoptError: Error | null = null;
  recoveryAdoptPromise: Promise<void> | null = null;
  connectPromise: Promise<void> | null = null;
  rotationPromise: Promise<void> | null = null;
  connectCalls = 0;
  binding: ProtocolBinding | null = null;
  expectedInstanceId = "sidecar-instance-1";
  probeLiveness?: (signal?: AbortSignal) => Promise<{ rttMs: number; sentAtMs: number }>;

  constructor(
    readonly order: string[],
    snapshot: Partial<V2TransportSnapshot> = {},
  ) {
    this.snapshotValue = {
      connected: false,
      committed: 0,
      active: 0,
      queued: 0,
      quarantined: 0,
      credits: 128,
      retransmissions: 0,
      awarenessRttMs: 7,
      ...snapshot,
    };
  }
  async connect(): Promise<void> {
    this.order.push("transport");
    this.connectCalls++;
    if (this.connectPromise) await this.connectPromise;
    this.snapshotValue.connected = true;
  }
  async run(
    _body?: Record<string, unknown>,
    _signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.snapshotValue.active++;
    this.snapshotValue.committed++;
    options?.onAdmissionReflected?.();
    options?.onExecutionRiskReflected?.();
    const final = new Promise<Record<string, unknown>>((resolve, reject) => {
      this.pending.push({ resolve, reject });
    }).finally(() => {
      this.snapshotValue.active--;
      this.snapshotValue.committed--;
    });
    return {
      async *events() {},
      final,
      stats: { bytes: 32, firstByteMs: 5, frames: 1 },
    };
  }
  async drain(): Promise<void> { this.drains++; this.snapshotValue.connected = false; }
  async forceStop(): Promise<void> { this.forceStops++; this.snapshotValue.connected = false; }
  async close(): Promise<void> { this.closes++; this.snapshotValue.connected = false; }
  snapshot(): V2TransportSnapshot { return { ...this.snapshotValue }; }
  setHostCeiling(value: number): void { this.hostCeilings.push(value); }
  async rotateCredential(): Promise<void> {
    this.rotations++;
    if (this.rotationPromise) await this.rotationPromise;
  }
  async resumeAwareness(): Promise<void> { this.awarenessResumes++; }
  async suspendSidecarRecovery(): Promise<V2SidecarRecoveryToken> {
    this.recoverySuspends++;
    if (!this.binding) throw new Error("missing fake binding");
    return {
      id: `capsule-${this.recoverySuspends}`,
      binding: this.binding,
      previousInstanceId: this.expectedInstanceId,
    };
  }
  async adoptRecoveredSidecar(): Promise<void> {
    this.recoveryAdopts++;
    if (this.recoveryAdoptError) throw this.recoveryAdoptError;
    if (this.recoveryAdoptPromise) await this.recoveryAdoptPromise;
  }
  failSidecarRecovery(): void { this.recoveryFailures++; }
}

function stalledHostDiagnostics(): V2TransportDiagnostics {
  return {
    windowFrames: 1,
    windowBytes: 764,
    schedulerFrames: 2,
    schedulerBytes: 324,
    schedulerStreams: 2,
    oldestQueuedFrameMs: 60_000,
    ackProgressAgeMs: 60_000,
    retransmissionAttempt: 16,
    peakRetransmissionAttempt: 16,
    retransmissionsTotal: 16,
    publicationsTotal: 32,
    publishedBytesTotal: 64 * 1024,
    admittedUncommitted: 0,
    roundTripEwmaMs: 250,
    pacedPublicationsTotal: 0,
    urgentPublicationsTotal: 32,
    logicalEventsTotal: 0,
    responseBatchFramesTotal: 0,
    batchedEventsTotal: 0,
    dataPublicationsTotal: 16,
    controlPublicationsTotal: 16,
    dataPublishedBytesTotal: 48 * 1024,
    controlPublishedBytesTotal: 16 * 1024,
    flightCompletionsTotal: 1,
    adaptiveCapacity: 0,
    capacityIncreasesTotal: 0,
    capacityReductionsTotal: 1,
    unadmittedTimeoutsTotal: 0,
    acknowledgedGoodputBytesPerSecond: null,
    uniqueAcknowledgedBytesTotal: 1024,
    retransmittedBytesTotal: 16 * 1024,
    stallEventsTotal: 1,
  };
}

class FakeGovernor implements V2RuntimeGovernor {
  capacityHintCalls = 0;
  constructor(public ceiling: number) {}
  capacityHint(): number { this.capacityHintCalls++; return this.ceiling; }
  snapshot() {
    return {
      reservationCapacityBytes: 1024,
      reservedBytes: 0,
      paused: false,
    };
  }
}

function credentials(clock: ManualClock, expiresInMs = 600_000): ProvisionedRuntimeCredentials {
  const sandbox = parseProvisionedSandbox({
    url: "https://prism.test/s/sandboxes/proxy",
    token: "sandbox-token",
    sandbox_session_id: "sandbox-session-1",
  });
  return {
    projectId: PROJECT,
    sandbox,
    resource: parseResourceCredential({
      access_token: "resource-token",
      expires_at: (clock.now() + expiresInMs) / 1_000,
      scopes: ["project_files:read"],
      resources_base_url: "https://prism.test/s/sandbox-resources",
    }, clock.now()),
    ySweet: parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${PROJECT}/ws`,
      baseUrl: `https://prism.test/y/d/${PROJECT}`,
      docId: PROJECT,
      token: "y-token",
      authorization: "full",
    }),
    provisionedAtMs: clock.now(),
  };
}

function runtimeHarness(options: {
  sidecarCapacity?: number;
  credits?: number;
  governorCeiling?: number;
  governor?: V2RuntimeGovernor;
  resourceExpiresInMs?: number;
  resourceRefreshError?: Error;
  livenessOutcomes?: Array<Error | null>;
  authSandbox?: ProvisionedSandbox;
  rotateSameSession?: boolean;
  initialSandbox?: ProvisionedSandbox;
  stopTimeoutMs?: number;
  sidecarRecoveryTimeoutMs?: number;
  sidecarCommitStallMs?: number;
  sidecarStallSampleThreshold?: number;
  enableR2Recovery?: boolean;
  recoveryAdoptError?: Error;
  heartbeatSessionId?: string;
  recoveryAdoptPending?: boolean;
  recoveryBootstrapDelayMs?: number;
  recoveryConnectPending?: boolean;
  gatewayHeartbeatOutcomes?: V2RuntimeHeartbeatResult[];
  initialRotationPending?: boolean;
} = {}) {
  const order: string[] = [];
  const clock = new ManualClock();
  let current = credentials(clock, options.resourceExpiresInMs ?? 600_000);
  let refreshes = 0;
  let ySweetMints = 0;
  let acquisitions = 0;
  let acquiredProvisions = 0;
  let provisions = 0;
  let gatewayHeartbeats = 0;
  let gatewayRotations = 0;
  let refreshedSandboxToken: string | null = null;
  const credentialOutcomes: string[] = [];
  const sandboxFailures: ProvisionedSandbox[] = [];
  const sandboxReady: ProvisionedSandbox[] = [];
  const awarenessCredentials: Array<{ sandboxToken: string; ySweetToken: string }> = [];
  const bootstrapCredentials: Array<{ sandboxToken: string; ySweetToken: string }> = [];
  const heartbeatOutcomes = [...(options.gatewayHeartbeatOutcomes ?? [])];
  let releaseInitialRotation: (() => void) | null = null;
  let releaseRecoveryAdopt: (() => void) | null = null;
  const initialRotationPromise = options.initialRotationPending
    ? new Promise<void>(resolve => { releaseInitialRotation = resolve; })
    : null;
  let heartbeatCallbacks: V2RuntimeHeartbeatCallbacks | null = null;
  const provisioner = {
    async provision(
      _projectId: typeof PROJECT,
      _signal?: AbortSignal,
      hooks: ProvisioningHooks = {},
    ) {
      provisions++;
      order.push("provision");
      await hooks.onSandboxAcquired?.(current.sandbox);
      return current;
    },
    async acquireSandbox() {
      acquisitions++;
      return options.authSandbox ?? current.sandbox;
    },
    async provisionAcquired(
      _projectId: typeof PROJECT,
      sandbox: ProvisionedSandbox,
      _signal?: AbortSignal,
      hooks: ProvisioningHooks = {},
    ) {
      acquiredProvisions++;
      order.push("provision-acquired");
      await hooks.onSandboxAcquired?.(sandbox);
      return { ...current, sandbox };
    },
    async refreshResources(value: ProvisionedRuntimeCredentials) {
      refreshes++;
      refreshedSandboxToken = value.sandbox.token.reveal();
      if (options.resourceRefreshError) throw options.resourceRefreshError;
      current = {
        ...value,
        resource: parseResourceCredential({
          access_token: `resource-token-${refreshes}`,
          expires_at: (clock.now() + 600_000) / 1_000,
          scopes: ["project_files:read"],
          resources_base_url: "https://prism.test/s/sandbox-resources",
        }, clock.now()),
      };
      return current.resource;
    },
    async mintYSweet() {
      ySweetMints++;
      return parseYSweetEnvelope({
        url: `wss://prism.test/y/d/${PROJECT}/ws`,
        baseUrl: `https://prism.test/y/d/${PROJECT}`,
        docId: PROJECT,
        token: `y-token-${ySweetMints}`,
        authorization: "full",
      });
    },
    reportSandboxFailure(_projectId: typeof PROJECT, sandbox: ProvisionedSandbox) {
      sandboxFailures.push(sandbox);
    },
    reportSandboxReady(_projectId: typeof PROJECT, sandbox: ProvisionedSandbox) {
      sandboxReady.push(sandbox);
    },
  };
  const heartbeat = new FakeHeartbeat(order, clock);
  const ydoc = new FakeYDoc(order);
  const awareness = new FakeAwareness();
  const awarenesses = [awareness];
  let awarenessCalls = 0;
  const assets = options.enableR2Recovery
    ? { ...ASSETS, revision: "r2" as const, protocolRevision: "r2" as const }
    : ASSETS;
  const descriptor: V2ReadyDescriptor = {
    instanceId: "sidecar-instance-1",
    releaseRevision: options.enableR2Recovery ? "r2" : "r1",
    protocolRevision: options.enableR2Recovery ? "r2" : "r1",
    launchNonce: "runtime-launch-placeholder",
    buildHash: ASSETS.buildHash,
    schemaHash: ASSETS.schemaHash,
    leaderGeneration: 4,
    capacity: options.sidecarCapacity ?? 128,
  };
  const bootstrap = new FakeBootstrap(order, descriptor);
  const bootstraps = [bootstrap];
  const transport = new FakeTransport(order, { credits: options.credits ?? 128 });
  transport.rotationPromise = initialRotationPromise;
  const transports = [transport];
  if (options.livenessOutcomes) {
    const outcomes = [...options.livenessOutcomes];
    transport.probeLiveness = async signal => {
      if (signal?.aborted) throw new DOMException("aborted", "AbortError");
      transport.livenessProbes++;
      const outcome = outcomes.shift() ?? null;
      if (outcome) throw outcome;
      return { rttMs: 1, sentAtMs: clock.now() };
    };
  }
  const governor = options.governor ?? new FakeGovernor(options.governorCeiling ?? 128);
  const runtime = new V2SandboxRuntime({
    projectId: PROJECT,
    workerIndex: 1,
    leaderGeneration: 4,
    provisioning: provisioner,
    governor,
    clock,
    assets,
    transportReservationBytes: 16 * 1024 * 1024,
    ySweetRefreshMs: 300_000,
    stopTimeoutMs: options.stopTimeoutMs,
    sidecarRecoveryTimeoutMs: options.sidecarRecoveryTimeoutMs,
    sidecarCommitStallMs: options.sidecarCommitStallMs,
    sidecarStallSampleThreshold: options.sidecarStallSampleThreshold,
    initialSandbox: options.initialSandbox,
    onCredentialRefresh: outcome => credentialOutcomes.push(outcome),
    createGateway: (_sandbox: ProvisionedSandbox) => ({
      heartbeat: async () => {
        gatewayHeartbeats++;
        return heartbeatOutcomes.shift() ?? {
          ok: true,
          status: 200,
          durationMs: 1,
          sessionId: options.heartbeatSessionId ?? "sandbox-session-1",
        };
      },
      rotateSameSession: () => {
        gatewayRotations++;
        return options.rotateSameSession ?? false;
      },
    }),
    createHeartbeat: (_gateway, callbacks) => {
      heartbeatCallbacks = callbacks;
      return heartbeat;
    },
    createYDoc: () => ydoc,
    createAwareness: (_binding: ProtocolBinding, runtimeCredentials) => {
      awarenessCredentials.push({
        sandboxToken: runtimeCredentials.sandbox.token.reveal(),
        ySweetToken: runtimeCredentials.ySweet.token.reveal(),
      });
      if (awarenessCalls++ === 0) return awareness;
      const next = new FakeAwareness();
      awarenesses.push(next);
      return next;
    },
    createBootstrap: input => {
      bootstrapCredentials.push({
        sandboxToken: input.credentials.sandbox.token.reveal(),
        ySweetToken: input.credentials.ySweet.token.reveal(),
      });
      if (bootstraps.length === 1 && !transport.binding) {
        (descriptor as { launchNonce: string }).launchNonce = input.binding.launchNonce;
        return bootstrap;
      }
      const nextDescriptor: V2ReadyDescriptor = {
        ...descriptor,
        instanceId: `sidecar-instance-${bootstraps.length + 1}`,
        launchNonce: input.binding.launchNonce,
      };
      const next = new FakeBootstrap(order, nextDescriptor);
      if (options.recoveryBootstrapDelayMs) {
        next.ensureReadyWait = signal => clock.sleep(options.recoveryBootstrapDelayMs!, signal);
      }
      bootstraps.push(next);
      return next;
    },
    createTransport: input => {
      if (!transport.binding) {
        transport.binding = input.binding;
        transport.expectedInstanceId = input.expectedSidecarInstanceId;
        return transport;
      }
      const next = new FakeTransport(order, { credits: options.credits ?? 128 });
      next.binding = input.binding;
      next.expectedInstanceId = input.expectedSidecarInstanceId;
      next.probeLiveness = async () => {
        next.livenessProbes++;
        return { rttMs: 1, sentAtMs: clock.now() };
      };
      next.recoveryAdoptError = options.recoveryAdoptError ?? null;
      if (options.recoveryAdoptPending) {
        next.recoveryAdoptPromise = new Promise<void>(resolve => {
          releaseRecoveryAdopt = resolve;
        });
      }
      if (options.recoveryConnectPending) next.connectPromise = new Promise(() => undefined);
      transports.push(next);
      return next;
    },
  });
  return {
    runtime,
    order,
    clock,
    heartbeat,
    transport,
    awareness,
    governor,
    bootstrap,
    bootstraps,
    awarenesses,
    transports,
    refreshes: () => refreshes,
    ySweetMints: () => ySweetMints,
    acquisitions: () => acquisitions,
    provisions: () => provisions,
    acquiredProvisions: () => acquiredProvisions,
    gatewayHeartbeats: () => gatewayHeartbeats,
    gatewayRotations: () => gatewayRotations,
    refreshedSandboxToken: () => refreshedSandboxToken,
    credentialOutcomes,
    sandboxFailures,
    sandboxReady,
    awarenessCredentials,
    bootstrapCredentials,
    releaseInitialRotation: () => releaseInitialRotation?.(),
    releaseRecoveryAdopt: () => releaseRecoveryAdopt?.(),
    heartbeatSuccess: () => heartbeatCallbacks?.onSuccess({
      ok: true,
      status: 200,
      durationMs: 1,
      sessionId: "sandbox-session-1",
    }),
    heartbeatExpired: (status = 401) => heartbeatCallbacks?.onExpired(status),
    heartbeatAuthenticationFailure: (status = 401, signal = new AbortController().signal) =>
      heartbeatCallbacks?.onAuthenticationFailure(status, signal),
    heartbeatTransientFailure: (status: number | null = null) =>
      heartbeatCallbacks?.onTransientFailure(status),
    heartbeatUnreachable: (status: number | null = null) =>
      heartbeatCallbacks?.onUnreachable(status),
  };
}

describe("V2SandboxRuntime", () => {
  it("reports ready and repeatedly unhealthy sandbox identities to provisioning", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    expect(harness.sandboxReady.map(sandbox => String(sandbox.sessionId))).toEqual([
      "sandbox-session-1",
    ]);

    harness.heartbeatUnreachable();
    expect(harness.sandboxFailures.map(sandbox => String(sandbox.sessionId))).toEqual([
      "sandbox-session-1",
    ]);
    await harness.runtime.stop();
  });

  it("reports a bootstrap timeout against the acquired sandbox session", async () => {
    const harness = runtimeHarness();
    harness.bootstrap.ensureReadyWait = async () => {
      const error = new Error("synthetic bootstrap timeout") as Error & { kind: string };
      error.name = "V2BootstrapError";
      error.kind = "timeout";
      throw error;
    };

    await expect(harness.runtime.start()).rejects.toMatchObject({
      name: "V2BootstrapError",
      kind: "timeout",
    });
    expect(harness.sandboxReady).toHaveLength(0);
    expect(harness.sandboxFailures.map(sandbox => String(sandbox.sessionId))).toEqual([
      "sandbox-session-1",
    ]);
    await harness.runtime.stop();
  });

  it("forwards exact admission and execution-risk callbacks to transport", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    let reflected = 0;
    let executionRisks = 0;
    const handle = await harness.runtime.run(
      { model: "gpt-5.6-sol", input: "handoff" },
      undefined,
      {
        onAdmissionReflected: () => { reflected++; },
        onExecutionRiskReflected: () => { executionRisks++; },
      },
    );
    expect(reflected).toBe(1);
    expect(executionRisks).toBe(1);
    harness.transport.pending.shift()?.resolve({ id: "resp_handoff", status: "completed" });
    await handle.final;
    await harness.runtime.stop();
  });

  it("classifies lifecycle guard failures as safe local pre-commit outcomes", async () => {
    const notReady = runtimeHarness();
    await expect(notReady.runtime.run({ model: "gpt-5.6-sol", input: "early" }))
      .rejects.toMatchObject({
        name: "V2PreCommitError",
        committed: false,
        code: "not_ready",
        message: "V2 sandbox runtime is not ready",
      });
    expect(notReady.transport.pending).toHaveLength(0);

    const recovering = runtimeHarness();
    await recovering.runtime.start();
    recovering.heartbeatExpired(401);
    await expect(recovering.runtime.run({ model: "gpt-5.6-sol", input: "recover" }))
      .rejects.toMatchObject({
        name: "V2PreCommitError",
        committed: false,
        code: "recovering",
        message: "V2 sandbox runtime is recovering",
      });
    expect(recovering.transport.pending).toHaveLength(0);
    await recovering.runtime.stop();

    const draining = runtimeHarness();
    await draining.runtime.start();
    const drained = draining.runtime.drain("test lifecycle guard");
    await expect(draining.runtime.run({ model: "gpt-5.6-sol", input: "late" }))
      .rejects.toMatchObject({
        name: "V2PreCommitError",
        committed: false,
        code: "draining",
        message: "V2 sandbox runtime is draining",
      });
    expect(draining.transport.pending).toHaveLength(0);
    await drained;
  });

  it("does not relabel transport committed failures as safe pre-commit guards", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    const committed = new V2CommittedError(
      "provider request already committed",
      "rate_limit_exceeded",
      false,
      { retryAfterMs: 17 },
    );
    harness.transport.run = async () => { throw committed; };

    await expect(harness.runtime.run({ model: "gpt-5.6-sol", input: "committed" }))
      .rejects.toBe(committed);
    expect(committed).not.toBeInstanceOf(V2PreCommitError);
    await harness.runtime.stop();
  });

  it("dynamically forwards prelude and late rate metadata from the transport handle", async () => {
    const harness = runtimeHarness();
    const realHandle = new V2RunHandle({
      maxBufferedBytes: 4_096,
      expectUpstreamPrelude: true,
    });
    harness.transport.run = async () => realHandle;
    await harness.runtime.start();

    const wrapped = await harness.runtime.run({ model: "gpt-5.6-sol", input: "metadata" });
    realHandle.acceptUpstreamPrelude({
      status: 200,
      contentType: "text/event-stream",
      upstreamRequestId: "req_runtime_prelude",
    });
    await expect(wrapped.upstreamPrelude).resolves.toMatchObject({
      upstreamRequestId: "req_runtime_prelude",
    });
    expect(wrapped.rateLimit).toBeUndefined();
    realHandle.settle(
      { id: "resp_runtime_metadata", status: "failed" },
      { retryAfterMs: 914, remainingTokens: 0 },
    );
    await expect(wrapped.final).resolves.toMatchObject({ id: "resp_runtime_metadata" });
    expect(wrapped.rateLimit).toEqual({ retryAfterMs: 914, remainingTokens: 0 });
    await harness.runtime.stop();
  });

  it("keeps resolved streamed 429 and rejected event:error routing inputs neutral", async () => {
    const resolved = runtimeHarness();
    const rejected = runtimeHarness();
    await resolved.runtime.start();
    await rejected.runtime.start();

    const seedUnhealthy = async (harness: ReturnType<typeof runtimeHarness>) => {
      const handle = await harness.runtime.run({ model: "gpt-5.6-sol", input: "seed" });
      harness.transport.pending.at(-1)!.reject(new V2CommittedError(
        "ambiguous transport reset",
        "transport",
        true,
      ));
      await expect(handle.final).rejects.toMatchObject({ code: "transport" });
      expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.2);
    };
    await seedUnhealthy(resolved);
    await seedUnhealthy(rejected);

    const realHandle = new V2RunHandle({ maxBufferedBytes: 4_096 });
    resolved.transport.run = async () => realHandle;
    const resolvedHandle = await resolved.runtime.run({ model: "gpt-5.6-sol", input: "resolved" });
    const final = {
      id: "resp_streamed_429",
      status: "failed",
      error: {
        type: "rate_limit_error",
        code: "rate_limit_exceeded",
        message: "Rate limit reached. Please try again in 136ms.",
      },
    };
    realHandle.settle(final, { retryAfterMs: 136, remainingTokens: 0 });
    await expect(resolvedHandle.final).resolves.toBe(final);
    expect(resolvedHandle.rateLimit).toEqual({ retryAfterMs: 136, remainingTokens: 0 });

    const evidenceOnly = new V2RunHandle({ maxBufferedBytes: 4_096 });
    resolved.transport.run = async () => evidenceOnly;
    const evidenceWrapped = await resolved.runtime.run({
      model: "gpt-5.6-sol",
      input: "evidence-only",
    });
    const incomplete = {
      id: "resp_streamed_incomplete_429",
      status: "incomplete",
      error: {
        code: "rate_limit_exceeded",
        message: "Rate limit reached. Please try again in 136ms.",
      },
    };
    evidenceOnly.settle(incomplete);
    await expect(evidenceWrapped.final).resolves.toBe(incomplete);

    const rejectedHandle = await rejected.runtime.run({ model: "gpt-5.6-sol", input: "rejected" });
    rejected.transport.pending.at(-1)!.reject(new V2CommittedError(
      "Rate limit reached. Please try again in 136ms.",
      "rate_limit_exceeded",
      false,
      { retryAfterMs: 136, remainingTokens: 0 },
    ));
    await expect(rejectedHandle.final).rejects.toMatchObject({ code: "rate_limit_exceeded" });

    const resolvedSnapshot = resolved.runtime.snapshot();
    const rejectedSnapshot = rejected.runtime.snapshot();
    expect(resolvedSnapshot.completionEwmaMs).toBe(10_000);
    expect(rejectedSnapshot.completionEwmaMs).toBe(10_000);
    expect(resolvedSnapshot.recentFailureRate).toBeCloseTo(
      rejectedSnapshot.recentFailureRate,
      8,
    );

    const workload: WorkloadDescriptor = {
      class: "text-small",
      requestBytes: 1_024,
      reservedBytes: 8 * 1024 ** 2,
      cpuPermits: 0,
      stream: true,
    };
    const candidate = (
      snapshot: typeof resolvedSnapshot,
      suffix: string,
    ): RouteCandidate => ({
      ...snapshot,
      projectId: projectId(`00000000-0000-4000-8000-${suffix.padStart(12, "0")}`),
      reserved: 0,
    });
    const left = chooseRuntime(workload, [candidate(resolvedSnapshot, "1")], {
      next: () => 0,
      uuid: () => "left",
    });
    const right = chooseRuntime(workload, [candidate(rejectedSnapshot, "2")], {
      next: () => 0,
      uuid: () => "right",
    });
    expect(left?.predictedCompletionMs).toBe(right?.predictedCompletionMs);

    await resolved.runtime.stop();
    await rejected.runtime.stop();
  });

  it("learns successful completion despite non-exhausted rate-limit budget headers", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    let resolve!: (value: Record<string, unknown>) => void;
    const final = new Promise<Record<string, unknown>>(done => { resolve = done; });
    harness.transport.run = async () => ({
      async *events() {},
      final,
      // Exhausted budget metadata affects the next admission decision, not the
      // validity of this successful request's completion sample.
      rateLimit: { limitTokens: 40_000_000, remainingTokens: 0 },
      stats: { bytes: 32, firstByteMs: 5, frames: 1 },
    });

    const handle = await harness.runtime.run({ model: "gpt-5.6-sol", input: "success" });
    await harness.clock.advanceBy(6_000);
    resolve({ id: "resp_success", status: "completed" });
    await expect(handle.final).resolves.toMatchObject({ status: "completed" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBeCloseTo(9_200, 5);
    await harness.runtime.stop();
  });

  it("reaches ready only after provision, heartbeat, YDoc, bootstrap, and transport", async () => {
    const harness = runtimeHarness();

    await harness.runtime.start();

    expect(harness.order).toEqual(["provision", "heartbeat", "ydoc", "bootstrap", "transport"]);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      protocolRevision: "r1",
      buildHash: ASSETS.buildHash,
      schemaHash: ASSETS.schemaHash,
      capacity: 128,
      active: 0,
      quarantined: 0,
    });
    await harness.runtime.stop();
  });

  it("emits heartbeat successes without masking transient failures", async () => {
    const harness = runtimeHarness();
    let changes = 0;
    harness.runtime.subscribe(() => { changes++; });
    await harness.runtime.start();
    const afterStart = changes;

    harness.heartbeatTransientFailure(502);
    expect(changes).toBe(afterStart + 1);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);

    harness.heartbeatSuccess();
    expect(changes).toBe(afterStart + 2);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);
    await harness.runtime.stop();
  });

  it("becomes synchronously unroutable on heartbeat expiry without draining", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    harness.heartbeatExpired(401);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "heartbeat_expired",
        status: 401,
        operation: "heartbeat",
        message: "V2 sandbox heartbeat credentials expired (401)",
      },
    });
    expect(harness.transport.drains).toBe(0);
    expect(harness.bootstrap.drains).toBe(0);

    harness.heartbeatUnreachable(502);
    expect(harness.runtime.snapshot().failure?.kind).toBe("heartbeat_expired");
    await harness.runtime.stop();
  });

  it("rotates a proven same-session sandbox token without replacing the runtime", async () => {
    const candidate = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "sandbox-token-2",
      sandbox_session_id: "sandbox-session-1",
    });
    const harness = runtimeHarness({
      authSandbox: candidate,
      rotateSameSession: true,
      resourceExpiresInMs: 300_001,
    });
    await harness.runtime.start();

    await expect(harness.heartbeatAuthenticationFailure()).resolves.toBe("retry");
    expect(harness.acquisitions()).toBe(1);
    expect(harness.runtime.takeReplacementSandbox()).toBeNull();
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0 });
    harness.heartbeatSuccess();
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.credentialOutcomes).toEqual(["updated_in_place"]);
    await harness.clock.advanceBy(1);
    expect(harness.refreshedSandboxToken()).toBe("sandbox-token-2");
    await harness.runtime.stop();
  });

  it("hands a changed sandbox session to the next generation exactly once", async () => {
    const candidate = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "sandbox-token-2",
      sandbox_session_id: "sandbox-session-2",
    });
    const harness = runtimeHarness({ authSandbox: candidate, rotateSameSession: false });
    await harness.runtime.start();

    await expect(harness.heartbeatAuthenticationFailure()).resolves.toBe("expired");
    expect(harness.runtime.takeReplacementSandbox()).toBe(candidate);
    expect(harness.credentialOutcomes).toEqual(["replacement_required"]);
    expect(harness.runtime.takeReplacementSandbox()).toBeNull();
    await harness.runtime.stop();
  });

  it("bootstraps a pre-acquired sandbox without another acquisition", async () => {
    const seed = parseProvisionedSandbox({
      url: "https://prism.test/s/sandboxes/proxy",
      token: "seed-token",
      sandbox_session_id: "sandbox-session-2",
    });
    const harness = runtimeHarness({ initialSandbox: seed });

    await harness.runtime.start();

    expect(harness.provisions()).toBe(0);
    expect(harness.acquiredProvisions()).toBe(1);
    await harness.runtime.stop();
  });

  it("becomes synchronously unroutable when heartbeat failures reach unreachable", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    harness.heartbeatUnreachable(503);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "heartbeat_unreachable",
        status: 503,
        operation: "heartbeat",
        message: "V2 sandbox heartbeat became unreachable (503)",
      },
    });
    expect(harness.transport.drains).toBe(0);
    expect(harness.bootstrap.drains).toBe(0);

    await harness.runtime.stop();
    harness.heartbeatExpired(410);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it("fails closed with a sanitized lifecycle failure when resource refresh expires", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1,
      resourceRefreshError: new DOMException(
        "authorization=Bearer super-secret-resource-token",
        "AbortError",
      ),
    });
    await harness.runtime.start();
    for (let index = 0; index < 10; index++) await Promise.resolve();

    expect(harness.refreshes()).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");

    await harness.clock.advanceBy(30_000);

    expect(harness.refreshes()).toBe(2);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      available: 0,
      failure: {
        name: "V2RuntimeLifecycleFailure",
        kind: "resource_credential_expired",
        status: null,
        operation: "refresh_resources",
        message: "V2 sandbox resource credential expired after refresh failed",
      },
    });
    expect(JSON.stringify(harness.runtime.snapshot().failure)).not.toContain("super-secret");
    expect(harness.transport.drains).toBe(0);
    await harness.runtime.stop();
  });

  it("gates readiness on a bound sidecar liveness proof", async () => {
    const harness = runtimeHarness({ livenessOutcomes: [null] });

    await harness.runtime.start();

    expect(harness.transport.livenessProbes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.runtime.stop();
  });

  it("removes capacity on a missed sidecar probe, heals, then fails after three misses", async () => {
    const miss = () => new Error("sidecar pong timed out");
    const harness = runtimeHarness({
      livenessOutcomes: [null, miss(), null, miss(), miss(), miss()],
    });
    await harness.runtime.start();

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0, available: 0 });

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().capacity).toBe(128);

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: {
        kind: "sidecar_unreachable",
        operation: "sidecar_liveness",
      },
    });
    await harness.runtime.stop();
  });

  it("relaunches one R2 sidecar in the same sandbox session without provisioning", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
    });
    await harness.runtime.start();
    const originalNonce = harness.transport.binding!.launchNonce;

    for (let attempt = 0; attempt < 3; attempt++) {
      await harness.clock.advanceBy(10_000);
    }
    for (let turn = 0; turn < 200 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }
    expect(harness.runtime.snapshot().state).toBe("ready");

    expect(harness.provisions()).toBe(1);
    expect(harness.acquisitions()).toBe(0);
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.bootstrap.forceStops).toBe(1);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    expect(harness.awarenesses).toHaveLength(2);
    expect(harness.transports[1]!.binding).toMatchObject({
      projectId: PROJECT,
      sessionId: "sandbox-session-1",
      leaderGeneration: 4,
      protocolRevision: "r2",
    });
    expect(harness.transports[1]!.binding!.launchNonce).not.toBe(originalNonce);
    await harness.runtime.stop();
  });

  it("relaunches an R2 sidecar when reliable host frames stall after request state settles", async () => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
    });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = {
      host: stalledHostDiagnostics(),
      sidecar: null,
    };

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0, available: 0 });
    expect(harness.transports).toHaveLength(1);
    await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 200 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }

    expect(harness.transport.livenessProbes).toBe(1);
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.transports).toHaveLength(2);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.runtime.stop();
  });

  it("restores routing when one reliable-stall sample makes progress", async () => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
    });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = {
      host: stalledHostDiagnostics(),
      sidecar: null,
    };

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0, available: 0 });
    harness.transport.snapshotValue.diagnostics = undefined;
    await harness.clock.advanceBy(10_000);

    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    expect(harness.runtime.snapshot().capacity).toBe(128);
    await harness.runtime.stop();
  });

  it("does not mistake queued frames behind fresh ACK progress for a stall", async () => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
    });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = {
      host: { ...stalledHostDiagnostics(), ackProgressAgeMs: 1 },
      sidecar: null,
    };

    await harness.clock.advanceBy(20_000);

    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    expect(harness.runtime.snapshot().capacity).toBe(128);
    await harness.runtime.stop();
  });

  it("evaluates replay-time ambiguity against the adopted candidate instead of the retired source", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      recoveryAdoptPending: true,
      sidecarRecoveryTimeoutMs: 100_000,
    });
    await harness.runtime.start();
    const handle = await harness.runtime.run({ model: "gpt-5.6-sol", input: "replay ambiguity" });
    void handle.final.catch(() => undefined);

    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (
      let turn = 0;
      turn < 200 && (harness.transports.length < 2 || harness.transports[1]!.recoveryAdopts < 1);
      turn++
    ) await Promise.resolve();
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);

    harness.transport.snapshotValue.connected = false;
    harness.transport.snapshotValue.quarantined = 1;
    harness.transport.snapshotValue.ambiguousQuarantined = 1;
    harness.transport.snapshotValue.ambiguityLimit = 1;
    harness.transport.pending[0]!.reject(new V2CommittedError(
      "Recovered journal outcome is ambiguous",
      "recovered_ambiguous",
      true,
    ));
    await expect(handle.final).rejects.toMatchObject({ code: "recovered_ambiguous" });
    expect(harness.runtime.snapshot().state).toBe("ready");

    harness.releaseRecoveryAdopt();
    for (let turn = 0; turn < 200 && !harness.transports[1]!.snapshotValue.connected; turn++) {
      await Promise.resolve();
    }
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.transports[1]!.snapshotValue.connected).toBe(true);
    await harness.runtime.stop();
  });

  it("uses a dedicated recovery deadline when a valid R2 bootstrap outlives stop timeout", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      recoveryBootstrapDelayMs: 20,
      stopTimeoutMs: 10,
      sidecarRecoveryTimeoutMs: 100,
    });
    await harness.runtime.start();

    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 200 && harness.bootstraps.length < 2; turn++) {
      await Promise.resolve();
    }
    expect(harness.bootstraps).toHaveLength(2);

    await harness.clock.advanceBy(11);
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.transports).toHaveLength(1);
    expect(harness.transport.recoveryFailures).toBe(0);

    await harness.clock.advanceBy(9);
    for (let turn = 0; turn < 200 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.transports).toHaveLength(2);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    await harness.runtime.stop();
  });

  for (const status of [401, 403] as const) {
    it(`refreshes a same-session sandbox token and retries a ${status} recovery heartbeat`, async () => {
      const failure = new Error("sidecar unavailable");
      const refreshedSandbox = parseProvisionedSandbox({
        url: "https://prism.test/s/sandboxes/proxy",
        token: `sandbox-token-${status}`,
        sandbox_session_id: "sandbox-session-1",
      });
      const harness = runtimeHarness({
        enableR2Recovery: true,
        livenessOutcomes: [null, failure, failure, failure],
        authSandbox: refreshedSandbox,
        rotateSameSession: true,
        gatewayHeartbeatOutcomes: [
          { ok: false, status, durationMs: 1, sessionId: null },
          { ok: true, status: 200, durationMs: 1, sessionId: "sandbox-session-1" },
        ],
      });
      await harness.runtime.start();

      for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 300 && harness.transports.length < 2; turn++) {
        await Promise.resolve();
      }

      expect(harness.runtime.snapshot().state).toBe("ready");
      expect(harness.transports).toHaveLength(2);
      expect(harness.gatewayHeartbeats()).toBeGreaterThanOrEqual(2);
      expect(harness.gatewayRotations()).toBe(1);
      expect(harness.acquisitions()).toBe(1);
      expect(harness.provisions()).toBe(1);
      expect(harness.credentialOutcomes).toEqual(["updated_in_place"]);
      expect(harness.awarenessCredentials.at(-1)?.sandboxToken).toBe(`sandbox-token-${status}`);
      expect(harness.bootstrapCredentials.at(-1)?.sandboxToken).toBe(`sandbox-token-${status}`);
      await harness.runtime.stop();
    });
  }

  it("fences an in-flight Y-Sweet rotation from a replacement R2 stack", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
      resourceExpiresInMs: 1_200_000,
      initialRotationPending: true,
      sidecarRecoveryTimeoutMs: 100_000,
    });
    await harness.runtime.start();

    await harness.clock.advanceBy(300_001);
    for (let turn = 0; turn < 200 && harness.transport.rotations < 1; turn++) {
      await Promise.resolve();
    }
    expect(harness.ySweetMints()).toBe(1);
    expect(harness.transport.rotations).toBe(1);
    expect(harness.awareness.rotations).toBe(0);

    harness.transport.probeLiveness = async signal => {
      if (signal?.aborted) throw new DOMException("aborted", "AbortError");
      harness.transport.livenessProbes++;
      throw failure;
    };
    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 100; turn++) await Promise.resolve();
    expect(harness.transports).toHaveLength(1);
    expect(harness.bootstrapCredentials).toHaveLength(1);

    harness.releaseInitialRotation();
    for (let turn = 0; turn < 300 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }

    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.transports).toHaveLength(2);
    expect(harness.awarenessCredentials.at(-1)?.ySweetToken).toBe("y-token-1");
    expect(harness.bootstrapCredentials.at(-1)?.ySweetToken).toBe("y-token-1");
    expect(harness.awarenesses[0]!.rotations).toBe(1);
    expect(harness.awarenesses[0]!.rotationTokens).toEqual(["y-token-1"]);
    expect(harness.awarenesses[1]!.rotations).toBe(0);
    await harness.runtime.stop();
  });

  it("remints and reconnects Y-Sweet immediately after an Awareness disconnect", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000 });
    await harness.runtime.start();
    expect(harness.runtime.snapshot().capacity).toBe(128);

    harness.awareness.fail(new Error("revoked Y-Sweet connection"));
    expect(harness.runtime.snapshot()).toMatchObject({ capacity: 0, available: 0 });
    for (let turn = 0; turn < 300 && harness.runtime.snapshot().capacity === 0; turn++) {
      await Promise.resolve();
    }

    expect(harness.ySweetMints()).toBe(1);
    expect(harness.transport.rotations).toBe(1);
    expect(harness.transport.awarenessResumes).toBe(1);
    expect(harness.awareness.rotationTokens).toEqual(["y-token-1"]);
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.transports).toHaveLength(1);
    expect(harness.provisions()).toBe(1);
    await harness.runtime.stop();
  });

  it("cleans a created R2 candidate before stop reports the runtime stopped", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      recoveryConnectPending: true,
      stopTimeoutMs: 25,
      sidecarRecoveryTimeoutMs: 100,
    });
    await harness.runtime.start();
    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (
      let turn = 0;
      turn < 200 && (harness.transports.length < 2 || harness.transports[1]!.connectCalls < 1);
      turn++
    ) {
      await Promise.resolve();
    }
    expect(harness.transports).toHaveLength(2);
    expect(harness.transports[1]!.connectCalls).toBe(1);

    let stopSettled = false;
    const stopping = harness.runtime.stop().then(() => { stopSettled = true; });
    for (let turn = 0; turn < 100 && !stopSettled; turn++) await Promise.resolve();
    await harness.clock.advanceBy(25);
    for (let turn = 0; turn < 200 && !stopSettled; turn++) await Promise.resolve();

    expect(stopSettled).toBe(true);
    expect(harness.transports[1]!.forceStops).toBeGreaterThanOrEqual(1);
    expect(harness.bootstraps[1]!.forceStops).toBeGreaterThanOrEqual(1);
    expect(harness.runtime.snapshot().state).toBe("stopped");
    await stopping;
  });

  it("fails the recovery capsule before fresh-generation fallback when adoption fails", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      recoveryAdoptError: new Error("replay barrier failed"),
    });
    await harness.runtime.start();
    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 200 && harness.runtime.snapshot().state !== "failed"; turn++) {
      await Promise.resolve();
    }

    expect(harness.runtime.snapshot().state).toBe("failed");
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.transports[1]!.recoveryFailures).toBe(1);
    expect(harness.provisions()).toBe(1);
    await harness.runtime.stop();
  });

  it("refuses local relaunch when heartbeat proves a different sandbox session", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      heartbeatSessionId: "different-sandbox-session",
    });
    await harness.runtime.start();
    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 200 && harness.runtime.snapshot().state !== "failed"; turn++) {
      await Promise.resolve();
    }

    expect(harness.runtime.snapshot().state).toBe("failed");
    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    await harness.runtime.stop();
  });

  it("bounds a single in-place relaunch whose replay barrier never completes", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, failure, failure, failure],
      recoveryAdoptPending: true,
      stopTimeoutMs: 10,
      sidecarRecoveryTimeoutMs: 10,
    });
    await harness.runtime.start();
    for (let attempt = 0; attempt < 3; attempt++) await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 100 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }
    expect(harness.transports).toHaveLength(2);
    await harness.clock.advanceBy(10);
    for (let turn = 0; turn < 200 && harness.runtime.snapshot().state !== "failed"; turn++) {
      await Promise.resolve();
    }

    expect(harness.runtime.snapshot().state).toBe("failed");
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    expect(harness.transports[1]!.recoveryFailures).toBe(1);
    await harness.runtime.stop();
  });

  it("advertises the lower sidecar and credit ceiling without duplicating global memory", async () => {
    const harness = runtimeHarness({
      sidecarCapacity: 128,
      credits: 90,
      governorCeiling: 64,
    });
    await harness.runtime.start();

    expect(harness.runtime.snapshot()).toMatchObject({
      capacity: 90,
      available: 90,
      ackEwmaMs: 7,
    });
    expect(harness.transport.hostCeilings.at(-1)).toBe(128);
    expect((harness.governor as FakeGovernor).capacityHintCalls).toBe(0);
    await harness.runtime.stop();
  });

  it("does not repeat one global memory hint as each runtime's capacity", async () => {
    const sharedGovernor = new FakeGovernor(0);
    const first = runtimeHarness({
      sidecarCapacity: 30,
      credits: 30,
      governor: sharedGovernor,
    });
    const second = runtimeHarness({
      sidecarCapacity: 30,
      credits: 30,
      governor: sharedGovernor,
    });

    await Promise.all([first.runtime.start(), second.runtime.start()]);
    expect([first.runtime.snapshot().capacity, second.runtime.snapshot().capacity])
      .toEqual([30, 30]);
    expect(first.transport.hostCeilings.at(-1)).toBe(30);
    expect(second.transport.hostCeilings.at(-1)).toBe(30);
    expect(sharedGovernor.capacityHintCalls).toBe(0);
    await Promise.all([first.runtime.stop(), second.runtime.stop()]);
  });

  it("refreshes expiring resources without dropping committed work", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 600_000 });
    await harness.runtime.start();
    const handle = await harness.runtime.run({ model: "gpt-5.6-sol", input: "active" });

    await harness.clock.advanceBy(300_001);

    expect(harness.refreshes()).toBe(1);
    expect(harness.ySweetMints()).toBe(1);
    expect(harness.transport.rotations).toBe(1);
    expect(harness.awareness.rotations).toBe(1);
    expect(harness.runtime.snapshot().active).toBe(1);
    harness.transport.pending[0]!.resolve({ id: "resp_1", status: "completed" });
    await expect(handle.final).resolves.toMatchObject({ id: "resp_1" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBeGreaterThan(0);
    await harness.runtime.stop();
  });

  it("owns a rejected Y-Sweet rotation without an unhandled process rejection", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 600_000 });
    await harness.runtime.start();
    harness.awareness.rotationError = new AwarenessChannelError(
      "Y-Sweet Awareness synchronization timed out",
      "timeout",
    );
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => { unhandled.push(error); };
    process.on("unhandledRejection", onUnhandled);
    try {
      await harness.clock.advanceBy(300_001);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      await Bun.sleep(0);
      expect(unhandled).toEqual([]);
      expect(harness.runtime.snapshot().state).toBe("ready");
    } finally {
      process.off("unhandledRejection", onUnhandled);
      await harness.runtime.stop();
    }
  });

  it("caps extreme completion duration and recovers its routing estimate while idle", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(10_000);

    const extreme = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "extreme-duration",
    });
    await harness.clock.advanceBy(553_855);
    harness.transport.pending[0]!.resolve({ id: "resp_extreme", status: "completed" });
    await expect(extreme.final).resolves.toMatchObject({ id: "resp_extreme" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(20_000);

    await harness.clock.advanceBy(60_000);
    expect(harness.runtime.snapshot().completionEwmaMs).toBe(15_000);

    const ordinary = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "ordinary-duration",
    });
    await harness.clock.advanceBy(6_000);
    harness.transport.pending[1]!.resolve({ id: "resp_ordinary", status: "completed" });
    await expect(ordinary.final).resolves.toMatchObject({ id: "resp_ordinary" });
    expect(harness.runtime.snapshot().completionEwmaMs).toBeCloseTo(12_932.13, 1);
    await harness.runtime.stop();
  });

  it("keeps one hundred cancellations and definitive upstream errors health-neutral", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    for (let index = 0; index < 100; index++) {
      const handle = await harness.runtime.run({
        model: "gpt-5.6-sol",
        input: `cancel-${index}`,
      });
      harness.transport.pending[index]!.reject(new V2CommittedError(
        "V2 request was cancelled after commitment",
        "cancelled",
        true,
      ));
      await expect(handle.final).rejects.toMatchObject({ code: "cancelled" });
    }
    expect(harness.runtime.snapshot().recentFailureRate).toBe(0);

    const definitive = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "definitive-upstream-error",
    });
    harness.transport.pending[100]!.reject(new V2CommittedError(
      "Missing required parameter: 'model'.",
      "missing_required_parameter",
      false,
    ));
    await expect(definitive.final).rejects.toMatchObject({
      code: "missing_required_parameter",
    });
    expect(harness.runtime.snapshot().recentFailureRate).toBe(0);
    await harness.runtime.stop();
  });

  it("ignores cancellation quarantine but fails at the capacity-aware ambiguity limit", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    const cancelled = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "cancelled-at-full-window",
    });
    harness.transport.snapshotValue.quarantined = 4;
    harness.transport.snapshotValue.ambiguousQuarantined = 0;
    harness.transport.snapshotValue.ambiguityLimit = 1;
    harness.transport.pending[0]!.reject(new V2CommittedError(
      "V2 request was cancelled after commitment",
      "cancelled",
      true,
    ));
    await expect(cancelled.final).rejects.toMatchObject({ code: "cancelled" });
    expect(harness.runtime.snapshot().state).toBe("ready");

    const ambiguous = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "permanent-ambiguity",
    });
    harness.transport.snapshotValue.quarantined = 1;
    harness.transport.snapshotValue.ambiguousQuarantined = 1;
    harness.transport.snapshotValue.ambiguityLimit = 1;
    harness.transport.pending[1]!.reject(new V2CommittedError(
      "Transport lost terminal proof",
      "transport",
      true,
    ));
    await expect(ambiguous.final).rejects.toMatchObject({ code: "transport" });
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: { kind: "ambiguity_exhausted" },
    });
    await harness.runtime.stop();
  });

  it("penalizes ambiguous transport failure temporarily and heals on success", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    const failed = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "ambiguous",
    });
    harness.transport.pending[0]!.reject(new V2CommittedError(
      "Transport lost terminal proof",
      "transport",
      true,
    ));
    await expect(failed.final).rejects.toMatchObject({ code: "transport" });
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);

    await harness.clock.advanceBy(60_000);
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.10);

    const healthy = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "healthy",
    });
    harness.transport.pending[1]!.resolve({ id: "resp_healthy", status: "completed" });
    await expect(healthy.final).resolves.toMatchObject({ id: "resp_healthy" });
    expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.08);
    await harness.runtime.stop();
  });

  it("force-stops a ready sidecar exactly once across concurrent stop calls", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    await Promise.all([harness.runtime.stop(), harness.runtime.stop()]);

    expect(harness.transport.forceStops).toBe(1);
    expect(harness.bootstrap.forceStops).toBe(1);
    expect(harness.transport.closes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it("preserves graceful transport drain without invoking force retirement", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    await harness.runtime.drain("graceful shutdown");

    expect(harness.transport.drains).toBe(1);
    expect(harness.transport.forceStops).toBe(0);
    expect(harness.bootstrap.forceStops).toBe(0);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it("retires a ready sidecar when transport connection fails", async () => {
    const harness = runtimeHarness();
    harness.transport.connect = async () => {
      harness.order.push("transport");
      throw new Error("synthetic connect failure");
    };

    await expect(harness.runtime.start()).rejects.toThrow("synthetic connect failure");

    expect(harness.transport.forceStops).toBe(1);
    expect(harness.bootstrap.forceStops).toBe(1);
    expect(harness.transport.closes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("failed");
  });

  it("bounds a hung remote force-stop and still closes the old runtime", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 10 });
    await harness.runtime.start();
    harness.transport.forceStop = async () => new Promise<void>(() => undefined);

    const stopping = harness.runtime.stop();
    let stopped = false;
    void stopping.then(() => { stopped = true; });
    for (let index = 0; index < 10 && !stopped; index++) {
      await harness.clock.advanceBy(10);
    }
    await stopping;

    expect(harness.bootstrap.forceStops).toBe(1);
    expect(harness.transport.closes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it("cannot resurrect after stop while ensureReady ignores cancellation", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 10 });
    let resolveReady!: (descriptor: V2ReadyDescriptor) => void;
    harness.bootstrap.ensureReady = async () => new Promise<V2ReadyDescriptor>(resolve => {
      resolveReady = resolve;
    });
    const starting = harness.runtime.start();
    for (let index = 0; index < 10 && !resolveReady; index++) await Promise.resolve();

    const stopping = harness.runtime.stop();
    let stopped = false;
    void stopping.then(() => { stopped = true; });
    for (let index = 0; index < 10 && !stopped; index++) {
      await harness.clock.advanceBy(10);
    }
    await stopping;
    resolveReady({
      instanceId: "late-sidecar",
      releaseRevision: "r1",
      protocolRevision: "r1",
      launchNonce: "late-launch",
      buildHash: ASSETS.buildHash,
      schemaHash: ASSETS.schemaHash,
      leaderGeneration: 4,
      capacity: 8,
    });
    await starting;

    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect(harness.transport.snapshotValue.connected).toBe(false);
    await expect(harness.runtime.start()).rejects.toThrow("single-use");
  });
});
