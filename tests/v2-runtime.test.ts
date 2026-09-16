import { describe, expect, it, spyOn } from "bun:test";
import * as Y from "yjs";
import { Awareness, applyAwarenessUpdate, encodeAwarenessUpdate } from "y-protocols/awareness";
import { DenoAwarenessPeer as R10DenoAwarenessPeer } from "../sandbox/v2/r10-awareness-peer";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
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
import { projectId, type ProjectId } from "../src/sandbox/types";
import { SupervisedSandboxRuntime, type SandboxSupervisorEvent } from "../src/sandbox/supervised-runtime";
import type { AwarenessChannel, AwarenessPeerUpdate } from "../src/v2/awareness/channel";
import { SplitAwarenessChannelError } from "../src/v2/awareness/split-channel";
import { AwarenessChannelError } from "../src/v2/awareness/y-sweet-channel";
import { V2SupervisorError } from "../src/v2/supervisor/host";
import type { V2ReadyDescriptor } from "../src/v2/bootstrap/controller";
import type {
  AwarenessEnvelope,
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "../src/v2/protocol/shared";
import { base64UrlEncode, utf8Bytes, type ReliableFrame } from "../src/v2/protocol/shared";
import { TranscriptProof } from "../src/v2/protocol/transcript";
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";
import {
  V2TransportLivenessError,
  V2TransportSession,
  type V2TransportSnapshot,
} from "../src/v2/transport/session";
import type { V2TransportDiagnostics } from "../src/v2/diagnostics/types";
import type { V2HostTransportDiagnostics } from "../src/v2/diagnostics/host-types";
import { createProviderErrorEnvelope } from "../src/v2/provider-error";
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
  type V2RuntimeLivenessDiagnostic,
  type V2RuntimeTransport,
  type V2RuntimeYDoc,
  type V2SandboxRuntimeOptions,
} from "../src/v2/runtime";
import type { V2SidecarRecoveryToken } from "../src/v2/transport/session";
import type { V2BootstrapSupervisor } from "../src/v2/supervisor/host";
import { SupervisorLifecycle } from "../src/v2/supervisor/lifecycle";
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
  lastSuccessAtMs: number | undefined;
  constructor(readonly order: string[], readonly clock: ManualClock) {}
  start(): void { this.started = true; this.order.push("heartbeat"); }
  async stop(): Promise<void> { this.stops++; this.started = false; }
  snapshot() { return { lastSuccessAtMs: this.lastSuccessAtMs ?? this.clock.now() }; }
}

class FakeYDoc implements V2RuntimeYDoc {
  constructor(readonly order: string[]) {}
  async connect(): Promise<void> { this.order.push("ydoc"); }
  async destroy(): Promise<void> {}
}

class FakeAwareness implements AwarenessChannel<HostFrame, SidecarFrame> {
  constructor(readonly projectId: ProjectId = PROJECT) {}
  readonly localPeerId = "host";
  rotations = 0;
  readonly rotationTokens: string[] = [];
  readonly failureListeners = new Set<(error: Error) => void>();
  rotationError: Error | null = null;
  async connect(): Promise<void> {}
  publish(_envelope: AwarenessEnvelope<HostFrame>): void {}
  clear(): void {}
  subscribe(_listener: (update: AwarenessPeerUpdate<SidecarFrame>) => void): () => void { return () => {}; }
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

class FakeSupervisor implements V2BootstrapSupervisor {
  readonly binding = {
    projectId: PROJECT,
    sandboxSessionId: "sandbox-session-1",
    leaderGeneration: 4,
    buildHash: "a".repeat(64),
    schemaHash: "b".repeat(64),
    nonce: "supervisor-nonce-1",
    protocolRevision: "r2" as const,
    schemaVersion: 2 as const,
  };
  readonly instanceId = "supervisor-instance-1";
  ensures = 0;
  rotations = 0;
  shutdowns = 0;
  closes = 0;
  async ensureReady(): Promise<void> { this.ensures++; }
  async connect(): Promise<void> { this.ensures++; }
  async launch() {
    return { encodedHello: "abc", process: { pid: 3, startTick: "4", processGroup: 3, session: 3 } };
  }
  async deliverCredential(): Promise<void> {}
  async stop(): Promise<string | null> { return null; }
  async rotateCredential(): Promise<void> { this.rotations++; }
  async shutdown(): Promise<void> { this.shutdowns++; }
  async close(): Promise<void> { this.closes++; }
}

class FakeTransport implements V2RuntimeTransport {
  snapshotValue: V2TransportSnapshot;
  readonly pending: Array<{
    resolve: (value: Record<string, unknown>) => void;
    reject: (error: unknown) => void;
  }> = [];
  hostCeilings: number[] = [];
  readonly bodies: Record<string, unknown>[] = [];
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
    body: Record<string, unknown> = {},
    _signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.bodies.push(body);
    this.snapshotValue.active++;
    this.snapshotValue.committed++;
    options?.onAdmissionReflected?.();
    options?.onAdmissionProgress?.({
      stage: "request_data",
      acknowledgedEncodedBytes: 1,
      totalEncodedBytes: 1,
    });
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

function receiveStallDiagnostics(
  host: Partial<V2HostTransportDiagnostics> = {},
  sidecar: Partial<V2TransportDiagnostics> = {},
): NonNullable<V2TransportSnapshot["diagnostics"]> {
  return {
    host: {
      ...stalledHostDiagnostics(), windowFrames: 0, windowBytes: 0,
      schedulerFrames: 0, schedulerBytes: 0, oldestQueuedFrameMs: 0, ackProgressAgeMs: 0,
      inboundAcceptedEpoch: 1, inboundAcceptedSequence: 3,
      inboundProcessedEpoch: 1, inboundProcessedSequence: 3,
      inboundAcceptedProgressAgeMs: 60_000, inboundProcessedProgressAgeMs: 60_000,
      inboundProcessingFrames: 0, sidecarDiagnosticsAgeMs: 0,
      sidecarPendingAgeMs: 60_000, sidecarPendingDataPublications: 4,
      dataPeerAgeMs: 60_000, controlPeerAgeMs: 0,
      lastDataRevision: 15,
      ...host,
    },
    sidecar: {
      version: 1, resource: null, malformedCount: 0,
      transport: { ...stalledHostDiagnostics(), ...sidecar },
    },
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

function credentials(clock: ManualClock, expiresInMs = 600_000, selectedProject = PROJECT): ProvisionedRuntimeCredentials {
  const sandbox = parseProvisionedSandbox({
    url: "https://prism.test/s/sandboxes/proxy",
    token: "sandbox-token",
    sandbox_session_id: "sandbox-session-1",
  });
  return {
    projectId: selectedProject,
    sandbox,
    resource: parseResourceCredential({
      access_token: "resource-token",
      expires_at: (clock.now() + expiresInMs) / 1_000,
      scopes: ["project_files:read"],
      resources_base_url: "https://prism.test/s/sandbox-resources",
    }, clock.now()),
    ySweet: parseYSweetEnvelope({
      url: `wss://prism.test/y/d/${selectedProject}/ws`,
      baseUrl: `https://prism.test/y/d/${selectedProject}`,
      docId: selectedProject,
      token: "y-token",
      authorization: "full",
    }),
    provisionedAtMs: clock.now(),
  };
}

function runtimeHarness(options: {
  project?: ProjectId;
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
  shutdownDeadlineMs?: () => number | undefined;
  sidecarRecoveryTimeoutMs?: number;
  sidecarCommitStallMs?: number;
  sidecarStallSampleThreshold?: number;
  sidecarSuspectRecheckMs?: number;
  enableR2Recovery?: boolean;
  recoveryAdoptError?: Error;
  heartbeatSessionId?: string;
  recoveryAdoptPending?: boolean;
  recoveryBootstrapDelayMs?: number;
  recoveryConnectPending?: boolean;
  gatewayHeartbeatOutcomes?: Array<V2RuntimeHeartbeatResult | Error>;
  gatewayHeartbeat?: () => V2RuntimeHeartbeatResult | Promise<V2RuntimeHeartbeatResult>;
  initialRotationPending?: boolean;
  enableSupervisor?: boolean;
  transportFactory?: (input: Parameters<V2SandboxRuntimeOptions["createTransport"]>[0], clock: ManualClock) => V2RuntimeTransport;
} = {}) {
  const order: string[] = [];
  const clock = new ManualClock();
  const selectedProject = options.project ?? PROJECT;
  let current = credentials(clock, options.resourceExpiresInMs ?? 600_000, selectedProject);
  let refreshes = 0;
  let ySweetMints = 0;
  let acquisitions = 0;
  let acquiredProvisions = 0;
  let provisions = 0;
  let gatewayHeartbeats = 0;
  let gatewayRotations = 0;
  let refreshedSandboxToken: string | null = null;
  const credentialOutcomes: string[] = [];
  const livenessDiagnostics: V2RuntimeLivenessDiagnostic[] = [];
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
        url: `wss://prism.test/y/d/${selectedProject}/ws`,
        baseUrl: `https://prism.test/y/d/${selectedProject}`,
        docId: selectedProject,
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
  const awareness = new FakeAwareness(selectedProject);
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
  const supervisor = new FakeSupervisor();
  const bootstrapSupervisors: Array<V2BootstrapSupervisor | null> = [];
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
    projectId: selectedProject,
    workerIndex: 1,
    leaderGeneration: 4,
    provisioning: provisioner,
    governor,
    clock,
    assets,
    transportReservationBytes: 16 * 1024 * 1024,
    ySweetRefreshMs: 300_000,
    stopTimeoutMs: options.stopTimeoutMs,
    shutdownDeadlineMs: options.shutdownDeadlineMs,
    sidecarRecoveryTimeoutMs: options.sidecarRecoveryTimeoutMs,
    sidecarCommitStallMs: options.sidecarCommitStallMs,
    sidecarStallSampleThreshold: options.sidecarStallSampleThreshold,
    sidecarSuspectRecheckMs: options.sidecarSuspectRecheckMs,
    initialSandbox: options.initialSandbox,
    onCredentialRefresh: outcome => credentialOutcomes.push(outcome),
    onLivenessDiagnostic: diagnostic => livenessDiagnostics.push(diagnostic),
    createGateway: (_sandbox: ProvisionedSandbox) => ({
      heartbeat: async () => {
        gatewayHeartbeats++;
        if (options.gatewayHeartbeat) return options.gatewayHeartbeat();
        const outcome = heartbeatOutcomes.shift();
        if (outcome instanceof Error) throw outcome;
        return outcome ?? {
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
      const next = new FakeAwareness(selectedProject);
      awarenesses.push(next);
      return next;
    },
    ...(options.enableSupervisor
      ? { createSupervisor: () => supervisor }
      : {}),
    createBootstrap: input => {
      bootstrapSupervisors.push(input.supervisor);
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
        return options.transportFactory?.(input, clock) ?? transport;
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
    ydoc,
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
    livenessDiagnostics,
    sandboxFailures,
    sandboxReady,
    awarenessCredentials,
    bootstrapCredentials,
    supervisor,
    bootstrapSupervisors,
    releaseInitialRotation: () => releaseInitialRotation?.(),
    releaseRecoveryAdopt: () => releaseRecoveryAdopt?.(),
    heartbeatSuccess: () => heartbeatCallbacks?.onSuccess({
      ok: true,
      status: 200,
      durationMs: 1,
      sessionId: "sandbox-session-1",
    }),
    heartbeatAttempt: (
      diagnostic: Parameters<V2RuntimeHeartbeatCallbacks["onAttempt"]>[0],
    ) => heartbeatCallbacks?.onAttempt(diagnostic),
    heartbeatExpired: (status = 401) => heartbeatCallbacks?.onExpired(status),
    heartbeatAuthenticationFailure: (status = 401, signal = new AbortController().signal) =>
      heartbeatCallbacks?.onAuthenticationFailure(status, signal),
    heartbeatTransientFailure: (status: number | null = null) =>
      heartbeatCallbacks?.onTransientFailure(status),
    heartbeatUnreachable: (status: number | null = null) =>
      heartbeatCallbacks?.onUnreachable(status),
  };
}

function pairedLaneRecoveryHarness(options: { repairDelayMs?: number; repairAvailable?: boolean; gatewayUnavailable?: boolean;
  receiveStall?: boolean; holdStartAck?: boolean } = {}) {
  let session!: V2TransportSession;
  let binding!: ProtocolBinding;
  let dataConnected = true;
  let controlConnected = true;
  let holdPrepared = false;
  const peerRevision = { data: 0, control: 0 };
  let peerPublications = 0;
  let peerDataPublications = 0;
  let peerFlightCompletions = 0;
  let peerPendingAtMs: number | null = null;
  let queuedPrepared: AwarenessEnvelope<SidecarFrame> | undefined;
  let peerSequence = 0;
  let hostAck = -1;
  let heldHostAck: number | null = null;
  let resumes = 0;
  let providerExecutions = 0;
  const repairs: Array<"data" | "control"> = [];
  const repairSignals: Array<AbortSignal | undefined> = [];
  const hostFrames = new Map<number, ReliableFrame<HostFrame>>();
  const outstanding: Array<ReliableFrame<SidecarFrame>> = [];
  const receivedAcks: number[] = [];
  const hostPings: number[] = [];
  const listeners = new Set<(update: AwarenessPeerUpdate<SidecarFrame>) => void>();
  const harness = runtimeHarness({
    enableR2Recovery: true, enableSupervisor: true, resourceExpiresInMs: 1_200_000,
    ...(options.gatewayUnavailable ? { gatewayHeartbeat: () => { throw new Error("offline complete outage"); } } : {}),
    transportFactory: (input, clock) => {
      binding = input.binding;
      session = new V2TransportSession({ ...input, clock, hostInstanceId: "paired-lane-host",
        publicationIntervalMs: 1, retransmitMs: 1_000_000, livenessTimeoutMs: 30_000,
        publicationTimer: { now: () => clock.now(), set: (callback, delay) => setTimeout(callback, delay),
          clear: key => clearTimeout(key as ReturnType<typeof setTimeout>), defer: queueMicrotask } });
      return session;
    },
  });
  const timers = new Map<object, { at: number; callback(): void }>();
  const setTimer = spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void,
    delay = 0, ...args: unknown[]) => {
    const key = { unref() { return key; } };
    timers.set(key, { at: harness.clock.now() + delay, callback: () => callback(...args) });
    return key;
  }) as typeof setTimeout);
  const clearTimer = spyOn(globalThis, "clearTimeout").mockImplementation((key => {
    timers.delete(key as object);
  }) as typeof clearTimeout);
  const send = (lane: "data" | "control", frames: SidecarFrame[] = [], sentAtMs?: number,
    deliver = true): AwarenessEnvelope<SidecarFrame> => {
    if (frames.length > 0 && outstanding.length === 0) peerPendingAtMs = harness.clock.now();
    outstanding.push(...frames.map(frame => ({ ...frame, epoch: 1, sequence: peerSequence++ })));
    peerPublications++;
    if (lane === "data") peerDataPublications++;
    const envelope: AwarenessEnvelope<SidecarFrame> = {
      protocol: "gp-awareness-v2", schemaVersion: 2, ...binding, protocolRevision: "r2",
      peerId: "sidecar", instanceId: "sidecar-instance-1", targetInstanceId: "paired-lane-host",
      epoch: 1, revision: peerRevision[lane]++, lane, ack: { epoch: 1, sequence: heldHostAck ?? hostAck },
      credits: 128, active: providerExecutions, queued: 0, draining: false,
      frames: lane === "data" ? [...outstanding] : [],
      ...(sentAtMs === undefined ? {} : { liveness: { kind: "pong" as const, sentAtMs } }),
      ...(options.receiveStall && lane === "control" ? { diagnostics: {
        version: 1 as const, resource: null, malformedCount: 0,
        transport: { ...stalledHostDiagnostics(), windowFrames: outstanding.length,
          windowBytes: outstanding.length * 100, schedulerFrames: 0, schedulerBytes: 0,
          ackProgressAgeMs: peerPendingAtMs === null ? 0 : harness.clock.now() - peerPendingAtMs,
          publicationsTotal: peerPublications, dataPublicationsTotal: peerDataPublications,
          controlPublicationsTotal: peerPublications - peerDataPublications,
          flightCompletionsTotal: peerFlightCompletions },
      } } : {}),
    };
    if (deliver && (lane === "data" ? dataConnected : controlConnected)) {
      queueMicrotask(() => { for (const listener of listeners) listener({ peerClientId: "paired-sidecar", envelope }); });
    }
    return envelope;
  };
  harness.awareness.subscribe = listener => { listeners.add(listener); return () => listeners.delete(listener); };
  harness.awareness.connect = async () => { resumes++; dataConnected = true; if (binding) send("data"); };
  (harness.awareness as AwarenessChannel<HostFrame, SidecarFrame>).reconnect = async (lane, signal) => {
    signal?.throwIfAborted(); repairs.push(lane!);
    repairSignals.push(signal);
    if (options.repairDelayMs) await harness.clock.sleep(options.repairDelayMs, signal);
    signal?.throwIfAborted();
    if (options.repairAvailable === false) return;
    if (options.holdStartAck) heldHostAck = null;
    if (lane === "data") dataConnected = true;
    else controlConnected = true;
    send(lane!);
  };
  harness.awareness.publish = envelope => {
    queueMicrotask(() => {
      if (envelope.lane === "control") {
        if (envelope.liveness?.kind === "ping") hostPings.push(envelope.liveness.sentAtMs);
        if (!controlConnected) return;
        receivedAcks.push(envelope.ack.sequence);
        const priorOutstanding = outstanding.length;
        for (let i = outstanding.length - 1; i >= 0; i--) {
          if (outstanding[i]!.sequence <= envelope.ack.sequence) outstanding.splice(i, 1);
        }
        if (priorOutstanding > 0 && outstanding.length === 0) {
          peerFlightCompletions++;
          peerPendingAtMs = null;
        }
        if (envelope.liveness?.kind === "ping") send("control", [], envelope.liveness.sentAtMs);
      }
      for (const frame of envelope.frames) {
        if (hostFrames.has(frame.sequence)) continue;
        if (options.holdStartAck && frame.kind === "request_start") heldHostAck = hostAck;
        hostFrames.set(frame.sequence, frame);
        while (hostFrames.has(hostAck + 1)) hostAck++;
        if (frame.kind === "request_commit" && "protocolRevision" in frame) {
          if (holdPrepared) dataConnected = false;
          send("data", [{ ...frame, kind: "request_prepared" }]);
          if (options.receiveStall) queuedPrepared = send("data", [], undefined, false);
        } else if (frame.kind === "request_start") {
          providerExecutions++;
          if (options.receiveStall) dataConnected = false;
          send("data", [{ kind: "request_ack", requestId: frame.requestId,
            requestDigest: frame.requestDigest, committed: true }]);
          if (options.receiveStall) send("control");
        } else send("data");
      }
    });
  };
  harness.supervisor.rotateCredential = async () => {
    harness.supervisor.rotations++;
    if (options.holdStartAck) return;
    throw new V2SupervisorError("offline supervisor rotation unavailable", "timeout", "challenge");
  };
  const settle = async () => {
    for (let turn = 0; turn < 100; turn++) {
      for (const [key, timer] of [...timers]) {
        if (timer.at <= harness.clock.now()) { timers.delete(key); timer.callback(); }
      }
      await Promise.resolve();
    }
    await Bun.sleep(0);
    for (let turn = 0; turn < 100; turn++) await Promise.resolve();
  };
  return {
    harness, repairs, repairSignals, outstanding, receivedAcks, hostPings,
    session: () => session, resumes: () => resumes, providerExecutions: () => providerExecutions,
    rotationFrames: () => [...hostFrames.values()].filter(frame => frame.kind === "credential_rotate").length,
    disconnectBoth() { dataConnected = false; controlConnected = false; },
    async start() { const starting = harness.runtime.start(); await settle(); await starting; },
    async startHeldStartFlight() {
      const body = { model: "gpt-6-astra", input: "original stored delta", previous_response_id: "resp_exact_rotation_parent",
        stream: true, store: true };
      const running = harness.runtime.run(body);
      void running.catch(() => undefined);
      for (let turn = 0; turn < 20 && providerExecutions === 0; turn++) {
        await harness.clock.advanceBy(1); await settle();
      }
      const handle = await running;
      void handle.final.catch(() => undefined);
      expect(session.snapshot().diagnostics?.host.windowFrames).toBeGreaterThan(0);
      return { handle, body };
    },
    async startMissingRequestAck() {
      const body = { model: "gpt-6-astra", input: "retained current delta", previous_response_id: "resp_exact_prior",
        stream: true, store: true };
      const running = harness.runtime.run(body);
      void running.catch(() => undefined);
      await settle();
      for (let turn = 0; turn < 20 && providerExecutions === 0; turn++) {
        await harness.clock.advanceBy(1); await settle();
      }
      expect(providerExecutions).toBe(1);
      expect(outstanding.map(frame => [frame.kind, frame.sequence])).toEqual([["request_ack", 1]]);
      expect(session.snapshot().diagnostics?.host).toMatchObject({
        inboundAcceptedSequence: 0, inboundProcessedSequence: 0, inboundProcessingFrames: 0,
      });
      return { running, body };
    },
    async deliverQueuedPrepared() {
      if (!queuedPrepared) throw new Error("No retained prepared publication");
      for (const listener of listeners) listener({ peerClientId: "paired-sidecar", envelope: queuedPrepared });
      await settle();
      expect(session.snapshot().diagnostics?.host).toMatchObject({
        inboundAcceptedSequence: 0, inboundProcessedSequence: 0, inboundProcessingFrames: 0,
      });
    },
    async commitAcrossDisconnect() {
      holdPrepared = true;
      const body = { model: "gpt-6-astra", input: "retained current delta", previous_response_id: "resp_exact_prior",
        stream: true, store: true };
      const running = harness.runtime.run(body);
      void running.catch(() => undefined);
      await settle();
      for (let turn = 0; turn < 20 && outstanding.length === 0; turn++) {
        await harness.clock.advanceBy(1); await settle();
      }
      expect(outstanding.map(frame => frame.kind)).toEqual(["request_prepared"]);
      controlConnected = false;
      await harness.clock.advanceBy(10_000 - harness.clock.now());
      await settle(); // The periodic 30s CONTROL challenge owns the older probe.
      harness.awareness.fail(new SplitAwarenessChannelError("data",
        new AwarenessChannelError("offline data disconnect", "disconnected", 1005)));
      await settle();
      for (let turn = 0; turn < 20 && providerExecutions === 0; turn++) {
        await harness.clock.advanceBy(1); await settle();
      }
      expect(providerExecutions).toBe(1);
      const handle = await running;
      void handle.final.catch(() => undefined);
      expect(session.snapshot().diagnostics?.host).toMatchObject({
        inboundAcceptedSequence: 1, inboundProcessedSequence: 1,
      });
      expect(providerExecutions).toBe(1);
      expect(receivedAcks.at(-1)).toBe(-1);
      return { handle, body };
    },
    async advance(ms: number) {
      await harness.clock.advanceBy(ms);
      if (options.receiveStall) { send("data"); send("control"); }
      await settle();
    },
    async complete() {
      const commit = [...hostFrames.values()].find(frame => frame.kind === "request_commit")!;
      if (commit.kind !== "request_commit" || !("requestDigest" in commit)) throw new Error("missing request identity");
      const final = { id: "resp_repaired", object: "response", status: "completed", output: [] };
      const event = { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) };
      const proof = new TranscriptProof(commit.requestId, commit.requestDigest);
      proof.append(event);
      send("data", [{ kind: "upstream_prelude", requestId: commit.requestId, status: 200,
        contentType: "text/event-stream", upstreamRequestId: null },
      { kind: "response_event", requestId: commit.requestId, eventIndex: 0, event: event.event,
        partIndex: 0, partCount: 1, offset: 0, dataBase64Url: base64UrlEncode(utf8Bytes(event.data)) },
      await proof.createTerminal()]);
      await settle();
    },
    async close() {
      try { await session.close(); await harness.runtime.stop(); }
      finally { setTimer.mockRestore(); clearTimer.mockRestore(); }
      expect(harness.clock.waiters).toHaveLength(0);
      expect(timers.size).toBe(0);
    },
  };
}

describe("V2SandboxRuntime", () => {
  it("unblocks an actual immutable flight after its unsent credential rotation times out", async () => {
    const test = pairedLaneRecoveryHarness({ holdStartAck: true });
    try {
      await test.start();
      const { handle } = await test.startHeldStartFlight();
      const session = test.session();
      const resume = session.resumeAwareness.bind(session);
      let nativeResumes = 0;
      session.resumeAwareness = async signal => {
        if (++nativeResumes === 1) throw new Error("offline first native reconnect failed");
        await resume(signal);
      };
      test.harness.awareness.fail(new SplitAwarenessChannelError("control",
        new AwarenessChannelError("offline disconnect", "disconnected")));
      await test.advance(0);
      expect(session.snapshot().diagnostics?.host).toMatchObject({ schedulerFrames: 1 });
      expect(test.rotationFrames()).toBe(0);
      for (let elapsed = 0; elapsed < 30_000; elapsed += 2_000) await test.advance(2_000);
      for (let elapsed = 0; elapsed < 2_000 && test.harness.runtime.snapshot().available === 0; elapsed += 25) await test.advance(25);
      expect(test.repairs).toEqual(["data"]);
      expect(test.rotationFrames()).toBe(0);
      expect(session.snapshot().diagnostics?.host).toMatchObject({ windowFrames: 0, schedulerFrames: 0 });
      expect(test.harness.runtime.snapshot()).toMatchObject({ state: "ready", availabilityReason: "available" });
      expect(test.harness.ySweetMints()).toBe(1);
      expect(test.harness.supervisor.rotations).toBe(1);
      expect(test.harness.awareness.rotationTokens).toEqual(["y-token-1"]);
      expect(test.harness.bootstrap.forceStops).toBe(0);
      expect(test.harness.bootstraps).toHaveLength(1);
      expect(test.providerExecutions()).toBe(1);
      await test.complete();
      await expect(handle.final).resolves.toMatchObject({ id: "resp_repaired" });
      expect(session.snapshot()).toMatchObject({ active: 0, committed: 0, quarantined: 0 });
    } finally { await test.close(); }
  });

  it.each([0, 250, 31_000, "scheduled"] as const)("repairs a queued rotation with delayed flight progress: %s", async scenario => {
    const scheduled = scenario === "scheduled";
    const progressDelayMs = typeof scenario === "number" ? scenario : 0;
    const h = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true,
      resourceExpiresInMs: 1_200_000, livenessOutcomes: [null] });
    await h.runtime.start();
    let repaired = false;
    const repairs: string[] = [];
    h.transport.snapshotValue.diagnostics = receiveStallDiagnostics({ windowFrames: 2, windowBytes: 260,
      schedulerFrames: 0, schedulerBytes: 0, ackProgressAgeMs: 0,
      sidecarPendingAgeMs: null, sidecarPendingDataPublications: 0 }, { windowFrames: 0, schedulerFrames: 0 });
    h.transport.resumeAwareness = async () => {
      if (++h.transport.awarenessResumes === 1) throw new Error("offline native reconnect unavailable");
    };
    h.transport.probeLiveness = async () => {
      if (!repaired) throw new V2TransportLivenessError("offline control publish failed", "publish");
      return { rttMs: 1, sentAtMs: h.clock.now() };
    };
    (h.transport as V2RuntimeTransport).rotateCredential = async (id, _sealed, signal) => {
      h.transport.rotations++;
      await h.clock.sleep(30_000, signal);
      throw new Error(`Credential rotation ACK timed out: ${id}`);
    };
    (h.transport as V2RuntimeTransport).reconnectAwareness = async (lane, signal) => {
      repairs.push(lane!);
      repaired = true;
      const progress = () => {
        const host = h.transport.snapshotValue.diagnostics!.host;
        h.transport.snapshotValue.diagnostics = { ...h.transport.snapshotValue.diagnostics!, host: {
          ...host, windowFrames: 0, windowBytes: 0, flightCompletionsTotal: host.flightCompletionsTotal + 1,
          uniqueAcknowledgedBytesTotal: host.uniqueAcknowledgedBytesTotal! + 260,
        } };
      };
      if (progressDelayMs) void h.clock.sleep(progressDelayMs).then(progress);
      else progress();
    };
    const body = { model: "gpt-6-astra", input: "current delta", previous_response_id: "resp_exact_rotation_parent",
      stream: true, store: true, service_tier: "fast" };
    const handle = await h.runtime.run(body);
    const flush = async () => { for (let turn = 0; turn < 300; turn++) await Promise.resolve(); };
    try {
      if (scheduled) await h.clock.advanceBy(300_000);
      else h.awareness.fail(new SplitAwarenessChannelError("control", new AwarenessChannelError("offline disconnect", "disconnected")));
      await flush();
      for (const step of [10_000, 2_000, 2_000, 16_000]) { await h.clock.advanceBy(step); await flush(); }
      if (progressDelayMs) {
        expect(h.runtime.snapshot().state).toBe("ready");
        expect(h.runtime.snapshot().available).toBe(0);
        await h.clock.advanceBy(progressDelayMs); await flush();
        await h.clock.advanceBy(2_000); await flush();
      }
      expect(h.runtime.snapshot()).toMatchObject({ state: "ready", availabilityReason: "available" });
      expect(repairs).toEqual(["data"]);
      expect(h.ySweetMints()).toBe(1);
      expect(h.transport.rotations).toBe(1);
      expect(h.supervisor.rotations).toBe(1);
      expect(h.awareness.rotationTokens).toEqual(scheduled ? [] : ["y-token-1"]);
      expect(h.transport.recoverySuspends).toBe(0);
      expect(h.bootstrap.forceStops).toBe(0);
      expect(h.transports).toHaveLength(1);
      expect(h.transport.bodies).toEqual([body]);
      h.transport.pending[0]!.resolve({ id: "resp_rotation_repaired", status: "completed" });
      await expect(handle.final).resolves.toMatchObject({ id: "resp_rotation_repaired" });
      await h.clock.advanceBy(scheduled ? 29_999 : 30_000); await flush();
      expect(h.ySweetMints()).toBe(1);
      if (scheduled) {
        await h.clock.advanceBy(1); await flush();
        expect(h.ySweetMints()).toBe(2);
        expect(h.transport.rotations).toBe(2);
      } else if (progressDelayMs === 0) {
        await h.clock.advanceBy(299_999 - h.clock.now()); await flush();
        expect(h.ySweetMints()).toBe(1);
        await h.clock.advanceBy(1); await flush();
        expect(h.ySweetMints()).toBe(2);
      }
    } finally {
      h.transport.pending[0]?.resolve({ status: "completed" });
      await h.runtime.stop();
    }
  });

  it.each(["no_progress", "unavailable", "repair_deadline", "shutdown", "wrong_session", "original_deadline"] as const)(
    "keeps an unproved rotation repair bounded and fail-closed: %s", async mode => {
    const h = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true,
      resourceExpiresInMs: 1_200_000, livenessOutcomes: [null],
      ...(mode === "wrong_session" ? { heartbeatSessionId: "different-session" } : {}) });
    await h.runtime.start();
    let repairs = 0;
    let repairSignal: AbortSignal | undefined;
    h.transport.snapshotValue.diagnostics = receiveStallDiagnostics({ windowFrames: 2, windowBytes: 260,
      schedulerFrames: 0, schedulerBytes: 0, ackProgressAgeMs: 0,
      sidecarPendingAgeMs: null, sidecarPendingDataPublications: 0 }, { windowFrames: 0, schedulerFrames: 0 });
    h.transport.resumeAwareness = async () => {
      if (++h.transport.awarenessResumes === 1) throw new Error("offline native reconnect unavailable");
    };
    h.transport.probeLiveness = async () => ({ rttMs: 1, sentAtMs: h.clock.now() });
    const rotationDelay = mode === "original_deadline" ? 145_000 : 30_000;
    (h.transport as V2RuntimeTransport).rotateCredential = async (id, _sealed, signal) => {
      h.transport.rotations++;
      await h.clock.sleep(rotationDelay, signal);
      throw new Error(`Credential rotation ACK timed out: ${id}`);
    };
    (h.transport as V2RuntimeTransport).reconnectAwareness = async (_lane, signal) => {
      repairs++;
      repairSignal = signal;
      if (mode === "unavailable") throw new Error("offline reconnect failed");
      if (mode === "repair_deadline" || mode === "shutdown" || mode === "original_deadline") {
        await h.clock.sleep(100_000, signal);
      }
    };
    const flush = async () => { for (let turn = 0; turn < 300; turn++) await Promise.resolve(); };
    try {
      h.awareness.fail(new SplitAwarenessChannelError("control", new AwarenessChannelError("offline disconnect", "disconnected")));
      await flush();
      await h.clock.advanceBy(rotationDelay); await flush();
      expect(repairs).toBe(mode === "wrong_session" ? 0 : 1);
      if (mode === "wrong_session") expect(h.runtime.snapshot().state).toBe("failed");
      else if (mode === "shutdown") {
        await h.runtime.stop(); await flush();
        expect(repairSignal?.aborted).toBe(true);
        expect(h.runtime.snapshot().state).toBe("stopped");
        expect(h.clock.waiters).toHaveLength(0);
      } else {
        expect(h.runtime.snapshot()).toMatchObject({ state: "ready", available: 0 });
        if (mode === "repair_deadline") {
          await h.clock.advanceBy(30_000); await flush();
          expect(repairSignal?.aborted).toBe(true);
        }
        await h.clock.advanceBy(149_999 - h.clock.now()); await flush();
        expect(h.runtime.snapshot().state).toBe("ready");
        expect(h.runtime.snapshot().available).toBe(0);
        await h.clock.advanceBy(1); await flush();
        expect(h.runtime.snapshot().state).toBe("failed");
        expect(repairSignal?.aborted).toBe(true);
      }
      expect(h.ySweetMints()).toBe(1);
      expect(h.transport.rotations).toBe(1);
      expect(h.transports).toHaveLength(1);
      expect(h.transport.recoverySuspends).toBe(0);
      expect(h.bootstrap.forceStops).toBe(mode === "shutdown" ? 1 : 0);
    } finally { await h.runtime.stop(); }
  });

  it.each([5_000, 45_000])("repairs unseen DATA after a %i ms old prepared duplicate while a newer ACK is pending", async duplicateAgeMs => {
    const test = pairedLaneRecoveryHarness({ receiveStall: true });
    try {
      await test.start();
      const { running } = await test.startMissingRequestAck();
      for (let elapsed = 0; elapsed < 60_000; elapsed += 5_000) {
        if (elapsed === 60_000 - duplicateAgeMs) await test.deliverQueuedPrepared();
        await test.advance(5_000);
      }
      for (let elapsed = 0; elapsed < 10_000 && test.repairs.length === 0; elapsed += 1_000) await test.advance(1_000);
      expect(test.repairs).toEqual(["data"]);
      for (let elapsed = 0; elapsed < 2_000 && test.harness.runtime.snapshot().available === 0; elapsed += 25) await test.advance(25);
      const handle = await running;
      expect(test.session().snapshot().diagnostics?.host).toMatchObject({ inboundAcceptedSequence: 1,
        inboundProcessedSequence: 1, inboundProcessingFrames: 0 });
      expect(test.receivedAcks.at(-1)).toBe(1);
      expect(test.providerExecutions()).toBe(1);
      expect(test.harness.ySweetMints()).toBe(0);
      expect(test.harness.supervisor.rotations).toBe(0);
      expect(test.harness.runtime.snapshot()).toMatchObject({ availabilityReason: "available", livenessRecovery: null });
      await test.complete();
      await expect(handle.final).resolves.toMatchObject({ id: "resp_repaired" });
    } finally { await test.close(); }
  });

  it("repairs stale CONTROL after DATA reconnect makes genuine positive ACK progress", async () => {
    const test = pairedLaneRecoveryHarness();
    try {
      await test.start();
      const { handle } = await test.commitAcrossDisconnect();
      const episode = test.harness.runtime.snapshot().livenessRecovery;
      expect(episode).toEqual({ startedAtMs: 10_000, deadlineAtMs: 160_000 });
      for (let elapsed = 0; elapsed < 120_000 && test.repairs.length === 0; elapsed += 1_000) await test.advance(1_000);
      expect({ repairs: test.repairs, supervisorRotations: test.harness.supervisor.rotations,
        state: test.harness.runtime.snapshot().state, providerExecutions: test.providerExecutions() })
        .toEqual({ repairs: ["control"], supervisorRotations: 0, state: "ready", providerExecutions: 1 });
      expect(test.harness.runtime.snapshot()).toMatchObject({ state: "ready", availabilityReason: "available", livenessRecovery: null });
      expect(test.receivedAcks.at(-1)).toBe(1);
      expect(test.harness.supervisor.rotations).toBe(0);
      expect(test.harness.ySweetMints()).toBe(0);
      expect(test.harness.bootstrap.forceStops).toBe(0);
      expect(test.providerExecutions()).toBe(1);
      expect(new Set(test.hostPings).size).toBe(3); // Initial proof, stale probe, fresh repaired probe.
      await test.complete();
      await expect(handle.final).resolves.toMatchObject({ id: "resp_repaired" });
      await test.advance(10_000);
      expect(new Set(test.hostPings).size).toBe(4);
      expect(test.repairs).toEqual(["control"]);
      expect(test.providerExecutions()).toBe(1);
    } finally { await test.close(); }
  });

  it("bounds paired-lane repair and keeps the original recovery deadline during complete outage", async () => {
    const test = pairedLaneRecoveryHarness({ repairAvailable: false, gatewayUnavailable: true });
    try {
      await test.start();
      await test.commitAcrossDisconnect();
      const episode = test.harness.runtime.snapshot().livenessRecovery;
      test.disconnectBoth();
      test.harness.heartbeat.lastSuccessAtMs = test.harness.clock.now();
      for (let elapsed = 0; elapsed < 149_000; elapsed += 1_000) {
        await test.advance(1_000);
        expect(test.harness.runtime.snapshot().livenessRecovery).toBe(episode);
        expect(test.providerExecutions()).toBe(1);
      }
      expect(test.repairs.length).toBeGreaterThan(0);
      expect(test.harness.runtime.snapshot().state).toBe("ready");
      await test.advance(1_000);
      expect(test.harness.runtime.snapshot()).toMatchObject({ state: "failed", livenessRecovery: null });
      expect(test.harness.livenessDiagnostics.filter(event => event.outcome === "recovery_deadline_expired")).toHaveLength(1);
      expect(test.harness.bootstrap.forceStops).toBe(0);
      expect(test.providerExecutions()).toBe(1);
    } finally { await test.close(); }
  });

  it.each(["shutdown", "session_expired"] as const)("aborts the single paired-lane repair owner on %s", async reason => {
    const test = pairedLaneRecoveryHarness({ repairDelayMs: 60_000 });
    let stopping: Promise<unknown> | undefined;
    try {
      await test.start();
      await test.commitAcrossDisconnect();
      for (let elapsed = 0; elapsed < 35_000 && test.repairs.length === 0; elapsed += 1_000) await test.advance(1_000);
      expect(test.repairs).toEqual(["control"]);
      expect(test.repairSignals[0]?.aborted).toBe(false);
      if (reason === "shutdown") stopping = test.harness.runtime.stop();
      else test.harness.heartbeatExpired(401);
      await test.advance(0);
      expect(test.repairSignals[0]?.aborted).toBe(true);
      await test.session().close();
      await stopping;
      await test.advance(60_000);
      expect(test.repairs).toEqual(["control"]);
      expect(test.harness.ySweetMints()).toBe(0);
      expect(test.providerExecutions()).toBe(1);
    } finally { await test.close(); }
  });

  it("retains a matching pending CONTROL proof beyond the fast DATA reconnect deadline", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    const proof = Promise.withResolvers<{ rttMs: number; sentAtMs: number }>();
    let probes = 0;
    let repairs = 0;
    harness.transport.probeLiveness = () => { probes++; return proof.promise; };
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async () => { repairs++; };
    try {
      await harness.clock.advanceBy(10_000);
      harness.awareness.fail(new SplitAwarenessChannelError("data",
        new AwarenessChannelError("offline data disconnect", "disconnected", 1005)));
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      await harness.clock.advanceBy(3_000);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(probes).toBe(1);
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.runtime.snapshot().livenessRecovery).toEqual({ startedAtMs: 10_000, deadlineAtMs: 160_000 });
      proof.resolve({ rttMs: 3_000, sentAtMs: 10_000 });
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(harness.runtime.snapshot()).toMatchObject({ availabilityReason: "available", livenessRecovery: null });
      expect(repairs).toBe(0);
      expect(harness.ySweetMints()).toBe(0);
    } finally { proof.resolve({ rttMs: 3_000, sentAtMs: 10_000 }); await harness.runtime.stop(); }
  });

  it.each(["unavailable", "instance_changed", "publish"] as const)("does not reinterpret %s proof failure as a CONTROL timeout", async code => {
    const harness = runtimeHarness({ enableR2Recovery: true,
      livenessOutcomes: [null, new V2TransportLivenessError("offline non-timeout proof failure", code)] });
    let repairs = 0;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async () => { repairs++; };
    await harness.runtime.start();
    try {
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(repairs).toBe(0);
      expect(harness.ySweetMints()).toBe(1);
    } finally { await harness.runtime.stop(); }
  });

  it("rejects a late shared CONTROL pong before the original deadline callback runs", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    const proof = Promise.withResolvers<{ rttMs: number; sentAtMs: number }>();
    harness.transport.probeLiveness = () => proof.promise;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async () => {};
    try {
      await harness.clock.advanceBy(10_000);
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      await harness.clock.advanceBy(3_000);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      const episode = harness.runtime.snapshot().livenessRecovery;
      harness.clock.value = 160_000; // Deliver proof before overdue timer callbacks.
      proof.resolve({ rttMs: 150_000, sentAtMs: 10_000 });
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(harness.runtime.snapshot().livenessRecovery).toBe(episode);
      expect(harness.runtime.snapshot().available).toBe(0);
      expect(harness.ySweetMints()).toBe(0);
      await harness.clock.advanceBy(0);
      expect(harness.runtime.snapshot().state).toBe("failed");
    } finally { proof.resolve({ rttMs: 150_000, sentAtMs: 10_000 }); await harness.runtime.stop(); }
  });

  it.each([30_000, 60_000, 90_000])("preserves a supervised eight-worker fleet through %i ms of shared transport loss", async outageMs => {
    let outage = false;
    let replacements = 0;
    const fleet = Array.from({ length: 8 }, (_, index) => {
      const events: SandboxSupervisorEvent[] = [];
      const harness = runtimeHarness({
        project: projectId(`00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`),
        enableR2Recovery: true,
        gatewayHeartbeat: () => {
          if (outage) throw new Error("shared heartbeat transport unavailable");
          return { ok: true, status: 200, durationMs: 1, sessionId: "sandbox-session-1" };
        },
      });
      let lastPeerAt = harness.clock.now();
      const snapshot = harness.transport.snapshot.bind(harness.transport);
      harness.transport.snapshot = () => ({
        ...snapshot(),
        sidecarPeerAgeMs: harness.clock.now() - lastPeerAt,
      });
      harness.transport.probeLiveness = async () => {
        harness.transport.livenessProbes++;
        if (outage) throw new Error("shared sidecar transport unavailable");
        lastPeerAt = harness.clock.now();
        return { rttMs: 1, sentAtMs: lastPeerAt };
      };
      const supervised = new SupervisedSandboxRuntime({
        projectId: harness.runtime.projectId,
        initialChild: harness.runtime,
        createChild: () => {
          replacements++;
          throw new Error("a temporary outage must retain its existing runtime");
        },
        clock: harness.clock,
        random: { next: () => 0 },
        onRecoveryEvent: event => events.push(event),
      });
      return { harness, supervised, events };
    });
    const handles: RunHandle[] = [];
    try {
      for (const { harness, supervised } of fleet) {
        await supervised.start();
        const handle = await supervised.run({ model: "gpt-5.6-sol", input: "one execution" });
        void handle.final.catch(() => undefined);
        handles.push(handle);
        harness.heartbeat.lastSuccessAtMs = harness.clock.now();
      }
      outage = true;
      for (let elapsed = 0; elapsed < outageMs; elapsed += 1_000) {
        await Promise.all(fleet.map(({ harness }) => harness.clock.advanceBy(1_000)));
        if (elapsed === 29_000) for (const { harness } of fleet) harness.heartbeatUnreachable();
        for (let turn = 0; turn < 30; turn++) await Promise.resolve();
      }
      expect(replacements).toBe(0);
      for (const { harness, supervised } of fleet) {
        expect(supervised.snapshot()).toMatchObject({ state: "ready", incarnation: 1, capacity: 0, available: 0 });
        expect(harness.transport.pending).toHaveLength(1);
        expect(harness.transport.recoverySuspends).toBe(0);
        expect(harness.heartbeat.stops).toBe(0);
      }
      outage = false;
      for (const { harness } of fleet) harness.heartbeat.lastSuccessAtMs = harness.clock.now();
      for (let elapsed = 0; elapsed < 20_000 && fleet.some(({ supervised }) => supervised.snapshot().available === 0); elapsed += 1_000) {
        await Promise.all(fleet.map(({ harness }) => harness.clock.advanceBy(1_000)));
        for (let turn = 0; turn < 30; turn++) await Promise.resolve();
      }
      for (const [index, { harness, supervised, events }] of fleet.entries()) {
        expect(supervised.snapshot()).toMatchObject({ state: "ready", incarnation: 1, capacity: 128, available: 127, active: 1 });
        expect(harness.transports).toHaveLength(1);
        expect(harness.bootstraps).toHaveLength(1);
        expect(harness.provisions()).toBe(1);
        expect(events.filter(event => event.type === "started")).toHaveLength(1);
        harness.transport.pending[0]!.resolve({ id: `resp_outage_${index}`, status: "completed" });
        await expect(handles[index]!.final).resolves.toMatchObject({ status: "completed" });
      }
      expect(replacements).toBe(0);
    } finally {
      outage = false;
      for (const { harness } of fleet) {
        for (const pending of harness.transport.pending) pending.resolve({ status: "completed" });
      }
      await Promise.all(fleet.map(({ supervised }) => supervised.stop()));
    }
  });

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

  it("does not extend a transport recovery episode after repeated preflight failures", async () => {
    let outage = false;
    const harness = runtimeHarness({
      enableR2Recovery: true,
      gatewayHeartbeat: () => {
        if (outage) throw new Error("persistent transport outage");
        return { ok: true, status: 200, durationMs: 1, sessionId: "sandbox-session-1" };
      },
    });
    harness.transport.probeLiveness = async () => {
      if (outage) throw new Error("no sidecar proof");
      return { rttMs: 1, sentAtMs: harness.clock.now() };
    };
    try {
      await harness.runtime.start();
      outage = true;
      harness.heartbeat.lastSuccessAtMs = 0;
      await harness.clock.advanceBy(10_000);
      const episode = harness.runtime.snapshot().livenessRecovery;
      expect(episode).toEqual({ startedAtMs: 10_000, deadlineAtMs: 160_000 });
      for (let elapsed = 10_000; elapsed < 159_000; elapsed += 1_000) {
        await harness.clock.advanceBy(1_000);
        for (let turn = 0; turn < 30; turn++) await Promise.resolve();
        expect(harness.runtime.snapshot().livenessRecovery).toBe(episode);
      }
      expect(harness.runtime.snapshot().state).toBe("ready");
      await harness.clock.advanceBy(1_000);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "failed", livenessRecovery: null });
      expect(harness.livenessDiagnostics.filter(event => event.outcome === "waiting_for_transport_recovery")).toHaveLength(1);
      expect(harness.livenessDiagnostics.filter(event => event.outcome === "recovery_deadline_expired")).toHaveLength(1);
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.bootstraps).toHaveLength(1);
    } finally {
      await harness.runtime.stop();
    }
  });

  it.each([401, 403, 409, 410])("keeps definitive heartbeat status %i authoritative during transport recovery", async status => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null, new Error("missed pong")] });
    try {
      await harness.runtime.start();
      await harness.clock.advanceBy(10_000);
      expect(harness.runtime.snapshot().livenessRecovery).not.toBeNull();
      harness.heartbeatExpired(status);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "failed", livenessRecovery: null });
    } finally {
      await harness.runtime.stop();
    }
  });

  it.each(["monitor", "reconnect"] as const)("shares exact-sidecar proof when %s owns the probe first", async first => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    let releaseProbe!: () => void;
    let probes = 0;
    let activeProbe = false;
    harness.transport.probeLiveness = async () => {
      probes++;
      if (activeProbe) throw new V2TransportLivenessError("probe already owned", "probe_in_flight");
      activeProbe = true;
      await new Promise<void>(resolve => { releaseProbe = resolve; });
      activeProbe = false;
      return { rttMs: 1, sentAtMs: harness.clock.now() };
    };
    const handle = await harness.runtime.run({ model: "gpt-6-astra", input: "retained", previous_response_id: "resp_exact", stream: true, store: true });
    void handle.final.catch(() => undefined);
    try {
      await harness.clock.advanceBy(first === "monitor" ? 10_000 : 9_000);
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      if (first === "reconnect") await harness.clock.advanceBy(1_000);
      expect(probes).toBe(1);
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.livenessDiagnostics.some(event => event.errorKind === "probe_in_flight")).toBe(false);
      releaseProbe();
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", livenessRecovery: null });
      expect(harness.runtime.snapshot().available).toBeGreaterThan(0);
      expect(harness.transport.bodies).toHaveLength(1);
      expect(harness.transport.bodies[0]).toMatchObject({ previous_response_id: "resp_exact", stream: true, store: true });
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.bootstraps).toHaveLength(1);
      harness.transport.pending[0]!.resolve({ id: "resp_once", status: "completed" });
      await expect(handle.final).resolves.toMatchObject({ id: "resp_once" });
    } finally {
      releaseProbe?.();
      harness.transport.pending[0]?.resolve({ status: "completed" });
      await harness.runtime.stop();
      expect(harness.clock.waiters).toHaveLength(0);
    }
  });

  it("expires the fixed recovery episode while an exact-sidecar probe remains pending", async () => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, new V2TransportLivenessError("first missed proof", "timeout")],
    });
    await harness.runtime.start();
    let releaseProbe!: () => void;
    let probing = false;
    try {
      await harness.clock.advanceBy(10_000);
      expect(harness.runtime.snapshot().livenessRecovery?.deadlineAtMs).toBe(160_000);
      harness.transport.probeLiveness = async () => {
        probing = true;
        await new Promise<void>(resolve => { releaseProbe = resolve; });
        return { rttMs: 1, sentAtMs: harness.clock.now() };
      };
      await harness.clock.advanceBy(2_000);
      expect(probing).toBe(true);
      await harness.clock.advanceBy(147_999);
      expect(harness.runtime.snapshot().state).toBe("ready");
      await harness.clock.advanceBy(1);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "failed", livenessRecovery: null });
      expect(harness.livenessDiagnostics.filter(event => event.outcome === "recovery_deadline_expired")).toHaveLength(1);
      releaseProbe();
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(harness.runtime.snapshot().state).toBe("failed");
      expect(harness.livenessDiagnostics.some(event => event.outcome === "recovered")).toBe(false);
      expect(harness.transport.recoverySuspends).toBe(0);
    } finally {
      releaseProbe?.();
      await harness.runtime.stop();
      expect(harness.clock.waiters).toHaveLength(0);
    }
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

  it("reports a supervisor readiness failure hidden inside a deployment error against its exact session", async () => {
    const harness = runtimeHarness();
    const inner = Object.assign(new Error("offline supervisor readiness failure"), {
      name: "V2SupervisorBootstrapError", kind: "readiness",
    });
    const outer = Object.assign(new Error("offline deployment wrapper", { cause: inner }), {
      name: "V2BootstrapError", kind: "deploy",
    });
    harness.bootstrap.ensureReadyWait = async () => { throw outer; };
    await expect(harness.runtime.start()).rejects.toBe(outer);
    expect(harness.sandboxFailures.map(sandbox => String(sandbox.sessionId))).toEqual(["sandbox-session-1"]);
    await harness.runtime.stop();
  });

  it("reports stable submitted-cleanup failure against the execution-risk sandbox session", async () => {
    const harness = runtimeHarness();
    const readiness = Object.assign(new Error("offline supervisor readiness failure"), {
      name: "V2SupervisorBootstrapError", kind: "readiness",
    });
    const cleanup = Object.assign(new Error("offline submitted cleanup unproven", { cause: readiness }), {
      name: "V2SupervisorBootstrapError", kind: "deploy", remoteCode: "bootstrap_cleanup_unproven",
    });
    const outer = Object.assign(new Error("offline deployment wrapper", { cause: cleanup }), {
      name: "V2BootstrapError", kind: "deploy",
    });
    harness.bootstrap.ensureReadyWait = async () => { throw outer; };
    await expect(harness.runtime.start()).rejects.toBe(outer);
    expect(harness.sandboxFailures.map(sandbox => String(sandbox.sessionId))).toEqual(["sandbox-session-1"]);
    await harness.runtime.stop();
  });

  it("does not fence a sandbox for unrelated deployment failures or cyclic cause metadata", async () => {
    const deployment = Object.assign(new Error("offline deployment"), { name: "V2BootstrapError", kind: "deploy" });
    const cyclic = Object.assign(new Error("offline cyclic wrapper"), { name: "V2BootstrapError", kind: "deploy" });
    cyclic.cause = cyclic;
    const collision = Object.assign(new Error("offline file collision"), { name: "V2SupervisorBootstrapError", kind: "deploy" });
    for (const failure of [deployment, cyclic, collision]) {
      const harness = runtimeHarness();
      harness.bootstrap.ensureReadyWait = async () => { throw failure; };
      await expect(harness.runtime.start()).rejects.toBe(failure);
      expect(harness.sandboxFailures).toHaveLength(0);
      await harness.runtime.stop();
    }
  });

  it("forwards exact admission and execution-risk callbacks to transport", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    let reflected = 0;
    let progress = 0;
    let executionRisks = 0;
    const handle = await harness.runtime.run(
      { model: "gpt-5.6-sol", input: "handoff" },
      undefined,
      {
        onAdmissionReflected: () => { reflected++; },
        onAdmissionProgress: () => { progress++; },
        onExecutionRiskReflected: () => { executionRisks++; },
      },
    );
    expect(reflected).toBe(1);
    expect(progress).toBe(1);
    expect(executionRisks).toBe(1);
    expect(harness.transport.bodies[0]).toMatchObject({ service_tier: "fast" });
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
    await realHandle.acceptEvent({
      event: "response.output_text.delta",
      data: JSON.stringify({ type: "response.output_text.delta", delta: "discard me" }),
    });
    wrapped.discardUnconsumedEvents?.();
    expect(await Array.fromAsync(wrapped.events())).toEqual([]);
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
      transportProfile: "standard",
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

  it("records heartbeat failure mode and recovery without logging routine success", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();

    harness.heartbeatAttempt({
      outcome: "success",
      status: 200,
      errorName: null,
      durationMs: 20,
      attempt: 1,
      previousConsecutiveFailures: 0,
      consecutiveFailures: 0,
    });
    expect(harness.livenessDiagnostics).toEqual([]);

    harness.heartbeatAttempt({
      outcome: "transport_error",
      status: null,
      errorName: "AccountTransportError",
      durationMs: 5_000,
      attempt: 2,
      previousConsecutiveFailures: 0,
      consecutiveFailures: 1,
    });
    harness.heartbeatAttempt({
      outcome: "success",
      status: 200,
      errorName: null,
      durationMs: 120,
      attempt: 3,
      previousConsecutiveFailures: 1,
      consecutiveFailures: 0,
    });

    expect(harness.livenessDiagnostics).toMatchObject([
      {
        signal: "heartbeat",
        outcome: "failed",
        errorName: "AccountTransportError",
        errorKind: "transport",
        durationMs: 5_000,
        heartbeatAgeMs: 0,
        transportConnected: true,
      },
      {
        signal: "heartbeat",
        outcome: "recovered",
        status: 200,
        errorName: null,
        errorKind: null,
        durationMs: 120,
      },
    ]);
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

  it("keeps all eight exact-sidecar runtimes routable through a phased heartbeat outage", async () => {
    const fleet = Array.from({ length: 8 }, () => runtimeHarness());
    await Promise.all(fleet.map(harness => harness.runtime.start()));

    for (const [phase, harness] of fleet.entries()) {
      harness.transport.snapshotValue.sidecarPeerAgeMs = 250 + (phase * 1_250);
      harness.heartbeatUnreachable(503);

      expect(harness.runtime.snapshot()).toMatchObject({
        state: "ready",
        capacity: 128,
        available: 128,
        transportConnected: true,
        failure: null,
      });
      expect(harness.sandboxFailures).toHaveLength(0);
    }

    expect(fleet.reduce((total, harness) => total + harness.runtime.snapshot().capacity, 0))
      .toBe(8 * 128);
    await Promise.all(fleet.map(harness => harness.runtime.stop()));
  });

  it("fails closed when heartbeat and exact-sidecar liveness are both lost", async () => {
    const stale = runtimeHarness();
    await stale.runtime.start();
    stale.transport.snapshotValue.sidecarPeerAgeMs = 20_001;
    stale.heartbeatUnreachable(503);
    expect(stale.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: { kind: "heartbeat_unreachable", status: 503 },
    });
    expect(stale.sandboxFailures).toHaveLength(1);
    await stale.runtime.stop();

    const disconnected = runtimeHarness();
    await disconnected.runtime.start();
    disconnected.transport.snapshotValue.sidecarPeerAgeMs = 1;
    disconnected.transport.snapshotValue.connected = false;
    disconnected.heartbeatUnreachable(503);
    expect(disconnected.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      transportConnected: false,
      failure: { kind: "heartbeat_unreachable", status: 503 },
    });
    expect(disconnected.sandboxFailures).toHaveLength(1);
    await disconnected.runtime.stop();
  });

  it("keeps session-change heartbeat statuses fail-closed despite fresh sidecar proof", async () => {
    const harness = runtimeHarness();
    await harness.runtime.start();
    harness.transport.snapshotValue.sidecarPeerAgeMs = 1;

    harness.heartbeatExpired(409);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: { kind: "heartbeat_expired", status: 409 },
    });
    await harness.runtime.stop();
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

  it("bounds the suspect recheck cadence by the normal liveness period", () => {
    expect(() => runtimeHarness({ sidecarSuspectRecheckMs: 0 })).toThrow(
      "sidecarSuspectRecheckMs must be between one and sidecarProbePeriodMs",
    );
    expect(() => runtimeHarness({ sidecarSuspectRecheckMs: 10_001 })).toThrow(
      "sidecarSuspectRecheckMs must be between one and sidecarProbePeriodMs",
    );
  });

  it("removes capacity on a missed sidecar probe, heals, then fails after three misses", async () => {
    const miss = () => new V2TransportLivenessError("sidecar pong timed out", "timeout");
    const harness = runtimeHarness({
      livenessOutcomes: [null, miss(), null, miss(), miss(), miss()],
    });
    await harness.runtime.start();

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      available: 0,
      availabilityReason: "sidecar_suspect",
    });

    await harness.clock.advanceBy(1_999);
    expect(harness.runtime.snapshot().capacity).toBe(0);
    expect(harness.transport.livenessProbes).toBe(2);
    await harness.clock.advanceBy(1);
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.transport.livenessProbes).toBe(3);

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().availabilityReason).toBe("sidecar_suspect");
    expect(harness.transport.livenessProbes).toBe(4);
    await harness.clock.advanceBy(2_000);
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.transport.livenessProbes).toBe(5);
    await harness.clock.advanceBy(1_999);
    expect(harness.runtime.snapshot().state).toBe("ready");
    await harness.clock.advanceBy(1);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      failure: {
        kind: "sidecar_unreachable",
        operation: "sidecar_liveness",
      },
    });
    expect(harness.transport.livenessProbes).toBe(6);
    expect(harness.livenessDiagnostics.map(event => ({
      signal: event.signal,
      outcome: event.outcome,
      attempt: event.attempt,
      errorKind: event.errorKind,
    }))).toEqual([
      { signal: "sidecar_probe", outcome: "failed", attempt: 1, errorKind: "timeout" },
      { signal: "sidecar_probe", outcome: "recovered", attempt: 2, errorKind: undefined },
      { signal: "sidecar_probe", outcome: "failed", attempt: 1, errorKind: "timeout" },
      { signal: "sidecar_probe", outcome: "failed", attempt: 2, errorKind: "timeout" },
      { signal: "sidecar_probe", outcome: "failed", attempt: 3, errorKind: "timeout" },
      { signal: "sidecar_recovery", outcome: "started", attempt: 3, errorKind: undefined },
      { signal: "sidecar_recovery", outcome: "failed", attempt: 3, errorKind: "other" },
    ]);
    await harness.runtime.stop();
  });

  it("keeps transient Prism heartbeat loss from preempting same-session sidecar recovery", async () => {
    const miss = () => new Error("sidecar pong timed out");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [
        null,
        miss(), miss(), miss(),
        miss(), miss(), miss(),
        null,
      ],
      gatewayHeartbeatOutcomes: [
        new Error("Prism heartbeat transport unavailable"),
        {
          ok: true,
          status: 200,
          durationMs: 1,
          sessionId: "sandbox-session-1",
        },
      ],
    });
    await harness.runtime.start();
    const active = await harness.runtime.run({
      model: "gpt-5.6-sol",
      input: "retain one committed request through transient liveness loss",
    });
    expect(harness.transport.pending).toHaveLength(1);

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      availabilityReason: "sidecar_suspect",
    });
    harness.heartbeatUnreachable(503);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      failure: null,
    });
    expect(harness.sandboxFailures).toHaveLength(0);

    await harness.clock.advanceBy(2_000);
    await harness.clock.advanceBy(2_000);
    for (let turn = 0; turn < 200 && harness.gatewayHeartbeats() < 1; turn++) {
      await Promise.resolve();
    }
    for (let turn = 0; turn < 50; turn++) await Promise.resolve();
    expect(harness.gatewayHeartbeats()).toBe(1);
    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      availabilityReason: "sidecar_suspect",
    });

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(2_000);
    await harness.clock.advanceBy(2_000);
    for (let turn = 0; turn < 200 && harness.runtime.snapshot().capacity === 0; turn++) {
      await Promise.resolve();
    }

    expect(harness.gatewayHeartbeats()).toBe(2);
    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 128,
      available: 127,
      failure: null,
    });
    expect(harness.sandboxFailures).toHaveLength(0);
    const final = {
      id: "resp_recovered_without_replay",
      status: "completed",
      output: [],
    };
    harness.transport.pending[0]!.resolve(final);
    await expect(active.final).resolves.toEqual(final);
    expect(harness.transport.bodies).toHaveLength(1);
    await harness.runtime.stop();
  });

  it("defers a transient HTTP recovery preflight before sidecar suspension", async () => {
    const miss = () => new Error("sidecar pong timed out");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null, miss(), miss(), miss()],
      gatewayHeartbeatOutcomes: [{
        ok: false,
        status: 503,
        durationMs: 5_000,
        sessionId: null,
      }],
    });
    await harness.runtime.start();

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(2_000);
    await harness.clock.advanceBy(2_000);
    for (let turn = 0; turn < 200 && harness.gatewayHeartbeats() < 1; turn++) {
      await Promise.resolve();
    }
    for (let turn = 0; turn < 50; turn++) await Promise.resolve();

    expect(harness.gatewayHeartbeats()).toBe(1);
    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      availabilityReason: "sidecar_suspect",
      failure: null,
    });
    expect(harness.livenessDiagnostics.at(-1)).toMatchObject({
      signal: "sidecar_recovery",
      outcome: "preflight_unavailable",
      status: 503,
      errorName: "V2SidecarRecoveryPreflightUnavailableError",
      errorKind: "http",
    });
    expect(harness.sandboxFailures).toHaveLength(0);
    await harness.runtime.stop();
  });

  it("recovers an eight-runtime common transport outage without generation replacement", async () => {
    const fleet = Array.from({ length: 8 }, () => {
      const miss = () => new Error("sidecar pong timed out");
      return runtimeHarness({
        enableR2Recovery: true,
        livenessOutcomes: [
          null,
          miss(), miss(), miss(),
          miss(), miss(), miss(),
          null,
        ],
        gatewayHeartbeatOutcomes: [
          new Error("Prism heartbeat transport unavailable"),
          {
            ok: true,
            status: 200,
            durationMs: 1,
            sessionId: "sandbox-session-1",
          },
        ],
      });
    });
    await Promise.all(fleet.map(harness => harness.runtime.start()));

    await Promise.all(fleet.map(harness => harness.clock.advanceBy(10_000)));
    for (const harness of fleet) harness.heartbeatUnreachable(null);
    expect(fleet.every(harness => (
      harness.runtime.snapshot().state === "ready" &&
      harness.runtime.snapshot().capacity === 0 &&
      harness.runtime.snapshot().availabilityReason === "sidecar_suspect"
    ))).toBe(true);

    await Promise.all(fleet.map(harness => harness.clock.advanceBy(2_000)));
    await Promise.all(fleet.map(harness => harness.clock.advanceBy(2_000)));
    for (let turn = 0; turn < 250; turn++) await Promise.resolve();
    expect(fleet.every(harness => (
      harness.gatewayHeartbeats() === 1 &&
      harness.runtime.snapshot().state === "ready" &&
      harness.transport.recoverySuspends === 0 &&
      harness.sandboxFailures.length === 0
    ))).toBe(true);

    await Promise.all(fleet.map(harness => harness.clock.advanceBy(10_000)));
    await Promise.all(fleet.map(harness => harness.clock.advanceBy(2_000)));
    await Promise.all(fleet.map(harness => harness.clock.advanceBy(2_000)));
    for (let turn = 0; turn < 250 && fleet.some(harness => (
      harness.runtime.snapshot().capacity === 0
    )); turn++) await Promise.resolve();

    expect(fleet.every(harness => (
      harness.gatewayHeartbeats() === 2 &&
      harness.transports.length === 1 &&
      harness.transport.recoverySuspends === 0 &&
      harness.runtime.snapshot().state === "ready" &&
      harness.runtime.snapshot().capacity === 128 &&
      harness.sandboxFailures.length === 0
    ))).toBe(true);
    await Promise.all(fleet.map(harness => harness.runtime.stop()));
  });

  it("never trusts a recovered heartbeat while the reliable request window is still stalled", async () => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
      gatewayHeartbeatOutcomes: [
        new Error("Prism heartbeat transport unavailable"),
        {
          ok: true,
          status: 200,
          durationMs: 1,
          sessionId: "sandbox-session-1",
        },
      ],
    });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = {
      host: stalledHostDiagnostics(),
      sidecar: null,
    };

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(2_000);
    for (let turn = 0; turn < 250 && harness.gatewayHeartbeats() < 1; turn++) {
      await Promise.resolve();
    }
    for (let turn = 0; turn < 50; turn++) await Promise.resolve();
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 0,
      availabilityReason: "sidecar_suspect",
    });
    expect(harness.transport.recoverySuspends).toBe(0);

    await harness.clock.advanceBy(10_000);
    await harness.clock.advanceBy(2_000);
    for (let turn = 0; turn < 250 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }
    for (let turn = 0; turn < 250 && harness.runtime.snapshot().capacity === 0; turn++) {
      await Promise.resolve();
    }

    expect(harness.transport.livenessProbes).toBe(1);
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.transports).toHaveLength(2);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    expect(harness.runtime.snapshot()).toMatchObject({
      state: "ready",
      capacity: 128,
      failure: null,
    });
    await harness.runtime.stop();
  });

  it("still fails closed when a monitored sidecar disconnects during heartbeat loss", async () => {
    const harness = runtimeHarness({
      livenessOutcomes: [null, new Error("sidecar pong timed out")],
    });
    await harness.runtime.start();
    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().availabilityReason).toBe("sidecar_suspect");

    harness.transport.snapshotValue.connected = false;
    harness.heartbeatUnreachable(503);

    expect(harness.runtime.snapshot()).toMatchObject({
      state: "failed",
      capacity: 0,
      transportConnected: false,
      failure: { kind: "heartbeat_unreachable", status: 503 },
    });
    expect(harness.sandboxFailures).toHaveLength(1);
    await harness.runtime.stop();
  });

  it("relaunches one R2 sidecar in the same sandbox session without provisioning", async () => {
    const failure = new Error("sidecar unavailable");
    const harness = runtimeHarness({
      enableR2Recovery: true,
      enableSupervisor: true,
      livenessOutcomes: [null, failure, failure, failure],
    });
    await harness.runtime.start();
    const originalNonce = harness.transport.binding!.launchNonce;

    await harness.clock.advanceBy(10_000);
    expect(harness.runtime.snapshot().availabilityReason).toBe("sidecar_suspect");
    expect(harness.transport.livenessProbes).toBe(2);
    await harness.clock.advanceBy(2_000);
    expect(harness.transport.livenessProbes).toBe(3);
    expect(harness.transport.recoverySuspends).toBe(0);
    await harness.clock.advanceBy(1_999);
    expect(harness.transport.recoverySuspends).toBe(0);
    await harness.clock.advanceBy(1);
    for (let turn = 0; turn < 200 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }
    for (
      let turn = 0;
      turn < 50 && harness.livenessDiagnostics.at(-1)?.outcome !== "succeeded";
      turn++
    ) await Promise.resolve();
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
    expect(harness.bootstrapSupervisors).toHaveLength(2);
    expect(harness.bootstrapSupervisors.every(value => value === harness.supervisor)).toBe(true);
    await harness.runtime.stop();
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(harness.supervisor.closes).toBe(1);
  });

  it.each([
    { name: "default 60-second", thresholdMs: 60_000, overrideMs: undefined },
    { name: "explicit 20-second", thresholdMs: 20_000, overrideMs: 20_000 },
  ])("keeps the $name reliable-stall allowance bounded by two samples", async scenario => {
    const harness = runtimeHarness({
      enableR2Recovery: true,
      livenessOutcomes: [null],
      sidecarCommitStallMs: scenario.overrideMs,
    });
    await harness.runtime.start();
    try {
      for (let elapsedMs = 10_000; elapsedMs < scenario.thresholdMs; elapsedMs += 10_000) {
        harness.transport.snapshotValue.diagnostics = {
          host: {
            ...stalledHostDiagnostics(),
            ackProgressAgeMs: elapsedMs,
            oldestQueuedFrameMs: elapsedMs,
          },
          sidecar: null,
        };
        await harness.clock.advanceBy(10_000);
        expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 128 });
        expect(harness.transport.recoverySuspends).toBe(0);
        expect(harness.bootstrap.forceStops).toBe(0);
      }
      harness.transport.snapshotValue.diagnostics = {
        host: {
          ...stalledHostDiagnostics(),
          ackProgressAgeMs: scenario.thresholdMs,
          oldestQueuedFrameMs: scenario.thresholdMs,
        },
        sidecar: null,
      };
      await harness.clock.advanceBy(10_000);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 0 });
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.bootstrap.forceStops).toBe(0);
      await harness.clock.advanceBy(1_999);
      expect(harness.transport.recoverySuspends).toBe(0);
      await harness.clock.advanceBy(1);
      for (let turn = 0; turn < 300 && harness.livenessDiagnostics.at(-1)?.outcome !== "succeeded"; turn++) {
        await Promise.resolve();
      }
      expect(harness.transport.recoverySuspends).toBe(1);
      expect(harness.bootstrap.forceStops).toBe(1);
      expect(harness.transports).toHaveLength(2);
      expect(harness.transports[1]!.recoveryAdopts).toBe(1);
      expect(harness.livenessDiagnostics.filter(event => event.errorKind === "reliable_stall"))
        .toHaveLength(2);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", capacity: 128 });
    } finally {
      await harness.runtime.stop();
    }
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
    await harness.clock.advanceBy(1_999);
    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    await harness.clock.advanceBy(1);
    for (let turn = 0; turn < 200 && harness.transports.length < 2; turn++) {
      await Promise.resolve();
    }

    for (
      let turn = 0;
      turn < 50 && harness.livenessDiagnostics.at(-1)?.outcome !== "succeeded";
      turn++
    ) await Promise.resolve();

    expect(harness.transport.livenessProbes).toBe(1);
    expect(harness.transport.recoverySuspends).toBe(1);
    expect(harness.transports).toHaveLength(2);
    expect(harness.transports[1]!.recoveryAdopts).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.livenessDiagnostics.map(event => ({
      signal: event.signal,
      outcome: event.outcome,
      errorKind: event.errorKind,
      reliableStall: event.reliableStall,
    }))).toEqual([
      {
        signal: "sidecar_probe",
        outcome: "failed",
        errorKind: "reliable_stall",
        reliableStall: true,
      },
      {
        signal: "sidecar_recovery",
        outcome: "waiting_for_transport_recovery",
        errorKind: undefined,
        reliableStall: undefined,
      },
      {
        signal: "sidecar_probe",
        outcome: "failed",
        errorKind: "reliable_stall",
        reliableStall: true,
      },
      {
        signal: "sidecar_recovery",
        outcome: "started",
        errorKind: undefined,
        reliableStall: true,
      },
      {
        signal: "sidecar_recovery",
        outcome: "succeeded",
        errorKind: undefined,
        reliableStall: undefined,
      },
    ]);
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
    await harness.clock.advanceBy(1_999);
    expect(harness.runtime.snapshot().capacity).toBe(0);
    await harness.clock.advanceBy(1);

    expect(harness.transport.recoverySuspends).toBe(0);
    expect(harness.transports).toHaveLength(1);
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.transport.livenessProbes).toBe(2);
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

  it.each(["data", "control"] as const)("repairs receive-only %s loss with fresh reliable proof and preserves committed lineage", async lane => {
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    const body = { model: "gpt-5.6-sol", service_tier: "fast", stream: true, store: true,
      input: "current delta", previous_response_id: "resp_exact_receive_prior" };
    const handle = await harness.runtime.run(body);
    harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics({ dataPeerAgeMs: lane === "data" ? 60_000 : 1 });
    let reconnects = 0;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async selected => {
      expect(selected).toBe(lane);
      reconnects++;
      harness.transport.snapshotValue.diagnostics = lane === "data"
        ? receiveStallDiagnostics({ inboundAcceptedSequence: 4, inboundProcessedSequence: 4,
          inboundAcceptedProgressAgeMs: 0, inboundProcessedProgressAgeMs: 0,
          dataPeerAgeMs: 0, sidecarPendingAgeMs: null })
        : receiveStallDiagnostics({ dataPeerAgeMs: 1, sidecarPendingAgeMs: null },
          { windowFrames: 0, flightCompletionsTotal: 2 });
    };
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 300 && !harness.livenessDiagnostics.some(event =>
        event.signal === "awareness" && event.outcome === "succeeded"); turn++) await Promise.resolve();
      expect(reconnects).toBe(1);
      expect(harness.runtime.snapshot().availabilityReason).toBe("available");
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.bootstrap.forceStops).toBe(0);
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.transports).toHaveLength(1);
      expect(harness.transport.bodies).toEqual([body]);
      expect(harness.livenessDiagnostics).toEqual(expect.arrayContaining([
        expect.objectContaining({ signal: "awareness", outcome: "succeeded", awarenessLane: lane,
          reconnectPhase: "host_reconnect" }),
      ]));
      harness.transport.pending[0]!.resolve({ id: "resp_exact_receive_next", status: "completed" });
      await expect(handle.final).resolves.toMatchObject({ id: "resp_exact_receive_next" });
    } finally {
      harness.transport.pending[0]?.resolve({ status: "completed" });
      await harness.runtime.stop();
    }
  });

  it.each([
    { name: "first matching publication", revision: 0, publications: 1, age: 0, lane: "control" },
    { name: "newer unseen publication", revision: 0, publications: 2, age: 0, lane: "data" },
    { name: "native DATA ahead of delayed diagnostics", revision: 16, publications: 16, age: 1, lane: "control" },
    { name: "exact DATA freshness boundary", revision: 15, publications: 16, age: 20_000, lane: "control" },
    { name: "expired DATA freshness", revision: 15, publications: 16, age: 20_001, lane: "data" },
    { name: "missing DATA revision", revision: undefined, publications: 16, age: 1, lane: "data" },
    { name: "null DATA revision", revision: null, publications: 16, age: 1, lane: "data" },
    { name: "negative DATA revision", revision: -1, publications: 16, age: 1, lane: "data" },
    { name: "fractional DATA revision", revision: 15.5, publications: 16, age: 1, lane: "data" },
    { name: "saturated DATA revision", revision: Number.MAX_SAFE_INTEGER, publications: 16, age: 1, lane: "data" },
    { name: "zero publication count", revision: 15, publications: 0, age: 1, lane: "data" },
    { name: "negative publication count", revision: 15, publications: -1, age: 1, lane: "data" },
    { name: "fractional publication count", revision: 15, publications: 1.5, age: 1, lane: "data" },
    { name: "saturated publication count", revision: 15, publications: Number.MAX_SAFE_INTEGER, age: 1, lane: "data" },
    { name: "negative DATA age", revision: 15, publications: 16, age: -1, lane: "data" },
    { name: "non-finite DATA age", revision: 15, publications: 16, age: Number.NaN, lane: "data" },
    { name: "missing DATA age", revision: 15, publications: 16, age: null, lane: "data" },
  ] as const)("chooses $lane repair for $name", async scenario => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics({
      lastDataRevision: scenario.revision, dataPeerAgeMs: scenario.age,
      sidecarPendingDataPublications: 1,
    }, { dataPublicationsTotal: scenario.publications });
    const repairs: string[] = [];
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async lane => {
      repairs.push(lane);
      harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics({
        inboundAcceptedSequence: 4, inboundProcessedSequence: 4,
        inboundAcceptedProgressAgeMs: 0, inboundProcessedProgressAgeMs: 0,
        sidecarPendingAgeMs: null,
      }, { windowFrames: 0 });
    };
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(repairs).toEqual([scenario.lane]);
      expect(harness.runtime.snapshot().availabilityReason).toBe("available");
      expect(harness.ySweetMints()).toBe(0);
    } finally { await harness.runtime.stop(); }
  });

  it.each([
    { name: "stale sidecar sample", host: { sidecarDiagnosticsAgeMs: 20_001 } },
    { name: "stale control lane", host: { controlPeerAgeMs: 20_001 } },
    { name: "new pending output after provider thinking", host: { sidecarPendingAgeMs: 59_999 } },
    { name: "no peer data publication progress", host: { sidecarPendingDataPublications: 0 } },
    { name: "client or memory backpressure", host: { inboundProcessingFrames: 1 } },
    { name: "accepted but unprocessed data", host: { inboundProcessedSequence: 2 } },
    { name: "recent accepted progress", host: { inboundAcceptedProgressAgeMs: 59_999 } },
    { name: "recent processed progress", host: { inboundProcessedProgressAgeMs: 59_999 } },
    { name: "provider thinking without pending output", host: {}, sidecar: { windowFrames: 0, schedulerFrames: 0 } },
  ])("does not recover receive transport from $name", async scenario => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics(scenario.host, scenario.sidecar);
    let reconnects = 0;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async () => { reconnects++; };
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(reconnects).toBe(0);
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.bootstrap.forceStops).toBe(0);
      expect(harness.transport.livenessProbes).toBe(2);
      expect(harness.runtime.snapshot().availabilityReason).toBe("available");
    } finally { await harness.runtime.stop(); }
  });

  it.each(["data", "control"] as const)("rejects a pong-only %s repair as receive recovery proof", async lane => {
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics({ dataPeerAgeMs: lane === "data" ? 60_000 : 1 });
    let reconnects = 0;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async () => { reconnects++; };
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 300 && harness.ySweetMints() === 0; turn++) await Promise.resolve();
      expect(reconnects).toBe(1);
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.livenessDiagnostics).toEqual(expect.arrayContaining([
        expect.objectContaining({ signal: "awareness", outcome: "failed", awarenessLane: lane,
          reconnectPhase: "host_reconnect", errorKind: "unavailable" }),
      ]));
      expect(harness.livenessDiagnostics.filter(event => event.outcome === "succeeded" &&
        event.reconnectPhase === "host_reconnect")).toHaveLength(0);
      expect(harness.bootstrap.forceStops).toBe(0);
    } finally { await harness.runtime.stop(); }
  });

  it("keeps one receive-reconnect owner and aborts it on shutdown", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    harness.transport.snapshotValue.diagnostics = receiveStallDiagnostics();
    let reconnects = 0;
    let reconnectSignal: AbortSignal | undefined;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async (_lane, signal) => {
      reconnects++;
      reconnectSignal = signal;
      await harness.clock.sleep(60_000, signal);
    };
    await harness.clock.advanceBy(10_000);
    for (let turn = 0; turn < 100 && reconnects === 0; turn++) await Promise.resolve();
    await harness.clock.advanceBy(2_000);
    expect(reconnects).toBe(1);
    expect(harness.ySweetMints()).toBe(0);
    await harness.runtime.stop();
    expect(reconnectSignal?.aborted).toBe(true);
    expect(harness.clock.waiters).toHaveLength(0);
  });

  it("repairs a silent control lane after an exact probe timeout while data and heartbeat stay live", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    const body = { model: "gpt-5.6-sol", service_tier: "fast", input: "current turn", previous_response_id: "resp_exact_prior" };
    const handle = await harness.runtime.run(body);
    let timedProbe = false;
    let reconnects = 0;
    harness.transport.snapshotValue.sidecarPeerAgeMs = 1;
    harness.transport.probeLiveness = async signal => {
      timedProbe = true;
      await harness.clock.sleep(30_000, signal);
      throw new V2TransportLivenessError("silent control challenge timed out", "timeout");
    };
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async (lane, signal) => {
      reconnects++;
      expect(lane).toBe("control");
      expect(signal?.aborted).toBe(false);
      harness.transport.probeLiveness = async () => ({ rttMs: 1, sentAtMs: harness.clock.now() });
    };
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 100 && !timedProbe; turn++) await Promise.resolve();
      expect(timedProbe).toBe(true);
      await harness.clock.advanceBy(30_000);
      for (let turn = 0; turn < 300 && reconnects === 0; turn++) await Promise.resolve();
      expect(reconnects).toBe(1);
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().availabilityReason !== "available"; turn++) await Promise.resolve();
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", transportConnected: true, availabilityReason: "available" });
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.transport.rotations).toBe(0);
      expect(harness.bootstrap.forceStops).toBe(0);
      expect(harness.transports).toHaveLength(1);
      expect(harness.transport.bodies).toEqual([body]);
      for (let turn = 0; turn < 100 && !harness.livenessDiagnostics.some(event => event.signal === "awareness" && event.outcome === "succeeded"); turn++) await Promise.resolve();
      expect(harness.livenessDiagnostics.filter(event => event.signal === "awareness"))
        .toEqual(expect.arrayContaining([
          expect.objectContaining({ outcome: "started", awarenessLane: "control" }),
          expect.objectContaining({ outcome: "succeeded", awarenessLane: "control", reconnectPhase: "host_reconnect" }),
        ]));
      harness.transport.pending[0]!.resolve({ id: "resp_exact_next", status: "completed" });
      await expect(handle.final).resolves.toMatchObject({ id: "resp_exact_next" });
    } finally {
      harness.transport.pending[0]?.resolve({ id: "resp_exact_next", status: "completed" });
      await harness.runtime.stop();
    }
  });

  it("bounds silent-lane reconnect before credential fallback and logs the failed lane", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true,
      livenessOutcomes: [null, new V2TransportLivenessError("silent control", "timeout"), null] });
    let reconnectSignal: AbortSignal | undefined;
    (harness.transport as V2RuntimeTransport).reconnectAwareness = async (_lane, signal) => {
      reconnectSignal = signal;
      await harness.clock.sleep(60_000, signal);
    };
    await harness.runtime.start();
    try {
      await harness.clock.advanceBy(10_000);
      for (let turn = 0; turn < 300 && !reconnectSignal; turn++) await Promise.resolve();
      expect(reconnectSignal?.aborted).toBe(false);
      await harness.clock.advanceBy(9_999);
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.bootstrap.forceStops).toBe(0);
      await harness.clock.advanceBy(1);
      for (let turn = 0; turn < 300 && harness.ySweetMints() === 0; turn++) await Promise.resolve();
      expect(reconnectSignal?.aborted).toBe(true);
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.livenessDiagnostics).toEqual(expect.arrayContaining([
        expect.objectContaining({ signal: "awareness", outcome: "failed", awarenessLane: "control",
          reconnectPhase: "host_reconnect", errorKind: "unavailable", durationMs: 10_000 }),
      ]));
    } finally { await harness.runtime.stop(); }
  });

  it("reconnects the existing credential and preserves one committed request before reminting", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1_200_000,
      enableSupervisor: true,
      livenessOutcomes: [null, null],
    });
    await harness.runtime.start();
    const handle = await harness.runtime.run({
      model: "gpt-6-astra",
      input: "offline retained request",
      previous_response_id: "resp_existing",
    });
    try {
      harness.awareness.fail(new SplitAwarenessChannelError(
        "data", new AwarenessChannelError("offline disconnect", "disconnected", 1005),
      ));
      expect(harness.runtime.snapshot().available).toBe(0);
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) {
        await Promise.resolve();
      }
      expect(harness.ySweetMints()).toBe(0);
      expect(harness.transport.awarenessResumes).toBe(1);
      expect(harness.transport.livenessProbes).toBe(2);
      expect(harness.transport.rotations).toBe(0);
      expect(harness.awareness.rotations).toBe(0);
      expect(harness.supervisor.rotations).toBe(0);
      expect(harness.transports).toHaveLength(1);
      expect(harness.provisions()).toBe(1);
      expect(harness.bootstrap.forceStops).toBe(0);
      expect(harness.transport.bodies).toHaveLength(1);
      expect(harness.transport.bodies[0]).toMatchObject({ previous_response_id: "resp_existing" });
      harness.transport.pending[0]!.resolve({ id: "resp_finished", status: "completed" });
      await expect(handle.final).resolves.toMatchObject({ id: "resp_finished" });
    } finally {
      harness.transport.pending[0]?.resolve({ status: "completed" });
      await harness.runtime.stop();
      expect(harness.clock.waiters).toHaveLength(0);
    }
  });

  it("bounds an existing-credential reconnect before falling back to one rotation", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1_200_000, enableSupervisor: true, livenessOutcomes: [null],
    });
    await harness.runtime.start();
    let reconnectAborted = false;
    harness.transport.resumeAwareness = async (signal?: AbortSignal) => {
      harness.transport.awarenessResumes++;
      if (harness.transport.awarenessResumes === 1) {
        try { await harness.clock.sleep(60_000, signal); }
        finally { reconnectAborted = signal?.aborted === true; }
      }
    };
    try {
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1006));
      for (let turn = 0; turn < 300 && harness.transport.awarenessResumes === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(0);
      await harness.clock.advanceBy(2_999);
      expect(harness.ySweetMints()).toBe(0);
      await harness.clock.advanceBy(1);
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) await Promise.resolve();
      expect(reconnectAborted).toBe(true);
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.transport.rotations).toBe(1);
      expect(harness.supervisor.rotations).toBe(1);
      expect(harness.transport.awarenessResumes).toBe(2);
      expect(harness.runtime.snapshot().available).toBeGreaterThan(0);
      expect(harness.transports).toHaveLength(1);
    } finally {
      await harness.runtime.stop();
      expect(harness.clock.waiters).toHaveLength(0);
    }
  });

  it("does not postpone scheduled credential renewal after a transient reconnect", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000, livenessOutcomes: [null] });
    await harness.runtime.start();
    try {
      await harness.clock.advanceBy(299_000);
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1006));
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(0);
      await harness.clock.advanceBy(1_000);
      for (let turn = 0; turn < 300 && harness.transport.rotations === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.transport.rotations).toBe(1);
    } finally { await harness.runtime.stop(); }
  });

  it("keeps authentication rejection authoritative when another lane also disconnects", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1_200_000, enableSupervisor: true, livenessOutcomes: [null],
    });
    await harness.runtime.start();
    try {
      harness.awareness.fail(new AwarenessChannelError("offline authentication rejection", "auth", 401));
      harness.awareness.fail(new AwarenessChannelError("offline data disconnect", "disconnected", 1006));
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.supervisor.rotations).toBe(1);
      expect(harness.transport.rotations).toBe(1);
      expect(harness.transport.awarenessResumes).toBe(1);
      expect(harness.transport.livenessProbes).toBe(1);
      expect(harness.runtime.snapshot().available).toBeGreaterThan(0);
    } finally { await harness.runtime.stop(); }
  });

  it("falls back to credential rotation when the exact-sidecar reconnect pong fails", async () => {
    const harness = runtimeHarness({
      resourceExpiresInMs: 1_200_000, enableSupervisor: true,
      livenessOutcomes: [null, new V2TransportLivenessError("offline pong timeout", "timeout")],
    });
    await harness.runtime.start();
    try {
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.transport.awarenessResumes).toBe(2);
      expect(harness.transport.livenessProbes).toBe(2);
      expect(harness.transport.rotations).toBe(1);
      expect(harness.supervisor.rotations).toBe(1);
      expect(harness.transports).toHaveLength(1);
      expect(harness.bootstrap.forceStops).toBe(0);
    } finally { await harness.runtime.stop(); }
  });

  it("does not clear a newer authentication failure with a stale reconnect pong", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000, livenessOutcomes: [null] });
    await harness.runtime.start();
    let releaseProbe!: () => void;
    let probing = false;
    harness.transport.probeLiveness = async () => {
      probing = true;
      await new Promise<void>(resolve => { releaseProbe = resolve; });
      return { rttMs: 1, sentAtMs: harness.clock.now() };
    };
    try {
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 300 && !probing; turn++) await Promise.resolve();
      expect(probing).toBe(true);
      expect(harness.ySweetMints()).toBe(0);
      harness.awareness.fail(new AwarenessChannelError("offline authentication rejection", "auth", 403));
      releaseProbe();
      for (let turn = 0; turn < 300 && harness.runtime.snapshot().available === 0; turn++) await Promise.resolve();
      expect(harness.ySweetMints()).toBe(1);
      expect(harness.transport.rotations).toBe(1);
      expect(harness.awareness.rotationTokens).toEqual(["y-token-1"]);
      expect(harness.livenessDiagnostics.some(event => event.outcome === "superseded")).toBe(true);
      expect(harness.runtime.snapshot().available).toBeGreaterThan(0);
    } finally { releaseProbe?.(); await harness.runtime.stop(); }
  });

  it("aborts an existing-credential reconnect on shutdown without a late rotation", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000, livenessOutcomes: [null] });
    await harness.runtime.start();
    let aborted = false;
    harness.transport.resumeAwareness = async (signal?: AbortSignal) => {
      harness.transport.awarenessResumes++;
      try { await harness.clock.sleep(60_000, signal); }
      finally { aborted = signal?.aborted === true; }
    };
    harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1006));
    for (let turn = 0; turn < 300 && harness.transport.awarenessResumes === 0; turn++) await Promise.resolve();
    expect(harness.transport.awarenessResumes).toBe(1);
    await harness.runtime.stop();
    expect(aborted).toBe(true);
    expect(harness.clock.waiters).toHaveLength(0);
    await harness.clock.advanceBy(60_000);
    expect(harness.ySweetMints()).toBe(0);
    expect(harness.transport.rotations).toBe(0);
    expect(harness.runtime.snapshot().state).toBe("stopped");
  });

  it.each(["supervisor", "sidecar"] as const)("fences a late %s credential rotation before host reconnection", async phase => {
    const harness = runtimeHarness({ enableSupervisor: true, livenessOutcomes: [null] });
    await harness.runtime.start();
    let releaseRotation!: () => void;
    let rotating = false;
    const pendingRotation = async () => {
      rotating = true;
      await new Promise<void>(resolve => { releaseRotation = resolve; });
    };
    if (phase === "supervisor") harness.supervisor.rotateCredential = pendingRotation;
    else harness.transport.rotateCredential = pendingRotation;
    try {
      if (phase === "supervisor") {
        harness.awareness.fail(new AwarenessChannelError("offline credential rejection", "auth", 401));
      } else {
        await harness.clock.advanceBy(300_000);
      }
      for (let turn = 0; turn < 100 && !rotating; turn++) await Promise.resolve();
      expect(rotating).toBe(true);
      const stopping = harness.runtime.stop();
      releaseRotation();
      await stopping;
      expect(harness.awareness.rotations).toBe(0);
      expect(harness.transport.awarenessResumes).toBe(0);
      expect(harness.runtime.snapshot().state).toBe("stopped");
      expect(harness.clock.waiters).toHaveLength(0);
    } finally {
      releaseRotation?.();
      await harness.runtime.stop();
    }
  });

  it("remints and reconnects Y-Sweet immediately when exact-sidecar reconnect proof is unavailable", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000, enableSupervisor: true });
    await harness.runtime.start();
    expect(harness.runtime.snapshot().capacity).toBe(128);

    harness.awareness.fail(new SplitAwarenessChannelError(
      "control",
      new AwarenessChannelError("synthetic disconnect", "disconnected", 1006),
    ));
    expect(harness.runtime.snapshot()).toMatchObject({ capacity: 0, available: 0 });
    for (let turn = 0; turn < 300 && harness.runtime.snapshot().capacity === 0; turn++) {
      await Promise.resolve();
    }
    for (
      let turn = 0;
      turn < 50 && harness.livenessDiagnostics.at(-1)?.outcome !== "succeeded";
      turn++
    ) await Promise.resolve();

    expect(harness.ySweetMints()).toBe(1);
    expect(harness.transport.rotations).toBe(1);
    expect(harness.transport.awarenessResumes).toBe(1);
    expect(harness.awareness.rotationTokens).toEqual(["y-token-1"]);
    expect(harness.supervisor.rotations).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("ready");
    expect(harness.runtime.snapshot().capacity).toBe(128);
    expect(harness.transports).toHaveLength(1);
    expect(harness.provisions()).toBe(1);
    expect(harness.livenessDiagnostics.map(event => ({
      signal: event.signal,
      outcome: event.outcome,
      errorName: event.errorName,
      errorKind: event.errorKind,
      lane: event.awarenessLane,
      connectionCode: event.connectionCode,
    }))).toEqual([
      {
        signal: "awareness",
        outcome: "failed",
        errorName: "SplitAwarenessChannelError",
        errorKind: "disconnected",
        lane: "control",
        connectionCode: 1006,
      },
      {
        signal: "awareness",
        outcome: "started",
        errorName: undefined,
        errorKind: undefined,
        lane: undefined,
        connectionCode: undefined,
      },
      {
        signal: "awareness",
        outcome: "succeeded",
        errorName: undefined,
        errorKind: undefined,
        lane: undefined,
        connectionCode: undefined,
      },
    ]);
    await harness.runtime.stop();
  });

  it("keeps one reconnect owner through rotation without starting competing sidecar recovery", async () => {
    const failure = new Error("connection unavailable");
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true,
      initialRotationPending: true, resourceExpiresInMs: 1_200_000,
      // The failed fast-reconnect pong transfers ownership to credential
      // fallback; periodic probes must not compete with that pending owner.
      livenessOutcomes: [null, failure, failure, failure, failure, null] });
    await harness.runtime.start();
    try {
      harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
      for (let turn = 0; turn < 300 && harness.transport.rotations !== 1; turn++) await Promise.resolve();
      expect(harness.transport.rotations).toBe(1);
      await harness.clock.advanceBy(10_000);
      await harness.clock.advanceBy(2_000);
      await harness.clock.advanceBy(2_000);
      for (let turn = 0; turn < 100; turn++) await Promise.resolve();
      expect(harness.livenessDiagnostics.some(event => event.signal === "sidecar_recovery" && event.outcome === "started")).toBe(false);
      expect(harness.bootstrap.forceStops).toBe(0);
      harness.transport.probeLiveness = async () => ({ rttMs: 1, sentAtMs: harness.clock.now() });
      harness.releaseInitialRotation();
      const recovered = () => harness.livenessDiagnostics.some(event => event.signal === "awareness" && event.outcome === "succeeded");
      for (let turn = 0; turn < 500 && !recovered(); turn++) await Promise.resolve();
      expect(recovered()).toBe(true);
      expect(harness.transports).toHaveLength(1);
      expect(harness.bootstrap.forceStops).toBe(0);
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready", transportConnected: true });
    } finally { harness.releaseInitialRotation(); await harness.runtime.stop(); }
  });

  it("retains the supervisor failure name and exact bounded reconnect phase", async () => {
    const harness = runtimeHarness({ resourceExpiresInMs: 1_200_000, enableSupervisor: true });
    await harness.runtime.start();
    harness.supervisor.rotateCredential = async () => {
      throw new V2SupervisorError("private failure detail", "timeout", undefined, "challenge");
    };
    harness.awareness.fail(new AwarenessChannelError("offline disconnect", "disconnected", 1005));
    for (let i = 0; i < 300 && !harness.livenessDiagnostics.some(event => event.supervisorPhase === "challenge"); i++) {
      await Promise.resolve();
    }
    expect(harness.livenessDiagnostics.at(-1)).toMatchObject({
      signal: "awareness", outcome: "failed", errorName: "V2SupervisorError", errorKind: "timeout",
      reconnectPhase: "supervisor_rotation", supervisorPhase: "challenge",
    });
    expect(JSON.stringify(harness.livenessDiagnostics)).not.toContain("private failure detail");
    await harness.runtime.stop();
  });

  it("locally fences a created R2 candidate and reports its unproven remote cleanup", async () => {
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
    let candidateForceStarted = false;
    harness.transports[1]!.forceStop = async () => {
      candidateForceStarted = true;
      return new Promise<void>(() => undefined);
    };

    let stopSettled = false;
    const stopping = harness.runtime.stop().then(() => null, error => error).finally(() => { stopSettled = true; });
    for (let turn = 0; turn < 100 && !stopSettled; turn++) await Promise.resolve();
    await harness.clock.advanceBy(25);
    for (let turn = 0; turn < 200 && !stopSettled; turn++) await Promise.resolve();

    expect(stopSettled).toBe(true);
    expect(candidateForceStarted).toBe(true);
    expect(harness.transports[1]!.closes).toBe(1);
    expect(harness.transports[1]!.recoveryFailures).toBe(1);
    expect(harness.bootstraps[1]!.forceStops).toBe(1);
    expect(harness.sandboxFailures).toHaveLength(0);
    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
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

  for (const rejection of [
    { label: "model unavailability", status: 404, code: "model_not_found", type: "invalid_request_error", param: "model", message: "The requested model is unavailable" },
    { label: "long TPM rate limit", status: 429, code: "rate_limit_exceeded", type: "rate_limit_error", param: null, message: "Rate limit reached on tokens per min (TPM): Limit 40000000, Used 40000000, Requested 609930. Please try again in 914ms. ".repeat(8) },
  ] as const) {
    for (const origin of ["http", "sse"] as const) {
      for (const boundary of ["start", "final"] as const) {
        it(`keeps definitive ${origin} ${rejection.label} at ${boundary} health-neutral without replacing the sandbox`, async () => {
          const harness = runtimeHarness({ enableR2Recovery: true });
          const recoveryEvents: SandboxSupervisorEvent[] = [];
          let replacements = 0;
          const supervised = new SupervisedSandboxRuntime({
            projectId: harness.runtime.projectId,
            initialChild: harness.runtime,
            createChild: () => {
              replacements++;
              throw new Error("provider rejection must not replace a healthy runtime");
            },
            clock: harness.clock,
            random: { next: () => 0 },
            onRecoveryEvent: event => recoveryEvents.push(event),
          });
          try {
            await supervised.start();
            const baseline = supervised.snapshot();
            const startupEvents = recoveryEvents.length;
            const providerError = await createProviderErrorEnvelope({
              origin,
              status: rejection.status,
              model: "gpt-6-astra",
              value: { error: {
                message: rejection.message,
                type: rejection.type,
                param: rejection.param,
                code: rejection.code,
              } },
            });
            const rejected = new V2CommittedError(
              providerError.error.message,
              rejection.code,
              false,
              undefined,
              providerError,
            );
            if (boundary === "start") {
              harness.transport.run = async () => { throw rejected; };
              await expect(supervised.run({ model: "gpt-6-astra", input: "unavailable model" }))
                .rejects.toBe(rejected);
            } else {
              const handle = await supervised.run({ model: "gpt-6-astra", input: "unavailable model" });
              harness.transport.pending.at(-1)!.reject(rejected);
              await expect(handle.final).rejects.toBe(rejected);
            }
            await harness.clock.advanceBy(1_000);
            expect(supervised.snapshot()).toMatchObject({
              state: "ready",
              incarnation: 1,
              capacity: baseline.capacity,
              available: baseline.available,
              active: 0,
              quarantined: 0,
              recentFailureRate: 0,
              completionEwmaMs: baseline.completionEwmaMs,
              failure: null,
            });
            expect(supervised.supervisorSnapshot()).toMatchObject({ phase: "healthy", childGeneration: 1 });
            expect(replacements).toBe(0);
            expect(recoveryEvents).toHaveLength(startupEvents);
            expect(harness.sandboxFailures).toHaveLength(0);
            expect(harness.transports).toHaveLength(1);
            expect(harness.transport.recoverySuspends).toBe(0);
            expect(harness.transport.forceStops).toBe(0);
          } finally {
            await supervised.stop();
          }
        });
      }
    }
  }

  it("keeps repeated cyber_policy refusals neutral while other and later requests succeed", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true });
    const recoveryEvents: SandboxSupervisorEvent[] = [];
    let replacements = 0;
    const supervised = new SupervisedSandboxRuntime({
      projectId: harness.runtime.projectId,
      initialChild: harness.runtime,
      createChild: () => {
        replacements++;
        throw new Error("cyber_policy must not replace a healthy runtime");
      },
      clock: harness.clock,
      random: { next: () => 0 },
      onRecoveryEvent: event => recoveryEvents.push(event),
    });
    try {
      await supervised.start();
      const baseline = supervised.snapshot();
      const startupEvents = recoveryEvents.length;
      const concurrent = await supervised.run({ model: "gpt-5.6-sol", input: "concurrent" });
      const concurrentOutcome = harness.transport.pending.at(-1)!;
      const providerError = await createProviderErrorEnvelope({
        origin: "sse",
        status: 500,
        value: { error: {
          message: "This content was flagged for possible cybersecurity risk.",
          type: "error",
          code: "cyber_policy",
        } },
      });
      for (let index = 0; index < 3; index++) {
        const handle = await supervised.run({ model: "gpt-5.6-sol", input: `refused-${index}` });
        const rejected = new V2CommittedError(
          providerError.error.message, "cyber_policy", false, undefined, providerError,
        );
        harness.transport.pending.at(-1)!.reject(rejected);
        await expect(handle.final).rejects.toBe(rejected);
        await harness.clock.advanceBy(1_000);
        expect(supervised.snapshot()).toMatchObject({
          state: "ready",
          incarnation: baseline.incarnation,
          capacity: baseline.capacity,
          available: baseline.available - 1,
          active: 1,
          quarantined: 0,
          recentFailureRate: 0,
          completionEwmaMs: baseline.completionEwmaMs,
          failure: null,
        });
        expect(harness.transport.bodies).toHaveLength(index + 2);
      }

      concurrentOutcome.resolve({ id: "resp_concurrent", status: "completed" });
      await expect(concurrent.final).resolves.toMatchObject({ id: "resp_concurrent" });
      const next = await supervised.run({ model: "gpt-5.6-sol", input: "next" });
      harness.transport.pending.at(-1)!.resolve({ id: "resp_next", status: "completed" });
      await expect(next.final).resolves.toMatchObject({ id: "resp_next" });
      expect(supervised.snapshot()).toMatchObject({
        state: "ready", active: 0, available: baseline.available, recentFailureRate: 0,
      });
      expect(supervised.supervisorSnapshot()).toMatchObject({ phase: "healthy", childGeneration: 1 });
      expect(replacements).toBe(0);
      expect(recoveryEvents).toHaveLength(startupEvents);
      expect(harness.sandboxFailures).toHaveLength(0);
      expect(harness.transports).toHaveLength(1);
      expect(harness.transport.bodies).toHaveLength(5);
      expect(harness.transport.recoverySuspends).toBe(0);
      expect(harness.transport.forceStops).toBe(0);
      expect(harness.transport.drains).toBe(0);
    } finally {
      await supervised.stop();
    }
  });

  it("still penalizes model-unavailable wording when its execution outcome is ambiguous", async () => {
    const harness = runtimeHarness();
    try {
      await harness.runtime.start();
      const handle = await harness.runtime.run({ model: "gpt-6-astra", input: "ambiguous model rejection" });
      harness.transport.pending.at(-1)!.reject(new V2CommittedError(
        "The requested model is unavailable but terminal proof was lost",
        "model_not_found",
        true,
      ));
      await expect(handle.final).rejects.toMatchObject({ code: "model_not_found", ambiguous: true });
      expect(harness.runtime.snapshot()).toMatchObject({ state: "ready" });
      expect(harness.runtime.snapshot().recentFailureRate).toBeCloseTo(0.20);
    } finally {
      await harness.runtime.stop();
    }
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

  it("keeps the project connection alive until supervisor cleanup and close settle", async () => {
    const harness = runtimeHarness({ enableR2Recovery: true, enableSupervisor: true });
    await harness.runtime.start();
    const events: string[] = [];
    const shutdown = Promise.withResolvers<void>();
    const closed = Promise.withResolvers<void>();
    harness.supervisor.shutdown = async () => { events.push("shutdown"); await shutdown.promise; };
    harness.supervisor.close = async () => { events.push("close"); await closed.promise; };
    harness.ydoc.destroy = async () => { events.push("destroy"); };
    const stopping = harness.runtime.stop();
    for (let i = 0; i < 100 && !events.length; i++) await Promise.resolve();
    expect(events).toEqual(["shutdown"]);
    shutdown.resolve();
    for (let i = 0; i < 100 && events.length < 2; i++) await Promise.resolve();
    expect(events).toEqual(["shutdown", "close"]);
    closed.resolve(); await stopping;
    expect(events).toEqual(["shutdown", "close", "destroy"]);
  });

  it.each(["stop", "drain"] as const)("reserves live supervisor cleanup time after a silent transport %s", async kind => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, shutdownDeadlineMs: () => shutdownDeadline });
    await harness.runtime.start();
    const retirement = Promise.withResolvers<void>();
    const proof = Promise.withResolvers<void>();
    let supervisorSignal: AbortSignal | undefined;
    let enteredDrain = false;
    harness.transport.drain = async (_reason?: string, signal?: AbortSignal) => {
      enteredDrain = true;
      if (signal?.aborted) throw signal.reason;
      await new Promise<void>((_resolve, reject) => signal?.addEventListener("abort", () => reject(signal.reason), { once: true }));
    };
    harness.transport.forceStop = async () => { harness.transport.forceStops++; await retirement.promise; };
    harness.bootstrap.forceStop = async () => { harness.bootstrap.forceStops++; await retirement.promise; };
    harness.transport.close = async () => { harness.transport.closes++; retirement.resolve(); };
    harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
      harness.supervisor.shutdowns++;
      supervisorSignal = signal;
      signal?.throwIfAborted();
      await proof.promise;
      signal?.throwIfAborted();
    };
    shutdownDeadline = harness.clock.now() + 60_000;
    let settled = false;
    const stopping = (kind === "drain" ? harness.runtime.drain("silent-mixed-flight") : harness.runtime.stop())
      .then(() => null, error => error).finally(() => { settled = true; });
    try {
      for (let turn = 0; turn < 100 && !(kind === "drain" ? enteredDrain : harness.transport.forceStops); turn++) await Promise.resolve();
      if (kind === "drain") {
        await harness.clock.advanceBy(45_000);
        for (let turn = 0; turn < 100 && !harness.transport.forceStops; turn++) await Promise.resolve();
        expect(harness.transport.forceStops).toBe(1);
        await harness.clock.advanceBy(7_500);
      } else {
        await harness.clock.advanceBy(45_000);
      }
      for (let turn = 0; turn < 100 && !harness.supervisor.shutdowns; turn++) await Promise.resolve();
      expect(harness.supervisor.shutdowns).toBe(1);
      expect(supervisorSignal?.aborted).toBe(false);
      expect(settled).toBe(false);
      expect(harness.clock.now()).toBeLessThan(shutdownDeadline);
      proof.resolve();
      await expect(stopping).resolves.toBeNull();
      expect(harness.supervisor.closes).toBe(1);
      expect(harness.transport.closes).toBe(1);
    } finally {
      retirement.resolve(); proof.resolve();
      await harness.clock.advanceBy(60_000);
      await stopping;
    }
  });

  it("obtains exact supervisor cleanup when the real R10 peer refuses a mixed draining flight", async () => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, stopTimeoutMs: 1_000,
      shutdownDeadlineMs: () => shutdownDeadline });
    const binding: ProtocolBinding = { protocolRevision: "r2", projectId: PROJECT,
      sessionId: "sandbox-session-1", leaderGeneration: 4, buildHash: "a".repeat(64),
      schemaHash: "b".repeat(64), launchNonce: "mixed-drain-launch" };
    const providers = new Map<string, { awareness: Awareness }>();
    const docs = { data: new Y.Doc(), control: new Y.Doc() };
    const host = { data: new Awareness(docs.data), control: new Awareness(docs.control) };
    const accepted: HostFrame[] = [];
    const peer = new R10DenoAwarenessPeer({
      binding, instanceId: "mixed-drain-sidecar",
      credential: { url: "wss://prism.test/y/d/offline/ws", baseUrl: "https://prism.test/y/d/offline",
        docId: PROJECT, token: "offline-only", authorization: "full", rotationKey: createCredentialRotationKey() },
      engine: { snapshot: () => ({ ready: true, credits: 8, active: 0, queued: 0, draining: false }),
        setHostCeiling() {}, accept: async (frame: HostFrame) => { accepted.push(frame); } } as any,
      providerFactory: (doc, _credential, lane) => {
        const listeners = new Map<string, Set<(...args: any[]) => void>>();
        const awareness = new Awareness(doc);
        const provider = {
          awareness,
          on(event: string, listener: (...args: any[]) => void) {
            if (!listeners.has(event)) listeners.set(event, new Set());
            listeners.get(event)!.add(listener);
          },
          off(event: string, listener: (...args: any[]) => void) { listeners.get(event)?.delete(listener); },
          connect() { queueMicrotask(() => { for (const listener of listeners.get("sync") ?? []) listener(true); }); },
          disconnect() {}, destroy() { awareness.destroy(); },
        };
        providers.set(lane, provider);
        return provider;
      },
      publicationIntervalMs: 60_000, retransmitMs: 60_000, connectTimeoutMs: 100,
    });
    let revision = 0;
    const send = (lane: "data" | "control", draining: boolean, frames: unknown[] = []) => {
      host[lane].setLocalStateField(lane === "data" ? "ghostprovV2Data" : "ghostprovV2Control", {
        protocol: "gp-awareness-v2", schemaVersion: 2, ...binding, peerId: "host",
        instanceId: "mixed-drain-host", targetInstanceId: "mixed-drain-sidecar", lane,
        epoch: 1, revision: revision++, ack: { epoch: 1, sequence: -1 }, credits: 8,
        active: 0, queued: 0, draining, frames,
      });
      applyAwarenessUpdate(providers.get(lane)!.awareness,
        encodeAwarenessUpdate(host[lane], [docs[lane].clientID]), "offline-runtime-composition");
    };
    const proof = Promise.withResolvers<void>();
    let supervisorSignal: AbortSignal | undefined;
    let entered = false;
    let settled = false;
    let stopping: Promise<void> | undefined;
    try {
      await peer.connect();
      await harness.runtime.start();
      send("control", false);
      harness.transport.drain = async (_reason?: string, signal?: AbortSignal) => {
        entered = true;
        send("control", true);
        send("data", true, [
          { kind: "request_open", requestId: "mixed-new-request", requestDigest: "c".repeat(64),
            decodedBytes: 1, encodedBytes: 1, encoding: "identity", partCount: 1, epoch: 1, sequence: 0 },
          { kind: "drain", reason: "shutdown", epoch: 1, sequence: 1 },
        ]);
        await new Promise<void>((_resolve, reject) => signal?.addEventListener("abort", () => reject(signal.reason), { once: true }));
      };
      harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
        harness.supervisor.shutdowns++; supervisorSignal = signal;
        signal?.throwIfAborted(); await proof.promise; signal?.throwIfAborted();
      };
      shutdownDeadline = harness.clock.now() + 1_000;
      stopping = harness.runtime.drain("mixed-flight-cleanup").finally(() => { settled = true; });
      for (let turn = 0; turn < 100 && !entered; turn++) await Promise.resolve();
      expect(accepted).toHaveLength(0);
      await harness.clock.advanceBy(500);
      for (let turn = 0; turn < 100 && !harness.supervisor.shutdowns; turn++) await Promise.resolve();
      expect(harness.transport.forceStops).toBe(1);
      expect(harness.supervisor.shutdowns).toBe(1);
      expect(supervisorSignal?.aborted).toBe(false);
      expect(settled).toBe(false);
      proof.resolve();
      await stopping;
      expect(harness.runtime.localCleanupComplete()).toBe(true);
      expect(accepted).toHaveLength(0);
    } finally {
      proof.resolve();
      await stopping?.catch(() => undefined);
      await peer.close();
      for (const awareness of Object.values(host)) awareness.destroy();
      for (const doc of Object.values(docs)) doc.destroy();
    }
  });

  it("preserves most of a long graceful budget while reserving the configured cleanup budget", async () => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, shutdownDeadlineMs: () => shutdownDeadline });
    await harness.runtime.start();
    let drainSignal: AbortSignal | undefined;
    const drain = Promise.withResolvers<void>();
    harness.transport.drain = async (_reason?: string, signal?: AbortSignal) => { drainSignal = signal; await drain.promise; };
    shutdownDeadline = harness.clock.now() + 300_000;
    const stopping = harness.runtime.drain("long-graceful-budget");
    for (let turn = 0; turn < 100 && !drainSignal; turn++) await Promise.resolve();
    await harness.clock.advanceBy(280_000);
    expect(drainSignal?.aborted).toBe(false);
    expect(harness.transport.forceStops).toBe(0);
    expect(harness.supervisor.shutdowns).toBe(0);
    drain.resolve();
    await stopping;
    expect(harness.transport.forceStops).toBe(0);
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(harness.runtime.localCleanupComplete()).toBe(true);
    expect(harness.clock.now()).toBeLessThan(shutdownDeadline);
  });

  it("keeps the local cleanup witness false until delayed closes finish after remote proof fails", async () => {
    const harness = runtimeHarness({ enableSupervisor: true, stopTimeoutMs: 100 });
    await harness.runtime.start();
    const closed = Promise.withResolvers<void>();
    harness.supervisor.shutdown = async () => { throw new Error("exact remote proof unavailable"); };
    harness.supervisor.close = async () => { harness.supervisor.closes++; await closed.promise; };
    const stopping = harness.runtime.stop().then(() => null, error => error);
    for (let turn = 0; turn < 100 && !harness.supervisor.closes; turn++) await Promise.resolve();
    expect(harness.runtime.localCleanupComplete()).toBe(false);
    await harness.clock.advanceBy(100);
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
    closed.resolve();
    for (let turn = 0; turn < 100 && !harness.runtime.localCleanupComplete(); turn++) await Promise.resolve();
    expect(harness.runtime.localCleanupComplete()).toBe(true);
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
  });

  it("keeps the local cleanup witness false when a physical close fails", async () => {
    const harness = runtimeHarness({ enableSupervisor: true });
    await harness.runtime.start();
    harness.transport.close = async () => { throw new Error("local transport close failed"); };
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
  });

  it("fails the local witness closed when rejected bootstrap retirement has no physical witness", async () => {
    const harness = runtimeHarness({ enableSupervisor: true });
    await harness.runtime.start();
    harness.bootstrap.forceStop = async () => { throw new Error("abortable bootstrap retirement rejected"); };
    harness.supervisor.shutdown = async () => { throw new Error("remote proof unavailable"); };
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
  });

  it("joins bootstrap retirement ownership after its abortable public stop has settled", async () => {
    const harness = runtimeHarness({ enableSupervisor: true });
    await harness.runtime.start();
    let retired = false;
    (harness.bootstrap as V2RuntimeBootstrap).retirementSettled = () => retired;
    harness.supervisor.shutdown = async () => { throw new Error("remote proof unavailable"); };
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
    retired = true;
    expect(harness.runtime.localCleanupComplete()).toBe(true);
    expect(harness.bootstrap.forceStops).toBe(1);
  });

  it("uses the shared system deadline for supervisor cleanup beyond the recovery stop timeout", async () => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, shutdownDeadlineMs: () => shutdownDeadline });
    await harness.runtime.start();
    const cleanup = Promise.withResolvers<void>();
    let supervisorSignal: AbortSignal | undefined;
    harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
      harness.supervisor.shutdowns++;
      supervisorSignal = signal;
      await cleanup.promise;
    };
    shutdownDeadline = harness.clock.now() + 60_000;
    let settled = false;
    const stopping = harness.runtime.drain("system-shutdown").finally(() => { settled = true; });
    for (let turn = 0; turn < 100 && harness.supervisor.shutdowns === 0; turn++) await Promise.resolve();
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(supervisorSignal?.aborted).toBe(false);

    await harness.clock.advanceBy(35_000);
    expect(settled).toBe(false);
    cleanup.resolve();
    await expect(stopping).resolves.toBeUndefined();
    expect(harness.supervisor.closes).toBe(1);
    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect(harness.clock.waiters).toHaveLength(0);
  });

  it.each([50_000, 60_000])("bounds supervisor cleanup to the system time remaining after a %d ms drain", async drainMs => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, shutdownDeadlineMs: () => shutdownDeadline });
    await harness.runtime.start();
    let draining = false;
    harness.transport.drain = async () => { draining = true; await harness.clock.sleep(drainMs); };
    let supervisorSignal: AbortSignal | undefined;
    let alreadyExpired: boolean | undefined;
    let destroys = 0;
    harness.ydoc.destroy = async () => { destroys++; };
    harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
      harness.supervisor.shutdowns++;
      supervisorSignal = signal;
      alreadyExpired = signal?.aborted;
      signal?.throwIfAborted();
      await new Promise<void>((_resolve, reject) => signal?.addEventListener("abort", () => reject(signal.reason), { once: true }));
    };
    shutdownDeadline = harness.clock.now() + 60_000;
    let settled = false;
    const stopping = harness.runtime.drain("system-shutdown")
      .then(() => null, error => error).finally(() => { settled = true; });
    for (let turn = 0; turn < 100 && !draining; turn++) await Promise.resolve();
    await harness.clock.advanceBy(drainMs);
    for (let turn = 0; turn < 100 && harness.supervisor.shutdowns === 0; turn++) await Promise.resolve();
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(alreadyExpired).toBe(drainMs === 60_000);
    if (drainMs < 60_000) {
      await harness.clock.advanceBy(9_999);
      expect(supervisorSignal?.aborted).toBe(false);
      expect(settled).toBe(false);
      await harness.clock.advanceBy(1);
    }
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    for (let turn = 0; turn < 100 && destroys === 0; turn++) await Promise.resolve();
    expect(supervisorSignal?.aborted).toBe(true);
    expect(harness.supervisor.closes).toBe(1);
    expect(destroys).toBe(1);
    expect(harness.clock.waiters).toHaveLength(0);
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
  });

  it("does not extend cleanup already started before system shutdown", async () => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, stopTimeoutMs: 10, shutdownDeadlineMs: () => shutdownDeadline });
    const cleanup = Promise.withResolvers<void>();
    let supervisorSignal: AbortSignal | undefined;
    harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
      harness.supervisor.shutdowns++;
      supervisorSignal = signal;
      await cleanup.promise;
    };
    harness.transport.connect = async () => { throw new Error("offline startup failure"); };
    const starting = harness.runtime.start().then(() => null, error => error);
    for (let turn = 0; turn < 100 && harness.supervisor.shutdowns === 0; turn++) await Promise.resolve();
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(supervisorSignal?.aborted).toBe(false);
    shutdownDeadline = harness.clock.now() + 60_000;
    const stopping = harness.runtime.stop().then(() => null, error => error);

    await harness.clock.advanceBy(10);
    expect(supervisorSignal?.aborted).toBe(true);
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect((await starting)?.message).toBe("offline startup failure");
    cleanup.resolve();
    for (let turn = 0; turn < 100 && harness.supervisor.closes === 0; turn++) await Promise.resolve();
    expect(harness.supervisor.closes).toBe(1);
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
  });

  it("rejects supervisor cleanup that resolves at the deadline before its timer runs", async () => {
    let shutdownDeadline: number | undefined;
    const harness = runtimeHarness({ enableSupervisor: true, shutdownDeadlineMs: () => shutdownDeadline });
    await harness.runtime.start();
    const cleanup = Promise.withResolvers<void>();
    let supervisorSignal: AbortSignal | undefined;
    harness.supervisor.shutdown = async (_reason?: string, signal?: AbortSignal) => {
      harness.supervisor.shutdowns++;
      supervisorSignal = signal;
      await cleanup.promise;
    };
    shutdownDeadline = harness.clock.now() + 60_000;
    const stopping = harness.runtime.drain("system-shutdown").then(() => null, error => error);
    for (let turn = 0; turn < 100 && harness.supervisor.shutdowns === 0; turn++) await Promise.resolve();
    // Advance the clock without dispatching timers, as after a stalled event loop.
    harness.clock.value = shutdownDeadline;
    expect(supervisorSignal?.aborted).toBe(false);
    cleanup.resolve();
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(supervisorSignal?.aborted).toBe(true);
    expect(harness.supervisor.closes).toBe(1);
    expect(harness.clock.waiters).toHaveLength(0);
  });

  it.each(["stop", "drain"] as const)("reports rejected supervisor cleanup through %s after releasing local resources", async kind => {
    const harness = runtimeHarness({ enableSupervisor: true });
    const lifecycle = new SupervisorLifecycle(1);
    const release = lifecycle.register(harness.supervisor);
    lifecycle.requireCleanup(harness.supervisor.binding);
    harness.supervisor.shutdown = async () => { harness.supervisor.shutdowns++; throw new Error("offline cleanup unproven"); };
    harness.supervisor.close = async () => { harness.supervisor.closes++; release(); };
    await harness.runtime.start();
    const stopping = kind === "stop" ? harness.runtime.stop() : harness.runtime.drain("offline_shutdown");
    await expect(stopping).rejects.toThrow("cleanup was incomplete");
    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(harness.supervisor.closes).toBe(1);
    expect(harness.runtime.stop()).toBe(stopping);
    await expect(harness.runtime.stop()).rejects.toThrow("cleanup was incomplete");
    await expect(lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
    expect(harness.supervisor.shutdowns).toBe(1);
  });

  it("accepts authoritative supervisor cleanup after subordinate retirement errors", async () => {
    const harness = runtimeHarness({ enableSupervisor: true });
    harness.transport.forceStop = async () => { throw new Error("offline transport unavailable"); };
    harness.bootstrap.forceStop = async () => { throw new Error("offline child acknowledgement unavailable"); };
    await harness.runtime.start();
    await expect(harness.runtime.stop()).resolves.toBeUndefined();
    expect(harness.supervisor.shutdowns).toBe(1);
    expect(harness.supervisor.closes).toBe(1);
  });

  it("uses one shutdown budget and locally fences non-cooperative remote retirement", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 10 });
    await harness.runtime.start();
    const events: string[] = [];
    let releaseTransport!: () => void;
    let releaseBootstrap!: () => void;
    const transportStopped = new Promise<void>(resolve => { releaseTransport = resolve; });
    const bootstrapStopped = new Promise<void>(resolve => { releaseBootstrap = resolve; });
    harness.transport.forceStop = async () => {
      events.push("transport-force-start");
      await transportStopped;
      events.push("transport-force-end");
    };
    harness.transport.close = async () => {
      events.push("transport-close");
      harness.transport.closes++;
    };
    harness.bootstrap.forceStop = async () => {
      events.push("bootstrap-force-start");
      await bootstrapStopped;
      events.push("bootstrap-force-end");
    };

    let stopped = false;
    const stopping = harness.runtime.stop().then(() => null, error => error).finally(() => { stopped = true; });
    for (let turn = 0; turn < 50 && events.length < 2; turn++) await Promise.resolve();
    expect(events).toEqual(["transport-force-start", "bootstrap-force-start"]);

    await harness.clock.advanceBy(9);
    expect(stopped).toBe(false);
    await harness.clock.advanceBy(1);
    for (let turn = 0; turn < 50 && !stopped; turn++) await Promise.resolve();

    expect(stopped).toBe(true);
    expect(harness.clock.now()).toBe(10);
    expect(events).toEqual([
      "transport-force-start",
      "bootstrap-force-start",
      "transport-close",
    ]);
    expect(harness.runtime.snapshot().state).toBe("stopped");

    releaseTransport();
    releaseBootstrap();
    for (let turn = 0; turn < 50 && events.length < 5; turn++) {
      await Promise.resolve();
    }
    expect(events).toEqual([
      "transport-force-start",
      "bootstrap-force-start",
      "transport-close",
      "transport-force-end",
      "bootstrap-force-end",
    ]);
    expect((await stopping)?.message).toContain("cleanup was incomplete");
  });

  it("does not reset the shutdown deadline after graceful drain fails", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 10 });
    await harness.runtime.start();
    let releaseTransport!: () => void;
    let releaseBootstrap!: () => void;
    let transportForceStarted = false;
    let bootstrapForceStarted = false;
    const transportStopped = new Promise<void>(resolve => { releaseTransport = resolve; });
    const bootstrapStopped = new Promise<void>(resolve => { releaseBootstrap = resolve; });
    harness.transport.drain = async () => {
      await harness.clock.sleep(6);
      throw new Error("synthetic drain failure");
    };
    harness.transport.forceStop = async () => {
      transportForceStarted = true;
      await transportStopped;
    };
    harness.bootstrap.forceStop = async () => {
      bootstrapForceStarted = true;
      await bootstrapStopped;
    };

    let stopped = false;
    const stopping = harness.runtime.drain("graceful shutdown")
      .then(() => null, error => error).finally(() => { stopped = true; });
    await harness.clock.advanceBy(6);
    for (
      let turn = 0;
      turn < 50 && !transportForceStarted;
      turn++
    ) {
      await Promise.resolve();
    }
    expect(transportForceStarted).toBe(true);
    expect(bootstrapForceStarted).toBe(true);
    expect(stopped).toBe(false);

    await harness.clock.advanceBy(3);
    expect(stopped).toBe(false);
    await harness.clock.advanceBy(1);
    for (let turn = 0; turn < 50 && !stopped; turn++) await Promise.resolve();

    expect(stopped).toBe(true);
    expect(harness.clock.now()).toBe(10);
    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect(harness.transport.closes).toBe(1);

    releaseTransport();
    releaseBootstrap();
    for (let turn = 0; turn < 50 && harness.transport.closes < 1; turn++) {
      await Promise.resolve();
    }
    expect(bootstrapForceStarted).toBe(true);
    expect(harness.transport.closes).toBe(1);
    expect((await stopping)?.message).toContain("cleanup was incomplete");
  });

  it("does not overlap an escalated drain with force-stop or close", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 50 });
    await harness.runtime.start();
    const events: string[] = [];
    let releaseDrain!: () => void;
    const drained = new Promise<void>(resolve => { releaseDrain = resolve; });
    harness.transport.drain = async () => {
      events.push("drain-start");
      await drained;
      events.push("drain-end");
    };
    harness.transport.forceStop = async () => {
      events.push("force-stop");
    };
    harness.transport.close = async () => {
      events.push("close");
      harness.transport.closes++;
    };

    const draining = harness.runtime.drain("graceful shutdown");
    for (let turn = 0; turn < 50 && !events.includes("drain-start"); turn++) {
      await Promise.resolve();
    }
    const stopping = harness.runtime.stop();
    for (let turn = 0; turn < 50; turn++) await Promise.resolve();

    expect(events).toEqual(["drain-start"]);
    releaseDrain();
    await stopping;

    expect(events).toEqual(["drain-start", "drain-end", "force-stop", "close"]);
    expect(stopping).toBe(draining);
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

  it("bounds a hung remote force-stop and fences its local transport", async () => {
    const harness = runtimeHarness({ stopTimeoutMs: 10 });
    await harness.runtime.start();
    harness.transport.forceStop = async () => new Promise<void>(() => undefined);

    const stopping = harness.runtime.stop().then(() => null, error => error);
    let stopped = false;
    void stopping.then(() => { stopped = true; });
    for (let index = 0; index < 10 && !stopped; index++) {
      await harness.clock.advanceBy(10);
    }
    expect((await stopping)?.message).toContain("cleanup was incomplete");

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

    const stopping = harness.runtime.stop().then(() => null, error => error);
    let stopped = false;
    void stopping.then(() => { stopped = true; });
    for (let index = 0; index < 10 && !stopped; index++) {
      await harness.clock.advanceBy(10);
    }
    expect((await stopping)?.message).toContain("cleanup was incomplete");
    expect(harness.runtime.localCleanupComplete()).toBe(false);
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
    for (let turn = 0; turn < 100 && !harness.runtime.localCleanupComplete(); turn++) await Promise.resolve();
    expect(harness.runtime.localCleanupComplete()).toBe(true);

    expect(harness.runtime.snapshot().state).toBe("stopped");
    expect(harness.transport.snapshotValue.connected).toBe(false);
    await expect(harness.runtime.start()).rejects.toThrow("single-use");
  });
});
