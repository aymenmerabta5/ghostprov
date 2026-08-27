import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type {
  AcquireSandboxOptions,
  ProvisionedRuntimeCredentials,
  ProvisioningHooks,
  YSweetRequestContext,
} from "../prism/provisioning-client";
import type {
  ProvisionedSandbox,
  ResourceCredential,
  YSweetEnvelope,
} from "../prism/protocol";
import type { RunHandle } from "../relay/run-handle";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
  SandboxRunOptions,
} from "../sandbox/registry";
import { describeSandboxFailure } from "../sandbox/failure";
import type { ProjectId } from "../sandbox/types";
import type { AwarenessChannel } from "./awareness/channel";
import type { V2ReadyDescriptor } from "./bootstrap/controller";
import type {
  HostFrame,
  ProtocolBinding,
  SidecarFrame,
} from "./protocol/shared";
import type { V2SidecarAssets } from "./sidecar/assets";
import type { CredentialRefreshOutcome } from "../observability/events";
import type {
  V2GzipOptions,
  V2SidecarRecoveryToken,
  V2TransportSession,
  V2TransportSnapshot,
} from "./transport/session";
import { V2PreCommitError } from "./transport/run-handle";
import {
  DecayingRuntimeFailureScore,
  classifyRuntimeError,
} from "./runtime-health";
import { DecayingRuntimeCompletionEstimate } from "./runtime-completion";
import type { AtomicBytePermitPool } from "./memory/atomic-byte-permits";
import { isRateLimited, rateLimitHintFromError } from "./rate-limit";

function resolvedRateLimitOutcome(value: unknown, handle: RunHandle): boolean {
  // Successful Responses calls commonly carry budget headers. Those headers
  // are useful evidence, but their mere presence must not make a completed
  // request routing-neutral or the completion estimator will remain frozen at
  // its cold baseline. Only an actual failed/incomplete rate-limit outcome (or
  // explicit exhausted header evidence) is neutral.
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const status = (value as Record<string, unknown>).status;
  if (status !== "failed" && status !== "incomplete") return false;
  return isRateLimited(value) ||
    rateLimitHintFromError(value, handle.rateLimit) !== undefined;
}

function shouldQuarantineSandboxSession(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const value = error as { name?: unknown; kind?: unknown };
  if (value.name === "V2BootstrapError") {
    return value.kind === "render" || value.kind === "hello" ||
      value.kind === "readiness" || value.kind === "timeout";
  }
  if (value.name === "V2TransportLivenessError") return true;
  if (value.name !== "V2RuntimeLifecycleFailure") return false;
  return value.kind === "heartbeat_unreachable" ||
    value.kind === "sidecar_unreachable" ||
    value.kind === "ambiguity_exhausted";
}

export interface V2RuntimeProvisioning {
  provision(
    projectId: ProjectId,
    signal?: AbortSignal,
    hooks?: ProvisioningHooks,
  ): Promise<ProvisionedRuntimeCredentials>;
  acquireSandbox?(
    signal?: AbortSignal,
    options?: AcquireSandboxOptions,
  ): Promise<ProvisionedSandbox>;
  provisionAcquired?(
    projectId: ProjectId,
    sandbox: ProvisionedSandbox,
    signal?: AbortSignal,
    hooks?: ProvisioningHooks,
  ): Promise<ProvisionedRuntimeCredentials>;
  refreshResources(
    current: ProvisionedRuntimeCredentials,
    signal?: AbortSignal,
  ): Promise<ResourceCredential>;
  mintYSweet?(
    projectId: ProjectId,
    context: YSweetRequestContext,
    signal?: AbortSignal,
  ): Promise<YSweetEnvelope>;
  reportSandboxFailure?(projectId: ProjectId, sandbox: ProvisionedSandbox): void;
  reportSandboxReady?(projectId: ProjectId, sandbox: ProvisionedSandbox): void;
}

export interface V2RuntimeGateway {
  heartbeat(signal?: AbortSignal): Promise<V2RuntimeHeartbeatResult>;
  rotateSameSession?(candidate: ProvisionedSandbox): boolean;
}

export interface V2RuntimeHeartbeatResult {
  ok: boolean;
  status: number;
  durationMs: number;
  sessionId: string | null;
}

export interface V2RuntimeHeartbeat {
  start(): void;
  stop(): Promise<void>;
  snapshot(): { lastSuccessAtMs: number | null };
}

export interface V2RuntimeHeartbeatCallbacks {
  onSuccess(result: V2RuntimeHeartbeatResult): void;
  onAuthenticationFailure(
    status: number,
    signal: AbortSignal,
  ): Promise<"retry" | "expired">;
  onExpired(status: number): void;
  onTransientFailure(status: number | null): void;
  onUnreachable(status: number | null): void;
}

export type V2RuntimeLifecycleFailureKind =
  | "heartbeat_expired"
  | "heartbeat_unreachable"
  | "sidecar_unreachable"
  | "ambiguity_exhausted"
  | "resource_credential_expired";

function normalizedHttpStatus(status: number | null): number | null {
  return Number.isInteger(status) && status !== null && status >= 100 && status <= 599
    ? status
    : null;
}

/** A bounded, secret-free failure that is safe to expose in runtime snapshots. */
export class V2RuntimeLifecycleFailure extends Error {
  readonly status: number | null;
  readonly operation: "heartbeat" | "sidecar_liveness" | "refresh_resources";

  constructor(
    readonly kind: V2RuntimeLifecycleFailureKind,
    status: number | null = null,
    options?: ErrorOptions,
  ) {
    const safeStatus = normalizedHttpStatus(status);
    const suffix = safeStatus === null ? "" : ` (${safeStatus})`;
    const message = kind === "heartbeat_expired"
      ? `V2 sandbox heartbeat credentials expired${suffix}`
      : kind === "heartbeat_unreachable"
        ? `V2 sandbox heartbeat became unreachable${suffix}`
        : kind === "sidecar_unreachable"
          ? "V2 sandbox sidecar became unreachable"
          : kind === "ambiguity_exhausted"
            ? "V2 sandbox ambiguous-commit capacity was exhausted"
          : "V2 sandbox resource credential expired after refresh failed";
    super(message, options);
    this.name = "V2RuntimeLifecycleFailure";
    this.status = safeStatus;
    this.operation = kind === "resource_credential_expired"
      ? "refresh_resources"
      : kind === "sidecar_unreachable" || kind === "ambiguity_exhausted"
        ? "sidecar_liveness"
        : "heartbeat";
  }
}

export interface V2RuntimeYDoc {
  connect(): Promise<void>;
  destroy(): Promise<void>;
}

export interface V2RuntimeBootstrap {
  ensureReady(signal?: AbortSignal): Promise<V2ReadyDescriptor>;
  drainAndStop(reason: string, signal?: AbortSignal): Promise<void>;
  forceStop?(reason: string, signal?: AbortSignal): Promise<void>;
  sealCredentialRotation?(ySweet: YSweetEnvelope, rotationId: string): Promise<string>;
}

export interface V2RuntimeTransport {
  connect(signal?: AbortSignal): Promise<void>;
  run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle>;
  drain(reason: string, signal?: AbortSignal): Promise<void>;
  forceStop?(reason: string, signal?: AbortSignal): Promise<void>;
  suspendSidecarRecovery?(reason: string, signal?: AbortSignal): Promise<V2SidecarRecoveryToken>;
  adoptRecoveredSidecar?(token: V2SidecarRecoveryToken): Promise<void>;
  failSidecarRecovery?(token: V2SidecarRecoveryToken, cause: unknown): void;
  close(): Promise<void>;
  snapshot(): V2TransportSnapshot;
  setHostCeiling?(value: number): void;
  rotateCredential?(rotationId: string, sealedJson: string, signal?: AbortSignal): Promise<void>;
  resumeAwareness?(signal?: AbortSignal): Promise<void>;
  probeLiveness?(signal?: AbortSignal): Promise<{ rttMs: number; sentAtMs: number }>;
}

export interface V2RuntimeGovernor {
  snapshot(): {
    reservationCapacityBytes: number;
    reservedBytes: number;
    paused: boolean;
  };
}

interface V2SidecarRelaunchContext {
  readonly generation: number;
  readonly sourceTransport: V2RuntimeTransport;
  capsule: V2SidecarRecoveryToken | null;
  awareness: AwarenessChannel<HostFrame, SidecarFrame> | null;
  bootstrap: V2RuntimeBootstrap | null;
  transport: V2RuntimeTransport | null;
  cleanup: Promise<void> | null;
}

export interface V2SandboxRuntimeOptions {
  projectId: ProjectId;
  workerIndex: number;
  leaderGeneration: number;
  provisioning: V2RuntimeProvisioning;
  governor: V2RuntimeGovernor;
  assets: V2SidecarAssets;
  transportReservationBytes: number;
  maxResponseBytes?: number;
  eventMemoryPool?: AtomicBytePermitPool;
  gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
  clock?: Clock;
  resourceRefreshLeadMs?: number;
  ySweetRefreshMs?: number;
  sidecarProbePeriodMs?: number;
  sidecarFailureThreshold?: number;
  sidecarCommitStallMs?: number;
  sidecarStallSampleThreshold?: number;
  sidecarRecoveryTimeoutMs?: number;
  stopTimeoutMs?: number;
  initialSandbox?: ProvisionedSandbox;
  acquireSandboxForAuth?(
    status: number,
    signal: AbortSignal,
  ): Promise<ProvisionedSandbox>;
  onCredentialRefresh?(
    outcome: CredentialRefreshOutcome,
    status: number,
  ): void;
  createGateway(sandbox: ProvisionedSandbox): V2RuntimeGateway;
  createHeartbeat(
    gateway: V2RuntimeGateway,
    callbacks: V2RuntimeHeartbeatCallbacks,
  ): V2RuntimeHeartbeat;
  createYDoc(credentials: ProvisionedRuntimeCredentials): V2RuntimeYDoc;
  createAwareness(
    binding: ProtocolBinding,
    credentials: ProvisionedRuntimeCredentials,
  ): AwarenessChannel<HostFrame, SidecarFrame>;
  createBootstrap(input: {
    binding: ProtocolBinding;
    workerIndex: number;
    projectId: ProjectId;
    leaderGeneration: number;
    credentials: ProvisionedRuntimeCredentials;
    ydoc: V2RuntimeYDoc;
    gateway: V2RuntimeGateway;
    awareness: AwarenessChannel<HostFrame, SidecarFrame>;
    assets: V2SidecarAssets;
  }): V2RuntimeBootstrap;
  createTransport(input: {
    binding: ProtocolBinding;
    transportProfile: V2SidecarAssets["transportProfile"];
    channel: AwarenessChannel<HostFrame, SidecarFrame>;
    maxResponseBytes: number;
    gzip?: (bytes: Uint8Array, options?: V2GzipOptions) => Promise<Uint8Array>;
    expectedSidecarInstanceId: string;
    eventMemoryPool?: AtomicBytePermitPool;
    onStateChange?(): void;
  }): V2RuntimeTransport | V2TransportSession;
}

type RuntimeState =
  | "configured"
  | "provisioning"
  | "syncing_credentials"
  | "syncing_workspace"
  | "bootstrapping"
  | "connecting_transport"
  | "ready"
  | "draining"
  | "failed"
  | "stopped";

export class V2SandboxRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly #options: V2SandboxRuntimeOptions;
  readonly #clock: Clock;
  readonly #runtimeHealth: DecayingRuntimeFailureScore;
  readonly #completionEstimate: DecayingRuntimeCompletionEstimate;
  readonly #listeners = new Set<() => void>();
  readonly #resourceRefreshLeadMs: number;
  readonly #ySweetRefreshMs: number;
  readonly #sidecarProbePeriodMs: number;
  readonly #sidecarFailureThreshold: number;
  readonly #sidecarCommitStallMs: number;
  readonly #sidecarStallSampleThreshold: number;
  readonly #sidecarRecoveryTimeoutMs: number;
  readonly #stopTimeoutMs: number;
  #state: RuntimeState = "configured";
  #started = false;
  #generation = 0;
  #controller: AbortController | null = null;
  #starting: Promise<void> | null = null;
  #refreshLoop: Promise<void> | null = null;
  #ySweetLoop: Promise<void> | null = null;
  #sidecarLoop: Promise<void> | null = null;
  #sidecarSuspect = false;
  #gateway: V2RuntimeGateway | null = null;
  #heartbeat: V2RuntimeHeartbeat | null = null;
  #ydoc: V2RuntimeYDoc | null = null;
  #awareness: AwarenessChannel<HostFrame, SidecarFrame> | null = null;
  #bootstrap: V2RuntimeBootstrap | null = null;
  #transport: V2RuntimeTransport | null = null;
  #credentials: ProvisionedRuntimeCredentials | null = null;
  #descriptor: V2ReadyDescriptor | null = null;
  #binding: ProtocolBinding | null = null;
  #sidecarRelaunching: Promise<V2RuntimeTransport> | null = null;
  #sidecarRelaunchContext: V2SidecarRelaunchContext | null = null;
  #refreshSandboxAuthentication: ((
    status: number,
    signal: AbortSignal,
  ) => Promise<"retry" | "expired">) | null = null;
  #credentialRefreshTask: Promise<"retry" | "expired"> | null = null;
  #ySweetRotation: Promise<void> | null = null;
  #awarenessFailureUnsubscribe: (() => void) | null = null;
  #ySweetReconnectPending = false;
  #awarenessFailureVersion = 0;
  #ySweetRefreshRequested = false;
  #wakeYSweetRefresh: (() => void) | null = null;
  #failure: unknown = null;
  #replacementSandbox: ProvisionedSandbox | null = null;
  #credentialRefreshPending = false;
  #shutdownPromise: Promise<void> | null = null;
  #shutdownKind: "drain" | "stop" | null = null;
  #escalateShutdown: (() => void) | null = null;

  constructor(options: V2SandboxRuntimeOptions) {
    if (!Number.isSafeInteger(options.workerIndex) || options.workerIndex < 1) {
      throw new RangeError("workerIndex must be a positive safe integer");
    }
    if (!Number.isSafeInteger(options.leaderGeneration) || options.leaderGeneration < 1) {
      throw new RangeError("leaderGeneration must be a positive safe integer");
    }
    if (
      !Number.isSafeInteger(options.transportReservationBytes) ||
      options.transportReservationBytes < 1
    ) {
      throw new RangeError("transportReservationBytes must be positive");
    }
    if (
      options.maxResponseBytes !== undefined &&
      (!Number.isSafeInteger(options.maxResponseBytes) || options.maxResponseBytes < 1)
    ) {
      throw new RangeError("maxResponseBytes must be positive");
    }
    const refreshLead = options.resourceRefreshLeadMs ?? 5 * 60_000;
    if (!Number.isSafeInteger(refreshLead) || refreshLead < 1) {
      throw new RangeError("resourceRefreshLeadMs must be positive");
    }
    const ySweetRefreshMs = options.ySweetRefreshMs ?? 45 * 60_000;
    if (!Number.isSafeInteger(ySweetRefreshMs) || ySweetRefreshMs < 5 * 60_000) {
      throw new RangeError("ySweetRefreshMs must be at least five minutes");
    }
    this.projectId = options.projectId;
    this.#options = options;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#runtimeHealth = new DecayingRuntimeFailureScore({ clock: this.#clock });
    this.#completionEstimate = new DecayingRuntimeCompletionEstimate({ clock: this.#clock });
    this.#resourceRefreshLeadMs = refreshLead;
    this.#ySweetRefreshMs = ySweetRefreshMs;
    this.#sidecarProbePeriodMs = options.sidecarProbePeriodMs ?? 10_000;
    this.#sidecarFailureThreshold = options.sidecarFailureThreshold ?? 3;
    // A reliable-window stall is stronger evidence than one missed out-of-band
    // pong. Observe it twice, one probe period apart, so the effective default
    // recovery boundary is 20 seconds without reacting to a single slow ACK.
    this.#sidecarCommitStallMs = options.sidecarCommitStallMs ?? this.#sidecarProbePeriodMs;
    this.#sidecarStallSampleThreshold = options.sidecarStallSampleThreshold ?? 2;
    // Recovery includes authenticated session proof, old-PID retirement, one
    // or more bootstrap attempts, journal replay, and a final liveness proof.
    // It must not inherit the much shorter shutdown cleanup budget.
    this.#sidecarRecoveryTimeoutMs = options.sidecarRecoveryTimeoutMs ?? 150_000;
    this.#stopTimeoutMs = options.stopTimeoutMs ?? 15_000;
    if (!Number.isSafeInteger(this.#sidecarProbePeriodMs) || this.#sidecarProbePeriodMs < 1) {
      throw new RangeError("sidecarProbePeriodMs must be positive");
    }
    if (!Number.isSafeInteger(this.#sidecarFailureThreshold) || this.#sidecarFailureThreshold < 1) {
      throw new RangeError("sidecarFailureThreshold must be positive");
    }
    if (!Number.isSafeInteger(this.#sidecarCommitStallMs) || this.#sidecarCommitStallMs < 1) {
      throw new RangeError("sidecarCommitStallMs must be positive");
    }
    if (
      !Number.isSafeInteger(this.#sidecarStallSampleThreshold) ||
      this.#sidecarStallSampleThreshold < 2
    ) {
      throw new RangeError("sidecarStallSampleThreshold must be at least two");
    }
    if (
      !Number.isSafeInteger(this.#sidecarRecoveryTimeoutMs) ||
      this.#sidecarRecoveryTimeoutMs < 1
    ) {
      throw new RangeError("sidecarRecoveryTimeoutMs must be positive");
    }
    if (!Number.isSafeInteger(this.#stopTimeoutMs) || this.#stopTimeoutMs < 1) {
      throw new RangeError("stopTimeoutMs must be positive");
    }
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  start(): Promise<void> {
    if (this.#state === "ready") return Promise.resolve();
    if (this.#starting) return this.#starting;
    if (this.#started || this.#state !== "configured") {
      return Promise.reject(new Error("V2 sandbox runtime instances are single-use"));
    }
    this.#started = true;
    const generation = ++this.#generation;
    const starting = this.#startGeneration(generation).finally(() => {
      if (this.#starting === starting) this.#starting = null;
    });
    this.#starting = starting;
    return starting;
  }

  drain(reason: string): Promise<void> {
    return this.#beginShutdown("drain", reason);
  }

  stop(): Promise<void> {
    return this.#beginShutdown("stop", "runtime-stop");
  }

  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    if (this.#state !== "ready" || !this.#transport || !this.#descriptor) {
      if (this.#state === "failed") {
        throw new V2PreCommitError(
          "V2 sandbox runtime is recovering",
          "recovering",
        );
      }
      if (this.#state === "draining" || this.#state === "stopped") {
        throw new V2PreCommitError(
          "V2 sandbox runtime is draining",
          "draining",
        );
      }
      throw new V2PreCommitError(
        "V2 sandbox runtime is not ready",
        "not_ready",
      );
    }
    if (this.#sidecarSuspect || this.#sidecarRelaunching || this.#ySweetReconnectPending) {
      throw new V2PreCommitError(
        "V2 sandbox sidecar is recovering",
        "recovering",
      );
    }
    this.#transport.setHostCeiling?.(this.#descriptor.capacity);
    const startedAt = this.#clock.now();
    let handle: RunHandle;
    try {
      handle = await this.#transport.run(body, signal, options);
    } catch (error) {
      this.#runtimeHealth.record(classifyRuntimeError(error));
      this.#deferAmbiguityHealthDuringRelaunch(error);
      this.#emit();
      throw error;
    }
    const final = handle.final.then(
      value => {
        if (resolvedRateLimitOutcome(value, handle)) {
          // HTTP-200 SSE rate-limit terminals are routing-neutral just like a
          // rejected event:error. Preserve the final/hint without teaching the
          // adaptive router that a fast 429 is a fast successful sandbox.
          this.#runtimeHealth.record("neutral");
          this.#emit();
          return value;
        }
        const sample = Math.max(
          1,
          this.#clock.now() - startedAt,
          handle.stats.firstByteMs ?? 0,
        );
        this.#completionEstimate.observe(sample);
        this.#runtimeHealth.record("healthy");
        this.#emit();
        return value;
      },
      error => {
        this.#runtimeHealth.record(classifyRuntimeError(error));
        this.#deferAmbiguityHealthDuringRelaunch(error);
        this.#emit();
        throw error;
      },
    );
    void final.catch(() => undefined);
    this.#emit();
    return {
      events: () => handle.events(),
      final,
      get rateLimit() { return handle.rateLimit; },
      get upstreamPrelude() { return handle.upstreamPrelude; },
      stats: handle.stats,
    };
  }

  takeReplacementSandbox(): ProvisionedSandbox | null {
    const sandbox = this.#replacementSandbox;
    this.#replacementSandbox = null;
    return sandbox;
  }

  snapshot(): RoutableSandboxSnapshot {
    const transport = this.#transport?.snapshot() ?? null;
    const verifiedRelease = this.#state === "ready" ? this.#descriptor : null;
    const descriptorCapacity = this.#descriptor?.capacity ?? 0;
    const active = transport?.active ?? 0;
    const quarantined = transport?.quarantined ?? 0;
    const creditCapacity = transport
      ? Math.max(0, transport.credits + active)
      : 0;
    const capacity = this.#state === "ready" && !this.#sidecarSuspect &&
        !this.#credentialRefreshPending && !this.#ySweetReconnectPending
      ? Math.max(0, Math.min(descriptorCapacity, creditCapacity))
      : 0;
    const heartbeatAt = this.#heartbeat?.snapshot().lastSuccessAtMs ?? null;
    const governor = this.#options.governor.snapshot();
    const memoryPressure = governor.reservationCapacityBytes > 0
      ? Math.max(0, Math.min(1, governor.reservedBytes / governor.reservationCapacityBytes))
      : governor.paused ? 1 : 0;
    return {
      projectId: this.projectId,
      releaseRevision: verifiedRelease?.releaseRevision ?? null,
      protocolRevision: verifiedRelease?.protocolRevision ?? null,
      buildHash: verifiedRelease?.buildHash ?? null,
      schemaHash: verifiedRelease?.schemaHash ?? null,
      state: this.#state,
      capacity,
      active,
      available: this.#state === "ready" && !this.#sidecarSuspect &&
          !this.#credentialRefreshPending && !this.#ySweetReconnectPending && transport
        ? Math.max(0, Math.min(transport.admissionCredits ?? transport.credits, capacity - active - quarantined))
        : 0,
      quarantined,
      ambiguousQuarantined: transport?.ambiguousQuarantined ?? 0,
      ambiguityLimit: transport?.ambiguityLimit ?? Math.max(
        1,
        Math.min(4, descriptorCapacity || 4),
      ),
      heartbeatAgeMs: heartbeatAt === null ? null : Math.max(0, this.#clock.now() - heartbeatAt),
      sidecarPeerAgeMs: transport?.sidecarPeerAgeMs ?? null,
      credentialExpiresInMs: this.#credentials
        ? Math.max(0, this.#credentials.resource.expiresAtMs - this.#clock.now())
        : null,
      ackEwmaMs: transport?.awarenessRttMs ?? 0,
      completionEwmaMs: this.#completionEstimate.value(),
      recentFailureRate: this.#runtimeHealth.value(),
      queueDepth: transport?.queued ?? 0,
      memoryPressure,
      failure: this.#failure ? describeSandboxFailure(this.#failure) : null,
      diagnostics: transport?.diagnostics,
    };
  }

  async #startGeneration(generation: number): Promise<void> {
    const controller = new AbortController();
    this.#controller = controller;
    this.#failure = null;
    this.#setState("provisioning");
    let effectiveSandbox = this.#options.initialSandbox ?? null;
    const startHeartbeat = (sandbox: ProvisionedSandbox) => {
      if (generation !== this.#generation || this.#heartbeat) return;
      effectiveSandbox = sandbox;
      this.#setState("syncing_credentials");
      const gateway = this.#options.createGateway(sandbox);
      let heartbeat: V2RuntimeHeartbeat | null = null;
      const callbackIsCurrent = () => generation === this.#generation &&
        heartbeat !== null && heartbeat === this.#heartbeat;
      const refreshAuthentication = (
        status: number,
        signal: AbortSignal,
      ): Promise<"retry" | "expired"> => {
        if (!callbackIsCurrent()) return Promise.resolve("retry");
        const existing = this.#credentialRefreshTask;
        if (existing) return this.#abortable(existing, signal);
        const refresh = (async (): Promise<"retry" | "expired"> => {
          if (!this.#options.acquireSandboxForAuth && !this.#options.provisioning.acquireSandbox) {
            return "retry";
          }
          // Provisioning still owns its original credential object until it
          // completes. Do not rotate underneath resource/token sync calls.
          if (!this.#credentials) return "retry";
          this.#credentialRefreshPending = true;
          this.#emit();
          let candidate: ProvisionedSandbox;
          try {
            candidate = this.#options.acquireSandboxForAuth
              ? await this.#options.acquireSandboxForAuth(status, controller.signal)
              : await this.#options.provisioning.acquireSandbox!(
                controller.signal,
                { maxAttempts: 1 },
              );
          } catch (error) {
            const value = error && typeof error === "object"
              ? error as { kind?: unknown; status?: unknown }
              : {};
            this.#notifyCredentialRefresh(
              controller.signal.aborted
                ? "aborted"
                : value.kind === "auth" || value.status === 401 || value.status === 403
                  ? "auth_rejected"
                  : "transport_failed",
              status,
            );
            return "expired";
          }
          if (!callbackIsCurrent()) return "expired";
          if (gateway.rotateSameSession?.(candidate)) {
            effectiveSandbox = candidate;
            if (this.#credentials) {
              this.#credentials = { ...this.#credentials, sandbox: candidate };
            }
            this.#emit();
            this.#notifyCredentialRefresh("updated_in_place", status);
            return "retry";
          }
          this.#replacementSandbox = candidate;
          this.#notifyCredentialRefresh("replacement_required", status);
          return "expired";
        })();
        const tracked = refresh.finally(() => {
          if (this.#credentialRefreshTask === tracked) this.#credentialRefreshTask = null;
        });
        this.#credentialRefreshTask = tracked;
        return this.#abortable(tracked, signal);
      };
      this.#refreshSandboxAuthentication = refreshAuthentication;
      heartbeat = this.#options.createHeartbeat(gateway, {
        onSuccess: () => {
          if (!callbackIsCurrent()) return;
          this.#credentialRefreshPending = false;
          this.#emit();
        },
        onExpired: status => {
          if (!callbackIsCurrent()) return;
          this.#failGeneration(
            generation,
            new V2RuntimeLifecycleFailure("heartbeat_expired", status),
          );
        },
        onAuthenticationFailure: refreshAuthentication,
        onTransientFailure: () => {
          if (!callbackIsCurrent()) return;
          this.#runtimeHealth.record("unhealthy");
          this.#emit();
        },
        onUnreachable: status => {
          if (!callbackIsCurrent()) return;
          this.#failGeneration(
            generation,
            new V2RuntimeLifecycleFailure("heartbeat_unreachable", status),
          );
        },
      });
      this.#gateway = gateway;
      this.#heartbeat = heartbeat;
      heartbeat.start();
    };
    try {
      const initialSandbox = this.#options.initialSandbox;
      const credentials = initialSandbox && this.#options.provisioning.provisionAcquired
        ? await this.#options.provisioning.provisionAcquired(
          this.projectId,
          initialSandbox,
          controller.signal,
          { onSandboxAcquired: startHeartbeat },
        )
        : await this.#options.provisioning.provision(
          this.projectId,
          controller.signal,
          { onSandboxAcquired: startHeartbeat },
        );
      if (generation !== this.#generation) return;
      startHeartbeat(credentials.sandbox);
      if (!this.#gateway) throw new Error("V2 sandbox gateway was not created");
      this.#credentials = effectiveSandbox
        ? { ...credentials, sandbox: effectiveSandbox }
        : credentials;

      this.#setState("syncing_workspace");
      const ydoc = this.#options.createYDoc(credentials);
      this.#ydoc = ydoc;
      await ydoc.connect();
      if (generation !== this.#generation) return;

      const sessionId = credentials.sandbox.sessionId ??
        (await this.#gateway.heartbeat(controller.signal)).sessionId;
      if (!sessionId) throw new Error("V2 sandbox session ID is unavailable");
      const binding: ProtocolBinding = {
        projectId: this.projectId,
        sessionId,
        leaderGeneration: this.#options.leaderGeneration,
        buildHash: this.#options.assets.buildHash,
        schemaHash: this.#options.assets.schemaHash,
        launchNonce: crypto.randomUUID(),
        protocolRevision: this.#options.assets.protocolRevision,
      };
      this.#binding = binding;
      const awareness = this.#options.createAwareness(binding, credentials);
      this.#awareness = awareness;
      this.#bindAwarenessFailure(awareness, generation);
      const bootstrap = this.#options.createBootstrap({
        binding,
        workerIndex: this.#options.workerIndex,
        projectId: this.projectId,
        leaderGeneration: this.#options.leaderGeneration,
        credentials,
        ydoc,
        gateway: this.#gateway,
        awareness,
        assets: this.#options.assets,
      });
      this.#bootstrap = bootstrap;
      this.#setState("bootstrapping");
      const descriptor = await bootstrap.ensureReady(controller.signal);
      if (
        descriptor.releaseRevision !== this.#options.assets.revision ||
        descriptor.buildHash !== binding.buildHash ||
        descriptor.schemaHash !== binding.schemaHash ||
        descriptor.leaderGeneration !== binding.leaderGeneration ||
        descriptor.protocolRevision !== binding.protocolRevision ||
        descriptor.launchNonce !== binding.launchNonce
      ) throw new Error("V2 bootstrap descriptor does not match its binding");
      if (generation !== this.#generation) return;
      this.#descriptor = descriptor;

      let transport!: V2RuntimeTransport;
      transport = this.#options.createTransport({
        binding,
        transportProfile: this.#options.assets.transportProfile,
        channel: awareness,
        maxResponseBytes: this.#options.maxResponseBytes ?? 64 * 1024 * 1024,
        gzip: this.#options.gzip,
        expectedSidecarInstanceId: descriptor.instanceId,
        eventMemoryPool: this.#options.eventMemoryPool,
        onStateChange: () => {
          if (generation === this.#generation && this.#transport === transport) this.#emit();
        },
      });
      this.#transport = transport;
      transport.setHostCeiling?.(descriptor.capacity);
      this.#setState("connecting_transport");
      await transport.connect(controller.signal);
      if (generation !== this.#generation) return;
      if (transport.probeLiveness) {
        await this.#proveInitialSidecar(transport, controller.signal);
      }
      if (generation !== this.#generation) return;
      this.#sidecarSuspect = false;
      this.#setState("ready");
      this.#reportSandboxReady(this.#credentials.sandbox);
      this.#refreshLoop = this.#refreshResources(generation, controller.signal);
      void this.#refreshLoop.catch(error => {
        if (controller.signal.aborted || generation !== this.#generation) return;
        const status = error && typeof error === "object" &&
          typeof (error as { status?: unknown }).status === "number"
          ? (error as { status: number }).status
          : null;
        this.#failGeneration(
          generation,
          new V2RuntimeLifecycleFailure(
            "resource_credential_expired",
            status,
            { cause: error },
          ),
        );
      });
      this.#ySweetLoop = this.#refreshYSweet(generation, controller.signal);
      void this.#ySweetLoop.catch(error => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        this.#failure = error;
        this.#emit();
      });
      if (transport.probeLiveness) {
        this.#sidecarLoop = this.#monitorSidecar(
          generation,
          transport,
          controller.signal,
        );
        void this.#sidecarLoop.catch(() => undefined);
      }
    } catch (error) {
      if (generation !== this.#generation) return;
      if (error instanceof DOMException && error.name === "AbortError") return;
      const failedSandbox = effectiveSandbox ?? this.#credentials?.sandbox ?? null;
      if (failedSandbox && shouldQuarantineSandboxSession(error)) {
        this.#reportSandboxFailure(failedSandbox);
      }
      this.#failure = error;
      this.#setState("failed");
      const transport = this.#transport;
      const bootstrap = this.#bootstrap;
      if (transport) {
        await this.#runBounded(signal => transport.forceStop
          ? transport.forceStop("startup-failed", signal)
          : transport.drain("startup-failed", signal));
      }
      if (bootstrap) {
        await this.#runBounded(signal => bootstrap.forceStop
          ? bootstrap.forceStop("startup-failed", signal)
          : bootstrap.drainAndStop("startup-failed", signal));
      }
      await this.#runBounded(() => this.#dispose(true));
      throw error;
    }
  }

  async #monitorSidecar(
    generation: number,
    transport: V2RuntimeTransport,
    signal: AbortSignal,
  ): Promise<void> {
    if (!transport.probeLiveness) return;
    let monitoredTransport = transport;
    let consecutiveFailures = 0;
    let consecutiveReliableStalls = 0;
    let deadline = this.#clock.now() + this.#sidecarProbePeriodMs;
    const recover = async (): Promise<boolean> => {
      try {
        monitoredTransport = await this.#relaunchSidecar(
          generation,
          monitoredTransport,
          signal,
        );
        consecutiveFailures = 0;
        consecutiveReliableStalls = 0;
        this.#sidecarSuspect = false;
        this.#emit();
        return true;
      } catch (relaunchError) {
        if (signal.aborted || generation !== this.#generation) return false;
        this.#failGeneration(
          generation,
          new V2RuntimeLifecycleFailure("sidecar_unreachable", null, {
            cause: relaunchError,
          }),
        );
        return false;
      }
    };
    while (!signal.aborted && generation === this.#generation) {
      try {
        await this.#clock.waitUntil(deadline, signal);
      } catch {
        if (signal.aborted) return;
      }
      if (signal.aborted || generation !== this.#generation) return;
      let reliableStallReady = false;
      try {
        const diagnostics = monitoredTransport.snapshot().diagnostics?.host;
        const windowStalled = !!diagnostics && diagnostics.windowFrames > 0 &&
          diagnostics.ackProgressAgeMs >= this.#sidecarCommitStallMs;
        const schedulerStalled = !!diagnostics && diagnostics.schedulerFrames > 0 &&
          diagnostics.oldestQueuedFrameMs >= this.#sidecarCommitStallMs &&
          (
            diagnostics.windowFrames === 0 ||
            diagnostics.ackProgressAgeMs >= this.#sidecarCommitStallMs
          );
        if (windowStalled || schedulerStalled) {
          consecutiveReliableStalls++;
          if (!this.#sidecarSuspect) {
            this.#sidecarSuspect = true;
            this.#emit();
          }
          if (consecutiveReliableStalls >= this.#sidecarStallSampleThreshold) {
            // A definitive prepared-expired outcome removes request-local
            // admission state, but it cannot retract a reliable frame already
            // in the cumulative window. Two independent samples prove that a
            // successful out-of-band pong must not keep it routable.
            reliableStallReady = true;
            throw new Error("V2 reliable host transport stopped making progress");
          }
        } else {
          consecutiveReliableStalls = 0;
          await monitoredTransport.probeLiveness!(signal);
          if (generation !== this.#generation) return;
          consecutiveFailures = 0;
          if (this.#sidecarSuspect) {
            this.#sidecarSuspect = false;
            this.#emit();
          }
        }
      } catch (error) {
        if (signal.aborted || (error instanceof DOMException && error.name === "AbortError")) {
          return;
        }
        if (generation !== this.#generation) return;
        if (!reliableStallReady) consecutiveFailures++;
        if (!this.#sidecarSuspect) {
          this.#sidecarSuspect = true;
          this.#emit();
        }
        if (
          reliableStallReady ||
          consecutiveFailures >= this.#sidecarFailureThreshold
        ) {
          if (!await recover()) return;
        }
      }
      deadline += this.#sidecarProbePeriodMs;
      while (deadline <= this.#clock.now()) deadline += this.#sidecarProbePeriodMs;
    }
  }

  async #proveInitialSidecar(
    transport: V2RuntimeTransport,
    signal: AbortSignal,
  ): Promise<void> {
    if (!transport.probeLiveness) return;
    let lastError: unknown = null;
    for (let attempt = 1; attempt <= this.#sidecarFailureThreshold; attempt++) {
      try {
        await transport.probeLiveness(signal);
        return;
      } catch (error) {
        if (signal.aborted || (error instanceof DOMException && error.name === "AbortError")) {
          throw error;
        }
        lastError = error;
        if (attempt < this.#sidecarFailureThreshold) {
          await this.#clock.sleep(1_000, signal);
        }
      }
    }
    throw new V2RuntimeLifecycleFailure("sidecar_unreachable", null, {
      cause: lastError,
    });
  }

  #relaunchSidecar(
    generation: number,
    sourceTransport: V2RuntimeTransport,
    parentSignal: AbortSignal,
  ): Promise<V2RuntimeTransport> {
    if (this.#sidecarRelaunching) return this.#sidecarRelaunching;
    const controller = new AbortController();
    const forwardAbort = () => controller.abort(parentSignal.reason);
    parentSignal.addEventListener("abort", forwardAbort, { once: true });
    if (parentSignal.aborted) forwardAbort();
    const timeoutController = new AbortController();
    void this.#clock.sleep(this.#sidecarRecoveryTimeoutMs, timeoutController.signal).then(() => {
      controller.abort(new DOMException(
        "V2 in-place sidecar recovery deadline expired",
        "TimeoutError",
      ));
    }, () => undefined);
    const relaunch = this.#performSidecarRelaunch(
      generation,
      sourceTransport,
      controller.signal,
    ).finally(() => {
      timeoutController.abort();
      parentSignal.removeEventListener("abort", forwardAbort);
      if (this.#sidecarRelaunching === relaunch) this.#sidecarRelaunching = null;
    });
    this.#sidecarRelaunching = relaunch;
    return relaunch;
  }

  async #performSidecarRelaunch(
    generation: number,
    sourceTransport: V2RuntimeTransport,
    signal: AbortSignal,
  ): Promise<V2RuntimeTransport> {
    const binding = this.#binding;
    const gateway = this.#gateway;
    const ydoc = this.#ydoc;
    const oldBootstrap = this.#bootstrap;
    if (
      !binding || binding.protocolRevision !== "r2" ||
      !this.#credentials || !gateway || !ydoc || !oldBootstrap?.forceStop ||
      !sourceTransport.suspendSidecarRecovery ||
      !sourceTransport.failSidecarRecovery
    ) throw new Error("R2 same-session sidecar recovery is unavailable");

    const context: V2SidecarRelaunchContext = {
      generation,
      sourceTransport,
      capsule: null,
      awareness: null,
      bootstrap: null,
      transport: null,
      cleanup: null,
    };
    this.#sidecarRelaunchContext = context;
    try {
      await this.#proveSameSandboxSession(generation, binding.sessionId, gateway, signal);
      this.#assertSidecarRelaunchCurrent(context, signal);

      // A rotation that was already sealed/ACKed by the old stack must update
      // runtime credentials before the replacement snapshots them. The
      // refresh loop will not start another rotation while relaunch is active.
      const rotation = this.#ySweetRotation;
      if (rotation) await this.#abortable(rotation, signal);
      this.#assertSidecarRelaunchCurrent(context, signal);

      context.capsule = await sourceTransport.suspendSidecarRecovery(
        "same-session-sidecar-relaunch",
        signal,
      );
      this.#assertSidecarRelaunchCurrent(context, signal);
      await this.#abortable(
        oldBootstrap.forceStop("same-session-sidecar-relaunch", signal),
        signal,
      );
      this.#assertSidecarRelaunchCurrent(context, signal);

      const credentials = this.#credentials;
      if (!credentials) throw new Error("V2 credentials disappeared during sidecar recovery");

      const nextBinding: ProtocolBinding = {
        ...binding,
        launchNonce: crypto.randomUUID(),
      };
      context.awareness = this.#options.createAwareness(nextBinding, credentials);
      context.bootstrap = this.#options.createBootstrap({
        binding: nextBinding,
        workerIndex: this.#options.workerIndex,
        projectId: this.projectId,
        leaderGeneration: this.#options.leaderGeneration,
        credentials,
        ydoc,
        gateway,
        awareness: context.awareness,
        assets: this.#options.assets,
      });
      const descriptor = await this.#abortable(context.bootstrap.ensureReady(signal), signal);
      this.#assertSidecarRelaunchCurrent(context, signal);
      if (
        descriptor.releaseRevision !== this.#options.assets.revision ||
        descriptor.buildHash !== nextBinding.buildHash ||
        descriptor.schemaHash !== nextBinding.schemaHash ||
        descriptor.leaderGeneration !== nextBinding.leaderGeneration ||
        descriptor.protocolRevision !== nextBinding.protocolRevision ||
        descriptor.launchNonce !== nextBinding.launchNonce ||
        descriptor.instanceId === context.capsule.previousInstanceId
      ) throw new Error("Recovered sidecar descriptor does not match its fresh binding");
      // Bootstrap can take long enough for Prism to roll the sandbox session.
      // Re-prove the exact session immediately before the capsule is adopted.
      await this.#proveSameSandboxSession(generation, binding.sessionId, gateway, signal);
      this.#assertSidecarRelaunchCurrent(context, signal);
      context.transport = this.#options.createTransport({
        binding: nextBinding,
        transportProfile: this.#options.assets.transportProfile,
        channel: context.awareness,
        maxResponseBytes: this.#options.maxResponseBytes ?? 64 * 1024 * 1024,
        gzip: this.#options.gzip,
        expectedSidecarInstanceId: descriptor.instanceId,
        eventMemoryPool: this.#options.eventMemoryPool,
        onStateChange: () => {
          if (
            generation === this.#generation &&
            context.transport !== null &&
            this.#transport === context.transport
          ) this.#emit();
        },
      });
      const candidate = context.transport;
      if (!candidate.adoptRecoveredSidecar || !candidate.failSidecarRecovery) {
        throw new Error("Recovered R2 transport cannot adopt active requests");
      }
      this.#assertSidecarRelaunchCurrent(context, signal);
      const replayBarrier = candidate.adoptRecoveredSidecar(context.capsule);
      void replayBarrier.catch(() => undefined);
      await this.#abortable(candidate.connect(signal), signal);
      this.#assertSidecarRelaunchCurrent(context, signal);
      await this.#abortable(replayBarrier, signal);
      if (candidate.probeLiveness) await candidate.probeLiveness(signal);
      this.#assertSidecarRelaunchCurrent(context, signal);

      candidate.setHostCeiling?.(descriptor.capacity);
      this.#binding = nextBinding;
      this.#awareness = context.awareness;
      this.#bindAwarenessFailure(context.awareness, generation);
      this.#bootstrap = context.bootstrap;
      this.#transport = candidate;
      this.#descriptor = descriptor;
      context.capsule = null;
      if (this.#sidecarRelaunchContext === context) this.#sidecarRelaunchContext = null;
      return candidate;
    } catch (error) {
      this.#failSidecarRelaunchCapsule(context, error);
      await this.#cleanupSidecarRelaunch(context, "sidecar-relaunch-failed");
      throw error;
    }
  }

  async #proveSameSandboxSession(
    generation: number,
    sessionId: string,
    gateway: V2RuntimeGateway,
    signal: AbortSignal,
  ): Promise<void> {
    let heartbeat = await this.#abortable(gateway.heartbeat(signal), signal);
    if (!heartbeat.ok && (heartbeat.status === 401 || heartbeat.status === 403)) {
      const refresh = this.#refreshSandboxAuthentication;
      if (!refresh || await refresh(heartbeat.status, signal) !== "retry") {
        throw new Error("Sandbox authentication could not refresh before sidecar recovery");
      }
      if (generation !== this.#generation) {
        throw new DOMException("V2 sidecar recovery generation changed", "AbortError");
      }
      heartbeat = await this.#abortable(gateway.heartbeat(signal), signal);
    }
    if (!heartbeat.ok) throw new Error("Sandbox heartbeat failed before sidecar recovery");
    if (heartbeat.sessionId !== sessionId) {
      throw new Error("Sandbox session changed before sidecar recovery");
    }
    if (this.#credentialRefreshPending) {
      this.#credentialRefreshPending = false;
      this.#emit();
    }
  }

  #assertSidecarRelaunchCurrent(
    context: V2SidecarRelaunchContext,
    signal: AbortSignal,
  ): void {
    if (
      signal.aborted ||
      context.generation !== this.#generation ||
      this.#sidecarRelaunchContext !== context ||
      context.cleanup !== null
    ) {
      throw signal.reason ?? new DOMException("V2 sidecar recovery aborted", "AbortError");
    }
  }

  #failSidecarRelaunchCapsule(
    context: V2SidecarRelaunchContext,
    cause: unknown,
  ): void {
    const capsule = context.capsule;
    if (!capsule) return;
    context.capsule = null;
    const target = context.transport ?? context.sourceTransport;
    try { target.failSidecarRecovery?.(capsule, cause); } catch {}
  }

  #cleanupSidecarRelaunch(
    context: V2SidecarRelaunchContext,
    reason: string,
  ): Promise<void> {
    if (context.cleanup) return context.cleanup;
    this.#failSidecarRelaunchCapsule(
      context,
      new V2RuntimeLifecycleFailure("sidecar_unreachable"),
    );
    const cleanup = this.#runBounded(async signal => {
      const operations: Promise<unknown>[] = [];
      if (context.transport) {
        operations.push(Promise.resolve().then(() => context.transport!.forceStop
          ? context.transport!.forceStop(reason, signal)
          : context.transport!.close()));
      }
      if (context.bootstrap) {
        operations.push(Promise.resolve().then(() => context.bootstrap!.forceStop
          ? context.bootstrap!.forceStop(reason, signal)
          : context.bootstrap!.drainAndStop(reason, signal)));
      }
      if (context.awareness) {
        operations.push(this.#abortable(
          Promise.resolve().then(() => context.awareness!.close()),
          signal,
        ));
      }
      await Promise.allSettled(operations);
    }).then(() => undefined).finally(() => {
      if (this.#sidecarRelaunchContext === context) this.#sidecarRelaunchContext = null;
    });
    context.cleanup = cleanup;
    return cleanup;
  }

  async #refreshResources(generation: number, signal: AbortSignal): Promise<void> {
    while (!signal.aborted && generation === this.#generation) {
      const credentials = this.#credentials;
      if (!credentials) return;
      const deadline = Math.max(
        this.#clock.now(),
        credentials.resource.expiresAtMs - this.#resourceRefreshLeadMs,
      );
      await this.#clock.waitUntil(deadline, signal);
      if (signal.aborted || generation !== this.#generation) return;
      const current = this.#credentials;
      if (!current) return;
      try {
        const resource = await this.#options.provisioning.refreshResources(
          current,
          signal,
        );
        if (generation !== this.#generation) return;
        this.#credentials = { ...this.#credentials!, resource };
        this.#emit();
      } catch (error) {
        if (signal.aborted) return;
        this.#runtimeHealth.record("unhealthy");
        if (this.#clock.now() >= current.resource.expiresAtMs) throw error;
        await this.#clock.sleep(30_000, signal);
      }
    }
  }

  #bindAwarenessFailure(
    awareness: AwarenessChannel<HostFrame, SidecarFrame>,
    generation: number,
  ): void {
    this.#awarenessFailureUnsubscribe?.();
    this.#awarenessFailureUnsubscribe = awareness.onConnectionFailure?.(() => {
      if (
        generation !== this.#generation ||
        awareness !== this.#awareness ||
        this.#controller?.signal.aborted
      ) return;
      this.#ySweetReconnectPending = true;
      this.#awarenessFailureVersion++;
      this.#ySweetRefreshRequested = true;
      this.#wakeYSweetRefresh?.();
      this.#emit();
    }) ?? null;
  }

  async #waitForYSweetRefresh(
    deadline: number,
    signal: AbortSignal,
  ): Promise<void> {
    if (this.#ySweetRefreshRequested) {
      this.#ySweetRefreshRequested = false;
      return;
    }
    const timerController = new AbortController();
    const forwardAbort = () => timerController.abort(signal.reason);
    signal.addEventListener("abort", forwardAbort, { once: true });
    if (signal.aborted) forwardAbort();
    let wake!: () => void;
    const awakened = new Promise<void>(resolve => { wake = resolve; });
    this.#wakeYSweetRefresh = wake;
    try {
      await Promise.race([
        this.#clock.waitUntil(deadline, timerController.signal),
        awakened,
      ]);
      if (signal.aborted) throw signal.reason;
    } finally {
      if (this.#wakeYSweetRefresh === wake) this.#wakeYSweetRefresh = null;
      signal.removeEventListener("abort", forwardAbort);
      timerController.abort();
      this.#ySweetRefreshRequested = false;
    }
  }

  async #refreshYSweet(generation: number, signal: AbortSignal): Promise<void> {
    if (
      !this.#options.provisioning.mintYSweet ||
      !this.#bootstrap?.sealCredentialRotation ||
      !this.#transport?.rotateCredential
    ) return;
    let deadline = this.#clock.now() + this.#ySweetRefreshMs;
    while (!signal.aborted && generation === this.#generation) {
      await this.#waitForYSweetRefresh(deadline, signal);
      if (signal.aborted || generation !== this.#generation) return;
      const relaunching = this.#sidecarRelaunching;
      if (relaunching) {
        try {
          await this.#abortable(relaunching, signal);
        } catch {
          if (signal.aborted || generation !== this.#generation) return;
        }
        deadline = this.#clock.now();
        continue;
      }
      const credentials = this.#credentials;
      const bootstrap = this.#bootstrap;
      const transport = this.#transport;
      const awareness = this.#awareness;
      if (
        !credentials || !bootstrap?.sealCredentialRotation ||
        !transport?.rotateCredential || !awareness
      ) {
        return;
      }
      try {
        const rotation = (async () => {
          const rotationId = `rotation-${crypto.randomUUID()}`;
          const ySweet = await this.#options.provisioning.mintYSweet!(
            this.projectId,
            {
              source: "v2-credential-rotation",
              bootstrapAttempt: 1,
              previouslyConnected: true,
              sandboxUrl: credentials.sandbox.url,
              sandboxId: credentials.sandbox.sandboxId ?? null,
              sandboxSessionId: credentials.sandbox.sessionId ?? null,
            },
            signal,
          );
          const sealedJson = await bootstrap.sealCredentialRotation!(ySweet, rotationId);
          if (
            generation !== this.#generation ||
            bootstrap !== this.#bootstrap ||
            transport !== this.#transport ||
            awareness !== this.#awareness
          ) return false;
          const awarenessFailureVersion = this.#awarenessFailureVersion;
          const reconnectFirst = this.#ySweetReconnectPending;
          if (reconnectFirst) {
            // An in-band sidecar rotation cannot cross a disconnected host
            // Awareness lane. Reconnect the host with the reminted credential
            // first, then deliver the sealed credential to the still-running
            // sidecar over the restored reliable transport.
            await awareness.rotateCredential?.(ySweet);
            await transport.resumeAwareness?.(signal);
          }
          if (
            generation !== this.#generation ||
            bootstrap !== this.#bootstrap ||
            transport !== this.#transport ||
            awareness !== this.#awareness ||
            awarenessFailureVersion !== this.#awarenessFailureVersion
          ) return false;
          await transport.rotateCredential!(rotationId, sealedJson, signal);
          if (!reconnectFirst) {
            await awareness.rotateCredential?.(ySweet);
            await transport.resumeAwareness?.(signal);
          }
          if (
            generation !== this.#generation ||
            bootstrap !== this.#bootstrap ||
            transport !== this.#transport ||
            awareness !== this.#awareness ||
            awarenessFailureVersion !== this.#awarenessFailureVersion
          ) return false;
          this.#credentials = { ...this.#credentials!, ySweet };
          this.#ySweetReconnectPending = false;
          this.#ySweetRefreshRequested = false;
          return true;
        })();
        const trackedRotation = rotation.then(() => undefined);
        // The refresh loop awaits `rotation` below, while recovery/shutdown may
        // independently await this void projection. Attach an owner now so a
        // recoverable reconnect failure never reaches process unhandledRejection.
        void trackedRotation.catch(() => undefined);
        this.#ySweetRotation = trackedRotation;
        const applied = await rotation;
        if (applied) this.#emit();
        deadline = applied
          ? this.#clock.now() + this.#ySweetRefreshMs
          : this.#clock.now();
      } catch (error) {
        if (signal.aborted) return;
        this.#runtimeHealth.record("unhealthy");
        await this.#clock.sleep(30_000, signal);
        deadline = this.#clock.now();
      } finally {
        this.#ySweetRotation = null;
      }
    }
  }

  #failGeneration(generation: number, failure: V2RuntimeLifecycleFailure): void {
    if (generation !== this.#generation) return;
    if (this.#state === "failed" || this.#state === "stopped") return;
    if (this.#credentials && shouldQuarantineSandboxSession(failure)) {
      this.#reportSandboxFailure(this.#credentials.sandbox);
    }
    this.#failure = failure;
    this.#runtimeHealth.record("unhealthy");
    ++this.#generation;
    this.#setState("failed");
    this.#controller?.abort();
  }

  #reportSandboxFailure(sandbox: ProvisionedSandbox): void {
    try {
      this.#options.provisioning.reportSandboxFailure?.(this.projectId, sandbox);
    } catch {
      // Recovery policy telemetry/admission must never replace the runtime's
      // authoritative lifecycle failure.
    }
  }

  #reportSandboxReady(sandbox: ProvisionedSandbox): void {
    try {
      this.#options.provisioning.reportSandboxReady?.(this.projectId, sandbox);
    } catch {
      // Readiness is already proven; auxiliary failure-history cleanup cannot
      // make a healthy runtime fail startup.
    }
  }

  #maybeFailAmbiguousTransport(error: unknown): void {
    if (classifyRuntimeError(error) === "neutral") return;
    if (!error || typeof error !== "object") return;
    const value = error as { committed?: unknown; ambiguous?: unknown };
    if (value.committed !== true || value.ambiguous !== true) return;
    const transport = this.#transport?.snapshot();
    if (!transport) return;
    const ambiguityLimit = transport.ambiguityLimit ?? Math.max(
      1,
      Math.min(4, this.#descriptor?.capacity ?? 4),
    );
    const ambiguousQuarantined = transport.ambiguousQuarantined ?? transport.quarantined;
    if (transport.connected && ambiguousQuarantined < ambiguityLimit) return;
    this.#failGeneration(
      this.#generation,
      new V2RuntimeLifecycleFailure(
        transport.connected ? "ambiguity_exhausted" : "sidecar_unreachable",
      ),
    );
  }

  #deferAmbiguityHealthDuringRelaunch(error: unknown): void {
    const relaunching = this.#sidecarRelaunching;
    if (!relaunching) {
      this.#maybeFailAmbiguousTransport(error);
      return;
    }
    void relaunching.then(
      () => {
        this.#maybeFailAmbiguousTransport(error);
        this.#emit();
      },
      () => undefined,
    );
  }

  #beginShutdown(kind: "drain" | "stop", reason: string): Promise<void> {
    if (this.#state === "stopped") return Promise.resolve();
    if (this.#shutdownPromise) {
      if (kind === "stop" && this.#shutdownKind === "drain") {
        this.#shutdownKind = "stop";
        this.#escalateShutdown?.();
      }
      return this.#shutdownPromise;
    }
    ++this.#generation;
    this.#controller?.abort(new DOMException("V2 runtime is shutting down", "AbortError"));
    this.#setState("draining");
    this.#shutdownKind = kind;
    let escalate!: () => void;
    const escalation = new Promise<void>(resolve => { escalate = resolve; });
    this.#escalateShutdown = escalate;
    const shutdown = this.#shutdownRuntime(kind, reason, escalation)
      .finally(() => {
        this.#shutdownKind = null;
        this.#escalateShutdown = null;
        this.#setState("stopped");
      });
    this.#shutdownPromise = shutdown;
    return shutdown;
  }

  async #shutdownRuntime(
    initialKind: "drain" | "stop",
    reason: string,
    escalation: Promise<void>,
  ): Promise<void> {
    const starting = this.#starting;
    const relaunching = this.#sidecarRelaunching;
    const relaunchContext = this.#sidecarRelaunchContext;
    const relaunchCleanup = relaunchContext
      ? this.#cleanupSidecarRelaunch(relaunchContext, "runtime-shutdown")
      : null;
    if (relaunching || relaunchCleanup) {
      await this.#runBounded(async signal => {
        await Promise.allSettled([
          relaunching ? this.#abortable(relaunching, signal) : Promise.resolve(),
          relaunchCleanup ? this.#abortable(relaunchCleanup, signal) : Promise.resolve(),
        ]);
      });
    }
    const transport = this.#transport;
    const bootstrap = this.#bootstrap;
    let clean = false;
    if (initialKind === "drain") {
      clean = await this.#runBounded(
        signal => transport
          ? transport.drain(reason, signal)
          : bootstrap?.drainAndStop(reason, signal) ?? Promise.resolve(),
        escalation,
      );
    }
    if (!clean || this.#shutdownKind === "stop") {
      if (transport) {
        await this.#runBounded(signal => transport.forceStop
          ? transport.forceStop(reason, signal)
          : transport.drain(reason, signal));
      }
      if (bootstrap) {
        await this.#runBounded(signal => bootstrap.forceStop
          ? bootstrap.forceStop(reason, signal)
          : bootstrap.drainAndStop(reason, signal));
      }
    }
    await this.#runBounded(() => this.#dispose(true));
    if (starting) {
      await this.#runBounded(signal => this.#abortable(starting, signal));
      await this.#runBounded(() => this.#dispose(true));
    }
  }

  async #runBounded(
    operation: (signal: AbortSignal) => Promise<void>,
    escalation?: Promise<void>,
  ): Promise<boolean> {
    const operationController = new AbortController();
    const timeoutController = new AbortController();
    const completed = Promise.resolve()
      .then(() => operation(operationController.signal))
      .then(() => true, () => false);
    const timeout = this.#clock.sleep(this.#stopTimeoutMs, timeoutController.signal)
      .then(() => false, () => false);
    const candidates: Promise<boolean>[] = [completed, timeout];
    if (escalation) candidates.push(escalation.then(() => false));
    const result = await Promise.race(candidates);
    timeoutController.abort();
    if (!result) operationController.abort(
      new DOMException("V2 runtime shutdown deadline expired", "TimeoutError"),
    );
    return result;
  }

  #abortable<T>(source: Promise<T>, signal: AbortSignal): Promise<T> {
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (operation: () => void) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        operation();
      };
      const onAbort = () => finish(() => reject(signal.reason));
      signal.addEventListener("abort", onAbort, { once: true });
      source.then(
        value => finish(() => resolve(value)),
        error => finish(() => reject(error)),
      );
    });
  }

  async #dispose(closeTransport: boolean): Promise<void> {
    this.#awarenessFailureUnsubscribe?.();
    this.#awarenessFailureUnsubscribe = null;
    this.#wakeYSweetRefresh?.();
    this.#wakeYSweetRefresh = null;
    const sidecarLoop = this.#sidecarLoop;
    this.#sidecarLoop = null;
    const refreshLoop = this.#refreshLoop;
    this.#refreshLoop = null;
    const ySweetLoop = this.#ySweetLoop;
    this.#ySweetLoop = null;
    const credentialRefreshTask = this.#credentialRefreshTask;
    this.#credentialRefreshTask = null;
    const ySweetRotation = this.#ySweetRotation;
    this.#ySweetRotation = null;
    const transport = this.#transport;
    const awareness = this.#awareness;
    const heartbeat = this.#heartbeat;
    const ydoc = this.#ydoc;
    this.#transport = null;
    this.#awareness = null;
    this.#heartbeat = null;
    this.#ydoc = null;
    this.#gateway = null;
    this.#bootstrap = null;
    this.#descriptor = null;
    this.#binding = null;
    this.#credentials = null;
    this.#refreshSandboxAuthentication = null;
    this.#sidecarRelaunchContext = null;
    this.#controller = null;
    this.#sidecarSuspect = false;
    this.#credentialRefreshPending = false;
    this.#ySweetReconnectPending = false;
    this.#awarenessFailureVersion = 0;
    this.#ySweetRefreshRequested = false;
    await Promise.allSettled([
      refreshLoop ?? Promise.resolve(),
      ySweetLoop ?? Promise.resolve(),
      credentialRefreshTask ?? Promise.resolve(),
      ySweetRotation ?? Promise.resolve(),
      sidecarLoop ?? Promise.resolve(),
      closeTransport ? transport?.close() ?? Promise.resolve() : Promise.resolve(),
      awareness?.close() ?? Promise.resolve(),
      heartbeat?.stop() ?? Promise.resolve(),
      ydoc?.destroy() ?? Promise.resolve(),
    ]);
  }

  #setState(state: RuntimeState): void {
    this.#state = state;
    this.#emit();
  }

  #emit(): void {
    for (const listener of this.#listeners) listener();
  }

  #notifyCredentialRefresh(
    outcome: CredentialRefreshOutcome,
    status: number,
  ): void {
    try { this.#options.onCredentialRefresh?.(outcome, status); } catch {}
  }
}
