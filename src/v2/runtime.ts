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
import type { RoutableSandboxRuntime, RoutableSandboxSnapshot } from "../sandbox/registry";
import { describeSandboxFailure } from "../sandbox/runtime";
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
  V2TransportSession,
  V2TransportSnapshot,
} from "./transport/session";
import {
  DecayingRuntimeFailureScore,
  classifyRuntimeError,
} from "./runtime-health";
import { DecayingRuntimeCompletionEstimate } from "./runtime-completion";

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
  sealCredentialRotation?(ySweet: YSweetEnvelope, rotationId: string): Promise<string>;
}

export interface V2RuntimeTransport {
  connect(signal?: AbortSignal): Promise<void>;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
  drain(reason: string, signal?: AbortSignal): Promise<void>;
  close(): Promise<void>;
  snapshot(): V2TransportSnapshot;
  setHostCeiling?(value: number): void;
  rotateCredential?(rotationId: string, sealedJson: string, signal?: AbortSignal): Promise<void>;
  probeLiveness?(signal?: AbortSignal): Promise<{ rttMs: number; sentAtMs: number }>;
}

export interface V2RuntimeGovernor {
  capacityHint(perActiveRequestBytes: number, physicalMaximum: number): number;
  snapshot(): {
    reservationCapacityBytes: number;
    reservedBytes: number;
    paused: boolean;
  };
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
  gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
  clock?: Clock;
  resourceRefreshLeadMs?: number;
  ySweetRefreshMs?: number;
  sidecarProbePeriodMs?: number;
  sidecarFailureThreshold?: number;
  sidecarCommitStallMs?: number;
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
    channel: AwarenessChannel<HostFrame, SidecarFrame>;
    maxResponseBytes: number;
    gzip?: (bytes: Uint8Array) => Promise<Uint8Array>;
    expectedSidecarInstanceId: string;
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
  #state: RuntimeState = "configured";
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
  #failure: unknown = null;
  #replacementSandbox: ProvisionedSandbox | null = null;
  #credentialRefreshPending = false;

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
    this.#sidecarCommitStallMs = options.sidecarCommitStallMs ?? 60_000;
    if (!Number.isSafeInteger(this.#sidecarProbePeriodMs) || this.#sidecarProbePeriodMs < 1) {
      throw new RangeError("sidecarProbePeriodMs must be positive");
    }
    if (!Number.isSafeInteger(this.#sidecarFailureThreshold) || this.#sidecarFailureThreshold < 1) {
      throw new RangeError("sidecarFailureThreshold must be positive");
    }
    if (!Number.isSafeInteger(this.#sidecarCommitStallMs) || this.#sidecarCommitStallMs < 1) {
      throw new RangeError("sidecarCommitStallMs must be positive");
    }
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  start(): Promise<void> {
    if (this.#state === "ready") return Promise.resolve();
    if (this.#starting) return this.#starting;
    const generation = ++this.#generation;
    const starting = this.#startGeneration(generation).finally(() => {
      if (this.#starting === starting) this.#starting = null;
    });
    this.#starting = starting;
    return starting;
  }

  async drain(reason: string): Promise<void> {
    if (this.#state === "stopped") return;
    this.#setState("draining");
    this.#controller?.abort();
    if (this.#transport) await this.#transport.drain(reason);
    else await this.#bootstrap?.drainAndStop(reason).catch(() => undefined);
    await this.#dispose(false);
    this.#setState("stopped");
  }

  async stop(): Promise<void> {
    if (this.#state === "stopped") return;
    ++this.#generation;
    this.#controller?.abort();
    await this.#dispose(true);
    this.#setState("stopped");
  }

  async run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
  ): Promise<RunHandle> {
    if (this.#state !== "ready" || !this.#transport || !this.#descriptor) {
      throw new Error(`V2 sandbox ${this.projectId} is not ready`);
    }
    const ceiling = this.#hostCeiling(this.#descriptor.capacity);
    this.#transport.setHostCeiling?.(ceiling);
    const startedAt = this.#clock.now();
    let handle: RunHandle;
    try {
      handle = await this.#transport.run(body, signal);
    } catch (error) {
      this.#runtimeHealth.record(classifyRuntimeError(error));
      this.#maybeFailAmbiguousTransport(error);
      this.#emit();
      throw error;
    }
    const final = handle.final.then(
      value => {
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
        this.#maybeFailAmbiguousTransport(error);
        this.#emit();
        throw error;
      },
    );
    void final.catch(() => undefined);
    this.#emit();
    return {
      events: () => handle.events(),
      final,
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
    const descriptorCapacity = this.#descriptor?.capacity ?? 0;
    const active = transport?.active ?? 0;
    const quarantined = transport?.quarantined ?? 0;
    const governorCapacity = descriptorCapacity > 0
      ? this.#hostCeiling(descriptorCapacity)
      : 0;
    const creditCapacity = transport
      ? Math.max(0, transport.credits + active)
      : 0;
    const capacity = this.#state === "ready" && !this.#sidecarSuspect && !this.#credentialRefreshPending
      ? Math.max(0, Math.min(descriptorCapacity, governorCapacity, creditCapacity))
      : 0;
    const heartbeatAt = this.#heartbeat?.snapshot().lastSuccessAtMs ?? null;
    const governor = this.#options.governor.snapshot();
    const memoryPressure = governor.reservationCapacityBytes > 0
      ? Math.max(0, Math.min(1, governor.reservedBytes / governor.reservationCapacityBytes))
      : governor.paused ? 1 : 0;
    return {
      projectId: this.projectId,
      state: this.#state,
      capacity,
      active,
      available: this.#state === "ready" && !this.#sidecarSuspect && !this.#credentialRefreshPending && transport
        ? Math.max(0, Math.min(transport.admissionCredits ?? transport.credits, capacity - active - quarantined))
        : 0,
      quarantined,
      ambiguousQuarantined: transport?.ambiguousQuarantined ?? 0,
      ambiguityLimit: transport?.ambiguityLimit ?? Math.max(
        1,
        Math.min(4, governorCapacity || descriptorCapacity || 4),
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
        onAuthenticationFailure: async (status, signal) => {
          if (
            !callbackIsCurrent() ||
            (!this.#options.acquireSandboxForAuth && !this.#options.provisioning.acquireSandbox)
          ) {
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
              ? await this.#options.acquireSandboxForAuth(status, signal)
              : await this.#options.provisioning.acquireSandbox!(
                signal,
                { maxAttempts: 1 },
              );
          } catch (error) {
            const value = error && typeof error === "object"
              ? error as { kind?: unknown; status?: unknown }
              : {};
            this.#notifyCredentialRefresh(
              signal.aborted
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
        },
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
      };
      const awareness = this.#options.createAwareness(binding, credentials);
      this.#awareness = awareness;
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
        descriptor.buildHash !== binding.buildHash ||
        descriptor.schemaHash !== binding.schemaHash ||
        descriptor.leaderGeneration !== binding.leaderGeneration
      ) throw new Error("V2 bootstrap descriptor does not match its binding");
      if (generation !== this.#generation) return;
      this.#descriptor = descriptor;

      const transport = this.#options.createTransport({
        binding,
        channel: awareness,
        maxResponseBytes: this.#options.maxResponseBytes ?? 64 * 1024 * 1024,
        gzip: this.#options.gzip,
        expectedSidecarInstanceId: descriptor.instanceId,
      });
      this.#transport = transport;
      transport.setHostCeiling?.(this.#hostCeiling(descriptor.capacity));
      this.#setState("connecting_transport");
      await transport.connect(controller.signal);
      if (generation !== this.#generation) return;
      if (transport.probeLiveness) {
        await this.#proveInitialSidecar(transport, controller.signal);
      }
      if (generation !== this.#generation) return;
      this.#sidecarSuspect = false;
      this.#setState("ready");
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
      this.#failure = error;
      this.#setState("failed");
      await this.#dispose(true);
      throw error;
    }
  }

  async #monitorSidecar(
    generation: number,
    transport: V2RuntimeTransport,
    signal: AbortSignal,
  ): Promise<void> {
    if (!transport.probeLiveness) return;
    let consecutiveFailures = 0;
    let deadline = this.#clock.now() + this.#sidecarProbePeriodMs;
    while (!signal.aborted && generation === this.#generation) {
      try {
        await this.#clock.waitUntil(deadline, signal);
      } catch {
        if (signal.aborted) return;
      }
      if (signal.aborted || generation !== this.#generation) return;
      try {
        const diagnostics = transport.snapshot().diagnostics?.host;
        if (
          diagnostics &&
          diagnostics.admittedUncommitted > 0 &&
          diagnostics.ackProgressAgeMs >= this.#sidecarCommitStallMs
        ) {
          throw new Error("V2 sidecar commit processing stopped making progress");
        }
        await transport.probeLiveness(signal);
        if (generation !== this.#generation) return;
        consecutiveFailures = 0;
        if (this.#sidecarSuspect) {
          this.#sidecarSuspect = false;
          this.#emit();
        }
      } catch (error) {
        if (signal.aborted || (error instanceof DOMException && error.name === "AbortError")) {
          return;
        }
        if (generation !== this.#generation) return;
        consecutiveFailures++;
        if (!this.#sidecarSuspect) {
          this.#sidecarSuspect = true;
          this.#emit();
        }
        if (consecutiveFailures >= this.#sidecarFailureThreshold) {
          this.#failGeneration(
            generation,
            new V2RuntimeLifecycleFailure("sidecar_unreachable", null, { cause: error }),
          );
          return;
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

  async #refreshYSweet(generation: number, signal: AbortSignal): Promise<void> {
    if (
      !this.#options.provisioning.mintYSweet ||
      !this.#bootstrap?.sealCredentialRotation ||
      !this.#transport?.rotateCredential
    ) return;
    let deadline = this.#clock.now() + this.#ySweetRefreshMs;
    while (!signal.aborted && generation === this.#generation) {
      await this.#clock.waitUntil(deadline, signal);
      if (signal.aborted || generation !== this.#generation) return;
      const credentials = this.#credentials;
      const bootstrap = this.#bootstrap;
      const transport = this.#transport;
      if (!credentials || !bootstrap?.sealCredentialRotation || !transport?.rotateCredential) {
        return;
      }
      try {
        const rotationId = `rotation-${crypto.randomUUID()}`;
        const ySweet = await this.#options.provisioning.mintYSweet(
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
        const sealedJson = await bootstrap.sealCredentialRotation(ySweet, rotationId);
        await transport.rotateCredential(rotationId, sealedJson, signal);
        if (generation !== this.#generation) return;
        this.#awareness?.rotateCredential?.(ySweet);
        this.#credentials = { ...this.#credentials!, ySweet };
        this.#emit();
        deadline = this.#clock.now() + this.#ySweetRefreshMs;
      } catch (error) {
        if (signal.aborted) return;
        this.#runtimeHealth.record("unhealthy");
        await this.#clock.sleep(30_000, signal);
        deadline = this.#clock.now();
      }
    }
  }

  #hostCeiling(physicalMaximum: number): number {
    return this.#options.governor.capacityHint(
      this.#options.transportReservationBytes,
      physicalMaximum,
    );
  }

  #failGeneration(generation: number, failure: V2RuntimeLifecycleFailure): void {
    if (generation !== this.#generation) return;
    if (this.#state === "failed" || this.#state === "stopped") return;
    this.#failure = failure;
    this.#runtimeHealth.record("unhealthy");
    ++this.#generation;
    this.#setState("failed");
    this.#controller?.abort();
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

  async #dispose(closeTransport: boolean): Promise<void> {
    const sidecarLoop = this.#sidecarLoop;
    this.#sidecarLoop = null;
    const refreshLoop = this.#refreshLoop;
    this.#refreshLoop = null;
    const ySweetLoop = this.#ySweetLoop;
    this.#ySweetLoop = null;
    if (refreshLoop) await refreshLoop.catch(() => undefined);
    if (ySweetLoop) await ySweetLoop.catch(() => undefined);
    if (sidecarLoop) await sidecarLoop.catch(() => undefined);
    const transport = this.#transport;
    const awareness = this.#awareness;
    const heartbeat = this.#heartbeat;
    const ydoc = this.#ydoc;
    this.#transport = null;
    this.#awareness = null;
    this.#heartbeat = null;
    this.#ydoc = null;
    await Promise.allSettled([
      closeTransport ? transport?.close() ?? Promise.resolve() : Promise.resolve(),
      !transport ? awareness?.close() ?? Promise.resolve() : Promise.resolve(),
      heartbeat?.stop() ?? Promise.resolve(),
      ydoc?.destroy() ?? Promise.resolve(),
    ]);
    this.#gateway = null;
    this.#bootstrap = null;
    this.#descriptor = null;
    this.#credentials = null;
    this.#sidecarSuspect = false;
    this.#credentialRefreshPending = false;
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
