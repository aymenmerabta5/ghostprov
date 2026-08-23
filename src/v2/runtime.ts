import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type {
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
import type {
  V2TransportSession,
  V2TransportSnapshot,
} from "./transport/session";
import {
  DecayingRuntimeFailureScore,
  classifyRuntimeError,
} from "./runtime-health";

export interface V2RuntimeProvisioning {
  provision(
    projectId: ProjectId,
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
  heartbeat(signal?: AbortSignal): Promise<{
    ok: boolean;
    status: number;
    durationMs: number;
    sessionId: string | null;
  }>;
}

export interface V2RuntimeHeartbeat {
  start(): void;
  stop(): Promise<void>;
  snapshot(): { lastSuccessAtMs: number | null };
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
  createGateway(sandbox: ProvisionedSandbox): V2RuntimeGateway;
  createHeartbeat(
    gateway: V2RuntimeGateway,
    callbacks: {
      onExpired(status: number): void;
      onTransientFailure(status: number | null): void;
    },
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
  readonly #listeners = new Set<() => void>();
  readonly #resourceRefreshLeadMs: number;
  readonly #ySweetRefreshMs: number;
  #state: RuntimeState = "configured";
  #generation = 0;
  #controller: AbortController | null = null;
  #starting: Promise<void> | null = null;
  #refreshLoop: Promise<void> | null = null;
  #ySweetLoop: Promise<void> | null = null;
  #gateway: V2RuntimeGateway | null = null;
  #heartbeat: V2RuntimeHeartbeat | null = null;
  #ydoc: V2RuntimeYDoc | null = null;
  #awareness: AwarenessChannel<HostFrame, SidecarFrame> | null = null;
  #bootstrap: V2RuntimeBootstrap | null = null;
  #transport: V2RuntimeTransport | null = null;
  #credentials: ProvisionedRuntimeCredentials | null = null;
  #descriptor: V2ReadyDescriptor | null = null;
  #failure: unknown = null;
  #completionEwmaMs: number | null = null;

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
    this.#resourceRefreshLeadMs = refreshLead;
    this.#ySweetRefreshMs = ySweetRefreshMs;
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
        this.#completionEwmaMs = this.#completionEwmaMs === null
          ? sample
          : this.#completionEwmaMs * 0.8 + sample * 0.2;
        this.#runtimeHealth.record("healthy");
        this.#emit();
        return value;
      },
      error => {
        this.#runtimeHealth.record(classifyRuntimeError(error));
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
    const capacity = this.#state === "ready"
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
      available: this.#state === "ready" && transport
        ? Math.max(0, Math.min(transport.admissionCredits ?? transport.credits, capacity - active - quarantined))
        : 0,
      quarantined,
      heartbeatAgeMs: heartbeatAt === null ? null : Math.max(0, this.#clock.now() - heartbeatAt),
      credentialExpiresInMs: this.#credentials
        ? Math.max(0, this.#credentials.resource.expiresAtMs - this.#clock.now())
        : null,
      ackEwmaMs: transport?.awarenessRttMs ?? 0,
      completionEwmaMs: this.#completionEwmaMs ?? 0,
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
    const startHeartbeat = (sandbox: ProvisionedSandbox) => {
      if (generation !== this.#generation || this.#heartbeat) return;
      this.#setState("syncing_credentials");
      const gateway = this.#options.createGateway(sandbox);
      const heartbeat = this.#options.createHeartbeat(gateway, {
        onExpired: status => {
          if (generation === this.#generation) void this.drain(`heartbeat-expired:${status}`);
        },
        onTransientFailure: () => {
          this.#runtimeHealth.record("unhealthy");
          this.#emit();
        },
      });
      this.#gateway = gateway;
      this.#heartbeat = heartbeat;
      heartbeat.start();
    };
    try {
      const credentials = await this.#options.provisioning.provision(
        this.projectId,
        controller.signal,
        { onSandboxAcquired: startHeartbeat },
      );
      if (generation !== this.#generation) return;
      startHeartbeat(credentials.sandbox);
      if (!this.#gateway) throw new Error("V2 sandbox gateway was not created");
      this.#credentials = credentials;

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
      });
      this.#transport = transport;
      transport.setHostCeiling?.(this.#hostCeiling(descriptor.capacity));
      this.#setState("connecting_transport");
      await transport.connect(controller.signal);
      if (generation !== this.#generation) return;
      this.#setState("ready");
      this.#refreshLoop = this.#refreshResources(generation, controller.signal);
      void this.#refreshLoop.catch(error => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        this.#failure = error;
        this.#emit();
      });
      this.#ySweetLoop = this.#refreshYSweet(generation, controller.signal);
      void this.#ySweetLoop.catch(error => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        this.#failure = error;
        this.#emit();
      });
    } catch (error) {
      if (generation !== this.#generation) return;
      if (error instanceof DOMException && error.name === "AbortError") return;
      this.#failure = error;
      this.#setState("failed");
      await this.#dispose(true);
      throw error;
    }
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
      try {
        const resource = await this.#options.provisioning.refreshResources(
          credentials,
          signal,
        );
        if (generation !== this.#generation) return;
        this.#credentials = { ...this.#credentials!, resource };
        this.#emit();
      } catch (error) {
        if (signal.aborted) return;
        this.#runtimeHealth.record("unhealthy");
        if (this.#clock.now() >= credentials.resource.expiresAtMs) throw error;
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

  async #dispose(closeTransport: boolean): Promise<void> {
    const refreshLoop = this.#refreshLoop;
    this.#refreshLoop = null;
    const ySweetLoop = this.#ySweetLoop;
    this.#ySweetLoop = null;
    if (refreshLoop) await refreshLoop.catch(() => undefined);
    if (ySweetLoop) await ySweetLoop.catch(() => undefined);
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
  }

  #setState(state: RuntimeState): void {
    this.#state = state;
    this.#emit();
  }

  #emit(): void {
    for (const listener of this.#listeners) listener();
  }
}
