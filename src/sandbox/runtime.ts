import type {
  ProvisionedRuntimeCredentials,
  ProvisioningHooks,
} from "../prism/provisioning-client";
import type { ProvisionedSandbox } from "../prism/protocol";
import type {
  ProjectId,
  SandboxFailureSnapshot,
  SandboxLifecycleState,
  SandboxSnapshot,
} from "./types";
import type { DaemonDescriptor } from "./daemon-controller";

export interface RuntimeYDoc {
  connect(): Promise<void>;
  destroy(): Promise<void>;
}

export interface RuntimeHeartbeat {
  start(): void;
  stop(): Promise<void>;
  snapshot(): { lastSuccessAtMs: number | null };
}

export interface RuntimeDaemonController {
  ensureReady(signal?: AbortSignal): Promise<DaemonDescriptor>;
}

export interface RuntimeHeartbeatCallbacks {
  onExpired(status: number): void;
  onTransientFailure(status: number | null): void;
}

export interface SandboxRuntimeOptions {
  projectId: ProjectId;
  provision(
    projectId: ProjectId,
    signal?: AbortSignal,
    hooks?: ProvisioningHooks,
  ): Promise<ProvisionedRuntimeCredentials>;
  createYDoc(credentials: ProvisionedRuntimeCredentials): RuntimeYDoc;
  createHeartbeat(
    sandbox: ProvisionedSandbox,
    callbacks: RuntimeHeartbeatCallbacks,
  ): RuntimeHeartbeat;
  createDaemonController(
    credentials: ProvisionedRuntimeCredentials,
    ydoc: RuntimeYDoc,
  ): RuntimeDaemonController;
}

type Listener = (snapshot: SandboxSnapshot) => void;

const SAFE_DETAIL = /^[a-zA-Z][a-zA-Z0-9._-]{0,63}$/;

function safeDetail(value: unknown): string | null {
  return typeof value === "string" && SAFE_DETAIL.test(value) ? value : null;
}

function redactFailureMessage(value: string): string {
  return value
    .replace(/\b(cookie|authorization|x-crixet-[a-z0-9_-]+)\s*[:=]\s*[^,\r\n]*/gi, "$1=[REDACTED]")
    .replace(/\b(token|api[_-]?key|secret|password)\s*[:=]\s*[^;\s,]+/gi, "$1=[REDACTED]")
    .replace(/[a-zA-Z0-9+/_=-]{64,}/g, "[REDACTED]")
    .slice(0, 240);
}

export function describeSandboxFailure(error: unknown): SandboxFailureSnapshot {
  const record = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const rawMessage = error instanceof Error ? error.message : String(error ?? "Sandbox runtime failed");
  const rawName = error instanceof Error ? error.name : "Error";
  const status = typeof record.status === "number" && Number.isInteger(record.status) &&
    record.status >= 100 && record.status <= 599
    ? record.status
    : null;
  return {
    name: safeDetail(rawName) ?? "Error",
    kind: safeDetail(record.kind),
    status,
    operation: safeDetail(record.operation),
    message: redactFailureMessage(rawMessage) || "Sandbox runtime failed",
  };
}

export class SandboxRuntime {
  readonly projectId: ProjectId;
  readonly #options: SandboxRuntimeOptions;
  readonly #listeners = new Set<Listener>();
  #state: SandboxLifecycleState;
  #generation = 0;
  #capacity = 0;
  #active = 0;
  #quarantined = 0;
  #heartbeat: RuntimeHeartbeat | null = null;
  #ydoc: RuntimeYDoc | null = null;
  #controller: AbortController | null = null;
  #daemon: DaemonDescriptor | null = null;

  constructor(options: SandboxRuntimeOptions) {
    this.projectId = options.projectId;
    this.#options = options;
    this.#state = { type: "configured", projectId: options.projectId };
  }

  subscribe(listener: Listener): () => void {
    this.#listeners.add(listener);
    listener(this.snapshot());
    return () => this.#listeners.delete(listener);
  }

  start(): Promise<void> {
    if (this.#state.type === "ready") return Promise.resolve();
    return this.#startGeneration(++this.#generation);
  }

  async recover(reason: string): Promise<void> {
    const generation = ++this.#generation;
    this.#setState({ type: "recovering", projectId: this.projectId, generation, attempt: 1 });
    this.#controller?.abort();
    await this.#disposeServices();
    await this.#startGeneration(generation, reason);
  }

  drain(reason: string): void {
    if (this.#state.type === "stopped") return;
    this.#setState({
      type: "draining",
      projectId: this.projectId,
      generation: this.#generation,
      reason,
    });
  }

  async stop(): Promise<void> {
    const generation = ++this.#generation;
    this.#controller?.abort();
    await this.#disposeServices();
    this.#capacity = 0;
    this.#daemon = null;
    this.#setState({ type: "stopped", projectId: this.projectId, generation });
  }

  snapshot(): SandboxSnapshot {
    const lastSuccess = this.#heartbeat?.snapshot().lastSuccessAtMs ?? null;
    return {
      projectId: this.projectId,
      state: this.#state,
      generation: this.#generation,
      capacity: this.#capacity,
      active: this.#active,
      available: Math.max(0, this.#capacity - this.#active - this.#quarantined),
      quarantined: this.#quarantined,
      heartbeatAgeMs: lastSuccess === null ? null : Math.max(0, Date.now() - lastSuccess),
      resourceExpiresInMs: null,
      daemonBuild: this.#daemon?.build ?? null,
      daemonPid: this.#daemon?.pid ?? null,
      protocols: this.#daemon?.protocols ?? [],
      failure: this.#state.type === "failed" ? describeSandboxFailure(this.#state.error) : null,
    } as SandboxSnapshot;
  }

  async #startGeneration(generation: number, _reason?: string): Promise<void> {
    const controller = new AbortController();
    this.#controller = controller;
    this.#setState({ type: "provisioning", projectId: this.projectId, generation });
    try {
      const startHeartbeat = (sandbox: ProvisionedSandbox) => {
        if (!this.#current(generation) || this.#heartbeat) return;
        this.#setState({ type: "syncing_credentials", projectId: this.projectId, generation });
        const heartbeat = this.#options.createHeartbeat(sandbox, {
          onExpired: status => {
            if (this.#current(generation)) this.drain(`heartbeat-expired:${status}`);
          },
          onTransientFailure: () => {},
        });
        this.#heartbeat = heartbeat;
        heartbeat.start();
      };
      const credentials = await this.#options.provision(
        this.projectId,
        controller.signal,
        { onSandboxAcquired: startHeartbeat },
      );
      if (!this.#current(generation)) return;
      startHeartbeat(credentials.sandbox);

      const ydoc = this.#options.createYDoc(credentials);
      this.#ydoc = ydoc;
      this.#setState({ type: "syncing_workspace", projectId: this.projectId, generation });
      await ydoc.connect();
      if (!this.#current(generation)) return;

      this.#setState({ type: "deploying_daemon", projectId: this.projectId, generation });
      const daemon = await this.#options.createDaemonController(credentials, ydoc).ensureReady(controller.signal);
      if (!this.#current(generation)) return;
      this.#daemon = daemon;
      this.#capacity = daemon.capacity;
      this.#setState({ type: "ready", projectId: this.projectId, generation, capacity: daemon.capacity });
    } catch (error: any) {
      if (!this.#current(generation)) return;
      if (error?.name === "AbortError") return;
      await this.#disposeServices();
      if (error?.kind === "auth") {
        this.#setState({ type: "auth_required", projectId: this.projectId, generation });
      } else {
        this.#setState({
          type: "failed",
          projectId: this.projectId,
          generation,
          error: error instanceof Error ? error : new Error(String(error)),
        });
      }
      throw error;
    }
  }

  #current(generation: number): boolean {
    return generation === this.#generation && this.#state.type !== "stopped";
  }

  #setState(state: SandboxLifecycleState): void {
    this.#state = state;
    const snapshot = this.snapshot();
    for (const listener of this.#listeners) listener(snapshot);
  }

  async #disposeServices(): Promise<void> {
    const heartbeat = this.#heartbeat;
    const ydoc = this.#ydoc;
    this.#heartbeat = null;
    this.#ydoc = null;
    await Promise.allSettled([
      heartbeat?.stop() ?? Promise.resolve(),
      ydoc?.destroy() ?? Promise.resolve(),
    ]);
  }
}
