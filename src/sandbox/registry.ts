import type { RunHandle } from "../relay/run-handle";
import type { ProvisionedSandbox } from "../prism/protocol";
import type { ProjectId, SandboxFailureSnapshot } from "./types";
import type { V2TransportSnapshot } from "../v2/transport/session";

export interface RoutableSandboxSnapshot {
  projectId: ProjectId;
  /** Changes when a stable supervisor installs a fresh runtime generation. */
  incarnation?: number;
  state: string;
  capacity: number;
  active: number;
  available: number;
  quarantined: number;
  ambiguousQuarantined?: number;
  ambiguityLimit?: number;
  heartbeatAgeMs: number | null;
  sidecarPeerAgeMs?: number | null;
  credentialExpiresInMs: number | null;
  ackEwmaMs: number;
  completionEwmaMs: number;
  recentFailureRate: number;
  queueDepth: number;
  memoryPressure: number;
  failure: SandboxFailureSnapshot | null;
  diagnostics?: V2TransportSnapshot["diagnostics"];
  /** Learned only from a canonical upstream rate-limit response on this project. */
  upstreamOrganization?: {
    id: string;
    model: string | null;
    source: "rate_limit";
    observedAgoMs: number;
  } | null;
  recovery?: {
    attempt: number;
    consecutiveFailures: number;
    retryInMs: number | null;
  };
}

export interface RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  start(): Promise<void>;
  drain(reason: string): Promise<void>;
  stop(): Promise<void>;
  snapshot(): RoutableSandboxSnapshot;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
  /** One-shot pre-acquired sandbox for the next same-project generation. */
  takeReplacementSandbox?(): ProvisionedSandbox | null;
  subscribe?(listener: () => void): () => void;
}

export type RegistryEvent =
  | { type: "added"; projectId: ProjectId }
  | { type: "changed"; projectId: ProjectId }
  | { type: "removed"; projectId: ProjectId };

export class SandboxRegistry {
  readonly #runtimes = new Map<ProjectId, RoutableSandboxRuntime>();
  readonly #runtimeUnsubscribes = new Map<ProjectId, () => void>();
  readonly #listeners = new Set<(event: RegistryEvent) => void>();

  constructor(runtimes: Iterable<RoutableSandboxRuntime> = []) {
    for (const runtime of runtimes) this.add(runtime);
  }

  add(runtime: RoutableSandboxRuntime): void {
    if (this.#runtimes.has(runtime.projectId)) {
      throw new Error(`Duplicate sandbox project ${runtime.projectId}`);
    }
    this.#runtimes.set(runtime.projectId, runtime);
    this.#emit({ type: "added", projectId: runtime.projectId });
    if (runtime.subscribe) {
      this.#runtimeUnsubscribes.set(
        runtime.projectId,
        runtime.subscribe(() => this.#emit({
          type: "changed",
          projectId: runtime.projectId,
        })),
      );
    }
  }

  async drain(projectId: ProjectId, reason: string): Promise<void> {
    const runtime = this.#runtimes.get(projectId);
    if (!runtime) return;
    await runtime.drain(reason);
    this.#emit({ type: "changed", projectId });
  }

  async removeStopped(projectId: ProjectId): Promise<void> {
    const runtime = this.#runtimes.get(projectId);
    if (!runtime) return;
    if (runtime.snapshot().state !== "stopped") {
      throw new Error(`Sandbox project ${projectId} must be stopped before removal`);
    }
    this.#runtimeUnsubscribes.get(projectId)?.();
    this.#runtimeUnsubscribes.delete(projectId);
    this.#runtimes.delete(projectId);
    this.#emit({ type: "removed", projectId });
  }

  subscribe(listener: (event: RegistryEvent) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  async startAll(): Promise<void> {
    await Promise.allSettled([...this.#runtimes.values()].map(async runtime => {
      try {
        await runtime.start();
      } finally {
        this.#emit({ type: "changed", projectId: runtime.projectId });
      }
    }));
  }

  async stopAll(): Promise<void> {
    await Promise.allSettled([...this.#runtimes.values()].map(async runtime => {
      try {
        await runtime.stop();
      } finally {
        this.#emit({ type: "changed", projectId: runtime.projectId });
      }
    }));
  }

  get(projectId: ProjectId): RoutableSandboxRuntime | undefined {
    return this.#runtimes.get(projectId);
  }

  runtimes(): readonly RoutableSandboxRuntime[] {
    return [...this.#runtimes.values()];
  }

  snapshots(): RoutableSandboxSnapshot[] {
    return [...this.#runtimes.values()].map(runtime => runtime.snapshot());
  }

  readyRuntimes(): RoutableSandboxRuntime[] {
    return [...this.#runtimes.values()].filter(runtime => runtime.snapshot().state === "ready");
  }

  #emit(event: RegistryEvent): void {
    for (const listener of this.#listeners) listener(event);
  }
}
