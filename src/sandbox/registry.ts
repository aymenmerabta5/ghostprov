import type { RunHandle } from "../relay/run-handle";
import type { ProvisionedSandbox } from "../prism/protocol";
import type { ProjectId, SandboxFailureSnapshot } from "./types";
import type { V2TransportSnapshot } from "../v2/transport/session";
import type { RateLimitHint, RateLimitProblem } from "../v2/rate-limit";

export interface UpstreamRateLimitObservation {
  hint: RateLimitHint;
  problem: RateLimitProblem | null;
  requestedModel: string | null;
  observedAgoMs: number;
}

export interface RoutableSandboxSnapshot {
  projectId: ProjectId;
  /** Exact verified V2 release; null until this generation proves readiness. */
  protocolRevision?: "r1" | "r2" | null;
  buildHash?: string | null;
  schemaHash?: string | null;
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
  /** Latest upstream evidence only; it never changes routing or admission. */
  upstreamRateLimit?: UpstreamRateLimitObservation | null;
  upstreamRateLimits?: Readonly<Record<string, UpstreamRateLimitObservation>>;
  recovery?: {
    attempt: number;
    consecutiveFailures: number;
    retryInMs: number | null;
  };
}

export interface SandboxRunOptions {
  /**
   * Called exactly once after the runtime has charged its own admission
   * credit for this request. It is never called for pre-admission failure.
   */
  onAdmissionReflected?(): void;
}

export interface RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  start(): Promise<void>;
  drain(reason: string): Promise<void>;
  stop(): Promise<void>;
  snapshot(): RoutableSandboxSnapshot;
  run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle>;
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
