import type { RunHandle } from "../relay/run-handle";
import type { ProjectId, SandboxFailureSnapshot } from "./types";

export interface RoutableSandboxSnapshot {
  projectId: ProjectId;
  state: string;
  capacity: number;
  active: number;
  available: number;
  quarantined: number;
  heartbeatAgeMs: number | null;
  credentialExpiresInMs: number | null;
  ackEwmaMs: number;
  completionEwmaMs: number;
  recentFailureRate: number;
  queueDepth: number;
  memoryPressure: number;
  failure: SandboxFailureSnapshot | null;
}

export interface RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  start(): Promise<void>;
  stop(): Promise<void>;
  snapshot(): RoutableSandboxSnapshot;
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle>;
}

export class SandboxRegistry {
  readonly #runtimes = new Map<ProjectId, RoutableSandboxRuntime>();

  constructor(runtimes: Iterable<RoutableSandboxRuntime>) {
    for (const runtime of runtimes) {
      if (this.#runtimes.has(runtime.projectId)) throw new Error(`Duplicate sandbox project ${runtime.projectId}`);
      this.#runtimes.set(runtime.projectId, runtime);
    }
  }

  async startAll(): Promise<void> {
    await Promise.allSettled([...this.#runtimes.values()].map(runtime => runtime.start()));
  }

  async stopAll(): Promise<void> {
    await Promise.allSettled([...this.#runtimes.values()].map(runtime => runtime.stop()));
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
}
