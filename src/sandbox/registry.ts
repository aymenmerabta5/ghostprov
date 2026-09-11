import type { RunHandle } from "../relay/run-handle";
import type { ProvisionedSandbox } from "../prism/protocol";
import type { ProjectId, SandboxFailureSnapshot } from "./types";
import type { V2TransportSnapshot } from "../v2/transport/session";
import type { RateLimitHint, RateLimitProblem } from "../v2/rate-limit";
import type { SandboxLivenessRecovery } from "./liveness";

export type SandboxAvailabilityReason =
  | "available"
  | "lifecycle_unavailable"
  | "sidecar_suspect"
  | "credential_refresh"
  | "awareness_reconnect"
  | "transport_unavailable"
  | "transport_recovery"
  | "transport_congested"
  | "zero_remote_credits"
  | "host_ceiling"
  | "execution_window"
  | "transfer_budget"
  | "quarantine";

export interface UpstreamRateLimitObservation {
  hint: RateLimitHint;
  problem: RateLimitProblem | null;
  requestedModel: string | null;
  observedAgoMs: number;
}

export interface RoutableSandboxSnapshot {
  projectId: ProjectId;
  /** Exact verified V2 release; null until this generation proves readiness. */
  releaseRevision?: "r1" | "r2" | "r3" | "r4" | "r5" | "r6" | "r7" | "r8" | "r9" | "r10" | null;
  protocolRevision?: "r1" | "r2" | null;
  transportProfile?: "standard" | "bulk-v1" | null;
  buildHash?: string | null;
  schemaHash?: string | null;
  /** Changes when a stable supervisor installs a fresh runtime generation. */
  incarnation?: number;
  state: string;
  capacity: number;
  active: number;
  available: number;
  /** General four-start execution admission advertised by V2. */
  admissionCredits?: number;
  /** Optional body-only prefetch capacity for eligible medium/bulk requests. */
  prefetchCredits?: number;
  transferCredits?: number;
  /** Sanitized reason this runtime can or cannot accept another request. */
  availabilityReason?: SandboxAvailabilityReason;
  quarantined: number;
  ambiguousQuarantined?: number;
  ambiguityLimit?: number;
  heartbeatAgeMs: number | null;
  sidecarPeerAgeMs?: number | null;
  /** Exact V2 transport connection state used with sidecar-peer liveness proof. */
  transportConnected?: boolean;
  /** Internal ownership of bounded liveness recovery, never a readiness proof. */
  livenessRecovery?: SandboxLivenessRecovery | null;
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
  /**
   * Reports request-body progress which is safe to use only while the request
   * remains before its execution-risk fence. V2 emits this for newly
   * cumulatively acknowledged request_data and one exact request_prepared
   * proof; observer failures never affect transport state.
   */
  onAdmissionProgress?(progress: SandboxAdmissionProgress): void;
  /**
   * Called exactly once when execution can no longer be safely retried on a
   * different runtime. R2 reports this only after request_start enters the
   * cumulative reliable window; admission alone is still pre-execution.
   */
  onExecutionRiskReflected?(): void;
}

/**
 * Monotonic, protocol-proven request-body progress. Byte counts describe the
 * encoded payload actually carried by request_data frames, not JSON input size.
 */
export interface SandboxAdmissionProgress {
  readonly stage: "request_data" | "request_prepared";
  readonly acknowledgedEncodedBytes: number;
  readonly totalEncodedBytes: number;
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
