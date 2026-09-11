import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "../core/ports";
import type { RunHandle } from "../relay/run-handle";
import type { ProvisionedSandbox } from "../prism/protocol";
import type {
  FleetRecoveryCoordinator,
  FleetRecoveryLease,
} from "../prism/recovery-coordinator";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
  SandboxRunOptions,
} from "./registry";
import { describeSandboxFailure } from "./failure";
import { hasFreshExactSidecarLiveness, isActiveSandboxLivenessRecovery } from "./liveness";
import type { ProjectId, SandboxFailureSnapshot } from "./types";
import { V2PreCommitError } from "../v2/transport/run-handle";

const DEFAULT_CHECK_INTERVAL_MS = 1_000;
// Three ten-second heartbeat failures may each consume the five-second
// request deadline. Keep the silent-age fallback behind that explicit streak
// so one slow heartbeat never churns a healthy sandbox generation.
const DEFAULT_HEARTBEAT_GRACE_MS = 40_000;
const DEFAULT_STARTUP_TIMEOUT_MS = 120_000;
const DEFAULT_INITIAL_BACKOFF_MS = 1_000;
const DEFAULT_MAX_BACKOFF_MS = 60_000;

const IMMEDIATELY_UNHEALTHY_STATES = new Set([
  "auth_required",
  "draining",
  "failed",
  "stopped",
]);

export type SandboxSupervisorPhase =
  | "idle"
  | "starting"
  | "healthy"
  | "recovering"
  | "draining"
  | "stopped";

export interface SandboxRecoverySnapshot {
  reason: string;
  attempt: number;
  consecutiveFailures: number;
  nextRetryAtMs: number | null;
  lastFailure: SandboxFailureSnapshot | null;
}

/**
 * Extra supervisor state which is deliberately kept out of the registry's
 * RoutableSandboxSnapshot contract.
 */
export interface SupervisedSandboxSnapshot {
  projectId: ProjectId;
  intendedLive: boolean;
  phase: SandboxSupervisorPhase;
  childGeneration: number;
  runtime: RoutableSandboxSnapshot;
  recovery?: SandboxRecoverySnapshot;
}

export interface SandboxSupervisorEvent {
  type: "started" | "retry_scheduled" | "completed" | "aborted";
  projectId: ProjectId;
  workerIndex?: number;
  transport?: "v2";
  reason: string;
  attempt: number;
  incarnation: number;
  /** Added by the system factory from the durable manifest generation. */
  leaderGeneration?: number;
  consecutiveFailures: number;
  retryDelayMs?: number;
  /** Wall time spent in the attempt represented by this event, excluding backoff. */
  attemptDurationMs: number;
  /** Wall time since this recovery lifecycle began, including retries and backoff. */
  totalDurationMs: number;
  failure: SandboxFailureSnapshot | null;
}

export interface SupervisedSandboxRuntimeOptions {
  /** Stable registry identity. Every child returned by createChild must match it. */
  projectId: ProjectId;
  /**
   * Creates a fresh runtime generation. It is called for the first generation
   * when initialChild is absent, and for every recovery generation thereafter.
   */
  createChild(seed?: ProvisionedSandbox): RoutableSandboxRuntime | Promise<RoutableSandboxRuntime>;
  /** Optional already-constructed first generation. It is never reused. */
  initialChild?: RoutableSandboxRuntime;
  clock?: Clock;
  random?: Pick<RandomSource, "next">;
  checkIntervalMs?: number;
  heartbeatGraceMs?: number;
  startupTimeoutMs?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  /**
   * Optional shared provisioning limiter. The returned release function is
   * held only while a child is created and its start() call is in flight.
   */
  acquirePermit?(signal: AbortSignal): Promise<() => void>;
  recoveryCoordinator?: FleetRecoveryCoordinator;
  workerIndex?: number;
  transport?: "v2";
  onRecoveryEvent?(event: SandboxSupervisorEvent): void;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T | PromiseLike<T>): void;
  reject(reason?: unknown): void;
}

interface PendingRecovery {
  reason: string;
  useCurrentChild: boolean;
  sourceChild: RoutableSandboxRuntime | null;
  failure: unknown;
  firstAttempt: Deferred<void> | null;
}

interface Retirement {
  promise: Promise<void>;
}

class ChildStartupTimeoutError extends Error {
  readonly name = "ChildStartupTimeoutError";

  constructor(timeoutMs: number) {
    super(`Sandbox child did not start within ${timeoutMs}ms`);
  }
}

class UnhealthyChildError extends Error {
  readonly name = "UnhealthyChildError";

  constructor(readonly reason: string) {
    super(`Sandbox child is unhealthy: ${reason}`);
  }
}

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function abortError(): DOMException {
  return new DOMException("Sandbox supervision aborted", "AbortError");
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function normalizedRandom(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : 0;
}

/** Exponential backoff with up to 20% positive jitter and a hard maximum. */
export function sandboxRecoveryBackoffMs(
  consecutiveFailures: number,
  initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS,
  maxBackoffMs = DEFAULT_MAX_BACKOFF_MS,
  random: () => number = Math.random,
): number {
  const initial = positiveInteger(initialBackoffMs, "initialBackoffMs");
  const maximum = positiveInteger(maxBackoffMs, "maxBackoffMs");
  if (maximum < initial) throw new RangeError("maxBackoffMs must be at least initialBackoffMs");
  const exponent = Math.max(0, Math.min(30, Math.trunc(consecutiveFailures) - 1));
  const base = Math.min(maximum, initial * (2 ** exponent));
  return Math.min(maximum, Math.round(base * (1 + normalizedRandom(random()) * 0.2)));
}

function emptySnapshot(projectId: ProjectId): RoutableSandboxSnapshot {
  return {
    projectId,
    releaseRevision: null,
    protocolRevision: null,
    transportProfile: null,
    buildHash: null,
    schemaHash: null,
    state: "configured",
    capacity: 0,
    active: 0,
    available: 0,
    quarantined: 0,
    heartbeatAgeMs: null,
    credentialExpiresInMs: null,
    ackEwmaMs: 0,
    completionEwmaMs: 0,
    recentFailureRate: 0,
    queueDepth: 0,
    memoryPressure: 0,
    failure: null,
  };
}

/**
 * Keeps one stable registry entry backed by disposable runtime generations.
 * A failed generation is stopped and discarded; recovery always provisions a
 * fresh child through createChild().
 */
export class SupervisedSandboxRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly #createChild: SupervisedSandboxRuntimeOptions["createChild"];
  readonly #acquirePermit?: SupervisedSandboxRuntimeOptions["acquirePermit"];
  readonly #recoveryCoordinator?: FleetRecoveryCoordinator;
  readonly #workerIndex?: number;
  readonly #transportMode?: "v2";
  readonly #onRecoveryEvent?: (event: SandboxSupervisorEvent) => void;
  readonly #clock: Clock;
  readonly #random: Pick<RandomSource, "next">;
  readonly #checkIntervalMs: number;
  readonly #heartbeatGraceMs: number;
  readonly #startupTimeoutMs: number;
  readonly #initialBackoffMs: number;
  readonly #maxBackoffMs: number;
  readonly #listeners = new Set<() => void>();
  readonly #retirements = new Map<RoutableSandboxRuntime, Retirement>();

  #child: RoutableSandboxRuntime | null = null;
  #childUnsubscribe: (() => void) | null = null;
  #childGeneration = 0;
  #lastSnapshot: RoutableSandboxSnapshot;
  #observedState: string | null = null;
  #stateSinceMs = 0;
  #heartbeatMissingSinceMs: number | null = null;

  #phase: SandboxSupervisorPhase = "idle";
  #intendedLive = false;
  #permanentlyStopped = false;
  #session: AbortController | null = null;
  #monitorPromise: Promise<void> | null = null;
  #recoveryPromise: Promise<void> | null = null;
  #pendingRecovery: PendingRecovery | null = null;
  #startPromise: Promise<void> | null = null;
  #shutdownPromise: Promise<void> | null = null;
  #recovery: SandboxRecoverySnapshot | null = null;
  #recoveryStartedAtMs: number | null = null;
  #recoveryAttemptStartedAtMs: number | null = null;
  #lastRecoveryAttemptDurationMs = 0;

  constructor(options: SupervisedSandboxRuntimeOptions) {
    this.projectId = options.projectId;
    this.#createChild = options.createChild;
    this.#acquirePermit = options.acquirePermit;
    this.#recoveryCoordinator = options.recoveryCoordinator;
    this.#workerIndex = options.workerIndex;
    this.#transportMode = options.transport;
    this.#onRecoveryEvent = options.onRecoveryEvent;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#checkIntervalMs = positiveInteger(
      options.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS,
      "checkIntervalMs",
    );
    this.#heartbeatGraceMs = positiveInteger(
      options.heartbeatGraceMs ?? DEFAULT_HEARTBEAT_GRACE_MS,
      "heartbeatGraceMs",
    );
    this.#startupTimeoutMs = positiveInteger(
      options.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS,
      "startupTimeoutMs",
    );
    this.#initialBackoffMs = positiveInteger(
      options.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
      "initialBackoffMs",
    );
    this.#maxBackoffMs = positiveInteger(
      options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
      "maxBackoffMs",
    );
    if (this.#maxBackoffMs < this.#initialBackoffMs) {
      throw new RangeError("maxBackoffMs must be at least initialBackoffMs");
    }
    this.#lastSnapshot = emptySnapshot(this.projectId);
    if (options.initialChild) this.#attachChild(options.initialChild);
  }

  subscribe(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  start(): Promise<void> {
    if (this.#permanentlyStopped) {
      return Promise.reject(new Error(`Sandbox supervisor ${this.projectId} has been stopped`));
    }
    if (this.#intendedLive) return this.#startPromise ?? Promise.resolve();

    this.#intendedLive = true;
    this.#phase = "starting";
    const session = new AbortController();
    this.#session = session;
    const firstAttempt = deferred<void>();
    this.#startPromise = firstAttempt.promise;
    void firstAttempt.promise.finally(() => {
      if (this.#startPromise === firstAttempt.promise) this.#startPromise = null;
    }).catch(() => undefined);
    this.#pendingRecovery = {
      reason: "start",
      useCurrentChild: this.#child !== null,
      sourceChild: this.#child,
      failure: this.#lastSnapshot.failure,
      firstAttempt,
    };
    this.#recoveryStartedAtMs = this.#clock.now();
    this.#recoveryAttemptStartedAtMs = null;
    this.#lastRecoveryAttemptDurationMs = 0;
    this.#notifyRecovery("started", "start", 0, 0, this.#lastSnapshot.failure);
    this.#emit();
    this.#startMonitor(session);
    this.#pumpRecovery();
    return firstAttempt.promise;
  }

  drain(reason: string): Promise<void> {
    return this.#beginShutdown("drain", reason);
  }

  stop(): Promise<void> {
    return this.#beginShutdown("stop", "supervisor-stop");
  }

  run(
    body: Record<string, unknown>,
    signal?: AbortSignal,
    options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    if (!this.#intendedLive) return Promise.reject(new V2PreCommitError(
      `Sandbox ${this.projectId} supervisor is not live`,
      "supervisor_not_live",
    ));
    if (this.#phase === "recovering") return Promise.reject(new V2PreCommitError(
      `Sandbox ${this.projectId} is recovering`,
      "supervisor_recovering",
    ));
    const child = this.#child;
    if (!child) return Promise.reject(new V2PreCommitError(
      `Sandbox ${this.projectId} has no active child`,
      "supervisor_no_child",
    ));
    // Do not catch or reclassify anything after this boundary: child.run may
    // have published transport state, so only its own commitment proof can
    // decide whether alternate-runtime failover is safe.
    return child.run(body, signal, options);
  }

  snapshot(): RoutableSandboxSnapshot {
    const child = this.#child;
    if (child && this.#phase !== "recovering" && this.#phase !== "draining" && this.#phase !== "stopped") {
      try {
        const value = child.snapshot();
        this.#assertProject(value.projectId);
        this.#lastSnapshot = value;
        return { ...value, incarnation: this.#childGeneration };
      } catch (error) {
        this.#requestRecovery("snapshot-failed");
        this.#lastSnapshot = {
          ...this.#lastSnapshot,
          failure: describeSandboxFailure(error),
        };
      }
    }

    const state = this.#phase === "draining"
      ? "draining"
      : this.#phase === "stopped"
        ? "stopped"
        : this.#phase === "recovering"
          ? "recovering"
          : this.#lastSnapshot.state;
    const hasCurrentRelease = this.#phase !== "recovering" && this.#phase !== "stopped";
    return {
      ...this.#lastSnapshot,
      projectId: this.projectId,
      incarnation: this.#childGeneration,
      ...(!hasCurrentRelease ? {
        releaseRevision: null,
        protocolRevision: null,
        transportProfile: null,
        buildHash: null,
        schemaHash: null,
      } : {}),
      state,
      capacity: 0,
      available: 0,
      livenessRecovery: null,
      ...(this.#recovery ? {
        recovery: {
          attempt: this.#recovery.attempt,
          consecutiveFailures: this.#recovery.consecutiveFailures,
          retryInMs: this.#recovery.nextRetryAtMs === null
            ? null
            : Math.max(0, this.#recovery.nextRetryAtMs - this.#clock.now()),
        },
      } : {}),
    };
  }

  supervisorSnapshot(): SupervisedSandboxSnapshot {
    const value: SupervisedSandboxSnapshot = {
      projectId: this.projectId,
      intendedLive: this.#intendedLive,
      phase: this.#phase,
      childGeneration: this.#childGeneration,
      runtime: this.snapshot(),
    };
    if (this.#recovery) value.recovery = { ...this.#recovery };
    return value;
  }

  #startMonitor(session: AbortController): void {
    const monitor = this.#monitor(session.signal);
    this.#monitorPromise = monitor;
    void monitor.finally(() => {
      if (this.#monitorPromise === monitor) this.#monitorPromise = null;
    }).catch(() => undefined);
  }

  async #monitor(signal: AbortSignal): Promise<void> {
    while (!signal.aborted) {
      try {
        await this.#clock.sleep(this.#checkIntervalMs, signal);
      } catch (error) {
        if (signal.aborted) return;
        void error;
        continue;
      }
      if (signal.aborted || !this.#intendedLive) return;
      this.#inspectChild();
    }
  }

  #inspectChild(): void {
    const child = this.#child;
    if (!child || !this.#intendedLive) return;
    let snapshot: RoutableSandboxSnapshot;
    try {
      snapshot = this.#observeChild(child);
    } catch (error) {
      this.#lastSnapshot = {
        ...this.#lastSnapshot,
        failure: describeSandboxFailure(error),
      };
      this.#requestRecovery("snapshot-failed");
      return;
    }
    const problem = this.#childProblem(snapshot, false);
    if (problem) this.#requestRecovery(problem);
  }

  #requestRecovery(reason: string): void {
    if (!this.#intendedLive || this.#permanentlyStopped) return;
    if (!this.#pendingRecovery) {
      this.#pendingRecovery = {
        reason,
        useCurrentChild: false,
        sourceChild: this.#child,
        failure: this.#lastSnapshot.failure,
        firstAttempt: null,
      };
    }
    this.#phase = "recovering";
    if (!this.#recovery) {
      this.#recoveryStartedAtMs = this.#clock.now();
      this.#recoveryAttemptStartedAtMs = null;
      this.#lastRecoveryAttemptDurationMs = 0;
      this.#recovery = {
        reason,
        attempt: 0,
        consecutiveFailures: 0,
        nextRetryAtMs: null,
        lastFailure: null,
      };
      this.#notifyRecovery("started", reason, 0, 0, this.#lastSnapshot.failure);
    }
    this.#emit();
    this.#pumpRecovery();
  }

  #pumpRecovery(): void {
    if (
      !this.#intendedLive ||
      this.#permanentlyStopped ||
      this.#recoveryPromise ||
      !this.#pendingRecovery ||
      !this.#session
    ) return;
    const pending = this.#pendingRecovery;
    this.#pendingRecovery = null;
    const recovery = this.#recoverForever(pending, this.#session.signal);
    this.#recoveryPromise = recovery;
    void recovery.finally(() => {
      if (this.#recoveryPromise === recovery) this.#recoveryPromise = null;
      if (!this.#intendedLive || this.#permanentlyStopped) return;
      this.#inspectChild();
      this.#pumpRecovery();
    }).catch(() => undefined);
  }

  async #recoverForever(pending: PendingRecovery, signal: AbortSignal): Promise<void> {
    let failures = 0;
    let attempt = 0;
    let useCurrentChild = pending.useCurrentChild;
    let firstAttempt = pending.firstAttempt;
    let replacementSandbox: ProvisionedSandbox | null = null;
    let attemptFailure: unknown = pending.failure;

    while (this.#isActive(signal)) {
      if (failures > 0) {
        const delay = sandboxRecoveryBackoffMs(
          failures,
          this.#initialBackoffMs,
          this.#maxBackoffMs,
          () => this.#random.next(),
        );
        this.#recovery = {
          reason: pending.reason,
          attempt,
          consecutiveFailures: failures,
          nextRetryAtMs: this.#clock.now() + delay,
          lastFailure: this.#recovery?.lastFailure ?? null,
        };
        this.#notifyRecovery(
          "retry_scheduled",
          pending.reason,
          attempt,
          failures,
          this.#recovery.lastFailure,
          delay,
        );
        this.#emit();
        try {
          await this.#clock.sleep(delay, signal);
        } catch (error) {
          if (signal.aborted) break;
          void error;
        }
        if (!this.#isActive(signal)) break;
      }

      attempt++;
      this.#recoveryAttemptStartedAtMs = this.#clock.now();
      this.#phase = pending.reason === "start" && attempt === 1
        ? "starting"
        : "recovering";
      this.#recovery = {
        reason: pending.reason,
        attempt,
        consecutiveFailures: failures,
        nextRetryAtMs: null,
        lastFailure: this.#recovery?.lastFailure ?? null,
      };
      this.#emit();

      let child: RoutableSandboxRuntime | null = null;
      let circuitLease: FleetRecoveryLease | null = null;
      try {
        if (!useCurrentChild) {
          replacementSandbox ??= this.#child?.takeReplacementSandbox?.() ?? null;
        }
        if (this.#recoveryCoordinator) {
          circuitLease = await this.#recoveryCoordinator.enter({
            projectId: this.projectId,
            reason: pending.reason,
            failure: replacementSandbox ? { status: 409 } : attemptFailure,
          }, signal);
        }
        if (!this.#isActive(signal)) throw abortError();
        if (useCurrentChild) {
          child = this.#child;
          useCurrentChild = false;
        } else {
          await this.#retireCurrent(signal);
        }
        if (!this.#isActive(signal)) throw abortError();

        let releasePermit: (() => void) | null = null;
        try {
          releasePermit = await this.#takePermit(signal);
          if (!this.#isActive(signal)) throw abortError();
          if (!child) {
            child = await this.#makeChild(signal, replacementSandbox);
            replacementSandbox = null;
            if (!this.#isActive(signal)) throw abortError();
            this.#attachChild(child);
          }
          try {
            await this.#startChild(child, signal);
          } catch (error) {
            // Keep the shared provisioning permit until the timed-out/failed
            // generation has been asked to stop. This prevents hung starts
            // from accumulating beyond the fleet recovery concurrency bound.
            replacementSandbox ??= child.takeReplacementSandbox?.() ?? null;
            await this.#retireChild(child, signal);
            child = null;
            throw error;
          }
        } finally {
          releasePermit?.();
        }

        if (!this.#isActive(signal)) throw abortError();
        await this.#waitForHealthyHeartbeat(child, signal);
        if (!this.#isActive(signal)) throw abortError();
        const snapshot = this.#observeChild(child);
        const problem = this.#childProblem(snapshot, true);
        if (problem) throw new UnhealthyChildError(problem);
        if (this.#pendingRecovery) {
          const queued = this.#pendingRecovery;
          this.#pendingRecovery = null;
          if (queued.sourceChild === child || queued.sourceChild === null) {
            throw new UnhealthyChildError(queued.reason);
          }
        }

        this.#phase = "healthy";
        this.#notifyRecovery(
          "completed",
          pending.reason,
          attempt,
          failures,
          null,
        );
        this.#recoveryAttemptStartedAtMs = null;
        this.#lastRecoveryAttemptDurationMs = 0;
        this.#recoveryStartedAtMs = null;
        this.#recovery = null;
        circuitLease?.succeed();
        circuitLease = null;
        firstAttempt?.resolve();
        firstAttempt = null;
        this.#emit();
        return;
      } catch (error) {
        if (this.#recoveryAttemptStartedAtMs !== null) {
          this.#lastRecoveryAttemptDurationMs = Math.max(
            0,
            this.#clock.now() - this.#recoveryAttemptStartedAtMs,
          );
          this.#recoveryAttemptStartedAtMs = null;
        }
        if (circuitLease) {
          if (this.#isActive(signal)) circuitLease.fail(error);
          else circuitLease.cancel();
          circuitLease = null;
        }
        firstAttempt?.reject(error);
        firstAttempt = null;
        if (!this.#isActive(signal)) {
          if (child) await this.#retireChild(child);
          break;
        }
        failures++;
        attemptFailure = error;
        this.#phase = "recovering";
        this.#recovery = {
          reason: error instanceof UnhealthyChildError ? error.reason : pending.reason,
          attempt,
          consecutiveFailures: failures,
          nextRetryAtMs: null,
          lastFailure: describeSandboxFailure(error),
        };
        this.#emit();
        if (child) {
          replacementSandbox ??= child.takeReplacementSandbox?.() ?? null;
          await this.#retireChild(child, signal);
        }
      }
    }

    firstAttempt?.reject(abortError());
  }

  async #takePermit(signal: AbortSignal): Promise<() => void> {
    if (!this.#acquirePermit) return () => {};
    const source = Promise.resolve().then(() => this.#acquirePermit!(signal));
    return this.#abortable(source, signal, release => release());
  }

  async #makeChild(
    signal: AbortSignal,
    seed: ProvisionedSandbox | null,
  ): Promise<RoutableSandboxRuntime> {
    const source = Promise.resolve().then(() => this.#createChild(seed ?? undefined));
    const timeoutController = new AbortController();
    const timeout = this.#clock.sleep(this.#startupTimeoutMs, timeoutController.signal)
      .then(() => ({ type: "timeout" as const }));
    const outcome = Promise.race([
      source.then(child => ({ type: "child" as const, child })),
      timeout,
    ]);
    let selected: RoutableSandboxRuntime;
    try {
      const value = await this.#abortable(outcome, signal, late => {
        if (late.type === "child") {
          void this.#retireChild(late.child).catch(() => undefined);
        }
      });
      if (value.type === "timeout") {
        void source.then(
          lateChild => this.#retireChild(lateChild).catch(() => undefined),
          () => undefined,
        );
        throw new ChildStartupTimeoutError(this.#startupTimeoutMs);
      }
      selected = value.child;
    } catch (error) {
      if (signal.aborted) {
        void source.then(
          lateChild => this.#retireChild(lateChild).catch(() => undefined),
          () => undefined,
        );
      }
      throw error;
    } finally {
      timeoutController.abort();
    }
    const child = selected;
    try {
      this.#assertProject(child.projectId);
      return child;
    } catch (error) {
      await this.#retireChild(child);
      throw error;
    }
  }

  async #startChild(child: RoutableSandboxRuntime, signal: AbortSignal): Promise<void> {
    if (signal.aborted) throw abortError();
    const timeoutController = new AbortController();
    const onAbort = () => timeoutController.abort();
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) timeoutController.abort();
    const starting = Promise.resolve().then(() => child.start());
    const timeout = this.#clock.sleep(this.#startupTimeoutMs, timeoutController.signal)
      .then(() => { throw new ChildStartupTimeoutError(this.#startupTimeoutMs); });
    try {
      await Promise.race([starting, timeout]);
    } finally {
      signal.removeEventListener("abort", onAbort);
      timeoutController.abort();
    }
  }

  #attachChild(child: RoutableSandboxRuntime): void {
    this.#assertProject(child.projectId);
    if (this.#child && this.#child !== child) {
      throw new Error(`Sandbox supervisor ${this.projectId} already has a child`);
    }
    this.#child = child;
    this.#childGeneration++;
    this.#observedState = null;
    this.#stateSinceMs = this.#clock.now();
    this.#heartbeatMissingSinceMs = null;
    this.#observeChild(child);
    const generation = this.#childGeneration;
    this.#childUnsubscribe = child.subscribe?.(() => {
      if (this.#child !== child || this.#childGeneration !== generation) return;
      try {
        const snapshot = this.#observeChild(child);
        this.#emit();
        const problem = this.#childProblem(snapshot, false);
        if (problem) this.#requestRecovery(problem);
      } catch (error) {
        this.#lastSnapshot = {
          ...this.#lastSnapshot,
          failure: describeSandboxFailure(error),
        };
        this.#requestRecovery("snapshot-failed");
      }
    }) ?? null;
    this.#emit();
  }

  #detachChild(child: RoutableSandboxRuntime): void {
    if (this.#child !== child) return;
    this.#childUnsubscribe?.();
    this.#childUnsubscribe = null;
    try {
      this.#lastSnapshot = child.snapshot();
    } catch {
      // Preserve the last safe snapshot.
    }
    this.#child = null;
    this.#observedState = null;
    this.#heartbeatMissingSinceMs = null;
    this.#emit();
  }

  async #retireCurrent(signal?: AbortSignal): Promise<void> {
    const child = this.#child;
    if (child) await this.#retireChild(child, signal);
  }

  async #retireChild(child: RoutableSandboxRuntime, signal?: AbortSignal): Promise<void> {
    this.#detachChild(child);
    let retirement = this.#retirements.get(child);
    if (!retirement) {
      const timeoutController = new AbortController();
      const stopping = Promise.resolve()
        .then(() => child.stop())
        .catch(() => undefined);
      const timeout = this.#clock.sleep(this.#startupTimeoutMs, timeoutController.signal)
        .catch(() => undefined);
      const promise = Promise.race([stopping, timeout])
        .finally(() => {
          timeoutController.abort();
          this.#retirements.delete(child);
        });
      retirement = { promise };
      this.#retirements.set(child, retirement);
    }
    if (!signal) {
      await retirement.promise;
      return;
    }
    await this.#abortable(retirement.promise, signal);
  }

  #observeChild(child: RoutableSandboxRuntime): RoutableSandboxSnapshot {
    const snapshot = child.snapshot();
    this.#assertProject(snapshot.projectId);
    this.#lastSnapshot = snapshot;
    const now = this.#clock.now();
    if (snapshot.state !== this.#observedState) {
      this.#observedState = snapshot.state;
      this.#stateSinceMs = now;
      this.#heartbeatMissingSinceMs = null;
    }
    if (snapshot.state === "ready" && snapshot.heartbeatAgeMs === null) {
      this.#heartbeatMissingSinceMs ??= now;
    } else {
      this.#heartbeatMissingSinceMs = null;
    }
    return snapshot;
  }

  #childProblem(snapshot: RoutableSandboxSnapshot, afterStart: boolean): string | null {
    if (IMMEDIATELY_UNHEALTHY_STATES.has(snapshot.state)) return `state:${snapshot.state}`;
    if (snapshot.credentialExpiresInMs !== null && snapshot.credentialExpiresInMs < 30_000) {
      return "credentials-expiring";
    }
    if (snapshot.state === "ready") {
      // The child owns recovery until its fixed deadline. It remains unroutable;
      // a stale heartbeat must not retire it while that exact owner reconnects.
      if (snapshot.capacity === 0 && snapshot.available === 0 &&
        isActiveSandboxLivenessRecovery(snapshot.livenessRecovery, this.#clock.now())) return null;
      const exactSidecarFresh = hasFreshExactSidecarLiveness(snapshot);
      if (
        snapshot.heartbeatAgeMs !== null &&
        snapshot.heartbeatAgeMs >= this.#heartbeatGraceMs &&
        !exactSidecarFresh
      ) return "heartbeat-stale";
      if (
        snapshot.heartbeatAgeMs === null &&
        this.#heartbeatMissingSinceMs !== null &&
        this.#clock.now() - this.#heartbeatMissingSinceMs >= this.#heartbeatGraceMs &&
        !exactSidecarFresh
      ) return "heartbeat-missing";
      return null;
    }
    if (afterStart) return `start-returned:${snapshot.state}`;
    if (this.#clock.now() - this.#stateSinceMs >= this.#startupTimeoutMs) {
      return `state-stuck:${snapshot.state}`;
    }
    return null;
  }

  async #waitForHealthyHeartbeat(
    child: RoutableSandboxRuntime,
    signal: AbortSignal,
  ): Promise<void> {
    const deadline = this.#clock.now() + this.#heartbeatGraceMs;
    for (;;) {
      if (!this.#isActive(signal)) throw abortError();
      const snapshot = this.#observeChild(child);
      if (snapshot.state !== "ready") {
        throw new UnhealthyChildError(`start-returned:${snapshot.state}`);
      }
      if (
        snapshot.heartbeatAgeMs !== null ||
        hasFreshExactSidecarLiveness(snapshot)
      ) {
        const problem = this.#childProblem(snapshot, true);
        if (problem) throw new UnhealthyChildError(problem);
        return;
      }
      const now = this.#clock.now();
      if (now >= deadline) throw new UnhealthyChildError("heartbeat-missing");
      await this.#clock.sleep(
        Math.min(this.#checkIntervalMs, deadline - now),
        signal,
      );
    }
  }

  #beginShutdown(kind: "drain" | "stop", reason: string): Promise<void> {
    if (this.#shutdownPromise) return this.#shutdownPromise;
    if (this.#phase === "stopped" && this.#permanentlyStopped) return Promise.resolve();
    this.#permanentlyStopped = true;
    this.#intendedLive = false;
    this.#phase = kind === "drain" ? "draining" : "stopped";
    this.#pendingRecovery = null;
    if (this.#recoveryStartedAtMs !== null) {
      this.#notifyRecovery(
        "aborted",
        reason,
        this.#recovery?.attempt ?? 0,
        this.#recovery?.consecutiveFailures ?? 0,
        this.#recovery?.lastFailure ?? null,
      );
      this.#recoveryStartedAtMs = null;
      this.#recoveryAttemptStartedAtMs = null;
      this.#lastRecoveryAttemptDurationMs = 0;
    }
    this.#recovery = null;
    this.#session?.abort();
    this.#emit();

    const shutdown = this.#shutdown(kind, reason);
    this.#shutdownPromise = shutdown;
    void shutdown.finally(() => {
      if (this.#shutdownPromise === shutdown) this.#shutdownPromise = null;
    }).catch(() => undefined);
    return shutdown;
  }

  async #shutdown(kind: "drain" | "stop", reason: string): Promise<void> {
    const child = this.#child;
    if (child) {
      let graceful = kind === "drain";
      if (graceful) {
        try {
          graceful = this.#childProblem(this.#observeChild(child), false) === null;
        } catch {
          graceful = false;
        }
      }
      this.#detachChild(child);
      if (!this.#retirements.has(child)) {
        const promise = this.#settleForShutdown(child, graceful, reason)
          .finally(() => this.#retirements.delete(child));
        this.#retirements.set(child, { promise });
      }
    }

    const activeRetirements = [...this.#retirements.values()].map(value => value.promise);
    await Promise.allSettled(activeRetirements);
    await Promise.allSettled([
      this.#monitorPromise ?? Promise.resolve(),
      this.#recoveryPromise ?? Promise.resolve(),
    ]);
    this.#phase = "stopped";
    this.#recovery = null;
    this.#emit();
  }

  #isActive(signal: AbortSignal): boolean {
    return !signal.aborted && this.#intendedLive && !this.#permanentlyStopped;
  }

  async #settleForShutdown(
    child: RoutableSandboxRuntime,
    graceful: boolean,
    reason: string,
  ): Promise<void> {
    const timeoutMs = Math.min(this.#startupTimeoutMs, 30_000);
    const runBounded = async (operation: Promise<void>): Promise<boolean> => {
      const timeoutController = new AbortController();
      try {
        return await Promise.race([
          operation.then(() => true, () => false),
          this.#clock.sleep(timeoutMs, timeoutController.signal).then(() => false),
        ]);
      } finally {
        timeoutController.abort();
      }
    };
    const completed = await runBounded(Promise.resolve().then(() => (
      graceful ? child.drain(reason) : child.stop()
    )));
    if (!completed && graceful) {
      await runBounded(Promise.resolve().then(() => child.stop()));
    }
  }

  #assertProject(value: ProjectId): void {
    if (value !== this.projectId) {
      throw new Error(`Sandbox child project ${value} does not match supervisor ${this.projectId}`);
    }
  }

  #abortable<T>(
    source: Promise<T>,
    signal: AbortSignal,
    onLateValue?: (value: T) => void,
  ): Promise<T> {
    if (signal.aborted) {
      void source.then(value => onLateValue?.(value), () => undefined);
      return Promise.reject(abortError());
    }
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const onAbort = () => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", onAbort);
        reject(abortError());
      };
      signal.addEventListener("abort", onAbort, { once: true });
      source.then(
        value => {
          if (settled) {
            onLateValue?.(value);
            return;
          }
          settled = true;
          signal.removeEventListener("abort", onAbort);
          resolve(value);
        },
        error => {
          if (settled) return;
          settled = true;
          signal.removeEventListener("abort", onAbort);
          reject(error);
        },
      );
    });
  }

  #emit(): void {
    for (const listener of this.#listeners) {
      try { listener(); } catch {}
    }
  }

  #notifyRecovery(
    type: SandboxSupervisorEvent["type"],
    reason: string,
    attempt: number,
    consecutiveFailures: number,
    failure: SandboxFailureSnapshot | null,
    retryDelayMs?: number,
  ): void {
    if (!this.#onRecoveryEvent) return;
    const startedAt = this.#recoveryStartedAtMs ?? this.#clock.now();
    const now = this.#clock.now();
    const attemptDurationMs = this.#recoveryAttemptStartedAtMs === null
      ? this.#lastRecoveryAttemptDurationMs
      : Math.max(0, now - this.#recoveryAttemptStartedAtMs);
    try {
      this.#onRecoveryEvent({
        type,
        projectId: this.projectId,
        workerIndex: this.#workerIndex,
        transport: this.#transportMode,
        reason,
        attempt,
        incarnation: this.#childGeneration,
        consecutiveFailures,
        ...(retryDelayMs === undefined ? {} : { retryDelayMs }),
        attemptDurationMs,
        totalDurationMs: Math.max(0, now - startedAt),
        failure,
      });
    } catch {}
  }
}

interface PermitWaiter {
  signal?: AbortSignal;
  resolve(release: () => void): void;
  reject(error: unknown): void;
  onAbort?: () => void;
}

/** Small FIFO limiter suitable for sharing provisioning capacity across supervisors. */
export class RecoveryPermitPool {
  readonly #limit: number;
  readonly #waiters: PermitWaiter[] = [];
  #inUse = 0;

  constructor(limit: number) {
    this.#limit = positiveInteger(limit, "limit");
  }

  acquire(signal?: AbortSignal): Promise<() => void> {
    if (signal?.aborted) return Promise.reject(abortError());
    return new Promise<() => void>((resolve, reject) => {
      const waiter: PermitWaiter = { signal, resolve, reject };
      if (signal) {
        waiter.onAbort = () => {
          const index = this.#waiters.indexOf(waiter);
          if (index >= 0) this.#waiters.splice(index, 1);
          reject(abortError());
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.#waiters.push(waiter);
      this.#dispatch();
    });
  }

  snapshot(): { limit: number; inUse: number; queued: number } {
    return { limit: this.#limit, inUse: this.#inUse, queued: this.#waiters.length };
  }

  #dispatch(): void {
    while (this.#inUse < this.#limit && this.#waiters.length > 0) {
      const waiter = this.#waiters.shift()!;
      if (waiter.signal?.aborted) continue;
      if (waiter.onAbort) waiter.signal?.removeEventListener("abort", waiter.onAbort);
      this.#inUse++;
      let released = false;
      waiter.resolve(() => {
        if (released) return;
        released = true;
        this.#inUse--;
        this.#dispatch();
      });
    }
  }
}
