import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import type { HeartbeatResult } from "./gateway";
import type { ProjectId } from "./types";

export interface HeartbeatGateway {
  heartbeat(signal?: AbortSignal): Promise<HeartbeatResult>;
}

export interface SandboxHeartbeatOptions {
  projectId: ProjectId;
  gateway: HeartbeatGateway;
  clock?: Clock;
  periodMs?: 10_000;
  initialDelayMs?: number;
  onResult?: (result: HeartbeatResult) => void;
  onExpired?: (status: number) => void;
  onTransientFailure?: (status: number | null) => void;
}

export interface HeartbeatSnapshot {
  projectId: ProjectId;
  running: boolean;
  inFlight: boolean;
  attemptCount: number;
  successCount: number;
  failureCount: number;
  lastAttemptAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastStatus: number | null;
}

function defaultInitialDelay(projectId: ProjectId): number {
  let hash = 2166136261;
  for (const character of projectId) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return 1 + (Math.abs(hash) % 10_000);
}

export class SandboxHeartbeat {
  readonly projectId: ProjectId;
  readonly #gateway: HeartbeatGateway;
  readonly #clock: Clock;
  readonly #periodMs: 10_000;
  readonly #initialDelayMs: number;
  readonly #onResult?: (result: HeartbeatResult) => void;
  readonly #onExpired?: (status: number) => void;
  readonly #onTransientFailure?: (status: number | null) => void;
  #controller: AbortController | null = null;
  #loop: Promise<void> | null = null;
  #inFlight = false;
  #attemptCount = 0;
  #successCount = 0;
  #failureCount = 0;
  #lastAttemptAtMs: number | null = null;
  #lastSuccessAtMs: number | null = null;
  #lastStatus: number | null = null;

  constructor(options: SandboxHeartbeatOptions) {
    this.projectId = options.projectId;
    this.#gateway = options.gateway;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#periodMs = options.periodMs ?? 10_000;
    this.#initialDelayMs = options.initialDelayMs ?? defaultInitialDelay(options.projectId);
    if (this.#periodMs !== 10_000) throw new RangeError("Heartbeat period must be exactly 10000ms");
    if (!(this.#initialDelayMs >= 0 && this.#initialDelayMs <= 10_000)) {
      throw new RangeError("Heartbeat initial delay must be between 0 and 10000ms");
    }
    this.#onResult = options.onResult;
    this.#onExpired = options.onExpired;
    this.#onTransientFailure = options.onTransientFailure;
  }

  start(): void {
    if (this.#loop) return;
    this.#controller = new AbortController();
    this.#loop = this.#run(this.#controller.signal).finally(() => {
      this.#loop = null;
      this.#controller = null;
      this.#inFlight = false;
    });
  }

  async stop(): Promise<void> {
    const loop = this.#loop;
    this.#controller?.abort();
    if (loop) await loop;
  }

  snapshot(): HeartbeatSnapshot {
    return {
      projectId: this.projectId,
      running: this.#loop !== null,
      inFlight: this.#inFlight,
      attemptCount: this.#attemptCount,
      successCount: this.#successCount,
      failureCount: this.#failureCount,
      lastAttemptAtMs: this.#lastAttemptAtMs,
      lastSuccessAtMs: this.#lastSuccessAtMs,
      lastStatus: this.#lastStatus,
    };
  }

  async #run(signal: AbortSignal): Promise<void> {
    let deadline = this.#clock.now() + this.#initialDelayMs;
    while (!signal.aborted) {
      try { await this.#clock.waitUntil(deadline, signal); }
      catch (error: any) {
        if (signal.aborted || error?.name === "AbortError") return;
        throw error;
      }
      if (signal.aborted) return;
      this.#inFlight = true;
      this.#attemptCount++;
      this.#lastAttemptAtMs = this.#clock.now();
      try {
        const result = await this.#gateway.heartbeat(signal);
        this.#lastStatus = result.status;
        this.#onResult?.(result);
        if (result.ok) {
          this.#successCount++;
          this.#lastSuccessAtMs = this.#clock.now();
        } else {
          this.#failureCount++;
          if ([401, 403, 404, 409, 410].includes(result.status)) this.#onExpired?.(result.status);
          else this.#onTransientFailure?.(result.status);
        }
      } catch (error: any) {
        if (signal.aborted || error?.name === "AbortError") return;
        this.#failureCount++;
        this.#lastStatus = null;
        this.#onTransientFailure?.(null);
      } finally {
        this.#inFlight = false;
      }
      deadline += this.#periodMs;
      while (deadline <= this.#clock.now()) deadline += this.#periodMs;
    }
  }
}
