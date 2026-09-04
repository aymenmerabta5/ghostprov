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
  requestTimeoutMs?: number;
  consecutiveTransientFailureThreshold?: number;
  onResult?: (result: HeartbeatResult) => void;
  onSuccess?: (result: HeartbeatResult) => void;
  onAuthenticationFailure?: (
    status: number,
    signal: AbortSignal,
  ) => Promise<"retry" | "expired">;
  onExpired?: (status: number) => void;
  onTransientFailure?: (status: number | null) => void;
  onUnreachable?: (status: number | null) => void;
  /** Fixed, content-free attempt telemetry. Routine successes may be ignored by callers. */
  onAttempt?: (diagnostic: SandboxHeartbeatAttemptDiagnostic) => void;
}

export type SandboxHeartbeatAttemptOutcome =
  | "success"
  | "http_error"
  | "transport_error"
  | "timeout";

export type SandboxHeartbeatErrorName =
  | "AccountAuthError"
  | "AccountTransportError"
  | "SandboxGatewayError"
  | "Error";

export interface SandboxHeartbeatAttemptDiagnostic {
  readonly outcome: SandboxHeartbeatAttemptOutcome;
  readonly status: number | null;
  readonly errorName: SandboxHeartbeatErrorName | null;
  readonly durationMs: number;
  readonly attempt: number;
  readonly previousConsecutiveFailures: number;
  readonly consecutiveFailures: number;
}

export interface HeartbeatSnapshot {
  projectId: ProjectId;
  running: boolean;
  inFlight: boolean;
  attemptCount: number;
  successCount: number;
  failureCount: number;
  consecutiveFailures: number;
  lastAttemptAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastFailureAtMs: number | null;
  lastStatus: number | null;
}

type AttemptOutcome =
  | { type: "result"; result: HeartbeatResult }
  | {
      type: "failure";
      kind: "http" | "transport" | "timeout";
      errorName: SandboxHeartbeatErrorName;
      status: number | null;
    }
  | { type: "stopped" };

function heartbeatErrorName(error: unknown): SandboxHeartbeatErrorName {
  let name = "Error";
  try {
    if (error && typeof error === "object" && "name" in error) name = String(error.name);
  } catch {}
  switch (name) {
    case "AccountAuthError":
    case "AccountTransportError":
    case "SandboxGatewayError":
      return name;
    default:
      return "Error";
  }
}

function heartbeatErrorStatus(error: unknown): number | null {
  let status: unknown;
  try {
    if (error && typeof error === "object" && "status" in error) status = error.status;
  } catch {}
  return typeof status === "number" && Number.isInteger(status) && status >= 100 && status <= 599
    ? status
    : null;
}

export function heartbeatPhaseOffset(
  projectId: ProjectId,
  intervalMs = 10_000,
): number {
  if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
    throw new RangeError("Heartbeat phase interval must be a positive safe integer");
  }
  let hash = 2166136261;
  for (const character of projectId) {
    hash ^= character.charCodeAt(0);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % intervalMs;
}

export class SandboxHeartbeat {
  readonly projectId: ProjectId;
  readonly #gateway: HeartbeatGateway;
  readonly #clock: Clock;
  readonly #periodMs: 10_000;
  readonly #initialDelayMs: number;
  readonly #requestTimeoutMs: number;
  readonly #consecutiveTransientFailureThreshold: number;
  readonly #onResult?: (result: HeartbeatResult) => void;
  readonly #onSuccess?: (result: HeartbeatResult) => void;
  readonly #onAuthenticationFailure?: SandboxHeartbeatOptions["onAuthenticationFailure"];
  readonly #onExpired?: (status: number) => void;
  readonly #onTransientFailure?: (status: number | null) => void;
  readonly #onUnreachable?: (status: number | null) => void;
  readonly #onAttempt?: (diagnostic: SandboxHeartbeatAttemptDiagnostic) => void;
  #controller: AbortController | null = null;
  #loop: Promise<void> | null = null;
  #inFlight = false;
  #attemptCount = 0;
  #successCount = 0;
  #failureCount = 0;
  #consecutiveFailures = 0;
  #consecutiveTransientFailures = 0;
  #unreachableReported = false;
  #lastAttemptAtMs: number | null = null;
  #lastSuccessAtMs: number | null = null;
  #lastFailureAtMs: number | null = null;
  #lastStatus: number | null = null;

  constructor(options: SandboxHeartbeatOptions) {
    this.projectId = options.projectId;
    this.#gateway = options.gateway;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#periodMs = options.periodMs ?? 10_000;
    this.#initialDelayMs = options.initialDelayMs ?? heartbeatPhaseOffset(options.projectId);
    this.#requestTimeoutMs = options.requestTimeoutMs ?? 5_000;
    this.#consecutiveTransientFailureThreshold = options.consecutiveTransientFailureThreshold ?? 3;
    if (this.#periodMs !== 10_000) throw new RangeError("Heartbeat period must be exactly 10000ms");
    if (!(this.#initialDelayMs >= 0 && this.#initialDelayMs <= 10_000)) {
      throw new RangeError("Heartbeat initial delay must be between 0 and 10000ms");
    }
    if (!Number.isFinite(this.#requestTimeoutMs) || this.#requestTimeoutMs <= 0) {
      throw new RangeError("Heartbeat request timeout must be positive");
    }
    if (
      !Number.isSafeInteger(this.#consecutiveTransientFailureThreshold) ||
      this.#consecutiveTransientFailureThreshold < 1
    ) {
      throw new RangeError("Heartbeat transient failure threshold must be a positive safe integer");
    }
    this.#onResult = options.onResult;
    this.#onSuccess = options.onSuccess;
    this.#onAuthenticationFailure = options.onAuthenticationFailure;
    this.#onExpired = options.onExpired;
    this.#onTransientFailure = options.onTransientFailure;
    this.#onUnreachable = options.onUnreachable;
    this.#onAttempt = options.onAttempt;
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
      consecutiveFailures: this.#consecutiveFailures,
      lastAttemptAtMs: this.#lastAttemptAtMs,
      lastSuccessAtMs: this.#lastSuccessAtMs,
      lastFailureAtMs: this.#lastFailureAtMs,
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
      try {
        let outcome = await this.#attempt(signal);
        if (outcome.type === "stopped") return;

        if (outcome.type === "result" && !outcome.result.ok && [401, 403].includes(outcome.result.status)) {
          let decision: "retry" | "expired" = "retry";
          if (this.#onAuthenticationFailure) {
            try {
              decision = await this.#onAuthenticationFailure(outcome.result.status, signal);
            } catch {
              decision = "expired";
            }
          }
          if (signal.aborted) return;
          if (decision === "retry") {
            outcome = await this.#attempt(signal);
            if (outcome.type === "stopped") return;
          }
        }

        this.#classifyFinalOutcome(outcome);
      } finally {
        this.#inFlight = false;
      }
      deadline += this.#periodMs;
      while (deadline <= this.#clock.now()) deadline += this.#periodMs;
    }
  }

  async #attempt(signal: AbortSignal): Promise<AttemptOutcome> {
    if (signal.aborted) return { type: "stopped" };

    const startedAtMs = this.#clock.now();
    const previousConsecutiveFailures = this.#consecutiveFailures;

    const requestController = new AbortController();
    const timeoutController = new AbortController();
    let timeoutCancelled = false;
    const stopAttempt = () => {
      requestController.abort();
      timeoutController.abort();
    };
    signal.addEventListener("abort", stopAttempt, { once: true });
    if (signal.aborted) {
      stopAttempt();
      signal.removeEventListener("abort", stopAttempt);
      return { type: "stopped" };
    }

    this.#attemptCount++;
    this.#lastAttemptAtMs = this.#clock.now();

    let gatewayRequest: Promise<HeartbeatResult>;
    try {
      gatewayRequest = Promise.resolve(this.#gateway.heartbeat(requestController.signal));
    } catch (error) {
      gatewayRequest = Promise.reject(error);
    }
    const request = gatewayRequest.then(
      result => ({ type: "result", result }) as const,
      error => {
        if (signal.aborted) return { type: "stopped" } as const;
        const errorName = heartbeatErrorName(error);
        return {
          type: "failure",
          kind: errorName === "AccountAuthError" ? "http" : "transport",
          errorName,
          status: heartbeatErrorStatus(error),
        } as const;
      },
    );

    let timeoutWait: Promise<void>;
    try {
      timeoutWait = this.#clock.waitUntil(
        this.#clock.now() + this.#requestTimeoutMs,
        timeoutController.signal,
      );
    } catch (error) {
      timeoutWait = Promise.reject(error);
    }
    const timeout = timeoutWait.then(
      () => {
        if (signal.aborted) return { type: "stopped" } as const;
        requestController.abort();
        return {
          type: "failure",
          kind: "timeout",
          errorName: "Error",
          status: null,
        } as const;
      },
      () => {
        if (signal.aborted || timeoutCancelled) return { type: "stopped" } as const;
        requestController.abort();
        return {
          type: "failure",
          kind: "timeout",
          errorName: "Error",
          status: null,
        } as const;
      },
    );

    try {
      const outcome = await Promise.race([request, timeout]);
      if (outcome.type === "result") this.#recordResult(outcome.result);
      else if (outcome.type === "failure") this.#recordFailure(null);
      if (outcome.type !== "stopped") {
        const status = outcome.type === "result"
          ? Number.isInteger(outcome.result.status) &&
              outcome.result.status >= 100 && outcome.result.status <= 599
            ? outcome.result.status
            : null
          : outcome.status;
        const diagnosticOutcome: SandboxHeartbeatAttemptOutcome = outcome.type === "result"
          ? outcome.result.ok ? "success" : "http_error"
          : outcome.kind === "http"
            ? "http_error"
            : outcome.kind === "timeout" ? "timeout" : "transport_error";
        this.#call(this.#onAttempt, {
          outcome: diagnosticOutcome,
          status,
          errorName: diagnosticOutcome === "success"
            ? null
            : outcome.type === "failure" ? outcome.errorName : "Error",
          durationMs: Math.max(0, this.#clock.now() - startedAtMs),
          attempt: this.#attemptCount,
          previousConsecutiveFailures,
          consecutiveFailures: this.#consecutiveFailures,
        });
      }
      return outcome;
    } finally {
      signal.removeEventListener("abort", stopAttempt);
      timeoutCancelled = true;
      timeoutController.abort();
    }
  }

  #recordResult(result: HeartbeatResult): void {
    this.#lastStatus = result.status;
    if (result.ok) {
      this.#successCount++;
      this.#lastSuccessAtMs = this.#clock.now();
      this.#consecutiveFailures = 0;
      this.#consecutiveTransientFailures = 0;
      this.#unreachableReported = false;
    } else {
      this.#recordFailure(result.status);
    }
    this.#call(this.#onResult, result);
    if (result.ok) this.#call(this.#onSuccess, result);
  }

  #recordFailure(status: number | null): void {
    this.#failureCount++;
    this.#consecutiveFailures++;
    this.#lastFailureAtMs = this.#clock.now();
    this.#lastStatus = status;
  }

  #classifyFinalOutcome(outcome: Exclude<AttemptOutcome, { type: "stopped" }>): void {
    if (outcome.type === "result" && outcome.result.ok) return;

    const status = outcome.type === "result" ? outcome.result.status : null;
    if (status !== null && [401, 403, 404, 409, 410].includes(status)) {
      this.#consecutiveTransientFailures = 0;
      this.#unreachableReported = false;
      this.#call(this.#onExpired, status);
      return;
    }

    this.#consecutiveTransientFailures++;
    this.#call(this.#onTransientFailure, status);
    if (
      !this.#unreachableReported &&
      this.#consecutiveTransientFailures >= this.#consecutiveTransientFailureThreshold
    ) {
      this.#unreachableReported = true;
      this.#call(this.#onUnreachable, status);
    }
  }

  #call<T>(callback: ((value: T) => void) | undefined, value: T): void {
    if (!callback) return;
    try {
      const returned = callback(value) as unknown;
      if (returned && typeof (returned as PromiseLike<void>).then === "function") {
        void Promise.resolve(returned).catch(() => undefined);
      }
    } catch {}
  }
}
