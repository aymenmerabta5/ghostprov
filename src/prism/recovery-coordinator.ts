import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "../core/ports";
import type { ProjectId } from "../sandbox/types";

const DEFAULT_INITIAL_BACKOFF_MS = 1_000;
const DEFAULT_MAX_BACKOFF_MS = 60_000;
const MAX_RETRY_AFTER_MS = 300_000;

export type FleetRecoveryCircuitState =
  | "closed"
  | "revalidating"
  | "open"
  | "half_open"
  | "closed_for_shutdown";

export type FleetRecoveryCircuitScope = "auth" | "outage" | null;

export type FleetRecoveryFailureKind =
  | "local"
  | "auth"
  | "rate_limit"
  | "server"
  | "account_transport"
  | "provision_transport"
  | "provision_timeout"
  | "other";

export type FleetRecoveryDisposition = "local" | "auth" | "outage";

/**
 * A deliberately broad boundary type. Callers may pass either the original
 * failure or a sanitized runtime failure snapshot without teaching the
 * coordinator about either runtime implementation.
 */
export interface FleetRecoveryTrigger {
  projectId?: ProjectId | string;
  reason?: string;
  failure?: unknown;
  status?: number | null;
  retryAfterMs?: number | null;
}

export interface FleetRecoveryClassification {
  kind: FleetRecoveryFailureKind;
  disposition: FleetRecoveryDisposition;
  retryAfterMs: number | null;
}

export interface FleetRecoverySnapshot {
  state: FleetRecoveryCircuitState;
  scope: FleetRecoveryCircuitScope;
  consecutiveFailures: number;
  queued: number;
  probeInFlight: boolean;
  revalidationInFlight: boolean;
  nextProbeAtMs: number | null;
  lastFailureKind: FleetRecoveryFailureKind | null;
}

export interface FleetRecoveryLease {
  /** True only for the single attempt allowed to test a suspected outage. */
  readonly probe: boolean;
  /** Call only after the replacement child has reached its healthy state. */
  succeed(): void;
  /** Reports the attempt's terminal failure to the fleet-wide circuit. */
  fail(error: unknown): void;
  /** Releases a probe without treating caller cancellation as an outage. */
  cancel(): void;
}

export interface FleetRecoveryCoordinatorOptions {
  clock?: Clock;
  random?: Pick<RandomSource, "next">;
  /** Account/cookie refresh. The coordinator makes this fleet-wide singleflight. */
  revalidateAccount?(signal: AbortSignal): void | Promise<unknown>;
  /** Receives only the same primitive, credential-free fields as snapshot(). */
  onTransition?(snapshot: Readonly<FleetRecoverySnapshot>): void | Promise<void>;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
  revalidationTimeoutMs?: number;
}

interface Waiter {
  id: number;
  signal?: AbortSignal;
  resolve(lease: FleetRecoveryLease): void;
  reject(error: unknown): void;
  onAbort?: () => void;
}

interface ActiveProbe {
  leaseId: number;
  epoch: number;
}

interface ActiveTimer {
  controller: AbortController;
  deadlineMs: number;
  serial: number;
}

interface ActiveRevalidation {
  controller: AbortController;
  timeoutController: AbortController;
  serial: number;
}

interface LeaseRecord {
  active: boolean;
  epoch: number;
  id: number;
  probe: boolean;
  signal?: AbortSignal;
  onAbort?: () => void;
}

function abortError(message = "Fleet recovery wait aborted"): DOMException {
  return new DOMException(message, "AbortError");
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

/** Exponential delay with 0-20% positive jitter and a hard base maximum. */
export function fleetRecoveryBackoffMs(
  consecutiveFailures: number,
  random: () => number = Math.random,
  initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS,
  maxBackoffMs = DEFAULT_MAX_BACKOFF_MS,
): number {
  const initial = positiveInteger(initialBackoffMs, "initialBackoffMs");
  const maximum = positiveInteger(maxBackoffMs, "maxBackoffMs");
  if (maximum < initial) {
    throw new RangeError("maxBackoffMs must be at least initialBackoffMs");
  }
  const exponent = Math.max(0, Math.min(30, Math.trunc(consecutiveFailures) - 1));
  const base = Math.min(maximum, initial * (2 ** exponent));
  return Math.min(maximum, Math.round(base * (1 + normalizedRandom(random()) * 0.2)));
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object"
    ? value as Record<string, unknown>
    : null;
}

function read(value: Record<string, unknown>, key: string): unknown {
  try {
    return value[key];
  } catch {
    return undefined;
  }
}

function normalizedWord(value: unknown): string | null {
  return typeof value === "string"
    ? value.trim().toLowerCase().replace(/[.\s-]+/g, "_")
    : null;
}

function normalizedStatus(value: unknown): number | null {
  return typeof value === "number" && Number.isInteger(value) && value >= 100 && value <= 599
    ? value
    : null;
}

function safeRetryAfter(value: unknown): number | null {
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < 0
  ) return null;
  return Math.min(MAX_RETRY_AFTER_MS, value);
}

function classification(
  kind: FleetRecoveryFailureKind,
  disposition: FleetRecoveryDisposition,
  retryAfterMs: number | null,
): FleetRecoveryClassification {
  return Object.freeze({ kind, disposition, retryAfterMs });
}

function valuesToInspect(value: unknown): readonly unknown[] {
  const values: unknown[] = [value];
  const seen = new Set<unknown>();
  for (let index = 0; index < values.length && index < 8; index++) {
    const current = values[index];
    if (seen.has(current)) continue;
    seen.add(current);
    const object = record(current);
    if (!object) continue;
    for (const key of ["failure", "error", "cause"] as const) {
      const nested = read(object, key);
      if (nested !== undefined && nested !== current) values.push(nested);
    }
  }
  return values;
}

function statusFromReason(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const match = /(?:^|[^0-9])(401|403|404|409|410|429|5[0-9]{2})(?:[^0-9]|$)/.exec(value);
  return match ? Number(match[1]) : null;
}

/**
 * Reduces arbitrary errors to a small allow-list. No message, URL, header,
 * cookie, token, or original error is retained by the coordinator.
 */
export function classifyFleetRecoveryFailure(value: unknown): FleetRecoveryClassification {
  const values = valuesToInspect(value);
  let retryAfterMs: number | null = null;

  for (const candidate of values) {
    const object = record(candidate);
    if (!object) continue;
    retryAfterMs ??= safeRetryAfter(read(object, "retryAfterMs"));
    retryAfterMs ??= safeRetryAfter(read(object, "retry_after_ms"));
  }

  // Status is authoritative and is intentionally considered before a broad
  // `kind: transport` label on wrapper errors.
  for (const candidate of values) {
    const object = record(candidate);
    const status = normalizedStatus(candidate) ??
      (object ? normalizedStatus(read(object, "status")) : null) ??
      (object ? statusFromReason(read(object, "reason")) : null);
    if (status === 404 || status === 409 || status === 410) {
      return classification("local", "local", retryAfterMs);
    }
    if (status === 401 || status === 403) {
      return classification("auth", "auth", retryAfterMs);
    }
    if (status === 429) {
      return classification("rate_limit", "outage", retryAfterMs);
    }
    if (status !== null && status >= 500) {
      return classification("server", "outage", retryAfterMs);
    }
  }

  for (const candidate of values) {
    const object = record(candidate);
    const bare = normalizedWord(candidate);
    const name = object ? normalizedWord(read(object, "name")) : null;
    const kind = object ? normalizedWord(read(object, "kind")) : bare;
    const operation = object ? normalizedWord(read(object, "operation")) : null;
    const reason = object ? normalizedWord(read(object, "reason")) : bare;
    const source = object ? normalizedWord(read(object, "source")) : null;

    if (
      name === "accountautherror" ||
      kind === "auth" ||
      kind === "auth_required" ||
      kind === "guest_identity_lost" ||
      reason === "auth" ||
      reason === "auth_required"
    ) return classification("auth", "auth", retryAfterMs);

    if (kind === "rate_limit" || kind === "rate_limited" || reason === "rate_limit") {
      return classification("rate_limit", "outage", retryAfterMs);
    }

    if (
      name === "accounttransporterror" ||
      kind === "heartbeat_unreachable" ||
      kind === "account_transport" ||
      reason === "account_transport" ||
      operation === "account_transport"
    ) return classification("account_transport", "outage", retryAfterMs);

    const provisioning = name === "provisioningerror" ||
      source === "provision" || source === "provisioning" ||
      operation === "provision" || operation === "provisioning" ||
      reason?.startsWith("provision_") === true;
    if (
      kind === "provision_timeout" ||
      reason === "provision_timeout" ||
      (provisioning && (kind === "timeout" || kind === "transport_timeout"))
    ) return classification("provision_timeout", "outage", retryAfterMs);
    if (
      kind === "provision_transport" ||
      kind === "resource_credential_expired" ||
      reason === "provision_transport" ||
      (provisioning && kind === "transport")
    ) return classification("provision_transport", "outage", retryAfterMs);
  }

  return classification("other", "local", retryAfterMs);
}

/**
 * Fleet-wide admission gate for recovery attempts.
 *
 * Callers must await enter() before taking a provisioning permit. Local
 * sandbox failures bypass the gate. Suspected account/outage failures admit
 * exactly one probe until a caller reports a fully healthy replacement.
 */
export class FleetRecoveryCoordinator {
  readonly #clock: Clock;
  readonly #random: Pick<RandomSource, "next">;
  readonly #revalidateAccount?: FleetRecoveryCoordinatorOptions["revalidateAccount"];
  readonly #onTransition?: FleetRecoveryCoordinatorOptions["onTransition"];
  readonly #initialBackoffMs: number;
  readonly #maxBackoffMs: number;
  readonly #revalidationTimeoutMs: number;
  readonly #waiters: Waiter[] = [];
  readonly #leases = new Set<LeaseRecord>();
  readonly #leaseRecords = new WeakMap<FleetRecoveryLease, LeaseRecord>();

  #state: FleetRecoveryCircuitState = "closed";
  #scope: FleetRecoveryCircuitScope = null;
  #consecutiveFailures = 0;
  #nextProbeAtMs: number | null = null;
  #lastFailureKind: FleetRecoveryFailureKind | null = null;
  #probe: ActiveProbe | null = null;
  #timer: ActiveTimer | null = null;
  #revalidation: ActiveRevalidation | null = null;
  #authValidationRequired = false;
  #closed = false;
  #epoch = 0;
  #nextId = 1;
  #serial = 0;
  #lastEmitted = "";

  constructor(options: FleetRecoveryCoordinatorOptions = {}) {
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#revalidateAccount = options.revalidateAccount;
    this.#onTransition = options.onTransition;
    this.#initialBackoffMs = positiveInteger(
      options.initialBackoffMs ?? DEFAULT_INITIAL_BACKOFF_MS,
      "initialBackoffMs",
    );
    this.#maxBackoffMs = positiveInteger(
      options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS,
      "maxBackoffMs",
    );
    this.#revalidationTimeoutMs = positiveInteger(
      options.revalidationTimeoutMs ?? 10_000,
      "revalidationTimeoutMs",
    );
    if (this.#maxBackoffMs < this.#initialBackoffMs) {
      throw new RangeError("maxBackoffMs must be at least initialBackoffMs");
    }
  }

  enter(trigger: FleetRecoveryTrigger | unknown, signal?: AbortSignal): Promise<FleetRecoveryLease> {
    if (this.#closed) return Promise.reject(abortError("Fleet recovery coordinator is closed"));
    if (signal?.aborted) return Promise.reject(abortError());

    const result = classifyFleetRecoveryFailure(trigger);
    if (result.disposition === "local") {
      return Promise.resolve(this.#makeLease(false, signal));
    }

    return new Promise<FleetRecoveryLease>((resolve, reject) => {
      const waiter: Waiter = { id: this.#nextId++, signal, resolve, reject };
      if (signal) {
        waiter.onAbort = () => {
          if (!this.#removeWaiter(waiter)) return;
          reject(abortError());
          this.#emit();
        };
        signal.addEventListener("abort", waiter.onAbort, { once: true });
      }
      this.#waiters.push(waiter);

      if (result.disposition === "auth") {
        const newlyRequiresValidation = !this.#authValidationRequired;
        this.#authValidationRequired = true;
        this.#scope = "auth";
        // An auth observation is newer and stronger evidence than any
        // already-admitted local/outage attempt. Fence those leases so they
        // cannot bypass the mandatory account revalidation when they finish.
        if (newlyRequiresValidation) {
          this.#epoch++;
        }
      } else if (this.#scope === null) {
        this.#scope = "outage";
      }

      if (this.#state === "closed") {
        if (this.#authValidationRequired) this.#startRevalidation();
        else this.#grantProbe();
      } else {
        this.#dispatch();
      }
      this.#emit();
    });
  }

  snapshot(): Readonly<FleetRecoverySnapshot> {
    return Object.freeze({
      state: this.#state,
      scope: this.#scope,
      consecutiveFailures: this.#consecutiveFailures,
      queued: this.#waiters.length,
      probeInFlight: this.#probe !== null,
      revalidationInFlight: this.#revalidation !== null,
      nextProbeAtMs: this.#nextProbeAtMs,
      lastFailureKind: this.#lastFailureKind,
    });
  }

  /** Synchronously fences all leases and aborts coordinator-owned work. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#epoch++;
    this.#cancelTimer();
    this.#revalidation?.controller.abort();
    this.#revalidation?.timeoutController.abort();
    this.#revalidation = null;
    this.#probe = null;
    for (const lease of this.#leases) this.#deactivateLease(lease);
    for (const waiter of this.#waiters.splice(0)) {
      this.#detachWaiter(waiter);
      waiter.reject(abortError("Fleet recovery coordinator is closed"));
    }
    this.#state = "closed_for_shutdown";
    this.#scope = null;
    this.#nextProbeAtMs = null;
    this.#authValidationRequired = false;
    this.#emit();
  }

  #dispatch(): void {
    if (
      this.#closed ||
      this.#probe ||
      this.#revalidation ||
      this.#waiters.length === 0
    ) return;

    if (this.#state === "closed") {
      if (this.#authValidationRequired) this.#startRevalidation();
      else this.#grantProbe();
      return;
    }
    if (this.#state !== "open") return;

    const deadline = this.#nextProbeAtMs ?? this.#clock.now();
    if (deadline > this.#clock.now()) {
      this.#armTimer(deadline);
      return;
    }
    this.#cancelTimer();
    this.#nextProbeAtMs = null;
    if (this.#authValidationRequired) this.#startRevalidation();
    else this.#grantProbe();
  }

  #startRevalidation(): void {
    if (this.#closed || this.#revalidation || this.#probe) return;
    this.#cancelTimer();
    const controller = new AbortController();
    const timeoutController = new AbortController();
    const active: ActiveRevalidation = {
      controller,
      timeoutController,
      serial: ++this.#serial,
    };
    this.#revalidation = active;
    this.#state = "revalidating";
    this.#scope = "auth";
    this.#nextProbeAtMs = null;
    this.#emit();

    const operation = Promise.resolve().then(() => {
      if (
        this.#closed ||
        controller.signal.aborted ||
        this.#revalidation?.serial !== active.serial
      ) throw abortError("Fleet account revalidation aborted");
      return this.#revalidateAccount?.(controller.signal);
    });
    let timedOut = false;
    const timeout = this.#clock.sleep(
      this.#revalidationTimeoutMs,
      timeoutController.signal,
    ).then(() => {
      timedOut = true;
      controller.abort();
      throw Object.assign(new Error("Fleet account revalidation timed out"), {
        kind: "timeout",
      });
    });
    const bounded = Promise.race([operation, timeout]).finally(() => {
      timeoutController.abort();
    });
    void bounded.then(
      () => {
        if (this.#closed || this.#revalidation?.serial !== active.serial) return;
        this.#revalidation = null;
        this.#authValidationRequired = false;
        if (this.#waiters.length > 0) {
          this.#grantProbe();
        } else {
          this.#state = "open";
          this.#nextProbeAtMs = this.#clock.now();
        }
        this.#emit();
      },
      error => {
        if (this.#closed || this.#revalidation?.serial !== active.serial) return;
        this.#revalidation = null;
        if (controller.signal.aborted && !timedOut) return;
        this.#authValidationRequired = true;
        const classified = classifyFleetRecoveryFailure(error);
        this.#openAfterFailure(
          classified.kind === "other" || classified.kind === "local"
            ? classification("auth", "auth", classified.retryAfterMs)
            : classified,
          "auth",
        );
      },
    );
  }

  #grantProbe(): void {
    if (this.#closed || this.#probe || this.#revalidation) return;
    const waiter = this.#shiftWaiter();
    if (!waiter) {
      this.#state = "open";
      this.#nextProbeAtMs ??= this.#clock.now();
      this.#emit();
      return;
    }
    this.#cancelTimer();
    this.#state = "half_open";
    this.#nextProbeAtMs = null;
    const lease = this.#makeLease(true, waiter.signal);
    const record = this.#leaseRecord(lease);
    this.#probe = { leaseId: record.id, epoch: record.epoch };
    waiter.resolve(lease);
    this.#emit();
  }

  #makeLease(probe: boolean, signal?: AbortSignal): FleetRecoveryLease {
    const leaseRecord: LeaseRecord = {
      active: true,
      epoch: this.#epoch,
      id: this.#nextId++,
      probe,
      signal,
    };
    let lease!: FleetRecoveryLease;
    lease = {
      probe,
      succeed: () => this.#settleLease(leaseRecord, "success"),
      fail: (error: unknown) => this.#settleLease(leaseRecord, "failure", error),
      cancel: () => this.#settleLease(leaseRecord, "cancel"),
    };
    Object.freeze(lease);
    this.#leaseRecords.set(lease, leaseRecord);
    this.#leases.add(leaseRecord);
    if (signal) {
      leaseRecord.onAbort = () => this.#settleLease(leaseRecord, "cancel");
      signal.addEventListener("abort", leaseRecord.onAbort, { once: true });
      if (signal.aborted) this.#settleLease(leaseRecord, "cancel");
    }
    return lease;
  }

  #leaseRecord(lease: FleetRecoveryLease): LeaseRecord {
    const value = this.#leaseRecords.get(lease);
    if (!value) throw new Error("Fleet recovery lease record is unavailable");
    return value;
  }

  #settleLease(
    lease: LeaseRecord,
    outcome: "success" | "failure" | "cancel",
    error?: unknown,
  ): void {
    if (!lease.active) return;
    this.#deactivateLease(lease);
    if (this.#closed) return;

    const ownsProbe = this.#probe?.leaseId === lease.id &&
      this.#probe.epoch === lease.epoch;
    if (lease.epoch !== this.#epoch) {
      // A newer auth incident can fence a probe which is already using an
      // external provisioning permit. Do not overlap it with another probe;
      // release the logical slot only when that stale attempt settles.
      if (ownsProbe) {
        this.#probe = null;
        this.#state = "open";
        this.#nextProbeAtMs ??= this.#clock.now();
        this.#dispatch();
        this.#emit();
      }
      return;
    }
    if (outcome === "success") {
      if (this.#authValidationRequired || this.#revalidation) {
        if (ownsProbe) {
          this.#probe = null;
          this.#state = "open";
          this.#nextProbeAtMs = this.#clock.now();
          this.#dispatch();
          this.#emit();
        }
        return;
      }
      // During an auth incident, only the designated sandbox probe admitted
      // after account revalidation can prove end-to-end recovery. A local
      // bypass that happened to finish meanwhile must not wake auth waiters.
      if (this.#scope === "auth" && !lease.probe) return;
      this.#closeAfterSuccess();
      return;
    }

    if (outcome === "cancel") {
      if (ownsProbe) {
        this.#probe = null;
        this.#state = "open";
        this.#nextProbeAtMs = this.#clock.now();
        this.#dispatch();
        this.#emit();
      }
      return;
    }

    const classified = classifyFleetRecoveryFailure(error);
    if (classified.disposition === "local") {
      if (ownsProbe) {
        this.#probe = null;
        this.#state = "open";
        this.#nextProbeAtMs = this.#clock.now();
        this.#dispatch();
        this.#emit();
      }
      return;
    }

    if (classified.disposition === "auth") this.#authValidationRequired = true;
    if (ownsProbe) this.#probe = null;
    this.#openAfterFailure(
      classified,
      this.#authValidationRequired ? "auth" : "outage",
    );
  }

  #openAfterFailure(
    classified: FleetRecoveryClassification,
    scope: Exclude<FleetRecoveryCircuitScope, null>,
  ): void {
    if (this.#closed) return;
    this.#epoch++;
    this.#revalidation?.controller.abort();
    this.#revalidation?.timeoutController.abort();
    this.#revalidation = null;
    this.#consecutiveFailures++;
    this.#lastFailureKind = classified.kind;
    this.#scope = scope;
    const exponential = fleetRecoveryBackoffMs(
      this.#consecutiveFailures,
      () => this.#random.next(),
      this.#initialBackoffMs,
      this.#maxBackoffMs,
    );
    const delay = Math.max(exponential, classified.retryAfterMs ?? 0);
    this.#nextProbeAtMs = this.#clock.now() + delay;
    this.#state = "open";
    this.#armTimer(this.#nextProbeAtMs);
    this.#emit();
  }

  #closeAfterSuccess(): void {
    if (this.#closed) return;
    if (
      this.#state === "closed" &&
      this.#scope === null &&
      this.#probe === null &&
      this.#revalidation === null &&
      this.#waiters.length === 0 &&
      this.#consecutiveFailures === 0
    ) return;
    this.#epoch++;
    this.#cancelTimer();
    this.#revalidation?.controller.abort();
    this.#revalidation?.timeoutController.abort();
    this.#revalidation = null;
    this.#probe = null;
    this.#state = "closed";
    this.#scope = null;
    this.#consecutiveFailures = 0;
    this.#nextProbeAtMs = null;
    this.#lastFailureKind = null;
    this.#authValidationRequired = false;

    for (const waiter of this.#waiters.splice(0)) {
      this.#detachWaiter(waiter);
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      waiter.resolve(this.#makeLease(false, waiter.signal));
    }
    this.#emit();
  }

  #armTimer(deadlineMs: number): void {
    if (this.#closed || this.#waiters.length === 0) return;
    if (deadlineMs <= this.#clock.now()) {
      this.#cancelTimer();
      queueMicrotask(() => this.#dispatch());
      return;
    }
    if (this.#timer?.deadlineMs === deadlineMs) return;
    this.#cancelTimer();
    const controller = new AbortController();
    const timer: ActiveTimer = {
      controller,
      deadlineMs,
      serial: ++this.#serial,
    };
    this.#timer = timer;
    void this.#clock.waitUntil(deadlineMs, controller.signal).then(
      () => {
        if (this.#closed || this.#timer?.serial !== timer.serial) return;
        this.#timer = null;
        this.#dispatch();
        this.#emit();
      },
      () => {
        if (
          this.#closed ||
          controller.signal.aborted ||
          this.#timer?.serial !== timer.serial
        ) return;
        // A broken injected clock must not permanently strand the fleet. Fail
        // open to one (and still only one) coordinated probe.
        this.#timer = null;
        this.#nextProbeAtMs = null;
        if (this.#authValidationRequired) this.#startRevalidation();
        else this.#grantProbe();
        this.#emit();
      },
    );
  }

  #cancelTimer(): void {
    const timer = this.#timer;
    this.#timer = null;
    timer?.controller.abort();
  }

  #shiftWaiter(): Waiter | null {
    for (;;) {
      const waiter = this.#waiters.shift();
      if (!waiter) return null;
      this.#detachWaiter(waiter);
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      return waiter;
    }
  }

  #removeWaiter(waiter: Waiter): boolean {
    const index = this.#waiters.indexOf(waiter);
    if (index < 0) return false;
    this.#waiters.splice(index, 1);
    this.#detachWaiter(waiter);
    if (this.#waiters.length === 0) {
      this.#cancelTimer();
      if (this.#revalidation && !this.#probe) {
        this.#revalidation.controller.abort();
        this.#revalidation.timeoutController.abort();
        this.#revalidation = null;
        this.#state = "open";
        this.#scope = "auth";
        this.#nextProbeAtMs = this.#clock.now();
        this.#authValidationRequired = true;
      }
    }
    return true;
  }

  #detachWaiter(waiter: Waiter): void {
    if (!waiter.onAbort) return;
    waiter.signal?.removeEventListener("abort", waiter.onAbort);
    waiter.onAbort = undefined;
  }

  #deactivateLease(lease: LeaseRecord): void {
    if (!lease.active) return;
    lease.active = false;
    if (lease.onAbort) lease.signal?.removeEventListener("abort", lease.onAbort);
    lease.onAbort = undefined;
    this.#leases.delete(lease);
  }

  #emit(): void {
    if (!this.#onTransition) return;
    const value = this.snapshot();
    const fingerprint = JSON.stringify(value);
    if (fingerprint === this.#lastEmitted) return;
    this.#lastEmitted = fingerprint;
    try {
      const returned = this.#onTransition(value);
      if (returned && typeof (returned as PromiseLike<void>).then === "function") {
        void Promise.resolve(returned).catch(() => undefined);
      }
    } catch {
      // Observability must never control recovery admission.
    }
  }
}
