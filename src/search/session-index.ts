import { createHmac, randomBytes } from "node:crypto";
import { ApiError, normalizeApiError } from "../http/errors";
import { SYSTEM_CLOCK } from "../core/ports";
import { DEFAULT_ROUTER_ADMISSION_TIMEOUT_MS } from "../router/admission-defaults";
import {
  authenticatedTenantScopeHmac,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";

const DOMAIN = "ghostprov/standalone-search-session/v1\0";
const DEFAULT_MAX_RECORDS = 4_096;
const DEFAULT_MAX_BYTES = 4 * 1024 * 1024;
const DEFAULT_MAX_ACTIVE_KEYS = 4_096;
const DEFAULT_MAX_PENDING_REQUESTS = 4_096;
const DEFAULT_MAX_PENDING_PER_KEY = 64;
const DEFAULT_TTL_MS = 60 * 60 * 1_000;
const MAX_PROVIDER_RESPONSE_ID_BYTES = 512;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

interface Entry {
  responseId: string;
  bytes: number;
  expiresAtMs: number;
}

interface PendingOperation {
  readonly deadlineMs: number;
  readonly signal: AbortSignal | undefined;
  readonly execute: () => Promise<void>;
  readonly reject: (error: unknown) => void;
  disposeWait: () => void;
}

interface SessionQueue {
  readonly pending: Set<PendingOperation>;
  running: boolean;
  scheduled: boolean;
}

export class StandaloneSearchQueueTimeoutError extends ApiError {
  constructor() {
    super(408, "Standalone search expired while waiting for its preceding session turn.", {
      type: "request_timeout_error",
      code: "search_session_queue_timeout",
    });
  }
}

function queueStopped(shutdown: boolean): ApiError {
  return new ApiError(503, shutdown
    ? "Standalone search is shutting down."
    : "Standalone search session was cleared before execution.", {
    type: "service_unavailable_error",
    code: shutdown ? "router_shutting_down" : "search_session_cleared",
  });
}

function schedule(callback: () => void, delayMs: number): () => void {
  const timer = setTimeout(callback, delayMs);
  timer.unref?.();
  return () => clearTimeout(timer);
}

export interface StandaloneSearchSessionRunOptions {
  readonly signal?: AbortSignal;
  /** Original ingress deadline; waiting here never starts a fresh budget. */
  readonly deadlineMs?: number;
}

export interface StandaloneSearchSessionIndexOptions {
  readonly maxRecords?: number;
  readonly maxBytes?: number;
  readonly maxActiveKeys?: number;
  readonly maxPendingRequests?: number;
  readonly maxPendingPerKey?: number;
  /** Fallback wait bound only when the caller has no ingress deadline. */
  readonly maxWaitMs?: number;
  readonly ttlMs?: number;
  readonly now?: () => number;
  /** Returns a cancellation function for the bounded wait timer. */
  readonly schedule?: (callback: () => void, delayMs: number) => () => void;
  readonly hmacKey?: Uint8Array;
}

export interface StandaloneSearchSessionOutcome<T> {
  readonly responseId: string;
  readonly value: T;
}

/**
 * Content-free, tenant-scoped search lineage. Bounded pending callbacks keep
 * caller-owned ingress reservations; completed queries and results are not kept.
 */
export class StandaloneSearchSessionIndex {
  readonly #entries = new Map<string, Entry>();
  readonly #queues = new Map<string, SessionQueue>();
  readonly #key: Uint8Array;
  readonly #maxRecords: number;
  readonly #maxBytes: number;
  readonly #maxActiveKeys: number;
  readonly #maxPendingRequests: number;
  readonly #maxPendingPerKey: number;
  readonly #maxWaitMs: number;
  readonly #ttlMs: number;
  readonly #now: () => number;
  readonly #schedule: (callback: () => void, delayMs: number) => () => void;
  #pendingRequests = 0;
  #bytes = 0;
  #generation = 0;
  #shutdown = false;

  constructor(options: StandaloneSearchSessionIndexOptions = {}) {
    this.#maxRecords = positive(options.maxRecords ?? DEFAULT_MAX_RECORDS, "maxRecords");
    this.#maxBytes = positive(options.maxBytes ?? DEFAULT_MAX_BYTES, "maxBytes");
    this.#maxActiveKeys = positive(
      options.maxActiveKeys ?? DEFAULT_MAX_ACTIVE_KEYS,
      "maxActiveKeys",
    );
    this.#maxPendingRequests = positive(
      options.maxPendingRequests ?? DEFAULT_MAX_PENDING_REQUESTS,
      "maxPendingRequests",
    );
    this.#maxPendingPerKey = positive(
      options.maxPendingPerKey ?? DEFAULT_MAX_PENDING_PER_KEY,
      "maxPendingPerKey",
    );
    this.#maxWaitMs = positive(
      options.maxWaitMs ?? DEFAULT_ROUTER_ADMISSION_TIMEOUT_MS,
      "maxWaitMs",
    );
    this.#ttlMs = positive(options.ttlMs ?? DEFAULT_TTL_MS, "ttlMs");
    this.#now = options.now ?? SYSTEM_CLOCK.now;
    this.#schedule = options.schedule ?? schedule;
    this.#key = Uint8Array.from(options.hmacKey ?? randomBytes(32));
    if (this.#key.byteLength < 32 || this.#key.byteLength > 64) {
      throw new RangeError("hmacKey must contain 32 through 64 bytes");
    }
  }

  async run<T>(
    scope: AuthenticatedTenantScope,
    sessionId: string,
    operation: (
      previousResponseId: string | null,
      compareDelete: (expectedResponseId: string) => boolean,
    ) => Promise<StandaloneSearchSessionOutcome<T>>,
    options: StandaloneSearchSessionRunOptions = {},
  ): Promise<T> {
    if (!isAuthenticatedTenantScope(scope)) {
      throw new TypeError("Standalone search requires an authenticated tenant scope");
    }
    if (typeof sessionId !== "string" || sessionId.length < 1 || sessionId.length > 128) {
      throw new TypeError("Standalone search session ID is invalid");
    }
    if (this.#shutdown) throw queueStopped(true);
    const now = this.#time();
    if (options.deadlineMs !== undefined && (
      !Number.isFinite(options.deadlineMs) || options.deadlineMs < 0 ||
      options.deadlineMs > Number.MAX_SAFE_INTEGER
    )) throw new RangeError("Standalone search deadline is invalid");
    const deadlineMs = options.deadlineMs ?? now + this.#maxWaitMs;
    const signal = options.signal;
    if (now >= deadlineMs) throw new StandaloneSearchQueueTimeoutError();
    if (signal?.aborted) throw this.#abortReason(signal, deadlineMs);
    const key = this.#digest(scope, sessionId);
    let queue = this.#queues.get(key);
    if (
      (!queue && this.#queues.size >= this.#maxActiveKeys) ||
      this.#pendingRequests >= this.#maxPendingRequests ||
      (queue && queue.pending.size >= this.#maxPendingPerKey)
    ) {
      // These are live execution owners, separate from evictable lineage hints.
      // Bypassing a full queue would break same-session provider ordering.
      throw new ApiError(503, "Standalone search session queue is full.", {
        type: "overloaded_error",
        code: "search_session_queue_full",
      });
    }
    if (!queue) {
      queue = { pending: new Set(), running: false, scheduled: false };
      this.#queues.set(key, queue);
    }
    const owner = queue;
    const generation = this.#generation;
    return new Promise<T>((resolve, reject) => {
      const pending: PendingOperation = {
        deadlineMs,
        signal,
        reject,
        disposeWait: () => undefined,
        execute: async () => {
          try {
            const startedAtMs = this.#time();
            this.#pruneExpired(startedAtMs);
            const entry = this.#entries.get(key);
            const previousResponseId = entry && entry.expiresAtMs > startedAtMs
              ? entry.responseId
              : null;
            const compareDelete = (expectedResponseId: string): boolean => {
              if (this.#shutdown || generation !== this.#generation) return false;
              const current = this.#entries.get(key);
              if (!current || current.responseId !== expectedResponseId) return false;
              this.#delete(key);
              return true;
            };
            const outcome = await operation(previousResponseId, compareDelete);
            if (!this.#shutdown && generation === this.#generation) {
              this.#commit(key, outcome.responseId, this.#time());
            }
            resolve(outcome.value);
          } catch (error) {
            reject(error);
          } finally {
            owner.running = false;
            this.#drain(key, owner);
          }
        },
      };
      owner.pending.add(pending);
      this.#pendingRequests++;
      const abort = () => this.#cancel(key, owner, pending, this.#abortReason(signal!, deadlineMs));
      let cancelTimer = () => {};
      const armDeadline = () => {
        if (!owner.pending.has(pending)) return;
        const remainingMs = deadlineMs - this.#time();
        if (remainingMs <= 0) {
          this.#cancel(key, owner, pending, new StandaloneSearchQueueTimeoutError());
          return;
        }
        cancelTimer = this.#schedule(armDeadline, Math.min(MAX_TIMER_DELAY_MS, Math.ceil(remainingMs)));
      };
      pending.disposeWait = () => {
        cancelTimer();
        signal?.removeEventListener("abort", abort);
      };
      signal?.addEventListener("abort", abort, { once: true });
      armDeadline();
      if (signal?.aborted) abort();
      if (!owner.running && !owner.scheduled) {
        owner.scheduled = true;
        queueMicrotask(() => {
          owner.scheduled = false;
          this.#drain(key, owner);
        });
      }
    });
  }

  clear(): void {
    this.#generation = this.#generation === Number.MAX_SAFE_INTEGER
      ? 0
      : this.#generation + 1;
    this.#entries.clear();
    for (const [key, queue] of this.#queues) {
      for (const pending of queue.pending) {
        this.#cancel(key, queue, pending, queueStopped(this.#shutdown));
      }
      // Running work still owns its serialization lock until terminal proof.
      // Clearing metadata must not permit a concurrent turn for the same key.
      if (!queue.running) this.#queues.delete(key);
    }
    this.#bytes = 0;
  }

  beginShutdown(): void {
    if (this.#shutdown) return;
    this.#shutdown = true;
    this.clear();
  }

  snapshot(): Readonly<{
    records: number;
    bytes: number;
    activeKeys: number;
    pendingRequests: number;
    maxActiveKeys: number;
    maxPendingRequests: number;
    maxPendingPerKey: number;
    maxWaitMs: number;
    maxRecords: number;
    maxBytes: number;
    ttlMs: number;
  }> {
    this.#pruneExpired(this.#time());
    return Object.freeze({
      records: this.#entries.size,
      bytes: this.#bytes,
      activeKeys: this.#queues.size,
      pendingRequests: this.#pendingRequests,
      maxActiveKeys: this.#maxActiveKeys,
      maxPendingRequests: this.#maxPendingRequests,
      maxPendingPerKey: this.#maxPendingPerKey,
      maxWaitMs: this.#maxWaitMs,
      maxRecords: this.#maxRecords,
      maxBytes: this.#maxBytes,
      ttlMs: this.#ttlMs,
    });
  }

  #abortReason(signal: AbortSignal, deadlineMs: number): Error {
    if (this.#time() >= deadlineMs) return new StandaloneSearchQueueTimeoutError();
    // The ingress timer can win the race with this queue's timer. Its generic
    // routing code must not attribute a wait before router entry to capacity.
    const code = normalizeApiError(signal.reason).error.code;
    if (code === "router_capacity_timeout") return new StandaloneSearchQueueTimeoutError();
    if (code === "router_shutting_down") return queueStopped(true);
    return new DOMException("Standalone search aborted while waiting for its session turn", "AbortError");
  }

  #removePending(queue: SessionQueue, pending: PendingOperation): boolean {
    if (!queue.pending.delete(pending)) return false;
    this.#pendingRequests--;
    pending.disposeWait();
    return true;
  }

  #cancel(key: string, queue: SessionQueue, pending: PendingOperation, error: unknown): void {
    if (!this.#removePending(queue, pending)) return;
    pending.reject(error);
    if (!queue.running && queue.pending.size === 0 && this.#queues.get(key) === queue) {
      this.#queues.delete(key);
    }
  }

  #drain(key: string, queue: SessionQueue): void {
    if (queue.running || this.#queues.get(key) !== queue) return;
    for (const pending of queue.pending) {
      if (this.#shutdown) {
        this.#cancel(key, queue, pending, queueStopped(true));
        continue;
      }
      if (this.#time() >= pending.deadlineMs || pending.signal?.aborted) {
        this.#cancel(key, queue, pending, pending.signal?.aborted
          ? this.#abortReason(pending.signal, pending.deadlineMs)
          : new StandaloneSearchQueueTimeoutError());
        continue;
      }
      this.#removePending(queue, pending);
      queue.running = true;
      void pending.execute();
      return;
    }
    this.#queues.delete(key);
  }

  #digest(scope: AuthenticatedTenantScope, sessionId: string): string {
    const scopeDigest = authenticatedTenantScopeHmac(scope, this.#key, DOMAIN);
    return createHmac("sha256", this.#key)
      .update(scopeDigest, "utf8")
      .update("\0", "utf8")
      .update(sessionId, "utf8")
      .digest("base64url");
  }

  #commit(key: string, responseId: string, now: number): void {
    if (
      typeof responseId !== "string" || responseId.length < 1 ||
      Buffer.byteLength(responseId, "utf8") > MAX_PROVIDER_RESPONSE_ID_BYTES
    ) return;
    const bytes = Buffer.byteLength(key, "utf8") + Buffer.byteLength(responseId, "utf8") + 96;
    if (bytes > this.#maxBytes) return;
    const existing = this.#entries.get(key);
    if (existing) {
      this.#entries.delete(key);
      this.#bytes -= existing.bytes;
    }
    while (
      this.#entries.size >= this.#maxRecords ||
      this.#bytes + bytes > this.#maxBytes
    ) {
      const oldest = this.#entries.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.#delete(oldest);
    }
    const entry: Entry = {
      responseId,
      bytes,
      expiresAtMs: now + this.#ttlMs,
    };
    this.#entries.set(key, entry);
    this.#bytes += bytes;
  }

  #pruneExpired(now: number): void {
    for (const [key, entry] of this.#entries) {
      if (entry.expiresAtMs > now) continue;
      this.#delete(key);
    }
  }

  #delete(key: string): void {
    const entry = this.#entries.get(key);
    if (!entry) return;
    this.#entries.delete(key);
    this.#bytes = Math.max(0, this.#bytes - entry.bytes);
  }

  #time(): number {
    const value = this.#now();
    if (!Number.isFinite(value) || value < 0 || value > Number.MAX_SAFE_INTEGER) {
      throw new RangeError("Standalone search session clock is invalid");
    }
    return value;
  }
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}
