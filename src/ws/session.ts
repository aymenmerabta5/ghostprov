import type { WebSocket } from "ws";
import type { RunHandle } from "../relay/run-handle";
import { config } from "../config";
import {
  definitiveProviderRateLimitFailedEvent,
  normalizeApiError,
} from "../http/errors";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  ResponseGraphCapacityError,
  SYNTHETIC_WARMUP_ID_PREFIX,
  releaseResponseGraphPayload,
  releaseResponseGraphResolution,
  type ResponseCreateEvent,
  type ResolvedRelayRequest,
} from "./response-graph";
import type {
  WebSocketCloseCause,
  WebSocketTelemetry,
} from "./telemetry";
import {
  releaseAtomicBytePermit,
  transferAtomicBytePermit,
} from "../v2/memory/permit-carrier";
import { jsonEncodedByteLengthAsync } from "../router/workload";
import type { ElasticByteReservation } from "../core/elastic-byte-reservation";

import {
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";

const MiB = 1024 * 1024;
const DEFAULT_MAX_PENDING_REQUESTS = 128;
const DEFAULT_MAX_PENDING_REQUEST_BYTES = 64 * MiB;
const DEFAULT_MAX_SEND_QUEUE_ITEMS = 512;
const DEFAULT_MAX_SEND_QUEUE_BYTES = 64 * MiB;
const DEFAULT_SEND_TIMEOUT_MS = 30_000;
const UPSTREAM_POLICY_HEADROOM_BYTES = 64;
const SOCKET_POLL_MS = 5;

export interface WebSocketProcessBudgetOptions {
  maxSessions: number;
  maxQueuedBytes: number;
  maxIngressBytes?: number;
}

export interface WebSocketProcessBudgetSnapshot {
  sessions: number;
  queuedBytes: number;
  ingressReservedBytes: number;
  maxSessions: number;
  maxQueuedBytes: number;
  maxIngressBytes: number;
}

export interface WebSocketProcessLease {
  canReserve(bytes: number): boolean;
  tryReserve(bytes: number): boolean;
  release(bytes: number): void;
  tryEnsureIngress(bytes: number): boolean;
  releaseIngress(): void;
  close(): void;
}

/** Shared process-level fence across every WebSocket server/session. */
export class WebSocketProcessBudget {
  readonly #maxSessions: number;
  readonly #maxQueuedBytes: number;
  readonly #maxIngressBytes: number;
  #sessions = 0;
  #queuedBytes = 0;
  #ingressReservedBytes = 0;
  #baseIngressReservedBytes = 0;
  readonly #promotionHeadrooms = new Map<number, number>();
  #backing: ElasticByteReservation | null = null;

  constructor(options: WebSocketProcessBudgetOptions) {
    this.#maxSessions = positiveInteger(options.maxSessions, "maxSessions");
    this.#maxQueuedBytes = positiveInteger(
      options.maxQueuedBytes,
      "maxQueuedBytes",
    );
    this.#maxIngressBytes = positiveInteger(
      options.maxIngressBytes ?? options.maxQueuedBytes,
      "maxIngressBytes",
    );
  }

  attachElasticBacking(backing: ElasticByteReservation): void {
    if (this.#backing) throw new Error("WebSocket process budget already has elastic backing");
    const maximumBytes = this.#maxQueuedBytes + this.#maxIngressBytes;
    if (!Number.isSafeInteger(maximumBytes) || backing.maximumBytes < maximumBytes) {
      throw new RangeError("WebSocket elastic backing is smaller than the process budget");
    }
    if (!backing.tryResize(this.#requiredBackingBytes())) {
      throw new Error("WebSocket elastic backing cannot cover existing retained bytes");
    }
    this.#backing = backing;
  }

  detachElasticBacking(backing: ElasticByteReservation): void {
    if (this.#backing !== backing) throw new Error("WebSocket elastic backing does not match");
    if (
      this.#sessions !== 0 ||
      this.#queuedBytes !== 0 ||
      this.#ingressReservedBytes !== 0 ||
      this.#baseIngressReservedBytes !== 0 ||
      this.#promotionHeadrooms.size !== 0
    ) throw new Error("WebSocket elastic backing still owns retained bytes");
    if (!backing.tryResize(0)) throw new Error("WebSocket elastic backing could not be released");
    this.#backing = null;
  }

  openSession(
    ingressReservationBytes = 0,
    ingressHeadroomBytes = 0,
  ): WebSocketProcessLease | null {
    if (
      !Number.isSafeInteger(ingressReservationBytes) ||
      ingressReservationBytes < 0
    ) throw new RangeError("ingressReservationBytes must be a non-negative safe integer");
    if (
      !Number.isSafeInteger(ingressHeadroomBytes) ||
      ingressHeadroomBytes < 0
    ) throw new RangeError("ingressHeadroomBytes must be a non-negative safe integer");
    const nextIngressReservedBytes = this.#ingressReservedBytes + ingressReservationBytes;
    const nextBaseIngressReservedBytes =
      this.#baseIngressReservedBytes + ingressReservationBytes;
    const nextPromotionHeadroomBytes = Math.max(
      this.#maximumPromotionHeadroomBytes(),
      ingressHeadroomBytes,
    );
    if (
      this.#sessions >= this.#maxSessions ||
      nextIngressReservedBytes + nextPromotionHeadroomBytes > this.#maxIngressBytes ||
      !this.#tryResizeBacking(
        this.#queuedBytes,
        nextIngressReservedBytes,
        nextBaseIngressReservedBytes,
        nextPromotionHeadroomBytes,
      )
    ) return null;
    this.#sessions++;
    this.#ingressReservedBytes = nextIngressReservedBytes;
    this.#baseIngressReservedBytes = nextBaseIngressReservedBytes;
    this.#addPromotionHeadroom(ingressHeadroomBytes);
    let closed = false;
    let queuedReserved = 0;
    let dynamicIngressReserved = 0;
    const validateIngressTarget = (bytes: number): number => {
      if (!Number.isSafeInteger(bytes) || bytes < 0) {
        throw new RangeError("WebSocket ingress target must be a non-negative safe integer");
      }
      return bytes;
    };
    const tryEnsureIngress = (bytes: number): boolean => {
      const target = validateIngressTarget(bytes);
      if (closed) return false;
      if (target <= dynamicIngressReserved) return true;
      const growth = target - dynamicIngressReserved;
      if (this.#ingressReservedBytes + growth > this.#maxIngressBytes) return false;
      if (!this.#tryResizeBacking(
        this.#queuedBytes,
        this.#ingressReservedBytes + growth,
        this.#baseIngressReservedBytes,
        this.#maximumPromotionHeadroomBytes(),
      )) return false;
      this.#ingressReservedBytes += growth;
      dynamicIngressReserved = target;
      return true;
    };
    return {
      canReserve: bytes => !closed && this.#canReserve(bytes),
      tryReserve: bytes => {
        if (closed || !this.#canReserve(bytes)) return false;
        const value = Math.max(0, Math.floor(bytes));
        if (!this.#tryResizeBacking(
          this.#queuedBytes + value,
          this.#ingressReservedBytes,
          this.#baseIngressReservedBytes,
          this.#maximumPromotionHeadroomBytes(),
        )) return false;
        queuedReserved += value;
        this.#queuedBytes += value;
        return true;
      },
      release: bytes => {
        if (closed) return;
        const value = Math.min(queuedReserved, Math.max(0, Math.floor(bytes)));
        queuedReserved -= value;
        this.#queuedBytes = Math.max(0, this.#queuedBytes - value);
        this.#releaseBackingToCurrentUsage();
      },
      tryEnsureIngress,
      releaseIngress: () => {
        if (closed) return;
        this.#ingressReservedBytes = Math.max(
          0,
          this.#ingressReservedBytes - dynamicIngressReserved,
        );
        dynamicIngressReserved = 0;
        this.#releaseBackingToCurrentUsage();
      },
      close: () => {
        if (closed) return;
        closed = true;
        this.#queuedBytes = Math.max(0, this.#queuedBytes - queuedReserved);
        this.#ingressReservedBytes = Math.max(
          0,
          this.#ingressReservedBytes - ingressReservationBytes - dynamicIngressReserved,
        );
        this.#baseIngressReservedBytes = Math.max(
          0,
          this.#baseIngressReservedBytes - ingressReservationBytes,
        );
        this.#removePromotionHeadroom(ingressHeadroomBytes);
        queuedReserved = 0;
        dynamicIngressReserved = 0;
        this.#sessions = Math.max(0, this.#sessions - 1);
        this.#releaseBackingToCurrentUsage();
      },
    };
  }

  snapshot(): WebSocketProcessBudgetSnapshot {
    return {
      sessions: this.#sessions,
      queuedBytes: this.#queuedBytes,
      ingressReservedBytes: this.#ingressReservedBytes,
      maxSessions: this.#maxSessions,
      maxQueuedBytes: this.#maxQueuedBytes,
      maxIngressBytes: this.#maxIngressBytes,
    };
  }

  #canReserve(bytes: number): boolean {
    return Number.isSafeInteger(bytes) && bytes >= 0 &&
      this.#queuedBytes + bytes <= this.#maxQueuedBytes;
  }

  #maximumPromotionHeadroomBytes(): number {
    let maximum = 0;
    for (const bytes of this.#promotionHeadrooms.keys()) maximum = Math.max(maximum, bytes);
    return maximum;
  }

  #addPromotionHeadroom(bytes: number): void {
    if (bytes === 0) return;
    this.#promotionHeadrooms.set(bytes, (this.#promotionHeadrooms.get(bytes) ?? 0) + 1);
  }

  #removePromotionHeadroom(bytes: number): void {
    if (bytes === 0) return;
    const count = this.#promotionHeadrooms.get(bytes) ?? 0;
    if (count <= 1) this.#promotionHeadrooms.delete(bytes);
    else this.#promotionHeadrooms.set(bytes, count - 1);
  }

  #requiredBackingBytes(
    queuedBytes = this.#queuedBytes,
    ingressReservedBytes = this.#ingressReservedBytes,
    baseIngressReservedBytes = this.#baseIngressReservedBytes,
    promotionHeadroomBytes = this.#maximumPromotionHeadroomBytes(),
  ): number {
    return queuedBytes + Math.max(
      ingressReservedBytes,
      baseIngressReservedBytes + promotionHeadroomBytes,
    );
  }

  #tryResizeBacking(
    queuedBytes: number,
    ingressReservedBytes: number,
    baseIngressReservedBytes: number,
    promotionHeadroomBytes: number,
  ): boolean {
    return this.#backing?.tryResize(this.#requiredBackingBytes(
      queuedBytes,
      ingressReservedBytes,
      baseIngressReservedBytes,
      promotionHeadroomBytes,
    )) ?? true;
  }

  #releaseBackingToCurrentUsage(): void {
    if (this.#backing && !this.#backing.tryResize(this.#requiredBackingBytes())) {
      throw new Error("WebSocket elastic backing failed to shrink");
    }
  }

}

export type WebSocketRelayStarter = (
  body: Record<string, unknown>,
  options: {
    signal: AbortSignal;
    clientId: string;
    authorizationScope: AuthenticatedTenantScope;
    encodedBodyBytes?: number;
    structuralBodyBytes?: number;
  },
) => Promise<RunHandle>;

export interface ResponsesWebSocketSessionOptions {
  startRelay: WebSocketRelayStarter;
  maxActive?: number;
  maxNamedStreams?: number;
  /** Maximum requests retained by this session, including active work. */
  maxPendingRequests?: number;
  /** Maximum aggregate wire bytes retained by queued and active requests. */
  maxPendingRequestBytes?: number;
  /** Socket bufferedAmount threshold; a single larger message is still allowed. */
  maxBufferedBytes?: number;
  /** Hard count bound for serialized messages waiting on this socket. */
  maxSendQueueItems?: number;
  /** Hard aggregate UTF-8 byte bound for serialized outgoing messages. */
  maxSendQueueBytes?: number;
  /** Maximum queue/backpressure/send-callback lifetime for one outgoing message. */
  sendTimeoutMs?: number;
  /** Stable trusted tenant scope supplied by the server integration. */
  authorizationScope: AuthenticatedTenantScope;
  /** Stable routing identity reused by every response on this connection. */
  clientId?: string;
  /** Shared process-level session and retained-byte fence. */
  processBudget?: WebSocketProcessBudget;
  /** Pre-opened by the upgrade path before `ws` can assemble its first frame. */
  processLease?: WebSocketProcessLease;
  /** Direct-session test/integration fallback; server paths pass processLease. */
  parserReservationBytes?: number;
  /** Server-owned idle timer hook; not part of the public Responses contract. */
  onActivity?: () => void;
  graph?: ResponseGraph;
  telemetry?: WebSocketTelemetry;
}

interface SemaphoreWaiter {
  resolve: (release: () => void) => void;
  reject: (error: unknown) => void;
  signal: AbortSignal;
  bytes: number;
  onAbort: () => void;
}

class WebSocketQueueFullError extends Error {
  readonly name = "WebSocketQueueFullError";
  readonly code = "websocket_request_queue_full";
}

export class WebSocketSessionCapacityError extends Error {
  readonly name = "WebSocketSessionCapacityError";
  readonly code = "websocket_session_capacity_exhausted";
}

class WebSocketRequestOutputError extends Error {
  readonly name = "WebSocketRequestOutputError";

  constructor(
    message: string,
    readonly code: string,
    readonly status: 500 | 503,
    readonly type: "internal_error" | "overloaded_error",
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export class WebSocketDeliveryError extends Error {
  readonly name = "WebSocketDeliveryError";

  constructor(message: string, readonly code: string, options?: ErrorOptions) {
    super(message, options);
  }
}

class AsyncSemaphore {
  #active = 0;
  #queuedBytes = 0;
  #closed = false;
  readonly #waiters: SemaphoreWaiter[] = [];
  readonly #capacity: number;
  readonly #maxQueued: number;
  readonly #maxQueuedBytes: number;

  constructor(
    capacity: number,
    maxQueued: number,
    maxQueuedBytes: number,
  ) {
    this.#capacity = capacity;
    this.#maxQueued = maxQueued;
    this.#maxQueuedBytes = maxQueuedBytes;
  }

  acquire(signal: AbortSignal, bytes: number): Promise<() => void> {
    if (signal.aborted) return Promise.reject(abortError());
    if (this.#closed) return Promise.reject(abortError());
    if (this.#active < this.#capacity && this.#waiters.length === 0) {
      this.#active++;
      return Promise.resolve(this.#release());
    }
    if (
      this.#waiters.length >= this.#maxQueued ||
      this.#queuedBytes + bytes > this.#maxQueuedBytes
    ) {
      return Promise.reject(new WebSocketQueueFullError(
        "This WebSocket connection has reached its pending response limit.",
      ));
    }
    return new Promise((resolve, reject) => {
      const waiter: SemaphoreWaiter = {
        resolve,
        reject,
        signal,
        bytes,
        onAbort: () => {
          const index = this.#waiters.indexOf(waiter);
          if (index === -1) return;
          this.#waiters.splice(index, 1);
          this.#queuedBytes = Math.max(0, this.#queuedBytes - waiter.bytes);
          reject(abortError());
        },
      };
      signal.addEventListener("abort", waiter.onAbort, { once: true });
      this.#waiters.push(waiter);
      this.#queuedBytes += bytes;
      this.#drain();
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    const error = abortError();
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal.removeEventListener("abort", waiter.onAbort);
      waiter.reject(error);
    }
    this.#queuedBytes = 0;
  }

  #release(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.#active = Math.max(0, this.#active - 1);
      this.#drain();
    };
  }

  #drain(): void {
    while (!this.#closed && this.#active < this.#capacity && this.#waiters.length) {
      const waiter = this.#waiters.shift()!;
      this.#queuedBytes = Math.max(0, this.#queuedBytes - waiter.bytes);
      waiter.signal.removeEventListener("abort", waiter.onAbort);
      if (waiter.signal.aborted) {
        waiter.reject(abortError());
        continue;
      }
      this.#active++;
      waiter.resolve(this.#release());
    }
  }
}

interface PendingSend {
  data: string;
  bytes: number;
  enqueuedAt: number;
  resolve: () => void;
  reject: (error: unknown) => void;
  started: boolean;
  settled: boolean;
}

function abortError(): DOMException {
  return new DOMException("WebSocket request aborted", "AbortError");
}

function abortable<T>(
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

async function disposeLateHandle(handle: RunHandle): Promise<void> {
  const cleanup: Promise<unknown>[] = [];
  try {
    const discard = handle.discardUnconsumedEvents;
    if (typeof discard === "function") {
      // V2 must remain able to accept and definitively settle committed events.
      // Claiming then closing its iterator would instead turn later delivery
      // into an ambiguous committed failure.
      discard.call(handle);
    } else {
      const iterator = handle.events();
      // Async generators which have never started do not execute their `finally`
      // block when return() is called first. Claim exactly one buffered frame so
      // its permit can be released, then close the generic iterator.
      cleanup.push(Promise.resolve()
        .then(() => iterator.next())
        .then(async result => {
          if (!result.done) releaseAtomicBytePermit(result.value);
          await iterator.return(undefined);
        })
        .catch(() => undefined));
    }
  } catch {}
  try {
    cleanup.push(Promise.resolve(handle.final).then(
      releaseAtomicBytePermit,
      () => undefined,
    ));
  } catch {}
  try {
    if (handle.upstreamPrelude) cleanup.push(handle.upstreamPrelude.catch(() => undefined));
  } catch {}
  await Promise.allSettled(cleanup);
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return value;
}

function errorEvent(
  code: string,
  message: string,
  options: {
    streamId?: string;
    param?: string;
    status?: number;
    type?: string;
  } = {},
): Record<string, unknown> {
  return {
    type: "error",
    status: options.status ?? 400,
    ...(options.streamId ? { stream_id: options.streamId } : {}),
    error: {
      type: options.type ?? "invalid_request_error",
      code,
      message,
      param: options.param ?? null,
    },
  };
}

function normalizedErrorEvent(
  error: unknown,
  streamId?: string,
): Record<string, unknown> {
  const retryableRateLimit = definitiveProviderRateLimitFailedEvent(error);
  if (retryableRateLimit) {
    return {
      ...retryableRateLimit,
      ...(streamId ? { stream_id: streamId } : {}),
    };
  }

  const normalized = normalizeApiError(error);
  return {
    type: "error",
    status: normalized.status,
    ...(streamId ? { stream_id: streamId } : {}),
    error: normalized.error,
  };
}

function isNamedStreamId(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9_.-]{1,256}$/.test(value);
}

function parseEventData(data: string): Record<string, unknown> {
  const parsed = JSON.parse(data);
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("upstream event data is not a JSON object");
  }
  return parsed as Record<string, unknown>;
}

function terminalEventType(final: unknown):
  | "response.completed"
  | "response.failed"
  | "response.incomplete" {
  if (!final || typeof final !== "object" || Array.isArray(final)) {
    throw new TypeError("WebSocket relay returned a malformed final response");
  }
  const status = (final as Record<string, unknown>).status;
  if (status === "completed" || status === "failed" || status === "incomplete") {
    return `response.${status}`;
  }
  throw new TypeError("WebSocket relay returned an invalid final response status");
}

function terminalFrameMatchesFinal(
  terminal: Record<string, unknown>,
  final: unknown,
): boolean {
  if (!final || typeof final !== "object" || Array.isArray(final)) return false;
  const expectedType = terminalEventType(final);
  if (terminal.type !== expectedType) return false;
  const response = terminal.response;
  if (!response || typeof response !== "object" || Array.isArray(response)) return false;
  const expected = final as Record<string, unknown>;
  const actual = response as Record<string, unknown>;
  return typeof expected.id === "string" && expected.id.length > 0 &&
    actual.id === expected.id && actual.status === expected.status;
}

export class ResponsesWebSocketSession {
  readonly #startRelay: WebSocketRelayStarter;
  readonly #maxNamedStreams: number;
  readonly #maxPendingRequests: number;
  readonly #maxPendingRequestBytes: number;
  readonly #maxBufferedBytes: number;
  readonly #maxSendQueueItems: number;
  readonly #maxSendQueueBytes: number;
  readonly #sendTimeoutMs: number;
  readonly #authorizationScope: AuthenticatedTenantScope;
  readonly #clientId: string;
  readonly #graph: ResponseGraph;
  readonly #semaphore: AsyncSemaphore;
  readonly #namedStreamRefs = new Map<string, number>();
  readonly #laneTails = new Map<string, Promise<void>>();
  readonly #controllers = new Set<AbortController>();
  #activeResponses = 0;
  readonly #sendQueue: PendingSend[] = [];
  readonly #telemetry?: WebSocketTelemetry;
  readonly #socket: WebSocket;
  readonly #processLease: WebSocketProcessLease | null;
  readonly #onActivity?: () => void;
  #activeSend: PendingSend | null = null;
  #pendingRequests = 0;
  #pendingRequestBytes = 0;
  #pendingSends = 0;
  #pendingSendBytes = 0;
  #pendingHandoffs = 0;
  #sending = false;
  #closed = false;

  constructor(
    socket: WebSocket,
    options: ResponsesWebSocketSessionOptions,
  ) {
    this.#socket = socket;
    this.#startRelay = options.startRelay;
    this.#onActivity = options.onActivity;
    this.#maxNamedStreams = positiveInteger(
      options.maxNamedStreams ?? config.wsNamedStreams,
      "maxNamedStreams",
    );
    const maxActive = positiveInteger(
      options.maxActive ?? config.wsActiveResponses,
      "maxActive",
    );
    this.#maxPendingRequests = positiveInteger(
      options.maxPendingRequests ?? DEFAULT_MAX_PENDING_REQUESTS,
      "maxPendingRequests",
    );
    this.#maxPendingRequestBytes = positiveInteger(
      options.maxPendingRequestBytes ?? DEFAULT_MAX_PENDING_REQUEST_BYTES,
      "maxPendingRequestBytes",
    );
    this.#maxBufferedBytes = positiveInteger(
      options.maxBufferedBytes ?? MiB,
      "maxBufferedBytes",
    );
    this.#maxSendQueueItems = positiveInteger(
      options.maxSendQueueItems ?? DEFAULT_MAX_SEND_QUEUE_ITEMS,
      "maxSendQueueItems",
    );
    this.#maxSendQueueBytes = positiveInteger(
      options.maxSendQueueBytes ?? DEFAULT_MAX_SEND_QUEUE_BYTES,
      "maxSendQueueBytes",
    );
    this.#sendTimeoutMs = positiveInteger(
      options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
      "sendTimeoutMs",
    );
    this.#authorizationScope = options.authorizationScope;
    if (!isAuthenticatedTenantScope(this.#authorizationScope)) {
      throw new TypeError("authorizationScope must be an AuthenticatedTenantScope");
    }
    this.#clientId = options.clientId ?? `ws-${crypto.randomUUID()}`;
    if (!/^[A-Za-z0-9_.:-]{1,128}$/.test(this.#clientId)) {
      throw new TypeError("clientId must be 1-128 safe characters");
    }
    this.#graph = options.graph ?? new ResponseGraph();
    this.#semaphore = new AsyncSemaphore(
      maxActive,
      this.#maxPendingRequests,
      this.#maxPendingRequestBytes,
    );
    this.#processLease = options.processLease ??
      options.processBudget?.openSession(options.parserReservationBytes ?? 0) ??
      null;
    if ((options.processLease || options.processBudget) && !this.#processLease) {
      throw new WebSocketSessionCapacityError(
        "GhostProv WebSocket session capacity is temporarily exhausted",
      );
    }
    this.#telemetry = options.telemetry;
    this.#telemetry?.sessionOpened();
  }

  receive(raw: string, rawBytes = Buffer.byteLength(raw)): void {
    if (this.#closed) return;
    this.#onActivity?.();
    let event: ResponseCreateEvent;
    try {
      event = JSON.parse(raw);
    } catch {
      this.#sendWithoutWaiting(errorEvent("invalid_json", "Client event must be valid JSON"));
      return;
    }
    const validation = this.#validate(event);
    if (validation) {
      this.#sendWithoutWaiting(validation);
      return;
    }

    const bytes = Math.max(0, Math.floor(rawBytes));
    if (!this.#reserveRequest(bytes)) {
      this.#telemetry?.requestRejected();
      this.#sendWithoutWaiting(errorEvent(
        "websocket_request_queue_full",
        "This WebSocket connection has reached its pending request limit.",
        {
          streamId: event.stream_id,
          status: 503,
          type: "overloaded_error",
        },
      ));
      return;
    }

    const lane = event.stream_id ?? "";
    if (event.stream_id) {
      this.#namedStreamRefs.set(
        event.stream_id,
        (this.#namedStreamRefs.get(event.stream_id) ?? 0) + 1,
      );
    }
    const controller = new AbortController();
    this.#controllers.add(controller);
    const previous = this.#laneTails.get(lane) ?? Promise.resolve();
    const task = previous
      .catch(() => undefined)
      .then(() => this.#execute(event, controller, bytes))
      .finally(() => {
        this.#controllers.delete(controller);
        if (event.stream_id) {
          const remaining = (this.#namedStreamRefs.get(event.stream_id) ?? 1) - 1;
          if (remaining <= 0) this.#namedStreamRefs.delete(event.stream_id);
          else this.#namedStreamRefs.set(event.stream_id, remaining);
        }
      });
    this.#laneTails.set(lane, task);
    void task.finally(() => {
      if (this.#laneTails.get(lane) === task) this.#laneTails.delete(lane);
    });
  }

  close(cause: WebSocketCloseCause = "remote_close"): void {
    this.#shutdown(
      new WebSocketDeliveryError(
        "WebSocket connection closed",
        "websocket_closed",
      ),
      cause,
    );
  }

  isIdle(): boolean {
    return this.#pendingRequests === 0 &&
      this.#pendingSends === 0 &&
      this.#controllers.size === 0;
  }

  #validate(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return errorEvent("invalid_request", "Client event must be a JSON object");
    }
    const event = value as ResponseCreateEvent;
    const streamId = typeof event.stream_id === "string" ? event.stream_id : undefined;
    if (event.type !== "response.create") {
      return errorEvent("invalid_event_type", "Only response.create events are supported", {
        streamId,
        param: "type",
      });
    }
    if (event.stream_id !== undefined && !isNamedStreamId(event.stream_id)) {
      return errorEvent(
        "invalid_stream_id",
        "stream_id must be 1-256 letters, numbers, underscores, hyphens, or periods",
        { streamId, param: "stream_id" },
      );
    }
    if (event.background != null) {
      return errorEvent(
        "unsupported_parameter",
        "background is not supported in WebSocket mode",
        { streamId, param: "background" },
      );
    }
    if (event.stream !== undefined && event.stream !== true) {
      return errorEvent(
        "unsupported_parameter",
        "stream:false is incompatible with implicit WebSocket streaming",
        { streamId, param: "stream" },
      );
    }
    if (
      streamId &&
      !this.#namedStreamRefs.has(streamId) &&
      this.#namedStreamRefs.size >= this.#maxNamedStreams
    ) {
      return errorEvent(
        "websocket_stream_limit_reached",
        "This WebSocket connection has reached its stream limit.",
        { streamId, param: "stream_id" },
      );
    }
    return null;
  }

  async #execute(
    event: ResponseCreateEvent,
    controller: AbortController,
    requestBytes: number,
  ): Promise<void> {
    let release: (() => void) | undefined;
    let handle: RunHandle | undefined;
    let eventIterator: AsyncGenerator<import("../relay/sse").SseFrame, void> | undefined;
    let eventIteratorFinished = false;
    let resolved: ResolvedRelayRequest | undefined;
    let pendingTerminal: Record<string, unknown> | undefined;
    let pendingHandoffCleanup: Promise<void> | null = null;
    let finalObserved = false;
    let responseStarted = false;
    let requestReleased = false;
    const releaseRequest = () => {
      if (requestReleased) return;
      requestReleased = true;
      this.#releaseRequest(requestBytes);
    };
    try {
      release = await this.#semaphore.acquire(controller.signal, requestBytes);
      if (this.#closed || controller.signal.aborted) throw abortError();
      this.#activeResponses++;
      responseStarted = true;
      this.#telemetry?.responseStarted(requestBytes);
      if (event.generate === false) {
        await this.#completeWarmup(event);
        return;
      }
      const streamId = event.stream_id;
      const resolution = this.#graph.resolve(event, {
        authorizationScope: this.#authorizationScope,
        inputOwnership: "owned",
      });
      resolved = resolution;
      let relayBody: Record<string, unknown> = resolution.relayBody;
      // The relay body now owns the parsed input reference; avoid retaining a
      // second path through the client event for the streamed response.
      delete event.input;
      this.#pendingHandoffs++;
      let handoffAccountingHeld = true;
      const finishHandoffAccounting = () => {
        if (!handoffAccountingHeld) return;
        handoffAccountingHeld = false;
        this.#pendingHandoffs = Math.max(0, this.#pendingHandoffs - 1);
        this.#maybeCloseProcessLease();
      };
      const releaseHandoffPayload = () => {
        try {
          releaseResponseGraphPayload(resolution);
          resolution.relayBody = {};
          relayBody = {};
        } finally {
          finishHandoffAccounting();
        }
      };
      try {
        const structuralBodyBytes = await jsonEncodedByteLengthAsync(relayBody, {
          signal: controller.signal,
        });
        const starting = Promise.resolve().then(() => this.#startRelay(relayBody, {
          signal: controller.signal,
          clientId: this.#clientId,
          authorizationScope: resolution.authorizationScope,
          // Public type/stream metadata normally covers the two enforced policy
          // fields; retain a small conservative allowance for minimal envelopes.
          encodedBodyBytes: resolution.materializedLocally
            ? undefined
              : Math.min(
                Number.MAX_SAFE_INTEGER,
                requestBytes + UPSTREAM_POLICY_HEADROOM_BYTES,
              ),
          structuralBodyBytes,
        }));
        const retainUntilLateCleanup = (late: Promise<RunHandle>) => {
          const lateCleanup = late.then(
            lateHandle => disposeLateHandle(lateHandle),
            () => undefined,
          ).catch(() => undefined);
          pendingHandoffCleanup = lateCleanup.then(
            releaseHandoffPayload,
            releaseHandoffPayload,
          );
        };
        try {
          handle = await abortable(starting, controller.signal);
          if (this.#closed || controller.signal.aborted) {
            const lateHandle = handle;
            handle = undefined;
            retainUntilLateCleanup(Promise.resolve(lateHandle));
            throw abortError();
          }
          releaseHandoffPayload();
        } catch (error) {
          // Cancellation may win while startRelay still owns the exact request
          // graph. Keep every graph pin and process/request byte charged until
          // that handoff settles and any returned handle is fully discarded.
          if (pendingHandoffCleanup === null) retainUntilLateCleanup(starting);
          throw error;
        }
      } finally {
        // A synchronous sizing failure occurred before startRelay could own the
        // request. There is no late handoff to await in that case.
        if (handoffAccountingHeld && pendingHandoffCleanup === null) {
          releaseHandoffPayload();
        }
      }
      let sawTerminal = false;
      eventIterator = handle.events();
      for (;;) {
        const next = await abortable(
          Promise.resolve().then(() => eventIterator!.next()),
          controller.signal,
          late => {
            if (!late.done) releaseAtomicBytePermit(late.value);
          },
        );
        if (next.done) {
          eventIteratorFinished = true;
          break;
        }
        const frame = next.value;
        let outbound: Record<string, unknown>;
        try {
          outbound = this.#graph.sanitizeOutbound(
            parseEventData(frame.data),
            resolution,
          );
        } catch (error) {
          releaseAtomicBytePermit(frame);
          throw error;
        }
        if (streamId) outbound.stream_id = streamId;
        const type = outbound.type;
        if (
          type === "response.completed" ||
          type === "response.failed" ||
          type === "response.incomplete"
        ) {
          sawTerminal = true;
          releaseAtomicBytePermit(pendingTerminal);
          transferAtomicBytePermit(frame, outbound, "websocket-terminal-pending");
          pendingTerminal = outbound;
          // A Responses terminal is the final logical stream event. Do not
          // request another iterator item before awaiting authoritative final;
          // a generic wrapper may otherwise stall forever after its terminal.
          break;
        }
        transferAtomicBytePermit(frame, outbound, "websocket-outbound");
        await this.#send(outbound);
      }
      const final = await abortable(Promise.resolve(handle.final), controller.signal);
      finalObserved = true;
      try {
        if (this.#closed || controller.signal.aborted) return;
        if (pendingTerminal) {
          if (!terminalFrameMatchesFinal(pendingTerminal, final)) {
            throw new WebSocketRequestOutputError(
              "WebSocket terminal event conflicts with its authoritative final response",
              "websocket_terminal_mismatch",
              500,
              "internal_error",
            );
          }
          const outbound = {
            ...pendingTerminal,
            type: terminalEventType(final),
            response: this.#graph.sanitizeOutbound(final, resolution),
          };
          transferAtomicBytePermit(
            pendingTerminal,
            outbound,
            "websocket-authoritative-terminal",
          );
          pendingTerminal = undefined;
          await this.#send(outbound);
        } else if (!sawTerminal) {
          const outbound = this.#graph.sanitizeOutbound({
            type: terminalEventType(final),
            response: final,
            ...(streamId ? { stream_id: streamId } : {}),
          }, resolution);
          transferAtomicBytePermit(final, outbound, "websocket-final-outbound");
          await this.#send(outbound);
        }
      } finally {
        // No-op when graph/send took ownership; releases final-only carriers.
        releaseAtomicBytePermit(final);
      }
    } catch (error: any) {
      if (
        this.#closed ||
        error?.name === "AbortError" ||
        error instanceof WebSocketDeliveryError
      ) return;
      const streamId = event.stream_id;
      if (error instanceof ResponseGraphCapacityError) {
        this.#telemetry?.responseGraphRejected();
      }
      if (!controller.signal.aborted) controller.abort(error);
      // Release the request/body reservation before queuing its terminal error,
      // otherwise a full process byte budget could deadlock error delivery.
      releaseRequest();
      const output = error instanceof PreviousResponseNotFoundError
        ? errorEvent(error.code, error.message, {
            streamId,
            param: "previous_response_id",
          })
        : error instanceof ResponseGraphCapacityError
          ? errorEvent(error.code, error.message, {
              streamId,
              status: 503,
              type: "overloaded_error",
            })
        : error instanceof WebSocketQueueFullError
          ? errorEvent(error.code, error.message, {
              streamId,
              status: 503,
              type: "overloaded_error",
            })
          : error instanceof WebSocketRequestOutputError
            ? errorEvent(error.code, error.message, {
                streamId,
                status: error.status,
                type: error.type,
              })
            : normalizedErrorEvent(error, streamId);
      try {
        await this.#sendWhenAvailable(output);
      } catch (sendError) {
        if (sendError instanceof WebSocketDeliveryError) {
          this.#failDelivery(sendError);
        }
      }
    } finally {
      if (pendingHandoffCleanup) await pendingHandoffCleanup;
      if (handle && !finalObserved) {
        // A socket/send failure can leave the iterator before it reaches
        // handle.final. Release a definitive late terminal carrier even when
        // the relay wins the cancellation race and settles successfully.
        void handle.final.then(releaseAtomicBytePermit, () => undefined);
      }
      if (eventIterator && !eventIteratorFinished) {
        try { void Promise.resolve(eventIterator.return(undefined)).catch(() => undefined); }
        catch {}
      }
      releaseAtomicBytePermit(pendingTerminal);
      if (resolved) releaseResponseGraphResolution(resolved);
      release?.();
      if (responseStarted) {
        this.#activeResponses = Math.max(0, this.#activeResponses - 1);
        this.#telemetry?.responseFinished(requestBytes);
      }
      releaseRequest();
    }
  }

  async #completeWarmup(event: ResponseCreateEvent): Promise<void> {
    const id = `${SYNTHETIC_WARMUP_ID_PREFIX}${crypto.randomUUID().replaceAll("-", "")}`;
    const final = {
      id,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      status: "completed",
      model: event.model ?? null,
      output: [],
      store: event.store ?? true,
      ...(Object.hasOwn(event, "previous_response_id")
        ? { previous_response_id: event.previous_response_id ?? null }
        : {}),
    };
    let recorded = false;
    try {
      await this.#send({
        type: "response.completed",
        response: final,
        ...(event.stream_id ? { stream_id: event.stream_id } : {}),
      }, () => {
        this.#graph.recordWarmup(id, event, {
          authorizationScope: this.#authorizationScope,
          inputOwnership: "owned",
        });
        recorded = true;
      });
    } catch (error) {
      if (recorded) this.#graph.evict(id);
      throw error;
    }
  }

  #reserveRequest(bytes: number): boolean {
    if (
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      this.#pendingRequests >= this.#maxPendingRequests ||
      this.#pendingRequestBytes + bytes > this.#maxPendingRequestBytes ||
      (this.#processLease && !this.#processLease.tryReserve(bytes))
    ) return false;
    this.#pendingRequests++;
    this.#pendingRequestBytes += bytes;
    this.#telemetry?.requestQueued(bytes);
    return true;
  }

  #releaseRequest(bytes: number): void {
    this.#pendingRequests = Math.max(0, this.#pendingRequests - 1);
    this.#pendingRequestBytes = Math.max(0, this.#pendingRequestBytes - bytes);
    this.#processLease?.release(bytes);
    this.#telemetry?.requestSettled(bytes);
  }

  #maybeCloseProcessLease(): void {
    if (!this.#closed || this.#pendingHandoffs !== 0) return;
    this.#processLease?.close();
  }

  #sendWithoutWaiting(value: unknown): void {
    void this.#send(value).catch(error => {
      if (error instanceof WebSocketDeliveryError) this.#failDelivery(error);
      // Local output admission can reject a validation/overload diagnostic.
      // Dropping that diagnostic is safer than terminating unrelated relays.
    });
  }

  #send(value: unknown, beforeEnqueue?: () => void): Promise<void> {
    if (this.#closed) return Promise.reject(new WebSocketDeliveryError(
      "WebSocket connection is closed",
      "websocket_closed",
    ));
    let encoded: { data: string; bytes: number };
    try {
      encoded = this.#encodeSend(value);
      transferAtomicBytePermit(value, encoded, "websocket-encoded");
    } catch (error) {
      releaseAtomicBytePermit(value);
      return Promise.reject(error);
    }
    return this.#enqueueSend(encoded, beforeEnqueue);
  }

  async #sendWhenAvailable(value: unknown): Promise<void> {
    if (this.#closed) throw new WebSocketDeliveryError(
      "WebSocket connection is closed",
      "websocket_closed",
    );
    let encoded: { data: string; bytes: number };
    try {
      encoded = this.#encodeSend(value);
      transferAtomicBytePermit(value, encoded, "websocket-encoded");
    } catch (error) {
      releaseAtomicBytePermit(value);
      throw error;
    }
    if (encoded.bytes > this.#maxSendQueueBytes) {
      releaseAtomicBytePermit(encoded);
      throw new WebSocketRequestOutputError(
        "WebSocket diagnostic exceeds its configured output limit",
        "websocket_response_too_large",
        500,
        "internal_error",
      );
    }
    const deadline = performance.now() + this.#sendTimeoutMs;
    while (!this.#hasSendCapacity(encoded.bytes)) {
      if (this.#closed || this.#socket.readyState !== this.#socket.OPEN) {
        releaseAtomicBytePermit(encoded);
        throw new WebSocketDeliveryError(
          "WebSocket closed before response delivery",
          "websocket_closed",
        );
      }
      const remaining = deadline - performance.now();
      if (remaining <= 0) {
        releaseAtomicBytePermit(encoded);
        throw new WebSocketDeliveryError(
          "WebSocket output queue remained full past its delivery deadline",
          "websocket_backpressure_timeout",
        );
      }
      await new Promise(resolve => setTimeout(resolve, Math.min(SOCKET_POLL_MS, remaining)));
    }
    return this.#enqueueSend(encoded);
  }

  #encodeSend(value: unknown): { data: string; bytes: number } {
    let data: string;
    try {
      data = JSON.stringify(value);
    } catch (cause) {
      throw new WebSocketRequestOutputError(
        "WebSocket response is not serializable",
        "websocket_response_encoding",
        500,
        "internal_error",
        { cause },
      );
    }
    return { data, bytes: Buffer.byteLength(data) };
  }

  #hasSendCapacity(bytes: number): boolean {
    return bytes <= this.#maxSendQueueBytes &&
      this.#pendingSends < this.#maxSendQueueItems &&
      this.#pendingSendBytes + bytes <= this.#maxSendQueueBytes &&
      (!this.#processLease || this.#processLease.canReserve(bytes));
  }

  #enqueueSend(
    encoded: { data: string; bytes: number },
    beforeEnqueue?: () => void,
  ): Promise<void> {
    if (!this.#hasSendCapacity(encoded.bytes)) {
      releaseAtomicBytePermit(encoded);
      this.#telemetry?.sendRejected();
      return Promise.reject(new WebSocketRequestOutputError(
        "WebSocket output queue exceeded its configured limit",
        "websocket_output_queue_full",
        503,
        "overloaded_error",
      ));
    }
    const processReserved = this.#processLease?.tryReserve(encoded.bytes) ?? true;
    if (!processReserved) {
      releaseAtomicBytePermit(encoded);
      this.#telemetry?.sendRejected();
      return Promise.reject(new WebSocketRequestOutputError(
        "WebSocket output queue exceeded its configured limit",
        "websocket_output_queue_full",
        503,
        "overloaded_error",
      ));
    }
    try {
      beforeEnqueue?.();
    } catch (error) {
      this.#processLease?.release(encoded.bytes);
      releaseAtomicBytePermit(encoded);
      return Promise.reject(error);
    }
    return new Promise<void>((resolve, reject) => {
      const item: PendingSend = {
        data: encoded.data,
        bytes: encoded.bytes,
        enqueuedAt: performance.now(),
        resolve,
        reject,
        started: false,
        settled: false,
      };
      transferAtomicBytePermit(encoded, item, "websocket-send-queue");
      this.#pendingSends++;
      this.#pendingSendBytes += encoded.bytes;
      this.#telemetry?.sendQueued(encoded.bytes);
      this.#sendQueue.push(item);
      this.#pumpSends();
    });
  }

  #pumpSends(): void {
    if (this.#sending || this.#closed) return;
    this.#sending = true;
    void this.#drainSends().finally(() => {
      this.#sending = false;
      if (!this.#closed && this.#sendQueue.length > 0) this.#pumpSends();
    });
  }

  async #drainSends(): Promise<void> {
    while (!this.#closed && this.#sendQueue.length > 0) {
      const item = this.#sendQueue.shift()!;
      this.#activeSend = item;
      item.started = true;
      this.#telemetry?.sendStarted(item.bytes);
      const deadline = item.enqueuedAt + this.#sendTimeoutMs;
      try {
        const delayed = await this.#waitForSocketCapacity(item.bytes, deadline);
        const callbackStarted = performance.now();
        await this.#sendEncoded(item.data, deadline);
        this.#telemetry?.recordSend({
          queueMs: Math.max(0, callbackStarted - item.enqueuedAt),
          callbackMs: Math.max(0, performance.now() - callbackStarted),
          bufferedBytes: this.#socket.bufferedAmount,
          delayed,
        });
        this.#onActivity?.();
        this.#settleSend(item);
      } catch (cause) {
        const error = cause instanceof WebSocketDeliveryError
          ? cause
          : new WebSocketDeliveryError(
              "WebSocket response delivery failed",
              "websocket_delivery_failed",
              { cause },
            );
        this.#settleSend(item, error);
        this.#failDelivery(error);
        return;
      } finally {
        if (this.#activeSend === item) this.#activeSend = null;
      }
    }
  }

  async #waitForSocketCapacity(bytes: number, deadline: number): Promise<boolean> {
    let delayed = false;
    for (;;) {
      if (this.#closed || this.#socket.readyState !== this.#socket.OPEN) {
        throw new WebSocketDeliveryError(
          "WebSocket closed before response delivery",
          "websocket_closed",
        );
      }
      const buffered = Math.max(0, this.#socket.bufferedAmount);
      if (buffered === 0 || buffered + bytes <= this.#maxBufferedBytes) return delayed;
      delayed = true;
      const remaining = deadline - performance.now();
      if (remaining <= 0) {
        throw new WebSocketDeliveryError(
          "WebSocket remained backpressured past its delivery deadline",
          "websocket_backpressure_timeout",
        );
      }
      await new Promise(resolve => setTimeout(resolve, Math.min(SOCKET_POLL_MS, remaining)));
    }
  }

  #sendEncoded(data: string, deadline: number): Promise<void> {
    return new Promise((resolve, reject) => {
      let settled = false;
      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        error ? reject(error) : resolve();
      };
      const remaining = Math.max(1, Math.ceil(deadline - performance.now()));
      const timer = setTimeout(() => finish(new WebSocketDeliveryError(
        "WebSocket send callback timed out",
        "websocket_send_timeout",
      )), remaining);
      timer.unref?.();
      try {
        this.#socket.send(data, error => finish(error ?? undefined));
      } catch (error) {
        finish(error);
      }
    });
  }

  #settleSend(item: PendingSend, error?: unknown): void {
    if (item.settled) return;
    item.settled = true;
    this.#pendingSends = Math.max(0, this.#pendingSends - 1);
    this.#pendingSendBytes = Math.max(0, this.#pendingSendBytes - item.bytes);
    this.#processLease?.release(item.bytes);
    releaseAtomicBytePermit(item);
    this.#telemetry?.sendSettled(item.bytes, item.started);
    error ? item.reject(error) : item.resolve();
  }

  #failDelivery(cause: unknown, closeCode = 1011): void {
    if (this.#closed) return;
    const error = cause instanceof WebSocketDeliveryError
      ? cause
      : new WebSocketDeliveryError(
          "WebSocket response delivery failed",
          "websocket_delivery_failed",
          { cause },
        );
    this.#telemetry?.deliveryFailed();
    this.#shutdown(error, "delivery_error");
    try {
      if (
        this.#socket.readyState === this.#socket.OPEN ||
        this.#socket.readyState === this.#socket.CONNECTING
      ) {
        this.#socket.close(closeCode, "GhostProv WebSocket delivery failed");
      }
    } catch {
      try { this.#socket.terminate(); } catch {}
    }
  }

  #shutdown(
    error: WebSocketDeliveryError,
    cause: WebSocketCloseCause,
  ): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#telemetry?.sessionClosed(cause, this.#activeResponses);
    for (const controller of this.#controllers) controller.abort();
    this.#controllers.clear();
    this.#semaphore.close();
    this.#graph.clear();
    this.#laneTails.clear();
    this.#namedStreamRefs.clear();
    if (this.#activeSend) this.#settleSend(this.#activeSend, error);
    for (const item of this.#sendQueue.splice(0)) this.#settleSend(item, error);
    this.#maybeCloseProcessLease();
  }
}
