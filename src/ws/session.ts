import type { WebSocket } from "ws";
import type { RunHandle } from "../relay/run-handle";
import { config } from "../config";
import { normalizeApiError } from "../http/errors";
import {
  PreviousResponseNotFoundError,
  ResponseGraph,
  type ResponseCreateEvent,
} from "./response-graph";
import type { WebSocketTelemetry } from "./telemetry";

const MiB = 1024 * 1024;
const DEFAULT_MAX_PENDING_REQUESTS = 128;
const DEFAULT_MAX_PENDING_REQUEST_BYTES = 64 * MiB;
const DEFAULT_MAX_SEND_QUEUE_ITEMS = 512;
const DEFAULT_MAX_SEND_QUEUE_BYTES = 64 * MiB;
const DEFAULT_SEND_TIMEOUT_MS = 30_000;
const SOCKET_POLL_MS = 5;

export interface WebSocketProcessBudgetOptions {
  maxSessions: number;
  maxQueuedBytes: number;
}

export interface WebSocketProcessBudgetSnapshot {
  sessions: number;
  queuedBytes: number;
  maxSessions: number;
  maxQueuedBytes: number;
}

interface WebSocketProcessLease {
  canReserve(bytes: number): boolean;
  tryReserve(bytes: number): boolean;
  release(bytes: number): void;
  close(): void;
}

/** Shared process-level fence across every WebSocket server/session. */
export class WebSocketProcessBudget {
  readonly #maxSessions: number;
  readonly #maxQueuedBytes: number;
  #sessions = 0;
  #queuedBytes = 0;

  constructor(options: WebSocketProcessBudgetOptions) {
    this.#maxSessions = positiveInteger(options.maxSessions, "maxSessions");
    this.#maxQueuedBytes = positiveInteger(
      options.maxQueuedBytes,
      "maxQueuedBytes",
    );
  }

  openSession(): WebSocketProcessLease | null {
    if (this.#sessions >= this.#maxSessions) return null;
    this.#sessions++;
    let closed = false;
    let reserved = 0;
    return {
      canReserve: bytes => !closed && this.#canReserve(bytes),
      tryReserve: bytes => {
        if (closed || !this.#canReserve(bytes)) return false;
        const value = Math.max(0, Math.floor(bytes));
        reserved += value;
        this.#queuedBytes += value;
        return true;
      },
      release: bytes => {
        if (closed) return;
        const value = Math.min(reserved, Math.max(0, Math.floor(bytes)));
        reserved -= value;
        this.#queuedBytes = Math.max(0, this.#queuedBytes - value);
      },
      close: () => {
        if (closed) return;
        closed = true;
        this.#queuedBytes = Math.max(0, this.#queuedBytes - reserved);
        reserved = 0;
        this.#sessions = Math.max(0, this.#sessions - 1);
      },
    };
  }

  snapshot(): WebSocketProcessBudgetSnapshot {
    return {
      sessions: this.#sessions,
      queuedBytes: this.#queuedBytes,
      maxSessions: this.#maxSessions,
      maxQueuedBytes: this.#maxQueuedBytes,
    };
  }

  #canReserve(bytes: number): boolean {
    return Number.isSafeInteger(bytes) && bytes >= 0 &&
      this.#queuedBytes + bytes <= this.#maxQueuedBytes;
  }
}

export type WebSocketRelayStarter = (
  body: Record<string, unknown>,
  options: { signal: AbortSignal; clientId: string },
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
  /** Stable routing identity reused by every response on this connection. */
  clientId?: string;
  /** Shared process-level session and retained-byte fence. */
  processBudget?: WebSocketProcessBudget;
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

export class ResponsesWebSocketSession {
  readonly #startRelay: WebSocketRelayStarter;
  readonly #maxNamedStreams: number;
  readonly #maxPendingRequests: number;
  readonly #maxPendingRequestBytes: number;
  readonly #maxBufferedBytes: number;
  readonly #maxSendQueueItems: number;
  readonly #maxSendQueueBytes: number;
  readonly #sendTimeoutMs: number;
  readonly #clientId: string;
  readonly #graph: ResponseGraph;
  readonly #semaphore: AsyncSemaphore;
  readonly #namedStreamRefs = new Map<string, number>();
  readonly #laneTails = new Map<string, Promise<void>>();
  readonly #controllers = new Set<AbortController>();
  readonly #sendQueue: PendingSend[] = [];
  readonly #telemetry?: WebSocketTelemetry;
  readonly #socket: WebSocket;
  readonly #processLease: WebSocketProcessLease | null;
  #activeSend: PendingSend | null = null;
  #pendingRequests = 0;
  #pendingRequestBytes = 0;
  #pendingSends = 0;
  #pendingSendBytes = 0;
  #sending = false;
  #closed = false;

  constructor(
    socket: WebSocket,
    options: ResponsesWebSocketSessionOptions,
  ) {
    this.#socket = socket;
    this.#startRelay = options.startRelay;
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
    this.#processLease = options.processBudget?.openSession() ?? null;
    if (options.processBudget && !this.#processLease) {
      throw new WebSocketSessionCapacityError(
        "GhostProv WebSocket session capacity is temporarily exhausted",
      );
    }
    this.#telemetry = options.telemetry;
    this.#telemetry?.sessionOpened();
  }

  receive(raw: string, rawBytes = Buffer.byteLength(raw)): void {
    if (this.#closed) return;
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

  close(): void {
    this.#shutdown(new WebSocketDeliveryError(
      "WebSocket connection closed",
      "websocket_closed",
    ));
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
    let requestReleased = false;
    const releaseRequest = () => {
      if (requestReleased) return;
      requestReleased = true;
      this.#releaseRequest(requestBytes);
    };
    try {
      release = await this.#semaphore.acquire(controller.signal, requestBytes);
      if (this.#closed || controller.signal.aborted) throw abortError();
      this.#telemetry?.responseStarted(requestBytes);
      if (event.generate === false) {
        await this.#completeWarmup(event);
        return;
      }
      const resolved = this.#graph.resolve(event);
      const handle = await this.#startRelay(resolved.relayBody, {
        signal: controller.signal,
        clientId: this.#clientId,
      });
      let sawTerminal = false;
      for await (const frame of handle.events()) {
        const outbound = this.#graph.sanitizeOutbound(
          parseEventData(frame.data),
          resolved,
        );
        if (event.stream_id) outbound.stream_id = event.stream_id;
        const type = outbound.type;
        if (
          type === "response.completed" ||
          type === "response.failed" ||
          type === "response.incomplete"
        ) {
          sawTerminal = true;
        }
        await this.#send(outbound);
      }
      const final = await handle.final;
      if (this.#closed || controller.signal.aborted) return;
      this.#graph.record(resolved, final);
      if (!sawTerminal) {
        await this.#send(this.#graph.sanitizeOutbound({
          type: "response.completed",
          response: final,
          ...(event.stream_id ? { stream_id: event.stream_id } : {}),
        }, resolved));
      }
    } catch (error: any) {
      if (
        this.#closed ||
        error?.name === "AbortError" ||
        error instanceof WebSocketDeliveryError
      ) return;
      const streamId = event.stream_id;
      if (error instanceof WebSocketRequestOutputError) controller.abort();
      // Release the request/body reservation before queuing its terminal error,
      // otherwise a full process byte budget could deadlock error delivery.
      releaseRequest();
      const output = error instanceof PreviousResponseNotFoundError
        ? errorEvent(error.code, error.message, {
            streamId,
            param: "previous_response_id",
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
      release?.();
      if (release) this.#telemetry?.responseFinished(requestBytes);
      releaseRequest();
    }
  }

  async #completeWarmup(event: ResponseCreateEvent): Promise<void> {
    const id = `resp_gp_warmup_${crypto.randomUUID().replaceAll("-", "")}`;
    const resolved = this.#graph.resolve({ ...event, generate: true });
    const final = {
      id,
      object: "response",
      created_at: Math.floor(Date.now() / 1000),
      status: "completed",
      model: event.model ?? null,
      output: [],
      store: event.store ?? true,
    };
    this.#graph.record(resolved, final);
    await this.#send(this.#graph.sanitizeOutbound({
      type: "response.completed",
      response: final,
      ...(event.stream_id ? { stream_id: event.stream_id } : {}),
    }, resolved));
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

  #sendWithoutWaiting(value: unknown): void {
    void this.#send(value).catch(error => {
      if (error instanceof WebSocketDeliveryError) this.#failDelivery(error);
      // Local output admission can reject a validation/overload diagnostic.
      // Dropping that diagnostic is safer than terminating unrelated relays.
    });
  }

  #send(value: unknown): Promise<void> {
    if (this.#closed) return Promise.reject(new WebSocketDeliveryError(
      "WebSocket connection is closed",
      "websocket_closed",
    ));
    let encoded: { data: string; bytes: number };
    try {
      encoded = this.#encodeSend(value);
    } catch (error) {
      return Promise.reject(error);
    }
    return this.#enqueueSend(encoded);
  }

  async #sendWhenAvailable(value: unknown): Promise<void> {
    if (this.#closed) throw new WebSocketDeliveryError(
      "WebSocket connection is closed",
      "websocket_closed",
    );
    const encoded = this.#encodeSend(value);
    if (encoded.bytes > this.#maxSendQueueBytes) {
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
        throw new WebSocketDeliveryError(
          "WebSocket closed before response delivery",
          "websocket_closed",
        );
      }
      const remaining = deadline - performance.now();
      if (remaining <= 0) {
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

  #enqueueSend(encoded: { data: string; bytes: number }): Promise<void> {
    if (
      !this.#hasSendCapacity(encoded.bytes) ||
      (this.#processLease && !this.#processLease.tryReserve(encoded.bytes))
    ) {
      this.#telemetry?.sendRejected();
      return Promise.reject(new WebSocketRequestOutputError(
        "WebSocket output queue exceeded its configured limit",
        "websocket_output_queue_full",
        503,
        "overloaded_error",
      ));
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
    this.#shutdown(error);
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

  #shutdown(error: WebSocketDeliveryError): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#telemetry?.sessionClosed();
    for (const controller of this.#controllers) controller.abort();
    this.#controllers.clear();
    this.#semaphore.close();
    this.#graph.clear();
    this.#laneTails.clear();
    this.#namedStreamRefs.clear();
    if (this.#activeSend) this.#settleSend(this.#activeSend, error);
    for (const item of this.#sendQueue.splice(0)) this.#settleSend(item, error);
    this.#processLease?.close();
  }
}
