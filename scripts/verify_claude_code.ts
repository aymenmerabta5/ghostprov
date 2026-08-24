import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import type { FetchLike } from "../src/core/ports";
import { ApiError } from "../src/http/errors";
import { RelayError, type RunHandle } from "../src/relay/run-handle";
import { parseFrames, sseFrame, type SseFrame } from "../src/relay/sse";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";

const DEFAULT_PORT = 18_788;
const DEFAULT_UPSTREAM = "http://127.0.0.1:18787";
const PROTECTED_LISTENER_PORTS = new Set([18_787, 8_787]);
const DEFAULT_MAX_BUFFERED_BYTES = 1024 * 1024;
const DEFAULT_MAX_EVENT_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAX_READ_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_INCOMING_BODY_BUDGET_BYTES = 128 * 1024 * 1024;
const DEFAULT_MAX_PARSER_SEGMENTS = 1024;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 1_000;
const HARD_MAX_QUEUED_EVENT_BYTES = 1024 * 1024;
const VERIFIER_CAPACITY = 128;
const TERMINAL_STATUSES = new Map<string, "completed" | "failed" | "incomplete">([
  ["response.completed", "completed"],
  ["response.failed", "failed"],
  ["response.incomplete", "incomplete"],
]);

export interface VerifierOptions {
  port: number;
  upstream: string;
}

export interface ForwardingRouter extends RouterLike {
  close(): void;
}

export interface ForwardingRouterDependencies {
  fetch?: FetchLike;
  maxBufferedBytes?: number;
  maxEventBytes?: number;
  maxReadChunkBytes?: number;
  maxIncomingBodyBytes?: number;
  maxParserSegments?: number;
}

class ForwardingProtocolError extends Error {
  readonly name = "ForwardingProtocolError";
}

function positiveSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
  return value;
}

interface ForwardWaiter {
  resolve: (lease: { release(): void }) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

function verifierClosingError(): ApiError {
  return new ApiError(503, "Claude Code verifier is shutting down", {
    type: "service_unavailable_error",
    code: "verifier_shutting_down",
  });
}

class ForwardSemaphore {
  readonly #capacity: number;
  readonly #waiters: ForwardWaiter[] = [];
  #active = 0;
  #closed = false;

  constructor(capacity: number) {
    this.#capacity = positiveSafeInteger(capacity, "forward capacity");
  }

  acquire(signal?: AbortSignal): Promise<{ release(): void }> {
    if (signal?.aborted) return Promise.reject(abortError());
    if (this.#closed) return Promise.reject(verifierClosingError());
    if (this.#active < this.#capacity && this.#waiters.length === 0) {
      return Promise.resolve(this.#grant());
    }
    return new Promise((resolve, reject) => {
      const waiter: ForwardWaiter = { resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#waiters.indexOf(waiter);
        if (index < 0) return;
        this.#waiters.splice(index, 1);
        reject(abortError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#waiters.push(waiter);
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.reject(verifierClosingError());
    }
  }

  snapshot(): { active: number; queued: number } {
    return { active: this.#active, queued: this.#waiters.length };
  }

  #grant(): { release(): void } {
    this.#active += 1;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#active = Math.max(0, this.#active - 1);
        this.#drain();
      },
    };
  }

  #drain(): void {
    if (this.#closed) return;
    while (this.#active < this.#capacity) {
      const waiter = this.#waiters.shift();
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        waiter.reject(abortError());
        continue;
      }
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(this.#grant());
    }
  }
}

interface BodyWaiter {
  bytes: number;
  resolve: (lease: { release(): void }) => void;
  reject: (error: unknown) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

class IncomingBodyBudget {
  readonly #capacityBytes: number;
  readonly #waiters: BodyWaiter[] = [];
  #reservedBytes = 0;
  #closed = false;

  constructor(capacityBytes: number) {
    this.#capacityBytes = positiveSafeInteger(capacityBytes, "incoming body budget");
  }

  acquire(bytes: number, signal?: AbortSignal): Promise<{ release(): void }> {
    if (!Number.isSafeInteger(bytes) || bytes < 0) {
      return Promise.reject(new ApiError(400, "Incoming body reservation is invalid", {
        type: "invalid_request_error",
        code: "invalid_body_reservation",
      }));
    }
    if (bytes > this.#capacityBytes) {
      return Promise.reject(new ApiError(413, "Incoming body exceeds verifier memory budget", {
        type: "invalid_request_error",
        code: "request_too_large",
      }));
    }
    if (signal?.aborted) return Promise.reject(abortError());
    if (this.#closed) return Promise.reject(verifierClosingError());
    if (bytes === 0 || (this.#waiters.length === 0 && this.#fits(bytes))) {
      return Promise.resolve(this.#grant(bytes));
    }
    return new Promise((resolve, reject) => {
      const waiter: BodyWaiter = { bytes, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.#waiters.indexOf(waiter);
        if (index < 0) return;
        this.#waiters.splice(index, 1);
        reject(abortError());
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.#waiters.push(waiter);
    });
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const waiter of this.#waiters.splice(0)) {
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.reject(verifierClosingError());
    }
  }

  snapshot(): { capacityBytes: number; reservedBytes: number; queued: number } {
    return {
      capacityBytes: this.#capacityBytes,
      reservedBytes: this.#reservedBytes,
      queued: this.#waiters.length,
    };
  }

  #fits(bytes: number): boolean {
    return this.#reservedBytes + bytes <= this.#capacityBytes;
  }

  #grant(bytes: number): { release(): void } {
    this.#reservedBytes += bytes;
    let released = false;
    return {
      release: () => {
        if (released) return;
        released = true;
        this.#reservedBytes = Math.max(0, this.#reservedBytes - bytes);
        this.#drain();
      },
    };
  }

  #drain(): void {
    if (this.#closed) return;
    for (;;) {
      const waiter = this.#waiters[0];
      if (!waiter) return;
      if (waiter.signal?.aborted) {
        this.#waiters.shift();
        waiter.reject(abortError());
        continue;
      }
      if (!this.#fits(waiter.bytes)) return;
      this.#waiters.shift();
      waiter.signal?.removeEventListener("abort", waiter.onAbort!);
      waiter.resolve(this.#grant(waiter.bytes));
    }
  }
}

interface BufferedVerifierFrame {
  frame: SseFrame;
  bytes: number;
}

class VerifierRunHandle implements RunHandle {
  readonly final: Promise<Record<string, unknown>>;
  readonly stats: RunHandle["stats"] = {
    bytes: 0,
    firstByteMs: null,
    frames: 0,
  };
  readonly #maxBufferedBytes: number;
  readonly #onIteratorCancel: () => void;
  readonly #startedAt = performance.now();
  readonly #buffer: BufferedVerifierFrame[] = [];
  readonly #eventWaiters = new Set<() => void>();
  readonly #spaceWaiters = new Set<() => void>();
  readonly #resolveFinal: (value: Record<string, unknown>) => void;
  readonly #rejectFinal: (error: unknown) => void;
  #bufferedBytes = 0;
  #closed = false;
  #failure: Error | null = null;
  #eventsClaimed = false;

  constructor(maxBufferedBytes: number, onIteratorCancel: () => void) {
    this.#maxBufferedBytes = Math.min(
      positiveSafeInteger(maxBufferedBytes, "maxBufferedBytes"),
      HARD_MAX_QUEUED_EVENT_BYTES,
    );
    this.#onIteratorCancel = onIteratorCancel;
    let resolveFinal!: (value: Record<string, unknown>) => void;
    let rejectFinal!: (error: unknown) => void;
    this.final = new Promise<Record<string, unknown>>((resolve, reject) => {
      resolveFinal = resolve;
      rejectFinal = reject;
    });
    this.#resolveFinal = resolveFinal;
    this.#rejectFinal = rejectFinal;
    void this.final.catch(() => undefined);
  }

  canAcceptEvent(frame: SseFrame): boolean {
    const bytes = this.#frameBytes(frame);
    return bytes <= HARD_MAX_QUEUED_EVENT_BYTES && bytes <= this.#maxBufferedBytes;
  }

  async acceptEvent(frame: SseFrame): Promise<void> {
    if (!frame || typeof frame.event !== "string" || !frame.event ||
        typeof frame.data !== "string") {
      throw new ForwardingProtocolError("Upstream SSE event is malformed");
    }
    const bytes = this.#frameBytes(frame);
    if (bytes > HARD_MAX_QUEUED_EVENT_BYTES || bytes > this.#maxBufferedBytes) {
      throw new ForwardingProtocolError("Upstream SSE event exceeds verifier queue limit");
    }
    while (!this.#closed && this.#bufferedBytes + bytes > this.#maxBufferedBytes) {
      await new Promise<void>(resolve => this.#spaceWaiters.add(resolve));
    }
    if (this.#closed) throw this.#failure ?? abortError();
    this.#buffer.push({ frame, bytes });
    this.#bufferedBytes += bytes;
    if (this.stats.firstByteMs === null) {
      this.stats.firstByteMs = Math.max(0, performance.now() - this.#startedAt);
    }
    this.stats.bytes += bytes;
    this.stats.frames += 1;
    this.#wake(this.#eventWaiters);
  }

  events(): AsyncGenerator<SseFrame> {
    if (this.#eventsClaimed) throw new Error("Verifier run events may only be consumed once");
    this.#eventsClaimed = true;
    let done = false;
    let cancelled = false;
    const cancel = (): void => {
      if (cancelled) return;
      cancelled = true;
      if (!this.#closed) this.#onIteratorCancel();
    };
    const iterator = {
      next: async (): Promise<IteratorResult<SseFrame>> => {
        if (done) return { value: undefined, done: true };
        const result = await this.#nextEvent();
        if (result.done) done = true;
        return result;
      },
      return: async (_value?: unknown): Promise<IteratorResult<SseFrame>> => {
        cancel();
        done = true;
        return { value: undefined, done: true };
      },
      throw: async (_error?: unknown): Promise<IteratorResult<SseFrame>> => {
        cancel();
        done = true;
        throw abortError();
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
    return iterator as unknown as AsyncGenerator<SseFrame>;
  }

  settle(final: Record<string, unknown>): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#resolveFinal(final);
    this.#wakeAll();
  }

  fail(error: Error): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#failure = error;
    this.#rejectFinal(error);
    this.#wakeAll();
  }

  cancel(error: Error): void {
    this.fail(error);
  }

  #wake(waiters: Set<() => void>): void {
    for (const resolve of waiters) resolve();
    waiters.clear();
  }

  #wakeAll(): void {
    this.#wake(this.#eventWaiters);
    this.#wake(this.#spaceWaiters);
  }

  #frameBytes(frame: SseFrame): number {
    return new TextEncoder().encode(sseFrame(frame.event, frame.data)).byteLength;
  }

  async #nextEvent(): Promise<IteratorResult<SseFrame>> {
    for (;;) {
      const entry = this.#buffer.shift();
      if (entry) {
        this.#bufferedBytes -= entry.bytes;
        this.#wake(this.#spaceWaiters);
        return { value: entry.frame, done: false };
      }
      if (this.#closed) return { value: undefined, done: true };
      await new Promise<void>(resolve => this.#eventWaiters.add(resolve));
    }
  }
}

function effectivePort(url: URL): number {
  if (url.port) return Number(url.port);
  return url.protocol === "https:" ? 443 : 80;
}

function isLoopbackHostname(hostname: string): boolean {
  const host = hostname.toLowerCase().replace(/^\[|\]$/g, "");
  if (host === "localhost" || host === "::1") return true;
  const octets = host.split(".");
  return octets.length === 4 && octets[0] === "127" && octets.every(octet => {
    if (!/^\d{1,3}$/.test(octet)) return false;
    const value = Number(octet);
    return value >= 0 && value <= 255;
  });
}

function hasNonRootRawPath(value: string): boolean {
  const schemeEnd = value.indexOf("://");
  if (schemeEnd < 0) return true;
  const authorityStart = schemeEnd + 3;
  const tail = value.slice(authorityStart);
  const delimiterOffset = tail.search(/[\\/?#]/);
  if (delimiterOffset < 0) return false;
  const delimiter = tail[delimiterOffset];
  if (delimiter === "?" || delimiter === "#") return false;
  if (delimiter === "\\") return true;
  const pathAndSuffix = tail.slice(delimiterOffset);
  const suffixOffset = pathAndSuffix.search(/[?#]/);
  const rawPath = suffixOffset < 0
    ? pathAndSuffix
    : pathAndSuffix.slice(0, suffixOffset);
  return rawPath !== "/";
}

function hasRawUserInfo(value: string): boolean {
  const schemeEnd = value.indexOf("://");
  if (schemeEnd < 0) return false;
  const tail = value.slice(schemeEnd + 3);
  const authorityEnd = tail.search(/[\\/?#]/);
  const authority = authorityEnd < 0 ? tail : tail.slice(0, authorityEnd);
  return authority.includes("@");
}

function validateUpstream(value: string): { upstream: string; url: URL } {
  if (!value || value !== value.trim()) {
    throw new TypeError("Verifier upstream must be a loopback HTTP(S) root");
  }
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new TypeError("Verifier upstream must be a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new TypeError("Verifier upstream must use HTTP or HTTPS");
  }
  if (!isLoopbackHostname(url.hostname)) {
    throw new TypeError("Verifier upstream must use a loopback host");
  }
  if (url.username || url.password || hasRawUserInfo(value)) {
    throw new TypeError("Verifier upstream must not contain credentials");
  }
  if (url.search || url.hash || value.includes("?") || value.includes("#")) {
    throw new TypeError("Verifier upstream must not contain a query or fragment");
  }
  if (url.pathname !== "/" || hasNonRootRawPath(value)) {
    throw new TypeError("Verifier upstream must be an origin without a path");
  }
  return { upstream: url.origin, url };
}

export function validateVerifierOptions(options: VerifierOptions): VerifierOptions {
  if (!Number.isSafeInteger(options.port) || options.port < 1 || options.port > 65_535) {
    throw new RangeError("Verifier listener port must be an integer from 1 through 65535");
  }
  if (PROTECTED_LISTENER_PORTS.has(options.port)) {
    throw new RangeError(`Verifier listener port ${options.port} is protected`);
  }
  const validated = validateUpstream(options.upstream);
  if (effectivePort(validated.url) === options.port) {
    throw new RangeError("Verifier upstream must not target its own listener port");
  }
  return { port: options.port, upstream: validated.upstream };
}

function abortError(): DOMException {
  return new DOMException("Verifier request was cancelled", "AbortError");
}

function upstreamFailure(message = "Verifier upstream request failed"): RelayError {
  return new RelayError(message, "upstream");
}

const MAX_RETRY_AFTER_SECONDS = 86_400;
const HTTP_DATE = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d{2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4} \d{2}:\d{2}:\d{2} GMT$/;

function safeRetryAfter(value: string | null): string | null {
  const candidate = value?.trim();
  if (!candidate) return null;
  if (/^(?:0|[1-9]\d{0,5})$/.test(candidate)) {
    const seconds = Number(candidate);
    return seconds <= MAX_RETRY_AFTER_SECONDS ? String(seconds) : null;
  }
  if (!HTTP_DATE.test(candidate)) return null;
  const timestamp = Date.parse(candidate);
  if (!Number.isFinite(timestamp)) return null;
  const canonical = new Date(timestamp).toUTCString();
  return canonical === candidate ? canonical : null;
}

function safeRetryHeaders(source: Headers): Headers {
  const headers = new Headers();
  const retryAfter = safeRetryAfter(source.get("retry-after"));
  if (retryAfter) headers.set("retry-after", retryAfter);
  const shouldRetry = source.get("x-should-retry")?.trim();
  if (shouldRetry === "true" || shouldRetry === "false") {
    headers.set("x-should-retry", shouldRetry);
  }
  return headers;
}

class ForwardingHttpError extends ApiError {
  constructor(status: number, readonly headers: Headers) {
    super(
      status,
      `Verifier upstream request failed (HTTP ${status})`,
      { type: "upstream_error", code: `upstream_http_${status}` },
    );
    this.name = "ForwardingHttpError";
  }
}

function safeHttpError(response: Response): ApiError {
  const safeStatus = Number.isInteger(response.status) && response.status >= 400 && response.status <= 599
    ? response.status
    : 502;
  return new ForwardingHttpError(safeStatus, safeRetryHeaders(response.headers));
}

function terminalFinal(frame: SseFrame): Record<string, unknown> | null {
  const expectedStatus = TERMINAL_STATUSES.get(frame.event);
  if (!expectedStatus) return null;
  let decoded: unknown;
  try {
    decoded = JSON.parse(frame.data);
  } catch {
    throw new ForwardingProtocolError("Upstream terminal event is malformed");
  }
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    throw new ForwardingProtocolError("Upstream terminal event is malformed");
  }
  const event = decoded as Record<string, unknown>;
  if (event.type !== frame.event || !event.response ||
      typeof event.response !== "object" || Array.isArray(event.response)) {
    throw new ForwardingProtocolError("Upstream terminal event is malformed");
  }
  const final = event.response as Record<string, unknown>;
  if (typeof final.id !== "string" || final.id.length === 0 || final.status !== expectedStatus) {
    throw new ForwardingProtocolError("Upstream terminal event is malformed");
  }
  return final;
}

class BoundedSseParser {
  readonly #maxEventBytes: number;
  readonly #maxReadChunkBytes: number;
  readonly #maxSegments: number;
  #parts: Uint8Array[] = [];
  #eventBytes = 0;
  #lineHasContent = false;
  #pendingCr = false;
  #pendingDispatch = false;

  constructor(maxEventBytes: number, maxReadChunkBytes: number, maxSegments: number) {
    this.#maxEventBytes = maxEventBytes;
    this.#maxReadChunkBytes = maxReadChunkBytes;
    this.#maxSegments = maxSegments;
  }

  async push(
    chunk: Uint8Array,
    accept: (frame: SseFrame) => Promise<Record<string, unknown> | null>,
  ): Promise<Record<string, unknown> | null> {
    if (chunk.byteLength > this.#maxReadChunkBytes) {
      throw new ForwardingProtocolError("Upstream SSE read chunk is too large");
    }
    let segmentStart = 0;
    for (let index = 0; index < chunk.byteLength; index += 1) {
      const byte = chunk[index]!;
      if (this.#pendingCr) {
        this.#pendingCr = false;
        if (byte === 0x0a) {
          if (this.#pendingDispatch) {
            this.#append(chunk.slice(segmentStart, index + 1));
            segmentStart = index + 1;
            const final = await this.#dispatch(accept);
            if (final) return final;
          }
          continue;
        }
        if (this.#pendingDispatch) {
          this.#append(chunk.slice(segmentStart, index));
          segmentStart = index;
          const final = await this.#dispatch(accept);
          if (final) return final;
        }
      }
      if (byte === 0x0d) {
        this.#pendingDispatch = !this.#lineHasContent;
        this.#lineHasContent = false;
        this.#pendingCr = true;
        continue;
      }
      if (byte === 0x0a) {
        const dispatch = !this.#lineHasContent;
        this.#lineHasContent = false;
        if (dispatch) {
          this.#append(chunk.slice(segmentStart, index + 1));
          segmentStart = index + 1;
          const final = await this.#dispatch(accept);
          if (final) return final;
        }
        continue;
      }
      this.#lineHasContent = true;
    }
    this.#append(chunk.slice(segmentStart));
    return null;
  }

  async finish(
    accept: (frame: SseFrame) => Promise<Record<string, unknown> | null>,
  ): Promise<Record<string, unknown> | null> {
    if (this.#pendingCr && this.#pendingDispatch) {
      this.#pendingCr = false;
      return this.#dispatch(accept);
    }
    if (this.#eventBytes === 0) return null;
    throw new ForwardingProtocolError("Upstream event stream ended mid-frame");
  }

  #append(bytes: Uint8Array): void {
    if (bytes.byteLength === 0) return;
    const nextBytes = this.#eventBytes + bytes.byteLength;
    if (nextBytes > this.#maxEventBytes) {
      throw new ForwardingProtocolError("Upstream SSE event is too large");
    }
    if (this.#parts.length >= this.#maxSegments) {
      throw new ForwardingProtocolError("Upstream SSE event has too many segments");
    }
    this.#parts.push(bytes);
    this.#eventBytes = nextBytes;
  }

  #takeFrame(): SseFrame | null {
    let raw: string;
    try {
      raw = new TextDecoder("utf-8", { fatal: true }).decode(this.#takeBytes());
    } catch {
      throw new ForwardingProtocolError("Upstream SSE event is not valid UTF-8");
    }
    const parsed = parseFrames(raw, 0);
    if (parsed.nextOffset !== raw.length || parsed.frames.length > 1) {
      throw new ForwardingProtocolError("Upstream SSE event is malformed");
    }
    return parsed.frames[0] ?? null;
  }

  #takeBytes(): Uint8Array {
    const bytes = new Uint8Array(this.#eventBytes);
    let offset = 0;
    for (const part of this.#parts) {
      bytes.set(part, offset);
      offset += part.byteLength;
    }
    this.#parts = [];
    this.#eventBytes = 0;
    return bytes;
  }

  async #dispatch(
    accept: (frame: SseFrame) => Promise<Record<string, unknown> | null>,
  ): Promise<Record<string, unknown> | null> {
    const frame = this.#takeFrame();
    this.#lineHasContent = false;
    this.#pendingCr = false;
    this.#pendingDispatch = false;
    return frame ? accept(frame) : null;
  }
}

function serializableStreamingBody(body: Record<string, unknown>): string {
  try {
    const serialized = JSON.stringify({ ...body, stream: true });
    if (typeof serialized === "string") return serialized;
  } catch {
    // The public error must not include request values or serialization details.
  }
  throw new ApiError(400, "Verifier could not serialize the Responses request", {
    type: "invalid_request_error",
    code: "invalid_body",
  });
}

export function createForwardingRouter(
  upstream: string,
  dependencies: ForwardingRouterDependencies = {},
): ForwardingRouter {
  const origin = validateUpstream(upstream).upstream;
  const fetchImpl = dependencies.fetch ?? globalThis.fetch;
  const maxBufferedBytes = Math.min(positiveSafeInteger(
    dependencies.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
    "maxBufferedBytes",
  ), HARD_MAX_QUEUED_EVENT_BYTES);
  const maxEventBytes = positiveSafeInteger(
    dependencies.maxEventBytes ?? DEFAULT_MAX_EVENT_BYTES,
    "maxEventBytes",
  );
  const maxReadChunkBytes = positiveSafeInteger(
    dependencies.maxReadChunkBytes ?? DEFAULT_MAX_READ_CHUNK_BYTES,
    "maxReadChunkBytes",
  );
  const maxParserSegments = positiveSafeInteger(
    dependencies.maxParserSegments ?? DEFAULT_MAX_PARSER_SEGMENTS,
    "maxParserSegments",
  );
  const incomingBodies = new IncomingBodyBudget(
    dependencies.maxIncomingBodyBytes ?? DEFAULT_INCOMING_BODY_BUDGET_BYTES,
  );
  const activeControllers = new Set<AbortController>();
  const forwards = new ForwardSemaphore(VERIFIER_CAPACITY);
  let closed = false;

  const router: ForwardingRouter = {
    acquireIncomingBody(bytes, signal) {
      return incomingBodies.acquire(bytes, signal);
    },

    async startRelay(body, options = {}): Promise<RunHandle> {
      if (closed) {
        throw verifierClosingError();
      }
      if (options.signal?.aborted) throw abortError();
      const permit = await forwards.acquire(options.signal);
      if (closed) {
        permit.release();
        throw verifierClosingError();
      }
      const forwardEvents = body.stream === true;
      let requestBody: string;
      try {
        requestBody = serializableStreamingBody(body);
      } catch (error) {
        permit.release();
        throw error;
      }
      const controller = new AbortController();
      const forwardAbort = () => controller.abort();
      options.signal?.addEventListener("abort", forwardAbort, { once: true });
      activeControllers.add(controller);
      let cleaned = false;
      const cleanup = (): void => {
        if (cleaned) return;
        cleaned = true;
        options.signal?.removeEventListener("abort", forwardAbort);
        activeControllers.delete(controller);
        permit.release();
      };

      let response: Response;
      try {
        response = await fetchImpl(`${origin}/v1/responses`, {
          method: "POST",
          redirect: "error",
          signal: controller.signal,
          headers: {
            accept: "text/event-stream",
            "content-type": "application/json",
          },
          body: requestBody,
        });
      } catch {
        cleanup();
        if (controller.signal.aborted) throw abortError();
        throw upstreamFailure();
      }
      if (controller.signal.aborted) {
        void response.body?.cancel().catch(() => undefined);
        cleanup();
        throw abortError();
      }
      if (!response.ok) {
        void response.body?.cancel().catch(() => undefined);
        cleanup();
        throw safeHttpError(response);
      }
      const contentType = response.headers.get("content-type")
        ?.split(";", 1)[0]
        .trim()
        .toLowerCase();
      if (contentType !== "text/event-stream" || !response.body) {
        void response.body?.cancel().catch(() => undefined);
        cleanup();
        throw upstreamFailure("Verifier upstream did not return an event stream");
      }

      const run = new VerifierRunHandle(maxBufferedBytes, () => controller.abort());
      const reader = response.body.getReader();
      const cancelRun = (): void => {
        const error = abortError();
        run.cancel(error);
        void reader.cancel().catch(() => undefined);
      };
      controller.signal.addEventListener("abort", cancelRun, { once: true });
      const pump = async (): Promise<void> => {
        const parser = new BoundedSseParser(
          maxEventBytes,
          maxReadChunkBytes,
          maxParserSegments,
        );
        const accept = async (frame: SseFrame): Promise<Record<string, unknown> | null> => {
          const final = terminalFinal(frame);
          if (forwardEvents && (!final || run.canAcceptEvent(frame))) {
            await run.acceptEvent(frame);
          }
          return final;
        };
        try {
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            const final = await parser.push(value, accept);
            if (final) {
              run.settle(final);
              void reader.cancel().catch(() => undefined);
              return;
            }
          }
          if (controller.signal.aborted) throw abortError();
          const final = await parser.finish(accept);
          if (final) {
            run.settle(final);
            return;
          }
          throw new ForwardingProtocolError("Upstream event stream ended without a terminal event");
        } catch {
          if (controller.signal.aborted) run.fail(abortError());
          else run.fail(upstreamFailure("Verifier upstream event stream failed"));
          void reader.cancel().catch(() => undefined);
        } finally {
          controller.signal.removeEventListener("abort", cancelRun);
          try {
            reader.releaseLock();
          } catch {
            // A hostile reader cannot prevent local verifier cleanup.
          }
          cleanup();
        }
      };
      void pump();
      return run;
    },

    snapshot() {
      const forwarding = forwards.snapshot();
      const incoming = incomingBodies.snapshot();
      const capacity = closed ? 0 : VERIFIER_CAPACITY;
      return {
        capacity,
        active: forwarding.active,
        available: Math.max(0, capacity - forwarding.active),
        queued: forwarding.queued,
        runtimes: [],
        resources: {
          kind: "claude-code-forwarding-verifier",
          incomingBodyCapacityBytes: incoming.capacityBytes,
          incomingBodyReservedBytes: incoming.reservedBytes,
          incomingBodyQueued: incoming.queued,
        },
      };
    },

    close(): void {
      if (closed) return;
      closed = true;
      forwards.close();
      incomingBodies.close();
      for (const controller of activeControllers) controller.abort();
    },
  };
  return router;
}

function optionsFromEnvironment(
  environment: Record<string, string | undefined>,
): VerifierOptions {
  const portValue = environment.GP_CLAUDE_VERIFY_PORT ?? String(DEFAULT_PORT);
  if (!/^(?:0|[1-9]\d*)$/.test(portValue)) {
    throw new RangeError("GP_CLAUDE_VERIFY_PORT must be a decimal listener port");
  }
  return validateVerifierOptions({
    port: Number(portValue),
    upstream: environment.GP_CLAUDE_VERIFY_UPSTREAM ?? DEFAULT_UPSTREAM,
  });
}

function listen(server: Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", onError);
      resolve();
    });
  });
}

export function trackVerifierUpgrades(server: Pick<Server, "on" | "off">): {
  close(): void;
} {
  const sockets = new Set<Duplex>();
  const track = (socket: Duplex): void => {
    if (sockets.has(socket)) return;
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  };
  const onConnection = (socket: Duplex): void => track(socket);
  const onUpgrade = (_request: IncomingMessage, socket: Duplex): void => {
    track(socket);
  };
  server.on("connection", onConnection);
  server.on("upgrade", onUpgrade);
  let closed = false;
  return {
    close(): void {
      if (closed) return;
      closed = true;
      server.off("connection", onConnection);
      server.off("upgrade", onUpgrade);
      for (const socket of sockets) socket.destroy();
      sockets.clear();
    },
  };
}

export async function closeVerifierServer(
  server: Pick<Server, "close">,
  sockets: { close(): void },
  timeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS,
): Promise<void> {
  positiveSafeInteger(timeoutMs, "shutdown timeout");
  let timer: ReturnType<typeof setTimeout> | undefined;
  const closed = new Promise<void>(resolve => {
    try {
      server.close(() => resolve());
    } catch {
      resolve();
    }
  });
  const timeout = new Promise<void>(resolve => {
    timer = setTimeout(resolve, timeoutMs);
  });
  try {
    await Promise.race([closed, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    sockets.close();
  }
}

export async function main(
  environment: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = optionsFromEnvironment(environment);
  const router = createForwardingRouter(options.upstream);
  const app = createGhostProvApp({ router });
  const server = createGhostProvServer({
    app,
    router,
    port: options.port,
    candidate: true,
  });
  const upgrades = trackVerifierUpgrades(server);
  try {
    await listen(server, options.port);
  } catch (error) {
    upgrades.close();
    router.close();
    throw error;
  }
  console.log(`Claude Code verifier listening on http://127.0.0.1:${options.port}`);

  let shutdownPromise: Promise<void> | null = null;
  const shutdown = (): Promise<void> => {
    if (shutdownPromise) return shutdownPromise;
    router.close();
    shutdownPromise = closeVerifierServer(server, upgrades);
    return shutdownPromise;
  };
  let resolveSignal!: () => void;
  const signal = new Promise<void>(resolve => { resolveSignal = resolve; });
  const onSignal = (): void => resolveSignal();
  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  try {
    const serverEnd = new Promise<void>((resolve, reject) => {
      server.once("close", resolve);
      server.once("error", reject);
    });
    const outcome = await Promise.race([
      serverEnd.then(() => "server" as const),
      signal.then(() => "signal" as const),
    ]);
    if (outcome === "signal") await shutdown();
  } finally {
    process.off("SIGINT", onSignal);
    process.off("SIGTERM", onSignal);
    await shutdown();
  }
}

if (import.meta.main) {
  void main().catch(error => {
    console.error(error instanceof Error ? error.message : "Claude Code verifier failed");
    process.exitCode = 1;
  });
}
