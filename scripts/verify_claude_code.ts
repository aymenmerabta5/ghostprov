import type { FetchLike } from "../src/core/ports";
import { ApiError } from "../src/http/errors";
import { RelayError, type RunHandle } from "../src/relay/run-handle";
import { normalizeUpstreamResponsesRequest } from "../src/relay/upstream-policy";
import type { SseFrame } from "../src/relay/sse";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { ForwardSemaphore, IncomingBodyBudget } from "./claude-code-verifier/admission";
import { VerifierRunHandle } from "./claude-code-verifier/run-handle";
import {
  abortError,
  ForwardingProtocolError,
  positiveSafeInteger,
  verifierClosingError,
  verifierForwardTimeoutError,
} from "./claude-code-verifier/shared";
import { BoundedSseParser } from "./claude-code-verifier/sse-parser";
import {
  closeVerifierServer,
  listen,
  trackVerifierUpgrades,
} from "./claude-code-verifier/shutdown";
import {
  createAuthenticatedTenantScope,
  resolveGhostProvApiKey,
} from "../src/http/api-key";

export { closeVerifierServer, trackVerifierUpgrades };

const DEFAULT_PORT = 18_788;
const DEFAULT_UPSTREAM = "http://127.0.0.1:18787";
const PROTECTED_LISTENER_PORTS = new Set([18_787, 8_787]);
const DEFAULT_MAX_BUFFERED_BYTES = 1024 * 1024;
const DEFAULT_MAX_EVENT_BYTES = 2 * 1024 * 1024;
const DEFAULT_MAX_READ_CHUNK_BYTES = 1024 * 1024;
const DEFAULT_INCOMING_BODY_BUDGET_BYTES = 128 * 1024 * 1024;
const DEFAULT_MAX_PARSER_SEGMENTS = 1024;
const DEFAULT_MAX_CONCURRENT_FORWARDS = 8;
const DEFAULT_MAX_FORWARD_WAITERS = 32;
const DEFAULT_MAX_INCOMING_BODY_WAITERS = 32;
const DEFAULT_PARSER_MEMORY_BYTES = 64 * 1024 * 1024;
const DEFAULT_FORWARD_TIMEOUT_MS = 300_000;
const PARSER_MEMORY_AMPLIFICATION_FACTOR = 4;
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
  /** Shared bearer credential for the protected loopback GhostProv upstream. */
  apiKey?: string;
  maxBufferedBytes?: number;
  maxEventBytes?: number;
  maxReadChunkBytes?: number;
  maxIncomingBodyBytes?: number;
  maxParserSegments?: number;
  maxConcurrentForwards?: number;
  maxForwardWaiters?: number;
  maxIncomingBodyWaiters?: number;
  maxParserMemoryBytes?: number;
  forwardTimeoutMs?: number;
  scheduleDeadline?: (callback: () => void, milliseconds: number) => () => void;
}

function scheduleDeadline(callback: () => void, milliseconds: number): () => void {
  const timer = setTimeout(callback, milliseconds);
  (timer as ReturnType<typeof setTimeout> & { unref?: () => void }).unref?.();
  return () => clearTimeout(timer);
}

function safeProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product) || product < 1) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return product;
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

function linkAbortSignal(signal: AbortSignal | undefined, controller: AbortController): () => void {
  if (!signal) return () => undefined;
  const abort = () => controller.abort();
  if (signal.aborted) abort();
  else {
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  }
  return () => signal.removeEventListener("abort", abort);
}

async function raceAbort<T>(
  promise: Promise<T>,
  signal: AbortSignal,
  error: () => Error,
): Promise<T> {
  if (signal.aborted) throw error();
  let onAbort!: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(error());
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([promise, aborted]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
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

function serializableStreamingBody(body: Record<string, unknown>): string {
  try {
    const serialized = JSON.stringify(normalizeUpstreamResponsesRequest(body));
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
  const apiKey = dependencies.apiKey?.trim();
  if (dependencies.apiKey !== undefined && !apiKey) {
    throw new TypeError("Verifier apiKey must be nonempty when configured");
  }
  const maxBufferedBytes = positiveSafeInteger(
    dependencies.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES,
    "maxBufferedBytes",
  );
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
  const maxParserMemoryBytes = positiveSafeInteger(
    dependencies.maxParserMemoryBytes ?? DEFAULT_PARSER_MEMORY_BYTES,
    "maxParserMemoryBytes",
  );
  const parserMemoryReservationBytes = safeProduct(
    maxEventBytes,
    PARSER_MEMORY_AMPLIFICATION_FACTOR,
    "parser memory reservation",
  );
  if (parserMemoryReservationBytes > maxParserMemoryBytes) {
    throw new RangeError("maxEventBytes exceeds the aggregate parser memory budget");
  }
  const requestedForwardCapacity = positiveSafeInteger(
    dependencies.maxConcurrentForwards ?? DEFAULT_MAX_CONCURRENT_FORWARDS,
    "maxConcurrentForwards",
  );
  const forwardCapacity = Math.min(
    requestedForwardCapacity,
    Math.floor(maxParserMemoryBytes / parserMemoryReservationBytes),
  );
  const maxForwardWaiters = positiveSafeInteger(
    dependencies.maxForwardWaiters ?? DEFAULT_MAX_FORWARD_WAITERS,
    "maxForwardWaiters",
  );
  const maxIncomingBodyWaiters = positiveSafeInteger(
    dependencies.maxIncomingBodyWaiters ?? DEFAULT_MAX_INCOMING_BODY_WAITERS,
    "maxIncomingBodyWaiters",
  );
  const forwardTimeoutMs = positiveSafeInteger(
    dependencies.forwardTimeoutMs ?? DEFAULT_FORWARD_TIMEOUT_MS,
    "forwardTimeoutMs",
  );
  const deadlineScheduler = dependencies.scheduleDeadline ?? scheduleDeadline;
  const incomingBodies = new IncomingBodyBudget(
    dependencies.maxIncomingBodyBytes ?? DEFAULT_INCOMING_BODY_BUDGET_BYTES,
    maxIncomingBodyWaiters,
  );
  const activeControllers = new Set<AbortController>();
  const forwards = new ForwardSemaphore(forwardCapacity, maxForwardWaiters);
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
      if (options.signal?.aborted) {
        permit.release();
        throw abortError();
      }
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
      const unlinkAbort = linkAbortSignal(options.signal, controller);
      activeControllers.add(controller);
      let deadlineExpired = false;
      let cancelDeadline = (): void => undefined;
      let cleaned = false;
      const cleanup = (): void => {
        if (cleaned) return;
        cleaned = true;
        cancelDeadline();
        unlinkAbort();
        activeControllers.delete(controller);
        permit.release();
      };
      cancelDeadline = deadlineScheduler(() => {
        deadlineExpired = true;
        controller.abort();
      }, forwardTimeoutMs);
      const cancellationError = (): Error => deadlineExpired
        ? verifierForwardTimeoutError()
        : abortError();
      if (controller.signal.aborted) {
        cleanup();
        throw cancellationError();
      }

      let response: Response;
      const fetchPromise = Promise.resolve().then(() => fetchImpl(`${origin}/v1/responses`, {
        method: "POST",
        redirect: "error",
        signal: controller.signal,
        headers: {
          accept: "text/event-stream",
          "content-type": "application/json",
          ...(apiKey ? { authorization: `Bearer ${apiKey}` } : {}),
        },
        body: requestBody,
      }));
      try {
        response = await raceAbort(fetchPromise, controller.signal, cancellationError);
      } catch {
        if (controller.signal.aborted) {
          void fetchPromise.then(lateResponse => {
            void lateResponse.body?.cancel().catch(() => undefined);
          }, () => undefined);
        }
        cleanup();
        if (controller.signal.aborted) throw cancellationError();
        throw upstreamFailure();
      }
      if (controller.signal.aborted) {
        void response.body?.cancel().catch(() => undefined);
        cleanup();
        throw cancellationError();
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
        const error = cancellationError();
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
          if (controller.signal.aborted) throw cancellationError();
          const final = await parser.finish(accept);
          if (final) {
            run.settle(final);
            return;
          }
          throw new ForwardingProtocolError("Upstream event stream ended without a terminal event");
        } catch {
          if (controller.signal.aborted) run.fail(cancellationError());
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
      const capacity = closed ? 0 : forwarding.capacity;
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
          incomingBodyQueueCapacity: incoming.maxWaiters,
          forwardQueueCapacity: forwarding.maxWaiters,
          forwardDeadlineMs: forwardTimeoutMs,
          parserEventLimitBytes: maxEventBytes,
          parserMemoryAmplificationFactor: PARSER_MEMORY_AMPLIFICATION_FACTOR,
          parserMemoryCapacityBytes: maxParserMemoryBytes,
          parserMemoryReservationBytesPerForward: parserMemoryReservationBytes,
          parserMemoryReservedBytes: forwarding.active * parserMemoryReservationBytes,
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

export async function main(
  environment: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = optionsFromEnvironment(environment);
  const apiKey = resolveGhostProvApiKey(environment.GHOSTPROV_API_KEY);
  const router = createForwardingRouter(options.upstream, { apiKey });
  const authorizationScope = createAuthenticatedTenantScope();
  const app = createGhostProvApp({ router, apiKey, authorizationScope });
  const server = createGhostProvServer({
    app,
    router,
    apiKey,
    port: options.port,
    candidate: true,
    authorizationScope,
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
