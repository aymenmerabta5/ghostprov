import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import type { FetchLike } from "../src/core/ports";
import { ApiError } from "../src/http/errors";
import { RelayError, type RunHandle } from "../src/relay/run-handle";
import { parseFrames, type SseFrame } from "../src/relay/sse";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { V2RunHandle } from "../src/v2/transport/run-handle";

const DEFAULT_PORT = 18_788;
const DEFAULT_UPSTREAM = "http://127.0.0.1:18787";
const PROTECTED_LISTENER_PORTS = new Set([18_787, 8_787]);
const DEFAULT_MAX_BUFFERED_BYTES = 1024 * 1024;
const DEFAULT_MAX_EVENT_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAX_READ_CHUNK_BYTES = 1024 * 1024;
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

function safeHttpError(status: number): ApiError {
  const safeStatus = Number.isInteger(status) && status >= 400 && status <= 599
    ? status
    : 502;
  return new ApiError(
    safeStatus,
    `Verifier upstream request failed (HTTP ${safeStatus})`,
    { type: "upstream_error", code: `upstream_http_${safeStatus}` },
  );
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
  #parts: Uint8Array[] = [];
  #eventBytes = 0;
  #previous1 = -1;
  #previous2 = -1;
  #previous3 = -1;

  constructor(maxEventBytes: number, maxReadChunkBytes: number) {
    this.#maxEventBytes = maxEventBytes;
    this.#maxReadChunkBytes = maxReadChunkBytes;
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
      const delimiter = (this.#previous1 === 0x0a && byte === 0x0a) ||
        (this.#previous3 === 0x0d && this.#previous2 === 0x0a &&
          this.#previous1 === 0x0d && byte === 0x0a);
      if (delimiter) {
        this.#append(chunk.slice(segmentStart, index + 1));
        const frame = this.#takeFrame();
        this.#resetDelimiter();
        segmentStart = index + 1;
        if (frame) {
          const final = await accept(frame);
          if (final) return final;
        }
        continue;
      }
      this.#previous3 = this.#previous2;
      this.#previous2 = this.#previous1;
      this.#previous1 = byte;
    }
    this.#append(chunk.slice(segmentStart));
    return null;
  }

  finish(): void {
    if (this.#eventBytes === 0) return;
    let trailing: string;
    try {
      trailing = new TextDecoder("utf-8", { fatal: true }).decode(this.#takeBytes());
    } catch {
      throw new ForwardingProtocolError("Upstream event stream ended mid-frame");
    }
    if (trailing.trim()) {
      throw new ForwardingProtocolError("Upstream event stream ended mid-frame");
    }
  }

  #append(bytes: Uint8Array): void {
    if (bytes.byteLength === 0) return;
    const nextBytes = this.#eventBytes + bytes.byteLength;
    if (nextBytes > this.#maxEventBytes) {
      throw new ForwardingProtocolError("Upstream SSE event is too large");
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

  #resetDelimiter(): void {
    this.#previous1 = -1;
    this.#previous2 = -1;
    this.#previous3 = -1;
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
  const activeControllers = new Set<AbortController>();
  let active = 0;
  let closed = false;

  const router: ForwardingRouter = {
    async startRelay(body, options = {}): Promise<RunHandle> {
      if (closed) {
        throw new ApiError(503, "Claude Code verifier is shutting down", {
          type: "service_unavailable_error",
          code: "verifier_shutting_down",
        });
      }
      if (options.signal?.aborted) throw abortError();
      const forwardEvents = body.stream === true;
      const requestBody = serializableStreamingBody(body);
      const controller = new AbortController();
      const forwardAbort = () => controller.abort();
      options.signal?.addEventListener("abort", forwardAbort, { once: true });
      activeControllers.add(controller);
      active += 1;
      let cleaned = false;
      const cleanup = (): void => {
        if (cleaned) return;
        cleaned = true;
        options.signal?.removeEventListener("abort", forwardAbort);
        activeControllers.delete(controller);
        active = Math.max(0, active - 1);
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
        throw safeHttpError(response.status);
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

      const run = new V2RunHandle({ maxBufferedBytes });
      const reader = response.body.getReader();
      const cancelRun = (): void => {
        const error = abortError();
        run.cancel(error);
        void reader.cancel().catch(() => undefined);
      };
      controller.signal.addEventListener("abort", cancelRun, { once: true });
      let pumpFinished = false;

      const pump = async (): Promise<void> => {
        const parser = new BoundedSseParser(maxEventBytes, maxReadChunkBytes);
        const accept = async (frame: SseFrame): Promise<Record<string, unknown> | null> => {
          const final = terminalFinal(frame);
          if (forwardEvents) await run.acceptEvent(frame);
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
          parser.finish();
          throw new ForwardingProtocolError("Upstream event stream ended without a terminal event");
        } catch {
          if (controller.signal.aborted) run.fail(abortError());
          else run.fail(upstreamFailure("Verifier upstream event stream failed"));
          void reader.cancel().catch(() => undefined);
        } finally {
          pumpFinished = true;
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

      return {
        final: run.final,
        stats: run.stats,
        async *events(): AsyncGenerator<SseFrame> {
          let exhausted = false;
          let sawTerminal = false;
          try {
            for await (const frame of run.events()) {
              if (TERMINAL_STATUSES.has(frame.event)) sawTerminal = true;
              yield frame;
            }
            exhausted = true;
          } finally {
            if (!exhausted && !sawTerminal && !pumpFinished && !controller.signal.aborted) {
              controller.abort();
            }
          }
        },
      };
    },

    snapshot() {
      const capacity = closed ? 0 : VERIFIER_CAPACITY;
      return {
        capacity,
        active,
        available: Math.max(0, capacity - active),
        queued: 0,
        runtimes: [],
        resources: { kind: "claude-code-forwarding-verifier" },
      };
    },

    close(): void {
      if (closed) return;
      closed = true;
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
  const onUpgrade = (_request: IncomingMessage, socket: Duplex): void => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  };
  server.on("upgrade", onUpgrade);
  let closed = false;
  return {
    close(): void {
      if (closed) return;
      closed = true;
      server.off("upgrade", onUpgrade);
      for (const socket of sockets) socket.destroy();
      sockets.clear();
    },
  };
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

  let closing = false;
  const close = (): void => {
    if (closing) return;
    closing = true;
    server.close();
    upgrades.close();
    router.close();
  };
  process.once("SIGINT", close);
  process.once("SIGTERM", close);
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("close", resolve);
      server.once("error", reject);
    });
  } finally {
    process.off("SIGINT", close);
    process.off("SIGTERM", close);
    close();
  }
}

if (import.meta.main) {
  void main().catch(error => {
    console.error(error instanceof Error ? error.message : "Claude Code verifier failed");
    process.exitCode = 1;
  });
}
