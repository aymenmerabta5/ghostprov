import type { FetchLike } from "../../core/ports";
import type { SseFrame } from "../../relay/sse";
import {
  AtomicBytePermit,
  AtomicBytePermitPool,
  estimateAtomicEventPermitBytes,
  type AtomicBytePermitRegion,
} from "../memory/atomic-byte-permits";
import { attachAtomicBytePermit } from "../memory/permit-carrier";
export {
  ATOMIC_EVENT_FIXED_OVERHEAD_BYTES,
  ATOMIC_EVENT_MEMORY_MULTIPLIER,
  estimateAtomicEventPermitBytes,
} from "../memory/atomic-byte-permits";
import {
  MAX_TRANSCRIPT_CHUNK_BYTES,
  TranscriptProof,
  V2TerminalProofError,
  type TranscriptSnapshot,
} from "../protocol/transcript";
import {
  utf8Text,
  type TerminalStatus,
  type UpstreamPrelude,
} from "../protocol/shared";
import {
  rateLimitHintFromError,
  rateLimitHintFromHeaders,
  rateLimitProblemFromError,
  type RateLimitHint,
} from "../rate-limit";
import {
  createProviderErrorEnvelope,
  type ProviderErrorEnvelope,
} from "../provider-error";

export interface SecretSource {
  reveal(): string;
}

export interface DirectResponsesExecution {
  body: Uint8Array;
  /** Host-proven canonical JSON with top-level `stream:true`; forward verbatim. */
  bodyIsCanonicalStream?: boolean;
  signal?: AbortSignal;
  beforeFetch(): Promise<void>;
  onPrelude(prelude: UpstreamPrelude): Promise<void>;
  /** Ordered canonical transcript chunks, each no larger than 64 KiB. */
  onTranscriptBytes?(bytes: Uint8Array): Promise<void>;
  onEvent(event: SseFrame): Promise<void>;
  /** Optional bounded parser telemetry hook for tests/diagnostics. */
  onParserSnapshot?(snapshot: IncrementalSseParserSnapshot): void;
}

export interface UpstreamTerminal {
  responseId: string;
  status: TerminalStatus;
  eventCount: number;
  byteCount: number;
  transcriptSha256: string;
  finalEventIndex: number;
  finalSha256: string;
  final: Record<string, unknown>;
  /** Terminal SSE event withheld from downstream until its journal proof is durable. */
  event?: SseFrame;
  /** Present only when the upstream terminal represents a rate-limit failure. */
  rateLimit?: RateLimitHint;
}

export class UpstreamExecutionError extends Error {
  readonly name = "UpstreamExecutionError";

  constructor(
    message: string,
    readonly code: string,
    readonly ambiguous: boolean,
    readonly status?: number,
    readonly rateLimit?: RateLimitHint,
    readonly providerError?: ProviderErrorEnvelope,
  ) {
    super(message);
  }
}

export interface DirectResponsesClientOptions {
  baseUrl: string;
  apiKey: SecretSource;
  fetch?: FetchLike;
  maxEventBytes?: number;
  /** Total time allowed to sample a definitive HTTP error body. */
  httpErrorReadTimeoutMs?: number;
  /** Process-global pool shared by all sidecar upstream clients. */
  atomicBytePermits?: AtomicBytePermitPool;
}

export interface IncrementalSseParserSnapshot {
  inputBytes: number;
  scannedBytes: number;
  completedEvents: number;
  currentEventBytes: number;
  /** Worst-case physical bytes charged to the global allocator. */
  currentReservedBytes: number;
  /** Rounded logical wire bytes covered by the current physical charge. */
  currentLogicalReservedBytes: number;
  currentRegion: AtomicBytePermitRegion | null;
  maxScratchBytes: number;
}

export interface IncrementalSseEvent {
  frame: SseFrame;
  /** Retained, newline-normalized logical bytes (CRLF's discarded LF is excluded). */
  logicalByteCount: number;
  /** @deprecated Alias for logicalByteCount. */
  rawByteCount: number;
  chargedBytes: number;
  region: AtomicBytePermitRegion | null;
  /** Transfer the physical charge beyond the parser callback (terminal proof). */
  transferPermit(label?: string): AtomicBytePermit | null;
  release(): void;
}

export interface IncrementalSseParserOptions {
  maxEventBytes: number;
  permits?: AtomicBytePermitPool;
  signal?: AbortSignal;
  reservationBlockBytes?: number;
}

function sanitizeMessage(value: unknown, fallback: string): string {
  const raw = typeof value === "string" && value.trim() ? value.trim() : fallback;
  return raw
    .replace(/\b(token|api[_-]?key|secret|password)\s*[:=]\s*[^;\s,]+/gi, "$1=[REDACTED]")
    .replace(/[A-Za-z0-9+/_=-]{48,}/g, "[REDACTED]")
    .slice(0, 512);
}

function safeCode(value: unknown, fallback: string): string {
  return typeof value === "string" && /^[A-Za-z][A-Za-z0-9._-]{0,63}$/.test(value)
    ? value
    : fallback;
}

function rateLimitTransportError(
  error: unknown,
  headerHint?: RateLimitHint,
): UpstreamExecutionError | null {
  const problem = rateLimitProblemFromError(error);
  if (!problem) return null;
  return new UpstreamExecutionError(
    sanitizeMessage(problem.message, "Upstream token rate limit was reached"),
    safeCode(problem.code, "rate_limit_exceeded"),
    false,
    429,
    rateLimitHintFromError(error, headerHint),
  );
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

/** OpenAI may terminate a 200 SSE response with `event: error` instead of response.failed. */
async function streamedUpstreamError(
  frame: SseFrame,
  headerHint?: RateLimitHint,
  headers?: Headers,
  model?: unknown,
): Promise<UpstreamExecutionError | null> {
  if (frame.event !== "error") return null;
  let decoded: unknown;
  try {
    decoded = JSON.parse(frame.data);
  } catch {
    const problem = rateLimitProblemFromError(frame.data);
    const message = problem?.message ?? "Upstream Responses stream emitted an invalid error event";
    const providerError = await createProviderErrorEnvelope({
      origin: "sse",
      status: problem?.status ?? 500,
      value: {
        error: {
          message,
          type: problem?.type ?? "upstream_error",
          param: null,
          code: problem?.code ?? "upstream_error",
        },
      },
      payload: frame.data,
      headers,
      model,
    });
    return new UpstreamExecutionError(
      providerError.error.message,
      providerError.error.code ?? "upstream_error",
      false,
      providerError.status,
      rateLimitHintFromError(frame.data, headerHint),
      providerError,
    );
  }
  const problem = rateLimitProblemFromError(decoded);
  const root = record(decoded);
  const explicitStatus = typeof root?.status === "number" &&
      Number.isInteger(root.status) && root.status >= 400 && root.status <= 599
    ? root.status
    : undefined;
  const providerError = await createProviderErrorEnvelope({
    origin: "sse",
    status: explicitStatus ?? problem?.status,
    value: decoded,
    payload: frame.data,
    headers,
    model,
    fallbackMessage: "Upstream Responses stream reported an error",
  });
  return new UpstreamExecutionError(
    providerError.error.message,
    safeCode(providerError.error.code ?? providerError.error.type, "upstream_error"),
    false,
    providerError.status,
    rateLimitHintFromError(decoded, headerHint),
    providerError,
  );
}

function parseBody(bytes: Uint8Array): Record<string, unknown> {
  let decoded: unknown;
  try {
    decoded = JSON.parse(utf8Text(bytes));
  } catch {
    throw new UpstreamExecutionError("Responses request body is invalid JSON", "invalid_body", false);
  }
  if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
    throw new UpstreamExecutionError("Responses request body must be an object", "invalid_body", false);
  }
  return decoded as Record<string, unknown>;
}

function validatedImmutableJsonText(bytes: Uint8Array): string {
  // Reuse the strict object validation before creating the immutable fallback.
  // This is reached only after an upstream parser rejected the byte-backed
  // request, so its extra decode/parse allocation is absent from the healthy
  // path and cannot affect ordinary or tiny-request latency.
  parseBody(bytes);
  return utf8Text(bytes);
}

function canonicalRequestModel(bytes: Uint8Array): string | undefined {
  let depth = 0;
  let inString = false;
  let escaped = false;
  let stringStart = 0;
  let stringKind: "key" | "model" | "other" = "other";
  let expectingTopKey = false;
  let lastKeyIsModel = false;
  let awaitingModelValue = false;
  const isModelKey = (start: number, end: number): boolean => (
    end - start === 5 &&
    bytes[start] === 0x6d &&
    bytes[start + 1] === 0x6f &&
    bytes[start + 2] === 0x64 &&
    bytes[start + 3] === 0x65 &&
    bytes[start + 4] === 0x6c
  );
  for (let index = 0; index < bytes.byteLength; index++) {
    const byte = bytes[index]!;
    if (inString) {
      if (escaped) {
        escaped = false;
        continue;
      }
      if (byte === 0x5c) {
        escaped = true;
        continue;
      }
      if (byte !== 0x22) continue;
      inString = false;
      if (stringKind === "key") {
        lastKeyIsModel = isModelKey(stringStart, index);
      } else if (stringKind === "model") {
        if (index - stringStart > 1_024) return undefined;
        try {
          const decoded = JSON.parse(utf8Text(bytes.subarray(stringStart - 1, index + 1)));
          return typeof decoded === "string" && decoded.length <= 256 ? decoded : undefined;
        } catch {
          return undefined;
        }
      }
      continue;
    }
    if (byte === 0x22) {
      inString = true;
      escaped = false;
      stringStart = index + 1;
      if (depth === 1 && expectingTopKey) {
        stringKind = "key";
        expectingTopKey = false;
      } else if (depth === 1 && awaitingModelValue) {
        stringKind = "model";
        awaitingModelValue = false;
      } else {
        stringKind = "other";
      }
      continue;
    }
    if (byte === 0x7b || byte === 0x5b) {
      depth++;
      if (depth === 1) expectingTopKey = true;
      continue;
    }
    if (byte === 0x7d || byte === 0x5d) {
      depth--;
      continue;
    }
    if (depth !== 1) continue;
    if (byte === 0x3a) {
      awaitingModelValue = lastKeyIsModel;
      lastKeyIsModel = false;
    } else if (byte === 0x2c) {
      expectingTopKey = true;
      awaitingModelValue = false;
      lastKeyIsModel = false;
    } else if (awaitingModelValue && byte !== 0x20 && byte !== 0x09 && byte !== 0x0a && byte !== 0x0d) {
      return undefined;
    }
  }
  return undefined;
}

const HTTP_ERROR_READ_MAX_BYTES = 64 * 1024;
const HTTP_ERROR_READ_TIMEOUT_MS = 250;
const HTTP_ERROR_READ_TIMEOUT_MAX_MS = 5_000;
const HTTP_ERROR_READ_STOP = Symbol("http-error-read-stop");

function cancelWithoutWaiting(cancel: () => Promise<unknown>): void {
  try {
    // Stream cancellation is advisory cleanup. A custom transport is allowed
    // to return a promise that never settles, so it must never retain request
    // capacity or delay a definitive outcome. Rejections are intentionally
    // observed and suppressed because the primary outcome is already known.
    void Promise.resolve(cancel()).catch(() => undefined);
  } catch {
    // Treat a synchronous cancellation failure like an async rejection.
  }
}

async function boundedErrorBody(
  response: Response,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<string> {
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let stoppedBeforeEnd = false;
  let cancelRequested = false;
  let stopResolved = false;
  let resolveStop!: (value: typeof HTTP_ERROR_READ_STOP) => void;
  const stop = new Promise<typeof HTTP_ERROR_READ_STOP>(resolve => { resolveStop = resolve; });
  const requestStop = (): void => {
    if (stopResolved) return;
    stopResolved = true;
    resolveStop(HTTP_ERROR_READ_STOP);
  };
  const cancelReader = (): void => {
    if (cancelRequested) return;
    cancelRequested = true;
    cancelWithoutWaiting(() => reader.cancel());
  };
  const deadline = performance.now() + timeoutMs;
  const timer = setTimeout(requestStop, timeoutMs);
  timer.unref?.();
  const onAbort = (): void => requestStop();
  if (signal?.aborted) requestStop();
  else signal?.addEventListener("abort", onAbort, { once: true });
  try {
    for (;;) {
      // Do not rely on the timer alone: a hostile stream can otherwise keep
      // resolving zero-byte reads through microtasks and starve timer tasks.
      if (performance.now() >= deadline) {
        stoppedBeforeEnd = true;
        cancelReader();
        break;
      }
      const result = await Promise.race([reader.read(), stop]);
      if (result === HTTP_ERROR_READ_STOP) {
        stoppedBeforeEnd = true;
        cancelReader();
        break;
      }
      const { done, value } = result;
      if (done) break;
      const remaining = HTTP_ERROR_READ_MAX_BYTES - total;
      if (remaining <= 0) {
        cancelReader();
        break;
      }
      const kept = value.byteLength > remaining ? value.subarray(0, remaining) : value;
      chunks.push(kept);
      total += kept.byteLength;
      if (kept.byteLength !== value.byteLength) {
        cancelReader();
        break;
      }
    }
  } catch (error) {
    cancelReader();
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
    try {
      reader.releaseLock();
    } catch {
      // A noncooperative implementation may retain a pending read after cancel.
    }
  }
  if (stoppedBeforeEnd) return "";
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8").decode(bytes);
}

async function httpError(
  response: Response,
  model: unknown,
  readTimeoutMs: number,
  signal?: AbortSignal,
): Promise<UpstreamExecutionError> {
  let rawBody = "";
  try {
    rawBody = await boundedErrorBody(response, readTimeoutMs, signal);
  } catch {
    // The HTTP status is already a definitive upstream outcome. A broken error
    // body must not turn it into an ambiguous execution or trigger replay.
  }
  let body: unknown = rawBody;
  try {
    body = JSON.parse(rawBody);
  } catch {}
  const root = record(body);
  const upstream = record(root?.error);
  const fallback = `Upstream Responses request failed (${response.status})`;
  const rateLimit = rateLimitHintFromError({
    status: response.status,
    error: {
      code: upstream?.code,
      type: upstream?.type,
      message: upstream?.message,
      param: upstream?.param,
    },
  }, rateLimitHintFromHeaders(response.headers));
  const providerError = await createProviderErrorEnvelope({
    origin: "http",
    status: response.status,
    value: body,
    payload: body,
    headers: response.headers,
    model,
    fallbackMessage: fallback,
  });
  return new UpstreamExecutionError(
    providerError.error.message,
    safeCode(providerError.error.code ?? providerError.error.type, `http_${response.status}`),
    false,
    response.status,
    rateLimit,
    providerError,
  );
}

const SSE_PARSER_SCRATCH_BYTES = 64 * 1024;

function parserAbortReason(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

function equalAscii(bytes: readonly number[], value: string, length: number): boolean {
  if (length !== value.length || bytes.length !== length) return false;
  for (let index = 0; index < length; index++) {
    if (bytes[index] !== value.charCodeAt(index)) return false;
  }
  return true;
}

/** One reusable <=64 KiB UTF-8 decoder buffer for the current SSE line. */
class SseLineDecoder {
  readonly #scratchBytes: number;
  #scratch: Uint8Array | null = null;
  #decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
  #scratchLength = 0;
  #collect = false;
  #output: string[] = [];

  constructor(scratchBytes = SSE_PARSER_SCRATCH_BYTES) {
    this.#scratchBytes = scratchBytes;
  }

  get scratchBytes(): number {
    return this.#scratchBytes;
  }

  reset(collect: boolean): void {
    // BOM handling is global to a byte stream, not per SSE line/value. The
    // parser strips only the optional stream-leading BOM; line decoders must
    // preserve every later U+FEFF.
    this.#decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
    this.#scratchLength = 0;
    this.#collect = collect;
    this.#output = [];
  }

  push(input: Uint8Array): void {
    const scratch = this.#scratch ??= new Uint8Array(this.#scratchBytes);
    let offset = 0;
    while (offset < input.byteLength) {
      const take = Math.min(
        scratch.byteLength - this.#scratchLength,
        input.byteLength - offset,
      );
      scratch.set(input.subarray(offset, offset + take), this.#scratchLength);
      this.#scratchLength += take;
      offset += take;
      if (this.#scratchLength === scratch.byteLength) this.#flush(true);
    }
  }

  finish(): string {
    this.#flush(true);
    const tail = this.#decoder.decode();
    if (this.#collect && tail.length > 0) this.#output.push(tail);
    const value = this.#collect ? this.#output.join("") : "";
    this.reset(false);
    return value;
  }

  releaseScratch(): void {
    this.#scratch?.fill(0);
    this.#scratch = null;
    this.#scratchLength = 0;
  }

  #flush(stream: boolean): void {
    if (this.#scratchLength === 0) return;
    const scratch = this.#scratch!;
    const decoded = this.#decoder.decode(
      scratch.subarray(0, this.#scratchLength),
      { stream },
    );
    if (this.#collect && decoded.length > 0) this.#output.push(decoded);
    this.#scratchLength = 0;
  }
}

/** Coalesces arbitrarily many tiny data lines before the one unavoidable frame string. */
class SseDataAccumulator {
  readonly #levels: Array<string | null> = [];
  readonly #pending: string[] = [];
  #pendingUnits = 0;
  #lines = 0;

  get hasData(): boolean {
    return this.#lines > 0;
  }

  appendLine(value: string): void {
    if (this.#lines > 0) this.#append("\n");
    this.#append(value);
    this.#lines++;
  }

  finish(): string {
    this.#flush();
    let value = "";
    for (let level = this.#levels.length - 1; level >= 0; level--) {
      if (this.#levels[level]) value += this.#levels[level];
    }
    this.clear();
    return value;
  }

  clear(): void {
    this.#levels.length = 0;
    this.#pending.length = 0;
    this.#pendingUnits = 0;
    this.#lines = 0;
  }

  #append(value: string): void {
    if (value.length === 0) return;
    this.#pending.push(value);
    this.#pendingUnits += value.length;
    if (this.#pendingUnits >= 32 * 1024 || this.#pending.length >= 1_024) this.#flush();
  }

  #flush(): void {
    if (this.#pending.length === 0) return;
    let chunk = this.#pending.join("");
    this.#pending.length = 0;
    this.#pendingUnits = 0;
    let level = 0;
    while (this.#levels[level]) {
      chunk = this.#levels[level]! + chunk;
      this.#levels[level] = null;
      level++;
    }
    this.#levels[level] = chunk;
  }
}

type SseLineField = "event" | "data" | "ignore";
const SAFE_UPSTREAM_EVENT = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/**
 * Single-pass byte SSE parser. It never rescans the pending prefix and never
 * concatenates incoming transport chunks. The only full-size value it creates
 * is the SseFrame data string required by the existing event API.
 */
export class IncrementalSseEventParser {
  readonly #maxEventBytes: number;
  readonly #permits?: AtomicBytePermitPool;
  readonly #signal?: AbortSignal;
  readonly #reservationBlockBytes: number;
  readonly #lineDecoder = new SseLineDecoder();
  readonly #data = new SseDataAccumulator();
  readonly #headerBytes: number[] = [];
  #headerLength = 0;
  #field: SseLineField | null = null;
  #fieldResolved = false;
  #skipValueSpace = false;
  #lineByteCount = 0;
  #eventName = "";
  #eventBytes = 0;
  #logicalReservedBytes = 0;
  #permit: AtomicBytePermit | null = null;
  #afterCr = false;
  #eventHasNonWhitespace = false;
  #inputBytes = 0;
  #scannedBytes = 0;
  #completedEvents = 0;
  #finished = false;
  #bomResolved = false;
  readonly #bomProbe: number[] = [];

  constructor(options: IncrementalSseParserOptions) {
    if (!Number.isSafeInteger(options.maxEventBytes) || options.maxEventBytes < 1) {
      throw new RangeError("maxEventBytes must be a positive safe integer");
    }
    const reservation = options.reservationBlockBytes ?? SSE_PARSER_SCRATCH_BYTES;
    if (
      !Number.isSafeInteger(reservation) ||
      reservation < 1 ||
      reservation > SSE_PARSER_SCRATCH_BYTES
    ) throw new RangeError("reservationBlockBytes must be between 1 and 64 KiB");
    this.#maxEventBytes = options.maxEventBytes;
    this.#permits = options.permits;
    this.#signal = options.signal;
    this.#reservationBlockBytes = reservation;
    this.#lineDecoder.reset(false);
  }

  snapshot(): IncrementalSseParserSnapshot {
    return {
      inputBytes: this.#inputBytes,
      scannedBytes: this.#scannedBytes,
      completedEvents: this.#completedEvents,
      currentEventBytes: this.#eventBytes,
      currentReservedBytes: this.#permit?.bytes ??
        estimateAtomicEventPermitBytes(this.#logicalReservedBytes),
      currentLogicalReservedBytes: this.#logicalReservedBytes,
      currentRegion: this.#permit?.region ?? null,
      maxScratchBytes: this.#lineDecoder.scratchBytes,
    };
  }

  async push(
    input: Uint8Array,
    onEvent: (event: IncrementalSseEvent) => void | Promise<void>,
  ): Promise<void> {
    if (this.#finished) throw new Error("SSE parser is already finished");
    if (!(input instanceof Uint8Array)) throw new TypeError("SSE parser input must be Uint8Array");
    this.#inputBytes += input.byteLength;
    let offset = 0;
    if (!this.#bomResolved) {
      const bom = [0xef, 0xbb, 0xbf] as const;
      while (offset < input.byteLength && !this.#bomResolved) {
        const value = input[offset++]!;
        this.#bomProbe.push(value);
        const probeIndex = this.#bomProbe.length - 1;
        if (value !== bom[probeIndex]) {
          this.#bomResolved = true;
          const prefix = Uint8Array.from(this.#bomProbe);
          this.#bomProbe.length = 0;
          await this.#pushResolved(prefix, onEvent);
        } else if (this.#bomProbe.length === bom.length) {
          // Match the original stream-wide TextDecoder behavior: consume one
          // optional leading UTF-8 BOM, and count its bytes as scanned input.
          this.#bomResolved = true;
          this.#scannedBytes += bom.length;
          this.#bomProbe.length = 0;
        }
      }
      if (!this.#bomResolved) return;
    }
    if (offset < input.byteLength) {
      await this.#pushResolved(input.subarray(offset), onEvent);
    }
  }

  async #pushResolved(
    input: Uint8Array,
    onEvent: (event: IncrementalSseEvent) => void | Promise<void>,
  ): Promise<void> {
    let start = 0;
    while (start < input.byteLength) {
      if (this.#signal?.aborted) throw parserAbortReason(this.#signal);
      if (this.#afterCr) {
        const byte = input[start]!;
        this.#scannedBytes++;
        this.#afterCr = false;
        if (byte === 0x0a) {
          // CR already completed the line/event immediately. LF carries no
          // retained state and canonical SSE normalizes CRLF to one newline.
          start++;
          continue;
        }
        if (byte === 0x0d) {
          await this.#charge(1);
          await this.#endLine(onEvent);
          this.#afterCr = true;
          start++;
          continue;
        }
        // The byte belongs to the new line; it has already consumed its one
        // scan step, so process a maximal non-delimiter run from this point.
        let end = start;
        while (
          end < input.byteLength &&
          input[end] !== 0x0a &&
          input[end] !== 0x0d
        ) {
          if (end !== start) this.#scannedBytes++;
          end++;
        }
        await this.#consumeLineSegment(input.subarray(start, end));
        start = end;
        continue;
      }

      let delimiter = -1;
      for (let index = start; index < input.byteLength; index++) {
        this.#scannedBytes++;
        const byte = input[index]!;
        if (byte === 0x0a || byte === 0x0d) {
          delimiter = index;
          break;
        }
      }
      if (delimiter < 0) {
        await this.#consumeLineSegment(input.subarray(start));
        break;
      }
      await this.#consumeLineSegment(input.subarray(start, delimiter));
      await this.#charge(1);
      const delimiterByte = input[delimiter]!;
      await this.#endLine(onEvent);
      this.#afterCr = delimiterByte === 0x0d;
      start = delimiter + 1;
    }
  }

  /** Finalize UTF-8 validation and return whether an incomplete frame remains. */
  async finish(
    onEvent?: (event: IncrementalSseEvent) => void | Promise<void>,
  ): Promise<{ trailingNonWhitespace: boolean; snapshot: IncrementalSseParserSnapshot }> {
    if (this.#finished) {
      return { trailingNonWhitespace: this.#eventHasNonWhitespace, snapshot: this.snapshot() };
    }
    try {
      if (!this.#bomResolved && this.#bomProbe.length > 0) {
        // Only a strict prefix of EF BB BF can remain here, which is invalid
        // UTF-8 at end-of-stream just as it was in the former global decoder.
        new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })
          .decode(Uint8Array.from(this.#bomProbe));
      }
      // Validate any incomplete line, but do not dispatch it without a blank-line
      // event delimiter (matching parseFrames behavior).
      this.#lineDecoder.finish();
    } catch (error) {
      this.abort();
      throw error;
    }
    this.#finished = true;
    const trailingNonWhitespace = this.#eventHasNonWhitespace;
    this.#permit?.release();
    this.#permit = null;
    this.#logicalReservedBytes = 0;
    this.#lineDecoder.releaseScratch();
    return { trailingNonWhitespace, snapshot: this.snapshot() };
  }

  abort(): void {
    if (this.#finished) return;
    this.#finished = true;
    this.#permit?.release();
    this.#permit = null;
    this.#logicalReservedBytes = 0;
    this.#data.clear();
    this.#lineDecoder.releaseScratch();
  }

  async #consumeLineSegment(segment: Uint8Array): Promise<void> {
    if (segment.byteLength === 0) return;
    await this.#charge(segment.byteLength);
    this.#lineByteCount += segment.byteLength;
    for (let index = 0; index < segment.byteLength; index++) {
      const byte = segment[index]!;
      if (!(byte === 0x20 || (byte >= 0x09 && byte <= 0x0d))) {
        this.#eventHasNonWhitespace = true;
      }
    }

    let offset = 0;
    if (!this.#fieldResolved) {
      let colon = -1;
      for (let index = 0; index < segment.byteLength; index++) {
        if (segment[index] === 0x3a) {
          colon = index;
          break;
        }
      }
      const headerEnd = colon < 0 ? segment.byteLength : colon;
      this.#captureHeader(segment.subarray(0, headerEnd));
      this.#lineDecoder.push(segment.subarray(0, headerEnd));
      offset = headerEnd;
      if (colon >= 0) {
        this.#lineDecoder.finish();
        this.#resolveField();
        this.#lineDecoder.reset(this.#field !== "ignore");
        this.#skipValueSpace = true;
        offset++;
      } else {
        return;
      }
    }

    if (this.#skipValueSpace && offset < segment.byteLength) {
      this.#skipValueSpace = false;
      if (segment[offset] === 0x20) offset++;
    }
    if (offset < segment.byteLength) this.#lineDecoder.push(segment.subarray(offset));
  }

  async #endLine(
    onEvent: (event: IncrementalSseEvent) => void | Promise<void>,
  ): Promise<void> {
    if (this.#lineByteCount === 0) {
      this.#lineDecoder.finish();
      await this.#completeEvent(onEvent);
      this.#resetLine();
      return;
    }
    if (!this.#fieldResolved) {
      this.#lineDecoder.finish();
      this.#resolveField();
      this.#lineDecoder.reset(this.#field !== "ignore");
    }
    const value = this.#lineDecoder.finish();
    if (this.#field === "event") this.#eventName = value;
    else if (this.#field === "data") this.#data.appendLine(value);
    this.#resetLine();
  }

  async #completeEvent(
    onEvent: (event: IncrementalSseEvent) => void | Promise<void>,
  ): Promise<void> {
    if (!this.#data.hasData) {
      this.#resetEvent();
      return;
    }
    const eventName = this.#eventName || "message";
    if (!SAFE_UPSTREAM_EVENT.test(eventName)) {
      throw new UpstreamExecutionError(
        "Upstream SSE event name is invalid",
        "invalid_event",
        true,
      );
    }
    const frame: SseFrame = {
      event: eventName,
      data: this.#data.finish(),
    };
    const rawByteCount = this.#eventBytes;
    const chargedBytes = estimateAtomicEventPermitBytes(rawByteCount);
    let eventPermit: AtomicBytePermit | null = null;
    if (this.#permit) {
      if (this.#permit.bytes > chargedBytes) {
        this.#permit.shrink(chargedBytes);
      }
      eventPermit = this.#permit.transfer("upstream-sse-event");
    }
    this.#permit = null;
    this.#logicalReservedBytes = 0;
    this.#eventBytes = 0;
    this.#eventName = "";
    this.#eventHasNonWhitespace = false;
    this.#completedEvents++;
    let released = false;
    const parsed: IncrementalSseEvent = {
      frame,
      logicalByteCount: rawByteCount,
      rawByteCount,
      chargedBytes,
      region: eventPermit?.region ?? null,
      transferPermit(label = "upstream-sse-event-retained") {
        if (released || !eventPermit?.active) return null;
        const successor = eventPermit.transfer(label);
        eventPermit = null;
        return successor;
      },
      release() {
        if (released) return;
        released = true;
        eventPermit?.release();
      },
    };
    try {
      await onEvent(parsed);
    } catch (error) {
      parsed.release();
      throw error;
    } finally {
      this.#lineDecoder.releaseScratch();
    }
  }

  async #charge(additionalBytes: number): Promise<void> {
    const target = this.#eventBytes + additionalBytes;
    if (target > this.#maxEventBytes) {
      throw new UpstreamExecutionError(
        "Upstream SSE event exceeds byte limit",
        "event_too_large",
        true,
      );
    }
    if (target > this.#logicalReservedBytes) {
      const rounded = Math.ceil(target / this.#reservationBlockBytes) * this.#reservationBlockBytes;
      const logicalReservation = Math.min(this.#maxEventBytes, rounded);
      const physicalReservation = estimateAtomicEventPermitBytes(logicalReservation);
      if (this.#permits) {
        if (!this.#permit) {
          const ordinaryCapacity = this.#permits.snapshot().ordinaryCapacityBytes;
          const immediateOrdinary = physicalReservation <= ordinaryCapacity
            ? this.#permits.tryAcquire(physicalReservation, {
                region: "ordinary",
                label: "upstream-sse-partial",
              })
            : null;
          // Never join the ordinary FIFO behind request bodies that can only
          // release after this response advances. The one reserved lane is the
          // progress guarantee; its own waiters remain strictly FIFO.
          this.#permit = immediateOrdinary ?? await this.#permits.acquireJumbo(
            physicalReservation,
            {
              signal: this.#signal,
              label: "upstream-sse-partial-jumbo",
            },
          );
        } else {
          await this.#permit.resize(physicalReservation, {
            signal: this.#signal,
            allowJumbo: true,
          });
        }
      }
      this.#logicalReservedBytes = logicalReservation;
    }
    this.#eventBytes = target;
  }

  #captureHeader(bytes: Uint8Array): void {
    this.#headerLength += bytes.byteLength;
    const remaining = Math.max(0, 8 - this.#headerBytes.length);
    for (let index = 0; index < Math.min(remaining, bytes.byteLength); index++) {
      this.#headerBytes.push(bytes[index]!);
    }
  }

  #resolveField(): void {
    this.#field = equalAscii(this.#headerBytes, "event", this.#headerLength)
      ? "event"
      : equalAscii(this.#headerBytes, "data", this.#headerLength)
      ? "data"
      : "ignore";
    this.#fieldResolved = true;
  }

  #resetLine(): void {
    this.#headerBytes.length = 0;
    this.#headerLength = 0;
    this.#field = null;
    this.#fieldResolved = false;
    this.#skipValueSpace = false;
    this.#lineByteCount = 0;
    this.#lineDecoder.reset(false);
  }

  #resetEvent(): void {
    this.#permit?.release();
    this.#permit = null;
    this.#logicalReservedBytes = 0;
    this.#eventBytes = 0;
    this.#eventName = "";
    this.#eventHasNonWhitespace = false;
    this.#data.clear();
    this.#lineDecoder.releaseScratch();
  }
}

export class DirectResponsesClient {
  readonly #url: string;
  readonly #apiKey: SecretSource;
  readonly #fetch: FetchLike;
  readonly #maxEventBytes: number;
  readonly #httpErrorReadTimeoutMs: number;
  readonly #atomicBytePermits?: AtomicBytePermitPool;

  constructor(options: DirectResponsesClientOptions) {
    let base: URL;
    try {
      base = new URL(options.baseUrl);
    } catch {
      throw new TypeError("OPENAI_BASE_URL is invalid");
    }
    if (base.protocol !== "https:" && base.protocol !== "http:") {
      throw new TypeError("OPENAI_BASE_URL must use HTTP or HTTPS");
    }
    this.#url = `${options.baseUrl.replace(/\/+$/, "")}/responses`;
    this.#apiKey = options.apiKey;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#maxEventBytes = options.maxEventBytes ?? 64 * 1024 * 1024;
    this.#httpErrorReadTimeoutMs = options.httpErrorReadTimeoutMs ?? HTTP_ERROR_READ_TIMEOUT_MS;
    this.#atomicBytePermits = options.atomicBytePermits;
    if (!Number.isSafeInteger(this.#maxEventBytes) || this.#maxEventBytes < 1) {
      throw new RangeError("maxEventBytes must be a positive safe integer");
    }
    if (
      !Number.isSafeInteger(this.#httpErrorReadTimeoutMs) ||
      this.#httpErrorReadTimeoutMs < 1 ||
      this.#httpErrorReadTimeoutMs > HTTP_ERROR_READ_TIMEOUT_MAX_MS
    ) throw new RangeError("httpErrorReadTimeoutMs must be an integer from 1 through 5000");
  }

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    let requestBody: BodyInit;
    let model: unknown;
    if (input.bodyIsCanonicalStream) {
      // The request assembler owns and securely erases its mutable Uint8Array.
      // Give fetch an immutable snapshot so a runtime that consumes BodyInit
      // lazily cannot observe reuse/erasure while uploading a large request.
      // V2 request working-set accounting already reserves one fetch copy.
      requestBody = new Blob([input.body as unknown as BlobPart], {
        type: "application/json",
      });
      model = canonicalRequestModel(input.body);
    } else {
      const body: Record<string, unknown> = { ...parseBody(input.body), stream: true };
      model = body.model;
      try {
        requestBody = JSON.stringify(body);
      } catch {
        throw new UpstreamExecutionError("Responses request body is not serializable", "invalid_body", false);
      }
    }
    try {
      await input.beforeFetch();
    } catch (error) {
      if (
        error instanceof Error &&
        (error.name === "V2ExecutionFenceError" || error.name === "SidecarOwnershipError")
      ) throw error;
      throw new UpstreamExecutionError(
        "Upstream request was not started because durable preparation failed",
        "before_fetch",
        false,
      );
    }

    const fetchOnce = async (body: BodyInit): Promise<Response> => {
      try {
        return await this.#fetch(this.#url, {
          method: "POST",
          signal: input.signal,
          headers: {
            accept: "text/event-stream",
            authorization: `Bearer ${this.#apiKey.reveal()}`,
            "content-type": "application/json",
          },
          body,
        });
      } catch (error) {
        const rateLimited = rateLimitTransportError(error);
        if (rateLimited) throw rateLimited;
        throw new UpstreamExecutionError(
          "Upstream Responses transport failed after request start",
          "transport",
          true,
        );
      }
    };
    let response = await fetchOnce(requestBody);
    if (!response.ok) {
      const failure = await httpError(
        response,
        model,
        this.#httpErrorReadTimeoutMs,
        input.signal,
      );
      if (
        input.bodyIsCanonicalStream &&
        failure.status === 400 &&
        failure.code === "invalid_json" &&
        !input.signal?.aborted
      ) {
        // `invalid_json` is a definitive pre-execution parser rejection. Retry
        // it exactly once from a locally revalidated immutable string. This
        // repairs a transient BodyInit/upload serialization failure without
        // replaying provider execution, and leaves every other 4xx untouched.
        requestBody = validatedImmutableJsonText(input.body);
        response = await fetchOnce(requestBody);
        if (!response.ok) {
          throw await httpError(
            response,
            model,
            this.#httpErrorReadTimeoutMs,
            input.signal,
          );
        }
      } else {
        throw failure;
      }
    }
    const responseRateLimit = rateLimitHintFromHeaders(response.headers);
    if (!response.body) {
      throw new UpstreamExecutionError(
        "Upstream response has no stream body",
        "missing_body",
        true,
        response.status,
        responseRateLimit,
      );
    }
    const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
    if (!contentType.includes("text/event-stream")) {
      cancelWithoutWaiting(() => response.body!.cancel());
      throw new UpstreamExecutionError(
        "Upstream response is not an event stream",
        "content_type",
        true,
        response.status,
        responseRateLimit,
      );
    }
    const rawRequestId = response.headers.get("x-request-id");
    const upstreamRequestId = rawRequestId && /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/.test(rawRequestId)
      ? rawRequestId
      : null;
    try {
      await input.onPrelude({
        status: response.status,
        contentType: contentType.slice(0, 256),
        upstreamRequestId,
      });
    } catch {
      cancelWithoutWaiting(() => response.body!.cancel());
      throw new UpstreamExecutionError(
        "Upstream response headers could not be delivered",
        "prelude",
        true,
        response.status,
        responseRateLimit,
      );
    }

    const transcript = new TranscriptProof("upstream", "0".repeat(64), {
      maxBytes: this.#maxEventBytes,
      retainBytes: false,
    });
    const reader = response.body.getReader();
    const parser = new IncrementalSseEventParser({
      maxEventBytes: this.#maxEventBytes,
      permits: this.#atomicBytePermits,
      signal: input.signal,
    });
    let terminalPermit: AtomicBytePermit | null = null;
    let terminalEvent: SseFrame | null = null;
    const terminalStreamComplete = {};
    const releaseTerminalPermit = (): void => {
      terminalPermit?.release();
      terminalPermit = null;
    };
    const consumeEvent = async (parsed: IncrementalSseEvent): Promise<void> => {
      try {
        const frame = parsed.frame;
        const upstreamError = await streamedUpstreamError(
          frame,
          responseRateLimit,
          response.headers,
          model,
        );
        if (upstreamError) throw upstreamError;
        try {
          await transcript.appendCanonicalStream(frame, {
            chunkBytes: MAX_TRANSCRIPT_CHUNK_BYTES,
            onChunk: input.onTranscriptBytes,
          });
        } catch (error) {
          if (!(error instanceof V2TerminalProofError) || error.code !== "byte_limit") throw error;
          throw new UpstreamExecutionError(
            "Upstream SSE transcript exceeds byte limit",
            "event_too_large",
            true,
            undefined,
            responseRateLimit,
          );
        }
        if (
          frame.event === "response.completed" ||
          frame.event === "response.failed" ||
          frame.event === "response.incomplete"
        ) {
          terminalEvent = frame;
          terminalPermit = parsed.transferPermit("upstream-terminal-proof");
          // The Responses terminal is the logical end of the stream. Stop
          // consuming immediately so retained proof memory cannot deadlock a
          // post-terminal event on the same global permit pool.
          throw terminalStreamComplete;
        }
        await input.onEvent(frame);
      } finally {
        parsed.release();
      }
    };

    let trailingNonWhitespace = false;
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        for (let offset = 0; offset < value.byteLength; offset += SSE_PARSER_SCRATCH_BYTES) {
          await parser.push(
            value.subarray(offset, offset + SSE_PARSER_SCRATCH_BYTES),
            consumeEvent,
          );
        }
        input.onParserSnapshot?.(parser.snapshot());
      }
      const finished = await parser.finish(consumeEvent);
      trailingNonWhitespace = finished.trailingNonWhitespace;
      input.onParserSnapshot?.(finished.snapshot);
    } catch (error) {
      parser.abort();
      if (error === terminalStreamComplete) {
        // The logical terminal is complete. Underlying stream cancellation is
        // advisory and may never settle; never let it retain the terminal/jumbo
        // permit or delay durable proof materialization.
        cancelWithoutWaiting(() => reader.cancel());
        // Expected early completion; terminal proof is materialized below.
        input.onParserSnapshot?.(parser.snapshot());
      } else {
        cancelWithoutWaiting(() => reader.cancel());
        releaseTerminalPermit();
      if (error instanceof UpstreamExecutionError) {
        if (error.code !== "event_too_large" || error.rateLimit) throw error;
        throw new UpstreamExecutionError(
          error.message,
          error.code,
          error.ambiguous,
          error.status,
          responseRateLimit,
          error.providerError,
        );
      }
      if (error instanceof Error && error.name === "ClaimJournalError") throw error;
      const rateLimited = rateLimitTransportError(error, responseRateLimit);
      if (rateLimited) throw rateLimited;
      throw new UpstreamExecutionError(
        "Upstream event stream failed after request start",
        "stream",
        true,
        undefined,
        responseRateLimit,
      );
      }
    } finally {
      reader.releaseLock();
    }
    if (trailingNonWhitespace) {
      releaseTerminalPermit();
      throw new UpstreamExecutionError(
        "Upstream event stream ended mid-frame",
        "missing_terminal",
        true,
        undefined,
        responseRateLimit,
      );
    }

    try {
      let proof;
      let snapshot: TranscriptSnapshot<false>;
      try {
        proof = await transcript.createTerminal();
        snapshot = transcript.snapshot();
      } catch {
        throw new UpstreamExecutionError(
          "Upstream event stream has no terminal proof",
          "missing_terminal",
          true,
          undefined,
          responseRateLimit,
        );
      }
      if (!snapshot.final || snapshot.finalEventIndex === null || !snapshot.terminalStatus) {
        throw new UpstreamExecutionError(
          "Upstream terminal state is incomplete",
          "missing_terminal",
          true,
          undefined,
          responseRateLimit,
        );
      }
      if (!terminalEvent) {
        throw new UpstreamExecutionError(
          "Upstream terminal event is unavailable",
          "missing_terminal",
          true,
          undefined,
          responseRateLimit,
        );
      }
      if (terminalPermit) {
        attachAtomicBytePermit(terminalEvent, terminalPermit);
        terminalPermit = null;
      }
      const terminalRateLimit = rateLimitHintFromError(snapshot.final, responseRateLimit) ?? responseRateLimit;
      return {
        responseId: proof.responseId,
        status: proof.terminalStatus,
        eventCount: proof.eventCount,
        byteCount: proof.byteCount,
        transcriptSha256: proof.transcriptSha256,
        finalEventIndex: proof.finalEventIndex,
        finalSha256: proof.finalSha256,
        final: snapshot.final,
        event: terminalEvent,
        ...(terminalRateLimit ? { rateLimit: terminalRateLimit } : {}),
      };
    } finally {
      releaseTerminalPermit();
    }
  }
}
