import type { FetchLike } from "../../core/ports";
import { parseFrames, type SseFrame } from "../../relay/sse";
import {
  TranscriptProof,
  type TranscriptSnapshot,
} from "../protocol/transcript";
import {
  utf8Bytes,
  utf8Text,
  type TerminalStatus,
} from "../protocol/shared";
import {
  rateLimitHintFromError,
  rateLimitHintFromHeaders,
  rateLimitProblemFromError,
  type RateLimitHint,
} from "../rate-limit";

export interface SecretSource {
  reveal(): string;
}

export interface DirectResponsesExecution {
  body: Uint8Array;
  signal?: AbortSignal;
  beforeFetch(): Promise<void>;
  onEvent(event: SseFrame): Promise<void>;
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
  transcriptBytes: Uint8Array;
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
  ) {
    super(message);
  }
}

export interface DirectResponsesClientOptions {
  baseUrl: string;
  apiKey: SecretSource;
  fetch?: FetchLike;
  maxEventBytes?: number;
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

async function httpError(response: Response): Promise<UpstreamExecutionError> {
  let body: any = null;
  try {
    body = await response.json();
  } catch {}
  const upstream = body?.error && typeof body.error === "object" ? body.error : null;
  const fallback = `Upstream Responses request failed (${response.status})`;
  const message = sanitizeMessage(upstream?.message, fallback);
  const rateLimit = rateLimitHintFromError({
    status: response.status,
    error: {
      code: upstream?.code,
      type: upstream?.type,
      message,
    },
  }, rateLimitHintFromHeaders(response.headers));
  return new UpstreamExecutionError(
    message,
    safeCode(upstream?.code ?? upstream?.type, `http_${response.status}`),
    false,
    response.status,
    rateLimit,
  );
}

export class DirectResponsesClient {
  readonly #url: string;
  readonly #apiKey: SecretSource;
  readonly #fetch: FetchLike;
  readonly #maxEventBytes: number;

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
    if (!Number.isSafeInteger(this.#maxEventBytes) || this.#maxEventBytes < 1) {
      throw new RangeError("maxEventBytes must be a positive safe integer");
    }
  }

  async execute(input: DirectResponsesExecution): Promise<UpstreamTerminal> {
    const body = { ...parseBody(input.body), stream: true };
    let requestBody: string;
    try {
      requestBody = JSON.stringify(body);
    } catch {
      throw new UpstreamExecutionError("Responses request body is not serializable", "invalid_body", false);
    }
    try {
      await input.beforeFetch();
    } catch {
      throw new UpstreamExecutionError(
        "Upstream request was not started because durable preparation failed",
        "before_fetch",
        false,
      );
    }

    let response: Response;
    try {
      response = await this.#fetch(this.#url, {
        method: "POST",
        signal: input.signal,
        headers: {
          accept: "text/event-stream",
          authorization: `Bearer ${this.#apiKey.reveal()}`,
          "content-type": "application/json",
        },
        body: requestBody,
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
    if (!response.ok) throw await httpError(response);
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
      await response.body.cancel().catch(() => undefined);
      throw new UpstreamExecutionError(
        "Upstream response is not an event stream",
        "content_type",
        true,
        response.status,
        responseRateLimit,
      );
    }

    const transcript = new TranscriptProof("upstream", "0".repeat(64), {
      maxBytes: this.#maxEventBytes,
    });
    const decoder = new TextDecoder("utf-8", { fatal: true });
    const reader = response.body.getReader();
    let pending = "";
    const emitAvailable = async (): Promise<void> => {
      const parsed = parseFrames(pending, 0);
      for (const frame of parsed.frames) {
        const eventBytes = utf8Bytes(`event: ${frame.event}\ndata: ${frame.data}\n\n`).byteLength;
        if (eventBytes > this.#maxEventBytes) {
          throw new UpstreamExecutionError(
            "Upstream SSE event exceeds byte limit",
            "event_too_large",
            true,
            undefined,
            responseRateLimit,
          );
        }
        transcript.append(frame);
        await input.onEvent(frame);
      }
      pending = pending.slice(parsed.nextOffset);
      if (utf8Bytes(pending).byteLength > this.#maxEventBytes) {
        throw new UpstreamExecutionError(
          "Upstream SSE event exceeds byte limit",
          "event_too_large",
          true,
          undefined,
          responseRateLimit,
        );
      }
    };

    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        pending += decoder.decode(value, { stream: true });
        await emitAvailable();
      }
      pending += decoder.decode();
      await emitAvailable();
    } catch (error) {
      await reader.cancel().catch(() => undefined);
      if (error instanceof UpstreamExecutionError) throw error;
      const rateLimited = rateLimitTransportError(error, responseRateLimit);
      if (rateLimited) throw rateLimited;
      throw new UpstreamExecutionError(
        "Upstream event stream failed after request start",
        "stream",
        true,
        undefined,
        responseRateLimit,
      );
    } finally {
      reader.releaseLock();
    }
    if (pending.trim()) {
      throw new UpstreamExecutionError(
        "Upstream event stream ended mid-frame",
        "missing_terminal",
        true,
        undefined,
        responseRateLimit,
      );
    }

    let proof;
    let snapshot: TranscriptSnapshot;
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
      transcriptBytes: snapshot.canonicalBytes,
      ...(terminalRateLimit ? { rateLimit: terminalRateLimit } : {}),
    };
  }
}
