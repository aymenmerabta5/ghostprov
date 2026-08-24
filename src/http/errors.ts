import {
  isRateLimited,
  rateLimitHintFromError,
  rateLimitProblemFromError,
} from "../v2/rate-limit";

export interface ApiErrorOptions {
  type?: string;
  param?: string | null;
  code?: string | null;
}

export interface OpenAIErrorShape {
  message: string;
  type: string;
  param: string | null;
  code: string | null;
}

export interface NormalizedApiError {
  status: number;
  error: OpenAIErrorShape;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly options: ApiErrorOptions = {},
  ) {
    super(message);
    this.name = "ApiError";
  }
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function parseJson(value: unknown): unknown {
  if (typeof value !== "string") return null;
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function statusFromDaemonCode(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const match = /^(?:http|rate_limited)_(\d{3})$/.exec(value);
  if (!match) return null;
  const status = Number(match[1]);
  return status >= 400 && status <= 599 ? status : null;
}

function normalizeOpenAIError(value: unknown, fallbackMessage: string): OpenAIErrorShape {
  const candidate = record(value);
  return {
    message: typeof candidate?.message === "string" && candidate.message.length > 0
      ? candidate.message
      : fallbackMessage,
    type: typeof candidate?.type === "string" && candidate.type.length > 0
      ? candidate.type
      : "upstream_error",
    param: typeof candidate?.param === "string" ? candidate.param : null,
    code: typeof candidate?.code === "string" ? candidate.code : null,
  };
}

function daemonEnvelope(error: unknown): Record<string, unknown> | null {
  const candidate = record(error);
  const details = record(candidate?.details);
  if (details) return details;

  const message = error instanceof Error
    ? error.message
    : typeof candidate?.message === "string"
      ? candidate.message
      : "";
  const marker = "upstream failed:";
  const index = message.indexOf(marker);
  if (index === -1) return null;
  return record(parseJson(message.slice(index + marker.length).trim()));
}

function normalizeDaemonError(error: unknown): NormalizedApiError | null {
  const envelope = daemonEnvelope(error);
  if (!envelope) return null;

  const status = statusFromDaemonCode(envelope.code) ?? 502;
  const parsedBody = parseJson(envelope.message);
  const body = record(parsedBody);
  const upstreamError = body?.error ?? parsedBody;
  const fallbackMessage = typeof envelope.message === "string" && envelope.message.length > 0
    ? envelope.message
    : "Upstream request failed";

  return {
    status,
    error: normalizeOpenAIError(upstreamError, fallbackMessage),
  };
}

function relayKind(error: unknown): string | null {
  const candidate = record(error);
  return typeof candidate?.kind === "string" ? candidate.kind : null;
}

function rateLimitMetadata(error: unknown): {
  retryAfterMs: number | null;
  resetTokensMs: number | null;
  resetProjectTokensMs: number | null;
  limitTokens: number | null;
  remainingTokens: number | null;
  limitProjectTokens: number | null;
  remainingProjectTokens: number | null;
} {
  const candidate = record(error);
  const nested = record(candidate?.rateLimit);
  const extracted = rateLimitHintFromError(error);
  const number = (value: unknown): number | null => (
    typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null
  );
  return {
    retryAfterMs: number(candidate?.retryAfterMs) ?? number(nested?.retryAfterMs) ?? extracted?.retryAfterMs ?? null,
    resetTokensMs: number(candidate?.resetTokensMs) ?? number(nested?.resetTokensMs) ?? extracted?.resetTokensMs ?? null,
    resetProjectTokensMs: number(candidate?.resetProjectTokensMs) ?? number(nested?.resetProjectTokensMs) ?? extracted?.resetProjectTokensMs ?? null,
    limitTokens: number(nested?.limitTokens) ?? extracted?.limitTokens ?? null,
    remainingTokens: number(nested?.remainingTokens) ?? extracted?.remainingTokens ?? null,
    limitProjectTokens: number(nested?.limitProjectTokens) ?? extracted?.limitProjectTokens ?? null,
    remainingProjectTokens: number(nested?.remainingProjectTokens) ?? extracted?.remainingProjectTokens ?? null,
  };
}

export function normalizeApiError(error: unknown): NormalizedApiError {
  if (error instanceof ApiError) {
    return {
      status: error.status,
      error: {
        message: error.message,
        type: error.options.type ?? (error.status >= 500 ? "server_error" : "invalid_request_error"),
        param: error.options.param ?? null,
        code: error.options.code ?? null,
      },
    };
  }

  if (record(error)?.name === "AbortError") {
    return {
      status: 499,
      error: {
        message: "Request was cancelled",
        type: "request_aborted",
        param: null,
        code: "request_aborted",
      },
    };
  }

  const structured = record(error);
  if (
    structured?.name === "RouterShutdownError" &&
    structured.status === 503 &&
    structured.code === "router_shutting_down"
  ) {
    return {
      status: 503,
      error: {
        message: error instanceof Error ? error.message : "GhostProv is shutting down",
        type: "service_unavailable_error",
        param: null,
        code: "router_shutting_down",
      },
    };
  }
  if (
    structured?.name === "RouterCapacityTimeoutError" &&
    structured.status === 503 &&
    structured.code === "router_capacity_timeout"
  ) {
    return {
      status: 503,
      error: {
        message: error instanceof Error
          ? error.message
          : "No sandbox capacity became available before the routing deadline",
        type: "service_unavailable_error",
        param: null,
        code: "router_capacity_timeout",
      },
    };
  }
  if (
    structured?.name === "RouterRateLimitedError" &&
    structured.status === 429 &&
    structured.code === "rate_limit_exceeded"
  ) {
    const problem = record(structured.problem);
    return {
      status: 429,
      error: {
        message: typeof problem?.message === "string"
          ? problem.message
          : error instanceof Error
            ? error.message
            : "Upstream token rate limit is cooling down",
        type: typeof problem?.type === "string" ? problem.type : "rate_limit_error",
        param: typeof problem?.param === "string" ? problem.param : null,
        code: typeof problem?.code === "string" ? problem.code : "rate_limit_exceeded",
      },
    };
  }

  const daemon = normalizeDaemonError(error);
  if (daemon) return daemon;

  if (
    isRateLimited(error) ||
    structured?.code === "rate_limit_exceeded" ||
    structured?.code === "rate_limit"
  ) {
    const problem = rateLimitProblemFromError(error);
    return {
      status: 429,
      error: {
        message: problem?.message ?? (error instanceof Error ? error.message : "Upstream token rate limit was reached"),
        type: problem?.type ?? "rate_limit_error",
        param: problem?.param ?? null,
        code: problem?.code ?? "rate_limit_exceeded",
      },
    };
  }

  const kind = relayKind(error);
  const message = error instanceof Error && error.message.length > 0
    ? error.message
    : "Internal server error";

  switch (kind) {
    case "exhausted":
      return { status: 429, error: { message, type: "rate_limit_error", param: null, code: "rate_limit_exceeded" } };
    case "no-bridge":
      return { status: 503, error: { message, type: "service_unavailable_error", param: null, code: "bridge_unavailable" } };
    case "timeout":
      return { status: 504, error: { message, type: "timeout_error", param: null, code: "upstream_timeout" } };
    case "sync":
      return { status: 503, error: { message, type: "service_unavailable_error", param: null, code: "sync" } };
    case "render":
      return { status: 502, error: { message, type: "upstream_error", param: null, code: kind } };
    case "upstream":
      return { status: 502, error: { message, type: "upstream_error", param: null, code: "upstream_error" } };
    default:
      return {
        status: 500,
        error: {
          message: "Internal server error",
          type: "internal_error",
          param: null,
          code: "internal_error",
        },
      };
  }
}

export function apiErrorResponse(error: unknown): Response {
  const normalized = normalizeApiError(error);
  const metadata = rateLimitMetadata(error);
  const headers = new Headers();
  if (normalized.status === 429) {
    const retryAfterMs = metadata.retryAfterMs;
    if (retryAfterMs !== null) {
      headers.set("retry-after", String(Math.max(1, Math.ceil(retryAfterMs / 1_000))));
    }
    const resetTokensMs = metadata.resetTokensMs;
    if (resetTokensMs !== null) headers.set("x-ratelimit-reset-tokens", `${Math.ceil(resetTokensMs)}ms`);
    const resetProjectTokensMs = metadata.resetProjectTokensMs;
    if (resetProjectTokensMs !== null) {
      headers.set("x-ratelimit-reset-project-tokens", `${Math.ceil(resetProjectTokensMs)}ms`);
    }
    if (metadata.limitTokens !== null) {
      headers.set("x-ratelimit-limit-tokens", String(Math.floor(metadata.limitTokens)));
    }
    if (metadata.remainingTokens !== null) {
      headers.set("x-ratelimit-remaining-tokens", String(Math.floor(metadata.remainingTokens)));
    }
    if (metadata.limitProjectTokens !== null) {
      headers.set("x-ratelimit-limit-project-tokens", String(Math.floor(metadata.limitProjectTokens)));
    }
    if (metadata.remainingProjectTokens !== null) {
      headers.set("x-ratelimit-remaining-project-tokens", String(Math.floor(metadata.remainingProjectTokens)));
    }
  }
  return Response.json({ error: normalized.error }, { status: normalized.status, headers });
}

export function apiErrorEvent(error: unknown): {
  type: "error";
  message: string;
  param: string | null;
  code: string | null;
} {
  const normalized = normalizeApiError(error);
  return {
    type: "error",
    message: normalized.error.message,
    param: normalized.error.param,
    code: normalized.error.code,
  };
}
