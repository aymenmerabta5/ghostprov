import {
  isRateLimited,
  rateLimitHintFromError,
  rateLimitProblemFromError,
} from "../v2/rate-limit";
import {
  providerErrorEnvelopeFrom,
  providerErrorPayloadJson,
  type ProviderErrorEnvelope,
} from "../v2/provider-error";

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
  providerError?: ProviderErrorEnvelope;
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

  const providerError = providerErrorEnvelopeFrom(error);
  if (providerError) {
    return {
      status: providerError.status,
      error: { ...providerError.error },
      providerError,
    };
  }

  const structured = record(error);
  if (
    structured?.name === "V2PreCommitError" &&
    structured.code === "request_too_large"
  ) {
    return {
      status: 413,
      error: {
        message: "Request body exceeds the V2 transport limit",
        type: "invalid_request_error",
        param: "body",
        code: "request_too_large",
      },
    };
  }
  if (structured?.name === "ResourceReservationTooLargeError") {
    return {
      status: 413,
      error: {
        message: "Request body is too large for this GhostProv host",
        type: "invalid_request_error",
        param: "body",
        code: "request_too_large",
      },
    };
  }
  if (
    structured?.name === "ResourceGovernorQueueFullError" ||
    structured?.name === "ResourceGovernorReplacementOverloadError" ||
    structured?.name === "ResourceGovernorTimeoutError" ||
    structured?.name === "ResourceGovernorClosedError"
  ) {
    const code = structured.name === "ResourceGovernorQueueFullError" ||
        structured.name === "ResourceGovernorReplacementOverloadError"
      ? structured.name === "ResourceGovernorReplacementOverloadError"
        ? "resource_replacement_overloaded"
        : "resource_admission_overloaded"
      : structured.name === "ResourceGovernorTimeoutError"
        ? "resource_admission_timeout"
        : "resource_governor_closed";
    return {
      status: 503,
      error: {
        message: structured.name === "ResourceGovernorTimeoutError"
          ? "GhostProv resource admission timed out"
          : structured.name === "ResourceGovernorClosedError"
            ? "GhostProv resource admission is shutting down"
            : "GhostProv resource admission is temporarily overloaded",
        type: structured.name === "ResourceGovernorQueueFullError" ||
            structured.name === "ResourceGovernorReplacementOverloadError"
          ? "overloaded_error"
          : "service_unavailable_error",
        param: null,
        code,
      },
    };
  }
  if (
    structured?.name === "CpuTransformQueueFullError" ||
    structured?.name === "CpuTransformTimeoutError"
  ) {
    const timedOut = structured.name === "CpuTransformTimeoutError";
    return {
      status: 503,
      error: {
        message: timedOut
          ? "GhostProv request preparation timed out"
          : "GhostProv request preparation is temporarily overloaded",
        type: timedOut ? "service_unavailable_error" : "overloaded_error",
        param: null,
        code: timedOut ? "cpu_transform_timeout" : "cpu_transform_overloaded",
      },
    };
  }
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
    structured?.name === "RouterQueueFullError" &&
    structured.status === 503 &&
    structured.code === "router_queue_full"
  ) {
    return {
      status: 503,
      error: {
        message: "GhostProv routing is temporarily overloaded",
        type: "overloaded_error",
        param: null,
        code: "router_queue_full",
      },
    };
  }
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
      return { status: 503, error: { message, type: "overloaded_error", param: null, code: "capacity_exhausted" } };
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
  const providerError = normalized.providerError;
  if (providerError) {
    for (const [name, value] of Object.entries(providerError.headers)) {
      if (value !== undefined) headers.set(name, value);
    }
    if (providerError.upstreamRequestId) {
      headers.set("x-request-id", providerError.upstreamRequestId);
    }
  }
  if (normalized.status === 429) {
    const retryAfterMs = metadata.retryAfterMs;
    if (retryAfterMs !== null && !headers.has("retry-after")) {
      headers.set("retry-after", String(Math.max(1, Math.ceil(retryAfterMs / 1_000))));
    }
    const resetTokensMs = metadata.resetTokensMs;
    if (resetTokensMs !== null && !headers.has("x-ratelimit-reset-tokens")) {
      headers.set("x-ratelimit-reset-tokens", `${Math.ceil(resetTokensMs)}ms`);
    }
    const resetProjectTokensMs = metadata.resetProjectTokensMs;
    if (resetProjectTokensMs !== null && !headers.has("x-ratelimit-reset-project-tokens")) {
      headers.set("x-ratelimit-reset-project-tokens", `${Math.ceil(resetProjectTokensMs)}ms`);
    }
    if (metadata.limitTokens !== null && !headers.has("x-ratelimit-limit-tokens")) {
      headers.set("x-ratelimit-limit-tokens", String(Math.floor(metadata.limitTokens)));
    }
    if (metadata.remainingTokens !== null && !headers.has("x-ratelimit-remaining-tokens")) {
      headers.set("x-ratelimit-remaining-tokens", String(Math.floor(metadata.remainingTokens)));
    }
    if (metadata.limitProjectTokens !== null && !headers.has("x-ratelimit-limit-project-tokens")) {
      headers.set("x-ratelimit-limit-project-tokens", String(Math.floor(metadata.limitProjectTokens)));
    }
    if (metadata.remainingProjectTokens !== null && !headers.has("x-ratelimit-remaining-project-tokens")) {
      headers.set("x-ratelimit-remaining-project-tokens", String(Math.floor(metadata.remainingProjectTokens)));
    }
  }
  const original = providerError?.origin === "http"
    ? providerErrorPayloadJson(providerError)
    : null;
  return Response.json(original ?? { error: normalized.error }, {
    status: normalized.status,
    headers,
  });
}

export function apiErrorEvent(error: unknown): Record<string, unknown> | string {
  const normalized = normalizeApiError(error);
  if (normalized.providerError) {
    const original = providerErrorPayloadJson(normalized.providerError);
    if (original) return original;
    if (
      normalized.providerError.payload.format === "text" &&
      !normalized.providerError.payload.truncated
    ) return normalized.providerError.payload.text;
    return {
      type: "error",
      error: normalized.error,
    };
  }
  return {
    type: "error",
    message: normalized.error.message,
    param: normalized.error.param,
    code: normalized.error.code,
  };
}
