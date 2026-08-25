import { randomUUID } from "node:crypto";
import { normalizeApiError } from "../http/errors";
import { providerErrorEnvelopeFrom } from "../v2/provider-error";

export type AnthropicErrorEnvelope = {
  type: "error";
  error: {
    type: string;
    message: string;
  };
  request_id: string;
};

export class AnthropicRequestError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly errorType = "invalid_request_error",
  ) {
    super(message);
    this.name = "AnthropicRequestError";
  }
}

export function newAnthropicRequestId(): string {
  return `req_${randomUUID().replaceAll("-", "")}`;
}

export function anthropicErrorType(status: number): string {
  if (status === 400) return "invalid_request_error";
  if (status === 401) return "authentication_error";
  if (status === 402) return "billing_error";
  if (status === 403) return "permission_error";
  if (status === 404) return "not_found_error";
  if (status === 409) return "conflict_error";
  if (status === 413) return "request_too_large";
  if (status === 429) return "rate_limit_error";
  if (status === 504) return "timeout_error";
  if (status === 529 || status === 503) return "overloaded_error";
  if (status >= 400 && status < 500) return "invalid_request_error";
  return "api_error";
}

function anthropicStatus(status: number): number {
  if (status === 503) return 529;
  if (status === 504 || status === 529) return status;
  if (status >= 500) return 500;
  if (status >= 400) return status;
  return 500;
}

function normalizeAnthropicError(error: unknown): {
  status: number;
  type: string;
  message: string;
} {
  if (error instanceof AnthropicRequestError) {
    const status = anthropicStatus(error.status);
    return {
      status,
      type: anthropicErrorType(status),
      message: error.message,
    };
  }

  const normalized = normalizeApiError(error);
  const status = anthropicStatus(normalized.status);
  return {
    status,
    type: anthropicErrorType(status),
    message: safeAnthropicMessage(status),
  };
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

function safeAnthropicMessage(status: number): string {
  if (status === 400) return "Invalid request";
  if (status === 401) return "Authentication error";
  if (status === 402) return "Billing error";
  if (status === 403) return "Permission denied";
  if (status === 404) return "Resource not found";
  if (status === 409) return "Request conflict";
  if (status === 413) return "Request too large";
  if (status === 429) return "Rate limit exceeded";
  if (status === 504) return "Request timed out";
  if (status === 529) return "Service temporarily overloaded";
  if (status >= 400 && status < 500) return "Invalid request";
  return "Internal server error";
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function headerValue(value: unknown, name: string): string | null {
  if (value instanceof Headers) return value.get(name);
  const headers = record(value);
  if (!headers) return null;
  const entry = Object.entries(headers).find(([key]) => key.toLowerCase() === name.toLowerCase());
  const candidate = entry?.[1];
  if (Array.isArray(candidate)) return typeof candidate[0] === "string" ? candidate[0] : null;
  return typeof candidate === "string" ? candidate : null;
}

function errorHeaders(error: unknown): Headers {
  const output = new Headers();
  const value = record(error);
  const response = record(value?.response);
  const details = record(value?.details);
  const provider = providerErrorEnvelopeFrom(error);
  const candidates = [
    value?.headers,
    response?.headers,
    details?.headers,
    provider?.headers,
  ];
  for (const candidate of candidates) {
    const retryAfter = safeRetryAfter(headerValue(candidate, "retry-after"));
    if (retryAfter) {
      output.set("retry-after", retryAfter);
    }
    const shouldRetry = headerValue(candidate, "x-should-retry")?.trim();
    if (shouldRetry === "true" || shouldRetry === "false") {
      output.set("x-should-retry", shouldRetry);
    }
  }
  return output;
}

function anthropicErrorEnvelope(
  normalized: ReturnType<typeof normalizeAnthropicError>,
  requestId: string,
): AnthropicErrorEnvelope {
  return {
    type: "error",
    error: {
      type: normalized.type,
      message: normalized.message,
    },
    request_id: requestId,
  };
}

export function anthropicErrorResponse(error: unknown, requestId: string): Response {
  const normalized = normalizeAnthropicError(error);
  const headers = errorHeaders(error);
  headers.set("request-id", requestId);
  return Response.json(anthropicErrorEnvelope(normalized, requestId), {
    status: normalized.status,
    headers,
  });
}

export function anthropicErrorEvent(error: unknown, requestId: string): AnthropicErrorEnvelope {
  return anthropicErrorEnvelope(normalizeAnthropicError(error), requestId);
}
