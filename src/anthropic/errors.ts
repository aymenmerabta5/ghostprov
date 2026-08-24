import { randomUUID } from "node:crypto";
import { normalizeApiError } from "../http/errors";

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
  return "api_error";
}

function normalizeAnthropicError(error: unknown): {
  status: number;
  type: string;
  message: string;
} {
  if (error instanceof AnthropicRequestError) {
    return {
      status: error.status,
      type: error.errorType,
      message: error.message,
    };
  }

  const normalized = normalizeApiError(error);
  const status = normalized.status === 503 ? 529 : normalized.status;
  return {
    status,
    type: anthropicErrorType(status),
    message: normalized.error.message,
  };
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
  return Response.json(anthropicErrorEnvelope(normalized, requestId), {
    status: normalized.status,
    headers: { "request-id": requestId },
  });
}

export function anthropicErrorEvent(error: unknown, requestId: string): AnthropicErrorEnvelope {
  return anthropicErrorEnvelope(normalizeAnthropicError(error), requestId);
}
