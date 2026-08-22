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

  const daemon = normalizeDaemonError(error);
  if (daemon) return daemon;

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
  return Response.json({ error: normalized.error }, { status: normalized.status });
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
