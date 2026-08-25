/**
 * Rate-limit metadata is deliberately small and serializable because it crosses
 * the V2 Awareness boundary. The host treats every value as observation-only;
 * callers receive the provider failure without a derived admission decision.
 */
export interface RateLimitHint {
  /** HTTP Retry-After, normalized to milliseconds. */
  retryAfterMs?: number;
  /** x-ratelimit-reset-tokens, normalized to milliseconds. */
  resetTokensMs?: number;
  /** x-ratelimit-limit-tokens. */
  limitTokens?: number;
  /** x-ratelimit-remaining-tokens. */
  remainingTokens?: number;
  /** x-ratelimit-limit-project-tokens, when a project-scoped limit applies. */
  limitProjectTokens?: number;
  /** x-ratelimit-remaining-project-tokens, when present. */
  remainingProjectTokens?: number;
  /** x-ratelimit-reset-project-tokens, normalized to milliseconds. */
  resetProjectTokensMs?: number;
}

/** Public, credential-scrubbed detail retained for the operator and caller. */
export interface RateLimitProblem {
  status: 429;
  message: string;
  type: string;
  code: string;
  param: string | null;
  model: string | null;
  organization: string | null;
  limitKind: string | null;
  tokenLimit: number | null;
  usedTokens: number | null;
  requestedTokens: number | null;
  retryAfterMs: number | null;
}

// The documented reset example is 6m0s. Keep a generous upper bound so parsing
// cannot retain an unbounded or nonsensical provider duration.
export const MAX_RATE_LIMIT_DELAY_MS = 60 * 60_000;
const DEFAULT_RATE_LIMIT_DELAY_MS = 1_000;

function boundedMs(value: number): number | undefined {
  if (!Number.isFinite(value) || value < 0) return undefined;
  return Math.min(MAX_RATE_LIMIT_DELAY_MS, Math.max(0, Math.ceil(value)));
}

function nonNegativeInteger(value: string | null): number | undefined {
  if (!value?.trim() || !/^\d+$/.test(value.trim())) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function safeCategory(value: unknown, fallback: string): string {
  const candidate = text(value);
  return candidate && /^[A-Za-z][A-Za-z0-9._-]{0,127}$/.test(candidate)
    ? candidate
    : fallback;
}

/** Keeps the upstream explanation intact while removing credential-shaped data. */
export function sanitizeRateLimitMessage(value: unknown): string {
  const raw = text(value) ?? "Upstream token rate limit was reached";
  return raw
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\b(?:Bearer|Basic|Digest)\s+[^\s,;]+/gi, match => `${match.split(/\s+/, 1)[0]} [REDACTED]`)
    .replace(/(\b(?:authorization|cookie|set-cookie|api[_-]?key|secret|password|access[_-]?token)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]")
    .trim()
    .slice(0, 2_000) || "Upstream token rate limit was reached";
}

/** Parses the HTTP Retry-After seconds form used by the OpenAI API. */
export function parseRetryAfterMs(value: string | null | undefined): number | undefined {
  if (!value?.trim()) return undefined;
  const seconds = Number(value.trim());
  if (Number.isFinite(seconds) && seconds >= 0) return boundedMs(seconds * 1_000);
  const date = Date.parse(value);
  return Number.isFinite(date) ? boundedMs(date - Date.now()) : undefined;
}

/** Parses OpenAI duration values such as `42ms`, `1.5s`, and `6m0s`. */
export function parseRateLimitDurationMs(value: string | null | undefined): number | undefined {
  const raw = value?.trim();
  if (!raw) return undefined;
  const pattern = /(\d+(?:\.\d+)?)\s*(ms|s|m|h)/gi;
  let total = 0;
  let covered = "";
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(raw))) {
    covered += match[0];
    const amount = Number(match[1]);
    const unit = match[2]!.toLowerCase();
    const factor = unit === "ms" ? 1 : unit === "s" ? 1_000 : unit === "m" ? 60_000 : 3_600_000;
    total += amount * factor;
  }
  if (!covered || covered.replace(/\s+/g, "") !== raw.replace(/\s+/g, "")) return undefined;
  return boundedMs(total);
}

export function rateLimitHintFromHeaders(headers: Headers): RateLimitHint | undefined {
  const retryAfterMs = parseRetryAfterMs(headers.get("retry-after"));
  const resetTokensMs = parseRateLimitDurationMs(headers.get("x-ratelimit-reset-tokens"));
  const limitTokens = nonNegativeInteger(headers.get("x-ratelimit-limit-tokens"));
  const remainingTokens = nonNegativeInteger(headers.get("x-ratelimit-remaining-tokens"));
  const limitProjectTokens = nonNegativeInteger(headers.get("x-ratelimit-limit-project-tokens"));
  const remainingProjectTokens = nonNegativeInteger(headers.get("x-ratelimit-remaining-project-tokens"));
  const resetProjectTokensMs = parseRateLimitDurationMs(headers.get("x-ratelimit-reset-project-tokens"));
  return retryAfterMs === undefined &&
      resetTokensMs === undefined &&
      limitTokens === undefined &&
      remainingTokens === undefined &&
      limitProjectTokens === undefined &&
      remainingProjectTokens === undefined &&
      resetProjectTokensMs === undefined
    ? undefined
    : {
      ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      ...(resetTokensMs === undefined ? {} : { resetTokensMs }),
      ...(limitTokens === undefined ? {} : { limitTokens }),
      ...(remainingTokens === undefined ? {} : { remainingTokens }),
      ...(limitProjectTokens === undefined ? {} : { limitProjectTokens }),
      ...(remainingProjectTokens === undefined ? {} : { remainingProjectTokens }),
      ...(resetProjectTokensMs === undefined ? {} : { resetProjectTokensMs }),
    };
}

export function retryAfterFromMessage(value: unknown): number | undefined {
  const message = text(value);
  if (!message) return undefined;
  const match = /(?:please\s+)?try\s+again\s+in\s+(\d+(?:\.\d+)?)\s*(milliseconds?|ms|seconds?|secs?|s|minutes?|mins?|m)\b/i.exec(message);
  if (!match) return undefined;
  const amount = Number(match[1]);
  const unit = match[2]!.toLowerCase();
  const factor = unit.startsWith("m") && unit !== "ms"
    ? 60_000
    : unit.startsWith("s")
      ? 1_000
      : 1;
  return boundedMs(amount * factor);
}

function tokenUsageFromMessage(value: unknown): {
  limitTokens: number;
  usedTokens: number;
  requestedTokens: number;
} | undefined {
  const message = text(value);
  if (!message) return undefined;
  const match = /\bTPM\b\s*\)?\s*:\s*Limit\s+([\d,]+)\s*,\s*Used\s+([\d,]+)\s*,\s*Requested\s+([\d,]+)/i.exec(message);
  if (!match) return undefined;
  const values = match.slice(1, 4).map(value => Number(value.replace(/,/g, "")));
  const [limitTokens, usedTokens, requestedTokens] = values;
  if (
    !Number.isSafeInteger(limitTokens) || limitTokens! < 1 ||
    !Number.isSafeInteger(usedTokens) || usedTokens! < 0 ||
    !Number.isSafeInteger(requestedTokens) || requestedTokens! < 0
  ) return undefined;
  return { limitTokens: limitTokens!, usedTokens: usedTokens!, requestedTokens: requestedTokens! };
}

/**
 * Proxies occasionally omit rate-limit headers on a streamed failure. The
 * canonical OpenAI TPM message still carries a safe conservative snapshot.
 */
export function tokenSnapshotFromMessage(value: unknown): RateLimitHint | undefined {
  const usage = tokenUsageFromMessage(value);
  if (!usage) return undefined;
  return {
    limitTokens: usage.limitTokens,
    remainingTokens: Math.max(0, usage.limitTokens - usage.usedTokens),
  };
}

interface ExtractedRateLimitFields {
  status?: number;
  code?: string;
  type?: string;
  message?: string;
  param?: string | null;
  hint?: RateLimitHint;
}

function statusFromCode(value: unknown): number | undefined {
  const candidate = text(value);
  const match = candidate && /^(?:http|rate_limited)_(\d{3})$/i.exec(candidate);
  if (!match) return undefined;
  const status = Number(match[1]);
  return status >= 400 && status <= 599 ? status : undefined;
}

function statusFromMessage(value: unknown): number | undefined {
  const candidate = text(value);
  if (!candidate) return undefined;
  const match = /(?:\blast\s+status\s*:\s*|\bHTTP\s+)(\d{3})\b|\b(429)\s+Too\s+Many\s+Requests\b/i.exec(candidate);
  const status = Number(match?.[1] ?? match?.[2]);
  return Number.isInteger(status) && status >= 400 && status <= 599 ? status : undefined;
}

function hintFromRecord(value: unknown): RateLimitHint | undefined {
  const hint = record(value);
  if (!hint) return undefined;
  const retryAfterMs = typeof hint.retryAfterMs === "number" ? boundedMs(hint.retryAfterMs) : undefined;
  const resetTokensMs = typeof hint.resetTokensMs === "number" ? boundedMs(hint.resetTokensMs) : undefined;
  const limitTokens = typeof hint.limitTokens === "number" && Number.isSafeInteger(hint.limitTokens) && hint.limitTokens >= 0
    ? hint.limitTokens
    : undefined;
  const remainingTokens = typeof hint.remainingTokens === "number" && Number.isSafeInteger(hint.remainingTokens) && hint.remainingTokens >= 0
    ? hint.remainingTokens
    : undefined;
  const limitProjectTokens = typeof hint.limitProjectTokens === "number" && Number.isSafeInteger(hint.limitProjectTokens) && hint.limitProjectTokens >= 0
    ? hint.limitProjectTokens
    : undefined;
  const remainingProjectTokens = typeof hint.remainingProjectTokens === "number" && Number.isSafeInteger(hint.remainingProjectTokens) && hint.remainingProjectTokens >= 0
    ? hint.remainingProjectTokens
    : undefined;
  const resetProjectTokensMs = typeof hint.resetProjectTokensMs === "number"
    ? boundedMs(hint.resetProjectTokensMs)
    : undefined;
  return retryAfterMs === undefined &&
      resetTokensMs === undefined &&
      limitTokens === undefined &&
      remainingTokens === undefined &&
      limitProjectTokens === undefined &&
      remainingProjectTokens === undefined &&
      resetProjectTokensMs === undefined
    ? undefined
    : {
      ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      ...(resetTokensMs === undefined ? {} : { resetTokensMs }),
      ...(limitTokens === undefined ? {} : { limitTokens }),
      ...(remainingTokens === undefined ? {} : { remainingTokens }),
      ...(limitProjectTokens === undefined ? {} : { limitProjectTokens }),
      ...(remainingProjectTokens === undefined ? {} : { remainingProjectTokens }),
      ...(resetProjectTokensMs === undefined ? {} : { resetProjectTokensMs }),
    };
}

function embeddedJson(value: unknown): unknown {
  const candidate = text(value);
  if (!candidate) return null;
  const marker = "upstream failed:";
  const marked = candidate.toLowerCase().indexOf(marker);
  const raw = marked >= 0 ? candidate.slice(marked + marker.length).trim() : candidate;
  if (!raw.startsWith("{") && !raw.startsWith("[")) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function mergeErrorFields(
  outer: ExtractedRateLimitFields,
  inner: ExtractedRateLimitFields,
): ExtractedRateLimitFields {
  const status = inner.status ?? outer.status;
  const code = inner.code ?? outer.code;
  const type = inner.type ?? outer.type;
  const message = inner.message ?? outer.message;
  const hint = mergeRateLimitHints(outer.hint, inner.hint);
  return {
    ...(status === undefined ? {} : { status }),
    ...(code === undefined ? {} : { code }),
    ...(type === undefined ? {} : { type }),
    ...(message === undefined ? {} : { message }),
    ...(inner.param !== undefined
      ? { param: inner.param }
      : outer.param !== undefined
        ? { param: outer.param }
        : {}),
    ...(hint ? { hint } : {}),
  };
}

function errorFields(
  value: unknown,
  depth = 0,
  seen: Set<object> = new Set(),
): ExtractedRateLimitFields {
  if (depth > 6) return {};
  const root = record(value);
  if (!root) {
    const message = text(value);
    return message
      ? {
        message,
        ...(statusFromMessage(message) === undefined ? {} : { status: statusFromMessage(message) }),
      }
      : {};
  }
  if (seen.has(root)) return {};
  seen.add(root);
  const nested = record(root?.error);
  const candidate = nested ?? root;
  const message = text(candidate?.message) ?? text(root.message) ?? undefined;
  const explicitStatus = typeof candidate.status === "number"
    ? candidate.status
    : typeof root.status === "number"
      ? root.status
      : undefined;
  const inferredStatus = statusFromCode(candidate.code) ??
    statusFromCode(root.code) ??
    statusFromMessage(message);
  const status = explicitStatus ?? inferredStatus;
  const hint = mergeRateLimitHints(
    hintFromRecord(root.rateLimit),
    hintFromRecord(candidate.rateLimit),
  );
  let output: ExtractedRateLimitFields = {
    ...(status === undefined ? {} : { status }),
    ...(text(candidate?.code) ? { code: text(candidate?.code)! } : {}),
    ...(text(candidate?.type) ? { type: text(candidate?.type)! } : {}),
    ...(message ? { message } : {}),
    ...(typeof candidate.param === "string" || candidate.param === null
      ? { param: candidate.param as string | null }
      : {}),
    ...(hint ? { hint } : {}),
  };

  for (const child of [nested, record(root.details), record(root.cause), embeddedJson(message)]) {
    if (!child || child === root) continue;
    output = mergeErrorFields(output, errorFields(child, depth + 1, seen));
  }
  return output;
}

export function isRateLimited(value: unknown): boolean {
  const fields = errorFields(value);
  if (fields.status === 429) return true;
  const identity = `${fields.code ?? ""} ${fields.type ?? ""}`.toLowerCase();
  if (/(?:^|[_\s-])rate[_\s-]?limit/.test(identity)) return true;
  return /\brate limit(?:ed)?\b|\btokens?\s+per\s+min(?:ute)?\b|\bTPM\b|\btoo many requests\b/i.test(fields.message ?? "");
}

/** Extracts the exact safe upstream explanation used by the API and viewer. */
export function rateLimitProblemFromError(value: unknown): RateLimitProblem | undefined {
  const fields = errorFields(value);
  if (!isRateLimited(value)) return undefined;
  const message = sanitizeRateLimitMessage(fields.message);
  const usage = tokenUsageFromMessage(message);
  const modelMatch = /\brate limit(?:ed)?\s+(?:reached\s+)?for\s+([A-Za-z0-9][A-Za-z0-9._:-]{0,127})\b/i.exec(message);
  const organizationMatch = /\bin organization\s+([A-Za-z0-9][A-Za-z0-9_-]{0,127})\b/i.exec(message);
  const limitKindMatch = /\bon\s+([A-Za-z][A-Za-z ]{1,63})\s*\((?:TPM|RPM)\)\s*:/i.exec(message);
  const limitKind = limitKindMatch?.[1]
    ?.trim()
    .toLowerCase()
    .replace(/\s+/g, "_") ?? null;
  return Object.freeze({
    status: 429 as const,
    message,
    type: safeCategory(fields.type, "rate_limit_error"),
    code: safeCategory(fields.code, "rate_limit_exceeded"),
    param: fields.param ?? null,
    model: modelMatch?.[1]?.toLowerCase() ?? null,
    organization: organizationMatch?.[1] ?? null,
    limitKind,
    tokenLimit: usage?.limitTokens ?? null,
    usedTokens: usage?.usedTokens ?? null,
    requestedTokens: usage?.requestedTokens ?? null,
    retryAfterMs: retryAfterFromMessage(message) ?? null,
  });
}

export function mergeRateLimitHints(
  ...values: readonly (RateLimitHint | undefined)[]
): RateLimitHint | undefined {
  let retryAfterMs: number | undefined;
  let resetTokensMs: number | undefined;
  let limitTokens: number | undefined;
  let remainingTokens: number | undefined;
  let limitProjectTokens: number | undefined;
  let remainingProjectTokens: number | undefined;
  let resetProjectTokensMs: number | undefined;
  for (const value of values) {
    if (!value) continue;
    if (value.retryAfterMs !== undefined) {
      retryAfterMs = Math.max(retryAfterMs ?? 0, value.retryAfterMs);
    }
    if (value.resetTokensMs !== undefined) {
      resetTokensMs = Math.max(resetTokensMs ?? 0, value.resetTokensMs);
    }
    if (value.limitTokens !== undefined) limitTokens = value.limitTokens;
    if (value.remainingTokens !== undefined) {
      remainingTokens = Math.min(remainingTokens ?? value.remainingTokens, value.remainingTokens);
    }
    if (value.limitProjectTokens !== undefined) limitProjectTokens = value.limitProjectTokens;
    if (value.remainingProjectTokens !== undefined) {
      remainingProjectTokens = Math.min(remainingProjectTokens ?? value.remainingProjectTokens, value.remainingProjectTokens);
    }
    if (value.resetProjectTokensMs !== undefined) {
      resetProjectTokensMs = Math.max(resetProjectTokensMs ?? 0, value.resetProjectTokensMs);
    }
  }
  return retryAfterMs === undefined &&
      resetTokensMs === undefined &&
      limitTokens === undefined &&
      remainingTokens === undefined &&
      limitProjectTokens === undefined &&
      remainingProjectTokens === undefined &&
      resetProjectTokensMs === undefined
    ? undefined
    : {
      ...(retryAfterMs === undefined ? {} : { retryAfterMs }),
      ...(resetTokensMs === undefined ? {} : { resetTokensMs }),
      ...(limitTokens === undefined ? {} : { limitTokens }),
      ...(remainingTokens === undefined ? {} : { remainingTokens }),
      ...(limitProjectTokens === undefined ? {} : { limitProjectTokens }),
      ...(remainingProjectTokens === undefined ? {} : { remainingProjectTokens }),
      ...(resetProjectTokensMs === undefined ? {} : { resetProjectTokensMs }),
    };
}

/**
 * Extracts a rate-limit signal from either a Responses terminal object or an
 * error object.  Header metadata is only used when the response itself is a
 * rate-limit failure, so normal successful responses never pause the fleet.
 */
export function rateLimitHintFromError(
  value: unknown,
  headerHint?: RateLimitHint,
): RateLimitHint | undefined {
  const fields = errorFields(value);
  const messageSnapshot = tokenSnapshotFromMessage(fields.message);
  const observedHint = mergeRateLimitHints(fields.hint, messageSnapshot, headerHint);
  const exhaustedByHeader = observedHint?.retryAfterMs !== undefined ||
    observedHint?.remainingTokens === 0 ||
    observedHint?.remainingProjectTokens === 0;
  if (!isRateLimited(value) && !exhaustedByHeader) return undefined;
  const retryAfterMs = retryAfterFromMessage(fields.message);
  return mergeRateLimitHints(
    observedHint,
    retryAfterMs === undefined ? undefined : { retryAfterMs },
  )
    ?? { retryAfterMs: DEFAULT_RATE_LIMIT_DELAY_MS };
}
