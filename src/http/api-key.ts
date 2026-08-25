import { createHash, timingSafeEqual } from "node:crypto";

export type ApiKeySurface = "openai" | "anthropic";
export const DEFAULT_GHOSTPROV_API_KEY = "GHOST_IS_KING";

export interface ApiKeyHeaders {
  authorization?: string | readonly string[] | null;
  xApiKey?: string | readonly string[] | null;
}

function singleHeader(value: string | readonly string[] | null | undefined): string | null {
  if (Array.isArray(value)) return value.length === 1 ? value[0] ?? null : null;
  return typeof value === "string" ? value : null;
}

function bearerToken(value: string | readonly string[] | null | undefined): string | null {
  const header = singleHeader(value);
  if (!header) return null;
  const match = /^Bearer[\t ]+([^\s,]+)$/i.exec(header.trim());
  return match?.[1] ?? null;
}

function exactApiKey(value: string | readonly string[] | null | undefined): string | null {
  const header = singleHeader(value)?.trim();
  return header && !/[\s,]/.test(header) ? header : null;
}

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest();
}

export function resolveGhostProvApiKey(value: string | undefined): string {
  const apiKey = (value ?? DEFAULT_GHOSTPROV_API_KEY).trim();
  if (!apiKey || apiKey.length > 4_096 || /[\u0000-\u001f\u007f]/.test(apiKey)) {
    throw new TypeError(
      "GHOSTPROV_API_KEY must be a nonempty single-line value of at most 4096 characters",
    );
  }
  return apiKey;
}

/** Compares fixed-size digests so key length does not select a fast path. */
export function apiKeyMatches(candidate: string | null, expected: string): boolean {
  const candidateDigest = digest(candidate ?? "");
  const expectedDigest = digest(expected);
  return candidate !== null && timingSafeEqual(candidateDigest, expectedDigest);
}

export function apiKeyAuthorized(
  headers: ApiKeyHeaders,
  expected: string,
  surface: ApiKeySurface,
): boolean {
  const bearer = bearerToken(headers.authorization);
  if (surface === "openai") return apiKeyMatches(bearer, expected);
  const anthropic = exactApiKey(headers.xApiKey);
  return apiKeyMatches(anthropic, expected) || apiKeyMatches(bearer, expected);
}

export function isProtectedApiPath(path: string): boolean {
  return path === "/v1" || path.startsWith("/v1/");
}
