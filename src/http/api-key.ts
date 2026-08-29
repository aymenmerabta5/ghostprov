import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";

const AUTHENTICATED_TENANT_SCOPE_BYTES = 32;
const AUTHENTICATED_TENANT_SCOPES = new WeakMap<object, Buffer>();
declare const AUTHENTICATED_TENANT_SCOPE: unique symbol;

/**
 * Opaque process-local proof that a request crossed GhostProv's server-owned
 * authentication boundary. Scope authority cannot be reconstructed from a
 * request body, header, client ID, or serialized value.
 */
export type AuthenticatedTenantScope = Readonly<{
  readonly [AUTHENTICATED_TENANT_SCOPE]: true;
}>;

/** Creates one application scope for all authenticated protocol surfaces. */
export function createAuthenticatedTenantScope(): AuthenticatedTenantScope {
  const scope = Object.freeze(Object.create(null)) as AuthenticatedTenantScope;
  AUTHENTICATED_TENANT_SCOPES.set(scope, randomBytes(AUTHENTICATED_TENANT_SCOPE_BYTES));
  return scope;
}

export function isAuthenticatedTenantScope(value: unknown): value is AuthenticatedTenantScope {
  return !!value && typeof value === "object" && AUTHENTICATED_TENANT_SCOPES.has(value);
}

/**
 * Derives a fixed-size index key without exposing or retaining the underlying
 * application scope bytes.
 */
export function authenticatedTenantScopeHmac(
  scope: AuthenticatedTenantScope,
  key: Uint8Array,
  domain: string,
): string {
  if (!isAuthenticatedTenantScope(scope)) {
    throw new TypeError("Authenticated tenant scope is missing or invalid");
  }
  const secret = AUTHENTICATED_TENANT_SCOPES.get(scope)!;
  return createHmac("sha256", Buffer.from(key))
    .update(domain, "utf8")
    .update(secret)
    .digest("base64url");
}

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
