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
const MIN_GHOSTPROV_API_KEY_CHARACTERS = 12;

export interface ApiKeyHeaders {
  authorization?: string | readonly string[] | null;
  xApiKey?: string | readonly string[] | null;
}

export interface PublicRequestAuthorityHeaders {
  host?: string | readonly string[] | null;
  forwardedHost?: string | readonly string[] | null;
  origin?: string | readonly string[] | null;
}

export type PublicRequestAuthorityFailure = "host" | "origin";
const EMPTY_ALLOWED_HOSTNAMES: ReadonlySet<string> = new Set();

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
  const apiKey = value?.trim() ?? "";
  if (!apiKey) {
    throw new TypeError("GHOSTPROV_API_KEY is required");
  }
  if (apiKey.length < MIN_GHOSTPROV_API_KEY_CHARACTERS) {
    throw new TypeError(
      `GHOSTPROV_API_KEY must contain at least ${MIN_GHOSTPROV_API_KEY_CHARACTERS} characters`,
    );
  }
  if (apiKey.length > 4_096 || /[\s,\u0000-\u001f\u007f]/.test(apiKey)) {
    throw new TypeError(
      "GHOSTPROV_API_KEY must be a single-token value of at most 4096 characters",
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

function normalizedHostname(value: string): string {
  return value.toLowerCase().replace(/^\[|\]$/g, "").replace(/\.$/, "");
}

function loopbackHostname(value: string): boolean {
  const hostname = normalizedHostname(value);
  if (
    hostname === "localhost" || hostname.endsWith(".localhost") ||
    hostname === "::1"
  ) {
    return true;
  }
  const ipv4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(hostname);
  return !!ipv4 && Number(ipv4[1]) === 127 &&
    ipv4.slice(1).every(component => Number(component) <= 255);
}

function authorityHostname(value: string | readonly string[] | null | undefined): string | null {
  const authority = singleHeader(value)?.trim();
  if (!authority || /[/?#@]/.test(authority)) return null;
  try {
    const parsed = new URL(`http://${authority}`);
    return parsed.username || parsed.password || parsed.pathname !== "/" ||
        parsed.search || parsed.hash
      ? null
      : normalizedHostname(parsed.hostname);
  } catch {
    return null;
  }
}

function allowedHostnames(values: readonly string[]): ReadonlySet<string> {
  if (values.length === 0) return EMPTY_ALLOWED_HOSTNAMES;
  const allowed = new Set<string>();
  for (const value of values) {
    const hostname = authorityHostname(value);
    if (!hostname) throw new TypeError("Allowed public host is invalid");
    allowed.add(hostname);
  }
  return allowed;
}

function hostnameAllowed(hostname: string | null, allowed: ReadonlySet<string>): boolean {
  return hostname !== null && (loopbackHostname(hostname) || allowed.has(hostname));
}

/**
 * Validates the HTTP authority before authentication or body admission. Missing
 * Origin is expected for CLI clients; a browser Origin, when present, must be
 * loopback or explicitly allowlisted just like Host.
 */
export function publicRequestAuthorityFailure(
  headers: PublicRequestAuthorityHeaders,
  additionalAllowedHosts: readonly string[] = [],
): PublicRequestAuthorityFailure | null {
  const allowed = allowedHostnames(additionalAllowedHosts);
  if (!hostnameAllowed(authorityHostname(headers.host), allowed)) return "host";
  if (
    headers.forwardedHost !== undefined && headers.forwardedHost !== null &&
    !hostnameAllowed(authorityHostname(headers.forwardedHost), allowed)
  ) return "host";

  if (headers.origin === undefined || headers.origin === null) return null;
  const origin = singleHeader(headers.origin)?.trim();
  if (!origin || origin === "null") return "origin";
  try {
    const parsed = new URL(origin);
    if (
      (parsed.protocol !== "http:" && parsed.protocol !== "https:") ||
      parsed.username || parsed.password || parsed.pathname !== "/" ||
      parsed.search || parsed.hash ||
      !hostnameAllowed(normalizedHostname(parsed.hostname), allowed)
    ) return "origin";
  } catch {
    return "origin";
  }
  return null;
}
