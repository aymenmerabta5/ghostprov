import { STANDALONE_SEARCH_MAX_REQUEST_BYTES } from "../search/limits";

export const ANTHROPIC_HTTP_MAX_BODY_BYTES = 32 * 1024 * 1024;

export function requestPath(value: string | undefined): string {
  try {
    return new URL(value ?? "/", "http://ghostprov.local").pathname;
  } catch {
    return (value ?? "/").split("?", 1)[0];
  }
}

export function isAnthropicRequestPath(path: string): boolean {
  return path === "/v1/messages" || path === "/v1/messages/count_tokens";
}

export function requestBodyLimitForUrl(value: string | undefined, sharedLimit: number): number {
  return requestBodyLimitForPath(requestPath(value), sharedLimit);
}

/** The caller supplies the exact path used by its HTTP router. */
export function requestBodyLimitForPath(path: string, sharedLimit: number): number {
  if (path === "/v1/alpha/search") {
    return Math.min(sharedLimit, STANDALONE_SEARCH_MAX_REQUEST_BYTES);
  }
  return isAnthropicRequestPath(path)
    ? Math.min(sharedLimit, ANTHROPIC_HTTP_MAX_BODY_BYTES)
    : sharedLimit;
}

/** Only implemented body consumers may reserve shared inference memory. */
export function acceptsRequestBody(method: string | undefined, path: string): boolean {
  if (method !== "POST") return false;
  return path === "/v1/responses" || path === "/v1/alpha/search" ||
    path === "/v1/images/generations" || path === "/v1/images/edits" ||
    isAnthropicRequestPath(path);
}
