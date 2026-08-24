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
  return isAnthropicRequestPath(requestPath(value))
    ? Math.min(sharedLimit, ANTHROPIC_HTTP_MAX_BODY_BYTES)
    : sharedLimit;
}
