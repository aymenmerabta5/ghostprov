import type { IncomingResourceLease } from "../router/router";

export interface HttpRequestBindings {
  incomingBodyLease?: IncomingResourceLease;
}

/**
 * Removes the request-local ingress lease before returning it. Hono creates a
 * fresh bindings object for each Node request, so this is an atomic one-shot
 * handoff even when an image batch starts several relay promises together.
 */
export function takeIncomingBodyLease(env: unknown): IncomingResourceLease | undefined {
  if (!env || typeof env !== "object" || Array.isArray(env)) return undefined;
  const bindings = env as HttpRequestBindings;
  const lease = bindings.incomingBodyLease;
  if (!lease) return undefined;
  bindings.incomingBodyLease = undefined;
  return lease;
}
