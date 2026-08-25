import { createHash, randomUUID } from "node:crypto";
import type { IncomingResourceLease } from "../router/router";

export interface HttpRequestBindings {
  incomingBodyLease?: IncomingResourceLease;
  httpConnectionClientId?: string;
}

const EXPLICIT_CLIENT_ID = /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/;
const CONNECTION_CLIENT_ID = /^http-connection-(?:[0-9]|[1-9][0-9]{0,3})$/;
const IDENTITY_BUCKETS = 1_024;

function identityBucket(value: string): string {
  const digest = createHash("sha256").update(value, "utf8").digest();
  return String(digest.readUInt32BE(0) % IDENTITY_BUCKETS).padStart(4, "0");
}

/**
 * Produces a privacy-safe, cardinality-bounded HTTP fairness identity.
 * Explicit caller IDs are accepted only in a small safe grammar and are never
 * forwarded verbatim. Invalid/oversized values fall back to the server-owned
 * connection identity (or a bounded request bucket outside the Node server).
 */
export function resolveHttpClientId(env: unknown, explicitValue?: string): string {
  const explicit = explicitValue?.trim();
  if (explicit && EXPLICIT_CLIENT_ID.test(explicit)) {
    return `http-explicit-${identityBucket(explicit)}`;
  }
  if (env && typeof env === "object" && !Array.isArray(env)) {
    const connection = (env as HttpRequestBindings).httpConnectionClientId;
    if (typeof connection === "string" && CONNECTION_CLIENT_ID.test(connection)) {
      return connection;
    }
  }
  return `http-request-${identityBucket(randomUUID())}`;
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
