import { createHmac, randomBytes } from "node:crypto";
import {
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";

const DEFAULT_MAX_ENTRIES = 1_024;
const DEFAULT_TTL_MS = 5 * 60_000;
const MAX_CONTEXT_UNITS = 128;

/**
 * A downstream transport hint, never request/continuation authority. Only keyed
 * digests and expiry times survive the rejected connection; no task IDs, bodies,
 * provider IDs, credentials, or headers are retained.
 */
export class WebSocketHttpFallbackIndex {
  readonly #scope: AuthenticatedTenantScope;
  readonly #secret = randomBytes(32);
  readonly #entries = new Map<string, number>();
  readonly #now: () => number;
  readonly #maxEntries: number;
  readonly #ttlMs: number;
  #closed = false;

  constructor(
    scope: AuthenticatedTenantScope,
    options: { now?: () => number; maxEntries?: number; ttlMs?: number } = {},
  ) {
    if (!isAuthenticatedTenantScope(scope)) {
      throw new TypeError("WebSocket fallback requires an AuthenticatedTenantScope");
    }
    this.#scope = scope;
    this.#now = options.now ?? (() => performance.now());
    this.#maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
    this.#ttlMs = options.ttlMs ?? DEFAULT_TTL_MS;
    for (const [name, value] of [
      ["maxEntries", this.#maxEntries],
      ["ttlMs", this.#ttlMs],
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 1) {
        throw new RangeError(`${name} must be a positive safe integer`);
      }
    }
  }

  key(scope: AuthenticatedTenantScope, context: unknown): string | null {
    this.#assertScope(scope);
    if (
      this.#closed || typeof context !== "string" ||
      context.length === 0 || context.length > MAX_CONTEXT_UNITS ||
      /[^a-zA-Z0-9._:-]/.test(context)
    ) return null;
    return createHmac("sha256", this.#secret).update(context).digest("hex");
  }

  remember(scope: AuthenticatedTenantScope, key: string | null): boolean {
    this.#assertScope(scope);
    if (this.#closed || key === null) return false;
    this.#assertKey(key);
    const now = this.#now();
    this.#prune(now);
    this.#entries.delete(key);
    while (this.#entries.size >= this.#maxEntries) {
      this.#entries.delete(this.#entries.keys().next().value!);
    }
    this.#entries.set(key, now + this.#ttlMs);
    return true;
  }

  requiresHttp(scope: AuthenticatedTenantScope, key: string | null): boolean {
    this.#assertScope(scope);
    if (this.#closed || key === null) return false;
    this.#assertKey(key);
    this.#prune(this.#now());
    return this.#entries.has(key);
  }

  snapshot(): { entries: number; maxEntries: number; closed: boolean } {
    this.#prune(this.#now());
    return { entries: this.#entries.size, maxEntries: this.#maxEntries, closed: this.#closed };
  }

  close(): void {
    this.#closed = true;
    this.#entries.clear();
    this.#secret.fill(0);
  }

  #assertScope(scope: AuthenticatedTenantScope): void {
    if (scope !== this.#scope) throw new TypeError("WebSocket fallback scope does not match");
  }

  #assertKey(key: string): void {
    if (key.length !== 64 || /[^a-f0-9]/.test(key)) {
      throw new TypeError("WebSocket fallback key must be a keyed digest");
    }
  }

  #prune(now: number): void {
    for (const [key, expiresAt] of this.#entries) {
      if (expiresAt > now) break;
      this.#entries.delete(key);
    }
  }
}
