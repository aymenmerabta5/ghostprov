import { createHmac, randomBytes } from "node:crypto";
import {
  authenticatedTenantScopeHmac,
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";

const DOMAIN = "ghostprov/standalone-search-session/v1\0";
const DEFAULT_MAX_RECORDS = 4_096;
const DEFAULT_MAX_BYTES = 4 * 1024 * 1024;
const DEFAULT_MAX_ACTIVE_KEYS = 4_096;
const DEFAULT_TTL_MS = 60 * 60 * 1_000;
const MAX_PROVIDER_RESPONSE_ID_BYTES = 512;

interface Entry {
  responseId: string;
  bytes: number;
  expiresAtMs: number;
}

export interface StandaloneSearchSessionIndexOptions {
  readonly maxRecords?: number;
  readonly maxBytes?: number;
  readonly maxActiveKeys?: number;
  readonly ttlMs?: number;
  readonly now?: () => number;
  readonly hmacKey?: Uint8Array;
}

export interface StandaloneSearchSessionOutcome<T> {
  readonly responseId: string;
  readonly value: T;
}

/**
 * Content-free, tenant-scoped search lineage. Per-key serialization preserves
 * ref-id ordering without retaining queries, results, URLs, or raw session IDs.
 */
export class StandaloneSearchSessionIndex {
  readonly #entries = new Map<string, Entry>();
  readonly #tails = new Map<string, Promise<void>>();
  readonly #key: Uint8Array;
  readonly #maxRecords: number;
  readonly #maxBytes: number;
  readonly #maxActiveKeys: number;
  readonly #ttlMs: number;
  readonly #now: () => number;
  #bytes = 0;

  constructor(options: StandaloneSearchSessionIndexOptions = {}) {
    this.#maxRecords = positive(options.maxRecords ?? DEFAULT_MAX_RECORDS, "maxRecords");
    this.#maxBytes = positive(options.maxBytes ?? DEFAULT_MAX_BYTES, "maxBytes");
    this.#maxActiveKeys = positive(
      options.maxActiveKeys ?? DEFAULT_MAX_ACTIVE_KEYS,
      "maxActiveKeys",
    );
    this.#ttlMs = positive(options.ttlMs ?? DEFAULT_TTL_MS, "ttlMs");
    this.#now = options.now ?? Date.now;
    this.#key = Uint8Array.from(options.hmacKey ?? randomBytes(32));
    if (this.#key.byteLength < 32 || this.#key.byteLength > 64) {
      throw new RangeError("hmacKey must contain 32 through 64 bytes");
    }
  }

  async run<T>(
    scope: AuthenticatedTenantScope,
    sessionId: string,
    operation: (previousResponseId: string | null) => Promise<StandaloneSearchSessionOutcome<T>>,
  ): Promise<T> {
    if (!isAuthenticatedTenantScope(scope)) {
      throw new TypeError("Standalone search requires an authenticated tenant scope");
    }
    if (typeof sessionId !== "string" || sessionId.length < 1 || sessionId.length > 128) {
      throw new TypeError("Standalone search session ID is invalid");
    }
    const key = this.#digest(scope, sessionId);
    if (!this.#tails.has(key) && this.#tails.size >= this.#maxActiveKeys) {
      // Metadata pressure must not become a global execution quota. The call
      // remains safe but deliberately loses cross-call ref continuity.
      return (await operation(null)).value;
    }

    const previous = this.#tails.get(key) ?? Promise.resolve();
    const execution = previous.catch(() => undefined).then(async () => {
      const now = this.#time();
      this.#pruneExpired(now);
      const entry = this.#entries.get(key);
      const previousResponseId = entry && entry.expiresAtMs > now
        ? entry.responseId
        : null;
      const outcome = await operation(previousResponseId);
      this.#commit(key, outcome.responseId, this.#time());
      return outcome.value;
    });
    const tail = execution.then(() => undefined, () => undefined);
    this.#tails.set(key, tail);
    try {
      return await execution;
    } finally {
      if (this.#tails.get(key) === tail) this.#tails.delete(key);
    }
  }

  clear(): void {
    this.#entries.clear();
    this.#bytes = 0;
  }

  snapshot(): Readonly<{
    records: number;
    bytes: number;
    activeKeys: number;
    maxRecords: number;
    maxBytes: number;
    ttlMs: number;
  }> {
    this.#pruneExpired(this.#time());
    return Object.freeze({
      records: this.#entries.size,
      bytes: this.#bytes,
      activeKeys: this.#tails.size,
      maxRecords: this.#maxRecords,
      maxBytes: this.#maxBytes,
      ttlMs: this.#ttlMs,
    });
  }

  #digest(scope: AuthenticatedTenantScope, sessionId: string): string {
    const scopeDigest = authenticatedTenantScopeHmac(scope, this.#key, DOMAIN);
    return createHmac("sha256", this.#key)
      .update(scopeDigest, "utf8")
      .update("\0", "utf8")
      .update(sessionId, "utf8")
      .digest("base64url");
  }

  #commit(key: string, responseId: string, now: number): void {
    if (
      typeof responseId !== "string" || responseId.length < 1 ||
      Buffer.byteLength(responseId, "utf8") > MAX_PROVIDER_RESPONSE_ID_BYTES
    ) return;
    const bytes = Buffer.byteLength(key, "utf8") + Buffer.byteLength(responseId, "utf8") + 96;
    if (bytes > this.#maxBytes) return;
    const existing = this.#entries.get(key);
    if (existing) {
      this.#entries.delete(key);
      this.#bytes -= existing.bytes;
    }
    while (
      this.#entries.size >= this.#maxRecords ||
      this.#bytes + bytes > this.#maxBytes
    ) {
      const oldest = this.#entries.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.#delete(oldest);
    }
    const entry: Entry = {
      responseId,
      bytes,
      expiresAtMs: now + this.#ttlMs,
    };
    this.#entries.set(key, entry);
    this.#bytes += bytes;
  }

  #pruneExpired(now: number): void {
    for (const [key, entry] of this.#entries) {
      if (entry.expiresAtMs > now) continue;
      this.#delete(key);
    }
  }

  #delete(key: string): void {
    const entry = this.#entries.get(key);
    if (!entry) return;
    this.#entries.delete(key);
    this.#bytes = Math.max(0, this.#bytes - entry.bytes);
  }

  #time(): number {
    const value = this.#now();
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new RangeError("Standalone search session clock is invalid");
    }
    return value;
  }
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}
