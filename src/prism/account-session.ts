import { createHash } from "node:crypto";
import { mkdir, open, rename, rm, stat } from "node:fs/promises";
import * as path from "node:path";
import { Cookie, CookieJar } from "tough-cookie";
import { FileLease } from "../core/file-lease";
import type { FetchLike } from "../core/ports";

const MAX_COOKIE_FILE_BYTES = 8 * 1024 * 1024;
const COOKIE_FILE_READ_CHUNK_BYTES = 64 * 1024;
const COOKIE_FILE_STABILITY_ATTEMPTS = 3;
const COOKIE_FILE_WRITE_LOCK_TIMEOUT_MS = 30_000;
const COOKIE_FILE_WRITE_LOCK_STALE_MS = 120_000;
const MAX_COOKIE_MUTATION_IDENTITIES = 1_024;
const MAX_COOKIE_MUTATION_ACTIVE_REQUESTS = 1_024;

export interface AccountSessionOptions {
  origin: string;
  initialCookieHeader?: string;
  cookieFile?: string;
  fetch?: FetchLike;
}

export interface AccountSessionSnapshot {
  origin: string;
  cookieFile?: string;
  cookieCount: number;
  loadedAtMs: number;
  persistedAtMs: number | null;
  lastResponseAtMs: number | null;
}

export interface AccountSessionReloadOptions {
  /** Reload the cookie file even when its modification time has not advanced. */
  force?: boolean;
}

export interface AccountSessionCookieCandidate {
  request(pathOrUrl: string, init?: RequestInit): Promise<Response>;
  /**
   * Revalidates the exact source-file content under the cooperative writer
   * lease before install. Returns false on a live-jar or file conflict.
   */
  commit(signal?: AbortSignal): Promise<boolean>;
}

export class AccountAuthError extends Error {
  constructor(readonly status: number, readonly pathname: string) {
    super(`Prism account authentication failed (${status}) for ${pathname}`);
    this.name = "AccountAuthError";
  }
}

export class AccountTransportError extends Error {
  constructor(readonly method: string, readonly pathname: string) {
    super(`Prism account transport failed for ${method} ${pathname}`);
    this.name = "AccountTransportError";
  }
}

function normalizeOrigin(value: string): string {
  const url = new URL(value);
  return url.origin;
}

async function applyCookieHeader(jar: CookieJar, header: string, url: string): Promise<void> {
  for (const pair of header.split(/;\s*/)) {
    if (!pair || !pair.includes("=")) continue;
    await jar.setCookie(`${pair}; Path=/`, url);
  }
}

interface CookieFileIdentity {
  readonly byteLength: number;
  readonly sha256: string;
}

interface CookieFileSnapshot {
  readonly jar: CookieJar;
  readonly identity: CookieFileIdentity;
  readonly mtimeMs: number;
}

class CookieFileReadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CookieFileReadError";
  }
}

class CookieFileConflictError extends Error {
  constructor() {
    super("Prism cookie file changed during persistence");
    this.name = "CookieFileConflictError";
  }
}

function cookieFileIdentity(bytes: Uint8Array): CookieFileIdentity {
  return Object.freeze({
    byteLength: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex"),
  });
}

function sameCookieFileIdentity(
  left: CookieFileIdentity | null,
  right: CookieFileIdentity | null,
): boolean {
  return left === null || right === null
    ? left === right
    : left.byteLength === right.byteLength && left.sha256 === right.sha256;
}

async function readBoundedCookieFile(
  handle: Awaited<ReturnType<typeof open>>,
): Promise<Buffer> {
  const initial = await handle.stat();
  if (initial.size > MAX_COOKIE_FILE_BYTES) {
    throw new CookieFileReadError("Prism cookie file exceeds the bounded read limit");
  }
  const chunks: Buffer[] = [];
  let total = 0;
  for (;;) {
    const remaining = MAX_COOKIE_FILE_BYTES - total;
    if (remaining === 0) {
      const probe = Buffer.allocUnsafe(1);
      const { bytesRead } = await handle.read(probe, 0, 1, total);
      if (bytesRead > 0) {
        throw new CookieFileReadError("Prism cookie file exceeds the bounded read limit");
      }
      break;
    }
    const buffer = Buffer.allocUnsafe(Math.min(COOKIE_FILE_READ_CHUNK_BYTES, remaining));
    const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, total);
    if (bytesRead === 0) break;
    chunks.push(Buffer.from(buffer.subarray(0, bytesRead)));
    total += bytesRead;
  }
  return Buffer.concat(chunks, total);
}

async function jarFromBytes(bytes: Uint8Array, origin: string): Promise<CookieJar> {
  const raw = Buffer.from(bytes).toString("utf8");
  try {
    return CookieJar.deserializeSync(JSON.parse(raw));
  } catch {
    const jar = new CookieJar();
    await applyCookieHeader(jar, raw.trim(), `${origin}/`);
    return jar;
  }
}

/**
 * Reads a small cookie file twice from one open handle. Requiring identical
 * bytes prevents a candidate from being based on a torn in-place rewrite.
 * An atomic replacement yields one complete descriptor generation; candidate
 * commit reopens the path under the cooperative writer lease before install.
 */
async function cookieFileSnapshot(
  file: string,
  origin: string,
): Promise<CookieFileSnapshot | null> {
  for (let attempt = 1; attempt <= COOKIE_FILE_STABILITY_ATTEMPTS; attempt++) {
    let handle: Awaited<ReturnType<typeof open>>;
    try {
      handle = await open(file, "r");
    } catch (error: any) {
      if (error?.code === "ENOENT") return null;
      throw error;
    }
    try {
      const first = await readBoundedCookieFile(handle);
      const firstMetadata = await handle.stat();
      const second = await readBoundedCookieFile(handle);
      const secondMetadata = await handle.stat();
      if (
        first.equals(second) &&
        first.byteLength === firstMetadata.size &&
        second.byteLength === secondMetadata.size
      ) {
        return Object.freeze({
          jar: await jarFromBytes(second, origin),
          identity: cookieFileIdentity(second),
          mtimeMs: secondMetadata.mtimeMs,
        });
      }
    } finally {
      await handle.close();
    }
  }
  throw new CookieFileReadError("Prism cookie file changed while it was being read");
}

function responseCookies(headers: Headers): string[] {
  const extended = headers as Headers & { getSetCookie?: () => string[] };
  const values = extended.getSetCookie?.();
  if (values?.length) return values;
  const single = headers.get("set-cookie");
  return single ? [single] : [];
}

function cloneJar(jar: CookieJar): CookieJar {
  const serialized = jar.serializeSync();
  if (!serialized) throw new Error("Cookie jar serialization failed");
  return CookieJar.deserializeSync(serialized);
}

interface ResolvedCookieMutation {
  readonly header: string;
  readonly identity: string;
}

function cookieStorageIdentity(cookie: Cookie): string | null {
  const domain = cookie.cdomain();
  if (!domain || !cookie.path) return null;
  // RFC cookie replacement identity is name + canonical domain + effective
  // path. hostOnly, Secure, SameSite, expiry, and value are not identity.
  return JSON.stringify([cookie.key, domain, cookie.path]);
}

async function resolveCookieMutations(
  jar: CookieJar,
  headers: readonly string[],
  url: URL,
): Promise<readonly ResolvedCookieMutation[]> {
  const scratch = cloneJar(jar);
  const resolved: ResolvedCookieMutation[] = [];
  for (const header of headers) {
    const cookie = await scratch.setCookie(header, url.href);
    if (!cookie) continue;
    const identity = cookieStorageIdentity(cookie);
    if (identity) resolved.push(Object.freeze({ header, identity }));
  }
  return Object.freeze(resolved);
}

class CookieMutationSequencer {
  #next = 0;
  #barrier = 0;
  readonly #activeRequests = new Set<number>();
  readonly #latestByIdentity = new Map<string, number>();

  startRequest(): number {
    if (this.#activeRequests.size >= MAX_COOKIE_MUTATION_ACTIVE_REQUESTS) {
      throw new Error("Cookie mutation request capacity is exhausted");
    }
    const sequence = ++this.#next;
    this.#activeRequests.add(sequence);
    return sequence;
  }

  nextMutation(): number {
    return ++this.#next;
  }

  isStale(identity: string, sequence: number): boolean {
    return sequence < this.#barrier ||
      (this.#latestByIdentity.get(identity) ?? 0) > sequence;
  }

  record(identities: Iterable<string>, sequence: number): void {
    const unique = new Set(identities);
    let additions = 0;
    for (const identity of unique) {
      if (!this.#latestByIdentity.has(identity)) additions += 1;
    }
    if (this.#latestByIdentity.size + additions > MAX_COOKIE_MUTATION_IDENTITIES) {
      // Conservatively fence every older request rather than retaining an
      // unbounded identity history behind one hung request.
      this.#barrier = Math.max(this.#barrier, this.#next);
      this.#latestByIdentity.clear();
      this.#prune();
      return;
    }
    for (const identity of unique) {
      const current = this.#latestByIdentity.get(identity) ?? 0;
      if (sequence >= current) this.#latestByIdentity.set(identity, sequence);
    }
    this.#prune();
  }

  finishRequest(sequence: number): void {
    this.#activeRequests.delete(sequence);
    this.#prune();
  }

  #prune(): void {
    const oldestActive = this.#activeRequests.values().next().value as number | undefined;
    if (oldestActive === undefined) {
      this.#latestByIdentity.clear();
      this.#barrier = 0;
      return;
    }
    if (oldestActive >= this.#barrier) this.#barrier = 0;
    for (const [identity, sequence] of this.#latestByIdentity) {
      if (sequence <= oldestActive) this.#latestByIdentity.delete(identity);
    }
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("Cookie candidate commit aborted", "AbortError");
  }
}

export class PrismAccountSession {
  readonly #origin: string;
  readonly #cookieFile?: string;
  readonly #cookieWriteLease?: FileLease;
  readonly #fetch: FetchLike;
  #jar: CookieJar;
  #fileMtimeMs = 0;
  #fileIdentity: CookieFileIdentity | null = null;
  #loadedAtMs = Date.now();
  #persistedAtMs: number | null = null;
  #lastResponseAtMs: number | null = null;
  #persistChain: Promise<void> = Promise.resolve();
  #mutationChain: Promise<void> = Promise.resolve();
  #jarVersion = 0;
  #jarEpoch = 0;
  #cookieMutations = new CookieMutationSequencer();
  #activeCookieRequests = 0;

  private constructor(options: AccountSessionOptions, jar: CookieJar) {
    this.#origin = normalizeOrigin(options.origin);
    this.#cookieFile = options.cookieFile ? path.resolve(options.cookieFile) : undefined;
    this.#cookieWriteLease = this.#cookieFile
      ? new FileLease({
          file: `${this.#cookieFile}.write.lock`,
          staleAfterMs: COOKIE_FILE_WRITE_LOCK_STALE_MS,
        })
      : undefined;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#jar = jar;
  }

  static async create(options: AccountSessionOptions): Promise<PrismAccountSession> {
    const origin = normalizeOrigin(options.origin);
    const cookieFile = options.cookieFile ? path.resolve(options.cookieFile) : undefined;
    const fileSnapshot = cookieFile ? await cookieFileSnapshot(cookieFile, origin) : null;
    const jar = fileSnapshot?.jar ?? new CookieJar();
    if (!fileSnapshot && options.initialCookieHeader) {
      await applyCookieHeader(jar, options.initialCookieHeader, `${origin}/`);
    }
    const session = new PrismAccountSession({ ...options, origin, cookieFile }, jar);
    if (fileSnapshot) {
      session.#fileMtimeMs = fileSnapshot.mtimeMs;
      session.#fileIdentity = fileSnapshot.identity;
    }
    return session;
  }

  async cookieHeader(url: string): Promise<string> {
    return this.#withJarLock(() => this.#jar.getCookieString(url));
  }

  async setCookie(cookie: string, url = `${this.#origin}/`): Promise<void> {
    await this.#withJarLock(async () => {
      const previous = cloneJar(this.#jar);
      const sequence = this.#cookieMutations.nextMutation();
      try {
        const stored = await this.#jar.setCookie(cookie, url);
        if (!await this.#queuePersist(this.#jar)) throw new CookieFileConflictError();
        this.#jarVersion += 1;
        const identity = stored ? cookieStorageIdentity(stored) : null;
        if (identity) this.#cookieMutations.record([identity], sequence);
      } catch (error) {
        this.#jar = previous;
        this.#jarVersion += 1;
        this.#advanceJarEpoch();
        throw error;
      }
    });
  }

  async reload(options: AccountSessionReloadOptions = {}): Promise<boolean> {
    if (!this.#cookieFile) return false;
    const baseline = await this.#withJarLock(async () => ({
      version: this.#jarVersion,
      fileMtimeMs: this.#fileMtimeMs,
    }));
    let metadata;
    try { metadata = await stat(this.#cookieFile); } catch { return false; }
    if (!options.force && metadata.mtimeMs <= baseline.fileMtimeMs) return false;
    const replacement = await cookieFileSnapshot(this.#cookieFile, this.#origin);
    if (!replacement) return false;
    return this.#withJarLock(async () => {
      if (this.#jarVersion !== baseline.version) return false;
      if (!options.force && metadata.mtimeMs <= this.#fileMtimeMs) return false;
      this.#jar = replacement.jar;
      this.#jarVersion += 1;
      this.#advanceJarEpoch();
      this.#fileMtimeMs = replacement.mtimeMs;
      this.#fileIdentity = replacement.identity;
      this.#loadedAtMs = Date.now();
      return true;
    });
  }

  async reloadIfChanged(): Promise<void> {
    await this.reload();
  }

  async createCookieCandidate(): Promise<AccountSessionCookieCandidate> {
    const source = await this.#withJarLock(async () => {
      await this.#persistChain;
      const fallback = cloneJar(this.#jar);
      let jar = fallback;
      let fileMtimeMs: number | null = null;
      let fileIdentity: CookieFileIdentity | null = null;
      if (this.#cookieFile) {
        const fromFile = await cookieFileSnapshot(this.#cookieFile, this.#origin);
        if (fromFile) {
          jar = fromFile.jar;
          fileMtimeMs = fromFile.mtimeMs;
          fileIdentity = fromFile.identity;
        } else {
          fileMtimeMs = null;
        }
      }
      return { jar, fileMtimeMs, fileIdentity, baseVersion: this.#jarVersion };
    });

    let finalized = false;
    let activeRequests = 0;
    let changed = false;
    let lastResponseAtMs: number | null = null;
    const mutations = new CookieMutationSequencer();
    let mutationChain: Promise<void> = Promise.resolve();
    const withMutationLock = <T>(operation: () => Promise<T>): Promise<T> => {
      const previous = mutationChain;
      let release!: () => void;
      mutationChain = new Promise<void>(resolve => { release = resolve; });
      return previous.then(operation).finally(release);
    };
    return {
      request: async (pathOrUrl, init = {}) => {
        if (finalized) throw new Error("Cookie candidate is already finalized");
        const sequence = mutations.startRequest();
        activeRequests += 1;
        let sessionCapacityReserved = false;
        try {
          await this.#withJarLock(async () => {
            if (this.#activeCookieRequests >= MAX_COOKIE_MUTATION_ACTIVE_REQUESTS) {
              throw new Error("Cookie mutation request capacity is exhausted");
            }
            this.#activeCookieRequests += 1;
            sessionCapacityReserved = true;
          });
          const { response, url } = await this.#fetchWithJar(source.jar, pathOrUrl, init);
          const cookies = responseCookies(response.headers);
          if (cookies.length > 0) {
            await withMutationLock(async () => {
              const resolved = await resolveCookieMutations(source.jar, cookies, url);
              const accepted = resolved.filter(mutation => (
                !mutations.isStale(mutation.identity, sequence)
              ));
              if (accepted.length === 0) return;
              for (const mutation of accepted) {
                await source.jar.setCookie(mutation.header, url.href);
              }
              mutations.record(accepted.map(mutation => mutation.identity), sequence);
              changed = true;
            });
          }
          lastResponseAtMs = Date.now();
          this.#throwAccountAuth(response, url);
          return response;
        } finally {
          mutations.finishRequest(sequence);
          if (sessionCapacityReserved) {
            await this.#withJarLock(async () => { this.#activeCookieRequests -= 1; });
          }
          activeRequests -= 1;
        }
      },
      commit: async signal => {
        if (finalized) throw new Error("Cookie candidate is already finalized");
        if (activeRequests !== 0) throw new Error("Cookie candidate has active requests");
        finalized = true;
        return this.#commitCookieCandidate({
          ...source,
          changed,
          lastResponseAtMs,
        }, signal);
      },
    };
  }

  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    await this.reloadIfChanged();
    const requestState = await this.#withJarLock(async () => {
      if (this.#activeCookieRequests >= MAX_COOKIE_MUTATION_ACTIVE_REQUESTS) {
        throw new Error("Cookie mutation request capacity is exhausted");
      }
      const mutations = this.#cookieMutations;
      const sequence = mutations.startRequest();
      this.#activeCookieRequests += 1;
      return {
        jar: this.#jar,
        epoch: this.#jarEpoch,
        mutations,
        sequence,
      };
    });
    try {
      const { response, url } = await this.#fetchWithJar(requestState.jar, pathOrUrl, init);
      const cookies = responseCookies(response.headers);
      if (cookies.length > 0) {
        await this.#withJarLock(async () => {
          // Never apply a response authenticated with a jar generation that has
          // since been replaced. Same-generation independent cookie identities
          // may still merge even when their responses complete out of order.
          if (
            this.#jar !== requestState.jar ||
            this.#jarEpoch !== requestState.epoch ||
            this.#cookieMutations !== requestState.mutations
          ) return;
          const resolved = await resolveCookieMutations(this.#jar, cookies, url);
          const accepted = resolved.filter(mutation => (
            !requestState.mutations.isStale(mutation.identity, requestState.sequence)
          ));
          if (accepted.length === 0) return;
          const previous = cloneJar(this.#jar);
          try {
            for (const mutation of accepted) {
              await this.#jar.setCookie(mutation.header, url.href);
            }
            if (!await this.#queuePersist(this.#jar)) {
              // The response was authenticated with a stale file generation. Do
              // not let its rotation erase a newer externally persisted jar.
              this.#jar = previous;
              this.#jarVersion += 1;
              this.#advanceJarEpoch();
              return;
            }
            this.#jarVersion += 1;
            requestState.mutations.record(
              accepted.map(mutation => mutation.identity),
              requestState.sequence,
            );
          } catch (error) {
            this.#jar = previous;
            this.#jarVersion += 1;
            this.#advanceJarEpoch();
            throw error;
          }
        });
      }
      this.#lastResponseAtMs = Date.now();
      this.#throwAccountAuth(response, url);
      return response;
    } finally {
      await this.#withJarLock(async () => {
        requestState.mutations.finishRequest(requestState.sequence);
        this.#activeCookieRequests -= 1;
      });
    }
  }

  snapshot(): AccountSessionSnapshot {
    const serialized = this.#jar.serializeSync();
    return {
      origin: this.#origin,
      cookieFile: this.#cookieFile,
      cookieCount: serialized?.cookies?.length ?? 0,
      loadedAtMs: this.#loadedAtMs,
      persistedAtMs: this.#persistedAtMs,
      lastResponseAtMs: this.#lastResponseAtMs,
    };
  }

  async flush(): Promise<void> {
    await this.#withJarLock(async () => {
      if (this.#cookieFile && !await this.#queuePersist(this.#jar)) {
        throw new CookieFileConflictError();
      }
    });
    await this.#persistChain;
  }

  async #fetchWithJar(
    jar: CookieJar,
    pathOrUrl: string,
    init: RequestInit,
  ): Promise<{ response: Response; url: URL }> {
    const url = new URL(pathOrUrl, `${this.#origin}/`);
    const headers = new Headers(init.headers);
    const cookie = await jar.getCookieString(url.href);
    if (cookie) headers.set("cookie", cookie);
    const method = (init.method ?? "GET").toUpperCase();
    try {
      return {
        response: await this.#fetch(url, { ...init, method, headers }),
        url,
      };
    } catch {
      throw new AccountTransportError(method, url.pathname);
    }
  }

  #throwAccountAuth(response: Response, url: URL): void {
    if ((response.status === 401 || response.status === 403) && url.pathname.startsWith("/api/")) {
      throw new AccountAuthError(response.status, url.pathname);
    }
  }

  async #commitCookieCandidate(
    candidate: {
      jar: CookieJar;
      fileMtimeMs: number | null;
      fileIdentity: CookieFileIdentity | null;
      baseVersion: number;
      changed: boolean;
      lastResponseAtMs: number | null;
    },
    signal?: AbortSignal,
  ): Promise<boolean> {
    return this.#withJarLock(async () => {
      throwIfAborted(signal);
      if (this.#jarVersion !== candidate.baseVersion) return false;
      const commit = async (): Promise<boolean> => {
        if (this.#cookieFile) {
          await this.#persistChain;
          const current = await cookieFileSnapshot(this.#cookieFile, this.#origin);
          if (!sameCookieFileIdentity(current?.identity ?? null, candidate.fileIdentity)) {
            return false;
          }
        }
        if (
          candidate.changed &&
          !await this.#queuePersist(candidate.jar, candidate.fileIdentity, true)
        ) return false;
        // Once persistence starts, finish the in-memory half of the commit even if the
        // caller aborts. This keeps the disk and live jars transactionally consistent.
        this.#jar = candidate.jar;
        this.#jarVersion += 1;
        this.#advanceJarEpoch();
        if (!candidate.changed) {
          this.#fileMtimeMs = candidate.fileMtimeMs ?? 0;
          this.#fileIdentity = candidate.fileIdentity;
        }
        this.#loadedAtMs = Date.now();
        if (candidate.lastResponseAtMs != null) {
          this.#lastResponseAtMs = candidate.lastResponseAtMs;
        }
        return true;
      };
      return this.#cookieWriteLease
        ? this.#withCookieWriteLease(commit, signal)
        : commit();
    });
  }

  #advanceJarEpoch(): void {
    this.#jarEpoch += 1;
    // Old requests retain their former sequencer and are fenced by jar/epoch.
    // Replacing the object keeps cleanup from an old request from touching the
    // new generation even if sequence numbers are reused.
    this.#cookieMutations = new CookieMutationSequencer();
  }

  #withJarLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#mutationChain;
    let release!: () => void;
    this.#mutationChain = new Promise<void>(resolve => { release = resolve; });
    return previous.then(operation).finally(release);
  }

  async #withCookieWriteLease<T>(
    operation: () => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    const lease = this.#cookieWriteLease;
    if (!lease) return operation();
    await lease.acquire({ timeoutMs: COOKIE_FILE_WRITE_LOCK_TIMEOUT_MS, signal });
    try {
      return await operation();
    } finally {
      // Releasing the cooperative lock is housekeeping after the guarded
      // operation's rename/install boundary. Never turn a completed disk/live
      // transaction into an apparent failure that callers would roll back.
      await lease.release().catch(() => undefined);
    }
  }

  async #queuePersist(
    jar: CookieJar,
    expectedFileIdentity?: CookieFileIdentity | null,
    writeLeaseHeld = false,
  ): Promise<boolean> {
    if (!this.#cookieFile) return true;
    const serialized = jar.serializeSync();
    if (!serialized) throw new Error("Cookie jar serialization failed");
    const body = JSON.stringify(serialized, null, 2);
    const bodyBytes = Buffer.from(body, "utf8");
    if (bodyBytes.byteLength > MAX_COOKIE_FILE_BYTES) {
      throw new CookieFileReadError("Prism cookie file exceeds the bounded write limit");
    }
    const bodyIdentity = cookieFileIdentity(bodyBytes);
    const guardedFileIdentity = expectedFileIdentity === undefined
      ? this.#fileIdentity
      : expectedFileIdentity;
    let persisted = false;
    const persist = async (): Promise<void> => {
      const file = this.#cookieFile!;
      const directory = path.dirname(file);
      await mkdir(directory, { recursive: true });
      const temporary = path.join(
        directory,
        `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.tmp`,
      );
      const handle = await open(temporary, "wx", 0o600);
      let renamed = false;
      try {
        await handle.writeFile(body);
        await handle.sync();
        const metadata = await handle.stat();
        await handle.close();
        const current = await cookieFileSnapshot(file, this.#origin);
        if (!sameCookieFileIdentity(current?.identity ?? null, guardedFileIdentity)) return;
        // Rename remains the final fallible transaction operation. The temp file
        // is already synced and mode 0600; directory sync below is best effort.
        await rename(temporary, file);
        renamed = true;
        persisted = true;
        this.#fileMtimeMs = metadata.mtimeMs;
        this.#fileIdentity = bodyIdentity;
        this.#persistedAtMs = Date.now();
        if (process.platform !== "win32") {
          const directoryHandle = await open(directory, "r").catch(() => null);
          if (directoryHandle) {
            try { await directoryHandle.sync(); } catch {}
            finally { await directoryHandle.close().catch(() => undefined); }
          }
        }
      } finally {
        await handle.close().catch(() => undefined);
        if (!renamed) await rm(temporary, { force: true }).catch(() => undefined);
      }
    };
    this.#persistChain = this.#persistChain
      .catch(() => undefined)
      .then(() => writeLeaseHeld ? persist() : this.#withCookieWriteLease(persist));
    await this.#persistChain;
    return persisted;
  }
}
