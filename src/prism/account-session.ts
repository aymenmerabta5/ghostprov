import { chmod, mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import { CookieJar } from "tough-cookie";
import type { FetchLike } from "../core/ports";

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
   * Atomically installs this candidate if the live cookie jar has not changed since the
   * candidate was created. Returns false on a concurrent live-jar mutation.
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

async function jarFromFile(file: string): Promise<CookieJar | null> {
  try {
    const raw = await readFile(file, "utf8");
    try {
      return CookieJar.deserializeSync(JSON.parse(raw));
    } catch {
      const jar = new CookieJar();
      await applyCookieHeader(jar, raw.trim(), "https://prism.openai.com/");
      return jar;
    }
  } catch (error: any) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

async function applyCookieHeader(jar: CookieJar, header: string, url: string): Promise<void> {
  for (const pair of header.split(/;\s*/)) {
    if (!pair || !pair.includes("=")) continue;
    await jar.setCookie(`${pair}; Path=/`, url);
  }
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

function throwIfAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("Cookie candidate commit aborted", "AbortError");
  }
}

export class PrismAccountSession {
  readonly #origin: string;
  readonly #cookieFile?: string;
  readonly #fetch: FetchLike;
  #jar: CookieJar;
  #fileMtimeMs = 0;
  #loadedAtMs = Date.now();
  #persistedAtMs: number | null = null;
  #lastResponseAtMs: number | null = null;
  #persistChain: Promise<void> = Promise.resolve();
  #mutationChain: Promise<void> = Promise.resolve();
  #jarVersion = 0;
  #jarEpoch = 0;

  private constructor(options: AccountSessionOptions, jar: CookieJar) {
    this.#origin = normalizeOrigin(options.origin);
    this.#cookieFile = options.cookieFile ? path.resolve(options.cookieFile) : undefined;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#jar = jar;
  }

  static async create(options: AccountSessionOptions): Promise<PrismAccountSession> {
    const origin = normalizeOrigin(options.origin);
    const cookieFile = options.cookieFile ? path.resolve(options.cookieFile) : undefined;
    const fromFile = cookieFile ? await jarFromFile(cookieFile) : null;
    const jar = fromFile ?? new CookieJar();
    if (!fromFile && options.initialCookieHeader) {
      await applyCookieHeader(jar, options.initialCookieHeader, `${origin}/`);
    }
    const session = new PrismAccountSession({ ...options, origin, cookieFile }, jar);
    if (cookieFile) {
      try { session.#fileMtimeMs = (await stat(cookieFile)).mtimeMs; } catch {}
    }
    return session;
  }

  async cookieHeader(url: string): Promise<string> {
    return this.#withJarLock(() => this.#jar.getCookieString(url));
  }

  async setCookie(cookie: string, url = `${this.#origin}/`): Promise<void> {
    await this.#withJarLock(async () => {
      this.#jarVersion += 1;
      await this.#jar.setCookie(cookie, url);
      await this.#queuePersist(this.#jar);
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
    const replacement = await jarFromFile(this.#cookieFile);
    if (!replacement) return false;
    return this.#withJarLock(async () => {
      if (this.#jarVersion !== baseline.version) return false;
      if (!options.force && metadata.mtimeMs <= this.#fileMtimeMs) return false;
      this.#jar = replacement;
      this.#jarVersion += 1;
      this.#jarEpoch += 1;
      this.#fileMtimeMs = metadata.mtimeMs;
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
      if (this.#cookieFile) {
        try { fileMtimeMs = (await stat(this.#cookieFile)).mtimeMs; } catch {}
        const fromFile = await jarFromFile(this.#cookieFile);
        if (fromFile) {
          jar = fromFile;
        } else {
          fileMtimeMs = null;
        }
      }
      return { jar, fileMtimeMs, baseVersion: this.#jarVersion };
    });

    let finalized = false;
    let activeRequests = 0;
    let changed = false;
    let lastResponseAtMs: number | null = null;
    return {
      request: async (pathOrUrl, init = {}) => {
        if (finalized) throw new Error("Cookie candidate is already finalized");
        activeRequests += 1;
        try {
          const { response, url } = await this.#fetchWithJar(source.jar, pathOrUrl, init);
          const cookies = responseCookies(response.headers);
          for (const value of cookies) await source.jar.setCookie(value, url.href);
          changed ||= cookies.length > 0;
          lastResponseAtMs = Date.now();
          this.#throwAccountAuth(response, url);
          return response;
        } finally {
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
    const requestState = await this.#withJarLock(async () => ({
      jar: this.#jar,
      epoch: this.#jarEpoch,
    }));
    const { response, url } = await this.#fetchWithJar(requestState.jar, pathOrUrl, init);
    const cookies = responseCookies(response.headers);
    if (cookies.length > 0) {
      await this.#withJarLock(async () => {
        // Never apply a response authenticated with a jar that has since been replaced.
        // Same-jar rotations may still merge, preserving concurrent independent cookies.
        if (this.#jar !== requestState.jar || this.#jarEpoch !== requestState.epoch) return;
        this.#jarVersion += 1;
        for (const value of cookies) await this.#jar.setCookie(value, url.href);
        await this.#queuePersist(this.#jar);
      });
    }
    this.#lastResponseAtMs = Date.now();
    this.#throwAccountAuth(response, url);
    return response;
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
      if (this.#cookieFile) await this.#queuePersist(this.#jar);
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
      baseVersion: number;
      changed: boolean;
      lastResponseAtMs: number | null;
    },
    signal?: AbortSignal,
  ): Promise<boolean> {
    return this.#withJarLock(async () => {
      throwIfAborted(signal);
      if (this.#jarVersion !== candidate.baseVersion) return false;
      if (candidate.changed) await this.#queuePersist(candidate.jar);
      // Once persistence starts, finish the in-memory half of the commit even if the
      // caller aborts. This keeps the disk and live jars transactionally consistent.
      this.#jar = candidate.jar;
      this.#jarVersion += 1;
      this.#jarEpoch += 1;
      if (!candidate.changed && candidate.fileMtimeMs != null) {
        this.#fileMtimeMs = candidate.fileMtimeMs;
      }
      this.#loadedAtMs = Date.now();
      if (candidate.lastResponseAtMs != null) {
        this.#lastResponseAtMs = candidate.lastResponseAtMs;
      }
      return true;
    });
  }

  #withJarLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.#mutationChain;
    let release!: () => void;
    this.#mutationChain = new Promise<void>(resolve => { release = resolve; });
    return previous.then(operation).finally(release);
  }

  #queuePersist(jar: CookieJar): Promise<void> {
    if (!this.#cookieFile) return Promise.resolve();
    const serialized = jar.serializeSync();
    if (!serialized) return Promise.reject(new Error("Cookie jar serialization failed"));
    const body = JSON.stringify(serialized, null, 2);
    this.#persistChain = this.#persistChain
      .catch(() => undefined)
      .then(async () => {
        const file = this.#cookieFile!;
        await mkdir(path.dirname(file), { recursive: true });
        const temporary = path.join(
          path.dirname(file),
          `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.tmp`,
        );
        await writeFile(temporary, body, { mode: 0o600 });
        if (process.platform !== "win32") await chmod(temporary, 0o600);
        const metadata = await stat(temporary);
        // Keep rename as the final fallible filesystem operation. After it succeeds,
        // candidate commit can update the live jar without a disk/live split.
        await rename(temporary, file);
        this.#fileMtimeMs = metadata.mtimeMs;
        this.#persistedAtMs = Date.now();
      });
    return this.#persistChain;
  }
}
