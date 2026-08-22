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
    return this.#jar.getCookieString(url);
  }

  async setCookie(cookie: string, url = `${this.#origin}/`): Promise<void> {
    await this.#jar.setCookie(cookie, url);
    await this.#queuePersist();
  }

  async reloadIfChanged(): Promise<void> {
    if (!this.#cookieFile) return;
    let metadata;
    try { metadata = await stat(this.#cookieFile); } catch { return; }
    if (metadata.mtimeMs <= this.#fileMtimeMs) return;
    const replacement = await jarFromFile(this.#cookieFile);
    if (!replacement) return;
    this.#jar = replacement;
    this.#fileMtimeMs = metadata.mtimeMs;
    this.#loadedAtMs = Date.now();
  }

  async request(pathOrUrl: string, init: RequestInit = {}): Promise<Response> {
    await this.reloadIfChanged();
    const url = new URL(pathOrUrl, `${this.#origin}/`);
    const headers = new Headers(init.headers);
    const cookie = await this.#jar.getCookieString(url.href);
    if (cookie) headers.set("cookie", cookie);
    const method = (init.method ?? "GET").toUpperCase();

    let response: Response;
    try {
      response = await this.#fetch(url, { ...init, method, headers });
    } catch {
      throw new AccountTransportError(method, url.pathname);
    }

    let changed = false;
    for (const value of responseCookies(response.headers)) {
      await this.#jar.setCookie(value, url.href);
      changed = true;
    }
    if (changed) await this.#queuePersist();
    this.#lastResponseAtMs = Date.now();

    if ((response.status === 401 || response.status === 403) && url.pathname.startsWith("/api/")) {
      throw new AccountAuthError(response.status, url.pathname);
    }
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
    if (this.#cookieFile) await this.#queuePersist();
    await this.#persistChain;
  }

  #queuePersist(): Promise<void> {
    if (!this.#cookieFile) return Promise.resolve();
    this.#persistChain = this.#persistChain
      .catch(() => undefined)
      .then(async () => {
        const file = this.#cookieFile!;
        await mkdir(path.dirname(file), { recursive: true });
        const temporary = path.join(
          path.dirname(file),
          `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.tmp`,
        );
        const serialized = this.#jar.serializeSync();
        if (!serialized) throw new Error("Cookie jar serialization failed");
        await writeFile(temporary, JSON.stringify(serialized, null, 2), { mode: 0o600 });
        await rename(temporary, file);
        if (process.platform !== "win32") await chmod(file, 0o600);
        const metadata = await stat(file);
        this.#fileMtimeMs = metadata.mtimeMs;
        this.#persistedAtMs = Date.now();
      });
    return this.#persistChain;
  }
}
