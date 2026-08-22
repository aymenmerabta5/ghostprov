import { createHash } from "node:crypto";
import {
  chmod,
  mkdir,
  readFile,
  rename,
  writeFile,
} from "node:fs/promises";
import * as path from "node:path";
import type { PrismSessionMode } from "../config/multi-sandbox";
import { FileLease } from "../core/file-lease";
import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type FetchLike,
  type RandomSource,
} from "../core/ports";
import { PrismAccountSession } from "./account-session";

export type PrismIdentityKind = "authenticated" | "anonymous";

export interface ResolvedPrismIdentity {
  kind: PrismIdentityKind;
  fingerprint: string;
  account: PrismAccountSession;
  resolvedAtMs: number;
}

interface IdentityMetadata {
  version: 1;
  kind: PrismIdentityKind;
  fingerprint: string;
}

interface AuthUser {
  id: string;
  kind: PrismIdentityKind;
}

export type PrismSessionErrorKind =
  | "auth_required"
  | "guest_identity_lost"
  | "mode_mismatch"
  | "protocol";

export class PrismSessionError extends Error {
  readonly name = "PrismSessionError";

  constructor(
    message: string,
    readonly kind: PrismSessionErrorKind,
    readonly status?: number,
  ) {
    super(message);
  }
}

export interface PrismSessionManagerOptions {
  origin: string;
  mode: PrismSessionMode;
  allowAnonymous: boolean;
  cookieFile: string;
  identityFile?: string;
  initialCookieHeader?: string;
  fetch?: FetchLike;
  clock?: Clock;
  random?: RandomSource;
  lockTimeoutMs?: number;
}

function isNodeError(error: unknown, code: string): boolean {
  return !!error && typeof error === "object" && (error as { code?: unknown }).code === code;
}

function fingerprint(userId: string): string {
  return createHash("sha256").update(userId).digest("hex");
}

function parseIdentityMetadata(value: unknown): IdentityMetadata {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new PrismSessionError("Prism identity metadata must be an object", "protocol");
  }
  const record = value as Record<string, unknown>;
  const kind = record.kind;
  const hash = record.fingerprint;
  if (
    record.version !== 1 ||
    (kind !== "authenticated" && kind !== "anonymous") ||
    typeof hash !== "string" ||
    !/^[a-f0-9]{64}$/.test(hash)
  ) {
    throw new PrismSessionError("Prism identity metadata is invalid", "protocol");
  }
  return { version: 1, kind, fingerprint: hash };
}

function parseAuthUser(value: unknown): AuthUser | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const state = value as Record<string, unknown>;
  const session = state.session && typeof state.session === "object" && !Array.isArray(state.session)
    ? state.session as Record<string, unknown>
    : null;
  const candidate = state.user ?? session?.user;
  if (candidate == null) return null;
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
    throw new PrismSessionError("Prism auth user is invalid", "protocol");
  }
  const user = candidate as Record<string, unknown>;
  if (typeof user.id !== "string" || user.id.length === 0) {
    throw new PrismSessionError("Prism auth user ID is missing", "protocol");
  }
  return {
    id: user.id,
    kind: user.is_anonymous === true ? "anonymous" : "authenticated",
  };
}

export class PrismSessionManager {
  readonly #mode: PrismSessionMode;
  readonly #allowAnonymous: boolean;
  readonly #identityFile: string;
  readonly #account: PrismAccountSession;
  readonly #lease: FileLease;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  readonly #lockTimeoutMs: number;
  #leaseHeld = false;
  #closed = false;
  #resolved: ResolvedPrismIdentity | null = null;
  #resolving: Promise<ResolvedPrismIdentity> | null = null;

  private constructor(
    options: PrismSessionManagerOptions,
    account: PrismAccountSession,
  ) {
    this.#mode = options.mode;
    this.#allowAnonymous = options.allowAnonymous;
    this.#identityFile = path.resolve(options.identityFile ?? `${options.cookieFile}.identity.json`);
    this.#account = account;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#lockTimeoutMs = options.lockTimeoutMs ?? 30_000;
    this.#lease = new FileLease({
      file: `${path.resolve(options.cookieFile)}.lock`,
      staleAfterMs: 60_000,
      clock: this.#clock,
      random: this.#random,
    });
  }

  static async create(options: PrismSessionManagerOptions): Promise<PrismSessionManager> {
    const account = await PrismAccountSession.create({
      origin: options.origin,
      cookieFile: options.cookieFile,
      initialCookieHeader: options.initialCookieHeader,
      fetch: options.fetch,
    });
    return new PrismSessionManager(options, account);
  }

  resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity> {
    if (this.#closed) return Promise.reject(new PrismSessionError("Prism session manager is closed", "protocol"));
    if (this.#resolved) return Promise.resolve(this.#resolved);
    if (!this.#resolving) {
      const resolving = this.#resolve(signal)
        .then(value => {
          this.#resolved = value;
          return value;
        })
        .finally(() => {
          if (this.#resolving === resolving) this.#resolving = null;
        });
      this.#resolving = resolving;
    }
    return this.#resolving;
  }

  async flush(): Promise<void> {
    await this.#ensureLease();
    await this.#account.flush();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#resolving) await this.#resolving.catch(() => undefined);
    if (!this.#leaseHeld) return;
    try {
      await this.#account.flush();
    } finally {
      await this.#lease.release();
      this.#leaseHeld = false;
    }
  }

  async #resolve(signal?: AbortSignal): Promise<ResolvedPrismIdentity> {
    await this.#ensureLease(signal);
    const metadata = await this.#readIdentityMetadata();
    let user = await this.#loadAuthUser(signal);
    if (!user) {
      if (metadata?.kind === "anonymous") {
        throw new PrismSessionError(
          "The persisted anonymous Prism identity is no longer available",
          "guest_identity_lost",
        );
      }
      const canCreateGuest = this.#mode === "anonymous" ||
        (this.#mode === "auto" && this.#allowAnonymous);
      if (!canCreateGuest) {
        throw new PrismSessionError("A signed-in Prism session is required", "auth_required");
      }
      const created = await this.#account.request("/api/auth/anonymous-session", {
        method: "POST",
        signal,
        headers: { accept: "application/json" },
      });
      if (!created.ok && created.status !== 409) {
        throw new PrismSessionError(
          `Anonymous Prism session creation failed (${created.status})`,
          "protocol",
          created.status,
        );
      }
      await this.#account.flush();
      user = await this.#loadAuthUser(signal);
      if (!user || user.kind !== "anonymous") {
        throw new PrismSessionError("Anonymous Prism session was not established", "protocol");
      }
    }
    return this.#acceptUser(user, metadata);
  }

  async #ensureLease(signal?: AbortSignal): Promise<void> {
    if (this.#leaseHeld) return;
    await this.#lease.acquire({ timeoutMs: this.#lockTimeoutMs, signal });
    this.#leaseHeld = true;
  }

  async #loadAuthUser(signal?: AbortSignal): Promise<AuthUser | null> {
    const response = await this.#account.request("/auth/session", {
      method: "GET",
      signal,
      headers: { accept: "application/json" },
    });
    if (response.status === 401 || response.status === 403) return null;
    if (!response.ok) {
      throw new PrismSessionError(
        `Prism auth session resolution failed (${response.status})`,
        "protocol",
        response.status,
      );
    }
    let body: unknown;
    try {
      body = await response.json();
    } catch {
      throw new PrismSessionError("Prism auth session returned invalid JSON", "protocol");
    }
    return parseAuthUser(body);
  }

  async #acceptUser(
    user: AuthUser,
    metadata: IdentityMetadata | null,
  ): Promise<ResolvedPrismIdentity> {
    if (this.#mode === "authenticated" && user.kind !== "authenticated") {
      throw new PrismSessionError("The Prism session is anonymous, not authenticated", "mode_mismatch");
    }
    if (this.#mode === "anonymous" && user.kind !== "anonymous") {
      throw new PrismSessionError("The Prism session is authenticated, not anonymous", "mode_mismatch");
    }
    const hash = fingerprint(user.id);
    if (metadata && (metadata.kind !== user.kind || metadata.fingerprint !== hash)) {
      throw new PrismSessionError("The persisted Prism identity does not match the active session", "mode_mismatch");
    }
    if (!metadata) {
      await this.#writeIdentityMetadata({
        version: 1,
        kind: user.kind,
        fingerprint: hash,
      });
    }
    return {
      kind: user.kind,
      fingerprint: hash,
      account: this.#account,
      resolvedAtMs: this.#clock.now(),
    };
  }

  async #readIdentityMetadata(): Promise<IdentityMetadata | null> {
    let raw: string;
    try {
      raw = await readFile(this.#identityFile, "utf8");
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return null;
      throw error;
    }
    try {
      return parseIdentityMetadata(JSON.parse(raw));
    } catch (error) {
      if (error instanceof PrismSessionError) throw error;
      throw new PrismSessionError("Prism identity metadata returned invalid JSON", "protocol");
    }
  }

  async #writeIdentityMetadata(metadata: IdentityMetadata): Promise<void> {
    await mkdir(path.dirname(this.#identityFile), { recursive: true });
    const temporary = path.join(
      path.dirname(this.#identityFile),
      `.${path.basename(this.#identityFile)}.${process.pid}.${this.#random.uuid()}.tmp`,
    );
    await writeFile(temporary, JSON.stringify(metadata, null, 2), { mode: 0o600 });
    await rename(temporary, this.#identityFile);
    if (process.platform !== "win32") await chmod(this.#identityFile, 0o600);
  }
}
