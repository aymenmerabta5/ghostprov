import {
  mkdir,
  open,
  readFile,
  rename,
  stat,
  unlink,
} from "node:fs/promises";
import * as path from "node:path";
import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "./ports";

interface LeaseRecord {
  pid: number;
  nonce: string;
  acquiredAtMs: number;
}

export class FileLeaseBusyError extends Error {
  readonly name = "FileLeaseBusyError";

  constructor(readonly file: string) {
    super(`Timed out waiting for exclusive state lease: ${path.basename(file)}`);
  }
}

export class FileLeaseOwnershipError extends Error {
  readonly name = "FileLeaseOwnershipError";

  constructor(readonly file: string) {
    super(`Exclusive state lease ownership changed: ${path.basename(file)}`);
  }
}

export interface FileLeaseOptions {
  file: string;
  staleAfterMs: number;
  clock?: Clock;
  random?: RandomSource;
}

function isNodeError(error: unknown, code: string): boolean {
  return !!error && typeof error === "object" && (error as { code?: unknown }).code === code;
}

function parseRecord(raw: string): LeaseRecord | null {
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (!Number.isInteger(value.pid) || (value.pid as number) < 1) return null;
    if (typeof value.nonce !== "string" || value.nonce.length < 1) return null;
    if (typeof value.acquiredAtMs !== "number" || !Number.isFinite(value.acquiredAtMs)) return null;
    return {
      pid: value.pid as number,
      nonce: value.nonce,
      acquiredAtMs: value.acquiredAtMs,
    };
  } catch {
    return null;
  }
}

function processIsAlive(pid: number): boolean {
  if (pid === process.pid) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = (error as { code?: unknown })?.code;
    return code === "EPERM";
  }
}

export class FileLease {
  readonly #file: string;
  readonly #staleAfterMs: number;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  #nonce: string | null = null;

  constructor(options: FileLeaseOptions) {
    if (!Number.isFinite(options.staleAfterMs) || options.staleAfterMs < 1) {
      throw new RangeError("staleAfterMs must be positive");
    }
    this.#file = path.resolve(options.file);
    this.#staleAfterMs = options.staleAfterMs;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
  }

  async acquire(options: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<void> {
    if (this.#nonce) return;
    const timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new RangeError("timeoutMs must be non-negative");
    }
    const deadline = this.#clock.now() + timeoutMs;
    for (;;) {
      if (options.signal?.aborted) throw new DOMException("File lease acquisition aborted", "AbortError");
      if (await this.#tryCreate()) return;
      if (await this.#reclaimStale()) continue;
      const remaining = deadline - this.#clock.now();
      if (remaining <= 0) throw new FileLeaseBusyError(this.#file);
      const jitter = Math.floor(this.#random.next() * 5);
      await this.#clock.sleep(Math.min(remaining, 10 + jitter), options.signal);
    }
  }

  async release(): Promise<void> {
    const nonce = this.#nonce;
    if (!nonce) return;
    let current: LeaseRecord | null;
    try {
      current = parseRecord(await readFile(this.#file, "utf8"));
    } catch (error) {
      if (isNodeError(error, "ENOENT")) {
        this.#nonce = null;
        return;
      }
      throw error;
    }
    if (current?.nonce !== nonce) {
      this.#nonce = null;
      throw new FileLeaseOwnershipError(this.#file);
    }
    try {
      await unlink(this.#file);
    } catch (error) {
      if (!isNodeError(error, "ENOENT")) throw error;
    }
    this.#nonce = null;
  }

  async #tryCreate(): Promise<boolean> {
    await mkdir(path.dirname(this.#file), { recursive: true });
    const nonce = this.#random.uuid();
    let handle;
    try {
      handle = await open(this.#file, "wx", 0o600);
    } catch (error) {
      if (isNodeError(error, "EEXIST")) return false;
      throw error;
    }
    let committed = false;
    try {
      const record: LeaseRecord = {
        pid: process.pid,
        nonce,
        acquiredAtMs: this.#clock.now(),
      };
      await handle.writeFile(JSON.stringify(record));
      await handle.sync();
      this.#nonce = nonce;
      committed = true;
      return true;
    } finally {
      await handle.close();
      if (!committed) await unlink(this.#file).catch(() => undefined);
    }
  }

  async #reclaimStale(): Promise<boolean> {
    let metadata;
    let raw: string;
    try {
      [metadata, raw] = await Promise.all([
        stat(this.#file),
        readFile(this.#file, "utf8"),
      ]);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      throw error;
    }
    if (this.#clock.now() - metadata.mtimeMs <= this.#staleAfterMs) return false;
    const record = parseRecord(raw);
    if (record && processIsAlive(record.pid)) return false;

    let latestRaw: string;
    try {
      latestRaw = await readFile(this.#file, "utf8");
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      throw error;
    }
    if (record) {
      if (parseRecord(latestRaw)?.nonce !== record.nonce) return false;
    } else if (latestRaw !== raw) {
      return false;
    }

    const quarantine = `${this.#file}.stale.${process.pid}.${this.#random.uuid()}`;
    try {
      await rename(this.#file, quarantine);
      await unlink(quarantine);
      return true;
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      if (isNodeError(error, "EACCES") || isNodeError(error, "EPERM")) return false;
      throw error;
    }
  }
}
