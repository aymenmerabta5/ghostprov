import {
  mkdir,
  open,
  opendir,
  rename,
  stat,
  unlink,
  type FileHandle,
} from "node:fs/promises";
import * as path from "node:path";

export interface RotatingJsonlSinkOptions {
  directory: string;
  prefix: string;
  rotateBytes: number;
  retainFiles: number;
  pid?: number;
  now?: () => number;
  /** Deterministic test seam; production checks operating-system PID liveness. */
  isProcessAlive?: (pid: number) => boolean;
}

function timestamp(value: number): string {
  return new Date(value).toISOString()
    .replaceAll("-", "")
    .replaceAll(":", "")
    .replace(".", "");
}

export class RotatingJsonlSink {
  readonly #directory: string;
  readonly #prefix: string;
  readonly #rotateBytes: number;
  readonly #retainFiles: number;
  readonly #pid: number;
  readonly #now: () => number;
  readonly #isProcessAlive: (pid: number) => boolean;
  readonly #activePath: string;
  #handle: FileHandle | null = null;
  #bytes = 0;
  #sequence = 0;
  #closed = false;
  #retentionChecked = false;

  constructor(options: RotatingJsonlSinkOptions) {
    if (!Number.isSafeInteger(options.rotateBytes) || options.rotateBytes < 1) {
      throw new RangeError("rotateBytes must be positive");
    }
    if (!Number.isSafeInteger(options.retainFiles) ||
      options.retainFiles < 1 || options.retainFiles > 100) {
      throw new RangeError("retainFiles must be from 1 through 100");
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(options.prefix)) {
      throw new TypeError("Log prefix is invalid");
    }
    if (options.pid !== undefined && (!Number.isSafeInteger(options.pid) || options.pid < 1)) {
      throw new RangeError("Log PID must be a positive safe integer");
    }
    this.#directory = path.resolve(options.directory);
    this.#prefix = options.prefix;
    this.#rotateBytes = options.rotateBytes;
    this.#retainFiles = options.retainFiles;
    this.#pid = options.pid ?? process.pid;
    this.#now = options.now ?? Date.now;
    this.#isProcessAlive = options.isProcessAlive ?? processIsAlive;
    this.#activePath = path.join(
      this.#directory,
      `${this.#prefix}.current.${this.#pid}.jsonl`,
    );
  }

  async writeBatch(lines: readonly string[]): Promise<void> {
    if (this.#closed || lines.length === 0) return;
    await this.#ensureOpen();
    let pending: string[] = [];
    let pendingBytes = 0;
    const flush = async () => {
      if (pendingBytes === 0) return;
      await this.#handle!.write(pending.join(""));
      this.#bytes += pendingBytes;
      pending = [];
      pendingBytes = 0;
    };
    for (const line of lines) {
      const lineBytes = Buffer.byteLength(line);
      if (lineBytes > this.#rotateBytes) {
        throw new RangeError("One observability record exceeds the rotation byte limit");
      }
      if (this.#bytes + pendingBytes > 0 &&
        this.#bytes + pendingBytes + lineBytes > this.#rotateBytes) {
        await flush();
        await this.#rotate();
        await this.#ensureOpen();
      }
      pending.push(line);
      pendingBytes += lineBytes;
    }
    await flush();
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (!this.#handle) return;
    await this.#handle.sync();
    await this.#handle.close();
    this.#handle = null;
    await this.#enforceRetention();
  }

  async #ensureOpen(): Promise<void> {
    if (this.#handle) return;
    await mkdir(this.#directory, { recursive: true });
    this.#handle = await open(this.#activePath, "a", 0o600);
    try {
      this.#bytes = (await stat(this.#activePath)).size;
    } catch {
      this.#bytes = 0;
    }
    if (!this.#retentionChecked) {
      this.#retentionChecked = true;
      await this.#enforceRetention(new Set([path.basename(this.#activePath)]));
    }
  }

  async #rotate(): Promise<void> {
    if (!this.#handle) return;
    await this.#handle.sync();
    await this.#handle.close();
    this.#handle = null;
    const rotated = path.join(
      this.#directory,
      `${this.#prefix}.${timestamp(this.#now())}.${this.#pid}.${this.#sequence++}.jsonl`,
    );
    await rename(this.#activePath, rotated);
    this.#bytes = 0;
    await this.#enforceRetention();
  }

  async #enforceRetention(protectedNames: ReadonlySet<string> = new Set()): Promise<void> {
    const escaped = this.#prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const anyRotatedPattern = new RegExp(
      `^${escaped}\\.\\d{8}T\\d{9}Z\\.(\\d+)\\.\\d+\\.jsonl$`,
    );
    const currentPattern = new RegExp(`^${escaped}\\.current\\.(\\d+)\\.jsonl$`);
    const retained: Array<{ name: string; mtimeMs: number }> = [];
    const remove = async (name: string) => {
      try {
        await unlink(path.join(this.#directory, name));
      } catch (error) {
        if (!isNodeError(error, "ENOENT")) throw error;
      }
    };
    const directory = await opendir(this.#directory);
    for await (const entry of directory) {
      if (!entry.isFile()) continue;
      const name = entry.name;
      if (protectedNames.has(name)) continue;
      const current = currentPattern.exec(name);
      if (current && this.#isProcessAlive(Number(current[1]))) continue;
      if (!current && !anyRotatedPattern.test(name)) continue;
      const metadata = await stat(path.join(this.#directory, name)).catch(() => null);
      if (!metadata?.isFile()) continue;
      const candidate = { name, mtimeMs: metadata.mtimeMs };
      if (retained.length < this.#retainFiles) {
        retained.push(candidate);
        continue;
      }
      let oldestIndex = 0;
      for (let index = 1; index < retained.length; index++) {
        const currentOldest = retained[oldestIndex]!;
        const contender = retained[index]!;
        if (
          contender.mtimeMs < currentOldest.mtimeMs ||
          (contender.mtimeMs === currentOldest.mtimeMs && contender.name < currentOldest.name)
        ) oldestIndex = index;
      }
      const oldest = retained[oldestIndex]!;
      const candidateIsNewer = candidate.mtimeMs > oldest.mtimeMs ||
        (candidate.mtimeMs === oldest.mtimeMs && candidate.name > oldest.name);
      if (candidateIsNewer) {
        retained[oldestIndex] = candidate;
        await remove(oldest.name);
      } else {
        await remove(candidate.name);
      }
    }
  }
}

function isNodeError(error: unknown, code: string): boolean {
  return !!error && typeof error === "object" &&
    (error as { code?: unknown }).code === code;
}

function processIsAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid < 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !isNodeError(error, "ESRCH");
  }
}
