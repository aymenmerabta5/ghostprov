import {
  mkdir,
  open,
  readdir,
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
  readonly #activePath: string;
  #handle: FileHandle | null = null;
  #bytes = 0;
  #sequence = 0;
  #closed = false;

  constructor(options: RotatingJsonlSinkOptions) {
    if (!Number.isSafeInteger(options.rotateBytes) || options.rotateBytes < 1) {
      throw new RangeError("rotateBytes must be positive");
    }
    if (!Number.isSafeInteger(options.retainFiles) || options.retainFiles < 1) {
      throw new RangeError("retainFiles must be positive");
    }
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(options.prefix)) {
      throw new TypeError("Log prefix is invalid");
    }
    this.#directory = path.resolve(options.directory);
    this.#prefix = options.prefix;
    this.#rotateBytes = options.rotateBytes;
    this.#retainFiles = options.retainFiles;
    this.#pid = options.pid ?? process.pid;
    this.#now = options.now ?? Date.now;
    this.#activePath = path.join(
      this.#directory,
      `${this.#prefix}.current.${this.#pid}.jsonl`,
    );
  }

  async writeBatch(lines: readonly string[]): Promise<void> {
    if (this.#closed || lines.length === 0) return;
    const content = lines.join("");
    const bytes = Buffer.byteLength(content);
    await this.#ensureOpen();
    if (this.#bytes > 0 && this.#bytes + bytes > this.#rotateBytes) {
      await this.#rotate();
      await this.#ensureOpen();
    }
    await this.#handle!.write(content);
    this.#bytes += bytes;
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    if (!this.#handle) return;
    await this.#handle.sync();
    await this.#handle.close();
    this.#handle = null;
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

  async #enforceRetention(): Promise<void> {
    const escaped = this.#prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const pattern = new RegExp(
      `^${escaped}\\.\\d{8}T\\d{9}Z\\.${this.#pid}\\.\\d+\\.jsonl$`,
    );
    const matching = (await readdir(this.#directory))
      .filter(name => pattern.test(name))
      .sort();
    const excess = matching.slice(0, Math.max(0, matching.length - this.#retainFiles));
    await Promise.all(excess.map(name => unlink(path.join(this.#directory, name))));
  }
}
