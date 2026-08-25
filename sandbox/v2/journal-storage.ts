import type {
  JournalRecord,
  JournalStorage,
} from "../../src/v2/sidecar/journal.ts";
import {
  canonicalJson,
  sha256Hex,
  utf8Bytes,
} from "../../src/v2/protocol/shared.ts";

interface StoredRecord {
  record: JournalRecord;
  checksum: string;
}

const SAFE_REQUEST = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

async function writeAll(file: Deno.FsFile, bytes: Uint8Array): Promise<void> {
  let offset = 0;
  while (offset < bytes.byteLength) {
    const written = await file.write(bytes.subarray(offset));
    if (written < 1) throw new Error("journal write made no progress");
    offset += written;
  }
}

function terminalName(requestId: string): string {
  if (!SAFE_REQUEST.test(requestId)) throw new Error("invalid terminal request ID");
  return `${requestId}.json`;
}

export interface DenoJournalStorageOptions {
  root?: string;
  /** Retained for non-destructive diagnostics/backward-compatible construction. */
  assertExclusiveOwnership?: () => Promise<void>;
  /** Holds the ownership lease throughout one destructive filesystem operation. */
  withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
}

export class DenoJournalStorage implements JournalStorage {
  readonly #root: string;
  readonly #journal: string;
  readonly #terminals: string;
  readonly replaceRecords?: (records: readonly JournalRecord[]) => Promise<void>;
  readonly pruneTerminals?: (retainedRequestIds: readonly string[]) => Promise<void>;

  constructor(options: string | DenoJournalStorageOptions = {}) {
    const root = typeof options === "string"
      ? options
      : options.root ?? "/tmp/ghostprov-v2";
    this.#root = root.replace(/\/+$/, "");
    this.#journal = `${this.#root}/claims.jsonl`;
    this.#terminals = `${this.#root}/terminals`;
    const assertExclusiveOwnership = typeof options === "string"
      ? undefined
      : options.assertExclusiveOwnership;
    if (
      assertExclusiveOwnership !== undefined &&
      typeof assertExclusiveOwnership !== "function"
    ) throw new TypeError("assertExclusiveOwnership must be a function");
    const withExclusiveOwnership = typeof options === "string"
      ? undefined
      : options.withExclusiveOwnership;
    if (
      withExclusiveOwnership !== undefined &&
      typeof withExclusiveOwnership !== "function"
    ) throw new TypeError("withExclusiveOwnership must be a function");
    if (withExclusiveOwnership) {
      this.replaceRecords = async records => {
        await withExclusiveOwnership(() => this.#replaceRecords(records));
      };
      this.pruneTerminals = async retainedRequestIds => {
        await withExclusiveOwnership(() => this.#pruneTerminals(retainedRequestIds));
      };
    }
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    if (!records.length) return;
    await this.#ensureDirectories();
    const bytes = await this.#encodeRecords(records);
    const file = await Deno.open(this.#journal, {
      create: true,
      append: true,
      write: true,
      mode: 0o600,
    });
    let durable = false;
    try {
      await writeAll(file, bytes);
      await file.syncData();
      durable = true;
    } finally {
      try { file.close(); } catch (error) { if (!durable) throw error; }
    }
    // Permission refresh is hygiene after the fsync durability boundary. It
    // must not report a false append failure that could delete a valid spool.
    await Deno.chmod(this.#journal, 0o600).catch(() => undefined);
  }

  async #replaceRecords(records: readonly JournalRecord[]): Promise<void> {
    await this.#ensureDirectories();
    const temporary = `${this.#journal}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
    const file = await Deno.open(temporary, {
      createNew: true,
      write: true,
      mode: 0o600,
    });
    let renamed = false;
    try {
      await writeAll(file, await this.#encodeRecords(records));
      await file.syncData();
      file.close();
      await Deno.rename(temporary, this.#journal);
      renamed = true;
      await Deno.chmod(this.#journal, 0o600).catch(() => undefined);
      await this.#syncDirectory(this.#root);
    } finally {
      try { file.close(); } catch {}
      if (!renamed) await Deno.remove(temporary).catch(() => undefined);
    }
  }

  async writeTerminal(requestId: string, bytes: Uint8Array): Promise<void> {
    await this.#ensureDirectories();
    const destination = `${this.#terminals}/${terminalName(requestId)}`;
    const temporary = `${destination}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
    const file = await Deno.open(temporary, {
      createNew: true,
      write: true,
      mode: 0o600,
    });
    let renamed = false;
    try {
      await writeAll(file, bytes);
      await file.syncData();
      file.close();
      await Deno.rename(temporary, destination);
      renamed = true;
      await Deno.chmod(destination, 0o600).catch(() => undefined);
      await this.#syncDirectory(this.#terminals);
    } finally {
      try { file.close(); } catch {}
      if (!renamed) await Deno.remove(temporary).catch(() => undefined);
    }
  }

  async readTerminal(requestId: string): Promise<Uint8Array | null> {
    const file = `${this.#terminals}/${terminalName(requestId)}`;
    try {
      return await Deno.readFile(file);
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return null;
      throw error;
    }
  }

  async terminalExists(requestId: string): Promise<boolean> {
    const file = `${this.#terminals}/${terminalName(requestId)}`;
    try {
      return (await Deno.stat(file)).isFile;
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return false;
      throw error;
    }
  }

  async deleteTerminal(requestId: string): Promise<void> {
    const file = `${this.#terminals}/${terminalName(requestId)}`;
    try {
      await Deno.remove(file);
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    }
  }

  async #pruneTerminals(retainedRequestIds: readonly string[]): Promise<void> {
    const retained = new Set(retainedRequestIds.map(terminalName));
    try {
      for await (const entry of Deno.readDir(this.#terminals)) {
        if (!entry.isFile) continue;
        const isTerminal = entry.name.endsWith(".json");
        const isTemporary = entry.name.endsWith(".tmp");
        if ((!isTerminal || retained.has(entry.name)) && !isTemporary) continue;
        await Deno.remove(`${this.#terminals}/${entry.name}`).catch(error => {
          if (!(error instanceof Deno.errors.NotFound)) throw error;
        });
      }
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return;
      throw error;
    }
  }

  async readRecords(): Promise<readonly JournalRecord[]> {
    let raw: string;
    try {
      raw = await Deno.readTextFile(this.#journal);
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return [];
      throw error;
    }
    const endedCleanly = raw.length === 0 || raw.endsWith("\n");
    const lines = raw.split("\n");
    if (lines.at(-1) === "") lines.pop();
    const records: JournalRecord[] = [];
    for (let index = 0; index < lines.length; index++) {
      try {
        const decoded = JSON.parse(lines[index]) as StoredRecord;
        if (
          !decoded ||
          typeof decoded !== "object" ||
          typeof decoded.checksum !== "string" ||
          decoded.checksum !== await sha256Hex(canonicalJson(decoded.record))
        ) throw new Error("journal checksum mismatch");
        records.push(decoded.record);
      } catch (error) {
        const isTornTail = index === lines.length - 1 && !endedCleanly;
        if (isTornTail) break;
        throw new Error("journal corruption before trailing append", { cause: error });
      }
    }
    // Appending after any unterminated tail (even a fully valid JSON record)
    // would concatenate the next record onto it. Rewrite the proven prefix now
    // so future appends remain independently parseable.
    if (!endedCleanly) {
      if (!this.replaceRecords) {
        throw new Error("torn journal repair requires exclusive sidecar ownership");
      }
      await this.replaceRecords(records);
    }
    return records;
  }

  async #encodeRecords(records: readonly JournalRecord[]): Promise<Uint8Array> {
    const lines: string[] = [];
    for (const record of records) {
      const stored: StoredRecord = {
        record,
        checksum: await sha256Hex(canonicalJson(record)),
      };
      lines.push(`${JSON.stringify(stored)}\n`);
    }
    return utf8Bytes(lines.join(""));
  }

  async #syncDirectory(path: string): Promise<void> {
    // Linux requires syncing the parent directory for rename durability. Some
    // filesystems reject directory fsync; the file itself is already synced in
    // that case and the next startup still validates the complete checksum log.
    try {
      const directory = await Deno.open(path, { read: true });
      try { await directory.sync(); } finally { directory.close(); }
    } catch {}
  }

  async #ensureDirectories(): Promise<void> {
    await Deno.mkdir(this.#root, { recursive: true, mode: 0o700 });
    await Deno.mkdir(this.#terminals, { recursive: true, mode: 0o700 });
    await Deno.chmod(this.#root, 0o700);
    await Deno.chmod(this.#terminals, 0o700);
  }
}
