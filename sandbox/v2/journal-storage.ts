import type {
  JournalRecord,
  JournalStorage,
} from "./journal.ts";
import {
  canonicalJson,
  sha256Hex,
  utf8Bytes,
} from "./shared.ts";

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

export class DenoJournalStorage implements JournalStorage {
  readonly #root: string;
  readonly #journal: string;
  readonly #terminals: string;

  constructor(root = "/tmp/ghostprov-v2") {
    this.#root = root.replace(/\/+$/, "");
    this.#journal = `${this.#root}/claims.jsonl`;
    this.#terminals = `${this.#root}/terminals`;
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    if (!records.length) return;
    await this.#ensureDirectories();
    const lines: string[] = [];
    for (const record of records) {
      const stored: StoredRecord = {
        record,
        checksum: await sha256Hex(canonicalJson(record)),
      };
      lines.push(`${JSON.stringify(stored)}\n`);
    }
    const file = await Deno.open(this.#journal, {
      create: true,
      append: true,
      write: true,
      mode: 0o600,
    });
    try {
      await writeAll(file, utf8Bytes(lines.join("")));
      await file.syncData();
    } finally {
      file.close();
    }
    await Deno.chmod(this.#journal, 0o600);
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
      await Deno.chmod(destination, 0o600);
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

  async deleteTerminal(requestId: string): Promise<void> {
    const file = `${this.#terminals}/${terminalName(requestId)}`;
    try {
      await Deno.remove(file);
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
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
    const endedCleanly = raw.endsWith("\n");
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
    return records;
  }

  async #ensureDirectories(): Promise<void> {
    await Deno.mkdir(this.#root, { recursive: true, mode: 0o700 });
    await Deno.mkdir(this.#terminals, { recursive: true, mode: 0o700 });
    await Deno.chmod(this.#root, 0o700);
    await Deno.chmod(this.#terminals, 0o700);
  }
}
