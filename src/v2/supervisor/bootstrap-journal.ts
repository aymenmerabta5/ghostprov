import { lstat, mkdir, open, rename, unlink } from "node:fs/promises";
import * as path from "node:path";
import { FileLease } from "../../core/file-lease";
import { validateBootstrapBinding, type BootstrapBinding } from "../bootstrap/crypto";
import { canonicalJson } from "../protocol/shared";
import type { SealedSupervisorSessionBootstrap } from "./session-bootstrap";
import { validateDirectSupervisorExecAcceptance, type DirectSupervisorExecStarted } from "../../prism/direct-exec";

const MAX_BYTES = 16 * 1024;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;

/** Private local recovery material. Never emit this record into diagnostics. */
export interface SupervisorBootstrapAttempt {
  readonly version: 1;
  readonly phase: "prepared" | "submitted" | "cleanup_proven";
  readonly sessionId: string;
  readonly binding: BootstrapBinding;
  readonly connectTimeoutMs: number;
  readonly sealed: SealedSupervisorSessionBootstrap;
  /** Optional for legacy records; omission is unknown acceptance, never proof of no execution. */
  readonly commandAcceptance?: DirectSupervisorExecStarted;
}

export interface BootstrapJournalEntry {
    load(): Promise<SupervisorBootstrapAttempt | null>;
    save(attempt: SupervisorBootstrapAttempt): Promise<void>;
    clear(): Promise<void>;
}

export interface SupervisorBootstrapJournal {
  withLease<T>(signal: AbortSignal | undefined, operation: (entry: BootstrapJournalEntry) => Promise<T>): Promise<T>;
}

function parse(raw: string, project: string): SupervisorBootstrapAttempt {
  if (Buffer.byteLength(raw) > MAX_BYTES) throw new Error("Supervisor bootstrap journal exceeds its bound");
  return validateSupervisorBootstrapAttempt(JSON.parse(raw), project);
}

/** Pure structural validation shared by persisted journals and read-only recovery assessment. */
export function validateSupervisorBootstrapAttempt(value: unknown, expectedProject?: string): SupervisorBootstrapAttempt {
  const v = value as SupervisorBootstrapAttempt;
  if (!v || typeof v !== "object" || Array.isArray(v) ||
    Object.keys(v).filter(key => key !== "commandAcceptance").sort().join(",") !== "binding,connectTimeoutMs,phase,sealed,sessionId,version" ||
    v.version !== 1 || !["prepared", "submitted", "cleanup_proven"].includes(v.phase) ||
    typeof v.sessionId !== "string" || !UUID.test(v.sessionId) ||
    !Number.isSafeInteger(v.connectTimeoutMs) || v.connectTimeoutMs < 1 || v.connectTimeoutMs > 300_000) {
    throw new Error("Supervisor bootstrap journal is invalid");
  }
  const binding = validateBootstrapBinding(v.binding);
  if ((expectedProject !== undefined && binding.projectId !== expectedProject) || !v.sealed || typeof v.sealed !== "object" ||
    canonicalJson(validateBootstrapBinding(v.sealed.binding)) !== canonicalJson(binding)) {
    throw new Error("Supervisor bootstrap journal binding does not match");
  }
  if (Object.hasOwn(v, "commandAcceptance")) {
    if (v.phase === "prepared") throw new Error("Prepared bootstrap cannot contain command acceptance");
    validateDirectSupervisorExecAcceptance(v.commandAcceptance, v.sessionId);
  }
  return v;
}

async function syncDirectory(directory: string): Promise<void> {
  // Match the manifest's durability contract on platforms with directory fsync.
  if (process.platform === "win32") return;
  const handle = await open(directory, "r");
  try { await handle.sync(); } finally { await handle.close(); }
}

/** One bounded pending attempt per manifest project, serialized across processes. */
export class FileSupervisorBootstrapJournal implements SupervisorBootstrapJournal {
  readonly #file: string;
  readonly #project: string;

  constructor(directory: string, project: string) {
    if (!UUID.test(project)) throw new TypeError("Invalid bootstrap journal project");
    this.#project = project;
    this.#file = path.join(path.resolve(directory), `${project}.json`);
  }

  async withLease<T>(signal: AbortSignal | undefined, operation: (entry: BootstrapJournalEntry) => Promise<T>): Promise<T> {
    await mkdir(path.dirname(this.#file), { recursive: true, mode: 0o700 });
    const lease = new FileLease({ file: `${this.#file}.lock`, staleAfterMs: 120_000 });
    await lease.acquire({ timeoutMs: 15_000, signal });
    try {
      return await operation({
        load: async () => {
          let handle;
          try {
            const metadata = await lstat(this.#file);
            if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error("Supervisor bootstrap journal must be a regular file");
            handle = await open(this.#file, "r");
          }
          catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
          try {
            if ((await handle.stat()).size > MAX_BYTES) throw new Error("Supervisor bootstrap journal exceeds its bound");
            const buffer = Buffer.alloc(MAX_BYTES + 1);
            const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
            if (bytesRead > MAX_BYTES) throw new Error("Supervisor bootstrap journal exceeds its bound");
            return parse(buffer.subarray(0, bytesRead).toString("utf8"), this.#project);
          } finally { await handle.close(); }
        },
        save: async attempt => {
          const content = JSON.stringify(attempt);
          parse(content, this.#project);
          const temporary = `${this.#file}.${crypto.randomUUID()}.tmp`;
          try {
            const handle = await open(temporary, "wx", 0o600);
            try { await handle.writeFile(content); await handle.sync(); }
            finally { await handle.close(); }
            await rename(temporary, this.#file);
            await syncDirectory(path.dirname(this.#file));
          } finally { await unlink(temporary).catch(error => { if (error.code !== "ENOENT") throw error; }); }
        },
        clear: async () => {
          await unlink(this.#file).catch(error => { if (error.code !== "ENOENT") throw error; });
          await syncDirectory(path.dirname(this.#file));
        },
      });
    } finally { await lease.release(); }
  }
}
