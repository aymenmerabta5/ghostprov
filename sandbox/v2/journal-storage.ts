import type {
  JournalRecord,
  JournalStorage,
  JournalTerminalReader,
} from "../../src/v2/sidecar/journal.ts";
import {
  JournalStorageCapacityError,
  JournalStorageDurabilityError,
  V2_JOURNAL_MAX_BYTES,
  V2_JOURNAL_MAX_RECORDS,
  journalStoredRecordBytes,
} from "../../src/v2/sidecar/journal.ts";
import {
  canonicalJson,
  sha256Hex,
  utf8Bytes,
  utf8Text,
} from "../../src/v2/protocol/shared.ts";
import type {
  JournalTerminalSpool,
  TerminalSpoolProof,
} from "../../src/v2/sidecar/journal.ts";
import {
  V2_TERMINAL_SPOOL_CAPACITY,
  V2_TERMINAL_SPOOL_RESERVATION_BYTES,
  V2_TERMINAL_SPOOL_TOTAL_BYTES,
} from "../../src/v2/sidecar/capacity.ts";

interface StoredRecord {
  record: JournalRecord;
  checksum: string;
}

const SAFE_REQUEST = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const JOURNAL_TEMP = /^claims\.jsonl\.[1-9][0-9]{0,19}\.[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\.tmp$/i;
const MAX_JOURNAL_TEMP_FILES = 16;
const MAX_JOURNAL_ROOT_ENTRIES = 4_096;

type AbsoluteStoragePathKind = "posix" | "drive" | "unc";

interface AbsoluteStoragePath {
  readonly kind: AbsoluteStoragePathKind;
  /** Filesystem root: `/`, `C:/`, or `//server/share`. */
  readonly anchor: string;
  readonly components: readonly string[];
  /** Normalized path using `/`; Deno accepts this form on Windows too. */
  readonly value: string;
}

function invalidAbsoluteStoragePath(label: string): never {
  throw new TypeError(`${label} must be an unambiguous absolute filesystem path`);
}

function assertWindowsPathComponent(component: string, label: string): void {
  // Dot components and Win32 aliases (trailing dots/spaces or alternate data
  // streams) make lexical containment differ from the path the kernel opens.
  if (
    component === "." ||
    component === ".." ||
    component.includes(":") ||
    /[. ]$/.test(component)
  ) invalidAbsoluteStoragePath(label);
}

function normalizeAbsoluteStoragePath(value: string, label: string): AbsoluteStoragePath {
  if (typeof value !== "string" || value.length === 0 || value.includes("\0")) {
    invalidAbsoluteStoragePath(label);
  }

  const drive = /^([A-Za-z]):[\\/]/.exec(value);
  if (drive) {
    const components = value.slice(3).split(/[\\/]+/).filter(Boolean);
    for (const component of components) assertWindowsPathComponent(component, label);
    const anchor = `${drive[1].toUpperCase()}:/`;
    return {
      kind: "drive",
      anchor,
      components,
      value: components.length ? `${anchor}${components.join("/")}` : anchor,
    };
  }

  // Reject Win32 device/extended-length namespaces. Their normalization rules
  // can bypass ordinary drive/share containment and are unnecessary here.
  if (/^[\\/]{2}[?.][\\/]/.test(value)) invalidAbsoluteStoragePath(label);
  const unc = /^[\\/]{2}([^\\/]+)[\\/]+([^\\/]+)(?:[\\/]+|$)/.exec(value);
  if (unc) {
    const server = unc[1];
    const share = unc[2];
    assertWindowsPathComponent(server, label);
    assertWindowsPathComponent(share, label);
    const components = value.slice(unc[0].length).split(/[\\/]+/).filter(Boolean);
    for (const component of components) assertWindowsPathComponent(component, label);
    const anchor = `//${server}/${share}`;
    return {
      kind: "unc",
      anchor,
      components,
      value: components.length ? `${anchor}/${components.join("/")}` : anchor,
    };
  }

  // A double-separator prefix is either a complete UNC share or ambiguous;
  // never reinterpret an incomplete network path as a POSIX path.
  if (/^[\\/]{2}/.test(value)) invalidAbsoluteStoragePath(label);

  if (!value.startsWith("/") || value.includes("\\")) {
    invalidAbsoluteStoragePath(label);
  }
  const components = value.split(/\/+/).filter(Boolean);
  if (components.some(component => component === "." || component === "..")) {
    invalidAbsoluteStoragePath(label);
  }
  return {
    kind: "posix",
    anchor: "/",
    components,
    value: components.length ? `/${components.join("/")}` : "/",
  };
}

function assertNativeStoragePath(path: AbsoluteStoragePath, label: string): void {
  // Bun unit tests construct the adapter without performing Deno I/O. Enforce
  // native path semantics in the actual sidecar runtime while preserving that
  // constructor-only portability seam.
  const deno = (globalThis as unknown as {
    Deno?: { build?: { os?: string } };
  }).Deno;
  if (!deno?.build?.os) return;
  const native = deno.build.os === "windows"
    ? path.kind === "drive" || path.kind === "unc"
    : path.kind === "posix";
  if (!native) {
    throw new TypeError(`${label} is not an absolute path on this host`);
  }
}

function storagePathComponentKey(path: AbsoluteStoragePath, component: string): string {
  return path.kind === "posix" ? component : component.toLowerCase();
}

function storagePathDescendsFrom(
  path: AbsoluteStoragePath,
  boundary: AbsoluteStoragePath,
): boolean {
  if (path.kind !== boundary.kind) return false;
  const pathAnchor = path.kind === "posix" ? path.anchor : path.anchor.toLowerCase();
  const boundaryAnchor = boundary.kind === "posix"
    ? boundary.anchor
    : boundary.anchor.toLowerCase();
  if (pathAnchor !== boundaryAnchor || path.components.length < boundary.components.length) {
    return false;
  }
  return boundary.components.every((component, index) => (
    storagePathComponentKey(boundary, component) ===
      storagePathComponentKey(path, path.components[index])
  ));
}

function rebaseStorageDescendant(
  path: AbsoluteStoragePath,
  boundary: AbsoluteStoragePath,
): AbsoluteStoragePath {
  const components = [
    ...boundary.components,
    ...path.components.slice(boundary.components.length),
  ];
  return {
    kind: boundary.kind,
    anchor: boundary.anchor,
    components,
    value: components.length
      ? appendStoragePath(boundary.anchor, components.join("/"))
      : boundary.anchor,
  };
}

function appendStoragePath(path: string, component: string): string {
  return path.endsWith("/") ? `${path}${component}` : `${path}/${component}`;
}

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
  /**
   * Lifecycle-provisioned real directory from which every storage component
   * descends. When explicit, storage never creates it or syncs its parent;
   * confinement and journal durability begin at this capability boundary.
   */
  rootBoundary?: string;
  /** Retained for non-destructive diagnostics/backward-compatible construction. */
  assertExclusiveOwnership?: () => Promise<void>;
  /** Holds the ownership lease throughout one destructive filesystem operation. */
  withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
  maxTerminalSpoolBytes?: number;
  maxTerminalBytes?: number;
  maxActiveSpools?: number;
  maxTerminalFiles?: number;
  maxJournalBytes?: number;
  maxJournalRecords?: number;
  /** Fault-model hook; production omits this and uses a native directory fsync. */
  syncDirectory?: (path: string) => Promise<void>;
}

interface ActiveSpoolState {
  requestId: string;
  temporary: string;
  destination: string;
  file: Deno.FsFile;
  byteCount: number;
  maxBytes: number;
  state: "active" | "promoted" | "aborted";
  operationTail: Promise<void>;
  failure: unknown | null;
}

class DenoTerminalSpool implements JournalTerminalSpool {
  readonly #state: ActiveSpoolState;
  readonly #append: (state: ActiveSpoolState, bytes: Uint8Array) => Promise<void>;
  readonly #promote: (state: ActiveSpoolState, proof: TerminalSpoolProof) => Promise<void>;
  readonly #abort: (state: ActiveSpoolState) => Promise<void>;

  constructor(
    readonly requestId: string,
    readonly maxBytes: number,
    state: ActiveSpoolState,
    append: (state: ActiveSpoolState, bytes: Uint8Array) => Promise<void>,
    promote: (state: ActiveSpoolState, proof: TerminalSpoolProof) => Promise<void>,
    abort: (state: ActiveSpoolState) => Promise<void>,
  ) {
    this.#state = state;
    this.#append = append;
    this.#promote = promote;
    this.#abort = abort;
  }

  get byteCount(): number { return this.#state.byteCount; }
  append(bytes: Uint8Array): Promise<void> { return this.#append(this.#state, bytes); }
  promote(proof: TerminalSpoolProof): Promise<void> { return this.#promote(this.#state, proof); }
  abort(): Promise<void> { return this.#abort(this.#state); }
}

const TERMINAL_READ_CHUNK_BYTES = 64 * 1024;

class DenoTerminalReader implements JournalTerminalReader {
  #closed = false;
  #reading = false;
  readonly #file: Deno.FsFile;

  constructor(
    readonly requestId: string,
    readonly byteCount: number,
    file: Deno.FsFile,
  ) { this.#file = file; }

  async *chunks(signal?: AbortSignal): AsyncIterable<Uint8Array> {
    if (this.#closed) throw new Error("terminal reader is closed");
    if (this.#reading) throw new Error("terminal reader is already active");
    this.#reading = true;
    let total = 0;
    try {
      while (total < this.byteCount) {
        if (signal?.aborted) throw signal.reason ?? new DOMException("aborted", "AbortError");
        const bytes = new Uint8Array(Math.min(
          TERMINAL_READ_CHUNK_BYTES,
          this.byteCount - total,
        ));
        let offset = 0;
        while (offset < bytes.byteLength) {
          const read = await this.#file.read(bytes.subarray(offset));
          if (read === null) throw new Error("terminal spool was truncated during recovery");
          if (read < 1) throw new Error("terminal spool read made no progress");
          offset += read;
        }
        total += bytes.byteLength;
        yield bytes;
      }
      const extra = new Uint8Array(1);
      if (await this.#file.read(extra) !== null) {
        throw new Error("terminal spool grew during recovery");
      }
    } finally {
      this.#reading = false;
    }
  }

  async rewind(): Promise<void> {
    if (this.#closed) throw new Error("terminal reader is closed");
    if (this.#reading) throw new Error("terminal reader is active");
    await this.#file.seek(0, Deno.SeekMode.Start);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#file.close();
  }
}

export class DenoJournalStorage implements JournalStorage {
  readonly #root: string;
  readonly #rootBoundary: string;
  readonly #rootPath: AbsoluteStoragePath;
  readonly #rootBoundaryPath: AbsoluteStoragePath;
  readonly #explicitRootBoundary: boolean;
  readonly #journal: string;
  readonly #terminals: string;
  readonly #withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
  readonly #maxTerminalSpoolBytes: number;
  readonly #maxTerminalBytes: number;
  readonly #maxActiveSpools: number;
  readonly #maxTerminalFiles: number;
  readonly #maxJournalBytes: number;
  readonly #maxJournalRecords: number;
  readonly #syncDirectoryOverride?: (path: string) => Promise<void>;
  readonly replaceRecords?: (records: readonly JournalRecord[]) => Promise<void>;
  readonly pruneTerminals?: (retainedRequestIds: readonly string[]) => Promise<void>;
  #terminalBytes: number | null = null;
  #terminalFiles: number | null = null;
  #activeSpools = 0;
  #activeSpoolBytes = 0;
  #reservedSpoolBytes = 0;
  readonly #activeSpoolIds = new Set<string>();
  #journalBytes: number | null = null;
  #journalRecords: number | null = null;
  #directoriesDurable = false;

  constructor(options: string | DenoJournalStorageOptions = {}) {
    const root = typeof options === "string"
      ? options
      : options.root ?? "/tmp/ghostprov-v2";
    const rootPath = normalizeAbsoluteStoragePath(root, "journal root");
    const explicitRootBoundary = typeof options !== "string" && options.rootBoundary !== undefined;
    const rootBoundaryPath = normalizeAbsoluteStoragePath(
      explicitRootBoundary ? options.rootBoundary! : rootPath.value,
      "journal rootBoundary",
    );
    assertNativeStoragePath(rootPath, "journal root");
    assertNativeStoragePath(rootBoundaryPath, "journal rootBoundary");
    if (!storagePathDescendsFrom(rootPath, rootBoundaryPath)) {
      throw new TypeError("journal root must descend from its absolute rootBoundary");
    }
    // The explicit boundary spelling is authoritative. On Windows this avoids
    // validating a case-insensitive prefix and later opening a differently
    // cased path on a case-sensitive directory/volume.
    const confinedRootPath = explicitRootBoundary
      ? rebaseStorageDescendant(rootPath, rootBoundaryPath)
      : rootPath;
    this.#rootPath = confinedRootPath;
    this.#rootBoundaryPath = rootBoundaryPath;
    this.#explicitRootBoundary = explicitRootBoundary;
    this.#root = confinedRootPath.value;
    this.#rootBoundary = rootBoundaryPath.value;
    this.#journal = appendStoragePath(this.#root, "claims.jsonl");
    this.#terminals = appendStoragePath(this.#root, "terminals");
    const assertExclusiveOwnership = typeof options === "string"
      ? undefined
      : options.assertExclusiveOwnership;
    if (
      assertExclusiveOwnership !== undefined &&
      typeof assertExclusiveOwnership !== "function"
    ) throw new TypeError("assertExclusiveOwnership must be a function");
    const syncDirectoryOverride = typeof options === "string"
      ? undefined
      : options.syncDirectory;
    if (syncDirectoryOverride !== undefined && typeof syncDirectoryOverride !== "function") {
      throw new TypeError("syncDirectory must be a function");
    }
    const withExclusiveOwnership = typeof options === "string"
      ? undefined
      : options.withExclusiveOwnership;
    if (
      withExclusiveOwnership !== undefined &&
      typeof withExclusiveOwnership !== "function"
    ) throw new TypeError("withExclusiveOwnership must be a function");
    this.#withExclusiveOwnership = withExclusiveOwnership;
    this.#maxTerminalSpoolBytes = typeof options === "string"
      ? V2_TERMINAL_SPOOL_RESERVATION_BYTES
      : options.maxTerminalSpoolBytes ?? V2_TERMINAL_SPOOL_RESERVATION_BYTES;
    this.#maxTerminalBytes = typeof options === "string"
      ? V2_TERMINAL_SPOOL_TOTAL_BYTES
      : options.maxTerminalBytes ?? V2_TERMINAL_SPOOL_TOTAL_BYTES;
    this.#maxActiveSpools = typeof options === "string"
      ? V2_TERMINAL_SPOOL_CAPACITY
      : options.maxActiveSpools ?? V2_TERMINAL_SPOOL_CAPACITY;
    this.#maxTerminalFiles = typeof options === "string"
      ? 4_096
      : options.maxTerminalFiles ?? 4_096;
    this.#maxJournalBytes = typeof options === "string"
      ? V2_JOURNAL_MAX_BYTES
      : options.maxJournalBytes ?? V2_JOURNAL_MAX_BYTES;
    this.#maxJournalRecords = typeof options === "string"
      ? V2_JOURNAL_MAX_RECORDS
      : options.maxJournalRecords ?? V2_JOURNAL_MAX_RECORDS;
    this.#syncDirectoryOverride = syncDirectoryOverride;
    if (
      !Number.isSafeInteger(this.#maxTerminalSpoolBytes) ||
      this.#maxTerminalSpoolBytes < 1 ||
      !Number.isSafeInteger(this.#maxTerminalBytes) ||
      this.#maxTerminalBytes < this.#maxTerminalSpoolBytes ||
      !Number.isSafeInteger(this.#maxActiveSpools) ||
      this.#maxActiveSpools < 1 ||
      this.#maxActiveSpools > 1_024 ||
      !Number.isSafeInteger(this.#maxTerminalFiles) ||
      this.#maxTerminalFiles < this.#maxActiveSpools ||
      !Number.isSafeInteger(this.#maxJournalBytes) ||
      this.#maxJournalBytes < 1 ||
      !Number.isSafeInteger(this.#maxJournalRecords) ||
      this.#maxJournalRecords < 1
    ) throw new RangeError("invalid terminal spool limits");
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
    await this.#ensureJournalStats();
    if (
      this.#journalBytes! + bytes.byteLength > this.#maxJournalBytes ||
      this.#journalRecords! + records.length > this.#maxJournalRecords
    ) throw new JournalStorageCapacityError("journal runtime capacity is exhausted");
    let created = false;
    const emptyJournal = this.#journalBytes === 0 && this.#journalRecords === 0;
    let file: Deno.FsFile;
    try {
      file = await Deno.open(this.#journal, { append: true, write: true });
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
      file = await Deno.open(this.#journal, {
        createNew: true,
        append: true,
        write: true,
        mode: 0o600,
      });
      created = true;
    }
    let durable = false;
    try {
      await writeAll(file, bytes);
      await file.syncData();
      if (created || emptyJournal) await this.#syncDirectory(this.#root, true);
      durable = true;
    } finally {
      try { file.close(); } catch (error) { if (!durable) throw error; }
    }
    // Permission refresh is hygiene after the fsync durability boundary. It
    // must not report a false append failure that could delete a valid spool.
    this.#journalBytes! += bytes.byteLength;
    this.#journalRecords! += records.length;
    await Deno.chmod(this.#journal, 0o600).catch(() => undefined);
  }

  async beginTerminalSpool(requestId: string, maxBytes: number): Promise<JournalTerminalSpool> {
    if (!SAFE_REQUEST.test(requestId)) throw new Error("invalid terminal request ID");
    if (
      !Number.isSafeInteger(maxBytes) ||
      maxBytes < 1 ||
      maxBytes > this.#maxTerminalSpoolBytes
    ) throw new Error("terminal spool byte limit is invalid");
    return this.#owned(async () => {
      await this.#ensureDirectories();
      await this.#ensureTerminalStats();
      if (this.#activeSpools >= this.#maxActiveSpools) {
        throw new JournalStorageCapacityError("terminal spool concurrency is exhausted");
      }
      if (this.#activeSpoolIds.has(requestId)) {
        throw new Error("terminal spool already exists for request");
      }
      if (
        this.#terminalFiles! >= this.#maxTerminalFiles ||
        this.#terminalBytes! + this.#reservedSpoolBytes + maxBytes >
          this.#maxTerminalBytes
      ) {
        throw new JournalStorageCapacityError("terminal spool capacity is exhausted");
      }
      const destination = `${this.#terminals}/${terminalName(requestId)}`;
      const temporary = `${destination}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
      const file = await Deno.open(temporary, {
        createNew: true,
        write: true,
        mode: 0o600,
      });
      const state: ActiveSpoolState = {
        requestId,
        temporary,
        destination,
        file,
        byteCount: 0,
        maxBytes,
        state: "active",
        operationTail: Promise.resolve(),
        failure: null,
      };
      this.#activeSpools++;
      this.#terminalFiles = this.#terminalFiles! + 1;
      this.#reservedSpoolBytes += maxBytes;
      this.#activeSpoolIds.add(requestId);
      return new DenoTerminalSpool(
        requestId,
        maxBytes,
        state,
        (value, bytes) => this.#queueSpool(value, () => this.#appendSpool(value, bytes)),
        (value, proof) => this.#queueSpool(
          value,
          () => this.#owned(() => this.#promoteSpool(value, proof)),
        ),
        value => this.#queueSpool(value, () => this.#abortSpool(value)),
      );
    });
  }

  async #appendSpool(state: ActiveSpoolState, bytes: Uint8Array): Promise<void> {
    if (state.state !== "active") throw new Error("terminal spool is closed");
    if (state.failure) throw state.failure;
    if (!(bytes instanceof Uint8Array) || bytes.byteLength < 1) {
      throw new Error("terminal spool append is empty");
    }
    const next = state.byteCount + bytes.byteLength;
    if (next > state.maxBytes) throw new Error("terminal spool quota is exhausted");
    try {
      await writeAll(state.file, bytes);
    } catch (error) {
      state.failure = error;
      throw error;
    }
    state.byteCount = next;
    this.#activeSpoolBytes += bytes.byteLength;
  }

  async #promoteSpool(state: ActiveSpoolState, proof: TerminalSpoolProof): Promise<void> {
    if (state.state !== "active") throw new Error("terminal spool is closed");
    if (state.failure) throw state.failure;
    if (
      !Number.isSafeInteger(proof.byteCount) ||
      proof.byteCount !== state.byteCount ||
      !/^[a-f0-9]{64}$/.test(proof.transcriptSha256)
    ) throw new Error("terminal spool proof does not match");
    await state.file.syncData();
    state.file.close();
    let replacedBytes = 0;
    try {
      const existing = await Deno.stat(state.destination);
      if (existing.isFile) replacedBytes = existing.size;
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    }
    await Deno.rename(state.temporary, state.destination);
    state.state = "promoted";
    this.#activeSpools--;
    this.#activeSpoolIds.delete(state.requestId);
    this.#activeSpoolBytes -= state.byteCount;
    this.#reservedSpoolBytes -= state.maxBytes;
    this.#terminalBytes = Math.max(0, this.#terminalBytes! - replacedBytes) + state.byteCount;
    if (replacedBytes > 0) this.#terminalFiles = Math.max(0, this.#terminalFiles! - 1);
    await Deno.chmod(state.destination, 0o600).catch(() => undefined);
    await this.#syncDirectory(this.#terminals, true);
  }

  async #abortSpool(state: ActiveSpoolState): Promise<void> {
    if (state.state !== "active") return;
    state.state = "aborted";
    this.#activeSpools--;
    this.#activeSpoolIds.delete(state.requestId);
    this.#activeSpoolBytes -= state.byteCount;
    this.#reservedSpoolBytes -= state.maxBytes;
    try { state.file.close(); } catch {}
    let removed = false;
    await Deno.remove(state.temporary).then(() => { removed = true; }).catch(error => {
      if (error instanceof Deno.errors.NotFound) { removed = true; return; }
      throw error;
    });
    if (removed) this.#terminalFiles = Math.max(0, this.#terminalFiles! - 1);
  }

  async #replaceRecords(records: readonly JournalRecord[]): Promise<void> {
    await this.#ensureDirectories();
    const encoded = await this.#encodeRecords(records);
    if (
      encoded.byteLength > this.#maxJournalBytes ||
      records.length > this.#maxJournalRecords
    ) throw new JournalStorageCapacityError("journal replacement capacity is exhausted");
    const temporary = `${this.#journal}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
    const file = await Deno.open(temporary, {
      createNew: true,
      write: true,
      mode: 0o600,
    });
    let renamed = false;
    try {
      await writeAll(file, encoded);
      await file.syncData();
      file.close();
      await Deno.rename(temporary, this.#journal);
      renamed = true;
      await Deno.chmod(this.#journal, 0o600).catch(() => undefined);
      await this.#syncDirectory(this.#root, true);
      this.#journalBytes = encoded.byteLength;
      this.#journalRecords = records.length;
    } finally {
      try { file.close(); } catch {}
      if (!renamed) await Deno.remove(temporary).catch(() => undefined);
    }
  }

  async writeTerminal(requestId: string, bytes: Uint8Array): Promise<void> {
    if (bytes.byteLength < 1 || bytes.byteLength > this.#maxTerminalSpoolBytes) {
      throw new Error("terminal spool exceeds its byte limit");
    }
    await this.#ensureDirectories();
    await this.#ensureTerminalStats();
    const destination = `${this.#terminals}/${terminalName(requestId)}`;
    let replacedBytes = 0;
    try {
      const existing = await Deno.stat(destination);
      if (existing.isFile) replacedBytes = existing.size;
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    }
    if (
      this.#terminalBytes! - replacedBytes + this.#reservedSpoolBytes + bytes.byteLength >
      this.#maxTerminalBytes
    ) throw new JournalStorageCapacityError("terminal spool quota is exhausted");
    if (replacedBytes === 0 && this.#terminalFiles! >= this.#maxTerminalFiles) {
      throw new JournalStorageCapacityError("terminal spool file quota is exhausted");
    }
    const temporary = `${destination}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
    const file = await Deno.open(temporary, {
      createNew: true,
      write: true,
      mode: 0o600,
    });
    this.#terminalFiles = this.#terminalFiles! + 1;
    let renamed = false;
    try {
      await writeAll(file, bytes);
      await file.syncData();
      file.close();
      await Deno.rename(temporary, destination);
      renamed = true;
      if (replacedBytes > 0) this.#terminalFiles = Math.max(0, this.#terminalFiles! - 1);
      this.#terminalBytes = Math.max(0, this.#terminalBytes! - replacedBytes) + bytes.byteLength;
      await Deno.chmod(destination, 0o600).catch(() => undefined);
      await this.#syncDirectory(this.#terminals, true);
    } finally {
      try { file.close(); } catch {}
      if (!renamed) {
        await Deno.remove(temporary).catch(() => undefined);
        this.#terminalFiles = Math.max(0, this.#terminalFiles! - 1);
      }
    }
  }

  async readTerminal(
    requestId: string,
    maxBytes = this.#maxTerminalSpoolBytes,
  ): Promise<Uint8Array | null> {
    const file = `${this.#terminals}/${terminalName(requestId)}`;
    try {
      const stat = await Deno.stat(file);
      if (!stat.isFile) return null;
      if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || stat.size > maxBytes) {
        throw new Error("terminal spool exceeds recovery byte limit");
      }
      const bytes = await Deno.readFile(file);
      if (bytes.byteLength > maxBytes) {
        bytes.fill(0);
        throw new Error("terminal spool exceeds recovery byte limit");
      }
      return bytes;
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return null;
      throw error;
    }
  }

  async openTerminal(
    requestId: string,
    maxBytes = this.#maxTerminalSpoolBytes,
  ): Promise<JournalTerminalReader | null> {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) {
      throw new Error("terminal spool recovery limit is invalid");
    }
    return this.#owned(async () => {
      const path = `${this.#terminals}/${terminalName(requestId)}`;
      let file: Deno.FsFile;
      try {
        file = await Deno.open(path, { read: true });
      } catch (error) {
        if (error instanceof Deno.errors.NotFound) return null;
        throw error;
      }
      try {
        const stat = await file.stat();
        if (!stat.isFile || stat.size < 1 || stat.size > maxBytes) {
          throw new Error("terminal spool exceeds recovery byte limit");
        }
        return new DenoTerminalReader(requestId, stat.size, file);
      } catch (error) {
        file.close();
        throw error;
      }
    });
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
      const size = this.#terminalBytes === null
        ? 0
        : (await Deno.stat(file)).size;
      await Deno.remove(file);
      if (this.#terminalBytes !== null) {
        this.#terminalBytes = Math.max(0, this.#terminalBytes - size);
      }
      if (this.#terminalFiles !== null) {
        this.#terminalFiles = Math.max(0, this.#terminalFiles - 1);
      }
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
        const path = `${this.#terminals}/${entry.name}`;
        const size = this.#terminalBytes !== null && (isTerminal || isTemporary)
          ? (await Deno.stat(path)).size
          : 0;
        await Deno.remove(path).catch(error => {
          if (!(error instanceof Deno.errors.NotFound)) throw error;
        });
        if (this.#terminalBytes !== null && (isTerminal || isTemporary)) {
          this.#terminalBytes = Math.max(0, this.#terminalBytes - size);
        }
        if (this.#terminalFiles !== null && (isTerminal || isTemporary)) {
          this.#terminalFiles = Math.max(0, this.#terminalFiles - 1);
        }
      }
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return;
      throw error;
    }
    this.#terminalBytes = null;
    this.#terminalFiles = null;
    await this.#ensureTerminalStats();
  }

  async readRecords(): Promise<readonly JournalRecord[]> {
    if (this.#withExclusiveOwnership) {
      await this.#owned(async () => {
        await this.#ensureDirectories();
        await this.#cleanupJournalTemps();
      });
    }
    let rawBytes: Uint8Array;
    let storedBytes = 0;
    try {
      const stat = await Deno.stat(this.#journal);
      if (!stat.isFile || stat.size > this.#maxJournalBytes) {
        throw new JournalStorageCapacityError("journal startup byte limit is exceeded");
      }
      storedBytes = stat.size;
      rawBytes = await Deno.readFile(this.#journal);
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) {
        this.#journalBytes = 0;
        this.#journalRecords = 0;
        return [];
      }
      throw error;
    }
    const endedCleanly = rawBytes.length === 0 || rawBytes[rawBytes.length - 1] === 0x0a;
    let completeEnd = rawBytes.length;
    if (!endedCleanly) {
      completeEnd = 0;
      for (let index = rawBytes.length - 1; index >= 0; index--) {
        if (rawBytes[index] !== 0x0a) continue;
        completeEnd = index + 1;
        break;
      }
    }
    let raw: string;
    try {
      raw = utf8Text(rawBytes.subarray(0, completeEnd));
    } catch (error) {
      throw new Error("journal corruption before trailing append", { cause: error });
    }
    const lines = raw.split("\n");
    if (lines.at(-1) === "") lines.pop();
    const records: JournalRecord[] = [];
    for (let index = 0; index < lines.length; index++) {
      if (records.length >= this.#maxJournalRecords) {
        throw new JournalStorageCapacityError("journal startup record limit is exceeded");
      }
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
    } else {
      this.#journalBytes = storedBytes;
      this.#journalRecords = records.length;
    }
    return records;
  }

  async #encodeRecords(records: readonly JournalRecord[]): Promise<Uint8Array> {
    const lines: string[] = [];
    let expectedBytes = 0;
    for (const record of records) {
      const stored: StoredRecord = {
        record,
        checksum: await sha256Hex(canonicalJson(record)),
      };
      lines.push(`${JSON.stringify(stored)}\n`);
      expectedBytes += journalStoredRecordBytes(record);
    }
    const encoded = utf8Bytes(lines.join(""));
    if (encoded.byteLength !== expectedBytes) {
      throw new Error("journal encoded byte accounting diverged");
    }
    return encoded;
  }

  #queueSpool(
    state: ActiveSpoolState,
    operation: () => Promise<void>,
  ): Promise<void> {
    const next = state.operationTail.catch(() => undefined).then(operation);
    state.operationTail = next.catch(() => undefined);
    return next;
  }

  #owned<T>(operation: () => Promise<T>): Promise<T> {
    if (!this.#withExclusiveOwnership) {
      return Promise.reject(new Error("terminal spool requires exclusive sidecar ownership"));
    }
    return this.#withExclusiveOwnership(operation);
  }

  async #ensureJournalStats(): Promise<void> {
    if (this.#journalBytes !== null && this.#journalRecords !== null) return;
    await this.readRecords();
  }

  async #cleanupJournalTemps(): Promise<void> {
    const candidates: Array<{ path: string; bytes: number }> = [];
    let entries = 0;
    let totalBytes = 0;
    const totalByteLimit = Math.min(Number.MAX_SAFE_INTEGER, this.#maxJournalBytes * 4);
    for await (const entry of Deno.readDir(this.#root)) {
      if (++entries > MAX_JOURNAL_ROOT_ENTRIES) {
        throw new JournalStorageCapacityError("journal root entry capacity is exceeded");
      }
      const looksTemporary = entry.name.startsWith("claims.jsonl.") && entry.name.endsWith(".tmp");
      if (!looksTemporary) continue;
      if (!JOURNAL_TEMP.test(entry.name) || !entry.isFile || entry.isSymlink) {
        throw new JournalStorageCapacityError("unexpected journal rewrite temporary entry");
      }
      if (candidates.length >= MAX_JOURNAL_TEMP_FILES) {
        throw new JournalStorageCapacityError("journal rewrite temporary count is exceeded");
      }
      const path = `${this.#root}/${entry.name}`;
      const stat = await Deno.lstat(path);
      if (!stat.isFile || stat.isSymlink || stat.size > this.#maxJournalBytes) {
        throw new JournalStorageCapacityError("journal rewrite temporary byte quota is exceeded");
      }
      totalBytes += stat.size;
      if (
        !Number.isSafeInteger(totalBytes) ||
        totalBytes > totalByteLimit
      ) throw new JournalStorageCapacityError("journal rewrite temporary byte quota is exceeded");
      candidates.push({ path, bytes: stat.size });
    }
    if (!candidates.length) return;
    for (const candidate of candidates) await Deno.remove(candidate.path);
    await this.#syncDirectory(this.#root, true);
  }

  async #ensureTerminalStats(): Promise<void> {
    if (this.#terminalBytes !== null && this.#terminalFiles !== null) return;
    let total = 0;
    let files = 0;
    try {
      for await (const entry of Deno.readDir(this.#terminals)) {
        if (
          !entry.isFile ||
          (!entry.name.endsWith(".json") && !entry.name.endsWith(".tmp"))
        ) continue;
        const stat = await Deno.stat(`${this.#terminals}/${entry.name}`);
        if (stat.isFile) {
          total += stat.size;
          files++;
        }
        if (total > this.#maxTerminalBytes) {
          throw new JournalStorageCapacityError("terminal spool disk quota is already exceeded");
        }
        if (files > this.#maxTerminalFiles) {
          throw new JournalStorageCapacityError("terminal spool file quota is already exceeded");
        }
      }
    } catch (error) {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    }
    this.#terminalBytes = total;
    this.#terminalFiles = files;
  }

  async #syncDirectory(path: string, required = false): Promise<void> {
    // Linux requires syncing the parent directory for rename durability. Some
    // filesystems reject directory fsync; the file itself is already synced in
    // that case and the next startup still validates the complete checksum log.
    try {
      if (this.#syncDirectoryOverride) {
        await this.#syncDirectoryOverride(path);
        return;
      }
      const directory = await Deno.open(path, { read: true });
      try { await directory.sync(); } finally { directory.close(); }
    } catch (error) {
      if (required && (this.#syncDirectoryOverride || Deno.build.os !== "windows")) {
        throw new JournalStorageDurabilityError(
          `Required journal directory durability fence failed: ${
            error instanceof Error ? error.message : "unknown sync error"
          }`,
          { cause: error },
        );
      }
    }
  }

  async #requireRealDirectory(path: string, message: string): Promise<void> {
    const info = await Deno.lstat(path);
    if (!info.isDirectory || info.isSymlink) throw new Error(message);
  }

  async #ensureDirectoryHierarchy(path: AbsoluteStoragePath): Promise<void> {
    if (!storagePathDescendsFrom(path, this.#rootBoundaryPath)) {
      throw new Error("journal storage path escaped its root boundary");
    }

    let current: string;
    let relative: readonly string[];
    if (this.#explicitRootBoundary) {
      // The ownership phase provisions this capability boundary. Journal
      // storage must neither create it recursively nor touch/sync its parent:
      // confinement and journal durability begin at this directory.
      await this.#requireRealDirectory(
        this.#rootBoundary,
        "journal root boundary must be a pre-existing real directory",
      );
      current = this.#rootBoundary;
      relative = path.components.slice(this.#rootBoundaryPath.components.length);
    } else {
      // Backward-compatible root-only construction may create a missing root,
      // but still validates every existing component from the filesystem
      // anchor so a symlink cannot redirect the recursive creation.
      await this.#requireRealDirectory(
        path.anchor,
        "journal filesystem anchor must be a real directory",
      );
      current = path.anchor;
      relative = path.components;
    }

    for (const component of relative) {
      const parent = current;
      current = appendStoragePath(current, component);
      let created = false;
      try {
        await this.#requireRealDirectory(
          current,
          "journal storage hierarchy must contain only real directories",
        );
      } catch (error) {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
        try {
          await Deno.mkdir(current, { mode: 0o700 });
          created = true;
        } catch (mkdirError) {
          // Concurrent first-use in this process may publish the same child
          // after our lstat. Validate that winner below instead of failing.
          if (!(mkdirError instanceof Deno.errors.AlreadyExists)) throw mkdirError;
        }
        await this.#requireRealDirectory(
          current,
          "journal storage hierarchy must contain only real directories",
        );
      }
      // An explicit capability fences every descendant parent once during
      // initialization: another phase could have created a pre-existing child
      // earlier in this boot without syncing it. Legacy traversal only fences
      // entries that it creates, avoiding fsyncs of unrelated host ancestors.
      if (this.#explicitRootBoundary || created) {
        await this.#syncDirectory(parent, true);
      }
    }
  }

  async #ensureDirectories(): Promise<void> {
    if (!this.#directoriesDurable) {
      await this.#ensureDirectoryHierarchy(this.#rootPath);
      await this.#ensureDirectoryHierarchy(normalizeAbsoluteStoragePath(
        this.#terminals,
        "journal terminal root",
      ));
    }
    const [rootInfo, terminalInfo] = await Promise.all([
      Deno.lstat(this.#root),
      Deno.lstat(this.#terminals),
    ]);
    if (
      !rootInfo.isDirectory || rootInfo.isSymlink ||
      !terminalInfo.isDirectory || terminalInfo.isSymlink
    ) throw new Error("journal storage directories must not be symbolic links");
    await Deno.chmod(this.#root, 0o700);
    await Deno.chmod(this.#terminals, 0o700);
    this.#directoriesDurable = true;
  }
}
