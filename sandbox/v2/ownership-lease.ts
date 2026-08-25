import { utf8Bytes, utf8Text } from "../../src/v2/protocol/shared.ts";

const SAFE_INSTANCE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

export class SidecarOwnershipError extends Error {
  readonly name = "SidecarOwnershipError";

  constructor(
    message: string,
    readonly code: "contended" | "lost" | "storage",
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface SidecarOwnershipProof {
  readonly instanceId: string;
  readonly nonce: string;
  assertOwned(): Promise<void>;
  withOwnership<T>(operation: () => Promise<T>): Promise<T>;
}

export interface SidecarOwnershipHandle {
  tryLockExclusive(): Promise<boolean>;
  replace(bytes: Uint8Array): Promise<void>;
  read(): Promise<Uint8Array>;
  pathStillMatches(): Promise<boolean>;
  unlock(): Promise<void>;
  close(): void;
}

export interface SidecarOwnershipBackend {
  readonly pid: number;
  now(): number;
  uuid(): string;
  open(root: string, path: string): Promise<SidecarOwnershipHandle>;
}

interface OwnerRecord {
  version: 1;
  instanceId: string;
  nonce: string;
  pid: number;
  acquiredAtMs: number;
}

function parseOwner(bytes: Uint8Array): OwnerRecord | null {
  try {
    const value = JSON.parse(utf8Text(bytes)) as Record<string, unknown>;
    if (
      value.version !== 1 ||
      typeof value.instanceId !== "string" ||
      !SAFE_INSTANCE.test(value.instanceId) ||
      typeof value.nonce !== "string" ||
      !SAFE_INSTANCE.test(value.nonce) ||
      !Number.isSafeInteger(value.pid) ||
      (value.pid as number) < 1 ||
      typeof value.acquiredAtMs !== "number" ||
      !Number.isFinite(value.acquiredAtMs) ||
      value.acquiredAtMs < 0 ||
      Object.keys(value).length !== 5
    ) return null;
    return value as unknown as OwnerRecord;
  } catch {
    return null;
  }
}

class DenoOwnershipHandle implements SidecarOwnershipHandle {
  readonly #file: Deno.FsFile;
  readonly #path: string;
  readonly #device: number | null;
  readonly #inode: number | null;

  private constructor(
    file: Deno.FsFile,
    path: string,
    info: Deno.FileInfo,
  ) {
    this.#file = file;
    this.#path = path;
    this.#device = info.dev;
    this.#inode = info.ino;
  }

  static async open(root: string, path: string): Promise<DenoOwnershipHandle> {
    await Deno.mkdir(root, { recursive: true, mode: 0o700 });
    await Deno.chmod(root, 0o700);
    let file: Deno.FsFile;
    try {
      file = await Deno.open(path, {
        createNew: true,
        read: true,
        write: true,
        mode: 0o600,
      });
    } catch (error) {
      if (!(error instanceof Deno.errors.AlreadyExists)) throw error;
      file = await Deno.open(path, { read: true, write: true });
    }
    try {
      const info = await file.stat();
      if (
        !Number.isSafeInteger(info.dev) || info.dev < 0 ||
        info.ino === null || !Number.isSafeInteger(info.ino) || info.ino < 0
      ) throw new Error("ownership file has no stable filesystem identity");
      const handle = new DenoOwnershipHandle(file, path, info);
      await Deno.chmod(path, 0o600);
      return handle;
    } catch (error) {
      file.close();
      throw error;
    }
  }

  tryLockExclusive(): Promise<boolean> {
    return this.#file.tryLock(true);
  }

  async replace(bytes: Uint8Array): Promise<void> {
    await this.#file.truncate(0);
    await this.#file.seek(0, Deno.SeekMode.Start);
    let offset = 0;
    while (offset < bytes.byteLength) {
      const written = await this.#file.write(bytes.subarray(offset));
      if (written < 1) throw new Error("ownership write made no progress");
      offset += written;
    }
    await this.#file.syncData();
  }

  async read(): Promise<Uint8Array> {
    const info = await this.#file.stat();
    if (info.size < 1 || info.size > 4_096) return new Uint8Array();
    const bytes = new Uint8Array(info.size);
    await this.#file.seek(0, Deno.SeekMode.Start);
    let offset = 0;
    while (offset < bytes.byteLength) {
      const read = await this.#file.read(bytes.subarray(offset));
      if (read === null) break;
      if (read < 1) throw new Error("ownership read made no progress");
      offset += read;
    }
    return offset === bytes.byteLength ? bytes : bytes.subarray(0, offset);
  }

  async pathStillMatches(): Promise<boolean> {
    try {
      const info = await Deno.stat(this.#path);
      if (this.#device !== null && info.dev !== null && info.dev !== this.#device) return false;
      if (this.#inode !== null && info.ino !== null && info.ino !== this.#inode) return false;
      return true;
    } catch (error) {
      if (error instanceof Deno.errors.NotFound) return false;
      throw error;
    }
  }

  unlock(): Promise<void> {
    return this.#file.unlock();
  }

  close(): void {
    this.#file.close();
  }
}

function denoBackend(): SidecarOwnershipBackend {
  return {
    pid: Deno.pid,
    now: () => Date.now(),
    uuid: () => crypto.randomUUID(),
    open: (root, path) => DenoOwnershipHandle.open(root, path),
  };
}

export class DenoSidecarOwnershipLease implements SidecarOwnershipProof {
  readonly instanceId: string;
  readonly nonce: string;
  readonly #record: OwnerRecord;
  readonly #handle: SidecarOwnershipHandle;
  #operationTail: Promise<void> = Promise.resolve();
  #acceptingOperations = true;
  #released = false;

  private constructor(
    instanceId: string,
    nonce: string,
    record: OwnerRecord,
    handle: SidecarOwnershipHandle,
  ) {
    this.instanceId = instanceId;
    this.nonce = nonce;
    this.#record = record;
    this.#handle = handle;
  }

  static async acquire(options: {
    instanceId: string;
    root?: string;
    backend?: SidecarOwnershipBackend;
  }): Promise<DenoSidecarOwnershipLease> {
    if (!SAFE_INSTANCE.test(options.instanceId)) {
      throw new TypeError("sidecar ownership instanceId is invalid");
    }
    const root = (options.root ?? "/tmp/ghostprov-v2").replace(/\/+$/, "");
    if (!root) throw new TypeError("sidecar ownership root is invalid");
    const backend = options.backend ?? denoBackend();
    const handle = await backend.open(root, `${root}/sidecar.owner.lock`).catch(cause => {
      throw new SidecarOwnershipError("Unable to open sidecar ownership file", "storage", { cause });
    });
    let locked = false;
    try {
      locked = await handle.tryLockExclusive();
      if (!locked) {
        throw new SidecarOwnershipError(
          "Another sidecar process owns this sandbox journal",
          "contended",
        );
      }
      const nonce = backend.uuid();
      if (!SAFE_INSTANCE.test(nonce)) throw new Error("ownership nonce is invalid");
      const record: OwnerRecord = {
        version: 1,
        instanceId: options.instanceId,
        nonce,
        pid: backend.pid,
        acquiredAtMs: backend.now(),
      };
      await handle.replace(utf8Bytes(JSON.stringify(record)));
      const lease = new DenoSidecarOwnershipLease(
        options.instanceId,
        nonce,
        record,
        handle,
      );
      await lease.assertOwned();
      return lease;
    } catch (cause) {
      if (locked) await handle.unlock().catch(() => undefined);
      handle.close();
      if (cause instanceof SidecarOwnershipError) throw cause;
      throw new SidecarOwnershipError("Unable to acquire sidecar ownership", "storage", { cause });
    }
  }

  async assertOwned(): Promise<void> {
    await this.withOwnership(async () => undefined);
  }

  async withOwnership<T>(operation: () => Promise<T>): Promise<T> {
    if (!this.#acceptingOperations || this.#released) {
      throw new SidecarOwnershipError("Sidecar ownership was released", "lost");
    }
    const previous = this.#operationTail;
    let releaseSlot!: () => void;
    const slot = new Promise<void>(resolve => { releaseSlot = resolve; });
    this.#operationTail = previous.catch(() => undefined).then(() => slot);
    await previous.catch(() => undefined);
    try {
      await this.#assertOwnedUnlocked();
      try {
        return await operation();
      } finally {
        await this.#assertOwnedUnlocked();
      }
    } finally {
      releaseSlot();
    }
  }

  async #assertOwnedUnlocked(): Promise<void> {
    if (this.#released) {
      throw new SidecarOwnershipError("Sidecar ownership was released", "lost");
    }
    let pathMatches: boolean;
    let current: OwnerRecord | null;
    try {
      pathMatches = await this.#handle.pathStillMatches();
      if (!pathMatches) {
        throw new SidecarOwnershipError("Sidecar ownership path was replaced", "lost");
      }
      current = parseOwner(await this.#handle.read());
      // Detect unlink/replacement that raced the metadata read.
      pathMatches = await this.#handle.pathStillMatches();
    } catch (cause) {
      if (cause instanceof SidecarOwnershipError) throw cause;
      throw new SidecarOwnershipError("Unable to validate sidecar ownership", "storage", { cause });
    }
    if (!pathMatches) {
      throw new SidecarOwnershipError("Sidecar ownership path was replaced", "lost");
    }
    if (
      !current ||
      current.instanceId !== this.#record.instanceId ||
      current.nonce !== this.#record.nonce ||
      current.pid !== this.#record.pid
    ) {
      throw new SidecarOwnershipError("Sidecar ownership identity changed", "lost");
    }
  }

  async release(): Promise<void> {
    if (this.#released) return;
    this.#acceptingOperations = false;
    await this.#operationTail.catch(() => undefined);
    this.#released = true;
    try {
      await this.#handle.unlock();
    } catch (cause) {
      throw new SidecarOwnershipError("Unable to release sidecar ownership", "storage", { cause });
    } finally {
      this.#handle.close();
    }
  }
}
