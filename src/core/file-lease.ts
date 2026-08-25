import { execFile } from "node:child_process";
import {
  lstat,
  mkdir,
  open,
  opendir,
  rename,
  rm,
  rmdir,
  stat,
} from "node:fs/promises";
import * as path from "node:path";
import {
  SYSTEM_CLOCK,
  SYSTEM_RANDOM,
  type Clock,
  type RandomSource,
} from "./ports";

const RECORD_VERSION = 2 as const;
const MAX_RECORD_BYTES = 4 * 1024;
const MAX_DIRECTORY_ENTRIES = 8;
const OWNER_PREFIX = "owner.";
const OWNER_SUFFIX = ".json";
const SAFE_NONCE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;

class FileLeaseRecordError extends Error {
  readonly name = "FileLeaseRecordError";
}

interface LegacyLeaseRecord {
  version?: undefined;
  pid: number;
  nonce: string;
  acquiredAtMs: number;
}

interface LeaseRecord {
  version: typeof RECORD_VERSION;
  pid: number;
  nonce: string;
  platform: NodeJS.Platform;
  processIdentity: string | null;
  processStartedAtMs: number | null;
  acquiredAtMs: number;
}

type ParsedLeaseRecord = LegacyLeaseRecord | LeaseRecord;

export interface FileLeaseProcessIdentity {
  readonly state: "alive" | "dead" | "unknown";
  readonly identity?: string;
  readonly startedAtMs?: number;
}

export type FileLeaseProcessProbe = (
  pid: number,
) => Promise<FileLeaseProcessIdentity>;

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
  /** Grace for incomplete/malformed initialization; valid dead owners reclaim immediately. */
  staleAfterMs: number;
  clock?: Clock;
  random?: RandomSource;
  processProbe?: FileLeaseProcessProbe;
}

function isNodeError(error: unknown, code: string): boolean {
  return !!error && typeof error === "object" && (error as { code?: unknown }).code === code;
}

function validPid(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 1;
}

function validTime(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function parseRecord(raw: string): ParsedLeaseRecord | null {
  try {
    const value = JSON.parse(raw) as Record<string, unknown>;
    if (!validPid(value.pid) || typeof value.nonce !== "string" || !SAFE_NONCE.test(value.nonce)) {
      return null;
    }
    if (!validTime(value.acquiredAtMs)) return null;
    if (value.version === undefined) {
      if (Object.keys(value).some(key => !["pid", "nonce", "acquiredAtMs"].includes(key))) {
        return null;
      }
      return {
        pid: value.pid,
        nonce: value.nonce,
        acquiredAtMs: value.acquiredAtMs,
      };
    }
    if (
      value.version !== RECORD_VERSION ||
      typeof value.platform !== "string" ||
      !(value.processIdentity === null || typeof value.processIdentity === "string") ||
      !(value.processStartedAtMs === null || validTime(value.processStartedAtMs)) ||
      Object.keys(value).some(key => ![
        "version", "pid", "nonce", "platform", "processIdentity",
        "processStartedAtMs", "acquiredAtMs",
      ].includes(key))
    ) return null;
    return {
      version: RECORD_VERSION,
      pid: value.pid,
      nonce: value.nonce,
      platform: value.platform as NodeJS.Platform,
      processIdentity: value.processIdentity as string | null,
      processStartedAtMs: value.processStartedAtMs as number | null,
      acquiredAtMs: value.acquiredAtMs,
    };
  } catch {
    return null;
  }
}

function processState(pid: number): "alive" | "dead" | "unknown" {
  try {
    process.kill(pid, 0);
    return "alive";
  } catch (error) {
    const code = (error as { code?: unknown })?.code;
    if (code === "ESRCH") return "dead";
    if (code === "EPERM" || code === "EACCES") return "alive";
    return "unknown";
  }
}

function execFileText(executable: string, args: readonly string[]): Promise<string> {
  return new Promise((resolve, reject) => {
    execFile(executable, [...args], {
      encoding: "utf8",
      windowsHide: true,
      timeout: 5_000,
      maxBuffer: 64 * 1024,
    }, (error, stdout) => error ? reject(error) : resolve(stdout));
  });
}

let linuxClockTicks: Promise<number | null> | null = null;

async function linuxClockTicksPerSecond(): Promise<number | null> {
  linuxClockTicks ??= execFileText("getconf", ["CLK_TCK"]).then(output => {
    const value = Number(output.trim());
    return Number.isFinite(value) && value > 0 ? value : null;
  }).catch(() => null);
  return linuxClockTicks;
}

async function linuxProcessIdentity(pid: number): Promise<FileLeaseProcessIdentity> {
  let statHandle: Awaited<ReturnType<typeof open>>;
  try { statHandle = await open(`/proc/${pid}/stat`, "r"); }
  catch (error) {
    if (isNodeError(error, "ENOENT") || isNodeError(error, "ESRCH")) return { state: "dead" };
    return { state: processState(pid) };
  }
  let processStat: string;
  try { processStat = await statHandle.readFile({ encoding: "utf8" }); }
  catch { return { state: processState(pid) }; }
  finally { await statHandle.close().catch(() => undefined); }
  const close = processStat.lastIndexOf(")");
  if (close < 0) return { state: "unknown" };
  const fields = processStat.slice(close + 2).trim().split(/\s+/);
  const state = fields[0];
  const startTicks = fields[19];
  if (state === "Z" || state === "X" || state === "x") return { state: "dead" };
  if (!startTicks || !/^\d+$/.test(startTicks)) return { state: "unknown" };

  let bootId = "unavailable";
  try {
    const bootHandle = await open("/proc/sys/kernel/random/boot_id", "r");
    try { bootId = (await bootHandle.readFile({ encoding: "utf8" })).trim() || bootId; }
    finally { await bootHandle.close().catch(() => undefined); }
  } catch {}
  if (bootId === "unavailable") {
    try {
      const procStat = await open("/proc/stat", "r");
      try {
        const match = /^btime\s+(\d+)$/m.exec(await procStat.readFile({ encoding: "utf8" }));
        if (match) bootId = `btime-${match[1]}`;
      } finally {
        await procStat.close().catch(() => undefined);
      }
    } catch {}
  }
  let startedAtMs: number | undefined;
  try {
    const [ticksPerSecond, uptimeHandle] = await Promise.all([
      linuxClockTicksPerSecond(),
      open("/proc/uptime", "r"),
    ]);
    try {
      const uptime = Number((await uptimeHandle.readFile({ encoding: "utf8" })).split(/\s+/, 1)[0]);
      const ticks = Number(startTicks);
      if (ticksPerSecond && Number.isFinite(uptime) && Number.isFinite(ticks)) {
        const value = Date.now() - Math.max(0, uptime - ticks / ticksPerSecond) * 1_000;
        if (Number.isFinite(value) && value >= 0) startedAtMs = value;
      }
    } finally {
      await uptimeHandle.close().catch(() => undefined);
    }
  } catch {}
  return {
    state: "alive",
    identity: `linux:${bootId}:${startTicks}`,
    ...(startedAtMs === undefined ? {} : { startedAtMs }),
  };
}

async function windowsProcessIdentity(pid: number): Promise<FileLeaseProcessIdentity> {
  const executable = path.join(
    process.env.SystemRoot || "C:\\Windows",
    "System32", "WindowsPowerShell", "v1.0", "powershell.exe",
  );
  const script = "$ErrorActionPreference='Stop'; " +
    `try { $p=Get-Process -Id ${pid} -ErrorAction Stop; ` +
    "[Console]::Out.Write($p.StartTime.ToUniversalTime().ToFileTimeUtc()) } " +
    "catch [Microsoft.PowerShell.Commands.ProcessCommandException] { " +
    "[Console]::Out.Write('dead') } catch { [Console]::Out.Write('unknown') }";
  try {
    const output = (await execFileText(executable, [
      "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script,
    ])).trim();
    if (output === "dead") return { state: "dead" };
    if (!/^\d+$/.test(output)) return { state: processState(pid) };
    const fileTime = BigInt(output);
    const startedAtMs = Number(fileTime / 10_000n - 11_644_473_600_000n);
    return {
      state: "alive",
      identity: `win32:${output}`,
      ...(Number.isSafeInteger(startedAtMs) && startedAtMs >= 0 ? { startedAtMs } : {}),
    };
  } catch {
    return { state: processState(pid) };
  }
}

async function posixProcessIdentity(pid: number): Promise<FileLeaseProcessIdentity> {
  try {
    const output = (await execFileText("ps", ["-o", "lstart=", "-p", String(pid)])).trim();
    if (!output) return { state: "dead" };
    const startedAtMs = Date.parse(output);
    return {
      state: "alive",
      identity: `${process.platform}:${output.replace(/\s+/g, " ")}`,
      ...(Number.isFinite(startedAtMs) ? { startedAtMs } : {}),
    };
  } catch {
    return { state: processState(pid) };
  }
}

let currentProcessIdentity: Promise<FileLeaseProcessIdentity> | null = null;

async function defaultProcessProbe(pid: number): Promise<FileLeaseProcessIdentity> {
  const probe = async () => {
    const liveness = processState(pid);
    if (liveness === "dead") return { state: "dead" } as FileLeaseProcessIdentity;
    if (process.platform === "linux") return linuxProcessIdentity(pid);
    if (process.platform === "win32") return windowsProcessIdentity(pid);
    if (process.platform === "darwin" || process.platform === "freebsd" || process.platform === "openbsd") {
      return posixProcessIdentity(pid);
    }
    return { state: processState(pid) } as FileLeaseProcessIdentity;
  };
  if (pid !== process.pid) return probe();
  currentProcessIdentity ??= probe();
  return currentProcessIdentity;
}

async function readBoundedText(file: string): Promise<string> {
  const handle = await open(file, "r");
  try {
    const metadata = await handle.stat();
    if (!metadata.isFile() || metadata.size < 1 || metadata.size > MAX_RECORD_BYTES) {
      throw new FileLeaseRecordError("Lease owner record is not a bounded regular file");
    }
    const buffer = Buffer.allocUnsafe(metadata.size);
    let offset = 0;
    while (offset < buffer.byteLength) {
      const { bytesRead } = await handle.read(buffer, offset, buffer.byteLength - offset, offset);
      if (bytesRead < 1) throw new FileLeaseRecordError("Lease owner record ended unexpectedly");
      offset += bytesRead;
    }
    return buffer.toString("utf8");
  } finally {
    await handle.close().catch(() => undefined);
  }
}

function ownerMarker(file: string, nonce: string): string {
  return path.join(file, `${OWNER_PREFIX}${nonce}${OWNER_SUFFIX}`);
}

function compareStrongProcessIdentity(
  recorded: string,
  current: string,
): "same" | "different" | "weak" {
  const recordedLinux = /^linux:([^:]+):(\d+)$/.exec(recorded);
  const currentLinux = /^linux:([^:]+):(\d+)$/.exec(current);
  if (recordedLinux && currentLinux) {
    if (
      recordedLinux[1] === "unavailable" &&
      currentLinux[1] === "unavailable" &&
      recordedLinux[2] === currentLinux[2]
    ) return "same";
    if (recordedLinux[1] === "unavailable" || currentLinux[1] === "unavailable") {
      return "weak";
    }
    return recorded === current ? "same" : "different";
  }
  return recorded === current ? "same" : "different";
}

async function syncDirectory(directory: string): Promise<void> {
  if (process.platform === "win32") return;
  const handle = await open(directory, "r");
  try { await handle.sync(); }
  finally { await handle.close().catch(() => undefined); }
}

async function readLockDirectoryBounded(directory: string) {
  const handle = await opendir(directory);
  const entries = [];
  try {
    for await (const entry of handle) {
      entries.push(entry);
      if (entries.length > MAX_DIRECTORY_ENTRIES) break;
    }
  } finally {
    await handle.close().catch(() => undefined);
  }
  return entries;
}

interface HeldLease {
  readonly nonce: string;
  readonly marker: string;
  readonly record: string;
}

export class FileLease {
  readonly #file: string;
  readonly #staleAfterMs: number;
  readonly #initializationGraceMs: number;
  readonly #clock: Clock;
  readonly #random: RandomSource;
  readonly #processProbe: FileLeaseProcessProbe;
  #probeCache: { key: string; atMs: number; value: FileLeaseProcessIdentity } | null = null;
  #held: HeldLease | null = null;
  #operationTail: Promise<void> = Promise.resolve();

  constructor(options: FileLeaseOptions) {
    if (!Number.isFinite(options.staleAfterMs) || options.staleAfterMs < 1) {
      throw new RangeError("staleAfterMs must be positive");
    }
    this.#file = path.resolve(options.file);
    this.#staleAfterMs = options.staleAfterMs;
    this.#initializationGraceMs = Math.min(options.staleAfterMs, 1_000);
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#random = options.random ?? SYSTEM_RANDOM;
    this.#processProbe = options.processProbe ?? defaultProcessProbe;
  }

  acquire(options: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<void> {
    return this.#enqueue(() => this.#acquire(options));
  }

  release(): Promise<void> {
    return this.#enqueue(async () => {
      if (this.#held) await this.#release();
    });
  }

  #enqueue(operation: () => Promise<void>): Promise<void> {
    const result = this.#operationTail.catch(() => undefined).then(operation);
    this.#operationTail = result.catch(() => undefined);
    return result;
  }

  async #acquire(options: { timeoutMs?: number; signal?: AbortSignal }): Promise<void> {
    if (this.#held) {
      await this.#assertHeld();
      return;
    }
    const timeoutMs = options.timeoutMs ?? 30_000;
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new RangeError("timeoutMs must be non-negative");
    }
    const deadline = this.#clock.now() + timeoutMs;
    for (;;) {
      if (options.signal?.aborted) throw new DOMException("File lease acquisition aborted", "AbortError");
      if (await this.#tryCreate()) {
        if (options.signal?.aborted) {
          await this.#release().catch(() => undefined);
          throw new DOMException("File lease acquisition aborted", "AbortError");
        }
        return;
      }
      if (await this.#reclaim()) continue;
      const remaining = deadline - this.#clock.now();
      if (remaining <= 0) throw new FileLeaseBusyError(this.#file);
      const jitter = Math.floor(this.#random.next() * 5);
      await this.#clock.sleep(Math.min(remaining, 10 + jitter), options.signal);
    }
  }

  async #tryCreate(): Promise<boolean> {
    const parent = path.dirname(this.#file);
    await mkdir(parent, { recursive: true });
    try {
      await lstat(this.#file);
      return false;
    } catch (error) {
      if (!isNodeError(error, "ENOENT")) throw error;
    }
    const nonce = this.#random.uuid();
    if (!SAFE_NONCE.test(nonce)) throw new Error("File lease nonce is invalid");
    const processIdentity = await this.#probeProcess(process.pid, `self:${process.pid}`);
    const record: LeaseRecord = {
      version: RECORD_VERSION,
      pid: process.pid,
      nonce,
      platform: process.platform,
      processIdentity: processIdentity.identity ?? null,
      processStartedAtMs: processIdentity.startedAtMs ?? null,
      acquiredAtMs: Date.now(),
    };
    const recordBody = JSON.stringify(record);
    // Build a complete generation beside the lock path, then install the
    // non-empty directory with one rename. The public lock path is therefore
    // never an observable half-initialized empty directory.
    const staging = `${this.#file}.candidate.${process.pid}.${nonce}`;
    const stagingMarker = ownerMarker(staging, nonce);
    await mkdir(staging, { mode: 0o700 });
    const handle = await open(stagingMarker, "wx", 0o600);
    let installed = false;
    try {
      await handle.writeFile(recordBody);
      await handle.sync();
      await handle.close();
      try {
        await rename(staging, this.#file);
        installed = true;
      } catch (error) {
        try {
          await lstat(this.#file);
          return false;
        } catch (missing) {
          if (!isNodeError(missing, "ENOENT")) throw missing;
        }
        throw error;
      }
      const marker = ownerMarker(this.#file, nonce);
      this.#held = Object.freeze({ nonce, marker, record: recordBody });
      await syncDirectory(this.#file).catch(() => undefined);
      await syncDirectory(parent).catch(() => undefined);
      return true;
    } finally {
      await handle.close().catch(() => undefined);
      if (!installed) {
        await rm(stagingMarker, { force: true }).catch(() => undefined);
        await rmdir(staging).catch(() => undefined);
      }
    }
  }

  async #release(): Promise<void> {
    const held = this.#held;
    if (!held) return;
    let raw: string | null;
    try { raw = await readBoundedText(held.marker); }
    catch (error) {
      if (!isNodeError(error, "ENOENT")) throw error;
      raw = null;
    }
    const record = raw === null ? null : parseRecord(raw);
    if (!record || record.nonce !== held.nonce || raw !== held.record) {
      this.#held = null;
      throw new FileLeaseOwnershipError(this.#file);
    }
    try {
      await rm(held.marker);
    } catch (error) {
      if (!isNodeError(error, "ENOENT")) throw error;
    }
    // From this point the exact owner marker is gone; the lease is no longer
    // held even if empty-directory cleanup is transiently blocked.
    this.#held = null;
    try {
      await rmdir(this.#file);
    } catch (error) {
      if (
        isNodeError(error, "ENOENT") ||
        isNodeError(error, "ENOTEMPTY") ||
        isNodeError(error, "EEXIST")
      ) return;
      throw new FileLeaseOwnershipError(this.#file);
    }
  }

  async #assertHeld(): Promise<void> {
    const held = this.#held;
    if (!held) return;
    let raw: string | null;
    try { raw = await readBoundedText(held.marker); }
    catch (error) {
      if (!isNodeError(error, "ENOENT")) throw error;
      raw = null;
    }
    const record = raw === null ? null : parseRecord(raw);
    if (!record || record.nonce !== held.nonce || raw !== held.record) {
      this.#held = null;
      throw new FileLeaseOwnershipError(this.#file);
    }
  }

  async #reclaim(): Promise<boolean> {
    let metadata;
    try { metadata = await lstat(this.#file); }
    catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      throw error;
    }
    if (metadata.isSymbolicLink()) return false;
    if (metadata.isFile()) return this.#reclaimLegacyFile();
    if (!metadata.isDirectory()) return false;

    let entries;
    try { entries = await readLockDirectoryBounded(this.#file); }
    catch (error) {
      if (isNodeError(error, "ENOENT") || isNodeError(error, "ENOTDIR")) return true;
      return false;
    }
    if (entries.length === 0) {
      if (Date.now() - metadata.mtimeMs <= this.#initializationGraceMs) return false;
      try {
        await rmdir(this.#file);
        return true;
      } catch (error) {
        if (isNodeError(error, "ENOENT")) return true;
        if (isNodeError(error, "ENOTEMPTY") || isNodeError(error, "EEXIST")) return false;
        throw error;
      }
    }
    if (entries.length > MAX_DIRECTORY_ENTRIES || entries.length !== 1) return false;
    const entry = entries[0]!;
    if (
      !entry.isFile() ||
      !entry.name.startsWith(OWNER_PREFIX) ||
      !entry.name.endsWith(OWNER_SUFFIX)
    ) return false;
    const marker = path.join(this.#file, entry.name);
    let raw: string;
    let markerMetadata;
    try {
      [raw, markerMetadata] = await Promise.all([readBoundedText(marker), stat(marker)]);
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      if (!(error instanceof FileLeaseRecordError)) return false;
      if (Date.now() - metadata.mtimeMs <= this.#initializationGraceMs) return false;
      return this.#removeMarker(marker, null);
    }
    const record = parseRecord(raw);
    if (!record) {
      if (Date.now() - markerMetadata.mtimeMs <= this.#initializationGraceMs) return false;
      return this.#removeMarker(marker, raw);
    }
    if (!await this.#recordIsReclaimable(record)) return false;
    return this.#removeMarker(marker, raw);
  }

  async #recordIsReclaimable(record: ParsedLeaseRecord): Promise<boolean> {
    const recordKey = "version" in record && record.version === RECORD_VERSION
      ? `${record.pid}:${record.nonce}:${record.platform}:${record.processIdentity ?? "?"}:${record.processStartedAtMs ?? "?"}`
      : `${record.pid}:${record.nonce}:legacy:${record.acquiredAtMs}`;
    const identity = await this.#probeProcess(record.pid, recordKey);
    if (identity.state === "dead") return true;
    if (identity.state !== "alive") return false;
    if ("version" in record && record.version === RECORD_VERSION) {
      if (record.platform !== process.platform) return true;
      if (record.processIdentity && identity.identity) {
        const comparison = compareStrongProcessIdentity(
          record.processIdentity,
          identity.identity,
        );
        if (comparison === "same") return false;
        if (comparison === "different") return true;
      }
      if (record.processStartedAtMs != null && identity.startedAtMs != null) {
        return Math.abs(record.processStartedAtMs - identity.startedAtMs) > 1_000;
      }
      if (identity.startedAtMs != null) {
        return identity.startedAtMs > record.acquiredAtMs + 1_000;
      }
      return false;
    }
    if (identity.startedAtMs != null) {
      return identity.startedAtMs > record.acquiredAtMs + 1_000;
    }
    return false;
  }

  async #probeProcess(pid: number, key: string): Promise<FileLeaseProcessIdentity> {
    const now = Date.now();
    const cached = this.#probeCache;
    if (cached && cached.key === key && now - cached.atMs <= 1_000) return cached.value;
    const value = await this.#processProbe(pid);
    this.#probeCache = { key, atMs: Date.now(), value };
    return value;
  }

  async #removeMarker(marker: string, expectedRaw: string | null): Promise<boolean> {
    if (expectedRaw !== null) {
      let latest: string;
      try { latest = await readBoundedText(marker); }
      catch (error) { return isNodeError(error, "ENOENT"); }
      if (latest !== expectedRaw) return false;
    }
    try { await rm(marker); }
    catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      throw error;
    }
    try {
      await rmdir(this.#file);
      return true;
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return true;
      if (isNodeError(error, "ENOTEMPTY") || isNodeError(error, "EEXIST")) return false;
      throw error;
    }
  }

  async #reclaimLegacyFile(): Promise<boolean> {
    let raw: string;
    let metadata;
    try { [raw, metadata] = await Promise.all([readBoundedText(this.#file), stat(this.#file)]); }
    catch (error) { return isNodeError(error, "ENOENT"); }
    const record = parseRecord(raw);
    if (record) {
      if (!await this.#recordIsReclaimable(record)) return false;
    } else if (Date.now() - metadata.mtimeMs <= this.#initializationGraceMs) {
      return false;
    }

    const migration = new FileLease({
      file: `${this.#file}.migration`,
      staleAfterMs: this.#staleAfterMs,
      clock: this.#clock,
      random: this.#random,
      processProbe: this.#processProbe,
    });
    try {
      await migration.acquire({ timeoutMs: this.#initializationGraceMs + 100 });
    } catch (error) {
      if (error instanceof FileLeaseBusyError) return false;
      throw error;
    }
    const quarantine = `${this.#file}.legacy.${process.pid}.${this.#random.uuid()}`;
    try {
      let latest: string;
      let latestMetadata;
      try {
        [latest, latestMetadata] = await Promise.all([
          readBoundedText(this.#file),
          lstat(this.#file),
        ]);
      } catch (error) {
        return isNodeError(error, "ENOENT");
      }
      if (!latestMetadata.isFile() || latest !== raw) return false;
      await rename(this.#file, quarantine);
      let quarantinedRaw: string;
      let quarantinedMetadata;
      try {
        [quarantinedRaw, quarantinedMetadata] = await Promise.all([
          readBoundedText(quarantine),
          lstat(quarantine),
        ]);
      } catch {
        await this.#restoreLegacyQuarantine(quarantine);
        return false;
      }
      if (!quarantinedMetadata.isFile() || quarantinedRaw !== raw) {
        await this.#restoreLegacyQuarantine(quarantine);
        return false;
      }
      await rm(quarantine);
      return true;
    } finally {
      await migration.release().catch(() => undefined);
    }
  }

  async #restoreLegacyQuarantine(quarantine: string): Promise<void> {
    try {
      await lstat(this.#file);
      return;
    } catch (error) {
      if (!isNodeError(error, "ENOENT")) return;
    }
    await rename(quarantine, this.#file).catch(() => undefined);
  }
}
