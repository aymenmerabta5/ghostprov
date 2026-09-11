import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, open, rename, unlink } from "node:fs/promises";
import * as path from "node:path";
import { FileLease } from "../core/file-lease";
import { projectId, type ProjectId } from "../sandbox/types";
import { canonicalJson } from "../v2/protocol/shared";
import { V2_R10_RELEASE } from "../v2/sidecar/assets";
import { ARCHIVED_R10_RELEASE_IDENTITIES } from "../v2/sidecar/retirement-identities";
import { FileSupervisorBootstrapJournal, type SupervisorBootstrapAttempt } from "../v2/supervisor/bootstrap-journal";
import { openSupervisorSessionBootstrap } from "../v2/supervisor/session-bootstrap";
import { FleetManifestStore } from "./manifest";
import type { FleetManifest, FleetWorkerRecord } from "./types";

export const MAX_BOOTSTRAP_MIGRATION_PROJECTS = 2;
const MAX_LEDGER_BYTES = 4 * 1024;
const RUNTIME_LEASE_TIMEOUT_MS = 250;
const SHA256 = /^[a-f0-9]{64}$(?![\s\S])/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;

interface MigrationGrant {
  readonly projectId: ProjectId;
  readonly workerIndex: number;
  readonly buildHash: string;
  readonly schemaHash: string;
  readonly bindingSha256: string;
  readonly journalSha256: string;
}

interface MigrationLedger {
  readonly version: 1;
  readonly identityFingerprint: string;
  readonly grants: readonly MigrationGrant[];
}

export class BootstrapMigrationError extends Error {
  readonly name = "BootstrapMigrationError";
  constructor(readonly code: string) { super(`Bootstrap migration rejected: ${code}`); }
}

function reject(code: string): never { throw new BootstrapMigrationError(code); }

function exactRecord(value: unknown, keys: readonly string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value) ||
    Object.keys(value).sort().join(",") !== [...keys].sort().join(",")) reject("ledger_shape");
  return value as Record<string, unknown>;
}

function archivedRelease(buildHash: string, schemaHash: string): boolean {
  return buildHash !== V2_R10_RELEASE.buildHash && ARCHIVED_R10_RELEASE_IDENTITIES.some(
    identity => identity.buildHash === buildHash && identity.schemaHash === schemaHash,
  );
}

function parseLedger(raw: string, fingerprint: string): MigrationLedger {
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { reject("ledger_shape"); }
  const value = exactRecord(parsed, ["version", "identityFingerprint", "grants"]);
  if (value.version !== 1 || value.identityFingerprint !== fingerprint ||
    !Array.isArray(value.grants) || value.grants.length < 1 ||
    value.grants.length > MAX_BOOTSTRAP_MIGRATION_PROJECTS) reject("ledger_identity_or_count");
  const seen = new Set<string>();
  const grants = value.grants.map(input => {
    const entry = exactRecord(input, ["projectId", "workerIndex", "buildHash", "schemaHash", "bindingSha256", "journalSha256"]);
    if (typeof entry.projectId !== "string" || !UUID.test(entry.projectId) || seen.has(entry.projectId) ||
      !Number.isSafeInteger(entry.workerIndex) || (entry.workerIndex as number) < 1) reject("grant_identity");
    for (const key of ["buildHash", "schemaHash", "bindingSha256", "journalSha256"] as const) {
      if (typeof entry[key] !== "string" || !SHA256.test(entry[key])) reject("grant_digest");
    }
    if (!archivedRelease(entry.buildHash as string, entry.schemaHash as string)) reject("grant_release");
    seen.add(entry.projectId);
    return Object.freeze({
      projectId: projectId(entry.projectId), workerIndex: entry.workerIndex as number,
      buildHash: entry.buildHash as string, schemaHash: entry.schemaHash as string,
      bindingSha256: entry.bindingSha256 as string, journalSha256: entry.journalSha256 as string,
    });
  });
  return Object.freeze({ version: 1, identityFingerprint: fingerprint, grants: Object.freeze(grants) });
}

function fingerprint(value: string): string {
  if (!SHA256.test(value)) reject("identity_fingerprint");
  return value;
}

function managedWorker(manifest: FleetManifest, grant: Pick<MigrationGrant, "projectId" | "workerIndex">): FleetWorkerRecord {
  const worker = manifest.workers.find(entry => entry.projectId === grant.projectId);
  if (manifest.identityKind !== "authenticated" || !worker?.managed || worker.index !== grant.workerIndex ||
    (worker.state === "draining" && worker.retirementReason !== "bootstrap_hold")) reject("managed_worker_identity");
  return worker;
}

function digest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

async function grantForAttempt(worker: FleetWorkerRecord, attempt: SupervisorBootstrapAttempt | null): Promise<MigrationGrant> {
  if (!attempt || attempt.phase !== "submitted" || attempt.binding.projectId !== worker.projectId ||
    attempt.binding.protocolRevision !== "r2" || attempt.binding.schemaVersion !== 2 ||
    !archivedRelease(attempt.binding.buildHash, attempt.binding.schemaHash)) reject("submitted_archived_journal_required");
  try {
    // Verify the sealed authority, then discard its private credential payload.
    await openSupervisorSessionBootstrap({ sessionId: attempt.sessionId, expectedBinding: attempt.binding, sealed: attempt.sealed });
  } catch { reject("journal_authority"); }
  return Object.freeze({
    projectId: worker.projectId, workerIndex: worker.index,
    buildHash: attempt.binding.buildHash, schemaHash: attempt.binding.schemaHash,
    bindingSha256: digest(attempt.binding), journalSha256: digest(attempt),
  });
}

async function readLedger(file: string, identity: string): Promise<MigrationLedger | null> {
  let metadata;
  try { metadata = await lstat(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; throw error; }
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > MAX_LEDGER_BYTES ||
    (process.platform !== "win32" && (metadata.mode & 0o077) !== 0)) reject("ledger_private_regular_file");
  const handle = await open(file, "r");
  try {
    const opened = await handle.stat();
    if (!opened.isFile() || opened.size > MAX_LEDGER_BYTES || opened.dev !== metadata.dev || opened.ino !== metadata.ino) {
      reject("ledger_file_changed");
    }
    const buffer = Buffer.alloc(MAX_LEDGER_BYTES + 1);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    if (bytesRead > MAX_LEDGER_BYTES) reject("ledger_size");
    return parseLedger(buffer.subarray(0, bytesRead).toString("utf8"), identity);
  } finally { await handle.close(); }
}

export interface BootstrapMigrationOptions {
  readonly manifestFile: string;
  readonly identityFingerprint: string;
}

/** Trusted operator-only input; never constructed from request data. */
export class FileBootstrapHoldMigration {
  readonly #manifestFile: string;
  readonly #identityFingerprint: string;

  constructor(options: BootstrapMigrationOptions) {
    if (!options.manifestFile.trim()) reject("manifest_path");
    this.#manifestFile = path.resolve(options.manifestFile);
    this.#identityFingerprint = fingerprint(options.identityFingerprint);
  }

  validate(manifest: FleetManifest): Promise<ReadonlySet<ProjectId>> {
    return this.#withValidated(manifest, async projects => projects, false);
  }

  /** Hold journal leases only around the serialized non-destructive hold decision. */
  withValidated<T>(manifest: FleetManifest, operation: (projects: ReadonlySet<ProjectId>) => Promise<T>): Promise<T> {
    return this.#withValidated(manifest, operation, true);
  }

  async #withValidated<T>(manifest: FleetManifest, operation: (projects: ReadonlySet<ProjectId>) => Promise<T>,
    skipInvalidActive: boolean): Promise<T> {
    if (manifest.identityFingerprint !== this.#identityFingerprint) reject("manifest_identity");
    const ledger = await readLedger(`${this.#manifestFile}.bootstrap-migration.json`, this.#identityFingerprint);
    if (!ledger) return operation(new Set());
    const eligible = new Set<ProjectId>();
    const visit = async (index: number): Promise<T> => {
      const grant = ledger.grants[index];
      if (!grant) return operation(eligible);
      const worker = managedWorker(manifest, grant);
      if (worker.retirementReason === "bootstrap_hold") {
        // The immutable exact ID consumed this slot. Losing its old journal
        // cannot refill the grant or transfer it to a successor project.
        eligible.add(worker.projectId);
        return visit(index + 1);
      }
      const timeout = AbortSignal.timeout(RUNTIME_LEASE_TIMEOUT_MS);
      let acquired = false;
      try {
        return await new FileSupervisorBootstrapJournal(`${this.#manifestFile}.bootstrap`, worker.projectId)
          .withLease(timeout, async entry => {
            acquired = true;
            let actual: MigrationGrant;
            try {
              actual = await grantForAttempt(worker, await entry.load());
              if (canonicalJson(actual) !== canonicalJson(grant)) reject("journal_changed");
            }
            catch (error) {
              // Independent recovery can legitimately replace an unused old
              // journal. Deny that grant without starving another exact grant.
              if (skipInvalidActive) return visit(index + 1);
              if (error instanceof BootstrapMigrationError) throw error;
              reject("journal_validation");
            }
            eligible.add(worker.projectId);
            return visit(index + 1);
          });
      } catch (error) {
        if (!acquired && timeout.aborted) return visit(index + 1);
        throw error;
      }
    };
    return visit(0);
  }
}

/** Offline preparation only. It neither clears journals nor changes fleet state. */
export async function prepareBootstrapHoldMigrationLedger(options: BootstrapMigrationOptions & {
  readonly projectIds: readonly ProjectId[];
}): Promise<{ file: string; grants: number; created: boolean }> {
  const identity = fingerprint(options.identityFingerprint);
  if (!options.manifestFile.trim()) reject("manifest_path");
  const manifestFile = path.resolve(options.manifestFile);
  const ids = [...options.projectIds].sort();
  if (ids.length < 1 || ids.length > MAX_BOOTSTRAP_MIGRATION_PROJECTS || new Set(ids).size !== ids.length ||
    ids.some(id => typeof id !== "string" || !UUID.test(id))) reject("project_count_or_identity");
  const file = `${manifestFile}.bootstrap-migration.json`;
  const lease = new FileLease({ file: `${file}.lock`, staleAfterMs: 120_000 });
  await lease.acquire({ timeoutMs: 15_000 });
  try {
    const manifest = await new FleetManifestStore({ file: manifestFile, identityFingerprint: identity }).load();
    const existing = await readLedger(file, identity);
    if (existing) {
      if (canonicalJson(existing.grants.map(grant => grant.projectId).sort()) !== canonicalJson(ids)) reject("immutable_ledger");
      const validated = await new FileBootstrapHoldMigration({ manifestFile, identityFingerprint: identity }).validate(manifest);
      if (validated.size !== existing.grants.length) reject("journal_busy");
      return { file, grants: existing.grants.length, created: false };
    }
    const grants: MigrationGrant[] = [];
    const visit = async (index: number): Promise<void> => {
      const id = ids[index];
      if (id) {
        const worker = manifest.workers.find(value => value.projectId === id);
        if (!worker) reject("managed_worker_identity");
        managedWorker(manifest, { projectId: id, workerIndex: worker.index });
        await new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id).withLease(undefined, async entry => {
          try { grants.push(await grantForAttempt(worker, await entry.load())); }
          catch (error) {
            if (error instanceof BootstrapMigrationError) throw error;
            reject("journal_validation");
          }
          await visit(index + 1);
        });
        return;
      }
      const ledger: MigrationLedger = { version: 1, identityFingerprint: identity, grants };
      const content = canonicalJson(ledger);
      if (Buffer.byteLength(content) > MAX_LEDGER_BYTES) reject("ledger_size");
      const temporary = `${file}.${randomUUID()}.tmp`;
      try {
        const handle = await open(temporary, "wx", 0o600);
        try { await handle.writeFile(content); await handle.sync(); } finally { await handle.close(); }
        await rename(temporary, file);
        await chmod(file, 0o600);
        if (process.platform !== "win32") {
          const directory = await open(path.dirname(file), "r");
          try { await directory.sync(); } finally { await directory.close(); }
        }
      } finally {
        await unlink(temporary).catch(error => { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; });
      }
    };
    await visit(0);
    return { file, grants: grants.length, created: true };
  } finally { await lease.release(); }
}
