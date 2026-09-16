import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, open, rename, unlink } from "node:fs/promises";
import * as path from "node:path";
import { FileLease, FileLeaseBusyError } from "../core/file-lease";
import { SYSTEM_CLOCK, type Clock } from "../core/ports";
import { projectId, type ProjectId } from "../sandbox/types";
import { canonicalJson } from "../v2/protocol/shared";
import { V2_R10_RELEASE } from "../v2/sidecar/assets";
import { ARCHIVED_R10_RELEASE_IDENTITIES } from "../v2/sidecar/retirement-identities";
import { FileSupervisorBootstrapJournal, type SupervisorBootstrapAttempt } from "../v2/supervisor/bootstrap-journal";
import { openSupervisorSessionBootstrap } from "../v2/supervisor/session-bootstrap";
import { FleetManifestStore, sameFleetWorkerRecord } from "./manifest";
import type { FleetManifest, FleetWorkerRecord } from "./types";

// One reviewed migration can cover the three unavailable members of the
// default eight-worker fleet while its required healthy majority is intact.
// These exact grants never refill or transfer to successor project IDs.
export const MAX_BOOTSTRAP_MIGRATION_PROJECTS = 3;
/** Explicit operator recovery is separate authority from the legacy majority-bound grants. */
export const MAX_BOOTSTRAP_OPERATOR_RECOVERY_PROJECTS = 5;
export const OPERATOR_BOOTSTRAP_PREPARATION_TIMEOUT_MS = 10 * 60_000;
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

interface MigrationLedgerIdentity {
  readonly identityFingerprint: string;
  readonly grants: readonly MigrationGrant[];
}

type MigrationLedger = MigrationLedgerIdentity & (
  | { readonly version: 1 }
  | { readonly version: 2; readonly purpose: "operator_bootstrap_recovery" }
);

/** Constructed only after the exact immutable operator ledger and journals validate. */
export interface ValidatedBootstrapMigrationAuthority {
  readonly projects: ReadonlySet<ProjectId>;
  readonly operatorRecoveryProjects: ReadonlySet<ProjectId>;
  readonly consumedProjects: ReadonlySet<ProjectId>;
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
  const version = parsed && typeof parsed === "object" && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>).version : undefined;
  const value = exactRecord(parsed, version === 2
    ? ["version", "purpose", "identityFingerprint", "grants"] : ["version", "identityFingerprint", "grants"]);
  if ((value.version !== 1 && value.version !== 2) ||
    (value.version === 2 && value.purpose !== "operator_bootstrap_recovery") || value.identityFingerprint !== fingerprint ||
    !Array.isArray(value.grants) || value.grants.length < 1 ||
    value.grants.length > (value.version === 2 ? MAX_BOOTSTRAP_OPERATOR_RECOVERY_PROJECTS : MAX_BOOTSTRAP_MIGRATION_PROJECTS)) {
    reject("ledger_identity_or_count");
  }
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
  const identity = { identityFingerprint: fingerprint, grants: Object.freeze(grants) };
  return value.version === 2
    ? Object.freeze({ version: 2, purpose: "operator_bootstrap_recovery", ...identity })
    : Object.freeze({ version: 1, ...identity });
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

async function readPrivateRecord(file: string): Promise<string | null> {
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
    return buffer.subarray(0, bytesRead).toString("utf8");
  } finally { await handle.close(); }
}

async function readLedger(file: string, identity: string): Promise<MigrationLedger | null> {
  const raw = await readPrivateRecord(file);
  return raw === null ? null : parseLedger(raw, identity);
}

interface MigrationConsumption {
  readonly version: 1;
  readonly identityFingerprint: string;
  readonly ledgerSha256: string;
  readonly consumedGrantSha256: readonly string[];
}

async function readConsumption(file: string, ledger: MigrationLedger): Promise<ReadonlySet<string>> {
  const raw = await readPrivateRecord(file);
  if (raw === null) return new Set();
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { reject("consumption_shape"); }
  const value = exactRecord(parsed, ["version", "identityFingerprint", "ledgerSha256", "consumedGrantSha256"]);
  if (value.version !== 1 || value.identityFingerprint !== ledger.identityFingerprint || value.ledgerSha256 !== digest(ledger) ||
    !Array.isArray(value.consumedGrantSha256) || value.consumedGrantSha256.length > ledger.grants.length) reject("consumption_identity");
  const allowed = new Set(ledger.grants.map(digest)), consumed = new Set<string>();
  for (const grant of value.consumedGrantSha256) {
    if (typeof grant !== "string" || !SHA256.test(grant) || !allowed.has(grant) || consumed.has(grant)) reject("consumption_grant");
    consumed.add(grant);
  }
  return consumed;
}

async function writePrivateRecord(file: string, value: unknown, beforeCommit?: () => void): Promise<void> {
  const content = canonicalJson(value);
  if (Buffer.byteLength(content) > MAX_LEDGER_BYTES) reject("ledger_size");
  const temporary = `${file}.${randomUUID()}.tmp`;
  try {
    const handle = await open(temporary, "wx", 0o600);
    try { await handle.writeFile(content); await handle.sync(); } finally { await handle.close(); }
    beforeCommit?.();
    await rename(temporary, file);
    await chmod(file, 0o600);
    if (process.platform !== "win32") {
      const directory = await open(path.dirname(file), "r");
      try { await directory.sync(); } finally { await directory.close(); }
    }
  } finally {
    await unlink(temporary).catch(error => { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; });
  }
}

export interface BootstrapMigrationOptions {
  readonly manifestFile: string;
  readonly identityFingerprint: string;
}

export interface OperatorRecoveryPreparationControls {
  readonly signal?: AbortSignal;
  /** Deterministic test clock; production uses one fixed ten-minute budget. */
  readonly clock?: Clock;
  readonly timeoutMs?: number;
}

interface OperatorPreparationContext {
  readonly signal: AbortSignal;
  readonly clock: Clock;
  readonly deadlineMs: number;
  assertActive(): void;
  retryLease<T>(operation: () => Promise<T>): Promise<T>;
}

async function withOperatorPreparation<T>(controls: OperatorRecoveryPreparationControls,
  operation: (context: OperatorPreparationContext) => Promise<T>): Promise<T> {
  const timeoutMs = controls.timeoutMs ?? OPERATOR_BOOTSTRAP_PREPARATION_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > OPERATOR_BOOTSTRAP_PREPARATION_TIMEOUT_MS) reject("preparation_timeout_bound");
  const clock = controls.clock ?? SYSTEM_CLOCK, deadlineMs = clock.now() + timeoutMs;
  const timeout = new AbortController(), timerCancellation = new AbortController();
  const signal = controls.signal ? AbortSignal.any([controls.signal, timeout.signal]) : timeout.signal;
  const timer = clock.waitUntil(deadlineMs, timerCancellation.signal).then(() => {
    timeout.abort(new DOMException("Operator bootstrap preparation deadline expired", "TimeoutError"));
  }, error => { if (!timerCancellation.signal.aborted) timeout.abort(error); });
  const assertActive = () => {
    controls.signal?.throwIfAborted();
    if (clock.now() >= deadlineMs || timeout.signal.aborted) reject("preparation_timeout");
    signal.throwIfAborted();
  };
  const context: OperatorPreparationContext = {
    signal, clock, deadlineMs, assertActive,
    async retryLease<T>(attempt: () => Promise<T>): Promise<T> {
      for (;;) {
        assertActive();
        try { const result = await attempt(); assertActive(); return result; }
        catch (error) {
          assertActive();
          if (!(error instanceof FileLeaseBusyError)) throw error;
          await clock.sleep(Math.min(250, deadlineMs - clock.now()), signal);
        }
      }
    },
  };
  try { return await operation(context); }
  catch (error) { assertActive(); throw error; }
  finally { timerCancellation.abort(); await timer; }
}

async function prepareOperatorRecoveryLedger(manifestFile: string, identity: string, ids: readonly ProjectId[],
  controls: OperatorRecoveryPreparationControls): Promise<{ file: string; grants: number; created: boolean }> {
  return withOperatorPreparation(controls, async context => {
    const file = `${manifestFile}.bootstrap-migration.json`;
    const lease = new FileLease({ file: `${file}.lock`, staleAfterMs: 120_000, clock: context.clock });
    try {
      await context.retryLease(() => lease.acquire({ signal: context.signal,
        timeoutMs: Math.min(15_000, context.deadlineMs - context.clock.now()) }));
      const manifest = await new FleetManifestStore({ file: manifestFile, identityFingerprint: identity }).load();
      const existing = await readLedger(file, identity);
      context.assertActive();
      if (manifest.identityKind !== "authenticated") reject("managed_worker_identity");
      if (existing && (existing.version !== 2 || canonicalJson(existing.grants.map(grant => grant.projectId).sort()) !== canonicalJson(ids))) reject("immutable_ledger");
      const consumed = existing ? await readConsumption(`${manifestFile}.bootstrap-migration-consumed.json`, existing) : new Set<string>();
      const grants: MigrationGrant[] = [];
      for (const id of ids) {
        context.assertActive();
        const previous = existing?.grants.find(grant => grant.projectId === id);
        const current = manifest.workers.find(worker => worker.projectId === id);
        if (previous && consumed.has(digest(previous))) {
          if (current && (!current.managed || current.index !== previous.workerIndex)) reject("managed_worker_identity");
          grants.push(previous);
          continue;
        }
        const worker = managedWorker(manifest, { projectId: id, workerIndex: previous?.workerIndex ?? current?.index ?? 0 });
        if (previous && worker.retirementReason === "bootstrap_hold") { grants.push(previous); continue; }
        // Unlike runtime admission, staging may wait for each independent
        // recovery writer. Never retain one journal lease while waiting for another.
        const actual = await context.retryLease(() => new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id)
          .withLease(context.signal, async entry => {
            context.assertActive();
            try { return await grantForAttempt(worker, await entry.load()); }
            catch (error) {
              if (error instanceof BootstrapMigrationError || error instanceof FileLeaseBusyError) throw error;
              reject("journal_validation");
            }
          }));
        if (previous && canonicalJson(actual) !== canonicalJson(previous)) reject("journal_changed");
        grants.push(actual);
      }
      context.assertActive();
      if (existing) return { file, grants: existing.grants.length, created: false };
      const ledger: MigrationLedger = { version: 2, purpose: "operator_bootstrap_recovery", identityFingerprint: identity, grants };
      // An atomic commit begun before cancellation finishes durably. No partial
      // ledger is published, and runtime admission revalidates every unused grant.
      await writePrivateRecord(file, ledger, context.assertActive);
      return { file, grants: grants.length, created: true };
    } finally { await lease.release(); }
  });
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
    return this.#withValidated(manifest, async authority => new Set([...authority.projects, ...authority.consumedProjects]), false);
  }

  /** Persist spent grant identity before exact held cleanup changes/removes its manifest record. */
  async recordConsumedHold(worker: Readonly<FleetWorkerRecord>, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const ledger = await readLedger(`${this.#manifestFile}.bootstrap-migration.json`, this.#identityFingerprint);
    if (!ledger) return;
    const grant = ledger.grants.find(value => value.projectId === worker.projectId);
    if (!grant) return;
    if (!worker.managed || worker.index !== grant.workerIndex || worker.state !== "draining" || worker.retirementReason !== "bootstrap_hold") {
      reject("consumption_requires_exact_hold");
    }
    const file = `${this.#manifestFile}.bootstrap-migration-consumed.json`;
    const lease = new FileLease({ file: `${file}.lock`, staleAfterMs: 120_000 });
    await lease.acquire({ timeoutMs: 15_000, signal });
    try {
      const manifest = await new FleetManifestStore({ file: this.#manifestFile, identityFingerprint: this.#identityFingerprint }).load();
      if (!sameFleetWorkerRecord(managedWorker(manifest, grant), worker)) reject("consumption_worker_changed");
      signal?.throwIfAborted();
      const consumed = new Set(await readConsumption(file, ledger)), grantSha256 = digest(grant);
      if (consumed.has(grantSha256)) return;
      consumed.add(grantSha256);
      const record: MigrationConsumption = { version: 1, identityFingerprint: this.#identityFingerprint,
        ledgerSha256: digest(ledger), consumedGrantSha256: [...consumed].sort() };
      signal?.throwIfAborted();
      await writePrivateRecord(file, record);
    } finally { await lease.release(); }
  }

  /** Hold journal leases only around the serialized non-destructive hold decision. */
  withValidated<T>(manifest: FleetManifest, operation: (projects: ReadonlySet<ProjectId>) => Promise<T>): Promise<T> {
    return this.#withValidated(manifest, authority => operation(authority.projects), true);
  }

  withValidatedAuthority<T>(manifest: FleetManifest,
    operation: (authority: ValidatedBootstrapMigrationAuthority) => Promise<T>): Promise<T> {
    return this.#withValidated(manifest, operation, true);
  }

  async #withValidated<T>(manifest: FleetManifest, operation: (authority: ValidatedBootstrapMigrationAuthority) => Promise<T>,
    skipInvalidActive: boolean): Promise<T> {
    if (manifest.identityFingerprint !== this.#identityFingerprint) reject("manifest_identity");
    const ledger = await readLedger(`${this.#manifestFile}.bootstrap-migration.json`, this.#identityFingerprint);
    if (!ledger) return operation(Object.freeze({ projects: new Set<ProjectId>(), operatorRecoveryProjects: new Set<ProjectId>(), consumedProjects: new Set<ProjectId>() }));
    if (manifest.identityKind !== "authenticated") reject("managed_worker_identity");
    const consumed = await readConsumption(`${this.#manifestFile}.bootstrap-migration-consumed.json`, ledger);
    const eligible = new Set<ProjectId>();
    const consumedProjects = new Set<ProjectId>();
    const visit = async (index: number): Promise<T> => {
      const grant = ledger.grants[index];
      if (!grant) return operation(Object.freeze({ projects: eligible,
        operatorRecoveryProjects: ledger.version === 2 ? eligible : new Set<ProjectId>(), consumedProjects }));
      if (consumed.has(digest(grant))) {
        const current = manifest.workers.find(worker => worker.projectId === grant.projectId);
        if (current && (!current.managed || current.index !== grant.workerIndex)) reject("managed_worker_identity");
        consumedProjects.add(grant.projectId);
        // Spent authority may still classify a retained hold, never a new active attempt.
        if (current?.state === "draining" && current.retirementReason === "bootstrap_hold") eligible.add(grant.projectId);
        return visit(index + 1);
      }
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

/** Local preparation only. V2 may wait for live recovery writers without changing their journals or process. */
export async function prepareBootstrapHoldMigrationLedger(options: BootstrapMigrationOptions & {
  readonly projectIds: readonly ProjectId[];
  readonly operatorRecovery?: true;
}, controls: OperatorRecoveryPreparationControls = {}): Promise<{ file: string; grants: number; created: boolean }> {
  const identity = fingerprint(options.identityFingerprint);
  if (!options.manifestFile.trim()) reject("manifest_path");
  const manifestFile = path.resolve(options.manifestFile);
  if (options.operatorRecovery !== undefined && options.operatorRecovery !== true) reject("operator_confirmation");
  const version = options.operatorRecovery === true ? 2 : 1;
  const ids = [...options.projectIds].sort();
  const maximum = version === 2 ? MAX_BOOTSTRAP_OPERATOR_RECOVERY_PROJECTS : MAX_BOOTSTRAP_MIGRATION_PROJECTS;
  if (ids.length < 1 || ids.length > maximum || new Set(ids).size !== ids.length ||
    ids.some(id => typeof id !== "string" || !UUID.test(id))) reject("project_count_or_identity");
  if (version === 2) return prepareOperatorRecoveryLedger(manifestFile, identity, ids, controls);
  if (Object.keys(controls).length > 0) reject("operator_preparation_controls");
  const file = `${manifestFile}.bootstrap-migration.json`;
  const lease = new FileLease({ file: `${file}.lock`, staleAfterMs: 120_000 });
  await lease.acquire({ timeoutMs: 15_000 });
  try {
    const manifest = await new FleetManifestStore({ file: manifestFile, identityFingerprint: identity }).load();
    const existing = await readLedger(file, identity);
    if (existing) {
      if (existing.version !== version || canonicalJson(existing.grants.map(grant => grant.projectId).sort()) !== canonicalJson(ids)) reject("immutable_ledger");
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
      await writePrivateRecord(file, ledger);
    };
    await visit(0);
    return { file, grants: grants.length, created: true };
  } finally { await lease.release(); }
}
