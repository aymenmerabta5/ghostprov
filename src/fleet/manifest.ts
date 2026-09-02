import { randomUUID } from "node:crypto";
import {
  chmod,
  mkdir,
  open,
  readFile,
  rename,
  unlink,
} from "node:fs/promises";
import { homedir } from "node:os";
import * as path from "node:path";
import type { PrismIdentityKind } from "../prism/session-manager";
import { projectId, type ProjectId } from "../sandbox/types";
import type {
  FleetManifest,
  FleetReplacementEvidence,
  FleetReplacementFailureKind,
  FleetRetirementReason,
  FleetWorkerRecord,
  FleetWorkerState,
} from "./types";

export type FleetManifestErrorKind =
  | "conflict"
  | "identity_mismatch"
  | "missing_worker"
  | "protocol";

export class FleetManifestError extends Error {
  readonly name = "FleetManifestError";

  constructor(message: string, readonly kind: FleetManifestErrorKind) {
    super(message);
  }
}

const SHA256 = /^[a-f0-9]{64}$/;
const MAX_MANIFEST_WORKERS = 128;

export function defaultFleetManifestFile(
  identityFingerprint: string,
  homeDirectory = homedir(),
): string {
  if (!SHA256.test(identityFingerprint)) {
    throw new TypeError("identityFingerprint must be a lowercase SHA-256 digest");
  }
  if (typeof homeDirectory !== "string" || homeDirectory.trim() === "") {
    throw new TypeError("homeDirectory must not be empty");
  }
  return path.join(
    path.resolve(homeDirectory),
    ".ghostprov",
    "fleets",
    identityFingerprint,
    "fleet.json",
  );
}

const STATES = new Set<FleetWorkerState>([
  "creating",
  "created",
  "ready",
  "draining",
  "failed",
]);
const RETIREMENT_REASONS = new Set<FleetRetirementReason>([
  "replacement",
  "missing",
  "surplus",
]);
const REPLACEMENT_FAILURE_KINDS = new Set<FleetReplacementFailureKind>([
  "hello",
  "readiness",
  "render",
  "timeout",
]);

function isNodeError(error: unknown, code: string): boolean {
  return !!error && typeof error === "object" && (error as { code?: unknown }).code === code;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new FleetManifestError(`${label} must be an object`, "protocol");
  }
  return value as Record<string, unknown>;
}

function finiteNonnegative(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new FleetManifestError(`${label} must be non-negative`, "protocol");
  }
  return value;
}

function parseReplacementEvidence(value: unknown): FleetReplacementEvidence | null {
  if (value === undefined || value === null) return null;
  const raw = record(value, "Fleet replacement evidence");
  if (typeof raw.signatureHash !== "string" || !SHA256.test(raw.signatureHash)) {
    throw new FleetManifestError("Fleet replacement evidence signature is invalid", "protocol");
  }
  if (typeof raw.kind !== "string" ||
    !REPLACEMENT_FAILURE_KINDS.has(raw.kind as FleetReplacementFailureKind)) {
    throw new FleetManifestError("Fleet replacement evidence kind is invalid", "protocol");
  }
  for (const key of ["count", "lastAttempt", "lastIncarnation"] as const) {
    if (!Number.isSafeInteger(raw[key]) || (raw[key] as number) < 0) {
      throw new FleetManifestError(`Fleet replacement evidence ${key} is invalid`, "protocol");
    }
  }
  if ((raw.count as number) < 1 || (raw.count as number) > 100) {
    throw new FleetManifestError("Fleet replacement evidence count is invalid", "protocol");
  }
  if (!Number.isSafeInteger(raw.lastLeaderGeneration) ||
    (raw.lastLeaderGeneration as number) < 1) {
    throw new FleetManifestError(
      "Fleet replacement evidence leader generation is invalid",
      "protocol",
    );
  }
  const firstObservedAtMs = finiteNonnegative(
    raw.firstObservedAtMs,
    "Fleet replacement evidence first observation",
  );
  const lastObservedAtMs = finiteNonnegative(
    raw.lastObservedAtMs,
    "Fleet replacement evidence last observation",
  );
  if (lastObservedAtMs < firstObservedAtMs) {
    throw new FleetManifestError(
      "Fleet replacement evidence observations are not monotonic",
      "protocol",
    );
  }
  return Object.freeze({
    signatureHash: raw.signatureHash,
    kind: raw.kind as FleetReplacementFailureKind,
    count: raw.count as number,
    firstObservedAtMs,
    lastObservedAtMs,
    lastAttempt: raw.lastAttempt as number,
    lastIncarnation: raw.lastIncarnation as number,
    lastLeaderGeneration: raw.lastLeaderGeneration as number,
    totalDurationMs: finiteNonnegative(
      raw.totalDurationMs,
      "Fleet replacement evidence total duration",
    ),
  });
}

function parseWorker(value: unknown): FleetWorkerRecord {
  const raw = record(value, "Fleet worker");
  if (!Number.isInteger(raw.index) || (raw.index as number) < 1) {
    throw new FleetManifestError("Fleet worker index must be positive", "protocol");
  }
  if (typeof raw.projectId !== "string") {
    throw new FleetManifestError("Fleet worker project ID is missing", "protocol");
  }
  if (typeof raw.title !== "string" || raw.title.length === 0) {
    throw new FleetManifestError("Fleet worker title is missing", "protocol");
  }
  if (typeof raw.state !== "string" || !STATES.has(raw.state as FleetWorkerState)) {
    throw new FleetManifestError("Fleet worker state is invalid", "protocol");
  }
  if (typeof raw.managed !== "boolean") {
    throw new FleetManifestError("Fleet worker management flag is invalid", "protocol");
  }
  if (raw.hasReachedReady !== undefined && typeof raw.hasReachedReady !== "boolean") {
    throw new FleetManifestError("Fleet worker readiness proof is invalid", "protocol");
  }
  if (!Number.isInteger(raw.leaderGeneration) || (raw.leaderGeneration as number) < 0) {
    throw new FleetManifestError("Fleet worker leader generation is invalid", "protocol");
  }
  if (raw.retirementReason !== undefined && raw.retirementReason !== null &&
    (typeof raw.retirementReason !== "string" ||
      !RETIREMENT_REASONS.has(raw.retirementReason as FleetRetirementReason))) {
    throw new FleetManifestError("Fleet worker retirement reason is invalid", "protocol");
  }
  const retirementReason = raw.retirementReason as FleetRetirementReason | null | undefined;
  if (raw.state !== "draining" && retirementReason != null) {
    throw new FleetManifestError("Only a draining worker may have a retirement reason", "protocol");
  }
  const replacementEvidence = parseReplacementEvidence(raw.replacementEvidence);
  if (raw.state === "draining" && replacementEvidence !== null) {
    throw new FleetManifestError("A draining worker cannot retain replacement evidence", "protocol");
  }
  return Object.freeze({
    index: raw.index as number,
    projectId: projectId(raw.projectId),
    title: raw.title,
    state: raw.state as FleetWorkerState,
    createdAtMs: finiteNonnegative(raw.createdAtMs, "Fleet worker creation time"),
    updatedAtMs: finiteNonnegative(raw.updatedAtMs, "Fleet worker update time"),
    leaderGeneration: raw.leaderGeneration as number,
    // Version-one manifests written before this field existed can prove only
    // a currently-ready worker. Treat every other legacy state as cold rather
    // than accidentally exceeding the stricter cold-start ceiling.
    hasReachedReady: raw.hasReachedReady === true ||
      (raw.hasReachedReady === undefined && raw.state === "ready"),
    managed: raw.managed,
    retirementReason: retirementReason ?? null,
    replacementEvidence,
  });
}

function parseManifest(
  value: unknown,
  expectedFingerprint: string,
): FleetManifest {
  const raw = record(value, "Fleet manifest");
  if (raw.version !== 1) {
    throw new FleetManifestError("Fleet manifest version is unsupported", "protocol");
  }
  if (raw.identityKind !== "authenticated" && raw.identityKind !== "anonymous") {
    throw new FleetManifestError("Fleet identity kind is invalid", "protocol");
  }
  if (typeof raw.identityFingerprint !== "string" || !SHA256.test(raw.identityFingerprint)) {
    throw new FleetManifestError("Fleet identity fingerprint is invalid", "protocol");
  }
  if (raw.identityFingerprint !== expectedFingerprint) {
    throw new FleetManifestError(
      "Fleet manifest belongs to another Prism identity",
      "identity_mismatch",
    );
  }
  if (!Array.isArray(raw.workers)) {
    throw new FleetManifestError("Fleet workers must be an array", "protocol");
  }
  if (raw.workers.length > MAX_MANIFEST_WORKERS) {
    throw new FleetManifestError("Fleet worker limit is exceeded", "protocol");
  }
  if (raw.legacyMigrationCompleted !== undefined &&
    typeof raw.legacyMigrationCompleted !== "boolean") {
    throw new FleetManifestError("Fleet legacy migration flag is invalid", "protocol");
  }
  const workers = raw.workers.map(parseWorker).sort((left, right) => left.index - right.index);
  const indexes = new Set<number>();
  const projects = new Set<ProjectId>();
  for (const worker of workers) {
    if (indexes.has(worker.index)) {
      throw new FleetManifestError("Fleet worker index is duplicated", "protocol");
    }
    if (projects.has(worker.projectId)) {
      throw new FleetManifestError("Fleet worker project is duplicated", "protocol");
    }
    indexes.add(worker.index);
    projects.add(worker.projectId);
  }
  return Object.freeze({
    version: 1,
    identityKind: raw.identityKind,
    identityFingerprint: raw.identityFingerprint,
    legacyMigrationCompleted: raw.legacyMigrationCompleted === true,
    workers: Object.freeze(workers),
  });
}

function updatedWorker(
  worker: FleetWorkerRecord,
  update: Partial<Pick<
    FleetWorkerRecord,
    "state" | "updatedAtMs" | "leaderGeneration" | "retirementReason"
      | "replacementEvidence" | "hasReachedReady"
  >>,
): FleetWorkerRecord {
  return Object.freeze({ ...worker, ...update });
}

export interface FleetManifestStoreOptions {
  file: string;
  identityFingerprint: string;
  /** Optional pre-identity-scoped v1 location, copied once and never deleted. */
  legacyFile?: string;
}

export class FleetManifestStore {
  readonly #file: string;
  readonly #identityFingerprint: string;
  readonly #legacyFile: string | null;
  #chain: Promise<void> = Promise.resolve();

  constructor(options: FleetManifestStoreOptions) {
    if (!SHA256.test(options.identityFingerprint)) {
      throw new TypeError("identityFingerprint must be a lowercase SHA-256 digest");
    }
    this.#file = path.resolve(options.file);
    this.#identityFingerprint = options.identityFingerprint;
    const legacyFile = options.legacyFile?.trim();
    this.#legacyFile = legacyFile && path.resolve(legacyFile) !== this.#file
      ? path.resolve(legacyFile)
      : null;
  }

  initialize(kind: PrismIdentityKind): Promise<FleetManifest> {
    return this.#enqueue(async () => {
      const existing = await this.#readMaybe();
      if (existing) {
        if (existing.identityKind !== kind) {
          throw new FleetManifestError(
            "Fleet manifest identity kind does not match the active session",
            "identity_mismatch",
          );
        }
        return existing;
      }
      if (this.#legacyFile) {
        let legacy: FleetManifest | null = null;
        try {
          legacy = await this.#readMaybe(this.#legacyFile);
        } catch (error) {
          // A shared historical path may legitimately belong to another
          // account. Never adopt it, but do not prevent that account from
          // receiving its own identity-scoped registry.
          if (!(error instanceof FleetManifestError) || error.kind !== "identity_mismatch") {
            throw error;
          }
        }
        if (legacy) {
          if (legacy.identityKind !== kind) {
            throw new FleetManifestError(
              "Legacy fleet manifest identity kind does not match the active session",
              "identity_mismatch",
            );
          }
          await this.#write(legacy);
          return legacy;
        }
      }
      const created: FleetManifest = {
        version: 1,
        identityKind: kind,
        identityFingerprint: this.#identityFingerprint,
        legacyMigrationCompleted: false,
        workers: Object.freeze([]),
      };
      await this.#write(created);
      return created;
    });
  }

  async load(): Promise<FleetManifest> {
    await this.#chain;
    const current = await this.#readMaybe();
    if (!current) {
      throw new FleetManifestError("Fleet manifest does not exist", "protocol");
    }
    return current;
  }

  reserveWorker(input: {
    index: number;
    projectId: ProjectId;
    title: string;
    nowMs: number;
    managed?: boolean;
  }): Promise<FleetManifest> {
    if (!Number.isInteger(input.index) || input.index < 1) {
      return Promise.reject(new FleetManifestError("Worker index must be positive", "protocol"));
    }
    if (!input.title.trim()) {
      return Promise.reject(new FleetManifestError("Worker title must not be empty", "protocol"));
    }
    if (!Number.isFinite(input.nowMs) || input.nowMs < 0) {
      return Promise.reject(new FleetManifestError("Worker timestamp is invalid", "protocol"));
    }
    return this.#mutate(current => {
      const sameProject = current.workers.find(worker => worker.projectId === input.projectId);
      if (sameProject) {
        if (
          sameProject.index === input.index &&
          sameProject.title === input.title &&
          sameProject.managed === (input.managed ?? true)
        ) return current;
        throw new FleetManifestError("Worker project reservation conflicts", "conflict");
      }
      if (current.workers.some(worker => worker.index === input.index)) {
        throw new FleetManifestError("Worker index reservation conflicts", "conflict");
      }
      if (current.workers.length >= MAX_MANIFEST_WORKERS) {
        throw new FleetManifestError("Fleet worker limit is exceeded", "conflict");
      }
      const worker: FleetWorkerRecord = Object.freeze({
        index: input.index,
        projectId: input.projectId,
        title: input.title,
        state: "creating",
        createdAtMs: input.nowMs,
        updatedAtMs: input.nowMs,
        leaderGeneration: 0,
        hasReachedReady: false,
        managed: input.managed ?? true,
        retirementReason: null,
        replacementEvidence: null,
      });
      return { ...current, workers: Object.freeze([...current.workers, worker]) };
    });
  }

  markCreated(id: ProjectId, nowMs: number): Promise<FleetManifest> {
    return this.markState(id, "created", nowMs);
  }

  markState(
    id: ProjectId,
    state: FleetWorkerState,
    nowMs: number,
  ): Promise<FleetManifest> {
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      return Promise.reject(new FleetManifestError("Worker timestamp is invalid", "protocol"));
    }
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== id) return worker;
        found = true;
        if (nowMs < worker.updatedAtMs) {
          throw new FleetManifestError("Worker update time cannot move backwards", "conflict");
        }
        return updatedWorker(worker, {
          state,
          updatedAtMs: nowMs,
          ...(state === "draining" ? {} : { retirementReason: null }),
          ...(state === "ready"
            ? { hasReachedReady: true, replacementEvidence: null }
            : state === "draining"
              ? { replacementEvidence: null }
              : {}),
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  markDraining(
    id: ProjectId,
    reason: FleetRetirementReason,
    nowMs: number,
  ): Promise<FleetManifest> {
    if (!RETIREMENT_REASONS.has(reason)) {
      return Promise.reject(new FleetManifestError("Retirement reason is invalid", "protocol"));
    }
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      return Promise.reject(new FleetManifestError("Worker timestamp is invalid", "protocol"));
    }
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== id) return worker;
        found = true;
        if (nowMs < worker.updatedAtMs) {
          throw new FleetManifestError("Worker update time cannot move backwards", "conflict");
        }
        return updatedWorker(worker, {
          state: "draining",
          retirementReason: reason,
          replacementEvidence: null,
          updatedAtMs: nowMs,
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  recordReplacementFailure(input: {
    id: ProjectId;
    signatureHash: string;
    kind: FleetReplacementFailureKind;
    observedAtMs: number;
    attempt: number;
    incarnation: number;
    leaderGeneration: number;
    consecutiveFailures: number;
    totalDurationMs: number;
  }): Promise<FleetManifest> {
    if (!SHA256.test(input.signatureHash) || !REPLACEMENT_FAILURE_KINDS.has(input.kind)) {
      return Promise.reject(new FleetManifestError("Replacement failure identity is invalid", "protocol"));
    }
    if (!Number.isFinite(input.observedAtMs) || input.observedAtMs < 0 ||
      !Number.isSafeInteger(input.attempt) || input.attempt < 0 ||
      !Number.isSafeInteger(input.incarnation) || input.incarnation < 0 ||
      !Number.isSafeInteger(input.leaderGeneration) || input.leaderGeneration < 1 ||
      !Number.isSafeInteger(input.consecutiveFailures) || input.consecutiveFailures < 1 ||
      !Number.isFinite(input.totalDurationMs) || input.totalDurationMs < 0) {
      return Promise.reject(new FleetManifestError("Replacement failure counters are invalid", "protocol"));
    }
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== input.id) return worker;
        found = true;
        if (worker.state === "draining") return worker;
        const previous = worker.replacementEvidence;
        const same = previous?.signatureHash === input.signatureHash &&
          previous.kind === input.kind;
        const duplicate = same &&
          previous.lastLeaderGeneration === input.leaderGeneration &&
          previous.lastAttempt === input.attempt &&
          previous.lastIncarnation === input.incarnation;
        const nextCount = duplicate
          ? previous.count
          : same
            ? Math.min(100, previous.count + 1)
            : 1;
        const evidence: FleetReplacementEvidence = Object.freeze({
          signatureHash: input.signatureHash,
          kind: input.kind,
          count: nextCount,
          firstObservedAtMs: same ? previous.firstObservedAtMs : input.observedAtMs,
          lastObservedAtMs: same
            ? Math.max(previous.lastObservedAtMs, input.observedAtMs)
            : input.observedAtMs,
          lastAttempt: input.attempt,
          lastIncarnation: input.incarnation,
          lastLeaderGeneration: input.leaderGeneration,
          totalDurationMs: same
            ? Math.max(previous.totalDurationMs, input.totalDurationMs)
            : input.totalDurationMs,
        });
        return updatedWorker(worker, {
          replacementEvidence: evidence,
          updatedAtMs: Math.max(worker.updatedAtMs, input.observedAtMs),
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  clearReplacementFailure(id: ProjectId, nowMs: number): Promise<FleetManifest> {
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      return Promise.reject(new FleetManifestError("Worker timestamp is invalid", "protocol"));
    }
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== id) return worker;
        found = true;
        if (worker.replacementEvidence === null) return worker;
        return updatedWorker(worker, {
          replacementEvidence: null,
          updatedAtMs: Math.max(worker.updatedAtMs, nowMs),
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  nextLeaderGeneration(id: ProjectId, nowMs = Date.now()): Promise<number> {
    if (!Number.isFinite(nowMs) || nowMs < 0) {
      return Promise.reject(new FleetManifestError("Worker timestamp is invalid", "protocol"));
    }
    let generation = 0;
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== id) return worker;
        found = true;
        generation = worker.leaderGeneration + 1;
        return updatedWorker(worker, {
          leaderGeneration: generation,
          updatedAtMs: Math.max(worker.updatedAtMs, nowMs),
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    }).then(() => generation);
  }

  completeLegacyMigration(): Promise<FleetManifest> {
    return this.#mutate(current => current.legacyMigrationCompleted
      ? current
      : { ...current, legacyMigrationCompleted: true });
  }

  removeImported(id: ProjectId): Promise<FleetManifest> {
    return this.#mutate(current => {
      const selected = current.workers.find(worker => worker.projectId === id);
      if (selected?.managed) {
        throw new FleetManifestError(
          "Managed project requires confirmed remote deletion",
          "conflict",
        );
      }
      if (selected && selected.state !== "draining") {
        throw new FleetManifestError(
          "Imported project must be draining before retirement",
          "conflict",
        );
      }
      const workers = current.workers.filter(worker => worker.projectId !== id);
      if (workers.length === current.workers.length) {
        throw new FleetManifestError("Imported worker was not found", "missing_worker");
      }
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  removeManaged(id: ProjectId): Promise<FleetManifest> {
    return this.#mutate(current => {
      const selected = current.workers.find(worker => worker.projectId === id);
      if (selected && !selected.managed) {
        throw new FleetManifestError(
          "Explicit project is not eligible for managed deletion",
          "conflict",
        );
      }
      const workers = current.workers.filter(worker => worker.projectId !== id);
      if (workers.length === current.workers.length) {
        throw new FleetManifestError("Managed worker was not found", "missing_worker");
      }
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  #mutate(
    operation: (current: FleetManifest) => FleetManifest,
  ): Promise<FleetManifest> {
    return this.#enqueue(async () => {
      const current = await this.#readMaybe();
      if (!current) throw new FleetManifestError("Fleet manifest does not exist", "protocol");
      const next = parseManifest(operation(current), this.#identityFingerprint);
      await this.#write(next);
      return next;
    });
  }

  #enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const pending = this.#chain.then(operation, operation);
    this.#chain = pending.then(() => undefined, () => undefined);
    return pending;
  }

  async #readMaybe(file = this.#file): Promise<FleetManifest | null> {
    let raw: string;
    try {
      raw = await readFile(file, "utf8");
    } catch (error) {
      if (isNodeError(error, "ENOENT")) return null;
      throw error;
    }
    let decoded: unknown;
    try {
      decoded = JSON.parse(raw);
    } catch {
      throw new FleetManifestError("Fleet manifest JSON is invalid", "protocol");
    }
    return parseManifest(decoded, this.#identityFingerprint);
  }

  async #write(manifest: FleetManifest): Promise<void> {
    const directory = path.dirname(this.#file);
    await mkdir(directory, { recursive: true });
    const temporary = path.join(
      directory,
      `.${path.basename(this.#file)}.${process.pid}.${randomUUID()}.tmp`,
    );
    const handle = await open(temporary, "wx", 0o600);
    let renamed = false;
    try {
      await handle.writeFile(JSON.stringify(manifest, null, 2));
      await handle.sync();
      await handle.close();
      await rename(temporary, this.#file);
      renamed = true;
      if (process.platform !== "win32") {
        await chmod(this.#file, 0o600);
        const directoryHandle = await open(directory, "r");
        try {
          await directoryHandle.sync();
        } finally {
          await directoryHandle.close();
        }
      }
    } finally {
      await handle.close().catch(() => undefined);
      if (!renamed) await unlink(temporary).catch(() => undefined);
    }
  }
}
