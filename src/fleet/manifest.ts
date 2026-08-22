import { randomUUID } from "node:crypto";
import {
  chmod,
  mkdir,
  open,
  readFile,
  rename,
  unlink,
} from "node:fs/promises";
import * as path from "node:path";
import type { PrismIdentityKind } from "../prism/session-manager";
import { projectId, type ProjectId } from "../sandbox/types";
import type {
  FleetManifest,
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

const STATES = new Set<FleetWorkerState>([
  "creating",
  "created",
  "ready",
  "draining",
  "failed",
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
  if (raw.managed !== true) {
    throw new FleetManifestError("Fleet worker is not managed", "protocol");
  }
  if (!Number.isInteger(raw.leaderGeneration) || (raw.leaderGeneration as number) < 0) {
    throw new FleetManifestError("Fleet worker leader generation is invalid", "protocol");
  }
  return Object.freeze({
    index: raw.index as number,
    projectId: projectId(raw.projectId),
    title: raw.title,
    state: raw.state as FleetWorkerState,
    createdAtMs: finiteNonnegative(raw.createdAtMs, "Fleet worker creation time"),
    updatedAtMs: finiteNonnegative(raw.updatedAtMs, "Fleet worker update time"),
    leaderGeneration: raw.leaderGeneration as number,
    managed: true,
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
  if (typeof raw.identityFingerprint !== "string" || !/^[a-f0-9]{64}$/.test(raw.identityFingerprint)) {
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
    workers: Object.freeze(workers),
  });
}

function updatedWorker(
  worker: FleetWorkerRecord,
  update: Partial<Pick<FleetWorkerRecord, "state" | "updatedAtMs" | "leaderGeneration">>,
): FleetWorkerRecord {
  return Object.freeze({ ...worker, ...update });
}

export interface FleetManifestStoreOptions {
  file: string;
  identityFingerprint: string;
}

export class FleetManifestStore {
  readonly #file: string;
  readonly #identityFingerprint: string;
  #chain: Promise<void> = Promise.resolve();

  constructor(options: FleetManifestStoreOptions) {
    if (!/^[a-f0-9]{64}$/.test(options.identityFingerprint)) {
      throw new TypeError("identityFingerprint must be a lowercase SHA-256 digest");
    }
    this.#file = path.resolve(options.file);
    this.#identityFingerprint = options.identityFingerprint;
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
      const created: FleetManifest = {
        version: 1,
        identityKind: kind,
        identityFingerprint: this.#identityFingerprint,
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
        if (sameProject.index === input.index && sameProject.title === input.title) return current;
        throw new FleetManifestError("Worker project reservation conflicts", "conflict");
      }
      if (current.workers.some(worker => worker.index === input.index)) {
        throw new FleetManifestError("Worker index reservation conflicts", "conflict");
      }
      const worker: FleetWorkerRecord = Object.freeze({
        index: input.index,
        projectId: input.projectId,
        title: input.title,
        state: "creating",
        createdAtMs: input.nowMs,
        updatedAtMs: input.nowMs,
        leaderGeneration: 0,
        managed: true,
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
        return updatedWorker(worker, { state, updatedAtMs: nowMs });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    });
  }

  nextLeaderGeneration(id: ProjectId): Promise<number> {
    let generation = 0;
    return this.#mutate(current => {
      let found = false;
      const workers = current.workers.map(worker => {
        if (worker.projectId !== id) return worker;
        found = true;
        generation = worker.leaderGeneration + 1;
        return updatedWorker(worker, {
          leaderGeneration: generation,
          updatedAtMs: Math.max(worker.updatedAtMs, Date.now()),
        });
      });
      if (!found) throw new FleetManifestError("Managed worker was not found", "missing_worker");
      return { ...current, workers: Object.freeze(workers) };
    }).then(() => generation);
  }

  removeManaged(id: ProjectId): Promise<FleetManifest> {
    return this.#mutate(current => {
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

  async #readMaybe(): Promise<FleetManifest | null> {
    let raw: string;
    try {
      raw = await readFile(this.#file, "utf8");
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
