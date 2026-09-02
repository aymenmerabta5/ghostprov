import {
  isAuthenticatedTenantScope,
  type AuthenticatedTenantScope,
} from "../http/api-key";
import type { ProjectId } from "../sandbox/types";

const KiB = 1024;
const MiB = KiB ** 2;
const HOUR_MS = 60 * 60 * 1_000;
const RECORD_FIXED_BYTES = 160;
const SCOPE_DIGEST_BYTES = 32;

/** Fixed process-wide policy for content-free provider response provenance. */
export const RESPONSE_PROVENANCE_LIMITS = Object.freeze({
  maxResponseIdBytes: 512,
  maxProjectIdBytes: 256,
  maxRecords: 4_096,
  maxBytes: 1 * MiB,
  ttlMs: HOUR_MS,
  maxRecordBytes: RECORD_FIXED_BYTES + 512 + 256 + SCOPE_DIGEST_BYTES,
});

export interface ResponseProvenance {
  readonly projectId: ProjectId;
  readonly incarnation: number;
}

export type ResponseProvenanceLookup =
  | Readonly<{ kind: "unknown" }>
  | Readonly<{ kind: "scope_mismatch" }>
  | Readonly<{ kind: "known"; provenance: ResponseProvenance }>;

const UNKNOWN_PROVENANCE = Object.freeze({ kind: "unknown" as const });
const SCOPE_MISMATCH = Object.freeze({ kind: "scope_mismatch" as const });

interface ProvenanceEntry {
  readonly responseId: string;
  readonly scope: AuthenticatedTenantScope;
  readonly provenance: ResponseProvenance;
  readonly bytes: number;
  readonly expiresAtMs: number;
  heapIndex: number;
}

declare const RESPONSE_PROVENANCE_RESERVATION: unique symbol;

/** Opaque one-shot capacity held before any provider execution can begin. */
export type ResponseProvenanceReservation = Readonly<{
  readonly [RESPONSE_PROVENANCE_RESERVATION]: true;
}>;

interface ReservationState {
  active: boolean;
  readonly generation: number;
}

export interface ResponseProvenanceSnapshot {
  readonly records: number;
  readonly bytes: number;
  readonly reservedRecords: number;
  readonly reservedBytes: number;
  readonly maxRecords: number;
  readonly maxBytes: number;
  readonly maxResponseIdBytes: number;
  readonly maxProjectIdBytes: number;
  readonly maxRecordBytes: number;
  readonly ttlMs: number;
}

function boundedUtf8(value: unknown, maxBytes: number): value is string {
  return typeof value === "string" && value.length > 0 &&
    value.length <= maxBytes && Buffer.byteLength(value, "utf8") <= maxBytes;
}

/**
 * Bounded registry for provider continuation affinity. Entries contain only an
 * opaque scope reference plus routing metadata; prompts, outputs, headers, and
 * raw authorization material never enter this registry.
 *
 * This is a tenant-scoped routing hint, not an authorization database. Provider
 * authentication remains authoritative for unknown or expired IDs. Reservations
 * bound concurrent pending terminal commits without evicting retained hints.
 * After immutable conflict checks, commit evicts only the oldest hints needed for
 * the exact new record, so metadata pressure cannot become a process-wide quota.
 * Conflicting live bindings remain immutable.
 */
export class ResponseProvenanceRegistry {
  readonly #nowSource: () => number;
  readonly #entries = new Map<string, ProvenanceEntry>();
  readonly #expiryHeap: ProvenanceEntry[] = [];
  readonly #reservations = new WeakMap<object, ReservationState>();
  #bytes = 0;
  #reservedRecords = 0;
  #reservedBytes = 0;
  #reservationGeneration = 1;

  constructor(now: () => number) {
    this.#nowSource = now;
  }

  lookup(
    responseId: unknown,
    scope: AuthenticatedTenantScope,
  ): ResponseProvenanceLookup {
    if (!isAuthenticatedTenantScope(scope)) return SCOPE_MISMATCH;
    const now = this.#now();
    this.#pruneExpired(now);
    if (!boundedUtf8(responseId, RESPONSE_PROVENANCE_LIMITS.maxResponseIdBytes)) {
      return UNKNOWN_PROVENANCE;
    }
    const entry = this.#entries.get(responseId);
    if (!entry) return UNKNOWN_PROVENANCE;
    if (entry.scope !== scope) return SCOPE_MISMATCH;
    return Object.freeze({ kind: "known" as const, provenance: entry.provenance });
  }

  /** Reserves one concurrent terminal commit without mutating retained hints. */
  reserve(): ResponseProvenanceReservation | null {
    const bytes = RESPONSE_PROVENANCE_LIMITS.maxRecordBytes;
    if (
      this.#reservedRecords >= RESPONSE_PROVENANCE_LIMITS.maxRecords ||
      this.#reservedBytes + bytes > RESPONSE_PROVENANCE_LIMITS.maxBytes
    ) return null;

    const reservation = Object.freeze(Object.create(null)) as ResponseProvenanceReservation;
    this.#reservations.set(reservation, {
      active: true,
      generation: this.#reservationGeneration,
    });
    this.#reservedRecords++;
    this.#reservedBytes += bytes;
    return reservation;
  }

  /**
   * Atomically consumes a pre-execution reservation. False means the response
   * ID was malformed or already had a conflicting immutable binding.
   */
  commit(
    reservation: ResponseProvenanceReservation,
    responseId: unknown,
    scope: AuthenticatedTenantScope,
    provenance: ResponseProvenance,
  ): boolean {
    const state = this.#reservationState(reservation);
    if (!state) return false;
    const now = this.#now();
    this.#pruneExpired(now);

    if (
      !boundedUtf8(responseId, RESPONSE_PROVENANCE_LIMITS.maxResponseIdBytes) ||
      !isAuthenticatedTenantScope(scope) ||
      !boundedUtf8(provenance.projectId, RESPONSE_PROVENANCE_LIMITS.maxProjectIdBytes) ||
      !Number.isSafeInteger(provenance.incarnation) ||
      provenance.incarnation < 0
    ) {
      this.#releaseState(state);
      return false;
    }

    const existing = this.#entries.get(responseId);
    if (existing) {
      const matches = existing.scope === scope &&
        existing.provenance.projectId === provenance.projectId &&
        existing.provenance.incarnation === provenance.incarnation;
      this.#releaseState(state);
      return matches;
    }

    const bytes = RECORD_FIXED_BYTES + SCOPE_DIGEST_BYTES +
      Buffer.byteLength(responseId, "utf8") +
      Buffer.byteLength(provenance.projectId, "utf8");
    if (bytes > RESPONSE_PROVENANCE_LIMITS.maxRecordBytes) {
      this.#releaseState(state);
      return false;
    }

    // Conflict checks run before any eviction. Once the authoritative ID is
    // known, replace only the oldest routing hints needed for this exact record.
    this.#releaseState(state);
    while (
      this.#expiryHeap[0] &&
      (
        this.#entries.size >= RESPONSE_PROVENANCE_LIMITS.maxRecords ||
        this.#bytes + bytes > RESPONSE_PROVENANCE_LIMITS.maxBytes
      )
    ) this.#remove(this.#expiryHeap[0]!);
    if (
      this.#entries.size >= RESPONSE_PROVENANCE_LIMITS.maxRecords ||
      this.#bytes + bytes > RESPONSE_PROVENANCE_LIMITS.maxBytes
    ) return false;
    const immutable = Object.freeze({
      projectId: provenance.projectId,
      incarnation: provenance.incarnation,
    });
    const entry: ProvenanceEntry = {
      responseId,
      scope,
      provenance: immutable,
      bytes,
      expiresAtMs: now + RESPONSE_PROVENANCE_LIMITS.ttlMs,
      heapIndex: this.#expiryHeap.length,
    };
    this.#entries.set(responseId, entry);
    this.#bytes += bytes;
    this.#expiryHeap.push(entry);
    this.#bubbleUp(entry.heapIndex);
    return true;
  }

  release(reservation: ResponseProvenanceReservation | null | undefined): void {
    if (!reservation || typeof reservation !== "object") return;
    const state = this.#reservationState(reservation);
    if (state) this.#releaseState(state);
  }

  snapshot(): ResponseProvenanceSnapshot {
    this.#pruneExpired(this.#now());
    return Object.freeze({
      records: this.#entries.size,
      bytes: this.#bytes,
      reservedRecords: this.#reservedRecords,
      reservedBytes: this.#reservedBytes,
      ...RESPONSE_PROVENANCE_LIMITS,
    });
  }

  /** Rare fleet-maintenance query; bounded by maxRecords and never on routing hot paths. */
  bindingsForProject(projectId: ProjectId): number {
    this.#pruneExpired(this.#now());
    let count = 0;
    for (const entry of this.#entries.values()) {
      if (entry.provenance.projectId === projectId) count++;
    }
    return count;
  }

  clear(): void {
    this.#entries.clear();
    this.#expiryHeap.length = 0;
    this.#bytes = 0;
    this.#reservedRecords = 0;
    this.#reservedBytes = 0;
    this.#reservationGeneration++;
  }

  #reservationState(
    reservation: ResponseProvenanceReservation,
  ): ReservationState | null {
    if (!reservation || typeof reservation !== "object") return null;
    const state = this.#reservations.get(reservation);
    return state?.active === true && state.generation === this.#reservationGeneration
      ? state
      : null;
  }

  #releaseState(state: ReservationState): void {
    if (!state.active || state.generation !== this.#reservationGeneration) return;
    state.active = false;
    this.#reservedRecords = Math.max(0, this.#reservedRecords - 1);
    this.#reservedBytes = Math.max(
      0,
      this.#reservedBytes - RESPONSE_PROVENANCE_LIMITS.maxRecordBytes,
    );
  }

  #now(): number {
    const now = this.#nowSource();
    if (!Number.isFinite(now) || now < 0) {
      throw new RangeError("Response provenance clock must be finite and non-negative");
    }
    return now;
  }

  #pruneExpired(now: number): void {
    while (this.#expiryHeap[0] && this.#expiryHeap[0]!.expiresAtMs <= now) {
      this.#remove(this.#expiryHeap[0]!);
    }
  }

  #remove(entry: ProvenanceEntry): void {
    if (this.#entries.get(entry.responseId) !== entry) return;
    this.#entries.delete(entry.responseId);
    this.#bytes = Math.max(0, this.#bytes - entry.bytes);
    this.#removeHeapAt(entry.heapIndex);
    entry.heapIndex = -1;
  }

  #before(left: ProvenanceEntry, right: ProvenanceEntry): boolean {
    return left.expiresAtMs < right.expiresAtMs ||
      (left.expiresAtMs === right.expiresAtMs && left.responseId < right.responseId);
  }

  #swapHeap(left: number, right: number): void {
    const leftEntry = this.#expiryHeap[left]!;
    const rightEntry = this.#expiryHeap[right]!;
    this.#expiryHeap[left] = rightEntry;
    this.#expiryHeap[right] = leftEntry;
    rightEntry.heapIndex = left;
    leftEntry.heapIndex = right;
  }

  #bubbleUp(start: number): void {
    for (let index = start; index > 0;) {
      const parent = Math.floor((index - 1) / 2);
      if (!this.#before(this.#expiryHeap[index]!, this.#expiryHeap[parent]!)) return;
      this.#swapHeap(index, parent);
      index = parent;
    }
  }

  #bubbleDown(start: number): void {
    for (let index = start;;) {
      const left = index * 2 + 1;
      if (left >= this.#expiryHeap.length) return;
      const right = left + 1;
      let child = left;
      if (
        right < this.#expiryHeap.length &&
        this.#before(this.#expiryHeap[right]!, this.#expiryHeap[left]!)
      ) child = right;
      if (!this.#before(this.#expiryHeap[child]!, this.#expiryHeap[index]!)) return;
      this.#swapHeap(index, child);
      index = child;
    }
  }

  #removeHeapAt(index: number): void {
    if (index < 0 || index >= this.#expiryHeap.length) return;
    const last = this.#expiryHeap.pop()!;
    if (index >= this.#expiryHeap.length) return;
    this.#expiryHeap[index] = last;
    last.heapIndex = index;
    const parent = Math.floor((index - 1) / 2);
    if (index > 0 && this.#before(last, this.#expiryHeap[parent]!)) {
      this.#bubbleUp(index);
    } else {
      this.#bubbleDown(index);
    }
  }
}

export class ResponseProvenanceScopeError extends Error {
  readonly name = "ResponseProvenanceScopeError";
  readonly code = "response_provenance_scope_mismatch";
  readonly status = 403;
  readonly committed = false;

  constructor() {
    super("Response continuation lacks matching authenticated tenant authority");
  }
}

export class ResponseProvenanceObsoleteError extends Error {
  readonly name = "ResponseProvenanceObsoleteError";
  readonly code = "response_provenance_obsolete";
  readonly status = 409;
  readonly committed = false;

  constructor() {
    super("Response continuation is bound to an obsolete sandbox incarnation");
  }
}

export class ResponseProvenanceCommitError extends Error {
  readonly name = "ResponseProvenanceCommitError";
  readonly code = "response_provenance_commit_failed";
  readonly status = 502;
  readonly committed = true;
  readonly ambiguous = false;

  constructor() {
    super("Completed response provenance could not be committed");
  }
}
