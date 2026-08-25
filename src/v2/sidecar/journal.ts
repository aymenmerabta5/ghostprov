import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import {
  parseProviderErrorEnvelope,
  PROVIDER_ERROR_ENVELOPE_MAX_BYTES,
  type ProviderErrorEnvelope,
} from "../provider-error";
import { canonicalJson, utf8Bytes, type UpstreamPrelude } from "../protocol/shared";
import {
  MAX_RATE_LIMIT_DELAY_MS,
  type RateLimitHint,
} from "../rate-limit";

export type DefinitiveOutcomeState = "provider_error" | "cancelled" | "forced_ambiguous";
export type ClaimState =
  | "claimed"
  | "fetch_starting"
  | "terminal"
  | DefinitiveOutcomeState
  | "acked";

export interface JournalProviderError {
  status?: number;
  code: string;
  message: string;
  rateLimit?: RateLimitHint;
  providerError?: ProviderErrorEnvelope;
}

export interface JournalForcedAmbiguous {
  code: string;
  message: string;
}

export type JournalDefinitiveOutcome =
  | { kind: "provider_error"; error: JournalProviderError }
  | { kind: "cancelled" }
  | { kind: "forced_ambiguous"; error: JournalForcedAmbiguous };

export interface RecoveredProviderError {
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  error: JournalProviderError;
  prelude?: UpstreamPrelude;
}

export interface RecoveredForcedAmbiguous {
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  error: JournalForcedAmbiguous;
}

export interface RecoveredJournalOutcome {
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  outcome: JournalDefinitiveOutcome;
  prelude?: UpstreamPrelude;
}

export interface JournalRecord {
  version: 1;
  sequence: number;
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  state: ClaimState;
  recordedAtMs: number;
  responseId?: string;
  terminalDigest?: string;
  outcome?: JournalDefinitiveOutcome;
  prelude?: UpstreamPrelude;
  /** One state snapshot emitted only by an exclusive atomic compaction. */
  compacted?: true;
}

export const V2_JOURNAL_MAX_BYTES = 64 * 1024 * 1024;
export const V2_JOURNAL_MAX_RECORDS = 100_000;
/** Provider envelope plus record identity/outcome/rate-limit framing. */
export const V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES =
  PROVIDER_ERROR_ENVELOPE_MAX_BYTES + 4 * 1024;
const V2_JOURNAL_SMALL_TRANSITION_BYTES = 1024;
const JOURNAL_CHECKSUM_PLACEHOLDER = "0".repeat(64);

/** Exact bytes written by DenoJournalStorage for one checksummed JSONL record. */
export function journalStoredRecordBytes(record: JournalRecord): number {
  return utf8Bytes(JSON.stringify({
    record,
    checksum: JOURNAL_CHECKSUM_PLACEHOLDER,
  }) + "\n").byteLength;
}

export interface TerminalSpoolProof {
  byteCount: number;
  transcriptSha256: string;
}

export interface JournalTerminalSpool {
  readonly requestId: string;
  readonly maxBytes: number;
  readonly byteCount: number;
  append(bytes: Uint8Array): Promise<void>;
  promote(proof: TerminalSpoolProof): Promise<void>;
  abort(): Promise<void>;
}

export class JournalStorageCapacityError extends Error {
  readonly name = "JournalStorageCapacityError";
}

/** A namespace mutation occurred but its directory entry was not proven durable. */
export class JournalStorageDurabilityError extends Error {
  readonly name = "JournalStorageDurabilityError";
}

/** One immutable terminal file handle. Reads are fixed-size and rewindable. */
export interface JournalTerminalReader {
  readonly requestId: string;
  readonly byteCount: number;
  chunks(signal?: AbortSignal): AsyncIterable<Uint8Array>;
  rewind(): Promise<void>;
  close(): void;
}

function memoryTerminalReader(
  requestId: string,
  bytes: Uint8Array,
): JournalTerminalReader {
  let closed = false;
  let reading = false;
  return {
    requestId,
    byteCount: bytes.byteLength,
    async *chunks(signal?: AbortSignal) {
      if (closed) throw new Error("terminal reader is closed");
      if (reading) throw new Error("terminal reader is already active");
      reading = true;
      try {
        for (let offset = 0; offset < bytes.byteLength; offset += 64 * 1024) {
          if (signal?.aborted) {
            throw signal.reason ?? new DOMException("aborted", "AbortError");
          }
          yield bytes.subarray(offset, offset + 64 * 1024);
        }
      } finally {
        reading = false;
      }
    },
    async rewind() {
      if (closed || reading) throw new Error("terminal reader cannot rewind");
    },
    close() { closed = true; },
  };
}

export interface JournalStorage {
  /**
   * Resolve only after records are durable. Implementations must not reject for
   * optional permission/cleanup work after that boundary.
   */
  appendAndSync(records: readonly JournalRecord[]): Promise<void>;
  /**
   * Atomically replace the append log with an equivalent compacted history.
   * Implementations expose this only when they can hold their exclusive
   * storage-ownership lease for the complete rewrite.
   */
  replaceRecords?(records: readonly JournalRecord[]): Promise<void>;
  beginTerminalSpool(requestId: string, maxBytes: number): Promise<JournalTerminalSpool>;
  writeTerminal(requestId: string, bytes: Uint8Array): Promise<void>;
  readTerminal(requestId: string, maxBytes?: number): Promise<Uint8Array | null>;
  openTerminal?(requestId: string, maxBytes?: number): Promise<JournalTerminalReader | null>;
  /** Existence-only recovery scan; production storage must avoid reading spool bytes. */
  terminalExists(requestId: string): Promise<boolean>;
  deleteTerminal(requestId: string): Promise<void>;
  /** Exclusively remove terminal files that are not named in the retained set. */
  pruneTerminals?(retainedRequestIds: readonly string[]): Promise<void>;
  readRecords(): Promise<readonly JournalRecord[]>;
}

export class ClaimJournalError extends Error {
  readonly name = "ClaimJournalError";

  constructor(
    message: string,
    readonly code: "capacity" | "conflict" | "corrupt" | "missing" | "storage" | "transition",
  ) {
    super(message);
  }
}

interface ClaimRuntimeState {
  requestId: string;
  requestDigest: string;
  leaderGeneration: number;
  state: ClaimState;
  responseId?: string;
  terminalDigest?: string;
  outcome?: JournalDefinitiveOutcome;
  prelude?: UpstreamPrelude;
  records: JournalRecord[];
}

interface PendingRecord {
  record: JournalRecord;
  apply(): void;
  resolve(): void;
  reject(error: unknown): void;
}

const SAFE_REQUEST = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SHA256 = /^[a-f0-9]{64}$/;
const SAFE_CODE = /^[A-Za-z][A-Za-z0-9._-]{0,63}$/;
const SAFE_UPSTREAM_REQUEST = /^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$/;
const RATE_LIMIT_KEYS = new Set([
  "retryAfterMs",
  "resetTokensMs",
  "limitTokens",
  "remainingTokens",
  "limitProjectTokens",
  "remainingProjectTokens",
  "resetProjectTokensMs",
]);
const JOURNAL_RECORD_KEYS = new Set([
  "version",
  "sequence",
  "requestId",
  "requestDigest",
  "leaderGeneration",
  "state",
  "recordedAtMs",
  "responseId",
  "terminalDigest",
  "outcome",
  "prelude",
  "compacted",
]);

function safeMessage(value: unknown, fallback: string): string {
  const raw = typeof value === "string" && value.trim() ? value.trim() : fallback;
  return raw
    .replace(/[\u0000-\u001f\u007f]/g, " ")
    .replace(/\b(?:Bearer|Basic|Digest)\s+[^\s,;]+/gi, match => `${match.split(/\s+/, 1)[0]} [REDACTED]`)
    .replace(/(\b(?:authorization|cookie|set-cookie|api[_-]?key|secret|password|access[_-]?token)\s*[:=]\s*)[^\s,;]+/gi, "$1[REDACTED]")
    .replace(/[A-Za-z0-9+/_=-]{48,}/g, "[REDACTED]")
    .trim()
    .slice(0, 512) || fallback;
}

function normalizedRateLimit(value: RateLimitHint | undefined): RateLimitHint | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ClaimJournalError("Provider rate-limit hint is invalid", "transition");
  }
  for (const key of Object.keys(value)) {
    if (!RATE_LIMIT_KEYS.has(key)) {
      throw new ClaimJournalError("Provider rate-limit hint has unknown fields", "transition");
    }
  }
  const result: RateLimitHint = {};
  for (const key of RATE_LIMIT_KEYS) {
    if (!Object.prototype.hasOwnProperty.call(value, key)) continue;
    const candidate = (value as Record<string, unknown>)[key];
    if (candidate === undefined) continue;
    const isDelay = key.endsWith("Ms");
    if (
      typeof candidate !== "number" ||
      !Number.isSafeInteger(candidate) ||
      candidate < 0 ||
      (isDelay && candidate > MAX_RATE_LIMIT_DELAY_MS)
    ) throw new ClaimJournalError("Provider rate-limit hint value is invalid", "transition");
    (result as Record<string, number>)[key] = candidate;
  }
  return Object.keys(result).length ? result : undefined;
}

function normalizedPrelude(value: UpstreamPrelude | undefined): UpstreamPrelude | undefined {
  if (value === undefined) return undefined;
  if (
    !value || typeof value !== "object" || Array.isArray(value) ||
    !Number.isSafeInteger(value.status) || value.status < 200 || value.status > 299 ||
    typeof value.contentType !== "string" || value.contentType.length < 1 ||
    utf8Bytes(value.contentType).byteLength > 256 ||
    /[\u0000-\u001f\u007f]/.test(value.contentType) ||
    !(value.upstreamRequestId === null || (
      typeof value.upstreamRequestId === "string" &&
      SAFE_UPSTREAM_REQUEST.test(value.upstreamRequestId)
    ))
  ) throw new ClaimJournalError("Upstream prelude is invalid", "transition");
  return {
    status: value.status,
    contentType: value.contentType,
    upstreamRequestId: value.upstreamRequestId,
  };
}

function normalizedProviderError(value: JournalProviderError): JournalProviderError {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ClaimJournalError("Provider error is invalid", "transition");
  }
  for (const key of Object.keys(value)) {
    if (!["status", "code", "message", "rateLimit", "providerError"].includes(key)) {
      throw new ClaimJournalError("Provider error has unknown fields", "transition");
    }
  }
  const status = value.status;
  if (status !== undefined && (!Number.isSafeInteger(status) || status < 400 || status > 599)) {
    throw new ClaimJournalError("Provider error status is invalid", "transition");
  }
  const code = SAFE_CODE.test(value.code) ? value.code : "upstream_error";
  const message = safeMessage(value.message, "Upstream provider request failed");
  const rateLimit = normalizedRateLimit(value.rateLimit);
  let providerError: ProviderErrorEnvelope | undefined;
  if (value.providerError !== undefined) {
    const parsed = parseProviderErrorEnvelope(value.providerError);
    if (!parsed) {
      throw new ClaimJournalError("Provider error envelope is invalid", "transition");
    }
    try {
      // Do not retain caller-owned nested objects in a record that represents a
      // durable boundary. Re-validate the clone so exotic/non-cloneable input
      // can never be silently persisted without the envelope's byte bounds.
      const cloned = structuredClone(parsed);
      providerError = parseProviderErrorEnvelope(cloned) ?? undefined;
    } catch {
      providerError = undefined;
    }
    if (!providerError) {
      throw new ClaimJournalError("Provider error envelope is invalid", "transition");
    }
  }
  return {
    ...(status === undefined ? {} : { status }),
    code,
    message,
    ...(rateLimit ? { rateLimit } : {}),
    ...(providerError ? { providerError } : {}),
  };
}

function normalizedForcedAmbiguous(value: JournalForcedAmbiguous): JournalForcedAmbiguous {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new ClaimJournalError("Forced ambiguous outcome is invalid", "transition");
  }
  for (const key of Object.keys(value)) {
    if (key !== "code" && key !== "message") {
      throw new ClaimJournalError("Forced ambiguous outcome has unknown fields", "transition");
    }
  }
  return {
    code: SAFE_CODE.test(value.code) ? value.code : "forced_ambiguous",
    message: safeMessage(value.message, "Execution ended without a proven terminal"),
  };
}

function validOutcomeShape(state: ClaimState, outcome: JournalDefinitiveOutcome | undefined): boolean {
  if (state === "provider_error") {
    if (
      outcome?.kind !== state ||
      Object.keys(outcome).some(key => key !== "kind" && key !== "error") ||
      Object.keys(outcome).length !== 2
    ) return false;
    try {
      return canonicalJson(normalizedProviderError(outcome.error)) === canonicalJson(outcome.error);
    } catch {
      return false;
    }
  }
  if (state === "cancelled") {
    return outcome?.kind === state && Object.keys(outcome).length === 1;
  }
  if (state === "forced_ambiguous") {
    if (
      outcome?.kind !== state ||
      Object.keys(outcome).some(key => key !== "kind" && key !== "error") ||
      Object.keys(outcome).length !== 2
    ) return false;
    try {
      return canonicalJson(normalizedForcedAmbiguous(outcome.error)) === canonicalJson(outcome.error);
    } catch {
      return false;
    }
  }
  return outcome === undefined;
}

function outcomesEqual(
  left: JournalDefinitiveOutcome | undefined,
  right: JournalDefinitiveOutcome,
): boolean {
  return left !== undefined && canonicalJson(left) === canonicalJson(right);
}

function validRecordShape(record: JournalRecord): void {
  if (
    Object.keys(record).some(key => !JOURNAL_RECORD_KEYS.has(key)) ||
    record.version !== 1 ||
    !Number.isSafeInteger(record.sequence) ||
    record.sequence < 1 ||
    !SAFE_REQUEST.test(record.requestId) ||
    !SHA256.test(record.requestDigest) ||
    !Number.isSafeInteger(record.leaderGeneration) ||
    record.leaderGeneration < 1 ||
    ![
      "claimed",
      "fetch_starting",
      "terminal",
      "provider_error",
      "cancelled",
      "forced_ambiguous",
      "acked",
    ].includes(record.state) ||
    typeof record.recordedAtMs !== "number" ||
    !Number.isFinite(record.recordedAtMs) ||
    record.recordedAtMs < 0 ||
    (record.compacted !== undefined && record.compacted !== true)
  ) {
    throw new ClaimJournalError("Journal record is malformed", "corrupt");
  }
  if (record.state === "terminal") {
    if (!record.responseId || !SAFE_REQUEST.test(record.responseId)) {
      throw new ClaimJournalError("Terminal journal response ID is invalid", "corrupt");
    }
    if (!record.terminalDigest || !SHA256.test(record.terminalDigest)) {
      throw new ClaimJournalError("Terminal journal digest is invalid", "corrupt");
    }
  } else if (record.responseId !== undefined || record.terminalDigest !== undefined) {
    throw new ClaimJournalError("Non-terminal journal proof fields are invalid", "corrupt");
  }
  if (!validOutcomeShape(record.state, record.outcome)) {
    throw new ClaimJournalError("Journal definitive outcome is invalid", "corrupt");
  }
  if (record.prelude !== undefined) {
    if (record.state !== "terminal" && record.state !== "provider_error") {
      throw new ClaimJournalError("Journal prelude state is invalid", "corrupt");
    }
    try {
      if (canonicalJson(normalizedPrelude(record.prelude)) !== canonicalJson(record.prelude)) {
        throw new Error("prelude changed during normalization");
      }
    } catch {
      throw new ClaimJournalError("Journal upstream prelude is invalid", "corrupt");
    }
  }
}

function nextStateAllowed(previous: ClaimState | null, next: ClaimState): boolean {
  return (
    (previous === null && next === "claimed") ||
    (previous === "claimed" && next === "fetch_starting") ||
    (previous === "claimed" && next === "provider_error") ||
    (previous === "claimed" && next === "cancelled") ||
    (previous === "claimed" && next === "forced_ambiguous") ||
    (previous === "fetch_starting" && next === "terminal") ||
    (previous === "fetch_starting" && next === "provider_error") ||
    (previous === "fetch_starting" && next === "cancelled") ||
    (previous === "fetch_starting" && next === "forced_ambiguous") ||
    (["terminal", "provider_error", "cancelled", "forced_ambiguous"] as ClaimState[])
      .includes(previous!) && next === "acked"
  );
}

export class ClaimJournal {
  readonly #storage: JournalStorage;
  readonly #clock: Clock;
  readonly #flushIntervalMs: number;
  readonly #maxBatch: number;
  readonly #maxPendingRecords: number;
  readonly #maxAcknowledged: number;
  readonly #acknowledgedRetentionMs: number;
  readonly #compactAfterRecords: number;
  readonly #maxTerminalSpoolBytes: number;
  readonly #terminalSpoolBufferBytes: number;
  readonly #maxUnacknowledgedRequests: number;
  readonly #maxJournalBytes: number;
  readonly #maxJournalRecords: number;
  readonly #withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
  readonly #states = new Map<string, ClaimRuntimeState>();
  readonly #pending: PendingRecord[] = [];
  readonly #pendingRequests = new Set<string>();
  readonly #acknowledged = new Set<string>();
  readonly #acknowledgedOrder: string[] = [];
  readonly #pendingAcknowledgedPrune = new Set<string>();
  readonly #capacityStates = new Map<string, ClaimState>();
  readonly #requestMutationTails = new Map<string, Promise<void>>();
  readonly #forcedOutcomeIntents = new Set<string>();
  readonly #ready: Promise<void>;
  #nextSequence = 1;
  #acknowledgedHead = 0;
  #appendedSinceCompaction = 0;
  #flushScheduled = false;
  #flushing: Promise<void> | null = null;
  #closed = false;
  #fatalError: ClaimJournalError | null = null;
  #unacknowledgedRequests = 0;
  #journalBytes = 0;
  #journalRecords = 0;
  #futureReservedBytes = 0;
  #futureReservedRecords = 0;
  #capacityTail: Promise<void> = Promise.resolve();

  constructor(options: {
    storage: JournalStorage;
    clock?: Clock;
    flushIntervalMs: number;
    maxBatch: number;
    maxPendingRecords?: number;
    maxAcknowledged?: number;
    acknowledgedRetentionMs?: number;
    compactAfterRecords?: number;
    maxTerminalSpoolBytes?: number;
    terminalSpoolBufferBytes?: number;
    maxUnacknowledgedRequests?: number;
    maxJournalBytes?: number;
    maxJournalRecords?: number;
    assertExclusiveOwnership?: () => Promise<void>;
    withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
  }) {
    if (!Number.isFinite(options.flushIntervalMs) || options.flushIntervalMs < 0) {
      throw new RangeError("flushIntervalMs must be non-negative");
    }
    if (!Number.isSafeInteger(options.maxBatch) || options.maxBatch < 1) {
      throw new RangeError("maxBatch must be a positive safe integer");
    }
    const maxAcknowledged = options.maxAcknowledged ?? 4_096;
    const acknowledgedRetentionMs = options.acknowledgedRetentionMs ?? 24 * 60 * 60_000;
    const compactAfterRecords = options.compactAfterRecords ?? 8_192;
    const maxPendingRecords = options.maxPendingRecords ?? Math.max(4_096, options.maxBatch);
    const maxTerminalSpoolBytes = options.maxTerminalSpoolBytes ?? 64 * 1024 * 1024;
    const terminalSpoolBufferBytes = options.terminalSpoolBufferBytes ?? 64 * 1024;
    const maxUnacknowledgedRequests = options.maxUnacknowledgedRequests ?? 4_096;
    const maxJournalBytes = options.maxJournalBytes ?? V2_JOURNAL_MAX_BYTES;
    const maxJournalRecords = options.maxJournalRecords ?? V2_JOURNAL_MAX_RECORDS;
    if (!Number.isSafeInteger(maxAcknowledged) || maxAcknowledged < 1) {
      throw new RangeError("maxAcknowledged must be a positive safe integer");
    }
    if (!Number.isFinite(acknowledgedRetentionMs) || acknowledgedRetentionMs < 1) {
      throw new RangeError("acknowledgedRetentionMs must be positive and finite");
    }
    if (!Number.isSafeInteger(compactAfterRecords) || compactAfterRecords < 1) {
      throw new RangeError("compactAfterRecords must be a positive safe integer");
    }
    if (!Number.isSafeInteger(maxPendingRecords) || maxPendingRecords < options.maxBatch) {
      throw new RangeError("maxPendingRecords must be a safe integer at least maxBatch");
    }
    if (!Number.isSafeInteger(maxTerminalSpoolBytes) || maxTerminalSpoolBytes < 1) {
      throw new RangeError("maxTerminalSpoolBytes must be a positive safe integer");
    }
    if (
      !Number.isSafeInteger(terminalSpoolBufferBytes) ||
      terminalSpoolBufferBytes < 1 ||
      terminalSpoolBufferBytes > Math.min(maxTerminalSpoolBytes, 1024 * 1024)
    ) throw new RangeError("terminalSpoolBufferBytes must fit within the spool limit");
    if (
      !Number.isSafeInteger(maxUnacknowledgedRequests) ||
      maxUnacknowledgedRequests < 1
    ) throw new RangeError("maxUnacknowledgedRequests must be a positive safe integer");
    if (!Number.isSafeInteger(maxJournalBytes) || maxJournalBytes < 1) {
      throw new RangeError("maxJournalBytes must be a positive safe integer");
    }
    if (!Number.isSafeInteger(maxJournalRecords) || maxJournalRecords < 1) {
      throw new RangeError("maxJournalRecords must be a positive safe integer");
    }
    if (
      options.assertExclusiveOwnership !== undefined &&
      typeof options.assertExclusiveOwnership !== "function"
    ) throw new TypeError("assertExclusiveOwnership must be a function");
    if (
      options.withExclusiveOwnership !== undefined &&
      typeof options.withExclusiveOwnership !== "function"
    ) throw new TypeError("withExclusiveOwnership must be a function");
    this.#storage = options.storage;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#flushIntervalMs = options.flushIntervalMs;
    this.#maxBatch = options.maxBatch;
    this.#maxPendingRecords = maxPendingRecords;
    this.#maxAcknowledged = maxAcknowledged;
    this.#acknowledgedRetentionMs = acknowledgedRetentionMs;
    this.#compactAfterRecords = compactAfterRecords;
    this.#maxTerminalSpoolBytes = maxTerminalSpoolBytes;
    this.#terminalSpoolBufferBytes = terminalSpoolBufferBytes;
    this.#maxUnacknowledgedRequests = maxUnacknowledgedRequests;
    this.#maxJournalBytes = maxJournalBytes;
    this.#maxJournalRecords = maxJournalRecords;
    this.#withExclusiveOwnership = options.withExclusiveOwnership;
    this.#ready = this.#initialize();
  }

  async claim(input: {
    requestId: string;
    requestDigest: string;
    leaderGeneration: number;
  }): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(input.requestId, () => this.#claimUnlocked(input));
  }

  async #claimUnlocked(input: {
    requestId: string;
    requestDigest: string;
    leaderGeneration: number;
  }): Promise<void> {
    this.#ensureOpen();
    if (
      !SAFE_REQUEST.test(input.requestId) ||
      !SHA256.test(input.requestDigest) ||
      !Number.isSafeInteger(input.leaderGeneration) ||
      input.leaderGeneration < 1
    ) throw new ClaimJournalError("Claim identity is invalid", "transition");
    const existing = this.#states.get(input.requestId);
    if (existing) {
      if (
        existing.requestDigest !== input.requestDigest ||
        existing.leaderGeneration !== input.leaderGeneration
      ) throw new ClaimJournalError("Claim identity conflicts with journal", "conflict");
      return;
    }
    if (this.#pendingRequests.has(input.requestId)) {
      throw new ClaimJournalError("Claim transition is already pending", "conflict");
    }
    if (
      this.#unacknowledgedRequests + this.#pendingRequests.size >=
      this.#maxUnacknowledgedRequests
    ) {
      throw new ClaimJournalError(
        "Journal unacknowledged request capacity is exhausted",
        "capacity",
      );
    }
    this.#pendingRequests.add(input.requestId);
    try {
      const record: JournalRecord = {
        version: 1,
        sequence: this.#nextSequence++,
        requestId: input.requestId,
        requestDigest: input.requestDigest,
        leaderGeneration: input.leaderGeneration,
        state: "claimed",
        recordedAtMs: this.#clock.now(),
      };
      await this.#append(record, () => {
        this.#states.set(input.requestId, {
          ...input,
          state: "claimed",
          records: [record],
        });
        this.#unacknowledgedRequests++;
      });
    } finally {
      this.#pendingRequests.delete(input.requestId);
    }
  }

  async markFetchStarting(requestId: string): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(
      requestId,
      () => this.#markFetchStartingUnlocked(requestId),
    );
  }

  async #markFetchStartingUnlocked(requestId: string): Promise<void> {
    this.#ensureOpen();
    this.#ensureNotForced(requestId);
    const current = this.#required(requestId);
    if (
      current.state === "fetch_starting" ||
      current.state === "terminal" ||
      current.state === "provider_error" ||
      current.state === "cancelled" ||
      current.state === "forced_ambiguous" ||
      current.state === "acked"
    ) return;
    if (current.state !== "claimed") {
      throw new ClaimJournalError("FETCH_STARTING transition is invalid", "transition");
    }
    const record = this.#record(current, "fetch_starting");
    await this.#append(record, () => {
      current.state = "fetch_starting";
      current.records.push(record);
    }, () => {
      this.#ensureNotForced(requestId);
      if (current.state !== "claimed") {
        throw new ClaimJournalError("FETCH_STARTING state changed before append", "conflict");
      }
    });
  }

  async beginTerminalSpool(requestId: string): Promise<JournalTerminalSpool> {
    await this.#ready;
    return this.#withRequestMutation(
      requestId,
      () => this.#beginTerminalSpoolUnlocked(requestId),
    );
  }

  async #beginTerminalSpoolUnlocked(requestId: string): Promise<JournalTerminalSpool> {
    this.#ensureOpen();
    this.#ensureNotForced(requestId);
    const current = this.#required(requestId);
    if (current.state !== "claimed" && current.state !== "fetch_starting") {
      throw new ClaimJournalError("Terminal spool transition is invalid", "transition");
    }
    try {
      const spool = await this.#storage.beginTerminalSpool(
        requestId,
        this.#maxTerminalSpoolBytes,
      );
      if (this.#forcedOutcomeIntents.has(requestId)) {
        try { await spool.abort(); }
        catch (error) {
          throw this.#latchFatal(this.#storageError(
            error,
            "Superseded terminal spool cleanup failed",
          ));
        }
        this.#ensureNotForced(requestId);
      }
      const fail = (error: unknown, message: string): ClaimJournalError => (
        this.#latchFatal(this.#storageError(error, message))
      );
      const capacity = Math.min(this.#terminalSpoolBufferBytes, spool.maxBytes);
      let buffer = new Uint8Array(capacity);
      let buffered = 0;
      let byteCount = 0;
      let state: "active" | "promoted" | "aborted" = "active";
      const flush = async (): Promise<void> => {
        if (buffered === 0) return;
        const chunk = buffered === buffer.byteLength
          ? buffer
          : buffer.slice(0, buffered);
        buffer = new Uint8Array(0);
        buffered = 0;
        try { await spool.append(chunk); }
        catch (error) { throw fail(error, "Terminal spool append failed"); }
        buffer = new Uint8Array(capacity);
      };
      return {
        requestId: spool.requestId,
        maxBytes: spool.maxBytes,
        get byteCount() { return byteCount; },
        async append(bytes) {
          if (state !== "active" || bytes.byteLength < 1) {
            throw new ClaimJournalError("Terminal spool append is invalid", "transition");
          }
          if (byteCount + bytes.byteLength > spool.maxBytes) {
            throw new ClaimJournalError("Terminal spool byte capacity is exceeded", "capacity");
          }
          let offset = 0;
          while (offset < bytes.byteLength) {
            const take = Math.min(buffer.byteLength - buffered, bytes.byteLength - offset);
            buffer.set(bytes.subarray(offset, offset + take), buffered);
            buffered += take;
            byteCount += take;
            offset += take;
            if (buffered === buffer.byteLength) await flush();
          }
        },
        async promote(proof) {
          if (state !== "active") {
            throw new ClaimJournalError("Terminal spool is closed", "transition");
          }
          if (proof.byteCount !== byteCount) {
            throw new ClaimJournalError("Terminal spool proof conflicts", "conflict");
          }
          await flush();
          try { await spool.promote(proof); state = "promoted"; }
          catch (error) { throw fail(error, "Terminal spool promotion failed"); }
        },
        async abort() {
          if (state !== "active") return;
          buffer.fill(0);
          buffer = new Uint8Array(0);
          buffered = 0;
          try { await spool.abort(); state = "aborted"; }
          catch (error) { throw fail(error, "Terminal spool cleanup failed"); }
        },
      };
    } catch (error) {
      if (error instanceof ClaimJournalError) throw error;
      if (error instanceof JournalStorageCapacityError) {
        throw new ClaimJournalError(error.message, "capacity");
      }
      throw this.#latchFatal(this.#storageError(error, "Terminal spool creation failed"));
    }
  }

  async markTerminal(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    terminalBytes: Uint8Array,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(requestId, () => this.#markTerminalUnlocked(
      requestId,
      responseId,
      terminalDigest,
      terminalBytes,
      prelude,
    ));
  }

  async #markTerminalUnlocked(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    terminalBytes: Uint8Array,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    this.#ensureOpen();
    this.#ensureNotForced(requestId);
    const durablePrelude = normalizedPrelude(prelude);
    const current = this.#required(requestId);
    if (
      current.state === "provider_error" ||
      current.state === "cancelled" ||
      current.state === "forced_ambiguous"
    ) {
      throw new ClaimJournalError("Terminal proof conflicts with definitive outcome", "conflict");
    }
    if (current.state === "terminal" || current.state === "acked") {
      if (
        current.responseId !== responseId ||
        current.terminalDigest !== terminalDigest ||
        (current.state === "terminal" &&
          canonicalJson(current.prelude ?? null) !== canonicalJson(durablePrelude ?? null))
      ) throw new ClaimJournalError("Terminal proof conflicts with journal", "conflict");
      return;
    }
    if (
      current.state !== "fetch_starting" ||
      !SAFE_REQUEST.test(responseId) ||
      !SHA256.test(terminalDigest) ||
      terminalBytes.byteLength < 1
    ) throw new ClaimJournalError("Terminal transition is invalid", "transition");
    try {
      await this.#runOwnedMutation(() => this.#storage.writeTerminal(requestId, terminalBytes));
    } catch (error) {
      throw this.#latchFatal(this.#storageError(error, "Terminal spool write failed"));
    }
    if (this.#forcedOutcomeIntents.has(requestId)) {
      await this.#deleteTerminal(requestId);
      this.#ensureNotForced(requestId);
    }
    if (current.state !== "fetch_starting") {
      await this.#deleteTerminal(requestId);
      throw new ClaimJournalError("Terminal state changed before append", "conflict");
    }
    const record: JournalRecord = {
      ...this.#record(current, "terminal"),
      responseId,
      terminalDigest,
      ...(durablePrelude ? { prelude: durablePrelude } : {}),
    };
    await this.#append(record, () => {
      current.state = "terminal";
      current.responseId = responseId;
      current.terminalDigest = terminalDigest;
      current.prelude = durablePrelude;
      current.records.push(record);
    }, () => {
      this.#ensureNotForced(requestId);
      if (current.state !== "fetch_starting") {
        throw new ClaimJournalError("Terminal state changed before append", "conflict");
      }
    });
  }

  async markTerminalFromSpool(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    byteCount: number,
    spool: JournalTerminalSpool,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(requestId, () => this.#markTerminalFromSpoolUnlocked(
      requestId,
      responseId,
      terminalDigest,
      byteCount,
      spool,
      prelude,
    ));
  }

  async #markTerminalFromSpoolUnlocked(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    byteCount: number,
    spool: JournalTerminalSpool,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    this.#ensureOpen();
    const durablePrelude = normalizedPrelude(prelude);
    if (this.#forcedOutcomeIntents.has(requestId)) {
      try { await spool.abort(); }
      catch (error) {
        throw this.#latchFatal(this.#storageError(
          error,
          "Superseded terminal spool cleanup failed",
        ));
      }
      this.#ensureNotForced(requestId);
    }
    const current = this.#required(requestId);
    if (
      current.state === "provider_error" ||
      current.state === "cancelled" ||
      current.state === "forced_ambiguous"
    ) {
      try { await spool.abort(); }
      catch (error) {
        throw this.#latchFatal(this.#storageError(
          error,
          "Superseded terminal spool cleanup failed",
        ));
      }
      throw new ClaimJournalError("Terminal proof conflicts with definitive outcome", "conflict");
    }
    if (current.state === "terminal" || current.state === "acked") {
      const matches = !(
        current.responseId !== responseId ||
        current.terminalDigest !== terminalDigest ||
        (current.state === "terminal" &&
          canonicalJson(current.prelude ?? null) !== canonicalJson(durablePrelude ?? null))
      );
      await spool.abort().catch(() => undefined);
      if (!matches) throw new ClaimJournalError("Terminal proof conflicts with journal", "conflict");
      return;
    }
    if (
      current.state !== "fetch_starting" ||
      spool.requestId !== requestId ||
      !SAFE_REQUEST.test(responseId) ||
      !SHA256.test(terminalDigest) ||
      !Number.isSafeInteger(byteCount) ||
      byteCount < 1 ||
      byteCount > this.#maxTerminalSpoolBytes
    ) {
      if (spool.requestId === requestId) await spool.abort().catch(() => undefined);
      throw new ClaimJournalError("Terminal transition is invalid", "transition");
    }
    try {
      await spool.promote({ byteCount, transcriptSha256: terminalDigest });
    } catch (error) {
      if (
        error instanceof ClaimJournalError &&
        (error.code === "capacity" || error.code === "conflict" || error.code === "transition")
      ) throw error;
      throw this.#latchFatal(this.#storageError(error, "Terminal spool promotion failed"));
    }
    if (this.#forcedOutcomeIntents.has(requestId)) {
      await this.#deleteTerminal(requestId);
      this.#ensureNotForced(requestId);
    }
    if (current.state !== "fetch_starting") {
      await this.#deleteTerminal(requestId);
      throw new ClaimJournalError("Terminal state changed before append", "conflict");
    }
    const record: JournalRecord = {
      ...this.#record(current, "terminal"),
      responseId,
      terminalDigest,
      ...(durablePrelude ? { prelude: durablePrelude } : {}),
    };
    await this.#append(record, () => {
      current.state = "terminal";
      current.responseId = responseId;
      current.terminalDigest = terminalDigest;
      current.prelude = durablePrelude;
      current.records.push(record);
    }, () => {
      this.#ensureNotForced(requestId);
      if (current.state !== "fetch_starting") {
        throw new ClaimJournalError("Terminal state changed before append", "conflict");
      }
    });
  }

  async markProviderError(
    requestId: string,
    error: JournalProviderError,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    const normalized = normalizedProviderError(error);
    await this.#ready;
    return this.#withRequestMutation(requestId, () => this.#markDefinitiveOutcomeUnlocked(
      requestId,
      { kind: "provider_error", error: normalized },
      prelude,
    ));
  }

  async markCancelled(requestId: string): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(requestId, () => this.#markDefinitiveOutcomeUnlocked(
      requestId,
      { kind: "cancelled" },
    ));
  }

  async markForcedAmbiguous(
    requestId: string,
    error: JournalForcedAmbiguous = {
      code: "forced_drain_timeout",
      message: "Committed execution did not settle after forced drain cancellation",
    },
  ): Promise<void> {
    const outcome: JournalDefinitiveOutcome = {
      kind: "forced_ambiguous",
      error: normalizedForcedAmbiguous(error),
    };
    this.#forcedOutcomeIntents.add(requestId);
    try {
      await this.#ready;
      await this.#withRequestMutation(
        requestId,
        () => this.#markDefinitiveOutcomeUnlocked(requestId, outcome),
      );
    } finally {
      this.#forcedOutcomeIntents.delete(requestId);
    }
  }

  async acknowledgeTerminal(requestId: string): Promise<void> {
    await this.#acknowledge(requestId, "terminal");
  }

  async acknowledgeOutcome(requestId: string): Promise<void> {
    await this.#acknowledge(requestId, "outcome");
  }

  async #acknowledge(requestId: string, expected: "terminal" | "outcome"): Promise<void> {
    await this.#ready;
    return this.#withRequestMutation(
      requestId,
      () => this.#acknowledgeUnlocked(requestId, expected),
    );
  }

  async #acknowledgeUnlocked(
    requestId: string,
    expected: "terminal" | "outcome",
  ): Promise<void> {
    this.#ensureOpen();
    const current = this.#required(requestId);
    if (current.state === "acked") {
      // A previous durable ACK may have been followed by a failed unlink. Keep
      // retrying cleanup rather than permanently leaking the terminal spool.
      // Compacted ACK fences intentionally discard the now-irrelevant terminal
      // kind, so either idempotent acknowledgement is safe after restart.
      await this.#deleteTerminal(requestId);
      this.#rememberAcknowledged(requestId);
      return;
    }
    const isOutcome = current.outcome !== undefined;
    if ((expected === "outcome") !== isOutcome) {
      throw new ClaimJournalError("Terminal ACK kind does not match journal", "transition");
    }
    if (
      current.state !== "terminal" &&
      current.state !== "provider_error" &&
      current.state !== "cancelled" &&
      current.state !== "forced_ambiguous"
    ) {
      throw new ClaimJournalError("Terminal ACK transition is invalid", "transition");
    }
    const record = this.#record(current, "acked");
    await this.#append(record, () => {
      current.state = "acked";
      current.records.push(record);
      this.#unacknowledgedRequests = Math.max(0, this.#unacknowledgedRequests - 1);
    });
    await this.#deleteTerminal(requestId);
    this.#rememberAcknowledged(requestId);
  }

  canRetry(requestId: string): boolean {
    return this.#states.get(requestId)?.state === "claimed";
  }

  status(requestId: string): ClaimState | null {
    return this.#states.get(requestId)?.state ?? null;
  }

  identity(requestId: string): {
    requestDigest: string;
    leaderGeneration: number;
    state: ClaimState;
  } | null {
    const current = this.#states.get(requestId);
    return current ? {
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      state: current.state,
    } : null;
  }

  async readRecoveredTerminal(requestId: string): Promise<{
    requestDigest: string;
    leaderGeneration: number;
    responseId: string;
    terminalDigest: string;
    bytes: Uint8Array;
    prelude?: UpstreamPrelude;
  } | null> {
    await this.#ready;
    const current = this.#states.get(requestId);
    if (
      current?.state !== "terminal" ||
      !current.responseId ||
      !current.terminalDigest
    ) return null;
    const bytes = await this.#storage.readTerminal(requestId, this.#maxTerminalSpoolBytes);
    if (!bytes) return null;
    return {
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      responseId: current.responseId,
      terminalDigest: current.terminalDigest,
      bytes,
      ...(current.prelude ? { prelude: { ...current.prelude } } : {}),
    };
  }

  async openRecoveredTerminal(requestId: string): Promise<{
    requestDigest: string;
    leaderGeneration: number;
    responseId: string;
    terminalDigest: string;
    reader: JournalTerminalReader;
    prelude?: UpstreamPrelude;
  } | null> {
    await this.#ready;
    const current = this.#states.get(requestId);
    if (
      current?.state !== "terminal" ||
      !current.responseId ||
      !current.terminalDigest
    ) return null;
    const reader = this.#storage.openTerminal
      ? await this.#storage.openTerminal(requestId, this.#maxTerminalSpoolBytes)
      : await this.#storage.readTerminal(requestId, this.#maxTerminalSpoolBytes)
        .then(bytes => bytes ? memoryTerminalReader(requestId, bytes) : null);
    if (!reader) return null;
    return {
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      responseId: current.responseId,
      terminalDigest: current.terminalDigest,
      reader,
      ...(current.prelude ? { prelude: { ...current.prelude } } : {}),
    };
  }

  async readRecoveredOutcome(requestId: string): Promise<RecoveredJournalOutcome | null> {
    await this.#ready;
    const current = this.#states.get(requestId);
    if (
      !current?.outcome ||
      current.state === "acked" ||
      current.state !== current.outcome.kind
    ) return null;
    return {
      requestId,
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      outcome: structuredClone(current.outcome),
      ...(current.prelude ? { prelude: { ...current.prelude } } : {}),
    };
  }

  async recover(): Promise<{
    terminals: readonly string[];
    providerErrors: readonly RecoveredProviderError[];
    cancelled: readonly string[];
    forcedAmbiguous: readonly RecoveredForcedAmbiguous[];
    ambiguous: readonly string[];
    retryable: readonly string[];
  }> {
    await this.#ready;
    if (this.#unacknowledgedRequests > this.#maxUnacknowledgedRequests) {
      throw new ClaimJournalError(
        "Journal unacknowledged request capacity is exceeded",
        "capacity",
      );
    }
    const terminals: string[] = [];
    const providerErrors: RecoveredProviderError[] = [];
    const cancelled: string[] = [];
    const forcedAmbiguous: RecoveredForcedAmbiguous[] = [];
    const ambiguous: string[] = [];
    const retryable: string[] = [];
    for (const state of this.#states.values()) {
      if (state.state === "claimed") retryable.push(state.requestId);
      else if (state.state === "fetch_starting") ambiguous.push(state.requestId);
      else if (state.state === "terminal") {
        const exists = await this.#storage.terminalExists(state.requestId);
        if (exists) terminals.push(state.requestId);
        else ambiguous.push(state.requestId);
      } else if (state.state === "provider_error" && state.outcome?.kind === "provider_error") {
        providerErrors.push({
          requestId: state.requestId,
          requestDigest: state.requestDigest,
          leaderGeneration: state.leaderGeneration,
          error: structuredClone(state.outcome.error),
          ...(state.prelude ? { prelude: { ...state.prelude } } : {}),
        });
      } else if (state.state === "cancelled" && state.outcome?.kind === "cancelled") {
        cancelled.push(state.requestId);
      } else if (
        state.state === "forced_ambiguous" &&
        state.outcome?.kind === "forced_ambiguous"
      ) {
        forcedAmbiguous.push({
          requestId: state.requestId,
          requestDigest: state.requestDigest,
          leaderGeneration: state.leaderGeneration,
          error: { ...state.outcome.error },
        });
      }
    }
    return {
      terminals: Object.freeze(terminals.sort()),
      providerErrors: Object.freeze(providerErrors.sort((left, right) => (
        left.requestId.localeCompare(right.requestId)
      ))),
      cancelled: Object.freeze(cancelled.sort()),
      forcedAmbiguous: Object.freeze(forcedAmbiguous.sort((left, right) => (
        left.requestId.localeCompare(right.requestId)
      ))),
      ambiguous: Object.freeze(ambiguous.sort()),
      retryable: Object.freeze(retryable.sort()),
    };
  }

  /**
   * A newly elected R2 generation can never accept or ACK an older generation's
   * work. Compact those unreachable records first, then remove their orphaned
   * terminal files so they cannot consume the bounded spool quota forever.
   */
  async pruneOtherGenerations(currentLeaderGeneration: number): Promise<number> {
    await this.#ready;
    this.#ensureOpen();
    if (!Number.isSafeInteger(currentLeaderGeneration) || currentLeaderGeneration < 1) {
      throw new ClaimJournalError("Current leader generation is invalid", "transition");
    }
    const obsolete = new Set(
      [...this.#states.values()]
        .filter(state => state.leaderGeneration !== currentLeaderGeneration)
        .map(state => state.requestId),
    );
    if (obsolete.size === 0) return 0;
    if (
      !this.#storage.replaceRecords ||
      !this.#storage.pruneTerminals ||
      !this.#withExclusiveOwnership
    ) {
      throw new ClaimJournalError(
        "Generation cleanup requires exclusive journal storage",
        "storage",
      );
    }
    try {
      const compacted = this.#compactedRecords(obsolete);
      await this.#storage.replaceRecords(compacted);
      await this.#withExclusiveOwnership(async () => {
        const byRequest = new Map(compacted.map(record => [record.requestId, record]));
        for (const state of this.#states.values()) {
          const snapshot = byRequest.get(state.requestId);
          if (snapshot) state.records = [snapshot];
        }
        for (const requestId of obsolete) {
          const capacityState = this.#capacityStates.get(requestId) ?? null;
          const reservation = this.#futureReservation(capacityState);
          this.#futureReservedBytes -= reservation.bytes;
          this.#futureReservedRecords -= reservation.records;
          this.#capacityStates.delete(requestId);
          this.#states.delete(requestId);
          this.#acknowledged.delete(requestId);
          this.#pendingAcknowledgedPrune.delete(requestId);
        }
        this.#acknowledgedOrder.splice(
          0,
          this.#acknowledgedOrder.length,
          ...this.#acknowledgedOrder.filter(requestId => !obsolete.has(requestId)),
        );
        this.#acknowledgedHead = 0;
        this.#unacknowledgedRequests = [...this.#states.values()]
          .filter(state => state.state !== "acked").length;
        this.#journalBytes = compacted.reduce(
          (total, record) => total + journalStoredRecordBytes(record),
          0,
        );
        this.#journalRecords = compacted.length;
        this.#appendedSinceCompaction = 0;
      });
      const retained = [...this.#states.values()]
        .filter(state => state.state === "terminal")
        .map(state => state.requestId);
      await this.#storage.pruneTerminals(retained);
      return obsolete.size;
    } catch (error) {
      throw this.#latchFatal(this.#storageError(
        error,
        "Journal generation cleanup failed",
      ));
    }
  }

  /**
   * Stop new mutations and synchronously drain every queued/durable append.
   * The lifecycle calls this after engine/transport drain and before releasing
   * the exclusive ownership lease.
   */
  async close(): Promise<void> {
    await this.#ready;
    if (this.#fatalError) throw this.#fatalError;
    if (this.#closed) {
      if (this.#flushing) await this.#flushing;
      return;
    }
    this.#closed = true;
    for (;;) {
      await this.#flush();
      if (!this.#flushing && this.#pending.length === 0) break;
    }
    if (this.#fatalError) throw this.#fatalError;
  }

  async #initialize(): Promise<void> {
    const records = await this.#storage.readRecords();
    this.#journalBytes = 0;
    this.#journalRecords = records.length;
    for (const record of records) this.#journalBytes += journalStoredRecordBytes(record);
    if (
      this.#journalBytes > this.#maxJournalBytes ||
      this.#journalRecords > this.#maxJournalRecords
    ) {
      throw new ClaimJournalError("Journal startup capacity is exceeded", "capacity");
    }
    let previousSequence = 0;
    for (const record of records) {
      validRecordShape(record);
      if (record.sequence <= previousSequence) {
        throw new ClaimJournalError("Journal sequences are not monotonic", "corrupt");
      }
      previousSequence = record.sequence;
      const current = this.#states.get(record.requestId);
      if (current) {
        if (
          current.requestDigest !== record.requestDigest ||
          current.leaderGeneration !== record.leaderGeneration
        ) throw new ClaimJournalError("Journal identity changes across transitions", "corrupt");
      }
      if (
        (record.compacted === true && current !== undefined) ||
        (record.compacted !== true && !nextStateAllowed(current?.state ?? null, record.state))
      ) {
        throw new ClaimJournalError("Journal state transition is corrupt", "corrupt");
      }
      if (current) {
        current.state = record.state;
        current.responseId = record.responseId ?? current.responseId;
        current.terminalDigest = record.terminalDigest ?? current.terminalDigest;
        current.outcome = record.outcome ?? current.outcome;
        current.prelude = record.prelude ?? current.prelude;
        current.records.push(record);
      } else {
        this.#states.set(record.requestId, {
          requestId: record.requestId,
          requestDigest: record.requestDigest,
          leaderGeneration: record.leaderGeneration,
          state: record.state,
          responseId: record.responseId,
          terminalDigest: record.terminalDigest,
          outcome: record.outcome,
          prelude: record.prelude,
          records: [record],
        });
      }
    }
    this.#nextSequence = previousSequence + 1;
    this.#unacknowledgedRequests = [...this.#states.values()]
      .filter(state => state.state !== "acked").length;
    this.#capacityStates.clear();
    this.#futureReservedBytes = 0;
    this.#futureReservedRecords = 0;
    for (const state of this.#states.values()) {
      this.#capacityStates.set(state.requestId, state.state);
      const reservation = this.#futureReservation(state.state);
      this.#futureReservedBytes += reservation.bytes;
      this.#futureReservedRecords += reservation.records;
    }

    const acknowledged = [...this.#states.values()]
      .filter(state => state.state === "acked")
      .sort((left, right) => (
        left.records[left.records.length - 1]!.sequence -
        right.records[right.records.length - 1]!.sequence
      ));
    const cutoff = this.#clock.now() - this.#acknowledgedRetentionMs;
    const retained = acknowledged.filter((state, index) => (
      index >= acknowledged.length - this.#maxAcknowledged &&
      state.records[state.records.length - 1]!.recordedAtMs > cutoff
    ));
    const retainedIds = new Set(retained.map(state => state.requestId));
    const prunable = new Set(
      acknowledged
        .filter(state => !retainedIds.has(state.requestId))
        .map(state => state.requestId),
    );
    if (
      this.#storage.replaceRecords &&
      this.#withExclusiveOwnership &&
      prunable.size > 0
    ) {
      try {
        const compacted = this.#compactedRecords(prunable);
        // One atomic rewrite avoids repeatedly serializing a shrinking legacy
        // journal. Only remove in-memory fences after the rewrite is proven.
        await this.#storage.replaceRecords(compacted);
        await this.#withExclusiveOwnership(async () => {
          const byRequest = new Map(compacted.map(record => [record.requestId, record]));
          for (const state of this.#states.values()) {
            const snapshot = byRequest.get(state.requestId);
            if (snapshot) state.records = [snapshot];
          }
          for (const requestId of prunable) this.#states.delete(requestId);
          for (const requestId of prunable) this.#capacityStates.delete(requestId);
          this.#journalBytes = compacted.reduce(
            (total, record) => total + journalStoredRecordBytes(record),
            0,
          );
          this.#journalRecords = compacted.length;
          this.#appendedSinceCompaction = 0;
        });
      } catch (error) {
        if (this.#isOwnershipFailure(error)) {
          throw this.#storageError(error, "Journal ownership was lost during compaction");
        }
        if (error instanceof JournalStorageDurabilityError) {
          throw this.#storageError(error, "Journal compaction durability fence failed");
        }
        // Compaction is optional maintenance. Retain the complete transition
        // history and duplicate fence when ownership/storage is unavailable.
      }
    }
    for (const state of this.#states.values()) {
      if (state.state === "acked") this.#rememberAcknowledged(state.requestId);
    }
    if (this.#storage.pruneTerminals && this.#withExclusiveOwnership) {
      const retained = [...this.#states.values()]
        .filter(state => state.state === "terminal")
        .map(state => state.requestId);
      try {
        await this.#storage.pruneTerminals(retained);
      } catch (error) {
        if (this.#isOwnershipFailure(error)) {
          throw this.#storageError(error, "Journal ownership was lost during cleanup");
        }
        if (error instanceof JournalStorageCapacityError) {
          throw this.#storageError(error, "Journal storage bounds were exceeded");
        }
        // Orphan cleanup is optional; terminal recovery remains authoritative.
      }
    }
  }

  async #markDefinitiveOutcomeUnlocked(
    requestId: string,
    outcome: JournalDefinitiveOutcome,
    prelude?: UpstreamPrelude,
  ): Promise<void> {
    this.#ensureOpen();
    const durablePrelude = outcome.kind === "provider_error"
      ? normalizedPrelude(prelude)
      : undefined;
    if (outcome.kind !== "forced_ambiguous") this.#ensureNotForced(requestId);
    const current = this.#required(requestId);
    if (
      current.state === outcome.kind ||
      current.state === "acked"
    ) {
      if (!outcomesEqual(current.outcome, outcome)) {
        throw new ClaimJournalError("Definitive outcome conflicts with journal", "conflict");
      }
      if (canonicalJson(current.prelude ?? null) !== canonicalJson(durablePrelude ?? null)) {
        throw new ClaimJournalError("Definitive outcome prelude conflicts with journal", "conflict");
      }
      return;
    }
    if (
      current.state === "terminal" ||
      current.state === "provider_error" ||
      current.state === "cancelled" ||
      current.state === "forced_ambiguous"
    ) {
      throw new ClaimJournalError("Definitive outcome conflicts with journal", "conflict");
    }
    const allowed = current.state === "claimed" || current.state === "fetch_starting";
    if (!allowed) {
      throw new ClaimJournalError("Definitive outcome transition is invalid", "transition");
    }
    const record: JournalRecord = {
      ...this.#record(current, outcome.kind),
      outcome,
      ...(durablePrelude ? { prelude: durablePrelude } : {}),
    };
    await this.#append(record, () => {
      current.state = outcome.kind;
      current.outcome = outcome;
      current.prelude = durablePrelude;
      current.records.push(record);
    }, () => {
      if (outcome.kind !== "forced_ambiguous") this.#ensureNotForced(requestId);
      if (current.state !== "claimed" && current.state !== "fetch_starting") {
        throw new ClaimJournalError("Definitive outcome state changed before append", "conflict");
      }
    });
  }

  #required(requestId: string): ClaimRuntimeState {
    const current = this.#states.get(requestId);
    if (!current) throw new ClaimJournalError("Journal request was not found", "missing");
    return current;
  }

  #ensureOpen(): void {
    if (this.#fatalError) throw this.#fatalError;
    if (this.#closed) throw new ClaimJournalError("Journal is closed", "storage");
  }

  async #withRequestMutation<T>(
    requestId: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const previous = this.#requestMutationTails.get(requestId) ?? Promise.resolve();
    let release!: () => void;
    const slot = new Promise<void>(resolve => { release = resolve; });
    this.#requestMutationTails.set(requestId, slot);
    await previous.catch(() => undefined);
    try {
      this.#ensureOpen();
      return await operation();
    } finally {
      release();
      if (this.#requestMutationTails.get(requestId) === slot) {
        this.#requestMutationTails.delete(requestId);
      }
    }
  }

  #ensureNotForced(requestId: string): void {
    if (this.#forcedOutcomeIntents.has(requestId)) {
      throw new ClaimJournalError(
        "Forced outcome superseded this request transition",
        "conflict",
      );
    }
  }

  #runOwnedMutation<T>(operation: () => Promise<T>): Promise<T> {
    return this.#withExclusiveOwnership
      ? this.#withExclusiveOwnership(operation)
      : operation();
  }

  async #deleteTerminal(requestId: string): Promise<void> {
    try {
      await this.#runOwnedMutation(() => this.#storage.deleteTerminal(requestId));
    } catch (error) {
      throw this.#storageError(error, "Terminal spool delete failed");
    }
  }

  #storageError(error: unknown, message: string): ClaimJournalError {
    return error instanceof ClaimJournalError
      ? error
      : new ClaimJournalError(message, "storage");
  }

  #isOwnershipFailure(error: unknown): boolean {
    return error instanceof Error && error.name === "SidecarOwnershipError";
  }

  #latchFatal(error: ClaimJournalError): ClaimJournalError {
    this.#fatalError = error;
    this.#closed = true;
    for (const value of this.#pending.splice(0)) value.reject(error);
    return error;
  }

  #record(current: ClaimRuntimeState, state: ClaimState): JournalRecord {
    return {
      version: 1,
      sequence: this.#nextSequence++,
      requestId: current.requestId,
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      state,
      recordedAtMs: this.#clock.now(),
      ...(state === "terminal" && current.responseId && current.terminalDigest
        ? { responseId: current.responseId, terminalDigest: current.terminalDigest }
        : {}),
    };
  }

  #rememberAcknowledged(requestId: string): void {
    if (this.#acknowledged.has(requestId)) return;
    this.#acknowledged.add(requestId);
    this.#acknowledgedOrder.push(requestId);
    this.#trimAcknowledged();
  }

  #trimAcknowledged(): void {
    const cutoff = this.#clock.now() - this.#acknowledgedRetentionMs;
    for (;;) {
      const effectiveSize = this.#acknowledged.size - this.#pendingAcknowledgedPrune.size;
      const oldest = this.#acknowledgedOrder[this.#acknowledgedHead];
      if (!oldest) break;
      const state = this.#states.get(oldest);
      const recordedAtMs = state?.records[state.records.length - 1]?.recordedAtMs;
      const expired = recordedAtMs !== undefined && recordedAtMs <= cutoff;
      if (effectiveSize <= this.#maxAcknowledged && !expired) break;
      // If compaction remains unavailable, prefer extra durable fences over an
      // unbounded pending-deletion set or unsafe request-ID reuse.
      if (this.#pendingAcknowledgedPrune.size >= this.#compactAfterRecords) break;
      this.#acknowledgedHead++;
      if (!state || state.state !== "acked") continue;
      this.#pendingAcknowledgedPrune.add(oldest);
    }
    if (
      this.#acknowledgedHead >= 1_024 &&
      this.#acknowledgedHead * 2 >= this.#acknowledgedOrder.length
    ) {
      this.#acknowledgedOrder.splice(0, this.#acknowledgedHead);
      this.#acknowledgedHead = 0;
    }
  }

  #finalizeAcknowledgedPrune(requestIds: ReadonlySet<string>): void {
    for (const requestId of requestIds) {
      this.#pendingAcknowledgedPrune.delete(requestId);
      this.#acknowledged.delete(requestId);
      const state = this.#states.get(requestId);
      if (state?.state === "acked") this.#states.delete(requestId);
    }
  }

  #compactedRecords(excluded: ReadonlySet<string> = new Set()): JournalRecord[] {
    const records: JournalRecord[] = [];
    for (const state of this.#states.values()) {
      if (excluded.has(state.requestId)) continue;
      const latest = state.records[state.records.length - 1]!;
      records.push({
        version: 1,
        sequence: latest.sequence,
        requestId: state.requestId,
        requestDigest: state.requestDigest,
        leaderGeneration: state.leaderGeneration,
        state: state.state,
        recordedAtMs: latest.recordedAtMs,
        ...(state.state === "terminal" && state.responseId && state.terminalDigest
          ? { responseId: state.responseId, terminalDigest: state.terminalDigest }
          : {}),
        ...(state.outcome && state.state === state.outcome.kind
          ? { outcome: structuredClone(state.outcome) }
          : {}),
        ...(state.prelude && (state.state === "terminal" || state.state === "provider_error")
          ? { prelude: { ...state.prelude } }
          : {}),
        compacted: true,
      });
    }
    records.sort((left, right) => left.sequence - right.sequence);
    return records;
  }

  #futureReservation(state: ClaimState | null): { bytes: number; records: number } {
    switch (state) {
      case "claimed":
        return {
          bytes: V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES +
            2 * V2_JOURNAL_SMALL_TRANSITION_BYTES,
          records: 3,
        };
      case "fetch_starting":
        return {
          bytes: V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES +
            V2_JOURNAL_SMALL_TRANSITION_BYTES,
          records: 2,
        };
      case "terminal":
      case "provider_error":
      case "cancelled":
      case "forced_ambiguous":
        return { bytes: V2_JOURNAL_SMALL_TRANSITION_BYTES, records: 1 };
      default:
        return { bytes: 0, records: 0 };
    }
  }

  #fitsJournalCapacity(
    recordBytes: number,
    previous: ClaimState | null,
    next: ClaimState,
  ): boolean {
    const before = this.#futureReservation(previous);
    const after = this.#futureReservation(next);
    const currentProjectedBytes = this.#journalBytes + this.#futureReservedBytes;
    const currentProjectedRecords = this.#journalRecords + this.#futureReservedRecords;
    const actualBytes = this.#journalBytes + recordBytes;
    const actualRecords = this.#journalRecords + 1;
    const projectedBytes = actualBytes + this.#futureReservedBytes - before.bytes + after.bytes;
    const projectedRecords = actualRecords +
      this.#futureReservedRecords - before.records + after.records;
    return actualBytes <= this.#maxJournalBytes &&
      actualRecords <= this.#maxJournalRecords &&
      (projectedBytes <= this.#maxJournalBytes || projectedBytes <= currentProjectedBytes) &&
      (projectedRecords <= this.#maxJournalRecords || projectedRecords <= currentProjectedRecords);
  }

  async #compactRuntimeJournal(): Promise<boolean> {
    if (!this.#storage.replaceRecords || !this.#withExclusiveOwnership) return false;
    await this.#flush();
    if (this.#fatalError) throw this.#fatalError;
    const prunable = new Set(this.#pendingAcknowledgedPrune);
    const records = this.#compactedRecords(prunable);
    const bytes = records.reduce((total, record) => total + journalStoredRecordBytes(record), 0);
    if (bytes > this.#maxJournalBytes || records.length > this.#maxJournalRecords) return false;
    try {
      await this.#storage.replaceRecords(records);
      await this.#withExclusiveOwnership(async () => {
        const byRequest = new Map(records.map(record => [record.requestId, record]));
        for (const state of this.#states.values()) {
          const snapshot = byRequest.get(state.requestId);
          if (snapshot) state.records = [snapshot];
        }
        this.#finalizeAcknowledgedPrune(prunable);
        for (const requestId of prunable) this.#capacityStates.delete(requestId);
        this.#journalBytes = bytes;
        this.#journalRecords = records.length;
        this.#appendedSinceCompaction = 0;
        this.#trimAcknowledged();
      });
      return true;
    } catch (error) {
      if (this.#isOwnershipFailure(error)) {
        throw this.#storageError(error, "Journal ownership was lost during compaction");
      }
      if (error instanceof JournalStorageDurabilityError) {
        throw this.#latchFatal(this.#storageError(
          error,
          "Journal compaction durability fence failed",
        ));
      }
      return false;
    }
  }

  async #reserveJournalRecord(
    record: JournalRecord,
    apply: () => void,
    guard?: () => void,
  ): Promise<void> {
    let release!: () => void;
    let queued!: Promise<void>;
    const previousCheck = this.#capacityTail;
    this.#capacityTail = new Promise<void>(resolve => { release = resolve; });
    await previousCheck;
    try {
      this.#ensureOpen();
      const recordBytes = journalStoredRecordBytes(record);
      if (
        record.state === "provider_error" &&
        recordBytes > V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES
      ) {
        throw new ClaimJournalError("Provider outcome exceeds its journal reservation", "capacity");
      }
      let previous = this.#capacityStates.get(record.requestId) ?? null;
      if (
        this.#appendedSinceCompaction >= this.#compactAfterRecords ||
        !this.#fitsJournalCapacity(recordBytes, previous, record.state)
      ) {
        await this.#compactRuntimeJournal();
        if (this.#fatalError) throw this.#fatalError;
        this.#ensureOpen();
        previous = this.#capacityStates.get(record.requestId) ?? null;
      }
      guard?.();
      if (!this.#fitsJournalCapacity(recordBytes, previous, record.state)) {
        throw new ClaimJournalError("Journal durable byte or record capacity is exhausted", "capacity");
      }
      if (this.#pending.length >= this.#maxPendingRecords) {
        throw new ClaimJournalError("Journal pending queue is exhausted", "storage");
      }
      const before = this.#futureReservation(previous);
      const after = this.#futureReservation(record.state);
      this.#journalBytes += recordBytes;
      this.#journalRecords++;
      this.#futureReservedBytes += after.bytes - before.bytes;
      this.#futureReservedRecords += after.records - before.records;
      this.#capacityStates.set(record.requestId, record.state);
      queued = new Promise<void>((resolve, reject) => {
        this.#pending.push({ record, apply, resolve, reject });
        if (this.#pending.length >= this.#maxBatch) {
          void this.#flush().catch(() => undefined);
        } else {
          this.#scheduleFlush();
        }
      });
    } finally {
      release();
    }
    return queued;
  }

  async #append(
    record: JournalRecord,
    apply: () => void,
    guard?: () => void,
  ): Promise<void> {
    validRecordShape(record);
    if (this.#fatalError) throw this.#fatalError;
    if (this.#closed) {
      throw new ClaimJournalError("Journal is closed", "storage");
    }
    return this.#reserveJournalRecord(record, apply, guard);
  }

  #scheduleFlush(): void {
    if (this.#flushScheduled) return;
    this.#flushScheduled = true;
    void (async () => {
      try {
        await this.#clock.sleep(this.#flushIntervalMs);
        await this.#flush();
      } catch (error) {
        const pending = this.#pending.splice(0);
        for (const value of pending) value.reject(error);
      } finally {
        this.#flushScheduled = false;
        if (this.#pending.length) this.#scheduleFlush();
      }
    })();
  }

  async #flush(): Promise<void> {
    if (this.#fatalError) {
      const pending = this.#pending.splice(0);
      for (const value of pending) value.reject(this.#fatalError);
      return;
    }
    if (this.#flushing) {
      await this.#flushing;
      if (this.#pending.length) await this.#flush();
      return;
    }
    const batch = this.#pending.splice(0, this.#maxBatch);
    if (!batch.length) return;
    const flushing = (async () => {
      try {
        await this.#runOwnedMutation(() => (
          this.#storage.appendAndSync(batch.map(value => value.record))
        ));
        for (const value of batch) value.apply();
        this.#appendedSinceCompaction = Math.min(
          this.#compactAfterRecords,
          this.#appendedSinceCompaction + batch.length,
        );
        for (const value of batch) value.resolve();
      } catch (error) {
        const wrapped = this.#latchFatal(this.#storageError(
          error,
          "Journal storage append failed",
        ));
        for (const value of batch) value.reject(wrapped);
      }
    })();
    this.#flushing = flushing;
    try {
      await flushing;
    } finally {
      if (this.#flushing === flushing) this.#flushing = null;
    }
  }
}
