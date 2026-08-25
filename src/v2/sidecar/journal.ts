import { SYSTEM_CLOCK, type Clock } from "../../core/ports";
import {
  parseProviderErrorEnvelope,
  type ProviderErrorEnvelope,
} from "../provider-error";
import { canonicalJson } from "../protocol/shared";
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
}

export interface JournalStorage {
  /**
   * Resolve only after records are durable. Implementations must not reject for
   * optional permission/cleanup work after that boundary.
   */
  appendAndSync(records: readonly JournalRecord[]): Promise<void>;
  /** Atomically replace the append log with an equivalent compacted history. */
  replaceRecords?(records: readonly JournalRecord[]): Promise<void>;
  writeTerminal(requestId: string, bytes: Uint8Array): Promise<void>;
  readTerminal(requestId: string): Promise<Uint8Array | null>;
  /** Existence-only recovery scan; production storage must avoid reading spool bytes. */
  terminalExists(requestId: string): Promise<boolean>;
  deleteTerminal(requestId: string): Promise<void>;
  /** Remove terminal files that are not named in the retained set. */
  pruneTerminals?(retainedRequestIds: readonly string[]): Promise<void>;
  readRecords(): Promise<readonly JournalRecord[]>;
}

export class ClaimJournalError extends Error {
  readonly name = "ClaimJournalError";

  constructor(
    message: string,
    readonly code: "conflict" | "corrupt" | "missing" | "storage" | "transition",
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
    record.recordedAtMs < 0
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
  readonly #withExclusiveOwnership?: <T>(operation: () => Promise<T>) => Promise<T>;
  readonly #states = new Map<string, ClaimRuntimeState>();
  readonly #pending: PendingRecord[] = [];
  readonly #pendingRequests = new Set<string>();
  readonly #acknowledged = new Set<string>();
  readonly #acknowledgedOrder: string[] = [];
  readonly #pendingAcknowledgedPrune = new Set<string>();
  readonly #ready: Promise<void>;
  #nextSequence = 1;
  #acknowledgedHead = 0;
  #appendedSinceCompaction = 0;
  #flushScheduled = false;
  #flushing: Promise<void> | null = null;
  #closed = false;
  #fatalError: ClaimJournalError | null = null;

  constructor(options: {
    storage: JournalStorage;
    clock?: Clock;
    flushIntervalMs: number;
    maxBatch: number;
    maxPendingRecords?: number;
    maxAcknowledged?: number;
    acknowledgedRetentionMs?: number;
    compactAfterRecords?: number;
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
    this.#withExclusiveOwnership = options.withExclusiveOwnership;
    this.#ready = this.#initialize();
  }

  async claim(input: {
    requestId: string;
    requestDigest: string;
    leaderGeneration: number;
  }): Promise<void> {
    await this.#ready;
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
      });
    } finally {
      this.#pendingRequests.delete(input.requestId);
    }
  }

  async markFetchStarting(requestId: string): Promise<void> {
    await this.#ready;
    this.#ensureOpen();
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
    });
  }

  async markTerminal(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    terminalBytes: Uint8Array,
  ): Promise<void> {
    await this.#ready;
    this.#ensureOpen();
    const current = this.#required(requestId);
    if (current.state === "terminal" || current.state === "acked") {
      if (
        current.responseId !== responseId ||
        current.terminalDigest !== terminalDigest
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
    const record: JournalRecord = {
      ...this.#record(current, "terminal"),
      responseId,
      terminalDigest,
    };
    await this.#append(record, () => {
      current.state = "terminal";
      current.responseId = responseId;
      current.terminalDigest = terminalDigest;
      current.records.push(record);
    });
  }

  async markProviderError(
    requestId: string,
    error: JournalProviderError,
  ): Promise<void> {
    const normalized = normalizedProviderError(error);
    await this.#markDefinitiveOutcome(requestId, {
      kind: "provider_error",
      error: normalized,
    });
  }

  async markCancelled(requestId: string): Promise<void> {
    await this.#markDefinitiveOutcome(requestId, { kind: "cancelled" });
  }

  async markForcedAmbiguous(
    requestId: string,
    error: JournalForcedAmbiguous = {
      code: "forced_drain_timeout",
      message: "Committed execution did not settle after forced drain cancellation",
    },
  ): Promise<void> {
    await this.#markDefinitiveOutcome(requestId, {
      kind: "forced_ambiguous",
      error: normalizedForcedAmbiguous(error),
    });
  }

  async acknowledgeTerminal(requestId: string): Promise<void> {
    await this.#acknowledge(requestId, "terminal");
  }

  async acknowledgeOutcome(requestId: string): Promise<void> {
    await this.#acknowledge(requestId, "outcome");
  }

  async #acknowledge(requestId: string, expected: "terminal" | "outcome"): Promise<void> {
    await this.#ready;
    this.#ensureOpen();
    const current = this.#required(requestId);
    const isOutcome = current.outcome !== undefined;
    if ((expected === "outcome") !== isOutcome) {
      throw new ClaimJournalError("Terminal ACK kind does not match journal", "transition");
    }
    if (current.state === "acked") {
      // A previous durable ACK may have been followed by a failed unlink. Keep
      // retrying cleanup rather than permanently leaking the terminal spool.
      await this.#deleteTerminal(requestId);
      this.#rememberAcknowledged(requestId);
      return;
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
  } | null> {
    await this.#ready;
    const current = this.#states.get(requestId);
    if (
      current?.state !== "terminal" ||
      !current.responseId ||
      !current.terminalDigest
    ) return null;
    const bytes = await this.#storage.readTerminal(requestId);
    if (!bytes) return null;
    return {
      requestDigest: current.requestDigest,
      leaderGeneration: current.leaderGeneration,
      responseId: current.responseId,
      terminalDigest: current.terminalDigest,
      bytes,
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
      if (!this.#flushing && this.#pending.length === 0) return;
    }
  }

  async #initialize(): Promise<void> {
    const records = await this.#storage.readRecords();
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
      if (!nextStateAllowed(current?.state ?? null, record.state)) {
        throw new ClaimJournalError("Journal state transition is corrupt", "corrupt");
      }
      if (current) {
        current.state = record.state;
        current.responseId = record.responseId ?? current.responseId;
        current.terminalDigest = record.terminalDigest ?? current.terminalDigest;
        current.outcome = record.outcome ?? current.outcome;
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
          records: [record],
        });
      }
    }
    this.#nextSequence = previousSequence + 1;

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
        // One atomic rewrite avoids repeatedly serializing a shrinking legacy
        // journal. Only remove in-memory fences after the rewrite is proven.
        await this.#storage.replaceRecords(this.#compactedRecords(prunable));
        await this.#withExclusiveOwnership(async () => {
          for (const requestId of prunable) this.#states.delete(requestId);
        });
      } catch (error) {
        if (this.#isOwnershipFailure(error)) {
          throw this.#storageError(error, "Journal ownership was lost during compaction");
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
        // Orphan cleanup is optional; terminal recovery remains authoritative.
      }
    }
  }

  async #markDefinitiveOutcome(
    requestId: string,
    outcome: JournalDefinitiveOutcome,
  ): Promise<void> {
    await this.#ready;
    this.#ensureOpen();
    const current = this.#required(requestId);
    if (
      current.state === outcome.kind ||
      current.state === "acked"
    ) {
      if (!outcomesEqual(current.outcome, outcome)) {
        throw new ClaimJournalError("Definitive outcome conflicts with journal", "conflict");
      }
      return;
    }
    const allowed = current.state === "claimed" || current.state === "fetch_starting";
    if (!allowed) {
      throw new ClaimJournalError("Definitive outcome transition is invalid", "transition");
    }
    const record: JournalRecord = {
      ...this.#record(current, outcome.kind),
      outcome,
    };
    await this.#append(record, () => {
      current.state = outcome.kind;
      current.outcome = outcome;
      current.records.push(record);
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
      if (!excluded.has(state.requestId)) records.push(...state.records);
    }
    records.sort((left, right) => left.sequence - right.sequence);
    return records;
  }

  #append(record: JournalRecord, apply: () => void): Promise<void> {
    validRecordShape(record);
    if (this.#fatalError) return Promise.reject(this.#fatalError);
    if (this.#closed) {
      return Promise.reject(new ClaimJournalError("Journal is closed", "storage"));
    }
    if (this.#pending.length >= this.#maxPendingRecords) {
      return Promise.reject(new ClaimJournalError(
        "Journal pending queue is exhausted",
        "storage",
      ));
    }
    return new Promise<void>((resolve, reject) => {
      this.#pending.push({ record, apply, resolve, reject });
      if (this.#pending.length >= this.#maxBatch) {
        void this.#flush().catch(() => undefined);
      } else {
        this.#scheduleFlush();
      }
    });
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
        if (
          this.#storage.replaceRecords &&
          this.#withExclusiveOwnership &&
          this.#appendedSinceCompaction >= this.#compactAfterRecords
        ) {
          const prunable = new Set(this.#pendingAcknowledgedPrune);
          try {
            await this.#storage.replaceRecords(this.#compactedRecords(prunable));
            await this.#withExclusiveOwnership(async () => {
              this.#appendedSinceCompaction = 0;
              this.#finalizeAcknowledgedPrune(prunable);
              this.#trimAcknowledged();
            });
          } catch (error) {
            if (this.#isOwnershipFailure(error)) {
              throw this.#storageError(error, "Journal ownership was lost during compaction");
            }
            // The transition append is already durable. Maintenance is retried
            // after the next successful batch and must not turn it into a
            // misleading request failure.
          }
        }
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
