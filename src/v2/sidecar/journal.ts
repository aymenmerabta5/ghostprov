import { SYSTEM_CLOCK, type Clock } from "../../core/ports";

export type ClaimState = "claimed" | "fetch_starting" | "terminal" | "acked";

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
}

export interface JournalStorage {
  appendAndSync(records: readonly JournalRecord[]): Promise<void>;
  writeTerminal(requestId: string, bytes: Uint8Array): Promise<void>;
  readTerminal(requestId: string): Promise<Uint8Array | null>;
  deleteTerminal(requestId: string): Promise<void>;
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
}

interface PendingRecord {
  record: JournalRecord;
  apply(): void;
  resolve(): void;
  reject(error: unknown): void;
}

const SAFE_REQUEST = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SHA256 = /^[a-f0-9]{64}$/;

function validRecordShape(record: JournalRecord): void {
  if (
    record.version !== 1 ||
    !Number.isSafeInteger(record.sequence) ||
    record.sequence < 1 ||
    !SAFE_REQUEST.test(record.requestId) ||
    !SHA256.test(record.requestDigest) ||
    !Number.isSafeInteger(record.leaderGeneration) ||
    record.leaderGeneration < 1 ||
    !["claimed", "fetch_starting", "terminal", "acked"].includes(record.state) ||
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
  }
}

function nextStateAllowed(previous: ClaimState | null, next: ClaimState): boolean {
  return (
    (previous === null && next === "claimed") ||
    (previous === "claimed" && next === "fetch_starting") ||
    (previous === "fetch_starting" && next === "terminal") ||
    (previous === "terminal" && next === "acked")
  );
}

export class ClaimJournal {
  readonly #storage: JournalStorage;
  readonly #clock: Clock;
  readonly #flushIntervalMs: number;
  readonly #maxBatch: number;
  readonly #states = new Map<string, ClaimRuntimeState>();
  readonly #pending: PendingRecord[] = [];
  readonly #pendingRequests = new Set<string>();
  readonly #ready: Promise<void>;
  #nextSequence = 1;
  #flushScheduled = false;
  #flushing: Promise<void> | null = null;

  constructor(options: {
    storage: JournalStorage;
    clock?: Clock;
    flushIntervalMs: number;
    maxBatch: number;
  }) {
    if (!Number.isFinite(options.flushIntervalMs) || options.flushIntervalMs < 0) {
      throw new RangeError("flushIntervalMs must be non-negative");
    }
    if (!Number.isSafeInteger(options.maxBatch) || options.maxBatch < 1) {
      throw new RangeError("maxBatch must be a positive safe integer");
    }
    this.#storage = options.storage;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#flushIntervalMs = options.flushIntervalMs;
    this.#maxBatch = options.maxBatch;
    this.#ready = this.#initialize();
  }

  async claim(input: {
    requestId: string;
    requestDigest: string;
    leaderGeneration: number;
  }): Promise<void> {
    await this.#ready;
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
      await this.#append({
        version: 1,
        sequence: this.#nextSequence++,
        requestId: input.requestId,
        requestDigest: input.requestDigest,
        leaderGeneration: input.leaderGeneration,
        state: "claimed",
        recordedAtMs: this.#clock.now(),
      }, () => {
        this.#states.set(input.requestId, {
          ...input,
          state: "claimed",
        });
      });
    } finally {
      this.#pendingRequests.delete(input.requestId);
    }
  }

  async markFetchStarting(requestId: string): Promise<void> {
    await this.#ready;
    const current = this.#required(requestId);
    if (current.state === "fetch_starting" || current.state === "terminal" || current.state === "acked") return;
    if (current.state !== "claimed") {
      throw new ClaimJournalError("FETCH_STARTING transition is invalid", "transition");
    }
    await this.#append(this.#record(current, "fetch_starting"), () => {
      current.state = "fetch_starting";
    });
  }

  async markTerminal(
    requestId: string,
    responseId: string,
    terminalDigest: string,
    terminalBytes: Uint8Array,
  ): Promise<void> {
    await this.#ready;
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
    await this.#storage.writeTerminal(requestId, terminalBytes);
    await this.#append({
      ...this.#record(current, "terminal"),
      responseId,
      terminalDigest,
    }, () => {
      current.state = "terminal";
      current.responseId = responseId;
      current.terminalDigest = terminalDigest;
    });
  }

  async acknowledgeTerminal(requestId: string): Promise<void> {
    await this.#ready;
    const current = this.#required(requestId);
    if (current.state === "acked") return;
    if (current.state !== "terminal") {
      throw new ClaimJournalError("Terminal ACK transition is invalid", "transition");
    }
    await this.#append(this.#record(current, "acked"), () => {
      current.state = "acked";
    });
    await this.#storage.deleteTerminal(requestId);
  }

  canRetry(requestId: string): boolean {
    return this.#states.get(requestId)?.state === "claimed";
  }

  async recover(): Promise<{
    terminals: readonly string[];
    ambiguous: readonly string[];
    retryable: readonly string[];
  }> {
    await this.#ready;
    const terminals: string[] = [];
    const ambiguous: string[] = [];
    const retryable: string[] = [];
    for (const state of this.#states.values()) {
      if (state.state === "claimed") retryable.push(state.requestId);
      else if (state.state === "fetch_starting") ambiguous.push(state.requestId);
      else if (state.state === "terminal") {
        if (await this.#storage.readTerminal(state.requestId)) terminals.push(state.requestId);
        else ambiguous.push(state.requestId);
      }
    }
    return {
      terminals: Object.freeze(terminals.sort()),
      ambiguous: Object.freeze(ambiguous.sort()),
      retryable: Object.freeze(retryable.sort()),
    };
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
      this.#states.set(record.requestId, {
        requestId: record.requestId,
        requestDigest: record.requestDigest,
        leaderGeneration: record.leaderGeneration,
        state: record.state,
        responseId: record.responseId ?? current?.responseId,
        terminalDigest: record.terminalDigest ?? current?.terminalDigest,
      });
    }
    this.#nextSequence = previousSequence + 1;
  }

  #required(requestId: string): ClaimRuntimeState {
    const current = this.#states.get(requestId);
    if (!current) throw new ClaimJournalError("Journal request was not found", "missing");
    return current;
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

  #append(record: JournalRecord, apply: () => void): Promise<void> {
    validRecordShape(record);
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
    if (this.#flushing) {
      await this.#flushing;
      if (this.#pending.length) await this.#flush();
      return;
    }
    const batch = this.#pending.splice(0, this.#maxBatch);
    if (!batch.length) return;
    const flushing = (async () => {
      try {
        await this.#storage.appendAndSync(batch.map(value => value.record));
        for (const value of batch) {
          value.apply();
          value.resolve();
        }
      } catch (error) {
        const wrapped = error instanceof ClaimJournalError
          ? error
          : new ClaimJournalError("Journal storage append failed", "storage");
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
