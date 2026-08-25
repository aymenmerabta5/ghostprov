import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  ClaimJournal,
  ClaimJournalError,
  type JournalRecord,
  type JournalStorage,
} from "../src/v2/sidecar/journal";
import {
  PROVIDER_ERROR_PAYLOAD_MAX_BYTES,
  type ProviderErrorEnvelope,
} from "../src/v2/provider-error";
import { DenoJournalStorage } from "../sandbox/v2/journal-storage";

const REQUEST = "req-journal-1";
const DIGEST = "a".repeat(64);

function providerErrorEnvelope(): ProviderErrorEnvelope {
  return {
    version: 1,
    origin: "http",
    status: 429,
    error: {
      message: "Rate limit reached for gpt-5.6-sol",
      type: "rate_limit_error",
      param: null,
      code: "rate_limit_exceeded",
    },
    headers: {
      "retry-after": "0.914",
      "x-ratelimit-limit-tokens": "40000000",
      "x-ratelimit-remaining-tokens": "0",
    },
    upstreamRequestId: "req_upstream_1",
    model: "gpt-5.6-sol",
    organizationFingerprint: "b".repeat(64),
    payload: {
      format: "json",
      text: JSON.stringify({
        error: {
          message: "Rate limit reached for gpt-5.6-sol",
          type: "rate_limit_error",
          param: null,
          code: "rate_limit_exceeded",
        },
      }),
      truncated: false,
    },
  };
}

class ManualClock implements Clock {
  value = 1_000;
  readonly waiters: Array<{ deadline: number; resolve: () => void }> = [];
  now(): number { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return this.waitUntil(this.value + ms, signal);
  }
  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise(resolve => this.waiters.push({ deadline, resolve }));
  }
  async advanceBy(ms: number): Promise<void> {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.deadline > this.value) continue;
      this.waiters.splice(this.waiters.indexOf(waiter), 1);
      waiter.resolve();
    }
    for (let index = 0; index < 8; index++) await Promise.resolve();
  }
}

class FakeJournalStorage implements JournalStorage {
  readonly records: JournalRecord[];
  readonly terminals = new Map<string, Uint8Array>();
  readonly timeline: string[] = [];
  syncedBatches = 0;
  replacements = 0;
  deleteFailures = 0;
  appendFailures = 0;
  appendAfterPersistFailures = 0;
  replaceFailures = 0;
  pruneFailures = 0;
  terminalReads = 0;
  terminalStats = 0;

  constructor(records: JournalRecord[] = []) {
    this.records = [...records];
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    if (this.appendFailures > 0) {
      this.appendFailures--;
      throw new Error("journal append failed");
    }
    this.records.push(...records.map(record => ({ ...record })));
    this.syncedBatches++;
    this.timeline.push(`sync:${records.map(record => record.state).join(",")}`);
    if (this.appendAfterPersistFailures > 0) {
      this.appendAfterPersistFailures--;
      throw new Error("post-durable cleanup failed");
    }
  }

  async replaceRecords(records: readonly JournalRecord[]): Promise<void> {
    if (this.replaceFailures > 0) {
      this.replaceFailures--;
      throw new Error("journal replacement failed");
    }
    this.records.splice(0, this.records.length, ...records.map(record => ({ ...record })));
    this.replacements++;
    this.timeline.push("compact");
  }

  async writeTerminal(requestId: string, bytes: Uint8Array): Promise<void> {
    this.terminals.set(requestId, bytes.slice());
    this.timeline.push("terminal-spool");
  }

  async readTerminal(requestId: string): Promise<Uint8Array | null> {
    this.terminalReads++;
    return this.terminals.get(requestId)?.slice() ?? null;
  }

  async terminalExists(requestId: string): Promise<boolean> {
    this.terminalStats++;
    return this.terminals.has(requestId);
  }

  async deleteTerminal(requestId: string): Promise<void> {
    if (this.deleteFailures > 0) {
      this.deleteFailures--;
      throw new Error("terminal delete failed");
    }
    this.terminals.delete(requestId);
    this.timeline.push("terminal-delete");
  }

  async pruneTerminals(retainedRequestIds: readonly string[]): Promise<void> {
    if (this.pruneFailures > 0) {
      this.pruneFailures--;
      throw new Error("terminal pruning failed");
    }
    const retained = new Set(retainedRequestIds);
    for (const requestId of this.terminals.keys()) {
      if (!retained.has(requestId)) this.terminals.delete(requestId);
    }
    this.timeline.push("terminal-prune");
  }

  async readRecords(): Promise<readonly JournalRecord[]> {
    return this.records.map(record => ({ ...record }));
  }
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 8; index++) await Promise.resolve();
}

async function settle<T>(clock: ManualClock, promise: Promise<T>): Promise<T> {
  await flushMicrotasks();
  await clock.advanceBy(2);
  return promise;
}

function record(
  sequence: number,
  state: JournalRecord["state"],
  extra: Partial<JournalRecord> = {},
): JournalRecord {
  return {
    version: 1,
    sequence,
    requestId: REQUEST,
    requestDigest: DIGEST,
    leaderGeneration: 3,
    state,
    recordedAtMs: 1_000 + sequence,
    ...extra,
  };
}

function acknowledgedRecords(
  requestId: string,
  digest: string,
  firstSequence: number,
): JournalRecord[] {
  return [
    record(firstSequence, "claimed", { requestId, requestDigest: digest }),
    record(firstSequence + 1, "fetch_starting", { requestId, requestDigest: digest }),
    record(firstSequence + 2, "terminal", {
      requestId,
      requestDigest: digest,
      responseId: `resp-${requestId}`,
      terminalDigest: digest,
    }),
    record(firstSequence + 3, "acked", { requestId, requestDigest: digest }),
  ];
}

describe("ClaimJournal", () => {
  it("group-commits CLAIMED before resolving and FETCH_STARTING before fetch", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });

    const claimed = journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    });
    await flushMicrotasks();
    expect(storage.syncedBatches).toBe(0);
    await clock.advanceBy(2);
    await claimed;
    expect(storage.records.map(value => value.state)).toEqual(["claimed"]);
    expect(journal.canRetry(REQUEST)).toBe(true);

    const fetchStarting = journal.markFetchStarting(REQUEST);
    await flushMicrotasks();
    await clock.advanceBy(2);
    await fetchStarting;
    expect(storage.records.map(value => value.state)).toEqual(["claimed", "fetch_starting"]);
    expect(journal.canRetry(REQUEST)).toBe(false);
  });

  it("batches independent claims into one durable append", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const first = journal.claim({ requestId: "req-a", requestDigest: "a".repeat(64), leaderGeneration: 1 });
    const second = journal.claim({ requestId: "req-b", requestDigest: "b".repeat(64), leaderGeneration: 1 });
    await flushMicrotasks();
    await clock.advanceBy(2);
    await Promise.all([first, second]);

    expect(storage.syncedBatches).toBe(1);
    expect(storage.records.map(value => value.requestId)).toEqual(["req-a", "req-b"]);
  });

  it("flushes pending mutations before lifecycle ownership is released", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 60_000, maxBatch: 64 });
    const claimed = journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    });
    await flushMicrotasks();

    await journal.close();
    await claimed;
    expect(storage.records.map(value => value.state)).toEqual(["claimed"]);
    await expect(journal.claim({
      requestId: "req-after-close",
      requestDigest: "b".repeat(64),
      leaderGeneration: 3,
    })).rejects.toThrow("Journal is closed");
  });

  it("persists terminal spool before its record and deletes only after durable ACK", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    let pending = journal.claim({ requestId: REQUEST, requestDigest: DIGEST, leaderGeneration: 3 });
    await flushMicrotasks(); await clock.advanceBy(2); await pending;
    pending = journal.markFetchStarting(REQUEST);
    await flushMicrotasks(); await clock.advanceBy(2); await pending;

    const terminalBytes = new TextEncoder().encode("terminal transcript");
    const terminal = journal.markTerminal(
      REQUEST,
      "resp_1",
      "c".repeat(64),
      terminalBytes,
    );
    await flushMicrotasks();
    expect(storage.timeline.at(-1)).toBe("terminal-spool");
    await clock.advanceBy(2);
    await terminal;
    expect(storage.timeline.slice(-2)).toEqual(["terminal-spool", "sync:terminal"]);
    expect(await storage.readTerminal(REQUEST)).toEqual(terminalBytes);

    const ack = journal.acknowledgeTerminal(REQUEST);
    await flushMicrotasks(); await clock.advanceBy(2); await ack;
    expect(storage.timeline.slice(-2)).toEqual(["sync:acked", "terminal-delete"]);
    expect(await storage.readTerminal(REQUEST)).toBeNull();
  });

  it("replays proven terminals and preserves unresolved fetch ambiguity", async () => {
    const terminalStorage = new FakeJournalStorage([
      record(1, "claimed"),
      record(2, "fetch_starting"),
      record(3, "terminal", { responseId: "resp_1", terminalDigest: "c".repeat(64) }),
    ]);
    terminalStorage.terminals.set(REQUEST, new TextEncoder().encode("terminal"));
    const terminalJournal = new ClaimJournal({
      storage: terminalStorage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });
    expect(await terminalJournal.recover()).toEqual({
      terminals: [REQUEST],
      providerErrors: [],
      cancelled: [],
      forcedAmbiguous: [],
      ambiguous: [],
      retryable: [],
    });
    expect(terminalStorage.terminalStats).toBe(1);
    expect(terminalStorage.terminalReads).toBe(0);

    const ambiguousJournal = new ClaimJournal({
      storage: new FakeJournalStorage([record(1, "claimed"), record(2, "fetch_starting")]),
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    expect(await ambiguousJournal.recover()).toEqual({
      terminals: [],
      providerErrors: [],
      cancelled: [],
      forcedAmbiguous: [],
      ambiguous: [REQUEST],
      retryable: [],
    });
  });

  it("recovers a definitive provider 429 without reclassifying it as ambiguous", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const providerError = providerErrorEnvelope();
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    await settle(clock, journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "Rate limit reached in organization org-test; authorization=secret-canary",
      rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
      providerError,
    }));
    providerError.payload.text = "caller-mutated-after-durable-mark";

    const restarted = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    const recovered = await restarted.recover();
    expect(recovered.ambiguous).toEqual([]);
    expect(recovered.providerErrors).toEqual([{
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
      error: {
        status: 429,
        code: "rate_limit_exceeded",
        message: "Rate limit reached in organization org-test; authorization=[REDACTED]",
        rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
        providerError: providerErrorEnvelope(),
      },
    }]);
    expect((await restarted.readRecoveredOutcome(REQUEST))?.outcome.kind).toBe("provider_error");

    await settle(clock, restarted.acknowledgeOutcome(REQUEST));
    const afterAck = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    expect((await afterAck.recover()).providerErrors).toEqual([]);
    expect(afterAck.status(REQUEST)).toBe("acked");
  });

  it("recovers a provider error when the append persisted before process failure", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    storage.appendAfterPersistFailures = 1;
    const providerError = providerErrorEnvelope();
    await expect(settle(clock, journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "Rate limit reached for org-test",
      rateLimit: { retryAfterMs: 136 },
      providerError,
    }))).rejects.toMatchObject({ code: "storage" });

    const restarted = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const recovered = await restarted.recover();
    expect(recovered.providerErrors).toHaveLength(1);
    expect(recovered.providerErrors[0]?.error.code).toBe("rate_limit_exceeded");
    expect(recovered.providerErrors[0]?.error.providerError).toEqual(providerError);
    expect(recovered.ambiguous).toEqual([]);
  });

  it("preserves a bounded provider envelope through exclusive compaction and restart", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      compactAfterRecords: 1,
      withExclusiveOwnership: async operation => operation(),
    });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    const providerError = providerErrorEnvelope();
    await settle(clock, journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "Rate limit reached for org-test",
      rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
      providerError,
    }));

    expect(storage.replacements).toBeGreaterThan(0);
    const persisted = storage.records.find(value => value.state === "provider_error");
    expect(persisted?.outcome?.kind).toBe("provider_error");
    if (persisted?.outcome?.kind === "provider_error") {
      expect(persisted.outcome.error.providerError).toEqual(providerError);
    }

    const restarted = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      compactAfterRecords: 1,
      withExclusiveOwnership: async operation => operation(),
    });
    expect((await restarted.recover()).providerErrors).toEqual([{
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
      error: {
        status: 429,
        code: "rate_limit_exceeded",
        message: "Rate limit reached for org-test",
        rateLimit: { retryAfterMs: 914, limitTokens: 40_000_000, remainingTokens: 0 },
        providerError,
      },
    }]);
  });

  it("fails closed for invalid or oversized provider envelopes at append and recovery", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    const invalid = {
      ...providerErrorEnvelope(),
      unexpected: true,
    } as unknown as ProviderErrorEnvelope;
    await expect(journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "Rate limit reached for org-test",
      providerError: invalid,
    })).rejects.toMatchObject({ code: "transition" });

    const oversized = providerErrorEnvelope();
    oversized.payload.text = "x".repeat(PROVIDER_ERROR_PAYLOAD_MAX_BYTES + 1);
    await expect(journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "Rate limit reached for org-test",
      rateLimit: { retryAfterMs: 914 },
      providerError: oversized,
    })).rejects.toMatchObject({ code: "transition" });
    expect(journal.status(REQUEST)).toBe("fetch_starting");
    expect(storage.records).toHaveLength(2);

    const corruptStorage = new FakeJournalStorage([
      record(1, "claimed"),
      record(2, "fetch_starting"),
      record(3, "provider_error", {
        outcome: {
          kind: "provider_error",
          error: {
            status: 429,
            code: "rate_limit_exceeded",
            message: "Rate limit reached for org-test",
            rateLimit: { retryAfterMs: 914 },
            providerError: oversized,
          },
        },
      }),
    ]);
    const restarted = new ClaimJournal({
      storage: corruptStorage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    await expect(restarted.recover()).rejects.toMatchObject({ code: "corrupt" });
  });

  it("keeps client cancellation and forced ambiguity distinct across restart", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: "req-cancelled",
      requestDigest: "c".repeat(64),
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markCancelled("req-cancelled"));
    await settle(clock, journal.claim({
      requestId: "req-forced",
      requestDigest: "f".repeat(64),
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting("req-forced"));
    await settle(clock, journal.markForcedAmbiguous("req-forced", {
      code: "forced_drain_timeout",
      message: "forced drain could not prove completion",
    }));

    const restarted = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const recovered = await restarted.recover();
    expect(recovered.cancelled).toEqual(["req-cancelled"]);
    expect(recovered.forcedAmbiguous).toEqual([{
      requestId: "req-forced",
      requestDigest: "f".repeat(64),
      leaderGeneration: 3,
      error: {
        code: "forced_drain_timeout",
        message: "forced drain could not prove completion",
      },
    }]);
    expect(recovered.ambiguous).toEqual([]);
    expect(recovered.providerErrors).toEqual([]);

    await settle(clock, restarted.acknowledgeOutcome("req-cancelled"));
    await settle(clock, restarted.acknowledgeOutcome("req-forced"));
    const afterAck = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const settled = await afterAck.recover();
    expect(settled.cancelled).toEqual([]);
    expect(settled.forcedAmbiguous).toEqual([]);
    expect(afterAck.status("req-cancelled")).toBe("acked");
    expect(afterAck.status("req-forced")).toBe("acked");
  });

  it("rejects conflicting claims and corrupt transition history", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const first = journal.claim({ requestId: REQUEST, requestDigest: DIGEST, leaderGeneration: 3 });
    await flushMicrotasks(); await clock.advanceBy(2); await first;
    await expect(journal.claim({
      requestId: REQUEST,
      requestDigest: "f".repeat(64),
      leaderGeneration: 3,
    })).rejects.toBeInstanceOf(ClaimJournalError);

    const corrupt = new ClaimJournal({
      storage: new FakeJournalStorage([record(2, "fetch_starting")]),
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    await expect(corrupt.recover()).rejects.toMatchObject({ code: "corrupt" });
  });

  it("never compacts or prunes without an explicit exclusive-ownership proof", async () => {
    const storage = new FakeJournalStorage([
      ...acknowledgedRecords("req-a", "a".repeat(64), 1),
      ...acknowledgedRecords("req-b", "b".repeat(64), 5),
    ]);
    storage.terminals.set("req-orphan", new TextEncoder().encode("orphan"));
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 1,
      compactAfterRecords: 1,
    });

    await journal.recover();
    expect(storage.replacements).toBe(0);
    expect(storage.terminals.has("req-orphan")).toBe(true);
    expect(journal.status("req-a")).toBe("acked");
    expect(journal.status("req-b")).toBe("acked");

    const unowned = new DenoJournalStorage("/tmp/ghostprov-v2-unowned-test");
    expect(unowned.replaceRecords).toBeUndefined();
    expect(unowned.pruneTerminals).toBeUndefined();
    const owned = new DenoJournalStorage({
      root: "/tmp/ghostprov-v2-owned-test",
      withExclusiveOwnership: async operation => operation(),
    });
    expect(typeof owned.replaceRecords).toBe("function");
    expect(typeof owned.pruneTerminals).toBe("function");
  });

  it("compacts a large legacy ACK history with one exclusive rewrite", async () => {
    const records: JournalRecord[] = [];
    for (let index = 0; index < 100; index++) {
      records.push(...acknowledgedRecords(
        `req-${index}`,
        index.toString(16).padStart(64, "0"),
        index * 4 + 1,
      ));
    }
    const storage = new FakeJournalStorage(records);
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 10,
      compactAfterRecords: 8,
      withExclusiveOwnership: async operation => operation(),
    });

    await journal.recover();
    expect(storage.replacements).toBe(1);
    expect(storage.records).toHaveLength(40);
    expect(journal.status("req-0")).toBeNull();
    expect(journal.status("req-99")).toBe("acked");
  });

  it("retains duplicate fences when optional startup compaction fails", async () => {
    const storage = new FakeJournalStorage([
      ...acknowledgedRecords("req-a", "a".repeat(64), 1),
      ...acknowledgedRecords("req-b", "b".repeat(64), 5),
    ]);
    storage.replaceFailures = 1;
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 1,
      withExclusiveOwnership: async operation => operation(),
    });

    await expect(journal.recover()).resolves.toBeDefined();
    expect(journal.status("req-a")).toBe("acked");
    expect(journal.status("req-b")).toBe("acked");
    expect(storage.records).toHaveLength(8);
  });

  it("fails closed when exclusive ownership is lost during maintenance", async () => {
    const storage = new FakeJournalStorage([
      ...acknowledgedRecords("req-a", "a".repeat(64), 1),
      ...acknowledgedRecords("req-b", "b".repeat(64), 5),
    ]);
    const ownershipError = new Error("ownership lost");
    ownershipError.name = "SidecarOwnershipError";
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 1,
      withExclusiveOwnership: async () => { throw ownershipError; },
    });

    await expect(journal.recover()).rejects.toMatchObject({ code: "storage" });
  });

  it("retains recent ACK digests while expiring tombstones beyond their TTL", async () => {
    const old = acknowledgedRecords("req-old", "a".repeat(64), 1)
      .map((value, index) => ({ ...value, recordedAtMs: 800 + index }));
    const recent = acknowledgedRecords("req-recent", "b".repeat(64), 5)
      .map((value, index) => ({ ...value, recordedAtMs: 950 + index }));
    const storage = new FakeJournalStorage([...old, ...recent]);
    const clock = new ManualClock();
    clock.value = 1_000;
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 10,
      acknowledgedRetentionMs: 100,
      withExclusiveOwnership: async operation => operation(),
    });

    await journal.recover();
    expect(journal.status("req-old")).toBeNull();
    expect(journal.status("req-recent")).toBe("acked");
    expect(storage.records.every(value => value.requestId === "req-recent")).toBe(true);
  });

  it("bounds acknowledged tombstones and compacts their transition chains", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 1,
      compactAfterRecords: 1,
      withExclusiveOwnership: async operation => operation(),
    });

    const complete = async (requestId: string, digest: string) => {
      await settle(clock, journal.claim({ requestId, requestDigest: digest, leaderGeneration: 1 }));
      await settle(clock, journal.markFetchStarting(requestId));
      await settle(clock, journal.markTerminal(
        requestId,
        `resp-${requestId}`,
        digest,
        new TextEncoder().encode(`terminal-${requestId}`),
      ));
      await settle(clock, journal.acknowledgeTerminal(requestId));
    };

    await complete("req-a", "a".repeat(64));
    await complete("req-b", "b".repeat(64));
    // The over-quota fence remains live until an exclusive atomic rewrite
    // proves its durable transition chain was removed.
    expect(journal.status("req-a")).toBe("acked");
    expect(journal.status("req-b")).toBe("acked");

    // The next durable append performs the rewrite that drops req-a's old
    // transition chain without renumbering the surviving journal sequence.
    await settle(clock, journal.claim({
      requestId: "req-c",
      requestDigest: "c".repeat(64),
      leaderGeneration: 1,
    }));
    expect(storage.records.some(value => value.requestId === "req-a")).toBe(false);
    expect(storage.records.some(value => value.requestId === "req-b")).toBe(true);
    expect(storage.records.some(value => value.requestId === "req-c")).toBe(true);
    expect(storage.replacements).toBeGreaterThan(0);

    // The retained tombstone still fences the newest acknowledged request
    // after compaction and across restart.
    await journal.claim({ requestId: "req-b", requestDigest: "b".repeat(64), leaderGeneration: 1 });
    expect(journal.status("req-b")).toBe("acked");
    await expect(journal.claim({
      requestId: "req-b",
      requestDigest: "d".repeat(64),
      leaderGeneration: 1,
    })).rejects.toMatchObject({ code: "conflict" });
    const restarted = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxAcknowledged: 1,
      compactAfterRecords: 1,
      withExclusiveOwnership: async operation => operation(),
    });
    await restarted.recover();
    expect(restarted.status("req-b")).toBe("acked");
  });

  it("retries terminal cleanup after the ACK record is already durable", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    await settle(clock, journal.markTerminal(
      REQUEST,
      "resp_1",
      "c".repeat(64),
      new TextEncoder().encode("terminal"),
    ));

    storage.deleteFailures = 1;
    await expect(settle(clock, journal.acknowledgeTerminal(REQUEST))).rejects.toMatchObject({
      code: "storage",
    });
    expect(journal.status(REQUEST)).toBe("acked");
    expect(storage.terminals.has(REQUEST)).toBe(true);

    await journal.acknowledgeTerminal(REQUEST);
    expect(storage.terminals.has(REQUEST)).toBe(false);
  });

  it("preserves a valid terminal when append persisted before a cleanup failure", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    storage.appendAfterPersistFailures = 1;

    await expect(settle(clock, journal.markTerminal(
      REQUEST,
      "resp_1",
      "c".repeat(64),
      new TextEncoder().encode("terminal"),
    ))).rejects.toMatchObject({ code: "storage" });
    expect(storage.terminals.has(REQUEST)).toBe(true);
    expect(journal.status(REQUEST)).toBe("fetch_starting");
    await expect(journal.claim({
      requestId: "req-after-uncertain-append",
      requestDigest: "d".repeat(64),
      leaderGeneration: 3,
    })).rejects.toMatchObject({ code: "storage" });
    const restarted = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });
    expect((await restarted.recover()).terminals).toEqual([REQUEST]);
  });

  it("prunes orphan terminal spools during initialization", async () => {
    const storage = new FakeJournalStorage([
      record(1, "claimed"),
      record(2, "fetch_starting"),
      record(3, "terminal", { responseId: "resp_1", terminalDigest: "c".repeat(64) }),
    ]);
    storage.terminals.set(REQUEST, new TextEncoder().encode("retained"));
    storage.terminals.set("req-orphan", new TextEncoder().encode("orphan"));
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });

    expect((await journal.recover()).terminals).toEqual([REQUEST]);
    expect(storage.terminals.has(REQUEST)).toBe(true);
    expect(storage.terminals.has("req-orphan")).toBe(false);
  });

  it("keeps startup recovery available when optional orphan cleanup fails", async () => {
    const storage = new FakeJournalStorage([
      record(1, "claimed"),
      record(2, "fetch_starting"),
      record(3, "terminal", { responseId: "resp_1", terminalDigest: "c".repeat(64) }),
    ]);
    storage.terminals.set(REQUEST, new TextEncoder().encode("retained"));
    storage.terminals.set("req-orphan", new TextEncoder().encode("orphan"));
    storage.pruneFailures = 1;
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });

    expect((await journal.recover()).terminals).toEqual([REQUEST]);
    expect(storage.terminals.has("req-orphan")).toBe(true);
  });
});
