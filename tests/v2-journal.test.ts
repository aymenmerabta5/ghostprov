import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  ClaimJournal,
  ClaimJournalError,
  JournalStorageCapacityError,
  JournalStorageDurabilityError,
  V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES,
  journalStoredRecordBytes,
  type JournalRecord,
  type JournalStorage,
  type JournalTerminalSpool,
} from "../src/v2/sidecar/journal";
import { sha256Hex, utf8Bytes } from "../src/v2/protocol/shared";
import { IncrementalSha256 } from "../src/v2/protocol/transcript";
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
  replaceDurabilityFailures = 0;
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

  async beginTerminalSpool(requestId: string, maxBytes: number): Promise<JournalTerminalSpool> {
    const chunks: Uint8Array[] = [];
    const hasher = new IncrementalSha256();
    let byteCount = 0;
    let active = true;
    const storage = this;
    return {
      requestId,
      maxBytes,
      get byteCount() { return byteCount; },
      async append(bytes) {
        if (!active || byteCount + bytes.byteLength > maxBytes) throw new Error("spool bounds");
        chunks.push(bytes.slice());
        hasher.update(bytes);
        byteCount += bytes.byteLength;
      },
      async promote(proof) {
        if (!active || proof.byteCount !== byteCount) throw new Error("spool proof");
        const value = new Uint8Array(byteCount);
        let offset = 0;
        for (const chunk of chunks) { value.set(chunk, offset); offset += chunk.byteLength; }
        if (hasher.digestHex() !== proof.transcriptSha256) throw new Error("spool digest");
        storage.terminals.set(requestId, value);
        storage.timeline.push("terminal-spool");
        active = false;
        chunks.length = 0;
      },
      async abort() { active = false; chunks.length = 0; },
    };
  }

  async replaceRecords(records: readonly JournalRecord[]): Promise<void> {
    if (this.replaceDurabilityFailures > 0) {
      this.replaceDurabilityFailures--;
      throw new JournalStorageDurabilityError("directory fsync failed after rename");
    }
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
  let settled = false;
  void promise.finally(() => { settled = true; }).catch(() => undefined);
  for (let round = 0; round < 32 && !settled; round++) {
    await flushMicrotasks();
    await clock.advanceBy(2);
  }
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
  it("rejects terminal spool disk limits that cannot contain one full spool", () => {
    expect(() => new DenoJournalStorage({
      maxTerminalSpoolBytes: 64,
      maxTerminalBytes: 63,
      withExclusiveOwnership: async operation => operation(),
    })).toThrow("terminal spool limits");
  });

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

  it("bounds unacknowledged request metadata independently of transcript bytes", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxUnacknowledgedRequests: 1,
    });
    await settle(clock, journal.claim({
      requestId: "req-first",
      requestDigest: "a".repeat(64),
      leaderGeneration: 3,
    }));
    await expect(journal.claim({
      requestId: "req-second",
      requestDigest: "b".repeat(64),
      leaderGeneration: 3,
    })).rejects.toMatchObject({ code: "capacity" });
    await settle(clock, journal.markCancelled("req-first"));
    await settle(clock, journal.acknowledgeOutcome("req-first"));
    await settle(clock, journal.claim({
      requestId: "req-second",
      requestDigest: "b".repeat(64),
      leaderGeneration: 3,
    }));
    expect(journal.status("req-second")).toBe("claimed");
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

  it("promotes an incremental spool only after its exact terminal proof", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({ requestId: REQUEST, requestDigest: DIGEST, leaderGeneration: 3 }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    const spool = await journal.beginTerminalSpool(REQUEST);
    const first = utf8Bytes("event: response.created\ndata: {}\n\n");
    const second = utf8Bytes("event: response.completed\ndata: {}\n\n");
    await spool.append(first);
    await spool.append(second);
    const bytes = new Uint8Array(first.byteLength + second.byteLength);
    bytes.set(first);
    bytes.set(second, first.byteLength);
    const digest = await sha256Hex(bytes);

    await settle(clock, journal.markTerminalFromSpool(
      REQUEST,
      "resp_1",
      digest,
      bytes.byteLength,
      spool,
    ));
    expect(storage.timeline.slice(-2)).toEqual(["terminal-spool", "sync:terminal"]);
    expect(await storage.readTerminal(REQUEST)).toEqual(bytes);
  });

  it("keeps an oversized terminal spool request-local instead of poisoning the journal", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxTerminalSpoolBytes: 4,
      terminalSpoolBufferBytes: 4,
    });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    const spool = await journal.beginTerminalSpool(REQUEST);
    await expect(spool.append(new Uint8Array(5))).rejects.toMatchObject({ code: "capacity" });
    await spool.abort();
    await settle(clock, journal.markCancelled(REQUEST));
    expect(journal.status(REQUEST)).toBe("cancelled");
  });

  it("keeps a terminal spool proof mismatch request-local", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));
    const spool = await journal.beginTerminalSpool(REQUEST);
    await spool.append(utf8Bytes("proof"));
    await expect(journal.markTerminalFromSpool(
      REQUEST,
      "resp-proof",
      "b".repeat(64),
      4,
      spool,
    )).rejects.toMatchObject({ code: "conflict" });
    await spool.abort();
    await settle(clock, journal.markForcedAmbiguous(REQUEST));
    expect(journal.status(REQUEST)).toBe("forced_ambiguous");
  });

  it("treats pre-fetch spool reservation exhaustion as nonfatal capacity", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    storage.beginTerminalSpool = async () => {
      throw new JournalStorageCapacityError("spool reservation exhausted");
    };
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await expect(journal.beginTerminalSpool(REQUEST)).rejects.toMatchObject({
      code: "capacity",
    });
    expect(journal.status(REQUEST)).toBe("claimed");
    await settle(clock, journal.markCancelled(REQUEST));
    await settle(clock, journal.acknowledgeOutcome(REQUEST));
    expect(journal.status(REQUEST)).toBe("acked");
  });

  it("cleans temp spools and orphaned promotions across crash boundaries", async () => {
    const prepare = async () => {
      const clock = new ManualClock();
      const storage = new FakeJournalStorage();
      const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
      await settle(clock, journal.claim({ requestId: REQUEST, requestDigest: DIGEST, leaderGeneration: 3 }));
      await settle(clock, journal.markFetchStarting(REQUEST));
      const spool = await journal.beginTerminalSpool(REQUEST);
      const bytes = utf8Bytes("event: response.completed\ndata: {}\n\n");
      await spool.append(bytes);
      return { clock, storage, journal, spool, bytes, digest: await sha256Hex(bytes) };
    };

    const beforePromote = await prepare();
    await beforePromote.spool.abort();
    const beforeRestart = new ClaimJournal({
      storage: beforePromote.storage,
      clock: beforePromote.clock,
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    expect((await beforeRestart.recover()).ambiguous).toEqual([REQUEST]);
    expect(beforePromote.storage.terminals.has(REQUEST)).toBe(false);

    const orphan = await prepare();
    orphan.storage.appendFailures = 1;
    await expect(settle(orphan.clock, orphan.journal.markTerminalFromSpool(
      REQUEST,
      "resp_1",
      orphan.digest,
      orphan.bytes.byteLength,
      orphan.spool,
    ))).rejects.toMatchObject({ code: "storage" });
    expect(orphan.storage.terminals.has(REQUEST)).toBe(true);
    const orphanRestart = new ClaimJournal({
      storage: orphan.storage,
      clock: orphan.clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });
    expect((await orphanRestart.recover()).ambiguous).toEqual([REQUEST]);
    expect(orphan.storage.terminals.has(REQUEST)).toBe(false);

    const afterPromote = await prepare();
    afterPromote.storage.appendAfterPersistFailures = 1;
    await expect(settle(afterPromote.clock, afterPromote.journal.markTerminalFromSpool(
      REQUEST,
      "resp_1",
      afterPromote.digest,
      afterPromote.bytes.byteLength,
      afterPromote.spool,
    ))).rejects.toMatchObject({ code: "storage" });
    const afterRestart = new ClaimJournal({
      storage: afterPromote.storage,
      clock: afterPromote.clock,
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    expect((await afterRestart.recover()).terminals).toEqual([REQUEST]);
  });

  it("replays proven terminals and preserves unresolved fetch ambiguity", async () => {
    const terminalStorage = new FakeJournalStorage([
      record(1, "claimed"),
      record(2, "fetch_starting"),
      record(3, "terminal", {
        responseId: "resp_1",
        terminalDigest: "c".repeat(64),
        prelude: {
          status: 200,
          contentType: "text/event-stream; charset=utf-8",
          upstreamRequestId: "req_exact_prelude",
        },
      }),
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
    const opened = await terminalJournal.openRecoveredTerminal(REQUEST);
    expect(opened?.prelude).toEqual({
      status: 200,
      contentType: "text/event-stream; charset=utf-8",
      upstreamRequestId: "req_exact_prelude",
    });
    opened?.reader.close();

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

  it("reserves worst-case provider outcomes and never creates an unrestartable journal", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const maxJournalBytes = 64 * 1024;
    const maxJournalRecords = 100;
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      compactAfterRecords: 1,
      maxJournalBytes,
      maxJournalRecords,
      maxUnacknowledgedRequests: 100,
      withExclusiveOwnership: async operation => operation(),
    });
    const maximum = providerErrorEnvelope();
    maximum.payload = {
      format: "text",
      text: "x".repeat(PROVIDER_ERROR_PAYLOAD_MAX_BYTES),
      truncated: true,
    };
    const accepted: string[] = [];
    let rejectedId = "";
    for (let index = 0; index < 20; index++) {
      const requestId = `req-provider-cap-${index}`;
      const digest = index.toString(16).padStart(64, "0");
      try {
        await settle(clock, journal.claim({ requestId, requestDigest: digest, leaderGeneration: 3 }));
      } catch (error) {
        expect(error).toMatchObject({ code: "capacity" });
        rejectedId = requestId;
        break;
      }
      await settle(clock, journal.markProviderError(requestId, {
        status: 429,
        code: "rate_limit_exceeded",
        message: "Rate limit reached",
        providerError: maximum,
      }, {
        status: 200,
        contentType: "text/event-stream; charset=utf-8",
        upstreamRequestId: "r".repeat(255),
      }));
      accepted.push(requestId);
      expect(storage.records.reduce(
        (total, value) => total + journalStoredRecordBytes(value),
        0,
      )).toBeLessThanOrEqual(maxJournalBytes);
      expect(storage.records.length).toBeLessThanOrEqual(maxJournalRecords);
    }
    expect(accepted.length).toBeGreaterThan(2);
    expect(rejectedId).not.toBe("");
    expect(journal.status(rejectedId)).toBeNull();
    const providerRecord = storage.records.find(value => value.state === "provider_error");
    expect(journalStoredRecordBytes(providerRecord!))
      .toBeLessThanOrEqual(V2_JOURNAL_MAX_OUTCOME_RECORD_BYTES);

    const restarted = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      compactAfterRecords: 1,
      maxJournalBytes,
      maxJournalRecords,
      maxUnacknowledgedRequests: 100,
      withExclusiveOwnership: async operation => operation(),
    });
    const recovered = await restarted.recover();
    expect(recovered.providerErrors.map(value => value.requestId).sort())
      .toEqual([...accepted].sort());
    expect(recovered.providerErrors.every(value => (
      value.error.providerError?.payload.text.length === PROVIDER_ERROR_PAYLOAD_MAX_BYTES
    ))).toBe(true);
  });

  it("reserves the complete future transition count before a new claim", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      maxJournalBytes: 1024 * 1024,
      maxJournalRecords: 3,
    });
    await expect(journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    })).rejects.toMatchObject({ code: "capacity" });
    expect(storage.records).toEqual([]);
    expect(journal.status(REQUEST)).toBeNull();
  });

  it("does not resolve the first claim before the storage directory-entry fence", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    let releaseDirectorySync!: () => void;
    const directorySync = new Promise<void>(resolve => { releaseDirectorySync = resolve; });
    storage.appendAndSync = async records => {
      storage.records.push(...records.map(value => ({ ...value })));
      storage.timeline.push("file-sync");
      await directorySync;
      storage.timeline.push("directory-sync");
      storage.syncedBatches++;
    };
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    let resolved = false;
    const claiming = journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }).then(() => { resolved = true; });
    await flushMicrotasks();
    await clock.advanceBy(2);
    await flushMicrotasks();
    expect(storage.timeline).toEqual(["file-sync"]);
    expect(resolved).toBe(false);
    expect(journal.status(REQUEST)).toBeNull();

    releaseDirectorySync();
    await claiming;
    expect(storage.timeline).toEqual(["file-sync", "directory-sync"]);
    expect(journal.status(REQUEST)).toBe("claimed");
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

  it("accepts an idempotent outcome ACK after compaction removes the acknowledged outcome", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const options = {
      storage,
      clock,
      flushIntervalMs: 2,
      maxBatch: 64,
      compactAfterRecords: 1,
      withExclusiveOwnership: async <T>(operation: () => Promise<T>) => operation(),
    };
    const journal = new ClaimJournal(options);
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "limited",
      providerError: providerErrorEnvelope(),
    }));
    await settle(clock, journal.acknowledgeOutcome(REQUEST));
    await settle(clock, journal.claim({
      requestId: "req-compaction-trigger",
      requestDigest: "c".repeat(64),
      leaderGeneration: 3,
    }));
    const compactedAck = storage.records.find(value => value.requestId === REQUEST);
    expect(compactedAck).toMatchObject({
      state: "acked",
      compacted: true,
    });
    expect(compactedAck).not.toHaveProperty("outcome");

    const restarted = new ClaimJournal(options);
    await restarted.recover();
    await expect(restarted.acknowledgeOutcome(REQUEST)).resolves.toBeUndefined();
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

  it("serializes a forced outcome ahead of a racing provider error", async () => {
    const clock = new ManualClock();
    const storage = new FakeJournalStorage();
    const journal = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    await settle(clock, journal.claim({
      requestId: REQUEST,
      requestDigest: DIGEST,
      leaderGeneration: 3,
    }));
    await settle(clock, journal.markFetchStarting(REQUEST));

    const forced = journal.markForcedAmbiguous(REQUEST, {
      code: "forced_drain_cancelled",
      message: "deadline won",
    });
    const provider = journal.markProviderError(REQUEST, {
      status: 429,
      code: "rate_limit_exceeded",
      message: "late provider error",
      providerError: providerErrorEnvelope(),
    });
    const providerResult = provider.catch(error => error);
    await settle(clock, forced);
    expect(await providerResult).toMatchObject({ code: "conflict" });
    expect(storage.records.filter(value => (
      value.requestId === REQUEST &&
      (value.state === "forced_ambiguous" || value.state === "provider_error")
    ))).toEqual([expect.objectContaining({ state: "forced_ambiguous" })]);

    const restarted = new ClaimJournal({ storage, clock, flushIntervalMs: 2, maxBatch: 64 });
    const recovered = await restarted.recover();
    expect(recovered.forcedAmbiguous).toHaveLength(1);
    expect(recovered.providerErrors).toEqual([]);
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
    expect(storage.records).toHaveLength(10);
    expect(storage.records.every(value => value.compacted === true)).toBe(true);
    expect(journal.status("req-0")).toBeNull();
    expect(journal.status("req-99")).toBe("acked");
  });

  it("prunes only prior leader generations before recovery readiness", async () => {
    const oldId = "req-old-generation";
    const currentId = "req-current-generation";
    const storage = new FakeJournalStorage([
      record(1, "claimed", { requestId: oldId, leaderGeneration: 2 }),
      record(2, "fetch_starting", { requestId: oldId, leaderGeneration: 2 }),
      record(3, "claimed", { requestId: currentId, leaderGeneration: 3 }),
      record(4, "fetch_starting", { requestId: currentId, leaderGeneration: 3 }),
      record(5, "terminal", {
        requestId: currentId,
        leaderGeneration: 3,
        responseId: "resp-current-generation",
        terminalDigest: DIGEST,
      }),
    ]);
    storage.terminals.set(oldId, utf8Bytes("old"));
    storage.terminals.set(currentId, utf8Bytes("current"));
    const journal = new ClaimJournal({
      storage,
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
      withExclusiveOwnership: async operation => operation(),
    });
    expect(await journal.pruneOtherGenerations(3)).toBe(1);
    const recovered = await journal.recover();
    expect(recovered.terminals).toEqual([currentId]);
    expect(recovered.ambiguous).toEqual([]);
    expect(journal.status(oldId)).toBeNull();
    expect(storage.terminals.has(oldId)).toBe(false);
    expect(storage.terminals.has(currentId)).toBe(true);
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

  it("fail-stops after an atomic replacement has uncertain directory durability", async () => {
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
    storage.replaceDurabilityFailures = 1;
    await expect(journal.markCancelled(REQUEST)).rejects.toMatchObject({ code: "storage" });
    await expect(journal.claim({
      requestId: "req-after-uncertain-rename",
      requestDigest: "e".repeat(64),
      leaderGeneration: 3,
    })).rejects.toMatchObject({ code: "storage" });
    expect(storage.records).toHaveLength(1);
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
