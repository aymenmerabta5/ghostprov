import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  ClaimJournal,
  ClaimJournalError,
  type JournalRecord,
  type JournalStorage,
} from "../src/v2/sidecar/journal";

const REQUEST = "req-journal-1";
const DIGEST = "a".repeat(64);

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

  constructor(records: JournalRecord[] = []) {
    this.records = [...records];
  }

  async appendAndSync(records: readonly JournalRecord[]): Promise<void> {
    this.records.push(...records.map(record => ({ ...record })));
    this.syncedBatches++;
    this.timeline.push(`sync:${records.map(record => record.state).join(",")}`);
  }

  async writeTerminal(requestId: string, bytes: Uint8Array): Promise<void> {
    this.terminals.set(requestId, bytes.slice());
    this.timeline.push("terminal-spool");
  }

  async readTerminal(requestId: string): Promise<Uint8Array | null> {
    return this.terminals.get(requestId)?.slice() ?? null;
  }

  async deleteTerminal(requestId: string): Promise<void> {
    this.terminals.delete(requestId);
    this.timeline.push("terminal-delete");
  }

  async readRecords(): Promise<readonly JournalRecord[]> {
    return this.records.map(record => ({ ...record }));
  }
}

async function flushMicrotasks(): Promise<void> {
  for (let index = 0; index < 8; index++) await Promise.resolve();
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
    });
    expect(await terminalJournal.recover()).toEqual({
      terminals: [REQUEST],
      ambiguous: [],
      retryable: [],
    });

    const ambiguousJournal = new ClaimJournal({
      storage: new FakeJournalStorage([record(1, "claimed"), record(2, "fetch_starting")]),
      clock: new ManualClock(),
      flushIntervalMs: 2,
      maxBatch: 64,
    });
    expect(await ambiguousJournal.recover()).toEqual({
      terminals: [],
      ambiguous: [REQUEST],
      retryable: [],
    });
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
});
