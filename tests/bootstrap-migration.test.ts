import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { FileLease, FileLeaseBusyError, FileLeaseOwnershipError } from "../src/core/file-lease";
import type { Clock } from "../src/core/ports";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, readdir, rm, stat, unlink, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { main, parseBootstrapMigrationArguments } from "../scripts/prepare_bootstrap_migration";
import { FileBootstrapHoldMigration, prepareBootstrapHoldMigrationLedger } from "../src/fleet/bootstrap-migration";
import { FleetManifestStore } from "../src/fleet/manifest";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { canonicalJson } from "../src/v2/protocol/shared";
import { V2_R4_RELEASE, V2_R10_RELEASE } from "../src/v2/sidecar/assets";
import { ARCHIVED_R10_RELEASE_IDENTITIES } from "../src/v2/sidecar/retirement-identities";
import { FileSupervisorBootstrapJournal, type BootstrapJournalEntry, type SupervisorBootstrapAttempt } from "../src/v2/supervisor/bootstrap-journal";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const A = projectId("11111111-1111-4111-8111-111111111111");
const B = projectId("22222222-2222-4222-8222-222222222222");
const C = projectId("33333333-3333-4333-8333-333333333333");
const D = projectId("44444444-4444-4444-8444-444444444444");
const E = projectId("55555555-5555-4555-8555-555555555555");
const F = projectId("66666666-6666-4666-8666-666666666666");
const FINGERPRINT = "a".repeat(64);
const TOKEN = "offline-bootstrap-migration-token";
const ROTATION_KEY = Buffer.alloc(32, 6).toString("base64url");
const CONFIRM = "--confirm-legacy-bootstrap-migration";
const RECOVERY_CONFIRM = "--confirm-operator-bootstrap-recovery";
const roots: string[] = [];

interface Fixture {
  readonly root: string;
  readonly manifestFile: string;
  readonly store: FleetManifestStore;
  readonly migration: FileBootstrapHoldMigration;
  readonly attempts: ReadonlyMap<ProjectId, SupervisorBootstrapAttempt>;
}

interface Ledger {
  version: number;
  purpose?: string;
  identityFingerprint: string;
  grants: {
    projectId: string;
    workerIndex: number;
    buildHash: string;
    schemaHash: string;
    bindingSha256: string;
    journalSha256: string;
  }[];
}

afterEach(async () => {
  for (const root of roots.splice(0)) {
    if (path.dirname(root) !== path.resolve(os.tmpdir()) || !path.basename(root).startsWith("ghostprov-bootstrap-migration-")) {
      throw new Error("Unexpected bootstrap migration fixture directory");
    }
    await rm(root, { recursive: true, force: true });
  }
});

async function attempt(
  id: ProjectId,
  release: Pick<BootstrapBinding, "buildHash" | "schemaHash"> = ARCHIVED_R10_RELEASE_IDENTITIES[0]!,
  phase: SupervisorBootstrapAttempt["phase"] = "submitted",
): Promise<SupervisorBootstrapAttempt> {
  const binding: BootstrapBinding = {
    projectId: id,
    sandboxSessionId: "offline-private-sandbox-session",
    leaderGeneration: 1,
    buildHash: release.buildHash,
    schemaHash: release.schemaHash,
    nonce: "offline-private-bootstrap-nonce",
    protocolRevision: "r2",
    schemaVersion: 2,
  };
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const sealed = await sealSupervisorSessionBootstrap({
    sessionId,
    binding,
    payload: {
      instanceId: "offline-private-instance",
      credential: {
        url: "wss://offline.invalid/ws",
        baseUrl: "https://offline.invalid/",
        docId: id,
        token: TOKEN,
        authorization: "full",
        rotationKey: ROTATION_KEY,
      },
    },
  });
  return { version: 1, phase, sessionId, binding, connectTimeoutMs: 1000, sealed };
}

function journalFile(fixture: Fixture, id: ProjectId): string {
  return path.join(`${fixture.manifestFile}.bootstrap`, `${id}.json`);
}

async function saveAttempt(fixture: Fixture, id: ProjectId, value: SupervisorBootstrapAttempt): Promise<void> {
  const journal = new FileSupervisorBootstrapJournal(`${fixture.manifestFile}.bootstrap`, id);
  await journal.withLease(undefined, entry => entry.save(value));
}

async function fixture(ids: readonly ProjectId[] = [A, B]): Promise<Fixture> {
  const root = path.resolve(await mkdtemp(path.join(os.tmpdir(), "ghostprov-bootstrap-migration-")));
  roots.push(root);
  const manifestFile = path.join(root, "fleet.json");
  const store = new FleetManifestStore({ file: manifestFile, identityFingerprint: FINGERPRINT });
  await store.initialize("authenticated");
  const attempts = new Map<ProjectId, SupervisorBootstrapAttempt>();
  const value: Fixture = {
    root,
    manifestFile,
    store,
    migration: new FileBootstrapHoldMigration({ manifestFile, identityFingerprint: FINGERPRINT }),
    attempts,
  };
  for (const [index, id] of ids.entries()) {
    await store.reserveWorker({ index: index + 1, projectId: id, title: `Offline worker ${index + 1}`, nowMs: 1 });
    await store.nextLeaderGeneration(id, 2);
    await store.nextLeaderGeneration(id, 3);
    await store.markState(id, "failed", 4);
    const pending = await attempt(id, ARCHIVED_R10_RELEASE_IDENTITIES[index % ARCHIVED_R10_RELEASE_IDENTITIES.length]!);
    attempts.set(id, pending);
    await saveAttempt(value, id, pending);
  }
  return value;
}

function prepare(value: Fixture, projectIds: readonly ProjectId[] = [A, B]) {
  return prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile, identityFingerprint: FINGERPRINT, projectIds });
}

function args(manifestFile = "offline-fleet.json", ids: readonly ProjectId[] = [A]): string[] {
  return ["--manifest", manifestFile, "--identity-fingerprint", FINGERPRINT, ...ids.flatMap(id => ["--project", id]), CONFIRM];
}

function recoveryArgs(manifestFile = "offline-fleet.json", ids: readonly ProjectId[] = [A]): string[] {
  return [...args(manifestFile, ids).slice(0, -1), RECOVERY_CONFIRM];
}

function digest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

class PreparationClock implements Clock {
  nowMs = 0;
  readonly sleeps: number[] = [];
  now() { return this.nowMs; }
  async sleep(ms: number, signal?: AbortSignal) {
    signal?.throwIfAborted(); this.sleeps.push(ms); this.nowMs += ms;
  }
  waitUntil(_deadline: number, signal?: AbortSignal): Promise<void> {
    return new Promise((_resolve, reject) => {
      if (signal?.aborted) reject(signal.reason);
      else signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
  }
}

describe("bootstrap migration CLI guards", () => {
  it("accepts only explicit immutable local inputs", () => {
    const parsed = parseBootstrapMigrationArguments(args("offline-fleet.json", [A, B]));
    expect(parsed).toEqual({ manifestFile: path.resolve("offline-fleet.json"), identityFingerprint: FINGERPRINT, projectIds: [A, B] });
    expect(Object.isFrozen(parsed)).toBe(true);
    expect(Object.isFrozen(parsed.projectIds)).toBe(true);
  });

  it("accepts three explicit legacy projects and keeps the bounded input immutable", () => {
    const parsed = parseBootstrapMigrationArguments(args("offline-fleet.json", [A, B, C]));
    expect(parsed.projectIds).toEqual([A, B, C]);
    expect(Object.isFrozen(parsed.projectIds)).toBe(true);
  });

  it.each([4, 5])("requires the operator recovery confirmation for %s exact projects", count => {
    const ids = [A, B, C, D, E].slice(0, count);
    expect(() => parseBootstrapMigrationArguments(args("offline.json", ids))).toThrow();
    expect(parseBootstrapMigrationArguments(recoveryArgs("offline.json", ids)))
      .toMatchObject({ operatorRecovery: true, projectIds: ids });
  });

  it("rejects a sixth operator grant and conflicting or duplicate confirmations", () => {
    for (const input of [recoveryArgs("offline.json", [A, B, C, D, E, F]),
      [...recoveryArgs(), CONFIRM], [...recoveryArgs(), RECOVERY_CONFIRM]]) {
      expect(() => parseBootstrapMigrationArguments(input)).toThrow();
    }
  });

  it.each([
    ["missing manifest", ["--identity-fingerprint", FINGERPRINT, "--project", A, CONFIRM]],
    ["missing fingerprint", ["--manifest", "offline.json", "--project", A, CONFIRM]],
    ["missing project", ["--manifest", "offline.json", "--identity-fingerprint", FINGERPRINT, CONFIRM]],
    ["missing confirmation", args().slice(0, -1)],
    ["duplicate project", args("offline.json", [A, A])],
    ["more than three projects", args("offline.json", [A, B, C, D])],
    ["duplicate manifest", [...args(), "--manifest", "other.json"]],
    ["duplicate fingerprint", [...args(), "--identity-fingerprint", FINGERPRINT]],
    ["duplicate confirmation", [...args(), CONFIRM]],
    ["unknown option", [...args(), "--allow-network"]],
    ["empty manifest", args("")],
    ["option as manifest", args("--project")],
    ["fingerprint newline", args().map(value => value === FINGERPRINT ? `${value}\n` : value)],
    ["fingerprint prefix", args().map(value => value === FINGERPRINT ? `0${value}` : value)],
    ["project newline", args().map(value => value === A ? `${value}\n` : value)],
    ["project suffix", args().map(value => value === A ? `${value}x` : value)],
  ] as const)("rejects %s before opening files", (_label, input) => {
    expect(() => parseBootstrapMigrationArguments(input)).toThrow();
  });

  it("emits only a credential-free result after guarded preparation", async () => {
    const value = await fixture([A]);
    const beforeManifest = await readFile(value.manifestFile, "utf8");
    const beforeJournal = await readFile(journalFile(value, A), "utf8");
    const output: string[] = [];
    await main(args(value.manifestFile), text => output.push(text));
    expect(output).toHaveLength(1);
    expect(JSON.parse(output[0]!)).toEqual({ file: `${value.manifestFile}.bootstrap-migration.json`, grants: 1, created: true });
    expect(await readFile(value.manifestFile, "utf8")).toBe(beforeManifest);
    expect(await readFile(journalFile(value, A), "utf8")).toBe(beforeJournal);
    await main(args(value.manifestFile), text => output.push(text));
    expect(JSON.parse(output[1]!)).toMatchObject({ grants: 1, created: false });
  });

  it("does not stage a ledger when confirmation is omitted", async () => {
    const value = await fixture([A]);
    const before = await readdir(value.root);
    const output: string[] = [];
    await expect(main(args(value.manifestFile).slice(0, -1), text => output.push(text))).rejects.toThrow(CONFIRM);
    expect(await readdir(value.root)).toEqual(before);
    expect(output).toEqual([]);
  });
});

describe("bootstrap hold migration ledger", () => {
  it.each([false, true])("stages five operator grants through staggered busy journals one at a time (existing: %s)", async existing => {
    const ids = [A, B, C, D, E], value = await fixture(ids), clock = new PreparationClock();
    if (existing) await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: ids, operatorRecovery: true });
    const busy = new Map<ProjectId, number>([[A, 1], [B, 2], [D, 1]]);
    const original = FileSupervisorBootstrapJournal.prototype.withLease;
    const visits: ProjectId[] = [];
    let simultaneous = 0, maximum = 0;
    const spy = spyOn(FileSupervisorBootstrapJournal.prototype, "withLease").mockImplementation(async function (
      this: FileSupervisorBootstrapJournal, signal: AbortSignal | undefined, operation: (entry: BootstrapJournalEntry) => Promise<unknown>,
    ) {
      return original.call(this, signal, async entry => {
        const id = (await entry.load())!.binding.projectId as ProjectId;
        simultaneous++; maximum = Math.max(maximum, simultaneous); visits.push(id);
        try {
          expect((await stat(`${value.manifestFile}.bootstrap-migration.json.lock`)).isDirectory()).toBe(true);
          expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(existing);
          const owned = await Promise.all(ids.map(async selected => {
            try { return (await stat(`${journalFile(value, selected)}.lock`)).isDirectory(); } catch { return false; }
          }));
          expect(owned.filter(Boolean)).toHaveLength(1);
          if ((busy.get(id) ?? 0) > 0) {
            busy.set(id, busy.get(id)! - 1);
            throw new FileLeaseBusyError("offline-journal.lock");
          }
          return operation(entry);
        } finally { simultaneous--; }
      });
    } as typeof original);
    const output: string[] = [];
    try {
      await main(recoveryArgs(value.manifestFile, ids), text => output.push(text), { clock, timeoutMs: 5000 });
      expect(maximum).toBe(1);
      expect(visits).toEqual([A, A, B, B, B, C, D, D, E]);
      expect(clock.sleeps).toEqual([250, 250, 250, 250]);
      expect(output).toEqual([JSON.stringify({ file: `${value.manifestFile}.bootstrap-migration.json`, grants: 5, created: !existing }, null, 2)]);
    } finally { spy.mockRestore(); }
  });

  it.each(["timeout", "abort"] as const)("ends operator lease retries on %s without publishing a partial ledger", async mode => {
    const value = await fixture([A, B]), clock = new PreparationClock(), cancellation = new AbortController();
    const original = FileSupervisorBootstrapJournal.prototype.withLease;
    const before = await Promise.all([A, B].map(id => readFile(journalFile(value, id), "utf8")));
    const visits: ProjectId[] = [];
    const spy = spyOn(FileSupervisorBootstrapJournal.prototype, "withLease").mockImplementation(async function (
      this: FileSupervisorBootstrapJournal, signal: AbortSignal | undefined, operation: (entry: BootstrapJournalEntry) => Promise<unknown>,
    ) {
      return original.call(this, signal, async entry => {
        const id = (await entry.load())!.binding.projectId as ProjectId;
        visits.push(id);
        if (id === B) throw new FileLeaseBusyError("offline-journal.lock");
        return operation(entry);
      });
    } as typeof original);
    const sleep = clock.sleep.bind(clock);
    clock.sleep = async (ms, signal) => { await sleep(ms, signal); if (mode === "abort") cancellation.abort(); };
    const output: string[] = [];
    try {
      const result = main(recoveryArgs(value.manifestFile, [A, B]), text => output.push(text),
        { clock, signal: cancellation.signal, timeoutMs: 1000 });
      if (mode === "timeout") await expect(result).rejects.toMatchObject({ code: "preparation_timeout" });
      else await expect(result).rejects.toMatchObject({ name: "AbortError" });
      expect(clock.nowMs).toBe(mode === "timeout" ? 1000 : 250);
      expect(visits).toEqual(mode === "timeout" ? [A, B, B, B, B] : [A, B]);
      expect(output).toEqual([]);
      expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
      await expect(stat(`${value.manifestFile}.bootstrap-migration.json.lock`)).rejects.toMatchObject({ code: "ENOENT" });
      expect(await Promise.all([A, B].map(id => readFile(journalFile(value, id), "utf8")))).toEqual(before);
      expect((await readdir(value.root)).some(name => name.endsWith(".tmp"))).toBe(false);
    } finally { spy.mockRestore(); }
  });

  it("releases the writer lease if cancellation wins immediately after acquisition", async () => {
    const value = await fixture([A]), cancellation = new AbortController(), original = FileLease.prototype.acquire;
    const spy = spyOn(FileLease.prototype, "acquire").mockImplementation(async function (this: FileLease, options) {
      await original.call(this, options);
      cancellation.abort();
    });
    try {
      await expect(prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
        identityFingerprint: FINGERPRINT, projectIds: [A], operatorRecovery: true }, { signal: cancellation.signal }))
        .rejects.toMatchObject({ name: "AbortError" });
      await expect(stat(`${value.manifestFile}.bootstrap-migration.json.lock`)).rejects.toMatchObject({ code: "ENOENT" });
      expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
    } finally { spy.mockRestore(); }
  });

  it.each(["ownership", "ordinary"] as const)("never retries %s errors as lease contention", async kind => {
    const value = await fixture([A]), clock = new PreparationClock();
    const error = kind === "ownership" ? new FileLeaseOwnershipError("offline-journal.lock") : new Error("offline validation failure");
    let calls = 0;
    const original = FileSupervisorBootstrapJournal.prototype.withLease;
    const spy = spyOn(FileSupervisorBootstrapJournal.prototype, "withLease").mockImplementation((async () => { calls++; throw error; }) as typeof original);
    try {
      await expect(prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
        identityFingerprint: FINGERPRINT, projectIds: [A], operatorRecovery: true }, { clock, timeoutMs: 1000 })).rejects.toBe(error);
      expect(calls).toBe(1); expect(clock.sleeps).toEqual([]);
      expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
    } finally { spy.mockRestore(); }
  });

  it("requires a finite operator deadline of at most ten minutes before acquiring state", async () => {
    const value = await fixture([A]);
    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 600_001]) {
      await expect(prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
        identityFingerprint: FINGERPRINT, projectIds: [A], operatorRecovery: true }, { timeoutMs }))
        .rejects.toMatchObject({ code: "preparation_timeout_bound" });
    }
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
    await expect(stat(`${value.manifestFile}.bootstrap-migration.json.lock`)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("withholds a staged grant at runtime if its journal changes after its individual staging lease", async () => {
    const value = await fixture([A, B]), original = FileSupervisorBootstrapJournal.prototype.withLease;
    const spy = spyOn(FileSupervisorBootstrapJournal.prototype, "withLease").mockImplementation(async function (
      this: FileSupervisorBootstrapJournal, signal: AbortSignal | undefined, operation: (entry: BootstrapJournalEntry) => Promise<unknown>,
    ) {
      return original.call(this, signal, async entry => {
        if ((await entry.load())!.binding.projectId === B) {
          // A's staging lease is gone; model its independent recovery writer.
          await expect(stat(`${journalFile(value, A)}.lock`)).rejects.toMatchObject({ code: "ENOENT" });
          await writeFile(journalFile(value, A), JSON.stringify({ ...value.attempts.get(A)!, connectTimeoutMs: 2000 }));
        }
        return operation(entry);
      });
    } as typeof original);
    try {
      await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
        identityFingerprint: FINGERPRINT, projectIds: [A, B], operatorRecovery: true });
    } finally { spy.mockRestore(); }
    await value.migration.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.operatorRecoveryProjects]).toEqual([B]);
    });
  });

  it.each([4, 5])("binds %s v2 operator grants separately from legacy majority authority", async count => {
    const ids = [A, B, C, D, E].slice(0, count), value = await fixture(ids);
    const beforeManifest = await readFile(value.manifestFile, "utf8");
    const beforeJournals = await Promise.all(ids.map(id => readFile(journalFile(value, id), "utf8")));
    const result = await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: ids, operatorRecovery: true });
    const ledger = JSON.parse(await readFile(result.file, "utf8"));
    expect(ledger).toMatchObject({ version: 2, purpose: "operator_bootstrap_recovery", identityFingerprint: FINGERPRINT });
    expect(ledger.grants).toHaveLength(count);
    await value.migration.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.projects].sort()).toEqual(ids);
      expect([...authority.operatorRecoveryProjects].sort()).toEqual(ids);
    });
    expect(await readFile(value.manifestFile, "utf8")).toBe(beforeManifest);
    expect(await Promise.all(ids.map(id => readFile(journalFile(value, id), "utf8")))).toEqual(beforeJournals);
    await expect(prepare(value, ids)).rejects.toThrow();
  });

  it("never upgrades a legacy ledger into operator recovery authority", async () => {
    const value = await fixture([A]), result = await prepare(value, [A]);
    const before = await readFile(result.file, "utf8");
    await value.migration.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.projects]).toEqual([A]);
      expect(authority.operatorRecoveryProjects.size).toBe(0);
    });
    await expect(prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A], operatorRecovery: true })).rejects.toThrow("immutable_ledger");
    expect(await readFile(result.file, "utf8")).toBe(before);
  });

  it("rejects a sixth operator project before writing a ledger", async () => {
    const value = await fixture([A, B, C, D, E, F]);
    await expect(prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A, B, C, D, E, F], operatorRecovery: true }))
      .rejects.toThrow("project_count_or_identity");
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
  });

  it.each(["missing purpose", "wrong purpose", "sixth grant", "legacy version", "duplicate", "foreign identity"] as const)(
    "rejects malformed v2 authority: %s", async kind => {
      const ids = [A, B, C, D, E], value = await fixture(ids);
      const result = await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
        identityFingerprint: FINGERPRINT, projectIds: ids, operatorRecovery: true });
      const ledger = JSON.parse(await readFile(result.file, "utf8")) as Ledger;
      if (kind === "missing purpose") Reflect.deleteProperty(ledger, "purpose");
      if (kind === "wrong purpose") ledger.purpose = "ordinary_replacement";
      if (kind === "sixth grant") ledger.grants.push({ ...ledger.grants[0]!, projectId: F, workerIndex: 6 });
      if (kind === "legacy version") { ledger.version = 1; Reflect.deleteProperty(ledger, "purpose"); }
      if (kind === "duplicate") ledger.grants[1] = { ...ledger.grants[0]! };
      if (kind === "foreign identity") ledger.identityFingerprint = "b".repeat(64);
      await writeFile(result.file, JSON.stringify(ledger));
      await expect(value.migration.withValidatedAuthority(await value.store.load(), async authority => authority)).rejects.toThrow();
    },
  );

  it("withholds v2 minority authority when an exact active journal changes", async () => {
    const value = await fixture([A, B]);
    await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A, B], operatorRecovery: true });
    await saveAttempt(value, A, { ...value.attempts.get(A)!, connectTimeoutMs: 2000 });
    await value.migration.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.projects]).toEqual([B]);
      expect([...authority.operatorRecoveryProjects]).toEqual([B]);
    });
  });

  it("preserves unused operator grants when an exact consumed hold finishes cleanup", async () => {
    const value = await fixture([A, B]);
    await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A, B], operatorRecovery: true });
    await value.store.markDraining(A, "bootstrap_hold", 5);
    const held = (await value.store.load()).workers.find(worker => worker.projectId === A)!;
    await saveAttempt(value, A, { ...value.attempts.get(A)!, phase: "cleanup_proven" });
    await value.migration.recordConsumedHold(held);
    await value.store.completeBootstrapHoldCleanup(held, 6);
    const restored = new FileBootstrapHoldMigration({ manifestFile: value.manifestFile, identityFingerprint: FINGERPRINT });
    await restored.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.operatorRecoveryProjects]).toEqual([B]);
      expect([...authority.consumedProjects]).toEqual([A]);
    });
    await unlink(journalFile(value, A));
    await value.store.removeManaged(A);
    expect([...await restored.validate(await value.store.load())].sort()).toEqual([A, B]);
    await value.store.reserveWorker({ index: 1, projectId: A, title: "same ID cannot refill", nowMs: 7 });
    await saveAttempt(value, A, value.attempts.get(A)!);
    await restored.withValidatedAuthority(await value.store.load(), async authority => {
      expect([...authority.operatorRecoveryProjects]).toEqual([B]);
      expect([...authority.consumedProjects]).toEqual([A]);
    });
  });

  it("never treats arbitrary replacement state as consumed operator authority", async () => {
    const value = await fixture([A, B]);
    await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A, B], operatorRecovery: true });
    await value.store.markDraining(A, "replacement", 5);
    const arbitrary = (await value.store.load()).workers.find(worker => worker.projectId === A)!;
    await expect(value.migration.recordConsumedHold(arbitrary)).rejects.toThrow("consumption_requires_exact_hold");
    await expect(value.migration.withValidatedAuthority(await value.store.load(), async authority => authority)).rejects.toThrow("managed_worker_identity");
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration-consumed.json`).exists()).toBe(false);
  });

  it("bounds and binds monotonic consumption records to the exact immutable ledger", async () => {
    const value = await fixture([A, B]);
    await prepareBootstrapHoldMigrationLedger({ manifestFile: value.manifestFile,
      identityFingerprint: FINGERPRINT, projectIds: [A, B], operatorRecovery: true });
    await value.store.markDraining(A, "bootstrap_hold", 5);
    const held = (await value.store.load()).workers.find(worker => worker.projectId === A)!;
    await value.migration.recordConsumedHold(held);
    const file = `${value.manifestFile}.bootstrap-migration-consumed.json`, original = await readFile(file, "utf8");
    await value.migration.recordConsumedHold(held);
    expect(await readFile(file, "utf8")).toBe(original);
    expect(Buffer.byteLength(original)).toBeLessThan(4096);
    const record = JSON.parse(original);
    for (const changed of [
      { ...record, identityFingerprint: "b".repeat(64) },
      { ...record, ledgerSha256: "b".repeat(64) },
      { ...record, consumedGrantSha256: ["b".repeat(64)] },
      { ...record, consumedGrantSha256: [record.consumedGrantSha256[0], record.consumedGrantSha256[0]] },
    ]) {
      await writeFile(file, JSON.stringify(changed));
      await expect(value.migration.withValidatedAuthority(await value.store.load(), async authority => authority)).rejects.toThrow();
    }
  });

  it("does not authorize anything or create state when the ledger is absent", async () => {
    const value = await fixture([]);
    const before = await readdir(value.root);
    expect([...await value.migration.validate(await value.store.load())]).toEqual([]);
    expect(await readdir(value.root)).toEqual(before);
  });

  it("binds two exact archived submissions without retaining recovery secrets", async () => {
    const value = await fixture();
    const result = await prepare(value);
    expect(result).toEqual({ file: `${value.manifestFile}.bootstrap-migration.json`, grants: 2, created: true });
    const raw = await readFile(result.file, "utf8");
    const ledger = JSON.parse(raw) as Ledger;
    expect(Object.keys(ledger).sort()).toEqual(["grants", "identityFingerprint", "version"]);
    expect(ledger.version).toBe(1);
    expect(ledger.identityFingerprint).toBe(FINGERPRINT);
    expect(ledger.grants).toHaveLength(2);
    for (const [index, id] of [A, B].entries()) {
      const pending = value.attempts.get(id)!;
      expect(ledger.grants.find(grant => grant.projectId === id)).toEqual({
        projectId: id,
        workerIndex: index + 1,
        buildHash: pending.binding.buildHash,
        schemaHash: pending.binding.schemaHash,
        bindingSha256: digest(pending.binding),
        journalSha256: digest(pending),
      });
      for (const secret of [pending.sessionId, pending.binding.nonce, pending.binding.sandboxSessionId,
        pending.sealed.salt, pending.sealed.iv, pending.sealed.ciphertext, TOKEN, ROTATION_KEY, "offline.invalid"]) {
        expect(raw).not.toContain(secret);
      }
    }
    if (process.platform !== "win32") expect((await stat(result.file)).mode & 0o777).toBe(0o600);
    // Recovery may have advanced the manifest generation after the old submission.
    expect((await value.store.load()).workers[0]!.leaderGeneration).toBe(2);
    expect([...await value.migration.validate(await value.store.load())].sort()).toEqual([A, B]);
  });

  it("is byte-preserving and idempotent for the same grants regardless of argument order", async () => {
    const value = await fixture();
    const first = await prepare(value);
    const before = await readFile(first.file, "utf8");
    expect(await prepare(value, [B, A])).toEqual({ ...first, created: false });
    expect(await readFile(first.file, "utf8")).toBe(before);
  });

  it("creates a single immutable ledger under concurrent identical preparation", async () => {
    const value = await fixture();
    const results = await Promise.all([prepare(value), prepare(value)]);
    expect(results.map(result => result.created).sort()).toEqual([false, true]);
    expect([...await value.migration.validate(await value.store.load())].sort()).toEqual([A, B]);
    expect((await readdir(value.root)).filter(file => file.endsWith(".tmp") || file.endsWith(".lock"))).toEqual([]);
  });

  it("never refills or replaces a consumed grant with a different project", async () => {
    const value = await fixture([A, B, C]);
    const result = await prepare(value, [A]);
    const before = await readFile(result.file, "utf8");
    await value.store.markDraining(A, "bootstrap_hold", 5);
    await unlink(journalFile(value, A));
    expect([...await value.migration.validate(await value.store.load())]).toEqual([A]);
    expect(await prepare(value, [A])).toEqual({ ...result, created: false });
    await expect(prepare(value, [B])).rejects.toThrow();
    await expect(prepare(value, [A, C])).rejects.toThrow();
    expect(await readFile(result.file, "utf8")).toBe(before);
  });

  it("preserves a consumed grant when the held project's journal changes", async () => {
    const value = await fixture([A]);
    await prepare(value, [A]);
    await value.store.markDraining(A, "bootstrap_hold", 5);
    await saveAttempt(value, A, await attempt(A, V2_R10_RELEASE));
    expect([...await value.migration.validate(await value.store.load())]).toEqual([A]);
  });

  it("temporarily omits a busy active journal without delaying other grants", async () => {
    const value = await fixture();
    await prepare(value);
    let entered!: () => void;
    let release!: () => void;
    const acquired = new Promise<void>(resolve => { entered = resolve; });
    const untilReleased = new Promise<void>(resolve => { release = resolve; });
    const occupied = new FileSupervisorBootstrapJournal(`${value.manifestFile}.bootstrap`, A)
      .withLease(undefined, async () => { entered(); await untilReleased; });
    await acquired;
    try {
      expect([...await value.migration.validate(await value.store.load())]).toEqual([B]);
    } finally {
      release();
      await occupied;
    }
    expect([...await value.migration.validate(await value.store.load())].sort()).toEqual([A, B]);
  });

  it.each([
    ["no projects", []],
    ["duplicate projects", [A, A]],
    ["more than three projects", [A, B, C, D]],
    ["a foreign project", [C]],
  ] as const)("rejects preparation with %s", async (_label, ids) => {
    const value = await fixture();
    await expect(prepare(value, ids)).rejects.toThrow();
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
  });

  it.each([
    ["current release", V2_R10_RELEASE],
    ["unregistered build", { buildHash: "d".repeat(64), schemaHash: ARCHIVED_R10_RELEASE_IDENTITIES[0]!.schemaHash }],
    ["cross-paired schema", { buildHash: ARCHIVED_R10_RELEASE_IDENTITIES[0]!.buildHash, schemaHash: V2_R4_RELEASE.schemaHash }],
    ["swapped hashes", { buildHash: ARCHIVED_R10_RELEASE_IDENTITIES[0]!.schemaHash, schemaHash: ARCHIVED_R10_RELEASE_IDENTITIES[0]!.buildHash }],
  ] as const)("rejects %s journals without creating a ledger", async (_label, release) => {
    const value = await fixture([A]);
    await saveAttempt(value, A, await attempt(A, release));
    await expect(prepare(value, [A])).rejects.toThrow();
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
  });

  it.each(["prepared", "cleanup_proven"] as const)("rejects a %s journal", async phase => {
    const value = await fixture([A]);
    await saveAttempt(value, A, { ...value.attempts.get(A)!, phase });
    await expect(prepare(value, [A])).rejects.toThrow();
  });

  it("rejects missing and foreign-bound journals", async () => {
    const value = await fixture([A]);
    await unlink(journalFile(value, A));
    await expect(prepare(value, [A])).rejects.toThrow();
    await writeFile(journalFile(value, A), JSON.stringify(await attempt(B)), { mode: 0o600 });
    await expect(prepare(value, [A])).rejects.toThrow();
  });

  it("rejects an exact archived binding with tampered encrypted bootstrap authority", async () => {
    const value = await fixture([A]);
    const pending = value.attempts.get(A)!;
    const bytes = Buffer.from(pending.sealed.ciphertext, "base64url");
    bytes[0] = bytes[0]! ^ 1;
    await saveAttempt(value, A, { ...pending, sealed: { ...pending.sealed, ciphertext: bytes.toString("base64url") } });
    await expect(prepare(value, [A])).rejects.toThrow();
    expect(await Bun.file(`${value.manifestFile}.bootstrap-migration.json`).exists()).toBe(false);
  });

  it.each(["unmanaged", "anonymous", "foreign identity"] as const)("rejects an %s manifest", async kind => {
    const value = await fixture([A]);
    const manifest = await value.store.load();
    await writeFile(value.manifestFile, JSON.stringify({
      ...manifest,
      ...(kind === "anonymous" ? { identityKind: "anonymous" } : {}),
      ...(kind === "foreign identity" ? { identityFingerprint: "b".repeat(64) } : {}),
      workers: manifest.workers.map(worker => ({ ...worker, ...(kind === "unmanaged" ? { managed: false } : {}) })),
    }));
    await expect(prepare(value, [A])).rejects.toThrow();
  });

  it("rejects a changed active journal even when the binding is unchanged", async () => {
    const value = await fixture([A]);
    const result = await prepare(value, [A]);
    const before = await readFile(result.file, "utf8");
    await saveAttempt(value, A, { ...value.attempts.get(A)!, connectTimeoutMs: 1001 });
    await expect(value.migration.validate(await value.store.load())).rejects.toThrow();
    await expect(prepare(value, [A])).rejects.toThrow();
    expect(await readFile(result.file, "utf8")).toBe(before);
  });

  it("runtime validation isolates changed active journals while preserving consumed grants", async () => {
    const value = await fixture();
    const result = await prepare(value);
    const ledgerBefore = await readFile(result.file, "utf8");
    await saveAttempt(value, A, { ...value.attempts.get(A)!, connectTimeoutMs: 1001 });
    const activeManifest = await value.store.load();
    const activeFiles = [value.manifestFile, journalFile(value, A), journalFile(value, B)];
    const activeBefore = await Promise.all(activeFiles.map(file => readFile(file, "utf8")));
    await expect(value.migration.validate(activeManifest)).rejects.toThrow();
    let callbacks = 0;
    expect(await value.migration.withValidated(activeManifest, async grants => {
      callbacks++;
      return [...grants];
    })).toEqual([B]);
    expect(callbacks).toBe(1);
    expect(await Promise.all(activeFiles.map(file => readFile(file, "utf8")))).toEqual(activeBefore);
    expect(await readFile(result.file, "utf8")).toBe(ledgerBefore);

    await value.store.markDraining(A, "bootstrap_hold", 5);
    await saveAttempt(value, A, await attempt(A, V2_R10_RELEASE));
    const heldBefore = await Promise.all(activeFiles.map(file => readFile(file, "utf8")));
    expect(await value.migration.withValidated(await value.store.load(), async grants => [...grants].sort())).toEqual([A, B]);
    expect(await Promise.all(activeFiles.map(file => readFile(file, "utf8")))).toEqual(heldBefore);
    expect(await readFile(result.file, "utf8")).toBe(ledgerBefore);
  });

  it("rejects changed worker authority after ledger creation", async () => {
    const value = await fixture([A]);
    await prepare(value, [A]);
    const manifest = await value.store.load();
    for (const changed of [
      { ...manifest, identityFingerprint: "b".repeat(64) },
      { ...manifest, workers: [] },
      { ...manifest, workers: manifest.workers.map(worker => ({ ...worker, index: 3 })) },
      { ...manifest, workers: manifest.workers.map(worker => ({ ...worker, managed: false })) },
    ]) {
      await expect(value.migration.validate(changed)).rejects.toThrow();
    }
  });

  it.each(["unknown field", "duplicate grant", "foreign third grant", "fourth grant", "foreign identity", "hash newline", "missing digest"] as const)(
    "rejects a ledger containing %s", async kind => {
      const value = await fixture();
      const result = await prepare(value);
      const ledger = JSON.parse(await readFile(result.file, "utf8")) as Ledger;
      if (kind === "unknown field") Object.assign(ledger, { secret: TOKEN });
      if (kind === "duplicate grant") ledger.grants[1] = { ...ledger.grants[0]! };
      if (kind === "foreign third grant" || kind === "fourth grant") ledger.grants.push({ ...ledger.grants[0]!, projectId: C, workerIndex: 3 });
      if (kind === "fourth grant") ledger.grants.push({ ...ledger.grants[0]!, projectId: D, workerIndex: 4 });
      if (kind === "foreign identity") ledger.identityFingerprint = "b".repeat(64);
      if (kind === "hash newline") ledger.grants[0]!.bindingSha256 += "\n";
      if (kind === "missing digest") Reflect.deleteProperty(ledger.grants[0]!, "journalSha256");
      await writeFile(result.file, JSON.stringify(ledger));
      await expect(value.migration.validate(await value.store.load())).rejects.toThrow();
      await expect(prepare(value)).rejects.toThrow();
    },
  );

  it("bounds ledger reads and preserves invalid existing bytes", async () => {
    const value = await fixture([A]);
    const file = `${value.manifestFile}.bootstrap-migration.json`;
    const raw = " ".repeat(64 * 1024 + 1);
    await writeFile(file, raw, { mode: 0o600 });
    await expect(value.migration.validate(await value.store.load())).rejects.toThrow();
    await expect(prepare(value, [A])).rejects.toThrow();
    expect(await readFile(file, "utf8")).toBe(raw);
  });
});
