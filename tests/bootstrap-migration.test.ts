import { afterEach, describe, expect, it } from "bun:test";
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
import { FileSupervisorBootstrapJournal, type SupervisorBootstrapAttempt } from "../src/v2/supervisor/bootstrap-journal";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const A = projectId("11111111-1111-4111-8111-111111111111");
const B = projectId("22222222-2222-4222-8222-222222222222");
const C = projectId("33333333-3333-4333-8333-333333333333");
const FINGERPRINT = "a".repeat(64);
const TOKEN = "offline-bootstrap-migration-token";
const ROTATION_KEY = Buffer.alloc(32, 6).toString("base64url");
const CONFIRM = "--confirm-legacy-bootstrap-migration";
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

function digest(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

describe("bootstrap migration CLI guards", () => {
  it("accepts only explicit immutable local inputs", () => {
    const parsed = parseBootstrapMigrationArguments(args("offline-fleet.json", [A, B]));
    expect(parsed).toEqual({ manifestFile: path.resolve("offline-fleet.json"), identityFingerprint: FINGERPRINT, projectIds: [A, B] });
    expect(Object.isFrozen(parsed)).toBe(true);
    expect(Object.isFrozen(parsed.projectIds)).toBe(true);
  });

  it.each([
    ["missing manifest", ["--identity-fingerprint", FINGERPRINT, "--project", A, CONFIRM]],
    ["missing fingerprint", ["--manifest", "offline.json", "--project", A, CONFIRM]],
    ["missing project", ["--manifest", "offline.json", "--identity-fingerprint", FINGERPRINT, CONFIRM]],
    ["missing confirmation", args().slice(0, -1)],
    ["duplicate project", args("offline.json", [A, A])],
    ["more than two projects", args("offline.json", [A, B, C])],
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
  it("does not authorize anything or create state when the ledger is absent", async () => {
    const value = await fixture([]);
    const before = await readdir(value.root);
    expect([...await value.migration.validate(await value.store.load())]).toEqual([]);
    expect(await readdir(value.root)).toEqual(before);
  });

  it("binds at most two exact archived submissions without retaining recovery secrets", async () => {
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
    ["more than two projects", [A, B, C]],
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

  it.each(["unknown field", "duplicate grant", "third grant", "foreign identity", "hash newline", "missing digest"] as const)(
    "rejects a ledger containing %s", async kind => {
      const value = await fixture();
      const result = await prepare(value);
      const ledger = JSON.parse(await readFile(result.file, "utf8")) as Ledger;
      if (kind === "unknown field") Object.assign(ledger, { secret: TOKEN });
      if (kind === "duplicate grant") ledger.grants[1] = { ...ledger.grants[0]! };
      if (kind === "third grant") ledger.grants.push({ ...ledger.grants[0]!, projectId: C, workerIndex: 3 });
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
