import { afterEach, describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
  defaultFleetManifestFile,
  FleetManifestError,
  FleetManifestStore,
} from "../src/fleet/manifest";
import { projectId } from "../src/sandbox/types";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");
const FINGERPRINT = "a".repeat(64);
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function manifestPath(): Promise<string> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-fleet-"));
  roots.push(root);
  const directory = path.join(root, "state");
  await mkdir(directory, { recursive: true });
  return path.join(directory, "fleet.json");
}

describe("FleetManifestStore", () => {
  it("places default fleet state under an identity-scoped home directory", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-home-"));
    roots.push(root);
    expect(defaultFleetManifestFile(FINGERPRINT, root)).toBe(path.join(
      root,
      ".ghostprov",
      "fleets",
      FINGERPRINT,
      "fleet.json",
    ));
    expect(() => defaultFleetManifestFile("not-a-fingerprint", root)).toThrow("SHA-256");
  });

  it("upgrades a version-one manifest with an idempotent legacy migration marker", async () => {
    const file = await manifestPath();
    await writeFile(file, JSON.stringify({
      version: 1,
      identityKind: "authenticated",
      identityFingerprint: FINGERPRINT,
      workers: [],
    }));
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });
    expect((await store.load()).legacyMigrationCompleted).toBe(false);
    await store.completeLegacyMigration();
    await store.completeLegacyMigration();
    expect((await store.load()).legacyMigrationCompleted).toBe(true);
  });

  it("copies a matching legacy registry into the identity-scoped path without deleting it", async () => {
    const legacyFile = await manifestPath();
    const targetFile = await manifestPath();
    const legacy = new FleetManifestStore({
      file: legacyFile,
      identityFingerprint: FINGERPRINT,
    });
    await legacy.initialize("authenticated");
    await legacy.reserveWorker({
      index: 1,
      projectId: A,
      title: "Legacy worker",
      nowMs: 1,
      managed: false,
    });
    const target = new FleetManifestStore({
      file: targetFile,
      legacyFile,
      identityFingerprint: FINGERPRINT,
    });

    expect((await target.initialize("authenticated")).workers[0]?.projectId).toBe(A);
    expect((await target.load()).workers[0]?.managed).toBe(false);
    expect((await legacy.load()).workers[0]?.projectId).toBe(A);
  });

  it("persists creation intent before completion and increments leader generations", async () => {
    const file = await manifestPath();
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });

    await store.initialize("authenticated");
    await store.reserveWorker({
      index: 1,
      projectId: A,
      title: "GhostProv Worker 1",
      nowMs: 1_000,
    });
    await store.markCreated(A, 1_100);
    const generation = await store.nextLeaderGeneration(A, 1_200);

    expect(generation).toBe(1);
    expect((await store.load()).workers[0]).toMatchObject({
      index: 1,
      projectId: A,
      state: "created",
      leaderGeneration: 1,
      createdAtMs: 1_000,
      updatedAtMs: 1_200,
    });
    if (process.platform !== "win32") expect((await stat(file)).mode & 0o777).toBe(0o600);
  });

  it("serializes concurrent updates without dropping workers", async () => {
    const file = await manifestPath();
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });
    await store.initialize("anonymous");

    await Promise.all([
      store.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1_000 }),
      store.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 1_001 }),
    ]);

    expect((await store.load()).workers.map(worker => worker.projectId)).toEqual([A, B]);
  });

  it("rejects a manifest owned by another Prism identity", async () => {
    const file = await manifestPath();
    await writeFile(file, JSON.stringify({
      version: 1,
      identityKind: "authenticated",
      identityFingerprint: "b".repeat(64),
      workers: [],
    }));
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });

    await expect(store.load()).rejects.toMatchObject({
      name: "FleetManifestError",
      kind: "identity_mismatch",
    });
  });

  it("rejects duplicate worker indexes and malformed state", async () => {
    const file = await manifestPath();
    const worker = {
      index: 1,
      projectId: A,
      title: "GhostProv Worker 1",
      state: "created",
      createdAtMs: 1,
      updatedAtMs: 1,
      leaderGeneration: 0,
      managed: true,
    };
    await writeFile(file, JSON.stringify({
      version: 1,
      identityKind: "authenticated",
      identityFingerprint: FINGERPRINT,
      workers: [worker, { ...worker, projectId: B }],
    }));
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });

    await expect(store.load()).rejects.toBeInstanceOf(FleetManifestError);
    await expect(store.load()).rejects.toMatchObject({ kind: "protocol" });
  });

  it("updates states and removes only an exact managed worker", async () => {
    const file = await manifestPath();
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });
    await store.initialize("authenticated");
    await store.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1_000 });
    await store.markState(A, "ready", 2_000);
    expect((await store.load()).workers[0]).toMatchObject({ state: "ready", updatedAtMs: 2_000 });

    await store.removeManaged(A);
    expect((await store.load()).workers).toEqual([]);
    await expect(store.removeManaged(A)).rejects.toMatchObject({ kind: "missing_worker" });
  });

  it("tracks explicit projects without making them decommissionable", async () => {
    const file = await manifestPath();
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });
    await store.initialize("authenticated");
    await store.reserveWorker({
      index: 1,
      projectId: A,
      title: "User explicit project",
      nowMs: 1_000,
      managed: false,
    });

    expect((await store.load()).workers[0].managed).toBe(false);
    await expect(store.removeManaged(A)).rejects.toMatchObject({ kind: "conflict" });
    await expect(store.removeImported(A)).rejects.toMatchObject({ kind: "conflict" });
    await store.markState(A, "draining", 1_001);
    await store.removeImported(A);
    expect((await store.load()).workers).toEqual([]);
  });

  it("never persists credential-shaped fields", async () => {
    const file = await manifestPath();
    const store = new FleetManifestStore({ file, identityFingerprint: FINGERPRINT });
    await store.initialize("anonymous");
    await store.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1_000 });
    const raw = await readFile(file, "utf8");

    expect(raw).not.toMatch(/cookie|access_token|api[_-]?key|y[_-]?sweet[_-]?token/i);
    expect(raw).not.toContain("prism_session_token");
  });
});
