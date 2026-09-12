import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { FileSupervisorBootstrapJournal, type BootstrapJournalEntry, type SupervisorBootstrapAttempt, type SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { supervisorCleanupFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";
import { loadV2SidecarAssets, loadV2SidecarRecoveryAssets, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY, V2_R10_C1091742_RETIREMENT_IDENTITY, type V2SidecarAssets } from "../src/v2/sidecar/assets";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import { SupervisorLifecycle } from "../src/v2/supervisor/lifecycle";

const assets = await loadV2SidecarAssets({ revision: "r10" });
const project = "11111111-1111-4111-8111-111111111111";
const binding = { projectId: project, sandboxSessionId: "offline-sandbox", leaderGeneration: 1,
  buildHash: assets.buildHash, schemaHash: assets.schemaHash, nonce: "old-bootstrap", protocolRevision: "r2" as const, schemaVersion: 2 as const };
const credential = parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: project, token: "offline-token", authorization: "full" });
const install = ".ghostprov-v2/direct-exec/install.sh", staticPath = ".ghostprov-v2/supervisor/static.json", sealedPath = ".ghostprov-v2/supervisor/bootstrap.enc.json";
const roots: string[] = [];
afterEach(async () => { for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true }); });

class Journal implements SupervisorBootstrapJournal, BootstrapJournalEntry {
  attempt: SupervisorBootstrapAttempt | null = null;
  failCleanupSave = false;
  readonly phases: string[] = [];
  async withLease<T>(signal: AbortSignal | undefined, run: (entry: BootstrapJournalEntry) => Promise<T>) { signal?.throwIfAborted(); return run(this); }
  async load() { return this.attempt; }
  async save(attempt: SupervisorBootstrapAttempt) {
    if (this.failCleanupSave && attempt.phase === "cleanup_proven") throw new Error("offline cleanup receipt persistence failed");
    this.attempt = structuredClone(attempt); this.phases.push(attempt.phase);
  }
  async clear() { this.attempt = null; this.phases.push("cleared"); }
}

class Files {
  readonly data = new Map<string, string>();
  refuseCleanup = false;
  async readPath(p: string) { return this.data.get(p) ?? null; }
  async writePath(p: string, s: string) { this.data.set(p, s); }
  async tombstonePath(p: string) { return this.data.delete(p); }
  async compareAndMutatePaths(input: GuardedYDocMutation) {
    if (input.guards.some(g => (this.data.get(g.path) ?? null) !== g.equals)) return false;
    if (this.refuseCleanup && input.mutations.some(m => m.kind === "tombstone")) throw new Error("offline cleanup disconnected");
    for (const m of input.mutations) m.kind === "write" ? this.data.set(m.path, m.content) : this.data.delete(m.path);
    return true;
  }
  async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; }
}

async function prior(journal: Journal, files: Files, phase: SupervisorBootstrapAttempt["phase"], priorAssets: V2SidecarAssets = assets) {
  const priorBinding = { ...binding, buildHash: priorAssets.buildHash, schemaHash: priorAssets.schemaHash };
  const sealed = await sealSupervisorSessionBootstrap({ sessionId: crypto.randomUUID(), binding: priorBinding,
    payload: { instanceId: "old-instance", credential: { url: credential.url, baseUrl: credential.baseUrl, docId: project,
      token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() } } });
  await journal.save({ version: 1, phase, sessionId: crypto.randomUUID(), binding: priorBinding, connectTimeoutMs: 1000, sealed });
  // Seal against the same private execution identity stored in the journal.
  const attempt = journal.attempt!;
  await journal.save({ ...attempt, sealed: await sealSupervisorSessionBootstrap({ sessionId: attempt.sessionId, binding: priorBinding,
    payload: { instanceId: "old-instance", credential: { url: credential.url, baseUrl: credential.baseUrl, docId: project,
      token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() } } }) });
  const generated = supervisorCleanupFiles({ entrypointPath: priorAssets.entrypointPath, supervisorNonce: priorBinding.nonce });
  files.data.set("exec", generated.launcher); files.data.set(install, generated.installer);
  files.data.set(staticPath, supervisorStaticConfig({ binding: priorBinding, entrypointPath: priorAssets.entrypointPath, connectTimeoutMs: 1000 }));
  files.data.set(sealedPath, JSON.stringify(journal.attempt!.sealed));
}

function controller(journal: SupervisorBootstrapJournal, files: Files, events: string[], options: { rejectOld?: boolean; rejectShutdown?: boolean; failSubmission?: boolean; failSync?: boolean; loseStage?: boolean; generation?: number; sync?: (count: number) => void; onShutdown?: () => void; lifecycle?: SupervisorLifecycle } = {}) {
  let syncs = 0;
  let release = () => {};
  const supervisor = new V2SandboxSupervisor({ binding: { ...binding, leaderGeneration: 2, nonce: "new-bootstrap" }, assets,
    ...(options.generation ? { binding: { ...binding, leaderGeneration: options.generation, nonce: `bootstrap-${options.generation}` } } : {}),
    ySweet: credential, ydoc: files, bootstrapJournal: journal, connectTimeoutMs: 1000,
    onCleanupRequired: value => options.lifecycle?.requireCleanup(value),
    onCleanupProven: value => options.lifecycle?.confirmCleanup(value),
    onClosed: () => release(),
    gateway: { async syncWorkspace() { options.sync?.(++syncs); if (options.failSync) throw new Error("offline sync"); if (options.loseStage) files.data.clear(); },
      async startSupervisorExec({ sessionId }) { events.push("submit"); if (options.failSubmission) throw new Error("offline lost acknowledgement");
        return { sessionId, workspaceRoot: `/codex_workspace/${sessionId}`, jobId: "offline-job" }; },
      async triggerRender() { throw new Error("unexpected render"); }, async fetchText() { throw new Error("unexpected fetch"); } },
    hostFactory: input => ({ binding: input.binding, instanceId: input.instanceId,
      async connect() { events.push(`connect:${input.binding.leaderGeneration}`); if (options.rejectOld && input.binding.leaderGeneration === 1) throw new Error("offline unknown supervisor"); },
      async ensureReady() {}, async launch() { throw new Error("unexpected launch"); }, async deliverCredential() {}, async stop() { return null; },
      async rotateCredential() {}, async shutdown() { if (input.recoverExisting) await this.connect(); events.push(`shutdown:${input.binding.leaderGeneration}`); if (options.rejectShutdown) throw new Error("offline ambiguous shutdown"); options.onShutdown?.(); }, async close() {} }),
  });
  if (options.lifecycle) release = options.lifecycle.register(supervisor);
  return supervisor;
}

describe("interrupted supervisor bootstrap recovery", () => {
  it.each(["shutdown", "recovery"] as const)("retains cleanup debt when %s cannot persist signed proof", async phase => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const lifecycle = new SupervisorLifecycle(1);
    if (phase === "recovery") await prior(journal, files, "submitted");
    const sup = controller(journal, files, events, { lifecycle });
    if (phase === "shutdown") await sup.ensureReady();
    journal.failCleanupSave = true;
    if (phase === "recovery") await expect(sup.ensureReady()).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    else await expect(sup.shutdown("offline_done")).rejects.toThrow("persistence failed");
    await sup.close();
    expect(journal.attempt?.phase).toBe("submitted");
    await expect(lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
  });

  it("keeps ambiguous launch cleanup owed after close and clears it after exact later recovery", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const lifecycle = new SupervisorLifecycle(1);
    const first = controller(journal, files, events, { failSubmission: true, lifecycle });
    await expect(first.ensureReady()).rejects.toThrow();
    await expect(first.shutdown("offline_done")).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    await first.close();
    expect(journal.attempt?.phase).toBe("submitted");
    const next = controller(journal, files, events, { generation: 3, lifecycle });
    await next.ensureReady();
    await next.shutdown("offline_done");
    await next.close();
    expect(journal.attempt).toBeNull();
    await expect(lifecycle.shutdown(new AbortController().signal)).resolves.toBeUndefined();
    expect(events.filter(event => event === "submit")).toHaveLength(2);
  });

  it("does not mistake closed recovery transports for old supervisor cleanup", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const lifecycle = new SupervisorLifecycle(1);
    const sup = controller(journal, files, events, { rejectOld: true, lifecycle });
    await expect(sup.ensureReady()).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    await expect(sup.shutdown("offline_done")).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    await sup.close();
    await expect(lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
    expect(journal.attempt?.phase).toBe("submitted");
    expect(events).toEqual(["connect:1"]);
  });

  it("discharges retained debt from a durable cleanup receipt before launching a successor", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "cleanup_proven");
    const lifecycle = new SupervisorLifecycle(1);
    lifecycle.requireCleanup(journal.attempt!.binding);
    const sup = controller(journal, files, events, { lifecycle });
    await sup.ensureReady();
    await sup.shutdown("offline_done");
    await sup.close();
    await expect(lifecycle.shutdown(new AbortController().signal)).resolves.toBeUndefined();
    expect(events).toEqual(["submit", "connect:2", "shutdown:2"]);
  });
  it.each([
    ["initial", V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY],
    ["c1091742", V2_R10_C1091742_RETIREMENT_IDENTITY],
  ] as const)("cleans the exact archived R10 %s files before submitting the selected current release", async (_name, identity) => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const previousAssets = await loadV2SidecarRecoveryAssets(identity);
    await prior(journal, files, "submitted", previousAssets);
    expect(files.data.get(install)).toContain(previousAssets.entrypointPath);
    const sup = controller(journal, files, events);
    try {
      await sup.ensureReady();
      expect(events).toEqual(["connect:1", "shutdown:1", "submit", "connect:2"]);
      expect(journal.attempt?.binding.buildHash).toBe(assets.buildHash);
      expect(files.data.get(staticPath)).toContain(assets.entrypointPath);
      expect(files.data.has(install)).toBe(false);
    } finally { await sup.shutdown("offline_done"); await sup.close(); }
  });

  it("rejects an unregistered or cross-paired prior release without connecting or changing files", async () => {
    for (const hashes of [
      { buildHash: "a".repeat(64), schemaHash: assets.schemaHash },
      { buildHash: assets.schemaHash, schemaHash: assets.buildHash },
    ]) {
      const journal = new Journal(), files = new Files(), events: string[] = [];
      await prior(journal, files, "submitted");
      const before = [...files.data];
      journal.attempt = { ...journal.attempt!, binding: { ...journal.attempt!.binding, ...hashes } };
      const sup = controller(journal, files, events);
      try {
        await expect(sup.ensureReady()).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict", reason: "release_mismatch" });
        expect(events).toEqual([]);
        expect([...files.data]).toEqual(before);
        expect(journal.attempt?.phase).toBe("submitted");
      } finally { await sup.close(); }
    }
  });

  it("does not submit when synchronization replaces the staged document with older state", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const first = controller(journal, files, events, { loseStage: true });
    try { await expect(first.ensureReady()).rejects.toThrow(); expect(events).toEqual([]); expect(journal.attempt).toBeNull(); }
    finally { await first.close(); }
    const next = controller(journal, files, events);
    try { await next.ensureReady(); expect(events).toEqual(["submit", "connect:2"]); }
    finally { await next.shutdown("offline_done"); await next.close(); }
  });

  it("retains the exact recovery receipt until signed supervisor shutdown", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events);
    try {
      await sup.ensureReady();
      expect(journal.attempt?.phase).toBe("submitted");
      expect(files.data.has(install)).toBe(false);
      await sup.shutdown("offline_done");
      expect(events).toEqual(["submit", "connect:2", "shutdown:2"]);
      expect(journal.attempt).toBeNull();
    } finally { await sup.close(); }
  });

  it("recovers a crash after successful startup even when all transient files are gone", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const first = controller(journal, files, events);
    await first.ensureReady(); await first.close();
    expect(journal.attempt?.phase).toBe("submitted");
    const next = controller(journal, files, events, { generation: 3 });
    try {
      await next.ensureReady();
      expect(events).toEqual(["submit", "connect:2", "connect:2", "shutdown:2", "submit", "connect:3"]);
      expect(journal.attempt?.binding.leaderGeneration).toBe(3);
    } finally { await next.shutdown("offline_done"); await next.close(); }
  });

  it("keeps submitted authority when later supervisor shutdown is ambiguous", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events, { rejectShutdown: true });
    try {
      await sup.ensureReady(); await expect(sup.shutdown("offline_done")).rejects.toThrow();
      expect(journal.attempt?.phase).toBe("submitted");
    } finally { await sup.close(); }
  });

  it("retains signed cleanup proof when cleanup synchronization fails", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events, { sync: count => { if (count === 3) throw new Error("offline cleanup sync"); } });
    try {
      await sup.ensureReady(); await expect(sup.shutdown("offline_done")).rejects.toThrow();
      expect(journal.attempt?.phase).toBe("cleanup_proven");
    } finally { await sup.close(); }
    const next = controller(journal, files, events, { generation: 3 });
    try {
      await next.ensureReady();
      expect(events.filter(event => event === "shutdown:2")).toHaveLength(1);
    } finally { await next.shutdown("offline_done"); await next.close(); }
  });

  it("persists signed cleanup before honoring cancellation after supervisor shutdown", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const cancellation = new AbortController();
    const sup = controller(journal, files, events, {
      onShutdown: () => cancellation.abort(new DOMException("offline shutdown deadline", "AbortError")),
    });
    try {
      await sup.ensureReady();
      await expect(sup.shutdown("offline_done", cancellation.signal)).rejects.toMatchObject({ name: "AbortError" });
      expect(journal.attempt?.phase).toBe("cleanup_proven");
    } finally { await sup.close(); }
    const next = controller(journal, files, events, { generation: 3 });
    try {
      await next.ensureReady();
      expect(events).toEqual(["submit", "connect:2", "shutdown:2", "submit", "connect:3"]);
    } finally { await next.shutdown("offline_done"); await next.close(); }
  });

  it("does not clear a changed journal after shutting down its own supervisor", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events);
    try {
      await sup.ensureReady();
      const changed = { ...journal.attempt!, connectTimeoutMs: 999 };
      await journal.save(changed);
      await expect(sup.shutdown("offline_done")).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" });
      expect(journal.attempt?.connectTimeoutMs).toBe(999);
    } finally { await sup.close(); }
  });
  it.each(["prepared", "cleanup_proven"] as const)("recovers a %s receipt with partial owned files", async phase => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, phase); files.data.delete("exec");
    const sup = controller(journal, files, events);
    try { await sup.ensureReady(); expect(events).toEqual(["submit", "connect:2"]); expect(journal.attempt?.binding.leaderGeneration).toBe(2); expect(files.data.has(install)).toBe(false); }
    finally { await sup.shutdown("offline_done"); await sup.close(); }
  });

  it("requires signed cleanup of a submitted attempt before another submission", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const sup = controller(journal, files, events);
    try { await sup.ensureReady(); expect(events).toEqual(["connect:1", "shutdown:1", "submit", "connect:2"]); expect(journal.phases).toContain("cleanup_proven"); }
    finally { await sup.close(); }
  });

  it("retains unresolved submitted state and never replays", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    for (let i = 0; i < 3; i++) { const sup = controller(journal, files, events, { rejectOld: true });
      try { await expect(sup.ensureReady()).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" }); }
      finally { await sup.close(); } }
    expect(events).toEqual(["connect:1", "connect:1", "connect:1"]); expect(journal.attempt?.phase).toBe("submitted");
    expect(files.data.has(install)).toBe(true);
  });

  it("preserves foreign edits and legacy files without a recovery receipt", async () => {
    for (const hasReceipt of [true, false]) {
      const journal = new Journal(), files = new Files(), events: string[] = [];
      if (hasReceipt) await prior(journal, files, "prepared");
      files.data.set(install, "foreign edit"); const sup = controller(journal, files, events);
      try { await expect(sup.ensureReady()).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" }); expect(events).toEqual([]); expect(files.data.get(install)).toBe("foreign edit"); }
      finally { await sup.close(); }
    }
  });

  it("persists submitted state before an ambiguous call and retains all recovery material", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events, { failSubmission: true });
    try { await expect(sup.ensureReady()).rejects.toThrow(); expect(journal.phases).toEqual(["prepared", "submitted"]);
      expect(journal.attempt?.phase).toBe("submitted"); expect(files.data.has(sealedPath)).toBe(true); expect(events).toEqual(["submit"]); }
    finally { await sup.close(); }
  });

  it("retains a prepared receipt when pre-submission cleanup cannot finish", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = []; files.refuseCleanup = true;
    const sup = controller(journal, files, events, { failSync: true });
    try { await expect(sup.ensureReady()).rejects.toThrow(); expect(journal.attempt?.phase).toBe("prepared"); expect(events).toEqual([]); }
    finally { await sup.close(); }
  });

  it("round-trips a bounded private journal and rejects oversized or wrong-project records", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "gp-bootstrap-journal-")); roots.push(root);
    const memory = new Journal(); await prior(memory, new Files(), "submitted");
    const disk = new FileSupervisorBootstrapJournal(root, project);
    await disk.withLease(undefined, async entry => { await entry.save(memory.attempt!); expect(await entry.load()).toEqual(memory.attempt); });
    expect((await readFile(path.join(root, `${project}.json`), "utf8"))).not.toContain("offline-token");
    await writeFile(path.join(root, `${project}.json`), " ".repeat(16 * 1024 + 1));
    await expect(disk.withLease(undefined, entry => entry.load())).rejects.toThrow("bound");
    const other = { ...memory.attempt!, binding: { ...binding, projectId: "22222222-2222-4222-8222-222222222222" } };
    await expect(disk.withLease(undefined, entry => entry.save(other))).rejects.toThrow("binding");
  });
});
