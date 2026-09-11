import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { FileSupervisorBootstrapJournal, type BootstrapJournalEntry, type SupervisorBootstrapAttempt, type SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { directSupervisorLaunchFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";

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
  readonly phases: string[] = [];
  async withLease<T>(_signal: AbortSignal | undefined, run: (entry: BootstrapJournalEntry) => Promise<T>) { return run(this); }
  async load() { return this.attempt; }
  async save(attempt: SupervisorBootstrapAttempt) { this.attempt = structuredClone(attempt); this.phases.push(attempt.phase); }
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

async function prior(journal: Journal, files: Files, phase: SupervisorBootstrapAttempt["phase"]) {
  const sealed = await sealSupervisorSessionBootstrap({ sessionId: crypto.randomUUID(), binding,
    payload: { instanceId: "old-instance", credential: { url: credential.url, baseUrl: credential.baseUrl, docId: project,
      token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() } } });
  await journal.save({ version: 1, phase, sessionId: crypto.randomUUID(), binding, connectTimeoutMs: 1000, sealed });
  // Seal against the same private execution identity stored in the journal.
  const attempt = journal.attempt!;
  await journal.save({ ...attempt, sealed: await sealSupervisorSessionBootstrap({ sessionId: attempt.sessionId, binding,
    payload: { instanceId: "old-instance", credential: { url: credential.url, baseUrl: credential.baseUrl, docId: project,
      token: "offline-token", authorization: "full", rotationKey: createCredentialRotationKey() } } }) });
  const generated = directSupervisorLaunchFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: binding.nonce });
  files.data.set("exec", generated.launcher); files.data.set(install, generated.installer);
  files.data.set(staticPath, supervisorStaticConfig({ binding, entrypointPath: assets.entrypointPath, connectTimeoutMs: 1000 }));
  files.data.set(sealedPath, JSON.stringify(journal.attempt!.sealed));
}

function controller(journal: SupervisorBootstrapJournal, files: Files, events: string[], options: { rejectOld?: boolean; failSubmission?: boolean; failSync?: boolean } = {}) {
  return new V2SandboxSupervisor({ binding: { ...binding, leaderGeneration: 2, nonce: "new-bootstrap" }, assets,
    ySweet: credential, ydoc: files, bootstrapJournal: journal, connectTimeoutMs: 1000,
    gateway: { async syncWorkspace() { if (options.failSync) throw new Error("offline sync"); },
      async startSupervisorExec({ sessionId }) { events.push("submit"); if (options.failSubmission) throw new Error("offline lost acknowledgement");
        return { sessionId, workspaceRoot: `/codex_workspace/${sessionId}`, jobId: "offline-job" }; },
      async triggerRender() { throw new Error("unexpected render"); }, async fetchText() { throw new Error("unexpected fetch"); } },
    hostFactory: input => ({ binding: input.binding, instanceId: input.instanceId,
      async connect() { events.push(`connect:${input.binding.leaderGeneration}`); if (options.rejectOld && input.binding.leaderGeneration === 1) throw new Error("offline unknown supervisor"); },
      async ensureReady() {}, async launch() { throw new Error("unexpected launch"); }, async deliverCredential() {}, async stop() { return null; },
      async rotateCredential() {}, async shutdown() { events.push(`shutdown:${input.binding.leaderGeneration}`); }, async close() {} }),
  });
}

describe("interrupted supervisor bootstrap recovery", () => {
  it.each(["prepared", "cleanup_proven"] as const)("recovers a %s receipt with partial owned files", async phase => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, phase); files.data.delete("exec");
    const sup = controller(journal, files, events);
    try { await sup.ensureReady(); expect(events).toEqual(["submit", "connect:2"]); expect(journal.attempt).toBeNull(); expect(files.data.has(install)).toBe(false); }
    finally { await sup.close(); }
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
