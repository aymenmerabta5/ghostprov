import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { FileSupervisorBootstrapJournal, type BootstrapJournalEntry, type SupervisorBootstrapAttempt, type SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { supervisorCleanupFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";
import { loadV2SidecarAssets, loadV2SidecarRecoveryAssets, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY, V2_R10_C1091742_RETIREMENT_IDENTITY, type V2SidecarAssets } from "../src/v2/sidecar/assets";
import { V2_R10_9D2AA8B7_RETIREMENT_IDENTITY } from "../src/v2/sidecar/retirement-identities";
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
  async load() { return this.attempt ? structuredClone(this.attempt) : null; }
  async save(attempt: SupervisorBootstrapAttempt) {
    if (this.failCleanupSave && attempt.phase === "cleanup_proven") throw new Error("offline cleanup receipt persistence failed");
    this.attempt = structuredClone(attempt); this.phases.push(attempt.phase);
  }
  async clear() { this.attempt = null; this.phases.push("cleared"); }
}

class Files {
  // Outer bootstrap deploys the registered entrypoint before the supervisor.
  readonly data = new Map(assets.files.map(file => [file.path, file.content]));
  readonly published = new Map(this.data);
  refuseCleanup = false;
  generation = 1;
  async verifyRemotePaths(expected: readonly { readonly path: string; readonly content: string }[], signal?: AbortSignal) {
    const generation = this.generation;
    signal?.throwIfAborted();
    if (expected.some(file => this.published.get(file.path) !== file.content)) throw new Error("offline remote staging mismatch");
    return Object.freeze({ assertCurrent: () => {
      if (this.generation !== generation) throw new Error("offline document generation changed");
    } });
  }
  async readPath(p: string) { return this.data.get(p) ?? null; }
  async writePath(p: string, s: string) { this.data.set(p, s); }
  async tombstonePath(p: string) { return this.data.delete(p); }
  async compareAndMutatePaths(input: GuardedYDocMutation) {
    if (input.guards.some(g => (this.data.get(g.path) ?? null) !== g.equals)) return false;
    if (this.refuseCleanup && input.mutations.some(m => m.kind === "tombstone")) throw new Error("offline cleanup disconnected");
    for (const m of input.mutations) {
      for (const target of [this.data, this.published]) m.kind === "write" ? target.set(m.path, m.content) : target.delete(m.path);
    }
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

function controller(journal: SupervisorBootstrapJournal | undefined, files: Files, events: string[], options: { rejectOld?: boolean; rejectShutdown?: boolean; failSubmission?: boolean; failSync?: boolean; loseStage?: boolean; generation?: number; sync?: (count: number) => void; onShutdown?: () => void; beforeRecoveryShutdown?: (signal?: AbortSignal) => Promise<void>; lifecycle?: SupervisorLifecycle; afterSubmission?: () => void; onConnect?: () => void; invalidAcceptance?: boolean; diagnosticReads?: string[] } = {}) {
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
        options.afterSubmission?.();
        return { sessionId, workspaceRoot: `/codex_workspace/${options.invalidAcceptance ? "another-session" : sessionId}`, jobId: "offline-job" }; },
      async readDiagnosticFile(sessionId) { options.diagnosticReads?.push(sessionId); return { status: 404, value: null }; },
      async triggerRender() { throw new Error("unexpected render"); }, async fetchText() { throw new Error("unexpected fetch"); } },
    hostFactory: input => ({ binding: input.binding, instanceId: input.instanceId,
      async connect() { options.onConnect?.(); events.push(`connect:${input.binding.leaderGeneration}`); if (options.rejectOld && input.binding.leaderGeneration === 1) throw new Error("offline unknown supervisor"); },
      async ensureReady() {}, async launch() { throw new Error("unexpected launch"); }, async deliverCredential() {}, async stop() { return null; },
      async rotateCredential() {}, async shutdown(_reason, signal) { if (input.recoverExisting) { await options.beforeRecoveryShutdown?.(signal); await this.connect(); } events.push(`shutdown:${input.binding.leaderGeneration}`); if (options.rejectShutdown) throw new Error("offline ambiguous shutdown"); options.onShutdown?.(); }, async close() {} }),
  });
  if (options.lifecycle) release = options.lifecycle.register(supervisor);
  return supervisor;
}

describe("interrupted supervisor bootstrap recovery", () => {
  it("keeps the journal prepared and never submits while the exact publication proof is pending", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    let entered!: () => void, release!: () => void, syncs = 0;
    const pending = new Promise<void>(resolve => { release = resolve; });
    const proofEntered = new Promise<void>(resolve => { entered = resolve; });
    const verify = files.verifyRemotePaths.bind(files);
    files.verifyRemotePaths = async (expected, signal) => {
      expect(expected.map(file => file.path)).toEqual([assets.entrypointPath, staticPath, "exec", install, sealedPath]);
      expect(Object.isFrozen(expected)).toBe(true);
      expect(expected.every(Object.isFrozen)).toBe(true);
      expect(Reflect.set(expected[0]!, "content", "changed after capture")).toBe(false);
      expect(Reflect.set(expected, "0", { path: "changed", content: "changed" })).toBe(false);
      expect(expected[0]!.content === assets.files.find(file => file.path === assets.entrypointPath)!.content).toBe(true);
      expect(expected[4]!.content === JSON.stringify(journal.attempt!.sealed)).toBe(true);
      entered();
      await pending;
      return verify(expected, signal);
    };
    const sup = controller(journal, files, events, { sync: () => { syncs++; } });
    const ensuring = sup.ensureReady();
    try {
      expect(await Promise.race([proofEntered.then(() => true), ensuring.then(() => false)])).toBe(true);
      expect(journal.phases).toEqual(["prepared"]);
      expect(events).toEqual([]);
      expect(syncs).toBe(0);
      expect(sup.ensureReady()).toBe(ensuring);
      release();
      await ensuring;
      expect(events).toEqual(["submit", "connect:2"]);
    } finally { release(); await ensuring.catch(() => undefined); await sup.shutdown("offline-complete"); await sup.close(); }
  });

  it("orders exact publication proof, backend sync, all local guards, journal submission, and one execution", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [], order: string[] = [];
    let verified = false, synced = false, reads = 0;
    const verify = files.verifyRemotePaths.bind(files), read = files.readPath.bind(files), save = journal.save.bind(journal);
    files.verifyRemotePaths = async (expected, signal) => {
      const receipt = await verify(expected, signal);
      verified = true; order.push("proof");
      return receipt;
    };
    files.readPath = async p => {
      if (synced && ++reads % 5 === 0) order.push("guards");
      return read(p);
    };
    journal.save = async attempt => {
      if (attempt.phase === "submitted" && !attempt.commandAcceptance) {
        expect(verified).toBe(true); expect(synced).toBe(true); expect(reads).toBe(5);
        order.push("submitted");
      }
      return save(attempt);
    };
    const sup = controller(journal, files, events, {
      sync: count => { if (count === 1) { expect(verified).toBe(true); synced = true; order.push("sync"); } },
      afterSubmission: () => { order.push("execute"); },
    });
    try {
      await sup.ensureReady();
      expect(order.slice(0, 6)).toEqual(["proof", "sync", "guards", "submitted", "guards", "execute"]);
      expect(events.filter(event => event === "submit")).toHaveLength(1);
    } finally { await sup.shutdown("offline-complete"); await sup.close(); }
  });

  it("fails closed before staging when the publication capability is missing", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    Object.defineProperty(files, "verifyRemotePaths", { value: undefined });
    const sup = controller(journal, files, events);
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      expect(journal.phases).toEqual([]); expect(events).toEqual([]);
      expect(files.data.has(sealedPath)).toBe(false);
    } finally { await sup.close(); }
  });

  it("rejects a legacy void proof instead of silently accepting unsupported generation fencing", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    files.refuseCleanup = true;
    Object.defineProperty(files, "verifyRemotePaths", { value: async () => undefined });
    const sup = controller(journal, files, events);
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
    } finally { await sup.close(); }
  });

  it("requires server publication proof even without a durable bootstrap journal", async () => {
    const files = new Files(), events: string[] = [];
    let proofs = 0;
    files.verifyRemotePaths = async () => { proofs++; throw new Error("offline publication refused"); };
    const sup = controller(undefined, files, events);
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      await expect(sup.ensureReady()).rejects.toThrow();
      expect(proofs).toBe(1); expect(events).toEqual([]);
      expect(files.data.has(sealedPath)).toBe(false);
    } finally { await sup.close(); }
  });

  it.each([assets.entrypointPath, staticPath, "exec", install, sealedPath])("rejects an unmatched server document file before journal submission: %s", async mismatchPath => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    files.refuseCleanup = true;
    let proofs = 0;
    const verify = files.verifyRemotePaths.bind(files);
    files.verifyRemotePaths = async (expected, signal) => {
      proofs++;
      files.published.set(mismatchPath, "offline mismatched server content");
      return verify(expected, signal);
    };
    const sup = controller(journal, files, events);
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      await expect(sup.ensureReady()).rejects.toThrow();
      expect(proofs).toBe(1);
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
      expect(journal.attempt?.commandAcceptance).toBeUndefined();
    } finally { await sup.close(); }
  });

  it.each([assets.entrypointPath, staticPath, "exec", install, sealedPath])("rejects a local file changed after server proof before journal submission: %s", async mismatchPath => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    files.refuseCleanup = true;
    const sup = controller(journal, files, events, { sync: count => {
      if (count === 1) files.data.set(mismatchPath, "offline local edit after proof");
    } });
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
      expect(files.data.get(mismatchPath)).toBe("offline local edit after proof");
    } finally { await sup.close(); }
  });

  it.each(["sync", "guard", "journal"] as const)("rejects an identical-content document generation replacement during %s", async boundary => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    files.refuseCleanup = true;
    let verifying = false;
    const verify = files.verifyRemotePaths.bind(files), read = files.readPath.bind(files), save = journal.save.bind(journal);
    files.verifyRemotePaths = async (expected, signal) => { const receipt = await verify(expected, signal); verifying = true; return receipt; };
    files.readPath = async p => {
      const value = await read(p);
      if (verifying && boundary === "guard") { verifying = false; files.generation++; }
      return value;
    };
    journal.save = async attempt => {
      await save(attempt);
      if (attempt.phase === "submitted" && boundary === "journal") files.generation++;
    };
    const sup = controller(journal, files, events, { sync: count => { if (count === 1 && boundary === "sync") files.generation++; } });
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      expect(journal.phases).toEqual(boundary === "journal" ? ["prepared", "submitted", "prepared"] : ["prepared"]);
      expect(journal.attempt?.phase).toBe("prepared"); expect(events).toEqual([]);
      expect(journal.attempt?.commandAcceptance).toBeUndefined();
    } finally { await sup.close(); }
  });

  it.each(["proof", "sync", "guard"] as const)("honors cancellation during %s without advancing the prepared journal", async boundary => {
    const journal = new Journal(), files = new Files(), events: string[] = [], cancellation = new AbortController();
    files.refuseCleanup = true;
    let verifying = false;
    const verify = files.verifyRemotePaths.bind(files), read = files.readPath.bind(files);
    files.verifyRemotePaths = async (expected, signal) => {
      const receipt = await verify(expected, signal); verifying = true;
      if (boundary === "proof") cancellation.abort();
      return receipt;
    };
    files.readPath = async p => {
      const value = await read(p);
      if (verifying && boundary === "guard") cancellation.abort();
      return value;
    };
    const sup = controller(journal, files, events, { sync: count => { if (count === 1 && boundary === "sync") cancellation.abort(); } });
    try {
      await expect(sup.ensureReady(cancellation.signal)).rejects.toMatchObject({ name: "AbortError" });
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
      expect(journal.attempt?.commandAcceptance).toBeUndefined();
    } finally { await sup.close(); }
  });

  it("cancels a pending publication proof without journal submission or execution", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [], cancellation = new AbortController();
    files.refuseCleanup = true;
    let entered!: () => void;
    const proofEntered = new Promise<void>(resolve => { entered = resolve; });
    files.verifyRemotePaths = async (_expected, signal) => {
      if (!signal) throw new Error("offline proof must have a lifecycle signal");
      entered();
      return new Promise<never>((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
      });
    };
    const sup = controller(journal, files, events), ensuring = sup.ensureReady(cancellation.signal);
    try {
      await proofEntered;
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
      cancellation.abort();
      await expect(ensuring).rejects.toMatchObject({ name: "AbortError" });
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
    } finally { cancellation.abort(); await ensuring.catch(() => undefined); await sup.close(); }
  });

  it.each(["proof", "receipt"] as const)("does not retain or log raw %s failure details or the encrypted credential", async boundary => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    files.refuseCleanup = true;
    let encrypted = "";
    files.verifyRemotePaths = async expected => {
      encrypted = expected.find(file => file.path === sealedPath)!.content;
      const reject = () => { throw new Error(`offline-token ${encrypted}`); };
      if (boundary === "proof") reject();
      return Object.freeze({ assertCurrent: reject });
    };
    const logging = ["log", "info", "warn", "error", "debug"] as const;
    const spies = logging.map(method => spyOn(console, method).mockImplementation(() => undefined));
    const sup = controller(journal, files, events);
    try {
      const failure = await sup.ensureReady().catch(error => error);
      expect(failure).toMatchObject({ kind: "deploy", remoteCode: "bootstrap_stage_lost" });
      expect(failure.cause).toBeUndefined();
      expect(String(failure)).not.toContain("offline-token"); expect(String(failure)).not.toContain(encrypted);
      expect(spies.every(spy => spy.mock.calls.length === 0)).toBe(true);
      expect(journal.phases).toEqual(["prepared"]); expect(events).toEqual([]);
    } finally { for (const spy of spies) spy.mockRestore(); await sup.close(); }
  });

  it("persists exact command acceptance before waiting for supervisor readiness", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    const sup = controller(journal, files, events, { onConnect: () => {
      expect(journal.attempt?.commandAcceptance).toEqual({ sessionId: journal.attempt!.sessionId,
        workspaceRoot: `/codex_workspace/${journal.attempt!.sessionId}`, jobId: "offline-job" });
    } });
    try {
      await sup.ensureReady();
      expect(journal.attempt?.phase).toBe("submitted");
      await sup.shutdown("offline-complete");
      expect(journal.attempt).toBeNull();
    } finally { await sup.close(); }
  });

  it("retains accepted command identity when cancellation races the acknowledgement", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [], cancellation = new AbortController();
    const sup = controller(journal, files, events, { afterSubmission: () => cancellation.abort() });
    try {
      await expect(sup.ensureReady(cancellation.signal)).rejects.toMatchObject({
        name: "V2SupervisorBootstrapError",
        kind: "deploy",
        remoteCode: "bootstrap_cleanup_unproven",
        cause: { name: "AbortError" },
      });
      expect(journal.attempt?.phase).toBe("submitted");
      expect(journal.attempt?.commandAcceptance?.jobId).toBe("offline-job");
      expect(events).toEqual(["submit"]);
    } finally { await sup.close(); }
  });

  it("records controller-proven cancellation before dispatch as prepared, never as an accepted job", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [], cancellation = new AbortController();
    files.refuseCleanup = true;
    const save = journal.save.bind(journal);
    journal.save = async attempt => { await save(attempt); if (attempt.phase === "submitted") cancellation.abort(); };
    const sup = controller(journal, files, events);
    try {
      await expect(sup.ensureReady(cancellation.signal)).rejects.toMatchObject({ name: "AbortError" });
      expect(events).toEqual([]);
      expect(journal.attempt?.phase).toBe("prepared");
      expect(journal.attempt?.commandAcceptance).toBeUndefined();
      expect(sup.diagnosticExecutions()).toMatchObject([{ dispatchState: "not_started", durableAcceptance: false }]);
    } finally { await sup.close(); }
  });

  it("keeps submission ambiguous if acceptance is invalid or cannot be persisted", async () => {
    for (const invalidAcceptance of [false, true]) {
      const journal = new Journal(), files = new Files(), events: string[] = [];
      const save = journal.save.bind(journal);
      if (!invalidAcceptance) journal.save = async attempt => {
        if (attempt.commandAcceptance) throw new Error("offline acceptance persistence failure");
        await save(attempt);
      };
      const sup = controller(journal, files, events, { invalidAcceptance });
      try {
        await expect(sup.ensureReady()).rejects.toMatchObject({
          kind: "deploy",
          remoteCode: "bootstrap_cleanup_unproven",
        });
        await expect(sup.ensureReady()).rejects.toMatchObject({
          kind: "deploy",
          remoteCode: "bootstrap_cleanup_unproven",
        });
        expect(journal.attempt?.phase).toBe("submitted");
        expect(journal.attempt?.commandAcceptance).toBeUndefined();
        expect(files.data.has(sealedPath)).toBe(true);
        expect(events).toEqual(["submit"]);
      } finally { await sup.close(); }
    }
  });

  it("retains exact diagnostic targets for both recovered-old and newly failed executions", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [], reads: string[] = [];
    await prior(journal, files, "submitted");
    const oldSession = journal.attempt!.sessionId;
    const sup = controller(journal, files, events, { diagnosticReads: reads, onConnect: () => {
      if (events.includes("submit")) throw new Error("offline new supervisor has no ready proof");
    } });
    try {
      await expect(sup.ensureReady()).rejects.toMatchObject({
        kind: "deploy",
        remoteCode: "bootstrap_cleanup_unproven",
      });
      const executions = sup.diagnosticExecutions();
      expect(executions).toHaveLength(2);
      expect(executions[0]).toMatchObject({ sessionId: oldSession, dispatchState: "may_have_started", durableAcceptance: false });
      expect(executions[1]).toMatchObject({ sessionId: journal.attempt!.sessionId, dispatchState: "accepted", durableAcceptance: true,
        commandAcceptance: { jobId: "offline-job" } });
      expect(JSON.stringify(executions)).not.toContain("offline-token");
      expect(JSON.stringify(executions)).not.toContain("ciphertext");
      for (const execution of executions) expect(await sup.readDiagnosticLogForExecution(execution.sessionId, "supervisor.jsonl")).toBeNull();
      await expect(sup.readDiagnosticLogForExecution(crypto.randomUUID(), "supervisor.jsonl")).rejects.toThrow("Unknown");
      expect(reads).toEqual([oldSession, journal.attempt!.sessionId]);
      await sup.readDiagnosticLog("supervisor.jsonl");
      expect(reads.at(-1)).toBe(journal.attempt!.sessionId);
    } finally { await sup.close(); }
  });

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

  it.each([false, true])("makes one cleanup-only attempt after shutdown interrupts recovery (unknown supervisor: %s)", async rejectOld => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const lifecycle = new SupervisorLifecycle(1);
    let entered!: () => void;
    const recoveryEntered = new Promise<void>(resolve => { entered = resolve; });
    const signals: AbortSignal[] = [];
    const sup = controller(journal, files, events, { lifecycle, rejectOld,
      beforeRecoveryShutdown: async signal => {
        if (!signal) throw new Error("Recovery must have a bounded signal");
        signals.push(signal);
        if (signals.length !== 1) { signal.throwIfAborted(); return; }
        entered();
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    });
    const starting = sup.ensureReady().then(() => null, error => error as unknown);
    await recoveryEntered;
    const stopping = sup.shutdown("system_shutdown");
    expect(await starting).toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    if (rejectOld) await expect(stopping).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    else await stopping;
    await sup.close();
    expect(signals).toHaveLength(2);
    expect(signals[0]!.aborted).toBe(true);
    expect(signals[1]!.aborted).toBe(false);
    expect(signals[1]).not.toBe(signals[0]);
    expect(events).toEqual(rejectOld ? ["connect:1"] : ["connect:1", "shutdown:1"]);
    if (rejectOld) {
      expect(journal.attempt?.phase).toBe("submitted");
      await expect(lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
    } else {
      expect(journal.attempt).toBeNull();
      await expect(lifecycle.shutdown(new AbortController().signal)).resolves.toBeUndefined();
    }
  });

  it("honors the shutdown signal before reconnecting interrupted recovery", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const startupCancellation = new AbortController(), shutdownCancellation = new AbortController();
    let attempts = 0;
    const sup = controller(journal, files, events, {
      beforeRecoveryShutdown: async signal => {
        attempts++;
        startupCancellation.abort(new DOMException("offline canceled recovery", "AbortError"));
        signal?.throwIfAborted();
      },
    });
    await expect(sup.ensureReady(startupCancellation.signal)).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    shutdownCancellation.abort(new DOMException("offline expired shutdown", "AbortError"));
    await expect(sup.shutdown("system_shutdown", shutdownCancellation.signal)).rejects.toMatchObject({ name: "AbortError" });
    await sup.close();
    expect(attempts).toBe(1);
    expect(events).toEqual([]);
    expect(journal.attempt?.phase).toBe("submitted");
  });

  it.each(["caller", "deadline"] as const)("retains cleanup debt when the fresh recovery %s signal expires", async source => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const startupCancellation = new AbortController(), callerCancellation = new AbortController(), deadlineCancellation = new AbortController();
    let entered!: () => void;
    const cleanupEntered = new Promise<void>(resolve => { entered = resolve; });
    let attempts = 0;
    const sup = controller(journal, files, events, {
      beforeRecoveryShutdown: async signal => {
        if (!signal) throw new Error("Recovery must have a bounded signal");
        if (++attempts === 1) {
          startupCancellation.abort(new DOMException("offline canceled startup", "AbortError"));
          signal.throwIfAborted();
        }
        entered();
        await new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    });
    await expect(sup.ensureReady(startupCancellation.signal)).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    const timeout = spyOn(AbortSignal, "timeout").mockImplementation(milliseconds => {
      expect(milliseconds).toBe(76_000);
      return deadlineCancellation.signal;
    });
    try {
      const stopping = sup.shutdown("system_shutdown", callerCancellation.signal).then(() => null, error => error as unknown);
      await cleanupEntered;
      (source === "caller" ? callerCancellation : deadlineCancellation).abort(new DOMException("offline cleanup expired", "AbortError"));
      expect(await stopping).toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
      expect(timeout).toHaveBeenCalledTimes(1);
      expect(attempts).toBe(2);
      expect(events).toEqual([]);
      expect(journal.attempt?.phase).toBe("submitted");
    } finally { timeout.mockRestore(); await sup.close(); }
  });

  it("recognizes a cloned durable cleanup receipt after interrupted receipt persistence", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    await prior(journal, files, "submitted");
    const lifecycle = new SupervisorLifecycle(1), cancellation = new AbortController();
    const save = journal.save.bind(journal);
    journal.save = async attempt => {
      await save(attempt);
      if (attempt.phase === "cleanup_proven") {
        cancellation.abort(new DOMException("offline interrupted receipt persistence", "AbortError"));
        throw cancellation.signal.reason;
      }
    };
    const sup = controller(journal, files, events, { lifecycle });
    await expect(sup.ensureReady(cancellation.signal)).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    expect(journal.attempt?.phase).toBe("cleanup_proven");
    await expect(sup.shutdown("system_shutdown")).resolves.toBeUndefined();
    await sup.close();
    expect(journal.attempt).toBeNull();
    expect(events).toEqual(["connect:1", "shutdown:1"]);
    await expect(lifecycle.shutdown(new AbortController().signal)).resolves.toBeUndefined();
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
    ["9d2aa8b7", V2_R10_9D2AA8B7_RETIREMENT_IDENTITY],
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
    // A new outer bootstrap redeploys the registered asset after document loss.
    for (const file of assets.files) files.data.set(file.path, file.content);
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

  it("retries transient cleanup synchronization after persisting signed proof", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    let syncs = 0;
    const sup = controller(journal, files, events, { sync: count => {
      syncs = count;
      if (count === 3) throw new Error("offline cleanup sync");
    } });
    try {
      await sup.ensureReady();
      await expect(sup.shutdown("offline_done")).resolves.toBeUndefined();
      expect(syncs).toBe(4);
      expect(journal.attempt).toBeNull();
      expect(journal.phases.slice(-2)).toEqual(["cleanup_proven", "cleared"]);
    } finally { await sup.close(); }
  });

  it("retains signed cleanup proof when every bounded synchronization retry fails", async () => {
    const journal = new Journal(), files = new Files(), events: string[] = [];
    let syncs = 0;
    const sup = controller(journal, files, events, { sync: count => {
      syncs = count;
      if (count >= 3) throw new Error("offline cleanup sync");
    } });
    try {
      await sup.ensureReady();
      await expect(sup.shutdown("offline_done")).rejects.toThrow();
      expect(syncs).toBe(5);
      expect(journal.attempt?.phase).toBe("cleanup_proven");
    } finally { await sup.close(); }
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

  it("round-trips accepted command metadata while retaining legacy unknown acceptance", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "gp-bootstrap-acceptance-")); roots.push(root);
    const memory = new Journal(); await prior(memory, new Files(), "submitted");
    const legacy = memory.attempt!;
    const acceptance = { sessionId: legacy.sessionId, workspaceRoot: `/codex_workspace/${legacy.sessionId}`, jobId: "exact-offline-job" };
    const disk = new FileSupervisorBootstrapJournal(root, project);
    await disk.withLease(undefined, async entry => {
      await entry.save(legacy);
      expect((await entry.load())?.commandAcceptance).toBeUndefined();
      await entry.save({ ...legacy, commandAcceptance: acceptance });
      expect((await entry.load())?.commandAcceptance).toEqual(acceptance);
      await entry.save({ ...legacy, phase: "cleanup_proven", commandAcceptance: acceptance });
      expect((await entry.load())?.commandAcceptance).toEqual(acceptance);
      for (const invalid of [null, {}, { ...acceptance, jobId: "x".repeat(257) }, { ...acceptance, jobId: "job\n" },
        { ...acceptance, sessionId: crypto.randomUUID() }, { ...acceptance, workspaceRoot: `${acceptance.workspaceRoot}/wrong` },
        { ...acceptance, workspaceRoot: `${acceptance.workspaceRoot}\n` }, { ...acceptance, output: "forbidden" }]) {
        await expect(entry.save({ ...legacy, commandAcceptance: invalid } as never)).rejects.toThrow();
      }
      await expect(entry.save({ ...legacy, phase: "prepared", commandAcceptance: acceptance })).rejects.toThrow("Prepared");
    });
    expect(await readFile(path.join(root, `${project}.json`), "utf8")).not.toContain("offline-token");
  });
});
