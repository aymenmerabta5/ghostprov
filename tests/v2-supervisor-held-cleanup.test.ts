import { describe, expect, it } from "bun:test";
import type { FleetWorkerRecord } from "../src/fleet/types";
import { projectId } from "../src/sandbox/types";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { loadV2SidecarAssets, loadV2SidecarRecoveryAssets, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY, type V2SidecarAssets } from "../src/v2/sidecar/assets";
import type { BootstrapJournalEntry, SupervisorBootstrapAttempt, SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { finalizeHeldSupervisorBootstrapCleanup, proveBlockedSupervisorBootstrapCleanup, proveHeldSupervisorBootstrapCleanup, type V2HeldSupervisorBootstrapCleanupOptions } from "../src/v2/supervisor/controller";
import { V2SupervisorError, type V2SupervisorHostOptions } from "../src/v2/supervisor/host";
import { SupervisorLifecycle } from "../src/v2/supervisor/lifecycle";
import { supervisorCleanupFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const assets = await loadV2SidecarAssets({ revision: "r10" });
const project = projectId("11111111-1111-4111-8111-111111111111");
const credential = parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: project, token: "offline-token", authorization: "full" });
const worker: Readonly<FleetWorkerRecord> = Object.freeze({ projectId: project, index: 11, title: "offline worker", state: "draining",
  createdAtMs: 1, updatedAtMs: 2, leaderGeneration: 3, hasReachedReady: false, managed: true, retirementReason: "bootstrap_hold", replacementEvidence: null });
const staticPath = ".ghostprov-v2/supervisor/static.json", installPath = ".ghostprov-v2/direct-exec/install.sh", sealedPath = ".ghostprov-v2/supervisor/bootstrap.enc.json";

class Journal implements SupervisorBootstrapJournal, BootstrapJournalEntry {
  attempt: SupervisorBootstrapAttempt | null = null;
  failSave = false;
  clears = 0;
  async withLease<T>(signal: AbortSignal | undefined, run: (entry: BootstrapJournalEntry) => Promise<T>) { signal?.throwIfAborted(); return run(this); }
  async load() { return this.attempt ? structuredClone(this.attempt) : null; }
  async save(attempt: SupervisorBootstrapAttempt) { if (this.failSave) throw new Error("offline persistence failure"); this.attempt = structuredClone(attempt); }
  async clear() { this.attempt = null; this.clears++; }
}

class Files {
  readonly data = new Map<string, string>();
  async readPath(path: string) { return this.data.get(path) ?? null; }
  async writePath(path: string, content: string) { this.data.set(path, content); }
  async tombstonePath(path: string) { return this.data.delete(path); }
  async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; }
  async compareAndMutatePaths(input: GuardedYDocMutation) {
    if (input.guards.some(guard => (this.data.get(guard.path) ?? null) !== guard.equals)) return false;
    for (const mutation of input.mutations) mutation.kind === "write" ? this.data.set(mutation.path, mutation.content) : this.data.delete(mutation.path);
    return true;
  }
}

async function fixture(phase: SupervisorBootstrapAttempt["phase"] | null = "submitted", priorAssets: V2SidecarAssets = assets) {
  const journal = new Journal(), files = new Files(), events: string[] = [], lifecycle = new SupervisorLifecycle(2);
  const binding: BootstrapBinding = { projectId: project, sandboxSessionId: "offline-sandbox", leaderGeneration: 2,
    buildHash: priorAssets.buildHash, schemaHash: priorAssets.schemaHash, nonce: "offline-held-bootstrap", protocolRevision: "r2", schemaVersion: 2 };
  const sessionId = "22222222-2222-4222-8222-222222222222", rotationKey = createCredentialRotationKey();
  const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: { instanceId: "offline-old-instance",
    credential: { url: credential.url, baseUrl: credential.baseUrl, docId: project, token: "offline-token", authorization: "full", rotationKey } } });
  if (phase) journal.attempt = { version: 1, phase, sessionId, binding, sealed, connectTimeoutMs: 1000 };
  const launch = supervisorCleanupFiles({ entrypointPath: priorAssets.entrypointPath, supervisorNonce: binding.nonce });
  files.data.set("exec", launch.launcher); files.data.set(installPath, launch.installer); files.data.set(sealedPath, JSON.stringify(sealed));
  files.data.set(staticPath, supervisorStaticConfig({ binding, entrypointPath: priorAssets.entrypointPath, connectTimeoutMs: 1000 }));
  let hostInput: V2SupervisorHostOptions | undefined;
  const control: { shutdown?: (signal?: AbortSignal) => Promise<void>; sync?: (signal?: AbortSignal) => Promise<void> } = {};
  const options: V2HeldSupervisorBootstrapCleanupOptions = {
    worker, assets, bootstrapJournal: journal, connectTimeoutMs: 1000, commandTimeoutMs: 1000,
    async getYSweet() { events.push("credential"); return credential; },
    async getArtifacts(received) {
      expect(received).toEqual(binding); events.push("artifacts");
      return { ydoc: files, gateway: { async syncWorkspace(signal) { events.push("sync"); await control.sync?.(signal); } },
        async close() { events.push("artifacts.close"); } };
    },
    onCleanupRequired: value => lifecycle.requireCleanup(value),
    onCleanupProven: value => { expect(value).toEqual(binding); events.push("proof"); lifecycle.confirmCleanup(value); },
    hostFactory: input => {
      hostInput = input;
      return { binding: input.binding, instanceId: input.instanceId,
        async connect() { throw new Error("unexpected readiness connect"); }, async ensureReady() { throw new Error("unexpected readiness"); },
        async launch() { throw new Error("unexpected launch"); }, async deliverCredential() { throw new Error("unexpected credential delivery"); },
        async rotateCredential() { throw new Error("unexpected rotation"); }, async stop() { throw new Error("unexpected stop"); },
        async shutdown(_reason, signal) { events.push("shutdown"); await control.shutdown?.(signal); }, async close() { events.push("host.close"); } };
    },
  };
  return { options, journal, files, events, binding, lifecycle, control, rotationKey, hostInput: () => hostInput };
}

const signal = () => new AbortController().signal;
const retired = (options: V2HeldSupervisorBootstrapCleanupOptions): V2HeldSupervisorBootstrapCleanupOptions => ({ ...options,
  worker: { ...options.worker, retirementReason: "replacement", updatedAtMs: 3 } });

describe("held supervisor cleanup proof", () => {
  it.each([true, false])("proves an active blocked project's exact journal without another launch (managed=%s)", async managed => {
    const value = await fixture(), before = new Map(value.files.data);
    const options = { ...value.options, worker: { ...worker, managed, state: "failed" as const, retirementReason: null } };
    expect(await proveBlockedSupervisorBootstrapCleanup(options, signal())).toBe(true);
    expect(value.events).toEqual(["credential", "shutdown", "proof", "host.close"]);
    expect(value.files.data).toEqual(before);
    expect(value.journal.attempt?.phase).toBe("cleanup_proven");
    expect(value.journal.clears).toBe(0);
    expect(value.hostInput()?.binding).toEqual(value.binding);
    value.events.length = 0;
    expect(await proveBlockedSupervisorBootstrapCleanup(options, signal())).toBe(true);
    expect(value.events).toEqual(["proof"]);
  });

  it("enables sequence-zero challenge only for explicitly authorized journals without acceptance", async () => {
    for (const accepted of [false, true]) {
      const value = await fixture();
      if (accepted) value.journal.attempt = { ...value.journal.attempt!, commandAcceptance: {
        sessionId: value.journal.attempt!.sessionId,
        workspaceRoot: `/codex_workspace/${value.journal.attempt!.sessionId}`,
        jobId: "offline-accepted-job",
      } };
      await proveBlockedSupervisorBootstrapCleanup({ ...value.options,
        worker: { ...worker, state: "failed", retirementReason: null },
        recoverUnacceptedInitial: true,
      }, signal());
      expect(value.hostInput()?.recoverUnacceptedInitial).toBe(accepted ? undefined : true);
      expect(value.journal.attempt?.phase).toBe("cleanup_proven");
    }
  });

  it.each([null, "prepared"] as const)("does not treat a blocked %s journal as execution cleanup proof", async phase => {
    const value = await fixture(phase);
    expect(await proveBlockedSupervisorBootstrapCleanup({ ...value.options,
      worker: { ...worker, state: "failed", retirementReason: null } }, signal())).toBe(false);
    expect(value.events).toEqual([]);
    expect(value.journal.clears).toBe(0);
  });

  it("rejects a hold transition before an ordinary recovery cleanup probe", async () => {
    const value = await fixture();
    await expect(proveBlockedSupervisorBootstrapCleanup(value.options, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" });
    expect(value.events).toEqual([]);
  });

  it.each([null, "prepared"] as const)("does not infer proof or open resources for a %s journal", async phase => {
    const value = await fixture(phase), before = structuredClone(value.journal.attempt);
    expect(await proveHeldSupervisorBootstrapCleanup(value.options, signal())).toBe(false);
    expect(value.events).toEqual([]); expect(value.journal.attempt).toEqual(before); expect(value.journal.clears).toBe(0);
  });

  it.each(["current", "archived"] as const)("obtains exact %s proof without replay and retains it across the manifest boundary", async release => {
    const priorAssets = release === "archived" ? await loadV2SidecarRecoveryAssets(V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY) : assets;
    const value = await fixture("submitted", priorAssets), before = new Map(value.files.data);
    expect(await proveHeldSupervisorBootstrapCleanup(value.options, signal())).toBe(true);
    expect(value.hostInput()).toMatchObject({ binding: value.binding, instanceId: "offline-old-instance", rotationKey: value.rotationKey, credential, recoverExisting: true });
    expect(value.events).toEqual(["credential", "shutdown", "proof", "host.close"]);
    expect(value.journal.attempt?.phase).toBe("cleanup_proven"); expect(value.files.data).toEqual(before); expect(value.journal.clears).toBe(0);
    // A crash before the manifest CAS reuses the durable proof without a host.
    value.events.length = 0;
    expect(await proveHeldSupervisorBootstrapCleanup(value.options, signal())).toBe(true);
    expect(value.events).toEqual(["proof"]);
    await expect(value.lifecycle.shutdown(signal())).resolves.toBeUndefined();
    await finalizeHeldSupervisorBootstrapCleanup(retired(value.options), signal());
    expect(value.journal.attempt).toBeNull(); expect(value.journal.clears).toBe(1);
    expect([...value.files.data.keys()]).toEqual([staticPath]);
  });

  it.each(["project", "future_generation", "unmanaged", "state", "index"] as const)("rejects changed %s authority before opening resources", async kind => {
    const value = await fixture();
    const changed = kind === "project" ? { projectId: projectId("33333333-3333-4333-8333-333333333333") }
      : kind === "future_generation" ? { leaderGeneration: 1 } : kind === "unmanaged" ? { managed: false }
      : kind === "state" ? { state: "ready" as const } : { index: 0 };
    await expect(proveHeldSupervisorBootstrapCleanup({ ...value.options, worker: { ...worker, ...changed } }, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" });
    expect(value.events).toEqual([]); expect(value.journal.attempt?.phase).toBe("submitted");
  });

  it("rejects unregistered cross-paired releases before opening resources", async () => {
    const value = await fixture();
    value.journal.attempt = { ...value.journal.attempt!, binding: { ...value.binding, schemaHash: "e".repeat(64) } };
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict", reason: "release_mismatch" });
    expect(value.events).toEqual([]); expect(value.journal.clears).toBe(0);
  });

  it("accepts the journal's exact manifest generation without advancing it", async () => {
    const value = await fixture();
    expect(await proveHeldSupervisorBootstrapCleanup({ ...value.options, worker: { ...worker, leaderGeneration: value.binding.leaderGeneration } }, signal())).toBe(true);
    expect(value.hostInput()?.binding.leaderGeneration).toBe(value.binding.leaderGeneration);
  });

  it("rejects a credential for another project before constructing a host", async () => {
    const value = await fixture();
    await expect(proveHeldSupervisorBootstrapCleanup({ ...value.options,
      getYSweet: async () => ({ ...credential, docId: projectId("33333333-3333-4333-8333-333333333333") }),
    }, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" });
    expect(value.hostInput()).toBeUndefined(); expect(value.events).toEqual([]); expect(value.journal.attempt?.phase).toBe("submitted");
  });

  it.each(["submitted", "cleanup_proven"] as const)("rejects malformed sealed authority in %s", async phase => {
    const value = await fixture(phase);
    value.journal.attempt = { ...value.journal.attempt!, sealed: { ...value.journal.attempt!.sealed, ciphertext: "invalid" } };
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, signal())).rejects.toThrow();
    expect(value.events).toEqual([]); expect(value.journal.attempt?.phase).toBe(phase);
  });

  it.each(["timeout", "abort"] as const)("retains debt and closes the host after %s", async failure => {
    const value = await fixture(), cancellation = new AbortController();
    value.control.shutdown = async () => {
      if (failure === "abort") { cancellation.abort(); cancellation.signal.throwIfAborted(); }
      throw new V2SupervisorError("offline timeout", "timeout", undefined, "transport");
    };
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, cancellation.signal)).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    expect(value.events).toEqual(["credential", "shutdown", "host.close"]); expect(value.journal.attempt?.phase).toBe("submitted");
    await expect(value.lifecycle.shutdown(signal())).rejects.toThrow("incomplete");
  });

  it("persists irreversible proof when cancellation races the signed reply", async () => {
    const value = await fixture(), cancellation = new AbortController();
    value.control.shutdown = async () => { cancellation.abort(); };
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, cancellation.signal)).rejects.toThrow();
    expect(value.journal.attempt?.phase).toBe("cleanup_proven"); expect(value.journal.clears).toBe(0);
    expect(value.events).toEqual(["credential", "shutdown", "proof", "host.close"]);
    await expect(value.lifecycle.shutdown(signal())).resolves.toBeUndefined();
  });

  it("does not release debt if signed proof cannot be persisted", async () => {
    const value = await fixture(); value.journal.failSave = true;
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    expect(value.journal.attempt?.phase).toBe("submitted"); expect(value.events).toEqual(["credential", "shutdown", "host.close"]);
    await expect(value.lifecycle.shutdown(signal())).rejects.toThrow("incomplete");
  });

  it("does not begin cleanup after caller cancellation", async () => {
    const value = await fixture(), cancellation = new AbortController(); cancellation.abort();
    await expect(proveHeldSupervisorBootstrapCleanup(value.options, cancellation.signal)).rejects.toThrow(); expect(value.events).toEqual([]);
  });
});

describe("held supervisor cleanup finalization", () => {
  it("requires a persisted retirement before touching the journal or artifacts", async () => {
    const value = await fixture("cleanup_proven");
    await expect(finalizeHeldSupervisorBootstrapCleanup(value.options, signal())).rejects.toMatchObject({ remoteCode: "bootstrap_ownership_conflict" });
    expect(value.events).toEqual([]); expect(value.journal.attempt?.phase).toBe("cleanup_proven");
  });

  it.each(["submitted", "prepared"] as const)("rejects %s during retirement without opening resources", async phase => {
    const value = await fixture(phase);
    await expect(finalizeHeldSupervisorBootstrapCleanup(retired(value.options), signal())).rejects.toMatchObject({ remoteCode: "bootstrap_cleanup_unproven" });
    expect(value.events).toEqual([]); expect(value.journal.attempt?.phase).toBe(phase);
  });

  it("is idempotent after a crash between journal clearing and project retirement", async () => {
    const value = await fixture("cleanup_proven");
    await finalizeHeldSupervisorBootstrapCleanup(retired(value.options), signal());
    value.events.length = 0;
    await finalizeHeldSupervisorBootstrapCleanup(retired(value.options), signal());
    expect(value.events).toEqual([]); expect(value.journal.clears).toBe(1);
  });

  it.each(["foreign_file", "sync_failure", "cancel_after_sync", "sync_readback"] as const)("retains durable proof after %s and closes owned resources", async failure => {
    const value = await fixture("cleanup_proven"), cancellation = new AbortController();
    if (failure === "foreign_file") value.files.data.set("exec", "foreign launcher");
    value.control.sync = async () => {
      if (failure === "sync_failure") throw new Error("offline sync failure");
      if (failure === "cancel_after_sync") cancellation.abort();
      if (failure === "sync_readback") value.files.data.set("exec", "unproven remote generation");
    };
    await expect(finalizeHeldSupervisorBootstrapCleanup(retired(value.options), cancellation.signal)).rejects.toThrow();
    expect(value.journal.attempt?.phase).toBe("cleanup_proven"); expect(value.journal.clears).toBe(0);
    expect(value.events[0]).toBe("artifacts"); expect(value.events.at(-1)).toBe("artifacts.close");
    expect(value.events).not.toContain("credential"); expect(value.events).not.toContain("shutdown");
  });
});
