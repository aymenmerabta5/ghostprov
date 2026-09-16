import { describe, expect, it } from "bun:test";
import type { FleetWorkerRecord } from "../src/fleet/types";
import { parseProvisionedSandbox, parseYSweetEnvelope } from "../src/prism/protocol";
import { createHeldBootstrapMaintenance, type HeldBootstrapMaintenanceOptions } from "../src/server/held-bootstrap-maintenance";
import { SandboxGateway } from "../src/sandbox/gateway";
import { projectId, SecretValue } from "../src/sandbox/types";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import type { SupervisorBootstrapAttempt, SupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { SupervisorLifecycle } from "../src/v2/supervisor/lifecycle";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const assets = await loadV2SidecarAssets({ revision: "r10" });
const project = projectId("11111111-1111-4111-8111-111111111111");
const worker: Readonly<FleetWorkerRecord> = Object.freeze({
  projectId: project, index: 11, title: "offline cleanup", state: "draining",
  managed: true, leaderGeneration: 3, createdAtMs: 1, updatedAtMs: 2,
  hasReachedReady: false, retirementReason: "bootstrap_hold", replacementEvidence: null,
});
const retired: Readonly<FleetWorkerRecord> = Object.freeze({ ...worker, retirementReason: "replacement", updatedAtMs: 3 });

async function fixture(phase: SupervisorBootstrapAttempt["phase"] | null) {
  const events: string[] = [];
  const binding: BootstrapBinding = {
    projectId: project, sandboxSessionId: "offline-existing-session", leaderGeneration: 2,
    nonce: "offline-existing-supervisor", buildHash: assets.buildHash, schemaHash: assets.schemaHash,
    protocolRevision: "r2", schemaVersion: 2,
  };
  const ySweet = parseYSweetEnvelope({ docId: project, url: "wss://offline.invalid/ws",
    baseUrl: "https://offline.invalid/", token: "offline-token", authorization: "full" });
  const sandbox = parseProvisionedSandbox({ url: "https://offline.invalid/", token: "offline-sandbox-token" });
  const sessionId = "22222222-2222-4222-8222-222222222222";
  const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: {
    instanceId: "offline-original-supervisor", credential: {
      url: ySweet.url, baseUrl: ySweet.baseUrl, docId: project, token: "offline-token",
      authorization: "full", rotationKey: createCredentialRotationKey(),
    },
  } });
  let record: SupervisorBootstrapAttempt | null = phase
    ? { version: 1, phase, binding, sessionId, sealed, connectTimeoutMs: 1000 } : null;
  const journal: SupervisorBootstrapJournal = {
    async withLease(signal, operation) {
      signal?.throwIfAborted();
      return operation({
        load: async () => record,
        save: async value => { events.push("proof.save"); record = value; },
        clear: async () => { events.push("journal.clear"); record = null; },
      });
    },
  };
  const lifecycle = new SupervisorLifecycle(4);
  const controls: { failShutdown: boolean; failSync: boolean } = { failShutdown: false, failSync: false };
  const options: HeldBootstrapMaintenanceOptions = {
    assets: selected => { expect(selected).toBe(project); return assets; },
    journalFactory: selected => { expect(selected).toBe(project); return journal; },
    lifecycle,
    account: { async request() { throw new Error("unexpected account request"); } },
    acquirePermit: async signal => {
      signal.throwIfAborted(); events.push("permit.acquire");
      return () => { events.push("permit.release"); };
    },
    provisioning: {
      async mintYSweet(selected, context, signal) {
        signal?.throwIfAborted(); expect(selected).toBe(project);
        expect(context.source).toBe("held-bootstrap-cleanup");
        events.push("credential"); return ySweet;
      },
      async acquireSandbox(signal, limits) {
        signal?.throwIfAborted(); expect(limits?.maxAttempts).toBe(1);
        events.push("workspace.acquire"); return sandbox;
      },
      async provisionAcquired(selected, received, signal) {
        signal?.throwIfAborted(); expect(selected).toBe(project); expect(received).toBe(sandbox);
        events.push("workspace.sync");
        return { projectId: project, sandbox, ySweet, provisionedAtMs: 1,
          resource: { token: new SecretValue("ResourceToken", "offline-resource"), expiresAtMs: 1e12,
            scopes: [], baseUrl: "https://offline.invalid/" } };
      },
    },
    hostFactory: input => ({
      binding: input.binding, instanceId: input.instanceId,
      async connect() { throw new Error("unexpected readiness"); },
      async ensureReady() { throw new Error("unexpected readiness"); },
      async launch() { throw new Error("unexpected launch"); },
      async deliverCredential() { throw new Error("unexpected credential delivery"); },
      async rotateCredential() { throw new Error("unexpected rotation"); },
      async stop() { throw new Error("unexpected stop"); },
      async shutdown() {
        events.push("supervisor.shutdown");
        if (controls.failShutdown) throw new Error("offline shutdown failure");
      },
      async close() { events.push("supervisor.close"); },
    }),
    createArtifacts: credentials => {
      expect(credentials.projectId).toBe(project); events.push("artifacts.open");
      return {
        ydoc: {
          async readPath() { return null; }, async writePath() {}, async tombstonePath() { return false; },
          async compareAndMutatePaths() { return true; },
          async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; },
        },
        gateway: { async syncWorkspace() {
          events.push("artifacts.sync"); if (controls.failSync) throw new Error("offline sync failure");
        } },
        async close() { events.push("artifacts.close"); },
      };
    },
  };
  return { events, lifecycle, controls, options, binding, ports: createHeldBootstrapMaintenance(options), record: () => record };
}

describe("held bootstrap maintenance composition", () => {
  it("classifies absent and prepared journals as ordinary startup without declared cleanup debt", async () => {
    for (const phase of [null, "prepared"] as const) {
      const value = await fixture(phase);
      expect(await value.ports.cleanupBlockedBootstrap({ ...worker, state: "failed", retirementReason: null }, new AbortController().signal)).toBe("not_required");
      expect(value.events).toEqual([]);
    }
  });

  it("keeps declared cleanup debt when its submitted journal is missing or replaced by prepared state", async () => {
    for (const phase of [null, "prepared"] as const) {
      const value = await fixture(phase); value.lifecycle.requireCleanup(value.binding);
      expect(await value.ports.cleanupBlockedBootstrap({ ...worker, state: "failed", retirementReason: null }, new AbortController().signal)).toBe("unproved");
      expect(value.events).toEqual([]);
    }
  });

  it("does not let one signed incarnation release another pending binding on the same project", async () => {
    const value = await fixture("submitted");
    const other = { ...value.binding, nonce: "offline-other-supervisor" };
    value.lifecycle.requireCleanup(value.binding); value.lifecycle.requireCleanup(other);
    expect(await value.ports.cleanupBlockedBootstrap({ ...worker, state: "failed", retirementReason: null }, new AbortController().signal)).toBe("unproved");
    expect(value.record()?.phase).toBe("cleanup_proven");
    value.lifecycle.confirmCleanup(other);
    expect(await value.ports.cleanupBlockedBootstrap({ ...worker, state: "failed", retirementReason: null }, new AbortController().signal)).toBe("proven");
    expect(value.events).not.toContain("workspace.acquire");
  });

  it("does no remote work for absent journals or durable proof", async () => {
    for (const phase of [null, "cleanup_proven"] as const) {
      const value = await fixture(phase);
      expect(await value.ports.cleanupHeldBootstrap(worker, new AbortController().signal)).toBe(phase !== null);
      expect(value.events).toEqual([]);
      expect(value.record()?.phase ?? null).toBe(phase);
    }
    const value = await fixture(null);
    await value.ports.finalizeHeldBootstrapCleanup(retired, new AbortController().signal);
    expect(value.events).toEqual([]);
  });

  it("proves the original supervisor with one permit and no workspace or launch", async () => {
    const value = await fixture("submitted");
    expect(await value.ports.cleanupHeldBootstrap(worker, new AbortController().signal)).toBe(true);
    expect(value.events).toEqual([
      "permit.acquire", "credential", "supervisor.shutdown", "proof.save", "supervisor.close", "permit.release",
    ]);
    expect(value.record()?.phase).toBe("cleanup_proven");
    await value.lifecycle.shutdown(new AbortController().signal);
  });

  it("shares the same bounded cleanup ports for a blocked active project without acquiring a new sandbox", async () => {
    const value = await fixture("submitted");
    expect(await value.ports.cleanupBlockedBootstrap({ ...worker, state: "failed", retirementReason: null }, new AbortController().signal)).toBe("proven");
    expect(value.events).toEqual([
      "permit.acquire", "credential", "supervisor.shutdown", "proof.save", "supervisor.close", "permit.release",
    ]);
    expect(value.record()?.phase).toBe("cleanup_proven");
    expect(value.events).not.toContain("workspace.acquire");
  });

  it("retains unproven cleanup debt and releases its permit after failure", async () => {
    const value = await fixture("submitted"); value.controls.failShutdown = true;
    await expect(value.ports.cleanupHeldBootstrap(worker, new AbortController().signal)).rejects.toThrow();
    expect(value.record()?.phase).toBe("submitted");
    expect(value.events.at(-1)).toBe("permit.release");
    expect(value.events).not.toContain("workspace.acquire");
    await expect(value.lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
  });

  it("opens workspace resources only after proof and the retirement transition", async () => {
    const value = await fixture("cleanup_proven");
    await expect(value.ports.finalizeHeldBootstrapCleanup(worker, new AbortController().signal)).rejects.toThrow();
    expect(value.events).toEqual([]);
    await value.ports.finalizeHeldBootstrapCleanup(retired, new AbortController().signal);
    expect(value.events).toEqual([
      "permit.acquire", "workspace.acquire", "workspace.sync", "artifacts.open", "artifacts.sync",
      "journal.clear", "artifacts.close", "permit.release",
    ]);
    expect(value.record()).toBeNull();
  });

  it("keeps durable proof when artifact synchronization fails", async () => {
    const value = await fixture("cleanup_proven"); value.controls.failSync = true;
    await expect(value.ports.finalizeHeldBootstrapCleanup(retired, new AbortController().signal)).rejects.toThrow();
    expect(value.record()?.phase).toBe("cleanup_proven");
    expect(value.events.slice(-2)).toEqual(["artifacts.close", "permit.release"]);
    expect(value.events).not.toContain("journal.clear");
  });

  it("does not open a host or workspace after shutdown begins", async () => {
    const value = await fixture("submitted"); value.lifecycle.beginShutdown();
    await expect(value.ports.cleanupHeldBootstrap(worker, new AbortController().signal)).rejects.toThrow();
    expect(value.events).toEqual([]);
    expect(value.record()?.phase).toBe("submitted");
  });

  it("rechecks shutdown after a queued permit resolves", async () => {
    const value = await fixture("submitted");
    let entered!: () => void;
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    let grant!: (release: () => void) => void;
    const permit = new Promise<() => void>(resolve => { grant = resolve; });
    const ports = createHeldBootstrapMaintenance({ ...value.options,
      acquirePermit: async () => { entered(); return permit; },
    });
    const cleanup = ports.cleanupHeldBootstrap(worker, new AbortController().signal)
      .then(() => null, error => error as Error);
    await waiting;
    value.lifecycle.beginShutdown();
    grant(() => { value.events.push("permit.release"); });
    expect(await cleanup).toBeInstanceOf(Error);
    expect(value.events).toEqual(["permit.release"]);
    expect(value.record()?.phase).toBe("submitted");
  });

  it("retains its permit until an aborted shared workspace sync actually settles", async () => {
    const value = await fixture("cleanup_proven");
    let entered!: () => void;
    const waiting = new Promise<void>(resolve => { entered = resolve; });
    let respond!: (response: Response) => void;
    const response = new Promise<Response>(resolve => { respond = resolve; });
    const cancellation = new AbortController();
    const ports = createHeldBootstrapMaintenance({ ...value.options,
      createArtifacts: credentials => {
        const artifacts = value.options.createArtifacts!(credentials);
        return { ...artifacts, gateway: new SandboxGateway({
          projectId: project, sandbox: credentials.sandbox, syncBatchMs: 0,
          clock: { now: () => 1, sleep: async () => {}, waitUntil: async () => {} },
          account: { async request() { entered(); return response; } },
        }) };
      },
    });
    let settled = false;
    const cleanup = ports.finalizeHeldBootstrapCleanup(retired, cancellation.signal)
      .then(() => { settled = true; return null; }, error => { settled = true; return error as Error; });
    await waiting;
    cancellation.abort();
    for (let index = 0; index < 30; index++) await Promise.resolve();
    try {
      expect(settled).toBe(false);
      expect(value.events).not.toContain("permit.release");
      expect(value.events).not.toContain("artifacts.close");
    } finally {
      respond(Response.json({ status: "synced", tokens: { hasCurrentYSweetToken: true } }));
      expect(await cleanup).toBeInstanceOf(Error);
    }
    expect(value.events.slice(-2)).toEqual(["artifacts.close", "permit.release"]);
    expect(value.record()?.phase).toBe("cleanup_proven");
    expect(value.events).not.toContain("journal.clear");
  });
});
