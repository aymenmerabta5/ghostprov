import { describe, expect, it } from "bun:test";
import type { BootstrapBinding, SealedBootstrapCredential } from "../src/v2/bootstrap/crypto";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import type { V2BootstrapYDoc, V2BootstrapGateway } from "../src/v2/bootstrap/controller";
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import type {
  V2BootstrapSupervisor,
  V2SupervisorHostOptions,
  V2SupervisorLaunchResult,
} from "../src/v2/supervisor/host";
import {
  SUPERVISOR_CREDENTIAL_PATH,
  SUPERVISOR_STATIC_PATH,
} from "../src/v2/supervisor/launcher";

const BINDING: BootstrapBinding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 4,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "supervisor-nonce-1",
  protocolRevision: "r2",
  schemaVersion: 2,
});

const ENTRYPOINT = `.ghostprov-v2/revisions/r9/${"b".repeat(64)}/${"a".repeat(64)}/sidecar.bundle.js`;
const ASSETS = Object.freeze({
  revision: "r9",
  protocolRevision: "r2",
  transportProfile: "bulk-v1",
  buildHash: BINDING.buildHash,
  schemaHash: BINDING.schemaHash,
  bundleSha256: "c".repeat(64),
  entrypointPath: ENTRYPOINT,
  manifestPath: `${ENTRYPOINT}.manifest.json`,
  obsoletePaths: [],
  files: [],
  manifest: { path: `${ENTRYPOINT}.manifest.json`, content: "{}", sha256: "d".repeat(64) },
} as unknown as V2SidecarAssets);

class FakeYDoc implements V2BootstrapYDoc {
  readonly files = new Map<string, string>();
  async readPath(path: string): Promise<string | null> { return this.files.get(path) ?? null; }
  async writePath(path: string, content: string): Promise<void> { this.files.set(path, content); }
  async tombstonePath(path: string): Promise<boolean> { return this.files.delete(path); }
  async compareAndMutatePaths(input: GuardedYDocMutation): Promise<boolean> {
    if (input.guards.some(guard => (this.files.get(guard.path) ?? null) !== guard.equals)) return false;
    for (const mutation of input.mutations) {
      if (mutation.kind === "write") this.files.set(mutation.path, mutation.content);
      else this.files.delete(mutation.path);
    }
    return true;
  }
  async renderSyncState() { return { clientStateVector: "state", clientDeleteSetUpdate: "delete" }; }
}

class FakeHost implements V2BootstrapSupervisor {
  readonly binding = BINDING;
  readonly instanceId = "supervisor-instance-1";
  connects = 0;
  launches = 0;
  credentials = 0;
  stops = 0;
  shutdowns = 0;
  async ensureReady(): Promise<void> { this.connects++; }
  async connect(): Promise<void> { this.connects++; }
  async launch(): Promise<V2SupervisorLaunchResult> {
    this.launches++;
    return {
      encodedHello: "abc",
      process: { pid: 3, startTick: "4", processGroup: 3, session: 3 },
    };
  }
  async deliverCredential(): Promise<void> { this.credentials++; }
  async stop(): Promise<string | null> { this.stops++; return null; }
  async rotateCredential(): Promise<void> {}
  async shutdown(): Promise<void> { this.shutdowns++; }
  async close(): Promise<void> {}
}

function bootstrapFixture(gateway: Pick<V2BootstrapGateway, "prepareRelayExecSession" | "runRelayExec">, host = new FakeHost()) {
  const ydoc = new FakeYDoc();
  const supervisor = new V2SandboxSupervisor({
    binding: BINDING,
    ySweet: parseYSweetEnvelope({ url: "wss://y.test/socket", baseUrl: "https://y.test",
      docId: BINDING.projectId, token: "offline-token", authorization: "full" }),
    ydoc,
    gateway: { async syncWorkspace() {}, async triggerRender() { throw new Error("unexpected Render"); },
      async fetchText() { return { status: 404, text: "", headers: new Headers() }; }, ...gateway },
    assets: ASSETS,
    hostFactory: () => host,
  });
  return { supervisor, ydoc, host };
}

describe("V2 sandbox supervisor bootstrap", () => {
  it("installs once, seals credentials, and delegates sidecar lifecycle without Render", async () => {
    const ydoc = new FakeYDoc();
    const continuation = {
      conversationId: "cdx1_11111111-1111-4111-8111-111111111111",
      previousResponseId: "resp_previous_1",
      codexSessionId: "codex-session-1111111111111111",
      listenSnapshot: {
        conversation_id: "cdx1_11111111-1111-4111-8111-111111111111",
        workspace_session_id: "workspace-session-1",
        codex_session_id: "codex-session-1111111111111111",
        last_turn_id: "turn-1",
        endpoint_identity: null,
        last_exec_at: null,
        transcript_cursor: 0,
        created_at: "2026-09-04T00:00:00.000Z",
        updated_at: "2026-09-04T00:00:00.000Z",
        last_saved_at: null,
        user_id: null,
        project_id: BINDING.projectId,
      },
    };
    let execs = 0;
    let execRequest: unknown = null;
    let execContinuation: unknown = null;
    let stagedCredential: string | null = null;
    let syncs = 0;
    const gateway: V2BootstrapGateway = {
      async syncWorkspace() { syncs++; },
      async triggerRender() { throw new Error("Render must not be used"); },
      async fetchText() { return { status: 404, text: "", headers: new Headers() }; },
      async prepareRelayExecSession() { return continuation; },
      async runRelayExec(request, receivedContinuation) {
        execs++;
        execRequest = request;
        execContinuation = receivedContinuation;
        stagedCredential = ydoc.files.get(SUPERVISOR_CREDENTIAL_PATH) ?? null;
        return { status: 200, output: "" };
      },
    };
    const host = new FakeHost();
    let hostOptions: V2SupervisorHostOptions | null = null;
    const ySweet = parseYSweetEnvelope({
      url: `wss://y.test/d/${BINDING.projectId}/ws`,
      baseUrl: `https://y.test/d/${BINDING.projectId}`,
      docId: BINDING.projectId,
      token: "secret-y-token",
      authorization: "full",
    });
    const supervisor = new V2SandboxSupervisor({
      binding: BINDING,
      ySweet,
      ydoc,
      gateway,
      assets: ASSETS,
      hostFactory: options => {
        hostOptions = options;
        return host;
      },
    });

    await Promise.all([supervisor.ensureReady(), supervisor.ensureReady()]);
    expect(execs).toBe(1);
    expect(syncs).toBe(2);
    expect(supervisor.instanceId).toBe((hostOptions as unknown as V2SupervisorHostOptions).instanceId);
    expect(execRequest).toEqual({
      entrypointPath: ENTRYPOINT,
      supervisorNonce: BINDING.nonce,
    });
    expect(execContinuation).toEqual(continuation);
    expect(JSON.parse(ydoc.files.get(SUPERVISOR_STATIC_PATH)!)).toMatchObject({
      version: 1,
      binding: BINDING,
      entrypointPath: ENTRYPOINT,
    });
    expect(stagedCredential).not.toContain("secret-y-token");
    expect(ydoc.files.has(SUPERVISOR_CREDENTIAL_PATH)).toBe(false);
    expect(hostOptions).toMatchObject({ binding: BINDING });

    await supervisor.launch({ sidecarBinding: BINDING, entrypointPath: ENTRYPOINT, staticConfig: "{}" });
    await supervisor.deliverCredential({
      sidecarBinding: BINDING,
      sealed: { version: 1 } as SealedBootstrapCredential,
    });
    await supervisor.stop({ launchNonces: [BINDING.nonce], reason: "test stop" });
    await supervisor.shutdown("test shutdown");
    expect({
      launches: host.launches,
      credentials: host.credentials,
      stops: host.stops,
      shutdowns: host.shutdowns,
    }).toEqual({ launches: 1, credentials: 1, stops: 1, shutdowns: 1 });
    await supervisor.shutdown("again");
    expect(host.shutdowns).toBe(1);
    await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "shutdown" });
    await supervisor.close();
  });

  it("retains a failed attempt instead of starting a second platform discovery turn", async () => {
    let discoveries = 0;
    const { supervisor } = bootstrapFixture({
      async prepareRelayExecSession() { discoveries++; throw new Error("offline discovery failure"); },
      async runRelayExec() { throw new Error("must not launch"); },
    });
    await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "exec" });
    await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "exec" });
    expect(discoveries).toBe(1);
    await supervisor.close();
  });

  it("joins a noncooperative discovery during close and fences every subsequent write and launch", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let execs = 0;
    const { supervisor, ydoc } = bootstrapFixture({
      async prepareRelayExecSession() {
        entered.resolve();
        await release.promise;
        throw new DOMException("discovery finished after close", "AbortError");
      },
      async runRelayExec() { execs++; return { status: 200, output: "" }; },
    });
    const ensuring = supervisor.ensureReady().catch(error => error);
    await entered.promise;
    const closing = supervisor.close();
    release.resolve();
    await closing;
    expect(await ensuring).toMatchObject({ name: "AbortError" });
    expect(execs).toBe(0);
    expect(ydoc.files.size).toBe(0);
    await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "shutdown" });
  });
});
