import { describe, expect, it } from "bun:test";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import type { V2BootstrapSupervisor, V2SupervisorHostOptions } from "../src/v2/supervisor/host";
import type { V2BootstrapGateway, V2BootstrapYDoc } from "../src/v2/bootstrap/controller";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { openSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { V2_R9_RELEASE, type V2SidecarAssets } from "../src/v2/sidecar/assets";
import { DIRECT_SUPERVISOR_INSTALL_PATH, DIRECT_SUPERVISOR_LAUNCH_PATH, directSupervisorLaunchFiles } from "../src/v2/supervisor/direct-launcher";
import { SUPERVISOR_CREDENTIAL_PATH, SUPERVISOR_STATIC_PATH } from "../src/v2/supervisor/launcher";

const ENTRYPOINT = `.ghostprov-v2/revisions/r9/${V2_R9_RELEASE.schemaHash}/${V2_R9_RELEASE.buildHash}/sidecar.bundle.js`;
const BINDING = Object.freeze({ projectId: "11111111-1111-4111-8111-111111111111", sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 1, buildHash: V2_R9_RELEASE.buildHash, schemaHash: V2_R9_RELEASE.schemaHash,
  nonce: "supervisor-test", protocolRevision: "r2" as const, schemaVersion: 2 as const });
const ASSETS: V2SidecarAssets = { ...V2_R9_RELEASE, entrypointPath: ENTRYPOINT,
  manifest: { path: "manifest.json", logicalPath: "manifest.json", content: "{}", sha256: "" }, files: [], obsoletePaths: [] };
const TRANSIENTS = [DIRECT_SUPERVISOR_LAUNCH_PATH, DIRECT_SUPERVISOR_INSTALL_PATH, SUPERVISOR_CREDENTIAL_PATH];

class Files implements V2BootstrapYDoc {
  readonly data = new Map<string, string>();
  async readPath(path: string) { return this.data.get(path) ?? null; }
  async writePath(path: string, content: string) { this.data.set(path, content); }
  async tombstonePath(path: string) { return this.data.delete(path); }
  async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; }
  async compareAndMutatePaths(input: GuardedYDocMutation) {
    if (input.guards.some(guard => (this.data.get(guard.path) ?? null) !== guard.equals)) return false;
    for (const mutation of input.mutations) {
      if (mutation.kind === "write") this.data.set(mutation.path, mutation.content);
      else this.data.delete(mutation.path);
    }
    return true;
  }
}

class Host implements V2BootstrapSupervisor {
  readonly binding = BINDING;
  readonly instanceId = "test-host";
  connects = 0; launches = 0; shutdowns = 0; closes = 0;
  onConnect: () => Promise<void> = async () => undefined;
  async ensureReady() { return this.connect(); }
  async connect() { this.connects++; await this.onConnect(); }
  async launch() { this.launches++; return { encodedHello: "hello", process: { pid: 4, startTick: "5", processGroup: 4, session: 4 } }; }
  async deliverCredential() {}
  async stop() { return null; }
  async rotateCredential() {}
  async shutdown() { this.shutdowns++; }
  async close() { this.closes++; }
}

function fixture() {
  const files = new Files(); const host = new Host();
  const calls: string[] = [];
  let hostOptions: V2SupervisorHostOptions | null = null;
  let staged: Map<string, string> | null = null;
  const gateway: V2BootstrapGateway = {
    async syncWorkspace() {}, async triggerRender() { throw new Error("unexpected Render"); },
    async fetchText() { throw new Error("unexpected generic request"); },
    async prepareRelayExecSession() { throw new Error("unexpected LLM discovery"); },
    async runRelayExec() { throw new Error("unexpected LLM launch"); },
    async startSupervisorExec(request) {
      calls.push(request.sessionId); staged = new Map(files.data);
      return { sessionId: request.sessionId, workspaceRoot: `/codex_workspace/${request.sessionId}`, jobId: "job-1" };
    },
  };
  const supervisor = new V2SandboxSupervisor({ binding: BINDING, ydoc: files, gateway, assets: ASSETS,
    ySweet: parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: BINDING.projectId,
      token: "secret-y-token", authorization: "full" }), hostFactory: options => { hostOptions = options; return host; } });
  return { files, host, calls, gateway, supervisor, staged: () => staged, hostOptions: () => hostOptions };
}

describe("direct R9 supervisor bootstrap", () => {
  it("seals to a private execution identity, installs once, and retains the supervisor for recovery", async () => {
    const f = fixture();
    try {
      await Promise.all([f.supervisor.ensureReady(), f.supervisor.ensureReady()]);
      expect(f.calls).toHaveLength(1);
      const staged = f.staged()!;
      const sealed = staged.get(SUPERVISOR_CREDENTIAL_PATH)!;
      expect(sealed).not.toContain("secret-y-token");
      expect([...staged.values()].some(value => value.includes(f.calls[0]!))).toBe(false);
      const opened = await openSupervisorSessionBootstrap({ sessionId: f.calls[0]!, expectedBinding: BINDING, sealed: JSON.parse(sealed) });
      expect(opened.instanceId).toBe(f.supervisor.instanceId);
      expect(opened.credential.rotationKey).toBe(f.hostOptions()!.rotationKey);
      expect(opened.credential.token).toBe("secret-y-token");
      expect(TRANSIENTS.some(path => f.files.data.has(path))).toBe(false);
      expect(f.files.data.has(SUPERVISOR_STATIC_PATH)).toBe(true);
      for (let i = 0; i < 2; i++) await f.supervisor.launch({ sidecarBinding: BINDING, entrypointPath: ENTRYPOINT, staticConfig: "{}" });
      expect(f.host.launches).toBe(2); expect(f.calls).toHaveLength(1);
      await f.supervisor.shutdown("test"); expect(f.host.shutdowns).toBe(1);
    } finally { await f.supervisor.close(); }
  });

  it("waits for authenticated readiness before retiring the staged credentials", async () => {
    const f = fixture(); const entered = Promise.withResolvers<void>(); const ready = Promise.withResolvers<void>();
    f.host.onConnect = async () => { entered.resolve(); await ready.promise; };
    const operation = f.supervisor.ensureReady();
    try {
      await entered.promise; expect(f.files.data.has(SUPERVISOR_CREDENTIAL_PATH)).toBe(true);
      ready.resolve(); await operation; expect(f.files.data.has(SUPERVISOR_CREDENTIAL_PATH)).toBe(false);
    } finally { ready.resolve(); await f.supervisor.close(); }
  });

  it("preserves a colliding user file and rejects an unavailable direct API without LLM fallback", async () => {
    const f = fixture(); f.files.data.set(DIRECT_SUPERVISOR_LAUNCH_PATH, "user content");
    try { await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "deploy" }); expect(f.calls).toHaveLength(0);
      expect(f.files.data.get(DIRECT_SUPERVISOR_LAUNCH_PATH)).toBe("user content"); }
    finally { await f.supervisor.close(); }
    const missing = fixture(); delete missing.gateway.startSupervisorExec;
    try { await expect(missing.supervisor.ensureReady()).rejects.toMatchObject({ kind: "exec" }); expect(missing.files.data.size).toBe(0); }
    finally { await missing.supervisor.close(); }
  });

  it("cleans its own transient files after a failed submission and never submits again", async () => {
    const f = fixture(); let starts = 0;
    f.gateway.startSupervisorExec = async () => { starts++; throw new Error("ambiguous transport failure"); };
    try {
      await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "exec" });
      await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "exec" });
      expect(starts).toBe(1); expect(TRANSIENTS.some(path => f.files.data.has(path))).toBe(false);
      expect(f.host.shutdowns).toBe(0); expect(f.host.closes).toBe(1);
    } finally { await f.supervisor.close(); }
  });

  it("joins an outstanding submission during close and prevents late readiness or another launch", async () => {
    const f = fixture(); const entered = Promise.withResolvers<void>(); const release = Promise.withResolvers<void>(); let starts = 0;
    f.gateway.startSupervisorExec = async request => { starts++; entered.resolve(); await release.promise;
      return { sessionId: request.sessionId, workspaceRoot: `/codex_workspace/${request.sessionId}`, jobId: "job-1" }; };
    const operation = f.supervisor.ensureReady().catch(error => error);
    await entered.promise; const closing = f.supervisor.close(); release.resolve(); await closing;
    expect(await operation).toMatchObject({ name: "AbortError" }); expect(starts).toBe(1); expect(f.host.connects).toBe(0);
    expect(TRANSIENTS.some(path => f.files.data.has(path))).toBe(false);
    await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "shutdown" });
  });

  it("cancels before submission without leaving an executable or credential behind", async () => {
    const f = fixture(); const entered = Promise.withResolvers<void>(); const release = Promise.withResolvers<void>(); let syncs = 0;
    f.gateway.syncWorkspace = async () => { if (++syncs === 1) { entered.resolve(); await release.promise; } };
    const operation = f.supervisor.ensureReady().catch(error => error);
    await entered.promise; const closing = f.supervisor.close(); release.resolve(); await closing;
    expect(await operation).toMatchObject({ name: "AbortError" }); expect(f.calls).toHaveLength(0);
    expect(TRANSIENTS.some(path => f.files.data.has(path))).toBe(false);
    const aborted = fixture(); const abort = new AbortController(); abort.abort();
    try { await expect(aborted.supervisor.ensureReady(abort.signal)).rejects.toMatchObject({ name: "AbortError" });
      expect(aborted.calls).toHaveLength(0); expect(aborted.files.data.size).toBe(0); }
    finally { await aborted.supervisor.close(); }
  });

  it("preserves foreign changes during cleanup and shuts down only after signed readiness", async () => {
    const f = fixture(); f.host.onConnect = async () => { f.files.data.set(DIRECT_SUPERVISOR_LAUNCH_PATH, "foreign edit"); };
    try {
      await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "deploy" });
      expect(f.host.shutdowns).toBe(1); expect(f.files.data.get(DIRECT_SUPERVISOR_LAUNCH_PATH)).toBe("foreign edit");
      expect(f.files.data.has(SUPERVISOR_CREDENTIAL_PATH)).toBe(false); expect(f.files.data.has(DIRECT_SUPERVISOR_INSTALL_PATH)).toBe(false);
    } finally { await f.supervisor.close(); }
    const failed = fixture(); failed.host.onConnect = async () => { throw new Error("no authenticated readiness"); };
    try { await expect(failed.supervisor.ensureReady()).rejects.toMatchObject({ kind: "readiness" }); expect(failed.host.shutdowns).toBe(0);
      expect(TRANSIENTS.some(path => failed.files.data.has(path))).toBe(false); }
    finally { await failed.supervisor.close(); }
  });
});

describe("direct R9 launch files", () => {
  it("accepts only the exact registered entrypoint and refuses shell/path boundary variants", () => {
    const files = directSupervisorLaunchFiles({ entrypointPath: ENTRYPOINT, supervisorNonce: BINDING.nonce });
    expect(files.installer).toContain("WORKSPACE=$(pwd -P)");
    expect(files.installer).toContain("--cached-only");
    expect(files.launcher).toContain("export CODEX_THREAD_ID=${WORKSPACE##*/}");
    expect(files.launcher).not.toContain("install.log");
    for (const entrypointPath of [`${ENTRYPOINT}\n`, `${ENTRYPOINT}/extra`, `prefix/${ENTRYPOINT}`,
      ENTRYPOINT.replace(V2_R9_RELEASE.buildHash, "a".repeat(64)), ENTRYPOINT.replace("/r9/", "/r8/"),
      `.ghostprov-v2/revisions/r9/${V2_R9_RELEASE.buildHash}/${V2_R9_RELEASE.schemaHash}/sidecar.bundle.js`]) {
      expect(() => directSupervisorLaunchFiles({ entrypointPath, supervisorNonce: BINDING.nonce })).toThrow();
    }
    for (const supervisorNonce of ["", "bad nonce", "bad;nonce", `${BINDING.nonce}\n`, "x".repeat(129)]) {
      expect(() => directSupervisorLaunchFiles({ entrypointPath: ENTRYPOINT, supervisorNonce })).toThrow();
    }
  });
});
