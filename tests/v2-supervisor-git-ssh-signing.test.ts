import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { GitSshSigningBootstrap, makeGitSshSigningPlan, parseGitSshSigningResult } from "../src/v2/supervisor/git-ssh-signing";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import type { V2BootstrapSupervisor, V2SupervisorHostOptions } from "../src/v2/supervisor/host";
import type { GuardedYDocMutation } from "../src/sandbox/ydoc";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { createCredentialRotationKey, type BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { directSupervisorLaunchFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { supervisorStaticConfig } from "../src/v2/supervisor/r10-launcher";

const SESSION = "11111111-1111-4111-8111-111111111111";
const OTHER = "22222222-2222-4222-8222-222222222222";
const assets = await loadV2SidecarAssets({ revision: "r10" });
const BINDING: BootstrapBinding = { projectId: SESSION, sandboxSessionId: "offline-sandbox",
  nonce: "offline-owned-supervisor", leaderGeneration: 1, buildHash: assets.buildHash, schemaHash: assets.schemaHash, protocolRevision: "r2", schemaVersion: 2 };
const ySweet = parseYSweetEnvelope({ url: "wss://offline.invalid/ws", baseUrl: "https://offline.invalid/", docId: SESSION,
  token: "synthetic-offline-token", authorization: "full" });

async function fixture(options: { original?: boolean; lockConflict?: boolean; backupDenied?: boolean; ambiguousTrigger?: boolean; badReceipt?: boolean; cleanupInitDenied?: boolean; controller?: boolean } = {}) {
  const files = new Map<string, string>();
  const directories = new Set<string>();
  let expectedSession = SESSION;
  let filesystemMode = false;
  let workspaceRemoved = false;
  const calls: { operation: string; target?: string; body?: unknown }[] = [];
  if (options.original !== false) files.set("/home/sandbox/.gitconfig", "original\n");
  if (options.lockConflict) files.set("/home/sandbox/.gitconfig.lock", "foreign-lock");
  const launcher = directSupervisorLaunchFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: BINDING.nonce });
  const staged = new Map([
    ["exec", launcher.launcher], [".ghostprov-v2/direct-exec/install.sh", launcher.installer],
    [".ghostprov-v2/supervisor/static.json", supervisorStaticConfig({ binding: BINDING, entrypointPath: assets.entrypointPath })],
    [".ghostprov-v2/supervisor/bootstrap.enc.json", JSON.stringify(await sealSupervisorSessionBootstrap({ sessionId: SESSION, binding: BINDING,
      payload: { instanceId: "offline-owned-instance", credential: { url: ySweet.url, baseUrl: ySweet.baseUrl, docId: ySweet.docId,
        token: ySweet.token.reveal(), authorization: ySweet.authorization, rotationKey: createCredentialRotationKey() } } }))],
  ]);
  if (options.controller) staged.clear();
  const resolve = (filename: string) => path.posix.resolve("/workspace", filename);
  const gateway = {
    async postJson(operation: string, raw: unknown, signal?: AbortSignal) {
      signal?.throwIfAborted();
      const body = raw as Record<string, string>;
      calls.push({ operation, body });
      if (operation === "init" && options.cleanupInitDenied) return new Response(null, { status: 500 });
      if (operation === "init") filesystemMode = true;
      if (operation === "token") filesystemMode = false;
      if (["write-directory", "rename-file", "delete-file"].includes(operation) && !filesystemMode) return new Response(null, { status: 500 });
      if (operation === "delete-file" && body.filename === `../codex_workspace/${expectedSession}`) workspaceRemoved = true;
      if (operation === "write-directory") {
        const target = resolve(body.filename!);
        if (files.has(target)) return new Response(null, { status: 500 });
        directories.add(target);
      } else if (operation === "rename-file") {
        const from = resolve(body.oldPath!);
        const to = resolve(body.newPath!);
        if (from === "/home/sandbox/.gitconfig" && options.backupDenied) return new Response(null, { status: 500 });
        if (directories.has(from)) {
          if (files.has(to) || directories.has(to)) return new Response(null, { status: 500 });
          for (const key of [...directories]) if (key === from || key.startsWith(from + "/")) { directories.delete(key); directories.add(to + key.slice(from.length)); }
          for (const [key, value] of [...files]) if (key.startsWith(from + "/")) { files.delete(key); files.set(to + key.slice(from.length), value); }
        } else if (files.has(from)) {
          files.set(to, files.get(from)!); files.delete(from);
        } else return new Response(null, { status: 500 });
      } else if (operation === "token" && options.ambiguousTrigger) {
        throw new Error("Synthetic ambiguous trigger");
      } else if (operation === "token" && options.controller) {
        const backup = `/codex_workspace/${expectedSession}/.ghostprov-v2/ssh-bootstrap/original.gitconfig`;
        if (files.has(backup)) { files.set("/home/sandbox/.gitconfig", files.get(backup)!); files.delete(backup); }
        else files.delete("/home/sandbox/.gitconfig");
        files.delete("/home/sandbox/.gitconfig.lock/owner");
        directories.delete("/home/sandbox/.gitconfig.lock");
      }
      return new Response(null, { status: 200 });
    },
    async fetchText(suffix: string, init: RequestInit = {}) {
      init.signal?.throwIfAborted();
      const url = new URL(suffix, "https://offline.invalid/");
      calls.push({ operation: url.pathname, target: url.searchParams.get("filename") ?? undefined });
      if (url.pathname === "/write-file") {
        if (!filesystemMode) return { status: 500, text: "", headers: new Headers() };
        files.set(resolve(url.searchParams.get("filename")!), String(init.body));
        if (url.searchParams.get("filename")!.endsWith("/ssh-bootstrap/plan.json")) expectedSession = JSON.parse(String(init.body)).sessionId;
        return { status: 200, text: "", headers: new Headers() };
      }
      if (workspaceRemoved) return { status: 404, text: "", headers: new Headers() };
      const receipt = { version: 1, sessionId: options.badReceipt ? OTHER : expectedSession, outcome: "submitted",
        configRestored: true, lockReleased: true, signingComplete: true, launchSubmitted: true, keysRemoved: true };
      return { status: 200, text: JSON.stringify({ body_b64: Buffer.from(JSON.stringify(receipt)).toString("base64") }), headers: new Headers() };
    },
    async syncWorkspace() {}, async triggerRender() { throw new Error("Forbidden alternative launcher"); },
  };
  const ydoc = { async readPath(key: string) { return staged.get(key) ?? null; },
    async writePath(key: string, content: string) { staged.set(key, content); }, async tombstonePath(key: string) { return staged.delete(key); },
    async compareAndMutatePaths(input: GuardedYDocMutation) {
      if (input.guards.some(guard => (staged.get(guard.path) ?? null) !== guard.equals)) return false;
      for (const mutation of input.mutations) {
        if (mutation.kind === "write") staged.set(mutation.path, mutation.content);
        else staged.delete(mutation.path);
      }
      return true;
    }, async renderSyncState() { return { clientStateVector: "", clientDeleteSetUpdate: "" }; } };
  return { bootstrap: new GitSshSigningBootstrap(), input: { assets, binding: BINDING, ySweet, gateway, ydoc }, files, calls, staged };
}

describe("Git SSH signing control contract (offline fakes)", () => {
  it("stages the frozen release, acquires a lock, preserves original config, and submits once", async () => {
    const f = await fixture();
    await expect(f.bootstrap.start(f.input, SESSION)).resolves.toMatchObject({ sessionId: SESSION });
    expect(f.files.get(`/codex_workspace/${SESSION}/.ghostprov-v2/ssh-bootstrap/original.gitconfig`)).toBe("original\n");
    expect(f.calls.filter(call => call.operation === "token")).toHaveLength(1);
    const count = f.calls.length;
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow("never resubmitted");
    expect(f.calls).toHaveLength(count);
  });

  it("proves absence after a failed backup operation instead of overwriting an existing file", async () => {
    const missing = await fixture({ original: false });
    await expect(missing.bootstrap.start(missing.input, SESSION)).resolves.toMatchObject({ sessionId: SESSION });
    const denied = await fixture({ backupDenied: true });
    await expect(denied.bootstrap.start(denied.input, SESSION)).rejects.toThrow();
    expect(denied.files.get("/home/sandbox/.gitconfig")).toBe("original\n");
    expect(denied.calls.some(call => call.operation === "token")).toBe(false);
  });

  it("preserves an existing Git lock and never triggers startup", async () => {
    const f = await fixture({ lockConflict: true });
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow();
    expect(f.files.get("/home/sandbox/.gitconfig.lock")).toBe("foreign-lock");
    expect(f.files.get("/home/sandbox/.gitconfig")).toBe("original\n");
    expect(f.calls.some(call => call.operation === "token")).toBe(false);
    expect(f.calls.filter(call => call.operation === "delete-file").map(call => call.body)).toEqual([
      { filename: `../codex_workspace/${SESSION}` },
    ]);
  });

  it("sends nothing for a cancelled operation or a mismatched sealed session", async () => {
    const f = await fixture();
    const abort = new AbortController(); abort.abort();
    await expect(f.bootstrap.start(f.input, SESSION, abort.signal)).rejects.toThrow();
    expect(f.calls).toHaveLength(0);
    const mismatch = await fixture();
    await expect(mismatch.bootstrap.start(mismatch.input, OTHER)).rejects.toThrow();
    expect(mismatch.calls).toHaveLength(0);
  });

  it("does not retry after an ambiguous trigger failure", async () => {
    const f = await fixture({ ambiguousTrigger: true });
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow();
    const count = f.calls.length;
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow();
    expect(f.calls).toHaveLength(count);
    expect(f.calls.filter(call => call.operation === "token")).toHaveLength(1);
    expect(f.bootstrap.result).toBeNull();
  });

  it("rejects a receipt for a different execution session", async () => {
    const f = await fixture({ badReceipt: true });
    await expect(f.bootstrap.start(f.input, SESSION)).rejects.toThrow("Invalid Git SSH signing receipt");
    expect(f.bootstrap.result).toBeNull();
  });

  it("requires signed cleanup and never sends a global-config deletion", async () => {
    const f = await fixture();
    await f.bootstrap.start(f.input, SESSION);
    const count = f.calls.length;
    await expect(f.bootstrap.cleanupAfterSignedShutdown(false, new AbortController().signal)).rejects.toThrow();
    expect(f.calls).toHaveLength(count);
    await f.bootstrap.cleanupAfterSignedShutdown(true, new AbortController().signal);
    expect(f.calls.filter(call => call.operation === "delete-file").map(call => call.body)).toEqual([
      { filename: `../codex_workspace/${SESSION}` },
    ]);
  });

  it("preserves the workspace if its receipt identity changes before cleanup", async () => {
    const options = { badReceipt: false };
    const f = await fixture(options);
    await f.bootstrap.start(f.input, SESSION);
    options.badReceipt = true;
    await expect(f.bootstrap.cleanupAfterSignedShutdown(true, new AbortController().signal)).rejects.toThrow();
    expect(f.calls.some(call => call.operation === "delete-file")).toBe(false);
    expect(f.calls.filter(call => call.operation === "init")).toHaveLength(1);
  });

  it("cannot delete or retry cleanup when the filesystem-mode transition fails", async () => {
    const options = { cleanupInitDenied: false };
    const f = await fixture(options);
    await f.bootstrap.start(f.input, SESSION);
    options.cleanupInitDenied = true;
    await expect(f.bootstrap.cleanupAfterSignedShutdown(true, new AbortController().signal)).rejects.toMatchObject({ operation: "init", status: 500 });
    await expect(f.bootstrap.cleanupAfterSignedShutdown(true, new AbortController().signal)).rejects.toThrow();
    expect(f.calls.filter(call => call.operation === "init")).toHaveLength(2);
    expect(f.calls.some(call => call.operation === "delete-file")).toBe(false);
  });
});

class Host implements V2BootstrapSupervisor {
  readonly binding = BINDING;
  readonly instanceId: string;
  connects = 0; launches = 0; shutdowns = 0; closes = 0;
  onConnect: (signal?: AbortSignal) => Promise<void> = async () => {};
  onShutdown: () => Promise<void> = async () => {};
  constructor(options: V2SupervisorHostOptions) { this.instanceId = options.instanceId; }
  async connect(signal?: AbortSignal) { this.connects++; await this.onConnect(signal); }
  async ensureReady(signal?: AbortSignal) { await this.connect(signal); }
  async launch() { this.launches++; return { encodedHello: "hello", process: { pid: 4, startTick: "5", processGroup: 4, session: 4 } }; }
  async deliverCredential() {}
  async rotateCredential() {}
  async stop() { return null; }
  async shutdown() { this.shutdowns++; await this.onShutdown(); }
  async close() { this.closes++; }
}

async function controllerFixture() {
  const f = await fixture({ controller: true });
  const hosts: Host[] = [];
  const supervisor = new V2SandboxSupervisor({ ...f.input, bootstrapMode: "git-ssh-signing",
    hostFactory: options => { const host = new Host(options); hosts.push(host); return host; } });
  return { ...f, supervisor, hosts };
}

describe("Git SSH signing supervisor integration", () => {
  it("selects the fourth mode, authenticates once, retains the host for recovery, and cleans only after signed shutdown", async () => {
    const f = await controllerFixture();
    try {
      await Promise.all([f.supervisor.ensureReady(), f.supervisor.ensureReady()]);
      expect(f.hosts).toHaveLength(1);
      expect(f.hosts[0]!.connects).toBe(1);
      expect(f.supervisor.instanceId).toBe(f.hosts[0]!.instanceId);
      expect(f.files.get("/home/sandbox/.gitconfig")).toBe("original\n");
      expect(f.staged.has("exec")).toBe(false);
      expect(f.staged.has(".ghostprov-v2/supervisor/bootstrap.enc.json")).toBe(false);
      for (let index = 0; index < 2; index++) await f.supervisor.launch({ sidecarBinding: BINDING, entrypointPath: assets.entrypointPath, staticConfig: "{}" });
      expect(f.hosts[0]!.launches).toBe(2);
      expect(f.calls.filter(call => call.operation === "token")).toHaveLength(1);
      expect(f.calls.filter(call => call.operation === "delete-file")).toHaveLength(0);
      await Promise.all([f.supervisor.shutdown("test"), f.supervisor.shutdown("test")]);
      expect(f.hosts[0]!.shutdowns).toBe(1);
      const removals = f.calls.filter(call => call.operation === "delete-file");
      expect(removals).toHaveLength(1);
      expect((removals[0]!.body as { filename: string }).filename).toMatch(/^\.\.\/codex_workspace\/[0-9a-f-]{36}$/);
      expect(f.files.get("/home/sandbox/.gitconfig")).toBe("original\n");
    } finally { await f.supervisor.close(); }
  });

  it("reports unproven cleanup after failed readiness without replay or deletion", async () => {
    const f = await fixture({ controller: true });
    const hosts: Host[] = [];
    const supervisor = new V2SandboxSupervisor({ ...f.input, bootstrapMode: "git-ssh-signing", hostFactory: options => {
      const host = new Host(options); host.onConnect = async () => { throw new Error("Synthetic readiness failure"); }; hosts.push(host); return host;
    } });
    try {
      await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "readiness" });
      const calls = f.calls.length;
      await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "readiness" });
      expect(f.calls).toHaveLength(calls);
      await expect(supervisor.shutdown("failed-start")).rejects.toMatchObject({ kind: "shutdown", remoteCode: "bootstrap_cleanup_unproven" });
      expect(f.calls).toHaveLength(calls);
      expect(hosts[0]!.shutdowns).toBe(0);
      expect(f.calls.some(call => call.operation === "delete-file")).toBe(false);
      expect(supervisor.instanceId).toBe("pending");
    } finally { await supervisor.close(); }
  });

  it("preserves the private workspace when signed shutdown fails", async () => {
    const f = await controllerFixture();
    try {
      await f.supervisor.ensureReady();
      f.hosts[0]!.onShutdown = async () => { throw new Error("Synthetic missing shutdown acknowledgement"); };
      await expect(f.supervisor.shutdown("test")).rejects.toThrow();
      expect(f.calls.some(call => call.operation === "delete-file")).toBe(false);
    } finally { await f.supervisor.close(); }
  });

  it("preserves a colliding project exec file before any filesystem mutation", async () => {
    const f = await controllerFixture();
    f.staged.set("exec", "foreign exec");
    try {
      await expect(f.supervisor.ensureReady()).rejects.toMatchObject({ kind: "deploy" });
      expect(f.staged.get("exec")).toBe("foreign exec");
      expect(f.calls).toHaveLength(0);
    } finally { await f.supervisor.close(); }
  });

  it("joins cancellation during authentication without relaunch or unproved deletion", async () => {
    const f = await fixture({ controller: true });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const supervisor = new V2SandboxSupervisor({ ...f.input, bootstrapMode: "git-ssh-signing", hostFactory: options => {
      const host = new Host(options);
      host.onConnect = async signal => { entered.resolve(); await release.promise; signal?.throwIfAborted(); };
      return host;
    } });
    const start = supervisor.ensureReady();
    const outcome = start.catch(error => error);
    await entered.promise;
    const closing = supervisor.close();
    release.resolve();
    await closing;
    expect((await outcome).name).toBe("AbortError");
    expect(f.calls.filter(call => call.operation === "token")).toHaveLength(1);
    expect(f.calls.some(call => call.operation === "delete-file")).toBe(false);
    await expect(supervisor.ensureReady()).rejects.toMatchObject({ kind: "shutdown" });
  });

  it("rejects an R9 release before constructing a Git launcher", async () => {
    const f = await fixture({ controller: true });
    const r9 = await loadV2SidecarAssets({ revision: "r9" });
    expect(() => new V2SandboxSupervisor({ ...f.input, assets: r9, bootstrapMode: "git-ssh-signing" })).toThrow("requires the registered R10 release");
    expect(f.calls).toHaveLength(0);
  });
});

describe("Git bootstrap identity and receipt boundaries", () => {
  it("accepts only exact private execution identities", () => {
    expect(makeGitSshSigningPlan(SESSION, "#!/bin/sh\nexit 0\n", 100).workspace).toBe(`/codex_workspace/${SESSION}`);
    for (const value of ["", `${SESSION}\n`, `${SESSION}/extra`, `../${SESSION}`, "x".repeat(36)]) expect(() => makeGitSshSigningPlan(value, "#!/bin/sh\nexit 0\n", 100)).toThrow();
  });

  it("requires exact receipt shape, identity, and configuration restoration", () => {
    const receipt = { version: 1, sessionId: SESSION, outcome: "submitted", configRestored: true, lockReleased: true, signingComplete: true, launchSubmitted: true, keysRemoved: true };
    expect(parseGitSshSigningResult(receipt, SESSION).outcome).toBe("submitted");
    for (const value of [null, [], {}, { ...receipt, sessionId: OTHER }, { ...receipt, sessionId: `${SESSION}\n` },
      { ...receipt, configRestored: false }, { ...receipt, lockReleased: false }, { ...receipt, launchSubmitted: false },
      { ...receipt, signingComplete: false }, { ...receipt, keysRemoved: false },
      { ...receipt, outcome: "expired" }, { ...receipt, extra: true }]) expect(() => parseGitSshSigningResult(value, SESSION)).toThrow();
  });
});
