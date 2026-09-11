import { afterEach, describe, expect, it, mock, spyOn } from "bun:test";
import type { BootstrapBinding } from "../src/v2/bootstrap/crypto";
import * as R9Process from "../sandbox/v2/supervisor-process";
import * as R10Process from "../sandbox/v2/r10-supervisor-process";

for (const [revision, processApi] of [["r9", R9Process], ["r10", R10Process]] as const) {
const {
  canonicalSidecarEntrypoint,
  parseLinuxProcessStat,
  parseSidecarIdentityRecord,
  RetainedSidecarChild,
  sidecarIdentityRecordPath,
  sidecarWrapperScript,
  SupervisorProcessManager,
  SIDECAR_OWNER_PATH,
  SIDECAR_PID_PATH,
} = processApi;
type RetainedSidecarChild = InstanceType<typeof RetainedSidecarChild>;

const workspaceRoot = "/codex_workspace/11111111-1111-4111-8111-111111111111";
const entrypoint = `.ghostprov-v2/revisions/${revision}/${"b".repeat(64)}/${"a".repeat(64)}/sidecar.bundle.js`;
const binding: BootstrapBinding = Object.freeze({
  projectId: "11111111-1111-4111-8111-111111111111",
  sandboxSessionId: "sandbox-session-1",
  leaderGeneration: 1,
  buildHash: "a".repeat(64),
  schemaHash: "b".repeat(64),
  nonce: "launch-1",
  protocolRevision: "r2",
  schemaVersion: 2,
});
const originalDeno = Object.getOwnPropertyDescriptor(globalThis, "Deno");

afterEach(() => {
  mock.restore();
  if (originalDeno) Object.defineProperty(globalThis, "Deno", originalDeno);
  else Reflect.deleteProperty(globalThis, "Deno");
});

function processFixture(options: { noHello?: boolean; invalidIdentity?: boolean; ignoreTerm?: boolean } = {}) {
  class NotFound extends Error {}
  class AlreadyExists extends Error {}
  interface File { content: string; ino: number; symlink?: boolean }
  const files = new Map<string, File>();
  const directories = new Set<string>();
  const spawned = Promise.withResolvers<void>();
  const commands: string[] = [];
  const signals: Array<{ pid: number; signal: string }> = [];
  const children: Array<{ child: Deno.ChildProcess; exit: () => void }> = [];
  let ownerLocked = true;
  let nextInode = 1;
  const put = (path: string, content: string) => files.set(path, { content, ino: nextInode++ });
  const info = (file: File) => ({
    isFile: true, isSymlink: file.symlink ?? false, size: new TextEncoder().encode(file.content).byteLength,
    dev: 1, ino: file.ino,
  });
  const get = (path: string) => {
    if (path.startsWith("/proc/")) throw new Error("post-turn procfs inspection is forbidden");
    const file = files.get(path);
    if (!file) throw new NotFound();
    return file;
  };
  const api = {
    pid: 10,
    env: { get: (name: string) => name === "GHOSTPROV_WORKSPACE_ROOT" ? workspaceRoot
      : name === "PROXY_CA_CERT_PATH" ? "/tmp/test-ca.pem" : undefined },
    execPath: () => "/usr/local/bin/deno",
    errors: { NotFound, AlreadyExists },
    kill: () => { throw new Error("raw PID signaling is forbidden"); },
    async mkdir(path: string, opts?: { recursive?: boolean }) {
      if (!opts?.recursive && directories.has(path)) throw new AlreadyExists();
      directories.add(path);
    },
    async *readDir(path: string) {
      for (const dir of directories) {
        if (dir.startsWith(`${path}/`) && !dir.slice(path.length + 1).includes("/")) {
          yield { name: dir.slice(path.length + 1), isDirectory: true };
        }
      }
    },
    async writeTextFile(path: string, content: string, opts?: { createNew?: boolean }) {
      if (opts?.createNew && files.has(path)) throw new AlreadyExists();
      put(path, content);
    },
    async lstat(path: string) { return info(get(path)); },
    async open(path: string) {
      const file = get(path);
      let offset = 0;
      return {
        stat: async () => info(file),
        async read(output: Uint8Array) {
          const bytes = new TextEncoder().encode(file.content);
          if (offset === bytes.byteLength) return null;
          const copied = Math.min(output.byteLength, bytes.byteLength - offset);
          output.set(bytes.subarray(offset, offset + copied));
          offset += copied;
          return copied;
        },
        syncData: async () => undefined,
        tryLock: async () => !ownerLocked,
        unlock: async () => undefined,
        close: () => undefined,
      };
    },
    async rename(source: string, destination: string) { files.set(destination, get(source)); files.delete(source); },
    async chmod() {},
    async remove(path: string) { if (!files.delete(path)) throw new NotFound(); },
    Command: class {
      constructor(command: string, readonly options: { args: string[] }) {
        commands.push(command);
        if (command !== "/usr/bin/setsid") throw new Error("inspection helper subprocess is forbidden");
      }
      spawn(): Deno.ChildProcess {
        const pid = 123 + children.length;
        const nonce = this.options.args[4]!;
        const status = Promise.withResolvers<Deno.CommandStatus>();
        let exited = false;
        const exit = () => {
          exited = true;
          status.resolve({ success: true, code: 0, signal: null });
        };
        const child = {
          pid,
          status: status.promise,
          stdout: new ReadableStream<Uint8Array>({ start: controller => controller.close() }),
          stderr: new ReadableStream<Uint8Array>({ start: controller => controller.close() }),
          kill(signal: "SIGTERM" | "SIGKILL") {
            if (exited) throw new TypeError("child has already exited");
            signals.push({ pid, signal });
            if (signal === "SIGKILL" || !options.ignoreTerm) exit();
          },
        } as unknown as Deno.ChildProcess;
        children.push({ child, exit });
        put(sidecarIdentityRecordPath(nonce, pid),
          `${options.invalidIdentity ? pid + 1 : pid} ${nonce} 987654 ${pid} ${pid} ${workspaceRoot}/${entrypoint}\n`);
        put(SIDECAR_PID_PATH, `${pid} ${nonce} 987654\n`);
        put(SIDECAR_OWNER_PATH, JSON.stringify({
          version: 1, instanceId: "instance-1", nonce: "owner-1", pid, acquiredAtMs: 1000,
        }));
        if (!options.noHello) put(`/tmp/ghostprov-v2/bootstrap-hello.${nonce}.${pid}`, "GPV2_BOOTSTRAP abc123\n");
        spawned.resolve();
        return child;
      }
    },
  };
  Object.defineProperty(globalThis, "Deno", { configurable: true, value: api });
  const manager = new SupervisorProcessManager({ supervisorBinding: binding, entrypointPath: entrypoint });
  const launch = (nonce = binding.nonce) => {
    const sidecarBinding = { ...binding, nonce };
    return manager.handle({ kind: "launch", sidecarBinding, entrypointPath: entrypoint,
      staticConfig: JSON.stringify({ binding: sidecarBinding }) });
  };
  return {
    manager, launch, files, directories, put, children, signals, commands, spawned: spawned.promise,
    unlockOwner: () => { ownerLocked = false; },
  };
}

function stat(pid: number, options: { pgrp?: number; session?: number; start?: string } = {}): string {
  const fields = [
    "S",
    "1",
    String(options.pgrp ?? pid),
    String(options.session ?? pid),
    "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
    options.start ?? "987654",
    "0", "0",
  ];
  return `${pid} (deno sidecar) ${fields.join(" ")}\n`;
}

describe(`V2 ${revision} supervisor process policy`, () => {
  it("resolves sidecar identity against the canonical sandbox workspace root", () => {
    expect(canonicalSidecarEntrypoint(workspaceRoot, entrypoint)).toBe(`${workspaceRoot}/${entrypoint}`);
    expect(canonicalSidecarEntrypoint("/workspace", entrypoint)).toBeNull();
    expect(canonicalSidecarEntrypoint(workspaceRoot, `${entrypoint}.old`)).toBeNull();
    expect(canonicalSidecarEntrypoint(workspaceRoot, `${entrypoint}\n`)).toBeNull();
    expect(canonicalSidecarEntrypoint(`${workspaceRoot}/extra`, entrypoint)).toBeNull();
  });

  it("parses the kernel start tick, process group, and session exactly", () => {
    expect(parseLinuxProcessStat(stat(123), 123)).toEqual({
      pid: 123,
      state: "S",
      processGroup: 123,
      session: 123,
      startTick: "987654",
    });
    expect(parseLinuxProcessStat(stat(123), 124)).toBeNull();
    expect(parseLinuxProcessStat(stat(123, { start: "bad" }), 123)).toBeNull();
  });

  it("generates a fixed wrapper that records identity before exec", () => {
    const wrapper = sidecarWrapperScript();
    expect(wrapper).toContain('PGRP="$3"');
    expect(wrapper).toContain('SESSION="$4"');
    expect(wrapper).toContain('START="${20}"');
    expect(wrapper).toContain("sidecar.pid.$NONCE.$PID.candidate");
    expect(wrapper).toContain("sidecar-identity.$NONCE.$PID");
    expect(wrapper).toContain('IFS= read -r STAT < "/proc/$PID/stat"');
    expect(wrapper).not.toContain("$(cat");
    expect(wrapper).toContain("--ghostprov-process-start=$START");
    expect(wrapper).not.toContain("date -u");
    expect(wrapper).not.toContain("pgrep");
  });

  it("requires the wrapper record to bind the exact retained PID, group, session, nonce and entrypoint", () => {
    const expected = { pid: 123, nonce: "launch-1", entrypointPath: `${workspaceRoot}/${entrypoint}` };
    const valid = `123 launch-1 987654 123 123 ${expected.entrypointPath}\n`;
    expect(parseSidecarIdentityRecord(valid, expected)).toEqual({ pid: 123, startTick: "987654", processGroup: 123, session: 123 });
    for (const invalid of [
      valid.replace(/^123/, "124"), valid.replace("launch-1", "launch-2"),
      valid.replace("987654", "bad"), valid.replace("987654", "1".repeat(33)),
      valid.replace(" 123 123 ", " 122 123 "), valid.replace(" 123 123 ", " 123 122 "),
      valid.replace("sidecar.bundle.js", "sidecar.bundle.js.old"),
      valid.replace("\n", "\r\n"), valid + "\n", valid + "suffix", `prefix${valid}`,
      valid.replace(" 987654 ", "  987654 "),
    ]) expect(parseSidecarIdentityRecord(invalid, expected)).toBeNull();
    expect(() => sidecarIdentityRecordPath("nonce\n", 123)).toThrow();
  });

  it("rejects an entrypoint whose registered hashes are cross-paired", () => {
    processFixture();
    expect(() => new SupervisorProcessManager({ supervisorBinding: binding,
      entrypointPath: entrypoint.replace("a".repeat(64), "c".repeat(64)) })).toThrow();
  });

  it("launches, delivers credentials and stops through its retained child without procfs or helper calls", async () => {
    const fixture = processFixture();
    const launched = await fixture.launch();
    expect(launched).toMatchObject({ kind: "launch_hello", process: { pid: 123, startTick: "987654" } });
    if (launched.kind !== "launch_hello") throw new Error("launch failed");
    expect(Object.keys(launched.process).sort()).toEqual(["pid", "processGroup", "session", "startTick"]);
    await expect(fixture.manager.handle({ kind: "credential", sidecarBinding: binding, sealedJson: "sealed-test-value" }))
      .resolves.toEqual({ kind: "credential_written", launchNonce: binding.nonce });
    await expect(fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" }))
      .resolves.toEqual({ kind: "stopped", launchNonce: binding.nonce });
    expect(fixture.commands).toEqual(["/usr/bin/setsid"]);
    expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGTERM" }]);
    expect(fixture.files.has(sidecarIdentityRecordPath(binding.nonce, 123))).toBe(false);
    expect(fixture.files.has(SIDECAR_PID_PATH)).toBe(false);
    expect(fixture.files.has(`${workspaceRoot}/.ghostprov-v2/bootstrap.enc.json`)).toBe(false);
  });

  it("returns the same hello for the same live launch without executing a second child", async () => {
    const fixture = processFixture();
    expect(await fixture.launch()).toEqual(await fixture.launch());
    expect(fixture.children).toHaveLength(1);
    await fixture.manager.handle({ kind: "shutdown", reason: "test" });
  });

  if (revision === "r10") it("retries only exact retained-child cleanup after shutdown fails without reopening launch", async () => {
    const fixture = processFixture({ ignoreTerm: true });
    await fixture.launch();
    const signal = spyOn(RetainedSidecarChild.prototype, "signal").mockImplementation(() => { throw new Error("child signal unavailable"); });
    const wait = spyOn(RetainedSidecarChild.prototype, "waitForExit").mockResolvedValue(false);
    await expect(fixture.manager.handle({ kind: "shutdown", reason: "first" })).rejects.toMatchObject({ code: "stop_failed" });
    await expect(fixture.launch("forbidden-successor")).rejects.toMatchObject({ code: "not_ready" });
    expect(fixture.children).toHaveLength(1);
    signal.mockRestore(); wait.mockRestore();
    fixture.children[0]!.exit();
    await expect(fixture.manager.handle({ kind: "shutdown", reason: "exact-cleanup-retry" })).resolves.toEqual({ kind: "shutdown" });
    expect(fixture.children).toHaveLength(1);
    expect(fixture.files.has(SIDECAR_PID_PATH)).toBe(false);
    await expect(fixture.launch("still-forbidden")).rejects.toMatchObject({ code: "not_ready" });
  });

  it("keeps an ambiguous live child and refuses a replacement when identity or static authority changes", async () => {
    for (const path of [sidecarIdentityRecordPath(binding.nonce, 123), `${workspaceRoot}/.ghostprov-v2/static.json`]) {
      const fixture = processFixture();
      await fixture.launch();
      fixture.put(path, "changed");
      await expect(fixture.launch("launch-2")).rejects.toMatchObject({ code: "process_conflict" });
      await expect(fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" }))
        .rejects.toMatchObject({ code: "identity_invalid" });
      expect(fixture.children).toHaveLength(1);
      expect(fixture.signals).toHaveLength(0);
      await fixture.manager.handle({ kind: "shutdown", reason: "test" });
      expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGKILL" }]);
    }
  });

  it("cancels a rejected wrapper identity through the child resource and never retries that nonce", async () => {
    const fixture = processFixture({ invalidIdentity: true });
    await expect(fixture.launch()).rejects.toMatchObject({ code: "identity_invalid" });
    expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGKILL" }]);
    await expect(fixture.launch()).rejects.toMatchObject({ code: "process_conflict" });
    expect(fixture.children).toHaveLength(1);
    expect(fixture.files.has(sidecarIdentityRecordPath(binding.nonce, 123))).toBe(false);
  });

  it("refuses credential delivery after owner-lock loss or owner-record replacement", async () => {
    for (const lost of ["lock", "record"]) {
      const fixture = processFixture();
      await fixture.launch();
      if (lost === "lock") fixture.unlockOwner();
      else fixture.put(SIDECAR_OWNER_PATH, fixture.files.get(SIDECAR_OWNER_PATH)!.content.replace("owner-1", "owner-2"));
      await expect(fixture.manager.handle({ kind: "credential", sidecarBinding: binding, sealedJson: "sealed-test-value" }))
        .rejects.toMatchObject({ code: "process_conflict" });
      expect(fixture.files.has(`${workspaceRoot}/.ghostprov-v2/bootstrap.enc.json`)).toBe(false);
      await fixture.manager.handle({ kind: "shutdown", reason: "test" });
    }
  });

  it("does not signal a PID after the original child exits, even when stale identity files remain", async () => {
    const fixture = processFixture();
    await fixture.launch();
    fixture.children[0]!.exit();
    await fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" });
    expect(fixture.signals).toHaveLength(0);
    await fixture.launch("launch-2");
    expect(fixture.children).toHaveLength(2);
    await fixture.manager.handle({ kind: "shutdown", reason: "test" });
    expect(fixture.signals).toEqual([{ pid: 124, signal: "SIGTERM" }]);
  });

  it("rejects a stop for another launch without signaling", async () => {
    const fixture = processFixture();
    await fixture.launch();
    await expect(fixture.manager.handle({ kind: "stop", launchNonces: ["other"], reason: "test" }))
      .rejects.toMatchObject({ code: "binding_conflict" });
    expect(fixture.signals).toHaveLength(0);
    await fixture.manager.handle({ kind: "shutdown", reason: "test" });
  });

  it("revalidates retained proofs before forced termination", async () => {
    const fixture = processFixture({ ignoreTerm: true });
    await fixture.launch();
    const originalWait = RetainedSidecarChild.prototype.waitForExit;
    spyOn(RetainedSidecarChild.prototype, "waitForExit").mockImplementation(async function(this: RetainedSidecarChild, timeout) {
      if (timeout === 15_000) return false;
      return originalWait.call(this, 0);
    });
    await fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" });
    expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGTERM" }, { pid: 123, signal: "SIGKILL" }]);
  });

  it("joins an in-progress launch on shutdown and cancels its exact retained child", async () => {
    const fixture = processFixture({ noHello: true });
    const launching = fixture.launch().then(() => null, error => error);
    await fixture.spawned;
    await expect(fixture.manager.handle({ kind: "shutdown", reason: "test" })).resolves.toEqual({ kind: "shutdown" });
    expect(await launching).toMatchObject({ code: "not_ready" });
    expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGKILL" }]);
    await expect(fixture.launch("launch-2")).rejects.toMatchObject({ code: "not_ready" });
  });

  it("never adopts or signals an identity file left by another supervisor", async () => {
    const fixture = processFixture();
    fixture.put(SIDECAR_PID_PATH, `123 ${binding.nonce} 987654\n`);
    await expect(fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" }))
      .resolves.toEqual({ kind: "stopped", launchNonce: null });
    expect(fixture.signals).toHaveLength(0);
    expect(fixture.files.has(SIDECAR_PID_PATH)).toBe(true);
  });

  it("fails closed when the identity file becomes a symlink", async () => {
    const fixture = processFixture();
    await fixture.launch();
    fixture.files.get(sidecarIdentityRecordPath(binding.nonce, 123))!.symlink = true;
    await expect(fixture.manager.handle({ kind: "credential", sidecarBinding: binding, sealedJson: "sealed-test-value" }))
      .rejects.toMatchObject({ code: "process_conflict" });
    await fixture.manager.handle({ kind: "shutdown", reason: "test" });
  });

  it("does not escalate after retained identity changes during graceful stop", async () => {
    const fixture = processFixture({ ignoreTerm: true });
    await fixture.launch();
    const originalWait = RetainedSidecarChild.prototype.waitForExit;
    spyOn(RetainedSidecarChild.prototype, "waitForExit").mockImplementation(async function(this: RetainedSidecarChild, timeout) {
      if (timeout === 15_000) {
        fixture.put(sidecarIdentityRecordPath(binding.nonce, 123), "stale");
        return false;
      }
      return originalWait.call(this, 0);
    });
    await expect(fixture.manager.handle({ kind: "stop", launchNonces: [binding.nonce], reason: "test" }))
      .rejects.toMatchObject({ code: "identity_invalid" });
    expect(fixture.signals).toEqual([{ pid: 123, signal: "SIGTERM" }]);
    await fixture.manager.handle({ kind: "shutdown", reason: "test" });
  });
});

}
