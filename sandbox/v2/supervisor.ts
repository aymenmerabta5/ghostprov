import { DenoSidecarOwnershipLease } from "./ownership-lease.ts";
import {
  SupervisorProcessManager,
  SUPERVISOR_RUNTIME_ROOT,
  parseLinuxProcessStat,
  readLinuxProcFile,
} from "./supervisor-process.ts";
import { DenoSupervisorPeer } from "./supervisor-peer.ts";
import { retireVerifiedSupervisor } from "./supervisor-retirement.ts";
import {
  createBootstrapKeyPair,
  type BootstrapBinding,
} from "../../src/v2/bootstrap/crypto.ts";
import {
  base64UrlEncode,
  canonicalJson,
  utf8Bytes,
} from "../../src/v2/protocol/shared.ts";
import {
  openSupervisorSessionBootstrap,
  type SealedSupervisorSessionBootstrap,
} from "../../src/v2/supervisor/session-bootstrap.ts";
import {
  SUPERVISOR_CREDENTIAL_PATH,
  SUPERVISOR_STATIC_PATH,
  type SupervisorStaticConfig,
} from "../../src/v2/supervisor/launcher.ts";
import {
  canonicalSandboxWorkspacePath,
  isCanonicalSandboxWorkspaceRoot,
} from "./workspace-root.ts";

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const SAFE_ENTRYPOINT = /^\.ghostprov-v2\/revisions\/r9\/[a-f0-9]{64}\/[a-f0-9]{64}\/sidecar\.bundle\.js$/;
const SUPERVISOR_PID_PATH = `${SUPERVISOR_RUNTIME_ROOT}/sidecar.pid`;
const SUPERVISOR_OWNER_PATH = `${SUPERVISOR_RUNTIME_ROOT}/sidecar.owner.lock`;
const SUPERVISOR_WRAPPER_PATH = `${SUPERVISOR_RUNTIME_ROOT}/supervisor-wrapper.sh`;
const SUPERVISOR_LIFECYCLE_PATH = `${SUPERVISOR_RUNTIME_ROOT}/lifecycle.state`;
const SUPERVISOR_RETIREMENT_PATH = `${SUPERVISOR_RUNTIME_ROOT}/retired.pid`;
const SUPERVISOR_ENV_ALLOWLIST = "OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH,NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM,GHOSTPROV_WORKSPACE_ROOT,CODEX_THREAD_ID";
const SUPERVISOR_READ_SUFFIX = "/tmp/ghostprov-v2,/proc,/usr/bin/setsid,/bin/sh,/usr/local/bin/deno,/usr/bin/deno,/root/.deno/bin/deno";
const SUPERVISOR_WRITE_SUFFIX = "/tmp/ghostprov-v2";

interface SupervisorPidRecord {
  readonly pid: number;
  readonly nonce: string;
  readonly startTick: string;
}

function workspaceRoot(): string {
  const value = Deno.env.get("GHOSTPROV_WORKSPACE_ROOT") ?? "";
  if (!isCanonicalSandboxWorkspaceRoot(value)) {
    throw new SupervisorLifecycleError("workspace_root");
  }
  return value;
}

function supervisorReadAllowlist(): string {
  return `${workspaceRoot()}/.ghostprov-v2,${SUPERVISOR_READ_SUFFIX}`;
}

function supervisorWriteAllowlist(): string {
  return `${workspaceRoot()}/.ghostprov-v2,${SUPERVISOR_WRITE_SUFFIX}`;
}

class SupervisorLifecycleError extends Error {
  readonly name = "SupervisorLifecycleError";
  constructor(readonly code: string, options?: ErrorOptions) {
    super("Supervisor lifecycle operation failed", options);
  }
}

function supervisorWorkspacePath(relativePath: string): string {
  const resolved = canonicalSandboxWorkspacePath(workspaceRoot(), relativePath);
  if (!resolved) throw new SupervisorLifecycleError("workspace_path");
  return resolved;
}

async function writeSupervisorLifecycleState(state: string): Promise<void> {
  if (!/^(?:credential_(?:wait|observed|accepted|rejected_(?:binding|key|ciphertext|payload|unknown))|awareness_connect|ready)$/.test(state)) {
    throw new SupervisorLifecycleError("lifecycle_state");
  }
  await atomicWrite(SUPERVISOR_LIFECYCLE_PATH, `${state}\n`);
}

async function stage<T>(code: string, operation: () => T | Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (cause) {
    if (cause instanceof SupervisorLifecycleError) throw cause;
    throw new SupervisorLifecycleError(code, { cause });
  }
}

function exactKeys(value: object, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  const sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}

async function readBounded(file: string, maximum: number): Promise<string | null> {
  try {
    const stat = await Deno.lstat(file);
    if (!stat.isFile || stat.isSymlink || stat.size < 1 || stat.size > maximum) return null;
    const value = await Deno.readTextFile(file);
    return value.length <= maximum ? value : null;
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) return null;
    throw error;
  }
}

async function atomicWrite(file: string, content: string, mode = 0o600): Promise<void> {
  const slash = file.lastIndexOf("/");
  const directory = slash > 0 ? file.slice(0, slash) : ".";
  await Deno.mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = `${file}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
  await Deno.writeTextFile(temporary, content, { createNew: true, mode });
  const handle = await Deno.open(temporary, { write: true });
  try {
    await handle.syncData();
  } finally {
    handle.close();
  }
  await Deno.rename(temporary, file);
  await Deno.chmod(file, mode);
}

function parseStaticConfig(value: string): SupervisorStaticConfig {
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new Error("Supervisor static config is malformed");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Supervisor static config is invalid");
  }
  const raw = parsed as Record<string, unknown>;
  if (
    !exactKeys(raw, ["version", "binding", "entrypointPath", "connectTimeoutMs"]) ||
    raw.version !== 1 ||
    !raw.binding || typeof raw.binding !== "object" || Array.isArray(raw.binding) ||
    typeof raw.entrypointPath !== "string" || !SAFE_ENTRYPOINT.test(raw.entrypointPath) ||
    !Number.isSafeInteger(raw.connectTimeoutMs) || (raw.connectTimeoutMs as number) < 1 ||
    (raw.connectTimeoutMs as number) > 300_000 ||
    canonicalJson(raw) !== value
  ) throw new Error("Supervisor static config fields are invalid");
  const binding = raw.binding as BootstrapBinding;
  if (
    binding.protocolRevision !== "r2" ||
    binding.schemaVersion !== 2 ||
    typeof binding.nonce !== "string" || !SAFE_ID.test(binding.nonce)
  ) throw new Error("Supervisor static binding is invalid");
  return Object.freeze({
    version: 1,
    binding,
    entrypointPath: raw.entrypointPath,
    connectTimeoutMs: raw.connectTimeoutMs as number,
  });
}

function entrypointPath(): string {
  const path = decodeURIComponent(new URL(import.meta.url).pathname);
  return Deno.build.os === "windows" && /^\/[A-Za-z]:/.test(path) ? path.slice(1) : path;
}

function parsePidRecord(value: string | null): SupervisorPidRecord | null {
  if (!value) return null;
  const match = /^([0-9]+) ([A-Za-z0-9][A-Za-z0-9._:-]{0,127}) ([0-9]+)\n$/.exec(value);
  if (!match) return null;
  const pid = Number(match[1]);
  if (!Number.isSafeInteger(pid) || pid < 2) return null;
  return { pid, nonce: match[2]!, startTick: match[3]! };
}

async function processArgs(pid: number): Promise<readonly string[] | null> {
  try {
    const bytes = await readLinuxProcFile(pid, "cmdline", 16 * 1024);
    if (!bytes) return null;
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes).split("\0").filter(Boolean);
  } catch {
    return null;
  }
}

async function processIdentity(pid: number) {
  const stat = await readLinuxProcFile(pid, "stat", 4096);
  return stat ? parseLinuxProcessStat(new TextDecoder().decode(stat), pid) : null;
}

async function exactSupervisorProcess(
  record: SupervisorPidRecord,
  expectedEntrypoint: string,
): Promise<boolean> {
  const identity = await processIdentity(record.pid);
  if (
    !identity ||
    ["Z", "X", "x"].includes(identity.state) ||
    identity.startTick !== record.startTick ||
    identity.processGroup !== record.pid ||
    identity.session !== record.pid
  ) return false;
  const args = await processArgs(record.pid);
  if (!args || args.length !== 13) return false;
  const owner = await readBounded(SUPERVISOR_OWNER_PATH, 4096);
  if (!owner) return false;
  try {
    const parsed = JSON.parse(owner) as Record<string, unknown>;
    if (
      !exactKeys(parsed, ["version", "instanceId", "nonce", "pid", "acquiredAtMs"]) ||
      parsed.version !== 1 || parsed.pid !== record.pid ||
      typeof parsed.instanceId !== "string" || !SAFE_ID.test(parsed.instanceId) ||
      typeof parsed.nonce !== "string" || !SAFE_ID.test(parsed.nonce)
    ) return false;
  } catch {
    return false;
  }
  let ownerFile: Deno.FsFile | null = null;
  try {
    ownerFile = await Deno.open(SUPERVISOR_OWNER_PATH, { read: true, write: true });
    const acquired = await ownerFile.tryLock(true);
    if (acquired) {
      await ownerFile.unlock().catch(() => undefined);
      return false;
    }
  } catch {
    return false;
  } finally {
    ownerFile?.close();
  }
  const cert = Deno.env.get("PROXY_CA_CERT_PATH") ?? "";
  const expected = [
    args[0],
    "run",
    "--cached-only",
    `--cert=${cert}`,
    `--allow-env=${SUPERVISOR_ENV_ALLOWLIST}`,
    "--allow-net",
    "--allow-run",
    "--allow-sys",
    `--allow-read=${supervisorReadAllowlist()}`,
    `--allow-write=${supervisorWriteAllowlist()}`,
    expectedEntrypoint,
    `--ghostprov-supervisor-nonce=${record.nonce}`,
    `--ghostprov-process-start=${record.startTick}`,
  ];
  return args.length === expected.length && args.every((value, index) => value === expected[index]);
}

function supervisorWrapperScript(): string {
  return [
    "#!/bin/sh",
    "set -eu",
    'DENO="$1"',
    'ENTRYPOINT="$2"',
    'NONCE="$3"',
    "PID=$$",
    'STAT=$(cat "/proc/$PID/stat")',
    'TAIL=${STAT##*) }',
    "set -- $TAIL",
    'PGRP="$3"',
    'SESSION="$4"',
    'START="${20}"',
    '[ "$PGRP" = "$PID" ] && [ "$SESSION" = "$PID" ] && [ -n "$START" ]',
    `mkdir -p "${SUPERVISOR_RUNTIME_ROOT}"`,
    "umask 077",
    `printf "%s %s %s\\n" "$PID" "$NONCE" "$START" > "${SUPERVISOR_RUNTIME_ROOT}/sidecar.pid.$NONCE.$PID.candidate"`,
    'exec "$DENO" run --cached-only "--cert=${PROXY_CA_CERT_PATH}" '.concat("\\"),
    `  --allow-env=${SUPERVISOR_ENV_ALLOWLIST} `.concat("\\"),
    "  --allow-net --allow-run --allow-sys ".concat("\\"),
    `  "--allow-read=$GHOSTPROV_WORKSPACE_ROOT/.ghostprov-v2,${SUPERVISOR_READ_SUFFIX}" `.concat("\\"),
    `  "--allow-write=$GHOSTPROV_WORKSPACE_ROOT/.ghostprov-v2,${SUPERVISOR_WRITE_SUFFIX}" `.concat("\\"),
    '  "$ENTRYPOINT" "--ghostprov-supervisor-nonce=$NONCE" "--ghostprov-process-start=$START"',
    "",
  ].join("\n");
}

async function supervisorRetirementProved(record: SupervisorPidRecord): Promise<boolean> {
  return await readBounded(SUPERVISOR_RETIREMENT_PATH, 4096) ===
    `${record.pid} ${record.nonce} ${record.startTick}\n`;
}

async function waitForRetirement(record: SupervisorPidRecord, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await supervisorRetirementProved(record)) return true;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  return false;
}

async function stopExistingSupervisor(
  record: SupervisorPidRecord,
  expectedEntrypoint: string,
): Promise<void> {
  await retireVerifiedSupervisor({
    isRetired: () => supervisorRetirementProved(record),
    isExactProcess: () => exactSupervisorProcess(record, expectedEntrypoint),
    terminate: () => Deno.kill(-record.pid, "SIGTERM"),
    waitForRetirement: timeoutMs => waitForRetirement(record, timeoutMs),
  });
}

function helloPath(nonce: string, pid: number): string {
  return `${SUPERVISOR_RUNTIME_ROOT}/bootstrap-hello.${nonce}.${pid}`;
}

function supervisorArgs(): { nonce: string; processStart: string } | null {
  if (Deno.args.length !== 2) return null;
  const nonce = /^--ghostprov-supervisor-nonce=([A-Za-z0-9][A-Za-z0-9._:-]{0,127})$/.exec(Deno.args[0]!)?.[1];
  const processStart = /^--ghostprov-process-start=([0-9]+)$/.exec(Deno.args[1]!)?.[1];
  return nonce && processStart ? { nonce, processStart } : null;
}

export async function runR9SupervisorInstaller(supervisorNonce: string): Promise<void> {
  if (!SAFE_ID.test(supervisorNonce) || Deno.args.length !== 1) {
    throw new Error("Supervisor installer argument is invalid");
  }
  const staticRaw = await stage("install_static_read", () => (
    Deno.readTextFile(supervisorWorkspacePath(SUPERVISOR_STATIC_PATH))
  ));
  const config = await stage("install_static_parse", () => parseStaticConfig(staticRaw));
  if (config.binding.nonce !== supervisorNonce) throw new Error("Supervisor installer binding is stale");
  const currentEntrypoint = await stage("install_entrypoint", () => entrypointPath());
  const expectedEntrypoint = `${workspaceRoot()}/${config.entrypointPath}`;
  if (currentEntrypoint !== expectedEntrypoint) throw new Error("Supervisor installer entrypoint is not immutable");

  await stage("install_state_mkdir", () => (
    Deno.mkdir(SUPERVISOR_RUNTIME_ROOT, { recursive: true, mode: 0o700 })
  ));
  const existing = await stage("install_state_read", async () => (
    parsePidRecord(await readBounded(SUPERVISOR_PID_PATH, 4096))
  ));
  if (existing) {
    if (existing.nonce === supervisorNonce && await exactSupervisorProcess(existing, expectedEntrypoint)) {
      const hello = await readBounded(helloPath(existing.nonce, existing.pid), 16 * 1024);
      if (!hello || !/^GPV2_BOOTSTRAP [A-Za-z0-9_-]+\n$/.test(hello)) {
        throw new Error("Existing supervisor has no retained bootstrap hello");
      }
      console.log("HELLO-BEGIN");
      console.log(hello.trimEnd());
      console.log("HELLO-END");
      return;
    }
    await stopExistingSupervisor(existing, expectedEntrypoint);
    const current = await readBounded(SUPERVISOR_PID_PATH, 4096);
    if (current && current === `${existing.pid} ${existing.nonce} ${existing.startTick}\n`) {
      await Deno.remove(SUPERVISOR_PID_PATH).catch(() => undefined);
    }
  }

  await stage("install_wrapper_write", () => (
    atomicWrite(SUPERVISOR_WRAPPER_PATH, supervisorWrapperScript(), 0o700)
  ));
  const child = await stage("install_process_spawn", () => new Deno.Command("/usr/bin/setsid", {
      args: ["/bin/sh", SUPERVISOR_WRAPPER_PATH, Deno.execPath(), expectedEntrypoint, supervisorNonce],
      stdin: "null",
      stdout: "null",
      stderr: "null",
    }).spawn());
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const hello = await readBounded(helloPath(supervisorNonce, child.pid), 16 * 1024);
    if (hello && /^GPV2_BOOTSTRAP [A-Za-z0-9_-]+\n$/.test(hello)) {
      console.log("HELLO-BEGIN");
      console.log(hello.trimEnd());
      console.log("HELLO-END");
      return;
    }
    const status = await Promise.race([
      child.status.then(result => ({ exited: true, success: result.success })),
      new Promise<{ exited: false }>(resolve => setTimeout(() => resolve({ exited: false }), 50)),
    ]);
    if (status.exited) throw new Error("Supervisor exited before bootstrap hello");
  }
  if (await exactSupervisorProcess(
    { pid: child.pid, nonce: supervisorNonce, startTick: parsePidRecord(
      await readBounded(`${SUPERVISOR_RUNTIME_ROOT}/sidecar.pid.${supervisorNonce}.${child.pid}.candidate`, 4096),
    )?.startTick ?? "0" },
    expectedEntrypoint,
  )) {
    try { Deno.kill(-child.pid, "SIGKILL"); } catch {}
  }
  throw new Error("Supervisor bootstrap hello timed out");
}

export async function runR9Supervisor(): Promise<void> {
  const args = supervisorArgs();
  if (!args) throw new Error("Supervisor launch arguments are invalid");
  const staticRaw = await Deno.readTextFile(supervisorWorkspacePath(SUPERVISOR_STATIC_PATH));
  const config = parseStaticConfig(staticRaw);
  if (config.binding.nonce !== args.nonce) throw new Error("Supervisor launch binding is stale");
  const currentEntrypoint = entrypointPath();
  if (currentEntrypoint !== `${workspaceRoot()}/${config.entrypointPath}`) {
    throw new Error("Supervisor launch entrypoint is not immutable");
  }
  await writeSupervisorLifecycleState("credential_wait");
  const encryptedPath = supervisorWorkspacePath(SUPERVISOR_CREDENTIAL_PATH);
  const sessionId = Deno.env.get("CODEX_THREAD_ID") ?? "";
  const sealed = JSON.parse(await Deno.readTextFile(encryptedPath)) as SealedSupervisorSessionBootstrap;
  const bootstrap = await openSupervisorSessionBootstrap({
    sessionId,
    expectedBinding: config.binding,
    sealed,
  });
  await Deno.remove(encryptedPath);
  await writeSupervisorLifecycleState("credential_accepted");
  const instanceId = bootstrap.instanceId;
  const ownership = await DenoSidecarOwnershipLease.acquire({
    instanceId,
    root: SUPERVISOR_RUNTIME_ROOT,
  });
  let peer: DenoSupervisorPeer | null = null;
  let manager: SupervisorProcessManager | null = null;
  const lifecycle = new AbortController();
  let resolveShutdown!: () => void;
  const shutdown = new Promise<void>(resolve => { resolveShutdown = resolve; });
  const onTerm = () => lifecycle.abort(new DOMException("Supervisor received SIGTERM", "AbortError"));
  const onInt = () => lifecycle.abort(new DOMException("Supervisor received SIGINT", "AbortError"));
  Deno.addSignalListener("SIGTERM", onTerm);
  Deno.addSignalListener("SIGINT", onInt);
  const record = `${Deno.pid} ${args.nonce} ${args.processStart}\n`;
  try {
    const keys = await createBootstrapKeyPair(config.binding);
    const hello = {
      version: 1,
      binding: config.binding,
      instanceId,
      publicJwk: keys.publicJwk,
    };
    await ownership.promoteProcessIdentity(args.nonce, args.processStart);
    await atomicWrite(
      helloPath(args.nonce, Deno.pid),
      `GPV2_BOOTSTRAP ${base64UrlEncode(utf8Bytes(canonicalJson(hello)))}\n`,
    );
    manager = new SupervisorProcessManager({
      supervisorBinding: config.binding,
      entrypointPath: config.entrypointPath,
    });
    peer = new DenoSupervisorPeer({
      credential: bootstrap.credential,
      binding: config.binding,
      instanceId,
      connectTimeoutMs: config.connectTimeoutMs,
      handle: command => manager!.handle(command),
      onShutdown: resolveShutdown,
    });
    await writeSupervisorLifecycleState("awareness_connect");
    await peer.connect(lifecycle.signal);
    await writeSupervisorLifecycleState("ready");
    await Promise.race([
      shutdown,
      new Promise<void>((_, reject) => {
        const onAbort = () => reject(lifecycle.signal.reason);
        lifecycle.signal.addEventListener("abort", onAbort, { once: true });
      }),
    ]);
  } finally {
    let cleanupProved = true;
    if (manager) {
      await manager.handle({ kind: "shutdown", reason: "supervisor_exit" }).catch(() => { cleanupProved = false; });
    }
    await peer?.close().catch(() => { cleanupProved = false; });
    await ownership.release().catch(() => { cleanupProved = false; });
    if (cleanupProved) await atomicWrite(SUPERVISOR_RETIREMENT_PATH, record);
    await Deno.remove(helloPath(args.nonce, Deno.pid)).catch(() => undefined);
    const current = await readBounded(SUPERVISOR_PID_PATH, 4096).catch(() => null);
    if (current === record) await Deno.remove(SUPERVISOR_PID_PATH).catch(() => undefined);
    Deno.removeSignalListener("SIGTERM", onTerm);
    Deno.removeSignalListener("SIGINT", onInt);
  }
}
