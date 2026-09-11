import { diagnosticLog } from "./diagnostic-log.ts";
import { archiveChildDiagnosticFiles, denoDiagnosticStorage } from "./diagnostic-log-storage.ts";
import type { BootstrapBinding } from "../../src/v2/bootstrap/crypto.ts";
import { canonicalJson } from "../../src/v2/protocol/shared.ts";
import type {
  SupervisorCommand,
  SupervisorProcessIdentity,
  SupervisorStatus,
} from "../../src/v2/supervisor/protocol.ts";
import { SupervisorCommandError } from "./supervisor-peer.ts";
import { SIDECAR_TERM_TIMEOUT_MS, SIDECAR_KILL_TIMEOUT_MS } from "./supervisor-retirement.ts";
import {
  canonicalSandboxWorkspacePath,
  isCanonicalSandboxWorkspaceRoot,
} from "./workspace-root.ts";

export const SIDECAR_RUNTIME_ROOT = "/tmp/ghostprov-v2" as const;
export const SUPERVISOR_RUNTIME_ROOT = "/tmp/ghostprov-v2/supervisor" as const;
const SIDECAR_STATIC_RELATIVE_PATH = ".ghostprov-v2/static.json" as const;
const SIDECAR_CREDENTIAL_RELATIVE_PATH = ".ghostprov-v2/bootstrap.enc.json" as const;
export const SIDECAR_PID_PATH = "/tmp/ghostprov-v2/sidecar.pid" as const;
export const SIDECAR_OWNER_PATH = "/tmp/ghostprov-v2/sidecar.owner.lock" as const;
export const SIDECAR_WRAPPER_PATH = "/tmp/ghostprov-v2/supervisor/sidecar-wrapper.sh" as const;
export const SIDECAR_LAUNCH_STATE_PATH = "/tmp/ghostprov-v2/supervisor/sidecar-launch.state" as const;

const SAFE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$(?![\s\S])/;
const SAFE_ENTRYPOINT = /^\.ghostprov-v2\/revisions\/r10\/[a-f0-9]{64}\/[a-f0-9]{64}\/sidecar\.bundle\.js$(?![\s\S])/;
const DENO_ENV_ALLOWLIST = "OPENAI_BASE_URL,OPENAI_API_KEY,PROXY_CA_CERT_PATH,NODE_ENV,PRODUCTION,FORCE_COLOR,NO_COLOR,COLORTERM,TERM,GHOSTPROV_WORKSPACE_ROOT";
const SIDECAR_READ_SUFFIX = "/tmp/ghostprov-v2";

function currentWorkspaceRoot(): string {
  const workspace = Deno.env.get("GHOSTPROV_WORKSPACE_ROOT") ?? "";
  if (!isCanonicalSandboxWorkspaceRoot(workspace)) {
    throw new SupervisorCommandError("sidecar_environment");
  }
  return workspace;
}

export function canonicalSidecarEntrypoint(
  workspaceRoot: string,
  entrypointPath: string,
): string | null {
  if (!SAFE_ENTRYPOINT.test(entrypointPath)) return null;
  return canonicalSandboxWorkspacePath(workspaceRoot, entrypointPath);
}

function sidecarWorkspacePath(relativePath: string): string {
  const resolved = canonicalSandboxWorkspacePath(currentWorkspaceRoot(), relativePath);
  if (!resolved) throw new SupervisorCommandError("sidecar_environment");
  return resolved;
}

function sidecarReadAllowlist(): string {
  const workspace = currentWorkspaceRoot();
  return `${workspace}/.ghostprov-v2,${SIDECAR_READ_SUFFIX}`;
}
const HELLO_TIMEOUT_MS = 30_000;
const MAX_SIDECAR_ATTEMPT_CLAIMS = 64;

export interface ParsedLinuxProcessIdentity extends SupervisorProcessIdentity {
  readonly state: string;
}

interface ManagedSidecar {
  readonly binding: BootstrapBinding;
  readonly entrypointPath: string;
  readonly staticConfig: string;
  readonly identity: SupervisorProcessIdentity;
  readonly child: RetainedSidecarChild;
  readonly identityRecordPath: string;
  readonly identityRecord: string;
  readonly expectedArgv: readonly string[];
  readonly ownerRecord: string | null;
  readonly encodedHello: string | null;
  readonly diagnostic: Promise<string>;
}

/** Signals always target Deno's retained child resource, never a reusable PID. */
export class RetainedSidecarChild {
  readonly child: Deno.ChildProcess;
  readonly settled: Promise<void>;
  #state: "running" | "exited" | "unknown" = "running";

  constructor(child: Deno.ChildProcess) {
    this.child = child;
    this.settled = child.status.then(
      () => { this.#state = "exited"; },
      () => { this.#state = "unknown"; },
    );
  }

  get running(): boolean { return this.#state === "running"; }
  get exited(): boolean { return this.#state === "exited"; }

  signal(signal: "SIGTERM" | "SIGKILL"): void {
    if (!this.running) return;
    this.child.kill(signal);
  }

  async waitForExit(timeoutMs: number): Promise<boolean> {
    if (this.exited) return true;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([
        this.settled,
        new Promise<void>(resolve => { timer = setTimeout(resolve, timeoutMs); }),
      ]);
      return this.exited;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
}

export function sidecarIdentityRecordPath(nonce: string, pid: number): string {
  if (!SAFE_ID.test(nonce) || !Number.isSafeInteger(pid) || pid < 2) {
    throw new TypeError("Sidecar identity path is invalid");
  }
  return `${SUPERVISOR_RUNTIME_ROOT}/sidecar-identity.${nonce}.${pid}`;
}

export function parseSidecarIdentityRecord(
  value: string,
  expected: { readonly pid: number; readonly nonce: string; readonly entrypointPath: string },
): SupervisorProcessIdentity | null {
  if (value.length > 4096 || !Number.isSafeInteger(expected.pid) || expected.pid < 2 ||
    !SAFE_ID.test(expected.nonce)) return null;
  const fields = value.split(" ");
  if (fields.length !== 6 || !/^[0-9]{1,32}$(?![\s\S])/.test(fields[2]!)) return null;
  const pid = String(expected.pid);
  if (fields[0] !== pid || fields[1] !== expected.nonce || fields[3] !== pid ||
    fields[4] !== pid || fields[5] !== `${expected.entrypointPath}\n`) return null;
  return Object.freeze({
    pid: expected.pid,
    startTick: fields[2]!,
    processGroup: expected.pid,
    session: expected.pid,
  });
}

async function captureStream(
  stream: ReadableStream<Uint8Array>,
  maximum: number,
): Promise<string> {
  const reader = stream.getReader();
  const captured: Uint8Array[] = [];
  let capturedBytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value.byteLength === 0 || capturedBytes >= maximum) continue;
      const retained = value.subarray(0, Math.min(value.byteLength, maximum - capturedBytes));
      captured.push(retained.slice());
      capturedBytes += retained.byteLength;
    }
  } catch {
    // A process teardown may close a pipe abruptly; retained safe diagnostics remain usable.
  } finally {
    reader.releaseLock();
  }
  const output = new Uint8Array(capturedBytes);
  let offset = 0;
  for (const chunk of captured) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8").decode(output);
}

function sidecarFailureCode(value: string): ConstructorParameters<typeof SupervisorCommandError>[0] {
  for (const line of value.split(/\r?\n/).slice(-32)) {
    try {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      if (parsed.event !== "ghostprov_v2_sidecar_fatal") continue;
      if (typeof parsed.permission === "string") {
        const permission = parsed.permission;
        if (permission === "env") return "sidecar_permission_env";
        if (permission === "ffi") return "sidecar_permission_ffi";
        if (permission === "net") return "sidecar_permission_net";
        if (permission === "read") return "sidecar_permission_read";
        if (permission === "run") return "sidecar_permission_run";
        if (permission === "sys") return "sidecar_permission_sys";
        if (permission === "write") return "sidecar_permission_write";
        return "sidecar_permission";
      }
      if (parsed.phase === "config") return "sidecar_config";
      if (parsed.phase === "environment") return "sidecar_environment";
      if (parsed.phase === "ownership") return "sidecar_ownership";
      if (parsed.phase === "bootstrap") return "sidecar_bootstrap";
      if (parsed.phase === "awareness_connect") return "sidecar_awareness";
      if (parsed.phase === "storage") return "sidecar_storage";
      if (parsed.phase === "engine_start" || parsed.phase === "runtime") return "sidecar_engine";
      return "launch_failed";
    } catch {
      // Only the sidecar's one-line allowlisted fatal record is interpreted.
    }
  }
  const required = /Requires (read|write|env|net|sys|run|ffi) access/i.exec(value)?.[1]?.toLowerCase();
  if (required === "env") return "sidecar_permission_env";
  if (required === "ffi") return "sidecar_permission_ffi";
  if (required === "net") return "sidecar_permission_net";
  if (required === "read") return "sidecar_permission_read";
  if (required === "run") return "sidecar_permission_run";
  if (required === "sys") return "sidecar_permission_sys";
  if (required === "write") return "sidecar_permission_write";
  if (required) return "sidecar_permission";
  if (/Module not found|No such file|not found/i.test(value)) return "entrypoint_invalid";
  if (value.trim()) return "sidecar_wrapper";
  return "sidecar_exit";
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

async function boundedDiagnostic(
  diagnostic: Promise<string>,
  timeoutMs = 1_000,
): Promise<string> {
  return Promise.race([
    diagnostic,
    delay(timeoutMs).then(() => ""),
  ]);
}

export function parseLinuxProcessStat(
  value: string,
  expectedPid: number,
): ParsedLinuxProcessIdentity | null {
  if (!Number.isSafeInteger(expectedPid) || expectedPid < 2 || value.length > 4096) return null;
  const prefix = `${expectedPid} (`;
  if (!value.startsWith(prefix)) return null;
  const close = value.lastIndexOf(") ");
  if (close < prefix.length) return null;
  const fields = value.slice(close + 2).trim().split(/\s+/);
  if (fields.length < 20 || !/^\d+$/.test(fields[19]!)) return null;
  const processGroup = Number(fields[2]);
  const session = Number(fields[3]);
  if (!Number.isSafeInteger(processGroup) || !Number.isSafeInteger(session)) return null;
  return Object.freeze({
    pid: expectedPid,
    state: fields[0]!,
    processGroup,
    session,
    startTick: fields[19]!,
  });
}

export function sidecarWrapperScript(): string {
  return [
    "#!/bin/sh",
    "set -eu",
    'DENO="$1"',
    'ENTRYPOINT="$2"',
    'NONCE="$3"',
    "PID=$$",
    'IFS= read -r STAT < "/proc/$PID/stat"',
    'TAIL=${STAT##*) }',
    "set -- $TAIL",
    'PGRP="$3"',
    'SESSION="$4"',
    'START="${20}"',
    '[ "$PGRP" = "$PID" ] && [ "$SESSION" = "$PID" ]',
    'case "$START" in ""|*[!0-9]*) exit 73;; esac',
    'umask 077',
    'set -C',
    'printf "%s %s %s\\n" "$PID" "$NONCE" "$START" > "/tmp/ghostprov-v2/sidecar.pid.$NONCE.$PID.candidate"',
    'printf "%s %s %s %s %s %s\\n" "$PID" "$NONCE" "$START" "$PGRP" "$SESSION" "$ENTRYPOINT" > "/tmp/ghostprov-v2/supervisor/sidecar-identity.$NONCE.$PID"',
    'exec "$DENO" run --cached-only "--cert=${PROXY_CA_CERT_PATH}" '.concat("\\"),
    `  --allow-env=${DENO_ENV_ALLOWLIST} `.concat("\\"),
    `  --allow-net "--allow-read=$GHOSTPROV_WORKSPACE_ROOT/.ghostprov-v2,${SIDECAR_READ_SUFFIX}" "--allow-write=/tmp/ghostprov-v2,$GHOSTPROV_WORKSPACE_ROOT/.ghostprov-v2/diagnostics" `.concat("\\"),
    '  "$ENTRYPOINT" "--ghostprov-launch-nonce=$NONCE" "--ghostprov-process-start=$START"',
    "",
  ].join("\n");
}

async function atomicWrite(file: string, content: string): Promise<void> {
  const slash = file.lastIndexOf("/");
  const directory = slash > 0 ? file.slice(0, slash) : ".";
  await Deno.mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = `${file}.${Deno.pid}.${crypto.randomUUID()}.tmp`;
  try {
    await Deno.writeTextFile(temporary, content, { createNew: true, mode: 0o600 });
    const handle = await Deno.open(temporary, { write: true });
    try {
      await handle.syncData();
    } finally {
      handle.close();
    }
    await Deno.rename(temporary, file);
    await Deno.chmod(file, 0o600);
  } finally {
    await Deno.remove(temporary).catch(error => {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    });
  }
}

async function recordLaunchState(state: string): Promise<void> {
  diagnosticLog("supervisor").record("launch_phase", { phase: state }, true);
  if (!/^(?:validate|claim|stage|spawn|identity|hello_wait|hello_ready|hello_timeout|child_[a-z_]+)$/.test(state)) return;
  await atomicWrite(SIDECAR_LAUNCH_STATE_PATH, `${state}\n`).catch(() => undefined);
}

async function readBounded(file: string, maximum: number): Promise<string | null> {
  let handle: Deno.FsFile | null = null;
  try {
    const stat = await Deno.lstat(file);
    if (!stat.isFile || stat.isSymlink || stat.size < 1 || stat.size > maximum) return null;
    handle = await Deno.open(file, { read: true });
    const opened = await handle.stat();
    if (!opened.isFile || opened.dev !== stat.dev || opened.ino !== stat.ino) return null;
    const bytes = new Uint8Array(maximum + 1);
    let length = 0;
    while (length < bytes.byteLength) {
      const read = await handle.read(bytes.subarray(length));
      if (read === null) break;
      if (read === 0) return null;
      length += read;
    }
    return length > 0 && length <= maximum
      ? new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length))
      : null;
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) return null;
    throw error;
  } finally {
    handle?.close();
  }
}

// Installer-only: the active Codex turn may inspect the supervisor it launches.
// Managed sidecars below must use their retained child resource instead.
export async function readLinuxProcFile(
  pid: number,
  leaf: "stat" | "cmdline",
  maximum: number,
): Promise<Uint8Array | null> {
  if (!Number.isSafeInteger(pid) || pid < 2 || !Number.isSafeInteger(maximum) || maximum < 1 || maximum > 64 * 1024) {
    return null;
  }
  try {
    const result = await new Deno.Command("/usr/bin/head", {
      args: ["-c", String(maximum + 1), `/proc/${pid}/${leaf}`],
      clearEnv: true,
      signal: AbortSignal.timeout(1_000),
      stdin: "null",
      stdout: "piped",
      stderr: "null",
    }).output();
    if (!result.success || result.stdout.byteLength < 1 || result.stdout.byteLength > maximum) return null;
    return result.stdout;
  } catch {
    return null;
  }
}

function exactSidecarArgs(
  deno: string,
  entrypoint: string,
  binding: BootstrapBinding,
  identity: SupervisorProcessIdentity,
): readonly string[] {
  return Object.freeze([
    deno,
    "run",
    "--cached-only",
    `--cert=${Deno.env.get("PROXY_CA_CERT_PATH") ?? ""}`,
    `--allow-env=${DENO_ENV_ALLOWLIST}`,
    "--allow-net",
    `--allow-read=${sidecarReadAllowlist()}`,
    `--allow-write=/tmp/ghostprov-v2,${currentWorkspaceRoot()}/.ghostprov-v2/diagnostics`,
    entrypoint,
    `--ghostprov-launch-nonce=${binding.nonce}`,
    `--ghostprov-process-start=${identity.startTick}`,
  ]);
}

async function exactProcessMatches(sidecar: ManagedSidecar): Promise<boolean> {
  if (!sidecar.child.running || sidecar.child.child.pid !== sidecar.identity.pid) return false;
  const identityRecord = await readBounded(sidecar.identityRecordPath, 4096);
  if (identityRecord !== sidecar.identityRecord || !parseSidecarIdentityRecord(identityRecord, {
    pid: sidecar.child.child.pid,
    nonce: sidecar.binding.nonce,
    entrypointPath: sidecar.entrypointPath,
  })) return false;
  if (await readBounded(sidecarWorkspacePath(SIDECAR_STATIC_RELATIVE_PATH), 64 * 1024) !== sidecar.staticConfig) {
    return false;
  }
  if (await readBounded(SIDECAR_WRAPPER_PATH, 8 * 1024) !== sidecarWrapperScript()) return false;
  const pidRecord = await readBounded(SIDECAR_PID_PATH, 4096);
  if (pidRecord !== `${sidecar.identity.pid} ${sidecar.binding.nonce} ${sidecar.identity.startTick}\n`) {
    return false;
  }
  const owner = await readBounded(SIDECAR_OWNER_PATH, 4096);
  if (!owner || (sidecar.ownerRecord !== null && owner !== sidecar.ownerRecord)) return false;
  try {
    const parsed = JSON.parse(owner) as Record<string, unknown>;
    if (
      !exactOwnerKeys(parsed) ||
      parsed.version !== 1 ||
      parsed.pid !== sidecar.identity.pid ||
      typeof parsed.instanceId !== "string" || !SAFE_ID.test(parsed.instanceId) ||
      typeof parsed.nonce !== "string" || !SAFE_ID.test(parsed.nonce) ||
      typeof parsed.acquiredAtMs !== "number" || !Number.isFinite(parsed.acquiredAtMs) || parsed.acquiredAtMs < 0
    ) return false;
  } catch {
    return false;
  }
  let ownerFile: Deno.FsFile | null = null;
  try {
    ownerFile = await Deno.open(SIDECAR_OWNER_PATH, { read: true, write: true });
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
  const args = sidecar.expectedArgv;
  return sidecar.child.running && args.length === 11 &&
    args[8] === sidecar.entrypointPath &&
    args[9] === `--ghostprov-launch-nonce=${sidecar.binding.nonce}` &&
    args[10] === `--ghostprov-process-start=${sidecar.identity.startTick}`;
}

function exactOwnerKeys(value: Record<string, unknown>): boolean {
  const actual = Object.keys(value).sort();
  const expected = ["version", "instanceId", "nonce", "pid", "acquiredAtMs"].sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

async function claimLaunchNonce(nonce: string): Promise<void> {
  let count = 0;
  try {
    for await (const entry of Deno.readDir(SUPERVISOR_RUNTIME_ROOT)) {
      if (entry.isDirectory && /^sidecar-attempt\.[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(entry.name)) {
        count++;
      }
      if (count >= MAX_SIDECAR_ATTEMPT_CLAIMS) break;
    }
  } catch (error) {
    if (!(error instanceof Deno.errors.NotFound)) throw error;
  }
  if (count >= MAX_SIDECAR_ATTEMPT_CLAIMS) throw new SupervisorCommandError("launch_failed");
  try {
    await Deno.mkdir(`${SUPERVISOR_RUNTIME_ROOT}/sidecar-attempt.${nonce}`, { mode: 0o700 });
  } catch (error) {
    if (error instanceof Deno.errors.AlreadyExists) throw new SupervisorCommandError("process_conflict");
    throw error;
  }
}

function assertSidecarBinding(supervisor: BootstrapBinding, sidecar: BootstrapBinding): void {
  if (
    sidecar.protocolRevision !== "r2" ||
    sidecar.projectId !== supervisor.projectId ||
    sidecar.sandboxSessionId !== supervisor.sandboxSessionId ||
    sidecar.leaderGeneration !== supervisor.leaderGeneration ||
    sidecar.buildHash !== supervisor.buildHash ||
    sidecar.schemaHash !== supervisor.schemaHash ||
    !SAFE_ID.test(sidecar.nonce)
  ) throw new SupervisorCommandError("binding_conflict");
}

function staticBinding(staticConfig: string): unknown {
  try {
    const parsed = JSON.parse(staticConfig);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>).binding
      : null;
  } catch {
    return null;
  }
}

export class SupervisorProcessManager {
  readonly #supervisorBinding: BootstrapBinding;
  readonly #entrypointPath: string;
  #current: ManagedSidecar | null = null;
  #retained: {
    readonly child: RetainedSidecarChild;
    readonly nonce: string;
    readonly identityRecordPath: string;
  } | null = null;
  #operation: Promise<SupervisorStatus> | null = null;
  #closing = false;
  #shutdown: Promise<SupervisorStatus> | null = null;

  constructor(options: {
    readonly supervisorBinding: BootstrapBinding;
    readonly entrypointPath: string;
  }) {
    if (!SAFE_ENTRYPOINT.test(options.entrypointPath) || options.entrypointPath !==
      `.ghostprov-v2/revisions/r10/${options.supervisorBinding.schemaHash}/${options.supervisorBinding.buildHash}/sidecar.bundle.js`) {
      throw new TypeError("Supervisor entrypoint is not an exact R10 release path");
    }
    this.#supervisorBinding = Object.freeze({ ...options.supervisorBinding });
    this.#entrypointPath = canonicalSidecarEntrypoint(currentWorkspaceRoot(), options.entrypointPath)!;
  }

  handle(command: SupervisorCommand): Promise<SupervisorStatus> {
    if (command.kind === "shutdown") {
      this.#closing = true;
      if (this.#shutdown) return this.#shutdown;
      const shutdown = this.#shutdownChild().catch(error => {
        // Retain the exact child and closing fence; only cleanup may retry.
        if (this.#shutdown === shutdown) this.#shutdown = null;
        throw error;
      });
      this.#shutdown = shutdown;
      return shutdown;
    }
    if (this.#closing || this.#operation) return Promise.reject(new SupervisorCommandError("not_ready"));
    const operation = (command.kind === "launch" ? this.#launch(command)
      : command.kind === "credential" ? this.#credential(command)
      : command.kind === "stop" ? this.#stop(command.launchNonces)
      : Promise.reject<SupervisorStatus>(new SupervisorCommandError("not_ready")))
      .finally(() => { if (this.#operation === operation) this.#operation = null; });
    this.#operation = operation;
    return operation;
  }

  async #shutdownChild(): Promise<SupervisorStatus> {
    await this.#operation?.catch(() => undefined);
    if (this.#current) {
      try {
        await this.#stop([this.#current.binding.nonce]);
      } catch {
        // A corrupted file cannot authorize a PID signal. The retained child
        // resource still authorizes cancellation of this supervisor's own child.
        await this.#cancelRetained();
      }
    } else {
      await this.#cancelRetained();
    }
    return { kind: "shutdown" };
  }

  #assertOpen(): void {
    if (this.#closing) throw new SupervisorCommandError("not_ready");
  }

  async #launch(command: Extract<SupervisorCommand, { kind: "launch" }>): Promise<SupervisorStatus> {
    await recordLaunchState("validate");
    assertSidecarBinding(this.#supervisorBinding, command.sidecarBinding);
    const expectedEntrypoint = canonicalSidecarEntrypoint(currentWorkspaceRoot(), command.entrypointPath);
    if (expectedEntrypoint !== this.#entrypointPath) {
      throw new SupervisorCommandError("entrypoint_invalid");
    }
    if (canonicalJson(staticBinding(command.staticConfig)) !== canonicalJson(command.sidecarBinding)) {
      throw new SupervisorCommandError("binding_conflict");
    }
    if (this.#current) {
      if (this.#current.child.exited) {
        await this.#clearExited();
      } else if (await exactProcessMatches(this.#current)) {
        if (
          canonicalJson(this.#current.binding) === canonicalJson(command.sidecarBinding) &&
          this.#current.encodedHello
        ) {
          return {
            kind: "launch_hello",
            sidecarBinding: this.#current.binding,
            encodedHello: this.#current.encodedHello,
            process: this.#current.identity,
          };
        }
        throw new SupervisorCommandError("process_conflict");
      } else {
        throw new SupervisorCommandError("process_conflict");
      }
    }
    if (this.#retained) {
      if (!this.#retained.child.exited) throw new SupervisorCommandError("process_conflict");
      await this.#clearExited();
    }
    this.#assertOpen();

    try {
      await recordLaunchState("claim");
      await claimLaunchNonce(command.sidecarBinding.nonce);
    } catch (error) {
      if (error instanceof SupervisorCommandError) throw error;
      throw new SupervisorCommandError("process_conflict");
    }

    try {
      await recordLaunchState("stage");
      await atomicWrite(sidecarWorkspacePath(SIDECAR_STATIC_RELATIVE_PATH), command.staticConfig);
      await Deno.remove(sidecarWorkspacePath(SIDECAR_CREDENTIAL_RELATIVE_PATH)).catch(error => {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
      });
      await atomicWrite(SIDECAR_WRAPPER_PATH, sidecarWrapperScript());
      await Deno.chmod(SIDECAR_WRAPPER_PATH, 0o700);
    } catch {
      throw new SupervisorCommandError("stage_failed");
    }
    const deno = Deno.execPath();
    // All previous retained children have proved exit above. Diagnostics never
    // authorize a process action and an archive failure must not block launch.
    try { archiveChildDiagnosticFiles(denoDiagnosticStorage(), currentWorkspaceRoot()); }
    catch { diagnosticLog("supervisor").record("launch_phase", { phase: "stage", error_code: "write" }, true); }
    let child: Deno.ChildProcess;
    try {
      await recordLaunchState("spawn");
      this.#assertOpen();
      child = new Deno.Command("/usr/bin/setsid", {
        args: [
          "/bin/sh",
          SIDECAR_WRAPPER_PATH,
          deno,
          this.#entrypointPath,
          command.sidecarBinding.nonce,
        ],
        stdin: "null",
        stdout: "piped",
        stderr: "piped",
      }).spawn();
    } catch {
      throw new SupervisorCommandError("spawn_failed");
    }
    diagnosticLog("supervisor").record("launch_phase", { phase: "spawn", child_pid: child.pid }, true);
    const retained = new RetainedSidecarChild(child);
    const identityRecordPath = sidecarIdentityRecordPath(command.sidecarBinding.nonce, child.pid);
    this.#retained = { child: retained, nonce: command.sidecarBinding.nonce, identityRecordPath };
    const diagnostic = Promise.all([
      captureStream(child.stdout, 8 * 1024),
      captureStream(child.stderr, 8 * 1024),
    ]).then(parts => {
      const output = parts.join("\n");
      for (const line of output.split(/\r?\n/).slice(-32)) {
        try { const value = JSON.parse(line); if (value?.event === "ghostprov_v2_sidecar_fatal") diagnosticLog("supervisor").record("fatal", value, true); } catch {}
      }
      return output;
    }, () => "");
    try {
      const identityDeadline = Date.now() + 3_000;
      let identity: SupervisorProcessIdentity | null = null;
      let identityRecord: string | null = null;
      await recordLaunchState("identity");
      while (Date.now() < identityDeadline && retained.running) {
        this.#assertOpen();
        identityRecord = await readBounded(identityRecordPath, 4096);
        if (identityRecord !== null) {
          identity = parseSidecarIdentityRecord(identityRecord, {
            pid: child.pid,
            nonce: command.sidecarBinding.nonce,
            entrypointPath: this.#entrypointPath,
          });
          if (!identity) throw new SupervisorCommandError("identity_invalid");
          break;
        }
        await delay(20);
      }
      if (!retained.running) {
        const code = sidecarFailureCode(await boundedDiagnostic(diagnostic));
        await recordLaunchState(`child_${code}`);
        throw new SupervisorCommandError(code);
      }
      if (!identity || !identityRecord) throw new SupervisorCommandError("identity_invalid");
      const managed: ManagedSidecar = Object.freeze({
        binding: Object.freeze({ ...command.sidecarBinding }),
        entrypointPath: this.#entrypointPath,
        staticConfig: command.staticConfig,
        identity,
        child: retained,
        identityRecordPath,
        identityRecord,
        expectedArgv: exactSidecarArgs(deno, this.#entrypointPath, command.sidecarBinding, identity),
        ownerRecord: null,
        encodedHello: null,
        diagnostic,
      });
      this.#current = managed;

      const helloPath = `${SIDECAR_RUNTIME_ROOT}/bootstrap-hello.${command.sidecarBinding.nonce}.${child.pid}`;
      const deadline = Date.now() + HELLO_TIMEOUT_MS;
      await recordLaunchState("hello_wait");
      while (Date.now() < deadline) {
        this.#assertOpen();
        if (!retained.running) {
          const code = sidecarFailureCode(await boundedDiagnostic(diagnostic));
          await recordLaunchState(`child_${code}`);
          throw new SupervisorCommandError(code);
        }
        const hello = await readBounded(helloPath, 16 * 1024);
        const encoded = hello ? /^GPV2_BOOTSTRAP ([A-Za-z0-9_-]{1,8192})\n$(?![\s\S])/.exec(hello)?.[1] : null;
        if (encoded) {
          const ownerRecord = await readBounded(SIDECAR_OWNER_PATH, 4096);
          const ready = Object.freeze({ ...managed, encodedHello: encoded, ownerRecord });
          if (!ownerRecord || !await exactProcessMatches(ready)) {
            throw new SupervisorCommandError("identity_invalid");
          }
          this.#assertOpen();
          await recordLaunchState("hello_ready");
          this.#current = ready;
          return {
            kind: "launch_hello",
            sidecarBinding: ready.binding,
            encodedHello: encoded,
            process: identity,
          };
        }
        await delay(50);
      }
      await recordLaunchState("hello_timeout");
      throw new SupervisorCommandError("hello_timeout");
    } catch (error) {
      // Retain a failed child unless its exit is proved. The nonce claim is
      // permanent for this supervisor lifetime, including failed launches.
      await this.#cancelRetained().catch(() => undefined);
      throw error;
    }
  }

  async #credential(command: Extract<SupervisorCommand, { kind: "credential" }>): Promise<SupervisorStatus> {
    assertSidecarBinding(this.#supervisorBinding, command.sidecarBinding);
    const current = this.#current;
    if (
      !current ||
      canonicalJson(current.binding) !== canonicalJson(command.sidecarBinding) ||
      !await exactProcessMatches(current)
    ) throw new SupervisorCommandError("process_conflict");
    this.#assertOpen();
    await atomicWrite(sidecarWorkspacePath(SIDECAR_CREDENTIAL_RELATIVE_PATH), command.sealedJson);
    return { kind: "credential_written", launchNonce: command.sidecarBinding.nonce };
  }

  async #stop(launchNonces: readonly string[]): Promise<SupervisorStatus> {
    const current = this.#current;
    const retained = this.#retained;
    if (!retained) return { kind: "stopped", launchNonce: null };
    if (!launchNonces.includes(retained.nonce)) {
      throw new SupervisorCommandError("binding_conflict");
    }
    if (current) await this.#terminate(current);
    else await this.#cancelRetained();
    await this.#clearExited();
    await Deno.remove(sidecarWorkspacePath(SIDECAR_CREDENTIAL_RELATIVE_PATH)).catch(error => {
      if (!(error instanceof Deno.errors.NotFound)) throw error;
    });
    return { kind: "stopped", launchNonce: retained.nonce };
  }

  async #terminate(current: ManagedSidecar): Promise<void> {
    if (current.child.exited) return;
    if (!await exactProcessMatches(current)) {
      if (current.child.exited) return;
      throw new SupervisorCommandError("identity_invalid");
    }
    try {
      current.child.signal("SIGTERM");
    } catch {
      if (await current.child.waitForExit(0)) return;
      throw new SupervisorCommandError("stop_failed");
    }
    if (await current.child.waitForExit(SIDECAR_TERM_TIMEOUT_MS)) return;
    if (!await exactProcessMatches(current)) {
      if (current.child.exited) return;
      throw new SupervisorCommandError("identity_invalid");
    }
    try {
      current.child.signal("SIGKILL");
    } catch {
      if (await current.child.waitForExit(0)) return;
      throw new SupervisorCommandError("stop_failed");
    }
    if (!await current.child.waitForExit(SIDECAR_KILL_TIMEOUT_MS)) throw new SupervisorCommandError("stop_failed");
  }

  async #cancelRetained(): Promise<void> {
    const retained = this.#retained;
    if (!retained) return;
    if (!retained.child.exited) {
      try { retained.child.signal("SIGKILL"); } catch { /* Exit may have won the race. */ }
      if (!await retained.child.waitForExit(SIDECAR_KILL_TIMEOUT_MS)) throw new SupervisorCommandError("stop_failed");
    }
    await this.#clearExited();
  }

  async #clearExited(): Promise<void> {
    const retained = this.#retained;
    if (!retained) return;
    if (!retained.child.exited) throw new SupervisorCommandError("process_conflict");
    const current = this.#current;
    if (current) {
      const pidRecord = `${current.identity.pid} ${current.binding.nonce} ${current.identity.startTick}\n`;
      if (await readBounded(SIDECAR_PID_PATH, 4096) === pidRecord) {
        await Deno.remove(SIDECAR_PID_PATH);
      }
    }
    for (const file of [
      retained.identityRecordPath,
      `${SIDECAR_RUNTIME_ROOT}/sidecar.pid.${retained.nonce}.${retained.child.child.pid}.candidate`,
      `${SIDECAR_RUNTIME_ROOT}/bootstrap-hello.${retained.nonce}.${retained.child.child.pid}`,
    ]) {
      await Deno.remove(file).catch(error => {
        if (!(error instanceof Deno.errors.NotFound)) throw error;
      });
    }
    this.#current = null;
    this.#retained = null;
  }
}
