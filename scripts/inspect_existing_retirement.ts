import { copyFile, lstat, mkdir, mkdtemp, open, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { PrismSessionManager } from "../src/prism/session-manager";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { projectId } from "../src/sandbox/types";
import { FileSupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { openSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import { supervisorCleanupFiles } from "../src/v2/supervisor/r10-direct-launcher";
import { loadV2SidecarRecoveryAssets } from "../src/v2/sidecar/assets";
import { canonicalJson, sha256Hex } from "../src/v2/protocol/shared";
import { buildRetirementPreflightScript, createPreflightAuthority, decodeRetirementPreflightFile,
  preflightResultPath, summarizeRetirementPreflightReadback, type PreflightResult } from "./lib/retirement-preflight-proof";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;
const MAX_TAIL = 1024 * 1024;
class InspectionFailure extends Error {
  readonly name = "InspectionFailure";
  constructor(readonly code: string) { super(code); }
}

export function describeExistingInspectionError(error: unknown) {
  const value = error !== null && typeof error === "object" ? error as Record<string, unknown> : {};
  const names = ["Error", "TypeError", "RangeError", "SyntaxError", "AbortError", "TimeoutError", "DOMException", "InspectionFailure",
    "FileLeaseBusyError", "FileLeaseOwnershipError", "FileLeaseRecordError", "YDocSessionError", "PrismSessionError", "PrismProjectError",
    "ProvisioningError", "SandboxGatewayError", "BootstrapCryptoError", "CookieFileReadError"];
  const kinds = ["auth", "sync", "missing", "stopped", "http", "protocol", "reprovision", "rate_limit", "timeout", "transport",
    "auth_required", "guest_identity_lost", "mode_mismatch", "binding", "ciphertext", "key", "payload", "conflict"];
  const codes = ["ENOENT", "EACCES", "EPERM", "ELOOP", "ENOTDIR", "ETIMEDOUT", "ECONNRESET", "ECONNREFUSED", "ABORT_ERR"];
  return { name: names.find(name => name === value.name) ?? null, kind: kinds.find(kind => kind === value.kind) ?? null,
    code: codes.find(code => code === value.code) ?? null,
    status: Number.isSafeInteger(value.status) && (value.status as number) >= 100 && (value.status as number) <= 599 ? value.status as number : null };
}
function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}
function boundedInteger(value: unknown, maximum = 1_000_000): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0 && (value as number) <= maximum;
}
function requiredPath(env: Record<string, string | undefined>, key: string): string {
  const value = env[key]?.trim();
  if (!value) throw new InspectionFailure("explicit_paths_required");
  return path.resolve(value);
}

export function existingRetirementOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_EXISTING_RETIREMENT_INSPECTION !== "1") throw new InspectionFailure("explicit_existing_inspection_guard_required");
  if (env.PORT !== "18888") throw new InspectionFailure("candidate_port_must_be_18888");
  const projects = (env.GP_LIVE_EXISTING_RETIREMENT_PROJECT_IDS ?? "").split(",").map(value => value.trim());
  if (projects.length < 1 || projects.length > 2 || projects.some(value => !UUID.test(value)) || new Set(projects).size !== projects.length) {
    throw new InspectionFailure("one_or_two_exact_project_ids_required");
  }
  const options = { projects, manifestFile: requiredPath(env, "GP_LIVE_EXISTING_RETIREMENT_MANIFEST_FILE"),
    sampleFile: requiredPath(env, "GP_LIVE_EXISTING_RETIREMENT_FLEET_FILE"),
    cookieFile: requiredPath(env, "PRISM_COOKIE_FILE"), origin: env.PRISM_ORIGIN ?? "https://prism.openai.com" };
  if (new Set([options.manifestFile, options.sampleFile, options.cookieFile].map(value => value.toLowerCase())).size !== 3 ||
    ![".json", ".jsonl"].includes(path.extname(options.sampleFile).toLowerCase())) throw new InspectionFailure("inspection_paths_invalid");
  return options;
}

interface RuntimeSample {
  projectId: string; state: string; capacity: number; available: number; active: number;
  queueDepth: number; quarantined: number; ambiguousQuarantined: number;
}
interface FleetSample { version: 1; capturedAtMs: number; sampleTs: string; desired: number; runtimes: RuntimeSample[] }

/** The exact latest sample is authoritative; a newer unsafe sample never falls back to an older safe one. */
export function validateExistingFleetSample(value: unknown, projects: readonly string[], now: number): FleetSample {
  const raw = record(value);
  const sampleMs = typeof raw?.sampleTs === "string" ? Date.parse(raw.sampleTs) : NaN;
  if (!raw || raw.version !== 1 || !boundedInteger(raw.capturedAtMs, Number.MAX_SAFE_INTEGER) || !Number.isFinite(sampleMs) ||
    raw.capturedAtMs > now + 1000 || sampleMs > now + 1000 || now - raw.capturedAtMs > 15_000 || now - sampleMs > 15_000 ||
    !boundedInteger(raw.desired, 128) || raw.desired < 1 || !Array.isArray(raw.runtimes) || raw.runtimes.length > 128) {
    throw new InspectionFailure("fleet_sample_invalid_or_stale");
  }
  const seen = new Set<string>();
  const runtimes = raw.runtimes.map(value => {
    const runtime = record(value);
    if (!runtime || typeof runtime.projectId !== "string" || !UUID.test(runtime.projectId) || seen.has(runtime.projectId) ||
      typeof runtime.state !== "string" || !/^[a-z_]{1,64}$/.test(runtime.state) ||
      ["capacity", "available", "active", "queueDepth", "quarantined", "ambiguousQuarantined"].some(key => !boundedInteger(runtime[key]))) {
      throw new InspectionFailure("fleet_runtime_sample_invalid");
    }
    seen.add(runtime.projectId);
    return { projectId: runtime.projectId, state: runtime.state, capacity: runtime.capacity as number, available: runtime.available as number,
      active: runtime.active as number, queueDepth: runtime.queueDepth as number, quarantined: runtime.quarantined as number,
      ambiguousQuarantined: runtime.ambiguousQuarantined as number };
  });
  for (const id of projects) {
    const runtime = runtimes.find(value => value.projectId === id);
    if (!runtime || runtime.state === "ready" || runtime.state === "draining" || runtime.available !== 0 || runtime.active !== 0 ||
      runtime.queueDepth !== 0 || runtime.quarantined !== 0 || runtime.ambiguousQuarantined !== 0) {
      throw new InspectionFailure("target_has_work_or_is_routable");
    }
  }
  return { version: 1, capturedAtMs: raw.capturedAtMs, sampleTs: raw.sampleTs as string, desired: raw.desired, runtimes };
}

export function latestFleetSampleFromCompleteTail(text: string, projects: readonly string[], now: number): FleetSample {
  if (Buffer.byteLength(text) > MAX_TAIL) throw new InspectionFailure("fleet_tail_byte_bound");
  const last = text.lastIndexOf("\n");
  if (last < 0) throw new InspectionFailure("fleet_sample_missing");
  const lines = text.slice(0, last).split("\n");
  for (let index = lines.length - 1; index >= 0; index--) {
    const line = lines[index]!;
    if (!line.includes('"fleet.sample"')) continue;
    let parsed: unknown;
    try { parsed = JSON.parse(line); } catch { throw new InspectionFailure("latest_fleet_sample_malformed"); }
    const event = record(parsed);
    if (event?.event !== "fleet.sample") continue;
    if (!Array.isArray(event.runtimes)) throw new InspectionFailure("fleet_runtime_sample_invalid");
    const runtimes = event.runtimes.map(value => {
      const runtime = record(value);
      return { projectId: runtime?.project_id, state: runtime?.state, capacity: runtime?.capacity, available: runtime?.available,
        active: runtime?.active, queueDepth: runtime?.queue_depth, quarantined: runtime?.quarantined,
        ambiguousQuarantined: runtime?.ambiguous_quarantined };
    });
    return validateExistingFleetSample({ version: 1, capturedAtMs: now, sampleTs: event.ts, desired: event.desired, runtimes }, projects, now);
  }
  throw new InspectionFailure("fleet_sample_missing");
}

async function readBoundedFile(file: string, maximum: number, tail = false): Promise<Buffer> {
  const before = await lstat(file);
  if (!before.isFile() || before.isSymbolicLink() || (!tail && before.size > maximum)) throw new InspectionFailure("local_file_bound_or_type");
  const handle = await open(file, "r");
  try {
    const opened = await handle.stat();
    if (opened.dev !== before.dev || opened.ino !== before.ino || (!tail && opened.size > maximum)) throw new InspectionFailure("local_file_changed");
    const start = tail ? Math.max(0, opened.size - maximum) : 0;
    const buffer = Buffer.alloc(Math.min(maximum, opened.size - start));
    let count = 0;
    while (count < buffer.length) {
      const value = await handle.read(buffer, count, buffer.length - count, start + count);
      if (value.bytesRead === 0) throw new InspectionFailure("local_file_truncated");
      count += value.bytesRead;
    }
    const after = await handle.stat(), named = await lstat(file);
    if (after.size < opened.size || named.dev !== opened.dev || named.ino !== opened.ino ||
      (!tail && (after.size !== opened.size || after.mtimeMs !== opened.mtimeMs))) throw new InspectionFailure("local_file_changed");
    if (start === 0) return buffer;
    const first = buffer.indexOf(10);
    if (first < 0) throw new InspectionFailure("fleet_tail_has_no_complete_line");
    return buffer.subarray(first + 1);
  } finally { await handle.close(); }
}

async function sample(options: ReturnType<typeof existingRetirementOptions>): Promise<FleetSample> {
  const jsonl = path.extname(options.sampleFile).toLowerCase() === ".jsonl";
  const bytes = await readBoundedFile(options.sampleFile, MAX_TAIL, jsonl);
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  return jsonl ? latestFleetSampleFromCompleteTail(text, options.projects, Date.now())
    : validateExistingFleetSample(JSON.parse(text), options.projects, Date.now());
}

interface Worker { index: number; projectId: string; title: string; state: string; leaderGeneration: number; managed: boolean }
async function manifestWorkers(file: string, fingerprint: string, projects: readonly string[]): Promise<Worker[]> {
  const raw = record(JSON.parse((await readBoundedFile(file, 256 * 1024)).toString("utf8")));
  if (!raw || raw.version !== 1 || raw.identityKind !== "authenticated" || raw.identityFingerprint !== fingerprint ||
    !Array.isArray(raw.workers) || raw.workers.length > 128) throw new InspectionFailure("manifest_identity_invalid");
  const ids = new Set<string>(), indices = new Set<number>();
  const workers = raw.workers.map(value => {
    const worker = record(value);
    if (!worker || typeof worker.projectId !== "string" || !UUID.test(worker.projectId) || ids.has(worker.projectId) ||
      !boundedInteger(worker.index, 128) || worker.index < 1 || indices.has(worker.index) ||
      typeof worker.title !== "string" || worker.title.length > 512 || typeof worker.state !== "string" ||
      !boundedInteger(worker.leaderGeneration, Number.MAX_SAFE_INTEGER) || typeof worker.managed !== "boolean") {
      throw new InspectionFailure("manifest_worker_invalid");
    }
    ids.add(worker.projectId); indices.add(worker.index);
    return { index: worker.index, projectId: worker.projectId, title: worker.title, state: worker.state,
      leaderGeneration: worker.leaderGeneration, managed: worker.managed };
  });
  return projects.map(id => {
    const worker = workers.find(value => value.projectId === id);
    if (!worker) throw new InspectionFailure("selected_project_missing_from_manifest");
    return worker;
  });
}

export function requireOwnedInspectionLauncher(current: string | null, exactRegisteredLauncher: string): void {
  if (current !== null && current !== exactRegisteredLauncher) throw new InspectionFailure("foreign_exec_launcher");
}

async function privateReceipt(file: string, value: unknown): Promise<void> {
  const temporary = `${file}.${crypto.randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try { await handle.writeFile(JSON.stringify(value)); await handle.sync(); } finally { await handle.close(); }
  await rename(temporary, file);
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = existingRetirementOptions(env);
  await sample(options);
  const root = path.resolve(".ghostprov/reliability-review-20260911");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "existing-retirement-"));
  const cookieFile = path.join(directory, "candidate-cookies.json");
  let session: PrismSessionManager | null = null;
  const reports: Array<Record<string, unknown>> = [];
  let failure: string | null = null;
  try {
    await copyFile(options.cookieFile, cookieFile);
    session = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await session.resolve(AbortSignal.timeout(60_000));
    const workers = await manifestWorkers(options.manifestFile, identity.fingerprint, options.projects);
    const projectClient = new PrismProjectsClient(identity.account);
    for (const worker of workers) {
      let ydoc: SandboxYDoc | null = null;
      let staged = false, restored = false, submissions = 0, acknowledged = false, journalUnchanged = false;
      let result: PreflightResult | null = null;
      let targetFailure: string | null = null;
      let targetPhase = "lease";
      let targetError: ReturnType<typeof describeExistingInspectionError> | null = null;
      const readbacks: Array<ReturnType<typeof summarizeRetirementPreflightReadback>> = [];
      const targetDirectory = path.join(directory, `worker-${worker.index}`);
      await mkdir(targetDirectory);
      const journal = new FileSupervisorBootstrapJournal(`${options.manifestFile}.bootstrap`, worker.projectId);
      try {
        await journal.withLease(AbortSignal.timeout(150_000), async entry => {
          targetPhase = "journal_load";
          const previous = await entry.load();
          if (!previous || previous.phase !== "submitted" || previous.binding.projectId !== worker.projectId) throw new InspectionFailure("submitted_journal_required");
          const journalDigest = await sha256Hex(canonicalJson(previous));
          const assertAuthority = async () => {
            await sample(options);
            const current = (await manifestWorkers(options.manifestFile, identity.fingerprint, [worker.projectId]))[0]!;
            if (canonicalJson(current) !== canonicalJson(worker)) throw new InspectionFailure("manifest_target_changed");
            const observed = await entry.load();
            if (!observed || await sha256Hex(canonicalJson(observed)) !== journalDigest) throw new InspectionFailure("journal_authority_changed");
          };
          targetPhase = "fresh_scope";
          await assertAuthority();
          targetPhase = "project_read";
          const metadata = await projectClient.get(projectId(worker.projectId), AbortSignal.timeout(30_000));
          if (metadata.deleted || metadata.title !== worker.title) throw new InspectionFailure("remote_project_identity_changed");
          targetPhase = "provision";
          const credentials = await new PrismProvisioningClient({ account: identity.account }).provision(projectId(worker.projectId), AbortSignal.timeout(60_000));
          const gateway = new SandboxGateway({ projectId: projectId(worker.projectId), account: identity.account, sandbox: credentials.sandbox,
            responseBodyLimitBytes: 64 * 1024 });
          ydoc = new SandboxYDoc({ projectId: projectId(worker.projectId), mintYSweet: async () => credentials.ySweet });
          targetPhase = "ydoc_connect";
          await ydoc.connect();
          targetPhase = "assets";
          const assets = await loadV2SidecarRecoveryAssets({ revision: "r10", buildHash: previous.binding.buildHash, schemaHash: previous.binding.schemaHash });
          targetPhase = "bootstrap_identity";
          const payload = await openSupervisorSessionBootstrap({ sessionId: previous.sessionId, expectedBinding: previous.binding, sealed: previous.sealed });
          const sessionId = crypto.randomUUID(), probeNonce = crypto.randomUUID();
          const authority = await createPreflightAuthority({ attempt: previous, assets, supervisorInstanceId: payload.instanceId, sessionId, probeNonce });
          const prepared = await buildRetirementPreflightScript(authority);
          targetPhase = "launcher_ownership";
          const expectedLauncher = supervisorCleanupFiles({ entrypointPath: assets.entrypointPath, supervisorNonce: previous.binding.nonce }).launcher;
          const originalExec = await ydoc.readPath("exec");
          requireOwnedInspectionLauncher(originalExec, expectedLauncher);
          const relative = preflightResultPath(probeNonce);
          const receipt = (phase: "prepared" | "submitted" | "observed") => privateReceipt(path.join(targetDirectory, "diagnostic-attempt.json"), {
            version: 1, phase, sessionId, probeNonce, journalDigest, authoritySha256: prepared.authoritySha256, scriptSha256: prepared.scriptSha256,
          });
          await receipt("prepared");
          targetPhase = "fresh_scope";
          await assertAuthority();
          try {
            targetPhase = "stage";
            staged = await ydoc.compareAndMutatePaths({ guards: [{ path: "exec", equals: originalExec }, { path: relative, equals: null }],
              mutations: [{ kind: "write", path: "exec", content: prepared.script }] });
            if (!staged) throw new InspectionFailure("diagnostic_stage_conflict");
            targetPhase = "sync";
            await gateway.syncWorkspace(AbortSignal.timeout(30_000));
            if (await ydoc.readPath("exec") !== prepared.script) throw new InspectionFailure("diagnostic_stage_changed");
            targetPhase = "fresh_scope_before_send";
            await assertAuthority();
            await receipt("submitted");
            if (++submissions !== 1) throw new InspectionFailure("diagnostic_replay_forbidden");
            targetPhase = "submit";
            await gateway.startSupervisorExec({ sessionId }, AbortSignal.timeout(30_000)).then(() => { acknowledged = true; }, () => undefined);
            targetPhase = "readback";
            const deadline = performance.now() + 45_000;
            for (let read = 0; read < 24 && performance.now() < deadline; read++) {
              const response = await gateway.fetchText(`codex/read_codex_workspace_file?path=${encodeURIComponent(relative)}&session_id=${encodeURIComponent(sessionId)}`,
                { method: "GET", signal: AbortSignal.timeout(Math.max(1, Math.min(5000, Math.floor(deadline - performance.now())))) });
              let parsed: unknown = null;
              try { parsed = JSON.parse(response.text); } catch { /* Metadata reports malformed JSON without content. */ }
              const shape = summarizeRetirementPreflightReadback(response.status, parsed, { sessionId, probeNonce });
              readbacks.push(shape);
              console.log(JSON.stringify({ event: "existing_retirement.readback", worker: worker.index, ...shape }));
              if (response.status === 200) {
                await writeFile(path.join(targetDirectory, "diagnostic-readback.private.json"), response.text, { mode: 0o600 });
                result = decodeRetirementPreflightFile(parsed, { sessionId, probeNonce, authoritySha256: prepared.authoritySha256, scriptSha256: prepared.scriptSha256 });
                await receipt("observed");
                break;
              }
              if (response.status !== 404) throw new InspectionFailure("diagnostic_read_rejected");
              await Bun.sleep(500);
            }
            if (!result) throw new InspectionFailure("diagnostic_result_missing");
          } finally {
            const restoreHeld = staged && submissions > 0 && result === null;
            if (staged && !restoreHeld) {
              targetPhase = "restore";
              restored = await ydoc.compareAndMutatePaths({ guards: [{ path: "exec", equals: prepared.script }],
                mutations: [originalExec === null ? { kind: "tombstone", path: "exec" } : { kind: "write", path: "exec", content: originalExec }] });
              if (!restored) throw new InspectionFailure("diagnostic_restore_conflict");
              await gateway.syncWorkspace(AbortSignal.timeout(30_000));
              if (await ydoc.readPath("exec") !== originalExec) throw new InspectionFailure("diagnostic_restore_changed");
            }
            const after = await entry.load();
            journalUnchanged = after !== null && await sha256Hex(canonicalJson(after)) === journalDigest;
            if (!journalUnchanged) throw new InspectionFailure("journal_authority_changed");
            if (restoreHeld) throw new InspectionFailure("diagnostic_execution_unproven_restore_held");
          }
        });
      } catch (error) {
        targetError = describeExistingInspectionError(error);
        targetFailure = error instanceof InspectionFailure ? error.code : error instanceof Error && /^preflight_[a-z_]+$/.test(error.message) ? error.message : "inspection_failed";
      } finally { await (ydoc as SandboxYDoc | null)?.destroy().catch(() => { targetFailure ??= "ydoc_close_failed"; }); }
      const checked = result as PreflightResult | null;
      reports.push({ worker: worker.index, phase: targetPhase, submissions, acknowledged, staged, restored, journalUnchanged, readbacks,
        result: checked ? { outcome: checked.outcome, code: checked.code, checks: checked.checks, processesInspected: checked.processesInspected,
          processScansCompleted: checked.processScansCompleted, processConflictCount: checked.processConflictCount,
          processConflictsTruncated: checked.processConflictsTruncated, processConflicts: checked.processConflicts, fileFailure: checked.fileFailure } : null,
        failure: targetFailure, error: targetError });
      if (targetFailure) { failure = targetFailure; break; }
    }
  } catch (error) { failure = error instanceof InspectionFailure ? error.code : "inspection_setup_failed"; }
  finally {
    await session?.close().catch(() => { failure ??= "session_close_failed"; });
    for (const file of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`]) {
      const relative = path.relative(directory, path.resolve(file));
      if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new InspectionFailure("private_cleanup_scope");
      await rm(file, { recursive: true, force: true }).catch(() => { failure ??= "private_cleanup_failed"; });
    }
    const inspectionCompleted = failure === null && reports.length === options.projects.length;
    const retirementProved = inspectionCompleted && reports.every(value => (value.result as { outcome?: string } | null)?.outcome === "retired");
    const report = { success: retirementProved, inspectionCompleted, retirementProved, diagnosticOnly: true,
      supervisorLaunches: 0, journalMutations: 0, projectsCreated: 0, projectsDeleted: 0, reports, failure };
    await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ event: "existing_retirement.completed", ...report, reportFile: path.join(directory, "report.json") }));
    if (!report.success) process.exitCode = 1;
  }
}

if (import.meta.main) await main();
