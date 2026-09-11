import { copyFile, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { PrismSessionManager } from "../src/prism/session-manager";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { projectId } from "../src/sandbox/types";
import { loadV2SidecarAssets, loadV2SidecarRecoveryAssets, V2_R10_RELEASE, V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY } from "../src/v2/sidecar/assets";
import { FileSupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { V2SupervisorHost } from "../src/v2/supervisor/host";
import { buildRetirementPreflightScript, createPreflightAuthority, decodeRetirementPreflightFile,
  preflightResultPath, summarizeRetirementPreflightReadback, type PreflightResult } from "./lib/retirement-preflight-proof";

class ProofFailure extends Error {
  constructor(readonly code: string) { super(code); }
}

export function retirementPreflightOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_RETIREMENT_PREFLIGHT !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new ProofFailure("explicit_disposable_live_guards_required");
  }
  if (env.PORT !== "18888") throw new ProofFailure("candidate_port_must_be_18888");
  if (!env.PRISM_COOKIE_FILE?.trim()) throw new ProofFailure("explicit_cookie_file_required");
  const releaseKind = env.GP_LIVE_RETIREMENT_PREFLIGHT_RELEASE ?? "current";
  if (releaseKind !== "current" && releaseKind !== "initial") throw new ProofFailure("invalid_preflight_release_selection");
  return { cookieFile: path.resolve(env.PRISM_COOKIE_FILE), origin: env.PRISM_ORIGIN ?? "https://prism.openai.com", releaseKind };
}

function contained(root: string, target: string): void {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new ProofFailure("cleanup_path_escape");
}

/** Creates one disposable supervisor; the separate diagnostic performs no lifecycle action. */
export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = retirementPreflightOptions(env);
  if (options.releaseKind === "initial" && (V2_R10_RELEASE.buildHash !== V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY.buildHash ||
    V2_R10_RELEASE.schemaHash !== V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY.schemaHash)) {
    throw new ProofFailure("initial_release_is_cleanup_only_after_upgrade");
  }
  const assets = options.releaseKind === "current" ? await loadV2SidecarAssets({ revision: "r10" })
    : await loadV2SidecarRecoveryAssets(V2_R10_INITIAL_RELEASE_RETIREMENT_IDENTITY);
  const root = path.resolve(".ghostprov/reliability-review-20260911");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "retirement-preflight-"));
  contained(root, directory);
  const cookieFile = path.join(directory, "candidate-cookies.json");
  const journalDirectory = path.join(directory, "journal");
  const id = projectId(crypto.randomUUID());
  const title = `GhostProv retirement preflight ${path.basename(directory)}`;
  let manager: PrismSessionManager | null = null;
  let projects: PrismProjectsClient | null = null;
  let ydoc: SandboxYDoc | null = null;
  let supervisor: V2SandboxSupervisor | null = null;
  let supervisorHost: V2SupervisorHost | null = null;
  let createAttempted = false;
  let deleted = false;
  let bootstrapSubmissions = 0;
  let diagnosticSubmissions = 0;
  let diagnosticAcknowledged = false;
  let signedShutdown = false;
  let phase = "create";
  let failure: string | null = null;
  let result: PreflightResult | null = null;
  let readbackCaptured = false;
  const readbacks: Array<ReturnType<typeof summarizeRetirementPreflightReadback>> = [];
  const setPhase = (value: string) => { phase = value; console.log(JSON.stringify({ event: "retirement_preflight.phase", phase })); };
  try {
    await copyFile(options.cookieFile, cookieFile);
    manager = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await manager.resolve(AbortSignal.timeout(60_000));
    projects = new PrismProjectsClient(identity.account);
    if ((await projects.list(AbortSignal.timeout(30_000))).some(value => value.projectId === id)) throw new ProofFailure("project_collision");
    await writeFile(path.join(directory, "owned-project.json"), JSON.stringify({ id, title }), { mode: 0o600 });
    createAttempted = true;
    await projects.create({ projectId: id, title }, AbortSignal.timeout(60_000));
    setPhase("provision");
    const credentials = await new PrismProvisioningClient({ account: identity.account }).provision(id, AbortSignal.timeout(90_000));
    const gateway = new SandboxGateway({ projectId: id, account: identity.account, sandbox: credentials.sandbox, responseBodyLimitBytes: 64 * 1024 });
    const bootstrapGateway = new Proxy(gateway, { get(target, key) {
      if (key === "startSupervisorExec") return async (...args: Parameters<SandboxGateway["startSupervisorExec"]>) => {
        if (++bootstrapSubmissions !== 1) throw new ProofFailure("bootstrap_submission_replay_forbidden");
        return target.startSupervisorExec(...args);
      };
      const value = Reflect.get(target, key);
      return typeof value === "function" ? value.bind(target) : value;
    } });
    ydoc = new SandboxYDoc({ projectId: id, mintYSweet: async () => credentials.ySweet });
    await ydoc.connect();
    for (const file of [...assets.files, assets.manifest]) await ydoc.writePath(file.path, file.content);
    await gateway.syncWorkspace(AbortSignal.timeout(45_000));
    const journal = new FileSupervisorBootstrapJournal(journalDirectory, id);
    supervisor = new V2SandboxSupervisor({
      binding: { projectId: id, sandboxSessionId: "retirement-preflight", leaderGeneration: 1, nonce: crypto.randomUUID(),
        buildHash: assets.buildHash, schemaHash: assets.schemaHash, protocolRevision: "r2", schemaVersion: 2 },
      assets, bootstrapMode: "direct", bootstrapJournal: journal, ydoc, gateway: bootstrapGateway, ySweet: credentials.ySweet,
      connectTimeoutMs: 30_000, commandTimeoutMs: 30_000,
      hostFactory: input => { const host = new V2SupervisorHost(input); supervisorHost = host; return host; },
    });
    setPhase("supervisor_start");
    await supervisor.ensureReady(AbortSignal.timeout(90_000));
    if (!supervisorHost) throw new ProofFailure("exact_supervisor_host_missing");
    setPhase("signed_shutdown_before_local_commit");
    await (supervisorHost as V2SupervisorHost).shutdown("retirement_preflight_prepare", AbortSignal.timeout(30_000));
    signedShutdown = true;
    await supervisor.close();
    await Bun.sleep(2_000);
    setPhase("diagnostic_preflight");
    // Keep the exact original journal lease through inspection. This never clears or rewrites it.
    await journal.withLease(AbortSignal.timeout(90_000), async entry => {
      const attempt = await entry.load();
      if (!attempt || attempt.phase !== "submitted") throw new ProofFailure("submitted_authority_missing");
      const sessionId = crypto.randomUUID(), probeNonce = crypto.randomUUID();
      const authority = await createPreflightAuthority({ attempt, assets, supervisorInstanceId: supervisor!.instanceId, sessionId, probeNonce });
      const prepared = await buildRetirementPreflightScript(authority);
      const relative = preflightResultPath(probeNonce);
      const privateLedger = path.join(directory, "diagnostic-attempt.json");
      const savePhase = async (state: "prepared" | "submitted" | "observed") => {
        await writeFile(privateLedger, JSON.stringify({ version: 1, phase: state, sessionId, probeNonce,
          authoritySha256: prepared.authoritySha256, scriptSha256: prepared.scriptSha256 }), { mode: 0o600 });
      };
      await savePhase("prepared");
      const staged = await ydoc!.compareAndMutatePaths({ guards: [{ path: "exec", equals: null }, { path: relative, equals: null }],
        mutations: [{ kind: "write", path: "exec", content: prepared.script }] });
      if (!staged) throw new ProofFailure("diagnostic_staging_conflict");
      await gateway.syncWorkspace(AbortSignal.timeout(30_000));
      if (await ydoc!.readPath("exec") !== prepared.script) throw new ProofFailure("diagnostic_staging_changed");
      await savePhase("submitted");
      setPhase("diagnostic_submission");
      if (++diagnosticSubmissions !== 1) throw new ProofFailure("diagnostic_submission_replay_forbidden");
      // A lost acknowledgement is ambiguous; observe this exact session, never resubmit.
      await gateway.startSupervisorExec({ sessionId }, AbortSignal.timeout(30_000)).then(() => { diagnosticAcknowledged = true; }, () => undefined);
      setPhase("diagnostic_readback");
      const deadline = performance.now() + 45_000;
      for (let read = 0; read < 24 && performance.now() < deadline; read++) {
        const response = await gateway.fetchText(`codex/read_codex_workspace_file?path=${encodeURIComponent(relative)}&session_id=${encodeURIComponent(sessionId)}`,
          { method: "GET", signal: AbortSignal.timeout(Math.min(5_000, Math.max(1, Math.floor(deadline - performance.now())))) });
        let parsed: unknown = null;
        try { parsed = JSON.parse(response.text); } catch { /* Report only the failed object-shape check. */ }
        const shape = summarizeRetirementPreflightReadback(response.status, parsed, { sessionId, probeNonce });
        readbacks.push(shape);
        console.log(JSON.stringify({ event: "retirement_preflight.readback_shape", ...shape }));
        if (response.status === 200) {
          // Preserve this already executed diagnostic privately for an offline validator rerun.
          // No lifecycle or diagnostic submission is replayed when readback validation fails.
          await writeFile(path.join(directory, "diagnostic-readback.private.json"), response.text, { mode: 0o600 });
          readbackCaptured = true;
          result = decodeRetirementPreflightFile(parsed, { sessionId, probeNonce,
            authoritySha256: prepared.authoritySha256, scriptSha256: prepared.scriptSha256 });
          await savePhase("observed");
          break;
        }
        if (response.status !== 404) throw new ProofFailure("diagnostic_read_rejected");
        await Bun.sleep(500);
      }
      if (!result) throw new ProofFailure("diagnostic_result_unobserved");
      if ((result as PreflightResult).outcome !== "retired") throw new ProofFailure("diagnostic_retirement_not_proved");
    });
  } catch (error) {
    failure = error instanceof ProofFailure ? error.code : error instanceof Error && /^preflight_[a-z_]+$/.test(error.message)
      ? error.message : error instanceof Error ? error.name : "Error";
  } finally {
    if (supervisor && !signedShutdown) {
      await supervisor.shutdown("retirement_preflight_cleanup", AbortSignal.timeout(45_000)).then(() => { signedShutdown = true; }, () => { failure ??= "shutdown_unproven"; });
    }
    await supervisor?.close().catch(() => { failure ??= "supervisor_close_failed"; });
    await ydoc?.destroy().catch(() => { failure ??= "ydoc_close_failed"; });
    if (createAttempted && projects) {
      try {
        const current = await projects.get(id, AbortSignal.timeout(30_000));
        if (!current.deleted && current.title !== title) throw new ProofFailure("project_ownership_changed");
        if (!current.deleted) await projects.deleteManaged(id, AbortSignal.timeout(30_000));
        try { deleted = (await projects.get(id, AbortSignal.timeout(30_000))).deleted; }
        catch (error) { if ([404, 410].includes((error as { status: number }).status)) deleted = true; else throw error; }
      } catch (error) {
        if ([404, 410].includes((error as { status: number }).status)) deleted = true;
        else failure ??= error instanceof ProofFailure ? error.code : "project_cleanup_failed";
      }
    }
    await manager?.close().catch(() => { failure ??= "session_close_failed"; });
    for (const target of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`, ...(deleted ? [journalDirectory] : [])]) {
      contained(directory, target);
      await rm(target, { force: true, recursive: true }).catch(() => { failure ??= "private_cleanup_failed"; });
    }
    const checked = result as PreflightResult | null;
    const report = { success: failure === null && checked?.outcome === "retired" && signedShutdown && deleted,
      phase, buildHash: assets.buildHash, releaseKind: options.releaseKind, bootstrapSubmissions, diagnosticSubmissions, diagnosticAcknowledged, readbackCaptured, readbacks,
      signedShutdown, childLaunched: false, deleted, result: checked ? { outcome: checked.outcome, code: checked.code,
        checks: checked.checks, processesInspected: checked.processesInspected, processScansCompleted: checked.processScansCompleted,
        processConflictCount: checked.processConflictCount, processConflictsTruncated: checked.processConflictsTruncated,
        processConflicts: checked.processConflicts, fileFailure: checked.fileFailure } : null, failure };
    await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ event: "retirement_preflight.completed", ...report, reportFile: path.join(directory, "report.json") }));
    if (!report.success) process.exitCode = 1;
  }
}

if (import.meta.main) await main();
