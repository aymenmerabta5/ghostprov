import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import * as Y from "yjs";
import { PrismSessionManager } from "../src/prism/session-manager";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { projectId } from "../src/sandbox/types";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { FileSupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { V2SupervisorHost, createV2SupervisorProvider, type V2SupervisorHostOptions } from "../src/v2/supervisor/host";
import { readSupervisorRetirementReceipt, validateSupervisorRetirementReceipt } from "../src/v2/supervisor/retirement-receipt";
import { decodeSandboxLogResponse } from "../src/v2/diagnostics/read-sandbox-log";

class ProofFailure extends Error {
  constructor(readonly code: string) { super(code); }
}

async function inspectReceipt(options: V2SupervisorHostOptions) {
  const doc = new Y.Doc();
  const provider = createV2SupervisorProvider(doc, () => options.credential);
  try {
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => finish(new ProofFailure("receipt_inspection_timeout")), 10_000);
      const sync = (value: boolean) => { if (value) finish(); };
      const finish = (error?: Error) => {
        clearTimeout(timeout); provider.off("sync", sync);
        if (error) reject(error); else resolve();
      };
      provider.on("sync", sync);
      provider.connect();
    });
    const raw = readSupervisorRetirementReceipt(doc);
    const authenticated = raw !== null && await validateSupervisorRetirementReceipt(raw, options.binding, options.instanceId, options.rotationKey) !== null;
    return { present: raw !== null, authenticated };
  } finally { provider.disconnect(); provider.destroy(); doc.destroy(); }
}

export function retiredSupervisorProofOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_RETIRED_SUPERVISOR_REPRO !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new ProofFailure("explicit_disposable_live_guards_required");
  }
  if (env.PORT !== "18888") throw new ProofFailure("candidate_port_must_be_18888");
  if (!env.PRISM_COOKIE_FILE?.trim()) throw new ProofFailure("explicit_cookie_file_required");
  const expected = env.GP_LIVE_RETIRED_SUPERVISOR_EXPECT ?? "recovered";
  if (expected !== "blocked" && expected !== "recovered") throw new ProofFailure("invalid_expected_outcome");
  return { cookieFile: path.resolve(env.PRISM_COOKIE_FILE), origin: env.PRISM_ORIGIN ?? "https://prism.openai.com", expected };
}

/** Simulate host loss after signed remote shutdown but before local journal save. */
export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = retiredSupervisorProofOptions(env);
  const root = path.resolve(".ghostprov/reliability-review-20260911");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "retired-repro-"));
  const relative = path.relative(root, directory);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new ProofFailure("invalid_proof_directory");
  const cookieFile = path.join(directory, "candidate-cookies.json");
  const journalDirectory = path.join(directory, "journal");
  const id = projectId(crypto.randomUUID());
  const title = `GhostProv retired-supervisor proof ${path.basename(directory)}`;
  let manager: PrismSessionManager | null = null;
  let projects: PrismProjectsClient | null = null;
  let ydoc: SandboxYDoc | null = null;
  let first: V2SandboxSupervisor | null = null;
  let second: V2SandboxSupervisor | null = null;
  let third: V2SandboxSupervisor | null = null;
  let firstHost: V2SupervisorHost | null = null;
  let firstHostOptions: V2SupervisorHostOptions | null = null;
  let gatewayForDiagnostics: SandboxGateway | null = null;
  let firstExecutionSession: string | null = null;
  const provedShutdowns = new Set<string>();
  const supervisorDiagnostics: Array<Record<string, unknown>> = [];
  let createAttempted = false;
  let deleted = false;
  let submissions = 0;
  let signedShutdown = false;
  let reproduced = false;
  let recovered = false;
  let normalRestart = false;
  let secondShutdown = false;
  let thirdShutdown = false;
  let phase = "create";
  let failure: string | null = null;
  let observed: { name: string; code: unknown; reason: unknown } | null = null;
  let release: { revision: string; buildHash: string; schemaHash: string } | null = null;
  const receiptObservations: Array<{ phase: string; present: boolean; authenticated: boolean }> = [];
  let errorDetails: { name: string; code: unknown; reason: unknown; causeName: string | null; causePhase: unknown } | null = null;
  await copyFile(options.cookieFile, cookieFile);
  try {
    manager = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await manager.resolve(AbortSignal.timeout(60_000));
    projects = new PrismProjectsClient(identity.account);
    await writeFile(path.join(directory, "owned-project.json"), JSON.stringify({ id, title }));
    createAttempted = true;
    await projects.create({ projectId: id, title }, AbortSignal.timeout(60_000));
    phase = "provision";
    const credentials = await new PrismProvisioningClient({ account: identity.account }).provision(id, AbortSignal.timeout(90_000));
    const gateway = new SandboxGateway({ projectId: id, account: identity.account, sandbox: credentials.sandbox });
    gatewayForDiagnostics = gateway;
    const submit = gateway.startSupervisorExec.bind(gateway);
    gateway.startSupervisorExec = async (request, signal) => {
      if (++submissions > (options.expected === "blocked" ? 1 : 3)) throw new ProofFailure("unexpected_extra_submission");
      firstExecutionSession ??= request.sessionId;
      return submit(request, signal);
    };
    ydoc = new SandboxYDoc({ projectId: id, mintYSweet: async () => credentials.ySweet });
    await ydoc.connect();
    const assets = await loadV2SidecarAssets({ revision: "r10" });
    release = { revision: assets.revision, buildHash: assets.buildHash, schemaHash: assets.schemaHash };
    for (const file of [...assets.files, assets.manifest]) await ydoc.writePath(file.path, file.content);
    await gateway.syncWorkspace(AbortSignal.timeout(45_000));
    const journal = new FileSupervisorBootstrapJournal(journalDirectory, id);
    const create = (generation: number, timeout: number) => new V2SandboxSupervisor({
      binding: { projectId: id, sandboxSessionId: `retired-proof-${generation}`, leaderGeneration: generation,
        nonce: crypto.randomUUID(), buildHash: assets.buildHash, schemaHash: assets.schemaHash, protocolRevision: "r2", schemaVersion: 2 },
      assets, bootstrapMode: "direct", bootstrapJournal: journal, ydoc: ydoc!, gateway, ySweet: credentials.ySweet,
      connectTimeoutMs: timeout, commandTimeoutMs: 75_000,
      hostFactory: input => {
        const host = new V2SupervisorHost(input);
        const shutdown = host.shutdown.bind(host);
        host.shutdown = async (reason, signal) => {
          await shutdown(reason, signal);
          provedShutdowns.add(input.binding.nonce);
        };
        if (generation === 1) { firstHost = host; firstHostOptions = input; }
        return host;
      },
    });
    phase = "first_start";
    first = create(1, 90_000);
    await first.ensureReady(AbortSignal.timeout(180_000));
    if (!firstHost) throw new ProofFailure("missing_exact_host");
    phase = "remote_shutdown_before_local_commit";
    await (firstHost as V2SupervisorHost).shutdown("retired_receipt_reproduction", AbortSignal.timeout(30_000));
    signedShutdown = true;
    await first.close();
    const before = JSON.parse(await readFile(path.join(journalDirectory, `${id}.json`), "utf8")) as { phase?: string };
    if (before.phase !== "submitted") throw new ProofFailure("simulation_did_not_retain_submitted_journal");
    await Bun.sleep(2_000);
    if (firstHostOptions) receiptObservations.push({ phase: "before_restart", ...await inspectReceipt(firstHostOptions) });
    phase = "restart_after_retirement";
    second = create(2, options.expected === "blocked" ? 8_000 : 90_000);
    if (options.expected === "blocked") {
      try {
        await second.ensureReady(AbortSignal.timeout(20_000));
        throw new ProofFailure("unexpected_restart_success");
      } catch (error) {
        const value = error as { name?: string; remoteCode?: unknown; reason?: unknown };
        observed = { name: value.name ?? "Error", code: value.remoteCode ?? null, reason: value.reason ?? null };
        if (value.remoteCode !== "bootstrap_cleanup_unproven" || value.reason !== "bootstrap" || submissions !== 1) throw error;
        reproduced = true;
      }
    } else {
      await second.ensureReady(AbortSignal.timeout(180_000));
      if (submissions !== 2) throw new ProofFailure("recovered_submission_count_mismatch");
      recovered = true;
      phase = "normal_shutdown_and_restart";
      await second.shutdown("retired_proof_second_complete", AbortSignal.timeout(45_000));
      secondShutdown = true;
      await second.close();
      third = create(3, 90_000);
      await third.ensureReady(AbortSignal.timeout(180_000));
      if (Number(submissions) !== 3) throw new ProofFailure("normal_restart_submission_count_mismatch");
      normalRestart = true;
      await third.shutdown("retired_proof_third_complete", AbortSignal.timeout(45_000));
      thirdShutdown = true;
    }
  } catch (error) {
    failure = error instanceof ProofFailure ? error.code : error instanceof Error ? error.name : "Error";
    const value = error as { name?: string; remoteCode?: unknown; code?: unknown; reason?: unknown; cause?: { name?: string; phase?: unknown } };
    errorDetails = { name: value.name ?? "Error", code: value.remoteCode ?? value.code ?? null, reason: value.reason ?? null,
      causeName: value.cause?.name ?? null, causePhase: value.cause?.phase ?? null };
    if (firstHostOptions) await inspectReceipt(firstHostOptions).then(value => {
      receiptObservations.push({ phase: "after_failure", ...value });
    }, () => undefined);
    if (gatewayForDiagnostics && firstExecutionSession) {
      for (const file of ["supervisor.1.jsonl", "supervisor.jsonl"] as const) {
        try {
          const result = await gatewayForDiagnostics.readDiagnosticFile(firstExecutionSession, file, AbortSignal.timeout(10_000));
          if (result.status !== 200) continue;
          const lines = decodeSandboxLogResponse(result.value, { sessionId: firstExecutionSession, file }).trim().split("\n");
          for (const line of lines.slice(-12)) {
            if (!line) continue;
            const entry = JSON.parse(line) as Record<string, unknown>;
            supervisorDiagnostics.push(Object.fromEntries(["event", "phase", "error_name", "error_code", "permission"].filter(key => entry[key] !== undefined).map(key => [key, entry[key]])));
          }
        } catch { supervisorDiagnostics.push({ event: "diagnostic_read_unavailable" }); }
      }
    }
  } finally {
    if (third && !thirdShutdown) await third.shutdown("retired_proof_cleanup", AbortSignal.timeout(45_000))
      .then(() => { thirdShutdown = true; }, () => { failure ??= "third_shutdown_unproven"; });
    await third?.close().catch(() => { failure ??= "third_close_failed"; });
    if (options.expected === "recovered" && second && !secondShutdown) await second.shutdown("retired_proof_cleanup", AbortSignal.timeout(45_000))
      .then(() => { secondShutdown = true; }, () => { failure ??= "second_shutdown_unproven"; });
    await second?.close().catch(() => { failure ??= "second_close_failed"; });
    if (first && !signedShutdown) {
      await first.shutdown("retired_proof_cleanup", AbortSignal.timeout(45_000)).then(() => { signedShutdown = true; }, () => { failure ??= "shutdown_unproven"; });
    }
    await first?.close().catch(() => { failure ??= "first_close_failed"; });
    await ydoc?.destroy().catch(() => { failure ??= "ydoc_close_failed"; });
    if (createAttempted && projects) {
      try {
        const project = await projects.get(id, AbortSignal.timeout(30_000));
        if (project.title !== title) throw new ProofFailure("project_ownership_changed");
        await projects.deleteManaged(id, AbortSignal.timeout(30_000));
        try { deleted = (await projects.get(id, AbortSignal.timeout(30_000))).deleted; }
        catch (error) { if ((error as { status?: number }).status === 404) deleted = true; else throw error; }
      } catch (error) {
        if ((error as { status?: number }).status === 404) deleted = true;
        else failure ??= error instanceof ProofFailure ? error.code : "project_cleanup_failed";
      }
    }
    await manager?.close().catch(() => { failure ??= "session_close_failed"; });
    for (const file of [cookieFile, `${cookieFile}.identity.json`]) await rm(file, { force: true });
    signedShutdown = first !== null && provedShutdowns.has(first.binding.nonce);
    secondShutdown = second !== null && provedShutdowns.has(second.binding.nonce);
    thirdShutdown = third !== null && provedShutdowns.has(third.binding.nonce);
    const report = { expected: options.expected, reproduced, recovered, normalRestart, signedShutdown, secondShutdown, thirdShutdown,
      submissions, deleted, phase, observed, release, receiptObservations, supervisorDiagnostics, errorDetails, failure };
    await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
    console.log(JSON.stringify({ event: "retired_supervisor_repro", ...report, directory }));
    const outcomePassed = options.expected === "blocked" ? reproduced : recovered && normalRestart && secondShutdown && thirdShutdown;
    if (!outcomePassed || !signedShutdown || !deleted || failure) process.exitCode = 1;
  }
}

if (import.meta.main) await main();
