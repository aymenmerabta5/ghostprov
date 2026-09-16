import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { lstat, open, readFile, rename } from "node:fs/promises";
import path from "node:path";
import { FileLease } from "../src/core/file-lease";
import { PrismAccountSession } from "../src/prism/account-session";
import { PrismProjectError, PrismProjectsClient } from "../src/prism/projects-client";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { projectId } from "../src/sandbox/types";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { loadV2SidecarRecoveryAssets } from "../src/v2/sidecar/assets";
import {
  FileSupervisorBootstrapJournal,
  validateSupervisorBootstrapAttempt,
  type BootstrapJournalEntry,
  type SupervisorBootstrapAttempt,
  type SupervisorBootstrapJournal,
} from "../src/v2/supervisor/bootstrap-journal";
import { proveBlockedSupervisorBootstrapCleanup } from "../src/v2/supervisor/controller";
import { V2SupervisorHost } from "../src/v2/supervisor/host";
import { sameSupervisorBinding } from "../src/v2/supervisor/protocol";
import {
  DIRECT_SUPERVISOR_INSTALL_PATH,
  DIRECT_SUPERVISOR_LAUNCH_PATH,
  supervisorCleanupFiles,
} from "../src/v2/supervisor/r10-direct-launcher";
import {
  SUPERVISOR_CREDENTIAL_PATH,
  SUPERVISOR_STATIC_PATH,
  supervisorStaticConfig,
} from "../src/v2/supervisor/r10-launcher";
import { canonicalJson } from "../src/v2/protocol/shared";
import { openSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";

const ROOT = path.resolve(import.meta.dir, "../.ghostprov/live-capacity-reliability/proof-35WVDV");
const ORIGIN = "https://prism.openai.com";
const PROJECT = projectId("c86e27e7-addd-43a0-85e2-b9f5c1792ae1");
const TITLE = "GhostProv Capacity Proof proof-35WVDV 3";
const WRITER_PID = 40_704;
const JOURNAL_NAME = `fleet.json.bootstrap/${PROJECT}.json`;
const RECEIPT_NAME = "worker-3-retained-cleanup.receipt.private.json";
const EXPECTED = Object.freeze({
  "report.json": "0e87b2a5bf937d10a102033477f8798fef7207530dfb39f9cc382438370e3451",
  "ownership.json": "fcc890bc04a781ee5573687dc89166c1d43ac1e21f54f919fee52b7d6ec4519f",
  "fleet.json": "50459d95e17cae976b68d09402f9fe702f29ef60a12cde3d45fc6663b74ace6d",
  "supervisor-cleanup.private.json": "846676b8aa789218ac6abc3e52b0039451e1a0af148a053f0282f67103b25103",
  "execution-descriptors.private.json": "856c1aa98deb6feeb999e51aa79732110f4551e6d950ba90495a554c123fb49d",
  "command-acceptance-8.private.json": "e933c8f7a9fdd469466c4a92045efad7b719721895d36569abf3bd0b0b1cabdd",
  "observations.jsonl": "5d871bfb58483519c0315fbaf091cb37fbab7ef1823cd1cdf8f072078aa97a4b",
  "candidate-cookies.json": "385500ec9b97d1a41463dff31c07e2cd7f60e49477ec14721464511fca8398a3",
  "candidate-cookies.json.identity.json": "a9a2a9f8b8ddd8ceee0b3aba94525cd181e744dba7ecc1b902b393aff09f38a2",
  "backend-10.private.json": "eadeef6f2209733ea8dab5812e46082885c41c6dbdc0dcdb211b423321daa33a",
  [JOURNAL_NAME]: "79b6fa4cd2f805afc394a76be3f1f55081fc83ddce260709358fe7e6820c85cb",
});
const MAX_FILE_BYTES: Readonly<Record<string, number>> = Object.freeze({
  "observations.jsonl": 32 * 1024 * 1024,
  "candidate-cookies.json": 8 * 1024 * 1024,
  "report.json": 128 * 1024,
});

export const RETAINED_WORKER3_CLEANUP_PLAN = Object.freeze({
  proofId: "proof-35WVDV",
  worker: 3,
  projectId: PROJECT,
  title: TITLE,
  journalBeforeSha256: EXPECTED[JOURNAL_NAME],
  proofAttempts: 1,
  workspaceAcquisitions: 1,
  projectDeleteAttempts: 1,
  recoveryMode: "journal_authorized_unaccepted_sequence_zero",
  cleanupWorkspaceMode: "new_disposable_sandbox_after_exact_supervisor_retirement",
  reusesRetainedSandbox: false,
  fullCleanupEligible: true,
  preservesFailedReport: true,
});

export function resolveRetainedWorker3CleanupAuthorization(
  argv: readonly string[],
  env: Readonly<Record<string, string | undefined>>,
): typeof RETAINED_WORKER3_CLEANUP_PLAN {
  if (argv.length !== 1 || argv[0] !== "--execute-authorized-cleanup" ||
    env.GP_LIVE_PROOF_35WVDV_WORKER3_CLEANUP !== "1" ||
    env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new Error("explicit_worker3_cleanup_authorization_required");
  }
  return RETAINED_WORKER3_CLEANUP_PLAN;
}

const sha256 = (value: Uint8Array | string): string => createHash("sha256").update(value).digest("hex");
const fail = (code: string): never => { throw new Error(code); };

async function stableBytes(name: string, expected: string | undefined = (EXPECTED as Readonly<Record<string, string>>)[name]): Promise<Uint8Array> {
  const file = path.join(ROOT, name);
  const limit = MAX_FILE_BYTES[name] ?? 256 * 1024;
  const before = await lstat(file);
  if (!before.isFile() || before.isSymbolicLink() || before.size < 1 || before.size > limit) fail("guard_file_shape");
  const value = await readFile(file);
  const after = await lstat(file);
  if (before.ino !== after.ino || before.size !== after.size || before.mtimeMs !== after.mtimeMs || value.byteLength > limit) {
    fail("guard_file_changed_during_read");
  }
  if (expected && sha256(value) !== expected) fail("guard_file_hash_changed");
  return value;
}

async function writeOnce(name: string, value: unknown): Promise<void> {
  const text = JSON.stringify(value, null, 2);
  if (Buffer.byteLength(text) > 128 * 1024) fail("receipt_bound");
  const handle = await open(path.join(ROOT, name), "wx", 0o600);
  try { await handle.writeFile(text); await handle.sync(); }
  finally { await handle.close(); }
}

async function replaceReceipt(value: unknown, expectedHash: string): Promise<string> {
  const file = path.join(ROOT, RECEIPT_NAME);
  const text = JSON.stringify(value, null, 2);
  if (Buffer.byteLength(text) > 128 * 1024 || sha256(await stableBytes(RECEIPT_NAME, expectedHash)) !== expectedHash) fail("receipt_changed");
  const temporary = `${file}.${randomUUID()}.tmp`;
  const handle = await open(temporary, "wx", 0o600);
  try { await handle.writeFile(text); await handle.sync(); }
  finally { await handle.close(); }
  if (sha256(await stableBytes(RECEIPT_NAME, expectedHash)) !== expectedHash) fail("receipt_changed");
  await rename(temporary, file);
  const next = sha256(text);
  if (sha256(await stableBytes(RECEIPT_NAME, next)) !== next) fail("receipt_readback_failed");
  return next;
}

async function assertWriterEnded(): Promise<void> {
  const command = `try { $p=Get-Process -Id ${WRITER_PID} -ErrorAction Stop; @{state='alive';pid=$p.Id}|ConvertTo-Json -Compress } catch [Microsoft.PowerShell.Commands.ProcessCommandException] { if($_.FullyQualifiedErrorId -like 'NoProcessFoundForGivenId*') { @{state='absent';pid=${WRITER_PID}}|ConvertTo-Json -Compress } else { throw } }`;
  const raw = await new Promise<string>((resolve, reject) => execFile("powershell.exe",
    ["-NoProfile", "-NonInteractive", "-Command", command],
    { windowsHide: true, timeout: 10_000, maxBuffer: 4_096 },
    (error, stdout) => error ? reject(new Error("writer_check_failed")) : resolve(stdout)));
  const result = JSON.parse(raw) as { state?: unknown; pid?: unknown };
  if (result.state !== "absent" || result.pid !== WRITER_PID) fail("original_writer_still_present");
}

interface AuditEvidence {
  readonly ownership: any;
  readonly worker: any;
  readonly attempt: SupervisorBootstrapAttempt;
  readonly cleanupAttempt: SupervisorBootstrapAttempt;
  readonly instanceId: string;
  readonly assets: Awaited<ReturnType<typeof loadV2SidecarRecoveryAssets>>;
}

async function audit(): Promise<AuditEvidence> {
  const values = new Map<string, Uint8Array>();
  for (const name of Object.keys(EXPECTED)) values.set(name, await stableBytes(name));
  const parse = (name: string): any => JSON.parse(Buffer.from(values.get(name)!).toString("utf8"));
  const report = parse("report.json"), ownership = parse("ownership.json"), fleet = parse("fleet.json");
  const ledger = parse("supervisor-cleanup.private.json"), descriptors = parse("execution-descriptors.private.json");
  const firstAcceptance = parse("command-acceptance-8.private.json"), lastBackend = parse("backend-10.private.json");
  if (report.success !== false || report.failure !== "all_runtimes_not_usable" || report.deleted !== 7 ||
    report.providerRequests !== 0 || report.projectCleanup?.find((row: any) => row.worker === 3)?.deleted !== false) fail("report_guard_failed");
  if (!Array.isArray(ownership.intended) || ownership.intended.length !== 8 || new Set(ownership.intended).size !== 8 ||
    ownership.intended[2] !== PROJECT || ownership.titles?.[PROJECT] !== TITLE ||
    !["attempted", "created"].every(field => canonicalJson(ownership[field]) === canonicalJson(ownership.intended)) ||
    !Array.isArray(ownership.deleted) || ownership.deleted.length !== 7 || ownership.deleted.includes(PROJECT)) fail("ownership_guard_failed");
  if (fleet.identityFingerprint !== "7b580cda1ae9a8ab4f16c29f5d836f1398988b6903a23ff4a993b999b56590f0") fail("fleet_identity_guard_failed");
  const worker = fleet.workers?.find((row: any) => row.projectId === PROJECT);
  if (!worker || worker.index !== 3 || worker.title !== "Imported Prism project 3" || worker.managed !== false ||
    worker.state !== "failed" || worker.leaderGeneration !== 2 || worker.retirementReason !== null) fail("worker_guard_failed");
  const closed = ledger.projects?.find((row: any) => row.projectId === PROJECT);
  if (!ledger.cleanupStarted || closed?.controllersRegistered !== 2 || closed.controllersClosed !== 2 ||
    closed.journalEmpty !== false || closed.deleted !== false || closed.supervisors?.length !== 2) fail("controller_guard_failed");
  const attempt = validateSupervisorBootstrapAttempt(parse(JOURNAL_NAME), PROJECT);
  if (attempt.phase !== "submitted" || Object.hasOwn(attempt, "commandAcceptance") ||
    attempt.sessionId !== "c4882249-6b74-4125-83d6-0664b29141b7" ||
    attempt.binding.sandboxSessionId !== "8f428480e925494ca62f5db2312741be" ||
    attempt.binding.leaderGeneration !== 2 || attempt.binding.nonce !== "supervisor-e6bec14f-c4f4-4d6f-a351-556f4dfa753b" ||
    attempt.binding.buildHash !== "8b9e7096f216496944d67c91e2f1b884c5e0c27bf44e47b6a3215f56e3946745" ||
    attempt.binding.schemaHash !== "51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f") fail("journal_guard_failed");
  const executions = descriptors.flatMap((row: any) => row.worker === 3 ? row.executions ?? [] : []);
  const current = executions.filter((row: any) => row.sessionId === attempt.sessionId &&
    sameSupervisorBinding(row.binding, attempt.binding));
  if (current.length !== 1 || current[0].dispatchState !== "may_have_started" || current[0].durableAcceptance !== false ||
    Object.hasOwn(current[0], "commandAcceptance")) fail("execution_frontier_guard_failed");
  if (firstAcceptance.projectId !== PROJECT || firstAcceptance.nonce === attempt.binding.nonce) fail("prior_generation_guard_failed");
  if (lastBackend.projectId !== PROJECT || lastBackend.sandboxSessionId !== null ||
    !Array.isArray(lastBackend.executions) || lastBackend.executions.length !== 0) fail("retained_backend_frontier_changed");
  const authority = await openSupervisorSessionBootstrap({ sessionId: attempt.sessionId,
    expectedBinding: attempt.binding, sealed: attempt.sealed });
  const assets = await loadV2SidecarRecoveryAssets({ revision: "r10", buildHash: attempt.binding.buildHash,
    schemaHash: attempt.binding.schemaHash });
  const cleanupAttempt = validateSupervisorBootstrapAttempt({ ...attempt, phase: "cleanup_proven" }, PROJECT);
  return { ownership, worker, attempt, cleanupAttempt, instanceId: authority.instanceId, assets };
}

function guardedJournal(evidence: AuditEvidence): SupervisorBootstrapJournal {
  const source = new FileSupervisorBootstrapJournal(path.join(ROOT, "fleet.json.bootstrap"), PROJECT);
  let proofSaved = false;
  return {
    withLease: (signal, operation) => source.withLease(signal, async entry => {
      if (!proofSaved) await stableBytes(JOURNAL_NAME);
      const guarded: BootstrapJournalEntry = {
        load: async () => {
          const value = await entry.load();
          const expected = proofSaved ? evidence.cleanupAttempt : evidence.attempt;
          if (!value || canonicalJson(value) !== canonicalJson(expected)) fail("journal_changed_under_lease");
          return value;
        },
        save: async value => {
          if (proofSaved || canonicalJson(value) !== canonicalJson(evidence.cleanupAttempt)) fail("unexpected_journal_mutation");
          await entry.save(value);
          const readback = await entry.load();
          if (!readback || canonicalJson(readback) !== canonicalJson(evidence.cleanupAttempt)) fail("cleanup_proof_readback_failed");
          proofSaved = true;
        },
        clear: async () => fail("proof_phase_cannot_clear_journal"),
      };
      return operation(guarded);
    }),
  };
}

function safeError(error: unknown): Record<string, unknown> {
  const result: Record<string, unknown> = { name: error instanceof Error ? error.name : "Error" };
  for (const key of ["remoteCode", "reason", "kind", "status"] as const) {
    const value = error && typeof error === "object" ? Object.getOwnPropertyDescriptor(error, key)?.value : undefined;
    if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 599 ||
      typeof value === "string" && /^[a-z_]{1,64}$/.test(value)) result[key] = value;
  }
  return result;
}

async function main(): Promise<void> {
  resolveRetainedWorker3CleanupAuthorization(process.argv.slice(2), process.env);
  const lease = new FileLease({ file: path.join(ROOT, "ownership.cleanup.lock"), staleAfterMs: 10 * 60_000 });
  await lease.acquire({ timeoutMs: 1_000 });
  let receiptHash = "";
  const receipt: Record<string, unknown> = {
    schema: "ghostprov.capacity-retained-cleanup", version: 1, proofId: "proof-35WVDV", worker: 3,
    projectId: PROJECT, title: TITLE, startedAt: new Date().toISOString(), success: false,
    proofAttempts: 0, supervisorLaunches: 0, providerRequests: 0, projectCreates: 0,
    workspaceAcquisitions: 0, projectDeleteAttempts: 0, durableCleanupProof: false,
    artifactsFinalized: false, journalCleared: false, deletionVerified: false,
  };
  const saveReceipt = async (): Promise<void> => { receiptHash = await replaceReceipt(receipt, receiptHash); };
  try {
    const evidence = await audit();
    await assertWriterEnded();
    await writeOnce(RECEIPT_NAME, receipt);
    receiptHash = sha256(JSON.stringify(receipt, null, 2));
    const cookieHash = EXPECTED["candidate-cookies.json"];
    let phase: "identity" | "proof" | "acquire" | "provision" | "artifacts" | "delete" = "identity";
    let requests = 0, mintCalls = 0, acquisitions = 0, deletes = 0;
    let freshSandbox: any = null;
    const account = await PrismAccountSession.create({ origin: ORIGIN,
      cookieFile: path.join(ROOT, "candidate-cookies.json"), fetch: async (input, init) => {
        const url = new URL(String(input)), method = (init?.method ?? "GET").toUpperCase();
        const headers = new Headers(init?.headers);
        let allowed = false;
        if (url.origin === ORIGIN && url.pathname === `/api/projects/${PROJECT}` && !url.search && !url.hash && init?.body == null) {
          allowed = method === "GET" && (phase === "identity" || phase === "delete") ||
            method === "DELETE" && phase === "delete" && ++deletes === 1;
        } else if (url.origin === ORIGIN && url.pathname === "/api/y" && method === "POST" && !url.search && !url.hash &&
          typeof init?.body === "string") {
          allowed = (phase === "proof" || phase === "provision" || phase === "artifacts") &&
            JSON.parse(init.body).docId === PROJECT;
        } else if (url.origin === ORIGIN && url.pathname === "/api/backend/1/new" && method === "POST" &&
          !url.search && !url.hash && init?.body == null) {
          allowed = phase === "acquire" && ++acquisitions === 1;
        } else if (freshSandbox && url.origin === ORIGIN &&
          url.pathname === `/api/projects/${PROJECT}/sandbox/resources-token` && method === "POST" && typeof init?.body === "string") {
          allowed = phase === "provision" && JSON.parse(init.body).sandbox_session_id === (freshSandbox.sessionId ?? null);
        } else if (freshSandbox && url.origin + url.pathname === `${freshSandbox.url}resources-token` &&
          method === "POST" && typeof init?.body === "string") {
          const body = JSON.parse(init.body);
          allowed = phase === "provision" && headers.get("x-crixet-sandbox-token") === freshSandbox.token.reveal() &&
            body.projectId === PROJECT;
        } else if (freshSandbox && url.origin + url.pathname === `${freshSandbox.url}token` &&
          method === "POST" && typeof init?.body === "string") {
          allowed = phase === "provision" && headers.get("x-crixet-sandbox-token") === freshSandbox.token.reveal() &&
            JSON.parse(init.body).docId === PROJECT;
        } else if (freshSandbox && url.origin + url.pathname === `${freshSandbox.url}wait-for-sync` &&
          method === "GET" && init?.body == null && !url.hash &&
          (!url.search || [...url.searchParams.keys()].join(",") === "prism_cache_bust")) {
          allowed = (phase === "provision" || phase === "artifacts") &&
            headers.get("x-crixet-sandbox-token") === freshSandbox.token.reveal();
        }
        if (!allowed || ++requests > 2_048) fail("http_scope_or_bound");
        const response = await fetch(url, { ...init, redirect: "manual" });
        const reader = response.body?.getReader(), chunks: Uint8Array[] = [];
        let total = 0, empty = 0;
        try {
          if (reader) for (;;) {
            const next = await reader.read();
            if (next.done) break;
            total += next.value.byteLength;
            empty = next.value.byteLength === 0 ? empty + 1 : 0;
            if (total > 64 * 1024 || chunks.length >= 4_096 || empty > 16) fail("http_body_bound");
            chunks.push(next.value);
          }
        } finally { await reader?.cancel().catch(() => undefined); }
        return new Response([204, 205, 304].includes(response.status) ? null : Buffer.concat(chunks, total),
          { status: response.status, headers: response.headers });
      } });
    const candidate = await account.createCookieCandidate();
    const provisioning = new PrismProvisioningClient({ account: candidate, requestTimeoutMs: 15_000 });
    const projects = new PrismProjectsClient(candidate);
    const beforeProof = await projects.get(PROJECT, AbortSignal.timeout(15_000));
    if (beforeProof.projectId !== PROJECT || beforeProof.title !== TITLE || beforeProof.deleted) fail("project_identity_before_proof");
    phase = "proof";
    let hosts = 0, shutdowns = 0, cleanupCallbacks = 0;
    receipt.proofAttempts = 1;
    await saveReceipt();
    const proofSignal = AbortSignal.timeout(180_000);
    const proven = await proveBlockedSupervisorBootstrapCleanup({
      worker: evidence.worker,
      assets: evidence.assets,
      bootstrapJournal: guardedJournal(evidence),
      recoverUnacceptedInitial: true,
      connectTimeoutMs: 90_000,
      commandTimeoutMs: 75_000,
      getYSweet: async () => {
        if (++mintCalls !== 1) fail("proof_credential_repeated");
        return provisioning.mintYSweet(PROJECT, { source: "held-bootstrap-cleanup", bootstrapAttempt: 1,
          previouslyConnected: true }, proofSignal);
      },
      getArtifacts: async () => fail("artifacts_before_cleanup_proof"),
      hostFactory: input => {
        if (++hosts !== 1 || !input.recoverExisting || !input.recoverUnacceptedInitial ||
          !sameSupervisorBinding(input.binding, evidence.attempt.binding) || input.instanceId !== evidence.instanceId) {
          fail("cleanup_host_authority_conflict");
        }
        const host = new V2SupervisorHost(input), shutdown = host.shutdown.bind(host);
        host.shutdown = async (reason, signal) => {
          if (++shutdowns !== 1) fail("cleanup_shutdown_repeated");
          await shutdown(reason, signal);
        };
        return host;
      },
      onCleanupRequired: binding => { if (!sameSupervisorBinding(binding, evidence.attempt.binding)) fail("cleanup_required_binding"); },
      onCleanupProven: binding => {
        if (!sameSupervisorBinding(binding, evidence.attempt.binding) || ++cleanupCallbacks !== 1) fail("cleanup_proof_binding");
      },
    }, proofSignal);
    if (!proven || hosts !== 1 || shutdowns !== 1 || cleanupCallbacks !== 1) fail("cleanup_proof_incomplete");
    receipt.durableCleanupProof = true;
    await saveReceipt();

    // Prism project identity does not identify a sandbox. The retained sandbox
    // is never reacquired or treated as this workspace. Only after its exact
    // supervisor has returned signed retirement proof do we create one distinct
    // disposable sandbox to synchronize project-owned tombstones before deleting
    // the test project.
    if (!RETAINED_WORKER3_CLEANUP_PLAN.fullCleanupEligible) fail("full_cleanup_not_authorized");
    const journal = new FileSupervisorBootstrapJournal(path.join(ROOT, "fleet.json.bootstrap"), PROJECT);
    await journal.withLease(AbortSignal.timeout(210_000), async entry => {
      const current = await entry.load();
      if (!current || canonicalJson(current) !== canonicalJson(evidence.cleanupAttempt)) fail("cleanup_proof_changed");
      const cleanupAttempt = current as SupervisorBootstrapAttempt;
      phase = "acquire";
      freshSandbox = await provisioning.acquireSandbox(AbortSignal.timeout(180_000), { maxAttempts: 1 });
      if (acquisitions !== RETAINED_WORKER3_CLEANUP_PLAN.workspaceAcquisitions ||
        freshSandbox.sessionId !== null && freshSandbox.sessionId === cleanupAttempt.binding.sandboxSessionId) {
        fail("cleanup_workspace_identity_conflict");
      }
      receipt.workspaceAcquisitions = acquisitions;
      receipt.cleanupWorkspaceMode = RETAINED_WORKER3_CLEANUP_PLAN.cleanupWorkspaceMode;
      receipt.reusedRetainedSandbox = false;
      phase = "provision";
      const credentials = await provisioning.provisionAcquired(PROJECT, freshSandbox, AbortSignal.timeout(160_000));
      if (credentials.projectId !== PROJECT || credentials.ySweet.docId !== PROJECT || credentials.sandbox !== freshSandbox) {
        fail("fresh_workspace_binding_conflict");
      }
      phase = "artifacts";
      const gateway = new SandboxGateway({ projectId: PROJECT, account: candidate, sandbox: freshSandbox,
        requestTimeoutMs: 15_000, responseBodyLimitBytes: 64 * 1024 });
      await gateway.syncWorkspace();
      const signal = AbortSignal.timeout(120_000);
      let firstCredential = true;
      const ydoc = new SandboxYDoc({ projectId: PROJECT, syncTimeoutMs: 20_000, mintYSweet: () => {
        if (firstCredential) { firstCredential = false; return Promise.resolve(credentials.ySweet); }
        return provisioning.mintYSweet(PROJECT, { source: "held-bootstrap-artifact-cleanup", bootstrapAttempt: 1,
          previouslyConnected: true }, signal);
      } });
      try {
        const generated = supervisorCleanupFiles({ entrypointPath: evidence.assets.entrypointPath,
          supervisorNonce: cleanupAttempt.binding.nonce });
        const expected = [
          { path: DIRECT_SUPERVISOR_LAUNCH_PATH, content: generated.launcher },
          { path: DIRECT_SUPERVISOR_INSTALL_PATH, content: generated.installer },
          { path: SUPERVISOR_CREDENTIAL_PATH, content: JSON.stringify(cleanupAttempt.sealed) },
        ];
        const staticValue = await ydoc.readPath(SUPERVISOR_STATIC_PATH);
        const observed = await Promise.all(expected.map(async file => ({ ...file, current: await ydoc.readPath(file.path) })));
        if (!observed.every(file => file.current === null)) {
          const staticExpected = supervisorStaticConfig({ binding: cleanupAttempt.binding,
            entrypointPath: evidence.assets.entrypointPath, connectTimeoutMs: cleanupAttempt.connectTimeoutMs });
          if (staticValue !== staticExpected || observed.some(file => file.current !== null && file.current !== file.content)) {
            fail("artifact_owner_conflict");
          }
          signal.throwIfAborted();
          const changed = await ydoc.compareAndMutatePaths({
            guards: [{ path: SUPERVISOR_STATIC_PATH, equals: staticValue },
              ...observed.map(file => ({ path: file.path, equals: file.current }))],
            mutations: observed.filter(file => file.current !== null)
              .map(file => ({ kind: "tombstone" as const, path: file.path })),
          });
          if (!changed) fail("artifact_changed_during_cleanup");
        }
        await gateway.syncWorkspace();
        signal.throwIfAborted();
        if ((await Promise.all(expected.map(file => ydoc.readPath(file.path)))).some(value => value !== null)) {
          fail("artifact_cleanup_readback_failed");
        }
        receipt.artifactsFinalized = true;
        await saveReceipt();
        await entry.clear();
        if (await entry.load() !== null) fail("journal_clear_readback_failed");
        receipt.journalCleared = true;
        await saveReceipt();
      } finally { await ydoc.destroy(); }
    });

    phase = "delete";
    const exact = await projects.get(PROJECT, AbortSignal.timeout(15_000));
    if (exact.projectId !== PROJECT || exact.title !== TITLE || exact.deleted) fail("project_identity_before_delete");
    receipt.projectDeleteAttempts = 1;
    receipt.deleteSubmitted = true;
    await saveReceipt();
    try { await projects.deleteManaged(PROJECT, AbortSignal.timeout(15_000)); receipt.deleteAccepted = true; }
    catch { receipt.deleteAccepted = false; }
    try {
      const after = await projects.get(PROJECT, AbortSignal.timeout(15_000));
      if (after.projectId !== PROJECT || after.title !== TITLE || !after.deleted) fail("project_delete_unproven");
      receipt.deletionProof = "exact_deleted_record";
    } catch (error) {
      if (!(error instanceof PrismProjectError) || error.status !== 404) throw error;
      receipt.deletionProof = "http_404_after_exact_identity";
    }
    receipt.deletionVerified = true;
    const ownership = { ...evidence.ownership, deleted: [...evidence.ownership.deleted, PROJECT] };
    const ownershipText = JSON.stringify(ownership, null, 2);
    const ownershipFile = path.join(ROOT, "ownership.json"), temporary = `${ownershipFile}.${randomUUID()}.tmp`;
    if (sha256(await stableBytes("ownership.json")) !== EXPECTED["ownership.json"]) fail("ownership_changed_before_update");
    const handle = await open(temporary, "wx", 0o600);
    try { await handle.writeFile(ownershipText); await handle.sync(); }
    finally { await handle.close(); }
    if (sha256(await stableBytes("ownership.json")) !== EXPECTED["ownership.json"]) fail("ownership_changed_before_update");
    await rename(temporary, ownershipFile);
    receipt.ownershipUpdated = true;
    if (acquisitions !== RETAINED_WORKER3_CLEANUP_PLAN.workspaceAcquisitions ||
      deletes !== RETAINED_WORKER3_CLEANUP_PLAN.projectDeleteAttempts) fail("cleanup_operation_count_conflict");
    receipt.success = true;
    receipt.completedAt = new Date().toISOString();
    await saveReceipt();
    if (sha256(await stableBytes("report.json")) !== EXPECTED["report.json"] ||
      sha256(await stableBytes("candidate-cookies.json")) !== cookieHash) fail("preserved_evidence_changed");
    console.log(JSON.stringify({ success: true, receipt: path.join(ROOT, RECEIPT_NAME), projectId: PROJECT }));
  } catch (error) {
    receipt.error = safeError(error);
    receipt.completedAt = new Date().toISOString();
    if (receiptHash) await saveReceipt().catch(() => undefined);
    throw error;
  } finally { await lease.release(); }
}

if (import.meta.main) {
  main().catch(error => {
    console.error(JSON.stringify({ success: false, error: safeError(error) }));
    process.exitCode = 1;
  });
}
