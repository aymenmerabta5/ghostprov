import { copyFile, mkdir, mkdtemp, open, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { closeSync, createWriteStream, fsyncSync, openSync, renameSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { types as utilTypes } from "node:util";
import path from "node:path";
import * as decoding from "lib0/decoding";
import type { WebsocketProvider } from "y-websocket";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { FleetProjectsClient } from "../src/fleet/manager";
import type { LoggerLike } from "../src/observability/logger";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { REQUEST_FAILURE_KINDS } from "../src/observability/request-error";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { isRuntimeHealthy } from "../src/router/policy";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import { createYSweetAwarenessProvider, YSweetAwarenessChannel } from "../src/v2/awareness/y-sweet-channel";
import { GP_V2_R2_SCHEMA_DESCRIPTOR, type HostFrame, type R2RequestStartFrame, type SidecarFrame } from "../src/v2/protocol/shared";
import { SANDBOX_LOG_FILES, parseDiagnosticLine, type SandboxLogFile } from "../src/v2/diagnostics/sandbox-log-schema";
import { decodeSandboxLogResponse, sandboxLogRelativePath } from "../src/v2/diagnostics/read-sandbox-log";
import type { V2BootstrapGateway } from "../src/v2/bootstrap/controller";
import type { BootstrapBinding } from "../src/v2/bootstrap/crypto";
import { FileSupervisorBootstrapJournal } from "../src/v2/supervisor/bootstrap-journal";
import { assertBootstrapCleanupComplete } from "../src/v2/supervisor/bootstrap-cleanup-proof";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { V2SupervisorHost, type V2BootstrapSupervisor } from "../src/v2/supervisor/host";
import { sameSupervisorBinding } from "../src/v2/supervisor/protocol";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";
import { checkR10Sidecar } from "./build_v2_sidecar_r10";
import { runWorkload, type WorkloadSummary } from "./lib/capacity-workload";
import { StartupArtifactCapture, createStartupArtifactExpectations, type StartupArtifactWorker } from "./lib/startup-artifact-capture";

const PORT = 18_888, BASE = `http://127.0.0.1:${PORT}`;
const TOTAL_MS = 45 * 60_000, CLEANUP_RESERVE_MS = 5 * 60_000;
const MAX_CHANNELS = 128, MAX_SUPERVISORS = 128;
const MAX_TRACE_BYTES = 192 * 1024 * 1024, MAX_BUFFER_BYTES = 2 * 1024 * 1024;
const MAX_DIAGNOSTIC_BYTES = 8 * 1024 * 1024;
class ProofFailure extends Error { constructor(readonly code: string) { super(code); } }
export { ProofFailure as CapacityProofFailure };
const object = (value: unknown): Record<string, unknown> | null => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
const safeInteger = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const count = (value: number, amount = 1) => Math.min(Number.MAX_SAFE_INTEGER, value + Math.max(0, amount));
const numeric = (value: unknown): number | null => typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.min(Number.MAX_SAFE_INTEGER, value) : null;
const safeFailure = (error: unknown) => error instanceof ProofFailure ? error.code : "external_operation_failed";

/** Resolves authorization before reading credentials, touching listeners, or opening projects. */
export function resolveCapacityProofOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_CAPACITY_RELIABILITY !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new ProofFailure("explicit_live_and_test_project_guards_required");
  }
  if (env.PORT !== "18888") throw new ProofFailure("explicit_candidate_port_18888_required");
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new ProofFailure("explicit_cookie_file_required");
  const projects = env.GP_LIVE_CAPACITY_PROJECTS ?? "1", cycles = env.GP_LIVE_CAPACITY_CYCLES ?? "1";
  const rootBytes = env.GP_LIVE_CAPACITY_ROOT_BYTES ?? "1048576";
  const stability = env.GP_LIVE_CAPACITY_STABILITY_SECONDS ?? "120";
  if (projects !== "1" && projects !== "8") throw new ProofFailure("project_count_must_be_1_or_8");
  if (cycles !== "1" && cycles !== "2") throw new ProofFailure("cycle_count_must_be_1_or_2");
  const faults = env.GP_LIVE_CAPACITY_FAULTS ?? "0";
  if ((faults !== "0" && faults !== "1") || (faults === "1" && projects !== "1")) throw new ProofFailure("fault_proof_requires_one_project");
  const startupArtifacts = env.GP_LIVE_CAPACITY_STARTUP_ARTIFACTS ?? "0";
  if ((startupArtifacts !== "0" && startupArtifacts !== "1") || (startupArtifacts === "1" && projects !== "8")) {
    throw new ProofFailure("startup_artifact_capture_requires_eight_projects");
  }
  if (rootBytes !== "1048576" && rootBytes !== "262144") throw new ProofFailure("root_bytes_must_be_1mib_or_256kib");
  if (!/^(120|1[2-9][0-9]|[2-5][0-9]{2}|600)$(?![\s\S])/.test(stability)) throw new ProofFailure("stability_seconds_must_be_120_to_600");
  if (env.GP_LIVE_MODEL !== undefined && env.GP_LIVE_MODEL !== "gpt-6-astra") throw new ProofFailure("astra_model_required");
  if (env.GP_LIVE_REASONING_EFFORT !== undefined && env.GP_LIVE_REASONING_EFFORT !== "xhigh") throw new ProofFailure("xhigh_reasoning_required");
  const origin = new URL(env.PRISM_ORIGIN ?? "https://prism.openai.com");
  if (origin.protocol !== "https:" || origin.username || origin.password || origin.pathname !== "/" || origin.search || origin.hash) {
    throw new ProofFailure("invalid_prism_origin");
  }
  return { cookieFile: path.resolve(cookieFile), origin: origin.origin, projects: Number(projects) as 1 | 8,
    cycles: Number(cycles) as 1 | 2, rootBytes: Number(rootBytes) as 262144 | 1048576,
    stabilityMs: Number(stability) * 1000, faults: faults === "1", startupArtifacts: startupArtifacts === "1",
    outputRoot: path.resolve(env.GP_LIVE_CAPACITY_OUTPUT_ROOT ?? ".ghostprov/live-capacity-reliability") };
}

/** Separate authorization/success scope; the reliability entrypoint still accepts only 1 or 8 projects. */
export function resolveStartupArtifactDiagnosticOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_STARTUP_ARTIFACT_DIAGNOSTIC !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new ProofFailure("explicit_startup_artifact_and_project_guards_required");
  }
  if (env.GP_LIVE_CAPACITY_PROJECTS !== undefined && env.GP_LIVE_CAPACITY_PROJECTS !== "2") throw new ProofFailure("startup_diagnostic_requires_exactly_two_projects");
  if (env.GP_LIVE_CAPACITY_CYCLES !== undefined && env.GP_LIVE_CAPACITY_CYCLES !== "1") throw new ProofFailure("startup_diagnostic_requires_one_cycle");
  if (env.GP_LIVE_CAPACITY_FAULTS !== undefined && env.GP_LIVE_CAPACITY_FAULTS !== "0") throw new ProofFailure("startup_diagnostic_forbids_fault_workload");
  if (env.GP_LIVE_CAPACITY_STARTUP_ARTIFACTS !== undefined && env.GP_LIVE_CAPACITY_STARTUP_ARTIFACTS !== "0") throw new ProofFailure("startup_diagnostic_has_its_own_capture_scope");
  const sourceLabel = env.GP_STARTUP_ARTIFACT_SOURCE_LABEL;
  if (!sourceLabel || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,79}$(?![\s\S])/.test(sourceLabel)) throw new ProofFailure("explicit_source_label_required");
  const base = resolveCapacityProofOptions({ ...env, GP_LIVE_CAPACITY_RELIABILITY: "1", GP_LIVE_CAPACITY_PROJECTS: "1",
    GP_LIVE_CAPACITY_CYCLES: "1", GP_LIVE_CAPACITY_FAULTS: "0" });
  return { ...base, projects: 2 as const, cycles: 1 as const, faults: false, scope: "startup_artifact_diagnostic" as const, sourceLabel };
}

type ProofRunOptions = Omit<ReturnType<typeof resolveCapacityProofOptions>, "projects"> & {
  readonly projects: 1 | 2 | 8;
  readonly scope: "capacity_reliability" | "startup_artifact_diagnostic";
  readonly sourceLabel: string;
};

export function startupArtifactCapturePlan(scope: ProofRunOptions["scope"], enabled: boolean) {
  const workers: readonly number[] = scope === "startup_artifact_diagnostic" ? [1, 2] : enabled ? [1, 8] : [];
  return Object.freeze({ cycle: 1, workers: Object.freeze(workers), expectedSessions: workers.length, expectedRows: workers.length * 12 });
}

/** For diagnostic ordering only; this index never changes health, routing, or cleanup authority. */
export class CapacityDiagnosticFailures {
  readonly #owned: ReadonlySet<string>;
  readonly #failed = new Set<string>();
  constructor(projects: readonly string[]) {
    if (projects.length < 1 || projects.length > 8 || new Set(projects).size !== projects.length ||
      projects.some(project => typeof project !== "string" || !project || project.length > 128)) {
      throw new ProofFailure("diagnostic_project_index_bound");
    }
    this.#owned = new Set(projects);
  }
  observe(event: unknown): void {
    const value = object(event), project = value?.project_id;
    if (typeof project !== "string" || !this.#owned.has(project)) return;
    if (value?.event === "request.failed" || value?.event === "sandbox.recovery.retry_scheduled" ||
      value?.event === "sandbox.recovery.started" && typeof value.cause === "string" && value.cause !== "startup") {
      this.#failed.add(project);
    }
  }
  has(project: string): boolean { return this.#failed.has(project); }
  get size(): number { return this.#failed.size; }
  prioritize<T extends { readonly projectId: string; readonly closed: boolean }>(records: readonly T[]): T[] {
    return [...records].sort((left, right) => Number(this.has(right.projectId)) - Number(this.has(left.projectId)) ||
      Number(right.closed) - Number(left.closed));
  }
}

/** An aborted create can still succeed remotely after a GET has reported absence. */
export function capacityAbsenceProvesDeletion(status: unknown, identityPreviouslyObserved: boolean): boolean {
  return identityPreviouslyObserved && (status === 404 || status === 410);
}

export interface CapacityPrivateBackendRecord {
  readonly index: number;
  readonly backendUrl: string;
  readonly sandboxToken: string;
  readonly sandboxSessionId: string | null;
  readonly projectId: string | null;
  readonly executions: readonly { readonly sessionId: string; readonly nonce: string }[];
}

/** Private authority only: exact response and request fields, never timing-based association. */
export class CapacityBackendEvidence {
  readonly #records: Array<{ value: CapacityPrivateBackendRecord; persisted: Promise<void> }> = [];
  readonly #resourceSessions = new Map<string, Set<string | null>>();
  readonly #commands = new Map<string, { projectId: string; nonce: string }>();
  #sessionBindings = 0;
  constructor(readonly owned: ReadonlySet<string>, readonly persist: (record: CapacityPrivateBackendRecord) => Promise<void>) {}
  async acquired(raw: unknown): Promise<void> {
    const value = object(raw);
    if (this.#records.length >= MAX_SUPERVISORS || typeof value?.url !== "string" || value.url.length > 2048 ||
      typeof value.token !== "string" || !value.token || value.token.length > 8192 || /[\r\n]/.test(value.token)) throw new ProofFailure("backend_evidence_bound_or_shape");
    const url = new URL(value.url);
    if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash) throw new ProofFailure("backend_evidence_url_invalid");
    const session = value.sandbox_session_id ?? null;
    if (session !== null && (typeof session !== "string" || !session || session.length > 512 || /[\r\n]/.test(session))) throw new ProofFailure("backend_evidence_session_invalid");
    const record = { value: Object.freeze({ index: this.#records.length + 1, backendUrl: url.href.endsWith("/") ? url.href : `${url.href}/`,
      sandboxToken: value.token, sandboxSessionId: session, projectId: null, executions: [] }) as CapacityPrivateBackendRecord, persisted: Promise.resolve() };
    this.#records.push(record); await this.#save(record);
  }
  resourceRequest(project: string, raw: unknown): void {
    const value = object(raw), session = value?.sandbox_session_id;
    if (!this.owned.has(project) || !value || Object.keys(value).length !== 1 ||
      (session !== null && (typeof session !== "string" || !session || session.length > 512))) throw new ProofFailure("resource_evidence_identity_invalid");
    let sessions = this.#resourceSessions.get(project);
    if (!sessions) { sessions = new Set(); this.#resourceSessions.set(project, sessions); }
    if (!sessions.has(session as string | null)) {
      if (++this.#sessionBindings > MAX_SUPERVISORS) throw new ProofFailure("resource_evidence_bound");
      sessions.add(session as string | null);
    }
  }
  async delivered(url: string, headers: Headers, raw: unknown): Promise<void> {
    const project = object(raw)?.projectId;
    if (typeof project !== "string" || !this.owned.has(project)) throw new ProofFailure("backend_delivery_outside_test_ledger");
    const matching = this.#matching(url, "resources-token", headers).filter(record => this.#resourceSessions.get(project)?.has(record.value.sandboxSessionId));
    const identities = new Set(matching.map(record => record.value.sandboxSessionId));
    if (!matching.length || identities.size !== 1 || matching.some(record => record.value.projectId !== null && record.value.projectId !== project)) throw new ProofFailure("backend_delivery_binding_ambiguous");
    for (const record of matching) {
      if (record.value.projectId === null) { record.value = Object.freeze({ ...record.value, projectId: project }); await this.#save(record); }
      else await record.persisted;
    }
  }
  registerCommand(project: string, nonce: string, sessionId: string): void {
    if (!this.owned.has(project) || this.#commands.size >= MAX_SUPERVISORS || this.#commands.has(sessionId) ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/.test(sessionId) ||
      !nonce || nonce.length > 256) throw new ProofFailure("command_evidence_bound_or_identity");
    this.#commands.set(sessionId, { projectId: project, nonce });
  }
  async command(url: string, headers: Headers, raw: unknown): Promise<void> {
    const sessionId = object(raw)?.session_id;
    const command = typeof sessionId === "string" ? this.#commands.get(sessionId) : undefined;
    if (!command || typeof sessionId !== "string") throw new ProofFailure("command_evidence_unregistered");
    const matching = this.#matching(url, "codex/codex_exec_async", headers).filter(record => record.value.projectId === command.projectId);
    if (!matching.length || new Set(matching.map(record => record.value.sandboxSessionId)).size !== 1) throw new ProofFailure("command_backend_binding_ambiguous");
    for (const record of matching) {
      if (record.value.executions.some(execution => execution.sessionId === sessionId)) throw new ProofFailure("command_evidence_replay");
      record.value = Object.freeze({ ...record.value, executions: Object.freeze([...record.value.executions, { sessionId, nonce: command.nonce }]) });
      await this.#save(record);
    }
  }
  #matching(rawUrl: string, suffix: string, headers: Headers) {
    const url = new URL(rawUrl); url.search = ""; url.hash = "";
    const token = headers.get("x-crixet-sandbox-token");
    if (!token) throw new ProofFailure("backend_evidence_token_missing");
    return this.#records.filter(record => `${record.value.backendUrl}${suffix}` === url.href && record.value.sandboxToken === token);
  }
  async #save(record: { value: CapacityPrivateBackendRecord; persisted: Promise<void> }): Promise<void> {
    const value = record.value;
    if (Buffer.byteLength(JSON.stringify(value)) > 16 * 1024) throw new ProofFailure("backend_evidence_record_bound");
    record.persisted = record.persisted.then(() => this.persist(value)); await record.persisted;
  }
  snapshot() { return { acquired: this.#records.length, associated: this.#records.filter(record => record.value.projectId !== null).length,
    commandSessions: this.#commands.size, resourceBindings: this.#sessionBindings }; }
}

export function capacityProjectMayDelete(input: { cleanupStarted: boolean; controllersClosed: boolean; unprovedShutdowns: number; journalEmpty: boolean; evidencePersisted: boolean }): boolean {
  return input.cleanupStarted && input.controllersClosed && input.unprovedShutdowns === 0 && input.journalEmpty && input.evidencePersisted;
}

/** The original gateway survives controller closure; this never acquires a replacement backend. */
export async function readCapacityExecutionDiagnostic(gateway: Pick<V2BootstrapGateway, "readDiagnosticFile">, project: string,
  execution: { readonly sessionId: string; readonly binding: { readonly projectId: string } }, file: SandboxLogFile, signal: AbortSignal): Promise<string | null> {
  if (execution.binding.projectId !== project || !gateway.readDiagnosticFile) throw new ProofFailure("execution_gateway_identity_missing");
  sandboxLogRelativePath(execution.sessionId, file);
  const response = await gateway.readDiagnosticFile(execution.sessionId, file, signal);
  if (response.status === 404) return null;
  if (response.status !== 200) throw new ProofFailure("execution_diagnostic_status");
  return decodeSandboxLogResponse(response.value, { sessionId: execution.sessionId, file });
}

/** A fulfilled controller shutdown can be a no-op; the exact host's signed reply is required. */
export class CapacitySupervisorProof {
  readonly #records = new Map<string, { binding: Readonly<BootstrapBinding>; submitted: boolean; signed: boolean }>();
  readonly #latest = new Map<string, string>();
  register(input: BootstrapBinding): void {
    const binding = Object.freeze({ ...input });
    const { projectId: project, nonce } = binding;
    if (this.#records.has(nonce) || this.#records.size >= MAX_SUPERVISORS) throw new ProofFailure("supervisor_identity_bound_or_conflict");
    this.#records.set(nonce, { binding, submitted: false, signed: false });
  }
  submit(project: string, nonce: string): void {
    const record = this.#records.get(nonce), previous = this.#latest.get(project);
    if (!record || record.binding.projectId !== project || record.submitted || record.signed || (previous && !this.#records.get(previous)?.signed)) {
      throw new ProofFailure("bootstrap_submission_replay_forbidden");
    }
    record.submitted = true; this.#latest.set(project, nonce);
  }
  signedShutdown(project: string, nonce: string): void {
    this.assertPendingShutdown(project, nonce);
    const record = this.#records.get(nonce);
    record!.signed = true;
  }
  shutdownState(binding: BootstrapBinding): "pending" | "signed" {
    const record = this.#records.get(binding.nonce);
    let exact = false;
    try { exact = !!record && sameSupervisorBinding(record.binding, binding); }
    catch { /* Malformed or cross-revision bindings are identity conflicts. */ }
    if (!record || !exact || !record.submitted) {
      throw new ProofFailure("shutdown_proof_identity_conflict");
    }
    return record.signed ? "signed" : "pending";
  }
  assertPendingShutdown(project: string, nonce: string): void {
    const record = this.#records.get(nonce);
    if (!record || record.binding.projectId !== project || !record.submitted || record.signed) {
      throw new ProofFailure("shutdown_proof_identity_conflict");
    }
  }
  commitSignedShutdown(project: string, nonce: string, persist: () => void): void {
    this.assertPendingShutdown(project, nonce);
    const record = this.#records.get(nonce)!;
    record.signed = true;
    try { persist(); }
    catch (error) { record.signed = false; throw error; }
  }
  snapshot() {
    const records = [...this.#records.values()].filter(record => record.submitted);
    return { submitted: records.length, signedShutdowns: records.filter(record => record.signed).length,
      unprovedShutdowns: records.filter(record => !record.signed).length };
  }
  recordsForProject(project: string) {
    return [...this.#records.entries()].filter(([, record]) => record.binding.projectId === project)
      .map(([nonce, record]) => ({ nonce, submitted: record.submitted, signedShutdown: record.signed }));
  }
  projectSnapshot(project: string) {
    const records = this.recordsForProject(project).filter(record => record.submitted);
    return { submitted: records.length, signedShutdowns: records.filter(record => record.signedShutdown).length,
      unprovedShutdowns: records.filter(record => !record.signedShutdown).length };
  }
}

/** Records only an authenticated shutdown fulfilled by the exact registered host binding. */
export function accountCapacitySupervisorShutdown<T extends Pick<V2BootstrapSupervisor, "binding" | "shutdown">>(
  host: T,
  proof: CapacitySupervisorProof,
  persist: () => void,
): T {
  const shutdown = host.shutdown.bind(host);
  let remoteProven = false;
  let operation: Promise<void> | null = null;
  host.shutdown = (reason, signal) => {
    if (operation) return operation;
    const current = (async () => {
      // Keep identity validation inside the promise contract exposed by
      // V2BootstrapSupervisor.shutdown. A conflicting recovered host must
      // reject before touching the remote supervisor without throwing from the
      // call site synchronously.
      const state = proof.shutdownState(host.binding);
      if (!remoteProven) {
        await shutdown(reason, signal);
        remoteProven = true;
      }
      if (state === "pending") proof.commitSignedShutdown(host.binding.projectId, host.binding.nonce, persist);
    })();
    operation = current;
    void current.finally(() => { if (operation === current) operation = null; }).catch(() => undefined);
    return current;
  };
  return host;
}

type WireTerminal = Extract<SidecarFrame, { kind: "response_terminal" }>;
interface ExecutionRecord {
  readonly lane: number; readonly turn: number; readonly digest: string;
  requestId: string | null; fence: string | null; project: string | null;
  responseId: string | null; startPublications: number; accepted: boolean;
}
/** One hundred expected digests; exact wire identities stay in memory and are never logged. */
export class CapacityRequestProof {
  readonly #byDigest = new Map<string, ExecutionRecord>();
  readonly #byRequest = new Map<string, ExecutionRecord>();
  readonly #responses = new Set<string>();
  constructor(readonly allResponses = new Set<string>()) {}
  tracksDigest(digest: string): boolean { return this.#byDigest.has(digest); }
  tracksRequest(requestId: string): boolean { return this.#byRequest.has(requestId); }
  begin(digest: string, lane: number, turn: number): void {
    if (this.#byDigest.size >= 100 || this.#byDigest.has(digest) || !/^[a-f0-9]{64}$(?![\s\S])/.test(digest) ||
      !Number.isInteger(lane) || lane < 0 || lane >= 10 || !Number.isInteger(turn) || turn < 0 || turn >= 10) throw new ProofFailure("request_proof_bound_or_conflict");
    this.#byDigest.set(digest, { digest, lane, turn, requestId: null, fence: null, project: null, responseId: null, startPublications: 0, accepted: false });
  }
  start(project: string, frame: R2RequestStartFrame): void {
    const record = this.#byDigest.get(frame.requestDigest);
    const fence = JSON.stringify([project, frame.projectId, frame.requestId, frame.sessionId, frame.leaderGeneration,
      frame.buildHash, frame.schemaHash, frame.launchNonce, frame.hostInstanceId, frame.targetInstanceId,
      frame.requestDigest, frame.decodedBytes, frame.encodedBytes, frame.encoding, frame.partCount]);
    if (!record || frame.projectId !== project || (record.fence !== null && record.fence !== fence) ||
      (this.#byRequest.has(frame.requestId) && this.#byRequest.get(frame.requestId) !== record)) throw new ProofFailure("request_execution_replayed_or_untracked");
    record.fence = fence; record.project = project; record.requestId = frame.requestId;
    this.#byRequest.set(frame.requestId, record);
    if (++record.startPublications > 1_000_000) throw new ProofFailure("request_publication_bound");
  }
  terminal(project: string, frame: WireTerminal): void {
    const record = this.#byRequest.get(frame.requestId);
    if (!record || record.project !== project || record.digest !== frame.requestDigest || frame.terminalStatus !== "completed" ||
      (record.responseId !== null && record.responseId !== frame.responseId)) throw new ProofFailure("wire_terminal_identity_mismatch");
    if (record.responseId === null) {
      if (this.#responses.has(frame.responseId) || this.allResponses.has(frame.responseId) || this.allResponses.size >= 204) throw new ProofFailure("duplicate_provider_terminal");
      record.responseId = frame.responseId; this.#responses.add(frame.responseId); this.allResponses.add(frame.responseId);
    }
  }
  complete(digest: string, responseId: string): void {
    const record = this.#byDigest.get(digest);
    if (!record || record.accepted || !record.responseId || record.responseId !== responseId) throw new ProofFailure("public_terminal_lacks_exact_wire_proof");
    record.accepted = true;
  }
  snapshot() {
    return { invoked: this.#byDigest.size, executionIdentities: this.#byRequest.size, providerTerminals: this.#responses.size,
      accepted: [...this.#byDigest.values()].filter(record => record.accepted).length,
      requests: [...this.#byDigest.values()].map(record => ({ lane: record.lane, turn: record.turn, startPublications: record.startPublications,
        terminalObserved: record.responseId !== null, accepted: record.accepted })) };
  }
}

/** At most 2 MiB queued and 192 MiB written, leaving room under a 256 MiB artifact budget. */
class TraceWriter {
  readonly stream;
  bytes = 0; records = 0; dropped = 0; errors = 0;
  constructor(file: string) {
    this.stream = createWriteStream(file, { flags: "wx", mode: 0o600, highWaterMark: MAX_BUFFER_BYTES });
    this.stream.on("error", () => { this.errors++; });
  }
  add(event: Record<string, unknown>): void {
    try {
      const line = JSON.stringify({ mono_ms: performance.now(), ...event }) + "\n", bytes = Buffer.byteLength(line);
      if (this.errors || bytes > 64 * 1024 || this.bytes + bytes > MAX_TRACE_BYTES || this.records >= 750_000 ||
        this.stream.writableLength + bytes > MAX_BUFFER_BYTES) { this.dropped++; return; }
      this.bytes += bytes; this.records++; this.stream.write(line);
    } catch { this.errors++; }
  }
  async close(): Promise<void> { await new Promise<void>(resolve => { this.stream.end(resolve); }); }
  snapshot() { return { bytes: this.bytes, records: this.records, dropped: this.dropped, errors: this.errors, bufferedBytes: this.stream.writableLength }; }
}

const MAX_FRAME_DIAGNOSTIC_ENTRIES = 64;
const FRAME_KINDS: ReadonlySet<string> = new Set([
  ...GP_V2_R2_SCHEMA_DESCRIPTOR.hostFrameKinds, ...GP_V2_R2_SCHEMA_DESCRIPTOR.sidecarFrameKinds,
]);
const PROVIDER_DIAGNOSTIC_CODES: ReadonlySet<string> = new Set([
  "invalid_json", "invalid_value", "invalid_request_error", "invalid_parameter", "unsupported_value",
  "unsupported_parameter", "missing_required_parameter", "model_not_found", "previous_response_not_found",
  "context_length_exceeded", "invalid_function_call_arguments", "cyber_policy", "rate_limit_exceeded",
  "insufficient_quota", "server_error", "upstream_error",
]);
const PROVIDER_DIAGNOSTIC_TYPES: ReadonlySet<string> = new Set([
  "invalid_request_error", "authentication_error", "permission_error", "rate_limit_error", "server_error", "upstream_error",
]);
const PROVIDER_DIAGNOSTIC_PARAMS: ReadonlySet<string> = new Set([
  "body", "input", "model", "previous_response_id", "stream", "store", "tools", "tool_choice",
  "reasoning", "reasoning.effort", "reasoning.summary", "max_output_tokens", "instructions", "include",
  "truncation", "parallel_tool_calls", "text", "text.format", "service_tier", "temperature", "top_p",
]);
const PROVIDER_DIAGNOSTIC_ORIGINS: ReadonlySet<string> = new Set(["http", "sse"]);
const PROVIDER_DIAGNOSTIC_FORMATS: ReadonlySet<string> = new Set(["json", "text"]);

function diagnosticOwnData(value: unknown, key: string): unknown {
  if (value === null || typeof value !== "object" || utilTypes.isProxy(value)) return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && "value" in descriptor ? descriptor.value : undefined;
  } catch { return undefined; }
}

function diagnosticRecord(value: unknown): object | null {
  return value !== null && typeof value === "object" && !utilTypes.isProxy(value) && !Array.isArray(value)
    ? value : null;
}

function diagnosticArrayLength(value: unknown): number {
  if (value === null || typeof value !== "object" || utilTypes.isProxy(value) || !Array.isArray(value)) return 0;
  const length = diagnosticOwnData(value, "length");
  return safeInteger(length) ? length : 0;
}

function diagnosticCategory(value: unknown, allowed: ReadonlySet<string>): string | null {
  if (value === undefined || value === null) return null;
  return typeof value === "string" && value.length <= 64 && allowed.has(value) ? value : "other";
}

function diagnosticBoolean(value: unknown): boolean | null {
  return typeof value === "boolean" ? value : null;
}

function frameErrorEvidence(frame: unknown, kind: "response_error" | "request_reject", sequence: number | null) {
  const provider = diagnosticRecord(diagnosticOwnData(frame, "providerError"));
  const error = diagnosticRecord(diagnosticOwnData(provider, "error"));
  const payload = diagnosticRecord(diagnosticOwnData(provider, "payload"));
  const status = diagnosticOwnData(provider, "status");
  return {
    sequence, kind, code: diagnosticCategory(diagnosticOwnData(frame, "code"), REQUEST_FAILURE_KINDS),
    ambiguous: diagnosticBoolean(diagnosticOwnData(frame, "ambiguous")),
    provider: diagnosticOwnData(provider, "version") === 1 ? {
      origin: diagnosticCategory(diagnosticOwnData(provider, "origin"), PROVIDER_DIAGNOSTIC_ORIGINS),
      status: typeof status === "number" && Number.isInteger(status) && status >= 400 && status <= 599 ? status : null,
      code: diagnosticCategory(diagnosticOwnData(error, "code"), PROVIDER_DIAGNOSTIC_CODES),
      type: diagnosticCategory(diagnosticOwnData(error, "type"), PROVIDER_DIAGNOSTIC_TYPES),
      param: diagnosticCategory(diagnosticOwnData(error, "param"), PROVIDER_DIAGNOSTIC_PARAMS),
      payloadFormat: diagnosticCategory(diagnosticOwnData(payload, "format"), PROVIDER_DIAGNOSTIC_FORMATS),
      payloadTruncated: diagnosticBoolean(diagnosticOwnData(payload, "truncated")),
    } : null,
  };
}

/** Fixed categories and numeric shape only. Never traverse provider text, IDs, headers or accessors. */
export function capacityEnvelopeMetadata(value: unknown) {
  const envelope = diagnosticRecord(value);
  const ack = diagnosticRecord(diagnosticOwnData(envelope, "ack")), frames = diagnosticOwnData(envelope, "frames");
  const frameCount = diagnosticArrayLength(frames), inspectedFrames = Math.min(frameCount, MAX_FRAME_DIAGNOSTIC_ENTRIES);
  const kinds: Record<string, number> = {};
  const errors: Array<ReturnType<typeof frameErrorEvidence>> = [];
  let minSequence: number | null = null, maxSequence: number | null = null, payloadChars = 0;
  let inspectedBatchItems = 0;
  for (let index = 0; index < inspectedFrames; index++) {
    const frame = diagnosticRecord(diagnosticOwnData(frames, String(index)));
    if (!frame) continue;
    const kind = diagnosticCategory(diagnosticOwnData(frame, "kind"), FRAME_KINDS) ?? "other";
    kinds[kind] = count(kinds[kind] ?? 0);
    const rawSequence = diagnosticOwnData(frame, "sequence"), sequence = safeInteger(rawSequence) ? rawSequence : null;
    if (sequence !== null) { minSequence = minSequence === null ? sequence : Math.min(minSequence, sequence); maxSequence = maxSequence === null ? sequence : Math.max(maxSequence, sequence); }
    if (kind === "response_error" || kind === "request_reject") errors.push(frameErrorEvidence(frame, kind, sequence));
    const data = diagnosticOwnData(frame, "dataBase64Url");
    if (typeof data === "string") payloadChars = count(payloadChars, data.length);
    if (kind === "response_batch") {
      const items = diagnosticOwnData(frame, "items");
      const inspect = Math.min(diagnosticArrayLength(items), MAX_FRAME_DIAGNOSTIC_ENTRIES - inspectedBatchItems);
      for (let itemIndex = 0; itemIndex < inspect; itemIndex++) {
        const itemData = diagnosticOwnData(diagnosticRecord(diagnosticOwnData(items, String(itemIndex))), "dataBase64Url");
        if (typeof itemData === "string") payloadChars = count(payloadChars, itemData.length);
      }
      inspectedBatchItems += inspect;
    }
  }
  const ackSequence = diagnosticOwnData(ack, "sequence");
  return { epoch: numeric(diagnosticOwnData(envelope, "epoch")), revision: numeric(diagnosticOwnData(envelope, "revision")),
    ackEpoch: numeric(diagnosticOwnData(ack, "epoch")),
    ackSequence: typeof ackSequence === "number" && Number.isSafeInteger(ackSequence) && ackSequence >= -1 ? ackSequence : null,
    frameCount, inspectedFrames, inspectedBatchItems, minSequence, maxSequence, payloadChars, kinds, errors };
}

type RawKind = "local" | "data" | "control" | "other" | "unknown";
const rawBucket = () => ({ entries: 0, stateBytes: 0, nullEntries: 0, greaterClock: 0, equalClock: 0, lowerClock: 0,
  updateEntries: 0, changeEntries: 0, lastClock: null as number | null, previousClock: null as number | null,
  lastStateBytes: 0, lastRawAtMs: null as number | null });

/** Observes the production provider; keeps only numeric headers and never retains state JSON. */
function observeProvider(provider: WebsocketProvider) {
  const stats = { messages: 0, bytes: 0, entries: 0, entryOverflow: 0, invalidHeaders: 0, handlerThrows: 0,
    updateEvents: 0, changeEvents: 0, syncTrue: 0, syncFalse: 0, connectionClose: 0, connectionError: 0,
    lastCloseCode: null as number | null, maxBufferedBytes: 0, lastMessageAtMs: null as number | null,
    raw: { local: rawBucket(), data: rawBucket(), control: rawBucket(), other: rawBucket(), unknown: rawBucket() } };
  const kindOf = (id: number): RawKind => {
    if (id === provider.doc.clientID) return "local";
    const state = object(provider.awareness.getStates().get(id));
    if (!state) return "unknown";
    if (object(state.ghostprovV2Data)?.peerId === "sidecar") return "data";
    if (object(state.ghostprovV2Control)?.peerId === "sidecar") return "control";
    return "other";
  };
  const original = provider.messageHandlers[1]!;
  provider.messageHandlers[1] = (...args) => {
    const decoder = args[1], now = performance.now();
    stats.messages = count(stats.messages); stats.bytes = count(stats.bytes, decoder.arr.byteLength); stats.lastMessageAtMs = now;
    const entries: Array<{ id: number; clock: number; previous: number; bytes: number; removed: boolean; kind: RawKind }> = [];
    try {
      const outer = decoding.createDecoder(decoder.arr); outer.pos = decoder.pos;
      const wire = decoding.readVarUint8Array(outer), header = decoding.createDecoder(wire), entryCount = decoding.readVarUint(header);
      if (!safeInteger(entryCount)) throw new ProofFailure("raw_header_bound");
      stats.entries = count(stats.entries, entryCount); stats.entryOverflow = count(stats.entryOverflow, Math.max(0, entryCount - 64));
      for (let index = 0; index < Math.min(entryCount, 64); index++) {
        const id = decoding.readVarUint(header), clock = decoding.readVarUint(header), bytes = decoding.readVarUint(header);
        if (!safeInteger(id) || !safeInteger(clock) || !safeInteger(bytes) || header.pos + bytes > wire.byteLength) throw new ProofFailure("raw_header_bound");
        const at = header.pos, removed = bytes === 4 && wire[at] === 110 && wire[at + 1] === 117 && wire[at + 2] === 108 && wire[at + 3] === 108;
        header.pos += bytes;
        const previous = provider.awareness.meta.get(id)?.clock ?? 0;
        entries.push({ id, clock, previous: safeInteger(previous) ? previous : 0, bytes, removed, kind: kindOf(id) });
      }
    } catch { stats.invalidHeaders = count(stats.invalidHeaders); }
    try { original(...args); }
    catch (error) { stats.handlerThrows = count(stats.handlerThrows); throw error; }
    finally {
      for (const entry of entries) {
        const kind = entry.kind === "unknown" || entry.kind === "other" ? kindOf(entry.id) : entry.kind, selected = stats.raw[kind];
        selected.entries = count(selected.entries); selected.stateBytes = count(selected.stateBytes, entry.bytes);
        selected.nullEntries = count(selected.nullEntries, Number(entry.removed));
        if (entry.clock > entry.previous) selected.greaterClock = count(selected.greaterClock);
        else if (entry.clock === entry.previous) selected.equalClock = count(selected.equalClock);
        else selected.lowerClock = count(selected.lowerClock);
        selected.lastClock = entry.clock; selected.previousClock = entry.previous;
        selected.lastStateBytes = entry.bytes; selected.lastRawAtMs = now;
      }
    }
  };
  const changed = (value: unknown, key: "updateEntries" | "changeEntries") => {
    const event = object(value); let left = 64;
    for (const list of [event?.added, event?.updated]) if (Array.isArray(list)) for (const id of list) {
      if (--left < 0) return;
      if (safeInteger(id)) { const selected = stats.raw[kindOf(id)]; selected[key] = count(selected[key]); }
    }
  };
  provider.awareness.on("update", (event: unknown) => { stats.updateEvents = count(stats.updateEvents); changed(event, "updateEntries"); });
  provider.awareness.on("change", (event: unknown) => { stats.changeEvents = count(stats.changeEvents); changed(event, "changeEntries"); });
  provider.on("sync", (synced: boolean) => { if (synced) stats.syncTrue = count(stats.syncTrue); else stats.syncFalse = count(stats.syncFalse); });
  provider.on("connection-close", event => { stats.connectionClose = count(stats.connectionClose); const code = object(event)?.code;
    stats.lastCloseCode = safeInteger(code) && code <= 65535 ? code : null; });
  provider.on("connection-error", () => { stats.connectionError = count(stats.connectionError); });
  return () => {
    const buffered = provider.ws?.bufferedAmount ?? 0;
    stats.maxBufferedBytes = Math.max(stats.maxBufferedBytes, safeInteger(buffered) ? buffered : 0);
    return { ...stats, currentBufferedBytes: safeInteger(buffered) ? buffered : 0, readyState: provider.ws?.readyState ?? null };
  };
}

async function bounded<T>(operation: Promise<T>, timeoutMs: number, code: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([operation, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new ProofFailure(code)), Math.max(1, timeoutMs)); })]); }
  finally { clearTimeout(timer); }
}

/** system.start means one worker is ready; retain this same deadline for the entire target fleet. */
export async function waitCapacityStartup(input: {
  readonly start: () => Promise<void>;
  readonly allUsable: () => boolean;
  readonly overallDeadlineMs: number;
  readonly signal: AbortSignal;
  readonly nowMs?: () => number;
  readonly sleep?: (ms: number) => Promise<void>;
}): Promise<void> {
  const now = input.nowMs ?? (() => performance.now()), sleep = input.sleep ?? (ms => Bun.sleep(ms));
  const startedAt = now();
  if (!Number.isFinite(startedAt) || !Number.isFinite(input.overallDeadlineMs)) throw new ProofFailure("startup_deadline_invalid");
  const startupDeadline = Math.min(startedAt + 6 * 60_000, input.overallDeadlineMs - CLEANUP_RESERVE_MS);
  input.signal.throwIfAborted();
  if (startupDeadline <= startedAt) throw new ProofFailure("startup_budget_exhausted");
  await bounded(input.start(), startupDeadline - startedAt, "startup_timeout");
  for (;;) {
    input.signal.throwIfAborted();
    const remaining = startupDeadline - now();
    if (remaining <= 0) throw new ProofFailure("all_runtimes_not_usable");
    if (input.allUsable()) return;
    await sleep(Math.min(250, remaining));
  }
}
async function captureAcquisitionResponse(response: Response, evidence: CapacityBackendEvidence, signal: AbortSignal): Promise<Response> {
  if (!response.ok) return response;
  if (!response.body) throw new ProofFailure("acquisition_evidence_body_missing");
  const reader = response.body.getReader(), bytes = new Uint8Array(16 * 1024); let used = 0;
  const abort = () => { void reader.cancel().catch(() => undefined); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    for (;;) {
      signal.throwIfAborted(); const item = await reader.read(); if (item.done) break;
      if (used + item.value.byteLength > bytes.byteLength) throw new ProofFailure("acquisition_evidence_body_bound");
      bytes.set(item.value, used); used += item.value.byteLength;
    }
    signal.throwIfAborted();
    const raw: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, used)));
    await evidence.acquired(raw); // Persist the exact backend before provisioning can dispatch any command.
    return new Response(bytes.slice(0, used), { status: response.status, statusText: response.statusText, headers: response.headers });
  } finally { signal.removeEventListener("abort", abort); await reader.cancel().catch(() => undefined); reader.releaseLock(); }
}
async function candidatePortAvailable(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const probe = createServer(); probe.once("error", () => reject(new ProofFailure("candidate_port_unavailable")));
    probe.listen(PORT, "127.0.0.1", () => probe.close(error => error ? reject(new ProofFailure("candidate_port_check_failed")) : resolve()));
  });
}

export interface CapacityProofHooks {
  /** Optional guarded fault seam. Wrap the native channel in place; never replace the provider. */
  readonly onChannel?: (channel: YSweetAwarenessChannel<HostFrame, SidecarFrame>, context: {
    readonly worker: number; readonly lane: "data" | "control"; readonly cycle: number;
  }) => void;
  readonly afterWorkload?: (context: {
    readonly system: BrowserlessSystem; readonly baseUrl: string; readonly apiKey: string; readonly signal: AbortSignal;
    readonly beginRequest: (digest: string, slot: 0 | 1) => void;
    readonly completeRequest: (digest: string, providerId: string) => void;
    readonly record: (event: Record<string, unknown>) => void;
  }) => Promise<Record<string, unknown>>;
}

export async function main(env: Record<string, string | undefined> = process.env, hooks: CapacityProofHooks = {}): Promise<void> {
  return runOwnedProof(env, { ...resolveCapacityProofOptions(env), scope: "capacity_reliability", sourceLabel: "current" }, hooks);
}

export async function runStartupArtifactDiagnostic(env: Record<string, string | undefined> = process.env): Promise<void> {
  return runOwnedProof(env, resolveStartupArtifactDiagnosticOptions(env), {});
}

async function runOwnedProof(env: Record<string, string | undefined>, options: ProofRunOptions, hooks: CapacityProofHooks): Promise<void> {
  const beginning = performance.now(), deadline = beginning + TOTAL_MS;
  if (options.faults !== Boolean(hooks.afterWorkload) || (hooks.onChannel && !options.faults)) throw new ProofFailure("fault_hooks_require_explicit_guard");
  const remaining = () => Math.max(1, deadline - performance.now());
  const workRemaining = () => Math.max(1, deadline - CLEANUP_RESERVE_MS - performance.now());
  const assets = await bounded(checkR10Sidecar(), 120_000, "r10_deterministic_check_timeout");
  const loadedAssets = await loadV2SidecarAssets({ revision: "r10" });
  if (loadedAssets.buildHash !== assets.identity.buildHash || loadedAssets.schemaHash !== assets.identity.schemaHash) throw new ProofFailure("loaded_release_mismatch");
  const capturePlan = startupArtifactCapturePlan(options.scope, options.startupArtifacts);
  const productionSource = capturePlan.expectedSessions > 0 ? {
    root: path.resolve(import.meta.dir, ".."), label: options.sourceLabel,
    files: await Promise.all(["src/server/system.ts", "src/sandbox/ydoc.ts", "src/sandbox/supervised-runtime.ts", "src/server/held-bootstrap-maintenance.ts", "src/v2/supervisor/lifecycle.ts"].map(async file => ({ file,
      sha256: createHash("sha256").update(await readFile(path.resolve(import.meta.dir, "..", file))).digest("hex") }))),
    bundleSha256: assets.identity.bundleSha256,
  } : undefined;
  await candidatePortAvailable();
  await mkdir(options.outputRoot, { recursive: true });
  const directory = await mkdtemp(path.join(options.outputRoot, "proof-"));
  const cookieFile = path.join(directory, "candidate-cookies.json"), manifestFile = path.join(directory, "fleet.json");
  const intended = Array.from({ length: options.projects }, () => projectId(crypto.randomUUID()));
  const titles = new Map(intended.map((id, index) => [id, `GhostProv ${options.scope === "startup_artifact_diagnostic" ? "Startup Artifact Diagnostic" : "Capacity Proof"} ${path.basename(directory)} ${index + 1}`]));
  const attempted = new Set<ProjectId>(), created = new Set<ProjectId>(), deleted = new Set<ProjectId>();
  const journalFactory = (id: ProjectId) => new FileSupervisorBootstrapJournal(`${manifestFile}.bootstrap`, id);
  const supervisors = new CapacitySupervisorProof(), trace = new TraceWriter(path.join(directory, "observations.jsonl"));
  const backendEvidence = new CapacityBackendEvidence(created, async record => {
    const file = path.join(directory, `backend-${record.index}.private.json`), temporary = `${file}.tmp`;
    const handle = await open(temporary, "w", 0o600);
    try { await handle.writeFile(JSON.stringify(record)); await handle.sync(); } finally { await handle.close(); }
    await rename(temporary, file);
  });
  const allResponseIds = new Set<string>();
  const cycles: Array<{ cycle: number; workload: WorkloadSummary; wire: ReturnType<CapacityRequestProof["snapshot"]>; stabilitySamples: number;
    faults?: Record<string, unknown>; faultWire?: ReturnType<CapacityRequestProof["snapshot"]> }> = [];
  const controllers: Array<{ cycle: number; worker: number; projectId: ProjectId; closed: boolean;
    gateway: V2BootstrapGateway; controller: V2SandboxSupervisor }> = [];
  const diagnosticFailures = new CapacityDiagnosticFailures(intended);
  const logger: LoggerLike = {
    emit: event => { try { assertSafeObservabilityEvent(event); diagnosticFailures.observe(event); trace.add(event); } catch { trace.errors++; } },
    snapshot: () => ({ queuedEvents: 0, queuedBytes: trace.stream.writableLength, droppedSamples: 0, droppedProgress: trace.dropped, droppedTerminals: 0, sinkErrors: trace.errors }),
    close: async () => {},
  };
  const work = new AbortController();
  const artifactAbort = new AbortController();
  const artifactCapture = capturePlan.expectedSessions > 0
    ? new StartupArtifactCapture({ record: row => trace.add({ event: "startup.artifact", ...row }), signal: artifactAbort.signal }) : null;
  const artifactCapturedWorkers = new Set<number>();
  let startupDiagnosticReady = false;
  const workTimer = setTimeout(() => work.abort(new ProofFailure("work_budget_expired")), workRemaining());
  const hardStop = setTimeout(() => {
    try { writeFileSync(path.join(directory, "deadline.json"), JSON.stringify({ success: false, code: "hard_deadline_expired",
      cleanupProven: false, attempted: attempted.size, deleted: deleted.size, supervisors: supervisors.snapshot() }), { mode: 0o600 }); } catch {}
    console.error(JSON.stringify({ event: "capacity.proof.deadline", directory, cleanupProven: false })); process.exit(1);
  }, remaining());
  const interrupt = () => { work.abort(new ProofFailure("operator_cancelled")); artifactAbort.abort(new ProofFailure("operator_cancelled")); };
  process.on("SIGINT", interrupt); process.on("SIGTERM", interrupt);
  let session: PrismSessionManager | null = null, system: BrowserlessSystem | null = null;
  let phase = "setup", cycle = 0, failure: string | null = null, closeProven = true, journalProven = true;
  let cleanupStarted = false, supervisorEvidencePersisted = true;
  const emptyJournals = new Set<ProjectId>();
  let directCalls = 0, llmCalls = 0, commandAcceptances = 0, diagnosticBytes = 0, diagnosticRejected = 0;
  let activeProof: CapacityRequestProof | null = null, sampleTimer: ReturnType<typeof setInterval> | undefined;
  const channels: Array<{ cycle: number; worker: number; lane: string; snapshot: (() => unknown) | null;
    publications: number; parseAttempts: number; parseFailures: number; lastIn: unknown; lastOut: unknown }> = [];
  const fail = (code: string) => { failure ??= code; work.abort(new ProofFailure(code)); };
  const mark = (next: string) => { phase = next; trace.add({ event: "capacity.proof.phase", scope: options.scope, phase, cycle });
    console.log(JSON.stringify({ event: "capacity.proof.phase", scope: options.scope, sourceLabel: options.sourceLabel, phase, cycle, directory })); };
  const ledger = () => writeFile(path.join(directory, "ownership.json"), JSON.stringify({ intended, titles: Object.fromEntries(titles), attempted: [...attempted], created: [...created], deleted: [...deleted] }, null, 2), { mode: 0o600 });
  const persistSupervisorLedger = (strict = false) => {
    try {
      const file = path.join(directory, "supervisor-cleanup.private.json"), temporary = `${file}.tmp`;
      const records = intended.map(id => ({ projectId: id, controllersRegistered: controllers.filter(record => record.projectId === id).length,
        controllersClosed: controllers.filter(record => record.projectId === id && record.closed).length,
        journalEmpty: emptyJournals.has(id), deleted: deleted.has(id), supervisors: supervisors.recordsForProject(id) }));
      const body = JSON.stringify({ version: 1, cleanupStarted, projects: records }, null, 2);
      if (Buffer.byteLength(body) > 256 * 1024) throw new ProofFailure("supervisor_evidence_bound");
      const descriptor = openSync(temporary, "w", 0o600);
      try { writeFileSync(descriptor, body); fsyncSync(descriptor); } finally { closeSync(descriptor); }
      renameSync(temporary, file);
    } catch { supervisorEvidencePersisted = false; failure ??= "supervisor_evidence_persistence_failed";
      if (strict) throw new ProofFailure("supervisor_evidence_persistence_failed"); }
  };
  const accountedHostFactory = (hostOptions: ConstructorParameters<typeof V2SupervisorHost>[0]) => {
    return accountCapacitySupervisorShutdown(new V2SupervisorHost(hostOptions), supervisors,
      () => persistSupervisorLedger(true));
  };
  const usableCount = () => system?.registry.snapshots().filter(snapshot => isRuntimeHealthy(snapshot) && snapshot.capacity > 0 && snapshot.available > 0 &&
    snapshot.quarantined === 0 && (snapshot.ambiguousQuarantined ?? 0) === 0).length ?? 0;
  const sample = () => {
    try {
      const router = system?.router.snapshot();
      trace.add({ event: "capacity.proof.sample", phase, cycle, elapsedMs: performance.now() - beginning, target: options.projects,
        usable: usableCount(), router: router ? { capacity: router.capacity, available: router.available, active: router.active, queued: router.queued } : null,
        writer: trace.snapshot() });
      for (const runtime of system?.registry.snapshots() ?? []) trace.add({ event: "capacity.proof.runtime", phase, cycle,
        worker: intended.indexOf(runtime.projectId) + 1, state: runtime.state, incarnation: runtime.incarnation, capacity: runtime.capacity,
        active: runtime.active, available: runtime.available, quarantine: runtime.quarantined, ambiguousQuarantine: runtime.ambiguousQuarantined,
        availabilityReason: runtime.availabilityReason, transportConnected: runtime.transportConnected, heartbeatAgeMs: runtime.heartbeatAgeMs,
        sidecarPeerAgeMs: runtime.sidecarPeerAgeMs, recovery: runtime.recovery, diagnostics: runtime.diagnostics });
      for (const channel of channels) if (channel.cycle === cycle) trace.add({ event: "capacity.proof.channel", phase, cycle,
        worker: channel.worker, lane: channel.lane, publications: channel.publications, parseAttempts: channel.parseAttempts,
        parseFailures: channel.parseFailures, incoming: channel.lastIn, outgoing: channel.lastOut, raw: channel.snapshot?.() ?? null });
    } catch { trace.errors++; }
  };
  const captureDiagnostics = async (label: "startup" | "settled" | "cleanup") => {
    if (!system || diagnosticBytes >= MAX_DIAGNOSTIC_BYTES) return;
    const until = Math.min(deadline - 240_000, performance.now() + 30_000);
    const destination = path.join(directory, `diagnostics-${cycle}-${label}`); await mkdir(destination, { recursive: true });
    for (const [worker, id] of intended.entries()) for (const file of SANDBOX_LOG_FILES) {
      if (performance.now() >= until || diagnosticBytes >= MAX_DIAGNOSTIC_BYTES) return;
      try {
        const response = await fetch(`${BASE}/v1/diagnostics/sandboxes/${id}/logs/${file}`, { headers: { authorization: `Bearer ${apiKey}` },
          signal: AbortSignal.timeout(Math.max(1, Math.min(3000, until - performance.now()))) });
        if (response.status === 404) { await response.body?.cancel(); continue; }
        if (!response.ok || !response.body) { await response.body?.cancel(); diagnosticRejected++; continue; }
        const bytes = new Uint8Array(Math.min(128 * 1024, MAX_DIAGNOSTIC_BYTES - diagnosticBytes)), reader = response.body.getReader(); let used = 0;
        try { for (;;) { const part = await reader.read(); if (part.done) break;
          if (used + part.value.byteLength > bytes.byteLength) throw new ProofFailure("diagnostic_file_bound"); bytes.set(part.value, used); used += part.value.byteLength; } }
        finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
        const content = new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, used)), lines = content.split("\n").filter(Boolean);
        if (lines.length > 1024) throw new ProofFailure("diagnostic_record_bound"); lines.forEach(parseDiagnosticLine);
        await writeFile(path.join(destination, `${worker + 1}-${file}`), content, { mode: 0o600 }); diagnosticBytes += used;
      } catch { diagnosticRejected++; }
    }
  };
  const captureExecutionDiagnostics = async () => {
    const until = Math.min(deadline - 230_000, performance.now() + 30_000);
    const records = controllers.map(({ cycle, worker, controller }) => ({ cycle, worker, executions: controller.diagnosticExecutions() }));
    const privateJson = JSON.stringify(records, null, 2);
    if (Buffer.byteLength(privateJson) > 1024 * 1024) throw new ProofFailure("execution_descriptor_bound");
    await writeFile(path.join(directory, "execution-descriptors.private.json"), privateJson, { mode: 0o600 });
    const destination = path.join(directory, `execution-logs-${cycle}`); await mkdir(destination, { recursive: true });
    // A failed runtime can retain an open controller while healthy controllers
    // are already closed. Prioritize failure evidence, then local closure, and
    // retain the exact original gateway/execution identity for every read.
    const ordered = diagnosticFailures.prioritize(controllers.map((record, controllerIndex) => ({
      record, controllerIndex, projectId: record.projectId, closed: record.closed,
    })));
    for (const { record, controllerIndex } of ordered) {
      if (record.cycle !== cycle) continue;
      trace.add({ event: "capacity.proof.execution_diagnostic", cycle, worker: record.worker,
        controllerIndex: controllerIndex + 1, failedProject: diagnosticFailures.has(record.projectId) });
      for (const [executionIndex, execution] of record.controller.diagnosticExecutions().entries()) for (const file of SANDBOX_LOG_FILES) {
        if (performance.now() >= until || diagnosticBytes >= MAX_DIAGNOSTIC_BYTES) return;
        try {
          const text = await bounded(readCapacityExecutionDiagnostic(record.gateway, record.projectId, execution, file,
            AbortSignal.timeout(Math.max(1, Math.min(3000, until - performance.now())))), 3500, "execution_diagnostic_timeout");
          if (text === null) continue;
          const size = Buffer.byteLength(text), lines = text.split("\n").filter(Boolean);
          if (size > 128 * 1024 || diagnosticBytes + size > MAX_DIAGNOSTIC_BYTES || lines.length > 1024) throw new ProofFailure("execution_diagnostic_bound");
          lines.forEach(parseDiagnosticLine);
          await writeFile(path.join(destination, `${controllerIndex + 1}-${executionIndex + 1}-${file}`), text, { mode: 0o600 }); diagnosticBytes += size;
        } catch { diagnosticRejected++; }
      }
    }
  };
  const apiKey = `capacity-proof-${crypto.randomUUID()}`;
  const closeSystem = async () => {
    if (!system) return;
    cleanupStarted = true; persistSupervisorLedger();
    closeProven = false; journalProven = false;
    try { await bounded(system.close(), Math.min(180_000, remaining() - 100_000), "system_close_timeout"); closeProven = true; }
    catch { failure ??= "system_cleanup_unproven"; }
    for (const id of intended) {
      if (controllers.some(record => record.projectId === id && !record.closed)) continue;
      try { await assertBootstrapCleanupComplete({ projectIds: [id], journalFactory, signal: AbortSignal.timeout(Math.min(15_000, remaining())) }); emptyJournals.add(id); }
      catch { emptyJournals.delete(id); failure ??= "journal_cleanup_unproven"; }
    }
    journalProven = emptyJournals.size === intended.length; persistSupervisorLedger();
    if (closeProven && journalProven && supervisors.snapshot().unprovedShutdowns === 0) { system = null; session = null; }
    else failure ??= "exact_supervisor_cleanup_unproven";
  };
  try {
    if ((await stat(options.cookieFile)).size > 8 * 1024 * 1024) throw new ProofFailure("cookie_file_bound");
    await copyFile(options.cookieFile, cookieFile); await ledger();
    for (let iteration = 1; iteration <= options.cycles; iteration++) {
      cycle = iteration;
      cleanupStarted = false;
      work.signal.throwIfAborted();
      session = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
      const identity = await bounded(session.resolve(AbortSignal.any([work.signal, AbortSignal.timeout(60_000)])), Math.min(61_000, workRemaining()), "identity_timeout");
      const request = identity.account.request.bind(identity.account);
      identity.account.request = async (url, init = {}) => {
        const target = new URL(url, options.origin), pathname = target.pathname;
        if (pathname.startsWith("/api/llm/")) { llmCalls++; return Promise.reject(new ProofFailure("unexpected_llm_bootstrap")); }
        const signal = AbortSignal.any([...(init.signal ? [init.signal] : []), AbortSignal.timeout(Math.min(60_000, remaining()))]);
        const method = (init.method ?? "GET").toUpperCase(), headers = new Headers(init.headers);
        const resourceProject = target.origin === options.origin ? /^\/api\/projects\/([0-9a-f-]{36})\/sandbox\/resources-token$(?![\s\S])/.exec(pathname)?.[1] : undefined;
        const direct = pathname.endsWith("/codex/codex_exec_async");
        const backendResource = !resourceProject && pathname.endsWith("/resources-token");
        if (method === "POST" && (resourceProject || direct || backendResource)) {
          if (typeof init.body !== "string" || Buffer.byteLength(init.body) > 16 * 1024) throw new ProofFailure("private_request_evidence_bound");
          const raw: unknown = JSON.parse(init.body);
          if (resourceProject) backendEvidence.resourceRequest(resourceProject, raw);
          else if (backendResource) await backendEvidence.delivered(target.href, headers, raw);
          else { directCalls++; await backendEvidence.command(target.href, headers, raw); }
        }
        const response = artifactCapture
          ? await artifactCapture.wrapAccountRequest(target.href, { ...init, signal }, () => request(url, { ...init, signal }))
          : await request(url, { ...init, signal });
        return method === "POST" && target.origin === options.origin && pathname === "/api/backend/1/new"
          ? captureAcquisitionResponse(response, backendEvidence, signal) : response;
      };
      const projects = new PrismProjectsClient(identity.account);
      if (cycle === 1) {
        mark("project_create");
        for (const id of intended) {
          work.signal.throwIfAborted();
          // Check only a generated identity; never enumerate unrelated account projects.
          try { await projects.get(id, AbortSignal.any([work.signal, AbortSignal.timeout(30_000)])); throw new ProofFailure("preexisting_project_collision"); }
          catch (error) { const status = object(error)?.status; if (status !== 404 && status !== 410) throw error; }
          attempted.add(id); await ledger(); // Durable intent precedes the one allowed create attempt.
          const createdProject = await bounded(projects.create({ projectId: id, title: titles.get(id)! }, AbortSignal.any([work.signal, AbortSignal.timeout(60_000)])), Math.min(61_000, workRemaining()), "project_create_timeout");
          if (createdProject.projectId !== id || createdProject.title !== titles.get(id) || createdProject.deleted) throw new ProofFailure("created_project_identity_mismatch");
          created.add(id); await ledger();
        }
      }
      const guardedProjects: FleetProjectsClient = {
        get: (id, signal) => { if (!created.has(id)) throw new ProofFailure("project_read_outside_test_ledger"); return projects.get(id, signal); },
        create: async () => { throw new ProofFailure("unexpected_fleet_project_creation"); },
        deleteManaged: async () => { throw new ProofFailure("unexpected_fleet_project_deletion"); },
      };
      activeProof = new CapacityRequestProof(allResponseIds); const proof = activeProof;
      const cycleProofs = [proof];
      mark("startup"); closeProven = false; journalProven = false;
      system = await createBrowserlessSystem({ ...env, PORT: String(PORT), GP_CANDIDATE: "1", GHOSTPROV_API_KEY: apiKey,
        PRISM_COOKIE_FILE: cookieFile, PRISM_COOKIE: undefined, GP_PRISM_SESSION: "authenticated", GP_ALLOW_ANONYMOUS: "0",
        PRISM_PROJECT_IDS: intended.join(","), PRISM_DOC_ID: "", GP_TARGET_SANDBOXES: String(options.projects), GP_MAX_PROJECTS: String(options.projects),
        GP_AUTO_CREATE_PROJECTS: "0", GP_PROJECT_REUSE: "managed", GP_GUEST_MAX_PROJECTS: "1", GP_FLEET_MANIFEST_FILE: manifestFile,
        GP_PROVISION_CONCURRENCY: String(Math.min(4, options.projects)), GP_COLD_START_CONCURRENCY: String(Math.min(2, options.projects)),
        GP_V2_SIDECAR_REVISION: "r10", GP_V2_SIDECAR_PROJECT_REVISIONS: "", GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay", GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "direct",
        GP_LOG_DIR: path.join(directory, "host"), GP_LOG_LEVEL: "debug", GP_V2_DIAGNOSTIC_ROOT: BASE,
        GP_V2_DIAGNOSTICS: "1", GP_V2_DIAGNOSTICS_INTERVAL_MS: "1000", GP_METRICS_INTERVAL_MS: "1000", GP_SHUTDOWN_DRAIN_MS: "180000",
      }, { sessionManager: session, projects: guardedProjects, assets: loadedAssets, logger,
        manifest: new FleetManifestStore({ file: manifestFile, identityFingerprint: identity.fingerprint }), bootstrapJournalFactory: journalFactory,
        heldBootstrapHostFactory: accountedHostFactory,
        awarenessChannelFactory: input => {
          const worker = intended.indexOf(input.envelope.docId) + 1;
          if (worker < 1 || channels.length >= MAX_CHANNELS) throw new ProofFailure("channel_identity_or_count_bound");
          const record = { cycle, worker, lane: input.field === "ghostprovV2Data" ? "data" : "control", snapshot: null as (() => unknown) | null,
            publications: 0, parseAttempts: 0, parseFailures: 0, lastIn: null as unknown, lastOut: null as unknown };
          channels.push(record);
          const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>({ ...input,
            providerFactory: (doc, credential) => { const provider = createYSweetAwarenessProvider(doc, credential); record.snapshot = observeProvider(provider); return provider; },
            parseRemote: value => { record.parseAttempts = count(record.parseAttempts);
              try { const parsed = input.parseRemote(value); record.lastIn = capacityEnvelopeMetadata(parsed); return parsed; }
              catch (error) { record.parseFailures = count(record.parseFailures); throw error; } },
          });
          const publish = channel.publish.bind(channel), subscribe = channel.subscribe.bind(channel);
          channel.publish = envelope => {
            record.publications = count(record.publications); record.lastOut = capacityEnvelopeMetadata(envelope);
            for (const frame of envelope.frames) if (frame.kind === "request_start") {
              try { const owner = cycleProofs.find(candidate => candidate.tracksDigest(frame.requestDigest));
                if (!owner) throw new ProofFailure("request_execution_untracked"); owner.start(channel.projectId, frame);
              } catch (error) { fail(safeFailure(error)); throw error; }
            }
            trace.add({ event: "capacity.proof.envelope", cycle: record.cycle, worker, lane: record.lane, direction: "out", metadata: record.lastOut });
            return publish(envelope);
          };
          channel.subscribe = listener => subscribe(update => {
            for (const frame of update.envelope.frames) if (frame.kind === "response_terminal" && frame.terminalStatus === "completed") {
              try { const owner = cycleProofs.find(candidate => candidate.tracksRequest(frame.requestId));
                if (!owner) throw new ProofFailure("request_terminal_untracked"); owner.terminal(channel.projectId, frame);
              } catch (error) { fail(safeFailure(error)); throw error; }
            }
            trace.add({ event: "capacity.proof.envelope", cycle: record.cycle, worker, lane: record.lane, direction: "in", metadata: capacityEnvelopeMetadata(update.envelope) });
            listener(update);
          });
          hooks.onChannel?.(channel, { worker, lane: record.lane === "data" ? "data" : "control", cycle });
          return channel;
        },
        sandboxSupervisorFactory: input => {
          if (cleanupStarted || work.signal.aborted) throw new ProofFailure("supervisor_factory_fenced_for_cleanup");
          if (!created.has(projectId(input.binding.projectId))) throw new ProofFailure("supervisor_outside_test_ledger");
          supervisors.register(input.binding); emptyJournals.delete(projectId(input.binding.projectId)); persistSupervisorLedger(true);
          const submit = input.gateway.startSupervisorExec?.bind(input.gateway);
          if (!submit) throw new ProofFailure("direct_bootstrap_unavailable");
          const workerIndex = intended.indexOf(projectId(input.binding.projectId)) + 1;
          const captureThisController = artifactCapture !== null && cycle === capturePlan.cycle &&
            capturePlan.workers.includes(workerIndex) && !artifactCapturedWorkers.has(workerIndex);
          let artifactExpectations: ReturnType<typeof createStartupArtifactExpectations> | null = null;
          let artifactRun: ReturnType<StartupArtifactCapture["accept"]> | null = null;
          let artifactPhase = "awaiting_acceptance";
          const gateway = new Proxy(input.gateway, { get(target, property) {
            if (property === "startSupervisorExec") return async (...args: Parameters<typeof submit>) => {
              work.signal.throwIfAborted();
              supervisors.submit(input.binding.projectId, input.binding.nonce); persistSupervisorLedger(true);
              backendEvidence.registerCommand(input.binding.projectId, input.binding.nonce, args[0].sessionId);
              let accepted: Awaited<ReturnType<typeof submit>>;
              try { accepted = await submit(...args); }
              catch (error) { fail("bootstrap_submission_failed_no_replay"); throw error; }
              const acceptedAtMs = performance.now();
              // Accepted session/job descriptors are private recovery evidence, never console or trace fields.
              const serialized = JSON.stringify({ projectId: input.binding.projectId, nonce: input.binding.nonce, accepted });
              if (Buffer.byteLength(serialized) > 4096 || commandAcceptances >= MAX_SUPERVISORS) throw new ProofFailure("command_acceptance_bound");
              await writeFile(path.join(directory, `command-acceptance-${++commandAcceptances}.private.json`), serialized, { flag: "wx", mode: 0o600 });
              if (captureThisController && artifactCapture && artifactExpectations) {
                artifactCapturedWorkers.add(workerIndex);
                artifactPhase = "accepted";
                try { artifactRun = artifactCapture.accept({ worker: workerIndex as StartupArtifactWorker,
                  sessionId: accepted.sessionId, acceptedAtMs, gateway: input.gateway, expectations: artifactExpectations, phase: () => artifactPhase }); }
                catch { trace.add({ event: "startup.artifact.failure", worker: intended.indexOf(projectId(input.binding.projectId)) + 1, code: "capture_registration_failed" }); }
              }
              return accepted;
            };
            const value: unknown = Reflect.get(target, property); return typeof value === "function" ? value.bind(target) : value;
          } });
          const record = { cycle, worker: intended.indexOf(projectId(input.binding.projectId)) + 1, projectId: projectId(input.binding.projectId),
            closed: false, gateway: input.gateway, controller: null as unknown as V2SandboxSupervisor };
          const controller = new V2SandboxSupervisor({ ...input, gateway, onClosed: () => {
            try { input.onClosed?.(); } finally { artifactPhase = "controller_closed"; record.closed = true; persistSupervisorLedger(); }
          }, hostFactory: hostOptions => {
            const host = accountedHostFactory(hostOptions);
            if (captureThisController && artifactCapture && !hostOptions.recoverExisting) {
              artifactExpectations = createStartupArtifactExpectations(input.assets, hostOptions.binding, hostOptions.connectTimeoutMs!);
              const connect = host.connect.bind(host); let firstConnect = true;
              host.connect = async signal => {
                if (!firstConnect) return connect(signal);
                firstConnect = false; artifactPhase = "readiness_pending";
                // Start the production readiness clock immediately. The bounded
                // first read must settle before control returns to file cleanup.
                try { await connect(signal); artifactPhase = "readiness_proven"; }
                catch (error) { artifactPhase = "readiness_failed"; throw error; }
                finally { await artifactRun?.initial; }
              };
            }
            return host;
          } });
          record.controller = controller; controllers.push(record); persistSupervisorLedger();
          return controller;
        },
      }, work.signal);
      if (!sampleTimer) sampleTimer = setInterval(sample, 2000);
      await waitCapacityStartup({ start: () => system!.start(), allUsable: () => usableCount() === options.projects,
        overallDeadlineMs: deadline, signal: work.signal });
      sample();
      trace.add({ event: "capacity.proof.ready", cycle, target: options.projects, usable: usableCount(), schemaHash: assets.identity.schemaHash,
        buildHash: assets.identity.buildHash, routerTiming: system.router.admissionTiming(), initialActivePerSandbox: system.config.v2InitialActivePerSandbox,
        maxActivePerSandbox: system.config.v2MaxActivePerSandbox, configuredSandboxCapacity: system.config.v2SandboxCapacity });
      if (artifactCapture && cycle === capturePlan.cycle) {
        mark("startup_artifact_capture");
        await artifactCapture.settle();
      }
      if (options.scope === "startup_artifact_diagnostic") {
        startupDiagnosticReady = true;
        await captureExecutionDiagnostics();
        continue;
      }
      await captureDiagnostics("startup"); mark("workload");
      const workload = await runWorkload({ baseUrl: BASE, apiKey, signal: work.signal, rootBytes: options.rootBytes,
        onPrepared: (digest, lane, turn) => proof.begin(digest, lane, turn), onTerminal: (digest, id) => proof.complete(digest, id),
        onMetric: metric => trace.add({ event: "capacity.proof.request", cycle, ...metric }),
        onProgress: progress => trace.add({ event: "capacity.proof.request_progress", cycle, ...progress }),
      });
      const wire = proof.snapshot(); let stabilitySamples = 0;
      cycles.push({ cycle, workload, wire, stabilitySamples });
      if (workload.completed !== 100 || workload.failed !== 0 || workload.skipped !== 0 || workload.callbackFailures !== 0 || wire.accepted !== 100) throw new ProofFailure("workload_incomplete");
      mark("settling");
      const settlingStarted = performance.now();
      const idleDeadline = settlingStarted + Math.min(system.router.admissionTiming().queueTimeoutMs, workRemaining());
      for (;;) {
        work.signal.throwIfAborted();
        const router = system.router.snapshot();
        if (usableCount() === options.projects && router.active === 0 && router.queued === 0) break;
        if (performance.now() >= idleDeadline) throw new ProofFailure("workload_did_not_settle");
        await Bun.sleep(250);
      }
      trace.add({ event: "capacity.proof.settled", cycle, durationMs: performance.now() - settlingStarted });
      mark("idle_stability");
      const until = performance.now() + options.stabilityMs;
      while (performance.now() < until) {
        work.signal.throwIfAborted();
        const router = system.router.snapshot();
        if (usableCount() !== options.projects || router.active !== 0 || router.queued !== 0) throw new ProofFailure("idle_stability_lost_readiness");
        stabilitySamples++; await Bun.sleep(1000);
      }
      cycles[cycles.length - 1]!.stabilitySamples = stabilitySamples;
      if (hooks.afterWorkload) {
        mark("fault_proof"); const faultProof = new CapacityRequestProof(allResponseIds); cycleProofs.push(faultProof);
        activeProof = faultProof;
        const faultSlots = new Set<number>();
        let result: Record<string, unknown>;
        try { result = await hooks.afterWorkload({ system, baseUrl: BASE, apiKey, signal: work.signal,
          beginRequest: (digest, slot) => {
            if ((slot !== 0 && slot !== 1) || faultSlots.has(slot)) throw new ProofFailure("fault_request_bound_or_replay");
            faultSlots.add(slot); faultProof.begin(digest, 0, slot);
          }, completeRequest: (digest, id) => faultProof.complete(digest, id), record: event => trace.add({ ...event, cycle }) }); }
        finally { cycles[cycles.length - 1]!.faultWire = faultProof.snapshot(); }
        const faultWire = faultProof.snapshot();
        cycles[cycles.length - 1]!.faults = result; cycles[cycles.length - 1]!.faultWire = faultWire;
        if (faultWire.accepted !== 2) throw new ProofFailure("fault_proof_incomplete");
      }
      await captureDiagnostics("settled");
      await captureExecutionDiagnostics();
      if (cycle < options.cycles) { mark("restart_cleanup"); await closeSystem(); if (failure) throw new ProofFailure(failure); mark("same_project_restart"); }
    }
  } catch (error) { failure ??= safeFailure(error); trace.add({ event: "capacity.proof.failure", phase, cycle, code: failure }); }
  finally {
    clearTimeout(workTimer); work.abort(new ProofFailure("work_finished"));
    cleanupStarted = true; persistSupervisorLedger();
    if (sampleTimer) clearInterval(sampleTimer);
    await artifactCapture?.settle();
    mark("cleanup"); sample();
    // Original execution reads have the failure priority information; the
    // public route may already be gone or consume the whole collection budget.
    await captureExecutionDiagnostics().catch(() => { diagnosticRejected++; });
    await captureDiagnostics("cleanup").catch(() => { diagnosticRejected++; });
    if (system) await closeSystem();
    else { await session?.close().catch(() => { failure ??= "session_close_failed"; });
      if (supervisors.snapshot().submitted === 0) { closeProven = true; journalProven = true; } }
    // Closing a sibling may fail while this project's exact writer/child cleanup
    // is complete. Audit each closed project under its own journal lease.
    for (const id of intended) {
      if (controllers.some(record => record.projectId === id && !record.closed)) { emptyJournals.delete(id); continue; }
      try { await assertBootstrapCleanupComplete({ projectIds: [id], journalFactory, signal: AbortSignal.timeout(Math.min(15_000, remaining())) }); emptyJournals.add(id); }
      catch { emptyJournals.delete(id); failure ??= "journal_cleanup_unproven"; }
    }
    journalProven = emptyJournals.size === intended.length; persistSupervisorLedger();
    const eligible = [...attempted].filter(id => capacityProjectMayDelete({ cleanupStarted,
      controllersClosed: controllers.every(record => record.projectId !== id || record.closed),
      unprovedShutdowns: supervisors.projectSnapshot(id).unprovedShutdowns, journalEmpty: emptyJournals.has(id), evidencePersisted: supervisorEvidencePersisted }));
    if (eligible.length) {
      let cleanup: PrismSessionManager | null = null;
      try {
        if (eligible.length) {
          cleanup = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
          const identity = await cleanup.resolve(AbortSignal.timeout(Math.min(15_000, remaining()))), projects = new PrismProjectsClient(identity.account);
          for (const id of eligible) {
            if (!supervisorEvidencePersisted) break;
            let identityObserved = created.has(id);
            try {
              const before = await projects.get(id, AbortSignal.timeout(Math.min(10_000, remaining())));
              if (before.projectId !== id || before.title !== titles.get(id)) throw new ProofFailure("cleanup_project_identity_changed");
              identityObserved = true;
              if (!before.deleted) await projects.deleteManaged(id, AbortSignal.timeout(Math.min(15_000, remaining())));
              const after = await projects.get(id, AbortSignal.timeout(Math.min(10_000, remaining())));
              if (!after.deleted || after.projectId !== id || after.title !== titles.get(id)) throw new ProofFailure("project_deletion_unproven");
              deleted.add(id);
            } catch (error) { const status = object(error)?.status;
              // A timed-out POST may still create later. An immediate absence does not
              // clear that ambiguous operation; preserve its ledger and private copy.
              if (capacityAbsenceProvesDeletion(status, identityObserved)) deleted.add(id);
              else failure ??= status === 404 || status === 410 ? "ambiguous_project_creation_unresolved" : safeFailure(error); }
            await ledger(); persistSupervisorLedger();
          }
        }
      } catch (error) { failure ??= safeFailure(error); }
      finally { await cleanup?.close().catch(() => { failure ??= "cleanup_session_close_failed"; }); }
    }
    if (eligible.length !== attempted.size) failure ??= "exact_cleanup_proof_missing";
    // Keep the private copy when cleanup needs follow-up. All other failure evidence remains intact.
    if (deleted.size === attempted.size) for (const file of [cookieFile, `${cookieFile}.identity.json`]) {
      await unlink(file).catch(error => { if (object(error)?.code !== "ENOENT") failure ??= "private_copy_cleanup_failed"; });
    }
    process.off("SIGINT", interrupt); process.off("SIGTERM", interrupt);
    trace.add({ event: "capacity.proof.final", failure, attempted: attempted.size, created: created.size, deleted: deleted.size,
      closeProven, journalProven, directCalls, llmCalls, commandAcceptances, supervisors: supervisors.snapshot(), wire: activeProof?.snapshot() ?? null });
    await bounded(trace.close(), Math.min(5000, remaining()), "trace_close_timeout").catch(() => { failure ??= "trace_close_timeout"; });
    if (trace.dropped || trace.errors) failure ??= "diagnostic_capture_incomplete";
    const captureSatisfied = !artifactCapture || artifactCapture.snapshot().sessions === capturePlan.expectedSessions && artifactCapture.snapshot().complete;
    if (!captureSatisfied) failure ??= "startup_artifact_capture_incomplete";
    const workloadSatisfied = options.scope === "capacity_reliability" ? cycles.length === options.cycles
      : startupDiagnosticReady && artifactCapture?.snapshot().sessions === 2 && artifactCapture.snapshot().complete;
    const success = failure === null && workloadSatisfied && deleted.size === options.projects && closeProven && journalProven && supervisors.snapshot().unprovedShutdowns === 0;
    const report = { success, scope: options.scope, failure, elapsedMs: performance.now() - beginning, projects: options.projects, requestedCycles: options.cycles,
      providerRequests: cycles.reduce((total, entry) => total + entry.workload.started, 0), productionSource,
      artifactCapture: artifactCapture?.snapshot(), artifactCapturePlan: capturePlan, startupDiagnosticReady,
      ...(options.scope === "capacity_reliability" ? { rootBytes: options.rootBytes, model: "gpt-6-astra", effort: "xhigh", stabilityMs: options.stabilityMs, faultsEnabled: options.faults } : {}),
      attempted: attempted.size, created: created.size, deleted: deleted.size, closeProven, journalProven,
      directCalls, llmCalls, commandAcceptances, supervisors: supervisors.snapshot(), backendEvidence: backendEvidence.snapshot(),
      projectCleanup: intended.map((id, worker) => ({ worker: worker + 1, ...supervisors.projectSnapshot(id),
        controllersClosed: controllers.every(record => record.projectId !== id || record.closed), journalEmpty: emptyJournals.has(id), deleted: deleted.has(id) })),
      diagnosticBytes, diagnosticRejected, diagnosticFailedProjects: diagnosticFailures.size, trace: trace.snapshot(), cycles };
    await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2), { mode: 0o600 });
    console.log(JSON.stringify({ event: "capacity.proof.result", scope: options.scope, directory, report: path.join(directory, "report.json"), success, failure,
      completed: cycles.reduce((total, value) => total + value.workload.completed, 0), deleted: deleted.size, elapsedMs: report.elapsedMs }));
    if (closeProven) clearTimeout(hardStop);
    if (!success) process.exitCode = 1;
  }
}

if (import.meta.main) main().catch(error => { console.error(JSON.stringify({ event: "capacity.proof.fatal", code: safeFailure(error) })); process.exitCode = 1; });
