import { copyFile, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import * as path from "node:path";
import type { BrowserlessSystem } from "../src/server/system";
import type { PrismSessionManager } from "../src/prism/session-manager";
import type { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import type { V2BootstrapGateway } from "../src/v2/bootstrap/controller";
import type { V2BootstrapController } from "../src/v2/bootstrap/controller";
import { V2_R10_RELEASE } from "../src/v2/sidecar/assets";
import { parseGitSshSigningResult, readGitSshSigningResult, GIT_SSH_SIGNING_HOOK_SHA256, GIT_SSH_SIGNER_SHA256, loadGitSshSigningSources, type GitSshSigningResult } from "../src/v2/supervisor/git-ssh-signing";
import { parseFrames } from "../src/relay/sse";
import { proveSignedShutdown } from "./lib/git-fsmonitor-proof";
import { R9ReconnectProof } from "./lib/git-ssh-signing-reconnect-proof";
import { parseDiagnosticLine, type SandboxLogFile } from "../src/v2/diagnostics/sandbox-log-schema";
import { checkR10Sidecar } from "./build_v2_sidecar_r10";
import { requireGitSshSigningLiveGuards } from "./lib/git-ssh-signing-proof";
type FsGateway = V2BootstrapGateway & { postJson(operation: string, body: unknown, signal?: AbortSignal): Promise<Response> };

interface SafeError { name: string | null; code: string | null; kind: string | null; phase: string | null; status: number | null; location: string | null; cause?: SafeError }
function safeError(error: unknown, depth = 0): SafeError {
  const value = error && typeof error === "object" ? error as Record<string, unknown> : {};
  const names = ["Error", "TypeError", "RangeError", "TimeoutError", "AbortError", "V2BootstrapError", "V2SupervisorError",
    "V2SupervisorBootstrapError", "V2RuntimeLifecycleFailure", "AwarenessChannelError", "SandboxGatewayError", "PrismHttpError",
    "AccountTransportError", "AccountAuthError", "CookieFileReadError", "CookieFileConflictError"];
  const codes = ["deploy", "render", "hello", "readiness", "timeout", "connect", "transport", "state", "closed", "command", "sequence",
    "protocol", "exec", "shutdown", "auth", "provision", "network", "http", "environment", "cleanup_filesystem_mode", "cleanup_workspace_unverified"];
  const word = (item: unknown, allowed: readonly string[]) => typeof item === "string" ? allowed.includes(item) ? item : "other" : null;
  const location = typeof value.stack === "string" ? value.stack.replaceAll("\\", "/").match(/(?:src|scripts|\.tmp)\/[A-Za-z0-9_.\/-]+\.ts:\d+:\d+/)?.[0] ?? null : null;
  return { name: word(value.name, names), code: word(value.code, codes), kind: word(value.kind, codes), phase: word(value.phase, codes),
    status: typeof value.status === "number" && Number.isInteger(value.status) && value.status >= 100 && value.status <= 599 ? value.status : null,
    location, ...(depth < 3 && value.cause ? { cause: safeError(value.cause, depth + 1) } : {}) };
}

function usable(snapshot: ReturnType<NonNullable<ReturnType<BrowserlessSystem["registry"]["get"]>>["snapshot"]>): boolean {
  return snapshot.state === "ready" && snapshot.capacity > 0 && snapshot.available > 0 && snapshot.transportConnected === true &&
    snapshot.active === 0 && snapshot.queueDepth === 0 && snapshot.quarantined === 0 && snapshot.ambiguousQuarantined === 0;
}

export async function runLiveGitSshSigning(): Promise<void> {
  requireGitSshSigningLiveGuards(process.env);
  await loadGitSshSigningSources();
  await checkR10Sidecar();
  const sourceCookie = process.env.PRISM_COOKIE_FILE;
  if (!sourceCookie) throw new Error("The authorized Prism credential source is missing");
  const model = process.env.GP_LIVE_MODEL ?? "gpt-6-astra";
  if (!/^[A-Za-z0-9._-]{1,128}$/.test(model)) throw new Error("Invalid model identifier");
  // Live dependencies are loaded only after the explicit guard and prior proof.
  const [{ PrismSessionManager }, { PrismProjectsClient }, { FleetManifestStore }, { createBrowserlessSystem },
    { V2SandboxSupervisor }, { ObservabilityLogger }, { RotatingJsonlSink }, { resolveObservabilityConfig }, { projectId }, { V2BootstrapController }] = await Promise.all([
    import("../src/prism/session-manager"), import("../src/prism/projects-client"), import("../src/fleet/manifest"),
    import("../src/server/system"), import("../src/v2/supervisor/controller"), import("../src/observability/logger"),
    import("../src/observability/rotating-jsonl"), import("../src/observability/config"), import("../src/sandbox/types"), import("../src/v2/bootstrap/controller"),
  ]);
  const root = path.resolve(".ghostprov/git-ssh-signing");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "r10-"));
  const cookieFile = path.join(directory, "candidate-cookies.json");
  const id = projectId(crypto.randomUUID());
  const title = `GhostProv Git SSH signing verification ${path.basename(directory)}`;
  const localKey = `git-ssh-signing-${crypto.randomUUID()}`;
  const config = resolveObservabilityConfig({ GP_LOG_LEVEL: "error", GP_LOG_DIR: path.join(directory, "host"),
    GP_LOG_ROTATE_BYTES: String(256 * 1024), GP_LOG_RETAIN_FILES: "1", GP_LOG_QUEUE_MAX_EVENTS: "256",
    GP_LOG_QUEUE_MAX_BYTES: String(1024 * 1024), GP_LOG_FLUSH_MS: "1000", GP_METRICS_INTERVAL_MS: "60000" });
  const logger = new ObservabilityLogger({ config, sinks: [new RotatingJsonlSink({ directory: config.directory,
    prefix: "git-ssh-signing-host", rotateBytes: config.rotateBytes, retainFiles: config.retainFiles })] });
  let controlReceipt: GitSshSigningResult | null = null;
  let controlSessionId: string | null = null;
  let workspaceRemoved = false;
  const receipt = () => controlReceipt;
  const reconnect = new R9ReconnectProof(1, { dropChallenge: true, dropReply: true });
  const blocked = { llm: 0, codexExec: 0, render: 0, fsmonitor: 0, foreignProjectMutation: 0 };
  const timings: Record<string, number> = {};
  let session: PrismSessionManager | null = null;
  let system: BrowserlessSystem | null = null;
  let supervisor: V2SandboxSupervisor | null = null;
  let evidenceGateway: (FsGateway & V2BootstrapGateway) | null = null;
  let supervisorInstance: string | null = null;
  let childPid: number | null = null;
  let latestNonce: string | null = null;
  let stage = "credentials";
  let projectAttempted = false;
  let projectCreated = false;
  let projectDeleted = false;
  let credentialCopied = false;
  let credentialDeleted = false;
  let bridgeStarts = 0;
  let tokenSubmissions = 0;
  let sidecarLaunches = 0;
  let readyProved = false;
  let reconnectsProved = 0;
  let recoveryProved = false;
  let modelRequestAttempts = 0;
  let responseCompleted = false;
  let stable = false;
  let shutdownProved = false;
  let failure: ReturnType<typeof safeError> | null = null;
  const hookFailure = null;
  const failureDiagnostics: Array<{ file: SandboxLogFile; records: readonly Record<string, unknown>[] }> = [];
  const bootstrapFailures: SafeError[] = [];
  const SOAK_MS = 120_000;
  const started = performance.now();
  const selectedSupervisor = () => supervisor;
  const selectedSystem = () => { if (!system) throw new Error("Missing candidate system"); return system; };
  const event = (name: string, data: Record<string, unknown> = {}) => console.log(JSON.stringify({ event: `git_ssh_signing.${name}`, ...data }));
  // Observe errors in this disposable process only. Return the exact original
  // promise; no execution, retry, deadline, or lifecycle behavior is replaced.
  const originalEnsure = V2BootstrapController.prototype.ensureReady;
  const observedEnsure = function(this: V2BootstrapController, signal?: AbortSignal) {
    const operation = originalEnsure.call(this, signal);
    void operation.catch(error => {
      if (bootstrapFailures.length >= 16) return;
      const detail = safeError(error);
      bootstrapFailures.push(detail);
      event("bootstrap_failure", { detail });
    });
    return operation;
  };
  V2BootstrapController.prototype.ensureReady = observedEnsure;

  async function confirmControlCleanup() {
    if (!shutdownProved || !evidenceGateway || !controlSessionId || receipt()?.outcome !== "submitted") throw new Error("Missing signed control cleanup authority");
    if (await readGitSshSigningResult(evidenceGateway, controlSessionId, AbortSignal.timeout(10000)) !== null) throw new Error("Control workspace still exists");
    workspaceRemoved = true;
  }

  async function waitIdle() {
    const deadline = performance.now() + 30_000;
    while (performance.now() < deadline) {
      const snapshot = selectedSystem().registry.get(id)?.snapshot();
      if (snapshot && usable(snapshot)) return;
      await Bun.sleep(250);
    }
    throw new Error("Candidate did not return to usable idle capacity");
  }

  async function recoverChild() {
    await waitIdle();
    const selected = selectedSupervisor();
    if (!selected || !latestNonce || childPid === null || sidecarLaunches !== 1) throw new Error("Missing exact original child");
    const originalNonce = latestNonce;
    const originalPid = childPid;
    const recoveryStart = performance.now();
    const stopped = await selected.stop({ launchNonces: [originalNonce], reason: "git_ssh_signing_controlled_recovery" }, AbortSignal.timeout(45_000));
    if (stopped !== originalNonce) throw new Error("Controlled stop lacked exact child proof");
    const deadline = performance.now() + 180_000;
    let progressAt = performance.now() + 15_000;
    while (performance.now() < deadline) {
      if (selectedSupervisor() !== selected || selected.instanceId !== supervisorInstance || sidecarLaunches > 2) throw new Error("Recovery changed supervisor identity");
      const snapshot = selectedSystem().registry.get(id)?.snapshot();
      if (Number(sidecarLaunches) === 2 && latestNonce !== originalNonce && childPid !== originalPid && snapshot && usable(snapshot)) {
        recoveryProved = true;
        timings.recoveryMs = Math.round(performance.now() - recoveryStart);
        event("recovered", { sameSupervisor: true, sidecarLaunches, elapsedMs: timings.recoveryMs });
        return;
      }
      if (performance.now() >= progressAt) { event("recovery_wait", { sidecarLaunches }); progressAt += 15_000; }
      await Bun.sleep(250);
    }
    throw new Error("Controlled recovery did not restore signed readiness");
  }

  async function sendOneRequest() {
    if (++modelRequestAttempts !== 1) throw new Error("Only one model request is authorized for this run");
    const requestStart = performance.now();
    const response = await fetch("http://127.0.0.1:18888/v1/responses", { method: "POST", signal: AbortSignal.timeout(180_000),
      headers: { authorization: `Bearer ${localKey}`, "content-type": "application/json" },
      body: JSON.stringify({ model, stream: true, store: true, max_output_tokens: 128,
        reasoning: { effort: "low" }, input: "Reply with exactly Hiii and no punctuation." }) });
    if (!response.ok || !response.body) {
      await response.body?.cancel();
      throw Object.assign(new Error("Candidate model request was rejected"), { status: response.status });
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let bytes = 0;
    let events = 0;
    let terminals = 0;
    let textCharacters = 0;
    let buffer = "";
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        bytes += next.value.byteLength;
        if (bytes > 2 * 1024 * 1024 || buffer.length + next.value.byteLength > 256 * 1024) throw new Error("Candidate response exceeded its byte bound");
        buffer += decoder.decode(next.value, { stream: true });
        const parsed = parseFrames(buffer, 0);
        buffer = buffer.slice(parsed.nextOffset);
        for (const frame of parsed.frames) {
          if (++events > 4096) throw new Error("Candidate response exceeded its event bound");
          if (frame.event === "response.output_text.delta") {
            const value: unknown = JSON.parse(frame.data);
            if (!value || typeof value !== "object" || !("delta" in value) || typeof value.delta !== "string") throw new Error("Invalid text event");
            textCharacters += value.delta.length;
            if (textCharacters > 4096) throw new Error("Candidate text exceeded its bound");
          }
          if (frame.event === "response.completed") {
            const value: unknown = JSON.parse(frame.data);
            if (!value || typeof value !== "object" || !("response" in value) || !value.response || typeof value.response !== "object" ||
                !("id" in value.response) || typeof value.response.id !== "string" || value.response.id.length > 256 ||
                !("status" in value.response) || value.response.status !== "completed") throw new Error("Invalid provider terminal");
            terminals++;
          }
          if (["response.failed", "response.incomplete", "error"].includes(frame.event ?? "")) throw new Error("Provider terminal failed");
        }
      }
    } finally { await reader.cancel().catch(() => undefined); reader.releaseLock(); }
    if (terminals !== 1 || textCharacters === 0 || buffer.trim()) throw new Error("Missing complete text response");
    responseCompleted = true;
    timings.requestMs = Math.round(performance.now() - requestStart);
    event("response_completed", { modelRequestAttempts, terminals, events, bytes, elapsedMs: timings.requestMs });
  }

  try {
    await copyFile(sourceCookie, cookieFile);
    credentialCopied = true;
    session = await PrismSessionManager.create({ origin: "https://prism.openai.com", mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await session.resolve(AbortSignal.timeout(60_000));
    const accountRequest = identity.account.request.bind(identity.account);
    identity.account.request = async (url, init) => {
      const pathname = new URL(url, "https://prism.openai.com").pathname;
      if (pathname.startsWith("/api/llm/")) { blocked.llm++; throw new Error("LLM launch is blocked"); }
      if (pathname.endsWith("/codex/codex_exec_async")) { blocked.codexExec++; throw new Error("Direct execution launch is blocked"); }
      if (pathname.endsWith("/render")) { blocked.render++; throw new Error("Render launch is blocked"); }
      return accountRequest(url, init);
    };
    const projects = new PrismProjectsClient(identity.account);
    const create = projects.create.bind(projects);
    const remove = projects.deleteManaged.bind(projects);
    projects.create = async (input, signal) => {
      if (input.projectId !== id || projectAttempted) { blocked.foreignProjectMutation++; throw new Error("Unexpected project creation"); }
      projectAttempted = true;
      return create(input, signal);
    };
    projects.deleteManaged = async (target, signal) => {
      if (target !== id) { blocked.foreignProjectMutation++; throw new Error("Foreign project deletion blocked"); }
      return remove(target, signal);
    };
    stage = "create_project";
    await writeFile(path.join(directory, "owned-project.json"), JSON.stringify({ id, title }));
    await projects.create({ projectId: id, title }, AbortSignal.timeout(60_000));
    projectCreated = true;
    event("project_created", { run: path.basename(directory) });
    stage = "signed_startup";
    system = await createBrowserlessSystem({ PORT: "18888", GP_CANDIDATE: "1", GHOSTPROV_API_KEY: localKey,
      PRISM_COOKIE_FILE: cookieFile, GP_PRISM_SESSION: "authenticated", GP_ALLOW_ANONYMOUS: "0", PRISM_PROJECT_IDS: id,
      PRISM_DOC_ID: "", GP_TARGET_SANDBOXES: "1", GP_MAX_PROJECTS: "1", GP_AUTO_CREATE_PROJECTS: "0", GP_PROJECT_REUSE: "managed",
      GP_GUEST_MAX_PROJECTS: "1", GP_FLEET_MANIFEST_FILE: path.join(directory, "fleet.json"), GP_PROVISION_CONCURRENCY: "1",
      GP_COLD_START_CONCURRENCY: "1", GP_V2_SIDECAR_REVISION: "r10", GP_V2_SIDECAR_PROJECT_REVISIONS: "",
      GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay", GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "git-ssh-signing", GP_V2_DIAGNOSTICS: "0" }, {
      sessionManager: session, projects, logger,
      manifest: new FleetManifestStore({ file: path.join(directory, "fleet.json"), identityFingerprint: identity.fingerprint }),
      sandboxSupervisorFactory: input => {
        if (input.binding.projectId !== id || supervisor || input.bootstrapMode !== "git-ssh-signing") throw new Error("Only the original candidate supervisor is allowed");
        const adaptedGateway = new Proxy(input.gateway, {
          get(target, property, receiver) {
            if (property === "startSupervisorExec") return () => { blocked.codexExec++; throw new Error("Direct launch forbidden in control"); };
            if (property === "postJson") return async (operation: string, body: unknown, signal?: AbortSignal) => {
              if (!["init", "write-directory", "rename-file", "token", "delete-file"].includes(operation)) throw new Error("Unapproved candidate filesystem operation");
              if (operation === "token") { bridgeStarts++; if (++tokenSubmissions !== 1) throw new Error("Control Git initialization was resubmitted"); }
              return (target as typeof target & FsGateway).postJson(operation, body, signal);
            };
            if (property === "fetchText") return async (suffix: string, init?: RequestInit) => {
              if (!suffix.startsWith("write-file?filename=") && !suffix.startsWith("codex/read_codex_workspace_file?path=")) throw new Error("Unapproved candidate filesystem route");
              if (suffix.startsWith("write-file?") && typeof init?.body === "string" && /(?:^|\n)\s*fsmonitor\s*=/.test(init.body)) {
                blocked.fsmonitor++; throw new Error("The filesystem-monitor launcher is blocked");
              }
              const response = await target.fetchText(suffix, init);
              if (suffix.startsWith("codex/read_codex_workspace_file?") && response.status === 200) {
                const query = new URL(suffix, "https://offline.invalid/");
                if (query.searchParams.get("path") === ".ghostprov-v2/ssh-bootstrap/result.json") {
                  const sessionId = query.searchParams.get("session_id");
                  const wrapper: unknown = JSON.parse(response.text);
                  if (!sessionId || !wrapper || typeof wrapper !== "object" || !("body_b64" in wrapper) || typeof wrapper.body_b64 !== "string" || wrapper.body_b64.length > 4096) throw new Error("Invalid control receipt");
                  controlReceipt = parseGitSshSigningResult(JSON.parse(Buffer.from(wrapper.body_b64, "base64").toString("utf8")), sessionId);
                  controlSessionId = sessionId;
                }
              }
              return response;
            };
            const value = Reflect.get(target, property, receiver);
            return typeof value === "function" ? value.bind(target) : value;
          },
        }) as V2BootstrapGateway;
        evidenceGateway = adaptedGateway as V2BootstrapGateway & FsGateway;
        const value = new V2SandboxSupervisor({ ...input, gateway: adaptedGateway,
          hostFactory: options => reconnect.createHost(options, 1, 1) });
        supervisor = value;
        const launch = value.launch.bind(value);
        value.launch = async (request, signal) => {
          const result = await launch(request, signal);
          if (value.instanceId === "pending" || (supervisorInstance !== null && supervisorInstance !== value.instanceId)) throw new Error("Child launch lacked stable supervisor identity");
          supervisorInstance = value.instanceId;
          sidecarLaunches++;
          childPid = result.process.pid;
          latestNonce = request.sidecarBinding.nonce;
          return result;
        };
        return value;
      },
    }, AbortSignal.timeout(180_000));
    event("starting");
    await system.start();
    await waitIdle();
    const running = selectedSupervisor();
    if (!running || sidecarLaunches !== 1 || bridgeStarts !== 1 || tokenSubmissions !== 1 || childPid === null) throw new Error("Missing signed R10 startup proof");
    supervisorInstance = running.instanceId;
    if (supervisorInstance === "pending") throw new Error("Supervisor identity was not authenticated");
    readyProved = true;
    timings.startupMs = Math.round(performance.now() - started);
    event("signed_ready", { sidecarLaunches, bridgeStarts, elapsedMs: timings.startupMs });
    stage = "reconnect";
    for (let round = 0; round < 3; round++) {
      await waitIdle();
      await reconnect.forceReconnect();
      if (running.instanceId !== supervisorInstance || Number(sidecarLaunches) !== 1 || bridgeStarts !== 1) throw new Error("Reconnect changed launch identity");
      reconnectsProved++;
      event("reconnected", { reconnectsProved });
    }
    stage = "recovery";
    await recoverChild();
    stage = "model_request";
    await sendOneRequest();
    stage = "stability";
    await waitIdle();
    const deadline = performance.now() + SOAK_MS;
    let progressAt = performance.now() + 15_000;
    while (performance.now() < deadline) {
      await Bun.sleep(1000);
      const snapshot = selectedSystem().registry.get(id)?.snapshot();
      if (!snapshot || !usable(snapshot) || running.instanceId !== supervisorInstance || Number(sidecarLaunches) !== 2 || bridgeStarts !== 1) throw new Error("Candidate lost stable ready capacity");
      if (performance.now() >= progressAt) { event("stability_sample", { elapsedMs: Math.round(performance.now() - (deadline - SOAK_MS)) }); progressAt += 15_000; }
    }
    stable = true;
    stage = "signed_shutdown";
    shutdownProved = await proveSignedShutdown(running, supervisorInstance, "git_ssh_signing_proof_complete", AbortSignal.timeout(45_000));
    stage = "execution_cleanup";
    await confirmControlCleanup();
    if (Object.values(blocked).some(value => value !== 0)) throw new Error("An unapproved execution route was attempted");
  } catch (error) {
    failure = safeError(error);
    const diagnosticSupervisor = selectedSupervisor();
    if (diagnosticSupervisor?.instanceId !== "pending") {
      for (const file of ["supervisor.jsonl", "sidecar.jsonl"] as const) {
        try {
          const log = await diagnosticSupervisor?.readDiagnosticLog(file, AbortSignal.timeout(5000));
          if (log && Buffer.byteLength(log) <= 128 * 1024) {
            const records = log.split("\n").filter(Boolean).slice(-24).map(parseDiagnosticLine);
            failureDiagnostics.push({ file, records });
          }
        } catch { /* Failure diagnostics are optional and cannot authorize cleanup. */ }
      }
    }
    event("failed", { stage, ...failure, hookFailure });
  } finally {
    const closing = selectedSupervisor();
    if (!shutdownProved && closing) {
      try { shutdownProved = await proveSignedShutdown(closing, supervisorInstance, "git_ssh_signing_cleanup", AbortSignal.timeout(45_000)); }
      catch (error) { failure ??= safeError(error); }
    }
    if (shutdownProved && !workspaceRemoved) {
      try { await confirmControlCleanup(); }
      catch (error) { failure ??= safeError(error); }
    }
    await system?.close().catch(error => { failure ??= safeError(error); });
    if (V2BootstrapController.prototype.ensureReady === observedEnsure) V2BootstrapController.prototype.ensureReady = originalEnsure;
    await session?.close().catch(error => { failure ??= safeError(error); });
    let cleanup: PrismSessionManager | null = null;
    try {
      if (projectAttempted) {
        cleanup = await PrismSessionManager.create({ origin: "https://prism.openai.com", mode: "authenticated", allowAnonymous: false, cookieFile });
        const identity = await cleanup.resolve(AbortSignal.timeout(60_000));
        const projects = new PrismProjectsClient(identity.account);
        const current = await projects.get(id, AbortSignal.timeout(30_000)).catch(error => {
          if (error && typeof error === "object" && "status" in error && (error.status === 404 || error.status === 410)) return null;
          throw error;
        });
        if (current && current.title !== title) throw new Error("Disposable project ownership changed before deletion");
        if (current && !current.deleted) await projects.deleteManaged(id, AbortSignal.timeout(60_000));
        const after = await projects.get(id, AbortSignal.timeout(30_000)).catch(error => {
          if (error && typeof error === "object" && "status" in error && (error.status === 404 || error.status === 410)) return null;
          throw error;
        });
        projectDeleted = after === null || after.deleted;
      }
    } catch (error) { failure ??= safeError(error); }
    await cleanup?.close().catch(error => { failure ??= safeError(error); });
    for (const file of [cookieFile, `${cookieFile}.identity.json`, `${cookieFile}.lock`]) {
      try {
        if (!path.resolve(file).startsWith(directory + path.sep)) throw new Error("Local cleanup escaped the owned directory");
        await rm(file, { force: true });
      } catch (error) { failure ??= safeError(error); }
    }
    credentialDeleted = credentialCopied && await stat(cookieFile).then(() => false, error => error?.code === "ENOENT");
    await logger.close().catch(error => { failure ??= safeError(error); });
    const reconnectProof = reconnect.snapshot();
    const faultProof = reconnectProof.length === 1 && reconnectProof[0]?.authenticatedRestores === 3 &&
      reconnectProof[0]?.injectedChallengeDrops === 1 && reconnectProof[0]?.injectedReplyDrops === 1 && reconnectProof[0]?.failures === 0;
    const success = failure === null && projectCreated && projectDeleted && credentialDeleted && bridgeStarts === 1 && tokenSubmissions === 1 &&
      readyProved && sidecarLaunches === 2 && reconnectsProved === 3 && faultProof && recoveryProved && modelRequestAttempts === 1 &&
      responseCompleted && stable && shutdownProved && workspaceRemoved && receipt()?.outcome === "submitted" &&
      Object.values(blocked).every(value => value === 0);
    const report = { live: true, method: "git-ssh-signing", revision: "r10", hookSha256: GIT_SSH_SIGNING_HOOK_SHA256,
      signerSha256: GIT_SSH_SIGNER_SHA256, buildHash: V2_R10_RELEASE.buildHash, bundleSha256: V2_R10_RELEASE.bundleSha256,
      candidatePort: 18888, projectCreated, projectDeleted, credentialDeleted, bridgeStarts, tokenSubmissions,
      readyProved, sidecarLaunches, reconnectsProved, reconnectProof, recoveryProved, modelRequestAttempts, responseCompleted,
      stableForMs: stable ? SOAK_MS : 0, signedShutdown: shutdownProved, executionWorkspaceRemoved: workspaceRemoved,
      configRestored: receipt()?.configRestored ?? false, configLockReleased: receipt()?.lockReleased ?? false,
      signingComplete: receipt()?.signingComplete ?? false, signingKeysRemoved: receipt()?.keysRemoved ?? false,
      blocked, timings, elapsedMs: Math.round(performance.now() - started), success, failureStage: failure ? stage : null, failure, hookFailure, failureDiagnostics, bootstrapFailures };
    await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
    event("result", { ...report, run: path.basename(directory) });
    if (!success) process.exitCode = 1;
  }
}

if (import.meta.main) {
  await runLiveGitSshSigning().catch(() => { console.error("Git SSH signing verifier stopped by its guard or preflight"); process.exitCode = 1; });
}
