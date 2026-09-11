import { copyFile, mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { createHash } from "node:crypto";
import path from "node:path";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { FleetProjectsClient } from "../src/fleet/manager";
import type { ObservabilityEvent, ObservabilityPriority } from "../src/observability/events";
import type { LoggerLike, LoggerSnapshot } from "../src/observability/logger";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { isRuntimeHealthy } from "../src/router/policy";
import { SandboxHeartbeat } from "../src/sandbox/heartbeat";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { createBrowserlessSystem, type BrowserlessSystem } from "../src/server/system";
import { YSweetAwarenessChannel } from "../src/v2/awareness/y-sweet-channel";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import type { HostFrame, R2RequestStartFrame, SidecarFrame } from "../src/v2/protocol/shared";
import { V2SandboxSupervisor } from "../src/v2/supervisor/controller";
import { V2SupervisorHost } from "../src/v2/supervisor/host";
import { checkR10Sidecar } from "./build_v2_sidecar_r10";

const PORT = 18_888;
const TARGET = 8;
const OUTAGES = [30_000, 60_000, 90_000] as const;
const MAX_REQUESTS = TARGET * (OUTAGES.length + 1) + 2;
const MIXED_INPUT_BYTES = [1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024, 16 * 1024, 64 * 1024, 256 * 1024] as const;

class ProofFailure extends Error {
  constructor(readonly code: string) { super(code); }
}

export function resolveTransportOutageProofOptions(env: Record<string, string | undefined>) {
  if (env.GP_LIVE_TRANSPORT_OUTAGE_RECOVERY !== "1" || env.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1") {
    throw new ProofFailure("explicit_live_and_test_project_guards_required");
  }
  if (Number(env.PORT ?? PORT) !== PORT) throw new ProofFailure("candidate_port_must_be_18888");
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new ProofFailure("explicit_cookie_file_required");
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$(?![\s\S])/.test(model)) throw new ProofFailure("invalid_model");
  return { cookieFile: path.resolve(cookieFile), model, origin: env.PRISM_ORIGIN ?? "https://prism.openai.com" };
}

/** A fulfilled controller shutdown can be a no-op; only the host's signed reply is proof. */
export class OutageSupervisorProof {
  readonly #records = new Map<string, { project: string; submitted: boolean; signedShutdown: boolean }>();
  readonly #latest = new Map<string, string>();

  register(project: string, nonce: string): void {
    if (this.#records.has(nonce) || this.#records.size >= TARGET * 16) throw new ProofFailure("supervisor_identity_bound_or_conflict");
    this.#records.set(nonce, { project, submitted: false, signedShutdown: false });
  }

  submit(project: string, nonce: string): void {
    const record = this.#records.get(nonce);
    const previous = this.#latest.get(project);
    if (!record || record.project !== project || record.submitted || record.signedShutdown ||
      (previous && !this.#records.get(previous)?.signedShutdown)) throw new ProofFailure("bootstrap_submission_replay_forbidden");
    record.submitted = true;
    this.#latest.set(project, nonce);
  }

  signedShutdown(project: string, nonce: string): void {
    const record = this.#records.get(nonce);
    if (!record || record.project !== project || !record.submitted) throw new ProofFailure("shutdown_proof_identity_conflict");
    record.signedShutdown = true;
  }

  snapshot() {
    const submitted = [...this.#records.values()].filter(record => record.submitted);
    return { submitted: submitted.length, signedShutdowns: submitted.filter(record => record.signedShutdown).length,
      unprovedShutdowns: submitted.filter(record => !record.signedShutdown).length };
  }
}

function object(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

export function validateOutageTerminal(value: unknown, marker: string, lane: "responses" | "anthropic"): string {
  const response = object(value);
  if (!response || typeof response.id !== "string" || response.id.length < 1 || response.id.length > 256 ||
    (lane === "responses" ? response.status !== "completed" : response.type !== "message" || response.stop_reason !== "end_turn")) {
    throw new ProofFailure("invalid_provider_terminal");
  }
  const items = lane === "anthropic" ? [response] : response.output;
  if (!Array.isArray(items) || items.length > 4096) throw new ProofFailure("invalid_terminal_output");
  let text = "";
  for (const item of items) {
    const content = object(item)?.content;
    if (!Array.isArray(content)) continue;
    if (content.length > 4096) throw new ProofFailure("terminal_content_count_bound");
    for (const value of content) {
      const part = object(value);
      if (part?.type !== (lane === "anthropic" ? "text" : "output_text") || typeof part.text !== "string") continue;
      if (text.length + part.text.length > 1024 * 1024) throw new ProofFailure("terminal_text_bound");
      text += part.text;
    }
  }
  // A per-request delimiter rejects another worker's marker and prefix aliases.
  if (!text.trimEnd().endsWith(marker)) throw new ProofFailure("terminal_marker_mismatch");
  return response.id;
}

type WireTerminal = Extract<SidecarFrame, { kind: "response_terminal" }>;
interface RequestProof {
  readonly project: string | null;
  executionProject: string | null;
  requestId: string | null;
  fence: string | null;
  digest: string | null;
  responseId: string | null;
  startPublications: number;
  events: number;
  accepted: boolean;
}

/** Counts immutable execution identities; repeated reliable publications are retransmissions. */
export class OutageRequestProof {
  readonly #requests = new Map<string, RequestProof>();
  readonly #byRequest = new Map<string, RequestProof>();
  readonly #active = new Map<string | null, RequestProof>();
  readonly #responses = new Set<string>();

  begin(marker: string, project: string | null): void {
    if (this.#requests.size >= MAX_REQUESTS || this.#requests.has(marker) || this.#active.has(project)) throw new ProofFailure("request_proof_bound_or_conflict");
    const record: RequestProof = { project, executionProject: null, requestId: null, fence: null, digest: null, responseId: null, startPublications: 0, events: 0, accepted: false };
    this.#requests.set(marker, record);
    this.#active.set(project, record);
  }

  start(project: string, frame: R2RequestStartFrame): void {
    const fence = JSON.stringify([project, frame.projectId, frame.requestId, frame.sessionId, frame.leaderGeneration,
      frame.buildHash, frame.schemaHash, frame.launchNonce, frame.hostInstanceId, frame.targetInstanceId,
      frame.requestDigest, frame.decodedBytes, frame.encodedBytes, frame.encoding, frame.partCount]);
    let record = this.#byRequest.get(frame.requestId);
    if (record) {
      if (record.fence !== fence) throw new ProofFailure("request_execution_identity_changed");
    } else {
      record = this.#active.get(project) ?? this.#active.get(null);
      if (!record || record.requestId !== null || project !== frame.projectId) throw new ProofFailure("request_execution_replayed_or_untracked");
      record.requestId = frame.requestId;
      record.executionProject = project;
      record.fence = fence;
      record.digest = frame.requestDigest;
      this.#byRequest.set(frame.requestId, record);
    }
    if (++record.startPublications > 1_000_000) throw new ProofFailure("request_publication_bound");
  }

  terminal(project: string, frame: WireTerminal): void {
    const record = this.#byRequest.get(frame.requestId);
    if (!record || record.executionProject !== project || record.digest !== frame.requestDigest || frame.terminalStatus !== "completed" ||
      (record.responseId !== null && record.responseId !== frame.responseId)) throw new ProofFailure("wire_terminal_identity_mismatch");
    if (record.responseId === null) {
      if (this.#responses.has(frame.responseId)) throw new ProofFailure("duplicate_provider_terminal");
      this.#responses.add(frame.responseId);
      record.responseId = frame.responseId;
    }
  }

  event(marker: string): void {
    const record = this.#requests.get(marker);
    if (!record || ++record.events > 100_000) throw new ProofFailure("request_event_bound");
  }

  complete(marker: string, value: unknown, lane: "responses" | "anthropic"): void {
    const id = validateOutageTerminal(value, marker, lane);
    const record = this.#requests.get(marker);
    if (!record || record.accepted || record.responseId === null || (lane === "responses" && record.responseId !== id)) {
      throw new ProofFailure("public_terminal_lacks_exact_wire_proof");
    }
    record.accepted = true;
    this.#active.delete(record.project);
  }

  snapshot() {
    return { invoked: this.#requests.size, executionIdentities: this.#byRequest.size, providerTerminals: this.#responses.size,
      accepted: [...this.#requests.values()].filter(record => record.accepted).length,
      pending: this.#active.size, requests: [...this.#requests.values()].map(record => ({
        startPublications: record.startPublications, events: record.events, terminalObserved: record.responseId !== null, accepted: record.accepted,
      })) };
  }
}

class ProofLogger implements LoggerLike {
  recoveryStarts = 0;
  recoveryWaits = 0;
  sinkErrors = 0;
  emit(event: ObservabilityEvent, _priority?: ObservabilityPriority): void {
    try { assertSafeObservabilityEvent(event); } catch { this.sinkErrors++; return; }
    if (event.event === "sandbox.recovery.started") this.recoveryStarts++;
    if (event.event === "sandbox.liveness" && event.liveness_outcome === "waiting_for_transport_recovery") this.recoveryWaits++;
    if (event.event.startsWith("sandbox.recovery.")) console.log(JSON.stringify({
      event: event.event, worker: event.worker_index, cause: event.cause,
      bootstrap_code: event.bootstrap_code, bootstrap_reason: event.bootstrap_reason,
      error_name: event.error_name, error_kind: event.error_kind,
    }));
  }
  snapshot(): LoggerSnapshot {
    return { queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: this.sinkErrors };
  }
  async close(): Promise<void> {}
}

interface LossState {
  startMs: number | null;
  untilMs: number;
  dataDropped: number;
  controlDropped: number;
  heartbeatsDropped: number;
}

function contained(root: string, target: string): void {
  const relative = path.relative(path.resolve(root), path.resolve(target));
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new ProofFailure("cleanup_path_escaped_test_root");
}

async function candidatePortAvailable(): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const server = createServer();
    server.once("error", reject);
    server.listen(PORT, "127.0.0.1", () => server.close(error => error ? reject(error) : resolve()));
  });
}

async function responseJson(response: Response): Promise<Record<string, unknown>> {
  if (!response.body) throw new ProofFailure("missing_public_response_body");
  const bytes = new Uint8Array(1024 * 1024);
  const reader = response.body.getReader();
  let length = 0;
  try {
    for (;;) {
      const item = await reader.read();
      if (item.done) break;
      if (length + item.value.byteLength > bytes.byteLength) throw new ProofFailure("public_response_byte_limit");
      bytes.set(item.value, length);
      length += item.value.byteLength;
    }
    const value: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length)));
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new ProofFailure("invalid_public_response");
    return value as Record<string, unknown>;
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

async function waitFor(check: () => boolean, timeoutMs: number, progress: () => void): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  let nextProgress = 0;
  while (!check()) {
    if (performance.now() >= deadline) throw new ProofFailure("phase_deadline_expired");
    if (performance.now() >= nextProgress) { progress(); nextProgress = performance.now() + 15_000; }
    await Bun.sleep(250);
  }
}

async function bounded<T>(operation: Promise<T>, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([operation, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new ProofFailure("operation_deadline_expired")), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}

export async function main(env: Record<string, string | undefined> = process.env): Promise<void> {
  const options = resolveTransportOutageProofOptions(env);
  const { identity: release } = await checkR10Sidecar();
  await candidatePortAvailable();
  const root = path.resolve(".ghostprov", "live-outage-recovery");
  await mkdir(root, { recursive: true });
  const directory = await mkdtemp(path.join(root, "proof-"));
  contained(root, directory);
  const cookieFile = path.join(directory, "candidate-cookies.json");
  const intended = Array.from({ length: TARGET }, () => projectId(crypto.randomUUID()));
  const titles = new Map(intended.map((id, index) => [id, `GhostProv Outage Proof ${path.basename(directory)} ${index + 1}`]));
  const cleanupEligible = new Set<ProjectId>();
  const deleted = new Set<ProjectId>();
  const loss = new Map(intended.map(id => [id, { startMs: null, untilMs: 0, dataDropped: 0, controlDropped: 0, heartbeatsDropped: 0 } as LossState]));
  const tracked = new Map<ProjectId, { creations: number; launches: number; submissions: number; shutdowns: number }>();
  const supervisors = new OutageSupervisorProof();
  const requests = new OutageRequestProof();
  const waves: Array<Record<string, unknown>> = [];
  const pongProof = new Map(intended.map(id => [id, { last: -1, dropped: 0, retained: 0 }]));
  const logger = new ProofLogger();
  const apiKey = `outage-proof-${crypto.randomUUID()}`;
  let session: PrismSessionManager | null = null;
  let projects: PrismProjectsClient | null = null;
  let system: BrowserlessSystem | null = null;
  let failure: unknown = null;
  let phase = "create";
  let outageMs = 0;
  let stabilitySamples = 0;
  let journalFilesAfterClose: number | null = null;
  let activeWave: { controller: AbortController; settled: Promise<unknown> } | null = null;
  const blocked = (id: ProjectId) => {
    const state = loss.get(id);
    if (!state) throw new ProofFailure("transport_outside_test_project_ledger");
    return state.startMs !== null && performance.now() < state.untilMs;
  };
  const ready = () => system?.registry.snapshots().filter(value => isRuntimeHealthy(value) && value.available > 0).length ?? 0;
  const progress = () => console.log(JSON.stringify({
    event: "outage.proof.progress", phase, ready: ready(), target: TARGET,
    blocked: intended.filter(blocked).length, recoveryStarts: logger.recoveryStarts,
  }));
  try {
    await copyFile(options.cookieFile, cookieFile);
    session = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
    const identity = await session.resolve(AbortSignal.timeout(60_000));
    projects = new PrismProjectsClient(identity.account);
    const preexisting = new Set((await projects.list(AbortSignal.timeout(60_000))).map(item => item.projectId));
    for (const id of intended) {
      if (preexisting.has(id)) throw new ProofFailure("preexisting_project_collision");
      cleanupEligible.add(id);
      await writeFile(path.join(directory, "projects.json"), JSON.stringify({ intended, cleanupEligible: [...cleanupEligible], titles: Object.fromEntries(titles) }, null, 2));
      await projects.create({ projectId: id, title: titles.get(id)! }, AbortSignal.timeout(60_000));
    }
    const source = projects;
    const guardedProjects: FleetProjectsClient = {
      get: (id, signal) => {
        if (!cleanupEligible.has(id)) throw new ProofFailure("project_read_outside_test_ledger");
        return source.get(id, signal);
      },
      create: async () => { throw new ProofFailure("unexpected_fleet_project_creation"); },
      deleteManaged: async () => { throw new ProofFailure("unexpected_fleet_project_deletion"); },
    };
    phase = "startup";
    system = await createBrowserlessSystem({
      ...env, PORT: String(PORT), GP_CANDIDATE: "1", GHOSTPROV_API_KEY: apiKey,
      PRISM_COOKIE_FILE: cookieFile, PRISM_COOKIE: undefined, GP_PRISM_SESSION: "authenticated", GP_ALLOW_ANONYMOUS: "0",
      PRISM_PROJECT_IDS: intended.join(","), PRISM_DOC_ID: "", GP_TARGET_SANDBOXES: String(TARGET),
      GP_MAX_PROJECTS: String(TARGET), GP_AUTO_CREATE_PROJECTS: "0", GP_PROJECT_REUSE: "managed",
      GP_GUEST_MAX_PROJECTS: "1", GP_FLEET_MANIFEST_FILE: path.join(directory, "fleet.json"),
      GP_PROVISION_CONCURRENCY: "4", GP_COLD_START_CONCURRENCY: "2",
      GP_V2_SIDECAR_REVISION: "r10", GP_V2_SIDECAR_PROJECT_REVISIONS: "", GP_V2_BOOTSTRAP_LAUNCH_MODE: "relay",
      GP_V2_SUPERVISOR_BOOTSTRAP_MODE: "direct", GP_LOG_DIR: path.join(directory, "logs"),
      GP_V2_DIAGNOSTIC_ROOT: path.join(directory, "diagnostics"),
    }, {
      sessionManager: session, projects: guardedProjects, logger,
      manifest: new FleetManifestStore({ file: path.join(directory, "fleet.json"), identityFingerprint: identity.fingerprint }),
      heartbeatFactory: ({ projectId: id, gateway, callbacks }) => {
        const heartbeat = gateway.heartbeat.bind(gateway);
        gateway.heartbeat = signal => {
          if (blocked(id)) { loss.get(id)!.heartbeatsDropped++; return Promise.reject(new Error("controlled test heartbeat loss")); }
          return heartbeat(signal);
        };
        return new SandboxHeartbeat({ projectId: id, gateway, ...callbacks });
      },
      awarenessChannelFactory: input => {
        const channel = new YSweetAwarenessChannel<HostFrame, SidecarFrame>(input);
        if (!loss.has(channel.projectId)) throw new ProofFailure("channel_outside_test_ledger");
        const publish = channel.publish.bind(channel);
        const subscribe = channel.subscribe.bind(channel);
        const drop = () => {
          if (!blocked(channel.projectId)) return false;
          const state = loss.get(channel.projectId)!;
          if (input.field === "ghostprovV2Control") state.controlDropped++;
          else state.dataDropped++;
          return true;
        };
        channel.publish = envelope => {
          for (const frame of envelope.frames) if (frame.kind === "request_start") {
            try { requests.start(channel.projectId, frame); } catch (error) { failure ??= error; throw error; }
          }
          if (!drop()) publish(envelope);
        };
        channel.subscribe = listener => subscribe(update => {
          if (drop()) return;
          if (phase === "mixed_workload" || phase === "retained_pong") {
            const pong = update.envelope.liveness;
            if (input.field === "ghostprovV2Control" && pong?.kind === "pong") {
              const proof = pongProof.get(channel.projectId)!;
              if (pong.sentAtMs !== proof.last) {
                proof.last = pong.sentAtMs;
                if (++proof.dropped > 1000) failure ??= new ProofFailure("pong_probe_bound");
                return;
              }
              proof.retained++;
            }
          }
          for (const frame of update.envelope.frames) if (frame.kind === "response_terminal") {
            try { requests.terminal(channel.projectId, frame); } catch (error) { failure ??= error; }
          }
          listener(update);
        });
        return channel;
      },
      sandboxSupervisorFactory: input => {
        const id = projectId(input.binding.projectId);
        if (!loss.has(id)) throw new ProofFailure("supervisor_outside_test_ledger");
        const state = tracked.get(id) ?? { creations: 0, launches: 0, submissions: 0, shutdowns: 0 };
        tracked.set(id, state);
        if (++state.creations > 16) throw new ProofFailure("startup_recovery_attempt_bound");
        supervisors.register(id, input.binding.nonce);
        const submit = input.gateway.startSupervisorExec?.bind(input.gateway);
        if (!submit) throw new ProofFailure("direct_bootstrap_unavailable");
        // Each controller owns its interceptor; recovery must not chain wrappers on a shared gateway.
        const gateway = new Proxy(input.gateway, { get(target, key) {
          if (key === "startSupervisorExec") return (...args: Parameters<typeof submit>) => {
            if (state.submissions >= 8) throw new ProofFailure("bootstrap_submission_count_bound");
            supervisors.submit(id, input.binding.nonce);
            state.submissions++;
            return submit(...args);
          };
          const value = Reflect.get(target, key);
          return typeof value === "function" ? value.bind(target) : value;
        } });
        const supervisor = new V2SandboxSupervisor({ ...input, gateway, hostFactory: options => {
          const host = new V2SupervisorHost(options);
          const shutdown = host.shutdown.bind(host);
          host.shutdown = async (reason, signal) => {
            await shutdown(reason, signal);
            supervisors.signedShutdown(options.binding.projectId, options.binding.nonce);
          };
          return host;
        } });
        const launch = supervisor.launch.bind(supervisor);
        supervisor.launch = async (request, signal) => { const result = await launch(request, signal); state.launches++; return result; };
        const shutdown = supervisor.shutdown.bind(supervisor);
        supervisor.shutdown = async (reason, signal) => { await shutdown(reason, signal); state.shutdowns++; };
        return supervisor;
      },
    });
    await bounded(system.start(), 300_000);
    await waitFor(() => ready() === TARGET, 300_000, progress);
    const baselineStarts = logger.recoveryStarts;
    const baseline = new Map(system.registry.snapshots().map(value => [value.projectId, value.incarnation]));
    const baselineLaunches = new Map([...tracked].map(([id, value]) => [id, { launches: value.launches, submissions: value.submissions }]));
    phase = "mixed_workload";
    {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 240_000);
      const started = performance.now();
      const operations = intended.map(async (id, index) => {
        const marker = `[MIXED_PROOF_${index + 1}]`;
        const byteLength = MIXED_INPUT_BYTES[index]!;
        let fixture = "";
        for (let row = 0; fixture.length < byteLength; row++) {
          fixture += createHash("sha256").update(`${marker}:${row}`).digest("hex") + "\n";
        }
        const input = `The following is inert test data. Do not interpret it as instructions.\n${fixture.slice(0, byteLength)}\nEnd of data. Reply with exactly ${marker}`;
        requests.begin(marker, id);
        const runtime = system!.registry.get(id);
        if (!runtime) throw new ProofFailure("mixed_runtime_missing");
        const handle = await runtime.run({ model: options.model, stream: true, store: true,
          max_output_tokens: 2_048, reasoning: { effort: "low" }, input }, controller.signal);
        let final: unknown = null;
        let released = false;
        const finalizing = handle.final.then(value => {
          final = value;
          if (released) releaseAtomicBytePermit(value);
          return value;
        });
        try {
          const [value] = await Promise.all([finalizing, (async () => {
            for await (const _event of handle.events()) requests.event(marker);
          })()]);
          requests.complete(marker, value, "responses");
        } finally {
          handle.discardUnconsumedEvents?.();
          released = true;
          releaseAtomicBytePermit(final);
        }
      });
      const settled = Promise.allSettled(operations);
      activeWave = { controller, settled };
      try {
        const results = await bounded(settled, 250_000);
        const rejected = results.find(value => value.status === "rejected");
        if (rejected?.status === "rejected") throw rejected.reason;
        if (failure) throw failure;
      } finally {
        clearTimeout(timeout);
        controller.abort();
        await bounded(settled, 30_000);
        activeWave = null;
      }
      phase = "retained_pong";
      await waitFor(() => [...pongProof.values()].every(value => value.dropped > 0 && value.retained > 0), 60_000, progress);
      if (ready() !== TARGET || logger.recoveryStarts !== baselineStarts ||
        [...tracked].some(([id, value]) => value.launches !== baselineLaunches.get(id)?.launches || value.submissions !== baselineLaunches.get(id)?.submissions)) {
        throw new ProofFailure("healthy_pong_loss_triggered_recovery");
      }
      console.log(JSON.stringify({ event: "outage.proof.mixed_passed", completed: operations.length,
        inputBytes: MIXED_INPUT_BYTES, elapsedMs: Math.round(performance.now() - started), pongProof: [...pongProof.values()] }));
    }
    for (const duration of OUTAGES) {
      phase = `outage_${duration / 1000}s`;
      outageMs = duration;
      for (const state of loss.values()) Object.assign(state, { startMs: null, untilMs: 0, dataDropped: 0, controlDropped: 0, heartbeatsDropped: 0 });
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 240_000);
      const started = performance.now();
      const operations = intended.map(async (id, index) => {
        const runtime = system!.registry.get(id);
        if (!runtime) throw new ProofFailure("test_runtime_missing");
        const marker = `[OUTAGE_PROOF_${duration}_${index + 1}]`;
        requests.begin(marker, id);
        const handle = await runtime.run({
          model: options.model, stream: true, store: true, max_output_tokens: 2_048, reasoning: { effort: "low" },
          input: `Write about 250 words describing how rain forms. End with exactly ${marker}`,
        }, controller.signal);
        let final: unknown = null;
        let released = false;
        const finalizing = handle.final.then(value => {
          final = value;
          if (released) releaseAtomicBytePermit(value);
          return value;
        });
        try {
          const [value] = await Promise.all([
            finalizing,
            (async () => {
              for await (const _event of handle.events()) {
                requests.event(marker);
                const state = loss.get(id)!;
                if (state.startMs === null) { state.startMs = performance.now(); state.untilMs = state.startMs + outageMs; }
              }
            })(),
          ]);
          requests.complete(marker, value, "responses");
          if (blocked(id)) throw new ProofFailure("request_completed_before_its_outage_ended");
        } finally {
          handle.discardUnconsumedEvents?.();
          released = true;
          releaseAtomicBytePermit(final);
        }
      });
      const settled = Promise.allSettled(operations);
      activeWave = { controller, settled };
      let finished = false;
      void settled.then(() => { finished = true; });
      try {
        await waitFor(() => finished, 250_000, progress);
        const results = await settled;
        if (failure) throw failure;
        const rejected = results.find(value => value.status === "rejected");
        if (rejected?.status === "rejected") throw rejected.reason;
      } finally {
        clearTimeout(timeout);
        controller.abort();
        for (const state of loss.values()) state.untilMs = 0;
        await bounded(settled, 30_000);
        activeWave = null;
      }
      await waitFor(() => ready() === TARGET, 60_000, progress);
      if (logger.recoveryStarts !== baselineStarts || system.registry.snapshots().some(value => value.incarnation !== baseline.get(value.projectId))) {
        throw new ProofFailure("outage_restarted_a_runtime");
      }
      if ([...tracked].some(([id, value]) => value.launches !== baselineLaunches.get(id)?.launches ||
        value.submissions !== baselineLaunches.get(id)?.submissions)) throw new ProofFailure("outage_relaunched_a_sidecar");
      if ([...loss.values()].some(value => value.startMs === null || value.dataDropped === 0 || value.controlDropped === 0 || value.heartbeatsDropped === 0)) {
        throw new ProofFailure("outage_did_not_cover_every_control_path");
      }
      const wave = { outageMs: duration, completed: operations.length, elapsedMs: Math.round(performance.now() - started), workers: [...loss.values()].map(value => ({ dataDropped: value.dataDropped, controlDropped: value.controlDropped, heartbeatsDropped: value.heartbeatsDropped })) };
      waves.push(wave);
      console.log(JSON.stringify({ event: "outage.proof.wave_passed", ...wave }));
    }
    phase = "public_surfaces";
    for (const route of ["/v1/responses", "/v1/messages"]) {
      const marker = route === "/v1/responses" ? "[OUTAGE_PUBLIC_RESPONSES_OK]" : "[OUTAGE_PUBLIC_ANTHROPIC_OK]";
      requests.begin(marker, null);
      const body = route === "/v1/responses"
        ? { model: options.model, stream: false, input: `Reply with exactly ${marker}`, max_output_tokens: 2_048, reasoning: { effort: "low" } }
        : { model: options.model, stream: false, max_tokens: 2_048, messages: [{ role: "user", content: `Reply with exactly ${marker}` }] };
      const response = await fetch(`http://127.0.0.1:${PORT}${route}`, {
        method: "POST", headers: { authorization: `Bearer ${apiKey}`, "content-type": "application/json",
          ...(route === "/v1/messages" ? { "anthropic-version": "2023-06-01" } : {}) },
        body: JSON.stringify(body), signal: AbortSignal.timeout(90_000),
      });
      if (!response.ok) { await response.body?.cancel(); throw new ProofFailure("public_request_failed"); }
      const value = await responseJson(response);
      requests.complete(marker, value, route === "/v1/responses" ? "responses" : "anthropic");
      if (failure) throw failure;
    }
    phase = "stability";
    for (let second = 0; second < 120; second++) {
      if (ready() !== TARGET || logger.recoveryStarts !== baselineStarts) throw new ProofFailure("stability_lost_readiness");
      stabilitySamples++;
      if (second % 15 === 0) progress();
      await Bun.sleep(1_000);
    }
    if (logger.sinkErrors !== 0) throw new ProofFailure("unsafe_observability_event");
  } catch (error) {
    failure = error;
  } finally {
    for (const state of loss.values()) state.untilMs = 0;
    activeWave?.controller.abort();
    if (activeWave) try { await bounded(activeWave.settled, 30_000); } catch (error) { failure ??= error; }
    try { await system?.close(); } catch (error) { failure ??= error; }
    try {
      journalFilesAfterClose = (await readdir(path.join(directory, "fleet.json.bootstrap"))).filter(name => name.endsWith(".json")).length;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") journalFilesAfterClose = 0;
      else failure ??= error;
    }
    if (supervisors.snapshot().unprovedShutdowns !== 0) failure ??= new ProofFailure("supervisor_shutdown_unproven");
    if (journalFilesAfterClose !== 0) failure ??= new ProofFailure("bootstrap_journal_cleanup_unproven");
    try { await session?.close(); } catch (error) { failure ??= error; }
    let cleanupSession: PrismSessionManager | null = null;
    try {
      if (cleanupEligible.size > 0) {
        cleanupSession = await PrismSessionManager.create({ origin: options.origin, mode: "authenticated", allowAnonymous: false, cookieFile });
        const cleanupIdentity = await cleanupSession.resolve(AbortSignal.timeout(60_000));
        const cleanupProjects = new PrismProjectsClient(cleanupIdentity.account);
        for (const id of cleanupEligible) {
          try {
            const item = await cleanupProjects.get(id, AbortSignal.timeout(30_000));
            if (!item.deleted) {
              if (item.title !== titles.get(id)) throw new ProofFailure("cleanup_project_title_changed");
              await cleanupProjects.deleteManaged(id, AbortSignal.timeout(30_000));
              const after = await cleanupProjects.get(id, AbortSignal.timeout(30_000));
              if (!after.deleted) throw new ProofFailure("project_deletion_unconfirmed");
            }
            deleted.add(id);
          } catch (error) {
            const status = error && typeof error === "object" && "status" in error ? error.status : null;
            if (status === 404 || status === 410) deleted.add(id);
            else failure ??= error;
          }
        }
      }
    } catch (error) { failure ??= error; }
    try { await cleanupSession?.close(); } catch (error) { failure ??= error; }
    for (const name of ["candidate-cookies.json", "candidate-cookies.json.identity.json", "candidate-cookies.json.lock", ...(deleted.size === cleanupEligible.size ? ["fleet.json.bootstrap"] : [])]) {
      const target = path.join(directory, name);
      contained(directory, target);
      await rm(target, { force: true, recursive: true }).catch(error => { failure ??= error; });
    }
  }
  if (deleted.size !== cleanupEligible.size) failure ??= new ProofFailure("test_project_cleanup_incomplete");
  const requestProof = requests.snapshot();
  if (requestProof.invoked !== MAX_REQUESTS || requestProof.accepted !== MAX_REQUESTS || requestProof.pending !== 0) failure ??= new ProofFailure("request_proof_incomplete");
  const report = { success: failure === null, phase, candidatePort: PORT, target: TARGET, release, waves, providerResponses: requestProof.providerTerminals,
    requestProof, shutdownProof: supervisors.snapshot(), journalFilesAfterClose, stabilitySamples, recoveryWaits: logger.recoveryWaits,
    mixedInputBytes: MIXED_INPUT_BYTES, pongProof: [...pongProof.values()].map(({ dropped, retained }) => ({ dropped, retained })),
    supervisors: [...tracked.values()], projectsCreatedOrAttempted: cleanupEligible.size, projectsDeleted: deleted.size,
    error: failure instanceof ProofFailure ? failure.code : failure ? "external_operation_failed" : null };
  await writeFile(path.join(directory, "report.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ event: "outage.proof.completed", reportFile: path.join(directory, "report.json"), ...report }));
  if (failure) throw new ProofFailure("live_transport_outage_proof_failed");
}

if (import.meta.main) main().catch(error => {
  console.error(JSON.stringify({ event: "outage.proof.failed", code: error instanceof ProofFailure ? error.code : "external_operation_failed" }));
  process.exitCode = 1;
});
