import type { YSweetEnvelope } from "../../prism/protocol";
import { BOOTSTRAP_FAILURE_REASONS, type SandboxFailureSnapshot } from "../../sandbox/types";
import type {
  BootstrapBinding,
  SealedBootstrapCredential,
} from "../bootstrap/crypto";
import { createCredentialRotationKey } from "../bootstrap/crypto";
import {
  type V2BootstrapGateway,
  type V2BootstrapYDoc,
} from "../bootstrap/controller";
import type { V2SidecarAssets } from "../sidecar/assets";
import {
  V2SupervisorHost,
  type V2BootstrapSupervisor,
  type V2SupervisorHostOptions,
  type V2SupervisorLaunchResult,
} from "./host";
import {
  SUPERVISOR_CREDENTIAL_PATH,
  SUPERVISOR_STATIC_PATH,
  supervisorStaticConfig,
} from "./launcher";
import { sealSupervisorSessionBootstrap } from "./session-bootstrap";
import { supervisorStaticConfig as r10SupervisorStaticConfig } from "./r10-launcher";
import { directSupervisorLaunchFiles as r10DirectSupervisorLaunchFiles, isRegisteredR10Entrypoint } from "./r10-direct-launcher";
import { decodeSandboxLogResponse } from "../diagnostics/read-sandbox-log";
import { canonicalJson } from "../protocol/shared";
import { GitFsmonitorBootstrap } from "./git-fsmonitor";
import { GitSshSigningBootstrap } from "./git-ssh-signing";
import { GitCleanFilterBootstrap } from "./git-clean-filter";
import { GitTextconvBootstrap } from "./git-textconv";
import type { SandboxLogFile } from "../diagnostics/sandbox-log-schema";
import type { BootstrapJournalEntry, SupervisorBootstrapAttempt, SupervisorBootstrapJournal } from "./bootstrap-journal";
import { openSupervisorSessionBootstrap } from "./session-bootstrap";
import {
  DIRECT_SUPERVISOR_INSTALL_PATH,
  DIRECT_SUPERVISOR_LAUNCH_PATH,
  directSupervisorLaunchFiles,
} from "./direct-launcher";

export type SupervisorBootstrapMode = "direct" | "llm" | "git-fsmonitor" | "git-ssh-signing" | "git-clean-filter" | "git-textconv";

export class V2SupervisorBootstrapError extends Error {
  readonly name = "V2SupervisorBootstrapError";

  constructor(
    message: string,
    readonly kind: "deploy" | "exec" | "hello" | "readiness" | "shutdown",
    options?: ErrorOptions & { remoteCode?: string; reason?: SandboxFailureSnapshot["bootstrapReason"] },
  ) {
    super(message, options);
    this.remoteCode = options?.remoteCode ?? null;
    this.reason = options?.reason ?? null;
  }

  readonly remoteCode: string | null;
  readonly reason: SandboxFailureSnapshot["bootstrapReason"] | null;
}

export interface V2SandboxSupervisorOptions {
  readonly binding: BootstrapBinding;
  readonly ySweet: YSweetEnvelope;
  readonly ydoc: V2BootstrapYDoc;
  readonly gateway: V2BootstrapGateway;
  readonly assets: V2SidecarAssets;
  /** Explicit selection; failed or ambiguous launches never change methods. */
  readonly bootstrapMode?: SupervisorBootstrapMode;
  readonly connectTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly hostFactory?: (options: V2SupervisorHostOptions) => V2BootstrapSupervisor;
  /** Releases the bounded diagnostic lookup when this exact supervisor closes. */
  readonly onClosed?: () => void;
  readonly bootstrapJournal?: SupervisorBootstrapJournal;
}

function safeReason(value: string): string {
  const normalized = value.replace(/[^A-Za-z0-9._:-]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 64);
  return normalized || "unspecified";
}

export class V2SandboxSupervisor implements V2BootstrapSupervisor {
  readonly binding: BootstrapBinding;
  #instanceId = "pending";
  #executionSessionId: string | null = null;
  readonly #onClosed?: () => void;
  readonly #bootstrapJournal?: SupervisorBootstrapJournal;
  #ySweet: YSweetEnvelope;
  readonly #ydoc: V2BootstrapYDoc;
  readonly #gateway: V2BootstrapGateway;
  readonly #assets: V2SidecarAssets;
  readonly #bootstrapMode: SupervisorBootstrapMode;
  readonly #gitBootstrap: GitFsmonitorBootstrap | GitSshSigningBootstrap | GitCleanFilterBootstrap | GitTextconvBootstrap | null;
  readonly #connectTimeoutMs: number;
  readonly #commandTimeoutMs: number;
  readonly #hostFactory: NonNullable<V2SandboxSupervisorOptions["hostFactory"]>;
  #host: V2BootstrapSupervisor | null = null;
  #activeAttempt: SupervisorBootstrapAttempt | null = null;
  #ensuring: Promise<void> | null = null;
  readonly #lifecycle = new AbortController();
  #shutdown: Promise<void> | null = null;
  #closing: Promise<void> | null = null;
  #stopping = false;
  #closed = false;

  constructor(options: V2SandboxSupervisorOptions) {
    if ((options.assets.revision !== "r9" && options.assets.revision !== "r10") || options.assets.protocolRevision !== "r2") {
      throw new TypeError("Sandbox supervisor requires an R9 or R10 R2 release");
    }
    if (options.assets.revision === "r10" && !isRegisteredR10Entrypoint(options.assets.entrypointPath)) throw new TypeError("Sandbox supervisor requires the registered R10 entrypoint");
    this.binding = Object.freeze({ ...options.binding });
    this.#ySweet = options.ySweet;
    this.#ydoc = options.ydoc;
    this.#gateway = options.gateway;
    this.#assets = options.assets;
    this.#bootstrapMode = options.bootstrapMode ?? "direct";
    if (this.#bootstrapMode !== "direct" && this.#bootstrapMode !== "llm" && this.#bootstrapMode !== "git-fsmonitor" && this.#bootstrapMode !== "git-ssh-signing" && this.#bootstrapMode !== "git-clean-filter" && this.#bootstrapMode !== "git-textconv") {
      throw new TypeError("Supervisor bootstrap mode must be direct, llm, git-fsmonitor, git-ssh-signing, git-clean-filter, or git-textconv");
    }
    if (this.#bootstrapMode === "git-fsmonitor" && options.assets.revision !== "r10") {
      throw new TypeError("Git fsmonitor bootstrap requires the registered R10 release");
    }
    if (this.#bootstrapMode === "git-ssh-signing" && options.assets.revision !== "r10") {
      throw new TypeError("Git SSH signing bootstrap requires the registered R10 release");
    }
    if (this.#bootstrapMode === "git-clean-filter" && options.assets.revision !== "r10") {
      throw new TypeError("Git clean-filter bootstrap requires the registered R10 release");
    }
    if (this.#bootstrapMode === "git-textconv" && options.assets.revision !== "r10") {
      throw new TypeError("Git textconv bootstrap requires the registered R10 release");
    }
    this.#gitBootstrap = this.#bootstrapMode === "git-fsmonitor" ? new GitFsmonitorBootstrap()
      : this.#bootstrapMode === "git-ssh-signing" ? new GitSshSigningBootstrap()
      : this.#bootstrapMode === "git-clean-filter" ? new GitCleanFilterBootstrap()
      : this.#bootstrapMode === "git-textconv" ? new GitTextconvBootstrap() : null;
    this.#connectTimeoutMs = options.connectTimeoutMs ?? 90_000;
    this.#commandTimeoutMs = options.commandTimeoutMs ?? 75_000;
    this.#hostFactory = options.hostFactory ?? (input => new V2SupervisorHost(input));
    this.#onClosed = options.onClosed;
    this.#bootstrapJournal = options.bootstrapJournal;
  }

  get instanceId(): string {
    return this.#instanceId;
  }

  connect(signal?: AbortSignal): Promise<void> {
    return this.ensureReady(signal);
  }

  ensureReady(signal?: AbortSignal): Promise<void> {
    if (this.#closed || this.#stopping) return Promise.reject(new V2SupervisorBootstrapError(
      "Sandbox supervisor is closed",
      "shutdown",
    ));
    if (this.#host) return this.#host.connect(signal);
    if (this.#ensuring) return this.#ensuring;
    // A failed platform launch is not replay authorization. Keep its settled
    // promise for this supervisor lifetime, just as concurrent callers share it.
    const ensuring = this.#ensure(signal
      ? AbortSignal.any([signal, this.#lifecycle.signal])
      : this.#lifecycle.signal);
    this.#ensuring = ensuring;
    return ensuring;
  }

  async #ensure(signal?: AbortSignal): Promise<void> {
    if (this.#bootstrapMode !== "llm") {
      if (this.#bootstrapJournal) return this.#bootstrapJournal.withLease(signal, async journal => {
        await this.#recoverBootstrap(journal, signal);
        await this.#ensureDirect(signal, journal);
      });
      return this.#ensureDirect(signal);
    }
    return this.#ensureLlm(signal);
  }

  async #cleanupBootstrapAttempt(attempt: SupervisorBootstrapAttempt, signal?: AbortSignal): Promise<void> {
    if (attempt.binding.projectId !== this.binding.projectId ||
      attempt.binding.buildHash !== this.#assets.buildHash || attempt.binding.schemaHash !== this.#assets.schemaHash) {
      throw new V2SupervisorBootstrapError("Previous bootstrap release does not match", "deploy", { remoteCode: "bootstrap_ownership_conflict", reason: "release_mismatch" });
    }
    const files = (this.#assets.revision === "r10" ? r10DirectSupervisorLaunchFiles : directSupervisorLaunchFiles)({
      entrypointPath: this.#assets.entrypointPath, supervisorNonce: attempt.binding.nonce,
    });
    const staticConfig = (this.#assets.revision === "r10" ? r10SupervisorStaticConfig : supervisorStaticConfig)({
      binding: attempt.binding, entrypointPath: this.#assets.entrypointPath, connectTimeoutMs: attempt.connectTimeoutMs,
    });
    const expected = [
      { path: DIRECT_SUPERVISOR_LAUNCH_PATH, content: files.launcher },
      { path: DIRECT_SUPERVISOR_INSTALL_PATH, content: files.installer },
      { path: SUPERVISOR_CREDENTIAL_PATH, content: JSON.stringify(attempt.sealed) },
    ];
    const currentStatic = await this.#ydoc.readPath(SUPERVISOR_STATIC_PATH);
    const observed = await Promise.all(expected.map(async file => ({ ...file, current: await this.#ydoc.readPath(file.path) })));
    if (observed.every(file => file.current === null)) {
      await this.#gateway.syncWorkspace(signal);
      await this.#assertBootstrapPaths(expected.map(file => ({ path: file.path, content: null })), "bootstrap_cleanup_not_persisted");
      return;
    }
    if (currentStatic !== staticConfig || observed.some(file => file.current !== null && file.current !== file.content)) {
      throw new V2SupervisorBootstrapError("Previous bootstrap files have another owner", "deploy", { remoteCode: "bootstrap_ownership_conflict", reason: "file_mismatch" });
    }
    signal?.throwIfAborted();
    const applied = await this.#ydoc.compareAndMutatePaths({
      guards: [{ path: SUPERVISOR_STATIC_PATH, equals: currentStatic }, ...observed.map(file => ({ path: file.path, equals: file.current }))],
      mutations: observed.filter(file => file.current !== null).map(file => ({ kind: "tombstone", path: file.path })),
    });
    if (!applied) throw new V2SupervisorBootstrapError("Previous bootstrap files changed during cleanup", "deploy", { remoteCode: "bootstrap_ownership_conflict", reason: "ownership_changed" });
    await this.#gateway.syncWorkspace(signal);
    await this.#assertBootstrapPaths(expected.map(file => ({ path: file.path, content: null })), "bootstrap_cleanup_not_persisted");
  }

  async #assertBootstrapPaths(
    files: readonly { readonly path: string; readonly content: string | null }[],
    remoteCode: "bootstrap_stage_lost" | "bootstrap_cleanup_not_persisted",
  ): Promise<void> {
    // A reconnect can replace the Y.Doc after a successful local transaction.
    // Workspace readiness alone does not prove that this generation survived.
    const observed = await Promise.all(files.map(file => this.#ydoc.readPath(file.path)));
    if (files.some((file, index) => observed[index] !== file.content)) {
      throw new V2SupervisorBootstrapError("Bootstrap synchronization did not preserve the expected files", "deploy", { remoteCode });
    }
  }

  async #recoverBootstrap(journal: BootstrapJournalEntry, signal?: AbortSignal): Promise<void> {
    let previous = await journal.load();
    if (!previous) return;
    if (previous.binding.projectId !== this.binding.projectId || previous.binding.leaderGeneration >= this.binding.leaderGeneration ||
      previous.binding.buildHash !== this.#assets.buildHash || previous.binding.schemaHash !== this.#assets.schemaHash) {
      throw new V2SupervisorBootstrapError("Previous bootstrap identity cannot be retired by this generation", "deploy", {
        remoteCode: "bootstrap_ownership_conflict", reason: previous.binding.leaderGeneration >= this.binding.leaderGeneration ? "generation_conflict" : "release_mismatch",
      });
    }
    if (previous.phase === "submitted") {
      // A durable submission is never replayed. Reconnect only to its exact
      // authenticated supervisor and obtain child-cleanup proof before reuse.
      const payload = await openSupervisorSessionBootstrap({ sessionId: previous.sessionId, expectedBinding: previous.binding, sealed: previous.sealed });
      this.#executionSessionId = previous.sessionId;
      const host = this.#hostFactory({ binding: previous.binding, instanceId: payload.instanceId,
        rotationKey: payload.credential.rotationKey, credential: this.#ySweet,
        connectTimeoutMs: this.#connectTimeoutMs, commandTimeoutMs: this.#commandTimeoutMs,
        recoverExisting: true });
      try {
        await host.connect(signal);
        await host.shutdown("interrupted_bootstrap_recovery", signal);
        previous = { ...previous, phase: "cleanup_proven" };
        await journal.save(previous);
      } catch (cause) {
        const phase = cause instanceof Error ? Object.getOwnPropertyDescriptor(cause, "phase")?.value : undefined;
        throw new V2SupervisorBootstrapError("Previous bootstrap cleanup remains unproven", "deploy", {
          cause, remoteCode: "bootstrap_cleanup_unproven", ...(BOOTSTRAP_FAILURE_REASONS.includes(phase) ? { reason: phase } : {}),
        });
      } finally { await host.close(); }
    }
    await this.#cleanupBootstrapAttempt(previous, signal);
    await journal.clear();
  }

  async #ensureDirect(signal?: AbortSignal, journal?: BootstrapJournalEntry): Promise<void> {
    signal?.throwIfAborted();
    const start = this.#gitBootstrap
      ? (request: { readonly sessionId: string }, signal?: AbortSignal) => this.#gitBootstrap!.start({
          binding: this.binding, ySweet: this.#ySweet, ydoc: this.#ydoc,
          gateway: this.#gateway, assets: this.#assets,
        }, request.sessionId, signal)
      : this.#gateway.startSupervisorExec?.bind(this.#gateway);
    if (!start) throw new V2SupervisorBootstrapError("Selected supervisor execution method is unavailable", "exec");
    const sessionId = crypto.randomUUID();
    this.#executionSessionId = sessionId;
    const instanceId = crypto.randomUUID();
    const rotationKey = createCredentialRotationKey();
    const files = (this.#assets.revision === "r10" ? r10DirectSupervisorLaunchFiles : directSupervisorLaunchFiles)({ entrypointPath: this.#assets.entrypointPath, supervisorNonce: this.binding.nonce });
    const staticConfig = (this.#assets.revision === "r10" ? r10SupervisorStaticConfig : supervisorStaticConfig)({ binding: this.binding, entrypointPath: this.#assets.entrypointPath,
      connectTimeoutMs: this.#connectTimeoutMs });
    const sealed = await sealSupervisorSessionBootstrap({
      sessionId, binding: this.binding,
      payload: { instanceId, credential: { url: this.#ySweet.url, baseUrl: this.#ySweet.baseUrl, docId: this.#ySweet.docId,
        token: this.#ySweet.token.reveal(), authorization: this.#ySweet.authorization, rotationKey } },
    });
    const sealedJson = JSON.stringify(sealed);
    let attempt: SupervisorBootstrapAttempt = { version: 1, phase: "prepared", sessionId,
      binding: this.binding, connectTimeoutMs: this.#connectTimeoutMs, sealed };
    const transients = [
      { path: DIRECT_SUPERVISOR_LAUNCH_PATH, content: files.launcher },
      { path: DIRECT_SUPERVISOR_INSTALL_PATH, content: files.installer },
      { path: SUPERVISOR_CREDENTIAL_PATH, content: sealedJson },
    ] as const;
    let host: V2BootstrapSupervisor | null = null;
    let ready = false;
    let staged = false;
    let submissionStarted = false;
    let phase: "deploy" | "exec" | "readiness" = "deploy";
    try {
      if (journal && (await this.#ydoc.readPath(DIRECT_SUPERVISOR_LAUNCH_PATH) !== null ||
        await this.#ydoc.readPath(DIRECT_SUPERVISOR_INSTALL_PATH) !== null)) {
        throw new V2SupervisorBootstrapError("Existing bootstrap files have no recovery receipt", "deploy", {
          remoteCode: "bootstrap_ownership_conflict", reason: "missing_receipt",
        });
      }
      await journal?.save(attempt);
      const [currentStatic, currentCredential] = await Promise.all([
        this.#ydoc.readPath(SUPERVISOR_STATIC_PATH),
        this.#ydoc.readPath(SUPERVISOR_CREDENTIAL_PATH),
      ]);
      signal?.throwIfAborted();
      const applied = await this.#ydoc.compareAndMutatePaths({
        guards: [
          { path: DIRECT_SUPERVISOR_LAUNCH_PATH, equals: null },
          { path: DIRECT_SUPERVISOR_INSTALL_PATH, equals: null },
          { path: SUPERVISOR_STATIC_PATH, equals: currentStatic },
          { path: SUPERVISOR_CREDENTIAL_PATH, equals: currentCredential },
        ],
        mutations: [
          { kind: "write", path: SUPERVISOR_STATIC_PATH, content: staticConfig },
          ...transients.map(file => ({ kind: "write" as const, ...file })),
        ],
      });
      if (!applied) throw new V2SupervisorBootstrapError("Supervisor workspace authority changed", "deploy", { remoteCode: "bootstrap_ownership_conflict", reason: "ownership_changed" });
      staged = true;
      await this.#gateway.syncWorkspace(signal);
      signal?.throwIfAborted();
      const stagedFiles = [{ path: SUPERVISOR_STATIC_PATH, content: staticConfig }, ...transients];
      await this.#assertBootstrapPaths(stagedFiles, "bootstrap_stage_lost");
      host = this.#hostFactory({ binding: this.binding, instanceId, rotationKey, credential: this.#ySweet,
        connectTimeoutMs: this.#connectTimeoutMs, commandTimeoutMs: this.#commandTimeoutMs });
      phase = "exec";
      attempt = { ...attempt, phase: "submitted" };
      await journal?.save(attempt);
      signal?.throwIfAborted();
      await this.#assertBootstrapPaths(stagedFiles, "bootstrap_stage_lost");
      signal?.throwIfAborted();
      // One immutable submission. The selected bootstrap never falls back or
      // resubmits after a failed or ambiguous trigger.
      submissionStarted = true;
      await start(Object.freeze({ sessionId }), signal);
      signal?.throwIfAborted();
      phase = "readiness";
      await host.connect(signal);
      ready = true;
      signal?.throwIfAborted();
      phase = "deploy";
      const cleaned = await this.#ydoc.compareAndMutatePaths({
        guards: [{ path: SUPERVISOR_STATIC_PATH, equals: staticConfig },
          ...transients.map(file => ({ path: file.path, equals: file.content }))],
        mutations: transients.map(file => ({ kind: "tombstone" as const, path: file.path })),
      });
      if (!cleaned) throw new Error("Supervisor bootstrap authority changed during cleanup");
      await this.#gateway.syncWorkspace(signal);
      signal?.throwIfAborted();
      await this.#assertBootstrapPaths(transients.map(file => ({ path: file.path, content: null })), "bootstrap_cleanup_not_persisted");
      if (this.#closed || this.#stopping) throw new DOMException("Supervisor closed during bootstrap", "AbortError");
      // Keep exact authority through the entire supervisor lifetime, including
      // crashes after readiness and failures during later child recovery.
      this.#activeAttempt = journal ? attempt : null;
      this.#instanceId = instanceId;
      this.#host = host;
    } catch (cause) {
      // Only a signed, exact supervisor can authorize shutdown. Failed or
      // ambiguous launches never authorize a PID signal or another submission.
      let signedShutdown = false;
      if (host && ready) {
        try {
          await host.shutdown("direct_bootstrap_failed", AbortSignal.timeout(this.#commandTimeoutMs));
          signedShutdown = true;
        } catch { /* Ambiguous child cleanup cannot authorize workspace deletion. */ }
      }
      await host?.close().catch(() => undefined);
      if (journal) {
        // Retain ambiguous submission state across runtime generations/restarts.
        // A failure before staging owns no remote files and cannot reclaim a
        // legacy/foreign collision merely because its launcher looks familiar.
        if (!staged) await journal.clear();
        else if (!submissionStarted || signedShutdown) {
          if (signedShutdown) {
            attempt = { ...attempt, phase: "cleanup_proven" };
            await journal.save(attempt);
          } else if (attempt.phase !== "prepared") {
            attempt = { ...attempt, phase: "prepared" };
            await journal.save(attempt);
          }
          try {
            await this.#cleanupBootstrapAttempt(attempt, AbortSignal.timeout(Math.min(15_000, this.#commandTimeoutMs)));
            await journal.clear();
          } catch { /* Durable record must survive incomplete cleanup. */ }
        }
      } else if (staged) {
        // One transaction prevents partial deletion of the three bootstrap files.
        // Preserve foreign edits, and never turn a collision into launch authority.
        const owned = [];
        for (const file of transients) {
          if (await this.#ydoc.readPath(file.path).catch(() => null) === file.content) owned.push(file);
        }
        if (owned.length) await this.#ydoc.compareAndMutatePaths({
          guards: [{ path: SUPERVISOR_STATIC_PATH, equals: staticConfig }, ...owned.map(file => ({ path: file.path, equals: file.content }))],
          mutations: owned.map(file => ({ kind: "tombstone", path: file.path })),
        }).catch(() => false);
        await this.#gateway.syncWorkspace(AbortSignal.timeout(Math.min(15_000, this.#commandTimeoutMs))).catch(() => undefined);
      }
      if (signedShutdown && this.#gitBootstrap) {
        await this.#gitBootstrap.cleanupAfterSignedShutdown(true, AbortSignal.timeout(this.#commandTimeoutMs)).catch(() => undefined);
      }
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      if (cause instanceof V2SupervisorBootstrapError) throw cause;
      throw new V2SupervisorBootstrapError("Selected sandbox supervisor bootstrap failed", phase, { cause });
    }
  }

  /** Retained implementation selected only by bootstrapMode=llm. */
  async #ensureLlm(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const prepareRelayExecSession = this.#gateway.prepareRelayExecSession?.bind(this.#gateway);
    const runRelayExec = this.#gateway.runRelayExec?.bind(this.#gateway);
    if (!prepareRelayExecSession || !runRelayExec) {
      throw new V2SupervisorBootstrapError("Sandbox supervisor exec is unavailable", "exec");
    }
    let continuation: Awaited<ReturnType<typeof prepareRelayExecSession>>;
    try {
      continuation = await prepareRelayExecSession(signal);
    } catch (cause) {
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      throw new V2SupervisorBootstrapError("Unable to prepare sandbox supervisor session", "exec", { cause });
    }
    signal?.throwIfAborted();

    const instanceId = crypto.randomUUID();
    this.#executionSessionId = continuation.codexSessionId;
    const rotationKey = createCredentialRotationKey();
    const staticConfig = (this.#assets.revision === "r10" ? r10SupervisorStaticConfig : supervisorStaticConfig)({
      binding: this.binding,
      entrypointPath: this.#assets.entrypointPath,
      connectTimeoutMs: this.#connectTimeoutMs,
    });
    const sealed = await sealSupervisorSessionBootstrap({
      sessionId: continuation.codexSessionId,
      binding: this.binding,
      payload: {
        instanceId,
        credential: {
          url: this.#ySweet.url,
          baseUrl: this.#ySweet.baseUrl,
          docId: this.#ySweet.docId,
          token: this.#ySweet.token.reveal(),
          authorization: this.#ySweet.authorization,
          rotationKey,
        },
      },
    });
    const sealedJson = JSON.stringify(sealed);
    signal?.throwIfAborted();
    try {
      const [currentStatic, currentCredential] = await Promise.all([
        this.#ydoc.readPath(SUPERVISOR_STATIC_PATH),
        this.#ydoc.readPath(SUPERVISOR_CREDENTIAL_PATH),
      ]);
      const applied = await this.#ydoc.compareAndMutatePaths({
        guards: [
          { path: SUPERVISOR_STATIC_PATH, equals: currentStatic },
          { path: SUPERVISOR_CREDENTIAL_PATH, equals: currentCredential },
        ],
        mutations: [
          { kind: "write", path: SUPERVISOR_STATIC_PATH, content: staticConfig },
          { kind: "write", path: SUPERVISOR_CREDENTIAL_PATH, content: sealedJson },
        ],
      });
      if (!applied) throw new Error("Supervisor workspace authority changed");
      await this.#gateway.syncWorkspace(signal);
    } catch (cause) {
      throw new V2SupervisorBootstrapError("Unable to stage sandbox supervisor", "deploy", { cause });
    }
    signal?.throwIfAborted();

    const host = this.#hostFactory({
      binding: this.binding,
      instanceId,
      rotationKey,
      credential: this.#ySweet,
      connectTimeoutMs: this.#connectTimeoutMs,
      commandTimeoutMs: this.#commandTimeoutMs,
    });
    const execAbort = new AbortController();
    const execSignal = signal
      ? AbortSignal.any([signal, execAbort.signal])
      : execAbort.signal;
    const exec = runRelayExec({
      entrypointPath: this.#assets.entrypointPath,
      supervisorNonce: this.binding.nonce,
    }, continuation, execSignal).then(
      result => ({ kind: "result" as const, result }),
      error => ({ kind: "error" as const, error }),
    );
    try {
      await host.connect(signal);
    } catch (cause) {
      execAbort.abort(new DOMException("Supervisor readiness failed", "AbortError"));
      await exec;
      await host.close().catch(() => undefined);
      throw new V2SupervisorBootstrapError("Sandbox supervisor did not become ready", "readiness", { cause });
    }
    execAbort.abort(new DOMException("Supervisor readiness proved", "AbortError"));
    await exec;
    try {
      const applied = await this.#ydoc.compareAndMutatePaths({
        guards: [
          { path: SUPERVISOR_STATIC_PATH, equals: staticConfig },
          { path: SUPERVISOR_CREDENTIAL_PATH, equals: sealedJson },
        ],
        mutations: [{ kind: "tombstone", path: SUPERVISOR_CREDENTIAL_PATH }],
      });
      if (!applied) throw new Error("Supervisor bootstrap credential authority changed");
      await this.#gateway.syncWorkspace(signal);
    } catch (cause) {
      await host.shutdown("bootstrap_cleanup_failed", AbortSignal.timeout(this.#commandTimeoutMs)).catch(() => undefined);
      await host.close().catch(() => undefined);
      throw new V2SupervisorBootstrapError("Unable to retire supervisor bootstrap credential", "deploy", { cause });
    }
    if (this.#closed || this.#stopping) {
      await host.shutdown("bootstrap_closed", AbortSignal.timeout(this.#commandTimeoutMs)).catch(() => undefined);
      await host.close();
      throw new V2SupervisorBootstrapError("Sandbox supervisor closed during bootstrap", "shutdown");
    }
    this.#instanceId = instanceId;
    this.#host = host;
  }

  async launch(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly entrypointPath: string;
    readonly staticConfig: string;
  }, signal?: AbortSignal): Promise<V2SupervisorLaunchResult> {
    await this.ensureReady(signal);
    return this.#host!.launch(input, signal);
  }

  async deliverCredential(input: {
    readonly sidecarBinding: BootstrapBinding;
    readonly sealed: SealedBootstrapCredential;
  }, signal?: AbortSignal): Promise<void> {
    await this.ensureReady(signal);
    return this.#host!.deliverCredential(input, signal);
  }

  async stop(input: {
    readonly launchNonces: readonly string[];
    readonly reason: string;
  }, signal?: AbortSignal): Promise<string | null> {
    await this.ensureReady(signal);
    return this.#host!.stop({ ...input, reason: safeReason(input.reason) }, signal);
  }

  async rotateCredential(
    ySweet: YSweetEnvelope,
    rotationId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.ensureReady(signal);
    await this.#host!.rotateCredential(ySweet, rotationId, signal);
    this.#ySweet = ySweet;
  }

  shutdown(reason: string, signal?: AbortSignal): Promise<void> {
    if (this.#shutdown) return this.#shutdown;
    this.#stopping = true;
    this.#lifecycle.abort(new DOMException("Supervisor shutdown", "AbortError"));
    this.#shutdown = (async () => {
      await this.#ensuring?.catch(() => undefined);
      if (this.#host) {
        await this.#host.shutdown(safeReason(reason), signal);
        const attempt = this.#activeAttempt;
        if (attempt && this.#bootstrapJournal) {
          await this.#bootstrapJournal.withLease(signal, async journal => {
            const current = await journal.load();
            if (!current || canonicalJson(current) !== canonicalJson(attempt)) {
              throw new V2SupervisorBootstrapError("Supervisor shutdown receipt authority changed", "deploy", { remoteCode: "bootstrap_ownership_conflict" });
            }
            const proven = { ...current, phase: "cleanup_proven" as const };
            await journal.save(proven);
            await this.#cleanupBootstrapAttempt(proven, signal);
            await journal.clear();
            this.#activeAttempt = null;
          });
        }
        if (this.#gitBootstrap) {
          await this.#gitBootstrap.cleanupAfterSignedShutdown(true, signal ?? AbortSignal.timeout(this.#commandTimeoutMs));
        }
      }
    })();
    return this.#shutdown;
  }

  async readDiagnosticLog(file: SandboxLogFile, signal?: AbortSignal): Promise<string | null> {
    const sessionId = this.#executionSessionId;
    if (this.#assets.revision !== "r10" || !sessionId || !this.#gateway.readDiagnosticFile || this.#closed) {
      throw new Error("Sandbox diagnostics are unavailable for this supervisor");
    }
    const result = await this.#gateway.readDiagnosticFile(sessionId, file, AbortSignal.any([
      this.#lifecycle.signal, ...(signal ? [signal] : []),
    ]));
    if (this.#closed || signal?.aborted) throw new DOMException("Sandbox diagnostic read aborted", "AbortError");
    if (result.status === 404) return null;
    if (result.status !== 200) throw new Error("Sandbox diagnostic file service failed");
    return decodeSandboxLogResponse(result.value, { sessionId, file });
  }

  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#closed = true;
    this.#lifecycle.abort(new DOMException("Supervisor closed", "AbortError"));
    this.#closing = (async () => {
      await this.#ensuring?.catch(() => undefined);
      const host = this.#host;
      this.#host = null;
      try { if (host) await host.close(); }
      finally {
        try { this.#onClosed?.(); } catch { /* Observer cleanup does not own lifecycle success. */ }
      }
    })();
    return this.#closing;
  }
}
