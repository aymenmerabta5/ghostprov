import type { YSweetEnvelope } from "../../prism/protocol";
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
import type { SandboxLogFile } from "../diagnostics/sandbox-log-schema";
import {
  DIRECT_SUPERVISOR_INSTALL_PATH,
  DIRECT_SUPERVISOR_LAUNCH_PATH,
  directSupervisorLaunchFiles,
} from "./direct-launcher";

export type SupervisorBootstrapMode = "direct" | "llm";

export class V2SupervisorBootstrapError extends Error {
  readonly name = "V2SupervisorBootstrapError";

  constructor(
    message: string,
    readonly kind: "deploy" | "exec" | "hello" | "readiness" | "shutdown",
    options?: ErrorOptions & { remoteCode?: string },
  ) {
    super(message, options);
    this.remoteCode = options?.remoteCode ?? null;
  }

  readonly remoteCode: string | null;
}

export interface V2SandboxSupervisorOptions {
  readonly binding: BootstrapBinding;
  readonly ySweet: YSweetEnvelope;
  readonly ydoc: V2BootstrapYDoc;
  readonly gateway: V2BootstrapGateway;
  readonly assets: V2SidecarAssets;
  /** Explicit selection; an ambiguous direct launch never falls back to an LLM. */
  readonly bootstrapMode?: SupervisorBootstrapMode;
  readonly connectTimeoutMs?: number;
  readonly commandTimeoutMs?: number;
  readonly hostFactory?: (options: V2SupervisorHostOptions) => V2BootstrapSupervisor;
  /** Releases the bounded diagnostic lookup when this exact supervisor closes. */
  readonly onClosed?: () => void;
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
  #ySweet: YSweetEnvelope;
  readonly #ydoc: V2BootstrapYDoc;
  readonly #gateway: V2BootstrapGateway;
  readonly #assets: V2SidecarAssets;
  readonly #bootstrapMode: SupervisorBootstrapMode;
  readonly #connectTimeoutMs: number;
  readonly #commandTimeoutMs: number;
  readonly #hostFactory: NonNullable<V2SandboxSupervisorOptions["hostFactory"]>;
  #host: V2BootstrapSupervisor | null = null;
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
    if (this.#bootstrapMode !== "direct" && this.#bootstrapMode !== "llm") {
      throw new TypeError("Supervisor bootstrap mode must be direct or llm");
    }
    this.#connectTimeoutMs = options.connectTimeoutMs ?? 90_000;
    this.#commandTimeoutMs = options.commandTimeoutMs ?? 75_000;
    this.#hostFactory = options.hostFactory ?? (input => new V2SupervisorHost(input));
    this.#onClosed = options.onClosed;
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
    if (this.#bootstrapMode === "direct") return this.#ensureDirect(signal);
    return this.#ensureLlm(signal);
  }

  async #ensureDirect(signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const start = this.#gateway.startSupervisorExec?.bind(this.#gateway);
    if (!start) throw new V2SupervisorBootstrapError("Direct supervisor execution is unavailable", "exec");
    const sessionId = crypto.randomUUID();
    this.#executionSessionId = sessionId;
    const instanceId = crypto.randomUUID();
    const rotationKey = createCredentialRotationKey();
    const files = (this.#assets.revision === "r10" ? r10DirectSupervisorLaunchFiles : directSupervisorLaunchFiles)({ entrypointPath: this.#assets.entrypointPath, supervisorNonce: this.binding.nonce });
    const staticConfig = (this.#assets.revision === "r10" ? r10SupervisorStaticConfig : supervisorStaticConfig)({ binding: this.binding, entrypointPath: this.#assets.entrypointPath,
      connectTimeoutMs: this.#connectTimeoutMs });
    const sealedJson = JSON.stringify(await sealSupervisorSessionBootstrap({
      sessionId, binding: this.binding,
      payload: { instanceId, credential: { url: this.#ySweet.url, baseUrl: this.#ySweet.baseUrl, docId: this.#ySweet.docId,
        token: this.#ySweet.token.reveal(), authorization: this.#ySweet.authorization, rotationKey } },
    }));
    const transients = [
      { path: DIRECT_SUPERVISOR_LAUNCH_PATH, content: files.launcher },
      { path: DIRECT_SUPERVISOR_INSTALL_PATH, content: files.installer },
      { path: SUPERVISOR_CREDENTIAL_PATH, content: sealedJson },
    ] as const;
    let host: V2BootstrapSupervisor | null = null;
    let ready = false;
    let staged = false;
    let phase: "deploy" | "exec" | "readiness" = "deploy";
    try {
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
      if (!applied) throw new Error("Supervisor workspace authority changed");
      staged = true;
      await this.#gateway.syncWorkspace(signal);
      signal?.throwIfAborted();
      host = this.#hostFactory({ binding: this.binding, instanceId, rotationKey, credential: this.#ySweet,
        connectTimeoutMs: this.#connectTimeoutMs, commandTimeoutMs: this.#commandTimeoutMs });
      phase = "exec";
      // One immutable submission. No job-status polling, replay, or LLM fallback.
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
      if (this.#closed || this.#stopping) throw new DOMException("Supervisor closed during bootstrap", "AbortError");
      this.#instanceId = instanceId;
      this.#host = host;
    } catch (cause) {
      // Only a signed, exact supervisor can authorize shutdown. Failed or
      // ambiguous launches never authorize a PID signal or another submission.
      if (host && ready) {
        await host.shutdown("direct_bootstrap_failed", AbortSignal.timeout(this.#commandTimeoutMs)).catch(() => undefined);
      }
      await host?.close().catch(() => undefined);
      if (staged) {
        for (const file of transients) {
          await this.#ydoc.compareAndMutatePaths({
            guards: [{ path: file.path, equals: file.content }],
            mutations: [{ kind: "tombstone", path: file.path }],
          }).catch(() => false);
        }
        await this.#gateway.syncWorkspace(AbortSignal.timeout(Math.min(15_000, this.#commandTimeoutMs))).catch(() => undefined);
      }
      if (cause instanceof DOMException && cause.name === "AbortError") throw cause;
      throw new V2SupervisorBootstrapError("Direct sandbox supervisor bootstrap failed", phase, { cause });
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
      if (this.#host) await this.#host.shutdown(safeReason(reason), signal);
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
    try { this.#onClosed?.(); } catch { /* Diagnostic cleanup does not own lifecycle success. */ }
    this.#lifecycle.abort(new DOMException("Supervisor closed", "AbortError"));
    this.#closing = (async () => {
      await this.#ensuring?.catch(() => undefined);
      const host = this.#host;
      this.#host = null;
      if (host) await host.close();
    })();
    return this.#closing;
  }
}
