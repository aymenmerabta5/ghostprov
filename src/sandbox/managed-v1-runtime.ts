import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { PrismAccountSession } from "../prism/account-session";
import type {
  PrismProvisioningClient,
  ProvisionedRuntimeCredentials,
} from "../prism/provisioning-client";
import type { ProvisionedSandbox } from "../prism/protocol";
import type { CredentialRefreshOutcome } from "../observability/events";
import type { CpuTransformPool } from "../router/cpu-transform-pool";
import type { RunHandle } from "../relay/run-handle";
import { SandboxSlotRunner } from "../relay/slot-runner";
import { DaemonController } from "./daemon-controller";
import { SandboxGateway } from "./gateway";
import { SandboxHeartbeat } from "./heartbeat";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
} from "./registry";
import { SandboxRuntime } from "./runtime";
import type { ProjectId } from "./types";
import { SandboxYDoc } from "./ydoc";

function launcherSource(generation: number): string {
  return [
    "$pdf_mode = 1;",
    "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk; rm -f /workspace/response.json');",
    `$pdflatex = 'GP_GENERATION=${generation} nohup python gp_daemon.py >> /tmp/gp_daemon.log 2>&1 & pdflatex -interaction=nonstopmode %O %S'`,
  ].join("\n") + "\n";
}

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "GhostProv daemon bootstrap.",
  "\\end{document}",
].join("\n") + "\n";

export function bundledV1DaemonSource(): string {
  return readFileSync(
    fileURLToPath(new URL("../../sandbox/gp_daemon.py", import.meta.url)),
    "utf8",
  );
}

export function v1DaemonBuild(source: string): string {
  const match = /DAEMON_BUILD\s*=\s*["']([^"']+)["']/.exec(source);
  if (!match) throw new Error("Bundled gp_daemon.py does not declare DAEMON_BUILD");
  return match[1]!;
}

export function trackedV1DaemonFiles(capacity: number): readonly string[] {
  const files = ["gp_status.json"];
  for (let slot = 1; slot <= capacity; slot++) {
    files.push(`job_s${slot}.json`, `resp_s${slot}.json`);
  }
  return Object.freeze(files);
}

export interface ManagedV1RuntimeOptions {
  projectId: ProjectId;
  account: PrismAccountSession;
  provisioning: PrismProvisioningClient;
  cpuPool: CpuTransformPool;
  daemonSource: string;
  expectedBuild: string;
  requestedCapacity: number;
  leaderGeneration?: number;
  initialSandbox?: ProvisionedSandbox;
  acquireSandboxForAuth?(
    status: number,
    signal: AbortSignal,
  ): Promise<ProvisionedSandbox>;
  onCredentialRefresh?(
    outcome: CredentialRefreshOutcome,
    status: number,
  ): void;
}

export class ManagedV1Runtime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly #cpuPool: CpuTransformPool;
  readonly #lifecycle: SandboxRuntime;
  #gateway: SandboxGateway | null = null;
  #runner: SandboxSlotRunner | null = null;
  #credentials: ProvisionedRuntimeCredentials | null = null;
  #sandboxOverride: ProvisionedSandbox | null = null;
  #replacementSandbox: ProvisionedSandbox | null = null;
  #credentialRefreshPending = false;
  #draining = false;

  constructor(options: ManagedV1RuntimeOptions) {
    const leaderGeneration = options.leaderGeneration ?? 0;
    if (!Number.isSafeInteger(leaderGeneration) || leaderGeneration < 0) {
      throw new RangeError("leaderGeneration must be a non-negative safe integer");
    }
    this.projectId = options.projectId;
    this.#cpuPool = options.cpuPool;
    this.#lifecycle = new SandboxRuntime({
      projectId: options.projectId,
      provision: async (projectId, signal, hooks) => {
        const credentials = options.initialSandbox
          ? await options.provisioning.provisionAcquired(
            projectId,
            options.initialSandbox,
            signal,
            hooks,
          )
          : await options.provisioning.provision(projectId, signal, hooks);
        const effective = this.#sandboxOverride
          ? { ...credentials, sandbox: this.#sandboxOverride }
          : credentials;
        this.#credentials = effective;
        return effective;
      },
      createHeartbeat: (sandbox, callbacks) => {
        const gateway = new SandboxGateway({
          projectId: options.projectId,
          account: options.account,
          sandbox,
        });
        this.#gateway = gateway;
        return new SandboxHeartbeat({
          projectId: options.projectId,
          gateway,
          initialDelayMs: 10_000,
          onAuthenticationFailure: async (status, signal) => {
            if (!this.#credentials) return "retry";
            this.#credentialRefreshPending = true;
            let candidate: ProvisionedSandbox;
            try {
              candidate = options.acquireSandboxForAuth
                ? await options.acquireSandboxForAuth(status, signal)
                : await options.provisioning.acquireSandbox(
                  signal,
                  { maxAttempts: 1 },
                );
            } catch (error) {
              const value = error && typeof error === "object"
                ? error as { kind?: unknown; status?: unknown }
                : {};
              this.#notifyCredentialRefresh(
                options,
                signal.aborted
                  ? "aborted"
                  : value.kind === "auth" || value.status === 401 || value.status === 403
                    ? "auth_rejected"
                    : "transport_failed",
                status,
              );
              return "expired";
            }
            if (gateway.rotateSameSession(candidate)) {
              this.#sandboxOverride = candidate;
              if (this.#credentials) {
                this.#credentials = { ...this.#credentials, sandbox: candidate };
              }
              this.#notifyCredentialRefresh(options, "updated_in_place", status);
              return "retry";
            }
            this.#replacementSandbox = candidate;
            this.#notifyCredentialRefresh(options, "replacement_required", status);
            return "expired";
          },
          onSuccess: () => {
            this.#credentialRefreshPending = false;
            callbacks.onSuccess();
          },
          onExpired: callbacks.onExpired,
          onTransientFailure: callbacks.onTransientFailure,
          onUnreachable: callbacks.onUnreachable,
        });
      },
      createYDoc: credentials => {
        let first = true;
        return new SandboxYDoc({
          projectId: options.projectId,
          mintYSweet: async () => {
            if (first) {
              first = false;
              return credentials.ySweet;
            }
            return options.provisioning.mintYSweet(options.projectId, {
              source: "ydoc-reconnect",
              bootstrapAttempt: 1,
              previouslyConnected: true,
              sandboxUrl: credentials.sandbox.url,
              sandboxId: credentials.sandbox.sandboxId ?? null,
              sandboxSessionId: credentials.sandbox.sessionId ?? null,
            });
          },
        });
      },
      createDaemonController: (_credentials, ydoc) => {
        if (!this.#gateway) throw new Error("Gateway missing before daemon controller");
        const controller = new DaemonController({
          ydoc: ydoc as SandboxYDoc,
          gateway: this.#gateway,
          expectedGeneration: leaderGeneration,
          assets: {
            daemonFile: "gp_daemon.py",
            daemonSource: options.daemonSource,
            launcherFile: ".latexmkrc",
            launcherSource: launcherSource(leaderGeneration),
            mainFile: "main.tex",
            mainSource: MAIN_SOURCE,
            trackedFiles: trackedV1DaemonFiles(options.requestedCapacity),
            expectedBuild: options.expectedBuild,
            requiredProtocols: ["segmented-v6.3-embedded-commit"],
          },
        });
        return {
          ensureReady: async signal => {
            const descriptor = await controller.ensureReady(signal);
            this.#runner = new SandboxSlotRunner({
              projectId: options.projectId,
              capacity: Math.min(options.requestedCapacity, descriptor.capacity),
              ydoc: ydoc as SandboxYDoc,
              gateway: this.#gateway!,
              daemon: controller,
              leaderGeneration,
              gzip: bytes => this.#cpuPool.gzip(bytes),
            });
            return descriptor;
          },
        };
      },
    });
  }

  subscribe(listener: () => void): () => void {
    return this.#lifecycle.subscribe(() => listener());
  }

  start(): Promise<void> {
    this.#draining = false;
    return this.#lifecycle.start();
  }

  async drain(reason: string): Promise<void> {
    this.#draining = true;
    this.#lifecycle.drain(reason);
    while (this.#runner) {
      const snapshot = this.#runner.snapshot();
      if (snapshot.active === 0 && snapshot.queued === 0) break;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    await this.stop();
  }

  async stop(): Promise<void> {
    this.#draining = true;
    await this.#lifecycle.stop();
    this.#runner = null;
    this.#gateway = null;
    this.#credentialRefreshPending = false;
  }

  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    if (
      this.#draining ||
      !this.#runner ||
      this.#lifecycle.snapshot().state.type !== "ready"
    ) {
      return Promise.reject(new Error(`Sandbox ${this.projectId} is not ready`));
    }
    return this.#runner.run(body, signal);
  }

  takeReplacementSandbox(): ProvisionedSandbox | null {
    const sandbox = this.#replacementSandbox;
    this.#replacementSandbox = null;
    return sandbox;
  }

  snapshot(): RoutableSandboxSnapshot {
    const lifecycle = this.#lifecycle.snapshot();
    const lanes = this.#runner?.snapshot();
    return {
      projectId: this.projectId,
      state: lifecycle.state.type,
      capacity: this.#credentialRefreshPending ? 0 : lanes?.capacity ?? lifecycle.capacity,
      active: lanes?.active ?? 0,
      available: this.#credentialRefreshPending ? 0 : lanes?.available ?? 0,
      quarantined: lanes?.quarantined ?? 0,
      heartbeatAgeMs: lifecycle.heartbeatAgeMs,
      credentialExpiresInMs: this.#credentials
        ? Math.max(0, this.#credentials.resource.expiresAtMs - Date.now())
        : null,
      ackEwmaMs: 1_500,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: lanes?.queued ?? 0,
      memoryPressure: 0,
      failure: lifecycle.failure,
    };
  }

  #notifyCredentialRefresh(
    options: ManagedV1RuntimeOptions,
    outcome: CredentialRefreshOutcome,
    status: number,
  ): void {
    try { options.onCredentialRefresh?.(outcome, status); } catch {}
  }
}
