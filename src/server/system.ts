import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { PrismAccountSession } from "../prism/account-session";
import { PrismProvisioningClient, type ProvisionedRuntimeCredentials } from "../prism/provisioning-client";
import { resolveMultiSandboxConfig } from "../config/multi-sandbox";
import { CpuTransformPool } from "../router/cpu-transform-pool";
import { ResourceGovernor, SystemMachineProbe } from "../router/resource-governor";
import { MultiSandboxRouter } from "../router/router";
import { SandboxRegistry, type RoutableSandboxRuntime, type RoutableSandboxSnapshot } from "../sandbox/registry";
import { SandboxGateway } from "../sandbox/gateway";
import { SandboxHeartbeat } from "../sandbox/heartbeat";
import { SandboxYDoc } from "../sandbox/ydoc";
import { DaemonController, type DaemonDescriptor } from "../sandbox/daemon-controller";
import { SandboxRuntime } from "../sandbox/runtime";
import { SandboxSlotRunner } from "../relay/slot-runner";
import type { RunHandle } from "../relay/run-handle";
import type { ProjectId } from "../sandbox/types";
import { createGhostProvApp } from "./app";
import { createGhostProvServer } from "./runtime";

const LAUNCHER_SOURCE = [
  "$pdf_mode = 1;",
  "system('rm -f main.pdf main.aux main.fls main.fdb_latexmk; rm -f /workspace/response.json');",
  "$pdflatex = 'nohup python gp_daemon.py >> /tmp/gp_daemon.log 2>&1 & pdflatex -interaction=nonstopmode %O %S'",
].join("\n") + "\n";

const MAIN_SOURCE = [
  "\\documentclass{article}",
  "\\begin{document}",
  "GhostProv daemon bootstrap.",
  "\\end{document}",
].join("\n") + "\n";

function daemonSource(): string {
  return readFileSync(fileURLToPath(new URL("../../sandbox/gp_daemon.py", import.meta.url)), "utf8");
}

function daemonBuild(source: string): string {
  const match = /DAEMON_BUILD\s*=\s*["']([^"']+)["']/.exec(source);
  if (!match) throw new Error("Bundled gp_daemon.py does not declare DAEMON_BUILD");
  return match[1];
}

function trackedDaemonFiles(capacity: number): readonly string[] {
  const files = ["gp_status.json"];
  for (let slot = 1; slot <= capacity; slot++) {
    files.push(`job_s${slot}.json`, `resp_s${slot}.json`);
  }
  return Object.freeze(files);
}

class ManagedSandboxRuntime implements RoutableSandboxRuntime {
  readonly projectId: ProjectId;
  readonly #cpuPool: CpuTransformPool;
  readonly #lifecycle: SandboxRuntime;
  #gateway: SandboxGateway | null = null;
  #runner: SandboxSlotRunner | null = null;
  #credentials: ProvisionedRuntimeCredentials | null = null;
  #draining = false;

  constructor(options: {
    projectId: ProjectId;
    account: PrismAccountSession;
    provisioning: PrismProvisioningClient;
    cpuPool: CpuTransformPool;
    daemonSource: string;
    expectedBuild: string;
    requestedCapacity: number;
  }) {
    this.projectId = options.projectId;
    this.#cpuPool = options.cpuPool;
    this.#lifecycle = new SandboxRuntime({
      projectId: options.projectId,
      provision: async (projectId, signal, hooks) => {
        const credentials = await options.provisioning.provision(projectId, signal, hooks);
        this.#credentials = credentials;
        return credentials;
      },
      createHeartbeat: (sandbox, callbacks) => {
        const gateway = new SandboxGateway({ projectId: options.projectId, account: options.account, sandbox });
        this.#gateway = gateway;
        return new SandboxHeartbeat({
          projectId: options.projectId,
          gateway,
          initialDelayMs: 10_000,
          onExpired: callbacks.onExpired,
          onTransientFailure: callbacks.onTransientFailure,
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
          assets: {
            daemonFile: "gp_daemon.py",
            daemonSource: options.daemonSource,
            launcherFile: ".latexmkrc",
            launcherSource: LAUNCHER_SOURCE,
            mainFile: "main.tex",
            mainSource: MAIN_SOURCE,
            trackedFiles: trackedDaemonFiles(options.requestedCapacity),
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
              gzip: bytes => this.#cpuPool.gzip(bytes),
            });
            return descriptor;
          },
        };
      },
    });
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
  }
  run(body: Record<string, unknown>, signal?: AbortSignal): Promise<RunHandle> {
    if (this.#draining || !this.#runner || this.#lifecycle.snapshot().state.type !== "ready") {
      return Promise.reject(new Error(`Sandbox ${this.projectId} is not ready`));
    }
    return this.#runner.run(body, signal);
  }
  snapshot(): RoutableSandboxSnapshot {
    const lifecycle = this.#lifecycle.snapshot();
    const lanes = this.#runner?.snapshot();
    return {
      projectId: this.projectId,
      state: lifecycle.state.type,
      capacity: lanes?.capacity ?? lifecycle.capacity,
      active: lanes?.active ?? 0,
      available: lanes?.available ?? 0,
      quarantined: lanes?.quarantined ?? 0,
      heartbeatAgeMs: lifecycle.heartbeatAgeMs,
      credentialExpiresInMs: this.#credentials ? Math.max(0, this.#credentials.resource.expiresAtMs - Date.now()) : null,
      ackEwmaMs: 1_500,
      completionEwmaMs: 10_000,
      recentFailureRate: 0,
      queueDepth: lanes?.queued ?? 0,
      memoryPressure: 0,
      failure: lifecycle.failure,
    };
  }
}

export interface BrowserlessSystem {
  config: ReturnType<typeof resolveMultiSandboxConfig>;
  account: PrismAccountSession;
  registry: SandboxRegistry;
  governor: ResourceGovernor;
  router: MultiSandboxRouter;
  app: ReturnType<typeof createGhostProvApp>;
  server: ReturnType<typeof createGhostProvServer>;
  start(): Promise<void>;
  close(): Promise<void>;
}

export async function createBrowserlessSystem(env: Record<string, string | undefined> = process.env): Promise<BrowserlessSystem> {
  const probe = new SystemMachineProbe();
  const config = resolveMultiSandboxConfig(env, probe.sample());
  const account = await PrismAccountSession.create({
    origin: config.prismOrigin,
    cookieFile: config.cookieFile,
    initialCookieHeader: config.initialCookieHeader,
  });
  const provisioning = new PrismProvisioningClient({ account });
  const governor = new ResourceGovernor({ probe });
  const cpuPool = new CpuTransformPool({ workers: governor.snapshot().cpuTransformWorkers });
  const source = daemonSource();
  const runtimes = config.projectIds.map(projectId => new ManagedSandboxRuntime({
    projectId,
    account,
    provisioning,
    cpuPool,
    daemonSource: source,
    expectedBuild: daemonBuild(source),
    requestedCapacity: config.slotsPerProject,
  }));
  const registry = new SandboxRegistry(runtimes);
  const router = new MultiSandboxRouter({ registry, governor });
  const app = createGhostProvApp({ router });
  const server = createGhostProvServer({ app, router, port: config.port, candidate: config.candidate });
  return {
    config, account, registry, governor, router, app, server,
    async start() {
      await registry.startAll();
      await new Promise<void>((resolve, reject) => {
        server.once("error", reject);
        server.listen(config.port, "127.0.0.1", () => {
          server.off("error", reject);
          resolve();
        });
      });
    },
    async close() {
      await registry.stopAll();
      await cpuPool.close();
      if (server.listening) await new Promise<void>(resolve => server.close(() => resolve()));
    },
  };
}
