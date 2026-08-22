import { PrismAccountSession } from "../prism/account-session";
import { PrismProvisioningClient } from "../prism/provisioning-client";
import { resolveMultiSandboxConfig } from "../config/multi-sandbox";
import { CpuTransformPool } from "../router/cpu-transform-pool";
import { ResourceGovernor, SystemMachineProbe } from "../router/resource-governor";
import { MultiSandboxRouter } from "../router/router";
import { SandboxRegistry, type RoutableSandboxRuntime } from "../sandbox/registry";
import { SandboxGateway } from "../sandbox/gateway";
import { SandboxHeartbeat } from "../sandbox/heartbeat";
import { SandboxYDoc } from "../sandbox/ydoc";
import {
  ManagedV1Runtime,
  bundledV1DaemonSource,
  v1DaemonBuild,
} from "../sandbox/managed-v1-runtime";
import { YSweetAwarenessChannel } from "../v2/awareness/y-sweet-channel";
import { V2BootstrapController } from "../v2/bootstrap/controller";
import { parseAwarenessEnvelope } from "../v2/protocol/validate";
import { V2SandboxRuntime } from "../v2/runtime";
import { loadV2SidecarAssets } from "../v2/sidecar/assets";
import { V2TransportSession } from "../v2/transport/session";
import { TransportSelectingRuntime } from "../v2/transport-selector";
import { createGhostProvApp } from "./app";
import { createGhostProvServer } from "./runtime";

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
  const identityKind = config.sessionMode === "anonymous" ? "anonymous" : "authenticated";
  const needsV1 = config.transport === "v1" || (
    config.transport === "auto" && identityKind === "authenticated"
  );
  const source = needsV1 ? bundledV1DaemonSource() : null;
  const assets = config.transport === "v1" ? null : await loadV2SidecarAssets();
  const runtimes: RoutableSandboxRuntime[] = config.projectIds.map((projectId, offset) => {
    const workerIndex = offset + 1;
    const v1 = needsV1
      ? new ManagedV1Runtime({
        projectId,
        account,
        provisioning,
        cpuPool,
        daemonSource: source!,
        expectedBuild: v1DaemonBuild(source!),
        requestedCapacity: config.slotsPerProject,
      })
      : undefined;
    if (config.transport === "v1") return v1!;
    const v2 = new V2SandboxRuntime({
      projectId,
      workerIndex,
      leaderGeneration: 1,
      provisioning,
      governor,
      assets: assets!,
      transportReservationBytes: config.v2TransportReservationBytes,
      maxResponseBytes: config.v2ResponseMaxBytes,
      ySweetRefreshMs: config.v2YSweetRefreshMs,
      gzip: bytes => cpuPool.gzip(bytes),
      createGateway: sandbox => new SandboxGateway({ projectId, account, sandbox }),
      createHeartbeat: (gateway, callbacks) => new SandboxHeartbeat({
        projectId,
        gateway,
        onExpired: callbacks.onExpired,
        onTransientFailure: callbacks.onTransientFailure,
      }),
      createYDoc: credentials => {
        let first = true;
        return new SandboxYDoc({
          projectId,
          mintYSweet: async () => {
            if (first) {
              first = false;
              return credentials.ySweet;
            }
            return provisioning.mintYSweet(projectId, {
              source: "v2-ydoc-reconnect",
              bootstrapAttempt: 1,
              previouslyConnected: true,
              sandboxUrl: credentials.sandbox.url,
              sandboxId: credentials.sandbox.sandboxId ?? null,
              sandboxSessionId: credentials.sandbox.sessionId ?? null,
            });
          },
        });
      },
      createAwareness: (binding, credentials) => new YSweetAwarenessChannel({
        envelope: credentials.ySweet,
        localPeerId: `host-${workerIndex}`,
        field: "ghostprovV2",
        parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      }),
      createBootstrap: input => new V2BootstrapController({
        projectId,
        workerIndex,
        sandboxSessionId: input.binding.sessionId,
        leaderGeneration: input.binding.leaderGeneration,
        ySweet: input.credentials.ySweet,
        ydoc: input.ydoc as SandboxYDoc,
        gateway: input.gateway as SandboxGateway,
        awareness: input.awareness,
        assets: input.assets,
        physicalMax: config.v2SandboxCapacity,
        acceptedMax: Math.max(config.v2SandboxCapacity, config.v2SandboxCapacity * 2),
        memoryBudgetBytes: config.v2SidecarMemoryBudgetBytes,
      }),
      createTransport: input => new V2TransportSession({
        binding: input.binding,
        channel: input.channel,
        maxResponseBytes: input.maxResponseBytes,
        gzip: input.gzip,
        hostCeiling: governor.capacityHint(
          config.v2TransportReservationBytes,
          config.v2SandboxCapacity,
        ),
      }),
    });
    return config.transport === "auto"
      ? new TransportSelectingRuntime({
        mode: "auto",
        identityKind,
        v2,
        v1,
      })
      : v2;
  });
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
