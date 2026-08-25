import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { MachineProbe } from "../src/core/ports";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { ManagedRuntimeFactory } from "../src/fleet/types";
import type { RunHandle } from "../src/relay/run-handle";
import { ResourceGovernor } from "../src/router/resource-governor";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { SupervisedSandboxRuntime } from "../src/sandbox/supervised-runtime";
import {
  createBrowserlessSystem,
  createV2CpuGzip,
  selectV2ProjectRelease,
  type BrowserlessSystemDependencies,
  type BrowserlessSystemServer,
} from "../src/server/system";
import type { V2SidecarAssets } from "../src/v2/sidecar/assets";
import type { PrismProjectSummary } from "../src/prism/projects-client";

const IDS = [1, 2, 3, 4].map(index => projectId(
  `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
));
const FINGERPRINT = "a".repeat(64);
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

class FakeServer implements BrowserlessSystemServer {
  listening = false;
  constructor(readonly order: string[]) {}
  once(): this { return this; }
  off(): this { return this; }
  listen(_port: number, _host: string, callback: () => void): this {
    this.listening = true;
    queueMicrotask(callback);
    return this;
  }
  close(callback?: (error?: Error) => void): this {
    this.order.push("server-stop-admission");
    this.listening = false;
    queueMicrotask(() => callback?.());
    return this;
  }
}

class FakeSampler {
  starts = 0;
  closes = 0;
  start(): void { this.starts++; }
  async close(): Promise<void> { this.closes++; }
}

class DeferredRuntime implements RoutableSandboxRuntime {
  state = "configured";
  readonly listeners = new Set<() => void>();
  releaseStart!: () => void;
  readonly startGate: Promise<void>;

  constructor(
    readonly projectId: ProjectId,
    delayed: boolean,
    readonly order: string[],
  ) {
    this.startGate = delayed
      ? new Promise(resolve => { this.releaseStart = resolve; })
      : Promise.resolve();
    if (!delayed) this.releaseStart = () => {};
  }
  async start(): Promise<void> {
    await this.startGate;
    this.state = "ready";
    this.emit();
  }
  async drain(): Promise<void> {
    this.order.push("runtime-drain", "awareness-clear", "tmp-clean", "runtime-stop");
    this.state = "stopped";
    this.emit();
  }
  async stop(): Promise<void> { this.state = "stopped"; this.emit(); }
  async run(): Promise<RunHandle> { throw new Error("not used"); }
  snapshot(): RoutableSandboxSnapshot {
    return {
      projectId: this.projectId,
      state: this.state,
      capacity: this.state === "ready" ? 128 : 0,
      active: 0,
      available: this.state === "ready" ? 128 : 0,
      quarantined: 0,
      heartbeatAgeMs: 0,
      credentialExpiresInMs: 60_000,
      ackEwmaMs: 1,
      completionEwmaMs: 1,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  emit(): void { for (const listener of this.listeners) listener(); }
}

class FailingRuntime extends DeferredRuntime {
  override async start(): Promise<void> {
    this.state = "failed";
    this.emit();
    throw new Error("sandbox generation failed");
  }
}

class Factory implements ManagedRuntimeFactory {
  readonly runtimes = new Map<ProjectId, DeferredRuntime>();
  released = false;
  constructor(readonly order: string[]) {}
  create(input: { projectId: ProjectId }): DeferredRuntime {
    const runtime = new DeferredRuntime(
      input.projectId,
      input.projectId !== IDS[0] && !this.released,
      this.order,
    );
    this.runtimes.set(input.projectId, runtime);
    return runtime;
  }
  releaseAll(): void {
    this.released = true;
    for (const runtime of this.runtimes.values()) runtime.releaseStart();
  }
}

async function harness(): Promise<{
  env: Record<string, string>;
  dependencies: BrowserlessSystemDependencies;
  factory: Factory;
  server: FakeServer;
  sampler: FakeSampler;
  sessionCloses: () => number;
  order: string[];
}> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-system-v2-"));
  roots.push(root);
  const cookieFile = path.join(root, "cookies.json");
  const manifest = new FleetManifestStore({
    file: path.join(root, "fleet.json"),
    identityFingerprint: FINGERPRINT,
  });
  const projects: PrismProjectSummary[] = IDS.map((id, index) => ({
    projectId: id,
    title: `Project ${index + 1}`,
    owner: "owner",
    deleted: false,
    createdAt: "2026-08-22T10:00:00.000Z",
  }));
  const order: string[] = [];
  const factory = new Factory(order);
  const server = new FakeServer(order);
  const sampler = new FakeSampler();
  let closes = 0;
  const machineProbe: MachineProbe = {
    sample: () => ({
      totalBytes: 32 * 1024 ** 3,
      freeBytes: 20 * 1024 ** 3,
      rssBytes: 256 * 1024 ** 2,
      heapBytes: 64 * 1024 ** 2,
      externalBytes: 16 * 1024 ** 2,
      logicalProcessors: 8,
    }),
  };
  return {
    env: {
      PRISM_COOKIE_FILE: cookieFile,
      PRISM_PROJECT_IDS: IDS.join(","),
      GP_PRISM_SESSION: "authenticated",
      GP_MAX_PROJECTS: "8",
      GP_PROVISION_CONCURRENCY: "4",
      GP_CANDIDATE: "1",
      GP_LOG_LEVEL: "off",
      PORT: "18787",
    },
    dependencies: {
      machineProbe,
      governor: new ResourceGovernor({ probe: machineProbe }),
      sessionManager: {
        resolve: async () => ({
          kind: "authenticated",
          fingerprint: FINGERPRINT,
          account: {} as any,
          resolvedAtMs: 0,
        }),
        close: async () => { closes++; order.push("account-close"); },
      },
      projects: {
        get: async id => {
          const found = projects.find(value => value.projectId === id && !value.deleted);
          if (!found) throw new Error(`missing project ${id}`);
          return found;
        },
      },
      manifest,
      runtimeFactory: factory,
      server,
      sampler,
      cpuPool: { close: async () => { order.push("cpu-pool-close"); } },
    },
    factory,
    server,
    sampler,
    sessionCloses: () => closes,
    order,
  };
}

describe("V2 browserless system composition", () => {
  it("selects the exact immutable release pair for each configured project", () => {
    const asset = (revision: string, fill: string) => Object.freeze({
      revision,
      buildHash: fill.repeat(64),
      schemaHash: (fill === "a" ? "b" : "c").repeat(64),
      entrypointPath: `.ghostprov-v2/revisions/${revision}/sidecar.bundle.js`,
      manifest: Object.freeze({
        path: `.ghostprov-v2/revisions/${revision}/manifest.json`,
        logicalPath: "manifest.json",
        content: "{}\n",
        sha256: (fill === "a" ? "d" : "e").repeat(64),
      }),
      files: Object.freeze([]),
      obsoletePaths: Object.freeze([]),
    }) as unknown as V2SidecarAssets;
    const firstAssets = asset("r1", "a");
    const secondAssets = asset("r2", "b");
    const selections = new Map([
      [IDS[0], { revision: "r1", assets: firstAssets }],
      [IDS[1], { revision: "r2", assets: secondAssets }],
    ]);
    const selector = {
      async preload() {},
      loadedForProject(project: ProjectId) {
        const selected = selections.get(project);
        if (!selected) throw new Error("unknown project");
        return selected;
      },
    };

    expect(selectV2ProjectRelease(selector, IDS[0])).toEqual({
      revision: "r1",
      assets: firstAssets,
    });
    expect(selectV2ProjectRelease(selector, IDS[1])).toEqual({
      revision: "r2",
      assets: secondAssets,
    });
  });

  it("propagates cancellation and transfers only an explicitly owned full gzip buffer", async () => {
    const calls: Array<{ bytes: Uint8Array; options: any }> = [];
    const gzip = createV2CpuGzip({
      async gzip(bytes, options) {
        calls.push({ bytes, options });
        return bytes;
      },
      async close() {},
    });
    const controller = new AbortController();
    const owned = new Uint8Array(16);
    await gzip(owned, { signal: controller.signal, inputOwnership: "owned" });
    const backing = new Uint8Array(32);
    await gzip(backing.subarray(4, 20), {
      signal: controller.signal,
      inputOwnership: "owned",
    });

    expect(calls[0].options).toEqual({
      signal: controller.signal,
      transferInput: true,
    });
    expect(calls[1].options).toEqual({
      signal: controller.signal,
      transferInput: false,
    });
  });

  it("binds after one ready runtime while remaining projects reconcile independently", async () => {
    const value = await harness();
    const system = await createBrowserlessSystem(value.env, value.dependencies);

    await system.start();

    expect(value.server.listening).toBe(true);
    expect(value.sampler.starts).toBe(1);
    expect(system.registry.readyRuntimes()).toHaveLength(1);
    value.factory.releaseAll();
    await system.fleet.waitForReady(4);
    expect(system.registry.readyRuntimes()).toHaveLength(4);
    await system.close();
    expect(value.sampler.closes).toBe(1);
    expect(value.sessionCloses()).toBe(1);
    expect(system.governor.snapshot().closed).toBe(true);
  }, 30_000);

  it("never accepts protected port 8787 for a candidate", async () => {
    const value = await harness();
    await expect(createBrowserlessSystem({
      ...value.env,
      PORT: "8787",
    }, value.dependencies)).rejects.toThrow("port 8787");
  });

  it("waits for same-project sandbox recovery without creating a Prism project", async () => {
    const value = await harness();
    const createdChildren: ProjectId[] = [];
    value.dependencies.runtimeFactory = {
      create: input => new SupervisedSandboxRuntime({
        projectId: input.projectId,
        initialChild: new FailingRuntime(input.projectId, false, value.order),
        createChild: () => {
          createdChildren.push(input.projectId);
          return new DeferredRuntime(input.projectId, false, value.order);
        },
        initialBackoffMs: 1,
        maxBackoffMs: 10,
        checkIntervalMs: 10,
      }),
    };
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
    }, value.dependencies);

    await system.start();

    expect(system.registry.readyRuntimes()).toHaveLength(1);
    expect(createdChildren).toEqual([IDS[0]]);
    await system.close();
  }, 30_000);

  it("orders graceful shutdown without deleting or creating projects", async () => {
    const value = await harness();
    const env = {
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
    };
    const system = await createBrowserlessSystem(env, value.dependencies);
    const beginShutdown = system.router.beginShutdown.bind(system.router);
    let shutdownRecorded = false;
    system.router.beginShutdown = () => {
      if (!shutdownRecorded) {
        shutdownRecorded = true;
        value.order.push("router-drain");
      }
      beginShutdown();
    };
    await system.start();

    await system.close();

    expect(value.order).toEqual([
      "server-stop-admission",
      "router-drain",
      "runtime-drain",
      "awareness-clear",
      "tmp-clean",
      "runtime-stop",
      "account-close",
      "cpu-pool-close",
    ]);
  });
});
