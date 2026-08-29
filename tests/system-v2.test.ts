import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type { MachineProbe } from "../src/core/ports";
import { isAuthenticatedTenantScope } from "../src/http/api-key";
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

class DeferredCloseServer extends FakeServer {
  closeCallback: ((error?: Error) => void) | null = null;

  override close(callback?: (error?: Error) => void): this {
    this.order.push("server-stop-admission");
    this.listening = false;
    this.closeCallback = callback ?? null;
    return this;
  }

  finishClose(): void {
    const callback = this.closeCallback;
    this.closeCallback = null;
    callback?.();
  }
}

class DeferredListenServer extends FakeServer {
  listenCallback: (() => void) | null = null;
  closeCalls = 0;

  override listen(_port: number, _host: string, callback: () => void): this {
    this.listening = true;
    this.listenCallback = callback;
    return this;
  }

  override close(callback?: (error?: Error) => void): this {
    this.closeCalls++;
    this.order.push("server-stop-admission");
    this.listening = false;
    queueMicrotask(() => callback?.());
    return this;
  }

  fireLateListenCallback(): void {
    this.listenCallback?.();
  }
}

class ThrowingListenServer extends FakeServer {
  override listen(_port: number, _host: string, _callback: () => void): this {
    throw new Error("synthetic listen failure");
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
  it("fails closed on a selected release before account or server side effects", async () => {
    const value = await harness();
    let accountResolves = 0;
    value.dependencies.sessionManager = {
      resolve: async () => {
        accountResolves++;
        throw new Error("account resolution must not run");
      },
      close: async () => {},
    };
    value.dependencies.revisionSelector = {
      preload: async () => { throw new Error("selected sidecar artifact is corrupt"); },
      loadedForProject: () => { throw new Error("not preloaded"); },
    };

    await expect(createBrowserlessSystem(value.env, value.dependencies))
      .rejects.toThrow("selected sidecar artifact is corrupt");
    expect(accountResolves).toBe(0);
    expect(value.server.listening).toBe(false);
    expect(value.sampler.starts).toBe(0);
  });

  it("selects the exact immutable release pair for each configured project", () => {
    const asset = (revision: string, fill: string) => Object.freeze({
      revision,
      protocolRevision: revision === "r1" ? "r1" : "r2",
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
    expect(isAuthenticatedTenantScope(
      system.router.responseProvenanceAuthorizationScope(),
    )).toBe(true);
    expect(system.anthropicContinuationIndex.snapshot()).toMatchObject({ entries: 0 });

    await system.start();

    expect(value.server.listening).toBe(true);
    expect(value.sampler.starts).toBe(1);
    expect(system.registry.readyRuntimes()).toHaveLength(1);
    expect(system.hostEventMemory.reservedBytes).toBe(system.config.v2HostEventMemory.totalBytes);
    expect(system.hostEventMemory.snapshot()).toMatchObject({
      ordinaryCapacityBytes: system.config.v2HostEventMemory.ordinaryBytes,
      jumboCapacityBytes: system.config.v2HostEventMemory.jumboBytes,
      closed: false,
    });
    expect(system.responseGraphMemory.reservedBytes)
      .toBe(system.responseGraphMemory.budget.limit);
    expect(system.responseGraphMemory.budget.used).toBe(0);
    expect(system.governor.snapshot().reservedBytes)
      .toBeGreaterThanOrEqual(
        system.hostEventMemory.reservedBytes +
        system.responseGraphMemory.reservedBytes +
        system.webSocketIngressMemory.reservedBytes,
      );
    value.factory.releaseAll();
    await system.fleet.waitForReady(4);
    expect(system.registry.readyRuntimes()).toHaveLength(4);
    await system.close();
    expect(value.sampler.closes).toBe(1);
    expect(value.sessionCloses()).toBe(1);
    expect(system.governor.snapshot().closed).toBe(true);
    expect(system.hostEventMemory.snapshot().closed).toBe(true);
    expect(system.responseGraphMemory.budget.used).toBe(0);
    expect(system.webSocketIngressMemory.budget.snapshot()).toMatchObject({
      sessions: 0,
      ingressReservedBytes: 0,
    });
  }, 30_000);

  it("binds with one exact healthy project when another explicit project metadata read fails", async () => {
    const value = await harness();
    value.factory.releaseAll();
    const projects = value.dependencies.projects!;
    value.dependencies.projects = {
      get: async (id, signal) => {
        if (id === IDS[0]) throw new Error("transient project metadata outage");
        return projects.get(id, signal);
      },
    };
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: [IDS[0], IDS[1]].join(","),
    }, value.dependencies);

    await system.start();

    expect(value.server.listening).toBe(true);
    expect(system.registry.readyRuntimes().map(runtime => runtime.projectId)).toEqual([IDS[1]]);
    expect(system.fleet.snapshot()).toMatchObject({
      desired: 2,
      ready: 1,
      failed: 1,
      unavailable: 1,
      unavailableProjects: [IDS[0]],
      projects: [IDS[0], IDS[1]],
    });
    expect(await (await system.app.request("/health")).json()).toMatchObject({
      ok: true,
      desired: 2,
      ready: 1,
      unavailable: 1,
      unavailableProjects: [IDS[0]],
    });
    await system.close();
  }, 30_000);

  it("fails startup when every exact configured project metadata read fails", async () => {
    const value = await harness();
    value.dependencies.projects = {
      get: async () => { throw new Error("project metadata unavailable"); },
    };
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: [IDS[0], IDS[1]].join(","),
    }, value.dependencies);

    await expect(system.start()).rejects.toMatchObject({
      name: "FleetReconciliationError",
      kind: "explicit_project_missing",
    });
    expect(value.server.listening).toBe(false);
    expect(system.registry.runtimes()).toEqual([]);
    expect(system.fleet.snapshot()).toMatchObject({
      desired: 0,
      ready: 0,
      failed: 0,
      unavailable: 0,
    });
    await system.close();
  }, 30_000);

  it("fails closed when one legal jumbo event cannot fit host headroom", async () => {
    const value = await harness();
    const constrained: MachineProbe = {
      sample: () => ({
        totalBytes: 512 * 1024 ** 2,
        freeBytes: 96 * 1024 ** 2,
        rssBytes: 384 * 1024 ** 2,
        heapBytes: 64 * 1024 ** 2,
        externalBytes: 16 * 1024 ** 2,
        logicalProcessors: 4,
      }),
    };
    value.dependencies.machineProbe = constrained;
    value.dependencies.governor = new ResourceGovernor({ probe: constrained });

    await expect(createBrowserlessSystem(value.env, value.dependencies))
      .rejects.toThrow(/memory|capacity/i);
    expect(value.server.listening).toBe(false);
    expect(value.sessionCloses()).toBe(1);
    expect(value.dependencies.governor.snapshot().closed).toBe(true);
  });

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

  it("cancels a pending listen and cannot resurrect a listener after close", async () => {
    const value = await harness();
    const server = new DeferredListenServer(value.order);
    value.dependencies.server = server;
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
    }, value.dependencies);

    const starting = system.start();
    for (const deadline = performance.now() + 5_000;
      !server.listenCallback && performance.now() < deadline;) await Bun.sleep(5);
    expect(server.listenCallback).not.toBeNull();
    const closing = system.close();
    await expect(starting).rejects.toMatchObject({ name: "AbortError" });
    await closing;
    server.fireLateListenCallback();
    await Bun.sleep(0);

    expect(server.listening).toBe(false);
    expect(server.closeCalls).toBe(1);
    expect(system.registry.runtimes()).toEqual([]);
    expect(value.sessionCloses()).toBe(1);
  });

  it("cleans the partial system exactly once after a synchronous listen failure", async () => {
    const value = await harness();
    const server = new ThrowingListenServer(value.order);
    value.dependencies.server = server;
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
    }, value.dependencies);

    await expect(system.start()).rejects.toThrow("synthetic listen failure");
    await system.close();

    expect(server.listening).toBe(false);
    expect(system.registry.runtimes()).toEqual([]);
    expect(value.sessionCloses()).toBe(1);
    expect(system.governor.snapshot().closed).toBe(true);
  });

  it("keeps host event permits alive until a slow HTTP server output closes", async () => {
    const value = await harness();
    const server = new DeferredCloseServer(value.order);
    value.dependencies.server = server;
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
      GP_SHUTDOWN_DRAIN_MS: "1000",
    }, value.dependencies);
    await system.start();
    const permit = system.hostEventMemory.permits.tryAcquire(1024, {
      region: "ordinary",
      label: "slow-sse-output",
    });
    expect(permit).not.toBeNull();

    const closing = system.close();
    for (const deadline = performance.now() + 5_000;
      !server.closeCallback && performance.now() < deadline;) await Bun.sleep(5);
    expect(server.closeCallback).not.toBeNull();
    expect(system.hostEventMemory.snapshot()).toMatchObject({
      closed: false,
      activePermits: 1,
    });

    permit!.release();
    server.finishClose();
    await closing;
    expect(system.hostEventMemory.snapshot()).toMatchObject({
      closed: true,
      activePermits: 0,
    });
  });
});
