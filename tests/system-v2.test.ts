import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { SYSTEM_CLOCK, type MachineProbe } from "../src/core/ports";
import { isAuthenticatedTenantScope } from "../src/http/api-key";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { FleetLifecycleEvent, ManagedRuntimeFactory } from "../src/fleet/types";
import type { FleetLifecycleObservabilityEventName, ObservabilityEvent, ObservabilityPriority } from "../src/observability/events";
import type { LoggerLike } from "../src/observability/logger";
import type { RunHandle } from "../src/relay/run-handle";
import { sseFrame } from "../src/relay/sse";
import { ResourceGovernor } from "../src/router/resource-governor";
import type {
  RoutableSandboxRuntime,
  RoutableSandboxSnapshot,
  SandboxRunOptions,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { SupervisedSandboxRuntime } from "../src/sandbox/supervised-runtime";
import { parseYSweetEnvelope } from "../src/prism/protocol";
import {
  createBrowserlessSystem,
  createV2CpuGzip,
  emitFleetLifecycle,
  selectV2ProjectRelease,
  type BrowserlessSystemDependencies,
  type BrowserlessSystemServer,
} from "../src/server/system";
import { loadV2SidecarAssets, type V2SidecarAssets } from "../src/v2/sidecar/assets";
import { createCredentialRotationKey } from "../src/v2/bootstrap/crypto";
import type { SupervisorBootstrapAttempt } from "../src/v2/supervisor/bootstrap-journal";
import { sealSupervisorSessionBootstrap } from "../src/v2/supervisor/session-bootstrap";
import type { PrismProjectSummary } from "../src/prism/projects-client";
import { V2RunHandle } from "../src/v2/transport/run-handle";
import {
  v2EventPhysicalWorkingSetBytes,
  v2RetainedEventPermitBytes,
} from "../src/v2/memory/event-memory-sizing";

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
  async run(
    _body: Record<string, unknown>,
    _signal?: AbortSignal,
    _options?: SandboxRunOptions,
  ): Promise<RunHandle> { throw new Error("not used"); }
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

class CapturingRuntime extends DeferredRuntime {
  readonly bodies: Record<string, unknown>[] = [];

  override async run(
    body: Record<string, unknown>,
    _signal?: AbortSignal,
    _options?: SandboxRunOptions,
  ): Promise<RunHandle> {
    this.bodies.push(structuredClone(body));
    const final = {
      id: "resp_system_anthropic_model",
      object: "response",
      created_at: 1,
      status: "completed",
      model: body.model,
      error: null,
      incomplete_details: null,
      output: [{
        id: "msg_system_anthropic_model",
        type: "message",
        role: "assistant",
        status: "completed",
        content: [{
          type: "output_text",
          text: "ok",
          annotations: [],
          logprobs: [],
        }],
      }],
      usage: {
        input_tokens: 1,
        output_tokens: 1,
        input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
        output_tokens_details: { reasoning_tokens: 0 },
        total_tokens: 2,
      },
    };
    return {
      async *events() {},
      final: Promise.resolve(final),
      stats: { bytes: 0, firstByteMs: 0, frames: 0 },
    };
  }
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
      GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef",
      PRISM_COOKIE_FILE: cookieFile,
      PRISM_PROJECT_IDS: IDS.join(","),
      GP_TARGET_SANDBOXES: String(IDS.length),
      GP_PRISM_SESSION: "authenticated",
      GP_MAX_PROJECTS: "8",
      GP_PROVISION_CONCURRENCY: "4",
      GP_CANDIDATE: "1",
      GP_LOG_LEVEL: "off",
      PORT: "18888",
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
  const sharedLifecycle = { projectId: IDS[0]!, workerIndex: 1 };
  const lifecycleFields = { project_id: IDS[0]!, worker_index: 1 };
  const lifecycleCases = {
    bootstrap_cleanup_started: {
      input: { ...sharedLifecycle, type: "bootstrap_cleanup_started", leaderGeneration: 3 },
      name: "fleet.bootstrap_cleanup.started", fields: { ...lifecycleFields, leader_generation: 3 },
    },
    bootstrap_cleanup_succeeded: {
      input: { ...sharedLifecycle, type: "bootstrap_cleanup_succeeded", leaderGeneration: 3 },
      name: "fleet.bootstrap_cleanup.succeeded", fields: { ...lifecycleFields, leader_generation: 3 },
    },
    bootstrap_cleanup_deferred: {
      input: { ...sharedLifecycle, type: "bootstrap_cleanup_deferred", leaderGeneration: 3, reason: "proof_unavailable",
        cleanupStage: "proof_threw", errorName: "V2SupervisorError", bootstrapPhase: "deploy",
        bootstrapCode: "bootstrap_cleanup_unproven", bootstrapReason: "bootstrap", supervisorPhase: "bootstrap" },
      name: "fleet.bootstrap_cleanup.deferred", fields: { ...lifecycleFields, leader_generation: 3, cleanup_reason: "proof_unavailable",
        cleanup_stage: "proof_threw", error_name: "V2SupervisorError", bootstrap_phase: "deploy",
        bootstrap_code: "bootstrap_cleanup_unproven", bootstrap_reason: "bootstrap", supervisor_phase: "bootstrap" },
    },
    replacement_qualified: {
      input: { ...sharedLifecycle, type: "replacement_qualified", leaderGeneration: 3, failureKind: "bootstrap_blocked", failureCount: 4 },
      name: "fleet.replacement.qualified", fields: { ...lifecycleFields, leader_generation: 3, failure_kind: "bootstrap_blocked", failure_count: 4 },
    },
    replacement_started: {
      input: { ...sharedLifecycle, type: "replacement_started", leaderGeneration: 3, failureKind: "bootstrap_blocked", failureCount: 4 },
      name: "fleet.replacement.started", fields: { ...lifecycleFields, leader_generation: 3, failure_kind: "bootstrap_blocked", failure_count: 4 },
    },
    successor_reserved: {
      input: { ...sharedLifecycle, type: "successor_reserved" },
      name: "fleet.replacement.successor_reserved", fields: lifecycleFields,
    },
    successor_created: {
      input: { ...sharedLifecycle, type: "successor_created" },
      name: "fleet.replacement.successor_created", fields: lifecycleFields,
    },
    successor_activated: {
      input: { ...sharedLifecycle, type: "successor_activated", leaderGeneration: 3 },
      name: "fleet.replacement.successor_activated", fields: { ...lifecycleFields, leader_generation: 3 },
    },
    old_retired: {
      input: { ...sharedLifecycle, type: "old_retired", retirementReason: "replacement", managed: true },
      name: "fleet.replacement.old_retired", fields: { ...lifecycleFields, retirement_reason: "replacement", managed: true },
    },
    mutation_blocked: {
      input: { ...sharedLifecycle, type: "mutation_blocked", operation: "replace", reason: "bootstrap_hold_limit" },
      name: "fleet.mutation.blocked", fields: { ...lifecycleFields, mutation_operation: "replace", block_reason: "bootstrap_hold_limit" },
    },
  } satisfies {
    [K in FleetLifecycleEvent["type"]]: {
      input: FleetLifecycleEvent & { type: K };
      name: FleetLifecycleObservabilityEventName;
      fields: Record<string, string | number | boolean>;
    };
  };

  it.each(Object.entries(lifecycleCases))("forwards the %s lifecycle variant exactly once", (_type, value) => {
    const emitted: Array<{ event: ObservabilityEvent; priority?: ObservabilityPriority }> = [];
    const logger: LoggerLike = {
      emit: (event, priority) => { emitted.push({ event, priority }); },
      snapshot: () => ({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }),
      close: async () => {},
    };
    emitFleetLifecycle(logger, value.input);
    expect(emitted).toHaveLength(1);
    expect(emitted[0]).toMatchObject({ event: { event: value.name, ...value.fields }, priority: "terminal" });
    expect(Object.keys(emitted[0]!.event).sort()).toEqual(["schema", "event", "ts", "mono_ms", ...Object.keys(value.fields)].sort());
  });

  it("rejects an unknown lifecycle variant rather than silently dropping it", () => {
    const logger: LoggerLike = {
      emit: () => { throw new Error("unknown lifecycle event must not be emitted"); },
      snapshot: () => ({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }),
      close: async () => {},
    };
    expect(() => emitFleetLifecycle(logger, { ...sharedLifecycle, type: "unknown_variant" } as never))
      .toThrow("Unknown fleet lifecycle event");
  });

  it("backs completed image batches with the system governor while freeing the event lane", async () => {
    const value = await harness();
    const bodies: Record<string, unknown>[] = [];
    const workers: Promise<void>[] = [];
    const controller = new AbortController();
    value.dependencies.runtimeFactory = {
      create: ({ projectId }) => {
        const runtime = new DeferredRuntime(projectId, false, value.order);
        runtime.run = async (body, signal) => {
          const index = bodies.push(body);
          const handle = new V2RunHandle({ requireTransferredEventMemory: true });
          signal?.addEventListener("abort", () => {
            handle.cancel(new DOMException("Image request cancelled", "AbortError"));
          }, { once: true });
          const worker = (async () => {
            const payloadBytes = 700 * 1024;
            const final = {
              id: `resp_system_image_${index}`,
              object: "response",
              status: "completed",
              output: [{ type: "image_generation_call", status: "completed", result: "" }],
            };
            const event = "response.completed";
            const data = JSON.stringify({ type: event, response: final });
            const wireBytes = new TextEncoder().encode(sseFrame(event, data)).byteLength + payloadBytes;
            const permit = await system.hostEventMemory.permits.acquire(v2EventPhysicalWorkingSetBytes(wireBytes), {
              region: "jumbo", signal,
            });
            try {
              final.output[0]!.result = "A".repeat(payloadBytes);
              permit.shrink(v2RetainedEventPermitBytes(wireBytes));
              await handle.acceptEvent({ event, data: JSON.stringify({ type: event, response: final }) }, wireBytes, permit);
              handle.settle(final);
            } catch (error) {
              permit.release();
              throw error;
            }
          })();
          workers.push(worker);
          void worker.catch(() => undefined);
          return handle;
        };
        return runtime;
      },
    };
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    let pending: Promise<Response> | undefined;
    try {
      await system.start();
      const baseline = system.governor.snapshot().reservedBytes;
      pending = Promise.resolve(system.app.request("/v1/images/generations", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${value.env.GHOSTPROV_API_KEY}` },
        body: JSON.stringify({ model: "gpt-image-2", prompt: "offline image fixture", n: 2 }),
        signal: controller.signal,
      }));
      let response: Response | undefined;
      void pending.then(result => { response = result; });
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(response?.status).toBe(200);
      expect(bodies).toHaveLength(2);
      expect(bodies.every(body => body.stream === true && body.store === true)).toBe(true);
      expect(system.hostEventMemory.snapshot()).toMatchObject({ activePermits: 0, waiters: 0, jumboActive: false });
      expect(system.governor.snapshot().reservedBytes).toBeGreaterThan(baseline);
      await response!.body!.cancel();
      expect(system.governor.snapshot().reservedBytes).toBe(baseline);
    } finally {
      controller.abort();
      await Promise.allSettled(workers);
      if (pending) {
        const response = await pending;
        if (!response.bodyUsed) await response.body?.cancel();
      }
      await system.close();
    }
  });

  it("boots a managed fleet without PRISM_PROJECT_IDS and persists created projects", async () => {
    const value = await harness();
    const remote: PrismProjectSummary[] = [];
    const created: ProjectId[] = [];
    value.dependencies.projects = {
      get: async id => {
        const found = remote.find(project => project.projectId === id && !project.deleted);
        if (found) return found;
        const error = Object.assign(new Error("missing managed project"), {
          name: "PrismProjectError",
          kind: "http",
          status: 404,
        });
        throw error;
      },
      create: async input => {
        const project: PrismProjectSummary = {
          projectId: input.projectId,
          title: input.title,
          owner: "owner",
          deleted: false,
          createdAt: "2026-09-02T00:00:00.000Z",
        };
        created.push(project.projectId);
        remote.push(project);
        return project;
      },
      deleteManaged: async () => {},
    };
    value.dependencies.runtimeFactory = {
      create: input => new DeferredRuntime(input.projectId, false, value.order),
    };
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: undefined,
      GP_TARGET_SANDBOXES: "2",
    }, value.dependencies);

    await system.start();
    await system.fleet.waitForReady(2);
    expect(system.config.projectIds).toEqual([]);
    expect(created).toHaveLength(2);
    expect(system.fleet.snapshot()).toMatchObject({ desired: 2, ready: 2 });
    expect((await value.dependencies.manifest!.load()).workers.every(worker => worker.managed))
      .toBe(true);
    await system.close();
  });

  it("rejects invalid public host allowlists before acquiring system ownership", async () => {
    let machineSamples = 0;
    let releasePreloads = 0;
    let accountResolves = 0;
    await expect(createBrowserlessSystem({
      GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef",
      PRISM_COOKIE_FILE: "D:/unused.json",
      PRISM_PROJECT_IDS: IDS[0],
      GP_TARGET_SANDBOXES: "1",
      GP_CANDIDATE: "1",
    }, {
      allowedHosts: ["host/path"],
      machineProbe: {
        sample: () => {
          machineSamples++;
          throw new Error("machine ownership must not be acquired");
        },
      },
      revisionSelector: {
        preload: async () => { releasePreloads++; },
        loadedForProject: () => { throw new Error("not used"); },
      },
      sessionManager: {
        resolve: async () => {
          accountResolves++;
          throw new Error("account ownership must not be acquired");
        },
        close: async () => {},
      },
    })).rejects.toThrow("Allowed public host is invalid");
    expect({ machineSamples, releasePreloads, accountResolves }).toEqual({
      machineSamples: 0,
      releasePreloads: 0,
      accountResolves: 0,
    });
  });

  it("threads an external startup abort through immutable release preload", async () => {
    const value = await harness();
    const controller = new AbortController();
    let observedSignal: AbortSignal | undefined;
    let accountResolves = 0;
    value.dependencies.revisionSelector = {
      preload: async signal => {
        observedSignal = signal;
        if (!signal) throw new Error("missing release preload startup signal");
        await new Promise<never>(() => undefined);
      },
      loadedForProject: () => { throw new Error("release preload did not finish"); },
    };
    value.dependencies.sessionManager = {
      resolve: async () => {
        accountResolves++;
        throw new Error("account resolution must not run after preload abort");
      },
      close: async () => {},
    };

    const creating = createBrowserlessSystem(
      value.env,
      value.dependencies,
      controller.signal,
    );
    for (let attempt = 0; attempt < 100 && !observedSignal; attempt++) await Bun.sleep(0);
    controller.abort(new DOMException("cancel system construction", "AbortError"));

    await expect(creating).rejects.toMatchObject({ name: "AbortError" });
    expect(observedSignal).toBe(controller.signal);
    expect(accountResolves).toBe(0);
  });

  it("passes the external startup signal into account resolution and closes on abort", async () => {
    const value = await harness();
    const controller = new AbortController();
    let observedSignal: AbortSignal | undefined;
    value.dependencies.sessionManager = {
      resolve: async signal => {
        observedSignal = signal;
        if (!signal) throw new Error("missing account startup signal");
        return await new Promise<never>(() => undefined);
      },
      close: async () => { value.order.push("account-close"); },
    };

    const creating = createBrowserlessSystem(
      value.env,
      value.dependencies,
      controller.signal,
    );
    for (let attempt = 0; attempt < 100 && !observedSignal; attempt++) await Bun.sleep(0);
    controller.abort(new DOMException("cancel account resolution", "AbortError"));

    await expect(creating).rejects.toMatchObject({ name: "AbortError" });
    expect(observedSignal).toBe(controller.signal);
    expect(value.order).toContain("account-close");
  });

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
    const events: unknown[] = [];
    value.dependencies.logger = {
      emit: event => { events.push(event); },
      snapshot: () => ({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }),
      close: async () => {},
    };
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    expect(isAuthenticatedTenantScope(
      system.router.responseProvenanceAuthorizationScope(),
    )).toBe(true);
    expect(system.anthropicContinuationIndex.snapshot()).toMatchObject({ records: 0 });

    await system.start();

    expect(value.server.listening).toBe(true);
    expect(value.sampler.starts).toBe(1);
    expect(system.registry.readyRuntimes()).toHaveLength(1);
    expect(events).toContainEqual(expect.objectContaining({ event: "server.started", desired: 4, ready: 1,
      routable: 1, held_project_count: 0, fleet_readiness: "degraded" }));
    expect(system.hostEventMemory.reservedBytes).toBe(system.config.v2HostEventMemory.totalBytes);
    expect(system.hostEventMemory.snapshot()).toMatchObject({
      ordinaryCapacityBytes: system.config.v2HostEventMemory.ordinaryBytes,
      jumboCapacityBytes: system.config.v2HostEventMemory.jumboBytes,
      closed: false,
    });
    expect(system.responseGraphMemory.maximumBytes)
      .toBe(system.responseGraphMemory.budget.limit);
    expect(system.responseGraphMemory.reservedBytes).toBe(0);
    expect(system.webSocketIngressMemory.reservedBytes).toBe(0);
    expect(system.responseGraphMemory.budget.used).toBe(0);
    expect(system.governor.snapshot().reservedBytes)
      .toBe(system.hostEventMemory.reservedBytes);
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

  it.each([false, true])("audits every manifest journal before certifying shutdown (retained=%s)", async retained => {
    const value = await harness();
    const inspected: ProjectId[] = [];
    value.dependencies.bootstrapJournalFactory = id => ({
      withLease: async (_signal, operation) => operation({
        load: async () => {
          inspected.push(id);
          value.order.push("journal-check");
          return retained && id === IDS[3] ? { phase: "submitted" } as never : null;
        },
        save: async () => { throw new Error("audit must not mutate journals"); },
        clear: async () => { throw new Error("audit must not mutate journals"); },
      }),
    });
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    expect(system.supportsVerifiedShutdown).toBe(true);
    await system.start();
    value.factory.releaseAll();
    await system.fleet.waitForReady(4);
    if (retained) await expect(system.close()).rejects.toThrow("bootstrap cleanup was incomplete");
    else await system.close();
    expect(inspected).toEqual(IDS);
    expect(value.order.indexOf("journal-check")).toBeGreaterThan(value.order.indexOf("runtime-stop"));
    expect(value.order.indexOf("account-close")).toBeGreaterThan(value.order.lastIndexOf("journal-check"));
    expect(value.sessionCloses()).toBe(1);
  });

  it("connects default R10 maintenance to held journals after healthy fleet readiness", async () => {
    const value = await harness();
    const events: unknown[] = [];
    value.dependencies.logger = {
      emit: event => { events.push(event); },
      snapshot: () => ({ queuedEvents: 0, queuedBytes: 0, droppedSamples: 0, droppedProgress: 0, droppedTerminals: 0, sinkErrors: 0 }),
      close: async () => {},
    };
    const held = projectId("00000000-0000-4000-8000-000000000005");
    const manifest = value.dependencies.manifest!;
    await manifest.initialize("authenticated");
    await manifest.reserveWorker({ index: 5, projectId: held, title: "offline held project", nowMs: 1 });
    await manifest.nextLeaderGeneration(held, 2);
    await manifest.markDraining(held, "bootstrap_hold", 3);
    let heldReads = 0;
    value.dependencies.bootstrapJournalFactory = id => ({
      withLease: async (_signal, operation) => operation({
        load: async () => { if (id === held) heldReads++; return null; },
        save: async () => { throw new Error("absent proof must not be created"); },
        clear: async () => { throw new Error("absent proof must not be cleared"); },
      }),
    });
    value.dependencies.runtimeFactory = { create: input => {
      const runtime = new DeferredRuntime(input.projectId, false, value.order);
      const snapshot = runtime.snapshot.bind(runtime);
      runtime.snapshot = () => ({ ...snapshot(), releaseRevision: "r10" });
      return runtime;
    } };
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    try {
      await system.start();
      await system.fleet.waitForReady(4);
      await system.fleet.reconcile();
      for (let turn = 0; turn < 10; turn++) await new Promise<void>(resolve => setImmediate(resolve));
      expect(heldReads).toBeGreaterThan(0);
      expect(events).toContainEqual(expect.objectContaining({ event: "fleet.bootstrap_cleanup.started",
        project_id: held, worker_index: 5, leader_generation: 1 }));
      expect(events).toContainEqual(expect.objectContaining({ event: "fleet.bootstrap_cleanup.deferred",
        project_id: held, worker_index: 5, leader_generation: 1, cleanup_reason: "proof_unavailable" }));
      expect((await manifest.load()).workers.find(worker => worker.projectId === held)?.retirementReason).toBe("bootstrap_hold");
    } finally {
      // A missing journal cannot resolve the manifest's outstanding cleanup
      // hold or authorize an unattended poweroff.
      await expect(system.close()).rejects.toThrow("bootstrap cleanup was incomplete");
      expect(value.sessionCloses()).toBe(1);
      expect((await manifest.load()).workers.find(worker => worker.projectId === held)?.retirementReason).toBe("bootstrap_hold");
    }
  });

  it("forwards the held-maintenance host factory to exact submitted cleanup", async () => {
    const value = await harness();
    const assets = await loadV2SidecarAssets({ revision: "r10" });
    const held = projectId("00000000-0000-4000-8000-000000000005");
    const manifest = value.dependencies.manifest!;
    await manifest.initialize("authenticated");
    await manifest.reserveWorker({ index: 5, projectId: held, title: "offline held project", nowMs: 1 });
    await manifest.nextLeaderGeneration(held, 2);
    await manifest.markDraining(held, "bootstrap_hold", 3);
    const ySweet = parseYSweetEnvelope({ docId: held, url: "wss://offline.invalid/ws",
      baseUrl: "https://offline.invalid/", token: "offline-token", authorization: "full" });
    const binding = { projectId: held, sandboxSessionId: "offline-held-session", leaderGeneration: 1,
      nonce: "offline-held-supervisor", buildHash: assets.buildHash, schemaHash: assets.schemaHash,
      protocolRevision: "r2" as const, schemaVersion: 2 as const };
    const sessionId = "22222222-2222-4222-8222-222222222222";
    const rotationKey = createCredentialRotationKey();
    const sealed = await sealSupervisorSessionBootstrap({ sessionId, binding, payload: {
      instanceId: "offline-held-instance", credential: { url: ySweet.url, baseUrl: ySweet.baseUrl,
        docId: held, token: "offline-token", authorization: "full", rotationKey },
    } });
    let record: SupervisorBootstrapAttempt | null = {
      version: 1, phase: "submitted", sessionId, binding, connectTimeoutMs: 1000, sealed,
    };
    value.dependencies.bootstrapJournalFactory = id => ({
      withLease: async (signal, operation) => {
        signal?.throwIfAborted();
        return operation({
          load: async () => id === held ? record : null,
          save: async next => { if (id === held) record = next; },
          clear: async () => { if (id === held) record = null; },
        });
      },
    });
    const resolveIdentity = value.dependencies.sessionManager!.resolve.bind(value.dependencies.sessionManager);
    value.dependencies.sessionManager!.resolve = async signal => {
      const identity = await resolveIdentity(signal);
      return { ...identity, account: { request: async (url: string | URL) => {
        expect(String(url)).toBe("/api/y");
        return Response.json({ docId: held, url: ySweet.url, baseUrl: ySweet.baseUrl,
          token: "offline-token", authorization: "full" });
      } } as any };
    };
    value.dependencies.assets = assets;
    value.dependencies.runtimeFactory = {
      create: input => new DeferredRuntime(input.projectId, false, value.order),
    };
    const calls: string[] = [];
    value.dependencies.heldBootstrapHostFactory = input => {
      expect(input.binding).toEqual(binding);
      expect(input.instanceId).toBe("offline-held-instance");
      expect(input.rotationKey).toBe(rotationKey);
      return {
        binding: input.binding,
        instanceId: input.instanceId,
        async connect() { throw new Error("cleanup must not request readiness"); },
        async ensureReady() { throw new Error("cleanup must not request readiness"); },
        async launch() { throw new Error("cleanup must not launch"); },
        async deliverCredential() { throw new Error("cleanup must not deliver credentials"); },
        async stop() { throw new Error("cleanup must not stop a sidecar"); },
        async rotateCredential() { throw new Error("cleanup must not rotate independently"); },
        async shutdown() { calls.push("shutdown"); },
        async close() { calls.push("close"); },
      };
    };
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    try {
      await system.start();
      await system.fleet.waitForReady(4);
      await system.fleet.reconcile();
      for (let turn = 0; turn < 100 && record?.phase !== "cleanup_proven"; turn++) {
        await new Promise<void>(resolveTurn => setImmediate(resolveTurn));
      }
      expect(record?.phase).toBe("cleanup_proven");
      expect(calls).toEqual(["shutdown", "close"]);
    } finally {
      // The unit stops before the manager's later artifact-finalization pass.
      record = null;
      await expect(system.close()).rejects.toThrow("bootstrap cleanup was incomplete");
    }
  });

  it("uses Anthropic model overrides from the supplied system environment", async () => {
    const value = await harness();
    const runtime = new CapturingRuntime(IDS[0]!, false, value.order);
    value.dependencies.runtimeFactory = { create: () => runtime };
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
      GP_TARGET_SANDBOXES: "1",
      GP_ANTHROPIC_OPUS_MODEL: "system-explicit-opus",
    }, value.dependencies);
    try {
      await system.start();
      const response = await system.app.request("/v1/messages", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "anthropic-version": "2023-06-01",
          "x-api-key": value.env.GHOSTPROV_API_KEY,
        },
        body: JSON.stringify({
          model: "claude-opus-4-1",
          max_tokens: 8,
          messages: [{ role: "user", content: "use explicit system model" }],
        }),
      });
      expect(response.status).toBe(200);
      await response.json();
      expect(runtime.bodies).toHaveLength(1);
      expect(runtime.bodies[0]).toMatchObject({
        model: "system-explicit-opus",
        stream: true,
        store: true,
      });
    } finally {
      await system.close();
    }
    // This composition test persists a real manifest. Use the same bounded
    // setup/cleanup budget as the neighboring fleet composition tests.
  }, 30_000);

  it("shares explicit public host allowlists with the system-composed app", async () => {
    const value = await harness();
    value.dependencies.allowedHosts = ["ghostprov.example"];
    const system = await createBrowserlessSystem(value.env, value.dependencies);
    try {
      expect((await system.app.request("http://ghostprov.example/health")).status).toBe(503);
      expect((await system.app.request("http://attacker.example/health")).status).toBe(421);
    } finally {
      await system.close();
    }
  });

  it("propagates an external abort into fleet reconciliation", async () => {
    const value = await harness();
    const controller = new AbortController();
    let reconciliationSignal: AbortSignal | undefined;
    value.dependencies.projects = {
      get: async (_id, signal) => {
        reconciliationSignal = signal;
        if (!signal) throw new Error("missing reconciliation startup signal");
        return await new Promise<never>((_resolve, reject) => {
          if (signal.aborted) reject(signal.reason);
          else signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    };
    const system = await createBrowserlessSystem(
      value.env,
      value.dependencies,
      controller.signal,
    );
    const starting = system.start();
    const reconciliationDeadline = Date.now() + 2_000;
    while (!reconciliationSignal && Date.now() < reconciliationDeadline) {
      await Bun.sleep(5);
    }
    expect(reconciliationSignal).toBeDefined();
    controller.abort(new DOMException("cancel fleet reconciliation", "AbortError"));
    await expect(starting).rejects.toMatchObject({ name: "AbortError" });
    expect(reconciliationSignal?.aborted).toBe(true);
    await system.close();
  });

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
      GP_TARGET_SANDBOXES: "2",
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
    });
    await system.close();
  }, 30_000);

  it("waits through a fleet-wide project control-plane outage instead of restart-looping", async () => {
    const value = await harness();
    value.dependencies.projects = {
      get: async () => { throw new Error("project metadata unavailable"); },
    };
    const controller = new AbortController();
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: [IDS[0], IDS[1]].join(","),
      GP_TARGET_SANDBOXES: "2",
    }, value.dependencies, controller.signal);

    const starting = system.start();
    try {
      const deadline = Date.now() + 2_000;
      while (system.fleet.snapshot().projects.length < 2 && Date.now() < deadline) {
        await Bun.sleep(5);
      }
      expect(value.server.listening).toBe(false);
      expect(system.registry.runtimes()).toEqual([]);
      expect(system.fleet.snapshot()).toMatchObject({
        desired: 2,
        ready: 0,
        failed: 2,
        unavailable: 2,
      });
    } finally {
      controller.abort(new DOMException("test outage complete", "AbortError"));
      await expect(starting).rejects.toMatchObject({ name: "AbortError" });
      await system.close();
    }
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

  it("never accepts protected port 18787 for a candidate", async () => {
    const value = await harness();
    await expect(createBrowserlessSystem({
      ...value.env,
      PORT: "18787",
    }, value.dependencies)).rejects.toThrow("port 18787");
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
      GP_TARGET_SANDBOXES: "1",
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
      GP_TARGET_SANDBOXES: "1",
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

  it.each([
    { totalMs: 1_000, setupMs: 0, completesAtMs: null },
    { totalMs: 180_000, setupMs: 0, completesAtMs: null },
    { totalMs: 1_000, setupMs: 300, completesAtMs: null },
    { totalMs: 1_000, setupMs: 0, completesAtMs: 200 },
  ])("reserves exact cleanup time within the shared shutdown deadline (%j)", async ({ totalMs, setupMs, completesAtMs }) => {
    const value = await harness();
    const cleanupTimeMs = 100;
    const allDraining = Promise.withResolvers<void>();
    const proofAvailable = Promise.withResolvers<void>();
    const signedShutdowns = new Set<ProjectId>();
    const cleanupStarts: number[] = [];
    let held = true;
    let heldRuntime: DeferredRuntime | undefined;
    let now = SYSTEM_CLOCK.now();
    const shutdownStartedAt = now;
    value.dependencies.runtimeFactory = { create: input => new class extends DeferredRuntime {
      constructor() {
        super(input.projectId, false, value.order);
        if (input.projectId === IDS[0]) heldRuntime = this;
      }
      override snapshot(): RoutableSandboxSnapshot {
        return { ...super.snapshot(), active: this.projectId === IDS[0] && held ? 1 : 0 };
      }
      override async drain(): Promise<void> {
        cleanupStarts.push(now);
        if (cleanupStarts.length === IDS.length) allDraining.resolve();
        // The fake supervisor cannot produce its exact cleanup proof until
        // its independent remote operation has had time to complete.
        await proofAvailable.promise;
        if (now >= shutdownStartedAt + totalMs) throw new Error("signed supervisor cleanup deadline expired");
        signedShutdowns.add(this.projectId);
        if (this.projectId === IDS[0]) held = false;
        await super.drain();
      }
      override async stop(): Promise<void> {
        if (!signedShutdowns.has(this.projectId)) throw new Error("signed supervisor cleanup remains unproven");
        await super.stop();
      }
    }() };
    value.dependencies.bootstrapJournalFactory = id => ({
      withLease: async (_signal, operation) => operation({
        load: async () => signedShutdowns.has(id) ? null : { phase: "submitted" } as never,
        save: async () => { throw new Error("final audit must not create proof"); },
        clear: async () => { throw new Error("final audit must not manufacture cleanup"); },
      }),
    });
    const system = await createBrowserlessSystem({ ...value.env, GP_SHUTDOWN_DRAIN_MS: String(totalMs) }, value.dependencies);
    await system.start();
    await system.fleet.waitForReady(IDS.length);
    await system.fleet.reconcile();
    const closeSampler = value.sampler.close.bind(value.sampler);
    value.sampler.close = async () => { now += setupMs; await closeSampler(); };
    const clockNow = spyOn(SYSTEM_CLOCK, "now").mockImplementation(() => now);
    const clockSleep = spyOn(SYSTEM_CLOCK, "sleep").mockImplementation(async (ms, signal) => {
      signal?.throwIfAborted();
      now += ms;
      if (held && completesAtMs !== null && now >= shutdownStartedAt + completesAtMs) {
        held = false;
        heldRuntime?.emit();
        value.order.push("active-response-completed");
      }
    });
    const closing = system.close().then(() => null, error => error);
    try {
      await allDraining.promise;
      now += cleanupTimeMs;
      proofAvailable.resolve();
      const error = await closing;

      if (completesAtMs === null) {
        expect(error).toMatchObject({ name: "RouterShutdownError" });
        expect(error.message).toContain("active work");
      } else {
        expect(error).toBeNull();
        expect(value.order.indexOf("active-response-completed")).toBeLessThan(value.order.indexOf("runtime-drain"));
      }
      expect(cleanupStarts).toHaveLength(IDS.length);
      expect(Math.max(...cleanupStarts) + cleanupTimeMs).toBeLessThan(shutdownStartedAt + totalMs);
      expect(signedShutdowns.size).toBe(IDS.length);
      expect(system.registry.runtimes()).toHaveLength(0);
      expect(now).toBeLessThan(shutdownStartedAt + totalMs);
      expect(value.sessionCloses()).toBe(1);
    } finally {
      proofAvailable.resolve();
      await closing;
      clockSleep.mockRestore();
      clockNow.mockRestore();
    }
  });

  it("cancels a pending listen and cannot resurrect a listener after close", async () => {
    const value = await harness();
    const server = new DeferredListenServer(value.order);
    value.dependencies.server = server;
    const system = await createBrowserlessSystem({
      ...value.env,
      PRISM_PROJECT_IDS: IDS[0],
      GP_TARGET_SANDBOXES: "1",
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
      GP_TARGET_SANDBOXES: "1",
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
      GP_TARGET_SANDBOXES: "1",
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
