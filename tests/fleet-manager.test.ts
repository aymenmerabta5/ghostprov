import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import type { MachineSample } from "../src/core/ports";
import { ProjectFleetManager } from "../src/fleet/manager";
import { FleetManifestStore } from "../src/fleet/manifest";
import type {
  ManagedProjectInspection,
  ManagedProjectInspector,
  ManagedRuntimeFactory,
} from "../src/fleet/types";
import type { RunHandle } from "../src/relay/run-handle";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { PrismProjectSummary } from "../src/prism/projects-client";

const A = projectId("e631d12c-d917-4434-b360-41b4715d6679");
const B = projectId("69288e8b-c183-45f3-9131-d64b05b98928");
const C = projectId("11111111-1111-4111-8111-111111111111");
const D = projectId("22222222-2222-4222-8222-222222222222");
const FINGERPRINT = "a".repeat(64);
const roots: string[] = [];

const MACHINE: MachineSample = {
  totalBytes: 32 * 1024 ** 3,
  freeBytes: 19 * 1024 ** 3,
  rssBytes: 320 * 1024 ** 2,
  heapBytes: 100 * 1024 ** 2,
  externalBytes: 20 * 1024 ** 2,
  logicalProcessors: 12,
};

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

function project(id: ProjectId, title = `GhostProv Worker ${id === A ? 1 : id === B ? 2 : 3}`): PrismProjectSummary {
  return { projectId: id, title, owner: "owner", deleted: false, createdAt: "2026-08-22T10:00:00.000Z" };
}

class FakeProjects {
  readonly created: Array<{ projectId: ProjectId; title: string }> = [];
  readonly deleted: ProjectId[] = [];

  constructor(readonly remote: PrismProjectSummary[]) {}

  async list(): Promise<readonly PrismProjectSummary[]> { return [...this.remote]; }

  async create(input: { projectId: ProjectId; title: string }): Promise<PrismProjectSummary> {
    this.created.push(input);
    const value = project(input.projectId, input.title);
    this.remote.push(value);
    return value;
  }

  async deleteManaged(id: ProjectId): Promise<void> { this.deleted.push(id); }
}

class FakeInspector implements ManagedProjectInspector {
  constructor(readonly values = new Map<ProjectId, ManagedProjectInspection>()) {}

  async inspect(id: ProjectId): Promise<ManagedProjectInspection> {
    return this.values.get(id) ?? { writable: false, empty: false, marker: null };
  }
}

class FakeRuntime implements RoutableSandboxRuntime {
  state = "configured";
  starts = 0;
  drains = 0;

  constructor(readonly projectId: ProjectId) {}

  async start(): Promise<void> { this.starts++; this.state = "ready"; }
  async drain(): Promise<void> { this.drains++; this.state = "stopped"; }
  async stop(): Promise<void> { this.state = "stopped"; }
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
      credentialExpiresInMs: 3_600_000,
      ackEwmaMs: 0,
      completionEwmaMs: 0,
      recentFailureRate: 0,
      queueDepth: 0,
      memoryPressure: 0,
      failure: null,
    };
  }
}

class FakeFactory implements ManagedRuntimeFactory {
  readonly created: Array<{ projectId: ProjectId; workerIndex: number; leaderGeneration: number }> = [];
  readonly runtimes = new Map<ProjectId, FakeRuntime>();

  create(input: { projectId: ProjectId; workerIndex: number; leaderGeneration: number }): FakeRuntime {
    this.created.push(input);
    const runtime = new FakeRuntime(input.projectId);
    this.runtimes.set(input.projectId, runtime);
    return runtime;
  }
}

async function state(): Promise<{ root: string; cookieFile: string; manifest: FleetManifestStore }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-fleet-manager-"));
  roots.push(root);
  return {
    root,
    cookieFile: path.join(root, "cookies.json"),
    manifest: new FleetManifestStore({
      file: path.join(root, "fleet.json"),
      identityFingerprint: FINGERPRINT,
    }),
  };
}

function config(cookieFile: string, input: {
  desired: number;
  explicit?: readonly ProjectId[];
  sessionMode?: "authenticated" | "auto" | "anonymous";
  guestMax?: number;
}) {
  return resolveMultiSandboxConfig({
    PRISM_COOKIE_FILE: cookieFile,
    PRISM_PROJECT_IDS: input.explicit?.join(","),
    GP_TRANSPORT: "v2",
    GP_PRISM_SESSION: input.sessionMode ?? "authenticated",
    GP_ALLOW_ANONYMOUS: input.sessionMode === "auto" ? "1" : "0",
    GP_AUTO_CREATE_PROJECTS: "1",
    GP_TARGET_SANDBOXES: String(input.desired),
    GP_GUEST_MAX_PROJECTS: String(input.guestMax ?? 4),
    GP_MAX_PROJECTS: "8",
    GP_PROVISION_CONCURRENCY: "2",
    GP_CANDIDATE: "1",
  }, MACHINE);
}

function manager(input: {
  config: ReturnType<typeof config>;
  manifest: FleetManifestStore;
  projects: FakeProjects;
  inspector?: FakeInspector;
  registry?: SandboxRegistry;
  factory?: FakeFactory;
  identityKind?: "authenticated" | "anonymous";
  randomIds?: ProjectId[];
}) {
  const factory = input.factory ?? new FakeFactory();
  const registry = input.registry ?? new SandboxRegistry();
  const ids = [...(input.randomIds ?? [D])];
  return {
    factory,
    registry,
    value: new ProjectFleetManager({
      config: input.config,
      identity: { kind: input.identityKind ?? "authenticated", fingerprint: FINGERPRINT },
      projects: input.projects,
      inspector: input.inspector ?? new FakeInspector(),
      manifest: input.manifest,
      registry,
      runtimeFactory: factory,
      random: { next: () => 0.5, uuid: () => ids.shift() ?? D },
    }),
  };
}

describe("ProjectFleetManager", () => {
  it("reuses explicit, manifest, and marked projects before creating the exact deficit", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 1 });
    await files.manifest.markCreated(B, 2);
    const projects = new FakeProjects([
      project(A, "User explicit project"),
      project(B, "GhostProv Worker 2"),
      project(C, "GhostProv Worker 3"),
    ]);
    const inspector = new FakeInspector(new Map([
      [C, { writable: true, empty: false, marker: { protocol: "gp-awareness-v2", workerIndex: 3 } }],
    ]));
    const harness = manager({
      config: config(files.cookieFile, { desired: 4, explicit: [A] }),
      manifest: files.manifest,
      projects,
      inspector,
      randomIds: [D],
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.projects).toEqual([A, B, C, D]);
    expect(projects.created).toEqual([{ projectId: D, title: "GhostProv Worker 4" }]);
    expect(harness.registry.runtimes()).toHaveLength(4);
    const records = (await files.manifest.load()).workers;
    expect(records.find(worker => worker.projectId === A)?.managed).toBe(false);
    expect(records.filter(worker => worker.projectId !== A).every(worker => worker.managed)).toBe(true);
  });

  it("is restart-idempotent after a persisted creating intent", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
    const projects = new FakeProjects([project(A, "GhostProv Worker 1")]);
    const harness = manager({
      config: config(files.cookieFile, { desired: 1 }),
      manifest: files.manifest,
      projects,
    });

    await harness.value.reconcile();

    expect(projects.created).toEqual([]);
    expect((await files.manifest.load()).workers[0].state).toBe("ready");
  });

  it("does not reuse a non-managed explicit project after it leaves the allowlist", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({
      index: 1,
      projectId: A,
      title: "User explicit project",
      nowMs: 1,
      managed: false,
    });
    await files.manifest.markCreated(A, 2);
    const projects = new FakeProjects([project(A, "User explicit project")]);
    const harness = manager({
      config: config(files.cookieFile, { desired: 1 }),
      manifest: files.manifest,
      projects,
      randomIds: [B],
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.projects).toEqual([B]);
    expect(projects.created).toEqual([{ projectId: B, title: "GhostProv Worker 2" }]);
  });

  it("drains excess local runtimes without deleting Prism projects", async () => {
    const files = await state();
    await files.manifest.initialize("authenticated");
    await files.manifest.reserveWorker({ index: 1, projectId: A, title: "GhostProv Worker 1", nowMs: 1 });
    await files.manifest.markCreated(A, 2);
    await files.manifest.reserveWorker({ index: 2, projectId: B, title: "GhostProv Worker 2", nowMs: 3 });
    await files.manifest.markCreated(B, 4);
    const projects = new FakeProjects([project(A), project(B)]);
    const left = new FakeRuntime(A);
    const right = new FakeRuntime(B);
    await left.start();
    await right.start();
    const registry = new SandboxRegistry([left, right]);
    const harness = manager({
      config: config(files.cookieFile, { desired: 1 }),
      manifest: files.manifest,
      projects,
      registry,
    });

    await harness.value.reconcile();

    expect(registry.get(A)).toBe(left);
    expect(registry.get(B)).toBeUndefined();
    expect(right.drains).toBe(1);
    expect(projects.deleted).toEqual([]);
  });

  it("applies the guest cap after auto mode resolves anonymously", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    const harness = manager({
      config: config(files.cookieFile, { desired: 4, sessionMode: "auto", guestMax: 1 }),
      identityKind: "anonymous",
      manifest: files.manifest,
      projects,
      randomIds: [A, B, C, D],
    });

    const snapshot = await harness.value.reconcile();

    expect(snapshot.desired).toBe(1);
    expect(projects.created).toHaveLength(1);
    expect(harness.registry.runtimes()).toHaveLength(1);
  });

  it("rejects reconciliation after shutdown without creating projects", async () => {
    const files = await state();
    const projects = new FakeProjects([]);
    const harness = manager({
      config: config(files.cookieFile, { desired: 1 }),
      manifest: files.manifest,
      projects,
      randomIds: [A],
    });

    await harness.value.shutdown("test-shutdown");

    await expect(harness.value.reconcile()).rejects.toThrow("shutting down");
    expect(projects.created).toEqual([]);
    expect(harness.registry.runtimes()).toEqual([]);
  });
});
