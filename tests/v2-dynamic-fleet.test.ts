import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import { ProjectFleetManager } from "../src/fleet/manager";
import { FleetManifestStore } from "../src/fleet/manifest";
import type { ManagedRuntimeFactory } from "../src/fleet/types";
import type { RunHandle } from "../src/relay/run-handle";
import { ResourceGovernor } from "../src/router/resource-governor";
import { chooseRuntime } from "../src/router/policy";
import { MultiSandboxRouter } from "../src/router/router";
import { classifyWorkload } from "../src/router/workload";
import {
  SandboxRegistry,
  type RoutableSandboxRuntime,
  type RoutableSandboxSnapshot,
} from "../src/sandbox/registry";
import { projectId, type ProjectId } from "../src/sandbox/types";
import type { PrismProjectSummary } from "../src/prism/projects-client";

const roots: string[] = [];
const FINGERPRINT = "a".repeat(64);

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

function id(index: number): ProjectId {
  return projectId(`00000000-0000-4000-8000-${String(index).padStart(12, "0")}`);
}

class Projects {
  readonly remote: PrismProjectSummary[] = [];
  readonly created: ProjectId[] = [];
  readonly deleted: ProjectId[] = [];
  async list(): Promise<readonly PrismProjectSummary[]> { return [...this.remote]; }
  async get(id: ProjectId): Promise<PrismProjectSummary> {
    const found = this.remote.find(value => value.projectId === id && !value.deleted);
    if (!found) throw new Error(`missing project ${id}`);
    return found;
  }
  async create(input: { projectId: ProjectId; title: string }): Promise<PrismProjectSummary> {
    const value = {
      projectId: input.projectId,
      title: input.title,
      owner: "owner",
      deleted: false,
      createdAt: "2026-08-22T10:00:00.000Z",
    };
    this.created.push(input.projectId);
    this.remote.push(value);
    return value;
  }
}

class Runtime implements RoutableSandboxRuntime {
  state = "configured";
  active = 0;
  calls = 0;
  constructor(readonly projectId: ProjectId) {}
  async start(): Promise<void> { this.state = "ready"; }
  async drain(): Promise<void> { this.state = "stopped"; }
  async stop(): Promise<void> { this.state = "stopped"; }
  async run(body: Record<string, unknown>): Promise<RunHandle> {
    this.active++;
    const call = ++this.calls;
    const final = Bun.sleep(1).then(() => ({
      id: `resp_${this.projectId}_${call}`,
      status: "completed",
      input: body.input,
    })).finally(() => { this.active--; });
    return {
      async *events() {},
      final,
      stats: { bytes: 0, firstByteMs: 1, frames: 0 },
    };
  }
  snapshot(): RoutableSandboxSnapshot {
    const capacity = this.state === "ready" ? 4 : 0;
    return {
      projectId: this.projectId,
      state: this.state,
      capacity,
      active: this.active,
      available: Math.max(0, capacity - this.active),
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
}

class Factory implements ManagedRuntimeFactory {
  readonly created: Runtime[] = [];
  create(input: { projectId: ProjectId }): Runtime {
    const runtime = new Runtime(input.projectId);
    this.created.push(runtime);
    return runtime;
  }
}

describe("dynamic V2 fleet scale", () => {
  it("keeps the 32-runtime routing hot path below a 2 ms p95", () => {
    const workload = classifyWorkload({ model: "gpt-5.6-sol", input: "route" });
    const candidates = Array.from({ length: 32 }, (_, index) => ({
      projectId: id(index + 1),
      state: "ready",
      capacity: 128,
      active: index % 17,
      reserved: index % 3,
      quarantined: 0,
      queueDepth: index % 5,
      ackEwmaMs: 10 + index,
      completionEwmaMs: 500 + index * 3,
      recentFailureRate: 0,
      heartbeatAgeMs: 100,
      credentialExpiresInMs: 60_000,
      memoryPressure: 0,
    }));
    const random = { next: () => 0.5, uuid: () => id(99) };
    for (let index = 0; index < 2_000; index++) {
      chooseRuntime(workload, candidates, random);
    }
    const durations: number[] = [];
    for (let index = 0; index < 20_000; index++) {
      const started = performance.now();
      chooseRuntime(workload, candidates, random);
      durations.push(performance.now() - started);
    }
    durations.sort((left, right) => left - right);
    const p95 = durations[Math.ceil(durations.length * 0.95) - 1]!;
    expect(p95).toBeLessThan(2);
  });

  it("reconciles and routes sizes 1, 4, 8, 16, and 32 without a fixed-four assumption", async () => {
    for (const size of [1, 4, 8, 16, 32]) {
      const root = await mkdtemp(path.join(os.tmpdir(), `ghostprov-dynamic-${size}-`));
      roots.push(root);
      const cookieFile = path.join(root, "cookies.json");
      const configuredIds = Array.from({ length: size }, (_, index) => id(index + 1));
      const config = resolveMultiSandboxConfig({
        GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef",
        PRISM_COOKIE_FILE: cookieFile,
        PRISM_PROJECT_IDS: configuredIds.join(","),
        GP_PRISM_SESSION: "authenticated",
        GP_MAX_PROJECTS: "32",
        GP_PROVISION_CONCURRENCY: "32",
        GP_CANDIDATE: "1",
      }, {
        totalBytes: 64 * 1024 ** 3,
        freeBytes: 52 * 1024 ** 3,
        rssBytes: 256 * 1024 ** 2,
        heapBytes: 64 * 1024 ** 2,
        externalBytes: 16 * 1024 ** 2,
        logicalProcessors: 16,
      });
      const manifest = new FleetManifestStore({
        file: path.join(root, "fleet.json"),
        identityFingerprint: FINGERPRINT,
      });
      const projects = new Projects();
      projects.remote.push(...configuredIds.map((projectId, index) => ({
        projectId,
        title: `Configured project ${index + 1}`,
        owner: "owner",
        deleted: false,
        createdAt: "2026-08-22T10:00:00.000Z",
      })));
      const factory = new Factory();
      const registry = new SandboxRegistry();
      const fleet = new ProjectFleetManager({
        config,
        identity: { kind: "authenticated", fingerprint: FINGERPRINT },
        projects,
        manifest,
        registry,
        runtimeFactory: factory,
      });

      const first = await fleet.reconcile();
      const second = await fleet.reconcile();

      expect(first.ready).toBe(size);
      expect(second.ready).toBe(size);
      expect(registry.runtimes()).toHaveLength(size);
      expect(factory.created).toHaveLength(size);
      expect(projects.created).toHaveLength(0);
      expect(projects.deleted).toEqual([]);

      const probe = {
        sample: () => ({
          totalBytes: 64 * 1024 ** 3,
          freeBytes: 52 * 1024 ** 3,
          rssBytes: 256 * 1024 ** 2,
          heapBytes: 64 * 1024 ** 2,
          externalBytes: 16 * 1024 ** 2,
          logicalProcessors: 16,
        }),
      };
      const router = new MultiSandboxRouter({
        registry,
        governor: new ResourceGovernor({ probe }),
      });
      const handles = await Promise.all(Array.from({ length: size * 4 }, (_, index) => (
        router.startRelay({ model: "gpt-5.6-sol", input: `${size}:${index}` }, {
          clientId: `client-${index % 8}`,
        })
      )));
      const finals = await Promise.all(handles.map(handle => handle.final));
      expect(finals).toHaveLength(size * 4);
      expect(new Set(finals.map(final => final.id)).size).toBe(size * 4);
      await router.close();
      await fleet.shutdown("test-complete");
    }
  }, 300_000);
});
