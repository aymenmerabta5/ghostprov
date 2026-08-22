import { describe, expect, it } from "bun:test";
import { DaemonController, DaemonControllerError } from "../src/sandbox/daemon-controller";
import type { Clock } from "../src/core/ports";

class FakeClock implements Clock {
  value = 1_000_000;
  now(): number { return this.value; }
  async sleep(ms: number): Promise<void> { this.value += ms; }
  async waitUntil(deadline: number): Promise<void> { this.value = Math.max(this.value, deadline); }
}

class FakeYDoc {
  files = new Map<string, string>();
  writes: Array<{ name: string; content: string }> = [];
  ensuredBatches: string[][] = [];
  renderState = {
    clientStateVector: "current-project-state",
    clientDeleteSetUpdate: "current-project-delete-set",
  };
  async readFile(name: string): Promise<string | null> { return this.files.get(name) ?? null; }
  async writeFile(name: string, content: string): Promise<void> {
    this.files.set(name, content);
    this.writes.push({ name, content });
  }
  async ensureFiles(names: readonly string[]): Promise<void> {
    this.ensuredBatches.push([...names]);
    for (const name of names) if (!this.files.has(name)) this.files.set(name, "");
  }
  async renderSyncState() { return this.renderState; }
}

class FakeGateway {
  renders = 0;
  syncs = 0;
  statusReads = 0;
  renderRequests: unknown[] = [];
  statuses: Array<Record<string, unknown> | Error | null> = [];
  async readFiles(): Promise<Map<string, Uint8Array>> {
    this.statusReads++;
    const status = this.statuses.shift() ?? null;
    if (status instanceof Error) throw status;
    return status
      ? new Map([["gp_status.json", new TextEncoder().encode(JSON.stringify(status))]])
      : new Map();
  }
  async triggerRender(request?: unknown): Promise<void> {
    this.renders++;
    this.renderRequests.push(request);
  }
  async syncWorkspace(): Promise<void> { this.syncs++; }
}

const ASSETS = {
  daemonFile: "gp_daemon.py",
  daemonSource: "DAEMON_BUILD = 'v-test'\n",
  launcherFile: ".latexmkrc",
  launcherSource: "launch daemon",
  mainFile: "main.tex",
  mainSource: "\\documentclass{article}\n\\begin{document}GhostProv\\end{document}\n",
  trackedFiles: ["gp_status.json", "job_s1.json", "resp_s1.json"],
  expectedBuild: "v-test",
  requiredProtocols: ["segmented-v6.3-embedded-commit"],
};

function status(overrides: Record<string, unknown> = {}) {
  return {
    daemon_build: "v-test",
    pid: 123,
    ts: 1_000,
    slots: 30,
    job_protocols: ["single-v5", "segmented-v6.3-embedded-commit"],
    active: {},
    ...overrides,
  };
}

describe("DaemonController", () => {
  it("returns a matching fresh daemon without rewriting or rendering", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    ydoc.files.set(ASSETS.daemonFile, ASSETS.daemonSource);
    ydoc.files.set(ASSETS.launcherFile, ASSETS.launcherSource);
    ydoc.files.set(ASSETS.mainFile, ASSETS.mainSource);
    const gateway = new FakeGateway();
    gateway.statuses.push(status({ ts: clock.now() / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS });

    const descriptor = await controller.ensureReady();

    expect(descriptor).toMatchObject({ build: "v-test", pid: 123, capacity: 30 });
    expect(ydoc.writes).toEqual([]);
    expect(gateway.renders).toBe(0);
    expect(gateway.syncs).toBe(1);
  });

  it("deduplicates burst readiness checks behind a short verified cache", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    ydoc.files.set(ASSETS.daemonFile, ASSETS.daemonSource);
    ydoc.files.set(ASSETS.launcherFile, ASSETS.launcherSource);
    ydoc.files.set(ASSETS.mainFile, ASSETS.mainSource);
    const gateway = new FakeGateway();
    gateway.statuses.push(status({ ts: clock.now() / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS });

    const first = await controller.ensureReady();
    const burst = await Promise.all(Array.from({ length: 20 }, () => controller.ensureReady()));

    expect(burst.every(descriptor => descriptor.pid === first.pid)).toBe(true);
    expect(gateway.statusReads).toBe(1);
    expect(gateway.syncs).toBe(1);
    expect(gateway.renders).toBe(0);
  });

  it("writes only changed assets, triggers once, and accepts matching status", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    ydoc.files.set(ASSETS.daemonFile, "old daemon");
    ydoc.files.set(ASSETS.launcherFile, ASSETS.launcherSource);
    ydoc.files.set(ASSETS.mainFile, ASSETS.mainSource);
    const gateway = new FakeGateway();
    gateway.statuses.push(null, status({ ts: (clock.now() + 500) / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS, pollMs: 500 });

    const descriptor = await controller.ensureReady();

    expect(descriptor.capacity).toBe(30);
    expect(ydoc.writes).toEqual([{ name: ASSETS.daemonFile, content: ASSETS.daemonSource }]);
    expect(gateway.renders).toBe(1);
  });

  it("seeds a missing render document without overwriting an existing one", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    const gateway = new FakeGateway();
    gateway.statuses.push(null, status({ ts: (clock.now() + 500) / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS, pollMs: 500 });

    await controller.ensureReady();

    expect(ydoc.writes).toContainEqual({ name: ASSETS.mainFile, content: ASSETS.mainSource });
    expect(ydoc.ensuredBatches).toContainEqual(ASSETS.trackedFiles);
    expect(ASSETS.trackedFiles.every(name => ydoc.files.has(name))).toBe(true);
    expect(gateway.renderRequests[0]).toMatchObject({
      mainDocument: ASSETS.mainFile,
      ...ydoc.renderState,
    });

    const existing = new FakeYDoc();
    existing.files.set(ASSETS.mainFile, "user document");
    const secondGateway = new FakeGateway();
    secondGateway.statuses.push(null, status({ ts: (clock.now() + 500) / 1000 }));
    await new DaemonController({ ydoc: existing, gateway: secondGateway, clock, assets: ASSETS, pollMs: 500 }).ensureReady();
    expect(existing.files.get(ASSETS.mainFile)).toBe("user document");
  });

  it("treats an empty-workspace ZIP 500 as an absent initial daemon status", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    const gateway = new FakeGateway();
    gateway.statuses.push(
      Object.assign(new Error("ZIP returned 500"), { status: 500, operation: "zip" }),
      status({ ts: (clock.now() + 500) / 1000 }),
    );
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS, pollMs: 500 });

    const descriptor = await controller.ensureReady();

    expect(descriptor.capacity).toBe(30);
    expect(gateway.renders).toBe(1);
  });

  it("rejects a stale or incompatible daemon until the deadline", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    ydoc.files.set(ASSETS.daemonFile, ASSETS.daemonSource);
    ydoc.files.set(ASSETS.launcherFile, ASSETS.launcherSource);
    const gateway = new FakeGateway();
    gateway.statuses.push(
      status({ daemon_build: "old", ts: clock.now() / 1000 }),
      status({ daemon_build: "old", ts: (clock.now() + 500) / 1000 }),
      status({ daemon_build: "old", ts: (clock.now() + 1_000) / 1000 }),
    );
    const controller = new DaemonController({
      ydoc,
      gateway,
      clock,
      assets: ASSETS,
      pollMs: 500,
      timeoutMs: 1_000,
    });

    await expect(controller.ensureReady()).rejects.toBeInstanceOf(DaemonControllerError);
    expect(gateway.renders).toBe(1);
  });
});
