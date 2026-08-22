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
  async readFile(name: string): Promise<string | null> { return this.files.get(name) ?? null; }
  async writeFile(name: string, content: string): Promise<void> {
    this.files.set(name, content);
    this.writes.push({ name, content });
  }
}

class FakeGateway {
  renders = 0;
  statuses: Array<Record<string, unknown> | null> = [];
  async readFiles(): Promise<Map<string, Uint8Array>> {
    const status = this.statuses.shift() ?? null;
    return status
      ? new Map([["gp_status.json", new TextEncoder().encode(JSON.stringify(status))]])
      : new Map();
  }
  async triggerRender(): Promise<void> { this.renders++; }
}

const ASSETS = {
  daemonFile: "gp_daemon.py",
  daemonSource: "DAEMON_BUILD = 'v-test'\n",
  launcherFile: ".latexmkrc",
  launcherSource: "launch daemon",
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
    const gateway = new FakeGateway();
    gateway.statuses.push(status({ ts: clock.now() / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS });

    const descriptor = await controller.ensureReady();

    expect(descriptor).toMatchObject({ build: "v-test", pid: 123, capacity: 30 });
    expect(ydoc.writes).toEqual([]);
    expect(gateway.renders).toBe(0);
  });

  it("writes only changed assets, triggers once, and accepts matching status", async () => {
    const clock = new FakeClock();
    const ydoc = new FakeYDoc();
    ydoc.files.set(ASSETS.daemonFile, "old daemon");
    ydoc.files.set(ASSETS.launcherFile, ASSETS.launcherSource);
    const gateway = new FakeGateway();
    gateway.statuses.push(null, status({ ts: (clock.now() + 500) / 1000 }));
    const controller = new DaemonController({ ydoc, gateway, clock, assets: ASSETS, pollMs: 500 });

    const descriptor = await controller.ensureReady();

    expect(descriptor.capacity).toBe(30);
    expect(ydoc.writes).toEqual([{ name: ASSETS.daemonFile, content: ASSETS.daemonSource }]);
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
