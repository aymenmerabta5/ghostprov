import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import {
  runGhostProvMain,
  type GhostProvWorkerProcess,
} from "../src/server";
import type { BrowserlessSystem } from "../src/server/system";
import { openGhostProvShutdownControl, requestGhostProvShutdown } from "../src/host/shutdown-control";

class FakeWorkerProcess extends EventEmitter implements GhostProvWorkerProcess {
  readonly pid = 4242;
  connected = true;
  exitCode: number | undefined;
  readonly sent: unknown[] = [];

  send(message: unknown): boolean {
    this.sent.push(message);
    return true;
  }

  disconnect(): void {
    this.connected = false;
    this.emit("disconnect");
  }
}

function fakeSystem(options: {
  start?: () => Promise<void>;
  close?: () => Promise<void>;
} = {}) {
  let starts = 0;
  let closes = 0;
  const system = {
    config: { port: 12345, projectIds: ["project-1"] },
    supportsVerifiedShutdown: true,
    async start() {
      starts++;
      await options.start?.();
    },
    async close() {
      closes++;
      await options.close?.();
    },
  } as unknown as BrowserlessSystem;
  return { system, starts: () => starts, closes: () => closes };
}

async function waitForStopped(worker: FakeWorkerProcess): Promise<any> {
  for (let attempt = 0; attempt < 200; attempt++) {
    const stopped = worker.sent.find((value: any) => value?.type === "ghostprov.stopped");
    if (stopped) return stopped;
    await Bun.sleep(5);
  }
  throw new Error("Worker did not publish stopped IPC");
}

describe("server main lifecycle", () => {
  it("installs persistent shutdown/fatal/IPC handlers before system creation", async () => {
    const worker = new FakeWorkerProcess();
    const value = fakeSystem();
    await runGhostProvMain({ GP_SUPERVISED_CHILD: "1" }, {
      process: worker,
      createSystem: async () => {
        for (const event of [
          "SIGINT", "SIGTERM", "SIGHUP", "message", "disconnect",
          "uncaughtException", "unhandledRejection",
        ]) expect(worker.listenerCount(event)).toBeGreaterThan(0);
        return value.system;
      },
    });
    expect(value.starts()).toBe(1);
    expect(worker.sent).toContainEqual(expect.objectContaining({ type: "ghostprov.ready" }));

    worker.emit("SIGTERM");
    const stopped = await waitForStopped(worker);
    expect(stopped).toMatchObject({ type: "ghostprov.stopped", exitCode: 0 });
    expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 0, success: true });
    expect(value.closes()).toBe(1);
    expect(worker.listenerCount("SIGTERM")).toBe(0);
  });

  it("aborts creation and merges duplicate IPC, signal, and fatal shutdown into one cleanup", async () => {
    const worker = new FakeWorkerProcess();
    let resolveCreation!: (system: BrowserlessSystem) => void;
    const creation = new Promise<BrowserlessSystem>(resolve => { resolveCreation = resolve; });
    let observedSignal: AbortSignal | null = null;
    let releaseClose!: () => void;
    const closeGate = new Promise<void>(resolve => { releaseClose = resolve; });
    const value = fakeSystem({ close: () => closeGate });
    const running = runGhostProvMain({ GP_SUPERVISED_CHILD: "1" }, {
      process: worker,
      createSystem: async (_env, signal) => {
        observedSignal = signal;
        return creation;
      },
    });

    worker.emit("SIGTERM");
    worker.emit("message", { type: "ghostprov.shutdown", reason: "fatal-ipc", exitCode: 1 });
    worker.emit("uncaughtException", new Error("private fatal detail"));
    expect((observedSignal as AbortSignal | null)?.aborted).toBe(true);
    resolveCreation(value.system);
    await Bun.sleep(0);
    expect(value.starts()).toBe(0);
    expect(value.closes()).toBe(1);
    // Construction cannot hold process handlers or stopped IPC open forever.
    expect(worker.listenerCount("SIGTERM")).toBe(0);
    releaseClose();
    await running;

    const stopped = worker.sent.filter((item: any) => item?.type === "ghostprov.stopped");
    expect(stopped).toHaveLength(1);
    expect(stopped[0]).toMatchObject({ exitCode: 1 });
    expect(worker.exitCode).toBe(1);
    expect(value.closes()).toBe(1);
  });

  it("does not wait forever for non-cooperative creation and closes a late system", async () => {
    const worker = new FakeWorkerProcess();
    let resolveCreation!: (system: BrowserlessSystem) => void;
    const creation = new Promise<BrowserlessSystem>(resolve => { resolveCreation = resolve; });
    let observedSignal: AbortSignal | null = null;
    const value = fakeSystem();
    const running = runGhostProvMain({ GP_SUPERVISED_CHILD: "1" }, {
      process: worker,
      createSystem: async (_env, signal) => {
        observedSignal = signal;
        return creation;
      },
    });

    for (let attempt = 0; attempt < 100 && !observedSignal; attempt++) await Bun.sleep(0);
    worker.emit("SIGTERM");
    const stoppedBeforeLateCreation = await Promise.race([
      waitForStopped(worker),
      Bun.sleep(100).then(() => null),
    ]);
    resolveCreation(value.system);
    await running;
    for (let attempt = 0; attempt < 100 && value.closes() === 0; attempt++) await Bun.sleep(0);

    expect((observedSignal as AbortSignal | null)?.aborted).toBe(true);
    expect(stoppedBeforeLateCreation).toMatchObject({ type: "ghostprov.stopped", exitCode: 0 });
    expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 0, success: false });
    expect(value.starts()).toBe(0);
    expect(value.closes()).toBe(1);
  });

  it("marks the actual process failed when late system cleanup rejects", async () => {
    const worker = new FakeWorkerProcess();
    let resolveCreation!: (system: BrowserlessSystem) => void;
    const creation = new Promise<BrowserlessSystem>(resolve => { resolveCreation = resolve; });
    const value = fakeSystem({ close: async () => { throw new Error("late cleanup failed"); } });
    const running = runGhostProvMain({ GP_SUPERVISED_CHILD: "1" }, {
      process: worker,
      createSystem: async () => creation,
    });

    worker.emit("SIGTERM");
    await running;
    expect(worker.exitCode).toBe(0);
    resolveCreation(value.system);
    for (let attempt = 0; attempt < 100 && worker.exitCode === 0; attempt++) await Bun.sleep(0);

    expect(value.closes()).toBe(1);
    expect(worker.exitCode).toBe(1);
  });

  it("is import-safe from another entrypoint whose filename ends in server.ts", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "ghostprov-import-safe-"));
    const entrypoint = path.join(directory, "consumer-server.ts");
    await writeFile(
      entrypoint,
      `await import(${JSON.stringify(pathToFileURL(path.resolve(import.meta.dir, "../src/server.ts")).href)});\n`,
      "utf8",
    );
    try {
      const child = Bun.spawn([process.execPath, "--no-env-file", entrypoint], {
        cwd: directory,
        stdout: "pipe",
        stderr: "pipe",
      });
      const [exitCode, stderr] = await Promise.all([
        child.exited,
        new Response(child.stderr).text(),
      ]);
      expect(exitCode).toBe(0);
      expect(stderr).toBe("");
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("closes a partial system exactly once when start/listen fails", async () => {
    const worker = new FakeWorkerProcess();
    const value = fakeSystem({
      start: async () => { throw new Error("listen failed"); },
    });
    await expect(runGhostProvMain({}, {
      process: worker,
      createSystem: async () => value.system,
    })).rejects.toThrow("listen failed");

    expect(value.starts()).toBe(1);
    expect(value.closes()).toBe(1);
    expect(worker.sent.filter((item: any) => item?.type === "ghostprov.stopped"))
      .toHaveLength(1);
    expect(worker.exitCode).toBe(1);
  });

  it("aborts an in-progress system start and joins duplicate upgrade/stop signals", async () => {
    const worker = new FakeWorkerProcess();
    let startSignal: AbortSignal | null = null;
    const value = fakeSystem({
      start: () => new Promise<void>((_resolve, reject) => {
        if (!startSignal) return reject(new Error("missing startup signal"));
        const fail = () => reject(startSignal!.reason);
        if (startSignal.aborted) fail();
        else startSignal.addEventListener("abort", fail, { once: true });
      }),
    });
    const running = runGhostProvMain({ GP_SUPERVISED_CHILD: "1" }, {
      process: worker,
      createSystem: async (_env, signal) => {
        startSignal = signal;
        return value.system;
      },
    });
    for (let attempt = 0; attempt < 100 && value.starts() === 0; attempt++) await Bun.sleep(0);
    worker.emit("SIGHUP");
    worker.emit("SIGTERM");
    await running;

    expect(value.starts()).toBe(1);
    expect(value.closes()).toBe(1);
    expect(worker.sent.filter((item: any) => item?.type === "ghostprov.stopped"))
      .toEqual([expect.objectContaining({ exitCode: 75 })]);
    expect(worker.exitCode).toBe(75);
  });

  it("rejects supervised candidate port 18787 before create or listen", async () => {
    const worker = new FakeWorkerProcess();
    let creates = 0;
    await expect(runGhostProvMain({
      GP_SUPERVISED_CHILD: "1",
      GP_CANDIDATE: "1",
      PORT: "18787",
    }, {
      process: worker,
      createSystem: async () => {
        creates++;
        return fakeSystem().system;
      },
    })).rejects.toThrow(/protected service port 18787/i);
    expect(creates).toBe(0);
    expect(worker.exitCode).toBe(1);
  });

  it.each([false, true])("direct control reports actual cleanup result (failure=%s)", async fail => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "ghostprov-direct-stop-"));
    const worker = new FakeWorkerProcess();
    const gate = Promise.withResolvers<void>();
    const entered = Promise.withResolvers<void>();
    const value = fakeSystem({ close: async () => {
      entered.resolve();
      await gate.promise;
      if (fail) throw new Error("offline cleanup failed");
    } });
    const processProbe = async () => ({ state: "alive" as const });
    try {
      await runGhostProvMain({}, {
        process: worker, cwd: directory, createSystem: async () => value.system,
        createShutdownControl: options => openGhostProvShutdownControl({ ...options, pollIntervalMs: 5, processProbe }),
      });
      let settled = false;
      const request = requestGhostProvShutdown(directory, { timeoutMs: 5_000, processProbe })
        .then(result => { settled = true; return result; }, error => { settled = true; throw error; });
      const outcome = request.then(result => result.outcome, () => "rejected");
      await entered.promise;
      expect(settled).toBe(false);
      gate.resolve();
      expect(await outcome).toBe(fail ? "rejected" : "clean");
      expect(worker.exitCode).toBe(fail ? 1 : 0);
      expect(value.closes()).toBe(1);
      expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 0, success: !fail });
    } finally {
      gate.resolve();
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("never approves poweroff while system construction remains unresolved", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "ghostprov-pending-start-stop-"));
    const worker = new FakeWorkerProcess();
    const creation = Promise.withResolvers<BrowserlessSystem>();
    const entered = Promise.withResolvers<void>();
    const value = fakeSystem();
    const processProbe = async () => ({ state: "alive" as const });
    const running = runGhostProvMain({}, {
      process: worker, cwd: directory,
      createSystem: async () => { entered.resolve(); return creation.promise; },
      createShutdownControl: options => openGhostProvShutdownControl({ ...options, pollIntervalMs: 5, processProbe }),
    });
    try {
      await entered.promise;
      await expect(requestGhostProvShutdown(directory, { timeoutMs: 5_000, processProbe }))
        .rejects.toThrow("incomplete cleanup");
      expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 0, success: false });
      creation.resolve(value.system);
      await running;
      for (let attempt = 0; attempt < 100 && value.closes() === 0; attempt++) await Bun.sleep(0);
      expect(value.closes()).toBe(1);
    } finally {
      creation.resolve(value.system);
      await running;
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("does not issue affirmative cleanup proof for a mode without durable restart evidence", async () => {
    const worker = new FakeWorkerProcess();
    const value = fakeSystem();
    const system = { ...value.system, supportsVerifiedShutdown: false };
    await runGhostProvMain({ GP_SUPERVISED_CHILD: "1", GP_SUPERVISOR_GENERATION: "3" }, {
      process: worker, createSystem: async () => system,
    });
    worker.emit("SIGTERM");
    await waitForStopped(worker);
    expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 3, success: false });
    expect(value.closes()).toBe(1);
  });

  it("does not certify an early stop before the fleet has been loaded", async () => {
    const worker = new FakeWorkerProcess();
    const outcomes: boolean[] = [];
    let creates = 0;
    await runGhostProvMain({}, {
      process: worker,
      createSystem: async () => { creates++; return fakeSystem().system; },
      createShutdownControl: async () => {
        worker.emit("SIGINT");
        return { finish: async (success: boolean) => { outcomes.push(success); } } as never;
      },
    });
    expect(creates).toBe(0);
    expect(outcomes).toEqual([false]);
    expect(worker.sent).toContainEqual({ type: "ghostprov.cleanup", pid: worker.pid, generation: 0, success: false });
  });
});
