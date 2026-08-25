import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import {
  runGhostProvMain,
  type GhostProvWorkerProcess,
} from "../src/server";
import type { BrowserlessSystem } from "../src/server/system";

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
    // Handlers remain installed until the one shared cleanup actually ends.
    expect(worker.listenerCount("SIGTERM")).toBeGreaterThan(0);
    releaseClose();
    await running;

    const stopped = worker.sent.filter((item: any) => item?.type === "ghostprov.stopped");
    expect(stopped).toHaveLength(1);
    expect(stopped[0]).toMatchObject({ exitCode: 1 });
    expect(worker.exitCode).toBe(1);
    expect(value.closes()).toBe(1);
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

  it("rejects supervised candidate port 8787 before create or listen", async () => {
    const worker = new FakeWorkerProcess();
    let creates = 0;
    await expect(runGhostProvMain({
      GP_SUPERVISED_CHILD: "1",
      GP_CANDIDATE: "1",
      PORT: "8787",
    }, {
      process: worker,
      createSystem: async () => {
        creates++;
        return fakeSystem().system;
      },
    })).rejects.toThrow(/protected port 8787/i);
    expect(creates).toBe(0);
    expect(worker.exitCode).toBe(1);
  });
});
