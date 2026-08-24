import { afterEach, describe, expect, it } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { WebSocket } from "ws";
import {
  GhostProvHostSupervisor,
  supervisorRestartDelayMs,
} from "../src/host/supervisor";

const supervisors: GhostProvHostSupervisor[] = [];
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.allSettled(supervisors.splice(0).map(value => value.close()));
  await Promise.allSettled(temporaryDirectories.splice(0).map(value => rm(value, {
    recursive: true,
    force: true,
  })));
});

describe("GhostProv host supervisor", () => {
  it("uses bounded restart backoff", () => {
    expect(supervisorRestartDelayMs(1, { baseMs: 100, maxMs: 1_000, jitter: 0 })).toBe(75);
    expect(supervisorRestartDelayMs(10, { baseMs: 100, maxMs: 1_000, jitter: 1 })).toBe(1_000);
    expect(() => supervisorRestartDelayMs(0)).toThrow("positive integer");
  });

  it("keeps the public listener alive across graceful upgrades and worker crashes", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-"));
    temporaryDirectories.push(directory);
    const statusFile = path.join(directory, "supervisor.json");
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      readyTimeoutMs: 10_000,
      gracefulShutdownMs: 5_000,
      requestWaitMs: 10_000,
      restartBaseMs: 10,
      restartMaxMs: 50,
      stableResetMs: 100,
      sampleIntervalMs: 100,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile,
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    supervisors.push(supervisor);
    await supervisor.start();
    const port = supervisor.snapshot().externalPort!;
    const root = `http://127.0.0.1:${port}`;
    expect(await (await fetch(`${root}/generation`)).json()).toMatchObject({ generation: 1 });

    const firstSocket = new WebSocket(`ws://127.0.0.1:${port}/v1/responses`);
    const firstSocketMessage = new Promise<any>((resolve, reject) => {
      firstSocket.once("message", data => resolve(JSON.parse(data.toString())));
      firstSocket.once("error", reject);
    });
    const firstSocketClosed = new Promise<number>(resolve => {
      firstSocket.once("close", code => resolve(code));
    });
    await expect(firstSocketMessage).resolves.toMatchObject({ generation: 1 });

    const slow = fetch(`${root}/slow?ms=100`).then(response => response.json());
    await Bun.sleep(20);
    const restart = supervisor.requestRestart("test-upgrade");
    const queued = fetch(`${root}/generation`).then(response => response.json());
    await expect(slow).resolves.toMatchObject({ generation: 1, delayed: true });
    expect(await firstSocketClosed).toBe(1012);
    await expect(queued).resolves.toMatchObject({ generation: 2 });
    await restart;

    const secondSocket = new WebSocket(`ws://127.0.0.1:${port}/v1/responses`);
    await expect(new Promise<any>((resolve, reject) => {
      secondSocket.once("message", data => resolve(JSON.parse(data.toString())));
      secondSocket.once("error", reject);
    })).resolves.toMatchObject({ generation: 2 });
    secondSocket.close();

    await expect(fetch(`${root}/crash`).then(response => response.json()))
      .resolves.toMatchObject({ generation: 2, crashing: true });
    let recovered: any = null;
    for (let attempt = 0; attempt < 100; attempt++) {
      try {
        const candidate = await fetch(`${root}/generation`).then(response => response.json());
        if ((candidate as any).generation === 3) {
          recovered = candidate;
          break;
        }
      } catch {}
      await Bun.sleep(20);
    }
    expect(recovered).toMatchObject({ generation: 3 });

    const diagnostic = await (await fetch(`${root}/__supervisor`)).json() as any;
    expect(diagnostic).toMatchObject({
      schema: 1,
      state: "ready",
      generation: 3,
      restarts: 2,
      upgrades: 1,
      forwardedTotal: expect.any(Number),
    });
    await Bun.sleep(20);
    const persisted = JSON.parse(await readFile(statusFile, "utf8"));
    expect(persisted).toMatchObject({ schema: 1, generation: 3 });
    const history = await readFile(path.join(directory, "supervisor.jsonl"), "utf8");
    expect(history).toContain("host.supervisor.worker_ready");
    expect(history).toContain("host.supervisor.restart_scheduled");
    expect(history).toContain("host.supervisor.sample");
  }, 30_000);
});
