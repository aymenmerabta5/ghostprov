import { afterEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm, utimes, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { WebSocket } from "ws";
import {
  GhostProvHostSupervisor,
  supervisorRestartDelayMs,
} from "../src/host/supervisor";

const supervisors: GhostProvHostSupervisor[] = [];
const temporaryDirectories: string[] = [];
const API_KEY = "test-supervisor-key-0123456789abcdef-0123456789";

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
      apiKey: API_KEY,
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

    const firstSocket = new WebSocket(`ws://127.0.0.1:${port}/v1/responses`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
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
    const queued = fetch(`${root}/v1/models`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    }).then(response => response.json());
    await expect(slow).resolves.toMatchObject({ generation: 1, delayed: true });
    expect(await firstSocketClosed).toBe(1012);
    await expect(queued).resolves.toMatchObject({ generation: 2 });
    await restart;

    const secondSocket = new WebSocket(`ws://127.0.0.1:${port}/v1/responses`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    });
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

    const diagnostic = await (await fetch(`${root}/__supervisor`, {
      headers: { authorization: `Bearer ${API_KEY}` },
    })).json() as any;
    expect(diagnostic).toMatchObject({
      schema: 1,
      state: "ready",
      generation: 3,
      restarts: 2,
      upgrades: 1,
      forwardedTotal: expect.any(Number),
    });
    // Closing joins the asynchronous diagnostic pumps; a fixed sleep can read
    // the append-only history before a heavily loaded filesystem flushes it.
    await supervisor.close();
    const persisted = JSON.parse(await readFile(statusFile, "utf8"));
    expect(persisted).toMatchObject({ schema: 1, generation: 3 });
    const history = await readFile(path.join(directory, "supervisor.jsonl"), "utf8");
    expect(history).toContain("host.supervisor.worker_ready");
    expect(history).toContain("host.supervisor.restart_scheduled");
    expect(history).toContain("host.supervisor.sample");
  }, 30_000);

  it("accepts only unique content-bound requests for its exact supervisor instance", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-control-identity-"));
    temporaryDirectories.push(directory);
    const controlFile = path.join(directory, "upgrade.request");
    const startedAt = new Date().toISOString();
    const instanceNonce = randomUUID();
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      readyTimeoutMs: 10_000,
      gracefulShutdownMs: 5_000,
      requestWaitMs: 10_000,
      restartBaseMs: 10,
      restartMaxMs: 50,
      stableResetMs: 100,
      sampleIntervalMs: 1_000,
      controlFile,
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
      startedAt,
      instanceNonce,
    });
    supervisors.push(supervisor);
    await supervisor.start();

    const request = (requestId: string, nonce: string) => JSON.stringify({
      schema: 1,
      requestId,
      action: "upgrade",
      requestedAt: new Date().toISOString(),
      target: { supervisorPid: process.pid, startedAt, instanceNonce: nonce },
    });
    const waitForGeneration = async (generation: number) => {
      for (let attempt = 0; attempt < 200; attempt++) {
        const snapshot = supervisor.snapshot();
        if (snapshot.generation >= generation && snapshot.state === "ready") return;
        await Bun.sleep(20);
      }
      throw new Error(`Supervisor did not reach generation ${generation}`);
    };

    // A late request targeting the previous process generation is ignored even
    // with a newer filesystem timestamp.
    await writeFile(controlFile, request(randomUUID(), randomUUID()), "utf8");
    const future = new Date(Date.now() + 60_000);
    await utimes(controlFile, future, future);
    await Bun.sleep(1_100);
    expect(supervisor.snapshot().generation).toBe(1);

    // A valid request is accepted even when its mtime moves backwards.
    const acceptedId = randomUUID();
    const accepted = request(acceptedId, instanceNonce);
    await writeFile(controlFile, accepted, "utf8");
    const old = new Date(1_000);
    await utimes(controlFile, old, old);
    await waitForGeneration(2);

    // Replaying the same request identity is rejected regardless of mtime.
    await writeFile(controlFile, accepted, "utf8");
    await utimes(controlFile, old, old);
    await Bun.sleep(1_100);
    expect(supervisor.snapshot().generation).toBe(2);

    // A different request with the same coarse/backward mtime is still seen.
    await writeFile(controlFile, request(randomUUID(), instanceNonce), "utf8");
    await utimes(controlFile, old, old);
    await waitForGeneration(3);
  }, 30_000);

  it("bounds restart HTTP and WebSocket queues by count and bytes without status-write amplification", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-bounded-queue-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      workerEnv: { GP_TEST_SUPERVISOR_RESTART_READY_DELAY_MS: "1500" },
      readyTimeoutMs: 10_000,
      gracefulShutdownMs: 5_000,
      requestWaitMs: 10_000,
      restartBaseMs: 10,
      restartMaxMs: 50,
      stableResetMs: 100,
      sampleIntervalMs: 10_000,
      maxQueuedRequests: 4,
      maxQueuedBytes: 4_096,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    supervisors.push(supervisor);
    await supervisor.start();
    const root = `http://127.0.0.1:${supervisor.snapshot().externalPort}`;
    const statusWritesBefore = supervisor.snapshot().statusWritesTotal;
    const restarting = supervisor.requestRestart("bounded-queue-test");
    const oversized = await fetch(`${root}/v1/models`, {
      method: "POST",
      headers: { authorization: `Bearer ${API_KEY}` },
      body: "x".repeat(8_192),
    });
    expect(oversized.status).toBe(503);
    expect(await oversized.text()).toContain("host_queue_full");
    expect(supervisor.snapshot()).toMatchObject({ queuedRequests: 0, queuedBytes: 0 });
    const accepted = Array.from({ length: 4 }, () => fetch(`${root}/v1/models`, {
      headers: {
        authorization: `Bearer ${API_KEY}`,
        "x-queue-padding": "x".repeat(128),
      },
    }));
    for (let attempt = 0; attempt < 100 && supervisor.snapshot().queuedRequests < 4; attempt++) {
      await Bun.sleep(10);
    }
    expect(supervisor.snapshot()).toMatchObject({ queuedRequests: 4 });
    expect(supervisor.snapshot().queuedBytes).toBeGreaterThan(0);
    expect(supervisor.snapshot().queuedBytes).toBeLessThanOrEqual(4_096);

    const overflow = await Promise.all(Array.from({ length: 40 }, async () => {
      const response = await fetch(`${root}/v1/models`, {
        headers: {
          authorization: `Bearer ${API_KEY}`,
          "x-private-canary": "must-not-leak",
        },
      });
      return { status: response.status, body: await response.text() };
    }));
    expect(overflow.every(value => value.status === 503)).toBe(true);
    expect(overflow.every(value => value.body.includes("host_queue_full"))).toBe(true);
    expect(overflow.every(value => !value.body.includes("must-not-leak"))).toBe(true);

    const websocket503 = await new Promise<{ status: number; body: string }>((resolve, reject) => {
      const websocket = new WebSocket(
        `ws://127.0.0.1:${supervisor.snapshot().externalPort}/v1/responses`,
        { headers: { authorization: `Bearer ${API_KEY}` } },
      );
      websocket.once("unexpected-response", (_request, response) => {
        let body = "";
        response.setEncoding("utf8");
        response.on("data", chunk => { body += chunk; });
        response.once("end", () => resolve({ status: response.statusCode ?? 0, body }));
      });
      websocket.once("open", () => reject(new Error("Overflow WebSocket unexpectedly opened")));
      websocket.once("error", () => undefined);
    });
    expect(websocket503.status).toBe(503);
    expect(websocket503.body).toContain("host_queue_full");

    await restarting;
    const acceptedResponses = await Promise.all(accepted);
    expect(acceptedResponses.every(response => response.status === 200)).toBe(true);
    expect(await Promise.all(acceptedResponses.map(response => response.json())))
      .toEqual(Array.from({ length: 4 }, () => expect.objectContaining({ generation: 2 })));

    const snapshot = supervisor.snapshot();
    expect(snapshot.queuedRequests).toBe(0);
    expect(snapshot.queuedBytes).toBe(0);
    expect(snapshot.queuedRequestsPeak).toBeLessThanOrEqual(4);
    expect(snapshot.queuedBytesPeak).toBeLessThanOrEqual(4_096);
    expect(snapshot.queueRejectedTotal).toBeGreaterThanOrEqual(42);
    expect(snapshot.statusWritesTotal - statusWritesBefore).toBeLessThan(20);
  }, 30_000);

  it.each([200, 503])(
    "never trusts a foreign %i listener as worker readiness or forwards its queued body",
    async foreignStatus => {
      const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-port-collision-"));
      temporaryDirectories.push(directory);
      const received: Array<{ url: string; body: string }> = [];
      const foreign = createServer((request, response) => {
        let body = "";
        request.setEncoding("utf8");
        request.on("data", chunk => { body += chunk; });
        request.once("end", () => {
          received.push({ url: request.url ?? "", body });
          response.writeHead(foreignStatus, { "content-type": "application/json" });
          response.end(JSON.stringify({ ok: foreignStatus === 200 }));
        });
      });
      await new Promise<void>((resolve, reject) => {
        foreign.once("error", reject);
        foreign.listen(0, "127.0.0.1", resolve);
      });
      const address = foreign.address();
      if (!address || typeof address === "string") throw new Error("Foreign test listener has no port");
      const supervisor = new GhostProvHostSupervisor({
        externalPort: 0,
        apiKey: API_KEY,
        cwd: process.cwd(),
        workerEntry: path.resolve("tests/support/fake-supervised-silent-worker.ts"),
        allocatePort: async () => address.port,
        readyTimeoutMs: 1_000,
        gracefulShutdownMs: 500,
        requestWaitMs: 700,
        restartBaseMs: 10,
        restartMaxMs: 50,
        stableResetMs: 100,
        sampleIntervalMs: 10_000,
        maxQueuedRequests: 4,
        maxQueuedBytes: 16_384,
        controlFile: path.join(directory, "upgrade.request"),
        stopFile: path.join(directory, "stop.request"),
        statusFile: path.join(directory, "supervisor.json"),
        logFile: path.join(directory, "supervisor.jsonl"),
      });
      supervisors.push(supervisor);
      try {
        const starting = supervisor.start().then(
          () => null,
          error => error as Error,
        );
        for (let attempt = 0; attempt < 200 && supervisor.snapshot().externalPort === null; attempt++) {
          await Bun.sleep(5);
        }
        const publicPort = supervisor.snapshot().externalPort;
        expect(publicPort).not.toBeNull();
        const secretBody = "queued-body-must-never-reach-foreign-listener";
        const response = await fetch(`http://127.0.0.1:${publicPort}/v1/responses`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${API_KEY}`,
          },
          body: secretBody,
        });
        expect(response.status).toBe(503);
        expect(await response.text()).toContain("host_restarting");
        expect(await starting).toBeInstanceOf(Error);
        expect(received).toEqual([]);
        const snapshot = supervisor.snapshot();
        expect(snapshot.state).not.toBe("ready");
        expect(snapshot).toMatchObject({
          forwardedTotal: 0,
          queuedRequests: 0,
          queuedBytes: 0,
        });
      } finally {
        await supervisor.close();
        await new Promise<void>(resolve => foreign.close(() => resolve()));
      }
    },
    15_000,
  );

  it("never revives a child condemned by the readiness timeout", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-condemned-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-condemned-worker.ts"),
      readyTimeoutMs: 100,
      gracefulShutdownMs: 1_000,
      requestWaitMs: 500,
      restartBaseMs: 500,
      restartMaxMs: 500,
      stableResetMs: 1_000,
      sampleIntervalMs: 10_000,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    supervisors.push(supervisor);
    const starting = supervisor.start().then(() => null, error => error as Error);
    for (let attempt = 0; attempt < 100 && supervisor.snapshot().externalPort === null; attempt++) {
      await Bun.sleep(5);
    }
    const response = await fetch(
      `http://127.0.0.1:${supervisor.snapshot().externalPort}/v1/models`,
      { headers: { authorization: `Bearer ${API_KEY}` } },
    );
    expect(response.status).toBe(503);
    expect(await response.text()).toContain("host_restarting");
    expect(await starting).toBeInstanceOf(Error);
    expect(supervisor.snapshot()).toMatchObject({
      internalPort: null,
      forwardedTotal: 0,
      queuedRequests: 0,
      queuedBytes: 0,
    });
  }, 15_000);

  it("never selects a pre-ready child condemned by a restart request", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-restart-condemned-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-condemned-worker.ts"),
      readyTimeoutMs: 2_000,
      gracefulShutdownMs: 500,
      requestWaitMs: 2_000,
      restartBaseMs: 10,
      restartMaxMs: 50,
      stableResetMs: 1_000,
      sampleIntervalMs: 10_000,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    supervisors.push(supervisor);
    const starting = supervisor.start();
    for (let attempt = 0; attempt < 100 && supervisor.snapshot().workerPid === null; attempt++) {
      await Bun.sleep(5);
    }
    const restarting = supervisor.requestRestart("condemn-before-ready");
    const response = await fetch(
      `http://127.0.0.1:${supervisor.snapshot().externalPort}/v1/models`,
      { headers: { authorization: `Bearer ${API_KEY}` } },
    );
    await Promise.all([starting, restarting]);
    expect(await response.json()).toMatchObject({ generation: 2, condemned: true });
    expect(supervisor.snapshot()).toMatchObject({
      state: "ready",
      generation: 2,
      forwardedTotal: 1,
    });
  }, 15_000);

  it("settles an aborted upstream IncomingMessage exactly once after headers", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-upstream-abort-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: process.cwd(),
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      readyTimeoutMs: 10_000,
      gracefulShutdownMs: 1_000,
      requestWaitMs: 1_000,
      sampleIntervalMs: 10_000,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    supervisors.push(supervisor);
    await supervisor.start();
    const before = supervisor.snapshot().proxyErrorsTotal;
    await expect(fetch(
      `http://127.0.0.1:${supervisor.snapshot().externalPort}/abort-after-headers`,
    ).then(response => response.text())).rejects.toBeDefined();
    for (let attempt = 0; attempt < 100 && supervisor.snapshot().activeProxyRequests !== 0; attempt++) {
      await Bun.sleep(5);
    }
    expect(supervisor.snapshot().activeProxyRequests).toBe(0);
    expect(supervisor.snapshot().proxyErrorsTotal - before).toBe(1);
  }, 15_000);

  it("closes cleanly when startup is cancelled before the listener or run loop owns resources", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-pre-listen-close-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: directory,
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      readyTimeoutMs: 1_000,
      gracefulShutdownMs: 500,
      requestWaitMs: 1_000,
      sampleIntervalMs: 10_000,
    });
    supervisors.push(supervisor);
    const starting = supervisor.start().then(() => null, error => error as Error);
    await supervisor.close();
    expect(await starting).toBeInstanceOf(Error);
    expect(supervisor.snapshot()).toMatchObject({
      state: "stopped",
      workerPid: null,
      externalPort: null,
      forcedStops: 0,
    });
  });

  it("surfaces a run-loop allocator failure immediately and leaves no zombie listener or child", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-run-failure-"));
    temporaryDirectories.push(directory);
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_KEY,
      cwd: directory,
      workerEntry: path.resolve("tests/support/fake-supervised-worker.ts"),
      allocatePort: async () => { throw new Error("allocator failed"); },
      readyTimeoutMs: 2_000,
      gracefulShutdownMs: 500,
      requestWaitMs: 2_000,
      sampleIntervalMs: 10_000,
    });
    supervisors.push(supervisor);
    await expect(supervisor.start()).rejects.toThrow("allocator failed");
    const releasedPort = supervisor.snapshot().externalPort;
    expect(releasedPort).not.toBeNull();
    await supervisor.close();
    expect(supervisor.snapshot()).toMatchObject({
      state: "stopped",
      workerPid: null,
      internalPort: null,
    });
    const probe = createServer();
    await new Promise<void>((resolve, reject) => {
      probe.once("error", reject);
      probe.listen(releasedPort!, "127.0.0.1", resolve);
    });
    await new Promise<void>(resolve => probe.close(() => resolve()));
  });
});
