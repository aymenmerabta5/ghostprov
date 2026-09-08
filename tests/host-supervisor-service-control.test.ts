import { describe, expect, it } from "bun:test";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { requestSupervisorShutdown } from "../scripts/request_shutdown";
import { requestSupervisorUpgrade } from "../scripts/request_upgrade";
import { readSupervisorStatus, waitForFreshSupervisorReady } from "../scripts/show_supervisor_status";
import { MAX_SUPERVISOR_LIFECYCLE_MS, supervisorOptions } from "../scripts/supervise";
import {
  confirmRunningSupervisorControlDescriptor,
  readRunningSupervisorControlDescriptor,
  SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
  writeSupervisorControlDescriptor,
} from "../scripts/supervisor_control";
import { GhostProvHostSupervisor } from "../src/host/supervisor";

const workspace = path.resolve(import.meta.dir, "..");
const API_ENV = Object.freeze({ GHOSTPROV_API_KEY: "test-ghostprov-key-0123456789abcdef" });

async function source(relativePath: string): Promise<string> {
  return readFile(path.join(workspace, relativePath), "utf8");
}

describe("host supervisor service controls", () => {
  it("uses exact module entrypoint guards for every lifecycle script", async () => {
    for (const file of [
      "scripts/supervise.ts",
      "scripts/request_upgrade.ts",
      "scripts/request_shutdown.ts",
      "scripts/show_supervisor_status.ts",
    ]) {
      const script = await source(file);
      expect(script).toContain("import.meta.main");
      expect(script).not.toContain("process.argv[1]?.endsWith");
    }
  });

  it("freezes the worker environment at supervisor startup", () => {
    const environment: NodeJS.ProcessEnv = {
      ...API_ENV,
      GP_V2_SIDECAR_REVISION: "r1",
      GP_V2_SIDECAR_PROJECT_REVISIONS: "project-a=r2",
    };
    const options = supervisorOptions(environment);

    environment.GP_V2_SIDECAR_REVISION = "r2";
    environment.GP_V2_SIDECAR_PROJECT_REVISIONS = "project-a=r1";

    expect(options.workerEnv).not.toBe(environment);
    expect(options.workerEnv.GP_V2_SIDECAR_REVISION).toBe("r1");
    expect(options.workerEnv.GP_V2_SIDECAR_PROJECT_REVISIONS).toBe("project-a=r2");
    expect(options.workerEnv.GHOSTPROV_API_KEY).toBe(API_ENV.GHOSTPROV_API_KEY);
    expect(options.maxQueuedRequests).toBe(256);
    expect(options.maxQueuedBytes).toBe(64 * 1024 * 1024);
    expect(supervisorOptions({ ...API_ENV, PORT: "0" }).externalPort).toBe(0);
    expect(supervisorOptions({ ...API_ENV, GP_CANDIDATE: "1" }).externalPort).toBe(18_888);
    expect(() => supervisorOptions({ ...API_ENV, PORT: "18787", GP_CANDIDATE: "1" }))
      .toThrow(/protected service port 18787/i);
    for (const value of ["true", " true ", "false", "2"]) {
      expect(() => supervisorOptions({ ...API_ENV, GP_CANDIDATE: value }))
        .toThrow("GP_CANDIDATE must be 0 or 1");
    }
    expect(() => supervisorOptions({
      ...API_ENV,
      GP_SUPERVISOR_GRACE_MS: String(MAX_SUPERVISOR_LIFECYCLE_MS + 1),
    })).toThrow("no greater than");
    expect(() => supervisorOptions({ ...API_ENV, GP_SUPERVISOR_QUEUE_MAX_REQUESTS: "0" }))
      .toThrow("positive integer");
  });

  it("does not auto-load an edited .env during a rolling worker restart", async () => {
    expect(process.env.GP_TEST_SUPERVISOR_ENV_AUTOLOAD).toBeUndefined();
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-env-"));
    const supervisor = new GhostProvHostSupervisor({
      externalPort: 0,
      apiKey: API_ENV.GHOSTPROV_API_KEY,
      workerEnv: { ...API_ENV },
      cwd: directory,
      workerEntry: path.resolve(workspace, "tests/support/fake-supervised-worker.ts"),
      readyTimeoutMs: 10_000,
      gracefulShutdownMs: 5_000,
      requestWaitMs: 10_000,
      restartBaseMs: 10,
      restartMaxMs: 50,
      stableResetMs: 100,
      sampleIntervalMs: 1_000,
      controlFile: path.join(directory, "upgrade.request"),
      stopFile: path.join(directory, "stop.request"),
      statusFile: path.join(directory, "supervisor.json"),
      logFile: path.join(directory, "supervisor.jsonl"),
    });
    try {
      await supervisor.start();
      const root = `http://127.0.0.1:${supervisor.snapshot().externalPort}`;
      expect(await fetch(root).then(response => response.json())).toMatchObject({
        generation: 1,
        testSupervisorEnv: null,
      });

      await writeFile(
        path.join(directory, ".env"),
        "GP_TEST_SUPERVISOR_ENV_AUTOLOAD=must-not-load\n",
        "utf8",
      );
      await supervisor.requestRestart("test-config-snapshot");
      expect(await fetch(root).then(response => response.json())).toMatchObject({
        generation: 2,
        testSupervisorEnv: null,
      });
    } finally {
      await supervisor.close();
      expect(supervisor.snapshot()).toMatchObject({
        state: "stopped",
        shutdownClean: true,
        forcedStops: 0,
      });
      await rm(directory, { recursive: true, force: true });
    }
  }, 30_000);

  it("owns pre-ready POSIX signals and Windows control shutdown without orphaning the child", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-pre-ready-"));
    const statusFile = path.join(directory, "supervisor.json");
    const child = spawn(process.execPath, [
      "--no-env-file",
      path.join(workspace, "scripts/supervise.ts"),
    ], {
      cwd: directory,
      env: {
        ...process.env,
        ...API_ENV,
        PORT: "0",
        GP_SUPERVISOR_WORKER_ENTRY: path.join(workspace, "tests/support/fake-supervised-worker.ts"),
        GP_TEST_SUPERVISOR_INITIAL_READY_DELAY_MS: "10000",
        GP_SUPERVISOR_READY_TIMEOUT_MS: "10000",
        GP_SUPERVISOR_GRACE_MS: "1000",
        GP_SUPERVISOR_REQUEST_WAIT_MS: "10000",
        GP_SUPERVISOR_CONTROL_FILE: path.join(directory, "upgrade.request"),
        GP_SUPERVISOR_STOP_FILE: path.join(directory, "stop.request"),
        GP_SUPERVISOR_STATUS_FILE: statusFile,
        GP_SUPERVISOR_LOG_FILE: path.join(directory, "supervisor.jsonl"),
      },
      stdio: "ignore",
      windowsHide: true,
    });
    try {
      const descriptorFile = path.join(directory, ".ghostprov", "supervisor-control.json");
      for (let attempt = 0; attempt < 200 && !await Bun.file(descriptorFile).exists(); attempt++) {
        await Bun.sleep(10);
      }
      expect(await Bun.file(descriptorFile).exists()).toBe(true);
      if (process.platform === "win32") {
        let running = await readRunningSupervisorControlDescriptor(directory);
        for (let attempt = 0; attempt < 200 && !running; attempt++) {
          await Bun.sleep(10);
          running = await readRunningSupervisorControlDescriptor(directory);
        }
        expect(running).not.toBeNull();
        await requestSupervisorShutdown({}, directory);
      } else {
        expect(child.kill("SIGTERM")).toBe(true);
      }
      const exit = await Promise.race([
        new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
          child.once("exit", (code, signal) => resolve({ code, signal }));
        }),
        Bun.sleep(5_000).then(() => { throw new Error("Pre-ready supervisor did not exit"); }),
      ]);
      expect(exit).toEqual({ code: 0, signal: null });
      const status = JSON.parse(await readFile(statusFile, "utf8"));
      expect(status).toMatchObject({
        state: "stopped",
        workerPid: null,
        shutdownClean: expect.any(Boolean),
        forcedStops: 0,
      });
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill();
      await rm(directory, { recursive: true, force: true });
    }
  }, 15_000);

  it("closes supervision and leaves no child after startup failure", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-start-failure-"));
    const statusFile = path.join(directory, "supervisor.json");
    const child = spawn(process.execPath, [
      "--no-env-file",
      path.join(workspace, "scripts/supervise.ts"),
    ], {
      cwd: directory,
      env: {
        ...process.env,
        ...API_ENV,
        PORT: "0",
        GP_SUPERVISOR_WORKER_ENTRY: path.join(directory, "missing-worker.ts"),
        // Exercise the missing-module exit, not a race against process startup.
        GP_SUPERVISOR_READY_TIMEOUT_MS: "1000",
        GP_SUPERVISOR_GRACE_MS: "1000",
        GP_SUPERVISOR_REQUEST_WAIT_MS: "100",
        GP_SUPERVISOR_CONTROL_FILE: path.join(directory, "upgrade.request"),
        GP_SUPERVISOR_STOP_FILE: path.join(directory, "stop.request"),
        GP_SUPERVISOR_STATUS_FILE: statusFile,
        GP_SUPERVISOR_LOG_FILE: path.join(directory, "supervisor.jsonl"),
      },
      stdio: "ignore",
      windowsHide: true,
    });
    try {
      const exit = await Promise.race([
        new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
          child.once("exit", (code, signal) => resolve({ code, signal }));
        }),
        Bun.sleep(5_000).then(() => { throw new Error("Failed supervisor did not exit"); }),
      ]);
      expect(exit.signal).toBeNull();
      expect(exit.code).not.toBe(0);
      const status = JSON.parse(await readFile(statusFile, "utf8"));
      expect(status).toMatchObject({
        state: "stopped",
        workerPid: null,
        forcedStops: 0,
      });
    } finally {
      if (child.exitCode === null && child.signalCode === null) child.kill();
      await rm(directory, { recursive: true, force: true });
    }
  }, 15_000);

  it("routes control requests through the running supervisor descriptor, not edited env paths", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-control-"));
    const previousCwd = process.cwd();
    const controlFile = path.join(directory, "old-config", "upgrade.request");
    const stopFile = path.join(directory, "old-config", "stop.request");
    const statusFile = path.join(directory, "old-config", "supervisor.json");
    try {
      process.chdir(directory);
      const instanceNonce = randomUUID();
      const startedAt = new Date().toISOString();
      await writeSupervisorControlDescriptor({
        schema: SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        controlFile,
        stopFile,
        statusFile,
        readyTimeoutMs: 900_000,
        gracefulShutdownMs: 1_200_000,
      });
      await mkdir(path.dirname(statusFile), { recursive: true });
      await writeFile(statusFile, JSON.stringify({
        schema: 1,
        state: "ready",
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        workerPid: process.pid,
        lastReadyAt: startedAt,
      }), "utf8");

      expect(await readRunningSupervisorControlDescriptor()).toMatchObject({
        supervisorPid: process.pid,
        controlFile,
        stopFile,
      });
      expect(await requestSupervisorUpgrade({
        GP_SUPERVISOR_CONTROL_FILE: path.join(directory, "edited-config", "upgrade.request"),
      })).toBe(controlFile);
      expect(await requestSupervisorShutdown({
        GP_SUPERVISOR_STOP_FILE: path.join(directory, "edited-config", "stop.request"),
      })).toBe(stopFile);
      expect(JSON.parse(await readFile(controlFile, "utf8"))).toMatchObject({
        schema: 1,
        requestId: expect.any(String),
        action: "upgrade",
        requestedAt: expect.any(String),
        target: { supervisorPid: process.pid, startedAt, instanceNonce },
      });
      expect(JSON.parse(await readFile(stopFile, "utf8"))).toMatchObject({
        schema: 1,
        requestId: expect.any(String),
        action: "shutdown",
        requestedAt: expect.any(String),
        target: { supervisorPid: process.pid, startedAt, instanceNonce },
      });
    } finally {
      process.chdir(previousCwd);
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("refuses unbound environment-path requests when no live descriptor exists", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-unbound-"));
    const fallback = path.join(directory, "edited", "upgrade.request");
    try {
      await expect(requestSupervisorUpgrade({
        GP_SUPERVISOR_CONTROL_FILE: fallback,
      }, directory)).rejects.toThrow(/no live.*descriptor/i);
      await expect(Bun.file(fallback).exists()).resolves.toBe(false);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("accepts status only when PID, start time, and random instance nonce match", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-status-"));
    const statusFile = path.join(directory, "supervisor.json");
    const startedAt = new Date().toISOString();
    const instanceNonce = randomUUID();
    try {
      await writeSupervisorControlDescriptor({
        schema: SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        controlFile: path.join(directory, "upgrade.request"),
        stopFile: path.join(directory, "stop.request"),
        statusFile,
        readyTimeoutMs: 1_000,
        gracefulShutdownMs: 1_000,
      }, directory);
      const status = JSON.stringify({
        schema: 1,
        state: "ready",
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        workerPid: process.pid,
        lastReadyAt: startedAt,
      });
      await writeFile(statusFile, status, "utf8");

      await expect(readSupervisorStatus({}, directory)).resolves.toBe(status);
      await expect(waitForFreshSupervisorReady({
        cwd: directory,
        notBefore: startedAt,
        previousInstanceNonce: randomUUID(),
        timeoutMs: 100,
        pollMs: 10,
      })).resolves.toMatchObject({
        descriptor: { instanceNonce },
        status: { state: "ready", workerPid: process.pid },
      });

      await writeFile(statusFile, JSON.stringify({
        ...JSON.parse(status),
        instanceNonce: randomUUID(),
      }), "utf8");
      await expect(readSupervisorStatus({}, directory)).resolves.toBeNull();
      await expect(waitForFreshSupervisorReady({
        cwd: directory,
        notBefore: startedAt,
        previousInstanceNonce: instanceNonce,
        timeoutMs: 30,
        pollMs: 10,
      })).rejects.toThrow(/before timeout/i);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("detects descriptor replacement across the request-write confirmation boundary", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "ghostprov-supervisor-toctou-"));
    const statusFile = path.join(directory, "supervisor.json");
    const startedAt = new Date().toISOString();
    const writeInstance = async (instanceNonce: string, suffix: string) => {
      const descriptor = {
        schema: SUPERVISOR_CONTROL_DESCRIPTOR_SCHEMA,
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        controlFile: path.join(directory, suffix, "upgrade.request"),
        stopFile: path.join(directory, suffix, "stop.request"),
        statusFile,
        readyTimeoutMs: 1_000,
        gracefulShutdownMs: 1_000,
      } as const;
      await writeSupervisorControlDescriptor(descriptor, directory);
      await writeFile(statusFile, JSON.stringify({
        schema: 1,
        state: "ready",
        supervisorPid: process.pid,
        startedAt,
        instanceNonce,
        workerPid: process.pid,
        lastReadyAt: startedAt,
      }), "utf8");
      return descriptor;
    };
    try {
      const first = await writeInstance(randomUUID(), "first");
      await expect(confirmRunningSupervisorControlDescriptor(first, directory))
        .resolves.toMatchObject({ instanceNonce: first.instanceNonce });

      await writeInstance(randomUUID(), "replacement");
      await expect(confirmRunningSupervisorControlDescriptor(first, directory)).resolves.toBeNull();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("keeps Windows rolling restart separate from a full configuration restart", async () => {
    const script = await source("scripts/ghostprov_service.ps1");
    expect(script).toContain('"restart", "config-restart", "status"');

    const rolling = script.slice(script.indexOf('  "restart" {'), script.indexOf('  "config-restart" {'));
    expect(rolling).toContain('scripts/request_upgrade.ts');
    expect(rolling).not.toContain("Start-ScheduledTask");
    expect(rolling).toContain('$Task.State -ne "Running"');
    expect(rolling).toContain('$Status.state -ne "ready"');

    const full = script.slice(script.indexOf('  "config-restart" {'), script.indexOf('  "status" {'));
    const stop = full.indexOf("Request-GracefulStop");
    const stopped = full.indexOf("Wait-TaskStopped");
    const start = full.indexOf("Start-ScheduledTask");
    const ready = full.indexOf("Wait-SupervisorReady");
    expect(stop).toBeGreaterThanOrEqual(0);
    expect(stopped).toBeGreaterThan(stop);
    expect(start).toBeGreaterThan(stopped);
    expect(ready).toBeGreaterThan(start);
    expect(full).toContain("Read-ControlDescriptor");
    expect(full).toContain("Get-TimeoutSeconds $OldDescriptor.gracefulShutdownMs");
    expect(full).toContain("PreviousInstanceNonce");
    expect(script).toContain("$LastReadyAt -ge $DescriptorStartedAt");
    expect(script).toContain("$Status.instanceNonce -eq [string]$Descriptor.instanceNonce");
  });

  it("maps Linux rolling reload and full configuration restart to different systemd operations", async () => {
    const script = await source("scripts/ghostprov_service.sh");
    expect(script).toContain("install|uninstall|start|stop|restart|config-restart|status");
    expect(script).toContain("ExecReload=\"$BUN_BIN\" scripts/request_upgrade.ts");
    expect(script).toContain("ExecStop=\"$BUN_BIN\" scripts/request_shutdown.ts");
    expect(script).toContain("TimeoutStartSec=1860s");
    expect(script).toContain("TimeoutStopSec=1860s");
    expect(script).toContain("restart) systemctl --user reload ghostprov.service ;;");
    expect(script).toContain("systemctl --user restart ghostprov.service");
    expect(script).toContain("show_supervisor_status.ts wait-ready");
    expect(script).toContain("OLD_INSTANCE_NONCE");
    expect(script).toContain("1830000");
  });

  it("baselines control content before publishing the supervisor descriptor", async () => {
    const script = await source("scripts/supervise.ts");
    const signal = script.indexOf('process.once("SIGTERM", terminate)');
    const prepare = script.indexOf("await supervisor.prepareControlPlane()");
    const publish = script.indexOf("await writeSupervisorControlDescriptor(");
    const start = script.indexOf("await supervisor.start()");
    expect(signal).toBeGreaterThanOrEqual(0);
    expect(prepare).toBeGreaterThan(signal);
    expect(prepare).toBeGreaterThanOrEqual(0);
    expect(publish).toBeGreaterThan(prepare);
    expect(start).toBeGreaterThan(publish);
  });
});
