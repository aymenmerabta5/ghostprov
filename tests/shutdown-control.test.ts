import { afterEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { existsSync, writeFileSync } from "node:fs";
import { mkdtemp, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { FileLease, FileLeaseOwnershipError } from "../src/core/file-lease";
import {
  MAX_SHUTDOWN_CONTROL_BYTES,
  MAX_SHUTDOWN_WAIT_MS,
  openGhostProvShutdownControl,
  parseGhostProvShutdownDescriptor,
  parseGhostProvShutdownRequest,
  parseGhostProvShutdownResult,
  requestGhostProvShutdown,
  type GhostProvShutdownControl,
  type GhostProvShutdownRequest,
} from "../src/host/shutdown-control";

const roots: string[] = [];
const controls: GhostProvShutdownControl[] = [];
const probe = async (pid: number) => ({ state: "alive" as const, identity: `offline-${pid}`, startedAtMs: 1 });
const target = { pid: process.pid, startedAt: "2026-09-12T00:00:00.000Z", instanceNonce: randomUUID() };
const request = { schema: 1, target, requestId: randomUUID(), requestedAt: target.startedAt } as const;
const result = { schema: 1, target, requestId: request.requestId, outcome: "clean", completedAt: target.startedAt } as const;
const descriptor = { schema: 1, mode: "direct", target, timeoutMs: 1_000 } as const;

afterEach(async () => {
  await Promise.allSettled(controls.splice(0).map(control => control.finish(false)));
  for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function fixture(onShutdown: () => Promise<void> = async () => {}) {
  const root = await mkdtemp(path.join(tmpdir(), "ghostprov-shutdown-control-"));
  roots.push(root);
  const control = await openGhostProvShutdownControl({ cwd: root, mode: "direct", timeoutMs: 1_000,
    onShutdown, autoPoll: false, processProbe: probe });
  controls.push(control);
  return { root, control, file: (name: string) => path.join(root, ".ghostprov", name) };
}

async function waitForRequest(file: string): Promise<GhostProvShutdownRequest> {
  for (let attempt = 0; attempt < 100; attempt++) {
    const raw = await readFile(file, "utf8").catch(() => null);
    const value = raw === null ? null : parseGhostProvShutdownRequest(raw);
    if (value) return value;
    await Bun.sleep(5);
  }
  throw new Error("Offline request was not written");
}

describe("bounded shutdown protocol", () => {
  it("accepts only complete exact schemas and canonical identities", () => {
    expect(parseGhostProvShutdownDescriptor(JSON.stringify(descriptor))).toEqual(descriptor);
    expect(parseGhostProvShutdownRequest(JSON.stringify(request))).toEqual(request);
    expect(parseGhostProvShutdownResult(JSON.stringify(result))).toEqual(result);
    for (const mutation of [
      { ...descriptor, timeoutMs: 0 }, { ...descriptor, timeoutMs: MAX_SHUTDOWN_WAIT_MS + 1 },
      { ...descriptor, mode: "other" }, { ...descriptor, file: "/arbitrary" },
      { ...descriptor, target: { ...target, pid: 0 } },
      { ...descriptor, target: { ...target, instanceNonce: `${target.instanceNonce}\n` } },
      { ...descriptor, target: { ...target, startedAt: "2026-09-12" } },
      { ...descriptor, target: { ...target, extra: true } },
    ]) expect(parseGhostProvShutdownDescriptor(JSON.stringify(mutation))).toBeNull();
    expect(parseGhostProvShutdownRequest(JSON.stringify({ ...request, receiptFile: "/arbitrary" }))).toBeNull();
    expect(parseGhostProvShutdownRequest(JSON.stringify({ ...request, requestId: `${request.requestId}\n` }))).toBeNull();
    expect(parseGhostProvShutdownResult(JSON.stringify({ ...result, outcome: true }))).toBeNull();
    expect(parseGhostProvShutdownResult(JSON.stringify({ ...result, success: true }))).toBeNull();
    expect(parseGhostProvShutdownResult(" ".repeat(MAX_SHUTDOWN_CONTROL_BYTES + 1))).toBeNull();
    expect(parseGhostProvShutdownRequest("null")).toBeNull();
  });

  it("waits for actual cleanup before returning an exact clean receipt", async () => {
    const cleanup = Promise.withResolvers<void>();
    let calls = 0;
    const value = await fixture(async () => { calls++; await cleanup.promise; });
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe });
    const accepted = await waitForRequest(value.file("shutdown.request.json"));
    await value.control.poll();
    expect(calls).toBe(1);
    expect(await readFile(value.file("shutdown.result.json"), "utf8").catch(() => null)).toBeNull();
    cleanup.resolve();
    const receipt = await pending;
    expect(receipt).toMatchObject({ outcome: "clean", target: value.control.descriptor.target, requestId: accepted.requestId });
    await value.control.shutdown();
    expect(calls).toBe(1);
  });

  it("publishes failure and rejects the waiter when cleanup throws", async () => {
    const value = await fixture(async () => { throw new Error("offline unproven cleanup"); });
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe });
    const rejected = pending.catch(error => error);
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.poll();
    expect((await rejected).message).toContain("incomplete cleanup");
    expect(JSON.parse(await readFile(value.file("shutdown.result.json"), "utf8"))).toMatchObject({ outcome: "failed" });
  });

  it("joins an externally started shutdown without running the callback", async () => {
    let called = false;
    const value = await fixture(async () => { called = true; });
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe });
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.finish(true);
    expect((await pending).outcome).toBe("clean");
    expect(called).toBe(false);
  });

  it("holds the startup gate through the post-cleanup action", async () => {
    const value = await fixture();
    const actionEntered = Promise.withResolvers<void>();
    const releaseAction = Promise.withResolvers<void>();
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe,
      afterCleanup: async receipt => {
        expect(receipt.outcome).toBe("clean");
        actionEntered.resolve();
        await releaseAction.promise;
      } });
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.finish(true);
    await actionEntered.promise;
    try {
      await expect(openGhostProvShutdownControl({ cwd: value.root, mode: "supervised", timeoutMs: 100,
        onShutdown: async () => {}, autoPoll: false, processProbe: probe })).rejects.toThrow("lease");
      expect(JSON.parse(await readFile(value.file("shutdown-control.json"), "utf8"))).toEqual(value.control.descriptor);
    } finally { releaseAction.resolve(); }
    expect((await pending).outcome).toBe("clean");
    const successor = await openGhostProvShutdownControl({ cwd: value.root, mode: "supervised", timeoutMs: 100,
      onShutdown: async () => {}, autoPoll: false, processProbe: probe });
    controls.push(successor);
    expect(successor.descriptor.target.instanceNonce).not.toBe(value.control.descriptor.target.instanceNonce);
  });

  it("never invokes the post-cleanup action without proof and propagates its failure", async () => {
    const value = await fixture();
    let calls = 0;
    await expect(requestGhostProvShutdown(value.root, { processProbe: probe, timeoutMs: 100,
      afterCleanup: async () => { calls++; } })).rejects.toThrow("timed out");
    expect(calls).toBe(0);
    // Remove the old request so the test waits for the new, exact request ID.
    await rm(value.file("shutdown.request.json"));
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe,
      afterCleanup: async () => { calls++; throw new Error("offline post-cleanup action failed"); } });
    const outcome = pending.catch(error => error);
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.finish(true);
    expect((await outcome).message).toContain("post-cleanup action failed");
    expect(calls).toBe(1);
  });

  it.each([false, true])("rejects a displaced startup gate before the post-cleanup action (replacement=%s)", async replace => {
    const value = await fixture();
    let actions = 0;
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe,
      afterCleanup: async () => { actions++; } }).catch(error => error);
    await waitForRequest(value.file("shutdown.request.json"));
    const gate = value.file("shutdown-request.lock");
    await rename(gate, `${gate}.displaced`);
    const replacement = new FileLease({ file: gate, staleAfterMs: 120_000, processProbe: probe });
    try {
      if (replace) await replacement.acquire({ timeoutMs: 1 });
      await value.control.finish(true);
      expect(await pending).toBeInstanceOf(FileLeaseOwnershipError);
      expect(actions).toBe(0);
      if (replace) await replacement.acquire({ timeoutMs: 1 });
      else expect(existsSync(gate)).toBe(false);
    } finally { await replacement.release(); }
  });

  it("rejects a clean receipt that arrives after the monotonic deadline", async () => {
    const value = await fixture();
    let calls = 0;
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe, timeoutMs: 1_000,
      monotonicNow: () => existsSync(value.file("shutdown.result.json")) ? 1_001 : 0,
      afterCleanup: async () => { calls++; } });
    const outcome = pending.catch(error => error);
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.finish(true);
    expect((await outcome).message).toContain("after the shutdown deadline");
    expect(calls).toBe(0);
  });

  it("rejects old receipts and never treats a live PID as cleanup proof", async () => {
    const value = await fixture();
    await writeFile(value.file("shutdown.result.json"), JSON.stringify({ ...result, target: value.control.descriptor.target }), { mode: 0o600 });
    await expect(requestGhostProvShutdown(value.root, { processProbe: probe, timeoutMs: 80 }))
      .rejects.toThrow("timed out");
  });

  it("rejects process death without a correlated receipt", async () => {
    const value = await fixture();
    let checks = 0;
    await expect(requestGhostProvShutdown(value.root, { processProbe: probe, isProcessAlive: () => ++checks === 1 }))
      .rejects.toThrow("exited without");
  });

  it("accepts a proven result even when its exact owner has since exited", async () => {
    const value = await fixture();
    let live = true;
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe, isProcessAlive: () => live });
    await waitForRequest(value.file("shutdown.request.json"));
    await value.control.finish(true);
    live = false;
    expect((await pending).outcome).toBe("clean");
  });

  it("ignores requests for another process generation", async () => {
    let calls = 0;
    const value = await fixture(async () => { calls++; });
    await writeFile(value.file("shutdown.request.json"), JSON.stringify({ ...request, requestedAt: new Date().toISOString(),
      target: { ...value.control.descriptor.target, instanceNonce: randomUUID() } }), { mode: 0o600 });
    await value.control.poll();
    expect(calls).toBe(0);
    expect(await readFile(value.file("shutdown.result.json"), "utf8").catch(() => null)).toBeNull();
  });

  it("pins one instance and never rewrites a request to its successor", async () => {
    const value = await fixture();
    let successorPublished = false;
    const pending = requestGhostProvShutdown(value.root, {
      processProbe: probe,
      isProcessAlive: () => {
        if (!successorPublished && existsSync(value.file("shutdown.request.json"))) {
          // This callback runs between descriptor reads. Publish the successor
          // here so the fixture tests pinned identity, rather than racing an
          // in-place write against the separate file-open identity guard.
          writeFileSync(value.file("shutdown-control.json"), JSON.stringify({ ...value.control.descriptor,
            target: { ...value.control.descriptor.target, instanceNonce: randomUUID() } }), { mode: 0o600 });
          successorPublished = true;
        }
        return true;
      },
    });
    const rejected = pending.catch(error => error);
    const accepted = await waitForRequest(value.file("shutdown.request.json"));
    expect((await rejected).message).toContain("instance changed");
    expect(successorPublished).toBe(true);
    expect(JSON.parse(await readFile(value.file("shutdown.request.json"), "utf8"))).toEqual(accepted);
    await expect(value.control.finish(true)).rejects.toThrow("owner changed");
    expect(await readFile(value.file("shutdown.result.json"), "utf8").catch(() => null)).toBeNull();
  });

  it("fails closed when the result destination cannot be written safely", async () => {
    const value = await fixture();
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe, timeoutMs: 150 });
    const rejected = pending.catch(error => error);
    await waitForRequest(value.file("shutdown.request.json"));
    await writeFile(value.file("shutdown.result.json"), "x".repeat(MAX_SHUTDOWN_CONTROL_BYTES + 1), { mode: 0o600 });
    await expect(value.control.finish(true)).rejects.toThrow("bounded private");
    expect(await rejected).toBeInstanceOf(Error);
  });

  it("rejects oversized descriptors before allocating their contents", async () => {
    const value = await fixture();
    await writeFile(value.file("shutdown-control.json"), "x".repeat(MAX_SHUTDOWN_CONTROL_BYTES + 1), { mode: 0o600 });
    await expect(requestGhostProvShutdown(value.root, { processProbe: probe })).rejects.toThrow("bounded private");
  });

  it("does not allow another owner to replace a live descriptor", async () => {
    const value = await fixture();
    await expect(openGhostProvShutdownControl({ cwd: value.root, mode: "direct", timeoutMs: 100,
      onShutdown: async () => {}, autoPoll: false, processProbe: probe })).rejects.toThrow("lease");
    expect(JSON.parse(await readFile(value.file("shutdown-control.json"), "utf8"))).toEqual(value.control.descriptor);
  });

  it("cancels a waiter without reporting success or issuing another request", async () => {
    const value = await fixture();
    const controller = new AbortController();
    const pending = requestGhostProvShutdown(value.root, { processProbe: probe, signal: controller.signal });
    const rejected = pending.catch(error => error);
    const accepted = await waitForRequest(value.file("shutdown.request.json"));
    controller.abort(new Error("cancelled"));
    expect((await rejected).message).toContain("cancelled");
    expect(JSON.parse(await readFile(value.file("shutdown.request.json"), "utf8"))).toEqual(accepted);
  });

  it("rejects a linked control directory", async () => {
    const parent = await mkdtemp(path.join(tmpdir(), "ghostprov-shutdown-link-"));
    const outside = await mkdtemp(path.join(tmpdir(), "ghostprov-shutdown-outside-"));
    roots.push(parent, outside);
    await symlink(outside, path.join(parent, ".ghostprov"), process.platform === "win32" ? "junction" : "dir");
    await expect(openGhostProvShutdownControl({ cwd: parent, mode: "direct", timeoutMs: 100,
      onShutdown: async () => {}, processProbe: probe })).rejects.toThrow("real workspace");
  });
});
