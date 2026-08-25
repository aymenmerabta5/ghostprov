import { afterEach, describe, expect, it } from "bun:test";
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  stat,
  utimes,
  writeFile,
} from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import {
  FileLease,
  FileLeaseBusyError,
  FileLeaseOwnershipError,
  type FileLeaseProcessProbe,
} from "../src/core/file-lease";

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function leasePath(): Promise<string> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-lease-"));
  roots.push(root);
  return path.join(root, "account.lock");
}

async function ownerPath(file: string): Promise<string> {
  const entries = await readdir(file);
  const owner = entries.find(value => value.startsWith("owner.") && value.endsWith(".json"));
  if (!owner) throw new Error("lease owner marker is missing");
  return path.join(file, owner);
}

async function ownerRecord(file: string): Promise<any> {
  return JSON.parse(await readFile(await ownerPath(file), "utf8"));
}

async function writeOwner(
  file: string,
  record: {
    pid: number;
    nonce: string;
    processIdentity: string | null;
    processStartedAtMs?: number | null;
  },
): Promise<void> {
  await mkdir(file, { recursive: true });
  await writeFile(path.join(file, `owner.${record.nonce}.json`), JSON.stringify({
    version: 2,
    pid: record.pid,
    nonce: record.nonce,
    platform: process.platform,
    processIdentity: record.processIdentity,
    processStartedAtMs: record.processStartedAtMs ?? null,
    acquiredAtMs: Date.now(),
  }), { mode: 0o600 });
}

const currentIdentity = "test-boot:current-process-start";
const deadPid = 2_147_483_647;
const deterministicProbe: FileLeaseProcessProbe = async pid => (
  pid === process.pid
    ? { state: "alive", identity: currentIdentity, startedAtMs: 1_000 }
    : { state: "dead" }
);

async function waitForReady(stream: ReadableStream<Uint8Array>): Promise<void> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let output = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) throw new Error(`file lease worker exited before readiness: ${output}`);
      output += decoder.decode(value, { stream: true });
      if (output.includes("READY")) return;
    }
  } finally {
    reader.releaseLock();
  }
}

describe("FileLease", () => {
  it("serializes owners and rejects an active competing lease", async () => {
    const file = await leasePath();
    const first = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });
    const second = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });

    await first.acquire();
    await expect(second.acquire({ timeoutMs: 5 })).rejects.toBeInstanceOf(FileLeaseBusyError);
    await first.release();
    await expect(second.acquire({ timeoutMs: 50 })).resolves.toBeUndefined();
    await second.release();
  });

  it("singleflights concurrent acquire calls on one instance", async () => {
    const file = await leasePath();
    const lease = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });

    await Promise.all(Array.from({ length: 16 }, () => lease.acquire({ timeoutMs: 100 })));
    expect((await readdir(file)).filter(value => value.startsWith("owner."))).toHaveLength(1);
    await lease.release();
    await expect(stat(file)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("orders acquire then release then reacquire on one instance", async () => {
    const file = await leasePath();
    const lease = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });
    const first = lease.acquire({ timeoutMs: 100 });
    const release = lease.release();
    const second = lease.acquire({ timeoutMs: 100 });

    await Promise.all([first, release, second]);
    expect((await ownerRecord(file)).processIdentity).toBe(currentIdentity);
    await lease.release();
  });

  it("reclaims a valid dead owner immediately without waiting for stale age", async () => {
    const file = await leasePath();
    await writeOwner(file, {
      pid: deadPid,
      nonce: "dead-owner",
      processIdentity: "old-boot:dead-start",
    });

    const lease = new FileLease({ file, staleAfterMs: 60_000, processProbe: deterministicProbe });
    await expect(lease.acquire({ timeoutMs: 100 })).resolves.toBeUndefined();
    expect((await ownerRecord(file)).nonce).not.toBe("dead-owner");
    await lease.release();
  });

  it("reclaims an empty initialization orphan within the acquire timeout", async () => {
    const file = await leasePath();
    await mkdir(file);

    const lease = new FileLease({ file, staleAfterMs: 60_000, processProbe: deterministicProbe });
    await expect(lease.acquire({ timeoutMs: 2_000 })).resolves.toBeUndefined();
    expect((await ownerRecord(file)).version).toBe(2);
    await lease.release();
  });

  it("reclaims a stale malformed legacy record left by a torn initial write", async () => {
    const file = await leasePath();
    await writeFile(file, "{", { mode: 0o600 });
    const old = new Date(Date.now() - 5_000);
    await utimes(file, old, old);

    const lease = new FileLease({ file, staleAfterMs: 60_000, processProbe: deterministicProbe });
    await expect(lease.acquire({ timeoutMs: 2_000 })).resolves.toBeUndefined();
    expect((await ownerRecord(file)).version).toBe(2);
    await lease.release();
  });

  it("migrates a dead legacy regular-file lock without touching a replacement directory", async () => {
    const file = await leasePath();
    await writeFile(file, JSON.stringify({
      pid: deadPid,
      nonce: "legacy-dead",
      acquiredAtMs: 1,
    }), { mode: 0o600 });

    const lease = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });
    await lease.acquire({ timeoutMs: 100 });
    expect((await stat(file)).isDirectory()).toBe(true);
    expect((await ownerRecord(file)).nonce).not.toBe("legacy-dead");
    await lease.release();
  });

  it("reclaims a reused PID but never steals the same process instance", async () => {
    const reusedFile = await leasePath();
    await writeOwner(reusedFile, {
      pid: process.pid,
      nonce: "old-process-instance",
      processIdentity: "test-boot:old-process-start",
    });
    const reclaim = new FileLease({
      file: reusedFile,
      staleAfterMs: 60_000,
      processProbe: deterministicProbe,
    });
    await expect(reclaim.acquire({ timeoutMs: 100 })).resolves.toBeUndefined();
    await reclaim.release();

    const activeFile = await leasePath();
    await writeOwner(activeFile, {
      pid: process.pid,
      nonce: "same-process-instance",
      processIdentity: currentIdentity,
      processStartedAtMs: 1_000,
    });
    const contender = new FileLease({
      file: activeFile,
      staleAfterMs: 1,
      processProbe: deterministicProbe,
    });
    await expect(contender.acquire({ timeoutMs: 5 })).rejects.toBeInstanceOf(FileLeaseBusyError);
    expect((await ownerRecord(activeFile)).nonce).toBe("same-process-instance");
  });

  it("fails closed when process identity is unknown", async () => {
    const file = await leasePath();
    await writeOwner(file, {
      pid: 42,
      nonce: "unknown-owner",
      processIdentity: "unverifiable",
    });
    const lease = new FileLease({
      file,
      staleAfterMs: 1,
      processProbe: async () => ({ state: "unknown" }),
    });

    await expect(lease.acquire({ timeoutMs: 5 })).rejects.toBeInstanceOf(FileLeaseBusyError);
    expect((await ownerRecord(file)).nonce).toBe("unknown-owner");
  });

  it("allows exactly one of sixteen simultaneous stale reclaimers to own the replacement", async () => {
    const file = await leasePath();
    await writeOwner(file, {
      pid: deadPid,
      nonce: "stale-generation",
      processIdentity: "old-boot:dead-start",
    });
    const leases = Array.from({ length: 16 }, () => new FileLease({
      file,
      staleAfterMs: 30_000,
      processProbe: deterministicProbe,
    }));

    const results = await Promise.allSettled(leases.map(lease => lease.acquire({ timeoutMs: 30 })));
    const winners = results
      .map((result, index) => result.status === "fulfilled" ? index : -1)
      .filter(index => index >= 0);
    expect(winners).toHaveLength(1);
    expect(results.filter(result => result.status === "rejected")).toHaveLength(15);
    expect((await ownerRecord(file)).nonce).not.toBe("stale-generation");
    await leases[winners[0]!]!.release();
  });

  it("does not remove an owner marker whose nonce changed", async () => {
    const file = await leasePath();
    const lease = new FileLease({ file, staleAfterMs: 30_000, processProbe: deterministicProbe });
    await lease.acquire();
    const marker = await ownerPath(file);
    await writeFile(marker, JSON.stringify({
      version: 2,
      pid: process.pid,
      nonce: "replacement",
      platform: process.platform,
      processIdentity: currentIdentity,
      processStartedAtMs: 1_000,
      acquiredAtMs: Date.now(),
    }), { mode: 0o600 });

    await expect(lease.release()).rejects.toBeInstanceOf(FileLeaseOwnershipError);
    expect(JSON.parse(await readFile(marker, "utf8")).nonce).toBe("replacement");
  });

  it("reclaims a real abruptly terminated owner without waiting sixty seconds", async () => {
    const file = await leasePath();
    const worker = Bun.spawn([
      process.execPath,
      path.join(import.meta.dir, "support", "file-lease-worker.ts"),
      file,
    ], { stdout: "pipe", stderr: "pipe" });
    try {
      await waitForReady(worker.stdout);
      worker.kill();
      await worker.exited;

      const lease = new FileLease({ file, staleAfterMs: 60_000 });
      const started = performance.now();
      await lease.acquire({ timeoutMs: 5_000 });
      expect(performance.now() - started).toBeLessThan(5_000);
      await lease.release();
    } finally {
      worker.kill();
      await worker.exited.catch(() => undefined);
    }
  }, 15_000);
});
