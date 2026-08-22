import { afterEach, describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, utimes, writeFile } from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { FileLease, FileLeaseBusyError, FileLeaseOwnershipError } from "../src/core/file-lease";

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map(root => rm(root, { recursive: true, force: true })));
});

async function leasePath(): Promise<string> {
  const root = await mkdtemp(path.join(os.tmpdir(), "ghostprov-lease-"));
  roots.push(root);
  return path.join(root, "account.lock");
}

describe("FileLease", () => {
  it("serializes owners and rejects an active competing lease", async () => {
    const file = await leasePath();
    const first = new FileLease({ file, staleAfterMs: 30_000 });
    const second = new FileLease({ file, staleAfterMs: 30_000 });

    await first.acquire();
    await expect(second.acquire({ timeoutMs: 5 })).rejects.toBeInstanceOf(FileLeaseBusyError);
    await first.release();
    await expect(second.acquire({ timeoutMs: 50 })).resolves.toBeUndefined();
    await second.release();
  });

  it("reclaims a stale lease only when its recorded process is gone", async () => {
    const file = await leasePath();
    await mkdir(path.dirname(file), { recursive: true });
    await writeFile(file, JSON.stringify({ pid: 2_147_483_647, nonce: "dead", acquiredAtMs: 1 }), { mode: 0o600 });
    const old = new Date(Date.now() - 60_000);
    await utimes(file, old, old);

    const lease = new FileLease({ file, staleAfterMs: 10 });
    await expect(lease.acquire({ timeoutMs: 100 })).resolves.toBeUndefined();
    expect(JSON.parse(await readFile(file, "utf8")).nonce).not.toBe("dead");
    await lease.release();
  });

  it("reclaims a stale malformed record left by a torn initial write", async () => {
    const file = await leasePath();
    await writeFile(file, "{", { mode: 0o600 });
    const old = new Date(Date.now() - 60_000);
    await utimes(file, old, old);

    const lease = new FileLease({ file, staleAfterMs: 10 });
    await expect(lease.acquire({ timeoutMs: 100 })).resolves.toBeUndefined();
    expect(JSON.parse(await readFile(file, "utf8")).nonce).toBeString();
    await lease.release();
  });

  it("never reclaims an active process lease even when its timestamp is old", async () => {
    const file = await leasePath();
    await writeFile(file, JSON.stringify({ pid: process.pid, nonce: "active", acquiredAtMs: 1 }), { mode: 0o600 });
    const old = new Date(Date.now() - 60_000);
    await utimes(file, old, old);

    const lease = new FileLease({ file, staleAfterMs: 10 });
    await expect(lease.acquire({ timeoutMs: 5 })).rejects.toBeInstanceOf(FileLeaseBusyError);
    expect(JSON.parse(await readFile(file, "utf8")).nonce).toBe("active");
  });

  it("does not remove a lock file whose ownership nonce changed", async () => {
    const file = await leasePath();
    const lease = new FileLease({ file, staleAfterMs: 30_000 });
    await lease.acquire();
    await writeFile(file, JSON.stringify({ pid: process.pid, nonce: "replacement", acquiredAtMs: Date.now() }), { mode: 0o600 });

    await expect(lease.release()).rejects.toBeInstanceOf(FileLeaseOwnershipError);
    expect(JSON.parse(await readFile(file, "utf8")).nonce).toBe("replacement");
  });
});
