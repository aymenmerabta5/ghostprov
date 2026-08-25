import { describe, expect, it } from "bun:test";
import {
  DenoSidecarOwnershipLease,
  SidecarOwnershipError,
  type SidecarOwnershipBackend,
  type SidecarOwnershipHandle,
} from "../sandbox/v2/ownership-lease";

interface FakeFile {
  bytes: Uint8Array;
  generation: number;
  locked: boolean;
  unlocks: number;
  closes: number;
}

class FakeHandle implements SidecarOwnershipHandle {
  readonly #file: FakeFile;
  readonly #openedGeneration: number;
  #ownsLock = false;
  #closed = false;

  constructor(file: FakeFile) {
    this.#file = file;
    this.#openedGeneration = file.generation;
  }

  async tryLockExclusive(): Promise<boolean> {
    if (this.#closed || this.#file.locked) return false;
    this.#file.locked = true;
    this.#ownsLock = true;
    return true;
  }

  async replace(bytes: Uint8Array): Promise<void> {
    if (!this.#ownsLock) throw new Error("not locked");
    this.#file.bytes = bytes.slice();
  }

  async read(): Promise<Uint8Array> {
    return this.#file.bytes.slice();
  }

  async pathStillMatches(): Promise<boolean> {
    return this.#file.generation === this.#openedGeneration;
  }

  async unlock(): Promise<void> {
    if (!this.#ownsLock) return;
    this.#ownsLock = false;
    this.#file.locked = false;
    this.#file.unlocks++;
  }

  close(): void {
    this.#closed = true;
    this.#file.closes++;
  }
}

class FakeBackend implements SidecarOwnershipBackend {
  readonly pid = 123;
  readonly file: FakeFile = {
    bytes: new TextEncoder().encode('{"stale":true}'),
    generation: 1,
    locked: false,
    unlocks: 0,
    closes: 0,
  };
  readonly promotions: Array<{
    root: string;
    launchNonce: string;
    pid: number;
    processStart: string;
    locked: boolean;
  }> = [];
  onPromote?: () => Promise<void>;
  #nonce = 0;

  now(): number { return 1_000; }
  uuid(): string { return `lease-${++this.#nonce}`; }
  async open(): Promise<SidecarOwnershipHandle> { return new FakeHandle(this.file); }
  async promoteProcessIdentity(
    root: string,
    launchNonce: string,
    pid: number,
    processStart: string,
  ): Promise<void> {
    this.promotions.push({ root, launchNonce, pid, processStart, locked: this.file.locked });
    await this.onPromote?.();
  }

  replacePath(): void {
    this.file.generation++;
    this.file.bytes = new TextEncoder().encode("{}");
  }
}

describe("DenoSidecarOwnershipLease", () => {
  it("uses the exclusive file lock rather than stale PID text", async () => {
    const backend = new FakeBackend();
    const first = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-one",
      backend,
    });
    await first.assertOwned();

    await expect(DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-two",
      backend,
    })).rejects.toMatchObject({
      name: "SidecarOwnershipError",
      code: "contended",
    });

    await first.release();
    const replacement = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-two",
      backend,
    });
    await replacement.assertOwned();
    expect(new TextDecoder().decode(backend.file.bytes)).toContain("sidecar-two");
    await replacement.release();
  });

  it("fails closed if the locked path is replaced", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });
    backend.replacePath();

    await expect(lease.assertOwned()).rejects.toBeInstanceOf(SidecarOwnershipError);
    await expect(lease.assertOwned()).rejects.toMatchObject({ code: "lost" });
    await lease.release();
    await lease.release();
  });

  it("rejects ownership assertions after release", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });
    await lease.release();
    await expect(lease.assertOwned()).rejects.toMatchObject({ code: "lost" });
  });

  it("single-flights concurrent release without double unlock or close", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });

    await Promise.all([lease.release(), lease.release(), lease.release()]);

    expect(backend.file.unlocks).toBe(1);
    expect(backend.file.closes).toBe(1);
  });

  it("serializes release behind an ownership-scoped destructive operation", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });
    let finish!: () => void;
    const blocker = new Promise<void>(resolve => { finish = resolve; });
    let operationFinished = false;
    const operation = lease.withOwnership(async () => {
      await blocker;
      operationFinished = true;
    });
    const releasing = lease.release();
    await Bun.sleep(0);
    expect(operationFinished).toBe(false);
    expect(backend.file.locked).toBe(true);
    await expect(lease.withOwnership(async () => undefined)).rejects.toMatchObject({ code: "lost" });

    finish();
    await operation;
    await releasing;
    expect(backend.file.locked).toBe(false);
  });

  it("promotes the candidate PID only while holding and revalidating ownership", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      root: "/tmp/interlock-test",
      backend,
    });

    await lease.promoteProcessIdentity("launch-nonce-1", "7001");

    expect(backend.promotions).toEqual([{
      root: "/tmp/interlock-test",
      launchNonce: "launch-nonce-1",
      pid: 123,
      processStart: "7001",
      locked: true,
    }]);
    await lease.release();
  });

  it("does not release ownership while atomic PID promotion is in flight", async () => {
    const backend = new FakeBackend();
    let finish!: () => void;
    const blocked = new Promise<void>(resolve => { finish = resolve; });
    backend.onPromote = () => blocked;
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });

    const promoting = lease.promoteProcessIdentity("launch-nonce-2", "7002");
    await Bun.sleep(0);
    const releasing = lease.release();
    await Bun.sleep(0);
    expect(backend.file.locked).toBe(true);

    finish();
    await promoting;
    await releasing;
    expect(backend.file.locked).toBe(false);
  });

  it("fails PID promotion if ownership identity changes during the operation", async () => {
    const backend = new FakeBackend();
    backend.onPromote = async () => backend.replacePath();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });

    await expect(lease.promoteProcessIdentity("launch-nonce-3", "7003"))
      .rejects.toMatchObject({ name: "SidecarOwnershipError", code: "lost" });
    await lease.release();
  });

  it("revalidates path identity after an ownership-scoped operation", async () => {
    const backend = new FakeBackend();
    const lease = await DenoSidecarOwnershipLease.acquire({
      instanceId: "sidecar-owner",
      backend,
    });

    await expect(lease.withOwnership(async () => {
      backend.replacePath();
    })).rejects.toMatchObject({ code: "lost" });
    await lease.release();
  });
});
