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
  }

  close(): void {
    this.#closed = true;
  }
}

class FakeBackend implements SidecarOwnershipBackend {
  readonly pid = 123;
  readonly file: FakeFile = {
    bytes: new TextEncoder().encode('{"stale":true}'),
    generation: 1,
    locked: false,
  };
  #nonce = 0;

  now(): number { return 1_000; }
  uuid(): string { return `lease-${++this.#nonce}`; }
  async open(): Promise<SidecarOwnershipHandle> { return new FakeHandle(this.file); }

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
