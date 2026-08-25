import { describe, expect, it } from "bun:test";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  attachAtomicBytePermit,
  atomicBytePermitOf,
  releaseAtomicBytePermit,
  shareAtomicBytePermit,
  transferAtomicBytePermit,
  tryResizeAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";

describe("atomic byte permit carriers", () => {
  it("shares one physical allocation across independently released owners", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const source = attachAtomicBytePermit({}, await pool.acquire(8_192));
    const final = {};
    shareAtomicBytePermit(source, final, "final");

    expect(pool.snapshot()).toMatchObject({
      activePermits: 1,
      ordinaryUsedBytes: 8_192,
    });
    releaseAtomicBytePermit(source);
    expect(atomicBytePermitOf(final)).toMatchObject({ active: true, bytes: 8_192 });
    expect(pool.snapshot().ordinaryUsedBytes).toBe(8_192);

    releaseAtomicBytePermit(final);
    releaseAtomicBytePermit(final);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("moves one shared reference without releasing its sibling", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 4_096, jumboBytes: 1 });
    const event = attachAtomicBytePermit({}, await pool.acquire(4_096));
    const final = {};
    const output = {};
    shareAtomicBytePermit(event, final, "final");
    transferAtomicBytePermit(event, output, "output");

    expect(atomicBytePermitOf(event)).toBeNull();
    expect(atomicBytePermitOf(output)).toMatchObject({ active: true, bytes: 4_096 });
    releaseAtomicBytePermit(output);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(4_096);
    releaseAtomicBytePermit(final);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });

  it("allows resize only while the carrier is exclusively referenced", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const source = attachAtomicBytePermit({}, await pool.acquire(2_048));
    expect(tryResizeAtomicBytePermit(source, 4_096)).toBe(true);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(4_096);

    const sibling = {};
    shareAtomicBytePermit(source, sibling, "sibling");
    expect(tryResizeAtomicBytePermit(source, 8_192)).toBe(false);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(4_096);
    releaseAtomicBytePermit(source);
    releaseAtomicBytePermit(sibling);
    expect(pool.snapshot().ordinaryUsedBytes).toBe(0);
    pool.close();
  });
});
