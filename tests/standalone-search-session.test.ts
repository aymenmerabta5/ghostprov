import { describe, expect, it } from "bun:test";
import { createAuthenticatedTenantScope } from "../src/http/api-key";
import { StandaloneSearchSessionIndex } from "../src/search/session-index";

describe("StandaloneSearchSessionIndex", () => {
  it("serializes one opaque session and advances only after successful completion", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(7) });
    const scope = createAuthenticatedTenantScope();
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const seen: Array<string | null> = [];
    const first = index.run(scope, "session-a", async previous => {
      seen.push(previous);
      await gate;
      return { responseId: "resp_first", value: "first" };
    });
    const second = index.run(scope, "session-a", async previous => {
      seen.push(previous);
      return { responseId: "resp_second", value: "second" };
    });
    for (let index = 0; index < 4; index++) await Promise.resolve();
    expect(seen).toEqual([null]);
    release();
    expect(await Promise.all([first, second])).toEqual(["first", "second"]);
    expect(seen).toEqual([null, "resp_first"]);
    expect(index.snapshot()).toMatchObject({ records: 1, activeKeys: 0 });
  });

  it("keeps trusted scopes isolated and retains the previous ID after a failed turn", async () => {
    const index = new StandaloneSearchSessionIndex({ hmacKey: new Uint8Array(32).fill(8) });
    const firstScope = createAuthenticatedTenantScope();
    const otherScope = createAuthenticatedTenantScope();
    await index.run(firstScope, "same-public-id", async previous => ({
      responseId: "resp_first",
      value: previous,
    }));
    await expect(index.run(firstScope, "same-public-id", async previous => {
      expect(previous).toBe("resp_first");
      throw new Error("failed follow-up");
    })).rejects.toThrow("failed follow-up");
    const retained = await index.run(firstScope, "same-public-id", async previous => ({
      responseId: "resp_recovered",
      value: previous,
    }));
    const isolated = await index.run(otherScope, "same-public-id", async previous => ({
      responseId: "resp_other",
      value: previous,
    }));
    expect(retained).toBe("resp_first");
    expect(isolated).toBeNull();
  });

  it("expires and evicts bounded content-free records without blocking execution", async () => {
    let now = 0;
    const index = new StandaloneSearchSessionIndex({
      hmacKey: new Uint8Array(32).fill(9),
      maxRecords: 1,
      maxBytes: 1024,
      ttlMs: 10,
      now: () => now,
    });
    const scope = createAuthenticatedTenantScope();
    await index.run(scope, "one", async () => ({ responseId: "resp_one", value: null }));
    await index.run(scope, "two", async () => ({ responseId: "resp_two", value: null }));
    expect(index.snapshot().records).toBe(1);
    const evicted = await index.run(scope, "one", async previous => ({
      responseId: "resp_one_new",
      value: previous,
    }));
    expect(evicted).toBeNull();
    now = 11;
    expect(index.snapshot()).toMatchObject({ records: 0, bytes: 0 });
  });
});
