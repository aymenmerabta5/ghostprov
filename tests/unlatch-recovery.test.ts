import { describe, expect, it } from "bun:test";
import { recoverUnlatchedPublication } from "../src/relay/unlatch-recovery";

describe("unlatched publication recovery", () => {
  it("revalidates or revives the daemon before republishing a commit", async () => {
    const events: string[] = [];

    const result = await recoverUnlatchedPublication({
      attempt: 0,
      maxAttempts: 2,
      ensureDaemonReady: async () => { events.push("ensure-daemon"); },
      republish: async () => { events.push("republish"); },
    });

    expect(result).toEqual({ recovered: true, nextAttempt: 1 });
    expect(events).toEqual(["ensure-daemon", "republish"]);
  });

  it("does not republish when daemon revival fails", async () => {
    let republished = false;
    const failure = new Error("daemon still unavailable");

    await expect(recoverUnlatchedPublication({
      attempt: 0,
      maxAttempts: 2,
      ensureDaemonReady: async () => { throw failure; },
      republish: async () => { republished = true; },
    })).rejects.toBe(failure);
    expect(republished).toBe(false);
  });

  it("returns exhausted without touching the daemon or publication", async () => {
    let calls = 0;
    const result = await recoverUnlatchedPublication({
      attempt: 2,
      maxAttempts: 2,
      ensureDaemonReady: async () => { calls++; },
      republish: async () => { calls++; },
    });

    expect(result).toEqual({ recovered: false, nextAttempt: 2 });
    expect(calls).toBe(0);
  });
});
