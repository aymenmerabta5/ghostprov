import { describe, expect, it } from "bun:test";
import {
  TerminalNotProvenError,
  canReleaseLane,
  matchesTerminalEnvelope,
  settleWithTerminalRescue,
} from "../src/relay/terminal-rescue";

describe("terminal rescue", () => {
  it("never releases a published lane without terminal proof", () => {
    expect(canReleaseLane({ requestPublished: false, terminalProven: false })).toBe(true);
    expect(canReleaseLane({ requestPublished: true, terminalProven: true })).toBe(true);
    expect(canReleaseLane({ requestPublished: true, terminalProven: false })).toBe(false);
  });

  it("requires both acknowledgement and request ID for segmented jobs", () => {
    const terminal = { gp_ack: "ack-1", gp_transport: { id: "req-1" }, gp_final: { id: "resp" } };
    expect(matchesTerminalEnvelope(terminal, "ack-1", "req-1")).toBe(true);
    expect(matchesTerminalEnvelope({ ...terminal, gp_transport: undefined }, "ack-1", "req-1")).toBe(false);
    expect(matchesTerminalEnvelope(terminal, "wrong", "req-1")).toBe(false);
    expect(matchesTerminalEnvelope(terminal, "ack-1", "wrong")).toBe(false);
    expect(matchesTerminalEnvelope({ ack: "ack-1", transport: { id: "req-1" }, status: "failed" }, "ack-1", "req-1")).toBe(true);
    expect(matchesTerminalEnvelope({ gp_ack: "ack-1", gp_final: {} }, "ack-1", null)).toBe(true);
  });

  it("turns a rejected run into a successful recovered terminal", async () => {
    const result = await settleWithTerminalRescue(
      Promise.reject(Object.assign(new Error("poll closed"), { kind: "sync" })),
      { hasTerminal: () => false, rescue: async () => ({ id: "resp_recovered" }) },
    );
    expect(result).toEqual({
      value: { id: "resp_recovered" },
      reason: "recovered-after-error:sync",
    });
  });

  it("preserves the original error when no terminal can be rescued", async () => {
    const error = Object.assign(new Error("upstream failed"), { kind: "upstream" });
    await expect(settleWithTerminalRescue(Promise.reject(error), {
      hasTerminal: () => false,
      rescue: async () => null,
    })).rejects.toBe(error);
  });

  it("does not rescue again when a rejected run already proved its terminal", async () => {
    const error = Object.assign(new Error("upstream rejected request"), { kind: "upstream" });
    let rescueCalls = 0;
    await expect(settleWithTerminalRescue(Promise.reject(error), {
      hasTerminal: () => true,
      rescue: async () => {
        rescueCalls++;
        return null;
      },
    })).rejects.toBe(error);
    expect(rescueCalls).toBe(0);
  });

  it("rejects a resolved run that cannot prove or rescue a terminal", async () => {
    await expect(settleWithTerminalRescue(Promise.resolve({ id: "unproven" }), {
      hasTerminal: () => false,
      rescue: async () => null,
    })).rejects.toBeInstanceOf(TerminalNotProvenError);
  });
});
