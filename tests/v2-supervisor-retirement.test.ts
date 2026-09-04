import { describe, expect, it } from "bun:test";
import { retireVerifiedSupervisor } from "../sandbox/v2/supervisor-retirement";

describe("Supervisor retirement", () => {
  it("allows child cleanup beyond five seconds before accepting exact retirement proof", async () => {
    let terms = 0;
    await retireVerifiedSupervisor({
      isRetired: async () => false,
      isExactProcess: async () => true,
      terminate: () => { terms++; },
      waitForRetirement: async timeoutMs => timeoutMs >= 17_001 && timeoutMs <= 20_000,
    });
    expect(terms).toBe(1);
  });

  it("fails closed after bounded cleanup without force-killing the supervisor", async () => {
    let terms = 0;
    let waits = 0;
    await expect(retireVerifiedSupervisor({
      isRetired: async () => false,
      isExactProcess: async () => true,
      terminate: () => { terms++; },
      waitForRetirement: async () => { waits++; return false; },
    })).rejects.toThrow("retained-child cleanup");
    expect({ terms, waits }).toEqual({ terms: 1, waits: 1 });
  });

  it("rejects missing or mismatched process proof without signaling", async () => {
    let terms = 0;
    await expect(retireVerifiedSupervisor({
      isRetired: async () => false,
      isExactProcess: async () => false,
      terminate: () => { terms++; },
      waitForRetirement: async () => { throw new Error("must not wait"); },
    })).rejects.toThrow("identity is invalid");
    expect(terms).toBe(0);
  });

  it("accepts an exact retained cleanup proof without touching a reusable PID", async () => {
    await retireVerifiedSupervisor({
      isRetired: async () => true,
      isExactProcess: async () => { throw new Error("must not inspect retired PID"); },
      terminate: () => { throw new Error("must not signal retired PID"); },
      waitForRetirement: async () => { throw new Error("must not wait"); },
    });
  });
});
