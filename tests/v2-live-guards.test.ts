import { describe, expect, it } from "bun:test";
import { resolveLiveV2Options } from "../scripts/live_v2_options";
import { main as singleMain } from "../scripts/live_v2_single";
import { main as fleetMain } from "../scripts/live_v2_fleet";
import {
  main as smokeMain,
} from "../scripts/smoke_v2_candidate";
import {
  main as freshFleetRecoveryMain,
  resolveFreshFleetRecoveryOptions,
} from "../scripts/live_fresh_fleet_recovery";
import {
  main as memoryAccountingMain,
  resolveLiveMemoryAccountingOptions,
} from "../scripts/live_memory_accounting";

const API_KEY = "test-ghostprov-key-0123456789abcdef";

describe("V2 live canary guards", () => {
  it("keeps every new canary import-safe", () => {
    expect(typeof singleMain).toBe("function");
    expect(typeof fleetMain).toBe("function");
    expect(typeof smokeMain).toBe("function");
    expect(typeof freshFleetRecoveryMain).toBe("function");
    expect(typeof memoryAccountingMain).toBe("function");
  });

  it("hard-fences the memory proof to one throwaway project and candidate port", () => {
    expect(() => resolveLiveMemoryAccountingOptions({})).toThrow(
      "GP_LIVE_MEMORY_ACCOUNTING=1",
    );
    expect(() => resolveLiveMemoryAccountingOptions({
      GP_LIVE_MEMORY_ACCOUNTING: "1",
    })).toThrow("GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1");
    expect(() => resolveLiveMemoryAccountingOptions({
      GP_LIVE_MEMORY_ACCOUNTING: "1",
      GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PRISM_COOKIE_FILE: "cookies.json",
      PORT: "18787",
    })).toThrow("protected service port 18787");
    expect(resolveLiveMemoryAccountingOptions({
      GP_LIVE_MEMORY_ACCOUNTING: "1",
      GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PRISM_COOKIE_FILE: "cookies.json",
      PORT: "18888",
    })).toMatchObject({ port: 18_888, timeoutMs: 600_000 });
  });

  it("hard-fences the destructive fresh-fleet proof to dedicated projects and 18888", () => {
    expect(() => resolveFreshFleetRecoveryOptions({})).toThrow(
      "GP_LIVE_FRESH_FLEET_RECOVERY=1",
    );
    expect(() => resolveFreshFleetRecoveryOptions({
      GP_LIVE_FRESH_FLEET_RECOVERY: "1",
    })).toThrow("GP_LIVE_ALLOW_TEST_PROJECT_MUTATION=1");
    expect(() => resolveFreshFleetRecoveryOptions({
      GP_LIVE_FRESH_FLEET_RECOVERY: "1",
      GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PRISM_COOKIE_FILE: "cookies.json",
      PORT: "18787",
    })).toThrow("protected service port 18787");
    expect(resolveFreshFleetRecoveryOptions({
      GP_LIVE_FRESH_FLEET_RECOVERY: "1",
      GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PRISM_COOKIE_FILE: "cookies.json",
      PORT: "18888",
    })).toMatchObject({ port: 18_888, stabilityMs: 35_000 });
  });

  it("defaults live candidates to 18888 and refuses the protected service port", () => {
    expect(resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
    }).port).toBe(18_888);
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PORT: "18787",
    })).toThrow("protected service port 18787");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PORT: "8787",
    })).toThrow("hard-fenced to candidate port 18888");
  });

  it("refuses live execution without every load opt-in", () => {
    expect(() => resolveLiveV2Options({})).toThrow("GP_LIVE_V2=1");
    expect(() => resolveLiveV2Options({ GP_LIVE_V2: "1" }))
      .toThrow("GHOSTPROV_API_KEY");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      GP_LIVE_PROJECT_COUNT: "5",
    })).toThrow("1 through 4");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      PORT: "18787",
    })).toThrow("protected service port 18787");
  });

  it("rejects automatic project creation and never enables project deletion", () => {
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      GP_AUTO_CREATE_PROJECTS: "1",
    })).toThrow("unsupported");
    const options = resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      GP_LIVE_PROJECT_COUNT: "4",
      PORT: "18888",
      GP_ALLOW_PROJECT_DELETE: "1",
    });

    expect(options).toMatchObject({
      projectCount: 4,
      port: 18888,
      deleteProjects: false,
    });
  });

  it("requires a separate scale opt-in for more than one project", () => {
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      GP_LIVE_PROJECT_COUNT: "4",
    }, { requireScale: true })).toThrow("GP_LIVE_SCALE=1");

    expect(resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GHOSTPROV_API_KEY: API_KEY,
      GP_LIVE_SCALE: "1",
      GP_LIVE_PROJECT_COUNT: "4",
    }, { requireScale: true }).scale).toBe(true);
  });
});
