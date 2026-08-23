import { describe, expect, it } from "bun:test";
import { resolveLiveV2Options } from "../scripts/live_v2_options";
import { main as singleMain } from "../scripts/live_v2_single";
import { main as fleetMain } from "../scripts/live_v2_fleet";
import {
  main as smokeMain,
  resolveSmokeProtectedRoot,
} from "../scripts/smoke_v2_candidate";

describe("V2 live canary guards", () => {
  it("keeps every new canary import-safe", () => {
    expect(typeof singleMain).toBe("function");
    expect(typeof fleetMain).toBe("function");
    expect(typeof smokeMain).toBe("function");
  });

  it("allows smoke protection of an explicit loopback router only", () => {
    expect(resolveSmokeProtectedRoot({})).toBe("http://127.0.0.1:8787");
    expect(resolveSmokeProtectedRoot({
      GP_SMOKE_PROTECTED_ROOT: "http://127.0.0.1:18787",
    })).toBe("http://127.0.0.1:18787");
    expect(() => resolveSmokeProtectedRoot({
      GP_SMOKE_PROTECTED_ROOT: "https://example.com",
    })).toThrow("loopback");
    expect(() => resolveSmokeProtectedRoot({
      GP_SMOKE_PROTECTED_ROOT: "http://127.0.0.1:18787/mutate",
    })).toThrow("origin");
  });

  it("refuses live execution without every transport/load opt-in", () => {
    expect(() => resolveLiveV2Options({})).toThrow("GP_LIVE_V2=1");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_TRANSPORT: "auto",
    })).toThrow("GP_TRANSPORT=v2");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_TRANSPORT: "v2",
      GP_LIVE_PROJECT_COUNT: "5",
    })).toThrow("1 through 4");
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_TRANSPORT: "v2",
      PORT: "8787",
    })).toThrow("protected port 8787");
  });

  it("never enables automatic project deletion or creation", () => {
    const options = resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_TRANSPORT: "v2",
      GP_LIVE_PROJECT_COUNT: "4",
      PORT: "18787",
      GP_ALLOW_PROJECT_DELETE: "1",
      GP_AUTO_CREATE_PROJECTS: "1",
    });

    expect(options).toMatchObject({
      projectCount: 4,
      port: 18787,
      deleteProjects: false,
      autoCreateProjects: false,
    });
  });

  it("requires a separate scale opt-in for more than one project", () => {
    expect(() => resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_TRANSPORT: "v2",
      GP_LIVE_PROJECT_COUNT: "4",
    }, { requireScale: true })).toThrow("GP_LIVE_SCALE=1");

    expect(resolveLiveV2Options({
      GP_LIVE_V2: "1",
      GP_LIVE_SCALE: "1",
      GP_TRANSPORT: "v2",
      GP_LIVE_PROJECT_COUNT: "4",
    }, { requireScale: true }).scale).toBe(true);
  });
});
