import { describe, expect, it } from "bun:test";
import { retiredSupervisorProofOptions } from "../scripts/live_retired_supervisor_repro";

describe("retired supervisor live reproduction guards", () => {
  const safe = { GP_LIVE_RETIRED_SUPERVISOR_REPRO: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
    PORT: "18888", PRISM_COOKIE_FILE: "not-opened-test-cookie.json" };

  it("requires both explicit live guards and a dedicated candidate port", () => {
    expect(() => retiredSupervisorProofOptions({})).toThrow();
    expect(() => retiredSupervisorProofOptions({ ...safe, GP_LIVE_RETIRED_SUPERVISOR_REPRO: "0" })).toThrow();
    expect(() => retiredSupervisorProofOptions({ ...safe, GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "0" })).toThrow();
    expect(() => retiredSupervisorProofOptions({ ...safe, PORT: "18787" })).toThrow();
    expect(() => retiredSupervisorProofOptions({ ...safe, PORT: undefined })).toThrow();
    expect(() => retiredSupervisorProofOptions({ ...safe, PRISM_COOKIE_FILE: "" })).toThrow();
    expect(retiredSupervisorProofOptions(safe).cookieFile).toContain("not-opened-test-cookie.json");
  });
});
