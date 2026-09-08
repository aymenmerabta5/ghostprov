import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { requireGitCleanFilterLiveGuards } from "../scripts/lib/git-clean-filter-proof";

describe("Git clean-filter live verifier guards", () => {
  it("requires independent live, project, model and candidate authorization", () => {
    const enabled = { GP_LIVE_GIT_CLEAN_FILTER: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GP_LIVE_ALLOW_MODEL_REQUEST: "1", GP_CANDIDATE: "1", PORT: "18888" };
    expect(() => requireGitCleanFilterLiveGuards(enabled)).not.toThrow();
    for (const key of Object.keys(enabled)) expect(() => requireGitCleanFilterLiveGuards({ ...enabled, [key]: undefined })).toThrow();
    for (const port of ["18787", "0", "18889"]) expect(() => requireGitCleanFilterLiveGuards({ ...enabled, PORT: port })).toThrow();
    expect(() => requireGitCleanFilterLiveGuards({ ...enabled, GP_LIVE_GIT_CLEAN_FILTER: "true" })).toThrow();
  });

  it("stops the executable before credentials or project operations with guards missing", async () => {
    const child = Bun.spawn([process.execPath, "--no-env-file", path.resolve("scripts/live_git_clean_filter.ts")], {
      cwd: path.resolve("."), env: { GP_LIVE_GIT_CLEAN_FILTER: "0", SystemRoot: process.env.SystemRoot ?? "" },
      stdout: "pipe", stderr: "pipe",
    });
    const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    expect(code).toBe(1);
    expect(stdout).toBe("");
    expect(stderr.trim()).toBe("Git clean-filter verifier stopped by its guard or preflight");
  });
});
