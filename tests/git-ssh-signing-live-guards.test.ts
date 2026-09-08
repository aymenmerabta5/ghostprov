import { describe, expect, it } from "bun:test";
import * as path from "node:path";
import { requireGitSshSigningLiveGuards } from "../scripts/lib/git-ssh-signing-proof";

describe("Git SSH signing live verifier guards", () => {
  it("requires independent live, project, model and candidate authorization", () => {
    const enabled = { GP_LIVE_GIT_SSH_SIGNING: "1", GP_LIVE_ALLOW_TEST_PROJECT_MUTATION: "1",
      GP_LIVE_ALLOW_MODEL_REQUEST: "1", GP_CANDIDATE: "1", PORT: "18888" };
    expect(() => requireGitSshSigningLiveGuards(enabled)).not.toThrow();
    for (const key of Object.keys(enabled)) expect(() => requireGitSshSigningLiveGuards({ ...enabled, [key]: undefined })).toThrow();
    for (const port of ["18787", "0", "18889"]) expect(() => requireGitSshSigningLiveGuards({ ...enabled, PORT: port })).toThrow();
    expect(() => requireGitSshSigningLiveGuards({ ...enabled, GP_LIVE_GIT_SSH_SIGNING: "true" })).toThrow();
  });

  it("stops the executable before credentials or project operations with guards missing", async () => {
    const child = Bun.spawn([process.execPath, "--no-env-file", path.resolve("scripts/live_git_ssh_signing.ts")], {
      cwd: path.resolve("."), env: { GP_LIVE_GIT_SSH_SIGNING: "0", SystemRoot: process.env.SystemRoot ?? "" },
      stdout: "pipe", stderr: "pipe",
    });
    const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    expect(code).toBe(1);
    expect(stdout).toBe("");
    expect(stderr.trim()).toBe("Git SSH signing verifier stopped by its guard or preflight");
  });
});
