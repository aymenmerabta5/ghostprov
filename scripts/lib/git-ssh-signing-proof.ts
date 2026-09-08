/** Guarded live verification is separate from normal production configuration. */
export function requireGitSshSigningLiveGuards(environment: Readonly<Record<string, string | undefined>>): void {
  if (environment.GP_LIVE_GIT_SSH_SIGNING !== "1" || environment.GP_LIVE_ALLOW_TEST_PROJECT_MUTATION !== "1" ||
      environment.GP_LIVE_ALLOW_MODEL_REQUEST !== "1" || environment.GP_CANDIDATE !== "1" || environment.PORT !== "18888") {
    throw new Error("Git SSH signing live proof requires project/model guards and candidate port 18888");
  }
}
