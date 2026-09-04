const CANONICAL_SANDBOX_WORKSPACE = /^\/codex_workspace\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const GHOSTPROV_WORKSPACE_RELATIVE = /^\.ghostprov-v2\/[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$/;

export function isCanonicalSandboxWorkspaceRoot(value: string): boolean {
  return CANONICAL_SANDBOX_WORKSPACE.test(value);
}

export function canonicalSandboxWorkspacePath(
  workspaceRoot: string,
  relativePath: string,
): string | null {
  if (
    !isCanonicalSandboxWorkspaceRoot(workspaceRoot) ||
    !GHOSTPROV_WORKSPACE_RELATIVE.test(relativePath) ||
    relativePath.endsWith("/") ||
    relativePath.includes("//") ||
    relativePath.split("/").includes("..")
  ) return null;
  return `${workspaceRoot}/${relativePath}`;
}
