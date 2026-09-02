import path from "node:path";
import { FleetDecommissionService } from "../src/fleet/decommission";
import { defaultFleetManifestFile, FleetManifestStore } from "../src/fleet/manifest";
import { PrismProjectsClient } from "../src/prism/projects-client";
import { PrismSessionManager } from "../src/prism/session-manager";
import { projectId, type ProjectId } from "../src/sandbox/types";

export interface DecommissionCommand {
  projects: readonly ProjectId[];
  confirm: string | null;
  execute: boolean;
}

export function parseDecommissionArguments(
  args: readonly string[],
  env: Record<string, string | undefined>,
): DecommissionCommand {
  const projects: ProjectId[] = [];
  let confirm: string | null = null;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === "--project") {
      const value = args[++index];
      if (!value) throw new Error("--project requires one explicit project UUID");
      projects.push(projectId(value));
      continue;
    }
    if (argument === "--confirm") {
      const value = args[++index];
      if (!value) throw new Error("--confirm requires the preview digest");
      if (confirm !== null) throw new Error("--confirm may be supplied only once");
      confirm = value;
      continue;
    }
    throw new Error(`Unknown argument: ${argument}`);
  }
  const unique = [...new Set(projects)].sort((left, right) => left.localeCompare(right));
  if (unique.length === 0) {
    throw new Error("At least one --project <uuid> target is required");
  }
  if (confirm !== null && !/^[a-f0-9]{64}$/.test(confirm)) {
    throw new Error("--confirm must be a lowercase 64-character SHA-256 digest");
  }
  if (confirm !== null && env.GP_ALLOW_PROJECT_DELETE !== "1") {
    throw new Error("Set GP_ALLOW_PROJECT_DELETE=1 to execute decommissioning");
  }
  return Object.freeze({
    projects: Object.freeze(unique),
    confirm,
    execute: confirm !== null,
  });
}

export async function main(
  args: readonly string[] = process.argv.slice(2),
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const command = parseDecommissionArguments(args, env);
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("PRISM_COOKIE_FILE is required");
  const rawMode = env.GP_PRISM_SESSION ?? "authenticated";
  if (rawMode !== "authenticated" && rawMode !== "auto" && rawMode !== "anonymous") {
    throw new Error("GP_PRISM_SESSION is invalid");
  }
  const session = await PrismSessionManager.create({
    origin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    mode: rawMode,
    allowAnonymous: env.GP_ALLOW_ANONYMOUS === "1",
    cookieFile: path.resolve(cookieFile),
    initialCookieHeader: env.PRISM_COOKIE,
  });
  try {
    const identity = await session.resolve();
    const manifest = new FleetManifestStore({
      file: env.GP_FLEET_MANIFEST_FILE?.trim()
        ? path.resolve(env.GP_FLEET_MANIFEST_FILE.trim())
        : defaultFleetManifestFile(identity.fingerprint),
      identityFingerprint: identity.fingerprint,
    });
    const service = new FleetDecommissionService({
      manifest,
      projects: new PrismProjectsClient(identity.account),
    });
    const preview = await service.preview(command.projects);
    if (!command.execute) {
      console.log(JSON.stringify({
        mode: "preview",
        identityFingerprint: preview.identityFingerprint,
        projects: preview.projects,
        confirmationDigest: preview.confirmationDigest,
        execute: "Set GP_ALLOW_PROJECT_DELETE=1 and repeat with --confirm <confirmationDigest>",
      }, null, 2));
      return;
    }
    await service.execute(preview, command.confirm!);
    console.log(JSON.stringify({
      mode: "executed",
      projects: preview.projects,
      deleted: preview.projects.length,
    }, null, 2));
  } finally {
    await session.close();
  }
}

if (import.meta.main) await main();
