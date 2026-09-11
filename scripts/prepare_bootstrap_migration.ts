import * as path from "node:path";
import { prepareBootstrapHoldMigrationLedger } from "../src/fleet/bootstrap-migration";
import { projectId, type ProjectId } from "../src/sandbox/types";

const SHA256 = /^[a-f0-9]{64}$(?![\s\S])/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$(?![\s\S])/;

export interface BootstrapMigrationCommand {
  readonly manifestFile: string;
  readonly identityFingerprint: string;
  readonly projectIds: readonly ProjectId[];
}

/** Explicit local inputs only: no account resolution or browser/API credential files. */
export function parseBootstrapMigrationArguments(args: readonly string[]): BootstrapMigrationCommand {
  if (args.length > 9) throw new Error("Bootstrap migration accepts at most two explicit projects");
  let manifestFile: string | undefined;
  let identityFingerprint: string | undefined;
  let confirmed = false;
  const projectIds: ProjectId[] = [];

  for (let index = 0; index < args.length; index++) {
    const argument = args[index];
    if (argument === "--manifest") {
      if (manifestFile !== undefined) throw new Error("--manifest may be supplied only once");
      const value = args[++index];
      if (!value || value.trim() === "" || value.startsWith("--") || value.includes("\0")) {
        throw new Error("--manifest requires an explicit file path");
      }
      manifestFile = path.resolve(value);
      continue;
    }
    if (argument === "--identity-fingerprint") {
      if (identityFingerprint !== undefined) throw new Error("--identity-fingerprint may be supplied only once");
      const value = args[++index];
      if (!value || !SHA256.test(value)) {
        throw new Error("--identity-fingerprint requires a lowercase SHA-256 digest");
      }
      identityFingerprint = value;
      continue;
    }
    if (argument === "--project") {
      const value = args[++index];
      if (!value || !UUID.test(value)) throw new Error("--project requires an exact lowercase project UUID");
      const id = projectId(value);
      if (projectIds.includes(id)) throw new Error("--project targets must be unique");
      if (projectIds.length === 2) throw new Error("Bootstrap migration accepts at most two explicit projects");
      projectIds.push(id);
      continue;
    }
    if (argument === "--confirm-legacy-bootstrap-migration") {
      if (confirmed) throw new Error("The migration confirmation flag may be supplied only once");
      confirmed = true;
      continue;
    }
    throw new Error("Unknown bootstrap migration argument");
  }

  if (manifestFile === undefined) throw new Error("--manifest is required");
  if (identityFingerprint === undefined) throw new Error("--identity-fingerprint is required");
  if (projectIds.length === 0) throw new Error("At least one explicit --project is required");
  if (!confirmed) throw new Error("--confirm-legacy-bootstrap-migration is required");
  return Object.freeze({
    manifestFile,
    identityFingerprint,
    projectIds: Object.freeze(projectIds),
  });
}

export async function main(
  args: readonly string[] = process.argv.slice(2),
  output: (value: string) => void = value => console.log(value),
): Promise<void> {
  const command = parseBootstrapMigrationArguments(args);
  const result = await prepareBootstrapHoldMigrationLedger(command);
  output(JSON.stringify(result, null, 2));
}

if (import.meta.main) await main();
