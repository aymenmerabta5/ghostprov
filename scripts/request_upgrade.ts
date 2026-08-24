import "dotenv/config";
import { mkdir, rename, writeFile } from "node:fs/promises";
import * as path from "node:path";

export async function requestSupervisorUpgrade(
  env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const target = path.resolve(env.GP_SUPERVISOR_CONTROL_FILE ?? ".ghostprov/upgrade.request");
  await mkdir(path.dirname(target), { recursive: true });
  const temporary = `${target}.${process.pid}.tmp`;
  await writeFile(temporary, JSON.stringify({ requestedAt: new Date().toISOString() }), {
    encoding: "utf8",
    mode: 0o600,
  });
  await rename(temporary, target);
  return target;
}

const isMain = process.argv[1]?.endsWith("request_upgrade.ts") ||
  process.argv[1]?.endsWith("request_upgrade.js");
if (isMain) console.log(`GhostProv upgrade requested through ${await requestSupervisorUpgrade()}`);
