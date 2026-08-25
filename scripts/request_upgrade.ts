import "dotenv/config";
import { writeBoundSupervisorControlRequest } from "./supervisor_control";

export async function requestSupervisorUpgrade(
  _env: NodeJS.ProcessEnv = process.env,
  cwd = process.cwd(),
): Promise<string> {
  return (await writeBoundSupervisorControlRequest("upgrade", cwd)).path;
}

const isMain = process.argv[1]?.endsWith("request_upgrade.ts") ||
  process.argv[1]?.endsWith("request_upgrade.js");
if (isMain) console.log(`GhostProv upgrade requested through ${await requestSupervisorUpgrade()}`);
