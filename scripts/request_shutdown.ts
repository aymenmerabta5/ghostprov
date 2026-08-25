import "dotenv/config";
import { writeBoundSupervisorControlRequest } from "./supervisor_control";

export async function requestSupervisorShutdown(
  _env: NodeJS.ProcessEnv = process.env,
  cwd = process.cwd(),
): Promise<string> {
  return (await writeBoundSupervisorControlRequest("shutdown", cwd)).path;
}

const isMain = process.argv[1]?.endsWith("request_shutdown.ts") ||
  process.argv[1]?.endsWith("request_shutdown.js");
if (isMain) console.log(`GhostProv shutdown requested through ${await requestSupervisorShutdown()}`);
