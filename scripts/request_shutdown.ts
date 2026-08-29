import { writeBoundSupervisorControlRequest } from "./supervisor_control";

export async function requestSupervisorShutdown(
  _env: NodeJS.ProcessEnv = process.env,
  cwd = process.cwd(),
): Promise<string> {
  return (await writeBoundSupervisorControlRequest("shutdown", cwd)).path;
}

if (import.meta.main) {
  console.log(`GhostProv shutdown requested through ${await requestSupervisorShutdown()}`);
}
