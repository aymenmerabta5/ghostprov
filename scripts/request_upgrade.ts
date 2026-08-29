import { writeBoundSupervisorControlRequest } from "./supervisor_control";

export async function requestSupervisorUpgrade(
  _env: NodeJS.ProcessEnv = process.env,
  cwd = process.cwd(),
): Promise<string> {
  return (await writeBoundSupervisorControlRequest("upgrade", cwd)).path;
}

if (import.meta.main) {
  console.log(`GhostProv upgrade requested through ${await requestSupervisorUpgrade()}`);
}
