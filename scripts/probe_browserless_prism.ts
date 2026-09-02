import { PrismAccountSession } from "../src/prism/account-session";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { resolveMultiSandboxConfig } from "../src/config/multi-sandbox";
import { SystemMachineProbe } from "../src/router/resource-governor";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { SandboxGateway } from "../src/sandbox/gateway";

if (process.env.GP_LIVE_VERIFY !== "1") {
  throw new Error("Set GP_LIVE_VERIFY=1 to run browserless Prism proof");
}

const config = resolveMultiSandboxConfig(process.env, new SystemMachineProbe().sample());
const probeProject = config.projectIds[0];
if (!probeProject) {
  throw new Error("Browserless proof requires one dedicated legacy PRISM_PROJECT_IDS seed");
}
const account = await PrismAccountSession.create({
  origin: config.prismOrigin,
  cookieFile: config.cookieFile,
  initialCookieHeader: config.initialCookieHeader,
});
const provisioning = new PrismProvisioningClient({ account });
const started = performance.now();
const credentials = await provisioning.provision(probeProject);
let first = true;
const ydoc = new SandboxYDoc({
  projectId: probeProject,
  mintYSweet: async () => {
    if (first) {
      first = false;
      return credentials.ySweet;
    }
    return provisioning.mintYSweet(probeProject, {
      source: "proof-reconnect",
      bootstrapAttempt: 1,
      previouslyConnected: true,
      sandboxUrl: credentials.sandbox.url,
      sandboxId: credentials.sandbox.sandboxId ?? null,
      sandboxSessionId: credentials.sandbox.sessionId ?? null,
    });
  },
});
await ydoc.connect();
const gateway = new SandboxGateway({
  projectId: probeProject,
  account,
  sandbox: credentials.sandbox,
});
const heartbeat = await gateway.heartbeat();
console.log(JSON.stringify({
  browserless: true,
  provisioned: true,
  resourceTokenPresent: credentials.resource.token.reveal().length > 20,
  ySweetTokenLength: credentials.ySweet.token.reveal().length,
  yjsConnected: ydoc.snapshot().connected,
  heartbeatStatus: heartbeat.status,
  heartbeatOk: heartbeat.ok,
  durationMs: Math.round(performance.now() - started),
}, null, 2));
await ydoc.destroy();
