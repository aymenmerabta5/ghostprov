import path from "node:path";
import { stat } from "node:fs/promises";
import { PrismAccountSession } from "../src/prism/account-session";
import { PrismProvisioningClient } from "../src/prism/provisioning-client";
import { SandboxGateway } from "../src/sandbox/gateway";
import { SandboxHeartbeat } from "../src/sandbox/heartbeat";
import { projectId, type ProjectId } from "../src/sandbox/types";
import { SandboxYDoc } from "../src/sandbox/ydoc";
import { YSweetAwarenessChannel } from "../src/v2/awareness/y-sweet-channel";
import { V2BootstrapController } from "../src/v2/bootstrap/controller";
import type { ProtocolBinding } from "../src/v2/protocol/shared";
import { parseAwarenessEnvelope } from "../src/v2/protocol/validate";
import { loadV2SidecarAssets } from "../src/v2/sidecar/assets";

const PROTECTED_ROOT = "http://127.0.0.1:8787";

export function sanitizeV2BootstrapRenderLog(value: string): string {
  const sanitized = value
    .replace(/\bBearer\s+[^\s"']+/gi, "Bearer [REDACTED]")
    .replace(/\b(?:sk-|gAAAA|eyJ)[A-Za-z0-9._=-]{16,}/g, "[REDACTED]")
    .replace(/((?:token|key|secret|authorization)\s*[=:]\s*)[^\s,"']+/gi, "$1[REDACTED]")
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, "?");
  if (sanitized.length <= 8_192) return sanitized;
  return `${sanitized.slice(0, 4_000)}\n...[truncated]...\n${sanitized.slice(-4_000)}`;
}

export interface V2BootstrapProbeOptions {
  projectId: ProjectId;
  cookieFile: string;
  prismOrigin: string;
  port: number;
  timeoutMs: number;
}

export function resolveV2BootstrapProbeOptions(
  env: Record<string, string | undefined>,
): V2BootstrapProbeOptions {
  if (env.GP_LIVE_V2_BOOTSTRAP !== "1") {
    throw new Error("Set GP_LIVE_V2_BOOTSTRAP=1 to run the live V2 bootstrap probe");
  }
  if (env.GP_CANDIDATE !== "1") {
    throw new Error("Set GP_CANDIDATE=1; the V2 bootstrap probe is candidate-only");
  }
  const rawProject = env.GP_LIVE_V2_PROJECT_ID?.trim();
  if (!rawProject) throw new Error("GP_LIVE_V2_PROJECT_ID is required");
  if (rawProject.includes(",")) {
    throw new Error("GP_LIVE_V2_PROJECT_ID must contain exactly one explicit project");
  }
  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  if (!cookieFile) throw new Error("PRISM_COOKIE_FILE is required");
  const port = Number(env.PORT ?? 18_787);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error("PORT must be an integer from 1 through 65535");
  }
  if (port === 8_787) throw new Error("V2 bootstrap probe refuses protected port 8787");
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 120_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000) {
    throw new Error("GP_LIVE_TIMEOUT_MS must be an integer of at least 1000ms");
  }
  return {
    projectId: projectId(rawProject),
    cookieFile: path.resolve(cookieFile),
    prismOrigin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    port,
    timeoutMs,
  };
}

async function protectedHealth(): Promise<Record<string, unknown>> {
  const response = await fetch(`${PROTECTED_ROOT}/health`, {
    signal: AbortSignal.timeout(10_000),
  });
  if (!response.ok) throw new Error(`Protected GhostProv health failed (${response.status})`);
  const body = await response.json();
  if (!body || typeof body !== "object" || (body as Record<string, unknown>).ok !== true) {
    throw new Error("Protected GhostProv did not report healthy");
  }
  return body as Record<string, unknown>;
}

async function assertCookieFile(file: string): Promise<void> {
  const metadata = await stat(file);
  if (!metadata.isFile()) throw new Error("PRISM_COOKIE_FILE must point to an existing file");
}

async function assertNoCurrentSecretValues(
  ydoc: SandboxYDoc,
  secrets: readonly string[],
): Promise<void> {
  const live = (await ydoc.listFiles()).filter(file => !file.deleted);
  for (const file of live) {
    const content = await ydoc.readFile(file.filename);
    if (content === null) continue;
    if (secrets.some(secret => secret.length > 0 && content.includes(secret))) {
      throw new Error(`Tracked Yjs value contains credential material: ${file.filename}`);
    }
  }
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolveV2BootstrapProbeOptions(env);
  await assertCookieFile(options.cookieFile);
  const primaryBefore = await protectedHealth();
  const account = await PrismAccountSession.create({
    origin: options.prismOrigin,
    cookieFile: options.cookieFile,
  });
  const provisioning = new PrismProvisioningClient({ account });
  const abort = AbortSignal.timeout(options.timeoutMs);
  const live: {
    gateway: SandboxGateway | null;
    heartbeat: SandboxHeartbeat | null;
  } = { gateway: null, heartbeat: null };
  let ydoc: SandboxYDoc | null = null;
  let awareness: YSweetAwarenessChannel<any, any> | null = null;
  let controller: V2BootstrapController | null = null;
  let failure: unknown = null;
  let result: Record<string, unknown> | null = null;

  try {
    const credentials = await provisioning.provision(
      options.projectId,
      abort,
      {
        onSandboxAcquired: sandbox => {
          live.gateway = new SandboxGateway({
            projectId: options.projectId,
            account,
            sandbox,
          });
          live.heartbeat = new SandboxHeartbeat({
            projectId: options.projectId,
            gateway: live.gateway,
            initialDelayMs: 10_000,
          });
          live.heartbeat.start();
        },
      },
    );
    const gateway = live.gateway;
    if (!gateway) throw new Error("Sandbox gateway was not initialized");
    const sessionId = credentials.sandbox.sessionId ?? (await gateway.heartbeat(abort)).sessionId;
    if (!sessionId) throw new Error("Provisioned sandbox did not expose a session ID");

    let initialYSweet = true;
    ydoc = new SandboxYDoc({
      projectId: options.projectId,
      mintYSweet: async () => {
        if (initialYSweet) {
          initialYSweet = false;
          return credentials.ySweet;
        }
        return provisioning.mintYSweet(options.projectId, {
          source: "v2-bootstrap-probe-reconnect",
          bootstrapAttempt: 1,
          previouslyConnected: true,
          sandboxUrl: credentials.sandbox.url,
          sandboxId: credentials.sandbox.sandboxId ?? null,
          sandboxSessionId: sessionId,
        }, abort);
      },
    });
    await ydoc.connect();
    const assets = await loadV2SidecarAssets();
    const binding: ProtocolBinding = {
      projectId: options.projectId,
      sessionId,
      leaderGeneration: 1,
      buildHash: assets.buildHash,
      schemaHash: assets.schemaHash,
    };
    awareness = new YSweetAwarenessChannel({
      envelope: credentials.ySweet,
      localPeerId: "host-bootstrap-probe",
      field: "ghostprovV2",
      parseRemote: value => parseAwarenessEnvelope(value, binding, "sidecar"),
      syncTimeoutMs: Math.min(options.timeoutMs, 30_000),
    });
    controller = new V2BootstrapController({
      projectId: options.projectId,
      workerIndex: 1,
      sandboxSessionId: sessionId,
      leaderGeneration: 1,
      ySweet: credentials.ySweet,
      ydoc,
      gateway,
      awareness,
      assets,
      timeoutMs: Math.min(options.timeoutMs, 45_000),
    });
    const started = performance.now();
    const descriptor = await controller.ensureReady(abort);
    const pingMs = await controller.ping(abort);
    await assertNoCurrentSecretValues(ydoc, [
      credentials.ySweet.token.reveal(),
      credentials.resource.token.reveal(),
      credentials.sandbox.token.reveal(),
    ]);
    await controller.drainAndStop("live-bootstrap-probe", abort);
    result = {
      projectId: options.projectId,
      instanceId: descriptor.instanceId,
      buildHash: descriptor.buildHash,
      schemaHash: descriptor.schemaHash,
      capacity: descriptor.capacity,
      bootstrapMs: Math.round(performance.now() - started),
      awarenessPingMs: Math.round(pingMs),
      heartbeatAttempts: live.heartbeat?.snapshot().attemptCount ?? 0,
      plaintextCredentialMatches: 0,
      projectDeleted: false,
    };
  } catch (error) {
    failure = error;
    if (live.gateway) {
      try {
        const logs = await live.gateway.fetchText("logs", { method: "GET" });
        console.error(JSON.stringify({
          v2BootstrapRenderStatus: logs.status,
          v2BootstrapRenderLog: sanitizeV2BootstrapRenderLog(logs.text),
        }));
      } catch (diagnosticError) {
        console.error(JSON.stringify({
          v2BootstrapDiagnosticError: diagnosticError instanceof Error
            ? diagnosticError.message.slice(0, 512)
            : "unknown diagnostic failure",
        }));
      }
    }
  } finally {
    if (controller) {
      await controller.drainAndStop("live-bootstrap-probe-cleanup").catch(() => undefined);
    }
    await awareness?.close().catch(() => undefined);
    await ydoc?.destroy().catch(() => undefined);
    await live.heartbeat?.stop().catch(() => undefined);
    await account.flush().catch(() => undefined);
  }

  let afterFailure: unknown = null;
  let primaryAfter: Record<string, unknown> | null = null;
  try {
    primaryAfter = await protectedHealth();
  } catch (error) {
    afterFailure = error;
  }
  if (failure && afterFailure) {
    throw new AggregateError([failure, afterFailure], "V2 bootstrap probe and protected health both failed");
  }
  if (failure) throw failure;
  if (afterFailure) throw afterFailure;
  console.log(JSON.stringify({
    ...result,
    primary8787HealthyBefore: primaryBefore.ok === true,
    primary8787HealthyAfter: primaryAfter?.ok === true,
  }, null, 2));
}

if (import.meta.main) await main();
