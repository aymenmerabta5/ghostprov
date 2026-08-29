import {
  assertCandidatePort,
  DEFAULT_CANDIDATE_PORT,
} from "../src/core/ports";
import { resolveGhostProvApiKey } from "../src/http/api-key";

export interface LiveV2Options {
  projectCount: number;
  port: number;
  scale: boolean;
  soak: boolean;
  deleteProjects: false;
  timeoutMs: number;
}

export function resolveLiveV2Options(
  env: Record<string, string | undefined>,
  requirements: { requireScale?: boolean } = {},
): LiveV2Options {
  if (env.GP_LIVE_V2 !== "1") throw new Error("Set GP_LIVE_V2=1 to run V2 live canaries");
  resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  if (env.GP_AUTO_CREATE_PROJECTS === "1") {
    throw new Error("GP_AUTO_CREATE_PROJECTS is unsupported; configure existing projects explicitly");
  }
  const projectCount = Number(env.GP_LIVE_PROJECT_COUNT ?? 1);
  if (!Number.isSafeInteger(projectCount) || projectCount < 1 || projectCount > 4) {
    throw new Error("GP_LIVE_PROJECT_COUNT must be an integer from 1 through 4");
  }
  const port = Number(env.PORT ?? DEFAULT_CANDIDATE_PORT);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error("PORT must be an integer from 1 through 65535");
  }
  assertCandidatePort(port, "V2 live canaries");
  if (port !== DEFAULT_CANDIDATE_PORT) {
    throw new Error(`V2 live canaries are hard-fenced to candidate port ${DEFAULT_CANDIDATE_PORT}`);
  }
  const scale = env.GP_LIVE_SCALE === "1";
  if (requirements.requireScale && !scale) {
    throw new Error("Set GP_LIVE_SCALE=1 to run the V2 fleet canary");
  }
  const timeoutMs = Number(env.GP_LIVE_TIMEOUT_MS ?? 20 * 60_000);
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000) {
    throw new Error("GP_LIVE_TIMEOUT_MS must be an integer of at least 1000ms");
  }
  return Object.freeze({
    projectCount,
    port,
    scale,
    soak: env.GP_LIVE_V2_SOAK === "1",
    deleteProjects: false,
    timeoutMs,
  });
}
