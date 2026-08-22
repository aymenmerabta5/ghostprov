import * as path from "node:path";
import type { MachineSample } from "../core/ports";
import { projectId, type ProjectId } from "../sandbox/types";

export interface MultiSandboxConfig {
  projectIds: readonly ProjectId[];
  prismOrigin: string;
  cookieFile?: string;
  initialCookieHeader?: string;
  port: number;
  candidate: boolean;
  heartbeatMs: 10_000;
  maxProjects: number;
  machine: MachineSample;
}

function positiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined || value.trim() === "") return fallback;
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) throw new TypeError(`${name} must be a positive integer`);
  return parsed;
}

export function resolveMultiSandboxConfig(
  env: Record<string, string | undefined>,
  machine: MachineSample,
): MultiSandboxConfig {
  const rawProjects = (env.PRISM_PROJECT_IDS ?? env.PRISM_DOC_ID ?? "").trim();
  if (!rawProjects) throw new TypeError("PRISM_PROJECT_IDS or PRISM_DOC_ID is required");
  const projectIds = [...new Set(rawProjects.split(",").map(value => value.trim()).filter(Boolean))].map(projectId);
  const maxProjects = positiveInteger(env.GP_MAX_PROJECTS, Math.max(4, projectIds.length), "GP_MAX_PROJECTS");
  if (projectIds.length > maxProjects) throw new RangeError(`Configured ${projectIds.length} projects exceeds GP_MAX_PROJECTS=${maxProjects}`);

  const cookieFile = env.PRISM_COOKIE_FILE?.trim();
  const initialCookieHeader = env.PRISM_COOKIE?.trim();
  if (!cookieFile && !initialCookieHeader) throw new TypeError("PRISM_COOKIE_FILE or PRISM_COOKIE is required");

  const candidate = env.GP_CANDIDATE === "1";
  const port = positiveInteger(env.PORT, candidate ? 18787 : 8787, "PORT");
  if (candidate && port === 8787) throw new RangeError("Candidate mode must never bind protected port 8787");

  const heartbeatMs = positiveInteger(env.GP_HEARTBEAT_MS, 10_000, "GP_HEARTBEAT_MS");
  if (heartbeatMs !== 10_000) throw new RangeError("GP_HEARTBEAT_MS must remain exactly 10000");

  return {
    projectIds: Object.freeze(projectIds),
    prismOrigin: (env.PRISM_ORIGIN ?? "https://prism.openai.com").replace(/\/+$/, ""),
    cookieFile: cookieFile ? path.resolve(cookieFile) : undefined,
    initialCookieHeader,
    port,
    candidate,
    heartbeatMs: 10_000,
    maxProjects,
    machine,
  };
}
