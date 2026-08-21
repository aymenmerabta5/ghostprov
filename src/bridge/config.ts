import * as path from "node:path";
import { fileURLToPath } from "node:url";

export interface BridgeConfig {
  repoRoot: string;
  docId: string;
  prismUrl: string;
  userDataDir: string;
  tokenFile: string;
  envFile: string;
}

export interface ChromiumLike {
  launchPersistentContext: (userDataDir: string, options: Record<string, unknown>) => Promise<any>;
}

function absolute(value: string | undefined, fallback: string): string {
  return path.resolve((value ?? "").trim() || fallback);
}

export function defaultRepoRoot(): string {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
}

export function resolveTokenFile(
  env: Record<string, string | undefined> = process.env,
  repoRoot = defaultRepoRoot(),
): string {
  return absolute(env.GP_TOKEN_FILE, path.join(repoRoot, ".pw", "tokens.json"));
}

export function resolveBridgeConfig(
  env: Record<string, string | undefined> = process.env,
  repoRoot = defaultRepoRoot(),
): BridgeConfig {
  const resolvedRoot = path.resolve(repoRoot);
  const docId = (env.PRISM_DOC_ID ?? "").trim();
  if (!docId) throw new Error("PRISM_DOC_ID is required for the Playwright bridge");
  const userDataDir = absolute(env.GP_USER_DATA_DIR, path.join(resolvedRoot, ".pw"));
  return {
    repoRoot: resolvedRoot,
    docId,
    prismUrl: (env.PRISM_PROJECT_URL ?? "").trim() ||
      `https://prism.openai.com/?u=${encodeURIComponent(docId)}&pg=1&m=main.tex`,
    userDataDir,
    tokenFile: absolute(env.GP_TOKEN_FILE, path.join(userDataDir, "tokens.json")),
    envFile: absolute(env.GP_ENV_FILE, path.join(resolvedRoot, ".env")),
  };
}

export const SAFE_CHROMIUM_ARGS = [
  "--disable-blink-features=AutomationControlled",
  "--disable-session-crashed-bubble",
  "--disable-infobars",
  "--no-first-run",
  "--disable-features=SessionRestore",
  "--disable-restore-session-state",
  "--disable-hang-monitor",
  "--disable-backgrounding-occluded-windows",
  "--disable-dev-shm-usage",
] as const;

export function launchBridgeContext(
  chromium: ChromiumLike,
  config: BridgeConfig,
  headless: boolean,
): Promise<any> {
  return chromium.launchPersistentContext(config.userDataDir, {
    headless,
    viewport: { width: 1280, height: 900 },
    timeout: 60_000,
    args: [...SAFE_CHROMIUM_ARGS],
  });
}
