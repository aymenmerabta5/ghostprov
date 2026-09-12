import { execFile } from "node:child_process";
import * as path from "node:path";
import { requestGhostProvShutdown, type RequestGhostProvShutdownOptions } from "../src/host/shutdown-control";

export interface StopGhostProvOptions {
  readonly cwd?: string;
  readonly platform?: NodeJS.Platform;
  readonly requestShutdown?: (cwd: string, options: RequestGhostProvShutdownOptions) => Promise<unknown>;
  readonly powerOff?: () => Promise<void>;
  readonly log?: (message: string) => void;
}

/** No shell, force flag, or positive timeout (which implies force on Windows). */
export function powerOffWindows(): Promise<void> {
  const executable = path.join(process.env.SystemRoot ?? "C:\\Windows", "System32", "shutdown.exe");
  return new Promise((resolve, reject) => {
    execFile(executable, ["/s", "/t", "0"], {
      windowsHide: true,
      timeout: 10_000,
      maxBuffer: 4 * 1024,
    }, error => {
      if (error) reject(new Error("Windows did not accept the shutdown request"));
      else resolve();
    });
  });
}

export async function stopGhostProv(
  args: readonly string[] = process.argv.slice(2),
  options: StopGhostProvOptions = {},
): Promise<void> {
  const log = options.log ?? console.log;
  if (args.length === 1 && args[0] === "--help") {
    log("bun run stop          Stop GhostProv and wait for verified cleanup.\nbun run shutdown:pc   Verify cleanup, then shut down Windows.");
    return;
  }
  if (args.length > 1 || (args.length === 1 && args[0] !== "--poweroff")) {
    throw new Error("Usage: bun run stop [--poweroff]");
  }
  const powerOff = args[0] === "--poweroff";
  if (powerOff && (options.platform ?? process.platform) !== "win32") {
    throw new Error("Automatic PC shutdown is supported only on Windows; use bun run stop");
  }
  log("Waiting for GhostProv to finish active work and verify remote cleanup...");
  await (options.requestShutdown ?? requestGhostProvShutdown)(options.cwd ?? process.cwd(), {
    ...(powerOff ? { afterCleanup: async () => {
      // The requester retains the startup gate across this OS handoff. A new
      // GhostProv process cannot begin startup between proof and poweroff.
      log("GhostProv cleanup completed successfully.");
      log("Shutting down Windows.");
      await (options.powerOff ?? powerOffWindows)();
    } } : {}),
  });
  if (!powerOff) log("GhostProv cleanup completed successfully.");
}

if (import.meta.main) {
  try {
    await stopGhostProv();
  } catch (error) {
    console.error(error instanceof Error ? error.message : "GhostProv shutdown failed");
    console.error("Shutdown command failed; no forced shutdown will be attempted.");
    process.exitCode = 1;
  }
}
