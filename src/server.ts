import "dotenv/config";
import {
  createBrowserlessSystem,
  type BrowserlessSystem,
} from "./server/system";
import { sanitizeDiagnosticMessage } from "./observability/redaction";

export { createBrowserlessSystem } from "./server/system";
export { createGhostProvApp } from "./server/app";
export { createGhostProvServer } from "./server/runtime";

const isMain = process.argv[1] && (
  process.argv[1].endsWith("server.ts") ||
  process.argv[1].endsWith("server.js")
);

export interface GhostProvWorkerProcess {
  readonly pid: number;
  connected?: boolean;
  exitCode?: number;
  on(event: string, listener: (...args: any[]) => void): unknown;
  off(event: string, listener: (...args: any[]) => void): unknown;
  send?(message: unknown): unknown;
  disconnect?(): void;
}

export interface GhostProvMainHooks {
  readonly process?: GhostProvWorkerProcess;
  readonly createSystem?: (
    env: Record<string, string | undefined>,
    signal: AbortSignal,
  ) => Promise<BrowserlessSystem>;
}

function mergeExitCode(current: number, incoming: number): number {
  if (!Number.isSafeInteger(incoming) || incoming < 0 || incoming > 255) incoming = 1;
  if (incoming === 0) return current;
  if (current === 0) return incoming;
  // A real failure must override the planned-upgrade sentinel. A later clean
  // signal or duplicate IPC request can never downgrade an existing failure.
  if (current === 75 && incoming !== 75) return incoming;
  if (incoming === 75) return current;
  return current;
}

export async function runGhostProvMain(
  env: Record<string, string | undefined> = process.env,
  hooks: GhostProvMainHooks = {},
): Promise<void> {
  const workerProcess = hooks.process ?? process as unknown as GhostProvWorkerProcess;
  const createSystem = hooks.createSystem ?? (
    (input: Record<string, string | undefined>) => createBrowserlessSystem(input)
  );
  const startupController = new AbortController();
  let system: BrowserlessSystem | null = null;
  let systemCreation: Promise<BrowserlessSystem> | null = null;
  let closing: Promise<void> | null = null;
  let requestedExitCode = 0;
  let requestedReason = "shutdown";
  let intentionalDisconnect = false;

  const close = (): Promise<void> => {
    if (closing) return closing;
    startupController.abort(new DOMException("GhostProv worker startup aborted", "AbortError"));
    closing = (async () => {
      try {
        const candidate = system ?? await systemCreation?.catch(() => null) ?? null;
        if (candidate) await candidate.close();
      } catch (error) {
        requestedExitCode = mergeExitCode(requestedExitCode, 1);
        console.error(`[ghostprov] shutdown failed: ${sanitizeDiagnosticMessage(
          error instanceof Error ? `${error.name}: ${error.message}` : error,
        )}`);
      } finally {
        for (const [event, listener] of listeners) workerProcess.off(event, listener);
        try {
          workerProcess.send?.({
            type: "ghostprov.stopped",
            pid: workerProcess.pid,
            reason: requestedReason.slice(0, 64),
            exitCode: requestedExitCode,
          });
        } catch {
          requestedExitCode = mergeExitCode(requestedExitCode, 1);
        }
        if (workerProcess.connected) {
          intentionalDisconnect = true;
          try { workerProcess.disconnect?.(); } catch {
            requestedExitCode = mergeExitCode(requestedExitCode, 1);
          }
        }
        workerProcess.exitCode = requestedExitCode;
      }
    })();
    return closing;
  };

  const requestClose = (reason: string, exitCode = 0): Promise<void> => {
    const merged = mergeExitCode(requestedExitCode, exitCode);
    if (merged !== requestedExitCode || requestedReason === "shutdown") {
      requestedReason = reason;
    }
    requestedExitCode = merged;
    startupController.abort(new DOMException("GhostProv worker shutdown requested", "AbortError"));
    return close();
  };
  const onSigint = () => { void requestClose("SIGINT"); };
  const onSigterm = () => { void requestClose("SIGTERM"); };
  const onSighup = () => { void requestClose("SIGHUP-upgrade", 75); };
  const onMessage = (message: unknown) => {
    if (!message || typeof message !== "object") return;
    const value = message as { type?: unknown; reason?: unknown; exitCode?: unknown };
    if (value.type !== "ghostprov.shutdown") return;
    void requestClose(
      typeof value.reason === "string" ? value.reason : "supervisor-shutdown",
      Number.isSafeInteger(value.exitCode) ? Number(value.exitCode) : 1,
    );
  };
  const onDisconnect = () => {
    if (!intentionalDisconnect) void requestClose("supervisor-disconnected", 1);
  };
  const fatal = (label: string, error: unknown) => {
    console.error(`[ghostprov] ${label}: ${sanitizeDiagnosticMessage(
      error instanceof Error ? `${error.name}: ${error.message}` : error,
    )}`);
    void requestClose(label, 1);
  };
  const onUncaughtException = (error: unknown) => fatal("uncaughtException", error);
  const onUnhandledRejection = (error: unknown) => fatal("unhandledRejection", error);
  const listeners: ReadonlyArray<readonly [string, (...args: any[]) => void]> = [
    ["SIGINT", onSigint],
    ["SIGTERM", onSigterm],
    ["SIGHUP", onSighup],
    ["message", onMessage],
    ["uncaughtException", onUncaughtException],
    ["unhandledRejection", onUnhandledRejection],
    ...(env.GP_SUPERVISED_CHILD === "1" ? [["disconnect", onDisconnect] as const] : []),
  ];
  for (const [event, listener] of listeners) workerProcess.on(event, listener);

  try {
    if (
      env.GP_SUPERVISED_CHILD === "1" &&
      env.GP_CANDIDATE === "1" &&
      Number(env.PORT) === 8787
    ) {
      throw new RangeError("Supervised candidate child cannot bind protected port 8787");
    }
    if (startupController.signal.aborted) {
      await close();
      return;
    }
    systemCreation = createSystem(env, startupController.signal);
    system = await systemCreation;
    if (startupController.signal.aborted) {
      await close();
      return;
    }
    await system.start();
    if (startupController.signal.aborted) {
      await close();
      return;
    }
    console.log(
      `ghostprov browserless router listening on http://127.0.0.1:${system.config.port} ` +
      `projects=${system.config.projectIds.length}`,
    );
    workerProcess.send?.({
      type: "ghostprov.ready",
      pid: workerProcess.pid,
      port: system.config.port,
      generation: Number(env.GP_SUPERVISOR_GENERATION ?? 0),
    });
  } catch (error) {
    const shutdownRequested = startupController.signal.aborted;
    if (!shutdownRequested) {
      requestedReason = "startup-failed";
      requestedExitCode = mergeExitCode(requestedExitCode, 1);
    }
    await close();
    if (!shutdownRequested) throw error;
  }
}

if (isMain) {
  await runGhostProvMain();
}
