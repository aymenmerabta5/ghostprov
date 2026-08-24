import "dotenv/config";
import { createBrowserlessSystem } from "./server/system";
import { sanitizeDiagnosticMessage } from "./observability/redaction";

export { createBrowserlessSystem } from "./server/system";
export { createGhostProvApp } from "./server/app";
export { createGhostProvServer } from "./server/runtime";

const isMain = process.argv[1] && (
  process.argv[1].endsWith("server.ts") ||
  process.argv[1].endsWith("server.js")
);

export async function runGhostProvMain(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const system = await createBrowserlessSystem(env);
  await system.start();
  console.log(
    `ghostprov browserless router listening on http://127.0.0.1:${system.config.port} ` +
    `projects=${system.config.projectIds.length}`,
  );

  process.send?.({
    type: "ghostprov.ready",
    pid: process.pid,
    port: system.config.port,
  });

  let closing: Promise<void> | null = null;
  let intentionalDisconnect = false;
  const close = (reason: string, exitCode = 0): Promise<void> => {
    if (closing) return closing;
    closing = (async () => {
      try {
        await system.close();
      } catch (error) {
        exitCode ||= 1;
        console.error(`[ghostprov] shutdown failed: ${sanitizeDiagnosticMessage(
          error instanceof Error ? `${error.name}: ${error.message}` : error,
        )}`);
      } finally {
        process.send?.({
          type: "ghostprov.stopped",
          pid: process.pid,
          reason: reason.slice(0, 64),
          exitCode,
        });
        if (process.connected) {
          intentionalDisconnect = true;
          process.disconnect?.();
        }
        process.exitCode = exitCode;
      }
    })();
    return closing;
  };

  const requestClose = (reason: string, exitCode = 0) => {
    if (closing) {
      process.exit(exitCode || 1);
      return;
    }
    void close(reason, exitCode);
  };
  process.once("SIGINT", () => requestClose("SIGINT"));
  process.once("SIGTERM", () => requestClose("SIGTERM"));
  process.once("SIGHUP", () => requestClose("SIGHUP-upgrade", 75));
  process.on("message", message => {
    if (!message || typeof message !== "object") return;
    const value = message as { type?: unknown; reason?: unknown; exitCode?: unknown };
    if (value.type !== "ghostprov.shutdown") return;
    requestClose(
      typeof value.reason === "string" ? value.reason : "supervisor-shutdown",
      Number.isSafeInteger(value.exitCode) ? Number(value.exitCode) : 0,
    );
  });
  if (env.GP_SUPERVISED_CHILD === "1") {
    process.once("disconnect", () => {
      if (!intentionalDisconnect) requestClose("supervisor-disconnected", 1);
    });
  }
  const fatal = (label: string, error: unknown) => {
    console.error(`[ghostprov] ${label}: ${sanitizeDiagnosticMessage(
      error instanceof Error ? `${error.name}: ${error.message}` : error,
    )}`);
    requestClose(label, 1);
  };
  process.once("uncaughtException", error => fatal("uncaughtException", error));
  process.once("unhandledRejection", error => fatal("unhandledRejection", error));
}

if (isMain) {
  await runGhostProvMain();
}
