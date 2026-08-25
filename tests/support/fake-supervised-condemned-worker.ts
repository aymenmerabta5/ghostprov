import { createServer } from "node:http";

const port = Number(process.env.PORT);
const generation = Number(process.env.GP_SUPERVISOR_GENERATION ?? 0);
if (!Number.isSafeInteger(port) || port < 1) throw new Error("fake condemned worker port invalid");

const server = createServer((_request, response) => {
  response.end(JSON.stringify({ generation, condemned: true }));
});
let closing = false;
const delayedReady = setTimeout(() => {
  if (closing) return;
  server.listen(port, "127.0.0.1", () => {
    process.send?.({ type: "ghostprov.ready", pid: process.pid, port, generation });
  });
}, 300);

function close(exitCode: number): void {
  if (closing) return;
  closing = true;
  clearTimeout(delayedReady);
  const finish = () => {
    process.exitCode = exitCode;
    if (process.connected) process.disconnect?.();
  };
  if (server.listening) server.close(finish);
  else finish();
}

// Intentionally ignore the first graceful shutdown long enough to publish a
// late readiness proof for a process the supervisor has already condemned.
let ignoredShutdown = false;
process.on("message", message => {
  if (!message || typeof message !== "object") return;
  const value = message as { type?: unknown; exitCode?: unknown };
  if (value.type !== "ghostprov.shutdown") return;
  if (!ignoredShutdown) {
    ignoredShutdown = true;
    return;
  }
  close(Number.isSafeInteger(value.exitCode) ? Number(value.exitCode) : 0);
});
process.once("SIGTERM", () => close(0));
process.once("SIGINT", () => close(0));
process.once("disconnect", () => close(1));
