// Deliberately never binds PORT and never emits ghostprov.ready. It exists only
// to prove that an unrelated listener on the allocated port cannot satisfy the
// supervisor readiness boundary.
let closing = false;
const keepAlive = setInterval(() => undefined, 1_000);

function close(exitCode: number): void {
  if (closing) return;
  closing = true;
  clearInterval(keepAlive);
  process.send?.({ type: "ghostprov.stopped", pid: process.pid, exitCode });
  process.exitCode = exitCode;
  if (process.connected) process.disconnect?.();
}

process.on("message", message => {
  if (!message || typeof message !== "object") return;
  const value = message as { type?: unknown; exitCode?: unknown };
  if (value.type === "ghostprov.shutdown") {
    close(Number.isSafeInteger(value.exitCode) ? Number(value.exitCode) : 0);
  }
});
process.once("SIGTERM", () => close(0));
process.once("SIGINT", () => close(0));
process.once("disconnect", () => close(1));
