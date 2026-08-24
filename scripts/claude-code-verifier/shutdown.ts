import type { IncomingMessage, Server } from "node:http";
import type { Duplex } from "node:stream";
import { positiveSafeInteger } from "./shared";

const DEFAULT_SHUTDOWN_TIMEOUT_MS = 1_000;

export function listen(server: Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    server.once("error", onError);
    server.listen(port, "127.0.0.1", () => {
      server.off("error", onError);
      resolve();
    });
  });
}

export function trackVerifierUpgrades(server: Pick<Server, "on" | "off">): {
  close(): void;
} {
  const sockets = new Set<Duplex>();
  const track = (socket: Duplex): void => {
    if (sockets.has(socket)) return;
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  };
  const onConnection = (socket: Duplex): void => track(socket);
  const onUpgrade = (_request: IncomingMessage, socket: Duplex): void => {
    track(socket);
  };
  server.on("connection", onConnection);
  server.on("upgrade", onUpgrade);
  let closed = false;
  return {
    close(): void {
      if (closed) return;
      closed = true;
      server.off("connection", onConnection);
      server.off("upgrade", onUpgrade);
      for (const socket of sockets) socket.destroy();
      sockets.clear();
    },
  };
}

export async function closeVerifierServer(
  server: Pick<Server, "close">,
  sockets: { close(): void },
  timeoutMs = DEFAULT_SHUTDOWN_TIMEOUT_MS,
): Promise<void> {
  positiveSafeInteger(timeoutMs, "shutdown timeout");
  let timer: ReturnType<typeof setTimeout> | undefined;
  const closed = new Promise<void>(resolve => {
    try {
      server.close(() => resolve());
    } catch {
      resolve();
    }
  });
  const timeout = new Promise<void>(resolve => {
    timer = setTimeout(resolve, timeoutMs);
  });
  try {
    await Promise.race([closed, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    sockets.close();
  }
}
