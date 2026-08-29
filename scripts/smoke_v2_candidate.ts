import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import { createGhostProvApp } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { createFakeV2System } from "../tests/support/fake-v2-system";
import {
  createAuthenticatedTenantScope,
  resolveGhostProvApiKey,
} from "../src/http/api-key";

export function resolveSmokeProtectedRoot(
  env: Record<string, string | undefined>,
): string {
  const raw = env.GP_SMOKE_PROTECTED_ROOT?.trim() || "http://127.0.0.1:8787";
  let value: URL;
  try {
    value = new URL(raw);
  } catch {
    throw new TypeError("GP_SMOKE_PROTECTED_ROOT must be a valid loopback origin");
  }
  if (
    value.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(value.hostname) ||
    value.username ||
    value.password
  ) throw new TypeError("GP_SMOKE_PROTECTED_ROOT must be an HTTP loopback origin");
  if (value.pathname !== "/" || value.search || value.hash) {
    throw new TypeError("GP_SMOKE_PROTECTED_ROOT must contain only an origin");
  }
  return value.origin;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}

async function primaryHealthy(root: string): Promise<void> {
  const response = await fetch(`${root}/health`, { signal: AbortSignal.timeout(10_000) });
  const body = await response.json().catch(() => null) as any;
  if (!response.ok || body?.ok !== true) throw new Error("Protected port 8787 is not healthy");
}

export async function main(): Promise<void> {
  const protectedRoot = resolveSmokeProtectedRoot(process.env);
  const apiKey = resolveGhostProvApiKey(process.env.GHOSTPROV_API_KEY);
  await primaryHealthy(protectedRoot);
  const authorizationScope = createAuthenticatedTenantScope();
  const system = await createFakeV2System({
    runtimes: 1,
    physicalPerRuntime: 8,
    authorizationScope,
  });
  const app = createGhostProvApp({ router: system.router, apiKey, authorizationScope });
  const server = createGhostProvServer({
    app,
    router: system.router,
    apiKey,
    port: 18_787,
    candidate: true,
    authorizationScope,
  });
  try {
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        server.off("error", reject);
        resolve();
      });
    });
    const port = (server.address() as AddressInfo).port;
    if (port === 8_787) throw new Error("Candidate smoke bound protected port 8787");
    const base = `http://127.0.0.1:${port}`;
    const response = await fetch(`${base}/v1/responses`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({ model: "gpt-5.6-sol", input: "V2_SMOKE_HTTP", store: false }),
    });
    const final = await response.json() as any;
    if (!response.ok || final.status !== "completed") throw new Error("Candidate HTTP smoke failed");

    const wsUrl = new URL("/v1/responses", base);
    wsUrl.protocol = "ws:";
    const socket = new WebSocket(wsUrl, {
      headers: { authorization: `Bearer ${apiKey}` },
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", reject);
    });
    try {
      const completed = new Promise<void>((resolve, reject) => {
        socket.on("message", raw => {
          const event = JSON.parse(raw.toString());
          if (event.stream_id !== "smoke") return;
          if (event.type === "response.completed") resolve();
          if (event.type === "error") reject(new Error(event.error?.message ?? "WebSocket smoke failed"));
        });
      });
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id: "smoke",
        model: "gpt-5.6-sol",
        input: "V2_SMOKE_WS",
        store: false,
      }));
      await withTimeout(completed, 5_000, "Candidate WebSocket smoke");
    } finally {
      socket.close();
    }
    console.log(JSON.stringify({
      candidatePort: port,
      http: "completed",
      websocket: "completed",
    }));
  } finally {
    await new Promise<void>(resolve => server.close(() => resolve()));
    await system.close();
  }
  await primaryHealthy(protectedRoot);
}

if (import.meta.main) await main();
