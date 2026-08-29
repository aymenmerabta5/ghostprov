import type { AddressInfo } from "node:net";
import WebSocket from "ws";
import { createGhostProvApp } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { createFakeV2System } from "../tests/support/fake-v2-system";
import {
  createAuthenticatedTenantScope,
  resolveGhostProvApiKey,
} from "../src/http/api-key";
import {
  DEFAULT_CANDIDATE_PORT,
  PROTECTED_SERVICE_PORT,
} from "../src/core/ports";

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}

export async function main(): Promise<void> {
  const apiKey = resolveGhostProvApiKey(process.env.GHOSTPROV_API_KEY);
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
    port: DEFAULT_CANDIDATE_PORT,
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
    if (port === PROTECTED_SERVICE_PORT) {
      throw new Error(`Candidate smoke bound protected port ${PROTECTED_SERVICE_PORT}`);
    }
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
}

if (import.meta.main) await main();
