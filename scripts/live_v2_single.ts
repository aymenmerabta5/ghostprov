import WebSocket from "ws";
import { createBrowserlessSystem } from "../src/server/system";
import { resolveLiveV2Options } from "./live_v2_options";
import { resolveGhostProvApiKey } from "../src/http/api-key";

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}

async function verifyHttp(base: string, model: string, apiKey: string): Promise<number> {
  const started = performance.now();
  const response = await fetch(`${base}/v1/responses`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model,
      store: false,
      stream: true,
      input: "Reply with exactly V2_SINGLE_HTTP_OK",
      reasoning: { effort: "medium" },
    }),
    signal: AbortSignal.timeout(5 * 60_000),
  });
  const raw = await response.text();
  if (!response.ok || !raw.includes("response.completed")) {
    throw new Error(`V2 single HTTP canary failed (${response.status})`);
  }
  return performance.now() - started;
}

async function verifyWebSocket(base: string, model: string, apiKey: string): Promise<number> {
  const url = new URL("/v1/responses", base);
  url.protocol = "ws:";
  const socket = new WebSocket(url, {
    headers: { authorization: `Bearer ${apiKey}` },
  });
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  const started = performance.now();
  try {
    const completed = new Promise<void>((resolve, reject) => {
      socket.on("message", raw => {
        const event = JSON.parse(raw.toString());
        if (event.stream_id !== "single-ws") return;
        if (event.type === "response.completed") resolve();
        if (event.type === "error") reject(new Error(event.error?.message ?? "WebSocket error"));
      });
    });
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "single-ws",
      model,
      store: false,
      input: "Reply with exactly V2_SINGLE_WS_OK",
    }));
    await withTimeout(completed, 5 * 60_000, "V2 single WebSocket");
    return performance.now() - started;
  } finally {
    socket.close();
  }
}

async function verifyImage(base: string, apiKey: string): Promise<number | null> {
  if (process.env.GP_LIVE_IMAGE !== "1") return null;
  const started = performance.now();
  const response = await fetch(`${base}/v1/images/generations`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: "gpt-image-2",
      prompt: "A single blue circle on a white background.",
      n: 1,
      quality: "low",
      size: "1024x1024",
      output_format: "png",
    }),
    signal: AbortSignal.timeout(10 * 60_000),
  });
  const body = await response.json() as any;
  if (!response.ok || typeof body.data?.[0]?.b64_json !== "string") {
    throw new Error(`V2 single image canary failed (${response.status})`);
  }
  return performance.now() - started;
}

export async function main(
  env: Record<string, string | undefined> = process.env,
): Promise<void> {
  const options = resolveLiveV2Options(env);
  const apiKey = resolveGhostProvApiKey(env.GHOSTPROV_API_KEY);
  if (options.projectCount !== 1) {
    throw new Error("Single-project canary requires GP_LIVE_PROJECT_COUNT=1");
  }
  const projects = (env.PRISM_PROJECT_IDS ?? "").split(",").map(value => value.trim()).filter(Boolean);
  if (projects.length !== 1) throw new Error("Single-project canary requires exactly one PRISM_PROJECT_IDS UUID");
  if (!env.PRISM_COOKIE_FILE?.trim()) throw new Error("PRISM_COOKIE_FILE is required");
  const candidateRoot = `http://127.0.0.1:${options.port}`;
  const model = env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
  const cycles: Record<string, unknown>[] = [];
  let failure: unknown = null;
  try {
    for (let cycle = 1; cycle <= 2; cycle++) {
      const system = await createBrowserlessSystem({
        ...env,
        GP_CANDIDATE: "1",
        GP_TARGET_SANDBOXES: "1",
        PORT: String(options.port),
      });
      try {
        const started = performance.now();
        await system.start();
        await system.fleet.waitForReady(1);
        const httpMs = await verifyHttp(candidateRoot, model, apiKey);
        const wsMs = await verifyWebSocket(candidateRoot, model, apiKey);
        const imageMs = await verifyImage(candidateRoot, apiKey);
        cycles.push({
          cycle,
          startupMs: Math.round(performance.now() - started - httpMs - wsMs - (imageMs ?? 0)),
          httpMs: Math.round(httpMs),
          websocketMs: Math.round(wsMs),
          imageMs: imageMs === null ? null : Math.round(imageMs),
          capacity: system.router.snapshot().capacity,
        });
      } finally {
        await system.close();
      }
    }
  } catch (error) {
    failure = error;
  }
  if (failure) throw failure;
  console.log(JSON.stringify({
    live: true,
    projectCount: 1,
    cycles,
    projectsDeleted: 0,
  }, null, 2));
}

if (import.meta.main) await main();
