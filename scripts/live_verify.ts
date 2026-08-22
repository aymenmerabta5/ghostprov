import { WebSocket } from "ws";
import { Buffer } from "node:buffer";

if (process.env.GP_LIVE_VERIFY !== "1") {
  throw new Error("Live verification mutates Prism state. Set GP_LIVE_VERIFY=1 explicitly.");
}

const base = (process.env.OPENAI_BASE_URL ?? "http://127.0.0.1:8787/v1").replace(/\/$/, "");
const model = process.env.GP_LIVE_MODEL ?? "gpt-5.6-sol";
const timeoutMs = Number(process.env.GP_LIVE_TIMEOUT_MS ?? 10 * 60_000);

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    promise.then(
      value => { clearTimeout(timer); resolve(value); },
      error => { clearTimeout(timer); reject(error); },
    );
  });
}

async function verifyHealth(): Promise<void> {
  const health = new URL(base);
  health.pathname = "/health";
  const response = await fetch(health);
  if (!response.ok || !(await response.json() as any).ok) throw new Error(`health failed: ${response.status}`);
}

async function verifyHttpStreaming(): Promise<void> {
  const started = performance.now();
  const response = await fetch(`${base}/responses`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer live-verify" },
    body: JSON.stringify({
      model,
      store: false,
      stream: true,
      input: "Reply with exactly LIVE_HTTP_OK",
      reasoning: { effort: "max", mode: "pro", summary: "concise" },
      service_tier: "fast",
    }),
  });
  if (!response.ok || !response.body) throw new Error(`stream request failed: ${response.status} ${await response.text()}`);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let firstByteMs: number | null = null;
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (firstByteMs === null) firstByteMs = performance.now() - started;
    text += decoder.decode(value, { stream: true });
  }
  if (firstByteMs === null || !text.includes("response.completed")) throw new Error("HTTP stream produced no completed event");
  console.log(`[live] HTTP first event ${Math.round(firstByteMs)}ms`);
}

async function verifyImageGeneration(): Promise<void> {
  if (process.env.GP_LIVE_IMAGE !== "1") return;
  const started = performance.now();
  const response = await fetch(`${base}/images/generations`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer live-verify" },
    body: JSON.stringify({
      model: "gpt-image-2",
      prompt: "A minimal solid red circle centered on a white background.",
      quality: "low",
      size: "1024x1024",
      output_format: "png",
      background: "opaque",
      n: 1,
    }),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`image request failed: ${response.status} ${text}`);
  const image = JSON.parse(text);
  const encoded = image.data?.[0]?.b64_json;
  if (typeof encoded !== "string" || encoded.length === 0) {
    throw new Error(`native image result missing: ${JSON.stringify({ data: image.data?.length, outputFormat: image.output_format })}`);
  }
  const png = Uint8Array.from(Buffer.from(encoded, "base64"));
  if (png.length < 8 || png[0] !== 0x89 || png[1] !== 0x50 || png[2] !== 0x4e || png[3] !== 0x47) {
    throw new Error("native image generation did not return PNG bytes");
  }
  console.log(`[live] native gpt-image-2 generation returned ${encoded.length} base64 chars in ${Math.round(performance.now() - started)}ms`);

  if (process.env.GP_LIVE_IMAGE_EDIT !== "1") return;
  const editStarted = performance.now();
  const form = new FormData();
  form.append("model", "gpt-image-2");
  form.append("prompt", "Keep the red circle and add a thin blue border around it.");
  form.append("quality", "low");
  form.append("size", "1024x1024");
  form.append("output_format", "png");
  form.append("image", new File([png], "generated.png", { type: "image/png" }));
  const editResponse = await fetch(`${base}/images/edits`, {
    method: "POST",
    headers: { authorization: "Bearer live-verify" },
    body: form,
  });
  const editText = await editResponse.text();
  if (!editResponse.ok) throw new Error(`image edit failed: ${editResponse.status} ${editText}`);
  const edit = JSON.parse(editText);
  const edited = edit.data?.[0]?.b64_json;
  if (typeof edited !== "string" || edited.length === 0) throw new Error("native image edit result missing");
  console.log(`[live] native gpt-image-2 edit returned ${edited.length} base64 chars in ${Math.round(performance.now() - editStarted)}ms`);
}

interface WsHarness {
  socket: WebSocket;
  nextCompleted(streamId: string): Promise<any>;
}

async function openWebSocket(): Promise<WsHarness> {
  const url = new URL(`${base}/responses`);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(url, { headers: { authorization: "Bearer live-verify" } });
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  const pending = new Map<string, { resolve: (event: any) => void; reject: (error: Error) => void }>();
  socket.on("message", data => {
    const event = JSON.parse(data.toString());
    const streamId = event.stream_id ?? "";
    const waiter = pending.get(streamId);
    if (!waiter) return;
    if (event.type === "response.completed") {
      pending.delete(streamId);
      waiter.resolve(event);
    } else if (event.type === "error" || event.type === "response.failed" || event.type === "response.incomplete") {
      pending.delete(streamId);
      waiter.reject(new Error(JSON.stringify(event)));
    }
  });
  return {
    socket,
    nextCompleted(streamId) {
      return new Promise((resolve, reject) => pending.set(streamId, { resolve, reject }));
    },
  };
}

async function verifyWebSocket(): Promise<void> {
  const harness = await openWebSocket();
  try {
    const tool = {
      type: "function",
      name: "gp_live_echo",
      description: "Return the supplied value",
      parameters: {
        type: "object",
        properties: { value: { type: "string" } },
        required: ["value"],
        additionalProperties: false,
      },
      strict: true,
    };
    const firstDone = harness.nextCompleted("codex-tool");
    harness.socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "codex-tool",
      model,
      store: false,
      stream: true,
      input: "Call gp_live_echo with value LIVE_TOOL_OK.",
      tools: [tool],
      tool_choice: { type: "function", name: "gp_live_echo" },
    }));
    const first = await firstDone;
    const call = first.response.output?.find((item: any) => item.type === "function_call");
    if (!call?.call_id) throw new Error(`tool call missing: ${JSON.stringify(first)}`);

    const secondDone = harness.nextCompleted("codex-tool");
    harness.socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "codex-tool",
      model,
      store: false,
      stream: true,
      previous_response_id: first.response.id,
      input: [{ type: "function_call_output", call_id: call.call_id, output: "LIVE_TOOL_OK" }],
      tools: [tool],
      tool_choice: "auto",
    }));
    await secondDone;

    const laneStarted = performance.now();
    const completions: Promise<any>[] = [];
    for (let index = 0; index < 8; index++) {
      const streamId = `live-${index}`;
      completions.push(harness.nextCompleted(streamId));
      harness.socket.send(JSON.stringify({
        type: "response.create",
        stream_id: streamId,
        model,
        store: false,
        stream: true,
        input: `Reply with exactly LANE_${index}`,
      }));
    }
    await Promise.all(completions);
    console.log(`[live] 8 WebSocket lanes completed in ${Math.round(performance.now() - laneStarted)}ms`);
  } finally {
    harness.socket.close();
  }
}

await withTimeout((async () => {
  await verifyHealth();
  await verifyHttpStreaming();
  await verifyWebSocket();
  await verifyImageGeneration();
})(), "live verification");

console.log("[live] verification passed");
