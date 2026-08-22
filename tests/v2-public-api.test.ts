import { describe, expect, it } from "bun:test";
import type { AddressInfo } from "node:net";
import { Buffer } from "node:buffer";
import WebSocket from "ws";
import { parseFrames } from "../src/relay/sse";
import { createGhostProvApp } from "../src/server/app";
import { createGhostProvServer } from "../src/server/runtime";
import { createFakeV2System } from "./support/fake-v2-system";

async function harness() {
  const system = await createFakeV2System({
    runtimes: 1,
    physicalPerRuntime: 16,
  });
  const app = createGhostProvApp({ router: system.router, keepAliveMs: 60_000 });
  const server = createGhostProvServer({
    app,
    router: system.router,
    port: 18_787,
    candidate: true,
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address() as AddressInfo;
  return {
    system,
    baseUrl: `http://127.0.0.1:${address.port}`,
    upstream: system.runtimes[0]!.upstream,
    async close() {
      await new Promise<void>(resolve => server.close(() => resolve()));
      await system.close();
    },
  };
}

async function openSocket(baseUrl: string) {
  const url = new URL("/v1/responses", baseUrl);
  url.protocol = "ws:";
  const socket = new WebSocket(url);
  const messages: any[] = [];
  const waiters = new Set<() => void>();
  socket.on("message", value => {
    messages.push(JSON.parse(value.toString()));
    for (const wake of waiters) wake();
    waiters.clear();
  });
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  return {
    socket,
    messages,
    async next(predicate: (message: any) => boolean, timeoutMs = 5_000): Promise<any> {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const found = messages.find(predicate);
        if (found) return found;
        if (Date.now() >= deadline) throw new Error("WebSocket event timed out");
        await Promise.race([
          new Promise<void>(resolve => waiters.add(resolve)),
          Bun.sleep(Math.min(20, deadline - Date.now())),
        ]);
      }
    },
  };
}

describe("V2 public API compatibility", () => {
  it("streams ordered Responses SSE and preserves reasoning/service fields", async () => {
    const value = await harness();
    try {
      const response = await fetch(`${value.baseUrl}/v1/responses`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-5.6-sol",
          input: "hello-v2",
          stream: true,
          store: false,
          reasoning: { effort: "max", mode: "pro" },
          service_tier: "fast",
        }),
      });
      const raw = await response.text();
      const events = parseFrames(raw, 0).frames;

      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toContain("text/event-stream");
      expect(events.map(event => event.event)).toEqual([
        "response.created",
        "response.output_text.delta",
        "response.completed",
      ]);
      expect(value.upstream.receivedBodies[0]).toMatchObject({
        stream: true,
        store: false,
        reasoning: { effort: "max", mode: "pro" },
        service_tier: "fast",
      });
    } finally {
      await value.close();
    }
  });

  it("accepts Codex stream:true compatibility and reconstructs store:false continuation", async () => {
    const value = await harness();
    const client = await openSocket(value.baseUrl);
    try {
      client.socket.send(JSON.stringify({
        type: "response.create",
        stream_id: "codex-prewarm",
        model: "gpt-5.6-sol",
        input: "prewarm",
        stream: true,
        generate: false,
      }));
      const prewarmed = await client.next(message => message.stream_id === "codex-prewarm");
      expect(prewarmed).toMatchObject({
        type: "response.completed",
        response: { status: "completed" },
      });

      client.socket.send(JSON.stringify({
        type: "response.create",
        stream_id: "invalid-stream",
        model: "gpt-5.6-sol",
        input: "invalid",
        stream: false,
      }));
      const rejected = await client.next(message => message.stream_id === "invalid-stream");
      expect(rejected).toMatchObject({
        type: "error",
        error: { code: "unsupported_parameter", param: "stream" },
      });

      const tool = {
        type: "function",
        name: "v2_echo",
        description: "Echo one value",
        parameters: {
          type: "object",
          properties: { value: { type: "string" } },
          required: ["value"],
          additionalProperties: false,
        },
        strict: true,
      };
      client.socket.send(JSON.stringify({
        type: "response.create",
        stream_id: "tool-lane",
        model: "gpt-5.6-sol",
        store: false,
        input: "call the tool",
        tools: [tool],
        tool_choice: { type: "function", name: "v2_echo" },
      }));
      const first = await client.next(message => (
        message.stream_id === "tool-lane" && message.type === "response.completed"
      ));
      const call = first.response.output.find((item: any) => item.type === "function_call");
      expect(call.call_id).toBe("call_fake_1");

      client.messages.length = 0;
      client.socket.send(JSON.stringify({
        type: "response.create",
        stream_id: "tool-lane",
        store: false,
        previous_response_id: first.response.id,
        input: [{
          type: "function_call_output",
          call_id: call.call_id,
          output: "V2_TOOL_OK",
        }],
        tools: [tool],
        tool_choice: "auto",
      }));
      await client.next(message => (
        message.stream_id === "tool-lane" && message.type === "response.completed"
      ));
      const continuation = value.upstream.receivedBodies.at(-1)!;
      expect(continuation).not.toHaveProperty("previous_response_id");
      expect(continuation).not.toHaveProperty("stream_id");
      expect(continuation.stream).toBe(true);
      expect(JSON.stringify(continuation.input)).toContain("function_call_output");
      expect(JSON.stringify(continuation.input)).toContain("call_fake_1");
    } finally {
      client.socket.close();
      await value.close();
    }
  });

  it("routes GPT Image generation and multipart editing through V2", async () => {
    const value = await harness();
    try {
      const generation = await fetch(`${value.baseUrl}/v1/images/generations`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          model: "gpt-image-2",
          prompt: "one pixel",
          n: 2,
          quality: "low",
          size: "1024x1024",
          output_format: "png",
        }),
      });
      const generated = await generation.json() as any;
      expect(generation.status).toBe(200);
      expect(generated.data).toHaveLength(2);
      expect(generated.data.every((image: any) => typeof image.b64_json === "string")).toBe(true);

      const png = Buffer.from(generated.data[0].b64_json, "base64");
      const form = new FormData();
      form.append("model", "gpt-image-2");
      form.append("prompt", "edit one pixel");
      form.append("quality", "low");
      form.append("size", "1024x1024");
      form.append("output_format", "png");
      form.append("image", new File([png], "source.png", { type: "image/png" }));
      const edit = await fetch(`${value.baseUrl}/v1/images/edits`, {
        method: "POST",
        body: form,
      });
      const edited = await edit.json() as any;
      expect(edit.status).toBe(200);
      expect(edited.data[0].b64_json).toBe(generated.data[0].b64_json);

      const bodies = value.upstream.receivedBodies;
      expect(bodies.filter(body => (
        Array.isArray(body.tools) && (body.tools as any[])[0]?.type === "image_generation"
      ))).toHaveLength(3);
      const editBody = bodies.at(-1)!;
      expect((editBody.tools as any[])[0]).toMatchObject({
        type: "image_generation",
        model: "gpt-image-2",
        action: "edit",
      });
      expect(JSON.stringify(editBody.input)).toContain("data:image/png;base64,");
    } finally {
      await value.close();
    }
  });
});
