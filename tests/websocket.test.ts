import { afterEach, describe, expect, it } from "bun:test";
import { createServer, type Server } from "node:http";
import { WebSocket } from "ws";
import { attachResponsesWebSocket, type WebSocketRelayStarter } from "../src/ws/server";
import type { RunHandle } from "../src/relay/run-handle";

const servers: Server[] = [];
const sockets: WebSocket[] = [];

afterEach(async () => {
  for (const socket of sockets.splice(0)) socket.close();
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
});

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function relayHandle(final: Promise<any>, frames: Array<{ event: string; data: string }> = []): RunHandle {
  return {
    async *events() { for (const frame of frames) yield frame; },
    final,
    stats: { bytes: 0, firstByteMs: null, frames: frames.length },
  };
}

function completed(id: string, output: unknown[] = []) {
  return { id, object: "response", model: "gpt-5.6-sol", status: "completed", output };
}

async function startServer(startRelay: WebSocketRelayStarter, options: { maxActive?: number; maxNamedStreams?: number } = {}) {
  const server = createServer((_req, res) => { res.writeHead(404).end(); });
  attachResponsesWebSocket(server, { startRelay, ...options });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  const socket = new WebSocket(`ws://127.0.0.1:${address.port}/v1/responses`);
  sockets.push(socket);
  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });
  return socket;
}

function collect(socket: WebSocket) {
  const messages: any[] = [];
  socket.on("message", data => messages.push(JSON.parse(data.toString())));
  return messages;
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for websocket state");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

describe("Responses WebSocket server", () => {
  it("sends direct event objects, makes streaming implicit, and echoes only named stream IDs", async () => {
    const bodies: Record<string, unknown>[] = [];
    const startRelay: WebSocketRelayStarter = async body => {
      bodies.push(body);
      const id = `resp_${bodies.length}`;
      return relayHandle(Promise.resolve(completed(id)), [
        { event: "response.created", data: JSON.stringify({ type: "response.created", response: { id } }) },
        { event: "response.output_text.delta", data: JSON.stringify({ type: "response.output_text.delta", delta: "hi" }) },
      ]);
    };
    const socket = await startServer(startRelay);
    const messages = collect(socket);
    socket.send(JSON.stringify({
      type: "response.create",
      model: "gpt-5.6-sol",
      store: false,
      input: "default",
      reasoning: {
        effort: "max",
        mode: "pro",
        context: "all_turns",
        summary: "concise",
      },
      service_tier: "fast",
    }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "agent-1", model: "gpt-5.6-sol", store: false, input: "named" }));

    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 2);
    expect(messages.every(message => !("parsed" in message))).toBe(true);
    const defaultEvents = messages.filter(message => message.response?.id === "resp_1" || message.delta === "hi" && !("stream_id" in message));
    expect(defaultEvents.some(message => !("stream_id" in message))).toBe(true);
    expect(messages.filter(message => message.stream_id === "agent-1").length).toBeGreaterThan(0);
    expect(bodies).toHaveLength(2);
    expect(bodies[0]).toMatchObject({
      stream: true,
      store: false,
      reasoning: {
        effort: "max",
        mode: "pro",
        context: "all_turns",
        summary: "concise",
      },
      service_tier: "fast",
    });
    expect(bodies[0]).not.toHaveProperty("stream_id");
    expect(bodies[1]).not.toHaveProperty("stream_id");
  });

  it("accepts Codex clients that redundantly send stream:true", async () => {
    const bodies: Record<string, unknown>[] = [];
    const socket = await startServer(async body => {
      bodies.push(body);
      return relayHandle(Promise.resolve(completed("resp_codex")));
    });
    const messages = collect(socket);

    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "codex",
      model: "gpt-5.6-sol",
      store: false,
      stream: true,
      input: "hello from Codex",
    }));

    await waitFor(() => messages.some(message =>
      message.type === "response.completed" || message.type === "error"
    ));
    expect(messages.some(message => message.type === "error")).toBe(false);
    expect(messages.some(message =>
      message.type === "response.completed" && message.stream_id === "codex"
    )).toBe(true);
    expect(bodies).toHaveLength(1);
    expect(bodies[0]).toMatchObject({ stream: true, store: false });
    expect(bodies[0]).not.toHaveProperty("stream_id");
  });

  it("serializes the same lane while different lanes overlap", async () => {
    const gates = new Map<string, ReturnType<typeof deferred<any>>>();
    const started: string[] = [];
    let active = 0;
    let maxActive = 0;
    const startRelay: WebSocketRelayStarter = async body => {
      const label = (body.input as any[]).at(-1).content[0].text as string;
      started.push(label);
      active++;
      maxActive = Math.max(maxActive, active);
      const gate = deferred<any>();
      gates.set(label, gate);
      const final = gate.promise.finally(() => { active--; });
      return relayHandle(final);
    };
    const socket = await startServer(startRelay, { maxActive: 16 });
    const messages = collect(socket);
    const create = (stream_id: string, text: string) => socket.send(JSON.stringify({
      type: "response.create", stream_id, model: "gpt-5.6-sol", store: false, input: text,
    }));
    create("same", "a1");
    create("same", "a2");
    create("other", "b1");
    await waitFor(() => started.length === 2);
    expect(started).toEqual(["a1", "b1"]);
    expect(maxActive).toBe(2);
    gates.get("a1")!.resolve(completed("resp_a1"));
    await waitFor(() => started.includes("a2"));
    gates.get("b1")!.resolve(completed("resp_b1"));
    gates.get("a2")!.resolve(completed("resp_a2"));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 3);
  });

  it("reconstructs a Codex store:false tool continuation from previous_response_id", async () => {
    const bodies: Record<string, unknown>[] = [];
    const call = { type: "function_call", id: "fc_1", call_id: "call_1", name: "tool", arguments: "{}" };
    const startRelay: WebSocketRelayStarter = async body => {
      bodies.push(body);
      return relayHandle(Promise.resolve(
        bodies.length === 1 ? completed("resp_parent", [call]) : completed("resp_child"),
      ));
    };
    const socket = await startServer(startRelay);
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", stream_id: "main", model: "gpt-5.6-sol", store: false, input: "use tool" }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_parent"));
    socket.send(JSON.stringify({
      type: "response.create",
      stream_id: "main",
      model: "gpt-5.6-sol",
      store: false,
      previous_response_id: "resp_parent",
      input: [{ type: "function_call_output", call_id: "call_1", output: "ok" }],
    }));
    await waitFor(() => messages.some(message => message.response?.id === "resp_child"));
    expect(messages.find(message => message.response?.id === "resp_child").response.previous_response_id).toBe("resp_parent");
    expect(bodies[1]).not.toHaveProperty("previous_response_id");
    expect(JSON.stringify(bodies[1].input)).toContain("function_call");
    expect(JSON.stringify(bodies[1].input)).toContain("function_call_output");
  });

  it("caps active responses per connection and drains queued lanes", async () => {
    const gates: Array<ReturnType<typeof deferred<any>>> = [];
    let active = 0;
    let maxActive = 0;
    const startRelay: WebSocketRelayStarter = async () => {
      active++;
      maxActive = Math.max(maxActive, active);
      const gate = deferred<any>();
      gates.push(gate);
      return relayHandle(gate.promise.finally(() => { active--; }));
    };
    const socket = await startServer(startRelay, { maxActive: 16 });
    const messages = collect(socket);
    for (let index = 0; index < 20; index++) {
      socket.send(JSON.stringify({
        type: "response.create",
        stream_id: `lane-${index}`,
        model: "gpt-5.6-sol",
        store: false,
        input: String(index),
      }));
    }
    await waitFor(() => gates.length === 16);
    expect(maxActive).toBe(16);
    gates.splice(0, 4).forEach((gate, index) => gate.resolve(completed(`first_${index}`)));
    await waitFor(() => gates.length === 16);
    gates.forEach((gate, index) => gate.resolve(completed(`rest_${index}`)));
    await waitFor(() => messages.filter(message => message.type === "response.completed").length === 20);
    expect(maxActive).toBe(16);
  });

  it("does not synthesize a duplicate terminal event", async () => {
    const final = completed("resp_once");
    const socket = await startServer(async () => relayHandle(Promise.resolve(final), [
      { event: "response.completed", data: JSON.stringify({ type: "response.completed", response: final }) },
    ]));
    const messages = collect(socket);
    socket.send(JSON.stringify({ type: "response.create", model: "gpt-5.6-sol", store: false, input: "once" }));
    await waitFor(() => messages.some(message => message.type === "response.completed"));
    await new Promise(resolve => setTimeout(resolve, 20));
    expect(messages.filter(message => message.type === "response.completed")).toHaveLength(1);
  });

  it("returns direct validation errors without closing other lanes", async () => {
    const startRelay: WebSocketRelayStarter = async () => relayHandle(Promise.resolve(completed("ok")));
    const socket = await startServer(startRelay, { maxNamedStreams: 2 });
    const messages = collect(socket);
    socket.send("not json");
    socket.send(JSON.stringify({ type: "response.create", stream_id: "bad space", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "background", background: true, input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "stream", stream: false, input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "one", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "two", input: "x" }));
    socket.send(JSON.stringify({ type: "response.create", stream_id: "three", input: "x" }));
    await waitFor(() => messages.filter(message => message.type === "error").length === 5);
    expect(messages.every(message => !("parsed" in message))).toBe(true);
    expect(messages.some(message => message.error?.param === "stream")).toBe(true);
    expect(messages.some(message => message.error?.code === "websocket_stream_limit_reached")).toBe(true);
    expect(socket.readyState).toBe(WebSocket.OPEN);
  });

  it("aborts queued relay startup and releases session state on disconnect", async () => {
    const aborted = deferred<void>();
    const startRelay: WebSocketRelayStarter = async (_body, options) => {
      options.signal.addEventListener("abort", () => aborted.resolve(), { once: true });
      return relayHandle(new Promise(() => {}), [{ event: "response.created", data: '{"type":"response.created"}' }]);
    };
    const socket = await startServer(startRelay);
    socket.send(JSON.stringify({ type: "response.create", stream_id: "main", model: "gpt-5.6-sol", store: false, input: "long" }));
    await new Promise(resolve => setTimeout(resolve, 20));
    socket.close();
    await Promise.race([
      aborted.promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error("abort not observed")), 1_000)),
    ]);
  });
});
