import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { PassThrough } from "node:stream";
import {
  createForwardingRouter,
  trackVerifierUpgrades,
  validateVerifierOptions,
} from "../scripts/verify_claude_code";

const FINAL = {
  id: "resp_verifier",
  object: "response",
  created_at: 1,
  model: "gpt-5.6-terra",
  status: "completed",
  error: null,
  incomplete_details: null,
  output: [{
    id: "msg_verifier",
    type: "message",
    role: "assistant",
    status: "completed",
    content: [{ type: "output_text", text: "authoritative", annotations: [] }],
  }],
  usage: {
    input_tokens: 1,
    output_tokens: 1,
    total_tokens: 2,
    input_tokens_details: { cached_tokens: 0, cache_write_tokens: 0 },
    output_tokens_details: { reasoning_tokens: 0 },
  },
};

function encodedFrame(event: string, data: unknown): Uint8Array {
  return new TextEncoder().encode(
    `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`,
  );
}

function sseResponse(frames: Array<[string, unknown]>, splitEvery = 11): Response {
  const all = new TextEncoder().encode(frames.map(([event, data]) => (
    `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`
  )).join(""));
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      for (let offset = 0; offset < all.byteLength; offset += splitEvery) {
        controller.enqueue(all.slice(offset, offset + splitEvery));
      }
      controller.close();
    },
  }), {
    status: 200,
    headers: { "content-type": "text/event-stream; charset=utf-8" },
  });
}

function completedFrames(): Array<[string, unknown]> {
  return [
    ["response.created", {
      type: "response.created",
      response: { id: FINAL.id, status: "in_progress" },
    }],
    ["response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "incremental",
    }],
    ["response.completed", { type: "response.completed", response: FINAL }],
  ];
}

async function within<T>(promise: Promise<T>, milliseconds = 100): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error("timed out")), milliseconds);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function rejectedValue(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    throw new Error("expected rejection");
  } catch (error) {
    return error;
  }
}

describe("Claude Code forwarding verifier", () => {
  it("refuses protected listener ports and unsafe upstream roots", () => {
    for (const port of [18787, 8787]) {
      expect(() => validateVerifierOptions({
        port,
        upstream: "http://127.0.0.1:18787",
      })).toThrow("protected");
    }
    for (const upstream of [
      "https://example.com",
      "http://user:password@127.0.0.1:18787",
      "http://@127.0.0.1:18787",
      "http://127.0.0.1:18787/v1",
      "http://127.0.0.1:18787/path/..",
      "http://127.0.0.1:18787?target=other",
      "http://127.0.0.1:18787?",
      "http://127.0.0.1:18787#fragment",
      "http://127.0.0.1:18787#",
      "file:///tmp/verifier",
    ]) {
      expect(() => validateVerifierOptions({ port: 18788, upstream })).toThrow();
    }
    for (const port of [0, -1, 1.5, 65_536, Number.NaN]) {
      expect(() => validateVerifierOptions({
        port,
        upstream: "http://127.0.0.1:18787",
      })).toThrow("port");
    }
    expect(() => validateVerifierOptions({
      port: 18788,
      upstream: "http://localhost:18788",
    })).toThrow("listener");
    expect(validateVerifierOptions({
      port: 18788,
      upstream: "http://127.0.0.1:18787/",
    })).toEqual({ port: 18788, upstream: "http://127.0.0.1:18787" });
    expect(validateVerifierOptions({
      port: 18_889,
      upstream: "http://[::1]:18787",
    })).toEqual({ port: 18_889, upstream: "http://[::1]:18787" });
  });

  it("posts a forced Responses stream and preserves frames plus authoritative final", async () => {
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const router = createForwardingRouter("http://127.0.0.1:19001", {
      fetch: async (input, init) => {
        requests.push({ url: String(input), init });
        return sseResponse(completedFrames(), 7);
      },
    });
    const body = {
      model: "gpt-5.6-terra",
      input: [{ role: "user", content: "hello" }],
      store: false,
      stream: true,
      include: ["reasoning.encrypted_content"],
    };

    const handle = await router.startRelay(body, { source: "anthropic" });
    const events = await Array.fromAsync(handle.events());

    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe("http://127.0.0.1:19001/v1/responses");
    expect(requests[0]?.init?.method).toBe("POST");
    expect(new Headers(requests[0]?.init?.headers).get("accept")).toBe("text/event-stream");
    expect(new Headers(requests[0]?.init?.headers).get("content-type")).toBe("application/json");
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({ ...body, stream: true });
    expect(events).toEqual(completedFrames().map(([event, data]) => ({
      event,
      data: JSON.stringify(data),
    })));
    await expect(handle.final).resolves.toEqual(FINAL);
    expect(handle.stats.frames).toBe(3);
    expect(handle.stats.bytes).toBeGreaterThan(0);
    router.close();
  });

  it("settles a non-streaming final without requiring an event consumer", async () => {
    const router = createForwardingRouter("http://127.0.0.1:19006", {
      maxBufferedBytes: 64,
      fetch: async () => sseResponse(completedFrames(), 7),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: false,
      });

      await expect(within(handle.final)).resolves.toEqual(FINAL);
      await expect(Array.fromAsync(handle.events())).resolves.toEqual([]);
    } finally {
      router.close();
    }
  });

  it("backpressures upstream pulls instead of buffering an SSE transcript", async () => {
    const frames = completedFrames().map(([event, data]) => encodedFrame(event, data));
    let index = 0;
    let pulls = 0;
    const router = createForwardingRouter("http://127.0.0.1:19002", {
      maxBufferedBytes: 64,
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        pull(controller) {
          pulls += 1;
          const frame = frames[index++];
          if (frame) controller.enqueue(frame);
          else controller.close();
        },
      }, { highWaterMark: 0 }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });

    const handle = await router.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
      stream: true,
    });
    for (let attempt = 0; attempt < 20 && pulls < 2; attempt += 1) await Bun.sleep(1);
    expect(pulls).toBe(2);
    await Bun.sleep(5);
    expect(pulls).toBe(2);

    expect(await Array.fromAsync(handle.events())).toHaveLength(3);
    await expect(handle.final).resolves.toEqual(FINAL);
    expect(pulls).toBe(3);
    router.close();
  });

  it("rejects an oversized coalesced read chunk instead of retaining its transcript", async () => {
    const chunk = new TextEncoder().encode(completedFrames().map(([event, data]) => (
      `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`
    )).join(""));
    const router = createForwardingRouter("http://127.0.0.1:19007", {
      maxReadChunkBytes: chunk.byteLength - 1,
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(chunk);
          controller.close();
        },
      }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: false,
      });
      const failure = await within(rejectedValue(handle.final));
      expect(String(failure)).toContain("upstream event stream failed");
    } finally {
      router.close();
    }
  });

  it("propagates cancellation to the upstream request and sanitizes its reason", async () => {
    const controller = new AbortController();
    const observed: { upstreamSignal?: AbortSignal } = {};
    let streamCancelled = false;
    const router = createForwardingRouter("http://127.0.0.1:19003", {
      fetch: async (_input, init) => {
        if (init?.signal) observed.upstreamSignal = init.signal;
        return new Response(new ReadableStream<Uint8Array>({
          pull() {
            return new Promise<void>(() => {});
          },
          cancel() {
            streamCancelled = true;
          },
        }, { highWaterMark: 0 }), {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      },
    });

    const handle = await router.startRelay(
      { model: "gpt-5.6-sol", input: "x" },
      { signal: controller.signal },
    );
    controller.abort(new Error("Authorization: Bearer must-not-leak"));
    const failure = await rejectedValue(handle.final);

    expect(observed.upstreamSignal?.aborted).toBe(true);
    expect(streamCancelled).toBe(true);
    expect(failure).toMatchObject({ name: "AbortError" });
    expect(String(failure)).not.toContain("must-not-leak");
    router.close();
  });

  it("never exposes upstream transport or HTTP error details", async () => {
    const transportRouter = createForwardingRouter("http://127.0.0.1:19004", {
      fetch: async () => {
        throw new Error("token=must-not-leak");
      },
    });
    const transportFailure = await rejectedValue(transportRouter.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
    }));
    expect(String(transportFailure)).toContain("upstream");
    expect(String(transportFailure)).not.toContain("must-not-leak");

    const httpRouter = createForwardingRouter("http://127.0.0.1:19005", {
      fetch: async () => Response.json({
        error: { message: "api_key=must-not-leak" },
      }, { status: 429, headers: { "retry-after": "1" } }),
    });
    const httpFailure = await rejectedValue(httpRouter.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
    }));
    expect(httpFailure).toMatchObject({ status: 429 });
    expect(String(httpFailure)).not.toContain("must-not-leak");
    transportRouter.close();
    httpRouter.close();
  });

  it("closes verifier-owned upgraded sockets without managing another listener", () => {
    const server = new EventEmitter();
    const tracker = trackVerifierUpgrades(server as never);
    const verifierSocket = new PassThrough();
    server.emit("upgrade", {}, verifierSocket, Buffer.alloc(0));

    tracker.close();

    expect(verifierSocket.destroyed).toBe(true);
    const laterSocket = new PassThrough();
    server.emit("upgrade", {}, laterSocket, Buffer.alloc(0));
    expect(laterSocket.destroyed).toBe(false);
    laterSocket.destroy();
  });

  it("is import-safe and contains no process-management path", () => {
    const source = readFileSync(
      resolve(import.meta.dirname, "..", "scripts", "verify_claude_code.ts"),
      "utf8",
    );
    expect(source).toContain("if (import.meta.main)");
    expect(source).toContain('"127.0.0.1"');
    for (const forbidden of [
      "child_process",
      "process.kill",
      "Bun.spawn",
      "Get-NetTCPConnection",
      "taskkill",
    ]) expect(source).not.toContain(forbidden);
  });
});
