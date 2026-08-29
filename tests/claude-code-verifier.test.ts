import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { PassThrough } from "node:stream";
import { parseFrames } from "../src/relay/sse";
import { createGhostProvApp } from "../src/server/app";
import {
  closeVerifierServer,
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

function pendingSseResponse(signal?: AbortSignal | null): Response {
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      const abort = () => controller.error(new DOMException("aborted", "AbortError"));
      if (signal?.aborted) abort();
      else signal?.addEventListener("abort", abort, { once: true });
    },
  }), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function rawSseResponse(raw: string, splitEvery = 11): Response {
  const bytes = new TextEncoder().encode(raw);
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      for (let offset = 0; offset < bytes.byteLength; offset += splitEvery) {
        controller.enqueue(bytes.slice(offset, offset + splitEvery));
      }
      controller.close();
    },
  }), {
    status: 200,
    headers: { "content-type": "text/event-stream" },
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
        upstream: "http://127.0.0.1:18888",
      })).toThrow("protected");
    }
    for (const upstream of [
      "https://example.com",
      "http://127.0.0.1:18787",
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
        upstream: "http://127.0.0.1:18888",
      })).toThrow("port");
    }
    expect(() => validateVerifierOptions({
      port: 18788,
      upstream: "http://localhost:18788",
    })).toThrow("listener");
    expect(validateVerifierOptions({
      port: 18788,
      upstream: "http://127.0.0.1:18888/",
    })).toEqual({ port: 18788, upstream: "http://127.0.0.1:18888" });
    expect(validateVerifierOptions({
      port: 18_889,
      upstream: "http://[::1]:18888",
    })).toEqual({ port: 18_889, upstream: "http://[::1]:18888" });
  });

  it("admits only eight forwards and removes an aborted ninth waiter", async () => {
    let fetches = 0;
    const forwardedInputs: string[] = [];
    const router = createForwardingRouter("http://127.0.0.1:19008", {
      fetch: async (_input, init) => {
        fetches += 1;
        forwardedInputs.push(String(JSON.parse(String(init?.body)).input));
        return pendingSseResponse(init?.signal);
      },
    });
    const activeAbort = new AbortController();
    const handles = await Promise.all(Array.from({ length: 8 }, (_, index) => (
      router.startRelay(
        { model: "gpt-5.6-sol", input: String(index), stream: true },
        index === 0 ? { signal: activeAbort.signal } : undefined,
      )
    )));
    const queuedAbort = new AbortController();
    const queued = router.startRelay(
      { model: "gpt-5.6-sol", input: "queued", stream: true },
      { signal: queuedAbort.signal },
    );
    try {
      await Bun.sleep(0);
      expect(fetches).toBe(8);
      expect(router.snapshot()).toMatchObject({
        capacity: 8,
        active: 8,
        available: 0,
        queued: 1,
      });

      queuedAbort.abort(new Error("token=must-not-leak"));
      const failure = await rejectedValue(queued);
      expect(failure).toMatchObject({ name: "AbortError" });
      expect(String(failure)).not.toContain("must-not-leak");
      expect(router.snapshot().queued).toBe(0);

      const fifoFirst = router.startRelay({
        model: "gpt-5.6-sol",
        input: "fifo-first",
        stream: true,
      });
      const fifoSecond = router.startRelay({
        model: "gpt-5.6-sol",
        input: "fifo-second",
        stream: true,
      });
      await Bun.sleep(0);
      expect(router.snapshot().queued).toBe(2);
      activeAbort.abort();
      const admitted = await within(fifoFirst);
      handles.push(admitted);
      expect(forwardedInputs.at(-1)).toBe("fifo-first");
      expect(router.snapshot().queued).toBe(1);
      router.close();
      await expect(fifoSecond).rejects.toMatchObject({
        status: 503,
        options: { code: "verifier_shutting_down" },
      });
      const activeFailures = await Promise.all(handles.map(handle => rejectedValue(handle.final)));
      expect(activeFailures.every(error => (
        typeof error === "object" && error !== null && (error as { name?: string }).name === "AbortError"
      ))).toBe(true);
      await Bun.sleep(0);
      expect(router.snapshot()).toMatchObject({ capacity: 0, active: 0, queued: 0 });
    } finally {
      router.close();
      await Promise.allSettled(handles.map(handle => handle.final));
    }
  });

  it("rechecks a caller abort after queued permit admission before fetch", async () => {
    let releaseController!: ReadableStreamDefaultController<Uint8Array>;
    const fetchedInputs: string[] = [];
    const router = createForwardingRouter("http://127.0.0.1:19019", {
      fetch: async (_input, init) => {
        const input = String(JSON.parse(String(init?.body)).input);
        fetchedInputs.push(input);
        if (input === "release-active") {
          return new Response(new ReadableStream<Uint8Array>({
            start(controller) {
              releaseController = controller;
            },
          }), {
            status: 200,
            headers: { "content-type": "text/event-stream" },
          });
        }
        return pendingSseResponse(init?.signal);
      },
    });
    const handles = await Promise.all(Array.from({ length: 8 }, (_, index) => (
      router.startRelay({
        model: "gpt-5.6-sol",
        input: index === 0 ? "release-active" : `active-${index}`,
        stream: false,
      })
    )));
    const queuedAbort = new AbortController();
    const queued = router.startRelay(
      { model: "gpt-5.6-sol", input: "abort-during-acquire", stream: false },
      { signal: queuedAbort.signal },
    );
    try {
      await Bun.sleep(0);
      expect(router.snapshot().queued).toBe(1);
      releaseController.enqueue(encodedFrame("response.completed", {
        type: "response.completed",
        response: FINAL,
      }));

      await within(new Promise<void>(resolve => {
        const abortOnceAdmitted = (): void => {
          if (router.snapshot().queued === 0) {
            queuedAbort.abort(new Error("token=must-not-leak"));
            resolve();
            return;
          }
          queueMicrotask(abortOnceAdmitted);
        };
        queueMicrotask(abortOnceAdmitted);
      }));

      const failure = await rejectedValue(queued);
      expect(failure).toMatchObject({ name: "AbortError" });
      expect(String(failure)).not.toContain("must-not-leak");
      expect(fetchedInputs).not.toContain("abort-during-acquire");
    } finally {
      router.close();
      await Promise.allSettled(handles.map(handle => handle.final));
    }
  });

  it("bounds aggregate incoming bodies with an abort-aware FIFO byte budget", async () => {
    const router = createForwardingRouter("http://127.0.0.1:19009", {
      maxIncomingBodyBytes: 10,
      fetch: async () => { throw new Error("not used"); },
    });
    const first = await router.acquireIncomingBody!(6);
    const order: string[] = [];
    const secondPromise = router.acquireIncomingBody!(5).then(lease => {
      order.push("second");
      return lease;
    });
    const thirdPromise = router.acquireIncomingBody!(4).then(lease => {
      order.push("third");
      return lease;
    });
    await Bun.sleep(0);
    expect(router.snapshot().resources).toMatchObject({
      incomingBodyCapacityBytes: 10,
      incomingBodyReservedBytes: 6,
      incomingBodyQueued: 2,
    });
    expect(order).toEqual([]);

    first.release();
    const second = await within(secondPromise);
    const third = await within(thirdPromise);
    expect(order).toEqual(["second", "third"]);
    expect(router.snapshot().resources).toMatchObject({
      incomingBodyReservedBytes: 9,
      incomingBodyQueued: 0,
    });
    const queuedAbort = new AbortController();
    const queued = router.acquireIncomingBody!(2, queuedAbort.signal);
    await Bun.sleep(0);
    queuedAbort.abort(new Error("body-secret"));
    const queuedFailure = await rejectedValue(queued);
    expect(queuedFailure).toMatchObject({ name: "AbortError" });
    expect(String(queuedFailure)).not.toContain("body-secret");
    second.release();
    third.release();
    await expect(router.acquireIncomingBody!(11)).rejects.toMatchObject({ status: 413 });
    router.close();
  });

  it("derives parser concurrency from an exposed aggregate memory ceiling", async () => {
    const router = createForwardingRouter("http://127.0.0.1:19022", {
      maxConcurrentForwards: 4,
      maxForwardWaiters: 1,
      maxEventBytes: 128,
      maxParserMemoryBytes: 1_024,
      forwardTimeoutMs: 1_234,
      fetch: async (_input, init) => pendingSseResponse(init?.signal),
    });
    const handles = await Promise.all([0, 1].map(index => router.startRelay({
      model: "gpt-5.6-sol",
      input: String(index),
      stream: false,
    })));
    const queued = router.startRelay({
      model: "gpt-5.6-sol",
      input: "queued",
      stream: false,
    });
    try {
      await Bun.sleep(0);
      expect(router.snapshot()).toMatchObject({
        capacity: 2,
        active: 2,
        queued: 1,
        resources: {
          forwardQueueCapacity: 1,
          forwardDeadlineMs: 1_234,
          parserEventLimitBytes: 128,
          parserMemoryAmplificationFactor: 4,
          parserMemoryCapacityBytes: 1_024,
          parserMemoryReservationBytesPerForward: 512,
          parserMemoryReservedBytes: 1_024,
        },
      });

      const failure = await rejectedValue(router.startRelay({
        model: "gpt-5.6-sol",
        input: "excess",
        stream: false,
      }));
      expect(failure).toMatchObject({
        status: 503,
        options: { type: "overloaded_error", code: "verifier_overloaded" },
      });
      expect(String(failure)).not.toContain("excess");
    } finally {
      router.close();
      await Promise.allSettled([...handles.map(handle => handle.final), queued]);
    }
  });

  it("bounds incoming-body waiters with the same sanitized overload", async () => {
    const router = createForwardingRouter("http://127.0.0.1:19023", {
      maxIncomingBodyBytes: 1,
      maxIncomingBodyWaiters: 1,
      fetch: async () => { throw new Error("not used"); },
    });
    const first = await router.acquireIncomingBody!(1);
    const queued = router.acquireIncomingBody!(1);
    try {
      await Bun.sleep(0);
      expect(router.snapshot().resources).toMatchObject({
        incomingBodyQueueCapacity: 1,
        incomingBodyQueued: 1,
      });
      const failure = await within(rejectedValue(router.acquireIncomingBody!(1)));
      expect(failure).toMatchObject({
        status: 503,
        options: { type: "overloaded_error", code: "verifier_overloaded" },
      });
    } finally {
      router.close();
      first.release();
      await Promise.allSettled([queued]);
    }
  });

  it("times out response headers through a deterministic internal deadline", async () => {
    let fireDeadline: (() => void) | undefined;
    let cleared = 0;
    const router = createForwardingRouter("http://127.0.0.1:19024", {
      forwardTimeoutMs: 321,
      scheduleDeadline: (callback: () => void, milliseconds: number) => {
        expect(milliseconds).toBe(321);
        fireDeadline = callback;
        return () => { cleared += 1; };
      },
      fetch: async () => new Promise<Response>(() => {}),
    });
    const pending = router.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
      stream: false,
    });
    try {
      await Bun.sleep(0);
      expect(fireDeadline).toBeFunction();
      fireDeadline!();
      const failure = await within(rejectedValue(pending));
      expect(failure).toMatchObject({
        status: 504,
        message: "Claude Code verifier upstream request timed out",
        options: { type: "timeout_error", code: "verifier_forward_timeout" },
      });
      expect(cleared).toBe(1);
    } finally {
      router.close();
      await Promise.allSettled([pending]);
    }
  });

  it("keeps the deadline active until stream completion and clears it", async () => {
    let fireDeadline: (() => void) | undefined;
    let cleared = 0;
    let bodyCancelled = false;
    const router = createForwardingRouter("http://127.0.0.1:19025", {
      forwardTimeoutMs: 654,
      scheduleDeadline: (callback: () => void) => {
        fireDeadline = callback;
        return () => { cleared += 1; };
      },
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        pull() {
          return new Promise<void>(() => {});
        },
        cancel() {
          bodyCancelled = true;
        },
      }, { highWaterMark: 0 }), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });
    const handle = await router.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
      stream: false,
    });
    try {
      expect(fireDeadline).toBeFunction();
      fireDeadline!();
      const failure = await within(rejectedValue(handle.final));
      expect(failure).toMatchObject({
        status: 504,
        options: { type: "timeout_error", code: "verifier_forward_timeout" },
      });
      expect(bodyCancelled).toBe(true);
      expect(cleared).toBe(1);
    } finally {
      router.close();
      await Promise.allSettled([handle.final]);
    }
  });

  it("posts a forced Responses stream and preserves frames plus authoritative final", async () => {
    const requests: Array<{ url: string; init: RequestInit | undefined }> = [];
    const router = createForwardingRouter("http://127.0.0.1:19001", {
      apiKey: "test-ghostprov-key-0123456789abcdef",
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
    expect(new Headers(requests[0]?.init?.headers).get("authorization"))
      .toBe("Bearer test-ghostprov-key-0123456789abcdef");
    expect(JSON.parse(String(requests[0]?.init?.body))).toEqual({
      ...body,
      stream: true,
      store: true,
    });
    expect(events).toEqual(completedFrames().map(([event, data]) => ({
      event,
      data: JSON.stringify(data),
    })));
    await expect(handle.final).resolves.toEqual(FINAL);
    expect(handle.stats.frames).toBe(3);
    expect(handle.stats.bytes).toBeGreaterThan(0);
    router.close();
  });

  it("forwards CR and mixed SSE with comments, multiline data, and split UTF-8", async () => {
    const terminal = `event: response.completed\ndata: ${JSON.stringify({
      type: "response.completed",
      response: FINAL,
    })}\n\n`;
    const raw = ": ignored\revent: custom\r\ndata: 🚀\rdata: second\r\r\n" + terminal;
    const router = createForwardingRouter("http://127.0.0.1:19013", {
      fetch: async () => rawSseResponse(raw, 1),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: true,
      });

      expect(await Array.fromAsync(handle.events())).toEqual([
        { event: "custom", data: "🚀\nsecond" },
        { event: "response.completed", data: JSON.stringify({
          type: "response.completed",
          response: FINAL,
        }) },
      ]);
      await expect(handle.final).resolves.toEqual(FINAL);
    } finally {
      router.close();
    }
  });

  it("dispatches a held-open CR-only terminal at the end of its chunk", async () => {
    let bodyCancelled = false;
    const raw = `event: response.completed\rdata: ${JSON.stringify({
      type: "response.completed",
      response: FINAL,
    })}\r\r`;
    const router = createForwardingRouter("http://127.0.0.1:19020", {
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(raw));
        },
        cancel() {
          bodyCancelled = true;
        },
      }, { highWaterMark: 0 }), {
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

      await expect(within(handle.final)).resolves.toEqual(FINAL);
      expect(bodyCancelled).toBe(true);
    } finally {
      router.close();
    }
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
    const pairs: Array<[string, unknown]> = [
      ["response.output_text.delta", {
        type: "response.output_text.delta",
        delta: "a".repeat(1024),
      }],
      ["response.output_text.delta", {
        type: "response.output_text.delta",
        delta: "b".repeat(1024),
      }],
      completedFrames()[2]!,
    ];
    const frames = pairs.map(([event, data]) => encodedFrame(event, data));
    const maxBufferedBytes = Math.max(...frames.map(frame => frame.byteLength));
    let index = 0;
    let pulls = 0;
    const router = createForwardingRouter("http://127.0.0.1:19002", {
      maxBufferedBytes,
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

  it("rejects a streamed event larger than the hard one MiB queue bound", async () => {
    const oversized = ["response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "x".repeat(1024 * 1024),
    }] as [string, unknown];
    const router = createForwardingRouter("http://127.0.0.1:19010", {
      maxEventBytes: 2 * 1024 * 1024,
      fetch: async () => sseResponse([oversized, completedFrames()[2]!], 64 * 1024),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: true,
      });
      const iterator = handle.events();
      await expect(within(iterator.next())).resolves.toEqual({ value: undefined, done: true });
      await expect(handle.final).rejects.toThrow("upstream event stream failed");
    } finally {
      router.close();
    }
  });

  it("keeps an oversized completed terminal out of the queue and settles final", async () => {
    const largeFinal = structuredClone(FINAL);
    largeFinal.output[0]!.content[0]!.text = "z".repeat(1024 * 1024);
    const delta: [string, unknown] = ["response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "prefix",
    }];
    const router = createForwardingRouter("http://127.0.0.1:19011", {
      maxEventBytes: 2 * 1024 * 1024,
      fetch: async () => sseResponse([
        delta,
        ["response.completed", { type: "response.completed", response: largeFinal }],
      ], 64 * 1024),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: true,
      });
      const events = await Array.fromAsync(handle.events());

      expect(events.map(event => event.event)).toEqual(["response.output_text.delta"]);
      await expect(handle.final).resolves.toEqual(largeFinal);
      expect(handle.stats.frames).toBe(1);
    } finally {
      router.close();
    }
  });

  it("composes a skipped oversized terminal into the matching HTTP event", async () => {
    const largeFinal = structuredClone(FINAL);
    largeFinal.output[0]!.content[0]!.text = "q".repeat(1024 * 1024);
    const router = createForwardingRouter("http://127.0.0.1:19021", {
      maxEventBytes: 2 * 1024 * 1024,
      fetch: async () => sseResponse([
        ["response.output_text.delta", {
          type: "response.output_text.delta",
          sequence_number: 9,
          delta: "prefix",
        }],
        ["response.completed", {
          type: "response.completed",
          sequence_number: 10,
          response: largeFinal,
        }],
      ], 64 * 1024),
    });
    try {
      const response = await createGhostProvApp({ router }).request("/v1/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ model: "gpt-5.6-sol", input: "x", stream: true }),
      });
      const frames = parseFrames(await response.text(), 0).frames;
      const terminal = JSON.parse(frames.at(-1)!.data);

      expect(frames.map(frame => frame.event)).toEqual([
        "response.output_text.delta",
        "response.completed",
      ]);
      expect(terminal).toEqual({
        type: "response.completed",
        sequence_number: 10,
        response: largeFinal,
      });
    } finally {
      router.close();
    }
  });

  it.each(["failed", "incomplete"] as const)(
    "preserves an authoritative response.%s terminal and final",
    async status => {
      const final = { ...structuredClone(FINAL), status };
      const event = `response.${status}`;
      const router = createForwardingRouter("http://127.0.0.1:19017", {
        fetch: async () => sseResponse([[
          event,
          { type: event, response: final },
        ]]),
      });
      try {
        const handle = await router.startRelay({
          model: "gpt-5.6-sol",
          input: "x",
          stream: true,
        });
        expect(await Array.fromAsync(handle.events())).toEqual([{
          event,
          data: JSON.stringify({ type: event, response: final }),
        }]);
        await expect(handle.final).resolves.toEqual(final);
      } finally {
        router.close();
      }
    },
  );

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

  it("fails closed when one SSE event exceeds the parser segment cap", async () => {
    const router = createForwardingRouter("http://127.0.0.1:19012", {
      maxParserSegments: 4,
      fetch: async () => sseResponse(completedFrames(), 1),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: false,
      });
      await expect(handle.final).rejects.toThrow("upstream event stream failed");
    } finally {
      router.close();
    }
  });

  it("fails closed when one SSE event exceeds the parser total-byte cap", async () => {
    const frame: [string, unknown] = ["response.output_text.delta", {
      type: "response.output_text.delta",
      delta: "x".repeat(256),
    }];
    const router = createForwardingRouter("http://127.0.0.1:19018", {
      maxEventBytes: 128,
      fetch: async () => sseResponse([frame], 32),
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: false,
      });
      await expect(handle.final).rejects.toThrow("upstream event stream failed");
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

  it("cancels upstream when iterator return happens before the first next", async () => {
    const observed: { signal?: AbortSignal } = {};
    let cancels = 0;
    const router = createForwardingRouter("http://127.0.0.1:19014", {
      fetch: async (_input, init) => {
        if (init?.signal) observed.signal = init.signal;
        return new Response(new ReadableStream<Uint8Array>({
          pull() {
            return new Promise<void>(() => {});
          },
          cancel() {
            cancels += 1;
          },
        }, { highWaterMark: 0 }), {
          status: 200,
          headers: { "content-type": "text/event-stream" },
        });
      },
    });
    try {
      const handle = await router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
        stream: true,
      });
      const iterator = handle.events();

      await expect(iterator.return(undefined)).resolves.toEqual({ value: undefined, done: true });
      await expect(iterator.return(new Error("return-secret") as never)).resolves.toEqual({
        value: undefined,
        done: true,
      });
      expect(observed.signal?.aborted).toBe(true);
      expect(cancels).toBe(1);
      await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
      const thrown = await rejectedValue(iterator.throw(new Error("token=must-not-leak")));
      expect(thrown).toMatchObject({ name: "AbortError" });
      expect(String(thrown)).not.toContain("must-not-leak");
      await expect(handle.final).rejects.toMatchObject({ name: "AbortError" });
    } finally {
      router.close();
    }
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

  it("cancels status bodies and preserves only safe retry headers", async () => {
    let bodyCancels = 0;
    let bodyPulls = 0;
    const router = createForwardingRouter("http://127.0.0.1:19015", {
      fetch: async () => new Response(new ReadableStream<Uint8Array>({
        pull() {
          bodyPulls += 1;
        },
        cancel() {
          bodyCancels += 1;
        },
      }, { highWaterMark: 0 }), {
        status: 429,
        headers: {
          "retry-after": "12",
          "x-should-retry": "false",
          "x-secret": "must-not-forward",
        },
      }),
    });
    try {
      const failure = await rejectedValue(router.startRelay({
        model: "gpt-5.6-sol",
        input: "x",
      })) as { headers?: Headers };

      expect(failure.headers?.get("retry-after")).toBe("12");
      expect(failure.headers?.get("x-should-retry")).toBe("false");
      expect(failure.headers?.get("x-secret")).toBeNull();
      expect(bodyPulls).toBe(0);
      expect(bodyCancels).toBe(1);
    } finally {
      router.close();
    }

    const unsafe = createForwardingRouter("http://127.0.0.1:19016", {
      fetch: async () => new Response(null, {
        status: 503,
        headers: { "retry-after": "999999", "x-should-retry": "yes" },
      }),
    });
    const unsafeFailure = await rejectedValue(unsafe.startRelay({
      model: "gpt-5.6-sol",
      input: "x",
    })) as { headers?: Headers };
    expect(unsafeFailure.headers?.has("retry-after")).toBe(false);
    expect(unsafeFailure.headers?.has("x-should-retry")).toBe(false);
    unsafe.close();
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

  it("bounds normal-socket shutdown before destroying only verifier-owned sockets", async () => {
    class HangingServer extends EventEmitter {
      closeCalls = 0;
      close(_callback?: (error?: Error) => void): this {
        this.closeCalls += 1;
        return this;
      }
    }
    const server = new HangingServer();
    const tracker = trackVerifierUpgrades(server as never);
    const normalSocket = new PassThrough();
    const upgradedSocket = new PassThrough();
    const unrelatedSocket = new PassThrough();
    server.emit("connection", normalSocket);
    server.emit("upgrade", {}, upgradedSocket, Buffer.alloc(0));

    await within(closeVerifierServer(server as never, tracker, 5), 100);

    expect(server.closeCalls).toBe(1);
    expect(normalSocket.destroyed).toBe(true);
    expect(upgradedSocket.destroyed).toBe(true);
    expect(unrelatedSocket.destroyed).toBe(false);
    unrelatedSocket.destroy();
  });

  it("is import-safe and contains no process-management path", () => {
    const scripts = resolve(import.meta.dirname, "..", "scripts");
    const support = resolve(scripts, "claude-code-verifier");
    const source = [
      readFileSync(resolve(scripts, "verify_claude_code.ts"), "utf8"),
      ...readdirSync(support).map(name => readFileSync(resolve(support, name), "utf8")),
    ].join("\n");
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
