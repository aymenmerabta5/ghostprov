import { describe, expect, it } from "bun:test";
import { ApiError } from "../src/http/errors";
import { executeImageBatch } from "../src/images/service";
import type { RunHandle } from "../src/relay/run-handle";
import { V2RunHandle } from "../src/v2/transport/run-handle";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function handle(final: Promise<unknown>): RunHandle {
  return {
    async *events() {},
    final,
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

function imageFinal(
  result: string,
  options: {
    createdAt?: number;
    revisedPrompt?: string;
    inputTokens?: number;
    imageInputTokens?: number;
    textInputTokens?: number;
    outputTokens?: number;
    imageOutputTokens?: number;
    textOutputTokens?: number;
    callStatus?: string;
  } = {},
) {
  const inputTokens = options.inputTokens ?? 10;
  const outputTokens = options.outputTokens ?? 20;
  return {
    id: `resp_${result}`,
    status: "completed",
    created_at: options.createdAt ?? 100,
    output: [{
      id: `ig_${result}`,
      type: "image_generation_call",
      status: options.callStatus ?? "completed",
      result,
      revised_prompt: options.revisedPrompt,
      background: "opaque",
      output_format: "png",
      quality: "low",
      size: "1024x1024",
    }],
    tool_usage: {
      image_gen: {
        input_tokens: inputTokens,
        input_tokens_details: {
          image_tokens: options.imageInputTokens ?? 2,
          text_tokens: options.textInputTokens ?? inputTokens - 2,
        },
        output_tokens: outputTokens,
        output_tokens_details: {
          image_tokens: options.imageOutputTokens ?? outputTokens,
          text_tokens: options.textOutputTokens ?? 0,
        },
        total_tokens: inputTokens + outputTokens,
      },
    },
  };
}

describe("image batch execution", () => {
  it("starts every image concurrently and returns results in submission order", async () => {
    const gates = [deferred<unknown>(), deferred<unknown>(), deferred<unknown>()];
    const started: number[] = [];
    const bodies = [{ index: 0 }, { index: 1 }, { index: 2 }];

    const pending = executeImageBatch(bodies, async body => {
      const index = body.index as number;
      started.push(index);
      return handle(gates[index].promise);
    });

    await Promise.resolve();
    expect(started).toEqual([0, 1, 2]);
    gates[2].resolve(imageFinal("C", { createdAt: 103 }));
    gates[0].resolve(imageFinal("A", { createdAt: 101 }));
    gates[1].resolve(imageFinal("B", { createdAt: 102 }));

    const response = await pending;
    expect(response.created).toBe(101);
    expect(response.data.map(image => image.b64_json)).toEqual(["A", "B", "C"]);
  });

  it("aggregates usage and returns native image metadata", async () => {
    const response = await executeImageBatch(
      [{ index: 0 }, { index: 1 }],
      async body => handle(Promise.resolve(imageFinal(
        body.index === 0 ? "A" : "B",
        {
          revisedPrompt: `revised-${body.index}`,
          inputTokens: 11,
          imageInputTokens: 3,
          textInputTokens: 8,
          outputTokens: 21,
          imageOutputTokens: 20,
          textOutputTokens: 1,
        },
      ))),
    );

    expect(response).toEqual({
      created: 100,
      data: [
        { b64_json: "A", revised_prompt: "revised-0" },
        { b64_json: "B", revised_prompt: "revised-1" },
      ],
      background: "opaque",
      output_format: "png",
      quality: "low",
      size: "1024x1024",
      usage: {
        input_tokens: 22,
        input_tokens_details: { image_tokens: 6, text_tokens: 16 },
        output_tokens: 42,
        output_tokens_details: { image_tokens: 40, text_tokens: 2 },
        total_tokens: 64,
      },
    });
  });

  it("accepts gpt-image-2 terminal results whose call status remains generating", async () => {
    const response = await executeImageBatch([{}], async () => handle(Promise.resolve(
      imageFinal("IMAGE_BYTES", { callStatus: "generating" }),
    )));

    expect(response.data).toEqual([{ b64_json: "IMAGE_BYTES" }]);
  });

  it("waits for all submitted children before rejecting with the first input-order failure", async () => {
    const slow = deferred<unknown>();
    let slowSettled = false;
    const first = new ApiError(400, "first failure", { code: "first" });
    const second = new ApiError(400, "second failure", { code: "second" });

    const pending = executeImageBatch(
      [{ index: 0 }, { index: 1 }, { index: 2 }],
      async body => {
        if (body.index === 0) return handle(Promise.reject(first));
        if (body.index === 1) return handle(slow.promise.finally(() => { slowSettled = true; }));
        return handle(Promise.reject(second));
      },
    );

    await Promise.resolve();
    expect(slowSettled).toBe(false);
    slow.resolve(imageFinal("B"));
    await expect(pending).rejects.toBe(first);
    expect(slowSettled).toBe(true);
  });

  it("passes the downstream abort signal to every relay start", async () => {
    const abort = new AbortController();
    const signals: Array<AbortSignal | undefined> = [];

    await executeImageBatch([{ index: 0 }, { index: 1 }], async (_body, signal) => {
      signals.push(signal);
      return handle(Promise.resolve(imageFinal("ok")));
    }, abort.signal);

    expect(signals).toEqual([abort.signal, abort.signal]);
  });

  it("drains a bounded large image event stream while awaiting its final", async () => {
    const bounded = new V2RunHandle({ maxBufferedBytes: 128 });
    const producer = (async () => {
      for (let sequence = 0; sequence < 64; sequence++) {
        await bounded.acceptEvent({
          event: "response.output_text.delta",
          data: JSON.stringify({
            type: "response.output_text.delta",
            sequence_number: sequence,
            delta: "large-image-terminal",
          }),
        });
      }
      bounded.settle(imageFinal("LARGE"));
    })();

    const response = await Promise.race([
      executeImageBatch([{}], async () => bounded),
      new Promise<never>((_, reject) => setTimeout(
        () => reject(new Error("image event drain deadlocked")),
        500,
      )),
    ]);
    await producer;
    expect(response.data).toEqual([{ b64_json: "LARGE" }]);
  });

  it("returns the image iterator promptly when downstream cancellation wins", async () => {
    let returned = 0;
    const iterator = {
      next: () => new Promise<IteratorResult<any>>(() => undefined),
      return: () => {
        returned++;
        return Promise.resolve({ done: true as const, value: undefined });
      },
      throw: (error?: unknown) => Promise.reject(error),
      [Symbol.asyncIterator]() { return this; },
    } as unknown as AsyncGenerator<any>;
    const controller = new AbortController();
    const pending = executeImageBatch([{}], async () => ({
      events: () => iterator,
      final: new Promise(() => undefined),
      stats: { bytes: 0, firstByteMs: null, frames: 0 },
    }), controller.signal);
    await Promise.resolve();
    controller.abort(new Error("private cancellation detail"));

    await expect(Promise.race([
      pending,
      new Promise<never>((_, reject) => setTimeout(
        () => reject(new Error("image cancellation did not settle")),
        100,
      )),
    ])).rejects.toMatchObject({ name: "AbortError" });
    expect(returned).toBe(1);
  });

  it.each([
    [{ status: "completed", output: [] }, "missing image result"],
    [{ status: "completed", output: [{ type: "image_generation_call", status: "completed", result: "A" }, { type: "image_generation_call", status: "completed", result: "B" }] }, "multiple image results"],
    [{ status: "completed", output: [{ type: "image_generation_call", status: "failed", result: "A" }] }, "failed image call"],
    [{ status: "failed", output: [{ type: "image_generation_call", status: "completed", result: "A" }] }, "failed outer response"],
    [{ status: "incomplete", output: [{ type: "image_generation_call", status: "completed", result: "A" }] }, "incomplete outer response"],
  ])("rejects malformed upstream finals: %s", async (final) => {
    try {
      await executeImageBatch([{}], async () => handle(Promise.resolve(final)));
      throw new Error("expected ApiError");
    } catch (error) {
      expect(error).toBeInstanceOf(ApiError);
      expect((error as ApiError).status).toBe(502);
      expect((error as ApiError).options.code).toBe("invalid_upstream_response");
    }
  });
});
