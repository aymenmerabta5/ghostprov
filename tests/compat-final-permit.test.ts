import { describe, expect, it } from "bun:test";
import type { RunHandle } from "../src/relay/run-handle";
import { executeImageBatch } from "../src/images/service";
import { createGhostProvApp, type RouterLike } from "../src/server/app";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import {
  attachAtomicBytePermit,
  releaseAtomicBytePermit,
} from "../src/v2/memory/permit-carrier";
import type { SseFrame } from "../src/relay/sse";
import {
  finalResponse,
  messageOutput,
  textPart,
} from "./support/anthropic-stream";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

function handle(
  final: Promise<unknown> | unknown,
  events: () => AsyncGenerator<any> = async function* () {},
): RunHandle {
  return {
    events,
    final: Promise.resolve(final),
    stats: { bytes: 0, firstByteMs: null, frames: 0 },
  };
}

class OneRouter implements RouterLike {
  calls = 0;
  constructor(readonly start: (call: number) => Promise<RunHandle> | RunHandle) {}
  async startRelay(): Promise<RunHandle> { return this.start(++this.calls); }
  snapshot() {
    return {
      capacity: 4,
      active: 0,
      available: 4,
      queued: 0,
      runtimes: [],
      resources: {},
    };
  }
}

function imageFinal(result: string) {
  return {
    id: `resp_${result.length}`,
    status: "completed",
    created_at: 100,
    output: [{
      id: "ig_1",
      type: "image_generation_call",
      status: "completed",
      result,
      output_format: "png",
      size: "1024x1024",
    }],
  };
}

function anthropicFinal(text: string) {
  return finalResponse("resp_permitted", [
    messageOutput("msg_permitted", [textPart(text)]),
  ]);
}

function anthropicRequest(stream: boolean) {
  return new Request("http://localhost/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "anthropic-version": "2023-06-01",
    },
    body: JSON.stringify({
      model: "claude-sonnet-4-6",
      max_tokens: 64,
      messages: [{ role: "user", content: "hello" }],
      stream,
    }),
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for permit release");
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

describe("compatibility final permit ownership", () => {
  it("holds multiple image finals through one slow JSON drain", async () => {
    const MiB = 1024 * 1024;
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 16 * MiB, jumboBytes: 1 });
    const router = new OneRouter(call => handle(attachAtomicBytePermit(
      imageFinal(String(call).repeat(MiB)),
      pool.tryAcquire(8 * MiB)!,
    )));
    const app = createGhostProvApp({ router });
    const response = await app.request("/v1/images/generations", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: "gpt-image-2", prompt: "draw", n: 2 }),
    });
    expect(response.status).toBe(200);
    expect(pool.snapshot()).toMatchObject({ activePermits: 2, ordinaryUsedBytes: 16 * MiB });

    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(first.value!.byteLength).toBeGreaterThan(2 * MiB);
    expect(pool.snapshot().activePermits).toBe(2);
    await expect(reader.read()).resolves.toEqual({ value: undefined, done: true });
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("releases successful and malformed image siblings on batch failure", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 16_384, jumboBytes: 1 });
    const good = attachAtomicBytePermit(imageFinal("good"), pool.tryAcquire(8_192)!);
    const malformed = attachAtomicBytePermit(
      { id: "bad", status: "completed", output: [] },
      pool.tryAcquire(8_192)!,
    );
    let call = 0;
    await expect(executeImageBatch([{}, {}], async () => handle(
      ++call === 1 ? good : malformed,
    ))).rejects.toMatchObject({ status: 502 });
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("releases a successful image sibling when another relay rejects", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const good = attachAtomicBytePermit(imageFinal("good"), pool.tryAcquire(8_192)!);
    let call = 0;
    await expect(executeImageBatch([{}, {}], async () => (
      ++call === 1 ? handle(good) : handle(Promise.reject(new Error("failed")))
    ))).rejects.toThrow("failed");
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("releases an image final that settles after batch cancellation", async () => {
    const gate = deferred<unknown>();
    const controller = new AbortController();
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const pending = executeImageBatch([{}], async () => handle(gate.promise), controller.signal);
    controller.abort("disconnect");
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    gate.resolve(attachAtomicBytePermit(imageFinal("late"), pool.tryAcquire(8_192)!));
    await waitFor(() => pool.snapshot().activePermits === 0);
    pool.close();
  });

  it("holds a 1 MiB Anthropic nonstream translation until JSON drains", async () => {
    const MiB = 1024 * 1024;
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8 * MiB, jumboBytes: 1 });
    const final = attachAtomicBytePermit(
      anthropicFinal("x".repeat(MiB)),
      pool.tryAcquire(8 * MiB)!,
    );
    const response = await createGhostProvApp({
      router: new OneRouter(() => handle(final)),
    }).fetch(anthropicRequest(false));
    expect(response.status).toBe(200);
    expect(pool.snapshot().activePermits).toBe(1);
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(first.value!.byteLength).toBeGreaterThan(MiB);
    expect(pool.snapshot().activePermits).toBe(1);
    await reader.read();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("holds an Anthropic streaming final through translated SSE drain", async () => {
    const MiB = 1024 * 1024;
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8 * MiB, jumboBytes: 1 });
    const final = attachAtomicBytePermit(
      anthropicFinal("x".repeat(MiB)),
      pool.tryAcquire(8 * MiB)!,
    );
    const response = await createGhostProvApp({
      router: new OneRouter(() => handle(final)),
    }).fetch(anthropicRequest(true));
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    expect(pool.snapshot().activePermits).toBe(1);
    for (;;) {
      const next = await reader.read();
      if (next.done) break;
    }
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });

  it("releases a late Anthropic final after pending-final cancellation", async () => {
    const gate = deferred<unknown>();
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const response = await createGhostProvApp({
      router: new OneRouter(() => handle(gate.promise)),
    }).fetch(anthropicRequest(true));
    const reader = response.body!.getReader();
    // Drain message_start, then trigger the iterator-done final wait.
    await reader.read();
    const pending = reader.read();
    await new Promise(resolve => setTimeout(resolve, 0));
    await reader.cancel("disconnect");
    gate.resolve(attachAtomicBytePermit(
      anthropicFinal("late"),
      pool.tryAcquire(8_192)!,
    ));
    await Promise.resolve(pending).catch(() => undefined);
    await waitFor(() => pool.snapshot().activePermits === 0);
    pool.close();
  });

  it("releases a carried Anthropic event when translation fails", async () => {
    const pool = new AtomicBytePermitPool({ ordinaryBytes: 8_192, jumboBytes: 1 });
    const malformed = attachAtomicBytePermit<SseFrame>({
      event: "response.output_text.delta",
      data: "not-json",
    }, pool.tryAcquire(8_192)!);
    const response = await createGhostProvApp({
      router: new OneRouter(() => handle(
        Promise.reject(new Error("no final")),
        async function* () { yield malformed; },
      )),
    }).fetch(anthropicRequest(true));
    await response.text();
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
    pool.close();
  });
});
