import { describe, expect, it } from "bun:test";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { releaseAtomicBytePermit } from "../src/v2/memory/permit-carrier";
import {
  DirectResponsesClient,
  estimateAtomicEventPermitBytes,
  IncrementalSseEventParser,
  type IncrementalSseEvent,
} from "../src/v2/sidecar/upstream";

describe("incremental upstream SSE parser", () => {
  it("scans one-byte transport chunks exactly once and preserves SSE semantics", async () => {
    const source = [
      ": keepalive\r\n",
      "event: response.output_text.delta\r\n",
      "data: hello 🌍\r\n",
      "data: second\r\n",
      "\r\n",
    ].join("");
    const input = new TextEncoder().encode(source);
    const parser = new IncrementalSseEventParser({ maxEventBytes: 1024 });
    const events: IncrementalSseEvent[] = [];
    for (const byte of input) {
      await parser.push(Uint8Array.of(byte), event => { events.push(event); });
    }
    const finished = await parser.finish();

    expect(events).toHaveLength(1);
    expect(events[0]!.frame).toEqual({
      event: "response.output_text.delta",
      data: "hello 🌍\nsecond",
    });
    expect(finished.trailingNonWhitespace).toBe(false);
    expect(finished.snapshot).toMatchObject({
      inputBytes: input.byteLength,
      scannedBytes: input.byteLength,
      completedEvents: 1,
      maxScratchBytes: 64 * 1024,
    });
    events[0]!.release();
  });

  it("charges retained partial bytes first and releases them on abort", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: estimateAtomicEventPermitBytes(128),
      jumboBytes: estimateAtomicEventPermitBytes(128),
    });
    const parser = new IncrementalSseEventParser({
      maxEventBytes: 128,
      permits: pool,
      reservationBlockBytes: 8,
    });
    const partial = new TextEncoder().encode("event: x\ndata: retained");
    await parser.push(partial, () => { throw new Error("unexpected event"); });
    expect(pool.snapshot().ordinaryUsedBytes).toBe(
      estimateAtomicEventPermitBytes(Math.ceil(partial.byteLength / 8) * 8),
    );
    expect(parser.snapshot().currentEventBytes).toBe(partial.byteLength);

    parser.abort();
    expect(pool.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
      activePermits: 0,
    });
  });

  it("strips only the stream-leading BOM and preserves a BOM inside event data", async () => {
    const input = new TextEncoder().encode(
      `\uFEFFevent: response.output_text.delta\ndata: \uFEFFkept\n\n`,
    );
    const parser = new IncrementalSseEventParser({ maxEventBytes: 1024 });
    const frames: Array<{ event: string; data: string }> = [];
    for (const byte of input) {
      await parser.push(Uint8Array.of(byte), parsed => {
        frames.push(parsed.frame);
        parsed.release();
      });
    }
    expect((await parser.finish()).trailingNonWhitespace).toBe(false);
    expect(frames).toEqual([{
      event: "response.output_text.delta",
      data: "\uFEFFkept",
    }]);
    expect(parser.snapshot().scannedBytes).toBe(input.byteLength);
  });

  it("normalizes discarded CRLF bytes and dispatches a trailing-CR delimiter immediately", async () => {
    const source = "event: x\r\ndata: y\r\r";
    const input = new TextEncoder().encode(source);
    const parser = new IncrementalSseEventParser({ maxEventBytes: 128 });
    const counts: number[] = [];
    const consume = (parsed: IncrementalSseEvent) => {
      counts.push(parsed.rawByteCount);
      parsed.release();
    };
    for (const byte of input) await parser.push(Uint8Array.of(byte), consume);
    expect(counts).toEqual([input.byteLength - 1]);
    await parser.finish(consume);
    expect(counts).toEqual([input.byteLength - 1]);
    expect(parser.snapshot()).toMatchObject({
      inputBytes: input.byteLength,
      scannedBytes: input.byteLength,
    });
  });

  it("releases its permit when fatal UTF-8 finalization fails", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: estimateAtomicEventPermitBytes(128),
      jumboBytes: estimateAtomicEventPermitBytes(128),
    });
    const parser = new IncrementalSseEventParser({
      maxEventBytes: 128,
      permits: pool,
      reservationBlockBytes: 8,
    });
    const invalid = new Uint8Array([...new TextEncoder().encode("data: "), 0xc3]);
    await parser.push(invalid, () => {});
    await expect(parser.finish()).rejects.toBeInstanceOf(TypeError);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, ordinaryUsedBytes: 0 });
  });

  it("promotes a growing event to the reserved lane without losing its charge", async () => {
    const firstCharge = estimateAtomicEventPermitBytes(8);
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: firstCharge + 16,
      jumboBytes: estimateAtomicEventPermitBytes(128),
    });
    const blocker = await pool.acquire(16);
    const parser = new IncrementalSseEventParser({
      maxEventBytes: 128,
      permits: pool,
      reservationBlockBytes: 8,
    });
    await parser.push(new TextEncoder().encode("data: 1"), () => {});
    expect(parser.snapshot().currentRegion).toBe("ordinary");
    await parser.push(new TextEncoder().encode("234567890"), () => {});
    expect(parser.snapshot()).toMatchObject({ currentRegion: "jumbo" });
    expect(pool.snapshot()).toMatchObject({ ordinaryUsedBytes: 16, jumboActive: true });

    parser.abort();
    blocker.release();
    expect(pool.snapshot().activePermits).toBe(0);
  });

  it("uses the reserved lane when N large body permits exhaust ordinary progress", async () => {
    const bodyBytes = 2 * 1024 * 1024;
    const bodyCount = 8;
    const maxEventBytes = 1024 * 1024;
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: bodyBytes * bodyCount,
      jumboBytes: estimateAtomicEventPermitBytes(maxEventBytes),
      maxWaiters: 32,
    });
    const bodies = await Promise.all(
      Array.from({ length: bodyCount }, () => pool.acquire(bodyBytes)),
    );
    expect(pool.snapshot().ordinaryAvailableBytes).toBe(0);

    const parser = new IncrementalSseEventParser({
      maxEventBytes,
      permits: pool,
    });
    const source = new TextEncoder().encode(
      `event: response.output_text.delta\ndata: ${"x".repeat(512 * 1024)}\n\n`,
    );
    let completed: IncrementalSseEvent | null = null;
    await Promise.race([
      (async () => {
        for (let offset = 0; offset < source.byteLength; offset += 64 * 1024) {
          await parser.push(source.subarray(offset, offset + 64 * 1024), event => {
            completed = event;
          });
        }
      })(),
      Bun.sleep(1_000).then(() => { throw new Error("body-held ordinary permit deadlock"); }),
    ]);
    expect(completed).not.toBeNull();
    expect(completed!.region).toBe("jumbo");
    expect(completed!.chargedBytes).toBe(estimateAtomicEventPermitBytes(completed!.rawByteCount));
    completed!.release();
    await parser.finish();
    for (const body of bodies) body.release();
    expect(pool.snapshot()).toMatchObject({
      activePermits: 0,
      waiters: 0,
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
    });
  });

  it("rejects one byte beyond the configured atomic event cap", async () => {
    const parser = new IncrementalSseEventParser({
      maxEventBytes: 64,
      reservationBlockBytes: 8,
    });
    await parser.push(new Uint8Array(64).fill("x".charCodeAt(0)), () => {});
    await expect(parser.push(Uint8Array.of("x".charCodeAt(0)), () => {})).rejects.toMatchObject({
      code: "event_too_large",
      ambiguous: true,
    });
    expect(parser.snapshot()).toMatchObject({
      inputBytes: 65,
      scannedBytes: 65,
      currentEventBytes: 64,
      maxScratchBytes: 64 * 1024,
    });
    parser.abort();
  });

  it("rejects event names the frozen wire grammar cannot carry", async () => {
    const parser = new IncrementalSseEventParser({ maxEventBytes: 1024 });
    await expect(parser.push(
      new TextEncoder().encode("event: .invalid\ndata: x\n\n"),
      () => {},
    )).rejects.toMatchObject({ code: "invalid_event" });
    parser.abort();
  });

  it("streams canonical chunks through the direct client and releases the global permit", async () => {
    const pool = new AtomicBytePermitPool({
      ordinaryBytes: estimateAtomicEventPermitBytes(64 * 1024),
      jumboBytes: estimateAtomicEventPermitBytes(1024 * 1024),
    });
    const delta = "x".repeat(256 * 1024);
    const final = { id: "resp_large", object: "response", status: "completed", output: [] };
    const source = [
      ["response.output_text.delta", { type: "response.output_text.delta", delta }],
      ["response.completed", { type: "response.completed", response: final }],
    ].map(([event, data]) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`).join("");
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: { reveal: () => "test" },
      atomicBytePermits: pool,
      maxEventBytes: 1024 * 1024,
      fetch: async () => new Response(source, {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      }),
    });
    let transcriptBytes = 0;
    let maxChunk = 0;
    let sawRetainedTerminalPermit = false;
    const terminal = await client.execute({
      body: new TextEncoder().encode('{"model":"gpt-test","input":"x"}'),
      beforeFetch: async () => {},
      onPrelude: async () => {},
      onTranscriptBytes: async bytes => {
        transcriptBytes += bytes.byteLength;
        maxChunk = Math.max(maxChunk, bytes.byteLength);
      },
      onParserSnapshot: () => {
        if (pool.snapshot().activePermits === 1) sawRetainedTerminalPermit = true;
      },
      onEvent: async () => {},
    });
    expect(transcriptBytes).toBe(terminal.byteCount);
    expect(maxChunk).toBeLessThanOrEqual(64 * 1024);
    expect(sawRetainedTerminalPermit).toBe(true);
    expect(terminal.event?.event).toBe("response.completed");
    releaseAtomicBytePermit(terminal.event);
    expect(pool.snapshot()).toMatchObject({
      activePermits: 0,
      ordinaryUsedBytes: 0,
      jumboUsedBytes: 0,
    });
  });

  it("finalizes at the terminal event without deadlocking on post-terminal bytes", async () => {
    const capacity = estimateAtomicEventPermitBytes(512 * 1024);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: capacity, jumboBytes: capacity });
    const final = { id: "resp_terminal", object: "response", status: "completed", output: [] };
    const source = `event: response.completed\ndata: ${JSON.stringify({
      type: "response.completed",
      response: final,
    })}\n\nevent: response.output_text.delta\ndata: ${"x".repeat(512 * 1024)}\n\n`;
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: { reveal: () => "test" },
      atomicBytePermits: pool,
      maxEventBytes: 512 * 1024,
      fetch: async () => new Response(source, {
        headers: { "content-type": "text/event-stream" },
      }),
    });
    const events: string[] = [];
    const terminal = await Promise.race([
      client.execute({
        body: new TextEncoder().encode('{"model":"gpt-test","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async event => { events.push(event.event); },
      }),
      Bun.sleep(1_000).then(() => { throw new Error("post-terminal permit deadlock"); }),
    ]);
    expect(terminal.responseId).toBe("resp_terminal");
    expect(terminal.event?.event).toBe("response.completed");
    expect(events).toEqual([]);
    releaseAtomicBytePermit(terminal.event);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
  });

  it("does not await a non-settling upstream cancel after the terminal event", async () => {
    const capacity = estimateAtomicEventPermitBytes(512 * 1024);
    const pool = new AtomicBytePermitPool({ ordinaryBytes: capacity, jumboBytes: capacity });
    const final = { id: "resp_cancel_hang", object: "response", status: "completed", output: [] };
    const bytes = new TextEncoder().encode(
      `event: response.completed\ndata: ${JSON.stringify({
        type: "response.completed",
        response: final,
      })}\n\n`,
    );
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(bytes); },
      cancel() { return new Promise<void>(() => undefined); },
    });
    const client = new DirectResponsesClient({
      baseUrl: "https://openai.test/v1",
      apiKey: { reveal: () => "test" },
      atomicBytePermits: pool,
      maxEventBytes: 512 * 1024,
      fetch: async () => new Response(body, {
        headers: { "content-type": "text/event-stream" },
      }),
    });

    const terminal = await Promise.race([
      client.execute({
        body: new TextEncoder().encode('{"model":"gpt-test","input":"x"}'),
        beforeFetch: async () => {},
        onPrelude: async () => {},
        onEvent: async () => {},
      }),
      Bun.sleep(250).then(() => { throw new Error("terminal waited for non-settling cancel"); }),
    ]);
    expect(terminal.responseId).toBe("resp_cancel_hang");
    releaseAtomicBytePermit(terminal.event);
    expect(pool.snapshot()).toMatchObject({ activePermits: 0, waiters: 0 });
  });
});
