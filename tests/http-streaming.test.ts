import { afterEach, describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import { createServer, get, type Server, type ServerResponse } from "node:http";
import { writeFetchResponse } from "../src/http/adapter";
import { AtomicBytePermitPool } from "../src/v2/memory/atomic-byte-permits";
import { attachAtomicBytePermit } from "../src/v2/memory/permit-carrier";

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
});

async function listen(response: () => Response): Promise<string> {
  const server = createServer((_req, res) => {
    void writeFetchResponse(res, response()).catch(error => res.destroy(error as Error));
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing test address");
  return `http://127.0.0.1:${address.port}`;
}

describe("writeFetchResponse", () => {
  it("delivers the first SSE chunk before stream completion", async () => {
    const encoder = new TextEncoder();
    const permits = new AtomicBytePermitPool({ ordinaryBytes: 1_024, jumboBytes: 1 });
    const sourcePermit = await permits.acquire(1_024);
    const first = attachAtomicBytePermit(
      encoder.encode('data: {"type":"response.created"}\n\n'),
      sourcePermit.transfer("http-test"),
    );
    const url = await listen(() => new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(first);
        setTimeout(() => {
          controller.enqueue(encoder.encode('data: {"type":"response.completed"}\n\n'));
          controller.close();
        }, 120);
      },
    }), { headers: { "content-type": "text/event-stream" } }));

    const startedAt = performance.now();
    const result = await new Promise<{ firstChunkAt: number; endedAt: number; body: string }>((resolve, reject) => {
      get(url, response => {
        let firstChunkAt = 0;
        let body = "";
        response.setEncoding("utf8");
        response.on("data", chunk => {
          if (!firstChunkAt) firstChunkAt = performance.now() - startedAt;
          body += chunk;
        });
        response.on("end", () => resolve({ firstChunkAt, endedAt: performance.now() - startedAt, body }));
      }).on("error", reject);
    });

    expect(result.firstChunkAt).toBeGreaterThan(0);
    expect(result.endedAt - result.firstChunkAt).toBeGreaterThan(70);
    expect(result.body).toContain("response.completed");
    expect(permits.snapshot().ordinaryUsedBytes).toBe(0);
    permits.close();
  });

  it("cancels the Fetch body when the client disconnects", async () => {
    let cancelled = false;
    const encoder = new TextEncoder();
    const url = await listen(() => new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode("first"));
      },
      cancel() {
        cancelled = true;
      },
    })));

    await new Promise<void>((resolve, reject) => {
      const request = get(url, response => {
        response.once("data", () => {
          response.destroy();
          resolve();
        });
      });
      request.on("error", reject);
    });

    const deadline = Date.now() + 1_000;
    while (!cancelled && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
    expect(cancelled).toBe(true);
  });

  it("cancels the Fetch body before reading when the response is already closed", async () => {
    let reads = 0;
    let cancellations = 0;
    let releases = 0;
    let resolveAdapter!: () => void;
    let rejectAdapter!: (error: unknown) => void;
    const adapterFinished = new Promise<void>((resolve, reject) => {
      resolveAdapter = resolve;
      rejectAdapter = reject;
    });
    const server = createServer((_req, res) => {
      res.destroy();
      const response = {
        status: 200,
        headers: new Headers({ "content-type": "text/event-stream" }),
        body: {
          getReader: () => ({
            read: async () => {
              reads++;
              return { done: false as const, value: new Uint8Array([1]) };
            },
            cancel: () => {
              cancellations++;
              return new Promise<void>(() => undefined);
            },
            releaseLock: () => { releases++; },
          }),
        },
      } as unknown as Response;
      void writeFetchResponse(res, response).then(resolveAdapter, rejectAdapter);
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing address");
    const clientFinished = new Promise<void>(resolve => {
      const request = get(`http://127.0.0.1:${address.port}`);
      request.once("error", () => resolve());
      request.once("close", () => resolve());
    });

    await Promise.race([
      adapterFinished,
      new Promise<never>((_resolve, reject) => setTimeout(
        () => reject(new Error("adapter waited for nonsettling reader.cancel")),
        500,
      )),
    ]);
    await clientFinished;
    expect(reads).toBe(0);
    expect(cancellations).toBe(1);
    expect(releases).toBe(1);
  });

  it("times out a non-draining write and releases its permit only after destroying output", async () => {
    class StalledResponse extends EventEmitter {
      destroyed = false;
      closed = false;
      headersSent = false;

      writeHead(): this {
        this.headersSent = true;
        return this;
      }

      write(): boolean {
        return false;
      }

      end(): void {}

      destroy(): this {
        this.destroyed = true;
        this.closed = true;
        queueMicrotask(() => this.emit("close"));
        return this;
      }
    }

    const permits = new AtomicBytePermitPool({ ordinaryBytes: 1_024, jumboBytes: 1 });
    const chunk = attachAtomicBytePermit(
      new Uint8Array(1_024),
      (await permits.acquire(1_024)).transfer("stalled-http-write"),
    );
    let cancelled = false;
    const response = new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(chunk);
      },
      cancel() {
        cancelled = true;
      },
    }));
    const output = new StalledResponse();

    const writing = writeFetchResponse(
      output as unknown as ServerResponse,
      response,
      { writeTimeoutMs: 10 },
    );
    await expect(Promise.race([
      writing,
      new Promise<never>((_resolve, reject) => setTimeout(
        () => reject(new Error("HTTP write deadline did not settle output")),
        250,
      )),
    ])).rejects.toMatchObject({ name: "HttpResponseWriteTimeoutError" });

    expect(output.destroyed).toBe(true);
    expect(cancelled).toBe(true);
    expect(permits.snapshot()).toMatchObject({
      ordinaryUsedBytes: 0,
      activePermits: 0,
    });
    permits.close();
  });
});
