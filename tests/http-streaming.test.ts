import { afterEach, describe, expect, it } from "bun:test";
import { createServer, get, type Server } from "node:http";
import { writeFetchResponse } from "../src/http/adapter";

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
    const url = await listen(() => new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode('data: {"type":"response.created"}\n\n'));
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
            cancel: async () => { cancellations++; },
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

    await adapterFinished;
    await clientFinished;
    expect(reads).toBe(0);
    expect(cancellations).toBe(1);
    expect(releases).toBe(1);
  });
});
