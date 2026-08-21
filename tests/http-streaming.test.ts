import { afterEach, describe, expect, it } from "vitest";
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

    await new Promise(resolve => setTimeout(resolve, 20));
    expect(cancelled).toBe(true);
  });
});
