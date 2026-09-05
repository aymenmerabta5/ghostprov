import { describe, expect, it } from "bun:test";
import { SandboxLogService, SANDBOX_LOG_READ_RESERVATION } from "../src/v2/diagnostics/sandbox-log-service";
import { createGhostProvApp, type RouterLike } from "../src/server/app";

const ID = "11111111-1111-4111-8111-111111111111";
function fixture(readTimeoutMs = 1000) {
  let bytes = 0;
  const service = new SandboxLogService({ maxSources: 2, readTimeoutMs, governor: {
    reserveFixedBytes(amount) {
      bytes += amount;
      let active = true;
      return { reservedBytes: amount, release() { if (active) bytes -= amount; active = false; }, async replace() { throw new Error("unused"); } };
    },
  } });
  return { service, bytes: () => bytes };
}

describe("sandbox log reads", () => {
  it("holds memory until delivery or cancellation and rejects a third concurrent reader", async () => {
    const f = fixture(); f.service.register(ID, { async readDiagnosticLog() { return "log\n"; } });
    const first = await f.service.read(ID, "sidecar.jsonl");
    const second = await f.service.read(ID, "supervisor.jsonl");
    expect(f.bytes()).toBe(2 * SANDBOX_LOG_READ_RESERVATION);
    expect((await f.service.read(ID, "sidecar.1.jsonl")).status).toBe(503);
    expect(await first.text()).toBe("log\n");
    expect(f.bytes()).toBe(SANDBOX_LOG_READ_RESERVATION);
    await second.body!.cancel();
    expect(f.bytes()).toBe(0);
    expect(f.service.snapshot().activeReads).toBe(0);
    f.service.close();
  });

  it("fences stale sources, including late results and old unregistration callbacks", async () => {
    const f = fixture(); const pending = Promise.withResolvers<string>();
    const removeOld = f.service.register(ID, { readDiagnosticLog: () => pending.promise });
    const oldRead = f.service.read(ID, "sidecar.jsonl");
    f.service.register(ID, { async readDiagnosticLog() { return "new\n"; } }); removeOld();
    pending.resolve("old secret must not escape");
    const failed = await oldRead;
    expect(failed.status).toBe(502); expect(await failed.text()).not.toContain("old secret");
    expect(await (await f.service.read(ID, "sidecar.jsonl")).text()).toBe("new\n");
    expect(f.bytes()).toBe(0); f.service.close();
  });

  it("cleans up errors, missing logs, invalid paths and aborted requests", async () => {
    const f = fixture(); let calls = 0;
    f.service.register(ID, { async readDiagnosticLog() { calls++; throw new Error("private provider detail"); } });
    expect((await f.service.read(ID, "../static.json")).status).toBe(404);
    expect((await f.service.read(ID + "\n", "sidecar.jsonl")).status).toBe(404);
    expect(calls).toBe(0);
    const failure = await f.service.read(ID, "sidecar.jsonl"); expect(await failure.text()).not.toContain("private");
    const abort = AbortSignal.abort(); await f.service.read(ID, "sidecar.jsonl", abort);
    f.service.register(ID, { async readDiagnosticLog() { return null; } });
    expect((await f.service.read(ID, "sidecar.jsonl")).status).toBe(404);
    expect(f.bytes()).toBe(0); f.service.close();
  });

  it("expires an unread response and frees its permit", async () => {
    const f = fixture(10); f.service.register(ID, { async readDiagnosticLog() { return "bounded\n"; } });
    const response = await f.service.read(ID, "sidecar.jsonl");
    await Bun.sleep(25);
    await expect(response.text()).rejects.toBeDefined();
    expect(f.bytes()).toBe(0); f.service.close();
  });

  it("does not free an in-flight fetch reservation before an abort-ignoring source settles", async () => {
    const f = fixture(); const pending = Promise.withResolvers<string>();
    f.service.register(ID, { readDiagnosticLog: () => pending.promise });
    const response = f.service.read(ID, "sidecar.jsonl");
    f.service.close(); expect(f.bytes()).toBe(SANDBOX_LOG_READ_RESERVATION);
    pending.resolve("late\n"); expect((await response).status).toBe(502);
    expect(f.bytes()).toBe(0); expect(f.service.snapshot()).toEqual({ sources: 0, activeReads: 0 });
  });

  it("authenticates the public endpoint before invoking a sandbox read", async () => {
    let reads = 0;
    const app = createGhostProvApp({ apiKey: "offline-api-key", router: {} as RouterLike,
      sandboxLogs: { async read() { reads++; return new Response("log\n"); } } });
    const url = `http://localhost/v1/diagnostics/sandboxes/${ID}/logs/sidecar.jsonl`;
    expect((await app.request(url)).status).toBe(401); expect(reads).toBe(0);
    expect(await (await app.request(url, { headers: { authorization: "Bearer offline-api-key" } })).text()).toBe("log\n");
    expect(reads).toBe(1);
  });
});
