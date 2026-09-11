import { describe, expect, it } from "bun:test";
import type { V2BootstrapSupervisor } from "../src/v2/supervisor/host";
import { SupervisorLifecycle } from "../src/v2/supervisor/lifecycle";

function owner(shutdown: () => Promise<void>, close: () => Promise<void>): Pick<V2BootstrapSupervisor, "shutdown" | "close"> {
  return { shutdown, close };
}

describe("system supervisor cleanup ownership", () => {
  it("keeps dependent resources open until retained shutdown and close actually settle", async () => {
    const lifecycle = new SupervisorLifecycle(2);
    const shutdown = Promise.withResolvers<void>(), closed = Promise.withResolvers<void>();
    const events: string[] = [];
    const release = lifecycle.register(owner(async () => { events.push("shutdown"); await shutdown.promise; },
      async () => { events.push("close"); await closed.promise; release(); }));
    const stopping = lifecycle.shutdown(new AbortController().signal).then(() => { events.push("account_close"); });
    await Promise.resolve(); expect(events).toEqual(["shutdown"]);
    shutdown.resolve(); for (let i = 0; i < 10 && events.length < 2; i++) await Promise.resolve();
    expect(events).toEqual(["shutdown", "close"]);
    closed.resolve(); await stopping;
    expect(events).toEqual(["shutdown", "close", "account_close"]);
  });

  it("joins duplicate system shutdowns and fences late registration", async () => {
    const lifecycle = new SupervisorLifecycle(1); let calls = 0;
    const item = owner(async () => { calls++; }, async () => {});
    lifecycle.register(item);
    const signal = new AbortController().signal;
    const first = lifecycle.shutdown(signal), second = lifecycle.shutdown(signal);
    expect(first).toBe(second); expect(() => lifecycle.register(item)).toThrow("closing");
    await first; expect(calls).toBe(1);
  });

  it("bounds owners and releases capacity only through the registered close callback", () => {
    const lifecycle = new SupervisorLifecycle(1);
    const item = owner(async () => {}, async () => {});
    const release = lifecycle.register(item);
    expect(() => lifecycle.assertAvailable()).toThrow("capacity");
    release(); release(); expect(() => lifecycle.register(item)).not.toThrow();
  });

  it("reports an expired cleanup deadline without falsely reporting completion", async () => {
    const lifecycle = new SupervisorLifecycle(1), abort = new AbortController();
    const pending = Promise.withResolvers<void>(); let closed = false;
    lifecycle.register(owner(() => pending.promise, async () => { closed = true; }));
    const result = lifecycle.shutdown(abort.signal).catch(error => error);
    const failure = new Error("offline deadline"); abort.abort(failure);
    expect(await result).toBe(failure); expect(closed).toBe(false);
    pending.resolve(); for (let i = 0; i < 10 && !closed; i++) await Promise.resolve();
    expect(closed).toBe(true);
  });

  it("reports failed exact shutdown and still closes the local owner", async () => {
    const lifecycle = new SupervisorLifecycle(1); let closed = false;
    lifecycle.register(owner(async () => { throw new Error("offline ambiguous shutdown"); }, async () => { closed = true; }));
    await expect(lifecycle.shutdown(new AbortController().signal)).rejects.toThrow("incomplete");
    expect(closed).toBe(true);
  });

  it("does not fail an empty registry after the outer deadline", async () => {
    const lifecycle = new SupervisorLifecycle(1), abort = new AbortController(); abort.abort();
    await expect(lifecycle.shutdown(abort.signal)).resolves.toBeUndefined();
  });
});
