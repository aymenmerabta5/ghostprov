import { afterEach, describe, expect, it } from "bun:test";
import type { NetworkInterfaceInfo } from "node:os";
import type { Clock } from "../src/core/ports";
import { observabilityEvent, type JsonValue } from "../src/observability/events";
import { assertSafeObservabilityEvent } from "../src/observability/redaction";
import { createPrismConnectivityProbe, PrismConnectivityMonitor, type PrismConnectivityProbeResult } from "../src/prism/connectivity";

class ManualClock implements Clock {
  value = 1_000;
  readonly waiters = new Set<{ deadline: number; finish(error?: unknown): void }>();
  now(): number { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> { return this.waitUntil(this.value + ms, signal); }
  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(signal.reason);
    if (deadline <= this.value) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const abort = () => waiter.finish(signal!.reason);
      const waiter = { deadline, finish: (error?: unknown) => {
        this.waiters.delete(waiter);
        signal?.removeEventListener("abort", abort);
        if (error === undefined) resolve(); else reject(error);
      } };
      this.waiters.add(waiter);
      signal?.addEventListener("abort", abort, { once: true });
    });
  }
  async advance(ms: number): Promise<void> {
    this.value += ms;
    for (const waiter of [...this.waiters]) if (waiter.deadline <= this.value) waiter.finish();
    await flush();
  }
}

async function flush(): Promise<void> { for (let i = 0; i < 40; i++) await Promise.resolve(); }
const monitors: PrismConnectivityMonitor[] = [];
afterEach(() => { for (const monitor of monitors.splice(0)) monitor.close(); });
function harness(probe: (signal: AbortSignal) => Promise<PrismConnectivityProbeResult>) {
  const clock = new ManualClock();
  const monitor = new PrismConnectivityMonitor({ probe, clock });
  monitors.push(monitor);
  return { clock, monitor };
}
const reachable = (): PrismConnectivityProbeResult => ({ reachable: true, reason: "reachable" });
const offline = (): PrismConnectivityProbeResult => ({ reachable: false, reason: "transport" });
const address: NetworkInterfaceInfo = {
  address: "192.0.2.2", netmask: "255.255.255.0", family: "IPv4", mac: "00:00:00:00:00:01",
  internal: false, cidr: "192.0.2.2/24",
};

describe("credential-free Prism connectivity probe", () => {
  it.each(["Ethernet", "Wi-Fi"])("checks Prism over an assigned %s interface without credentials", async name => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const probe = createPrismConnectivityProbe("https://prism.test/private/path", {
      interfaces: () => ({ [name]: [address] }),
      fetch: async (url, init) => { calls.push({ url: String(url), init }); return new Response(null, { status: 401 }); },
    });
    expect(await probe(new AbortController().signal)).toEqual(reachable());
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe("https://prism.test/");
    expect(calls[0]!.init).toMatchObject({ method: "HEAD", credentials: "omit", redirect: "manual", cache: "no-store" });
    expect(calls[0]!.init!.headers).toBeUndefined();
    expect(calls[0]!.init!.body).toBeUndefined();
  });

  it("detects absent Ethernet and Wi-Fi before attempting HTTPS", async () => {
    let calls = 0;
    const probe = createPrismConnectivityProbe("https://prism.test", {
      interfaces: () => ({ loopback: [{ ...address, internal: true, address: "127.0.0.1" }] }),
      fetch: async () => { calls++; return new Response(null); },
    });
    expect(await probe(new AbortController().signal)).toEqual({ reachable: false, reason: "no_interface" });
    expect(calls).toBe(0);
  });

  it("uses HTTPS if interface enumeration is unavailable", async () => {
    const probe = createPrismConnectivityProbe("https://prism.test", {
      interfaces: () => { throw new Error("unsupported"); }, fetch: async () => new Response(null),
    });
    expect(await probe(new AbortController().signal)).toEqual(reachable());
  });

  it.each(["ENOTFOUND", "EAI_AGAIN", "DNS_NOTFOUND"])("classifies DNS failure %s without retaining its message", async code => {
    const probe = createPrismConnectivityProbe("https://prism.test", {
      interfaces: () => ({ Ethernet: [address] }),
      fetch: async () => { throw Object.assign(new Error("private host and request details"), { code }); },
    });
    expect(await probe(new AbortController().signal)).toEqual({ reachable: false, reason: "dns" });
  });

  it.each([408, 429, 500, 503])("treats root HTTP %s as shared unavailability", async status => {
    const probe = createPrismConnectivityProbe("https://prism.test", {
      interfaces: () => ({ Ethernet: [address] }), fetch: async () => new Response(null, { status }),
    });
    expect(await probe(new AbortController().signal)).toEqual({ reachable: false, reason: "service_unavailable" });
  });

  it.each([200, 204, 302, 400, 401, 403, 404, 405])("accepts HTTP %s as connection proof without following redirects", async status => {
    const probe = createPrismConnectivityProbe("https://prism.test", {
      interfaces: () => ({ Ethernet: [address] }), fetch: async () => new Response(null, { status }),
    });
    expect(await probe(new AbortController().signal)).toEqual(reachable());
  });

  it.each(["http://prism.test", "https://user:password@prism.test", "https://prism.test/?token=value", "https://prism.test/#secret"])("rejects unsafe probe configuration %s", origin => {
    expect(() => createPrismConnectivityProbe(origin)).toThrow("credential-free HTTPS origin");
  });
});

describe("shared recovery connectivity monitor", () => {
  it("does no healthy background polling", async () => {
    let calls = 0;
    const { clock, monitor } = harness(async () => { calls++; return reachable(); });
    await clock.advance(60_000);
    expect(calls).toBe(0);
    expect(monitor.isRecoveryBlocked()).toBe(false);
    monitor.requestCheck(); await flush();
    expect(calls).toBe(1);
    expect(monitor.snapshot().recoveredAtMs).toBeNull();
    await clock.advance(60_000);
    expect(calls).toBe(1);
    expect(clock.waiters.size).toBe(0);
  });

  it("coalesces concurrent and reentrant reports into one physical probe", async () => {
    let calls = 0;
    let finish!: (value: PrismConnectivityProbeResult) => void;
    const { monitor } = harness(async () => { calls++; return new Promise(resolve => { finish = resolve; }); });
    const unsubscribe = monitor.subscribe(() => monitor.requestCheck());
    for (let i = 0; i < 256; i++) monitor.requestCheck();
    await flush();
    expect(calls).toBe(1);
    expect(monitor.isRecoveryBlocked()).toBe(true);
    finish(reachable()); await flush(); unsubscribe();
    expect(monitor.snapshot().state).toBe("reachable");
    expect(calls).toBe(1);
  });

  it("invalidates a cached success immediately but bounds subsequent probe frequency", async () => {
    let calls = 0;
    const { clock, monitor } = harness(async () => { calls++; return calls === 1 ? reachable() : offline(); });
    monitor.requestCheck(); await flush();
    monitor.requestCheck(); await flush();
    expect(monitor.snapshot().state).toBe("checking");
    expect(calls).toBe(1);
    await clock.advance(9_999); expect(calls).toBe(1);
    await clock.advance(1); expect(calls).toBe(2);
    expect(monitor.snapshot().state).toBe("unreachable");
  });

  it("holds all recovery waiters through a long outage and resumes them together", async () => {
    let connected = false;
    let calls = 0;
    const { clock, monitor } = harness(async () => { calls++; return connected ? reachable() : offline(); });
    monitor.requestCheck(); await flush();
    let resumed = 0;
    const waiters = Array.from({ length: 64 }, () => monitor.waitForRecovery(new AbortController().signal).then(() => { resumed++; }));
    for (let i = 0; i < 90; i++) {
      for (let worker = 0; worker < 8; worker++) monitor.requestCheck();
      await clock.advance(10_000);
      expect(clock.waiters.size).toBe(1);
    }
    expect(resumed).toBe(0);
    expect(calls).toBe(91);
    connected = true; await clock.advance(10_000); await Promise.all(waiters);
    expect(resumed).toBe(64);
    expect(monitor.snapshot().recoveredAtMs).toBe(clock.now());
    expect(clock.waiters.size).toBe(0);
  });

  it("times out one noncooperative probe without launching overlapping work", async () => {
    let calls = 0;
    let finish!: (value: PrismConnectivityProbeResult) => void;
    let probeSignal!: AbortSignal;
    const { clock, monitor } = harness(async signal => {
      calls++; probeSignal = signal; return new Promise(resolve => { finish = resolve; });
    });
    monitor.requestCheck(); await flush(); await clock.advance(5_000);
    expect(probeSignal.aborted).toBe(true);
    expect(monitor.snapshot()).toMatchObject({ state: "unreachable", reason: "timeout" });
    for (let i = 0; i < 10; i++) { monitor.requestCheck(); await clock.advance(10_000); }
    expect(calls).toBe(1);
    expect(clock.waiters.size).toBe(0);
    finish(reachable()); await flush();
    expect(monitor.snapshot().state).toBe("unreachable");
    await clock.advance(10_000);
    expect(calls).toBe(2);
    monitor.close(); finish(reachable()); await flush();
    expect(monitor.snapshot().state).toBe("closed");
    expect(clock.waiters.size).toBe(0);
  });

  it("cancels one waiter without releasing its peers or cancelling the shared probe", async () => {
    const { clock, monitor } = harness(async () => offline());
    monitor.requestCheck(); await flush();
    const controller = new AbortController();
    const waiting = monitor.waitForRecovery(controller.signal).catch(error => error);
    controller.abort();
    expect(await waiting).toMatchObject({ name: "AbortError" });
    expect(monitor.isRecoveryBlocked()).toBe(true);
    expect(clock.waiters.size).toBe(1);
    const peer = monitor.waitForRecovery(new AbortController().signal).catch(error => error);
    monitor.close();
    expect(await peer).toMatchObject({ name: "AbortError" });
    expect(clock.waiters.size).toBe(0);
  });

  it("cleans up subscriptions and rejects additions beyond the explicit bound", () => {
    const { monitor } = harness(async () => reachable());
    const subscriptions = Array.from({ length: 256 }, () => monitor.subscribe(() => {}));
    expect(() => monitor.subscribe(() => {})).toThrow("listener bound");
    subscriptions[0]!();
    expect(() => monitor.subscribe(() => {})).not.toThrow();
    monitor.close();
    expect(() => monitor.subscribe(() => {})).not.toThrow();
  });

  it("publishes only allowlisted connectivity categories", () => {
    expect(() => assertSafeObservabilityEvent(observabilityEvent("prism.connectivity.changed", {
      connectivity_state: "unreachable", connectivity_reason: "dns",
    }))).not.toThrow();
    const invalidFields: Array<Record<string, JsonValue>> = [
      { connectivity_state: "unreachable", connectivity_reason: "private host" },
      { connectivity_state: "private host", connectivity_reason: "dns" },
      { connectivity_state: "unreachable", connectivity_reason: "dns", interface: "Ethernet" },
      { connectivity_state: "unreachable" },
    ];
    for (const fields of invalidFields) expect(() => assertSafeObservabilityEvent(observabilityEvent("prism.connectivity.changed", fields))).toThrow();
  });
});
