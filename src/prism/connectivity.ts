import { networkInterfaces } from "node:os";
import { SYSTEM_CLOCK, type Clock, type FetchLike } from "../core/ports";
import type { PRISM_CONNECTIVITY_REASONS, PRISM_CONNECTIVITY_STATES } from "../observability/events";

export type PrismConnectivityState = typeof PRISM_CONNECTIVITY_STATES[number];
export type PrismConnectivityReason = typeof PRISM_CONNECTIVITY_REASONS[number];
export interface PrismConnectivitySnapshot {
  readonly state: PrismConnectivityState;
  readonly reason: PrismConnectivityReason;
  readonly checkedAtMs: number | null;
  readonly recoveredAtMs: number | null;
}
export interface RecoveryConnectivity {
  snapshot(): Readonly<PrismConnectivitySnapshot>;
  requestCheck(): void;
  isRecoveryBlocked(): boolean;
  waitForRecovery(signal: AbortSignal): Promise<void>;
  subscribe(listener: () => void): () => void;
}
export interface PrismConnectivityProbeResult {
  readonly reachable: boolean;
  readonly reason: Exclude<PrismConnectivityReason, "unobserved" | "checking" | "closed">;
}

const MAX_LISTENERS = 256;
const DEFAULT_PROBE_TIMEOUT_MS = 5_000;
const DEFAULT_RECHECK_MS = 10_000;

/** No cookie jar, provider request, OS setting changes, or ICMP dependency. */
export function createPrismConnectivityProbe(origin: string, options: {
  readonly fetch?: FetchLike;
  readonly interfaces?: typeof networkInterfaces;
} = {}): (signal: AbortSignal) => Promise<PrismConnectivityProbeResult> {
  const endpoint = new URL(origin);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password ||
    endpoint.search || endpoint.hash) throw new TypeError("Prism connectivity requires a credential-free HTTPS origin");
  endpoint.pathname = "/";
  const fetcher = options.fetch ?? fetch;
  const interfaces = options.interfaces ?? networkInterfaces;
  return async signal => {
    signal.throwIfAborted();
    try {
      // Both Ethernet and Wi-Fi are covered. An assigned non-loopback address
      // alone cannot prove internet access; HTTPS must also complete below.
      const entries = Object.values(interfaces());
      if (!entries.some(addresses => addresses?.some(address => !address.internal &&
        address.address !== "0.0.0.0" && address.address !== "::"))) {
        return { reachable: false, reason: "no_interface" };
      }
    } catch { /* Interface inspection may be unsupported; use the actual endpoint. */ }
    try {
      const response = await fetcher(endpoint, {
        method: "HEAD", credentials: "omit", redirect: "manual", cache: "no-store", signal,
      });
      void response.body?.cancel().catch(() => undefined);
      signal.throwIfAborted();
      // A 4xx at the unauthenticated root still proves DNS, TCP and TLS. Rate
      // limiting or server failure means the shared control plane is unusable.
      const reachable = response.status >= 200 && response.status < 500 &&
        response.status !== 408 && response.status !== 429;
      return { reachable, reason: reachable ? "reachable" : "service_unavailable" };
    } catch (error) {
      if (signal.aborted) return { reachable: false, reason: "timeout" };
      const code = error && typeof error === "object" ? Object.getOwnPropertyDescriptor(error, "code")?.value : null;
      return { reachable: false, reason: code === "ENOTFOUND" || code === "EAI_AGAIN" || code === "DNS_NOTFOUND"
        ? "dns" : "transport" };
    }
  };
}

/** One host-owned probe and retry timer, shared by every runtime and fleet guard. */
export class PrismConnectivityMonitor implements RecoveryConnectivity {
  readonly #clock: Clock;
  readonly #probe: (signal: AbortSignal) => Promise<PrismConnectivityProbeResult>;
  readonly #timeoutMs: number;
  readonly #recheckMs: number;
  readonly #lifetime = new AbortController();
  readonly #listeners = new Set<() => void>();
  readonly #onTransition?: (snapshot: Readonly<PrismConnectivitySnapshot>) => void;
  #snapshot: Readonly<PrismConnectivitySnapshot> = Object.freeze({
    state: "unknown", reason: "unobserved", checkedAtMs: null, recoveredAtMs: null,
  });
  #loop: Promise<void> | null = null;
  #attempt: AbortController | null = null;

  constructor(options: {
    readonly probe: (signal: AbortSignal) => Promise<PrismConnectivityProbeResult>;
    readonly clock?: Clock;
    readonly timeoutMs?: number;
    readonly recheckMs?: number;
    readonly onTransition?: (snapshot: Readonly<PrismConnectivitySnapshot>) => void;
  }) {
    this.#probe = options.probe;
    this.#clock = options.clock ?? SYSTEM_CLOCK;
    this.#timeoutMs = options.timeoutMs ?? DEFAULT_PROBE_TIMEOUT_MS;
    this.#recheckMs = options.recheckMs ?? DEFAULT_RECHECK_MS;
    this.#onTransition = options.onTransition;
    if (!Number.isSafeInteger(this.#timeoutMs) || this.#timeoutMs < 1 || this.#timeoutMs > 10_000 ||
      !Number.isSafeInteger(this.#recheckMs) || this.#recheckMs < 1 || this.#recheckMs > 30_000) {
      throw new RangeError("Connectivity probe bounds are invalid");
    }
  }

  snapshot(): Readonly<PrismConnectivitySnapshot> { return this.#snapshot; }
  isRecoveryBlocked(): boolean { return this.#snapshot.state === "checking" || this.#snapshot.state === "unreachable"; }

  requestCheck(): void {
    if (this.#lifetime.signal.aborted || this.#loop) return;
    // New transport evidence invalidates a cached success immediately. Keep
    // the probe cadence bounded without treating a stale success as proof that
    // a newly disconnected host can safely retire a runtime.
    const loop = Promise.resolve().then(() => this.#run());
    this.#loop = loop;
    this.#set("checking", "checking");
    void loop.catch(() => undefined).finally(() => { if (this.#loop === loop) this.#loop = null; });
  }

  subscribe(listener: () => void): () => void {
    if (this.#lifetime.signal.aborted) return () => {};
    if (this.#listeners.size >= MAX_LISTENERS) throw new Error("Connectivity listener bound exceeded");
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  waitForRecovery(signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(signal.reason);
    if (this.#lifetime.signal.aborted) return Promise.reject(new DOMException("Connectivity monitor closed", "AbortError"));
    if (!this.isRecoveryBlocked()) return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const cleanup = () => { unsubscribe(); signal.removeEventListener("abort", abort); };
      const abort = () => { cleanup(); reject(signal.reason); };
      const changed = () => {
        if (this.#lifetime.signal.aborted) { cleanup(); reject(new DOMException("Connectivity monitor closed", "AbortError")); }
        else if (!this.isRecoveryBlocked()) { cleanup(); resolve(); }
      };
      const unsubscribe = this.subscribe(changed);
      signal.addEventListener("abort", abort, { once: true });
      if (signal.aborted) abort(); else changed();
    });
  }

  close(): void {
    if (this.#lifetime.signal.aborted) return;
    this.#lifetime.abort();
    this.#attempt?.abort();
    this.#set("closed", "closed");
    this.#listeners.clear();
  }

  async #run(): Promise<void> {
    const signal = this.#lifetime.signal;
    const previousCheck = this.#snapshot.checkedAtMs;
    if (previousCheck !== null) await this.#clock.waitUntil(previousCheck + this.#recheckMs, signal);
    while (!signal.aborted) {
      const attempt = new AbortController();
      const timer = new AbortController();
      this.#attempt = attempt;
      const abort = () => { attempt.abort(); timer.abort(); };
      signal.addEventListener("abort", abort, { once: true });
      const deadline = this.#clock.now() + this.#timeoutMs;
      void this.#clock.waitUntil(deadline, timer.signal).then(() => {
        if (signal.aborted) return;
        attempt.abort();
        this.#set("unreachable", "timeout");
      }, () => undefined);
      let result: PrismConnectivityProbeResult;
      try {
        // Retain the physical owner even after timeout. An uncooperative probe
        // can never overlap or accumulate probes on the next retry tick.
        result = await this.#probe(attempt.signal);
      } catch { result = { reachable: false, reason: "transport" }; }
      finally {
        timer.abort();
        signal.removeEventListener("abort", abort);
        if (this.#attempt === attempt) this.#attempt = null;
      }
      if (signal.aborted) return;
      if (attempt.signal.aborted || this.#clock.now() >= deadline) result = { reachable: false, reason: "timeout" };
      this.#set(result.reachable ? "reachable" : "unreachable", result.reason);
      if (result.reachable) return;
      await this.#clock.sleep(this.#recheckMs, signal);
    }
  }

  #set(state: PrismConnectivityState, reason: PrismConnectivityReason): void {
    const previous = this.#snapshot;
    const now = this.#clock.now();
    this.#snapshot = Object.freeze({ state, reason,
      checkedAtMs: state === "reachable" || state === "unreachable" ? now : previous.checkedAtMs,
      recoveredAtMs: state === "reachable" && previous.state === "unreachable"
        ? now : previous.recoveredAtMs,
    });
    for (const listener of this.#listeners) { try { listener(); } catch {} }
    if (state !== previous.state || reason !== previous.reason) {
      try { this.#onTransition?.(this.#snapshot); } catch {}
    }
  }
}
