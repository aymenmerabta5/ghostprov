import { describe, expect, it } from "bun:test";
import type { Clock } from "../src/core/ports";
import {
  SandboxHeartbeat,
  heartbeatPhaseOffset,
} from "../src/sandbox/heartbeat";
import { projectId } from "../src/sandbox/types";
import type { HeartbeatResult } from "../src/sandbox/gateway";

class ManualClock implements Clock {
  value = 0;
  waiters: Array<{ deadline: number; resolve: () => void; reject: (error: unknown) => void; signal?: AbortSignal; onAbort?: () => void }> = [];
  now(): number { return this.value; }
  sleep(ms: number, signal?: AbortSignal): Promise<void> { return this.waitUntil(this.value + ms, signal); }
  waitUntil(deadline: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new DOMException("aborted", "AbortError"));
    if (deadline <= this.value) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const waiter: (typeof this.waiters)[number] = { deadline, resolve, reject, signal };
      waiter.onAbort = () => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new DOMException("aborted", "AbortError"));
      };
      signal?.addEventListener("abort", waiter.onAbort, { once: true });
      this.waiters.push(waiter);
    });
  }
  async advanceBy(ms: number): Promise<void> {
    this.value += ms;
    for (const waiter of [...this.waiters]) {
      if (waiter.signal?.aborted) {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        waiter.reject(new DOMException("aborted", "AbortError"));
      } else if (waiter.deadline <= this.value) {
        this.waiters.splice(this.waiters.indexOf(waiter), 1);
        waiter.signal?.removeEventListener("abort", waiter.onAbort!);
        waiter.resolve();
      }
    }
    for (let index = 0; index < 5; index++) await Promise.resolve();
  }
}

class FakeHeartbeatGateway {
  calls: number[] = [];
  active = 0;
  maxActive = 0;
  next: (signal?: AbortSignal) => Promise<HeartbeatResult>;
  constructor(readonly clock: ManualClock, next?: (signal?: AbortSignal) => Promise<HeartbeatResult>) {
    this.next = next ?? (async () => ({ ok: true, status: 200, durationMs: 1, sessionId: null }));
  }
  async heartbeat(signal?: AbortSignal): Promise<HeartbeatResult> {
    this.calls.push(this.clock.now());
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    try { return await this.next(signal); }
    finally { this.active--; }
  }
}

const PROJECTS = [
  projectId("e631d12c-d917-4434-b360-41b4715d6679"),
  projectId("69288e8b-c183-45f3-9131-d64b05b98928"),
  projectId("11111111-1111-4111-8111-111111111111"),
  projectId("22222222-2222-4222-8222-222222222222"),
];

describe("SandboxHeartbeat", () => {
  it("phases many projects deterministically while preserving exact cadence", async () => {
    const projects = Array.from({ length: 32 }, (_, index) => projectId(
      `${index.toString(16).padStart(8, "0")}-0000-4000-8000-${index.toString(16).padStart(12, "0")}`,
    ));
    const offsets = projects.map(project => heartbeatPhaseOffset(project));
    expect(offsets.every(offset => offset >= 0 && offset < 10_000)).toBe(true);
    expect(new Set(offsets).size).toBeGreaterThan(16);
    expect(projects.map(project => heartbeatPhaseOffset(project))).toEqual(offsets);

    const clock = new ManualClock();
    const gateway = new FakeHeartbeatGateway(clock);
    const heartbeat = new SandboxHeartbeat({
      projectId: projects[0],
      gateway,
      clock,
    });
    heartbeat.start();
    await clock.advanceBy(offsets[0]);
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    expect(gateway.calls).toEqual([offsets[0], offsets[0] + 10_000, offsets[0] + 20_000]);
    await heartbeat.stop();
  });

  it("heartbeats every sandbox on independent ten-second deadlines", async () => {
    const clock = new ManualClock();
    const gateways = PROJECTS.map(() => new FakeHeartbeatGateway(clock));
    const heartbeats = PROJECTS.map((project, index) => new SandboxHeartbeat({
      projectId: project,
      gateway: gateways[index],
      clock,
      initialDelayMs: 10_000,
    }));
    heartbeats.forEach(heartbeat => heartbeat.start());

    for (let tick = 0; tick < 6; tick++) await clock.advanceBy(10_000);

    for (const gateway of gateways) {
      expect(gateway.calls).toEqual([10_000, 20_000, 30_000, 40_000, 50_000, 60_000]);
      expect(gateway.maxActive).toBe(1);
    }
    await Promise.all(heartbeats.map(heartbeat => heartbeat.stop()));
  });

  it("never overlaps or queues catch-up heartbeats when one request is slow", async () => {
    const clock = new ManualClock();
    let release!: (result: HeartbeatResult) => void;
    const pending = new Promise<HeartbeatResult>(resolve => { release = resolve; });
    const gateway = new FakeHeartbeatGateway(clock, () => pending);
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      requestTimeoutMs: 60_000,
    });
    heartbeat.start();
    await clock.advanceBy(10_000);
    await clock.advanceBy(30_000);
    expect(gateway.calls).toEqual([10_000]);
    expect(gateway.maxActive).toBe(1);
    release({ ok: true, status: 200, durationMs: 30_000, sessionId: null });
    for (let index = 0; index < 5; index++) await Promise.resolve();
    await clock.advanceBy(10_000);
    expect(gateway.calls).toEqual([10_000, 50_000]);
    await heartbeat.stop();
  });

  it("reports expired sessions and transient failures without throwing the loop", async () => {
    const clock = new ManualClock();
    const statuses = [409, 503, 200];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const status = statuses.shift()!;
      return { ok: status === 200, status, durationMs: 1, sessionId: null };
    });
    const expired: number[] = [];
    const transient: Array<number | null> = [];
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      onExpired: status => expired.push(status),
      onTransientFailure: status => transient.push(status),
    });
    heartbeat.start();
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    expect(expired).toEqual([409]);
    expect(transient).toEqual([503]);
    expect(heartbeat.snapshot().successCount).toBe(1);
    await heartbeat.stop();
  });

  it("times out and aborts a hung request without blocking the scheduled loop", async () => {
    const clock = new ManualClock();
    const signals: AbortSignal[] = [];
    const gateway = new FakeHeartbeatGateway(clock, signal => {
      signals.push(signal!);
      return new Promise((_resolve, reject) => {
        signal!.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
      });
    });
    const transient: Array<number | null> = [];
    const attempts: Array<{
      outcome: string;
      status: number | null;
      errorName: string | null;
      durationMs: number;
      attempt: number;
      previousConsecutiveFailures: number;
      consecutiveFailures: number;
    }> = [];
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      requestTimeoutMs: 2_500,
      onTransientFailure: status => transient.push(status),
      onAttempt: diagnostic => attempts.push(diagnostic),
    });

    heartbeat.start();
    await clock.advanceBy(10_000);
    expect(heartbeat.snapshot().inFlight).toBe(true);
    await clock.advanceBy(2_499);
    expect(heartbeat.snapshot().failureCount).toBe(0);
    await clock.advanceBy(1);
    expect(signals[0].aborted).toBe(true);
    expect(transient).toEqual([null]);
    expect(heartbeat.snapshot()).toMatchObject({
      attemptCount: 1,
      failureCount: 1,
      consecutiveFailures: 1,
      lastAttemptAtMs: 10_000,
      lastFailureAtMs: 12_500,
      lastStatus: null,
      inFlight: false,
    });
    expect(attempts).toEqual([{
      outcome: "timeout",
      status: null,
      errorName: "Error",
      durationMs: 2_500,
      attempt: 1,
      previousConsecutiveFailures: 0,
      consecutiveFailures: 1,
    }]);

    await clock.advanceBy(7_500);
    expect(gateway.calls).toEqual([10_000, 20_000]);
    expect(gateway.maxActive).toBe(1);
    await heartbeat.stop();
  });

  it("retries authentication failures immediately and recovers without expiring", async () => {
    const clock = new ManualClock();
    const statuses = [401, 200];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const status = statuses.shift()!;
      return { ok: status === 200, status, durationMs: 1, sessionId: null };
    });
    const results: number[] = [];
    const expired: number[] = [];
    const successes: Array<ReturnType<SandboxHeartbeat["snapshot"]>> = [];
    let heartbeat!: SandboxHeartbeat;
    heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      onResult: result => results.push(result.status),
      onSuccess: () => successes.push(heartbeat.snapshot()),
      onExpired: status => expired.push(status),
    });

    heartbeat.start();
    await clock.advanceBy(10_000);
    for (let index = 0; index < 5; index++) await Promise.resolve();
    expect(gateway.calls).toEqual([10_000, 10_000]);
    expect(gateway.maxActive).toBe(1);
    expect(results).toEqual([401, 200]);
    expect(expired).toEqual([]);
    expect(heartbeat.snapshot()).toMatchObject({
      attemptCount: 2,
      successCount: 1,
      failureCount: 1,
      consecutiveFailures: 0,
      lastAttemptAtMs: 10_000,
      lastSuccessAtMs: 10_000,
      lastFailureAtMs: 10_000,
      lastStatus: 200,
    });
    expect(successes[0]).toMatchObject({ successCount: 1, consecutiveFailures: 0, lastStatus: 200 });
    await heartbeat.stop();
  });

  it("declares authentication expired only after one failed immediate retry", async () => {
    const clock = new ManualClock();
    const statuses = [403, 401];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const status = statuses.shift()!;
      return { ok: false, status, durationMs: 1, sessionId: null };
    });
    const expired: number[] = [];
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      onExpired: status => expired.push(status),
    });

    heartbeat.start();
    await clock.advanceBy(10_000);
    for (let index = 0; index < 5; index++) await Promise.resolve();
    expect(gateway.calls).toEqual([10_000, 10_000]);
    expect(gateway.maxActive).toBe(1);
    expect(expired).toEqual([401]);
    expect(heartbeat.snapshot()).toMatchObject({
      attemptCount: 2,
      successCount: 0,
      failureCount: 2,
      consecutiveFailures: 2,
      lastStatus: 401,
    });
    await heartbeat.stop();
  });

  it("awaits credential remediation before retrying an authentication failure", async () => {
    const clock = new ManualClock();
    const statuses = [401, 200];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const status = statuses.shift()!;
      return { ok: status === 200, status, durationMs: 1, sessionId: "session-1" };
    });
    let release!: () => void;
    const remediation = new Promise<void>(resolve => { release = resolve; });
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      onAuthenticationFailure: async () => {
        await remediation;
        return "retry";
      },
    });
    heartbeat.start();

    await clock.advanceBy(10_000);
    expect(gateway.calls).toEqual([10_000]);
    release();
    for (let index = 0; index < 10; index++) await Promise.resolve();

    expect(gateway.calls).toEqual([10_000, 10_000]);
    expect(heartbeat.snapshot().successCount).toBe(1);
    await heartbeat.stop();
  });

  it("fails closed without retrying the old token when remediation requires replacement", async () => {
    const clock = new ManualClock();
    const gateway = new FakeHeartbeatGateway(clock, async () => ({
      ok: false,
      status: 401,
      durationMs: 1,
      sessionId: "session-2",
    }));
    const expired: number[] = [];
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      onAuthenticationFailure: async () => "expired",
      onExpired: status => expired.push(status),
    });
    heartbeat.start();
    await clock.advanceBy(10_000);
    for (let index = 0; index < 10; index++) await Promise.resolve();

    expect(gateway.calls).toEqual([10_000]);
    expect(expired).toEqual([401]);
    await heartbeat.stop();
  });

  it("reports unreachable once per transient streak and resets the streak on success", async () => {
    const clock = new ManualClock();
    const outcomes: Array<number | Error> = [503, new Error("offline"), 502, 500, 200, 503, 503, 503];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const outcome = outcomes.shift()!;
      if (outcome instanceof Error) throw outcome;
      return { ok: outcome === 200, status: outcome, durationMs: 1, sessionId: null };
    });
    const transient: Array<number | null> = [];
    const unreachable: Array<number | null> = [];
    const attemptOutcomes: string[] = [];
    const heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      consecutiveTransientFailureThreshold: 3,
      onTransientFailure: status => transient.push(status),
      onUnreachable: status => unreachable.push(status),
      onAttempt: diagnostic => attemptOutcomes.push(diagnostic.outcome),
    });

    heartbeat.start();
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    expect(unreachable).toEqual([502]);
    expect(heartbeat.snapshot()).toMatchObject({ consecutiveFailures: 3, lastFailureAtMs: 30_000 });
    await clock.advanceBy(10_000);
    expect(unreachable).toEqual([502]);
    await clock.advanceBy(10_000);
    expect(heartbeat.snapshot().consecutiveFailures).toBe(0);
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    await clock.advanceBy(10_000);
    expect(unreachable).toEqual([502, 503]);
    expect(transient).toEqual([503, null, 502, 500, 503, 503, 503]);
    expect(attemptOutcomes).toEqual([
      "http_error",
      "transport_error",
      "http_error",
      "http_error",
      "success",
      "http_error",
      "http_error",
      "http_error",
    ]);
    await heartbeat.stop();
  });

  it("isolates callback exceptions and invokes result and success callbacks after state updates", async () => {
    const clock = new ManualClock();
    const statuses = [503, 200, 409, 200];
    const gateway = new FakeHeartbeatGateway(clock, async () => {
      const status = statuses.shift()!;
      return { ok: status === 200, status, durationMs: 1, sessionId: null };
    });
    const observed: Array<{ callback: string; snapshot: ReturnType<SandboxHeartbeat["snapshot"]> }> = [];
    let heartbeat!: SandboxHeartbeat;
    heartbeat = new SandboxHeartbeat({
      projectId: PROJECTS[0],
      gateway,
      clock,
      initialDelayMs: 10_000,
      consecutiveTransientFailureThreshold: 1,
      onResult: () => {
        observed.push({ callback: "result", snapshot: heartbeat.snapshot() });
        throw new Error("result callback failed");
      },
      onSuccess: () => {
        observed.push({ callback: "success", snapshot: heartbeat.snapshot() });
        throw new Error("success callback failed");
      },
      onTransientFailure: () => { throw new Error("transient callback failed"); },
      onUnreachable: () => { throw new Error("unreachable callback failed"); },
      onExpired: () => { throw new Error("expired callback failed"); },
    });

    heartbeat.start();
    for (let tick = 0; tick < 4; tick++) await clock.advanceBy(10_000);
    expect(gateway.calls).toEqual([10_000, 20_000, 30_000, 40_000]);
    expect(heartbeat.snapshot()).toMatchObject({ attemptCount: 4, successCount: 2, failureCount: 2 });
    expect(observed.find(item => item.callback === "success")?.snapshot).toMatchObject({
      successCount: 1,
      consecutiveFailures: 0,
      lastSuccessAtMs: 20_000,
      lastStatus: 200,
    });
    expect(observed[0].snapshot).toMatchObject({ failureCount: 1, lastFailureAtMs: 10_000, lastStatus: 503 });
    await heartbeat.stop();
  });

  it("validates timeout and transient threshold options", () => {
    const clock = new ManualClock();
    const gateway = new FakeHeartbeatGateway(clock);
    const options = { projectId: PROJECTS[0], gateway, clock };
    expect(() => new SandboxHeartbeat({ ...options, requestTimeoutMs: 0 })).toThrow(RangeError);
    expect(() => new SandboxHeartbeat({ ...options, requestTimeoutMs: Number.POSITIVE_INFINITY })).toThrow(RangeError);
    expect(() => new SandboxHeartbeat({ ...options, consecutiveTransientFailureThreshold: 0 })).toThrow(RangeError);
    expect(() => new SandboxHeartbeat({ ...options, consecutiveTransientFailureThreshold: 1.5 })).toThrow(RangeError);
  });
});
