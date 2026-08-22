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
  next: () => Promise<HeartbeatResult>;
  constructor(readonly clock: ManualClock, next?: () => Promise<HeartbeatResult>) {
    this.next = next ?? (async () => ({ ok: true, status: 200, durationMs: 1, sessionId: null }));
  }
  async heartbeat(): Promise<HeartbeatResult> {
    this.calls.push(this.clock.now());
    this.active++;
    this.maxActive = Math.max(this.maxActive, this.active);
    try { return await this.next(); }
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
    const heartbeat = new SandboxHeartbeat({ projectId: PROJECTS[0], gateway, clock, initialDelayMs: 10_000 });
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
});
