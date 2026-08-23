# GhostProv V2 Long-Run Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent adaptive-capacity and cancellation-quarantine exhaustion and return a typed queue error before client WebSocket idle timeout.

**Architecture:** Adaptive capacity uses measured active-flight health with an initial-capacity floor. A reliable `request_cancelled` sidecar frame releases only client-cancellation quarantine after local lease cleanup. Router jobs retain an absolute, configurable queue deadline across safe retry.

**Tech Stack:** Bun 1.4, TypeScript 7, Deno sidecar, split Y-Sweet Awareness lanes, `bun test --isolate`.

## Global Constraints

- Work directly on `master`; no branch, worktree, or subagent.
- Never stop or restart port 18787.
- Initial adaptive capacity 8 is the minimum; maximum remains 16.
- Genuine ambiguous failures remain quarantined.
- Queue timeout defaults to 30 seconds and never applies after dispatch.
- Preserve exact-once execution, terminal proof, replay, and public API compatibility.

---

### Task 1: Correct adaptive capacity semantics

**Files:**
- Modify: `src/v2/transport/adaptive-capacity.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `tests/v2-adaptive-capacity.test.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`

**Interfaces:**
- Removes: `TransportHealthSample.ackStalled`.
- Guarantees: `limit >= initial` for the controller lifetime.

- [ ] **Step 1: Write failing floor and idle tests**

```ts
const capacity = new AdaptiveTransportCapacity({ initial: 8, maximum: 16 });
expect(capacity.observe({ rttMs: 200, congested: false })).toBe(false);
expect(capacity.limit).toBe(8);
for (let index = 0; index < 64; index++) {
  capacity.observe({ rttMs: 250, congested: false });
}
expect(capacity.limit).toBe(16);
capacity.observe({ rttMs: 10_000, congested: false });
expect(capacity.limit).toBe(8);
capacity.observe({ rttMs: 10_000, congested: true });
expect(capacity.limit).toBe(8);
```

Add a peer regression where the clock advances through a long idle interval before a healthy ACK and advertised credits do not decrease.

- [ ] **Step 2: Run and verify RED**

Run: `bun test --isolate tests/v2-adaptive-capacity.test.ts tests/v2-deno-awareness-peer.test.ts`

Expected: current controller falls below 8 and still accepts `ackStalled`.

- [ ] **Step 3: Implement minimum-floor active-flight health**

Store `#minimum = options.initial`; severe reduction uses `Math.max(#minimum, Math.ceil(limit / 2))`. Remove `ackStalled` from sample and Deno peer calculation. Retain RTT and congestion thresholds plus healthy streak growth.

- [ ] **Step 4: Run focused tests and commit**

Run: `bun test --isolate tests/v2-adaptive-capacity.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-congestion.test.ts`

```powershell
git add -- src/v2/transport/adaptive-capacity.ts sandbox/v2/awareness-peer.ts tests/v2-adaptive-capacity.test.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "fix: keep v2 adaptive capacity above safe floor"
```

### Task 2: Definitive cancellation completion

**Files:**
- Modify: `src/v2/protocol/shared.ts`
- Modify: `src/v2/protocol/validate.ts`
- Modify: `src/v2/sidecar/engine.ts`
- Modify: `src/v2/transport/session.ts`
- Modify: `tests/v2-protocol.test.ts`
- Modify: `tests/v2-sidecar-engine.test.ts`
- Modify: `tests/v2-transport-session.test.ts`

**Interfaces:**
- Adds sidecar frame: `{ kind: "request_cancelled"; requestId: string }`.

- [ ] **Step 1: Write failing protocol/order/quarantine tests**

```ts
expect(parseFrame({
  kind: "request_cancelled",
  requestId: "req-1",
  epoch: 1,
  sequence: 0,
}, "sidecar")).toMatchObject({ kind: "request_cancelled" });
```

Sidecar test records `lease.release`, `active.delete`, and `emit:request_cancelled`, asserting acknowledgement is last. Host test commits, aborts, observes quarantine 1, delivers matching completion, and expects quarantine 0. Duplicate/unrelated completion must not change it; an ambiguous non-cancel response error must remain at 1.

- [ ] **Step 2: Run and verify RED**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-sidecar-engine.test.ts tests/v2-transport-session.test.ts`

Expected: new frame is unknown and cancellation quarantine remains allocated.

- [ ] **Step 3: Implement post-lease cancellation acknowledgement**

Add `cancelRequested` to active sidecar state. `request_cancel` sets it and aborts. Suppress ordinary `response_error` for an explicitly cancelled execution; in `finally`, after zero/release/delete, enqueue `request_cancelled`. If no active state exists, enqueue it immediately. Host accepts it only for `state.cancelled`, removes state, and decrements one existing quarantine.

- [ ] **Step 4: Run fault/replay tests and commit**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-sidecar-engine.test.ts tests/v2-transport-session.test.ts tests/v2-faults.test.ts tests/v2-large-payload.test.ts`

```powershell
git add -- src/v2/protocol/shared.ts src/v2/protocol/validate.ts src/v2/sidecar/engine.ts src/v2/transport/session.ts tests/v2-protocol.test.ts tests/v2-sidecar-engine.test.ts tests/v2-transport-session.test.ts
git commit -m "fix: release completed cancellation quarantine"
```

### Task 3: Bounded router queue deadline

**Files:**
- Modify: `src/router/router.ts`
- Modify: `src/config/multi-sandbox.ts`
- Modify: `src/server/system.ts`
- Modify: `src/http/errors.ts`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `tests/multi-sandbox-router.test.ts`
- Modify: `tests/multi-sandbox-config.test.ts`
- Modify: `tests/http-errors.test.ts`
- Modify: `tests/documentation.test.ts`

**Interfaces:**
- Adds `RouterCapacityTimeoutError` with status 503/code `router_capacity_timeout`.
- Adds `GP_ROUTER_QUEUE_TIMEOUT_MS`, default 30000, range 1000–300000.

- [ ] **Step 1: Write failing deadline and race tests**

Use an injected manual clock/timer or a 10 ms constructor timeout. With zero runtimes, expect typed rejection before 50 ms and one memory release. Add tests proving selection cancels the timer, retry retains the original deadline, abort/shutdown races reject/release once, and a later runtime cannot resolve an expired job.

- [ ] **Step 2: Run and verify RED**

Run: `bun test --isolate tests/multi-sandbox-router.test.ts tests/multi-sandbox-config.test.ts tests/http-errors.test.ts`

Expected: zero-capacity job remains pending until caller abort.

- [ ] **Step 3: Implement absolute queue deadline**

Add `queueDeadlineMs` and `queueTimer` to `QueuedJob`. `#enqueue` arms remaining time; `#nextJob`, removal, dispatch, abort, and shutdown clear it. Expiry removes only a still-queued job, releases its lease, emits failure, rejects with `RouterCapacityTimeoutError`, and schedules drain. Retry reuses the original deadline.

- [ ] **Step 4: Run router/docs tests and commit**

Run: `bun test --isolate tests/multi-sandbox-router.test.ts tests/multi-sandbox-config.test.ts tests/http-errors.test.ts tests/documentation.test.ts`

```powershell
git add -- src/router/router.ts src/config/multi-sandbox.ts src/server/system.ts src/http/errors.ts .env.example README.md tests/multi-sandbox-router.test.ts tests/multi-sandbox-config.test.ts tests/http-errors.test.ts tests/documentation.test.ts
git commit -m "fix: bound zero-capacity router queue time"
```

### Task 4: Exhaustion regression and complete verification

**Files:**
- Create: `tests/v2-long-run-stability.test.ts`
- Modify: `README.md` to list the test.

- [ ] **Step 1: Add deterministic exhaustion regression**

Simulate eight adaptive controllers through long idle periods and thirteen completed cancellations distributed across all runtimes. Assert aggregate usable capacity remains at least 51 (`8 * 8 - 13`) and no queued request survives past its deadline.

- [ ] **Step 2: Run focused and complete suites**

Run: `bun test --isolate tests/v2-long-run-stability.test.ts tests/v2-concurrency.test.ts tests/v2-split-flight-integration.test.ts tests/websocket.test.ts`

Run: `bun test --isolate`

- [ ] **Step 3: Run remaining gates**

Run: `bun run typecheck`

Run: `bun run test:daemon`

Run: `bun audit`

Run: `bun run verify:v2-smoke`

- [ ] **Step 4: Review and push**

Verify `git diff --check`, clean tracked status, live port 18787 ownership, and `HEAD == origin/master` after `git push origin master`. Do not restart the live router.

