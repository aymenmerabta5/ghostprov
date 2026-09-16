# V2 Credit and Completion Recovery Implementation Plan

> Historical plan: its two-second stall examples describe the revision implemented at that time. The selected host and R10 peer now use a sixty-second default; frozen legacy sidecar releases remain unchanged.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent idle-time ACK clocks and drained congestion from latching sandbox credits at zero, and prevent extreme model durations from permanently removing healthy runtimes from routing.

**Architecture:** Reliable delivery owns an active-window progress timestamp used by both diagnostics and sidecar congestion. Fully drained transports clear congestion immediately. A separate bounded, time-decaying completion estimator preserves the existing routing interface while making outliers temporary.

**Tech Stack:** Bun 1.4, TypeScript 7, Bun test with `--isolate`, Yjs Awareness, existing `Clock` and V2 transport abstractions.

## Global Constraints

- Work directly on `master` with the user's explicit authorization.
- Do not stop, restart, or replace the live router on `127.0.0.1:18787`.
- Add no dependency and change no public API shape.
- Preserve reliable delivery, terminal proof, cancellation quarantine, and reroute safety.
- Follow test-first red→green cycles and commit each independently reviewable fix.

---

### Task 1: Track ACK Age Only During an Active Reliable Flight

**Files:**
- Modify: `tests/v2-delivery-timing.test.ts`
- Modify: `src/v2/protocol/delivery-timing.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `src/v2/transport/session.ts`

**Interfaces:**
- Consumes: reliable `published`, `acknowledged`, `retransmitted`, and `reset` transitions.
- Produces: `ReliableDeliveryTiming.ackProgressAgeMs(nowMs): number`.

- [ ] **Step 1: Write the failing active-flight clock test**

Add a test proving:

```ts
const timing = new ReliableDeliveryTiming({ epoch: 1 });
expect(timing.ackProgressAgeMs(50_000)).toBe(0);

timing.published([{ epoch: 1, sequence: 0 }], 50_000);
expect(timing.ackProgressAgeMs(50_400)).toBe(400);

timing.published([{ epoch: 1, sequence: 0 }], 50_600);
expect(timing.ackProgressAgeMs(50_700)).toBe(700);

timing.retransmitted(50_800);
expect(timing.ackProgressAgeMs(50_900)).toBe(900);

timing.published([{ epoch: 1, sequence: 1 }], 51_000);
timing.acknowledged({ epoch: 1, sequence: 0 }, true, 51_100);
expect(timing.ackProgressAgeMs(51_300)).toBe(200);

timing.acknowledged({ epoch: 1, sequence: 1 }, false, 51_400);
expect(timing.ackProgressAgeMs(500_000)).toBe(0);
```

Also verify epoch reset clears age and a clock earlier than the latest progress boundary is rejected.

- [ ] **Step 2: Run RED**

Run: `bun test --isolate tests/v2-delivery-timing.test.ts`

Expected: FAIL because `ackProgressAgeMs` does not exist.

- [ ] **Step 3: Implement the progress boundary**

Add `#lastProgressAtMs: number | null = null` to `ReliableDeliveryTiming`.

- First publication into an empty tracked set records `nowMs`.
- ACK with outstanding frames records `nowMs`.
- ACK that empties the window records `null`.
- Retransmission and duplicate publication do not change it.
- Reset records `null`.

Expose:

```ts
ackProgressAgeMs(nowMs: number): number {
  assertTime(nowMs);
  if (this.#lastProgressAtMs === null) return 0;
  if (nowMs < this.#lastProgressAtMs) {
    throw new RangeError("delivery progress clock moved backwards");
  }
  return nowMs - this.#lastProgressAtMs;
}
```

- [ ] **Step 4: Use the delivery clock everywhere**

Remove the sidecar peer's `#lastAckAt` and host session's `#lastAckAtMs`. Replace both diagnostic age calculations and sidecar congestion input with `this.#delivery.ackProgressAgeMs(now)`.

- [ ] **Step 5: Verify GREEN**

Run:

```powershell
bun test --isolate tests/v2-delivery-timing.test.ts tests/v2-transport-session.test.ts tests/v2-deno-awareness-peer.test.ts
bun run typecheck
```

Expected: delivery age, diagnostics, retransmission, split-flight, and transport tests pass.

- [ ] **Step 6: Commit Task 1**

```powershell
git add -- src/v2/protocol/delivery-timing.ts sandbox/v2/awareness-peer.ts src/v2/transport/session.ts tests/v2-delivery-timing.test.ts
git commit -m "fix: anchor congestion age to active flights"
```

---

### Task 2: Restore Credits When Congestion Fully Drains

**Files:**
- Modify: `tests/v2-congestion.test.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`
- Modify: `src/v2/transport/congestion.ts`

**Interfaces:**
- Consumes: `CongestionSample` scheduler/window state.
- Produces: unchanged `TransportCongestion.update` and `congested` interfaces with a drained-state terminal rule.

- [ ] **Step 1: Write failing drained-recovery tests**

Update the existing low-water test so partial work still requires two samples:

```ts
const lowActive = s({
  schedulerFrames: 1,
  schedulerBytes: 1024,
  windowFrames: 1,
  ackProgressAgeMs: 100,
});
expect(congestion.update(s({ schedulerFrames: 128 }))).toBe(true);
expect(congestion.update(lowActive)).toBe(true);
expect(congestion.update(lowActive)).toBe(false);
```

Add:

```ts
const congestion = new TransportCongestion();
expect(congestion.update(s({ windowFrames: 1, ackProgressAgeMs: 2_000 }))).toBe(true);
expect(congestion.update(s())).toBe(false);
```

Add a Deno peer integration test that publishes one response frame, waits past the real two-second stall threshold, sends a non-progressing host envelope to force zero credits, then ACKs the only frame and expects the next control envelope to restore credits immediately.

- [ ] **Step 2: Run RED**

Run: `bun test --isolate tests/v2-congestion.test.ts tests/v2-deno-awareness-peer.test.ts`

Expected: drained congestion remains latched after the single ACK and the integration test observes zero credits.

- [ ] **Step 3: Implement explicit drained recovery**

Refactor `TransportCongestion.update` for readability and add:

```ts
const drained = value.schedulerFrames === 0 &&
  value.schedulerBytes === 0 &&
  value.windowFrames === 0;
if (this.#congested && drained) {
  this.#congested = false;
  this.#healthySamples = 0;
  return false;
}
```

Keep existing high thresholds and two-sample low-water hysteresis whenever work remains.

- [ ] **Step 4: Verify GREEN**

Run: `bun test --isolate tests/v2-congestion.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-adaptive-capacity.test.ts`

Expected: idle recovery passes while real active-window stalls and two-sample hysteresis remain covered.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/v2/transport/congestion.ts tests/v2-congestion.test.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "fix: restore sidecar credits after drain"
```

---

### Task 3: Add a Bounded Decaying Completion Estimate

**Files:**
- Create: `src/v2/runtime-completion.ts`
- Create: `tests/v2-runtime-completion.test.ts`

**Interfaces:**
- Consumes: `Clock` and `SYSTEM_CLOCK`.
- Produces: `DecayingRuntimeCompletionEstimate.observe(durationMs)` and `.value()`.

- [ ] **Step 1: Write failing estimator tests**

Use a manual clock and prove:

```ts
const estimate = new DecayingRuntimeCompletionEstimate({ clock });
expect(estimate.value()).toBe(10_000);

estimate.observe(553_855);
expect(estimate.value()).toBe(20_000);

estimate.observe(553_855);
expect(estimate.value()).toBe(28_000);

clock.value = 60_000;
expect(estimate.value()).toBe(19_000);

clock.value = 120_000;
expect(estimate.value()).toBe(14_500);
```

Also prove a 6,000-ms sample produces 9,200 ms from baseline and invalid configuration, negative/non-finite samples, and backward clocks throw.

- [ ] **Step 2: Run RED**

Run: `bun test --isolate tests/v2-runtime-completion.test.ts`

Expected: FAIL because the estimator module does not exist.

- [ ] **Step 3: Implement the estimator**

Create a class with defaults `baselineMs=10_000`, `maximumSampleMs=60_000`, `weight=0.20`, and `halfLifeMs=60_000`.

Before every observation/read, decay toward baseline:

```ts
const factor = Math.pow(0.5, elapsedMs / this.#halfLifeMs);
this.#estimateMs = this.#baselineMs +
  (this.#estimateMs - this.#baselineMs) * factor;
```

Clamp valid samples to `[1, maximumSampleMs]`, then apply the weighted update.

- [ ] **Step 4: Verify GREEN**

Run: `bun test --isolate tests/v2-runtime-completion.test.ts`

Expected: all bounds, outlier, decay, and validation tests pass.

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- src/v2/runtime-completion.ts tests/v2-runtime-completion.test.ts
git commit -m "feat: add decaying runtime completion estimate"
```

---

### Task 4: Integrate Completion Recovery and Document Both Fixes

**Files:**
- Modify: `src/v2/runtime.ts`
- Modify: `tests/v2-runtime.test.ts`
- Modify: `README.md`
- Modify: `tests/documentation.test.ts`

**Interfaces:**
- Consumes: `DecayingRuntimeCompletionEstimate` from Task 3.
- Produces: the existing finite `completionEwmaMs` runtime snapshot field with bounded recovery.

- [ ] **Step 1: Write failing runtime integration tests**

After runtime startup, assert the neutral estimate is 10,000 ms. Complete one request after advancing the manual clock by 553,855 ms and expect exactly 20,000 ms, then advance sixty idle seconds and expect 15,000 ms. Complete a normal request and verify the estimate moves toward that sample rather than becoming unbounded.

- [ ] **Step 2: Run RED**

Run: `bun test --isolate tests/v2-runtime.test.ts`

Expected: the current unbounded EWMA reports approximately 553,855 ms for its first sample and never decays.

- [ ] **Step 3: Integrate the estimator**

Replace nullable `#completionEwmaMs` with one `DecayingRuntimeCompletionEstimate` initialized from the runtime clock. On successful terminal settlement call `observe(sample)`. In `snapshot`, expose `value()` as `completionEwmaMs`. Do not observe cancellations or failures.

- [ ] **Step 4: Add documentation regression and README text**

Document that:

```md
- ACK-stall age starts with the active reliable flight; idle time never suppresses credits, and a fully drained transport restores credits immediately.
- Per-runtime completion estimates cap extreme model durations and decay halfway toward a neutral ten-second baseline every idle minute.
```

Add `tests/v2-runtime-completion.test.ts` to the README test inventory and assert the two statements in `tests/documentation.test.ts`.

- [ ] **Step 5: Run focused verification**

```powershell
bun run typecheck
bun test --isolate tests/v2-delivery-timing.test.ts tests/v2-congestion.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-runtime-completion.test.ts tests/v2-runtime.test.ts tests/router-policy.test.ts tests/documentation.test.ts
```

Expected: all focused tests and typecheck pass.

- [ ] **Step 6: Commit Task 4**

```powershell
git add -- src/v2/runtime.ts tests/v2-runtime.test.ts README.md tests/documentation.test.ts
git commit -m "fix: recover runtime completion routing score"
```

---

### Task 5: Full Verification and Delivery

**Files:**
- Verify: complete repository and commit range.

**Interfaces:**
- Consumes: Tasks 1–4.
- Produces: clean, pushed `master` while preserving the live router process.

- [ ] **Step 1: Run static, daemon, and dependency gates**

```powershell
bun run typecheck
bun run test:daemon
bun audit
```

- [ ] **Step 2: Run all tests**

Run: `bun test --isolate`

Expected: zero failures across the full suite.

- [ ] **Step 3: Run protected smoke**

```powershell
$env:GP_SMOKE_PROTECTED_ROOT='http://127.0.0.1:18787'
bun run verify:v2-smoke
```

Expected: HTTP and WebSocket pass on an ephemeral candidate and the protected PID is identical before/after.

- [ ] **Step 4: Review and push**

Run `git diff --check`, review the production/test diff and commit range, push `master`, fetch `origin/master`, and verify both SHAs match with a clean working tree.
