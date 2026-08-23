# GhostProv V2 Awareness Pacing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove redundant cumulative Awareness publications and fixed-delay retransmission amplification under concurrent streaming.

**Architecture:** A single publication pacer per host/sidecar peer coalesces data across all request streams while urgent ACK/control state bypasses the 25 ms cadence. A bounded sequence RTT/delivery tracker owns first-send timestamps and progress-anchored retransmission deadlines; the unchanged 512 KiB ceiling bounds the enlarged 128-frame window.

**Tech Stack:** Bun 1.4, TypeScript 7, Deno sidecar bundle, Yjs/Y-Sweet Awareness, `bun test --isolate`.

## Global Constraints

- Work directly on `master`; do not create a branch or worktree.
- Do not stop or restart the live server on ports 18787 or 8787.
- Data publication interval: 25 ms.
- Response batch interval: 25 ms.
- Reliable window: 128 frames and 512 KiB.
- Physical frame ceiling: 16 KiB.
- Preserve exact-once claims, ordered transcript validation, replay, terminal quarantine, bounded backpressure, congestion routing, and the four-uncommitted-request cap.
- Use test-first red/green cycles and `bun test --isolate`.

---

### Task 1: Shared publication pacer

**Files:**
- Create: `src/v2/protocol/publication-pacer.ts`
- Create: `tests/v2-publication-pacer.test.ts`

**Interfaces:**
- Produces: `PublicationReason`, `PublicationTimer`, and `PublicationPacer` with `requestPaced()`, `requestUrgent()`, `flushNow()`, and `close()`.
- Consumes: no transport-specific frame types.

- [ ] **Step 1: Write deterministic failing cadence and urgent-coalescing tests**

```ts
const timer = new ManualPublicationTimer();
const reasons: PublicationReason[] = [];
const pacer = new PublicationPacer({ intervalMs: 25, timer, publish: reason => reasons.push(reason) });
pacer.flushNow("initial");
for (let elapsed = 5; elapsed <= 50; elapsed += 5) {
  timer.advanceBy(5);
  pacer.requestPaced();
}
expect(reasons).toEqual(["initial", "paced", "paced"]);

pacer.requestPaced();
pacer.requestUrgent();
pacer.requestUrgent();
timer.flushMicrotasks();
expect(reasons.at(-1)).toBe("urgent");
expect(timer.pendingTimers()).toBe(0);
```

- [ ] **Step 2: Run the new test and verify RED**

Run: `bun test --isolate tests/v2-publication-pacer.test.ts`

Expected: FAIL because `src/v2/protocol/publication-pacer.ts` does not exist.

- [ ] **Step 3: Implement the transport-independent pacer**

```ts
export type PublicationReason = "initial" | "paced" | "urgent" | "retransmission";

export interface PublicationTimer {
  now(): number;
  set(callback: () => void, delayMs: number): number | object;
  clear(handle: number | object): void;
  defer(callback: () => void): void;
}

const SYSTEM_PUBLICATION_TIMER: PublicationTimer = {
  now: () => performance.now(),
  set: (callback, delayMs) => setTimeout(callback, delayMs),
  clear: handle => clearTimeout(handle as ReturnType<typeof setTimeout>),
  defer: callback => queueMicrotask(callback),
};

export class PublicationPacer {
  readonly #intervalMs: number;
  readonly #timer: PublicationTimer;
  readonly #publish: (reason: PublicationReason) => void;
  #cadence: number | object | null = null;
  #lastPublishedAt: number | null = null;
  #urgentScheduled = false;
  #generation = 0;
  #dirty = false;
  #closed = false;

  constructor(options: {
    intervalMs?: number;
    timer?: PublicationTimer;
    publish(reason: PublicationReason): void;
  }) {
    this.#intervalMs = options.intervalMs ?? 25;
    if (!Number.isFinite(this.#intervalMs) || this.#intervalMs < 1) {
      throw new RangeError("publication interval must be positive");
    }
    this.#timer = options.timer ?? SYSTEM_PUBLICATION_TIMER;
    this.#publish = options.publish;
  }

  requestPaced(): void {
    if (this.#closed) return;
    this.#dirty = true;
    if (this.#cadence !== null || this.#urgentScheduled) return;
    const now = this.#timer.now();
    const deadline = (this.#lastPublishedAt ?? now) + this.#intervalMs;
    this.#cadence = this.#timer.set(() => {
      this.#cadence = null;
      if (!this.#closed && this.#dirty) this.#emit("paced");
    }, Math.max(0, deadline - now));
  }

  requestUrgent(): void {
    if (this.#closed) return;
    this.#dirty = true;
    if (this.#cadence !== null) {
      this.#timer.clear(this.#cadence);
      this.#cadence = null;
    }
    if (this.#urgentScheduled) return;
    this.#urgentScheduled = true;
    const generation = ++this.#generation;
    this.#timer.defer(() => {
      if (generation !== this.#generation) return;
      this.#urgentScheduled = false;
      if (!this.#closed && this.#dirty) this.#emit("urgent");
    });
  }

  flushNow(reason: PublicationReason): void {
    if (this.#closed) return;
    this.#cancelPending();
    this.#emit(reason);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#cancelPending();
  }

  #cancelPending(): void {
    if (this.#cadence !== null) this.#timer.clear(this.#cadence);
    this.#cadence = null;
    this.#dirty = false;
    this.#urgentScheduled = false;
    this.#generation++;
  }

  #emit(reason: PublicationReason): void {
    this.#dirty = false;
    this.#lastPublishedAt = this.#timer.now();
    this.#publish(reason);
  }
}
```

The implementation must retain one dirty bit, one cadence timer, and one microtask generation. `flushNow("retransmission")` must cancel a pending paced publication because the retransmission envelope already contains the newest cumulative state.

- [ ] **Step 4: Run the pacer tests and verify GREEN**

Run: `bun test --isolate tests/v2-publication-pacer.test.ts`

Expected: all pacer cadence, urgent-preemption, retransmission-flush, and close tests pass.

- [ ] **Step 5: Commit Task 1**

```powershell
git add -- src/v2/protocol/publication-pacer.ts tests/v2-publication-pacer.test.ts
git commit -m "feat: pace v2 awareness publications"
```

### Task 2: Sequence RTT and progress-anchored delivery timing

**Files:**
- Create: `src/v2/protocol/delivery-timing.ts`
- Create: `tests/v2-delivery-timing.test.ts`
- Test: `src/v2/protocol/retransmit.ts`

**Interfaces:**
- Consumes: `AckCursor`, `FrameIdentity`, and `computeV2RetransmitDelayMs()`.
- Produces: `ReliableDeliveryTiming.published()`, `.acknowledged()`, `.retransmitted()`, `.reset()`, `.nextDelayMs()`, `.roundTripEwmaMs`, `.attempt`, and `.trackedSequences`.

- [ ] **Step 1: Write failing first-send, EWMA, bound, epoch, and deadline tests**

```ts
const timing = new ReliableDeliveryTiming({ epoch: 1, minimumMs: 250, maxTracked: 128 });
timing.published([{ epoch: 1, sequence: 0 }], 100);
timing.published([{ epoch: 1, sequence: 0 }], 180);
expect(timing.nextDelayMs(100, 0)).toBe(250);
expect(timing.acknowledged({ epoch: 1, sequence: 0 }, false, 300)).toBe(true);
expect(timing.roundTripEwmaMs).toBe(200);

timing.published([{ epoch: 1, sequence: 1 }], 400);
const beforeDuplicate = timing.nextDelayMs(400, 0);
timing.published([{ epoch: 1, sequence: 1 }], 500);
expect(timing.nextDelayMs(500, 0)).toBe(beforeDuplicate! - 100);

timing.retransmitted(800);
expect(timing.attempt).toBe(1);
expect(timing.nextDelayMs(800, 0)).toBe(800);
```

Also assert that 129 unacknowledged unique sequences are rejected when `maxTracked` is 128, stale ACK epochs are ignored, and `reset(2)` clears samples/timestamps.

- [ ] **Step 2: Run the delivery timing test and verify RED**

Run: `bun test --isolate tests/v2-delivery-timing.test.ts`

Expected: FAIL because `ReliableDeliveryTiming` does not exist.

- [ ] **Step 3: Implement bounded delivery timing**

```ts
export class ReliableDeliveryTiming {
  #epoch: number;
  readonly #minimumMs: number;
  readonly #maxTracked: number;
  readonly #firstSentAt = new Map<number, number>();
  #lastAcknowledged = -1;
  #roundTripEwmaMs: number | null = null;
  #attempt = 0;
  #deadlineBaseMs: number | null = null;

  constructor(options: { epoch: number; minimumMs?: number; maxTracked?: number }) {
    this.#epoch = options.epoch;
    this.#minimumMs = options.minimumMs ?? 250;
    this.#maxTracked = options.maxTracked ?? GP_V2_WINDOW_MAX_FRAMES;
  }

  get roundTripEwmaMs(): number | null { return this.#roundTripEwmaMs; }
  get attempt(): number { return this.#attempt; }
  get trackedSequences(): number { return this.#firstSentAt.size; }

  published(frames: readonly FrameIdentity[], nowMs: number): void {
    for (const frame of frames) {
      if (frame.epoch !== this.#epoch || frame.sequence <= this.#lastAcknowledged) continue;
      if (this.#firstSentAt.has(frame.sequence)) continue;
      if (this.#firstSentAt.size >= this.#maxTracked) {
        throw new RangeError("reliable RTT tracker exceeds its sequence bound");
      }
      this.#firstSentAt.set(frame.sequence, nowMs);
    }
    if (this.#firstSentAt.size > 0 && this.#deadlineBaseMs === null) {
      this.#deadlineBaseMs = nowMs;
    }
  }

  acknowledged(cursor: AckCursor, hasOutstanding: boolean, nowMs: number): boolean {
    if (cursor.epoch < this.#epoch || cursor.sequence <= this.#lastAcknowledged) return false;
    if (cursor.epoch > this.#epoch) throw new RangeError("ACK belongs to a future delivery epoch");
    let sampledSequence = -1;
    let sampledAt: number | null = null;
    for (const [sequence, firstSentAt] of this.#firstSentAt) {
      if (sequence > cursor.sequence) continue;
      this.#firstSentAt.delete(sequence);
      if (sequence > sampledSequence) {
        sampledSequence = sequence;
        sampledAt = firstSentAt;
      }
    }
    this.#lastAcknowledged = cursor.sequence;
    if (sampledAt !== null) {
      const sample = Math.max(0, nowMs - sampledAt);
      this.#roundTripEwmaMs = this.#roundTripEwmaMs === null
        ? sample
        : this.#roundTripEwmaMs * 0.8 + sample * 0.2;
    }
    this.#attempt = 0;
    this.#deadlineBaseMs = hasOutstanding ? nowMs : null;
    return true;
  }

  retransmitted(nowMs: number): void {
    if (this.#deadlineBaseMs === null) throw new Error("cannot retransmit an empty delivery window");
    this.#attempt++;
    this.#deadlineBaseMs = nowMs;
  }

  nextDelayMs(nowMs: number, jitterSeed: number): number | null {
    if (this.#deadlineBaseMs === null) return null;
    const delay = computeV2RetransmitDelayMs(
      this.#roundTripEwmaMs,
      jitterSeed,
      this.#minimumMs,
      this.#attempt,
    );
    return Math.max(0, this.#deadlineBaseMs + delay - nowMs);
  }

  reset(epoch: number): void {
    if (!Number.isSafeInteger(epoch) || epoch <= this.#epoch) {
      throw new RangeError("delivery epoch must advance");
    }
    this.#epoch = epoch;
    this.#firstSentAt.clear();
    this.#lastAcknowledged = -1;
    this.#roundTripEwmaMs = null;
    this.#attempt = 0;
    this.#deadlineBaseMs = null;
  }
}
```

`published()` records only absent sequence timestamps and starts a deadline only when none exists. `acknowledged()` samples the highest newly covered tracked sequence, deletes every covered timestamp, applies `old * 0.8 + sample * 0.2`, and resets attempt/deadline only on ACK progress. `retransmitted()` increments the attempt and anchors the next deadline at the retransmission time.

- [ ] **Step 4: Run delivery and retransmission tests and verify GREEN**

Run: `bun test --isolate tests/v2-delivery-timing.test.ts tests/v2-transport-session.test.ts`

Expected: all timing tests and existing bounded-jitter tests pass.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/v2/protocol/delivery-timing.ts tests/v2-delivery-timing.test.ts
git commit -m "feat: measure v2 reliable sequence rtt"
```

### Task 3: Increase count window and response batch interval

**Files:**
- Modify: `src/v2/protocol/shared.ts`
- Modify: `src/v2/sidecar/response-batcher.ts`
- Modify: `tests/v2-protocol.test.ts`
- Modify: `tests/v2-window.test.ts`
- Modify: `tests/v2-response-batcher.test.ts`
- Modify: `tests/v2-batching-throughput.test.ts`

**Interfaces:**
- Produces: `GP_V2_WINDOW_MAX_FRAMES === 128` and a readable `ResponseBatcher` whose default `flushMs` is 25.
- Preserves: 512 KiB window, 16 KiB frame, 32-event batch, and 14 KiB target.

- [ ] **Step 1: Change tests to require 128/129 and a 25 ms default deadline**

```ts
expect(GP_V2_WINDOW_MAX_FRAMES).toBe(128);
const tooMany = { ...validEnvelope(), frames: Array.from({ length: 129 }, (_, index) => ping(index)) };
expect(() => parseAwarenessEnvelope(tooMany, BINDING, "host")).toThrow("128-frame");

const batcher = new ResponseBatcher({ emit: frame => frames.push(frame) });
batcher.push("r", 0, { event: "response.created", data: "{}" });
await Bun.sleep(10);
expect(frames).toHaveLength(0);
await Bun.sleep(25);
expect(frames).toHaveLength(1);
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-window.test.ts tests/v2-response-batcher.test.ts`

Expected: failures report the old 32-frame and 5 ms defaults.

- [ ] **Step 3: Set constants and refactor the batcher without changing its bounds**

```ts
export const GP_V2_WINDOW_MAX_FRAMES = 128;

constructor(options: ResponseBatcherOptions) {
  this.#emit = options.emit;
  this.#flushMs = options.flushMs ?? 25;
  this.#targetBytes = options.targetBytes ?? 14 * 1024;
  if (this.#flushMs < 1 || this.#targetBytes < 512 || this.#targetBytes >= 16 * 1024) {
    throw new RangeError("invalid response batch limits");
  }
}
```

Expand the minified batcher into named interfaces/helpers while retaining explicit flush, discontinuity flush, size flush, 32-item flush, cancellation, and `unref()` behavior.

- [ ] **Step 4: Run protocol/window/batcher/throughput tests and verify GREEN**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-window.test.ts tests/v2-response-batcher.test.ts tests/v2-batching-throughput.test.ts`

Expected: all tests pass, including more than 10,000 reordered operations.

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- src/v2/protocol/shared.ts src/v2/sidecar/response-batcher.ts tests/v2-protocol.test.ts tests/v2-window.test.ts tests/v2-response-batcher.test.ts tests/v2-batching-throughput.test.ts
git commit -m "feat: widen v2 paced transport windows"
```

### Task 4: Pace the Deno sidecar peer and make retransmission adaptive

**Files:**
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`

**Interfaces:**
- Consumes: `PublicationPacer`, `ReliableDeliveryTiming`, sidecar frame kinds, and existing scheduler/window classes.
- Produces: one shared sidecar data cadence, urgent ACK/control publication, and measured sidecar RTT.

- [ ] **Step 1: Write failing peer integration tests**

Add tests that enqueue one data frame every 5 ms across 16 stream IDs and assert publications remain cadence-bounded; enqueue two `request_ack` frames in one turn and assert one urgent publication before 25 ms; ACK a sequence after a known delay and assert non-null RTT diagnostics; verify retransmission backoff does not fire again at the old fixed floor.

```ts
for (let index = 0; index < 16; index++) {
  peer.enqueue(responseEvent(index), "data", `req-${index}`);
  await Bun.sleep(5);
}
expect(publications).toBeLessThanOrEqual(4);

peer.enqueue(requestAck("a"), "terminal", "a");
peer.enqueue(requestAck("b"), "terminal", "b");
await Bun.sleep(0);
expect(publications).toBe(previous + 1);
```

- [ ] **Step 2: Run the peer tests and verify RED**

Run: `bun test --isolate tests/v2-deno-awareness-peer.test.ts`

Expected: the existing microtask publisher emits approximately one publication per spaced data frame and RTT remains null.

- [ ] **Step 3: Integrate pacing, urgency classification, and delivery timing**

```ts
type PublicationUrgency = "paced" | "urgent";

function sidecarUrgency(frame: SidecarFrame): PublicationUrgency {
  return frame.kind === "response_event" || frame.kind === "response_batch"
    ? "paced"
    : "urgent";
}

#requestPublication(urgency: PublicationUrgency | null): void {
  if (urgency === "urgent") this.#pacer.requestUrgent();
  else if (urgency === "paced") this.#pacer.requestPaced();
}
```

Make `#pump()` return the strongest urgency actually pushed. Inbound host frames request an urgent cumulative ACK. ACK progress alone emits nothing; newly unlocked data uses the pump result. `#publish(reason)` calls `delivery.published()`, and the RTO callback calls `delivery.retransmitted()` followed by `pacer.flushNow("retransmission")`. Only ACK progress or retransmission moves the RTO base.

Add `publicationIntervalMs?: number` to `DenoAwarenessPeerOptions` for deterministic integration tests; production defaults to 25 ms and validates values below 1 ms.

- [ ] **Step 4: Run sidecar peer, sidecar engine, faults, and large-payload tests**

Run: `bun test --isolate tests/v2-deno-awareness-peer.test.ts tests/v2-sidecar-engine.test.ts tests/v2-faults.test.ts tests/v2-large-payload.test.ts`

Expected: all tests pass with no ordering, replay, or timer regressions.

- [ ] **Step 5: Commit Task 4**

```powershell
git add -- sandbox/v2/awareness-peer.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "feat: pace v2 sidecar awareness traffic"
```

### Task 5: Pace the host session without delaying terminal control

**Files:**
- Modify: `src/v2/transport/session.ts`
- Modify: `tests/v2-transport-session.test.ts`
- Modify: `tests/support/fake-v2-system.ts`

**Interfaces:**
- Consumes: the shared pacer/timing modules and host frame kinds.
- Produces: paced request data, urgent cumulative response ACK/control, measured host RTT, and progress-anchored host RTO.

- [ ] **Step 1: Add failing host publication/ACK/RTO tests**

Capture host envelopes in `TransportHarness`; start multiple requests in one turn and assert one paced publication contains their available frames. Publish a response terminal and assert the resulting `response_ack` is sent through the urgent path. Publish an ACK after a controlled delay and assert `snapshot().awarenessRttMs` reflects the first sequence send rather than the last arbitrary publication.

- [ ] **Step 2: Run host transport tests and verify RED**

Run: `bun test --isolate tests/v2-transport-session.test.ts`

Expected: publication count or first-send RTT assertions fail under the old direct publisher.

- [ ] **Step 3: Integrate host pacing and delivery timing**

```ts
function hostUrgency(frame: HostFrame): PublicationUrgency {
  switch (frame.kind) {
    case "request_open":
    case "request_data":
    case "request_commit":
      return "paced";
    default:
      return "urgent";
  }
}
```

Replace direct `#publish(false)` calls with the strongest pump urgency. Initial readiness uses `flushNow("initial")`; delivered sidecar frames always request an urgent cumulative ACK; terminal ACK, cancel, drain, rotation, and ceiling changes request urgent publication. Closing the session closes the pacer and retransmission timer. Keep request data order and the four-uncommitted cap unchanged.

Add `publicationIntervalMs?: number` to `V2TransportSessionOptions`; production defaults to 25 ms. `TransportHarness` uses 1 ms and waits for that timer before flushing the fake Awareness network so existing protocol tests remain deterministic.

- [ ] **Step 4: Run host transport, concurrency, and fake-system suites**

Run: `bun test --isolate tests/v2-transport-session.test.ts tests/v2-concurrency.test.ts tests/v2-faults.test.ts tests/system-v2.test.ts tests/multi-sandbox-integration.test.ts`

Expected: all tests pass with exact upstream call counts and terminal proofs.

- [ ] **Step 5: Commit Task 5**

```powershell
git add -- src/v2/transport/session.ts tests/v2-transport-session.test.ts tests/support/fake-v2-system.ts
git commit -m "feat: pace v2 host awareness traffic"
```

### Task 6: Publication-efficiency diagnostics

**Files:**
- Modify: `src/v2/diagnostics/types.ts`
- Modify: `src/v2/diagnostics/sidecar-observer.ts`
- Modify: `src/observability/sampler.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `tests/v2-diagnostics-types.test.ts`
- Modify: `tests/v2-sidecar-observer.test.ts`
- Modify: `tests/v2-diagnostics-observability.test.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`

**Interfaces:**
- Adds: `pacedPublicationsTotal`, `urgentPublicationsTotal`, `logicalEventsTotal`, `responseBatchFramesTotal`, and `batchedEventsTotal` to `V2TransportDiagnostics`.
- Logs: raw counters plus `logical_events_per_batch` and `logical_events_per_publication`.

- [ ] **Step 1: Extend safe diagnostic fixtures first and verify parser RED**

```ts
transport: {
  windowFrames: 1,
  windowBytes: 100,
  schedulerFrames: 2,
  schedulerBytes: 200,
  schedulerStreams: 1,
  oldestQueuedFrameMs: 4,
  ackProgressAgeMs: 5,
  retransmissionAttempt: 0,
  peakRetransmissionAttempt: 1,
  retransmissionsTotal: 2,
  publicationsTotal: 10,
  publishedBytesTotal: 400,
  admittedUncommitted: 1,
  roundTripEwmaMs: 6,
  pacedPublicationsTotal: 7,
  urgentPublicationsTotal: 3,
  logicalEventsTotal: 40,
  responseBatchFramesTotal: 8,
  batchedEventsTotal: 32,
}
```

Assert the fleet JSON maps these to snake_case and derives `4` events/batch and `4` events/publication when total publications are 10.

- [ ] **Step 2: Run diagnostics tests and verify RED**

Run: `bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-sidecar-observer.test.ts tests/v2-diagnostics-observability.test.ts`

Expected: exact parsing rejects the new fields until production types are updated.

- [ ] **Step 3: Implement bounded counters and ratios**

```ts
const perBatch = value.responseBatchFramesTotal === 0
  ? 0
  : value.batchedEventsTotal / value.responseBatchFramesTotal;
const perPublication = value.publicationsTotal === 0
  ? 0
  : value.logicalEventsTotal / value.publicationsTotal;
```

The sidecar increments logical events once for each batch item or `response_event` with `partIndex === 0`; segmented parts never double-count. Publication reason increments the paced/urgent counter. Host diagnostics report the same publication counters and zero response-event counters. Keep exact parsing, safe integer validation, and the 2 KiB diagnostic ceiling.

- [ ] **Step 4: Run all diagnostics and observability tests**

Run: `bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-sidecar-observer.test.ts tests/v2-diagnostics-observability.test.ts tests/v2-diagnostics-overhead.test.ts tests/observability-sampler.test.ts tests/observability-redaction.test.ts`

Expected: all tests pass and no forbidden content-shaped field is emitted.

- [ ] **Step 5: Commit Task 6**

```powershell
git add -- src/v2/diagnostics/types.ts src/v2/diagnostics/sidecar-observer.ts src/observability/sampler.ts sandbox/v2/awareness-peer.ts tests/v2-diagnostics-types.test.ts tests/v2-sidecar-observer.test.ts tests/v2-diagnostics-observability.test.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "feat: report v2 publication efficiency"
```

### Task 7: Full regression and release verification

**Files:**
- Modify only files required by failures attributable to Tasks 1–6.
- Verify: repository, sidecar bundle, daemon compatibility, documentation, and generated bootstrap assets.

**Interfaces:**
- Produces: a source-verified commit ready for a user-managed live restart.
- Does not produce: a live performance claim before the acceptance rerun.

- [ ] **Step 1: Run focused transport fault/regression suites**

Run: `bun test --isolate tests/v2-publication-pacer.test.ts tests/v2-delivery-timing.test.ts tests/v2-protocol.test.ts tests/v2-window.test.ts tests/v2-response-batcher.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-transport-session.test.ts tests/v2-faults.test.ts tests/v2-large-payload.test.ts tests/v2-sidecar-engine.test.ts tests/v2-sidecar-assets.test.ts`

Expected: zero failures.

- [ ] **Step 2: Run the complete isolated Bun suite**

Run: `bun test --isolate`

Expected: zero failures across every test file.

- [ ] **Step 3: Run daemon, type, dependency, and smoke verification**

Run: `bun run test:daemon`

Run: `bun run typecheck`

Run: `bun audit`

Run: `bun run verify:v2-smoke`

Expected: each command exits 0; smoke verifies source/bootstrap behavior without stopping the live server.

- [ ] **Step 4: Review repository state and constraints**

Run: `git diff --check`

Run: `git status --short --branch`

Run: `git log -8 --oneline --decorate`

Expected: no whitespace errors, only intended tracked changes before the final commit, `master` remains the active branch, and no worktree was created.

- [ ] **Step 5: Commit and push verified source**

```powershell
git add --all
git commit -m "feat: pace v2 awareness transport"
git push origin master
```

After push, report automated evidence and request exactly one user-managed restart/rerun. Do not claim that live latency improved until the new efficiency counters and 1/4/8/16-agent acceptance data prove it.
