# GhostProv V2 Split-Lane Flight Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Separate ACK/control state from reliable payload state and send one bounded payload flight per cumulative ACK so multi-agent traffic cannot amplify into repeated 512 KiB Awareness publications.

**Architecture:** The host composes two existing Y-Sweet Awareness channels and the Deno sidecar mirrors two providers, one for `control` and one for `data`. `AckClockedFlight` freezes each 128 KiB/32-frame payload until its high sequence is acknowledged; adaptive credits and a safe never-admitted timeout prevent one slow project from retaining work while healthy projects are idle.

**Tech Stack:** Bun 1.4, TypeScript 7, Deno, Yjs/y-websocket Awareness, `bun test --isolate`.

## Global Constraints

- Work directly on `master`; do not create a branch, worktree, or subagent.
- Never stop or restart the live router on port 18787 during implementation.
- Control envelopes contain zero reliable frames.
- Data flight target is 128 KiB and 32 frames; hard limits remain 512 KiB, 128 frames, and 16 KiB per frame.
- Initial/max transport-active capacity is 8/16 and never exceeds physical capacity.
- Only requests that were never admitted or published may time out and reroute.
- Preserve exact-once claims, terminal proof, replay, quarantine, client backpressure, and the public API.

---

### Task 1: Lane-aware protocol and composed host channel

**Files:**
- Modify: `src/v2/protocol/shared.ts`
- Modify: `src/v2/protocol/validate.ts`
- Modify: `src/v2/awareness/channel.ts`
- Modify: `src/v2/awareness/y-sweet-channel.ts`
- Create: `src/v2/awareness/split-channel.ts`
- Modify: `src/server/system.ts`
- Test: `tests/v2-protocol.test.ts`
- Test: `tests/v2-awareness-channel.test.ts`

**Interfaces:**
- Produces: `AwarenessLane = "data" | "control"`.
- Changes: `AwarenessChannel.publish(envelope, lane?)` and `clear(lane?)`.
- Produces: `SplitAwarenessChannel<TLocal, TRemote>` composed from data/control child channels.

- [ ] **Step 1: Write failing protocol and split-state tests**

```ts
expect(parseAwarenessEnvelope({ ...validEnvelope(), lane: "control" }, BINDING, "host").lane)
  .toBe("control");
expect(() => parseAwarenessEnvelope({ ...validEnvelope(), lane: "invalid" }, BINDING, "host"))
  .toThrow("lane");

split.publish(hostEnvelope(), "data");
split.publish(hostEnvelope(), "control");
expect(data.published.at(-1)?.lane).toBe("data");
expect(control.published.at(-1)?.lane).toBe("control");
split.clear("control");
expect(control.clears).toBe(1);
expect(data.clears).toBe(0);
```

Also assert merged subscriptions retain independent lane revisions and credential rotation reaches both children.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-awareness-channel.test.ts`

Expected: lane parsing and `SplitAwarenessChannel` are missing.

- [ ] **Step 3: Implement the protocol and composed channel**

```ts
export type AwarenessLane = "data" | "control";

export interface AwarenessChannel<TLocal, TRemote> {
  readonly projectId: ProjectId;
  readonly localPeerId: string;
  connect(signal?: AbortSignal): Promise<void>;
  publish(envelope: AwarenessEnvelope<TLocal>, lane?: AwarenessLane): void;
  clear(lane?: AwarenessLane): void;
  subscribe(listener: (update: AwarenessPeerUpdate<TRemote>) => void): () => void;
  peerIds(): readonly string[];
  rotateCredential?(envelope: YSweetEnvelope): void;
  close(): Promise<void>;
}
```

`SplitAwarenessChannel.publish()` clones the envelope with the selected lane and routes it to exactly one child. `subscribe()` decorates each child update with that child's lane. `connect()` closes both children if either connection fails. The server builds child channels using `ghostprovV2Data` and `ghostprovV2Control` fields.

- [ ] **Step 4: Run protocol/channel/type tests and verify GREEN**

Run: `bun test --isolate tests/v2-protocol.test.ts tests/v2-awareness-channel.test.ts`

Run: `bun run typecheck`

Expected: all pass.

- [ ] **Step 5: Commit Task 1**

```powershell
git add -- src/v2/protocol/shared.ts src/v2/protocol/validate.ts src/v2/awareness/channel.ts src/v2/awareness/y-sweet-channel.ts src/v2/awareness/split-channel.ts src/server/system.ts tests/v2-protocol.test.ts tests/v2-awareness-channel.test.ts
git commit -m "feat: split v2 awareness control and data lanes"
```

### Task 2: ACK-clocked flight state

**Files:**
- Create: `src/v2/protocol/flight.ts`
- Create: `tests/v2-flight.test.ts`
- Modify: `src/v2/protocol/shared.ts`

**Interfaces:**
- Produces: `GP_V2_FLIGHT_MAX_FRAMES = 32`, `GP_V2_FLIGHT_MAX_BYTES = 128 * 1024`.
- Produces: `AckClockedFlight.active`, `.highSequence`, `.published()`, `.acknowledged()`, and `.reset()`.

- [ ] **Step 1: Write failing immutable-flight tests**

```ts
const flight = new AckClockedFlight({ epoch: 1 });
flight.published([{ epoch: 1, sequence: 0 }, { epoch: 1, sequence: 1 }]);
expect(flight.active).toBe(true);
expect(flight.acknowledged({ epoch: 1, sequence: 0 })).toBe(false);
expect(() => flight.published([{ epoch: 1, sequence: 0 }, { epoch: 1, sequence: 2 }]))
  .toThrow("immutable");
expect(flight.acknowledged({ epoch: 1, sequence: 1 })).toBe(true);
expect(flight.active).toBe(false);
```

Also test stale/future epochs, reset, empty publication, and unchanged retransmission.

- [ ] **Step 2: Run and verify RED**

Run: `bun test --isolate tests/v2-flight.test.ts`

Expected: module missing.

- [ ] **Step 3: Implement bounded immutable flights**

```ts
export class AckClockedFlight {
  #epoch: number;
  #highSequence: number | null = null;
  get active(): boolean { return this.#highSequence !== null; }
  get highSequence(): number | null { return this.#highSequence; }
  published(frames: readonly FrameIdentity[]): void;
  acknowledged(cursor: AckCursor): boolean;
  reset(epoch: number): void;
}
```

The first non-empty publication records the last sequence. A retransmission must end at the same high sequence. A different high sequence while active throws. Only a same-epoch ACK covering the high sequence clears the flight.

- [ ] **Step 4: Run flight/window tests and verify GREEN**

Run: `bun test --isolate tests/v2-flight.test.ts tests/v2-window.test.ts`

Expected: all pass.

- [ ] **Step 5: Commit Task 2**

```powershell
git add -- src/v2/protocol/flight.ts src/v2/protocol/shared.ts tests/v2-flight.test.ts
git commit -m "feat: add ack-clocked v2 data flights"
```

### Task 3: Host split-lane flights

**Files:**
- Modify: `src/v2/transport/session.ts`
- Modify: `tests/v2-transport-session.test.ts`
- Modify: `tests/support/fake-awareness.ts`

**Interfaces:**
- Consumes: `SplitAwarenessChannel`, `AckClockedFlight`, flight constants.
- Produces: independent data/control revisions and data-flight publication.

- [ ] **Step 1: Write failing ACK-amplification and flight-boundary tests**

```ts
await harness.startLargeRequest();
const data = harness.hostPublications.filter(value => value.lane === "data");
expect(data.at(-1)?.frames.length).toBeGreaterThan(0);
for (let index = 0; index < 100; index++) harness.publishSidecarData(responseFrame(index));
await harness.settle();
const controls = harness.hostPublications.filter(value => value.lane === "control");
expect(controls.every(value => value.frames.length === 0)).toBe(true);
expect(encodedBytes(controls)).toBeLessThan(encodedBytes(data) / 4);
```

Add tests proving the first data flight is at most 32 frames/128 KiB, no second flight is published before its high ACK, partial ACK does not pump, and control revision 20 cannot suppress data revision 3.

- [ ] **Step 2: Run host tests and verify RED**

Run: `bun test --isolate tests/v2-transport-session.test.ts`

Expected: control publications still contain cumulative frames and the sender slides new data into an active flight.

- [ ] **Step 3: Integrate host lanes and flights**

Maintain `#dataRevision`, `#controlRevision`, `#sidecarRevisionByLane`, `#dataDirty`, and one `AckClockedFlight`. `#pump()` returns immediately while a flight is active and otherwise fills only to the flight targets. A data publication marks the immutable high sequence. An urgent callback publishes a small control envelope and, only when dirty, a separate data envelope. RTO publishes only data. Control ACK completion clears the flight and enables the next pump.

```ts
const frames = lane === "data" ? outbound.frames : [];
this.#channel.publish({ ...base, lane, revision, frames }, lane);
```

- [ ] **Step 4: Run host fault/concurrency tests and verify GREEN**

Run: `bun test --isolate tests/v2-transport-session.test.ts tests/v2-faults.test.ts tests/v2-concurrency.test.ts`

Expected: all pass and the amplification assertion shows at least 75% fewer bytes.

- [ ] **Step 5: Commit Task 3**

```powershell
git add -- src/v2/transport/session.ts tests/v2-transport-session.test.ts tests/support/fake-awareness.ts
git commit -m "feat: send host traffic in ack-clocked flights"
```

### Task 4: Deno sidecar split-lane flights

**Files:**
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`
- Modify: `sandbox/v2/main.ts`

**Interfaces:**
- Consumes: lane protocol and `AckClockedFlight`.
- Produces: two sidecar providers/fields with independent revisions and one bounded response flight.

- [ ] **Step 1: Write failing sidecar split-provider tests**

Create fake providers keyed by lane. Assert initial readiness exists only in control state, response frames exist only in data state, 100 host data updates produce control envelopes with zero frames, and the next response flight waits for the prior high ACK.

- [ ] **Step 2: Run sidecar tests and verify RED**

Run: `bun test --isolate tests/v2-deno-awareness-peer.test.ts tests/v2-sidecar-assets.test.ts`

Expected: only one provider/state exists and ACKs repeat response frames.

- [ ] **Step 3: Implement two Deno providers and flight gating**

Create data/control Y.Docs and providers with fields `ghostprovV2Data` and `ghostprovV2Control`. Connect and close both. Each handler processes only its lane; control is authoritative for credits/diagnostics and data for reliable frames. Publication mirrors the host rules. `#pump()` freezes while the flight is active and applies 32-frame/128 KiB targets.

- [ ] **Step 4: Run sidecar/replay/large-payload suites and verify GREEN**

Run: `bun test --isolate tests/v2-deno-awareness-peer.test.ts tests/v2-sidecar-engine.test.ts tests/v2-sidecar-assets.test.ts tests/v2-faults.test.ts tests/v2-large-payload.test.ts`

Expected: all pass with offline bundle generation intact.

- [ ] **Step 5: Commit Task 4**

```powershell
git add -- sandbox/v2/awareness-peer.ts sandbox/v2/main.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "feat: send sidecar traffic in split flights"
```

### Task 5: Adaptive capacity and safe unadmitted reroute

**Files:**
- Create: `src/v2/transport/adaptive-capacity.ts`
- Create: `tests/v2-adaptive-capacity.test.ts`
- Modify: `src/config/multi-sandbox.ts`
- Modify: `src/v2/bootstrap/controller.ts`
- Modify: `sandbox/v2/main.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `src/v2/transport/session.ts`
- Modify: `tests/multi-sandbox-config.test.ts`
- Modify: `tests/v2-bootstrap-controller.test.ts`
- Modify: `tests/v2-transport-session.test.ts`
- Modify: `tests/router-policy.test.ts`

**Interfaces:**
- Produces: `AdaptiveTransportCapacity.observe()` and `.available()`.
- Adds config: initial/max active per sandbox, defaults 8/16.
- Adds host unadmitted timeout, default 1,000 ms.

- [ ] **Step 1: Write failing capacity hysteresis and safe-timeout tests**

```ts
const capacity = new AdaptiveTransportCapacity({ initial: 8, maximum: 16 });
expect(capacity.observe({ rttMs: 5_000, congested: false, ackStalled: false })).toBe(true);
expect(capacity.limit).toBe(4);
for (let index = 0; index < 8; index++) {
  capacity.observe({ rttMs: 500, congested: false, ackStalled: false });
}
expect(capacity.limit).toBe(5);
expect(capacity.available(3, 100)).toBe(2);
```

Host tests set zero credits, wait a 10 ms injected timeout, and assert an unadmitted request rejects pre-commit with no published frame. A separately admitted request must never be timed out.

- [ ] **Step 2: Run capacity/config/session tests and verify RED**

Run: `bun test --isolate tests/v2-adaptive-capacity.test.ts tests/multi-sandbox-config.test.ts tests/v2-transport-session.test.ts`

Expected: controller/config/timeout missing.

- [ ] **Step 3: Implement adaptive credits, config propagation, and safe timeout**

The sidecar observes completed data-flight RTT and congestion, then advertises:

```ts
const credits = Math.min(
  engine.credits,
  adaptive.available(engine.active, engine.credits),
);
```

The host starts a timer only for a state with `admitted === false`; admission and removal clear it. Expiry rejects `V2PreCommitError("Sandbox capacity changed before admission", "capacity_changed")` so the existing router retry excludes that project.

- [ ] **Step 4: Run routing, capacity, and 512-request tests**

Run: `bun test --isolate tests/v2-adaptive-capacity.test.ts tests/multi-sandbox-config.test.ts tests/v2-bootstrap-controller.test.ts tests/v2-transport-session.test.ts tests/router-policy.test.ts tests/v2-concurrency.test.ts`

Expected: all pass, limits remain within physical bounds, and no admitted request reroutes.

- [ ] **Step 5: Commit Task 5**

```powershell
git add -- src/v2/transport/adaptive-capacity.ts tests/v2-adaptive-capacity.test.ts src/config/multi-sandbox.ts src/v2/bootstrap/controller.ts sandbox/v2/main.ts sandbox/v2/awareness-peer.ts src/v2/transport/session.ts tests/multi-sandbox-config.test.ts tests/v2-bootstrap-controller.test.ts tests/v2-transport-session.test.ts tests/router-policy.test.ts
git commit -m "feat: adapt v2 capacity to transport health"
```

### Task 6: Split-flight observability, documentation, and local fleet scale

**Files:**
- Modify: `src/v2/diagnostics/types.ts`
- Modify: `src/v2/diagnostics/sidecar-observer.ts`
- Modify: `src/observability/sampler.ts`
- Modify: `src/v2/transport/session.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `tests/v2-diagnostics-types.test.ts`
- Modify: `tests/observability-sampler.test.ts`
- Modify: `tests/documentation.test.ts`
- Modify: `README.md`
- Modify ignored local file: `.env`

**Interfaces:**
- Adds diagnostics: data/control publication and byte totals, flight completions, adaptive capacity, unadmitted timeouts.
- Logs `logical_events_per_data_publication`.

- [ ] **Step 1: Write failing exact-parser and JSON mapping tests**

Extend `safeDiagnostics()` with integer counters and assert exact snake-case mappings and derived ratios. Assert the encoded diagnostic object remains at or below 2,048 bytes.

- [ ] **Step 2: Run diagnostics/documentation tests and verify RED**

Run: `bun test --isolate tests/v2-diagnostics-types.test.ts tests/observability-sampler.test.ts tests/documentation.test.ts`

Expected: exact parser rejects new fields and README lacks the split-flight contract.

- [ ] **Step 3: Implement counters, docs, and ignored eight-project local settings**

Update `.env` without printing secrets:

```dotenv
GP_AUTO_CREATE_PROJECTS=1
GP_TARGET_SANDBOXES=8
GP_MAX_PROJECTS=8
GP_V2_INITIAL_ACTIVE_PER_SANDBOX=8
GP_V2_MAX_ACTIVE_PER_SANDBOX=16
```

Keep existing four explicit project IDs; the fleet manager creates only the exact managed deficit on the next start.

- [ ] **Step 4: Run diagnostics, docs, and configuration tests**

Run: `bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-diagnostics-overhead.test.ts tests/observability-sampler.test.ts tests/observability-redaction.test.ts tests/documentation.test.ts tests/multi-sandbox-config.test.ts`

Expected: all pass, no unsafe fields, and diagnostics stay bounded.

- [ ] **Step 5: Commit Task 6 (excluding ignored `.env`)**

```powershell
git add -- src/v2/diagnostics/types.ts src/v2/diagnostics/sidecar-observer.ts src/observability/sampler.ts src/v2/transport/session.ts sandbox/v2/awareness-peer.ts tests/v2-diagnostics-types.test.ts tests/observability-sampler.test.ts tests/documentation.test.ts README.md
git commit -m "feat: observe v2 split flight efficiency"
```

### Task 7: Full verification, push, and shutdown

**Files:**
- Verify all tracked source and ignored local fleet settings.

**Interfaces:**
- Produces: pushed `master`, clean tracked worktree, untouched live port 18787, and a scheduled OS shutdown only after success.

- [ ] **Step 1: Run focused split-flight/fault/API suites**

Run: `bun test --isolate tests/v2-awareness-channel.test.ts tests/v2-flight.test.ts tests/v2-transport-session.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-faults.test.ts tests/v2-large-payload.test.ts tests/v2-public-api.test.ts tests/websocket.test.ts tests/images-http.test.ts`

Expected: zero failures.

- [ ] **Step 2: Run the complete isolated suite**

Run: `bun test --isolate`

Expected: zero failures.

- [ ] **Step 3: Run remaining verification**

Run: `bun run typecheck`

Run: `bun run test:daemon`

Run: `bun audit`

Run: `bun run verify:v2-smoke`

Expected: every command exits 0; smoke binds only an ephemeral candidate port.

- [ ] **Step 4: Verify repository and live listener, then push**

Run: `git diff --check`

Run: `git status --short --branch`

Confirm port 18787 still owns the same running process, then run `git push origin master` and verify `HEAD == origin/master`.

- [ ] **Step 5: Schedule recoverable final shutdown**

Only after every prior step passes, schedule Windows shutdown with a two-minute delay so the completion response can be delivered:

```powershell
shutdown.exe /s /t 120 /c "GhostProv split-flight work completed and verified"
```

Do not schedule shutdown if any required verification or push remains incomplete.
