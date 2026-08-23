# GhostProv V2 Concurrency Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in, privacy-safe boundary diagnostics that determine whether 16-agent latency originates in the Windows host, Y-Sweet/Awareness transport, Linux/Deno sandbox resources, upstream OpenAI execution, or client WebSocket backpressure.

**Architecture:** Diagnostics are disabled by default and use fixed-shape, bounded snapshots. Sidecar resource and transport metrics piggyback on existing Awareness envelopes, while local request-stage durations ride existing ACK/first-event/terminal frames. Host observability correlates these local durations without subtracting cross-machine clocks, and a guarded probe runs identical 1/4/8/16-client waves to classify the bottleneck.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Deno sidecar bundle, Yjs/Y-Sweet Awareness, Hono/ws, Bun test with `--isolate`.

## Global Constraints

- Execute inline without subagents and work directly on `master`, per the user's standing instruction.
- Do not restart, stop, replace, or bind over the user's running `127.0.0.1:18787` process; the user performs the one manual restart after implementation.
- Do not change router policy, admission, physical capacity, reliable-window limits, batching, retransmission behavior, or fairness in this plan.
- Diagnostics default off and activate only with `GP_V2_DIAGNOSTICS=1`.
- Never log prompts, responses, SSE data, tool arguments, request bodies, headers, cookies, credentials, URLs, client IDs, or stream IDs.
- Never subtract timestamps from different machines. Transfer sidecar-local durations and compare them only with host-local durations.
- Diagnostic data must add at most 2 KiB to an Awareness envelope, use at most 1 MiB fixed sidecar memory, consume less than 1% sidecar CPU, and cause less than 2% deterministic throughput and p95-latency regression.
- Diagnostics may not create an Awareness publication, protocol frame, or terminal-proof dependency by themselves.
- Missing, unsupported, or malformed diagnostic fields degrade to `null`/a bounded error counter and never reject a core envelope or request.
- Preserve the user's existing project/environment files and never create or delete Prism projects.

---

### Task 1: Diagnostics Configuration, Safe Types, and Bounded Statistics

**Files:**
- Create: `src/v2/diagnostics/types.ts`
- Create: `src/v2/diagnostics/statistics.ts`
- Modify: `src/config/multi-sandbox.ts`
- Modify: `.env.example`
- Test: `tests/v2-diagnostics-types.test.ts`
- Test: `tests/v2-diagnostics-statistics.test.ts`
- Test: `tests/multi-sandbox-config.test.ts`

**Interfaces:**
- Produces: `V2DiagnosticsConfig`, `V2SidecarResourceSnapshot`, `V2TransportDiagnostics`, `V2RequestAckDiagnostics`, `V2FirstEventDiagnostics`, and `V2TerminalDiagnostics`.
- Produces: `parseDiagnosticObject(value): V2EnvelopeDiagnostics | null` and `encodedDiagnosticBytes(value): number`.
- Produces: `BoundedDistribution` with `record`, `snapshotAndReset`, and constant-memory storage.
- Extends: `MultiSandboxConfig` with `v2DiagnosticsEnabled: boolean` and `v2DiagnosticsIntervalMs: number`.

- [ ] **Step 1: Write failing configuration and privacy tests**

```ts
expect(resolveMultiSandboxConfig(baseEnv, MACHINE)).toMatchObject({
  v2DiagnosticsEnabled: false,
  v2DiagnosticsIntervalMs: 1_000,
});
expect(resolveMultiSandboxConfig({
  ...baseEnv,
  GP_V2_DIAGNOSTICS: "1",
  GP_V2_DIAGNOSTICS_INTERVAL_MS: "500",
}, MACHINE)).toMatchObject({
  v2DiagnosticsEnabled: true,
  v2DiagnosticsIntervalMs: 500,
});
expect(() => resolveMultiSandboxConfig({
  ...baseEnv,
  GP_V2_DIAGNOSTICS: "yes",
}, MACHINE)).toThrow("GP_V2_DIAGNOSTICS");

const parsed = parseDiagnosticObject({
  version: 1,
  resource: {
    processRssBytes: 100,
    heapUsedBytes: 50,
    heapTotalBytes: 80,
    externalBytes: 10,
    systemTotalBytes: null,
    systemFreeBytes: null,
    processCpuPercent: null,
    normalizedLoad1: null,
    eventLoopP50Ms: 1,
    eventLoopP95Ms: 2,
    eventLoopMaxMs: 3,
  },
  transport: safeTransportDiagnostics(),
});
expect(parsed?.version).toBe(1);
expect(encodedDiagnosticBytes(parsed!)).toBeLessThanOrEqual(2_048);
expect(parseDiagnosticObject({ version: 1, prompt: "forbidden" })).toBeNull();
expect(parseDiagnosticObject({ version: 1, resource: { processRssBytes: NaN } })).toBeNull();
```

- [ ] **Step 2: Run the tests and confirm RED**

Run:

```powershell
bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-diagnostics-statistics.test.ts tests/multi-sandbox-config.test.ts
```

Expected: FAIL because `src/v2/diagnostics/*` and configuration fields do not exist.

- [ ] **Step 3: Implement exact bounded diagnostic types**

```ts
export interface V2SidecarResourceSnapshot {
  processRssBytes: number;
  heapUsedBytes: number;
  heapTotalBytes: number;
  externalBytes: number;
  systemTotalBytes: number | null;
  systemFreeBytes: number | null;
  processCpuPercent: number | null;
  normalizedLoad1: number | null;
  eventLoopP50Ms: number;
  eventLoopP95Ms: number;
  eventLoopMaxMs: number;
}

export interface V2TransportDiagnostics {
  windowFrames: number;
  windowBytes: number;
  schedulerFrames: number;
  schedulerBytes: number;
  schedulerStreams: number;
  oldestQueuedFrameMs: number;
  ackProgressAgeMs: number;
  retransmissionAttempt: number;
  peakRetransmissionAttempt: number;
  retransmissionsTotal: number;
  publicationsTotal: number;
  publishedBytesTotal: number;
  admittedUncommitted: number;
  roundTripEwmaMs: number | null;
}

export interface V2EnvelopeDiagnostics {
  version: 1;
  resource: V2SidecarResourceSnapshot | null;
  transport: V2TransportDiagnostics;
  malformedCount: number;
}
```

Implement strict key allowlists, finite-number checks, non-negative clamps, integer checks for counters, and a final `encodedDiagnosticBytes <= 2_048` guard. Forbidden-key matching must reuse the observability privacy vocabulary without importing Node-only modules into the Deno bundle.

- [ ] **Step 4: Implement constant-memory distributions**

```ts
export interface DistributionSnapshot {
  count: number;
  p50: number;
  p95: number;
  max: number;
}

export class BoundedDistribution {
  readonly #values: Float64Array;
  #count = 0;
  #cursor = 0;

  constructor(capacity = 256) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 4_096) {
      throw new RangeError("distribution capacity must be 1 through 4096");
    }
    this.#values = new Float64Array(capacity);
  }

  record(value: number): void {
    if (!Number.isFinite(value) || value < 0) return;
    this.#values[this.#cursor] = value;
    this.#cursor = (this.#cursor + 1) % this.#values.length;
    this.#count = Math.min(this.#count + 1, this.#values.length);
  }

  snapshotAndReset(): DistributionSnapshot {
    const sorted = Array.from(this.#values.subarray(0, this.#count)).sort((a, b) => a - b);
    const percentile = (p: number) => sorted.length
      ? sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)]!
      : 0;
    const snapshot = { count: sorted.length, p50: percentile(0.5), p95: percentile(0.95), max: percentile(1) };
    this.#count = 0;
    this.#cursor = 0;
    return snapshot;
  }
}
```

- [ ] **Step 5: Add strict config parsing and documented defaults**

Add:

```ts
v2DiagnosticsEnabled: binaryFlag(env.GP_V2_DIAGNOSTICS, "GP_V2_DIAGNOSTICS"),
v2DiagnosticsIntervalMs: boundedPositiveInteger(
  env.GP_V2_DIAGNOSTICS_INTERVAL_MS,
  1_000,
  "GP_V2_DIAGNOSTICS_INTERVAL_MS",
  100,
  60_000,
),
```

Document only non-secret defaults in `.env.example`:

```dotenv
GP_V2_DIAGNOSTICS=0
GP_V2_DIAGNOSTICS_INTERVAL_MS=1000
```

- [ ] **Step 6: Verify Task 1 and commit**

Run:

```powershell
bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-diagnostics-statistics.test.ts tests/multi-sandbox-config.test.ts
bun run typecheck
```

Expected: all tests pass and TypeScript exits 0.

```powershell
git add src/v2/diagnostics src/config/multi-sandbox.ts .env.example tests/v2-diagnostics-types.test.ts tests/v2-diagnostics-statistics.test.ts tests/multi-sandbox-config.test.ts
git commit -m "feat: define bounded v2 diagnostics"
```

---

### Task 2: Fixed-Memory Linux/Deno Sidecar Resource Sampler

**Files:**
- Create: `src/v2/diagnostics/sidecar-sampler.ts`
- Modify: `src/v2/bootstrap/controller.ts`
- Modify: `src/server/system.ts`
- Modify: `sandbox/v2/main.ts`
- Test: `tests/v2-sidecar-diagnostics.test.ts`
- Test: `tests/v2-bootstrap-controller.test.ts`
- Test: `tests/system-v2.test.ts`

**Interfaces:**
- Consumes: `V2SidecarResourceSnapshot`, `BoundedDistribution`, and Task 1 configuration.
- Produces: `SidecarResourceSampler` with `start()`, `snapshot()`, and `close()`.
- Produces: `SidecarRuntimeProbe` so deterministic tests do not depend on the local operating system.
- Extends: sidecar static JSON with `diagnostics: { enabled: boolean; intervalMs: number }`.

- [ ] **Step 1: Write failing sampler lifecycle/resource tests**

```ts
const sampler = new SidecarResourceSampler({
  enabled: true,
  intervalMs: 1_000,
  now: () => clock.now,
  probe: {
    memory: () => ({ rss: 500, heapUsed: 200, heapTotal: 300, external: 25 }),
    systemMemory: () => ({ total: 4_000, free: 1_500 }),
    normalizedLoad1: () => 0.25,
    processCpuPercent: () => null,
  },
});
sampler.recordEventLoopDelay(2);
sampler.recordEventLoopDelay(9);
expect(sampler.snapshot()).toMatchObject({
  processRssBytes: 500,
  systemFreeBytes: 1_500,
  normalizedLoad1: 0.25,
  processCpuPercent: null,
  eventLoopP50Ms: 2,
  eventLoopMaxMs: 9,
});
await sampler.close();
expect(sampler.snapshot().eventLoopMaxMs).toBe(0);
```

Add a bootstrap assertion that `static.json` contains exactly:

```ts
diagnostics: { enabled: true, intervalMs: 1_000 }
```

- [ ] **Step 2: Confirm RED**

Run:

```powershell
bun test --isolate tests/v2-sidecar-diagnostics.test.ts tests/v2-bootstrap-controller.test.ts tests/system-v2.test.ts
```

Expected: FAIL because the sidecar sampler and static diagnostic config are absent.

- [ ] **Step 3: Implement the sampler with optional Deno probes**

```ts
export interface SidecarRuntimeProbe {
  memory(): { rss: number; heapUsed: number; heapTotal: number; external: number };
  systemMemory(): { total: number; free: number } | null;
  processCpuPercent(): number | null;
  normalizedLoad1(): number | null;
}

export class SidecarResourceSampler {
  start(): void;
  recordEventLoopDelay(delayMs: number): void;
  snapshot(): V2SidecarResourceSnapshot;
  close(): Promise<void>;
}
```

The production probe uses `Deno.memoryUsage()`, guarded `Deno.systemMemoryInfo()`, and guarded `Deno.loadavg()` normalized by `navigator.hardwareConcurrency`. Unsupported calls return `null`. Use a recursive one-shot timer and measure drift against the expected deadline; skip overlap rather than queueing samples.

- [ ] **Step 4: Wire opt-in config through bootstrap without behavioral changes**

Extend `V2BootstrapControllerOptions` with:

```ts
diagnostics?: { enabled: boolean; intervalMs: number };
```

Write the exact object into `.ghostprov-v2/static.json`. In `src/server/system.ts`, pass values from `MultiSandboxConfig`. In `sandbox/v2/main.ts`, validate both fields, create/start the sampler only when enabled, inject it into sidecar composition, and close it during shutdown.

- [ ] **Step 5: Prove diagnostics-off output is unchanged**

Serialize the static config with diagnostics disabled and compare every existing non-diagnostic field with the pre-change expected object. Assert no timer is created and no `diagnostics` property appears in an Awareness envelope while disabled.

- [ ] **Step 6: Verify Task 2 and commit**

```powershell
bun test --isolate tests/v2-sidecar-diagnostics.test.ts tests/v2-bootstrap-controller.test.ts tests/system-v2.test.ts tests/v2-sidecar-assets.test.ts
bun run typecheck
git add src/v2/diagnostics/sidecar-sampler.ts src/v2/bootstrap/controller.ts src/server/system.ts sandbox/v2/main.ts tests/v2-sidecar-diagnostics.test.ts tests/v2-bootstrap-controller.test.ts tests/system-v2.test.ts
git commit -m "feat: sample v2 sandbox resources"
```

---

### Task 3: Optional, Failure-Isolated Protocol Diagnostics

**Files:**
- Modify: `src/v2/protocol/shared.ts`
- Modify: `src/v2/protocol/validate.ts`
- Modify: `tests/v2-protocol.test.ts`
- Modify: `tests/v2-window.test.ts`
- Test: `tests/v2-diagnostics-protocol.test.ts`

**Interfaces:**
- Consumes: Task 1 diagnostic types.
- Extends: `AwarenessEnvelope<T>` with `diagnostics?: V2EnvelopeDiagnostics`.
- Extends: `request_ack`, first `response_event`, and `response_terminal` with optional bounded diagnostic objects.
- Produces: `parseAwarenessEnvelopeWithDiagnostics(value, binding, peer): { envelope: AwarenessEnvelope<T>; malformed: boolean }`; existing `parseAwarenessEnvelope` delegates to it and returns only `.envelope`.

- [ ] **Step 1: Write failing optional-compatibility tests**

```ts
const core = validEnvelope();
expect(parseAwarenessEnvelope(core, BINDING, "host")).toEqual(core);

const withDiagnostics = {
  ...core,
  diagnostics: safeEnvelopeDiagnostics(),
};
expect(parseAwarenessEnvelope(withDiagnostics, BINDING, "host").diagnostics)
  .toEqual(safeEnvelopeDiagnostics());

const malformed = {
  ...core,
  diagnostics: { version: 1, resource: { prompt: "forbidden" } },
};
const parsed = parseAwarenessEnvelopeWithDiagnostics(malformed, BINDING, "host");
expect(parsed.envelope.diagnostics).toBeUndefined();
expect(parsed.malformed).toBe(true);
```

Assert diagnostics-off serialization exactly equals `JSON.stringify(core)` and terminal proof remains identical with and without diagnostic objects.

- [ ] **Step 2: Confirm RED**

```powershell
bun test --isolate tests/v2-diagnostics-protocol.test.ts tests/v2-protocol.test.ts tests/v2-window.test.ts
```

Expected: FAIL because optional diagnostic fields are rejected as unknown.

- [ ] **Step 3: Extend types and schema descriptor without changing core limits**

Add optional fields while preserving:

```ts
export const GP_V2_FRAME_MAX_BYTES = 16 * 1024;
export const GP_V2_WINDOW_MAX_FRAMES = 32;
export const GP_V2_WINDOW_MAX_BYTES = 512 * 1024;
```

Use these shapes:

```ts
type RequestAckFrame = {
  kind: "request_ack";
  requestId: string;
  requestDigest: string;
  committed: true;
  diagnostics?: V2RequestAckDiagnostics;
};

type ResponseEventFrame = {
  kind: "response_event";
  requestId: string;
  eventIndex: number;
  event: string;
  partIndex: number;
  partCount: number;
  offset: number;
  dataBase64Url: string;
  diagnostics?: V2FirstEventDiagnostics;
};
```

Permit first-event diagnostics only for `eventIndex === 0 && partIndex === 0`. `parseAwarenessEnvelopeWithDiagnostics` strips invalid optional diagnostics and returns `malformed:true`; invalid core fields still throw `V2ProtocolError`. Host and sidecar peers use the detailed parser to increment their bounded malformed counters, while compatibility callers keep using `parseAwarenessEnvelope`.

- [ ] **Step 4: Prove size, privacy, and terminal isolation**

Assert the largest valid diagnostic frame stays below 16 KiB, forbidden diagnostic keys return `malformed: true`, and `TranscriptProof` output is unchanged because transport diagnostics never enter canonical SSE bytes.

- [ ] **Step 5: Verify Task 3 and commit**

```powershell
bun test --isolate tests/v2-diagnostics-protocol.test.ts tests/v2-protocol.test.ts tests/v2-window.test.ts tests/v2-transcript.test.ts
bun run typecheck
git add src/v2/protocol/shared.ts src/v2/protocol/validate.ts tests/v2-diagnostics-protocol.test.ts tests/v2-protocol.test.ts tests/v2-window.test.ts
git commit -m "feat: carry optional v2 diagnostics"
```

---

### Task 4: Sidecar Transport Counters and Request-Stage Timings

**Files:**
- Create: `src/v2/diagnostics/sidecar-observer.ts`
- Modify: `src/v2/protocol/scheduler.ts`
- Modify: `src/v2/sidecar/engine.ts`
- Modify: `src/v2/sidecar/upstream.ts`
- Modify: `sandbox/v2/awareness-peer.ts`
- Modify: `sandbox/v2/main.ts`
- Test: `tests/v2-sidecar-observer.test.ts`
- Modify: `tests/v2-scheduler.test.ts`
- Modify: `tests/v2-sidecar-engine.test.ts`
- Modify: `tests/v2-upstream.test.ts`
- Modify: `tests/v2-deno-awareness-peer.test.ts`

**Interfaces:**
- Consumes: Tasks 1–3 diagnostic objects and Task 2 sampler.
- Produces: `SidecarDiagnosticsObserver` with request lifecycle, publication, ACK, queue, and terminal methods.
- Extends: `WeightedFrameScheduler.snapshot(nowMs?)` with encoded bytes and oldest queued age while preserving selection order.
- Produces: sidecar-local request ACK, first-event, and terminal diagnostic objects.

- [ ] **Step 1: Write failing scheduler/counter tests**

```ts
const scheduler = new WeightedFrameScheduler<SidecarFrame>({ now: () => clock.now });
scheduler.enqueue("data", "req-1", responseFrame("req-1", 0));
clock.now = 25;
expect(scheduler.snapshot(clock.now)).toMatchObject({
  queuedFrames: 1,
  streams: 1,
  queuedEncodedBytes: expect.any(Number),
  oldestQueuedFrameMs: 25,
});
const taken = scheduler.take(1, 16 * 1024);
expect(taken).toHaveLength(1);
expect(scheduler.snapshot(clock.now).queuedFrames).toBe(0);
```

Write an observer test that records request-open, commit receipt, assembly, durable claim, admission, fetch, first event, publication, ACK progress, and terminal using one manual clock. Assert every reported duration is derived only from that clock.

- [ ] **Step 2: Confirm RED**

```powershell
bun test --isolate tests/v2-sidecar-observer.test.ts tests/v2-scheduler.test.ts tests/v2-sidecar-engine.test.ts tests/v2-upstream.test.ts tests/v2-deno-awareness-peer.test.ts
```

Expected: FAIL because queue age/bytes and lifecycle observations are absent.

- [ ] **Step 3: Implement fixed-memory observer state**

```ts
export interface SidecarDiagnosticsObserver {
  requestOpened(requestId: string): void;
  commitReceived(requestId: string): void;
  assemblyCompleted(requestId: string): void;
  claimCompleted(requestId: string, durationMs: number): void;
  admissionCompleted(requestId: string, durationMs: number): void;
  fetchStarted(requestId: string): void;
  upstreamEvent(requestId: string, eventIndex: number): V2FirstEventDiagnostics | undefined;
  framePublished(requestId: string | null, encodedBytes: number): void;
  ackAdvanced(sequence: number): void;
  requestAck(requestId: string): V2RequestAckDiagnostics | undefined;
  requestTerminal(requestId: string): V2TerminalDiagnostics | undefined;
  transportSnapshot(): V2TransportDiagnostics;
  removeRequest(requestId: string): void;
}
```

Bound request state to the sidecar accepted maximum. Evict only completed/cancelled diagnostic state; if the bound is reached, increment `malformedCount`/drop diagnostics rather than affecting the request.

- [ ] **Step 4: Instrument sidecar stages without adding awaits/publications**

Record timestamps around existing operations in `SidecarEngine.#commit` and `#execute`. Add an optional observer callback to `DirectResponsesExecution`:

```ts
onFetchStarted?(): void;
onFirstEvent?(): void;
```

Call them adjacent to the existing fetch and first parsed SSE event. Attach diagnostics to existing ACK/event/terminal frames only when enabled. Do not include SSE event names or data in diagnostic state.

- [ ] **Step 5: Instrument Awareness queues/publications**

In `DenoAwarenessPeer`, update the observer when frames enter/leave the scheduler, the reliable window changes, ACK advances, or an existing envelope is published/retransmitted. Compute publication bytes from the final serialized diagnostic-enabled envelope. Piggyback `diagnostics` only inside `#publish`; never call `#schedulePublish` because a sample changed.

Add this invariant test:

```ts
for (let index = 0; index < 64; index++) peer.enqueue(responseFrame(`req-${index % 16}`, index), "data", `req-${index % 16}`);
await Bun.sleep(0);
expect(publications).toBe(1);
expect(diagnosticOnlyPublications).toBe(0);
expect(encodedDiagnosticsBytes).toBeLessThanOrEqual(2_048);
```

- [ ] **Step 6: Verify Task 4 and commit**

```powershell
bun test --isolate tests/v2-sidecar-observer.test.ts tests/v2-scheduler.test.ts tests/v2-sidecar-engine.test.ts tests/v2-upstream.test.ts tests/v2-deno-awareness-peer.test.ts tests/v2-faults.test.ts tests/v2-sidecar-assets.test.ts
bun run typecheck
git add src/v2/diagnostics/sidecar-observer.ts src/v2/protocol/scheduler.ts src/v2/sidecar/engine.ts src/v2/sidecar/upstream.ts sandbox/v2/awareness-peer.ts sandbox/v2/main.ts tests/v2-sidecar-observer.test.ts tests/v2-scheduler.test.ts tests/v2-sidecar-engine.test.ts tests/v2-upstream.test.ts tests/v2-deno-awareness-peer.test.ts
git commit -m "feat: trace v2 sidecar boundaries"
```

---

### Task 5: Host Transport Diagnostics and Run-Handle Correlation

**Files:**
- Create: `src/v2/diagnostics/host-observer.ts`
- Modify: `src/relay/run-handle.ts`
- Modify: `src/v2/transport/run-handle.ts`
- Modify: `src/v2/transport/session.ts`
- Modify: `src/v2/runtime.ts`
- Modify: `src/sandbox/registry.ts`
- Test: `tests/v2-host-observer.test.ts`
- Modify: `tests/v2-transport-session.test.ts`
- Modify: `tests/v2-run-handle.test.ts`
- Modify: `tests/v2-runtime.test.ts`

**Interfaces:**
- Consumes: sidecar diagnostics from Tasks 3–4.
- Produces: `RunDiagnostics` attached to `RunHandle.stats.diagnostics`.
- Expands: `V2TransportSnapshot` with bounded host transport diagnostics and latest sidecar resource/transport snapshots.
- Produces: `HostRequestDiagnosticsObserver` that records only host-local durations.

- [ ] **Step 1: Write failing host-correlation tests**

```ts
const pending = session.run(BODY);
clock.advanceBy(10);
harness.flushHostPublication();
clock.advanceBy(20);
harness.publishRequestAck({
  firstFrameToCommitMs: 4,
  commitToAssemblyMs: 1,
  claimSyncMs: 2,
  admissionWaitMs: 0,
  commitProcessingMs: 3,
});
const handle = await pending;
expect(handle.stats.diagnostics).toMatchObject({
  hostPrepareMs: 10,
  hostPublishToAckMs: 20,
  sidecarCommitProcessingMs: 3,
  transportCommitResidualMs: 17,
});
```

Add a two-clock test with host clock at `1_000_000` and sidecar durations near `5`; assert no result contains a subtraction involving the sidecar's absolute clock.

- [ ] **Step 2: Confirm RED**

```powershell
bun test --isolate tests/v2-host-observer.test.ts tests/v2-transport-session.test.ts tests/v2-run-handle.test.ts tests/v2-runtime.test.ts
```

Expected: FAIL because `RunHandle.stats.diagnostics` and expanded snapshots are absent.

- [ ] **Step 3: Extend run statistics with an optional fixed object**

```ts
export interface RunDiagnostics {
  hostPrepareMs: number | null;
  hostSchedulerWaitMs: number | null;
  hostPublishToAckMs: number | null;
  sidecarFirstFrameToCommitMs: number | null;
  sidecarClaimSyncMs: number | null;
  sidecarAdmissionWaitMs: number | null;
  sidecarCommitProcessingMs: number | null;
  transportCommitResidualMs: number | null;
  upstreamFirstEventMs: number | null;
  sidecarFirstEventSchedulerMs: number | null;
  hostAckToFirstFrameMs: number | null;
  hostAssemblyToDeliveryMs: number | null;
  peakTransport: V2TransportDiagnostics | null;
}

export interface RunHandleStats {
  bytes: number;
  firstByteMs: number | null;
  frames: number;
  diagnostics?: RunDiagnostics;
}
```

All existing fake handles remain valid because `diagnostics` is optional.

- [ ] **Step 4: Record host stages and safe residuals**

In `V2TransportSession`, record preparation start/end, first scheduler enqueue, first publication, ACK receipt, first response-frame receipt, completed event assembly, and handle delivery. Calculate:

```ts
transportCommitResidualMs = Math.max(
  0,
  hostPublishToAckMs - sidecarCommitProcessingMs,
);
```

Do not subtract sidecar-local `firstFrameToCommitMs` from a host timestamp; carry it as an independent field.

- [ ] **Step 5: Expose bounded runtime snapshots**

Expand `V2TransportSnapshot` and `RoutableSandboxSnapshot` with optional `diagnostics`. The snapshot includes host window/scheduler pressure plus the latest valid sidecar snapshot. Keep `capacity`, `available`, `queued`, `ackEwmaMs`, and routing semantics unchanged.

- [ ] **Step 6: Verify Task 5 and commit**

```powershell
bun test --isolate tests/v2-host-observer.test.ts tests/v2-transport-session.test.ts tests/v2-run-handle.test.ts tests/v2-runtime.test.ts tests/multi-sandbox-router.test.ts
bun run typecheck
git add src/v2/diagnostics/host-observer.ts src/relay/run-handle.ts src/v2/transport/run-handle.ts src/v2/transport/session.ts src/v2/runtime.ts src/sandbox/registry.ts tests/v2-host-observer.test.ts tests/v2-transport-session.test.ts tests/v2-run-handle.test.ts tests/v2-runtime.test.ts
git commit -m "feat: correlate v2 host transport stages"
```

---

### Task 6: Aggregate WebSocket Delivery Telemetry

**Files:**
- Create: `src/ws/telemetry.ts`
- Modify: `src/ws/session.ts`
- Modify: `src/ws/server.ts`
- Modify: `src/server/runtime.ts`
- Modify: `src/server/system.ts`
- Test: `tests/websocket-telemetry.test.ts`
- Modify: `tests/websocket.test.ts`

**Interfaces:**
- Produces: `WebSocketTelemetry` with session/response lifecycle methods, `recordSend`, and `snapshotAndReset`.
- Produces: `WebSocketTelemetrySnapshot` containing only aggregate counts/distributions.
- Injects: one telemetry instance into every `ResponsesWebSocketSession` owned by a server.

- [ ] **Step 1: Write failing aggregate/privacy tests**

```ts
const telemetry = new WebSocketTelemetry({ capacity: 256 });
telemetry.sessionOpened();
telemetry.responseStarted();
telemetry.recordSend({ queueMs: 12, callbackMs: 3, bufferedBytes: 2_048, delayed: true });
expect(telemetry.snapshotAndReset()).toMatchObject({
  activeSessions: 1,
  activeResponses: 1,
  sendQueueDepthPeak: 1,
  bufferedBytesPeak: 2_048,
  delayedByBackpressure: 1,
  enqueueToSend: { p95: 12 },
  sendCallback: { p95: 3 },
});
expect(JSON.stringify(telemetry.snapshotAndReset())).not.toMatch(/client|stream|url|token|prompt/i);
```

- [ ] **Step 2: Confirm RED**

```powershell
bun test --isolate tests/websocket-telemetry.test.ts tests/websocket.test.ts
```

Expected: FAIL because the aggregate telemetry service is absent.

- [ ] **Step 3: Implement constant-memory aggregate telemetry**

Use two `BoundedDistribution` instances and scalar counters. `snapshotAndReset` resets interval peaks/distributions but preserves current active session/response gauges.

- [ ] **Step 4: Instrument the existing send chain**

In `ResponsesWebSocketSession.send`, record enqueue time before appending to `sendTail`, time spent waiting for buffered bytes, and callback duration around the existing `socket.send`. Increment/decrement session and response gauges in existing lifecycle methods. Do not store or pass `stream_id`, request data, or serialized outbound JSON to telemetry.

- [ ] **Step 5: Inject one server-wide service**

Extend server factory options with optional `webSocketTelemetry`. Create one in `createBrowserlessSystem` when diagnostics are enabled and pass it through `createGhostProvServer`/WebSocket upgrade composition. Disabled mode passes no telemetry and preserves the current send chain.

- [ ] **Step 6: Verify Task 6 and commit**

```powershell
bun test --isolate tests/websocket-telemetry.test.ts tests/websocket.test.ts tests/http-streaming.test.ts tests/system-v2.test.ts
bun run typecheck
git add src/ws/telemetry.ts src/ws/session.ts src/ws/server.ts src/server/runtime.ts src/server/system.ts tests/websocket-telemetry.test.ts tests/websocket.test.ts
git commit -m "feat: sample websocket delivery pressure"
```

---

### Task 7: Privacy-Safe Observability Integration

**Files:**
- Modify: `src/observability/events.ts`
- Modify: `src/observability/redaction.ts`
- Modify: `src/observability/traced-run-handle.ts`
- Modify: `src/observability/sampler.ts`
- Modify: `src/server/system.ts`
- Modify: `tests/observability-redaction.test.ts`
- Modify: `tests/observability-run-handle.test.ts`
- Modify: `tests/observability-sampler.test.ts`
- Test: `tests/v2-diagnostics-observability.test.ts`

**Interfaces:**
- Consumes: `RunDiagnostics`, runtime diagnostic snapshots, and `WebSocketTelemetrySnapshot`.
- Extends: existing `transport.committed`, `response.first_event`, `request.completed`, `request.failed`, and `fleet.sample` allowlists.
- Produces: diagnostic JSONL using existing bounded logger semantics.

- [ ] **Step 1: Write failing allowlist and lifecycle tests**

Assert a traced handle emits:

```ts
expect(logger.events.find(event => event.event === "transport.committed")).toMatchObject({
  host_prepare_ms: 10,
  host_publish_to_ack_ms: 20,
  sidecar_claim_sync_ms: 2,
  transport_commit_residual_ms: 17,
});
expect(logger.events.find(event => event.event === "response.first_event")).toMatchObject({
  upstream_first_event_ms: 100,
  sidecar_scheduler_ms: 4,
  host_ack_to_first_frame_ms: 25,
});
```

Create a full fleet sample and assert:

```ts
expect(JSON.stringify(sample)).not.toMatch(/prompt|output|content|arguments|headers?|cookie|authorization|token|secret|body|url|client|stream_id/i);
expect(() => assertSafeObservabilityEvent(sample)).not.toThrow();
```

- [ ] **Step 2: Confirm RED**

```powershell
bun test --isolate tests/v2-diagnostics-observability.test.ts tests/observability-redaction.test.ts tests/observability-run-handle.test.ts tests/observability-sampler.test.ts
```

Expected: FAIL because diagnostic event fields are not allowlisted or emitted.

- [ ] **Step 3: Extend explicit event allowlists**

Add only snake-case scalar fields to request lifecycle events. Keep resource, transport, and WebSocket fleet snapshots fixed-shape and built explicitly in `FleetSampler`; never cast arbitrary runtime objects into `JsonValue`.

- [ ] **Step 4: Emit diagnostics conditionally**

When `handle.stats.diagnostics` is absent, event JSON must remain unchanged. When present, use a helper that maps every property explicitly and converts non-finite values to `null`. Terminal events include peak pressure but never raw errors.

- [ ] **Step 5: Integrate aggregate WebSocket and sidecar samples**

Inject a `webSocketSnapshot?: () => WebSocketTelemetrySnapshot` dependency into `FleetSampler`. Per-runtime diagnostics include only the latest safe resource/transport object and malformed diagnostic count. Reset interval WebSocket distributions after each fleet sample.

- [ ] **Step 6: Verify Task 7 and commit**

```powershell
bun test --isolate tests/v2-diagnostics-observability.test.ts tests/observability-redaction.test.ts tests/observability-run-handle.test.ts tests/observability-sampler.test.ts tests/router-observability.test.ts
bun run typecheck
git add src/observability src/server/system.ts tests/v2-diagnostics-observability.test.ts tests/observability-redaction.test.ts tests/observability-run-handle.test.ts tests/observability-sampler.test.ts
git commit -m "feat: log v2 boundary diagnostics"
```

---

### Task 8: Controlled 1/4/8/16 Concurrency Probe and Classifier

**Files:**
- Create: `src/v2/diagnostics/classify.ts`
- Create: `scripts/diagnose_v2_concurrency.ts`
- Modify: `package.json`
- Modify: `.env.example`
- Modify: `README.md`
- Test: `tests/v2-diagnostics-classifier.test.ts`
- Test: `tests/v2-diagnostics-probe.test.ts`
- Modify: `tests/documentation.test.ts`

**Interfaces:**
- Produces: `classifyV2Diagnostics(report): V2DiagnosticClassification` using the approved ordered rules.
- Produces: import-safe `resolveDiagnosticProbeOptions`, `runDiagnosticStage`, `waitForFleetIdle`, and `main`.
- Adds: `diagnose:v2-concurrency` package command.

- [ ] **Step 1: Write failing classifier tests for every outcome**

```ts
expect(classifyV2Diagnostics(report({
  sidecarCpuP95: 90,
  sidecarEventLoopP95Ms: 5,
})).primary).toBe("sandbox_resource_pressure");

expect(classifyV2Diagnostics(report({
  ackProgressAgeP95Ms: baseline.ackProgressAgeP95Ms * 2.5,
  sidecarCpuP95: 20,
  sidecarEventLoopP95Ms: 2,
})).primary).toBe("awareness_transport_pressure");

expect(classifyV2Diagnostics(report({
  upstreamFirstEventP95Ms: baseline.upstreamFirstEventP95Ms * 2.5,
  transportResidualRatio: 1.1,
  webSocketDeliveryRatio: 1.1,
})).primary).toBe("upstream_provider_pressure");

expect(classifyV2Diagnostics(report({
  webSocketSendP95Ms: 80,
})).primary).toBe("client_backpressure");
```

- [ ] **Step 2: Write failing probe guards and identical-body tests**

```ts
expect(() => resolveDiagnosticProbeOptions({})).toThrow("GP_LIVE_V2_DIAGNOSTICS=1");
expect(() => resolveDiagnosticProbeOptions({
  GP_LIVE_V2_DIAGNOSTICS: "1",
  GP_V2_DIAGNOSTIC_ROOT: "http://127.0.0.1:8787",
})).toThrow("8787");

const bodies = await captureBodies(runDiagnosticStage(options, 16, fakeWebSocketFactory));
expect(new Set(bodies.map(body => JSON.stringify(body))).size).toBe(1);
expect(bodies).toHaveLength(16);
```

- [ ] **Step 3: Confirm RED**

```powershell
bun test --isolate tests/v2-diagnostics-classifier.test.ts tests/v2-diagnostics-probe.test.ts tests/documentation.test.ts
```

Expected: FAIL because classifier and probe do not exist.

- [ ] **Step 4: Implement the ordered classifier**

Return:

```ts
export interface V2DiagnosticClassification {
  primary:
    | "sandbox_resource_pressure"
    | "awareness_transport_pressure"
    | "upstream_provider_pressure"
    | "client_backpressure"
    | "mixed_pressure"
    | "no_pressure_detected";
  triggered: readonly string[];
  evidence: Readonly<Record<string, number | null>>;
}
```

Evaluate sandbox, transport, upstream, and client rules independently, then return `mixed_pressure` when more than one category triggers. Keep exact thresholds from the design specification.

- [ ] **Step 5: Implement guarded staged WebSocket execution**

Use stages `[1, 4, 8, 16]` and three waves each. Resolve and recursively freeze this exact event once before the first stage:

```ts
const padding = "0123456789abcdef".repeat(16_384);
const event = deepFreeze({
  type: "response.create",
  model: "gpt-5.6-sol",
  store: false,
  service_tier: "fast",
  reasoning: { effort: "low" },
  input: [{
    role: "user",
    content: [{
      type: "input_text",
      text:
        "Return exactly 256 numbered lines from 0001 through 0256. " +
        "Each line must contain only its four-digit number. Do not use tools.\n" +
        "<diagnostic-padding>\n" + padding,
    }],
  }],
});
```

Each client uses a separate WebSocket connection and sends the exact serialized event. Record event timestamps, byte counts, maximum inter-event gap, exact terminal identity, and errors without recording event data.

`waitForFleetIdle` polls `/health` and `/__sandboxes` until `active === 0 && queued === 0` for three consecutive polls, with a 60-second deadline and abort support.

- [ ] **Step 6: Produce one bounded JSON report**

Write to `.ghostprov/diagnostics/v2-<ISO timestamp>.json` using an atomic temporary file/rename. Include options, stage aggregates, per-project aggregates, telemetry coverage, classification, and failures. Exclude request bodies and event data. Print only the report path and compact classification summary.

- [ ] **Step 7: Add command and documentation**

```json
"diagnose:v2-concurrency": "bun scripts/diagnose_v2_concurrency.ts"
```

Document:

```powershell
$env:GP_V2_DIAGNOSTICS = "1"
$env:GP_LIVE_V2_DIAGNOSTICS = "1"
$env:GP_V2_DIAGNOSTIC_ROOT = "http://127.0.0.1:18787"
bun run diagnose:v2-concurrency
```

State clearly that enabling host diagnostics requires a manual restart and the probe incurs real API usage.

Add these exact test entries to the README inventory before the documentation test runs:

```text
tests/v2-diagnostics-types.test.ts
tests/v2-diagnostics-statistics.test.ts
tests/v2-sidecar-diagnostics.test.ts
tests/v2-diagnostics-protocol.test.ts
tests/v2-sidecar-observer.test.ts
tests/v2-host-observer.test.ts
tests/websocket-telemetry.test.ts
tests/v2-diagnostics-observability.test.ts
tests/v2-diagnostics-classifier.test.ts
tests/v2-diagnostics-probe.test.ts
tests/v2-diagnostics-overhead.test.ts
```

- [ ] **Step 8: Verify Task 8 and commit**

```powershell
bun test --isolate tests/v2-diagnostics-classifier.test.ts tests/v2-diagnostics-probe.test.ts tests/documentation.test.ts
bun run typecheck
git add src/v2/diagnostics/classify.ts scripts/diagnose_v2_concurrency.ts package.json .env.example README.md tests/v2-diagnostics-classifier.test.ts tests/v2-diagnostics-probe.test.ts tests/documentation.test.ts
git commit -m "feat: add v2 concurrency diagnostic probe"
```

---

### Task 9: Observer-Effect Gate, Full Verification, and Handoff

**Files:**
- Test: `tests/v2-diagnostics-overhead.test.ts`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-08-23-v2-concurrency-diagnostics-design.md` only if implementation reveals a factual mismatch; do not change approved scope.

**Interfaces:**
- Consumes: completed diagnostic host/sidecar/probe implementation.
- Produces: deterministic proof that diagnostics do not change protocol behavior and remain inside size/CPU/latency budgets.

- [ ] **Step 1: Write the diagnostics-off equivalence gate**

Run identical deterministic host/sidecar exchanges with diagnostics absent and disabled. Assert the same core frame canonical JSON, sequence numbers, ACK cursors, SSE events, terminal proof, response ID, byte count, and publication count.

- [ ] **Step 2: Write the diagnostics-on observer-effect gate**

Execute at least 20 warm-up and 100 measured deterministic 16-stream waves with diagnostics off and on. Use fixed manual clocks for protocol equivalence, then use `performance.now()` around complete in-memory waves for observer-effect ratios. Assert:

```ts
expect(on.encodedDiagnosticBytesMax).toBeLessThanOrEqual(2_048);
expect(on.additionalPublications).toBe(0);
expect(on.sidecarSamplerPeakBytes).toBeLessThanOrEqual(1024 * 1024);
expect(on.sidecarSamplerCpuPercent - off.sidecarSamplerCpuPercent).toBeLessThan(1);
expect(on.throughput / off.throughput).toBeGreaterThanOrEqual(0.98);
expect(on.p95Ms / off.p95Ms).toBeLessThanOrEqual(1.02);
```

Measure CPU with `process.cpuUsage()` around isolated sampler batches, normalize microseconds by elapsed wall time and one logical core, use five batch medians, and print measured ratios on failure. Require at least 99% diagnostic-stage coverage for completed requests; missing diagnostics increment coverage counters rather than failing requests.

- [ ] **Step 3: Run focused diagnostic verification**

```powershell
bun test --isolate tests/v2-diagnostics-types.test.ts tests/v2-diagnostics-statistics.test.ts tests/v2-sidecar-diagnostics.test.ts tests/v2-diagnostics-protocol.test.ts tests/v2-sidecar-observer.test.ts tests/v2-host-observer.test.ts tests/websocket-telemetry.test.ts tests/v2-diagnostics-observability.test.ts tests/v2-diagnostics-classifier.test.ts tests/v2-diagnostics-probe.test.ts tests/v2-diagnostics-overhead.test.ts
bun run typecheck
```

Expected: all focused tests pass and no warning/error output appears.

- [ ] **Step 4: Run the complete verification suite**

```powershell
bun test --isolate
bun run typecheck
bun run test:daemon
bun audit --production
bun run verify:v2-smoke
```

Expected: all Bun and Python tests pass, TypeScript exits 0, audit reports no vulnerabilities, and deterministic HTTP/WebSocket smoke completes on an OS-assigned candidate port.

- [ ] **Step 5: Prove the user's live listener was untouched**

Record the current PID/start time before implementation and recheck it after verification. If the user has stopped it themselves, record that no agent command stopped or restarted it. Never launch the instrumented build on port 18787 during implementation.

- [ ] **Step 6: Commit the observer-effect gate**

```powershell
git add tests/v2-diagnostics-overhead.test.ts README.md
git commit -m "test: enforce v2 diagnostic overhead budgets"
```

- [ ] **Step 7: Push and request the single manual restart**

Fetch `origin/master`, refuse a non-fast-forward update, and push the verified commits. Tell the operator to set `GP_V2_DIAGNOSTICS=1`, manually restart once, wait for four ready runtimes, then run `bun run diagnose:v2-concurrency` with the explicit live opt-in. Do not make performance changes until the resulting report is classified and reviewed.

## Completion Evidence

The handoff must include:

- commit hashes and clean `master...origin/master` status;
- exact Bun/Python test counts, typecheck, audit, and smoke output;
- diagnostics-off core-protocol equivalence result;
- maximum diagnostic envelope bytes, sidecar sampler memory, CPU overhead, throughput ratio, and p95-latency ratio;
- confirmation that diagnostics cause zero additional Awareness publications;
- exact restart/probe commands;
- report path and classification after the operator's controlled run;
- an explicit statement that no routing/window/capacity tuning was included.
