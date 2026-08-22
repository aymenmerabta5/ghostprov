# GhostProv V2 Performance Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add privacy-safe rotating JSONL request tracing, fleet/host performance sampling, and a zero-downtime external monitor that makes 16-agent slowdowns attributable to a specific subsystem.

**Architecture:** A focused observability package exposes allowlisted event types and a non-blocking bounded logger with console and rotating-file sinks. The router wraps each `RunHandle` to trace admission, queueing, routing, commit ACK, first event, and terminal settlement, while a periodic sampler records host and per-sandbox pressure. A standalone monitor reuses the rotating sink and polls the existing read-only endpoints without restarting GhostProv.

**Tech Stack:** Bun 1.4.0, TypeScript 7.0.2, Node/Bun filesystem APIs, `node:perf_hooks`, Hono, Bun test with `--isolate`.

## Global Constraints

- Never stop, replace, or bind over the existing process on port `18787` during implementation.
- Never log prompts, instructions, model output, tool arguments, headers, cookies, tokens, encrypted reasoning, multipart bodies, or URLs with queries.
- Logging may not reject, delay, or alter request routing, event ordering, response backpressure, or terminal settlement.
- Default file rotation is 64 MiB with five retained files.
- Default queue limits are 10,000 events and 8 MiB encoded data.
- Request terminal/failure events outrank progress and periodic sample events.
- All configuration is validated before the public listener binds.
- Preserve the user's existing uncommitted `package.json` removal of `dev:candidate`.

---

### Task 1: Observability Configuration, Schema, and Redaction

**Files:**
- Create: `src/observability/config.ts`
- Create: `src/observability/events.ts`
- Create: `src/observability/redaction.ts`
- Test: `tests/observability-config.test.ts`
- Test: `tests/observability-redaction.test.ts`

**Interfaces:**
- Produces: `resolveObservabilityConfig(env): ObservabilityConfig`
- Produces: `ObservabilityEvent`, `ObservabilityPriority`, and explicit event payload interfaces.
- Produces: `sanitizeDiagnosticMessage(value): string` and `assertSafeObservabilityEvent(event): void`.

- [ ] **Step 1: Write failing configuration tests**

```ts
expect(resolveObservabilityConfig({})).toMatchObject({
  level: "info",
  rotateBytes: 64 * 1024 * 1024,
  retainFiles: 5,
  queueMaxEvents: 10_000,
  queueMaxBytes: 8 * 1024 * 1024,
  flushMs: 250,
  metricsIntervalMs: 5_000,
});
expect(() => resolveObservabilityConfig({ GP_LOG_ROTATE_BYTES: "0" })).toThrow();
```

- [ ] **Step 2: Run tests and confirm RED**

Run: `bun test --isolate tests/observability-config.test.ts`
Expected: FAIL because the observability module does not exist.

- [ ] **Step 3: Implement strict configuration parsing**

```ts
export interface ObservabilityConfig {
  level: "off" | "error" | "info" | "debug";
  directory: string;
  rotateBytes: number;
  retainFiles: number;
  queueMaxEvents: number;
  queueMaxBytes: number;
  flushMs: number;
  metricsIntervalMs: number;
  slowRequestMs: number;
  slowFirstEventMs: number;
}
```

Resolve `GP_LOG_DIR` absolutely and enforce finite positive bounds. Use `C:/Users/Aymen/.ghostprov/fresh-v2/logs` only in `.env`; code defaults to `.ghostprov/logs` for portability.

- [ ] **Step 4: Write failing redaction and allowlist tests**

```ts
const safe = sanitizeDiagnosticMessage("Authorization: Bearer sk-secret-value");
expect(safe).toContain("[REDACTED]");
expect(safe).not.toContain("sk-secret-value");
expect(() => assertSafeObservabilityEvent({
  schema: 1,
  event: "request.accepted",
  ts: new Date().toISOString(),
  prompt: "forbidden",
} as any)).toThrow("prompt");
```

- [ ] **Step 5: Implement allowlisted event types and redaction**

Define events for `server.started`, `server.stopping`, `request.accepted`, `admission.acquired`, `request.queued`, `route.selected`, `transport.committed`, `response.first_event`, `request.completed`, `request.failed`, `fleet.sample`, `monitor.sample`, `monitor.error`, `logger.dropped`, and `logger.error`. Validate only field names; do not recursively serialize arbitrary objects.

- [ ] **Step 6: Run Task 1 tests and commit**

Run: `bun test --isolate tests/observability-config.test.ts tests/observability-redaction.test.ts`
Expected: PASS.

```powershell
git add src/observability tests/observability-config.test.ts tests/observability-redaction.test.ts
git commit -m "feat: define safe observability events"
```

### Task 2: Bounded Asynchronous Rotating JSONL Logger

**Files:**
- Create: `src/observability/rotating-jsonl.ts`
- Create: `src/observability/logger.ts`
- Test: `tests/observability-logger.test.ts`
- Test: `tests/observability-rotation.test.ts`

**Interfaces:**
- Consumes: `ObservabilityConfig`, `ObservabilityEvent`, `ObservabilityPriority`.
- Produces: `ObservabilityLogger` with `emit`, `snapshot`, and `close`.
- Produces: `RotatingJsonlSink` with `writeBatch` and `close`.

- [ ] **Step 1: Write failing bounded-queue tests**

```ts
const logger = new ObservabilityLogger({
  config: testConfig({ queueMaxEvents: 2 }),
  sinks: [sink],
});
logger.emit(event("fleet.sample"), "sample");
logger.emit(event("request.completed"), "terminal");
logger.emit(event("request.failed"), "terminal");
await logger.close();
expect(sink.events.map(value => value.event)).toEqual([
  "request.completed",
  "request.failed",
  "logger.dropped",
]);
```

- [ ] **Step 2: Confirm RED**

Run: `bun test --isolate tests/observability-logger.test.ts`
Expected: FAIL because the logger does not exist.

- [ ] **Step 3: Implement the logger queue**

```ts
export interface LoggerSnapshot {
  queuedEvents: number;
  queuedBytes: number;
  droppedSamples: number;
  droppedProgress: number;
  sinkErrors: number;
}

export interface LoggerLike {
  emit(event: ObservabilityEvent, priority?: ObservabilityPriority): void;
  snapshot(): LoggerSnapshot;
  close(deadlineMs?: number): Promise<void>;
}
```

Encode at enqueue time to enforce the byte limit. Maintain separate terminal, progress, and sample queues; drain them in priority order. Serialize sink writes through one promise chain. Sink errors increment counters and fall back to sanitized console output.

- [ ] **Step 4: Write failing rotation and retention tests**

Use a temporary directory. Write batches that cross a 1 KiB test threshold, assert every JSON line parses, assert exact naming, and assert only the configured number of matching rotated files remain. Place unrelated files in the same directory and prove they are never removed.

- [ ] **Step 5: Implement atomic bounded rotation**

Use exclusive active-file ownership per PID, append-only writes, `FileHandle.sync()` on close/rotation, and an exact filename regular expression. Rotate before a batch crosses the limit. Retention sorts only matching rotated files by timestamp/sequence.

- [ ] **Step 6: Verify hot-path performance**

Enqueue 100,000 small events into an in-memory sink and assert p95 enqueue duration below 0.25 ms and queue bytes never exceed the configured cap.

- [ ] **Step 7: Run Task 2 tests and commit**

Run: `bun test --isolate tests/observability-logger.test.ts tests/observability-rotation.test.ts`
Expected: PASS.

```powershell
git add src/observability tests/observability-logger.test.ts tests/observability-rotation.test.ts
git commit -m "feat: add bounded rotating jsonl logging"
```

### Task 3: Request Lifecycle Tracing in the Router

**Files:**
- Create: `src/observability/traced-run-handle.ts`
- Modify: `src/router/router.ts`
- Modify: `src/server/system.ts`
- Modify: `src/server/app.ts`
- Test: `tests/router-observability.test.ts`
- Test: `tests/observability-run-handle.test.ts`

**Interfaces:**
- Consumes: `LoggerLike` and existing `RunHandle`.
- Produces: `traceRunHandle(handle, context): RunHandle` without pre-consuming events.
- Extends: `MultiSandboxRouter` constructor with optional `logger?: LoggerLike`.

- [ ] **Step 1: Write failing lifecycle-order tests**

```ts
const handle = await router.startRelay(body, { source: "websocket" });
for await (const _event of handle.events()) {}
await handle.final;
expect(logger.events.map(value => value.event)).toEqual([
  "request.accepted",
  "admission.acquired",
  "request.queued",
  "route.selected",
  "transport.committed",
  "response.first_event",
  "request.completed",
]);
```

Test pre-commit failure, committed terminal failure, abort while queued, and a final resolved without event consumption. Assert exactly one terminal event per trace.

- [ ] **Step 2: Confirm RED**

Run: `bun test --isolate tests/router-observability.test.ts tests/observability-run-handle.test.ts`
Expected: FAIL because lifecycle tracing is absent.

- [ ] **Step 3: Add trace state to queued jobs**

Extend `QueuedJob` with `traceId`, `acceptedAtMs`, `admittedAtMs`, `queuedAtMs`, `selectedAtMs`, `projectId`, `commitAckMs`, and `retryCount`. Generate `crypto.randomUUID()` at ingress. Build each event from explicit scalar fields.

- [ ] **Step 4: Trace admission, queue, and route selection**

Log admission duration around `governor.acquire`. Log the complete chosen candidate snapshot and eligible candidate count before dispatch. Do not log policy internals or body contents.

- [ ] **Step 5: Wrap committed handles**

```ts
return {
  events: async function* () {
    for await (const frame of handle.events()) {
      if (!firstSeen) logFirstEvent();
      yield frame;
    }
  },
  final: tracedFinal,
  stats: handle.stats,
};
```

Record commit ACK when `runtime.run()` resolves. Terminal logging attaches to `handle.final` and remains independent of event consumption. Preserve the original final value/error and stats object.

- [ ] **Step 6: Wire the logger into system composition**

Construct the logger before router creation, pass it to `MultiSandboxRouter`, emit `server.started`, and close it last with a fixed deadline during shutdown. Tests inject an in-memory logger.

- [ ] **Step 7: Run router/public API regression tests and commit**

Run: `bun test --isolate tests/router-observability.test.ts tests/observability-run-handle.test.ts tests/multi-sandbox-router.test.ts tests/router-http.test.ts tests/websocket.test.ts tests/v2-public-api.test.ts`
Expected: PASS.

```powershell
git add src/observability/traced-run-handle.ts src/router/router.ts src/server tests/router-observability.test.ts tests/observability-run-handle.test.ts
git commit -m "feat: trace request lifecycle latency"
```

### Task 4: Fleet, Host, and Event-Loop Sampler

**Files:**
- Create: `src/observability/sampler.ts`
- Modify: `src/server/system.ts`
- Test: `tests/observability-sampler.test.ts`

**Interfaces:**
- Consumes: router snapshot, logger snapshot, machine probe, process metrics, optional CPU pool snapshot.
- Produces: `FleetSampler` with `start()` and `close()`.

- [ ] **Step 1: Write failing sample tests**

Use a fake clock/probe and assert a `fleet.sample` contains aggregate capacity and every sanitized runtime metric from the design. Assert no fields match `/token|cookie|url|prompt|output/i`.

- [ ] **Step 2: Confirm RED**

Run: `bun test --isolate tests/observability-sampler.test.ts`
Expected: FAIL because the sampler does not exist.

- [ ] **Step 3: Implement event-loop/CPU/memory deltas**

Use `monitorEventLoopDelay({ resolution: 20 })`, `process.cpuUsage(previous)`, `process.memoryUsage()`, machine free/total memory, router resources, and runtime snapshots. Reset histogram values after each sample. Skip overlapping samples rather than queueing them.

- [ ] **Step 4: Integrate lifecycle**

Start after system construction and before listener bind. Stop the timer and event-loop histogram during shutdown before logger close.

- [ ] **Step 5: Run Task 4 tests and commit**

Run: `bun test --isolate tests/observability-sampler.test.ts tests/system-v2.test.ts tests/graceful-shutdown.test.ts`
Expected: PASS.

```powershell
git add src/observability/sampler.ts src/server/system.ts tests/observability-sampler.test.ts
git commit -m "feat: sample fleet and host pressure"
```

### Task 5: Zero-Downtime External Monitor

**Files:**
- Create: `scripts/monitor_v2.ts`
- Modify: `package.json`
- Test: `tests/v2-monitor.test.ts`

**Interfaces:**
- Consumes: `/health`, `/__sandboxes`, `ObservabilityLogger`.
- Produces: import-safe `resolveMonitorOptions`, `pollMonitorSample`, and `main`.
- Adds script: `"monitor:v2": "bun scripts/monitor_v2.ts"` while preserving the user's removal of `dev:candidate`.

- [ ] **Step 1: Write failing monitor tests**

Test one successful sample, a 503/transport failure, capped backoff with deterministic jitter, rejection of mutating URLs, import safety, and cleanup on abort.

- [ ] **Step 2: Confirm RED**

Run: `bun test --isolate tests/v2-monitor.test.ts`
Expected: FAIL because the monitor does not exist.

- [ ] **Step 3: Implement the monitor**

```ts
export interface MonitorOptions {
  root: string;
  intervalMs: number;
  requestTimeoutMs: number;
}

export async function pollMonitorSample(
  options: MonitorOptions,
  fetchImpl: typeof fetch,
): Promise<MonitorSample>;
```

Allow only loopback HTTP(S) roots by default. Poll both read-only endpoints with timeouts, log poll latency, and retry failures with exponential backoff capped at 30 seconds plus 0-20% jitter. `SIGINT`/`SIGTERM` abort polling and flush logs.

- [ ] **Step 4: Add the package command and default `.env` values**

Add only `monitor:v2`; preserve the existing user edit removing `dev:candidate`. Add logging settings to the ignored root `.env` and update `.env.example` without secrets.

- [ ] **Step 5: Start the external monitor without restarting GhostProv**

Run: `bun run monitor:v2`
Expected: process remains active, polls `http://127.0.0.1:18787`, and creates a JSONL file under the configured log directory. Stop only the monitor after a sample is proven; never stop port `18787`.

- [ ] **Step 6: Run Task 5 tests and commit**

Run: `bun test --isolate tests/v2-monitor.test.ts tests/documentation.test.ts`
Expected: PASS.

```powershell
git add scripts/monitor_v2.ts package.json .env.example tests/v2-monitor.test.ts
git commit -m "feat: add zero-downtime v2 monitor"
```

### Task 6: Documentation and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `.env.example`
- Test: `tests/documentation.test.ts`

**Interfaces:**
- Documents exact events, settings, start commands, log interpretation, rotation, privacy, and manual-restart activation.

- [ ] **Step 1: Add failing documentation assertions**

Assert README contains `monitor:v2`, `GP_LOG_DIR`, `request.accepted`, `transport.committed`, `response.first_event`, `fleet.sample`, and the slow-request interpretation sequence.

- [ ] **Step 2: Update documentation**

Document immediate monitor startup, later built-in activation, JSONL examples containing only safe fields, and commands to inspect the last log lines in PowerShell.

- [ ] **Step 3: Run the complete verification suite**

```powershell
bun test --isolate
bun run typecheck
bun run test:daemon
bun audit --production
bun run verify:v2-smoke
```

Expected: all Bun and Python tests pass, audit reports no vulnerabilities, and deterministic HTTP/WebSocket smoke completes.

- [ ] **Step 4: Verify live monitor evidence**

With the existing `18787` process still running, poll health and inspect the created monitor JSONL. Confirm at least one valid `monitor.sample`, no credential-shaped values, and no mutation to active/queued work.

- [ ] **Step 5: Commit final documentation**

```powershell
git add README.md .env.example tests/documentation.test.ts
git commit -m "docs: explain v2 performance diagnostics"
```

## Completion Evidence

The final handoff reports:

- commit hashes;
- exact test counts;
- typecheck/audit/smoke results;
- monitor log path;
- whether the built-in logger awaits a manual restart;
- current `18787` health without having stopped it;
- known diagnosis, if a 16-agent workload ran during monitoring.
