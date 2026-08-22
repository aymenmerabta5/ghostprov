# GhostProv V2 Performance Observability Design

**Status:** Approved design direction; implementation requires a later GhostProv restart to activate built-in tracing.

## Objective

Make a slow or unhealthy multi-agent run diagnosable from one bounded, privacy-safe JSONL trace. For every request, operators must be able to distinguish host admission pressure, router queueing, sandbox selection imbalance, Awareness transport latency, upstream first-token latency, long model generation, client backpressure, and terminal recovery. The implementation must not log prompts, responses, credentials, headers, tool arguments, or raw error objects.

The live service on port `18787` must not be stopped during implementation. A separate monitor provides immediate aggregate visibility; built-in request tracing becomes active on the operator's next manual restart.

## Success Criteria

1. Every accepted request receives an opaque `trace_id` that follows it from ingress through terminal settlement.
2. One request trace reports admission wait, router queue wait, route/commit ACK latency, first-event latency, terminal duration, event count, and byte count.
3. A five-second fleet sample reports host pressure and sanitized metrics for every sandbox.
4. Logs identify the bottleneck category without inspecting request or response bodies.
5. Logging never blocks routing or response backpressure.
6. The asynchronous logging queue is bounded by event count and encoded bytes.
7. Files rotate at a configured size and old files are retained to a configured count.
8. Logging hot-path enqueue p95 remains below 0.25 ms in deterministic tests.
9. At default settings, logging consumes no more than 8 MiB of queued memory and less than 1% CPU during a 512-request deterministic load.
10. The external monitor works against an already-running V2 service without restarting it.

## Architecture

### Structured Logger

Create a small observability module with an allowlisted event API rather than accepting arbitrary objects. It owns:

- a schema version;
- wall-clock and monotonic timestamps;
- a bounded priority queue;
- a console sink;
- a rotating JSONL file sink;
- drop accounting and sink-failure reporting;
- graceful flush with a fixed shutdown deadline.

The router and runtimes enqueue plain immutable events. A single asynchronous writer batches up to 256 events or 256 KiB per write and flushes at least every 250 ms. Request terminal and failure events have higher priority than periodic samples. If the queue is full, periodic samples are dropped first, followed by non-terminal progress events. The logger emits a summarized `logger.dropped` event when capacity returns.

Logging failures never reject or delay a model request. Console fallback remains available if the file sink becomes unhealthy.

### File Rotation

The default directory is:

```text
C:\Users\Aymen\.ghostprov\fresh-v2\logs
```

Active files use `ghostprov-v2.current.<pid>.jsonl`. Rotation occurs before a write would exceed 64 MiB. Rotated names include an ISO-like UTC timestamp and monotonically increasing sequence. Five rotated files are retained by default. Cleanup only targets files matching GhostProv's exact naming convention inside the resolved configured directory.

Each JSON event occupies exactly one line. Readers ignore a final partial line after a process crash.

## Request Lifecycle Events

The router generates a cryptographically random trace ID. It never derives the ID from prompt, client, response, or project data.

### `request.accepted`

Fields:

- `trace_id`
- `source`: `http`, `websocket`, `image`, or `internal`
- `model`
- `service_tier`
- `stream`
- `workload_class`
- `request_bytes`
- `reserved_bytes`
- aggregate `capacity`, `active`, `available`, and `queued`

No input, instructions, tools, previous-response contents, headers, or client identifier are logged.

### `admission.acquired`

Fields:

- `trace_id`
- `admission_wait_ms`
- `reserved_bytes`
- governor `reserved_total_bytes`
- governor `reservation_capacity_bytes`
- `memory_paused`

This separates host RAM admission from router/sandbox queueing.

### `request.queued`

Fields:

- `trace_id`
- aggregate queue depth
- per-client queue depth without recording the client ID
- active and available capacity

### `route.selected`

Fields:

- `trace_id`
- selected `project_id`
- route decision time
- queue wait time
- selected sandbox capacity, active, reserved, queued, quarantined, Awareness RTT EWMA, completion EWMA, failure rate, heartbeat age, credential horizon, and memory pressure
- number of eligible candidates

Project UUIDs are operational identifiers, not credentials. Tokens and sandbox URLs are forbidden.

### `transport.committed`

`V2SandboxRuntime.run()` returns only after a matching committed ACK. The time between dispatch and returned handle is therefore recorded as `commit_ack_ms`. Fields also include total queue wait and selected project ID. V1 reports its published-handle latency under the same field with `transport: "v1"`.

### `response.first_event`

Wrap the returned `RunHandle` without pre-consuming it or changing backpressure. The wrapper logs only the first event observed by the real consumer:

- `trace_id`
- `first_event_ms` from ingress
- `post_commit_first_event_ms`
- project ID

If a client never consumes events, this event is intentionally absent; terminal logging still occurs through `handle.final`.

### `request.completed`

Fields:

- `trace_id`
- project ID
- total duration
- queue wait
- commit ACK latency
- first-event latency when observed
- encoded response bytes
- logical frame count
- terminal status
- retry count before commitment

### `request.failed`

Fields:

- `trace_id`
- stage: admission, queue, route, pre-commit, committed, transport, terminal, or client cancellation
- sanitized error name, kind, status, and bounded message
- whether commitment occurred
- whether the request was quarantined
- retry count

The message is limited to 256 printable characters and passed through the existing credential redactor.

## Periodic Fleet and Host Sampling

Every five seconds, emit one `fleet.sample` event containing:

- aggregate capacity, active, available, and queued;
- process RSS, heap, external bytes, and reserved request bytes;
- system total/free memory;
- event-loop delay p50, p95, and maximum for the interval;
- process CPU utilization delta;
- CPU-transform active and queued counts;
- logger queue bytes/events and dropped counts;
- one sanitized entry per runtime with state, capacity, active, available, quarantined, queue depth, Awareness RTT EWMA, completion EWMA, failure rate, heartbeat age, credential expiry horizon, and memory pressure.

Use Node's event-loop delay histogram with a bounded resolution and reset it after every sample. Sampling must not synchronously write to disk.

## Immediate External Monitor

Add `scripts/monitor_v2.ts` and `bun run monitor:v2`. It operates without changing or restarting the running service.

The monitor:

1. polls `/health` and `/__sandboxes` every two seconds;
2. writes the same JSONL fleet sample schema;
3. records polling latency and HTTP failures;
4. retries with capped exponential backoff and jitter;
5. uses the same rotating sink;
6. exits cleanly on Ctrl+C;
7. never calls a mutating GhostProv endpoint.

The external monitor cannot measure individual request queue/commit/first-event phases; those require built-in tracing after restart.

## Diagnosing a Slow 16-Agent Run

Interpret the trace in this order:

1. High `admission_wait_ms` or `memory_paused=true`: host memory pressure.
2. High router queue wait with zero available capacity: fleet concurrency is saturated.
3. High router queue wait while capacity is available: scheduler fairness or stale capacity reporting.
4. High `commit_ack_ms`: Y-Sweet/Awareness transport or sandbox event-loop pressure.
5. Normal commit ACK but high `post_commit_first_event_ms`: upstream model/provider latency.
6. Normal first event but high total duration: long reasoning or generation time.
7. One project with higher RTT, completion EWMA, failure rate, or quarantine: unhealthy sandbox or routing skew.
8. High host event-loop p95/max: local CPU transform or synchronous work is delaying routing.
9. Rising client buffer/backpressure counters with normal upstream timing: the consuming Codex process is slow.

## Privacy and Redaction

All event types use explicit field construction. The logger rejects unknown fields in tests and never serializes arbitrary request, response, error, credential, or runtime objects.

Forbidden content includes:

- prompts and instructions;
- model output and tool arguments;
- Authorization and Cookie headers;
- OpenAI, Prism, sandbox, resource, and Y-Sweet tokens;
- full URLs containing query strings;
- encrypted reasoning content;
- raw multipart/image data;
- browser or account identity information.

Credential-shaped values are replaced with `[REDACTED]`. Newlines and control characters are normalized before JSON encoding.

## Configuration

```dotenv
GP_LOG_LEVEL=info
GP_LOG_DIR=C:/Users/Aymen/.ghostprov/fresh-v2/logs
GP_LOG_ROTATE_BYTES=67108864
GP_LOG_RETAIN_FILES=5
GP_LOG_QUEUE_MAX_EVENTS=10000
GP_LOG_QUEUE_MAX_BYTES=8388608
GP_LOG_FLUSH_MS=250
GP_METRICS_INTERVAL_MS=5000
GP_SLOW_REQUEST_MS=10000
GP_SLOW_FIRST_EVENT_MS=5000
```

`GP_LOG_LEVEL=off` disables built-in logging. Invalid values fail startup before binding the public listener. The external monitor uses the same directory and rotation settings but a separate filename prefix.

## Testing

Add deterministic tests for:

- JSON schema and field allowlists;
- credential/prompt/response redaction;
- bounded queue priority and drop reporting;
- serialized asynchronous writes;
- rotation and exact retention scope;
- sink failure and console fallback;
- lifecycle event ordering and exactly-once terminal logging;
- first-event wrapping without changing stream backpressure;
- queue, commit ACK, first-event, and total timing;
- aggregate/per-sandbox samples;
- event-loop and memory sampling;
- monitor retries, jitter, and clean shutdown;
- Windows paths and atomic rotation behavior;
- hot-path enqueue performance budget;
- 512-request deterministic load with no terminal event loss.

The existing HTTP, SSE, WebSocket, continuation, image, failure, and concurrency suites must remain green.

## Rollout

1. Implement and verify without stopping port `18787`.
2. Start the external monitor against the existing process for immediate fleet evidence.
3. Commit and push the implementation.
4. The operator later restarts GhostProv manually.
5. Confirm `server.started`, four ready runtimes, and periodic samples.
6. Run a 16-agent workload and classify its bottleneck using the decision order above.

No implementation step is authorized to stop, replace, or bind over the current `18787` process.
