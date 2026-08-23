# GhostProv V2 Concurrency Diagnostics Design

## Objective

Identify why GhostProv V2 is fast with one Codex agent but develops multi-second-to-minute commit and first-event latency when many agents run concurrently. The diagnostic must distinguish Windows-host pressure, Y-Sweet/Awareness congestion, Linux-sandbox resource pressure, upstream OpenAI latency, and client WebSocket backpressure without changing routing, admission, window sizes, or concurrency behavior during the measurement run.

## Evidence and Current Understanding

The 13-minute fixed-build trace contains 76 accepted requests and a complete low-to-high concurrency ramp. It shows:

- Windows host CPU remained below 20%, free RAM remained above 11.8 GiB, event-loop p95 remained near 2 ms, and the router always reported at least 485 available lanes.
- Router admission and queueing remained below a few milliseconds.
- Per-sandbox load strongly predicts latency even when request size moves in the opposite direction.

| Existing work on selected sandbox | Median request | Commit p50 | Commit p95 | Post-commit first-event p95 |
| ---: | ---: | ---: | ---: | ---: |
| 0–1 | 1.44 MiB | 1.58 s | 4.64 s | 1.91 s |
| 2–3 | 285 KiB | 2.52 s | 5.64 s | 4.91 s |
| 4–5 | 253 KiB | 3.75 s | 18.34 s | 18.24 s |
| 6+ | 235 KiB | 15.16 s | 45.67 s | 34.07 s |

This rules out the Windows host and router admission as the primary cause. It does not distinguish Y-Sweet transport delay from Deno-sidecar CPU, memory, event-loop, filesystem, or upstream-provider delay because those boundaries are not currently measured.

The previous publication-storm fix remains valid: a 64-frame burst now produces one cumulative Awareness update instead of 64, and no-progress retransmissions back off. The follow-up diagnostic must not assume that increasing the 32-frame window is the next fix. Some 257+ event responses reached 46–89 events per second, while several 13-event responses remained sparse; upstream generation timing and transport capacity must be measured separately.

## Non-Goals

- Do not change the 32-frame or 512 KiB reliable window.
- Do not change per-sandbox physical capacity, router scoring, admission, batching, retransmission, or fairness.
- Do not add another transport or expose a sandbox port.
- Do not infer one-way latency by subtracting clocks from different machines.
- Do not log prompts, responses, SSE data, tool arguments, headers, cookies, credentials, URLs, client IDs, or stream IDs.
- Do not automatically create, delete, or replace Prism projects.

## Considered Approaches

### 1. Boundary instrumentation first — selected

Measure local durations and bounded resource/queue snapshots at every boundary, then run identical workloads at 1, 4, 8, and 16 concurrent clients. This requires one sidecar redeployment and one controlled diagnostic run but does not bias the result by changing runtime behavior.

### 2. Apply a conservative per-sandbox concurrency cap now — deferred

Limiting uncommitted work would probably reduce p95 latency, but it could hide whether the actual constraint is Y-Sweet, the sidecar, or the upstream provider. It also risks reducing aggregate throughput before the sustainable limit is known.

### 3. Redesign batching/windows immediately — deferred

Larger frame windows, event batching, or per-stream windows could improve transport throughput. They are protocol changes with a larger correctness and memory surface, and the current trace proves that some streams already achieve high event rates. A redesign will be specified only if the diagnostic identifies transport congestion.

## Architecture

Diagnostics are opt-in through `GP_V2_DIAGNOSTICS=1` and omitted entirely when disabled. The diagnostic path has four focused components:

1. A fixed-memory sidecar resource probe samples the Linux sandbox and Deno process.
2. Host and sidecar transport observers record bounded counters, queue pressure, ACK progress, and local stage durations.
3. The existing observability logger records allowlisted diagnostic fields and aggregate WebSocket delivery pressure.
4. A controlled probe runs identical request bodies at staged concurrency and produces one machine-readable comparison report.

The sidecar piggybacks its latest fixed-shape snapshot on Awareness envelopes that are already being published. Diagnostics never trigger an Awareness publication by themselves. The maximum additional serialized envelope size is 2 KiB. Per-request timings are added only to existing request ACK, first response event, and terminal frames; no extra diagnostic frames are created.

## Sidecar Resource Probe

The sidecar samples once per second into a single replace-in-place snapshot. It records only finite, non-negative numeric or boolean values:

- process RSS, heap used, heap total, and external memory;
- sandbox total and free memory when the Deno runtime exposes them;
- process CPU utilization normalized to one logical core; if the runtime cannot expose it, normalized Linux load is used, otherwise the field is `null`;
- event-loop delay p50, p95, and maximum from a fixed-size drift histogram;
- sidecar engine active requests, queued admissions, credits, and reserved bytes;
- outbound reliable-window frames and encoded bytes;
- scheduler queued frames, encoded bytes, and logical streams;
- retransmission attempt, cumulative retransmissions, cumulative publications, and cumulative published bytes;
- time since the last cumulative ACK advance;
- maximum window, scheduler, event-loop, and memory values since the previous sample.

Unavailable operating-system metrics are represented as `null`; their absence never fails bootstrap or a request. The sampler uses at most 1 MiB of sidecar memory and less than 1% CPU at one-second resolution.

## Request Boundary Timings

Cross-machine wall or monotonic clocks are never subtracted. The sidecar reports durations measured entirely on its own clock, and the host reports durations measured entirely on its own clock.

The existing committed request ACK gains an optional diagnostic timing object containing:

- first request frame to commit-frame receipt;
- commit-frame receipt to completed assembly;
- durable claim synchronization;
- sidecar admission wait;
- total sidecar commit processing through ACK enqueue.

The first response event gains optional fields, present only on part zero of event zero, containing:

- upstream fetch start to first upstream SSE event;
- first upstream event to sidecar scheduler enqueue;
- sidecar scheduler wait before the event enters the reliable window.

The existing terminal frame gains an optional aggregate containing:

- upstream execution duration;
- upstream event count and bytes already present in terminal proof;
- peak sidecar scheduler frames/bytes for this request;
- peak ACK-progress age observed during this request.

The host records:

- request serialization/compression duration;
- scheduler wait before the first request frame enters the window;
- first publication to committed ACK;
- committed ACK to first response frame;
- response-frame assembly to handle delivery;
- total request and terminal duration.

The difference between host commit duration and reported sidecar processing durations is classified as transport/window delay, not as precise one-way network latency.

## Transport Snapshot

Both directions expose the same bounded transport snapshot:

- window frame count and encoded bytes;
- scheduler frame count, encoded bytes, and stream count;
- oldest queued-frame age;
- last ACK-progress age;
- current and peak retransmission attempt;
- cumulative publication and retransmission counters;
- cumulative encoded bytes published;
- locally observed round-trip EWMA;
- admitted-but-uncommitted request count.

Snapshots are cumulative or replace-in-place. No per-frame history, payload, or unbounded label cardinality is retained.

## WebSocket Delivery Probe

The host maintains one aggregate, fixed-memory WebSocket sample rather than logging connection or stream identifiers. It records:

- active WebSocket sessions and responses;
- send-queue depth;
- socket buffered bytes and peak buffered bytes;
- enqueue-to-send p50/p95/maximum;
- `socket.send` callback p50/p95/maximum;
- count of responses delayed by the configured backpressure ceiling.

This distinguishes a slow Codex consumer from a response that reached the host slowly. The sample is emitted with the existing fleet sample and contains no client-identifying value.

## Protocol and Compatibility

Diagnostic objects are optional and versioned independently from terminal proof. Core request, response, ACK, replay, and terminal fields remain unchanged. A missing or malformed diagnostic object is discarded and counted; it never invalidates an otherwise valid transport envelope or terminal proof.

The sidecar and host are deployed together under a new content-addressed build hash. A host with diagnostics disabled accepts valid core frames without diagnostic objects. Old sidecars are replaced by normal V2 bootstrap reconciliation; no daemon change is required.

## Observability Events

Existing events gain explicit allowlisted scalar fields rather than arbitrary nested errors or objects:

- `transport.committed` receives preparation, scheduler, transport-residual, and sidecar commit-stage durations;
- `response.first_event` receives upstream-first-event, sidecar-scheduler, and host-delivery durations;
- `request.completed` and `request.failed` receive aggregate peak transport pressure;
- `fleet.sample` receives fixed-shape host, WebSocket, and per-sidecar resource/transport snapshots.

Diagnostic parsing or sampling errors increment bounded counters and use sanitized error kinds. Raw exception objects are never serialized.

## Controlled Diagnostic Probe

Add an import-safe `diagnose:v2-concurrency` command. It opens independent WebSocket clients against the running router and executes four stages: 1, 4, 8, and 16 concurrent responses.

Each stage uses the same immutable request body, model, reasoning configuration, service tier, input bytes, and requested output shape. The body contains deterministic non-secret padding so request size does not grow between stages. Every stage waits for fleet idle before beginning and runs three waves. Actual event and byte counts are recorded because upstream output chunking can vary.

The report contains, per stage and per sandbox:

- commit, first-event, total, and maximum inter-event-gap p50/p95/max;
- upstream-first-event and sidecar processing p50/p95/max;
- transport residual and ACK-progress age p50/p95/max;
- event and byte throughput;
- sidecar CPU, memory, event-loop, scheduler, and window peaks;
- WebSocket send and buffering peaks;
- failures, cancellations, quarantines, and terminal-proof mismatches.

The command never creates or deletes projects and refuses port 8787.

## Decision Rules

The diagnostic classifies the result using these ordered rules:

1. **Sandbox resource pressure:** sidecar CPU exceeds 85% of one logical core, event-loop p95 exceeds 20 ms, free sandbox memory falls below 10%, or process RSS reaches 80% of its configured budget while transport-residual p95 is at least twice the 1-client baseline.
2. **Awareness transport pressure:** scheduler/window occupancy or ACK-progress-age p95 reaches twice the 1-client baseline while sandbox CPU, event-loop, and memory stay below their thresholds.
3. **Upstream provider pressure:** upstream fetch-to-first-event or upstream-execution p95 reaches twice the 1-client baseline while sidecar processing, transport residual, and WebSocket delivery each remain below 125% of their 1-client baselines.
4. **Client backpressure:** host response delivery is timely but WebSocket enqueue/send latency exceeds 50 ms or buffered bytes reach the configured ceiling.
5. **Mixed pressure:** more than one boundary crosses its threshold; the report shows each contribution rather than selecting one misleading label.

The resulting optimization is a separate design:

- sandbox pressure leads to lower per-sandbox admission plus more sandboxes;
- transport pressure leads to backlog-aware routing/admission, batching, or revised frame windows;
- upstream pressure leads to provider/model/account-concurrency investigation;
- client pressure leads to WebSocket batching/backpressure changes.

## Safety and Observer-Effect Budgets

- Diagnostics are disabled by default.
- No diagnostic operation may block request, response, ACK, terminal proof, or shutdown paths.
- Sidecar sampling uses fixed memory and never reads request or response content.
- Additional envelope data is capped at 2 KiB.
- No diagnostic-only Awareness publication is allowed.
- Logging remains bounded by the existing event/byte queue.
- Deterministic benchmarks must show less than 2% throughput regression and less than 2% p95 latency regression with diagnostics enabled.
- Telemetry failure degrades to missing metrics, never request failure.

## Testing

Tests must prove:

- diagnostics disabled produces byte-for-byte core protocol behavior;
- diagnostic objects are bounded, finite, versioned, and reject forbidden fields;
- malformed diagnostics are ignored without weakening core frame validation;
- sidecar resource sampling is fixed-memory and skips overlapping samples;
- event-loop and CPU deltas are calculated from deterministic probes;
- local stage durations never combine clocks from different machines;
- request ACK, first event, and terminal diagnostics preserve ordering and terminal proof;
- transport counters track publication, retransmission, queue, window, and ACK progress accurately;
- WebSocket sampling is aggregate and contains no client or stream identity;
- the controlled probe is import-safe, uses identical bodies at every stage, waits for idle, and refuses protected ports;
- diagnostics-on deterministic throughput and latency remain within the 2% observer-effect budgets;
- all HTTP, SSE, WebSocket, image, continuation, fault, replay, concurrency, and sidecar-bundle suites remain green.

## Rollout

1. Implement and verify diagnostics without changing transport behavior.
2. Commit and push the instrumented host and sidecar bundle.
3. The operator manually restarts GhostProv once; normal bootstrap redeploys all four sidecars.
4. Run the controlled 1/4/8/16 probe with `GP_V2_DIAGNOSTICS=1`.
5. Compare the classified result with the existing 13-minute real-Codex trace.
6. Write and approve a separate optimization design for the measured bottleneck.
7. Disable diagnostics after the optimization is validated unless an operator explicitly keeps them enabled.
