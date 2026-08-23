# GhostProv V2 Transport Throughput Design

## Objective

Eliminate multi-agent token stalls while preserving exact-once execution, ordered streaming, terminal proof, bounded memory, and fair sharing across requests.

## Proven Bottleneck

The diagnostic run rules out host, sandbox, upstream-first-event, and WebSocket pressure:

- host CPU max 15.94%, 13.08 GiB minimum free RAM;
- sidecar RSS max 148.6 MiB and event-loop p95 max 2.19 ms;
- upstream first event p95 810 ms;
- WebSocket queue/callback p95 1.58/0.80 ms with zero buffering;
- sidecar commit processing p95 17 ms;
- transport residual p95 18.24 s;
- response scheduler p95 516 frames, max 563 frames;
- oldest queued frame p95 99 s, max 118.7 s;
- reliable window remained at its 32-frame ceiling while using far less than its 512 KiB byte ceiling.

Frames average roughly 1 KiB. The transport is count-bound rather than byte-, CPU-, or memory-bound.

## Selected Approach

Use three complementary mechanisms:

1. batch consecutive small logical SSE events into bounded physical frames;
2. backpressure upstream response reads when the sidecar transport queue reaches a bounded high watermark;
3. advertise zero new credits and route away from a sandbox while response transport is congested.

Increasing the frame count alone is deferred. Adding projects is complementary scaling, not a fix for per-sandbox collapse.

## Response Batch Protocol

Add a `response_batch` sidecar frame containing one request ID, a first event index, and an ordered array of complete logical events. Each item contains only the event name and base64url-encoded data already transported by `response_event`.

The batcher is per request and flushes when any condition occurs:

- the next item would make serialized reliable-frame size exceed 14 KiB;
- the batch reaches 32 logical events;
- 5 ms elapsed since the first queued item;
- a large/segmented event, terminal, error, cancellation, or drain arrives.

Events too large for a batch retain the existing segmented `response_event` format. The host expands a batch into the same ordered SSE frames before transcript validation and client delivery. Diagnostics and batching metadata never enter canonical SSE transcript bytes.

Terminal/error frames may not overtake an unflushed batch from the same request. Flush completes first, then the existing priority scheduler receives the terminal.

## Bounded Sidecar Backpressure

Make the sidecar frame sink asynchronous. `DirectResponsesClient.onEvent` already awaits its callback; the engine will await batch enqueue when transport pressure is above the high watermark. This pauses the upstream response reader without dropping data or blocking unrelated requests.

Watermarks:

- high: 256 scheduled frames or 256 KiB scheduled bytes;
- low: 64 frames and 64 KiB;
- hard bound: 1,024 frames or 1 MiB, after which producers remain paused rather than allocating further;
- wake producers fairly when both values return below the low watermark.

Control ACKs, cancellation, drain, and credential rotation remain independently enqueueable. Terminal frames wait only for their own request's pending batch.

## Congestion-Aware Admission and Routing

Define a sidecar as congested when any condition holds:

- scheduled frames >= 128;
- scheduled bytes >= 256 KiB;
- oldest queued frame >= 2 seconds;
- ACK-progress age >= 2 seconds while an outbound window is non-empty.

Clear congestion only after all conditions remain below 32 frames, 64 KiB, 500 ms queue age, and 500 ms ACK age for two consecutive envelopes.

While congested, the sidecar advertises zero credits for new requests. Existing committed requests continue and retain fair scheduler weights. The host also caps admitted-but-uncommitted requests at four per sandbox, so a simultaneous burst cannot fill one request-direction window before routing observes updated credits.

`V2SandboxRuntime.available` uses the resulting credits without changing physical upstream capacity. Router scoring adds an explicit transport-pressure penalty and excludes a currently congested runtime when another healthy runtime is available. No committed request is rerouted.

## Limits and Memory

- Keep the 32-frame and 512 KiB reliable-window limits unchanged for the first rollout.
- Keep the 16 KiB physical-frame ceiling.
- Keep per-request batch state below 16 KiB and global scheduled state below 1 MiB.
- Keep scheduler fairness per logical request.
- Preserve request/response byte ceilings, retransmission semantics, terminal quarantine, and replay.

## Failure Handling

- Timer flush failure fails only that committed request using existing ambiguity rules.
- Malformed batches are protocol failures and quarantine the affected committed request.
- Duplicate/reordered reliable batches remain idempotent through sequence cursors and event indexes.
- Disconnect/restart replays proven terminal spools through the same batch decoder without another upstream call.
- Cancellation wakes blocked producers and removes only their stream state.

## Observability

Existing diagnostics add:

- logical events per physical frame;
- batches emitted and average/max batch bytes;
- producer-backpressure duration and waiter count;
- congestion transitions and duration;
- advertised credit suppression;
- batch expansion/validation failures.

No content or identity fields are logged.

## Testing

Tests must prove:

- exact ordered expansion for batches split at every size boundary;
- 14 KiB physical target and 16 KiB hard ceiling;
- 5 ms, 32-event, terminal, error, cancellation, and drain flushing;
- large events keep existing segmentation;
- terminal proof is byte-for-byte identical with batching on/off;
- dropped, duplicate, and reordered Awareness updates remain exactly once;
- scheduler memory never exceeds 1 MiB and producers wake fairly;
- congestion hysteresis suppresses/restores credits without oscillation;
- four uncommitted requests per sandbox is enforced while committed upstream concurrency remains 128;
- 16-stream deterministic throughput improves at least 4x with no terminal loss;
- HTTP, SSE, WebSocket continuation, images, replay, faults, large payloads, and 512-request suites remain green.

## Live Acceptance

Using the controlled 1/4/8/16 probe and the real Codex workload:

- commit p95 <= 5 seconds;
- first-visible-event p95 <= 5 seconds excluding measured upstream time;
- oldest sidecar queue age p95 <= 5 seconds and max <= 10 seconds;
- scheduler p95 <= 128 frames;
- zero unproven terminals or protocol failures;
- WebSocket buffering remains zero;
- host and sidecar resource thresholds remain healthy;
- aggregate event throughput is at least 4x the pre-batching deterministic baseline.

If batching meets these targets, the frame window remains 32. A later increase to 128 frames is considered only if queue pressure remains transport-bound after batching and memory/publication budgets still pass.

## Rollout

Implement behind `GP_V2_BATCHING=1`, default off until deterministic verification passes. Push the verified build, enable batching locally, manually restart once, rerun the controlled diagnostic, then compare against the captured baseline. After acceptance, make batching the default while retaining a temporary zero opt-out.
