# GhostProv V2 Split-Lane Flight Transport Design

## Objective

Eliminate the remaining multi-agent tail stalls by preventing ACK and capacity updates from republishing large cumulative request/response windows. Preserve exact-once upstream execution, ordered SSE delivery, terminal proof, replay, bounded memory, credential rotation, and horizontal fleet scaling.

The public OpenAI-compatible HTTP and WebSocket APIs do not change. The live router on port 18787 is not restarted during implementation.

## Proven Remaining Bottleneck

The first pacing rollout materially improved the six-minute 16-agent run:

- failures fell from 15 to zero;
- completed requests increased from 67 to 102 in a comparable interval;
- total latency p50/p95 fell from 35.2/137.6 seconds to 12.4/73.8 seconds;
- commit p50/p95 fell from 7.1/85.1 seconds to 1.34/46.7 seconds;
- sidecar scheduler p95/max fell from 340/410 frames to 22/59;
- sidecar retransmission max fell from 380 to 7;
- logical events averaged 6.6 per batch and 5.6 per sidecar publication.

The run also isolates the residual cause:

- sidecar window bytes reached 523,251 bytes at p95 and 524,030 bytes max, effectively the 512 KiB ceiling;
- sidecar RTT reached 42.0 seconds p95 and 57.9 seconds max;
- one sandbox ended with 54.5 seconds sidecar RTT and eight queued requests while other sandboxes were idle;
- host and sidecars serialized approximately 740 MiB of Awareness state in 370 seconds;
- 2,264 of 2,409 host publications were urgent ACK/control publications, and each still carried the host's cumulative outbound window;
- CPU, RAM, event loops, upstream-first latency, and client WebSocket buffering remained healthy.

The transport changed from count-bound to byte-bound. More pacing or a larger window cannot fix this: the Awareness state model still couples tiny reverse-direction ACKs to large forward-direction data.

## Considered Approaches

### 1. Split ephemeral lanes plus ACK-clocked data flights (selected)

Use two independent Awareness client states on the same Y-Sweet document and credential:

- a control lane containing ACK, credits, lifecycle state, and diagnostics with no reliable frames;
- a data lane containing reliable request/response frames.

Freeze each data flight after publication until its high sequence is cumulatively acknowledged. This removes ACK amplification without persisting transport data or changing the external API.

### 2. Smaller cumulative windows on the existing single lane

A 64–128 KiB ceiling would reduce individual state size but would still resend the window on every ACK and require more round trips. It treats the byte amplification symptom rather than the coupling that causes it.

### 3. Persistent Yjs mailbox/log

Incremental Yjs document updates would avoid full-state Awareness encoding, but they introduce durable project state, tombstone/history growth, compaction, and migration requirements. GhostProv transport traffic must remain ephemeral, so this is not selected.

## Split Awareness Channel

Extend the channel contract with `AwarenessLane = "data" | "control"`:

- `publish(envelope, lane)` writes only the selected lane;
- `clear(lane?)` clears one lane or both;
- subscriptions merge remote updates from both lanes;
- credential rotation updates both lane credential sources;
- closing destroys both lane providers and Y.Docs.

The host composes two existing `YSweetAwarenessChannel` instances:

- field `ghostprovV2Data`, local peer suffix `:data`;
- field `ghostprovV2Control`, local peer suffix `:control`.

Each child owns its own empty Y.Doc and WebSocket provider, so an Awareness update serializes only that child's local state. The Deno sidecar mirrors the same two-provider layout. Both use the existing Y-Sweet token, binding, reconnect behavior, and cleanup.

`AwarenessEnvelope` gains an optional lane field for bootstrap/test compatibility. The split wrapper always writes it. The data and control lanes maintain independent revision counters; receivers reject stale revisions only within the same lane.

## Lane Semantics

Control envelopes are authoritative for:

- cumulative ACK cursor;
- advertised credits;
- active/queued/draining state;
- congestion transitions;
- diagnostics and resource telemetry.

Control envelopes must contain zero reliable frames. Their expected serialized size remains a few KiB even while a data flight is full.

Data envelopes are authoritative for reliable frames. Their ACK and capacity fields may be accepted only as monotonic ACK hints; credits and diagnostics never roll back from a data envelope. This optional piggyback preserves progress if the control update is delayed without coupling future control publications to data.

Bootstrap readiness is published on control. Bootstrap ping, drain, and their reliable replies use data. Existing bootstrap consumers subscribe to the merged split channel and therefore retain their current behavior.

## ACK-Clocked Data Flights

Add `AckClockedFlight` as a transport-independent protocol component.

Limits:

- target flight bytes: 128 KiB;
- target flight frames: 32;
- hard reliable window remains 512 KiB and 128 frames;
- physical frame remains 16 KiB.

Flow:

1. When no flight is active, the fair scheduler fills one outbound flight up to both target limits.
2. Publishing the data lane records the flight epoch and high sequence.
3. New frames remain in the scheduler while the flight is active; upstream backpressure applies at the existing watermarks.
4. Control ACK publications continue independently with zero frames.
5. A cumulative ACK at or beyond the flight high sequence completes the flight, removes acknowledged frames, and permits the next fair flight.
6. Partial ACK progress updates RTT/RTO but does not extend the flight with new data.
7. An RTO retransmits only the unchanged active flight.

Terminal and request-control reliable frames use scheduler priority but never overtake an already numbered flight. They become the first frames selected for the next flight. The host already accepts an early terminal only after the complete transcript, preserving defense in depth.

When an active flight is fully acknowledged and no next flight exists, the sender clears its data-lane state so a reconnect cannot replay a stale completed flight. Duplicate/reordered frames remain idempotent through epoch/sequence cursors.

## Adaptive Per-Sandbox Capacity

Physical upstream capacity remains 128, but transport admission becomes an independently bounded control loop:

- initial transport capacity: 8 active requests;
- maximum transport capacity: 16 active requests;
- minimum: 1;
- RTT at or above 5 seconds, congestion, or an active-window ACK stall halves the limit;
- eight consecutive healthy flight ACKs at or below 750 ms increase the limit by one;
- intermediate samples retain the current limit.

The sidecar advertises `min(engine credits, adaptive limit - active)` on the control lane. This prevents a slow sandbox from continuously accepting new committed work while healthy projects are idle. The physical engine limit and memory admission remain additional hard ceilings.

Configuration adds:

- `GP_V2_INITIAL_ACTIVE_PER_SANDBOX`, default 8;
- `GP_V2_MAX_ACTIVE_PER_SANDBOX`, default 16 and never above physical capacity.

Fleet sizing uses the maximum transport-active value rather than the physical engine limit when no explicit `GP_TARGET_SANDBOXES` is supplied. Explicit project and sandbox settings remain authoritative.

## Safe Pre-Admission Rerouting

The host retains at most four admitted-but-uncommitted requests. A request that remains unadmitted and therefore has never entered the reliable scheduler is rejected after one second with a typed pre-commit capacity-change error. The router's existing one-time pre-commit retry can immediately choose another project.

The timeout never applies after admission or publication. GhostProv therefore never reroutes a request that might have been durably claimed or started upstream.

## Horizontal Scaling

The transport has no four-project limit. For this installation, eight authenticated projects provide enough isolation for the current 16-agent workload while leaving room for bursts. Local ignored configuration will be updated to permit eight managed projects and automatic creation on the next operator start; the running four-project process remains untouched.

More projects complement, but do not replace, the split-lane fix. Fleet growth is useful only after per-project ACK amplification is removed.

## Observability

Retain existing counters and add content-free lane/flight metrics:

- data/control publication totals;
- data/control serialized byte totals;
- active flight high sequence, frames, and bytes;
- flight completions and retransmissions;
- adaptive capacity and capacity reductions/increases;
- unadmitted timeout count;
- logical events per data publication.

Diagnostics remain exact, bounded below 2 KiB, and contain no payload, request identity, client identity, token, URL, or credential.

## Failure Semantics

- Failure to connect either lane fails transport startup; the other lane is closed.
- Loss of a data publication is recovered by the existing measured RTO.
- Loss of a control ACK causes a bounded retransmission, not loss or duplicate upstream execution.
- Data-lane state never advances to a new flight before cumulative ACK of the current high sequence.
- Lane revisions are independent, so a newer control update cannot suppress an older-but-unconsumed data update.
- Sidecar replacement resets data/control revisions, flight state, reliable epochs, and timing state together.
- Close/drain clears both local states and cancels every lane, pacing, admission, and RTO timer.
- Terminal journal, proof, quarantine, and replay rules are unchanged.

## Testing

Tests must prove:

- control publication never contains or serializes the active data flight;
- publishing 100 control ACKs while a 128 KiB flight is active remains O(control bytes), not O(flight bytes);
- data/control revisions can arrive in either order without dropping frames or rolling back ACK/credits;
- one flight stays immutable until its high sequence is acknowledged;
- partial ACK, dropped data, dropped control, duplication, reordering, and RTO preserve exact-once ordering;
- fair scheduling resumes at every flight boundary;
- terminal, cancel, drain, and credential rotation remain bounded and correct;
- adaptive capacity halves/grows with hysteresis and never exceeds configured/physical limits;
- only never-admitted requests time out and reroute;
- 16 concurrent streams complete with bounded publications, queue age, and memory;
- 512 deterministic requests, HTTP, SSE, WebSocket continuation, image generation, replay, bootstrap, and large payloads remain green.

## Acceptance Targets

Deterministic verification must show at least a 75% reduction in serialized bytes versus the cumulative-envelope counterfactual for the same frames and ACK count.

The next live 1/4/8/16-agent run must achieve:

- zero protocol failures, ambiguous reroutes, or quarantines;
- commit p95 at or below 5 seconds;
- post-upstream first-visible p95 at or below 5 seconds;
- sidecar RTT p95 at or below 2 seconds and max at or below 5 seconds;
- oldest scheduler frame p95 at or below 5 seconds and max at or below 10 seconds;
- no sandbox retaining unadmitted work while another healthy sandbox is idle;
- at least 70% fewer serialized Awareness bytes than the 740 MiB pacing run;
- zero client WebSocket buffering and healthy resource thresholds.

Automated tests prove protocol behavior, not live Y-Sweet service latency. No live-improvement claim is made until a restarted candidate produces these measurements.

## Rollout

The split-lane schema, sidecar bundle, flight gating, adaptive capacity, and host transport deploy as one build/schema-hash change. Source and candidate verification use ephemeral ports and never stop the router on 18787. After push, the current PC is shut down only after repository state, tests, and live-port preservation are verified.
