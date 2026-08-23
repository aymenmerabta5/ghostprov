# GhostProv V2 Awareness Pacing Design

## Objective

Reduce multi-agent stream latency by stopping redundant full-state Awareness publications while preserving exact-once upstream execution, cumulative ACK safety, ordered SSE delivery, terminal proof, bounded memory, replay, and routing fairness.

This change optimizes the existing V2 Awareness data plane. It does not change the public HTTP/WebSocket API, provisioning model, journal format, or live server process.

## Evidence and Root Cause

The post-batching 16-agent run proves that host resources, sidecar resources, upstream startup, and client WebSocket delivery are not the bottleneck:

- host CPU remained below 20% with at least 11.76 GiB free RAM;
- sidecar RSS remained below 154 MiB and event-loop p95 below 12.7 ms;
- client WebSocket queue/callback p95 remained below 1.4/0.8 ms with zero buffered bytes;
- sidecar commit processing remained approximately 18 ms and upstream-first p95 approximately 1.08 seconds;
- the response scheduler still reached 410 frames and the oldest queued frame reached 164.9 seconds;
- sidecars retransmitted up to 380 times and published up to 173 MiB of cumulative Awareness state;
- 5,118 completed logical events caused 6,191 sidecar publications, proving that the 5 ms per-request batcher usually emitted singleton batches and did not reduce publication frequency.

The implementation explains the measurements. Every sidecar frame currently schedules a new full cumulative Awareness state in a microtask. The sidecar has no RTT estimator and starts retransmitting after a fixed 250 ms floor. Every publication also rearms that timer, even when it contains no newly sent reliable sequence. Under concurrency, this repeatedly serializes and sends the same unacknowledged window, consuming the constrained Awareness path while the upstream and both processes remain healthy.

## Considered Approaches

### 1. Paced cumulative Awareness windows (selected)

Use one publication cadence per peer across all logical streams, enlarge the count window, and derive retransmission timing from sequence ACK RTT. This directly removes the measured amplification without changing transport durability or provisioning.

### 2. Strict ACK-locked stop-and-wait envelopes

Publish one maximum window and send no new state until it is acknowledged. This minimizes duplicate bytes, but a lost or slow ACK blocks every stream for a full RTO and makes urgent control traffic share the stall. It is retained only as a possible future congestion mode, not the default.

### 3. Replace Awareness with a persistent Yjs data log

Yjs document updates would be incremental, but deletions leave history/tombstones and the project document would need compaction, migration, and crash-recovery semantics. That is a separate data-plane redesign with materially greater persistence risk. It is not required to address the proven bottleneck.

## Transport Constants

- Data publication interval: 25 ms.
- Response batch interval: 25 ms.
- Reliable window: 128 physical frames.
- Reliable byte ceiling: 512 KiB.
- Physical frame ceiling: 16 KiB.
- Response batch ceiling: 32 logical events and a 14 KiB serialization target.
- Retransmission floor/cap: 250 ms/30 seconds.
- RTT EWMA alpha: 0.2.

The count-window increase is bounded by the unchanged 512 KiB byte ceiling. A workload with large frames therefore gains no extra byte exposure, while small token frames can make four times more progress per cumulative ACK.

## Publication Pacer

Add a transport-independent `PublicationPacer` used once by each host session and once by each sidecar peer. It has three operations:

- `requestPaced()` marks the latest state dirty and publishes it no earlier than the shared 25 ms cadence;
- `requestUrgent()` cancels a pending cadence timer and coalesces all urgent work in the current microtask before publishing once;
- `flushNow(reason)` cancels pending work and publishes synchronously for initial readiness or an expired retransmission deadline.

A publication always contains the newest cumulative envelope, so coalescing never loses a reliable frame or ACK. One pacer serves every request on that sandbox; there are no per-request publication timers.

Sidecar response data (`response_event` and `response_batch`) and host request payload frames use the paced path. The following use the urgent path:

- cumulative ACKs after newly delivered inbound frames;
- request commit ACK/reject;
- response terminal/error and terminal ACK;
- cancellation, drain, credential rotation/ACK, ping/pong;
- congestion/credit suppression and host-ceiling changes.

Urgent publication may carry newly pumped data in the same envelope. This is correct and avoids a redundant paced publication. Publishing an unchanged window merely because its ACK advanced is removed; ACK progress only publishes when it unlocks new queued frames or changes control state.

The frame pump reports `none`, `paced`, or `urgent` according to the actual reliable frame kinds moved into the outbound window. This prevents a terminal/control frame that was waiting behind a full window from accidentally inheriting data cadence after ACK progress.

## RTT and Retransmission

Add a bounded `ReliableRttTracker` per outbound window. For each reliable sequence it stores only the first publication timestamp. Duplicate cumulative publications never replace that timestamp. A cumulative ACK removes all covered timestamps and samples the highest newly acknowledged tracked sequence. The EWMA is updated as:

`ewma = previous * 0.8 + sample * 0.2`

The tracker contains at most the 128 sequences allowed by the reliable window and resets with an epoch change.

The retransmission deadline is anchored to transport progress, not arbitrary publications:

- the first publication of a non-empty window starts attempt zero;
- cumulative ACK progress resets the attempt and deadline for any remaining window;
- an expired deadline publishes immediately, increments the attempt, and starts the backed-off deadline from that retransmission;
- unrelated urgent or paced publications do not move an existing retransmission deadline;
- an empty window cancels the deadline.

The existing bounded delay formula remains: `max(250 ms, 2 * RTT_EWMA)`, exponential backoff per attempt, up to 10% deterministic jitter, capped at 30 seconds. The sidecar and host both use measured sequence RTT; the host no longer derives RTT from the most recent arbitrary publication timestamp.

## Response Batching

Change the default per-request batch timer from 5 ms to 25 ms. Size, count, ordering, terminal flush, cancellation flush, and large-event segmentation semantics remain unchanged.

The batcher and publication pacer solve different layers:

- batching reduces physical reliable frames when one response emits adjacent small logical events;
- pacing coalesces physical frames from all responses into fewer cumulative Awareness publications.

Terminal/error emission still flushes that request's batch before enqueueing its terminal frame. If the outbound window is full, scheduler priority moves the terminal when space opens; the pump classifies it as urgent and publishes it with any preceding reliable data already in the window. The host continues to require the complete transcript before accepting an early terminal proof.

## Observability

Extend bounded transport diagnostics with cumulative, content-free counters:

- paced publications;
- urgent publications;
- logical response events;
- response batch frames;
- logical events carried by response batches.

Fleet logs expose both raw counters and two derived ratios:

- batched logical events per response batch;
- logical response events per total Awareness publication.

Existing total publications, bytes, retransmissions, peak attempt, window, scheduler, queue-age, ACK-age, and RTT fields remain. Diagnostic parsing stays exact and below the 2 KiB envelope ceiling. No request content, identity, token, URL, or credential is logged.

## Failure and Safety Semantics

- Pacing never removes data from an outbound window; only a valid cumulative ACK does.
- A lost paced publication is recovered by the adaptive RTO.
- Urgent control is coalesced for one microtask, not delayed by the 25 ms data cadence.
- Duplicate and reordered envelopes remain idempotent through epoch/sequence cursors.
- Byte, memory, upstream backpressure, congestion hysteresis, and four-uncommitted-request limits remain unchanged.
- Terminal proof and replay use the same canonical SSE transcript and remain independent of batching/publication boundaries.
- Closing either peer cancels cadence and retransmission timers without a late publication.

## Testing

Tests must prove:

- data requests arriving every 5 ms across different streams publish only on the shared cadence;
- multiple urgent requests in one turn publish once and preempt a pending data timer;
- an external retransmission flush cancels a pending redundant paced publish;
- sequence timestamps are first-send-only, bounded, epoch-safe, cumulatively removed, and update RTT EWMA;
- duplicate publications do not reset a retransmission deadline;
- both peers report measured RTT and use it in bounded exponential RTO;
- the reliable count window accepts 128 frames, rejects 129, and still rejects more than 512 KiB;
- the default response batch waits 25 ms, retains the 32-event/14 KiB bounds, and flushes before terminals/errors;
- sidecar diagnostics and fleet logs report publication and batching efficiency without unsafe fields;
- dropped, duplicated, reordered, replayed, cancelled, and large-payload traffic remains exact once and ordered;
- HTTP, SSE, WebSocket continuation, image generation, fleet routing, and 512-request concurrency suites remain green.

## Live Acceptance

After source verification, the user performs one manual restart and repeats the controlled 1/4/8/16-agent run. Success requires:

- no protocol failures, unproven terminals, or new quarantines;
- sidecar publications materially below logical events during concurrent streaming;
- retransmissions at least 80% below the post-batching run;
- published Awareness bytes at least 70% below the post-batching run;
- scheduler p95 at or below 128 frames;
- oldest queued frame p95 at or below 5 seconds and max at or below 10 seconds;
- post-upstream first-visible-event p95 at or below 5 seconds;
- client WebSocket buffering remains zero and resource thresholds remain healthy.

These are live acceptance targets, not claims that unit tests can establish. If the counters show that the Awareness service itself remains the limiting component after amplification is removed, the next design will evaluate strict ACK-locked envelopes or a different ephemeral data plane using the new RTT/publication evidence.

## Rollout

Keep `GP_V2_BATCHING=1` as the existing batching opt-in for this rollout. Publication pacing, the 128-frame schema, and adaptive RTT/RTO are protocol behavior and therefore deploy together through the existing build/schema hash bootstrap. Do not restart or stop the currently running server while editing or verifying source. After all automated verification and push complete, request one user-managed restart and compare a fresh run against the captured baseline.
