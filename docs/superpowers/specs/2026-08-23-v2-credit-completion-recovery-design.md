# V2 Credit and Completion Recovery Design

## Context

The one-hour eight-sandbox trace completed 2,172 requests without infrastructure failures or router queueing, but it exposed two independent causes of silent fleet shrinkage:

1. Five idle runtimes advertised zero credits for long periods even though their sidecars had zero active work, zero scheduler backlog, an adaptive limit of at least eight, and adequate memory. Three entered this state at the staggered forty-five-minute credential-rotation boundary. One runtime remained at zero for about thirty minutes and another for about thirty-eight minutes.
2. Two model generations lasting 228 seconds and 554 seconds raised one runtime's lifetime completion EWMA to 154 seconds. The router then stopped selecting that otherwise healthy runtime. In the final thirty minutes, nearly all work was concentrated on only three of eight projects.

The zero-credit mechanism is deterministic in source. Sidecar congestion compares a newly created reliable window with the timestamp of the previous request's ACK. Any first frame after more than two idle seconds is immediately classified as stalled before it has even been published. When its window drains, the congestion state requires a second healthy observation that may never arrive on an idle runtime, leaving credits latched at zero.

## Goals

- Measure ACK progress age only while reliable frames are genuinely outstanding.
- Ensure a fully drained scheduler/window cannot retain a congested zero-credit state.
- Preserve congestion backpressure for real scheduler, queue-age, byte, and active-window ACK stalls.
- Prevent long or complex model generations from permanently poisoning per-runtime routing estimates.
- Preserve the existing `completionEwmaMs` snapshot field and router-policy API.
- Let idle runtimes recover automatically without traffic, restart, or credential rotation.
- Keep the fleet useful under long-running workloads and cancellation churn before scaling toward one hundred parallel agents.

## Non-goals

- Changing reliable flight, frame, batching, or retransmission limits.
- Hiding genuine ambiguous/protocol failures from the runtime-health score.
- Eliminating request-size-dependent Y-Sweet transport latency.
- Changing project count or provisioning policy.
- Restarting or replacing the live router on port 18787 during source work.

## Active-Flight ACK Age

`ReliableDeliveryTiming` will own the timestamp of the last meaningful delivery-progress boundary:

- no outstanding reliable frames: progress age is exactly zero, regardless of wall-clock idle time;
- first publication into an empty window: progress age starts at that first-send time;
- cumulative ACK with frames still outstanding: progress age restarts at the ACK time;
- cumulative ACK that empties the window: progress age returns to zero;
- duplicate publication: does not move progress age;
- retransmission: does not pretend ACK progress occurred;
- epoch reset: clears progress age.

Both host and sidecar diagnostics will use this source. Sidecar congestion will therefore never treat time between independent requests as an ACK stall.

## Drained Congestion Recovery

`TransportCongestion` keeps its existing high thresholds and two-sample hysteresis while work remains in flight. It gains one safe terminal rule: if scheduler frames and bytes are zero and the reliable window is empty, congestion clears immediately.

There is no traffic to protect in a fully drained state, so retaining zero credits provides no backpressure benefit. Clearing immediately also guarantees that the ACK which empties a stalled flight publishes restored credits without waiting for another request or credential event.

## Bounded Completion Estimate

Full model duration remains useful as a coarse load-duration signal, but it is not a permanent sandbox-health property. A focused `DecayingRuntimeCompletionEstimate` will replace the unbounded lifetime EWMA while keeping the public field name.

Defaults:

- neutral baseline: 10,000 ms;
- observation weight: 0.20;
- maximum accepted sample: 60,000 ms;
- minimum accepted sample: 1 ms;
- idle half-life toward baseline: 60,000 ms.

On observation, elapsed-time decay is applied first, the duration is clamped, and the weighted update is applied. On read, the current value decays toward 10 seconds even when no new work is routed to that runtime.

Examples from a neutral runtime:

- one 554-second completion becomes 20 seconds, not 111 seconds;
- two consecutive extreme completions become 28 seconds;
- after one idle minute, 28 seconds becomes 19 seconds;
- after a second idle minute, it becomes 14.5 seconds.

This preserves a temporary preference for faster observed runtimes while guaranteeing bounded, automatic recovery. ACK RTT, active/reserved load, memory pressure, quarantine, and the separate decaying failure score continue to influence routing.

## Error Handling and Compatibility

- Delivery timestamps must be finite, non-negative, and monotonic within an epoch.
- Completion observations must be finite and non-negative; invalid configuration or a backward clock throws immediately.
- Existing routing snapshots, JSON observability fields, and public APIs do not change shape.
- No dependency is added.
- The sidecar bundle hash changes because its peer uses the corrected delivery clock and congestion recovery; the next operator-managed router restart will redeploy it automatically.

## Tests

### Delivery and congestion

- An arbitrary idle interval with an empty window reports zero ACK-progress age.
- A newly published frame starts age at its first send, not the preceding request's ACK.
- Duplicate publication and retransmission do not fabricate progress.
- ACK progress resets age, empty ACK clears it, and epoch reset clears it.
- A congested but fully drained transport clears on the first healthy observation.
- Partially active low-water transport retains two-sample hysteresis.
- Deno peer tests continue proving RTT, retransmission, split-lane, pacing, and adaptive-capacity behavior.

### Completion recovery

- Initial value is exactly 10 seconds.
- A 554-second sample is capped and produces 20 seconds.
- Repeated extreme samples remain bounded.
- The value halves its deviation from baseline every idle minute.
- Normal completions move the estimate normally.
- Invalid tuning, samples, and backward clocks are rejected.
- `V2SandboxRuntime` exposes the estimate, uses successful terminal duration only, and recovers while idle.

## Acceptance

- A first reliable frame after hours of idle has ACK-progress age zero before publication and cannot force credits to zero.
- An empty scheduler/window never remains congested.
- Adaptive capacity retains its safe floor and genuine in-flight ACK stalls still trigger congestion.
- A 554-second completion cannot produce a runtime estimate above 20 seconds from baseline.
- Any completion estimate returns halfway toward 10 seconds after sixty idle seconds.
- Existing cancellation, quarantine, 512-request concurrency, dynamic 32-sandbox, WebSocket, Images, and large-payload tests remain green.
- Typecheck, all isolated tests, daemon tests, audit, and protected HTTP/WebSocket smoke pass without changing the live port-18787 PID.
