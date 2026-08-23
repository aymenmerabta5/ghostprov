# GhostProv V2 Long-Run Stability Design

## Objective

Prevent a healthy eight-sandbox fleet from exhausting all routable capacity after prolonged client churn, and ensure genuine zero-capacity periods return an explicit OpenAI-compatible error before Codex's WebSocket idle timeout.

## Proven Failure Chain

The complete process JSONL log proves one deterministic chain:

1. All measured sidecar RTTs remained healthy (roughly 220–850 ms), but adaptive limits repeatedly fell to 1 because elapsed idle time since the preceding ACK was classified as an active ACK stall.
2. Thirteen client-initiated cancellations after commitment were distributed across all eight projects.
3. Each cancellation permanently retained one host quarantine even after the sidecar stopped execution and released its local admission lease.
4. With every adaptive limit at 1 and every runtime holding at least one cancellation quarantine, usable capacity became zero.
5. Capacity remained zero for 253 fleet samples, from 09:56:16 through 10:17:17. The router held 10–12 queued requests with zero active work.
6. WebSocket connections remained open but emitted no response events, so Codex reported `idle timeout waiting for websocket`.
7. Forty-eight queued aborts and six pre-admission capacity-change errors followed; they were consequences, not causes.

Large `ack_progress_age_ms` values occurred while reliable windows were empty. They describe idle time, not a stuck flight.

## Selected Fixes

### 1. Safe adaptive capacity floor

The configured initial capacity is also the minimum capacity. With defaults, adaptation operates only from 8 through 16.

Capacity reduction occurs only when:

- measured reliable-flight RTT is at least 5 seconds; or
- `TransportCongestion` reports pressure while an outbound window is non-empty.

Idle time between flights is never a failure signal. Remove `ackStalled` from the adaptive sample contract and from the Deno peer. Healthy streak growth remains unchanged.

This preserves headroom for quarantines and guarantees that an idle, healthy project cannot decay from 8 to 1.

### 2. Definitive cancellation completion

Add reliable sidecar frame:

```text
request_cancelled { requestId }
```

When the sidecar receives `request_cancel` for an active execution, it records cancellation intent and aborts the upstream reader. It emits `request_cancelled` only from the execution `finally` path, after:

- request body zeroing;
- admission lease release;
- removal from the active map.

If the upstream produces a proven terminal despite cancellation, terminal proof remains authoritative and cancellation completion is idempotent.

The host keeps its conservative cancellation quarantine until either a proven terminal or matching `request_cancelled` arrives. On cancellation completion it removes the cancelled state and decrements exactly one quarantine. Non-cancellation ambiguous errors remain quarantined.

If no active execution exists when a cancel frame arrives, the sidecar may acknowledge immediately because no local lease remains.

### 3. Bounded router queue deadline

Add `GP_ROUTER_QUEUE_TIMEOUT_MS`, default 30,000 ms, bounded from 1,000 through 300,000 ms.

Each queued relay job retains one absolute queue deadline across its one safe pre-commit retry. The timer is active only while the job is in the global router queue, not while dispatched to a runtime.

Expiry returns `RouterCapacityTimeoutError`:

- HTTP status 503;
- code `router_capacity_timeout`;
- message `No sandbox capacity became available before the routing deadline`.

Expiry removes the job, abort listener, queue timer, and memory lease exactly once; emits one sanitized `request.failed`; and schedules the next drain. Codex receives a concrete error rather than an application-level idle timeout.

## Rejected Alternatives

- WebSocket keepalive/progress events would conceal zero capacity without restoring it and could violate the Responses event contract.
- Quarantine TTLs could release genuine ambiguous executions without proof.
- Increasing sandboxes would postpone but not prevent exhaustion because every sandbox independently converged to the same invalid state.

## Safety Invariants

- Adaptive capacity never exceeds configured maximum or drops below configured initial capacity.
- Only client-cancelled states can be released by `request_cancelled`.
- Genuine ambiguous failures, instance replacement, terminal mismatch, and protocol corruption remain quarantined.
- A cancellation acknowledgement is sent only after the sidecar's local execution lease is gone.
- Queue expiry never affects a job already dispatched or committed.
- Timers are cleared on selection, abort, shutdown, success, failure, and retry transitions.
- The live server on port 18787 is not restarted during source work.

## Tests

Tests must prove:

- hours of idle time followed by a 200 ms ACK cannot reduce capacity;
- severe RTT halves capacity only down to the initial floor;
- cancellation completion is ordered after lease release;
- host cancellation quarantine returns to its previous value on matching completion;
- unrelated or duplicate completion frames cannot release quarantine;
- genuine ambiguous response errors remain quarantined;
- a zero-capacity queued job returns typed 503 before the configured deadline;
- queue deadline persists across one safe retry;
- dispatched jobs are not expired by their former queue timer;
- abort/shutdown races release timers and memory exactly once;
- an eight-sandbox exhaustion simulation cannot reach zero capacity from cancellations and idle periods;
- full protocol, replay, 512-request concurrency, WebSocket, image, and large-payload suites remain green.

## Acceptance

- Minimum aggregate idle capacity with eight healthy default sandboxes: 64 before quarantine subtraction.
- Thirteen completed client cancellations leave zero cancellation quarantines.
- Zero-capacity simulation returns `router_capacity_timeout` within the configured bound.
- No new protocol failures, duplicate terminals, reroutes after commitment, or resource leaks.
