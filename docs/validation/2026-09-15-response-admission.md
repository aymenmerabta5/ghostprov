# R10 response-admission correction

This change follows the process-32352 log incident in which response queues grew under parallel work, admission credits fell to zero, and request deadlines expired. It does not constitute live swarm stability proof. This document records the historical R10 revision whose default base was two seconds; the current selected host/R10 default is sixty seconds, while frozen legacy releases remain unchanged.

## Reproduced failures

- A real R10 Awareness peer retained a response queue below the existing high-water limits. Its oldest queued response was older than two seconds, but a valid cumulative ACK had just advanced. The previous congestion policy still advertised zero credits. A deterministic test failed before the change and now retains positive admission through six further 1.2-second ACK rounds with output still queued.
- A request reflected execution risk near the end of its admission budget. The router's old handle-proof fence inherited the nearly expired deadline plus 25 ms, so a valid delayed handle was rejected as ambiguous. The late-handoff regression failed before correction and now accepts the handle without replay, retaining ownership until completion.
- A retry predictor considered another runtime admissible while the shared fleet pacer had no token. The regression showed unnecessary cancellation of the current copy. Prediction and selection now use the same pacing checks.

## Behavior

`ProgressAwareCongestion` separates byte/frame pressure from delivery stalls. The existing admission high-water marks (128 queued frames or 256 KiB) and recovery low-water marks (32 frames and 64 KiB, two observations) are unchanged. Queue age behind an active flight does not alone imply stalled delivery. A validated advancing ACK clears the delivery-stall gate, while actual buffer pressure remains gated.

At that revision, the no-progress threshold was the larger of the configured base and RTT plus four RTT variations, bounded to ten seconds for its then-default two-second base. A queue with no active flight was still checked for stalled scheduling. Watchdogs used the corresponding progress deadline rather than repeatedly waking on an old but draining queue.

R10 capacity recovery accepts clean RTT samples up to three seconds, while retaining the existing severe-latency threshold, healthy streak, configured ceiling, and two-strike stall handling. The protocol's 32-frame / 128-KiB response flight remains unchanged.

After execution risk, the committed-handle wait is separately bounded to at most sixty seconds. Smaller configured budgets retain the cancellation-proof slack. The request never becomes eligible for replay, and an actual caller cancellation still uses the short cancellation-proof fence. Admission and safe pre-execution retries retain the original absolute deadline.

The user-requested local `GP_ROUTER_QUEUE_TIMEOUT_MS` override was changed from `30000` to `60000` for the next launch, without printing or modifying other environment values. Startup telemetry reports the resolved admission, initial-attempt, and committed-handle budgets so the next run can verify the effective setting.

## Response-heavy validation

The performance harness now has an explicit R10 mode using the real R10 engine, peer, output backpressure, host transport, and encoded Yjs/Awareness updates. Its R10 clock and publication cadence are deterministic test seams. Large-input tests retain their existing default small-response fixture; the new tests explicitly use large streamed responses.

- Three workloads at modeled RTTs of 600, 1,200, and 2,500 ms each carry six 256-KiB requests and 256 response events per request with 1-KiB deltas and a complete final response. Output begins while later requests are still being admitted.
- A fourth workload carries four requests and 128 response events each, with dropped response publication and duplicated/reordered ACK updates.
- Across those workloads: 22 exact requests and terminals, 5,120 response events, no duplicate provider execution, preserved response window/queue bounds, and admission reopening while response generation is still active.
- The harness captures public completion before its separate integrity hashing/ACK cleanup. That verification work can advance the modeled clock and must not inflate the public latency measurement. Existing latency assertions were retained.

## Immutable release

New selected R10 identity:

- Build: `16847357a8ee0c930d4d15e433e3004216f0e8645fdcb94df76a39a4c251f01a`
- Schema: `51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f`
- Bundle SHA-256: `1ad4b7d3101cdc6686496b885bfeb593ddc0b7c1886a2fbca2633a1b37129131`
- Bundle bytes: `613098`

The previous `9d2aa8b7...` tuple and its existing artifact are retained as an exact archived R10 identity. The upgrade test verifies cleanup using that old binding before a new launch. No R1–R9 artifact, schema, or build identity was changed. No R11 or upstream WebSocket transport was introduced.

## Gates and limits

Focused peer/transport/congestion tests passed, as did the four response-heavy workloads and the late-handoff/pacing regressions. R8/R9/R10 deterministic bundle checks, pinned Deno checks, Deno upstream/journal tests, and both TypeScript checks passed. Final full-suite results are recorded after the final run below.

Initial full-suite failures included a missing README test entry, a short-budget handoff regression, and a harness completion-timestamp problem; all were corrected without reducing workloads or loosening existing latency assertions. One existing Windows helper readiness check failed on its first run and passed unchanged in isolation.

The running production listener was not stopped or reconfigured. No live Prism/provider calls, project mutations, or swarm were launched for these checks. Workers 6 and 11 still have separate unproven bootstrap cleanup; this report does not claim they were recovered. A new run must show the new R10 build and the requested 60-second admission budget before a live swarm can validate sustained capacity.

Final full-suite result: **2,967 passed, 0 failed, 3 skipped**, 151,042 assertions across 206 files (190.09 seconds). The skipped opt-in checks were the older large-input soak and the separate 16-MiB/64-MiB cases. The new response-heavy R10 scenarios ran as part of the passing full suite. Final `git diff --check` passed.
