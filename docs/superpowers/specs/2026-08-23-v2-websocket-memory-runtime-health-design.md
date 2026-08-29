# V2 WebSocket Memory and Runtime Health Design

> **Historical and superseded continuation policy (2026-08-28):** This document preserves the design record as written. Any upstream `store:false`, stateless replay, or local generated-history requirement below is superseded by the active `stream:true`/`store:true` persisted-continuation policy in `README.md` and `AGENTS.md`.

## Context

The first eight-sandbox, sixteen-agent trace proved that V2 transport admission is stable: the router never queued work, cancellation quarantine returned to zero, the memory governor never paused, and WebSocket delivery never backpressured. It also exposed two host-side scaling defects:

1. `ResponseGraph` retains a fully flattened conversation on every continued response node without charging those copies to either the per-session or process-wide byte budget. An isolated 80-turn, 32-KiB-per-turn chain accounted for 2.51 MiB while retaining 104.26 MiB of heap and growing RSS by 201.23 MiB. This is quadratic retained memory over a linear continuation chain.
2. `V2SandboxRuntime` calls every rejected request a sandbox failure and reports a lifetime `failures / attempts` ratio as `recentFailureRate`. One cancellation in three requests permanently produced a 0.33 score and nearly removed that sandbox from routing. A definitive client/upstream error similarly stranded another sandbox.

These defects are independent of the one observed missing-model response, which remains outside this change.

## Goals

- Bound retained WebSocket continuation state by the existing per-session and process-wide cache budgets.
- Preserve exact `store:false` and `previous_response_id` reconstruction semantics for Codex.
- Ensure client cancellations and definitive upstream/client failures do not damage sandbox health.
- Temporarily penalize genuinely ambiguous, protocol, and transport failures, while allowing automatic recovery without requiring new traffic.
- Preserve low-overhead routing and the existing `RoutableSandboxSnapshot.recentFailureRate` interface.
- Make an eight- or sixteen-sandbox fleet retain its useful routing diversity under high cancellation churn.

## Non-goals

- Changing Y-Sweet flight, frame, batching, or retransmission limits.
- Reducing the unavoidable cost of transmitting multi-megabyte Codex contexts.
- Changing Prism project count, provisioning, or heartbeat behavior.
- Restarting or modifying the live router on port 18787 during source work.
- Special-casing the single missing-model response without further reproduction.

## WebSocket Continuation Memory

`ResponseGraph` will continue storing one node per completed local response. A node retains only its incremental input, incremental output, model, parent/child links, byte charge, and LRU metadata. It will no longer retain an expanded or flattened conversation.

When Codex sends a local continuation:

1. Traverse parent links iteratively from the selected parent to the root.
2. Reverse that bounded lineage.
3. Clone and append each node's input and output exactly once into one request-scoped history array.
4. Append the new input delta to that array and pass it to the relay.
5. Retain no expanded history after the request-scoped relay body becomes unreachable.

This changes retained memory from quadratic in continuation depth to linear in cached deltas. The active request must still contain one fully materialized history because the upstream Responses request requires it. Existing node-count, per-session byte, process-byte, fork, LRU, and encrypted-reasoning behavior remains unchanged.

The materializer must be iterative so a maximum-depth graph cannot overflow the JavaScript call stack. A missing parent discovered during traversal is treated as `PreviousResponseNotFoundError`; cycles are impossible through the public record path, but traversal remains bounded by the configured node ceiling.

## Runtime Health Classification

A focused runtime-health component will own classification and time decay. It exposes a score from zero through one, where zero is healthy and one is maximally unhealthy.

### Neutral outcomes

The following outcomes do not increase or decrease the score:

- downstream `AbortError`;
- committed cancellation with code `cancelled`;
- definitive committed errors with `ambiguous === false`, including HTTP validation, rate-limit, and upstream service responses;
- normal pre-commit admission/capacity movement with code `capacity` or `capacity_changed`;
- requested draining with code `draining`;
- request/response-bound violations such as `request_too_large` and `response_too_large`.

These outcomes describe the caller, account-wide upstream service, expected lifecycle, or current admission state rather than a faulty sandbox.

### Healthy and unhealthy outcomes

- A proven completed response records a healthy sample.
- An ambiguous committed failure other than cancellation records an unhealthy sample.
- Protocol, instance, channel, and unexpected pre-commit transport failures record an unhealthy sample.
- Unknown errors default to unhealthy so new infrastructure failures are not silently ignored.

### Decay

The score uses a bounded exponential moving update with weight `0.20` and a sixty-second time half-life:

- unhealthy: `score = score + 0.20 * (1 - score)` after applying elapsed-time decay;
- healthy: `score = score * 0.80` after applying elapsed-time decay;
- neutral: no sample update;
- read: apply elapsed-time decay without requiring another request.

One isolated infrastructure failure therefore creates a temporary 0.20 penalty instead of a permanent 1/N lifetime ratio. Repeated failures accumulate, successes heal, and an unused sandbox automatically becomes eligible again. The existing router policy continues consuming `recentFailureRate` without another hot-path abstraction.

## Error Handling and Compatibility

- Classification uses stable structural fields (`name`, `code`, `committed`, and `ambiguous`) so it remains reliable across bundling boundaries.
- The health scorer accepts only finite monotonic clock values and clamps every public score to `[0, 1]`.
- Response graph budgets continue counting retained incremental node data. Request-scoped materialized histories are not cached and do not need a persistent cache charge.
- Public HTTP, SSE, WebSocket, Images, and model behavior does not change.
- No new dependency is introduced.

## Tests

### WebSocket memory

- Preserve existing continuation, fork, eviction, encrypted-reasoning, and shared-budget tests.
- Build an eighty-node linear continuation with 32-KiB deltas under forced GC.
- Verify the current implementation fails an amplification gate while the new implementation keeps retained heap growth below ten times accounted node bytes.
- Verify a maximum-depth continuation materializes iteratively and in exact chronological order.
- Verify `clear()` releases the complete shared byte charge.

### Runtime health

- Verify cancellation, abort, definitive committed errors, capacity movement, draining, and request bounds are neutral.
- Verify ambiguous, protocol, instance, and unknown transport errors are unhealthy.
- Verify one unhealthy sample produces 0.20, halves after sixty seconds without traffic, accumulates under repetition, and heals after success.
- Integrate the scorer into `V2SandboxRuntime` and prove cancellation or definitive HTTP failure leaves `recentFailureRate` at zero.
- Prove an ambiguous failure raises the rate and that the rate later decays through the injected clock.
- Reproduce the observed early-cancellation scenario and prove it cannot permanently starve a sandbox in routing policy inputs.

## Acceptance

- The isolated memory reproduction accounts for approximately 2.5 MiB and retains less than 25 MiB of additional heap after forced GC.
- Retained continuation memory is linear rather than quadratic in chain depth.
- One hundred neutral cancellations across a runtime leave `recentFailureRate === 0`.
- A genuinely ambiguous failure remains visible immediately and decays by half within sixty seconds without new requests.
- Existing 512-request concurrency, WebSocket continuation, V2 fault recovery, cancellation quarantine, Images, large-payload, and dynamic 32-sandbox tests remain green.
- `bun run typecheck`, `bun test --isolate`, daemon tests, audit, and protected HTTP/WebSocket smoke all pass.
