# Large-context request latency

The September 6 log snapshot contained 623 accepted requests. Of 562 completed
requests, 68 exceeded one minute and 22 exceeded one minute to the first event.
The changes on `br/fix-large-thread-websocket-reset` address the host-side causes
reproduced from that analysis. They have not been deployed to the protected
production listener.

## Changes and safety boundaries

- CPU preparation now has its own 15-second deadline, separate from the
  one-second wait for transport admission. The original routing deadline and
  abort signal still apply. A preparation timeout returns a sanitized
  `request_preparation_timeout` once; the router does not repeat the CPU work
  across sandboxes.
- The fleet retains the ordinary four-unit limit, while one four-unit bulk
  upload can coexist with two units of medium traffic under a six-unit hard
  ceiling. A second bulk upload remains blocked. Aged bulk waiters do not claim
  the medium headroom. Per-runtime transfer limits and memory accounting remain
  authoritative.
- A queue drain considers each queued job at most once. A blocked job with
  accumulated fairness credit cannot repeatedly consume the same drain pass
  before a new medium request is considered.
- Continuation snapshotting accepts content through the existing 64 MiB
  canonical limit, with the existing structural-node and preparation-queue
  bounds. The old 8 MiB advisory cap silently bypassed inference for larger
  histories.
- Native HTTP matching treats only the seven known, bounded Codex tracing
  fields as non-model metadata. Every current tracing value is restored to the
  immutable full and suffix bodies without alteration. Unknown fields, invalid
  value types, and over-budget tracing metadata remain exact settings.
- Native replay recognizes both the exact provider output and the lossless
  Codex spelling that omits completed status and empty annotations/logprobs.
  Both variants share the existing 4,096-record budget; both commits are awaited
  before public terminal delivery. Text, reasoning ciphertext, IDs, nonempty
  metadata, tool arguments, and unknown fields are preserved. Explicit caller
  continuation fields still bypass inference, including an explicit null.
- Logs now distinguish first text/tool-argument events and provider total time
  from the first metadata event. Request telemetry adds only an input count and
  continuation-presence flag. Fleet telemetry reports ordinary and bulk transfer
  accounting. No request/output text or tracing identifiers are logged.

The known tracing fields are `session_id`, `thread_id`, `turn_id`, `root_turn_id`,
`x-codex-window-id`, `x-codex-installation-id`, and `x-codex-turn-metadata`. They
never create or select authorization scope. Model settings, tools, and unknown
metadata retain exact matching requirements.

## Installed-client proof

The isolated verifier used installed `codex-cli 0.153.4`, a temporary Codex home,
synthetic 18 MiB history, an OS-assigned loopback port, and a fake relay. It did
not read user credentials or call a provider or Prism.

Before the native compatibility correction, the continued request forwarded
18,934,411 bytes and all 18 synthetic history items without a provider parent.
Afterwards, it forwarded 1,582 bytes, no old history items, and the exact parent
response ID. The extended checked-in verifier reproduced this result. Each case
made exactly one relay execution, small requests still used WebSockets, and all
body/WebSocket reservations returned to zero.

The byte reduction is 99.9916%. CLI elapsed time is not a reliable live-latency
benchmark: the baseline follow-up took 7.049 seconds, one corrected run took
1.259 seconds, and a repeated corrected run took 7.196 seconds. These include
client startup, HTTP fallback negotiation, and local scheduling. No production
p50/p95 improvement is claimed from these fixture timings.

## Verification

Final offline result: **2,360 passed, zero failures, zero skipped tests** across
179 files. This includes exact 16 MiB and 64 MiB transport integrity and ten
modeled fourteen-request fleet waves (140 requests). Both TypeScript checks
passed, as did `git diff --check` and the four-case installed-Codex verifier.

Regression tests first reproduced premature preparation expiry, unwanted
rerouting of local preparation work, medium starvation behind a bulk upload,
and silent continuation bypass for an 18 MiB history. Corrected coverage also
includes an exact 55 MiB history, tracing byte boundaries, unknown metadata,
lossy replay rejection, cancellation, bounded cleanup, and terminal ordering.

Run the ordinary suite with `bun --no-env-file test --isolate`. Enable the
offline opt-in integrity and modeled fleet checks with `GP_V2_PERF_LARGE=1`,
`GP_V2_PERF_64M=1`, and `GP_RUN_V2_CAPACITY_SOAK=1`. The latter is a deterministic
component model, not a live sandbox soak.

The installed-client verifier is
`bun --no-env-file scripts/verify_codex_ws_fallback.ts --run --report <path>`.
Type checking uses `bun --no-env-file run typecheck` and
`bun --no-env-file run typecheck:scripts`.

R8/R9/R10 deterministic release checks and pinned Deno checks passed. Every
checked-in R1-R10 release artifact retained its original hash. No revision,
upstream transport, provider retention policy, or execution-risk boundary was
changed. Live Prism/provider timing, production rollout, and recovery fault
injection remain outside this offline implementation task.
