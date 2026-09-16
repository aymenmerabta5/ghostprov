# Review and reliability corrections — 15 September 2026

The current checkout corrects the validated repository-review defects and the implementation gaps identified while analyzing PID 23460. The running production listener was not stopped, restarted, probed, reconfigured, or replaced. These changes take effect when a future process loads the new code; this report does not claim the existing process now has eight healthy workers.

The starting checkout was master at `2f0d799a9e410292d032508d81ece2daacb2b099`, with existing uncommitted admission, cleanup, and diagnostics changes. Their baseline patch and file hashes were saved outside the repository. Existing modifications were preserved. No commit or push was requested or performed.

## Corrected behavior

- **R10 cleanup:** exact current host control ownership remains valid for cleanup-only frames while draining, while new execution remains fenced. Removed, stale, foreign, and mismatched control states cannot authorize cleanup. An unaccepted mixed execution/cleanup flight is not skipped or falsely acknowledged.
- **Shutdown budget:** persistent-supervisor runtimes reserve cleanup time inside the existing absolute deadline. A silent drain or force-stop cannot consume all time before supervisor shutdown. The reserve is capped by the configured ordinary stop budget and half the remaining time, preserving most of long graceful budgets.
- **Held cleanup and recovery:** production all-R10/direct composition now wires the bounded journal-proof and artifact-finalization helper through shared startup permits. Failed/pending held owners no longer block unrelated reconciliation. Repeated bootstrap cleanup failures use existing journal authority before another sandbox/generation starts.
- **Late ownership:** physical cleanup witnesses track actual factory/start/bootstrap/local-close work separately from public stop promises. Late child rejection creates explicit cleanup debt even after an outer stop previously fulfilled. Registry removal requires the exact owner, completed local cleanup, and required remote proof. Rejected cleanup can recover in the same process when authoritative proof becomes available; pending or failed physical cleanup remains fenced.
- **Ingress timing:** native and Anthropic inferred continuations end ingress admission timing at execution risk, retain their full-history memory ownership, and continue forwarding genuine caller cancellation until release.
- **Anthropic stops:** one bounded pending match makes earliest-stop selection independent of chunk boundaries, including Unicode, ties, owners and segment transitions.
- **Fleet diagnostics:** the aggregate sample retains compact runtime summaries; bounded `fleet.runtime_sample` records carry full details correlated by sample ID/index/count. A 32-runtime full-schema fixture fits the unchanged per-event and queue limits. Startup explicitly reports desired, ready, routable, held count and degraded readiness.
- **Cancellation diagnosis:** an exact original request signal may carry a fixed WebSocket cancellation origin and an observed numeric close code. No peer reason text or client identity is retained.
- **Admission security:** only implemented body-consuming HTTP method/path pairs reserve ingress memory. Authentication, body limits, multipart reservation multiplier and Hono dispatch use the same effective pathname, including encoded aliases. Public health and existing 404/405/501 behavior are preserved.
- **Content-free request failures:** shared closed-category projection and sink validation prevent provider messages, arbitrary error atoms and raw request IDs from entering persistent request telemetry. The fallback HTTP console emitter uses the same projection. Provider request IDs are hashed for correlation; authenticated public errors and prelude IDs remain exact.

No upstream stream/store policy, caller provider lineage, model thinking, fallback eligibility, provider retry behavior, or response-window/memory ceiling was relaxed. Transient heartbeat failures still use corroborating transport/sidecar evidence rather than automatically replacing healthy workers.

## Transport efficiency

Three targeted regressions explain concrete improvements without assuming that all observed live delay had one cause:

1. A 32-frame processed flight formerly emitted 32 immediate host ACK updates; it now emits one immediate completed-flight ACK and paces intermediate processed progress.
2. Timer and fast retransmission share a spacing timestamp, preventing immediate duplicate resend of the same immutable flight.
3. After durable cancellation, only unpublished output from that cancelled stream is retired. The active numbered flight and other streams remain exact. A fixture's next flight shrank from 32 mostly obsolete frames to two necessary frames, with accounting released.

Six concurrent response-heavy requests retained effectively unchanged fault-free modeled commitment latency:

| RTT | Before / after commit P95 |
| --- | --- |
| 600 ms | 14.045 / 14.071 s |
| 1,200 ms | 19.103 / 19.077 s |
| 2,500 ms | 43.442 / 43.431 s |

One fixed loss/reorder workload observed commit P95 improving from **9.136 to 7.561 seconds**, with attempted wire bytes falling from **4,206,171 to 3,903,257**. Both runs retained four exact requests/terminals and four simulated upstream executions. This is a bounded modeled result, not proof that the live 20–27 second tail has disappeared. Some harness scheduling uses real wall timers, so small variation is expected.

**Routability remains a live verification limit.** These changes remove measured avoidable ACK/retransmission/cancelled-output overhead; they do not disable the legitimate response-pressure admission gate. Fault-free modeled commitment latency stayed effectively flat. Therefore the original live six-to-one routable pattern is not claimed eliminated, and the still-running old process has not received these fixes. A later authorized candidate workload must compare usable capacity and latency under the same streaming demand before a production improvement is certified.

## Concurrency and context validation

The real mutable R10 engine/peer, real Yjs/Awareness encoding, and synthetic upstreams were used offline.

- Fourteen mixed 128 KiB/1 MiB/2.9 MiB requests across six usable workers out of eight, with 256 one-KiB response events each while later work admits: **14 exact requests/terminals**, zero retransmissions; modeled commit P95 **27.165 s**.
- Forty 128 KiB requests across eight workers under the same streamed response load: **40 exact requests/terminals**, zero retransmissions; modeled commit P95 **6.579 s**.
- Exact incompressible **16 MiB and 64 MiB R10** requests each produced one simulated upstream execution and an exact terminal, with zero host retransmissions.
- Existing opt-in ten-wave, fourteen-agent soak and 16/64 MiB transport cases passed unchanged: **24 tests, 1,526 assertions**.
- Deno journal validation including the real 64 MiB spool boundary: **13 passed, two Windows-inapplicable cases ignored**.

Response flights remain within 32 frames/128 KiB, host flights within existing protocol bounds, output queues bounded, and final request/queue ownership returns to zero in the load fixtures. The fleet harness assigns requests to independent runtime links; the separate router and lifecycle integration suites exercise production selection and recovery contracts.

## Immutable release

Selected R10 identity:

- Build: `fe8bbf0d0b897f227db7b7516871ec2c4f70ae82fa219d85fd316608d176171f`
- Schema: `51eab64c3b30ea9c7989b47c21cdf4743bbb5e718e754849fb64f4216a6b604f`
- Bundle SHA-256: `f05a5a0d052adc3273821e8a48542361fe579e22f2520939501780c14a594ef7`
- Bundle bytes: **614,020**

The prior `16847357...` R10 tuple/artifact is retained as an exact archived retirement identity. All fourteen prior release artifacts matched their pre-task SHA-256 hashes. R1–R9 remain frozen; no R11 or new upstream transport was introduced.

## Final gates

- Focused regression suites, independent auth/privacy reviews, and lifecycle race reviews passed.
- Main and script TypeScript checks passed.
- R8/R9/R10 deterministic and pinned Deno checks passed.
- Release/retirement checks: 49 tests passed.
- Full isolated suite: **3,067 passed, zero failed, three opt-in stress cases skipped**, 152,699 assertions across 210 files in 136.52 seconds. The skipped stress cases were separately enabled and passed.
- The 534 executable/source/configuration files captured before the final run are checked by SHA-256 after validation; documentation artifacts are recorded separately.
- Final source/release hash and diff verification is recorded in the external validation manifest.

The first full run encountered a supervisor afterEach cleanup timeout and a shutdown identity-race test receiving the alternate fail-closed error “control file changed while opening.” The supervisor test passed unchanged in isolation and both subsequent full runs. The descriptor test's concurrent in-place write also reproduced in the second full run. Its fixture now uses the existing process-liveness seam to publish the successor between descriptor reads, deterministically exercising the intended pinned-instance check. All rejection, original-request preservation, and no-receipt assertions remain, with an additional assertion that replacement occurred. The two affected files passed 36 focused tests before the final full run. No production shutdown-control code or test timeout was weakened.

Validation artifacts are under `E:/Temp/ghostprov-fixes-20260915`: `full-suite-verified.log`, `stress-final.log`, `deno-large.log`, `sidecar-final.log`, `r10-scalability.log`, `transport-results.md`, and the before/after transport JSON reports. The initial failed runs remain in `full-suite.log` and `full-suite-final.log`. The successful main test runs used process-local `TEMP`/`TMP` under the repository's ignored `.ghostprov/review-fixes-test-temp`; no global environment or service configuration changed. For the opt-in Deno spool check, the already-installed Deno 2.9.5 binary was invoked directly after the alternate temporary path prevented `bun x --no-install` from locating its cache; nothing was downloaded.

No live Prism/provider experiment, production cleanup, host shutdown, deployment, or hosted CI verification was performed. An actually unreachable supervisor still needs exact recoverable cleanup evidence; the code does not invent that proof or erase journals. The two Windows-inapplicable Deno checks require their supported platform.
